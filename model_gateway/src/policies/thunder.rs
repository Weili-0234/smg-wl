//! ThunderPolicy: program-aware capacity-tracking load-balancing policy.
//!
//! See `docs/thunder/03-algorithm.md` for the Python-faithful algorithm core
//! and `docs/thunder/04-smg-integration.md` §5 for SMG-side trait integration.
//!
//! ## Phase 3 scope (this commit)
//!
//! Skeleton only. Implements `LoadBalancingPolicy` with **Default sub-mode**:
//! select the worker with the **fewest active programs** assigned to it. This
//! is the Q5.6 faithful Python "least-active-count" rule.
//!
//! Deferred to later phases (per worklog D-19):
//! - `usage_consumer` task receiving UsageEvent → P4
//! - `WorkerRegistry::subscribe_events` integration → P5
//! - TR sub-mode capacity gate (admission 503) → P5
//! - Pause/resume + BFD + force-timeout → P6
//! - `ProgramRequestGuard` RAII → P6

use std::{
    collections::HashMap,
    f64,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedSender},
    Notify, RwLock,
};
use tracing::{debug, trace, warn};

use super::{
    thunder_metrics, LoadBalancingPolicy, SelectWorkerInfo, StreamingProgressEvent, UsageEvent,
};
use crate::worker::Worker;

/// Neutral fallback for chars-per-token ratio when no calibration data is
/// available. Matches the SMG MVP's hardcoded `chars / 4` baseline.
const NEUTRAL_RATIO: f64 = 4.0;
/// Neutral fallback for completion-fraction (actual_completion / max_tokens).
/// 0.5 = "expect about half of the declared budget on average" — a saner
/// guess than 0 (always under-reserve) or 1.0 (always over-reserve).
const NEUTRAL_FRACTION: f64 = 0.5;
/// EMA mixing weight for new observations. Match Python's `0.2` (router.py:404).
const EMA_ALPHA: f64 = 0.2;
/// Wall-time half-life for calibration decay back toward neutral (M3 D-31).
const CALIBRATION_HALF_LIFE: Duration = Duration::from_secs(3600);
/// Fallback completion budget when no `declared_max_tokens` is present in the
/// request (e.g., legacy clients omitting `max_tokens`).
const FALLBACK_COMPLETION_TOKENS: u64 = 256;
/// M5 starvation mitigation: Paused programs older than this get priority-
/// boosted ahead of larger programs in BFD ordering. Default = half of the
/// force_resume_timeout (1800s default), so a program waits ≤ 900s before
/// gaining priority and ≤ 1800s before force-admit kicks in.
const PAUSED_PRIORITY_BOOST_AFTER: Duration = Duration::from_secs(900);

/// Decay-weighted EMA update for a calibration value. Decays the previously
/// stored value toward `neutral` based on wall-time elapsed since `last_at`,
/// then mixes in `observed` at weight `EMA_ALPHA`.
///
/// First-observation special case: directly assigns `observed` (matches
/// Python `router.py:399` "first request → directly assign").
pub(crate) fn update_calibration_with_decay(
    stored: &mut Option<f64>,
    last_at: &mut Option<Instant>,
    observed: f64,
    neutral: f64,
    now: Instant,
) {
    let decayed = match (*stored, *last_at) {
        (Some(prev), Some(t_old)) => {
            let elapsed = now.saturating_duration_since(t_old).as_secs_f64();
            let half_life_s = CALIBRATION_HALF_LIFE.as_secs_f64();
            let retain = (-elapsed * f64::consts::LN_2 / half_life_s).exp();
            retain * prev + (1.0 - retain) * neutral
        }
        _ => neutral,
    };

    let new_value = match *stored {
        None => observed,
        Some(_) => EMA_ALPHA * observed + (1.0 - EMA_ALPHA) * decayed,
    };

    *stored = Some(new_value);
    *last_at = Some(now);
}

/// Sub-mode selector. Phase 3 only implements `Default`. `Tr` (transactional)
/// arrives in Phase 5 with capacity-gated admission; until then Tr falls back
/// to Default with a warn log so the gateway keeps routing traffic.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ThunderSubMode {
    #[default]
    Default,
    Tr,
}

/// Configuration for `ThunderPolicy`. Defaults match worklog D-4.
#[derive(Debug, Clone)]
pub struct ThunderConfig {
    pub sub_mode: ThunderSubMode,
    /// Reserved fraction of backend capacity to keep free for in-flight work.
    /// (Used by P5+ TR-mode gate; ignored in Default.)
    pub capacity_reserved_fraction: f64,
    /// Wait time when admission blocks (P5+).
    pub resume_timeout_secs: u64,
    /// Tick interval for the scheduler task (P6+).
    pub scheduler_tick_ms: u64,
    /// Period between `/get_server_info` capacity fetches against each
    /// backend, in seconds. P4+: drives `BackendState.capacity_tokens`.
    pub capacity_poll_interval_secs: u64,
}

impl Default for ThunderConfig {
    fn default() -> Self {
        Self {
            sub_mode: ThunderSubMode::Default,
            capacity_reserved_fraction: 0.10,
            resume_timeout_secs: 1800,
            scheduler_tick_ms: 100,
            capacity_poll_interval_secs: 5,
        }
    }
}

/// Lifecycle state of a Program in Thunder's algorithm.
///
/// Mirrors the paper's `τ ∈ {Reasoning, Acting}` phases plus the engineering
/// states needed by the implementation (`Idle` for never-admitted programs,
/// `Paused` for off-GPU programs in the global waiting queue).
///
/// State transitions:
/// - Idle → Reasoning: request admitted (`assign()`)
/// - Reasoning → Acting: request completes and the agent is between LLM calls
///   (`usage_consumer_task` or `ProgramRequestGuard::Drop`, when `in_flight == 0`)
/// - Reasoning (with `marked_for_pause`) → Paused: deferred pause taken when
///   in-flight work drains
/// - Acting/Idle → Paused: scheduler picks as victim immediately
/// - Paused → Reasoning: scheduler picks for wake (`wake_program_to`)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ProgramStatus {
    #[default]
    Idle,
    Reasoning,
    Acting,
    Paused,
}

/// Per-program state tracked by Thunder.
#[derive(Debug, Clone)]
pub struct Program {
    pub program_id: String,
    /// URL of the backend currently assigned to this program (sticky routing).
    pub backend_url: Option<String>,
    /// Count of in-flight requests for this program (admission tracking).
    pub in_flight: u32,
    /// Count of requests currently waiting in `pick_tr` for capacity to free
    /// (i.e. blocked on the per-program `Notify`). Used by `try_greedy_resume`
    /// to tier paused programs as "Reasoning" (has a pending request) vs
    /// "Acting" (idle between LLM calls), matching paper Eq 8.
    pub pending_requests: u32,
    /// Latest known context footprint for this program. On UsageEvent this is
    /// REPLACED with `event.total_tokens` (not added) — the paper's `c_P` is
    /// the program's current context length, not cumulative history.
    pub total_tokens: u64,
    /// Tokens from this program currently included in
    /// `backend.active_program_tokens` as retained KV footprint. Excludes
    /// transient admission reservations (those live in
    /// `estimated_reserved_tokens`). Unbooked on pause and replaced on each
    /// UsageEvent so the backend total never double-counts a program.
    pub accounted_tokens: u64,
    /// Step counter — increments per admission.
    pub step_count: u32,
    /// Tokens this program has *reserved* on its backend at admit-time.
    pub estimated_reserved_tokens: u64,
    /// Per-program `chars / actual_prefill_tokens` ratio (M3).
    pub local_char_to_token_ratio: Option<f64>,
    /// Per-program `actual_completion_tokens / declared_max_tokens` (M3).
    pub local_completion_fraction: Option<f64>,
    /// Last calibration timestamp (M3 wall-time decay).
    pub last_calibration_at: Option<Instant>,
    /// Lifecycle state (M4). Default Idle.
    pub status: ProgramStatus,
    /// Pause-deferral flag (M4). Set when scheduler wants to pause an ACTING
    /// program; pause completes when status transitions out of Acting.
    pub marked_for_pause: bool,
    /// When the program was last paused (M4 + M5 starvation mitigation).
    pub paused_at: Option<Instant>,
}

impl Program {
    fn new(program_id: String) -> Self {
        Self {
            program_id,
            backend_url: None,
            in_flight: 0,
            pending_requests: 0,
            total_tokens: 0,
            accounted_tokens: 0,
            step_count: 0,
            estimated_reserved_tokens: 0,
            local_char_to_token_ratio: None,
            local_completion_fraction: None,
            last_calibration_at: None,
            status: ProgramStatus::Idle,
            marked_for_pause: false,
            paused_at: None,
        }
    }
}

/// Per-backend (worker URL) state tracked by Thunder.
#[derive(Debug, Clone, Default)]
pub struct BackendState {
    /// Set of program_ids currently assigned to this backend (Default-mode signal).
    pub active_programs: std::collections::HashSet<String>,
    /// Cumulative tokens dispatched to this backend (P4+).
    pub active_program_tokens: u64,
    /// Reported KV-cache capacity (P4+ via metrics client).
    pub capacity_tokens: u64,
}

impl BackendState {
    fn active_count(&self) -> usize {
        self.active_programs.len()
    }
}

/// Mutable state shared across the policy + scheduler task.
///
/// **Performance footgun (D-3):** Single `RwLock<RouterState>` is the simplest
/// correctness model. Phase 4+ may benchmark and migrate to per-backend
/// sharding if contention becomes measurable.
#[derive(Debug, Default)]
pub struct RouterState {
    pub programs: HashMap<String, Program>,
    pub backends: HashMap<String, BackendState>,
    /// Per-program resume signals (TR sub-mode, P5+P6). Key: program_id;
    /// value: `Notify` the scheduler / `usage_consumer` fires when backend
    /// capacity frees so that paused requests can re-evaluate admission.
    ///
    /// Lifetime: a `Notify` is created on first pause and stays in the map
    /// until the policy is dropped — leaking a few `Arc<Notify>` per
    /// long-lived program is cheap (≤ tens of bytes each) and avoids any
    /// race where a freshly-arriving second request mis-pairs with a
    /// just-deleted handle. Re-cleanup deferred to P9 if it ever matters.
    pub waiting_events: HashMap<String, Arc<Notify>>,
    /// Global chars/token ratio (M3 calibration; tier 2 fallback after
    /// per-program). Updated by `usage_consumer_task` on every UsageEvent
    /// with `prompt_tokens > 0`.
    pub global_char_to_token_ratio: Option<f64>,
    /// Global completion fraction (`actual_completion / declared_max_tokens`).
    pub global_completion_fraction: Option<f64>,
    /// Last global calibration timestamp (M3 wall-time decay).
    pub last_global_calibration_at: Option<Instant>,
}

impl RouterState {
    /// Ensure backends map is populated for the given URL set, removing
    /// entries no longer present. Called on every selection — cheap because
    /// HashMap ops are O(1) and the set is tiny (≤ tens of backends).
    fn refresh_backends(&mut self, urls: &[String]) {
        for url in urls {
            self.backends.entry(url.clone()).or_default();
        }
        // Drop backends no longer in the active set
        self.backends.retain(|url, _| urls.iter().any(|u| u == url));
    }

    /// Default-mode selection: pick the backend whose active_program count is
    /// smallest. Ties broken by URL string ordering (deterministic).
    fn select_least_active(&self, urls: &[String]) -> Option<String> {
        urls.iter()
            .min_by(|a, b| {
                let a_count = self
                    .backends
                    .get(a.as_str())
                    .map(|s| s.active_count())
                    .unwrap_or(0);
                let b_count = self
                    .backends
                    .get(b.as_str())
                    .map(|s| s.active_count())
                    .unwrap_or(0);
                a_count.cmp(&b_count).then_with(|| a.cmp(b))
            })
            .cloned()
    }

    /// Record (or refresh) the program → backend assignment.
    fn assign(&mut self, program_id: &str, backend_url: &str) {
        let program = self
            .programs
            .entry(program_id.to_string())
            .or_insert_with(|| Program::new(program_id.to_string()));
        program.backend_url = Some(backend_url.to_string());
        program.in_flight = program.in_flight.saturating_add(1);
        program.step_count = program.step_count.saturating_add(1);
        // M4: status transitions to Reasoning on admit. paused_at cleared.
        program.status = ProgramStatus::Reasoning;
        program.paused_at = None;
        let backend = self.backends.entry(backend_url.to_string()).or_default();
        backend.active_programs.insert(program_id.to_string());
    }

    /// Effective context length used for paper Eq 8/9 scoring. Takes the max
    /// of the program's known footprints so the sort key is monotonic against
    /// the largest reservation the scheduler has committed to.
    fn context_tokens_for_scoring(p: &Program) -> u64 {
        p.total_tokens
            .max(p.accounted_tokens)
            .max(p.estimated_reserved_tokens)
            .max(1)
    }

    /// Pick a victim program on `backend_url` to pause. Implements paper Eq 9:
    /// `S_pause(P) = 1/c_P + 𝕀(τ = Acting)` — Acting programs strictly first
    /// (their retained KV is idle while a tool runs), then shortest context.
    /// Excludes already-Paused programs and those with `marked_for_pause` set.
    fn pick_victim(&self, backend_url: &str) -> Option<String> {
        self.programs
            .iter()
            .filter(|(_, p)| {
                p.backend_url.as_deref() == Some(backend_url)
                    && p.status != ProgramStatus::Paused
                    && !p.marked_for_pause
            })
            .min_by_key(|(pid, p)| {
                (
                    p.status != ProgramStatus::Acting,
                    Self::context_tokens_for_scoring(p),
                    (*pid).clone(),
                )
            })
            .map(|(pid, _)| pid.clone())
    }

    /// Transition `pid` to Paused on `backend_url`. If the program still has
    /// in-flight LLM work, defer by setting `marked_for_pause=true` (taken
    /// when `in_flight` reaches 0 via `check_marked_for_pause`). Acting
    /// programs are off-GPU between LLM calls and can be paused immediately.
    /// Idempotent.
    fn pause_until_safe(&mut self, pid: &str, backend_url: &str) {
        let Some(p) = self.programs.get_mut(pid) else {
            return;
        };
        // Reasoning programs with in-flight requests cannot be cleanly
        // preempted. Acting programs have in_flight == 0 by construction so
        // they pass straight through to the immediate-pause path.
        if p.in_flight > 0 {
            p.marked_for_pause = true;
            return;
        }
        if p.status == ProgramStatus::Paused {
            return; // already paused
        }
        let reserved = p.estimated_reserved_tokens;
        let accounted = p.accounted_tokens;
        // Snapshot whether the program had a pending request at pause time —
        // used by `try_greedy_resume` to put it in the Reasoning tier per
        // paper Eq 8. (Also dynamically observed at resume time, but recording
        // the phase intent is informative for debug.)
        let was_acting = p.status == ProgramStatus::Acting;
        p.status = ProgramStatus::Paused;
        p.paused_at = Some(Instant::now());
        p.estimated_reserved_tokens = 0;
        p.accounted_tokens = 0;
        p.backend_url = None;
        if let Some(b) = self.backends.get_mut(backend_url) {
            b.active_program_tokens = b
                .active_program_tokens
                .saturating_sub(reserved.saturating_add(accounted));
            b.active_programs.remove(pid);
        }
        // Ensure waiting_event exists so wake can target it.
        self.waiting_events
            .entry(pid.to_string())
            .or_insert_with(|| Arc::new(Notify::new()));
        trace!(
            program_id = %pid,
            backend = %backend_url,
            from_acting = was_acting,
            reserved_unbooked = reserved,
            accounted_unbooked = accounted,
            "thunder pause"
        );
    }

    /// Shortest-first greedy resume with paper-Eq-8 three-tier priority:
    ///
    ///   Tier 1 (Reasoning): `pending_requests > 0 && step_count > 1`
    ///                       — has a client request blocked on the per-program
    ///                       Notify; resuming unblocks real work.
    ///   Tier 2 (New):       `step_count == 1`
    ///                       — admitted once but never completed a turn;
    ///                       prioritized so first-time programs don't starve.
    ///   Tier 3 (Acting):    everything else
    ///                       — paused while idle between LLM calls.
    ///
    /// Within each tier, programs sort ASC by `context_tokens_for_scoring`
    /// (`1/c_P` from paper Eq 8). Starvation mitigation boosts programs
    /// paused longer than `PAUSED_PRIORITY_BOOST_AFTER` to the front
    /// (cross-tier) so no program waits forever.
    ///
    /// Backend placement is BFD across DP replicas: paused programs go to the
    /// backend with the most remaining capacity that fits them (paper §8 —
    /// once paused, a program's KV is assumed evicted, so resume placement is
    /// node-agnostic and serves load balancing).
    ///
    /// Programs that don't fit anywhere stay Paused for next tick.
    fn try_greedy_resume(&mut self) {
        let now = Instant::now();
        // (pid, tier, est, starvation_boost)
        // tier: 0 = Reasoning, 1 = New, 2 = Acting
        let mut paused: Vec<(String, u8, u64, bool)> = self
            .programs
            .iter()
            .filter(|(_, p)| p.status == ProgramStatus::Paused)
            .map(|(pid, p)| {
                let tier: u8 = if p.pending_requests > 0 && p.step_count > 1 {
                    0 // Reasoning: client request waiting
                } else if p.step_count <= 1 {
                    1 // New: fresh program, no completed turn yet
                } else {
                    2 // Acting: idle between LLM calls
                };
                let est = Self::context_tokens_for_scoring(p);
                let starved = p
                    .paused_at
                    .map(|t| now.saturating_duration_since(t) > PAUSED_PRIORITY_BOOST_AFTER)
                    .unwrap_or(false);
                (pid.clone(), tier, est, starved)
            })
            .collect();

        // Sort key: starvation-boosted first, then by tier (R < New < A),
        // then by context length ASC (shortest first per paper Eq 8),
        // then by pid for determinism.
        paused.sort_by(|a, b| {
            let (a_pid, a_tier, a_est, a_starved) = a;
            let (b_pid, b_tier, b_est, b_starved) = b;
            b_starved
                .cmp(a_starved)
                .then_with(|| a_tier.cmp(b_tier))
                .then_with(|| a_est.cmp(b_est))
                .then_with(|| a_pid.cmp(b_pid))
        });

        let urls: Vec<String> = self.backends.keys().cloned().collect();

        for (pid, tier, est, _) in paused {
            // Re-fetch sorted backends per iteration since assignments
            // mutate remaining capacity.
            let mut by_remaining: Vec<(String, u64)> = urls
                .iter()
                .map(|u| {
                    let remaining = self
                        .backends
                        .get(u)
                        .map(|b| b.capacity_tokens.saturating_sub(b.active_program_tokens))
                        .unwrap_or(0);
                    (u.clone(), remaining)
                })
                .collect();
            by_remaining.sort_by_key(|(_, c)| std::cmp::Reverse(*c));

            for (url, remaining) in &by_remaining {
                if *remaining >= est {
                    self.wake_program_to(&pid, url, est);
                    debug!(
                        program_id = %pid,
                        backend = %url,
                        tier = tier,
                        est = est,
                        "thunder resume"
                    );
                    break;
                }
            }
            // No fit → stays Paused for next tick.
        }
    }

    /// M5+M6: assign a paused program to a backend and targeted-notify it.
    fn wake_program_to(&mut self, pid: &str, backend_url: &str, estimated: u64) {
        if let Some(p) = self.programs.get_mut(pid) {
            p.backend_url = Some(backend_url.to_string());
            p.status = ProgramStatus::Reasoning;
            p.estimated_reserved_tokens = estimated;
            p.paused_at = None;
        }
        if let Some(b) = self.backends.get_mut(backend_url) {
            b.active_program_tokens = b.active_program_tokens.saturating_add(estimated);
            b.active_programs.insert(pid.to_string());
        }
        if let Some(notify) = self.waiting_events.get(pid) {
            notify.notify_one(); // ★ M6: targeted, not broadcast
        }
    }

    /// M4: scheduler tick body. Iterates backends; for each over the
    /// capacity threshold (after `capacity_reserved_fraction`), repeatedly
    /// picks victims and pauses until under threshold.
    fn proactive_pause_pass(&mut self, capacity_reserved_fraction: f64) {
        let urls: Vec<String> = self.backends.keys().cloned().collect();
        'outer: for url in urls {
            'inner: loop {
                let over = match self.backends.get(&url) {
                    Some(b) if b.capacity_tokens > 0 => {
                        let thr = (b.capacity_tokens as f64
                            * (1.0 - capacity_reserved_fraction))
                            as u64;
                        b.active_program_tokens > thr
                    }
                    _ => break 'inner,
                };
                if !over {
                    break 'inner;
                }
                let Some(victim) = self.pick_victim(&url) else {
                    continue 'outer;
                };
                self.pause_until_safe(&victim, &url);
            }
        }
    }

    /// Check `marked_for_pause` flag and apply deferred pause when the
    /// program is no longer in-flight. Called from `usage_consumer_task`
    /// (success path) and `ProgramRequestGuard::Drop` (error/disconnect
    /// path) — both points where `in_flight` may have just reached 0.
    pub(crate) fn check_marked_for_pause(&mut self, pid: &str) {
        let (mark, url) = match self.programs.get(pid) {
            Some(p) if p.marked_for_pause && p.in_flight == 0 => (true, p.backend_url.clone()),
            _ => return,
        };
        if !mark {
            return;
        }
        if let Some(p) = self.programs.get_mut(pid) {
            p.marked_for_pause = false;
        }
        if let Some(u) = url {
            self.pause_until_safe(pid, &u);
        }
    }

    /// Get-or-create the per-program `Notify` that paused requests await on.
    /// (TR sub-mode, P5+P6.)
    fn waiting_event_for(&mut self, program_id: &str) -> Arc<Notify> {
        self.waiting_events
            .entry(program_id.to_string())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    /// Returns `true` if `backend_url` has headroom to absorb
    /// `estimated_tokens` more, considering the configured
    /// `reserved_fraction` slack. Optimistic on unknown / not-yet-polled
    /// backends so cold-start traffic isn't gated by missing capacity data.
    fn has_capacity(
        &self,
        backend_url: &str,
        estimated_tokens: u64,
        reserved_fraction: f64,
    ) -> bool {
        let Some(b) = self.backends.get(backend_url) else {
            return true; // unknown backend → optimistic admit
        };
        if b.capacity_tokens == 0 {
            return true; // not yet polled → optimistic admit (warmup)
        }
        // Saturating math: `reserved_fraction` is validated to [0.0,1.0] in
        // config, but the cast to u64 still rounds down so the bound is
        // conservative.
        let usable_f = (b.capacity_tokens as f64) * (1.0 - reserved_fraction).max(0.0);
        let usable = usable_f as u64;
        b.active_program_tokens.saturating_add(estimated_tokens) <= usable
    }
}

/// Thunder policy entry point. Held by the policy registry as
/// `Arc<dyn LoadBalancingPolicy>`.
#[derive(Debug)]
pub struct ThunderPolicy {
    config: ThunderConfig,
    state: Arc<RwLock<RouterState>>,
    /// Backend capacity fetcher. Held so tests can inject a mock; production
    /// uses `HttpMetricsClient`. The poll task receives a clone — this field
    /// stays so the policy can be `Debug` and so future code paths (P5+ TR
    /// admission gate) can call `metrics_client.fetch_capacity` synchronously.
    #[expect(
        dead_code,
        reason = "owned by the spawned poll task via clone; field kept for Debug + future direct use in P5+"
    )]
    metrics_client: Arc<dyn thunder_metrics::MetricsClient>,
    /// Sender side of the usage-event channel. Routers fire-and-forget a
    /// `UsageEvent` here on every successful non-streaming response; the
    /// consumer task spawned in `with_metrics_client` updates per-program +
    /// per-backend token totals.
    usage_tx: UnboundedSender<UsageEvent>,
    /// Sender side of the streaming-progress channel (M2). Routers emit
    /// `StreamingProgressEvent` per ~20 tokens during streaming; the
    /// consumer task drains and increments `Program.total_tokens`. Mirrors
    /// `usage_tx` precedent.
    progress_tx: UnboundedSender<StreamingProgressEvent>,
}

impl ThunderPolicy {
    /// Construct a `ThunderPolicy` backed by `HttpMetricsClient` (production path).
    pub fn new(config: ThunderConfig) -> Self {
        Self::with_metrics_client(
            config,
            Arc::new(thunder_metrics::HttpMetricsClient) as Arc<dyn thunder_metrics::MetricsClient>,
        )
    }

    /// Construct a `ThunderPolicy` with a caller-provided metrics client.
    /// Used by tests to inject a mock without spinning up an HTTP server.
    ///
    /// **Side effects:** spawns a `tokio::task` that polls each known backend
    /// every `config.capacity_poll_interval_secs` seconds and updates
    /// `BackendState.capacity_tokens`. The task holds a `Weak<RwLock<RouterState>>`
    /// reference to break the would-be Arc cycle so the policy can drop cleanly
    /// — when `upgrade()` returns `None` (policy dropped), the task exits.
    pub fn with_metrics_client(
        config: ThunderConfig,
        metrics_client: Arc<dyn thunder_metrics::MetricsClient>,
    ) -> Self {
        let state = Arc::new(RwLock::new(RouterState::default()));

        // ----- capacity poll task -----
        let poll_secs = config.capacity_poll_interval_secs.max(1);
        let state_for_poll = Arc::downgrade(&state);
        let mc_for_poll = metrics_client.clone();
        #[expect(
            clippy::disallowed_methods,
            reason = "fire-and-forget capacity poller — exits cleanly when ThunderPolicy is dropped (Weak::upgrade returns None)"
        )]
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(poll_secs));
            loop {
                interval.tick().await;
                let Some(state_arc) = state_for_poll.upgrade() else {
                    debug!("ThunderPolicy state dropped; capacity poll task exiting");
                    return;
                };
                let urls: Vec<String> = {
                    let guard = state_arc.read().await;
                    guard.backends.keys().cloned().collect()
                };
                for url in urls {
                    match mc_for_poll.fetch_capacity(&url).await {
                        Ok(cap) => {
                            let mut guard = state_arc.write().await;
                            if let Some(b) = guard.backends.get_mut(&url) {
                                b.capacity_tokens = cap.capacity_tokens;
                            }
                            trace!(
                                worker_url = %url,
                                capacity_tokens = cap.capacity_tokens,
                                "capacity refreshed"
                            );
                        }
                        Err(e) => {
                            warn!(worker_url = %url, error = %e, "capacity fetch failed");
                        }
                    }
                }
            }
        });

        // ----- usage_consumer task -----
        // Unbounded channel — backpressure tradeoff (D-2): routers fire-and-forget
        // and must not block the response path. If the consumer falls far behind,
        // memory grows; this is acceptable because each event is ~64B and the
        // consumer is a tight async loop. Bounded + try_send considered for P9
        // if benchmarks show pathological growth.
        let (usage_tx, mut usage_rx) = unbounded_channel::<UsageEvent>();
        let state_for_consumer = Arc::downgrade(&state);
        #[expect(
            clippy::disallowed_methods,
            reason = "fire-and-forget usage consumer — exits when the channel closes (policy dropped) via Weak::upgrade returning None or recv returning None"
        )]
        tokio::spawn(async move {
            while let Some(event) = usage_rx.recv().await {
                let Some(state_arc) = state_for_consumer.upgrade() else {
                    debug!("ThunderPolicy state dropped; usage consumer exiting");
                    return;
                };
                let pid = event
                    .program_id
                    .clone()
                    .unwrap_or_else(|| "default".to_string());

                // Snapshot per-program accounting BEFORE mutating state.
                // TR sub-mode (P5+) reserves `estimated_reserved_tokens` on
                // the chosen backend at admit time so concurrent arrivals
                // see the load. On UsageEvent we un-reserve that estimate
                // AND unbook the program's prior retained KV footprint, then
                // replace both with the latest response's `total_tokens`.
                // Paper's `c_P` is the program's current context length —
                // NOT cumulative history — so per-request totals must replace
                // the previously-accounted value, not add to it.
                let mut guard = state_arc.write().await;
                let (reserved, previous_accounted) = guard
                    .programs
                    .get(&pid)
                    .map(|p| (p.estimated_reserved_tokens, p.accounted_tokens))
                    .unwrap_or((0, 0));
                let event_total_tokens = u64::from(event.total_tokens);

                if let Some(b) = guard.backends.get_mut(&event.backend_url) {
                    b.active_program_tokens = b
                        .active_program_tokens
                        .saturating_sub(reserved.saturating_add(previous_accounted));
                    b.active_program_tokens =
                        b.active_program_tokens.saturating_add(event_total_tokens);
                }
                let now = Instant::now();
                // M3 calibration update: chars/token ratio (per-program + global)
                // and completion fraction (per-program + global). Excludes cached
                // prefill from the prefill ratio (Anthropic prompt caching, M8).
                let actual_prefill = u64::from(event.prompt_tokens).saturating_sub(
                    event.cache_read_input_tokens.map(u64::from).unwrap_or(0),
                );
                let observed_ratio = if event.request_text_chars > 0 && actual_prefill > 0 {
                    Some(event.request_text_chars as f64 / actual_prefill as f64)
                } else {
                    None
                };
                let observed_fraction = match event.declared_max_tokens {
                    Some(mt) if mt > 0 && event.completion_tokens > 0 => Some(
                        (f64::from(event.completion_tokens) / f64::from(mt)).clamp(0.0, 1.0),
                    ),
                    _ => None,
                };

                if let Some(p) = guard.programs.get_mut(&pid) {
                    // REPLACE, not add — `total_tokens` is the current context
                    // length, not cumulative history.
                    p.total_tokens = event_total_tokens;
                    p.accounted_tokens = event_total_tokens;
                    p.estimated_reserved_tokens = 0;
                    if p.in_flight > 0 {
                        p.in_flight -= 1;
                    }
                    // Paper τ-transition: response complete + no other LLM
                    // call in flight ⇒ program is now in Acting phase (agent
                    // is between LLM calls, tool exec / orchestration time).
                    if p.in_flight == 0 && p.status == ProgramStatus::Reasoning {
                        p.status = ProgramStatus::Acting;
                    }
                    if let Some(observed) = observed_ratio {
                        update_calibration_with_decay(
                            &mut p.local_char_to_token_ratio,
                            &mut p.last_calibration_at,
                            observed,
                            NEUTRAL_RATIO,
                            now,
                        );
                    }
                    if let Some(observed) = observed_fraction {
                        update_calibration_with_decay(
                            &mut p.local_completion_fraction,
                            &mut p.last_calibration_at,
                            observed,
                            NEUTRAL_FRACTION,
                            now,
                        );
                    }
                }
                // Global ratios — split disjoint field borrows so the helper
                // can take two `&mut` simultaneously without aliasing.
                {
                    let RouterState {
                        global_char_to_token_ratio,
                        global_completion_fraction,
                        last_global_calibration_at,
                        ..
                    } = &mut *guard;
                    if let Some(observed) = observed_ratio {
                        update_calibration_with_decay(
                            global_char_to_token_ratio,
                            last_global_calibration_at,
                            observed,
                            NEUTRAL_RATIO,
                            now,
                        );
                    }
                    if let Some(observed) = observed_fraction {
                        update_calibration_with_decay(
                            global_completion_fraction,
                            last_global_calibration_at,
                            observed,
                            NEUTRAL_FRACTION,
                            now,
                        );
                    }
                }
                // M4: take any deferred pause if the request that just
                // completed brought in_flight to 0 while marked_for_pause
                // was set by the scheduler during the in-flight window.
                guard.check_marked_for_pause(&pid);

                // Broadcast wake — capacity may have freed for any
                // currently-paused program. ★ Decision tag (autonomous):
                // Broadcast (vs targeted-by-backend) is simpler. The
                // re-evaluation under the lock filters out non-applicable
                // wakes immediately. Backend count is bounded (≤ tens) so
                // thundering herd is small. Optimization deferred to P9.
                let waiting: Vec<Arc<Notify>> =
                    guard.waiting_events.values().cloned().collect();
                drop(guard);
                for n in &waiting {
                    n.notify_waiters();
                }

                trace!(
                    program_id = %pid,
                    backend = %event.backend_url,
                    total_tokens = event.total_tokens,
                    reserved_unwound = reserved,
                    previous_accounted_unwound = previous_accounted,
                    waiters_woken = waiting.len(),
                    "usage applied"
                );
            }
            debug!("usage_consumer channel closed; task exiting");
        });

        // ----- scheduler tick task (M4 proactive pause; M5 BFD resume) -----
        // Runs every `scheduler_tick_ms` (default 100ms). On each tick:
        //   (a) proactive_pause_pass: pause victims on backends over capacity
        //   (b) try_greedy_resume (M5): BFD resume of paused programs
        // The tick can also be woken early via `capacity_freed_signal`
        // (M6: usage_consumer / Drop fire it when capacity frees).
        let tick_dur = Duration::from_millis(config.scheduler_tick_ms.max(10));
        let reserved_fraction = config.capacity_reserved_fraction;
        let state_for_scheduler = Arc::downgrade(&state);
        #[expect(
            clippy::disallowed_methods,
            reason = "fire-and-forget scheduler — exits when policy dropped via Weak::upgrade"
        )]
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick_dur);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let Some(state_arc) = state_for_scheduler.upgrade() else {
                    debug!("ThunderPolicy state dropped; scheduler tick exiting");
                    return;
                };
                let mut guard = state_arc.write().await;
                guard.proactive_pause_pass(reserved_fraction);
                guard.try_greedy_resume(); // M5 BFD greedy_resume + M6 targeted notify
            }
        });

        // ----- progress_consumer task (M2 incremental streaming tokens) -----
        // Mirrors usage_consumer: unbounded channel; fire-and-forget receiver
        // updates only `Program.total_tokens` (NOT `backend.active_program_tokens`,
        // matching Python's two-layer model where backend stats are computed
        // from program totals at observation time, not maintained incrementally).
        let (progress_tx, mut progress_rx) = unbounded_channel::<StreamingProgressEvent>();
        let state_for_progress = Arc::downgrade(&state);
        #[expect(
            clippy::disallowed_methods,
            reason = "fire-and-forget progress consumer — exits when channel closes (policy dropped)"
        )]
        tokio::spawn(async move {
            while let Some(event) = progress_rx.recv().await {
                let Some(state_arc) = state_for_progress.upgrade() else {
                    debug!("ThunderPolicy state dropped; progress consumer exiting");
                    return;
                };
                let mut guard = state_arc.write().await;
                if let Some(p) = guard.programs.get_mut(&event.program_id) {
                    p.total_tokens = p.total_tokens.saturating_add(event.delta_tokens);
                    trace!(
                        program_id = %event.program_id,
                        delta = event.delta_tokens,
                        cumulative = p.total_tokens,
                        "incremental streaming progress applied"
                    );
                }
            }
            debug!("progress_consumer channel closed; task exiting");
        });

        Self {
            config,
            state,
            metrics_client,
            usage_tx,
            progress_tx,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(ThunderConfig::default())
    }

    /// Create a `ProgramRequestGuard` for `program_id`. Held by the router for
    /// the lifetime of an in-flight request — on `Drop` (cancel / error /
    /// dropped future) it asynchronously decrements `Program.in_flight` and
    /// broadcasts the per-program `Notify` so paused requests can re-check.
    pub fn create_guard(&self, program_id: &str) -> ProgramRequestGuard {
        ProgramRequestGuard::new(self.state.clone(), program_id.to_string())
    }

    /// Test/admin accessor — clones the current state for read-only inspection.
    /// (Used by Phase 8's `/thunder/programs` admin endpoint when it lands.)
    pub async fn snapshot_state(&self) -> RouterState {
        let guard = self.state.read().await;
        RouterState {
            programs: guard.programs.clone(),
            backends: guard.backends.clone(),
            // `Arc<Notify>` clone is a refcount bump — cheap. Snapshots
            // shouldn't usually inspect Notify identity but cloning keeps
            // the type symmetric.
            waiting_events: guard.waiting_events.clone(),
            global_char_to_token_ratio: guard.global_char_to_token_ratio,
            global_completion_fraction: guard.global_completion_fraction,
            last_global_calibration_at: guard.last_global_calibration_at,
        }
    }
}

/// RAII guard tracking a request's in-flight lifetime in `ThunderPolicy`.
///
/// **Lifecycle:** Created by `ThunderPolicy::create_guard` after a successful
/// `select_worker_async` admit, held by the router for the duration of the
/// upstream call.
///
/// **Drop semantics (D-22 simplification):** if the guard is dropped without
/// a prior `complete()` call, an async cleanup task is spawned that:
///   1. Decrements `Program.in_flight` (so admission accounting stays sane
///      even if the client cancels mid-request).
///   2. Broadcasts every `Notify` in `waiting_events` (a slot may have just
///      freed).
///
/// Calling `complete()` *suppresses* the cleanup — used on the happy path
/// where `usage_consumer` already handled in-flight decrement via the
/// matching `UsageEvent`.
///
/// ★ Decision tag (autonomous): `Drop` is sync but the `RouterState` lock is
/// async, so the cleanup is `tokio::spawn`ed. We capture `Weak<RwLock<…>>`
/// to avoid keeping the policy alive past its natural lifetime; if the
/// policy was dropped before the cleanup runs, `upgrade()` returns `None`
/// and the task exits.
#[derive(Debug)]
pub struct ProgramRequestGuard {
    state: Weak<RwLock<RouterState>>,
    program_id: String,
    completed: bool,
}

impl ProgramRequestGuard {
    /// Construct a guard. Prefer `ThunderPolicy::create_guard`.
    pub fn new(state: Arc<RwLock<RouterState>>, program_id: String) -> Self {
        Self {
            state: Arc::downgrade(&state),
            program_id,
            completed: false,
        }
    }

    /// Mark the request as having completed via the normal `UsageEvent` path
    /// (the consumer already decremented `in_flight`). Suppresses cleanup
    /// on `Drop` so we don't double-decrement.
    pub fn complete(&mut self) {
        self.completed = true;
    }

    /// Test-only accessor for the program_id (avoids exposing the field).
    #[cfg(test)]
    pub fn program_id(&self) -> &str {
        &self.program_id
    }
}

impl Drop for ProgramRequestGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let Some(state) = self.state.upgrade() else {
            return; // policy already dropped — nothing to clean up
        };
        let pid = std::mem::take(&mut self.program_id);
        // `tokio::spawn` is fire-and-forget; matches the existing capacity-
        // poll / usage-consumer fire-and-forget pattern in this file.
        #[expect(
            clippy::disallowed_methods,
            reason = "fire-and-forget cleanup task — exits when policy dropped via Weak::upgrade returning None"
        )]
        tokio::spawn(async move {
            let mut guard = state.write().await;

            // Mirror usage_consumer_task's un-reserve pattern but skip the
            // "+ actual_total_tokens" step since no UsageEvent arrived.
            let (reserved, backend_url) = guard
                .programs
                .get(&pid)
                .map(|p| (p.estimated_reserved_tokens, p.backend_url.clone()))
                .unwrap_or((0, None));

            if let Some(url) = backend_url {
                if let Some(b) = guard.backends.get_mut(&url) {
                    b.active_program_tokens = b.active_program_tokens.saturating_sub(reserved);
                }
            }
            if let Some(p) = guard.programs.get_mut(&pid) {
                p.estimated_reserved_tokens = 0;
                if p.in_flight > 0 {
                    p.in_flight -= 1;
                }
                // Paper τ-transition: disconnect / error also drains in-flight;
                // if the program was Reasoning and now has no LLM call running,
                // it's in Acting (between LLM calls). The scheduler treats
                // this as an eligible pause candidate per paper Eq 9.
                if p.in_flight == 0 && p.status == ProgramStatus::Reasoning {
                    p.status = ProgramStatus::Acting;
                }
            }
            // Take any deferred pause if the disconnect just brought
            // in_flight to 0 while marked_for_pause is set.
            guard.check_marked_for_pause(&pid);
            // A slot may have freed — broadcast so paused programs re-check.
            // (M6 will replace broadcast with a scheduler signal.)
            let waiting: Vec<Arc<Notify>> = guard.waiting_events.values().cloned().collect();
            drop(guard);
            for n in &waiting {
                n.notify_waiters();
            }
            trace!(
                program_id = %pid,
                reserved_unwound = reserved,
                "ProgramRequestGuard drop fallback (no usage)"
            );
        });
    }
}

/// RAII guard for the per-program `pending_requests` counter.
///
/// `pick_tr` increments `pending_requests` when a request fails its first
/// admission attempt and parks on the per-program `Notify`. The Reasoning
/// tier in `try_greedy_resume` keys off `pending_requests > 0` to know a
/// client is waiting on a paused program, so the counter must stay balanced
/// across every exit path — including future cancellation.
///
/// Explicit success / timeout paths call `consume(&mut state)` under the
/// already-held write lock for a synchronous decrement. If the future is
/// dropped mid-await (upstream HTTP timeout, runtime shutdown, parent
/// cancellation), `Drop` fires a fire-and-forget async decrement task,
/// mirroring [`ProgramRequestGuard::Drop`]. Without this guard, cancelled
/// `pick_tr` futures leak the counter and `try_greedy_resume` eventually
/// misclassifies idle programs as Reasoning-tier candidates.
pub(crate) struct PendingRequestGuard {
    state: Weak<RwLock<RouterState>>,
    program_id: String,
    armed: bool,
}

impl PendingRequestGuard {
    pub(crate) fn new(state: Weak<RwLock<RouterState>>, program_id: String) -> Self {
        Self {
            state,
            program_id,
            armed: true,
        }
    }

    /// Consume the guard synchronously while the caller still holds the
    /// write lock. Disarms `Drop`, then decrements `pending_requests`.
    /// Saturates at zero so concurrent races (e.g. a stale prior `Drop` that
    /// already ran) cannot underflow.
    pub(crate) fn consume(mut self, state: &mut RouterState) {
        self.armed = false;
        if let Some(p) = state.programs.get_mut(&self.program_id) {
            if p.pending_requests > 0 {
                p.pending_requests -= 1;
            }
        }
    }
}

impl Drop for PendingRequestGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(state) = self.state.upgrade() else {
            return; // policy already dropped — nothing to clean up
        };
        let pid = std::mem::take(&mut self.program_id);
        #[expect(
            clippy::disallowed_methods,
            reason = "fire-and-forget cleanup task — exits when policy dropped via Weak::upgrade returning None"
        )]
        tokio::spawn(async move {
            let mut guard = state.write().await;
            if let Some(p) = guard.programs.get_mut(&pid) {
                if p.pending_requests > 0 {
                    p.pending_requests -= 1;
                }
            }
        });
    }
}

#[async_trait]
impl LoadBalancingPolicy for ThunderPolicy {
    fn select_worker(&self, workers: &[Arc<dyn Worker>], info: &SelectWorkerInfo) -> Option<usize> {
        // Sync path: use blocking_write. Only safe outside an async context (it
        // panics if called from inside a tokio runtime). The canonical entry
        // point is `select_worker_async`; this exists for trait-object
        // completeness + the per-policy parity tests added in Phase 1.
        let mut state = self.state.blocking_write();
        Self::pick_default_inner(&mut state, workers, info, self.config.sub_mode)
    }

    async fn select_worker_async(
        &self,
        workers: &[Arc<dyn Worker>],
        info: &SelectWorkerInfo<'_>,
    ) -> Option<usize> {
        // ★ Decision tag (autonomous): Default mode keeps the single-locked
        // fast path (no awaits inside the critical section). TR mode is a
        // multi-await loop (try-admit → register Notify → drop lock → await
        // → loop), so it owns its own lock acquisition pattern in `pick_tr`.
        match self.config.sub_mode {
            ThunderSubMode::Default => {
                let mut state = self.state.write().await;
                Self::pick_default_inner(&mut state, workers, info, ThunderSubMode::Default)
            }
            ThunderSubMode::Tr => self.pick_tr(workers, info).await,
        }
    }

    fn name(&self) -> &'static str {
        "thunder"
    }

    fn needs_request_text(&self) -> bool {
        // Default mode does not consult cache; TR mode (P5+) may. Keep false
        // for now to skip request_text extraction in routers.
        false
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Hand the router a `Sender` so it can fire-and-forget a `UsageEvent`
    /// after each successful non-streaming response. The consumer task
    /// spawned in `with_metrics_client` drains the channel and updates
    /// per-program + per-backend token counters.
    fn usage_sender(&self) -> Option<&UnboundedSender<UsageEvent>> {
        Some(&self.usage_tx)
    }

    fn streaming_progress_sender(&self) -> Option<&UnboundedSender<StreamingProgressEvent>> {
        Some(&self.progress_tx)
    }
}

impl ThunderPolicy {
    /// Inner helper called by both sync and async select paths.
    fn pick_default_inner(
        state: &mut RouterState,
        workers: &[Arc<dyn Worker>],
        info: &SelectWorkerInfo,
        sub_mode: ThunderSubMode,
    ) -> Option<usize> {
        if workers.is_empty() {
            return None;
        }

        // Refresh backend index from current worker set
        let urls: Vec<String> = workers
                    .iter()
                    .map(|w| w.url().to_string())
                    .filter(|u| info.avoid_backend != Some(u.as_str()))
                    .collect();
        state.refresh_backends(&urls);

        // Q5.2 fallback: missing program_id resolves to a "default" pseudo-program
        let program_id = info.program_id.unwrap_or("default");

        // Sticky routing: if program already has a backend and that backend is
        // still in the available worker list, reuse it.
        if let Some(existing_url) = state
            .programs
            .get(program_id)
            .and_then(|p| p.backend_url.as_ref())
            .cloned()
        {
            if let Some(idx) = workers.iter().position(|w| w.url() == existing_url) {
                state.assign(program_id, &existing_url);
                trace!(program_id = %program_id, backend = %existing_url, "thunder sticky route");
                return Some(idx);
            }
        }

        match sub_mode {
            ThunderSubMode::Default => {
                let chosen_url = state.select_least_active(&urls)?;
                let idx = workers.iter().position(|w| w.url() == chosen_url)?;
                state.assign(program_id, &chosen_url);
                debug!(
                    program_id = %program_id,
                    backend = %chosen_url,
                    active_count = state.backends.get(&chosen_url).map(|s| s.active_count()).unwrap_or(0),
                    "thunder default-mode select"
                );
                Some(idx)
            }
            ThunderSubMode::Tr => {
                // The async `select_worker_async` dispatches TR to `pick_tr`
                // before this function is ever reached. Sync `select_worker`
                // (parity tests + trait-object completeness) cannot await on
                // a Notify so we degrade gracefully to least-active here.
                warn!(
                    "ThunderSubMode::Tr called via sync `select_worker`; \
                     capacity gate skipped (sync path cannot await)"
                );
                let chosen_url = state.select_least_active(&urls)?;
                let idx = workers.iter().position(|w| w.url() == chosen_url)?;
                state.assign(program_id, &chosen_url);
                Some(idx)
            }
        }
    }

    // ---------- TR sub-mode (capacity-gated admission, P5+P6) ----------

    /// TR sub-mode: capacity-aware admission. If the chosen backend has no
    /// headroom for the program's estimated token cost, register a per-program
    /// `Notify` and await with a deadline. On wake (or timeout) re-evaluate.
    ///
    /// Loop shape (per `Notify::notify_waiters` semantics):
    ///   1. acquire write-lock, try to admit
    ///   2. else register Notify → drop lock
    ///   3. await `notified()` with `timeout(remaining_deadline, ...)`
    ///   4. on timeout → force-admit fallback (skip capacity check)
    ///   5. on wake → loop back to step 1
    ///
    /// Tokens are *reserved* on the chosen backend at admit time so a herd of
    /// arrivals doesn't all see the same headroom and double-admit. The
    /// reservation is un-done by `usage_consumer` when the actual `UsageEvent`
    /// arrives (see Task 3).
    async fn pick_tr(
        &self,
        workers: &[Arc<dyn Worker>],
        info: &SelectWorkerInfo<'_>,
    ) -> Option<usize> {
        if workers.is_empty() {
            return None;
        }
        let program_id = info.program_id.unwrap_or("default").to_string();
        let timeout_dur = Duration::from_secs(self.config.resume_timeout_secs);
        let deadline = Instant::now() + timeout_dur;

        // RAII guard for the `pending_requests` counter on `Program`. The
        // Reasoning tier in `try_greedy_resume` reads `pending_requests > 0`
        // to identify paused programs with a client request waiting. Every
        // exit path of `pick_tr` must decrement the counter, including future
        // cancellation — handled here by `PendingRequestGuard::Drop` firing
        // an async decrement. Sync success/timeout paths use `consume()` for
        // a synchronous decrement under the already-held write lock.
        let mut pending_guard: Option<PendingRequestGuard> = None;

        loop {
            // ---- Step 1: acquire lock and try to admit ----
            // M3: `estimated_tokens` re-computed each iteration since calibration
            // may have drifted. Saved across the lock drop so force-admit fall-
            // through after timeout can reuse it.
            let estimated_tokens: u64;
            let notify = {
                let mut state = self.state.write().await;
                let urls: Vec<String> = workers
                    .iter()
                    .map(|w| w.url().to_string())
                    .filter(|u| info.avoid_backend != Some(u.as_str()))
                    .collect();
                state.refresh_backends(&urls);
                estimated_tokens = self.estimate_request_tokens(info, &state);

                // Choose a candidate backend: sticky if assigned & still
                // healthy, else least-active.
                let chosen_url = state
                    .programs
                    .get(&program_id)
                    .and_then(|p| p.backend_url.clone())
                    .filter(|u| urls.contains(u))
                    .or_else(|| state.select_least_active(&urls));

                let Some(chosen_url) = chosen_url else {
                    if let Some(g) = pending_guard.take() {
                        g.consume(&mut state);
                    }
                    return None; // no backends in registry
                };

                // M5: detect scheduler-pre-reserved program (wake_program_to
                // already booked the backend on our behalf). Skip the duplicate
                // reservation but still bump in_flight/step_count via assign().
                let already_reserved = state
                    .programs
                    .get(&program_id)
                    .map(|p| {
                        p.estimated_reserved_tokens > 0
                            && p.backend_url.as_deref() == Some(chosen_url.as_str())
                    })
                    .unwrap_or(false);

                if already_reserved
                    || state.has_capacity(
                        &chosen_url,
                        estimated_tokens,
                        self.config.capacity_reserved_fraction,
                    )
                {
                    let Some(idx) = workers.iter().position(|w| w.url() == chosen_url) else {
                        if let Some(g) = pending_guard.take() {
                            g.consume(&mut state);
                        }
                        return None;
                    };
                    state.assign(&program_id, &chosen_url);
                    if !already_reserved {
                        if let Some(b) = state.backends.get_mut(&chosen_url) {
                            b.active_program_tokens =
                                b.active_program_tokens.saturating_add(estimated_tokens);
                        }
                        if let Some(p) = state.programs.get_mut(&program_id) {
                            p.estimated_reserved_tokens =
                                p.estimated_reserved_tokens.saturating_add(estimated_tokens);
                        }
                    }
                    if let Some(g) = pending_guard.take() {
                        g.consume(&mut state);
                    }
                    debug!(
                        program_id = %program_id,
                        backend = %chosen_url,
                        est = estimated_tokens,
                        scheduler_resumed = already_reserved,
                        "thunder TR admit"
                    );
                    return Some(idx);
                }

                // Block: register a Notify for this program and mark the
                // program as having a pending request (Reasoning-tier signal
                // for resume). Note the `notified()` future MUST be created
                // AFTER the next pause checkpoint — registering it inside the
                // locked region here is wrong (it would be dropped on
                // drop(state)). We return the Arc<Notify> and create the
                // future just below.
                if pending_guard.is_none() {
                    let now = Instant::now();
                    let p = state
                        .programs
                        .entry(program_id.clone())
                        .or_insert_with(|| Program::new(program_id.clone()));
                    p.pending_requests = p.pending_requests.saturating_add(1);
                    // Brand-new programs (`status == Idle`, never admitted)
                    // need an explicit `Paused` status so that
                    // `try_greedy_resume`'s iteration filter sees them and
                    // applies the 3-tier scoring (paper Eq 8). Without this,
                    // first-admission failures would wake only via the
                    // capacity-free broadcast, with no priority among
                    // concurrent newcomers. We deliberately do NOT flip
                    // Reasoning (in-flight) or Acting (idle-with-accounting)
                    // programs to Paused here — those still own backend
                    // capacity and must go through the scheduler's
                    // pause_until_safe path for correct unbookkeeping.
                    if p.status == ProgramStatus::Idle {
                        p.status = ProgramStatus::Paused;
                        p.paused_at = Some(now);
                    }
                    pending_guard = Some(PendingRequestGuard::new(
                        Arc::downgrade(&self.state),
                        program_id.clone(),
                    ));
                }
                let n = state.waiting_event_for(&program_id);
                debug!(
                    program_id = %program_id,
                    backend = %chosen_url,
                    est = estimated_tokens,
                    cap = state.backends.get(&chosen_url).map(|b| b.capacity_tokens).unwrap_or(0),
                    used = state.backends.get(&chosen_url).map(|b| b.active_program_tokens).unwrap_or(0),
                    "thunder TR pause (full)"
                );
                n
                // lock dropped here at end of block
            };

            // ---- Step 2: await Notify with deadline ----
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                warn!(
                    program_id = %program_id,
                    "thunder TR force-resume on timeout (deadline already passed)"
                );
                if let Some(g) = pending_guard.take() {
                    let mut state = self.state.write().await;
                    g.consume(&mut state);
                }
                return self
                    .force_admit_after_timeout(workers, &program_id, estimated_tokens)
                    .await;
            }
            // Subtle: register the future BEFORE awaiting. `Notify::notified`
            // returns a future that registers when first polled; awaiting
            // through `tokio::time::timeout` polls it once which is enough.
            let waited = tokio::time::timeout(remaining, notify.notified()).await;
            if waited.is_err() {
                warn!(
                    program_id = %program_id,
                    "thunder TR force-resume on timeout"
                );
                if let Some(g) = pending_guard.take() {
                    let mut state = self.state.write().await;
                    g.consume(&mut state);
                }
                return self
                    .force_admit_after_timeout(workers, &program_id, estimated_tokens)
                    .await;
            }
            // Notified — loop and re-check capacity. May still be full
            // (broadcast wake notifies all waiters), in which case we
            // re-pause.
        }
    }

    /// Conservative token-cost estimate for a request: 4 chars / token for
    /// Estimate token cost of a request (M3 calibrated). Three-tier lookup:
    /// per-program `local_char_to_token_ratio` → `RouterState.global_*` →
    /// `NEUTRAL_RATIO=4.0`. Same tiered lookup for completion fraction.
    /// Caller must hold a `&RouterState` (read or write guard); typically the
    /// caller is `pick_default_inner` or `pick_tr` which already hold the lock.
    #[expect(
        clippy::unused_self,
        reason = "method signature stable; self may be used in future Tier-2 polish for per-protocol ratio"
    )]
    fn estimate_request_tokens(&self, info: &SelectWorkerInfo<'_>, state: &RouterState) -> u64 {
        let request_chars = info.request_text.map(str::len).unwrap_or(0);

        let chars_per_token = info
            .program_id
            .and_then(|pid| state.programs.get(pid))
            .and_then(|p| p.local_char_to_token_ratio)
            .or(state.global_char_to_token_ratio)
            .filter(|r| *r > 0.0)
            .unwrap_or(NEUTRAL_RATIO);
        let prompt_estimate = (request_chars as f64 / chars_per_token).ceil() as u64;

        let completion_estimate = match info.declared_max_tokens {
            Some(mt) if mt > 0 => {
                let fraction = info
                    .program_id
                    .and_then(|pid| state.programs.get(pid))
                    .and_then(|p| p.local_completion_fraction)
                    .or(state.global_completion_fraction)
                    .map(|f| f.clamp(0.0, 1.0))
                    .unwrap_or(NEUTRAL_FRACTION);
                (f64::from(mt) * fraction).ceil() as u64
            }
            _ => FALLBACK_COMPLETION_TOKENS,
        };

        prompt_estimate.saturating_add(completion_estimate)
    }

    /// Last-resort admit when the resume-timeout deadline fires. Picks the
    /// least-active backend regardless of capacity and reserves the estimate
    /// so usage_consumer can un-reserve it on completion.
    async fn force_admit_after_timeout(
        &self,
        workers: &[Arc<dyn Worker>],
        program_id: &str,
        estimated_tokens: u64,
    ) -> Option<usize> {
        let mut state = self.state.write().await;
        // Force-admit is last resort — ignore avoid_backend filter (we'd rather
        // hit a previously-failed backend than time out the request entirely).
        let urls: Vec<String> = workers.iter().map(|w| w.url().to_string()).collect();
        state.refresh_backends(&urls);
        let chosen_url = state.select_least_active(&urls)?;
        let idx = workers.iter().position(|w| w.url() == chosen_url)?;
        state.assign(program_id, &chosen_url);
        if let Some(b) = state.backends.get_mut(&chosen_url) {
            b.active_program_tokens = b.active_program_tokens.saturating_add(estimated_tokens);
        }
        if let Some(p) = state.programs.get_mut(program_id) {
            p.estimated_reserved_tokens =
                p.estimated_reserved_tokens.saturating_add(estimated_tokens);
        }
        debug!(
            program_id = %program_id,
            backend = %chosen_url,
            est = estimated_tokens,
            "thunder TR force-admit after timeout"
        );
        Some(idx)
    }
}

#[cfg(test)]
mod tests {
    use openai_protocol::worker::HealthCheckConfig;

    use super::*;
    use crate::worker::{BasicWorkerBuilder, WorkerType};

    fn no_health_check() -> HealthCheckConfig {
        HealthCheckConfig {
            disable_health_check: true,
            ..Default::default()
        }
    }

    fn mock_workers(n: usize) -> Vec<Arc<dyn Worker>> {
        (0..n)
            .map(|i| {
                Arc::new(
                    BasicWorkerBuilder::new(format!("http://w{i}:8000"))
                        .worker_type(WorkerType::Regular)
                        .api_key("test")
                        .health_config(no_health_check())
                        .build(),
                ) as Arc<dyn Worker>
            })
            .collect()
    }

    #[tokio::test]
    async fn default_mode_picks_least_active() {
        let policy = ThunderPolicy::with_defaults();
        let workers = mock_workers(3);
        // Two requests with different program_ids → should land on different backends
        // (least-active starts at 0 for each, so picks w0 then w1 by deterministic tiebreak).
        let info1 = SelectWorkerInfo {
            program_id: Some("p1"),
            ..Default::default()
        };
        let info2 = SelectWorkerInfo {
            program_id: Some("p2"),
            ..Default::default()
        };
        let i1 = policy.select_worker_async(&workers, &info1).await;
        let i2 = policy.select_worker_async(&workers, &info2).await;
        assert!(i1.is_some());
        assert!(i2.is_some());
        // Sticky: same program goes to same backend
        let i1_again = policy.select_worker_async(&workers, &info1).await;
        assert_eq!(i1, i1_again, "thunder must be sticky on program_id");
    }

    #[tokio::test]
    async fn missing_program_id_falls_back_to_default_key() {
        let policy = ThunderPolicy::with_defaults();
        let workers = mock_workers(2);
        let info = SelectWorkerInfo::default(); // program_id = None
        let i1 = policy.select_worker_async(&workers, &info).await;
        let i2 = policy.select_worker_async(&workers, &info).await;
        // Both hit the "default" pseudo-program → sticky to same backend
        assert_eq!(i1, i2);
    }

    #[tokio::test]
    async fn empty_worker_set_returns_none() {
        let policy = ThunderPolicy::with_defaults();
        let info = SelectWorkerInfo::default();
        assert_eq!(policy.select_worker_async(&[], &info).await, None);
    }

    #[tokio::test]
    async fn snapshot_state_after_routes() {
        let policy = ThunderPolicy::with_defaults();
        let workers = mock_workers(2);
        let info = SelectWorkerInfo {
            program_id: Some("snap-test"),
            ..Default::default()
        };
        let _ = policy.select_worker_async(&workers, &info).await;
        let state = policy.snapshot_state().await;
        assert!(state.programs.contains_key("snap-test"));
        let prog = &state.programs["snap-test"];
        assert!(prog.backend_url.is_some());
        assert_eq!(prog.step_count, 1);
    }

    /// Stub `MetricsClient` for unit tests — no HTTP, returns a fixed capacity.
    #[derive(Debug, Default)]
    struct StubMetrics;
    #[async_trait]
    impl thunder_metrics::MetricsClient for StubMetrics {
        async fn fetch_capacity(
            &self,
            _worker_url: &str,
        ) -> Result<thunder_metrics::BackendCapacity, String> {
            Ok(thunder_metrics::BackendCapacity {
                capacity_tokens: 10_000,
                model_name: Some("stub-model".to_string()),
            })
        }
    }

    /// Sending a UsageEvent through `policy.usage_sender()` must reach the
    /// consumer task and bump `Program.total_tokens` for the matching pid.
    #[tokio::test]
    async fn usage_event_updates_program_total_tokens() {
        let policy = ThunderPolicy::with_metrics_client(
            ThunderConfig::default(),
            Arc::new(StubMetrics) as Arc<dyn thunder_metrics::MetricsClient>,
        );
        // Route once so the program exists in state (in_flight increments to 1).
        let workers = mock_workers(2);
        let info = SelectWorkerInfo {
            program_id: Some("usage-test"),
            ..Default::default()
        };
        let _ = policy.select_worker_async(&workers, &info).await;

        let tx = policy
            .usage_sender()
            .expect("ThunderPolicy must expose a usage sender");
        tx.send(UsageEvent {
            program_id: Some("usage-test".to_string()),
            backend_url: workers[0].url().to_string(),
            prompt_tokens: 50,
            completion_tokens: 30,
            total_tokens: 80,
            request_text_chars: 200,
            cache_read_input_tokens: None,
            declared_max_tokens: None,
        })
        .expect("send must succeed (consumer alive)");

        // Give the consumer a brief moment to drain. Yield doesn't always
        // schedule the spawned task; sleep a tiny amount.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let state = policy.snapshot_state().await;
        let prog = state
            .programs
            .get("usage-test")
            .expect("program must exist after route");
        assert_eq!(
            prog.total_tokens, 80,
            "usage event must record current total_tokens (REPLACE)"
        );
        assert_eq!(prog.in_flight, 0, "consumer must decrement in_flight on event");

        let backend = state
            .backends
            .get(workers[0].url())
            .expect("backend state must exist");
        assert_eq!(
            backend.active_program_tokens, 80,
            "backend active_program_tokens must track current footprint"
        );

        // Send a second UsageEvent on the same program; the backend total
        // must REPLACE the prior accounted footprint, not double-count it.
        tx.send(UsageEvent {
            program_id: Some("usage-test".to_string()),
            backend_url: workers[0].url().to_string(),
            prompt_tokens: 90,
            completion_tokens: 30,
            total_tokens: 120,
            request_text_chars: 260,
            cache_read_input_tokens: None,
            declared_max_tokens: None,
        })
        .expect("send must succeed (consumer alive)");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let state = policy.snapshot_state().await;
        let prog = state
            .programs
            .get("usage-test")
            .expect("program must exist after second usage");
        assert_eq!(prog.total_tokens, 120);
        let backend = state
            .backends
            .get(workers[0].url())
            .expect("backend state must exist");
        assert_eq!(
            backend.active_program_tokens, 120,
            "backend footprint must replace prior context, not accumulate per-turn totals"
        );
    }

    /// `program_id = None` on the event must default to the "default" pid
    /// (matches the routing-side fallback in `pick_default_inner`).
    #[tokio::test]
    async fn usage_event_with_none_pid_targets_default_pseudo_program() {
        let policy = ThunderPolicy::with_metrics_client(
            ThunderConfig::default(),
            Arc::new(StubMetrics) as Arc<dyn thunder_metrics::MetricsClient>,
        );
        let workers = mock_workers(1);
        // Route with no pid → creates "default" program entry.
        let _ = policy
            .select_worker_async(&workers, &SelectWorkerInfo::default())
            .await;
        let tx = policy.usage_sender().expect("usage sender must be Some");
        tx.send(UsageEvent {
            program_id: None,
            backend_url: workers[0].url().to_string(),
            prompt_tokens: 1,
            completion_tokens: 2,
            total_tokens: 3,
            request_text_chars: 0,
            cache_read_input_tokens: None,
            declared_max_tokens: None,
        })
        .expect("send");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let state = policy.snapshot_state().await;
        let prog = state.programs.get("default").expect("default program");
        assert_eq!(prog.total_tokens, 3);
    }

    // ---------- TR sub-mode tests (Phase 5+6) ----------

    /// `has_capacity` returns true on unknown / not-yet-polled backends
    /// (cold-start optimism) and applies `reserved_fraction` slack when
    /// capacity is known.
    #[tokio::test]
    async fn has_capacity_optimistic_on_unknown_or_zero() {
        let mut state = RouterState::default();
        // Unknown backend → optimistic (true)
        assert!(state.has_capacity("http://unknown:8000", 1_000, 0.10));
        // Known but not-yet-polled (capacity_tokens = 0) → optimistic
        state.refresh_backends(&["http://w0:8000".to_string()]);
        assert!(state.has_capacity("http://w0:8000", 1_000, 0.10));
        // Known + polled: 1000 capacity, 0.10 reserved → 900 usable
        state
            .backends
            .get_mut("http://w0:8000")
            .expect("seeded above")
            .capacity_tokens = 1_000;
        assert!(state.has_capacity("http://w0:8000", 800, 0.10));
        assert!(!state.has_capacity("http://w0:8000", 901, 0.10));
    }

    /// TR-mode admit on a healthy backend reserves `estimated_reserved_tokens`
    /// on both `Program` and `BackendState`.
    #[tokio::test]
    async fn tr_mode_admit_reserves_estimated_tokens() {
        let policy = ThunderPolicy::with_metrics_client(
            ThunderConfig {
                sub_mode: ThunderSubMode::Tr,
                capacity_reserved_fraction: 0.0,
                resume_timeout_secs: 5,
                ..Default::default()
            },
            Arc::new(StubMetrics) as Arc<dyn thunder_metrics::MetricsClient>,
        );
        let workers = mock_workers(1);
        // Manually seed capacity (without waiting for the poll task).
        {
            let mut g = policy.state.write().await;
            g.refresh_backends(&[workers[0].url().to_string()]);
            g.backends
                .get_mut(workers[0].url())
                .expect("seeded above")
                .capacity_tokens = 10_000;
        }
        let info = SelectWorkerInfo {
            program_id: Some("tr-admit"),
            request_text: Some(&"x".repeat(40)), // 40 chars / 4 = 10 prompt tokens
            ..Default::default()
        };
        let idx = policy.select_worker_async(&workers, &info).await;
        assert_eq!(idx, Some(0));

        let snap = policy.snapshot_state().await;
        let prog = snap.programs.get("tr-admit").expect("program created");
        // 10 (prompt) + 256 (completion budget) = 266
        assert_eq!(prog.estimated_reserved_tokens, 266);
        let backend = snap.backends.get(workers[0].url()).expect("backend tracked");
        assert_eq!(backend.active_program_tokens, 266);
    }

    /// usage_consumer must un-reserve `estimated_reserved_tokens` when the
    /// matching `UsageEvent` arrives, replacing it with the actual usage.
    #[tokio::test]
    async fn tr_mode_usage_consumer_unreserves_then_applies_actual() {
        let policy = ThunderPolicy::with_metrics_client(
            ThunderConfig {
                sub_mode: ThunderSubMode::Tr,
                capacity_reserved_fraction: 0.0,
                resume_timeout_secs: 5,
                ..Default::default()
            },
            Arc::new(StubMetrics) as Arc<dyn thunder_metrics::MetricsClient>,
        );
        let workers = mock_workers(1);
        {
            let mut g = policy.state.write().await;
            g.refresh_backends(&[workers[0].url().to_string()]);
            g.backends
                .get_mut(workers[0].url())
                .expect("seeded")
                .capacity_tokens = 10_000;
        }
        let info = SelectWorkerInfo {
            program_id: Some("tr-unreserve"),
            request_text: Some(&"y".repeat(40)),
            ..Default::default()
        };
        let _ = policy.select_worker_async(&workers, &info).await;

        // Snapshot before: reserved = 266
        let pre = policy.snapshot_state().await;
        assert_eq!(
            pre.backends
                .get(workers[0].url())
                .expect("seeded")
                .active_program_tokens,
            266
        );

        // Send a UsageEvent reporting actual_total = 100 tokens
        let tx = policy.usage_sender().expect("usage sender");
        tx.send(UsageEvent {
            program_id: Some("tr-unreserve".to_string()),
            backend_url: workers[0].url().to_string(),
            prompt_tokens: 60,
            completion_tokens: 40,
            total_tokens: 100,
            request_text_chars: 40,
            cache_read_input_tokens: None,
            declared_max_tokens: None,
        })
        .expect("send");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let post = policy.snapshot_state().await;
        let prog = post.programs.get("tr-unreserve").expect("program");
        assert_eq!(
            prog.estimated_reserved_tokens, 0,
            "reservation must be cleared"
        );
        assert_eq!(prog.total_tokens, 100, "actual total must be recorded");
        let backend = post.backends.get(workers[0].url()).expect("backend");
        assert_eq!(
            backend.active_program_tokens, 100,
            "backend total = actual (266 reserved un-done, +100 actual)"
        );
    }

    /// Pause-resume happy path: a TR request sees zero capacity, blocks on
    /// Notify, then resumes when capacity is freed (via a synthetic
    /// UsageEvent broadcast).
    #[tokio::test]
    async fn tr_mode_pauses_then_resumes_on_capacity_free() {
        let policy = Arc::new(ThunderPolicy::with_metrics_client(
            ThunderConfig {
                sub_mode: ThunderSubMode::Tr,
                capacity_reserved_fraction: 0.0,
                resume_timeout_secs: 30, // long enough; the resume path fires first
                ..Default::default()
            },
            Arc::new(StubMetrics) as Arc<dyn thunder_metrics::MetricsClient>,
        ));
        let workers = mock_workers(1);
        // Saturate against the polled capacity. `StubMetrics::fetch_capacity`
        // returns 10_000, and the poll task's first immediate tick will
        // overwrite anything we set here, so we must use 10_000 — both the
        // pre-poll seed AND the polled value reach the same saturated state.
        {
            let mut g = policy.state.write().await;
            g.refresh_backends(&[workers[0].url().to_string()]);
            let b = g
                .backends
                .get_mut(workers[0].url())
                .expect("seeded above");
            b.capacity_tokens = 10_000;
            b.active_program_tokens = 10_000; // saturated against polled capacity
            // Pre-create the program so usage_consumer's `programs.get_mut`
            // path is exercised against an existing entry.
            g.programs.insert(
                "blocked-prog".to_string(),
                Program::new("blocked-prog".to_string()),
            );
        }

        // Spawn the TR select in a background task — it should pause.
        let policy_for_task = policy.clone();
        let workers_for_task = workers.clone();
        #[expect(
            clippy::disallowed_methods,
            reason = "test-only fire-and-forget; awaited via JoinHandle below"
        )]
        let select_task = tokio::spawn(async move {
            let info = SelectWorkerInfo {
                program_id: Some("blocked-prog"),
                request_text: Some("x"), // 1 char → 0 prompt + 256 = 256 estimated
                ..Default::default()
            };
            policy_for_task
                .select_worker_async(&workers_for_task, &info)
                .await
        });

        // Give the task time to register the Notify and pause.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !select_task.is_finished(),
            "TR select must be paused on saturated backend"
        );
        let snap = policy.snapshot_state().await;
        assert!(
            snap.waiting_events.contains_key("blocked-prog"),
            "Notify must be registered for paused program"
        );

        // Free capacity directly + send UsageEvent to broadcast the wake.
        // (In production, capacity-free comes from real backend usage; here
        // we synthesize.)
        {
            let mut g = policy.state.write().await;
            let b = g
                .backends
                .get_mut(workers[0].url())
                .expect("seeded");
            b.active_program_tokens = 0; // freed
        }
        // Send a no-op-ish UsageEvent to trigger the broadcast.
        let tx = policy.usage_sender().expect("usage sender");
        tx.send(UsageEvent {
            program_id: Some("freer".to_string()), // distinct pid; doesn't matter
            backend_url: workers[0].url().to_string(),
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
            request_text_chars: 0,
            cache_read_input_tokens: None,
            declared_max_tokens: None,
        })
        .expect("send");

        // The blocked task should now resume + admit.
        let result = tokio::time::timeout(Duration::from_secs(5), select_task)
            .await
            .expect("must resume within 5s")
            .expect("task must not panic");
        assert_eq!(result, Some(0), "blocked program must admit on resume");
    }

    /// `ProgramRequestGuard::Drop` decrements `Program.in_flight` when the
    /// guard is not marked complete (cancel / error path).
    #[tokio::test]
    async fn program_request_guard_drop_decrements_in_flight() {
        let policy = ThunderPolicy::with_metrics_client(
            ThunderConfig::default(),
            Arc::new(StubMetrics) as Arc<dyn thunder_metrics::MetricsClient>,
        );
        let workers = mock_workers(1);
        let info = SelectWorkerInfo {
            program_id: Some("guard-drop"),
            ..Default::default()
        };
        // Admit once → in_flight = 1
        let _ = policy.select_worker_async(&workers, &info).await;
        let pre = policy.snapshot_state().await;
        assert_eq!(pre.programs["guard-drop"].in_flight, 1);

        // Drop a guard for the same program — async cleanup spawned.
        {
            let _g = policy.create_guard("guard-drop");
        } // drop here
        // Give the spawned task a moment to run.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let post = policy.snapshot_state().await;
        assert_eq!(
            post.programs["guard-drop"].in_flight, 0,
            "guard Drop must decrement in_flight"
        );
    }

    /// `ProgramRequestGuard::complete()` suppresses the Drop cleanup so the
    /// happy path (where `usage_consumer` already decremented) doesn't
    /// double-decrement.
    #[tokio::test]
    async fn program_request_guard_complete_suppresses_drop() {
        let policy = ThunderPolicy::with_metrics_client(
            ThunderConfig::default(),
            Arc::new(StubMetrics) as Arc<dyn thunder_metrics::MetricsClient>,
        );
        let workers = mock_workers(1);
        let info = SelectWorkerInfo {
            program_id: Some("guard-complete"),
            ..Default::default()
        };
        let _ = policy.select_worker_async(&workers, &info).await;
        assert_eq!(
            policy.snapshot_state().await.programs["guard-complete"].in_flight,
            1
        );
        {
            let mut g = policy.create_guard("guard-complete");
            g.complete();
        } // drop here — must NOT decrement
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            policy.snapshot_state().await.programs["guard-complete"].in_flight,
            1,
            "complete() must suppress Drop cleanup"
        );
    }

    /// Guard exposes its program_id (test-only accessor).
    #[test]
    fn program_request_guard_exposes_program_id() {
        let state = Arc::new(RwLock::new(RouterState::default()));
        let g = ProgramRequestGuard::new(state, "pid-x".to_string());
        assert_eq!(g.program_id(), "pid-x");
    }

    /// Force-admit-after-timeout fires when the deadline passes without
    /// any capacity-free signal.
    #[tokio::test]
    async fn tr_mode_force_admits_after_timeout() {
        let policy = ThunderPolicy::with_metrics_client(
            ThunderConfig {
                sub_mode: ThunderSubMode::Tr,
                capacity_reserved_fraction: 0.0,
                resume_timeout_secs: 1, // 1s timeout → fires fast
                ..Default::default()
            },
            Arc::new(StubMetrics) as Arc<dyn thunder_metrics::MetricsClient>,
        );
        let workers = mock_workers(1);
        // Saturate against polled capacity (StubMetrics returns 10_000).
        {
            let mut g = policy.state.write().await;
            g.refresh_backends(&[workers[0].url().to_string()]);
            let b = g.backends.get_mut(workers[0].url()).expect("seeded");
            b.capacity_tokens = 10_000;
            b.active_program_tokens = 10_000;
        }
        let info = SelectWorkerInfo {
            program_id: Some("force-prog"),
            request_text: Some("x"),
            ..Default::default()
        };
        let start = Instant::now();
        let result = policy.select_worker_async(&workers, &info).await;
        let elapsed = start.elapsed();
        assert_eq!(result, Some(0), "force-admit must return the only worker");
        assert!(
            elapsed >= Duration::from_millis(900),
            "force-admit must wait ≥ ~1s timeout (took {elapsed:?})"
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "force-admit should not wait significantly past timeout (took {elapsed:?})"
        );
    }

    /// Gap 1 regression: a brand-new program that hits capacity on its FIRST
    /// admission attempt must enter the Paused set with `pending_requests > 0`
    /// so `try_greedy_resume`'s 3-tier sort can prioritize it (tier 1: New).
    /// Without this, first-admission failures wake only via the broadcast
    /// `notify_waiters` on capacity-free, with no priority among concurrent
    /// newcomers.
    #[tokio::test]
    async fn tr_mode_blocked_new_program_is_in_paused_set() {
        let policy = Arc::new(ThunderPolicy::with_metrics_client(
            ThunderConfig {
                sub_mode: ThunderSubMode::Tr,
                resume_timeout_secs: 60,
                scheduler_tick_ms: 10_000, // long tick so this test owns timing
                ..Default::default()
            },
            Arc::new(StubMetrics) as Arc<dyn thunder_metrics::MetricsClient>,
        ));
        let workers = mock_workers(1);

        // Saturate against polled capacity (StubMetrics returns 10_000); the
        // poll task's first immediate tick overwrites anything lower.
        {
            let mut g = policy.state.write().await;
            g.refresh_backends(&[workers[0].url().to_string()]);
            let b = g.backends.get_mut(workers[0].url()).expect("seeded");
            b.capacity_tokens = 10_000;
            b.active_program_tokens = 10_000;
        }

        // Spawn pick_tr for a brand-new program; it must block.
        let p_clone = policy.clone();
        let w_clone = workers.clone();
        let task = tokio::spawn(async move {
            p_clone
                .select_worker_async(
                    &w_clone,
                    &SelectWorkerInfo {
                        program_id: Some("newcomer"),
                        request_text: Some("hi"),
                        ..Default::default()
                    },
                )
                .await
        });

        // Wait for pick_tr to reach the block path.
        tokio::time::sleep(Duration::from_millis(50)).await;

        {
            let snap = policy.snapshot_state().await;
            let p = snap
                .programs
                .get("newcomer")
                .expect("newcomer must be registered after blocking");
            assert_eq!(
                p.status,
                ProgramStatus::Paused,
                "blocked new program must be in Paused set for 3-tier resume"
            );
            assert_eq!(
                p.pending_requests, 1,
                "block path must increment pending_requests"
            );
            assert!(
                p.paused_at.is_some(),
                "paused_at must be set for starvation accounting"
            );
            assert_eq!(
                p.step_count, 0,
                "never admitted → step_count stays 0 → tier 1 (New) eligible"
            );
        }

        // Free capacity and trigger try_greedy_resume manually (scheduler
        // tick is set to 10s so we drive it ourselves for determinism).
        {
            let mut g = policy.state.write().await;
            let b = g.backends.get_mut(workers[0].url()).expect("seeded");
            b.active_program_tokens = 0; // freed
            g.try_greedy_resume();
        }

        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("pick_tr must complete within 2s")
            .expect("task must not panic")
            .expect("admit must succeed");
        assert_eq!(result, 0);

        // After admit, the guard's consume() must have decremented.
        let snap = policy.snapshot_state().await;
        let p = snap.programs.get("newcomer").unwrap();
        assert_eq!(
            p.pending_requests, 0,
            "PendingRequestGuard::consume() must decrement on admit"
        );
        assert_eq!(p.status, ProgramStatus::Reasoning);
        assert_eq!(p.step_count, 1, "step_count bumped by assign()");
    }

    /// Gap 2 regression: if `pick_tr`'s future is cancelled mid-await (e.g.
    /// upstream HTTP timeout, runtime cancellation), `PendingRequestGuard::Drop`
    /// must fire an async decrement so `pending_requests` doesn't leak.
    /// Without this guard, long-lived services accumulate phantom Reasoning-
    /// tier candidates on programs whose clients no longer exist, crowding
    /// out real waiters at resume time.
    #[tokio::test]
    async fn tr_mode_cancellation_releases_pending_request_counter() {
        let policy = Arc::new(ThunderPolicy::with_metrics_client(
            ThunderConfig {
                sub_mode: ThunderSubMode::Tr,
                resume_timeout_secs: 60,
                scheduler_tick_ms: 10_000,
                ..Default::default()
            },
            Arc::new(StubMetrics) as Arc<dyn thunder_metrics::MetricsClient>,
        ));
        let workers = mock_workers(1);

        // Saturate against polled capacity (StubMetrics returns 10_000).
        {
            let mut g = policy.state.write().await;
            g.refresh_backends(&[workers[0].url().to_string()]);
            let b = g.backends.get_mut(workers[0].url()).expect("seeded");
            b.capacity_tokens = 10_000;
            b.active_program_tokens = 10_000;
        }

        let p_clone = policy.clone();
        let w_clone = workers.clone();
        let task = tokio::spawn(async move {
            p_clone
                .select_worker_async(
                    &w_clone,
                    &SelectWorkerInfo {
                        program_id: Some("cancel-test"),
                        request_text: Some("hi"),
                        ..Default::default()
                    },
                )
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        {
            let snap = policy.snapshot_state().await;
            let p = snap.programs.get("cancel-test").unwrap();
            assert_eq!(p.pending_requests, 1, "counter incremented at block");
        }

        // Cancel the awaiting future. The local guard inside pick_tr is
        // dropped, which spawns an async decrement task.
        task.abort();

        // Yield generously so the spawned decrement task can acquire the
        // write lock and run.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let snap = policy.snapshot_state().await;
        let p = snap.programs.get("cancel-test").unwrap();
        assert_eq!(
            p.pending_requests, 0,
            "PendingRequestGuard::Drop must release the counter on cancellation"
        );
    }

    // ===== M5+M6 shortest-first resume + targeted notify tests =====

    /// CRITICAL paper-parity test: a program paused on backend X must be
    /// allowed to resume on backend Y if Y has the most remaining capacity.
    /// This validates that global resume relocates programs across backends,
    /// not just resumes them on the same backend (paper §8 — once paused, a
    /// program's KV is assumed evicted, so resume placement is node-agnostic).
    #[test]
    fn paused_program_resumes_on_different_backend_with_capacity() {
        let mut state = RouterState::default();
        // Backend X: where program was originally running but now over-loaded
        // by other programs (simulated: low remaining cap).
        state.backends.insert(
            "X".to_string(),
            BackendState {
                active_programs: ["other".to_string()].into_iter().collect(),
                active_program_tokens: 950, // X is nearly full from "other"
                capacity_tokens: 1000,
            },
        );
        // Backend Y: was empty, just freed up.
        state.backends.insert(
            "Y".to_string(),
            BackendState {
                active_programs: Default::default(),
                active_program_tokens: 0,
                capacity_tokens: 1000,
            },
        );

        // Program P was running on X, then got proactively paused (backend_url
        // cleared on pause; this is the post-pause state).
        let mut p = Program::new("relocate-me".to_string());
        p.status = ProgramStatus::Paused;
        p.total_tokens = 200; // resume estimate
        p.paused_at = Some(Instant::now());
        p.backend_url = None;
        state.programs.insert("relocate-me".to_string(), p);
        state
            .waiting_events
            .insert("relocate-me".to_string(), Arc::new(Notify::new()));

        // Resume should pick Y (1000 free) over X (50 free) because est=200
        // doesn't fit in X but fits in Y.
        state.try_greedy_resume();

        let resumed = state.programs.get("relocate-me").unwrap();
        assert_eq!(
            resumed.status,
            ProgramStatus::Reasoning,
            "must transition out of Paused"
        );
        assert_eq!(
            resumed.backend_url.as_deref(),
            Some("Y"),
            "MUST resume on Y (different from original X), not X"
        );
        assert_eq!(
            resumed.estimated_reserved_tokens, 200,
            "reservation transferred to new backend"
        );
        // Y's accounting reflects the new program.
        let y = state.backends.get("Y").unwrap();
        assert_eq!(y.active_program_tokens, 200, "Y now booked with P's reservation");
        assert!(
            y.active_programs.contains("relocate-me"),
            "P registered on Y"
        );
        // X's accounting unchanged (P was already removed at pause time).
        let x = state.backends.get("X").unwrap();
        assert_eq!(x.active_program_tokens, 950);
        assert!(!x.active_programs.contains("relocate-me"));
    }

    #[test]
    fn resume_assigns_shortest_program_first() {
        let mut state = RouterState::default();
        state.backends.insert(
            "A".to_string(),
            BackendState {
                active_programs: Default::default(),
                active_program_tokens: 0,
                capacity_tokens: 200,
            },
        );
        state.backends.insert(
            "B".to_string(),
            BackendState {
                active_programs: Default::default(),
                active_program_tokens: 0,
                capacity_tokens: 40,
            },
        );
        for (pid, total) in [("p_big", 150), ("p_small", 50)] {
            let mut prog = Program::new(pid.to_string());
            prog.status = ProgramStatus::Paused;
            prog.total_tokens = total;
            prog.paused_at = Some(Instant::now());
            state.programs.insert(pid.to_string(), prog);
            state
                .waiting_events
                .insert(pid.to_string(), Arc::new(Notify::new()));
        }
        state.try_greedy_resume();
        // Paper Eq 8 restore scoring is shortest-first. p_small (50) resumes
        // before p_big (150); B has too little capacity for either, so both
        // land on A.
        let p_big = state.programs.get("p_big").unwrap();
        let p_small = state.programs.get("p_small").unwrap();
        assert_eq!(p_small.status, ProgramStatus::Reasoning);
        assert_eq!(p_small.backend_url.as_deref(), Some("A"));
        assert_eq!(p_big.status, ProgramStatus::Reasoning);
        assert_eq!(p_big.backend_url.as_deref(), Some("A"));
    }

    #[test]
    fn resume_skips_program_that_doesnt_fit_anywhere() {
        let mut state = RouterState::default();
        state.backends.insert(
            "A".to_string(),
            BackendState {
                active_programs: Default::default(),
                active_program_tokens: 0,
                capacity_tokens: 100,
            },
        );
        let mut prog = Program::new("p_huge".to_string());
        prog.status = ProgramStatus::Paused;
        prog.total_tokens = 10_000; // doesn't fit in 100
        state.programs.insert("p_huge".to_string(), prog);
        state
            .waiting_events
            .insert("p_huge".to_string(), Arc::new(Notify::new()));
        state.try_greedy_resume();
        // p_huge stays Paused
        assert_eq!(
            state.programs.get("p_huge").unwrap().status,
            ProgramStatus::Paused
        );
    }

    #[test]
    fn resume_prioritizes_long_paused_program_for_starvation_mitigation() {
        // Backend has room for exactly ONE of the two paused programs. Both
        // are in the same tier; the starvation boost on p_old pulls it ahead
        // of the shortest-first ordering. p_old should resume first; p_new
        // stays paused for next tick.
        let mut state = RouterState::default();
        state.backends.insert(
            "A".to_string(),
            BackendState {
                active_programs: Default::default(),
                active_program_tokens: 0,
                capacity_tokens: 250,
            },
        );
        let mut p_old = Program::new("old".to_string());
        p_old.status = ProgramStatus::Paused;
        p_old.total_tokens = 200; // small enough to fit alone
        p_old.paused_at = Some(Instant::now() - PAUSED_PRIORITY_BOOST_AFTER - Duration::from_secs(1));
        state.programs.insert("old".to_string(), p_old);
        state
            .waiting_events
            .insert("old".to_string(), Arc::new(Notify::new()));

        let mut p_new = Program::new("new".to_string());
        p_new.status = ProgramStatus::Paused;
        p_new.total_tokens = 220; // larger but fresh — would lose priority to p_old
        p_new.paused_at = Some(Instant::now());
        state.programs.insert("new".to_string(), p_new);
        state
            .waiting_events
            .insert("new".to_string(), Arc::new(Notify::new()));

        state.try_greedy_resume();
        // p_old wins via priority boost; takes 200 of 250.
        assert_eq!(
            state.programs.get("old").unwrap().status,
            ProgramStatus::Reasoning,
            "old (priority-boosted) should resume"
        );
        // p_new (220) doesn't fit in remaining 50 → stays paused.
        assert_eq!(
            state.programs.get("new").unwrap().status,
            ProgramStatus::Paused,
            "new stays paused — old got the slot via priority"
        );
    }

    /// Paper Eq 8 / Python parity: at restore time, the scheduler ranks
    /// paused programs by τ-tier first. Reasoning (pending request,
    /// step_count > 1) beats New (step_count <= 1) beats Acting (idle
    /// between LLM calls). Within tier, shortest context wins.
    #[test]
    fn resume_orders_by_reasoning_new_acting_tier() {
        let mut state = RouterState::default();
        // Backend has just enough capacity for ONE program at a time.
        state.backends.insert(
            "A".to_string(),
            BackendState {
                active_programs: Default::default(),
                active_program_tokens: 0,
                capacity_tokens: 400,
            },
        );

        // r_long: Reasoning tier — pending request + has history. Long context;
        // sized to consume all backend capacity so the other tiers' candidates
        // are forced to wait.
        let mut r_long = Program::new("r_long".to_string());
        r_long.status = ProgramStatus::Paused;
        r_long.total_tokens = 400;
        r_long.step_count = 5;
        r_long.pending_requests = 1;
        r_long.paused_at = Some(Instant::now());
        state.programs.insert("r_long".to_string(), r_long);
        state
            .waiting_events
            .insert("r_long".to_string(), Arc::new(Notify::new()));

        // newcomer: New tier — step_count=1, no pending. Short context.
        let mut newcomer = Program::new("newcomer".to_string());
        newcomer.status = ProgramStatus::Paused;
        newcomer.total_tokens = 50;
        newcomer.step_count = 1;
        newcomer.pending_requests = 0;
        newcomer.paused_at = Some(Instant::now());
        state.programs.insert("newcomer".to_string(), newcomer);
        state
            .waiting_events
            .insert("newcomer".to_string(), Arc::new(Notify::new()));

        // a_short: Acting tier — no pending, has history. Shortest context
        // overall, but tier-3 → resumes last.
        let mut a_short = Program::new("a_short".to_string());
        a_short.status = ProgramStatus::Paused;
        a_short.total_tokens = 10;
        a_short.step_count = 5;
        a_short.pending_requests = 0;
        a_short.paused_at = Some(Instant::now());
        state.programs.insert("a_short".to_string(), a_short);
        state
            .waiting_events
            .insert("a_short".to_string(), Arc::new(Notify::new()));

        state.try_greedy_resume();

        // r_long takes the slot (Reasoning tier wins despite longer context).
        // newcomer and a_short stay Paused — capacity exhausted after r_long.
        assert_eq!(
            state.programs.get("r_long").unwrap().status,
            ProgramStatus::Reasoning,
            "Reasoning tier (pending request) resumes first regardless of context length"
        );
        assert_eq!(
            state.programs.get("newcomer").unwrap().status,
            ProgramStatus::Paused,
            "New tier waits"
        );
        assert_eq!(
            state.programs.get("a_short").unwrap().status,
            ProgramStatus::Paused,
            "Acting tier (no pending) waits even when it has the shortest context"
        );
    }

    /// Within the Reasoning tier, shortest context wins (paper Eq 8 `1/c_P`).
    #[test]
    fn resume_within_reasoning_tier_picks_shortest_context() {
        let mut state = RouterState::default();
        state.backends.insert(
            "A".to_string(),
            BackendState {
                active_programs: Default::default(),
                active_program_tokens: 0,
                capacity_tokens: 100,
            },
        );

        for (pid, tokens) in [("r_big", 80), ("r_small", 30)] {
            let mut prog = Program::new(pid.to_string());
            prog.status = ProgramStatus::Paused;
            prog.total_tokens = tokens;
            prog.step_count = 5;
            prog.pending_requests = 1;
            prog.paused_at = Some(Instant::now());
            state.programs.insert(pid.to_string(), prog);
            state
                .waiting_events
                .insert(pid.to_string(), Arc::new(Notify::new()));
        }

        state.try_greedy_resume();
        // Only one fits at a time. r_small (30) wins on shortest-first.
        assert_eq!(
            state.programs.get("r_small").unwrap().status,
            ProgramStatus::Reasoning,
            "shortest in Reasoning tier resumes first"
        );
        // r_big (80) doesn't fit in remaining 70 → stays paused.
        assert_eq!(
            state.programs.get("r_big").unwrap().status,
            ProgramStatus::Paused
        );
    }

    #[tokio::test]
    async fn wake_program_to_uses_targeted_notify_one() {
        // Validate that wake_program_to fires the program's specific Notify
        // (M6 targeted wake, not broadcast). Use a Notify and observe via
        // tokio::sync::Notify::notified() future polling.
        let mut state = RouterState::default();
        state.backends.insert(
            "A".to_string(),
            BackendState {
                active_programs: Default::default(),
                active_program_tokens: 0,
                capacity_tokens: 200,
            },
        );
        let notify = Arc::new(Notify::new());
        state
            .waiting_events
            .insert("p1".to_string(), notify.clone());
        let mut prog = Program::new("p1".to_string());
        prog.status = ProgramStatus::Paused;
        state.programs.insert("p1".to_string(), prog);

        // Spawn a waiter that registers AFTER state setup.
        let n_clone = notify.clone();
        #[expect(
            clippy::disallowed_methods,
            reason = "test harness; the waiter's lifetime is bounded by tokio::time::timeout"
        )]
        let waiter = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(200), n_clone.notified())
                .await
                .is_ok()
        });
        // Yield to let the waiter register before wake.
        tokio::time::sleep(Duration::from_millis(20)).await;

        state.wake_program_to("p1", "A", 50);

        let woken = waiter.await.unwrap();
        assert!(woken, "wake_program_to must fire the targeted notify_one");
    }

    // ===== M4 proactive pause tests =====

    #[test]
    fn pick_victim_returns_shortest_context_with_acting_priority() {
        // Paper Eq 9: S_pause = 1/c_P + 𝕀(τ = Acting).
        // Acting first (idle KV), then shortest context.
        let mut state = RouterState::default();
        let url = "http://b1:8000".to_string();
        for (pid, tokens, status) in [
            ("a", 500, ProgramStatus::Reasoning),
            ("b", 80, ProgramStatus::Reasoning),
            ("c", 20, ProgramStatus::Acting),
        ] {
            state.programs.insert(
                pid.to_string(),
                Program {
                    program_id: pid.to_string(),
                    backend_url: Some(url.clone()),
                    in_flight: 0,
                    pending_requests: 0,
                    total_tokens: tokens,
                    accounted_tokens: 0,
                    step_count: 1,
                    estimated_reserved_tokens: 0,
                    local_char_to_token_ratio: None,
                    local_completion_fraction: None,
                    last_calibration_at: None,
                    status,
                    marked_for_pause: false,
                    paused_at: None,
                },
            );
        }
        // c is Acting with shortest context — wins the score outright.
        assert_eq!(state.pick_victim(&url), Some("c".to_string()));
    }

    #[test]
    fn pick_victim_excludes_paused_and_marked() {
        let mut state = RouterState::default();
        let url = "http://b1:8000".to_string();
        for (pid, tokens, st, mark) in [
            ("a", 500, ProgramStatus::Reasoning, false),
            ("b", 2, ProgramStatus::Paused, false),   // paused → excluded
            ("c", 1, ProgramStatus::Reasoning, true), // marked → excluded
            ("d", 30, ProgramStatus::Reasoning, false),
        ] {
            state.programs.insert(
                pid.to_string(),
                Program {
                    program_id: pid.to_string(),
                    backend_url: Some(url.clone()),
                    in_flight: 1,
                    pending_requests: 0,
                    total_tokens: tokens,
                    accounted_tokens: 0,
                    step_count: 1,
                    estimated_reserved_tokens: 0,
                    local_char_to_token_ratio: None,
                    local_completion_fraction: None,
                    last_calibration_at: None,
                    status: st,
                    marked_for_pause: mark,
                    paused_at: None,
                },
            );
        }
        // d is the only Reasoning candidate not excluded; shortest context wins.
        assert_eq!(state.pick_victim(&url), Some("d".to_string()));
    }

    #[test]
    fn pause_until_safe_immediate_when_idle() {
        // Reasoning + in_flight=0 (e.g. between requests) → safe to pause immediately.
        let mut state = RouterState::default();
        let url = "http://b1:8000".to_string();
        state.backends.insert(
            url.clone(),
            BackendState {
                active_programs: ["v".to_string()].into_iter().collect(),
                active_program_tokens: 500,
                capacity_tokens: 1000,
            },
        );
        state.programs.insert(
            "v".to_string(),
            Program {
                program_id: "v".to_string(),
                backend_url: Some(url.clone()),
                in_flight: 0, // no request currently running
                pending_requests: 0,
                total_tokens: 0,
                accounted_tokens: 0,
                step_count: 1,
                estimated_reserved_tokens: 500,
                local_char_to_token_ratio: None,
                local_completion_fraction: None,
                last_calibration_at: None,
                status: ProgramStatus::Idle,
                marked_for_pause: false,
                paused_at: None,
            },
        );
        state.pause_until_safe("v", &url);
        let p = state.programs.get("v").unwrap();
        assert_eq!(p.status, ProgramStatus::Paused);
        assert_eq!(p.estimated_reserved_tokens, 0);
        assert!(p.paused_at.is_some());
        assert_eq!(p.backend_url, None);
        let b = state.backends.get(&url).unwrap();
        assert_eq!(b.active_program_tokens, 0);
        assert!(!b.active_programs.contains("v"));
    }

    #[test]
    fn pause_until_safe_defers_in_flight_request() {
        // Concurrency safety: any program with in_flight > 0 must defer
        // pause via marked_for_pause, never immediately un-reserve. Otherwise
        // an actively-streaming request would have its capacity accounting
        // cleared while bytes are still flowing.
        let mut state = RouterState::default();
        let url = "http://b1:8000".to_string();
        state.backends.insert(
            url.clone(),
            BackendState {
                active_programs: ["w".to_string()].into_iter().collect(),
                active_program_tokens: 500,
                capacity_tokens: 1000,
            },
        );
        state.programs.insert(
            "w".to_string(),
            Program {
                program_id: "w".to_string(),
                backend_url: Some(url.clone()),
                in_flight: 1, // request actively running
                pending_requests: 0,
                total_tokens: 0,
                accounted_tokens: 0,
                step_count: 1,
                estimated_reserved_tokens: 500,
                local_char_to_token_ratio: None,
                local_completion_fraction: None,
                last_calibration_at: None,
                status: ProgramStatus::Reasoning,
                marked_for_pause: false,
                paused_at: None,
            },
        );
        state.pause_until_safe("w", &url);
        let p = state.programs.get("w").unwrap();
        assert_eq!(
            p.status,
            ProgramStatus::Reasoning,
            "must NOT immediately pause — still in-flight"
        );
        assert!(p.marked_for_pause, "must defer via marked_for_pause");
        assert_eq!(
            p.estimated_reserved_tokens, 500,
            "reservation must NOT be cleared while request still running"
        );
        let b = state.backends.get(&url).unwrap();
        assert_eq!(
            b.active_program_tokens, 500,
            "backend accounting must stay intact while in-flight"
        );
    }

    #[test]
    fn pause_until_safe_pauses_off_gpu_acting_program() {
        // Acting + in_flight=0: program is between LLM calls; its retained
        // KV is idle. The scheduler can pause it immediately (paper Eq 9:
        // Acting programs are the preferred pause victims).
        let mut state = RouterState::default();
        let url = "http://b1:8000".to_string();
        state.backends.insert(
            url.clone(),
            BackendState {
                active_programs: ["a".to_string()].into_iter().collect(),
                active_program_tokens: 500,
                capacity_tokens: 1000,
            },
        );
        state.programs.insert(
            "a".to_string(),
            Program {
                program_id: "a".to_string(),
                backend_url: Some(url.clone()),
                in_flight: 0,
                pending_requests: 0,
                total_tokens: 500,
                accounted_tokens: 500,
                step_count: 1,
                estimated_reserved_tokens: 0,
                local_char_to_token_ratio: None,
                local_completion_fraction: None,
                last_calibration_at: None,
                status: ProgramStatus::Acting,
                marked_for_pause: false,
                paused_at: None,
            },
        );
        state.pause_until_safe("a", &url);
        let p = state.programs.get("a").unwrap();
        assert_eq!(p.status, ProgramStatus::Paused);
        assert_eq!(p.accounted_tokens, 0, "accounted unbooked on pause");
        let b = state.backends.get(&url).unwrap();
        assert_eq!(
            b.active_program_tokens, 0,
            "backend footprint released on pause"
        );
    }

    #[test]
    fn proactive_pause_pass_evicts_until_under_threshold() {
        let mut state = RouterState::default();
        let url = "http://b1:8000".to_string();
        state.backends.insert(
            url.clone(),
            BackendState {
                active_programs: ["a", "b", "c"].into_iter().map(String::from).collect(),
                active_program_tokens: 950,
                capacity_tokens: 1000, // threshold @ 0.10 reserved = 900; active=950 > 900 → pause needed
            },
        );
        for (pid, tokens) in [("a", 500), ("b", 40), ("c", 410)] {
            state.programs.insert(
                pid.to_string(),
                Program {
                    program_id: pid.to_string(),
                    backend_url: Some(url.clone()),
                    in_flight: 0, // idle programs (between requests) — eligible for immediate pause
                    pending_requests: 0,
                    total_tokens: tokens,
                    accounted_tokens: 0,
                    step_count: 1,
                    estimated_reserved_tokens: tokens,
                    local_char_to_token_ratio: None,
                    local_completion_fraction: None,
                    last_calibration_at: None,
                    status: ProgramStatus::Idle,
                    marked_for_pause: false,
                    paused_at: None,
                },
            );
        }
        // active=950 > threshold=900 → pause victims until ≤ 900
        state.proactive_pause_pass(0.10);
        let b = state.backends.get(&url).unwrap();
        assert!(
            b.active_program_tokens <= 900,
            "post-pause should be ≤ threshold; got {}",
            b.active_program_tokens
        );
        // b (shortest context = 40) should have been paused first.
        assert_eq!(
            state.programs.get("b").unwrap().status,
            ProgramStatus::Paused
        );
    }

    #[test]
    fn check_marked_for_pause_takes_deferred_pause_when_no_longer_acting() {
        let mut state = RouterState::default();
        let url = "http://b1:8000".to_string();
        state.backends.insert(
            url.clone(),
            BackendState {
                active_programs: ["m".to_string()].into_iter().collect(),
                active_program_tokens: 500,
                capacity_tokens: 1000,
            },
        );
        state.programs.insert(
            "m".to_string(),
            Program {
                program_id: "m".to_string(),
                backend_url: Some(url.clone()),
                in_flight: 0,
                pending_requests: 0,
                total_tokens: 0,
                accounted_tokens: 0,
                step_count: 1,
                estimated_reserved_tokens: 500,
                local_char_to_token_ratio: None,
                local_completion_fraction: None,
                last_calibration_at: None,
                status: ProgramStatus::Idle, // no longer Acting
                marked_for_pause: true,
                paused_at: None,
            },
        );
        state.check_marked_for_pause("m");
        assert_eq!(state.programs.get("m").unwrap().status, ProgramStatus::Paused);
        assert!(!state.programs.get("m").unwrap().marked_for_pause);
    }

    // ===== M3 calibration tests =====

    #[test]
    fn calibration_first_observation_initializes_directly() {
        let mut stored: Option<f64> = None;
        let mut last_at: Option<Instant> = None;
        let now = Instant::now();
        update_calibration_with_decay(&mut stored, &mut last_at, 5.5, NEUTRAL_RATIO, now);
        assert_eq!(stored, Some(5.5), "first observation = direct assign");
        assert!(last_at.is_some());
    }

    #[test]
    fn calibration_ema_no_time_elapsed() {
        let mut stored: Option<f64> = Some(4.0);
        let mut last_at: Option<Instant> = Some(Instant::now());
        let now = last_at.unwrap();
        update_calibration_with_decay(&mut stored, &mut last_at, 5.0, NEUTRAL_RATIO, now);
        // 0.2 * 5.0 + 0.8 * 4.0 = 4.2 (no decay since elapsed=0 → retain=1)
        let v = stored.unwrap();
        assert!((v - 4.2).abs() < 1e-6, "EMA without decay: got {v}");
    }

    #[test]
    fn calibration_decay_with_one_half_life() {
        let mut stored: Option<f64> = Some(8.0);
        let t0 = Instant::now();
        let mut last_at: Option<Instant> = Some(t0);
        let now = t0 + CALIBRATION_HALF_LIFE; // exactly one half-life
        update_calibration_with_decay(&mut stored, &mut last_at, 4.0, NEUTRAL_RATIO, now);
        // retain ≈ 0.5 → decayed = 0.5*8 + 0.5*4 = 6
        // EMA: 0.2*4 + 0.8*6 = 5.6
        let v = stored.unwrap();
        assert!((v - 5.6).abs() < 1e-2, "decay+EMA at one half-life: got {v}");
    }

    #[tokio::test]
    async fn estimate_uses_per_program_ratio_when_present()  {
        let policy = ThunderPolicy::with_defaults();
        let mut state = RouterState::default();
        state.programs.insert(
            "p1".to_string(),
            Program {
                program_id: "p1".to_string(),
                local_char_to_token_ratio: Some(2.0), // half of neutral
                ..Program::new("p1".to_string())
            },
        );
        let text_for_test = "a".repeat(80);
        let info = SelectWorkerInfo {
            request_text: Some(text_for_test.as_str()),
            program_id: Some("p1"),
            ..Default::default()
        };
        let est = policy.estimate_request_tokens(&info, &state);
        // 80 chars / 2.0 ratio = 40 prompt + 256 fallback completion = 296
        assert_eq!(est, 296);
    }

    #[tokio::test]
    async fn estimate_falls_through_to_global_when_program_has_no_local()  {
        let policy = ThunderPolicy::with_defaults();
        let mut state = RouterState {
            global_char_to_token_ratio: Some(8.0),
            ..Default::default()
        };
        state.programs.insert(
            "p1".to_string(),
            Program::new("p1".to_string()), // no local ratio
        );
        let text_for_test = "a".repeat(80);
        let info = SelectWorkerInfo {
            request_text: Some(text_for_test.as_str()),
            program_id: Some("p1"),
            ..Default::default()
        };
        let est = policy.estimate_request_tokens(&info, &state);
        // 80 / 8 = 10 prompt + 256 = 266
        assert_eq!(est, 266);
    }

    #[tokio::test]
    async fn estimate_falls_through_to_neutral_when_no_calibration()  {
        let policy = ThunderPolicy::with_defaults();
        let state = RouterState::default();
        let text_for_test = "a".repeat(80);
        let info = SelectWorkerInfo {
            request_text: Some(text_for_test.as_str()),
            ..Default::default()
        };
        let est = policy.estimate_request_tokens(&info, &state);
        // 80 / 4.0 (neutral) = 20 prompt + 256 = 276
        assert_eq!(est, 276);
    }

    #[tokio::test]
    async fn estimate_uses_max_tokens_with_completion_fraction()  {
        let policy = ThunderPolicy::with_defaults();
        let state = RouterState {
            global_completion_fraction: Some(0.3),
            ..Default::default()
        };
        let text_for_test = "a".repeat(80);
        let info = SelectWorkerInfo {
            request_text: Some(text_for_test.as_str()),
            declared_max_tokens: Some(1000),
            ..Default::default()
        };
        let est = policy.estimate_request_tokens(&info, &state);
        // 80/4=20 prompt + 1000*0.3=300 completion = 320
        assert_eq!(est, 320);
    }

    #[tokio::test]
    async fn estimate_completion_falls_back_to_256_when_max_tokens_missing()  {
        let policy = ThunderPolicy::with_defaults();
        let state = RouterState::default();
        let text_for_test = "a".repeat(40);
        let info = SelectWorkerInfo {
            request_text: Some(text_for_test.as_str()),
            declared_max_tokens: None,
            ..Default::default()
        };
        let est = policy.estimate_request_tokens(&info, &state);
        // 40/4=10 + 256 fallback = 266
        assert_eq!(est, 266);
    }

    /// M8: Anthropic prompt-cache hits must be excluded from prefill ratio.
    /// chars / (input - cache_read) — not chars / input — so cache hits don't
    /// pollute the chars-per-token estimate.
    #[tokio::test]
    async fn calibration_excludes_anthropic_cache_read_input_tokens() {
        let policy = ThunderPolicy::with_metrics_client(
            ThunderConfig::default(),
            Arc::new(StubMetrics) as Arc<dyn thunder_metrics::MetricsClient>,
        );
        let workers = mock_workers(1);
        let info = SelectWorkerInfo {
            program_id: Some("ant-cache"),
            ..Default::default()
        };
        let _ = policy.select_worker_async(&workers, &info).await;
        let tx = policy.usage_sender().expect("sender");
        // Anthropic example: 300 input_tokens of which 250 are cache_read.
        // Actual prefill = 50 fresh tokens. Request text was 200 chars.
        // Ratio should be 200 / 50 = 4.0 (not 200 / 300 = 0.667).
        tx.send(UsageEvent {
            program_id: Some("ant-cache".to_string()),
            backend_url: workers[0].url().to_string(),
            prompt_tokens: 300,
            completion_tokens: 20,
            total_tokens: 320,
            request_text_chars: 200,
            cache_read_input_tokens: Some(250),
            declared_max_tokens: None,
        })
        .expect("send");

        tokio::time::sleep(Duration::from_millis(50)).await;
        let snap = policy.snapshot_state().await;
        let p = snap.programs.get("ant-cache").expect("program");
        let ratio = p.local_char_to_token_ratio.unwrap();
        assert!(
            (ratio - 4.0).abs() < 1e-6,
            "ratio must use actual_prefill (input - cache_read), got {ratio}"
        );
    }

    /// Calibration must update on UsageEvent reaching the consumer.
    #[tokio::test]
    async fn calibration_updates_on_usage_event() {
        let policy = ThunderPolicy::with_metrics_client(
            ThunderConfig::default(),
            Arc::new(StubMetrics) as Arc<dyn thunder_metrics::MetricsClient>,
        );
        let workers = mock_workers(1);
        let info = SelectWorkerInfo {
            program_id: Some("calibrate"),
            ..Default::default()
        };
        let _ = policy.select_worker_async(&workers, &info).await;
        let tx = policy.usage_sender().expect("sender");
        tx.send(UsageEvent {
            program_id: Some("calibrate".to_string()),
            backend_url: workers[0].url().to_string(),
            prompt_tokens: 50,
            completion_tokens: 20,
            total_tokens: 70,
            request_text_chars: 200,
            cache_read_input_tokens: None,
            declared_max_tokens: Some(100),
        })
        .expect("send");

        tokio::time::sleep(Duration::from_millis(50)).await;
        let snap = policy.snapshot_state().await;
        let p = snap.programs.get("calibrate").expect("program");
        assert_eq!(
            p.local_char_to_token_ratio,
            Some(200.0 / 50.0),
            "first observation: chars/prompt = 4.0"
        );
        assert_eq!(
            p.local_completion_fraction,
            Some(0.2),
            "first observation: 20/100 = 0.2"
        );
        assert_eq!(snap.global_char_to_token_ratio, Some(4.0));
        assert_eq!(snap.global_completion_fraction, Some(0.2));
    }

    /// M2: progress_consumer_task drains StreamingProgressEvent and updates
    /// Program.total_tokens incrementally (Python parity for
    /// update_program_tokens_streaming).
    #[tokio::test]
    async fn streaming_progress_increments_program_total_tokens() {
        let policy = ThunderPolicy::with_metrics_client(
            ThunderConfig::default(),
            Arc::new(StubMetrics) as Arc<dyn thunder_metrics::MetricsClient>,
        );
        let workers = mock_workers(1);
        let info = SelectWorkerInfo {
            program_id: Some("prog-stream"),
            ..Default::default()
        };
        let _ = policy.select_worker_async(&workers, &info).await;

        let tx = policy
            .streaming_progress_sender()
            .expect("ThunderPolicy must expose a streaming progress sender");
        tx.send(StreamingProgressEvent {
            program_id: "prog-stream".to_string(),
            delta_tokens: 20,
        })
        .expect("send must succeed (consumer alive)");
        tx.send(StreamingProgressEvent {
            program_id: "prog-stream".to_string(),
            delta_tokens: 20,
        })
        .expect("send must succeed");

        tokio::time::sleep(Duration::from_millis(50)).await;
        let state = policy.snapshot_state().await;
        let prog = state
            .programs
            .get("prog-stream")
            .expect("program present after select");
        assert_eq!(
            prog.total_tokens, 40,
            "two progress events of 20 each must accumulate"
        );
    }

    /// M2: progress events for unknown programs must not panic (defensive).
    #[tokio::test]
    async fn streaming_progress_for_missing_program_is_no_op() {
        let policy = ThunderPolicy::with_metrics_client(
            ThunderConfig::default(),
            Arc::new(StubMetrics) as Arc<dyn thunder_metrics::MetricsClient>,
        );
        let tx = policy.streaming_progress_sender().expect("sender");
        tx.send(StreamingProgressEvent {
            program_id: "non-existent".to_string(),
            delta_tokens: 100,
        })
        .expect("send");
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Just survive without panic.
    }

    /// M1: Drop fallback must un-reserve estimated_reserved_tokens from
    /// backend.active_program_tokens. Without this, every client disconnect
    /// leaks reservation and TR mode eventually thinks all backends are full.
    #[tokio::test]
    async fn drop_unreserves_estimated_tokens() {
        let policy = ThunderPolicy::with_defaults();
        let backend_url = "http://b1:8000".to_string();

        {
            let mut state = policy.state.write().await;
            state.backends.insert(backend_url.clone(), BackendState {
                active_programs: ["pid-leak".to_string()].into_iter().collect(),
                active_program_tokens: 500,
                capacity_tokens: 1000,
            });
            state.programs.insert("pid-leak".to_string(), Program {
                program_id: "pid-leak".to_string(),
                backend_url: Some(backend_url.clone()),
                in_flight: 1,
                pending_requests: 0,
                total_tokens: 0,
                accounted_tokens: 0,
                step_count: 1,
                estimated_reserved_tokens: 500,
                local_char_to_token_ratio: None,
                local_completion_fraction: None,
                last_calibration_at: None,
                status: ProgramStatus::Idle,
                marked_for_pause: false,
                paused_at: None,
            });
        }

        {
            let _guard = policy.create_guard("pid-leak");
        }

        for _ in 0..50 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
            let state = policy.state.read().await;
            let b = state.backends.get(&backend_url).unwrap();
            if b.active_program_tokens == 0 {
                let p = state.programs.get("pid-leak").unwrap();
                assert_eq!(p.estimated_reserved_tokens, 0, "reservation cleared");
                assert_eq!(p.in_flight, 0, "in_flight decremented");
                return;
            }
        }
        panic!("Drop fallback never un-reserved tokens (capacity leak persists)");
    }

    /// M1: complete() must suppress Drop's un-reserve so usage_consumer's
    /// cleanup is the sole authority on the happy path.
    #[tokio::test]
    async fn complete_suppresses_drop_unreserve() {
        let policy = ThunderPolicy::with_defaults();
        let backend_url = "http://b1:8000".to_string();
        {
            let mut state = policy.state.write().await;
            state.backends.insert(backend_url.clone(), BackendState {
                active_programs: ["pid-c".to_string()].into_iter().collect(),
                active_program_tokens: 500,
                capacity_tokens: 1000,
            });
            state.programs.insert("pid-c".to_string(), Program {
                program_id: "pid-c".to_string(),
                backend_url: Some(backend_url.clone()),
                in_flight: 1,
                pending_requests: 0,
                total_tokens: 0,
                accounted_tokens: 0,
                step_count: 1,
                estimated_reserved_tokens: 500,
                local_char_to_token_ratio: None,
                local_completion_fraction: None,
                last_calibration_at: None,
                status: ProgramStatus::Idle,
                marked_for_pause: false,
                paused_at: None,
            });
        }

        {
            let mut g = policy.create_guard("pid-c");
            g.complete();
        }

        for _ in 0..20 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let state = policy.state.read().await;
        let b = state.backends.get(&backend_url).unwrap();
        assert_eq!(
            b.active_program_tokens, 500,
            "complete() must suppress Drop's un-reserve"
        );
        let p = state.programs.get("pid-c").unwrap();
        assert_eq!(p.estimated_reserved_tokens, 500, "reserved untouched");
        assert_eq!(p.in_flight, 1, "in_flight untouched");
    }

    /// M1: Drop on a missing program must not panic (defensive).
    #[tokio::test]
    async fn drop_with_no_program_does_not_panic() {
        let policy = ThunderPolicy::with_defaults();
        {
            let _g = policy.create_guard("pid-missing");
        }
        for _ in 0..20 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// M1: saturating_sub clamps when reserved > backend's current balance
    /// (defensive against any prior accounting drift).
    #[tokio::test]
    async fn drop_saturates_when_reserved_exceeds_backend_balance() {
        let policy = ThunderPolicy::with_defaults();
        let backend_url = "http://b1:8000".to_string();
        {
            let mut state = policy.state.write().await;
            state.backends.insert(backend_url.clone(), BackendState {
                active_programs: ["pid-sat".to_string()].into_iter().collect(),
                active_program_tokens: 100,
                capacity_tokens: 1000,
            });
            state.programs.insert("pid-sat".to_string(), Program {
                program_id: "pid-sat".to_string(),
                backend_url: Some(backend_url.clone()),
                in_flight: 1,
                pending_requests: 0,
                total_tokens: 0,
                accounted_tokens: 0,
                step_count: 1,
                estimated_reserved_tokens: 500,
                local_char_to_token_ratio: None,
                local_completion_fraction: None,
                last_calibration_at: None,
                status: ProgramStatus::Idle,
                marked_for_pause: false,
                paused_at: None,
            });
        }
        {
            let _g = policy.create_guard("pid-sat");
        }
        for _ in 0..50 {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
            let state = policy.state.read().await;
            let b = state.backends.get(&backend_url).unwrap();
            if b.active_program_tokens == 0 {
                return;
            }
        }
        panic!("active_program_tokens did not saturate to 0");
    }
}
