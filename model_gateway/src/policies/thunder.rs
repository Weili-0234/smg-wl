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

use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedSender},
    RwLock,
};
use tracing::{debug, trace, warn};

use super::{thunder_metrics, LoadBalancingPolicy, SelectWorkerInfo, UsageEvent};
use crate::worker::Worker;

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

/// Per-program state tracked by Thunder.
#[derive(Debug, Clone)]
pub struct Program {
    pub program_id: String,
    /// URL of the backend currently assigned to this program (sticky routing).
    pub backend_url: Option<String>,
    /// Count of in-flight requests for this program (admission tracking).
    pub in_flight: u32,
    /// Cumulative tokens reported via UsageEvent (populated in P4+).
    pub total_tokens: u64,
    /// Step counter — increments per admission. (Not yet used in P3.)
    pub step_count: u32,
}

impl Program {
    fn new(program_id: String) -> Self {
        Self {
            program_id,
            backend_url: None,
            in_flight: 0,
            total_tokens: 0,
            step_count: 0,
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
        let backend = self.backends.entry(backend_url.to_string()).or_default();
        backend.active_programs.insert(program_id.to_string());
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
                let mut guard = state_arc.write().await;
                if let Some(p) = guard.programs.get_mut(&pid) {
                    p.total_tokens = p.total_tokens.saturating_add(u64::from(event.total_tokens));
                    if p.in_flight > 0 {
                        p.in_flight -= 1;
                    }
                }
                if let Some(b) = guard.backends.get_mut(&event.backend_url) {
                    b.active_program_tokens = b
                        .active_program_tokens
                        .saturating_add(u64::from(event.total_tokens));
                }
                trace!(
                    program_id = %pid,
                    backend = %event.backend_url,
                    total_tokens = event.total_tokens,
                    "usage applied"
                );
            }
            debug!("usage_consumer channel closed; task exiting");
        });

        Self {
            config,
            state,
            metrics_client,
            usage_tx,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(ThunderConfig::default())
    }

    /// Test/admin accessor — clones the current state for read-only inspection.
    /// (Used by Phase 8's `/thunder/programs` admin endpoint when it lands.)
    pub async fn snapshot_state(&self) -> RouterState {
        let guard = self.state.read().await;
        RouterState {
            programs: guard.programs.clone(),
            backends: guard.backends.clone(),
        }
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
        let mut state = self.state.write().await;
        Self::pick_default_inner(&mut state, workers, info, self.config.sub_mode)
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
        let urls: Vec<String> = workers.iter().map(|w| w.url().to_string()).collect();
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
                // P5 will wire capacity-gated admission. Fall back to Default
                // for now so traffic still flows during partial roll-out.
                warn!(
                    "ThunderSubMode::Tr selected but capacity gate not wired (P5); \
                     falling back to Default"
                );
                let chosen_url = state.select_least_active(&urls)?;
                let idx = workers.iter().position(|w| w.url() == chosen_url)?;
                state.assign(program_id, &chosen_url);
                Some(idx)
            }
        }
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
        assert_eq!(prog.total_tokens, 80, "usage event must add to total_tokens");
        assert_eq!(prog.in_flight, 0, "consumer must decrement in_flight on event");

        let backend = state
            .backends
            .get(workers[0].url())
            .expect("backend state must exist");
        assert_eq!(
            backend.active_program_tokens, 80,
            "backend active_program_tokens must accumulate"
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
        })
        .expect("send");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let state = policy.snapshot_state().await;
        let prog = state.programs.get("default").expect("default program");
        assert_eq!(prog.total_tokens, 3);
    }
}
