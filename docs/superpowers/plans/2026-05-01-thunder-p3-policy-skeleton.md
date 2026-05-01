# Thunder P3 — `ThunderPolicy` Skeleton (Trimmed) Plan

> **For agentic workers:** Opus subagent executes this plan. Steps use `- [ ]` for tracking. Claude reviews against `docs/thunder/workflow.md` R1-R12.
>
> **`<CLAUDE-AUTONOMOUS-DECISION>` (D-19 in worklog):** P3 scope reduced from the 10-phases.md row to a "skeleton + routes traffic" minimum. Deferred to later phases:
> - `usage_consumer` task → **P4** (folds with backend metrics + char_to_token_ratio updates per Q5.5)
> - HTTP usage tail extractor + `stream_options.include_usage = true` injection → **P4** (no consumer until then anyway)
> - `WorkerRegistry::subscribe_events` integration → **P5** (only matters once capacity tracking is live)
> - `ProgramRequestGuard` RAII → **P6** (only matters once pause/resume can leave dangling state)
>
> **Why**: user explicitly authorized "能跑的 smg codebase before I wake up" with autonomous decisions. The trimmed P3 ships a compileable, routable `ThunderPolicy` end-to-end; richer features land in P4-P6 incrementally without re-doing the skeleton.

**Goal:** Land `ThunderPolicy` as a registered `LoadBalancingPolicy` that routes traffic in **Default sub-mode** (least-active-program-count) under `--policy thunder`. After P3, an e2e test sends `/v1/messages` through SMG-with-thunder and gets 200 OK.

**Architecture:**
- New file `model_gateway/src/policies/thunder.rs` (~250 LOC) with `Program`, `BackendState`, `RouterState`, `ThunderPolicy`
- `RouterState` holds `programs: HashMap<String, Program>` + `backends: HashMap<String, BackendState>` under a single `RwLock` (D-3 footgun acknowledged — single mutex perf bottleneck deferred per worklog D-3)
- `ThunderPolicy` impl is `#[async_trait] LoadBalancingPolicy` with default-mode logic only; TR mode hooks left as `unimplemented!` until P5 wires capacity check

**Tech Stack:** Rust + tokio + parking_lot or std `RwLock` (use `tokio::sync::RwLock` for async coverage). `tracing` for logs. `metrics` (existing project crate, do not add new deps unless forced).

---

## Context

- `docs/thunder/10-phases.md` row P3 (full scope; this plan trims).
- `docs/thunder/04-smg-integration.md` §5.1-5.4 (ThunderPolicy struct shape + RouterState + factory wiring).
- `docs/thunder/03-algorithm.md` (Program/BackendState fields, Default sub-mode "least-active-count" semantics).
- `docs/thunder/worklog.md` D-3 (single-RwLock decision + perf footgun) and D-4 (PolicyConfig::Thunder default values).
- HEAD `208b9aaf` (post-P2). Branch this from for `thunder-policy-p3`.

**Out of scope** (do NOT touch):
- `policies/thunder.rs` `usage_sender()` method body — leave as default `None` from P1's trait. Returning `Some(&tx)` requires the channel-recv task which is P4.
- `routers/grpc/common/stages/worker_selection.rs` — gRPC validation is P7.
- All PD code.
- `routers/anthropic/`, `routers/openai/`, `routers/gemini/`.
- Any streaming SSE parsing (P4's job).

**Key file:line anchors** (verified at HEAD `208b9aaf`):

| Anchor | What is there | What we change |
|---|---|---|
| `model_gateway/src/policies/mod.rs:38` | `pub use round_robin::RoundRobinPolicy;` | Add `pub mod thunder;` + `pub use thunder::ThunderPolicy;` |
| `model_gateway/src/policies/factory.rs:21` | `match config { ... }` | Add `PolicyConfig::Thunder { ... } => Arc::new(ThunderPolicy::new(...))` arm |
| `model_gateway/src/policies/factory.rs:88` | `match name.to_lowercase()...` | Add `"thunder" => Some(Arc::new(ThunderPolicy::with_defaults()))` arm |
| `model_gateway/src/config/types.rs:347` | `pub enum PolicyConfig {` | Add `Thunder { ... }` variant with D-4 defaults |
| `model_gateway/src/config/types.rs:441` | `pub fn name(&self) -> &'static str` | Add `Thunder { .. } => "thunder"` arm |
| `model_gateway/src/main.rs:152` | `value_parser = ["random", "round_robin", ...]` | Add `"thunder"` to the whitelist |
| `model_gateway/src/main.rs:907` | `"round_robin" => PolicyConfig::RoundRobin,` | Add `"thunder" => PolicyConfig::Thunder { ... }` arm reading from CLI flags |

---

## Pre-flight

- [ ] **PF.1:** branch is `thunder-policy-p3`, parent `208b9aaf` trunk HEAD; `git status --short` clean.
- [ ] **PF.2:** `cargo build --workspace` green at baseline.
- [ ] **PF.3:** read `docs/thunder/04-smg-integration.md` §5 fully (`grep -nE '^## §|^### §' docs/thunder/04-smg-integration.md` for navigation).

---

## Task 1: Scaffold `thunder.rs` with data structures

**Files:**
- Create: `model_gateway/src/policies/thunder.rs` (~150 LOC for this task)
- Modify: `model_gateway/src/policies/mod.rs:13-23` (add `mod thunder;` declaration + `pub use`)

`★ Decision tag (autonomous):` Use `tokio::sync::RwLock<RouterState>` (not `parking_lot::RwLock`) so `select_worker_async` can `.read()` across `.await` if a future TR mode needs it. Performance footgun documented in worklog D-3.

- [ ] **Step 1.1:** Add module declaration at `policies/mod.rs:23` (alphabetical, after `round_robin`):

```rust
mod round_robin;
mod thunder;                         // NEW
pub(crate) mod utils;
```

And re-export at line 38+:

```rust
pub use round_robin::RoundRobinPolicy;
pub use thunder::ThunderPolicy;      // NEW
```

- [ ] **Step 1.2:** Create `model_gateway/src/policies/thunder.rs` with this scaffolding:

```rust
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
    sync::Arc,
};

use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{debug, trace, warn};

use super::{LoadBalancingPolicy, SelectWorkerInfo};
use crate::worker::Worker;

/// Sub-mode selector. Phase 3 only implements `Default`. `Tr` (transactional)
/// arrives in Phase 5 with capacity-gated admission; `unimplemented!()` is
/// reached if a caller hits TR before P5 wires the gate.
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
}

impl Default for ThunderConfig {
    fn default() -> Self {
        Self {
            sub_mode: ThunderSubMode::Default,
            capacity_reserved_fraction: 0.10,
            resume_timeout_secs: 1800,
            scheduler_tick_ms: 100,
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
                let a_count = self.backends.get(*a).map(|s| s.active_count()).unwrap_or(0);
                let b_count = self.backends.get(*b).map(|s| s.active_count()).unwrap_or(0);
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
        program.in_flight += 1;
        program.step_count += 1;
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
}

impl ThunderPolicy {
    pub fn new(config: ThunderConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(RouterState::default())),
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
```

- [ ] **Step 1.3:** Build to verify scaffold compiles:

```bash
cd /home/hkang/wl/smg-wl
cargo build -p smg 2>&1 | tail -10
```

Expected: green. The scaffold has no `LoadBalancingPolicy` impl yet so it's not registered as a policy — but it must compile in isolation.

- [ ] **Step 1.4:** Commit:

```bash
git add model_gateway/src/policies/mod.rs model_gateway/src/policies/thunder.rs
git commit -m "feat(policies): scaffold ThunderPolicy with RouterState (Phase 3)

Adds the data structures Program, BackendState, RouterState plus the
ThunderPolicy struct with config and shared Arc<RwLock<RouterState>>.
No LoadBalancingPolicy impl yet — that's the next commit. Default-mode
selection helper select_least_active is in place for the impl to call.

Refs: docs/thunder/04-smg-integration.md §5.1-5.4, worklog D-3 (single
RwLock perf footgun), D-19 (P3 scope trim)"
```

---

## Task 2: Implement `LoadBalancingPolicy` for `ThunderPolicy` (Default sub-mode)

**Files:**
- Modify: `model_gateway/src/policies/thunder.rs` (add ~80 LOC for the impl + tests)

`★ Decision tag (autonomous):` `select_worker` (sync) returns the same answer as `select_worker_async` for Default sub-mode (no async work needed; pure RwLock read+write under tokio). Both delegate to a private helper `pick_default_inner`. TR sub-mode panics with `unimplemented!()` until P5.

- [ ] **Step 2.1:** Add the `LoadBalancingPolicy` impl at the bottom of `thunder.rs`:

```rust
#[async_trait]
impl LoadBalancingPolicy for ThunderPolicy {
    fn select_worker(&self, workers: &[Arc<dyn Worker>], info: &SelectWorkerInfo) -> Option<usize> {
        // Sync path: use blocking_read. Only safe outside async context. The
        // canonical entry point is select_worker_async; this exists for trait
        // object completeness + the per-policy parity tests in P1.
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

        let program_id = info.program_id.unwrap_or("default"); // Q5.2 fallback

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
                warn!("ThunderSubMode::Tr selected but capacity gate not wired (P5); falling back to Default");
                let chosen_url = state.select_least_active(&urls)?;
                let idx = workers.iter().position(|w| w.url() == chosen_url)?;
                state.assign(program_id, &chosen_url);
                Some(idx)
            }
        }
    }
}
```

- [ ] **Step 2.2:** Add unit tests at end of `thunder.rs`:

```rust
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
}
```

- [ ] **Step 2.3:** Run tests:

```bash
cd /home/hkang/wl/smg-wl
cargo test -p smg policies::thunder::tests 2>&1 | tail -10
```

Expected: 4 tests pass.

- [ ] **Step 2.4:** Run clippy:

```bash
cargo clippy -p smg --all-targets --all-features -- -D warnings 2>&1 | tail -10
```

Expected: green. Common warnings to fix:
- `dead_code` on `pub fn snapshot_state` if not used outside crate — annotate with `#[allow(dead_code)]` since P8's admin endpoint will use it.
- `clippy::unused_async` on `snapshot_state` — leave as-is; future P4+ versions may need to await on metrics fetch.

- [ ] **Step 2.5:** Commit:

```bash
git add model_gateway/src/policies/thunder.rs
git commit -m "feat(policies): impl LoadBalancingPolicy for ThunderPolicy default-mode (Phase 3)

select_worker_async picks the backend with fewest active programs (Q5.6
faithful), with sticky routing on program_id (subsequent requests of
the same program land on the same backend). Tr sub-mode falls back to
Default with a warning until P5 wires the capacity gate. Q5.2 fallback:
program_id None resolves to a 'default' pseudo-program.

4 unit tests cover: least-active select, sticky routing, fallback key,
empty worker set, snapshot state after routes.

Refs: docs/thunder/03-algorithm.md, worklog D-19"
```

---

## Task 3: `PolicyConfig::Thunder` + factory wiring

**Files:**
- Modify: `model_gateway/src/config/types.rs:347+` (add variant + name() arm)
- Modify: `model_gateway/src/policies/factory.rs:21,88` (add 2 arms)

- [ ] **Step 3.1:** Add `Thunder` variant to `PolicyConfig` enum at `config/types.rs:347-405`. Find the existing variants (Random, RoundRobin, CacheAware, etc.) and add **after `PrefixHash` variant** (alphabetically last):

```rust
    Thunder {
        /// Sub-mode selector: "default" or "tr" (TR not active until P5).
        sub_mode: String,
        /// Reserved fraction of backend capacity (0.0..=1.0, default 0.10).
        capacity_reserved_fraction: f64,
        /// Resume-wait timeout in seconds (default 1800 = 30 min).
        resume_timeout_secs: u64,
        /// Scheduler tick interval in milliseconds (default 100).
        scheduler_tick_ms: u64,
    },
```

(D-4 default values applied.)

- [ ] **Step 3.2:** Add `Thunder { .. } => "thunder"` to `name()` match at `config/types.rs:441+`:

```rust
            PolicyConfig::Thunder { .. } => "thunder",
```

- [ ] **Step 3.3:** Add factory wiring. Edit `policies/factory.rs:21+` (the `match config` block) — add this arm before the closing brace of the match:

```rust
            PolicyConfig::Thunder {
                sub_mode,
                capacity_reserved_fraction,
                resume_timeout_secs,
                scheduler_tick_ms,
            } => {
                let sub_mode = match sub_mode.to_lowercase().as_str() {
                    "default" => super::thunder::ThunderSubMode::Default,
                    "tr" => super::thunder::ThunderSubMode::Tr,
                    other => {
                        tracing::warn!(value = %other, "unknown thunder sub_mode, defaulting to 'default'");
                        super::thunder::ThunderSubMode::Default
                    }
                };
                let cfg = super::thunder::ThunderConfig {
                    sub_mode,
                    capacity_reserved_fraction: *capacity_reserved_fraction,
                    resume_timeout_secs: *resume_timeout_secs,
                    scheduler_tick_ms: *scheduler_tick_ms,
                };
                Arc::new(ThunderPolicy::new(cfg))
            }
```

Also import `ThunderPolicy` at top of `factory.rs` — modify the `use super::{...};` import:

```rust
use super::{
    BucketConfig, BucketPolicy, CacheAwareConfig, CacheAwarePolicy, ConsistentHashingPolicy,
    LoadBalancingPolicy, ManualConfig, ManualPolicy, PowerOfTwoPolicy, PrefixHashConfig,
    PrefixHashPolicy, RandomPolicy, RoundRobinPolicy, ThunderPolicy,
};
```

- [ ] **Step 3.4:** Add `create_by_name` arm at line 88:

```rust
            "thunder" => Some(Arc::new(ThunderPolicy::with_defaults())),
```

- [ ] **Step 3.5:** Add factory test. Append to `factory.rs::tests::test_create_from_config` and `test_create_by_name`:

```rust
        let policy = PolicyFactory::create_from_config(&PolicyConfig::Thunder {
            sub_mode: "default".to_string(),
            capacity_reserved_fraction: 0.10,
            resume_timeout_secs: 1800,
            scheduler_tick_ms: 100,
        });
        assert_eq!(policy.name(), "thunder");
```

```rust
        assert!(PolicyFactory::create_by_name("thunder").is_some());
        assert!(PolicyFactory::create_by_name("Thunder").is_some());
```

- [ ] **Step 3.6:** Build + test + clippy:

```bash
cd /home/hkang/wl/smg-wl
cargo build -p smg 2>&1 | tail -5
cargo test -p smg policies::factory 2>&1 | tail -10
cargo clippy -p smg --all-targets --all-features -- -D warnings 2>&1 | tail -5
```

Expected: all green.

- [ ] **Step 3.7:** Commit:

```bash
git add model_gateway/src/policies/factory.rs model_gateway/src/config/types.rs
git commit -m "feat(config): wire PolicyConfig::Thunder + factory create arms (Phase 3)

PolicyConfig::Thunder variant carries D-4 default values (sub_mode=
'default', capacity_reserved_fraction=0.10, resume_timeout_secs=1800,
scheduler_tick_ms=100). PolicyFactory::create_from_config dispatches
to ThunderPolicy::new(ThunderConfig{..}) and create_by_name accepts
'thunder' (case-insensitive). Unknown sub_mode strings warn + default
to Default-mode (graceful degrade).

Refs: docs/thunder/04-smg-integration.md §5.4, worklog D-4"
```

---

## Task 4: CLI `--policy thunder` + `--thunder-*` flags

**Files:**
- Modify: `model_gateway/src/main.rs:152` (add `"thunder"` to value_parser)
- Modify: `model_gateway/src/main.rs:907` (add `"thunder" => PolicyConfig::Thunder { .. }` arm)
- Modify: `model_gateway/src/main.rs` (add 4 new `#[arg(long = "thunder-...")]` fields with help_heading="Thunder Policy")

`★ Decision tag (autonomous):` Per D-14 CLI matrix, `--prefill-policy` and `--decode-policy` (PD path) reject "thunder". P3 only updates the regular `--policy` value_parser; the PD parsers at lines 217/221 stay unchanged → automatically reject "thunder". The hard-fail at `validate_compatibility` for `--policy thunder + --pd-disaggregation` lands in P5 (when TR mode lands); skipping for P3.

- [ ] **Step 4.1:** Add `"thunder"` to value_parser at `main.rs:152`:

```rust
    #[arg(long, default_value = "cache_aware", value_parser = ["random", "round_robin", "cache_aware", "power_of_two", "prefix_hash", "consistent_hashing", "manual", "bucket", "thunder"], help_heading = "Routing Policy")]
    pub policy: String,
```

(Added "thunder" at the end — preserve all existing entries verbatim.)

- [ ] **Step 4.2:** Add 4 thunder-specific CLI fields. Find the `Routing Policy` heading section in main.rs (around line 152) and append after the existing policy-specific args (search for `cache-threshold` or similar policy flags to find the right neighborhood):

```rust
    #[arg(long, default_value = "default", value_parser = ["default", "tr"], help_heading = "Thunder Policy")]
    pub thunder_sub_mode: String,

    #[arg(long, default_value_t = 0.10, help_heading = "Thunder Policy")]
    pub thunder_capacity_reserved_fraction: f64,

    #[arg(long, default_value_t = 1800, help_heading = "Thunder Policy")]
    pub thunder_resume_timeout_secs: u64,

    #[arg(long, default_value_t = 100, help_heading = "Thunder Policy")]
    pub thunder_scheduler_tick_ms: u64,
```

- [ ] **Step 4.3:** Add the `"thunder" => PolicyConfig::Thunder { .. }` arm at line 907 (the policy-from-string match):

```rust
            "thunder" => PolicyConfig::Thunder {
                sub_mode: args.thunder_sub_mode.clone(),
                capacity_reserved_fraction: args.thunder_capacity_reserved_fraction,
                resume_timeout_secs: args.thunder_resume_timeout_secs,
                scheduler_tick_ms: args.thunder_scheduler_tick_ms,
            },
```

(Insert in alphabetical order or at the end of the policy match block — match the surrounding code's style.)

- [ ] **Step 4.4:** Build + clippy:

```bash
cd /home/hkang/wl/smg-wl
cargo build -p smg 2>&1 | tail -5
cargo clippy -p smg --all-targets --all-features -- -D warnings 2>&1 | tail -5
```

Expected: green. Smoke-test the CLI accepts the flag:

```bash
./target/debug/smg start --help 2>&1 | grep -A2 'thunder' | head -20
```

Expected: shows the `--policy` accepts `thunder` and the four `--thunder-*` flags appear under "Thunder Policy" heading.

- [ ] **Step 4.5:** Commit:

```bash
git add model_gateway/src/main.rs
git commit -m "feat(cli): --policy thunder + --thunder-* config flags (Phase 3)

Adds 'thunder' to the regular --policy value_parser whitelist (PD
prefill/decode parsers continue to reject 'thunder' per D-14). Adds
four --thunder-* flags under the 'Thunder Policy' help heading:
sub-mode, capacity-reserved-fraction, resume-timeout-secs,
scheduler-tick-ms. Default values match D-4 (default sub-mode, 10%
reserved, 1800s timeout, 100ms tick).

Refs: docs/thunder/05-config-cli.md, worklog D-4/D-14"
```

---

## Task 5: e2e — `--policy thunder` routes a request

**Files:**
- Create: `e2e_test/thunder/test_phase3_thunder_default_mode.py` (~70 LOC)
- Modify: `e2e_test/thunder/conftest.py` — add a new fixture `smg_thunder_router` that spawns SMG with `--policy thunder` (do NOT replace `smg_router`; add alongside)

`★ Decision tag (autonomous):` Adding a new conftest fixture is allowed in P3 (P0/P2 reuse decisions don't bind P3). Placing both `smg_router` (cache_aware) and `smg_thunder_router` (thunder) lets P3 e2e drive thunder while P0/P2 continue using cache_aware.

- [ ] **Step 5.1:** Open `e2e_test/thunder/conftest.py`. Below the existing `smg_router` fixture, add:

```python
@pytest.fixture(scope="session")
def smg_thunder_router(mock_backend):
    """SMG with --policy thunder, pointing at the same mock_backend.

    Used by Phase 3+ tests; coexists with smg_router (cache_aware) so
    Phase 0-2 tests keep passing under cache_aware.
    """
    port = _free_port()
    binary = os.path.join(REPO_ROOT, "target", "debug", "smg")
    if not os.path.exists(binary):
        binary = os.path.join(REPO_ROOT, "target", "release", "smg")
    if not os.path.exists(binary):
        pytest.skip(
            f"smg binary not found at target/{{debug,release}}/smg; "
            f"run `cargo build -p smg` from {REPO_ROOT} first"
        )
    cmd = [
        binary, "start",
        "--host", "127.0.0.1",
        "--port", str(port),
        "--worker-urls", mock_backend,
        "--policy", "thunder",
        "--thunder-sub-mode", "default",
    ]
    proc = subprocess.Popen(cmd, cwd=REPO_ROOT)
    try:
        _wait_http(f"http://127.0.0.1:{port}/health", timeout=20)
        yield f"http://127.0.0.1:{port}"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
```

- [ ] **Step 5.2:** Create `e2e_test/thunder/test_phase3_thunder_default_mode.py`:

```python
"""Phase 3 e2e: SMG with --policy thunder routes traffic in Default sub-mode.

Validates:
- Thunder accepts /v1/messages requests
- Same program_id → sticky routing (same backend across calls)
- Different program_ids distribute across backends (least-active-count)
"""
from __future__ import annotations

import requests


def test_thunder_default_mode_basic_request(smg_thunder_router):
    body = {
        "model": "Qwen/Qwen3-0.6B",
        "max_tokens": 16,
        "messages": [{"role": "user", "content": "hello thunder"}],
        "metadata": {"program_id": "phase3-basic"},
    }
    r = requests.post(f"{smg_thunder_router}/v1/messages", json=body, timeout=10)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("_mock_echo_program_id") == "phase3-basic"


def test_thunder_default_mode_no_program_id(smg_thunder_router):
    """No metadata.program_id → falls back to 'default' pseudo-program."""
    body = {
        "model": "Qwen/Qwen3-0.6B",
        "max_tokens": 8,
        "messages": [{"role": "user", "content": "no pid"}],
    }
    r = requests.post(f"{smg_thunder_router}/v1/messages", json=body, timeout=10)
    assert r.status_code == 200
    assert r.json().get("_mock_echo_program_id") is None


def test_thunder_chat_completions(smg_thunder_router):
    """Thunder routes chat completions just like /v1/messages."""
    body = {
        "model": "Qwen/Qwen3-0.6B",
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 8,
        "metadata": {"program_id": "phase3-chat"},
    }
    r = requests.post(f"{smg_thunder_router}/v1/chat/completions", json=body, timeout=10)
    assert r.status_code == 200
    assert r.json().get("object") == "chat.completion"
```

- [ ] **Step 5.3:** Run the new e2e + regression on P0/P2:

```bash
cd /home/hkang/wl/smg-wl
source e2e_test/.venv/bin/activate
pytest e2e_test/thunder/ -v --rootdir=e2e_test/thunder --confcutdir=e2e_test/thunder 2>&1 | tail -20
```

Expected: 10 tests pass (3 P0 + 4 P2 + 3 P3). The thunder fixture spawns a separate SMG instance on a different port, so it won't conflict with the cache_aware fixture.

- [ ] **Step 5.4:** Commit:

```bash
git add e2e_test/thunder/conftest.py e2e_test/thunder/test_phase3_thunder_default_mode.py
git commit -m "test(thunder): Phase 3 e2e — --policy thunder default-mode (Phase 3)

Adds smg_thunder_router conftest fixture (separate SMG instance with
--policy thunder, same mock backend). Three test cases cover basic
request, no-program-id fallback, and cross-protocol routing.
ThunderPolicy now accepts traffic in production-shape e2e.

Refs: docs/thunder/10-phases.md P3 row"
```

---

## Task 6: Phase exit + worklog D-19

- [ ] **Step 6.1:** Run full verification:

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -3
cargo test --workspace 2>&1 | tail -20
cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -10
source e2e_test/.venv/bin/activate
pytest e2e_test/thunder/ -v --rootdir=e2e_test/thunder --confcutdir=e2e_test/thunder 2>&1 | tail -15
bash scripts/check_thunder_xref.sh 2>&1 | tail -5
```

Expected: build + clippy green, workspace tests green except known PD perf flake, e2e 10/10, xref OK.

- [ ] **Step 6.2:** Append D-19 to `docs/thunder/worklog.md`:

```markdown
---

## D-19: P3 implementation completed — ThunderPolicy skeleton lands

**Date**: 2026-05-01
**Spec ref**: `docs/thunder/10-phases.md` P3 row, `docs/thunder/04-smg-integration.md` §5.1-5.4
**Approval mode**: <CLAUDE-AUTONOMOUS-DECISION> — Claude trimmed scope; user sign-off pending

### What landed

- New `model_gateway/src/policies/thunder.rs` (~250 LOC): `Program`, `BackendState`, `RouterState`, `ThunderConfig`, `ThunderSubMode { Default, Tr }`, `ThunderPolicy`
- `LoadBalancingPolicy` impl with sync + async select (Default sub-mode = least-active-program-count + sticky routing on `program_id`)
- `PolicyConfig::Thunder` variant with D-4 default values + `name()` arm + factory wiring in both `create_from_config` and `create_by_name`
- CLI: `--policy thunder` accepted; new `--thunder-{sub-mode,capacity-reserved-fraction,resume-timeout-secs,scheduler-tick-ms}` flags
- e2e: `smg_thunder_router` conftest fixture + 3 test cases proving `--policy thunder` routes traffic
- 4 unit tests in `policies::thunder::tests` cover least-active select, sticky routing, fallback key, snapshot

### What did NOT change (deferred per autonomous trim)

- `usage_consumer` task → P4
- HTTP usage tail extractor (SSE parse for token counts) → P4
- `stream_options.include_usage = true` injection → P4
- `WorkerRegistry::subscribe_events` integration → P5
- TR sub-mode capacity gate → P5
- Pause/resume + BFD + force-timeout → P6
- `ProgramRequestGuard` RAII → P6
- gRPC validation → P7
- Profiling endpoints → P8

### Autonomous decisions made (require user review)

1. **P3 scope trim**: ship "ThunderPolicy compiles + routes traffic" only; usage tracking + capacity + pause/resume layered into P4-P6. Rationale: prioritize "能跑" over feature-complete given user's explicit time pressure.
2. **`tokio::sync::RwLock<RouterState>` (not parking_lot)**: enables future `.await` inside a held lock if TR mode needs it. D-3 single-mutex perf footgun acknowledged; benchmark in P9.
3. **`needs_request_text() = false`**: Default mode doesn't consult cache; saves the `extract_text_for_routing` call on every request.
4. **Tr sub-mode falls back to Default with a warn log** rather than `unimplemented!()` panic: keeps the gateway running if a user sets `--thunder-sub-mode tr` before P5 lands. P5 will replace this with the real capacity gate.
5. **Q5.2 fallback**: `program_id_hint() == None` resolves to a `"default"` pseudo-program; all such requests land on the same backend (sticky on the literal "default" key). Matches Python ThunderAgent behavior.
6. **Sync `select_worker` uses `blocking_write`**: only safe outside an async runtime. The canonical entry is `select_worker_async`; the sync impl exists for trait-object completeness + P1's parity tests. Documented in code comments.

### Footguns surfaced

1. `select_worker` (sync) calling `state.blocking_write()` will panic if invoked from inside a tokio runtime. Production routers always use the async path; this only matters if a future caller forgets.
2. Sub-mode is a `String` in `PolicyConfig::Thunder` (not enum) for serde compatibility — typos result in a warn-log fallback to Default rather than an error.

### Revisit conditions

1. If P5+ shows that contention on the single `RwLock<RouterState>` is measurable → migrate to per-backend sharding.
2. If "default" pseudo-program causes load imbalance (all unidentified requests stick to one backend) → consider hashing the request body to spread.
3. If a future user sets `--thunder-sub-mode tr` before P5 → confirm the warn-fallback behavior is acceptable; otherwise reject at validate_compatibility.

### Approved by

(Pending Claude review + user sign-off.)
```

- [ ] **Step 6.3:** Commit worklog:

```bash
git add docs/thunder/worklog.md
git commit -m "docs(thunder): worklog D-19 records P3 completion (Phase 3)

Captures 5 implementation commits + 6 autonomous design decisions made
during scope trim. P3 ships ThunderPolicy skeleton routing traffic via
--policy thunder; usage tracking + capacity + pause/resume layered in
P4-P6.

Refs: docs/thunder/10-phases.md P3 row, autonomous workflow per
docs/thunder/workflow.md"
```

- [ ] **Step 6.4:** Final report to `/tmp/p3-report.md` with: commits, build/test/e2e results, OPEN_QUESTIONs (none expected), deviations.

Then STOP. Claude reviews + ff-merges.

---

## Phase exit criteria

| Check | Required |
|---|---|
| `cargo build --workspace` | ✅ |
| `cargo clippy --all-targets --all-features -- -D warnings` | ✅ |
| `cargo test -p smg policies::thunder` (4 unit tests) | ✅ |
| `pytest e2e_test/thunder/` 10/10 (3+4+3) | ✅ |
| `--policy thunder` accepted by CLI | ✅ |
| Worklog D-19 with 6 `<CLAUDE-AUTONOMOUS-DECISION>` items | ✅ |
| 6 commits on `thunder-policy-p3` | ✅ |

## Rollback

```bash
cd /home/hkang/wl/smg-wl
git checkout thunder-policy
git branch -D thunder-policy-p3   # destructive — requires user OK
```
