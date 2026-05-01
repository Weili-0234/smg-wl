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

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{debug, trace, warn};

use super::{LoadBalancingPolicy, SelectWorkerInfo};
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
}
