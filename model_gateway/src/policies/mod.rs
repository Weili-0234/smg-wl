//! Load balancing policies for SGLang router
//!
//! This module provides a unified abstraction for routing policies that work
//! across both regular and prefill-decode (PD) routing modes.

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use openai_protocol::worker::WorkerLoadResponse;
use smg_mesh::OptionalMeshSyncManager;
use tokio::sync::mpsc::UnboundedSender;

use crate::worker::{HashRing, Worker};

mod bucket;
mod cache_aware;
mod consistent_hashing;
mod dp_min_token;
mod factory;
mod manual;
mod power_of_two;
mod prefix_hash;
mod random;
mod registry;
mod round_robin;
pub(crate) mod utils;

pub use bucket::BucketPolicy;
pub use cache_aware::CacheAwarePolicy;
pub use consistent_hashing::ConsistentHashingPolicy;
pub use dp_min_token::MinimumTokensPolicy;
pub use factory::PolicyFactory;
// Re-export PrefixMatchResult from kv_index for production use
pub use kv_index::PrefixMatchResult;
pub use manual::{ManualConfig, ManualPolicy};
pub use power_of_two::PowerOfTwoPolicy;
pub use prefix_hash::{PrefixHashConfig, PrefixHashPolicy};
pub use random::RandomPolicy;
pub use registry::PolicyRegistry;
pub use round_robin::RoundRobinPolicy;

/// Per-request usage event emitted by routers after the upstream stream completes.
///
/// Stateless policies ignore this; ThunderPolicy (Phase 3+) consumes it via the
/// `usage_sender` channel to update `BackendState.active_program_tokens` and the
/// per-program `char_to_token_ratio` calibration.
///
/// `request_text_chars` is captured by the router at admission time (length of
/// the value returned by `GenerationRequest::extract_text_for_routing`) so the
/// consumer can compute `tokens_per_char = total_tokens / request_text_chars`.
#[derive(Debug, Clone)]
pub struct UsageEvent {
    /// Program identifier this usage belongs to (None for non-program requests
    /// or when the client did not send `metadata.program_id`).
    pub program_id: Option<String>,
    /// Backend URL the request was routed to (matches `worker.url()`).
    pub backend_url: String,
    /// Prompt tokens reported by upstream usage payload.
    pub prompt_tokens: u32,
    /// Completion tokens reported by upstream usage payload.
    pub completion_tokens: u32,
    /// Sum of prompt + completion (kept explicit so consumers don't repeat the math).
    pub total_tokens: u32,
    /// Char-length of the routing-extracted request text (for char→token ratio calibration).
    pub request_text_chars: usize,
}

/// Core trait for load balancing policies
///
/// This trait provides a unified interface for implementing routing algorithms
/// that can work with both regular single-worker selection and PD dual-worker selection.
#[async_trait]
pub trait LoadBalancingPolicy: Send + Sync + Debug {
    /// Select a single worker from the available workers
    ///
    /// This is used for regular routing mode where requests go to a single worker.
    /// Now uses Arc<dyn Worker> for better performance and to avoid unnecessary cloning.
    ///
    /// # Arguments
    /// * `workers` - Available workers to select from
    /// * `info` - Additional information for routing decisions
    fn select_worker(&self, workers: &[Arc<dyn Worker>], info: &SelectWorkerInfo) -> Option<usize>;

    /// Update policy state after request completion
    ///
    /// This is called when a request completes (successfully or not) to allow
    /// policies to update their internal state.
    fn on_request_complete(&self, _worker_url: &str, _success: bool) {
        // Default: no-op for stateless policies
    }

    /// Get policy name for metrics and debugging
    fn name(&self) -> &'static str;

    /// Check if this policy needs request text for routing decisions
    fn needs_request_text(&self) -> bool {
        false // Default: most policies don't need request text
    }

    /// Update worker load information
    ///
    /// This is called periodically with current load information for load-aware policies.
    fn update_loads(&self, _loads: &std::collections::HashMap<String, WorkerLoadResponse>) {
        // Default: no-op for policies that don't use load information
    }

    /// Set mesh sync manager
    fn set_mesh_sync(&mut self, _mesh_sync: OptionalMeshSyncManager) {
        // Default: no-op for policies that don't use mesh sync
    }

    /// Reset any internal state
    ///
    /// This is useful for policies that maintain state (e.g., round-robin counters).
    fn reset(&self) {
        // Default: no-op for stateless policies
    }

    /// Get as Any for downcasting
    fn as_any(&self) -> &dyn std::any::Any;

    /// Async variant of `select_worker`. The default implementation delegates
    /// to `select_worker` so existing policies keep working unchanged. Policies
    /// that need to do async work during selection (e.g., ThunderPolicy
    /// awaiting a per-program Notify after a pause) override this method.
    ///
    /// ## Why both sync and async?
    ///
    /// Most policies (cache_aware, round_robin, etc.) make selection decisions
    /// from in-memory state and don't need `.await`. Forcing them to be async
    /// would add overhead and complicate their tests. ThunderPolicy is the
    /// outlier — it may pause selection until capacity frees — and it's the
    /// only thing that overrides the default.
    async fn select_worker_async(
        &self,
        workers: &[Arc<dyn Worker>],
        info: &SelectWorkerInfo<'_>,
    ) -> Option<usize> {
        self.select_worker(workers, info)
    }

    /// Optional usage-event sender. Routers fire-and-forget a `UsageEvent`
    /// after the upstream stream completes. Stateless policies return `None`
    /// (the default) and routers short-circuit the send. ThunderPolicy returns
    /// `Some(&self.usage_tx)` so it can update `BackendState.active_program_tokens`
    /// and per-program `char_to_token_ratio`.
    fn usage_sender(&self) -> Option<&UnboundedSender<UsageEvent>> {
        None
    }
}

pub trait DPRankLoadPolicy: Send + Sync + Debug {
    fn select_dp_rank(&self, worker: &dyn Worker, estimated_cost: isize) -> Option<isize>;
}

/// Configuration for cache-aware policy
#[derive(Debug, Clone)]
pub struct CacheAwareConfig {
    pub cache_threshold: f32,
    pub balance_abs_threshold: usize,
    pub balance_rel_threshold: f32,
    pub eviction_interval_secs: u64,
    pub max_tree_size: usize,
    /// Backend KV cache block size (tokens per block) for event-driven routing.
    /// Used by `compute_request_content_hashes` to chunk request tokens into blocks.
    /// Must match the backend's block size. Default: 16 (SGLang page size).
    pub block_size: usize,
}

impl Default for CacheAwareConfig {
    fn default() -> Self {
        Self {
            cache_threshold: 0.5,
            balance_abs_threshold: 32,
            balance_rel_threshold: 1.1,
            eviction_interval_secs: 30,
            max_tree_size: 10000,
            block_size: 16,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BucketConfig {
    pub balance_abs_threshold: usize,
    pub balance_rel_threshold: f32,
    pub bucket_adjust_interval_secs: usize,
}

impl Default for BucketConfig {
    fn default() -> Self {
        Self {
            balance_abs_threshold: 32,
            balance_rel_threshold: 1.0001,
            bucket_adjust_interval_secs: 5,
        }
    }
}

/// Helper function to filter healthy workers and return their indices
pub(crate) fn get_healthy_worker_indices(workers: &[Arc<dyn Worker>]) -> Vec<usize> {
    workers
        .iter()
        .enumerate()
        .filter(|(_, w)| w.is_healthy() && w.circuit_breaker_can_execute())
        .map(|(idx, _)| idx)
        .collect()
}

/// Helper function to normalize model_id to a key for policy lookups.
///
/// Returns UNKNOWN_MODEL_ID for empty model_ids to ensure consistent behavior
/// across single-model and multi-model deployments.
#[inline]
pub(crate) fn normalize_model_key(model_id: &str) -> &str {
    if model_id.is_empty() {
        crate::worker::UNKNOWN_MODEL_ID
    } else {
        model_id
    }
}

/// Information passed to policy for worker selection
#[derive(Debug, Clone, Default)]
pub struct SelectWorkerInfo<'a> {
    /// Request text for cache-aware routing
    pub request_text: Option<&'a str>,
    /// Tokenized request for prefix-hash routing
    /// Used by PrefixHashPolicy for token-based prefix hashing
    pub tokens: Option<&'a [u32]>,
    /// HTTP headers for header-based routing policies
    /// Policies can extract routing information from headers like:
    /// - X-SMG-Target-Worker: Direct routing to a specific worker by index
    /// - X-SMG-Routing-Key: Consistent hash routing for session affinity
    pub headers: Option<&'a http::HeaderMap>,
    /// Pre-computed hash ring for O(log n) consistent hashing
    /// Built and cached by WorkerRegistry, passed through to avoid per-request rebuilds
    pub hash_ring: Option<Arc<HashRing>>,
    /// Program identifier extracted from the request body (typically from
    /// `metadata.program_id` for Anthropic Messages requests). Read by
    /// program-aware policies (Thunder) for capacity tracking. Default None
    /// keeps existing policies' behavior unchanged.
    pub program_id: Option<&'a str>,
}

#[cfg(test)]
mod tests {
    use openai_protocol::worker::{HealthCheckConfig, WorkerStatus};

    use super::*;
    use crate::worker::{BasicWorkerBuilder, WorkerType};

    fn no_health_check() -> HealthCheckConfig {
        HealthCheckConfig {
            disable_health_check: true,
            ..Default::default()
        }
    }

    #[test]
    fn test_get_healthy_worker_indices() {
        let workers: Vec<Arc<dyn Worker>> = vec![
            Arc::new(
                BasicWorkerBuilder::new("http://w1:8000")
                    .worker_type(WorkerType::Regular)
                    .api_key("test_api_key")
                    .health_config(no_health_check())
                    .build(),
            ),
            Arc::new(
                BasicWorkerBuilder::new("http://w2:8000")
                    .worker_type(WorkerType::Regular)
                    .api_key("test_api_key2")
                    .health_config(no_health_check())
                    .build(),
            ),
            Arc::new(
                BasicWorkerBuilder::new("http://w3:8000")
                    .worker_type(WorkerType::Regular)
                    .api_key("test_api_key")
                    .health_config(no_health_check())
                    .build(),
            ),
        ];

        // All healthy initially
        let indices = get_healthy_worker_indices(&workers);
        assert_eq!(indices, vec![0, 1, 2]);

        // Mark one unhealthy
        workers[1].set_status(WorkerStatus::NotReady);
        let indices = get_healthy_worker_indices(&workers);
        assert_eq!(indices, vec![0, 2]);
    }

    #[test]
    fn usage_event_struct_exists_and_is_constructible() {
        let ev = UsageEvent {
            program_id: Some("p1".to_string()),
            backend_url: "http://w1:8001".to_string(),
            prompt_tokens: 100,
            completion_tokens: 50,
            total_tokens: 150,
            request_text_chars: 400,
        };
        assert_eq!(ev.total_tokens, 150);
        assert_eq!(ev.program_id.as_deref(), Some("p1"));
    }

    #[test]
    fn select_worker_info_carries_program_id() {
        let pid = "agent-step-7";
        let info = SelectWorkerInfo {
            request_text: Some("hello"),
            tokens: None,
            headers: None,
            hash_ring: None,
            program_id: Some(pid),
        };
        assert_eq!(info.program_id, Some("agent-step-7"));
    }

    #[test]
    fn select_worker_info_default_program_id_is_none() {
        let info = SelectWorkerInfo::default();
        assert_eq!(info.program_id, None);
    }

    #[tokio::test]
    async fn select_worker_async_default_delegates_to_sync() {
        struct Stub;
        impl Debug for Stub {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("Stub")
            }
        }
        #[async_trait::async_trait]
        impl LoadBalancingPolicy for Stub {
            fn select_worker(
                &self,
                workers: &[Arc<dyn Worker>],
                _info: &SelectWorkerInfo,
            ) -> Option<usize> {
                if workers.is_empty() { None } else { Some(0) }
            }
            fn name(&self) -> &'static str {
                "stub"
            }
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }
        let stub = Stub;
        let workers: Vec<Arc<dyn Worker>> = vec![];
        let info = SelectWorkerInfo::default();
        // Default async impl must delegate to sync — same answer for empty + non-empty.
        let sync_result = stub.select_worker(&workers, &info);
        let async_result = stub.select_worker_async(&workers, &info).await;
        assert_eq!(sync_result, async_result);
    }

    #[test]
    fn usage_sender_default_returns_none() {
        struct Stub;
        impl Debug for Stub {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("Stub")
            }
        }
        #[async_trait::async_trait]
        impl LoadBalancingPolicy for Stub {
            fn select_worker(
                &self,
                _workers: &[Arc<dyn Worker>],
                _info: &SelectWorkerInfo,
            ) -> Option<usize> {
                None
            }
            fn name(&self) -> &'static str {
                "stub"
            }
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }
        let stub = Stub;
        assert!(stub.usage_sender().is_none());
    }
}
