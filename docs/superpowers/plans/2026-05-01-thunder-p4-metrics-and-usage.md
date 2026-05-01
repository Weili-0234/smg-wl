# Thunder P4 — Backend Capacity Polling + Usage Consumer (Trimmed) Plan

> **For agentic workers:** Opus subagent executes. Steps use `- [ ]`. Claude reviews against R1-R12.
>
> **`<CLAUDE-AUTONOMOUS-DECISION>` (D-20):** P4 scope reduced from the 10-phases.md row to "non-streaming usage tracking + HTTP capacity polling". Deferred:
> - **Streaming usage tail extractor** (parse SSE `[DONE]` tail for usage chunk) → P9 polish or P4.5
> - **`stream_options.include_usage = true` injection** on outbound streaming requests → same as above
> - **gRPC `KvEventMonitor`-driven capacity** → P7 (gRPC validation phase)
>
> **Why**: ThunderPolicy's value (capacity-aware pause/resume in P6) needs **some** capacity + usage signal. Non-streaming covers it; streaming usage requires careful SSE parsing that adds risk for time-pressured first-working-version delivery. Streaming requests will work pass-through (no Thunder state update), per P0 baseline.

**Goal:** Wire HTTP backend capacity polling + non-streaming usage emission. After P4, `BackendState.capacity_tokens` is non-zero (populated from `/get_server_info`) and `Program.total_tokens` increases as non-streaming `/v1/messages` and `/v1/chat/completions` requests complete. Sets the table for P5 admission gate and P6 BFD pause/resume.

**Architecture:**
1. New file `model_gateway/src/policies/thunder_metrics.rs` (~80 LOC): tiny `MetricsClient` trait + `HttpMetricsClient` impl that calls existing `discover_metadata::get_server_info` helper.
2. `ThunderPolicy::new` spawns 2 tokio tasks:
   - `capacity_poll_task` — every 5s, fetch `/get_server_info` from each known backend, update `BackendState.capacity_tokens`.
   - `usage_consumer_task` — receives `UsageEvent` from `mpsc::UnboundedReceiver`, updates `Program.total_tokens` + `BackendState.active_program_tokens`.
3. `ThunderPolicy::usage_sender()` returns `Some(&self.usage_tx)` (override the P1 default `None`).
4. `routers/http/router.rs::route_typed_request_once` post-non-stream-success: parse usage from response body, emit `UsageEvent` via `policy.usage_sender()` (no-op if None).

**Tech Stack:** tokio (already used). serde_json for usage parsing. `tracing` for logs.

---

## Context

- `docs/thunder/10-phases.md` row P4 (full scope; this plan trims).
- `docs/thunder/04-smg-integration.md` §5.6-5.7 (usage tail + hook design).
- `docs/thunder/worklog.md` D-2/D-19 (usage_sender hook, P3 trim).
- HEAD `dbef6669` + 6 P3 commits (after P3 ff-merge into trunk).

**Out of scope** (do NOT touch):
- All streaming SSE parsing — `routers/http/router.rs:712,923` (the `bytes_stream` branches) stay unchanged.
- gRPC code — P7 phase.
- `routers/anthropic/`, `routers/openai/`, `routers/gemini/`.

**Key file:line anchors:**

| Anchor | What is there |
|---|---|
| `model_gateway/src/workflow/steps/local/discover_metadata.rs:162` | `pub async fn get_server_info(url, api_key) -> Result<ServerInfo, String>` — reusable! |
| `crates/protocols/src/...ServerInfo` | Existing `ServerInfo` type with `cache_config`, `model_config` fields |
| `model_gateway/src/routers/http/router.rs:287` | `async fn route_typed_request_once<T>` — where usage emission goes after non-stream success |
| `model_gateway/src/policies/thunder.rs:::ThunderPolicy::new` | Where the 2 tasks get spawned |

---

## Pre-flight

- [ ] PF.1: branch `thunder-policy-p4`, parent post-P3 trunk; `git status --short` clean.
- [ ] PF.2: `cargo build --workspace` baseline green.
- [ ] PF.3: confirm `discover_metadata::get_server_info` is `pub` (or `pub(crate)`) so we can call from `policies/`.

---

## Task 1: `MetricsClient` trait + HTTP impl

**Files:**
- Create: `model_gateway/src/policies/thunder_metrics.rs` (~80 LOC)
- Modify: `model_gateway/src/policies/mod.rs` (add `mod thunder_metrics;`)

- [ ] **Step 1.1:** Create `thunder_metrics.rs`:

```rust
//! Backend metrics fetcher used by ThunderPolicy's capacity poll task.
//!
//! Today: HTTP-only, polls `/get_server_info`. gRPC backends will get a
//! different impl in Phase 7.

use async_trait::async_trait;
use tracing::{debug, warn};

/// Capacity snapshot returned by a metrics fetch.
#[derive(Debug, Clone)]
pub struct BackendCapacity {
    /// Total KV cache capacity in tokens.
    pub capacity_tokens: u64,
    /// Backend-reported model name (informational).
    pub model_name: Option<String>,
}

#[async_trait]
pub trait MetricsClient: Send + Sync + std::fmt::Debug {
    async fn fetch_capacity(&self, worker_url: &str) -> Result<BackendCapacity, String>;
}

#[derive(Debug, Default)]
pub struct HttpMetricsClient;

#[async_trait]
impl MetricsClient for HttpMetricsClient {
    async fn fetch_capacity(&self, worker_url: &str) -> Result<BackendCapacity, String> {
        // Reuse existing helper that knows how to talk to vLLM /get_server_info
        // and how to interpret cache_config.
        let info = crate::workflow::steps::local::discover_metadata::get_server_info(worker_url, None)
            .await?;
        // ServerInfo shape: cache_config.{block_size, num_gpu_blocks} OR
        // total_kv_cache_tokens directly. Mock_vllm.py exposes both for safety.
        let capacity_tokens = info
            .cache_config
            .as_ref()
            .map(|c| {
                // Prefer total_kv_cache_tokens if present, else block_size * num_gpu_blocks
                c.total_kv_cache_tokens
                    .or_else(|| {
                        c.block_size
                            .zip(c.num_gpu_blocks)
                            .map(|(b, n)| (b as u64) * (n as u64))
                    })
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        if capacity_tokens == 0 {
            warn!(worker_url, "fetch_capacity: backend returned 0 capacity");
        } else {
            debug!(worker_url, capacity_tokens, "fetch_capacity ok");
        }
        Ok(BackendCapacity {
            capacity_tokens,
            model_name: info.model_config.map(|m| m.model),
        })
    }
}
```

`★ Decision tag (autonomous):` If `ServerInfo`'s actual field shape differs from the assumptions (e.g., `total_kv_cache_tokens` doesn't exist and only `block_size * num_gpu_blocks` is available), adapt — emit `OPEN_QUESTION:` if neither pattern matches.

- [ ] **Step 1.2:** Add `mod thunder_metrics;` to `policies/mod.rs` (after `mod thunder;`). Also `pub use thunder_metrics::{HttpMetricsClient, MetricsClient};` (keeps the type accessible if external code wants to inject a mock).

- [ ] **Step 1.3:** `cargo build -p smg` green. If `ServerInfo` field names don't match, fix and proceed.

- [ ] **Step 1.4:** Commit:

```bash
git add model_gateway/src/policies/thunder_metrics.rs model_gateway/src/policies/mod.rs
git commit -m "feat(policies): MetricsClient trait + HttpMetricsClient (Phase 4)

Tiny trait + HTTP impl using existing
workflow::steps::local::discover_metadata::get_server_info helper.
Returns BackendCapacity{capacity_tokens, model_name}. Used by the
periodic capacity-poll task in ThunderPolicy::new (next commit).

Refs: docs/thunder/04-smg-integration.md §5.7"
```

---

## Task 2: Capacity poll task in `ThunderPolicy::new`

**Files:**
- Modify: `model_gateway/src/policies/thunder.rs` (extend `ThunderPolicy::new`)

- [ ] **Step 2.1:** Add to `ThunderPolicy` struct: `metrics_client: Arc<dyn thunder_metrics::MetricsClient>`. Wire in `new`:

```rust
impl ThunderPolicy {
    pub fn new(config: ThunderConfig) -> Self {
        Self::with_metrics_client(config, Arc::new(thunder_metrics::HttpMetricsClient))
    }

    pub fn with_metrics_client(
        config: ThunderConfig,
        metrics_client: Arc<dyn thunder_metrics::MetricsClient>,
    ) -> Self {
        let state = Arc::new(RwLock::new(RouterState::default()));
        let policy = Self {
            config: config.clone(),
            state: state.clone(),
            metrics_client: metrics_client.clone(),
            usage_tx: tokio::sync::mpsc::unbounded_channel().0, // placeholder; replaced in Task 3
        };
        // Spawn capacity poll task
        let state_for_poll = Arc::downgrade(&state);
        let mc = metrics_client;
        let poll_secs = config.capacity_poll_interval_secs;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(poll_secs));
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
                    match mc.fetch_capacity(&url).await {
                        Ok(cap) => {
                            let mut guard = state_arc.write().await;
                            if let Some(b) = guard.backends.get_mut(&url) {
                                b.capacity_tokens = cap.capacity_tokens;
                            }
                        }
                        Err(e) => {
                            warn!(url, error = %e, "capacity fetch failed");
                        }
                    }
                }
            }
        });
        policy
    }
}
```

- [ ] **Step 2.2:** Add `capacity_poll_interval_secs: u64` to `ThunderConfig`, default 5. (And to `PolicyConfig::Thunder` variant + CLI flag — follow the same pattern as the other thunder fields.)

- [ ] **Step 2.3:** Build + clippy. `Weak<RwLock<RouterState>>` cycle-break is the canonical pattern for tokio tasks holding state references.

- [ ] **Step 2.4:** Commit:

```bash
git add ...
git commit -m "feat(policies): ThunderPolicy spawns capacity-poll task (Phase 4)

ThunderPolicy::with_metrics_client spawns a tokio task that polls
each known backend via MetricsClient every capacity_poll_interval_secs
(default 5s). Updates BackendState.capacity_tokens. Uses Weak<RwLock>
so the task exits cleanly when ThunderPolicy is dropped (no Arc cycle).

Refs: docs/thunder/04-smg-integration.md §5.7"
```

---

## Task 3: `usage_sender()` + usage_consumer task

**Files:**
- Modify: `model_gateway/src/policies/thunder.rs`

- [ ] **Step 3.1:** Replace the placeholder `usage_tx` with a real channel + spawn the consumer task in `with_metrics_client`:

```rust
        let (usage_tx, mut usage_rx) = tokio::sync::mpsc::unbounded_channel::<UsageEvent>();
        let state_for_consumer = Arc::downgrade(&state);
        tokio::spawn(async move {
            while let Some(event) = usage_rx.recv().await {
                let Some(state_arc) = state_for_consumer.upgrade() else {
                    debug!("ThunderPolicy state dropped; usage consumer exiting");
                    return;
                };
                let mut guard = state_arc.write().await;
                let pid = event.program_id.as_deref().unwrap_or("default");
                if let Some(p) = guard.programs.get_mut(pid) {
                    p.total_tokens = p.total_tokens.saturating_add(event.total_tokens as u64);
                    if p.in_flight > 0 {
                        p.in_flight -= 1;
                    }
                }
                if let Some(b) = guard.backends.get_mut(&event.backend_url) {
                    b.active_program_tokens = b.active_program_tokens.saturating_add(event.total_tokens as u64);
                }
                trace!(program_id = %pid, total_tokens = event.total_tokens, "usage applied");
            }
        });
        Self {
            config,
            state,
            metrics_client,
            usage_tx,
        }
```

- [ ] **Step 3.2:** Override `usage_sender()` on `LoadBalancingPolicy` impl for `ThunderPolicy`:

```rust
    fn usage_sender(&self) -> Option<&tokio::sync::mpsc::UnboundedSender<UsageEvent>> {
        Some(&self.usage_tx)
    }
```

(Add this method to the `impl LoadBalancingPolicy for ThunderPolicy` block.)

- [ ] **Step 3.3:** Build + unit test:

Add a test that constructs `ThunderPolicy`, sends a synthetic `UsageEvent` via `policy.usage_sender().unwrap().send(...)`, sleeps 100ms, then snapshot_state and assert `Program.total_tokens` increased.

- [ ] **Step 3.4:** Commit.

---

## Task 4: Non-streaming usage emission from router

**Files:**
- Modify: `model_gateway/src/routers/http/router.rs::route_typed_request_once` (around the non-streaming response collection)

- [ ] **Step 4.1:** After the non-streaming response body is read (search for `Body::from_stream` and the non-streaming branch), parse usage and emit:

```rust
        // After successful non-streaming response: emit UsageEvent if policy supports it.
        if let Some(tx) = policy.usage_sender() {
            if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&full_body_bytes) {
                if let Some(usage) = parsed.get("usage") {
                    let prompt_tokens = usage.get("prompt_tokens").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                    let completion_tokens = usage.get("completion_tokens").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                    let total_tokens = prompt_tokens + completion_tokens;
                    let _ = tx.send(crate::policies::UsageEvent {
                        program_id: program_id.map(|s| s.to_string()),
                        backend_url: worker.url().to_string(),
                        prompt_tokens,
                        completion_tokens,
                        total_tokens,
                        request_text_chars: text.map(|s| s.len()).unwrap_or(0),
                    });
                }
            }
        }
```

(Adapt `full_body_bytes`, `worker`, `program_id`, `text` variable names to whatever's in scope at that point in `route_typed_request_once`.)

`★ Decision tag (autonomous):` `let _ = tx.send(...)` swallows send errors — appropriate for fire-and-forget. The receiver task can't fail unless the policy is being dropped, which is a process-shutdown scenario.

- [ ] **Step 4.2:** Build + e2e regression. Phase 0 + P2 + P3 e2e (10 tests) should still pass.

- [ ] **Step 4.3:** Add a P4 e2e: send a non-streaming `/v1/messages` request to thunder, then poll `/get_server_info` or use admin endpoint... actually since admin endpoint is P8, just verify via cargo unit test on `ThunderPolicy::snapshot_state()` that `program.total_tokens > 0` after a request.

Actually simpler: a Rust integration test in `model_gateway/tests/` that spins up `ThunderPolicy::with_metrics_client(MockMetricsClient)`, sends a synthetic usage event via the policy's `usage_sender()`, and asserts state updated. Skip the e2e for P4; it lands in P5/P6 with capacity tests.

- [ ] **Step 4.4:** Commit.

---

## Task 5: Phase exit + worklog D-20

- [ ] Run full verification (build/test/clippy/e2e/xref).
- [ ] Append D-20 to worklog with `<CLAUDE-AUTONOMOUS-DECISION>` notes:
  - P4 trim (streaming usage deferred)
  - `Weak<RwLock>` cycle-break pattern
  - Fire-and-forget `let _ = tx.send(...)` for usage emission
  - Default capacity_poll_interval_secs = 5
- [ ] Final report `/tmp/p4-report.md`.
- [ ] STOP for Claude review + ff-merge.

---

## Phase exit criteria

| Check | Required |
|---|---|
| `cargo build --workspace` | ✅ |
| `cargo clippy --all-targets --all-features -- -D warnings` | ✅ |
| `cargo test -p smg policies::thunder` (now includes usage flow tests) | ✅ |
| Phase 0+P2+P3 e2e regression: 10/10 pass | ✅ |
| Worklog D-20 with autonomous tags | ✅ |
| 4-5 commits on `thunder-policy-p4` | ✅ |
