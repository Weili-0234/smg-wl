# Thunder-as-Policy Integration — Requirements & Design Context

**Status**: Pre-spec brainstorm capture. Written by Claude after long brainstorming session, just before user `/compact`. Self-contained — post-compact Claude reads this first.
**Worktree**: `/home/hkang/wl/smg-wl/` on branch `thunder-policy` tracking `lightseekorg-upstream/main` at `04f9b2d6`.
**Git remotes**: `origin = Weili-0234/smg-wl.git` (user's fork) | `lightseekorg-upstream = lightseekorg/smg.git` (official) | `colleague-fork = ergt10/smg_thunder.git` (legacy / colleague's fork containing feat/thunder branch with Phase 1-4).
**Old worktree (kept as reference)**: `/home/hkang/wl/smg_thunder/` on `feat/thunder` branch — contains old `THUNDER_PHASE5PLUS_DESIGN.md` spec (928 LOC, 70% reusable content), `routers/thunder/` module (~250 LOC, ~40% reusable forwarding logic), `e2e_test/thunder/mock_vllm.py` (290 LOC, 100% reusable), `e2e_test/thunder/test_phase{3,4}.sh` (reusable with sed substitution).

---

## 0. The Mission (1 paragraph)

Port [ThunderAgent Python algorithm](https://github.com/HaoKang-Timmy/ThunderAgent) (~3,224 LOC) into SMG (lightseekorg/smg) as a **`LoadBalancingPolicy` implementation parallel to `cache_aware`**, NOT as a separate `RoutingMode`. Thunder algorithm provides program-aware admission control + backend selection: tracks multi-step LLM agent programs, makes a tradeoff between KV cache reuse (locality) and load balance, with explicit pause/resume scheduling under capacity pressure (vs. cache_aware's heuristic fallback). Goal: maximize reuse of SMG existing infrastructure (worker registry, kv_index, circuit breaker, retry, observability), keep deployer-facing surface transparent ("just another policy"), support all client protocols (OpenAI, Anthropic, Gemini, gRPC) via SMG's universal `WorkerSelector → policy` path.

---

## 1. The Major Pivot (history)

User's colleague delivered Phase 1-4 on `feat/thunder` branch (commits `734dbbec`, `8470e953`, `0ab0b5ed`, `3758e9f0`) implementing thunder as a **separate `RoutingMode::Thunder`** with its own `routers/thunder/` module that overrides `RouterTrait::route_chat`.

After deep brainstorming, user redirected: **abandon RoutingMode approach, restart from upstream/main, integrate thunder as a `LoadBalancingPolicy`**. Reasons:
- Maximize reuse of SMG infrastructure (cache_aware policy, kv_index, kv_event_monitor, worker registry, CB, retry, health checker — Phase 1-4 bypassed all)
- Make deployer-facing surface transparent (`--policy thunder` parallel to `--policy cache_aware`)
- Universal protocol coverage: OpenAI/Anthropic/Gemini/gRPC routers ALL go through `WorkerSelector::select_worker → LoadBalancingPolicy` → one thunder policy catches all client protocols automatically
- Don't break SMG's by-default worker resilience (CB, retry, health probes)

Phase 1-4 commits are abandoned (kept in old worktree for reference). ~488 LOC of router-shaped scaffolding is throwaway; ~520 LOC reusable (mock_vllm.py + forwarding logic + bash tests).

---

## 2. Decision Log (SIGNED-OFF)

Every decision below was reached through brainstorming with user. Implementation MUST trace every algorithmic deviation to one of these tasks; deviations not on this list are unauthorized scope expansion.

### 2.1 Q1 — Concurrency: single `Arc<RwLock<RouterState>>` (FAITHFUL)

State container holds `programs: HashMap<String, Program>`, `backends: HashMap<String, BackendState>`, `waiting_queue: VecDeque<String>`, `char_to_token_ratio: f64`. **Hard rule**: no `.await` inside guard. All network I/O outside. Justification: faithful to Python asyncio cooperative semantics (Python relies on no-await-between-reads atomicity); single RwLock makes Python's atomic regions physically explicit.

### 2.2 Q2 — Observability: emit via SMG `metrics!` macros + downcast for endpoints

Numerical metrics → `smg_thunder_*` prefix via `metrics::{counter, gauge, histogram}!` macros. Thunder request lifecycles ALSO call `Metrics::record_router_request/duration/error/ttft/tpot/tokens` so thunder traffic appears in shared SMG dashboards alongside other routers. Resource endpoints `GET /thunder/programs`, `/thunder/profiles` exposed via `as_any()` downcast in `build_app`. Tracing via `tracing::info!/warn!` with structured fields aligned to SMG conventions. OTel span stays open across pause/resume. **NO `/thunder/metrics` HTTP endpoint** — all goes to existing `/metrics` Prometheus scrape.

### 2.3 Phase 5a/5b split (Q3+Q4)

Phase 5a is pure refactor / type changes (no behavior change): pytest migration of e2e tests, `RouterState` scaffolding with full Program fields, observability `describe_*!` placeholders, `MetricsClient` trait skeleton, streaming proxy mpsc-relay rewrite (now obsolete — see §2.21). Phase 5b is feature: program lifecycle, endpoints, default mode, etc. Frontload type changes; later phases only fill behavior.

NOTE: With the policy pivot, Phase 5a/5b may need re-defining (router-mode oriented). See §6.

### 2.4 Q5.1 — Resume timeout configurable (FORK, limited)

Python `_wait_for_resume` hardcodes `timeout=1800.0` (router.py:846); docstring says 20 min (wrong). Rust port: CLI flag `--thunder-resume-timeout-secs`, default `1800` (matches Python actual code, not docstring).

### 2.5 Q5.2 — Missing program_id → fallback to "default" (FAITHFUL)

Python `app.py:46` uses literal `"default"`. Rust port: same. ADD `tracing::warn!(program_id="default", "thunder request missing program_id, using shared default program — clients sharing this ID will mutate each other's state")` + counter `smg_thunder_program_id_missing_total`. Spec must call out: "this is Python reference behavior; fixing would be a fork — explicitly NOT done."

### 2.6 Q5.3 — `shared_tokens` enable + initial value (FORK + FAITHFUL)

Python defines `BackendState.update_shared_tokens()` (state.py:129) but `_scheduled_check` NEVER calls it → effective `shared_tokens ≡ 0` in Python. **Rust port: scheduler tick calls `update_shared_tokens()` each cycle**. Formula verbatim from `vllm_metrics.py:307-311`: `shared_tokens = max(0, reasoning_program_tokens − kv_cache_usage_perc × total_capacity)`. Initial value `0` on construction (faithful state.py:54). Stale on metric fetch fail: retain last successful value. **NO mitigations** (no EMA, no smoothing, no synergy claims). Three known footguns documented in spec but not mitigated.

POSSIBLE UPGRADE (open question for thunder-as-policy): replace heuristic formula with direct query to SMG's `kv_index.prefix_match_with_counts` for **real** prefix-cache savings per program. Defer to spec rewrite.

### 2.7 Q5.4 — RAII ProgramRequestGuard (FORK)

Python streaming path's `finally` block doesn't always execute on client disconnect/upstream error → program leaks in `(REASONING, ACTIVE)` indefinitely until 30-min timeout. Rust port: `ProgramRequestGuard` RAII struct returned by `RouterState::start_program_request()`. Drop runs `force_terminate_program` (idempotent). On success path, explicit `complete()` makes Drop no-op. Cleanup uses mpsc to dedicated cleanup task (avoid `block_in_place` issues; works under both single-threaded and multi-threaded tokio runtimes).

### 2.8 Q5.5 — char_to_token_ratio updated on both streaming and non-streaming (FAITHFUL)

Initial 5.0 (Python router.py:114). First sample after init: direct-assign. Subsequent: `0.2 * new + 0.8 * old` momentum. Both code paths feed `prompt_tokens` from response usage block. If `usage` absent (upstream non-compliant): skip + `tracing::debug!`.

### 2.9 Q5.6 — Multi-worker default-mode selection in Phase 5b (FAITHFUL)

Python `select_backend_for_new_program_default` chooses backend with fewest active programs. Phase placement: 5b (lifecycle phase) — per-backend program count is free byproduct of program registry, doesn't need full `BackendState`. With policy pivot, this may move into thunder policy itself (see §6).

### 2.10 Q5.7 — Detailed footgun documentation (DOC POLICY)

Spec §9 catalogs known instability sources for `shared_tokens`: (1) `kv_cache_usage_perc` instantaneous swings, (2) non-program traffic inflating `vllm_actual_used`, (3) prefix cache cross-program reuse window. Each entry: trigger, observable symptom, inspection guidance. **No mitigations** — documentation only.

### 2.11 §10.2 — RouterManager accessor (RESOLVED)

Was open question; resolved during brainstorm. With policy pivot, this concern is moot (thunder no longer registers via RoutingMode → no RouterManager downcast needed; thunder lives in policies/). Discard in spec rewrite.

### 2.12 §10.3 — Two-layer streaming token counting (FAITHFUL)

Python: mid-stream chunk-count approximation drives `on_token_progress` every 20 chunks (state.total_tokens += 20); end-of-stream `usage.total_tokens` overwrites authoritative (router.py:408 `state.total_tokens = total_tokens`). **SUPERSEDED by §2.21** — user just decided to drop mid-stream tracking entirely.

### 2.13 §1.14 (was: NOT integrating WorkerRegistry/CB) — SUPERSEDED

Original spec said thunder bypasses WorkerRegistry/CB. With policy pivot, thunder DEEPLY integrates: it IS a `LoadBalancingPolicy`, so it operates on `&[Arc<dyn Worker>]` directly, gets CB filtering automatically (via `is_available()` in `WorkerSelector`), uses kv_event_monitor / kv_index data, etc. SUPERSEDED by §3 architecture.

### 2.14 Q5.8 — Dynamic backend membership (GAP FILL + INTEGRATION)

Python defines `backends: Dict[str, BackendState]` from static config; no add/remove API. But ALGORITHM is naturally compatible with dynamic membership (BFD iterates current backends.values(), shared_tokens=0 on new = fresh capacity, BackendState.healthy already considered for selection skip).

Implementation: thunder policy subscribes to `WorkerRegistry::subscribe_events` (`worker/registry.rs:143-151`). On `WorkerEvent::Removed/Inactive`: pause all programs on that backend, transfer to global queue, BFD next tick resumes elsewhere. On `WorkerEvent::Registered/Active`: add to backends dict with `shared_tokens=0`, `cache_config=None` until first metric fetch; BFD next tick treats as fresh capacity. With policy pivot, this might be replaced by `init_workers/add_worker/remove_worker` hooks like cache_aware uses (see policies/cache_aware.rs:190-265).

### 2.15 Multi-protocol scope (S1, S4)

Support: text-only LLM clients (coding agents like codex-cli sending OpenAI Chat, claude-code sending Anthropic Messages). Both OpenAI + Anthropic + SGLang endpoints. Generation endpoints only (`/v1/chat/completions`, `/v1/messages`, `/v1/responses`, `/v1/completions`, `/v1/generate` if applicable). NON-generation endpoints (embeddings, classify, rerank, audio_transcriptions, realtime_*) are out of scope — pass through without thunder logic. Shared program registry: same `program_id` from different protocols = same program (canonical ID space).

### 2.16 Architectural pivot: thunder = LoadBalancingPolicy parallel to cache_aware

The fundamental structural choice. All decisions below are derived from this:

- Thunder algorithm logic lives in `policies/thunder.rs` implementing `LoadBalancingPolicy` trait (with extension §2.17)
- Deployer config: `--policy thunder` parallel to `--policy cache_aware`
- Universal coverage: ALL routers (OpenAI/Anthropic/Gemini/gRPC/HTTP) call `WorkerSelector::select_worker(...)` which delegates to the configured policy → thunder catches all
- ThunderPolicy holds `Arc<RwLock<RouterState>>` internally (Q1 single-state approach)
- ThunderPolicy holds Arc to KvEventMonitor (set via `set_kv_event_monitor()` mirror cache_aware pattern)
- ThunderPolicy holds dynamic membership via `init_workers/add_worker/remove_worker` hooks (mirror cache_aware)

### 2.17 Q1 — LoadBalancingPolicy trait extension (SIGNED-OFF #19)

Add `async fn select_worker_async` with default impl falling back to sync `select_worker`:

```rust
#[async_trait]
pub trait LoadBalancingPolicy: Send + Sync + Debug {
    fn select_worker(&self, workers: &[Arc<dyn Worker>], info: &SelectWorkerInfo) -> Option<usize>;

    // NEW: async variant with default fallback
    async fn select_worker_async(&self, workers: &[Arc<dyn Worker>], info: &SelectWorkerInfo) -> Option<usize> {
        self.select_worker(workers, info)
    }

    fn name(&self) -> &'static str;
    fn needs_request_text(&self) -> bool;
    fn on_request_complete(&self, worker_url: &str, success: bool);
    fn as_any(&self) -> &dyn std::any::Any;
}
```

8 existing policies (consistent_hashing, power_of_two, prefix_hash, manual, cache_aware, round_robin, random, bucket) need 0 changes. ~10-13 caller sites (mostly in already-async functions) need `.select_worker_async(...).await` migration. async-trait already heavily used in repo (RouterTrait, AnthropicRouter, GeminiRouter, GrpcRouter etc., per `mod.rs:70` and others) — pattern is with-the-grain.

### 2.18 Q3 — SelectWorkerInfo extension with program_id (SIGNED-OFF #20)

Extend `pub struct SelectWorkerInfo<'a>` (policies/mod.rs:167) with `pub program_id: Option<&'a str>`. 8 existing policies don't consume it (no changes). ~10 caller sites pass `None` except in thunder policy mode where routers extract program_id from request body before `WorkerSelector` call.

Open implementation question: should each router (openai, anthropic, gemini, ...) extract program_id, or should there be a shared utility? Likely: each router has 1-3 lines of body extraction code added (extract from `body.program_id` or `body.extra_body.program_id` for OpenAI; from `body.metadata.program_id` for Anthropic; etc.). To be detailed in spec.

### 2.19 Q2 — Clean restart (SIGNED-OFF #18)

`feat/thunder` branch + Phase 1-4 commits abandoned. New worktree at `/home/hkang/wl/smg-wl/` on `thunder-policy` branch from `lightseekorg-upstream/main` (currently `04f9b2d6`). New origin = `Weili-0234/smg-wl.git` (user's fork). Code reuse from old worktree:
- **100% reuse**: `e2e_test/thunder/mock_vllm.py` (290 LOC, vLLM mock with `/get_server_info`, `/control/capacity`, `/control/state`, controllable streaming) — copy verbatim into new worktree
- **~40% reuse**: `routers/thunder/proxy.rs` (forwarding logic without RouterTrait override) — refactor into thunder policy
- **Bash tests** (`test_phase3.sh`, `test_phase4.sh`): sed replacement of `--backend thunder` → `--policy thunder`, otherwise reusable

CLI migration: `--backend thunder --worker-urls ...` → `--policy thunder` (worker URLs go through SMG's standard worker registration path; no `--worker-urls` flag for thunder needed — thunder gets workers via WorkerRegistry membership).

### 2.20a Cross-protocol capacity counting — DEPLOYMENT ASSUMPTION + DOUBLE-CHECK REQUIRED

**User-stated assumption (sign-off pending verification)**: "我们假设所有 backend 都同时支持两种协议" (we assume every backend simultaneously serves both OpenAI and Anthropic endpoints — typically via litellm-proxy sidecar or vLLM with anthropic adapter, dual-registered in WorkerRegistry as same URL with different `default_provider`).

**Status: DOUBLE-CHECK REQUIRED in spec implementation phase.** Spec must explicitly state this assumption upfront and validate during Phase 1-2 implementation:

1. **Open verification questions** that must be resolved with concrete evidence (file:line + e2e test) before locking in this assumption:
   - **Q-DC1**: Does WorkerSelector pre-filter (`worker_selection.rs:117, 140`) by provider behave correctly when same-URL workers are double-registered with different `default_provider`? Specifically: do BOTH worker entries appear in candidate slice when the request matches one provider, or only the matching one? **Expected: only matching one** (correct behavior — thunder gets a slice that respects protocol).
   - **Q-DC2**: Does thunder's `BackendState` keyed-by-URL aggregation correctly cross-protocol when workers are double-registered? Specifically: when ProgramA (OpenAI) and ProgramB (Anthropic) both running on physical backend X, does `BackendState["http://x:8000"].active_program_tokens` correctly sum both?
   - **Q-DC3**: BFD resume target selection — when Program P is paused and BFD plans to resume it, does thunder filter candidate backends by P's `required_provider`? If not, P could be resumed onto a backend that doesn't serve its protocol → 404 at forward time.
   - **Q-DC4**: Per-program protocol affinity — does Program need a new `required_provider: Option<ProviderType>` field? Implementation decision is YES (this is a SIGNED-OFF deviation from Python — Python is single-protocol so didn't need this).
   - **Q-DC5**: Per-backend supported_providers tracking — does BackendState need `supported_providers: HashSet<ProviderType>` updated by WorkerRegistry events? Implementation decision is YES.

2. **Will-fail scenarios** to document in spec under "Known limitations / footguns":
   - **F-DC1**: Operator double-registers same physical backend with **different URLs** (e.g., `http://x:8000/openai` vs `http://x:8000/anthropic`) instead of same URL with different `default_provider`. Then thunder's `BackendState` keyed by URL sees TWO independent backends → capacity miscounted (split into two pools instead of aggregated). **Anti-pattern; spec deployment guide must forbid**.
   - **F-DC2**: Operator registers backend X with only ONE protocol (e.g., only OpenAI). An Anthropic client tries to use X via thunder. WorkerSelector pre-filter excludes X from Anthropic candidate slice → thunder never sees X for Anthropic requests → X's capacity, even if low, doesn't help Anthropic load. This is **correct behavior given the registration data**, not a bug, but spec must be clear: "to enable cross-protocol load balancing, every dual-protocol backend MUST be double-registered."
   - **F-DC3**: One protocol's clients monopolize backend X's capacity → other protocol's clients on X get crowded out. Thunder's BFD pause/resume DOES correctly arbitrate this (capacity is jointly enforced cross-protocol), but resume migration limited to backends that support the program's protocol. Need to document and verify with multi-tenant load test.
   - **F-DC4**: Backend X is dual-registered with same URL but ONE registration is removed (e.g., one protocol service deployed/undeployed independently). Thunder's BackendState keyed by URL keeps X alive (because the other registration still exists), but `supported_providers` should be updated to drop the gone protocol. Verify event handling.

3. **Spec must contain prominent banner section** in §1 (Decision log) and §3 (Architecture) saying:
   ```
   ⚠ ASSUMPTION (double-check during implementation): All backends are dual-registered as
     OpenAI + Anthropic providers under the SAME URL. Thunder relies on this for
     cross-protocol capacity counting. Single-protocol backends are supported but won't
     participate in cross-protocol load balancing.
   ```

4. **Phase 1 implementation must include an e2e test** that exercises:
   - Two backends double-registered (each with both providers)
   - One OpenAI client + one Anthropic client running concurrently
   - Verify `/thunder/programs` and Prometheus metric show cross-protocol aggregated capacity per backend
   - Force backend overflow via mock `/control/capacity`; verify pause/resume picks protocol-correct backends

### 2.20 Q3 — Streaming tracking simplification (SIGNED-OFF #21)

Drop mid-stream every-20-chunk `on_token_progress` callback. Keep only end-of-stream `usage` parsing → `update_program_after_request` overwrites `Program.total_tokens` with authoritative value.

Implications:
- No mpsc relay token counter needed (the §2.3 5a streaming proxy rewrite is no longer required for thunder; SMG's existing streaming pass-through is fine)
- §9.4 footgun (mid-stream undercount under spec decoding / stream_interval > 1) becomes irrelevant
- `/thunder/programs` mid-stream view shows `total_tokens` based on initial estimate (char_to_token_ratio prediction); updates only at end-of-stream — acceptable
- BFD scheduler ticks may run while streams are in flight, using estimated total_tokens. This is fine — when stream ends, real total overwrites estimate; scheduler self-corrects next tick

Hook mechanism for end-of-stream usage: thunder policy registers a callback (or uses an internal channel) that streaming responses notify when complete. Open implementation question — see §6 next-steps.

---

## 3. SMG Architecture Findings (verified during brainstorm)

### 3.1 Three-path architecture (per `docs/concepts/architecture/overview.md`)

```
RouterManager → 3 paths based on worker type:
1. gRPC Path        — SGLang/vLLM/TRT-LLM via gRPC; gateway tokenization, chat templates, tool parse
2. HTTP Path        — OpenAI-compatible HTTP; "Regular HTTP mode" or "PD (Prefill-Decode) mode"
3. 3rd Party Path   — External APIs (OpenAI, Anthropic, Gemini, xAI, Together AI, OpenRouter, Bedrock, OCI)
```

ALL paths converge at the same load balancing infrastructure — call `WorkerSelector::select_worker` which delegates to the configured `LoadBalancingPolicy`.

### 3.2 LoadBalancingPolicy trait (`model_gateway/src/policies/mod.rs:44`)

```rust
pub trait LoadBalancingPolicy: Send + Sync + Debug {
    fn select_worker(&self, workers: &[Arc<dyn Worker>], info: &SelectWorkerInfo) -> Option<usize>;
    fn name(&self) -> &'static str;
    fn needs_request_text(&self) -> bool;
    fn on_request_complete(&self, worker_url: &str, success: bool);
    fn as_any(&self) -> &dyn std::any::Any;
}
```

8 implementations: `consistent_hashing.rs:181`, `power_of_two.rs:33`, `prefix_hash.rs:224`, `manual.rs:216`, `cache_aware.rs:595`, `round_robin.rs:27`, `random.rs:22`, `bucket.rs:186`.

### 3.3 SelectWorkerInfo (`policies/mod.rs:167`)

```rust
pub struct SelectWorkerInfo<'a> {
    pub request_text: Option<&'a str>,
    pub tokens: Option<&'a [u32]>,
    pub headers: Option<&'a http::HeaderMap>,
    pub hash_ring: Option<Arc<HashRing>>,
}
```

→ Will be extended with `pub program_id: Option<&'a str>` (Q3).

### 3.4 cache_aware as the model to follow (`policies/cache_aware.rs`)

Methods beyond trait that thunder will mirror:
- `init_workers(&workers)` (line 190) — bootstrap from registry
- `add_worker(&worker)` (line 223), `remove_worker(&worker)` (line 265) — dynamic membership
- `set_kv_event_monitor(&self, monitor)` (line 184) — receive KV events from backends
- `set_mesh_sync(&self, mesh_sync)` (line 175) — multi-instance state sync (thunder may not need)

Holds rich state internally (radix tree per model). Uses `DashMap` and atomic operations for lock-free reads.

### 3.5 KV-aware infrastructure (REUSE TARGETS)

- **`crates/kv_index/`** — RadixTree trait with `prefix_match()` and `prefix_match_with_counts()` (lib.rs:36-60). Thunder can query this for real prefix-cache savings per program (potential Q5.3 upgrade).
- **`worker/kv_event_monitor.rs`** — gRPC subscribe to backend's `subscribe_kv_events` (line 368). PositionalIndexer per-model with block_size. Backend (vLLM/SGLang) must expose this.
- **`worker/hash_ring.rs`** — consistent hashing for sticky routing.
- **`worker/circuit_breaker.rs`** — `Closed/Open/HalfOpen` state machine. Atomic-stored state. Thunder should integrate (via `Worker::is_available()` filtering candidates).
- **`worker/metrics_aggregator.rs`** — per-worker load tracking.
- **`service_discovery.rs`** — auto worker discovery (k8s, etc.).
- **`worker/monitor.rs`** — periodic health probes.

### 3.6 Resilience guarantees thunder must NOT break

Per `docs/concepts/architecture/overview.md`:
- Circuit Breaker stops routing to failing workers
- Retry Handler retries with exponential backoff (`routers/common/retry.rs::RetryExecutor`, used by openai router but NOT in thunder's pause/resume path — streaming retry would duplicate output)
- Health Checker periodic worker probes
- Timeout Manager request and connection timeouts

Thunder integration: by being a policy, thunder automatically benefits from CB filtering (workers filtered by `is_available()` before being passed to `select_worker`). Thunder does NOT use RetryExecutor (pause/resume replaces retry semantics).

### 3.7 Worker resilience reality (verified)

- `WorkerSelector::select_worker` (`routers/common/worker_selection.rs:73`) is **already async**
- `worker_selection.rs:114` filters `candidates.filter(|w| w.is_available())` before policy sees them
- `is_available() = is_healthy() && circuit_breaker_can_execute()` (`worker/worker.rs:222-224`)
- openai router retries SAME worker (does NOT do mid-request failover) — thunder's pause/resume does program-state-preserving migration which is strictly stronger
- CB triggers route-around for NEW requests once Open; in-flight requests at trip moment may still fail

### 3.8 Anthropic router (`routers/anthropic/`) — NO translation

Verified: AnthropicRouter is pure Anthropic-end-to-end passthrough. It assumes backend serves `/v1/messages`. There is **no** anthropic→openai or openai→anthropic translation function in SMG. (`worker.rs:54` POSTs `&CreateMessageRequest` directly; line 31 hits `{worker.url()}/v1/messages`.) MCP tool calls via `mcp.rs` are anthropic-native (input_json_delta accumulation).

→ Thunder-as-policy works for Anthropic clients ONLY IF backend speaks `/v1/messages`. User's intended deployment: each backend exposes BOTH OpenAI and Anthropic endpoints (via litellm-proxy or similar). Thunder forwards to backend's matching endpoint per client protocol.

### 3.9 OpenAI router has Provider abstraction (`routers/openai/provider/`)

Inside openai router there's a `Provider` trait (`provider/provider_trait.rs:8`) with `transform_request(payload, endpoint)`. Two impls: `OpenAIProvider`, `AnthropicProvider`. Used to transform incoming OpenAI client requests → different backend formats. **OpenAI client → Anthropic backend** is supported by openai router; but **Anthropic client → OpenAI backend** is NOT supported anywhere.

For thunder's mission, this asymmetry doesn't directly matter — thunder doesn't need to translate; it just needs to make routing decisions.

### 3.10 Worker resilience caller pattern (real numbers)

Real `policy.select_worker(...)` direct calls (excluding bucket.rs internal delegation and tests):
- `routers/grpc/common/stages/worker_selection.rs:157, 276, 277` (3 sites)
- `routers/http/router.rs:175, 568` (2 sites)
- `routers/http/pd_router.rs:861` (1 site)

External callers go through `WorkerSelector::select_worker(...)` (already async). Total real call sites that need `.await` migration: **~6-13** (lower bound = direct policy.select_worker; upper bound includes bucket.rs internal delegation).

### 3.11 async-trait already adopted

`#[async_trait]` is used in repo for: RouterTrait (`routers/mod.rs:70`), AnthropicRouter, GeminiRouter, GrpcRouter, RouterManager, middleware/tenant_resolution. Adding async to LoadBalancingPolicy is consistent with established pattern.

---

## 4. Algorithm Core (faithful to Python ThunderAgent, modulo signed-off forks)

Reference: `/home/hkang/wl/smg_thunder/ThunderAgent/ThunderAgent/` (read-only).

### 4.1 Program lifecycle (orthogonal axes)

`Program { program_id, status: REASONING|ACTING, state: ACTIVE|PAUSED|TERMINATED, ... }`

Transitions:
- Create → `(REASONING, ACTIVE)`
- After response → ACTING, `acting_since=Instant::now()`
- Next request → REASONING, `acting_since=None`
- Pause (ACTING only) → PAUSED, save `origin_backend`, register `waiting_event`
- Mark for pause (REASONING) → `marked_for_pause=True`, add tokens to `backend.future_paused_tokens`. **REASONING programs are NEVER paused mid-generation** — only marked, deferred until request completes
- Resume → ACTIVE, may migrate to different backend
- Terminate → unregister

### 4.2 Per-backend state

```
BackendState {
    url,
    shared_tokens (0 initially, updated each scheduler tick — Q5.3 fork enables this call),
    future_paused_tokens,
    cache_config (from /get_server_info),
    latest_metrics,
    healthy,
    metrics_client,
}
```

Capacity formulas (verbatim from Python `backend/state.py`):
- `active_program_tokens = reasoning_tokens + tool_coefficient * acting_tokens`
- `remaining_capacity = total_kv_capacity - (active_tokens - shared_tokens + active_count * BUFFER_PER_PROGRAM(=100))`
- `remaining_capacity_with_decay`: ACTING tokens weighted by `2^(-t)` where `t = now - acting_since` (Phase 12 toggle via `--use-acting-token-decay`)

### 4.3 Scheduler tick (every 5s)

```
1. fetch_metrics() per backend (NETWORK — outside guard)
2. For each backend with successful fetch: backend.update_shared_tokens()  ← Q5.3 fork (added call)
3. _greedy_resume()  (BFD)
4. For each backend with remaining_capacity() < 0: _pause_until_safe(backend)
```

### 4.4 BFD greedy resume (verbatim Python)

Priority groups: `reasoning_step>1` > `new_step==1` > `acting`. Within each group: ascending by tokens.
1. Compute per-backend remaining (with decay if enabled). Skip if rem ≤ BUFFER. Sum total_capacity.
2. Walk candidates, include if cumulative + required ≤ total_capacity.
3. Re-sort selected DESC by tokens; backends DESC by remaining.
4. Place each program on backend[0]; pop backend if remaining ≤ BUFFER; re-sort.

### 4.5 _pause_until_safe (verbatim Python)

While `remaining_capacity() < 0`:
- Pause smallest ACTING program (PAUSED state).
- If no ACTING: mark smallest REASONING for future pause (sets `marked_for_pause=True`, adds tokens to `future_paused_tokens`; actual PAUSED transition deferred until request completes).

### 4.6 char_to_token_ratio momentum

Initial 5.0. First sample: direct-assign. Subsequent: `0.2 * new + 0.8 * old`. Used to estimate `total_tokens = prompt_chars / ratio` BEFORE response arrives, for new-program admission (Phase 7 TR mode).

### 4.7 30-min force-resume timeout

`_wait_for_resume(timeout=1800.0)` per Python (Q5.1: configurable). On timeout, force-resume via `select_backend_for_new_program_default` (least-loaded by program count).

### 4.8 Sub-modes

- **default**: scheduling_enabled=False. No scheduler task, no capacity gate, no pausing. Pure proxy with program tracking + multi-worker load balance.
- **TR**: scheduling_enabled=True. Full scheduler, BFD, pause/resume, capacity admission.

CLI flag: `--thunder-sub-mode {default|tr}`. (Both modes use thunder POLICY but TR adds scheduler + admission.)

---

## 5. Architecture Sketch: Thunder-as-Policy

```
                       Client (codex-cli, claude-code, etc.)
                                  │
                                  ▼
                        ┌─ Gateway Layer ─┐
                        │ Rate limiter, auth, WASM, request_id, metrics, OTel
                        └────────┬────────┘
                                 ▼
                     ┌─ Router Manager ─┐
                     │  routes by worker type to:
                     │   • OpenAI router   (3rd party path or HTTP path)
                     │   • Anthropic router (3rd party path)
                     │   • Gemini router    (3rd party path)
                     │   • HTTP router      (HTTP path; regular or PD)
                     │   • gRPC router      (gRPC path)
                     └────────┬─────────┘
                              ▼
                  Each router: (1) parse request body
                                  (2) extract program_id (NEW: per-router minor change)
                                  (3) build SelectWorkerInfo with program_id
                                  (4) WorkerSelector::select_worker (already async)
                                       │
                                       ▼
                            ┌─ ThunderPolicy::select_worker_async ─┐
                            │  (NEW trait method, default fallback)│
                            │                                       │
                            │  state.write() guard:                │
                            │    program = get_or_create(program_id)│
                            │    update lifecycle (REASONING etc.)   │
                            │                                       │
                            │  if sub_mode == TR:                    │
                            │    estimate tokens                     │
                            │    capacity check                      │
                            │    if no capacity:                    │
                            │      enqueue + drop guard +            │
                            │      .await Notify (with timeout)     │
                            │                                       │
                            │  Backend selection:                    │
                            │    query kv_index (or shared_tokens)   │
                            │    apply tradeoff (cache vs. balance)  │
                            │    return Some(idx)                    │
                            └────────────────┬───────────────────────┘
                                             ▼
                                   forward to worker[idx]
                                             ▼
                              streaming response (passes through)
                                             ▼
                              router parses end-of-stream usage
                              → notifies thunder policy via
                                shared state callback (NOT trait method)
                                ← simplified per Q5.20
                                             ▼
                              ProgramRequestGuard::Drop (RAII)
                              cleanup on cancel/error paths
```

Background:
- Thunder policy spawns scheduler tick task on construction
- Subscribes to `WorkerRegistry::subscribe_events` for membership
- Subscribes to `kv_event_monitor` for KV cache state per backend

---

## 6. Open Implementation Questions (defer to spec rewrite)

These are NOT blockers for spec; they are concrete details to nail in spec:

1. **End-of-stream usage hook** — how does thunder policy receive `usage.total_tokens` after streaming ends?
   Options: (a) policy holds Arc<RouterState>; each router's streaming code clones the Arc and notifies on stream end; (b) generic policy method `on_stream_complete(program_id, usage)` added to trait; (c) per-router callback registry. Pick in spec.

2. **Per-router program_id extraction** — each router (openai, anthropic, gemini, ...) needs to extract program_id from its own request type and pass into SelectWorkerInfo. Where does this code live? Likely small helper functions per router.

3. **kv_index vs shared_tokens** — should thunder use SMG's authoritative kv_index data instead of (or in addition to) Python's `shared_tokens` heuristic? This is an UPGRADE from Python; needs sign-off as a fork.

4. **`/thunder/programs` and `/thunder/profiles` endpoints** — policy can't directly expose HTTP endpoints (policies aren't routers). Options:
   - Mount thunder admin routes globally in `build_app` if active policy is thunder (downcast policy via PolicyRegistry)
   - Generic `/admin/policy/{name}/...` endpoint pattern
   Pick in spec.

5. **PolicyFactory integration** — `policies/factory.rs::create_from_config` (line 17) and `create_by_name` (line 79) need a `Thunder` arm. Configuration form: same shape as cache_aware? Or thunder-specific config fields?

6. **Anthropic body program_id extraction** — Anthropic `CreateMessageRequest` doesn't have a top-level `program_id` field. Where does anthropic-using clients put program_id? `metadata.program_id`? `extra_body.program_id`? Need to check what claude-code-style clients actually send and document.

7. **Phase plan re-derivation** — original spec's 12-phase plan assumed RouterTrait approach. With policy approach, phase boundaries shift. Suggested fresh phase plan:
   - **Phase 1**: trait extension (`async fn select_worker_async`) + SelectWorkerInfo extension. All 8 existing policies use default fallback. ~10-13 caller sites migrate. Verify `cargo build --workspace && cargo test --workspace` pass.
   - **Phase 2**: copy `e2e_test/thunder/mock_vllm.py` from old worktree; build pytest fixtures.
   - **Phase 3**: `policies/thunder.rs` skeleton + `RouterState` data model (full Program fields) + PolicyFactory wiring.
   - **Phase 4**: Default sub-mode lifecycle (program tracking, multi-worker selection by least active count, char/token ratio, end-of-stream usage hook).
   - **Phase 5**: Backend metrics + capacity (kv_event_monitor integration; `shared_tokens` updated each scheduler tick — Q5.3 fork).
   - **Phase 6**: TR sub-mode admission (capacity check, 503 on full).
   - **Phase 7**: Pause/resume + BFD scheduler + force-timeout.
   - **Phase 8**: SGLang/SkyRL backends.
   - **Phase 9**: Profiling.
   - **Phase 10**: Polish (acting-token weight, decay).

---

## 7. Things NOT to Break (must preserve)

From `docs/concepts/architecture/overview.md` and brainstorm:

- All existing endpoints continue working (`/v1/chat/completions`, `/v1/completions`, `/v1/responses`, `/v1/embeddings`, `/v1/rerank`, `/messages`, `/tokenize`, `/v1/models`, `/health`, `/metrics`, `/ws/metrics`, etc.)
- Multi-tenant rate limiter, OIDC auth, WASM plugins continue working
- All 8 existing LB policies continue working with their existing behavior (default sync `select_worker`)
- Cache-aware policy (production default) continues working
- Worker registry, service discovery, CB, retry, health checker remain functional
- Prometheus metric names: existing `smg_*` metrics not touched; thunder adds new `smg_thunder_*` series only
- `make pre-commit` (fmt + check + test) passes
- `cargo clippy --all-targets --all-features -- -D warnings` passes (workspace strict clippy with `unwrap_used` denied / `expect_used` warned)
- 3rd party path (anthropic / gemini / external providers) continues working — thunder enhances, doesn't replace
- gRPC path continues working

---

## 8. Reference: Old Worktree Reusable Assets

Path: `/home/hkang/wl/smg_thunder/`

| Asset | Old Path | LOC | Reuse % | Action |
|---|---|---|---|---|
| Mock vLLM backend | `e2e_test/thunder/mock_vllm.py` | 290 | 100% | Copy verbatim to new worktree (`e2e_test/thunder/mock_vllm.py`); already supports `/get_server_info`, `/control/capacity`, `/control/state`, controllable streaming |
| Streaming forwarding (mpsc-relay template) | `routers/openai/chat.rs:194-220` (in NEW worktree, official SMG code) | ~25 | reference | Pattern to follow if thunder needs custom forwarding |
| Old non-streaming forward | `routers/thunder/proxy.rs::forward_non_streaming_chat` | 71 | ~50% | Lift core POST + filtered_headers logic |
| Old streaming forward | `routers/thunder/proxy.rs::forward_streaming_chat` | 72 | ~30% | Most logic supplied by SMG existing streaming pass-through |
| e2e bash tests | `e2e_test/thunder/test_phase{3,4}.sh` | ~210 | 80% | sed `--backend thunder` → `--policy thunder`; reuse curl assertions |
| Old spec | `THUNDER_PHASE5PLUS_DESIGN.md` | 928 | 70% content | Reference for: §1 decision log mapping, §3 concurrency rules, §4 data model, §6 BFD pseudocode, §9 footguns, §11 testing strategy, §12 glossary. Don't copy structure; rewrite. |

Python reference (read-only):
- `/home/hkang/wl/smg_thunder/ThunderAgent/ThunderAgent/`
- Key files: `scheduler/router.py:78-961`, `program/state.py:33-47`, `backend/state.py:40-278`, `backend/vllm_metrics.py:285-343`, `scheduler/vllm_request_processor.py:101-217`, `app.py:46-225`

Foreign source (read-only verification):
- `/home/hkang/wl/smg_thunder/vllm/` — vLLM source for streaming chunk format verification
- `/home/hkang/wl/smg_thunder/sglang/` — SGLang source same purpose

---

## 9. Post-compact Continuation Checklist

After user `/compact`, NEW Claude reads this file FIRST. Then:

1. **Acknowledge** restart context — no re-confirmation of decisions; everything in §2 is sign-off ground truth.

2. **Run two delayed investigations** (these were rate-limited during the original brainstorm; need to be done fresh):
   - **(2a) User-facing surface audit**: in NEW worktree (`/home/hkang/wl/smg-wl/`), read `model_gateway/src/main.rs` CLI struct, `model_gateway/src/config/types.rs` config schema, `model_gateway/src/server.rs:build_app` endpoint list, `model_gateway/src/observability/metrics.rs:151+` describe_* list. List 5-10 "must not break" items per §7 to confirm thunder-as-policy preserves them all. Flag any tension.
   - **(2b) Detailed thunder-policy implementation design**: write concrete Rust pseudocode for: `LoadBalancingPolicy` trait extension; `ThunderPolicy` struct with internal state; `select_worker_async` body; PolicyFactory integration; SelectWorkerInfo extension; per-router program_id extraction helpers; end-of-stream usage hook mechanism; spawn-and-cleanup for scheduler task. Cite file:line for every existing-code reference. Flag unforeseen issues.

3. **After investigations**, write the new spec at `/home/hkang/wl/smg-wl/THUNDER_POLICY_DESIGN.md` (committable). It should:
   - Reuse ~70% content from old spec at `/home/hkang/wl/smg_thunder/THUNDER_PHASE5PLUS_DESIGN.md` (algorithm semantics, Q5.x decisions, BFD pseudocode, footguns, testing strategy)
   - Replace structural sections (architecture, phase plan, endpoint exposure) with policy-oriented design from §5 + investigation results
   - Have explicit decision log linking every algorithmic deviation to a SIGNED-OFF task ID from §2

4. **Open Q5.3 upgrade question** — should thunder use kv_index for real prefix-cache savings instead of/alongside Python's `shared_tokens` heuristic? Discuss with user before signing off as new fork.

5. **Open Q on phase plan** — propose phase plan from §6.7 to user, get sign-off, then proceed to implementation.

6. **Implementation**: each phase is one commit with `feat(thunder): <summary> (Phase N)` message. Per-phase contract: cargo build/test/clippy pass + e2e validation under `e2e_test/thunder/`.

---

## 10. Glossary

| Term | Meaning |
|---|---|
| Thunder | Program-aware policy with admission control + backend selection |
| Program | Long-lived multi-step LLM agent task identified by `program_id` |
| Status | REASONING (on GPU) vs ACTING (off GPU, between requests) |
| State | ACTIVE (on backend) vs PAUSED (queued for resume) vs TERMINATED |
| Sub-mode | `default` (no scheduling) vs `tr` (full pause/resume) |
| BFD | Best Fit Decreasing — bin-packing for greedy resume |
| `shared_tokens` | Prefix cache savings per backend (Python: heuristic; Rust port may upgrade to kv_index real data) |
| `acting_token_weight` (`tool_coefficient`) | Multiplier for ACTING tokens in `active_program_tokens` formula |
| `acting_token_decay` | `2^(-t)` weighting on ACTING tokens (Phase 10 toggle) |
| `char_to_token_ratio` | Global momentum-blended ratio for token estimation; initial 5.0 |
| Mark-for-pause | REASONING never paused mid-generation; marked, defer to request end |
| `origin_backend` | Where program ran before pause; used as resume target hint |

---

## 11. Closing Note for Post-compact Claude

This spec is the result of 7 hours of dense brainstorming with user (algorithm author). Every decision in §2 is hard-earned and signed-off. **Do NOT relitigate**. Use §6 to identify what's open and §9 to pick up cleanly.

User preferences (from CLAUDE.local.md):
- Rust newcomer, SMG newbie — explain Rust idioms briefly when introducing
- Wants design tradeoffs explained before implementation
- Wants plan before non-trivial edits
- Communication: concise, structured, decision-oriented
- Will compact session right after reading this; explanatory mode is ON
- Auto mode is ON but does NOT bypass the brainstorming HARD-GATE — design must be approved before implementation

This file is checked in as a working document. After spec is complete and signed-off, this file can be deleted (or moved to `docs/decisions/` for archive).
