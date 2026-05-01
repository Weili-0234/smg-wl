> Part of the [Thunder Policy spec](00-INDEX.md). Companion: [worklog](worklog.md) — design decisions with revisit conditions.

# Thunder — Footguns & Known Limitations

## 10. Footguns and known limitations

### 10.1 Q5.3 `kv_cache_usage_perc` instantaneous swings

**Trigger**: vLLM reports `kv_cache_usage_perc` as instantaneous gauge. Bursty traffic causes 30-60% swings within seconds.

**Effect**: `shared_tokens` swings inversely → BFD admits more → next tick pauses → thrashing.

**Observable**: `smg_thunder_pause_total` and `smg_thunder_resume_total` both incrementing every tick; `smg_thunder_backend_shared_tokens` peak/trough ratio > 2 within 30s.

**Inspection**: dump `/thunder/programs` for 30s; same `program_id` flipping ACTIVE↔PAUSED. Future mitigation candidate: EMA smoothing of `kv_cache_usage_perc`. **Do not implement without separate brainstorm.**

### 10.2 Q5.3 non-program traffic inflating `vllm_actual_used`

**Trigger**: external clients sharing the same vLLM backend → `kv_cache_usage_perc × total_capacity > reasoning_program_tokens` → `shared_tokens = 0` (clamped).

**Inspection**: `curl <backend>/metrics | grep kv_cache_usage`; if higher than `smg_thunder_backend_kv_capacity_used / total`, external clients are sharing. Operationally: dedicated vLLM backends per thunder cluster avoid this.

### 10.3 Q5.3 prefix cache cross-program reuse window

**Trigger**: vLLM's prefix cache retains evicted blocks briefly; B starts with prefix sharing with just-finished A → temporal mismatch.

**Effect**: brief `shared_tokens` over-estimation immediately after program A's completion (benign, transient).

**Inspection**: correlate `smg_thunder_backend_shared_tokens` time series with program lifecycle events; spikes aligning with completions → benign.

### 10.4 OpenAI Chat usage requires `include_usage`

**Trigger**: client doesn't send `stream_options.include_usage = true` → no usage chunk in stream → no UsageEvent → thunder doesn't update `total_tokens` or `char_to_token_ratio` for that request.

**Mitigation (P3 — already signed off via §2.19)**: gateway-side injection of `include_usage = true` when policy is thunder, faithful to Python `vllm_request_processor.py:138-143`.

### 10.5 Sidecar dependency for `/v1/messages` and `/v1/responses` parity

Backend MUST run a sidecar (litellm-proxy or equivalent) that exposes all three generation endpoints. If sidecar absent, SMG forwards `/v1/messages` to a backend port that doesn't speak it → 404/501 from upstream. **Deployment runbook must include sidecar setup**. e2e test fixture `mock_vllm.py` is extended to accept `/v1/messages` and `/v1/responses` paths so SMG-side pass-through is testable without a real sidecar (Phase 0).

**Sidecar mount-path invariant**: SMG's `worker.endpoint_url(route)` builds the URL as `format!("{}{}", base_url, route)` (see `worker/worker.rs:444`). With `worker_url = http://localhost:8011`, request to `/v1/messages` becomes `http://localhost:8011/v1/messages`. The sidecar MUST mount its three endpoints at the **root** of the worker URL (no `/anthropic` or `/proxy` prefix). litellm-proxy's default config does this; deployment runbook must verify (or set `--worker-urls` to include the sidecar prefix path explicitly, e.g. `--worker-urls http://localhost:8011/proxy`).

**Sidecar-induced behaviors verified during litellm-proxy code audit** (refs: `litellm/llms/anthropic/experimental_pass_through/messages/handler.py:294-303`, `litellm/types/llms/anthropic_messages/anthropic_request.py:6-13`, `litellm/llms/openai/openai.py:1187`):

- **`metadata.program_id` is stripped by `AnthropicMetadata` Pydantic whitelist** when sidecar handles `/v1/messages` (only `user_id` survives). **Not a thunder problem**: thunder reads `body.metadata.program_id` at the SMG router entry (before forwarding), so the strip only affects what's seen downstream by vLLM (which doesn't need it). SMG-internal pipelines pass `program_id` via closure capture into the `usage_tail` wrapper — no dependency on sidecar preservation.
- **`stream_options.include_usage` is NOT auto-injected by litellm for vLLM api_base** (auto-inject only happens for `api.openai.com`). The gateway-side injection in §5.6 is **mandatory**, not a convenience.
- **OpenAI Responses API: `previous_response_id` and `store=true` require litellm session storage** (Postgres/Redis). Without it, those features silently degrade (handler logs warning, falls back to non-persistent input). Out of scope for thunder; deployment runbook should document.

### 10.6 External workers (`RuntimeType::External`) bypass policy

SMG's 3rd-party-style routers (`HTTP_OPENAI`/`HTTP_ANTHROPIC`/`HTTP_GEMINI`) call `WorkerSelector::find_best_worker` (`routers/common/worker_selection.rs:122-127`) which uses hardcoded `min_by_key(|w| w.load())`. **Thunder is not invoked when workers are registered as `RuntimeType::External`**. Operators wanting thunder over external API providers (api.openai.com, api.anthropic.com) need a separate FORK to refactor `WorkerSelector` — explicitly out of scope for this work.

### 10.7 Request-timeout / resume-timeout interaction

SMG `--request-timeout-secs` (default 1800) and `--thunder-resume-timeout-secs` (default 1800) share the same value. If thunder waits the full 1800s and force-resumes + forwards, the outer SMG request timeout fires concurrently → race. **Deployment constraint**: `request_timeout_secs ≥ thunder_resume_timeout_secs + 60s`. Implementation: validate at startup (`PolicyConfig::Thunder` constructor takes a reference to `RouterConfig.request_timeout_secs` and refuses if violated). Or document and recommend lowering `--thunder-resume-timeout-secs` to e.g. 1500.

### 10.8 Performance: single `RouterState` mutex (D-3)

See §5.11. If contention surfaces in benchmarks, FIRST upgrade to `parking_lot::RwLock`; only THEN consider lock-splitting as separate FORK.

### 10.9 retry × pause/resume — thunder internal idempotency (D-9)

`routers/http/router.rs:281-290` runs `select_worker_for_model` inside `RetryExecutor`'s closure (`route_typed_request_once`), so each retry attempt re-enters `policy.select_worker_async`. Thunder solves this **internally** by detecting `program.status == REASONING && state == ACTIVE` at entry and skipping lifecycle + capacity logic on subsequent attempts (only re-picking the backend worker, which handles in-retry CB-open transitions). Scheduler `_pause_until_safe` skips programs with `in_flight == true` to avoid pause-during-retry race.

**Decision rationale and alternatives considered**: see `worklog.md` D-9.

**Phase placement**: Option C semantics are wired in P6 (pause/resume scheduler) — earlier phases (P3 default mode) don't have admission/capacity, so retry × pause interaction can't manifest yet. The `in_flight` flag on ProgramRequestGuard ships in P3.

**Metrics**: `smg_thunder_retry_repick_total{from, to}` increments when retry causes a backend switch within a single client request. Spike → backend instability or CB-open thrashing.

### 10.10 Model name rewriting is the deployer's responsibility

Client may send `model: "claude-3-5-sonnet-20241022"` to `/v1/messages` while the actual backend (vLLM/sglang) serves `Qwen/Qwen3-0.6B`. SMG's `Worker::supports_model(model_id)` filter would exclude all backends → 404. **Resolution**: the litellm-proxy sidecar rewrites the `model` field in the request body before forwarding to vLLM/sglang. Thunder does NOT rewrite model. Verified litellm has model_alias / model_list mapping (`litellm/proxy/proxy_server.py` config drives model routing).

**Deployment runbook**: register sidecar workers with the model_id of the *physical* backend they serve (e.g., `Qwen/Qwen3-0.6B`), not the alias the client uses. Clients hit SMG with whatever model name the sidecar's model_list maps; sidecar rewrites; vLLM responds. **Out of thunder scope** — thunder only sees the resolved model_id from `Worker::supports_model`.

### 10.11 Concurrent in-flight requests under same program_id (signed-off footgun)

Thunder assumes **at most one in-flight request per `program_id` at any time**. Python ThunderAgent assumes the same (asyncio cooperative semantics; agent loop produces one LLM call at a time). If a buggy or adversarial client sends 2+ concurrent requests with the same program_id, thunder's behavior:

1. First request enters `select_worker_async`, transitions program to `(REASONING, ACTIVE)`, holds ProgramRequestGuard with `in_flight=true`.
2. Second concurrent request enters `select_worker_async` for the same program_id. Detects `program.status == REASONING && state == ACTIVE` (same as the retry-idempotency path of D-9!).
3. **The retry path returns the same worker without admission**. This means the second concurrent request would proceed AS IF it were a retry of the first — but it's actually a separate logical request. Capacity not double-counted (good), but step_count not incremented either (semantics ambiguous — is it step 1 or step 2 of the agent loop?).

**Decision (D-something to be assigned at impl time)**: when ProgramRequestGuard's `in_flight` flag is detected by a NEW `select_worker_async` call (vs a retry of the same call — distinguishable by inspecting RetryExecutor's `attempt` parameter, or by an explicit attempt counter passed via SelectWorkerInfo), thunder rejects with 503 and emits `tracing::warn!(program_id, "concurrent request with same program_id rejected; thunder assumes single in-flight per program")` + `smg_thunder_concurrent_program_rejected_total` counter.

**Alternative considered**: serialize via per-program lock. Rejected — adds blocking on hot path, doesn't match Python semantics, makes lifecycle even more complex.

**Implementation note**: distinguishing "retry of same logical request" from "concurrent NEW request" requires either (a) `RetryExecutor` to pass `attempt: u32` into the policy via SelectWorkerInfo (extending the trait), or (b) ProgramRequestGuard to expose a `request_token: u64` that select_worker_async checks against the program's saved token. (b) is cleaner — implement at P6.

### 10.12 axum / tower middleware verdict — 30-min await is safe (verified 2026-04-30)

Subagent audit of `model_gateway/src/server.rs:build_app:901` middleware stack confirms NO middleware cancels handler-internal `tokio::sync::Notify::notified().await` for up to 1800s. Specifically: (i) `concurrency_limit_middleware` queue timeout (60s default `--queue-timeout-secs`) only governs queue admission, not handler execution; (ii) `--request-timeout-secs` is applied to backend `reqwest::Client::send` calls (after thunder returns), not the inner await; (iii) WASM `OnRequest` body-buffering happens before the handler runs; (iv) axum/hyper drops the entire response future when client disconnects, which propagates cancellation to all `.await` points via tokio's structured concurrency — no task leaks.

**Practical consequence**: thunder can `tokio::time::timeout(Duration::from_secs(resume_timeout_secs), notify.notified()).await` inside `select_worker_async` without any middleware-level intervention. Force-resume timeout (Q5.1 default 1800s) is the only timeout on this path.

**Open verifications** (require runtime testing in P6):
- TCP keep-alive over 30 minutes: client-side connection may drop without RST. axum's drop-future-on-disconnect requires the kernel to detect disconnect; some clients/proxies may keep TCP open indefinitely.
- HTTP/2 ping interval: `axum_server` 0.8 doesn't expose a configurable HTTP/2 keepalive ping. Long awaits over HTTP/2 may be terminated by intermediate proxies.

These are runtime-only verifications and don't block the design.

---
