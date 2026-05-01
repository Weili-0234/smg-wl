> Part of the [Thunder Policy spec](00-INDEX.md). Companion: [worklog](worklog.md) — design decisions with revisit conditions.

# Thunder — Overview & Architecture

## 0. TL;DR

Port the Python ThunderAgent program-aware scheduling algorithm into SMG as a `LoadBalancingPolicy` impl named `ThunderPolicy`, parallel to `cache_aware`. **Deployment scope: internal vLLM/sglang backends behind SMG, where each backend URL serves all three protocols (`/v1/chat/completions`, `/v1/messages`, `/v1/responses`) — typically via a litellm-proxy sidecar in front of vLLM. External 3rd-party API workers (OpenAI/Anthropic/Gemini) are out of scope.** Algorithm behavior is faithful to Python with explicitly-signed-off forks (RAII guard, scheduler-tick `update_shared_tokens()`, configurable timeout). SMG infrastructure (worker registry, CB, retry, kv_event_monitor, observability) reused; thunder adds only program-level state + 5-second BFD scheduler.

5 things to know:

1. **Routing scope reality** (per `routers/router_manager.rs:201-249`): only `HTTP_REGULAR` (`routers/http/router.rs`) and `GRPC_REGULAR`/`GRPC_PD` (`routers/grpc/...`) routers consult `LoadBalancingPolicy`. The 3rd-party-style routers (`HTTP_OPENAI`/`HTTP_ANTHROPIC`/`HTTP_GEMINI`) use `WorkerSelector::find_best_worker` (`routers/common/worker_selection.rs:122-127`) which is **hardcoded `min_by_key(|w| w.load())` and does NOT call any policy**. Internal worker registration (`RuntimeType::Vllm/Sglang/...`, NOT `External`) avoids the 3rd-party-style routers entirely → thunder reaches every request.
2. **One file is the algorithm**: `policies/thunder.rs` (~800-1000 LOC), single `Arc<RwLock<RouterState>>`.
3. **Trait extension is small**: `async fn select_worker_async` (default forwards to sync) + `fn usage_sender` (default `None`). 8 existing policies inherit no-op.
4. **Phase 0 unblocker**: `routers/http/router.rs` does not implement `route_messages` (returns 501 — `routers/mod.rs:226-238` default). Phase 0 adds it as a pass-through (sidecar-translates Anthropic on the backend), implementing `GenerationRequest` for `CreateMessageRequest` to reuse `route_typed_request`.
5. **HTTP path streaming has no usage extraction today** (`routers/http/router.rs:697,908` are pure `bytes_stream()` forwards). Phase 3 adds a usage tail extractor in the typed-request streaming path, gated on `policy.usage_sender().is_some()`.

---

## 1. Mission

Integrate the [ThunderAgent](https://github.com/HaoKang-Timmy/ThunderAgent) program-aware admission + KV-cache-vs-load tradeoff scheduler into SMG (lightseekorg/smg) Rust gateway. Constraints (signed off):

| Goal | Means |
|---|---|
| Algorithm fidelity preserved | Decisions in §2 either FAITHFUL (mirror Python exactly) or FORK (signed-off deviation, narrowly scoped) |
| Cross-protocol coverage (OpenAI Chat, Anthropic Messages, gRPC, …) | Live as a `LoadBalancingPolicy` so all routers' `WorkerSelector` path picks it up automatically |
| Maximize SMG infrastructure reuse | Worker registry membership events, kv_event_monitor, circuit breaker, retry handler, health checker, observability all consumed unchanged |
| Minimize deployer-facing breakage | Thunder appears as `--policy thunder` parallel to `--policy cache_aware`; one new section in `--help`; 0 endpoint additions for the policy itself (admin endpoints `/thunder/*` mounted via downcast pattern) |
| Don't break SMG worker resilience | Thunder is filtered by `Worker::is_available()` like every other policy; CB/retry/health-check semantics unchanged |

---

---

## 3. Architecture overview (thunder-as-policy)

### 3.1 Deployment shape (assumed)

```
┌──────────────────┐       ┌──────────────────────────┐
│ codex-cli        │──Chat ─│ SMG (this gateway)       │
│ claude-code      │──Msg ─│  ┌────────────────────┐  │
│ raw curl         │──Resp─│  │ thunder policy     │  │
└──────────────────┘       │  └────────────────────┘  │
                           └──────────┬───────────────┘
                                      ▼
                           ┌──────────────────────────┐
                           │ litellm-proxy sidecar    │  ← per backend host
                           │ exposes 3 endpoints      │
                           │  /v1/chat/completions    │
                           │  /v1/messages            │
                           │  /v1/responses           │
                           └──────────┬───────────────┘
                                      ▼
                           ┌──────────────────────────┐
                           │ vLLM / sglang (1+)       │
                           └──────────────────────────┘
```

SMG-side worker registration: `--worker-urls http://my-vllm-with-sidecar:8000` → registered as `RuntimeType::Vllm` (or `Sglang`), **NOT** `External`. Single URL per backend host serves all 3 protocols.

### 3.2 SMG request flow (verified against `router_manager.rs:201-249`)

```
Client POST /v1/{chat/completions, messages, responses}
  │
  ▼
server.rs:198/289/...   axum handler v1_chat_completions / v1_messages / v1_responses
  │
  ▼
router_manager.rs:554/598/638   RouterManager::route_chat / _messages / _responses
  │
  ▼
router_manager.rs:255   select_router_for_request(headers, model_id)
  │
  ▼
router_manager.rs:201   get_router_for_model(model_id)
  │  is_external?  no  (worker is RuntimeType::Vllm/Sglang/...)
  │  is_grpc?      no  →  RouterId::HTTP_REGULAR  (or GRPC_REGULAR if grpc)
  ▼
HTTP_REGULAR = `routers/http/router.rs`
  │  route_chat (line 1117)         ─┐
  │  route_responses (line 1139)     │ all three call `route_typed_request`
  │  route_messages (NEW, Phase 0)   ─┘   (line 196)
  │
  ▼
http/router.rs:170   select_worker_for_model(...)
  │
  ▼
http/router.rs:175   policy.select_worker_async(workers, info).await   ← thunder enters here
  │
  ▼
ThunderPolicy::select_worker_async
  │  guard A:  get_or_create(program_id);  if TR-mode + no capacity: enqueue
  │  if waiting: notify.notified().await   (Q5.1 timeout 1800s)
  │  guard B:  pick backend URL → workers[idx]
  ▼
http/router.rs forward:  POST {worker.url()}{endpoint_path}
  │  (endpoint_path is /v1/chat/completions or /v1/messages or /v1/responses)
  │  sidecar routes Anthropic→OpenAI internally
  ▼
streaming response → bytes_stream() forwarded to client
  │
  │  WRAPPED by usage_tail extractor (Phase 1):
  │  scans final SSE chunk for "usage": {…}
  │  fires UsageEvent into ThunderPolicy.usage_tx
  ▼
ThunderPolicy usage_consumer task
  │  recv UsageEvent
  │  guard:  update Program.total_tokens, char_to_token_ratio momentum
  ▼

(scheduler task, separately, every 5s):
  fetch metrics (outside guard)
  guard:  apply metrics, update_shared_tokens, greedy_resume, pause_until_safe
  notify_resumed_waiters → wakes select_worker_async paused-flow
```

### 3.3 Out-of-scope paths (explicit non-goals)

| Path | Reason out-of-scope |
|---|---|
| `HTTP_OPENAI` (`routers/openai/router.rs`) | External worker only; uses `WorkerSelector::find_best_worker` with hardcoded `min_by_key(load)` (`worker_selection.rs:122-127`) — does NOT consult policy. Thunder cannot intercept without a separate WorkerSelector refactor (deliberately not done in this work). |
| `HTTP_ANTHROPIC` (`routers/anthropic/router.rs`) | Same — External Anthropic API workers, no policy. |
| `HTTP_GEMINI` (`routers/gemini/...`) | Same — External Gemini, no policy. |
| `HTTP_PD` (`routers/http/pd_router.rs`) | PD disaggregation deliberately deferred (user request 2026-04-30). |
| `GRPC_PD` (`routers/grpc/pd_router.rs`) | Same. |
| `/v1/embeddings`, `/v1/rerank`, `/v1/audio/*`, `/v1/realtime/*`, `/v1/interactions`, `/v1/classify` | Non-generation endpoints; thunder does not intercept. |

If a deployer registers `RuntimeType::External` workers and wants thunder there, they'd need to refactor `WorkerSelector` to consult policy — separate FORK, separate sign-off.

### 3.4 Background subscriptions held inside ThunderPolicy

- `WorkerRegistry::subscribe_events` broadcast (`worker/registry.rs:143-151`) → membership churn; RouterState reconciles `backends` map.
- `KvEventMonitor` per-model PositionalIndexer (`worker/kv_event_monitor.rs`) → real-time KV-cache state per backend (used for `shared_tokens` upgrade; open question deferred).
- `usage_rx: mpsc::UnboundedReceiver<UsageEvent>` (drained by usage_consumer task) → end-of-stream tokens from routers.

---
