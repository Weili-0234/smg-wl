> Part of the [Thunder Policy spec](00-INDEX.md). Companion: [worklog](worklog.md) — design decisions with revisit conditions.

# Thunder — SMG Integration Surface

## 5. SMG integration surface

### 5.1 LoadBalancingPolicy trait extension (decision §2.15, §2.20)

Diff at `model_gateway/src/policies/mod.rs:44`:

```rust
use async_trait::async_trait;

#[async_trait]
pub trait LoadBalancingPolicy: Send + Sync + Debug {
    // EXISTING (sync) — unchanged, default fallback target for select_worker_async
    fn select_worker(&self, workers: &[Arc<dyn Worker>], info: &SelectWorkerInfo) -> Option<usize>;

    // NEW: async variant. Default forwards to sync select_worker.
    // Thunder overrides; 8 existing policies inherit no-op.
    async fn select_worker_async(
        &self,
        workers: &[Arc<dyn Worker>],
        info: &SelectWorkerInfo<'_>,
    ) -> Option<usize> {
        self.select_worker(workers, info)
    }

    // NEW: usage event sender (D-2). Default None.
    // Routers fire-and-forget UsageEvent post-stream. Stateless policies → None → drop.
    fn usage_sender(&self) -> Option<&tokio::sync::mpsc::UnboundedSender<UsageEvent>> {
        None
    }

    // EXISTING (unchanged)
    fn on_request_complete(&self, _worker_url: &str, _success: bool) {}
    fn name(&self) -> &'static str;
    fn needs_request_text(&self) -> bool { false }
    fn update_loads(&self, _loads: &HashMap<String, WorkerLoadResponse>) {}
    fn set_mesh_sync(&mut self, _mesh_sync: OptionalMeshSyncManager) {}
    fn reset(&self) {}
    fn as_any(&self) -> &dyn std::any::Any;
}
```

`UsageEvent` lives in `policies/mod.rs` (not in `policies/thunder.rs`) so the trait method can reference it without forcing every policy to import thunder:

```rust
#[derive(Debug, Clone)]
pub struct UsageEvent {
    pub program_id: Option<String>,    // String, not &str — channel needs 'static
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
    pub cached_tokens: Option<u32>,    // from prompt_tokens_details when present
    pub prompt_chars: usize,           // for char_to_token_ratio momentum (Q5.5)
    pub timestamp: Instant,
}
```

> **Rust idiom note for new-to-Rust users**: `#[async_trait]` is a procedural macro that desugars `async fn` in traits to `fn(...) -> Pin<Box<dyn Future + Send>>`. It's needed because vanilla Rust traits don't yet support async fn with dynamic dispatch on stable. SMG already uses this macro pervasively (`RouterTrait`, `AnthropicRouter`, etc. — `routers/mod.rs:70`).

### 5.2 SelectWorkerInfo extension (decision §2.16)

Diff at `model_gateway/src/policies/mod.rs:167`:

```rust
#[derive(Debug, Clone, Default)]
pub struct SelectWorkerInfo<'a> {
    pub request_text: Option<&'a str>,
    pub tokens: Option<&'a [u32]>,
    pub headers: Option<&'a http::HeaderMap>,
    pub hash_ring: Option<Arc<HashRing>>,
    pub program_id: Option<&'a str>,        // NEW — populated by routers under thunder policy
}
```

8 existing policies don't read `program_id`; zero behavior change.

### 5.3 ThunderPolicy struct

```rust
pub struct ThunderPolicy {
    config: Arc<ThunderConfig>,                        // immutable

    state: Arc<RwLock<RouterState>>,                   // single mutex (Q1, §2.1)

    usage_tx: tokio::sync::mpsc::UnboundedSender<UsageEvent>,   // D-2 hook
    // Held in Drop to abort scheduler:
    _scheduler_task: Option<tokio::task::JoinHandle<()>>,
    // Membership subscription:
    _registry_task: Option<tokio::task::JoinHandle<()>>,
    // KV monitor (set later via set_kv_event_monitor mirror cache_aware pattern):
    kv_monitor: RwLock<Option<Arc<KvEventMonitor>>>,
    // Mesh sync manager (interior mutability — same wart as cache_aware):
    mesh_sync: RwLock<Option<OptionalMeshSyncManager>>,
}

pub struct RouterState {
    pub programs: HashMap<String, Program>,
    pub backends: HashMap<String, BackendState>,
    pub waiting_queue: VecDeque<String>,                  // BFD priority recomputed each tick
    pub waiting_events: HashMap<String, Arc<Notify>>,     // per-program resume signals
    pub char_to_token_ratio: f64,
    pub first_sample_received: bool,
}
```

Construction (in `PolicyFactory`, see §5.4) spawns the scheduler task and the registry-event consumer task. Both hold `Arc::downgrade(&state)` (i.e., `Weak<RwLock<RouterState>>`) inside their async closures so the policy can be dropped without leaks. JoinHandles in the `_scheduler_task` / `_registry_task` fields abort the tasks on Drop.

### 5.4 PolicyConfig + PolicyFactory wiring (decision §2.22, D-4)

Add to `model_gateway/src/config/types.rs:347` enum:

```rust
#[serde(rename = "thunder")]
Thunder {
    #[serde(default)]
    sub_mode: ThunderSubMode,                            // typed enum, not String
    #[serde(default = "default_thunder_scheduler_interval")]
    scheduler_interval_secs: u64,                        // 5
    #[serde(default = "default_thunder_resume_timeout")]
    resume_timeout_secs: u64,                            // 1800 (Q5.1 sign-off)
    #[serde(default = "default_thunder_tool_coefficient")]
    tool_coefficient: f64,                               // 0.5
    #[serde(default)]
    use_acting_token_decay: bool,                        // false — Phase P9 toggle, off by default
},

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThunderSubMode {
    #[default]
    Default,
    Tr,
}

fn default_thunder_scheduler_interval() -> u64 { 5 }
fn default_thunder_resume_timeout() -> u64 { 1800 }
fn default_thunder_tool_coefficient() -> f64 { 0.5 }
```

Update `PolicyConfig::name()` at line 442:

```rust
PolicyConfig::Thunder { .. } => "thunder",
```

Add to `model_gateway/src/policies/factory.rs:17`:

```rust
PolicyConfig::Thunder {
    sub_mode,
    scheduler_interval_secs,
    resume_timeout_secs,
    tool_coefficient,
    use_acting_token_decay,
} => {
    let cfg = ThunderConfig {
        sub_mode: *sub_mode,
        scheduler_interval_secs: *scheduler_interval_secs,
        resume_timeout_secs: *resume_timeout_secs,
        tool_coefficient: *tool_coefficient,
        use_acting_token_decay: *use_acting_token_decay,
    };
    Arc::new(ThunderPolicy::new(cfg))
}
```

Add to `create_by_name`:

```rust
"thunder" => Some(Arc::new(ThunderPolicy::default())),
```

Add `"thunder"` to value_parser arrays at `main.rs:152, 217, 222`. Add a new `Help heading = "Thunder Policy"` block of CLI flags:

```
--thunder-sub-mode {default,tr}
--thunder-scheduler-interval-secs <u64>
--thunder-resume-timeout-secs <u64>
--thunder-tool-coefficient <f64>
--thunder-use-acting-token-decay
```

These are forwarded into `PolicyConfig::Thunder { … }` when `--policy thunder` is selected; ignored otherwise.

### 5.5a program_id extraction

Add `model_gateway/src/routers/common/program_id.rs`:

```rust
pub const DEFAULT_PROGRAM_ID: &str = "default";

/// OpenAI ChatCompletionRequest / ResponsesRequest / GenerateRequest (typed).
/// Looks at `metadata.program_id`, then top-level `program_id` (via the `other`
/// flattened map for OpenAI, or as direct field for others).
pub fn extract_from_openai_chat(req: &openai_protocol::chat::ChatCompletionRequest) -> Option<&str> {
    req.metadata.as_ref()
        .and_then(|m| m.get("program_id"))
        .map(String::as_str)
        .or_else(|| req.other.get("program_id").and_then(|v| v.as_str()))
        .filter(|s| !s.is_empty())
}

pub fn extract_from_openai_responses(req: &openai_protocol::responses::ResponsesRequest) -> Option<&str> {
    req.metadata.as_ref()
        .and_then(|m| m.get("program_id"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
}

/// Anthropic — requires Metadata.program_id field extension (see §5.5d).
pub fn extract_from_anthropic(req: &openai_protocol::messages::CreateMessageRequest) -> Option<&str> {
    req.metadata.as_ref().and_then(|m| m.program_id.as_deref())
        .filter(|s| !s.is_empty())
}

/// Resolve to "default" with single warn + counter increment when None.
pub fn resolve_or_default<'a>(extracted: Option<&'a str>) -> &'a str {
    if extracted.is_none() {
        metrics::counter!("smg_thunder_program_id_missing_total").increment(1);
        // tracing::warn! once-per-session via OnceLock
    }
    extracted.unwrap_or(DEFAULT_PROGRAM_ID)
}
```

Call sites in scope (7 total — 1 already async + 6 sync to migrate; all live in policy-consulting routers):

| Router | File:line | Async? | Action |
|---|---|---|---|
| `HTTP_REGULAR.route_typed_request` | `routers/http/router.rs:175` | sync | s/`select_worker`/`select_worker_async`/ + propagate `.await` up; populate `info.program_id` from typed request via the `GenerationRequest` trait (extend trait with `program_id_hint(&self) -> Option<&str>`, default `None`; impl per generation type) |
| `GRPC_REGULAR.select_single_worker` | `routers/grpc/common/stages/worker_selection.rs:157` | sync | same; extract program_id from `RequestContext` (which already holds the deserialized request body) |
| `GRPC_PD.select_pd_pair` (prefill) | `routers/grpc/common/stages/worker_selection.rs:276` | sync | OUT OF SCOPE (PD deferred) — keep sync; pass `info.program_id = None` so default fallback kicks in |
| `GRPC_PD.select_pd_pair` (decode) | `routers/grpc/common/stages/worker_selection.rs:277` | sync | OUT OF SCOPE — same |
| `HTTP_PD` | `routers/http/pd_router.rs:861` | sync | OUT OF SCOPE — keep sync |
| `HTTP_REGULAR.route_transcriptions` | `routers/http/router.rs:568` | sync | OUT OF SCOPE — non-generation endpoint; keep sync |
| (External-path sites) `HTTP_OPENAI/HTTP_ANTHROPIC/HTTP_GEMINI` | (`WorkerSelector::find_best_worker`) | already async | OUT OF SCOPE per §3.3 |

In-scope sync→async migration is **2 sites**: `http/router.rs:175` and `grpc/common/stages/worker_selection.rs:157`. Both already live inside async axum/tonic handler stacks; `.await` propagation is local.

### 5.5b GenerationRequest trait extension for program_id

Trait lives in `crates/protocols/src/common.rs:40`. Add:

```rust
pub trait GenerationRequest {
    // ... existing methods (is_stream, extract_text_for_routing, get_model, ...) ...

    /// Optional program_id hint for thunder policy.
    /// Default: None; concrete impls override to expose `metadata.program_id` /
    /// `extra_body.program_id` / Anthropic `metadata.program_id` etc.
    fn program_id_hint(&self) -> Option<&str> { None }
}
```

Impl on each in-scope generation type:
- `ChatCompletionRequest` (`crates/protocols/src/chat.rs`): delegate to `extract_from_openai_chat`
- `ResponsesRequest` (`crates/protocols/src/responses.rs`): delegate to `extract_from_openai_responses`
- `CompletionRequest`, `GenerateRequest`: similar pattern
- `CreateMessageRequest` (`crates/protocols/src/messages.rs`): delegate to `extract_from_anthropic` (after §5.5d field add). **NOTE**: This requires implementing `GenerationRequest` for `CreateMessageRequest` — currently NOT implemented (audit finding, `crates/protocols/src/common.rs:40` shows 6 impls but Anthropic excluded). This impl is required by Phase 0 to enable `route_messages` reuse of `route_typed_request`.

`route_typed_request` then calls `req.program_id_hint()` and stuffs into `SelectWorkerInfo.program_id`.

**Full method list required by `GenerationRequest` for `CreateMessageRequest`** (verified via complete read-through of `routers/http/router.rs:196-355` + helpers — see `worklog.md` D-13):

| Trait method | OpenAI ChatCompletionRequest precedent | Anthropic CreateMessageRequest impl |
|---|---|---|
| `is_stream() -> bool` | `crates/protocols/src/chat.rs:590-592` | `self.stream.unwrap_or(false)` (1 line; CreateMessageRequest.stream is `Option<bool>`) |
| `get_model() -> Option<&str>` | `chat.rs:594-596` | `Some(&self.model)` (1 line) |
| `extract_text_for_routing() -> String` | `chat.rs:598-640` (~40 LOC; iterates ChatMessage variants, accumulates text into a single buffer) | **~30-50 LOC NEW** — iterate `self.system: Option<SystemContent>` then `self.messages: Vec<InputMessage>`; for each `InputMessage.content: MessageContent`, extract only `Text` variant (skip Image/ToolUse/ToolResult/Document/etc.); accumulate into a single buffer mirroring the chat.rs pattern |
| `program_id_hint() -> Option<&str>` (new in P1) | `chat.rs` impl delegates to `extract_from_openai_chat` | delegates to `extract_from_anthropic` (after §5.5d) |

`extract_text_for_routing` is **not optional**: cache_aware policy reads `SelectWorkerInfo.request_text` (set from this method) for prefix-cache match. Even when policy is thunder, the trait bound on `route_typed_request<T: GenerationRequest>` requires the method exists — code won't compile without it. ~30-50 LOC is the realistic Phase 0 work item for this method alone.

### 5.5c HTTP_REGULAR `route_messages` pass-through (Phase 0 — UNBLOCKER)

Currently `routers/http/router.rs` does NOT override `route_messages`; falls through to `routers/mod.rs:226-238` default which returns 501. Diff to add:

```rust
// in routers/http/router.rs impl RouterTrait for HTTPRouter
async fn route_messages(
    &self,
    headers: Option<&HeaderMap>,
    _tenant_meta: &TenantRequestMeta,
    body: &CreateMessageRequest,
    model_id: &str,
) -> Response {
    self.route_typed_request(headers, body, "/v1/messages", model_id).await
}
```

Prerequisites:
1. `CreateMessageRequest: GenerationRequest` impl (§5.5b above) — required by `route_typed_request<T: GenerationRequest>` bound at `routers/http/router.rs:196`.
2. Backend serves `/v1/messages` (deployment assumption: litellm-proxy sidecar in front of vLLM/sglang).

This is a **pure pass-through** addition; SMG performs zero protocol translation. The sidecar handles Anthropic↔OpenAI translation upstream of the backend. From SMG's perspective, `/v1/messages` is just another endpoint path to forward, no different from `/v1/chat/completions`.

### 5.5e route_to_endpoint metric label for `/v1/messages`

`grpc/utils/metrics.rs:8 route_to_endpoint(route)` is a hardcoded `match` over a path whitelist; unmatched paths return `"other"` and bucket into the same Prometheus label as miscellaneous routes. `/v1/messages` currently matches no arm and falls to `"other"`.

Phase 0 adds 1 line:

```rust
"/v1/messages" => metrics_labels::ENDPOINT_MESSAGES,
```

The `ENDPOINT_MESSAGES = "messages"` constant **already exists** at `observability/metrics.rs:387` — no new label registration. Without this 1-line fix, thunder's `/v1/messages` traffic is silently aggregated under `endpoint="other"` in `smg_router_*` metrics — no functional bug, but a regression for dashboards and on-call alarms keyed by endpoint. P0 must include this.

### 5.5d Anthropic Metadata.program_id field extension (SIGNED-OFF FORK)

Diff at `crates/protocols/src/messages.rs:177`:

```rust
pub struct Metadata {
    pub user_id: Option<String>,
    /// SMG extension: tracks multi-step LLM agent programs for thunder policy.
    /// If unset and thunder is active, the gateway uses "default" as program_id
    /// and emits a warn-once log + smg_thunder_program_id_missing_total counter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub program_id: Option<String>,
}
```

`skip_serializing_if` keeps wire format Anthropic-spec-compatible when unset.

### 5.6 HTTP path streaming usage tail extractor (D-1, scope expanded)

`routers/http/router.rs` lines 697 and 908 (the streaming branches in `route_typed_request`) are currently pure `bytes_stream()` forwards — **no SSE parsing, no usage extraction**. Same gap exists for the 3rd-party-style `routers/openai/chat.rs` but that path is out of scope (§3.3). For HTTP_REGULAR, a minimal usage extractor wrapper is required:

```rust
// in routers/http/router.rs:697 / :908 streaming branch
let upstream_stream = res.bytes_stream();
let usage_tx = policy.usage_sender().cloned();   // Option<UnboundedSender<UsageEvent>>
let program_id = info.program_id.map(String::from);

let wrapped = wrap_with_usage_tail_extract(upstream_stream, usage_tx, program_id, prompt_chars);
// then build axum Response::new(Body::from_stream(wrapped))
```

Where `wrap_with_usage_tail_extract` lives in `routers/http/usage_tail.rs` (~80 LOC):

```rust
pub fn wrap_with_usage_tail_extract<S>(
    stream: S,
    usage_tx: Option<UnboundedSender<UsageEvent>>,
    program_id: Option<String>,
    prompt_chars: usize,
) -> impl Stream<Item = Result<Bytes, E>>
where S: Stream<Item = Result<Bytes, E>>
{
    async_stream::try_stream! {
        let mut buf = BytesMut::new();
        let mut usage_seen: Option<UsageInfo> = None;
        for await chunk in stream {
            let chunk = chunk?;
            buf.extend_from_slice(&chunk);
            // Scan complete SSE events (terminated by \n\n)
            while let Some(pos) = buf.windows(2).position(|w| w == b"\n\n") {
                let event = buf.split_to(pos + 2);
                if let Some(usage) = try_parse_usage_chunk(&event) {
                    usage_seen = Some(usage);
                }
            }
            yield chunk;
        }
        // After upstream completes: fire UsageEvent
        if let (Some(tx), Some(usage)) = (usage_tx, usage_seen) {
            let _ = tx.send(UsageEvent {
                program_id,
                prompt_tokens: usage.prompt_tokens,
                completion_tokens: usage.completion_tokens,
                total_tokens: usage.total_tokens,
                cached_tokens: usage.cached_tokens,
                prompt_chars,
                timestamp: Instant::now(),
            });
        }
    }
}

fn try_parse_usage_chunk(event: &[u8]) -> Option<UsageInfo> {
    // Each event is 1+ lines starting "data: {json}". Find the JSON, parse, look for "usage".
    // Fast-path: skip any chunk lacking the literal byte sequence b"\"usage\""
    if !memmem::find(event, b"\"usage\"").is_some() { return None; }
    // Slow path: parse JSON
    let line = event.strip_prefix(b"data: ")?;
    let value: serde_json::Value = serde_json::from_slice(line).ok()?;
    let usage = value.get("usage")?.as_object()?;
    Some(UsageInfo {
        prompt_tokens: usage.get("prompt_tokens")?.as_u64()? as u32,
        completion_tokens: usage.get("completion_tokens")?.as_u64()? as u32,
        total_tokens: usage.get("total_tokens")?.as_u64()? as u32,
        cached_tokens: usage.get("prompt_tokens_details")
            .and_then(|d| d.get("cached_tokens"))
            .and_then(|v| v.as_u64())
            .map(|v| v as u32),
    })
}
```

To get usage in the stream, the upstream request must include `stream_options: {include_usage: true}`. Implementation policy:

- **Gateway-injected** in `route_typed_request` body-prep when `policy.name() == "thunder"` and `is_stream`: mutate the JSON payload to set `stream_options.include_usage = true` before forwarding. Matches Python `vllm_request_processor.py:138-143`. Backend (vLLM/sglang or sidecar) emits a final SSE chunk with the `usage` block; extractor catches it.

This applies uniformly to all three endpoints because `route_typed_request` is generic over any `T: GenerationRequest + Serialize`. The Anthropic path uses sidecar-translated upstream `/v1/messages` which (litellm-proxy) re-emits an OpenAI-shaped final chunk with `usage` — same SSE format, same extractor.

`gRPC path` separately: `routers/grpc/common/responses/streaming.rs:715` already provides `finalize(usage: Option<Usage>)` natively (audit finding from fork γ). gRPC routers call `policy.usage_sender()` directly after stream end with values already on hand — no SSE inspection needed.

### 5.7 Hook mechanism: `usage_sender` trait method (D-2)

Already covered in §5.1. Recap:

- ThunderPolicy returns `Some(&self.usage_tx)` from `usage_sender()`.
- Other 8 policies return `None` (default impl).
- Routers always call `policy.usage_sender()` post-stream; `if let Some(tx) = ... { tx.send(...) }` short-circuits when None.
- Send is fire-and-forget (`UnboundedSender`); failure (channel closed = thunder shutting down) silently dropped + counter incremented.

ThunderPolicy spawns one consumer task at construction:

```rust
let mut usage_rx = ...;
let state_weak = Arc::downgrade(&self.state);
let usage_consumer = tokio::spawn(async move {
    while let Some(event) = usage_rx.recv().await {
        let Some(state) = state_weak.upgrade() else { return; };  // policy dropped → exit
        let mut s = state.write();
        if let Some(pid) = &event.program_id {
            if let Some(prog) = s.programs.get_mut(pid) {
                prog.total_tokens = event.total_tokens as i64;
                if prog.status == ProgramStatus::Reasoning {
                    prog.status = ProgramStatus::Acting;
                    prog.acting_since = Some(event.timestamp);
                }
            }
        }
        s.update_char_to_token_ratio(event.prompt_chars, event.prompt_tokens as i64);
    }
});
```

### 5.8 Scheduler task lifecycle

Spawned in `ThunderPolicy::new()`. Tokio runtime is guaranteed active because `PolicyFactory::create_from_config` is called during `AppContext::build` which runs inside `#[tokio::main]`.

```rust
pub fn new(config: ThunderConfig) -> Self {
    let state = Arc::new(RwLock::new(RouterState::default()));
    let (usage_tx, usage_rx) = tokio::sync::mpsc::unbounded_channel();

    // Usage consumer task (§5.7):
    let usage_consumer = spawn_usage_consumer(state.clone(), usage_rx);

    // Scheduler task (only if sub_mode == Tr):
    let scheduler_task = if config.sub_mode == ThunderSubMode::Tr {
        Some(spawn_scheduler(state.clone(), Arc::new(config.clone())))
    } else {
        None
    };

    Self {
        config: Arc::new(config),
        state,
        usage_tx,
        _scheduler_task: scheduler_task,
        _registry_task: None,        // set later via spawn_registry_consumer (§5.10)
        kv_monitor: RwLock::new(None),
        mesh_sync: RwLock::new(None),
    }
}
```

Scheduler task uses `tokio::time::interval` for the 5-second tick:

```rust
fn spawn_scheduler(state: Arc<RwLock<RouterState>>, config: Arc<ThunderConfig>) -> JoinHandle<()> {
    let state_weak = Arc::downgrade(&state);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(config.scheduler_interval_secs));
        loop {
            interval.tick().await;
            let Some(state) = state_weak.upgrade() else { return; };  // policy dropped
            scheduler_tick(state, config.clone()).await;
        }
    })
}
```

Drop semantics: when `ThunderPolicy` is dropped, `_scheduler_task: Option<JoinHandle>` drops, which aborts the task (tokio guarantee). The `Weak` upgrade in the loop body returns `None` if state is already dropped, providing a second exit path. Cancellation is cooperative (next tick).

For SMG's graceful shutdown grace period (`--shutdown-grace-period-secs`, default 180), we don't need explicit drain — the scheduler is a periodic housekeeping loop, not an in-flight request, so abrupt abort is fine. Routers' in-flight `select_worker_async` calls that are `.await`ing `notify.notified()` for resume will time out and force-resume normally.

### 5.9 Notify integration (paused-program wake)

In `select_worker_async`:

```rust
async fn select_worker_async(
    &self,
    workers: &[Arc<dyn Worker>],
    info: &SelectWorkerInfo<'_>,
) -> Option<usize> {
    let pid = info.program_id.unwrap_or(DEFAULT_PROGRAM_ID).to_string();

    // Phase A: register / lifecycle update (write guard A)
    let need_wait = {
        let mut s = self.state.write();
        let program = s.get_or_create_program(&pid);
        s.update_program_before_request(&pid);
        if self.config.sub_mode == ThunderSubMode::Tr {
            s.check_capacity_and_maybe_enqueue(&pid)
        } else {
            false
        }
    };

    // Phase B: if enqueued, wait for resume (NO guard held during await)
    if need_wait {
        let notify = self.state.read().waiting_events.get(&pid).cloned();
        if let Some(n) = notify {
            let timeout = Duration::from_secs(self.config.resume_timeout_secs);
            match tokio::time::timeout(timeout, n.notified()).await {
                Ok(_) => { /* resumed normally */ }
                Err(_) => {
                    // Q5.1: force-resume via least-loaded fallback
                    let mut s = self.state.write();
                    s.force_resume_program(&pid);
                    // emit smg_thunder_force_resume_total
                }
            }
        }
    }

    // Phase C: backend selection (write guard C — short)
    let mut s = self.state.write();
    let backend_url = s.programs.get(&pid)?.backend_url.clone()
        .or_else(|| s.select_backend_for_new_program_default(&workers))?;

    // Map backend URL → worker index
    workers.iter().position(|w| w.url() == backend_url)
}
```

Scheduler tick wakes paused programs:

```rust
fn notify_resumed_waiters(&mut self) {
    for pid in self.recently_resumed.drain(..) {
        if let Some(notify) = self.waiting_events.get(&pid) {
            notify.notify_one();   // wake exactly the request waiting on this program
        }
    }
}
```

`notify_one` (not `notify_waiters`) because we want to release exactly one `select_worker_async` await per program; multiple in-flight resume signals shouldn't accumulate.

### 5.10 KvEventMonitor + WorkerRegistry events

KV monitor follows the cache_aware pattern (`policies/cache_aware.rs:184`):

```rust
impl ThunderPolicy {
    pub fn set_kv_event_monitor(&self, monitor: Option<Arc<KvEventMonitor>>) {
        *self.kv_monitor.write() = monitor;
    }
}
```

Wired in `policies/registry.rs:96` already invokes `set_kv_event_monitor` on every registered policy that has the method — fork α confirmed this (audit finding A.H.2). Thunder needs no main.rs plumbing; the existing registry call propagates.

Worker registry membership consumed via:

```rust
fn spawn_registry_consumer(state: Arc<RwLock<RouterState>>, registry: Arc<WorkerRegistry>) -> JoinHandle<()> {
    let mut rx = registry.subscribe_events();
    let state_weak = Arc::downgrade(&state);
    tokio::spawn(async move {
        // Initial reconciliation (catch-up for late subscribers, registry.rs:377-381)
        if let Some(state) = state_weak.upgrade() {
            let snapshot = registry.snapshot();
            let mut s = state.write();
            for w in snapshot {
                s.upsert_backend(&w);   // populate BackendState including supported_providers
            }
        }
        // Ongoing events:
        while let Ok(event) = rx.recv().await {
            let Some(state) = state_weak.upgrade() else { return; };
            let mut s = state.write();
            match event {
                WorkerEvent::Registered(w) | WorkerEvent::Active(w) => s.upsert_backend(&w),
                WorkerEvent::Removed(w) | WorkerEvent::Inactive(w) => {
                    // Pause all programs on this backend; transfer to global queue.
                    s.handle_backend_removed(&w.url());
                }
            }
        }
    })
}
```

`upsert_backend` adds the URL to `RouterState.backends` if not present, with `shared_tokens=0` and `cache_config=None` until first metric fetch (Q5.8 sign-off, gap-fill semantics from requirements §2.14).

### 5.11 Concurrency model + performance footgun (D-3)

Single `Arc<RwLock<RouterState>>` per §2.1. Hard rules (compile-time / lint-time enforceable):

1. **No `.await` inside `state.write()` or `state.read()` guard.** Compute network futures' inputs first, drop the guard, then await.
2. **All I/O outside the guard.** `metrics_client.fetch_metrics(...).await` happens between two write-guarded sections.
3. **Single shared state in scope.** Every method that mutates RouterState takes `&mut self` on RouterState; no per-field interior mutability that could be modified out-of-band.

**Performance footgun (signed off, NOT mitigated)**: every `select_worker_async` request acquires `state.write()` at least once. Scheduler ticks also acquire write. Theoretical contention if RPS into thunder × scheduler tick frequency × tick critical-section duration becomes non-negligible. Realistic estimate: 100 concurrent agents × 1 request/30s = 3.3 RPS into select; tick critical section = ms; total contention budget = far below saturation.

**If future benchmarks show contention**:

1. **First step**: switch from `std::sync::RwLock` → `parking_lot::RwLock` (no syscall overhead; ~10× faster uncontended; write-preferring fairness avoids reader starvation). Single-line change.
2. **Second step**: only after (1) is insufficient — consider lock-splitting (e.g., separate `RwLock<Programs>` + `RwLock<Backends>`). This requires a SIGNED-OFF FORK with race-free audit, because it breaks the Python atomic-region equivalence. Document as non-faithful deviation.
3. **Forbidden first step**: do NOT switch to `DashMap`/lock-free unless step 1+2 audited and approved separately.

Document this hierarchy in the PR description for any phase that touches RouterState.

---
