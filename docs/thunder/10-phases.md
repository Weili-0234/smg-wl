> Part of the [Thunder Policy spec](00-INDEX.md). Companion: [worklog](worklog.md) — design decisions with revisit conditions.

# Thunder — Phase Plan & Sign-off Rules

## 11. Phase plan

Each phase = one commit on `thunder-policy` branch. Per-phase contract: `cargo build/test/clippy` pass + e2e test under `e2e_test/thunder/`. PD-mode and External-worker paths are explicitly **out of scope** (§3.3).

| # | Title | Scope | Risk | Test |
|---|---|---|---|---|
| **P0** | HTTP_REGULAR.route_messages pass-through + GenerationRequest impl for `CreateMessageRequest` | (a) `crates/protocols/src/common.rs:40` add `program_id_hint` default-None to `GenerationRequest` trait; (b) impl `GenerationRequest` for `CreateMessageRequest` (`crates/protocols/src/messages.rs`); (c) extend `crates/protocols/src/messages.rs:177 Metadata` with `program_id: Option<String>` (skip_serializing_if); (d) add `route_messages` to `routers/http/router.rs` impl as `route_typed_request(headers, body, "/v1/messages", model_id)` pass-through | Anthropic body parsing now flows into HTTP_REGULAR; some risk that `route_typed_request`'s OpenAI-flavored validation (e.g. text extraction) misbehaves on Anthropic shape. Mitigate with focused unit tests on `extract_text_for_routing` for `CreateMessageRequest`. | Drive `/v1/messages` against an extended `mock_vllm.py` that accepts the path; verify 200 + payload pass-through; verify 4xx surfaces correctly. |
| **P1** | Trait extension + SelectWorkerInfo + program_id helper + 2 sync→async migrations | (a) `policies/mod.rs:44` add `async fn select_worker_async` default-fallback + `fn usage_sender` default-None + `pub struct UsageEvent`; (b) `policies/mod.rs:167` add `program_id: Option<&'a str>` to `SelectWorkerInfo`; (c) new `routers/common/program_id.rs`; (d) async-migrate `http/router.rs:175 select_worker_for_model` and `grpc/common/stages/worker_selection.rs:157 select_single_worker` to call `.select_worker_async(...).await`; populate `info.program_id` from `req.program_id_hint()`; (e) verify all 8 existing policies inherit no-op via unit test asserting `select_worker_async == select_worker` | Highest surface risk: every policy file recompiles (no behavior change expected). 2 async propagation chains touched. | `cargo test --workspace` green; new unit test per existing policy verifying default fallback parity. |
| **P2** | Mock backend + pytest fixtures (multi-protocol) | Copy `mock_vllm.py` from `/home/hkang/wl/smg_thunder/e2e_test/thunder/`; **extend it to accept `POST /v1/chat/completions`, `POST /v1/messages`, `POST /v1/responses`** (all three return identical OpenAI-shaped responses to simulate sidecar normalization); create `e2e_test/thunder/conftest.py` and a smoke `test_phase2_smoke.py` driving SMG with policy=cache_aware against the multi-endpoint mock | Pure test infra. | All 3 endpoints pass-through round-trip green via cache_aware. |
| **P3** | ThunderPolicy skeleton + Default sub-mode + RouterState | New `policies/thunder.rs` with full Program/BackendState; `select_worker_async` for `sub_mode = Default` (least-active-count, Q5.6 faithful); `PolicyConfig::Thunder` + `PolicyFactory::create_from_config` Thunder arm + `create_by_name` arm; CLI flags + `--policy thunder` value_parser update at `main.rs:152`; HTTP path streaming usage tail extractor (§5.6) + gateway-side `stream_options.include_usage = true` injection; usage_sender + usage_consumer task updates `total_tokens` + `char_to_token_ratio` (Q5.5); `WorkerRegistry::subscribe_events` integration; `ProgramRequestGuard` RAII for cancel cleanup (Q5.4) | Largest LOC concentration (~700 LOC in thunder.rs alone). Streaming usage extractor parsing edge cases (final chunk delimiters across vLLM/sglang/sidecar). | 2 backends, 3 protocols (chat/messages/responses) on the same `program_id`; assert step_count increments cross-protocol; assert program_id stickiness. |
| **P4** | Backend metrics + Q5.3 shared_tokens + scheduler-task lifecycle | `MetricsClient` trait + vLLM impl + sglang variant; scheduler tick fetches metrics outside guard; calls `update_shared_tokens()` per tick (Q5.3 FORK); `BackendState` cache_config / latest_metrics populated; KvEventMonitor wired via `policies/registry.rs:96`; scheduler task spawn at `ThunderPolicy::new` with `Weak<RouterState>` cycle-break; graceful shutdown integration | `MetricsClient` trait shape stability across vLLM/sglang versions. | `/metrics` exposes `smg_thunder_backend_shared_tokens`; mock sets capacity via `/control/capacity`; thunder reads correctly. |
| **P5** | TR sub-mode admission (capacity check, no pause yet) | `--thunder-sub-mode tr` activation; `select_worker_async` TR branch with capacity gate; on no-capacity returns `Err(503)` (router maps to 503); request-timeout/resume-timeout validation at startup (§10.8) | New error path; behavior gating. | Force `mock_vllm` capacity = 0; client sees 503 with correct error_code; once capacity restored, normal admission resumes. |
| **P6** | Pause/resume + BFD scheduler + force-timeout | `RouterState::{pause_program, resume_program, mark_for_pause, clear_mark_and_pause, force_terminate_program}`; per-program `Notify` for resume signaling (§5.9); BFD `greedy_resume` + `pause_until_safe` in scheduler tick (§4.4 / §4.5 verbatim Python); `tokio::time::timeout` for resume wait + force-resume fallback (Q5.1) | Highest algorithmic risk: BFD edge cases, pause↔resume race, RAII guard cleanup correctness. | E2E: capacity exhaust → next admit pauses → mock frees → BFD migrates → unblock; separately force-timeout (resume_timeout=5s, never free) → force-resume kicks. |
| **P7** | gRPC path validation | Verify thunder works with `GRPC_REGULAR` (program_id extraction from `RequestContext` at `grpc/common/stages/worker_selection.rs:157`); add sglang gRPC mock; verify all 3 protocols on gRPC | gRPC `RequestContext` shape; ensure program_id field threaded correctly from request body through pipeline state. | gRPC e2e: 3 protocols → single `program_id` shared across; pause/resume works on gRPC backend. |
| **P8** | Profiling | Per-program timing dict (`request_arrive`, `pause_time`, `request_start`, `first_token`, `request_end`); `/thunder/profiles` endpoint mounted via `as_any()` downcast in `build_app`; per-stage Prometheus histograms | Optional polish. | `/thunder/profiles` returns all programs with full timing payload. |
| **P9** | Polish | `--thunder-use-acting-token-decay` toggle + `remaining_capacity_with_decay` formula (Q5.7); benchmark; deployment runbook (sidecar setup, ParkingRwLock upgrade hint per §10.6, request-timeout coupling per §10.8) | Wraps up. | Benchmark report committed; CHANGELOG entry; deployment runbook in `docs/`. |

Phases requiring e2e tests: P0 (mock pass-through), P1 (trait fallback parity), P2 (fixtures), P3 (cross-protocol single-program), P4 (metrics integration), P5 (capacity gate), P6 (pause/resume), P7 (gRPC). P8-P9 e2e optional.

---

---

## 12. Sign-off requirement (per-PR)

Every PR for Phases P0-P9 MUST:

1. Reference the phase number in commit message: `feat(thunder): <summary> (Phase N)` or `refactor(thunder): <summary> (Phase N)`.
2. Include an e2e test under `e2e_test/thunder/`.
3. Pass `cargo build --workspace`, `cargo test --workspace`, `cargo clippy --all-targets --all-features -- -D warnings`, `pytest e2e_test/thunder/ -v`.
4. Cite spec section number for any non-obvious design choice (e.g., "see §5.6 for usage tail extractor rationale").
5. Not introduce algorithmic deviation from Python that is not signed off in §2. If implementation finds a needed deviation, **pause and brainstorm with the user before proceeding**.

---

---

## 15. Appendix: file-level change inventory

### New files

| Path | Purpose | Approx LOC | Phase |
|---|---|---|---|
| `model_gateway/src/policies/thunder.rs` | ThunderPolicy + RouterState + scheduler | 800-1000 | P3-P6 |
| `model_gateway/src/routers/common/program_id.rs` | program_id extraction helpers | 80 | P1 |
| `model_gateway/src/routers/http/usage_tail.rs` | HTTP path streaming SSE tail extractor (D-1) | 80 | P3 |
| `e2e_test/thunder/mock_vllm.py` | Mock backend (lifted from old worktree, extended for /v1/messages + /v1/responses paths) | ~320 | P2 |
| `e2e_test/thunder/conftest.py` | pytest fixtures | 60 | P2 |
| `e2e_test/thunder/test_phase{0..7}.py` | Per-phase e2e tests | 100-200 each | per-phase |

### Modified files

| Path | Change | Phase |
|---|---|---|
| `crates/protocols/src/common.rs:40` | Add `fn program_id_hint(&self) -> Option<&str>` default-None to `GenerationRequest` trait | P0 |
| `crates/protocols/src/messages.rs:177` | Add `Metadata.program_id: Option<String>` (skip_serializing_if); impl `GenerationRequest` for `CreateMessageRequest` | P0 |
| `crates/protocols/src/chat.rs`, `crates/protocols/src/responses.rs`, etc. | Implement `program_id_hint` on each in-scope generation type | P1 |
| `model_gateway/src/routers/http/router.rs` | Add `route_messages` impl as pass-through to `route_typed_request(.., "/v1/messages", ..)`; line 175 sync→async migration; line 697/908 streaming branches wrap with usage_tail extractor when policy is thunder | P0 + P1 + P3 |
| `model_gateway/src/routers/grpc/common/stages/worker_selection.rs:157` | Sync→async migration; populate program_id from RequestContext | P1 |
| `model_gateway/src/policies/mod.rs:44, 167` | Trait extension + SelectWorkerInfo field + UsageEvent type | P1 |
| `model_gateway/src/policies/factory.rs:17, 80` | Thunder match arm | P3 |
| `model_gateway/src/policies/registry.rs:96` | (already wires `set_kv_event_monitor` per fork α audit; no change needed for that; potentially cache `Arc<ThunderPolicy>` for `/thunder/*` admin downcast) | P3 / P8 |
| `model_gateway/src/config/types.rs:347, 442` | `PolicyConfig::Thunder` variant + `name()` arm + defaults (D-4) | P3 |
| `model_gateway/src/main.rs:152` | Add `"thunder"` to value_parser whitelist; new "Thunder Policy" CLI heading + 5 flags | P3 |
| `model_gateway/src/server.rs:build_app` | (P8 only) Conditional `/thunder/programs`, `/thunder/profiles` routes | P8 |
| `model_gateway/src/observability/metrics.rs:151+` | "Layer 7: Thunder" `describe_*` registrations | P3-P6 incrementally |

**Explicitly NOT touched** (per §3.3 out-of-scope):
- `routers/openai/chat.rs`, `routers/openai/responses/route.rs`, `routers/openai/router.rs`
- `routers/anthropic/router.rs`, `routers/anthropic/streaming.rs`, etc.
- `routers/gemini/`
- `routers/http/pd_router.rs`
- `routers/grpc/pd_router.rs`
- `routers/grpc/common/stages/worker_selection.rs:276, 277` (PD prefill/decode pair)
- `routers/common/worker_selection.rs` (the `WorkerSelector` used by 3rd-party paths)

Approximate net LOC: +1300 to +1600 LOC (new files), +150 LOC of modifications. Phase 0 alone is ~50 LOC of trait + impl + 8 LOC of `route_messages` pass-through.

---
