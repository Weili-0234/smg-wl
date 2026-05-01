> Part of the [Thunder Policy spec](00-INDEX.md). Companion: [worklog](worklog.md) — design decisions with revisit conditions.

# Thunder — Cross-Protocol Deployment

## 7. Cross-protocol capacity counting (simplified)

### 7.1 Deployment shape

Every backend is registered to SMG ONCE per URL with `RuntimeType::Vllm`/`Sglang`/etc. (NOT `External`). The backend host runs a litellm-proxy sidecar that exposes all three generation endpoints (`/v1/chat/completions`, `/v1/messages`, `/v1/responses`) on the same port — sidecar performs Anthropic↔OpenAI translation upstream of vLLM/sglang. From SMG's perspective the backend is a single `Worker` with one URL.

### 7.2 Why cross-protocol counting just works

Trace: Client → SMG → `RouterManager::get_router_for_model` (`router_manager.rs:201-249`) → for non-External workers, dispatch by `(is_grpc, is_pd)` only — **protocol of the request is irrelevant to dispatch**. All three protocols → `HTTP_REGULAR` (or `GRPC_REGULAR` for gRPC backend). Same router → same `policy_registry.get_policy_or_default(model_id)` → same `ThunderPolicy` instance → same `RouterState.backends` map keyed by URL. `BackendState.active_program_tokens` aggregates programs regardless of which endpoint each program's requests came in on.

No `Program.required_provider` field needed. No `BackendState.supported_providers` set needed. No protocol filter in BFD greedy_resume. The `BackendState` struct in §4.2 omits these fields; the BFD pseudocode in §4.4 operates directly on the unfiltered `backend_list`.

### 7.3 What's lost vs. the prior dual-registration design

Nothing functional. The prior `THUNDER_POLICY_REQUIREMENTS.md §2.20a` was scoped against `RuntimeType::External` workers (where dispatch DOES branch on `provider_for_model`); that scope is dropped by §3.3, so the verification questions Q-DC1..5 and fail scenarios F-DC1..4 are moot.

### 7.4 Phase plan e2e validation

A simpler test replaces the prior cross-protocol fail-scenario suite (Phase 1):

- Single internal vLLM backend behind a litellm-proxy mock (or the existing `mock_vllm.py` extended to accept `/v1/messages` + `/v1/responses` paths)
- One client driving all 3 endpoints concurrently with the same `program_id`
- Assert `/thunder/programs` shows `step_count` incrementing across protocols on a single program; `smg_thunder_backend_active_program_tokens` for the URL aggregates across all 3 protocols

---
