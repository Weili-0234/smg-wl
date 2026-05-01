# Thunder P0 — HTTP Regular Router `/v1/messages` Pass-Through Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land Phase 0 of the Thunder integration by adding Anthropic Messages (`/v1/messages`) pass-through support to the HTTP Regular router, plus the underlying protocol-trait extensions that subsequent phases (P1+) will rely on. After this phase, the gateway can route Anthropic-shape requests to internal vLLM/sglang backends fronted by litellm-proxy sidecars **using existing policies** (cache_aware, round_robin, etc.). ThunderPolicy itself does not exist yet — that arrives in P3.

**Architecture:** Five Rust changes (one trait extension, one struct field, one match-arm wiring, one trait impl, one router impl) + Python test fixture lift + one Python e2e test. Total scope: ~80 LOC of Rust + ~30 LOC of Python mock extension + ~80 LOC of e2e test. All five Rust changes are protocol-agnostic prerequisites — they don't import or depend on `ThunderPolicy`. The pass-through is exercised end-to-end via cache_aware as the active policy, proving the seam works before P1 adds policy-async migration.

**Tech Stack:** Rust 1.x (workspace, 21 crates, strict clippy: `unwrap_used` denied / `expect_used` warned), `axum` + `tower` + `reqwest` for HTTP, `serde` for JSON, `tokio` for async. Python 3.12 + stdlib `http.server` for the mock backend. Pytest for e2e. SLURM `srun` for compute-node execution (per `docs/thunder/slurm-cluster.md`).

---

## Context (READ FIRST)

This plan is the executable form of:

- **`docs/thunder/10-phases.md` row P0** — phase contract: `cargo build/test/clippy` pass + e2e test under `e2e_test/thunder/`.
- **`docs/thunder/04-smg-integration.md` §5.5b/c/d/e** — the four sub-points (GenerationRequest extension, Metadata.program_id, route_to_endpoint arm, route_messages pass-through).
- **`docs/thunder/worklog.md` D-6** — the original P0-unblocker decision (HTTP_REGULAR doesn't have `route_messages` today; thunder needs it before pause/resume can apply to Anthropic traffic).
- **`docs/thunder/worklog.md` D-13** — the pre-flight verification that found `route_typed_request<T>` is protocol-agnostic, so a generic pass-through works without per-Anthropic carve-outs. The same entry expanded P0 scope by ~15 LOC (`extract_text_for_routing` for `CreateMessageRequest`).

**Out of scope** (do NOT touch in this phase):

- `model_gateway/src/policies/thunder.rs` — does not exist yet; arrives P3.
- `model_gateway/src/routers/anthropic/` — Anthropic 3rd-party path, deliberately untouched per §3.3 internal-only scope.
- `model_gateway/src/routers/http/pd_router.rs` and any PD code — PD support deferred (worklog D-7).
- `model_gateway/src/routers/grpc/` — gRPC path validation lives in P7.
- Any CLI flag or `PolicyConfig::Thunder` work — that's P3.

**Key file:line anchors** (verified 2026-04-30 on commit `fee7a129`):

| Anchor | What is there | What we change |
|---|---|---|
| `crates/protocols/src/common.rs:40` | `pub trait GenerationRequest` (3 methods) | Add 4th method `program_id_hint` with default-None |
| `crates/protocols/src/messages.rs:24-76` | `pub struct CreateMessageRequest` | Add `impl GenerationRequest for CreateMessageRequest` (4 methods) |
| `crates/protocols/src/messages.rs:178-182` | `pub struct Metadata { user_id: Option<String> }` | Add `program_id: Option<String>` field |
| `model_gateway/src/routers/grpc/utils/metrics.rs:8` | `route_to_endpoint(route)` 5-arm match | Add `"/v1/messages" => ENDPOINT_MESSAGES` arm |
| `model_gateway/src/routers/http/router.rs:1089` | `impl RouterTrait for Router` (the HTTP regular router) | Add `route_messages` 3-line pass-through |
| `model_gateway/src/routers/mod.rs:226` | `RouterTrait::route_messages` default = NOT_IMPLEMENTED | Unchanged — `Router` overrides the default |
| `model_gateway/src/server.rs:964` | `.route("/v1/messages", post(v1_messages))` | Unchanged — axum already routes the path |
| `model_gateway/src/observability/metrics.rs:387` | `pub const ENDPOINT_MESSAGES: &str = "messages"` | Unchanged — constant already exists |

Read these locations before starting if the spec citations are unclear.

---

## Pre-flight verification

- [ ] **PF.1: Confirm worktree + branch + commit**

```bash
cd /home/hkang/wl/smg-wl
git status --short
git rev-parse --abbrev-ref HEAD
git log --oneline -3
```

Expected output:
- `git status --short` shows only untracked files (e.g. `?? handoff.md`); no modified-tracked files.
- Branch is `thunder-policy`.
- HEAD is `fee7a129 docs(thunder): integrate ThunderAgent design spec as policy hierarchy` (or a later commit if P0 has been started).

If you are in `/home/hkang/wl/smg_thunder/` instead, **STOP** — that is the abandoned worktree. The active design and code lives in `smg-wl`.

- [ ] **PF.2: Confirm baseline `cargo build --workspace` is green**

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -5
```

Expected: `Finished ...` with no errors. If errors appear, do NOT begin P0 — they indicate environmental drift since the verified `04f9b2d6` baseline.

- [ ] **PF.3: Read the four spec sections referenced in `Context` above**

Read each file once so you can answer: "why does P0 add `program_id_hint` to the trait when ThunderPolicy doesn't exist yet?" (Answer: because `SelectWorkerInfo.program_id` extraction in P1 reads it via the trait — adding the trait method in P0 lets non-thunder phases compile cleanly without thunder coupling, and the default-None means existing impls keep working without modification.)

- [ ] **PF.4: Delete `handoff.md` after reading**

```bash
cd /home/hkang/wl/smg-wl
ls handoff.md && rm handoff.md && git status --short | head
```

Per the handoff itself: it is session continuity, not project artifact. The same content is preserved in `worklog.md` D-9..D-15 + `00-INDEX.md`.

---

## File Structure

After P0, the following files exist or are modified:

```
/home/hkang/wl/smg-wl/                        ← worktree, branch: thunder-policy
├── crates/protocols/src/
│   ├── common.rs                              ← MODIFIED (Task 1: +1 trait method)
│   └── messages.rs                            ← MODIFIED (Task 2: +1 field; Task 4: +1 impl block)
├── model_gateway/src/
│   ├── routers/
│   │   ├── http/router.rs                     ← MODIFIED (Task 5: +1 trait method impl)
│   │   └── grpc/utils/metrics.rs              ← MODIFIED (Task 3: +1 match arm)
│   └── (no other src changes)
└── e2e_test/thunder/                          ← NEW DIRECTORY
    ├── __init__.py                            ← NEW (empty marker)
    ├── conftest.py                            ← NEW (~50 LOC, fixtures)
    ├── mock_vllm.py                           ← NEW (lifted from smg_thunder + extended +30 LOC)
    └── test_phase0_messages_passthrough.py    ← NEW (~80 LOC)
```

Files **NOT** touched: anything under `policies/`, `routers/anthropic/`, `routers/grpc/router.rs`, `config/`, `main.rs`, `server.rs`, `observability/metrics.rs`, `worker/`, all CLI surface, all docs.

The Python e2e test directory is brand new in `smg-wl`. The `smg_thunder` worktree has an older `e2e_test/thunder/mock_vllm.py` (290 LOC) that is the source for Task 6.

---

## Task 1: Add `program_id_hint` to `GenerationRequest` trait

**Files:**
- Modify: `crates/protocols/src/common.rs:40-49` (trait definition)
- Test: `crates/protocols/src/common.rs` `#[cfg(test)] mod tests` block at line 797 (extend)

**Why this is task 1:** Other tasks depend on this method existing on the trait. Default-None means no existing impl breaks. Tasks 4 and 5 will both call `req.program_id_hint()` at the call site.

`★ Insight ─────────────────────────────────────`
- A default trait-method body is the right Rust idiom for an "additive" extension: existing impls (ChatCompletionRequest, ResponsesRequest, GenerateRequest, EmbeddingRequest, RerankRequest, CompletionRequest) inherit the default automatically — no editing 6 files just to opt in. Only the type that actually has a notion of program_id (CreateMessageRequest in Task 4) needs to override.
- Returning `Option<&str>` (rather than `&str` or `String`) signals "may be absent" and avoids allocation on the hot path. Callers do `.unwrap_or("default")` per Q5.2 fallback when they want a string.
`─────────────────────────────────────────────────`

- [ ] **Step 1.1: Write the failing test**

Open `crates/protocols/src/common.rs` and append to the `#[cfg(test)] mod tests` block (around line 856, after the existing `conversation_ref_is_empty` test):

```rust
    #[test]
    fn program_id_hint_default_returns_none() {
        struct Stub;
        impl GenerationRequest for Stub {
            fn is_stream(&self) -> bool { false }
            fn get_model(&self) -> Option<&str> { None }
            fn extract_text_for_routing(&self) -> String { String::new() }
        }
        assert_eq!(Stub.program_id_hint(), None);
    }
```

- [ ] **Step 1.2: Run the test to verify it fails**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p openai-protocol common::tests::program_id_hint_default_returns_none 2>&1 | tail -10
```

Expected: compile error, e.g. `error[E0599]: no method named program_id_hint found for type Stub`. This is the red bar.

- [ ] **Step 1.3: Add the trait method with a default impl**

Edit `crates/protocols/src/common.rs:40-49` — change the trait body to add a fourth method:

```rust
/// Trait for unified access to generation request properties
/// Implemented by ChatCompletionRequest, CompletionRequest, GenerateRequest,
/// EmbeddingRequest, RerankRequest, and ResponsesRequest
pub trait GenerationRequest: Send + Sync {
    /// Check if the request is for streaming
    fn is_stream(&self) -> bool;

    /// Get the model name if specified
    fn get_model(&self) -> Option<&str>;

    /// Extract text content for routing decisions
    fn extract_text_for_routing(&self) -> String;

    /// Optional program identifier used by program-aware policies (e.g. Thunder)
    /// to group requests for capacity tracking and pause/resume scheduling.
    /// Default returns None; implementers that carry a program_id (e.g.
    /// `CreateMessageRequest` via `metadata.program_id`) override this.
    fn program_id_hint(&self) -> Option<&str> {
        None
    }
}
```

(Only the trailing `fn program_id_hint` block is new; preserve the three existing methods exactly.)

- [ ] **Step 1.4: Run the test to verify it passes**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p openai-protocol common::tests::program_id_hint_default_returns_none 2>&1 | tail -5
```

Expected: `test ... ok`.

- [ ] **Step 1.5: Run the workspace build to confirm no regressions**

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -5
```

Expected: `Finished ...`. If a downstream crate fails because of an `impl GenerationRequest for X` block lacking `program_id_hint`, that means the default isn't taking effect — likely a syntactic issue (e.g. method written outside the trait body). Re-read the diff in Step 1.3 and ensure the new method is inside the trait braces.

- [ ] **Step 1.6: Commit**

```bash
cd /home/hkang/wl/smg-wl
git add crates/protocols/src/common.rs
git commit -m "feat(protocols): add program_id_hint to GenerationRequest trait (Phase 0)

Defaults to None so existing impls inherit the new method without
modification. CreateMessageRequest will override in a follow-up commit
to read from Metadata.program_id; ThunderPolicy in Phase 3 will read
the hint via SelectWorkerInfo.program_id.

Refs: docs/thunder/04-smg-integration.md §5.5b, worklog D-13"
```

---

## Task 2: Add `Metadata.program_id` field

**Files:**
- Modify: `crates/protocols/src/messages.rs:177-182` (Metadata struct)
- Test: append to `messages.rs` `#[cfg(test)]` block (or create one if absent)

**Why this is task 2:** Task 4 (impl GenerationRequest for CreateMessageRequest) reads `self.metadata.as_ref()?.program_id`. Adding the field first means the impl compiles when written.

`★ Insight ─────────────────────────────────────`
- `#[serde(skip_serializing_if = "Option::is_none")]` keeps wire compatibility with the existing Anthropic Messages JSON shape — clients that don't send `program_id` see an unchanged response surface, and SMG won't echo a stray `"program_id": null`.
- The `Metadata` struct currently has only `user_id`. Anthropic's spec defines `metadata.user_id` as a hashed-end-user-identifier; we are extending `metadata` with an SMG-specific `program_id`. Litellm-proxy strips arbitrary fields from `metadata`, but per the spec §10.5 footgun + worklog D-13 verification, thunder reads `program_id` at SMG entry (BEFORE the request hits the sidecar), so the strip doesn't matter.
`─────────────────────────────────────────────────`

- [ ] **Step 2.1: Check whether `messages.rs` has an existing `#[cfg(test)]` mod**

```bash
cd /home/hkang/wl/smg-wl
grep -n '#\[cfg(test)\]' crates/protocols/src/messages.rs | head
```

If a `#[cfg(test)] mod tests { ... }` block already exists at end of file, append to it. If not, create one at the end of the file (after the last item).

- [ ] **Step 2.2: Write the failing test**

Append to (or create) the tests mod in `crates/protocols/src/messages.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn metadata_deserializes_program_id() {
        let v = json!({"user_id": "u1", "program_id": "agent-step-42"});
        let m: Metadata = serde_json::from_value(v).unwrap();
        assert_eq!(m.program_id.as_deref(), Some("agent-step-42"));
        assert_eq!(m.user_id.as_deref(), Some("u1"));
    }

    #[test]
    fn metadata_serializes_skips_none_program_id() {
        let m = Metadata { user_id: Some("u1".into()), program_id: None };
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v, json!({"user_id": "u1"}));
    }
}
```

(If the tests mod already existed, just paste the two `#[test]` functions inside its braces and skip the wrapping `mod tests { ... }`.)

- [ ] **Step 2.3: Run the test to verify it fails**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p openai-protocol messages::tests::metadata 2>&1 | tail -10
```

Expected: compile error, e.g. `error[E0560]: struct Metadata has no field named program_id`.

- [ ] **Step 2.4: Add the field**

Edit `crates/protocols/src/messages.rs:177-182` so that the struct becomes:

```rust
/// Request metadata
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Metadata {
    /// An external identifier for the user who is associated with the request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,

    /// SMG-specific program identifier used by program-aware load-balancing
    /// policies (Thunder) to group requests for capacity tracking and
    /// pause/resume scheduling. Not part of the upstream Anthropic spec;
    /// safely omitted from outbound requests when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub program_id: Option<String>,
}
```

- [ ] **Step 2.5: Run the test to verify it passes**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p openai-protocol messages::tests::metadata 2>&1 | tail -5
```

Expected: 2 tests `ok`.

- [ ] **Step 2.6: Verify `cargo build --workspace` still passes**

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -5
```

If a downstream test fixture used struct-literal `Metadata { user_id: ... }` it now fails to compile. Search:

```bash
cd /home/hkang/wl/smg-wl
grep -rn 'Metadata {' --include='*.rs' | grep -v 'src/messages.rs'
```

If any callers exist, update them with `program_id: None,` or use `..Default::default()` if `Metadata` derives `Default` (it does not in this codebase, so add the field explicitly).

- [ ] **Step 2.7: Commit**

```bash
cd /home/hkang/wl/smg-wl
git add crates/protocols/src/messages.rs
git commit -m "feat(protocols): add Metadata.program_id for program-aware routing (Phase 0)

skip_serializing_if keeps wire compatibility with existing Anthropic
Messages clients. Read by ThunderPolicy in Phase 3 via
GenerationRequest::program_id_hint -> SelectWorkerInfo.program_id.
Litellm-proxy strips it on its way to the backend, but thunder reads
it at SMG entry — see footgun §10.5.

Refs: docs/thunder/04-smg-integration.md §5.5b, worklog D-13"
```

---

## Task 3: Wire `ENDPOINT_MESSAGES` arm in `route_to_endpoint`

**Files:**
- Modify: `model_gateway/src/routers/grpc/utils/metrics.rs:8-18`
- Test: append `#[cfg(test)]` block to same file (no existing test block)

**Why now:** Independent of Tasks 1/2/4/5. Adding it early reduces cognitive load for the bigger tasks. Without this arm, `/v1/messages` traffic silently buckets into `endpoint="other"` in Prometheus — observable footgun called out in worklog D-13.

`★ Insight ─────────────────────────────────────`
- This is NOT gRPC-specific: the file lives at `routers/grpc/utils/metrics.rs` for historical reasons, but it's used by both HTTP and gRPC paths. The HTTP router imports it at `routers/http/router.rs:46`: `use crate::routers::grpc::utils::{error_type_from_status, route_to_endpoint};`. Don't move the function in this phase; the existing crate boundary is fine.
- `metrics_labels::ENDPOINT_MESSAGES = "messages"` already exists at `observability/metrics.rs:387` — the constant is just unwired. We're plumbing, not declaring.
`─────────────────────────────────────────────────`

- [ ] **Step 3.1: Write the failing test**

Append to `model_gateway/src/routers/grpc/utils/metrics.rs` (after the `error_type_from_status` function, line ~30):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_to_endpoint_messages_is_messages() {
        assert_eq!(route_to_endpoint("/v1/messages"), metrics_labels::ENDPOINT_MESSAGES);
        assert_eq!(route_to_endpoint("/v1/messages"), "messages");
    }

    #[test]
    fn route_to_endpoint_unknown_is_other() {
        assert_eq!(route_to_endpoint("/v1/foo"), "other");
    }
}
```

- [ ] **Step 3.2: Run the test to verify it fails**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p model_gateway routers::grpc::utils::metrics::tests 2>&1 | tail -10
```

Expected: `route_to_endpoint_messages_is_messages` fails with `assertion left == right` showing left = `"other"`.

- [ ] **Step 3.3: Add the match arm**

Edit `model_gateway/src/routers/grpc/utils/metrics.rs:8-18`:

```rust
/// Map route path to endpoint label for metrics
pub(crate) fn route_to_endpoint(route: &str) -> &'static str {
    match route {
        "/v1/chat/completions" => metrics_labels::ENDPOINT_CHAT,
        "/generate" => metrics_labels::ENDPOINT_GENERATE,
        "/v1/completions" => metrics_labels::ENDPOINT_COMPLETIONS,
        "/v1/rerank" => metrics_labels::ENDPOINT_RERANK,
        "/v1/responses" => metrics_labels::ENDPOINT_RESPONSES,
        "/v1/messages" => metrics_labels::ENDPOINT_MESSAGES,
        "/v1/audio/transcriptions" => metrics_labels::ENDPOINT_AUDIO_TRANSCRIPTIONS,
        _ => "other",
    }
}
```

(Only the `"/v1/messages" => ...` line is new — keep the others in their original order; the placement above `audio/transcriptions` keeps OpenAI-Chat / Gen / Completions / Rerank / Responses / Messages adjacent.)

- [ ] **Step 3.4: Run the test to verify it passes**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p model_gateway routers::grpc::utils::metrics::tests 2>&1 | tail -5
```

Expected: 2 tests `ok`.

- [ ] **Step 3.5: Commit**

```bash
cd /home/hkang/wl/smg-wl
git add model_gateway/src/routers/grpc/utils/metrics.rs
git commit -m "feat(metrics): wire /v1/messages route_to_endpoint label (Phase 0)

Without this arm, /v1/messages traffic buckets into endpoint=\"other\"
in smg_router_* Prometheus series. ENDPOINT_MESSAGES constant has
existed at observability/metrics.rs:387 since the messages router
arrived; this commit just connects the label to the route.

Refs: docs/thunder/04-smg-integration.md §5.5e, worklog D-13"
```

---

## Task 4: Implement `GenerationRequest` for `CreateMessageRequest`

**Files:**
- Modify: `crates/protocols/src/messages.rs` (add `use` at top + `impl` block after the inherent `impl CreateMessageRequest` at line 82-106)
- Test: append to the `#[cfg(test)] mod tests` block created in Task 2

**Why now:** Tasks 1+2 are done; this task uses both. Task 5 will call `req.program_id_hint()` via the trait, which only works after this impl exists.

`★ Insight ─────────────────────────────────────`
- `extract_text_for_routing` for Anthropic shape mirrors `chat.rs:598-655` (the OpenAI version) but iterates `Option<SystemContent>` (Anthropic's separate system prompt) plus `Vec<InputMessage>` (each carrying `InputContent` that's String OR Vec of typed blocks). For routing decisions only Text blocks matter — Image / Document / ToolUse / ToolResult / Thinking variants have no useful text and would dilute the cache-aware routing signal. Spec D-13 explicitly scoped this to ~30-50 LOC; we don't extract from ToolResultBlock content even though it could be text.
- The inherent methods `is_stream` (line 84) and `get_model` (line 89) already exist on `CreateMessageRequest`. The trait impl can call them — but it's cleaner to inline the bodies in the trait impl since the trait signature differs (`get_model` returns `Option<&str>` in the trait but `&str` directly inherent). Inlining avoids adapter wrappers.
`─────────────────────────────────────────────────`

- [ ] **Step 4.1: Add the `use` import for `GenerationRequest`**

Open `crates/protocols/src/messages.rs:1-12` (the imports). The current block is:

```rust
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use validator::Validate;

use crate::{skills::MessagesSkillRef, validated::Normalizable};
```

Change the last line to also import `GenerationRequest`:

```rust
use crate::{common::GenerationRequest, skills::MessagesSkillRef, validated::Normalizable};
```

(Sort alphabetically per `rustfmt.toml`'s `group_imports = "StdExternalCrate"` — `common` before `skills` before `validated`.)

- [ ] **Step 4.2: Write the failing test**

In the existing `#[cfg(test)] mod tests` block in `messages.rs` (created or extended in Task 2), add four tests:

```rust
    #[test]
    fn generation_request_is_stream_default_false() {
        let req = CreateMessageRequest {
            model: "test-model".into(),
            messages: vec![],
            max_tokens: 100,
            metadata: None,
            service_tier: None,
            stop_sequences: None,
            stream: None,
            system: None,
            temperature: None,
            thinking: None,
            tool_choice: None,
            tools: None,
            top_k: None,
            top_p: None,
            container: None,
            mcp_servers: None,
        };
        assert!(!<CreateMessageRequest as GenerationRequest>::is_stream(&req));
        assert_eq!(<CreateMessageRequest as GenerationRequest>::get_model(&req), Some("test-model"));
    }

    #[test]
    fn generation_request_program_id_hint_reads_metadata() {
        let with_id = CreateMessageRequest {
            model: "m".into(),
            messages: vec![],
            max_tokens: 1,
            metadata: Some(Metadata { user_id: None, program_id: Some("p1".into()) }),
            service_tier: None,
            stop_sequences: None, stream: None, system: None, temperature: None,
            thinking: None, tool_choice: None, tools: None, top_k: None, top_p: None,
            container: None, mcp_servers: None,
        };
        assert_eq!(with_id.program_id_hint(), Some("p1"));

        let without = CreateMessageRequest { metadata: None, ..with_id.clone() };
        assert_eq!(without.program_id_hint(), None);

        let no_pid = CreateMessageRequest {
            metadata: Some(Metadata { user_id: Some("u".into()), program_id: None }),
            ..with_id
        };
        assert_eq!(no_pid.program_id_hint(), None);
    }

    #[test]
    fn extract_text_for_routing_string_content() {
        let req = CreateMessageRequest {
            model: "m".into(),
            messages: vec![InputMessage {
                role: Role::User,
                content: InputContent::String("hello world".into()),
            }],
            max_tokens: 1,
            metadata: None,
            service_tier: None, stop_sequences: None, stream: None,
            system: Some(SystemContent::String("you are helpful".into())),
            temperature: None, thinking: None, tool_choice: None,
            tools: None, top_k: None, top_p: None, container: None, mcp_servers: None,
        };
        assert_eq!(req.extract_text_for_routing(), "you are helpful hello world");
    }

    #[test]
    fn extract_text_for_routing_blocks_skip_non_text() {
        let req = CreateMessageRequest {
            model: "m".into(),
            messages: vec![InputMessage {
                role: Role::User,
                content: InputContent::Blocks(vec![
                    InputContentBlock::Text(TextBlock {
                        text: "what is in this image?".into(),
                        cache_control: None,
                        citations: None,
                    }),
                    InputContentBlock::Image(ImageBlock {
                        source: ImageSource::Url { url: "https://x/y.png".into() },
                        cache_control: None,
                    }),
                ]),
            }],
            max_tokens: 1,
            metadata: None,
            service_tier: None, stop_sequences: None, stream: None, system: None,
            temperature: None, thinking: None, tool_choice: None,
            tools: None, top_k: None, top_p: None, container: None, mcp_servers: None,
        };
        // Image block is skipped; only the Text block contributes.
        assert_eq!(req.extract_text_for_routing(), "what is in this image?");
    }
```

- [ ] **Step 4.3: Run the tests to verify they fail**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p openai-protocol messages::tests::generation_request 2>&1 | tail -15
cargo test -p openai-protocol messages::tests::extract_text 2>&1 | tail -15
```

Expected: compile error mentioning `the trait GenerationRequest is not implemented for CreateMessageRequest`.

- [ ] **Step 4.4: Add the impl block**

Insert immediately after the existing `impl CreateMessageRequest { ... }` block (which ends around line 106) — i.e. before `impl Tool { ... }`:

```rust
impl GenerationRequest for CreateMessageRequest {
    fn is_stream(&self) -> bool {
        self.stream.unwrap_or(false)
    }

    fn get_model(&self) -> Option<&str> {
        Some(&self.model)
    }

    fn extract_text_for_routing(&self) -> String {
        let mut buffer = String::new();
        let mut has_content = false;

        if let Some(system) = &self.system {
            match system {
                SystemContent::String(s) => {
                    if !s.is_empty() {
                        buffer.push_str(s);
                        has_content = true;
                    }
                }
                SystemContent::Blocks(blocks) => {
                    for block in blocks {
                        if !block.text.is_empty() {
                            if has_content {
                                buffer.push(' ');
                            }
                            buffer.push_str(&block.text);
                            has_content = true;
                        }
                    }
                }
            }
        }

        for msg in &self.messages {
            match &msg.content {
                InputContent::String(s) => {
                    if !s.is_empty() {
                        if has_content {
                            buffer.push(' ');
                        }
                        buffer.push_str(s);
                        has_content = true;
                    }
                }
                InputContent::Blocks(blocks) => {
                    for block in blocks {
                        if let InputContentBlock::Text(text_block) = block {
                            if !text_block.text.is_empty() {
                                if has_content {
                                    buffer.push(' ');
                                }
                                buffer.push_str(&text_block.text);
                                has_content = true;
                            }
                        }
                    }
                }
            }
        }

        buffer
    }

    fn program_id_hint(&self) -> Option<&str> {
        self.metadata.as_ref()?.program_id.as_deref()
    }
}
```

This is ~55 LOC; spec budget was 30-50 + program_id_hint, so this is on-target.

- [ ] **Step 4.5: Run the tests to verify they pass**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p openai-protocol messages::tests 2>&1 | tail -10
```

Expected: all tests in the messages tests mod pass (Task 2's metadata tests + Task 4's 4 new tests = 6 total).

- [ ] **Step 4.6: Run the workspace build + clippy**

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -5
cargo clippy -p openai-protocol --all-targets --all-features -- -D warnings 2>&1 | tail -10
```

Expected: both green. Common clippy fail mode here: an `unused_imports` warning if Task 2 introduced imports we're now using. Adjust the import list as needed.

- [ ] **Step 4.7: Commit**

```bash
cd /home/hkang/wl/smg-wl
git add crates/protocols/src/messages.rs
git commit -m "feat(protocols): impl GenerationRequest for CreateMessageRequest (Phase 0)

Routing-text extraction handles SystemContent (String|Blocks) +
InputContent::{String,Blocks} and filters InputContentBlock::Text only,
skipping Image/Document/ToolUse/ToolResult/Thinking variants.

program_id_hint reads from Metadata.program_id (added in the previous
commit), enabling Phase 1's SelectWorkerInfo.program_id population for
Anthropic Messages traffic without changing the router code path.

Refs: docs/thunder/04-smg-integration.md §5.5b, worklog D-13"
```

---

## Task 5: Add `Router::route_messages` pass-through

**Files:**
- Modify: `model_gateway/src/routers/http/router.rs` — imports near line 11-21 + new method in `impl RouterTrait for Router` block (after `route_responses` at line 1148)

**Why now:** Tasks 1-4 have made the trait + impl + types + arm available; this task connects the dispatcher (`RouterManager::route_messages` at `router_manager.rs:598`) to a concrete HTTP-regular implementation. Without this, the default `RouterTrait::route_messages` at `mod.rs:226` returns `NOT_IMPLEMENTED` for HTTP regular routers — the very gap that motivates P0.

`★ Insight ─────────────────────────────────────`
- We do NOT need to add `route_messages` to `RouterTrait` — it's already there with a default impl (mod.rs:226). We're overriding the default for `Router` specifically. Other routers (anthropic at `routers/anthropic/router.rs:91`, grpc at `routers/grpc/router.rs:615`) already override; HTTP regular was the only gap.
- `route_typed_request<T>` is generic over `T: GenerationRequest + serde::Serialize + Clone`. `CreateMessageRequest` derives `Serialize` (line 22) and `Clone` (line 22) and now (Task 4) implements `GenerationRequest`. All three bounds are satisfied — the call compiles with no extra glue.
`─────────────────────────────────────────────────`

- [ ] **Step 5.1: Add `CreateMessageRequest` to the imports**

Open `model_gateway/src/routers/http/router.rs:11-21`. Current import group:

```rust
use openai_protocol::{
    chat::ChatCompletionRequest,
    classify::ClassifyRequest,
    common::GenerationRequest,
    completion::CompletionRequest,
    embedding::EmbeddingRequest,
    generate::GenerateRequest,
    rerank::{RerankRequest, RerankResponse, RerankResult},
    responses::ResponsesRequest,
    transcription::TranscriptionRequest,
};
```

Add `messages::CreateMessageRequest` between `generate::GenerateRequest` and `rerank::*` (alphabetical):

```rust
use openai_protocol::{
    chat::ChatCompletionRequest,
    classify::ClassifyRequest,
    common::GenerationRequest,
    completion::CompletionRequest,
    embedding::EmbeddingRequest,
    generate::GenerateRequest,
    messages::CreateMessageRequest,
    rerank::{RerankRequest, RerankResponse, RerankResult},
    responses::ResponsesRequest,
    transcription::TranscriptionRequest,
};
```

- [ ] **Step 5.2: Add the `route_messages` impl method**

In the `impl RouterTrait for Router` block, find `route_responses` (line ~1139-1148) — it ends with `}` followed by a blank line and then `async fn cancel_response`. Insert the new method **between** `route_responses` and `cancel_response`:

```rust
    async fn route_messages(
        &self,
        headers: Option<&HeaderMap>,
        _tenant_meta: &TenantRequestMeta,
        body: &CreateMessageRequest,
        model_id: &str,
    ) -> Response {
        self.route_typed_request(headers, body, "/v1/messages", model_id)
            .await
    }
```

(Mirrors `route_chat` and `route_responses` structure exactly — the only differences are the body type and the route path string.)

- [ ] **Step 5.3: Verify the build**

```bash
cd /home/hkang/wl/smg-wl
cargo build -p model_gateway 2>&1 | tail -10
```

Expected: green. If you see an error like `the method route_typed_request is private` — confirm `route_typed_request` is a method on `Router` itself (not a trait method), defined at `router.rs:196`. It is `pub async fn`, so the visibility is fine; the failure would more likely be a missing `Clone` or `Serialize` bound, which Tasks 1-4 already provide.

- [ ] **Step 5.4: Add a smoke unit test (optional but recommended)**

Append to the `#[cfg(test)] mod tests` block at the bottom of `router.rs` (around line 1227+):

```rust
    #[test]
    fn route_messages_compiles_with_create_message_request() {
        // Compile-time assertion: route_typed_request accepts CreateMessageRequest.
        // (Real e2e covered by e2e_test/thunder/test_phase0_messages_passthrough.py.)
        fn _assert_bounds<T: GenerationRequest + serde::Serialize + Clone + Send + Sync>() {}
        _assert_bounds::<CreateMessageRequest>();
    }
```

This catches regressions in the trait bounds at `cargo test` time without needing the full HTTP stack spun up.

- [ ] **Step 5.5: Run the unit test**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p model_gateway routers::http::router::tests::route_messages_compiles 2>&1 | tail -5
```

Expected: `ok` (or compile failure if Tasks 1-4 are incomplete — re-verify them).

- [ ] **Step 5.6: Commit**

```bash
cd /home/hkang/wl/smg-wl
git add model_gateway/src/routers/http/router.rs
git commit -m "feat(router): impl Router::route_messages pass-through (Phase 0)

The HTTP regular Router (used by internal vLLM/sglang backends fronted
by litellm-proxy sidecars) now overrides RouterTrait::route_messages
with a generic pass-through to route_typed_request. The pass-through is
protocol-agnostic per worklog D-13 verification: route_typed_request
makes no OpenAI-specific assumptions on T beyond the GenerationRequest
trait methods (now implemented on CreateMessageRequest).

This unblocks Anthropic Messages traffic on the HTTP regular path with
existing policies (cache_aware, round_robin); ThunderPolicy in Phase 3
will plug in via the same trait once available.

Refs: docs/thunder/04-smg-integration.md §5.5d, worklog D-6/D-13"
```

---

## Task 6: Lift `mock_vllm.py` and extend with `/v1/messages`

**Files:**
- Create: `e2e_test/thunder/__init__.py`
- Create: `e2e_test/thunder/mock_vllm.py` (lifted from `/home/hkang/wl/smg_thunder/e2e_test/thunder/mock_vllm.py` + extension)
- Create: `e2e_test/thunder/conftest.py` (fixtures)

**Why now:** Rust changes complete; need a backend that accepts `/v1/messages`. The existing `smg_thunder` mock handles only `/v1/chat/completions`. We extend it with `/v1/messages` returning the **same** OpenAI-shape `chat.completion` body — this simulates litellm-proxy's translation behavior (Anthropic in → OpenAI in upstream → OpenAI out → Anthropic out, except for P0 simplicity we skip the Anthropic-out translation and assert the body byte-stream as-is).

`★ Insight ─────────────────────────────────────`
- Keeping the mock pure stdlib (`http.server.ThreadingHTTPServer`) avoids dragging FastAPI/uvicorn into the test environment. The original mock was deliberately zero-dep so any team member can run it without `uv pip install` — preserve that.
- For P0 the mock returns OpenAI-shape responses for both `/v1/chat/completions` and `/v1/messages`. The "real" sidecar topology (P2) will translate Anthropic-out — but P0 only proves the gateway forwards the bytes. A more faithful `Anthropic-shape` response can wait until P3+ when we test cross-protocol program_id stickiness.
- File rename from `mock_vllm.py` to `mock_sglang_compat.py` (per worklog D-12) is a P2 task — keep the legacy name in P0 so the diff is minimal.
`─────────────────────────────────────────────────`

- [ ] **Step 6.1: Create the directory and empty `__init__.py`**

```bash
cd /home/hkang/wl/smg-wl
mkdir -p e2e_test/thunder
touch e2e_test/thunder/__init__.py
```

- [ ] **Step 6.2: Copy `mock_vllm.py` from the legacy worktree**

```bash
cp /home/hkang/wl/smg_thunder/e2e_test/thunder/mock_vllm.py /home/hkang/wl/smg-wl/e2e_test/thunder/mock_vllm.py
```

Verify the file is 290 LOC: `wc -l /home/hkang/wl/smg-wl/e2e_test/thunder/mock_vllm.py` should print `290`.

- [ ] **Step 6.3: Extend the `do_POST` dispatcher with a `/v1/messages` arm**

Open `/home/hkang/wl/smg-wl/e2e_test/thunder/mock_vllm.py`. Find the `do_POST` method (around line 123-129):

```python
        def do_POST(self):
            if self.path == "/v1/chat/completions":
                self._handle_chat()
            elif self.path == "/control/capacity":
                self._handle_capacity_update()
            else:
                self._send_text(404, f"not found: {self.path}\n")
```

Replace with:

```python
        def do_POST(self):
            if self.path == "/v1/chat/completions":
                self._handle_chat()
            elif self.path == "/v1/messages":
                self._handle_messages()
            elif self.path == "/control/capacity":
                self._handle_capacity_update()
            else:
                self._send_text(404, f"not found: {self.path}\n")
```

- [ ] **Step 6.4: Add the `_handle_messages` method**

Find the existing `_handle_chat` method (around line 132-149). After `_non_stream_chat` and `_stream_chat` (around line 200ish), insert `_handle_messages`:

```python
        # ---------- /v1/messages handler (Phase 0 pass-through tests) ----------
        def _handle_messages(self) -> None:
            """Anthropic Messages payload arrives here. For Phase 0 tests we just
            log the program_id and return an OpenAI-shape chat.completion body —
            this matches the litellm-proxy translation behavior the real
            sidecar topology will use (Anthropic in → OpenAI internally).

            Real Anthropic-shape responses are out of scope for P0; P3+ tests
            will exercise cross-protocol program_id stickiness with proper
            Anthropic-out translation.
            """
            payload = self._read_json_body()
            with state.lock:
                state.request_count += 1
                req_idx = state.request_count
            stream = bool(payload.get("stream"))
            messages = payload.get("messages") or []
            metadata = payload.get("metadata") or {}
            program_id = metadata.get("program_id")
            LOG.info(
                "messages #%d stream=%s messages=%d program_id=%s",
                req_idx, stream, len(messages), program_id or "<none>",
            )
            if stream:
                # Phase 0 doesn't exercise streaming on /v1/messages; emit
                # a minimal SSE that pass-through tests can ignore. Real
                # streaming arrives in P3+.
                self._stream_chat(req_idx)
                return
            # Reuse the OpenAI-shape non-stream payload.
            content = state.canned_content
            prompt_tokens = sum(len(str(m.get("content", ""))) // 4 for m in messages) or 1
            completion_tokens = max(1, len(content) // 4)
            payload_out = {
                "id": f"chatcmpl-mock-{req_idx}",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": state.model_name,
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": content},
                    "finish_reason": "stop",
                }],
                "usage": {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": prompt_tokens + completion_tokens,
                },
                # Echo the program_id so the e2e test can assert plumbing worked.
                "_mock_echo_program_id": program_id,
            }
            self._send_json(200, payload_out)
```

(`_mock_echo_program_id` is a non-spec field — the e2e test reads it to assert the body reached the backend with `metadata.program_id` intact.)

- [ ] **Step 6.5: Smoke-test the mock standalone**

```bash
cd /home/hkang/wl/smg-wl
python3 e2e_test/thunder/mock_vllm.py --port 18999 &
sleep 1
curl -sS -X POST http://localhost:18999/v1/messages \
    -H "content-type: application/json" \
    -d '{"model":"test","max_tokens":10,"messages":[{"role":"user","content":"hi"}],"metadata":{"program_id":"smoke-1"}}' \
    | python3 -m json.tool
kill %1 2>/dev/null; wait 2>/dev/null
```

Expected: JSON output containing `"_mock_echo_program_id": "smoke-1"` and `"object": "chat.completion"`. If the curl command hangs, the `do_POST` dispatcher likely fell through to the 404 branch — re-check Step 6.3.

- [ ] **Step 6.6: Create `conftest.py` with helper fixtures**

```bash
cat > /home/hkang/wl/smg-wl/e2e_test/thunder/conftest.py <<'EOF'
"""Pytest fixtures for thunder Phase 0 e2e tests.

For Phase 0 we run the mock backend and the SMG binary on the same host
(the test runner — currently the SLURM compute node via srun). Phase 2
will introduce launcher scripts + 4 sglang backends; for P0 a single
mock + a single SMG is enough.
"""
from __future__ import annotations

import os
import socket
import subprocess
import time
from contextlib import closing

import pytest
import requests

THUNDER_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(THUNDER_DIR, "..", ".."))


def _free_port() -> int:
    """Bind a transient socket to find an unused TCP port."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_http(url: str, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=1)
            if r.status_code < 500:
                return
        except Exception as e:
            last_err = e
        time.sleep(0.1)
    raise RuntimeError(f"timeout waiting for {url}: {last_err}")


@pytest.fixture(scope="session")
def mock_backend():
    """Start the mock_vllm.py backend; yield its base URL; stop it on teardown."""
    port = _free_port()
    proc = subprocess.Popen(
        ["python3", os.path.join(THUNDER_DIR, "mock_vllm.py"), "--port", str(port)],
        cwd=THUNDER_DIR,
    )
    try:
        _wait_http(f"http://127.0.0.1:{port}/health", timeout=10)
        yield f"http://127.0.0.1:{port}"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture(scope="session")
def smg_router(mock_backend):
    """Start SMG with cache_aware policy and one worker pointing at the mock.

    Phase 0 uses cache_aware (the default-friendly policy); ThunderPolicy
    arrives in Phase 3 and replaces this fixture's --policy arg.
    """
    port = _free_port()
    binary = os.path.join(REPO_ROOT, "target", "debug", "smg")
    if not os.path.exists(binary):
        # Fall back to release if a debug build hasn't been done yet.
        binary = os.path.join(REPO_ROOT, "target", "release", "smg")
    if not os.path.exists(binary):
        pytest.skip(
            f"smg binary not found at target/{{debug,release}}/smg; "
            f"run `cargo build -p model_gateway` from {REPO_ROOT} first"
        )
    cmd = [
        binary, "start",
        "--host", "127.0.0.1",
        "--port", str(port),
        "--worker-urls", mock_backend,
        "--policy", "cache_aware",
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
EOF
```

(The `--policy cache_aware` choice is intentional: ThunderPolicy doesn't exist yet, and we want to prove the protocol seam works under any default-shaped policy. P3 will swap this to `--policy thunder`.)

- [ ] **Step 6.7: Verify Python syntax**

```bash
cd /home/hkang/wl/smg-wl
python3 -m py_compile e2e_test/thunder/mock_vllm.py e2e_test/thunder/conftest.py
echo $?
```

Expected: `0`.

- [ ] **Step 6.8: Commit**

```bash
cd /home/hkang/wl/smg-wl
git add e2e_test/thunder/__init__.py e2e_test/thunder/mock_vllm.py e2e_test/thunder/conftest.py
git commit -m "test(thunder): lift mock backend + add /v1/messages handler (Phase 0)

mock_vllm.py is lifted from the abandoned /home/hkang/wl/smg_thunder
worktree (zero-dep stdlib HTTP server) and extended with a
/v1/messages handler that returns an OpenAI-shape chat.completion
body. The body includes a non-spec _mock_echo_program_id field so the
Phase 0 e2e test can assert metadata.program_id traversed the gateway
intact.

conftest.py spawns mock_backend + smg_router (cache_aware policy) for
session-scoped reuse. ThunderPolicy arrives in Phase 3.

Refs: docs/thunder/10-phases.md P0 row, worklog D-12 (mock retention)"
```

---

## Task 7: e2e test — Anthropic Messages → SMG → Mock pass-through

**Files:**
- Create: `e2e_test/thunder/test_phase0_messages_passthrough.py`

`★ Insight ─────────────────────────────────────`
- The test is intentionally minimal — it asserts (a) HTTP 200, (b) response body byte-stream forwarded (we look for the canned content), and (c) `metadata.program_id` reached the backend (via the mock's `_mock_echo_program_id` echo). It does NOT test pause/resume, capacity, or thunder-specific behavior — those arrive in P5/P6 e2e.
- We deliberately do NOT exercise streaming on `/v1/messages` in P0. The streaming SSE shapes for Anthropic vs OpenAI differ (`event: message_start` vs bare `data: {...}`), and the mock returns OpenAI-shape SSE. Asserting the streaming behavior cross-protocol is a P3 concern.
`─────────────────────────────────────────────────`

- [ ] **Step 7.1: Write the test**

```bash
cat > /home/hkang/wl/smg-wl/e2e_test/thunder/test_phase0_messages_passthrough.py <<'EOF'
"""Phase 0 e2e: SMG forwards POST /v1/messages to a backend.

After Phase 0:
- crates/protocols implements GenerationRequest for CreateMessageRequest
- model_gateway/src/routers/http/router.rs::Router::route_messages exists
- /v1/messages route_to_endpoint label = "messages"

The test does NOT use ThunderPolicy (which doesn't exist until Phase 3);
it uses cache_aware and asserts the protocol seam works for arbitrary
GenerationRequest impls.
"""
from __future__ import annotations

import requests


def test_messages_non_streaming_passthrough(smg_router):
    """POST /v1/messages with a CreateMessageRequest body returns 200 +
    backend-shaped body + program_id reached the backend."""
    req_body = {
        "model": "Qwen/Qwen3-0.6B",
        "max_tokens": 32,
        "messages": [
            {"role": "user", "content": "hello, gateway!"},
        ],
        "metadata": {
            "program_id": "phase0-test-1",
            "user_id": "alice",
        },
    }
    r = requests.post(
        f"{smg_router}/v1/messages",
        json=req_body,
        headers={"content-type": "application/json"},
        timeout=10,
    )
    assert r.status_code == 200, f"expected 200, got {r.status_code}: {r.text}"
    body = r.json()
    # Mock returns OpenAI-shape; gateway forwards bytes-as-is.
    assert body["object"] == "chat.completion"
    assert "choices" in body and body["choices"]
    # Backend received metadata.program_id and echoed it back.
    assert body.get("_mock_echo_program_id") == "phase0-test-1", \
        f"program_id was lost in transit; body={body}"


def test_messages_metadata_program_id_optional(smg_router):
    """Requests without metadata.program_id still succeed; backend gets None."""
    req_body = {
        "model": "Qwen/Qwen3-0.6B",
        "max_tokens": 16,
        "messages": [{"role": "user", "content": "hi"}],
    }
    r = requests.post(f"{smg_router}/v1/messages", json=req_body, timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert body["object"] == "chat.completion"
    assert body.get("_mock_echo_program_id") is None


def test_messages_blocks_content_routes_correctly(smg_router):
    """Block-form content (Anthropic native) is forwarded; backend gets the body."""
    req_body = {
        "model": "Qwen/Qwen3-0.6B",
        "max_tokens": 16,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "block-form text"},
            ],
        }],
        "metadata": {"program_id": "phase0-blocks"},
    }
    r = requests.post(f"{smg_router}/v1/messages", json=req_body, timeout=10)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("_mock_echo_program_id") == "phase0-blocks"
EOF
```

- [ ] **Step 7.2: Build the SMG binary if not present**

```bash
cd /home/hkang/wl/smg-wl
test -x target/debug/smg || cargo build -p model_gateway 2>&1 | tail -5
```

If you are running from the SLURM login node and `cargo build` is fast there, this is fine. If you need to run it on a compute node (per `docs/thunder/slurm-cluster.md`):

```bash
srun --jobid=30385 --overlap --gpus=0 bash -c \
    'cd /home/hkang/wl/smg-wl && cargo build -p model_gateway' 2>&1 | tail -5
```

Verify the SLURM jobid 30385 is still active first via `squeue -u hkang`. If it has rotated, ask the user for the current jobid before retrying.

- [ ] **Step 7.3: Run the e2e tests**

```bash
cd /home/hkang/wl/smg-wl
pytest e2e_test/thunder/test_phase0_messages_passthrough.py -v 2>&1 | tail -30
```

Expected: 3 tests pass.

If `cache_aware` policy is rejected by `--policy` whitelist, check `model_gateway/src/main.rs` for the `value_parser` list — `cache_aware` should be present. If not, fall back to `--policy round_robin` in `conftest.py:smg_router` and re-run.

If SMG fails to start with "no workers available" or similar, check that `--worker-urls` accepts a single URL. The expected CLI shape is `--worker-urls http://127.0.0.1:PORT`; if it requires comma-separated even for one URL, conftest is fine; if it requires repeated `--worker-urls` per URL, adapt conftest.

- [ ] **Step 7.4: Commit**

```bash
cd /home/hkang/wl/smg-wl
git add e2e_test/thunder/test_phase0_messages_passthrough.py
git commit -m "test(thunder): e2e Phase 0 — /v1/messages pass-through (Phase 0)

Three test cases exercise the new HTTP regular Router::route_messages
impl: (1) string-content message + metadata.program_id roundtrip;
(2) request without metadata still succeeds; (3) Anthropic block-form
content is forwarded.

The tests use --policy cache_aware (ThunderPolicy doesn't exist yet);
they prove the protocol seam works for arbitrary GenerationRequest
impls. ThunderPolicy-specific e2e arrives in P5/P6.

Refs: docs/thunder/10-phases.md P0 row, docs/thunder/08-testing.md"
```

---

## Task 8: Phase exit verification

**Files:** none modified — this task runs the phase contract from `docs/thunder/10-phases.md` §12.

`★ Insight ─────────────────────────────────────`
- The phase contract requires `cargo build/test/clippy` + e2e + spec citation in commit message + no algorithmic deviation from Python that isn't signed off in §2. We've already cited spec sections in Tasks 1-7 commits and made no algorithmic changes (P0 is pure protocol plumbing). The remaining check is the workspace-wide test run.
- Running `bash scripts/check_thunder_xref.sh` is not part of the P0 phase contract per se, but it's a cheap sanity check that the spec docs we referenced still resolve. Run it; warn if it errors.
`─────────────────────────────────────────────────`

- [ ] **Step 8.1: Run full workspace tests**

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -5
cargo test --workspace 2>&1 | tail -20
cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -20
```

Expected: all green. Most likely failure modes:
- `unused_imports` clippy warning if Task 4's `use crate::common::GenerationRequest` is consumed only by the new impl (it is, so warning shouldn't trigger).
- A test elsewhere doing struct-literal `Metadata { user_id }` failing — Task 2.6 should have caught this; if it slipped through, fix here and amend Task 2's commit OR add a new cleanup commit.

- [ ] **Step 8.2: Run the e2e test suite**

```bash
cd /home/hkang/wl/smg-wl
pytest e2e_test/thunder/ -v 2>&1 | tail -10
```

Expected: 3 passes (the 3 added in Task 7).

If you need to run on a compute node:

```bash
srun --jobid=30385 --overlap --gpus=0 bash -c \
    'cd /home/hkang/wl/smg-wl && pytest e2e_test/thunder/ -v' 2>&1 | tail -10
```

- [ ] **Step 8.3: Run xref sanity check on spec docs**

```bash
cd /home/hkang/wl/smg-wl
bash scripts/check_thunder_xref.sh
```

Expected: `[OK] thunder cross-references look clean (...)`. If [WARN] lines appear about `§X.Y` references or legacy paths, those are accepted compromises (per worklog D-15). [ERROR] lines indicate broken markdown links — investigate and fix before final commit.

- [ ] **Step 8.4: Verify the commit log structure**

```bash
cd /home/hkang/wl/smg-wl
git log --oneline thunder-policy ^04f9b2d6 | head -10
```

Expected: 7 new commits on top of `fee7a129`, all prefixed `feat(...)` or `test(thunder):` and ending with `(Phase 0)`. Sample:

```
xxxxxxxx test(thunder): e2e Phase 0 — /v1/messages pass-through (Phase 0)
xxxxxxxx test(thunder): lift mock backend + add /v1/messages handler (Phase 0)
xxxxxxxx feat(router): impl Router::route_messages pass-through (Phase 0)
xxxxxxxx feat(protocols): impl GenerationRequest for CreateMessageRequest (Phase 0)
xxxxxxxx feat(metrics): wire /v1/messages route_to_endpoint label (Phase 0)
xxxxxxxx feat(protocols): add Metadata.program_id for program-aware routing (Phase 0)
xxxxxxxx feat(protocols): add program_id_hint to GenerationRequest trait (Phase 0)
fee7a129 docs(thunder): integrate ThunderAgent design spec as policy hierarchy
```

If the user prefers one squashed commit per phase (per `10-phases.md` §11 "Each phase = one commit on `thunder-policy` branch"), squash interactively after Step 8.4:

```bash
cd /home/hkang/wl/smg-wl
# Confirm with user before running. Optional — atomic-task commits are
# also acceptable for review and bisect.
git rebase -i HEAD~7
# In the editor: pick the first commit, squash the remaining 6.
# Final commit message should be "feat(thunder): Phase 0 — HTTP regular
# /v1/messages pass-through" with a body describing the 7 tasks.
```

(Squashing is **optional** and risky if not coordinated — recommend leaving as 7 commits and letting the user squash later if they prefer one-commit-per-phase. Atomic commits are easier to bisect and review.)

- [ ] **Step 8.5: Update worklog with a P0-completion entry**

Append to `/home/hkang/wl/smg-wl/docs/thunder/worklog.md`:

```markdown
---

## D-16: P0 implementation completed — /v1/messages pass-through landed

**Date**: 2026-04-30 (or actual date P0 lands)
**Spec ref**: `docs/thunder/10-phases.md` P0 row, `docs/thunder/04-smg-integration.md` §5.5b/c/d/e

### What landed

- `GenerationRequest::program_id_hint` (default-None) on the trait at `crates/protocols/src/common.rs:40`
- `Metadata.program_id: Option<String>` at `crates/protocols/src/messages.rs:178`
- `impl GenerationRequest for CreateMessageRequest` (4 methods, ~55 LOC) at `crates/protocols/src/messages.rs`
- `"/v1/messages" => ENDPOINT_MESSAGES` arm at `model_gateway/src/routers/grpc/utils/metrics.rs:8`
- `Router::route_messages` pass-through at `model_gateway/src/routers/http/router.rs`
- e2e: `e2e_test/thunder/{__init__.py,conftest.py,mock_vllm.py,test_phase0_messages_passthrough.py}` — 3 tests pass

### What did NOT change

- No policy code touched (thunder.rs doesn't exist yet)
- No CLI changes (`--policy thunder` still rejected at clap parse)
- No anthropic router changes (3rd-party path out of scope)
- No PD changes
- No gRPC changes (gRPC validation in P7)

### Revisit conditions

1. If P3 reveals that `extract_text_for_routing` for CreateMessageRequest needs to include ToolResultBlock content (e.g. for cache-aware routing of tool-heavy programs), expand the impl — this is non-breaking.
2. If litellm-proxy is later observed to pass through `metadata.program_id` (current spec §10.5 footgun says it strips), revisit whether the gateway should forward `program_id` as well so backends can use it for KV-cache stickiness hints.

### Approved by

(Pending P0 implementation commit + user review.)
```

(Save the file but **do not auto-commit** the worklog update — the user reviews P0 implementation first, then signs off the worklog entry as a separate commit. This preserves the discipline that worklog approvals are explicit.)

- [ ] **Step 8.6: Notify the user**

Final hand-off message to the user (template):

> Phase 0 is complete. 7 commits on `thunder-policy` (or 1 if squashed):
> - 5 Rust changes (~80 LOC net): trait method, struct field, match arm, trait impl, router impl
> - 2 Python additions: mock extension, e2e test (3 cases pass)
>
> Workspace `cargo build/test/clippy` green; `pytest e2e_test/thunder/` 3/3 pass.
>
> Worklog has a draft D-16 entry awaiting your sign-off (separate commit).
>
> Ready for P1 plan when you are. P1 = trait extension (`select_worker_async` + `usage_sender` on `LoadBalancingPolicy`) + `SelectWorkerInfo.program_id` field + 2 sync→async migrations. Estimated ~150 LOC, no behavior change for existing policies.

---

## Phase exit criteria (summary)

| Check | Command | Required |
|---|---|---|
| Workspace builds | `cargo build --workspace` | ✅ |
| Workspace tests pass | `cargo test --workspace` | ✅ |
| Clippy clean | `cargo clippy --all-targets --all-features -- -D warnings` | ✅ |
| e2e green | `pytest e2e_test/thunder/ -v` | ✅ |
| Spec xref clean | `bash scripts/check_thunder_xref.sh` | preferred (not enforced) |
| Commits cite spec | `git log --grep="docs/thunder"` | ✅ |
| No algorithmic deviation | manual review vs `02-decisions.md` table | ✅ (none expected for P0) |

---

## Rollback

P0 is fully reversible up to commit `fee7a129`:

```bash
cd /home/hkang/wl/smg-wl
git reset --hard fee7a129  # destructive — confirm with user before running
rm -rf e2e_test/thunder
```

(Confirm with user before any `git reset --hard`. The 7 atomic commits make per-task rollback also possible — `git reset --hard HEAD~1` undoes only the most recent task.)

---

## Self-review notes

- **Spec coverage**: every sub-bullet of `docs/thunder/10-phases.md` P0 row is addressed by a task — (a) program_id_hint = Task 1, (b) impl GenerationRequest = Task 4, (c) Metadata.program_id = Task 2, (d) route_messages pass-through = Task 5. Plus the D-13 additions: extract_text_for_routing impl = Task 4 step 4.4, route_to_endpoint arm = Task 3, sidecar mount-path invariant = mentioned but not a code change (deployment runbook in P9).
- **Type consistency**: `Option<String>` on `Metadata.program_id` ↔ `Option<&str>` on `program_id_hint()` — `as_ref()?.program_id.as_deref()` bridges the two. `is_stream` returns `bool` (matches trait); `get_model` returns `Option<&str>` (matches trait, even though inherent method on CreateMessageRequest returns `&str`).
- **Placeholder scan**: every "Step 1.X" / "Step 2.X" / etc. in this plan contains either a concrete command, full code, or a specific assertion. No "TODO" / "TBD" / "similar to" placeholders.
- **Dependency order**: Tasks 1+2 → Task 4; Task 4 → Task 5; Task 3 is independent and ordered for cognitive load. Task 6 → Task 7. Task 8 runs after all others.
