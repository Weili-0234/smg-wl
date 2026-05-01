# Thunder P1 — `LoadBalancingPolicy` Trait Extension + `program_id` Plumbing Plan

> **For agentic workers:** Codex CLI (gpt-5.5 high) executes this plan via `codex exec`. Steps use checkbox (`- [ ]`) syntax for tracking. Claude reviews the diff afterward against the R1-R12 checklist in `docs/thunder/workflow.md`. Do NOT invoke superpowers skills (Claude-only); just follow the steps.

**Goal:** Extend `LoadBalancingPolicy` trait with two additive primitives — async `select_worker_async` (default delegates to sync) and `usage_sender` (default None) — plus add `program_id: Option<&'a str>` field to `SelectWorkerInfo`, then async-migrate the two non-PD call sites (HTTP regular + gRPC regular) so they call `select_worker_async(...).await` and pass `info.program_id = req.program_id_hint()`. After P1, every existing policy still works (no behavior change), but the seam for ThunderPolicy (P3) is in place.

**Architecture:** Three protocol-level changes (trait, struct, helper module) + two router-level migrations + per-policy parity test sweep. ThunderPolicy itself does NOT exist yet (lands in P3); P1 only opens the door. The async migration is "structural-only": no policy actually overrides `select_worker_async` in this phase, so the runtime behavior of cache_aware / round_robin / random / power_of_two / consistent_hashing / prefix_hash / bucket / dp_min_token / manual is identical before and after this phase.

**Tech Stack:** Rust 1.x workspace; `async_trait` crate for object-safe async-fn-in-trait (matches project convention at `routers/grpc/common/stages/worker_selection.rs:5`); `tokio::sync::mpsc::UnboundedSender` for `UsageEvent` channel; strict clippy `-D warnings` with `unwrap_used` denied.

---

## Context (READ FIRST)

This plan is the executable form of:

- **`docs/thunder/10-phases.md` row P1** — phase contract: 5 sub-bullets (a)-(e), risk = "every policy file recompiles", test = `cargo test --workspace` green + per-policy default fallback parity.
- **`docs/thunder/04-smg-integration.md` §5.5b/§5.7** — the `SelectWorkerInfo.program_id` extension and `usage_sender` hook design.
- **`docs/thunder/worklog.md` D-2** — the `usage_sender` design: optional trait method, default returns None, ThunderPolicy returns `Some(&self.usage_tx)`. Stateless policies stay None.
- **`docs/thunder/worklog.md` D-13** — pre-flight verification of `route_typed_request` protocol-agnostic: this gives confidence that adding `program_id` to `SelectWorkerInfo` and calling `req.program_id_hint()` doesn't disturb non-thunder policies.

**Out of scope** (do NOT touch in P1):

- `model_gateway/src/policies/thunder.rs` — does not exist; lands in P3.
- `model_gateway/src/routers/http/pd_router.rs` and `model_gateway/src/routers/grpc/pd_router.rs` — PD path. The PD call site `routers/grpc/common/stages/worker_selection.rs:179 select_pd_pair` (which calls `policy.select_worker` at lines 276+277) **stays sync in this phase** — PD support is deferred per worklog D-7.
- `model_gateway/src/routers/anthropic/router.rs`, `routers/openai/`, `routers/gemini/` — 3rd-party paths use `WorkerSelector`, not `LoadBalancingPolicy`.
- All `--policy thunder` CLI work — that's P3.

**Key file:line anchors** (verified 2026-04-30 on commit `e8c75e1f`):

| Anchor | What is there | What we change |
|---|---|---|
| `model_gateway/src/policies/mod.rs:44` | `pub trait LoadBalancingPolicy: Send + Sync + Debug` (sync-only methods) | Add `#[async_trait]` macro + `async fn select_worker_async` default + `fn usage_sender` default-None |
| `model_gateway/src/policies/mod.rs:165-181` | `pub struct SelectWorkerInfo<'a>` (4 fields) | Add `pub program_id: Option<&'a str>` field |
| `model_gateway/src/policies/mod.rs` (top of file) | `pub use ...` re-exports + module decls | Add `pub use UsageEvent` re-export; declare `UsageEvent` struct |
| `model_gateway/src/routers/common/` | existing module dir with `header_utils.rs`, `retry.rs` | Create `program_id.rs` (~30 LOC helper) |
| `model_gateway/src/routers/http/router.rs:141` | `fn select_worker_for_model(&self, model_id, text, headers)` | Async-migrate to `async fn`, add `program_id: Option<&str>` param, call `policy.select_worker_async(...).await` |
| `model_gateway/src/routers/http/router.rs:197` | `pub async fn route_typed_request<T>` | Already async; populate `program_id = typed_req.program_id_hint()` and pass to `select_worker_for_model` |
| `model_gateway/src/routers/grpc/common/stages/worker_selection.rs:119` | `fn select_single_worker(&self, model_id, text, tokens, headers)` | Async-migrate to `async fn`, add `program_id: Option<&str>` param, call `policy.select_worker_async(...).await` |
| `model_gateway/src/routers/grpc/common/stages/worker_selection.rs:51` | `impl PipelineStage for WorkerSelectionStage::execute` | Update Regular branch to populate program_id from `ctx.input.body` (or equivalent — see Task 5) and `.await` the call |
| `model_gateway/src/policies/{cache_aware,round_robin,random,power_of_two,consistent_hashing,prefix_hash,bucket,dp_min_token,manual}.rs` | 9 existing policies | NOT modified; they inherit the default `select_worker_async` and `usage_sender` impls |

---

## Pre-flight verification

- [ ] **PF.1: Confirm worktree + branch + commit**

```bash
cd /home/hkang/wl/smg-wl
git status --short
git rev-parse --abbrev-ref HEAD
git log --oneline -3
```

Expected:
- Branch is `thunder-policy-p1` (the per-phase sub-branch created by Claude before invoking Codex).
- HEAD is `e8c75e1f` or its descendant.
- `git status --short` is clean (no uncommitted changes).

If you are NOT on `thunder-policy-p1`, **STOP** with `OPEN_QUESTION: not on expected sub-branch, found <X>`.

- [ ] **PF.2: Confirm baseline `cargo build --workspace` is green**

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -5
```

Expected: `Finished ...`. The first build after a branch switch may be slow (~5min cold); subsequent are incremental. If errors appear, **STOP** with `OPEN_QUESTION: baseline build red on P1 branch, errors=<paste 5 lines>`.

- [ ] **PF.3: Verify `async_trait` crate is already a workspace dep**

```bash
cd /home/hkang/wl/smg-wl
grep -nE 'async-trait|async_trait' Cargo.toml model_gateway/Cargo.toml
```

Expected: at least one match (project-wide convention; gRPC stages already use it). If absent, **STOP** with `OPEN_QUESTION: async_trait not in deps, expected to be present`.

- [ ] **PF.4: Read context**

Read these files in full before starting:
- `docs/thunder/10-phases.md` (P1 row, ~5 lines, plus §12 sign-off rules)
- `docs/thunder/04-smg-integration.md` §5.5b and §5.7 (use grep to locate the headers)
- `docs/thunder/worklog.md` D-2, D-9, D-13 entries

These tell you **why** the changes are shaped this way. Do NOT skip — design decisions in the worklog override surface impressions from the plan.

---

## File Structure

After P1, the following files exist or are modified:

```
/home/hkang/wl/smg-wl/                        ← worktree, branch: thunder-policy-p1
├── model_gateway/src/
│   ├── policies/
│   │   └── mod.rs                            ← MODIFIED (Tasks 1, 2, 3: +UsageEvent struct, +program_id field, +async trait methods, #[async_trait] attr)
│   └── routers/
│       ├── common/
│       │   └── program_id.rs                 ← NEW (Task 4: helper to extract program_id from typed_req)
│       ├── http/router.rs                    ← MODIFIED (Task 5: async-migrate select_worker_for_model + thread program_id)
│       └── grpc/common/stages/
│           └── worker_selection.rs           ← MODIFIED (Task 6: async-migrate select_single_worker + thread program_id)
└── (per-policy parity test added to mod.rs tests block — Task 7)
```

Files **NOT** touched: every policy impl file (`bucket.rs`, `cache_aware.rs`, `consistent_hashing.rs`, `dp_min_token.rs`, `manual.rs`, `power_of_two.rs`, `prefix_hash.rs`, `random.rs`, `round_robin.rs`); `routers/anthropic/`; `routers/openai/`; `routers/gemini/`; `routers/http/pd_router.rs`; `routers/grpc/pd_router.rs`; `routers/grpc/common/stages/worker_selection.rs::select_pd_pair`; CLI / config / observability / worker / e2e_test.

---

## Task 1: Add `UsageEvent` struct to `policies/mod.rs`

**Files:**
- Modify: `model_gateway/src/policies/mod.rs` (add struct definition before `LoadBalancingPolicy` trait at line 40)

**Why this is task 1:** The trait method `usage_sender` (Task 3) returns `Option<&UnboundedSender<UsageEvent>>`. The type must exist before the trait references it. Putting it in `policies/mod.rs` (not `policies/thunder.rs`) means the trait reference doesn't force every policy module to import thunder.

- [ ] **Step 1.1: Write the failing test**

Append to the existing `#[cfg(test)] mod tests` block at end of `model_gateway/src/policies/mod.rs` (around line 232):

```rust
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
```

- [ ] **Step 1.2: Run the test to verify it fails**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p smg policies::tests::usage_event_struct_exists_and_is_constructible 2>&1 | tail -10
```

Expected: compile error, e.g. `error[E0422]: cannot find struct UsageEvent in this scope`.

- [ ] **Step 1.3: Add the `UsageEvent` struct**

Insert into `model_gateway/src/policies/mod.rs` BEFORE the `LoadBalancingPolicy` trait (between line 39 and 44 — after the `pub use round_robin::RoundRobinPolicy;` re-export block, before the `/// Core trait` doc comment):

```rust
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
```

- [ ] **Step 1.4: Run the test to verify it passes**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p smg policies::tests::usage_event_struct_exists_and_is_constructible 2>&1 | tail -5
```

Expected: `test ... ok`.

- [ ] **Step 1.5: Verify workspace build**

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -5
```

Expected: green.

- [ ] **Step 1.6: Commit**

```bash
cd /home/hkang/wl/smg-wl
git add model_gateway/src/policies/mod.rs
git commit -m "feat(policies): add UsageEvent struct for usage_sender hook (Phase 1)

UsageEvent lives in policies/mod.rs (not policies/thunder.rs) so the
LoadBalancingPolicy trait method usage_sender (added in a follow-up
commit) can reference it without forcing every policy module to import
thunder. ThunderPolicy in Phase 3 will consume these events via its
usage_tx channel.

Refs: docs/thunder/04-smg-integration.md §5.7, worklog D-2"
```

---

## Task 2: Add `program_id` field to `SelectWorkerInfo`

**Files:**
- Modify: `model_gateway/src/policies/mod.rs:165-181` (struct definition)

**Why this is task 2:** Independent of Task 1 (no UsageEvent reference), but Task 5 (HTTP migration) and Task 6 (gRPC migration) both populate this field — so it must exist before the call sites are updated.

`★ Note for reviewer:` The field is `Option<&'a str>` to match the existing borrow lifetime on `request_text` and `tokens`. Using `Option<String>` would force callers to allocate per-request.

- [ ] **Step 2.1: Write the failing test**

Append to the same `#[cfg(test)] mod tests` block:

```rust
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
```

- [ ] **Step 2.2: Run the tests to verify they fail**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p smg policies::tests::select_worker_info 2>&1 | tail -10
```

Expected: compile error mentioning `struct SelectWorkerInfo has no field named program_id`.

- [ ] **Step 2.3: Add the field**

Edit `model_gateway/src/policies/mod.rs:166-181`. Current struct:

```rust
#[derive(Debug, Clone, Default)]
pub struct SelectWorkerInfo<'a> {
    pub request_text: Option<&'a str>,
    pub tokens: Option<&'a [u32]>,
    pub headers: Option<&'a http::HeaderMap>,
    pub hash_ring: Option<Arc<HashRing>>,
}
```

Add the `program_id` field at the end:

```rust
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
```

(Preserve the existing field doc comments verbatim; only add the new field.)

- [ ] **Step 2.4: Run the tests to verify they pass**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p smg policies::tests::select_worker_info 2>&1 | tail -5
```

Expected: 2 tests `ok`.

- [ ] **Step 2.5: Verify workspace build (struct-literal callers)**

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -10
```

If callers initialize `SelectWorkerInfo { request_text, tokens, headers, hash_ring }` without `..Default::default()`, the build will fail with "missing field program_id". Find and update them:

```bash
grep -rn 'SelectWorkerInfo {' --include='*.rs' /home/hkang/wl/smg-wl/model_gateway/src/
```

Known call sites you will encounter (from manual code-trace, line numbers approximate):
- `routers/http/router.rs:178` — inside `select_worker_for_model`
- `routers/grpc/common/stages/worker_selection.rs:159` — inside `select_single_worker`
- `routers/grpc/common/stages/worker_selection.rs:270` — inside `select_pd_pair` (PD; see Note below)

For all of these, add `program_id: None,` as the last field. Tasks 5 and 6 will replace the `None` at the HTTP and gRPC-regular sites with the actual `req.program_id_hint()` value; the PD site at line 270 keeps `program_id: None` permanently in P1 (PD scope deferred per worklog D-7).

- [ ] **Step 2.6: Re-verify build**

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -5
```

Expected: green.

- [ ] **Step 2.7: Commit**

```bash
cd /home/hkang/wl/smg-wl
git add model_gateway/src/policies/mod.rs model_gateway/src/routers/
git commit -m "feat(policies): add program_id field to SelectWorkerInfo (Phase 1)

Default None means existing policies behave identically. ThunderPolicy
in Phase 3 reads info.program_id; the HTTP and gRPC-regular call sites
(updated in follow-up commits) populate it from
GenerationRequest::program_id_hint(). PD call site retains None for
this phase per worklog D-7 (PD scope deferred).

Refs: docs/thunder/04-smg-integration.md §5.5b, worklog D-13"
```

---

## Task 3: Add `select_worker_async` + `usage_sender` to `LoadBalancingPolicy` trait

**Files:**
- Modify: `model_gateway/src/policies/mod.rs:44-92` (trait definition + add `#[async_trait]` macro + import)

**Why this is task 3:** Tasks 1+2 provide the types this method references (`UsageEvent`, `SelectWorkerInfo` with new field). Tasks 5+6 call this method.

`★ Reviewer note on async_trait:` The project already uses `async_trait` elsewhere (`routers/grpc/common/stages/mod.rs::PipelineStage`). Adding `#[async_trait]` to `LoadBalancingPolicy` is the lowest-friction path: it preserves object safety (`Arc<dyn LoadBalancingPolicy>` keeps working) and existing sync methods are unaffected. Native Rust 1.75+ AFIT (`async fn` directly in a trait) would constrain `dyn` usage and is rejected here.

- [ ] **Step 3.1: Write the failing test**

Append to the same `#[cfg(test)] mod tests` block:

```rust
    #[tokio::test]
    async fn select_worker_async_default_delegates_to_sync() {
        struct Stub;
        impl std::fmt::Debug for Stub {
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
                if workers.is_empty() {
                    None
                } else {
                    Some(0)
                }
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
        impl std::fmt::Debug for Stub {
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
```

- [ ] **Step 3.2: Run the tests to verify they fail**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p smg policies::tests::select_worker_async_default 2>&1 | tail -10
cargo test -p smg policies::tests::usage_sender_default 2>&1 | tail -5
```

Expected: compile errors mentioning `no method named select_worker_async` and `no method named usage_sender`.

- [ ] **Step 3.3: Add `#[async_trait]` macro + the two new trait methods**

Edit `model_gateway/src/policies/mod.rs`:

1. **Add import at top of file** (line ~6, with the other use statements):

```rust
use async_trait::async_trait;
```

(Preserve existing imports; insert this line in alphabetical order with the other crate-level uses, OR add as a new line after `use std::{fmt::Debug, sync::Arc};`.)

2. **Add `#[async_trait]` attribute to the trait** at line 44:

```rust
#[async_trait]
pub trait LoadBalancingPolicy: Send + Sync + Debug {
```

3. **Add the two new methods at the end of the trait body** (between the existing `as_any` method and the closing `}` at line 92):

```rust
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
    fn usage_sender(&self) -> Option<&tokio::sync::mpsc::UnboundedSender<UsageEvent>> {
        None
    }
```

- [ ] **Step 3.4: Run the tests to verify they pass**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p smg policies::tests::select_worker_async_default 2>&1 | tail -5
cargo test -p smg policies::tests::usage_sender_default 2>&1 | tail -5
```

Expected: both tests `ok`.

- [ ] **Step 3.5: Verify workspace build + clippy**

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -5
cargo clippy -p smg --all-targets --all-features -- -D warnings 2>&1 | tail -10
```

Expected: both green. Common failure: forgetting `#[async_trait]` on the test's `impl LoadBalancingPolicy for Stub` — adding the macro on the trait definition propagates to all impls; both Stub impls in the tests above already include `#[async_trait::async_trait]`.

If clippy fires `async_yields_async` or `manual_async_fn` on the default body, the lint is wrong here — `self.select_worker(...)` is sync; the default body is `async { sync_call }`. Acceptable; no override needed.

- [ ] **Step 3.6: Commit**

```bash
cd /home/hkang/wl/smg-wl
git add model_gateway/src/policies/mod.rs
git commit -m "feat(policies): add select_worker_async + usage_sender trait methods (Phase 1)

select_worker_async default-delegates to select_worker; only ThunderPolicy
will override (Phase 3). usage_sender returns None by default; only
ThunderPolicy will return Some(&self.usage_tx).

Adds #[async_trait] attribute on the trait — matches the project's
convention (PipelineStage at routers/grpc/common/stages/mod.rs uses the
same macro). Object safety preserved: Arc<dyn LoadBalancingPolicy>
continues to work.

Refs: docs/thunder/04-smg-integration.md §5.5b/§5.7, worklog D-2"
```

---

## Task 4: Create `routers/common/program_id.rs` helper module

**Files:**
- Create: `model_gateway/src/routers/common/program_id.rs` (~30 LOC)
- Modify: `model_gateway/src/routers/common/mod.rs` (add `pub mod program_id;`)

**Why now:** Tasks 5 and 6 (the call-site migrations) call `program_id::extract(req)`. Centralizing this in one module is cheap (~30 LOC) and gives a future hook point if program_id ever needs sanitization, namespace prefixing, or per-tenant rewriting.

- [ ] **Step 4.1: Write the failing test**

Create the file `model_gateway/src/routers/common/program_id.rs` with **only** the test stub (no impl yet):

```rust
//! Program ID extraction helper for program-aware policies (e.g. ThunderPolicy).
//!
//! This module centralizes the call to `GenerationRequest::program_id_hint`
//! so future hooks (sanitization, tenant prefixing) have a single seam.

#[cfg(test)]
mod tests {
    use super::*;
    use openai_protocol::common::GenerationRequest;

    #[derive(Clone, serde::Serialize)]
    struct Stub<'a> {
        pid: Option<&'a str>,
    }

    impl GenerationRequest for Stub<'_> {
        fn is_stream(&self) -> bool {
            false
        }
        fn get_model(&self) -> Option<&str> {
            None
        }
        fn extract_text_for_routing(&self) -> String {
            String::new()
        }
        fn program_id_hint(&self) -> Option<&str> {
            self.pid
        }
    }

    #[test]
    fn extract_returns_program_id_hint() {
        let req = Stub { pid: Some("agent-1") };
        assert_eq!(extract(&req), Some("agent-1"));
    }

    #[test]
    fn extract_returns_none_when_hint_is_none() {
        let req = Stub { pid: None };
        assert_eq!(extract(&req), None);
    }
}
```

- [ ] **Step 4.2: Add the module declaration**

Open `model_gateway/src/routers/common/mod.rs` and find the existing module declarations (e.g. `pub mod header_utils; pub mod retry;`). Add:

```rust
pub mod program_id;
```

(Place alphabetically among the others.)

- [ ] **Step 4.3: Run the tests to verify they fail**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p smg routers::common::program_id::tests 2>&1 | tail -10
```

Expected: compile error `cannot find function extract in this scope` or `unresolved import super::*`.

- [ ] **Step 4.4: Add the `extract` function**

Edit `model_gateway/src/routers/common/program_id.rs` and **prepend** the function definition before the `#[cfg(test)]` block:

```rust
//! Program ID extraction helper for program-aware policies (e.g. ThunderPolicy).
//!
//! This module centralizes the call to `GenerationRequest::program_id_hint`
//! so future hooks (sanitization, tenant prefixing) have a single seam.

use openai_protocol::common::GenerationRequest;

/// Extract the program identifier hint from a typed generation request.
///
/// Today this is a thin pass-through to `GenerationRequest::program_id_hint`.
/// The indirection exists to give future per-tenant or per-deployment rewrites
/// a single place to land — e.g., if `metadata.program_id` ever needs
/// namespace prefixing for multi-tenant cross-isolation.
///
/// Returns `None` for requests whose protocol does not carry a program_id
/// concept (every type today except `CreateMessageRequest`).
pub fn extract<T: GenerationRequest + ?Sized>(req: &T) -> Option<&str> {
    req.program_id_hint()
}

#[cfg(test)]
mod tests {
    // ... (existing test block stays as-is)
}
```

(The `?Sized` bound lets callers pass `&dyn GenerationRequest` if they ever need to. Today they pass concrete `&T`, but the bound is free.)

- [ ] **Step 4.5: Run the tests to verify they pass**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p smg routers::common::program_id::tests 2>&1 | tail -5
```

Expected: 2 tests `ok`.

- [ ] **Step 4.6: Verify workspace build**

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -5
cargo clippy -p smg --all-targets --all-features -- -D warnings 2>&1 | tail -5
```

Expected: green.

- [ ] **Step 4.7: Commit**

```bash
cd /home/hkang/wl/smg-wl
git add model_gateway/src/routers/common/program_id.rs model_gateway/src/routers/common/mod.rs
git commit -m "feat(routers): add common::program_id::extract helper (Phase 1)

Thin pass-through to GenerationRequest::program_id_hint, centralized for
future per-tenant / per-deployment rewrite hooks. Used by HTTP and gRPC
regular routers in follow-up commits to populate
SelectWorkerInfo.program_id.

Refs: docs/thunder/10-phases.md P1 row (c), worklog D-13"
```

---

## Task 5: Async-migrate HTTP `select_worker_for_model` + thread `program_id`

**Files:**
- Modify: `model_gateway/src/routers/http/router.rs` around lines 138-195 (function signature + body) + line ~197 callers in `route_typed_request` and any other callers

**Why now:** Tasks 1-4 provide all the building blocks. This task is the "structural" part of the spec D-13 finding: HTTP regular path now passes program_id through to the policy.

`★ Reviewer warning:` Async migration in Rust can subtly change `Send` bounds. If a `select_worker_for_model` caller holds a non-`Send` value across the new `.await` point, the compiler will reject. The existing call site is inside `pub async fn route_typed_request<T>` which is already async, so the migration is straightforward — but watch for caller code that uses `&policy` references over `.await` boundaries.

- [ ] **Step 5.1: Migrate the function signature**

Open `model_gateway/src/routers/http/router.rs` around line 141. Current signature:

```rust
fn select_worker_for_model(
    &self,
    model_id: &str,
    text: Option<&str>,
    headers: Option<&HeaderMap>,
) -> Option<Arc<dyn Worker>> {
```

Change to:

```rust
async fn select_worker_for_model(
    &self,
    model_id: &str,
    text: Option<&str>,
    headers: Option<&HeaderMap>,
    program_id: Option<&str>,
) -> Option<Arc<dyn Worker>> {
```

(`async fn` keyword + new `program_id` parameter.)

- [ ] **Step 5.2: Update the function body to use `select_worker_async` + populate `program_id`**

Inside the same function, find the call to `policy.select_worker(&available, &SelectWorkerInfo { ... })?` (around line 176). Replace with:

```rust
    let idx = policy
        .select_worker_async(
            &available,
            &SelectWorkerInfo {
                request_text: text,
                tokens: None, // HTTP doesn't have tokens, use gRPC for PrefixHash
                headers,
                hash_ring,
                program_id,
            },
        )
        .await?;
```

(Two changes: `select_worker_async` instead of `select_worker`, plus `.await?` after the call, plus the new `program_id` field.)

- [ ] **Step 5.3: Update the caller in `route_typed_request`**

Find `route_typed_request` at line ~197. It currently calls (search for `select_worker_for_model`):

```rust
let worker = self
    .select_worker_for_model(model_id, Some(&text), headers)
    .await;
```

Wait — re-verify the exact current call. Use `grep -n 'select_worker_for_model' model_gateway/src/routers/http/router.rs` to find ALL callers in the file. Each must be updated to:

1. Add `.await` if not already present (was sync before, now async)
2. Pass the new `program_id` argument as `crate::routers::common::program_id::extract(typed_req)`

Update **every** caller. Common locations (use grep to confirm exact line numbers):
- `route_typed_request_once` at ~line 281 (called inside the retry loop) — pass `crate::routers::common::program_id::extract(typed_req)` since `typed_req` is in scope.
- Possibly other helpers like `route_post_empty_request` or `cancel_response` — check whether they call `select_worker_for_model`. If they do but don't have a typed_req, pass `None` as program_id.

**Add this import to the top of router.rs** (in the `crate::` use block at line 30+):

```rust
use crate::routers::common::program_id as common_program_id;
```

Then call sites become `common_program_id::extract(typed_req)`.

- [ ] **Step 5.4: Build to discover all callers**

```bash
cd /home/hkang/wl/smg-wl
cargo build -p smg 2>&1 | tail -30
```

The compiler will report every caller that didn't get updated. Fix each by following the same pattern:
- Sync → async: add `.await` to the call site
- Missing `program_id` arg: pass `common_program_id::extract(typed_req)` if a typed_req is in scope, else `None`

If a sync caller is in a non-async context (e.g., a sync helper function called from multiple async sites), you have two options:
- (a) Make the helper async too — propagate up the chain
- (b) Refactor the caller to compute the worker selection ahead of time

For P1, option (a) is preferred (matches the spec's "2 async propagation chains touched" note). Document any unexpected propagation with `OPEN_QUESTION` in the round summary.

- [ ] **Step 5.5: Verify compilation + run e2e**

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -5
cargo test --workspace 2>&1 | tail -20
cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -10
source e2e_test/.venv/bin/activate
pytest e2e_test/thunder/test_phase0_messages_passthrough.py -v \
    --rootdir=e2e_test/thunder --confcutdir=e2e_test/thunder 2>&1 | tail -10
```

Expected:
- workspace build green
- workspace tests green (the pre-existing `test_large_batch_bootstrap_injection` PD perf flake from P0 is allowed to fail; if a NEW test fails, investigate)
- clippy green
- Phase 0 e2e still 3/3 pass — proves the async migration didn't regress the protocol pass-through

- [ ] **Step 5.6: Commit**

```bash
cd /home/hkang/wl/smg-wl
git add model_gateway/src/routers/http/router.rs
git commit -m "refactor(router): async-migrate HTTP select_worker_for_model + thread program_id (Phase 1)

select_worker_for_model now takes program_id: Option<&str>, populates
SelectWorkerInfo.program_id, and calls policy.select_worker_async(...).await.
Behaviorally identical for non-thunder policies — they inherit the default
async impl that delegates to select_worker. ThunderPolicy in Phase 3 will
override and use the program_id for capacity tracking.

route_typed_request and any internal callers updated to pass the program_id
extracted via routers::common::program_id::extract(typed_req).

Refs: docs/thunder/04-smg-integration.md §5.5b, worklog D-13"
```

---

## Task 6: Async-migrate gRPC regular `select_single_worker` + thread `program_id`

**Files:**
- Modify: `model_gateway/src/routers/grpc/common/stages/worker_selection.rs:51, 119` (PipelineStage execute + select_single_worker)

**Why now:** Symmetric to Task 5 but on the gRPC path. The PipelineStage's `execute` is already async; the migration here is making `select_single_worker` async and threading program_id from `RequestContext`.

`★ Reviewer warning:` `select_pd_pair` (lines 179-302) ALSO calls `policy.select_worker` at lines 276+277. **Do NOT migrate that one.** PD scope is deferred per worklog D-7. The PD path will continue to call sync `select_worker` (the trait still has it; nothing forces removal). Verify your final diff shows ONLY `select_single_worker` modified, not `select_pd_pair`.

- [ ] **Step 6.1: Migrate `select_single_worker` signature + body**

Open `model_gateway/src/routers/grpc/common/stages/worker_selection.rs` around line 119.

Current signature:

```rust
fn select_single_worker(
    &self,
    model_id: &str,
    text: Option<&str>,
    tokens: Option<&[u32]>,
    headers: Option<&http::HeaderMap>,
) -> Option<Arc<dyn Worker>> {
```

Change to:

```rust
async fn select_single_worker(
    &self,
    model_id: &str,
    text: Option<&str>,
    tokens: Option<&[u32]>,
    headers: Option<&http::HeaderMap>,
    program_id: Option<&str>,
) -> Option<Arc<dyn Worker>> {
```

In the body, find the call at line 157:

```rust
let idx = policy.select_worker(
    &available,
    &SelectWorkerInfo {
        request_text: text,
        tokens,
        headers,
        hash_ring,
    },
)?;
```

Change to:

```rust
let idx = policy
    .select_worker_async(
        &available,
        &SelectWorkerInfo {
            request_text: text,
            tokens,
            headers,
            hash_ring,
            program_id,
        },
    )
    .await?;
```

(Same three changes as Task 5: method name, `.await?`, new field.)

- [ ] **Step 6.2: Update the `execute` caller to extract program_id from `RequestContext`**

Find the `WorkerSelectionMode::Regular` arm in `execute` (around line 75):

```rust
WorkerSelectionMode::Regular => {
    match self.select_single_worker(model_id, text, tokens, headers) {
        Some(w) => WorkerSelection::Single { worker: w },
        ...
```

`RequestContext` (in `routers/grpc/context/`) carries `ctx.input.body` or similar typed request. To extract `program_id`, you need to know the request type at this stage.

**STOP and check:** what is `ctx.input.body`'s type at this stage? Is it `&dyn GenerationRequest`? `serde_json::Value`? An enum? Read `routers/grpc/context/mod.rs` (or the file defining `RequestContext`) to determine.

Read these to find out:

```bash
grep -nE 'pub struct RequestContext|input.body|InputContext' model_gateway/src/routers/grpc/context/*.rs 2>/dev/null | head -20
grep -nE 'fn routing_text|program_id_hint' model_gateway/src/routers/grpc/context/*.rs 2>/dev/null | head -10
```

You'll likely find one of these patterns:
- (P-A) `ctx.input.body` is a typed enum like `RequestBody::Chat(ChatCompletionRequest)` / `RequestBody::Messages(CreateMessageRequest)`. Match on it and call `.program_id_hint()` on the inner.
- (P-B) `ctx.input` already has a `program_id` field populated by an earlier preparation stage. Look for it.
- (P-C) The body is a generic `T: GenerationRequest`-bounded value held opaquely; there's a method like `ctx.input.program_id_hint()` or `prep.program_id()` already.

**If P-A (enum match)**: write the match in the Regular arm:

```rust
WorkerSelectionMode::Regular => {
    let program_id = ctx.input.body.program_id_hint(); // or whatever the accessor is
    match self
        .select_single_worker(model_id, text, tokens, headers, program_id)
        .await
    {
        ...
```

**If P-B (pre-extracted)**: just reference the field:

```rust
let program_id = ctx.input.program_id.as_deref();
```

**If P-C (generic accessor exists)**: use it:

```rust
let program_id = prep.program_id();
```

**If none of these patterns fit cleanly**: write `OPEN_QUESTION: gRPC RequestContext does not expose program_id_hint cleanly. Pattern observed: <describe>. Recommend either (i) extending RequestPreparationStage to extract program_id alongside routing_text, or (ii) plumbing the typed body through to WorkerSelectionStage. Which?` and stop. Claude review will resolve.

- [ ] **Step 6.3: Verify the PD path is NOT modified**

Critical sanity check before committing:

```bash
cd /home/hkang/wl/smg-wl
git diff model_gateway/src/routers/grpc/common/stages/worker_selection.rs | grep -E '^[-+]' | head -50
```

Confirm:
- `select_single_worker` is in the diff (signature + body changes)
- `select_pd_pair` is **NOT** in the diff (apart from a possible `program_id: None,` field addition from Task 2, which is fine)
- `policy.select_worker(` calls at PD lines 276-277 still call SYNC `select_worker` (no `.await`, no `_async`)

If `select_pd_pair` was modified, `git checkout -p` to revert just that function and re-verify.

- [ ] **Step 6.4: Build + test**

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -10
cargo test --workspace 2>&1 | tail -20
cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -10
```

Expected: all green. Common fail: a pipeline stage further upstream propagates `Send` bound issues — fix by making the pipeline contracts tolerate `Send` futures (the pipeline already uses `async_trait::async_trait` so this should not be a problem).

- [ ] **Step 6.5: Commit**

```bash
cd /home/hkang/wl/smg-wl
git add model_gateway/src/routers/grpc/common/stages/worker_selection.rs
git commit -m "refactor(grpc): async-migrate select_single_worker + thread program_id (Phase 1)

select_single_worker is now async and takes program_id: Option<&str>.
Calls policy.select_worker_async(...).await with SelectWorkerInfo.program_id
populated. PD path (select_pd_pair) deliberately untouched — PD scope
deferred per worklog D-7.

Refs: docs/thunder/04-smg-integration.md §5.5b, worklog D-7/D-13"
```

---

## Task 7: Per-policy default-fallback parity tests (9 policies)

**Files:**
- Modify: `model_gateway/src/policies/mod.rs` (extend `#[cfg(test)] mod tests`)

**Why this is task 7:** Spec P1(e) requires "verify all existing policies inherit no-op via unit test asserting `select_worker_async == select_worker`". This is the safety net that proves the async migration didn't introduce divergence.

The 9 policies (verified at HEAD `e8c75e1f`): bucket, cache_aware, consistent_hashing, dp_min_token, manual, power_of_two, prefix_hash, random, round_robin.

`★ Reviewer note:` Each policy's `new()` constructor takes different args. To keep tests shallow, write one parametrized test per policy that constructs an empty/default-config instance, sets up 3 mock workers, calls both `select_worker(...)` and `select_worker_async(...).await`, and asserts equal results. If a policy is RNG-based (Random), seed the call deterministically — `RandomPolicy::select_worker` uses `thread_rng()`; for parity it's enough to assert "both return Some(idx) and the idx is in 0..workers.len()" rather than exact equality.

- [ ] **Step 7.1: Inspect each policy's constructor**

```bash
cd /home/hkang/wl/smg-wl
for f in bucket cache_aware consistent_hashing dp_min_token manual power_of_two prefix_hash random round_robin; do
    echo "=== $f ==="
    grep -nE "pub fn new|impl ${f^}Policy" model_gateway/src/policies/$f.rs | head -3
done
```

Note the constructor signatures. Most are `Policy::new()` with no args; a few (CacheAwarePolicy, BucketPolicy, ManualPolicy, PrefixHashPolicy) take a config struct.

- [ ] **Step 7.2: Write the parity tests**

Append to the `#[cfg(test)] mod tests` block in `policies/mod.rs`:

```rust
    /// Helper: build N healthy mock workers.
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

    /// Helper: assert sync and async selection give compatible results.
    /// "Compatible" means either both return None, or both return Some(idx)
    /// where idx is a valid index into workers. We don't require exact equality
    /// because RNG-based policies (Random) are deterministic per call but the
    /// two calls happen at slightly different points and may sample differently.
    async fn assert_parity(
        policy: &dyn LoadBalancingPolicy,
        workers: &[Arc<dyn Worker>],
        info: &SelectWorkerInfo<'_>,
    ) {
        let sync = policy.select_worker(workers, info);
        let asy = policy.select_worker_async(workers, info).await;
        match (sync, asy) {
            (None, None) => {}
            (Some(a), Some(b)) => {
                assert!(a < workers.len(), "sync idx out of range: {a}");
                assert!(b < workers.len(), "async idx out of range: {b}");
            }
            (s, a) => panic!(
                "policy {} parity violated: sync={:?} async={:?}",
                policy.name(),
                s,
                a
            ),
        }
    }

    #[tokio::test]
    async fn round_robin_parity() {
        let policy = RoundRobinPolicy::new();
        let workers = mock_workers(3);
        let info = SelectWorkerInfo::default();
        assert_parity(&policy, &workers, &info).await;
    }

    #[tokio::test]
    async fn random_parity() {
        let policy = RandomPolicy::new();
        let workers = mock_workers(3);
        let info = SelectWorkerInfo::default();
        assert_parity(&policy, &workers, &info).await;
    }

    #[tokio::test]
    async fn power_of_two_parity() {
        let policy = PowerOfTwoPolicy::new();
        let workers = mock_workers(3);
        let info = SelectWorkerInfo::default();
        assert_parity(&policy, &workers, &info).await;
    }

    #[tokio::test]
    async fn consistent_hashing_parity() {
        let policy = ConsistentHashingPolicy::new();
        let workers = mock_workers(3);
        let info = SelectWorkerInfo::default();
        assert_parity(&policy, &workers, &info).await;
    }

    #[tokio::test]
    async fn cache_aware_parity() {
        let policy = CacheAwarePolicy::new(CacheAwareConfig::default());
        let workers = mock_workers(3);
        let info = SelectWorkerInfo {
            request_text: Some("hello world"),
            ..Default::default()
        };
        assert_parity(&policy, &workers, &info).await;
    }

    #[tokio::test]
    async fn bucket_parity() {
        let policy = BucketPolicy::new(BucketConfig::default());
        let workers = mock_workers(3);
        let info = SelectWorkerInfo::default();
        assert_parity(&policy, &workers, &info).await;
    }

    #[tokio::test]
    async fn prefix_hash_parity() {
        let policy = PrefixHashPolicy::new(PrefixHashConfig::default());
        let workers = mock_workers(3);
        let info = SelectWorkerInfo {
            tokens: Some(&[1, 2, 3]),
            ..Default::default()
        };
        assert_parity(&policy, &workers, &info).await;
    }

    #[tokio::test]
    async fn dp_min_token_parity() {
        let policy = MinimumTokensPolicy::new();
        let workers = mock_workers(3);
        let info = SelectWorkerInfo::default();
        assert_parity(&policy, &workers, &info).await;
    }

    #[tokio::test]
    async fn manual_parity() {
        // ManualPolicy needs a routing-key map; use an empty default config
        let policy = ManualPolicy::new(ManualConfig::default());
        let workers = mock_workers(3);
        let info = SelectWorkerInfo::default();
        assert_parity(&policy, &workers, &info).await;
    }
```

(If any policy's `new()` signature differs from what's shown above, adapt — e.g., `PrefixHashPolicy::new` may take a tokenizer reference; substitute with `PrefixHashPolicy::new_for_test()` if a test constructor exists, else write `OPEN_QUESTION: PrefixHashPolicy::new requires <X> not available in test scope, skipping this parity test for now`.)

`PrefixHashConfig::default()` and `ManualConfig::default()` may not have `Default` impls — check:

```bash
grep -nE 'impl Default for PrefixHashConfig|impl Default for ManualConfig' model_gateway/src/policies/*.rs
```

If they don't derive `Default`, construct with explicit values; if construction is too involved, replace that policy's parity test with a doc-comment explaining "parity for <X> is covered by the integration tests in <Y>" and continue.

- [ ] **Step 7.3: Run the tests**

```bash
cd /home/hkang/wl/smg-wl
cargo test -p smg policies::tests 2>&1 | tail -30
```

Expected: all 9 parity tests + the earlier UsageEvent / SelectWorkerInfo / select_worker_async / usage_sender tests pass. If a single policy fails parity, that's a real regression — investigate before continuing.

- [ ] **Step 7.4: Commit**

```bash
cd /home/hkang/wl/smg-wl
git add model_gateway/src/policies/mod.rs
git commit -m "test(policies): per-policy select_worker_async parity tests (Phase 1)

Adds 9 parametrized #[tokio::test] cases verifying that every existing
policy (bucket, cache_aware, consistent_hashing, dp_min_token, manual,
power_of_two, prefix_hash, random, round_robin) returns compatible
results from select_worker (sync) and select_worker_async (default
async fallback). \"Compatible\" tolerates RNG-driven divergence: both
must return None, or both must return Some(idx) with idx in valid
range. Strict equality is not required because Random samples per call.

These tests prove the async migration in Phase 1 introduced no behavior
regression for any existing policy.

Refs: docs/thunder/10-phases.md P1 row (e), worklog D-13"
```

---

## Task 8: Phase exit verification + worklog D-17 + branch merge

**Files:**
- Modify: `docs/thunder/worklog.md` (append D-17 entry)

`★ Reviewer note on merge:` This task ends with the **sub-branch merge ceremony** specified in `docs/thunder/workflow.md`:

```
git checkout thunder-policy
git merge --ff-only thunder-policy-p1
git branch -d thunder-policy-p1
```

Codex executes the merge **only** after Claude review signs off. Until then, leave the branch tip on `thunder-policy-p1`.

- [ ] **Step 8.1: Run full workspace verification**

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -5
cargo test --workspace 2>&1 | tail -30
cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -20
```

Expected:
- build green
- test green except the same pre-existing PD perf flake (`test_large_batch_bootstrap_injection`) noted in P0's D-16. If a NEW failure appears, do NOT merge — investigate and write `OPEN_QUESTION` if root cause is unclear.
- clippy green

- [ ] **Step 8.2: Run Phase 0 e2e regression check**

```bash
cd /home/hkang/wl/smg-wl
source e2e_test/.venv/bin/activate
pytest e2e_test/thunder/test_phase0_messages_passthrough.py -v \
    --rootdir=e2e_test/thunder --confcutdir=e2e_test/thunder 2>&1 | tail -10
```

Expected: 3/3 pass. P1 doesn't add new e2e tests (it's pure trait plumbing); the P0 e2e covering pass-through behavior is the right regression check.

- [ ] **Step 8.3: Run xref**

```bash
cd /home/hkang/wl/smg-wl
bash scripts/check_thunder_xref.sh 2>&1 | tail -20
```

Expected: `[OK] thunder cross-references look clean`.

- [ ] **Step 8.4: Verify the commit log**

```bash
cd /home/hkang/wl/smg-wl
git log --oneline thunder-policy..HEAD
```

Expected: 7 commits (one per task) all prefixed with `feat(...)` / `refactor(...)` / `test(...)` and ending with `(Phase 1)`. Each cites a spec section in its message body.

- [ ] **Step 8.5: Append D-17 to worklog**

Open `docs/thunder/worklog.md` and append after the existing D-16 entry:

```markdown
---

## D-17: P1 implementation completed — LoadBalancingPolicy trait extension landed

**Date**: 2026-04-30 (or actual date P1 lands)
**Spec ref**: `docs/thunder/10-phases.md` P1 row, `docs/thunder/04-smg-integration.md` §5.5b/§5.7

### What landed

- `UsageEvent` struct in `model_gateway/src/policies/mod.rs`
- `SelectWorkerInfo.program_id: Option<&'a str>` field in same file
- `#[async_trait]` on `LoadBalancingPolicy` trait + `async fn select_worker_async` default impl + `fn usage_sender` default-None
- New `model_gateway/src/routers/common/program_id.rs` (~30 LOC helper module)
- Async migration: `routers/http/router.rs::select_worker_for_model` + `routers/grpc/common/stages/worker_selection.rs::select_single_worker`
- 9 per-policy parity tests asserting `select_worker == select_worker_async` for bucket, cache_aware, consistent_hashing, dp_min_token, manual, power_of_two, prefix_hash, random, round_robin
- Phase 0 e2e regression: 3/3 still pass

### What did NOT change

- Zero policy implementation files modified (bucket.rs ... round_robin.rs untouched)
- PD path (`routers/grpc/common/stages/worker_selection.rs::select_pd_pair`) deliberately not migrated — PD scope deferred per worklog D-7
- `routers/anthropic/`, `routers/openai/`, `routers/gemini/` — 3rd-party path, out of scope
- `routers/http/pd_router.rs`, `routers/grpc/pd_router.rs` — PD routers, out of scope
- `policies/thunder.rs` — does not exist; arrives in P3
- CLI / config / observability / worker / e2e — out of scope

### Footguns surfaced

(Fill in any new findings discovered during P1 execution. Examples to look for:
- Async propagation chains that touched files not in the original scope estimate
- Clippy lints that fired on the default async impl
- gRPC RequestContext shape that required an OPEN_QUESTION resolution)

### Revisit conditions

1. If P3 adds a policy that needs async work in selection AND that policy is in the PD path, the deferral above must be reconsidered — the PD `select_pd_pair` will need its own async migration.
2. If `usage_sender` design proves insufficient (e.g., backpressure issues from unbounded channel under high load), revisit channel type — possibly switch to bounded with `try_send` + drop-on-full semantics.
3. If `program_id_hint` becomes performance-critical (millions of QPS), benchmark the `as_deref()` chain in `Metadata` lookup; today it's negligible.

### Approved by

(Pending P1 implementation commit + Claude review + user sign-off.)
```

- [ ] **Step 8.6: Commit the worklog entry**

```bash
cd /home/hkang/wl/smg-wl
git add docs/thunder/worklog.md
git commit -m "docs(thunder): worklog D-17 records P1 completion (Phase 1)

Captures the 7 commits + per-policy parity test sweep + footguns
surfaced during the async migration.

Refs: docs/thunder/10-phases.md P1 row"
```

- [ ] **Step 8.7: Write the final report**

Write a final report to `/tmp/codex-report-p1.md` containing:

```markdown
# Codex Report — Thunder Phase 1

## Commits
<paste output of: git log --oneline thunder-policy..HEAD>

## Build / test / clippy / e2e
- cargo build --workspace: <last 5 lines>
- cargo test --workspace: <summary line; flag any non-PD-perf failures>
- cargo clippy --all-targets --all-features -- -D warnings: <last 5 lines>
- pytest Phase 0 e2e: <summary line>

## OPEN_QUESTIONs raised
<verbatim, or "none">

## Deviations from plan
<verbatim, with rationale>

## Diff stat
<paste output of: git diff --stat thunder-policy..HEAD>
```

Then **STOP**. Do NOT execute the merge ceremony — that's gated on Claude review per `docs/thunder/workflow.md`.

---

## Phase exit criteria (summary)

| Check | Command | Required |
|---|---|---|
| Workspace builds | `cargo build --workspace` | ✅ |
| Workspace tests pass | `cargo test --workspace` (PD perf flake from D-16 allowed) | ✅ |
| Clippy clean | `cargo clippy --all-targets --all-features -- -D warnings` | ✅ |
| Phase 0 e2e regression | `pytest e2e_test/thunder/test_phase0_messages_passthrough.py` | ✅ |
| Per-policy parity tests | `cargo test -p smg policies::tests` (9 parity cases pass) | ✅ |
| Spec xref clean | `bash scripts/check_thunder_xref.sh` | preferred |
| Commits cite spec | every P1 commit body cites `docs/thunder/...` and ends with `(Phase 1)` | ✅ |
| PD path untouched | `select_pd_pair` diff = only `program_id: None,` field addition (Task 2's struct-literal cleanup), nothing else | ✅ |
| Final report | `/tmp/codex-report-p1.md` exists with sections above | ✅ |

---

## Rollback

If P1 review fails irrecoverably:

```bash
cd /home/hkang/wl/smg-wl
git checkout thunder-policy
git branch -D thunder-policy-p1   # destructive — confirm with user before running
```

The trunk `thunder-policy` is unaffected; rollback discards all P1 work and Codex / Claude restart from scratch with a refined plan.

---

## Self-review notes

- **Spec coverage**: every P1 sub-bullet in `10-phases.md` (a)-(e) is covered: (a) trait extension = Tasks 1+3; (b) SelectWorkerInfo field = Task 2; (c) program_id helper = Task 4; (d) async migrations = Tasks 5+6; (e) per-policy parity = Task 7.
- **Type consistency**: `Option<&'a str>` on `SelectWorkerInfo.program_id` ↔ `Option<&str>` on `select_worker_for_model`'s new param ↔ `Option<&str>` from `program_id::extract`. Lifetime threading is consistent: borrow flows from typed_req → param → struct field → policy.
- **Placeholder scan**: Step 6.2 has explicit "STOP and check" + an `OPEN_QUESTION` template if the gRPC `RequestContext` shape doesn't fit P-A/B/C patterns. This is a genuine fork-in-the-road; not a placeholder.
- **Dependency order**: Tasks 1+2 → Task 3 (trait uses both types); Tasks 1-4 → Task 5 (HTTP needs all building blocks); Tasks 1-4 → Task 6 (gRPC same); Tasks 1-7 → Task 8.
- **PD non-touch invariant**: enforced in Task 6 Step 6.3 with an explicit `git diff` sanity check before commit.
