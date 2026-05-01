# Phase 7 M2 — Streaming wire-up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Wire streaming `/v1/chat/completions`, `/v1/messages`, and `/v1/responses` requests through Thunder state — extract usage at stream end, track incremental tokens during stream, force `include_usage=true` injection for OpenAI Chat, strip usage chunk from response when client didn't ask, and route streaming spawn through `ProgramRequestGuard` for cleanup.

**Architecture:** New `model_gateway/src/sse/` module with per-protocol parsers (OpenAI Chat / Anthropic Messages / OpenAI Responses). New `StreamingProgressEvent` channel on `LoadBalancingPolicy` trait (mirrors P1 `usage_sender`). Streaming spawn in `routers/http/router.rs` reshaped to feed extractor and emit usage + progress events.

**Tech Stack:** Rust, tokio mpsc, axum/hyper bytes_stream, serde_json, async_trait

**Spec reference:** `docs/superpowers/specs/2026-05-01-thunder-phase7-production-design.md` §3.2

---

### Task 1: Create `sse` module skeleton + StreamingProgressEvent + UsageEvent extension

**Files:**
- Create: `model_gateway/src/sse/mod.rs` (public API)
- Create: `model_gateway/src/sse/extractor.rs` (state machine + tests)
- Create: `model_gateway/src/sse/openai_chat.rs` (parser + tests)
- Create: `model_gateway/src/sse/anthropic.rs` (parser + tests)
- Create: `model_gateway/src/sse/responses.rs` (parser + tests)
- Modify: `model_gateway/src/lib.rs` (register `sse` module)
- Modify: `model_gateway/src/policies/mod.rs` (add StreamingProgressEvent, streaming_progress_sender, extend UsageEvent)

- [ ] **Step 1.1: Add `pub mod sse;` to `lib.rs`**

After `pub mod routers;` line in `model_gateway/src/lib.rs`:
```rust
pub mod sse;
```

- [ ] **Step 1.2: Create `sse/mod.rs` with public API**

Full content shown in spec §3.2.2. Key types: `SseProtocol`, `ParsedUsage`, `ParsedChunk`, `SseExtractor`. Will be implemented across files.

- [ ] **Step 1.3: Add StreamingProgressEvent + UsageEvent.cache_read_input_tokens to `policies/mod.rs`**

After existing UsageEvent struct, add:
```rust
#[derive(Debug, Clone)]
pub struct StreamingProgressEvent {
    pub program_id: String,
    pub delta_tokens: u64,
}
```

Add `cache_read_input_tokens: Option<u32>` field to UsageEvent.

Add to LoadBalancingPolicy trait (default returns None):
```rust
fn streaming_progress_sender(&self) -> Option<&UnboundedSender<StreamingProgressEvent>> {
    None
}
```

- [ ] **Step 1.4: Compile check**

```bash
cd /home/hkang/wl/smg-wl && cargo build --package smg 2>&1 | tail -10
```

Expected: builds successfully with empty sse module.

### Task 2: SseExtractor state machine + buffer-splitting

- [ ] **Step 2.1: Write SseExtractor + ProtocolState skeleton in `extractor.rs`**
- [ ] **Step 2.2: Write 4 unit tests for buffer splitting (cross-chunk, empty events, keepalive comments)**
- [ ] **Step 2.3: Implement minimal feed() + flush() that handles event boundaries**
- [ ] **Step 2.4: Run tests, ensure pass**

### Task 3: OpenAI Chat parser

- [ ] **Step 3.1: Write 7 unit tests in `openai_chat.rs::tests`**
- [ ] **Step 3.2: Implement OpenAi Chat parser (extract usage from last data chunk; count events for token_delta; strip usage chunk if requested)**
- [ ] **Step 3.3: Run tests, all pass**

### Task 4: Anthropic Messages parser

- [ ] **Step 4.1: Write 7 unit tests in `anthropic.rs::tests`** (cumulative output_tokens, message_start input_tokens, cache_read_input_tokens, message_stop signal)
- [ ] **Step 4.2: Implement Anthropic parser**
- [ ] **Step 4.3: Run tests, all pass**

### Task 5: OpenAI Responses parser

- [ ] **Step 5.1: Write 5 unit tests in `responses.rs::tests`**
- [ ] **Step 5.2: Implement Responses parser (response.completed event extraction; output_text.delta event counting)**
- [ ] **Step 5.3: Run tests, all pass**

### Task 6: ThunderPolicy progress_consumer_task + streaming_progress_sender impl

- [ ] **Step 6.1: Write unit test that progress events update Program.total_tokens**
- [ ] **Step 6.2: In `policies/thunder.rs::ThunderPolicy::new`, create unbounded channel + spawn `progress_consumer_task` that drains StreamingProgressEvent and updates Program.total_tokens**
- [ ] **Step 6.3: Implement `streaming_progress_sender()` method**
- [ ] **Step 6.4: Run test, passes**

### Task 7: Router streaming wire-up

- [ ] **Step 7.1: Detect SseProtocol from endpoint URL (helper fn in routers/http/router.rs)**
- [ ] **Step 7.2: For OpenAI Chat is_stream requests, force `stream_options.include_usage=true`; record original_user_explicitly_asked_for_usage flag**
- [ ] **Step 7.3: Replace existing streaming spawn block (router.rs:823-861) with extractor-based loop:**
  - Create SseExtractor with strip_usage flag
  - Create guard via policy.create_guard(pid)
  - Loop: feed chunk → forward filtered bytes → emit progress on threshold → emit UsageEvent on usage extraction + complete()
  - On exit: flush() residual + handle final usage if seen on flush
  - guard auto-drops; M1 fallback handles disconnect path
- [ ] **Step 7.4: Compile + run existing thunder e2e to ensure no regression**

### Task 8: Mock vLLM streaming modes

- [ ] **Step 8.1: Add to `e2e_test/thunder/mock_vllm.py`:**
  - `--stream-mode openai-chat` (5 content delta + usage chunk + [DONE])
  - `--stream-mode openai-chat-no-usage` (5 content delta + [DONE], no usage chunk)
  - `--stream-mode anthropic-messages` (message_start + 5 content_block_delta + message_delta + message_stop)
  - `--stream-mode openai-responses` (response.created + 5 output_text.delta + response.completed)
  - `--stream-mode broken-eof` (3 chunks + abrupt close)
- [ ] **Step 8.2: Manual smoke test: run mock with each mode and verify output**

### Task 9: E2E tests + commit

- [ ] **Step 9.1: Create `e2e_test/thunder/test_phase7_m2_streaming.py` with 6 tests**
- [ ] **Step 9.2: Run e2e suite — pytest pass**
- [ ] **Step 9.3: cargo clippy + cargo test full thunder**
- [ ] **Step 9.4: Worklog D-25 ~ D-28 entries**
- [ ] **Step 9.5: Commit with message `feat(sse,thunder): streaming usage extraction across 3 protocols (Phase 7 M2 Gap6)`**
- [ ] **Step 9.6: ff-merge to thunder-policy**
