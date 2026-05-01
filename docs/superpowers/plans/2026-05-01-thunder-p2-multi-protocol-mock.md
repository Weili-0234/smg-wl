# Thunder P2 — Multi-Protocol Mock + Smoke Test Plan

> **For agentic workers:** Opus subagent executes this plan via `superpowers:subagent-driven-development`. Steps use `- [ ]` for tracking. Claude reviews against R1-R12 in `docs/thunder/workflow.md`.

**Goal:** Extend `mock_vllm.py` with a `POST /v1/responses` handler and add a Phase 2 smoke test proving SMG routes ALL 3 protocols (`/v1/chat/completions`, `/v1/messages`, `/v1/responses`) to the same backend under `--policy cache_aware`. This locks down the multi-protocol surface before P3 starts swapping in `--policy thunder`.

**Architecture:** Pure test-infrastructure phase. No Rust code changes (P0 already wired `/v1/messages` and `/v1/chat/completions`; the protocols/responses crate already implements `GenerationRequest` for `ResponsesRequest` per chat.rs:580-655 mirror).

**Tech Stack:** Python 3.13 + stdlib `http.server` (no FastAPI). Pytest in `e2e_test/.venv`. SMG cargo binary already built at `target/debug/smg`.

---

## Context

- `docs/thunder/10-phases.md` row P2.
- P0 mock state at HEAD (`6a306544`): handles `/v1/chat/completions`, `/v1/messages`, `/control/capacity`, `/control/state`, `/v1/models`, `/version`, `/health`, `/get_server_info`. Missing: `/v1/responses`.
- P0 e2e: `test_phase0_messages_passthrough.py` (3 cases, all green). Same conftest fixtures will drive P2 smoke.
- `crates/protocols/src/responses.rs:3057+` already implements `GenerationRequest` for `ResponsesRequest`. SMG's `Router::route_responses` at `routers/http/router.rs:1139` already pass-throughs to `route_typed_request(.., "/v1/responses", ..)`. So the gateway side is already wired — P2 only needs the mock to accept the path.

**Out of scope** (do NOT touch):
- Any Rust file. P2 is Python-only.
- Conftest fixtures (P0 set them up; reuse).
- Anything under `routers/anthropic/`, `routers/openai/`, etc.

---

## Pre-flight

- [ ] **PF.1:** confirm worktree clean on `thunder-policy-p2` (sub-branch). `git status --short` empty.
- [ ] **PF.2:** confirm `target/debug/smg` exists (built during P1). If missing, `cargo build -p smg`.
- [ ] **PF.3:** confirm `e2e_test/.venv` is healthy: `source e2e_test/.venv/bin/activate && pytest --version`.
- [ ] **PF.4:** confirm P0 e2e still green to baseline: `pytest e2e_test/thunder/test_phase0_messages_passthrough.py -v --rootdir=e2e_test/thunder --confcutdir=e2e_test/thunder`. **Kill any stale `mock_vllm.py` processes before running** — `pkill -f mock_vllm.py` (the P0 tests rely on conftest's `_free_port()` so this is safety, not necessity).

---

## Task 1: Add `/v1/responses` POST handler to mock

**Files:**
- Modify: `e2e_test/thunder/mock_vllm.py` (~30 LOC: dispatcher arm + `_handle_responses` method)

The OpenAI Responses API has its own response shape (different from `chat.completion`). For Thunder testing purposes the mock returns the **same OpenAI chat.completion shape** for all 3 endpoints — this matches what a real litellm-proxy sidecar does (Anthropic/Responses translated to OpenAI Chat upstream then translated back). SMG's `route_typed_request` is byte-stream-forwarding so it doesn't validate the response body shape.

- [ ] **Step 1.1:** Add dispatcher arm in `do_POST` (around line 148-156):

```python
def do_POST(self):
    if self.path == "/v1/chat/completions":
        self._handle_chat()
    elif self.path == "/v1/messages":
        self._handle_messages()
    elif self.path == "/v1/responses":
        self._handle_responses()
    elif self.path == "/control/capacity":
        self._handle_capacity_update()
    else:
        self._send_text(404, f"not found: {self.path}\n")
```

- [ ] **Step 1.2:** Add `_handle_responses` method after `_handle_messages` (insertion point: just before `_handle_capacity_update` around line 306):

```python
        # ---------- /v1/responses handler (Phase 2 multi-protocol smoke) ----------
        def _handle_responses(self) -> None:
            """OpenAI Responses API endpoint. Like _handle_messages, returns an
            OpenAI chat.completion shape — matches what a litellm-proxy sidecar
            produces after Responses-in → Chat-in upstream translation. Phase 2
            tests assert pass-through, not body-shape compliance.
            """
            payload = self._read_json_body()
            with state.lock:
                state.request_count += 1
                req_idx = state.request_count
            stream = bool(payload.get("stream"))
            # Responses API uses `input` field (string or array) instead of `messages`.
            input_field = payload.get("input")
            metadata = payload.get("metadata") or {}
            program_id = metadata.get("program_id")
            LOG.info(
                "responses #%d stream=%s has_input=%s program_id=%s",
                req_idx, stream, input_field is not None, program_id or "<none>",
            )
            if stream:
                self._stream_chat(req_idx)
                return
            content = state.canned_content
            # Approximate prompt size from input field
            if isinstance(input_field, str):
                prompt_chars = len(input_field)
            elif isinstance(input_field, list):
                prompt_chars = sum(len(str(item)) for item in input_field)
            else:
                prompt_chars = 0
            prompt_tokens = max(1, prompt_chars // 4)
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
                # Echo program_id so e2e can assert plumbing
                "_mock_echo_program_id": program_id,
                "_mock_endpoint": "/v1/responses",
            }
            self._send_json(200, payload_out)
```

- [ ] **Step 1.3:** Update mock module docstring (top of file, around line 7) to mention the new endpoint:

```python
"""Mock vLLM backend for Thunder router e2e tests.

Pure stdlib (no pip install needed) — uses http.server.ThreadingHTTPServer.
Mimics the subset of vLLM's HTTP API that Thunder cares about:

  POST /v1/chat/completions      OpenAI Chat (canonical)
  POST /v1/messages              Anthropic Messages (P0+; OpenAI-shape response)
  POST /v1/responses             OpenAI Responses (P2+; OpenAI-shape response)
  GET  /v1/models                vLLM-compat for SMG worker discovery
  GET  /version                  vLLM-compat for SMG worker discovery
  GET  /get_server_info          vLLM cache-config-shaped JSON
  POST /control/capacity         Test knob: dynamically resize KV capacity
  GET  /control/state            Test introspection
"""
```

(Preserve the `Run:` and below sections.)

- [ ] **Step 1.4:** Smoke-test the mock standalone:

```bash
python3 /home/hkang/wl/smg-wl/e2e_test/thunder/mock_vllm.py --port 18999 &
sleep 1
curl -sS -X POST http://localhost:18999/v1/responses \
    -H "content-type: application/json" \
    -d '{"model":"test","input":"hello","metadata":{"program_id":"smoke-r1"}}' | python3 -m json.tool
kill %1; wait 2>/dev/null
```

Expected: JSON with `"_mock_endpoint": "/v1/responses"` and `"_mock_echo_program_id": "smoke-r1"`.

- [ ] **Step 1.5:** `python3 -m py_compile e2e_test/thunder/mock_vllm.py` exit 0.

- [ ] **Step 1.6:** Commit:

```bash
git add e2e_test/thunder/mock_vllm.py
git commit -m "test(thunder): mock backend handles /v1/responses (Phase 2)

Returns OpenAI chat.completion shape for /v1/responses, matching what a
litellm-proxy sidecar produces after Responses→Chat upstream
translation. Phase 2 smoke test asserts pass-through, not body-shape
compliance.

Refs: docs/thunder/10-phases.md P2 row"
```

---

## Task 2: Phase 2 multi-protocol smoke test

**Files:**
- Create: `e2e_test/thunder/test_phase2_multi_protocol_smoke.py` (~80 LOC)

`★ Decision tag (autonomous):` The plan calls for "smoke test driving SMG with `--policy cache_aware`". P0's conftest already builds an `smg_router` fixture with `--policy cache_aware`. We **reuse the existing fixture** without modification — keeps both P0 and P2 e2e on the same SMG instance for fast iteration. (`<CLAUDE-AUTONOMOUS-DECISION>` — flagged for worklog if user wants different fixture.)

- [ ] **Step 2.1:** Write the test file:

```python
"""Phase 2 multi-protocol smoke: SMG routes /v1/chat/completions,
/v1/messages, /v1/responses to the same backend under --policy cache_aware.

After Phase 2:
- mock_vllm.py handles all 3 endpoints (P0 added /v1/messages; P2 adds /v1/responses)
- This test proves the protocol seam is symmetric across all three.

ThunderPolicy doesn't exist yet (Phase 3); we use cache_aware as the
default-friendly policy.
"""
from __future__ import annotations

import requests


def test_chat_completions_passthrough(smg_router):
    body = {
        "model": "Qwen/Qwen3-0.6B",
        "messages": [{"role": "user", "content": "ping chat"}],
        "max_tokens": 16,
        "metadata": {"program_id": "smoke-chat"},
    }
    r = requests.post(f"{smg_router}/v1/chat/completions", json=body, timeout=10)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("object") == "chat.completion"


def test_messages_passthrough(smg_router):
    body = {
        "model": "Qwen/Qwen3-0.6B",
        "max_tokens": 16,
        "messages": [{"role": "user", "content": "ping messages"}],
        "metadata": {"program_id": "smoke-msg"},
    }
    r = requests.post(f"{smg_router}/v1/messages", json=body, timeout=10)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("_mock_echo_program_id") == "smoke-msg"


def test_responses_passthrough(smg_router):
    body = {
        "model": "Qwen/Qwen3-0.6B",
        "input": "ping responses",
        "metadata": {"program_id": "smoke-rsp"},
    }
    r = requests.post(f"{smg_router}/v1/responses", json=body, timeout=10)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("_mock_endpoint") == "/v1/responses"
    assert body.get("_mock_echo_program_id") == "smoke-rsp"


def test_program_id_consistent_across_protocols(smg_router):
    """Three requests, same program_id, three protocols. Backend should see
    the same program_id on all three. This is the seam ThunderPolicy will
    rely on in Phase 3+ for cross-protocol program-aware routing."""
    pid = "cross-proto-pid"
    chat_body = {"model": "Qwen/Qwen3-0.6B", "messages": [{"role": "user", "content": "1"}],
                 "max_tokens": 8, "metadata": {"program_id": pid}}
    msg_body = {"model": "Qwen/Qwen3-0.6B", "max_tokens": 8,
                "messages": [{"role": "user", "content": "2"}],
                "metadata": {"program_id": pid}}
    rsp_body = {"model": "Qwen/Qwen3-0.6B", "input": "3", "metadata": {"program_id": pid}}

    r1 = requests.post(f"{smg_router}/v1/chat/completions", json=chat_body, timeout=10)
    r2 = requests.post(f"{smg_router}/v1/messages", json=msg_body, timeout=10)
    r3 = requests.post(f"{smg_router}/v1/responses", json=rsp_body, timeout=10)

    assert r1.status_code == 200, r1.text
    assert r2.status_code == 200, r2.text
    assert r3.status_code == 200, r3.text
    # /v1/messages and /v1/responses echo program_id; /v1/chat/completions doesn't
    # because the canonical OpenAI chat.completion handler in P0 didn't add the
    # echo field. That's fine — proving 200 OK on chat is the seam check.
    assert r2.json().get("_mock_echo_program_id") == pid
    assert r3.json().get("_mock_echo_program_id") == pid
```

- [ ] **Step 2.2:** Run the test:

```bash
cd /home/hkang/wl/smg-wl
source e2e_test/.venv/bin/activate
# Kill any stale mock from prior sessions
pkill -f 'mock_vllm.py --port' 2>/dev/null
pkill -f 'target/debug/smg start' 2>/dev/null
sleep 1
pytest e2e_test/thunder/test_phase2_multi_protocol_smoke.py -v \
    --rootdir=e2e_test/thunder --confcutdir=e2e_test/thunder 2>&1 | tail -20
```

Expected: 4/4 pass.

If `test_program_id_consistent_across_protocols` fails because chat_completions returns no echo field, that's OK — the assertion only checks status 200 + the messages/responses echoes. (Confirm by reading the test body before flagging.)

- [ ] **Step 2.3:** Commit:

```bash
git add e2e_test/thunder/test_phase2_multi_protocol_smoke.py
git commit -m "test(thunder): Phase 2 multi-protocol smoke (chat/messages/responses) (Phase 2)

Four test cases: (1-3) per-protocol passthrough; (4) cross-protocol
program_id stickiness when backend logs the metadata. Tests use
--policy cache_aware via P0's existing conftest fixture; ThunderPolicy
in Phase 3 will swap the policy and run the same suite.

Refs: docs/thunder/10-phases.md P2 row"
```

---

## Task 3: Phase exit + worklog D-18 + ff-merge prep

- [ ] **Step 3.1:** Run full verification:

```bash
cd /home/hkang/wl/smg-wl
cargo build --workspace 2>&1 | tail -3
source e2e_test/.venv/bin/activate
pytest e2e_test/thunder/ -v --rootdir=e2e_test/thunder --confcutdir=e2e_test/thunder 2>&1 | tail -15
bash scripts/check_thunder_xref.sh 2>&1 | tail -5
```

Expected: build green, 7 e2e pass (3 from P0 + 4 from P2), xref OK.

- [ ] **Step 3.2:** Append D-18 to `docs/thunder/worklog.md`:

```markdown
---

## D-18: P2 implementation completed — multi-protocol mock + smoke test

**Date**: 2026-05-01
**Spec ref**: `docs/thunder/10-phases.md` P2 row
**Approval mode**: <CLAUDE-AUTONOMOUS-DECISION> — Claude authored plan + reviewed work; user sign-off pending.

### What landed

- `mock_vllm.py`: `/v1/responses` POST handler (~50 LOC) returning OpenAI chat.completion shape with `_mock_endpoint` and `_mock_echo_program_id` fields
- `test_phase2_multi_protocol_smoke.py`: 4 test cases proving SMG routes all 3 protocols to same backend under `--policy cache_aware`

### What did NOT change

- Zero Rust file modified — P2 is Python-only
- Conftest reused unchanged — same `smg_router` fixture from P0
- ThunderPolicy not yet in scope (P3)

### Autonomous decisions made

1. **Reuse P0's `smg_router` fixture** unchanged rather than creating a new P2-specific fixture. Rationale: same SMG binary, same `--policy cache_aware` flag, no per-phase fixture state. If user prefers per-phase fixture isolation, easy to revisit later.
2. **Mock returns OpenAI chat.completion shape for /v1/responses** rather than the canonical OpenAI Responses API shape. Rationale: matches what litellm-proxy produces (Responses-in → Chat-in upstream → Chat-out). SMG just byte-stream-forwards. If real OpenAI Responses shape becomes a P3+ test requirement, mock can be extended.
3. **No Phase 2 streaming test**. Rationale: streaming SSE shape varies across the 3 protocols (chat: bare data:; messages: event+data; responses: ad-hoc). P3 will tackle streaming-aware routing; P2 stays non-streaming for surface clarity.

### Revisit conditions

1. If real-world responses-shape compliance is needed → extend mock with proper Responses payload.
2. If streaming behavior diverges across protocols in production → add P2.5 streaming smoke.

### Approved by

(Pending user review.)
```

- [ ] **Step 3.3:** Commit worklog:

```bash
git add docs/thunder/worklog.md
git commit -m "docs(thunder): worklog D-18 records P2 completion (Phase 2)

Multi-protocol mock + smoke test landed. 3 autonomous decisions
documented: fixture reuse, OpenAI-shape /v1/responses, no streaming
test in P2.

Refs: docs/thunder/10-phases.md P2 row"
```

- [ ] **Step 3.4:** Final state report. Run:

```bash
cd /home/hkang/wl/smg-wl
git log --oneline thunder-policy..HEAD
git diff --stat thunder-policy..HEAD
```

Then STOP. Claude reviews + ff-merges manually.

---

## Phase exit criteria

| Check | Required |
|---|---|
| `cargo build --workspace` green | ✅ |
| `pytest e2e_test/thunder/` 7/7 pass (3 P0 + 4 P2) | ✅ |
| `bash scripts/check_thunder_xref.sh` `[OK]` | preferred |
| Commits cite spec | ✅ |
| Worklog D-18 with `<CLAUDE-AUTONOMOUS-DECISION>` tag | ✅ |
| 3 commits on `thunder-policy-p2` (mock + test + worklog) | ✅ |
