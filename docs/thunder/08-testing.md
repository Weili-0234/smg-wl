> Part of the [Thunder Policy spec](00-INDEX.md). Companion: [worklog](worklog.md) — design decisions with revisit conditions.

# Thunder — Testing Strategy

## 9. Testing strategy

### 9.1 Pytest fixtures (lifted from old worktree)

`e2e_test/thunder/conftest.py`:

- **`mock_vllm_server`** (class-scoped): spawns `mock_vllm.py` with configurable `--port`, `--total-kv-cache-tokens`, `--stream-chunk-count`, `--stream-delay-ms`. Yields `(url, control_client)` where `control_client` exposes `set_capacity(int)`, `get_state()`. **Lifted verbatim from `/home/hkang/wl/smg_thunder/e2e_test/thunder/mock_vllm.py` (290 LOC, 100% reusable)**.

- **`smg_thunder_server`** (function-scoped): spawns `smg --policy thunder --worker-urls <mock_url>` with configurable extras (`--thunder-sub-mode tr`, `--thunder-resume-timeout-secs 5`). Yields `(thunder_url, prometheus_url)`. (Old fixture used `--backend thunder` — sed replacement.)

- **`thunder_client`**: `httpx.Client` pre-pointed at thunder URL.

### 9.2 Phase-by-phase tests (summarized; details in §11)

1. Setup: spawn fresh mock + smg
2. Drive: requests with/without streaming, with/without `program_id`
3. Observe: query `/thunder/programs`, `/metrics`, mock's `/control/state`
4. Assert: program/backend state matches expected; metric values within tolerance

### 9.3 What's NOT covered in e2e

- Concurrency races beyond what scheduler tick triggers naturally — covered by Rust unit tests on `RouterState` methods.
- Drop-on-cancel correctness — Rust unit test using `tokio::test` + explicit `drop(handle)`.
- Benchmark/load tests — out of scope until Phase P9 polish.

---
