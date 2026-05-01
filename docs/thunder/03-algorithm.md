> Part of the [Thunder Policy spec](00-INDEX.md). Companion: [worklog](worklog.md) — design decisions with revisit conditions.

# Thunder — Algorithm Core (faithful Python port)

## 4. Algorithm core (faithful to Python, with sign-off forks)

Reference: `/home/hkang/wl/smg_thunder/ThunderAgent/ThunderAgent/` (read-only). Verbatim translation, modulo §2 forks.

### 4.1 Program lifecycle (orthogonal axes)

```rust
pub struct Program {
    pub program_id: String,
    pub status: ProgramStatus,           // REASONING | ACTING
    pub state: ProgramState,             // ACTIVE | PAUSED | TERMINATED
    pub backend_url: Option<String>,     // None when PAUSED-queued or new-not-yet-placed
    pub total_tokens: i64,
    pub step_count: u32,
    pub acting_since: Option<Instant>,   // for 2^-t decay (Phase P9)
    pub origin_backend: Option<String>,  // for force-resume target hint
    pub marked_for_pause: bool,          // REASONING-not-yet-paused
}

pub enum ProgramStatus { Reasoning, Acting }
pub enum ProgramState { Active, Paused, Terminated }
```

`waiting_event` from Python is **not** a field on `Program` — tracked separately as `HashMap<String, Arc<Notify>>` on `RouterState` to avoid `Notify` ownership ambiguity across pause/resume.

Transitions:

| From | Event | To |
|---|---|---|
| (none) | first request with program_id | `(REASONING, ACTIVE)` |
| `(REASONING, ACTIVE)` | response received | `(ACTING, ACTIVE)`, `acting_since=Instant::now()` |
| `(ACTING, ACTIVE)` | next request | `(REASONING, ACTIVE)`, `acting_since=None` |
| `(ACTING, ACTIVE)` | scheduler `pause_until_safe` picks me | `(ACTING, PAUSED)`, save `origin_backend`, register waiting_event |
| `(REASONING, ACTIVE)` | scheduler `pause_until_safe` picks me | `(REASONING, ACTIVE)` with `marked_for_pause=true`; finalizes to PAUSED only when request completes |
| `(*, PAUSED)` | BFD greedy_resume | `(*, ACTIVE)` on possibly-different backend |
| `(*, ACTIVE)` | timeout (30 min default) | force-resume via least-loaded fallback |
| `(*, *)` | request stream cancelled / `ProgramRequestGuard::Drop` | TERMINATED, drop from registry |

### 4.2 Per-backend state

```rust
pub struct BackendState {
    pub url: String,
    pub shared_tokens: i64,                       // updated by scheduler tick (Q5.3 fork)
    pub future_paused_tokens: i64,
    pub cache_config: Option<VllmCacheConfig>,
    pub latest_metrics: Option<MetricsSnapshot>,
    pub healthy: bool,
    pub metrics_client: Box<dyn MetricsClient>,   // trait object: vllm | sglang | …
}
```

Capacity formulas (verbatim Python `backend/state.py:185-194`):

- `active_program_tokens = reasoning_tokens + tool_coefficient * acting_tokens`
- `remaining_capacity = total_kv_capacity - (active_program_tokens - shared_tokens + active_count * BUFFER_PER_PROGRAM)`
- `BUFFER_PER_PROGRAM = 100` (Python constant)
- `remaining_capacity_with_decay`: ACTING tokens weighted by `2^(-t)` where `t = (now - acting_since).as_secs_f64()` (toggled by `--thunder-use-acting-token-decay`).

### 4.3 Scheduler tick (every `--thunder-scheduler-interval-secs`, default 5s)

```rust
async fn scheduler_tick(state: Arc<RwLock<RouterState>>, config: Arc<ThunderConfig>) {
    // STEP 1: fetch metrics (NETWORK — outside guard, hard rule §2.1)
    let backend_urls: Vec<String> = state.read().backends.keys().cloned().collect();
    let mut fetched: Vec<(String, Result<MetricsSnapshot>)> = vec![];
    for url in backend_urls {
        let client = state.read().backends.get(&url).map(|b| b.metrics_client.clone_box());
        if let Some(c) = client { fetched.push((url, c.fetch_metrics().await)); }
    }

    // STEP 2: write-guarded apply + shared_tokens update + BFD + pause
    {
        let mut s = state.write();
        for (url, result) in fetched {
            if let Ok(metrics) = result {
                s.apply_metrics_for_backend(&url, metrics);
                s.update_shared_tokens_for_backend(&url);   // Q5.3 fork — ADD this call
            }
            // metric fetch error: retain stale shared_tokens, log warn
        }
        s.greedy_resume();
        for url in s.backends.keys().cloned().collect::<Vec<_>>() {
            if s.backends.get(&url).map(|b| b.remaining_capacity() < 0).unwrap_or(false) {
                s.pause_until_safe(&url);
            }
        }
        s.notify_resumed_waiters();   // wake `waiting_event` for newly-resumed programs
    }
    // emit smg_thunder_scheduler_tick_duration_seconds outside guard
}
```

### 4.4 BFD greedy resume (verbatim Python `router.py:719-844`)

```
function greedy_resume(state):
  per_backend_remaining = {}
  total_capacity = 0
  for each backend in state.backends:
    rem = backend.remaining_capacity_with_decay() if config.use_acting_token_decay
          else backend.remaining_capacity()
    if rem > BUFFER_PER_PROGRAM:
      per_backend_remaining[url] = rem
      total_capacity += rem

  if total_capacity <= 0 or waiting_queue is empty: return

  # priority groups
  reasoning_group  = waiting_queue ∩ {pid : program.step_count > 1 and program.status == REASONING}
  new_program_group = waiting_queue ∩ {pid : program.step_count == 1}
  acting_group      = waiting_queue \ (reasoning_group ∪ new_program_group)

  # within each group: ascending by total_tokens
  candidates = sort(reasoning_group, asc, key=tokens) ++
               sort(new_program_group, asc, key=tokens) ++
               sort(acting_group, asc, key=tokens)

  # cumulative-capacity feasibility selection
  selected = []
  cumulative = 0
  for pid in candidates:
    required = program(pid).total_tokens + BUFFER_PER_PROGRAM
    if cumulative + required <= total_capacity:
      selected.append(pid); cumulative += required

  if not selected: return

  # BFD placement: re-sort selected DESC, backends DESC by remaining
  selected.sort_by(tokens, desc)
  backend_list = sorted(per_backend_remaining.items(), by=remaining, desc)

  for pid in selected:
    required = program(pid).total_tokens + BUFFER_PER_PROGRAM
    if backend_list is empty: break
    max_remaining = backend_list[0].remaining
    if required > max_remaining:
      smallest_required = selected[-1].total_tokens + BUFFER_PER_PROGRAM
      if smallest_required > max_remaining: break
      else: continue

    target_url = backend_list[0].url
    resume_program(pid, target_url)
    backend_list[0].remaining -= required
    if backend_list[0].remaining <= BUFFER_PER_PROGRAM:
      pop from backend_list
    else:
      re-sort backend_list desc
```

### 4.5 `pause_until_safe` (verbatim Python `router.py:685-717`)

```
while backend.remaining_capacity() < 0:
  acting_programs = sorted(programs on backend with status=ACTING, by tokens ascending)
  if acting_programs:
    pause(acting_programs[0])  # save origin_backend, register waiting_event, transition to PAUSED
    continue
  reasoning_programs = sorted(
    programs on backend with status=REASONING and not marked_for_pause,
    by tokens ascending
  )
  if reasoning_programs:
    mark_for_pause(reasoning_programs[0])  # add tokens to future_paused_tokens
    continue
  break  # nothing to pause; accept overflow
```

### 4.6 `char_to_token_ratio` momentum (Q5.5)

Initial 5.0 (Python `router.py:114`). After every successful response with `usage.prompt_tokens` available:

```rust
let new_ratio = prompt_chars as f64 / prompt_tokens as f64;
state.char_to_token_ratio = if state.first_sample_received {
    0.2 * new_ratio + 0.8 * state.char_to_token_ratio
} else {
    state.first_sample_received = true;
    new_ratio
};
```

If `usage` block missing (upstream non-compliant): skip + `tracing::debug!`.

### 4.7 30-minute force-resume timeout (Q5.1)

`select_worker_async` paused-flow:

```rust
let timeout = Duration::from_secs(config.resume_timeout_secs);  // default 1800
match tokio::time::timeout(timeout, notify.notified()).await {
    Ok(_) => { /* normal resume */ }
    Err(_) => {
        // force-resume via least-loaded fallback (Q5.6 default-mode selection logic)
        let fallback_backend = state.write().select_backend_for_new_program_default();
        // log warn + emit smg_thunder_force_resume_total
    }
}
```

### 4.8 Sub-modes

| Sub-mode | Scheduler runs? | Capacity gate? | Pause/resume? | Use case |
|---|---|---|---|---|
| `default` (`ThunderSubMode::Default`) | No | No | No | Program tracking + multi-worker LB by least active count |
| `tr` (`ThunderSubMode::Tr`) | Yes | Yes | Yes | Full ThunderAgent semantics |

Both modes use **the same `ThunderPolicy`**; sub-mode is a runtime field that toggles the scheduler-spawn step and the admission branch in `select_worker_async`.

---

---

## 13. Glossary

| Term | Meaning |
|---|---|
| **Program** | Long-lived agent task with multiple LLM requests sharing a `program_id`. Has lifecycle Status × State. |
| **Status** | REASONING (on GPU, generating) vs. ACTING (off GPU, executing tools). |
| **State** | ACTIVE (on backend) vs. PAUSED (queued for resume) vs. TERMINATED. |
| **Sub-mode** | thunder operating mode: `default` (program tracking, no capacity gating) vs. `tr` (full pause/resume). |
| **BFD** | Best Fit Decreasing — bin-packing heuristic for greedy resume. |
| **Active program tokens** | `reasoning_tokens + tool_coefficient × acting_tokens`. |
| **Shared tokens** | Prefix cache savings: `max(0, reasoning_program_tokens − kv_cache_usage_perc × total_capacity)`. Q5.3 fork — Python defines but never calls; Rust calls in scheduler tick. |
| **Future paused tokens** | Tokens of REASONING programs marked-for-pause, deferred until they transition to ACTING. |
| **Mark-for-pause** | A REASONING program is never paused mid-generation; instead it's marked, and pause finalizes when the request completes. |
| **char_to_token_ratio** | Global momentum-blended ratio (0.2*new + 0.8*old), used to estimate token count from prompt char count for new-program admission. Initial 5.0. |
| **UsageEvent** | Channel message from router post-stream → ThunderPolicy. Carries authoritative `usage.{prompt,completion,total,cached}_tokens` + `prompt_chars`. |
| **DEFAULT_PROGRAM_ID** | Literal `"default"` — fallback when client omits `program_id` (Q5.2 FAITHFUL). |

---
