# Phase 7 M1 — ProgramRequestGuard capacity leak fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix the production-blocker bug where `ProgramRequestGuard::Drop` only decrements `in_flight` but never subtracts `estimated_reserved_tokens` from `backend.active_program_tokens`, causing capacity leak on every client disconnect.

**Architecture:** Mirror `usage_consumer_task`'s un-reserve logic (already proven safe with saturating_sub) inside Drop's spawned cleanup task. Skip the "+ actual_total_tokens" step since no usage event arrived. Idempotency via existing `completed: bool` flag.

**Tech Stack:** Rust, tokio::spawn, std::sync::Weak, tokio::sync::RwLock

**Spec reference:** `docs/superpowers/specs/2026-05-01-thunder-phase7-production-design.md` §3.1

---

### Task 1: Drop fallback un-reserve

**Files:**
- Modify: `model_gateway/src/policies/thunder.rs:470-501` (Drop impl)
- Test: `model_gateway/src/policies/thunder.rs` mod tests (append)

- [ ] **Step 1.1: Write the failing test for un-reserve**

Append to `model_gateway/src/policies/thunder.rs` mod tests block (after the existing `mod tests` content):

```rust
    #[tokio::test]
    async fn drop_unreserves_estimated_tokens() {
        let policy = ThunderPolicy::with_defaults();
        let backend_url = "http://b1:8000".to_string();

        // Set up a program with reservation.
        {
            let mut state = policy.state.write().await;
            state.backends.insert(backend_url.clone(), BackendState {
                active_programs: ["pid-leak".to_string()].into_iter().collect(),
                active_program_tokens: 500,
                capacity_tokens: 1000,
            });
            state.programs.insert("pid-leak".to_string(), Program {
                program_id: "pid-leak".to_string(),
                backend_url: Some(backend_url.clone()),
                in_flight: 1,
                total_tokens: 0,
                step_count: 1,
                estimated_reserved_tokens: 500,
            });
        }

        // Drop guard — simulates client disconnect / error path.
        {
            let _guard = policy.create_guard("pid-leak");
            // _guard out of scope here → Drop fires
        }

        // Drop spawns async task; let it run.
        for _ in 0..50 {
            tokio::task::yield_now().await;
            let state = policy.state.read().await;
            let b = state.backends.get(&backend_url).unwrap();
            if b.active_program_tokens == 0 {
                let p = state.programs.get("pid-leak").unwrap();
                assert_eq!(p.estimated_reserved_tokens, 0, "reservation cleared");
                assert_eq!(p.in_flight, 0, "in_flight decremented");
                return;
            }
        }
        panic!("Drop fallback never un-reserved tokens (expected 0, capacity leaked)");
    }
```

- [ ] **Step 1.2: Run test to verify it fails**

```bash
cd /home/hkang/wl/smg-wl && cargo test --package smg --lib policies::thunder::tests::drop_unreserves_estimated_tokens 2>&1 | tail -20
```

Expected: FAIL with `assertion failed: reservation never cleared` or panic about active_program_tokens still 500.

- [ ] **Step 1.3: Apply the fix to `Drop::drop` body**

In `model_gateway/src/policies/thunder.rs`, replace the existing Drop impl body (currently lines 470-501) with:

```rust
impl Drop for ProgramRequestGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let Some(state) = self.state.upgrade() else {
            return; // policy already dropped — nothing to clean up
        };
        let pid = std::mem::take(&mut self.program_id);
        // `tokio::spawn` is fire-and-forget; matches the existing capacity-
        // poll / usage-consumer fire-and-forget pattern in this file.
        #[expect(
            clippy::disallowed_methods,
            reason = "fire-and-forget cleanup task — exits when policy dropped via Weak::upgrade returning None"
        )]
        tokio::spawn(async move {
            let mut guard = state.write().await;

            // Snapshot the per-program reservation BEFORE mutating state.
            // Mirrors usage_consumer_task's un-reserve pattern but skips the
            // "+ actual_total_tokens" step since no UsageEvent arrived.
            let (reserved, backend_url) = guard
                .programs
                .get(&pid)
                .map(|p| (p.estimated_reserved_tokens, p.backend_url.clone()))
                .unwrap_or((0, None));

            if let Some(url) = backend_url {
                if let Some(b) = guard.backends.get_mut(&url) {
                    b.active_program_tokens = b.active_program_tokens.saturating_sub(reserved);
                }
            }
            if let Some(p) = guard.programs.get_mut(&pid) {
                p.estimated_reserved_tokens = 0;
                if p.in_flight > 0 {
                    p.in_flight -= 1;
                }
            }
            // A slot may have freed — broadcast so paused programs re-check.
            // (M6 will replace broadcast with a scheduler signal.)
            let waiting: Vec<Arc<Notify>> = guard.waiting_events.values().cloned().collect();
            drop(guard);
            for n in &waiting {
                n.notify_waiters();
            }
            trace!(
                program_id = %pid,
                reserved_unwound = reserved,
                "ProgramRequestGuard drop fallback (no usage)"
            );
        });
    }
}
```

- [ ] **Step 1.4: Run test to verify it passes**

```bash
cd /home/hkang/wl/smg-wl && cargo test --package smg --lib policies::thunder::tests::drop_unreserves_estimated_tokens 2>&1 | tail -5
```

Expected: `test result: ok. 1 passed`

- [ ] **Step 1.5: Add three more tests for edge cases**

Append to the same `mod tests` block:

```rust
    #[tokio::test]
    async fn complete_suppresses_drop_unreserve() {
        let policy = ThunderPolicy::with_defaults();
        let backend_url = "http://b1:8000".to_string();
        {
            let mut state = policy.state.write().await;
            state.backends.insert(backend_url.clone(), BackendState {
                active_programs: ["pid-c".to_string()].into_iter().collect(),
                active_program_tokens: 500,
                capacity_tokens: 1000,
            });
            state.programs.insert("pid-c".to_string(), Program {
                program_id: "pid-c".to_string(),
                backend_url: Some(backend_url.clone()),
                in_flight: 1,
                total_tokens: 0,
                step_count: 1,
                estimated_reserved_tokens: 500,
            });
        }

        {
            let mut g = policy.create_guard("pid-c");
            g.complete();
            // out of scope → Drop fires but suppressed by `completed`
        }

        // Yield several times; if Drop ran cleanup, tokens would have changed.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        let state = policy.state.read().await;
        let b = state.backends.get(&backend_url).unwrap();
        assert_eq!(
            b.active_program_tokens, 500,
            "complete() must suppress Drop's un-reserve"
        );
        let p = state.programs.get("pid-c").unwrap();
        assert_eq!(p.estimated_reserved_tokens, 500, "reserved untouched");
        assert_eq!(p.in_flight, 1, "in_flight untouched");
    }

    #[tokio::test]
    async fn drop_with_no_program_does_not_panic() {
        let policy = ThunderPolicy::with_defaults();
        // Don't insert any program; just create + drop.
        {
            let _g = policy.create_guard("pid-missing");
        }
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        // No assertion needed — survival without panic is the test.
    }

    #[tokio::test]
    async fn drop_saturates_when_reserved_exceeds_backend_balance() {
        let policy = ThunderPolicy::with_defaults();
        let backend_url = "http://b1:8000".to_string();
        {
            let mut state = policy.state.write().await;
            state.backends.insert(backend_url.clone(), BackendState {
                active_programs: ["pid-sat".to_string()].into_iter().collect(),
                active_program_tokens: 100, // smaller than reservation
                capacity_tokens: 1000,
            });
            state.programs.insert("pid-sat".to_string(), Program {
                program_id: "pid-sat".to_string(),
                backend_url: Some(backend_url.clone()),
                in_flight: 1,
                total_tokens: 0,
                step_count: 1,
                estimated_reserved_tokens: 500,
            });
        }
        {
            let _g = policy.create_guard("pid-sat");
        }
        for _ in 0..50 {
            tokio::task::yield_now().await;
            let state = policy.state.read().await;
            let b = state.backends.get(&backend_url).unwrap();
            if b.active_program_tokens == 0 {
                return; // saturating_sub clamped to 0
            }
        }
        panic!("active_program_tokens did not saturate to 0");
    }
```

- [ ] **Step 1.6: Run all four new tests**

```bash
cd /home/hkang/wl/smg-wl && cargo test --package smg --lib policies::thunder::tests:: 2>&1 | grep -E "drop_|complete_suppresses" | tail -10
```

Expected: 4 tests pass.

- [ ] **Step 1.7: Run the full thunder test suite to ensure no regressions**

```bash
cd /home/hkang/wl/smg-wl && cargo test --package smg --lib policies::thunder 2>&1 | tail -10
```

Expected: all tests pass (existing + 4 new = ~22 total).

- [ ] **Step 1.8: Run clippy strict mode**

```bash
cd /home/hkang/wl/smg-wl && cargo clippy --package smg --all-targets --all-features -- -D warnings 2>&1 | tail -20
```

Expected: no warnings/errors.

- [ ] **Step 1.9: Worklog D-23 entry**

Append to `docs/thunder/worklog.md` (create section if missing):

```markdown

## D-23 (2026-05-01): Phase 7 M1 — capacity leak fix

`<SIGNED-OFF>` Phase 7 launched as full production scope (8 milestones, no deferrals). M1 ships first as the production blocker bug fix.

**Decision**: `ProgramRequestGuard::Drop` cleanup task now mirrors `usage_consumer_task`'s un-reserve logic — saturating_sub of `estimated_reserved_tokens` from `backend.active_program_tokens`, plus zeroing `program.estimated_reserved_tokens` to prevent double-unreserve. Idempotency preserved via existing `completed: bool` flag.

**Why now**: Without this fix, any client disconnect on a TR-mode admit (and after M2 lands, every streaming disconnect too) leaks reservation. Production uptime > a few hours would saturate every backend's apparent capacity.

**Tests**: 4 new unit tests in `policies::thunder::tests` cover happy-path Drop, complete() suppression, missing program, saturating_sub edge case.
```

- [ ] **Step 1.10: Commit M1**

```bash
cd /home/hkang/wl/smg-wl && git add model_gateway/src/policies/thunder.rs docs/thunder/worklog.md docs/superpowers/plans/2026-05-01-thunder-phase7-m1-capacity-leak.md
git commit -m "$(cat <<'EOF'
fix(policies): ProgramRequestGuard::Drop un-reserves tokens (Phase 7 M1)

Production-blocker bug fix: Drop's cleanup task now subtracts
estimated_reserved_tokens from backend.active_program_tokens, mirroring
usage_consumer_task's un-reserve pattern. Prevents capacity leak on every
client disconnect in TR mode.

4 new tests cover happy-path un-reserve, complete() suppression, missing
program defense, and saturating_sub clamping.

Spec: docs/superpowers/specs/2026-05-01-thunder-phase7-production-design.md §3.1
Plan: docs/superpowers/plans/2026-05-01-thunder-phase7-m1-capacity-leak.md
EOF
)"
```

Expected: commit created on `phase7-m1-gap5` branch.

- [ ] **Step 1.11: ff-merge to feat/thunder**

```bash
cd /home/hkang/wl/smg-wl && git checkout feat/thunder && git merge --ff-only phase7-m1-gap5 && git log --oneline -3
```

Expected: `feat/thunder` advanced to include M1 commit.

---

## Self-review checklist

- [x] Spec coverage: §3.1 fully addressed (Drop impl rewrite + 4 tests + worklog entry)
- [x] No placeholders: all code blocks contain actual code; commands are executable
- [x] Type consistency: `BackendState`, `Program`, `ProgramRequestGuard` references match existing struct definitions in `thunder.rs`
- [x] Test names follow Rust convention (snake_case)
- [x] Saturating_sub used everywhere arithmetic touches u64 capacity
