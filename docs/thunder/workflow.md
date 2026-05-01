# Thunder Phase Execution Workflow (P1+ SOP)

> Companion to [`10-phases.md`](10-phases.md) (the phase plan) and [`worklog.md`](worklog.md) (decision history). This file describes **HOW** each phase gets executed; the phase plan describes **WHAT** each phase delivers.

## Background

P0 was executed by Claude (Opus 4.7) dispatching opus subagents per task. The user's cost-vs-quality calculation for P1+ shifts execution to **Codex CLI (gpt-5.5 high)**, with Claude in a pure review role. This file freezes the SOP for that mode.

## Roles

| Actor | Responsibility |
|---|---|
| **User (Weili)** | Algorithm authority. Signs off on D-N decisions and Open Questions Codex/Claude can't resolve. |
| **Claude (Opus 4.7)** | Plan author + reviewer. Writes per-phase plans in superpowers TDD-step format, builds Codex briefs, reviews diffs and test output, escalates to user when Codex deviates from spec or hits a design ambiguity. **Does NOT implement.** |
| **Codex (gpt-5.5 high via `codex exec`)** | Implementer. Reads the plan + brief, follows the TDD steps verbatim, runs `cargo build/test/clippy` + `pytest`, commits per-task. **Does NOT make design decisions** (see "Codex autonomy boundaries" below). |

## Per-phase loop

```
┌──── PHASE PN ────┐
│                  │
│ A. Plan author   │ ← Claude (this file's section "Plan format")
│      ↓           │
│ B. Pre-flight    │ ← Claude (verify worktree/branch/build, create sub-branch)
│      ↓           │
│ C. Codex brief   │ ← Claude (compose execution brief, see "Codex brief template")
│      ↓           │
│ D. Codex execute │ ← `codex exec --model gpt-5.5 --reasoning-effort high < brief.md`
│      ↓           │
│ E. Claude review │ ← Claude (see "Review checklist")
│      ↓           │
│ F. Decision      │ ← clean / iterate / escalate (see "Escalation triggers")
│      ↓           │
│ G. Phase merge   │ ← ff-merge sub-branch into thunder-policy + worklog entry
│                  │
└── next phase ────┘
```

## Plan format (Claude → file)

Use `superpowers:writing-plans` TDD-step format, identical to P0's `2026-04-30-thunder-p0-route-messages-passthrough.md`. Save to:

```
docs/superpowers/plans/YYYY-MM-DD-thunder-pN-<feature-slug>.md
```

Required sections per plan:

1. **Header** — Goal / Architecture / Tech Stack
2. **Context** — spec section anchors + key file:line references
3. **Pre-flight verification** — worktree, branch, baseline build
4. **File Structure** — what gets touched, what's deliberately not
5. **Task N: ...** blocks with `- [ ]` checklist steps:
   - Write failing test (with full code)
   - Run test, see fail (expected output)
   - Implement (with full code)
   - Run test, see pass
   - Build/clippy
   - Commit (with exact commit message)
6. **Phase exit criteria** — cargo build/test/clippy + e2e + spec citations + xref
7. **Self-review notes** — spec coverage, type consistency, placeholder scan

The plan must be executable by an actor that has zero conversation context. Codex doesn't have the brainstorm history; it has the plan + brief only.

## Branch strategy

Trunk: `thunder-policy` (linear, signed-off phases only).

Per phase:

```bash
# At phase start (Claude does this)
cd /home/hkang/wl/smg-wl
git checkout thunder-policy
git checkout -b thunder-policy-pN

# Codex commits land on thunder-policy-pN

# At phase end after Claude review passes:
git checkout thunder-policy
git merge --ff-only thunder-policy-pN
git branch -d thunder-policy-pN

# If review fails irrecoverably:
git branch -D thunder-policy-pN  # destructive, only after explicit user OK
```

**Why ff-only**: keeps `thunder-policy` linear; if a phase requires merge commits it's a signal the trunk advanced (shouldn't happen in serial execution) and we want the failure loud.

## Codex brief template

Each phase gets a single brief file at `/tmp/codex-brief-pN.md` (not committed, regenerable). Required sections:

```markdown
# Codex Execution Brief: Thunder Phase N

## Identity
You are Codex CLI (gpt-5.5 high) executing a pre-written plan. You implement code; you do NOT make design decisions. When in doubt, write `OPEN_QUESTION: ...` in your round summary and stop.

## Worktree
- Path: /home/hkang/wl/smg-wl
- Branch: thunder-policy-pN (already created and checked out)
- Do NOT touch /home/hkang/wl/smg_thunder/ (abandoned worktree).

## Plan
Read this file END-TO-END before starting:
docs/superpowers/plans/YYYY-MM-DD-thunder-pN-*.md

Follow the `- [ ]` steps in order. Do NOT skip ahead. Do NOT batch commits.

## Critical commands
- Cargo package name is `smg`, not `model_gateway` (the latter is the directory).
- Workspace builds: `cargo build --workspace` (≈5min cold cache after openssl).
- Per-package: `cargo build/test/clippy -p smg` or `-p openai-protocol`.
- e2e venv: source /home/hkang/wl/smg-wl/e2e_test/.venv/bin/activate
- e2e pytest: `pytest e2e_test/thunder/<test>.py -v --rootdir=e2e_test/thunder --confcutdir=e2e_test/thunder`

## Autonomy boundaries
You MAY NOT, without writing OPEN_QUESTION and stopping:
1. Make any choice that contradicts a D-1..D-N decision in docs/thunder/02-decisions.md or docs/thunder/worklog.md.
2. Change a public trait signature beyond what the plan instructs.
3. Modify any file in 10-phases.md's "Explicitly NOT touched" list.
4. Rename or change semantics of `--policy thunder` CLI flag.
5. Use `unwrap()`, `expect()`, or `unsafe { ... }` to silence clippy. The workspace has `unwrap_used` denied; if a value is genuinely Option/Result, propagate with `?` or pattern match.
6. Skip any e2e test in the plan because "setup is too involved". If the e2e environment isn't ready, document the failure and stop.

## Reporting
After each task in the plan:
- Write a one-line summary to /tmp/codex-progress-pN.md (append).
- Commit with the exact message in the plan.

After the entire plan:
- Write a final report to /tmp/codex-report-pN.md including:
  - List of commits (`git log --oneline thunder-policy..HEAD`)
  - cargo build/test/clippy results (last 5 lines each)
  - pytest result (full summary line)
  - Any OPEN_QUESTION raised (verbatim)
  - Any deviation from the plan, with rationale

## Now begin.
```

## Codex invocation

```bash
cd /home/hkang/wl/smg-wl
codex exec --model gpt-5.5 --reasoning-effort high \
    --working-dir /home/hkang/wl/smg-wl \
    < /tmp/codex-brief-pN.md \
    2>&1 | tee /tmp/codex-output-pN.log
```

The `tee` to a log file lets Claude grep through Codex's reasoning trace afterward without re-running.

## Review checklist (Claude → user)

After Codex returns, before any merge, Claude verifies:

| # | Check | Pass criterion |
|---|---|---|
| R1 | Branch sanity | HEAD on `thunder-policy-pN`, parent matches expected `thunder-policy` HEAD |
| R2 | Plan coverage | Every `- [ ]` checkbox in the plan has a corresponding commit or explicit deferral note |
| R3 | Commit message format | `feat/test/fix/docs/style(scope): summary (Phase N)` for every commit |
| R4 | No forbidden file touched | `git diff --stat thunder-policy..HEAD` shows zero entries from 10-phases.md "NOT touched" list |
| R5 | No forbidden idiom | `git diff thunder-policy..HEAD` shows no `unwrap()`, `expect(`, `unsafe ` introductions |
| R6 | cargo build green | `cargo build --workspace` exits 0 |
| R7 | cargo test green | `cargo test --workspace` exits 0 (pre-existing flakes called out + ignored if same as last phase's baseline) |
| R8 | clippy green | `cargo clippy --all-targets --all-features -- -D warnings` exits 0 |
| R9 | e2e green | `pytest e2e_test/thunder/test_phaseN_*.py -v --rootdir... --confcutdir...` all pass |
| R10 | xref clean | `bash scripts/check_thunder_xref.sh` shows `[OK]` (WARNs OK) |
| R11 | Spec compliance | For each AC in plan, manually trace to commit; for each D-N decision touched, manually re-read the entry to confirm the implementation matches the "Chosen design" section |
| R12 | OPEN_QUESTION review | If `/tmp/codex-report-pN.md` has any OPEN_QUESTION lines, those are escalated to user before proceeding |

R11 is where Claude review provides the most leverage; it's what Codex literally cannot do (no conversation context with user, no deep familiarity with the worklog rationale).

## Decision matrix at Step F

| Findings | Action |
|---|---|
| 0 findings | ✅ Merge phase, write worklog entry, advance to next phase |
| Only [P3-P9] minor (nits, doc typos, missing test edge case) | Compile findings into a fix-brief, re-invoke `codex exec` for an iteration round (max 3 iterations) |
| ≥1 [P0-P2] finding (semantic deviation, broken invariant, missing AC) | If fix is mechanical and Claude is confident: write fix-brief and iterate. If fix requires design judgment: escalate to user |
| Same finding survives 3 iteration rounds | Escalate to user (likely plan defect or spec ambiguity, not Codex bug) |
| OPEN_QUESTION in Codex report | Escalate immediately, do NOT iterate |
| Codex modified plan or worflow.md | Reject, restore from trunk, escalate |

## State files (per phase, in `/tmp`, not committed)

- `/tmp/codex-brief-pN.md` — Claude's brief for Codex
- `/tmp/codex-progress-pN.md` — Codex's per-task append-only log
- `/tmp/codex-report-pN.md` — Codex's final report
- `/tmp/codex-output-pN.log` — full stdout/stderr from `codex exec`
- `/tmp/claude-review-pN.md` — Claude's review findings (R1-R12 + decision)
- `/tmp/codex-fix-brief-pN-iterM.md` — fix briefs for iteration rounds (M = 1, 2, 3)

These are intentionally not committed: they're transient state, regeneratable from the plan + git history. If you need them archived, `cp /tmp/codex-{brief,report}-pN.md docs/thunder/codex-archive/pN/` at phase merge time.

## Worklog entry per phase

After successful merge, Claude appends a `D-(N+M)` entry to `worklog.md` with:

- **Date**: actual merge date
- **Spec ref**: phase row in 10-phases.md
- **What landed**: the commit hashes + LOC
- **What did NOT change**: the deliberately-not-touched files (re-cite from 10-phases.md)
- **Footguns surfaced**: anything new (like P0's `/v1/models` discovery requirement)
- **Revisit conditions**: when a future phase or production observation should reopen the design
- **Approved by**: `(pending user review)` until user confirms

User then signs off the worklog entry as a separate small commit.

## Cancellation / abort

If Codex execution must be aborted mid-flight (e.g., hung, runaway tokens):

```bash
# Find PID of codex exec
pgrep -f 'codex exec' | head
kill <pid>

# Rollback the sub-branch
cd /home/hkang/wl/smg-wl
git checkout thunder-policy
git branch -D thunder-policy-pN
```

The trunk `thunder-policy` is never affected; aborting always discards Codex's work.

## Open Questions to user template

When Claude needs user input, format as:

```markdown
# Phase N — Open Question for Weili

**Context**: <1-2 sentences>
**Codex's choice**: <what it did or proposed>
**Why it might be wrong**: <which D-N or spec section is in tension>
**Options**:
  A. ...
  B. ...
  C. ...
**Claude's recommendation**: <X> because <reason>
**Cost of getting it wrong**: <impact + when we'd notice>
```

This format is concise, gives the user enough to decide quickly, and signals that Claude already did the research instead of just punting.
