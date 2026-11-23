# Inbox Auto-Triage

Inbox auto-triage classifies and routes control-plane inbox items (escalations, blocked/failed tasks, failed runs, quota pauses, bounced merges, open inter-agent messages, and goals awaiting human review) so routine operational work is owned and drained automatically while only genuine human decisions surface to the operator.

Inbox auto-triage is **OFF by default** (`inbox.mode: "off"`). Upgrading or running a daemon without opting in creates no triage tables, writes nothing to SQLite, and keeps `work.snapshot` byte-identical to the legacy projection.

---

## Routine vs Human Blocker

- **Human blockers** (surfaced in `work.snapshot.inbox` with `ask`, `blockedWork`, and `sourceRefs`, notified once):
  - **R02**: Unresolved `P0` escalations (blocking or nonblocking), except `verification_failed` P0s proven to belong to the current goal review cycle (which fold under the goal's ask). Independent asks in the same run stay separate entries.
  - **R10 (`restartRecovery: "handoff"`, default)**: A daemon-restart casualty (goal + failed run + interrupted tasks) folded into one single ask: resume in place or cancel.
  - **R15**: Goals in `awaiting_human` with a review verdict, deterministic gate failure, circuit breaker, or `verificationMode: "none"`.
  - **R21**: Manual quota pauses (`mode: "manual"`), or `paused_quota` runs whose in-memory quota scheduler state remains missing past `recoverMaxSec`.
  - **Coalesced `scope_decision` ask**: At most one scope-level ask per repository when one or more routine causes are `parked` (e.g., after batch remediation/undetermined findings or exhausting the 3-attempt lifetime budget). Repeated failures never create N separate human blockers.

- **Routine items** (tracked in `work.snapshot.inboxAuto`, zero Director chat wakes or routine human notifications):
  - **R00**: Items on runs whose linked goals are **all** terminal (`achieved` or `cancelled`) — reconciles settled-goal escalations, expires open messages, or marks ledger supersession.
  - **R03 / R04 / R05**: `P1`/`P2` escalations owned by their live addressee, steered to a live ancestor/Tier 1 (rung A), or investigated by a bounded read-only batch task (rung B).
  - **R06 / R07 / R08**: Blocked tasks derived under an open blocking escalation (`R06`), resumed once at the exact expected generation when the winning `escalation.resolve` granted `continue: true` (`R07`), held when answered without `continue` (`R07`), or sent to ladder A/B for replanning (`R08`).
  - **R09**: Failed tasks in an active run owned by their live parent, else ladder A/B.
  - **R10 (`restartRecovery: "resume"`)**: Fenced in-place recovery of positively identified restart-interrupted runs (never starts a fresh run).
  - **R11 / R12 / R13 / R14x**: Failed runs derived under blocking escalations (`R11`) or diagnosed via rung B (`R12`); goals waiting solely on a failed linked run (`R13`); completed runs missing a review verdict (`R14x`, diagnosed via rung B — never auto-verified or auto-accepted).
  - **R16 / R17**: Bounced merges proof-resolved when a later `merged` row exists on the exact same `(run_id, task_id, worktree_id, branch)` (`R16`), else owned by the live submitter or ladder A/B (`R17`).
  - **R18 / R19**: Open messages expired via guarded `expireMessage` when recipient/sender is durably terminal (`completed`/`failed`/`cancelled`) or the run settled (`R18`), else owned by the live recipient or ladder A/B (`R19`).
  - **R20**: Quota pauses under `auto_timer` or `user_timer` (held under the existing quota owner; user timers are never advanced).
  - **Held (vetoes)**: Routine items on operator-stopped runs (`R01`), paused goals, or quota-paused runs are held without action; independent human asks on those runs still surface.

---

## Acknowledgment vs Resolution

- **Acknowledgment** creates a durable cause ticket (`inbox_triage_cause`, keyed by `<origin_kind>:<origin_source_id>`) naming the matched rule, disposition, owner or hold reason, `nextAction`, and `nextCheckAt`. Acknowledgment hides nothing on its own and closes nothing.
- **Resolution** occurs only when the source row reaches durable truth (answered, resolved, expired, or superseded by verifiable SQLite proof) and the ledger records the evidence (`outcome_kind`, `outcome`). Resolution is never inferred from age, silence, projection windowing, or having sent a steer.

---

## Bounds

These bounds hold while the daemon is up with `inbox.mode: "on"` and scheduler authority held (no bound is claimed on human response times, external executors, or outages):

| Measure | Bound | Mechanism |
| --- | --- | --- |
| Observation cadence | First tick ≤ 5 s after startup reconciliation, then fixed 30 s | Unref'd scheduler + 250 ms debounced event nudges (never postpone tick) |
| Acknowledgment (new items and legacy backlog) | ≤ 60 s | Synchronous unwindowed SQLite control pass (zero awaits) |
| Human blocker surfaced & notified once | ≤ 60 s | Single transaction (`inbox_triage_ask.notified_at` + journal), mirrored post-commit |
| Owner loss detection (death, generation drift, steer refusal) | ≤ 60 s from observable loss | Per-task liveness health check on every owned cause every pass |
| Silent owner lease / assignment cap | 5 min lease, ≤ 3 no-progress renewals, 20 min absolute cap per assignment | `lease_expires_at` + `assignment_expires_at`; only cause-specific events count as progress |
| Stale recovery (owner loss or restart) | ≤ 120 s: safe action issued or cause durably assigned | Control-pass assignment + action pass |
| Retries & backoff | Max 3 counted dispatches per cause lifetime (across restarts/epochs); spacing ≥ 1 min, ≥ 2 min, then 4 min grace | `cause.attempts`, `next_check_at`; nudges never shorten backoff |
| Hard-failure cutoff | ≤ 9 min from first dispatch (+ ≤ 30 s jitter) | `absolute_deadline_at` |
| Silent-path cutoff | ≤ 69 min from first assignment (3 × 20 + 7 + 2 min) | `absolute_deadline_at` → `parked` + single `scope_decision` ask |
| Clear after answer or supersession | ≤ 60 s | Control-pass source-truth check |

---

## Configuration & Rollout

All keys live under the `inbox` section of global `~/.pi-harness/config.json`. For safety, **`inbox.mode` and `inbox.schedulerHost` are global-only** (stripped from repository `.pi-harness.json`).

| Key | Default | Description |
| --- | --- | --- |
| `inbox.mode` | `"off"` | `"off"` (zero writes/DDL, legacy snapshot), `"report"` (read-only plan via RPC, zero writes/DDL), or `"on"` (active triage). |
| `inbox.schedulerHost` | `null` | Exact `os.hostname()` authorized to run active triage. Authority requires `mode === "on"` **and** `schedulerHost === os.hostname()`. A copied DB/config on another host stays disarmed. |
| `inbox.restartRecovery` | `"handoff"` | Policy for `daemon_restart` casualties: `"handoff"` (single human ask) or `"resume"` (fenced in-place recovery). |
| `inbox.sweepIntervalSec` | `30` | Periodic sweep interval in seconds. |
| `inbox.ackMaxSec` | `60` | Upper bound in seconds for acknowledging items. |
| `inbox.lostOwnerDetectSec` | `60` | Upper bound in seconds for detecting lost task owners. |
| `inbox.recoverMaxSec` | `120` | Upper bound in seconds for recovery/reassignment and stale intent reconciliation. |
| `inbox.maxAutoAttempts` | `3` | Lifetime counted dispatch cap per cause (reset only via `inbox.triage.rearm`). |
| `inbox.backoffMin` | `[1, 2, 4]` | Retry spacing and final grace window in minutes. |
| `inbox.leaseMin` | `5` | Owner lease duration in minutes. |
| `inbox.maxNoProgressRenewals` | `3` | Consecutive lease renewals allowed without cause-specific progress. |
| `inbox.maxDispatchPerTick` | `2` | Max expensive dispatches (resumes, steers, batch launches) per sweep tick. |
| `inbox.batchMaxItems` | `20` | Max causes attached to a single rung-B batch investigation task. |
| `inbox.batchModel` | `null` | Model for rung-B read-only investigation batches (rung B is disabled and falls through to `parked` when both `batchModel` and `batchProfileId` are unset). |
| `inbox.batchProfileId` | `null` | Optional runner profile ID for rung-B batches. |
| `inbox.batchMaxTurns` | `15` | Advisory max-turns hint for rung-B batch tasks. |

### Enabling Safely

1. Preview without writes by setting `"mode": "report"` and calling `inbox.triage.report`.
2. Activate on one host by setting both `"mode": "on"` and `"schedulerHost": "<exact os.hostname()>"` in global `config.json` (or via `config.update` RPC).
3. Disarm immediately by setting `"mode": "off"` or clearing `"schedulerHost"`; `applyConfig` disarms synchronously and cancels pending timers/activations.

---

## `restartRecovery` Policy

- **`"handoff"` (default)**: Preserves the existing park-on-restart behavior while folding the goal (`awaiting_human`), failed run (`cause: "daemon_restart"`), and interrupted tasks into **one** human ask (`run-M of goal-N was interrupted by a daemon restart: resume in place | cancel`). Nothing is resumed automatically.
- **`"resume"`**: Performs fenced in-place recovery counted against the cause's lifetime budget of 3, only when the target is positively identified (current linked run of exactly one non-terminal, non-paused goal, or a standalone run never linked to any goal) with no veto active. If same-run activation cannot be fenced, triage fails closed and never starts a fresh run. After exhausting the 3-attempt budget, triage surfaces the same single handoff ask.

---

## Daemon RPCs

- **`inbox.triage.report`** `{ repoRoot?, chatSessionId?, runId? }`: Pure, zero-write routing plan across all unwindowed source items. Works in `"off"`, `"report"`, and `"on"` modes; never triggers DDL.
- **`inbox.triage.list`** `{ state?, runId?, causeId?, repoRoot?, limit?, journal?, journalLimit? }`: Read-only query over `inbox_triage_cause` (and optionally `inbox_triage_journal`). Returns empty lists without triggering DDL if triage has never been activated.
- **`inbox.triage.rearm`** `{ causeId, reason? }`: Operator action that grants a fresh lifetime attempt/deadline budget on an unresolved cause and returns it to `acknowledged` (requires active authority).
- **`escalation.resolve`** `{ escalationId, reason, chatSessionId?, continue? }`: Passing `continue: true` on the **winning** resolution transition records a single-use continuation grant bound to that exact resolution event sequence and the proven blocked task's `(taskId, generation)`. Reopening the escalation revokes the grant; replaying `escalation.resolve` grants nothing; resolving without `continue: true` leaves the blocked task held (`answered; awaiting explicit resume`).

---

## What Triage Never Does

- Never accepts or verifies a goal (`goal.accept` / `goal.review`) or modifies goal verdicts, criteria, or budgets.
- Never resolves an escalation on its own judgment (only invokes the existing `reconcileSettledGoalEscalations` when **all** linked goals are already achieved or cancelled).
- Never writes escalation status `'acked'` (`ackEscalation(..., false)`).
- Never answers a message on an agent's behalf (only expires definitively obsolete messages via guarded `expireMessage`).
- Never overrides operator stops (`stopped`), paused/terminal goals, quota pauses, or user-scheduled quota timers.
- Never replays refused or ambiguous steers, never starts a fresh run during restart recovery, and never triages its own batch investigation tasks.
- Never deletes or rewrites source rows or closes items purely by age.
