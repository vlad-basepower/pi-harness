import type { Database } from "bun:sqlite";
import type {
  EntityRef,
  InboxItem,
  InboxTriageDisposition,
  InboxTriageOwnerKind,
  InboxTriageState,
} from "../protocol/daemon-rpc.js";

export type CauseDisposition = Exclude<InboxTriageDisposition, "derived">;
export type CauseOutcomeKind = "source_resolved" | "proof" | "answered" | "superseded";
export type ItemRole = "primary" | "derived";
export type EffectStatus = "intent" | "dispatched" | "confirmed" | "failed" | "ambiguous";
export type AssignmentKind = "steer" | "batch";
export type AssignmentStatus = "open" | "returned" | "expired" | "failed";
export type AskKind = "blocker" | "scope_decision";
export type AskClearKind = "answered" | "superseded";

export type TriageJournalType =
  | "acked"
  | "owned"
  | "held"
  | "effect_intent"
  | "effect_receipt"
  | "owner_lost"
  | "lease_renewed"
  | "assigned"
  | "batch_returned"
  | "parked"
  | "surfaced"
  | "notified"
  | "cleared"
  | "resolved"
  | "reopened"
  | "rearmed"
  | "continuation_recorded";

export interface TriageCauseRecord {
  causeId: string;
  originKind: InboxItem["kind"];
  originSourceId: string;
  runId: string | null;
  goalId: string | null;
  repoRoot: string | null;
  rule: string;
  disposition: CauseDisposition;
  state: InboxTriageState;
  revision: number;
  ownerKind: InboxTriageOwnerKind | null;
  ownerId: string | null;
  ownerEpoch: string | null;
  assignmentId: string | null;
  nextAction: string;
  nextCheckAt: number | null;
  leaseExpiresAt: number | null;
  assignmentExpiresAt: number | null;
  lastProgressSeq: number;
  noProgressRenewals: number;
  attempts: number;
  firstDispatchAt: number | null;
  firstAssignedAt: number | null;
  absoluteDeadlineAt: number | null;
  holdReason: string | null;
  askId: string | null;
  outcomeKind: CauseOutcomeKind | null;
  outcome: string | null;
  missingSince: number | null;
  lastActionResult: string | null;
  ackedAt: number;
  resolvedAt: number | null;
  updatedAt: number;
}

export interface TriageItemRecord {
  itemKind: InboxItem["kind"];
  sourceId: string;
  sourceEpoch: string;
  causeId: string;
  role: ItemRole;
  firstSeenAt: number;
  lastSeenAt: number;
  closedAt: number | null;
  closeKind: string | null;
}

export interface TriageEffectRecord {
  effectKey: string;
  causeId: string;
  attempt: number;
  primitive: string;
  targetKind: string;
  targetId: string;
  targetEpoch: string;
  status: EffectStatus;
  intentAt: number;
  dispatchedAt: number | null;
  receipt: string | null;
  receiptAt: number | null;
}

export interface TriageAssignmentRecord {
  assignmentId: string;
  scope: string;
  kind: AssignmentKind;
  ownerTaskId: string | null;
  ownerGeneration: number | null;
  status: AssignmentStatus;
  createdAt: number;
  expiresAt: number;
  resultJson: string | null;
}

export interface TriageAskRecord {
  askId: string;
  causeId: string | null;
  scope: string | null;
  kind: AskKind;
  ask: string;
  blockedWork: string;
  sourceRefs: EntityRef[];
  createdAt: number;
  notifiedAt: number | null;
  clearedAt: number | null;
  clearKind: AskClearKind | null;
}

export interface TriageJournalRecord {
  seq: number;
  at: number;
  causeId: string;
  itemKey: string | null;
  runId: string | null;
  goalId: string | null;
  type: TriageJournalType;
  payload: unknown;
}

export interface TriageContinuationRecord {
  escalationId: string;
  recordedAt: number;
  consumedAt: number | null;
  consumedTaskId: string | null;
  consumedGeneration: number | null;
}

export interface AckCauseInput {
  causeId: string;
  originKind: InboxItem["kind"];
  originSourceId: string;
  runId: string | null;
  goalId: string | null;
  repoRoot: string | null;
  rule: string;
  disposition: CauseDisposition;
  state?: InboxTriageState;
  ownerKind?: InboxTriageOwnerKind | null;
  ownerId?: string | null;
  ownerEpoch?: string | null;
  assignmentId?: string | null;
  nextAction: string;
  nextCheckAt?: number | null;
  leaseExpiresAt?: number | null;
  assignmentExpiresAt?: number | null;
  lastProgressSeq?: number;
  firstAssignedAt?: number | null;
  absoluteDeadlineAt?: number | null;
  holdReason?: string | null;
  missingSince?: number | null;
  itemKey?: string | null;
}

export interface SetOwnerInput {
  causeId: string;
  expectedRevision: number;
  rule?: string;
  disposition?: CauseDisposition;
  ownerKind: InboxTriageOwnerKind;
  ownerId: string | null;
  ownerEpoch: string | null;
  assignmentId?: string | null;
  nextAction: string;
  nextCheckAt: number | null;
  leaseExpiresAt: number | null;
  assignmentExpiresAt: number | null;
  lastProgressSeq?: number;
  firstAssignedAt?: number | null;
  absoluteDeadlineAt?: number | null;
  itemKey?: string | null;
}

export interface ClaimEffectIntentInput {
  causeId: string;
  expectedRevision: number;
  maxAutoAttempts: number;
  primitive: string;
  targetKind: string;
  targetId: string;
  targetEpoch: string;
  nextCheckAt: number;
  absoluteDeadlineAt?: number | null;
  allowBypassDueCheck?: boolean;
  /** True only for supersession/proof settlement rules (R00/R16) that close surfaced/held/parked causes. */
  allowAnyOpenState?: boolean;
  itemKey?: string | null;
}

export interface EffectCausePatch {
  expectedRevision: number;
  state?: Exclude<InboxTriageState, "resolved" | "surfaced">;
  rule?: string;
  disposition?: CauseDisposition;
  ownerKind?: InboxTriageOwnerKind | null;
  ownerId?: string | null;
  ownerEpoch?: string | null;
  assignmentId?: string | null;
  nextAction?: string;
  nextCheckAt?: number | null;
  leaseExpiresAt?: number | null;
  assignmentExpiresAt?: number | null;
  lastProgressSeq?: number;
  noProgressRenewals?: number;
  firstAssignedAt?: number | null;
  absoluteDeadlineAt?: number | null;
  holdReason?: string | null;
  lastActionResult?: string | null;
}

export const INBOX_TRIAGE_TABLES = [
  "inbox_triage_cause",
  "inbox_triage_item",
  "inbox_triage_effect",
  "inbox_triage_assignment",
  "inbox_triage_assignment_member",
  "inbox_triage_ask",
  "inbox_triage_journal",
  "inbox_triage_continuation",
] as const;

const INBOX_TRIAGE_SCHEMA = `
CREATE TABLE IF NOT EXISTS inbox_triage_cause (
  cause_id TEXT PRIMARY KEY,
  origin_kind TEXT NOT NULL,
  origin_source_id TEXT NOT NULL,
  run_id TEXT,
  goal_id TEXT,
  repo_root TEXT,
  rule TEXT NOT NULL,
  disposition TEXT NOT NULL CHECK (disposition IN ('routine_auto','routine_owned','human_blocker','held')),
  state TEXT NOT NULL CHECK (state IN ('acknowledged','owned','held','surfaced','parked','resolved')),
  revision INTEGER NOT NULL DEFAULT 1,
  owner_kind TEXT CHECK (owner_kind IN ('task','batch','daemon','human')),
  owner_id TEXT,
  owner_epoch TEXT,
  assignment_id TEXT,
  next_action TEXT NOT NULL,
  next_check_at INTEGER,
  lease_expires_at INTEGER,
  assignment_expires_at INTEGER,
  last_progress_seq INTEGER NOT NULL DEFAULT 0,
  no_progress_renewals INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  first_dispatch_at INTEGER,
  first_assigned_at INTEGER,
  absolute_deadline_at INTEGER,
  hold_reason TEXT,
  ask_id TEXT,
  outcome_kind TEXT CHECK (outcome_kind IN ('source_resolved','proof','answered','superseded')),
  outcome TEXT,
  missing_since INTEGER,
  last_action_result TEXT,
  acked_at INTEGER NOT NULL,
  resolved_at INTEGER,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_it_cause_state_due ON inbox_triage_cause(state, next_check_at);
CREATE INDEX IF NOT EXISTS idx_it_cause_run ON inbox_triage_cause(run_id, state);
CREATE INDEX IF NOT EXISTS idx_it_cause_owner ON inbox_triage_cause(state, owner_kind, owner_id);
CREATE INDEX IF NOT EXISTS idx_it_cause_repo ON inbox_triage_cause(repo_root, state);

CREATE TABLE IF NOT EXISTS inbox_triage_item (
  item_kind TEXT NOT NULL,
  source_id TEXT NOT NULL,
  source_epoch TEXT NOT NULL DEFAULT '',
  cause_id TEXT NOT NULL REFERENCES inbox_triage_cause(cause_id),
  role TEXT NOT NULL CHECK (role IN ('primary','derived')),
  first_seen_at INTEGER NOT NULL,
  last_seen_at INTEGER NOT NULL,
  closed_at INTEGER,
  close_kind TEXT,
  PRIMARY KEY (item_kind, source_id, source_epoch)
);
CREATE INDEX IF NOT EXISTS idx_it_item_cause ON inbox_triage_item(cause_id, closed_at);

CREATE TABLE IF NOT EXISTS inbox_triage_effect (
  effect_key TEXT PRIMARY KEY,
  cause_id TEXT NOT NULL REFERENCES inbox_triage_cause(cause_id),
  attempt INTEGER NOT NULL,
  primitive TEXT NOT NULL,
  target_kind TEXT NOT NULL,
  target_id TEXT NOT NULL,
  target_epoch TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL CHECK (status IN ('intent','dispatched','confirmed','failed','ambiguous')),
  intent_at INTEGER NOT NULL,
  dispatched_at INTEGER,
  receipt TEXT,
  receipt_at INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_it_effect_active_target
  ON inbox_triage_effect(target_kind, target_id)
  WHERE status IN ('intent','dispatched');
CREATE INDEX IF NOT EXISTS idx_it_effect_cause ON inbox_triage_effect(cause_id, attempt);

CREATE TABLE IF NOT EXISTS inbox_triage_assignment (
  assignment_id TEXT PRIMARY KEY,
  scope TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('steer','batch')),
  owner_task_id TEXT,
  owner_generation INTEGER,
  status TEXT NOT NULL CHECK (status IN ('open','returned','expired','failed')),
  created_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  result_json TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_it_assignment_open_batch_scope
  ON inbox_triage_assignment(scope)
  WHERE kind = 'batch' AND status = 'open';
CREATE INDEX IF NOT EXISTS idx_it_assignment_status ON inbox_triage_assignment(status, expires_at);

CREATE TABLE IF NOT EXISTS inbox_triage_assignment_member (
  assignment_id TEXT NOT NULL REFERENCES inbox_triage_assignment(assignment_id),
  cause_id TEXT NOT NULL REFERENCES inbox_triage_cause(cause_id),
  PRIMARY KEY (assignment_id, cause_id)
);
CREATE INDEX IF NOT EXISTS idx_it_assign_member_cause ON inbox_triage_assignment_member(cause_id);

CREATE TABLE IF NOT EXISTS inbox_triage_ask (
  ask_id TEXT PRIMARY KEY,
  cause_id TEXT,
  scope TEXT,
  kind TEXT NOT NULL CHECK (kind IN ('blocker','scope_decision')),
  ask TEXT NOT NULL,
  blocked_work TEXT NOT NULL,
  source_refs_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  notified_at INTEGER,
  cleared_at INTEGER,
  clear_kind TEXT CHECK (clear_kind IN ('answered','superseded'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_it_ask_active_blocker
  ON inbox_triage_ask(cause_id)
  WHERE kind = 'blocker' AND cleared_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_it_ask_active_scope
  ON inbox_triage_ask(scope)
  WHERE kind = 'scope_decision' AND cleared_at IS NULL;

CREATE TABLE IF NOT EXISTS inbox_triage_journal (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  at INTEGER NOT NULL,
  cause_id TEXT NOT NULL,
  item_key TEXT,
  run_id TEXT,
  goal_id TEXT,
  type TEXT NOT NULL,
  payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_it_journal_cause ON inbox_triage_journal(cause_id, seq);
CREATE INDEX IF NOT EXISTS idx_it_journal_run ON inbox_triage_journal(run_id, seq);

CREATE TABLE IF NOT EXISTS inbox_triage_continuation (
  escalation_id TEXT PRIMARY KEY,
  recorded_at INTEGER NOT NULL,
  consumed_at INTEGER,
  consumed_task_id TEXT,
  consumed_generation INTEGER
);
`;

function isUniqueConstraintError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const code = (err as { code?: string }).code ?? "";
  return code.includes("SQLITE_CONSTRAINT") || /UNIQUE constraint failed/i.test(err.message);
}

function parseJsonSafe<T>(raw: unknown, fallback: T): T {
  if (typeof raw !== "string") return fallback;
  try {
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

function mapCause(r: Record<string, unknown>): TriageCauseRecord {
  return {
    causeId: String(r.cause_id),
    originKind: String(r.origin_kind) as InboxItem["kind"],
    originSourceId: String(r.origin_source_id),
    runId: r.run_id == null ? null : String(r.run_id),
    goalId: r.goal_id == null ? null : String(r.goal_id),
    repoRoot: r.repo_root == null ? null : String(r.repo_root),
    rule: String(r.rule),
    disposition: String(r.disposition) as CauseDisposition,
    state: String(r.state) as InboxTriageState,
    revision: Number(r.revision),
    ownerKind: r.owner_kind == null ? null : (String(r.owner_kind) as InboxTriageOwnerKind),
    ownerId: r.owner_id == null ? null : String(r.owner_id),
    ownerEpoch: r.owner_epoch == null ? null : String(r.owner_epoch),
    assignmentId: r.assignment_id == null ? null : String(r.assignment_id),
    nextAction: String(r.next_action),
    nextCheckAt: r.next_check_at == null ? null : Number(r.next_check_at),
    leaseExpiresAt: r.lease_expires_at == null ? null : Number(r.lease_expires_at),
    assignmentExpiresAt: r.assignment_expires_at == null ? null : Number(r.assignment_expires_at),
    lastProgressSeq: Number(r.last_progress_seq ?? 0),
    noProgressRenewals: Number(r.no_progress_renewals ?? 0),
    attempts: Number(r.attempts ?? 0),
    firstDispatchAt: r.first_dispatch_at == null ? null : Number(r.first_dispatch_at),
    firstAssignedAt: r.first_assigned_at == null ? null : Number(r.first_assigned_at),
    absoluteDeadlineAt: r.absolute_deadline_at == null ? null : Number(r.absolute_deadline_at),
    holdReason: r.hold_reason == null ? null : String(r.hold_reason),
    askId: r.ask_id == null ? null : String(r.ask_id),
    outcomeKind: r.outcome_kind == null ? null : (String(r.outcome_kind) as CauseOutcomeKind),
    outcome: r.outcome == null ? null : String(r.outcome),
    missingSince: r.missing_since == null ? null : Number(r.missing_since),
    lastActionResult: r.last_action_result == null ? null : String(r.last_action_result),
    ackedAt: Number(r.acked_at),
    resolvedAt: r.resolved_at == null ? null : Number(r.resolved_at),
    updatedAt: Number(r.updated_at),
  };
}

function mapItem(r: Record<string, unknown>): TriageItemRecord {
  return {
    itemKind: String(r.item_kind) as InboxItem["kind"],
    sourceId: String(r.source_id),
    sourceEpoch: String(r.source_epoch),
    causeId: String(r.cause_id),
    role: String(r.role) as ItemRole,
    firstSeenAt: Number(r.first_seen_at),
    lastSeenAt: Number(r.last_seen_at),
    closedAt: r.closed_at == null ? null : Number(r.closed_at),
    closeKind: r.close_kind == null ? null : String(r.close_kind),
  };
}

function mapEffect(r: Record<string, unknown>): TriageEffectRecord {
  return {
    effectKey: String(r.effect_key),
    causeId: String(r.cause_id),
    attempt: Number(r.attempt),
    primitive: String(r.primitive),
    targetKind: String(r.target_kind),
    targetId: String(r.target_id),
    targetEpoch: String(r.target_epoch),
    status: String(r.status) as EffectStatus,
    intentAt: Number(r.intent_at),
    dispatchedAt: r.dispatched_at == null ? null : Number(r.dispatched_at),
    receipt: r.receipt == null ? null : String(r.receipt),
    receiptAt: r.receipt_at == null ? null : Number(r.receipt_at),
  };
}

function mapAssignment(r: Record<string, unknown>): TriageAssignmentRecord {
  return {
    assignmentId: String(r.assignment_id),
    scope: String(r.scope),
    kind: String(r.kind) as AssignmentKind,
    ownerTaskId: r.owner_task_id == null ? null : String(r.owner_task_id),
    ownerGeneration: r.owner_generation == null ? null : Number(r.owner_generation),
    status: String(r.status) as AssignmentStatus,
    createdAt: Number(r.created_at),
    expiresAt: Number(r.expires_at),
    resultJson: r.result_json == null ? null : String(r.result_json),
  };
}

function mapAsk(r: Record<string, unknown>): TriageAskRecord {
  return {
    askId: String(r.ask_id),
    causeId: r.cause_id == null ? null : String(r.cause_id),
    scope: r.scope == null ? null : String(r.scope),
    kind: String(r.kind) as AskKind,
    ask: String(r.ask),
    blockedWork: String(r.blocked_work),
    sourceRefs: parseJsonSafe<EntityRef[]>(r.source_refs_json, []),
    createdAt: Number(r.created_at),
    notifiedAt: r.notified_at == null ? null : Number(r.notified_at),
    clearedAt: r.cleared_at == null ? null : Number(r.cleared_at),
    clearKind: r.clear_kind == null ? null : (String(r.clear_kind) as AskClearKind),
  };
}

function mapJournal(r: Record<string, unknown>): TriageJournalRecord {
  return {
    seq: Number(r.seq),
    at: Number(r.at),
    causeId: String(r.cause_id),
    itemKey: r.item_key == null ? null : String(r.item_key),
    runId: r.run_id == null ? null : String(r.run_id),
    goalId: r.goal_id == null ? null : String(r.goal_id),
    type: String(r.type) as TriageJournalType,
    payload: parseJsonSafe<unknown>(r.payload_json, null),
  };
}

function mapContinuation(r: Record<string, unknown>): TriageContinuationRecord {
  return {
    escalationId: String(r.escalation_id),
    recordedAt: Number(r.recorded_at),
    consumedAt: r.consumed_at == null ? null : Number(r.consumed_at),
    consumedTaskId: r.consumed_task_id == null ? null : String(r.consumed_task_id),
    consumedGeneration: r.consumed_generation == null ? null : Number(r.consumed_generation),
  };
}

export function makeCauseId(originKind: InboxItem["kind"], originSourceId: string): string {
  return `${originKind}:${originSourceId}`;
}

export function makeItemKey(itemKind: InboxItem["kind"], sourceId: string, sourceEpoch: string): string {
  return `${itemKind}:${sourceId}:${sourceEpoch}`;
}

export function makeEffectKey(causeId: string, attempt: number): string {
  return `${causeId}#a${attempt}`;
}

/**
 * Durable SQLite ledger & append-only journal for inbox auto-triage.
 *
 * - Lazy DDL (contract P5): constructor executes ZERO SQL. `ensureSchema()` must be called only on
 *   activation in mode `"on"`.
 * - Controlled time: every mutating method takes `now: number` explicitly (no `Date.now()`).
 * - Optimistic CAS: every cause mutation guards on `(cause_id, revision)` and increments `revision`.
 * - Atomic journaling: every transition appends its `inbox_triage_journal` row inside the same
 *   `BEGIN IMMEDIATE` transaction and returns the journal record(s) for post-commit event mirroring.
 */
export class InboxTriageStore {
  private schemaEnsured = false;

  constructor(private readonly db: Database) {}

  /** Returns true iff the triage tables already exist in SQLite (performs zero writes). */
  hasSchema(): boolean {
    if (this.schemaEnsured) return true;
    const row = this.db
      .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'inbox_triage_cause'")
      .get() as { name?: string } | undefined;
    if (row?.name === "inbox_triage_cause") {
      this.schemaEnsured = true;
      return true;
    }
    return false;
  }

  /** Creates triage tables and indexes lazily; call only when activating mode `"on"`. */
  ensureSchema(): void {
    if (this.schemaEnsured) return;
    this.db.exec(INBOX_TRIAGE_SCHEMA);
    this.schemaEnsured = true;
  }

  private tx<T>(fn: () => T): T {
    return this.db.transaction(fn).immediate();
  }

  private appendJournalInTx(
    at: number,
    causeId: string,
    itemKey: string | null,
    runId: string | null,
    goalId: string | null,
    type: TriageJournalType,
    payload: unknown,
  ): TriageJournalRecord {
    const res = this.db
      .prepare(
        `INSERT INTO inbox_triage_journal(at, cause_id, item_key, run_id, goal_id, type, payload_json)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(at, causeId, itemKey, runId, goalId, type, JSON.stringify(payload ?? null));
    return {
      seq: Number(res.lastInsertRowid),
      at,
      causeId,
      itemKey,
      runId,
      goalId,
      type,
      payload: payload ?? null,
    };
  }

  // ---- Cause & Item ack / upsert ----

  ackCause(
    input: AckCauseInput,
    now: number,
  ): { inserted: boolean; cause: TriageCauseRecord; journal: TriageJournalRecord | null } {
    this.ensureSchema();
    return this.tx(() => {
      const state: InboxTriageState = input.state ?? "acknowledged";
      const res = this.db
        .prepare(
          `INSERT OR IGNORE INTO inbox_triage_cause(
             cause_id, origin_kind, origin_source_id, run_id, goal_id, repo_root,
             rule, disposition, state, revision,
             owner_kind, owner_id, owner_epoch, assignment_id,
             next_action, next_check_at, lease_expires_at, assignment_expires_at,
             last_progress_seq, no_progress_renewals, attempts,
             first_dispatch_at, first_assigned_at, absolute_deadline_at,
             hold_reason, ask_id, outcome_kind, outcome, missing_since, last_action_result,
             acked_at, resolved_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, NULL, ?, ?, ?, NULL, NULL, NULL, ?, NULL, ?, NULL, ?)`,
        )
        .run(
          input.causeId,
          input.originKind,
          input.originSourceId,
          input.runId,
          input.goalId,
          input.repoRoot,
          input.rule,
          input.disposition,
          state,
          input.ownerKind ?? null,
          input.ownerId ?? null,
          input.ownerEpoch ?? null,
          input.assignmentId ?? null,
          input.nextAction,
          input.nextCheckAt ?? now,
          input.leaseExpiresAt ?? null,
          input.assignmentExpiresAt ?? null,
          input.lastProgressSeq ?? 0,
          input.firstAssignedAt ?? null,
          input.absoluteDeadlineAt ?? null,
          input.holdReason ?? null,
          input.missingSince ?? null,
          now,
          now,
        );
      const cause = this.getCauseInTx(input.causeId)!;
      if (res.changes === 0) {
        return { inserted: false, cause, journal: null };
      }
      const journal = this.appendJournalInTx(
        now,
        cause.causeId,
        input.itemKey ?? null,
        cause.runId,
        cause.goalId,
        "acked",
        {
          rule: cause.rule,
          disposition: cause.disposition,
          state: cause.state,
          ownerKind: cause.ownerKind,
          ownerId: cause.ownerId,
          nextAction: cause.nextAction,
          nextCheckAt: cause.nextCheckAt,
        },
      );
      return { inserted: true, cause, journal };
    });
  }

  upsertItem(
    input: {
      itemKind: InboxItem["kind"];
      sourceId: string;
      sourceEpoch: string;
      causeId: string;
      role: ItemRole;
    },
    now: number,
  ): { inserted: boolean; item: TriageItemRecord } {
    this.ensureSchema();
    return this.tx(() => {
      const ins = this.db
        .prepare(
          `INSERT OR IGNORE INTO inbox_triage_item(
             item_kind, source_id, source_epoch, cause_id, role, first_seen_at, last_seen_at, closed_at, close_kind
           ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)`,
        )
        .run(input.itemKind, input.sourceId, input.sourceEpoch, input.causeId, input.role, now, now);
      if (ins.changes === 0) {
        this.db
          .prepare(
            `UPDATE inbox_triage_item
             SET last_seen_at = ?, cause_id = ?, role = ?
             WHERE item_kind = ? AND source_id = ? AND source_epoch = ? AND closed_at IS NULL`,
          )
          .run(now, input.causeId, input.role, input.itemKind, input.sourceId, input.sourceEpoch);
      }
      const row = this.db
        .prepare(
          `SELECT * FROM inbox_triage_item WHERE item_kind = ? AND source_id = ? AND source_epoch = ?`,
        )
        .get(input.itemKind, input.sourceId, input.sourceEpoch) as Record<string, unknown>;
      return { inserted: ins.changes === 1, item: mapItem(row) };
    });
  }

  closeItem(
    itemKind: InboxItem["kind"],
    sourceId: string,
    sourceEpoch: string,
    closeKind: string,
    now: number,
  ): boolean {
    this.ensureSchema();
    const res = this.db
      .prepare(
        `UPDATE inbox_triage_item
         SET closed_at = ?, close_kind = ?
         WHERE item_kind = ? AND source_id = ? AND source_epoch = ? AND closed_at IS NULL`,
      )
      .run(now, closeKind, itemKind, sourceId, sourceEpoch);
    return res.changes === 1;
  }

  /**
   * Updates non-terminal classification metadata (rule, disposition, next_action, next_check_at, missing_since,
   * run/goal/repo links) via CAS on `(cause_id, revision)` without altering attempts or budget.
   */
  updateCauseMetadata(
    causeId: string,
    expectedRevision: number,
    patch: {
      rule?: string;
      disposition?: CauseDisposition;
      nextAction?: string;
      nextCheckAt?: number | null;
      runId?: string | null;
      goalId?: string | null;
      repoRoot?: string | null;
      missingSince?: number | null;
      lastActionResult?: string | null;
    },
    now: number,
  ): { updated: boolean; cause: TriageCauseRecord | null } {
    this.ensureSchema();
    return this.tx(() => {
      const current = this.getCauseInTx(causeId);
      if (!current || current.revision !== expectedRevision || current.state === "resolved") {
        return { updated: false, cause: current };
      }
      const res = this.db
        .prepare(
          `UPDATE inbox_triage_cause
           SET rule = ?, disposition = ?, next_action = ?, next_check_at = ?,
               run_id = ?, goal_id = ?, repo_root = ?, missing_since = ?, last_action_result = ?,
               revision = revision + 1, updated_at = ?
           WHERE cause_id = ? AND revision = ? AND state <> 'resolved'`,
        )
        .run(
          patch.rule ?? current.rule,
          patch.disposition ?? current.disposition,
          patch.nextAction ?? current.nextAction,
          patch.nextCheckAt !== undefined ? patch.nextCheckAt : current.nextCheckAt,
          patch.runId !== undefined ? patch.runId : current.runId,
          patch.goalId !== undefined ? patch.goalId : current.goalId,
          patch.repoRoot !== undefined ? patch.repoRoot : current.repoRoot,
          patch.missingSince !== undefined ? patch.missingSince : current.missingSince,
          patch.lastActionResult !== undefined ? patch.lastActionResult : current.lastActionResult,
          now,
          causeId,
          expectedRevision,
        );
      return { updated: res.changes === 1, cause: this.getCauseInTx(causeId) };
    });
  }

  // ---- Ownership, Lease & Owner Loss ----

  setOwner(
    input: SetOwnerInput,
    now: number,
  ): { updated: boolean; cause: TriageCauseRecord | null; journal: TriageJournalRecord | null } {
    this.ensureSchema();
    return this.tx(() => {
      const current = this.getCauseInTx(input.causeId);
      if (!current || current.revision !== input.expectedRevision || current.state === "resolved") {
        return { updated: false, cause: current, journal: null };
      }
      const firstAssignedAt = current.firstAssignedAt ?? input.firstAssignedAt ?? now;
      const absDeadline =
        current.absoluteDeadlineAt !== null
          ? input.absoluteDeadlineAt != null
            ? Math.min(current.absoluteDeadlineAt, input.absoluteDeadlineAt)
            : current.absoluteDeadlineAt
          : (input.absoluteDeadlineAt ?? null);
      const res = this.db
        .prepare(
          `UPDATE inbox_triage_cause
           SET state = 'owned',
               rule = ?,
               disposition = ?,
               owner_kind = ?,
               owner_id = ?,
               owner_epoch = ?,
               assignment_id = ?,
               next_action = ?,
               next_check_at = ?,
               lease_expires_at = ?,
               assignment_expires_at = ?,
               last_progress_seq = ?,
               no_progress_renewals = 0,
               first_assigned_at = ?,
               absolute_deadline_at = ?,
               hold_reason = NULL,
               revision = revision + 1,
               updated_at = ?
           WHERE cause_id = ? AND revision = ? AND state NOT IN ('resolved', 'surfaced')`,
        )
        .run(
          input.rule ?? current.rule,
          input.disposition ?? current.disposition,
          input.ownerKind,
          input.ownerId,
          input.ownerEpoch,
          input.assignmentId ?? current.assignmentId,
          input.nextAction,
          input.nextCheckAt,
          input.leaseExpiresAt,
          input.assignmentExpiresAt,
          input.lastProgressSeq ?? current.lastProgressSeq,
          firstAssignedAt,
          absDeadline,
          now,
          input.causeId,
          input.expectedRevision,
        );
      if (res.changes === 0) {
        return { updated: false, cause: this.getCauseInTx(input.causeId), journal: null };
      }
      const cause = this.getCauseInTx(input.causeId)!;
      const journal = this.appendJournalInTx(
        now,
        cause.causeId,
        input.itemKey ?? null,
        cause.runId,
        cause.goalId,
        "owned",
        {
          ownerKind: cause.ownerKind,
          ownerId: cause.ownerId,
          ownerEpoch: cause.ownerEpoch,
          assignmentId: cause.assignmentId,
          leaseExpiresAt: cause.leaseExpiresAt,
          assignmentExpiresAt: cause.assignmentExpiresAt,
          nextAction: cause.nextAction,
        },
      );
      return { updated: true, cause, journal };
    });
  }

  renewLease(
    input: {
      causeId: string;
      expectedRevision: number;
      expectedOwnerEpoch: string | null;
      leaseExpiresAt: number;
      lastProgressSeq: number;
      noProgressRenewals: number;
      nextCheckAt?: number | null;
      itemKey?: string | null;
    },
    now: number,
  ): { renewed: boolean; cause: TriageCauseRecord | null; journal: TriageJournalRecord | null } {
    this.ensureSchema();
    return this.tx(() => {
      const current = this.getCauseInTx(input.causeId);
      if (
        !current ||
        current.state !== "owned" ||
        current.revision !== input.expectedRevision ||
        current.ownerEpoch !== input.expectedOwnerEpoch
      ) {
        return { renewed: false, cause: current, journal: null };
      }
      const res = this.db
        .prepare(
          `UPDATE inbox_triage_cause
           SET lease_expires_at = ?,
               last_progress_seq = ?,
               no_progress_renewals = ?,
               next_check_at = ?,
               revision = revision + 1,
               updated_at = ?
           WHERE cause_id = ? AND revision = ? AND state = 'owned'`,
        )
        .run(
          input.leaseExpiresAt,
          input.lastProgressSeq,
          input.noProgressRenewals,
          input.nextCheckAt !== undefined ? input.nextCheckAt : current.nextCheckAt,
          now,
          input.causeId,
          input.expectedRevision,
        );
      if (res.changes === 0) {
        return { renewed: false, cause: this.getCauseInTx(input.causeId), journal: null };
      }
      const cause = this.getCauseInTx(input.causeId)!;
      const journal = this.appendJournalInTx(
        now,
        cause.causeId,
        input.itemKey ?? null,
        cause.runId,
        cause.goalId,
        "lease_renewed",
        {
          ownerKind: cause.ownerKind,
          ownerId: cause.ownerId,
          leaseExpiresAt: cause.leaseExpiresAt,
          lastProgressSeq: cause.lastProgressSeq,
          noProgressRenewals: cause.noProgressRenewals,
        },
      );
      return { renewed: true, cause, journal };
    });
  }

  markOwnerLost(
    input: {
      causeId: string;
      expectedRevision: number;
      reason: string;
      nextAction: string;
      nextCheckAt: number;
      itemKey?: string | null;
    },
    now: number,
  ): { updated: boolean; cause: TriageCauseRecord | null; journal: TriageJournalRecord | null } {
    this.ensureSchema();
    return this.tx(() => {
      const current = this.getCauseInTx(input.causeId);
      if (!current || current.revision !== input.expectedRevision || current.state !== "owned") {
        return { updated: false, cause: current, journal: null };
      }
      const prevOwner = {
        ownerKind: current.ownerKind,
        ownerId: current.ownerId,
        ownerEpoch: current.ownerEpoch,
        assignmentId: current.assignmentId,
      };
      const res = this.db
        .prepare(
          `UPDATE inbox_triage_cause
           SET state = 'acknowledged',
               owner_kind = NULL,
               owner_id = NULL,
               owner_epoch = NULL,
               assignment_id = NULL,
               lease_expires_at = NULL,
               assignment_expires_at = NULL,
               no_progress_renewals = 0,
               next_action = ?,
               next_check_at = ?,
               last_action_result = ?,
               revision = revision + 1,
               updated_at = ?
           WHERE cause_id = ? AND revision = ? AND state = 'owned'`,
        )
        .run(input.nextAction, input.nextCheckAt, input.reason, now, input.causeId, input.expectedRevision);
      if (res.changes === 0) {
        return { updated: false, cause: this.getCauseInTx(input.causeId), journal: null };
      }
      const cause = this.getCauseInTx(input.causeId)!;
      const journal = this.appendJournalInTx(
        now,
        cause.causeId,
        input.itemKey ?? null,
        cause.runId,
        cause.goalId,
        "owner_lost",
        { reason: input.reason, ...prevOwner, nextAction: input.nextAction, nextCheckAt: input.nextCheckAt },
      );
      return { updated: true, cause, journal };
    });
  }

  // ---- Hold & Release ----

  holdCause(
    input: {
      causeId: string;
      expectedRevision: number;
      holdReason: string;
      nextAction: string;
      rule?: string;
      ownerKind?: InboxTriageOwnerKind | null;
      ownerId?: string | null;
      nextCheckAt?: number | null;
      itemKey?: string | null;
    },
    now: number,
  ): { updated: boolean; cause: TriageCauseRecord | null; journal: TriageJournalRecord | null } {
    this.ensureSchema();
    return this.tx(() => {
      const current = this.getCauseInTx(input.causeId);
      if (!current || current.revision !== input.expectedRevision || current.state === "resolved") {
        return { updated: false, cause: current, journal: null };
      }
      const res = this.db
        .prepare(
          `UPDATE inbox_triage_cause
           SET state = 'held',
               disposition = 'held',
               rule = ?,
               hold_reason = ?,
               owner_kind = ?,
               owner_id = ?,
               owner_epoch = NULL,
               lease_expires_at = NULL,
               assignment_expires_at = NULL,
               next_action = ?,
               next_check_at = ?,
               revision = revision + 1,
               updated_at = ?
           WHERE cause_id = ? AND revision = ? AND state NOT IN ('resolved', 'surfaced')`,
        )
        .run(
          input.rule ?? current.rule,
          input.holdReason,
          input.ownerKind !== undefined ? input.ownerKind : current.ownerKind,
          input.ownerId !== undefined ? input.ownerId : current.ownerId,
          input.nextAction,
          input.nextCheckAt !== undefined ? input.nextCheckAt : null,
          now,
          input.causeId,
          input.expectedRevision,
        );
      if (res.changes === 0) {
        return { updated: false, cause: this.getCauseInTx(input.causeId), journal: null };
      }
      const cause = this.getCauseInTx(input.causeId)!;
      const journal = this.appendJournalInTx(
        now,
        cause.causeId,
        input.itemKey ?? null,
        cause.runId,
        cause.goalId,
        "held",
        { holdReason: input.holdReason, rule: cause.rule, nextAction: cause.nextAction },
      );
      return { updated: true, cause, journal };
    });
  }

  releaseHold(
    input: {
      causeId: string;
      expectedRevision: number;
      rule: string;
      disposition: CauseDisposition;
      nextAction: string;
      nextCheckAt: number;
      itemKey?: string | null;
    },
    now: number,
  ): { updated: boolean; cause: TriageCauseRecord | null; journal: TriageJournalRecord | null } {
    this.ensureSchema();
    return this.tx(() => {
      const current = this.getCauseInTx(input.causeId);
      if (!current || current.revision !== input.expectedRevision || current.state !== "held") {
        return { updated: false, cause: current, journal: null };
      }
      const prevHoldReason = current.holdReason;
      const res = this.db
        .prepare(
          `UPDATE inbox_triage_cause
           SET state = 'acknowledged',
               disposition = ?,
               rule = ?,
               hold_reason = NULL,
               owner_kind = NULL,
               owner_id = NULL,
               owner_epoch = NULL,
               next_action = ?,
               next_check_at = ?,
               revision = revision + 1,
               updated_at = ?
           WHERE cause_id = ? AND revision = ? AND state = 'held'`,
        )
        .run(
          input.disposition,
          input.rule,
          input.nextAction,
          input.nextCheckAt,
          now,
          input.causeId,
          input.expectedRevision,
        );
      if (res.changes === 0) {
        return { updated: false, cause: this.getCauseInTx(input.causeId), journal: null };
      }
      const cause = this.getCauseInTx(input.causeId)!;
      const journal = this.appendJournalInTx(
        now,
        cause.causeId,
        input.itemKey ?? null,
        cause.runId,
        cause.goalId,
        "acked",
        {
          releasedFromHold: prevHoldReason,
          rule: cause.rule,
          disposition: cause.disposition,
          nextAction: cause.nextAction,
          nextCheckAt: cause.nextCheckAt,
        },
      );
      return { updated: true, cause, journal };
    });
  }

  // ---- Surface + Ask + Notified (one atomic transaction) ----

  surfaceCause(
    input: {
      causeId: string;
      expectedRevision: number;
      rule?: string;
      askId: string;
      ask: string;
      blockedWork: string;
      sourceRefs: EntityRef[];
      nextAction?: string;
      itemKey?: string | null;
    },
    now: number,
  ): {
    surfaced: boolean;
    notified: boolean;
    cause: TriageCauseRecord | null;
    ask: TriageAskRecord | null;
    journals: TriageJournalRecord[];
  } {
    this.ensureSchema();
    return this.tx(() => {
      const current = this.getCauseInTx(input.causeId);
      if (!current || current.revision !== input.expectedRevision || current.state === "resolved") {
        return { surfaced: false, notified: false, cause: current, ask: null, journals: [] };
      }
      const existingAsk = this.getActiveAskForCauseInTx(input.causeId);
      const journals: TriageJournalRecord[] = [];

      if (existingAsk) {
        // Update ask text/blockedWork/sourceRefs in place without re-notifying.
        this.db
          .prepare(
            `UPDATE inbox_triage_ask
             SET ask = ?, blocked_work = ?, source_refs_json = ?
             WHERE ask_id = ?`,
          )
          .run(input.ask, input.blockedWork, JSON.stringify(input.sourceRefs), existingAsk.askId);

        let newlyNotified = false;
        if (existingAsk.notifiedAt === null) {
          const nRes = this.db
            .prepare(`UPDATE inbox_triage_ask SET notified_at = ? WHERE ask_id = ? AND notified_at IS NULL`)
            .run(now, existingAsk.askId);
          newlyNotified = nRes.changes === 1;
        }
        const wasSurfaced = current.state === "surfaced" && current.askId === existingAsk.askId;
        if (!wasSurfaced) {
          const res = this.db
            .prepare(
              `UPDATE inbox_triage_cause
               SET state = 'surfaced',
                   disposition = 'human_blocker',
                   rule = ?,
                   owner_kind = 'human',
                   owner_id = 'operator',
                   owner_epoch = NULL,
                   lease_expires_at = NULL,
                   assignment_expires_at = NULL,
                   hold_reason = NULL,
                   ask_id = ?,
                   next_action = ?,
                   next_check_at = NULL,
                   revision = revision + 1,
                   updated_at = ?
               WHERE cause_id = ? AND revision = ? AND state <> 'resolved'`,
            )
            .run(
              input.rule ?? current.rule,
              existingAsk.askId,
              input.nextAction ?? "awaiting human decision",
              now,
              input.causeId,
              input.expectedRevision,
            );
          if (res.changes === 0) {
            return { surfaced: false, notified: false, cause: this.getCauseInTx(input.causeId), ask: existingAsk, journals: [] };
          }
          const updated = this.getCauseInTx(input.causeId)!;
          journals.push(
            this.appendJournalInTx(now, updated.causeId, input.itemKey ?? null, updated.runId, updated.goalId, "surfaced", {
              askId: existingAsk.askId,
              ask: input.ask,
              blockedWork: input.blockedWork,
              rule: updated.rule,
            }),
          );
        }
        const finalCause = this.getCauseInTx(input.causeId)!;
        const finalAsk = this.getAskInTx(existingAsk.askId)!;
        if (newlyNotified) {
          journals.push(
            this.appendJournalInTx(now, finalCause.causeId, input.itemKey ?? null, finalCause.runId, finalCause.goalId, "notified", {
              askId: finalAsk.askId,
              kind: finalAsk.kind,
              ask: finalAsk.ask,
              blockedWork: finalAsk.blockedWork,
            }),
          );
        }
        return { surfaced: !wasSurfaced, notified: newlyNotified, cause: finalCause, ask: finalAsk, journals };
      }

      // Create new blocker ask with notified_at set in the SAME transaction as surfacing + journal.
      this.db
        .prepare(
          `INSERT INTO inbox_triage_ask(
             ask_id, cause_id, scope, kind, ask, blocked_work, source_refs_json, created_at, notified_at, cleared_at, clear_kind
           ) VALUES (?, ?, ?, 'blocker', ?, ?, ?, ?, ?, NULL, NULL)`,
        )
        .run(
          input.askId,
          input.causeId,
          current.repoRoot ?? "",
          input.ask,
          input.blockedWork,
          JSON.stringify(input.sourceRefs),
          now,
          now,
        );

      const res = this.db
        .prepare(
          `UPDATE inbox_triage_cause
           SET state = 'surfaced',
               disposition = 'human_blocker',
               rule = ?,
               owner_kind = 'human',
               owner_id = 'operator',
               owner_epoch = NULL,
               lease_expires_at = NULL,
               assignment_expires_at = NULL,
               hold_reason = NULL,
               ask_id = ?,
               next_action = ?,
               next_check_at = NULL,
               revision = revision + 1,
               updated_at = ?
           WHERE cause_id = ? AND revision = ? AND state <> 'resolved'`,
        )
        .run(
          input.rule ?? current.rule,
          input.askId,
          input.nextAction ?? "awaiting human decision",
          now,
          input.causeId,
          input.expectedRevision,
        );
      if (res.changes === 0) {
        throw new Error(`CAS failed surfacing cause ${input.causeId}`);
      }
      const cause = this.getCauseInTx(input.causeId)!;
      const ask = this.getAskInTx(input.askId)!;
      journals.push(
        this.appendJournalInTx(now, cause.causeId, input.itemKey ?? null, cause.runId, cause.goalId, "surfaced", {
          askId: ask.askId,
          ask: ask.ask,
          blockedWork: ask.blockedWork,
          rule: cause.rule,
        }),
      );
      journals.push(
        this.appendJournalInTx(now, cause.causeId, input.itemKey ?? null, cause.runId, cause.goalId, "notified", {
          askId: ask.askId,
          kind: ask.kind,
          ask: ask.ask,
          blockedWork: ask.blockedWork,
        }),
      );
      return { surfaced: true, notified: true, cause, ask, journals };
    });
  }

  // ---- Park + Coalesced Scope Decision Ask (notified once, membership updated in place) ----

  parkCause(
    input: {
      causeId: string;
      expectedRevision: number;
      reason: string;
      nextAction: string;
      scope: string;
      scopeAskId: string;
      scopeSourceRefs: EntityRef[];
      scopeParkedCount?: number;
      itemKey?: string | null;
    },
    now: number,
  ): {
    parked: boolean;
    scopeNotified: boolean;
    cause: TriageCauseRecord | null;
    scopeAsk: TriageAskRecord | null;
    journals: TriageJournalRecord[];
  } {
    this.ensureSchema();
    return this.tx(() => {
      const current = this.getCauseInTx(input.causeId);
      if (
        !current ||
        current.revision !== input.expectedRevision ||
        current.state === "resolved" ||
        current.state === "surfaced"
      ) {
        return { parked: false, scopeNotified: false, cause: current, scopeAsk: null, journals: [] };
      }
      const res = this.db
        .prepare(
          `UPDATE inbox_triage_cause
           SET state = 'parked',
               owner_kind = NULL,
               owner_id = NULL,
               owner_epoch = NULL,
               lease_expires_at = NULL,
               assignment_expires_at = NULL,
               next_action = ?,
               next_check_at = NULL,
               last_action_result = ?,
               revision = revision + 1,
               updated_at = ?
           WHERE cause_id = ? AND revision = ? AND state NOT IN ('resolved', 'surfaced')`,
        )
        .run(input.nextAction, input.reason, now, input.causeId, input.expectedRevision);
      if (res.changes === 0) {
        return { parked: false, scopeNotified: false, cause: this.getCauseInTx(input.causeId), scopeAsk: null, journals: [] };
      }

      const cause = this.getCauseInTx(input.causeId)!;
      const journals: TriageJournalRecord[] = [
        this.appendJournalInTx(now, cause.causeId, input.itemKey ?? null, cause.runId, cause.goalId, "parked", {
          reason: input.reason,
          nextAction: input.nextAction,
          assignmentId: cause.assignmentId,
          attempts: cause.attempts,
        }),
      ];

      // Count total currently parked causes for this scope so the coalesced ask text stays exact.
      const countRow = this.db
        .prepare(
          `SELECT COUNT(*) AS cnt FROM inbox_triage_cause WHERE COALESCE(repo_root, '') = ? AND state = 'parked'`,
        )
        .get(input.scope) as { cnt: number };
      const parkedCount = input.scopeParkedCount ?? Math.max(1, Number(countRow?.cnt ?? 1));
      const askText = `${parkedCount} ${parkedCount === 1 ? "finding needs" : "findings need"} a prioritisation decision: create follow-up work | dismiss with reason | take over`;
      const blockedWork = `scope ${input.scope || "(global)"} · ${parkedCount} parked ${parkedCount === 1 ? "cause" : "causes"}`;

      const { ask: scopeAsk, notified: scopeNotified } = this.upsertScopeDecisionAskInTx(
        {
          askId: input.scopeAskId,
          scope: input.scope,
          ask: askText,
          blockedWork,
          sourceRefs: input.scopeSourceRefs,
        },
        now,
      );
      if (scopeNotified) {
        journals.push(
          this.appendJournalInTx(now, cause.causeId, null, cause.runId, cause.goalId, "notified", {
            askId: scopeAsk.askId,
            kind: "scope_decision",
            scope: input.scope,
            ask: scopeAsk.ask,
            parkedCount,
          }),
        );
      }

      return { parked: true, scopeNotified, cause, scopeAsk, journals };
    });
  }

  private upsertScopeDecisionAskInTx(
    input: {
      askId: string;
      scope: string;
      ask: string;
      blockedWork: string;
      sourceRefs: EntityRef[];
    },
    now: number,
  ): { ask: TriageAskRecord; notified: boolean } {
    const existing = this.getActiveScopeAskInTx(input.scope);
    if (existing) {
      this.db
        .prepare(
          `UPDATE inbox_triage_ask
           SET ask = ?, blocked_work = ?, source_refs_json = ?
           WHERE ask_id = ?`,
        )
        .run(input.ask, input.blockedWork, JSON.stringify(input.sourceRefs), existing.askId);
      return { ask: this.getAskInTx(existing.askId)!, notified: false };
    }
    this.db
      .prepare(
        `INSERT INTO inbox_triage_ask(
           ask_id, cause_id, scope, kind, ask, blocked_work, source_refs_json, created_at, notified_at, cleared_at, clear_kind
         ) VALUES (?, NULL, ?, 'scope_decision', ?, ?, ?, ?, ?, NULL, NULL)`,
      )
      .run(
        input.askId,
        input.scope,
        input.ask,
        input.blockedWork,
        JSON.stringify(input.sourceRefs),
        now,
        now,
      );
    return { ask: this.getAskInTx(input.askId)!, notified: true };
  }

  upsertScopeDecisionAsk(
    input: {
      askId: string;
      scope: string;
      ask: string;
      blockedWork: string;
      sourceRefs: EntityRef[];
      causeIdForJournal?: string;
    },
    now: number,
  ): { ask: TriageAskRecord; notified: boolean; journal: TriageJournalRecord | null } {
    this.ensureSchema();
    return this.tx(() => {
      const { ask, notified } = this.upsertScopeDecisionAskInTx(input, now);
      let journal: TriageJournalRecord | null = null;
      if (notified) {
        journal = this.appendJournalInTx(
          now,
          input.causeIdForJournal ?? `scope:${input.scope}`,
          null,
          null,
          null,
          "notified",
          { askId: ask.askId, kind: "scope_decision", scope: input.scope, ask: ask.ask },
        );
      }
      return { ask, notified, journal };
    });
  }

  clearScopeDecisionAsk(
    scope: string,
    clearKind: AskClearKind,
    now: number,
  ): { cleared: boolean; ask: TriageAskRecord | null; journal: TriageJournalRecord | null } {
    this.ensureSchema();
    return this.tx(() => {
      const existing = this.getActiveScopeAskInTx(scope);
      if (!existing) return { cleared: false, ask: null, journal: null };
      const res = this.db
        .prepare(`UPDATE inbox_triage_ask SET cleared_at = ?, clear_kind = ? WHERE ask_id = ? AND cleared_at IS NULL`)
        .run(now, clearKind, existing.askId);
      if (res.changes === 0) return { cleared: false, ask: existing, journal: null };
      const ask = this.getAskInTx(existing.askId)!;
      const journal = this.appendJournalInTx(now, `scope:${scope}`, null, null, null, "cleared", {
        askId: ask.askId,
        kind: "scope_decision",
        scope,
        clearKind,
      });
      return { cleared: true, ask, journal };
    });
  }

  // ---- Resolve / Clear / Reopen / Rearm ----

  resolveCause(
    input: {
      causeId: string;
      expectedRevision: number;
      outcomeKind: CauseOutcomeKind;
      outcome: string;
      clearKind?: AskClearKind;
      itemKey?: string | null;
    },
    now: number,
  ): {
    resolved: boolean;
    askCleared: boolean;
    cause: TriageCauseRecord | null;
    journals: TriageJournalRecord[];
  } {
    this.ensureSchema();
    return this.tx(() => {
      const current = this.getCauseInTx(input.causeId);
      if (!current || current.revision !== input.expectedRevision || current.state === "resolved") {
        return { resolved: false, askCleared: false, cause: current, journals: [] };
      }
      const res = this.db
        .prepare(
          `UPDATE inbox_triage_cause
           SET state = 'resolved',
               outcome_kind = ?,
               outcome = ?,
               resolved_at = ?,
               owner_kind = NULL,
               owner_id = NULL,
               owner_epoch = NULL,
               lease_expires_at = NULL,
               assignment_expires_at = NULL,
               next_check_at = NULL,
               revision = revision + 1,
               updated_at = ?
           WHERE cause_id = ? AND revision = ? AND state <> 'resolved'`,
        )
        .run(input.outcomeKind, input.outcome, now, now, input.causeId, input.expectedRevision);
      if (res.changes === 0) {
        return { resolved: false, askCleared: false, cause: this.getCauseInTx(input.causeId), journals: [] };
      }
      this.db
        .prepare(`UPDATE inbox_triage_item SET closed_at = ?, close_kind = ? WHERE cause_id = ? AND closed_at IS NULL`)
        .run(now, input.outcomeKind, input.causeId);

      const cause = this.getCauseInTx(input.causeId)!;
      const journals: TriageJournalRecord[] = [];

      let askCleared = false;
      const activeAsk = this.getActiveAskForCauseInTx(input.causeId);
      if (activeAsk) {
        const ck: AskClearKind =
          input.clearKind ?? (input.outcomeKind === "answered" || input.outcomeKind === "source_resolved" ? "answered" : "superseded");
        const cRes = this.db
          .prepare(`UPDATE inbox_triage_ask SET cleared_at = ?, clear_kind = ? WHERE ask_id = ? AND cleared_at IS NULL`)
          .run(now, ck, activeAsk.askId);
        if (cRes.changes === 1) {
          askCleared = true;
          journals.push(
            this.appendJournalInTx(now, cause.causeId, input.itemKey ?? null, cause.runId, cause.goalId, "cleared", {
              askId: activeAsk.askId,
              clearKind: ck,
              outcome: input.outcome,
            }),
          );
        }
      }

      journals.push(
        this.appendJournalInTx(now, cause.causeId, input.itemKey ?? null, cause.runId, cause.goalId, "resolved", {
          outcomeKind: input.outcomeKind,
          outcome: input.outcome,
          previousState: current.state,
        }),
      );

      // If this was the last parked cause in its scope, clear the scope_decision ask too.
      if (current.state === "parked") {
        const scope = current.repoRoot ?? "";
        const remaining = this.db
          .prepare(
            `SELECT COUNT(*) AS cnt FROM inbox_triage_cause WHERE COALESCE(repo_root, '') = ? AND state = 'parked'`,
          )
          .get(scope) as { cnt: number };
        if (Number(remaining?.cnt ?? 0) === 0) {
          const scopeAsk = this.getActiveScopeAskInTx(scope);
          if (scopeAsk) {
            const scRes = this.db
              .prepare(`UPDATE inbox_triage_ask SET cleared_at = ?, clear_kind = 'superseded' WHERE ask_id = ? AND cleared_at IS NULL`)
              .run(now, scopeAsk.askId);
            if (scRes.changes === 1) {
              journals.push(
                this.appendJournalInTx(now, cause.causeId, null, cause.runId, cause.goalId, "cleared", {
                  askId: scopeAsk.askId,
                  kind: "scope_decision",
                  scope,
                  clearKind: "superseded",
                }),
              );
            }
          }
        }
      }

      return { resolved: true, askCleared, cause, journals };
    });
  }

  /**
   * Reopens a previously resolved cause (e.g. escalation resolved -> open/acked via `ackEscalation(false)`).
   * Preserves `attempts`, `first_dispatch_at`, `first_assigned_at`, `absolute_deadline_at`, and full journal history
   * (no fresh budget). Re-scanning an already open cause is a no-op (`reopened: false`).
   */
  reopenCause(
    input: {
      causeId: string;
      expectedRevision: number;
      rule: string;
      disposition: CauseDisposition;
      nextAction: string;
      nextCheckAt?: number | null;
      reason: string;
      itemKey?: string | null;
    },
    now: number,
  ): { reopened: boolean; cause: TriageCauseRecord | null; journal: TriageJournalRecord | null } {
    this.ensureSchema();
    return this.tx(() => {
      const current = this.getCauseInTx(input.causeId);
      if (!current || current.revision !== input.expectedRevision || current.state !== "resolved") {
        return { reopened: false, cause: current, journal: null };
      }
      const prevOutcome = { outcomeKind: current.outcomeKind, outcome: current.outcome, resolvedAt: current.resolvedAt };
      const res = this.db
        .prepare(
          `UPDATE inbox_triage_cause
           SET state = 'acknowledged',
               rule = ?,
               disposition = ?,
               owner_kind = NULL,
               owner_id = NULL,
               owner_epoch = NULL,
               assignment_id = NULL,
               lease_expires_at = NULL,
               assignment_expires_at = NULL,
               hold_reason = NULL,
               ask_id = NULL,
               outcome_kind = NULL,
               outcome = NULL,
               resolved_at = NULL,
               next_action = ?,
               next_check_at = ?,
               revision = revision + 1,
               updated_at = ?
           WHERE cause_id = ? AND revision = ? AND state = 'resolved'`,
        )
        .run(
          input.rule,
          input.disposition,
          input.nextAction,
          input.nextCheckAt !== undefined ? input.nextCheckAt : now,
          now,
          input.causeId,
          input.expectedRevision,
        );
      if (res.changes === 0) {
        return { reopened: false, cause: this.getCauseInTx(input.causeId), journal: null };
      }
      const cause = this.getCauseInTx(input.causeId)!;
      const journal = this.appendJournalInTx(
        now,
        cause.causeId,
        input.itemKey ?? null,
        cause.runId,
        cause.goalId,
        "reopened",
        { reason: input.reason, previousOutcome: prevOutcome, preservedAttempts: cause.attempts },
      );
      return { reopened: true, cause, journal };
    });
  }

  /**
   * Explicit operator rearm (`inbox.triage.rearm`): grants a fresh lifetime attempt/deadline budget
   * on an unresolved cause and returns it to `acknowledged` ready for immediate scheduling.
   */
  rearmCause(
    causeId: string,
    now: number,
    reason?: string,
  ): { rearmed: boolean; cause: TriageCauseRecord | null; journal: TriageJournalRecord | null; reason?: string } {
    this.ensureSchema();
    return this.tx(() => {
      const current = this.getCauseInTx(causeId);
      if (!current) return { rearmed: false, cause: null, journal: null, reason: `Unknown cause ${causeId}` };
      if (current.state === "resolved") {
        return { rearmed: false, cause: current, journal: null, reason: "Cause is already resolved" };
      }
      const prevAttempts = current.attempts;
      const prevState = current.state;
      const res = this.db
        .prepare(
          `UPDATE inbox_triage_cause
           SET state = 'acknowledged',
               owner_kind = NULL,
               owner_id = NULL,
               owner_epoch = NULL,
               assignment_id = NULL,
               lease_expires_at = NULL,
               assignment_expires_at = NULL,
               no_progress_renewals = 0,
               attempts = 0,
               first_dispatch_at = NULL,
               first_assigned_at = NULL,
               absolute_deadline_at = NULL,
               hold_reason = NULL,
               next_check_at = ?,
               last_action_result = NULL,
               revision = revision + 1,
               updated_at = ?
           WHERE cause_id = ? AND revision = ? AND state <> 'resolved'`,
        )
        .run(now, now, causeId, current.revision);
      if (res.changes === 0) {
        return { rearmed: false, cause: this.getCauseInTx(causeId), journal: null, reason: "Concurrent update" };
      }

      // If it was the last parked cause in its scope, clear the scope_decision ask.
      if (prevState === "parked") {
        const scope = current.repoRoot ?? "";
        const remaining = this.db
          .prepare(
            `SELECT COUNT(*) AS cnt FROM inbox_triage_cause WHERE COALESCE(repo_root, '') = ? AND state = 'parked'`,
          )
          .get(scope) as { cnt: number };
        if (Number(remaining?.cnt ?? 0) === 0) {
          const scopeAsk = this.getActiveScopeAskInTx(scope);
          if (scopeAsk) {
            this.db
              .prepare(`UPDATE inbox_triage_ask SET cleared_at = ?, clear_kind = 'superseded' WHERE ask_id = ? AND cleared_at IS NULL`)
              .run(now, scopeAsk.askId);
          }
        }
      }

      const cause = this.getCauseInTx(causeId)!;
      const journal = this.appendJournalInTx(
        now,
        cause.causeId,
        null,
        cause.runId,
        cause.goalId,
        "rearmed",
        { reason: reason ?? "operator rearm", previousState: prevState, previousAttempts: prevAttempts },
      );
      return { rearmed: true, cause, journal };
    });
  }

  // ---- Effect Intent / Dispatch / Receipt ----

  claimEffectIntent(
    input: ClaimEffectIntentInput,
    now: number,
  ): {
    claimed: boolean;
    reason?: "not_found" | "revision_mismatch" | "invalid_state" | "budget_exhausted" | "not_due" | "target_busy";
    effect?: TriageEffectRecord;
    cause?: TriageCauseRecord;
    journal?: TriageJournalRecord;
  } {
    this.ensureSchema();
    return this.tx(() => {
      const current = this.getCauseInTx(input.causeId);
      if (!current) return { claimed: false, reason: "not_found" };
      if (current.revision !== input.expectedRevision) return { claimed: false, reason: "revision_mismatch" };
      const stateOk = input.allowAnyOpenState
        ? current.state !== "resolved"
        : current.state === "acknowledged" || current.state === "owned";
      if (!stateOk) {
        return { claimed: false, reason: "invalid_state" };
      }
      if (current.attempts >= input.maxAutoAttempts) {
        return { claimed: false, reason: "budget_exhausted" };
      }
      if (!input.allowBypassDueCheck && current.nextCheckAt !== null && now < current.nextCheckAt) {
        return { claimed: false, reason: "not_due" };
      }

      const attempt = current.attempts + 1;
      const effectKey = makeEffectKey(current.causeId, attempt);
      const firstDispatchAt = current.firstDispatchAt ?? now;
      const absoluteDeadlineAt =
        current.absoluteDeadlineAt !== null
          ? input.absoluteDeadlineAt != null
            ? Math.min(current.absoluteDeadlineAt, input.absoluteDeadlineAt)
            : current.absoluteDeadlineAt
          : (input.absoluteDeadlineAt ?? null);

      try {
        this.db
          .prepare(
            `INSERT INTO inbox_triage_effect(
               effect_key, cause_id, attempt, primitive, target_kind, target_id, target_epoch,
               status, intent_at, dispatched_at, receipt, receipt_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'intent', ?, NULL, NULL, NULL)`,
          )
          .run(
            effectKey,
            current.causeId,
            attempt,
            input.primitive,
            input.targetKind,
            input.targetId,
            input.targetEpoch,
            now,
          );
      } catch (err) {
        if (isUniqueConstraintError(err)) {
          return { claimed: false, reason: "target_busy" };
        }
        throw err;
      }

      const upd = this.db
        .prepare(
          `UPDATE inbox_triage_cause
           SET attempts = ?,
               first_dispatch_at = ?,
               absolute_deadline_at = ?,
               next_check_at = ?,
               revision = revision + 1,
               updated_at = ?
           WHERE cause_id = ? AND revision = ?`,
        )
        .run(
          attempt,
          firstDispatchAt,
          absoluteDeadlineAt,
          input.nextCheckAt,
          now,
          current.causeId,
          input.expectedRevision,
        );
      if (upd.changes === 0) {
        throw new Error(`CAS mismatch inside claimEffectIntent for ${current.causeId}`);
      }

      const effect = this.getEffectInTx(effectKey)!;
      const cause = this.getCauseInTx(current.causeId)!;
      const journal = this.appendJournalInTx(
        now,
        cause.causeId,
        input.itemKey ?? null,
        cause.runId,
        cause.goalId,
        "effect_intent",
        {
          effectKey,
          attempt,
          primitive: input.primitive,
          targetKind: input.targetKind,
          targetId: input.targetId,
          targetEpoch: input.targetEpoch,
          nextCheckAt: input.nextCheckAt,
        },
      );
      return { claimed: true, effect, cause, journal };
    });
  }

  markEffectDispatched(effectKey: string, now: number): boolean {
    this.ensureSchema();
    const res = this.db
      .prepare(
        `UPDATE inbox_triage_effect
         SET status = 'dispatched', dispatched_at = ?
         WHERE effect_key = ? AND status = 'intent'`,
      )
      .run(now, effectKey);
    return res.changes === 1;
  }

  recordEffectReceipt(
    effectKey: string,
    status: "confirmed" | "failed" | "ambiguous",
    receipt: string,
    now: number,
    causePatch?: EffectCausePatch,
  ): {
    recorded: boolean;
    causeUpdated: boolean;
    effect: TriageEffectRecord | null;
    cause: TriageCauseRecord | null;
    journal: TriageJournalRecord | null;
  } {
    this.ensureSchema();
    return this.tx(() => {
      const existing = this.getEffectInTx(effectKey);
      if (!existing || (existing.status !== "intent" && existing.status !== "dispatched")) {
        return {
          recorded: false,
          causeUpdated: false,
          effect: existing,
          cause: existing ? this.getCauseInTx(existing.causeId) : null,
          journal: null,
        };
      }

      const upd = this.db
        .prepare(
          `UPDATE inbox_triage_effect
           SET status = ?, receipt = ?, receipt_at = ?
           WHERE effect_key = ? AND status IN ('intent', 'dispatched')`,
        )
        .run(status, receipt, now, effectKey);
      if (upd.changes === 0) {
        return {
          recorded: false,
          causeUpdated: false,
          effect: this.getEffectInTx(effectKey),
          cause: this.getCauseInTx(existing.causeId),
          journal: null,
        };
      }

      const effect = this.getEffectInTx(effectKey)!;
      let cause = this.getCauseInTx(existing.causeId);
      const journal = this.appendJournalInTx(
        now,
        existing.causeId,
        null,
        cause?.runId ?? null,
        cause?.goalId ?? null,
        "effect_receipt",
        {
          effectKey,
          attempt: effect.attempt,
          primitive: effect.primitive,
          status,
          receipt,
        },
      );

      let causeUpdated = false;
      if (causePatch && cause && cause.state !== "resolved" && cause.state !== "surfaced" && cause.revision === causePatch.expectedRevision) {
        const cUpd = this.db
          .prepare(
            `UPDATE inbox_triage_cause
             SET state = ?,
                 rule = ?,
                 disposition = ?,
                 owner_kind = ?,
                 owner_id = ?,
                 owner_epoch = ?,
                 assignment_id = ?,
                 next_action = ?,
                 next_check_at = ?,
                 lease_expires_at = ?,
                 assignment_expires_at = ?,
                 last_progress_seq = ?,
                 no_progress_renewals = ?,
                 first_assigned_at = ?,
                 absolute_deadline_at = ?,
                 hold_reason = ?,
                 last_action_result = ?,
                 revision = revision + 1,
                 updated_at = ?
             WHERE cause_id = ? AND revision = ? AND state NOT IN ('resolved', 'surfaced')`,
          )
          .run(
            causePatch.state ?? cause.state,
            causePatch.rule ?? cause.rule,
            causePatch.disposition ?? cause.disposition,
            causePatch.ownerKind !== undefined ? causePatch.ownerKind : cause.ownerKind,
            causePatch.ownerId !== undefined ? causePatch.ownerId : cause.ownerId,
            causePatch.ownerEpoch !== undefined ? causePatch.ownerEpoch : cause.ownerEpoch,
            causePatch.assignmentId !== undefined ? causePatch.assignmentId : cause.assignmentId,
            causePatch.nextAction ?? cause.nextAction,
            causePatch.nextCheckAt !== undefined ? causePatch.nextCheckAt : cause.nextCheckAt,
            causePatch.leaseExpiresAt !== undefined ? causePatch.leaseExpiresAt : cause.leaseExpiresAt,
            causePatch.assignmentExpiresAt !== undefined ? causePatch.assignmentExpiresAt : cause.assignmentExpiresAt,
            causePatch.lastProgressSeq ?? cause.lastProgressSeq,
            causePatch.noProgressRenewals ?? cause.noProgressRenewals,
            causePatch.firstAssignedAt !== undefined ? causePatch.firstAssignedAt : cause.firstAssignedAt,
            causePatch.absoluteDeadlineAt !== undefined ? causePatch.absoluteDeadlineAt : cause.absoluteDeadlineAt,
            causePatch.holdReason !== undefined ? causePatch.holdReason : cause.holdReason,
            causePatch.lastActionResult !== undefined ? causePatch.lastActionResult : receipt,
            now,
            cause.causeId,
            causePatch.expectedRevision,
          );
        causeUpdated = cUpd.changes === 1;
        cause = this.getCauseInTx(existing.causeId);
      }

      return { recorded: true, causeUpdated, effect, cause, journal };
    });
  }

  // ---- Assignments (steer / batch) & Membership ----

  createAssignment(
    input: {
      assignmentId: string;
      scope: string;
      kind: AssignmentKind;
      ownerTaskId: string | null;
      ownerGeneration: number | null;
      expiresAt: number;
      causeIds: string[];
    },
    now: number,
  ): { created: boolean; assignment: TriageAssignmentRecord | null; journals: TriageJournalRecord[] } {
    this.ensureSchema();
    return this.tx(() => {
      try {
        this.db
          .prepare(
            `INSERT INTO inbox_triage_assignment(
               assignment_id, scope, kind, owner_task_id, owner_generation, status, created_at, expires_at, result_json
             ) VALUES (?, ?, ?, ?, ?, 'open', ?, ?, NULL)`,
          )
          .run(
            input.assignmentId,
            input.scope,
            input.kind,
            input.ownerTaskId,
            input.ownerGeneration,
            now,
            input.expiresAt,
          );
      } catch (err) {
        if (isUniqueConstraintError(err)) {
          const existing = input.kind === "batch" ? this.getOpenBatchForScopeInTx(input.scope) : null;
          return { created: false, assignment: existing, journals: [] };
        }
        throw err;
      }

      const memberStmt = this.db.prepare(
        `INSERT OR IGNORE INTO inbox_triage_assignment_member(assignment_id, cause_id) VALUES (?, ?)`,
      );
      const journals: TriageJournalRecord[] = [];
      for (const cid of input.causeIds) {
        memberStmt.run(input.assignmentId, cid);
        const c = this.getCauseInTx(cid);
        journals.push(
          this.appendJournalInTx(now, cid, null, c?.runId ?? null, c?.goalId ?? null, "assigned", {
            assignmentId: input.assignmentId,
            scope: input.scope,
            kind: input.kind,
            ownerTaskId: input.ownerTaskId,
            ownerGeneration: input.ownerGeneration,
            expiresAt: input.expiresAt,
          }),
        );
      }

      return { created: true, assignment: this.getAssignmentInTx(input.assignmentId)!, journals };
    });
  }

  addAssignmentMembers(
    assignmentId: string,
    causeIds: string[],
    now: number,
  ): { added: string[]; journals: TriageJournalRecord[] } {
    this.ensureSchema();
    return this.tx(() => {
      const assign = this.getAssignmentInTx(assignmentId);
      if (!assign || assign.status !== "open") return { added: [], journals: [] };
      const stmt = this.db.prepare(
        `INSERT OR IGNORE INTO inbox_triage_assignment_member(assignment_id, cause_id) VALUES (?, ?)`,
      );
      const added: string[] = [];
      const journals: TriageJournalRecord[] = [];
      for (const cid of causeIds) {
        const res = stmt.run(assignmentId, cid);
        if (res.changes === 1) {
          added.push(cid);
          const c = this.getCauseInTx(cid);
          journals.push(
            this.appendJournalInTx(now, cid, null, c?.runId ?? null, c?.goalId ?? null, "assigned", {
              assignmentId,
              scope: assign.scope,
              kind: assign.kind,
              ownerTaskId: assign.ownerTaskId,
              ownerGeneration: assign.ownerGeneration,
              expiresAt: assign.expiresAt,
            }),
          );
        }
      }
      return { added, journals };
    });
  }

  setAssignmentOwnerTask(assignmentId: string, ownerTaskId: string, ownerGeneration: number): boolean {
    this.ensureSchema();
    const res = this.db
      .prepare(
        `UPDATE inbox_triage_assignment
         SET owner_task_id = ?, owner_generation = ?
         WHERE assignment_id = ? AND status = 'open'`,
      )
      .run(ownerTaskId, ownerGeneration, assignmentId);
    return res.changes === 1;
  }

  closeAssignment(
    assignmentId: string,
    status: "returned" | "expired" | "failed",
    result: unknown,
    now: number,
  ): { closed: boolean; assignment: TriageAssignmentRecord | null; memberCauseIds: string[]; journals: TriageJournalRecord[] } {
    this.ensureSchema();
    return this.tx(() => {
      const existing = this.getAssignmentInTx(assignmentId);
      if (!existing || existing.status !== "open") {
        return { closed: false, assignment: existing, memberCauseIds: [], journals: [] };
      }
      const res = this.db
        .prepare(
          `UPDATE inbox_triage_assignment
           SET status = ?, result_json = ?
           WHERE assignment_id = ? AND status = 'open'`,
        )
        .run(status, result === undefined ? null : JSON.stringify(result), assignmentId);
      if (res.changes === 0) {
        return { closed: false, assignment: this.getAssignmentInTx(assignmentId), memberCauseIds: [], journals: [] };
      }
      const memberCauseIds = this.listAssignmentMembersInTx(assignmentId);
      const journals: TriageJournalRecord[] = [];
      if (existing.kind === "batch") {
        for (const cid of memberCauseIds) {
          const c = this.getCauseInTx(cid);
          journals.push(
            this.appendJournalInTx(now, cid, null, c?.runId ?? null, c?.goalId ?? null, "batch_returned", {
              assignmentId,
              status,
              ownerTaskId: existing.ownerTaskId,
            }),
          );
        }
      }
      return { closed: true, assignment: this.getAssignmentInTx(assignmentId)!, memberCauseIds, journals };
    });
  }

  // ---- R07 Continuation Permissions ----

  recordContinuation(
    escalationId: string,
    now: number,
    meta?: { runId?: string | null; goalId?: string | null },
  ): { recorded: boolean; record: TriageContinuationRecord; journal: TriageJournalRecord | null } {
    this.ensureSchema();
    return this.tx(() => {
      const res = this.db
        .prepare(
          `INSERT OR IGNORE INTO inbox_triage_continuation(escalation_id, recorded_at, consumed_at, consumed_task_id, consumed_generation)
           VALUES (?, ?, NULL, NULL, NULL)`,
        )
        .run(escalationId, now);
      const row = this.db
        .prepare(`SELECT * FROM inbox_triage_continuation WHERE escalation_id = ?`)
        .get(escalationId) as Record<string, unknown>;
      const record = mapContinuation(row);
      if (res.changes === 0) return { recorded: false, record, journal: null };
      const journal = this.appendJournalInTx(
        now,
        makeCauseId("escalation", escalationId),
        null,
        meta?.runId ?? null,
        meta?.goalId ?? null,
        "continuation_recorded",
        { escalationId },
      );
      return { recorded: true, record, journal };
    });
  }

  getContinuation(escalationId: string): TriageContinuationRecord | null {
    if (!this.hasSchema()) return null;
    const row = this.db
      .prepare(`SELECT * FROM inbox_triage_continuation WHERE escalation_id = ?`)
      .get(escalationId) as Record<string, unknown> | undefined;
    return row ? mapContinuation(row) : null;
  }

  consumeContinuation(escalationId: string, taskId: string, generation: number, now: number): boolean {
    this.ensureSchema();
    const res = this.db
      .prepare(
        `UPDATE inbox_triage_continuation
         SET consumed_at = ?, consumed_task_id = ?, consumed_generation = ?
         WHERE escalation_id = ? AND consumed_at IS NULL`,
      )
      .run(now, taskId, generation, escalationId);
    return res.changes === 1;
  }

  // ---- Read-only Queries (never trigger DDL) ----

  private getCauseInTx(causeId: string): TriageCauseRecord | null {
    const r = this.db.prepare(`SELECT * FROM inbox_triage_cause WHERE cause_id = ?`).get(causeId) as
      | Record<string, unknown>
      | undefined;
    return r ? mapCause(r) : null;
  }

  private getEffectInTx(effectKey: string): TriageEffectRecord | null {
    const r = this.db.prepare(`SELECT * FROM inbox_triage_effect WHERE effect_key = ?`).get(effectKey) as
      | Record<string, unknown>
      | undefined;
    return r ? mapEffect(r) : null;
  }

  private getAssignmentInTx(assignmentId: string): TriageAssignmentRecord | null {
    const r = this.db
      .prepare(`SELECT * FROM inbox_triage_assignment WHERE assignment_id = ?`)
      .get(assignmentId) as Record<string, unknown> | undefined;
    return r ? mapAssignment(r) : null;
  }

  private getOpenBatchForScopeInTx(scope: string): TriageAssignmentRecord | null {
    const r = this.db
      .prepare(`SELECT * FROM inbox_triage_assignment WHERE scope = ? AND kind = 'batch' AND status = 'open'`)
      .get(scope) as Record<string, unknown> | undefined;
    return r ? mapAssignment(r) : null;
  }

  private listAssignmentMembersInTx(assignmentId: string): string[] {
    const rows = this.db
      .prepare(`SELECT cause_id FROM inbox_triage_assignment_member WHERE assignment_id = ? ORDER BY cause_id ASC`)
      .all(assignmentId) as Array<{ cause_id: string }>;
    return rows.map((r) => String(r.cause_id));
  }

  private getAskInTx(askId: string): TriageAskRecord | null {
    const r = this.db.prepare(`SELECT * FROM inbox_triage_ask WHERE ask_id = ?`).get(askId) as
      | Record<string, unknown>
      | undefined;
    return r ? mapAsk(r) : null;
  }

  private getActiveAskForCauseInTx(causeId: string): TriageAskRecord | null {
    const r = this.db
      .prepare(`SELECT * FROM inbox_triage_ask WHERE cause_id = ? AND kind = 'blocker' AND cleared_at IS NULL`)
      .get(causeId) as Record<string, unknown> | undefined;
    return r ? mapAsk(r) : null;
  }

  private getActiveScopeAskInTx(scope: string): TriageAskRecord | null {
    const r = this.db
      .prepare(`SELECT * FROM inbox_triage_ask WHERE scope = ? AND kind = 'scope_decision' AND cleared_at IS NULL`)
      .get(scope) as Record<string, unknown> | undefined;
    return r ? mapAsk(r) : null;
  }

  getCause(causeId: string): TriageCauseRecord | null {
    if (!this.hasSchema()) return null;
    return this.getCauseInTx(causeId);
  }

  listCauses(filter?: {
    state?: InboxTriageState;
    states?: InboxTriageState[];
    runId?: string;
    causeId?: string;
    repoRoot?: string;
    limit?: number;
  }): TriageCauseRecord[] {
    if (!this.hasSchema()) return [];
    const where: string[] = [];
    const params: Array<string | number> = [];
    if (filter?.state) {
      where.push("state = ?");
      params.push(filter.state);
    } else if (filter?.states?.length) {
      where.push(`state IN (${filter.states.map(() => "?").join(", ")})`);
      params.push(...filter.states);
    }
    if (filter?.runId) {
      where.push("run_id = ?");
      params.push(filter.runId);
    }
    if (filter?.causeId) {
      where.push("cause_id = ?");
      params.push(filter.causeId);
    }
    if (filter?.repoRoot) {
      where.push("repo_root = ?");
      params.push(filter.repoRoot);
    }
    const limitClause = filter?.limit != null ? " LIMIT ?" : "";
    if (filter?.limit != null) params.push(filter.limit);
    const sql = `SELECT * FROM inbox_triage_cause${where.length ? ` WHERE ${where.join(" AND ")}` : ""} ORDER BY acked_at ASC, cause_id ASC${limitClause}`;
    const rows = this.db.prepare(sql).all(...params) as Record<string, unknown>[];
    return rows.map(mapCause);
  }

  listDueCauses(now: number): TriageCauseRecord[] {
    if (!this.hasSchema()) return [];
    const rows = this.db
      .prepare(
        `SELECT * FROM inbox_triage_cause
         WHERE state IN ('acknowledged', 'owned')
           AND next_check_at IS NOT NULL
           AND next_check_at <= ?
         ORDER BY next_check_at ASC, acked_at ASC, cause_id ASC`,
      )
      .all(now) as Record<string, unknown>[];
    return rows.map(mapCause);
  }

  listOwnedCauses(): TriageCauseRecord[] {
    if (!this.hasSchema()) return [];
    const rows = this.db
      .prepare(`SELECT * FROM inbox_triage_cause WHERE state = 'owned' ORDER BY acked_at ASC, cause_id ASC`)
      .all() as Record<string, unknown>[];
    return rows.map(mapCause);
  }

  listItemsForCause(causeId: string, openOnly = false): TriageItemRecord[] {
    if (!this.hasSchema()) return [];
    const sql = openOnly
      ? `SELECT * FROM inbox_triage_item WHERE cause_id = ? AND closed_at IS NULL ORDER BY first_seen_at ASC`
      : `SELECT * FROM inbox_triage_item WHERE cause_id = ? ORDER BY first_seen_at ASC`;
    const rows = this.db.prepare(sql).all(causeId) as Record<string, unknown>[];
    return rows.map(mapItem);
  }

  listOpenItems(): TriageItemRecord[] {
    if (!this.hasSchema()) return [];
    const rows = this.db
      .prepare(`SELECT * FROM inbox_triage_item WHERE closed_at IS NULL ORDER BY first_seen_at ASC`)
      .all() as Record<string, unknown>[];
    return rows.map(mapItem);
  }

  getEffect(effectKey: string): TriageEffectRecord | null {
    if (!this.hasSchema()) return null;
    return this.getEffectInTx(effectKey);
  }

  listEffectsForCause(causeId: string): TriageEffectRecord[] {
    if (!this.hasSchema()) return [];
    const rows = this.db
      .prepare(`SELECT * FROM inbox_triage_effect WHERE cause_id = ? ORDER BY attempt ASC`)
      .all(causeId) as Record<string, unknown>[];
    return rows.map(mapEffect);
  }

  listOutstandingEffects(): TriageEffectRecord[] {
    if (!this.hasSchema()) return [];
    const rows = this.db
      .prepare(`SELECT * FROM inbox_triage_effect WHERE status IN ('intent', 'dispatched') ORDER BY intent_at ASC`)
      .all() as Record<string, unknown>[];
    return rows.map(mapEffect);
  }

  getAssignment(assignmentId: string): TriageAssignmentRecord | null {
    if (!this.hasSchema()) return null;
    return this.getAssignmentInTx(assignmentId);
  }

  getOpenBatchForScope(scope: string): TriageAssignmentRecord | null {
    if (!this.hasSchema()) return null;
    return this.getOpenBatchForScopeInTx(scope);
  }

  listOpenAssignments(kind?: AssignmentKind): TriageAssignmentRecord[] {
    if (!this.hasSchema()) return [];
    const rows = kind
      ? (this.db
          .prepare(`SELECT * FROM inbox_triage_assignment WHERE status = 'open' AND kind = ? ORDER BY created_at ASC`)
          .all(kind) as Record<string, unknown>[])
      : (this.db
          .prepare(`SELECT * FROM inbox_triage_assignment WHERE status = 'open' ORDER BY created_at ASC`)
          .all() as Record<string, unknown>[]);
    return rows.map(mapAssignment);
  }

  listAssignmentMembers(assignmentId: string): string[] {
    if (!this.hasSchema()) return [];
    return this.listAssignmentMembersInTx(assignmentId);
  }

  isTriageOwnedTask(taskId: string): boolean {
    if (!this.hasSchema()) return false;
    const row = this.db
      .prepare(`SELECT 1 AS ok FROM inbox_triage_assignment WHERE kind = 'batch' AND owner_task_id = ? LIMIT 1`)
      .get(taskId) as { ok?: number } | undefined;
    return Boolean(row?.ok);
  }

  getActiveAskForCause(causeId: string): TriageAskRecord | null {
    if (!this.hasSchema()) return null;
    return this.getActiveAskForCauseInTx(causeId);
  }

  getActiveScopeAsk(scope: string): TriageAskRecord | null {
    if (!this.hasSchema()) return null;
    return this.getActiveScopeAskInTx(scope);
  }

  listActiveAsks(kind?: AskKind): TriageAskRecord[] {
    if (!this.hasSchema()) return [];
    const rows = kind
      ? (this.db
          .prepare(`SELECT * FROM inbox_triage_ask WHERE cleared_at IS NULL AND kind = ? ORDER BY created_at ASC`)
          .all(kind) as Record<string, unknown>[])
      : (this.db
          .prepare(`SELECT * FROM inbox_triage_ask WHERE cleared_at IS NULL ORDER BY created_at ASC`)
          .all() as Record<string, unknown>[]);
    return rows.map(mapAsk);
  }

  listJournal(filter?: { causeId?: string; runId?: string; type?: TriageJournalType; limit?: number }): TriageJournalRecord[] {
    if (!this.hasSchema()) return [];
    const where: string[] = [];
    const params: Array<string | number> = [];
    if (filter?.causeId) {
      where.push("cause_id = ?");
      params.push(filter.causeId);
    }
    if (filter?.runId) {
      where.push("run_id = ?");
      params.push(filter.runId);
    }
    if (filter?.type) {
      where.push("type = ?");
      params.push(filter.type);
    }
    const limitClause = filter?.limit != null ? " LIMIT ?" : "";
    if (filter?.limit != null) params.push(filter.limit);
    const sql = `SELECT * FROM inbox_triage_journal${where.length ? ` WHERE ${where.join(" AND ")}` : ""} ORDER BY seq ASC${limitClause}`;
    const rows = this.db.prepare(sql).all(...params) as Record<string, unknown>[];
    return rows.map(mapJournal);
  }
}
