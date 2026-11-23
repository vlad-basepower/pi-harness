import type { InboxTriageConfig } from "../config.js";
import type { GoalRecord } from "../goals/engine.js";
import type {
  EntityRef,
  InboxItem,
  InboxTriageCauseSummary,
  InboxTriageDisposition,
  InboxTriageInfo,
  InboxTriageJournalEntrySummary,
  InboxTriageListParams,
  InboxTriageListResult,
  InboxTriageOwnerKind,
  InboxTriagePlanRow,
  InboxTriageRearmResult,
  InboxTriageReportParams,
} from "../protocol/daemon-rpc.js";
import type {
  ControlPlaneDb,
  EscalationRecord,
  EventRecord,
  MergeQueueRecord,
  MessageRecord,
  RunRecord,
  TaskRecord,
} from "./db.js";
import {
  type CauseDisposition,
  type CauseOutcomeKind,
  type ContinuationGrantInput,
  type InboxTriageStore,
  type TriageAssignmentRecord,
  type TriageCauseRecord,
  type TriageContinuationRecord,
  type TriageEffectRecord,
  type TriageJournalRecord,
  makeCauseId,
  makeItemKey,
} from "./inbox-triage-store.js";
import { directorTaskRunOwner } from "./ids.js";
import {
  type RunQuotaPauseStatus,
  deriveRunCause,
  deriveRunStatus,
  isDirectorTaskContainer,
} from "./orchestrator.js";

/** Synthetic Director task container owner ID used exclusively for inbox triage rung-B batches. */
export const INBOX_TRIAGE_BATCH_OWNER = "inbox-triage";

const BATCH_TASK_TITLE_RE = /^Inbox triage batch \[([^\]]+)\]/;

export function formatBatchTaskTitle(assignmentId: string, scope: string): string {
  const label = scope ? ` (${scope.split("/").pop() || scope})` : "";
  return `Inbox triage batch [${assignmentId}]${label}`;
}

export function parseBatchAssignmentIdFromTitle(title: string): string | null {
  const m = BATCH_TASK_TITLE_RE.exec(title);
  return m?.[1] ?? null;
}

export function isTriageBatchRun(runId: string): boolean {
  return isDirectorTaskContainer(runId) && directorTaskRunOwner(runId) === INBOX_TRIAGE_BATCH_OWNER;
}

/** Extracts the full text output from a settled batch task (preferring resultJson.text / summary / handoffJson). */
export function extractBatchTaskOutput(task: Pick<TaskRecord, "summary" | "resultJson" | "handoffJson">): string {
  const candidates: string[] = [];
  if (task.summary) candidates.push(task.summary);
  if (task.resultJson) {
    const parsed = parseJsonObj(task.resultJson);
    if (typeof parsed?.text === "string" && parsed.text) candidates.push(parsed.text);
    if (typeof parsed?.summary === "string" && parsed.summary) candidates.push(parsed.summary);
  }
  if (task.handoffJson) candidates.push(task.handoffJson);
  for (const c of candidates) {
    if (TRIAGE_OUTCOMES_RE.test(c)) return c;
  }
  return candidates[0] ?? "";
}

const LIVE_TASK_STATUS_SET = new Set<TaskRecord["status"]>([
  "running",
  "draining",
  "waiting_children",
  "merging",
]);

const TERMINAL_MESSAGE_TASK_STATUS_SET = new Set<TaskRecord["status"]>([
  "completed",
  "failed",
  "cancelled",
]);

const TERMINAL_GOAL_STATUS_SET = new Set<GoalRecord["status"]>(["achieved", "cancelled"]);

const BLOCKED_ON_ESC_RE = /^Blocked on (esc-[A-Za-z0-9_-]+)/;
const DAEMON_RESTART_SUMMARY = "Interrupted by daemon restart";
const RUN_FAILURE_WAIT_REASON_RE =
  /linked run\b.*(?:failed|ended|interrupted)|interrupted by a daemon restart|blocked on escalation|\brun-[A-Za-z0-9_-]+\s+(?:failed|was interrupted)/i;
const HOLD_OR_CANCEL_RESOLUTION_RE = /\b(hold|cancel|cancelled|canceled|do not resume|don't resume|stop)\b/i;

export function isDoneReviewTask(task: Pick<TaskRecord, "tier" | "kind" | "title">): boolean {
  return task.tier === 3 && task.kind === "verify" && task.title.startsWith("Done-Review:");
}

export function isLiveTaskStatus(status: TaskRecord["status"]): boolean {
  return LIVE_TASK_STATUS_SET.has(status);
}

// ============================================================================
// 1. Pure Inbox Projection (byte-compatible with server.ts work.snapshot)
// ============================================================================

export interface BuildInboxRunEntry {
  run: RunRecord;
  goalId: string | null;
  tasks: TaskRecord[];
  quotaPause: { mode: string; reason: string } | null;
}

export interface BuildInboxItemsInput {
  goals: ReadonlyArray<
    Pick<GoalRecord, "id" | "version" | "title" | "status" | "linkedRunId" | "lastReviewSummary" | "updatedAt">
  >;
  runEntries: ReadonlyArray<BuildInboxRunEntry>;
  escalations: ReadonlyArray<EscalationRecord>;
  merges: ReadonlyArray<MergeQueueRecord>;
  messages: ReadonlyArray<MessageRecord>;
  goalByRunId: ReadonlyMap<string, Pick<GoalRecord, "id" | "title" | "status" | "version">>;
  /**
   * When true (triage sweep), include `acked` escalations as well as `open` (unresolved = open OR acked).
   * Defaults to false for exact byte-compatibility with legacy `work.snapshot`.
   */
  includeAckedEscalations?: boolean;
}

const SEV_RANK: Record<InboxItem["severity"], number> = { P0: 0, P1: 1, P2: 2, info: 3 };

export function sortInboxItems(items: InboxItem[]): InboxItem[] {
  // At equal severity an escalation (the concrete finding) precedes the derived items it causes, such as its
  // goal awaiting a human after the run settled.
  const kindRank = (item: InboxItem) => (item.kind === "escalation" ? 0 : 1);
  return items.sort(
    (a, b) => SEV_RANK[a.severity] - SEV_RANK[b.severity] || kindRank(a) - kindRank(b) || b.createdAt - a.createdAt,
  );
}

/**
 * Pure copy of the "Build severity-sorted Inbox" derivation from `src/daemon/server.ts` `work.snapshot`.
 * Keeps identical kinds, ids, severities, titles, details, refs, and sort order.
 */
export function buildInboxItems(input: BuildInboxItemsInput): InboxItem[] {
  const { goals, runEntries, escalations, merges, messages, goalByRunId, includeAckedEscalations = false } = input;
  const inbox: InboxItem[] = [];

  for (const g of goals) {
    if (g.status === "awaiting_human") {
      inbox.push({
        id: `inbox:goal:${g.id}`,
        severity: "P0",
        kind: "goal_awaiting_human",
        title: `${g.id} (v${g.version}) awaits input: ${g.title}`,
        detail: g.lastReviewSummary ?? "Goal paused awaiting human review or decision",
        ref: { kind: "goal", id: g.id, runId: g.linkedRunId ?? undefined, label: `${g.id} ${g.title}` },
        goalId: g.id,
        runId: g.linkedRunId,
        createdAt: g.updatedAt,
      });
    }
  }

  for (const e of escalations) {
    if (includeAckedEscalations ? e.status !== "open" && e.status !== "acked" : e.status !== "open") continue;
    const owningGoal = goalByRunId.get(e.runId);
    inbox.push({
      id: `inbox:esc:${e.id}`,
      severity: e.severity,
      kind: "escalation",
      title: `[${e.severity}/${e.category}] ${e.title}`,
      detail: `from ${e.fromTaskId} in ${e.runId}${e.blocking ? " · blocking" : ""}`,
      ref: { kind: "escalation", id: e.id, runId: e.runId, goalId: owningGoal?.id, label: `${e.id} ${e.title}` },
      goalId: owningGoal?.id ?? null,
      runId: e.runId,
      taskId: e.fromTaskId,
      createdAt: e.createdAt,
    });
  }

  for (const re of runEntries) {
    if (re.run.status === "paused_quota") {
      inbox.push({
        id: `inbox:quota:${re.run.id}`,
        severity: re.quotaPause?.mode === "manual" ? "P0" : "P1",
        kind: "quota_paused",
        title: `${re.run.id} paused for quota`,
        detail: re.quotaPause?.reason ?? "Quota limit reached",
        ref: { kind: "run", id: re.run.id, goalId: re.goalId ?? undefined, label: re.run.id },
        goalId: re.goalId,
        runId: re.run.id,
        createdAt: re.run.updatedAt,
      });
    } else if (re.run.status === "failed" && !re.run.id.startsWith("director-tasks-")) {
      inbox.push({
        id: `inbox:run:${re.run.id}`,
        severity: "P1",
        kind: "run_failed",
        title: `${re.run.id} failed`,
        detail: re.run.goal.split("\n")[0] ?? re.run.id,
        ref: { kind: "run", id: re.run.id, goalId: re.goalId ?? undefined, label: re.run.id },
        goalId: re.goalId,
        runId: re.run.id,
        createdAt: re.run.updatedAt,
      });
    }
    for (const t of re.tasks) {
      if (t.status === "blocked" || (t.status === "failed" && re.run.status === "active")) {
        inbox.push({
          id: `inbox:task:${t.id}`,
          severity: t.status === "blocked" ? "P1" : "P2",
          kind: t.status === "blocked" ? "task_blocked" : "task_failed",
          title: `${t.id} (${t.status}): ${t.title}`,
          detail: t.summary ?? `${re.run.id} · T${t.tier}/${t.kind}`,
          ref: { kind: "task", id: t.id, runId: re.run.id, goalId: re.goalId ?? undefined, label: `${t.id} ${t.title}` },
          goalId: re.goalId,
          runId: re.run.id,
          taskId: t.id,
          createdAt: t.updatedAt,
        });
      }
    }
  }

  for (const m of merges) {
    if (m.status === "conflict" || m.status === "ci_failed") {
      const owningGoal = goalByRunId.get(m.runId);
      inbox.push({
        id: `inbox:merge:${m.id}`,
        severity: "P1",
        kind: "merge_bounced",
        title: `Merge ${m.id} (${m.status}): ${m.branch}`,
        detail: (m.log ?? "").split("\n")[0] ?? m.status,
        ref: { kind: "merge", id: m.id, runId: m.runId, goalId: owningGoal?.id, label: m.id },
        goalId: owningGoal?.id ?? null,
        runId: m.runId,
        taskId: m.taskId,
        createdAt: m.finishedAt ?? m.createdAt,
      });
    }
  }

  for (const msg of messages) {
    if (msg.status === "open") {
      inbox.push({
        id: `inbox:msg:${msg.id}`,
        severity: "P2",
        kind: "open_message",
        title: `Open message ${msg.id} (${msg.fromTaskId} -> ${msg.toTaskId})`,
        detail: msg.question.slice(0, 140),
        ref: { kind: "message", id: msg.id, runId: msg.runId, label: msg.id },
        runId: msg.runId,
        taskId: msg.toTaskId,
        createdAt: msg.createdAt,
      });
    }
  }

  return sortInboxItems(inbox);
}

// ============================================================================
// 2. Facts Model & Default Unwindowed SQL Loader (zero writes, no DDL)
// ============================================================================

export interface TriageRunStatusEvent {
  seq: number;
  status: string | null;
  cause: string | null;
  reason: string | null;
  createdAt: number;
}

export interface TriageGoalSummary {
  id: string;
  chatSessionId: string | null;
  repoRoot: string;
  version: number;
  title: string;
  status: GoalRecord["status"];
  linkedRunId: string | null;
  reviewCycle: number;
  verificationMode: GoalRecord["verificationMode"];
  lastReviewSummary: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface TriageGoalReviewSummary {
  id: string;
  goalId: string;
  goalVersion: number;
  cycle: number;
  verdict: string;
  summary: string;
  source?: string;
  createdAt: number;
}

export interface TriageFacts {
  now: number;
  config: InboxTriageConfig;
  authoritative: boolean;
  /** Runs keyed by runId; `run.status` is the derived effective status (via pure `deriveRunStatus`). */
  runs: ReadonlyMap<string, RunRecord>;
  /** Raw stored `runs.status` from SQLite before `deriveRunStatus`. */
  rawRunStatus: ReadonlyMap<string, RunRecord["status"]>;
  /** All tasks keyed by taskId. */
  tasks: ReadonlyMap<string, TaskRecord>;
  /** Tasks grouped by runId. */
  tasksByRun: ReadonlyMap<string, TaskRecord[]>;
  /** Per-task executing liveness set (thin read-only liveness adapter, NOT run-wide). */
  liveTaskIds: ReadonlySet<string>;
  /** All escalations keyed by escalationId (including resolved, for R07 & reopen detection). */
  escalations: ReadonlyMap<string, EscalationRecord>;
  /** Escalations grouped by runId. */
  escalationsByRun: ReadonlyMap<string, EscalationRecord[]>;
  /** All merge_queue rows keyed by mergeId. */
  merges: ReadonlyMap<string, MergeQueueRecord>;
  /** Merges grouped by runId. */
  mergesByRun: ReadonlyMap<string, MergeQueueRecord[]>;
  /** All messages keyed by messageId. */
  messages: ReadonlyMap<string, MessageRecord>;
  /** Messages grouped by runId. */
  messagesByRun: ReadonlyMap<string, MessageRecord[]>;
  /** All goals keyed by goalId (empty when standalone DB has no goals table). */
  goals: ReadonlyMap<string, TriageGoalSummary>;
  /** All goals currently linked to a run via `goals.linked_run_id` (supports mixed/multiple goals). */
  goalsByRunId: ReadonlyMap<string, TriageGoalSummary[]>;
  /** Runs ever linked to a goal (current `linked_run_id` OR historical `goal.committed` events). */
  runsEverLinkedToGoal: ReadonlySet<string>;
  /** Reviews grouped by goalId. */
  reviewsByGoalId: ReadonlyMap<string, TriageGoalReviewSummary[]>;
  /** Newest `run.status` event per runId. */
  latestRunStatusEvent: ReadonlyMap<string, TriageRunStatusEvent>;
  /** In-memory quota pause status per runId. */
  quotaStatusByRunId: ReadonlyMap<string, RunQuotaPauseStatus | null>;
  /** Recorded R07 continuation grants keyed by escalationId (ordered by resolutionEventSeq ASC). */
  continuations: ReadonlyMap<string, ReadonlyArray<TriageContinuationRecord>>;
  /** Latest `escalation.updated` event per escalationId (seq + status) for Amendment 1 current-resolution checks. */
  latestEscalationUpdated?: ReadonlyMap<string, { seq: number; status: string | null }>;
  /** Latest `task.resumed` event created_at per runId (for dispatched resume postcondition verification). */
  latestTaskResumedAtByRun?: ReadonlyMap<string, number>;
  /** Persisted first-observed missing-quota timestamps keyed by causeId (R21). */
  missingQuotaSinceByCauseId: ReadonlyMap<string, number>;
  /** Task IDs created by triage itself (batch assignments); excluded from triage ("no triage of triage"). */
  triageOwnedTaskIds: ReadonlySet<string>;
  /** Latest non-`inbox.triage.*` event seq per taskId (for cause-specific owner progress). */
  latestEventSeqByTask: ReadonlyMap<string, number>;
  /** Latest non-`inbox.triage.*` event seq per runId. */
  latestEventSeqByRun: ReadonlyMap<string, number>;
  /** Latest `agent.steer_undelivered` event timestamp per taskId. */
  steerUndeliveredAtByTask: ReadonlyMap<string, number>;
  /** Optional explicit escalation generation map (for tests or typed linkage). */
  escalationGeneration?: ReadonlyMap<string, number>;
  /** Event seq when each escalation was created (from `escalation.created`). */
  escalationCreatedSeq?: ReadonlyMap<string, number>;
  /** Event seq when each task entered its current generation (from `task.resumed`). */
  taskCurrentGenStartedSeq?: ReadonlyMap<string, number>;
}

function parseJsonArray<T = string>(raw: unknown): T[] {
  if (typeof raw !== "string") return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? (parsed as T[]) : [];
  } catch {
    return [];
  }
}

function parseJsonObj(raw: unknown): Record<string, unknown> | null {
  if (typeof raw !== "string") return null;
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? (parsed as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

/**
 * Default unwindowed read-only SQL fact loader over `ControlPlaneDb.db`.
 * Uses pure `deriveRunStatus` (never `getEffectiveRun`), reads unwindowed source tables, and triggers
 * ZERO writes and ZERO DDL (reads triage tables only when `store.hasSchema()` is already true).
 */
export function loadTriageFacts(
  db: ControlPlaneDb,
  store: InboxTriageStore,
  opts: {
    now: number;
    config: InboxTriageConfig;
    authoritative: boolean;
    isTaskExecuting(taskId: string): boolean;
    getRunQuotaStatus(runId: string): RunQuotaPauseStatus | null;
  },
): TriageFacts {
  const sqlite = db.db;

  // 1. Load all tasks (unwindowed)
  const taskRows = sqlite.prepare("SELECT * FROM tasks ORDER BY created_at ASC, id ASC").all() as Record<
    string,
    unknown
  >[];
  const tasks = new Map<string, TaskRecord>();
  const tasksByRun = new Map<string, TaskRecord[]>();
  const liveTaskIds = new Set<string>();
  const liveRunsInMemory = new Set<string>();

  for (const r of taskRows) {
    const task: TaskRecord = {
      id: String(r.id),
      runId: String(r.run_id),
      parentId: r.parent_id == null ? null : String(r.parent_id),
      tier: Number(r.tier) as TaskRecord["tier"],
      kind: String(r.kind) as TaskRecord["kind"],
      title: String(r.title),
      objective: String(r.objective),
      criteria: String(r.criteria ?? ""),
      status: String(r.status) as TaskRecord["status"],
      profileId: String(r.profile_id),
      model: r.model == null ? null : String(r.model),
      effort: r.effort == null ? null : String(r.effort),
      readOnly: Boolean(r.read_only),
      maxTurns: Number(r.max_turns),
      generation: Number(r.generation),
      turnsUsed: Number(r.turns_used),
      contextUsed: Number(r.context_used),
      contextSize: Number(r.context_size),
      worktreeId: r.worktree_id == null ? null : String(r.worktree_id),
      sessionId: r.session_id == null ? null : String(r.session_id),
      summary: r.summary == null ? null : String(r.summary),
      handoffJson: r.handoff_json == null ? null : String(r.handoff_json),
      resultJson: r.result_json == null ? null : String(r.result_json),
      createdAt: Number(r.created_at),
      updatedAt: Number(r.updated_at),
    };
    tasks.set(task.id, task);
    let list = tasksByRun.get(task.runId);
    if (!list) {
      list = [];
      tasksByRun.set(task.runId, list);
    }
    list.push(task);

    if (opts.isTaskExecuting(task.id)) {
      liveTaskIds.add(task.id);
      liveRunsInMemory.add(task.runId);
    }
  }

  // 2. Load all runs (unwindowed) and derive pure effective status
  const runRows = sqlite.prepare("SELECT * FROM runs ORDER BY created_at DESC, id DESC").all() as Record<
    string,
    unknown
  >[];
  const runs = new Map<string, RunRecord>();
  const rawRunStatus = new Map<string, RunRecord["status"]>();
  const quotaStatusByRunId = new Map<string, RunQuotaPauseStatus | null>();

  for (const r of runRows) {
    const raw: RunRecord = {
      id: String(r.id),
      goal: String(r.goal),
      repoRoot: String(r.repo_root),
      baseBranch: String(r.base_branch),
      baseSha: String(r.base_sha),
      status: String(r.status) as RunRecord["status"],
      coordinationGroup: r.coordination_group == null ? null : String(r.coordination_group),
      isolationMode: (r.isolation_mode ? String(r.isolation_mode) : "standalone") as RunRecord["isolationMode"],
      createdAt: Number(r.created_at),
      updatedAt: Number(r.updated_at),
    };
    rawRunStatus.set(raw.id, raw.status);
    const runTasks = tasksByRun.get(raw.id) ?? [];
    const effectiveStatus = deriveRunStatus(raw, runTasks, liveRunsInMemory.has(raw.id));
    const derivedRun = effectiveStatus === raw.status ? raw : { ...raw, status: effectiveStatus };
    runs.set(raw.id, derivedRun);
    if (effectiveStatus === "paused_quota" || raw.status === "paused_quota") {
      quotaStatusByRunId.set(raw.id, opts.getRunQuotaStatus(raw.id));
    }
  }

  // 3. Load all escalations (unwindowed)
  const escRows = sqlite.prepare("SELECT * FROM escalations ORDER BY created_at DESC, id DESC").all() as Record<
    string,
    unknown
  >[];
  const escalations = new Map<string, EscalationRecord>();
  const escalationsByRun = new Map<string, EscalationRecord[]>();
  for (const r of escRows) {
    const esc: EscalationRecord = {
      id: String(r.id),
      runId: String(r.run_id),
      fromTaskId: String(r.from_task_id),
      toTaskId: r.to_task_id == null ? null : String(r.to_task_id),
      severity: String(r.severity) as EscalationRecord["severity"],
      category: String(r.category) as EscalationRecord["category"],
      title: String(r.title),
      evidencePaths: parseJsonArray<string>(r.evidence_paths_json),
      attempted: parseJsonArray<string>(r.attempted_json),
      suggestedSplits: parseJsonArray<string>(r.suggested_splits_json),
      blocking: Boolean(r.blocking),
      status: String(r.status) as EscalationRecord["status"],
      resolution: r.resolution == null ? null : String(r.resolution),
      createdAt: Number(r.created_at),
      ackedAt: r.acked_at == null ? null : Number(r.acked_at),
    };
    escalations.set(esc.id, esc);
    let list = escalationsByRun.get(esc.runId);
    if (!list) {
      list = [];
      escalationsByRun.set(esc.runId, list);
    }
    list.push(esc);
  }

  // 4. Load all merge_queue rows (unwindowed)
  const mergeRows = sqlite.prepare("SELECT * FROM merge_queue ORDER BY created_at DESC, id DESC").all() as Record<
    string,
    unknown
  >[];
  const merges = new Map<string, MergeQueueRecord>();
  const mergesByRun = new Map<string, MergeQueueRecord[]>();
  for (const r of mergeRows) {
    const m: MergeQueueRecord = {
      id: String(r.id),
      runId: String(r.run_id),
      taskId: String(r.task_id),
      worktreeId: String(r.worktree_id),
      branch: String(r.branch),
      status: String(r.status) as MergeQueueRecord["status"],
      log: r.log == null ? null : String(r.log),
      createdAt: Number(r.created_at),
      finishedAt: r.finished_at == null ? null : Number(r.finished_at),
    };
    merges.set(m.id, m);
    let list = mergesByRun.get(m.runId);
    if (!list) {
      list = [];
      mergesByRun.set(m.runId, list);
    }
    list.push(m);
  }

  // 5. Load all messages (unwindowed)
  const msgRows = sqlite.prepare("SELECT * FROM messages ORDER BY created_at DESC, id DESC").all() as Record<
    string,
    unknown
  >[];
  const messages = new Map<string, MessageRecord>();
  const messagesByRun = new Map<string, MessageRecord[]>();
  for (const r of msgRows) {
    const expiredReason = r.expired_reason == null ? null : String(r.expired_reason);
    const msg: MessageRecord = {
      id: String(r.id),
      runId: String(r.run_id),
      fromTaskId: String(r.from_task_id),
      toTaskId: String(r.to_task_id),
      question: String(r.question),
      answer: r.answer == null ? null : String(r.answer),
      ...(expiredReason !== null ? { expiredReason } : {}),
      status: String(r.status) as MessageRecord["status"],
      createdAt: Number(r.created_at),
      answeredAt: r.answered_at == null ? null : Number(r.answered_at),
    };
    messages.set(msg.id, msg);
    let list = messagesByRun.get(msg.runId);
    if (!list) {
      list = [];
      messagesByRun.set(msg.runId, list);
    }
    list.push(msg);
  }

  // 6. Load goals & goal_reviews (optional table when standalone ControlPlaneDb is used)
  const goals = new Map<string, TriageGoalSummary>();
  const goalsByRunId = new Map<string, TriageGoalSummary[]>();
  const runsEverLinkedToGoal = new Set<string>();
  const reviewsByGoalId = new Map<string, TriageGoalReviewSummary[]>();

  try {
    const goalRows = sqlite.prepare("SELECT * FROM goals ORDER BY updated_at DESC, id DESC").all() as Record<
      string,
      unknown
    >[];
    for (const r of goalRows) {
      const g: TriageGoalSummary = {
        id: String(r.id),
        chatSessionId: r.chat_session_id == null ? null : String(r.chat_session_id),
        repoRoot: String(r.repo_root),
        version: Number(r.version),
        title: String(r.title),
        status: String(r.status) as GoalRecord["status"],
        linkedRunId: r.linked_run_id == null ? null : String(r.linked_run_id),
        reviewCycle: Number(r.review_cycle ?? 0),
        verificationMode: String(r.verification_mode ?? "auto") as GoalRecord["verificationMode"],
        lastReviewSummary: r.last_review_summary == null ? null : String(r.last_review_summary),
        createdAt: Number(r.created_at),
        updatedAt: Number(r.updated_at),
      };
      goals.set(g.id, g);
      if (g.linkedRunId) {
        runsEverLinkedToGoal.add(g.linkedRunId);
        let list = goalsByRunId.get(g.linkedRunId);
        if (!list) {
          list = [];
          goalsByRunId.set(g.linkedRunId, list);
        }
        list.push(g);
      }
    }

    const revRows = sqlite.prepare("SELECT * FROM goal_reviews ORDER BY created_at DESC, id DESC").all() as Record<
      string,
      unknown
    >[];
    for (const r of revRows) {
      const rev: TriageGoalReviewSummary = {
        id: String(r.id),
        goalId: String(r.goal_id),
        goalVersion: Number(r.goal_version),
        cycle: Number(r.cycle),
        verdict: String(r.verdict),
        summary: String(r.summary),
        source: r.source == null ? undefined : String(r.source),
        createdAt: Number(r.created_at),
      };
      let list = reviewsByGoalId.get(rev.goalId);
      if (!list) {
        list = [];
        reviewsByGoalId.set(rev.goalId, list);
      }
      list.push(rev);
    }
  } catch (err) {
    if (!(err instanceof Error && /no such table:\s*(goals|goal_reviews)/i.test(err.message))) {
      throw err;
    }
  }

  // 7. Read relevant event facts (run.status, goal.committed, escalation.created, task.resumed, agent.steer_undelivered)
  const latestRunStatusEvent = new Map<string, TriageRunStatusEvent>();
  const runStatusRows = sqlite
    .prepare(
      `SELECT e.seq, e.run_id, e.payload_json, e.created_at
       FROM events e
       JOIN (
         SELECT run_id, MAX(seq) AS max_seq FROM events WHERE type = 'run.status' GROUP BY run_id
       ) m ON e.seq = m.max_seq`,
    )
    .all() as Array<{ seq: number; run_id: string; payload_json: string; created_at: number }>;
  for (const r of runStatusRows) {
    const p = parseJsonObj(r.payload_json);
    latestRunStatusEvent.set(String(r.run_id), {
      seq: Number(r.seq),
      status: typeof p?.status === "string" ? p.status : null,
      cause: typeof p?.cause === "string" ? p.cause : null,
      reason: typeof p?.reason === "string" ? p.reason : null,
      createdAt: Number(r.created_at),
    });
  }

  // Track runs ever linked to a goal via goal.committed / goal.amended / goal.reviewed events
  const goalEventRows = sqlite
    .prepare(`SELECT DISTINCT run_id FROM events WHERE type IN ('goal.committed', 'goal.amended', 'goal.reviewed')`)
    .all() as Array<{ run_id: string }>;
  for (const r of goalEventRows) {
    if (r.run_id) runsEverLinkedToGoal.add(String(r.run_id));
  }

  // Latest non-triage event seq per task and per run
  const latestEventSeqByTask = new Map<string, number>();
  const taskSeqRows = sqlite
    .prepare(
      `SELECT task_id, MAX(seq) AS max_seq
       FROM events
       WHERE task_id IS NOT NULL AND type NOT LIKE 'inbox.triage.%'
       GROUP BY task_id`,
    )
    .all() as Array<{ task_id: string; max_seq: number }>;
  for (const r of taskSeqRows) {
    latestEventSeqByTask.set(String(r.task_id), Number(r.max_seq));
  }

  const latestEventSeqByRun = new Map<string, number>();
  const runSeqRows = sqlite
    .prepare(
      `SELECT run_id, MAX(seq) AS max_seq
       FROM events
       WHERE type NOT LIKE 'inbox.triage.%'
       GROUP BY run_id`,
    )
    .all() as Array<{ run_id: string; max_seq: number }>;
  for (const r of runSeqRows) {
    latestEventSeqByRun.set(String(r.run_id), Number(r.max_seq));
  }

  // Latest agent.steer_undelivered per task
  const steerUndeliveredAtByTask = new Map<string, number>();
  const steerUndelivRows = sqlite
    .prepare(
      `SELECT task_id, MAX(created_at) AS max_at
       FROM events
       WHERE task_id IS NOT NULL AND type = 'agent.steer_undelivered'
       GROUP BY task_id`,
    )
    .all() as Array<{ task_id: string; max_at: number }>;
  for (const r of steerUndelivRows) {
    steerUndeliveredAtByTask.set(String(r.task_id), Number(r.max_at));
  }

  // Escalation creation seq & task generation start seq for generation-aware R06/R07 proofs
  const escalationCreatedSeq = new Map<string, number>();
  const escCreatedRows = sqlite
    .prepare(`SELECT seq, payload_json FROM events WHERE type = 'escalation.created'`)
    .all() as Array<{ seq: number; payload_json: string }>;
  for (const r of escCreatedRows) {
    const p = parseJsonObj(r.payload_json);
    if (typeof p?.id === "string") {
      escalationCreatedSeq.set(p.id, Number(r.seq));
    }
  }

  const taskCurrentGenStartedSeq = new Map<string, number>();
  const latestTaskResumedAtByRun = new Map<string, number>();
  const taskResumedRows = sqlite
    .prepare(`SELECT seq, run_id, task_id, payload_json, created_at FROM events WHERE type = 'task.resumed' ORDER BY seq ASC`)
    .all() as Array<{ seq: number; run_id: string; task_id: string | null; payload_json: string; created_at: number }>;
  for (const r of taskResumedRows) {
    if (r.run_id) {
      const prevAt = latestTaskResumedAtByRun.get(String(r.run_id)) ?? 0;
      if (Number(r.created_at) > prevAt) latestTaskResumedAtByRun.set(String(r.run_id), Number(r.created_at));
    }
    const tid = r.task_id ? String(r.task_id) : null;
    if (!tid) continue;
    const task = tasks.get(tid);
    const p = parseJsonObj(r.payload_json);
    const gen = typeof p?.generation === "number" ? p.generation : null;
    if (task && gen === task.generation) {
      taskCurrentGenStartedSeq.set(tid, Number(r.seq));
    }
  }

  // Latest escalation.updated event per escalationId (Amendment 1 current-resolution seq)
  const latestEscalationUpdated = new Map<string, { seq: number; status: string | null }>();
  const escUpdatedRows = sqlite
    .prepare(`SELECT seq, payload_json FROM events WHERE type = 'escalation.updated' ORDER BY seq ASC`)
    .all() as Array<{ seq: number; payload_json: string }>;
  for (const r of escUpdatedRows) {
    const p = parseJsonObj(r.payload_json);
    if (typeof p?.id === "string") {
      latestEscalationUpdated.set(p.id, {
        seq: Number(r.seq),
        status: typeof p.status === "string" ? p.status : null,
      });
    }
  }

  // 8. Triage-side read-only metadata (ONLY if schema already exists — zero DDL!)
  const continuations = new Map<string, TriageContinuationRecord[]>();
  const missingQuotaSinceByCauseId = new Map<string, number>();
  const triageOwnedTaskIds = new Set<string>();

  // Any task inside the dedicated inbox-triage Director task container is ALWAYS triage-owned ("no triage of triage").
  for (const task of tasks.values()) {
    if (isTriageBatchRun(task.runId)) {
      triageOwnedTaskIds.add(task.id);
    }
  }

  if (store.hasSchema()) {
    const contRows = sqlite
      .prepare("SELECT * FROM inbox_triage_continuation ORDER BY resolution_event_seq ASC")
      .all() as Record<string, unknown>[];
    for (const r of contRows) {
      const escId = String(r.escalation_id);
      const list = continuations.get(escId) ?? [];
      list.push({
        escalationId: escId,
        resolutionEventSeq: Number(r.resolution_event_seq),
        taskId: String(r.task_id),
        taskGeneration: Number(r.task_generation),
        grantedAt: Number(r.granted_at),
        consumedAt: r.consumed_at == null ? null : Number(r.consumed_at),
        revokedAt: r.revoked_at == null ? null : Number(r.revoked_at),
      });
      continuations.set(escId, list);
    }

    const causeRows = sqlite
      .prepare("SELECT cause_id, missing_since FROM inbox_triage_cause WHERE missing_since IS NOT NULL")
      .all() as Array<{ cause_id: string; missing_since: number }>;
    for (const r of causeRows) {
      missingQuotaSinceByCauseId.set(String(r.cause_id), Number(r.missing_since));
    }

    const batchTaskRows = sqlite
      .prepare("SELECT owner_task_id FROM inbox_triage_assignment WHERE kind = 'batch' AND owner_task_id IS NOT NULL")
      .all() as Array<{ owner_task_id: string }>;
    for (const r of batchTaskRows) {
      triageOwnedTaskIds.add(String(r.owner_task_id));
    }
  }

  return {
    now: opts.now,
    config: opts.config,
    authoritative: opts.authoritative,
    runs,
    rawRunStatus,
    tasks,
    tasksByRun,
    liveTaskIds,
    escalations,
    escalationsByRun,
    merges,
    mergesByRun,
    messages,
    messagesByRun,
    goals,
    goalsByRunId,
    runsEverLinkedToGoal,
    reviewsByGoalId,
    latestRunStatusEvent,
    quotaStatusByRunId,
    continuations,
    latestEscalationUpdated,
    latestTaskResumedAtByRun,
    missingQuotaSinceByCauseId,
    triageOwnedTaskIds,
    latestEventSeqByTask,
    latestEventSeqByRun,
    steerUndeliveredAtByTask,
    escalationCreatedSeq,
    taskCurrentGenStartedSeq,
  };
}

/**
 * Returns the newest `escalation.updated` event (seq & status) for `escalationId` in `runId`.
 * Used by `escalation.resolve` RPC right after a winning transition to bind Amendment 1 continuation grants.
 */
export function findLatestEscalationUpdatedEvent(
  db: ControlPlaneDb,
  runId: string,
  escalationId: string,
): { seq: number; status: string | null } | null {
  const rows = db.db
    .prepare(`SELECT seq, payload_json FROM events WHERE run_id = ? AND type = 'escalation.updated' ORDER BY seq DESC`)
    .all(runId) as Array<{ seq: number; payload_json: string }>;
  for (const r of rows) {
    const p = parseJsonObj(r.payload_json);
    if (p?.id === escalationId) {
      return { seq: Number(r.seq), status: typeof p.status === "string" ? p.status : null };
    }
  }
  return null;
}

/**
 * Checks whether `escalation.fromTaskId` is currently in `status === 'blocked'` with a proven same-generation
 * link to `escalation` (identical proof predicate as R06/R07). Returns `{ taskId, taskGeneration }` or null.
 */
export function findProvenBlockedTaskForEscalation(
  db: ControlPlaneDb,
  escalation: EscalationRecord,
): { taskId: string; taskGeneration: number } | null {
  if (!escalation.blocking) return null;
  const task = db.getTask(escalation.fromTaskId);
  if (!task || task.runId !== escalation.runId || task.status !== "blocked") return null;

  const candidateIds = new Set<string>();
  if (task.summary) {
    const match = BLOCKED_ON_ESC_RE.exec(task.summary);
    if (match?.[1]) candidateIds.add(match[1]);
  }
  if (task.resultJson) {
    const parsed = parseJsonObj(task.resultJson);
    if (typeof parsed?.escalationId === "string" && parsed.escalationId) {
      candidateIds.add(parsed.escalationId);
    }
    if (Array.isArray(parsed?.escalationIds)) {
      for (const id of parsed.escalationIds) {
        if (typeof id === "string" && id) candidateIds.add(id);
      }
    }
  }
  if (!candidateIds.has(escalation.id)) return null;

  // Generation check: reject escalations proven to belong to an earlier generation of the task.
  const escCreatedRows = db.db
    .prepare(`SELECT seq, payload_json FROM events WHERE run_id = ? AND type = 'escalation.created' ORDER BY seq DESC`)
    .all(escalation.runId) as Array<{ seq: number; payload_json: string }>;
  let escCreatedSeq: number | undefined;
  for (const r of escCreatedRows) {
    const p = parseJsonObj(r.payload_json);
    if (p?.id === escalation.id) {
      escCreatedSeq = Number(r.seq);
      break;
    }
  }
  if (escCreatedSeq !== undefined) {
    const resumedRows = db.db
      .prepare(`SELECT seq, payload_json FROM events WHERE task_id = ? AND type = 'task.resumed' ORDER BY seq DESC`)
      .all(task.id) as Array<{ seq: number; payload_json: string }>;
    for (const r of resumedRows) {
      const p = parseJsonObj(r.payload_json);
      if (typeof p?.generation === "number" && p.generation === task.generation) {
        if (escCreatedSeq < Number(r.seq)) return null;
        break;
      }
    }
  }

  return { taskId: task.id, taskGeneration: task.generation };
}

// ============================================================================
// 3. Veto Layer (Contract Section 6 — pure, deterministic)
// ============================================================================

export type VetoCode =
  | "mode_not_on"
  | "authority_lost"
  | "director_container"
  | "run_stopped"
  | "run_paused_quota"
  | "goal_paused"
  | "goal_terminal"
  | "target_changed"
  | "blocking_escalation"
  | "done_review_task";

export interface VetoTarget {
  runId?: string | null;
  goalId?: string | null;
  taskId?: string | null;
  expectedGeneration?: number | null;
  expectedStatus?: string | null;
  /** True for rules whose purpose is handling quota pause (R20/R21). */
  allowQuotaPaused?: boolean;
  /** True for rules whose purpose is closing items on settled goals (R00). */
  allowSettledGoal?: boolean;
  /** True for rules whose purpose is expiring dead messages on stopped runs (R18). */
  allowStoppedRun?: boolean;
  /** Blocking escalations already resolved for this target (e.g. R07). */
  ignoreBlockingEscalationIds?: ReadonlySet<string>;
}

export interface VetoResult {
  vetoed: boolean;
  code?: VetoCode;
  holdReason?: string;
  releasePredicate?: string;
}

/**
 * Pure veto evaluator (Contract Section 6).
 * Checked before every claim AND again before every activation.
 * A veto blocks ACTIONS only; independent, still-actionable human asks stay surfaced.
 */
export function evaluateVeto(
  target: VetoTarget,
  facts: TriageFacts,
  opts?: { ignoreModeAndAuthority?: boolean },
): VetoResult {
  if (!opts?.ignoreModeAndAuthority) {
    if (facts.config.mode !== "on") {
      return {
        vetoed: true,
        code: "mode_not_on",
        holdReason: `inbox.mode is '${facts.config.mode}'`,
        releasePredicate: "inbox.mode === 'on'",
      };
    }
    if (!facts.authoritative) {
      return {
        vetoed: true,
        code: "authority_lost",
        holdReason: "scheduler authority is not held on this host",
        releasePredicate: "schedulerHost === hostname and not frozen",
      };
    }
  }

  if (target.runId) {
    if (isDirectorTaskContainer(target.runId)) {
      return {
        vetoed: true,
        code: "director_container",
        holdReason: `target run ${target.runId} is a director-tasks container`,
        releasePredicate: "non-container run",
      };
    }
    const run = facts.runs.get(target.runId);
    if (run) {
      if (run.status === "stopped" && !target.allowStoppedRun) {
        return {
          vetoed: true,
          code: "run_stopped",
          holdReason: `run ${run.id} is stopped`,
          releasePredicate: `run ${run.id} leaves status 'stopped'`,
        };
      }
      if (run.status === "paused_quota" && !target.allowQuotaPaused) {
        return {
          vetoed: true,
          code: "run_paused_quota",
          holdReason: `run ${run.id} is paused for quota`,
          releasePredicate: `run ${run.id} leaves status 'paused_quota'`,
        };
      }
    }
  }

  if (target.goalId) {
    const goal = facts.goals.get(target.goalId);
    if (goal) {
      if (goal.status === "paused") {
        return {
          vetoed: true,
          code: "goal_paused",
          holdReason: `linked goal ${goal.id} is paused`,
          releasePredicate: `goal ${goal.id} leaves status 'paused'`,
        };
      }
      if (!target.allowSettledGoal && TERMINAL_GOAL_STATUS_SET.has(goal.status)) {
        return {
          vetoed: true,
          code: "goal_terminal",
          holdReason: `linked goal ${goal.id} is ${goal.status}`,
          releasePredicate: `goal ${goal.id} is non-terminal`,
        };
      }
    }
  }

  if (target.runId) {
    const linkedGoals = facts.goalsByRunId.get(target.runId) ?? [];
    const pausedGoal = linkedGoals.find((g) => g.status === "paused");
    if (pausedGoal) {
      return {
        vetoed: true,
        code: "goal_paused",
        holdReason: `linked goal ${pausedGoal.id} is paused`,
        releasePredicate: `goal ${pausedGoal.id} leaves status 'paused'`,
      };
    }
    if (!target.allowSettledGoal && linkedGoals.length > 0 && linkedGoals.every((g) => TERMINAL_GOAL_STATUS_SET.has(g.status))) {
      return {
        vetoed: true,
        code: "goal_terminal",
        holdReason: `all linked goals (${linkedGoals.map((g) => g.id).join(", ")}) are terminal`,
        releasePredicate: "non-terminal linked goal",
      };
    }
  }

  if (target.taskId) {
    const task = facts.tasks.get(target.taskId);
    if (!task) {
      return {
        vetoed: true,
        code: "target_changed",
        holdReason: `target task ${target.taskId} no longer exists`,
        releasePredicate: "target task exists",
      };
    }
    if (isDoneReviewTask(task)) {
      return {
        vetoed: true,
        code: "done_review_task",
        holdReason: `target task ${task.id} is a Done-Review task`,
        releasePredicate: "non-Done-Review task",
      };
    }
    if (target.expectedGeneration != null && task.generation !== target.expectedGeneration) {
      return {
        vetoed: true,
        code: "target_changed",
        holdReason: `task ${task.id} generation changed (${target.expectedGeneration} -> ${task.generation})`,
        releasePredicate: "re-evaluate at current task generation",
      };
    }
    if (target.expectedStatus != null && task.status !== target.expectedStatus) {
      return {
        vetoed: true,
        code: "target_changed",
        holdReason: `task ${task.id} status changed (${target.expectedStatus} -> ${task.status})`,
        releasePredicate: "re-evaluate at current task status",
      };
    }
    const runEscs = facts.escalationsByRun.get(task.runId) ?? [];
    const taskGenStartedSeq = facts.taskCurrentGenStartedSeq?.get(task.id);
    const blockingEsc = runEscs.find((e) => {
      if ((e.status !== "open" && e.status !== "acked") || !e.blocking || e.fromTaskId !== task.id) return false;
      if (target.ignoreBlockingEscalationIds?.has(e.id)) return false;
      const explicitGen = facts.escalationGeneration?.get(e.id);
      if (explicitGen !== undefined && explicitGen !== task.generation) return false;
      if (explicitGen === undefined && taskGenStartedSeq !== undefined) {
        const escSeq = facts.escalationCreatedSeq?.get(e.id);
        if (escSeq !== undefined && escSeq < taskGenStartedSeq) return false;
      }
      return true;
    });
    if (blockingEsc) {
      return {
        vetoed: true,
        code: "blocking_escalation",
        holdReason: `unresolved blocking escalation ${blockingEsc.id} on task ${task.id}`,
        releasePredicate: `escalation ${blockingEsc.id} resolved`,
      };
    }
  }

  return { vetoed: false };
}

// ============================================================================
// 4. Pure Router (Rules R00–R22, Contract Section 7)
// ============================================================================

export type TriageActionSpec =
  | {
      primitive: "reconcile_settled_goal_escalations";
      targetKind: "escalation";
      targetId: string;
      targetEpoch: string;
    }
  | {
      primitive: "expire_message";
      targetKind: "message";
      targetId: string;
      targetEpoch: string;
      reason: string;
      notifyAskerTaskId: string | null;
    }
  | {
      primitive: "ledger_supersede";
      targetKind: string;
      targetId: string;
      targetEpoch: string;
      reason: string;
    }
  | {
      primitive: "ledger_proof";
      targetKind: "merge";
      targetId: string;
      targetEpoch: string;
      proof: string;
    }
  | {
      primitive: "resume_task";
      targetKind: "task";
      targetId: string;
      targetEpoch: string;
      expectedGeneration: number;
      escalationIds: string[];
      continuationGrants: Array<{ escalationId: string; resolutionEventSeq: number }>;
    }
  | {
      primitive: "resume_goal";
      targetKind: "goal";
      targetId: string;
      targetEpoch: string;
      runId: string;
    }
  | {
      primitive: "resume_run";
      targetKind: "run";
      targetId: string;
      targetEpoch: string;
    }
  | {
      primitive: "steer_task";
      targetKind: "task";
      targetId: string;
      targetEpoch: string;
      message: string;
      fallbackToBatch: true;
    }
  | {
      primitive: "batch_investigate";
      targetKind: "scope";
      targetId: string;
      targetEpoch: string;
      scope: string;
    };

export interface TriageDecision {
  rule: string;
  itemKind: InboxItem["kind"];
  sourceId: string;
  sourceEpoch: string;
  causeId: string;
  /** Additional primary cause IDs when an item (e.g. R06/R11) depends on multiple independent causes. */
  additionalCauseIds?: string[];
  role: "primary" | "derived";
  disposition: InboxTriageDisposition;
  runId: string | null;
  goalId: string | null;
  repoRoot: string | null;
  ownerKind: InboxTriageOwnerKind | null;
  ownerId: string | null;
  ownerEpoch: string | null;
  nextAction: string;
  nextCheckAt?: number | null;
  holdReason?: string | null;
  releasePredicate?: string | null;
  ask?: string | null;
  blockedWork?: string | null;
  action?: TriageActionSpec | null;
  /** True when the item originates from triage's own batch work and must be ignored. */
  ignored?: boolean;
  /** True when R21 observes missing quota state and needs a durable missing_since stamp. */
  recordMissingSince?: boolean;
}

export function isTaskLive(taskId: string | null | undefined, facts: TriageFacts): boolean {
  if (!taskId) return false;
  const task = facts.tasks.get(taskId);
  if (!task) return false;
  return LIVE_TASK_STATUS_SET.has(task.status) && facts.liveTaskIds.has(taskId);
}

export function allLinkedGoalsTerminal(runId: string | null | undefined, facts: TriageFacts): boolean {
  if (!runId) return false;
  const linked = facts.goalsByRunId.get(runId) ?? [];
  return linked.length >= 1 && linked.every((g) => TERMINAL_GOAL_STATUS_SET.has(g.status));
}

export function computeSourceEpoch(item: InboxItem, facts: TriageFacts): string {
  const sourceId = item.ref.id;
  switch (item.kind) {
    case "escalation":
    case "merge_bounced":
    case "open_message":
      return "";
    case "task_blocked":
    case "task_failed": {
      const task = facts.tasks.get(sourceId);
      return task ? `g${task.generation}` : "g0";
    }
    case "goal_awaiting_human": {
      const goal = facts.goals.get(sourceId);
      return goal ? `v${goal.version}:c${goal.reviewCycle}` : "v0:c0";
    }
    case "run_failed": {
      const ev = facts.latestRunStatusEvent.get(sourceId);
      if (ev) return `s${ev.seq}`;
      const run = facts.runs.get(sourceId);
      return run ? `u${run.updatedAt}` : "u0";
    }
    case "quota_paused": {
      const qp = facts.quotaStatusByRunId.get(sourceId);
      if (qp?.pausedAt != null) return `p${qp.pausedAt}`;
      const run = facts.runs.get(sourceId);
      return run ? `u${run.updatedAt}` : "u0";
    }
  }
}

export function findProvenBlockingEscalations(
  task: TaskRecord,
  facts: TriageFacts,
): { unresolved: EscalationRecord[]; resolved: EscalationRecord[] } {
  const candidateIds = new Set<string>();

  if (task.summary) {
    const match = BLOCKED_ON_ESC_RE.exec(task.summary);
    if (match?.[1]) candidateIds.add(match[1]);
  }
  if (task.resultJson) {
    const parsed = parseJsonObj(task.resultJson);
    if (typeof parsed?.escalationId === "string" && parsed.escalationId) {
      candidateIds.add(parsed.escalationId);
    }
    if (Array.isArray(parsed?.escalationIds)) {
      for (const id of parsed.escalationIds) {
        if (typeof id === "string" && id) candidateIds.add(id);
      }
    }
  }

  const unresolved: EscalationRecord[] = [];
  const resolved: EscalationRecord[] = [];
  const taskGenStartedSeq = facts.taskCurrentGenStartedSeq?.get(task.id);

  for (const escId of candidateIds) {
    const esc = facts.escalations.get(escId);
    if (!esc) continue;
    if (esc.runId !== task.runId || esc.fromTaskId !== task.id) continue;
    if (!esc.blocking) continue;

    // Generation check: reject escalations proven to belong to a prior generation.
    const explicitGen = facts.escalationGeneration?.get(esc.id);
    if (explicitGen !== undefined && explicitGen !== task.generation) continue;
    if (explicitGen === undefined && taskGenStartedSeq !== undefined) {
      const escSeq = facts.escalationCreatedSeq?.get(esc.id);
      if (escSeq !== undefined && escSeq < taskGenStartedSeq) continue;
    }

    if (esc.status === "open" || esc.status === "acked") {
      unresolved.push(esc);
    } else if (esc.status === "resolved") {
      resolved.push(esc);
    }
  }

  const byAge = (a: EscalationRecord, b: EscalationRecord) => a.createdAt - b.createdAt || a.id.localeCompare(b.id);
  unresolved.sort(byAge);
  resolved.sort(byAge);
  return { unresolved, resolved };
}

/**
 * Finds the best live capable task in the same run for ladder rung A:
 * addressee (if passed and live), else nearest live ancestor of `fromTaskId`, else live Tier 1.
 * Excludes Done-Review tasks and triage-owned batch tasks.
 */
export function findLadderATask(
  runId: string,
  fromTaskId: string | null | undefined,
  facts: TriageFacts,
  addresseeTaskId?: string | null,
): TaskRecord | null {
  const isCapable = (t: TaskRecord | undefined): boolean =>
    Boolean(
      t &&
        t.runId === runId &&
        isTaskLive(t.id, facts) &&
        !isDoneReviewTask(t) &&
        !facts.triageOwnedTaskIds.has(t.id),
    );

  if (addresseeTaskId) {
    const addr = facts.tasks.get(addresseeTaskId);
    if (addr && isCapable(addr)) return addr;
  }

  // Nearest live ancestor of fromTaskId
  const visited = new Set<string>();
  let cursor: string | null = fromTaskId ? facts.tasks.get(fromTaskId)?.parentId ?? null : null;
  while (cursor && !visited.has(cursor)) {
    visited.add(cursor);
    const parent: TaskRecord | undefined = facts.tasks.get(cursor);
    if (!parent) break;
    if (isCapable(parent)) return parent;
    cursor = parent.parentId;
  }

  // Fallback: live Tier 1 orchestrator of the same run
  const runTasks = facts.tasksByRun.get(runId) ?? [];
  const tier1 = runTasks.find((t) => t.tier === 1 && t.profileId !== "director" && isCapable(t));
  return tier1 ?? null;
}

export function findSupersededMergedSubmission(
  bounced: MergeQueueRecord,
  facts: TriageFacts,
): MergeQueueRecord | null {
  const runMerges = facts.mergesByRun.get(bounced.runId) ?? [];
  const bounceTime = bounced.finishedAt ?? bounced.createdAt;
  const candidates = runMerges.filter((m) => {
    if (m.id === bounced.id) return false;
    if (m.status !== "merged") return false;
    if (m.runId !== bounced.runId) return false;
    if (m.taskId !== bounced.taskId) return false;
    if (m.worktreeId !== bounced.worktreeId) return false;
    if (m.branch !== bounced.branch) return false;
    // Strict ordering after the bounce; equal/ambiguous timestamps are NOT proof.
    if (m.createdAt <= bounced.createdAt) return false;
    const mergedTime = m.finishedAt ?? m.createdAt;
    if (mergedTime <= bounceTime) return false;
    return true;
  });
  if (candidates.length === 0) return null;
  candidates.sort((a, b) => a.createdAt - b.createdAt || a.id.localeCompare(b.id));
  return candidates[0]!;
}

export function formatBlockedWork(
  runId: string | null,
  goalId: string | null,
  facts: TriageFacts,
): string {
  const parts: string[] = [];
  const goal =
    (goalId ? facts.goals.get(goalId) : null) ??
    (runId ? (facts.goalsByRunId.get(runId) ?? [])[0] ?? null : null);
  if (goal) {
    parts.push(`${goal.id} v${goal.version} (${goal.status})`);
  }
  const run = runId ? facts.runs.get(runId) : null;
  if (run) {
    parts.push(`${run.id} (${run.status})`);
  }
  if (runId) {
    const blocked = (facts.tasksByRun.get(runId) ?? [])
      .filter((t) => t.status === "blocked" || t.status === "returned_for_rescope")
      .map((t) => t.id);
    if (blocked.length > 0) {
      parts.push(`tasks ${blocked.join(", ")} blocked`);
    }
  }
  return parts.join(" · ") || (runId ?? goalId ?? "unlinked work");
}

function resolveRepoRoot(runId: string | null, goalId: string | null, facts: TriageFacts): string | null {
  if (runId) {
    const run = facts.runs.get(runId);
    if (run?.repoRoot) return run.repoRoot;
  }
  if (goalId) {
    const goal = facts.goals.get(goalId);
    if (goal?.repoRoot) return goal.repoRoot;
  }
  return null;
}

function resolvePrimaryGoalId(runId: string | null, explicitGoalId: string | null | undefined, facts: TriageFacts): string | null {
  if (explicitGoalId) return explicitGoalId;
  if (!runId) return null;
  const linked = facts.goalsByRunId.get(runId) ?? [];
  return linked[0]?.id ?? null;
}

function isRunRestartCasualty(runId: string, facts: TriageFacts): boolean {
  const ev = facts.latestRunStatusEvent.get(runId);
  if (ev?.cause !== "daemon_restart") return false;
  const tasks = facts.tasksByRun.get(runId) ?? [];
  return tasks.some((t) => t.status === "failed" && t.summary === DAEMON_RESTART_SUMMARY);
}

function buildLadderDecision(params: {
  rule: string;
  itemKind: InboxItem["kind"];
  sourceId: string;
  sourceEpoch: string;
  causeId: string;
  runId: string | null;
  goalId: string | null;
  repoRoot: string | null;
  steerTarget: TaskRecord | null;
  steerMessage: string;
  batchNextAction: string;
}): TriageDecision {
  const scope = params.repoRoot ?? "";
  if (params.steerTarget) {
    return {
      rule: params.rule,
      itemKind: params.itemKind,
      sourceId: params.sourceId,
      sourceEpoch: params.sourceEpoch,
      causeId: params.causeId,
      role: "primary",
      disposition: "routine_owned",
      runId: params.runId,
      goalId: params.goalId,
      repoRoot: params.repoRoot,
      ownerKind: "task",
      ownerId: params.steerTarget.id,
      ownerEpoch: `g${params.steerTarget.generation}`,
      nextAction: `steer ${params.steerTarget.id} to handle ${params.sourceId}`,
      action: {
        primitive: "steer_task",
        targetKind: "task",
        targetId: params.steerTarget.id,
        targetEpoch: `g${params.steerTarget.generation}`,
        message: params.steerMessage,
        fallbackToBatch: true,
      },
    };
  }
  return {
    rule: params.rule,
    itemKind: params.itemKind,
    sourceId: params.sourceId,
    sourceEpoch: params.sourceEpoch,
    causeId: params.causeId,
    role: "primary",
    disposition: "routine_owned",
    runId: params.runId,
    goalId: params.goalId,
    repoRoot: params.repoRoot,
    ownerKind: "batch",
    ownerId: null,
    ownerEpoch: null,
    nextAction: params.batchNextAction,
    action: {
      primitive: "batch_investigate",
      targetKind: "scope",
      targetId: scope,
      targetEpoch: "",
      scope,
    },
  };
}

function makeHoldDecision(params: {
  rule: string;
  itemKind: InboxItem["kind"];
  sourceId: string;
  sourceEpoch: string;
  causeId: string;
  runId: string | null;
  goalId: string | null;
  repoRoot: string | null;
  veto: VetoResult;
  ownerKind?: InboxTriageOwnerKind | null;
  ownerId?: string | null;
  nextAction?: string;
}): TriageDecision {
  const reason = params.veto.holdReason ?? "held by veto";
  return {
    rule: params.rule,
    itemKind: params.itemKind,
    sourceId: params.sourceId,
    sourceEpoch: params.sourceEpoch,
    causeId: params.causeId,
    role: "primary",
    disposition: "held",
    runId: params.runId,
    goalId: params.goalId,
    repoRoot: params.repoRoot,
    ownerKind: params.ownerKind ?? "human",
    ownerId: params.ownerId ?? "operator",
    ownerEpoch: null,
    nextAction: params.nextAction ?? `${reason}; awaiting release (${params.veto.releasePredicate ?? "condition cleared"})`,
    holdReason: reason,
    releasePredicate: params.veto.releasePredicate ?? null,
    action: null,
  };
}

function findProvenVerificationGoalFold(esc: EscalationRecord, facts: TriageFacts): TriageGoalSummary | null {
  if (esc.category !== "verification_failed" || esc.toTaskId !== null) return null;
  const linked = facts.goalsByRunId.get(esc.runId) ?? [];
  for (const g of linked) {
    if (g.status !== "awaiting_human" || g.reviewCycle <= 0) continue;
    const reviews = facts.reviewsByGoalId.get(g.id) ?? [];
    const matchingReview = reviews.find((r) => r.goalVersion === g.version && r.cycle === g.reviewCycle);
    if (!matchingReview) continue;
    // Escalation must have been created during or after this exact review cycle
    if (esc.createdAt >= matchingReview.createdAt - 5_000) {
      return g;
    }
  }
  return null;
}

/**
 * Pure, total, deterministic router implementing rules R00–R22 as amended in Contract Section 7.
 * Performs no LLM calls, no I/O, and no wall-clock reads (`now` comes strictly from `facts.now`).
 */
export function routeItem(item: InboxItem, facts: TriageFacts): TriageDecision {
  const sourceId = item.ref.id;
  const sourceEpoch = computeSourceEpoch(item, facts);
  const defaultCauseId = makeCauseId(item.kind, sourceId);
  const runId = item.runId ?? null;
  const goalId = resolvePrimaryGoalId(runId, item.goalId, facts);
  const repoRoot = resolveRepoRoot(runId, goalId, facts);

  // Contract Section 2: work created by triage itself never becomes a new cause ("no triage of triage").
  if (item.taskId && facts.triageOwnedTaskIds.has(item.taskId)) {
    return {
      rule: "TRIAGE_OWNED",
      itemKind: item.kind,
      sourceId,
      sourceEpoch,
      causeId: defaultCauseId,
      role: "primary",
      disposition: "held",
      runId,
      goalId,
      repoRoot,
      ownerKind: "daemon",
      ownerId: "inbox-triage",
      ownerEpoch: null,
      nextAction: "managed by triage assignment",
      ignored: true,
      action: null,
    };
  }

  // R00: superseded by settled goals — ALL goals linked to the run are achieved/cancelled (>= 1).
  if (runId && allLinkedGoalsTerminal(runId, facts)) {
    if (item.kind === "escalation") {
      return {
        rule: "R00",
        itemKind: item.kind,
        sourceId,
        sourceEpoch,
        causeId: defaultCauseId,
        role: "primary",
        disposition: "routine_auto",
        runId,
        goalId,
        repoRoot,
        ownerKind: "daemon",
        ownerId: "inbox-triage",
        ownerEpoch: null,
        nextAction: "reconcile settled-goal escalation",
        action: {
          primitive: "reconcile_settled_goal_escalations",
          targetKind: "escalation",
          targetId: sourceId,
          targetEpoch: sourceEpoch,
        },
      };
    }
    if (item.kind === "open_message") {
      const msg = facts.messages.get(sourceId);
      return {
        rule: "R00",
        itemKind: item.kind,
        sourceId,
        sourceEpoch,
        causeId: defaultCauseId,
        role: "primary",
        disposition: "routine_auto",
        runId,
        goalId,
        repoRoot,
        ownerKind: "daemon",
        ownerId: "inbox-triage",
        ownerEpoch: null,
        nextAction: "expire message (all linked goals settled)",
        action: {
          primitive: "expire_message",
          targetKind: "message",
          targetId: sourceId,
          targetEpoch: sourceEpoch,
          reason: "all linked goals settled",
          notifyAskerTaskId: msg && isTaskLive(msg.fromTaskId, facts) ? msg.fromTaskId : null,
        },
      };
    }
    return {
      rule: "R00",
      itemKind: item.kind,
      sourceId,
      sourceEpoch,
      causeId: defaultCauseId,
      role: "primary",
      disposition: "routine_auto",
      runId,
      goalId,
      repoRoot,
      ownerKind: "daemon",
      ownerId: "inbox-triage",
      ownerEpoch: null,
      nextAction: "supersede in ledger (all linked goals settled)",
      action: {
        primitive: "ledger_supersede",
        targetKind: item.ref.kind,
        targetId: sourceId,
        targetEpoch: sourceEpoch,
        reason: "all linked goals settled",
      },
    };
  }

  // Per-kind routing (R02–R22), respecting Contract Section 6 veto layer:
  // independent still-actionable human asks (R02, R15, R21 manual) stay surfaced even on stopped/paused runs,
  // while routine actions respect stopped/paused/quota vetoes (R01/held).
  switch (item.kind) {
    case "escalation": {
      const esc = facts.escalations.get(sourceId);
      if (!esc) {
        return buildLadderDecision({
          rule: "R22",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          runId,
          goalId,
          repoRoot,
          steerTarget: null,
          steerMessage: "",
          batchNextAction: `investigate missing escalation row ${sourceId}`,
        });
      }

      // R02: unresolved P0 escalation (blocking or nonblocking!)
      if (esc.severity === "P0") {
        const foldedGoal = findProvenVerificationGoalFold(esc, facts);
        if (foldedGoal) {
          return {
            rule: "R02",
            itemKind: item.kind,
            sourceId,
            sourceEpoch,
            causeId: makeCauseId("goal_awaiting_human", foldedGoal.id),
            role: "derived",
            disposition: "derived",
            runId,
            goalId: foldedGoal.id,
            repoRoot,
            ownerKind: "human",
            ownerId: "operator",
            ownerEpoch: null,
            nextAction: `folded under ${foldedGoal.id} v${foldedGoal.version} cycle ${foldedGoal.reviewCycle}`,
            action: null,
          };
        }
        const evidence = esc.evidencePaths.length > 0 ? ` · Evidence: ${esc.evidencePaths.join(", ")}` : "";
        const blockedWork = formatBlockedWork(esc.runId, goalId, facts);
        const ask = `[P0/${esc.category}] ${esc.title}${evidence}${esc.blocking ? " · blocking" : " · nonblocking"}`;
        return {
          rule: "R02",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          role: "primary",
          disposition: "human_blocker",
          runId: esc.runId,
          goalId,
          repoRoot,
          ownerKind: "human",
          ownerId: "operator",
          ownerEpoch: null,
          nextAction: "await human decision on P0 escalation",
          ask,
          blockedWork,
          action: null,
        };
      }

      // Source veto check for routine P1/P2 escalations (R01 stopped run, paused goal, paused_quota, director container)
      const sourceVeto = evaluateVeto({ runId: esc.runId, goalId }, facts, { ignoreModeAndAuthority: true });
      if (sourceVeto.vetoed) {
        return makeHoldDecision({
          rule: sourceVeto.code === "run_stopped" ? "R01" : "R04",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          runId: esc.runId,
          goalId,
          repoRoot,
          veto: sourceVeto,
        });
      }

      // R03: P1/P2 escalation with live addressee task (no steer sent; it already received escalation.created)
      if (esc.toTaskId && isTaskLive(esc.toTaskId, facts)) {
        const toTask = facts.tasks.get(esc.toTaskId)!;
        return {
          rule: "R03",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          role: "primary",
          disposition: "routine_owned",
          runId: esc.runId,
          goalId,
          repoRoot,
          ownerKind: "task",
          ownerId: toTask.id,
          ownerEpoch: `g${toTask.generation}`,
          nextAction: `await live addressee ${toTask.id} resolution`,
          action: null,
        };
      }

      const run = facts.runs.get(esc.runId);
      // R04: P1/P2 escalation, addressee null/settled, run active -> ladder A then B
      if (run?.status === "active") {
        const steerTarget = findLadderATask(esc.runId, esc.fromTaskId, facts);
        return buildLadderDecision({
          rule: "R04",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          runId: esc.runId,
          goalId,
          repoRoot,
          steerTarget,
          steerMessage: `Triage assignment for ${esc.id} (${esc.severity}/${esc.category}) from ${esc.fromTaskId}: ${esc.title}. Investigate and resolve or escalate with concrete evidence.`,
          batchNextAction: `investigate ${esc.id} (${esc.severity}/${esc.category}) via batch`,
        });
      }

      // R05: P1/P2 escalation, run settled -> ladder B (no direct human surfacing by category or age)
      return buildLadderDecision({
        rule: "R05",
        itemKind: item.kind,
        sourceId,
        sourceEpoch,
        causeId: defaultCauseId,
        runId: esc.runId,
        goalId,
        repoRoot,
        steerTarget: null,
        steerMessage: "",
        batchNextAction: `investigate settled-run escalation ${esc.id} (${esc.severity}/${esc.category}) via batch`,
      });
    }

    case "task_blocked": {
      const task = facts.tasks.get(sourceId);
      if (!task) {
        return buildLadderDecision({
          rule: "R22",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          runId,
          goalId,
          repoRoot,
          steerTarget: null,
          steerMessage: "",
          batchNextAction: `investigate missing task ${sourceId}`,
        });
      }

      const proven = findProvenBlockingEscalations(task, facts);

      // R06: task_blocked with proven link to an unresolved blocking escalation (same generation)
      if (proven.unresolved.length >= 1) {
        const primary = proven.unresolved[0]!;
        return {
          rule: "R06",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: makeCauseId("escalation", primary.id),
          additionalCauseIds: proven.unresolved.slice(1).map((e) => makeCauseId("escalation", e.id)),
          role: "derived",
          disposition: "derived",
          runId: task.runId,
          goalId,
          repoRoot,
          ownerKind: null,
          ownerId: null,
          ownerEpoch: null,
          nextAction: `derived under blocking escalation ${primary.id}`,
          action: null,
        };
      }

      // R07: task_blocked (NOT returned_for_rescope) whose proven blocking escalation(s) are all resolved (>= 1)
      if (task.status === "blocked" && proven.resolved.length >= 1) {
        const ignoreIds = new Set(proven.resolved.map((e) => e.id));
        const sourceVeto = evaluateVeto(
          {
            runId: task.runId,
            goalId,
            taskId: task.id,
            expectedGeneration: task.generation,
            expectedStatus: "blocked",
            ignoreBlockingEscalationIds: ignoreIds,
          },
          facts,
          { ignoreModeAndAuthority: true },
        );
        if (sourceVeto.vetoed) {
          return makeHoldDecision({
            rule: sourceVeto.code === "run_stopped" ? "R01" : "R07",
            itemKind: item.kind,
            sourceId,
            sourceEpoch,
            causeId: defaultCauseId,
            runId: task.runId,
            goalId,
            repoRoot,
            veto: sourceVeto,
          });
        }

        const matchedGrants: Array<{ escalationId: string; resolutionEventSeq: number }> = [];
        const hasRecordedContinue = proven.resolved.every((esc) => {
          if (esc.resolution && HOLD_OR_CANCEL_RESOLUTION_RE.test(esc.resolution)) return false;
          const latestEv = facts.latestEscalationUpdated?.get(esc.id);
          if (latestEv && latestEv.status !== "resolved") return false;
          const grants = facts.continuations.get(esc.id) ?? [];
          const valid = grants.find(
            (g) =>
              g.consumedAt === null &&
              g.revokedAt === null &&
              g.taskId === task.id &&
              g.taskGeneration === task.generation &&
              (latestEv !== undefined ? g.resolutionEventSeq === latestEv.seq : true),
          );
          if (!valid) return false;
          matchedGrants.push({ escalationId: esc.id, resolutionEventSeq: valid.resolutionEventSeq });
          return true;
        });

        if (hasRecordedContinue && matchedGrants.length === proven.resolved.length) {
          return {
            rule: "R07",
            itemKind: item.kind,
            sourceId,
            sourceEpoch,
            causeId: defaultCauseId,
            role: "primary",
            disposition: "routine_auto",
            runId: task.runId,
            goalId,
            repoRoot,
            ownerKind: "daemon",
            ownerId: "inbox-triage",
            ownerEpoch: null,
            nextAction: `resume blocked task ${task.id} at generation ${task.generation}`,
            action: {
              primitive: "resume_task",
              targetKind: "task",
              targetId: task.id,
              targetEpoch: sourceEpoch,
              expectedGeneration: task.generation,
              escalationIds: proven.resolved.map((e) => e.id),
              continuationGrants: matchedGrants,
            },
          };
        }

        // No recorded continue permission (or resolution said hold/cancel) -> held in Auto, no notification
        return {
          rule: "R07",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          role: "primary",
          disposition: "held",
          runId: task.runId,
          goalId,
          repoRoot,
          ownerKind: "human",
          ownerId: "operator",
          ownerEpoch: null,
          nextAction: "answered; awaiting explicit resume",
          holdReason: "answered; awaiting explicit resume",
          releasePredicate: "explicit continue permission recorded or manual task.resume",
          action: null,
        };
      }

      // Source veto check before R08
      const sourceVeto = evaluateVeto({ runId: task.runId, goalId, taskId: task.id }, facts, {
        ignoreModeAndAuthority: true,
      });
      if (sourceVeto.vetoed) {
        return makeHoldDecision({
          rule: sourceVeto.code === "run_stopped" ? "R01" : "R08",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          runId: task.runId,
          goalId,
          repoRoot,
          veto: sourceVeto,
        });
      }

      // R08: task_blocked without proof / returned_for_rescope -> ladder A then B (replanning, never blind replay)
      const steerTarget = findLadderATask(task.runId, task.id, facts);
      return buildLadderDecision({
        rule: "R08",
        itemKind: item.kind,
        sourceId,
        sourceEpoch,
        causeId: defaultCauseId,
        runId: task.runId,
        goalId,
        repoRoot,
        steerTarget,
        steerMessage: `Task ${task.id} (${task.status}) needs replanning or unblocking: ${task.summary ?? task.title}. Do not blindly replay.`,
        batchNextAction: `investigate ${task.status} task ${task.id} via batch`,
      });
    }

    case "task_failed": {
      const task = facts.tasks.get(sourceId);
      if (!task) {
        return buildLadderDecision({
          rule: "R22",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          runId,
          goalId,
          repoRoot,
          steerTarget: null,
          steerMessage: "",
          batchNextAction: `investigate missing task ${sourceId}`,
        });
      }

      const sourceVeto = evaluateVeto({ runId: task.runId, goalId, taskId: task.id }, facts, {
        ignoreModeAndAuthority: true,
      });
      if (sourceVeto.vetoed) {
        return makeHoldDecision({
          rule: sourceVeto.code === "run_stopped" ? "R01" : "R09",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          runId: task.runId,
          goalId,
          repoRoot,
          veto: sourceVeto,
        });
      }

      // R09: task_failed in active run -> live parent owns it (no steer); else ladder A then B
      if (task.parentId && isTaskLive(task.parentId, facts)) {
        const parent = facts.tasks.get(task.parentId)!;
        return {
          rule: "R09",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          role: "primary",
          disposition: "routine_owned",
          runId: task.runId,
          goalId,
          repoRoot,
          ownerKind: "task",
          ownerId: parent.id,
          ownerEpoch: `g${parent.generation}`,
          nextAction: `await live parent ${parent.id} handling of failed child ${task.id}`,
          action: null,
        };
      }

      const steerTarget = findLadderATask(task.runId, task.id, facts);
      return buildLadderDecision({
        rule: "R09",
        itemKind: item.kind,
        sourceId,
        sourceEpoch,
        causeId: defaultCauseId,
        runId: task.runId,
        goalId,
        repoRoot,
        steerTarget,
        steerMessage: `Child task ${task.id} failed (${task.summary ?? task.title}). Reassign, retry, or escalate.`,
        batchNextAction: `investigate failed task ${task.id} via batch`,
      });
    }

    case "run_failed": {
      const run = facts.runs.get(sourceId);
      if (!run) {
        return buildLadderDecision({
          rule: "R22",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          runId,
          goalId,
          repoRoot,
          steerTarget: null,
          steerMessage: "",
          batchNextAction: `investigate missing run ${sourceId}`,
        });
      }

      const runTasks = facts.tasksByRun.get(run.id) ?? [];

      // R10: restart casualty (run.status event cause daemon_restart AND interrupted tasks with exact writer summary)
      if (isRunRestartCasualty(run.id, facts)) {
        const linkedGoals = facts.goalsByRunId.get(run.id) ?? [];
        const nonTerminalNonPausedGoals = linkedGoals.filter(
          (g) => g.status !== "achieved" && g.status !== "cancelled" && g.status !== "paused",
        );
        const positivelyGoalLinked = linkedGoals.length === 1 && nonTerminalNonPausedGoals.length === 1;
        const positivelyStandalone = linkedGoals.length === 0 && !facts.runsEverLinkedToGoal.has(run.id);
        const ambiguousOrDetached = !positivelyGoalLinked && !positivelyStandalone;

        // Paused linked goal vetoes recovery/handoff
        const sourceVeto = evaluateVeto({ runId: run.id, goalId }, facts, { ignoreModeAndAuthority: true });
        if (sourceVeto.vetoed) {
          return makeHoldDecision({
            rule: sourceVeto.code === "run_stopped" ? "R01" : "R10",
            itemKind: item.kind,
            sourceId,
            sourceEpoch,
            causeId: defaultCauseId,
            runId: run.id,
            goalId,
            repoRoot,
            veto: sourceVeto,
          });
        }

        if (ambiguousOrDetached) {
          return buildLadderDecision({
            rule: "R10",
            itemKind: item.kind,
            sourceId,
            sourceEpoch,
            causeId: defaultCauseId,
            runId: run.id,
            goalId,
            repoRoot,
            steerTarget: null,
            steerMessage: "",
            batchNextAction: `investigate detached/ambiguous restart casualty ${run.id} via batch`,
          });
        }

        const linkedGoal = positivelyGoalLinked ? nonTerminalNonPausedGoals[0]! : null;
        if (facts.config.restartRecovery === "handoff") {
          const ask = linkedGoal
            ? `${run.id} of ${linkedGoal.id} was interrupted by a daemon restart: resume in place | cancel`
            : `${run.id} was interrupted by a daemon restart: resume in place | cancel`;
          return {
            rule: "R10",
            itemKind: item.kind,
            sourceId,
            sourceEpoch,
            causeId: defaultCauseId,
            role: "primary",
            disposition: "human_blocker",
            runId: run.id,
            goalId: linkedGoal?.id ?? null,
            repoRoot,
            ownerKind: "human",
            ownerId: "operator",
            ownerEpoch: null,
            nextAction: "await human decision on daemon restart handoff",
            ask,
            blockedWork: formatBlockedWork(run.id, linkedGoal?.id ?? null, facts),
            action: null,
          };
        }

        // restartRecovery === "resume": fenced in-place recovery
        if (linkedGoal) {
          return {
            rule: "R10",
            itemKind: item.kind,
            sourceId,
            sourceEpoch,
            causeId: defaultCauseId,
            role: "primary",
            disposition: "routine_auto",
            runId: run.id,
            goalId: linkedGoal.id,
            repoRoot,
            ownerKind: "daemon",
            ownerId: "inbox-triage",
            ownerEpoch: null,
            nextAction: `resume restart-interrupted ${linkedGoal.id} (${run.id}) in place`,
            action: {
              primitive: "resume_goal",
              targetKind: "goal",
              targetId: linkedGoal.id,
              targetEpoch: `v${linkedGoal.version}:c${linkedGoal.reviewCycle}`,
              runId: run.id,
            },
          };
        }

        return {
          rule: "R10",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          role: "primary",
          disposition: "routine_auto",
          runId: run.id,
          goalId: null,
          repoRoot,
          ownerKind: "daemon",
          ownerId: "inbox-triage",
          ownerEpoch: null,
          nextAction: `resume standalone restart-interrupted ${run.id} in place`,
          action: {
            primitive: "resume_run",
            targetKind: "run",
            targetId: run.id,
            targetEpoch: sourceEpoch,
          },
        };
      }

      // R11: run_failed whose blocked tasks have proven blocking escalations (multiple allowed, never merge asks)
      const blockedTasks = runTasks.filter(
        (t) => t.profileId !== "director" && (t.status === "blocked" || t.status === "returned_for_rescope"),
      );
      const runStatusEv = facts.latestRunStatusEvent.get(run.id);
      const isBlockedRun =
        runStatusEv?.cause === "blocked_escalation" ||
        deriveRunCause(runTasks, run.status)?.cause === "blocked_escalation";
      if (isBlockedRun && blockedTasks.length > 0) {
        const provenUnresolvedMap = new Map<string, EscalationRecord>();
        for (const bt of blockedTasks) {
          for (const esc of findProvenBlockingEscalations(bt, facts).unresolved) {
            provenUnresolvedMap.set(esc.id, esc);
          }
        }
        const provenUnresolved = [...provenUnresolvedMap.values()].sort(
          (a, b) => a.createdAt - b.createdAt || a.id.localeCompare(b.id),
        );
        if (provenUnresolved.length > 0) {
          const primary = provenUnresolved[0]!;
          return {
            rule: "R11",
            itemKind: item.kind,
            sourceId,
            sourceEpoch,
            causeId: makeCauseId("escalation", primary.id),
            additionalCauseIds: provenUnresolved.slice(1).map((e) => makeCauseId("escalation", e.id)),
            role: "derived",
            disposition: "derived",
            runId: run.id,
            goalId,
            repoRoot,
            ownerKind: null,
            ownerId: null,
            ownerEpoch: null,
            nextAction: `derived under blocking escalation(s) ${provenUnresolved.map((e) => e.id).join(", ")}`,
            action: null,
          };
        }
      }

      // Source veto check before R12
      const sourceVeto = evaluateVeto({ runId: run.id, goalId }, facts, { ignoreModeAndAuthority: true });
      if (sourceVeto.vetoed) {
        return makeHoldDecision({
          rule: sourceVeto.code === "run_stopped" ? "R01" : "R12",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          runId: run.id,
          goalId,
          repoRoot,
          veto: sourceVeto,
        });
      }

      // R12: other run_failed -> ladder B (getLastRunStatusReason is context only)
      const reasonCtx = runStatusEv?.reason ? ` (${runStatusEv.reason.slice(0, 120)})` : "";
      return buildLadderDecision({
        rule: "R12",
        itemKind: item.kind,
        sourceId,
        sourceEpoch,
        causeId: defaultCauseId,
        runId: run.id,
        goalId,
        repoRoot,
        steerTarget: null,
        steerMessage: "",
        batchNextAction: `diagnose failed run ${run.id}${reasonCtx} via batch`,
      });
    }

    case "goal_awaiting_human": {
      const goal = facts.goals.get(sourceId);
      if (!goal) {
        return buildLadderDecision({
          rule: "R22",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          runId,
          goalId,
          repoRoot,
          steerTarget: null,
          steerMessage: "",
          batchNextAction: `investigate missing goal ${sourceId}`,
        });
      }

      const linkedRun = goal.linkedRunId ? facts.runs.get(goal.linkedRunId) ?? null : null;
      const reviews = facts.reviewsByGoalId.get(goal.id) ?? [];
      const versionReviews = reviews.filter((r) => r.goalVersion === goal.version);
      const latestVersionReview = versionReviews[0] ?? null;

      // R13: goal_awaiting_human derived under its linked run's cause ONLY when that run's failure is the recorded reason of this wait
      if (
        linkedRun &&
        linkedRun.status === "failed" &&
        goal.lastReviewSummary != null &&
        RUN_FAILURE_WAIT_REASON_RE.test(goal.lastReviewSummary) &&
        latestVersionReview?.verdict !== "escalate_human"
      ) {
        // Resolve transitively if the linked run itself derives under proven blocking escalation(s) (R11).
        const runItem: InboxItem = {
          id: `inbox:run:${linkedRun.id}`,
          severity: "P1",
          kind: "run_failed",
          title: `${linkedRun.id} failed`,
          detail: linkedRun.goal.split("\n")[0] ?? linkedRun.id,
          ref: { kind: "run", id: linkedRun.id, goalId: goal.id, label: linkedRun.id },
          goalId: goal.id,
          runId: linkedRun.id,
          createdAt: linkedRun.updatedAt,
        };
        const runDecision = routeItem(runItem, facts);
        return {
          rule: "R13",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: runDecision.causeId,
          additionalCauseIds: runDecision.additionalCauseIds,
          role: "derived",
          disposition: "derived",
          runId: linkedRun.id,
          goalId: goal.id,
          repoRoot,
          ownerKind: null,
          ownerId: null,
          ownerEpoch: null,
          nextAction: `derived under linked run cause ${runDecision.causeId}`,
          action: null,
        };
      }

      // R14x: linked run completed AND NO goal_reviews row for current goal_version (failed automation, e.g. quota-stopped reviewer)
      // R14 auto re-verify is REMOVED: zero unattended verify/review/accept calls -> ladder B diagnosis.
      const autoVerifyOff =
        goal.verificationMode === "none" ||
        (goal.lastReviewSummary != null && /auto-?verify.*off|verification_mode.*none/i.test(goal.lastReviewSummary));
      if (linkedRun && linkedRun.status === "completed" && versionReviews.length === 0 && !autoVerifyOff) {
        return buildLadderDecision({
          rule: "R14x",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          runId: linkedRun.id,
          goalId: goal.id,
          repoRoot,
          steerTarget: null,
          steerMessage: "",
          batchNextAction: `diagnose missing verdict for ${goal.id} v${goal.version} (no auto-verify/accept)`,
        });
      }

      // R15: goal_awaiting_human with verdict / circuit breaker / gates failure / auto-verify off / unlinked -> human_blocker
      const reason =
        goal.lastReviewSummary ??
        latestVersionReview?.summary ??
        "Goal awaits human review or decision";
      const gatesOrCircuitFailed =
        /gate.*fail|deterministic.*fail|circuit.?breaker|verification failed/i.test(reason);
      const validOptions = gatesOrCircuitFailed
        ? "resume with note | amend | cancel"
        : "accept with evidence | resume with note | amend | cancel";
      const ask = `${reason} — Options: ${validOptions}`;
      return {
        rule: "R15",
        itemKind: item.kind,
        sourceId,
        sourceEpoch,
        causeId: defaultCauseId,
        role: "primary",
        disposition: "human_blocker",
        runId: goal.linkedRunId,
        goalId: goal.id,
        repoRoot,
        ownerKind: "human",
        ownerId: "operator",
        ownerEpoch: null,
        nextAction: "await human decision on goal",
        ask,
        blockedWork: formatBlockedWork(goal.linkedRunId, goal.id, facts),
        action: null,
      };
    }

    case "merge_bounced": {
      const m = facts.merges.get(sourceId);
      if (!m) {
        return buildLadderDecision({
          rule: "R22",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          runId,
          goalId,
          repoRoot,
          steerTarget: null,
          steerMessage: "",
          batchNextAction: `investigate missing merge row ${sourceId}`,
        });
      }

      // R16: superseded by a later merged row on the SAME run_id, task_id, worktree_id, and branch
      const laterMerged = findSupersededMergedSubmission(m, facts);
      if (laterMerged) {
        const proof = `submission superseded by ${laterMerged.id}`;
        return {
          rule: "R16",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          role: "primary",
          disposition: "routine_auto",
          runId: m.runId,
          goalId,
          repoRoot,
          ownerKind: "daemon",
          ownerId: "inbox-triage",
          ownerEpoch: null,
          nextAction: `record ledger proof: ${proof}`,
          action: {
            primitive: "ledger_proof",
            targetKind: "merge",
            targetId: m.id,
            targetEpoch: sourceEpoch,
            proof,
          },
        };
      }

      // Source veto check before R17
      const sourceVeto = evaluateVeto({ runId: m.runId, goalId }, facts, { ignoreModeAndAuthority: true });
      if (sourceVeto.vetoed) {
        return makeHoldDecision({
          rule: sourceVeto.code === "run_stopped" ? "R01" : "R17",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          runId: m.runId,
          goalId,
          repoRoot,
          veto: sourceVeto,
        });
      }

      // R17: submitting task live -> passive owner; else ladder A then B
      if (isTaskLive(m.taskId, facts)) {
        const submitter = facts.tasks.get(m.taskId)!;
        return {
          rule: "R17",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          role: "primary",
          disposition: "routine_owned",
          runId: m.runId,
          goalId,
          repoRoot,
          ownerKind: "task",
          ownerId: submitter.id,
          ownerEpoch: `g${submitter.generation}`,
          nextAction: `await submitting task ${submitter.id} to resolve bounced merge ${m.id}`,
          action: null,
        };
      }

      const steerTarget = findLadderATask(m.runId, m.taskId, facts);
      return buildLadderDecision({
        rule: "R17",
        itemKind: item.kind,
        sourceId,
        sourceEpoch,
        causeId: defaultCauseId,
        runId: m.runId,
        goalId,
        repoRoot,
        steerTarget,
        steerMessage: `Merge ${m.id} (${m.status}) on branch ${m.branch} for settled task ${m.taskId} needs follow-up.`,
        batchNextAction: `investigate bounced merge ${m.id} (${m.status}) via batch`,
      });
    }

    case "open_message": {
      const msg = facts.messages.get(sourceId);
      if (!msg) {
        return buildLadderDecision({
          rule: "R22",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          runId,
          goalId,
          repoRoot,
          steerTarget: null,
          steerMessage: "",
          batchNextAction: `investigate missing message ${sourceId}`,
        });
      }

      const toTask = facts.tasks.get(msg.toTaskId);
      const fromTask = facts.tasks.get(msg.fromTaskId);
      const run = facts.runs.get(msg.runId);

      // R18: definitively obsolete — recipient durably terminal (completed/failed/cancelled),
      // sender durably terminal, or run settled (completed/failed/stopped).
      // NEVER expire merely because missing in RAM, blocked, paused, or paused_quota.
      let expireReason: string | null = null;
      if (toTask && TERMINAL_MESSAGE_TASK_STATUS_SET.has(toTask.status)) {
        expireReason = `recipient task ${toTask.id} is ${toTask.status}`;
      } else if (fromTask && TERMINAL_MESSAGE_TASK_STATUS_SET.has(fromTask.status)) {
        expireReason = `sender task ${fromTask.id} is ${fromTask.status}`;
      } else if (run && (run.status === "completed" || run.status === "failed" || run.status === "stopped")) {
        expireReason = `run ${run.id} is ${run.status}`;
      }

      if (expireReason) {
        const askerLive = isTaskLive(msg.fromTaskId, facts);
        return {
          rule: "R18",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          role: "primary",
          disposition: "routine_auto",
          runId: msg.runId,
          goalId,
          repoRoot,
          ownerKind: "daemon",
          ownerId: "inbox-triage",
          ownerEpoch: null,
          nextAction: `expire obsolete message ${msg.id} (${expireReason})`,
          action: {
            primitive: "expire_message",
            targetKind: "message",
            targetId: msg.id,
            targetEpoch: sourceEpoch,
            reason: expireReason,
            notifyAskerTaskId: askerLive ? msg.fromTaskId : null,
          },
        };
      }

      // Source veto check before R19 (e.g. run paused_quota or goal paused)
      const sourceVeto = evaluateVeto({ runId: msg.runId, goalId }, facts, { ignoreModeAndAuthority: true });
      if (sourceVeto.vetoed) {
        return makeHoldDecision({
          rule: sourceVeto.code === "run_stopped" ? "R01" : "R19",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          runId: msg.runId,
          goalId,
          repoRoot,
          veto: sourceVeto,
        });
      }

      // R19: recipient live -> passive owner = recipient; else ladder A then B
      if (toTask && isTaskLive(toTask.id, facts)) {
        return {
          rule: "R19",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          role: "primary",
          disposition: "routine_owned",
          runId: msg.runId,
          goalId,
          repoRoot,
          ownerKind: "task",
          ownerId: toTask.id,
          ownerEpoch: `g${toTask.generation}`,
          nextAction: `await live recipient ${toTask.id} answer to ${msg.id}`,
          action: null,
        };
      }

      const steerTarget = findLadderATask(msg.runId, msg.toTaskId, facts);
      return buildLadderDecision({
        rule: "R19",
        itemKind: item.kind,
        sourceId,
        sourceEpoch,
        causeId: defaultCauseId,
        runId: msg.runId,
        goalId,
        repoRoot,
        steerTarget,
        steerMessage: `Open message ${msg.id} (${msg.fromTaskId} -> ${msg.toTaskId}) cannot be answered because recipient is ${toTask?.status ?? "missing"}.`,
        batchNextAction: `investigate open message ${msg.id} via batch`,
      });
    }

    case "quota_paused": {
      const run = facts.runs.get(sourceId);
      const quota = facts.quotaStatusByRunId.get(sourceId) ?? null;

      // R20: auto_timer or user_timer -> held under existing quota owner (never advance/override user timer)
      if (quota && (quota.mode === "auto_timer" || quota.mode === "user_timer")) {
        return {
          rule: "R20",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          role: "primary",
          disposition: "held",
          runId: sourceId,
          goalId,
          repoRoot,
          ownerKind: "daemon",
          ownerId: quota.mode === "user_timer" ? "quota-user-timer" : "quota-auto-resume",
          ownerEpoch: null,
          nextAction: quota.resumeAt
            ? `held under ${quota.mode} quota owner (resumeAt ${quota.resumeAt})`
            : `held under ${quota.mode} quota owner`,
          holdReason: `quota paused (${quota.mode}): ${quota.reason}`,
          releasePredicate: `run ${sourceId} leaves status 'paused_quota'`,
          action: null,
        };
      }

      // R21 (manual): manual quota pause -> human_blocker with recorded reason
      if (quota && quota.mode === "manual") {
        return {
          rule: "R21",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          role: "primary",
          disposition: "human_blocker",
          runId: sourceId,
          goalId,
          repoRoot,
          ownerKind: "human",
          ownerId: "operator",
          ownerEpoch: null,
          nextAction: "await human quota resolution",
          ask: `Manual quota pause on ${sourceId}: ${quota.reason}. Resume the run when quota is available or switch profile.`,
          blockedWork: formatBlockedWork(sourceId, goalId, facts),
          action: null,
        };
      }

      // R21 (null / missing in-memory quota state): start durable missing-since on first observation,
      // surface only after recoverMaxSec and a re-check.
      const missingSince = facts.missingQuotaSinceByCauseId.get(defaultCauseId);
      const recoverMs = facts.config.recoverMaxSec * 1000;
      if (missingSince === undefined || facts.now - missingSince < recoverMs) {
        const since = missingSince ?? facts.now;
        return {
          rule: "R21",
          itemKind: item.kind,
          sourceId,
          sourceEpoch,
          causeId: defaultCauseId,
          role: "primary",
          disposition: "held",
          runId: sourceId,
          goalId,
          repoRoot,
          ownerKind: "daemon",
          ownerId: "quota-owner-pending",
          ownerEpoch: null,
          nextAction: `await quota scheduler state restoration for ${sourceId}`,
          nextCheckAt: since + recoverMs,
          holdReason: `run ${run?.id ?? sourceId} is paused_quota without in-memory quota state; waiting ${facts.config.recoverMaxSec}s before surfacing`,
          releasePredicate: "in-memory quota state restored or recoverMaxSec elapsed",
          recordMissingSince: missingSince === undefined,
          action: null,
        };
      }

      return {
        rule: "R21",
        itemKind: item.kind,
        sourceId,
        sourceEpoch,
        causeId: defaultCauseId,
        role: "primary",
        disposition: "human_blocker",
        runId: sourceId,
        goalId,
        repoRoot,
        ownerKind: "human",
        ownerId: "operator",
        ownerEpoch: null,
        nextAction: "await human quota resolution (missing quota owner)",
        ask: `Run ${sourceId} is paused_quota with no active quota scheduler after ${facts.config.recoverMaxSec}s. Resume the run or switch profile.`,
        blockedWork: formatBlockedWork(sourceId, goalId, facts),
        action: null,
      };
    }

    default: {
      // R22: anything else -> ladder B diagnosis (never oldest unrelated cause, never generic human fallback)
      return buildLadderDecision({
        rule: "R22",
        itemKind: item.kind,
        sourceId,
        sourceEpoch,
        causeId: defaultCauseId,
        runId,
        goalId,
        repoRoot,
        steerTarget: null,
        steerMessage: "",
        batchNextAction: `diagnose unhandled inbox item ${item.id} via batch`,
      });
    }
  }
}

export function decisionToPlanRow(item: InboxItem, decision: TriageDecision, facts: TriageFacts): InboxTriagePlanRow {
  const veto =
    decision.action && decision.disposition !== "human_blocker"
      ? evaluateVeto(
          {
            runId: decision.runId,
            goalId: decision.goalId,
            taskId: decision.action.targetKind === "task" ? decision.action.targetId : undefined,
            allowSettledGoal: decision.rule === "R00",
            allowStoppedRun: decision.rule === "R18" || decision.rule === "R00",
            allowQuotaPaused: decision.rule === "R20" || decision.rule === "R21",
          },
          facts,
          { ignoreModeAndAuthority: true },
        )
      : { vetoed: false };

  return {
    itemId: item.id,
    itemKind: decision.itemKind,
    sourceId: decision.sourceId,
    sourceEpoch: decision.sourceEpoch,
    causeId: decision.causeId,
    role: decision.role,
    runId: decision.runId,
    goalId: decision.goalId,
    repoRoot: decision.repoRoot,
    rule: decision.rule,
    disposition: decision.disposition,
    ownerKind: decision.ownerKind,
    ownerId: decision.ownerId,
    nextAction: decision.nextAction,
    ...(decision.holdReason ? { holdReason: decision.holdReason } : {}),
    ...(decision.ask ? { ask: decision.ask } : {}),
    ...(decision.blockedWork ? { blockedWork: decision.blockedWork } : {}),
    ...(veto.vetoed && veto.code ? { vetoedBy: veto.code } : {}),
  };
}

// ============================================================================
// 5. InboxTriage Two-Pass Sweep Engine (Contract Sections 4, 5, 8, 9, 10)
// ============================================================================

export interface TriageClock {
  now(): number;
}

export interface TriageScheduler {
  every(ms: number, fn: () => void): () => void;
  after(ms: number, fn: () => void): () => void;
}

export interface TriageDispatcher {
  steerTask(taskId: string, message: string): Promise<{ steered: boolean; code?: string }>;
  resumeTask(taskId: string, expectedGeneration: number): Promise<{ outcome: string; generation?: number }>;
  resumeRun(runId: string): Promise<{ outcome: string }>;
  resumeGoal(goalId: string, expectedRunId?: string): Promise<{ outcome: string }>;
  /** Null or omitted when rung B is not configured. */
  launchBatch?:
    | ((
        assignmentId: string,
        scope: string,
        brief: string,
      ) => Promise<{ taskId: string; generation: number } | null>)
    | null;
}

export interface TriageDeps {
  db: ControlPlaneDb;
  store: InboxTriageStore;
  config(): InboxTriageConfig;
  clock: TriageClock;
  scheduler: TriageScheduler;
  hostname(): string;
  loadFacts?: () => TriageFacts;
  isTaskExecuting(taskId: string): boolean;
  getRunQuotaStatus(runId: string): RunQuotaPauseStatus | null;
  dispatcher: TriageDispatcher;
  emit(runId: string, type: string, payload: unknown): void;
  /** Optional maintenance/admission freeze check; when true, triage stays disarmed. */
  isFrozen?: () => boolean;
  /** Optional deterministic barrier hook fired after claimEffectIntent and before external dispatch (tests). */
  onAfterClaim?: (effectKey: string) => void | Promise<void>;
  /** Optional deterministic barrier hook fired after external dispatch and before recordEffectReceipt (tests). */
  onBeforeReceipt?: (effectKey: string) => void | Promise<void>;
}

export interface InboxTriageScope {
  repoRoot?: string;
  chatSessionId?: string;
  runId?: string;
}

export type DueTriageAction =
  | {
      kind: "cause_action";
      causeId: string;
      decision: TriageDecision;
      action: Exclude<TriageActionSpec, { primitive: "batch_investigate" }>;
      expensive: boolean;
    }
  | {
      kind: "batch_launch";
      assignmentId: string;
      scope: string;
      memberCauseIds: string[];
      brief: string;
      expensive: true;
    };

export interface ControlReport {
  ran: boolean;
  at: number;
  enumerated: number;
  acked: number;
  reopened: number;
  surfaced: number;
  notified: number;
  held: number;
  released: number;
  owned: number;
  ownerLost: number;
  leaseRenewed: number;
  reconciledEffects: number;
  parked: number;
  resolved: number;
  batchesPrepared: number;
  dueActions: DueTriageAction[];
}

export interface ActionReport {
  ran: boolean;
  at: number;
  dispatched: number;
  expensiveDispatched: number;
  confirmed: number;
  failed: number;
  ambiguous: number;
  skippedVeto: number;
  skippedBusyOrBudget: number;
}

export interface SweepReport {
  skipped: boolean;
  control: ControlReport;
  action: ActionReport;
}

export interface BatchOutcomeItem {
  cause_id: string;
  outcome: "proof" | "needs_human" | "remediation" | "undetermined";
  evidence: string[];
  ask?: string;
  next_action?: string;
}

export interface BatchIngestReport {
  assignmentId: string;
  closed: boolean;
  parsed: boolean;
  resolved: number;
  surfaced: number;
  parked: number;
  ignoredUnknown: number;
  unverifiedProofs: number;
}

const RELEVANT_NUDGE_PREFIXES = ["escalation.", "merge.", "message.", "goal.", "quota."];
const RELEVANT_NUDGE_EXACT = new Set(["task.status", "task.resumed", "run.status", "agent.steer_undelivered"]);
const FIRST_TICK_MS = 5_000;
const NUDGE_DEBOUNCE_MS = 250;
const ASSIGNMENT_MAX_MS = 20 * 60_000;
const TRIAGE_OUTCOMES_RE = /<triage_outcomes>([\s\S]*?)<\/triage_outcomes>/i;

function dedupeEntityRefs(refs: EntityRef[]): EntityRef[] {
  const seen = new Set<string>();
  const out: EntityRef[] = [];
  for (const r of refs) {
    const key = `${r.kind}:${r.id}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(r);
  }
  return out;
}

function makeDefaultRefForCause(cause: TriageCauseRecord): EntityRef {
  switch (cause.originKind) {
    case "escalation":
      return { kind: "escalation", id: cause.originSourceId, runId: cause.runId ?? undefined, goalId: cause.goalId ?? undefined };
    case "goal_awaiting_human":
      return { kind: "goal", id: cause.originSourceId, runId: cause.runId ?? undefined };
    case "run_failed":
    case "quota_paused":
      return { kind: "run", id: cause.originSourceId, goalId: cause.goalId ?? undefined };
    case "task_blocked":
    case "task_failed":
      return { kind: "task", id: cause.originSourceId, runId: cause.runId ?? undefined, goalId: cause.goalId ?? undefined };
    case "merge_bounced":
      return { kind: "merge", id: cause.originSourceId, runId: cause.runId ?? undefined, goalId: cause.goalId ?? undefined };
    case "open_message":
      return { kind: "message", id: cause.originSourceId, runId: cause.runId ?? undefined };
  }
}

function makeTriageInfo(cause: TriageCauseRecord): InboxTriageInfo {
  return {
    disposition: cause.disposition,
    state: cause.state,
    rule: cause.rule,
    causeId: cause.causeId,
    ownerKind: cause.ownerKind,
    ownerId: cause.ownerId,
    nextAction: cause.nextAction,
    nextCheckAt: cause.nextCheckAt,
    attempts: cause.attempts,
    ackedAt: cause.ackedAt,
    holdReason: cause.holdReason,
    askId: cause.askId,
  };
}

function causeToSummary(c: TriageCauseRecord): InboxTriageCauseSummary {
  return {
    causeId: c.causeId,
    originKind: c.originKind,
    originSourceId: c.originSourceId,
    runId: c.runId,
    goalId: c.goalId,
    repoRoot: c.repoRoot,
    rule: c.rule,
    disposition: c.disposition,
    state: c.state,
    revision: c.revision,
    ownerKind: c.ownerKind,
    ownerId: c.ownerId,
    ownerEpoch: c.ownerEpoch,
    assignmentId: c.assignmentId,
    nextAction: c.nextAction,
    nextCheckAt: c.nextCheckAt,
    leaseExpiresAt: c.leaseExpiresAt,
    assignmentExpiresAt: c.assignmentExpiresAt,
    noProgressRenewals: c.noProgressRenewals,
    attempts: c.attempts,
    firstDispatchAt: c.firstDispatchAt,
    absoluteDeadlineAt: c.absoluteDeadlineAt,
    holdReason: c.holdReason,
    askId: c.askId,
    outcomeKind: c.outcomeKind,
    outcome: c.outcome,
    ackedAt: c.ackedAt,
    resolvedAt: c.resolvedAt,
    updatedAt: c.updatedAt,
  };
}

function journalToSummary(j: TriageJournalRecord): InboxTriageJournalEntrySummary {
  return {
    seq: j.seq,
    at: j.at,
    causeId: j.causeId,
    itemKey: j.itemKey,
    runId: j.runId,
    goalId: j.goalId,
    type: j.type,
    payload: j.payload,
  };
}

function parseBatchOutcomes(summaryText: string): BatchOutcomeItem[] | null {
  const match = TRIAGE_OUTCOMES_RE.exec(summaryText);
  if (!match?.[1]) return null;
  let raw = match[1].trim();
  // Strip optional fenced markdown block inside the tag
  if (raw.startsWith("```")) {
    raw = raw.replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/, "").trim();
  }
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return null;
    const valid: BatchOutcomeItem[] = [];
    for (const entry of parsed) {
      if (!entry || typeof entry !== "object" || Array.isArray(entry)) return null;
      const obj = entry as Record<string, unknown>;
      if (typeof obj.cause_id !== "string" || !obj.cause_id) return null;
      if (
        obj.outcome !== "proof" &&
        obj.outcome !== "needs_human" &&
        obj.outcome !== "remediation" &&
        obj.outcome !== "undetermined"
      ) {
        return null;
      }
      const evidence = Array.isArray(obj.evidence) ? obj.evidence.filter((x): x is string => typeof x === "string") : [];
      valid.push({
        cause_id: obj.cause_id,
        outcome: obj.outcome,
        evidence,
        ...(typeof obj.ask === "string" ? { ask: obj.ask } : {}),
        ...(typeof obj.next_action === "string" ? { next_action: obj.next_action } : {}),
      });
    }
    return valid;
  } catch {
    return null;
  }
}

export class InboxTriage {
  private started = false;
  private disarmed = false;
  private authorityEpoch = 0;
  private sweeping = false;
  private pendingRerun = false;
  private configOverride: InboxTriageConfig | null = null;
  private cancelFirstTick: (() => void) | null = null;
  private cancelPeriodicTick: (() => void) | null = null;
  private cancelNudge: (() => void) | null = null;
  private inFlightEffects = new Set<string>();
  private idCounter = 0;

  constructor(private readonly deps: TriageDeps) {}

  private currentConfig(): InboxTriageConfig {
    return this.configOverride ?? this.deps.config();
  }

  private nextId(prefix: string, now: number): string {
    this.idCounter += 1;
    return `${prefix}-${now}-${this.idCounter}`;
  }

  /** Returns true iff mode is `"on"`, hostname matches `schedulerHost`, not disarmed, and not frozen. */
  isAuthoritative(cfg: InboxTriageConfig = this.currentConfig()): boolean {
    if (this.disarmed) return false;
    if (cfg.mode !== "on") return false;
    if (!cfg.schedulerHost || cfg.schedulerHost !== this.deps.hostname()) return false;
    if (this.deps.isFrozen?.()) return false;
    return true;
  }

  private isRungBEnabled(cfg: InboxTriageConfig = this.currentConfig()): boolean {
    if (!this.deps.dispatcher.launchBatch) return false;
    return Boolean(cfg.batchModel || cfg.batchProfileId);
  }

  private loadFacts(): TriageFacts {
    const cfg = this.currentConfig();
    const authoritative = this.isAuthoritative(cfg);
    if (this.deps.loadFacts) {
      return this.deps.loadFacts();
    }
    return loadTriageFacts(this.deps.db, this.deps.store, {
      now: this.deps.clock.now(),
      config: cfg,
      authoritative,
      isTaskExecuting: (taskId) => this.deps.isTaskExecuting(taskId),
      getRunQuotaStatus: (runId) => this.deps.getRunQuotaStatus(runId),
    });
  }

  private mirrorJournals(journals: ReadonlyArray<TriageJournalRecord | null | undefined>): void {
    for (const j of journals) {
      if (!j || !j.runId) continue;
      this.deps.emit(j.runId, `inbox.triage.${j.type}`, {
        seq: j.seq,
        at: j.at,
        causeId: j.causeId,
        itemKey: j.itemKey,
        goalId: j.goalId,
        ...(j.payload && typeof j.payload === "object" ? (j.payload as Record<string, unknown>) : { payload: j.payload }),
      });
    }
  }

  private clearTimers(): void {
    this.cancelFirstTick?.();
    this.cancelFirstTick = null;
    this.cancelPeriodicTick?.();
    this.cancelPeriodicTick = null;
    this.cancelNudge?.();
    this.cancelNudge = null;
  }

  private armTimers(cfg: InboxTriageConfig): void {
    this.clearTimers();
    const intervalMs = Math.max(1_000, cfg.sweepIntervalSec * 1_000);
    const firstMs = Math.min(FIRST_TICK_MS, intervalMs);
    this.cancelFirstTick = this.deps.scheduler.after(firstMs, () => {
      this.cancelFirstTick = null;
      void this.sweepOnce();
    });
    this.cancelPeriodicTick = this.deps.scheduler.every(intervalMs, () => {
      void this.sweepOnce();
    });
  }

  start(): void {
    this.started = true;
    const cfg = this.currentConfig();
    if (cfg.mode !== "on") return;
    if (!cfg.schedulerHost || cfg.schedulerHost !== this.deps.hostname() || this.deps.isFrozen?.()) {
      return;
    }
    this.disarmed = false;
    this.authorityEpoch += 1;
    this.deps.store.ensureSchema();
    this.armTimers(cfg);
  }

  stop(): void {
    this.started = false;
    this.authorityEpoch += 1;
    this.clearTimers();
  }

  /** Synchronously disarms triage: stops timers and invalidates any pending/in-flight activations. */
  disarm(): void {
    this.disarmed = true;
    this.authorityEpoch += 1;
    this.clearTimers();
  }

  applyConfig(cfg: InboxTriageConfig): void {
    this.configOverride = cfg;
    if (cfg.mode !== "on" || !cfg.schedulerHost || cfg.schedulerHost !== this.deps.hostname() || this.deps.isFrozen?.()) {
      this.disarm();
      return;
    }
    this.disarmed = false;
    this.authorityEpoch += 1;
    if (this.started) {
      this.deps.store.ensureSchema();
      this.armTimers(cfg);
    }
  }

  onEvent(ev: EventRecord): void {
    // Triage MUST ignore its own inbox.triage.* events to avoid feedback loops.
    if (ev.type.startsWith("inbox.triage.")) return;
    // Amendment 1: an escalation reopen (resolved -> open/acked) immediately revokes any unconsumed grant.
    if (ev.type === "escalation.updated" && this.deps.store.hasSchema() && this.isAuthoritative()) {
      const p = ev.payload as { id?: unknown; status?: unknown } | null;
      if (typeof p?.id === "string" && typeof p.status === "string" && p.status !== "resolved") {
        const rev = this.deps.store.revokeContinuations(p.id, this.deps.clock.now(), {
          reason: `escalation_updated_to_${p.status}`,
          runId: ev.runId,
        });
        if (rev.journal) this.mirrorJournals([rev.journal]);
      }
    }
    if (!this.started || !this.isAuthoritative()) return;
    const relevant =
      RELEVANT_NUDGE_EXACT.has(ev.type) || RELEVANT_NUDGE_PREFIXES.some((prefix) => ev.type.startsWith(prefix));
    if (!relevant) return;
    // Debounce nudges at 250ms; NEVER postpone the fixed periodic tick.
    if (this.cancelNudge) return;
    this.cancelNudge = this.deps.scheduler.after(NUDGE_DEBOUNCE_MS, () => {
      this.cancelNudge = null;
      void this.sweepOnce();
    });
  }

  // ---- Pure Read-Only Views (report / list / project) ----

  private buildAllItems(facts: TriageFacts, includeAckedEscalations = true): InboxItem[] {
    const goalByRunId = new Map<string, TriageGoalSummary>();
    for (const [runId, list] of facts.goalsByRunId) {
      if (list[0]) goalByRunId.set(runId, list[0]);
    }
    const items = buildInboxItems({
      goals: [...facts.goals.values()],
      runEntries: [...facts.runs.values()].map((run) => ({
        run,
        goalId: goalByRunId.get(run.id)?.id ?? null,
        tasks: facts.tasksByRun.get(run.id) ?? [],
        quotaPause: facts.quotaStatusByRunId.get(run.id) ?? null,
      })),
      escalations: [...facts.escalations.values()],
      merges: [...facts.merges.values()],
      messages: [...facts.messages.values()],
      goalByRunId,
      includeAckedEscalations,
    });
    return items.filter((item) => !(item.taskId && facts.triageOwnedTaskIds.has(item.taskId)));
  }

  private itemInScope(
    item: { runId?: string | null; goalId?: string | null; repoRoot?: string | null },
    scope: InboxTriageScope | undefined,
    facts: TriageFacts,
  ): boolean {
    if (!scope) return true;
    if (scope.runId && item.runId !== scope.runId) return false;
    const repoRoot =
      item.repoRoot ??
      (item.runId ? facts.runs.get(item.runId)?.repoRoot : null) ??
      (item.goalId ? facts.goals.get(item.goalId)?.repoRoot : null) ??
      null;
    if (scope.repoRoot && repoRoot !== scope.repoRoot) return false;
    if (scope.chatSessionId) {
      const linkedGoal =
        (item.goalId ? facts.goals.get(item.goalId) : null) ??
        (item.runId ? (facts.goalsByRunId.get(item.runId) ?? [])[0] ?? null : null);
      if (linkedGoal) {
        if (linkedGoal.chatSessionId !== scope.chatSessionId) return false;
      } else if (item.runId && !scope.runId) {
        return false;
      }
    }
    return true;
  }

  /** Pure, zero-write routing plan across all unwindowed source items. Works in every mode. */
  report(scope?: InboxTriageReportParams): InboxTriagePlanRow[] {
    const facts = this.loadFacts();
    const items = this.buildAllItems(facts, true);
    const rows: InboxTriagePlanRow[] = [];
    for (const item of items) {
      const decision = routeItem(item, facts);
      if (decision.ignored) continue;
      if (!this.itemInScope(decision, scope, facts)) continue;
      rows.push(decisionToPlanRow(item, decision, facts));
    }
    return rows;
  }

  /** Read-only query over the durable triage ledger (never creates DDL). */
  list(params?: InboxTriageListParams): InboxTriageListResult {
    const causes = this.deps.store
      .listCauses({
        state: params?.state,
        runId: params?.runId,
        causeId: params?.causeId,
        repoRoot: params?.repoRoot,
        limit: params?.limit,
      })
      .map(causeToSummary);
    if (params?.journal) {
      const journal = this.deps.store
        .listJournal({
          causeId: params.causeId,
          runId: params.runId,
          limit: params.journalLimit ?? 200,
        })
        .map(journalToSummary);
      return { causes, journal };
    }
    return { causes };
  }

  /**
   * Projects Human (`human`) and Automation (`auto`) inbox views (Contract Section 10).
   * - In `mode !== "on"`: returns `{ human: items, auto: [] }` (byte-identical to legacy).
   * - In `mode === "on"`: pure, zero-write projection from unwindowed sources + durable asks/causes,
   *   scoped to caller's `{ repoRoot, chatSessionId, runId }` with inline classification for un-acked items
   *   so the legacy backlog is never flashed into the human inbox.
   */
  project(items: InboxItem[], scope?: InboxTriageScope): { human: InboxItem[]; auto: InboxItem[] } {
    const cfg = this.currentConfig();
    if (cfg.mode !== "on") {
      return { human: items, auto: [] };
    }

    const facts = this.loadFacts();
    const allItems = this.buildAllItems(facts, true);
    const allItemsByCauseId = new Map<string, InboxItem[]>();
    const decisionByItemId = new Map<string, TriageDecision>();
    const derivedRefsByCauseId = new Map<string, EntityRef[]>();

    for (const item of allItems) {
      const dec = routeItem(item, facts);
      if (dec.ignored) continue;
      if (!this.itemInScope(dec, scope, facts)) continue;
      decisionByItemId.set(item.id, dec);
      if (dec.role === "derived") {
        const targets = [dec.causeId, ...(dec.additionalCauseIds ?? [])];
        for (const cid of targets) {
          const list = derivedRefsByCauseId.get(cid) ?? [];
          list.push(item.ref);
          derivedRefsByCauseId.set(cid, list);
        }
      } else {
        const list = allItemsByCauseId.get(dec.causeId) ?? [];
        list.push(item);
        allItemsByCauseId.set(dec.causeId, list);
      }
    }

    const human: InboxItem[] = [];
    const auto: InboxItem[] = [];
    const emittedCauseIds = new Set<string>();

    // 1. Durable ledger causes in scope (exempt from snapshot windows!)
    const storedCauses = this.deps.store.listCauses({
      states: ["acknowledged", "owned", "held", "surfaced", "parked"],
    });
    const activeAsksByCauseId = new Map(
      this.deps.store
        .listActiveAsks("blocker")
        .filter((a) => a.causeId !== null)
        .map((a) => [a.causeId!, a]),
    );

    for (const cause of storedCauses) {
      if (!this.itemInScope(cause, scope, facts)) continue;
      emittedCauseIds.add(cause.causeId);
      const primaryItem = allItemsByCauseId.get(cause.causeId)?.[0];
      const baseRef = primaryItem?.ref ?? makeDefaultRefForCause(cause);
      const askRow = activeAsksByCauseId.get(cause.causeId);
      const sourceRefs = dedupeEntityRefs([
        baseRef,
        ...(askRow?.sourceRefs ?? []),
        ...(derivedRefsByCauseId.get(cause.causeId) ?? []),
      ]);

      const baseItem: InboxItem = primaryItem
        ? { ...primaryItem }
        : {
            id: `inbox:cause:${cause.causeId}`,
            severity: cause.state === "surfaced" ? "P0" : "P1",
            kind: cause.originKind,
            title: `${cause.originSourceId} (${cause.rule})`,
            detail: cause.nextAction,
            ref: baseRef,
            goalId: cause.goalId,
            runId: cause.runId,
            createdAt: cause.ackedAt,
          };

      const enriched: InboxItem = {
        ...baseItem,
        triage: makeTriageInfo(cause),
        sourceRefs,
        ...(askRow ? { ask: askRow.ask, blockedWork: askRow.blockedWork } : {}),
      };

      if (cause.state === "surfaced") {
        human.push(enriched);
      } else {
        auto.push(enriched);
      }
    }

    // 2. Coalesced scope_decision asks in scope
    for (const scopeAsk of this.deps.store.listActiveAsks("scope_decision")) {
      if (scope?.repoRoot && scopeAsk.scope !== scope.repoRoot) continue;
      if (scope?.runId) {
        const matchesRun = scopeAsk.sourceRefs.some((r) => r.runId === scope.runId);
        if (!matchesRun) continue;
      }
      if (scope?.chatSessionId && !scope.repoRoot) {
        const matchesChat = scopeAsk.sourceRefs.some((r) => {
          const g =
            (r.goalId ? facts.goals.get(r.goalId) : null) ??
            (r.runId ? (facts.goalsByRunId.get(r.runId) ?? [])[0] ?? null : null);
          return g?.chatSessionId === scope.chatSessionId;
        });
        if (!matchesChat) continue;
      }
      const firstRef: EntityRef = scopeAsk.sourceRefs[0] ?? { kind: "run", id: scopeAsk.scope ?? "global" };
      human.push({
        id: `inbox:ask:${scopeAsk.askId}`,
        severity: "P1",
        kind: "escalation",
        title: scopeAsk.ask,
        detail: scopeAsk.blockedWork,
        ref: firstRef,
        createdAt: scopeAsk.createdAt,
        ask: scopeAsk.ask,
        blockedWork: scopeAsk.blockedWork,
        sourceRefs: scopeAsk.sourceRefs,
        triage: {
          disposition: "human_blocker",
          state: "surfaced",
          rule: "SCOPE_DECISION",
          causeId: `scope:${scopeAsk.scope ?? ""}`,
          ownerKind: "human",
          ownerId: "operator",
          nextAction: "prioritise parked findings",
          askId: scopeAsk.askId,
        },
      });
    }

    // 3. Inline classification for un-acked items (zero mutation; never flashes routine backlog into human)
    const inlineCandidates = allItems.length > 0 ? allItems : items;
    for (const item of inlineCandidates) {
      const dec = decisionByItemId.get(item.id) ?? routeItem(item, facts);
      if (dec.ignored || dec.role === "derived") continue;
      if (!this.itemInScope(dec, scope, facts)) continue;
      if (emittedCauseIds.has(dec.causeId)) continue;
      emittedCauseIds.add(dec.causeId);

      const inlineRefs: EntityRef[] = [item.ref, ...(derivedRefsByCauseId.get(dec.causeId) ?? [])];
      if (dec.rule === "R10" && dec.runId) {
        for (const t of facts.tasksByRun.get(dec.runId) ?? []) {
          if (t.status === "failed" && t.summary === DAEMON_RESTART_SUMMARY) {
            inlineRefs.push({ kind: "task", id: t.id, runId: dec.runId, label: t.title });
          }
        }
      }
      const sourceRefs = dedupeEntityRefs(inlineRefs);
      const enriched: InboxItem = {
        ...item,
        sourceRefs,
        ...(dec.ask ? { ask: dec.ask } : {}),
        ...(dec.blockedWork ? { blockedWork: dec.blockedWork } : {}),
        triage: {
          disposition: dec.disposition,
          state: dec.disposition === "human_blocker" ? "surfaced" : dec.disposition === "held" ? "held" : "acknowledged",
          rule: dec.rule,
          causeId: dec.causeId,
          ownerKind: dec.ownerKind,
          ownerId: dec.ownerId,
          nextAction: dec.nextAction,
          holdReason: dec.holdReason ?? null,
        },
      };

      if (dec.disposition === "human_blocker") {
        human.push(enriched);
      } else {
        auto.push(enriched);
      }
    }

    return {
      human: sortInboxItems(human),
      auto: sortInboxItems(auto),
    };
  }

  // ---- External Command Entrypoints ----

  /**
   * Amendment 1: records a single-use continuation grant bound to the winning resolution event seq
   * and the proven blocked task id/generation. No-ops when mode !== "on" or authority is not held (zero DDL).
   */
  recordContinuation(input: ContinuationGrantInput): { recorded: boolean } {
    const cfg = this.currentConfig();
    if (!this.isAuthoritative(cfg)) return { recorded: false };
    const now = this.deps.clock.now();
    const esc = this.deps.db.getEscalation(input.escalationId);
    const res = this.deps.store.recordContinuation(input, now, { runId: esc?.runId ?? null });
    this.mirrorJournals([res.journal]);
    return { recorded: res.recorded };
  }

  rearm(causeId: string, reason?: string): InboxTriageRearmResult {
    const cfg = this.currentConfig();
    if (!this.isAuthoritative(cfg)) {
      return {
        causeId,
        rearmed: false,
        reason: cfg.mode !== "on" ? `inbox.mode is '${cfg.mode}'` : "scheduler authority is not held on this host",
      };
    }
    const now = this.deps.clock.now();
    const res = this.deps.store.rearmCause(causeId, now, reason);
    this.mirrorJournals([res.journal]);
    return {
      causeId,
      rearmed: res.rearmed,
      ...(res.cause ? { revision: res.cause.revision } : {}),
      ...(res.reason ? { reason: res.reason } : {}),
    };
  }

  // ---- Pass A: Synchronous Control Pass (NO awaits) ----

  controlPass(): ControlReport {
    const now = this.deps.clock.now();
    const empty: ControlReport = {
      ran: false,
      at: now,
      enumerated: 0,
      acked: 0,
      reopened: 0,
      surfaced: 0,
      notified: 0,
      held: 0,
      released: 0,
      owned: 0,
      ownerLost: 0,
      leaseRenewed: 0,
      reconciledEffects: 0,
      parked: 0,
      resolved: 0,
      batchesPrepared: 0,
      dueActions: [],
    };
    const cfg = this.currentConfig();
    if (!this.isAuthoritative(cfg)) {
      return empty;
    }

    this.deps.store.ensureSchema();
    let facts = this.loadFacts();
    const report: ControlReport = { ...empty, ran: true, dueActions: [] };
    const pendingJournals: TriageJournalRecord[] = [];

    // 0a. Amendment 1: revoke any unconsumed continuation grant whose escalation was reopened or superseded
    // by a later escalation.updated event (even if the reopen happened while the daemon was stopped).
    let continuationsMutated = false;
    for (const [escId, grants] of facts.continuations) {
      const esc = facts.escalations.get(escId);
      const latestEv = facts.latestEscalationUpdated?.get(escId);
      const hasStale = grants.some(
        (g) =>
          g.consumedAt === null &&
          g.revokedAt === null &&
          (!esc ||
            esc.status !== "resolved" ||
            (latestEv !== undefined && (latestEv.status !== "resolved" || latestEv.seq !== g.resolutionEventSeq))),
      );
      if (hasStale) {
        const rev = this.deps.store.revokeContinuations(escId, now, {
          reason: esc && esc.status !== "resolved" ? `escalation_status_${esc.status}` : "resolution_seq_superseded",
          runId: esc?.runId ?? null,
        });
        if (rev.revoked > 0) {
          continuationsMutated = true;
          if (rev.journal) pendingJournals.push(rev.journal);
        }
      }
    }
    if (continuationsMutated) {
      facts = this.loadFacts();
    }

    // 0b. Cold-start batch child adoption: adopt any batch task created in director-tasks-inbox-triage*
    // before its open assignment recorded owner_task_id, so we NEVER spawn a duplicate batch task.
    for (const assign of this.deps.store.listOpenAssignments("batch")) {
      if (assign.ownerTaskId !== null) continue;
      for (const task of facts.tasks.values()) {
        if (!isTriageBatchRun(task.runId)) continue;
        if (parseBatchAssignmentIdFromTitle(task.title) === assign.assignmentId) {
          this.deps.store.setAssignmentOwnerTask(assign.assignmentId, task.id, task.generation);
          break;
        }
      }
    }

    // 1. Reconcile orphaned / timed-out outstanding effects BEFORE routing/dispatch
    report.reconciledEffects += this.reconcileOutstandingEffects(facts, cfg, now, pendingJournals);

    // 2. Enumerate unwindowed source items and classify with pure router
    const items = this.buildAllItems(facts, true);
    report.enumerated = items.length;

    const primaryDecisionByCauseId = new Map<string, { item: InboxItem; decision: TriageDecision }>();
    const derivedItems: Array<{ item: InboxItem; decision: TriageDecision }> = [];
    const sourceRefsByCauseId = new Map<string, EntityRef[]>();

    for (const item of items) {
      const decision = routeItem(item, facts);
      if (decision.ignored) continue;
      if (decision.role === "derived") {
        derivedItems.push({ item, decision });
        for (const targetCid of [decision.causeId, ...(decision.additionalCauseIds ?? [])]) {
          const list = sourceRefsByCauseId.get(targetCid) ?? [];
          list.push(item.ref);
          sourceRefsByCauseId.set(targetCid, list);
        }
      } else {
        primaryDecisionByCauseId.set(decision.causeId, { item, decision });
        const list = sourceRefsByCauseId.get(decision.causeId) ?? [];
        list.unshift(item.ref);
        sourceRefsByCauseId.set(decision.causeId, list);
      }
    }

    // 3. Reopen resolved causes whose source reopened (e.g. ackEscalation(resolve=false)), ack new causes, upsert items
    for (const [causeId, { item, decision }] of primaryDecisionByCauseId) {
      const itemKey = makeItemKey(item.kind, decision.sourceId, decision.sourceEpoch);
      const existing = this.deps.store.getCause(causeId);
      const causeDisposition: CauseDisposition =
        decision.disposition === "derived" ? "routine_owned" : decision.disposition;

      if (!existing) {
        const ackRes = this.deps.store.ackCause(
          {
            causeId,
            originKind: item.kind,
            originSourceId: decision.sourceId,
            runId: decision.runId,
            goalId: decision.goalId,
            repoRoot: decision.repoRoot,
            rule: decision.rule,
            disposition: causeDisposition,
            state: "acknowledged",
            ownerKind: decision.ownerKind,
            ownerId: decision.ownerId,
            ownerEpoch: decision.ownerEpoch,
            nextAction: decision.nextAction,
            nextCheckAt: decision.nextCheckAt ?? now,
            holdReason: decision.holdReason ?? null,
            missingSince: decision.recordMissingSince ? now : null,
            itemKey,
          },
          now,
        );
        if (ackRes.inserted) {
          report.acked += 1;
          if (ackRes.journal) pendingJournals.push(ackRes.journal);
        }
      } else if (existing.state === "resolved") {
        // Ledger-only proof/supersession (R16/R00) intentionally leaves the historical source row intact (P1);
        // re-seeing that same proof/supersession edge must be a complete no-op, not a reopen loop.
        const alreadySettledByLedger =
          (existing.outcomeKind === "proof" && decision.rule === "R16") ||
          (existing.outcomeKind === "superseded" && decision.rule === "R00");
        if (!alreadySettledByLedger) {
          // Reopen the SAME cause with preserved attempt budget & history (no fresh budget!)
          const reopen = this.deps.store.reopenCause(
            {
              causeId,
              expectedRevision: existing.revision,
              rule: decision.rule,
              disposition: causeDisposition,
              nextAction: decision.nextAction,
              nextCheckAt: decision.nextCheckAt ?? now,
              reason: `source ${decision.sourceId} reopened`,
              itemKey,
            },
            now,
          );
          if (reopen.reopened) {
            report.reopened += 1;
            if (reopen.journal) pendingJournals.push(reopen.journal);
          }
        }
      } else {
        // Persist missing_since on first R21 null observation if not yet recorded
        if (decision.recordMissingSince && existing.missingSince === null) {
          this.deps.store.updateCauseMetadata(
            causeId,
            existing.revision,
            {
              rule: decision.rule,
              nextAction: decision.nextAction,
              nextCheckAt: decision.nextCheckAt ?? now + cfg.recoverMaxSec * 1_000,
              missingSince: now,
            },
            now,
          );
        } else if (existing.missingSince !== null && decision.rule === "R20") {
          // Quota state restored; clear missing_since
          this.deps.store.updateCauseMetadata(causeId, existing.revision, { missingSince: null }, now);
        }
      }

      this.deps.store.upsertItem(
        {
          itemKind: item.kind,
          sourceId: decision.sourceId,
          sourceEpoch: decision.sourceEpoch,
          causeId,
          role: "primary",
        },
        now,
      );
    }

    for (const { item, decision } of derivedItems) {
      // Only link derived item when its primary cause exists in ledger
      if (this.deps.store.getCause(decision.causeId)) {
        this.deps.store.upsertItem(
          {
            itemKind: item.kind,
            sourceId: decision.sourceId,
            sourceEpoch: decision.sourceEpoch,
            causeId: decision.causeId,
            role: "derived",
          },
          now,
        );
      }
    }

    // 4. Clear answered / superseded causes STRICTLY from source truth
    const activeCauses = this.deps.store.listCauses({
      states: ["acknowledged", "owned", "held", "surfaced", "parked"],
    });
    for (const cause of activeCauses) {
      const terminal = this.checkSourceTruthResolved(cause, facts);
      if (!terminal) continue;
      const res = this.deps.store.resolveCause(
        {
          causeId: cause.causeId,
          expectedRevision: cause.revision,
          outcomeKind: terminal.outcomeKind,
          outcome: terminal.outcome,
          clearKind: terminal.clearKind,
        },
        now,
      );
      if (res.resolved) {
        report.resolved += 1;
        pendingJournals.push(...res.journals);
      }
    }

    // 5. Apply classification transitions: surface human blockers, holds, releases, passive ownership
    for (const [causeId, { item, decision }] of primaryDecisionByCauseId) {
      const cause = this.deps.store.getCause(causeId);
      if (!cause || cause.state === "resolved") continue;
      const itemKey = makeItemKey(item.kind, decision.sourceId, decision.sourceEpoch);

      // Human blocker -> surface + ask + notified in ONE transaction
      if (decision.disposition === "human_blocker") {
        const refs = dedupeEntityRefs(sourceRefsByCauseId.get(causeId) ?? [item.ref]);
        // Also include restart-interrupted task refs for R10 handoff
        if (decision.rule === "R10" && decision.runId) {
          for (const t of facts.tasksByRun.get(decision.runId) ?? []) {
            if (t.status === "failed" && t.summary === DAEMON_RESTART_SUMMARY) {
              refs.push({ kind: "task", id: t.id, runId: decision.runId, label: t.title });
            }
          }
        }
        const surf = this.deps.store.surfaceCause(
          {
            causeId,
            expectedRevision: cause.revision,
            rule: decision.rule,
            askId: this.nextId("ask", now),
            ask: decision.ask ?? item.title,
            blockedWork: decision.blockedWork ?? formatBlockedWork(decision.runId, decision.goalId, facts),
            sourceRefs: dedupeEntityRefs(refs),
            nextAction: decision.nextAction,
            itemKey,
          },
          now,
        );
        if (surf.surfaced) report.surfaced += 1;
        if (surf.notified) report.notified += 1;
        pendingJournals.push(...surf.journals);
        continue;
      }

      // Held -> holdCause (or keep held)
      if (decision.disposition === "held") {
        if (cause.state !== "held" || cause.holdReason !== (decision.holdReason ?? null)) {
          const held = this.deps.store.holdCause(
            {
              causeId,
              expectedRevision: cause.revision,
              rule: decision.rule,
              holdReason: decision.holdReason ?? "held",
              nextAction: decision.nextAction,
              ownerKind: decision.ownerKind,
              ownerId: decision.ownerId,
              nextCheckAt: decision.nextCheckAt ?? null,
              itemKey,
            },
            now,
          );
          if (held.updated) {
            report.held += 1;
            if (held.journal) pendingJournals.push(held.journal);
          }
        }
        continue;
      }

      // Previously held, now actionable again -> release hold
      if (cause.state === "held") {
        const rel = this.deps.store.releaseHold(
          {
            causeId,
            expectedRevision: cause.revision,
            rule: decision.rule,
            disposition: decision.disposition === "derived" ? "routine_owned" : decision.disposition,
            nextAction: decision.nextAction,
            nextCheckAt: now,
            itemKey,
          },
          now,
        );
        if (rel.updated) {
          report.released += 1;
          if (rel.journal) pendingJournals.push(rel.journal);
        }
      }

      const refreshed = this.deps.store.getCause(causeId);
      if (!refreshed || refreshed.state === "resolved" || refreshed.state === "parked" || refreshed.state === "surfaced") {
        continue;
      }

      // Passive live owner (R03, R09, R17, R19 with live owner and action === null)
      if (decision.disposition === "routine_owned" && !decision.action && decision.ownerKind === "task" && decision.ownerId) {
        const needsAssign =
          refreshed.state === "acknowledged" ||
          (refreshed.state === "owned" &&
            refreshed.ownerKind === "task" &&
            refreshed.ownerId === decision.ownerId &&
            refreshed.ownerEpoch !== decision.ownerEpoch);
        if (needsAssign) {
          const leaseMs = cfg.leaseMin * 60_000;
          const assignMs = Math.min(ASSIGNMENT_MAX_MS, cfg.leaseMin * (cfg.maxNoProgressRenewals + 1) * 60_000);
          const firstAssignedAt = refreshed.firstAssignedAt ?? now;
          const silentCutoffMs = (3 * 20 + 7 + 2) * 60_000;
          const owned = this.deps.store.setOwner(
            {
              causeId,
              expectedRevision: refreshed.revision,
              rule: decision.rule,
              disposition: "routine_owned",
              ownerKind: "task",
              ownerId: decision.ownerId,
              ownerEpoch: decision.ownerEpoch,
              nextAction: decision.nextAction,
              nextCheckAt: now + leaseMs,
              leaseExpiresAt: now + leaseMs,
              assignmentExpiresAt: now + assignMs,
              lastProgressSeq: facts.latestEventSeqByTask.get(decision.ownerId) ?? 0,
              firstAssignedAt,
              absoluteDeadlineAt: firstAssignedAt + silentCutoffMs,
              itemKey,
            },
            now,
          );
          if (owned.updated) {
            report.owned += 1;
            if (owned.journal) pendingJournals.push(owned.journal);
          }
        }
      }
    }

    // 6. Health-check EVERY owned cause every pass (even when not due!)
    this.healthCheckOwnedCauses(facts, cfg, now, report, pendingJournals);

    // 7. Budget exhaustion / absolute deadline check + compute due actions & durable batch attachment
    this.scheduleActionsAndBatches(facts, cfg, now, primaryDecisionByCauseId, sourceRefsByCauseId, report, pendingJournals);

    // 8. Mirror post-commit journal records to ControlPlaneDb events
    this.mirrorJournals(pendingJournals);
    return report;
  }

  private reconcileOutstandingEffects(
    facts: TriageFacts,
    cfg: InboxTriageConfig,
    now: number,
    pendingJournals: TriageJournalRecord[],
  ): number {
    let reconciled = 0;
    const recoverMs = cfg.recoverMaxSec * 1_000;
    for (const effect of this.deps.store.listOutstandingEffects()) {
      const isInFlight = this.inFlightEffects.has(effect.effectKey);
      const timedOut = now - effect.intentAt >= recoverMs;
      if (isInFlight && !timedOut) continue;

      const cause = this.deps.store.getCause(effect.causeId);
      if (effect.status === "intent") {
        // Crashed BEFORE dispatch: positively unstarted; attempt stays counted, retry after backoff.
        const rec = this.deps.store.recordEffectReceipt(
          effect.effectKey,
          "failed",
          "interrupted_before_dispatch",
          now,
          cause && cause.state !== "resolved" && cause.state !== "surfaced"
            ? { expectedRevision: cause.revision, lastActionResult: "interrupted_before_dispatch" }
            : undefined,
        );
        if (rec.recorded) {
          reconciled += 1;
          if (rec.journal) pendingJournals.push(rec.journal);
        }
        continue;
      }

      // status === 'dispatched': verify postcondition on source truth!
      const verified = this.verifyDispatchedEffectPostcondition(effect, facts);
      if (verified.holds) {
        const rec = this.deps.store.recordEffectReceipt(
          effect.effectKey,
          "confirmed",
          verified.receipt,
          now,
          cause && cause.state !== "resolved" && cause.state !== "surfaced"
            ? {
                expectedRevision: cause.revision,
                ...(verified.ownerPatch ?? {}),
                lastActionResult: verified.receipt,
              }
            : undefined,
        );
        if (rec.recorded) {
          reconciled += 1;
          if (rec.journal) pendingJournals.push(rec.journal);
        }
      } else {
        // Ambiguous after dispatch: NEVER replay steers or notes; mark ambiguous.
        // For steers, set nextCheckAt=now so the next pass immediately escalates to rung B (never replaying the steer).
        // For other primitives (resume/batch), preserve the cause's backoff deadline so we never double-fire immediately.
        const isSteer = effect.primitive === "steer_task";
        const receipt = isSteer ? "ambiguous_steer" : "ambiguous_after_dispatch";
        const rec = this.deps.store.recordEffectReceipt(
          effect.effectKey,
          "ambiguous",
          receipt,
          now,
          cause && cause.state !== "resolved" && cause.state !== "surfaced"
            ? {
                expectedRevision: cause.revision,
                state: "acknowledged",
                ownerKind: null,
                ownerId: null,
                ownerEpoch: null,
                leaseExpiresAt: null,
                assignmentExpiresAt: null,
                ...(isSteer ? { nextCheckAt: now } : {}),
                lastActionResult: receipt,
              }
            : undefined,
        );
        if (rec.recorded) {
          reconciled += 1;
          if (rec.journal) pendingJournals.push(rec.journal);
        }
        if (effect.primitive === "launch_batch") {
          const assignmentId = this.extractBatchAssignmentId(effect.targetId);
          const closed = this.deps.store.closeAssignment(assignmentId, "failed", { reason: receipt }, now);
          pendingJournals.push(...closed.journals);
        }
      }
    }
    return reconciled;
  }

  private extractBatchAssignmentId(targetId: string): string {
    if (this.deps.store.getAssignment(targetId)) return targetId;
    const colon = targetId.indexOf(":");
    return colon > 0 ? targetId.slice(0, colon) : targetId;
  }

  private verifyDispatchedEffectPostcondition(
    effect: TriageEffectRecord,
    facts: TriageFacts,
  ): { holds: boolean; receipt: string; ownerPatch?: Partial<EffectCausePatchLite> } {
    switch (effect.primitive) {
      case "resume_task": {
        const task = facts.tasks.get(effect.targetId);
        const expectedGen = Number(effect.targetEpoch.replace(/^g/, "")) || 0;
        if (task && (task.generation > expectedGen || isTaskLive(task.id, facts) || task.status === "completed")) {
          return {
            holds: true,
            receipt: `postcondition_verified:gen_${task.generation}_${task.status}`,
          };
        }
        return { holds: false, receipt: "resume_task_postcondition_unverified" };
      }
      case "resume_run":
      case "resume_goal": {
        const runId =
          effect.primitive === "resume_run"
            ? effect.targetId
            : facts.goals.get(effect.targetId)?.linkedRunId ?? null;
        const run = runId ? facts.runs.get(runId) : null;
        if (run && (run.status === "active" || run.status === "completed")) {
          return { holds: true, receipt: `postcondition_verified:run_${run.status}` };
        }
        const resumedAt = runId ? facts.latestTaskResumedAtByRun?.get(runId) : undefined;
        if (resumedAt !== undefined && resumedAt >= effect.intentAt) {
          return { holds: true, receipt: `postcondition_verified:task_resumed_at_${resumedAt}` };
        }
        return { holds: false, receipt: `${effect.primitive}_postcondition_unverified` };
      }
      case "expire_message": {
        const msg = facts.messages.get(effect.targetId);
        if (msg && (msg.status === "expired" || msg.status === "answered")) {
          return { holds: true, receipt: `postcondition_verified:message_${msg.status}` };
        }
        return { holds: false, receipt: "expire_message_postcondition_unverified" };
      }
      case "reconcile_settled_goal_escalations": {
        const esc = facts.escalations.get(effect.targetId);
        if (esc && esc.status === "resolved") {
          return { holds: true, receipt: "postcondition_verified:escalation_resolved" };
        }
        return { holds: false, receipt: "reconcile_postcondition_unverified" };
      }
      case "launch_batch": {
        const assignmentId = this.extractBatchAssignmentId(effect.targetId);
        const assign = this.deps.store.getAssignment(assignmentId);
        if (assign && assign.ownerTaskId && facts.tasks.has(assign.ownerTaskId)) {
          return {
            holds: true,
            receipt: `postcondition_verified:batch_task_${assign.ownerTaskId}`,
            ownerPatch: {
              state: "owned",
              ownerKind: "batch",
              ownerId: assign.ownerTaskId,
              ownerEpoch: `g${assign.ownerGeneration ?? 1}`,
            },
          };
        }
        return { holds: false, receipt: "launch_batch_postcondition_unverified" };
      }
      case "steer_task":
      default:
        // Peer status alone is NEVER a steer receipt; ambiguous steers must NOT be replayed.
        return { holds: false, receipt: "ambiguous_steer" };
    }
  }

  private checkSourceTruthResolved(
    cause: TriageCauseRecord,
    facts: TriageFacts,
  ): { outcomeKind: CauseOutcomeKind; outcome: string; clearKind?: "answered" | "superseded" } | null {
    switch (cause.originKind) {
      case "escalation": {
        const esc = facts.escalations.get(cause.originSourceId);
        if (esc && esc.status === "resolved") {
          return {
            outcomeKind: cause.state === "surfaced" ? "answered" : "source_resolved",
            outcome: esc.resolution ?? "escalation resolved",
            clearKind: "answered",
          };
        }
        return null;
      }
      case "goal_awaiting_human": {
        const goal = facts.goals.get(cause.originSourceId);
        if (goal && goal.status !== "awaiting_human") {
          const terminal = TERMINAL_GOAL_STATUS_SET.has(goal.status);
          return {
            outcomeKind: terminal ? "superseded" : "answered",
            outcome: `goal ${goal.id} transitioned to ${goal.status}`,
            clearKind: terminal ? "superseded" : "answered",
          };
        }
        return null;
      }
      case "open_message": {
        const msg = facts.messages.get(cause.originSourceId);
        if (msg && msg.status === "answered") {
          return { outcomeKind: "answered", outcome: msg.answer ?? "message answered", clearKind: "answered" };
        }
        if (msg && msg.status === "expired") {
          return {
            outcomeKind: "source_resolved",
            outcome: msg.expiredReason ?? "message expired",
            clearKind: "superseded",
          };
        }
        return null;
      }
      case "task_blocked": {
        const task = facts.tasks.get(cause.originSourceId);
        if (task && task.status !== "blocked" && task.status !== "returned_for_rescope") {
          return { outcomeKind: "source_resolved", outcome: `task ${task.id} is now ${task.status}` };
        }
        return null;
      }
      case "task_failed": {
        const task = facts.tasks.get(cause.originSourceId);
        if (!task) return null;
        if (task.status !== "failed") {
          return { outcomeKind: "source_resolved", outcome: `task ${task.id} is now ${task.status}` };
        }
        const run = facts.runs.get(task.runId);
        if (run && run.status === "completed") {
          return { outcomeKind: "superseded", outcome: `run ${run.id} completed`, clearKind: "superseded" };
        }
        return null;
      }
      case "run_failed": {
        const run = facts.runs.get(cause.originSourceId);
        if (run && run.status !== "failed") {
          return { outcomeKind: "superseded", outcome: `run ${run.id} is now ${run.status}`, clearKind: "superseded" };
        }
        return null;
      }
      case "quota_paused": {
        const run = facts.runs.get(cause.originSourceId);
        if (run && run.status !== "paused_quota") {
          return { outcomeKind: "source_resolved", outcome: `run ${run.id} left paused_quota (${run.status})`, clearKind: "superseded" };
        }
        return null;
      }
      case "merge_bounced": {
        const m = facts.merges.get(cause.originSourceId);
        if (m && m.status === "merged") {
          return { outcomeKind: "source_resolved", outcome: `merge ${m.id} merged` };
        }
        return null;
      }
    }
  }

  private healthCheckOwnedCauses(
    facts: TriageFacts,
    cfg: InboxTriageConfig,
    now: number,
    report: ControlReport,
    pendingJournals: TriageJournalRecord[],
  ): void {
    const leaseMs = cfg.leaseMin * 60_000;
    const ownedCauses = this.deps.store.listOwnedCauses();

    for (const cause of ownedCauses) {
      // Absolute silent-path / lifetime deadline check
      if (cause.absoluteDeadlineAt !== null && now >= cause.absoluteDeadlineAt) {
        if (cause.assignmentId) {
          const closed = this.deps.store.closeAssignment(cause.assignmentId, "expired", { reason: "absolute_deadline" }, now);
          pendingJournals.push(...closed.journals);
        }
        const parked = this.parkCauseWithScopeAsk(cause, "absolute_deadline_reached", "decide follow-up, dismiss, or take over", now, pendingJournals);
        if (parked) report.parked += 1;
        continue;
      }

      if (cause.ownerKind === "task") {
        const ownerId = cause.ownerId;
        const ownerTask = ownerId ? facts.tasks.get(ownerId) : undefined;
        const undeliveredAt = ownerId ? facts.steerUndeliveredAtByTask.get(ownerId) : undefined;

        let lostReason: string | null = null;
        if (!ownerId || !ownerTask) {
          lostReason = "owner_task_missing";
        } else if (!isTaskLive(ownerId, facts)) {
          lostReason = `owner_task_${ownerTask.status}`;
        } else if (undeliveredAt !== undefined && undeliveredAt >= cause.updatedAt) {
          lostReason = "steer_undelivered";
        } else if (cause.assignmentExpiresAt !== null && now >= cause.assignmentExpiresAt) {
          // Absolute 20-minute assignment expiry, even with noisy unrelated events!
          lostReason = "assignment_expired";
        }

        if (lostReason) {
          if (cause.assignmentId) {
            const st = lostReason === "assignment_expired" ? "expired" : "failed";
            const closed = this.deps.store.closeAssignment(cause.assignmentId, st, { reason: lostReason }, now);
            pendingJournals.push(...closed.journals);
          }
          const lost = this.deps.store.markOwnerLost(
            {
              causeId: cause.causeId,
              expectedRevision: cause.revision,
              reason: lostReason,
              nextAction: `reassign after ${lostReason}`,
              nextCheckAt: now,
            },
            now,
          );
          if (lost.updated) {
            report.ownerLost += 1;
            if (lost.journal) pendingJournals.push(lost.journal);
          }
          continue;
        }

        // Owner is live and within assignment window: check lease expiry
        if (cause.leaseExpiresAt !== null && now >= cause.leaseExpiresAt && ownerId) {
          // Cause-specific progress ONLY: non-triage events explicitly on this owner task_id.
          const latestTaskSeq = facts.latestEventSeqByTask.get(ownerId) ?? 0;
          if (latestTaskSeq > cause.lastProgressSeq) {
            const ren = this.deps.store.renewLease(
              {
                causeId: cause.causeId,
                expectedRevision: cause.revision,
                expectedOwnerEpoch: cause.ownerEpoch,
                leaseExpiresAt: now + leaseMs,
                lastProgressSeq: latestTaskSeq,
                noProgressRenewals: 0,
                nextCheckAt: now + leaseMs,
              },
              now,
            );
            if (ren.renewed) {
              report.leaseRenewed += 1;
              if (ren.journal) pendingJournals.push(ren.journal);
            }
          } else {
            const nextNoProgress = cause.noProgressRenewals + 1;
            if (nextNoProgress > cfg.maxNoProgressRenewals) {
              if (cause.assignmentId) {
                const closed = this.deps.store.closeAssignment(
                  cause.assignmentId,
                  "expired",
                  { reason: "silent_owner_lease_exhausted" },
                  now,
                );
                pendingJournals.push(...closed.journals);
              }
              const lost = this.deps.store.markOwnerLost(
                {
                  causeId: cause.causeId,
                  expectedRevision: cause.revision,
                  reason: "silent_owner_lease_exhausted",
                  nextAction: "reassign after silent owner lease exhaustion",
                  nextCheckAt: now,
                },
                now,
              );
              if (lost.updated) {
                report.ownerLost += 1;
                if (lost.journal) pendingJournals.push(lost.journal);
              }
            } else {
              const ren = this.deps.store.renewLease(
                {
                  causeId: cause.causeId,
                  expectedRevision: cause.revision,
                  expectedOwnerEpoch: cause.ownerEpoch,
                  leaseExpiresAt: now + leaseMs,
                  lastProgressSeq: cause.lastProgressSeq,
                  noProgressRenewals: nextNoProgress,
                  nextCheckAt: now + leaseMs,
                },
                now,
              );
              if (ren.renewed) {
                report.leaseRenewed += 1;
                if (ren.journal) pendingJournals.push(ren.journal);
              }
            }
          }
        }
        continue;
      }

      if (cause.ownerKind === "batch") {
        const assign = cause.assignmentId ? this.deps.store.getAssignment(cause.assignmentId) : null;
        if (!assign || assign.status !== "open") {
          const lost = this.deps.store.markOwnerLost(
            {
              causeId: cause.causeId,
              expectedRevision: cause.revision,
              reason: "batch_assignment_closed",
              nextAction: "re-evaluate after batch closed",
              nextCheckAt: now,
            },
            now,
          );
          if (lost.updated) {
            report.ownerLost += 1;
            if (lost.journal) pendingJournals.push(lost.journal);
          }
          continue;
        }
        if (now >= assign.expiresAt) {
          const closed = this.deps.store.closeAssignment(assign.assignmentId, "expired", { reason: "batch_expired" }, now);
          pendingJournals.push(...closed.journals);
          const lost = this.deps.store.markOwnerLost(
            {
              causeId: cause.causeId,
              expectedRevision: cause.revision,
              reason: "batch_expired",
              nextAction: "re-evaluate after 20m batch expiry",
              nextCheckAt: now,
            },
            now,
          );
          if (lost.updated) {
            report.ownerLost += 1;
            if (lost.journal) pendingJournals.push(lost.journal);
          }
          continue;
        }
        if (assign.ownerTaskId) {
          const batchTask = facts.tasks.get(assign.ownerTaskId);
          if (batchTask && (batchTask.status === "failed" || batchTask.status === "cancelled")) {
            const closed = this.deps.store.closeAssignment(
              assign.assignmentId,
              "failed",
              { reason: `batch_task_${batchTask.status}` },
              now,
            );
            pendingJournals.push(...closed.journals);
            const lost = this.deps.store.markOwnerLost(
              {
                causeId: cause.causeId,
                expectedRevision: cause.revision,
                reason: `batch_task_${batchTask.status}`,
                nextAction: "re-evaluate after batch task failure",
                nextCheckAt: now,
              },
              now,
            );
            if (lost.updated) {
              report.ownerLost += 1;
              if (lost.journal) pendingJournals.push(lost.journal);
            }
          } else if (batchTask && batchTask.status === "completed") {
            const outputText = extractBatchTaskOutput(batchTask);
            const ing = this.ingestBatchResultInternal(assign.assignmentId, outputText, facts, now, pendingJournals);
            report.resolved += ing.resolved;
            report.surfaced += ing.surfaced;
            report.parked += ing.parked;
          }
        }
      }
    }
  }

  private scheduleActionsAndBatches(
    facts: TriageFacts,
    cfg: InboxTriageConfig,
    now: number,
    primaryDecisionByCauseId: Map<string, { item: InboxItem; decision: TriageDecision }>,
    sourceRefsByCauseId: Map<string, EntityRef[]>,
    report: ControlReport,
    pendingJournals: TriageJournalRecord[],
  ): void {
    const batchCandidatesByScope = new Map<string, Array<{ cause: TriageCauseRecord; decision: TriageDecision }>>();
    // Supersession/proof settlement rules (R00/R16) apply to ANY non-resolved cause (including surfaced/held/parked).
    const openCauses = this.deps.store.listCauses({
      states: ["acknowledged", "owned", "held", "surfaced", "parked"],
    });

    for (const cause of openCauses) {
      const entry = primaryDecisionByCauseId.get(cause.causeId);
      if (!entry) continue;
      const { item, decision } = entry;
      if (!decision.action) continue;

      const isSupersessionOrProof = decision.rule === "R00" || decision.rule === "R16";
      if (cause.state !== "acknowledged" && !isSupersessionOrProof) continue;

      if (isSupersessionOrProof && decision.action.primitive !== "batch_investigate") {
        report.dueActions.push({
          kind: "cause_action",
          causeId: cause.causeId,
          decision,
          action: decision.action,
          expensive: false,
        });
        continue;
      }

      // Check hard-failure / lifetime budget exhaustion (after 4m grace window or absolute deadline)
      const graceElapsed = cause.nextCheckAt === null || now >= cause.nextCheckAt;
      const absExpired = cause.absoluteDeadlineAt !== null && now >= cause.absoluteDeadlineAt;
      if ((cause.attempts >= cfg.maxAutoAttempts && graceElapsed) || absExpired) {
        if (decision.rule === "R10" && cfg.restartRecovery === "resume") {
          // Contract Section 7 R10: after budget exhaustion -> same single ask as handoff!
          const linkedGoal = decision.goalId ? facts.goals.get(decision.goalId) : null;
          const runId = decision.runId ?? decision.sourceId;
          const ask = linkedGoal
            ? `${runId} of ${linkedGoal.id} was interrupted by a daemon restart: resume in place | cancel`
            : `${runId} was interrupted by a daemon restart: resume in place | cancel`;
          const refs = dedupeEntityRefs(sourceRefsByCauseId.get(cause.causeId) ?? [item.ref]);
          const surf = this.deps.store.surfaceCause(
            {
              causeId: cause.causeId,
              expectedRevision: cause.revision,
              rule: "R10",
              askId: this.nextId("ask", now),
              ask,
              blockedWork: formatBlockedWork(decision.runId, decision.goalId, facts),
              sourceRefs: refs,
              nextAction: "await human decision after restart recovery budget exhausted",
            },
            now,
          );
          if (surf.surfaced) report.surfaced += 1;
          if (surf.notified) report.notified += 1;
          pendingJournals.push(...surf.journals);
        } else {
          // Contract Section 5C: 3 failed attempts -> parked + ONE coalesced scope_decision ask (never N blockers)
          const parked = this.parkCauseWithScopeAsk(
            cause,
            `attempts_exhausted(${cause.attempts}/${cfg.maxAutoAttempts})`,
            decision.nextAction,
            now,
            pendingJournals,
          );
          if (parked) report.parked += 1;
        }
        continue;
      }

      // Not due yet (nudges NEVER shorten backoff!)
      if (cause.nextCheckAt !== null && now < cause.nextCheckAt) continue;
      // Waiting out final grace period with attempts == maxAutoAttempts -> no more dispatches!
      if (cause.attempts >= cfg.maxAutoAttempts) continue;

      // Determine whether a previous refused/ambiguous steer forces rung B (never replay steers!)
      const priorSteerFailed =
        cause.lastActionResult === "ambiguous_steer" ||
        (cause.lastActionResult?.startsWith("steer_refused:") ?? false) ||
        cause.lastActionResult === "steer_undelivered";

      if (decision.action.primitive === "batch_investigate" || (decision.action.primitive === "steer_task" && priorSteerFailed)) {
        const scope = cause.repoRoot ?? "";
        const list = batchCandidatesByScope.get(scope) ?? [];
        list.push({ cause, decision });
        batchCandidatesByScope.set(scope, list);
        continue;
      }

      const cheap =
        decision.action.primitive === "ledger_proof" ||
        decision.action.primitive === "ledger_supersede" ||
        decision.action.primitive === "reconcile_settled_goal_escalations" ||
        decision.action.primitive === "expire_message";

      report.dueActions.push({
        kind: "cause_action",
        causeId: cause.causeId,
        decision,
        action: decision.action,
        expensive: !cheap,
      });
    }

    // Durably attach rung-B causes to per-scope batch assignments (or park when rung B is disabled)
    const rungBEnabled = this.isRungBEnabled(cfg);
    for (const [scope, candidates] of batchCandidatesByScope) {
      if (!rungBEnabled) {
        for (const { cause, decision } of candidates) {
          const parked = this.parkCauseWithScopeAsk(
            cause,
            "rung_b_disabled",
            decision.nextAction,
            now,
            pendingJournals,
          );
          if (parked) report.parked += 1;
        }
        continue;
      }

      const existingBatch = this.deps.store.getOpenBatchForScope(scope);
      if (existingBatch) {
        const existingMembers = new Set(this.deps.store.listAssignmentMembers(existingBatch.assignmentId));
        if (existingBatch.ownerTaskId === null) {
          // Batch created but not yet launched: attach up to batchMaxItems and queue launch!
          const room = Math.max(0, cfg.batchMaxItems - existingMembers.size);
          const toAttach = candidates
            .map((c) => c.cause.causeId)
            .filter((cid) => !existingMembers.has(cid))
            .slice(0, room);
          if (toAttach.length > 0) {
            const added = this.deps.store.addAssignmentMembers(existingBatch.assignmentId, toAttach, now);
            pendingJournals.push(...added.journals);
          }
          const finalMembers = this.deps.store.listAssignmentMembers(existingBatch.assignmentId);
          report.dueActions.push({
            kind: "batch_launch",
            assignmentId: existingBatch.assignmentId,
            scope,
            memberCauseIds: finalMembers,
            brief: this.buildBatchBrief(scope, finalMembers, primaryDecisionByCauseId),
            expensive: true,
          });
        } else {
          // Batch already running for this scope; overflow stays durably queued for the next batch.
          for (const { cause } of candidates) {
            if (existingMembers.has(cause.causeId)) continue;
            if (!cause.nextAction.startsWith("queued for next batch")) {
              this.deps.store.updateCauseMetadata(
                cause.causeId,
                cause.revision,
                { nextAction: `queued for next batch in scope ${scope || "(global)"} (active ${existingBatch.assignmentId})` },
                now,
              );
            }
          }
        }
        continue;
      }

      // Create ONE new open batch for this scope (bounded by batchMaxItems)
      const slice = candidates.slice(0, cfg.batchMaxItems);
      const memberCauseIds = slice.map((c) => c.cause.causeId);
      const assignmentId = this.nextId("batch", now);
      const created = this.deps.store.createAssignment(
        {
          assignmentId,
          scope,
          kind: "batch",
          ownerTaskId: null,
          ownerGeneration: null,
          expiresAt: now + ASSIGNMENT_MAX_MS,
          causeIds: memberCauseIds,
        },
        now,
      );
      if (created.created && created.assignment) {
        report.batchesPrepared += 1;
        pendingJournals.push(...created.journals);
        report.dueActions.push({
          kind: "batch_launch",
          assignmentId,
          scope,
          memberCauseIds,
          brief: this.buildBatchBrief(scope, memberCauseIds, primaryDecisionByCauseId),
          expensive: true,
        });
        // Mark overflow beyond batchMaxItems durably attached to next batch
        for (const { cause } of candidates.slice(cfg.batchMaxItems)) {
          this.deps.store.updateCauseMetadata(
            cause.causeId,
            cause.revision,
            { nextAction: `queued for next batch in scope ${scope || "(global)"} (after ${assignmentId})` },
            now,
          );
        }
      }
    }
  }

  private buildBatchBrief(
    scope: string,
    memberCauseIds: string[],
    primaryDecisionByCauseId: Map<string, { item: InboxItem; decision: TriageDecision }>,
  ): string {
    const lines = [
      `Read-only triage investigation batch for scope ${scope || "(global)"}.`,
      `Investigate the ${memberCauseIds.length} causes below and return JSON inside <triage_outcomes>...</triage_outcomes>:`,
    ];
    for (const cid of memberCauseIds) {
      const entry = primaryDecisionByCauseId.get(cid);
      const c = this.deps.store.getCause(cid);
      lines.push(
        `- ${cid} (${entry?.decision.rule ?? c?.rule ?? "?"}): ${entry?.item.title ?? c?.nextAction ?? cid}`,
      );
    }
    return lines.join("\n");
  }

  private parkCauseWithScopeAsk(
    cause: TriageCauseRecord,
    reason: string,
    nextAction: string,
    now: number,
    pendingJournals: TriageJournalRecord[],
  ): boolean {
    const scope = cause.repoRoot ?? "";
    const existingParked = this.deps.store
      .listCauses({ state: "parked" })
      .filter((c) => (c.repoRoot ?? "") === scope);
    const refs = dedupeEntityRefs([
      ...existingParked.map(makeDefaultRefForCause),
      makeDefaultRefForCause(cause),
    ]);
    const parked = this.deps.store.parkCause(
      {
        causeId: cause.causeId,
        expectedRevision: cause.revision,
        reason,
        nextAction,
        scope,
        scopeAskId: this.nextId("ask-scope", now),
        scopeSourceRefs: refs,
      },
      now,
    );
    pendingJournals.push(...parked.journals);
    return parked.parked;
  }

  private computeBackoffTiming(
    cause: TriageCauseRecord,
    cfg: InboxTriageConfig,
    now: number,
  ): { nextCheckAt: number; absoluteDeadlineAt: number } {
    const backoff = cfg.backoffMin.length > 0 ? cfg.backoffMin : [1, 2, 4];
    const stepMin = backoff[Math.min(cause.attempts, backoff.length - 1)] ?? 4;
    const nextCheckAt = now + stepMin * 60_000;
    const firstDispatchAt = cause.firstDispatchAt ?? now;
    const hardCutoffMin = backoff.reduce((a, b) => a + b, 0) + 2; // default 1+2+4+2 = 9 min
    const hardDeadline = firstDispatchAt + hardCutoffMin * 60_000;
    const absoluteDeadlineAt =
      cause.absoluteDeadlineAt !== null ? Math.min(cause.absoluteDeadlineAt, hardDeadline) : hardDeadline;
    return { nextCheckAt, absoluteDeadlineAt };
  }

  // ---- Pass B: Async Action Pass (bounded by maxDispatchPerTick) ----

  async actionPass(dueActions: DueTriageAction[] = []): Promise<ActionReport> {
    const startNow = this.deps.clock.now();
    const report: ActionReport = {
      ran: false,
      at: startNow,
      dispatched: 0,
      expensiveDispatched: 0,
      confirmed: 0,
      failed: 0,
      ambiguous: 0,
      skippedVeto: 0,
      skippedBusyOrBudget: 0,
    };
    const cfg = this.currentConfig();
    if (!this.isAuthoritative(cfg)) {
      return report;
    }

    report.ran = true;
    const startEpoch = this.authorityEpoch;
    const pendingJournals: TriageJournalRecord[] = [];

    for (const due of dueActions) {
      if (!this.isAuthoritative() || this.authorityEpoch !== startEpoch) break;
      if (due.expensive && report.expensiveDispatched >= cfg.maxDispatchPerTick) continue;

      if (due.kind === "batch_launch") {
        await this.executeBatchLaunch(due, startEpoch, report, pendingJournals);
        continue;
      }

      await this.executeCauseAction(due, startEpoch, report, pendingJournals);
    }

    this.mirrorJournals(pendingJournals);
    return report;
  }

  private async executeCauseAction(
    due: Extract<DueTriageAction, { kind: "cause_action" }>,
    startEpoch: number,
    report: ActionReport,
    pendingJournals: TriageJournalRecord[],
  ): Promise<void> {
    const cfg = this.currentConfig();
    const now = this.deps.clock.now();
    const cause = this.deps.store.getCause(due.causeId);
    const isSupersessionOrProof = due.decision.rule === "R00" || due.decision.rule === "R16";
    if (!cause || cause.state === "resolved" || (!isSupersessionOrProof && cause.state !== "acknowledged")) return;

    // Re-read fresh facts & re-evaluate veto BEFORE claim
    const freshFacts = this.loadFacts();
    const action = due.action;
    const vetoTarget: VetoTarget = {
      runId: cause.runId,
      goalId: cause.goalId,
      taskId: action.targetKind === "task" ? action.targetId : undefined,
      expectedGeneration: action.primitive === "resume_task" ? action.expectedGeneration : undefined,
      expectedStatus: action.primitive === "resume_task" ? "blocked" : undefined,
      allowSettledGoal: due.decision.rule === "R00",
      allowStoppedRun: due.decision.rule === "R18" || due.decision.rule === "R00",
      ignoreBlockingEscalationIds:
        action.primitive === "resume_task" ? new Set(action.escalationIds) : undefined,
    };
    const veto = evaluateVeto(vetoTarget, freshFacts);
    if (veto.vetoed) {
      report.skippedVeto += 1;
      if (veto.code !== "mode_not_on" && veto.code !== "authority_lost") {
        const held = this.deps.store.holdCause(
          {
            causeId: cause.causeId,
            expectedRevision: cause.revision,
            holdReason: veto.holdReason ?? "vetoed",
            nextAction: `held (${veto.holdReason ?? "vetoed"})`,
          },
          now,
        );
        if (held.journal) pendingJournals.push(held.journal);
      }
      return;
    }

    // Cheap local ledger-only proof/supersession actions do not consume external dispatch cap
    if (action.primitive === "ledger_proof") {
      const res = this.deps.store.resolveCause(
        {
          causeId: cause.causeId,
          expectedRevision: cause.revision,
          outcomeKind: "proof",
          outcome: action.proof,
          clearKind: "superseded",
        },
        now,
      );
      if (res.resolved) {
        report.confirmed += 1;
        pendingJournals.push(...res.journals);
      }
      return;
    }
    if (action.primitive === "ledger_supersede") {
      const res = this.deps.store.resolveCause(
        {
          causeId: cause.causeId,
          expectedRevision: cause.revision,
          outcomeKind: "superseded",
          outcome: action.reason,
          clearKind: "superseded",
        },
        now,
      );
      if (res.resolved) {
        report.confirmed += 1;
        pendingJournals.push(...res.journals);
      }
      return;
    }

    // Claim durable intent BEFORE the external side effect (counts attempt & fences target via unique index)
    const { nextCheckAt, absoluteDeadlineAt } = this.computeBackoffTiming(cause, cfg, now);
    const claim = this.deps.store.claimEffectIntent(
      {
        causeId: cause.causeId,
        expectedRevision: cause.revision,
        maxAutoAttempts: isSupersessionOrProof ? Math.max(cfg.maxAutoAttempts, cause.attempts + 1) : cfg.maxAutoAttempts,
        primitive: action.primitive,
        targetKind: action.targetKind,
        targetId: action.targetId,
        targetEpoch: action.targetEpoch,
        nextCheckAt,
        absoluteDeadlineAt,
        allowBypassDueCheck: isSupersessionOrProof,
        allowAnyOpenState: isSupersessionOrProof,
        ...(action.primitive === "resume_task"
          ? {
              continuationCheck: {
                taskId: action.targetId,
                taskGeneration: action.expectedGeneration,
                grants: action.continuationGrants,
              },
            }
          : {}),
      },
      now,
    );
    if (!claim.claimed || !claim.effect || !claim.cause) {
      report.skippedBusyOrBudget += 1;
      return;
    }
    if (claim.journal) pendingJournals.push(claim.journal);
    const effectKey = claim.effect.effectKey;
    const claimedRevision = claim.cause.revision;

    if (this.deps.onAfterClaim) {
      await this.deps.onAfterClaim(effectKey);
    }

    // Re-check authority & veto RIGHT BEFORE activation (catches hostname mismatch / disarm / freeze between claim & activation!)
    const preActivateFacts = this.loadFacts();
    const preActivateVeto = evaluateVeto(vetoTarget, preActivateFacts);
    if (!this.isAuthoritative() || this.authorityEpoch !== startEpoch || preActivateVeto.vetoed) {
      const reason =
        !this.isAuthoritative() || this.authorityEpoch !== startEpoch
          ? "authority_lost_before_activation"
          : `vetoed_before_activation:${preActivateVeto.code}`;
      const rec = this.deps.store.recordEffectReceipt(effectKey, "failed", reason, this.deps.clock.now(), {
        expectedRevision: claimedRevision,
        lastActionResult: reason,
      });
      if (rec.journal) pendingJournals.push(rec.journal);
      report.skippedVeto += 1;
      return;
    }

    this.deps.store.markEffectDispatched(effectKey, this.deps.clock.now());
    this.inFlightEffects.add(effectKey);
    report.dispatched += 1;
    if (due.expensive) report.expensiveDispatched += 1;

    try {
      await this.dispatchActionPrimitive(action, cause, effectKey, claimedRevision, cfg, report, pendingJournals);
    } finally {
      this.inFlightEffects.delete(effectKey);
    }
  }

  private async dispatchActionPrimitive(
    action: Exclude<TriageActionSpec, { primitive: "batch_investigate" | "ledger_proof" | "ledger_supersede" }>,
    cause: TriageCauseRecord,
    effectKey: string,
    claimedRevision: number,
    cfg: InboxTriageConfig,
    report: ActionReport,
    pendingJournals: TriageJournalRecord[],
  ): Promise<void> {
    const leaseMs = cfg.leaseMin * 60_000;
    const assignMs = Math.min(ASSIGNMENT_MAX_MS, cfg.leaseMin * (cfg.maxNoProgressRenewals + 1) * 60_000);
    const silentCutoffMs = (3 * 20 + 7 + 2) * 60_000;

    const recordDispatchError = (err: unknown): void => {
      const msg = err instanceof Error ? err.message : String(err);
      const status = action.primitive === "steer_task" ? "ambiguous" : "failed";
      const receipt = action.primitive === "steer_task" ? "ambiguous_steer" : `error:${msg.slice(0, 120)}`;
      const rec = this.deps.store.recordEffectReceipt(effectKey, status, receipt, this.deps.clock.now(), {
        expectedRevision: claimedRevision,
        lastActionResult: receipt,
      });
      if (rec.journal) pendingJournals.push(rec.journal);
      if (status === "ambiguous") report.ambiguous += 1;
      else report.failed += 1;
    };

    switch (action.primitive) {
      case "reconcile_settled_goal_escalations": {
        try {
          this.deps.db.reconcileSettledGoalEscalations();
        } catch (err) {
          recordDispatchError(err);
          return;
        }
        if (this.deps.onBeforeReceipt) await this.deps.onBeforeReceipt(effectKey);
        const now = this.deps.clock.now();
        const esc = this.deps.db.getEscalation(action.targetId);
        if (esc && esc.status === "resolved") {
          const rec = this.deps.store.recordEffectReceipt(effectKey, "confirmed", "reconciled", now);
          if (rec.journal) pendingJournals.push(rec.journal);
          const cur = this.deps.store.getCause(cause.causeId);
          if (cur && cur.state !== "resolved") {
            const res = this.deps.store.resolveCause(
              {
                causeId: cur.causeId,
                expectedRevision: cur.revision,
                outcomeKind: "superseded",
                outcome: esc.resolution ?? "all linked goals settled",
                clearKind: "superseded",
              },
              now,
            );
            pendingJournals.push(...res.journals);
          }
          report.confirmed += 1;
        } else {
          const rec = this.deps.store.recordEffectReceipt(effectKey, "failed", "not_reconciled", now, {
            expectedRevision: claimedRevision,
            lastActionResult: "not_reconciled",
          });
          if (rec.journal) pendingJournals.push(rec.journal);
          report.failed += 1;
        }
        return;
      }

      case "expire_message": {
        let exp: ReturnType<ControlPlaneDb["expireMessage"]>;
        try {
          exp = this.deps.db.expireMessage(action.targetId, action.reason, this.deps.clock.now());
        } catch (err) {
          recordDispatchError(err);
          return;
        }
        if (this.deps.onBeforeReceipt) await this.deps.onBeforeReceipt(effectKey);
        const now = this.deps.clock.now();
        const receipt = exp.transitioned ? "expired" : `already_${exp.message?.status ?? "missing"}`;
        const rec = this.deps.store.recordEffectReceipt(effectKey, "confirmed", receipt, now);
        if (rec.journal) pendingJournals.push(rec.journal);
        const cur = this.deps.store.getCause(cause.causeId);
        if (cur && cur.state !== "resolved" && exp.message) {
          const res = this.deps.store.resolveCause(
            {
              causeId: cur.causeId,
              expectedRevision: cur.revision,
              outcomeKind: exp.message.status === "answered" ? "answered" : "source_resolved",
              outcome:
                exp.message.status === "answered"
                  ? exp.message.answer ?? "answered"
                  : exp.message.expiredReason ?? action.reason,
            },
            now,
          );
          pendingJournals.push(...res.journals);
        }
        // Live asker gets at most ONE best-effort steer (never replayed)
        if (exp.transitioned && action.notifyAskerTaskId) {
          await this.deps.dispatcher
            .steerTask(
              action.notifyAskerTaskId,
              `Message ${action.targetId} expired (${action.reason}). Continue without awaiting a reply.`,
            )
            .catch(() => {});
        }
        report.confirmed += 1;
        return;
      }

      case "resume_task": {
        let res: { outcome: string; generation?: number };
        try {
          res = await this.deps.dispatcher.resumeTask(action.targetId, action.expectedGeneration);
        } catch (err) {
          recordDispatchError(err);
          return;
        }
        if (this.deps.onBeforeReceipt) await this.deps.onBeforeReceipt(effectKey);
        const now = this.deps.clock.now();
        if (res.outcome === "resumed" || res.outcome === "already_active" || res.outcome === "already_queued") {
          const newGen = res.generation ?? action.expectedGeneration + 1;
          const firstAssignedAt = cause.firstAssignedAt ?? now;
          const rec = this.deps.store.recordEffectReceipt(effectKey, "confirmed", res.outcome, now, {
            expectedRevision: claimedRevision,
            state: "owned",
            ownerKind: "task",
            ownerId: action.targetId,
            ownerEpoch: `g${newGen}`,
            nextAction: `resumed ${action.targetId} at gen ${newGen}`,
            nextCheckAt: now + leaseMs,
            leaseExpiresAt: now + leaseMs,
            assignmentExpiresAt: now + assignMs,
            firstAssignedAt,
            absoluteDeadlineAt: firstAssignedAt + silentCutoffMs,
            lastActionResult: res.outcome,
          });
          if (rec.journal) pendingJournals.push(rec.journal);
          report.confirmed += 1;
        } else if (res.outcome === "already_completed") {
          const rec = this.deps.store.recordEffectReceipt(effectKey, "confirmed", res.outcome, now);
          if (rec.journal) pendingJournals.push(rec.journal);
          const cur = this.deps.store.getCause(cause.causeId);
          if (cur && cur.state !== "resolved") {
            const resolved = this.deps.store.resolveCause(
              {
                causeId: cur.causeId,
                expectedRevision: cur.revision,
                outcomeKind: "source_resolved",
                outcome: "task already completed",
              },
              now,
            );
            pendingJournals.push(...resolved.journals);
          }
          report.confirmed += 1;
        } else {
          // 'waiting' or failure is NOT counted as resumed; do not consume continuation!
          const rec = this.deps.store.recordEffectReceipt(effectKey, "failed", res.outcome, now, {
            expectedRevision: claimedRevision,
            lastActionResult: res.outcome,
          });
          if (rec.journal) pendingJournals.push(rec.journal);
          report.failed += 1;
        }
        return;
      }

      case "resume_goal":
      case "resume_run": {
        let res: { outcome: string };
        try {
          res =
            action.primitive === "resume_goal"
              ? await this.deps.dispatcher.resumeGoal(action.targetId, action.runId)
              : await this.deps.dispatcher.resumeRun(action.targetId);
        } catch (err) {
          recordDispatchError(err);
          return;
        }
        if (this.deps.onBeforeReceipt) await this.deps.onBeforeReceipt(effectKey);
        const now = this.deps.clock.now();
        if (res.outcome === "resumed" || res.outcome === "already_active") {
          const firstAssignedAt = cause.firstAssignedAt ?? now;
          const rec = this.deps.store.recordEffectReceipt(effectKey, "confirmed", res.outcome, now, {
            expectedRevision: claimedRevision,
            state: "owned",
            ownerKind: "daemon",
            ownerId: action.targetId,
            ownerEpoch: action.targetEpoch,
            nextAction: `resumed ${action.targetId} (${res.outcome})`,
            nextCheckAt: now + leaseMs,
            leaseExpiresAt: now + leaseMs,
            assignmentExpiresAt: now + assignMs,
            firstAssignedAt,
            absoluteDeadlineAt: firstAssignedAt + silentCutoffMs,
            lastActionResult: res.outcome,
          });
          if (rec.journal) pendingJournals.push(rec.journal);
          report.confirmed += 1;
        } else {
          // nothing_to_resume includes wind-down errors, not proof -> record failed so backoff/handoff governs
          const rec = this.deps.store.recordEffectReceipt(effectKey, "failed", res.outcome, now, {
            expectedRevision: claimedRevision,
            lastActionResult: res.outcome,
          });
          if (rec.journal) pendingJournals.push(rec.journal);
          report.failed += 1;
        }
        return;
      }

      case "steer_task": {
        let res: { steered: boolean; code?: string };
        try {
          res = await this.deps.dispatcher.steerTask(action.targetId, action.message);
        } catch (err) {
          recordDispatchError(err);
          return;
        }
        if (this.deps.onBeforeReceipt) await this.deps.onBeforeReceipt(effectKey);
        const now = this.deps.clock.now();
        if (res.steered) {
          const assignId = this.nextId("steer", now);
          const targetTask = this.deps.db.getTask(action.targetId);
          const assignRes = this.deps.store.createAssignment(
            {
              assignmentId: assignId,
              scope: cause.repoRoot ?? "",
              kind: "steer",
              ownerTaskId: action.targetId,
              ownerGeneration: targetTask?.generation ?? null,
              expiresAt: now + assignMs,
              causeIds: [cause.causeId],
            },
            now,
          );
          pendingJournals.push(...assignRes.journals);
          const firstAssignedAt = cause.firstAssignedAt ?? now;
          const rec = this.deps.store.recordEffectReceipt(effectKey, "confirmed", "steered", now, {
            expectedRevision: claimedRevision,
            state: "owned",
            ownerKind: "task",
            ownerId: action.targetId,
            ownerEpoch: action.targetEpoch,
            assignmentId: assignId,
            nextAction: `steered ${action.targetId}; awaiting progress`,
            nextCheckAt: now + leaseMs,
            leaseExpiresAt: now + leaseMs,
            assignmentExpiresAt: now + assignMs,
            firstAssignedAt,
            absoluteDeadlineAt: firstAssignedAt + silentCutoffMs,
            lastActionResult: "steered",
          });
          if (rec.journal) pendingJournals.push(rec.journal);
          report.confirmed += 1;
        } else {
          // Refused steer is NEVER replayed -> mark steer_refused so next pass routes to rung B
          const receipt = `steer_refused:${res.code ?? "refused"}`;
          const rec = this.deps.store.recordEffectReceipt(effectKey, "failed", receipt, now, {
            expectedRevision: claimedRevision,
            nextCheckAt: now, // immediately eligible for rung B reassignment
            lastActionResult: receipt,
          });
          if (rec.journal) pendingJournals.push(rec.journal);
          report.failed += 1;
        }
        return;
      }
    }
  }

  private async executeBatchLaunch(
    due: Extract<DueTriageAction, { kind: "batch_launch" }>,
    startEpoch: number,
    report: ActionReport,
    pendingJournals: TriageJournalRecord[],
  ): Promise<void> {
    const cfg = this.currentConfig();
    const launchBatch = this.deps.dispatcher.launchBatch;
    if (!launchBatch || !this.isRungBEnabled(cfg)) return;

    const assign = this.deps.store.getAssignment(due.assignmentId);
    if (!assign || assign.status !== "open" || assign.ownerTaskId !== null) return;

    const now = this.deps.clock.now();
    // Claim intent for every eligible member cause BEFORE launching the batch task
    const claimedMembers: Array<{ causeId: string; effectKey: string; revision: number; cause: TriageCauseRecord }> = [];
    for (const cid of due.memberCauseIds) {
      const c = this.deps.store.getCause(cid);
      if (!c || c.state !== "acknowledged") continue;
      const { nextCheckAt, absoluteDeadlineAt } = this.computeBackoffTiming(c, cfg, now);
      const claim = this.deps.store.claimEffectIntent(
        {
          causeId: c.causeId,
          expectedRevision: c.revision,
          maxAutoAttempts: cfg.maxAutoAttempts,
          primitive: "launch_batch",
          targetKind: "batch_member",
          targetId: `${due.assignmentId}:${c.causeId}`,
          targetEpoch: "",
          nextCheckAt,
          absoluteDeadlineAt,
        },
        now,
      );
      if (claim.claimed && claim.effect && claim.cause) {
        if (claim.journal) pendingJournals.push(claim.journal);
        claimedMembers.push({
          causeId: c.causeId,
          effectKey: claim.effect.effectKey,
          revision: claim.cause.revision,
          cause: claim.cause,
        });
      }
    }

    if (claimedMembers.length === 0) {
      report.skippedBusyOrBudget += 1;
      return;
    }

    if (this.deps.onAfterClaim) {
      await this.deps.onAfterClaim(claimedMembers[0]!.effectKey);
    }

    // Re-check authority right before calling launchBatch
    if (!this.isAuthoritative() || this.authorityEpoch !== startEpoch) {
      const t = this.deps.clock.now();
      for (const m of claimedMembers) {
        const rec = this.deps.store.recordEffectReceipt(m.effectKey, "failed", "authority_lost_before_activation", t, {
          expectedRevision: m.revision,
          lastActionResult: "authority_lost_before_activation",
        });
        if (rec.journal) pendingJournals.push(rec.journal);
      }
      const closed = this.deps.store.closeAssignment(due.assignmentId, "failed", { reason: "authority_lost" }, t);
      pendingJournals.push(...closed.journals);
      report.skippedVeto += 1;
      return;
    }

    for (const m of claimedMembers) {
      this.deps.store.markEffectDispatched(m.effectKey, this.deps.clock.now());
      this.inFlightEffects.add(m.effectKey);
    }
    report.dispatched += 1;
    report.expensiveDispatched += 1;

    try {
      const launched = await launchBatch(due.assignmentId, due.scope, due.brief);
      if (this.deps.onBeforeReceipt) await this.deps.onBeforeReceipt(claimedMembers[0]!.effectKey);
      const t = this.deps.clock.now();
      if (!launched) {
        const closed = this.deps.store.closeAssignment(due.assignmentId, "failed", { reason: "batch_launch_returned_null" }, t);
        pendingJournals.push(...closed.journals);
        for (const m of claimedMembers) {
          const rec = this.deps.store.recordEffectReceipt(m.effectKey, "failed", "batch_launch_returned_null", t, {
            expectedRevision: m.revision,
            lastActionResult: "batch_launch_returned_null",
          });
          if (rec.journal) pendingJournals.push(rec.journal);
        }
        report.failed += 1;
        return;
      }

      this.deps.store.setAssignmentOwnerTask(due.assignmentId, launched.taskId, launched.generation);
      const leaseMs = cfg.leaseMin * 60_000;
      const silentCutoffMs = (3 * 20 + 7 + 2) * 60_000;
      for (const m of claimedMembers) {
        const firstAssignedAt = m.cause.firstAssignedAt ?? t;
        const rec = this.deps.store.recordEffectReceipt(
          m.effectKey,
          "confirmed",
          `batch_launched:${launched.taskId}@g${launched.generation}`,
          t,
          {
            expectedRevision: m.revision,
            state: "owned",
            ownerKind: "batch",
            ownerId: launched.taskId,
            ownerEpoch: `g${launched.generation}`,
            assignmentId: due.assignmentId,
            nextAction: `assigned to batch ${due.assignmentId} (${launched.taskId})`,
            nextCheckAt: t + leaseMs,
            leaseExpiresAt: t + leaseMs,
            assignmentExpiresAt: assign.expiresAt,
            firstAssignedAt,
            absoluteDeadlineAt: firstAssignedAt + silentCutoffMs,
            lastActionResult: `batch_launched:${launched.taskId}`,
          },
        );
        if (rec.journal) pendingJournals.push(rec.journal);
      }
      report.confirmed += 1;
    } catch (err) {
      const t = this.deps.clock.now();
      const msg = err instanceof Error ? err.message : String(err);
      const closed = this.deps.store.closeAssignment(due.assignmentId, "failed", { error: msg }, t);
      pendingJournals.push(...closed.journals);
      for (const m of claimedMembers) {
        const rec = this.deps.store.recordEffectReceipt(m.effectKey, "failed", `batch_error:${msg.slice(0, 100)}`, t, {
          expectedRevision: m.revision,
          lastActionResult: `batch_error:${msg.slice(0, 100)}`,
        });
        if (rec.journal) pendingJournals.push(rec.journal);
      }
      report.failed += 1;
    } finally {
      for (const m of claimedMembers) {
        this.inFlightEffects.delete(m.effectKey);
      }
    }
  }

  // ---- Serialized Two-Pass Sweep ----

  async sweepOnce(): Promise<SweepReport> {
    if (this.sweeping) {
      this.pendingRerun = true;
      const now = this.deps.clock.now();
      return {
        skipped: true,
        control: {
          ran: false,
          at: now,
          enumerated: 0,
          acked: 0,
          reopened: 0,
          surfaced: 0,
          notified: 0,
          held: 0,
          released: 0,
          owned: 0,
          ownerLost: 0,
          leaseRenewed: 0,
          reconciledEffects: 0,
          parked: 0,
          resolved: 0,
          batchesPrepared: 0,
          dueActions: [],
        },
        action: {
          ran: false,
          at: now,
          dispatched: 0,
          expensiveDispatched: 0,
          confirmed: 0,
          failed: 0,
          ambiguous: 0,
          skippedVeto: 0,
          skippedBusyOrBudget: 0,
        },
      };
    }

    this.sweeping = true;
    try {
      let lastControl!: ControlReport;
      let lastAction!: ActionReport;
      do {
        this.pendingRerun = false;
        lastControl = this.controlPass();
        lastAction =
          lastControl.ran && lastControl.dueActions.length > 0
            ? await this.actionPass(lastControl.dueActions)
            : {
                ran: lastControl.ran,
                at: this.deps.clock.now(),
                dispatched: 0,
                expensiveDispatched: 0,
                confirmed: 0,
                failed: 0,
                ambiguous: 0,
                skippedVeto: 0,
                skippedBusyOrBudget: 0,
              };
      } while (this.pendingRerun && this.isAuthoritative());

      return { skipped: false, control: lastControl, action: lastAction };
    } finally {
      this.sweeping = false;
    }
  }

  // ---- Batch Outcome Ingestion (Contract Section 5B) ----

  ingestBatchResult(assignmentId: string, summaryText: string): BatchIngestReport {
    const cfg = this.currentConfig();
    if (!this.isAuthoritative(cfg)) {
      return {
        assignmentId,
        closed: false,
        parsed: false,
        resolved: 0,
        surfaced: 0,
        parked: 0,
        ignoredUnknown: 0,
        unverifiedProofs: 0,
      };
    }
    const now = this.deps.clock.now();
    const facts = this.loadFacts();
    const pendingJournals: TriageJournalRecord[] = [];
    const result = this.ingestBatchResultInternal(assignmentId, summaryText, facts, now, pendingJournals);
    this.mirrorJournals(pendingJournals);
    return result;
  }

  private ingestBatchResultInternal(
    assignmentId: string,
    summaryText: string,
    facts: TriageFacts,
    now: number,
    pendingJournals: TriageJournalRecord[],
  ): BatchIngestReport {
    const cfg = this.currentConfig();
    const parsed = parseBatchOutcomes(summaryText);
    const closed = this.deps.store.closeAssignment(
      assignmentId,
      parsed ? "returned" : "failed",
      parsed ?? { error: "unparsable_triage_outcomes" },
      now,
    );
    pendingJournals.push(...closed.journals);

    const out: BatchIngestReport = {
      assignmentId,
      closed: closed.closed,
      parsed: parsed !== null,
      resolved: 0,
      surfaced: 0,
      parked: 0,
      ignoredUnknown: 0,
      unverifiedProofs: 0,
    };
    if (!closed.closed) return out;

    const memberSet = new Set(closed.memberCauseIds);

    // Unparsable output -> count one attempt per member (if not already counted by batch launch) & return to acknowledged/parked
    if (!parsed) {
      for (const cid of closed.memberCauseIds) {
        const cause = this.deps.store.getCause(cid);
        if (!cause || cause.state === "resolved" || cause.state === "surfaced") continue;
        this.countBatchFailureForMember(cause, assignmentId, "unparsable_batch_output", cfg, now, pendingJournals, out);
      }
      return out;
    }

    const handledMembers = new Set<string>();
    for (const item of parsed) {
      // Unknown cause_id MUST be ignored!
      if (!memberSet.has(item.cause_id)) {
        out.ignoredUnknown += 1;
        continue;
      }
      handledMembers.add(item.cause_id);
      const cause = this.deps.store.getCause(item.cause_id);
      if (!cause || cause.state === "resolved" || cause.state === "surfaced") continue;

      if (item.outcome === "needs_human" && item.ask && item.ask.trim().length > 0) {
        const surf = this.deps.store.surfaceCause(
          {
            causeId: cause.causeId,
            expectedRevision: cause.revision,
            askId: this.nextId("ask", now),
            ask: item.ask.trim(),
            blockedWork: formatBlockedWork(cause.runId, cause.goalId, facts),
            sourceRefs: [makeDefaultRefForCause(cause)],
            nextAction: item.next_action ?? "await human decision",
          },
          now,
        );
        if (surf.surfaced) out.surfaced += 1;
        pendingJournals.push(...surf.journals);
        continue;
      }

      if (item.outcome === "proof") {
        // Daemon closes a source row ONLY when it can re-verify the evidence itself from SQLite (R00/R16/R18 or terminal source truth)!
        const verified = this.reverifyBatchProof(cause, facts, now);
        if (verified) {
          const res = this.deps.store.resolveCause(
            {
              causeId: cause.causeId,
              expectedRevision: cause.revision,
              outcomeKind: verified.outcomeKind,
              outcome: verified.outcome,
              clearKind: "superseded",
            },
            now,
          );
          if (res.resolved) {
            out.resolved += 1;
            pendingJournals.push(...res.journals);
          }
        } else {
          out.unverifiedProofs += 1;
          const parked = this.parkCauseWithScopeAsk(
            cause,
            "unverifiable_proof",
            item.next_action ?? "review unverified batch proof",
            now,
            pendingJournals,
          );
          if (parked) out.parked += 1;
        }
        continue;
      }

      // remediation / undetermined (or needs_human without concrete ask) -> parked with outcome + next_action
      const parked = this.parkCauseWithScopeAsk(
        cause,
        `${item.outcome}:${item.evidence.slice(0, 3).join("; ") || "batch"}`,
        item.next_action ?? `review ${item.outcome} finding`,
        now,
        pendingJournals,
      );
      if (parked) out.parked += 1;
    }

    // Any member omitted from parsed output gets counted failure / parked
    for (const cid of closed.memberCauseIds) {
      if (handledMembers.has(cid)) continue;
      const cause = this.deps.store.getCause(cid);
      if (!cause || cause.state === "resolved" || cause.state === "surfaced") continue;
      this.countBatchFailureForMember(cause, assignmentId, "omitted_from_batch_output", cfg, now, pendingJournals, out);
    }

    return out;
  }

  private countBatchFailureForMember(
    cause: TriageCauseRecord,
    assignmentId: string,
    reason: string,
    cfg: InboxTriageConfig,
    now: number,
    pendingJournals: TriageJournalRecord[],
    out: BatchIngestReport,
  ): void {
    // Check if launch_batch already counted an attempt for this assignmentId
    const existingEffects = this.deps.store.listEffectsForCause(cause.causeId);
    const alreadyCountedForBatch = existingEffects.some(
      (e) => e.primitive === "launch_batch" && e.targetId === `${assignmentId}:${cause.causeId}`,
    );

    let current = cause;
    if (current.state === "owned") {
      const lost = this.deps.store.markOwnerLost(
        {
          causeId: current.causeId,
          expectedRevision: current.revision,
          reason,
          nextAction: `retry after ${reason}`,
          nextCheckAt: now,
        },
        now,
      );
      if (lost.journal) pendingJournals.push(lost.journal);
      current = lost.cause ?? current;
    }

    if (!alreadyCountedForBatch && current.state === "acknowledged") {
      const { nextCheckAt, absoluteDeadlineAt } = this.computeBackoffTiming(current, cfg, now);
      const claim = this.deps.store.claimEffectIntent(
        {
          causeId: current.causeId,
          expectedRevision: current.revision,
          maxAutoAttempts: cfg.maxAutoAttempts,
          primitive: "batch_ingest",
          targetKind: "batch_member",
          targetId: `${assignmentId}:${current.causeId}:ingest`,
          targetEpoch: "",
          nextCheckAt,
          absoluteDeadlineAt,
          allowBypassDueCheck: true,
        },
        now,
      );
      if (claim.claimed && claim.effect && claim.cause) {
        if (claim.journal) pendingJournals.push(claim.journal);
        const rec = this.deps.store.recordEffectReceipt(claim.effect.effectKey, "failed", reason, now, {
          expectedRevision: claim.cause.revision,
          lastActionResult: reason,
        });
        if (rec.journal) pendingJournals.push(rec.journal);
        current = rec.cause ?? claim.cause;
      }
    }

    if (current.attempts >= cfg.maxAutoAttempts) {
      const parked = this.parkCauseWithScopeAsk(current, reason, current.nextAction, now, pendingJournals);
      if (parked) out.parked += 1;
    }
  }

  private reverifyBatchProof(
    cause: TriageCauseRecord,
    facts: TriageFacts,
    now: number,
  ): { outcomeKind: CauseOutcomeKind; outcome: string } | null {
    // 1. Source row already reached terminal truth
    const alreadyTerminal = this.checkSourceTruthResolved(cause, facts);
    if (alreadyTerminal) {
      return { outcomeKind: alreadyTerminal.outcomeKind, outcome: alreadyTerminal.outcome };
    }

    // 2. R00 predicate: ALL linked goals are achieved/cancelled
    if (cause.runId && allLinkedGoalsTerminal(cause.runId, facts)) {
      if (cause.originKind === "escalation") {
        this.deps.db.reconcileSettledGoalEscalations();
        const esc = this.deps.db.getEscalation(cause.originSourceId);
        if (esc && esc.status === "resolved") {
          return { outcomeKind: "proof", outcome: esc.resolution ?? "verified R00: all linked goals settled" };
        }
        return null;
      }
      if (cause.originKind === "open_message") {
        const exp = this.deps.db.expireMessage(cause.originSourceId, "verified R00: all linked goals settled", now);
        if (exp.message && exp.message.status !== "open") {
          return { outcomeKind: "proof", outcome: "verified R00: all linked goals settled" };
        }
        return null;
      }
      return { outcomeKind: "proof", outcome: "verified R00: all linked goals settled" };
    }

    // 3. R16 predicate: merge_bounced superseded by later merged same-lineage row
    if (cause.originKind === "merge_bounced") {
      const m = facts.merges.get(cause.originSourceId);
      const later = m ? findSupersededMergedSubmission(m, facts) : null;
      if (later) {
        return { outcomeKind: "proof", outcome: `submission superseded by ${later.id}` };
      }
      return null;
    }

    // 4. R18 predicate: open_message recipient/sender durably terminal or run settled
    if (cause.originKind === "open_message") {
      const msg = facts.messages.get(cause.originSourceId);
      if (!msg) return null;
      const toTask = facts.tasks.get(msg.toTaskId);
      const fromTask = facts.tasks.get(msg.fromTaskId);
      const run = facts.runs.get(msg.runId);
      let reason: string | null = null;
      if (toTask && TERMINAL_MESSAGE_TASK_STATUS_SET.has(toTask.status)) {
        reason = `recipient task ${toTask.id} is ${toTask.status}`;
      } else if (fromTask && TERMINAL_MESSAGE_TASK_STATUS_SET.has(fromTask.status)) {
        reason = `sender task ${fromTask.id} is ${fromTask.status}`;
      } else if (run && (run.status === "completed" || run.status === "failed" || run.status === "stopped")) {
        reason = `run ${run.id} is ${run.status}`;
      }
      if (reason) {
        const exp = this.deps.db.expireMessage(msg.id, reason, now);
        if (exp.message && exp.message.status !== "open") {
          return { outcomeKind: "proof", outcome: `verified R18: ${reason}` };
        }
      }
      return null;
    }

    return null;
  }
}

interface EffectCausePatchLite {
  state?: "acknowledged" | "owned" | "held" | "parked";
  ownerKind?: InboxTriageOwnerKind | null;
  ownerId?: string | null;
  ownerEpoch?: string | null;
}

