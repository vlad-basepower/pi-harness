import type { InboxTriageConfig } from "../config.js";
import type { GoalRecord } from "../goals/engine.js";
import type {
  EntityRef,
  InboxItem,
  InboxTriageDisposition,
  InboxTriageOwnerKind,
  InboxTriagePlanRow,
} from "../protocol/daemon-rpc.js";
import type {
  ControlPlaneDb,
  EscalationRecord,
  MergeQueueRecord,
  MessageRecord,
  RunRecord,
  TaskRecord,
} from "./db.js";
import {
  type InboxTriageStore,
  type TriageContinuationRecord,
  makeCauseId,
} from "./inbox-triage-store.js";
import {
  type RunQuotaPauseStatus,
  deriveRunCause,
  deriveRunStatus,
  isDirectorTaskContainer,
} from "./orchestrator.js";

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
  /** Recorded R07 continuation permissions keyed by escalationId. */
  continuations: ReadonlyMap<string, TriageContinuationRecord>;
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
  const taskResumedRows = sqlite
    .prepare(`SELECT seq, task_id, payload_json FROM events WHERE type = 'task.resumed' ORDER BY seq ASC`)
    .all() as Array<{ seq: number; task_id: string | null; payload_json: string }>;
  for (const r of taskResumedRows) {
    const tid = r.task_id ? String(r.task_id) : null;
    if (!tid) continue;
    const task = tasks.get(tid);
    const p = parseJsonObj(r.payload_json);
    const gen = typeof p?.generation === "number" ? p.generation : null;
    if (task && gen === task.generation) {
      taskCurrentGenStartedSeq.set(tid, Number(r.seq));
    }
  }

  // 8. Triage-side read-only metadata (ONLY if schema already exists — zero DDL!)
  const continuations = new Map<string, TriageContinuationRecord>();
  const missingQuotaSinceByCauseId = new Map<string, number>();
  const triageOwnedTaskIds = new Set<string>();

  if (store.hasSchema()) {
    const contRows = sqlite.prepare("SELECT * FROM inbox_triage_continuation").all() as Record<string, unknown>[];
    for (const r of contRows) {
      continuations.set(String(r.escalation_id), {
        escalationId: String(r.escalation_id),
        recordedAt: Number(r.recorded_at),
        consumedAt: r.consumed_at == null ? null : Number(r.consumed_at),
        consumedTaskId: r.consumed_task_id == null ? null : String(r.consumed_task_id),
        consumedGeneration: r.consumed_generation == null ? null : Number(r.consumed_generation),
      });
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
    missingQuotaSinceByCauseId,
    triageOwnedTaskIds,
    latestEventSeqByTask,
    latestEventSeqByRun,
    steerUndeliveredAtByTask,
    escalationCreatedSeq,
    taskCurrentGenStartedSeq,
  };
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

        const hasRecordedContinue = proven.resolved.every((esc) => {
          const cont = facts.continuations.get(esc.id);
          if (!cont || cont.consumedAt !== null) return false;
          if (esc.resolution && HOLD_OR_CANCEL_RESOLUTION_RE.test(esc.resolution)) return false;
          return true;
        });

        if (hasRecordedContinue) {
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
