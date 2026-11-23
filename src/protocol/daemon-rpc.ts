export interface RpcRequest {
  kind: "req";
  id: string;
  op: DaemonOp;
  params?: Record<string, unknown>;
}

export interface RpcResponse {
  kind: "res";
  id: string;
  ok: boolean;
  result?: unknown;
  error?: string;
}

export interface DaemonEventFrame {
  kind: "event";
  seq: number;
  runId: string;
  taskId: string | null;
  type: string;
  payload: unknown;
  createdAt: number;
  /** Set only on frames delivered for a read-only `watch.start` subscription. */
  watchId?: string;
}

export type WireFrame = RpcRequest | RpcResponse | DaemonEventFrame;

export type DaemonOp =
  | "ping"
  | "status"
  | "daemon.shutdown"
  | "config.get"
  | "config.update"
  | "profiles.upsert"
  | "profiles.remove"
  | "profiles.probe"
  | "chat.open"
  | "chat.create"
  | "chat.send"
  | "chat.steer"
  | "chat.cancel"
  | "chat.update"
  | "chat.set_project"
  | "chat.history"
  | "chat.list"
  | "project.list"
  | "project.get"
  | "project.create"
  | "project.update"
  | "project.remove"
  | "project.browse"
  | "remote.list"
  | "remote.add"
  | "remote.remove"
  | "remote.connect"
  | "remote.disconnect"
  | "peer.hello"
  | "peer.proxy"
  | "peer.sync_outbox"
  | "session.send_message"
  | "session.deliver_message"
  | "session.list_messages"
  | "session.read"
  | "sessions.list"
  | "session.leave"
  | "kb.search"
  | "kb.get"
  | "goal.create"
  | "goal.amend"
  | "goal.commit"
  | "goal.list"
  | "goal.get"
  | "goal.revisions"
  | "goal.resume"
  | "goal.table_display"
  | "goal.review"
  | "goal.accept"
  | "goal.pause"
  | "goal.cancel"
  | "run.create"
  | "run.list"
  | "run.get"
  | "run.stop"
  | "runs.stop_all"
  | "run.resume"
  | "run.schedule_resume"
  | "replay.page"
  | "attach"
  | "detach"
  | "tree.get"
  | "runs.trees"
  | "work.snapshot"
  | "watch.start"
  | "watch.stop"
  | "task.get"
  | "task.steer"
  | "task.cancel"
  | "task.resume"
  | "escalation.list"
  | "escalation.get"
  | "escalation.ack"
  | "escalation.resolve"
  | "escalation.reconcile"
  | "inbox.triage.report"
  | "inbox.triage.list"
  | "inbox.triage.rearm"
  | "artifact.list"
  | "artifact.read"
  | "timer.list"
  | "timer.cancel"
  | "merge.list"
  | "messages.list"
  | "event.get"
  | "models.list"
  | "models.sync"
  | "quota.get"
  | "quota.sync"
  | "cost.get"
  | "worktrees.list"
  | "worktrees.get"
  | "worktrees.clean"
  | "pty.open"
  | "pty.input"
  | "pty.resize"
  | "pty.close"
  | "tool.call";

export type EntityKind =
  | "goal"
  | "run"
  | "task"
  | "escalation"
  | "review"
  | "revision"
  | "artifact"
  | "timer"
  | "worktree"
  | "merge"
  | "message"
  | "tool_call"
  | "transcript";

export interface EntityRef {
  kind: EntityKind;
  id: string;
  runId?: string;
  goalId?: string;
  label?: string;
}

export interface SnapshotWorktreeEntry {
  id: string | null;
  runId: string | null;
  taskId: string | null;
  status: string;
  branch: string;
  path: string;
  existsOnDisk: boolean;
  dirty: boolean;
  orphaned: boolean;
  archiveRef: string | null;
}

export type InboxTriageDisposition = "routine_auto" | "routine_owned" | "human_blocker" | "held" | "derived";
export type InboxTriageState = "acknowledged" | "owned" | "held" | "surfaced" | "parked" | "resolved";
export type InboxTriageOwnerKind = "task" | "batch" | "daemon" | "human";

export interface InboxTriageInfo {
  disposition: InboxTriageDisposition;
  state: InboxTriageState;
  rule: string;
  causeId: string;
  ownerKind?: InboxTriageOwnerKind | null;
  ownerId?: string | null;
  nextAction?: string;
  nextCheckAt?: number | null;
  attempts?: number;
  ackedAt?: number;
  holdReason?: string | null;
  askId?: string | null;
}

export interface InboxItem {
  id: string;
  severity: "P0" | "P1" | "P2" | "info";
  kind:
    | "goal_awaiting_human"
    | "escalation"
    | "task_failed"
    | "task_blocked"
    | "run_failed"
    | "quota_paused"
    | "merge_bounced"
    | "open_message";
  title: string;
  detail: string;
  ref: EntityRef;
  goalId?: string | null;
  runId?: string | null;
  taskId?: string | null;
  createdAt: number;
  triage?: InboxTriageInfo;
  ask?: string;
  blockedWork?: string;
  sourceRefs?: EntityRef[];
}

export interface WorkSnapshotInboxFields {
  inbox: InboxItem[];
  /** Automation-owned / held / parked items when inbox triage mode is "on"; absent in "off" and "report". */
  inboxAuto?: InboxItem[];
}

export interface EscalationResolveParams {
  escalationId: string;
  reason?: string;
  resolution?: string;
  chatSessionId?: string;
  /** Optional explicit operator permission for inbox triage (R07) to auto-resume the exact blocked task. */
  continue?: boolean;
}

export interface InboxTriageReportParams {
  repoRoot?: string;
  chatSessionId?: string;
  runId?: string;
}

export interface InboxTriagePlanRow {
  itemId: string;
  itemKind: InboxItem["kind"];
  sourceId: string;
  sourceEpoch: string;
  causeId: string;
  role: "primary" | "derived";
  runId: string | null;
  goalId: string | null;
  repoRoot: string | null;
  rule: string;
  disposition: InboxTriageDisposition;
  ownerKind: InboxTriageOwnerKind | null;
  ownerId: string | null;
  nextAction: string;
  holdReason?: string | null;
  ask?: string | null;
  blockedWork?: string | null;
  vetoedBy?: string | null;
}

export interface InboxTriageReportResult {
  mode: "off" | "report" | "on";
  authoritative: boolean;
  generatedAt: number;
  rows: InboxTriagePlanRow[];
  countsByRule: Record<string, number>;
}

export interface InboxTriageListParams {
  state?: InboxTriageState;
  runId?: string;
  causeId?: string;
  repoRoot?: string;
  limit?: number;
  journal?: boolean;
  journalLimit?: number;
}

export interface InboxTriageCauseSummary {
  causeId: string;
  originKind: InboxItem["kind"];
  originSourceId: string;
  runId: string | null;
  goalId: string | null;
  repoRoot: string | null;
  rule: string;
  disposition: Exclude<InboxTriageDisposition, "derived">;
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
  noProgressRenewals: number;
  attempts: number;
  firstDispatchAt: number | null;
  absoluteDeadlineAt: number | null;
  holdReason: string | null;
  askId: string | null;
  outcomeKind: "source_resolved" | "proof" | "answered" | "superseded" | null;
  outcome: string | null;
  ackedAt: number;
  resolvedAt: number | null;
  updatedAt: number;
}

export interface InboxTriageJournalEntrySummary {
  seq: number;
  at: number;
  causeId: string;
  itemKey: string | null;
  runId: string | null;
  goalId: string | null;
  type: string;
  payload: unknown;
}

export interface InboxTriageListResult {
  causes: InboxTriageCauseSummary[];
  journal?: InboxTriageJournalEntrySummary[];
}

export interface InboxTriageRearmParams {
  causeId: string;
  reason?: string;
}

export interface InboxTriageRearmResult {
  causeId: string;
  rearmed: boolean;
  revision?: number;
  reason?: string;
}

/** `cost.get` params: the chat session, or the run whose owning session (or the run itself) is wanted. */
export interface CostGetParams {
  chatSessionId?: string;
  runId?: string;
}

export interface CostAmount {
  currency: string;
  amount: number;
}

export interface CostModelAmount extends CostAmount {
  /** `null` when the source did not say which model incurred the charge. */
  model: string | null;
}

export interface CostRoute {
  profileId: string;
  billing: "api" | "local";
  invocations: number;
  running: number;
  /** reported: every invocation has model-attributed cost; partial: some cost reported; unavailable: nothing reported; unsupported: the route never reports charges. */
  status: "reported" | "partial" | "unavailable" | "unsupported";
  note: string;
}

/** Known spend of one session scope; amounts are never summed across currencies. */
export interface CostSnapshot {
  scope: { kind: "chat" | "run"; id: string; label: string };
  /** The scope has at least one non-subscription invocation, even without any cost data. */
  available: boolean;
  totals: CostAmount[];
  byModel: CostModelAmount[];
  coverage: {
    /** True only when every non-subscription invocation has finished with model-attributed reported cost. */
    complete: boolean;
    routes: CostRoute[];
    unattributed: CostAmount[];
  };
}
