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
  | "chat.history"
  | "chat.list"
  | "sessions.list"
  | "session.leave"
  | "kb.search"
  | "kb.get"
  | "goal.create"
  | "goal.amend"
  | "goal.commit"
  | "goal.list"
  | "goal.get"
  | "goal.table_display"
  | "goal.review"
  | "goal.pause"
  | "goal.cancel"
  | "run.create"
  | "run.list"
  | "run.get"
  | "run.stop"
  | "run.resume"
  | "run.schedule_resume"
  | "replay.page"
  | "attach"
  | "detach"
  | "tree.get"
  | "runs.trees"
  | "watch.start"
  | "watch.stop"
  | "task.steer"
  | "task.cancel"
  | "task.resume"
  | "escalation.ack"
  | "models.list"
  | "models.sync"
  | "quota.get"
  | "quota.sync"
  | "cost.get"
  | "worktrees.list"
  | "worktrees.clean"
  | "pty.open"
  | "pty.input"
  | "pty.resize"
  | "pty.close"
  | "tool.call";

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
