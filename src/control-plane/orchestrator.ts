import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join, relative, resolve } from "node:path";
import type { HarnessConfig, RunnerProfileConfig } from "../config.js";
import type { ModelCatalogCompiler } from "../catalog/compiler.js";
import type { QuotaClient } from "../quota/client.js";
import { AcpRunner } from "../runners/acp-runner.js";
import { PiRunner } from "../runners/pi-runner.js";
import { toolPolicyForTask } from "../runners/types.js";
import { recordCostInvocation } from "../cost/ledger.js";
import { resolveLaunchRoute } from "./launch-route.js";
import type { RunAgentRequest, RunAgentResult, RunnerExecutionHandle } from "../runners/types.js";
import { MAX_STEER_CONTINUATIONS } from "../runners/types.js";
import { BisectingMergeQueue } from "../worktree/merge-queue.js";
import { invalidateGateEnvCache, type GateRuntimeConfig } from "../tools/gate-env.js";
import { nativeToolsPathDirs, resolveExternalMcpServers } from "../tools/native-tools.js";
import { canonicalPath, runGit, WorktreeConcurrencyLimitError, WorktreeManager } from "../worktree/manager.js";
import type { QueuePositionInput } from "./admission-queue.js";
import { WriteScopeConflictError, type AgentTier, type ControlPlaneDb, type RunRecord, type TaskKind, type TaskRecord, type TaskStatus, type WorktreeRecord } from "./db.js";
import { buildHydrationTurn, buildImmutableSystemPrompt, buildMissingReturnWrapUpPrompt, type AnchoredHandoff } from "./prompts.js";
import { spillSummaryOverflow, writeUniqueTaskDossier } from "./dossiers.js";
import { ControlPlaneToolExecutor, compactTaskHandoff, isReadOnlyKind, validateTaskHandoff } from "./tools.js";
import { SUMMARY_LIMIT } from "./handoffs.js";
import { directorTaskRunId, directorTaskRunOwner, isDirectorTaskContainer } from "./ids.js";
import { canonicalProjectPath } from "./projects.js";
import { ensureGoalEvidenceDir } from "../goals/engine.js";

interface ActiveTaskExecution {
  taskId: string;
  abort: AbortController;
  generationSignal?: AbortSignal;
  handle?: RunnerExecutionHandle;
  promise: Promise<void>;
  /** Task generation the current runner of this execution was started for. */
  runnerGeneration?: number;
  /** True once the current generation's initial prompt was built from the task row (later row edits miss it). */
  promptBuilt?: boolean;
  /** Steers accepted after the initial prompt was built but before the runner exposed its handle, per generation. */
  pendingSteers?: Array<{ generation: number; message: string; onOutcome?: (accepted: boolean, reason: string) => void }>;
  /** Serializes every steer to the current handle so live steers never overtake queued ones still draining. */
  steerChain?: Promise<void>;
  /** "settling" from the moment runner.run() returns: steering can no longer reach that session. */
  phase?: "starting" | "running" | "settling";
}

/** Steering messages a task may have queued at once. */
const MAX_PENDING_STEERS = 20;

/** Structured outcome of a steering request: how it will be delivered, or why it was refused. */
export interface SteerTaskResult {
  steered: boolean;
  /** native: taken by the live session; queued_wait: delivered by the task's current wait_children; queued_turn: opens its next turn. */
  mode: "native" | "queued_wait" | "queued_turn" | "refused";
  taskId: string;
  runId: string;
  status: TaskStatus;
  code?: "not_found" | "not_live" | "run_stopped" | "no_session" | "ending" | "steer_cap" | "queue_full" | "aborting";
  /** Always present when steered is false. */
  reason?: string;
  nextAction?: string;
}

/** Stop/cancel epochs captured when a resume was requested. */
interface StopSnapshot {
  run: number;
  repo: number;
  global: number;
  /** Per-task cancel epochs (task id -> epoch). */
  tasks: Map<string, number>;
  /**
   * Per-task explicit-cancel epochs taken at request entry, before waiting on any pending stop. Only explicit
   * cancelTask calls bump them (never a stopRun sweep), so a cancel issued during that wait still fences the resume.
   */
  explicit?: Map<string, number>;
}

interface TaskQuotaPauseMeta {
  wireModel: string;
  profileId: string;
  model: string | null;
  reason: string;
  resetAt: string | null;
  pausedAt: number;
}

export interface RunQuotaPauseStatus {
  runId: string;
  pausedTaskIds: string[];
  mode: "auto_timer" | "user_timer" | "manual";
  source: "quota_endpoint" | "passive_reset" | "user_timer" | "none";
  resumeAt: string | null;
  reason: string;
  pausedAt: number;
}

interface InternalRunQuotaState extends RunQuotaPauseStatus {
  timer: ReturnType<typeof setTimeout> | null;
  earlyWakePoller: ReturnType<typeof setInterval> | null;
}

export type CustomRunnerFactory = (profile: RunnerProfileConfig) => {
  run: (req: RunAgentRequest) => Promise<RunAgentResult>;
};

export const LIVE_TASK_STATUSES = new Set<TaskRecord["status"]>(["running", "draining", "waiting_children", "merging"]);
const IN_FLIGHT_TASK_STATUSES = new Set<TaskRecord["status"]>([
  "pending",
  "ready",
  "running",
  "draining",
  "waiting_children",
  "merging",
]);

export { isDirectorTaskContainer };

/** startRun was refused because /stop (all runs or this repo) was issued while the launch was resolving. */
export class RunStartStoppedError extends Error {
  constructor(readonly repoRoot: string) {
    super(`No run was started in ${repoRoot}: a stop was issued while the run was starting. Nothing is running; start it again to continue.`);
    this.name = "RunStartStoppedError";
  }
}

/** The Director's independent Done-Reviewer: review activity, not execution, so it never drives run status. */
function isDoneReviewTask(t: TaskRecord): boolean {
  return t.tier === 3 && t.kind === "verify" && t.title.startsWith("Done-Review:");
}

/**
 * Computes the authoritative dynamic status of a Run from its live in-memory execution state
 * and current SQLite tasks so a run NEVER displays `completed` while work is still running,
 * and NEVER stays stuck in `active` once all of its tasks have settled.
 */
export function deriveRunStatus(
  run: RunRecord,
  tasks: TaskRecord[],
  liveInMemory?: boolean,
): RunRecord["status"] {
  if (isDirectorTaskContainer(run.id)) return deriveContainerStatus(run, tasks, liveInMemory);
  // An explicit stop is sticky: status reads never undo it; only an explicit resume or launch writes "active".
  if (run.status === "stopped") return "stopped";
  if (liveInMemory === true) return "active";
  // A running Done-Reviewer must never flip a settled run back to active (a snapshot would then abort the review).
  const execTasks = tasks.filter((t) => !isDoneReviewTask(t));
  if (execTasks.some((t) => LIVE_TASK_STATUSES.has(t.status))) return "active";
  if (execTasks.length === 0) return run.status;
  // An explicit completion (e.g. accepted failed-run verification) is authoritative until an explicit resume,
  // even when orphaned queued rows remain beside the settled coordinator.
  if (run.status === "completed") return "completed";

  if (execTasks.some((t) => t.status === "paused_quota")) return "paused_quota";

  const hasQueued = execTasks.some((t) => IN_FLIGHT_TASK_STATUSES.has(t.status));
  if (hasQueued) {
    if (liveInMemory === false) {
      return execTasks.some((t) => t.status === "failed") ? "failed" : "stopped";
    }
    return run.status === "active" ? "active" : run.status;
  }

  const realOrch = execTasks.find((t) => t.tier === 1 && t.profileId !== "director");
  if (realOrch) {
    if (realOrch.status === "failed") return "failed";
    if (realOrch.status === "cancelled") return "stopped";
    // A blocked coordinator never settled its work: report failed (there is no blocked run status), never completed.
    if (realOrch.status === "blocked" || realOrch.status === "returned_for_rescope") return "failed";
    if (realOrch.status === "completed") {
      // Do not falsely declare a run completed if non-orchestrator workers are blocked with unresolved work
      const realChildren = execTasks.filter((t) => t.id !== realOrch.id && t.profileId !== "director");
      if (realChildren.some((t) => t.status === "blocked")) return "failed";
      return "completed";
    }
  }

  const realTasks = execTasks.filter((t) => t.profileId !== "director");
  if (realTasks.length === 0) return run.status === "active" ? "completed" : run.status;
  if (realTasks.some((t) => t.status === "failed")) return "failed";
  if (realTasks.every((t) => t.status === "cancelled")) return "stopped";
  return "completed";
}

/**
 * Machine cause for a derived run status: a run that derives "failed" while no real task failed is blocked on an
 * escalation (blocked or returned-for-rescope tasks), which is recoverable rather than a failure.
 */
export function deriveRunCause(
  tasks: TaskRecord[],
  derived: RunRecord["status"],
): { cause: "blocked_escalation"; reason: string } | null {
  if (derived !== "failed") return null;
  const real = tasks.filter((t) => t.profileId !== "director");
  if (real.some((t) => t.status === "failed")) return null;
  const blocked = real.filter((t) => t.status === "blocked" || t.status === "returned_for_rescope");
  if (blocked.length === 0) return null;
  const summaries = blocked.map((t) => `${t.id} ${t.status}: ${(t.summary ?? t.title).replace(/\s+/g, " ").slice(0, 160)}`);
  return { cause: "blocked_escalation", reason: `Blocked: ${summaries.join("; ")}`.slice(0, 600) };
}

/**
 * Container runs (`director-tasks-<chatId>`) are buckets for a chat's delegated lookups. Their status
 * follows the most recently updated task, and a cancelled child (or an older /stop) never turns the
 * container into "stopped": that would block every later delegation from the same chat.
 */
function deriveContainerStatus(run: RunRecord, tasks: TaskRecord[], liveInMemory?: boolean): RunRecord["status"] {
  if (liveInMemory === true) return "active";
  if (tasks.some((t) => LIVE_TASK_STATUSES.has(t.status))) return "active";
  const realTasks = tasks.filter((t) => t.profileId !== "director");
  if (realTasks.some((t) => t.status === "paused_quota")) return "paused_quota";
  if (liveInMemory !== false && realTasks.some((t) => IN_FLIGHT_TASK_STATUSES.has(t.status))) return "active";
  if (realTasks.length === 0) return run.status === "active" || run.status === "stopped" ? "completed" : run.status;
  const latestTask = [...realTasks].sort((a, b) => b.updatedAt - a.updatedAt)[0]!;
  return latestTask.status === "failed" ? "failed" : "completed";
}

export interface ResumeTaskOptions {
  note?: string;
  profileId?: string;
  model?: string;
  effort?: string;
  allowCompleted?: boolean;
  position?: QueuePositionInput;
  forceStart?: boolean;
  /** Re-checked before any state is mutated and again after the last await; resume aborts if it returns false. */
  validateBeforeActivate?: () => boolean;
}

export interface ResumeTaskResult {
  task: TaskRecord;
  outcome: "resumed" | "already_active" | "already_queued" | "waiting" | "already_completed";
  message: string;
  queuePosition?: number;
  noteDelivered?: boolean;
  /** Steering outcome of this caller's note on an already active task. */
  noteDelivery?: SteerTaskResult;
}

export interface ResumeRunResult {
  resumed: boolean;
  resumedTaskIds: string[];
  run: RunRecord;
  outcome: "resumed" | "already_active" | "already_completed" | "nothing_to_resume";
  runStatus: RunRecord["status"];
  message: string;
  cause?: string;
  activeTaskIds?: string[];
  failed?: Array<{ taskId: string; error: string }>;
  /** Set when this caller passed a note: whether it reached the run's orchestrator. */
  noteDelivered?: boolean;
  /** Steering outcome of this caller's note when it was steered into the live orchestrator. */
  noteDelivery?: SteerTaskResult;
  /** True when the run was already running (outcome "already_active"). */
  alreadyRunning?: boolean;
  /** Machine-readable reason for a resume that restarted nothing. */
  reason?: "already_running" | "no_resumable_tasks" | "blocked_on_escalation" | "already_completed";
}

export class HarnessOrchestrator {
  private active = new Map<string, ActiveTaskExecution>();
  private allocatingWorktrees = new Set<string>();
  private worktreeManagers = new Map<string, WorktreeManager>();
  private mergeQueues = new Map<string, BisectingMergeQueue>();
  private pendingRollovers = new Map<
    string,
    { handoff: AnchoredHandoff; target?: { profileId?: string; model?: string; effort?: string } }
  >();
  private taskQuotaHints = new Map<string, TaskQuotaPauseMeta>();
  private taskResumeNotes = new Map<string, { kind?: "quota" | "resume"; reason?: string; userMessage?: string | null }>();
  private runQuotaStates = new Map<string, InternalRunQuotaState>();
  private runConsecutiveQuotaPauses = new Map<string, number>();
  private taskAutoRolloverProgress = new Map<string, { fp: string; streak: number }>();
  private quiescenceCheckPending = new Set<string>();
  /** Runs with a stopRun in progress (a resume issued after a stop waits for it). */
  private stoppingRuns = new Map<string, Promise<void>>();
  /**
   * Bumped by stopRun (key: run id) and cancelTask (key: task id, also for an already-settled task) so an
   * older in-flight resume, rollover or launch never undoes a stop or cancel.
   */
  private stopEpoch = new Map<string, number>();
  /** Bumped (key: task id) only by explicit cancelTask calls, never by a stopRun sweep's own cancellations. */
  private explicitCancelEpoch = new Map<string, number>();
  /** Bumped by a repo-scoped runs.stop_all (key: canonical repo root). */
  private repoStopEpoch = new Map<string, number>();
  private globalStopEpoch = 0;
  /** In-flight single-flight resumes, tagged with the stop/cancel epochs they were requested under. */
  private resumingTasks = new Map<string, { key: string; promise: Promise<ResumeTaskResult>; note?: string }>();
  private resumingRuns = new Map<string, { key: string; promise: Promise<ResumeRunResult>; note?: string }>();
  /** Per-task worktree allocation/teardown chain, so cancel, resume and allocation never interleave. */
  private worktreeOps = new Map<string, Promise<void>>();
  /** Worktree teardowns started by cancelTask, removed by identity when they settle. */
  private taskTeardowns = new Map<string, Promise<void>>();
  /** Deferred activations waiting for a write lease, keyed by canonical repo root. */
  private leaseWaitQueue = new Map<string, Set<() => void>>();
  private unsubLeaseEvents: () => void;
  /** Accepted steering not yet delivered (FIFO per task, at most MAX_PENDING_STEERS). */
  private pendingSteers = new Map<string, string[]>();
  /** In-flight waitForChildrenEventDriven calls per waiting task. */
  private waitingChildrenTasks = new Map<string, number>();
  /** Steering continuation turns reserved by the current execution of a task. */
  private steerContinuations = new Map<string, number>();
  /** Tasks whose queued steering batch holds one continuation reservation (released if a wait_children takes the batch). */
  private steerReservedBatches = new Set<string>();
  /** The note-free base message of resume results, so a joined caller never inherits the first caller's note prose. */
  private resumeBaseMessages = new WeakMap<object, string>();
  readonly toolExecutor: ControlPlaneToolExecutor;

  constructor(
    readonly db: ControlPlaneDb,
    readonly config: HarnessConfig,
    public catalog: ModelCatalogCompiler,
    public quota: QuotaClient,
    private readonly customRunnerFactory?: CustomRunnerFactory,
  ) {
    this.toolExecutor = new ControlPlaneToolExecutor(db, () => this.catalog, {
      spawnAndStartTask: (input) => this.spawnAndStartTask(input),
      waitForChildren: (parentId, ids, mode, timeoutMs, signal) =>
        this.waitForChildrenEventDriven(parentId, ids, mode, timeoutMs, signal),
      steerTask: (taskId, message) => this.steerTask(taskId, message),
      steerTaskDetailed: (taskId, message) => this.steerTaskDetailed(taskId, message),
      drainPendingSteers: (taskId) => this.drainPendingSteers(taskId, { viaWait: true }),
      cancelTask: (taskId, reason) => this.cancelTask(taskId, reason),
      stopTaskForBlockingIssue: (taskId, reason) => {
        const exec = this.active.get(taskId);
        if (exec) {
          exec.abort.abort(new Error(reason));
          void exec.handle?.cancel("blocking_issue").catch(() => {});
        }
      },
      scheduleRollover: async (taskId, handoff, target) => {
        const task = this.db.getTask(taskId)!;
        const run = this.db.getRun(task.runId)!;
        const route = await this.taskRoute(task, run.repoRoot, target);
        if (this.db.getTask(taskId)?.status !== "running") throw new Error("Task is no longer running.");
        this.db.updateTask(taskId, { status: "draining", handoffJson: JSON.stringify(handoff) });
        this.pendingRollovers.set(taskId, { handoff, target: { ...target, profileId: route.profile.id, model: route.model } });
        const exec = this.active.get(taskId);
        if (exec?.handle) {
          void exec.handle.cancel("clean_rollover").catch(() => {});
        }
      },
      getWorktreeManager: (repoRoot) => this.getWorktreeManager(repoRoot),
      getMergeQueue: (repoRoot) => this.getMergeQueue(repoRoot),
      getTaskSignal: (taskId) => this.active.get(taskId)?.generationSignal,
      describeRunCause: (runId) => this.describeRunCause(runId),
      getTaskCancelSignal: (taskId) => this.active.get(taskId)?.abort.signal,
      resumeTask: (taskId, opts) => this.resumeTask(taskId, opts),
      resumeTaskDetailed: (taskId, opts) => this.resumeTaskDetailed(taskId, opts),
      forceStartWorker: (taskId, opts) => this.forceStartWorker(taskId, opts),
    }, config);
    // Wakes deferred activations waiting on a write lease whenever a holder can no longer write.
    this.unsubLeaseEvents = db.onEvent((ev) => {
      if (this.leaseWaitQueue.size === 0) return;
      const p = ev.payload as { to?: string; status?: string } | null;
      const released =
        (ev.type === "task.status" && ["completed", "failed", "cancelled", "returned_for_rescope", "blocked"].includes(p?.to ?? "")) ||
        (ev.type === "run.status" && ["completed", "failed", "stopped"].includes(p?.status ?? ""));
      if (!released) return;
      const run = this.db.getRun(ev.runId);
      if (run) this.onWriteLeaseReleased(run.repoRoot);
    });
  }

  /** Applies live configuration updates across all cached WorktreeManagers & MergeQueues and wakes queued workers immediately. */
  updateConfig(next: HarnessConfig, catalog = this.catalog, quota = this.quota): void {
    // The config object is shared with the tool executor, so clear optional keys the new config omits.
    for (const key of Object.keys(this.config)) {
      if (!(key in next)) delete (this.config as unknown as Record<string, unknown>)[key];
    }
    Object.assign(this.config, next);
    invalidateGateEnvCache();
    this.catalog = catalog;
    this.quota = quota;
    const gate = this.gateRuntimeConfig();
    for (const mgr of this.worktreeManagers.values()) {
      mgr.updateConfig(next.worktree, gate);
    }
    for (const mq of this.mergeQueues.values()) {
      mq.updateConfig(next.worktree, gate);
    }
    this.db.admissionQueue.notify("worker");
  }

  activeTaskCount(): number {
    return this.active.size;
  }

  /** Number of runs currently holding an armed auto-resume or user-scheduled quota timer. */
  scheduledQuotaResumeCount(): number {
    let n = 0;
    for (const st of this.runQuotaStates.values()) {
      if (st.timer || st.earlyWakePoller) n++;
    }
    return n;
  }

  getRunQuotaStatus(runId: string): RunQuotaPauseStatus | null {
    const st = this.runQuotaStates.get(runId);
    if (!st) return null;
    return {
      runId: st.runId,
      pausedTaskIds: [...st.pausedTaskIds],
      mode: st.mode,
      source: st.source,
      resumeAt: st.resumeAt,
      reason: st.reason,
      pausedAt: st.pausedAt,
    };
  }

  listQuotaPausedRuns(): RunQuotaPauseStatus[] {
    return [...this.runQuotaStates.values()].map((st) => ({
      runId: st.runId,
      pausedTaskIds: [...st.pausedTaskIds],
      mode: st.mode,
      source: st.source,
      resumeAt: st.resumeAt,
      reason: st.reason,
      pausedAt: st.pausedAt,
    }));
  }

  dispose(): void {
    for (const runId of [...this.runQuotaStates.keys()]) {
      this.clearRunQuotaTimers(runId);
    }
    this.runQuotaStates.clear();
    for (const [taskId, exec] of [...this.active.entries()]) {
      exec.abort.abort(new Error("Daemon shutting down"));
      void exec.handle?.cancel("daemon_shutdown").catch(() => {});
      this.active.delete(taskId);
    }
    this.taskQuotaHints.clear();
    this.taskResumeNotes.clear();
    this.pendingRollovers.clear();
    this.unsubLeaseEvents();
    for (const key of [...this.leaseWaitQueue.keys()]) this.onWriteLeaseReleased(key);
  }

  private clearRunQuotaTimers(runId: string): void {
    const st = this.runQuotaStates.get(runId);
    if (!st) return;
    if (st.timer) {
      clearTimeout(st.timer);
      st.timer = null;
    }
    if (st.earlyWakePoller) {
      clearInterval(st.earlyWakePoller);
      st.earlyWakePoller = null;
    }
  }

  /** Bumps the global stop epoch (runs.stop_all) so in-flight resumes and goal launches started earlier lose to the stop. */
  bumpGlobalStopEpoch(): number {
    this.globalStopEpoch += 1;
    return this.globalStopEpoch;
  }

  /** Bumps the stop epoch of one repo (repo-filtered runs.stop_all); other repos' resumes and launches are unaffected. */
  bumpRepoStopEpoch(repoRoot: string): number {
    const key = canonicalProjectPath(repoRoot);
    const next = (this.repoStopEpoch.get(key) ?? 0) + 1;
    this.repoStopEpoch.set(key, next);
    return next;
  }

  getRepoStopEpoch(repoRoot: string): number {
    return this.repoStopEpoch.get(canonicalProjectPath(repoRoot)) ?? 0;
  }

  /** Global + repo stop epochs for a launch in `repoRoot`; pass to startRun so a stop issued meanwhile wins. */
  launchStopFence(repoRoot: string): { global: number; repo: number } {
    return { global: this.globalStopEpoch, repo: this.getRepoStopEpoch(repoRoot) };
  }

  launchFenceBroken(repoRoot: string, fence: { global: number; repo: number }): boolean {
    return this.globalStopEpoch !== fence.global || this.getRepoStopEpoch(repoRoot) !== fence.repo;
  }

  private bumpStopEpoch(key: string): void {
    this.stopEpoch.set(key, (this.stopEpoch.get(key) ?? 0) + 1);
  }

  /** Runs one worktree allocation/teardown for a task after every earlier one for that task has finished. */
  private runWorktreeOp<T>(taskId: string, fn: () => Promise<T>): Promise<T> {
    const prev = this.worktreeOps.get(taskId) ?? Promise.resolve();
    const p = prev.then(fn);
    const tail = p.then(
      () => {},
      () => {},
    );
    this.worktreeOps.set(taskId, tail);
    void tail.then(() => {
      if (this.worktreeOps.get(taskId) === tail) this.worktreeOps.delete(taskId);
    });
    return p;
  }

  /**
   * Waits until a cancelled runner (or allocation) for this task has fully exited and its teardown has run. False
   * when that has not happened by `deadline` (epoch ms).
   */
  private async awaitTaskWindDown(taskId: string, deadline = Infinity): Promise<boolean> {
    for (;;) {
      const exec = this.active.get(taskId);
      let pending: Promise<unknown> | undefined;
      if (exec?.abort.signal.aborted) {
        pending = exec.promise.catch(() => {});
      } else {
        const op = this.worktreeOps.get(taskId);
        if (op && (!exec || this.allocatingWorktrees.has(taskId))) pending = op;
      }
      if (!pending) return true;
      if (!(await this.settlesBy(pending, deadline))) return false;
    }
  }

  /** True when `p` settles before `deadline` (epoch ms); Infinity waits without a timer. */
  private async settlesBy(p: Promise<unknown>, deadline: number): Promise<boolean> {
    const settled = p.then(() => true, () => true);
    if (deadline === Infinity) return settled;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const ok = await Promise.race([
      settled,
      new Promise<boolean>((resolveTimeout) => {
        timer = setTimeout(() => resolveTimeout(false), Math.max(0, deadline - Date.now()));
        timer.unref?.();
      }),
    ]);
    clearTimeout(timer);
    return ok;
  }

  /** Bound on how long a resume waits for cancelled runners and their teardowns before evicting or reporting them. */
  private cancelUnwindTimeoutMs = 15_000;

  /**
   * Bounded wait for cancelled runners and cancel-started worktree teardowns of these tasks. A cancelled entry still
   * registered at `deadline` (epoch ms) is evicted (its callbacks and tool calls are fenced to its own execution and
   * generation, and its finally is identity-guarded) and reported as task.cancel_unwind_timeout.
   */
  private async awaitCancelUnwind(taskIds: string[], deadline = Date.now() + this.cancelUnwindTimeoutMs): Promise<void> {
    await Promise.all(
      taskIds.map(async (id) => {
        const entry = this.active.get(id);
        const aborted = entry?.abort.signal.aborted ? entry : undefined;
        const teardown = this.taskTeardowns.get(id);
        if (!aborted && !teardown) return;
        const timedOut = !(await this.settlesBy(Promise.allSettled([aborted?.promise, teardown].filter(Boolean)), deadline));
        if (timedOut && aborted && this.active.get(id) === aborted) {
          this.active.delete(id);
          this.pendingRollovers.delete(id);
          // The evicted entry's finally is identity-guarded and will not release task-keyed steering state.
          this.disposeSteeringState(id);
          const task = this.db.getTask(id);
          if (task) this.db.emitEvent(task.runId, id, "task.cancel_unwind_timeout", { taskId: id });
        }
      }),
    );
  }

  /** Drops a task's steering reservations and queue, reporting every steer that was never delivered. */
  private disposeSteeringState(taskId: string): void {
    this.steerContinuations.delete(taskId);
    this.steerReservedBatches.delete(taskId);
    this.waitingChildrenTasks.delete(taskId);
    const undelivered = this.pendingSteers.get(taskId);
    this.pendingSteers.delete(taskId);
    const owner = this.db.getTask(taskId);
    if (undelivered?.length && owner) {
      this.db.emitEvent(owner.runId, taskId, "agent.steer_undelivered", { messages: undelivered });
    }
  }

  /** Resolves when a write lease in `repoRoot` is released, on abort, or after a 15 s safety interval. */
  private waitForWriteLease(repoRoot: string, signal: AbortSignal): Promise<void> {
    if (signal.aborted) return Promise.resolve();
    const key = canonicalPath(repoRoot);
    return new Promise<void>((resolveWait) => {
      let waiters = this.leaseWaitQueue.get(key);
      if (!waiters) {
        waiters = new Set();
        this.leaseWaitQueue.set(key, waiters);
      }
      const finish = () => {
        clearTimeout(timer);
        signal.removeEventListener("abort", finish);
        const set = this.leaseWaitQueue.get(key);
        set?.delete(finish);
        if (set && set.size === 0) this.leaseWaitQueue.delete(key);
        resolveWait();
      };
      waiters.add(finish);
      const timer = setTimeout(finish, 15_000);
      timer.unref?.();
      signal.addEventListener("abort", finish, { once: true });
    });
  }

  /** Wakes every activation waiting for a write lease in `repoRoot`; each retries its all-or-nothing reacquire. */
  private onWriteLeaseReleased(repoRoot: string): void {
    const waiters = this.leaseWaitQueue.get(canonicalPath(repoRoot));
    if (!waiters) return;
    for (const wake of [...waiters]) wake();
  }

  /**
   * True while `exec` is still the task's registered, uncancelled execution and the task row is still at `generation`.
   * A runner evicted after a cancel-unwind timeout, or superseded by a rollover or resume, fails this check.
   */
  private isCurrentExecution(taskId: string, exec: ActiveTaskExecution, generation: number): boolean {
    return this.active.get(taskId) === exec && !exec.abort.signal.aborted && this.db.getTask(taskId)?.generation === generation;
  }

  /**
   * Rejects a bridged tool call made for a task generation that is no longer the live one (an evicted, cancelled,
   * rolled-over or resumed runner). Nothing is executed for a rejected call.
   */
  assertCurrentToolGeneration(taskId: string, expectedGeneration: number): void {
    const exec = this.active.get(taskId);
    if (!exec || exec.runnerGeneration !== expectedGeneration || !this.isCurrentExecution(taskId, exec, expectedGeneration)) {
      const current = this.db.getTask(taskId)?.generation;
      throw new Error(
        `Stale tool call for task ${taskId} generation ${expectedGeneration}` +
          (current !== undefined ? ` (current generation ${current})` : "") +
          ": that runner is no longer the task's live execution. Nothing was executed.",
      );
    }
  }

  /** True while a runner for this task is live and not cancelled. */
  private isLiveExecution(taskId: string): boolean {
    const exec = this.active.get(taskId);
    return Boolean(exec && !exec.abort.signal.aborted);
  }

  /**
   * Read-only per-task liveness accessor: true iff this task currently has a non-aborted, non-settling
   * execution handle matching its current live status and generation (consistent with `steerTaskDetailed`).
   */
  isTaskExecuting(taskId: string): boolean {
    if (!this.isLiveExecution(taskId)) return false;
    const exec = this.active.get(taskId);
    if (!exec || exec.phase === "settling") return false;
    const task = this.db.getTask(taskId);
    if (!task || !LIVE_TASK_STATUSES.has(task.status)) return false;
    if (exec.runnerGeneration !== undefined && exec.runnerGeneration !== task.generation) return false;
    return true;
  }

  /**
   * Human-readable recorded cause for a run's current state: live quota pause, persisted run.status /
   * quota reason, or an unresolved (open or reopened) P0 escalation. Null when no cause was recorded.
   */
  describeRunCause(runId: string): string | null {
    const quota = this.getRunQuotaStatus(runId);
    if (quota) return `paused on provider quota: ${quota.reason}`;
    const reason = this.db.getLastRunStatusReason(runId);
    if (reason) return reason;
    const p0 = this.db.listEscalations(runId, "unresolved").find((e) => e.severity === "P0");
    if (p0) return `${p0.status} P0 escalation ${p0.id}: ${p0.title}`;
    return null;
  }

  private causeOrHint(runId: string, cause: string | null | undefined): string {
    return cause?.trim() || `no cause was recorded; inspect with \`pi-harness tree ${runId}\``;
  }

  /** Runs that have a task live (or allocating its worktree) in memory, whatever their stored status. */
  liveRunIds(): string[] {
    const ids = new Set<string>();
    for (const taskId of [...this.active.keys(), ...this.allocatingWorktrees]) {
      const t = this.db.getTask(taskId);
      if (t) ids.add(t.runId);
    }
    return [...ids];
  }

  isRunActiveInMemory(runId: string): boolean {
    for (const taskId of this.active.keys()) {
      const t = this.db.getTask(taskId);
      if (t?.runId === runId) return true;
    }
    for (const taskId of this.allocatingWorktrees) {
      const t = this.db.getTask(taskId);
      if (t?.runId === runId) return true;
    }
    return false;
  }

  /** Recomputes and persists the dynamic status for a single run based on live tasks + SQLite state. */
  reconcileRunStatus(runId: string, failedCause?: { reason: string; cause: string }): RunRecord | null {
    const run = this.db.getRun(runId);
    if (!run) return null;
    const tasks = this.db.listTasks(runId);
    const live = this.isRunActiveInMemory(runId);
    const derived = deriveRunStatus(run, tasks, live);
    if (derived !== run.status) {
      const why = (derived === "failed" ? failedCause : undefined) ?? deriveRunCause(tasks, derived);
      this.db.updateRunStatus(runId, derived, why?.reason, why?.cause);
      return this.db.getRun(runId);
    }
    return run;
  }

  /** Returns a run with its dynamically reconciled status (safe for read paths like runs.trees/run.list/sessions.list). */
  getEffectiveRun(runId: string): RunRecord | null {
    const run = this.db.getRun(runId);
    if (!run) return null;
    const tasks = this.db.listTasks(runId);
    const live = this.isRunActiveInMemory(runId);
    const derived = deriveRunStatus(run, tasks, live);
    if (derived !== run.status && tasks.length > 0) {
      const why = deriveRunCause(tasks, derived);
      this.db.updateRunStatus(runId, derived, why?.reason, why?.cause);
      return this.db.getRun(runId);
    }
    return derived === run.status ? run : { ...run, status: derived };
  }

  /**
   * Called on daemon startup when 0 agent tasks are alive in memory:
   *  - Fails orphaned in-flight / unlaunched tasks from a previous crashed daemon, clears stale merges & leases.
   *  - Re-arms quota quiescence timers for runs that were cleanly paused on quota before the restart.
   */
  reconcileStaleRunsOnStartup(): void {
    if (this.active.size > 0 || this.allocatingWorktrees.size > 0) return;
    const runs = this.db.listRuns(200);
    for (const run of runs) {
      const tasks = this.db.listTasks(run.id);
      let mutated = false;
      let interrupted = false;
      for (const t of tasks) {
        if (LIVE_TASK_STATUSES.has(t.status) || t.status === "ready" || t.status === "pending") {
          this.db.updateTask(t.id, { status: "failed", summary: "Interrupted by daemon restart" });
          mutated = true;
          interrupted = true;
        }
      }
      const failedMerges = this.db.failStaleMergesForRun(run.id, "Interrupted by daemon restart");
      if (failedMerges > 0) mutated = true;

      const updatedTasks = this.db.listTasks(run.id);
      const hasQuotaPaused = updatedTasks.some((t) => t.status === "paused_quota");
      if (!hasQuotaPaused) {
        this.db.releaseWriteLeasesForRun(run.id);
      } else {
        // Even when a sibling task is cleanly quota-paused, ALWAYS release leases owned by dead/terminal tasks!
        this.db.db
          .prepare(
            `UPDATE write_leases SET active = 0
             WHERE run_id = ? AND active = 1
               AND task_id IN (SELECT id FROM tasks WHERE run_id = ? AND status IN ('failed','cancelled','completed'))`,
          )
          .run(run.id, run.id);
      }

      if (mutated || run.status === "active") {
        // Statuses stay "failed"; the cause tells consumers the run was interrupted and resumes in place.
        this.reconcileRunStatus(
          run.id,
          interrupted ? { reason: "Interrupted by daemon restart; resume continues it in place", cause: "daemon_restart" } : undefined,
        );
      }
      if (hasQuotaPaused || run.status === "paused_quota") {
        void this.checkRunQuotaQuiescence(run.id).catch(() => {});
      }
    }
    // Covers runs beyond the listRuns window; quota-paused task leases survive for resume.
    this.db.purgeStaleWriteLeasesOnStartup();
  }

  private gateRuntimeConfig(): GateRuntimeConfig {
    return {
      gateShell: this.config.goals.gateShell,
      gateEnv: this.config.goals.gateEnv,
      stateDir: this.config.stateDir,
    };
  }

  getWorktreeManager(repoRoot: string): WorktreeManager {
    let mgr = this.worktreeManagers.get(repoRoot);
    if (!mgr) {
      mgr = new WorktreeManager(this.db, repoRoot, this.config.worktree, this.gateRuntimeConfig());
      this.worktreeManagers.set(repoRoot, mgr);
    }
    return mgr;
  }

  getMergeQueue(repoRoot: string): BisectingMergeQueue {
    let mq = this.mergeQueues.get(repoRoot);
    if (!mq) {
      mq = new BisectingMergeQueue(this.db, this.getWorktreeManager(repoRoot), repoRoot, this.config.worktree, this.gateRuntimeConfig());
      this.mergeQueues.set(repoRoot, mq);
    }
    return mq;
  }

  /** The goal linked to this run, if any (the goals table is absent in standalone control-plane databases). */
  private resolveLinkedGoalForRun(runId: string): { id: string } | null {
    try {
      const row = this.db.db.prepare("SELECT id FROM goals WHERE linked_run_id = ? LIMIT 1").get(runId) as { id: string } | null;
      return row ?? null;
    } catch {
      return null;
    }
  }

  private taskRoute(task: TaskRecord, cwd: string, target?: { profileId?: string; model?: string }) {
    return resolveLaunchRoute(this.config, {
      profileId: target?.profileId ?? task.profileId,
      model: target?.model ?? task.model ?? undefined,
      tier: task.tier,
      catalog: this.catalog, quota: this.quota,
      cwd,
    });
  }

  /** Every delegation bucket owned by exactly `ownerId` (one per workspace it delegated in), oldest first. */
  listDirectorTaskRuns(ownerId: string): RunRecord[] {
    const base = directorTaskRunId(ownerId, null);
    const rows = this.db.db
      .prepare("SELECT id FROM runs WHERE id = ? OR substr(id, 1, ?) = ? ORDER BY created_at, id")
      .all(base, base.length + 4, `${base}.ws-`) as { id: string }[];
    return rows
      .filter((r) => directorTaskRunOwner(r.id) === ownerId)
      .map((r) => this.db.getRun(r.id))
      .filter((r): r is RunRecord => r !== null);
  }

  /** The owner's bucket for this workspace, created on first use. A bucket never changes its workspace. */
  async ensureDirectorTaskRun(ownerId: string, repoRoot: string): Promise<RunRecord> {
    const root = canonicalProjectPath(repoRoot);
    const match = () => this.listDirectorTaskRuns(ownerId).find((r) => canonicalProjectPath(r.repoRoot) === root);
    const found = match();
    if (found) return found;
    // Director chat also works in directories without a git checkout.
    const base = await this.getWorktreeManager(root).resolveRepoInfo().catch(() => ({ baseBranch: "", baseSha: "" }));
    return this.db.transaction(() => {
      const existing = match();
      if (existing) return existing;
      const baseId = directorTaskRunId(ownerId, null);
      const id = this.db.getRun(baseId) ? directorTaskRunId(ownerId, root) : baseId;
      const clash = this.db.getRun(id);
      if (clash) throw new Error(`Director task run ${id} already belongs to ${clash.repoRoot}; cannot reuse it for ${root}`);
      const run = this.db.createRun({ id, goal: `Director tasks for ${ownerId}`, repoRoot: root, ...base });
      const owner = this.db.createTask({
        runId: id, tier: 1, kind: "orchestrator", title: "Director delegation", objective: "Track this Director's delegated tasks and artifacts.",
        profileId: "director", readOnly: true,
      });
      // This durable owner never runs an agent or enters the ready queue.
      this.db.updateTask(owner.id, { status: "completed" });
      return run;
    });
  }

  async executeDirectorTaskTool(ownerId: string, repoRoot: string, name: string, args: Record<string, unknown>, signal?: AbortSignal) {
    if (!["spawn_task", "wait_tasks", "wait_children", "cancel_task", "artifact_read"].includes(name)) throw new Error(`Unknown Director task tool ${name}`);
    const ownerTask = (runId: string) => this.db.listTasks(runId, { parentId: null })[0]!;
    if (name === "wait_tasks" || name === "wait_children") {
      // One wait over the synthetic owners of all this owner's buckets, so tasks delegated in earlier workspaces stay waitable.
      const runs = this.listDirectorTaskRuns(ownerId);
      if (runs.length === 0) return { children: [] };
      const [primary, ...others] = runs.map((r) => ownerTask(r.id).id);
      const ids = Array.isArray(args.ids) ? args.ids.map((id) => this.db.getTask(String(id))?.id ?? String(id)) : undefined;
      const fallbackSec =
        typeof args.fallback_interval_s === "number"
          ? args.fallback_interval_s
          : typeof args.timeout_s === "number"
            ? args.timeout_s
            : this.config.events.fallbackWakeupIntervalSec;
      const timeout = Math.max(1, fallbackSec) * 1000;
      const res = await this.waitForChildrenEventDriven(primary!, ids, args.mode === "any" ? "any" : "all", timeout, signal, others);
      const escalationsByRun = new Map<string, ReturnType<ControlPlaneDb["listEscalations"]>>();
      const escalationsFor = (runId: string) => {
        if (!escalationsByRun.has(runId)) escalationsByRun.set(runId, this.db.listEscalations(runId));
        return escalationsByRun.get(runId)!;
      };
      return {
        children: res.children.map((task) => compactTaskHandoff(task, escalationsFor(task.runId))),
        ...(res.wakeReason === "fallback_interval" || res.wakeReason === "child_inactive"
          ? { wakeReason: res.wakeReason }
          : {}),
        ...(res.inactiveTaskIds.length > 0 ? { inactiveTaskIds: res.inactiveTaskIds } : {}),
      };
    }
    // A task delegated in an earlier workspace is handled in its own bucket; reads never create a bucket that already exists elsewhere.
    const taskId = name === "spawn_task" || typeof args.task_id !== "string" ? "" : args.task_id.trim();
    const taskRunId = taskId ? this.db.getTask(taskId)?.runId : undefined;
    const root = canonicalProjectPath(repoRoot);
    // A cancel never reaches into another workspace's bucket: scope is enforced here, not left to the caller.
    const taskBucket = taskRunId && directorTaskRunOwner(taskRunId) === ownerId ? this.db.getRun(taskRunId) : null;
    const run =
      (taskBucket && (name !== "cancel_task" || canonicalProjectPath(taskBucket.repoRoot) === root) ? taskBucket : null) ??
      (name === "spawn_task"
        ? await this.ensureDirectorTaskRun(ownerId, repoRoot)
        : this.listDirectorTaskRuns(ownerId).find((r) => canonicalProjectPath(r.repoRoot) === root));
    if (!run) {
      // Reads and cancels never create a bucket; answer as an empty bucket would.
      if (name === "cancel_task") throw new Error(`Unknown child task ${String(args.task_id)}`);
      if (typeof args.path === "string" && args.path) throw new Error(`No indexed dossier in this run: ${resolve(root, args.path)}`);
      return { artifacts: [] };
    }
    // spawn_task reports an aborted turn itself, with the delegation context, instead of a bare AbortError.
    if (name !== "spawn_task") signal?.throwIfAborted();
    // The caller's turn signal travels separately from code_exec's scriptSignal (the synthetic owner is completed).
    return this.toolExecutor.execute(run.id, ownerTask(run.id).id, name, args, undefined, signal);
  }

  async startRun(input: {
    goal: string;
    repoRoot: string;
    profileId?: string;
    model?: string;
    effort?: string;
    /** Stop epochs captured when the launch was requested (default: now); a stop issued since then wins. */
    stopFence?: { global: number; repo: number };
    /** Called synchronously right after the run row exists and before Tier 1 launches (e.g. to link a goal). */
    onRunCreated?: (run: RunRecord) => void;
    /**
     * Called synchronously after the last await, right before the run and Tier 1 rows exist, to read the latest
     * goal text (e.g. an amendment that arrived while the launch was resolving its route or repository).
     */
    resolveGoal?: () => { goal: string; taskTitle?: string };
  }): Promise<{ run: RunRecord; orchestratorTask: TaskRecord }> {
    const fence = input.stopFence ?? this.launchStopFence(input.repoRoot);
    const { profile, model } = await resolveLaunchRoute(this.config, {
      ...input, tier: 1, catalog: this.catalog, quota: this.quota, cwd: input.repoRoot,
    });
    const wtMgr = this.getWorktreeManager(input.repoRoot);
    const { baseBranch, baseSha } = await wtMgr.resolveRepoInfo();
    // Checked after the last await and before the run exists, so a stop issued meanwhile leaves nothing running.
    if (this.launchFenceBroken(input.repoRoot, fence)) throw new RunStartStoppedError(input.repoRoot);
    const resolved = input.resolveGoal ? input.resolveGoal() : { goal: input.goal };
    const goal = resolved.goal;
    const run = this.db.createRun({ goal, repoRoot: input.repoRoot, baseBranch, baseSha });
    if (input.onRunCreated) {
      try {
        input.onRunCreated(run);
      } catch (err) {
        this.db.updateRunStatus(run.id, "failed", `Run setup failed: ${err instanceof Error ? err.message : String(err)}`);
        throw err;
      }
    }

    const orchestratorTask = this.db.createTask({
      runId: run.id,
      parentId: null,
      tier: 1,
      kind: "orchestrator",
      title: `Orchestrator: ${(resolved.taskTitle ?? goal.split("\n")[0] ?? goal).slice(0, 72)}`,
      objective: goal,
      criteria: "Decompose into disjoint Tier 2 Workers, resolve escalations, verify integration on base branch.",
      readOnly: true,
      profileId: profile.id,
      model,
      effort: input.effort ?? null,
      maxTurns: this.config.rollover.orchestratorMaxTurns,
    });

    this.launchTaskLoop(orchestratorTask.id);
    return { run, orchestratorTask };
  }

  async spawnAndStartTask(input: {
    runId: string;
    parentId: string;
    tier: AgentTier;
    kind: TaskKind;
    title: string;
    objective: string;
    criteria?: string;
    readOnly: boolean;
    profileId?: string;
    model?: string;
    effort?: string;
    maxTurns?: number;
    dependsOn?: string[];
    writeScopes?: string[];
    position?: QueuePositionInput;
    forceStart?: boolean;
    /** The calling turn's signal; delegation is refused when it aborted before the task was created. */
    signal?: AbortSignal;
  }): Promise<TaskRecord> {
    const run = this.db.getRun(input.runId);
    if (!run) throw new Error(`Unknown run ${input.runId}`);
    const toolPolicy = toolPolicyForTask(input.tier, input.kind, input.readOnly);
    const { profile, model } = await resolveLaunchRoute(this.config, {
      ...input, catalog: this.catalog, quota: this.quota, cwd: run.repoRoot,
    });

    const parent = this.db.getTask(input.parentId);
    if (!parent || parent.runId !== run.id) throw new Error(`Unknown parent task ${input.parentId} in ${run.id}`);
    // Synchronous stop checks after the last await and before createTask, each with its own cause. When causes
    // overlap, the broadest one is reported first: whole-run stop, then the caller's cancellation, then the turn.
    const container = isDirectorTaskContainer(run.id);
    if (!container && (this.stoppingRuns.has(run.id) || this.db.getRun(run.id)?.status === "stopped")) {
      // Only a stop in progress or one with a recorded reason is called explicit; a bare derived status is not.
      const explicitStop = this.stoppingRuns.has(run.id) || Boolean(this.db.getLastRunStatusReason(run.id));
      throw new Error(
        `Cannot delegate from run ${run.id}: the run is stopped (${this.causeOrHint(run.id, this.describeRunCause(run.id))}). No task was created. ` +
          (explicitStop
            ? "This is an explicit stop, not a turn or budget limit. "
            : "No stop reason was recorded, so this status may be derived rather than an explicit stop; it is not a turn or budget limit. ") +
          "Nothing restarts automatically. Agents in this run should stop and return; " +
          `the Director (resume_run) or the operator (\`pi-harness resume ${run.id}\`) can resume the run.`,
      );
    }
    if (parent.status === "cancelled") {
      const who = parent.tier === 1
        ? `The Director can resume it with resume_task ${parent.id}, or the operator with \`pi-harness resume ${run.id}\`.`
        : container || parent.parentId === null
          ? `The Director can resume it with resume_task ${parent.id} or delegate the work again.`
          : `Its parent orchestrator can resume it with resume_task ${parent.id} or delegate the work again.`;
      throw new Error(
        `Cannot delegate from task ${parent.id}: it was cancelled (${this.causeOrHint(run.id, parent.summary)}). No task was created. ` +
          `This is an explicit cancellation, not a turn or budget limit; this task should stop and return. ${who}`,
      );
    }
    if (input.signal?.aborted) {
      const why = input.signal.reason instanceof Error ? input.signal.reason.message : input.signal.reason ? String(input.signal.reason) : "";
      throw new Error(
        `Delegation was not started: the calling turn in ${container ? `chat ${directorTaskRunOwner(run.id)}` : `run ${run.id}`} ` +
          `was cancelled before the task was created (${why || `no cause was recorded; inspect with \`pi-harness tree ${run.id}\``}). ` +
          "No task was created. A fresh request can delegate again.",
      );
    }
    const task = this.db.createTask({
      runId: input.runId,
      parentId: input.parentId,
      tier: input.tier,
      kind: input.kind,
      title: input.title,
      objective: input.objective,
      criteria: input.criteria,
      readOnly: toolPolicy !== "read_write_workspace",
      profileId: profile.id,
      model,
      effort: input.effort ?? null,
      maxTurns: input.maxTurns ?? (input.tier === 3 ? this.config.rollover.taskMaxTurns : this.config.rollover.workerMaxTurns),
      dependsOn: input.dependsOn,
      writeScopes: input.writeScopes,
    });

    // Tier 2 Workers get their own isolated git worktree automatically when ready (deferred for `pending`
    // tasks waiting on `depends_on` so they branch off fresh HEAD after their dependency merges!).
    // If `maxConcurrentWorkers` is reached, queue the worker cleanly in `admissionQueue` instead of failing!
    if (task.tier === 2 && task.status === "ready") {
      const wtMgr = this.getWorktreeManager(run.repoRoot);
      if (wtMgr.isGitRepo()) {
        const queuedAhead = this.db.admissionQueue.list({ lane: "worker", repoRoot: run.repoRoot });
        if (!input.forceStart && queuedAhead.length > 0) {
          const qEntry = this.db.admissionQueue.enqueue({
            lane: "worker",
            repoRoot: run.repoRoot,
            targetId: task.id,
            position: input.position,
            forceStart: false,
            note: task.title,
          });
          this.db.updateTask(task.id, { summary: `Queued for worker slot (#${qEntry.position})` });
        } else {
          this.allocatingWorktrees.add(task.id);
          try {
            // Serialized with cancelTask's teardown: a cancel during allocation tears the new worktree down after it exists.
            await this.runWorktreeOp(task.id, () => wtMgr.createForWorker(run.id, task.id, undefined, { force: Boolean(input.forceStart) }));
          } catch (error) {
            if (this.db.getTask(task.id)?.status === "cancelled") {
              // Cancelled while allocating: never queue or fail over the recorded cancellation.
            } else if (error instanceof WorktreeConcurrencyLimitError || String(error).includes("concurrency cap reached")) {
              const qEntry = this.db.admissionQueue.enqueue({
                lane: "worker",
                repoRoot: run.repoRoot,
                targetId: task.id,
                position: input.position,
                forceStart: false,
                note: task.title,
              });
              this.db.updateTask(task.id, { summary: `Queued for worker slot (#${qEntry.position})` });
            } else {
              this.db.updateTask(task.id, { status: "failed", summary: "Could not allocate the worker worktree." });
              throw error;
            }
          } finally {
            this.allocatingWorktrees.delete(task.id);
          }
        }
      }
    } else if (task.tier === 3 && input.parentId) {
      // Tier 3 Task Agents inherit their parent Worker's worktree
      if (parent.worktreeId) {
        this.db.updateTask(task.id, { worktreeId: parent.worktreeId });
      }
    }

    const updated = this.db.getTask(task.id)!;
    if (updated.status === "ready") {
      this.launchTaskLoop(updated.id);
    }
    return updated;
  }

  launchTaskLoop(taskId: string): void {
    if (this.active.has(taskId) || this.allocatingWorktrees.has(taskId)) return;
    const abort = new AbortController();
    const entry: ActiveTaskExecution = {
      taskId,
      abort,
      promise: Promise.resolve(),
      phase: "starting",
    };
    // Register the execution before the run turns active so synchronous run.status listeners see it live.
    this.active.set(taskId, entry);
    const initialTask = this.db.getTask(taskId);
    if (initialTask) {
      const owningRun = this.db.getRun(initialTask.runId);
      if (owningRun && owningRun.status !== "active") {
        try {
          this.db.updateRunStatus(owningRun.id, "active");
        } catch (error) {
          this.active.delete(taskId);
          throw error;
        }
      }
    }
    entry.promise = this.runTaskWithRollovers(taskId, entry).catch((err) => {
      const task = this.db.getTask(taskId);
      if (!task || entry.abort.signal.aborted) return;
      this.db.updateTask(taskId, { status: "failed", summary: `Task launch failed: ${String(err instanceof Error ? err.message : err).slice(0, 240)}` });
      this.db.emitEvent(task.runId, taskId, "agent.error", { error: String(err) });
    }).finally(() => {
      // An entry evicted after a cancel-unwind timeout must never remove the newer generation's entry.
      if (this.active.get(taskId) === entry) {
        this.active.delete(taskId);
        this.pendingRollovers.delete(taskId);
        this.disposeSteeringState(taskId);
      }
      for (const item of entry.pendingSteers?.splice(0) ?? []) item.onOutcome?.(false, "generation_ended_without_handle");
      const t = this.db.getTask(taskId);
      if (t) this.afterTaskInactiveInRun(t.runId);
    });
  }

  /**
   * After a task of `runId` stops being live: launches newly ready tasks (never in a stopped or stopping run, which
   * would overwrite the stop with "active"), and once nothing in the run is live arms the quota pause or reconciles
   * the run's terminal status.
   */
  private afterTaskInactiveInRun(runId: string): void {
    const runHalted =
      !isDirectorTaskContainer(runId) && (this.stoppingRuns.has(runId) || this.db.getRun(runId)?.status === "stopped");
    const readyTasks = runHalted ? [] : this.db.listTasks(runId, { status: "ready" });
    let launchedAny = false;
    for (const rt of readyTasks) {
      if (!this.active.has(rt.id)) {
        this.launchTaskLoop(rt.id);
        launchedAny = true;
      }
    }
    if (!launchedAny && !this.isRunActiveInMemory(runId)) {
      const paused = this.db.listTasks(runId).some((x) => x.status === "paused_quota");
      if (paused) {
        void this.checkRunQuotaQuiescence(runId).catch(() => {});
      } else {
        this.reconcileRunStatus(runId);
      }
    }
  }

  /**
   * Quiescence Barrier: called when 0 tasks are actively running in `runId`.
   * If any tasks are in `paused_quota`:
   *  - Transitions the run to `paused_quota`.
   *  - If a Quota Endpoint is wired OR passive ACP rate-limit data provided a concrete reset time
   *    (and `autoResumeEnabled` is true): arms an automatic daemon timer (+ early-wakeup poller).
   *  - Otherwise (no Quota Endpoint & no reset time, or automatic resume disabled): stops cleanly and waits for the user to
   *    resume manually or pick a wait duration!
   */
  private async checkRunQuotaQuiescence(runId: string): Promise<void> {
    if (this.quiescenceCheckPending.has(runId)) return;
    const run = this.db.getRun(runId);
    if (!run || ["stopped", "cancelled", "completed", "failed"].includes(run.status)) return;
    if (this.isRunActiveInMemory(runId)) return;

    const tasks = this.db.listTasks(runId);
    const pausedTasks = tasks.filter((t) => t.status === "paused_quota");
    if (pausedTasks.length === 0) return;

    this.quiescenceCheckPending.add(runId);
    try {
      // Query quota status for tasks that actually hit quota (excluding parents paused only to wait on children)
      const directlyQuotaPaused = pausedTasks.filter((t) => this.taskQuotaHints.has(t.id));
      const queryTargets = directlyQuotaPaused.length > 0 ? directlyQuotaPaused : pausedTasks;
      const queries = queryTargets.map((t) => {
        const hint = this.taskQuotaHints.get(t.id);
        const profile = this.config.profiles.find((p) => p.id === t.profileId);
        const provider =
          profile?.kind === "pi"
            ? profile.provider
            : profile?.adapter && profile.adapter !== "generic"
              ? profile.adapter
              : t.profileId;
        return {
          profileId: t.profileId,
          provider,
          wireModelId: hint?.wireModel ?? t.model ?? "*",
          canonicalModelId: t.model ?? undefined,
          hintResetAt: hint?.resetAt ?? null,
        };
      });

      const schedule = await this.quota.resolveResumeSchedule(queries, { forceSync: true });

      // Ensure run wasn't manually resumed or stopped while we queried the Quota Endpoint
      const freshRun = this.db.getRun(runId);
      if (!freshRun || freshRun.status === "stopped" || this.isRunActiveInMemory(runId)) return;
      const stillPaused = this.db.listTasks(runId).filter((t) => t.status === "paused_quota");
      if (stillPaused.length === 0) return;

      this.clearRunQuotaTimers(runId);
      this.db.updateRunStatus(runId, "paused_quota");

      const firstReason =
        stillPaused.map((t) => this.taskQuotaHints.get(t.id)?.reason ?? t.summary).find(Boolean) ?? "Quota limit reached";
      const autoEnabled = this.config.quotaRetry.autoResumeEnabled !== false;
      const prevPauseStreak = this.runConsecutiveQuotaPauses.get(runId) ?? 0;
      const pauseStreak = prevPauseStreak + 1;
      this.runConsecutiveQuotaPauses.set(runId, pauseStreak);

      let effectiveResumeAtMs = schedule.resumeAtMs;
      // Prevent tight 1.5s infinite 429 loops when a lagging Quota Endpoint reports `availableNow: true`
      // on consecutive quota pauses with no progress in between.
      if (effectiveResumeAtMs !== null && pauseStreak >= 2 && effectiveResumeAtMs - Date.now() < 15_000) {
        const baseSec = Math.max(15, this.config.quotaRetry.fallbackCooldownSec ?? 60);
        const maxSec = Math.max(baseSec, this.config.quotaRetry.maxCooldownSec ?? 900);
        const backoffMs = Math.min(maxSec, baseSec * Math.pow(2, pauseStreak - 2)) * 1000;
        effectiveResumeAtMs = Date.now() + backoffMs;
      }
      const canAutoSchedule = autoEnabled && effectiveResumeAtMs !== null;

      const state: InternalRunQuotaState = {
        runId,
        pausedTaskIds: stillPaused.map((t) => t.id),
        mode: canAutoSchedule ? "auto_timer" : "manual",
        source: canAutoSchedule ? (schedule.source === "unknown" ? "none" : schedule.source) : "none",
        resumeAt: canAutoSchedule && effectiveResumeAtMs ? new Date(effectiveResumeAtMs).toISOString() : null,
        reason: firstReason,
        pausedAt: Date.now(),
        timer: null,
        earlyWakePoller: null,
      };

      if (canAutoSchedule && effectiveResumeAtMs) {
        const deadline = effectiveResumeAtMs;
        const delayMs = Math.max(250, deadline - Date.now());
        const armTimer = () => {
          const remaining = deadline - Date.now();
          state.timer = setTimeout(() => {
            if (this.runQuotaStates.get(runId) !== state) return;
            if (Date.now() < deadline) armTimer();
            else void this.resumeRunFromQuota(runId, { trigger: "auto_timer" }).catch(() => {});
          }, Math.max(250, Math.min(remaining, 2_147_483_647)));
          state.timer.unref?.();
        };
        armTimer();

        // If a Quota Endpoint is wired and the wait is long, also poll periodically to wake up early if quota returns!
        const pollMs = Math.max(5_000, this.config.quotaRetry.earlyWakePollMs ?? 30_000);
        if (this.quota.isConfigured() && delayMs > pollMs) {
          state.earlyWakePoller = setInterval(() => {
            void this.quota
              .resolveResumeSchedule(queries, { forceSync: true })
              .then((check) => {
                if (check.availableNow && this.runQuotaStates.get(runId) === state) {
                  void this.resumeRunFromQuota(runId, { trigger: "quota_recovered" }).catch(() => {});
                }
              })
              .catch(() => {});
          }, pollMs);
          state.earlyWakePoller.unref?.();
        }
      }

      this.runQuotaStates.set(runId, state);
      this.db.emitEvent(runId, null, "quota.run_paused", {
        runId,
        pausedTaskIds: state.pausedTaskIds,
        mode: state.mode,
        source: state.source,
        resumeAt: state.resumeAt,
        reason: state.reason,
        hasQuotaEndpoint: this.quota.isConfigured(),
        detail: canAutoSchedule
          ? schedule.detail
          : !autoEnabled
            ? `Automatic quota resume is disabled (quotaRetry.autoResumeEnabled is false)${
                effectiveResumeAtMs !== null ? `; quota is expected to reset at ${new Date(effectiveResumeAtMs).toISOString()}` : ""
              } — paused cleanly. Resume manually or choose a wait duration.`
            : "No Quota Endpoint or reset time available — paused cleanly. Resume anytime or choose a wait duration.",
      });
    } finally {
      this.quiescenceCheckPending.delete(runId);
    }
  }

  /**
   * Lets the user choose how long to wait before automatically resuming a quota-paused run
   * (especially useful when no Quota Endpoint is wired up!).
   */
  scheduleRunQuotaResume(runId: string, delayMs: number): RunQuotaPauseStatus {
    const run = this.db.getRun(runId);
    if (!run) throw new Error(`Unknown run ${runId}`);
    const pausedTasks = this.db.listTasks(runId).filter((t) => t.status === "paused_quota");
    if (pausedTasks.length === 0 && run.status !== "paused_quota") {
      throw new Error(`Run ${runId} has no quota-paused tasks to schedule`);
    }

    const clampedMs = Math.max(1_000, Math.min(delayMs, 24 * 60 * 60 * 1000));
    this.clearRunQuotaTimers(runId);
    const prev = this.runQuotaStates.get(runId);
    const resumeAtIso = new Date(Date.now() + clampedMs).toISOString();

    const state: InternalRunQuotaState = {
      runId,
      pausedTaskIds: pausedTasks.map((t) => t.id),
      mode: "user_timer",
      source: "user_timer",
      resumeAt: resumeAtIso,
      reason: prev?.reason ?? pausedTasks[0]?.summary ?? "Quota limit reached",
      pausedAt: prev?.pausedAt ?? Date.now(),
      timer: null,
      earlyWakePoller: null,
    };

    state.timer = setTimeout(() => {
      void this.resumeRunFromQuota(runId, { trigger: "user_timer" }).catch(() => {});
    }, clampedMs);
    state.timer.unref?.();

    this.runQuotaStates.set(runId, state);
    if (run.status !== "paused_quota") {
      this.db.updateRunStatus(runId, "paused_quota");
    }
    this.db.emitEvent(runId, null, "quota.run_scheduled", {
      runId,
      pausedTaskIds: state.pausedTaskIds,
      resumeAt: resumeAtIso,
      delayMs: clampedMs,
      source: "user_timer",
    });
    return this.getRunQuotaStatus(runId)!;
  }

  /**
   * Resumes a quota-paused run (manually, via user-chosen wait timer, via Quota Endpoint timer,
   * or early wakeup) and COMPLETELY RESETS the quota pause state machine.
   * If no quota is actually available when resumed, the restarted tasks naturally walk through
   * the exact same states (native retry up to timeout -> paused_quota -> quiescence -> pause again).
   */
  async resumeRunFromQuota(
    runId: string,
    opts?: { trigger?: "manual" | "auto_timer" | "quota_recovered" | "user_timer"; userMessage?: string },
  ): Promise<{ resumed: boolean; resumedTaskIds: string[]; run: RunRecord }> {
    const { fenced: _none, ...res } = this.resumeRunFromQuotaInternal(runId, opts);
    return res;
  }

  /**
   * Synchronous body of `resumeRunFromQuota`. With `fence` (the stop/cancel snapshot taken when a run resume was
   * requested), a run stop issued since then refuses the whole resume and an explicitly cancelled task is skipped.
   */
  private resumeRunFromQuotaInternal(
    runId: string,
    opts: { trigger?: "manual" | "auto_timer" | "quota_recovered" | "user_timer"; userMessage?: string } | undefined,
    fence?: StopSnapshot,
  ): { resumed: boolean; resumedTaskIds: string[]; run: RunRecord; fenced?: string } {
    const run = this.db.getRun(runId);
    if (!run) throw new Error(`Unknown run ${runId}`);

    if (fence) {
      const repoRoot = run.repoRoot;
      const stoppedSince =
        (this.stopEpoch.get(runId) ?? 0) !== fence.run ||
        this.globalStopEpoch !== fence.global ||
        (repoRoot ? this.getRepoStopEpoch(repoRoot) : 0) !== fence.repo ||
        this.stoppingRuns.has(runId) ||
        run.status === "stopped";
      if (stoppedSince) {
        return {
          resumed: false,
          resumedTaskIds: [],
          run,
          fenced: `Resume of run ${runId} was not applied: it was stopped after the resume was requested (${this.causeOrHint(runId, this.describeRunCause(runId))}). Nothing was started. Resume it again to continue.`,
        };
      }
    }

    const trigger = opts?.trigger ?? "manual";
    this.clearRunQuotaTimers(runId);
    this.runQuotaStates.delete(runId);

    const tasks = this.db.listTasks(runId);
    const pausedTasks = tasks.filter((t) => t.status === "paused_quota" && (!fence || this.stopWonSince(t, fence) === null));
    if (pausedTasks.length === 0) {
      if (run.status === "paused_quota") {
        this.db.updateRunStatus(runId, "active");
      }
      return { resumed: false, resumedTaskIds: [], run: this.db.getRun(runId)! };
    }

    // Reset passive quota exhaustion state for all affected profiles so the state machine starts fresh!
    const affectedProfiles = new Set(pausedTasks.map((t) => t.profileId));
    for (const pid of affectedProfiles) {
      this.quota.clearPassiveExhaustion(pid);
    }

    this.db.updateRunStatus(runId, "active");
    const resumedTaskIds: string[] = [];

    for (const t of pausedTasks) {
      const hint = this.taskQuotaHints.get(t.id);
      this.taskQuotaHints.delete(t.id);
      this.taskResumeNotes.set(t.id, {
        kind: "quota",
        reason: hint?.reason ?? t.summary ?? "quota limit",
        userMessage: opts?.userMessage ?? null,
      });
      this.db.updateTask(t.id, {
        status: "ready",
        summary: null,
        sessionId: null,
      });
      resumedTaskIds.push(t.id);
    }

    this.db.emitEvent(runId, null, "quota.run_resumed", {
      runId,
      trigger,
      resumedTaskIds,
      userMessage: opts?.userMessage ?? null,
    });

    for (const id of resumedTaskIds) {
      this.launchTaskLoop(id);
    }

    return { resumed: true, resumedTaskIds, run: this.db.getRun(runId)! };
  }

  /**
   * Resumes a single failed, crashed, cancelled, blocked, or paused task IN-PLACE (same taskId,
   * bumped generation, preserved worktree/WIP, deterministic recovery handoff) instead of spawning a duplicate.
   * Backward-compatible wrapper over `resumeTaskDetailed`: returns the task for every idempotent outcome,
   * and throws when a resume note could not reach the task.
   */
  async resumeTask(taskId: string, opts?: ResumeTaskOptions): Promise<TaskRecord> {
    const res = await this.resumeTaskDetailed(taskId, opts);
    if (opts?.note?.trim() && res.outcome !== "resumed" && !res.noteDelivered) throw new Error(res.message);
    return res.task;
  }

  /**
   * Idempotent, single-flight, stop-aware task resume. Repeated or concurrent calls return the existing
   * identity and actual state ("already_active", "already_queued", "waiting", "already_completed") and never
   * bump the generation twice or allocate a second worktree. An explicit stop issued after the resume was
   * requested always wins.
   */
  async resumeTaskDetailed(taskId: string, opts?: ResumeTaskOptions): Promise<ResumeTaskResult> {
    return this.resumeTaskInternal(taskId, opts);
  }

  /** Stop/cancel epochs at request time: this run, its repo, all runs, and each listed task's cancel epoch. */
  private stopEpochSnapshot(runId: string, taskIds: string[]): StopSnapshot {
    const repoRoot = this.db.getRun(runId)?.repoRoot;
    return {
      run: this.stopEpoch.get(runId) ?? 0,
      repo: repoRoot ? this.getRepoStopEpoch(repoRoot) : 0,
      global: this.globalStopEpoch,
      tasks: new Map(taskIds.map((id) => [id, this.stopEpoch.get(id) ?? 0])),
      explicit: this.explicitCancelSnapshot(taskIds),
    };
  }

  private explicitCancelSnapshot(taskIds: string[]): Map<string, number> {
    return new Map(taskIds.map((id) => [id, this.explicitCancelEpoch.get(id) ?? 0]));
  }

  private snapshotKey(snap: StopSnapshot, taskIds: string[]): string {
    return [
      snap.run,
      snap.repo,
      snap.global,
      ...taskIds.map((id) => `${id}=${snap.tasks.get(id) ?? 0}/${snap.explicit?.get(id) ?? 0}`),
    ].join(":");
  }

  /** Which newer stop (if any) beat a resume requested under `snap`: the whole run / repo / all runs, or this task alone. */
  private stopWonSince(task: TaskRecord, snap: StopSnapshot): "run" | "task" | null {
    const repoRoot = this.db.getRun(task.runId)?.repoRoot;
    if (
      (this.stopEpoch.get(task.runId) ?? 0) !== snap.run ||
      this.globalStopEpoch !== snap.global ||
      (repoRoot ? this.getRepoStopEpoch(repoRoot) : 0) !== snap.repo ||
      this.stoppingRuns.has(task.runId)
    ) {
      return "run";
    }
    if ((this.stopEpoch.get(task.id) ?? 0) !== (snap.tasks.get(task.id) ?? 0)) return "task";
    // An explicit cancel issued after request entry (also while waiting on an earlier stop) always wins.
    if (snap.explicit && (this.explicitCancelEpoch.get(task.id) ?? 0) !== (snap.explicit.get(task.id) ?? 0)) return "task";
    return null;
  }

  private async resumeTaskInternal(
    taskId: string,
    opts: ResumeTaskOptions | undefined,
    runSnapshot?: StopSnapshot,
    /** Shared wind-down deadline (epoch ms) of an enclosing run resume. */
    windDownDeadline?: number,
  ): Promise<ResumeTaskResult> {
    const initial = this.db.getTask(taskId);
    if (!initial) throw new Error(`Unknown task ${taskId}`);
    if (initial.profileId === "director") {
      throw new Error(
        `Task ${initial.id} is the Director's delegation container, not a runnable task. Resume the delegated task itself with resume_task, or delegate again with spawn_task.`,
      );
    }
    // Stop epochs are captured at request time, so any stop or cancel issued after this call wins. A stop already
    // in progress was issued first: wait for it, then capture (its own cancels must not refuse this resume). Explicit
    // cancels are snapshotted at entry, before that wait, so one issued during the wait still fences this resume.
    let snap = runSnapshot;
    if (!snap) {
      const pendingStop = this.stoppingRuns.get(initial.runId);
      if (pendingStop) {
        const entry = this.stopEpochSnapshot(initial.runId, [initial.id]);
        await pendingStop.catch(() => {});
        snap = { ...entry, tasks: new Map([[initial.id, this.stopEpoch.get(initial.id) ?? 0]]) };
      } else {
        snap = this.stopEpochSnapshot(initial.runId, [initial.id]);
      }
    }
    // A cancelled runner or its worktree teardown still unwinding is waited for (bounded), never reported as active.
    // One deadline covers both this wait and the wind-down in resumeTaskBody.
    const deadline = windDownDeadline ?? Date.now() + this.cancelUnwindTimeoutMs;
    await this.awaitCancelUnwind([initial.id], deadline);
    if (this.taskTeardowns.has(initial.id)) {
      throw new Error(`Worktree of task ${initial.id} is still being archived; retry the resume in a few seconds.`);
    }
    const key = this.snapshotKey(snap, [initial.id]);
    // Join only a resume requested under the same epochs; one from before a newer stop or cancel is never joined.
    for (;;) {
      const inflight = this.resumingTasks.get(taskId);
      if (!inflight) break;
      if (inflight.key === key) return this.joinTaskResume(taskId, inflight, opts);
      await inflight.promise.catch(() => {});
    }
    const p = this.resumeTaskBody(taskId, opts, snap, deadline);
    const entry = { key, promise: p, note: opts?.note?.trim() || undefined };
    this.resumingTasks.set(taskId, entry);
    try {
      return await p;
    } finally {
      if (this.resumingTasks.get(taskId) === entry) this.resumingTasks.delete(taskId);
    }
  }

  /** A concurrent caller shares the first caller's result; its own different note is still delivered or reported. */
  private async joinTaskResume(
    taskId: string,
    inflight: { promise: Promise<ResumeTaskResult>; note?: string },
    opts: ResumeTaskOptions | undefined,
  ): Promise<ResumeTaskResult> {
    const res = await inflight.promise;
    const note = opts?.note?.trim();
    // The first caller's note delivery (and the prose describing it) is not this caller's.
    const { noteDelivered: _first, noteDelivery: _firstDelivery, ...shared } = res;
    const baseMessage = this.resumeBaseMessages.get(res) ?? res.message;
    if (!note) return { ...shared, message: baseMessage };
    if (note === inflight.note) return res;
    // This caller's own precondition guards its note: a failed guard never forwards it.
    let valid: boolean;
    try {
      valid = opts?.validateBeforeActivate ? Boolean(await opts.validateBeforeActivate()) : true;
    } catch {
      valid = false;
    }
    if (!valid) {
      throw new Error(`Task ${taskId} resume cancelled: precondition no longer holds. Your note was not delivered.`);
    }
    const own = await this.deliverNoteToTaskDetailed(taskId, note);
    const sentence = own.delivery
      ? own.delivered
        ? "Your note was delivered as steering."
        : `Your note could not be delivered as steering yet: ${own.delivery.reason}`
      : own.delivered
        ? "Your note was also delivered to the task."
        : "Your note could not be delivered: the task is not live.";
    return {
      ...shared,
      message: `${baseMessage} ${sentence}`,
      noteDelivered: own.delivered,
      ...(own.delivery ? { noteDelivery: own.delivery } : {}),
    };
  }

  /**
   * Delivers a note to a task that was just resumed or is running: appended to a resume note its new generation
   * has not read yet, otherwise steered into the live session. Reports the steering outcome when it was steered.
   */
  private async deliverNoteToTaskDetailed(taskId: string, note: string): Promise<{ delivered: boolean; delivery?: SteerTaskResult }> {
    const pending = this.taskResumeNotes.get(taskId);
    if (pending?.kind === "resume" || pending?.kind === "quota") {
      // Not consumed by the new generation's first turn yet: it reads both notes.
      pending.userMessage = [pending.userMessage, note].filter(Boolean).join("\n");
      return { delivered: true };
    }
    if (!this.isLiveExecution(taskId)) return { delivered: false };
    const delivery = await this.steerTaskDetailed(taskId, note);
    return { delivered: delivery.steered, delivery };
  }

  private async resumeTaskBody(
    taskId: string,
    opts: ResumeTaskOptions | undefined,
    snap: StopSnapshot,
    deadline: number,
  ): Promise<ResumeTaskResult> {
    // A cancelled runner (or its allocation / teardown) still winding down is waited for, never reported as active.
    if (!(await this.awaitTaskWindDown(taskId, deadline))) throw new Error(this.windDownPendingMessage(taskId));
    const task = this.db.getTask(taskId)!;
    const where = `run ${task.runId}`;
    const settled = ["completed", "cancelled", "failed"].includes(task.status);
    // Queued workers have a live admission loop in the active map, so the queue is checked first.
    const queued = settled ? null : this.db.admissionQueue.get("worker", task.id);
    if (queued) {
      if (opts?.position !== undefined) this.db.admissionQueue.reorder("worker", task.id, opts.position);
      if (opts?.forceStart) this.db.admissionQueue.markForceStart("worker", task.id);
      const entry = this.db.admissionQueue.get("worker", task.id) ?? queued;
      const note = opts?.note?.trim();
      const baseMessage = `Task ${task.id} in ${where} is already queued for a worker slot (#${entry.position}${entry.forceStart ? ", force-start" : ""}); nothing was restarted.`;
      const result: ResumeTaskResult = {
        task,
        outcome: "already_queued",
        queuePosition: entry.position,
        message: baseMessage + (note ? " The note was not delivered: the task has no live session yet." : ""),
        ...(note ? { noteDelivered: false } : {}),
      };
      this.resumeBaseMessages.set(result, baseMessage);
      return result;
    }
    if (this.isLiveExecution(task.id) || this.allocatingWorktrees.has(task.id)) {
      const noteDelivery = opts?.note?.trim() ? await this.steerTaskDetailed(task.id, opts.note.trim()) : undefined;
      const noteDelivered = noteDelivery?.steered;
      const baseMessage = `Task ${task.id} in ${where} is already ${task.status} (generation ${task.generation}); nothing was restarted.`;
      const result: ResumeTaskResult = {
        task: this.db.getTask(task.id)!,
        outcome: "already_active",
        message: baseMessage +
          (noteDelivered === true ? " The note was delivered as steering." : noteDelivered === false ? ` The note could not be delivered as steering yet: ${noteDelivery?.reason ?? "no live session"}` : ""),
        ...(noteDelivery ? { noteDelivered, noteDelivery } : {}),
      };
      this.resumeBaseMessages.set(result, baseMessage);
      return result;
    }
    if (task.status === "ready") {
      const won = this.stopWonSince(task, snap);
      if (won) throw new Error(this.resumeNotAppliedMessage(task, won));
      this.launchTaskLoop(task.id);
      return {
        task: this.db.getTask(task.id)!,
        outcome: "resumed",
        message: `Task ${task.id} in ${where} was ready but not running; it was started (generation ${task.generation}).`,
      };
    }
    if (task.status === "pending") {
      return {
        task,
        outcome: "waiting",
        message: `Task ${task.id} in ${where} is waiting on its dependencies (${(this.db.db.prepare("SELECT depends_on FROM task_dependencies WHERE task_id = ?").all(task.id) as Array<{ depends_on: string }>).map((d) => d.depends_on).join(", ") || "none recorded"}); it starts automatically when they complete.`,
      };
    }
    if (task.status === "completed" && !opts?.allowCompleted) {
      return {
        task,
        outcome: "already_completed",
        message: `Task ${task.id} in ${where} already completed; nothing was restarted. Read its result with wait_children or \`pi-harness tree ${task.runId}\`.`,
      };
    }

    const stopWon = () => this.stopWonSince(task, snap);
    const won = stopWon();
    if (won) {
      // Report the newest stop's recorded cause once it has been written.
      await this.stoppingRuns.get(task.runId)?.catch(() => {});
      throw new Error(this.resumeNotAppliedMessage(task, won));
    }
    return this.performTaskResume(task, opts, stopWon);
  }

  private windDownPendingMessage(taskId: string): string {
    return `Task ${taskId} is still winding down (its cancelled runner or worktree teardown has not finished); nothing was started. Retry the resume in a few seconds.`;
  }

  private resumeNotAppliedMessage(task: TaskRecord, by: "run" | "task"): string {
    const latest = this.db.getTask(task.id) ?? task;
    const cause = by === "run" ? (this.describeRunCause(task.runId) ?? latest.summary) : latest.summary;
    return `Resume of task ${task.id} in run ${task.runId} was not applied: it was stopped after the resume was requested (${this.causeOrHint(task.runId, cause)}). Nothing was started. Resume it again to continue.`;
  }

  private async performTaskResume(
    task: TaskRecord,
    opts: ResumeTaskOptions | undefined,
    stopWon: () => "run" | "task" | null,
  ): Promise<ResumeTaskResult> {
    const run = this.db.getRun(task.runId);
    if (!run) throw new Error(`Unknown run ${task.runId}`);

    const toolPolicy = toolPolicyForTask(task.tier, task.kind, task.readOnly);
    // An earlier cancel's teardown (or allocation) finishes before the worktree is inspected or recreated.
    await this.worktreeOps.get(task.id);
    const currentWtId = this.db.getTask(task.id)?.worktreeId ?? task.worktreeId;
    let wt = currentWtId ? this.db.getWorktree(currentWtId) : null;
    let createdWorktreeId: string | null = null;
    /** Set when the previous worktree was archived and this generation starts on a fresh one from base. */
    let archivedFrom: { worktreeId: string; ref: string | null } | null = null;
    /** Tier 2 writers take their leases only together with the switch to ready, after the last await below. */
    const needsLeases = toolPolicy === "read_write_workspace" && task.tier === 2;
    let profile: RunnerProfileConfig;
    let model: string;
    const preconditionFailed = () => new Error(`Task ${task.id} resume cancelled: precondition no longer holds.`);
    const abandon = (err: Error): never => {
      if (createdWorktreeId) {
        const id = createdWorktreeId;
        void this.runWorktreeOp(task.id, () =>
          this.getWorktreeManager(run.repoRoot).teardown(id, { archiveBeforeRemove: true, finalStatus: "archived" }),
        ).catch(() => {});
      }
      throw err;
    };
    if (opts?.validateBeforeActivate && !opts.validateBeforeActivate()) throw preconditionFailed();
    try {
      if (toolPolicy === "read_write_workspace") {
        if (wt && wt.status === "merged") {
          throw new Error(
            `Cannot resume worker ${task.id}: its worktree ${wt.id} was already merged. Its changes are integrated; spawn a new worker for follow-up work.`,
          );
        }
        // Fail fast on a conflict before slow setup; nothing is held across the awaits below.
        if (needsLeases) this.db.reacquireWriteLeasesForTask(run.id, task.id, { dryRun: true });
        const wtMgr = this.getWorktreeManager(run.repoRoot);
        if (wtMgr.isGitRepo()) {
          if (!wt || wt.runId !== run.id || !existsSync(wt.path)) {
            if (wt && wt.runId === run.id) archivedFrom = { worktreeId: wt.id, ref: wt.archiveRef };
            try {
              wt = await this.runWorktreeOp(task.id, () =>
                wtMgr.createForWorker(run.id, task.id, undefined, { force: Boolean(opts?.forceStart) }),
              );
              createdWorktreeId = wt.id;
            } catch (err) {
              if (err instanceof WorktreeConcurrencyLimitError || String(err).includes("concurrency cap reached")) {
                // Queue cleanly instead of failing!
                this.db.admissionQueue.enqueue({
                  lane: "worker",
                  repoRoot: run.repoRoot,
                  targetId: task.id,
                  position: opts?.position,
                  forceStart: Boolean(opts?.forceStart),
                  note: task.title,
                });
                wt = null;
              } else {
                throw err;
              }
            }
          } else if (wt.status !== "active") {
            this.db.updateWorktree(wt.id, { status: "active", cleanedAt: null });
          }
        }
      }

      const cwd = wt?.path ?? run.repoRoot;
      const route = await this.taskRoute(task, cwd, {
        profileId: opts?.profileId,
        model: opts?.model,
      });
      profile = route.profile;
      model = route.model;
    } catch (err) {
      return abandon(err instanceof Error ? err : new Error(String(err)));
    }

    const priorReason = task.summary?.trim() || `previous status was '${task.status}'`;
    const priorHandoff = task.handoffJson ? (JSON.parse(task.handoffJson) as AnchoredHandoff) : null;
    const handoff = await this.buildCleanRolloverHandoff({
      runId: run.id,
      task,
      priorHandoff,
      explicitHandoff: null,
      worktreePath: wt?.path ?? null,
      result: { text: priorReason, stopReason: "error" },
    });
    const archiveNote = archivedFrom
      ? `The previous worktree ${archivedFrom.worktreeId} was archived when the task was cancelled; this generation starts on a fresh worktree from base branch ${run.baseBranch || "HEAD"}` +
        (archivedFrom.ref ? ` and the archived work is at ${archivedFrom.ref} (inspect it with \`git log ${archivedFrom.ref}\` and cherry-pick what is still needed).` : " (nothing unmerged was archived).")
      : null;
    if (archiveNote) handoff.nextActions.unshift(archiveNote);
    if (opts?.note?.trim()) {
      handoff.nextActions.unshift(`Resume guidance: ${opts.note.trim()}`);
    }
    // Re-check after the last await: an explicit stop issued meanwhile always wins over this older resume.
    const won = stopWon();
    if (won) {
      this.db.admissionQueue.remove("worker", task.id);
      return abandon(new Error(this.resumeNotAppliedMessage(task, won)));
    }
    if (opts?.validateBeforeActivate && !opts.validateBeforeActivate()) {
      return abandon(preconditionFailed());
    }
    // No await from here to the switch to ready, so a competing createTask or resume cannot take the scopes between
    // this check and the acquisition below; a conflict reaches the caller instead of a silently deferred start.
    if (needsLeases) {
      try {
        this.db.reacquireWriteLeasesForTask(run.id, task.id, { dryRun: true });
      } catch (err) {
        this.db.admissionQueue.remove("worker", task.id);
        return abandon(err instanceof Error ? err : new Error(String(err)));
      }
    }

    this.quota.clearPassiveExhaustion(profile.id);
    this.clearRunQuotaTimers(run.id);
    this.runQuotaStates.delete(run.id);
    this.runConsecutiveQuotaPauses.delete(run.id);
    this.taskAutoRolloverProgress.delete(task.id);
    this.taskQuotaHints.delete(task.id);
    this.taskResumeNotes.set(task.id, {
      kind: "resume",
      reason: priorReason,
      userMessage: opts?.note?.trim() || "Continue from preserved worktree/artifacts without redoing completed steps.",
    });

    // If a Goal is linked to this run and was paused/stopped, flip it back to executing.
    // Done before reactivating the run so run.status listeners see the goal's live status.
    try {
      this.db.db
        .prepare("UPDATE goals SET status = 'executing', updated_at = ? WHERE linked_run_id = ? AND status NOT IN ('achieved','cancelled')")
        .run(Date.now(), run.id);
    } catch {
      // goals table may not exist in unit tests using ControlPlaneDb alone
    }

    const runBefore = this.db.getRun(run.id)?.status ?? run.status;
    if (runBefore !== "active") {
      this.db.updateRunStatus(run.id, "active", `Resumed task ${task.id}`);
    }

    const nextGen = task.generation + 1;
    // Lease acquisition and the switch to ready are one transaction.
    this.db.transaction(() => {
      if (needsLeases) this.db.reacquireWriteLeasesForTask(run.id, task.id);
      this.db.updateTask(task.id, {
        status: "ready",
        generation: nextGen,
        turnsUsed: 0,
        profileId: profile.id,
        model,
        effort: opts?.effort !== undefined ? opts.effort : task.effort,
        summary: null,
        sessionId: null,
        handoffJson: JSON.stringify(handoff),
      });
    });

    this.db.emitEvent(run.id, task.id, "task.resumed", {
      taskId: task.id,
      fromStatus: task.status,
      generation: nextGen,
      model,
      effort: opts?.effort !== undefined ? opts.effort : task.effort,
      note: opts?.note ?? null,
    });

    this.launchTaskLoop(task.id);
    return {
      task: this.db.getTask(task.id)!,
      outcome: "resumed",
      message: `Task ${task.id} resumed in place in run ${run.id} (generation ${nextGen}, was ${task.status}${runBefore === "stopped" ? "; run was stopped and is active again" : ""}).` +
        (archiveNote ? ` ${archiveNote}` : ""),
    };
  }

  /**
   * Resumes a run in-place: delegates quota-paused runs to `resumeRunFromQuota`, and resumes the Tier 1
   * orchestrator (failed, blocked, returned for rescope, or cancelled by a stop) plus tasks interrupted by a
   * daemon restart. Cancelled child workers are never revived; Tier 1 decides which ones to resume_task.
   * Idempotent and single-flight: a live or completed run returns its identity and actual state.
   */
  async resumeRun(
    runId: string,
    opts?: { trigger?: "manual" | "auto_timer" | "quota_recovered" | "user_timer"; userMessage?: string; note?: string },
  ): Promise<ResumeRunResult> {
    if (!this.db.getRun(runId)) throw new Error(`Unknown run ${runId}`);
    // Epochs (run, repo, all runs, and every task's cancel epoch) are captured at request time, without yielding
    // unless a stop issued earlier is still in progress; each candidate's resume is then fenced by this snapshot.
    // Explicit task cancels are snapshotted at entry, before any wait, so one issued during that wait still fences.
    const pendingStop = this.stoppingRuns.get(runId);
    let snap: StopSnapshot;
    if (pendingStop) {
      const entry = this.stopEpochSnapshot(runId, this.db.listTasks(runId).map((t) => t.id));
      await pendingStop.catch(() => {});
      snap = { ...entry, tasks: this.stopEpochSnapshot(runId, this.db.listTasks(runId).map((t) => t.id)).tasks };
    } else {
      snap = this.stopEpochSnapshot(runId, this.db.listTasks(runId).map((t) => t.id));
    }
    const key = this.snapshotKey(snap, [...snap.tasks.keys()].sort());
    const note = (opts?.note ?? opts?.userMessage)?.trim() || undefined;
    for (;;) {
      const inflight = this.resumingRuns.get(runId);
      if (!inflight) break;
      if (inflight.key === key) {
        // A concurrent caller shares the first caller's result; its own different note is delivered or reported.
        const res = await inflight.promise;
        if (!note) {
          const { noteDelivered: _first, noteDelivery: _firstDelivery, ...shared } = res;
          return shared;
        }
        if (note === inflight.note) return res;
        return this.withRunNote(res, await this.deliverNoteToRun(runId, note));
      }
      // Requested before a newer stop or cancel: never joined; let it fail its own fence first.
      await inflight.promise.catch(() => {});
    }
    const p = this.resumeRunInternal(runId, opts, snap);
    const entry = { key, promise: p, note };
    this.resumingRuns.set(runId, entry);
    try {
      return await p;
    } finally {
      if (this.resumingRuns.get(runId) === entry) this.resumingRuns.delete(runId);
    }
  }

  private runResult(
    runId: string,
    outcome: ResumeRunResult["outcome"],
    message: string,
    extra: Partial<ResumeRunResult> = {},
  ): ResumeRunResult {
    const run = this.getEffectiveRun(runId) ?? this.db.getRun(runId)!;
    let reason: ResumeRunResult["reason"];
    if (outcome === "already_active") reason = "already_running";
    else if (outcome === "already_completed") reason = "already_completed";
    else if (outcome === "nothing_to_resume") {
      const blocked = this.db.listTasks(runId).filter((t) => t.status === "blocked" && t.profileId !== "director");
      const open = blocked.length > 0 ? this.db.listEscalations(runId, "open") : [];
      if (open.length > 0) {
        reason = "blocked_on_escalation";
        message +=
          ` Blocked on open escalation(s): ${open.slice(0, 8).map((e) => `${e.id} (${e.title.slice(0, 120)})`).join("; ")}. ` +
          `Resolve or answer them, then resume_task ${blocked[0]!.id} continues the blocked task in place.`;
      } else {
        reason = "no_resumable_tasks";
      }
    }
    return {
      resumed: outcome === "resumed",
      resumedTaskIds: [],
      run,
      outcome,
      runStatus: run.status,
      message,
      ...(outcome === "already_active" ? { alreadyRunning: true } : {}),
      ...(reason ? { reason } : {}),
      ...extra,
    };
  }

  /** Delivers a caller's note to the run's Tier 1 orchestrator (pending resume note or live steering). */
  private async deliverNoteToRun(runId: string, note: string): Promise<{ delivered: boolean; delivery?: SteerTaskResult }> {
    const orch = this.db.listTasks(runId).find((t) => t.tier === 1 && t.profileId !== "director");
    return orch ? this.deliverNoteToTaskDetailed(orch.id, note) : { delivered: false };
  }

  private withRunNote(res: ResumeRunResult, { delivered, delivery }: { delivered: boolean; delivery?: SteerTaskResult }): ResumeRunResult {
    return {
      ...res,
      noteDelivered: delivered,
      ...(delivery ? { noteDelivery: delivery } : {}),
      message: `${res.message} ${delivered ? "Your note was delivered to the run's orchestrator." : "Your note could not be delivered: the run's orchestrator is not live."}`,
    };
  }

  private async resumeRunInternal(
    runId: string,
    opts: { trigger?: "manual" | "auto_timer" | "quota_recovered" | "user_timer"; userMessage?: string; note?: string } | undefined,
    snapshot: StopSnapshot,
  ): Promise<ResumeRunResult> {
    // Runners cancelled by an earlier stop are waited for, so they are never reported as already active. One deadline
    // bounds every wait. Cancelled child tasks are never resumed here, so their teardowns are not waited for (unless
    // still allocating, which counts as live); a resumable task still winding down at the deadline is reported instead.
    const deadline = Date.now() + this.cancelUnwindTimeoutMs;
    await this.awaitCancelUnwind(this.db.listTasks(runId).map((t) => t.id), deadline);
    const orchId = this.db.listTasks(runId).find((t) => t.tier === 1 && t.profileId !== "director")?.id;
    const windingDown: Array<{ taskId: string; error: string }> = [];
    await Promise.all(
      this.db
        .listTasks(runId)
        .filter((t) => t.status !== "completed" && (t.status !== "cancelled" || t.id === orchId || this.allocatingWorktrees.has(t.id)))
        .map(async (t) => {
          if (!(await this.awaitTaskWindDown(t.id, deadline))) windingDown.push({ taskId: t.id, error: this.windDownPendingMessage(t.id) });
        }),
    );
    const windingDownIds = new Set(windingDown.map((f) => f.taskId));
    const run = this.db.getRun(runId)!;
    const tasks = this.db.listTasks(run.id);
    // An explicit stop is checked before the quota pause: a stopped run resumes its orchestrator in place below.
    if (run.status !== "stopped" && (run.status === "paused_quota" || tasks.some((t) => t.status === "paused_quota"))) {
      const note = (opts?.note ?? opts?.userMessage)?.trim() || undefined;
      // Fenced by the request-time snapshot: a stop or cancel issued after this resume was requested wins.
      const res = this.resumeRunFromQuotaInternal(run.id, { trigger: opts?.trigger, userMessage: note }, snapshot);
      const base = res.fenced ?? (res.resumed
        ? `Run ${run.id} resumed from its quota pause: ${res.resumedTaskIds.join(", ")}.`
        : `Run ${run.id} had no quota-paused tasks left to resume.`);
      const noteText = note ? (res.resumed ? " Your note was passed to the resumed tasks." : " Your note was not delivered.") : "";
      return {
        ...this.runResult(run.id, res.resumed ? "resumed" : "nothing_to_resume", base + noteText,
          note ? { noteDelivered: res.resumed } : {}),
        resumed: res.resumed,
        resumedTaskIds: res.resumedTaskIds,
      };
    }

    const liveIds = tasks
      .filter((t) => t.profileId !== "director" && (this.isLiveExecution(t.id) || this.allocatingWorktrees.has(t.id) || LIVE_TASK_STATUSES.has(t.status)))
      .map((t) => t.id);
    const container = isDirectorTaskContainer(run.id);
    // A caller's note on a run that is already active is delivered to its live orchestrator, never dropped silently.
    const callerNote = (opts?.note ?? opts?.userMessage)?.trim() || undefined;
    const alreadyActive = async (message: string, extra: Partial<ResumeRunResult> = {}): Promise<ResumeRunResult> => {
      const res = this.runResult(run.id, "already_active", message, extra);
      if (callerNote && container) {
        return {
          ...res,
          noteDelivered: false,
          message: `${res.message} Your note was not delivered: a Director delegation container has no orchestrator to steer; steer a delegated task directly with steer_task.`,
        };
      }
      return callerNote ? this.withRunNote(res, await this.deliverNoteToRun(run.id, callerNote)) : res;
    };
    if (container) {
      if (liveIds.length > 0) {
        return alreadyActive(`Run ${run.id} already has live delegated tasks (${liveIds.join(", ")}); nothing was restarted.`, { activeTaskIds: liveIds });
      }
      return this.runResult(
        run.id,
        "nothing_to_resume",
        `Run ${run.id} is a Director delegation container, not an autonomous run. Nothing was restarted. Resume one task with resume_task <task-id>, or delegate again from the chat.`,
      );
    }

    // NEVER revive cancelled child workers or completed tasks on `resumeRun`! The Tier 1 orchestrator is resumed
    // in place (also when an explicit stop cancelled it), plus workers interrupted by a daemon restart.
    const orch = tasks.find((t) => t.tier === 1 && t.profileId !== "director");
    const RESUMABLE_ORCH = new Set(["failed", "blocked", "returned_for_rescope", "cancelled"]);
    const RESUMABLE_ORPHAN = new Set(["failed", "blocked", "returned_for_rescope"]);
    const candidates = tasks.filter((t) => {
      if (t.profileId === "director" || this.isLiveExecution(t.id) || this.allocatingWorktrees.has(t.id)) return false;
      if (windingDownIds.has(t.id)) return false;
      if (orch && t.id === orch.id) return RESUMABLE_ORCH.has(t.status);
      if (t.status === "cancelled" || t.status === "completed") return false;
      if (t.status === "failed" && t.summary === "Interrupted by daemon restart") return true;
      if (!orch && RESUMABLE_ORPHAN.has(t.status)) return true;
      return false;
    });
    const cancelledChildren = tasks.filter((t) => t.status === "cancelled" && t.id !== orch?.id && t.profileId !== "director");
    if (candidates.length === 0 && windingDown.length > 0) {
      return this.runResult(run.id, "nothing_to_resume", `Run ${run.id} was not resumed: ${windingDown.map((f) => f.error).join(" ")}`, {
        failed: windingDown,
      });
    }
    if (candidates.length === 0) {
      if (liveIds.length > 0) {
        return alreadyActive(`Run ${run.id} is already running (${liveIds.slice(0, 12).join(", ")}); nothing was restarted.`, { activeTaskIds: liveIds });
      }
      const eff = this.getEffectiveRun(run.id) ?? run;
      if (eff.status === "completed") {
        return this.runResult(run.id, "already_completed", `Run ${run.id} already completed; nothing to resume. Review the goal or amend it to continue.`);
      }
      const cause = this.describeRunCause(run.id);
      const next = cancelledChildren.length > 0
        ? `A run resume never revives cancelled tasks; resume a specific one with resume_task (e.g. ${cancelledChildren[0]!.id}) from the Director or its parent orchestrator, or delegate the work again.`
        : `Inspect with \`pi-harness tree ${run.id}\`.`;
      return this.runResult(
        run.id,
        "nothing_to_resume",
        `Run ${run.id} is ${eff.status} (${this.causeOrHint(run.id, cause)}) and has no resumable orchestrator or interrupted tasks; nothing was restarted. ${next}`,
        cause ? { cause } : {},
      );
    }

    let note = opts?.note ?? opts?.userMessage;
    const stopCause = this.describeRunCause(run.id);
    if (orch && orch.status === "cancelled" && candidates.some((t) => t.id === orch.id)) {
      const ids = cancelledChildren.slice(0, 12).map((t) => t.id);
      const header =
        `Run ${run.id} was stopped (${stopCause ?? "no stop reason recorded"}) and has now been resumed in place. ` +
        (ids.length > 0
          ? `Tasks cancelled by that stop were NOT restarted: ${ids.join(", ")}. Inspect the tree and call resume_task for the ones still needed; do not respawn work that is already merged.`
          : "No child tasks were restarted.");
      note = note ? `${header}\n${note}` : header;
    }
    const resumedTaskIds: string[] = [];
    const failed: Array<{ taskId: string; error: string }> = [...windingDown];
    let noteSteered = false;
    let noteDelivery: SteerTaskResult | undefined;
    for (const t of candidates) {
      try {
        const res = await this.resumeTaskInternal(t.id, note ? { note } : undefined, snapshot, deadline);
        if (res.outcome === "resumed") resumedTaskIds.push(res.task.id);
        if (res.noteDelivered) noteSteered = true;
        if (res.noteDelivery) noteDelivery = res.noteDelivery;
      } catch (err) {
        failed.push({ taskId: t.id, error: err instanceof Error ? err.message : String(err) });
      }
    }
    if (resumedTaskIds.length > 0) {
      this.db.emitEvent(run.id, null, "run.resumed", {
        runId: run.id,
        trigger: opts?.trigger ?? "manual",
        resumedTaskIds,
        note: note ?? null,
      });
    }
    if (resumedTaskIds.length === 0) {
      if (failed.length === 0) {
        const res = this.runResult(run.id, "already_active", `Run ${run.id} is already running; nothing was restarted.`);
        return callerNote ? this.withRunNote(res, { delivered: noteSteered, delivery: noteDelivery }) : res;
      }
      return this.runResult(run.id, "nothing_to_resume", `Run ${run.id} was not resumed: ${failed.map((f) => f.error).join(" ")}`, {
        failed,
        ...(callerNote ? { noteDelivered: noteSteered } : {}),
      });
    }
    const notRestarted = cancelledChildren.length > 0 ? ` Cancelled tasks were not restarted: ${cancelledChildren.slice(0, 12).map((t) => t.id).join(", ")}.` : "";
    return {
      ...this.runResult(run.id, "resumed", `Run ${run.id} resumed in place: ${resumedTaskIds.join(", ")}.${notRestarted}`, {
        ...(failed.length > 0 ? { failed } : {}),
        ...(callerNote ? { noteDelivered: true } : {}),
      }),
      resumedTaskIds,
    };
  }

  private async runTaskWithRollovers(taskId: string, exec: ActiveTaskExecution): Promise<void> {
    while (!exec.abort.signal.aborted) {
      exec.promptBuilt = false;
      exec.handle = undefined;
      exec.steerChain = undefined;
      exec.phase = "starting";
      const task = this.db.getTask(taskId);
      if (!task) return;
      if (["completed", "cancelled", "failed", "returned_for_rescope", "paused_quota"].includes(task.status)) return;

      const run = this.db.getRun(task.runId);
      if (!run) return;

      let wt = task.worktreeId ? this.db.getWorktree(task.worktreeId) : null;
      const toolPolicy = toolPolicyForTask(task.tier, task.kind, task.readOnly);
      // Deferred worktree & write-lease activation for Tier 2 Workers that started `pending` on `depends_on`
      if (task.tier === 2 && toolPolicy === "read_write_workspace") {
        if (!this.db.listActiveWriteScopes(run.id, task.id).length && this.db.listAssignedWriteScopes(run.id, task.id).length) {
          // An overlapping lease still held by another task: wait for its release instead of failing the activation.
          let waitingOn: string | null = null;
          for (;;) {
            try {
              this.db.reacquireWriteLeasesForTask(run.id, task.id);
              break;
            } catch (err) {
              if (!(err instanceof WriteScopeConflictError)) throw err;
              const holder = err.holders[0]?.taskId ?? "another task";
              if (holder !== waitingOn) {
                waitingOn = holder;
                this.db.updateTask(task.id, { summary: `Waiting for write lease held by ${holder}` });
              }
              await this.waitForWriteLease(run.repoRoot, exec.abort.signal);
              if (exec.abort.signal.aborted || this.db.getTask(task.id)?.status !== task.status) return;
            }
          }
          if (waitingOn) this.db.updateTask(task.id, { summary: null });
        }
        if (!wt || wt.runId !== task.runId) {
          const wtMgr = this.getWorktreeManager(run.repoRoot);
          // If worktree concurrency cap is currently full across runs, wait cleanly in strict queue order instead of failing!
          while (!wt && wtMgr.isGitRepo() && !exec.abort.signal.aborted) {
            let qEntry = this.db.admissionQueue.get("worker", task.id);
            if (!qEntry) {
              qEntry = this.db.admissionQueue.enqueue({
                lane: "worker",
                repoRoot: run.repoRoot,
                targetId: task.id,
                note: task.title,
              });
            }
            if (!this.db.admissionQueue.isFrontOrForced("worker", run.repoRoot, task.id)) {
              await this.waitForWorktreeSlot(run.repoRoot, exec.abort.signal);
              continue;
            }
            this.allocatingWorktrees.add(task.id);
            try {
              const force = qEntry.forceStart;
              wt = await this.runWorktreeOp(task.id, () => wtMgr.createForWorker(run.id, task.id, undefined, { force }));
              this.db.admissionQueue.remove("worker", task.id);
            } catch (err) {
              const msg = err instanceof Error ? err.message : String(err);
              if (!(err instanceof WorktreeConcurrencyLimitError) && !msg.includes("concurrency cap reached")) {
                this.db.admissionQueue.remove("worker", task.id);
                throw err;
              }
              this.allocatingWorktrees.delete(task.id);
              await this.waitForWorktreeSlot(run.repoRoot, exec.abort.signal);
              continue;
            } finally {
              this.allocatingWorktrees.delete(task.id);
            }
          }
          if (exec.abort.signal.aborted) {
            this.db.admissionQueue.remove("worker", task.id);
            return;
          }
        }
      }
      if (toolPolicy === "read_write_workspace" && this.getWorktreeManager(run.repoRoot).isGitRepo() && (!wt || wt.runId !== task.runId)) {
        throw new Error("A writable agent requires its assigned worktree.");
      }
      const cwd = wt?.path ?? run.repoRoot;
      const { profile, model, wireModel } = await this.taskRoute(task, cwd);
      if (exec.abort.signal.aborted) return;
      // Re-read after the route await so an edit made meanwhile (e.g. a mid-launch goal amendment) is in the
      // runner's initial assignment rather than lost behind a steer that has no runner to reach yet.
      const latestTask = this.db.getTask(task.id);
      if (!latestTask || ["completed", "cancelled", "failed", "returned_for_rescope", "paused_quota"].includes(latestTask.status)) return;
      task.model = model;

      this.db.updateTask(latestTask.id, { status: "running", model, profileId: profile.id });
      const hydratedTask: TaskRecord = { ...latestTask, status: "running", model, profileId: profile.id };

      const handoff = hydratedTask.handoffJson ? (JSON.parse(hydratedTask.handoffJson) as AnchoredHandoff) : null;
      const quotaResumeNote = this.taskResumeNotes.get(task.id) ?? null;
      if (quotaResumeNote) this.taskResumeNotes.delete(task.id);

      // Every write a runner callback or tool call makes is fenced to this execution and generation, so a runner
      // evicted after a cancel-unwind timeout (or superseded by a rollover or resume) never touches its successor.
      // Set before the prompt is built so a steer queued right after knows the generation it targets.
      const runnerGen = task.generation;
      exec.runnerGeneration = runnerGen;
      // Continuation reservations belong to one generation; queued steers are kept and open this one's first turn.
      this.steerContinuations.delete(task.id);
      this.steerReservedBatches.delete(task.id);
      const systemPrompt = buildImmutableSystemPrompt(task.tier, task.kind, this.config.codeMode, this.config.codeExec);
      const writeScopes = task.tier === 2 ? this.db.listActiveWriteScopes(run.id, task.id)
        : task.tier === 3 && task.parentId ? this.db.listActiveWriteScopes(run.id, task.parentId) : [];
      const scopedKbHints = task.tier === 2 ? this.db.kb.getScopedAmbientIndex(run.repoRoot, writeScopes, 5) : [];

      const linkedGoal = this.resolveLinkedGoalForRun(run.id);
      // Fail closed: a goal evidence dir that fails validation (e.g. a symlinked segment) is never advertised or granted.
      const goalEvDir = linkedGoal ? ensureGoalEvidenceDir(run.repoRoot, linkedGoal.id) : null;
      const goalsRoot = join(run.repoRoot, ".harness", "goals");
      const artifactsRoot = resolve(run.repoRoot, this.config.worktree.artifactsDir ?? ".harness/artifacts");

      // Steering accepted while no session could take it opens this generation's first turn.
      const earlySteers = this.drainPendingSteers(task.id);
      const hydration = buildHydrationTurn({
        task: hydratedTask,
        handoff,
        tasksSnapshot: this.db.listTasks(run.id),
        artifacts: task.tier === 1 ? this.db.listArtifacts(run.id, undefined, 30, "dossier") : this.db.listArtifacts(run.id, task.id, 30),
        openEscalations: this.db.listEscalations(run.id, "open"),
        blackboard: this.db.blackboardGet(run.id),
        scopedKbHints,
        openPeerRequests: task.tier === 1 ? this.db.listOpenPeerExchangesForRun(run.id) : [],
        answeredMessages: this.db.listAnsweredMessagesFrom(task.id),
        writeScopes,
        worktreePath: wt?.path ?? null,
        worktreeBranch: wt?.branch ?? null,
        quotaResumeNote,
        goalEvidenceDir: goalEvDir ?? undefined,
      });
      exec.promptBuilt = true;
      // Steers queued for an earlier iteration of this execution are already in the task row this prompt was built from.
      for (const item of exec.pendingSteers?.splice(0) ?? []) item.onOutcome?.(false, "rolled_over");
      const initialPrompt = earlySteers.length > 0
        ? `${hydration}\n\n[Steering received before this session started]\n${earlySteers.join("\n\n")}`
        : hydration;

      const rolloverThreshold =
        task.tier === 1
          ? this.config.rollover.orchestratorThreshold
          : task.tier === 2
            ? this.config.rollover.workerThreshold
            : undefined;

      const bridgeExt = import.meta.url.endsWith(".js") ? "mcp-bridge.js" : "mcp-bridge.ts";
      const mcpBridgeEntrypoint = join(dirname(fileURLToPath(import.meta.url)), "..", "runners", bridgeExt);
      const catalogEntry = this.catalog.loadCached()?.models.find((m) => m.id === model);
      const maxGenerations = this.config.rollover.maxGenerations ?? 12;

      let genToolEvents = 0;
      const isCurrent = () => this.isCurrentExecution(task.id, exec, runnerGen);
      const generation = new AbortController();
      exec.generationSignal = AbortSignal.any([generation.signal, exec.abort.signal]);
      const cost = recordCostInvocation(this.db, {
        runId: run.id,
        taskId: task.id,
        activity: "task",
        profile,
        model,
        wireModel,
      });
      const runnerReq: RunAgentRequest = {
        runId: run.id,
        taskId: task.id,
        tier: task.tier,
        kind: task.kind,
        cwd,
        toolPolicy,
        systemPrompt,
        initialPrompt,
        // Only the runner sees the profile's wire ID; task rows, events and quota keep the canonical ID.
        model: wireModel,
        effort: task.effort,
        maxTurns: task.maxTurns,
        rolloverThreshold,
        contextWindow: catalogEntry?.contextK ? catalogEntry.contextK * 1000 : undefined,
        quotaRetryTimeoutMs: this.config.quotaRetry.nativeRetryTimeoutMs,
        tools: this.toolExecutor.tools(task.tier),
        executeTool: (name, args) => {
          try {
            this.assertCurrentToolGeneration(task.id, runnerGen);
          } catch (err) {
            return Promise.reject(err);
          }
          return this.toolExecutor.execute(run.id, task.id, name, args);
        },
        taskGeneration: runnerGen,
        daemonSocketPath: this.config.socketPath,
        mcpBridgeEntrypoint,
        externalMcpServers: resolveExternalMcpServers(this.config.stateDir, this.config.externalMcpServers),
        extraPathDirs: nativeToolsPathDirs(this.config.stateDir),
        extraReadRoots: [...new Set([
          goalsRoot,
          join(run.repoRoot, ".harness", "artifacts"),
          artifactsRoot,
          ...(this.config.worktree.extraReadRoots ?? []),
        ])],
        // A goal-linked run may write only its own verified goal evidence directory (plus operator roots), never
        // every goal's and never an unverified path.
        extraWriteRoots: linkedGoal
          ? toolPolicy === "read_write_workspace"
            ? [...(goalEvDir ? [goalEvDir] : []), ...(this.config.worktree.extraWriteRoots ?? [])]
            : undefined
          : [goalsRoot, ...(this.config.worktree.extraWriteRoots ?? [])],
        extraEnv: linkedGoal
          ? {
              ...(goalEvDir ? { HARNESS_GOAL_EVIDENCE_DIR: goalEvDir } : {}),
              HARNESS_GOAL_ID: linkedGoal.id,
              HARNESS_GOALS_DIR: goalsRoot,
              HARNESS_ARTIFACTS_DIR: artifactsRoot,
            }
          : undefined,
        allowShellInReadOnly: task.kind === "verify",
        isTaskSettled: () => {
          const t = this.db.getTask(task.id);
          return !t || t.generation !== task.generation ||
            ["completed", "returned_for_rescope", "blocked", "cancelled", "failed"].includes(t.status);
        },
        missingReturnWrapUpPrompt: task.tier !== 1 ? buildMissingReturnWrapUpPrompt(task.tier, task.kind) : undefined,
        drainPendingSteers: () => (isCurrent() ? this.drainPendingSteers(task.id) : []),
        signal: exec.generationSignal,
        onHandle: (h) => {
          if (!isCurrent()) return;
          exec.handle = h;
          // Queued steers go onto the chain synchronously, so any live steer issued after the handle appears waits behind them.
          for (const item of exec.pendingSteers?.splice(0) ?? []) {
            exec.steerChain = (exec.steerChain ?? Promise.resolve()).then(async () => {
              if (!isCurrent() || item.generation !== runnerGen || exec.handle !== h) {
                item.onOutcome?.(false, "generation_superseded");
                return;
              }
              const accepted = await h.steer(item.message).catch(() => false);
              if (!isCurrent() || item.generation !== runnerGen || exec.handle !== h || exec.runnerGeneration !== runnerGen) {
                item.onOutcome?.(false, "generation_superseded");
                return;
              }
              this.db.emitEvent(run.id, task.id, "agent.steered", { message: item.message, accepted });
              item.onOutcome?.(accepted, accepted ? "delivered" : "steer_rejected");
            }).catch(() => {});
          }
        },
        callbacks: {
          // Cost is still recorded on this invocation's own record: the spend happened even for a stale runner.
          onSessionId: (sessionId) => {
            cost.onSessionId(sessionId);
            if (!isCurrent()) return;
            this.db.updateTask(task.id, { sessionId });
          },
          onTextDelta: (text) => {
            if (!isCurrent()) return;
            this.db.emitEvent(run.id, task.id, "agent.text_delta", { text });
          },
          onThoughtDelta: (text) => {
            if (!isCurrent()) return;
            this.db.emitEvent(run.id, task.id, "agent.thought_delta", { text });
          },
          onUsage: (usage) => {
            cost.onUsage(usage);
            if (!isCurrent()) return;
            this.db.updateTask(task.id, { contextUsed: usage.used, contextSize: usage.size });
            this.db.emitEvent(run.id, task.id, "agent.usage", usage);
          },
          onTurn: (turnsUsed) => {
            if (!isCurrent()) return;
            this.db.updateTask(task.id, { turnsUsed });
          },
          onArtifact: (art) => {
            if (!isCurrent()) return;
            this.db.recordArtifact({ runId: run.id, taskId: task.id, ...art });
          },
          onPassiveQuota: (sig) => {
            if (!isCurrent()) return;
            this.quota.ingestPassiveSignal(sig);
            this.db.emitEvent(run.id, task.id, "quota.passive_signal", sig);
          },
          onQuotaRetryState: (ev) => {
            if (!isCurrent()) return;
            this.db.emitEvent(run.id, task.id, ev.state === "retrying" ? "quota.agent_retrying" : "quota.agent_recovered", {
              ...ev,
              profileId: task.profileId,
              model: task.model,
              title: task.title,
            });
          },
          onToolEvent: (ev) => {
            if (!isCurrent()) return;
            genToolEvents += 1;
            this.db.emitEvent(run.id, task.id, "agent.tool", ev);
          },
        },
      };

      try {
        const runner = this.customRunnerFactory
          ? this.customRunnerFactory(profile)
          : profile.kind === "acp"
            ? new AcpRunner(profile)
            : new PiRunner(profile);

        exec.phase = "running";
        const res = await runner.run(runnerReq).finally(() => {
          exec.phase = "settling";
        });
        const latest = this.db.getTask(task.id)!;
        if (exec.abort.signal.aborted || latest.status === "cancelled") return;

        // Honor terminal/settled status from `return_task` or `report_issue` BEFORE rollover/maxTurns/quota
        // so a task that already completed on its final/threshold turn is NEVER reopened into generation N+1!
        if (["completed", "returned_for_rescope", "blocked", "cancelled"].includes(latest.status)) {
          this.pendingRollovers.delete(task.id);
          this.runConsecutiveQuotaPauses.delete(run.id);
          this.taskAutoRolloverProgress.delete(task.id);
          if (isReadOnlyKind(latest.kind) && latest.status === "completed") {
            try {
              validateTaskHandoff(latest.kind, JSON.parse(latest.resultJson ?? "{}"));
            } catch {
              this.db.updateTask(task.id, { status: "failed", resultJson: null, summary: "Task ended without a valid sourced handoff." });
            }
          }
          if ((latest.tier === 1 || latest.tier === 2) && latest.status === "completed") {
            const hasPausedChildren = this.db
              .listTasks(run.id, latest.tier === 2 ? { parentId: latest.id } : undefined)
              .some((t) => t.id !== latest.id && t.status === "paused_quota");
            if (hasPausedChildren) {
              this.db.updateTask(task.id, {
                status: "paused_quota",
                summary: "Waiting for quota-paused child task(s) to resume",
              });
            }
          }
          return;
        }

        // Clean rollover requested either via explicit `request_rollover` or the context threshold.
        // Turn counts never force a rollover: maxTurns is only an advisory reminder inside the runner.
        const explicitRollover = this.pendingRollovers.get(task.id);
        if (explicitRollover || res.stopReason === "rollover_requested") {
          this.pendingRollovers.delete(task.id);
          const nextGen = latest.generation + 1;
          // A stop or cancel during the awaits below wins: the stale continuation writes nothing and launches nothing.
          const fence = this.rolloverFence(latest);
          // Fail fast after 2 consecutive zero-progress automatic context-threshold rollovers
          if (!explicitRollover && res.stopReason === "rollover_requested") {
            const fp = await this.computeAutoRolloverFingerprint(run.id, task.id, wt?.path ?? null);
            if (this.rolloverFenceBroken(exec, task.id, fence)) return;
            const prev = this.taskAutoRolloverProgress.get(task.id);
            const streak = prev && prev.fp === fp ? prev.streak + 1 : 1;
            this.taskAutoRolloverProgress.set(task.id, { fp, streak });
            if (streak >= 2) {
              this.db.updateTask(task.id, {
                status: "failed",
                summary: `Aborted after ${streak} consecutive zero-progress automatic rollovers (no commits, worktree edits, artifacts, or child tasks).`,
              });
              this.db.createEscalation({
                runId: run.id,
                fromTaskId: task.id,
                toTaskId: task.parentId,
                severity: task.tier === 1 ? "P0" : "P1",
                category: "budget_exhausted",
                title: `Task ${task.id} looped ${streak}x on auto-rollover with zero progress`,
                evidencePaths: [],
                blocking: true,
              });
              return;
            }
          } else {
            this.taskAutoRolloverProgress.delete(task.id);
          }
          if (nextGen > maxGenerations) {
            this.db.updateTask(task.id, {
              status: "failed",
              summary: `Exceeded max rollover generations (${maxGenerations}); escalating to prevent infinite rollover loop.`,
            });
            this.db.createEscalation({
              runId: run.id,
              fromTaskId: task.id,
              toTaskId: task.parentId,
              severity: task.tier === 1 ? "P0" : "P1",
              category: "budget_exhausted",
              title: `Task ${task.id} hit max rollover generations (${maxGenerations})`,
              evidencePaths: [],
              blocking: true,
            });
            return;
          }
          const nextHandoff = await this.buildCleanRolloverHandoff({
            runId: run.id,
            task: latest,
            priorHandoff: handoff,
            explicitHandoff: explicitRollover?.handoff ?? null,
            worktreePath: wt?.path ?? null,
            result: res,
          });
          if (this.rolloverFenceBroken(exec, task.id, fence)) return;
          this.db.updateTask(task.id, {
            status: "ready",
            generation: nextGen,
            contextUsed: 0,
            sessionId: null,
            profileId: explicitRollover?.target?.profileId ?? latest.profileId,
            model: explicitRollover?.target?.model ?? latest.model,
            effort: explicitRollover?.target?.effort ?? latest.effort,
            handoffJson: JSON.stringify(nextHandoff),
          });
          this.db.emitEvent(run.id, task.id, "agent.rollover", {
            fromGen: latest.generation,
            toGen: nextGen,
            reason: explicitRollover ? "explicit_handoff" : "context_threshold",
          });
          continue; // Immediately boot generation N+1 cleanly!
        }

        // Agent stopped early due to quota / rate-limit timeout: preserve worktree, WIP & handoff, do NOT fail!
        if (res.stopReason === "quota_paused") {
          const qp = res.quotaPause;
          const reason = qp?.reason ?? "Quota / rate limit reached";
          const fence = this.rolloverFence(latest);
          const pauseHandoff = await this.buildCleanRolloverHandoff({
            runId: run.id,
            task: latest,
            priorHandoff: handoff,
            explicitHandoff: null,
            worktreePath: wt?.path ?? null,
            result: res,
          });
          if (this.rolloverFenceBroken(exec, task.id, fence)) return;
          this.taskQuotaHints.set(task.id, {
            wireModel,
            profileId: latest.profileId,
            model: latest.model,
            reason,
            resetAt: qp?.resetAt ?? null,
            pausedAt: Date.now(),
          });
          this.db.updateTask(task.id, {
            status: "paused_quota",
            summary: `Paused (quota): ${reason.slice(0, 240)}`,
            handoffJson: JSON.stringify(pauseHandoff),
          });
          this.db.emitEvent(run.id, task.id, "quota.agent_paused", {
            runId: run.id,
            taskId: task.id,
            tier: latest.tier,
            title: latest.title,
            profileId: latest.profileId,
            model: latest.model,
            reason,
            resetAt: qp?.resetAt ?? null,
            retryCount: qp?.retryCount ?? 1,
            watchdogTimedOut: Boolean(qp?.watchdogTimedOut),
          });
          return;
        }

        if (exec.abort.signal.aborted) {
          const why = exec.abort.signal.reason instanceof Error ? exec.abort.signal.reason.message : "";
          this.db.updateTask(task.id, { status: "cancelled", summary: why ? `Cancelled: ${why}` : "Cancelled" });
          return;
        }
        if (res.stopReason === "cancelled") {
          // Nobody cancelled this task (the harness abort was not signalled): the runner ended the session itself.
          const failures = res.toolFailures?.length ? ` Last tool failure: ${res.toolFailures[res.toolFailures.length - 1]!.slice(0, 200)}` : "";
          this.db.updateTask(task.id, {
            status: "failed",
            summary: `Runner ended the session as cancelled without a harness cancellation.${failures}`,
          });
          return;
        }

        // If Tier 1 or Tier 2 exited while child tasks are paused on quota, pause parent too instead of closing prematurely!
        if (latest.tier === 1 || latest.tier === 2) {
          const hasPausedChildren = this.db
            .listTasks(run.id, latest.tier === 2 ? { parentId: latest.id } : undefined)
            .some((t) => t.id !== latest.id && t.status === "paused_quota");
          if (hasPausedChildren) {
            this.db.updateTask(task.id, {
              status: "paused_quota",
              summary: "Waiting for quota-paused child task(s) to resume",
            });
            return;
          }
        }

        if (res.stopReason === "error") {
          const detail = res.text.trim().slice(-240);
          this.db.updateTask(task.id, {
            status: "failed",
            summary: detail ? `Task runner reported an error: ${detail}` : "Task runner reported an error.",
          });
          return;
        }
        if (isReadOnlyKind(latest.kind)) {
          await this.settleWithoutReturn(exec, latest, run, wt, res, "failed", "Task ended without a valid sourced handoff.");
          return;
        }

        // Guard against hallucinated XML tool-call prose exits on Tier 1 Orchestrators (e.g. `<invoke name="tool_search">`)
        if (
          latest.tier === 1 &&
          genToolEvents === 0 &&
          this.db.listTasks(run.id, { parentId: latest.id }).length === 0 &&
          /<(?:invoke|tool_call|function_calls)\b/i.test(res.text)
        ) {
          this.db.updateTask(task.id, {
            status: "failed",
            summary: "Orchestrator exited with unexecuted tool-call XML prose instead of invoking tools.",
          });
          return;
        }

        // Enforce settlement safety on natural exit for Tier 2 Workers (verify clean AND HEAD contained in targetBranch)
        if (latest.tier === 2 && latest.worktreeId) {
          const latestWt = this.db.getWorktree(latest.worktreeId);
          if (latestWt && !latestWt.cleanedAt) {
            const wtMgr = this.getWorktreeManager(run.repoRoot);
            const dirty = await wtMgr.isDirty(latestWt.path).catch(() => false);
            const wtHead = await runGit(latestWt.path, ["rev-parse", "HEAD"]).then((r) => r.stdout).catch(() => null);
            const hasUnmergedCommits =
              wtHead != null &&
              wtHead !== latestWt.baseSha &&
              !(await runGit(run.repoRoot, ["merge-base", "--is-ancestor", wtHead, run.baseBranch]).then(
                () => true,
                () => false,
              ));
            if (dirty || hasUnmergedCommits || latestWt.status === "bounced" || latestWt.status === "queued") {
              await this.settleWithoutReturn(
                exec, latest, run, latestWt, res, "failed",
                `Worker exited with unmerged/dirty worktree (status: ${latestWt.status}, dirty: ${dirty}, unmergedCommits: ${hasUnmergedCommits}).`,
              );
              return;
            }
            if (latestWt.status === "merged") {
              void wtMgr.teardown(latestWt.id, { archiveBeforeRemove: false, finalStatus: "merged" }).catch(() => {});
            }
          }
        }

        this.runConsecutiveQuotaPauses.delete(run.id);
        this.taskAutoRolloverProgress.delete(task.id);
        // Agent finished its turn naturally without calling `return_task`
        if (latest.tier !== 1) {
          await this.settleWithoutReturn(exec, latest, run, wt, res, "completed", res.text.slice(0, 1000) || "Completed");
          return;
        }
        const finalReply = res.text.trim();
        if (finalReply.length > SUMMARY_LIMIT) {
          // Tier 1's natural reply is its handoff: keep the full text readable from a dossier, bound the summary.
          let spill: { summary: string; overflowPath?: string } | null = null;
          let spillError = "";
          try {
            spill = spillSummaryOverflow(this.db, run, task.id, finalReply);
          } catch (err) {
            spillError = err instanceof Error ? err.message : String(err);
          }
          if (spill) {
            const saved = spill;
            this.db.transaction(() => {
              if (saved.overflowPath) {
                this.db.recordArtifact({ runId: run.id, taskId: task.id, kind: "dossier", path: saved.overflowPath, summary: "Full task summary" });
              }
              this.db.updateTask(task.id, {
                status: "completed",
                summary: saved.summary,
                resultJson: JSON.stringify({
                  status: "completed",
                  text: res.text,
                  summary: saved.summary,
                  artifact_paths: saved.overflowPath ? [saved.overflowPath] : [],
                }),
              });
            });
            return;
          }
          // The warning also goes into resultJson.summary, which parent-facing handoffs prefer over the raw text.
          const warned = `${res.text.slice(0, 1000)} [Full summary could not be saved: ${spillError.slice(0, 200)}]`;
          this.db.updateTask(task.id, {
            status: "completed",
            summary: warned,
            resultJson: JSON.stringify({ status: "completed", text: res.text, summary: warned }),
          });
          return;
        }
        this.db.updateTask(task.id, {
          status: "completed",
          summary: res.text.slice(0, 1000) || "Completed",
          resultJson: JSON.stringify({ status: "completed", text: res.text }),
        });
        return;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (exec.abort.signal.aborted) return;
        // If an explicit clean rollover was scheduled right before the runner rejected on cancel, proceed with rollover!
        const explicitRollover = this.pendingRollovers.get(task.id);
        const latest = this.db.getTask(task.id);
        if (explicitRollover && latest && latest.generation + 1 <= maxGenerations) {
          this.pendingRollovers.delete(task.id);
          const nextGen = latest.generation + 1;
          const fence = this.rolloverFence(latest);
          const nextHandoff = await this.buildCleanRolloverHandoff({
            runId: run.id,
            task: latest,
            priorHandoff: handoff,
            explicitHandoff: explicitRollover.handoff,
            worktreePath: wt?.path ?? null,
            result: { text: "", stopReason: "rollover_requested" },
          });
          if (this.rolloverFenceBroken(exec, task.id, fence)) return;
          this.db.updateTask(task.id, {
            status: "ready",
            generation: nextGen,
            contextUsed: 0,
            sessionId: null,
            profileId: explicitRollover.target?.profileId ?? latest.profileId,
            model: explicitRollover.target?.model ?? latest.model,
            effort: explicitRollover.target?.effort ?? latest.effort,
            handoffJson: JSON.stringify(nextHandoff),
          });
          this.db.emitEvent(run.id, task.id, "agent.rollover", {
            fromGen: latest.generation,
            toGen: nextGen,
            reason: "explicit_handoff",
          });
          continue;
        }
        this.pendingRollovers.delete(task.id);
        const flat = msg.replace(/\s+/g, " ").trim();
        const clipped = flat.length <= 240 ? flat : `${flat.slice(0, 150)} … ${flat.slice(-87)}`;
        this.db.updateTask(task.id, { status: "failed", summary: `Task runner failed before settlement: ${clipped}` });
        this.db.emitEvent(run.id, task.id, "agent.error", { error: msg });
        return;
      } finally {
        cost.finish();
        generation.abort(); // Outstanding MCP scripts must not survive their runner (including a rollover).
        exec.generationSignal = undefined;
        exec.handle = undefined;
        exec.steerChain = undefined;
        // A queued steer never crosses into the next generation: its prompt is rebuilt from the amended task row.
        if ((this.db.getTask(task.id)?.generation ?? runnerGen) > runnerGen && exec.pendingSteers?.length) {
          const stale = exec.pendingSteers.filter((item) => item.generation <= runnerGen);
          exec.pendingSteers = exec.pendingSteers.filter((item) => item.generation > runnerGen);
          for (const item of stale) item.onOutcome?.(false, "rolled_over");
        }
      }
    }
  }

  /**
   * Settles a non-Tier-1 task that ended naturally without return_task. Status and summary stay exactly as the
   * caller decided; resultJson gains a harness-synthesized, labelled fallback (git state, indexed artifacts, a bounded
   * excerpt of the final output). Read-only kinds keep their generic failed handoff and only get their output saved
   * as a dossier the parent can list with artifact_read({ task_id }). Writes nothing when the task was cancelled,
   * settled or resumed while the evidence was gathered.
   */
  private async settleWithoutReturn(
    exec: ActiveTaskExecution,
    task: TaskRecord,
    run: RunRecord,
    wt: WorktreeRecord | null,
    res: RunAgentResult,
    status: "completed" | "failed",
    baseSummary: string,
  ): Promise<void> {
    const gen = task.generation;
    const readOnly = isReadOnlyKind(task.kind);
    const git: { head?: string; branch?: string; committed?: string; uncommitted?: string; status?: string[] } = {};
    const wtPath = wt && !wt.cleanedAt && existsSync(wt.path) ? wt.path : null;
    if (wtPath) {
      const out = (args: string[]) => runGit(wtPath, args, 5_000).then((r) => r.stdout, () => null);
      const [head, branch, committed, uncommitted, statusShort] = await Promise.all([
        out(["rev-parse", "HEAD"]),
        out(["rev-parse", "--abbrev-ref", "HEAD"]),
        wt?.baseSha ? out(["diff", "--shortstat", `${wt.baseSha}..HEAD`]) : Promise.resolve(null),
        out(["diff", "--shortstat"]),
        out(["status", "--short"]),
      ]);
      if (head) git.head = head;
      if (branch) git.branch = branch;
      if (committed !== null) git.committed = committed || "no commits since base";
      if (uncommitted !== null) git.uncommitted = uncommitted || "none";
      if (statusShort !== null) git.status = statusShort.split("\n").filter(Boolean).slice(0, 40);
    }
    let artifactPaths: string[] = [];
    try {
      artifactPaths = this.db.listArtifacts(run.id, task.id, 50).map((a) => a.path).slice(0, 20);
    } catch {
      artifactPaths = [];
    }
    const text = res.text.trim();
    const EXCERPT = 1200;

    // Race gate (synchronous up to the transaction): someone else cancelled, settled or resumed this task meanwhile.
    const row = this.db.getTask(task.id);
    if (
      this.active.get(task.id) !== exec ||
      exec.abort.signal.aborted ||
      !row ||
      row.generation !== gen ||
      !["running", "waiting_children", "draining"].includes(row.status)
    ) {
      return;
    }
    // The full output is saved only past the gate, so a superseded generation never leaves stray files or rows.
    const saveOutput = readOnly ? text.length > 0 : text.length > EXCERPT;
    let outputPath: string | null = null;
    this.db.transaction(() => {
      if (saveOutput) {
        try {
          // A new, never-overwritten name: the task may have written its own artifact under any name.
          outputPath = writeUniqueTaskDossier(
            this.db, run, task.id, `output-gen${gen}`, ".md", text,
            readOnly ? "Unstructured task output" : "Full final output (no return_task)",
          ).path;
        } catch {
          outputPath = null;
        }
      }
      const paths = outputPath && !artifactPaths.includes(outputPath) ? [...artifactPaths, outputPath] : artifactPaths;
      const fallbackBase = { reason: "missing_return_task", wrap_up_attempted: Boolean(res.wrapUpAttempted) };
      if (readOnly) {
        this.db.updateTask(task.id, {
          status: "failed",
          summary: baseSummary,
          resultJson: JSON.stringify({ status: "failed", fallback: { ...fallbackBase, artifact_paths: paths } }),
        });
        return;
      }
      const header =
        `[Fallback handoff: task ended without return_task; wrap-up turn ${res.wrapUpAttempted ? "attempted" : "not available"}; ` +
        "unverified, synthesized by the harness]";
      const gitLine = wtPath
        ? `Git: ${git.branch ?? "?"} @ ${git.head?.slice(0, 12) ?? "?"}; committed since base: ${git.committed ?? "unknown"}; ` +
          `uncommitted: ${git.uncommitted ?? "unknown"}${git.status?.length ? ` (${git.status.length} changed path(s))` : ""}`
        : "Git: no worktree";
      const artLine = `Artifacts: ${paths.length} indexed${paths.length ? " (see artifact_paths)" : ""}`;
      const pointer = outputPath ? `\n[Full output: ${relative(run.repoRoot, outputPath)}]` : "";
      const fixed = `${header}\n${gitLine}\n${artLine}\nFinal worker output (unstructured excerpt):\n`;
      const budget = Math.max(0, Math.min(EXCERPT, SUMMARY_LIMIT - fixed.length - pointer.length));
      const excerpt = !text ? "(no output)" : text.length <= budget ? text : `…${text.slice(-(budget - 1))}`;
      const summary = `${fixed}${excerpt}${pointer}`.slice(0, SUMMARY_LIMIT);
      this.db.updateTask(task.id, {
        status,
        summary: baseSummary,
        resultJson: JSON.stringify({
          status,
          text: res.text,
          fallback: { ...fallbackBase, stop_reason: res.stopReason, summary, artifact_paths: paths, git },
        }),
      });
    });
  }

  /** Generation and stop/cancel epochs before a rollover or quota-pause handoff is awaited. */
  private rolloverFence(task: TaskRecord): { gen: number; snap: StopSnapshot } {
    return { gen: task.generation, snap: this.stopEpochSnapshot(task.runId, [task.id]) };
  }

  /** True when the task was cancelled, its run stopped, or its generation moved on while a handoff was built. */
  private rolloverFenceBroken(exec: ActiveTaskExecution, taskId: string, fence: { gen: number; snap: StopSnapshot }): boolean {
    if (exec.abort.signal.aborted) return true;
    const t = this.db.getTask(taskId);
    if (!t || t.status === "cancelled" || t.generation !== fence.gen) return true;
    if (!isDirectorTaskContainer(t.runId) && this.db.getRun(t.runId)?.status === "stopped") return true;
    return this.stopWonSince(t, fence.snap) !== null;
  }

  /**
   * Builds a lossless anchored handoff across rollovers: preserves prior invariants/decisions
   * across generations and enriches automatic context-threshold rollovers with deterministic
   * git worktree state, recent indexed artifacts, recent failures, and the predecessor's last output.
   */
  private async buildCleanRolloverHandoff(input: {
    runId: string;
    task: TaskRecord;
    priorHandoff: AnchoredHandoff | null;
    explicitHandoff: AnchoredHandoff | null;
    worktreePath: string | null;
    result: RunAgentResult;
  }): Promise<AnchoredHandoff> {
    const mergeUnique = (a: string[], b: string[]): string[] => {
      const seen = new Set<string>();
      const out: string[] = [];
      for (const item of [...a, ...b]) {
        const trimmed = item.trim();
        if (!trimmed || seen.has(trimmed)) continue;
        seen.add(trimmed);
        out.push(trimmed);
      }
      return out;
    };

    const mergeDecisions = (
      a: Array<{ decision: string; rationale: string }>,
      b: Array<{ decision: string; rationale: string }>,
    ): Array<{ decision: string; rationale: string }> => {
      const seen = new Set<string>();
      const out: Array<{ decision: string; rationale: string }> = [];
      for (const d of [...a, ...b]) {
        const key = d.decision.trim().toLowerCase();
        if (!key || seen.has(key)) continue;
        seen.add(key);
        out.push(d);
      }
      return out;
    };

    const priorInvariants = input.priorHandoff?.goalInvariants ?? [];
    const priorDecisions = input.priorHandoff?.architecturalDecisions ?? [];

    // Collect only accepted steers since the last rollover so stale/rejected steers never replay across generations
    const lastRolloverSeq =
      (
        this.db.db
          .prepare("SELECT MAX(seq) AS s FROM events WHERE run_id = ? AND task_id = ? AND type = 'agent.rollover'")
          .get(input.runId, input.task.id) as { s: number | null } | undefined
      )?.s ?? 0;
    const steeredRows = this.db.db
      .prepare(
        "SELECT payload_json FROM events WHERE run_id = ? AND task_id = ? AND type = 'agent.steered' AND seq > ? ORDER BY seq ASC LIMIT 6",
      )
      .all(input.runId, input.task.id, lastRolloverSeq) as Array<{ payload_json: string }>;
    const acceptedSteers: string[] = [];
    for (const row of steeredRows) {
      try {
        const p = JSON.parse(row.payload_json) as { message?: string; accepted?: boolean };
        if (p.accepted !== false && p.message?.trim()) {
          acceptedSteers.push(`Steered update: ${p.message.trim().slice(0, 400)}`);
        }
      } catch {
        // ignore malformed payload
      }
    }

    if (input.explicitHandoff) {
      return {
        goalInvariants: mergeUnique(priorInvariants, input.explicitHandoff.goalInvariants),
        architecturalDecisions: mergeDecisions(priorDecisions, input.explicitHandoff.architecturalDecisions),
        activeRisks: input.explicitHandoff.activeRisks,
        nextActions: mergeUnique(input.explicitHandoff.nextActions, acceptedSteers),
      };
    }

    const activeRisks: string[] = [
      input.result.stopReason === "error"
        ? `Generation ${input.task.generation} ended before completing: ${input.result.text.slice(0, 300)}`
        : input.result.stopReason === "quota_paused"
          ? `Generation ${input.task.generation} paused on a quota or rate limit.`
          : `Auto-rollover triggered at context threshold during generation ${input.task.generation} (no explicit request_rollover submitted).`,
    ];
    const nextActions: string[] = [];

    if (input.worktreePath) {
      const [statusOut, diffStatOut, logOut] = await Promise.all([
        runGit(input.worktreePath, ["status", "--short"]).then((r) => r.stdout).catch(() => ""),
        runGit(input.worktreePath, ["diff", "--stat"]).then((r) => r.stdout).catch(() => ""),
        runGit(input.worktreePath, ["log", "-n", "5", "--oneline"]).then((r) => r.stdout).catch(() => ""),
      ]);
      const modified = statusOut
        .split("\n")
        .map((l) => l.trim())
        .filter(Boolean)
        .slice(0, 15);
      if (modified.length > 0) {
        activeRisks.push(`Uncommitted worktree changes: ${modified.join(", ")}`);
        if (diffStatOut.trim()) {
          activeRisks.push(`Uncommitted diff stat: ${diffStatOut.trim().split("\n").slice(-5).join(" | ")}`);
        }
        nextActions.push("Inspect existing uncommitted changes in the worktree before making new edits; run verification tests.");
      }
      const commits = logOut
        .split("\n")
        .map((l) => l.trim())
        .filter(Boolean)
        .slice(0, 3);
      if (commits.length > 0) {
        nextActions.push(`Recent worktree commits: ${commits.join(" | ")}`);
      }
    }

    if (input.task.tier === 1 || input.task.tier === 2) {
      const children = this.db
        .listTasks(input.runId, input.task.tier === 2 ? { parentId: input.task.id } : undefined)
        .filter((t) => t.id !== input.task.id);
      const running = children.filter((t) => LIVE_TASK_STATUSES.has(t.status));
      const failed = children.filter((t) => t.status === "failed");
      const openEsc = this.db.listEscalations(input.runId, "open");
      if (running.length > 0) {
        nextActions.push(`Wait for ${running.length} active child task(s): ${running.map((t) => t.id).join(", ")}`);
      }
      if (failed.length > 0) {
        activeRisks.push(`Failed child task(s): ${failed.map((t) => `${t.id} (${t.title})`).join(", ")}`);
      }
      if (openEsc.length > 0) {
        activeRisks.push(`Open escalations requiring resolution: ${openEsc.map((e) => `${e.id}: ${e.title}`).join(" | ")}`);
      }
    }

    const recentArtifacts = this.db.listArtifacts(input.runId, input.task.id, 8);
    if (recentArtifacts.length > 0) {
      nextActions.push(
        `Recently touched artifacts: ${recentArtifacts.map((a) => `${a.kind}:${a.path}`).slice(0, 8).join(", ")}`,
      );
    }
    for (const s of acceptedSteers) {
      nextActions.push(s);
    }
    const openPeerMsgs = this.db.listOpenMessages(input.task.id);
    for (const m of openPeerMsgs.slice(0, 5)) {
      nextActions.push(`Unanswered peer question ${m.id} from ${m.fromTaskId}: <untrusted_peer_input>${m.question.slice(0, 260)}</untrusted_peer_input>`);
    }
    if (input.result.toolFailures?.length) {
      activeRisks.push(`Recent tool failures: ${input.result.toolFailures.slice(-3).join(" | ")}`);
    }
    const lastText = input.result.text.trim();
    if (lastText) {
      const excerpt = lastText.length > 600 ? `…${lastText.slice(-600)}` : lastText;
      nextActions.push(`Predecessor gen ${input.task.generation} final progress note: ${excerpt}`);
    }
    if (nextActions.length === 0) {
      nextActions.push("Resume from authoritative SQLite Task Tree, Blackboard, and Artifact Index.");
    }

    return {
      goalInvariants: mergeUnique(priorInvariants.length > 0 ? priorInvariants : [input.task.objective], []),
      architecturalDecisions: priorDecisions,
      activeRisks,
      nextActions,
    };
  }

  private async computeAutoRolloverFingerprint(runId: string, taskId: string, worktreePath: string | null): Promise<string> {
    let gitState = "no-wt";
    if (worktreePath && existsSync(worktreePath)) {
      const [head, status, diff] = await Promise.all([
        runGit(worktreePath, ["rev-parse", "HEAD"]).then((r) => r.stdout).catch(() => ""),
        runGit(worktreePath, ["status", "--porcelain"]).then((r) => r.stdout).catch(() => ""),
        runGit(worktreePath, ["diff"]).then((r) => r.stdout).catch(() => ""),
      ]);
      const diffHash = createHash("sha256").update(diff).digest("hex").slice(0, 16);
      gitState = `${head}|${status}|${diffHash}`;
    }
    const artCount = this.db.listArtifacts(runId, taskId, 200).length;
    const kids = this.db.listTasks(runId, { parentId: taskId });
    const kidSig = kids.map((k) => `${k.id}:${k.status}:${k.generation}:${k.updatedAt}`).join(",");
    const bbKeys = Object.keys(this.db.blackboardGet(runId)).sort().join(",");
    const openEsc = this.db.listEscalations(runId, "open").map((e) => e.id).join(",");
    return `${gitState}::art=${artCount}::kids=${kidSig}::bb=${bbKeys}::esc=${openEsc}`;
  }

  private waitForWorktreeSlot(repoRoot: string, signal: AbortSignal): Promise<void> {
    if (signal.aborted) return Promise.resolve();
    const targetRepo = canonicalPath(repoRoot);
    return new Promise<void>((resolve) => {
      let done = false;
      const finish = () => {
        if (done) return;
        done = true;
        unsubEvents();
        unsubQueue();
        clearTimeout(timer);
        signal.removeEventListener("abort", finish);
        resolve();
      };
      const unsubEvents = this.db.onEvent((ev) => {
        // A run turning terminal frees the slots of its non-live worktrees.
        const runEnded =
          ev.type === "run.status" &&
          ["completed", "failed", "stopped"].includes((ev.payload as { status?: string } | null)?.status ?? "");
        if (runEnded || ev.type === "worktree.removed" || ev.type === "merge.finished" || ev.type === "task.status") {
          const evRun = this.db.getRun(ev.runId);
          if (!evRun || canonicalPath(evRun.repoRoot) === targetRepo) finish();
        }
      });
      const unsubQueue = this.db.admissionQueue.onChange((lane) => {
        if (!lane || lane === "worker") finish();
      });
      const timer = setTimeout(finish, 10_000);
      timer.unref?.();
      signal.addEventListener("abort", finish, { once: true });
    });
  }

  /**
   * Emergency force-start for a queued worker (bypasses `maxConcurrentWorkers`) or a quota-paused
   * worker (clears passive quota exhaustion when `bypassQuota` is true).
   */
  async forceStartWorker(
    taskId: string,
    opts?: { bypassQuota?: boolean; reason?: string },
  ): Promise<{ task: TaskRecord; action: string; warning: string }> {
    const task = this.db.getTask(taskId);
    if (!task) throw new Error(`Unknown task ${taskId}`);
    const warning = "Force-start bypasses admission/quota protections; use sparingly for urgent work only.";

    if (task.status === "paused_quota") {
      if (!opts?.bypassQuota) {
        throw new Error(`Task ${taskId} is paused on quota; pass bypass_quota: true to force-start despite quota exhaustion.`);
      }
      this.quota.clearPassiveExhaustion(task.profileId);
      const resumed = await this.resumeTask(taskId, {
        forceStart: true,
        note: opts.reason ?? "Force-started bypassing quota pause",
      });
      return { task: resumed, action: "force_resumed_from_quota", warning };
    }

    if (task.status === "pending") {
      throw new Error(`Task ${taskId} is waiting on unmet depends_on prerequisites, not the concurrency queue.`);
    }

    const qEntry = this.db.admissionQueue.get("worker", taskId);
    if (qEntry) {
      this.db.admissionQueue.markForceStart("worker", taskId);
      if (!this.active.has(taskId) && task.status === "ready") {
        this.launchTaskLoop(taskId);
      }
      return { task: this.db.getTask(taskId)!, action: "force_admitted_from_queue", warning };
    }

    if (task.status === "ready") {
      const run = this.db.getRun(task.runId);
      if (run) {
        this.db.admissionQueue.enqueue({
          lane: "worker",
          repoRoot: run.repoRoot,
          targetId: taskId,
          position: "front",
          forceStart: true,
          note: opts?.reason ?? task.title,
        });
      }
      if (!this.active.has(taskId)) this.launchTaskLoop(taskId);
      return { task: this.db.getTask(taskId)!, action: "force_started_ready_task", warning };
    }

    throw new Error(`Task ${taskId} is in status '${task.status}' and cannot be force-started.`);
  }

  /**
   * Steers a live task. Returns false when steering was refused for a task that is not settled. Throws an
   * actionable error for settled tasks or stopped runs: steering never silently undoes a stop.
   */
  async steerTask(taskId: string, message: string): Promise<boolean> {
    const r = await this.steerTaskDetailed(taskId, message);
    if (r.code === "not_live" || r.code === "run_stopped") throw new Error(r.reason);
    return r.steered;
  }

  /**
   * Steers a task and reports exactly how the message will be delivered (native session, the task's current
   * wait_children, or its next turn) or why it was refused. The only await is the native steer; every queue and
   * cap decision after it is synchronous, so concurrent steers never strand a message.
   */
  async steerTaskDetailed(taskId: string, message: string): Promise<SteerTaskResult> {
    const task = this.db.getTask(taskId);
    if (!task) {
      return { steered: false, mode: "refused", taskId, runId: "", status: "failed", code: "not_found", reason: `Unknown task ${taskId}.` };
    }
    const resumeHint = (t: TaskRecord) =>
      t.status === "completed"
        ? "Read its result with wait_children, or delegate follow-up work as a new task."
        : `resume_task({ task_id: "${t.id}", note })`;
    const refuse = (code: NonNullable<SteerTaskResult["code"]>, reason: string, nextAction?: string): SteerTaskResult => {
      const t = this.db.getTask(taskId) ?? task;
      return { steered: false, mode: "refused", taskId, runId: t.runId, status: t.status, code, reason, ...(nextAction ? { nextAction } : {}) };
    };
    const settledStatuses = ["cancelled", "failed", "completed", "blocked", "returned_for_rescope"];
    const refuseSettledOrStopped = (): SteerTaskResult | null => {
      const t = this.db.getTask(taskId) ?? task;
      const run = this.db.getRun(t.runId);
      const runStopped = Boolean(run && run.status === "stopped" && !isDirectorTaskContainer(t.runId));
      const text = this.describeSteerRefusal(taskId) ??
        (settledStatuses.includes(t.status) || runStopped
          ? `Steering was not delivered: ${runStopped ? `run ${t.runId} is stopped` : `task ${t.id} is ${t.status}`} and its session is ending; steering only reaches a running task.`
          : null);
      // A cancelled runner still winding down on a non-terminal row is "aborting", not a settled task.
      const aborting = this.active.get(taskId)?.abort.signal.aborted === true && !settledStatuses.includes(t.status);
      return text ? refuse(runStopped ? "run_stopped" : aborting ? "aborting" : "not_live", text, resumeHint(t)) : null;
    };
    const refuseExec = (exec: ActiveTaskExecution | undefined): SteerTaskResult | null => {
      const t = this.db.getTask(taskId) ?? task;
      if (!exec || this.active.get(taskId) !== exec) {
        return refuse("no_session", `Steering was not delivered: task ${t.id} (${t.status}) has no live session.`, resumeHint(t));
      }
      if (exec.abort.signal.aborted) {
        return refuse("aborting", `Steering was not delivered: task ${t.id} is being cancelled.`, resumeHint(t));
      }
      if (exec.phase === "settling") {
        return refuse("ending", `Steering was not delivered: task ${t.id} already ended its session and is settling.`, resumeHint(t));
      }
      return null;
    };

    // Phase 0 (sync): refusals that need no session.
    const early = refuseSettledOrStopped() ?? refuseExec(this.active.get(taskId));
    if (early) return early;
    const exec = this.active.get(taskId)!;

    // Phase 1 (the only await): a task blocked in wait_children gets the message through that wait instead.
    let native = false;
    // The generation and handle the native steer targets: a completion for an older one is never accepted.
    const sentGeneration = exec.runnerGeneration;
    const sentHandle = exec.handle;
    if ((this.waitingChildrenTasks.get(taskId) ?? 0) === 0 && sentHandle) {
      try {
        native = await this.steerHandleChained(exec, sentHandle, message);
      } catch {
        native = false;
      }
    }

    // Phase 2 (sync, no await before return): re-read everything.
    const late = refuseExec(exec) ?? (
      exec.runnerGeneration !== sentGeneration || exec.handle !== sentHandle
        ? refuse("no_session", `Steering was not delivered: task ${taskId} moved to a new session while the steer was in flight.`, resumeHint(this.db.getTask(taskId) ?? task))
        : null
    ) ?? refuseSettledOrStopped();
    if (late) return late;
    const current = this.db.getTask(taskId)!;
    const accept = (mode: "native" | "queued_wait" | "queued_turn"): SteerTaskResult => {
      this.db.emitEvent(current.runId, taskId, "agent.steered", { message, accepted: true, mode });
      return { steered: true, mode, taskId, runId: current.runId, status: this.db.getTask(taskId)?.status ?? current.status };
    };
    if (native) return accept("native");
    const queue = this.pendingSteers.get(taskId) ?? [];
    const queued = queue.length;
    if (queued >= MAX_PENDING_STEERS) {
      return refuse("queue_full", `Steering was not delivered: task ${taskId} already has ${queued} undelivered steering messages queued.`,
        "Wait for the task to take the queued steering, then send again.");
    }
    if ((this.waitingChildrenTasks.get(taskId) ?? 0) > 0) {
      // The waiter's agent.steered listener ends its wait synchronously; costs no continuation.
      this.pendingSteers.set(taskId, [...queue, message]);
      return accept("queued_wait");
    }
    const used = this.steerContinuations.get(taskId) ?? 0;
    if (queued === 0 && used >= MAX_STEER_CONTINUATIONS) {
      return refuse("steer_cap",
        `Task already used its ${MAX_STEER_CONTINUATIONS} steering continuations and is not waiting on children; cancel_task then resume_task({ note }) to redirect it.`,
        `cancel_task({ task_id: "${taskId}" }) then resume_task({ task_id: "${taskId}", note })`);
    }
    this.pendingSteers.set(taskId, [...queue, message]);
    // A delivery is already owed when messages are queued, so batching onto it is free.
    if (queued === 0) {
      this.steerContinuations.set(taskId, used + 1);
      this.steerReservedBatches.add(taskId);
    }
    return accept("queued_turn");
  }

  /**
   * Takes every queued steering message of a task (oldest first); drains are never refused. A batch taken by a
   * wait_children (`viaWait`) used no continuation turn, so the continuation it reserved is released.
   */
  drainPendingSteers(taskId: string, opts?: { viaWait?: boolean }): string[] {
    const q = this.pendingSteers.get(taskId);
    if (!q?.length) return [];
    this.pendingSteers.delete(taskId);
    if (this.steerReservedBatches.delete(taskId) && opts?.viaWait) {
      const used = (this.steerContinuations.get(taskId) ?? 0) - 1;
      if (used > 0) this.steerContinuations.set(taskId, used);
      else this.steerContinuations.delete(taskId);
    }
    return q;
  }

  /**
   * Sends a steer to the execution's current handle. Every native steer goes through one chain, so a steer never
   * overtakes earlier ones (including queued-before-handle steers) still draining; false when the session moved on.
   */
  private steerHandleChained(exec: ActiveTaskExecution, handle: RunnerExecutionHandle, message: string): Promise<boolean> {
    const expectedGen = exec.runnerGeneration;
    const stale = () => exec.handle !== handle || exec.runnerGeneration !== expectedGen || exec.abort.signal.aborted;
    const step = (exec.steerChain ?? Promise.resolve()).then(async () => {
      if (stale()) return false;
      const accepted = await handle.steer(message);
      // A completion that arrives after the session rolled over or ended belongs to no live session.
      return accepted && !stale();
    });
    exec.steerChain = step.then(() => {}, () => {});
    return step;
  }

  /**
   * Delivers guidance to a live task wherever its execution is: before the initial prompt is built the updated task
   * row already carries it, while the runner spawns it is queued for the handle, and once live it is steered.
   * A queued steer is not delivered yet: `onOutcome` reports whether the handle accepted it or why it never reached one.
   */
  async deliverOrQueueLiveSteer(
    taskId: string,
    message: string,
    opts?: { onOutcome?: (accepted: boolean, reason: string) => void },
  ): Promise<{ delivered: boolean; mode: "initial_prompt" | "steer" | "queued_before_handle" | "none" }> {
    if (this.describeSteerRefusal(taskId) !== null) return { delivered: false, mode: "none" };
    const exec = this.active.get(taskId);
    if (!exec || exec.abort.signal.aborted) return { delivered: false, mode: "none" };
    if (exec.promptBuilt !== true) return { delivered: true, mode: "initial_prompt" };
    if (exec.handle) {
      const sentGeneration = exec.runnerGeneration;
      const sentHandle = exec.handle;
      const ok = await this.steerHandleChained(exec, sentHandle, message).catch(() => false);
      // A completion for an older session must not be recorded against (or wake) its successor.
      if (
        this.active.get(taskId) !== exec || exec.abort.signal.aborted || exec.phase === "settling" ||
        exec.runnerGeneration !== sentGeneration || exec.handle !== sentHandle
      ) {
        return { delivered: false, mode: "none" };
      }
      const task = this.db.getTask(taskId);
      if (task) this.db.emitEvent(task.runId, taskId, "agent.steered", { message, accepted: ok });
      return { delivered: ok, mode: ok ? "steer" : "none" };
    }
    (exec.pendingSteers ??= []).push({
      generation: exec.runnerGeneration ?? this.db.getTask(taskId)?.generation ?? 1,
      message,
      onOutcome: opts?.onOutcome,
    });
    return { delivered: false, mode: "queued_before_handle" };
  }

  /** Why steering cannot reach this task (settled, cancelled, quota-paused, or stopped run), or null when it may. */
  describeSteerRefusal(taskId: string): string | null {
    const task = this.db.getTask(taskId);
    if (!task || this.isLiveExecution(taskId)) return null;
    const run = this.db.getRun(task.runId);
    const container = isDirectorTaskContainer(task.runId);
    const runStopped = Boolean(run && run.status === "stopped" && !container);
    if (task.status === "paused_quota" && !runStopped) {
      const quota = this.getRunQuotaStatus(task.runId);
      const when = quota?.mode === "manual" || !quota
        ? `it resumes when the operator runs \`pi-harness resume ${task.runId}\` (or the Director calls resume_run)`
        : `it resumes automatically${quota.resumeAt ? ` at ${quota.resumeAt}` : " when quota recovers"}`;
      return `Steering was not delivered: task ${task.id} is paused on provider quota (${this.causeOrHint(task.runId, task.summary)}); ${when}. ` +
        "Nothing was changed; send the guidance again once it is running, or pass it as the note when resuming.";
    }
    const settled = ["cancelled", "failed", "completed", "blocked", "returned_for_rescope"].includes(task.status);
    // A cancelled runner still winding down counts as cancelled, never as live.
    const aborting = this.active.get(taskId)?.abort.signal.aborted === true;
    if (!settled && !runStopped && !aborting) return null;
    // A whole-run stop is the broader cause and is reported first.
    const cause = runStopped || !settled
      ? this.causeOrHint(task.runId, this.describeRunCause(task.runId) ?? task.summary)
      : this.causeOrHint(task.runId, task.summary);
    const state = runStopped ? `run ${task.runId} is stopped` : settled ? `task ${task.id} is ${task.status}` : `task ${task.id} is being cancelled`;
    let recovery: string;
    if (task.status === "completed") {
      recovery = "Its result is final; read it with wait_children, or delegate follow-up work as a new task.";
    } else if (runStopped && task.tier === 1) {
      recovery = `To continue the run use resume_run (Director) or \`pi-harness resume ${task.runId}\` (operator).`;
    } else if (runStopped) {
      recovery = `The run must be resumed first (Director resume_run or operator \`pi-harness resume ${task.runId}\`); that never revives cancelled tasks, so then resume this one with resume_task({ task_id: "${task.id}", note }).`;
    } else if (container || task.parentId === null || task.tier === 1) {
      recovery = `The Director can continue it with this guidance using resume_task({ task_id: "${task.id}", note }), or delegate again.`;
    } else {
      recovery = `Its parent orchestrator (or the Director) can continue it with this guidance using resume_task({ task_id: "${task.id}", note }), or delegate again.`;
    }
    return `Steering was not delivered: ${state} (${cause}); steering only reaches a running task and never undoes a stop. ${recovery}`;
  }

  async cancelTask(taskId: string, reason: string, opts?: { internalStopSweep?: boolean; bulk?: boolean }): Promise<void> {
    // Bumped even for an already-settled task, so a resume requested before this cancel never applies.
    this.bumpStopEpoch(taskId);
    // Only an explicit cancel (not a stopRun sweep) fences resumes that were waiting on that earlier stop.
    if (!opts?.internalStopSweep) this.explicitCancelEpoch.set(taskId, (this.explicitCancelEpoch.get(taskId) ?? 0) + 1);
    this.db.admissionQueue.remove("worker", taskId);
    const exec = this.active.get(taskId);
    if (exec) exec.abort.abort(new Error(reason));
    const before = this.db.getTask(taskId);
    const cancelled = exec?.handle ? exec.handle.cancel(reason).catch(() => {}) : Promise.resolve();
    // Queued on the task's worktree chain now (synchronously), after any in-flight allocation: a worktree allocated
    // mid-cancel is torn down once it exists, and a later resume waits for this teardown before touching the worktree.
    if (before?.tier === 2) {
      const teardown = this.runWorktreeOp(taskId, async () => {
        await cancelled;
        const task = this.db.getTask(taskId);
        // A resume that already started a newer generation owns the worktree now.
        if (!task?.worktreeId || task.generation !== before.generation || task.status === "merging") return;
        // NEVER tear down a parent's worktree when cancelling an inherited Tier 3 child (tier 2 only here), and
        // never while a merge is actively draining it.
        const run = this.db.getRun(task.runId);
        const wt = this.db.getWorktree(task.worktreeId);
        if (run && wt && !wt.cleanedAt && wt.status !== "merged" && wt.status !== "queued") {
          await this.getWorktreeManager(run.repoRoot)
            .teardown(wt.id, { archiveBeforeRemove: true, finalStatus: "archived" })
            .catch(() => {});
        }
      }).catch(() => {});
      // Not awaited here (cancel stays non-blocking); a resume waits for it in awaitCancelUnwind.
      this.taskTeardowns.set(taskId, teardown);
      void teardown.finally(() => {
        if (this.taskTeardowns.get(taskId) === teardown) this.taskTeardowns.delete(taskId);
      });
    }
    await cancelled;
    this.taskQuotaHints.delete(taskId);
    this.taskResumeNotes.delete(taskId);
    this.pendingRollovers.delete(taskId);
    const task = this.db.getTask(taskId);
    if (task && !["completed", "cancelled", "failed"].includes(task.status)) {
      this.db.updateTask(taskId, { status: "cancelled", summary: `Cancelled: ${reason}` });
      // No runner loop will exit for a queued or never-launched task, so settle its run here (a stop sweep does that itself).
      if (!exec && !opts?.bulk) this.afterTaskInactiveInRun(task.runId);
    }
  }

  /**
   * Explicit stop: cancels active and queued tasks and persists "stopped" with its reason. A Director
   * delegation container only has its children cancelled; its status is never poisoned to "stopped",
   * so a fresh request in the same chat can delegate again.
   */
  async stopRun(runId: string, reason = "Stopped by user"): Promise<void> {
    const pending = this.stoppingRuns.get(runId);
    this.bumpStopEpoch(runId);
    const container = isDirectorTaskContainer(runId);
    const body = (async () => {
      if (pending) await pending.catch(() => {});
      this.clearRunQuotaTimers(runId);
      this.runQuotaStates.delete(runId);
      const first = this.db.listTasks(runId);
      await Promise.all(first.map((t) => this.cancelTask(t.id, reason, { internalStopSweep: true, bulk: true })));
      if (!container) this.db.updateRunStatus(runId, "stopped", reason);
      // Second sweep: catches tasks that raced createTask while the first sweep was cancelling.
      const seen = new Set(first.map((t) => t.id));
      const raced = this.db.listTasks(runId).filter((t) => !seen.has(t.id));
      await Promise.all(raced.map((t) => this.cancelTask(t.id, reason, { internalStopSweep: true, bulk: true })));
      this.db.releaseWriteLeasesForRun(runId);
      const stoppedRun = this.db.getRun(runId);
      if (stoppedRun) this.onWriteLeaseReleased(stoppedRun.repoRoot);
      if (!container) {
        if (this.db.getRun(runId)?.status !== "stopped") this.db.updateRunStatus(runId, "stopped", reason);
      } else {
        this.reconcileRunStatus(runId);
      }
    })();
    this.stoppingRuns.set(runId, body);
    try {
      await body;
    } finally {
      if (this.stoppingRuns.get(runId) === body) this.stoppingRuns.delete(runId);
    }
  }

  async waitForChildren(
    parentId: string,
    ids: string[] | undefined,
    mode: "any" | "all",
    timeoutMs: number,
    signal = this.active.get(parentId)?.generationSignal,
  ): Promise<TaskRecord[]> {
    const res = await this.waitForChildrenEventDriven(parentId, ids, mode, timeoutMs, signal);
    return res.children;
  }

  /**
   * Event-driven child wait: subscribes directly to SQLite task/escalation events (zero polling)
   * and wakes immediately on settlement, escalation, or steering/amendment, with a configurable long
   * fallback interval that also checks whether any running child process died silently.
   */
  async waitForChildrenEventDriven(
    parentId: string,
    ids: string[] | undefined,
    mode: "any" | "all",
    fallbackIntervalMs = this.config.events.fallbackWakeupIntervalSec * 1000,
    signal = this.active.get(parentId)?.generationSignal,
    /** Further parents whose children join this wait (a Director's buckets in other workspaces); they never run. */
    extraParentIds: string[] = [],
  ): Promise<{
    children: TaskRecord[];
    wakeReason: "already_settled" | "event" | "escalation" | "steered" | "fallback_interval" | "child_inactive";
    inactiveTaskIds: string[];
  }> {
    signal?.throwIfAborted();
    const parent = this.db.getTask(parentId);
    if (!parent) throw new Error(`Unknown parent task ${parentId}`);
    const parents = [parent];
    for (const id of new Set(extraParentIds)) {
      if (id === parentId) continue;
      const extra = this.db.getTask(id);
      if (!extra) throw new Error(`Unknown parent task ${id}`);
      parents.push(extra);
    }
    const parentIds = new Set(parents.map((p) => p.id));
    const runIds = new Set(parents.map((p) => p.runId));

    const terminal = new Set(["completed", "failed", "cancelled", "returned_for_rescope", "blocked", "paused_quota"]);
    const deadline = Date.now() + Math.max(50, fallbackIntervalMs);

    const getTargets = () => {
      const kids = parents.flatMap((p) => this.db.listTasks(p.runId, { parentId: p.id }));
      return ids?.length ? kids.filter((k) => ids.includes(k.id)) : kids;
    };

    const isDone = (targets: TaskRecord[]) => {
      if (targets.length === 0) return true;
      return mode === "any" ? targets.some((t) => terminal.has(t.status)) : targets.every((t) => terminal.has(t.status));
    };

    const findInactiveLiveChildren = (targets: TaskRecord[]) =>
      targets
        .filter((t) => LIVE_TASK_STATUSES.has(t.status) && !this.active.has(t.id) && !this.allocatingWorktrees.has(t.id))
        .map((t) => t.id);

    let targets = getTargets();
    if (isDone(targets)) {
      return { children: targets, wakeReason: "already_settled", inactiveTaskIds: [] };
    }
    // Steering queued for this task is delivered by this wait at once.
    if (this.pendingSteers.get(parentId)?.length) {
      return { children: targets, wakeReason: "steered", inactiveTaskIds: [] };
    }

    if (parent.status === "running") this.db.updateTask(parentId, { status: "waiting_children" });
    this.waitingChildrenTasks.set(parentId, (this.waitingChildrenTasks.get(parentId) ?? 0) + 1);
    return new Promise((resolve, reject) => {
      let settled = false;
      const finish = (
        wakeReason: "event" | "escalation" | "steered" | "fallback_interval" | "child_inactive",
        inactiveTaskIds: string[] = [],
      ) => {
        if (settled) return;
        settled = true;
        // Synchronous, so a steer arriving after this wake no longer counts it as waiting.
        const waiting = (this.waitingChildrenTasks.get(parentId) ?? 1) - 1;
        if (waiting > 0) this.waitingChildrenTasks.set(parentId, waiting);
        else this.waitingChildrenTasks.delete(parentId);
        unsub();
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        if (parent.status === "running" && this.db.getTask(parentId)?.status === "waiting_children") {
          this.db.updateTask(parentId, { status: "running" });
        }
        if (signal?.aborted) reject(signal.reason);
        else resolve({ children: targets, wakeReason, inactiveTaskIds });
      };
      const onAbort = () => finish("event");
      const checkEvent = () => {
        targets = getTargets();
        if (isDone(targets)) finish("event");
      };
      const onFallbackTimer = () => {
        targets = getTargets();
        if (isDone(targets)) {
          finish("event");
          return;
        }
        const inactive = findInactiveLiveChildren(targets);
        finish(inactive.length > 0 ? "child_inactive" : "fallback_interval", inactive);
      };
      const unsub = this.db.onEvent((ev) => {
        if (!runIds.has(ev.runId)) return;
        if (ev.type === "escalation.created") {
          const p = ev.payload as { fromTaskId?: string; severity?: string } | null;
          // Re-read the targets so a child added after the wait started can wake it with its own escalation.
          targets = getTargets();
          if ((p?.fromTaskId && targets.some((t) => t.id === p.fromTaskId)) || p?.severity === "P0" || p?.severity === "P1") {
            finish("escalation");
            return;
          }
        }
        if (ev.type === "goal.amended" || (ev.type === "agent.steered" && ev.taskId !== null && parentIds.has(ev.taskId))) {
          targets = getTargets();
          finish("steered");
          return;
        }
        if (ev.type === "task.status" || ev.type === "run.status") {
          checkEvent();
        }
      });
      const timer = setTimeout(onFallbackTimer, Math.max(50, deadline - Date.now()));
      timer.unref?.();
      signal?.addEventListener("abort", onAbort, { once: true });
      if (signal?.aborted) onAbort();
      else checkEvent();
    });
  }
}
