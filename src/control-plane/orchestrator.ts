import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import type { HarnessConfig, RunnerProfileConfig } from "../config.js";
import type { ModelCatalogCompiler } from "../catalog/compiler.js";
import type { QuotaClient } from "../quota/client.js";
import { AcpRunner } from "../runners/acp-runner.js";
import { PiRunner } from "../runners/pi-runner.js";
import { toolPolicyForTask } from "../runners/types.js";
import { resolveLaunchRoute } from "./launch-route.js";
import type { RunAgentRequest, RunAgentResult, RunnerExecutionHandle } from "../runners/types.js";
import { BisectingMergeQueue } from "../worktree/merge-queue.js";
import { runGit, WorktreeManager } from "../worktree/manager.js";
import type { AgentTier, ControlPlaneDb, RunRecord, TaskKind, TaskRecord } from "./db.js";
import { buildHydrationTurn, buildImmutableSystemPrompt, type AnchoredHandoff } from "./prompts.js";
import { ControlPlaneToolExecutor, compactTaskHandoff, isReadOnlyKind, validateTaskHandoff } from "./tools.js";

interface ActiveTaskExecution {
  taskId: string;
  abort: AbortController;
  generationSignal?: AbortSignal;
  handle?: RunnerExecutionHandle;
  promise: Promise<void>;
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

const LIVE_TASK_STATUSES = new Set<TaskRecord["status"]>(["running", "draining", "waiting_children", "merging"]);
const IN_FLIGHT_TASK_STATUSES = new Set<TaskRecord["status"]>([
  "pending",
  "ready",
  "running",
  "draining",
  "waiting_children",
  "merging",
]);

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
  if (liveInMemory === true) return "active";
  if (tasks.some((t) => LIVE_TASK_STATUSES.has(t.status))) return "active";
  if (tasks.length === 0) return run.status;

  if (tasks.some((t) => t.status === "paused_quota")) {
    return run.status === "stopped" ? "stopped" : "paused_quota";
  }

  if (run.status === "stopped") return "stopped";

  const hasQueued = tasks.some((t) => IN_FLIGHT_TASK_STATUSES.has(t.status));
  if (hasQueued) {
    if (liveInMemory === false) {
      return tasks.some((t) => t.status === "failed") ? "failed" : "stopped";
    }
    return run.status === "active" ? "active" : run.status;
  }

  const realOrch = tasks.find((t) => t.tier === 1 && t.profileId !== "director");
  if (realOrch) {
    if (realOrch.status === "failed") return "failed";
    if (realOrch.status === "cancelled") return "stopped";
    if (realOrch.status === "completed") {
      // Do not falsely declare a run completed if non-orchestrator workers are blocked with unresolved work
      const realChildren = tasks.filter((t) => t.id !== realOrch.id && t.profileId !== "director");
      if (realChildren.some((t) => t.status === "blocked")) return "failed";
      return "completed";
    }
  }

  const realTasks = tasks.filter((t) => t.profileId !== "director");
  if (realTasks.length === 0) return run.status === "active" ? "completed" : run.status;
  // For container runs (`director-tasks-<chatId>`), derive status from the most recently updated task
  // rather than letting an ancient restarted/failed lookup taint all future completed lookups forever.
  if (run.id.startsWith("director-tasks-")) {
    const latestTask = [...realTasks].sort((a, b) => b.updatedAt - a.updatedAt)[0]!;
    if (latestTask.status === "failed") return "failed";
    if (latestTask.status === "cancelled") return "stopped";
    return "completed";
  }
  if (realTasks.some((t) => t.status === "failed")) return "failed";
  if (realTasks.every((t) => t.status === "cancelled")) return "stopped";
  return "completed";
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
      getTaskCancelSignal: (taskId) => this.active.get(taskId)?.abort.signal,
      resumeTask: (taskId, opts) => this.resumeTask(taskId, opts),
    }, config);
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
  reconcileRunStatus(runId: string): RunRecord | null {
    const run = this.db.getRun(runId);
    if (!run) return null;
    const tasks = this.db.listTasks(runId);
    const live = this.isRunActiveInMemory(runId);
    const derived = deriveRunStatus(run, tasks, live);
    if (derived !== run.status) {
      this.db.updateRunStatus(runId, derived);
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
      this.db.updateRunStatus(runId, derived);
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
      for (const t of tasks) {
        if (LIVE_TASK_STATUSES.has(t.status) || t.status === "ready" || t.status === "pending") {
          this.db.updateTask(t.id, { status: "failed", summary: "Interrupted by daemon restart" });
          mutated = true;
        }
      }
      const failedMerges = this.db.failStaleMergesForRun(run.id, "Interrupted by daemon restart");
      if (failedMerges > 0) mutated = true;

      const updatedTasks = this.db.listTasks(run.id);
      const hasQuotaPaused = updatedTasks.some((t) => t.status === "paused_quota");
      if (!hasQuotaPaused) {
        this.db.releaseWriteLeasesForRun(run.id);
      }

      if (mutated || run.status === "active") {
        this.reconcileRunStatus(run.id);
      }
      if (hasQuotaPaused || run.status === "paused_quota") {
        void this.checkRunQuotaQuiescence(run.id).catch(() => {});
      }
    }
  }

  getWorktreeManager(repoRoot: string): WorktreeManager {
    let mgr = this.worktreeManagers.get(repoRoot);
    if (!mgr) {
      mgr = new WorktreeManager(this.db, repoRoot, this.config.worktree);
      this.worktreeManagers.set(repoRoot, mgr);
    }
    return mgr;
  }

  getMergeQueue(repoRoot: string): BisectingMergeQueue {
    let mq = this.mergeQueues.get(repoRoot);
    if (!mq) {
      mq = new BisectingMergeQueue(this.db, this.getWorktreeManager(repoRoot), repoRoot, this.config.worktree);
      this.mergeQueues.set(repoRoot, mq);
    }
    return mq;
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

  async ensureDirectorTaskRun(ownerId: string, repoRoot: string): Promise<RunRecord> {
    const id = `director-tasks-${ownerId}`;
    const found = this.db.getRun(id);
    if (found) return found;
    // Director chat also works in directories without a git checkout.
    const base = await this.getWorktreeManager(repoRoot).resolveRepoInfo().catch(() => ({ baseBranch: "", baseSha: "" }));
    return this.db.transaction(() => {
      const existing = this.db.getRun(id);
      if (existing) return existing;
      const run = this.db.createRun({ id, goal: `Director tasks for ${ownerId}`, repoRoot, ...base });
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
    const run = await this.ensureDirectorTaskRun(ownerId, repoRoot);
    signal?.throwIfAborted();
    const owner = this.db.listTasks(run.id, { parentId: null })[0]!;
    if (name === "wait_tasks" || name === "wait_children") {
      const ids = Array.isArray(args.ids) ? args.ids.map(String) : undefined;
      const fallbackSec =
        typeof args.fallback_interval_s === "number"
          ? args.fallback_interval_s
          : typeof args.timeout_s === "number"
            ? args.timeout_s
            : this.config.events.fallbackWakeupIntervalSec;
      const timeout = Math.max(1, fallbackSec) * 1000;
      const res = await this.waitForChildrenEventDriven(owner.id, ids, args.mode === "any" ? "any" : "all", timeout, signal);
      const escalations = this.db.listEscalations(run.id);
      return {
        children: res.children.map((task) => compactTaskHandoff(task, escalations)),
        ...(res.wakeReason === "fallback_interval" || res.wakeReason === "child_inactive"
          ? { wakeReason: res.wakeReason }
          : {}),
        ...(res.inactiveTaskIds.length > 0 ? { inactiveTaskIds: res.inactiveTaskIds } : {}),
      };
    }
    return this.toolExecutor.execute(run.id, owner.id, name, args);
  }

  async startRun(input: {
    goal: string;
    repoRoot: string;
    profileId?: string;
    model?: string;
    effort?: string;
  }): Promise<{ run: RunRecord; orchestratorTask: TaskRecord }> {
    const { profile, model } = await resolveLaunchRoute(this.config, {
      ...input, tier: 1, catalog: this.catalog, quota: this.quota, cwd: input.repoRoot,
    });
    const wtMgr = this.getWorktreeManager(input.repoRoot);
    const { baseBranch, baseSha } = await wtMgr.resolveRepoInfo();
    const run = this.db.createRun({ goal: input.goal, repoRoot: input.repoRoot, baseBranch, baseSha });

    const orchestratorTask = this.db.createTask({
      runId: run.id,
      parentId: null,
      tier: 1,
      kind: "orchestrator",
      title: `Orchestrator: ${(input.goal.split("\n")[0] ?? input.goal).slice(0, 72)}`,
      objective: input.goal,
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
  }): Promise<TaskRecord> {
    const run = this.db.getRun(input.runId);
    if (!run) throw new Error(`Unknown run ${input.runId}`);
    const toolPolicy = toolPolicyForTask(input.tier, input.kind, input.readOnly);
    const { profile, model } = await resolveLaunchRoute(this.config, {
      ...input, catalog: this.catalog, quota: this.quota, cwd: run.repoRoot,
    });

    const parent = this.db.getTask(input.parentId);
    if (!parent || parent.runId !== run.id) throw new Error(`Unknown parent task ${input.parentId} in ${run.id}`);
    if (this.db.getRun(run.id)?.status === "stopped" || parent.status === "cancelled") throw new Error("Task delegation was stopped.");
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
    if (task.tier === 2 && task.status === "ready") {
      const wtMgr = this.getWorktreeManager(run.repoRoot);
      this.allocatingWorktrees.add(task.id);
      try {
        await wtMgr.createForWorker(run.id, task.id);
      } catch (error) {
        this.db.updateTask(task.id, { status: "failed", summary: "Could not allocate the worker worktree." });
        throw error;
      } finally {
        this.allocatingWorktrees.delete(task.id);
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
    const initialTask = this.db.getTask(taskId);
    if (initialTask) {
      const owningRun = this.db.getRun(initialTask.runId);
      if (owningRun && owningRun.status !== "active") {
        this.db.updateRunStatus(owningRun.id, "active");
      }
    }
    const abort = new AbortController();
    const entry: ActiveTaskExecution = {
      taskId,
      abort,
      promise: Promise.resolve(),
    };
    this.active.set(taskId, entry);
    entry.promise = this.runTaskWithRollovers(taskId, entry).catch((err) => {
      const task = this.db.getTask(taskId);
      if (!task || entry.abort.signal.aborted) return;
      this.db.updateTask(taskId, { status: "failed", summary: "Task launch failed." });
      this.db.emitEvent(task.runId, taskId, "agent.error", { error: String(err) });
    }).finally(() => {
      this.pendingRollovers.delete(taskId);
      this.active.delete(taskId);
      // Kick any newly unblocked dependent tasks in the same run
      const t = this.db.getTask(taskId);
      if (t) {
        const readyTasks = this.db.listTasks(t.runId, { status: "ready" });
        let launchedAny = false;
        for (const rt of readyTasks) {
          if (!this.active.has(rt.id)) {
            this.launchTaskLoop(rt.id);
            launchedAny = true;
          }
        }
        // Quiescence barrier: once ALL active tasks in this run have exited, either arm quota pause
        // OR dynamically reconcile the run's true terminal status!
        if (!launchedAny && !this.isRunActiveInMemory(t.runId)) {
          const paused = this.db.listTasks(t.runId).some((x) => x.status === "paused_quota");
          if (paused) {
            void this.checkRunQuotaQuiescence(t.runId).catch(() => {});
          } else {
            this.reconcileRunStatus(t.runId);
          }
        }
      }
    });
  }

  /**
   * Quiescence Barrier: called when 0 tasks are actively running in `runId`.
   * If any tasks are in `paused_quota`:
   *  - Transitions the run to `paused_quota`.
   *  - If a Quota Endpoint is wired OR passive ACP rate-limit data provided a concrete reset time
   *    (and `autoResumeEnabled` is true): arms an automatic daemon timer (+ early-wakeup poller).
   *  - Otherwise (no Quota Endpoint & no reset time): stops cleanly and waits for the user to
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
    const run = this.db.getRun(runId);
    if (!run) throw new Error(`Unknown run ${runId}`);

    const trigger = opts?.trigger ?? "manual";
    this.clearRunQuotaTimers(runId);
    this.runQuotaStates.delete(runId);

    const tasks = this.db.listTasks(runId);
    const pausedTasks = tasks.filter((t) => t.status === "paused_quota");
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
   */
  async resumeTask(
    taskId: string,
    opts?: { note?: string; profileId?: string; model?: string; effort?: string; allowCompleted?: boolean },
  ): Promise<TaskRecord> {
    const task = this.db.getTask(taskId);
    if (!task) throw new Error(`Unknown task ${taskId}`);
    if (task.profileId === "director") throw new Error(`Task ${task.id} is a synthetic Director owner and cannot be resumed.`);
    if (task.status === "completed" && !opts?.allowCompleted) throw new Error(`Task ${task.id} is already completed.`);
    if (this.active.has(task.id)) throw new Error(`Task ${task.id} is already running.`);

    const run = this.db.getRun(task.runId);
    if (!run) throw new Error(`Unknown run ${task.runId}`);

    const toolPolicy = toolPolicyForTask(task.tier, task.kind, task.readOnly);
    let wt = task.worktreeId ? this.db.getWorktree(task.worktreeId) : null;
    let reacquiredLeases = false;
    let profile: RunnerProfileConfig;
    let model: string;
    try {
      if (toolPolicy === "read_write_workspace") {
        if (wt && wt.status === "merged") {
          throw new Error(`Cannot resume worker ${task.id}: its worktree ${wt.id} was already merged.`);
        }
        // Atomically validate and re-acquire assigned write leases before resuming a Tier 2 worker!
        if (task.tier === 2) {
          this.db.reacquireWriteLeasesForTask(run.id, task.id);
          reacquiredLeases = true;
        }
        const wtMgr = this.getWorktreeManager(run.repoRoot);
        if (!wt || wt.runId !== run.id || !existsSync(wt.path)) {
          wt = await wtMgr.createForWorker(run.id, task.id);
        } else if (wt.status !== "active") {
          this.db.updateWorktree(wt.id, { status: "active", cleanedAt: null });
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
      if (reacquiredLeases) {
        this.db.db.prepare("UPDATE write_leases SET active = 0 WHERE task_id = ?").run(task.id);
      }
      throw err;
    }
    this.quota.clearPassiveExhaustion(profile.id);

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
    if (opts?.note?.trim()) {
      handoff.nextActions.unshift(`Resume guidance: ${opts.note.trim()}`);
    }

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

    if (run.status !== "active") {
      this.db.updateRunStatus(run.id, "active");
    }

    // If a Goal is linked to this run and was paused/stopped, flip it back to executing
    try {
      this.db.db
        .prepare("UPDATE goals SET status = 'executing', updated_at = ? WHERE linked_run_id = ? AND status NOT IN ('achieved','cancelled')")
        .run(Date.now(), run.id);
    } catch {
      // goals table may not exist in unit tests using ControlPlaneDb alone
    }

    const nextGen = task.generation + 1;
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

    this.db.emitEvent(run.id, task.id, "task.resumed", {
      taskId: task.id,
      fromStatus: task.status,
      generation: nextGen,
      model,
      effort: opts?.effort !== undefined ? opts.effort : task.effort,
      note: opts?.note ?? null,
    });

    this.launchTaskLoop(task.id);
    return this.db.getTask(task.id)!;
  }

  /**
   * Resumes a run in-place: delegates quota-paused runs to `resumeRunFromQuota`, and resumes
   * crashed/failed/stopped/cancelled tasks in-place while keeping completed work untouched.
   */
  async resumeRun(
    runId: string,
    opts?: { trigger?: "manual" | "auto_timer" | "quota_recovered" | "user_timer"; userMessage?: string; note?: string },
  ): Promise<{ resumed: boolean; resumedTaskIds: string[]; run: RunRecord }> {
    const run = this.db.getRun(runId);
    if (!run) throw new Error(`Unknown run ${runId}`);

    const tasks = this.db.listTasks(run.id);
    if (run.status === "paused_quota" || tasks.some((t) => t.status === "paused_quota")) {
      return this.resumeRunFromQuota(run.id, opts);
    }

    const RESUMABLE = new Set(["failed", "cancelled", "blocked", "returned_for_rescope"]);
    const candidates = tasks.filter(
      (t) => t.profileId !== "director" && RESUMABLE.has(t.status) && !this.active.has(t.id),
    );
    if (candidates.length === 0) {
      return { resumed: false, resumedTaskIds: [], run: this.getEffectiveRun(run.id) ?? run };
    }

    const note = opts?.note ?? opts?.userMessage;
    const resumedTaskIds: string[] = [];
    for (const t of candidates) {
      const resumed = await this.resumeTask(t.id, note ? { note } : undefined);
      resumedTaskIds.push(resumed.id);
    }
    this.db.emitEvent(run.id, null, "run.resumed", {
      runId: run.id,
      trigger: opts?.trigger ?? "manual",
      resumedTaskIds,
      note: note ?? null,
    });
    return { resumed: resumedTaskIds.length > 0, resumedTaskIds, run: this.db.getRun(run.id)! };
  }

  private async runTaskWithRollovers(taskId: string, exec: ActiveTaskExecution): Promise<void> {
    while (!exec.abort.signal.aborted) {
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
          this.db.reacquireWriteLeasesForTask(run.id, task.id);
        }
        if (!wt || wt.runId !== task.runId) {
          const wtMgr = this.getWorktreeManager(run.repoRoot);
          this.allocatingWorktrees.add(task.id);
          try {
            wt = await wtMgr.createForWorker(run.id, task.id);
          } finally {
            this.allocatingWorktrees.delete(task.id);
          }
        }
      }
      if (toolPolicy === "read_write_workspace" && (!wt || wt.runId !== task.runId)) {
        throw new Error("A writable agent requires its assigned worktree.");
      }
      const cwd = wt?.path ?? run.repoRoot;
      const { profile, model, wireModel } = await this.taskRoute(task, cwd);
      if (exec.abort.signal.aborted) return;
      task.model = model;

      this.db.updateTask(task.id, { status: "running", model, profileId: profile.id });

      const handoff = task.handoffJson ? (JSON.parse(task.handoffJson) as AnchoredHandoff) : null;
      const quotaResumeNote = this.taskResumeNotes.get(task.id) ?? null;
      if (quotaResumeNote) this.taskResumeNotes.delete(task.id);

      const systemPrompt = buildImmutableSystemPrompt(task.tier, task.kind, this.config.codeMode);
      const writeScopes = task.tier === 2 ? this.db.listActiveWriteScopes(run.id, task.id)
        : task.tier === 3 && task.parentId ? this.db.listActiveWriteScopes(run.id, task.parentId) : [];
      const scopedKbHints = task.tier === 2 ? this.db.kb.getScopedAmbientIndex(run.repoRoot, writeScopes, 5) : [];

      const initialPrompt = buildHydrationTurn({
        task,
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
      });

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
      const generation = new AbortController();
      exec.generationSignal = AbortSignal.any([generation.signal, exec.abort.signal]);
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
        executeTool: (name, args) => this.toolExecutor.execute(run.id, task.id, name, args),
        daemonSocketPath: this.config.socketPath,
        mcpBridgeEntrypoint,
        externalMcpServers: this.config.externalMcpServers,
        allowShellInReadOnly: task.kind === "verify",
        signal: exec.generationSignal,
        onHandle: (h) => {
          exec.handle = h;
        },
        callbacks: {
          onSessionId: (sessionId) => {
            this.db.updateTask(task.id, { sessionId });
          },
          onTextDelta: (text) => {
            this.db.emitEvent(run.id, task.id, "agent.text_delta", { text });
          },
          onThoughtDelta: (text) => {
            this.db.emitEvent(run.id, task.id, "agent.thought_delta", { text });
          },
          onUsage: (usage) => {
            this.db.updateTask(task.id, { contextUsed: usage.used, contextSize: usage.size });
            this.db.emitEvent(run.id, task.id, "agent.usage", usage);
          },
          onTurn: (turnsUsed) => {
            this.db.updateTask(task.id, { turnsUsed });
          },
          onArtifact: (art) => {
            this.db.recordArtifact({ runId: run.id, taskId: task.id, ...art });
          },
          onPassiveQuota: (sig) => {
            this.quota.ingestPassiveSignal(sig);
            this.db.emitEvent(run.id, task.id, "quota.passive_signal", sig);
          },
          onQuotaRetryState: (ev) => {
            this.db.emitEvent(run.id, task.id, ev.state === "retrying" ? "quota.agent_retrying" : "quota.agent_recovered", {
              ...ev,
              profileId: task.profileId,
              model: task.model,
              title: task.title,
            });
          },
          onToolEvent: (ev) => {
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

        const res = await runner.run(runnerReq);
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

        // Clean rollover requested either via explicit `request_rollover`, context threshold, or bounded `max_turns` for Tier 1/2
        const explicitRollover = this.pendingRollovers.get(task.id);
        const maxTurnsRollover = res.stopReason === "max_turns" && (latest.tier === 1 || latest.tier === 2);
        if (explicitRollover || res.stopReason === "rollover_requested" || maxTurnsRollover) {
          this.pendingRollovers.delete(task.id);
          const nextGen = latest.generation + 1;
          // Fail fast after 2 consecutive zero-progress automatic context-threshold rollovers
          if (!explicitRollover && res.stopReason === "rollover_requested") {
            const fp = await this.computeAutoRolloverFingerprint(run.id, task.id, wt?.path ?? null);
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
            reason: explicitRollover ? "explicit_handoff" : maxTurnsRollover ? "max_turns" : "context_threshold",
          });
          continue; // Immediately boot generation N+1 cleanly!
        }

        // Agent stopped early due to quota / rate-limit timeout: preserve worktree, WIP & handoff, do NOT fail!
        if (res.stopReason === "quota_paused") {
          const qp = res.quotaPause;
          const reason = qp?.reason ?? "Quota / rate limit reached";
          const pauseHandoff = await this.buildCleanRolloverHandoff({
            runId: run.id,
            task: latest,
            priorHandoff: handoff,
            explicitHandoff: null,
            worktreePath: wt?.path ?? null,
            result: res,
          });
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
            summary: "Task paused for quota.",
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

        if (res.stopReason === "cancelled" || exec.abort.signal.aborted) {
          this.db.updateTask(task.id, { status: "cancelled", summary: "Cancelled" });
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

        if (isReadOnlyKind(latest.kind) || res.stopReason === "error" || res.stopReason === "max_turns") {
          this.db.updateTask(task.id, {
            status: "failed",
            summary: res.stopReason === "max_turns"
              ? `Task hit max_turns (${latest.maxTurns}) without completing.`
              : isReadOnlyKind(latest.kind)
                ? "Task ended without a valid sourced handoff."
                : "Task runner reported an error.",
          });
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
              this.db.updateTask(task.id, {
                status: "failed",
                summary: `Worker exited with unmerged/dirty worktree (status: ${latestWt.status}, dirty: ${dirty}, unmergedCommits: ${hasUnmergedCommits}).`,
              });
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
          const nextHandoff = await this.buildCleanRolloverHandoff({
            runId: run.id,
            task: latest,
            priorHandoff: handoff,
            explicitHandoff: explicitRollover.handoff,
            worktreePath: wt?.path ?? null,
            result: { text: "", stopReason: "rollover_requested" },
          });
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
        this.db.updateTask(task.id, { status: "failed", summary: `Task runner failed before settlement: ${msg.slice(0, 240)}` });
        this.db.emitEvent(run.id, task.id, "agent.error", { error: msg });
        return;
      } finally {
        generation.abort(); // Outstanding MCP scripts must not survive their runner (including a rollover).
        exec.generationSignal = undefined;
        exec.handle = undefined;
      }
    }
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
          : input.result.stopReason === "max_turns"
            ? `Generation ${input.task.generation} reached its max_turns budget (no explicit request_rollover submitted).`
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
      const [head, status] = await Promise.all([
        runGit(worktreePath, ["rev-parse", "HEAD"]).then((r) => r.stdout).catch(() => ""),
        runGit(worktreePath, ["status", "--porcelain"]).then((r) => r.stdout).catch(() => ""),
      ]);
      gitState = `${head}|${status}`;
    }
    const artCount = this.db.listArtifacts(runId, taskId, 200).length;
    const kids = this.db.listTasks(runId, { parentId: taskId });
    const kidSig = kids.map((k) => `${k.id}:${k.status}:${k.generation}:${k.updatedAt}`).join(",");
    const bbKeys = Object.keys(this.db.blackboardGet(runId)).sort().join(",");
    const openEsc = this.db.listEscalations(runId, "open").map((e) => e.id).join(",");
    return `${gitState}::art=${artCount}::kids=${kidSig}::bb=${bbKeys}::esc=${openEsc}`;
  }

  async steerTask(taskId: string, message: string): Promise<boolean> {
    const exec = this.active.get(taskId);
    if (!exec?.handle) return false;
    const ok = await exec.handle.steer(message);
    const task = this.db.getTask(taskId);
    if (task) {
      this.db.emitEvent(task.runId, taskId, "agent.steered", { message, accepted: ok });
    }
    return ok;
  }

  async cancelTask(taskId: string, reason: string): Promise<void> {
    const exec = this.active.get(taskId);
    if (exec) {
      exec.abort.abort(new Error(reason));
      await exec.handle?.cancel(reason).catch(() => {});
    }
    this.taskQuotaHints.delete(taskId);
    this.taskResumeNotes.delete(taskId);
    this.pendingRollovers.delete(taskId);
    const task = this.db.getTask(taskId);
    if (task && !["completed", "cancelled", "failed"].includes(task.status)) {
      this.db.updateTask(taskId, { status: "cancelled", summary: `Cancelled: ${reason}` });
    }
    // Archive & tear down a cancelled worker's worktree immediately — unless a merge is actively draining it.
    // Tier 3 tasks only borrow their parent Worker's worktree, so cancelling them must not remove it.
    if (task?.tier === 2 && task.worktreeId) {
      const run = this.db.getRun(task.runId);
      const wt = this.db.getWorktree(task.worktreeId);
      if (run && wt && !wt.cleanedAt && wt.status !== "merged" && wt.status !== "queued" && task.status !== "merging") {
        void this.getWorktreeManager(run.repoRoot)
          .teardown(wt.id, { archiveBeforeRemove: true, finalStatus: "archived" })
          .catch(() => {});
      }
    }
  }

  async stopRun(runId: string, reason = "Stopped by user"): Promise<void> {
    this.clearRunQuotaTimers(runId);
    this.runQuotaStates.delete(runId);
    const tasks = this.db.listTasks(runId);
    await Promise.all(tasks.map((t) => this.cancelTask(t.id, reason)));
    this.db.releaseWriteLeasesForRun(runId);
    this.db.updateRunStatus(runId, "stopped");
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
  ): Promise<{
    children: TaskRecord[];
    wakeReason: "already_settled" | "event" | "escalation" | "steered" | "fallback_interval" | "child_inactive";
    inactiveTaskIds: string[];
  }> {
    signal?.throwIfAborted();
    const parent = this.db.getTask(parentId);
    if (!parent) throw new Error(`Unknown parent task ${parentId}`);

    const terminal = new Set(["completed", "failed", "cancelled", "returned_for_rescope", "blocked", "paused_quota"]);
    const deadline = Date.now() + Math.max(50, fallbackIntervalMs);

    const getTargets = () => {
      const kids = this.db.listTasks(parent.runId, { parentId });
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

    const targetIdSet = new Set(targets.map((t) => t.id));
    if (parent.status === "running") this.db.updateTask(parentId, { status: "waiting_children" });
    return new Promise((resolve, reject) => {
      let settled = false;
      const finish = (
        wakeReason: "event" | "escalation" | "steered" | "fallback_interval" | "child_inactive",
        inactiveTaskIds: string[] = [],
      ) => {
        if (settled) return;
        settled = true;
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
        if (ev.runId !== parent.runId) return;
        if (ev.type === "escalation.created") {
          const p = ev.payload as { fromTaskId?: string; severity?: string } | null;
          if ((p?.fromTaskId && targetIdSet.has(p.fromTaskId)) || p?.severity === "P0" || p?.severity === "P1") {
            targets = getTargets();
            finish("escalation");
            return;
          }
        }
        if (ev.type === "goal.amended" || (ev.type === "agent.steered" && ev.taskId === parentId)) {
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
