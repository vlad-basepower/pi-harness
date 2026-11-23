import type {
  ArtifactRecord,
  EscalationRecord,
  MergeQueueRecord,
  MessageRecord,
  RunRecord,
  TaskRecord,
  WorktreeRecord,
} from "../control-plane/db.js";
import type { RunActivitySummary, TaskActivitySnapshot } from "../control-plane/telemetry.js";
import type { TimerRecord } from "../control-plane/timers.js";
import type { DaemonClient } from "../daemon/remote.js";
import type { GoalRecord, GoalReviewVerdict, GoalRevisionRecord } from "../goals/engine.js";
import type { DaemonEventFrame, InboxItem, SnapshotWorktreeEntry } from "../protocol/daemon-rpc.js";
import type { AgentStateCounts, AttentionSlot, LiveHudItem, TuiEntityRef as EntityRef } from "./core/index.js";

export interface AttachableSession {
  id: string;
  host?: string;
  nodeId?: string | null;
  address?: string;
  kind: "chat" | "run";
  title: string;
  projectId?: string | null;
  projectName?: string | null;
  repoRoot: string;
  status: string;
  running: boolean;
  attached: boolean;
  attachedByMe?: boolean;
  activeGoals?: number;
  profileId?: string;
  model?: string | null;
  effort?: string | null;
  updatedAt: number;
}

export interface SnapshotGoalEntry {
  goal: GoalRecord;
  reviews: GoalReviewVerdict[];
  revisions: GoalRevisionRecord[];
}

export interface SnapshotRunEntry {
  run: RunRecord;
  goalId: string | null;
  tasks: TaskRecord[];
  escalations: EscalationRecord[];
  quotaPause: {
    mode: string;
    reason: string;
    resumeAt: number | null;
  } | null;
  activity: RunActivitySummary | null;
}

export interface WorkSnapshot {
  generatedAt: number;
  latestSeq: number;
  goals: SnapshotGoalEntry[];
  runs: SnapshotRunEntry[];
  escalations: EscalationRecord[];
  timers: TimerRecord[];
  merges: MergeQueueRecord[];
  worktrees: SnapshotWorktreeEntry[];
  messages: MessageRecord[];
  inbox: InboxItem[];
  inboxAuto?: InboxItem[];
}

const EMPTY_SNAPSHOT: WorkSnapshot = {
  generatedAt: 0,
  latestSeq: 0,
  goals: [],
  runs: [],
  escalations: [],
  timers: [],
  merges: [],
  worktrees: [],
  messages: [],
  inbox: [],
  inboxAuto: [],
};

const WORK_EVENT_PREFIXES = ["goal.", "escalation.", "merge.", "timer.", "quota.", "worktree.", "inbox.triage."];
const WORK_EVENT_EXACT = new Set([
  "run.status",
  "run.created",
  "task.created",
  "task.status",
  "task.resumed",
  "agent.timer_sleep",
  "agent.rollover",
]);
const ACTIVE_GOAL_STATUSES = new Set<GoalRecord["status"]>([
  "executing",
  "verifying",
  "reviewing",
  "awaiting_human",
  "paused",
]);

const TASK_ACTIVITY_PRIORITY: Record<TaskRecord["status"], number> = {
  running: 0,
  merging: 1,
  draining: 2,
  waiting_children: 3,
  blocked: 4,
  returned_for_rescope: 5,
  paused_quota: 6,
  ready: 7,
  pending: 8,
  failed: 9,
  completed: 10,
  cancelled: 11,
};

export interface LiveHudSummary {
  slots: AttentionSlot[];
  agentCounts: AgentStateCounts;
  hudItems: LiveHudItem[];
  hudSecondaryLine: string | null;
  activeGoalsCount: number;
}

/**
 * Normalized client-side state store fed by `work.snapshot` and debounced live daemon events.
 * Shared by Dock, Work Navigator, Inspectors, and Go-To-Anything palette.
 */
export class WorkStore {
  private snapshot: WorkSnapshot = EMPTY_SNAPSHOT;
  private listeners = new Set<() => void>();
  private refreshTimer: ReturnType<typeof setTimeout> | null = null;
  private inflight: Promise<WorkSnapshot> | null = null;
  private pendingRefresh = false;
  private disposed = false;
  private detachEvent: (() => void) | null = null;
  lastError: string | null = null;

  constructor(
    private readonly client: DaemonClient,
    private scope: { chatSessionId?: string; repoRoot?: string; runId?: string },
  ) {
    this.detachEvent = client.onEvent((ev) => this.handleDaemonEvent(ev));
  }

  setScope(scope: { chatSessionId?: string; repoRoot?: string; runId?: string }): void {
    this.scope = scope;
  }

  get state(): WorkSnapshot {
    return this.snapshot;
  }

  subscribe(fn: () => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  private emit(): void {
    for (const fn of this.listeners) {
      try {
        fn();
      } catch {
        // ignore listener errors
      }
    }
  }

  private handleDaemonEvent(ev: DaemonEventFrame): void {
    if (this.disposed) return;
    if (ev.watchId) return; // watch frames are handled by live transcript inspector
    if (
      WORK_EVENT_EXACT.has(ev.type) ||
      WORK_EVENT_PREFIXES.some((p) => ev.type.startsWith(p))
    ) {
      this.scheduleRefresh(140);
    } else if (ev.type === "agent.tool") {
      // Attached-run streams deliver tool calls; refresh with a gentler debounce so live currentAction stays fresh.
      this.scheduleRefresh(600);
    }
  }

  scheduleRefresh(delayMs = 150): void {
    if (this.disposed) return;
    if (this.refreshTimer) clearTimeout(this.refreshTimer);
    this.refreshTimer = setTimeout(() => {
      this.refreshTimer = null;
      void this.refresh();
    }, delayMs);
    this.refreshTimer.unref?.();
  }

  async refresh(): Promise<WorkSnapshot> {
    if (this.disposed) return this.snapshot;
    if (this.inflight) {
      this.pendingRefresh = true;
      await this.inflight;
      if (this.inflight) return this.inflight;
      return this.snapshot;
    }
    this.inflight = this.client
      .request<WorkSnapshot>("work.snapshot", {
        ...(this.scope.chatSessionId ? { chatSessionId: this.scope.chatSessionId } : {}),
        ...(this.scope.repoRoot ? { repoRoot: this.scope.repoRoot } : {}),
        ...(this.scope.runId ? { runId: this.scope.runId } : {}),
      })
      .then((snap) => {
        this.snapshot = snap ?? EMPTY_SNAPSHOT;
        this.lastError = null;
        this.emit();
        return this.snapshot;
      })
      .catch((err) => {
        this.lastError = err instanceof Error ? err.message : String(err);
        this.emit();
        return this.snapshot;
      })
      .finally(() => {
        this.inflight = null;
        if (this.pendingRefresh && !this.disposed) {
          this.pendingRefresh = false;
          void this.refresh();
        }
      });
    return this.inflight;
  }

  dispose(): void {
    this.disposed = true;
    if (this.refreshTimer) clearTimeout(this.refreshTimer);
    this.refreshTimer = null;
    this.detachEvent?.();
    this.detachEvent = null;
    this.listeners.clear();
  }

  getGoalEntry(goalId: string): SnapshotGoalEntry | undefined {
    return this.snapshot.goals.find((g) => g.goal.id === goalId);
  }

  getRunEntry(runId: string): SnapshotRunEntry | undefined {
    return this.snapshot.runs.find((r) => r.run.id === runId);
  }

  getEscalation(id: string): EscalationRecord | undefined {
    return this.snapshot.escalations.find((e) => e.id === id);
  }

  getTimer(id: string): TimerRecord | undefined {
    return this.snapshot.timers.find((t) => t.id === id);
  }

  getWorktree(id: string): SnapshotWorktreeEntry | undefined {
    return this.snapshot.worktrees.find((w) => w.id === id || w.path === id);
  }

  getMerge(id: string): MergeQueueRecord | undefined {
    return this.snapshot.merges.find((m) => m.id === id);
  }

  get openEscalationsCount(): number {
    return this.snapshot.escalations.filter((e) => e.status === "open").length;
  }

  get unresolvedEscalationsCount(): number {
    return this.snapshot.escalations.filter((e) => e.status === "open" || e.status === "acked").length;
  }

  get pendingTimersCount(): number {
    return this.snapshot.timers.filter((t) => t.status === "pending").length;
  }

  /**
   * Computes exact live agent counts across states plus open escalations and active merges.
   * Excludes synthetic `director` placeholder tasks; ignores stale queued/blocked/failed/paused
   * tasks belonging to historical completed/stopped/failed runs not tied to an active goal.
   */
  getAgentCounts(): AgentStateCounts {
    const activeGoalIds = new Set(
      this.snapshot.goals
        .filter((e) => ACTIVE_GOAL_STATUSES.has(e.goal.status))
        .map((e) => e.goal.id),
    );

    let active = 0;
    let waiting = 0;
    let blocked = 0;
    let paused = 0;
    let queued = 0;
    let failed = 0;

    for (const entry of this.snapshot.runs) {
      const isLiveRun =
        entry.run.status === "active" ||
        entry.run.status === "paused_quota" ||
        (entry.goalId != null && activeGoalIds.has(entry.goalId));
      let hasPausedQuotaTask = false;

      for (const t of entry.tasks) {
        if (t.profileId === "director") continue;
        if (t.status === "running" || t.status === "draining" || t.status === "merging") {
          active++;
          continue;
        }
        if (!isLiveRun) continue;
        switch (t.status) {
          case "waiting_children":
            waiting++;
            break;
          case "blocked":
          case "returned_for_rescope":
            blocked++;
            break;
          case "paused_quota":
            paused++;
            hasPausedQuotaTask = true;
            break;
          case "pending":
          case "ready":
            queued++;
            break;
          case "failed":
            failed++;
            break;
          default:
            break;
        }
      }

      if (entry.run.status === "paused_quota" && !hasPausedQuotaTask) {
        paused++;
      }
    }

    const liveRunIds = new Set(
      this.snapshot.runs
        .filter((r) => r.run.status === "active" || r.run.status === "paused_quota")
        .map((r) => r.run.id),
    );
    const openEscalations = this.snapshot.escalations.filter((e) => e.status === "open").length;
    let activeMerges = 0;
    let bouncedMerges = 0;
    for (const m of this.snapshot.merges) {
      if (m.status === "merged") continue;
      if (!liveRunIds.has(m.runId)) continue;
      activeMerges++;
      if (m.status === "conflict" || m.status === "ci_failed") bouncedMerges++;
    }

    return {
      active,
      waiting,
      blocked,
      paused,
      queued,
      failed,
      openEscalations,
      activeMerges,
      bouncedMerges,
    };
  }

  /** True when any goal, run, or agent is currently in flight (used for gentle live HUD telemetry polling). */
  hasLiveWork(): boolean {
    if (this.snapshot.goals.some((e) => ACTIVE_GOAL_STATUSES.has(e.goal.status))) return true;
    if (this.snapshot.runs.some((r) => r.run.status === "active" || r.run.status === "paused_quota")) return true;
    const c = this.getAgentCounts();
    return c.active + c.waiting + c.blocked + c.paused + c.queued + c.openEscalations + c.activeMerges > 0;
  }

  private getPrioritizedRunTasks(run: SnapshotRunEntry): Array<{
    id: string;
    tier: number;
    kind: TaskRecord["kind"];
    title: string;
    status: TaskRecord["status"];
    currentAction: string | null;
  }> {
    const fromActivity: Array<{
      id: string;
      tier: number;
      kind: TaskRecord["kind"];
      title: string;
      status: TaskRecord["status"];
      currentAction: string | null;
    }> = run.activity?.tasks
      ? run.activity.tasks.map((t: TaskActivitySnapshot) => ({
          id: t.id,
          tier: t.tier,
          kind: t.kind,
          title: t.title,
          status: t.status,
          currentAction: t.currentAction,
        }))
      : run.tasks
          .filter((t) => t.profileId !== "director")
          .map((t) => ({
            id: t.id,
            tier: t.tier,
            kind: t.kind,
            title: t.title,
            status: t.status,
            currentAction: null,
          }));

    return fromActivity
      .filter((t) => t.status !== "completed" && t.status !== "cancelled")
      .sort((a, b) => (TASK_ACTIVITY_PRIORITY[a.status] ?? 99) - (TASK_ACTIVITY_PRIORITY[b.status] ?? 99));
  }

  private formatTaskBrief(t: {
    tier: number;
    kind: TaskRecord["kind"];
    title: string;
    status: TaskRecord["status"];
    currentAction: string | null;
  }): string {
    const role = `T${t.tier}/${t.kind}`;
    const base = `${role} ${t.title}`;
    if (t.currentAction) return `${base} — ${t.currentAction}`;
    if (t.status === "waiting_children") return `${base} — waiting on children`;
    if (t.status === "blocked") return `${base} — blocked`;
    if (t.status === "returned_for_rescope") return `${base} — rescope needed`;
    if (t.status === "paused_quota") return `${base} — paused (quota)`;
    if (t.status === "ready" || t.status === "pending") return `${base} — queued`;
    return base;
  }

  private describeRunActivity(run?: SnapshotRunEntry): string {
    if (!run) return "";
    const openEsc = run.escalations.find((e) => e.status === "open");
    if (openEsc) return `Esc ${openEsc.severity}: ${openEsc.title}`;
    if (run.quotaPause) return `quota paused: ${run.quotaPause.reason}`;
    const liveTasks = this.getPrioritizedRunTasks(run);
    if (liveTasks.length > 0) {
      return this.formatTaskBrief(liveTasks[0]!);
    }
    const phase = run.activity?.phase;
    if (phase && phase !== "idle" && phase !== "completed" && phase !== "stopped") {
      return phase;
    }
    return "";
  }

  private describeGoalActivity(goal: GoalRecord, run?: SnapshotRunEntry): string {
    if (goal.status === "awaiting_human") {
      return goal.lastReviewSummary || "awaiting human input";
    }
    const openEsc = run?.escalations.find((e) => e.status === "open");
    if (openEsc) return `Esc ${openEsc.severity}: ${openEsc.title}`;
    if (run?.quotaPause) return `quota paused: ${run.quotaPause.reason}`;

    const lp = goal.liveProgress;
    const phase = lp?.phase?.trim() || "";
    const summary = lp?.summary?.trim() || "";
    if (phase && summary) {
      return summary.toLowerCase().startsWith(phase.toLowerCase()) ? summary : `${phase}: ${summary}`;
    }
    if (summary) return summary;
    if (phase) {
      const topTask = run ? this.getPrioritizedRunTasks(run)[0] : undefined;
      return topTask ? `${phase} · ${this.formatTaskBrief(topTask)}` : phase;
    }

    const runAct = this.describeRunActivity(run);
    if (runAct) return runAct;

    if (goal.status === "reviewing" && goal.reviewCycle > 0) {
      return `review cycle ${goal.reviewCycle}`;
    }
    return goal.lastReviewSummary || goal.status;
  }

  /**
   * Builds numbered attention slots for quick-jump (`Alt+1`..`Alt+5`):
   *  - Open P0/P1 escalations and `awaiting_human` goals appear FIRST so blockers are always 1 keypress away.
   *  - Active/verifying/reviewing/paused goals follow, up to 5 compact slots.
   */
  buildAttentionSlots(maxSlots = 5): AttentionSlot[] {
    const slots: AttentionSlot[] = [];
    const seenKeys = new Set<string>();

    const pushSlot = (item: Omit<AttentionSlot, "slot">) => {
      if (slots.length >= maxSlots) return;
      const key = `${item.ref.kind}:${item.ref.id}`;
      if (seenKeys.has(key)) return;
      seenKeys.add(key);
      slots.push({ slot: slots.length + 1, ...item });
    };

    // 1. Open escalations & awaiting_human goals from severity-sorted inbox
    for (const item of this.snapshot.inbox) {
      if (item.kind === "escalation" || item.kind === "goal_awaiting_human" || item.kind === "quota_paused") {
        const ref: EntityRef = {
          kind: item.ref.kind,
          id: item.ref.id,
          runId: item.ref.runId,
          goalId: item.ref.goalId,
          label: item.ref.label ?? item.title,
        };
        pushSlot({
          label: item.ref.id,
          status: item.severity === "P0" ? "p0" : item.kind === "goal_awaiting_human" ? "awaiting_human" : "p1",
          badge: item.kind === "escalation" ? item.severity : item.kind === "goal_awaiting_human" ? "input" : "quota",
          ref,
        });
      }
    }

    // 2. Active goals (executing / verifying / reviewing / awaiting_human / paused)
    for (const entry of this.snapshot.goals) {
      const g = entry.goal;
      if (!ACTIVE_GOAL_STATUSES.has(g.status)) continue;
      const run = g.linkedRunId ? this.getRunEntry(g.linkedRunId) : undefined;
      const openEsc = run ? run.escalations.filter((e) => e.status === "open").length : 0;
      const runningTasks = run
        ? run.tasks.filter(
            (t) => t.profileId !== "director" && (t.status === "running" || t.status === "draining" || t.status === "merging"),
          ).length
        : 0;
      const badge = openEsc > 0 ? `!${openEsc}` : runningTasks > 0 ? `${runningTasks}w` : g.status;
      pushSlot({
        label: `${g.id} ${g.title.slice(0, 18)}`,
        status: openEsc > 0 ? "blocked" : g.status,
        badge,
        ref: { kind: "goal", id: g.id, runId: g.linkedRunId ?? undefined, label: `${g.id} ${g.title}` },
      });
    }

    // 3. Standalone active runs not bound to a goal
    for (const r of this.snapshot.runs) {
      if (r.goalId) continue;
      if (r.run.id.startsWith("director-tasks-")) continue;
      if (r.run.status !== "active" && r.run.status !== "paused_quota") continue;
      const running = r.tasks.filter(
        (t) => t.profileId !== "director" && (t.status === "running" || t.status === "draining" || t.status === "merging"),
      ).length;
      pushSlot({
        label: r.run.id,
        status: r.run.status,
        badge: running > 0 ? `${running}w` : r.run.status,
        ref: { kind: "run", id: r.run.id, label: r.run.id },
      });
    }

    return slots;
  }

  /**
   * Builds the complete live HUD model for `FixedDock`:
   *  - Exact `slots` (`Alt+1`..`Alt+5`),
   *  - Exact `agentCounts` across all task states + escalations/merges,
   *  - Ordered `hudItems` with live "what is happening right now" descriptions,
   *  - Optional `hudSecondaryLine` showing live worker/subagent activity when a single goal/run is active.
   */
  buildLiveHud(): LiveHudSummary {
    const slots = this.buildAttentionSlots(5);
    const slotByKey = new Map(slots.map((s) => [`${s.ref.kind}:${s.ref.id}`, s.slot]));
    const agentCounts = this.getAgentCounts();
    const activeGoalsCount = this.snapshot.goals.filter((e) => ACTIVE_GOAL_STATUSES.has(e.goal.status)).length;

    const hudItems: LiveHudItem[] = [];
    const seenKeys = new Set<string>();
    const pushItem = (item: Omit<LiveHudItem, "slot">) => {
      const key = `${item.ref.kind}:${item.ref.id}`;
      if (seenKeys.has(key)) return;
      seenKeys.add(key);
      hudItems.push({ ...item, slot: slotByKey.get(key) });
    };

    const quotaSurfacedGoalIds = new Set<string>();

    // 1. Urgent inbox items first (matches Alt+1..5 slot ordering)
    for (const item of this.snapshot.inbox) {
      if (item.kind === "escalation") {
        const esc = this.getEscalation(item.ref.id);
        const ref: EntityRef = {
          kind: item.ref.kind,
          id: item.ref.id,
          runId: item.ref.runId,
          goalId: item.ref.goalId,
          label: item.ref.label ?? item.title,
        };
        pushItem({
          kind: "escalation",
          id: item.ref.id,
          status: item.severity === "P0" ? "p0" : "p1",
          title: esc?.title ?? item.title,
          badge: item.severity,
          activity: item.detail,
          ref,
        });
      } else if (item.kind === "goal_awaiting_human") {
        const goalEntry = this.getGoalEntry(item.goalId ?? item.ref.id);
        const g = goalEntry?.goal;
        const ref: EntityRef = {
          kind: "goal",
          id: g?.id ?? item.ref.id,
          runId: g?.linkedRunId ?? item.ref.runId,
          goalId: g?.id ?? item.goalId ?? undefined,
          label: item.ref.label ?? item.title,
        };
        pushItem({
          kind: "goal",
          id: g?.id ?? item.ref.id,
          status: "awaiting_human",
          title: g?.title ?? item.title,
          badge: "input",
          activity: g?.lastReviewSummary ?? item.detail,
          ref,
        });
      } else if (item.kind === "quota_paused") {
        const linkedGoal = item.goalId ? this.getGoalEntry(item.goalId)?.goal : undefined;
        if (item.goalId) quotaSurfacedGoalIds.add(item.goalId);
        const ref: EntityRef = {
          kind: item.ref.kind,
          id: item.ref.id,
          runId: item.ref.runId,
          goalId: item.ref.goalId,
          label: item.ref.label ?? item.title,
        };
        pushItem({
          kind: "run",
          id: item.ref.id,
          status: "paused_quota",
          title: linkedGoal ? linkedGoal.title : "Quota paused",
          badge: "quota",
          activity: item.detail,
          ref,
        });
      }
    }

    // 2. Active/in-flight goals with live progress / active task telemetry
    //    Put goals not already represented by a quota_paused inbox row first so both HUD rows show distinct work.
    const activeGoalEntries = this.snapshot.goals
      .filter((e) => ACTIVE_GOAL_STATUSES.has(e.goal.status))
      .sort((a, b) => Number(quotaSurfacedGoalIds.has(a.goal.id)) - Number(quotaSurfacedGoalIds.has(b.goal.id)));
    for (const entry of activeGoalEntries) {
      const g = entry.goal;
      const run = g.linkedRunId ? this.getRunEntry(g.linkedRunId) : undefined;
      const openEsc = run ? run.escalations.filter((e) => e.status === "open").length : 0;
      const runningTasks = run
        ? run.tasks.filter(
            (t) => t.profileId !== "director" && (t.status === "running" || t.status === "draining" || t.status === "merging"),
          ).length
        : 0;
      const badge = openEsc > 0 ? `!${openEsc}` : runningTasks > 0 ? `${runningTasks}w` : g.status;
      pushItem({
        kind: "goal",
        id: g.id,
        status: openEsc > 0 ? "blocked" : g.status,
        title: g.title,
        badge,
        activity: this.describeGoalActivity(g, run),
        ref: { kind: "goal", id: g.id, runId: g.linkedRunId ?? undefined, label: `${g.id} ${g.title}` },
      });
    }

    // 3. Standalone active runs not bound to a goal
    for (const r of this.snapshot.runs) {
      if (r.goalId) continue;
      if (r.run.id.startsWith("director-tasks-")) continue;
      if (r.run.status !== "active" && r.run.status !== "paused_quota") continue;
      const running = r.tasks.filter(
        (t) => t.profileId !== "director" && (t.status === "running" || t.status === "draining" || t.status === "merging"),
      ).length;
      pushItem({
        kind: "run",
        id: r.run.id,
        status: r.run.status,
        title: (r.run.goal.split("\n")[0] ?? r.run.id).trim(),
        badge: running > 0 ? `${running}w` : r.run.status,
        activity: this.describeRunActivity(r) || r.run.status,
        ref: { kind: "run", id: r.run.id, label: r.run.id },
      });
    }

    // 4. Fallback: ad-hoc subagents spawned inside `director-tasks-*` when no formal goals/runs occupy the HUD
    if (hudItems.length === 0) {
      for (const r of this.snapshot.runs) {
        if (!r.run.id.startsWith("director-tasks-")) continue;
        if (r.run.status !== "active" && r.run.status !== "paused_quota") continue;
        for (const t of this.getPrioritizedRunTasks(r)) {
          pushItem({
            kind: "task",
            id: t.id,
            status: t.status,
            title: t.title,
            badge: `T${t.tier}/${t.kind}`,
            activity: t.currentAction ?? t.status,
            ref: { kind: "task", id: t.id, runId: r.run.id, label: `${t.id} ${t.title}` },
          });
        }
      }
    }

    // Secondary detail line when only 1 item is active so HUD row 2 shows live worker/criteria progress
    let hudSecondaryLine: string | null = null;
    if (hudItems.length === 1) {
      const only = hudItems[0]!;
      const run = only.ref.runId
        ? this.getRunEntry(only.ref.runId)
        : only.ref.kind === "run"
          ? this.getRunEntry(only.ref.id)
          : undefined;
      if (run) {
        const liveTasks = this.getPrioritizedRunTasks(run);
        if (liveTasks.length >= 2) {
          hudSecondaryLine = liveTasks
            .slice(0, 2)
            .map((t) => this.formatTaskBrief(t))
            .join(" · ");
        } else if (liveTasks.length === 1) {
          const brief = this.formatTaskBrief(liveTasks[0]!);
          if (!only.activity || !only.activity.includes(liveTasks[0]!.title)) {
            hudSecondaryLine = brief;
          }
        }
      }
      if (!hudSecondaryLine && only.kind === "goal") {
        const goal = this.getGoalEntry(only.id)?.goal;
        if (goal && goal.criteria.length > 0) {
          const passed = goal.liveProgress?.criteriaResults?.filter((r) => r.passed).length ?? 0;
          const cyclePart = goal.reviewCycle > 0 ? ` · review cycle ${goal.reviewCycle}` : "";
          hudSecondaryLine = `Criteria ${passed}/${goal.criteria.length}${cyclePart}`;
        }
      }
    }

    return {
      slots,
      agentCounts,
      hudItems,
      hudSecondaryLine,
      activeGoalsCount,
    };
  }
}

export type { ArtifactRecord, EscalationRecord, GoalRecord, MergeQueueRecord, MessageRecord, RunRecord, TaskRecord, TimerRecord, WorktreeRecord };
