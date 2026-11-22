import { decodeKittyPrintable, matchesKey, wrapTextWithAnsi, type Component } from "@earendil-works/pi-tui";
import type { GoalRecord } from "../goals/engine.js";
import type { RunQuotaPauseStatus } from "../control-plane/orchestrator.js";
import type { EscalationRecord, RunRecord, TaskRecord } from "../control-plane/db.js";
import type { RunActivitySummary } from "../control-plane/telemetry.js";
import type { DaemonEventFrame } from "../protocol/daemon-rpc.js";
import { formatResetCountdown } from "./overlays.js";
import { badge, fillLineBg, fitWidth, parseMouseWheel, type HarnessTheme, type SemanticTone } from "./primitives/index.js";

/** One entry of the `runs.trees` RPC result (contract.runs-watch-commands.v1). */
export interface RunsTreeEntry {
  run: RunRecord;
  /** ALL tasks of the run, any status, flat (hierarchy via parentId + tier). */
  tasks: TaskRecord[];
  /** Open escalations. */
  escalations: EscalationRecord[];
  quotaPause: RunQuotaPauseStatus | null;
  /** Automatically harvested live phase, file edits, and per-task actions (zero model calls). */
  activity?: RunActivitySummary | null;
}

export interface RunsTreesResult {
  generatedAt: number;
  runs: RunsTreeEntry[];
}

/** Live frame from a `watch.start` subscription; `watchId` is the optional extra field. */
export type WatchEventFrame = DaemonEventFrame & { watchId?: string };

export type RunsFilter = "running_recent" | "running" | "failed" | "all";

export const RUNS_FILTERS: readonly RunsFilter[] = ["running_recent", "running", "failed", "all"];

const FILTER_LABELS: Record<RunsFilter, string> = {
  running_recent: "Running+Recent",
  running: "Running",
  failed: "Failed",
  all: "All",
};

export const DEFAULT_RECENT_MS = 24 * 60 * 60 * 1000;

export interface RunsTreeRow {
  kind: "goal" | "run" | "task";
  /** Stable selection key: goal id for goal rows, run id for run rows, `runId/taskId` for task rows. */
  key: string;
  /** Owning run id; for a goal row, its linked run id (or "" while the goal has no run). */
  runId: string;
  /** Goal rows have no run entry when the goal is a draft or its run is filtered out. */
  entry?: RunsTreeEntry;
  task?: TaskRecord;
  goal?: GoalRecord;
  /** Tree depth: 0 for goals / standalone runs, 1 for tasks (or a goal's run), … */
  depth: number;
  /** Connector prefix (`│  ├─ ` …), empty for top-level rows. */
  connector: string;
  /** Open escalations raised by this task (task rows) or the whole run (run rows). */
  openEscalations: number;
  /** Goal and run rows only. */
  collapsed?: boolean;
  /** Run rows only: the run is the child of a goal row. */
  nested?: boolean;
}

/** Goal operations a goal row can request (same set the legacy Goals dashboard exposed). */
export type GoalsRunsGoalAction = "review" | "pause" | "cancel" | "inspect" | "print_table";

const RUNNING_GOAL_STATUSES = new Set<GoalRecord["status"]>(["executing", "verifying", "reviewing"]);

const ACTIVE_TASK_STATUSES = new Set<TaskRecord["status"]>([
  "running",
  "draining",
  "waiting_children",
  "merging",
  "blocked",
  "paused_quota",
]);

const LIVE_TASK_STATUSES = new Set<TaskRecord["status"]>(["running", "draining", "waiting_children", "merging"]);
const IN_FLIGHT_TASK_STATUSES = new Set<TaskRecord["status"]>([
  "pending",
  "ready",
  "running",
  "draining",
  "waiting_children",
  "merging",
]);

/** Dynamically derives a run's displayed status from live task states so it never contradicts active tasks. */
export function effectiveRunStatus(entry: RunsTreeEntry): RunRecord["status"] {
  const { run, tasks } = entry;
  if (tasks.some((t) => LIVE_TASK_STATUSES.has(t.status))) return "active";
  if (tasks.length === 0) return run.status;
  if (tasks.some((t) => t.status === "paused_quota")) return run.status === "stopped" ? "stopped" : "paused_quota";
  if (run.status === "stopped") return "stopped";
  if (tasks.some((t) => IN_FLIGHT_TASK_STATUSES.has(t.status))) return run.status;

  // All tasks are terminal — never keep displaying ACTIVE!
  const realOrch = tasks.find((t) => t.tier === 1 && t.profileId !== "director");
  if (realOrch) {
    if (realOrch.status === "failed") return "failed";
    if (realOrch.status === "cancelled") return "stopped";
    if (realOrch.status === "completed") return "completed";
  }
  const realTasks = tasks.filter((t) => t.profileId !== "director");
  if (realTasks.length === 0) return run.status === "active" ? "completed" : run.status;
  if (realTasks.some((t) => t.status === "failed")) return "failed";
  if (realTasks.every((t) => t.status === "cancelled")) return "stopped";
  return "completed";
}

function isActiveRun(entry: RunsTreeEntry): boolean {
  const st = effectiveRunStatus(entry);
  return st === "active" || st === "paused_quota";
}

function isFailedRun(entry: RunsTreeEntry): boolean {
  return effectiveRunStatus(entry) === "failed";
}

function taskKey(runId: string, taskId: string): string {
  return `${runId}/${taskId}`;
}

function openEscalationsByTask(entry: RunsTreeEntry): Map<string, number> {
  const m = new Map<string, number>();
  for (const e of entry.escalations) {
    if (e.status !== "open") continue;
    m.set(e.fromTaskId, (m.get(e.fromTaskId) ?? 0) + 1);
  }
  return m;
}

/** A goal counts as running while it executes/verifies/reviews or its linked run is still active. */
function isRunningGoal(goal: GoalRecord, entry: RunsTreeEntry | undefined): boolean {
  return RUNNING_GOAL_STATUSES.has(goal.status) || (entry ? isActiveRun(entry) : false);
}

/**
 * Default disclosure: running goals and runs open, everything else closed. Callers apply explicit
 * toggles on top, keyed by the goal / run id.
 */
export function defaultClosedKeys(trees: RunsTreesResult | RunsTreeEntry[], goals: readonly GoalRecord[] = []): Set<string> {
  const entries = Array.isArray(trees) ? trees : trees.runs;
  const byRun = new Map(entries.map((e) => [e.run.id, e]));
  const closed = new Set<string>();
  for (const e of entries) if (!isActiveRun(e)) closed.add(e.run.id);
  for (const g of goals) if (!isRunningGoal(g, g.linkedRunId ? byRun.get(g.linkedRunId) : undefined)) closed.add(g.id);
  return closed;
}

/**
 * Pure tree builder: applies filter + search, then emits Goal → Run → T1 → T2 → T3 DFS ordering
 * (children by parentId; tasks whose parent is missing attach to the run) with tree connectors.
 * A goal's `linkedRunId` run is nested under it; runs no goal links to stay top-level (standalone).
 * `collapsed` is the final set of closed goal / run ids.
 * `matched` counts goals + standalone runs shown, `hidden` counts those filtered out.
 */
export function buildRunsTreeRows(
  trees: RunsTreesResult | RunsTreeEntry[],
  opts: {
    filter: RunsFilter;
    query: string;
    collapsed: Set<string>;
    now: number;
    recentMs?: number;
    goals?: readonly GoalRecord[];
  },
): { rows: RunsTreeRow[]; matched: number; hidden: number } {
  const entries = Array.isArray(trees) ? trees : trees.runs;
  const goals = opts.goals ?? [];
  const recentCutoff = opts.now - (opts.recentMs ?? DEFAULT_RECENT_MS);
  const q = opts.query.trim().toLowerCase();
  const matches = (parts: string[]) => parts.some((s) => s.toLowerCase().includes(q));

  /** Rows for one run (header + tasks), or null when the filter / search hides it. */
  const runRows = (entry: RunsTreeEntry, nested: boolean, search: string): RunsTreeRow[] | null => {
    const { run } = entry;
    const escByTask = openEscalationsByTask(entry);

    const taskPasses = (t: TaskRecord): boolean => {
      switch (opts.filter) {
        case "all":
          return true;
        case "running":
          return ACTIVE_TASK_STATUSES.has(t.status);
        case "running_recent":
          return ACTIVE_TASK_STATUSES.has(t.status) || t.updatedAt >= recentCutoff;
        case "failed":
          return t.status === "failed" || t.status === "returned_for_rescope" || (escByTask.get(t.id) ?? 0) > 0;
      }
    };
    const runPasses = (): boolean => {
      switch (opts.filter) {
        case "all":
          return true;
        case "running":
          return isActiveRun(entry);
        case "running_recent":
          return isActiveRun(entry) || run.updatedAt >= recentCutoff;
        case "failed":
          return isFailedRun(entry) || entry.escalations.some((e) => e.status === "open");
      }
    };

    const filterTasks = entry.tasks.filter(taskPasses);
    if (!runPasses() && filterTasks.length === 0) return null;

    // Search: a run-level match keeps all filter-passing tasks; otherwise only matching tasks.
    let shownTasks = filterTasks;
    if (search) {
      const runMatch = [run.goal, run.id, effectiveRunStatus(entry)].some((s) => s.toLowerCase().includes(search));
      if (!runMatch) {
        shownTasks = filterTasks.filter((t) =>
          [t.id, t.title, t.status, t.model ?? "", t.profileId].some((s) => s.toLowerCase().includes(search)),
        );
        if (shownTasks.length === 0) return null;
      }
    }

    // Keep ancestors so the hierarchy stays intact.
    const byId = new Map(entry.tasks.map((t) => [t.id, t]));
    const visible = new Set<string>();
    for (const t of shownTasks) {
      let cur: TaskRecord | undefined = t;
      while (cur && !visible.has(cur.id)) {
        visible.add(cur.id);
        cur = cur.parentId ? byId.get(cur.parentId) : undefined;
      }
    }

    const rows: RunsTreeRow[] = [];
    const collapsed = opts.collapsed.has(run.id);
    rows.push({
      kind: "run",
      key: run.id,
      runId: run.id,
      entry,
      depth: nested ? 1 : 0,
      connector: nested ? "└─ " : "",
      openEscalations: entry.escalations.filter((e) => e.status === "open").length,
      collapsed,
      nested,
    });
    if (collapsed) return rows;

    const children = new Map<string | null, TaskRecord[]>();
    for (const t of entry.tasks) {
      if (!visible.has(t.id)) continue;
      const parent = t.parentId && visible.has(t.parentId) ? t.parentId : null;
      const list = children.get(parent) ?? [];
      list.push(t);
      children.set(parent, list);
    }
    for (const list of children.values()) {
      list.sort((a, b) => a.createdAt - b.createdAt || a.id.localeCompare(b.id));
    }
    const seen = new Set<string>();
    const visit = (parentId: string | null, depth: number, lead: string) => {
      const list = children.get(parentId) ?? [];
      list.forEach((t, i) => {
        if (seen.has(t.id)) return;
        seen.add(t.id);
        const last = i === list.length - 1;
        rows.push({
          kind: "task",
          key: taskKey(run.id, t.id),
          runId: run.id,
          entry,
          task: t,
          depth,
          connector: `${lead}${last ? "└─ " : "├─ "}`,
          openEscalations: escByTask.get(t.id) ?? 0,
        });
        visit(t.id, depth + 1, `${lead}${last ? "   " : "│  "}`);
      });
    };
    visit(null, nested ? 2 : 1, nested ? "   " : "");
    return rows;
  };

  const goalPasses = (g: GoalRecord, entry: RunsTreeEntry | undefined): boolean => {
    switch (opts.filter) {
      case "all":
        return true;
      case "running":
        return isRunningGoal(g, entry);
      case "running_recent":
        return isRunningGoal(g, entry) || g.updatedAt >= recentCutoff;
      case "failed":
        return (
          g.status === "awaiting_human" ||
          (entry ? isFailedRun(entry) || entry.escalations.some((e) => e.status === "open") : false)
        );
    }
  };

  const byRun = new Map(entries.map((e) => [e.run.id, e]));
  const linkedRunIds = new Set<string>();
  const nestedRunIds = new Set<string>();
  const rows: RunsTreeRow[] = [];
  let matched = 0;

  // Running items first; each group keeps the daemon's newest-first order.
  const runningFirst = <T,>(items: readonly T[], running: (item: T) => boolean): T[] => [
    ...items.filter(running),
    ...items.filter((i) => !running(i)),
  ];
  const orderedGoals = runningFirst(goals, (g) => isRunningGoal(g, g.linkedRunId ? byRun.get(g.linkedRunId) : undefined));
  for (const g of orderedGoals) {
    const entry = g.linkedRunId ? byRun.get(g.linkedRunId) : undefined;
    if (entry) linkedRunIds.add(entry.run.id);
    const goalMatch = !q || matches([g.id, g.title, g.status, g.description]);
    const own = goalPasses(g, entry) && goalMatch;
    // A matching goal keeps its whole run; otherwise the run must match the search itself.
    const childRows = entry && !nestedRunIds.has(entry.run.id) ? runRows(entry, true, own ? "" : q) : null;
    if (childRows) nestedRunIds.add(entry!.run.id);
    if (!own && !childRows) continue;
    matched++;
    rows.push({
      kind: "goal",
      key: g.id,
      runId: g.linkedRunId ?? "",
      ...(entry ? { entry } : {}),
      goal: g,
      depth: 0,
      connector: "",
      openEscalations: 0,
      collapsed: opts.collapsed.has(g.id),
    } as RunsTreeRow);
    if (childRows && !opts.collapsed.has(g.id)) rows.push(...childRows);
  }

  let standalone = 0;
  for (const entry of runningFirst(entries, isActiveRun)) {
    if (linkedRunIds.has(entry.run.id)) continue;
    standalone++;
    const r = runRows(entry, false, q);
    if (!r) continue;
    matched++;
    rows.push(...r);
  }

  return { rows, matched, hidden: goals.length + standalone - matched };
}

function taskTone(status: TaskRecord["status"]): SemanticTone {
  if (status === "completed") return "success";
  if (status === "failed" || status === "cancelled") return "error";
  if (status === "paused_quota" || status === "blocked" || status === "returned_for_rescope") return "warning";
  if (ACTIVE_TASK_STATUSES.has(status)) return "accent";
  return "neutral";
}

function runTone(status: RunRecord["status"]): SemanticTone {
  if (status === "completed") return "success";
  if (status === "failed" || status === "stopped") return "error";
  if (status === "paused_quota") return "warning";
  if (status === "active") return "accent";
  return "neutral";
}

export function tierLabel(task: TaskRecord): string {
  return task.kind === "verify" ? "Reviewer" : `T${task.tier}`;
}

function statusLabel(status: string): string {
  return status === "paused_quota" ? "PAUSED QUOTA" : status.replace(/_/g, " ").toUpperCase();
}

function firstLine(s: string): string {
  return s.split("\n")[0] ?? s;
}

export interface RunsTreeOverlayOptions {
  theme: HarnessTheme;
  fetchTrees: () => Promise<RunsTreesResult>;
  /** Goals to join with their linked runs (chat scope only; attached-run scope has none). Refreshed with every refetch. */
  fetchGoals?: () => Promise<GoalRecord[]>;
  /** Starts a `scope: 'runs'` watch and returns its stop function. */
  subscribe: (onEvent: (ev: WatchEventFrame) => void) => Promise<() => void>;
  onOpenTranscript: (row: {
    runId: string;
    taskId: string;
    title: string;
    model?: string | null;
    profileId?: string | null;
    effort?: string | null;
  }) => void;
  onAction: (action: "resume" | "wait" | "stop", runId: string) => void;
  /** Goal-row actions; the overlay closes after each so the resulting card is visible in the transcript. */
  onGoalAction?: (action: GoalsRunsGoalAction, goal: GoalRecord) => void;
  onClose: () => void;
  requestRender: () => void;
  /** Visible terminal lines of the list (defaults to the terminal height minus chrome). */
  viewportRows?: number;
  recentMs?: number;
  now?: () => number;
  /** Debounce for event-driven refetches (default 150ms). */
  refetchDebounceMs?: number;
  /** How often goals are re-read while open, since goal status changes are not run events (default 4s; 0 disables). */
  goalPollMs?: number;
}

/** Full run text for the expanded run body: control sequences stripped, paragraph breaks preserved. */
function runBodyText(text: string): string[] {
  const clean = text
    .replace(/\r\n?/g, "\n")
    .replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, "")
    .replace(/\t/g, "  ")
    .replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/g, "")
    .replace(/\s+$/, "")
    .replace(/^(?:[ \t]*\n)+/, "");
  return clean.split("\n");
}

/**
 * `/runs` and `/goals` overlay: one filterable Goal → Run → T1 → T2 → T3 tree with live updates.
 * Goals sit with their linked runs; runs no goal links to stay reachable as standalone rows.
 * Running items start open and the rest closed; explicit toggles (keyed by goal / run id) override that
 * default and survive refreshes. State (selection, filter, query, toggles, scroll) survives transcript
 * popups opened on top of it.
 */
export class RunsTreeOverlay implements Component {
  private trees: RunsTreesResult | null = null;
  private goals: GoalRecord[] = [];
  private loadError: string | null = null;
  private watchError: string | null = null;
  private lastIndex = 0;
  private ticker: ReturnType<typeof setInterval> | null = null;
  private goalPoll: ReturnType<typeof setInterval> | null = null;
  private filter: RunsFilter = "running_recent";
  private query = "";
  private searching = false;
  /** Explicit open (true) / closed (false) choices by goal or run id; absent means the running-status default. */
  private toggles = new Map<string, boolean>();
  private selectedKey: string | null = null;
  /** First visible physical line of the list. */
  private scrollTop = 0;
  /** True after the user scrolled lines directly; selection changes re-center the viewport again. */
  private manualScroll = false;
  private layout: { selStart: number; selEnd: number; total: number; vp: number } | null = null;
  private stopWatch: (() => void) | null = null;
  private refetchTimer: ReturnType<typeof setTimeout> | null = null;
  private disposed = false;
  private fetchSeq = 0;

  constructor(private readonly opts: RunsTreeOverlayOptions) {
    void this.refetch();
    const pollMs = this.opts.goalPollMs ?? 4000;
    if (this.opts.fetchGoals && pollMs > 0) this.goalPoll = setInterval(() => void this.refetch(), pollMs);
    void this.opts
      .subscribe((ev) => this.onEvent(ev))
      .then((stop) => {
        if (this.disposed) stop();
        else this.stopWatch = stop;
      })
      .catch((err) => {
        this.watchError = `Live updates unavailable: ${err instanceof Error ? err.message : String(err)}`;
        this.opts.requestRender();
      });
  }

  /** Current filter / search / selection state (for tests and integration). */
  get state(): {
    filter: RunsFilter;
    query: string;
    searching: boolean;
    selectedKey: string | null;
    /** Goal / run ids the user explicitly closed. */
    collapsed: string[];
    /** Goal / run ids the user explicitly opened. */
    expanded: string[];
  } {
    const keys = (open: boolean) => [...this.toggles].filter(([, v]) => v === open).map(([k]) => k);
    return {
      filter: this.filter,
      query: this.query,
      searching: this.searching,
      selectedKey: this.selectedKey,
      collapsed: keys(false),
      expanded: keys(true),
    };
  }

  async refetch(): Promise<void> {
    const seq = ++this.fetchSeq;
    try {
      const [trees, goals] = await Promise.all([
        this.opts.fetchTrees(),
        this.opts.fetchGoals ? this.opts.fetchGoals() : Promise.resolve<GoalRecord[]>([]),
      ]);
      if (this.disposed || seq !== this.fetchSeq) return;
      this.trees = trees;
      this.goals = goals;
      this.loadError = null;
      this.syncTicker();
    } catch (err) {
      if (this.disposed || seq !== this.fetchSeq) return;
      this.loadError = err instanceof Error ? err.message : String(err);
    }
    this.opts.requestRender();
  }

  /** Keeps quota countdowns ticking while any run is quota-paused. */
  private syncTicker(): void {
    const paused = this.trees?.runs.some((e) => e.quotaPause) ?? false;
    if (paused && !this.ticker) this.ticker = setInterval(() => this.opts.requestRender(), 1000);
    else if (!paused && this.ticker) {
      clearInterval(this.ticker);
      this.ticker = null;
    }
  }

  private onEvent(_ev: WatchEventFrame): void {
    if (this.disposed) return;
    if (this.refetchTimer) clearTimeout(this.refetchTimer);
    this.refetchTimer = setTimeout(() => {
      this.refetchTimer = null;
      void this.refetch();
    }, this.opts.refetchDebounceMs ?? 150);
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    if (this.refetchTimer) clearTimeout(this.refetchTimer);
    this.refetchTimer = null;
    if (this.ticker) clearInterval(this.ticker);
    this.ticker = null;
    if (this.goalPoll) clearInterval(this.goalPoll);
    this.goalPoll = null;
    const stop = this.stopWatch;
    this.stopWatch = null;
    stop?.();
  }

  /** Closed goal / run ids: the running-status default with the user's explicit toggles applied. */
  private closedKeys(): Set<string> {
    if (!this.trees) return new Set();
    const closed = defaultClosedKeys(this.trees, this.goals);
    for (const [key, open] of this.toggles) {
      if (open) closed.delete(key);
      else closed.add(key);
    }
    return closed;
  }

  private build(): { rows: RunsTreeRow[]; matched: number; hidden: number } {
    if (!this.trees) return { rows: [], matched: 0, hidden: 0 };
    return buildRunsTreeRows(this.trees, {
      filter: this.filter,
      query: this.query,
      collapsed: this.closedKeys(),
      now: this.opts.now?.() ?? Date.now(),
      recentMs: this.opts.recentMs,
      goals: this.goals,
    });
  }

  /** Resolves the selection by id; falls back to the nearest surviving row (or the first). */
  private selectedIndex(rows: RunsTreeRow[]): number {
    if (rows.length === 0) return -1;
    let idx = this.selectedKey ? rows.findIndex((r) => r.key === this.selectedKey) : -1;
    if (idx === -1 && this.selectedKey) {
      const runId = this.selectedKey.split("/")[0];
      idx = rows.findIndex((r) => r.kind === "run" && r.runId === runId);
    }
    if (idx === -1) idx = Math.min(this.lastIndex, rows.length - 1);
    this.lastIndex = idx;
    return idx;
  }

  private select(rows: RunsTreeRow[], i: number): void {
    this.lastIndex = Math.max(0, Math.min(rows.length - 1, i));
    this.selectedKey = rows[this.lastIndex]!.key;
    this.manualScroll = false;
  }

  private viewportRows(): number {
    return this.opts.viewportRows ?? Math.max(6, (process.stdout.rows ?? 30) - 12);
  }

  private scrollBy(lines: number): void {
    const max = Math.max(0, (this.layout?.total ?? 0) - (this.layout?.vp ?? 0));
    this.scrollTop = Math.max(0, Math.min(max, this.scrollTop + lines));
    this.manualScroll = true;
  }

  /** Down: first scroll through a selected row taller than the viewport, then move to the next row. */
  private moveDown(rows: RunsTreeRow[], idx: number): void {
    const L = this.layout;
    if (L && L.selEnd > this.scrollTop + L.vp) this.scrollBy(1);
    else if (rows.length) this.select(rows, idx + 1);
  }

  private moveUp(rows: RunsTreeRow[], idx: number): void {
    const L = this.layout;
    if (L && this.scrollTop > L.selStart && this.scrollTop < L.selEnd) this.scrollBy(-1);
    else if (rows.length) this.select(rows, idx - 1);
  }

  handleInput(data: string): void {
    const mouse = parseMouseWheel(data, 1);
    if (mouse.isMouse) {
      if (mouse.deltaLines !== 0) {
        const { rows } = this.build();
        if (rows.length > 0) {
          for (let n = Math.abs(mouse.deltaLines); n > 0; n--) {
            const idx = this.selectedIndex(rows);
            if (mouse.deltaLines > 0) this.moveDown(rows, idx);
            else this.moveUp(rows, idx);
          }
          this.opts.requestRender();
        }
      }
      return;
    }
    if (this.searching) {
      this.handleSearchInput(data);
      return;
    }
    const { rows } = this.build();
    const idx = this.selectedIndex(rows);
    const row = idx >= 0 ? rows[idx] : undefined;

    if (matchesKey(data, "escape")) {
      if (this.query) {
        this.query = "";
        this.opts.requestRender();
      } else {
        this.dispose();
        this.opts.onClose();
      }
      return;
    }
    if (matchesKey(data, "up") || data === "k") {
      this.moveUp(rows, idx);
    } else if (matchesKey(data, "down") || data === "j") {
      this.moveDown(rows, idx);
    } else if (matchesKey(data, "pageUp")) {
      this.scrollBy(-Math.max(1, (this.layout?.vp ?? this.viewportRows()) - 1));
    } else if (matchesKey(data, "pageDown")) {
      this.scrollBy(Math.max(1, (this.layout?.vp ?? this.viewportRows()) - 1));
    } else if (matchesKey(data, "home")) {
      this.scrollBy(-Infinity);
    } else if (matchesKey(data, "end")) {
      this.scrollBy(Infinity);
    } else if (matchesKey(data, "tab") || data === "f") {
      this.filter = RUNS_FILTERS[(RUNS_FILTERS.indexOf(this.filter) + 1) % RUNS_FILTERS.length]!;
    } else if (data.length === 1 && data >= "1" && data <= "4") {
      this.filter = RUNS_FILTERS[Number(data) - 1]!;
    } else if (data === "/") {
      this.searching = true;
    } else if (matchesKey(data, "space") || matchesKey(data, "left") || matchesKey(data, "right")) {
      if (!row) return;
      // Agent rows fold their run; goal and run rows fold themselves.
      const target = row.kind === "goal" ? row.key : row.runId;
      if (!target || (row.kind === "goal" && !row.entry)) return;
      const closed = this.closedKeys().has(target);
      const collapse = matchesKey(data, "left") ? true : matchesKey(data, "right") ? false : !closed;
      this.toggles.set(target, !collapse);
      if (collapse && row.kind === "task") this.selectedKey = target;
    } else if (matchesKey(data, "enter")) {
      if (!row) return;
      if (row.kind === "goal") return this.goalAction("inspect", row);
      if (row.kind === "task" && row.task) {
        this.opts.onOpenTranscript({
          runId: row.runId,
          taskId: row.task.id,
          title: row.task.title,
          model: row.task.model,
          profileId: row.task.profileId,
          effort: row.task.effort,
        });
        return;
      }
      this.toggles.set(row.runId, this.closedKeys().has(row.runId));
    } else if (row?.kind === "goal" && (data === "r" || data === "p" || data === "x" || data === "l" || data === "t")) {
      return this.goalAction(data === "r" ? "review" : data === "p" ? "pause" : data === "x" ? "cancel" : "print_table", row);
    } else if (data === "r" || data === "w" || data === "x") {
      // On a goal row only `w` reaches here (its linked run); draft goals have no run.
      if (!row?.runId) return;
      this.opts.onAction(data === "r" ? "resume" : data === "w" ? "wait" : "stop", row.runId);
      return;
    } else {
      return;
    }
    this.opts.requestRender();
  }

  private goalAction(action: GoalsRunsGoalAction, row: RunsTreeRow): void {
    if (!row.goal || !this.opts.onGoalAction) return;
    this.opts.onGoalAction(action, row.goal);
    this.dispose();
    this.opts.onClose();
  }

  private handleSearchInput(data: string): void {
    if (matchesKey(data, "escape")) {
      this.query = "";
      this.searching = false;
    } else if (matchesKey(data, "enter") || matchesKey(data, "down") || matchesKey(data, "tab")) {
      this.searching = false;
    } else if (matchesKey(data, "backspace")) {
      this.query = this.query.slice(0, -1);
    } else {
      const ch = decodeKittyPrintable(data) ?? data;
      // Accept printable text (incl. pastes); ignore control / escape sequences.
      if (!ch || /[\x00-\x1f\x7f]/.test(ch)) return;
      this.query += ch;
    }
    this.opts.requestRender();
  }

  invalidate(): void {}

  render(width: number): readonly string[] {
    const t = this.opts.theme;
    const lines: string[] = [];
    const tabs = RUNS_FILTERS.map((f, i) => {
      const label = `${i + 1} ${FILTER_LABELS[f]}`;
      return f === this.filter ? t.accentBold(`[${label}]`) : t.muted(` ${label} `);
    }).join(" ");
    lines.push(fitWidth(tabs, width));

    const { rows, matched, hidden } = this.build();
    const counts = t.dim(`${matched} matched · ${hidden} hidden`);
    const search = this.searching
      ? `${t.accentBold("/")} ${this.query}${t.accent("▏")}`
      : this.query
        ? `${t.accentBold("/")} ${this.query} ${t.dim("(Esc clears)")}`
        : t.dim("/ search");
    lines.push(fitWidth(`${search}  ${counts}`, width));
    lines.push(t.border("─".repeat(Math.max(0, width))));

    if (!this.trees) {
      lines.push(fitWidth(this.loadError ? t.error(`Failed to load runs: ${this.loadError}`) : t.muted("Loading runs…"), width));
      return lines;
    }
    if (this.loadError) lines.push(fitWidth(t.warning(`Refresh failed: ${this.loadError}`), width));
    if (this.watchError) lines.push(fitWidth(t.warning(this.watchError), width));
    if (rows.length === 0) {
      lines.push(
        fitWidth(
          t.muted(
            this.trees.runs.length === 0 && this.goals.length === 0
              ? this.opts.fetchGoals
                ? "No goals or runs yet. Chat with the Director to design & launch a Goal."
                : "No autonomous runs yet. Launch a Goal in Director Chat or via `pi-harness run`."
              : "Nothing matches this filter. Press 4 for All or Esc to clear search.",
          ),
          width,
        ),
      );
      return lines;
    }

    const idx = this.selectedIndex(rows);
    if (idx >= 0) this.selectedKey = rows[idx]!.key;
    const flat: string[] = [];
    let selStart = 0;
    let selEnd = 0;
    rows.forEach((row, i) => {
      const block = this.renderBlock(row, i === idx, width);
      if (i === idx) {
        selStart = flat.length;
        selEnd = flat.length + block.length;
      }
      flat.push(...block);
    });

    const vp = this.viewportRows();
    // Keep the selected row in view unless the user scrolled lines; a row taller than the viewport shows its top.
    if (!this.manualScroll) {
      if (selStart < this.scrollTop) this.scrollTop = selStart;
      else if (selEnd > this.scrollTop + vp) this.scrollTop = Math.min(selStart, selEnd - vp);
    }
    this.scrollTop = Math.max(0, Math.min(this.scrollTop, Math.max(0, flat.length - vp)));
    this.layout = { selStart, selEnd, total: flat.length, vp };
    lines.push(...flat.slice(this.scrollTop, this.scrollTop + vp));
    if (flat.length > vp) {
      const last = Math.min(flat.length, this.scrollTop + vp);
      lines.push(fitWidth(t.dim(`  ${idx + 1}/${rows.length} rows · lines ${this.scrollTop + 1}-${last}/${flat.length}`), width));
    }
    for (const l of wrapTextWithAnsi(t.dim(this.hint(rows[idx]!)), Math.max(1, width))) lines.push(fitWidth(l, width));
    return lines;
  }

  private hint(row: RunsTreeRow): string {
    const nav = "↑↓ move/scroll · PgUp/PgDn scroll · Space/←→ fold · ";
    const tail = "Tab/f/1-4 filter · / search · Esc close";
    if (row.kind === "goal") return `${nav}Enter card · l table · r review · p pause · x cancel · w wait · ${tail}`;
    const enter = row.kind === "task" ? "transcript" : "fold";
    return `${nav}Enter ${enter} · r resume · w wait · x stop · ${tail}`;
  }

  /** Physical lines of one row: the header line, plus the full wrapped text for an expanded run. */
  private renderBlock(row: RunsTreeRow, active: boolean, width: number): string[] {
    const header = this.renderRow(row, active);
    const out = [active ? fillLineBg(header, width, this.opts.theme.badgeBg) : fitWidth(header, width)];
    if (row.kind === "run" && !row.collapsed) {
      const t = this.opts.theme;
      const gutter = `  ${row.nested ? "   " : ""}  `;
      const bar = t.dim("│ ");
      const inner = Math.max(1, width - gutter.length - 2);
      for (const para of runBodyText(row.entry!.run.goal)) {
        const wrapped = para.trim() === "" ? [""] : wrapTextWithAnsi(para, inner);
        for (const w of wrapped) out.push(fitWidth(`${gutter}${bar}${w}`, width));
      }
    }
    return out;
  }

  private renderRow(row: RunsTreeRow, active: boolean): string {
    const t = this.opts.theme;
    const marker = active ? t.accentBold("▸ ") : "  ";
    const id = (s: string) => (active ? t.accentBold(s) : t.bold(s));
    const esc = row.openEscalations > 0 ? ` ${badge(`⚑${row.openEscalations}`, "warning", t)}` : "";

    if (row.kind === "goal") {
      const g = row.goal!;
      const fold = row.entry ? (row.collapsed ? "▸" : "▾") : "·";
      const tone: SemanticTone =
        g.status === "achieved"
          ? "success"
          : g.status === "awaiting_human" || g.status === "cancelled"
            ? "error"
            : RUNNING_GOAL_STATUSES.has(g.status)
              ? "accent"
              : "neutral";
      const passed = g.liveProgress?.criteriaResults?.filter((r) => r.passed).length;
      const crit = passed !== undefined && g.criteria.length > 0 ? `${passed}/${g.criteria.length}` : String(g.criteria.length);
      const run = g.linkedRunId ? "" : " · draft";
      return `${marker}${t.muted(fold)} ${badge("GOAL", "info", t)} ${badge(g.status.replace(/_/g, " ").toUpperCase(), tone, t)} ${id(`${g.id} v${g.version}`)} ${g.title} ${t.dim(`(${crit} criteria${run})`)}`;
    }

    const entry = row.entry!;
    const qp = entry.quotaPause;
    if (row.kind === "run") {
      const run = entry.run;
      const effStatus = effectiveRunStatus(entry);
      const fold = row.collapsed ? "▸" : "▾";
      const n = entry.tasks.length;
      const act = entry.activity;
      const primaryTask = entry.tasks.find((x) => x.tier === 1) ?? entry.tasks[0];
      const modelTag = primaryTask?.model
        ? ` · ${primaryTask.model}${primaryTask.effort ? ` · ${primaryTask.effort}` : ""}`
        : "";
      const phaseTag =
        effStatus === "active" && act?.phase && !["active", "idle", "completed", "failed", "stopped"].includes(act.phase)
          ? ` ${t.accent(act.phase)}`
          : "";
      const editsTag = act && act.filesEdited > 0 ? ` · ${act.filesEdited}f` : "";
      const quota = qp
        ? ` ${badge(`⏸ ${qp.resumeAt ? `resume ${formatResetCountdown(qp.resumeAt)}` : "quota paused"}`, "warning", t)}`
        : "";
      // An expanded run prints its full text in the body below, so the header carries no goal text.
      const title = row.collapsed ? ` ${firstLine(run.goal)}` : "";
      return `${marker}${t.dim(row.connector)}${t.muted(fold)} ${badge("RUN", "info", t)} ${badge(statusLabel(effStatus), runTone(effStatus), t)} ${id(run.id)}${title} ${t.dim(`(${n} agent${n === 1 ? "" : "s"}${modelTag}${editsTag})`)}${phaseTag}${esc}${quota}`;
    }

    const task = row.task!;
    const tier = tierLabel(task);
    const tierBadge = badge(tier, task.kind === "verify" ? "info" : "accent", t);
    const ctx = task.contextSize > 0 ? ` ${Math.round((task.contextUsed / task.contextSize) * 100)}%` : "";
    const effortTag = task.effort ? ` · ${task.effort}` : "";
    const model = task.model ? `${task.model}${effortTag} · ${task.profileId}` : `${task.profileId}${effortTag}`;
    const taskAct = entry.activity?.tasks.find((x) => x.id === task.id);
    const liveAction =
      taskAct?.currentAction && LIVE_TASK_STATUSES.has(task.status) ? ` ${t.muted(taskAct.currentAction)}` : "";
    const quota =
      task.status === "paused_quota" && qp?.resumeAt ? ` ${t.warning(`⏱ ${formatResetCountdown(qp.resumeAt)}`)}` : "";
    return `${marker}${t.dim(row.connector)}${tierBadge} ${badge(statusLabel(task.status), taskTone(task.status), t)} ${id(task.id)} ${task.title} ${t.dim(`(${model}${ctx})`)}${liveAction}${esc}${quota}`;
  }
}
