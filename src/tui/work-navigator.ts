import { matchesKey } from "@earendil-works/pi-tui";
import type { InboxItem } from "../protocol/daemon-rpc.js";
import {
  clamp,
  type ColumnSpec,
  type TuiEntityRef as EntityRef,
  firstLine,
  formatAge,
  formatCountdown,
  Frame,
  type FrameTab,
  handleTabKeys,
  type Hint,
  isPopKey,
  middleEllipsis,
  type Navigator,
  sanitizeLine,
  type Screen,
  type ScreenContext,
  statusBadge,
  TableListView,
  tag,
  type Theme,
} from "./core/index.js";
import { type InspectorDeps, openEntityInspector } from "./inspectors.js";
import { QuotaWaitPickerScreen } from "./quota-wait-picker.js";

export type WorkTabId = "inbox" | "goals" | "runs" | "escalations" | "timers" | "worktrees";

const TAB_ORDER: readonly WorkTabId[] = ["inbox", "goals", "runs", "escalations", "timers", "worktrees"];

const TAB_COLUMNS: Record<WorkTabId, ColumnSpec[]> = {
  inbox: [
    { key: "sev", header: "SEV", width: 5, priority: 0 },
    { key: "kind", header: "KIND", width: 12, priority: 2 },
    { key: "title", header: "TITLE", minWidth: 26, priority: 0 },
    { key: "detail", header: "DETAIL", minWidth: 16, priority: 3 },
    { key: "age", header: "AGE", width: 8, align: "right", priority: 1 },
  ],
  goals: [
    { key: "status", header: "STATUS", width: 16, priority: 0 },
    { key: "goal", header: "GOAL", width: 12, priority: 0 },
    { key: "title", header: "TITLE", minWidth: 24, priority: 0 },
    { key: "activity", header: "ACTIVITY", width: 18, priority: 1 },
    { key: "updated", header: "UPDATED", width: 8, align: "right", priority: 2 },
  ],
  runs: [
    { key: "status", header: "STATUS", width: 15, priority: 0 },
    { key: "run", header: "RUN", width: 12, priority: 0 },
    { key: "goal", header: "GOAL", minWidth: 24, priority: 0 },
    { key: "tasks", header: "TASKS", width: 14, priority: 1 },
    { key: "updated", header: "UPDATED", width: 8, align: "right", priority: 2 },
  ],
  escalations: [
    { key: "status", header: "STATUS", width: 11, priority: 0 },
    { key: "sev", header: "SEV", width: 5, priority: 0 },
    { key: "id", header: "ID", width: 12, priority: 0 },
    { key: "title", header: "TITLE", minWidth: 24, priority: 0 },
    { key: "run", header: "RUN", width: 12, priority: 2 },
    { key: "age", header: "AGE", width: 8, align: "right", priority: 1 },
  ],
  timers: [
    { key: "status", header: "STATUS", width: 11, priority: 0 },
    { key: "id", header: "TIMER", width: 12, priority: 0 },
    { key: "owner", header: "OWNER", width: 14, priority: 2 },
    { key: "reason", header: "REASON", minWidth: 24, priority: 0 },
    { key: "wake", header: "WAKE", width: 12, align: "right", priority: 1 },
  ],
  worktrees: [
    { key: "status", header: "STATUS", width: 12, priority: 0 },
    { key: "kind", header: "KIND", width: 7, priority: 1 },
    { key: "id", header: "ID", width: 14, priority: 0 },
    { key: "branch", header: "BRANCH", minWidth: 22, priority: 0 },
    { key: "meta", header: "TASK / STATE", width: 18, align: "right", priority: 2 },
  ],
};

interface WorkRowValue {
  ref: EntityRef;
  quickResolveEscId?: string;
  quotaRunId?: string;
  quotaReason?: string | null;
  quotaResumeAt?: number | null;
}

/**
 * Unified Work Navigator (`Ctrl+G`, `Ctrl+T` for Runs).
 * Surfaces every object in the harness across 6 aligned-table tabs with Left/Right + Tab + 1-6 navigation,
 * live badges, store error visibility, quick-resolve, quota wait picker, and Enter drill-down.
 */
export class WorkNavigatorScreen implements Screen {
  readonly variant = "sheet" as const;
  readonly title = "Work";
  private readonly frame = new Frame();
  private nav: Navigator | null = null;
  private readonly list: TableListView<WorkRowValue>;
  private activeTab: WorkTabId;
  private unresolvedEscalationsOnly = true;
  private unsub: (() => void) | null = null;

  constructor(
    private readonly deps: InspectorDeps,
    initialTab?: WorkTabId,
  ) {
    this.activeTab =
      initialTab ?? (deps.store.state.inbox.length > 0 ? "inbox" : "goals");
    this.list = new TableListView<WorkRowValue>({ columns: TAB_COLUMNS[this.activeTab] });
  }

  onShow(ctx: ScreenContext): void {
    this.nav = ctx.navigator;
    this.unsub = this.deps.store.subscribe(() => {
      this.rebuildList();
      ctx.requestRender();
    });
    this.rebuildList();
    void this.deps.store.refresh();
  }

  onClose(): void {
    this.unsub?.();
    this.unsub = null;
  }

  setTab(tab: WorkTabId): void {
    this.activeTab = tab;
    this.list.setColumns(TAB_COLUMNS[tab]);
    this.list.clearFilter();
    this.rebuildList();
  }

  get currentTab(): WorkTabId {
    return this.activeTab;
  }

  private rebuildList(): void {
    const snap = this.deps.store.state;
    this.list.setColumns(TAB_COLUMNS[this.activeTab]);

    if (this.activeTab === "inbox") {
      this.list.emptyText = this.deps.store.lastError
        ? "Unable to load inbox — press 'r' to retry."
        : "Inbox clear — no escalations, paused runs, or pending decisions.";
      this.list.setTableItems(
        snap.inbox.map((item: InboxItem) => {
          const runEntry = item.runId ? this.deps.store.getRunEntry(item.runId) : undefined;
          const cleanTitle = sanitizeLine(item.title).replace(/^\[P[0-2]\/[^\]]+\]\s*/i, "");
          const kindLabel = item.kind.replace(/_/g, " ");
          return {
            id: item.id,
            value: {
              ref: {
                kind: item.ref.kind,
                id: item.ref.id,
                runId: item.ref.runId,
                goalId: item.ref.goalId,
                label: item.ref.label ?? item.title,
              },
              quickResolveEscId: item.kind === "escalation" ? item.ref.id : undefined,
              quotaRunId: item.kind === "quota_paused" ? (item.runId ?? item.ref.id) : undefined,
              quotaReason: runEntry?.quotaPause?.reason ?? item.detail,
              quotaResumeAt: runEntry?.quotaPause?.resumeAt ?? null,
            },
            searchText: `${item.severity} ${item.kind} ${item.title} ${item.ask ?? item.detail} ${item.goalId ?? ""} ${item.runId ?? ""}`,
            cells: (sel, theme) => {
              const sevTone = item.severity === "P0" ? "danger" : item.severity === "P1" ? "warn" : "info";
              return {
                sev: tag(item.severity, sevTone, theme),
                kind: theme.muted(kindLabel),
                title: sel ? theme.accentBold(cleanTitle) : theme.bold(cleanTitle),
                detail: theme.muted(firstLine(item.ask ?? item.detail, 64)),
                age: theme.dim(formatAge(item.createdAt)),
              };
            },
          };
        }),
      );
      return;
    }

    if (this.activeTab === "goals") {
      this.list.emptyText = "No goals yet. Ask the Director to plan work or create a goal.";
      this.list.setTableItems(
        snap.goals.map(({ goal, reviews }) => {
          const run = goal.linkedRunId ? this.deps.store.getRunEntry(goal.linkedRunId) : undefined;
          const openEsc = run ? run.escalations.filter((e) => e.status === "open" || e.status === "acked").length : 0;
          const running = run ? run.tasks.filter((t) => t.status === "running").length : 0;
          return {
            id: goal.id,
            value: {
              ref: { kind: "goal", id: goal.id, runId: goal.linkedRunId ?? undefined, label: `${goal.id} ${goal.title}` },
            },
            searchText: `${goal.id} ${goal.status} ${goal.title} ${goal.linkedRunId ?? ""}`,
            cells: (sel, theme) => {
              const actParts: string[] = [];
              if (openEsc > 0) actParts.push(tag(`!${openEsc} esc`, "danger", theme));
              if (running > 0) actParts.push(tag(`${running}w`, "accent", theme));
              if (reviews.length > 0) actParts.push(theme.dim(`r#${reviews.length}`));
              if (goal.status === "draft" && goal.liveProgress?.phase === "queued") {
                const position = goal.liveProgress.summary?.match(/#\d+/)?.[0];
                actParts.push(tag(position ? `queued ${position}` : "queued", "info", theme));
              }
              return {
                status: statusBadge(goal.status, theme),
                goal: `${sel ? theme.accentBold(goal.id) : theme.bold(goal.id)} ${theme.dim(`v${goal.version}`)}`,
                title: sel ? theme.text(sanitizeLine(goal.title)) : sanitizeLine(goal.title),
                activity: actParts.length > 0 ? actParts.join(" ") : theme.dim("—"),
                updated: theme.dim(formatAge(goal.updatedAt)),
              };
            },
          };
        }),
      );
      return;
    }

    if (this.activeTab === "runs") {
      this.list.emptyText = "No runs recorded.";
      this.list.setTableItems(
        snap.runs.map((entry) => {
          const r = entry.run;
          const running = entry.tasks.filter((t) => t.status === "running").length;
          const openEsc = entry.escalations.filter((e) => e.status === "open" || e.status === "acked").length;
          const isQuotaPaused = r.status === "paused_quota" || Boolean(entry.quotaPause);
          return {
            id: r.id,
            value: {
              ref: { kind: "run", id: r.id, goalId: entry.goalId ?? undefined, label: r.id },
              quotaRunId: isQuotaPaused ? r.id : undefined,
              quotaReason: entry.quotaPause?.reason ?? null,
              quotaResumeAt: entry.quotaPause?.resumeAt ?? null,
            },
            searchText: `${r.id} ${r.status} ${entry.goalId ?? ""} ${r.goal}`,
            cells: (sel, theme) => {
              const goalPrefix = entry.goalId ? `${tag(entry.goalId, "info", theme)} ` : "";
              const taskParts = [`${running}/${entry.tasks.length}`];
              if (openEsc > 0) taskParts.push(tag(`!${openEsc}`, "danger", theme));
              if (entry.quotaPause?.resumeAt) taskParts.push(theme.warn(formatCountdown(entry.quotaPause.resumeAt)));
              return {
                status: statusBadge(r.status, theme),
                run: sel ? theme.accentBold(r.id) : theme.bold(r.id),
                goal: `${goalPrefix}${firstLine(r.goal, 64)}`,
                tasks: taskParts.join(" "),
                updated: theme.dim(formatAge(r.updatedAt)),
              };
            },
          };
        }),
      );
      return;
    }

    if (this.activeTab === "escalations") {
      // Default shows all UNRESOLVED escalations (`open` + `acked`) so acknowledged items never vanish!
      const items = this.unresolvedEscalationsOnly
        ? snap.escalations.filter((e) => e.status !== "resolved")
        : snap.escalations;
      this.list.emptyText = this.unresolvedEscalationsOnly
        ? "No unresolved escalations. Press 'a' to include resolved escalations."
        : "No escalations recorded.";
      this.list.setTableItems(
        items.map((e) => ({
          id: e.id,
          value: {
            ref: { kind: "escalation", id: e.id, runId: e.runId, label: `${e.id} ${e.title}` },
            quickResolveEscId: e.status !== "resolved" ? e.id : undefined,
          },
          searchText: `${e.id} ${e.severity} ${e.category} ${e.status} ${e.title} ${e.runId} ${e.fromTaskId}`,
          cells: (sel, theme) => {
            const sevTone = e.severity === "P0" ? "danger" : e.severity === "P1" ? "warn" : "info";
            const blk = e.blocking ? `${tag("BLK", "danger", theme)} ` : "";
            return {
              status: statusBadge(e.status, theme),
              sev: tag(e.severity, sevTone, theme),
              id: sel ? theme.accentBold(e.id) : theme.bold(e.id),
              title: `${blk}${sanitizeLine(e.title)}`,
              run: theme.muted(e.runId),
              age: theme.dim(formatAge(e.createdAt)),
            };
          },
        })),
      );
      return;
    }

    if (this.activeTab === "timers") {
      this.list.emptyText = "No timers scheduled.";
      this.list.setTableItems(
        snap.timers.map((t) => ({
          id: t.id,
          value: { ref: { kind: "timer", id: t.id, runId: t.runId ?? undefined, label: `${t.id} ${t.reason}` } },
          searchText: `${t.id} ${t.status} ${t.reason} ${t.ownerId} ${t.runId ?? ""}`,
          cells: (sel, theme) => ({
            status: statusBadge(t.status, theme),
            id: sel ? theme.accentBold(t.id) : theme.bold(t.id),
            owner: theme.muted(`${t.ownerKind}:${t.ownerId}`),
            reason: firstLine(t.reason, 64),
            wake: t.status === "pending" ? theme.accent(formatCountdown(t.wakeAt)) : theme.dim(t.status),
          }),
        })),
      );
      return;
    }

    // worktrees + merges tab using normalized SnapshotWorktreeEntry fields
    this.list.emptyText = "No active worktrees or merge queue items.";
    this.list.setTableItems([
      ...snap.worktrees.map((wt) => {
        const wtKey = wt.id ?? wt.path;
        return {
          id: `wt:${wtKey}`,
          value: {
            ref: {
              kind: "worktree" as const,
              id: wtKey,
              runId: wt.runId ?? undefined,
              label: `${wt.id ?? wt.branch} ${wt.branch}`,
            },
          },
          searchText: `worktree ${wt.id ?? ""} ${wt.branch} ${wt.status} ${wt.runId ?? ""} ${wt.taskId ?? ""} ${wt.dirty ? "dirty" : ""} ${wt.orphaned ? "orphaned" : ""}`,
          cells: (sel: boolean, theme: Theme) => {
            const flags: string[] = [];
            if (wt.taskId) flags.push(wt.taskId);
            if (wt.dirty) flags.push(theme.warn("dirty"));
            if (wt.orphaned) flags.push(theme.danger("orphan"));
            if (!wt.existsOnDisk) flags.push(theme.dim("missing"));
            return {
              status: statusBadge(wt.status, theme),
              kind: tag("wt", "info", theme),
              id: sel ? theme.accentBold(wt.id ?? "(untracked)") : theme.bold(wt.id ?? "(untracked)"),
              branch: middleEllipsis(wt.branch, 40),
              meta: flags.length > 0 ? flags.join(" ") : theme.dim("clean"),
            };
          },
        };
      }),
      ...snap.merges.map((m) => ({
        id: `merge:${m.id}`,
        value: { ref: { kind: "merge" as const, id: m.id, runId: m.runId, label: `${m.id} ${m.branch}` } },
        searchText: `merge ${m.id} ${m.branch} ${m.status} ${m.runId} ${m.taskId}`,
        cells: (sel: boolean, theme: Theme) => ({
          status: statusBadge(m.status, theme),
          kind: tag("merge", "accent", theme),
          id: sel ? theme.accentBold(m.id) : theme.bold(m.id),
          branch: middleEllipsis(m.branch, 40),
          meta: theme.dim(formatAge(m.finishedAt ?? m.createdAt)),
        }),
      })),
    ]);
  }

  render(width: number, maxHeight: number, theme: Theme): string[] {
    const snap = this.deps.store.state;
    const unresolvedEsc = this.deps.store.unresolvedEscalationsCount;
    const tabs: FrameTab[] = [
      { id: "inbox", label: "Inbox", count: snap.inbox.length, tone: snap.inbox.length > 0 ? "danger" : undefined },
      { id: "goals", label: "Goals", count: snap.goals.length },
      { id: "runs", label: "Runs", count: snap.runs.length },
      { id: "escalations", label: "Escalations", count: unresolvedEsc, tone: unresolvedEsc > 0 ? "danger" : undefined },
      { id: "timers", label: "Timers", count: this.deps.store.pendingTimersCount },
      { id: "worktrees", label: "Worktrees", count: snap.worktrees.length + snap.merges.length },
    ];

    const sel = this.list.selectedItem()?.value;
    const hints: Hint[] = [
      { key: "Enter", label: "inspect" },
      { key: "←→/Tab/1-6", label: "tabs" },
      { key: "/", label: "filter" },
      ...(sel?.quickResolveEscId ? [{ key: "R", label: "resolve" }] : []),
      ...(sel?.quotaRunId ? [{ key: "w", label: "wait/resume" }] : []),
      ...(this.activeTab === "escalations"
        ? [{ key: "a", label: this.unresolvedEscalationsOnly ? "show resolved" : "unresolved only" }]
        : []),
      ...(this.activeTab === "worktrees" ? [{ key: "C", label: "sweep stale" }] : []),
      { key: "r", label: "refresh" },
      { key: "Esc", label: "close" },
    ];

    return this.frame.render({
      width,
      maxHeight,
      theme,
      breadcrumbs: this.nav?.breadcrumbs ?? ["Work"],
      rightHeader: this.deps.store.lastError
        ? theme.danger(`${theme.glyphs.warn} sync error`)
        : snap.generatedAt > 0
          ? theme.muted(`updated ${formatAge(snap.generatedAt)}`)
          : undefined,
      tabs,
      activeTabId: this.activeTab,
      hints,
      renderBody: (innerW, bodyH) => {
        const out: string[] = [];
        let listH = bodyH;
        if (this.deps.store.lastError) {
          out.push(
            clamp(
              theme.danger(`${theme.glyphs.warn} Failed to refresh work snapshot: ${sanitizeLine(this.deps.store.lastError)} (press 'r' to retry)`),
              innerW,
            ),
          );
          listH = Math.max(1, bodyH - 1);
        }
        out.push(...this.list.render(innerW, listH, theme));
        return out;
      },
    });
  }

  handleInput(data: string, ctx: ScreenContext): void {
    if (this.frame.handleInlineInput(data, ctx.requestRender)) return;

    // When filtering, let ListView consume Escape / Left / Right / typing first!
    if (this.list.isFiltering) {
      const res = this.list.handleInput(data);
      if (res === "select") {
        const sel = this.list.selectedItem();
        if (sel) openEntityInspector(sel.value.ref, this.deps, ctx);
        return;
      }
      ctx.requestRender();
      return;
    }

    // Layered Escape: clear persisted filter query before popping screen
    if (matchesKey(data, "escape") && this.list.handleEscape()) {
      ctx.requestRender();
      return;
    }

    if (isPopKey(data, ctx)) {
      ctx.navigator.pop();
      return;
    }

    if (
      handleTabKeys(data, TAB_ORDER, this.activeTab, (next) => {
        this.setTab(next);
      })
    ) {
      ctx.requestRender();
      return;
    }

    if (data === "r") {
      this.frame.setNotice("Refreshing snapshot...", "info");
      ctx.requestRender();
      void this.deps.store.refresh().then(() => {
        this.frame.setNotice(this.deps.store.lastError ? `Refresh failed: ${this.deps.store.lastError}` : "", this.deps.store.lastError ? "danger" : "info");
        ctx.requestRender();
      });
      return;
    }

    // Quick-resolve selected escalation directly from Inbox or Escalations tab
    if (data === "R") {
      const escId = this.list.selectedItem()?.value.quickResolveEscId;
      if (escId) {
        this.frame.askPrompt({
          label: `Resolve ${escId}`,
          initialValue: "Resolved by user",
          onSubmit: async (note) => {
            await this.deps.client.request("escalation.ack", {
              escalationId: escId,
              resolution: note,
              resolve: true,
            });
            this.frame.setNotice(`Resolved ${escId}`, "ok");
            await this.deps.store.refresh();
          },
        });
        ctx.requestRender();
        return;
      }
    }

    // 1-key Quota Wait Picker on quota-paused runs
    if (data === "w") {
      const sel = this.list.selectedItem()?.value;
      if (sel?.quotaRunId) {
        ctx.navigator.push(
          new QuotaWaitPickerScreen(this.deps.client, {
            runId: sel.quotaRunId,
            reason: sel.quotaReason,
            resumeAt: sel.quotaResumeAt,
            onDone: async (msg) => {
              this.frame.setNotice(msg, "ok");
              await this.deps.store.refresh();
            },
          }),
        );
        return;
      }
    }

    if (data === "a" && this.activeTab === "escalations") {
      this.unresolvedEscalationsOnly = !this.unresolvedEscalationsOnly;
      this.rebuildList();
      ctx.requestRender();
      return;
    }

    if (data === "C" && this.activeTab === "worktrees") {
      this.frame.askConfirm({
        message: "Sweep finished & orphaned worktrees for this repo?",
        onConfirm: async () => {
          const res = await this.deps.client.request<{ removed?: string[] }>("worktrees.clean", {
            repoRoot: this.deps.repoRoot,
          });
          this.frame.setNotice(`Swept ${res.removed?.length ?? 0} worktree(s)`, "ok");
          await this.deps.store.refresh();
        },
      });
      ctx.requestRender();
      return;
    }

    const res = this.list.handleInput(data);
    if (res === "select") {
      const sel = this.list.selectedItem();
      if (sel) openEntityInspector(sel.value.ref, this.deps, ctx);
      return;
    }
    if (res === "handled") {
      ctx.requestRender();
    }
  }
}
