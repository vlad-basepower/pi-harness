import { randomUUID } from "node:crypto";
import { fitWidth } from "./primitives/width.js";
import { type Component, Container, Markdown, Spacer } from "@earendil-works/pi-tui";
import type { CompiledModelCatalog } from "../catalog/compiler.js";
import type { HarnessConfig } from "../config.js";
import type { EscalationRecord, EventRecord, ReplayPage, RunRecord, TaskRecord } from "../control-plane/db.js";
import type { RunQuotaPauseStatus } from "../control-plane/orchestrator.js";
import type { DaemonClient } from "../daemon/remote.js";
import type { ChatSessionRecord, ChatTurnRecord } from "../director/session.js";
import type { GoalRecord, GoalReviewVerdict } from "../goals/engine.js";
import type { KbCard } from "../kb/store.js";
import { parseChatControlEvent } from "../protocol/chat-events.js";
import type { DaemonEventFrame } from "../protocol/daemon-rpc.js";
import type { LiveWorktreeInfo } from "../worktree/manager.js";
import {
  autocompleteCommands,
  paletteCommands,
  renderHelp,
  resolveCommand,
  type CommandMode,
  type SlashCommand,
} from "./commands.js";
import {
  CommandPaletteOverlay,
  EffortPickerOverlay,
  formatGoalDetailCard,
  formatGoalReviewCard,
  formatResetCountdown,
  KbOverlay,
  ModelConfigOverlay,
  ModelPickerOverlay,
  QuotaOverlay,
  QuotaWaitPickerOverlay,
  SessionsOverlay,
  WorktreesOverlay,
  type AttachableSession,
} from "./overlays.js";
import {
  AssistantCard,
  BannerCard,
  Composer,
  createTheme,
  type HarnessTheme,
  ErrorCard,
  EventCard,
  FooterHint,
  GoalHeader,
  formatAgentError,
  OverlayManager,
  StatusBar,
  SystemCard,
  TerminalSession,
  TimedSpinner,
  ToolPill,
  GoalNotice,
  UserCard,
} from "./primitives/index.js";
import { ToolTranscript } from "./primitives/tool-chain.js";
import { RunsTreeOverlay, type GoalsRunsGoalAction, type RunsTreesResult } from "./runs-tree.js";
import { componentForHistoryTurn, extractDisplay, ToolOutputCard, type ToolDisplayPayload } from "./tool-output/index.js";
import { SettingsOverlay } from "./settings-overlay.js";
import { AgentTranscriptOverlay } from "./transcript-overlay.js";

export type TuiExitAction =
  | { kind: "exit" }
  | { kind: "switch"; targetId: string; targetKind: "chat" | "run" };

/**
 * Resolves composer input against the slash command registry. Input that looks like a slash command
 * (`/word`, not a path such as `/Users/me`) but names no command in `mode` reports `unknown`.
 */
function parseSlashInput(mode: CommandMode, text: string): { cmd: SlashCommand; arg: string } | { error: string } | null {
  const resolved = resolveCommand(mode, text);
  if (resolved && resolved.arg && !resolved.cmd.args) {
    return { error: `/${resolved.cmd.name} takes no arguments. Type /help for commands.` };
  }
  if (resolved) return resolved;
  return /^\/[A-Za-z][\w-]*(\s|$)/.test(text) ? { error: `Unknown command ${text.split(/\s/)[0]}. Type /help for commands.` } : null;
}

export type RunsAction = "resume" | "wait" | "stop";

/** Returns true when a Director tool call is waiting on delegated subagent(s) rather than Director reasoning. */
export function isSubagentWaitTool(name?: string, title?: string): boolean {
  const hay = `${name ?? ""} ${title ?? ""}`.toLowerCase();
  return /\b(wait_tasks|wait_children|goal_wait|goal_trigger_review)\b/.test(hay) ||
    hay.includes("mcp__harness__wait_tasks") ||
    hay.includes("mcp__harness__wait_children") ||
    hay.includes("mcp__harness__goal_wait") ||
    hay.includes("mcp__harness__goal_trigger_review");
}

/** True for frames delivered to a `watch.start` subscription; only the overlay that owns the watch consumes them. */
export function isWatchFrame(ev: DaemonEventFrame): boolean {
  return Boolean(ev.watchId);
}

type WatchClient = Pick<DaemonClient, "request" | "onEvent">;

async function consumeReplay(
  client: WatchClient,
  first: ReplayPage,
  subscription: { watchId: string } | { attachmentId: string },
  append: (event: EventRecord) => void,
  closed: () => boolean = () => false,
): Promise<void> {
  let page = first;
  const parts: Buffer[] = [];
  while (!closed()) {
    for (const event of page.replay) append(event);
    if (page.fragment) {
      parts.push(Buffer.from(page.fragment.data, "base64"));
      if (page.fragment.final) {
        append({ ...page.fragment.event, payload: JSON.parse(Buffer.concat(parts).toString("utf8")) });
        parts.length = 0;
      }
    }
    if (!page.next) break;
    page = await client.request<ReplayPage>("replay.page", subscription);
  }
}

/**
 * Starts a read-only daemon watch (never attach/detach) and routes frames tagged with its watchId to
 * `onEvent`. Frames that arrive before `watch.start` answers are buffered. `stop` is idempotent.
 */
async function startWatch(
  client: WatchClient,
  params:
    | { scope: "runs"; chatSessionId?: string; runIds?: string[] }
    | { scope: "task"; runId: string; taskId: string },
  onEvent: (ev: DaemonEventFrame) => void,
  closed?: () => boolean,
  onReplay?: (ev: EventRecord) => void,
): Promise<{ replay: EventRecord[]; stop: () => Promise<void> }> {
  let watchId: string | null = null;
  const early: DaemonEventFrame[] = [];
  const unsub = client.onEvent((ev) => {
    if (!ev.watchId) return;
    if (watchId === null) early.push(ev);
    else if (ev.watchId === watchId) onEvent(ev);
  });
  const replay: EventRecord[] = [];
  let res: ReplayPage & { watchId: string };
  try {
    res = await client.request<ReplayPage & { watchId: string }>("watch.start", params);
    watchId = res.watchId;
    for (const ev of early) if (ev.watchId === watchId) onEvent(ev);
    await consumeReplay(client, res, { watchId }, onReplay ?? ((ev) => replay.push(ev)), closed);
  } catch (err) {
    unsub();
    if (watchId) void client.request("watch.stop", { watchId }).catch(() => {});
    throw err;
  }
  let stopped = false;
  return {
    replay,
    stop: async () => {
      if (stopped) return;
      stopped = true;
      unsub();
      await client.request("watch.stop", { watchId: res.watchId }).catch(() => {});
    },
  };
}

/**
 * The single Goals & Runs overlay for both modes; every entry point (`/runs`, `/goals`, palette, Ctrl+T) lands here.
 * Read-only: it uses `runs.trees` + `watch.*` and never attaches, so the current session stays attached.
 * Chat mode also passes `fetchGoals`/`onGoalAction` so goals appear with their linked runs.
 * Enter on an agent stacks its live transcript on top; closing it returns to the same tree state.
 */
export function openRunsOverlay(ctx: {
  client: WatchClient;
  theme: HarnessTheme;
  overlays: OverlayManager;
  requestRender: () => void;
  onAction: (action: RunsAction, runId: string) => void;
  fetchGoals?: () => Promise<GoalRecord[]>;
  onGoalAction?: (action: GoalsRunsGoalAction, goal: GoalRecord) => void;
  chatSessionId?: string;
  runIds?: string[];
}): RunsTreeOverlay {
  const openTranscript = (row: {
    runId: string;
    taskId: string;
    title: string;
    model?: string | null;
    profileId?: string | null;
    effort?: string | null;
  }) => {
    let watch: Awaited<ReturnType<typeof startWatch>> | null = null;
    let closed = false;
    let handle: ReturnType<OverlayManager["push"]> | null = null;
    const comp = new AgentTranscriptOverlay({
      theme: ctx.theme,
      ...row,
      startWatch: async (onEvent, onReplay) => {
        watch = await startWatch(ctx.client, { scope: "task", runId: row.runId, taskId: row.taskId }, onEvent, () => closed, onReplay);
        if (closed) void watch.stop();
        return watch;
      },
      onClose: () => handle?.close(),
      requestRender: ctx.requestRender,
    });
    const modelBase = row.model || row.profileId || "";
    const subParts = [row.runId, row.taskId, ...(modelBase ? [modelBase] : []), ...(row.effort ? [row.effort] : [])];
    handle = ctx.overlays.push(
      {
        title: "Agent Transcript",
        subtitle: subParts.join(" · "),
        footer: "mouse wheel / ↑↓ scroll · read-only · Esc back to /runs",
      },
      comp,
      {
        width: "94%",
        captureMouseWheel: true,
        onClose: () => {
          closed = true;
          void watch?.stop();
        },
      },
    );
  };

  let handle: ReturnType<OverlayManager["push"]> | null = null;
  const scopeParams = {
    ...(ctx.chatSessionId ? { chatSessionId: ctx.chatSessionId } : {}),
    ...(ctx.runIds ? { runIds: ctx.runIds } : {}),
  };
  const comp = new RunsTreeOverlay({
    theme: ctx.theme,
    fetchTrees: async () => {
      type Page = RunsTreesResult & { next?: { id: string; createdAt: number } };
      let page = await ctx.client.request<Page>("runs.trees", scopeParams);
      const runs = [...page.runs];
      while (page.next) {
        page = await ctx.client.request<Page>("runs.trees", { ...scopeParams, before: page.next });
        runs.push(...page.runs);
      }
      return { generatedAt: page.generatedAt, runs };
    },
    subscribe: async (onEvent) => {
      const watch = await startWatch(ctx.client, { scope: "runs", ...scopeParams }, onEvent);
      return () => void watch.stop();
    },
    ...(ctx.fetchGoals ? { fetchGoals: ctx.fetchGoals } : {}),
    ...(ctx.onGoalAction ? { onGoalAction: ctx.onGoalAction } : {}),
    onOpenTranscript: openTranscript,
    onAction: ctx.onAction,
    onClose: () => handle?.close(),
    requestRender: ctx.requestRender,
  });
  const subtitle = ctx.chatSessionId
    ? `session ${ctx.chatSessionId}`
    : ctx.runIds?.length === 1
      ? `run ${ctx.runIds[0]}`
      : undefined;
  // Ctrl+T / Ctrl+C pop the overlay without Esc, so stop the watch on every close path.
  handle = ctx.overlays.push(
    { title: ctx.fetchGoals ? "Goals & Runs" : "Autonomous Runs", ...(subtitle ? { subtitle } : {}) },
    comp,
    { width: "94%", captureMouseWheel: true, onClose: () => comp.dispose() },
  );
  return comp;
}

async function resolveLeaveNote(client: DaemonClient): Promise<string> {
  try {
    const res = await client.request<{ daemonWillQuit: boolean; running: boolean; activeWorkSummary?: string }>(
      "session.leave",
    );
    if (res.daemonWillQuit) {
      return "[Exited — no background agents running; harnessd stopped.]";
    }
    const detail = res.activeWorkSummary ? ` (${res.activeWorkSummary})` : "";
    return `[Detached — background work${detail} continues in harnessd. Reattach with: pi-harness attach]`;
  } catch {
    return "[Exited.]";
  }
}

/** Transcript component for one persisted chat turn (history replay on chat.open / session switch). */
export function historyTurnComponent(t: ChatTurnRecord, theme: HarnessTheme, modelLabel: string): Component {
  if (t.role === "user") return new UserCard(theme, t.content);
  if (t.role === "assistant") return new AssistantCard(theme, t.content, modelLabel);
  if (t.event) return new EventCard(theme, t.event);
  return componentForHistoryTurn(t, theme) ?? new SystemCard(theme, [t.content], "muted");
}

/** Live tool components of the in-flight chat turn, keyed by tool event id, in arrival order. */
export type PendingTools = Map<string, ToolPill | ToolOutputCard>;

type ToolEvent = { id?: string; name?: string; status?: string; title?: string; args?: string; result?: string; error?: string };

/**
 * Applies one `chat.tool` event to the pending tools area: a `ToolOutputCard` when the event carries a
 * structured display (contract/tool-display-v1), otherwise the compact `ToolPill` created or updated in place.
 */
export function applyChatToolEvent(payload: unknown, pending: PendingTools, container: Container, theme: HarnessTheme): void {
  const p = (payload ?? {}) as ToolEvent;
  const display = extractDisplay(payload);
  if (display) {
    const key = p.id || `display-${pending.size}`;
    if (pending.get(key) instanceof ToolOutputCard) return;
    const card = new ToolOutputCard(theme, display);
    pending.set(key, card);
    container.addChild(card);
    return;
  }
  const rawLabel = p.title ?? p.name ?? "tool";
  const displayLabel = rawLabel.replace(/^mcp[._/]harness[._/]+/i, "harness:").replace(/^mcp__harness__/i, "harness:");
  const key = p.id || displayLabel;
  const st = p.status === "completed" ? "ok" : p.status === "failed" ? "error" : "running";
  const existing = pending.get(key);
  if (existing instanceof ToolPill && (p.id || st !== "running" || existing.status === "running")) {
    existing.update(st, p.error ?? p.result, p.args);
  } else if (!(existing instanceof ToolOutputCard)) {
    const pill = new ToolPill(theme, st, displayLabel, p.args);
    pill.update(st, p.error ?? p.result);
    pending.set(key, pill);
    container.addChild(pill);
  }
}

/**
 * Moves pending tool components into scrollback in arrival order. With `cardsOnly` (cancel / error
 * paths) only structured cards are kept, since the daemon already persisted them as tool_output turns.
 */
export function preservePendingTools(pending: PendingTools, scrollback: Container, opts: { cardsOnly?: boolean } = {}): void {
  for (const c of pending.values()) {
    if (opts.cardsOnly && !(c instanceof ToolOutputCard)) continue;
    scrollback.addChild(c);
  }
}

/**
 * Primary zero-arg interactive TUI: Persistent Tier 0 Director Chat on native terminal scrollback
 * (`@earendil-works/pi-tui` `TuiMainScreen`) built on `src/tui/primitives/`:
 *   - Bulletproof `Ctrl+C` / `Ctrl+D` / `Esc` / `Ctrl+]` / `Ctrl+,` / `Ctrl+O` controls
 *   - Zero `7u1;2c` shell escape leaks on exit (`drainInput` before `tui.stop()`)
 *   - Immutable width-memoized scrollback cards + memoized 1-line `StatusBar`
 *   - Live `ToolPill` + streaming `Markdown` during turns + follow-up turn queueing
 *   - Clean `ErrorCard` with full raw stderr persisted to `~/.pi-harness/logs/errors/`
 */
export async function runDirectorChatTui(opts: {
  client: DaemonClient;
  repoRoot: string;
  chatSessionId?: string;
  preferLastGlobal?: boolean;
}): Promise<TuiExitAction> {
  type ChatOpenPayload = {
    session: ChatSessionRecord;
    history: ChatTurnRecord[];
    goals: GoalRecord[];
    config: HarnessConfig;
    tookOver?: boolean;
    activeTurnId: string | null;
    activePartial?: string;
    activeText?: string;
    historyNext?: number | null;
    historyThrough?: number;
  };

  // Handshake BEFORE touching terminal raw mode so a failed RPC never leaves escape sequences in zsh!
  const bufferedEvents: DaemonEventFrame[] = [];
  let buffering = true;
  let handleEvent: (ev: DaemonEventFrame) => void;
  const unsubEvent = opts.client.onEvent((event) => {
    if (buffering) bufferedEvents.push(event);
    else handleEvent(event);
  });
  const resumeEvents = () => {
    buffering = false;
    for (const event of bufferedEvents.splice(0)) handleEvent(event);
  };
  const loadHistory = async (payload: ChatOpenPayload) => {
    let afterSeq = payload.historyNext;
    while (afterSeq != null) {
      const page = await opts.client.request<Pick<ChatOpenPayload, "history" | "historyNext">>("chat.history", {
        chatSessionId: payload.session.id, afterSeq, throughSeq: payload.historyThrough,
      });
      if (page.historyNext != null && page.historyNext <= afterSeq) throw new Error("Chat history cursor did not advance");
      for (const row of page.history) payload.history.push(row);
      afterSeq = page.historyNext;
    }
  };
  let disconnected = false;
  const unsubOpeningClose = opts.client.onClose(() => { disconnected = true; });
  let opened: ChatOpenPayload;
  try {
    opened = await opts.client.request<ChatOpenPayload>("chat.open", {
      repoRoot: opts.repoRoot,
      ...(opts.chatSessionId ? { chatSessionId: opts.chatSessionId } : {}),
      ...(opts.preferLastGlobal ? { preferLastGlobal: true } : {}),
    });
    await loadHistory(opened);
    if (disconnected) throw new Error("Disconnected while opening Director chat");
  } catch (err) {
    unsubEvent();
    throw err;
  } finally {
    unsubOpeningClose();
  }

  let session: ChatSessionRecord = opened.session;
  let activeRepoRoot: string = opened.session.repoRoot;
  let config: HarnessConfig = opened.config;
  let goals: GoalRecord[] = opened.goals;
  let cachedCatalog: CompiledModelCatalog | null = null;
  let streamedText = "";
  let lastSegment: { card: AssistantCard; text: string } | undefined;
  type TurnEnd = { assistantTurn?: ChatTurnRecord | null; error?: string; cancelled?: boolean };
  let activeTurnId = opened.activeTurnId;
  let busy = Boolean(activeTurnId);
  let controlling = false;
  let pendingEnd: TurnEnd | undefined;
  const drafts = new Map<string, string>();
  let exitAction: TuiExitAction = { kind: "exit" };
  let evictedByTakeover = false;
  const queuedMessages: string[] = [];
  const activeToolPills: PendingTools = new Map();

  const theme = createTheme();
  const term = new TerminalSession();
  const tui = term.tui;

  const chatContainer = new ToolTranscript(theme);
  const goalHeader = new GoalHeader(theme);
  const spinner = new TimedSpinner(tui, theme);
  const statusBar = new StatusBar(theme, {
    tone: "success",
    modeLabel: "director",
    modelLabel: session.model ?? session.profileId,
    effortLabel: session.effort,
    goalsSummary: `${goals.length} goals`,
  });
  const header = new Container();
  const footerHint = new FooterHint(theme);
  const composer = new Composer(tui, theme, {
    placeholder: "Message Director…",
    slashCommands: autocompleteCommands("chat"),
    cwd: opts.repoRoot,
    maxRows: (width) => tui.terminal.rows - header.render(width).length - footerHint.render(width).length,
  });
  const overlays = new OverlayManager(tui, theme, composer);

  const seedTranscript = (payload: ChatOpenPayload, notice?: string) => {
    chatContainer.clear();
    const dirLabel = payload.session.repoRoot.split("/").filter(Boolean).pop() ?? payload.session.repoRoot;
    chatContainer.addChild(
      new BannerCard(theme, "pi-harness", `${payload.session.title} · ${payload.session.id} (${dirLabel})`),
    );
    const modelLabel = payload.session.model ?? payload.session.profileId;
    for (const t of payload.history) chatContainer.addChild(historyTurnComponent(t, theme, modelLabel));
    if (payload.tookOver) {
      chatContainer.addChild(
        new SystemCard(theme, [`Took over ${payload.session.id} (previous terminal was detached).`], "info"),
      );
    }
    if (notice) {
      chatContainer.addChild(new SystemCard(theme, [notice], "success"));
    }
  };


  tui.addChild(chatContainer);
  tui.addChild(new Spacer(1));
  tui.addChild(spinner);
  header.addChild(goalHeader);
  header.addChild(statusBar);
  header.addChild({
    invalidate() {},
    render(width) {
      if (!busy || queuedMessages.length === 0) return [];
      return [fitWidth(` Queued (${queuedMessages.length}): ${queuedMessages[0]}`, width)];
    },
  });
  tui.addChild(header);
  tui.addChild(composer);
  tui.addChild(footerHint);
  tui.setFocus(composer);

  const activeModelDisplay = () => {
    const shortCli = session.profileId.replace(/^acp:/, "");
    if (!session.model) return session.profileId;
    const bareModel = session.model.includes("/") ? session.model.slice(session.model.indexOf("/") + 1) : session.model;
    return `${shortCli}:${bareModel}`;
  };

  const waitingSubagentCalls = new Map<string, string>();
  const isWaitingOnSubagent = () => waitingSubagentCalls.size > 0;
  const syncActivityIndicator = () => {
    if (!busy) return;
    if (isWaitingOnSubagent()) {
      const hasReview = [...waitingSubagentCalls.values()].some((n) => n.toLowerCase().includes("review"));
      spinner.setLabel(hasReview ? "Waiting on review subagent…" : "Waiting on subagent(s)…");
    } else {
      spinner.setLabel(`Thinking (${session.model ?? session.profileId})…`);
    }
  };

  const refreshStatus = () => {
    const humanNeeded = goals.filter((g) => g.status === "awaiting_human").length;
    const waitingSubagent = busy && isWaitingOnSubagent();
    goalHeader.update(goals);
    statusBar.update({
      tone: humanNeeded > 0 ? "error" : waitingSubagent ? "info" : busy ? "accent" : "success",
      modeLabel: waitingSubagent ? "waiting on subagent" : busy ? "thinking" : "director",
      modelLabel: activeModelDisplay(),
      effortLabel: session.effort,
      goalsSummary: "",
      alertText: humanNeeded > 0 ? `${humanNeeded} P0!` : null,
    });
  };

  const appendSystem = (lines: string[], tone: "muted" | "success" | "warning" | "info" = "muted") => {
    chatContainer.addChild(new SystemCard(theme, lines, tone));
    tui.requestRender();
  };

  const appendError = (err: unknown, context: string) => {
    const formatted = formatAgentError(err, { stateDir: config.stateDir, context });
    chatContainer.addChild(new ErrorCard(theme, formatted));
    tui.requestRender();
  };

  const clearPending = () => {
    streamedText = "";
    lastSegment = undefined;
    activeToolPills.clear();
    waitingSubagentCalls.clear();
  };

  const appendDelta = (delta: string) => {
    if (!delta) return;
    if (!lastSegment || chatContainer.children.at(-1) !== lastSegment.card) {
      const card = new AssistantCard(theme, "", session.model ?? session.profileId);
      lastSegment = { card, text: "" };
      chatContainer.addChild(card);
    }
    lastSegment.text += delta;
    lastSegment.card.setText(lastSegment.text);
    streamedText += delta;
    tui.requestRender();
  };

  const seedActiveTranscript = (partial: string, allText: string) => {
    appendDelta(partial);
    streamedText = allText || partial;
  };

  const finishTranscript = (finalText?: string) => {
    const final = finalText?.trim();
    const streamed = streamedText.trim();
    // ACP returns the whole turn, while Pi may return only its final message.
    if (final && (!streamed || !streamed.endsWith(final))) {
      const last = lastSegment?.text.trim();
      const prefix = streamed && final.startsWith(streamed) ? streamed : last && final.startsWith(last) ? last : "";
      appendDelta(final.slice(prefix.length));
    }
    for (const component of activeToolPills.values()) {
      if (component instanceof ToolPill) component.finishCall();
    }
    clearPending();
  };

  let goalsRequest = 0;
  let pollingGoals = false;
  const refreshGoals = async () => {
    const repo = activeRepoRoot;
    const chatId = session.id;
    const request = ++goalsRequest;
    try {
      const next = await opts.client.request<GoalRecord[]>("goal.list", { repoRoot: repo, chatSessionId: chatId });
      if (repo !== activeRepoRoot || chatId !== session.id || request !== goalsRequest) return;
      goals = next;
      refreshStatus();
      tui.requestRender();
    } catch {
      // The next refresh retries after a transient disconnect.
    }
  };

  seedTranscript(opened);
  if (busy) seedActiveTranscript(opened.activePartial ?? "", opened.activeText ?? "");

  const changeSession = async (op: "chat.open" | "chat.create", params: Record<string, unknown>) => {
    if (buffering || controlling) return;
    buffering = true;
    try {
      const next = await opts.client.request<ChatOpenPayload>(op, { repoRoot: activeRepoRoot, ...params });
      const draft = [...queuedMessages, composer.getExpandedText()].filter(Boolean).join("\n\n");
      if (draft) drafts.set(session.id, draft);
      queuedMessages.length = 0;
      composer.setText(drafts.get(next.session.id) ?? "");
      drafts.delete(next.session.id);
      session = next.session;
      activeRepoRoot = session.repoRoot;
      goals = next.goals;
      config = next.config;
      activeTurnId = next.activeTurnId;
      busy = Boolean(activeTurnId);
      pendingEnd = undefined;
      clearPending();
      if (busy) spinner.start(`Thinking (${session.model ?? session.profileId})…`);
      else spinner.stop();
      try {
        await loadHistory(next);
      } finally {
        seedTranscript(next, op === "chat.create" ? `Started new session ${session.id}.` : undefined);
        if (busy) seedActiveTranscript(next.activePartial ?? "", next.activeText ?? "");
        refreshStatus();
      }
    } catch (err) {
      appendError(err, op);
    } finally {
      resumeEvents();
      if (!busy) sendNextQueued();
      tui.requestRender(true);
    }
  };
  const switchToChatSessionInPlace = (chatSessionId: string) => void changeSession("chat.open", { chatSessionId });
  const createNewChatSession = (title?: string) => void changeSession("chat.create", { title });

  const switchToTarget = (targetId: string) => {
    if (targetId === session.id) {
      appendSystem([`Already attached to ${targetId}.`], "muted");
      return;
    }
    if (targetId.startsWith("run-")) {
      exitAction = { kind: "switch", targetId, targetKind: "run" };
      void term.shutdown(undefined, { overlays, spinner, cleanup: detachListenersOnly });
      return;
    }
    switchToChatSessionInPlace(targetId);
  };

  const openSessionsPicker = () => {
    void opts.client
      .request<AttachableSession[]>("sessions.list")
      .then((sessions) => {
        let handle: ReturnType<OverlayManager["push"]> | null = null;
        const comp = new SessionsOverlay(
          theme,
          sessions,
          (chosen) => switchToTarget(chosen.id),
          () => createNewChatSession(),
          () => handle?.close(),
          () => handle?.rerender(),
        );
        handle = overlays.push(
          {
            title: "Sessions & Attach Switcher",
            subtitle: `${sessions.length} session${sessions.length === 1 ? "" : "s"}`,
            footer: "Enter attach & take over · n new chat · Esc close",
          },
          comp,
          { width: "92%" },
        );
      })
      .catch((err) => appendError(err, "sessions.list"));
  };

  const openEffortPicker = (efforts?: string[]) => {
    const activeModel = cachedCatalog?.models.find(
      (m) => m.id === session.model || m.preferredRoute.wireModelId === session.model,
    );
    const available = efforts ?? activeModel?.efforts ?? ["low", "medium", "high", "xhigh", "max"];
    let handle: ReturnType<OverlayManager["push"]> | null = null;
    const comp = new EffortPickerOverlay(
      theme,
      available,
      session.effort,
      (effort) => {
        void opts.client
          .request<ChatSessionRecord>("chat.update", { chatSessionId: session.id, effort })
          .then((updated) => {
            session = updated;
            appendSystem([`Active reasoning effort set to ${theme.bold(effort)}`], "success");
            refreshStatus();
            tui.requestRender();
          })
          .catch((err) => appendError(err, "effort.update"));
      },
      () => handle?.close(),
      () => handle?.rerender(),
    );
    handle = overlays.push(
      {
        title: "Reasoning Effort",
        subtitle: session.model ?? session.profileId,
        footer: "↑/↓ move · Enter apply · Esc cancel",
      },
      comp,
      { width: "68%" },
    );
  };

  /** `/model` CLI-default row: switch the ACP CLI without pinning a model. */
  const switchCliDefault = (profileId: string) => {
    void opts.client
      .request<ChatSessionRecord>("chat.update", { chatSessionId: session.id, profileId, model: null })
      .then((updated) => {
        session = updated;
        appendSystem([`Switched active ACP CLI to ${theme.bold(profileId)} (using its default model)`], "success");
        refreshStatus();
        tui.requestRender();
      })
      .catch((err) => appendError(err, "cli.switch"));
  };

  const openModelConfig = (reopenPicker = false) => {
    let handle: ReturnType<OverlayManager["push"]> | null = null;
    const comp = new ModelConfigOverlay(
      theme,
      opts.client,
      activeRepoRoot,
      config,
      (nextCfg) => {
        config = nextCfg;
        refreshStatus();
        tui.requestRender();
      },
      () => {
        handle?.close();
        if (reopenPicker) openModelPicker();
      },
      () => handle?.rerender(),
    );
    handle = overlays.push(
      {
        title: "Configure Enabled Models",
        subtitle: "ranked catalog",
        footer: "Space/Enter toggle · x All Off · a All On · Esc done",
      },
      comp,
      { width: "90%", captureMouseWheel: true },
    );
  };

  const openModelPicker = () => {
    spinner.start("Loading model catalog…", "Esc to cancel");
    void opts.client
      .request<CompiledModelCatalog>("models.list", { cwd: activeRepoRoot, chatSessionId: session.id })
      // Keep the per-CLI default rows usable even when the catalog cannot load.
      .catch((err): CompiledModelCatalog => {
        appendError(err, "models.list");
        return { syncedAt: new Date().toISOString(), sources: [], models: [] };
      })
      .then((cat) => {
        spinner.stop();
        if (cat.models.length > 0) cachedCatalog = cat;
        let handle: ReturnType<OverlayManager["push"]> | null = null;
        const comp = new ModelPickerOverlay(
          theme,
          opts.client,
          activeRepoRoot,
          cat,
          session.model ?? null,
          session.profileId,
          (entry, route) => {
            void opts.client
              .request<ChatSessionRecord>("chat.update", {
                chatSessionId: session.id,
                profileId: route.profileId,
                model: route.wireModelId,
              })
              .then((updated) => {
                session = updated;
                appendSystem(
                  [
                    `Selected ${theme.bold(entry.id)} via ${theme.accent(route.profileId)} (score: ${entry.codingScore?.toFixed(1) ?? "-"})`,
                  ],
                  "success",
                );
                refreshStatus();
                const routeEfforts = route.supportedEfforts ?? entry.efforts;
                if (routeEfforts.length > 1) {
                  openEffortPicker(routeEfforts);
                } else {
                  tui.requestRender();
                }
              })
              .catch((err) => appendError(err, "model.select"));
          },
          () => handle?.close(),
          () => handle?.rerender(),
          { profiles: config.profiles, probeStatuses: cat.profileStatuses, onSelect: switchCliDefault },
          () => openModelConfig(true),
        );
        handle = overlays.push(
          {
            title: "Model Catalog & ACP Route Picker",
            subtitle: `${cat.models.length} enabled · synced ${cat.syncedAt.slice(0, 16).replace("T", " ")}`,
            footer: "Type to filter · Tab switch CLI · Ctrl+E toggle models · Ctrl+R sync · Enter select · Esc close",
          },
          comp,
          { width: "92%" },
        );
      })
      .catch((err) => {
        spinner.stop();
        appendError(err, "models.list");
      });
  };

  const openSettings = () => {
    let handle: ReturnType<OverlayManager["push"]> | null = null;
    const comp = new SettingsOverlay(
      theme,
      opts.client,
      config,
      (nextCfg) => {
        config = nextCfg;
        refreshStatus();
        tui.requestRender();
      },
      () => handle?.close(),
      () => handle?.rerender(),
      activeRepoRoot,
    );
    handle = overlays.push(
      {
        title: "Settings & External Harness CLIs",
        subtitle: `${config.stateDir}/config.json`,
        footer: "↑/↓ move · Enter edit/toggle · Esc back/close",
      },
      comp,
      { width: "90%" },
    );
  };

  const openQuota = () => {
    void opts.client
      .request<{
        configured: boolean;
        snapshot: Record<string, unknown> | null;
        coverage?: Array<{ profileId: string; adapter: string; hasData: boolean; source: string; note: string }>;
      }>("quota.get")
      .then((data) => {
        let handle: ReturnType<OverlayManager["push"]> | null = null;
        const comp = new QuotaOverlay(
          theme,
          opts.client,
          data,
          () => handle?.close(),
          () => handle?.rerender(),
        );
        handle = overlays.push(
          {
            title: "Live Subscription & API Quotas",
            subtitle: data.configured
              ? "endpoint + passive"
              : data.snapshot
                ? "passive / local harvest"
                : "no endpoint configured",
            footer: "r refresh now · Esc close",
          },
          comp,
          { width: "88%" },
        );
      })
      .catch((err) => appendError(err, "quota.get"));
  };

  const resumeRunNow = (runId: string) => {
    void opts.client
      .request<{ resumed: boolean; resumedTaskIds: string[] }>("run.resume", { runId })
      .then((res) => {
        if (res.resumed) {
          appendSystem([`Resumed ${theme.bold(runId)} (${res.resumedTaskIds.length} task(s) restarted; quota state reset).`], "success");
        } else {
          appendSystem([`Run ${runId} had no quota-paused tasks.`], "muted");
        }
      })
      .catch((err) => appendError(err, `run.resume:${runId}`));
  };

  const scheduleRunWait = (runId: string, delayMs: number) => {
    void opts.client
      .request<RunQuotaPauseStatus>("run.schedule_resume", { runId, delayMs })
      .then((st) => {
        appendSystem(
          [
            `Scheduled ${theme.bold(runId)} to auto-resume ${formatResetCountdown(st.resumeAt)} (${st.resumeAt ?? ""}).`,
            theme.dim("Open /runs and press r anytime to override and resume immediately."),
          ],
          "info",
        );
      })
      .catch((err) => appendError(err, `run.schedule_resume:${runId}`));
  };

  const openQuotaWaitPicker = (runId: string, reason = "Paused due to quota limit", currentResumeAt: string | null = null) => {
    let handle: ReturnType<OverlayManager["push"]> | null = null;
    const comp = new QuotaWaitPickerOverlay(
      theme,
      runId,
      reason,
      currentResumeAt,
      (choice) => {
        if (choice.kind === "now") resumeRunNow(runId);
        else scheduleRunWait(runId, choice.delayMs);
      },
      () => handle?.close(),
      () => handle?.rerender(),
    );
    handle = overlays.push(
      {
        title: "Quota Pause — Resume or Schedule Wait",
        subtitle: runId,
        footer: "↑/↓ move · Enter apply · Esc keep paused",
      },
      comp,
      { width: "80%" },
    );
  };

  const onRunsAction = (action: RunsAction, runId: string) => {
    if (action === "stop") {
      void opts.client
        .request("run.stop", { runId })
        .then(() => appendSystem([`Stopped run ${theme.bold(runId)}.`], "warning"))
        .catch((err) => appendError(err, "run.stop"));
    } else if (action === "resume") {
      resumeRunNow(runId);
    } else {
      void opts.client
        .request<{ quotaPause?: RunQuotaPauseStatus | null }>("run.get", { runId })
        .then((d) => openQuotaWaitPicker(runId, d.quotaPause?.reason ?? "Paused due to quota", d.quotaPause?.resumeAt ?? null))
        .catch(() => openQuotaWaitPicker(runId));
    }
  };

  const openWorktrees = () => {
    void opts.client
      .request<LiveWorktreeInfo[]>("worktrees.list", { repoRoot: activeRepoRoot })
      .then((list) => {
        let handle: ReturnType<OverlayManager["push"]> | null = null;
        const comp = new WorktreesOverlay(
          theme,
          opts.client,
          activeRepoRoot,
          list,
          () => handle?.close(),
          () => handle?.rerender(),
        );
        handle = overlays.push(
          {
            title: "Git Worktrees Manager",
            subtitle: `${list.length} active`,
            footer: "c clean stale/orphaned · r refresh · Esc close",
          },
          comp,
          { width: "88%" },
        );
      })
      .catch((err) => appendError(err, "worktrees.list"));
  };

  const openKb = (query: string) => {
    void opts.client
      .request<KbCard[]>("kb.search", { repoRoot: activeRepoRoot, query: query || undefined, includeCandidates: true })
      .then((cards) => {
        let handle: ReturnType<OverlayManager["push"]> | null = null;
        const comp = new KbOverlay(
          theme,
          opts.client,
          cards,
          () => handle?.close(),
          () => handle?.rerender(),
        );
        handle = overlays.push(
          {
            title: "Knowledge Base",
            subtitle: query ? `query: "${query}"` : `${cards.length} cards`,
            footer: "Enter open full context (Stage 2) · Esc close",
          },
          comp,
          { width: "90%" },
        );
      })
      .catch((err) => appendError(err, "kb.search"));
  };

  const triggerGoalReview = (goalId: string) => {
    spinner.start(`Reviewing ${goalId} (deterministic gates + Skeptical Done-Review)…`);
    void opts.client
      .request<{ goal: GoalRecord; review: GoalReviewVerdict; circuitBreakerTripped?: string }>("goal.review", { goalId })
      .then(
        (res) => {
          spinner.stop();
          const tone =
            res.review.verdict === "approved"
              ? "success"
              : res.review.verdict === "escalate_human"
                ? "warning"
                : "info";
          appendSystem(formatGoalReviewCard(res, theme), tone);
          void refreshGoals();
        },
        (err) => {
          spinner.stop();
          appendError(err, `goal.review:${goalId}`);
        },
      );
  };

  // Goal rows reuse the legacy Goals dashboard actions.
  const onGoalAction = (action: GoalsRunsGoalAction, g: GoalRecord) => {
    if (action === "review") {
      triggerGoalReview(g.id);
    } else if (action === "pause" || action === "cancel") {
      void opts.client
        .request(action === "pause" ? "goal.pause" : "goal.cancel", { goalId: g.id })
        .then(() => {
          appendSystem([`Goal ${theme.bold(g.id)} ${action === "pause" ? "paused" : "cancelled"}.`], "info");
          void refreshGoals();
        })
        .catch((err) => appendError(err, `goal.${action}`));
    } else if (action === "print_table") {
      void opts.client
        .request<{ display: ToolDisplayPayload }>("goal.table_display", {
          repoRoot: activeRepoRoot,
          chatSessionId: session.id,
          persistChatSessionId: session.id,
        })
        .then((res) => {
          chatContainer.addChild(new ToolOutputCard(theme, res.display));
          tui.requestRender();
        })
        .catch((err) => appendError(err, "goal.table_display"));
    } else {
      void opts.client
        .request<{ goal: GoalRecord; reviews: GoalReviewVerdict[]; display?: ToolDisplayPayload }>("goal.get", {
          goalId: g.id,
          persistChatSessionId: session.id,
        })
        .then((detail) => {
          if (detail.display) {
            chatContainer.addChild(new ToolOutputCard(theme, detail.display));
            tui.requestRender();
          } else {
            appendSystem(formatGoalDetailCard(detail.goal, detail.reviews, theme), "info");
          }
        })
        .catch((err) => appendError(err, "goal.get"));
    }
  };

  /** `/runs` and `/goals` both open this one Goals & Runs view. */
  const openRuns = () =>
    openRunsOverlay({
      client: opts.client,
      theme,
      overlays,
      requestRender: () => tui.requestRender(),
      onAction: onRunsAction,
      fetchGoals: async () => {
        await refreshGoals();
        return goals;
      },
      onGoalAction,
      chatSessionId: session.id,
    });

  const showStatusCard = () => {
    void opts.client
      .request<{ pid: number; uptimeMs: number; profiles: Array<{ id: string; enabled: boolean }> }>("status")
      .then((st) => {
        const enabled = st.profiles
          .filter((p) => p.enabled)
          .map((p) => p.id)
          .join(", ");
        appendSystem(
          [
            `${theme.bold("Harness Status")} · daemon pid ${st.pid} (up ${Math.round(st.uptimeMs / 1000)}s)`,
            `Chat: ${theme.accent(session.id)} │ Model: ${theme.bold(session.model ?? session.profileId)} │ Effort: ${session.effort ?? "default"}`,
            `Enabled CLIs: ${enabled || "(none)"}`,
            `Goals: ${goals.length} total (${goals.filter((g) => g.status === "executing").length} executing, ${goals.filter((g) => g.status === "achieved").length} achieved)`,
          ],
          "info",
        );
      })
      .catch((err) => appendError(err, "status"));
  };

  const showHelpCard = () => appendSystem(renderHelp("chat", theme), "info");

  const quitChat = () =>
    void term.shutdown(undefined, {
      overlays,
      spinner,
      resolveNote: () => resolveLeaveNote(opts.client),
      cleanup: cleanupFullExit,
    });

  const runChatCommand = (cmd: SlashCommand, arg: string) => {
    switch (cmd.name) {
      case "runs":
        return openRuns();
      case "goals":
        return openRuns();
      case "sessions":
        return arg ? switchToTarget(arg) : openSessionsPicker();
      case "new":
        return createNewChatSession(arg || undefined);
      case "model":
        return openModelPicker();
      case "effort":
        if (!arg) return openEffortPicker();
        return void opts.client
          .request<ChatSessionRecord>("chat.update", { chatSessionId: session.id, effort: arg })
          .then((updated) => {
            session = updated;
            appendSystem([`Active reasoning effort set to ${theme.bold(arg)}`], "success");
            refreshStatus();
            tui.requestRender();
          })
          .catch((err) => appendError(err, "effort.update"));
      case "quota":
        return openQuota();
      case "worktrees":
        return openWorktrees();
      case "kb":
        return openKb(arg);
      case "settings":
        return openSettings();
      case "status":
        return showStatusCard();
      case "clear":
        chatContainer.clear();
        chatContainer.addChild(new BannerCard(theme, "pi-harness", `Director Chat · ${session.id}`));
        return tui.requestRender(true);
      case "help":
        return showHelpCard();
      case "quit":
        return quitChat();
    }
  };

  const openCommandPalette = () => {
    const commands = paletteCommands("chat", (cmd) => runChatCommand(cmd, ""));
    let handle: ReturnType<OverlayManager["push"]> | null = null;
    const comp = new CommandPaletteOverlay(
      theme,
      commands,
      () => handle?.close(),
      () => handle?.rerender(),
    );
    handle = overlays.push(
      {
        title: "Command Palette",
        subtitle: `${commands.length} commands`,
        footer: "↑/↓ move · Enter run · Esc close",
      },
      comp,
      { width: "80%" },
    );
  };

  let unsubClose = () => {};

  const detachListenersOnly = () => {
    unsubEvent();
    unsubClose();
  };

  const cleanupFullExit = () => {
    detachListenersOnly();
    opts.client.close();
  };

  const restoreQueued = () => {
    if (queuedMessages.length) {
      composer.setText([...queuedMessages.splice(0), composer.getExpandedText()].filter(Boolean).join("\n\n"));
    }
  };

  const sendNextQueued = () => {
    if (busy || buffering || evictedByTakeover) return;
    const next = queuedMessages.shift();
    if (next) dispatchChatTurn(next);
  };

  const settleTurn = (turnId: string, end: TurnEnd, recall = false) => {
    if (activeTurnId !== turnId || evictedByTakeover) return;
    if (controlling) {
      pendingEnd ??= end;
      return;
    }
    activeTurnId = null;
    busy = false;
    composer.clearConfirmBanner();
    spinner.stop();
    finishTranscript(end.assistantTurn?.content);
    if (end.cancelled) appendSystem(["Cancelled current response."], "warning");
    else if (end.error) appendError(new Error(end.error), `chat.send:${session.profileId}`);
    refreshStatus();
    if (end.error || recall) restoreQueued();
    else sendNextQueued();
    tui.requestRender();
    void refreshGoals();
  };

  const dispatchChatTurn = (text: string) => {
    const turnId = randomUUID();
    activeTurnId = turnId;
    busy = true;
    chatContainer.addChild(new UserCard(theme, text));
    clearPending();
    refreshStatus();
    spinner.start(`Thinking (${session.model ?? session.profileId})…`);
    void opts.client.request<TurnEnd>("chat.send", { chatSessionId: session.id, turnId, text }).then(
      (res) => settleTurn(turnId, res),
      (err) => settleTurn(turnId, { error: err instanceof Error ? err.message : String(err) }),
    );
  };

  // A terminal event can arrive before the control RPC acknowledges cancellation or steering.
  // Hold that event until the acknowledgement identifies what to do with the queue.
  const controlTurn = async (action: "escape" | "ctrl+c" | "steer", directMessage?: string) => {
    const turnId = activeTurnId;
    if (!turnId || controlling || buffering) return;
    const draft = directMessage === undefined ? composer.getExpandedText() : "";
    const hasDirect = directMessage !== undefined && Boolean(directMessage.trim());
    const steerFromDraft = action === "steer" && !hasDirect && Boolean(draft.trim());
    const message = hasDirect ? directMessage! : steerFromDraft ? draft : queuedMessages[0] ?? "";
    if (action === "steer" && !message) return;
    if (steerFromDraft) composer.clear();
    const restoreSteerDraft = () => {
      if (hasDirect) composer.setText([directMessage!, composer.getExpandedText()].filter(Boolean).join("\n\n"));
      else if (steerFromDraft) composer.setText([draft, composer.getExpandedText()].filter(Boolean).join("\n\n"));
    };
    controlling = true;
    let end: TurnEnd | undefined;
    try {
      if (action === "steer") {
        const res = await opts.client.request<{ steered: boolean; cancelled?: boolean }>("chat.steer", {
          chatSessionId: session.id, turnId, message,
        });
        if (evictedByTakeover) return;
        if (res.steered || res.cancelled) {
          if (res.steered) {
            if (!hasDirect && !steerFromDraft) queuedMessages.shift();
          } else {
            if (hasDirect || steerFromDraft) queuedMessages.unshift(message);
            end = { cancelled: true };
          }
        } else {
          if (hasDirect) {
            // Turn just finished right as the user hit Enter while waiting on subagents -> send normally!
            queuedMessages.unshift(message);
          } else {
            restoreSteerDraft();
            appendSystem(["Turn already ended. Message kept in input."], "warning");
          }
        }
      } else {
        const res = await opts.client.request<{ cancelled: boolean }>("chat.cancel", {
          chatSessionId: session.id, turnId,
        });
        if (res.cancelled) end = { cancelled: true };
      }
    } catch (err) {
      restoreSteerDraft();
      appendError(err, action === "steer" ? "chat.steer" : "chat.cancel");
    } finally {
      controlling = false;
      end = end ? { ...pendingEnd, ...end, error: undefined } : pendingEnd;
      pendingEnd = undefined;
      if (end) settleTurn(turnId, end, action === "ctrl+c");
      tui.requestRender();
    }
  };

  term.bindControls({
    overlays,
    spinner,
    footerHint,
    detachBanner: "[Exited.]",
    callbacks: {
      getComposerText: () => composer.getText(),
      clearComposerText: () => composer.clear(),
      isAutocompleteOpen: () => composer.isAutocompleteOpen(),
      isBusy: () => busy,
      onAbortGeneration: (key) => void controlTurn(key),
      onEscConfirmChange: (state) => composer.setConfirmBanner(state),
      onSteer: () => void controlTurn("steer"),
      onEnqueue: () => {
        if (buffering) return false;
        const text = composer.getExpandedText().trim();
        if (!text) return false;
        composer.clear();
        composer.addToHistory(text);
        const slash = parseSlashInput("chat", text);
        if (slash && "error" in slash) {
          appendSystem([slash.error], "warning");
          return true;
        }
        if (slash) {
          runChatCommand(slash.cmd, slash.arg);
          return true;
        }
        queuedMessages.push(text);
        tui.requestRender();
        return true;
      },
      onRecallQueued: () => {
        if (controlling || buffering || !queuedMessages.length) return false;
        const recalled = queuedMessages.shift()!;
        composer.setText([recalled, composer.getExpandedText()].filter(Boolean).join("\n\n"));
        tui.requestRender();
        return true;
      },
      onOpenSettings: openSettings,
      onOpenPalette: openCommandPalette,
      onOpenTree: openRuns,
      onResolveExitNote: () => resolveLeaveNote(opts.client),
      onShutdownCleanup: cleanupFullExit,
    },
  });

  composer.onSubmit = (raw) => {
    if (buffering) {
      composer.setText(raw);
      return;
    }
    const text = raw.trim();
    composer.clear();
    if (!text) return;
    composer.addToHistory(text);

    const slash = parseSlashInput("chat", text);
    if (slash && "error" in slash) return appendSystem([slash.error], "warning");
    if (slash) return runChatCommand(slash.cmd, slash.arg);

    if (busy) {
      // Default for Director chat: Enter sends immediately (steer/deliver now).
      // Use Ctrl+Q / Alt+Enter to explicitly enqueue for after the current turn.
      if (!controlling) {
        void controlTurn("steer", text);
        return;
      }
      queuedMessages.push(text);
      tui.requestRender();
      return;
    }

    dispatchChatTurn(text);
  };

  handleEvent = (ev) => {
    // Frames tagged with a watchId belong to the overlay that started that watch.
    if (isWatchFrame(ev)) return;
    if (ev.type === "session.taken_over") {
      const p = ev.payload as { sessionId?: string; reason?: string };
      if (p.sessionId !== session.id) return;
      evictedByTakeover = true;
      void term.shutdown(
        `[Detached — session ${p.sessionId ?? session.id} was taken over by another terminal.]`,
        { overlays, spinner, cleanup: cleanupFullExit },
      );
      return;
    }
    if (ev.type.startsWith("chat.")) {
      const p = ev.payload as TurnEnd & { chatSessionId: string; turnId: string; summary?: string; event?: unknown };
      if (p.chatSessionId !== session.id) return;
      if (ev.type === "chat.turn.start") {
        if (activeTurnId || busy) return;
        activeTurnId = p.turnId;
        busy = true;
        clearPending();
        const typedEv = parseChatControlEvent(p.event);
        if (typedEv) {
          chatContainer.addChild(new EventCard(theme, typedEv));
        } else if (p.summary) {
          appendSystem([p.summary], "info");
        }
        refreshStatus();
        spinner.start(`Thinking (${session.model ?? session.profileId})…`);
        tui.requestRender();
        return;
      }
      if (!activeTurnId || p.turnId !== activeTurnId) return;
      if (ev.type === "chat.steered") {
        const steered = ev.payload as { message?: string; role?: "user" | "system_notice"; event?: unknown };
        const typedEv = parseChatControlEvent(steered.event);
        if (typedEv) {
          chatContainer.addChild(new EventCard(theme, typedEv));
          tui.requestRender();
        } else if (steered.role === "system_notice") {
          if (steered.message) appendSystem([steered.message], "info");
        } else {
          chatContainer.addChild(new UserCard(theme, String(steered.message ?? "")));
          tui.requestRender();
        }
        return;
      }
      if (ev.type === "chat.turn.end") {
        settleTurn(p.turnId, p);
        return;
      }
    }
    if (ev.type === "chat.delta" && busy) {
      const delta = String((ev.payload as { delta?: string })?.delta ?? "");
      if (!delta) return;
      if (waitingSubagentCalls.size > 0) {
        waitingSubagentCalls.clear();
        syncActivityIndicator();
        refreshStatus();
      }
      appendDelta(delta);
    } else if (ev.type === "chat.tool" && busy) {
      const p = ev.payload as ToolEvent;
      const name = p.title ?? p.name ?? "tool";
      const key = p.id || name;
      if (isSubagentWaitTool(p.name, p.title)) {
        if (p.status === "completed" || p.status === "failed") {
          waitingSubagentCalls.delete(key);
          waitingSubagentCalls.delete(name);
        } else {
          waitingSubagentCalls.set(key, name);
        }
        syncActivityIndicator();
        refreshStatus();
      }
      applyChatToolEvent(p, activeToolPills, chatContainer, theme);
      tui.requestRender();
    } else if (ev.type === "goal.progress") {
      void refreshGoals();
    } else if (ev.type === "goal.reviewed") {
      const p = ev.payload as { goalId: string; version: number; cycle: number; verdict: string; status: string; summary: string; circuitBreakerTripped?: string };
      if (p.status === "achieved" && !p.circuitBreakerTripped) {
        chatContainer.addChild(new GoalNotice(theme, `${p.goalId} achieved`));
        tui.requestRender();
      } else {
        appendSystem(
          [`[${p.goalId} v${p.version}] Review #${p.cycle}: ${p.verdict.toUpperCase()} (${p.status}) — ${p.summary}`],
          "warning",
        );
      }
      void refreshGoals();
    } else if (ev.type === "goal.amended") {
      const p = ev.payload as { goalId: string; delta: { fromVersion: number; toVersion: number; rationale: string } };
      appendSystem([`[${p.goalId}] Amended v${p.delta.fromVersion} → v${p.delta.toVersion}: ${p.delta.rationale}`], "info");
      void refreshGoals();
    } else if (ev.type === "goal.committed") {
      const p = ev.payload as { goalId: string };
      chatContainer.addChild(new GoalNotice(theme, `${p.goalId} started`));
      tui.requestRender();
      void refreshGoals();
    } else if (ev.type === "quota.agent_paused") {
      const p = ev.payload as {
        runId: string;
        taskId: string;
        tier: number;
        title: string;
        profileId: string;
        model?: string | null;
        reason: string;
        resetAt?: string | null;
      };
      const eta = p.resetAt ? ` (reset ${formatResetCountdown(p.resetAt)})` : "";
      appendSystem(
        [
          `${theme.bold(`⏸ Agent paused early (quota)`)} · ${theme.accent(p.taskId)} [T${p.tier}] "${p.title}" (${p.model ?? p.profileId})`,
          `${p.reason}${eta} — worktree & WIP preserved; healthy sibling tasks continue.`,
        ],
        "warning",
      );
    } else if (ev.type === "quota.run_paused") {
      const p = ev.payload as {
        runId: string;
        pausedTaskIds: string[];
        mode: "auto_timer" | "user_timer" | "manual";
        source: string;
        resumeAt?: string | null;
        reason: string;
      };
      const actionLine =
        p.mode === "auto_timer" && p.resumeAt
          ? `Auto-resume scheduled ${formatResetCountdown(p.resumeAt)} (${p.source}). Open /runs and press r to resume now or w to pick a wait timer.`
          : `No Quota Endpoint / reset time wired — paused cleanly. Open /runs and press r to resume now or w to pick how long to wait.`;
      appendSystem(
        [
          `${theme.bold(`⏸ Run ${p.runId} paused (quota)`)} — all active workers exited (${p.pausedTaskIds.length} task(s) waiting)`,
          actionLine,
        ],
        "warning",
      );
    } else if (ev.type === "quota.run_scheduled") {
      const p = ev.payload as { runId: string; resumeAt: string };
      appendSystem(
        [`⏱ Run ${theme.bold(p.runId)} scheduled to auto-resume ${formatResetCountdown(p.resumeAt)} (${p.resumeAt}).`],
        "info",
      );
    } else if (ev.type === "quota.run_resumed") {
      const p = ev.payload as { runId: string; trigger: string; resumedTaskIds: string[] };
      appendSystem(
        [`▶ Run ${theme.bold(p.runId)} resumed (${p.trigger}) — restarted ${p.resumedTaskIds.length} task(s).`],
        "success",
      );
    }
  };

  unsubClose = opts.client.onClose(() => {
    if (evictedByTakeover) return;
    void term.shutdown(`[Connection to harnessd closed — check ${config.stateDir}/daemon.log if unexpected.]`, {
      overlays,
      spinner,
      cleanup: cleanupFullExit,
    });
  });

  if (busy) spinner.start(`Thinking (${session.model ?? session.profileId})…`);
  refreshStatus();
  term.start();
  resumeEvents();
  tui.requestRender();
  const goalTimer = setInterval(async () => {
    if (pollingGoals) return;
    pollingGoals = true;
    await refreshGoals();
    pollingGoals = false;
  }, 3_000);
  try {
    await term.closed;
  } finally {
    clearInterval(goalTimer);
  }
  return exitAction;
}

/**
 * Focused Run Stream TUI (`pi-harness attach <run-id>`); `Ctrl+T` opens the read-only `/runs` tree without detaching.
 */
export async function runNativeScrollbackTui(opts: {
  client: DaemonClient;
  runId: string;
  initialTaskId?: string | null;
}): Promise<TuiExitAction> {
  // Handshake BEFORE touching terminal raw mode
  const snap = await opts.client.request<{
    run: RunRecord;
    tasks: TaskRecord[];
    escalations: EscalationRecord[];
    quotaPause?: RunQuotaPauseStatus | null;
  }>("tree.get", { runId: opts.runId });

  const focusedTaskId: string | null = opts.initialTaskId ?? null;
  let lastSeq = 0;
  let run: RunRecord = snap.run;
  let tasks: TaskRecord[] = snap.tasks;
  let escalations: EscalationRecord[] = snap.escalations;
  let quotaPause: RunQuotaPauseStatus | null = snap.quotaPause ?? null;
  let streamingBuffer = "";
  let exitAction: TuiExitAction = { kind: "exit" };
  let evictedByTakeover = false;
  const activeRunToolPills = new Map<string, ToolPill>();

  const theme = createTheme();
  const term = new TerminalSession();
  const tui = term.tui;

  const streamContainer = new ToolTranscript(theme);
  const pendingContainer = new Container();
  const pendingMarkdown = new Markdown("", 1, 0, theme.toMarkdownTheme());
  const spinner = new TimedSpinner(tui, theme);
  const focusedOrRootTask = () =>
    (focusedTaskId ? tasks.find((t) => t.id === focusedTaskId) : undefined) ??
    tasks.find((t) => t.tier === 1) ??
    tasks[0];
  const initialTask = focusedOrRootTask();
  const initialModel = initialTask?.model ?? initialTask?.profileId ?? null;
  const initialEffort = initialTask?.effort ?? null;

  const statusBar = new StatusBar(theme, {
    tone: "accent",
    modeLabel: opts.runId,
    modelLabel: initialModel ? `${focusedTaskId ?? "ALL"} · ${initialModel}` : (focusedTaskId ?? "ALL"),
    effortLabel: initialEffort,
    goalsSummary: `${tasks.filter((t) => t.status === "running").length} running`,
  });
  const composer = new Composer(tui, theme, {
    placeholder: "Steer focused agent…",
    slashCommands: autocompleteCommands("run"),
  });
  const footerHint = new FooterHint(theme);
  const overlays = new OverlayManager(tui, theme, composer);

  const runTitle = [
    `pi-harness run ${opts.runId}`,
    ...(initialModel ? [initialModel] : []),
    ...(initialEffort ? [initialEffort] : []),
  ].join(" · ");

  streamContainer.addChild(
    new BannerCard(
      theme,
      runTitle,
      `status: ${run.status} · focus: ${focusedTaskId ?? "ALL"}`,
    ),
  );

  tui.addChild(streamContainer);
  tui.addChild(pendingContainer);
  tui.addChild(new Spacer(1));
  tui.addChild(spinner);
  tui.addChild(statusBar);
  tui.addChild(composer);
  tui.addChild(footerHint);
  tui.setFocus(composer);

  const refreshStatus = () => {
    const running = tasks.filter((t) => t.status === "running").length;
    const pausedQuota = tasks.filter((t) => t.status === "paused_quota").length;
    const isQuotaPaused = run.status === "paused_quota" || (running === 0 && pausedQuota > 0);
    const quotaAlert = isQuotaPaused
      ? quotaPause?.resumeAt
        ? `⏸ quota (${formatResetCountdown(quotaPause.resumeAt)})`
        : "⏸ quota (Ctrl+T, then w)"
      : null;

    const activeTask = focusedOrRootTask();
    const activeModel = activeTask?.model ?? activeTask?.profileId ?? null;
    statusBar.update({
      tone: escalations.length > 0 ? "error" : isQuotaPaused ? "warning" : running > 0 ? "accent" : "success",
      modeLabel: `${opts.runId} (${run.status})`,
      modelLabel: activeModel ? `focus:${focusedTaskId ?? "ALL"} · ${activeModel}` : `focus:${focusedTaskId ?? "ALL"}`,
      effortLabel: activeTask?.effort ?? null,
      goalsSummary: isQuotaPaused ? `${pausedQuota} paused` : `${running} running`,
      alertText: escalations.length > 0 ? `${escalations.length} esc` : quotaAlert,
    });
  };

  const flushStreaming = () => {
    if (streamingBuffer) {
      streamContainer.addChild(new AssistantCard(theme, streamingBuffer));
      streamingBuffer = "";
      pendingContainer.clear();
    }
  };

  const appendEvent = (ev: DaemonEventFrame) => {
    // Frames tagged with a watchId belong to the overlay that started that watch.
    if (isWatchFrame(ev)) return;
    if (ev.type === "session.taken_over") {
      evictedByTakeover = true;
      const p = ev.payload as { sessionId?: string };
      void term.shutdown(
        `[Detached — session ${p.sessionId ?? opts.runId} was taken over by another terminal.]`,
        { overlays, spinner, cleanup: cleanupFullExit },
      );
      return;
    }
    if (ev.seq > lastSeq) lastSeq = ev.seq;
    if (focusedTaskId && ev.taskId && ev.taskId !== focusedTaskId) return;

    if (ev.type === "agent.text_delta") {
      const text = String((ev.payload as { text?: string })?.text ?? "");
      if (!text) return;
      streamingBuffer += text;
      if (pendingContainer.children.length === 0) pendingContainer.addChild(pendingMarkdown);
      pendingMarkdown.setText(streamingBuffer);
      tui.requestRender();
      return;
    }

    flushStreaming();
    const prefix = ev.taskId ? `[${ev.taskId}]` : `[run]`;

    if (ev.type === "agent.tool") {
      const p = ev.payload as ToolEvent;
      const rawLabel = p.title ?? p.name ?? "tool";
      const displayLabel = rawLabel.replace(/^mcp[._/]harness[._/]+/i, "harness:").replace(/^mcp__harness__/i, "harness:");
      const key = p.id ? `${ev.taskId ?? ""}:${p.id}` : `${ev.taskId ?? ""}:${displayLabel}`;
      const st = p.status === "completed" ? "ok" : p.status === "failed" ? "error" : "running";
      const existing = activeRunToolPills.get(key);
      if (existing && (p.id || st !== "running" || existing.status === "running")) {
        existing.update(st, p.error ?? p.result, p.args);
      } else {
        const pill = new ToolPill(theme, st, `${prefix} ${displayLabel}`, p.args);
        pill.update(st, p.error ?? p.result);
        activeRunToolPills.set(key, pill);
        streamContainer.addChild(pill);
      }
    } else if (ev.type === "agent.error") {
      const formatted = formatAgentError(String((ev.payload as { error?: string })?.error ?? "Agent error"));
      streamContainer.addChild(new ErrorCard(theme, formatted));
    } else if (
      ev.type === "quota.agent_retrying" ||
      ev.type === "quota.agent_recovered" ||
      ev.type === "quota.agent_paused" ||
      ev.type === "quota.run_paused" ||
      ev.type === "quota.run_scheduled" ||
      ev.type === "quota.run_resumed"
    ) {
      const p = (ev.payload ?? {}) as Record<string, unknown>;
      if (ev.type === "quota.agent_retrying") {
        streamContainer.addChild(
          new SystemCard(
            theme,
            [
              `${prefix} Rate-limited (${String(p.reason ?? "429")}) — letting CLI retry natively (watchdog ${Math.round(Number(p.timeoutMs ?? 45000) / 1000)}s)…`,
            ],
            "muted",
          ),
        );
      } else if (ev.type === "quota.agent_recovered") {
        streamContainer.addChild(
          new SystemCard(theme, [`${prefix} Recovered after ${String(p.retryCount ?? 1)} transient 429 retry(ies).`], "success"),
        );
      } else if (ev.type === "quota.agent_paused") {
        const eta = p.resetAt ? ` · reset ${formatResetCountdown(String(p.resetAt))}` : "";
        streamContainer.addChild(
          new SystemCard(
            theme,
            [`${prefix} ⏸ Stopped early due to quota (${String(p.reason ?? "quota")}${eta}) — worktree & WIP preserved.`],
            "warning",
          ),
        );
      } else if (ev.type === "quota.run_paused") {
        const resumeAt = typeof p.resumeAt === "string" ? p.resumeAt : null;
        const mode = String(p.mode ?? "manual");
        const msg =
          mode === "auto_timer" && resumeAt
            ? `⏸ All workers exited — run paused on quota. Auto-resuming ${formatResetCountdown(resumeAt)}. Send any message to resume now, or press w in /runs (Ctrl+T) to change the wait.`
            : `⏸ All workers exited — run paused on quota (no Quota Endpoint wired). Send any message to resume now, or press w in /runs (Ctrl+T) to pick a wait timer.`;
        streamContainer.addChild(new SystemCard(theme, [msg], "warning"));
      } else if (ev.type === "quota.run_scheduled") {
        streamContainer.addChild(
          new SystemCard(
            theme,
            [`⏱ Auto-resume scheduled ${formatResetCountdown(String(p.resumeAt ?? ""))} (${String(p.resumeAt ?? "")}). Send any message to resume sooner.`],
            "info",
          ),
        );
      } else if (ev.type === "quota.run_resumed") {
        quotaPause = null;
        streamContainer.addChild(
          new SystemCard(
            theme,
            [`▶ Run resumed (${String(p.trigger ?? "manual")}) — restarted ${Array.isArray(p.resumedTaskIds) ? p.resumedTaskIds.length : 0} task(s).`],
            "success",
          ),
        );
      }
      void opts.client
        .request<{
          run: RunRecord;
          tasks: TaskRecord[];
          escalations: EscalationRecord[];
          quotaPause?: RunQuotaPauseStatus | null;
        }>("tree.get", { runId: opts.runId })
        .then((fresh) => {
          run = fresh.run;
          tasks = fresh.tasks;
          escalations = fresh.escalations;
          quotaPause = fresh.quotaPause ?? null;
          refreshStatus();
          tui.requestRender();
        })
        .catch(() => {});
    } else if (
      ev.type === "task.created" ||
      ev.type === "task.status" ||
      ev.type === "agent.rollover" ||
      ev.type === "escalation.created" ||
      ev.type === "merge.finished" ||
      ev.type === "goal.reviewed" ||
      ev.type === "goal.amended"
    ) {
      const p = (ev.payload ?? {}) as Record<string, unknown>;
      const detail =
        ev.type === "task.created"
          ? `spawned T${p.tier} "${p.title}" (${p.model ?? p.profileId})`
          : ev.type === "task.status"
            ? `${p.from} → ${p.to}${p.summary ? ` — ${p.summary}` : ""}`
            : ev.type === "agent.rollover"
              ? `clean rollover gen ${p.fromGen} → ${p.toGen} (${p.reason})`
              : ev.type === "escalation.created"
                ? `[${p.severity}/${p.category}] ${p.title}`
                : ev.type === "merge.finished"
                  ? `branch ${p.branch} → ${p.status}`
                  : String(p.summary ?? ev.type);
      const tone = ev.type === "escalation.created" ? "warning" : ev.type === "merge.finished" ? "success" : "muted";
      streamContainer.addChild(new SystemCard(theme, [`${prefix} ${detail}`], tone));
      void opts.client
        .request<{ run: RunRecord; tasks: TaskRecord[]; escalations: EscalationRecord[] }>("tree.get", { runId: opts.runId })
        .then((fresh) => {
          run = fresh.run;
          tasks = fresh.tasks;
          escalations = fresh.escalations;
          refreshStatus();
          tui.requestRender();
        })
        .catch(() => {});
    }

    refreshStatus();
    tui.requestRender();
  };

  let unsubEvent = () => {};
  let unsubClose = () => {};

  let leftRun = false;
  const detachListenersOnly = () => {
    leftRun = true;
    unsubEvent();
    unsubClose();
  };

  const cleanupFullExit = () => {
    detachListenersOnly();
    opts.client.close();
  };

  const openRunSessionsPicker = () => {
    void opts.client.request<AttachableSession[]>("sessions.list").then((sessions) => {
      let handle: ReturnType<OverlayManager["push"]> | null = null;
      const comp = new SessionsOverlay(
        theme,
        sessions,
        (chosen) => {
          exitAction = { kind: "switch", targetId: chosen.id, targetKind: chosen.kind };
          void term.shutdown(undefined, { overlays, spinner, cleanup: detachListenersOnly });
        },
        () => {
          exitAction = { kind: "switch", targetId: "__new__", targetKind: "chat" };
          void term.shutdown(undefined, { overlays, spinner, cleanup: detachListenersOnly });
        },
        () => handle?.close(),
        () => handle?.rerender(),
      );
      handle = overlays.push(
        {
          title: "Sessions & Attach Switcher",
          subtitle: `${sessions.length} session${sessions.length === 1 ? "" : "s"}`,
          footer: "Enter attach & take over · n new chat · Esc close",
        },
        comp,
        { width: "92%" },
      );
    });
  };

  const notice = (lines: string[], tone: "muted" | "success" | "warning" | "info" = "info") => {
    streamContainer.addChild(new SystemCard(theme, lines, tone));
    tui.requestRender();
  };

  const quitRun = () =>
    void term.shutdown(undefined, {
      overlays,
      spinner,
      resolveNote: () => resolveLeaveNote(opts.client),
      cleanup: cleanupFullExit,
    });

  const runError = (err: unknown, context: string) => {
    streamContainer.addChild(new ErrorCard(theme, formatAgentError(err, { context })));
    tui.requestRender();
  };

  const stopRun = (runId: string) =>
    void opts.client
      .request("run.stop", { runId })
      .then(() => (runId === opts.runId ? quitRun() : notice([`Stopped run ${runId}.`], "warning")))
      .catch((err) => runError(err, "run.stop"));

  const openQuotaWaitPicker = (runId: string, reason: string, resumeAt: string | null) => {
    let handle: ReturnType<OverlayManager["push"]> | null = null;
    const comp = new QuotaWaitPickerOverlay(
      theme,
      runId,
      reason,
      resumeAt,
      (choice) => {
        if (choice.kind === "now") {
          void opts.client.request("run.resume", { runId }).catch((err) => runError(err, "run.resume"));
        } else {
          void opts.client
            .request("run.schedule_resume", { runId, delayMs: choice.delayMs })
            .catch((err) => runError(err, "run.schedule_resume"));
        }
      },
      () => handle?.close(),
      () => handle?.rerender(),
    );
    handle = overlays.push(
      {
        title: "Quota Pause — Resume or Schedule Wait",
        subtitle: runId,
        footer: "↑/↓ move · Enter apply · Esc keep paused",
      },
      comp,
      { width: "80%" },
    );
  };

  const onRunsAction = (action: RunsAction, runId: string) => {
    if (action === "resume") {
      void opts.client
        .request<{ resumed: boolean; resumedTaskIds: string[] }>("run.resume", { runId })
        .then((res) =>
          notice(
            [res.resumed ? `Resumed ${runId} (${res.resumedTaskIds.length} task(s)).` : `Run ${runId} had no quota-paused tasks.`],
            res.resumed ? "success" : "muted",
          ),
        )
        .catch((err) => runError(err, "run.resume"));
    } else if (action === "wait") {
      if (runId === opts.runId) {
        openQuotaWaitPicker(runId, quotaPause?.reason ?? "Paused due to quota limit", quotaPause?.resumeAt ?? null);
      } else {
        void opts.client
          .request<{ quotaPause?: RunQuotaPauseStatus | null }>("run.get", { runId })
          .then((d) => openQuotaWaitPicker(runId, d.quotaPause?.reason ?? "Paused due to quota", d.quotaPause?.resumeAt ?? null))
          .catch(() => openQuotaWaitPicker(runId, "Paused due to quota", null));
      }
    } else {
      stopRun(runId);
    }
  };

  const openRuns = () =>
    openRunsOverlay({
      client: opts.client,
      theme,
      overlays,
      requestRender: () => tui.requestRender(),
      onAction: onRunsAction,
      runIds: [opts.runId],
    });

  const runRunCommand = (cmd: SlashCommand, arg: string) => {
    switch (cmd.name) {
      case "runs":
        return openRuns();
      case "sessions":
        if (!arg) return openRunSessionsPicker();
        exitAction = { kind: "switch", targetId: arg, targetKind: arg.startsWith("run-") ? "run" : "chat" };
        return void term.shutdown(undefined, { overlays, spinner, cleanup: detachListenersOnly });
      case "stop":
        return stopRun(opts.runId);
      case "help":
        return notice(renderHelp("run", theme));
      case "quit":
        return quitRun();
    }
  };

  const openRunPalette = () => {
    const commands = paletteCommands("run", (cmd) => runRunCommand(cmd, ""));
    let handle: ReturnType<OverlayManager["push"]> | null = null;
    const comp = new CommandPaletteOverlay(
      theme,
      commands,
      () => handle?.close(),
      () => handle?.rerender(),
    );
    handle = overlays.push(
      { title: "Command Palette", subtitle: `${commands.length} commands`, footer: "↑/↓ move · Enter run · Esc close" },
      comp,
      { width: "80%" },
    );
  };

  term.bindControls({
    overlays,
    spinner,
    footerHint,
    detachBanner: "[Exited.]",
    callbacks: {
      getComposerText: () => composer.getText(),
      clearComposerText: () => composer.clear(),
      isAutocompleteOpen: () => composer.isAutocompleteOpen(),
      isBusy: () => false,
      onAbortGeneration: () => {},
      onOpenTree: openRuns,
      onOpenPalette: openRunPalette,
      onResolveExitNote: () => resolveLeaveNote(opts.client),
      onShutdownCleanup: cleanupFullExit,
    },
  });

  composer.onSubmit = (raw) => {
    const text = raw.trim();
    composer.clear();
    if (!text) return;
    composer.addToHistory(text);

    const slash = parseSlashInput("run", text);
    if (slash && "error" in slash) return notice([slash.error], "warning");
    if (slash) return runRunCommand(slash.cmd, slash.arg);

    // Sending ANY message while the run is paused on quota manually resumes & resets the quota state machine!
    const hasPausedTasks = tasks.some((t) => t.status === "paused_quota");
    if (run.status === "paused_quota" || (hasPausedTasks && !tasks.some((t) => t.status === "running"))) {
      void opts.client
        .request<{ resumed: boolean; resumedTaskIds: string[] }>("run.resume", { runId: opts.runId, message: text })
        .then((res) => {
          streamContainer.addChild(new UserCard(theme, `[▶ resume] ${text}`));
          if (!res.resumed) {
            streamContainer.addChild(new SystemCard(theme, ["No quota-paused tasks remained to resume."], "muted"));
          }
          tui.requestRender();
        });
      return;
    }

    const targetId = focusedTaskId ?? tasks.find((t) => t.tier === 1)?.id;
    if (!targetId) {
      streamContainer.addChild(new SystemCard(theme, ["No active task to steer."], "warning"));
      tui.requestRender();
      return;
    }
    void opts.client.request("task.steer", { taskId: targetId, message: text }).then(() => {
      streamContainer.addChild(new UserCard(theme, `[→ ${targetId}] ${text}`));
      tui.requestRender();
    });
  };

  let earlyRunEvents: DaemonEventFrame[] | null = [];
  unsubEvent = opts.client.onEvent((ev) => {
    if (isWatchFrame(ev)) return;
    if (ev.type === "session.taken_over") appendEvent(ev);
    else if (ev.runId === opts.runId) {
      if (earlyRunEvents) earlyRunEvents.push(ev);
      else appendEvent(ev);
    }
  });
  unsubClose = opts.client.onClose(() => {
    if (evictedByTakeover) return;
    void term.shutdown("[Connection to harnessd closed.]", { overlays, spinner, cleanup: cleanupFullExit });
  });

  term.start();
  try {
    const attached = await opts.client.request<ReplayPage & { attachmentId: string; tookOver?: boolean }>("attach", {
      runId: opts.runId,
      taskId: focusedTaskId,
      sinceSeq: lastSeq,
    });
    await consumeReplay(opts.client, attached, { attachmentId: attached.attachmentId }, (ev) => {
      appendEvent({ kind: "event", ...ev });
    }, () => leftRun || evictedByTakeover);
    if (!leftRun && !evictedByTakeover) {
      for (const ev of earlyRunEvents) if (ev.seq > lastSeq) appendEvent(ev);
      earlyRunEvents = null;
      flushStreaming();
      if (attached.tookOver) {
        streamContainer.addChild(new SystemCard(theme, [`Took over ${opts.runId} (previous terminal was detached).`], "info"));
      }
      refreshStatus();
      tui.requestRender();
    }
  } catch (err) {
    if (!leftRun && !evictedByTakeover) {
      await term.shutdown(`[Failed to attach: ${err instanceof Error ? err.message : String(err)}]`, { overlays, spinner, cleanup: cleanupFullExit });
    }
  }
  await term.closed;
  return exitAction;
}
