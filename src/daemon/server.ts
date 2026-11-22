import { randomBytes } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, statSync, unlinkSync } from "node:fs";
import { createServer, type Server, type Socket } from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

export const DAEMON_PROTOCOL_REV = 5;

export function currentDaemonBuildId(): string {
  try {
    const serverFile = fileURLToPath(import.meta.url);
    return `${DAEMON_PROTOCOL_REV}:${Math.floor(statSync(serverFile).mtimeMs)}`;
  } catch {
    return `${DAEMON_PROTOCOL_REV}:0`;
  }
}
import { ModelCatalogCompiler } from "../catalog/compiler.js";
import { saveGlobalConfig, type HarnessConfig, type RunnerProfileConfig } from "../config.js";
import { ControlPlaneDb, type ReplayCursor, type EventRecord } from "../control-plane/db.js";
import { HarnessOrchestrator, type CustomRunnerFactory } from "../control-plane/orchestrator.js";
import { buildRunActivitySummary } from "../control-plane/telemetry.js";
import { summarizeCost } from "../cost/ledger.js";
import { DirectorRuntime } from "../director/runtime.js";
import type { GoalCriterion } from "../goals/engine.js";
import { formatChatEventSummary, type ChatControlEvent } from "../protocol/chat-events.js";
import type { DaemonEventFrame, RpcRequest, RpcResponse } from "../protocol/daemon-rpc.js";
import { attachNdjsonReader, writeDaemonMessage } from "../protocol/frames.js";
import { buildGoalDetailDisplay, buildGoalsTableDisplay, summarizeToolDisplay } from "../protocol/tool-display.js";
import { QuotaClient } from "../quota/client.js";
import { AcpRunner } from "../runners/acp-runner.js";
import { PiRunner } from "../runners/pi-runner.js";
import { PtyHost } from "./pty-host.js";

interface ClientAttachment {
  id: string;
  boundAtSeq: number;
  throughSeq: number;
  pendingSeq?: number;
  next?: ReplayCursor;
  socket: Socket;
  runId: string;
  taskId: string | null;
}

/** Read-only event subscription; never binds or evicts the socket's chat/run session. */
type ClientWatch = { throughSeq: number; pendingSeq?: number; next?: ReplayCursor; id: string; lastSeq: number; buffer: DaemonEventFrame[] | null } & (
  | { scope: "runs"; chatSessionId?: string; runIds?: Set<string> }
  | { scope: "task"; runId: string; taskId: string }
);

function chatSelection(params: Record<string, unknown>) {
  const { repoRoot, profileId, model, effort } = params;
  if (profileId != null && typeof profileId !== "string") throw new Error("Expected a profile ID");
  if (model != null && typeof model !== "string") throw new Error("Expected a model ID");
  return {
    repoRoot: String(repoRoot ?? process.cwd()),
    profileId: profileId ?? undefined,
    model,
    effort: typeof effort === "string" ? effort : null,
  };
}

function isRunsWatchEvent(type: string): boolean {
  return (
    type.startsWith("run.") ||
    type.startsWith("task.") ||
    type.startsWith("quota.") ||
    type.startsWith("escalation.") ||
    type === "agent.rollover"
  );
}

/**
 * `harnessd`: persistent headless daemon owning SQLite state, running agents,
 * worktree GC, model catalog + quota sync, and optional raw PTY shells.
 * Closing any terminal simply disconnects its socket — agents continue running uninterrupted.
 */
export class HarnessDaemon {
  readonly db: ControlPlaneDb;
  quota: QuotaClient;
  catalog: ModelCatalogCompiler;
  readonly orchestrator: HarnessOrchestrator;
  readonly director: DirectorRuntime;
  readonly ptyHost = new PtyHost();

  private server: Server | null = null;
  private allSockets = new Set<Socket>();
  private attachments = new Map<Socket, ClientAttachment>();
  private runBindings = new Map<string, Socket>();
  private chatBindings = new Map<string, Socket>();
  private chatTurns = new Map<string, { id: string; text: string; cancelling: boolean }>();
  private pendingWakeups = new Map<string, ChatControlEvent[]>();
  private socketToChatId = new Map<Socket, string>();
  private chatSubscribers = new Set<Socket>();
  private tuiSockets = new Set<Socket>();
  private takenOverSockets = new WeakSet<Socket>();
  private ptyDetachFns = new Map<Socket, Map<string, () => void>>();
  private watches = new Map<Socket, Map<string, ClientWatch>>();
  private unsubEvents: (() => void) | null = null;
  private sweepTimer: ReturnType<typeof setInterval> | null = null;
  private idleShutdownTimer: ReturnType<typeof setTimeout> | null = null;
  private autoQuitWhenIdle = false;
  private startedAt = Date.now();

  constructor(
    public config: HarnessConfig,
    opts?: { dbPath?: string; customRunnerFactory?: CustomRunnerFactory; autoQuitWhenIdle?: boolean },
  ) {
    this.autoQuitWhenIdle = Boolean(opts?.autoQuitWhenIdle);
    const dbPath = opts?.dbPath ?? join(config.stateDir, "control-plane.sqlite");
    this.db = new ControlPlaneDb(dbPath);
    this.quota = new QuotaClient(config.quotaEndpoint);
    this.catalog = new ModelCatalogCompiler(config, this.quota);
    this.orchestrator = new HarnessOrchestrator(this.db, config, this.catalog, this.quota, opts?.customRunnerFactory);
    this.director = new DirectorRuntime(this.db, config, this.catalog, this.quota, this.orchestrator, opts?.customRunnerFactory);
    this.director.setWakeHandler((chatSessionId, event) => {
      this.scheduleDirectorWakeup(chatSessionId, event);
    });
  }

  /**
   * Event-driven wakeup for Tier 0 Director when a typed control-plane event fires
   * (e.g. `goal.settled`, `timer.fired`) — zero polling, zero string-prefix parsing.
   */
  private scheduleDirectorWakeup(chatSessionId: string, event: ChatControlEvent): void {
    const activeTurn = this.chatTurns.get(chatSessionId);
    if (activeTurn) {
      const summary = formatChatEventSummary(event);
      void this.director
        .steerChat(chatSessionId, summary, { role: "system_notice", event })
        .then((accepted) => {
          if (accepted) {
            this.sendChatEvent(chatSessionId, activeTurn.id, "chat.steered", {
              message: summary,
              role: "system_notice",
              event,
            });
          } else {
            const q = this.pendingWakeups.get(chatSessionId) ?? [];
            q.push(event);
            this.pendingWakeups.set(chatSessionId, q);
          }
        })
        .catch(() => {});
      return;
    }
    void this.runDirectorWakeupTurn(chatSessionId, event);
  }

  private drainPendingWakeup(chatSessionId: string): void {
    if (this.chatTurns.has(chatSessionId)) return;
    const q = this.pendingWakeups.get(chatSessionId);
    const next = q?.shift();
    if (!next) return;
    if (q && q.length === 0) this.pendingWakeups.delete(chatSessionId);
    void this.runDirectorWakeupTurn(chatSessionId, next);
  }

  private async runDirectorWakeupTurn(chatSessionId: string, event: ChatControlEvent): Promise<void> {
    if (this.chatTurns.has(chatSessionId)) return;
    if (!this.director.chats.getSession(chatSessionId)) return;
    const id = `wake-${randomBytes(4).toString("hex")}`;
    const turn = { id, text: "", cancelling: false };
    this.chatTurns.set(chatSessionId, turn);
    this.sendChatEvent(chatSessionId, id, "chat.turn.start", {
      reason: event.kind,
      event,
      summary: formatChatEventSummary(event),
    });
    try {
      const result = await this.director.sendChatMessage(
        chatSessionId,
        "",
        (delta) => {
          turn.text += delta;
          this.sendChatEvent(chatSessionId, id, "chat.delta", { delta });
        },
        (tool) => {
          this.sendChatEvent(chatSessionId, id, "chat.tool", tool);
        },
        { event },
      );
      this.sendChatEvent(chatSessionId, id, "chat.turn.end", {
        assistantTurn: result.assistantTurn,
        cancelled: turn.cancelling,
      });
    } catch (err) {
      this.sendChatEvent(chatSessionId, id, "chat.turn.end", {
        error: err instanceof Error ? err.message : String(err),
        cancelled: turn.cancelling,
      });
    } finally {
      this.chatTurns.delete(chatSessionId);
      this.drainPendingWakeup(chatSessionId);
    }
  }

  private applyConfigMutation(next: HarnessConfig, opts?: { profilesChanged?: boolean }): HarnessConfig {
    // Keep default profile pointers valid if a profile was removed or disabled
    const enabledIds = next.profiles.filter((p) => p.enabled !== false).map((p) => p.id);
    const fallback = enabledIds[0];
    if (fallback) {
      if (!next.defaultOrchestratorProfile || !enabledIds.includes(next.defaultOrchestratorProfile)) {
        next = { ...next, defaultOrchestratorProfile: fallback };
      }
      if (!next.defaultWorkerProfile || !enabledIds.includes(next.defaultWorkerProfile)) {
        next = { ...next, defaultWorkerProfile: fallback };
      }
      if (!next.defaultTaskProfile || !enabledIds.includes(next.defaultTaskProfile)) {
        next = { ...next, defaultTaskProfile: fallback };
      }
    }

    this.config = next;
    Object.assign(this.orchestrator.config, next);
    this.quota = new QuotaClient(next.quotaEndpoint);
    this.orchestrator.quota = this.quota;
    this.catalog = new ModelCatalogCompiler(next, this.quota);
    this.orchestrator.catalog = this.catalog;
    this.director.updateConfig(next, this.catalog, this.quota);
    saveGlobalConfig(next);

    if (opts?.profilesChanged) {
      this.catalog.invalidateCache();
      // Warm up the newly enabled/changed CLI catalog in the background immediately
      void this.catalog.sync(process.cwd()).catch(() => {});
    }
    return next;
  }

  async start(): Promise<string> {
    this.orchestrator.reconcileStaleRunsOnStartup();
    this.db.timers.restorePending();
    await this.ptyHost.init();
    const sockPath = this.config.socketPath;
    mkdirSync(dirname(sockPath), { recursive: true });
    if (existsSync(sockPath)) {
      try {
        unlinkSync(sockPath);
      } catch {
        // ignore stale socket removal error
      }
    }

    this.unsubEvents = this.db.onEvent((ev) => this.fanoutEvent(ev));

    this.server = createServer((socket) => this.handleConnection(socket));
    await new Promise<void>((resolve, reject) => {
      this.server!.once("error", reject);
      this.server!.listen(sockPath, () => {
        try {
          chmodSync(sockPath, 0o600);
        } catch {
          // ignore on unsupported filesystems
        }
        resolve();
      });
    });

    // Periodic hourly worktree orphan sweep across known repos
    this.sweepTimer = setInterval(() => {
      void this.sweepAllKnownRepos().catch(() => {});
    }, 3_600_000);
    this.sweepTimer.unref();

    // Non-blocking initial quota sync (endpoint if configured + passive local CLI harvest)
    try {
      this.quota.harvestLocal(this.config.profiles, { force: true });
    } catch {
      // ignore local harvest errors on startup
    }
    void this.quota.sync(false).catch(() => {});

    return sockPath;
  }

  async stop(): Promise<void> {
    if (this.idleShutdownTimer) {
      clearTimeout(this.idleShutdownTimer);
      this.idleShutdownTimer = null;
    }
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
    this.director.dispose();
    this.orchestrator.dispose();
    this.pendingWakeups.clear();
    this.unsubEvents?.();
    this.ptyHost.closeAll();
    for (const sock of this.allSockets) {
      sock.destroy();
    }
    this.allSockets.clear();
    this.attachments.clear();
    this.chatBindings.clear();
    this.runBindings.clear();
    this.socketToChatId.clear();
    this.chatSubscribers.clear();
    this.tuiSockets.clear();
    this.watches.clear();
    if (this.server) {
      await new Promise<void>((resolve) => this.server!.close(() => resolve()));
      this.server = null;
    }
    if (existsSync(this.config.socketPath)) {
      try {
        unlinkSync(this.config.socketPath);
      } catch {
        // ignore
      }
    }
    this.db.close();
  }

  private async sweepAllKnownRepos(): Promise<void> {
    const repos = new Set(this.db.listRuns(100).map((r) => r.repoRoot));
    for (const repo of repos) {
      await this.orchestrator.getWorktreeManager(repo).sweep();
    }
  }

  private getRunIdsForChatSession(chatSessionId: string): string[] {
    const ids = new Set<string>();
    for (const g of this.director.goals.listGoals({ chatSessionId })) {
      if (g.linkedRunId) ids.add(g.linkedRunId);
    }
    const dirRunId = `director-tasks-${chatSessionId}`;
    if (this.db.getRun(dirRunId)) ids.add(dirRunId);
    return [...ids];
  }

  private getChatSessionIdForRun(runId: string): string | null {
    if (runId.startsWith("director-tasks-")) {
      return runId.slice("director-tasks-".length) || null;
    }
    const row = this.db.db
      .prepare("SELECT chat_session_id FROM goals WHERE linked_run_id = ? AND chat_session_id IS NOT NULL ORDER BY updated_at DESC LIMIT 1")
      .get(runId) as { chat_session_id?: string | null } | undefined;
    return row?.chat_session_id ? String(row.chat_session_id) : null;
  }

  private fanoutEvent(ev: EventRecord): void {
    const frame: DaemonEventFrame = {
      kind: "event",
      seq: ev.seq,
      runId: ev.runId,
      taskId: ev.taskId,
      type: ev.type,
      payload: ev.payload,
      createdAt: ev.createdAt,
    };
    for (const att of this.attachments.values()) {
      if (att.runId !== ev.runId) continue;
      if (att.taskId && ev.taskId && att.taskId !== ev.taskId) continue;
      if (att.next) att.pendingSeq = ev.seq;
      else writeDaemonMessage(att.socket, frame);
    }
    const owningChatId = this.getChatSessionIdForRun(ev.runId);
    if (
      ev.type.startsWith("goal.") ||
      ev.type === "quota.agent_paused" ||
      ev.type === "quota.run_paused" ||
      ev.type === "quota.run_scheduled" ||
      ev.type === "quota.run_resumed"
    ) {
      for (const sock of this.chatSubscribers) {
        if (this.attachments.has(sock)) continue;
        const boundChatId = this.socketToChatId.get(sock);
        if (owningChatId && boundChatId && owningChatId !== boundChatId) continue;
        writeDaemonMessage(sock, frame);
      }
    }
    for (const [sock, map] of this.watches) {
      for (const w of map.values()) {
        if (ev.seq <= w.lastSeq) continue;
        if (w.scope === "runs") {
          if (!isRunsWatchEvent(ev.type)) continue;
          if (w.runIds && !w.runIds.has(ev.runId)) continue;
          if (w.chatSessionId && owningChatId !== w.chatSessionId) continue;
        } else if (ev.runId !== w.runId || ev.taskId !== w.taskId) {
          continue;
        }
        if (w.next) {
          w.pendingSeq = ev.seq;
          continue;
        }
        w.lastSeq = ev.seq;
        const watchFrame: DaemonEventFrame = { ...frame, watchId: w.id };
        if (w.buffer) w.buffer.push(watchFrame);
        else writeDaemonMessage(sock, watchFrame);
      }
    }
  }

  private startWatch(socket: Socket, p: Record<string, unknown>): unknown {
    const id = `watch-${randomBytes(4).toString("hex")}`;
    let watch: ClientWatch;
    let result: Record<string, unknown>;
    if (p.scope === "runs") {
      const lastSeq = this.db.getLatestEventSeq();
      const chatSessionId = typeof p.chatSessionId === "string" && p.chatSessionId ? p.chatSessionId : undefined;
      const runIds = Array.isArray(p.runIds) ? new Set((p.runIds as unknown[]).map((x) => String(x))) : undefined;
      watch = { id, scope: "runs", chatSessionId, runIds, throughSeq: lastSeq, lastSeq, buffer: [] };
      result = { watchId: id, replay: [], lastSeq };
    } else if (p.scope === "task") {
      const runId = String(p.runId);
      const taskId = String(p.taskId);
      const sinceSeq = typeof p.sinceSeq === "number" ? p.sinceSeq : 0;
      const lastSeq = Math.max(sinceSeq, this.db.getLatestEventSeq());
      const page = this.db.getReplayPage(runId, taskId, { sinceSeq, payloadOffset: 0 }, lastSeq);
      watch = { id, scope: "task", runId, taskId, throughSeq: lastSeq, lastSeq, next: page.next, buffer: [] };
      result = {
        watchId: id,
        ...page,
        lastSeq,
        run: this.db.getRun(runId) ?? undefined,
        task: this.db.getTask(taskId) ?? undefined,
      };
    } else {
      throw new Error(`Unknown watch scope: ${String(p.scope)}`);
    }
    let map = this.watches.get(socket);
    if (!map) {
      map = new Map();
      this.watches.set(socket, map);
    }
    map.set(id, watch);
    // Live events emitted before the RPC response is written are buffered, then flushed after it,
    // so the client always sees the replay first and every live frame exactly once.
    setImmediate(() => {
      const buffered = watch.buffer ?? [];
      watch.buffer = null;
      if (this.watches.get(socket)?.get(id) !== watch) return;
      for (const f of buffered) writeDaemonMessage(socket, f);
    });
    return result;
  }

  private cancelIdleShutdown(): void {
    if (this.idleShutdownTimer) {
      clearTimeout(this.idleShutdownTimer);
      this.idleShutdownTimer = null;
    }
  }

  private getLiveWorkSummary(): {
    running: boolean;
    activeChats: number;
    activeTasks: number;
    scheduledQuotaResumes: number;
    activeReviews: number;
    activePtys: number;
    otherTuiClients: number;
    description: string;
  } {
    const activeChats = this.director.activeChatCount();
    const activeTasks = this.orchestrator.activeTaskCount();
    const scheduledQuotaResumes = this.orchestrator.scheduledQuotaResumeCount();
    const activeReviews = this.director.activeReviewCount();
    const activePtys = this.ptyHost.activePtyCount();
    const otherTuiClients = this.tuiSockets.size;
    const parts: string[] = [];
    if (activeTasks > 0) parts.push(`${activeTasks} active agent task${activeTasks === 1 ? "" : "s"}`);
    if (scheduledQuotaResumes > 0) parts.push(`${scheduledQuotaResumes} scheduled quota resume${scheduledQuotaResumes === 1 ? "" : "s"}`);
    if (activeChats > 0) parts.push(`${activeChats} in-flight chat${activeChats === 1 ? "" : "s"}`);
    if (activeReviews > 0) parts.push(`${activeReviews} goal review${activeReviews === 1 ? "" : "s"}`);
    if (activePtys > 0) parts.push(`${activePtys} open shell${activePtys === 1 ? "" : "s"}`);
    if (otherTuiClients > 0) parts.push(`${otherTuiClients} other attached terminal${otherTuiClients === 1 ? "" : "s"}`);
    const running = activeChats + activeTasks + scheduledQuotaResumes + activeReviews + activePtys + otherTuiClients > 0;
    return {
      running,
      activeChats,
      activeTasks,
      scheduledQuotaResumes,
      activeReviews,
      activePtys,
      otherTuiClients,
      description: parts.join(", "),
    };
  }

  private evictSocket(prevSock: Socket, sessionId: string): void {
    this.takenOverSockets.add(prevSock);
    this.tuiSockets.delete(prevSock);
    this.chatSubscribers.delete(prevSock);
    this.attachments.delete(prevSock);
    const prevChat = this.socketToChatId.get(prevSock);
    if (prevChat) {
      if (this.chatBindings.get(prevChat) === prevSock) this.chatBindings.delete(prevChat);
      this.socketToChatId.delete(prevSock);
    }
    for (const [rid, s] of this.runBindings.entries()) {
      if (s === prevSock) this.runBindings.delete(rid);
    }
    writeDaemonMessage(prevSock, {
      kind: "event",
      seq: 0,
      runId: sessionId,
      taskId: sessionId,
      type: "session.taken_over",
      payload: { sessionId, reason: "Attached from another terminal" },
      createdAt: Date.now(),
    } satisfies DaemonEventFrame);
    setTimeout(() => {
      try {
        prevSock.end();
      } catch {
        // ignore
      }
    }, 60).unref();
    setTimeout(() => {
      try {
        if (!prevSock.destroyed) prevSock.destroy();
      } catch {
        // ignore
      }
    }, 200).unref();
  }

  private unbindSocketSession(socket: Socket): void {
    const oldChat = this.socketToChatId.get(socket);
    if (oldChat) {
      if (this.chatBindings.get(oldChat) === socket) this.chatBindings.delete(oldChat);
      this.socketToChatId.delete(socket);
    }
    const oldAtt = this.attachments.get(socket);
    if (oldAtt) {
      if (this.runBindings.get(oldAtt.runId) === socket) this.runBindings.delete(oldAtt.runId);
      this.attachments.delete(socket);
    }
  }

  private requireChatOwner(socket: Socket, chatSessionId: string): void {
    if (this.chatBindings.get(chatSessionId) !== socket) throw new Error("Chat is attached to another terminal or detached");
  }

  private sendChatEvent(chatSessionId: string, turnId: string, type: string, payload: Record<string, unknown>): void {
    const target = this.chatBindings.get(chatSessionId);
    if (!target || this.chatTurns.get(chatSessionId)?.id !== turnId) return;
    writeDaemonMessage(target, {
      kind: "event", seq: 0, runId: chatSessionId, taskId: chatSessionId, type,
      payload: { ...payload, chatSessionId, turnId }, createdAt: Date.now(),
    } satisfies DaemonEventFrame);
  }

  private chatHistoryPage(chatSessionId: string, afterSeq: number, historyThrough: number) {
    const sizes = this.db.db.prepare(`
      SELECT seq, length(CAST(content AS BLOB)) + coalesce(length(CAST(display_json AS BLOB)), 0) AS bytes
      FROM chat_turns WHERE chat_session_id = ? AND seq > ? AND seq <= ? ORDER BY seq LIMIT 100
    `).all(chatSessionId, afterSeq, historyThrough) as { seq: number; bytes: number }[];
    let bytes = 0;
    let count = 0;
    for (const row of sizes) {
      if (count && bytes + row.bytes > 512 * 1024) break;
      bytes += row.bytes;
      count++;
    }
    const history = this.director.chats.getHistory(chatSessionId, afterSeq, count);
    const last = history.at(-1)?.seq ?? historyThrough;
    return { history, historyNext: last < historyThrough ? last : null, historyThrough };
  }

  private bindChatSocket(socket: Socket, chatSessionId: string): { tookOver: boolean } {
    if (socket.destroyed || this.takenOverSockets.has(socket)) throw new Error("Chat connection closed or taken over");
    this.cancelIdleShutdown();
    const curChat = this.socketToChatId.get(socket);
    if (curChat !== chatSessionId) {
      this.unbindSocketSession(socket);
    }
    let tookOver = false;
    const prev = this.chatBindings.get(chatSessionId);
    if (prev && prev !== socket) {
      this.evictSocket(prev, chatSessionId);
      tookOver = true;
    }
    this.chatBindings.set(chatSessionId, socket);
    this.socketToChatId.set(socket, chatSessionId);
    this.chatSubscribers.add(socket);
    this.tuiSockets.add(socket);
    this.director.chats.touchSession(chatSessionId);
    return { tookOver };
  }

  private bindRunSocket(socket: Socket, runId: string, taskId: string | null): { tookOver: boolean } {
    this.cancelIdleShutdown();
    const curAtt = this.attachments.get(socket);
    if (!curAtt || curAtt.runId !== runId) {
      this.unbindSocketSession(socket);
    }
    let tookOver = false;
    const prev = this.runBindings.get(runId);
    if (prev && prev !== socket) {
      this.evictSocket(prev, runId);
      tookOver = true;
    }
    this.runBindings.set(runId, socket);
    const boundAtSeq = this.db.getLatestEventSeq();
    this.attachments.set(socket, { id: randomBytes(8).toString("hex"), boundAtSeq, throughSeq: boundAtSeq, socket, runId, taskId });
    this.tuiSockets.add(socket);
    return { tookOver };
  }

  private handleConnection(socket: Socket): void {
    this.allSockets.add(socket);
    this.cancelIdleShutdown();
    const detach = attachNdjsonReader(socket, {
      onMessage: (raw) => {
        const req = raw as RpcRequest;
        if (req?.kind !== "req") return;
        void this.dispatch(socket, req).then(
          (result) => writeDaemonMessage(socket, { kind: "res", id: req.id, ok: true, result } satisfies RpcResponse),
          (err) =>
            writeDaemonMessage(socket, {
              kind: "res",
              id: req.id,
              ok: false,
              error: err instanceof Error ? err.message : String(err),
            } satisfies RpcResponse),
        );
      },
      onEnd: () => this.cleanupSocket(socket),
      onError: () => this.cleanupSocket(socket),
    });

    socket.once("close", () => {
      detach();
      this.cleanupSocket(socket);
    });
  }

  private cleanupSocket(socket: Socket): void {
    this.allSockets.delete(socket);
    const wasTui = this.tuiSockets.delete(socket);
    const wasTakenOver = this.takenOverSockets.has(socket);
    this.unbindSocketSession(socket);
    this.chatSubscribers.delete(socket);
    this.watches.delete(socket);
    const ptyMap = this.ptyDetachFns.get(socket);
    if (ptyMap) {
      for (const fn of ptyMap.values()) fn();
      this.ptyDetachFns.delete(socket);
    }

    // If the last interactive TUI disconnected abruptly (e.g. terminal window closed) and nothing is running, quit cleanly
    if (this.autoQuitWhenIdle && wasTui && !wasTakenOver) {
      const work = this.getLiveWorkSummary();
      if (!work.running) {
        this.cancelIdleShutdown();
        this.idleShutdownTimer = setTimeout(() => {
          this.idleShutdownTimer = null;
          if (!this.getLiveWorkSummary().running) {
            void this.stop().finally(() => process.exit(0));
          }
        }, 1200);
        this.idleShutdownTimer.unref();
      }
    }
  }

  private async dispatch(socket: Socket, req: RpcRequest): Promise<unknown> {
    if (this.takenOverSockets.has(socket)) throw new Error("Session taken over by another terminal");
    const p = req.params ?? {};

    switch (req.op) {
      case "ping":
        return {
          pong: true,
          pid: process.pid,
          uptimeMs: Date.now() - this.startedAt,
          protocolRev: DAEMON_PROTOCOL_REV,
          buildId: currentDaemonBuildId(),
        };

      case "daemon.shutdown":
        setTimeout(() => {
          void this.stop().finally(() => process.exit(0));
        }, 25).unref();
        return { shuttingDown: true, pid: process.pid };

      case "status":
        return {
          pid: process.pid,
          uptimeMs: Date.now() - this.startedAt,
          socketPath: this.config.socketPath,
          hasLibghosttyVt: this.ptyHost.hasLibghosttyVt,
          runs: this.db.listRuns(20),
          goals: this.director.goals.listGoals(),
          profiles: this.config.profiles.map((pr) => ({ id: pr.id, kind: pr.kind, billing: pr.billing, enabled: pr.enabled !== false })),
        };

      case "config.get":
        return this.config;

      case "config.update": {
        const patch = (p.patch ?? {}) as Record<string, unknown>;
        const next: HarnessConfig = {
          ...this.config,
          ...(typeof patch.defaultOrchestratorProfile === "string"
            ? { defaultOrchestratorProfile: patch.defaultOrchestratorProfile }
            : {}),
          ...(typeof patch.defaultWorkerProfile === "string" ? { defaultWorkerProfile: patch.defaultWorkerProfile } : {}),
          ...(typeof patch.defaultTaskProfile === "string" ? { defaultTaskProfile: patch.defaultTaskProfile } : {}),
          ...(patch.rollover ? { rollover: { ...this.config.rollover, ...(patch.rollover as HarnessConfig["rollover"]) } } : {}),
          ...(patch.worktree ? { worktree: { ...this.config.worktree, ...(patch.worktree as HarnessConfig["worktree"]) } } : {}),
          ...(patch.goals ? { goals: { ...this.config.goals, ...(patch.goals as HarnessConfig["goals"]) } } : {}),
          ...(patch.events ? { events: { ...this.config.events, ...(patch.events as HarnessConfig["events"]) } } : {}),
          ...(patch.catalog ? { catalog: { ...this.config.catalog, ...(patch.catalog as HarnessConfig["catalog"]) } } : {}),
          ...(patch.quotaRetry
            ? { quotaRetry: { ...this.config.quotaRetry, ...(patch.quotaRetry as HarnessConfig["quotaRetry"]) } }
            : {}),
          ...("quotaEndpoint" in patch
            ? { quotaEndpoint: (patch.quotaEndpoint as HarnessConfig["quotaEndpoint"] | null) ?? undefined }
            : {}),
        };
        return this.applyConfigMutation(next);
      }

      case "profiles.upsert": {
        const profile = p.profile as RunnerProfileConfig;
        if (!profile?.id) throw new Error("profile.id is required");
        const idx = this.config.profiles.findIndex((x) => x.id === profile.id);
        const nextProfiles = [...this.config.profiles];
        if (idx >= 0) nextProfiles[idx] = profile;
        else nextProfiles.push(profile);
        return this.applyConfigMutation({ ...this.config, profiles: nextProfiles }, { profilesChanged: true });
      }

      case "profiles.remove": {
        const id = String(p.id);
        return this.applyConfigMutation(
          {
            ...this.config,
            profiles: this.config.profiles.filter((x) => x.id !== id),
          },
          { profilesChanged: true },
        );
      }

      case "profiles.probe": {
        const id = String(p.id);
        const profile = this.config.profiles.find((x) => x.id === id);
        if (!profile) throw new Error(`Unknown profile ${id}`);
        const cwd = String(p.cwd ?? process.cwd());
        const models =
          profile.kind === "pi" ? new PiRunner(profile).probeModels() : await new AcpRunner(profile).probeModels(cwd);
        return { id, models };
      }

      case "chat.open": {
        let session =
          typeof p.chatSessionId === "string" && p.chatSessionId
            ? this.director.chats.getSession(p.chatSessionId)
            : p.preferLastGlobal
              ? this.director.chats.getMostRecentSession()
              : null;

        if (typeof p.chatSessionId === "string" && p.chatSessionId && !session) {
          throw new Error(`Unknown chat session: ${p.chatSessionId}`);
        }
        if (!session) {
          session = await this.director.openDefaultChat(chatSelection(p));
        }
        const { tookOver } = this.bindChatSocket(socket, session.id);
        const turn = this.chatTurns.get(session.id);
        const { historyThrough } = this.db.db.prepare(
          "SELECT COALESCE(MAX(seq), 0) AS historyThrough FROM chat_turns WHERE chat_session_id = ?",
        ).get(session.id) as { historyThrough: number };
        return {
          session,
          ...this.chatHistoryPage(session.id, 0, historyThrough),
          activeTurnId: turn?.id ?? null,
          activeText: turn?.text ?? "",
          activePartial: turn ? this.director.getChatPartial(session.id) : "",
          goals: this.director.goals.listGoals({ chatSessionId: session.id }),
          config: this.config,
          tookOver,
          busy: Boolean(turn),
        };
      }

      case "chat.create": {
        const session = await this.director.createChat({
          ...chatSelection(p),
          title: typeof p.title === "string" ? p.title : undefined,
        });
        this.bindChatSocket(socket, session.id);
        return {
          session,
          history: [],
          historyThrough: 0,
          historyNext: null,
          activeTurnId: null,
          activeText: "",
          activePartial: "",
          goals: this.director.goals.listGoals({ chatSessionId: session.id }),
          config: this.config,
          tookOver: false,
          busy: false,
        };
      }

      case "sessions.list": {
        const chats = this.director.chats.listSessions(25).map((c) => {
          const bound = this.chatBindings.get(c.id);
          const busy = this.director.isChatBusy(c.id);
          const activeGoals = this.director.goals
            .listGoals({ chatSessionId: c.id })
            .filter((g) => ["executing", "verifying", "reviewing"].includes(g.status)).length;
          return {
            id: c.id,
            kind: "chat" as const,
            title: c.title,
            repoRoot: c.repoRoot,
            status: busy ? "busy" : activeGoals > 0 ? `${activeGoals} active goal${activeGoals === 1 ? "" : "s"}` : "idle",
            running: busy || activeGoals > 0,
            attached: Boolean(bound),
            attachedByMe: bound === socket,
            model: c.model ?? c.profileId,
            effort: c.effort,
            updatedAt: c.updatedAt,
          };
        });
        const runs = this.db.listRuns(20).map((raw) => {
          const r = this.orchestrator.getEffectiveRun(raw.id) ?? raw;
          const bound = this.runBindings.get(r.id);
          const live = this.orchestrator.isRunActiveInMemory(r.id);
          const quotaSt = this.orchestrator.getRunQuotaStatus(r.id);
          return {
            id: r.id,
            kind: "run" as const,
            title: (r.goal.split("\n")[0] ?? r.goal).slice(0, 80),
            repoRoot: r.repoRoot,
            status: live ? "running" : quotaSt ? (quotaSt.resumeAt ? "quota (timer)" : "paused_quota") : r.status,
            running: live || Boolean(quotaSt),
            attached: Boolean(bound),
            attachedByMe: bound === socket,
            model: null,
            effort: null,
            updatedAt: r.updatedAt,
          };
        });
        return [...chats, ...runs].sort(
          (a, b) => Number(b.running) - Number(a.running) || b.updatedAt - a.updatedAt,
        );
      }

      case "session.leave": {
        this.tuiSockets.delete(socket);
        this.unbindSocketSession(socket);
        this.chatSubscribers.delete(socket);
        const work = this.getLiveWorkSummary();
        const daemonWillQuit = this.autoQuitWhenIdle && !work.running;
        if (daemonWillQuit) {
          this.cancelIdleShutdown();
          setTimeout(() => {
            if (!this.getLiveWorkSummary().running) {
              void this.stop().finally(() => process.exit(0));
            }
          }, 35).unref();
        }
        return {
          daemonWillQuit,
          running: work.running,
          activeWorkSummary: work.description,
        };
      }

      case "chat.send": {
        const chatSessionId = String(p.chatSessionId);
        this.requireChatOwner(socket, chatSessionId);
        if (this.chatTurns.has(chatSessionId)) throw new Error("Director is already responding");
        const id = typeof p.turnId === "string" && p.turnId ? p.turnId : req.id;
        const turn = { id, text: "", cancelling: false };
        this.chatTurns.set(chatSessionId, turn);
        try {
          const result = await this.director.sendChatMessage(
            chatSessionId, String(p.text),
            (delta) => {
              turn.text += delta;
              this.sendChatEvent(chatSessionId, id, "chat.delta", { delta });
            },
            (tool) => {
              this.sendChatEvent(chatSessionId, id, "chat.tool", tool);
            },
          );
          this.sendChatEvent(chatSessionId, id, "chat.turn.end", { assistantTurn: result.assistantTurn, cancelled: turn.cancelling });
          return result;
        } catch (err) {
          this.sendChatEvent(chatSessionId, id, "chat.turn.end", {
            error: err instanceof Error ? err.message : String(err), cancelled: turn.cancelling,
          });
          throw err;
        } finally {
          this.chatTurns.delete(chatSessionId);
          this.drainPendingWakeup(chatSessionId);
        }
      }

      case "chat.steer":
      case "chat.cancel": {
        const chatSessionId = String(p.chatSessionId);
        this.requireChatOwner(socket, chatSessionId);
        const turn = this.chatTurns.get(chatSessionId);
        if (!turn || turn.id !== p.turnId || turn.cancelling) return { steered: false, cancelled: false };
        if (req.op === "chat.steer") {
          const message = String(p.message);
          if (await this.director.steerChat(chatSessionId, message, { role: "user" })) {
            this.sendChatEvent(chatSessionId, turn.id, "chat.steered", { message, role: "user" });
            return { steered: true };
          }
          this.requireChatOwner(socket, chatSessionId);
          if (this.chatTurns.get(chatSessionId) !== turn || turn.cancelling) return { steered: false, cancelled: false };
        }
        turn.cancelling = true;
        try {
          return { steered: false, cancelled: await this.director.cancelChat(chatSessionId) };
        } finally {
          turn.cancelling = false;
        }
      }

      case "chat.history":
        if (typeof p.afterSeq === "number" && typeof p.throughSeq === "number") {
          return this.chatHistoryPage(String(p.chatSessionId), p.afterSeq, p.throughSeq);
        }
        return this.director.chats.getHistory(
          String(p.chatSessionId),
          typeof p.sinceSeq === "number" ? p.sinceSeq : 0,
          Math.min(100, Math.max(1, Number(p.limit) || 100)),
        );

      case "chat.update":
        return this.director.chats.updateSession(String(p.chatSessionId), {
          profileId: typeof p.profileId === "string" ? p.profileId : undefined,
          model: p.model === null ? null : typeof p.model === "string" ? p.model : undefined,
          effort: p.effort === null ? null : typeof p.effort === "string" ? p.effort : undefined,
          title: typeof p.title === "string" ? p.title : undefined,
          modelSelectionContext:
            p.modelSelectionContext === null
              ? null
              : typeof p.modelSelectionContext === "string"
                ? p.modelSelectionContext
                : undefined,
        });

      case "chat.list":
        return this.director.chats.listSessions();

      case "kb.search":
        return this.db.kb.search({
          query: typeof p.query === "string" ? p.query : undefined,
          repoScope: typeof p.repoRoot === "string" ? p.repoRoot : undefined,
          includeCandidates: Boolean(p.includeCandidates),
          includeStale: Boolean(p.includeStale),
          limit: typeof p.limit === "number" ? p.limit : 20,
        });

      case "kb.get":
        return this.db.kb.getMany(Array.isArray(p.ids) ? (p.ids as string[]) : []);

      case "goal.create":
        return this.director.goals.createGoal({
          repoRoot: String(p.repoRoot ?? process.cwd()),
          chatSessionId: typeof p.chatSessionId === "string" ? p.chatSessionId : null,
          title: String(p.title),
          description: String(p.description ?? p.title),
          criteria: (p.criteria as Array<Omit<GoalCriterion, "id" | "required">>) ?? [],
          nonGoals: Array.isArray(p.nonGoals) ? (p.nonGoals as string[]) : [],
          verificationMode:
            typeof p.verificationMode === "string" &&
            ["auto", "none", "deterministic", "independent_review"].includes(p.verificationMode)
              ? (p.verificationMode as "auto" | "none" | "deterministic" | "independent_review")
              : undefined,
        });

      case "goal.commit":
        return await this.director.commitGoal(String(p.goalId), {
          profileId: typeof p.profileId === "string" ? p.profileId : undefined,
          model: typeof p.model === "string" ? p.model : undefined,
          effort: typeof p.effort === "string" ? p.effort : undefined,
        });

      case "goal.amend":
        return await this.director.amendGoal(String(p.goalId), {
          rationale: String(p.rationale ?? "Amended by user"),
          title: typeof p.title === "string" ? p.title : undefined,
          description: typeof p.description === "string" ? p.description : undefined,
          addCriteria: Array.isArray(p.addCriteria) ? (p.addCriteria as GoalCriterion[]) : undefined,
          removeCriteriaIds: Array.isArray(p.removeCriteriaIds) ? (p.removeCriteriaIds as string[]) : undefined,
          nonGoals: Array.isArray(p.nonGoals) ? (p.nonGoals as string[]) : undefined,
        });

      case "goal.list":
        return this.director.goals.listGoals({
          repoRoot: typeof p.repoRoot === "string" ? p.repoRoot : undefined,
          chatSessionId: typeof p.chatSessionId === "string" && p.chatSessionId ? p.chatSessionId : undefined,
        });

      case "goal.get": {
        const g = this.director.goals.getGoal(String(p.goalId));
        if (!g) throw new Error(`Unknown goal ${p.goalId}`);
        const reviews = this.director.goals.listReviews(g.id);
        const tasks = g.linkedRunId ? this.db.listTasks(g.linkedRunId) : [];
        const escalations = g.linkedRunId ? this.db.listEscalations(g.linkedRunId, "open") : [];
        const activity = g.linkedRunId ? buildRunActivitySummary(this.db, g.linkedRunId) : null;
        const display = buildGoalDetailDisplay({ goal: g, reviews, tasks, openEscalations: escalations, activity });
        if (typeof p.persistChatSessionId === "string" && p.persistChatSessionId) {
          this.director.chats.appendTurn(
            p.persistChatSessionId,
            "tool_output",
            summarizeToolDisplay("goal_get", display),
            { toolName: "goal_get", display },
          );
        }
        return { goal: g, reviews, tasks, escalations, activity, display };
      }

      case "goal.table_display": {
        const goals = this.director.goals.listGoals({
          repoRoot: typeof p.repoRoot === "string" ? p.repoRoot : undefined,
          chatSessionId: typeof p.chatSessionId === "string" && p.chatSessionId ? p.chatSessionId : undefined,
        });
        const display = buildGoalsTableDisplay(
          goals.map((goal) => ({
            goal,
            reviews: this.director.goals.listReviews(goal.id),
            tasks: goal.linkedRunId ? this.db.listTasks(goal.linkedRunId) : [],
            activity: goal.linkedRunId ? buildRunActivitySummary(this.db, goal.linkedRunId) : null,
            openEscalations: goal.linkedRunId ? this.db.listEscalations(goal.linkedRunId, "open") : [],
          })),
        );
        if (typeof p.persistChatSessionId === "string" && p.persistChatSessionId) {
          this.director.chats.appendTurn(
            p.persistChatSessionId,
            "tool_output",
            summarizeToolDisplay("goal_list", display),
            { toolName: "goal_list", display },
          );
        }
        return { goals, display };
      }

      case "goal.review":
        return await this.director.verifyAndReviewGoal(String(p.goalId), undefined, {
          verificationMode:
            typeof p.verificationMode === "string" &&
            ["auto", "none", "deterministic", "independent_review"].includes(p.verificationMode)
              ? (p.verificationMode as "auto" | "none" | "deterministic" | "independent_review")
              : undefined,
          summary: typeof p.summary === "string" ? p.summary : undefined,
        });

      case "goal.pause": {
        const g = this.director.goals.setStatus(String(p.goalId), "paused", "Paused by user");
        if (g.linkedRunId) await this.orchestrator.stopRun(g.linkedRunId, "Goal paused");
        return g;
      }

      case "goal.cancel": {
        const g = this.director.goals.setStatus(String(p.goalId), "cancelled", "Cancelled by user");
        if (g.linkedRunId) await this.orchestrator.stopRun(g.linkedRunId, "Goal cancelled");
        return g;
      }

      case "run.create":
        return await this.orchestrator.startRun({
          goal: String(p.goal),
          repoRoot: String(p.repoRoot ?? process.cwd()),
          profileId: typeof p.profileId === "string" ? p.profileId : undefined,
          model: typeof p.model === "string" ? p.model : undefined,
          effort: typeof p.effort === "string" ? p.effort : undefined,
        });

      case "run.list": {
        const limit = typeof p.limit === "number" ? p.limit : 20;
        if (typeof p.chatSessionId === "string" && p.chatSessionId) {
          return this.getRunIdsForChatSession(p.chatSessionId)
            .map((id) => this.orchestrator.getEffectiveRun(id))
            .filter((r): r is NonNullable<typeof r> => r !== null)
            .sort((a, b) => b.createdAt - a.createdAt)
            .slice(0, limit);
        }
        return this.db.listRuns(limit).map((r) => this.orchestrator.getEffectiveRun(r.id) ?? r);
      }

      case "run.get": {
        const runId = String(p.runId);
        return {
          run: this.orchestrator.getEffectiveRun(runId),
          tasks: this.db.listTasks(runId),
          escalations: this.db.listEscalations(runId),
          worktrees: this.db.listWorktrees(runId),
          quotaPause: this.orchestrator.getRunQuotaStatus(runId),
        };
      }

      case "run.stop":
        await this.orchestrator.stopRun(String(p.runId), typeof p.reason === "string" ? p.reason : "Stopped by user");
        return { stopped: true };

      case "run.resume": {
        const runId = String(p.runId);
        return await this.orchestrator.resumeRun(runId, {
          trigger: "manual",
          userMessage: typeof p.message === "string" && p.message.trim() ? p.message.trim() : undefined,
          note: typeof p.note === "string" && p.note.trim() ? p.note.trim() : undefined,
        });
      }

      case "task.resume": {
        const task = await this.orchestrator.resumeTask(String(p.taskId), {
          note: typeof p.note === "string" ? p.note : undefined,
          model: typeof p.model === "string" ? p.model : undefined,
          effort: typeof p.effort === "string" ? p.effort : undefined,
          profileId: typeof p.profileId === "string" ? p.profileId : undefined,
        });
        return { resumed: true, task };
      }

      case "run.schedule_resume": {
        const runId = String(p.runId);
        const delayMs = Number(p.delayMs);
        if (!Number.isFinite(delayMs) || delayMs <= 0) {
          throw new Error("delayMs must be a positive number of milliseconds");
        }
        return this.orchestrator.scheduleRunQuotaResume(runId, delayMs);
      }

      case "attach": {
        const runId = String(p.runId);
        const taskId = typeof p.taskId === "string" ? p.taskId : null;
        const sinceSeq = typeof p.sinceSeq === "number" ? p.sinceSeq : 0;
        const { tookOver } = this.bindRunSocket(socket, runId, taskId);
        const attachment = this.attachments.get(socket)!;
        const page = this.db.getReplayPage(runId, taskId, { sinceSeq, payloadOffset: 0 }, attachment.boundAtSeq);
        attachment.next = page.next;
        return {
          attachmentId: attachment.id,
          attached: true,
          tookOver,
          run: this.db.getRun(runId),
          tasks: this.db.listTasks(runId),
          escalations: this.db.listEscalations(runId, "open"),
          quotaPause: this.orchestrator.getRunQuotaStatus(runId),
          ...page,
        };
      }

      case "replay.page": {
        const attachment = this.attachments.get(socket);
        const watch = this.watches.get(socket)?.get(String(p.watchId));
        const target = watch?.scope === "task" ? watch : attachment && attachment.id === p.attachmentId ? attachment : undefined;
        if (!target?.next) throw new Error("Replay expired or complete");
        const page = this.db.getReplayPage(target.runId, target.taskId, target.next, target.throughSeq,
          "boundAtSeq" in target ? target.boundAtSeq : undefined);
        // Finish each snapshot before advancing the watermark to matching live events.
        if (!page.next && target.pendingSeq !== undefined) {
          page.next = { sinceSeq: target.throughSeq, payloadOffset: 0 };
          target.throughSeq = target.pendingSeq;
          target.pendingSeq = undefined;
        }
        target.next = page.next;
        if (watch?.scope === "task") watch.lastSeq = target.throughSeq;
        return page;
      }

      case "detach":
        this.unbindSocketSession(socket);
        return { detached: true };

      case "tree.get": {
        const runId = String(p.runId);
        return {
          run: this.db.getRun(runId),
          tasks: this.db.listTasks(runId),
          escalations: this.db.listEscalations(runId, "open"),
          worktrees: this.db.listWorktrees(runId),
          quotaPause: this.orchestrator.getRunQuotaStatus(runId),
        };
      }

      case "runs.trees": {
        const limit = typeof p.limit === "number" ? Math.min(50, Math.max(1, Math.floor(p.limit))) : 50;
        const before = p.before as { id: string; createdAt: number } | undefined;
        const scopedChatId = typeof p.chatSessionId === "string" && p.chatSessionId ? p.chatSessionId : null;
        const runs = Array.isArray(p.runIds)
          ? (p.runIds as unknown[]).map((id) => this.db.getRun(String(id))).filter((r) => r !== null)
          : scopedChatId
            ? this.getRunIdsForChatSession(scopedChatId)
                .map((id) => this.db.getRun(id))
                .filter((r) => r !== null)
                .sort((a, b) => b.createdAt - a.createdAt)
            : this.db.listRuns(limit + 1, before);
        const hasMore = !Array.isArray(p.runIds) && !scopedChatId && runs.length > limit;
        if (hasMore) runs.pop();
        const last = runs.at(-1);
        return {
          next: hasMore && last ? { id: last.id, createdAt: last.createdAt } : undefined,
          generatedAt: Date.now(),
          runs: runs.map((raw) => {
            const run = this.orchestrator.getEffectiveRun(raw.id) ?? raw;
            return {
              run,
              tasks: this.db.listTasks(run.id),
              escalations: this.db.listEscalations(run.id, "open"),
              quotaPause: this.orchestrator.getRunQuotaStatus(run.id),
              activity: buildRunActivitySummary(this.db, run.id),
            };
          }),
        };
      }

      case "watch.start":
        return this.startWatch(socket, p);

      case "watch.stop": {
        const map = this.watches.get(socket);
        const stopped = map?.delete(String(p.watchId)) ?? false;
        if (map && map.size === 0) this.watches.delete(socket);
        return { stopped };
      }

      case "task.steer":
        return { steered: await this.orchestrator.steerTask(String(p.taskId), String(p.message)) };

      case "task.cancel":
        await this.orchestrator.cancelTask(String(p.taskId), String(p.reason ?? "Cancelled by user"));
        return { cancelled: true };

      case "escalation.ack":
        return this.db.ackEscalation(String(p.escalationId), String(p.resolution ?? "Resolved"), true);

      case "models.list": {
        this.quota.harvestLocal(this.config.profiles);
        const chatSessionId =
          typeof p.chatSessionId === "string" && p.chatSessionId
            ? p.chatSessionId
            : (this.socketToChatId.get(socket) ?? null);
        const catalog = await this.catalog.getCatalog({
          cwd: typeof p.cwd === "string" ? p.cwd : process.cwd(),
          role: typeof p.role === "string" ? p.role : undefined,
          excludeExhausted: Boolean(p.excludeExhausted),
          includeDisabled: Boolean(p.includeDisabled),
        });
        const sessionModelContext = chatSessionId ? this.director.chats.getModelSelectionContext(chatSessionId) : null;
        return { ...catalog, sessionModelContext };
      }

      case "models.sync": {
        this.quota.harvestLocal(this.config.profiles, { force: true });
        const chatSessionId =
          typeof p.chatSessionId === "string" && p.chatSessionId
            ? p.chatSessionId
            : (this.socketToChatId.get(socket) ?? null);
        const catalog = await this.catalog.sync(typeof p.cwd === "string" ? p.cwd : process.cwd());
        const sessionModelContext = chatSessionId ? this.director.chats.getModelSelectionContext(chatSessionId) : null;
        return { ...catalog, sessionModelContext };
      }

      case "quota.get":
        this.quota.harvestLocal(this.config.profiles);
        await this.quota.sync(false).catch(() => {});
        return {
          snapshot: this.quota.getEffectiveSnapshotJson(),
          configured: Boolean(this.config.quotaEndpoint?.url),
          coverage: this.quota.getProfileQuotaCoverage(this.config.profiles),
        };

      case "quota.sync":
        this.quota.harvestLocal(this.config.profiles, { force: true });
        if (this.config.quotaEndpoint?.url) {
          await this.quota.sync(true);
        }
        return {
          snapshot: this.quota.getEffectiveSnapshotJson(),
          configured: Boolean(this.config.quotaEndpoint?.url),
          coverage: this.quota.getProfileQuotaCoverage(this.config.profiles),
        };

      case "cost.get": {
        const runId = typeof p.runId === "string" && p.runId ? p.runId : null;
        const chatSessionId =
          typeof p.chatSessionId === "string" && p.chatSessionId ? p.chatSessionId : runId ? this.getChatSessionIdForRun(runId) : null;
        if (chatSessionId) {
          const session = this.director.chats.getSession(chatSessionId);
          if (!session) throw new Error(`Unknown chat session: ${chatSessionId}`);
          return summarizeCost(
            this.db,
            { chatSessionId, runIds: this.getRunIdsForChatSession(chatSessionId), snapshotScope: { kind: "chat", id: chatSessionId, label: session.title } },
            this.config.profiles,
          );
        }
        const run = runId ? this.db.getRun(runId) : null;
        if (!runId || !run) throw new Error(`Unknown run: ${runId ?? "(none)"}`);
        return summarizeCost(this.db, { chatSessionId: null, runIds: [runId], snapshotScope: { kind: "run", id: runId, label: run.goal } }, this.config.profiles);
      }

      case "worktrees.list": {
        const repoRoot = String(p.repoRoot ?? process.cwd());
        return await this.orchestrator.getWorktreeManager(repoRoot).listLive(typeof p.runId === "string" ? p.runId : undefined);
      }

      case "worktrees.clean": {
        const repoRoot = String(p.repoRoot ?? process.cwd());
        return await this.orchestrator.getWorktreeManager(repoRoot).sweep({
          runId: typeof p.runId === "string" ? p.runId : undefined,
          forceAll: Boolean(p.forceAll),
        });
      }

      case "pty.open": {
        const cwd = String(p.cwd ?? process.cwd());
        const info = this.ptyHost.open({
          cwd,
          cols: typeof p.cols === "number" ? p.cols : undefined,
          rows: typeof p.rows === "number" ? p.rows : undefined,
        });
        const { snapshot, detach } = this.ptyHost.attach(info.id, (chunk) => {
          writeDaemonMessage(socket, {
            kind: "event",
            seq: 0,
            runId: "pty",
            taskId: info.id,
            type: "pty.output",
            payload: { chunk },
            createdAt: Date.now(),
          } satisfies DaemonEventFrame);
        });
        let map = this.ptyDetachFns.get(socket);
        if (!map) {
          map = new Map();
          this.ptyDetachFns.set(socket, map);
        }
        map.set(info.id, detach);
        return { info, snapshot };
      }

      case "pty.input":
        this.ptyHost.write(String(p.ptyId), String(p.data));
        return { ok: true };

      case "pty.resize":
        this.ptyHost.resize(String(p.ptyId), Number(p.cols), Number(p.rows));
        return { ok: true };

      case "pty.close":
        this.ptyHost.close(String(p.ptyId));
        return { ok: true };

      case "tool.call": {
        const taskId = String(p.taskId);
        const name = String(p.name);
        const args = (p.args ?? {}) as Record<string, unknown>;
        if (taskId.startsWith("chat-")) {
          return await this.director.executeDirectorMcpTool(taskId, name, args);
        }
        return await this.orchestrator.toolExecutor.execute(String(p.runId), taskId, name, args);
      }

      default:
        throw new Error(`Unknown daemon op: ${req.op}`);
    }
  }
}
