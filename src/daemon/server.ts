import { randomBytes, randomUUID } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, readFileSync, realpathSync, statSync, unlinkSync, watch, type FSWatcher } from "node:fs";
import { createConnection, createServer, type Server, type Socket } from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

export const DAEMON_PROTOCOL_REV = 8;

export function currentDaemonBuildId(): string {
  try {
    const serverFile = fileURLToPath(import.meta.url);
    return `${DAEMON_PROTOCOL_REV}:${Math.floor(statSync(serverFile).mtimeMs)}`;
  } catch {
    return `${DAEMON_PROTOCOL_REV}:0`;
  }
}
import { ModelCatalogCompiler } from "../catalog/compiler.js";
import {
  diffHarnessConfig,
  findMisplacedConfigKeys,
  isPlainObject,
  normalizeWorktreeRoots,
  readRepoConfig,
  resolveConfig,
  saveGlobalConfig,
  serializeGlobalConfig,
  stableEqual,
  validateConfigRaw,
  type GoalsConfig,
  type HarnessConfig,
  type MisplacedConfigKey,
  type RemoteHostConfig,
  type RunnerProfileConfig,
} from "../config.js";
import { ControlPlaneDb, type ReplayCursor, type EventRecord, type RunRecord } from "../control-plane/db.js";
import { readRunDossier, readWorkspaceArtifact } from "../control-plane/dossiers.js";
import { compactTaskHandoff } from "../control-plane/handoffs.js";
import { directorTaskRunOwner } from "../control-plane/ids.js";
import { canonicalProjectPath, type ProjectRecord } from "../control-plane/projects.js";
import type { ChatSessionRecord } from "../director/session.js";
import { deriveRunStatus, HarnessOrchestrator, isDirectorTaskContainer, type CustomRunnerFactory } from "../control-plane/orchestrator.js";
import { loadOrCreateNodeId, parseSessionAddress, type SessionMessageRecord } from "../control-plane/session-messages.js";
import { buildRunActivitySummary } from "../control-plane/telemetry.js";
import { summarizeCost } from "../cost/ledger.js";
import { DirectorRuntime, StaleWakeEventError } from "../director/runtime.js";
import { parseGoalCriterionPatches, type GoalCriterion } from "../goals/engine.js";
import {
  coalesceGoalEvents,
  formatChatEventSummary,
  isGoalLifecycleEvent,
  isReviewInvalidated,
  type ChatControlEvent,
} from "../protocol/chat-events.js";
import type {
  DaemonEventFrame,
  DaemonOp,
  InboxItem,
  RpcRequest,
  RpcResponse,
  SnapshotWorktreeEntry,
} from "../protocol/daemon-rpc.js";
import { attachDaemonReader, writeDaemonMessage } from "../protocol/frames.js";
import { buildGoalDetailDisplay, buildGoalsTableDisplay, reconcileGoalWithLiveRun, summarizeToolDisplay } from "../protocol/tool-display.js";
import { QuotaClient } from "../quota/client.js";
import { ensureOctofs, resolveExternalMcpServers, shouldStageOctofs } from "../tools/native-tools.js";
import { AcpRunner } from "../runners/acp-runner.js";
import { PiRunner } from "../runners/pi-runner.js";
import { canonicalPath, runGit } from "../worktree/manager.js";
import { parseValidatedSshTarget, PeerManager, type PeerIdentity, type PeerLink, type RemoteHostStatus } from "./peer.js";
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
  | { scope: "run"; runId: string; taskId: null }
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
 * Shallow-merges a validated `config.update` goals patch. A `gateEnv` map replaces the previous one whole;
 * `gateShell: null | ""` and `gateEnv: null` remove the key instead of storing null.
 */
function patchGoals(prev: GoalsConfig, patch: Record<string, unknown>): GoalsConfig {
  const next = { ...prev, ...patch } as Record<string, unknown>;
  if (next.gateShell === null || next.gateShell === "") delete next.gateShell;
  if (next.gateEnv === null) delete next.gateEnv;
  return next as unknown as GoalsConfig;
}

/** Top-level fields `config.update` applies; profiles and remotes have dedicated ops, runtime paths are fixed. */
const CONFIG_UPDATE_FIELDS: ReadonlySet<string> = new Set([
  "defaultOrchestratorProfile",
  "defaultWorkerProfile",
  "defaultTaskProfile",
  "codeMode",
  "rollover",
  "worktree",
  "goals",
  "events",
  "catalog",
  "quotaRetry",
  "quotaEndpoint",
]);

/** Warning lines for known config keys found at unsupported locations; built from schema names only, never values. */
function formatMisplacedConfigWarnings(fileLabel: "config.json" | ".pi-harness.json", items: MisplacedConfigKey[]): string[] {
  return items.map((m) => `${fileLabel}: "${m.path}" is ignored; supported location: ${m.supported}`);
}

export const MAX_PEER_MESSAGE_HOPS = 10;

/** Single positive allowlist enforced identically on inbound peer links, reverse peer calls, and `peer.proxy`. */
const PEER_ALLOWED_OPS: ReadonlySet<DaemonOp> = new Set<DaemonOp>([
  "ping",
  "project.list",
  "project.get",
  "project.create",
  "project.update",
  "project.remove",
  "project.browse",
  "sessions.list",
  "chat.create",
  "session.deliver_message",
  "session.list_messages",
  "session.read",
  "peer.sync_outbox",
]);

/** Default safe reverse-direction allowlist when a remote host calls back into an initiating daemon (`reverseAccess !== "full"`). */
const REVERSE_MESSAGES_ONLY_OPS: ReadonlySet<DaemonOp> = new Set<DaemonOp>([
  "ping",
  "project.list",
  "project.get",
  "sessions.list",
  "session.deliver_message",
  "session.list_messages",
  "session.read",
  "peer.sync_outbox",
]);

const OUTBOX_TTL_MS = 24 * 60 * 60_000;
const PEER_HOP_WINDOW_MS = 30 * 60_000;

async function probeUnixSocketState(sockPath: string, timeoutMs = 250): Promise<"live" | "stale" | "unknown"> {
  return await new Promise<"live" | "stale" | "unknown">((resolve) => {
    let settled = false;
    const done = (state: "live" | "stale" | "unknown") => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try {
        probe.destroy();
      } catch {
        // ignore
      }
      resolve(state);
    };
    const probe = createConnection(sockPath);
    const timer = setTimeout(() => done("unknown"), timeoutMs);
    timer.unref?.();
    probe.once("connect", () => done("live"));
    probe.once("error", (err: NodeJS.ErrnoException) => {
      if (err.code === "ECONNREFUSED" || err.code === "ENOENT") {
        done("stale");
      } else {
        done("unknown");
      }
    });
  });
}

/**
 * `harnessd`: persistent headless daemon owning SQLite state, running agents,
 * worktree GC, model catalog + quota sync, and optional raw PTY shells.
 * Closing any terminal simply disconnects its socket — agents continue running uninterrupted.
 */
export class HarnessDaemon {
  readonly db: ControlPlaneDb;
  readonly nodeId: string;
  readonly peers: PeerManager;
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
  /** The goal's review invalidation epoch when a review-derived event was raised; a later epoch means the goal moved past it. */
  private eventReviewEpochs = new WeakMap<ChatControlEvent, number>();
  private socketToChatId = new Map<Socket, string>();
  private chatSubscribers = new Set<Socket>();
  private tuiSockets = new Set<Socket>();
  private takenOverSockets = new WeakSet<Socket>();
  private ptyDetachFns = new Map<Socket, Map<string, () => void>>();
  private watches = new Map<Socket, Map<string, ClientWatch>>();
  private flushingPeers = new Set<string>();
  private pendingFlushPeers = new Set<string>();
  private flushRetryTimers = new Map<string, ReturnType<typeof setTimeout>>();
  private unsubEvents: (() => void) | null = null;
  private sweepTimer: ReturnType<typeof setInterval> | null = null;
  private idleShutdownTimer: ReturnType<typeof setTimeout> | null = null;
  private configWatcher: FSWatcher | null = null;
  private configReloadTimer: ReturnType<typeof setTimeout> | null = null;
  private lastConfigRaw = "";
  /** Repository overlay snapshotted at startup; reloads layer `config.json` beneath it. */
  private readonly repoOverlay: Record<string, unknown>;
  /** Misplaced keys in the startup repo overlay; fixed for the daemon lifetime. */
  private readonly repoConfigWarnings: string[];
  /** Misplaced keys in the `config.json` text the effective config was last built from. */
  private globalConfigWarnings: string[] = [];
  /** Without a repo root, the constructor config is the layer beneath `config.json` on reload. */
  private readonly reloadBase: HarnessConfig | undefined;
  private autoQuitWhenIdle = false;
  private hasCustomRunner = false;
  private startedAt = Date.now();

  constructor(
    public config: HarnessConfig,
    opts?: {
      dbPath?: string;
      customRunnerFactory?: CustomRunnerFactory;
      autoQuitWhenIdle?: boolean;
      hostName?: string;
      repoRoot?: string;
    },
  ) {
    this.repoOverlay = readRepoConfig(opts?.repoRoot);
    this.repoConfigWarnings = formatMisplacedConfigWarnings(
      ".pi-harness.json",
      findMisplacedConfigKeys(this.repoOverlay, "repo"),
    );
    // Cloned because the orchestrator mutates the constructor config in place.
    this.reloadBase = opts?.repoRoot ? undefined : structuredClone(config);
    this.autoQuitWhenIdle = Boolean(opts?.autoQuitWhenIdle);
    this.hasCustomRunner = Boolean(opts?.customRunnerFactory);
    const dbPath = opts?.dbPath ?? join(config.stateDir, "control-plane.sqlite");
    this.db = new ControlPlaneDb(dbPath);
    this.nodeId = dbPath === ":memory:" ? `node-${randomBytes(6).toString("hex")}` : loadOrCreateNodeId(config.stateDir);
    this.quota = new QuotaClient(config.quotaEndpoint);
    this.catalog = new ModelCatalogCompiler(config, this.quota);
    this.orchestrator = new HarnessOrchestrator(this.db, config, this.catalog, this.quota, opts?.customRunnerFactory);
    this.director = new DirectorRuntime(this.db, config, this.catalog, this.quota, this.orchestrator, opts?.customRunnerFactory);
    this.peers = new PeerManager(
      this.nodeId,
      DAEMON_PROTOCOL_REV,
      (op, params, peer) => this.dispatchPeerRequest(op, params, peer),
      (peer) => {
        void this.flushOutboxForPeer(peer);
      },
      opts?.hostName,
    );
    this.director.setWakeHandler((chatSessionId, event) => {
      this.scheduleDirectorWakeup(chatSessionId, event);
    });
    this.director.setPeerBridge({
      listRemotes: () => this.listRemoteStatuses(),
      listSessions: async (host) =>
        (await this.listFederatedSessions(null, { includeRemotes: true, host })).filter((s) => s.kind === "chat"),
      createSession: (input) => this.createSessionOnHost(input),
      sendMessage: (input) => this.sendSessionMessage(input),
      readSession: (input) => this.readSessionOnHost(input),
    });
  }

  /**
   * Event-driven wakeup for Tier 0 Director when a typed control-plane event fires
   * (e.g. `goal.settled`, `timer.fired`) — zero polling, zero string-prefix parsing.
   */
  private scheduleDirectorWakeup(chatSessionId: string, event: ChatControlEvent): void {
    // The Director caused this goal event itself (e.g. its own goal_accept); waking it would be a self-wake.
    if (isDirectorSourcedGoalEvent(event)) return;
    this.stampReviewEpoch(event);
    if (event.kind === "goal.reviewed" && this.isStaleQueuedEvent(event)) return;
    const activeTurn = this.chatTurns.get(chatSessionId);
    if (activeTurn) {
      // Queue background goal/timer/peer events until the active Director turn finishes instead of interrupting mid-turn!
      const q = this.pendingWakeups.get(chatSessionId) ?? [];
      q.push(event);
      this.pendingWakeups.set(chatSessionId, q);
      return;
    }
    void this.runDirectorWakeupTurn(chatSessionId, event);
  }

  /**
   * Starts the next wake turn from the queue. A goal lifecycle event at the head takes every queued goal lifecycle
   * event with it, coalesced into one turn; other events keep their order. Each queued event is delivered, dropped as
   * stale, or left queued exactly once.
   */
  private drainPendingWakeup(chatSessionId: string): void {
    if (this.chatTurns.has(chatSessionId)) return;
    const queued = this.pendingWakeups.get(chatSessionId);
    if (!queued || queued.length === 0) {
      this.pendingWakeups.delete(chatSessionId);
      return;
    }
    const q = queued.flatMap((e): ChatControlEvent[] => (e.kind === "goal.events_batch" ? e.events : [e]));
    const persist = (a: ChatControlEvent[]) => {
      if (a.length > 0) this.pendingWakeups.set(chatSessionId, a);
      else this.pendingWakeups.delete(chatSessionId);
    };
    const head = q[0]!;
    if (!isGoalLifecycleEvent(head)) {
      q.shift();
      persist(q);
      if (this.isStaleQueuedEvent(head)) return this.drainPendingWakeup(chatSessionId);
      void this.runDirectorWakeupTurn(chatSessionId, head);
      return;
    }
    const goalEvents = q.filter(isGoalLifecycleEvent);
    // The only place the remainder is written back.
    persist(q.filter((e) => !isGoalLifecycleEvent(e)));
    const live = coalesceGoalEvents(goalEvents).filter((e) => !this.isStaleQueuedEvent(e));
    if (live.length === 0) return this.drainPendingWakeup(chatSessionId);
    void this.runDirectorWakeupTurn(chatSessionId, live.length === 1 ? live[0]! : { kind: "goal.events_batch", events: live });
  }

  /** Records the invalidation epoch a review-derived event was raised under, once, so later invalidations (incl. pause then resume) are detectable. */
  private stampReviewEpoch(event: ChatControlEvent): void {
    if (event.kind === "goal.events_batch") {
      for (const e of event.events) this.stampReviewEpoch(e);
      return;
    }
    if (
      event.kind === "goal.reviewed" ||
      (event.kind === "goal.settled" && (event.cause === "verification_error" || event.cause === "gates_failed"))
    ) {
      if (!this.eventReviewEpochs.has(event)) {
        this.eventReviewEpochs.set(event, this.director.getReviewInvalidationEpoch(event.goalId));
      }
    }
  }

  private isReviewEpochStale(event: ChatControlEvent, goalId: string): boolean {
    const epoch = this.eventReviewEpochs.get(event);
    return epoch !== undefined && this.director.getReviewInvalidationEpoch(goalId) !== epoch;
  }

  /** The still-live part of a wake event after asynchronous preparation: a batch keeps its live members; null when nothing is left. */
  private revalidateWakeEvent(event: ChatControlEvent): ChatControlEvent | null {
    if (event.kind === "goal.events_batch") {
      const live = event.events.filter((e) => !this.isStaleQueuedEvent(e));
      if (live.length === 0) return null;
      return live.length === 1 ? live[0]! : { kind: "goal.events_batch", events: live };
    }
    return this.isStaleQueuedEvent(event) ? null : event;
  }

  /** A queued event the goal or run has since moved past, or one the Director caused itself. */
  private isStaleQueuedEvent(next: ChatControlEvent): boolean {
    // Drop stale queued `goal.settled` snapshots whose run has since been resumed/reactivated
    // or whose goal has already reached a newer terminal/reviewing state.
    if (next.kind === "goal.settled") {
      const curRun = this.db.getRun(next.runId);
      const curGoal = this.director.goals.getGoal(next.goalId);
      if (curRun?.status === "active") return true;
      // A review failure of a goal the user has since paused is not reported, as when it is raised (notifyAutoVerifyFailure).
      if (curGoal?.status === "paused" && (next.cause === "verification_error" || next.cause === "gates_failed")) return true;
      if ((next.cause === "verification_error" || next.cause === "gates_failed") && this.isReviewEpochStale(next, next.goalId)) return true;
      if (curGoal && ["achieved", "cancelled", "reviewing", "verifying"].includes(curGoal.status)) return true;
      // An amended goal produces its own events; a settlement of the older version is stale.
      if (curGoal && curGoal.version !== next.goalVersion) return true;
    } else if (next.kind === "goal.escalated") {
      // Escalations are raised on live runs too, so an active run never drops them; only a disposed one does.
      if (this.db.getEscalation(next.escalationId)?.status !== "open") return true;
      const curGoal = this.director.goals.getGoal(next.goalId);
      if (curGoal && ["achieved", "cancelled"].includes(curGoal.status)) return true;
    }
    if (isDirectorSourcedGoalEvent(next)) return true;
    // Drop queued review notices the goal has since moved past: amended, paused, cancelled, re-reviewed, accepted, or resumed.
    if (next.kind === "goal.reviewed") {
      const curGoal = this.director.goals.getGoal(next.goalId);
      if (!curGoal || isReviewInvalidated(curGoal, next) || curGoal.status !== next.status) return true;
      if (this.isReviewEpochStale(next, next.goalId)) return true;
    }
    return false;
  }

  private async runDirectorWakeupTurn(chatSessionId: string, event: ChatControlEvent): Promise<void> {
    if (this.chatTurns.has(chatSessionId)) {
      const q = this.pendingWakeups.get(chatSessionId) ?? [];
      q.push(event);
      this.pendingWakeups.set(chatSessionId, q);
      return;
    }
    if (!this.director.chats.getSession(chatSessionId)) return;
    this.stampReviewEpoch(event);
    const liveEvent = this.revalidateWakeEvent(event);
    if (!liveEvent) {
      this.drainPendingWakeup(chatSessionId);
      return;
    }
    event = liveEvent;
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
        { event, revalidateEvent: (ev) => this.revalidateWakeEvent(ev) },
      );
      this.sendChatEvent(chatSessionId, id, "chat.turn.end", {
        assistantTurn: result.assistantTurn,
        cancelled: turn.cancelling,
      });
      if (event.kind === "session.message_received") {
        this.db.sessionMessages.markWakeStatus(event.messageId, "done");
      }
    } catch (err) {
      if (err instanceof StaleWakeEventError) {
        this.sendChatEvent(chatSessionId, id, "chat.turn.end", {
          assistantTurn: null,
          cancelled: turn.cancelling,
        });
      } else {
        this.sendChatEvent(chatSessionId, id, "chat.turn.end", {
          error: err instanceof Error ? err.message : String(err),
          cancelled: turn.cancelling,
        });
      }
    } finally {
      this.chatTurns.delete(chatSessionId);
      this.drainPendingWakeup(chatSessionId);
    }
  }

  /** Points default profiles that are unset, unknown, or disabled at the first enabled profile, when one exists. */
  private normalizeDefaultProfiles(config: HarnessConfig): HarnessConfig {
    const enabledIds = (Array.isArray(config.profiles) ? config.profiles : [])
      .filter(
        (p): p is RunnerProfileConfig =>
          Boolean(p) &&
          typeof p === "object" &&
          typeof (p as { id?: unknown }).id === "string" &&
          (p as { id: string }).id.length > 0 &&
          p.enabled !== false,
      )
      .map((p) => p.id);
    const fallback = enabledIds[0];
    if (!fallback) return config;
    let next = config;
    for (const key of ["defaultOrchestratorProfile", "defaultWorkerProfile", "defaultTaskProfile"] as const) {
      const id = next[key];
      if (!id || !enabledIds.includes(id)) next = { ...next, [key]: fallback };
    }
    return next;
  }

  private applyConfigMutation(
    next: HarnessConfig,
    opts?: { profilesChanged?: boolean; skipSave?: boolean },
  ): HarnessConfig {
    // Keep default profile pointers valid if a profile was removed or disabled
    next = this.normalizeDefaultProfiles(next);

    const prevEndpoint = JSON.stringify(this.config.quotaEndpoint ?? null);
    const nextEndpoint = JSON.stringify(next.quotaEndpoint ?? null);
    const prevRemotes = JSON.stringify(this.config.remotes ?? []);
    const nextRemotes = JSON.stringify(next.remotes ?? []);
    const prevExternalMcp = JSON.stringify(this.config.externalMcpServers ?? []);
    const nextExternalMcp = JSON.stringify(next.externalMcpServers ?? []);
    this.config = next;
    if (prevEndpoint !== nextEndpoint) {
      this.quota = new QuotaClient(next.quotaEndpoint);
    }
    this.catalog = new ModelCatalogCompiler(next, this.quota);
    // Live-propagate into Orchestrator (WorktreeManagers + MergeQueues + worker queue wakeup) and Director (GoalEngine + goal queue drain)
    this.orchestrator.updateConfig(next, this.catalog, this.quota);
    this.director.updateConfig(next, this.catalog, this.quota);
    if (prevRemotes !== nextRemotes) {
      this.peers.syncRemotes(next.remotes ?? []);
    }
    if (!this.hasCustomRunner && prevExternalMcp !== nextExternalMcp && shouldStageOctofs(next.externalMcpServers)) {
      void ensureOctofs(next.stateDir).catch(() => {});
    }

    if (!opts?.skipSave) this.persistConfig();

    if (opts?.profilesChanged) {
      this.catalog.invalidateCache();
      // Warm up the newly enabled/changed CLI catalog in the background immediately
      void this.catalog.sync(process.cwd()).catch(() => {});
    }
    return next;
  }

  /** Saves the effective config and records the written text so the watcher ignores the daemon's own write. */
  private persistConfig(): void {
    const serialized = serializeGlobalConfig(this.config);
    saveGlobalConfig(this.config);
    this.lastConfigRaw = serialized;
    this.setGlobalConfigWarnings(JSON.parse(serialized), false);
  }

  /** Stores the misplaced-key warnings for a parsed `config.json`; logs them when announced or changed. */
  private setGlobalConfigWarnings(parsed: unknown, announce: boolean): void {
    const next = formatMisplacedConfigWarnings("config.json", findMisplacedConfigKeys(parsed, "global"));
    const changed = !stableEqual(this.globalConfigWarnings, next);
    this.globalConfigWarnings = next;
    if (next.length > 0 && (announce || changed)) {
      for (const w of next) console.error(`[harnessd ${new Date().toISOString()}] config warning: ${w}`);
    } else if (next.length === 0 && changed) {
      console.error(
        `[harnessd ${new Date().toISOString()}] config warnings cleared: ${join(this.config.stateDir, "config.json")}`,
      );
    }
  }

  /**
   * Rebuilds the config from `config.json` + the startup repo overlay (or the constructor config) and applies it.
   * Missing, empty, or unchanged files are ignored; invalid files are rejected and the previous settings kept.
   */
  private reloadConfigFromDisk(): void {
    const cfgFile = join(this.config.stateDir, "config.json");
    let raw: string;
    try {
      raw = readFileSync(cfgFile, "utf8");
    } catch {
      return;
    }
    if (!raw.trim() || raw === this.lastConfigRaw) return;
    this.lastConfigRaw = raw;
    let parsed: unknown;
    let errors: string[];
    try {
      parsed = JSON.parse(raw);
      errors = validateConfigRaw(parsed);
    } catch {
      // Never log the parser message: it can quote fragments of the file, which may hold credentials.
      errors = ["Invalid JSON syntax"];
    }
    if (errors.length > 0) {
      console.error(
        `[harnessd ${new Date().toISOString()}] config reload rejected ${cfgFile} (keeping previous settings): ${errors.join("; ")}`,
      );
      return;
    }
    // Snapshot: until the first mutation, this.config is the object the orchestrator updates in place.
    const prev = structuredClone(this.config);
    const next = this.normalizeDefaultProfiles(
      resolveConfig(
        parsed as Record<string, unknown>,
        this.repoOverlay,
        { stateDir: prev.stateDir, socketPath: prev.socketPath },
        this.reloadBase,
      ),
    );
    if (diffHarnessConfig(prev, next).length > 0) {
      const applied = this.applyConfigMutation(next, {
        profilesChanged: !stableEqual(next.profiles, prev.profiles),
        skipSave: true,
      });
      const changes = diffHarnessConfig(prev, applied);
      if (changes.length > 0) {
        console.error(`[harnessd ${new Date().toISOString()}] config reload applied ${cfgFile} changes=${JSON.stringify(changes)}`);
      }
    }
    this.setGlobalConfigWarnings(parsed, true);
  }

  private stopConfigWatcher(): void {
    if (this.configReloadTimer) {
      clearTimeout(this.configReloadTimer);
      this.configReloadTimer = null;
    }
    if (this.configWatcher) {
      try {
        this.configWatcher.close();
      } catch {
        // ignore
      }
      this.configWatcher = null;
    }
  }

  /** Watches `<stateDir>/config.json` so edits on disk apply live without restarting `harnessd`. */
  private startConfigWatcher(): void {
    const stateDir = this.config.stateDir;
    const cfgFile = join(stateDir, "config.json");
    try {
      mkdirSync(stateDir, { recursive: true });
      if (existsSync(cfgFile)) {
        this.lastConfigRaw = readFileSync(cfgFile, "utf8");
      }
    } catch {
      return;
    }
    if (this.lastConfigRaw.trim()) {
      let parsed: unknown;
      try {
        parsed = JSON.parse(this.lastConfigRaw);
      } catch {
        // Invalid JSON yields no warnings; the next valid edit goes through the reload path.
      }
      this.setGlobalConfigWarnings(parsed, true);
    }

    try {
      this.configWatcher = watch(stateDir, (_eventType, filename) => {
        if (filename && filename !== "config.json") return;
        if (this.configReloadTimer) clearTimeout(this.configReloadTimer);
        this.configReloadTimer = setTimeout(() => {
          this.configReloadTimer = null;
          this.reloadConfigFromDisk();
        }, 120);
        this.configReloadTimer.unref?.();
      });
      this.configWatcher.on("error", (err) => {
        console.error(
          `[harnessd ${new Date().toISOString()}] config watcher stopped for ${cfgFile}: ${err instanceof Error ? err.message : String(err)}`,
        );
        this.stopConfigWatcher();
      });
      this.configWatcher.unref?.();
    } catch {
      // ignore unsupported fs.watch environments
    }
  }

  async start(): Promise<string> {
    await this.ptyHost.init();
    const sockPath = this.config.socketPath;
    mkdirSync(dirname(sockPath), { recursive: true, mode: 0o700 });
    if (existsSync(sockPath)) {
      const sockState = await probeUnixSocketState(sockPath);
      if (sockState === "live") {
        throw new Error(`Refusing to start harnessd: another live daemon is already listening on ${sockPath}`);
      }
      if (sockState === "unknown") {
        throw new Error(`Refusing to unlink ${sockPath}: socket liveness could not be confirmed (timed out or inaccessible)`);
      }
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

    // Reconcile stale runs/goals and restore durable timers AFTER the daemon socket is bound,
    // so immediate wakeups / MCP bridges can connect without racing socket creation!
    this.orchestrator.reconcileStaleRunsOnStartup();
    this.director.goals.reconcileStaleGoalsOnStartup();
    try {
      // Escalations left open on runs whose goals all settled (achieved/cancelled) are resolved once at startup.
      this.db.reconcileSettledGoalEscalations();
    } catch {
      // Best effort; the escalations stay listable and resolvable.
    }
    this.db.timers.restorePending();

    // Non-blocking startup orphan worktree sweep + periodic hourly sweep across known repos
    void this.sweepAllKnownRepos().catch(() => {});
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

    // Non-blocking background staging of native `octofs` MCP binary on real (non-test) daemons
    if (!this.hasCustomRunner && shouldStageOctofs(this.config.externalMcpServers)) {
      void ensureOctofs(this.config.stateDir).catch(() => {});
    }

    // Start auto-connect peer links for any configured remote hosts
    this.peers.syncRemotes(this.config.remotes ?? []);

    // Live-watch `<stateDir>/config.json` so external edits apply immediately without restart
    this.startConfigWatcher();
    for (const w of this.repoConfigWarnings) {
      console.error(`[harnessd ${new Date().toISOString()}] config warning: ${w}`);
    }

    // Recover any durable inbound peer wakeups that were persisted before a crash/restart
    for (const pending of this.db.sessionMessages.listPendingInboundWakes()) {
      if (!this.director.chats.getSession(pending.toSessionId)) {
        this.db.sessionMessages.markWakeStatus(pending.id, "done");
        continue;
      }
      this.scheduleDirectorWakeup(pending.toSessionId, {
        kind: "session.message_received",
        messageId: pending.id,
        fromHost: pending.fromHost,
        fromSessionId: pending.fromSessionId,
        fromProjectName: pending.fromProjectName,
        body: pending.body,
        expectReply: pending.expectReply,
        hopCount: pending.hopCount,
      });
    }

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
    this.stopConfigWatcher();
    for (const t of this.flushRetryTimers.values()) clearTimeout(t);
    this.flushRetryTimers.clear();
    this.flushingPeers.clear();
    this.pendingFlushPeers.clear();
    this.peers.dispose();
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
    const repos = new Set(this.db.listRuns(100).map((r) => canonicalPath(r.repoRoot)));
    for (const repo of repos) {
      try {
        await this.orchestrator.getWorktreeManager(repo).sweep();
      } catch {
        // One missing or failing repo must not stop the sweep of the others.
      }
    }
  }

  private getRunIdsForChatSession(chatSessionId: string): string[] {
    const ids = new Set<string>();
    for (const g of this.director.goals.listGoals({ chatSessionId })) {
      if (g.linkedRunId) ids.add(g.linkedRunId);
    }
    for (const run of this.orchestrator.listDirectorTaskRuns(chatSessionId)) ids.add(run.id);
    return [...ids];
  }

  private getChatSessionIdForRun(runId: string): string | null {
    if (isDirectorTaskContainer(runId)) return directorTaskRunOwner(runId);
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
      ev.type.startsWith("escalation.") ||
      ev.type.startsWith("merge.") ||
      ev.type.startsWith("timer.") ||
      ev.type === "run.status" ||
      ev.type === "run.created" ||
      ev.type === "task.created" ||
      ev.type === "task.status" ||
      ev.type === "task.resumed" ||
      ev.type === "worktree.created" ||
      ev.type === "worktree.removed" ||
      ev.type === "quota.agent_paused" ||
      ev.type === "quota.agent_retrying" ||
      ev.type === "quota.run_paused" ||
      ev.type === "quota.run_scheduled" ||
      ev.type === "quota.run_resumed" ||
      ev.type === "agent.timer_sleep" ||
      ev.type === "agent.rollover"
    ) {
      const evRepoRoot = this.db.getRun(ev.runId)?.repoRoot ?? null;
      for (const sock of this.chatSubscribers) {
        if (this.attachments.has(sock)) continue;
        const boundChatId = this.socketToChatId.get(sock);
        if (owningChatId && boundChatId && owningChatId !== boundChatId) continue;
        if (!owningChatId && boundChatId && evRepoRoot) {
          const boundRepo = this.director.chats.getSession(boundChatId)?.repoRoot;
          if (boundRepo && boundRepo !== evRepoRoot) {
            const norm = (p: string) => {
              try {
                return realpathSync(p);
              } catch {
                return p;
              }
            };
            if (norm(boundRepo) !== norm(evRepoRoot)) continue;
          }
        }
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
        } else if (w.scope === "run") {
          if (ev.runId !== w.runId) continue;
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
    } else if (p.scope === "run") {
      const runId = String(p.runId);
      const sinceSeq = typeof p.sinceSeq === "number" ? p.sinceSeq : 0;
      const lastSeq = Math.max(sinceSeq, this.db.getLatestEventSeq());
      const page = this.db.getReplayPage(runId, null, { sinceSeq, payloadOffset: 0 }, lastSeq);
      watch = { id, scope: "run", runId, taskId: null, throughSeq: lastSeq, lastSeq, next: page.next, buffer: [] };
      result = {
        watchId: id,
        ...page,
        lastSeq,
        run: this.db.getRun(runId) ?? undefined,
      };
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
    pendingTimers: number;
    pendingWakeups: number;
    queuedPeerMessages: number;
    activeReviews: number;
    activePtys: number;
    otherTuiClients: number;
    description: string;
  } {
    const activeChats = this.director.activeChatCount();
    const activeTasks = this.orchestrator.activeTaskCount();
    const scheduledQuotaResumes = this.orchestrator.scheduledQuotaResumeCount();
    const pendingTimers = this.db.timers.list({ status: "pending" }).length;
    const pendingWakeups = [...this.pendingWakeups.values()].reduce((sum, q) => sum + q.length, 0);
    this.db.sessionMessages.expireStaleOutbound(OUTBOX_TTL_MS);
    const knownPeers = this.peers.getKnownPeerSets();
    const queuedPeerMessages = this.db.sessionMessages.countQueuedOutboundForKnownPeers(knownPeers.hosts, knownPeers.nodes);
    const activeReviews = this.director.activeReviewCount();
    const activePtys = this.ptyHost.activePtyCount();
    const otherTuiClients = this.tuiSockets.size;
    const parts: string[] = [];
    if (activeTasks > 0) parts.push(`${activeTasks} active agent task${activeTasks === 1 ? "" : "s"}`);
    if (scheduledQuotaResumes > 0) parts.push(`${scheduledQuotaResumes} scheduled quota resume${scheduledQuotaResumes === 1 ? "" : "s"}`);
    if (pendingTimers > 0) parts.push(`${pendingTimers} pending timer${pendingTimers === 1 ? "" : "s"}`);
    if (pendingWakeups > 0) parts.push(`${pendingWakeups} pending wakeup${pendingWakeups === 1 ? "" : "s"}`);
    if (queuedPeerMessages > 0) parts.push(`${queuedPeerMessages} queued peer message${queuedPeerMessages === 1 ? "" : "s"}`);
    if (activeChats > 0) parts.push(`${activeChats} in-flight chat${activeChats === 1 ? "" : "s"}`);
    if (activeReviews > 0) parts.push(`${activeReviews} goal review${activeReviews === 1 ? "" : "s"}`);
    if (activePtys > 0) parts.push(`${activePtys} open shell${activePtys === 1 ? "" : "s"}`);
    if (otherTuiClients > 0) parts.push(`${otherTuiClients} other attached terminal${otherTuiClients === 1 ? "" : "s"}`);
    const running =
      activeChats +
        activeTasks +
        scheduledQuotaResumes +
        pendingTimers +
        pendingWakeups +
        queuedPeerMessages +
        activeReviews +
        activePtys +
        otherTuiClients >
      0;
    return {
      running,
      activeChats,
      activeTasks,
      scheduledQuotaResumes,
      pendingTimers,
      pendingWakeups,
      queuedPeerMessages,
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
    const detach = attachDaemonReader(socket, {
      onMessage: (raw) => {
        // Once upgraded via `peer.hello`, a socket is strictly a peer link:
        //  - `res` frames complete our outbound reverse requests on that link
        //  - `req` frames ONLY execute through the strict positive-allowlist `dispatchPeerRequest`
        const inboundPeer = this.peers.getInboundLinkForSocket(socket);
        if (inboundPeer) {
          const kind = (raw as { kind?: string })?.kind;
          if (kind === "res") {
            inboundPeer.handleWireFrame(raw);
            return;
          }
          if (kind === "req") {
            const req = raw as RpcRequest;
            void this.dispatchPeerRequest(req.op, req.params ?? {}, inboundPeer).then(
              (result) => writeDaemonMessage(socket, { kind: "res", id: req.id, ok: true, result } satisfies RpcResponse),
              (err) =>
                writeDaemonMessage(socket, {
                  kind: "res",
                  id: req.id,
                  ok: false,
                  error: err instanceof Error ? err.message : String(err),
                } satisfies RpcResponse),
            );
          }
          return;
        }
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

  private listRemoteStatuses(): RemoteHostStatus[] {
    return this.peers.listStatuses(this.db.sessionMessages.queuedCountsByHost());
  }

  private isLocalHostTarget(host: string | null | undefined): boolean {
    if (!host) return true;
    const h = host.trim();
    if (!h || h.toLowerCase() === "local" || h === this.nodeId) return true;
    // Only match localHostName if no configured, live, or previously observed remote peer uses that name
    if (
      h.toLowerCase() === this.peers.localHostName.toLowerCase() &&
      !this.peers.findLink(h) &&
      !this.peers.getRemoteConfig(h) &&
      !this.peers.getKnownNodeId(h)
    ) {
      return true;
    }
    return false;
  }

  private async listFederatedProjects(opts?: { includeRemotes?: boolean; host?: string }): Promise<Array<Record<string, unknown>>> {
    const requestedHost = opts?.host?.trim();
    if (requestedHost && !this.isLocalHostTarget(requestedHost)) {
      const remoteProjects = (await this.peers.callRemote<Array<Record<string, unknown>>>(
        requestedHost,
        "project.list",
        { includeRemotes: false },
        10_000,
      )) ?? [];
      const remoteNodeId = this.peers.findLink(requestedHost)?.remoteIdentity?.nodeId ?? this.peers.getKnownNodeId(requestedHost);
      return remoteProjects.map((pr) => ({ ...pr, host: requestedHost, nodeId: remoteNodeId ?? null }));
    }

    const local = (await this.db.projects.listProjects()).map((pr) => ({ ...pr, host: "local", nodeId: this.nodeId }));
    if (!opts?.includeRemotes || (requestedHost && this.isLocalHostTarget(requestedHost))) {
      return local;
    }

    const links = this.peers.connectedLinks();
    if (links.length === 0) return local;
    const settled = await Promise.allSettled(
      links.map(async (link) => {
        const items = (await link.request<Array<Record<string, unknown>>>("project.list", { includeRemotes: false }, 5_000)) ?? [];
        const hostName = link.alias;
        const nodeId = link.remoteIdentity?.nodeId ?? null;
        return items.map((pr) => ({ ...pr, host: hostName, nodeId }));
      }),
    );
    const out: Array<Record<string, unknown>> = [...local];
    for (const s of settled) {
      if (s.status === "fulfilled") out.push(...s.value);
    }
    return out;
  }

  private async listLocalSessions(socket: Socket | null) {
    const allProjects = await this.db.projects.listProjects();
    const projById = new Map(allProjects.map((pr) => [pr.id, pr]));
    const projByPath = new Map(allProjects.map((pr) => [pr.rootPath, pr]));
    const chats = this.director.chats.listSessions(25).map((c) => {
      const bound = this.chatBindings.get(c.id);
      const busy = this.director.isChatBusy(c.id);
      const activeGoals = this.director.goals
        .listGoals({ chatSessionId: c.id })
        .filter((g) => ["executing", "verifying", "reviewing"].includes(g.status)).length;
      const proj = (c.projectId ? projById.get(c.projectId) : undefined) ?? projByPath.get(c.repoRoot);
      return {
        id: c.id,
        host: "local",
        address: `local:${c.id}`,
        kind: "chat" as const,
        title: c.title,
        projectId: proj?.id ?? c.projectId ?? null,
        projectName: proj?.name ?? null,
        repoRoot: c.repoRoot,
        status: busy ? "busy" : activeGoals > 0 ? `${activeGoals} active goal${activeGoals === 1 ? "" : "s"}` : "idle",
        running: busy || activeGoals > 0,
        attached: Boolean(bound),
        attachedByMe: socket !== null && bound === socket,
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
      const proj = projByPath.get(r.repoRoot);
      return {
        id: r.id,
        host: "local",
        address: `local:${r.id}`,
        kind: "run" as const,
        title: (r.goal.split("\n")[0] ?? r.goal).slice(0, 80),
        projectId: proj?.id ?? null,
        projectName: proj?.name ?? null,
        repoRoot: r.repoRoot,
        status: live ? "running" : quotaSt ? (quotaSt.resumeAt ? "quota (timer)" : "paused_quota") : r.status,
        running: live || Boolean(quotaSt),
        attached: Boolean(bound),
        attachedByMe: socket !== null && bound === socket,
        model: null,
        effort: null,
        updatedAt: r.updatedAt,
      };
    });
    return [...chats, ...runs].sort((a, b) => Number(b.running) - Number(a.running) || b.updatedAt - a.updatedAt);
  }

  private async listFederatedSessions(
    socket: Socket | null,
    opts?: { includeRemotes?: boolean; host?: string },
  ): Promise<Array<Record<string, unknown>>> {
    const requestedHost = opts?.host?.trim();
    if (requestedHost && !this.isLocalHostTarget(requestedHost)) {
      const remote = (await this.peers.callRemote<Array<Record<string, unknown>>>(
        requestedHost,
        "sessions.list",
        { includeRemotes: false },
        10_000,
      )) ?? [];
      const remoteNodeId = this.peers.findLink(requestedHost)?.remoteIdentity?.nodeId ?? this.peers.getKnownNodeId(requestedHost);
      return remote.map((s) => ({
        ...s,
        host: requestedHost,
        nodeId: remoteNodeId ?? null,
        address: `${requestedHost}:${String(s.id)}`,
        attachedByMe: false,
      }));
    }

    const local = (await this.listLocalSessions(socket)).map((s) => ({ ...s, nodeId: this.nodeId }));
    if (!opts?.includeRemotes || (requestedHost && this.isLocalHostTarget(requestedHost))) {
      return local;
    }

    const links = this.peers.connectedLinks();
    if (links.length === 0) return local;
    const settled = await Promise.allSettled(
      links.map(async (link) => {
        const items = (await link.request<Array<Record<string, unknown>>>("sessions.list", { includeRemotes: false }, 5_000)) ?? [];
        const hostName = link.alias;
        const nodeId = link.remoteIdentity?.nodeId ?? null;
        return items.map((s) => ({
          ...s,
          host: hostName,
          nodeId,
          address: `${hostName}:${String(s.id)}`,
          attachedByMe: false,
        }));
      }),
    );
    const out: Array<Record<string, unknown>> = [...local];
    for (const s of settled) {
      if (s.status === "fulfilled") out.push(...s.value);
    }
    return out.sort(
      (a, b) => Number(Boolean(b.running)) - Number(Boolean(a.running)) || Number(b.updatedAt ?? 0) - Number(a.updatedAt ?? 0),
    );
  }

  private async createSessionOnHost(input: {
    host?: string;
    project?: string;
    title?: string;
    localFallbackRepoRoot?: string;
  }): Promise<Record<string, unknown>> {
    const targetHost = input.host?.trim();
    if (targetHost && !this.isLocalHostTarget(targetHost)) {
      const res = await this.peers.callRemote<{
        session: { id: string; repoRoot: string; title: string };
        project?: unknown;
        projectRegistered?: boolean;
      }>(
        targetHost,
        "chat.create",
        { projectId: input.project, title: input.title, headless: true },
        15_000,
      );
      return {
        host: targetHost,
        address: `${targetHost}:${res.session.id}`,
        session: res.session,
        project: res.project ?? null,
        ...(res.projectRegistered ? { projectRegistered: true } : {}),
      };
    }

    const explicitProject = input.project ? this.db.projects.resolveProject(input.project) : null;
    if (input.project && !explicitProject) {
      throw new Error(`Unknown project "${input.project}"`);
    }
    const created = await this.director.createChatDetailed({
      repoRoot: explicitProject?.rootPath ?? input.localFallbackRepoRoot ?? process.cwd(),
      projectId: explicitProject?.id ?? undefined,
      title: input.title,
    });
    const { session } = created;
    const { project: boundProject, registered } = this.ensureChatProject(session);
    return {
      host: "local",
      address: `local:${session.id}`,
      session,
      project: boundProject,
      ...(created.projectRegistered || registered ? { projectRegistered: true } : {}),
    };
  }

  /**
   * The chat's project, registering its path when the chat has none (e.g. its project was removed earlier).
   * `registered` is true only when this call inserted the project, which is then noted once in the chat.
   */
  private ensureChatProject(session: ChatSessionRecord): { project: ProjectRecord; registered: boolean } {
    const bound = session.projectId ? this.db.projects.getById(session.projectId) : null;
    if (bound) return { project: bound, registered: false };
    const { project, created } = this.db.projects.ensureProjectForPathWithStatus(session.repoRoot);
    if (created) this.director.noteProjectRegistered(session.id, project);
    return { project, registered: created };
  }

  private readLocalSession(sessionId: string, limit = 12): Record<string, unknown> {
    const bounded = Math.min(50, Math.max(1, limit));
    const session = this.director.chats.getSession(sessionId);
    if (!session) throw new Error(`Unknown chat session: ${sessionId}`);
    const project = session.projectId ? this.db.projects.getById(session.projectId) : null;
    const recentHistory = this.director.chats.getRecentHistory(sessionId, bounded);
    const turns = recentHistory.map((t) => ({
      seq: t.seq,
      role: t.role,
      content: t.content,
      createdAt: t.createdAt,
    }));
    const messages = this.db.sessionMessages.listForSession(sessionId, 20);
    return {
      host: "local",
      address: `local:${session.id}`,
      session: {
        id: session.id,
        title: session.title,
        projectId: session.projectId,
        projectName: project?.name ?? null,
        repoRoot: session.repoRoot,
        model: session.model ?? session.profileId,
        busy: this.director.isChatBusy(session.id),
      },
      turns,
      messages,
    };
  }

  private async readSessionOnHost(input: { target: string; limit?: number }): Promise<Record<string, unknown>> {
    const { host, sessionId } = parseSessionAddress(input.target);
    const limit = Math.min(50, Math.max(1, input.limit ?? 12));
    if (host && !this.isLocalHostTarget(host)) {
      const res = await this.peers.callRemote<Record<string, unknown>>(
        host,
        "session.read",
        { target: sessionId, limit },
        15_000,
      );
      return { ...res, host, address: `${host}:${sessionId}` };
    }
    return this.readLocalSession(sessionId, limit);
  }

  private lastUserTurnTimestamp(chatSessionId: string): number {
    const row = this.db.db
      .prepare("SELECT COALESCE(MAX(created_at), 0) AS ts FROM chat_turns WHERE chat_session_id = ? AND role = 'user'")
      .get(chatSessionId) as { ts: number } | undefined;
    return Number(row?.ts ?? 0);
  }

  private deliverLocalSessionMessage(input: {
    id: string;
    fromNode: string;
    fromHost: string;
    fromSessionId: string;
    fromProjectName?: string | null;
    toSessionId: string;
    body: string;
    replyToId?: string | null;
    expectReply?: boolean;
    hopCount?: number;
  }): { record: SessionMessageRecord; duplicate: boolean; woke: boolean; circuitBreakerTripped: boolean } {
    const targetSession = this.director.chats.getSession(input.toSessionId);
    if (!targetSession) {
      throw new Error(`Unknown target chat session "${input.toSessionId}" on host "${this.peers.localHostName}"`);
    }

    const { record, inserted } = this.db.sessionMessages.recordInbound({
      id: input.id,
      fromNode: input.fromNode,
      fromHost: input.fromHost,
      fromSessionId: input.fromSessionId,
      fromProjectName: input.fromProjectName ?? null,
      toNode: this.nodeId,
      toHost: this.peers.localHostName,
      toSessionId: input.toSessionId,
      body: input.body,
      replyToId: input.replyToId ?? null,
      expectReply: input.expectReply !== false,
      hopCount: input.hopCount ?? 1,
    });

    if (!inserted) {
      return { record, duplicate: true, woke: false, circuitBreakerTripped: false };
    }

    const sinceUser = this.lastUserTurnTimestamp(input.toSessionId);
    // Bound anti-loop window to 30m since last user turn so headless sessions without user turns aren't muted forever
    const hopWindowStart = Math.max(sinceUser, Date.now() - PEER_HOP_WINDOW_MS);
    const inboundInWindow = this.db.sessionMessages.countInboundSince(input.toSessionId, hopWindowStart);
    const hopCount = Math.max(record.hopCount, inboundInWindow);
    const circuitBreakerTripped = hopCount > MAX_PEER_MESSAGE_HOPS;

    // Emit live event to attached TUI socket (if any) so the user immediately sees peer traffic
    const bound = this.chatBindings.get(input.toSessionId);
    if (bound) {
      writeDaemonMessage(bound, {
        kind: "event",
        seq: 0,
        runId: input.toSessionId,
        taskId: input.toSessionId,
        type: "session.message_received",
        payload: {
          messageId: record.id,
          fromHost: record.fromHost,
          fromSessionId: record.fromSessionId,
          fromProjectName: record.fromProjectName,
          toSessionId: record.toSessionId,
          body: record.body,
          expectReply: record.expectReply,
          hopCount,
          circuitBreakerTripped,
        },
        createdAt: Date.now(),
      } satisfies DaemonEventFrame);
    }

    if (circuitBreakerTripped) {
      this.db.sessionMessages.markWakeStatus(record.id, "tripped");
      this.director.chats.appendTurn(
        input.toSessionId,
        "system_notice",
        `[PEER MESSAGE from ${record.fromHost}:${record.fromSessionId}] Auto-wakeup paused by anti-loop circuit breaker (${hopCount}/${MAX_PEER_MESSAGE_HOPS} consecutive peer hops without human input). Message: ${record.body}`,
      );
      return { record, duplicate: false, woke: false, circuitBreakerTripped: true };
    }

    const event: ChatControlEvent = {
      kind: "session.message_received",
      messageId: record.id,
      fromHost: record.fromHost,
      fromSessionId: record.fromSessionId,
      fromProjectName: record.fromProjectName,
      body: record.body,
      expectReply: record.expectReply,
      hopCount,
    };
    this.scheduleDirectorWakeup(input.toSessionId, event);
    return { record, duplicate: false, woke: true, circuitBreakerTripped: false };
  }

  private async sendSessionMessage(input: {
    fromSessionId: string;
    target: string;
    body: string;
    expectReply?: boolean;
    replyToId?: string;
  }): Promise<Record<string, unknown>> {
    const { host, sessionId: toSessionId } = parseSessionAddress(input.target);
    if (!toSessionId) throw new Error(`Invalid target session address: "${input.target}"`);

    const fromSess = this.director.chats.getSession(input.fromSessionId);
    const fromProject = fromSess?.projectId ? this.db.projects.getById(fromSess.projectId) : null;
    const prevMsg = input.replyToId ? this.db.sessionMessages.getById(input.replyToId) : null;
    const hopCount = prevMsg ? prevMsg.hopCount + 1 : 1;

    // Unified local delivery (same host, same or different project)
    if (this.isLocalHostTarget(host)) {
      const id = `smsg-${randomUUID()}`;
      const delivered = this.deliverLocalSessionMessage({
        id,
        fromNode: this.nodeId,
        fromHost: "local",
        fromSessionId: input.fromSessionId,
        fromProjectName: fromProject?.name ?? null,
        toSessionId,
        body: input.body,
        replyToId: input.replyToId ?? null,
        expectReply: input.expectReply !== false,
        hopCount,
      });
      return {
        messageId: delivered.record.id,
        target: `local:${toSessionId}`,
        status: "delivered",
        woke: delivered.woke,
        circuitBreakerTripped: delivered.circuitBreakerTripped,
      };
    }

    // Remote delivery: validate target host is a configured or previously observed peer so typos never pin auto-quit
    const targetHost = host!;
    const link = this.peers.findLink(targetHost);
    const resolvedToNode =
      link?.remoteIdentity?.nodeId ??
      this.peers.getKnownNodeId(targetHost) ??
      (prevMsg && prevMsg.fromHost.toLowerCase() === targetHost.toLowerCase() ? prevMsg.fromNode : null) ??
      this.db.sessionMessages.findKnownNodeForHost(targetHost);
    if (!link && !this.peers.isKnownPeer(targetHost) && !resolvedToNode) {
      throw new Error(`Unknown remote host "${targetHost}". Add it first with \`pi-harness remote add ${targetHost} <ssh-target>\` or /remotes.`);
    }

    const record = this.db.sessionMessages.enqueueOutbound({
      fromNode: this.nodeId,
      fromHost: this.peers.localHostName,
      fromSessionId: input.fromSessionId,
      fromProjectName: fromProject?.name ?? null,
      toNode: resolvedToNode,
      toHost: targetHost,
      toSessionId,
      body: input.body,
      replyToId: input.replyToId ?? null,
      expectReply: input.expectReply !== false,
      hopCount,
    });

    if (!link) {
      return {
        messageId: record.id,
        target: `${targetHost}:${toSessionId}`,
        status: "queued",
        woke: false,
        note: `Remote host "${targetHost}" is offline; message queued durably and will auto-deliver on reconnect.`,
      };
    }

    try {
      const res = await link.request<{ woke?: boolean; circuitBreakerTripped?: boolean }>(
        "session.deliver_message",
        {
          id: record.id,
          fromNode: record.fromNode,
          fromHost: record.fromHost,
          fromSessionId: record.fromSessionId,
          fromProjectName: record.fromProjectName,
          toSessionId: record.toSessionId,
          body: record.body,
          replyToId: record.replyToId,
          expectReply: record.expectReply,
          hopCount: record.hopCount,
        },
        15_000,
      );
      this.db.sessionMessages.markDelivered(record.id);
      return {
        messageId: record.id,
        target: `${targetHost}:${toSessionId}`,
        status: "delivered",
        woke: Boolean(res?.woke),
        circuitBreakerTripped: Boolean(res?.circuitBreakerTripped),
      };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (/Unknown target chat session|Conflicting session message envelope/i.test(msg)) {
        this.db.sessionMessages.markFailed(record.id, msg);
        throw new Error(msg);
      }
      // Schedule a bounded retry on the still-live peer link so transient timeouts never stall the outbox
      this.scheduleOutboxRetry(link);
      return {
        messageId: record.id,
        target: `${targetHost}:${toSessionId}`,
        status: "queued",
        woke: false,
        note: `Delivery deferred (${msg}); message remains queued and will retry automatically.`,
      };
    }
  }

  private scheduleOutboxRetry(peer: PeerLink, delayMs = 3_000): void {
    const key = peer.remoteIdentity?.nodeId ?? peer.alias.toLowerCase();
    if (this.flushRetryTimers.has(key)) return;
    const timer = setTimeout(() => {
      this.flushRetryTimers.delete(key);
      if (peer.isOpen) void this.flushOutboxForPeer(peer);
    }, delayMs);
    timer.unref?.();
    this.flushRetryTimers.set(key, timer);
  }

  private async flushOutboxForPeer(peer: PeerLink): Promise<number> {
    const key = peer.remoteIdentity?.nodeId ?? peer.alias.toLowerCase();
    if (this.flushingPeers.has(key)) {
      this.pendingFlushPeers.add(key);
      return 0;
    }
    this.flushingPeers.add(key);
    let flushed = 0;
    try {
      do {
        this.pendingFlushPeers.delete(key);
        this.db.sessionMessages.expireStaleOutbound(OUTBOX_TTL_MS);
        const configuredAliases = new Set((this.config.remotes ?? []).map((r) => r.name.toLowerCase()));
        const queued = this.db.sessionMessages.listQueuedOutboundForPeer({
          alias: peer.alias,
          direction: peer.direction,
          hostName: peer.remoteIdentity?.hostName ?? null,
          nodeId: peer.remoteIdentity?.nodeId ?? null,
          configuredAliases,
        });
        let hadTransientFailure = false;
        for (const msg of queued) {
          if (!peer.isOpen) break;
          if (Date.now() - msg.createdAt > OUTBOX_TTL_MS) {
            this.db.sessionMessages.markFailed(msg.id, "Expired in outbox after delivery TTL");
            continue;
          }
          try {
            await peer.request(
              "session.deliver_message",
              {
                id: msg.id,
                fromNode: msg.fromNode,
                fromHost: msg.fromHost,
                fromSessionId: msg.fromSessionId,
                fromProjectName: msg.fromProjectName,
                toSessionId: msg.toSessionId,
                body: msg.body,
                replyToId: msg.replyToId,
                expectReply: msg.expectReply,
                hopCount: msg.hopCount,
              },
              15_000,
            );
            this.db.sessionMessages.markDelivered(msg.id);
            flushed++;
          } catch (err) {
            const text = err instanceof Error ? err.message : String(err);
            if (/Unknown target chat session|Conflicting session message envelope/i.test(text)) {
              this.db.sessionMessages.markFailed(msg.id, text);
            } else {
              hadTransientFailure = true;
              continue; // Don't let one transient timeout permanently block unrelated messages; retry shortly
            }
          }
        }
        if (hadTransientFailure && peer.isOpen) {
          this.scheduleOutboxRetry(peer);
        }
      } while (this.pendingFlushPeers.has(key) && peer.isOpen);
    } finally {
      this.flushingPeers.delete(key);
      if (this.pendingFlushPeers.has(key)) {
        const replacement = this.peers.findLink(key);
        if (replacement && replacement.isOpen) {
          setImmediate(() => void this.flushOutboxForPeer(replacement));
        }
      }
    }
    return flushed;
  }

  /**
   * Strict positive-allowlist local-only handler for ALL peer-originated RPCs (both inbound sockets
   * after `peer.hello` and reverse RPCs over outbound links). Never forwards to a third host, enforces
   * reverse capability policy (`reverseAccess: "messages"` default vs `"full"`), and always derives
   * sender identity from the authenticated `PeerLink`.
   */
  private async dispatchPeerRequest(op: DaemonOp, p: Record<string, unknown>, peer: PeerLink): Promise<unknown> {
    if (!PEER_ALLOWED_OPS.has(op)) {
      throw new Error(`RPC "${op}" is not permitted over a peer link`);
    }
    if (peer.direction === "outbound") {
      const cfg = this.peers.getRemoteConfig(peer.alias);
      if (cfg?.reverseAccess !== "full" && !REVERSE_MESSAGES_ONLY_OPS.has(op)) {
        throw new Error(`Reverse RPC "${op}" requires reverseAccess: "full" on remote "${peer.alias}"`);
      }
    }
    switch (op) {
      case "ping": {
        const live = this.getLiveWorkSummary();
        return {
          pong: true,
          pid: process.pid,
          nodeId: this.nodeId,
          hostName: this.peers.localHostName,
          protocolRev: DAEMON_PROTOCOL_REV,
          running: live.running,
        };
      }
      case "project.list":
        return await this.db.projects.listProjects();
      case "project.get": {
        const ref = String(p.projectId ?? p.id ?? p.name ?? p.path ?? "");
        const proj = this.db.projects.resolveProject(ref);
        if (!proj) throw new Error(`Unknown project: ${ref}`);
        const all = await this.db.projects.listProjects();
        return { ...(all.find((x) => x.id === proj.id) ?? proj), nodeId: this.nodeId };
      }
      case "project.create": {
        const cloneUrl = typeof p.cloneUrl === "string" && p.cloneUrl.trim() ? p.cloneUrl.trim() : undefined;
        if (cloneUrl && (cloneUrl.startsWith("-") || !/^(?:https:\/\/|ssh:\/\/|git@)/i.test(cloneUrl))) {
          throw new Error(`Invalid clone URL "${cloneUrl}" (must use https://, ssh://, or git@)`);
        }
        const created = await this.db.projects.createProject({
          path: String(p.path ?? ""),
          name: typeof p.name === "string" ? p.name : undefined,
          description: typeof p.description === "string" ? p.description : undefined,
          createDir: p.createDir === undefined ? true : Boolean(p.createDir),
          gitInit: Boolean(p.gitInit),
          cloneUrl,
          defaultProfileId: typeof p.defaultProfileId === "string" ? p.defaultProfileId : undefined,
          defaultModel: typeof p.defaultModel === "string" ? p.defaultModel : undefined,
          defaultEffort: typeof p.defaultEffort === "string" ? p.defaultEffort : undefined,
        });
        const all = await this.db.projects.listProjects();
        return all.find((x) => x.id === created.id) ?? created;
      }
      case "project.update": {
        const ref = String(p.projectId ?? p.id ?? "");
        const updated = this.db.projects.updateProject(ref, {
          name: typeof p.name === "string" ? p.name : undefined,
          description: p.description === null ? null : typeof p.description === "string" ? p.description : undefined,
          defaultProfileId: p.defaultProfileId === null ? null : typeof p.defaultProfileId === "string" ? p.defaultProfileId : undefined,
          defaultModel: p.defaultModel === null ? null : typeof p.defaultModel === "string" ? p.defaultModel : undefined,
          defaultEffort: p.defaultEffort === null ? null : typeof p.defaultEffort === "string" ? p.defaultEffort : undefined,
        });
        const all = await this.db.projects.listProjects();
        return all.find((x) => x.id === updated.id) ?? updated;
      }
      case "project.remove": {
        const ref = String(p.projectId ?? p.id ?? "");
        return this.db.projects.removeProject(ref);
      }
      case "project.browse":
        return this.db.projects.browseDirectory(typeof p.path === "string" ? p.path : undefined);
      case "sessions.list":
        return await this.listLocalSessions(null);
      case "chat.create":
        return await this.createSessionOnHost({
          project: typeof p.projectId === "string" ? p.projectId : undefined,
          title: typeof p.title === "string" ? p.title : undefined,
        });
      case "session.deliver_message": {
        // Always derive sender node & host from the authenticated PeerLink (using our configured alias on outbound links)
        const authNode = peer.remoteIdentity?.nodeId;
        if (!authNode) throw new Error("Unauthenticated peer link");
        const authHost = peer.direction === "outbound" ? peer.alias : (peer.remoteIdentity?.hostName || peer.alias);
        return this.deliverLocalSessionMessage({
          id: String(p.id),
          fromNode: authNode,
          fromHost: authHost,
          fromSessionId: String(p.fromSessionId),
          fromProjectName: typeof p.fromProjectName === "string" ? p.fromProjectName : null,
          toSessionId: String(p.toSessionId),
          body: String(p.body ?? ""),
          replyToId: typeof p.replyToId === "string" ? p.replyToId : null,
          expectReply: p.expectReply === undefined ? true : Boolean(p.expectReply),
          hopCount: typeof p.hopCount === "number" ? p.hopCount : 1,
        });
      }
      case "session.list_messages": {
        const { host, sessionId } = parseSessionAddress(String(p.sessionId ?? p.target ?? ""));
        if (host && !this.isLocalHostTarget(host)) throw new Error("Cross-host forwarding is not permitted over a peer link");
        return this.db.sessionMessages.listForSession(sessionId, typeof p.limit === "number" ? p.limit : 50);
      }
      case "session.read": {
        const rawTarget = String(p.target ?? p.sessionId ?? "").trim();
        const { host, sessionId } = parseSessionAddress(rawTarget);
        if ((host && !this.isLocalHostTarget(host)) || sessionId.includes(":")) {
          throw new Error("Cross-host forwarding is not permitted over a peer link");
        }
        return this.readLocalSession(sessionId, typeof p.limit === "number" ? p.limit : 12);
      }
      case "peer.sync_outbox": {
        const flushed = await this.flushOutboxForPeer(peer);
        return { flushed };
      }
      default:
        throw new Error(`RPC "${op}" is not permitted over a peer link`);
    }
  }

  private async dispatch(socket: Socket, req: RpcRequest): Promise<unknown> {
    if (this.takenOverSockets.has(socket)) throw new Error("Session taken over by another terminal");
    const p = req.params ?? {};

    switch (req.op) {
      case "ping": {
        const live = this.getLiveWorkSummary();
        return {
          pong: true,
          pid: process.pid,
          nodeId: this.nodeId,
          hostName: this.peers.localHostName,
          uptimeMs: Date.now() - this.startedAt,
          protocolRev: DAEMON_PROTOCOL_REV,
          buildId: currentDaemonBuildId(),
          running: live.running,
          liveWork: live.description,
          remotes: this.listRemoteStatuses(),
        };
      }

      case "daemon.shutdown":
        setTimeout(() => {
          void this.stop().finally(() => process.exit(0));
        }, 25).unref();
        return { shuttingDown: true, pid: process.pid };

      case "status":
        return {
          pid: process.pid,
          nodeId: this.nodeId,
          hostName: this.peers.localHostName,
          uptimeMs: Date.now() - this.startedAt,
          socketPath: this.config.socketPath,
          configPath: join(this.config.stateDir, "config.json"),
          configWarnings: [...this.globalConfigWarnings, ...this.repoConfigWarnings],
          effectiveLimits: {
            maxConcurrentGoals: this.config.goals.maxConcurrentGoals ?? 4,
            maxConcurrentWorkers: this.config.worktree.maxConcurrentWorkers ?? 4,
            reviewerMaxTurns: this.config.rollover.reviewerMaxTurns,
            extraReadRoots: this.config.worktree.extraReadRoots ?? [],
            extraWriteRoots: this.config.worktree.extraWriteRoots ?? [],
          },
          effectiveExternalMcpServers: resolveExternalMcpServers(
            this.config.stateDir,
            this.orchestrator.config.externalMcpServers,
          ).map((s) => ({ name: s.name, enabled: s.enabled !== false })),
          hasLibghosttyVt: this.ptyHost.hasLibghosttyVt,
          runs: this.db.listRuns(20),
          goals: this.director.goals.listGoals(),
          profiles: this.config.profiles.map((pr) => ({ id: pr.id, kind: pr.kind, billing: pr.billing, enabled: pr.enabled !== false })),
          remotes: this.listRemoteStatuses(),
        };

      case "remote.list":
        return this.listRemoteStatuses();

      case "remote.add": {
        const name = String(p.name ?? "").trim();
        if (!name || !/^[A-Za-z0-9._-]+$/.test(name)) {
          throw new Error("Remote host name must be non-empty alphanumeric (letters, digits, '.', '_', '-')");
        }
        if (name.toLowerCase() === "local") {
          throw new Error('Remote host name cannot be "local"');
        }
        const rawTarget = String(p.sshTarget ?? p.target ?? "").trim();
        if (!rawTarget) throw new Error("sshTarget is required (e.g. 'ssh://user@gpu-box' or 'user@gpu-box')");
        const sshTarget =
          rawTarget.startsWith("ssh://") || rawTarget.startsWith("/") || rawTarget.startsWith("./")
            ? rawTarget
            : `ssh://${rawTarget}`;
        const bridgeCommand = typeof p.bridgeCommand === "string" && p.bridgeCommand.trim() ? p.bridgeCommand.trim() : undefined;
        if (sshTarget.startsWith("ssh://")) {
          parseValidatedSshTarget(sshTarget, bridgeCommand);
        }
        const reverseAccess = p.reverseAccess === "full" ? "full" : p.reverseAccess === "messages" ? "messages" : undefined;
        const entry: RemoteHostConfig = {
          name,
          sshTarget,
          ...(bridgeCommand ? { bridgeCommand } : {}),
          autoConnect: p.autoConnect === undefined ? true : Boolean(p.autoConnect),
          ...(reverseAccess ? { reverseAccess } : {}),
        };
        const existing = (this.config.remotes ?? []).filter((r) => r.name.toLowerCase() !== name.toLowerCase());
        const nextRemotes = [...existing, entry];
        this.config = { ...this.config, remotes: nextRemotes };
        this.orchestrator.updateConfig(this.config, this.catalog, this.quota);
        this.director.updateConfig(this.config, this.catalog, this.quota);
        this.persistConfig();
        this.peers.syncRemotes(nextRemotes, p.connectNow === false ? { skipConnectNames: new Set([name.toLowerCase()]) } : undefined);
        if (p.connectNow !== false) {
          try {
            return await this.peers.connectRemote(name);
          } catch {
            // Return status with lastError populated instead of failing remote.add when host is asleep/offline
          }
        }
        return this.listRemoteStatuses().find((r) => r.name.toLowerCase() === name.toLowerCase()) ?? entry;
      }

      case "remote.remove": {
        const name = String(p.name ?? "").trim();
        if (!name) throw new Error("Remote name is required");
        const before = this.config.remotes ?? [];
        const nextRemotes = before.filter((r) => r.name.toLowerCase() !== name.toLowerCase());
        if (nextRemotes.length === before.length) throw new Error(`Unknown remote host: ${name}`);
        const knownNodeId = this.peers.getKnownNodeId(name);
        const { remotes: _removed, ...rest } = this.config;
        this.config = nextRemotes.length > 0 ? { ...rest, remotes: nextRemotes } : rest;
        this.orchestrator.updateConfig(this.config, this.catalog, this.quota);
        this.director.updateConfig(this.config, this.catalog, this.quota);
        this.persistConfig();
        this.peers.syncRemotes(nextRemotes);
        const failedQueued = this.db.sessionMessages.failQueuedForHost(name, knownNodeId, `Remote host "${name}" was removed`);
        return { removed: name, failedQueued };
      }

      case "remote.connect": {
        const name = String(p.name ?? "").trim();
        if (!name) throw new Error("Remote name is required");
        return await this.peers.connectRemote(name);
      }

      case "remote.disconnect": {
        const name = String(p.name ?? "").trim();
        if (!name) throw new Error("Remote name is required");
        return this.peers.disconnectRemote(name);
      }

      case "peer.hello": {
        const identity: PeerIdentity = {
          nodeId: String(p.nodeId ?? ""),
          hostName: String(p.hostName ?? "remote"),
          protocolRev: Number(p.protocolRev ?? 0),
        };
        if (!identity.nodeId) throw new Error("peer.hello requires nodeId");
        this.peers.registerInboundSocket(socket, identity);
        return {
          nodeId: this.nodeId,
          hostName: this.peers.localHostName,
          protocolRev: DAEMON_PROTOCOL_REV,
        } satisfies PeerIdentity;
      }

      case "peer.proxy": {
        const host = String(p.host ?? "").trim();
        const op = String(p.op ?? "") as DaemonOp;
        const params = (p.params ?? {}) as Record<string, unknown>;
        if (!host) throw new Error("host is required");
        if (!op) throw new Error("op is required");
        if (!PEER_ALLOWED_OPS.has(op)) {
          throw new Error(`Op "${op}" cannot be proxied`);
        }
        return await this.peers.callRemote(host, op, params, typeof p.timeoutMs === "number" ? p.timeoutMs : 30_000);
      }

      case "peer.sync_outbox": {
        const inbound = this.peers.getInboundLinkForSocket(socket);
        if (inbound) {
          const flushed = await this.flushOutboxForPeer(inbound);
          return { flushed };
        }
        let total = 0;
        for (const link of this.peers.connectedLinks()) {
          total += await this.flushOutboxForPeer(link);
        }
        return { flushed: total };
      }

      case "config.get":
        return this.config;

      case "config.update": {
        if (p.patch === null || (p.patch !== undefined && !isPlainObject(p.patch))) {
          throw new Error("Invalid config patch: config must be a JSON object");
        }
        const patch = (p.patch ?? {}) as Record<string, unknown>;
        const unsupported = Object.keys(patch).filter((key) => !CONFIG_UPDATE_FIELDS.has(key));
        if (unsupported.length > 0) {
          throw new Error(`Invalid config patch: unsupported config.update field: ${unsupported.join(", ")}`);
        }
        const misplaced = findMisplacedConfigKeys(patch, "global");
        if (misplaced.length > 0) {
          throw new Error(
            `Invalid config patch: ${misplaced.map((m) => `"${m.path}" is not supported; supported location: ${m.supported}`).join("; ")}`,
          );
        }
        const patchErrors = validateConfigRaw(patch);
        if (patchErrors.length > 0) throw new Error(`Invalid config patch: ${patchErrors.join("; ")}`);
        const { roots } = normalizeWorktreeRoots(patch.worktree);
        const next: HarnessConfig = {
          ...this.config,
          ...(typeof patch.defaultOrchestratorProfile === "string"
            ? { defaultOrchestratorProfile: patch.defaultOrchestratorProfile }
            : {}),
          ...(typeof patch.defaultWorkerProfile === "string" ? { defaultWorkerProfile: patch.defaultWorkerProfile } : {}),
          ...(typeof patch.defaultTaskProfile === "string" ? { defaultTaskProfile: patch.defaultTaskProfile } : {}),
          ...(patch.codeMode === "collapsed" || patch.codeMode === "hybrid" || patch.codeMode === "off"
            ? { codeMode: patch.codeMode }
            : {}),
          ...(patch.rollover ? { rollover: { ...this.config.rollover, ...(patch.rollover as HarnessConfig["rollover"]) } } : {}),
          ...(patch.worktree ? { worktree: {
            ...this.config.worktree,
            ...(patch.worktree as HarnessConfig["worktree"]),
            ...(roots.extraReadRoots !== undefined ? { extraReadRoots: roots.extraReadRoots } : {}),
            ...(roots.extraWriteRoots !== undefined ? { extraWriteRoots: roots.extraWriteRoots } : {}),
          } } : {}),
          ...(patch.goals ? { goals: patchGoals(this.config.goals, patch.goals as Record<string, unknown>) } : {}),
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

      case "project.list":
        return await this.listFederatedProjects({
          includeRemotes: Boolean(p.includeRemotes),
          host: typeof p.host === "string" ? p.host : undefined,
        });

      case "project.get": {
        const host = typeof p.host === "string" ? p.host.trim() : undefined;
        if (host && !this.isLocalHostTarget(host)) {
          const remote = (await this.peers.callRemote<Record<string, unknown>>(host, "project.get", { ...p, host: undefined })) ?? {};
          const remoteNodeId =
            this.peers.findLink(host)?.remoteIdentity?.nodeId ?? this.peers.getKnownNodeId(host) ??
            (typeof remote.nodeId === "string" ? remote.nodeId : null);
          return { ...remote, host, nodeId: remoteNodeId ?? null };
        }
        const ref = String(p.projectId ?? p.id ?? p.name ?? p.path ?? "");
        const proj = this.db.projects.resolveProject(ref);
        if (!proj) throw new Error(`Unknown project: ${ref}`);
        const all = await this.db.projects.listProjects();
        return { ...(all.find((x) => x.id === proj.id) ?? proj), host: "local", nodeId: this.nodeId };
      }

      case "project.create": {
        const host = typeof p.host === "string" ? p.host.trim() : undefined;
        if (host && !this.isLocalHostTarget(host)) {
          const remote = (await this.peers.callRemote<Record<string, unknown>>(host, "project.create", { ...p, host: undefined })) ?? {};
          return { ...remote, host };
        }
        const created = await this.db.projects.createProject({
          path: String(p.path ?? ""),
          name: typeof p.name === "string" ? p.name : undefined,
          description: typeof p.description === "string" ? p.description : undefined,
          createDir: p.createDir === undefined ? true : Boolean(p.createDir),
          gitInit: Boolean(p.gitInit),
          cloneUrl: typeof p.cloneUrl === "string" && p.cloneUrl.trim() ? p.cloneUrl.trim() : undefined,
          defaultProfileId: typeof p.defaultProfileId === "string" ? p.defaultProfileId : undefined,
          defaultModel: typeof p.defaultModel === "string" ? p.defaultModel : undefined,
          defaultEffort: typeof p.defaultEffort === "string" ? p.defaultEffort : undefined,
        });
        const all = await this.db.projects.listProjects();
        return { ...(all.find((x) => x.id === created.id) ?? created), host: "local" };
      }

      case "project.update": {
        const host = typeof p.host === "string" ? p.host.trim() : undefined;
        if (host && !this.isLocalHostTarget(host)) {
          const remote = (await this.peers.callRemote<Record<string, unknown>>(host, "project.update", { ...p, host: undefined })) ?? {};
          return { ...remote, host };
        }
        const ref = String(p.projectId ?? p.id ?? "");
        const updated = this.db.projects.updateProject(ref, {
          name: typeof p.name === "string" ? p.name : undefined,
          description: p.description === null ? null : typeof p.description === "string" ? p.description : undefined,
          defaultProfileId: p.defaultProfileId === null ? null : typeof p.defaultProfileId === "string" ? p.defaultProfileId : undefined,
          defaultModel: p.defaultModel === null ? null : typeof p.defaultModel === "string" ? p.defaultModel : undefined,
          defaultEffort: p.defaultEffort === null ? null : typeof p.defaultEffort === "string" ? p.defaultEffort : undefined,
        });
        const all = await this.db.projects.listProjects();
        return { ...(all.find((x) => x.id === updated.id) ?? updated), host: "local" };
      }

      case "project.remove": {
        const host = typeof p.host === "string" ? p.host.trim() : undefined;
        if (host && !this.isLocalHostTarget(host)) {
          return await this.peers.callRemote(host, "project.remove", { ...p, host: undefined });
        }
        const ref = String(p.projectId ?? p.id ?? "");
        return this.db.projects.removeProject(ref);
      }

      case "project.browse": {
        const host = typeof p.host === "string" ? p.host.trim() : undefined;
        if (host && !this.isLocalHostTarget(host)) {
          return await this.peers.callRemote(host, "project.browse", { path: typeof p.path === "string" ? p.path : undefined });
        }
        return this.db.projects.browseDirectory(typeof p.path === "string" ? p.path : undefined);
      }

      case "chat.set_project": {
        const chatSessionId = String(p.chatSessionId);
        const projectRef = String(p.projectId ?? p.project ?? "");
        const { session, project } = this.director.switchChatProject(chatSessionId, projectRef);
        return { session, project };
      }

      case "chat.open": {
        const requestedProjectRef = typeof p.projectId === "string" && p.projectId.trim() ? p.projectId.trim() : undefined;
        const explicitProject = requestedProjectRef ? this.db.projects.resolveProject(requestedProjectRef) : null;
        if (requestedProjectRef && !explicitProject) {
          throw new Error(`Unknown project "${requestedProjectRef}". Add it first with \`pi-harness project add <path>\` or /projects.`);
        }

        let session =
          typeof p.chatSessionId === "string" && p.chatSessionId
            ? this.director.chats.getSession(p.chatSessionId)
            : explicitProject
              ? this.director.chats.getProjectSession(explicitProject.id) ??
                this.director.chats.getWorkspaceSession(explicitProject.rootPath)
              : p.preferLastGlobal
                ? this.director.chats.getMostRecentSession()
                : null;

        if (typeof p.chatSessionId === "string" && p.chatSessionId && !session) {
          throw new Error(`Unknown chat session: ${p.chatSessionId}`);
        }

        let targetProject = explicitProject;
        let projectRegistered = false;
        if (!session) {
          if (!targetProject && p.preferLastProject) {
            targetProject = this.db.projects.getMostRecent();
          }
          const sel = chatSelection({
            ...p,
            repoRoot: targetProject?.rootPath ?? (typeof p.repoRoot === "string" && p.repoRoot ? p.repoRoot : process.cwd()),
          });
          const opened = await this.director.openDefaultChatDetailed({
            ...sel,
            projectId: targetProject?.id ?? undefined,
          });
          session = opened.session;
          projectRegistered = opened.projectRegistered;
        }
        const ensured = this.ensureChatProject(session);
        const boundProject = ensured.project;
        if (ensured.registered) projectRegistered = true;
        if (boundProject && session.projectId !== boundProject.id) {
          session = this.director.chats.updateSession(session.id, { projectId: boundProject.id });
        }
        if (boundProject) this.db.projects.touchProject(boundProject.id);
        session = await this.director.resolveOrMigrateSessionModel(session);

        const { tookOver } = this.bindChatSocket(socket, session.id);
        const turn = this.chatTurns.get(session.id);
        const { historyThrough } = this.db.db.prepare(
          "SELECT COALESCE(MAX(seq), 0) AS historyThrough FROM chat_turns WHERE chat_session_id = ?",
        ).get(session.id) as { historyThrough: number };
        return {
          session,
          project: boundProject,
          ...(projectRegistered ? { projectRegistered: true } : {}),
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
        const requestedProjectRef = typeof p.projectId === "string" && p.projectId.trim() ? p.projectId.trim() : undefined;
        const explicitProject = requestedProjectRef ? this.db.projects.resolveProject(requestedProjectRef) : null;
        if (requestedProjectRef && !explicitProject) {
          throw new Error(`Unknown project "${requestedProjectRef}"`);
        }
        const created = await this.director.createChatDetailed({
          ...chatSelection({
            ...p,
            repoRoot: explicitProject?.rootPath ?? (typeof p.repoRoot === "string" && p.repoRoot ? p.repoRoot : process.cwd()),
          }),
          projectId: explicitProject?.id ?? undefined,
          title: typeof p.title === "string" ? p.title : undefined,
        });
        const { session } = created;
        const { project: boundProject, registered } = this.ensureChatProject(session);
        if (!p.headless) {
          this.bindChatSocket(socket, session.id);
        }
        const { historyThrough } = this.db.db.prepare(
          "SELECT COALESCE(MAX(seq), 0) AS historyThrough FROM chat_turns WHERE chat_session_id = ?",
        ).get(session.id) as { historyThrough: number };
        return {
          session,
          project: boundProject,
          ...(created.projectRegistered || registered ? { projectRegistered: true } : {}),
          ...this.chatHistoryPage(session.id, 0, historyThrough),
          activeTurnId: null,
          activeText: "",
          activePartial: "",
          goals: this.director.goals.listGoals({ chatSessionId: session.id }),
          config: this.config,
          tookOver: false,
          busy: false,
        };
      }

      case "sessions.list":
        return await this.listFederatedSessions(socket, {
          includeRemotes: Boolean(p.includeRemotes),
          host: typeof p.host === "string" ? p.host : undefined,
        });

      case "session.send_message": {
        const fromSessionId = String(p.fromSessionId ?? this.socketToChatId.get(socket) ?? "").trim();
        if (!fromSessionId) throw new Error("fromSessionId is required");
        const target = String(p.target ?? "").trim();
        const body = String(p.body ?? p.message ?? "").trim();
        if (!target) throw new Error("target is required");
        if (!body) throw new Error("body is required");
        return await this.sendSessionMessage({
          fromSessionId,
          target,
          body,
          expectReply: p.expectReply === undefined ? true : Boolean(p.expectReply),
          replyToId: typeof p.replyToId === "string" ? p.replyToId : undefined,
        });
      }

      case "session.deliver_message": {
        const inboundPeer = this.peers.getInboundLinkForSocket(socket);
        return this.deliverLocalSessionMessage({
          id: String(p.id),
          fromNode: String(p.fromNode ?? inboundPeer?.remoteIdentity?.nodeId ?? "unknown"),
          fromHost: String(p.fromHost ?? inboundPeer?.alias ?? "remote"),
          fromSessionId: String(p.fromSessionId),
          fromProjectName: typeof p.fromProjectName === "string" ? p.fromProjectName : null,
          toSessionId: String(p.toSessionId),
          body: String(p.body ?? ""),
          replyToId: typeof p.replyToId === "string" ? p.replyToId : null,
          expectReply: p.expectReply === undefined ? true : Boolean(p.expectReply),
          hopCount: typeof p.hopCount === "number" ? p.hopCount : 1,
        });
      }

      case "session.list_messages": {
        const target = String(p.target ?? p.sessionId ?? this.socketToChatId.get(socket) ?? "").trim();
        if (!target) throw new Error("sessionId or target is required");
        const { host, sessionId } = parseSessionAddress(target);
        const limit = typeof p.limit === "number" ? p.limit : 50;
        if (host && !this.isLocalHostTarget(host)) {
          return await this.peers.callRemote(host, "session.list_messages", { sessionId, limit });
        }
        return this.db.sessionMessages.listForSession(sessionId, limit);
      }

      case "session.read": {
        const target = String(p.target ?? p.sessionId ?? "").trim();
        if (!target) throw new Error("target is required");
        return await this.readSessionOnHost({
          target,
          limit: typeof p.limit === "number" ? p.limit : undefined,
        });
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

      case "goal.commit": {
        // One shared start-or-resume contract (it also clears the previous epoch's claims on an in-place resume);
        // the Director's detailed result, read after that clear, is returned as is.
        const res = await this.director.commitGoalDetailed(String(p.goalId), {
          profileId: typeof p.profileId === "string" ? p.profileId : undefined,
          model: typeof p.model === "string" ? p.model : undefined,
          effort: typeof p.effort === "string" ? p.effort : undefined,
        });
        return {
          ...res.goal,
          outcome: res.outcome,
          message: res.message,
          ...(res.runStatus ? { runStatus: res.runStatus } : {}),
          ...(res.resumedTaskIds ? { resumedTaskIds: res.resumedTaskIds } : {}),
          ...(res.queuePosition !== undefined ? { queuePosition: res.queuePosition } : {}),
        };
      }

      case "goal.amend":
        return await this.director.amendGoal(String(p.goalId), {
          rationale: String(p.rationale ?? "Amended by user"),
          title: typeof p.title === "string" ? p.title : undefined,
          description: typeof p.description === "string" ? p.description : undefined,
          appendDescription: typeof p.appendDescription === "string" ? p.appendDescription : undefined,
          confirmReplaceDescription: p.confirmReplaceDescription === true ? true : undefined,
          addCriteria: Array.isArray(p.addCriteria) ? (p.addCriteria as GoalCriterion[]) : undefined,
          updateCriteria: parseGoalCriterionPatches(p.updateCriteria),
          removeCriteriaIds: Array.isArray(p.removeCriteriaIds) ? (p.removeCriteriaIds as string[]) : undefined,
          nonGoals: Array.isArray(p.nonGoals) ? (p.nonGoals as string[]) : undefined,
        }, { source: "user" });

      case "goal.list":
        return this.director.goals
          .listGoals({
            repoRoot: typeof p.repoRoot === "string" ? p.repoRoot : undefined,
            chatSessionId: typeof p.chatSessionId === "string" && p.chatSessionId ? p.chatSessionId : undefined,
          })
          .map(reconcileGoalWithLiveRun);

      case "goal.get": {
        const stored = this.director.goals.getGoal(String(p.goalId));
        if (!stored) throw new Error(`Unknown goal ${p.goalId}`);
        const g = reconcileGoalWithLiveRun(stored);
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
        const acceptOverrides = reviews.filter((r) => r.source === "director_accept_override");
        return { goal: g, reviews, acceptOverrides, tasks, escalations, activity, display };
      }

      case "goal.table_display": {
        const goals = this.director.goals
          .listGoals({
            repoRoot: typeof p.repoRoot === "string" ? p.repoRoot : undefined,
            chatSessionId: typeof p.chatSessionId === "string" && p.chatSessionId ? p.chatSessionId : undefined,
          })
          .map(reconcileGoalWithLiveRun);
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

      case "goal.revisions":
        return this.director.goals.listRevisions(String(p.goalId));

      case "goal.resume": {
        if (!this.director.goals.getGoal(String(p.goalId))) throw new Error(`Unknown goal ${p.goalId}`);
        // Delegated once to the shared contract; never re-derives or overwrites a newer pause after the await.
        const res = await this.director.resumeGoalDetailed(String(p.goalId), {
          note: typeof p.note === "string" ? p.note : undefined,
        });
        const resumed = res.outcome === "resumed" || res.outcome === "started";
        return {
          resumed,
          goal: res.goal,
          resumedTaskIds: res.resumedTaskIds ?? [],
          outcome: res.outcome,
          ...(res.runStatus ? { runStatus: res.runStatus } : {}),
          ...(res.queuePosition !== undefined ? { queuePosition: res.queuePosition } : {}),
          message: res.message,
          ...(resumed ? {} : { reason: res.message }),
        };
      }

      case "goal.accept": {
        const evidencePaths = Array.isArray(p.evidencePaths) ? p.evidencePaths.map(String) : [];
        return await this.director.acceptGoalOverride(String(p.goalId), {
          reason: String(p.reason ?? ""),
          evidencePaths,
          overrideDeterministic: p.overrideDeterministic === true,
          source: "user",
        });
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

      case "goal.pause":
        // Also cancels earlier resume_goal timers and wins over a launch or resume already in flight.
        return await this.director.pauseGoal(String(p.goalId), "Paused by user");

      case "goal.cancel":
        return await this.director.pauseGoal(String(p.goalId), "Cancelled by user", "cancelled");

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

      case "run.stop": {
        const runId = String(p.runId);
        const reason = typeof p.reason === "string" ? p.reason : "Stopped by user";
        let cancelledChat = false;
        if (isDirectorTaskContainer(runId)) {
          // Stop the chat's live turn first so it cannot delegate again, then cancel the container's children.
          cancelledChat = await this.director.cancelChat(directorTaskRunOwner(runId) ?? "");
        }
        await this.orchestrator.stopRun(runId, reason);
        return { stopped: true, ...(cancelledChat ? { cancelledChat } : {}) };
      }

      case "runs.stop_all": {
        const reason = typeof p.reason === "string" && p.reason.trim() ? p.reason.trim() : "Stopped by user (/stop)";
        const repoFilter = typeof p.repoRoot === "string" && p.repoRoot.trim() ? canonicalProjectPath(p.repoRoot.trim()) : undefined;
        const inScope = (repoRoot: string | null | undefined) =>
          !repoFilter || (repoRoot ? canonicalProjectPath(repoRoot) === repoFilter : false);
        // An explicit stop wins over every older in-flight resume or launch, in this repo only when filtered.
        if (repoFilter) this.orchestrator.bumpRepoStopEpoch(repoFilter);
        else this.orchestrator.bumpGlobalStopEpoch();
        // Abort live Director turns, reviews and goal-resume timers FIRST (scoped to the repo) so none can delegate
        // or resume after the stop; the Director counts what it actually cancelled.
        const { cancelledChats, cancelledReviews, cancelledTimers } = await this.director.cancelAllActive(
          reason,
          repoFilter ? { repoRoot: repoFilter } : undefined,
        );
        // Pending wakeups of chats outside the filter stay queued.
        for (const chatId of [...this.pendingWakeups.keys()]) {
          if (inScope(this.director.chatRepoRoot(chatId))) this.pendingWakeups.delete(chatId);
        }
        // Queued goals leave the queue, so stopping a linked run below cannot drain it and start new work.
        let dequeuedGoals = 0;
        for (const entry of this.db.admissionQueue.list({ lane: "goal" })) {
          if (inScope(entry.repoRoot) && this.db.admissionQueue.remove("goal", entry.targetId)) dequeuedGoals++;
        }
        if (dequeuedGoals > 0) this.director.syncQueuedGoalProgress();
        // Every active or quota-paused run (no row cap), plus runs whose tasks are live in memory.
        const candidates = new Map<string, RunRecord>();
        for (const r of this.db.listActiveOrQuotaPausedRuns()) candidates.set(r.id, r);
        for (const id of this.orchestrator.liveRunIds()) {
          const r = this.db.getRun(id);
          if (r) candidates.set(r.id, r);
        }
        const runs = [...candidates.values()].filter((r) => {
          if (!inScope(r.repoRoot)) return false;
          const eff = this.orchestrator.getEffectiveRun(r.id);
          return eff?.status === "active" || eff?.status === "paused_quota";
        });
        const stoppedRunIds: string[] = [];
        for (const r of runs) {
          // Director delegation containers only have their children cancelled; they are never poisoned to "stopped".
          await this.orchestrator.stopRun(r.id, reason);
          stoppedRunIds.push(r.id);
        }
        return {
          stopped: true,
          stoppedRunIds,
          cancelledChats,
          cancelledReviews,
          cancelledTimers,
          dequeuedGoals,
        };
      }

      case "run.resume": {
        const runId = String(p.runId);
        this.director.assertRunGoalNotTerminal(runId);
        const priorRunStatus = this.orchestrator.getEffectiveRun(runId)?.status;
        const res = await this.orchestrator.resumeRun(runId, {
          trigger: "manual",
          userMessage: typeof p.message === "string" && p.message.trim() ? p.message.trim() : undefined,
          note: typeof p.note === "string" && p.note.trim() ? p.note.trim() : undefined,
        });
        this.director.onRunResumed(runId, priorRunStatus, res.outcome === "resumed");
        return res;
      }

      case "task.resume": {
        const taskBefore = this.db.getTask(String(p.taskId));
        if (taskBefore) this.director.assertRunGoalNotTerminal(taskBefore.runId);
        const priorRunStatus = taskBefore ? this.orchestrator.getEffectiveRun(taskBefore.runId)?.status : undefined;
        const res = await this.orchestrator.resumeTaskDetailed(String(p.taskId), {
          note: typeof p.note === "string" ? p.note : undefined,
          model: typeof p.model === "string" ? p.model : undefined,
          effort: typeof p.effort === "string" ? p.effort : undefined,
          profileId: typeof p.profileId === "string" ? p.profileId : undefined,
        });
        this.director.onRunResumed(res.task.runId, priorRunStatus, res.outcome === "resumed");
        return {
          resumed: res.outcome === "resumed",
          task: res.task,
          outcome: res.outcome,
          message: res.message,
          runStatus: this.orchestrator.getEffectiveRun(res.task.runId)?.status ?? null,
          ...(res.queuePosition !== undefined ? { queuePosition: res.queuePosition } : {}),
          ...(res.noteDelivered !== undefined ? { noteDelivered: res.noteDelivered } : {}),
          ...(res.noteDelivery ? { noteDelivery: res.noteDelivery } : {}),
        };
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
        const target =
          watch && (watch.scope === "task" || watch.scope === "run")
            ? watch
            : attachment && attachment.id === p.attachmentId
              ? attachment
              : undefined;
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
        if (watch && (watch.scope === "task" || watch.scope === "run")) watch.lastSeq = target.throughSeq;
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

      case "work.snapshot": {
        const chatSessionId = typeof p.chatSessionId === "string" && p.chatSessionId ? p.chatSessionId : undefined;
        const repoRoot = typeof p.repoRoot === "string" && p.repoRoot ? p.repoRoot : undefined;
        const filterRunId = typeof p.runId === "string" && p.runId ? p.runId : undefined;
        const goals = this.director.goals.listGoals({ repoRoot, chatSessionId }).map(reconcileGoalWithLiveRun);
        const goalByRunId = new Map(goals.filter((g) => g.linkedRunId).map((g) => [g.linkedRunId!, g]));
        const runIds = filterRunId
          ? [filterRunId]
          : chatSessionId
            ? this.getRunIdsForChatSession(chatSessionId)
            : this.db
                .listRuns(500)
                .filter((r) => !repoRoot || r.repoRoot === repoRoot)
                .slice(0, 50)
                .map((r) => r.id);
        const runIdSet = new Set(runIds);
        const runEntries = runIds
          .map((id) => this.db.getRun(id))
          .filter((r): r is NonNullable<typeof r> => r !== null)
          .sort((a, b) => b.createdAt - a.createdAt)
          .map((raw) => {
            // Read-only projection: derive the status in memory; never persist it from a snapshot.
            const tasks = this.db.listTasks(raw.id);
            const status = deriveRunStatus(raw, tasks, this.orchestrator.isRunActiveInMemory(raw.id));
            const run = status === raw.status ? raw : { ...raw, status };
            const escalations = this.db.listEscalations(run.id);
            return {
              run,
              goalId: goalByRunId.get(run.id)?.id ?? null,
              tasks,
              escalations,
              quotaPause: this.orchestrator.getRunQuotaStatus(run.id),
              activity: buildRunActivitySummary(this.db, run.id),
            };
          });
        // Always include all open/acked escalations without recency truncation, then append recent resolved ones
        const openEscs = this.db.listAllEscalations({ runIds, status: "open", limit: 500 });
        const ackedEscs = this.db.listAllEscalations({ runIds, status: "acked", limit: 500 });
        const recentEscs = this.db.listAllEscalations({ runIds, limit: 150 });
        const escMap = new Map(openEscs.map((e) => [e.id, e]));
        for (const e of ackedEscs) escMap.set(e.id, e);
        for (const e of recentEscs) if (!escMap.has(e.id)) escMap.set(e.id, e);
        const escalations = [...escMap.values()].sort((a, b) => b.createdAt - a.createdAt);

        const repoChatIds = repoRoot
          ? new Set(this.director.chats.listSessions(200).filter((c) => c.repoRoot === repoRoot).map((c) => c.id))
          : null;
        const timers = this.db.timers
          .list({})
          .filter((t) => {
            if (filterRunId) return t.runId === filterRunId;
            if (chatSessionId) {
              if (t.ownerKind === "chat") return t.ownerId === chatSessionId;
              return t.runId != null && runIdSet.has(t.runId);
            }
            if (repoChatIds) {
              if (t.ownerKind === "chat") return repoChatIds.has(t.ownerId);
              return t.runId != null && runIdSet.has(t.runId);
            }
            return true;
          })
          .slice(0, 100);
        const merges = this.db.listMergeQueue({ runIds, limit: 100 });
        const rawLiveWorktrees = repoRoot
          ? await this.orchestrator.getWorktreeManager(repoRoot).listLive(filterRunId).catch(() => [])
          : [];
        const worktrees: SnapshotWorktreeEntry[] = rawLiveWorktrees
          .filter((wt) => {
            if (wt.orphaned) return true;
            if (!chatSessionId && !filterRunId) return true;
            return wt.record ? runIdSet.has(wt.record.runId) : true;
          })
          .map((wt) => ({
            id: wt.record?.id ?? null,
            runId: wt.record?.runId ?? null,
            taskId: wt.record?.taskId ?? null,
            status: wt.record?.status ?? (wt.orphaned ? "orphaned" : "active"),
            branch: wt.branch ?? "(detached)",
            path: wt.path,
            existsOnDisk: wt.existsOnDisk,
            dirty: wt.dirty,
            orphaned: wt.orphaned,
            archiveRef: wt.record?.archiveRef ?? null,
          }));
        const messages = runIds.flatMap((id) => this.db.listMessagesForRun(id, 50));

        // Build severity-sorted Inbox of items needing human attention
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
          if (e.status !== "open") continue;
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
        const sevRank = { P0: 0, P1: 1, P2: 2, info: 3 };
        // At equal severity an escalation (the concrete finding) precedes the derived items it causes, such as its
        // goal awaiting a human after the run settled.
        const kindRank = (item: InboxItem) => (item.kind === "escalation" ? 0 : 1);
        inbox.sort(
          (a, b) => sevRank[a.severity] - sevRank[b.severity] || kindRank(a) - kindRank(b) || b.createdAt - a.createdAt,
        );

        return {
          generatedAt: Date.now(),
          latestSeq: this.db.getLatestEventSeq(),
          goals: goals.map((goal) => ({
            goal,
            reviews: this.director.goals.listReviews(goal.id),
            revisions: this.director.goals.listRevisions(goal.id),
          })),
          runs: runEntries,
          escalations,
          timers,
          merges,
          worktrees,
          messages,
          inbox,
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

      case "task.get": {
        const task = this.db.getTask(String(p.taskId));
        if (!task) throw new Error(`Unknown task ${p.taskId}`);
        const run = this.db.getRun(task.runId);
        const escalations = this.db.listEscalations(task.runId).filter((e) => e.fromTaskId === task.id || e.toTaskId === task.id);
        const artifacts = this.db.listArtifacts(task.runId, task.id, 100);
        const worktree = task.worktreeId ? this.db.getWorktree(task.worktreeId) : null;
        const merges = this.db.listMergeQueue({ runId: task.runId, limit: 50 }).filter((m) => m.taskId === task.id);
        const messages = this.db.listMessagesForRun(task.runId, 50).filter((m) => m.fromTaskId === task.id || m.toTaskId === task.id);
        const children = this.db.listTasks(task.runId, { parentId: task.id });
        const writeScopes = this.db.listAssignedWriteScopes(task.runId, task.id);
        return {
          task,
          handoff: compactTaskHandoff(task, escalations),
          run: run ? (this.orchestrator.getEffectiveRun(run.id) ?? run) : null,
          escalations,
          artifacts,
          worktree,
          merges,
          messages,
          children,
          writeScopes,
        };
      }

      case "task.steer":
        return await this.orchestrator.steerTaskDetailed(String(p.taskId), String(p.message));

      case "task.cancel":
        await this.orchestrator.cancelTask(String(p.taskId), String(p.reason ?? "Cancelled by user"));
        return { cancelled: true };

      case "escalation.list": {
        const chatSessionId = typeof p.chatSessionId === "string" && p.chatSessionId ? p.chatSessionId : undefined;
        const runId = typeof p.runId === "string" && p.runId ? p.runId : undefined;
        const status =
          p.status === "open" || p.status === "acked" || p.status === "resolved" || p.status === "unresolved"
            ? p.status
            : undefined;
        const limit =
          typeof p.limit === "number" && Number.isFinite(p.limit) ? Math.max(1, Math.min(1000, Math.floor(p.limit))) : 100;
        const runIds = runId ? [runId] : chatSessionId ? this.getRunIdsForChatSession(chatSessionId) : undefined;
        return this.db.listAllEscalations({ runIds, status, limit });
      }

      case "escalation.get": {
        const esc = this.db.getEscalation(String(p.escalationId));
        if (!esc) throw new Error(`Unknown escalation ${p.escalationId}`);
        const run = this.db.getRun(esc.runId);
        const fromTask = this.db.getTask(esc.fromTaskId);
        const goal = this.director.goals.listGoals().find((g) => g.linkedRunId === esc.runId) ?? null;
        return { escalation: esc, run, fromTask, goal };
      }

      case "escalation.ack": {
        const resolveFlag = p.resolve === undefined ? true : Boolean(p.resolve);
        const resolution = String(p.resolution ?? (resolveFlag ? "Resolved" : "Acknowledged"));
        return this.db.ackEscalation(String(p.escalationId), resolution, resolveFlag);
      }

      case "escalation.resolve": {
        const escalationId = String(p.escalationId ?? "");
        const rawReason = p.reason !== undefined ? p.reason : p.resolution;
        if (typeof rawReason !== "string" || rawReason.trim().length === 0) {
          throw new Error("reason must be a non-empty string");
        }
        let chatSessionId: string | undefined;
        let repoRoot: string | undefined;
        if (p.chatSessionId !== undefined) {
          // A provided session must be real, so a blank or unknown ID never bypasses workspace scoping.
          if (typeof p.chatSessionId !== "string" || p.chatSessionId.trim().length === 0) {
            throw new Error("chatSessionId must be a non-empty string when provided");
          }
          chatSessionId = p.chatSessionId;
          const sess = this.director.chats.getSession(chatSessionId);
          if (!sess) throw new Error(`Unknown chat session ${chatSessionId}`);
          repoRoot = sess.repoRoot;
        }
        return this.director.resolveEscalation({
          escalationId,
          reason: rawReason,
          chatSessionId,
          repoRoot,
          resolvedBy: "operator",
        });
      }

      case "escalation.reconcile":
        return { mode: "daemon", ...this.db.reconcileSettledGoalEscalations() };

      case "artifact.list": {
        const runId = String(p.runId);
        const taskId = typeof p.taskId === "string" && p.taskId ? p.taskId : undefined;
        return this.db.listArtifacts(runId, taskId, typeof p.limit === "number" ? p.limit : 100);
      }

      case "artifact.read": {
        const run = this.db.getRun(String(p.runId));
        if (!run) throw new Error(`Unknown run ${p.runId}`);
        const reqPath = typeof p.path === "string" && p.path ? p.path : undefined;
        const maxChars = typeof p.maxChars === "number" ? p.maxChars : 16_000;
        const offset = typeof p.offset === "number" ? p.offset : 0;
        try {
          return readRunDossier(this.db, run, {
            path: reqPath,
            task_id: typeof p.taskId === "string" ? p.taskId : undefined,
            max_chars: maxChars,
            offset,
          });
        } catch (err) {
          if (!reqPath) throw err;
          // Allow reading any indexed non-dossier artifact belonging to this run safely within its workspace
          const indexed = this.db.listArtifacts(run.id, typeof p.taskId === "string" ? p.taskId : undefined, 200);
          const match = indexed.find((a) => a.path === reqPath);
          if (!match) throw err;
          const task = this.db.getTask(match.taskId);
          const wt = task?.worktreeId ? this.db.getWorktree(task.worktreeId) : null;
          const root = wt && existsSync(wt.path) ? wt.path : run.repoRoot;
          return readWorkspaceArtifact(root, reqPath, maxChars, offset);
        }
      }

      case "timer.list": {
        const status =
          p.status === "pending" || p.status === "fired" || p.status === "cancelled" ? p.status : undefined;
        const ownerKind = p.ownerKind === "chat" || p.ownerKind === "task" ? p.ownerKind : undefined;
        const ownerId = typeof p.ownerId === "string" && p.ownerId ? p.ownerId : undefined;
        const limit = typeof p.limit === "number" ? p.limit : 100;
        return this.db.timers.list({ ownerKind, ownerId, status }).slice(0, limit);
      }

      case "timer.cancel": {
        const cancelled = this.db.timers.cancel(
          String(p.timerId),
          typeof p.reason === "string" ? p.reason : "cancelled_by_user",
        );
        if (!cancelled) throw new Error(`Unknown timer ${p.timerId}`);
        return { timer: cancelled };
      }

      case "merge.list": {
        const runId = typeof p.runId === "string" && p.runId ? p.runId : undefined;
        const chatSessionId = typeof p.chatSessionId === "string" && p.chatSessionId ? p.chatSessionId : undefined;
        const runIds = !runId && chatSessionId ? this.getRunIdsForChatSession(chatSessionId) : undefined;
        return this.db.listMergeQueue({ runId, runIds, limit: typeof p.limit === "number" ? p.limit : 100 });
      }

      case "messages.list": {
        const runId = String(p.runId);
        return this.db.listMessagesForRun(runId, typeof p.limit === "number" ? p.limit : 50);
      }

      case "event.get": {
        const seq = Number(p.seq);
        if (!Number.isFinite(seq) || seq <= 0) throw new Error("event.get requires a positive seq number");
        const ev = this.db.getEventBySeq(seq);
        if (!ev) throw new Error(`Unknown event seq ${seq}`);
        return ev;
      }

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
          typeof p.chatSessionId === "string" && p.chatSessionId
            ? p.chatSessionId
            : runId
              ? this.getChatSessionIdForRun(runId)
              : (this.socketToChatId.get(socket) ?? null);
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

      case "worktrees.get": {
        const wt = this.db.getWorktree(String(p.worktreeId));
        if (!wt) throw new Error(`Unknown worktree ${p.worktreeId}`);
        const run = this.db.getRun(wt.runId);
        const task = this.db.getTask(wt.taskId);
        let statusOut = "";
        let diffStat = "";
        if (existsSync(wt.path)) {
          statusOut = await runGit(wt.path, ["status", "--short"]).then((r) => r.stdout).catch(() => "");
          diffStat = await runGit(wt.path, ["diff", "--stat", wt.baseSha]).then((r) => r.stdout).catch(() => "");
        }
        return { worktree: wt, run, task, existsOnDisk: existsSync(wt.path), gitStatus: statusOut, diffStat };
      }

      case "worktrees.clean": {
        const repoRoot = String(p.repoRoot ?? process.cwd());
        const mgr = this.orchestrator.getWorktreeManager(repoRoot);
        if (typeof p.worktreeId === "string" && p.worktreeId) {
          const wt = this.db.getWorktree(p.worktreeId);
          if (!wt) throw new Error(`Unknown worktree ${p.worktreeId}`);
          const run = this.db.getRun(wt.runId);
          if (!run) throw new Error(`Worktree ${p.worktreeId} has no owning run`);
          if (canonicalPath(run.repoRoot) !== canonicalPath(repoRoot)) {
            throw new Error(`Worktree ${p.worktreeId} belongs to ${run.repoRoot}, not ${repoRoot}`);
          }
          const task = this.db.getTask(wt.taskId);
          const settled = Boolean(task && ["completed", "failed", "cancelled"].includes(task.status));
          if (!settled && !Boolean(p.force)) {
            throw new Error(
              task
                ? `Cannot remove worktree ${p.worktreeId}: task ${task.id} is still ${task.status}`
                : `Cannot remove worktree ${p.worktreeId}: owning task ${wt.taskId} record is missing`,
            );
          }
          const wasCleaned = wt.cleanedAt != null;
          if (!settled && !wasCleaned && !existsSync(wt.path)) {
            throw new Error(`Cannot remove offline active worktree ${p.worktreeId}: checkout directory is missing; left in place`);
          }
          const ref = await mgr.teardown(p.worktreeId, {
            archiveBeforeRemove: true,
            requireArchive: true,
            finalStatus: "archived",
          });
          return {
            removed: wasCleaned ? [] : [p.worktreeId],
            archivedRefs: ref ? [ref] : [],
            expiredArchiveRefs: [],
            skipped: [],
            errors: [],
          };
        }
        return await mgr.sweep({
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
        // An orchestrator task's bridge sends `<taskId>@g<generation>` (see AcpRunner); callers may also pass expectedGeneration.
        const token = /^(.+)@g(\d+)$/.exec(String(p.taskId));
        const taskId = token ? token[1]! : String(p.taskId);
        const expectedGeneration = p.expectedGeneration ?? (token ? Number(token[2]) : undefined);
        const name = String(p.name);
        const args = (p.args ?? {}) as Record<string, unknown>;
        if (taskId.startsWith("chat-")) {
          return await this.director.executeDirectorMcpTool(taskId, name, args);
        }
        if (this.director.hasActiveReviewer(taskId)) {
          return await this.director.executeReviewerMcpTool(taskId, name, args);
        }
        // A bridge started for an older generation (evicted, cancelled, rolled over or resumed runner) is refused.
        if (expectedGeneration !== undefined && expectedGeneration !== null) {
          this.orchestrator.assertCurrentToolGeneration(taskId, Number(expectedGeneration));
        }
        return await this.orchestrator.toolExecutor.execute(String(p.runId), taskId, name, args);
      }

      default:
        throw new Error(`Unknown daemon op: ${req.op}`);
    }
  }
}

/** Goal lifecycle events the Director itself caused; they never wake (or re-wake) the Director. */
function isDirectorSourcedGoalEvent(event: ChatControlEvent): boolean {
  return (event.kind === "goal.settled" || event.kind === "goal.reviewed") && event.source === "director";
}
