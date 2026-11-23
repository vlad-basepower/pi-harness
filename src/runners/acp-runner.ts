import { toolEventPreview } from "../protocol/tool-display.js";
import { randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { constants } from "node:fs";
import { lstat, mkdir, open, realpath, stat } from "node:fs/promises";
import { homedir } from "node:os";
import { basename, dirname, isAbsolute, relative, resolve, sep } from "node:path";
import { Readable, Writable } from "node:stream";
import { client, ndJsonStream, PROTOCOL_VERSION, type AgentApp, type ClientConnection } from "@agentclientprotocol/sdk/unstable";
import { interpolateEnv, type AcpProfileConfig } from "../config.js";
import {
  buildNewSessionParams,
  buildPostSessionActions,
  extractArtifactsFromToolUpdate,
  isAmpAcpProfile,
  isReadOnlyExternalMcpTool,
  isSafeReadOnlyShellCommand,
  prepareAcpSpawn,
  reportedModels,
  reportedModelSatisfies,
  selectValues,
  type AcpModelState,
  type AcpConfigOption,
  type AcpMcpServerEntry,
} from "./acp-shims.js";
import { isProtectedWritePath } from "./protected-paths.js";
import { detectQuotaErrorText } from "./quota-detect.js";
import { MAX_RUNNER_STEER_CONTINUATIONS, turnReminderText, validateRunPolicy } from "./types.js";
import type { ProbedModelInfo, QuotaPauseInfo, RunAgentRequest, RunAgentResult, RunnerCost, RunnerUsage, ToolPolicy } from "./types.js";

/** Returns an accurate, human-readable description of the active tool policy and permissions. */
export function describeToolPolicyMode(toolPolicy: ToolPolicy, allowShellInReadOnly = false): string {
  if (toolPolicy === "control_plane_only") {
    return "control-plane-only (harness MCP tools only; no workspace reads, writes, or shell)";
  }
  if (toolPolicy === "read_only_workspace") {
    return allowShellInReadOnly
      ? "read-only workspace with verification shell (no file writes)"
      : "read-only workspace (no file writes or shell)";
  }
  return "read-write workspace";
}

const CODEX_CODE_MODE_UNAVAILABLE_RE = /code\s+mode\s+is\s+unavailable|codex-code-mode-host/i;

/** Detects Codex 0.170+ fail-closed MCP breakage caused by a missing `codex-code-mode-host` binary. */
export function detectCodexCodeModeMisconfig(text: string): string | null {
  if (!CODEX_CODE_MODE_UNAVAILABLE_RE.test(text)) return null;
  return (
    "Profile misconfigured: Codex Code Mode is unavailable (`codex-code-mode-host` is missing or failed closed), " +
    "blocking the harness MCP tool bridge. Install `codex-code-mode-host` alongside `codex-acp` on PATH, " +
    "or disable it with `features.code_mode_host=false` (CLI `-c features.code_mode_host=false` or `CODEX_CONFIG`)."
  );
}

const terminatingChildren = new WeakSet<ChildProcess>();

function terminateChild(child: ChildProcess | null | undefined): void {
  if (!child) return;
  if (terminatingChildren.has(child)) return;
  terminatingChildren.add(child);

  const pid = child.pid;
  const signalGroupOrChild = (sig: NodeJS.Signals) => {
    if (process.platform !== "win32" && pid) {
      try {
        process.kill(-pid, sig);
        return;
      } catch {
        // Group may already be gone (ESRCH) or not a process group leader; fall through to direct child
      }
    }
    if (child.exitCode === null && child.signalCode === null) {
      try {
        child.kill(sig);
      } catch {
        // ignore
      }
    }
  };

  signalGroupOrChild("SIGTERM");
  // Keep the SIGKILL grace timer active even if the leader (e.g. `npx`) already exited,
  // so SIGTERM-ignoring descendants in the same process group are reliably reaped.
  const killTimer = setTimeout(() => {
    signalGroupOrChild("SIGKILL");
  }, 2000);
  killTimer.unref?.();
}

function withTimeout<T>(
  promise: Promise<T>,
  ms: number,
  message: string,
  onTimeout?: () => void,
): Promise<T> {
  if (ms <= 0) return promise;
  return new Promise<T>((resolvePromise, rejectPromise) => {
    const timer = setTimeout(() => {
      onTimeout?.();
      rejectPromise(new Error(message));
    }, ms);
    timer.unref?.();
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolvePromise(value);
      },
      (err) => {
        clearTimeout(timer);
        rejectPromise(err);
      },
    );
  });
}

function requireWorkspacePath(root: string, path: string): void {
  const rel = relative(root, path);
  if (isAbsolute(rel) || rel === ".." || rel.startsWith(`..${sep}`)) {
    throw new Error("ACP filesystem path is outside the task workspace");
  }
}

async function workspaceTarget(root: string, requested: string, allowNew: boolean) {
  const realRoot = await realpath(root);
  const absolute = resolve(realRoot, requested);
  try {
    const path = await realpath(absolute);
    requireWorkspacePath(realRoot, path);
    return { path, exists: true };
  } catch (error) {
    if (!allowNew || (error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }

  // Walk up parent directories until an existing ancestor is found, ensuring no intermediate
  // symlink escapes `realRoot` and no dangling symlink occupies the target path.
  const missingSegments: string[] = [];
  let cursor = absolute;
  let existingAncestor: string | null = null;
  while (true) {
    const entry = await lstat(cursor).catch((error: NodeJS.ErrnoException) => {
      if (error.code === "ENOENT") return undefined;
      throw error;
    });
    if (entry) {
      if (missingSegments.length === 0) {
        // A dangling symlink is an existing directory entry, not a new workspace file.
        throw new Error("ACP filesystem target is not a new workspace file");
      }
      existingAncestor = await realpath(cursor);
      requireWorkspacePath(realRoot, existingAncestor);
      break;
    }
    const parent = dirname(cursor);
    if (parent === cursor) {
      throw new Error("ACP filesystem path is outside the task workspace");
    }
    missingSegments.unshift(basename(cursor));
    cursor = parent;
  }

  const path = resolve(existingAncestor, ...missingSegments);
  requireWorkspacePath(realRoot, path);
  if (await lstat(path).catch((error: NodeJS.ErrnoException) => {
    if (error.code !== "ENOENT") throw error;
    return undefined;
  })) throw new Error("ACP filesystem target is not a new workspace file");
  return { path, exists: false };
}

async function resolveExtraRoots(rawRoots: string[] | undefined): Promise<string[]> {
  if (!rawRoots?.length) return [];
  const out: string[] = [];
  for (const raw of rawRoots) {
    const trimmed = raw.trim();
    if (!trimmed) continue;
    const expanded = trimmed === "~" ? homedir() : trimmed.startsWith("~/") ? resolve(homedir(), trimmed.slice(2)) : resolve(trimmed);
    try {
      out.push(await realpath(expanded));
    } catch {
      out.push(expanded);
    }
  }
  return out;
}

/**
 * Which allowed roots veto reads of `.git` and `.harness/worktrees` (same predicate as writes):
 * `extra` only the extra roots, `all` the primary root too.
 */
type ReadProtection = "none" | "extra" | "all";

async function resolveAllowedTarget(
  primaryRoot: string,
  extraRoots: string[],
  requested: string,
  allowNew: boolean,
  protectWrites = false,
  protectReads: ReadProtection = "none",
): Promise<{ path: string; exists: boolean; root: string }> {
  let firstErr: unknown;
  for (const root of [primaryRoot, ...extraRoots]) {
    const readProtected = protectReads === "all" || (protectReads === "extra" && root !== primaryRoot);
    // Refuse protected targets before any side effect, checking the lexical path first and the canonical
    // path once resolved. A protected hit is final and never falls through to later roots.
    if (protectWrites && isProtectedWritePath(root, requested)) throw PROTECTED_WRITE_ERROR();
    if (readProtected && isProtectedWritePath(root, requested)) throw PROTECTED_READ_ERROR();
    let target: { path: string; exists: boolean };
    try {
      target = await workspaceTarget(root, requested, allowNew);
    } catch (err) {
      if (!firstErr) firstErr = err;
      continue;
    }
    if (protectWrites && isProtectedWritePath(root, target.path)) throw PROTECTED_WRITE_ERROR();
    if (readProtected && isProtectedWritePath(root, target.path)) throw PROTECTED_READ_ERROR();
    return { ...target, root };
  }
  throw firstErr ?? new Error("ACP filesystem path is outside allowed roots");
}

const PROTECTED_WRITE_ERROR = () => new Error("ACP filesystem write target is protected (.git or .harness/worktrees)");
const PROTECTED_READ_ERROR = () => new Error("ACP filesystem read target is protected (.git or .harness/worktrees)");

/**
 * Read protection for a policy: writers keep unrestricted reads; read-only agents never read `.git` or
 * `.harness/worktrees` through extra roots, nor through the primary root unless the workspace is itself a
 * linked git worktree (a task or `_verify` checkout, whose `.git` is a file) so a reviewer can inspect its own
 * checkout.
 */
async function readProtectionFor(workspace: string, canWrite: boolean): Promise<ReadProtection> {
  if (canWrite) return "none";
  const dotGit = await lstat(resolve(workspace, ".git")).catch(() => undefined);
  return dotGit?.isFile() ? "extra" : "all";
}

async function openWorkspaceFile(
  primaryRoot: string,
  extraRoots: string[],
  requested: string,
  write: boolean,
  protectReads: ReadProtection = "none",
) {
  const { path: target, exists, root } = await resolveAllowedTarget(primaryRoot, extraRoots, requested, write, write, protectReads);
  if (write && !exists) {
    await mkdir(dirname(target), { recursive: true });
    const actualParent = await realpath(dirname(target));
    requireWorkspacePath(root, actualParent);
  }
  const flags = (write ? constants.O_WRONLY : constants.O_RDONLY) | constants.O_NOFOLLOW |
    constants.O_NONBLOCK | (exists ? 0 : constants.O_CREAT | constants.O_EXCL);
  const file = await open(target, flags);
  try {
    const actual = await realpath(target);
    requireWorkspacePath(root, actual);
    if (write && isProtectedWritePath(root, actual)) throw PROTECTED_WRITE_ERROR();
    if (!write && (protectReads === "all" || (protectReads === "extra" && root !== primaryRoot)) &&
        isProtectedWritePath(root, actual)) throw PROTECTED_READ_ERROR();
    const opened = await file.stat();
    const current = await stat(actual);
    if (!opened.isFile() || (write && opened.nlink !== 1) || opened.dev !== current.dev || opened.ino !== current.ino) {
      throw new Error("ACP filesystem request requires a regular workspace file");
    }
    return file;
  } catch (error) {
    await file.close();
    throw error;
  }
}

/** Flattens JSON-RPC RequestError.data (the ACP SDK wraps thrown agent errors as -32603 `{ details }`, or as the parsed object when the message was JSON). Walks `cause` so re-wrapped errors keep their detail. */
function acpErrorDetail(err: unknown): string {
  const parts: string[] = [];
  // True when `v` carries detail, even if an earlier hop already recorded the same text.
  const push = (v: unknown): boolean => {
    if (typeof v !== "string") return false;
    const s = v.trim();
    if (!s) return false;
    if (!parts.includes(s)) parts.push(s);
    return true;
  };
  let cur: unknown = err;
  for (let depth = 0; cur && typeof cur === "object" && depth < 3; depth++) {
    // Throwing getters or cyclic data end the walk instead of escaping from error reporting.
    try {
      const data = (cur as { data?: unknown }).data;
      if (typeof data === "string") push(data);
      else if (data && typeof data === "object") {
        const d = data as Record<string, unknown>;
        const found = [
          push(d.details),
          push(d.message),
          push(typeof d.error === "string" ? d.error : (d.error as { message?: unknown } | undefined)?.message),
        ].some(Boolean);
        if (!found && Object.keys(d).length > 0) push(JSON.stringify(d));
      }
      cur = (cur as { cause?: unknown }).cause;
    } catch {
      break;
    }
  }
  return parts.join(" | ").slice(0, 1000);
}

/** Last whole stderr lines that fit in `max` chars, without Node process warnings (never the failure reason). */
function stderrTail(buf: string, max = 500): string {
  const lines = buf
    .split(/\r?\n/)
    .map((l) => l.trimEnd())
    .filter((l) => l.trim() && !/^\(node:\d+\)\s/.test(l) && !/^\(Use `node --trace-/.test(l));
  let out = "";
  for (let i = lines.length - 1; i >= 0; i--) {
    const next = out ? `${lines[i]}\n${out}` : lines[i]!;
    if (next.length > max) break;
    out = next;
  }
  return out || (lines.at(-1)?.slice(-max) ?? "");
}

function extractToolErrorSnippet(update: Record<string, unknown>): string | undefined {
  const rawOut = update.rawOutput;
  if (typeof rawOut === "string" && rawOut.trim()) return rawOut.trim().slice(0, 220);
  if (rawOut && typeof rawOut === "object") {
    const obj = rawOut as Record<string, unknown>;
    const msg = obj.error ?? obj.message ?? obj.stderr ?? obj.output;
    if (typeof msg === "string" && msg.trim()) return msg.trim().slice(0, 220);
  }
  const content = update.content;
  if (Array.isArray(content)) {
    for (const item of content) {
      if (item && typeof item === "object") {
        const c = item as Record<string, unknown>;
        const inner = (c.content && typeof c.content === "object" ? (c.content as Record<string, unknown>).text : c.text) as
          | string
          | undefined;
        if (typeof inner === "string" && inner.trim()) return inner.trim().slice(0, 220);
      }
    }
  }
  return undefined;
}

const MAX_CONSECUTIVE_SILENT_RECOVERIES = 4;
const MAX_TOTAL_RECOVERY_CONTINUATIONS = 10;
/** Wall-clock cap on the single missing-return wrap-up turn. */
const WRAP_UP_TIMEOUT_MS = 180_000;
const IDENTICAL_FAILURE_BREAKER = 6;
const DEFAULT_QUOTA_RETRY_TIMEOUT_MS = 45_000;
const DEFAULT_STARTUP_TIMEOUT_MS = 45_000;
const DEFAULT_IDLE_TIMEOUT_MS = 600_000;

export class AcpRunner {
  constructor(private readonly profile: AcpProfileConfig) {}

  /**
   * Executes an agent turn over ACP v1 (with unstable extensions for usage_update,
   * systemPrompt, configOptions, mcpServers, and _session/steering).
   * Features:
   *  - Subscription-pool-aware soft quota watchdog (lets native 429 retries succeed across pool
   *    rotations, disarms on any forward progress, and cleanly returns `quota_paused` on timeout)
   *  - Automatic tool-failure continuation recovery so agents keep thinking after a failed/denied tool
   * Accepts an optional in-process `testAgent` for deterministic unit testing.
   */
  async run(req: RunAgentRequest, testAgent?: AgentApp): Promise<RunAgentResult> {
    validateRunPolicy(req);
    if (this.profile.models?.length && !this.profile.models.includes(req.model)) {
      throw new Error(`Model ${req.model} is unavailable on ACP profile ${this.profile.id}`);
    }
    const workspace = await realpath(req.cwd);
    const extraWriteRoots = await resolveExtraRoots(req.extraWriteRoots);
    const extraReadRoots = await resolveExtraRoots([...(req.extraReadRoots ?? []), ...(req.extraWriteRoots ?? [])]);
    const controlOnly = req.toolPolicy === "control_plane_only";
    const canWrite = req.toolPolicy === "read_write_workspace";
    const allowShell = canWrite || (req.toolPolicy === "read_only_workspace" && Boolean(req.allowShellInReadOnly));
    const readProtection = await readProtectionFor(workspace, canWrite);
    const policyModeDesc = describeToolPolicyMode(req.toolPolicy, Boolean(req.allowShellInReadOnly));
    const prepared = prepareAcpSpawn(this.profile, {
      systemPrompt: req.systemPrompt,
      toolPolicy: req.toolPolicy,
      allowShellInReadOnly: Boolean(req.allowShellInReadOnly),
    });
    if ((allowShell || !controlOnly) && req.extraPathDirs?.length) {
      const existingPath = prepared.env.PATH ?? process.env.PATH ?? "";
      prepared.env.PATH = [...req.extraPathDirs, existingPath].filter(Boolean).join(process.platform === "win32" ? ";" : ":");
    }
    for (const [key, value] of Object.entries(req.extraEnv ?? {})) {
      if (typeof value === "string" && !/^path$/i.test(key)) prepared.env[key] = value;
    }

    const child = testAgent
      ? null
      : spawn(prepared.command, prepared.args, {
          cwd: req.cwd,
          env: prepared.env,
          detached: process.platform !== "win32",
          stdio: ["pipe", "pipe", "pipe"],
        });

    let spawnErr: Error | null = null;
    if (child) {
      child.on("error", (err) => {
        spawnErr = err;
      });
      // Prevent unhandled EPIPE / stream errors from crashing `harnessd` when an ACP CLI exits early
      child.stdin?.on("error", () => {});
      child.stdout?.on("error", () => {});
      child.stderr?.on("error", () => {});
    }

    const hasHarnessBridge = Boolean(req.daemonSocketPath && req.mcpBridgeEntrypoint);
    const harnessToolNames = new Set(req.tools.map(({ name }) => name));
    const claudeToolNames = new Set(req.tools.map(({ name }) => `mcp__harness__${name}`));
    const externalMcpEntries: AcpMcpServerEntry[] = [];
    if (!controlOnly && req.externalMcpServers?.length) {
      const seen = new Set<string>(["harness"]);
      for (const ext of req.externalMcpServers) {
        const name = typeof ext?.name === "string" ? ext.name.trim() : "";
        const normalized = name.toLowerCase();
        if (!normalized || seen.has(normalized)) continue;
        seen.add(normalized);
        if (ext.enabled === false || typeof ext.command !== "string" || !ext.command.trim()) continue;
        const mountName = normalized === "octofs" ? "octofs" : name;
        externalMcpEntries.push({
          name: mountName,
          command: ext.command.trim(),
          args: ext.args ?? [],
          env: Object.entries(ext.env ?? {}).map(([envName, val]) => ({ name: envName, value: interpolateEnv(val) })),
        });
      }
    }
    const externalMcpNames = new Set(externalMcpEntries.map((s) => s.name));
    const codexHarnessCalls = new Set<string>();
    const codexExternalCalls = new Set<string>();
    const quotaTimeoutMs = Math.max(0, req.quotaRetryTimeoutMs ?? DEFAULT_QUOTA_RETRY_TIMEOUT_MS);
    const startupTimeoutMs = Math.max(0, req.startupTimeoutMs ?? DEFAULT_STARTUP_TIMEOUT_MS);
    const idleTimeoutMs = Math.max(0, req.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS);

    let accumulatedText = "";
    let textSinceLastTool = "";
    let needsParagraphBreak = false;
    let lastUsage: RunnerUsage | undefined;
    // A cost is delivered once its turn ends so a turn that several models contributed to is not credited to one.
    let pendingCost: RunnerCost | undefined;
    const flushCost = (turnModelCount = 0) => {
      if (!pendingCost || !lastUsage) return;
      lastUsage = {
        ...lastUsage,
        cost: turnModelCount > 1 ? { ...pendingCost, model: undefined } : pendingCost,
      };
      pendingCost = undefined;
      req.callbacks.onUsage?.(lastUsage);
    };
    let rolloverTriggered = false;
    let rolloverWarningSent = false;
    let wrapUpAttempted = false;
    let wrapUpTimer: ReturnType<typeof setTimeout> | null = null;
    // maxTurns is an advisory progress-reminder threshold: one reminder, never a stop.
    let turnReminderSent = false;
    let turnsUsed = 0;
    let completedToolInWave = false;
    const inFlightToolCalls = new Set<string>();
    let sessionId: string | undefined;
    let steeringSupported = false;
    const isAmpProfile = isAmpAcpProfile(this.profile);
    let activeConnection: ClientConnection | null = null;
    const toolTitlesById = new Map<string, string>();

    // Inactivity watchdog + bounded cancellation grace timer for active `session/prompt` calls
    let idleTimedOut = false;
    let idleTimer: ReturnType<typeof setTimeout> | null = null;
    let cancelGraceTimer: ReturnType<typeof setTimeout> | null = null;
    let resolveActivePromptOnGrace: ((val: Record<string, unknown>) => void) | null = null;
    let rejectActivePromptOnIdle: ((err: Error) => void) | null = null;
    const clearIdleWatchdog = () => {
      if (idleTimer) {
        clearTimeout(idleTimer);
        idleTimer = null;
      }
      if (cancelGraceTimer) {
        clearTimeout(cancelGraceTimer);
        cancelGraceTimer = null;
      }
      if (childExitFlushTimer) {
        clearTimeout(childExitFlushTimer);
        childExitFlushTimer = null;
      }
      resolveActivePromptOnGrace = null;
      rejectActivePromptOnIdle = null;
      rejectActivePromptOnChildExit = null;
    };

    // Child exit/close + PID liveness detection: a dead ACP child fails the active turn promptly instead of
    // waiting out the idle watchdog (which an in-flight tool call stretches to 30 minutes).
    let rejectActivePromptOnChildExit: ((err: Error) => void) | null = null;
    let childExitFlushTimer: ReturnType<typeof setTimeout> | null = null;
    let childGoneError: Error | null = null;
    // Recorded even after the prompt settled, so a transport EOF that wins the race can still report the exit status.
    let childExitInfo: { code: number | null; signal: NodeJS.Signals | null } | null = null;
    const childExitMessage = (code: number | null, signal: NodeJS.Signals | null) =>
      `ACP agent process exited during an active turn (code=${code ?? "null"}, signal=${signal ?? "null"})`;
    const onChildGone = (code: number | null, signal: NodeJS.Signals | null) => {
      if (!childExitInfo && (code !== null || signal !== null)) childExitInfo = { code, signal };
      // Abort, rollover, idle timeout, and cancel grace terminate the child on purpose.
      if (req.signal.aborted || rolloverTriggered || idleTimedOut || cancelGraceTimer) return;
      if (!rejectActivePromptOnChildExit) return;
      const rejectNow = () => {
        const cb = rejectActivePromptOnChildExit;
        if (!cb || req.signal.aborted || rolloverTriggered || idleTimedOut || cancelGraceTimer) return;
        childGoneError = new Error(childExitMessage(code ?? child?.exitCode ?? null, signal ?? child?.signalCode ?? null));
        clearIdleWatchdog();
        terminateChild(child);
        cb(childGoneError);
      };
      // Exit and close only mean the process and its OS pipes are gone, not that the stream adapter and SDK have
      // delivered every buffered message. Give a final JSON-RPC response (success or error) already on stdout a bounded
      // moment to settle the prompt; settling clears this timer. Keep an armed timer instead of resetting it.
      if (childExitFlushTimer) return;
      childExitFlushTimer = setTimeout(() => {
        childExitFlushTimer = null;
        rejectNow();
      }, 250);
      childExitFlushTimer.unref?.();
    };
    const onChildExit = (code: number | null, signal: NodeJS.Signals | null) => onChildGone(code, signal);
    let livenessTimer: ReturnType<typeof setInterval> | null = null;
    if (child) {
      child.once("exit", onChildExit);
      child.once("close", onChildExit);
      livenessTimer = setInterval(() => {
        if (child.exitCode !== null || child.signalCode !== null) {
          onChildGone(child.exitCode, child.signalCode);
        } else if (typeof child.pid === "number") {
          try {
            process.kill(child.pid, 0);
          } catch (err) {
            // EPERM means the PID exists but belongs to someone else: treat as alive.
            if ((err as NodeJS.ErrnoException).code === "ESRCH") onChildGone(child.exitCode, child.signalCode);
          }
        }
      }, 5_000);
      livenessTimer.unref?.();
    }
    const armCancelGraceTimer = (stopReason: string) => {
      if (cancelGraceTimer || !resolveActivePromptOnGrace) return;
      const graceMs = idleTimeoutMs > 0 ? Math.min(5_000, Math.max(100, idleTimeoutMs * 2)) : 5_000;
      cancelGraceTimer = setTimeout(() => {
        cancelGraceTimer = null;
        const cb = resolveActivePromptOnGrace;
        clearIdleWatchdog();
        terminateChild(child);
        cb?.({ stopReason });
      }, graceMs);
      cancelGraceTimer.unref?.();
    };
    const resetIdleWatchdog = () => {
      if (!rejectActivePromptOnIdle || idleTimeoutMs <= 0 || idleTimedOut) return;
      if (idleTimer) clearTimeout(idleTimer);
      // When an acknowledged tool call (e.g. `wait_children`, `wait_timer`, `submit_for_merge`) is in flight,
      // give it a generous ceiling instead of tripping the short model-inactivity timer.
      const effectiveMs = inFlightToolCalls.size > 0 ? Math.max(idleTimeoutMs, 1_800_000) : idleTimeoutMs;
      idleTimer = setTimeout(() => {
        idleTimer = null;
        if (quotaWatchdogTriggered || rolloverTriggered || req.signal.aborted) return;
        idleTimedOut = true;
        const cb = rejectActivePromptOnIdle;
        clearIdleWatchdog();
        if (sessionId && activeConnection) {
          void activeConnection.agent.notify("session/cancel", { sessionId }).catch(() => {});
        }
        terminateChild(child);
        cb?.(new Error(`ACP session timed out after ${effectiveMs}ms of inactivity`));
      }, effectiveMs);
      idleTimer.unref?.();
    };

    // Fire-and-forget steering into the live session; adapters without a steering channel get nothing.
    const sendSteeringText = (text: string) => {
      if (!sessionId || !activeConnection) return;
      if (steeringSupported) {
        void activeConnection.agent
          .request("_session/steering", { sessionId, prompt: [{ type: "text", text }] })
          .catch(() => {});
      } else if (isAmpProfile) {
        void activeConnection.agent
          .request("_amp/session/steer", { sessionId, steerId: randomUUID(), prompt: [{ type: "text", text }] })
          .catch(() => {});
      }
    };
    const isReviewer = req.tools.some((t) => t.name === "submit_review_verdict");
    const advanceTurnWaveIfNeeded = () => {
      if (!completedToolInWave || inFlightToolCalls.size > 0) return;
      completedToolInWave = false;
      // A new tool call after a completed wave means work is continuing: remind once, never cancel.
      if (!turnReminderSent && req.maxTurns > 0 && turnsUsed >= req.maxTurns) {
        turnReminderSent = true;
        sendSteeringText(turnReminderText(req.tier, isReviewer));
      }
      turnsUsed += 1;
      req.callbacks.onTurn?.(turnsUsed);
    };

    // Per-turn & cumulative tool-failure / read-only-denial tracking (for continuation recovery + robust circuit breaker)
    let turnFailedTools: Array<{ title: string; detail?: string }> = [];
    let turnDeniedTools: string[] = [];
    let turnSucceededTools = 0;
    const allToolFailures: string[] = [];
    let lastIdenticalFailureKey = "";
    let identicalFailureStreak = 0;
    let identicalFailureBreakerTripped = false;

    // Soft subscription-pool quota retry watchdog state
    let quotaWatchdogTimer: ReturnType<typeof setTimeout> | null = null;
    let quotaWatchdogTriggered = false;
    let quotaRetrying = false;
    let quotaRetryHits = 0;
    let quotaRejected = false;
    let textSinceQuotaRejection = "";
    let lastQuotaReason = "";
    let lastQuotaResetAt: string | null = null;
    let lastQuotaHard = false;

    const disarmQuotaWatchdog = (recovered: boolean) => {
      if (quotaWatchdogTimer) {
        clearTimeout(quotaWatchdogTimer);
        quotaWatchdogTimer = null;
      }
      if (quotaRetrying && recovered && !quotaWatchdogTriggered) {
        quotaRetrying = false;
        req.callbacks.onQuotaRetryState?.({
          state: "recovered",
          retryCount: quotaRetryHits,
          timeoutMs: quotaTimeoutMs,
        });
      } else {
        quotaRetrying = false;
      }
    };

    const noteQuotaSignal = (info: { reason: string; resetAt?: string | null; isHardExhaustion?: boolean }) => {
      if (quotaWatchdogTriggered || rolloverTriggered || req.signal.aborted) return;
      quotaRetryHits += 1;
      if (info.reason) lastQuotaReason = info.reason;
      if (info.resetAt) lastQuotaResetAt = info.resetAt;
      if (info.isHardExhaustion) lastQuotaHard = true;

      if (!quotaRetrying) {
        quotaRetrying = true;
        req.callbacks.onQuotaRetryState?.({
          state: "retrying",
          retryCount: quotaRetryHits,
          timeoutMs: quotaTimeoutMs,
          reason: lastQuotaReason,
          resetAt: lastQuotaResetAt,
        });
      }

      // Arm once per stall episode (do NOT reset timer on repeated 429s so a tight retry loop cannot hang forever)
      if (!quotaWatchdogTimer && quotaTimeoutMs > 0) {
        quotaWatchdogTimer = setTimeout(() => {
          quotaWatchdogTimer = null;
          if (rolloverTriggered || req.signal.aborted) return;
          quotaWatchdogTriggered = true;
          if (sessionId && activeConnection) {
            void activeConnection.agent.notify("session/cancel", { sessionId }).catch(() => {});
          }
          if (child) {
            setTimeout(() => {
              terminateChild(child);
            }, 350).unref?.();
          }
        }, quotaTimeoutMs);
        quotaWatchdogTimer.unref?.();
      }
    };

    let stderrBuf = "";
    let stderrLineBuf = "";
    let stderrPartialMatched = false;
    const inspectStderrText = (text: string): boolean => {
      const detected = detectQuotaErrorText(text);
      if (detected.isQuota) {
        noteQuotaSignal({
          reason: detected.summary,
          resetAt: detected.resetAtIso ?? null,
          isHardExhaustion: detected.isHardExhaustion,
        });
        return true;
      }
      return false;
    };
    child?.stderr?.setEncoding("utf8").on("data", (chunk: string) => {
      stderrBuf = (stderrBuf + chunk).slice(-4096);
      stderrLineBuf += chunk;
      const lines = stderrLineBuf.split(/\r?\n/);
      stderrLineBuf = lines.pop() ?? "";
      if (lines.length > 0) {
        for (let i = 0; i < lines.length; i++) {
          const line = lines[i]!;
          if (i === 0 && stderrPartialMatched) {
            stderrPartialMatched = false;
            continue;
          }
          if (line.trim()) inspectStderrText(line);
        }
        stderrPartialMatched = false;
      }
      if (!stderrPartialMatched && stderrLineBuf.trim()) {
        stderrPartialMatched = inspectStderrText(stderrLineBuf);
      }
      if (stderrLineBuf.length > 4096) {
        stderrLineBuf = stderrLineBuf.slice(-512);
        stderrPartialMatched = false;
      }
    });
    child?.stderr?.on("end", () => {
      if (!stderrPartialMatched && stderrLineBuf.trim()) {
        inspectStderrText(stderrLineBuf);
      }
      stderrLineBuf = "";
      stderrPartialMatched = false;
    });

    const app = client()
      .onNotification("session/update", ({ params }) => {
        const update = (params as unknown as { update?: Record<string, unknown> }).update;
        if (params.sessionId !== sessionId || !update || typeof update !== "object") return;
        resetIdleWatchdog();
        const kind = String(update.sessionUpdate ?? "");

        if (kind === "agent_message_chunk") {
          advanceTurnWaveIfNeeded();
          const content = update.content as { type?: string; text?: string } | undefined;
          if (content?.type === "text" && typeof content.text === "string") {
            if (quotaRejected) {
              textSinceQuotaRejection += content.text;
            }
            if (content.text.trim().length > 0) {
              // Forward progress! Subscription-pool rotation succeeded — disarm any pending 429 watchdog
              // unless the assistant text itself is a rate-limit / quota exhaustion explanation.
              const quotaCheckText = quotaRejected ? textSinceQuotaRejection : content.text;
              if (!detectQuotaErrorText(quotaCheckText).isQuota) {
                disarmQuotaWatchdog(true);
              }
            }
            let chunk = content.text;
            if (needsParagraphBreak && accumulatedText.length > 0 && !accumulatedText.endsWith("\n") && !chunk.startsWith("\n")) {
              chunk = "\n\n" + chunk;
            }
            needsParagraphBreak = false;
            accumulatedText += chunk;
            textSinceLastTool += content.text;
            req.callbacks.onTextDelta?.(chunk);
          }
        } else if (kind === "agent_thought_chunk") {
          advanceTurnWaveIfNeeded();
          const content = update.content as { type?: string; text?: string } | undefined;
          if (content?.type === "text" && typeof content.text === "string") {
            req.callbacks.onThoughtDelta?.(content.text);
          }
        } else if (kind === "usage_update") {
          const used = Number(update.used ?? 0);
          const rawSize = Number(update.size ?? 0);
          // Some adapters (e.g. amp-acp) report a default 200k size even for 1M-context models;
          // prefer the larger of adapter-reported size and catalog contextWindow when available.
          const size = Math.max(rawSize > 0 ? rawSize : 0, req.contextWindow ?? 0);
          const costObj = update.cost as { amount?: unknown; currency?: unknown } | null | undefined;
          const updateMeta = (update._meta ?? {}) as Record<string, unknown>;
          if (typeof costObj?.amount === "number" && typeof costObj.currency === "string") {
            pendingCost = {
              amount: costObj.amount,
              currency: costObj.currency,
              basis: "session_cumulative",
              model: typeof updateMeta["_claude/model"] === "string" ? updateMeta["_claude/model"] : undefined,
            };
          }
          lastUsage = { used, size, cost: lastUsage?.cost };
          req.callbacks.onUsage?.(lastUsage);

          // Harvest live passive subscription quota signals (`_claude/rateLimit`, generic `rateLimit`/`rateLimits`)
          const rlPayload = (updateMeta["_claude/rateLimit"] ?? updateMeta.rateLimit) as Record<string, unknown> | undefined;
          if (rlPayload && typeof rlPayload === "object") {
            req.callbacks.onPassiveQuota?.({
              profileId: this.profile.id,
              model: typeof updateMeta["_claude/model"] === "string" ? updateMeta["_claude/model"] : req.model,
              raw: rlPayload,
              observedAt: Date.now(),
            });
            if (rlPayload.status === "rejected" || rlPayload.overageStatus === "rejected") {
              quotaRejected = true;
              textSinceQuotaRejection = "";
              const resetIso =
                typeof rlPayload.resetsAt === "number" ? new Date(rlPayload.resetsAt * 1000).toISOString() : null;
              noteQuotaSignal({
                reason: `Rate limit rejected (${String(rlPayload.rateLimitType ?? "quota")})`,
                resetAt: resetIso,
                isHardExhaustion: true,
              });
            } else if (rlPayload.status !== undefined || rlPayload.overageStatus !== undefined) {
              quotaRejected = false;
              textSinceQuotaRejection = "";
            }
          }
          const codexRl = (updateMeta["_codex/rateLimits"] ?? updateMeta.rateLimits) as Record<string, unknown> | undefined;
          if (codexRl && typeof codexRl === "object") {
            req.callbacks.onPassiveQuota?.({
              profileId: this.profile.id,
              model: req.model,
              raw: { codexRateLimits: codexRl },
              observedAt: Date.now(),
            });
          }

          const ratio = size > 0 ? used / size : 0;
          const softWarnThreshold = req.rolloverThreshold ? Math.max(0.5, req.rolloverThreshold - 0.12) : undefined;
          if (
            !rolloverWarningSent &&
            !rolloverTriggered &&
            softWarnThreshold &&
            req.rolloverThreshold &&
            ratio >= softWarnThreshold &&
            ratio < req.rolloverThreshold &&
            sessionId &&
            activeConnection &&
            (steeringSupported || isAmpProfile)
          ) {
            rolloverWarningSent = true;
            const pct = Math.round(ratio * 100);
            const limitPct = Math.round(req.rolloverThreshold * 100);
            sendSteeringText(
              `Context usage is at ${pct}% (hard rollover at ${limitPct}%). Finish your current atomic step and call \`request_rollover\` with a thorough anchored handoff (goal_invariants, architectural_decisions, active_risks, next_actions) so no context is lost.`,
            );
          }

          if (
            !rolloverTriggered &&
            req.rolloverThreshold &&
            size > 0 &&
            ratio >= req.rolloverThreshold &&
            sessionId &&
            activeConnection
          ) {
            rolloverTriggered = true;
            void activeConnection.agent.notify("session/cancel", { sessionId }).catch(() => {});
            armCancelGraceTimer("cancelled");
          }
        } else if (kind === "tool_call" || kind === "tool_call_update") {
          if (accumulatedText.length > 0) needsParagraphBreak = true;
          textSinceLastTool = "";
          const callId = update.toolCallId ? String(update.toolCallId) : update.id ? String(update.id) : undefined;
          const status = String(update.status ?? "in_progress");
          if (kind === "tool_call") {
            advanceTurnWaveIfNeeded();
            if (callId && status !== "completed" && status !== "failed") {
              inFlightToolCalls.add(callId);
            }
          } else if (callId && (status === "in_progress" || status === "pending")) {
            inFlightToolCalls.add(callId);
          }
          if (this.profile.adapter === "codex" && callId) {
            const meta = update._meta as { is_mcp_tool_call?: boolean } | undefined;
            const input = update.rawInput as { server?: string; tool?: string } | undefined;
            if (kind === "tool_call") {
              codexHarnessCalls.delete(callId);
              codexExternalCalls.delete(callId);
              if (update.kind === "execute" && meta?.is_mcp_tool_call === true && input?.server && input.tool) {
                if (input.server === "harness" && harnessToolNames.has(input.tool)) {
                  codexHarnessCalls.add(callId);
                } else if (externalMcpNames.has(input.server) && (canWrite || isReadOnlyExternalMcpTool(input.server, input.tool))) {
                  codexExternalCalls.add(callId);
                }
              }
            }
            if (update.status === "completed" || update.status === "failed") {
              codexHarnessCalls.delete(callId);
              codexExternalCalls.delete(callId);
            }
          }
          const rawTitle = update.title ? String(update.title) : update.name ? String(update.name) : undefined;
          if (callId && rawTitle) {
            toolTitlesById.set(callId, rawTitle);
          }
          const resolvedTitle = rawTitle ?? (callId ? toolTitlesById.get(callId) : undefined) ?? "tool";
          if (status === "completed") {
            if (callId) inFlightToolCalls.delete(callId);
            resetIdleWatchdog();
            completedToolInWave = true;
            quotaRejected = false;
            textSinceQuotaRejection = "";
            disarmQuotaWatchdog(true);
            turnSucceededTools += 1;
            // Any successful tool call resets the identical-failure streak so exploratory debugging never trips the breaker
            identicalFailureStreak = 0;
            lastIdenticalFailureKey = "";
          } else if (status === "failed") {
            if (callId) inFlightToolCalls.delete(callId);
            resetIdleWatchdog();
            completedToolInWave = true;
            const detail = extractToolErrorSnippet(update);
            turnFailedTools.push({ title: resolvedTitle, detail });
            allToolFailures.push(detail ? `${resolvedTitle}: ${detail}` : resolvedTitle);
            const failKey = `${resolvedTitle.toLowerCase()}::${(detail ?? "").toLowerCase().slice(0, 120)}`;
            if (failKey === lastIdenticalFailureKey) {
              identicalFailureStreak += 1;
            } else {
              lastIdenticalFailureKey = failKey;
              identicalFailureStreak = 1;
            }
            if (identicalFailureStreak >= IDENTICAL_FAILURE_BREAKER && !identicalFailureBreakerTripped && sessionId && activeConnection) {
              identicalFailureBreakerTripped = true;
              void activeConnection.agent.notify("session/cancel", { sessionId }).catch(() => {});
            }
          }
          req.callbacks.onToolEvent?.({
            id: callId,
            name: resolvedTitle,
            status,
            kind: update.kind ? String(update.kind) : undefined,
            title: resolvedTitle,
            args: toolEventPreview(update.rawInput),
            ...(status === "failed" ? { error: toolEventPreview(update.rawOutput) } : { result: toolEventPreview(update.rawOutput) }),
          });
          for (const art of extractArtifactsFromToolUpdate(update)) {
            req.callbacks.onArtifact?.(art);
          }
        }
      })
      .onRequest("session/request_permission", async ({ params }) => {
        if (sessionId && params.sessionId === sessionId) resetIdleWatchdog();
        const p = params as unknown as {
          _meta?: { is_mcp_tool_approval?: boolean };
          toolCall?: {
            toolCallId?: string; title?: string; name?: string; kind?: string;
            rawInput?: Record<string, unknown>;
            locations?: Array<{ path: string }>;
            _meta?: {
              claudeCode?: { toolName?: string; mcpServer?: { name?: string; source?: string } };
              "gemini-cli"?: { mcp?: { server_name?: string; tool_name?: string } };
            };
          };
          options?: Array<{ optionId: string; kind: string }>;
        };
        const callId = p.toolCall?.toolCallId ? String(p.toolCall.toolCallId) : undefined;
        const title = String(p.toolCall?.title ?? p.toolCall?.name ?? (callId ? toolTitlesById.get(callId) : "") ?? "");
        const toolKind = p.toolCall?.kind;
        const rawInput = (p.toolCall?.rawInput && typeof p.toolCall.rawInput === "object" ? p.toolCall.rawInput : undefined) as
          | Record<string, unknown>
          | undefined;
        const options = p.options ?? [];

        const allowOpt =
          options.find((o) => o.kind === "allow_once") ??
          options.find((o) => o.kind === "allow_always");
        const denyOpt = options.find((o) => o.kind === "reject_once") ?? options.find((o) => o.kind === "reject_always");

        const extractExternalMcp = (
          rawName: string,
        ): { server: string; tool: string } | null => {
          const firstToken = rawName.trim().split(/\s+/)[0] ?? "";
          if (!firstToken) return null;
          for (const srv of externalMcpNames) {
            const escaped = srv.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
            const re = new RegExp(`^(?:mcp(?:__|_|\\.|/|:))?${escaped}(?:__|_|\\.|/|:)(.+)$`);
            const m = firstToken.match(re);
            if (m?.[1]) return { server: srv, tool: m[1] };
          }
          return null;
        };

        // Normalize MCP tool names emitted by Gemini / Antigravity / Codex / generic ACP adapters across ALL separators:
        // e.g. `mcp__harness__code_exec`, `mcp_harness_goal_list`, `mcp.harness.tool_search`, `harness:code_exec`, `harness/code_exec`, `harness.code_exec`
        const rawCandidateName = p.toolCall?.name ?? p.toolCall?.title ?? title;
        const normalizedHarnessTool = rawCandidateName
          .trim()
          .split(/\s+/)[0]
          ?.replace(/^mcp(?:__|_|\/|\.|:)harness(?:__|_|\/|\.|:)/, "")
          .replace(/^harness(?:__|_|\/|\.|:)/, "") ?? "";
        const nonFileToolKind =
          toolKind !== "read" && toolKind !== "edit" && toolKind !== "delete" && toolKind !== "move";
        const noFileLocations = !p.toolCall?.locations?.length;
        const matchesNormalizedHarness =
          nonFileToolKind &&
          noFileLocations &&
          normalizedHarnessTool.length > 0 &&
          harnessToolNames.has(normalizedHarnessTool);
        const normalizedExt = nonFileToolKind ? extractExternalMcp(rawCandidateName) : null;
        const matchesNormalizedExternal =
          normalizedExt !== null &&
          (canWrite || isReadOnlyExternalMcpTool(normalizedExt.server, normalizedExt.tool));

        const claude = p.toolCall?._meta?.claudeCode;
        // Adapter 0.109 sends `_meta.claudeCode` only for MCP/subagent calls; native tools carry the name in `toolCall.name`.
        const claudeToolName =
          claude?.toolName ??
          (this.profile.adapter === "claude" && typeof p.toolCall?.name === "string" ? p.toolCall.name : undefined);
        const claudeUntrustedSource =
          claude?.mcpServer?.source === "project" ||
          claude?.mcpServer?.source === "user" ||
          claude?.mcpServer?.source === "local";
        const claudeExtParsed = claudeToolName ? extractExternalMcp(claudeToolName) : null;
        const claudeExternalMcp =
          this.profile.adapter === "claude" &&
          claude?.mcpServer?.name !== undefined &&
          !claudeUntrustedSource &&
          externalMcpNames.has(claude.mcpServer.name) &&
          (canWrite ||
            isReadOnlyExternalMcpTool(
              claude.mcpServer.name,
              claudeExtParsed?.tool ?? claudeToolName ?? "",
            ));
        // Allow Claude's built-in ToolSearch (required to load deferred MCP tools) and harness MCP calls across
        // any non-user/project/local MCP server source (`dynamic`, `sdk`, `acp`, `inline`, etc.) and any toolKind.
        const claudeToolSearch =
          this.profile.adapter === "claude" &&
          claudeToolName === "ToolSearch" &&
          (hasHarnessBridge || externalMcpNames.size > 0);
        const claudeHarnessMcp =
          this.profile.adapter === "claude" &&
          !claudeUntrustedSource &&
          claudeToolName !== undefined &&
          claudeToolNames.has(claudeToolName) &&
          (claude?.mcpServer === undefined || claude.mcpServer.name === "harness");

        const codexRawServer = typeof rawInput?.server === "string" ? rawInput.server : undefined;
        const codexRawTool = typeof rawInput?.tool === "string" ? rawInput.tool : undefined;
        const codexDirectHarness = Boolean(codexRawServer === "harness" && codexRawTool && harnessToolNames.has(codexRawTool));
        const codexDirectExternal = Boolean(
          codexRawServer &&
            codexRawTool &&
            externalMcpNames.has(codexRawServer) &&
            (canWrite || isReadOnlyExternalMcpTool(codexRawServer, codexRawTool)),
        );
        const codexTrackedHarness =
          toolKind === "execute" &&
          p._meta?.is_mcp_tool_approval === true &&
          callId !== undefined &&
          codexHarnessCalls.has(callId);
        const codexTrackedExternal =
          toolKind === "execute" &&
          p._meta?.is_mcp_tool_approval === true &&
          callId !== undefined &&
          codexExternalCalls.has(callId);
        const codexHarnessMcp =
          this.profile.adapter === "codex" &&
          ((toolKind === "execute" && p._meta?.is_mcp_tool_approval === true && codexDirectHarness) ||
            codexTrackedHarness ||
            matchesNormalizedHarness);
        const codexExternalMcp =
          this.profile.adapter === "codex" &&
          ((toolKind === "execute" && p._meta?.is_mcp_tool_approval === true && codexDirectExternal) ||
            codexTrackedExternal ||
            matchesNormalizedExternal);

        const geminiMcp = p.toolCall?._meta?.["gemini-cli"]?.mcp;
        const geminiExternalMcp =
          this.profile.adapter === "gemini" &&
          ((geminiMcp?.server_name !== undefined &&
            externalMcpNames.has(geminiMcp.server_name) &&
            (canWrite || isReadOnlyExternalMcpTool(geminiMcp.server_name, geminiMcp.tool_name ?? ""))) ||
            matchesNormalizedExternal);
        const geminiHarnessMcp =
          this.profile.adapter === "gemini" &&
          ((geminiMcp?.server_name === "harness" &&
            geminiMcp.tool_name !== undefined &&
            harnessToolNames.has(geminiMcp.tool_name)) ||
            matchesNormalizedHarness);

        const genericHarnessMcp = this.profile.adapter === "generic" && matchesNormalizedHarness;
        const genericExternalMcp = this.profile.adapter === "generic" && matchesNormalizedExternal;

        const harnessMcpAllowed =
          hasHarnessBridge && (claudeHarnessMcp || codexHarnessMcp || geminiHarnessMcp || genericHarnessMcp);
        const externalMcpAllowed =
          !controlOnly && (claudeExternalMcp || codexExternalMcp || geminiExternalMcp || genericExternalMcp);

        const validateReadLocations = async (): Promise<boolean> => {
          const paths: string[] = [];
          for (const loc of p.toolCall?.locations ?? []) {
            if (loc?.path) paths.push(loc.path);
          }
          if (rawInput) {
            for (const key of ["path", "file_path", "filePath", "directory", "dir", "cwd", "target_file", "SearchPath"]) {
              const val = rawInput[key];
              if (typeof val === "string" && val.trim()) paths.push(val.trim());
            }
          }
          // The literal directory before the first glob metacharacter (`src/**/*.ts` -> `src`); a rooted glob
          // keeps `/` so it can never collapse to the workspace.
          const extractGlobPrefix = (raw: string): string => {
            const globIdx = raw.search(/[*?[{]/);
            if (globIdx < 0) return raw;
            const prefix = raw.slice(0, globIdx);
            const stripped = prefix.replace(/\/+$/, "");
            return stripped || (prefix.startsWith("/") ? "/" : ".");
          };
          const hasGlob = (s: string) => /[*?[{]/.test(s);
          // The tool resolves relative paths against the workspace, so bind them there before validation;
          // otherwise a relative path that escapes the workspace could match a nonexistent path under an extra
          // read root.
          const bindToWorkspace = (raw: string): string => (isAbsolute(raw) ? raw : resolve(workspace, raw));
          for (const rawPath of paths) {
            if (rawPath.startsWith("~")) return false; // never expanded here, so its target is unprovable
            if (hasGlob(rawPath) && /(?:^|[\\/])\.\.(?:[\\/]|$)/.test(rawPath)) return false;
            try {
              await resolveAllowedTarget(workspace, extraReadRoots, bindToWorkspace(extractGlobPrefix(rawPath)), true, false, readProtection);
            } catch {
              return false;
            }
          }
          // Glob's pattern can name its own root (`/etc/**`, `{/etc,src}/**`, `**/../..`); Grep's pattern is a
          // regex, so only Glob is checked.
          if (p.toolCall?.name === "Glob") {
            if (typeof rawInput?.pattern !== "string") return false;
            const pattern = rawInput.pattern.trim();
            if (!pattern || pattern.includes("\0")) return false;
            // Braces may only alternate file names or extensions after the final `/`; a brace that selects a
            // directory, or a malformed one, could make the search root differ from the validated prefix.
            if (pattern.includes("{") || pattern.includes("}")) {
              const lastSlash = pattern.lastIndexOf("/");
              const dirPart = lastSlash >= 0 ? pattern.slice(0, lastSlash + 1) : "";
              const filePart = lastSlash >= 0 ? pattern.slice(lastSlash + 1) : pattern;
              if (dirPart.includes("{") || dirPart.includes("}")) return false;
              if (!/^(?:[^{}]|\{(?:(?!\.\.)[^{}/\\,])+(?:,(?:(?!\.\.)[^{}/\\,])+)+\})+$/.test(filePart)) return false;
            }
            if (/(?:^|[{,])\s*~/.test(pattern)) return false;
            if (/(?:^|[\\/{,])\.\.(?:[\\/},]|$)/.test(pattern)) return false;
            if (/[{,]\s*\//.test(pattern)) return false;
            const baseValues = ["path", "directory", "dir", "cwd", "SearchPath"]
              .map((key) => rawInput[key])
              .filter((v) => v !== undefined);
            if (baseValues.length > 1) return false; // several bases could disagree about where Glob searches
            try {
              let effectiveBase = workspace;
              if (baseValues.length === 1) {
                const base = baseValues[0];
                if (typeof base !== "string" || !base.trim() || base.trim().startsWith("~")) return false;
                effectiveBase = (
                  await resolveAllowedTarget(workspace, extraReadRoots, bindToWorkspace(base.trim()), true, false, readProtection)
                ).path;
              }
              const prefix = extractGlobPrefix(pattern);
              await resolveAllowedTarget(
                workspace,
                extraReadRoots,
                isAbsolute(prefix) ? prefix : resolve(effectiveBase, prefix),
                true,
                false,
                readProtection,
              );
            } catch {
              return false;
            }
          }
          return true;
        };

        const unwrapShellArgv = (argv: string[]): string => {
          if (argv.length >= 3) {
            // Only a system shell wrapper in plain or login mode counts as transparent (other startup flags, such
            // as interactive mode, change parsing); anything else is validated as a command.
            if (/^(?:\/(?:usr\/)?bin\/)?(?:sh|bash|zsh)$/.test(argv[0]!) && /^-l?c$/.test(argv[1]!)) {
              return argv[2]!;
            }
          }
          // A direct argv runs without a shell: single-quote each element so the validator sees the exact vector.
          return argv.map((a) => `'${a.replaceAll("'", `'\\''`)}'`).join(" ");
        };

        // Only the command the adapter will actually run is validated. The display title is adapter-rendered
        // text that is not tied to the executed command, so a call without a command field is denied.
        const extractShellCommand = (): string => {
          const fields = ["command", "cmd", "argv"].filter((key) => rawInput?.[key] !== undefined);
          if (fields.length !== 1) return ""; // none, or several that could disagree about what runs
          const value = rawInput![fields[0]!];
          if (typeof value === "string" && fields[0] !== "argv") return value;
          if (Array.isArray(value) && value.length > 0 && value.every((a) => typeof a === "string")) return unwrapShellArgv(value);
          return "";
        };

        // The shell's working directory: the workspace unless the call names one, which must itself be inside
        // an allowed read root. Returns undefined when the named directory is not provably inside.
        const resolveShellCwd = async (): Promise<string | undefined> => {
          const named = ["cwd", "workdir", "directory", "dir"].map((key) => rawInput?.[key]).filter((v) => v !== undefined);
          if (named.length === 0) return workspace;
          if (named.length > 1 || typeof named[0] !== "string" || !named[0]) return undefined;
          try {
            const target = await resolveAllowedTarget(workspace, extraReadRoots, named[0], false, false, readProtection);
            return (await stat(target.path)).isDirectory() ? target.path : undefined;
          } catch {
            return undefined;
          }
        };

        const READ_ONLY_OTHER_TOOL_RE =
          /^(?:read|read_file|view_file|view_file_outline|view_code_item|list_dir|list_directory|list_files|find_by_name|find_files|glob|grep|grep_search|search|search_files|codebase_search|ast_grep|view_signatures|extract_lines)\b/i;

        const isRead = toolKind === "read" || toolKind === "search";
        const isWrite = toolKind === "edit" || toolKind === "delete" || toolKind === "move";
        const isExecute = toolKind === "execute";
        let workspaceCall = false;
        // Allow native workspace reads/searches, read-only shell inspection commands, writes (when canWrite),
        // and full shell commands (when allowShell) for non-control-plane agents.
        if (!controlOnly && !claude?.mcpServer && !p._meta?.is_mcp_tool_approval && !geminiMcp) {
          if (isRead) {
            workspaceCall = await validateReadLocations();
          } else if (canWrite && isWrite) {
            try {
              // Check every reported target: locations plus any path the tool input names, since either may be
              // what the adapter writes. Non-string path values are unprovable and deny.
              const targets = (p.toolCall?.locations ?? []).map((location) => location.path as unknown);
              for (const key of ["path", "file_path", "filePath", "target_file", "notebook_path", "old_path", "new_path", "source", "destination"]) {
                if (rawInput?.[key] !== undefined) targets.push(rawInput[key]);
              }
              for (const target of targets) {
                if (typeof target !== "string" || !target.trim()) throw new Error("unprovable write target");
                await resolveAllowedTarget(workspace, extraWriteRoots, target, toolKind === "edit" || toolKind === "move", true);
              }
              workspaceCall = targets.length > 0;
            } catch {
              workspaceCall = false;
            }
          } else if (isExecute) {
            if (allowShell) {
              workspaceCall = true;
            } else {
              const cmd = extractShellCommand();
              const shellCwd = cmd.length > 0 ? await resolveShellCwd() : undefined;
              workspaceCall =
                shellCwd !== undefined &&
                isSafeReadOnlyShellCommand(cmd, {
                  cwd: shellCwd,
                  readRoots: [workspace, ...extraReadRoots],
                  protectedRoots:
                    readProtection === "all" ? [workspace, ...extraReadRoots] : readProtection === "extra" ? extraReadRoots.filter((root) => root !== workspace) : [],
                });
            }
          } else if (toolKind === "think") {
            workspaceCall = true;
          } else if (canWrite && (toolKind === "fetch" || toolKind === "other" || toolKind === undefined)) {
            workspaceCall = true;
          } else if ((toolKind === "other" || toolKind === undefined) && READ_ONLY_OTHER_TOOL_RE.test(rawCandidateName.trim())) {
            workspaceCall = await validateReadLocations();
          }
        }
        if (!sessionId || params.sessionId !== sessionId || !(workspaceCall || claudeToolSearch || harnessMcpAllowed || externalMcpAllowed)) {
          const deniedLabel = title || toolKind || "native tool";
          turnDeniedTools.push(deniedLabel);
          allToolFailures.push(`${deniedLabel} (blocked by ${policyModeDesc})`);
          return (
            denyOpt
              ? { outcome: { outcome: "selected", optionId: denyOpt.optionId } }
              : { outcome: { outcome: "cancelled" } }
          ) as never;
        }

        if (allowOpt) {
          return { outcome: { outcome: "selected", optionId: allowOpt.optionId } } as never;
        }
        return { outcome: { outcome: "cancelled" } } as never;
      })
      .onRequest("fs/read_text_file", async ({ params }) => {
        if (sessionId && params.sessionId === sessionId) resetIdleWatchdog();
        if (!sessionId || params.sessionId !== sessionId || controlOnly) {
          throw new Error(`Read denied: agent is running in ${policyModeDesc} mode`);
        }
        const p = params as unknown as { path: string; line?: number; limit?: number };
        const file = await openWorkspaceFile(workspace, extraReadRoots, p.path, false, readProtection);
        let raw: string;
        try {
          raw = await file.readFile("utf8");
        } finally {
          await file.close();
        }
        req.callbacks.onArtifact?.({
          kind: "read",
          path: p.path,
          summary: "fs/read_text_file",
          gitSha: null,
          exitCode: null,
        });
        if (p.line || p.limit) {
          const lines = raw.split("\n");
          const start = Math.max(0, (p.line ?? 1) - 1);
          const slice = p.limit ? lines.slice(start, start + p.limit) : lines.slice(start);
          return { content: slice.join("\n") };
        }
        return { content: raw };
      })
      .onRequest("fs/write_text_file", async ({ params }) => {
        if (sessionId && params.sessionId === sessionId) resetIdleWatchdog();
        if (!sessionId || params.sessionId !== sessionId || !canWrite) {
          throw new Error(`Write denied: agent is running in ${policyModeDesc} mode`);
        }
        const p = params as unknown as { path: string; content: string };
        const file = await openWorkspaceFile(workspace, extraWriteRoots, p.path, true);
        try {
          await file.truncate(0);
          await file.writeFile(p.content, "utf8");
        } finally {
          await file.close();
        }
        req.callbacks.onArtifact?.({
          kind: "write",
          path: p.path,
          summary: "fs/write_text_file",
          gitSha: null,
          exitCode: null,
        });
        return {};
      });

    const conn = testAgent
      ? app.connect(testAgent)
      : app.connect(
          ndJsonStream(
            Writable.toWeb(child!.stdin!) as unknown as WritableStream<Uint8Array>,
            Readable.toWeb(child!.stdout!) as unknown as ReadableStream<Uint8Array>,
          ),
        );
    activeConnection = conn;

    const onAbort = () => {
      if (sessionId) {
        void conn.agent.notify("session/cancel", { sessionId }).catch(() => {});
      }
      terminateChild(child);
    };
    req.signal.addEventListener("abort", onAbort, { once: true });

    try {
      const initRes = (await withTimeout(
        conn.agent.request("initialize", {
          protocolVersion: PROTOCOL_VERSION,
          clientInfo: { name: "pi-harness", version: "0.1.0" },
          clientCapabilities: {
            fs: { readTextFile: !controlOnly, writeTextFile: canWrite },
            ...(this.profile.adapter === "claude" ? {} : {
              _meta: { jetbrains: { air: { capabilities: ["sessionFailure"] } } },
            }),
          },
        } as never),
        startupTimeoutMs,
        `ACP initialize timed out after ${startupTimeoutMs}ms`,
        () => terminateChild(child),
      )) as unknown as Record<string, unknown>;

      const initMeta = (initRes._meta ?? {}) as Record<string, unknown>;
      const steeringMeta = initMeta.steering as { supported?: boolean } | undefined;
      steeringSupported = Boolean(steeringMeta?.supported);
      const agentCapabilities = (initRes.agentCapabilities ?? {}) as Record<string, unknown>;
      const sessionCaps = (agentCapabilities.sessionCapabilities ?? {}) as Record<string, unknown>;

      // Wire Harness MCP server via stdio when daemonSocketPath + bridge are configured,
      // plus any user-configured external MCP servers (e.g. ast-grep-mcp, octofs, serena) for non-control-plane agents.
      const mcpServers: AcpMcpServerEntry[] = [];
      if (req.daemonSocketPath && req.mcpBridgeEntrypoint) {
        mcpServers.push({
          name: "harness",
          command: process.execPath,
          args: [req.mcpBridgeEntrypoint],
          env: [
            { name: "PI_HARNESS_SOCKET", value: req.daemonSocketPath },
            { name: "PI_HARNESS_RUN_ID", value: req.runId },
            // `<taskId>@g<generation>` is an execution token the bridge forwards verbatim, so the daemon refuses tool
            // calls from this bridge once that generation is no longer the task's live one.
            { name: "PI_HARNESS_TASK_ID", value: req.taskGeneration !== undefined ? `${req.taskId}@g${req.taskGeneration}` : req.taskId },
          ],
        });
      }
      mcpServers.push(...externalMcpEntries);

      const { params: newSessionParams, needsTurn1SystemPrompt } = buildNewSessionParams(this.profile.adapter, {
        cwd: req.cwd,
        systemPrompt: req.systemPrompt,
        toolPolicy: req.toolPolicy,
        allowShellInReadOnly: Boolean(req.allowShellInReadOnly),
        model: req.model,
        effort: req.effort,
        mcpServers,
        agentCapabilities,
        additionalDirectories: [...new Set([...extraReadRoots, ...extraWriteRoots])],
      });

      const newSessionRes = (await withTimeout(
        conn.agent.request("session/new", newSessionParams as never),
        startupTimeoutMs,
        `ACP session/new timed out after ${startupTimeoutMs}ms`,
        () => terminateChild(child),
      )) as AcpModelState & { sessionId: string };
      sessionId = String(newSessionRes.sessionId);
      req.callbacks.onSessionId?.(sessionId);

      const modelConfigIds = new Set(["model"]);
      const postActions = buildPostSessionActions(this.profile.adapter, sessionId, newSessionRes, {
        toolPolicy: req.toolPolicy,
        model: req.model,
        effort: req.effort,
      }, modelConfigIds);
      for (const action of postActions) {
        let reply: AcpModelState;
        try {
          reply = await conn.agent.request(action.method as never, action.params as never);
        } catch (err) {
          if (action.requiredModel) {
            throw new Error(
              `Could not select model ${action.requiredModel}: ${err instanceof Error ? err.message : String(err)}`,
              { cause: err },
            );
          }
          continue; // Effort is optional.
        }
        const currentModels = reportedModels(reply, modelConfigIds);
        // session/set_model acknowledges the selected ID with an empty success response.
        const acknowledgedLegacyModel = action.method === "session/set_model" &&
          newSessionRes.models?.availableModels?.some(({ modelId }) => modelId === req.model);
        if (action.requiredModel && !currentModels.length && !acknowledgedLegacyModel) {
          throw new Error(`Could not select model ${req.model}: ${action.method} returned no current model`);
        }
        for (const current of currentModels) {
          if (!reportedModelSatisfies(this.profile.adapter, reply, current, req.model, newSessionRes)) {
            throw new Error(`Could not select model ${req.model}: ${action.method} reported ${current}`);
          }
        }
      }

      req.onHandle?.({
        steer: async (message: string) => {
          if (!sessionId) return false;
          if (steeringSupported) {
            try {
              await conn.agent.request("_session/steering", {
                sessionId,
                prompt: [{ type: "text", text: message }],
              });
              return true;
            } catch {
              return false;
            }
          }
          if (isAmpProfile) {
            try {
              await conn.agent.request("_amp/session/steer", {
                sessionId,
                steerId: randomUUID(),
                prompt: [{ type: "text", text: message }],
              });
              return true;
            } catch {
              return false;
            }
          }
          return false;
        },
        cancel: async (reason?: string) => {
          if (reason === "clean_rollover") {
            rolloverTriggered = true;
          }
          if (sessionId) {
            await conn.agent.notify("session/cancel", { sessionId }).catch(() => {});
          }
          if (reason === "clean_rollover") {
            armCancelGraceTimer("cancelled");
          }
        },
      });

      const shouldPrependSystemPrompt =
        needsTurn1SystemPrompt || (prepared.prependSystemPromptToTurn1 && !sessionCaps.systemPrompt);
      const initialPromptText = shouldPrependSystemPrompt
        ? `${req.systemPrompt}\n\n---\n\n${req.initialPrompt}`
        : req.initialPrompt;

      let nextPromptText = initialPromptText;
      let promptRes: Record<string, unknown> = {};
      let rawStop = "end_turn";
      let airFailureCode = "";
      let consecutiveSilentRecoveries = 0;
      let breakerExplainedOnce = false;
      // Upstream adapter request limit (rawStop "max_turn_requests"): the harness has no turn limit, so a
      // productive turn is re-prompted on the same session; a limit hit with zero progress ends with an error.
      let upstreamLimitStalled = false;
      // One bounded wrap-up turn when the agent ends naturally without settling its task (no return_task).
      let wrapUpTimedOut = false;
      let steerContinuations = 0;

      for (let recoveryAttempt = 0; recoveryAttempt <= MAX_TOTAL_RECOVERY_CONTINUATIONS; recoveryAttempt++) {
        turnsUsed += 1;
        req.callbacks.onTurn?.(turnsUsed);
        completedToolInWave = false;
        inFlightToolCalls.clear();
        turnFailedTools = [];
        turnDeniedTools = [];
        turnSucceededTools = 0;
        textSinceLastTool = "";

        const promptPromise = conn.agent.request("session/prompt", {
          sessionId,
          prompt: [{ type: "text", text: nextPromptText }],
        } as never) as unknown as Promise<Record<string, unknown>>;

        promptRes = await new Promise<Record<string, unknown>>((resolvePrompt, rejectPromptRaw) => {
          resolveActivePromptOnGrace = resolvePrompt;
          // When the harness cut the wrap-up turn off at its time cap, an adapter that answers session/cancel by
          // rejecting the pending prompt ("Request cancelled") ends the turn normally, like a cancelled response would.
          // Abort, rollover, idle timeout, child death, quota and any other RPC error still reject.
          const rejectPrompt = (err: unknown) => {
            if (wrapUpTimedOut && !req.signal.aborted && !rolloverTriggered && !idleTimedOut && !(childGoneError && err === childGoneError)) {
              const detail = `${err instanceof Error ? err.message : String(err)} ${acpErrorDetail(err)}`;
              const code = (err as { code?: unknown } | null)?.code;
              const name = err instanceof Error ? err.name : "";
              const explicitCancel = /-32800|\brequest cancel+ed\b|\boperation cancel+ed\b/i;
              // Only an explicit request-cancelled answer counts: -32800, an internal error wrapping it, or a code-less
              // abort. Any other specific code (e.g. -32602 mentioning "cancelToken") is a genuine RPC error.
              const isCancelled =
                code === -32800 ||
                (code === -32603 && explicitCancel.test(detail)) ||
                (typeof code !== "number" && (name === "AbortError" || /\babort(ed)?\b/i.test(detail) || explicitCancel.test(detail)));
              if (isCancelled && !detectQuotaErrorText(detail).isQuota) {
                resolvePrompt({ stopReason: "end_turn" });
                return;
              }
            }
            rejectPromptRaw(err);
          };
          rejectActivePromptOnChildExit = rejectPrompt;
          if (child && (child.exitCode !== null || child.signalCode !== null)) {
            // The child died before this prompt started: fail now rather than wait on a dead pipe.
            childGoneError = new Error(childExitMessage(child.exitCode, child.signalCode));
            clearIdleWatchdog();
            promptPromise.catch(() => {});
            rejectPrompt(childGoneError);
            return;
          }
          if (idleTimeoutMs > 0) {
            rejectActivePromptOnIdle = rejectPrompt;
            resetIdleWatchdog();
          }
          promptPromise.then(
            (val) => {
              clearIdleWatchdog();
              resolvePrompt(val);
            },
            (err) => {
              clearIdleWatchdog();
              rejectPrompt(err);
            },
          );
        });

        // Harvest passive quota & terminal sessionFailure signals
        const promptMeta = (promptRes._meta ?? {}) as Record<string, unknown>;
        const airFailure = ((promptMeta.jetbrains as Record<string, unknown> | undefined)?.air as Record<string, unknown> | undefined)
          ?.sessionFailure as Record<string, unknown> | string | undefined;
        if (airFailure) {
          const code = typeof airFailure === "string" ? airFailure : String(airFailure.code ?? airFailure.reason ?? "");
          if (code) {
            airFailureCode = code;
            req.callbacks.onPassiveQuota?.({
              profileId: this.profile.id,
              model: req.model,
              raw: { sessionFailure: code, ...(lastQuotaResetAt ? { resetAt: lastQuotaResetAt } : {}) },
              observedAt: Date.now(),
            });
          }
        }

        // Merge PromptResponse.usage if available, and run fallback rollover evaluation when usage_update was omitted
        const promptUsage = promptRes.usage as Record<string, unknown> | undefined;
        if (promptUsage) {
          const hadUsageUpdate = lastUsage !== undefined;
          const totalFromParts =
            Number(promptUsage.inputTokens ?? 0) +
            Number(promptUsage.outputTokens ?? 0) +
            Number(promptUsage.cachedReadTokens ?? 0) +
            Number(promptUsage.cachedWriteTokens ?? 0);
          const fallbackUsed =
            promptUsage.totalTokens != null ? Number(promptUsage.totalTokens) : totalFromParts;
          const used = lastUsage?.used ?? fallbackUsed;
          const size = (lastUsage?.size && lastUsage.size > 0 ? lastUsage.size : undefined) ?? req.contextWindow ?? 0;
          lastUsage = {
            used,
            size,
            inputTokens: Number(promptUsage.inputTokens ?? 0),
            outputTokens: Number(promptUsage.outputTokens ?? 0),
            cachedReadTokens: promptUsage.cachedReadTokens == null ? undefined : Number(promptUsage.cachedReadTokens),
            cachedWriteTokens: promptUsage.cachedWriteTokens == null ? undefined : Number(promptUsage.cachedWriteTokens),
            thoughtTokens: promptUsage.thoughtTokens == null ? undefined : Number(promptUsage.thoughtTokens),
            cost: lastUsage?.cost,
          };
          if (!hadUsageUpdate) {
            req.callbacks.onUsage?.(lastUsage);
            if (!rolloverTriggered && req.rolloverThreshold && size > 0 && used / size >= req.rolloverThreshold) {
              rolloverTriggered = true;
            }
          }
        }
        const turnModels = (promptMeta.quota as { model_usage?: unknown[] } | undefined)?.model_usage;
        flushCost(Array.isArray(turnModels) ? turnModels.length : 0);

        rawStop = String(promptRes.stopReason ?? "end_turn");
        if (wrapUpTimer) {
          clearTimeout(wrapUpTimer);
          wrapUpTimer = null;
        }
        // A wrap-up turn the harness cut off at its time cap ends as a normal turn end, not a self-cancel.
        if (wrapUpTimedOut && rawStop === "cancelled") rawStop = "end_turn";

        const turnQuotaDetected = detectQuotaErrorText(quotaRejected ? textSinceQuotaRejection || accumulatedText : accumulatedText);
        // Require either a real rejection/rate-limit signal OR a short hard-exhaustion message with 0 successful tools,
        // so ordinary prose mentioning "rate limiting" never falsely pauses healthy turns while text-only caps still pause.
        const shortHardQuotaText =
          turnSucceededTools === 0 &&
          accumulatedText.trim().length > 0 &&
          accumulatedText.trim().length <= 600 &&
          turnQuotaDetected.isHardExhaustion;
        const turnQuotaFalseCompletion =
          (quotaRejected && (textSinceQuotaRejection.trim().length === 0 || turnQuotaDetected.isQuota)) ||
          (quotaRetryHits > 0 && turnSucceededTools === 0 && turnQuotaDetected.isQuota) ||
          shortHardQuotaText;

        // Never continue if rollover, quota watchdog, quota rejection, user abort, or quota failure occurred
        if (
          rolloverTriggered ||
          quotaWatchdogTriggered ||
          turnQuotaFalseCompletion ||
          req.signal.aborted ||
          ["quota_exhausted", "rate_limited", "budget_exhausted"].includes(airFailureCode)
        ) {
          break;
        }

        // The wrap-up turn is final: no breaker, silent-recovery or adapter-limit continuation after it.
        if (wrapUpAttempted) break;

        if (rawStop === "max_turn_requests") {
          if (turnSucceededTools === 0) {
            upstreamLimitStalled = true;
            break;
          }
          nextPromptText =
            "The agent runtime ended that turn at its own request limit; the harness has no turn limit. Continue from where you left off.";
          if (accumulatedText.length > 0) needsParagraphBreak = true;
          // Productive continuation: does not consume the failure-recovery budget.
          recoveryAttempt -= 1;
          continue;
        }

        // If the identical-failure circuit breaker tripped (6+ consecutive identical failures with zero
        // intervening successes), give the model ONE clean chance to stop retrying and explain the blocker.
        if (identicalFailureBreakerTripped && !breakerExplainedOnce && recoveryAttempt < MAX_TOTAL_RECOVERY_CONTINUATIONS) {
          breakerExplainedOnce = true;
          identicalFailureBreakerTripped = false;
          identicalFailureStreak = 0;
          lastIdenticalFailureKey = "";
          const lastFail = turnFailedTools[turnFailedTools.length - 1];
          nextPromptText = [
            `Circuit breaker triggered: tool "${lastFail?.title ?? "tool"}" failed ${IDENTICAL_FAILURE_BREAKER} consecutive times with the same error${lastFail?.detail ? ` (${lastFail.detail})` : ""} and no successful tool calls in between.`,
            "Do NOT retry the exact same failing call again. Either switch to a genuinely different approach, or clearly report that you are unable to complete this action and explain why.",
          ].join("\n");
          if (accumulatedText.length > 0) needsParagraphBreak = true;
          continue;
        }

        // Any successful tool call resets the silent-recovery streak so normal multi-step exploration never trips!
        if (turnSucceededTools > 0) {
          consecutiveSilentRecoveries = 0;
        }

        // Tool-Failure Continuation Recovery:
        // If a tool call failed or was denied and the agent stopped immediately without producing
        // any reasoning/response after the failure, prompt it to continue thinking on the same session.
        // If the model DOES emit text (e.g. explaining it cannot do something), we accept it immediately!
        const hadToolProblem = turnFailedTools.length > 0 || turnDeniedTools.length > 0;
        const stoppedSilentAfterTool = hadToolProblem && (textSinceLastTool.trim().length === 0 || rawStop === "cancelled");
        if (
          stoppedSilentAfterTool &&
          consecutiveSilentRecoveries < MAX_CONSECUTIVE_SILENT_RECOVERIES &&
          recoveryAttempt < MAX_TOTAL_RECOVERY_CONTINUATIONS
        ) {
          consecutiveSilentRecoveries += 1;
          const failureSummary = [
            ...turnFailedTools.map((f) => `- Tool "${f.title}" failed${f.detail ? `: ${f.detail}` : ""}`),
            ...turnDeniedTools.map((d) =>
              canWrite
                ? `- Tool "${d}" was blocked: execution permission denied by harness policy (${policyModeDesc})`
                : `- Tool "${d}" was blocked by harness policy (${policyModeDesc})`,
            ),
          ].join("\n");
          nextPromptText = [
            "Your previous tool call did not succeed and your turn ended before you finished responding:",
            failureSummary,
            "",
            "Do NOT silently stop here. Figure out an allowed alternative approach if possible — OR if this task/action genuinely cannot be completed, clearly report that you are unable to do it and explain the blocker.",
          ].join("\n");
          if (accumulatedText.length > 0) needsParagraphBreak = true;
          continue;
        }
        // Steering queued while the agent could not take it natively opens the next turn (before any wrap-up).
        if (rawStop !== "cancelled" && !req.isTaskSettled?.() && steerContinuations < MAX_RUNNER_STEER_CONTINUATIONS) {
          const steers = req.drainPendingSteers?.() ?? [];
          if (steers.length > 0) {
            steerContinuations += 1;
            nextPromptText = `[Steering Update]\n${steers.join("\n\n")}`;
            if (accumulatedText.length > 0) needsParagraphBreak = true;
            // Does not consume the failure-recovery budget.
            recoveryAttempt -= 1;
            continue;
          }
        }
        if (
          rawStop !== "cancelled" &&
          req.missingReturnWrapUpPrompt &&
          !wrapUpAttempted &&
          !req.isTaskSettled?.()
        ) {
          wrapUpAttempted = true;
          nextPromptText = req.missingReturnWrapUpPrompt;
          if (accumulatedText.length > 0) needsParagraphBreak = true;
          wrapUpTimer = setTimeout(() => {
            wrapUpTimer = null;
            wrapUpTimedOut = true;
            if (sessionId) void conn.agent.notify("session/cancel", { sessionId }).catch(() => {});
            armCancelGraceTimer("end_turn");
          }, WRAP_UP_TIMEOUT_MS);
          wrapUpTimer.unref?.();
          // Does not consume the failure-recovery budget.
          recoveryAttempt -= 1;
          continue;
        }
        break;
      }

      const finalQuotaDetected = detectQuotaErrorText(quotaRejected ? textSinceQuotaRejection || accumulatedText : accumulatedText);
      const finalShortHardQuota =
        turnSucceededTools === 0 &&
        accumulatedText.trim().length > 0 &&
        accumulatedText.trim().length <= 600 &&
        finalQuotaDetected.isHardExhaustion;
      const quotaFalseCompletion =
        (quotaRejected && (textSinceQuotaRejection.trim().length === 0 || finalQuotaDetected.isQuota)) ||
        (quotaRetryHits > 0 && turnSucceededTools === 0 && finalQuotaDetected.isQuota) ||
        finalShortHardQuota;

      if (
        quotaWatchdogTriggered ||
        quotaFalseCompletion ||
        ["quota_exhausted", "rate_limited", "budget_exhausted"].includes(airFailureCode)
      ) {
        const isHard =
          lastQuotaHard ||
          finalQuotaDetected.isHardExhaustion ||
          airFailureCode === "quota_exhausted" ||
          airFailureCode === "budget_exhausted";
        const reason =
          lastQuotaReason ||
          (finalQuotaDetected.isQuota ? finalQuotaDetected.summary : "") ||
          (airFailureCode ? `ACP sessionFailure: ${airFailureCode}` : `Rate-limit retry timed out after ${Math.round(quotaTimeoutMs / 1000)}s`);
        const resetAt = lastQuotaResetAt ?? finalQuotaDetected.resetAtIso ?? null;
        req.callbacks.onPassiveQuota?.({
          profileId: this.profile.id,
          model: req.model,
          raw: {
            sessionFailure: isHard ? "quota_exhausted" : "rate_limited",
            ...(resetAt ? { resetAt } : {}),
          },
          observedAt: Date.now(),
        });
        const quotaPause: QuotaPauseInfo = {
          reason,
          resetAt,
          retryCount: Math.max(1, quotaRetryHits),
          watchdogTimedOut: quotaWatchdogTriggered,
          isHardExhaustion: isHard,
        };
        return {
          text: accumulatedText,
          stopReason: "quota_paused",
          usage: lastUsage,
          sessionId,
          quotaPause,
          toolFailures: allToolFailures.length > 0 ? allToolFailures : undefined,
        };
      }

      if (this.profile.adapter === "codex" && turnSucceededTools === 0) {
        const codexMisconfig = detectCodexCodeModeMisconfig(`${stderrBuf}\n${allToolFailures.join("\n")}`);
        if (codexMisconfig) {
          const tail = stderrTail(stderrBuf);
          const detail = tail ? ` (stderr: ${tail})` : "";
          throw new Error(`ACP runner (${this.profile.id}): ${codexMisconfig}${detail}`);
        }
      }

      if (upstreamLimitStalled && !rolloverTriggered && !req.signal.aborted) {
        const msg =
          `ACP runner (${this.profile.id}): the upstream agent adapter ended a turn at its own request limit (max_turn_requests) ` +
          "without any successful tool call. This is an adapter limit, not a harness turn limit; the harness stopped re-prompting to avoid a hot loop.";
        return {
          text: accumulatedText ? `${accumulatedText}\n\n${msg}` : msg,
          stopReason: "error",
          usage: lastUsage,
          sessionId,
          toolFailures: allToolFailures.length > 0 ? allToolFailures : undefined,
        };
      }

      const stopReason: RunAgentResult["stopReason"] = rolloverTriggered
        ? "rollover_requested"
        : rawStop === "cancelled" || req.signal.aborted
          ? "cancelled"
          : "completed";

      return {
        text: accumulatedText,
        stopReason,
        usage: lastUsage,
        sessionId,
        toolFailures: allToolFailures.length > 0 ? allToolFailures : undefined,
        ...(wrapUpAttempted ? { wrapUpAttempted: true } : {}),
      };
    } catch (err) {
      if (this.profile.adapter === "codex" && turnSucceededTools === 0) {
        const primaryMsg = (spawnErr ?? err) instanceof Error ? (spawnErr ?? err as Error).message : String(spawnErr ?? err);
        const codexMisconfig = detectCodexCodeModeMisconfig(`${primaryMsg}\n${stderrBuf}\n${allToolFailures.join("\n")}`);
        if (codexMisconfig) {
          if (primaryMsg.includes(codexMisconfig)) throw (spawnErr ?? err);
          const tail = stderrTail(stderrBuf);
          const detail = tail ? ` (stderr: ${tail})` : "";
          throw new Error(`ACP runner (${this.profile.id}): ${codexMisconfig}${detail}`);
        }
      }
      if (rolloverTriggered) {
        return {
          text: accumulatedText,
          stopReason: "rollover_requested",
          usage: lastUsage,
          sessionId,
          toolFailures: allToolFailures.length > 0 ? allToolFailures : undefined,
        };
      }
      if (idleTimedOut && !quotaWatchdogTriggered) {
        throw new Error(`ACP runner (${this.profile.id}) timed out after ${idleTimeoutMs}ms of inactivity`);
      }
      let primary = spawnErr ?? err;
      const childDied = Boolean(childGoneError && primary === childGoneError);
      // Normal pipe closure rejects the prompt with a transport EOF before the exit flush timer fires. Reconcile it
      // with the child's exit (bounded wait) so the outcome does not depend on which event arrived first. A JSON-RPC
      // error response from the agent carries a numeric code and is never treated as transport closure.
      if (!childDied && child && sessionId && !spawnErr && !req.signal.aborted && typeof (primary as { code?: unknown } | null)?.code !== "number") {
        if (!childExitInfo && child.exitCode === null && child.signalCode === null) {
          await new Promise<void>((resolveWait) => {
            const done = () => {
              clearTimeout(waitTimer);
              child.off("exit", done);
              resolveWait();
            };
            const waitTimer = setTimeout(done, 250);
            child.once("exit", done);
          });
        }
        const info = childExitInfo as { code: number | null; signal: NodeJS.Signals | null } | null;
        const code = info?.code ?? child.exitCode;
        const signal = info?.signal ?? child.signalCode;
        if (code !== null || signal !== null) {
          primary = childGoneError = new Error(childExitMessage(code, signal));
        }
      }
      const baseMsg = primary instanceof Error ? primary.message : String(primary);
      const errDetail = acpErrorDetail(primary);
      const errMsg = errDetail && !baseMsg.includes(errDetail) ? `${baseMsg}: ${errDetail}` : baseMsg;
      // Harvest structured `rateLimit` info from ACP JSON-RPC error data (e.g. Claude `turnErrorData` when refused before first usage)
      const rawData =
        (primary as { data?: unknown } | null)?.data ?? (primary as { cause?: { data?: unknown } } | null)?.cause?.data;
      const errData = rawData && typeof rawData === "object" ? (rawData as Record<string, unknown>) : undefined;
      const errRateLimit =
        errData && typeof errData === "object" && errData.rateLimit && typeof errData.rateLimit === "object"
          ? (errData.rateLimit as Record<string, unknown>)
          : null;
      if (errRateLimit) {
        req.callbacks.onPassiveQuota?.({
          profileId: this.profile.id,
          model: req.model,
          raw: errRateLimit,
          observedAt: Date.now(),
        });
        if (typeof errRateLimit.resetsAt === "number" && !lastQuotaResetAt) {
          lastQuotaResetAt = new Date(errRateLimit.resetsAt * 1000).toISOString();
        }
      }
      const combined = `${errMsg}\n${stderrBuf}`;
      // For a child death the stderr tail is history (live stderr quota lines already drive quotaRetrying), so only
      // the error itself is scanned.
      const isChildExit = Boolean(childGoneError && primary === childGoneError);
      const detected = detectQuotaErrorText(isChildExit ? errMsg : combined);

      // A child death (including transport EOF reconciled with the exit above) is classified from quota state that is
      // still unresolved now (quotaRetrying clears on forward progress), never from lifetime retry history: a recovered
      // worker that later crashes fails instead of pausing. Any other error, such as an explicit JSON-RPC error from a
      // live agent, pauses only when the error itself signals quota.
      const quotaSignal =
        quotaWatchdogTriggered ||
        (isChildExit && quotaRetrying) ||
        detected.isQuota ||
        errData?.errorKind === "rate_limit" ||
        Boolean(errRateLimit);
      if (quotaSignal) {
        const reason = lastQuotaReason || detected.summary || errMsg;
        const resetAt = lastQuotaResetAt ?? detected.resetAtIso ?? null;
        const isHard = lastQuotaHard || detected.isHardExhaustion;
        req.callbacks.onPassiveQuota?.({
          profileId: this.profile.id,
          model: req.model,
          raw: {
            sessionFailure: isHard ? "quota_exhausted" : "rate_limited",
            ...(resetAt ? { resetAt } : {}),
          },
          observedAt: Date.now(),
        });
        return {
          text: accumulatedText,
          stopReason: "quota_paused",
          usage: lastUsage,
          sessionId,
          quotaPause: {
            reason,
            resetAt,
            retryCount: Math.max(1, quotaRetryHits),
            watchdogTimedOut: quotaWatchdogTriggered,
            isHardExhaustion: isHard,
          },
          toolFailures: allToolFailures.length > 0 ? allToolFailures : undefined,
        };
      }

      const tail = stderrTail(stderrBuf);
      const detail = tail ? ` (stderr: ${tail})` : "";
      if (this.profile.adapter === "codex") {
        const codexMisconfig = detectCodexCodeModeMisconfig(combined);
        if (codexMisconfig) {
          if (errMsg.includes(codexMisconfig)) throw new Error(errMsg);
          throw new Error(`ACP runner (${this.profile.id}): ${codexMisconfig}${detail}`);
        }
      }
      throw new Error(`ACP runner (${this.profile.id}) failed: ${errMsg}${detail}`);
    } finally {
      flushCost();
      clearIdleWatchdog();
      if (livenessTimer) clearInterval(livenessTimer);
      if (wrapUpTimer) clearTimeout(wrapUpTimer);
      child?.off("exit", onChildExit);
      child?.off("close", onChildExit);
      disarmQuotaWatchdog(false);
      req.signal.removeEventListener("abort", onAbort);
      try {
        await Promise.resolve(conn.close()).catch(() => {});
      } catch {
        // ignore close error on already-exited pipe
      }
      terminateChild(child);
      prepared.cleanup();
    }
  }

  /**
   * Probes an ACP CLI out-of-band to discover its currently runnable models & effort options
   * for the automated Model Catalog compiler.
   */
  async probeModels(cwd: string, timeoutMs = 15_000, testAgent?: AgentApp): Promise<ProbedModelInfo[]> {
    if (this.profile.models && this.profile.models.length > 0) {
      return this.profile.models.map((wireModelId) => ({ wireModelId }));
    }

    const child = testAgent
      ? null
      : spawn(this.profile.command, this.profile.args ?? [], {
          cwd,
          env: { ...process.env, ...(this.profile.env ?? {}) },
          detached: process.platform !== "win32",
          stdio: ["pipe", "pipe", "pipe"],
        });
    if (child) {
      child.on("error", () => {});
      child.stdin?.on("error", () => {});
      child.stdout?.on("error", () => {});
      child.stderr?.on("error", () => {});
    }

    const app = client();
    const conn = testAgent
      ? app.connect(testAgent)
      : app.connect(
          ndJsonStream(
            Writable.toWeb(child!.stdin!) as unknown as WritableStream<Uint8Array>,
            Readable.toWeb(child!.stdout!) as unknown as ReadableStream<Uint8Array>,
          ),
        );

    const timer = setTimeout(() => {
      try {
        void Promise.resolve(conn.close()).catch(() => {});
      } catch {
        // ignore
      }
      terminateChild(child);
    }, timeoutMs);
    timer.unref?.();

    try {
      await conn.agent.request("initialize", {
        protocolVersion: PROTOCOL_VERSION,
        clientInfo: { name: "pi-harness-probe", version: "0.1.0" },
        clientCapabilities: {},
      } as never);

      const res = (await conn.agent.request("session/new", {
        cwd,
        mcpServers: [],
      } as never)) as unknown as Record<string, unknown>;

      const results: ProbedModelInfo[] = [];
      const configOptions = (res.configOptions ?? []) as AcpConfigOption[];
      const effortOpt = configOptions.find(
        (c) => c.category === "thought_level" || c.id === "effort" || c.id === "reasoning_effort",
      );
      const supportedEfforts = selectValues(effortOpt)?.map((o) => o.value);

      const legacyModels = (
        res.models as { availableModels?: Array<{ modelId: string; name?: string; description?: string }> } | undefined
      )?.availableModels;
      const legacyById = new Map((legacyModels ?? []).map((m) => [m.modelId, m]));
      const explicitClaudeByFamily = new Map<string, string | null>();
      for (const m of legacyModels ?? []) {
        const match = m.modelId.match(/^(?:anthropic\/)?(claude-(opus|sonnet|haiku|fable)-\d+(?:[.-]\d+)*(?:-\d{8})?)$/i);
        if (match) {
          const family = match[2]!.toLowerCase();
          const prev = explicitClaudeByFamily.get(family);
          if (prev === undefined) {
            explicitClaudeByFamily.set(family, match[1]!);
          } else if (prev !== match[1]!) {
            // Multiple distinct versions in the same family -> ambiguous; do not guess
            explicitClaudeByFamily.set(family, null);
          }
        }
      }

      const modelOpt = configOptions.find((c) => c.category === "model" || c.id === "model");
      for (const opt of selectValues(modelOpt) ?? []) {
        const legacy = legacyById.get(opt.value);
        const bareClaude = opt.value.trim().match(/^(?:anthropic\/)?(?:claude-)?(opus|sonnet|haiku|fable)$/i);
        const candidateExplicit = bareClaude ? explicitClaudeByFamily.get(bareClaude[1]!.toLowerCase()) : undefined;
        const explicitModelId = candidateExplicit ?? undefined;
        results.push({
          wireModelId: opt.value,
          name: opt.name ?? legacy?.name,
          ...(opt.description ?? legacy?.description ? { description: opt.description ?? legacy?.description } : {}),
          ...(explicitModelId ? { explicitModelId } : {}),
          supportedEfforts,
        });
      }

      // Legacy fallback (gemini --acp `models.availableModels`)
      if (results.length === 0 && legacyModels) {
        for (const m of legacyModels) {
          results.push({
            wireModelId: m.modelId,
            name: m.name,
            ...(m.description ? { description: m.description } : {}),
          });
        }
      }

      return results;
    } finally {
      clearTimeout(timer);
      try {
        await Promise.resolve(conn.close()).catch(() => {});
      } catch {
        // ignore
      }
      terminateChild(child);
    }
  }
}
