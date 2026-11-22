import { toolEventPreview } from "../protocol/tool-display.js";
import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { constants } from "node:fs";
import { lstat, open, realpath, stat } from "node:fs/promises";
import { basename, dirname, isAbsolute, relative, resolve, sep } from "node:path";
import { Readable, Writable } from "node:stream";
import { client, ndJsonStream, PROTOCOL_VERSION, type AgentApp, type ClientConnection } from "@agentclientprotocol/sdk/unstable";
import type { AcpProfileConfig } from "../config.js";
import {
  buildNewSessionParams,
  buildPostSessionActions,
  extractArtifactsFromToolUpdate,
  isAmpAcpProfile,
  prepareAcpSpawn,
  reportedModels,
  selectValues,
  type AcpModelState,
  type AcpConfigOption,
  type AcpMcpServerEntry,
} from "./acp-shims.js";
import { detectQuotaErrorText } from "./quota-detect.js";
import { validateRunPolicy } from "./types.js";
import type { ProbedModelInfo, QuotaPauseInfo, RunAgentRequest, RunAgentResult, RunnerCost, RunnerUsage } from "./types.js";

function requireWorkspacePath(root: string, path: string): void {
  const rel = relative(root, path);
  if (isAbsolute(rel) || rel === ".." || rel.startsWith(`..${sep}`)) {
    throw new Error("ACP filesystem path is outside the task workspace");
  }
}

async function workspaceTarget(root: string, requested: string, allowNew: boolean) {
  const absolute = resolve(root, requested);
  try {
    const path = await realpath(absolute);
    requireWorkspacePath(root, path);
    return { path, exists: true };
  } catch (error) {
    if (!allowNew || (error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const parent = await realpath(dirname(absolute));
  requireWorkspacePath(root, parent);
  const path = resolve(parent, basename(absolute));
  // A dangling symlink is an existing directory entry, not a new workspace file.
  if (await lstat(path).catch((error) => {
    if (error.code !== "ENOENT") throw error;
    return undefined;
  })) throw new Error("ACP filesystem target is not a new workspace file");
  return { path, exists: false };
}

async function openWorkspaceFile(root: string, requested: string, write: boolean) {
  const { path: target, exists } = await workspaceTarget(root, requested, write);
  const flags = (write ? constants.O_WRONLY : constants.O_RDONLY) | constants.O_NOFOLLOW |
    constants.O_NONBLOCK | (exists ? 0 : constants.O_CREAT | constants.O_EXCL);
  const file = await open(target, flags);
  try {
    const actual = await realpath(target);
    requireWorkspacePath(root, actual);
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
const IDENTICAL_FAILURE_BREAKER = 6;
const DEFAULT_QUOTA_RETRY_TIMEOUT_MS = 45_000;

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
    const prepared = prepareAcpSpawn(this.profile, {
      systemPrompt: req.systemPrompt,
      toolPolicy: req.toolPolicy,
    });
    const controlOnly = req.toolPolicy === "control_plane_only";
    const canWrite = req.toolPolicy === "read_write_workspace";

    const child = testAgent
      ? null
      : spawn(prepared.command, prepared.args, {
          cwd: req.cwd,
          env: prepared.env,
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
    const codexHarnessCalls = new Set<string>();
    const quotaTimeoutMs = Math.max(0, req.quotaRetryTimeoutMs ?? DEFAULT_QUOTA_RETRY_TIMEOUT_MS);

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
    let sessionId: string | undefined;
    let steeringSupported = false;
    const isAmpProfile = isAmpAcpProfile(this.profile);
    let activeConnection: ClientConnection | null = null;
    const toolTitlesById = new Map<string, string>();

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
          if (child && !child.killed) {
            setTimeout(() => {
              try {
                if (!child.killed) child.kill("SIGTERM");
              } catch {
                // ignore
              }
            }, 350).unref();
          }
        }, quotaTimeoutMs);
        quotaWatchdogTimer.unref?.();
      }
    };

    let stderrBuf = "";
    child?.stderr?.setEncoding("utf8").on("data", (chunk: string) => {
      stderrBuf = (stderrBuf + chunk).slice(-4096);
      const detected = detectQuotaErrorText(chunk);
      if (detected.isQuota) {
        noteQuotaSignal({
          reason: detected.summary,
          resetAt: detected.resetAtIso ?? null,
          isHardExhaustion: detected.isHardExhaustion,
        });
      }
    });

    const app = client()
      .onNotification("session/update", ({ params }) => {
        const update = (params as unknown as { update?: Record<string, unknown> }).update;
        if (params.sessionId !== sessionId || !update || typeof update !== "object") return;
        const kind = String(update.sessionUpdate ?? "");

        if (kind === "agent_message_chunk") {
          const content = update.content as { type?: string; text?: string } | undefined;
          if (content?.type === "text" && typeof content.text === "string") {
            if (content.text.trim().length > 0) {
              // Forward progress! Subscription-pool rotation succeeded — disarm any pending 429 watchdog.
              disarmQuotaWatchdog(true);
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
          const content = update.content as { type?: string; text?: string } | undefined;
          if (content?.type === "text" && typeof content.text === "string") {
            req.callbacks.onThoughtDelta?.(content.text);
          }
        } else if (kind === "usage_update") {
          const used = Number(update.used ?? 0);
          const size = Number(update.size ?? 0);
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
              const resetIso =
                typeof rlPayload.resetsAt === "number" ? new Date(rlPayload.resetsAt * 1000).toISOString() : null;
              noteQuotaSignal({
                reason: `Rate limit rejected (${String(rlPayload.rateLimitType ?? "quota")})`,
                resetAt: resetIso,
                isHardExhaustion: true,
              });
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
            const warnMsg =
              `Context usage is at ${pct}% (hard rollover at ${limitPct}%). Finish your current atomic step and call \`request_rollover\` with a thorough anchored handoff (goal_invariants, architectural_decisions, active_risks, next_actions) so no context is lost.`;
            if (steeringSupported) {
              void activeConnection.agent
                .request("_session/steering", { sessionId, prompt: [{ type: "text", text: warnMsg }] })
                .catch(() => {});
            } else if (isAmpProfile) {
              void activeConnection.agent
                .request("_amp/session/steer", {
                  sessionId,
                  steerId: randomUUID(),
                  prompt: [{ type: "text", text: warnMsg }],
                })
                .catch(() => {});
            }
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
          }
        } else if (kind === "tool_call" || kind === "tool_call_update") {
          if (accumulatedText.length > 0) needsParagraphBreak = true;
          textSinceLastTool = "";
          const callId = update.toolCallId ? String(update.toolCallId) : update.id ? String(update.id) : undefined;
          if (this.profile.adapter === "codex" && callId) {
            const meta = update._meta as { is_mcp_tool_call?: boolean } | undefined;
            const input = update.rawInput as { server?: string; tool?: string } | undefined;
            if (kind === "tool_call") {
              codexHarnessCalls.delete(callId);
              if (update.kind === "execute" && meta?.is_mcp_tool_call === true &&
                  input?.server === "harness" && input.tool && harnessToolNames.has(input.tool)) {
                codexHarnessCalls.add(callId);
              }
            }
            if (update.status === "completed" || update.status === "failed") codexHarnessCalls.delete(callId);
          }
          const rawTitle = update.title ? String(update.title) : update.name ? String(update.name) : undefined;
          if (callId && rawTitle) {
            toolTitlesById.set(callId, rawTitle);
          }
          const resolvedTitle = rawTitle ?? (callId ? toolTitlesById.get(callId) : undefined) ?? "tool";
          const status = String(update.status ?? "in_progress");
          if (status === "completed") {
            disarmQuotaWatchdog(true);
            turnSucceededTools += 1;
            // Any successful tool call resets the identical-failure streak so exploratory debugging never trips the breaker
            identicalFailureStreak = 0;
            lastIdenticalFailureKey = "";
          } else if (status === "failed") {
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
        const p = params as unknown as {
          _meta?: { is_mcp_tool_approval?: boolean };
          toolCall?: {
            toolCallId?: string; title?: string; name?: string; kind?: string;
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
        const options = p.options ?? [];

        const allowOpt = options.find((o) => o.kind === "allow_once");
        const denyOpt = options.find((o) => o.kind === "reject_once") ?? options.find((o) => o.kind === "reject_always");
        const claude = p.toolCall?._meta?.claudeCode;
        const claudeCall = this.profile.adapter === "claude" && toolKind === "other" &&
          claude?.mcpServer?.name === "harness" && claude.mcpServer.source === "dynamic" &&
          claude.toolName !== undefined && claudeToolNames.has(claude.toolName);
        const codexCall = this.profile.adapter === "codex" && toolKind === "execute" &&
          p._meta?.is_mcp_tool_approval === true && callId !== undefined && codexHarnessCalls.has(callId);
        const geminiMcp = p.toolCall?._meta?.["gemini-cli"]?.mcp;
        const geminiCall = this.profile.adapter === "gemini" && geminiMcp?.server_name === "harness" &&
          geminiMcp.tool_name !== undefined && harnessToolNames.has(geminiMcp.tool_name);
        const genericCall = this.profile.adapter === "generic" && toolKind === "other" &&
          p.toolCall?.name !== undefined && harnessToolNames.has(p.toolCall.name);
        const isRead = toolKind === "read" || toolKind === "search";
        const isWrite = toolKind === "edit" || toolKind === "delete" || toolKind === "move";
        let workspaceCall = false;
        // Native commands may run inside the CLI sandbox. An opaque command approval
        // can request elevation and supplies no checkable set of filesystem targets.
        if (!controlOnly && !claude?.mcpServer && !p._meta?.is_mcp_tool_approval && !geminiMcp &&
            (isRead || (canWrite && isWrite))) {
          try {
            const locations = p.toolCall?.locations ?? [];
            for (const location of locations) {
              await workspaceTarget(workspace, location.path, toolKind === "edit" || toolKind === "move");
            }
            workspaceCall = locations.length > 0;
          } catch {
            workspaceCall = false;
          }
        }
        if (!sessionId || params.sessionId !== sessionId || !(workspaceCall || (hasHarnessBridge && (claudeCall || codexCall || geminiCall || genericCall)))) {
          const deniedLabel = title || toolKind || "native tool";
          turnDeniedTools.push(deniedLabel);
          allToolFailures.push(`${deniedLabel} (blocked by ${req.toolPolicy})`);
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
        if (!sessionId || params.sessionId !== sessionId || controlOnly) {
          throw new Error("Read denied: agent is running in control-plane-only mode");
        }
        const p = params as unknown as { path: string; line?: number; limit?: number };
        const file = await openWorkspaceFile(workspace, p.path, false);
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
        if (!sessionId || params.sessionId !== sessionId || !canWrite) {
          throw new Error(`Write denied: agent is running in ${controlOnly ? "control-plane-only" : "strictly read-only"} mode`);
        }
        const p = params as unknown as { path: string; content: string };
        const file = await openWorkspaceFile(workspace, p.path, true);
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
      child?.kill("SIGTERM");
    };
    req.signal.addEventListener("abort", onAbort, { once: true });

    try {
      const initRes = (await conn.agent.request("initialize", {
        protocolVersion: PROTOCOL_VERSION,
        clientInfo: { name: "pi-harness", version: "0.1.0" },
        clientCapabilities: {
          fs: { readTextFile: !controlOnly, writeTextFile: canWrite },
          ...(this.profile.adapter === "claude" ? {} : {
            _meta: { jetbrains: { air: { capabilities: ["sessionFailure"] } } },
          }),
        },
      } as never)) as unknown as Record<string, unknown>;

      const initMeta = (initRes._meta ?? {}) as Record<string, unknown>;
      const steeringMeta = initMeta.steering as { supported?: boolean } | undefined;
      steeringSupported = Boolean(steeringMeta?.supported);

      // Wire Harness MCP server via stdio when daemonSocketPath + bridge are configured
      const mcpServers: AcpMcpServerEntry[] = [];
      if (req.daemonSocketPath && req.mcpBridgeEntrypoint) {
        mcpServers.push({
          name: "harness",
          command: process.execPath,
          args: [req.mcpBridgeEntrypoint],
          env: [
            { name: "PI_HARNESS_SOCKET", value: req.daemonSocketPath },
            { name: "PI_HARNESS_RUN_ID", value: req.runId },
            { name: "PI_HARNESS_TASK_ID", value: req.taskId },
          ],
        });
      }

      const { params: newSessionParams, needsTurn1SystemPrompt } = buildNewSessionParams(this.profile.adapter, {
        cwd: req.cwd,
        systemPrompt: req.systemPrompt,
        toolPolicy: req.toolPolicy,
        model: req.model,
        effort: req.effort,
        mcpServers,
        agentCapabilities: (initRes.agentCapabilities ?? {}) as Record<string, unknown>,
      });

      const newSessionRes = (await conn.agent.request(
        "session/new",
        newSessionParams as never,
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
          if (action.requiredModel) throw new Error(`Could not select model ${action.requiredModel}: ${err}`);
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
          if (current !== req.model) {
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
        cancel: async () => {
          if (sessionId) {
            await conn.agent.notify("session/cancel", { sessionId }).catch(() => {});
          }
        },
      });

      const initialPromptText =
        prepared.prependSystemPromptToTurn1 || needsTurn1SystemPrompt
          ? `${req.systemPrompt}\n\n---\n\n${req.initialPrompt}`
          : req.initialPrompt;

      let nextPromptText = initialPromptText;
      let promptRes: Record<string, unknown> = {};
      let rawStop = "end_turn";
      let airFailureCode = "";
      let consecutiveSilentRecoveries = 0;
      let breakerExplainedOnce = false;

      for (let recoveryAttempt = 0; recoveryAttempt <= MAX_TOTAL_RECOVERY_CONTINUATIONS; recoveryAttempt++) {
        turnFailedTools = [];
        turnDeniedTools = [];
        turnSucceededTools = 0;
        textSinceLastTool = "";

        promptRes = (await conn.agent.request("session/prompt", {
          sessionId,
          prompt: [{ type: "text", text: nextPromptText }],
        } as never)) as unknown as Record<string, unknown>;

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

        // Merge PromptResponse.usage if available
        const promptUsage = promptRes.usage as Record<string, unknown> | undefined;
        if (promptUsage) {
          lastUsage = {
            used: lastUsage?.used ?? Number(promptUsage.totalTokens ?? 0),
            size: lastUsage?.size ?? 0,
            inputTokens: Number(promptUsage.inputTokens ?? 0),
            outputTokens: Number(promptUsage.outputTokens ?? 0),
            cachedReadTokens: promptUsage.cachedReadTokens == null ? undefined : Number(promptUsage.cachedReadTokens),
            cachedWriteTokens: promptUsage.cachedWriteTokens == null ? undefined : Number(promptUsage.cachedWriteTokens),
            thoughtTokens: promptUsage.thoughtTokens == null ? undefined : Number(promptUsage.thoughtTokens),
            cost: lastUsage?.cost,
          };
        }
        const turnModels = (promptMeta.quota as { model_usage?: unknown[] } | undefined)?.model_usage;
        flushCost(Array.isArray(turnModels) ? turnModels.length : 0);

        rawStop = String(promptRes.stopReason ?? "end_turn");

        // Never continue if rollover, quota watchdog, user abort, or quota failure occurred
        if (
          rolloverTriggered ||
          quotaWatchdogTriggered ||
          req.signal.aborted ||
          ["quota_exhausted", "rate_limited", "budget_exhausted"].includes(airFailureCode)
        ) {
          break;
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
            ...turnDeniedTools.map((d) => `- Tool "${d}" was blocked because this session is strictly read-only`),
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
        break;
      }

      if (quotaWatchdogTriggered || ["quota_exhausted", "rate_limited", "budget_exhausted"].includes(airFailureCode)) {
        const isHard = lastQuotaHard || airFailureCode === "quota_exhausted" || airFailureCode === "budget_exhausted";
        const reason =
          lastQuotaReason ||
          (airFailureCode ? `ACP sessionFailure: ${airFailureCode}` : `Rate-limit retry timed out after ${Math.round(quotaTimeoutMs / 1000)}s`);
        req.callbacks.onPassiveQuota?.({
          profileId: this.profile.id,
          model: req.model,
          raw: {
            sessionFailure: isHard ? "quota_exhausted" : "rate_limited",
            ...(lastQuotaResetAt ? { resetAt: lastQuotaResetAt } : {}),
          },
          observedAt: Date.now(),
        });
        const quotaPause: QuotaPauseInfo = {
          reason,
          resetAt: lastQuotaResetAt,
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
      };
    } catch (err) {
      if (rolloverTriggered) {
        return { text: accumulatedText, stopReason: "rollover_requested", usage: lastUsage, sessionId };
      }
      const primary = spawnErr ?? err;
      const errMsg = primary instanceof Error ? primary.message : String(primary);
      // Harvest structured `rateLimit` info from ACP JSON-RPC error data (e.g. Claude `turnErrorData` when refused before first usage)
      const errData = (primary && typeof primary === "object" ? (primary as { data?: Record<string, unknown> }).data : undefined);
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
      const detected = detectQuotaErrorText(combined);

      if (quotaWatchdogTriggered || detected.isQuota || errData?.errorKind === "rate_limit" || Boolean(errRateLimit)) {
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

      const detail = stderrBuf.trim() ? ` (stderr: ${stderrBuf.trim().slice(-500)})` : "";
      throw new Error(`ACP runner (${this.profile.id}) failed: ${errMsg}${detail}`);
    } finally {
      flushCost();
      disarmQuotaWatchdog(false);
      req.signal.removeEventListener("abort", onAbort);
      try {
        await Promise.resolve(conn.close()).catch(() => {});
      } catch {
        // ignore close error on already-exited pipe
      }
      if (child && !child.killed) {
        try {
          child.kill("SIGTERM");
        } catch {
          // ignore
        }
      }
      prepared.cleanup();
    }
  }

  /**
   * Probes an ACP CLI out-of-band to discover its currently runnable models & effort options
   * for the automated Model Catalog compiler.
   */
  async probeModels(cwd: string, timeoutMs = 15_000): Promise<ProbedModelInfo[]> {
    if (this.profile.models && this.profile.models.length > 0) {
      return this.profile.models.map((wireModelId) => ({ wireModelId }));
    }

    const child = spawn(this.profile.command, this.profile.args ?? [], {
      cwd,
      env: { ...process.env, ...(this.profile.env ?? {}) },
      stdio: ["pipe", "pipe", "pipe"],
    });
    child.on("error", () => {});
    child.stdin?.on("error", () => {});
    child.stdout?.on("error", () => {});
    child.stderr?.on("error", () => {});

    const app = client();
    const conn = app.connect(
      ndJsonStream(
        Writable.toWeb(child.stdin!) as unknown as WritableStream<Uint8Array>,
        Readable.toWeb(child.stdout!) as unknown as ReadableStream<Uint8Array>,
      ),
    );

    const timer = setTimeout(() => {
      try {
        void Promise.resolve(conn.close()).catch(() => {});
      } catch {
        // ignore
      }
      try {
        child.kill("SIGKILL");
      } catch {
        // ignore
      }
    }, timeoutMs);

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

      const modelOpt = configOptions.find((c) => c.category === "model" || c.id === "model");
      for (const opt of selectValues(modelOpt) ?? []) {
        results.push({ wireModelId: opt.value, name: opt.name, supportedEfforts });
      }

      // Legacy fallback (gemini --acp `models.availableModels`)
      const legacyModels = (res.models as { availableModels?: Array<{ modelId: string; name?: string }> } | undefined)
        ?.availableModels;
      if (results.length === 0 && legacyModels) {
        for (const m of legacyModels) {
          results.push({ wireModelId: m.modelId, name: m.name });
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
      if (!child.killed) {
        try {
          child.kill("SIGTERM");
        } catch {
          // ignore
        }
      }
    }
  }
}
