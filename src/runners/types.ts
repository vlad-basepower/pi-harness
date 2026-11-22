import type { AgentTier, ArtifactRecord, TaskKind } from "../control-plane/db.js";
import type { ToolDisplayPayload } from "../protocol/tool-display.js";

export type ToolPolicy = "control_plane_only" | "read_only_workspace" | "read_write_workspace";

export function toolPolicyForTask(tier: AgentTier, kind: TaskKind, readOnly = false): ToolPolicy {
  if (tier === 1) return "control_plane_only";
  if (!readOnly && (tier === 2 || (tier === 3 && kind === "execute"))) return "read_write_workspace";
  return "read_only_workspace";
}

/** A charge exactly as the provider or adapter reported it; never estimated from token prices. */
export interface RunnerCost {
  amount: number;
  /** ISO 4217 code or unit name as reported (e.g. "USD", "credits"). */
  currency: string;
  /** `session_cumulative`: running total for the external session. `delta`: one call's charge. */
  basis: "session_cumulative" | "delta";
  /** Actual model reported with the charge; omitted when the source does not say. */
  model?: string;
  /** Stable id of a `delta` charge so redelivery counts once. */
  chargeId?: string;
}

export interface RunnerUsage {
  used: number;
  size: number;
  inputTokens?: number;
  outputTokens?: number;
  cachedReadTokens?: number;
  cachedWriteTokens?: number;
  thoughtTokens?: number;
  cost?: RunnerCost;
}

export interface PassiveQuotaSignal {
  profileId: string;
  model?: string | null;
  raw: Record<string, unknown>;
  observedAt: number;
}

export interface QuotaRetryStateEvent {
  state: "retrying" | "recovered";
  retryCount: number;
  timeoutMs: number;
  reason?: string;
  resetAt?: string | null;
}

export interface QuotaPauseInfo {
  reason: string;
  resetAt?: string | null;
  retryCount: number;
  watchdogTimedOut: boolean;
  isHardExhaustion: boolean;
}

export interface RunnerCallbacks {
  onSessionId?: (sessionId: string) => void;
  onTextDelta?: (text: string) => void;
  onThoughtDelta?: (text: string) => void;
  onUsage?: (usage: RunnerUsage) => void;
  onArtifact?: (artifact: Omit<ArtifactRecord, "id" | "runId" | "taskId" | "createdAt">) => void;
  onPassiveQuota?: (signal: PassiveQuotaSignal) => void;
  onQuotaRetryState?: (ev: QuotaRetryStateEvent) => void;
  onToolEvent?: (ev: {
    id?: string;
    name: string;
    status: string;
    kind?: string;
    title?: string;
    args?: string;
    result?: string;
    error?: string;
    via?: "code_exec";
    /** Structured rich output (contract/tool-display-v1); events without it render as a compact tool pill. */
    display?: ToolDisplayPayload;
  }) => void;
}

export interface RunnerExecutionHandle {
  steer(message: string): Promise<boolean>;
  cancel(reason?: string): Promise<void>;
}

export interface ToolDefinition {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
}

export interface RunAgentRequest {
  runId: string;
  taskId: string;
  tier: AgentTier;
  kind: TaskKind;
  cwd: string;
  toolPolicy: ToolPolicy;
  systemPrompt: string;
  initialPrompt: string;
  model: string;
  effort?: string | null;
  maxTurns: number;
  rolloverThreshold?: number;
  /** Soft timeout (ms) allowing native 429/rate-limit retries across subscription pools before pausing early. */
  quotaRetryTimeoutMs?: number;
  /** Role-scoped harness tools exposed via in-process callback (Pi) or MCP bridge (ACP) */
  tools: ToolDefinition[];
  executeTool: (name: string, args: Record<string, unknown>) => Promise<unknown>;
  /** Daemon Unix socket path + auth token for ACP stdio MCP bridge subprocesses */
  daemonSocketPath?: string;
  mcpBridgeEntrypoint?: string;
  signal: AbortSignal;
  callbacks: RunnerCallbacks;
  onHandle?: (handle: RunnerExecutionHandle) => void;
}

/** Reject a stale or overprivileged launch request before contacting the provider. */
export function validateRunPolicy(req: RunAgentRequest): void {
  if (typeof req.model !== "string" || !req.model.trim()) {
    throw new Error("A nonblank profile-native model ID is required");
  }
  const maximum = toolPolicyForTask(req.tier, req.kind);
  if (req.toolPolicy !== maximum && req.toolPolicy !== "control_plane_only" &&
      !(req.toolPolicy === "read_only_workspace" && maximum === "read_write_workspace")) {
    throw new Error(`Invalid tool policy ${req.toolPolicy} for tier ${req.tier} ${req.kind}`);
  }
}

export interface RunAgentResult {
  text: string;
  stopReason: "completed" | "cancelled" | "rollover_requested" | "quota_paused" | "max_turns" | "error";
  usage?: RunnerUsage;
  sessionId?: string;
  quotaPause?: QuotaPauseInfo;
  toolFailures?: string[];
}

export interface ProbedModelInfo {
  wireModelId: string;
  name?: string;
  supportedEfforts?: string[];
}
