import type { ExternalMcpServerConfig } from "../config.js";
import type { AgentTier, ArtifactRecord, TaskKind } from "../control-plane/db.js";
import type { ToolDisplayPayload } from "../protocol/tool-display.js";

export type ToolPolicy = "control_plane_only" | "read_only_workspace" | "read_write_workspace";

export function toolPolicyForTask(tier: AgentTier, kind: TaskKind, readOnly = false): ToolPolicy {
  if (tier === 1) return "read_only_workspace";
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
  onTurn?: (turnsUsed: number) => void;
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
  /**
   * Advisory progress-reminder threshold (tool-call turns). When reached the runner sends ONE
   * progress reminder and keeps working; 0 disables the reminder. Never stops the run.
   */
  maxTurns: number;
  rolloverThreshold?: number;
  /** Soft timeout (ms) allowing native 429/rate-limit retries across subscription pools before pausing early. */
  quotaRetryTimeoutMs?: number;
  /** Stream/turn inactivity watchdog timeout in ms (defaults to 600_000 ms for unattended safety). */
  idleTimeoutMs?: number;
  /** ACP `initialize`/`session/new` startup timeout in ms (defaults to 45_000 ms). */
  startupTimeoutMs?: number;
  /** Fallback context window size (tokens) when an adapter omits `usage_update.size`. */
  contextWindow?: number;
  /** Role-scoped harness tools exposed via in-process callback (Pi) or MCP bridge (ACP) */
  tools: ToolDefinition[];
  executeTool: (name: string, args: Record<string, unknown>) => Promise<unknown>;
  /** Task generation this request runs; carried in the MCP bridge's task ID so the daemon rejects a stale runner's tool calls. */
  taskGeneration?: number;
  /** Daemon Unix socket path + auth token for ACP stdio MCP bridge subprocesses */
  daemonSocketPath?: string;
  mcpBridgeEntrypoint?: string;
  /** Optional external stdio MCP servers forwarded alongside the harness bridge (e.g. octofs, ast-grep). */
  externalMcpServers?: ExternalMcpServerConfig[];
  /** Extra directories prepended to PATH for shell-enabled ACP agents (bundled ast-grep / octofs). */
  extraPathDirs?: string[];
  /** Additional allowed read-only directories outside `cwd` (e.g. main `.harness/goals`, `.harness/artifacts`, `extraReadRoots`). */
  extraReadRoots?: string[];
  /** Additional allowed writable directories outside `cwd` (e.g. main `.harness/goals`, `extraWriteRoots`). */
  extraWriteRoots?: string[];
  /** Extra environment variables for ACP agent subprocesses (e.g. `HARNESS_GOAL_EVIDENCE_DIR`); never overrides PATH. */
  extraEnv?: Record<string, string>;
  /** Permit shell execution (`Bash`/`shell_command`/`execute`) in `read_only_workspace` (used for verify/Done-Review in isolated checkouts). */
  allowShellInReadOnly?: boolean;
  signal: AbortSignal;
  callbacks: RunnerCallbacks;
  onHandle?: (handle: RunnerExecutionHandle) => void;
  /** True once the task has settled (return_task, report_issue, cancel) or its generation moved on. */
  isTaskSettled?: () => boolean;
  /**
   * Sent once as a bounded wrap-up turn when the agent ends its turn naturally while the task is still unsettled
   * (no return_task). Absent means no wrap-up turn (Tier 1 finishes by ending its turn).
   */
  missingReturnWrapUpPrompt?: string;
  /**
   * Takes steering messages queued for this task while the agent could not receive them natively. Runners call it
   * when a turn ends naturally; a non-empty result opens the next turn with that text instead of ending the run.
   */
  drainPendingSteers?: () => string[];
}

/** Steering continuation turns the orchestrator reserves per task generation (drains are never refused). */
export const MAX_STEER_CONTINUATIONS = 10;
/** Runner-side loop guard on turns opened only to deliver queued steering. */
export const MAX_RUNNER_STEER_CONTINUATIONS = 32;

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
  // Turn counts are advisory reminders, never stops, so there is no turn-limit stop reason.
  stopReason: "completed" | "cancelled" | "rollover_requested" | "quota_paused" | "error";
  usage?: RunnerUsage;
  sessionId?: string;
  quotaPause?: QuotaPauseInfo;
  toolFailures?: string[];
  /** True when the runner sent the missing-return wrap-up turn during this run. */
  wrapUpAttempted?: boolean;
}

export interface ProbedModelInfo {
  wireModelId: string;
  name?: string;
  description?: string;
  explicitModelId?: string;
  supportedEfforts?: string[];
}

/**
 * Text of the single advisory progress reminder a runner sends when a task reaches its maxTurns
 * threshold. Chosen by tier (and reviewer role), never by inspecting req.tools: code mode hides
 * return_task behind code_exec.
 */
export function turnReminderText(tier: AgentTier, isReviewer = false): string {
  const prefix =
    "Progress check: you have reached this task's progress-reminder threshold of tool-call turns. " +
    "This is advisory only: the harness will not stop you and nothing has been cancelled. Keep working while the work is productive.";
  if (isReviewer) {
    return `${prefix} When you have evidence for every required criterion, call submit_review_verdict (or return the requested JSON verdict).`;
  }
  if (tier === 1) {
    return `${prefix} Check that delegated work is still progressing with wait_children, and if your context is getting heavy call request_rollover with an anchored handoff. You finish by ending your turn once the goal is integrated and verified.`;
  }
  if (tier === 2) {
    return `${prefix} When your slice is ready: commit, submit_for_merge if you changed the repo, then return_task. If your context is getting heavy, commit clean WIP and call request_rollover. If the slice is larger than assigned, return_task with scope_too_large and evidence.`;
  }
  return `${prefix} When your result is ready, hand it back with return_task. If the assignment is larger than expected, return_task with scope_too_large, evidence paths and suggested splits instead of cutting the work short.`;
}
