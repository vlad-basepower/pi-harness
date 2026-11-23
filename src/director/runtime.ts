import { randomUUID } from "node:crypto";
import { closeSync, constants, existsSync, fstatSync, mkdirSync, openSync, readSync, realpathSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import {
  compareSemanticVersions,
  parseClaudeFamilyVersion,
  recommendModelsByTaskKind,
  type ModelCatalogCompiler,
} from "../catalog/compiler.js";
import { vendorSuccessors } from "../catalog/lifecycle.js";
import { createCodeModeExecutor, effectiveCodeMode } from "../code-mode/index.js";
import type { HarnessConfig } from "../config.js";
import { resolveLaunchRoute } from "../control-plane/launch-route.js";
import { compactTaskHandoff, toolsForTier } from "../control-plane/tools.js";
import { writeCodeOutput, writeTaskDossier } from "../control-plane/dossiers.js";
import type { AdmissionLane, QueuePositionInput } from "../control-plane/admission-queue.js";
import type { ControlPlaneDb } from "../control-plane/db.js";
import type { CustomRunnerFactory, HarnessOrchestrator } from "../control-plane/orchestrator.js";
import { buildRunActivitySummary } from "../control-plane/telemetry.js";
import { recordCostInvocation, summarizeCost } from "../cost/ledger.js";
import type { TimerRecord } from "../control-plane/timers.js";
import {
  GoalEngine,
  shouldRunIndependentReview,
  type CriterionEvaluation,
  type GoalCriterion,
  type GoalRecord,
  type GoalReviewVerdict,
  type VerificationMode,
} from "../goals/engine.js";
import {
  formatChatEventForModel,
  formatChatEventSummary,
  type ChatControlEvent,
} from "../protocol/chat-events.js";
import {
  buildGoalDetailDisplay,
  buildGoalsTableDisplay,
  buildRunActivityDisplay,
  codeExecEventSink,
  summarizeToolDisplay,
  type ToolDisplayPayload,
} from "../protocol/tool-display.js";
import type { QuotaClient } from "../quota/client.js";
import { AcpRunner } from "../runners/acp-runner.js";
import { PiRunner } from "../runners/pi-runner.js";
import type { RunnerCallbacks, RunnerExecutionHandle, ToolDefinition } from "../runners/types.js";
import { nativeToolsPathDirs, resolveExternalMcpServers } from "../tools/native-tools.js";
import { canonicalPath, runGit } from "../worktree/manager.js";
import {
  buildDirectorSystemPrompt,
  DirectorSessionStore,
  type ChatSessionRecord,
  type ChatTurnRecord,
} from "./session.js";

const DIRECTOR_TOOLS: ToolDefinition[] = [
  {
    name: "goal_create",
    description:
      "Draft a structured, versioned Goal Contract with concrete deterministic check commands and behavioral/architectural acceptance criteria.",
    inputSchema: {
      type: "object",
      required: ["title", "description", "criteria"],
      properties: {
        title: { type: "string" },
        description: { type: "string" },
        criteria: {
          type: "array",
          items: {
            type: "object",
            required: ["description", "kind"],
            properties: {
              id: { type: "string" },
              description: { type: "string" },
              kind: { type: "string", enum: ["deterministic", "behavioral", "architectural"] },
              command: { type: "string" },
              required: { type: "boolean" },
            },
          },
        },
        non_goals: { type: "array", items: { type: "string" } },
        max_review_cycles: { type: "number" },
        verification_mode: {
          type: "string",
          enum: ["auto", "none", "deterministic", "independent_review"],
          description:
            "Verification depth chosen by the agent based on risk (default 'auto': runs deterministic checks and only spawns an independent LLM reviewer if required behavioral/architectural criteria exist).",
        },
        commit_immediately: { type: "boolean", description: "Start autonomous Tier 1 execution with the selected model" },
        model: { type: "string", minLength: 1, description: "Required for immediate commit. Select from list_models." },
        profile_id: { type: "string" },
        effort: { type: "string" },
      },
    },
  },
  {
    name: "goal_commit",
    description:
      "Commit a draft Goal Contract and launch its autonomous Tier 1 Run-Until-Done Orchestrator. If maxConcurrentGoals is full (and force_start is not set), enqueues the goal cleanly in the unified admission queue at `position` (defaults to back) and starts it automatically as soon as capacity opens.",
    inputSchema: {
      type: "object",
      required: ["goal_id", "model"],
      properties: {
        goal_id: { type: "string" },
        profile_id: { type: "string" },
        model: { type: "string" },
        effort: { type: "string" },
        position: { description: "Optional queue position when at maxConcurrentGoals: 1-based integer (1 = front), 'front', or 'back'." },
        force_start: {
          type: "boolean",
          description: "Emergency override: start immediately bypassing maxConcurrentGoals. Do NOT abuse — use only for urgent work or explicit user requests.",
        },
      },
    },
  },
  {
    name: "queue_list",
    description: "Inspect the unified Goal & Worker admission queue (queued items, 1-based positions, active counts, and live concurrency limits).",
    inputSchema: {
      type: "object",
      properties: {
        lane: { type: "string", enum: ["goal", "worker"], description: "Optional lane filter ('goal' or 'worker')." },
      },
    },
  },
  {
    name: "queue_reorder",
    description: "Move a queued goal or worker to any 1-based position in the unified admission queue ('front', 'back', or integer >= 1).",
    inputSchema: {
      type: "object",
      required: ["target_id", "position"],
      properties: {
        lane: { type: "string", enum: ["goal", "worker"], description: "Queue lane (defaults to 'goal' for goal-* IDs, 'worker' for t-* IDs)." },
        target_id: { type: "string", description: "Queued goal ID (e.g. goal-3) or worker task ID (e.g. t-4)." },
        position: { description: "Target position: 1-based integer (1 = next up), 'front', or 'back'." },
      },
    },
  },
  {
    name: "queue_force_start",
    description:
      "Emergency override: force-start a queued goal or worker immediately bypassing maxConcurrentGoals/maxConcurrentWorkers (and optionally bypassing a quota pause when bypass_quota=true). WARNING: Do NOT abuse — use only for urgent high-priority work or explicit user requests.",
    inputSchema: {
      type: "object",
      required: ["target_id"],
      properties: {
        lane: { type: "string", enum: ["goal", "worker"], description: "Queue lane (auto-detected from goal-* vs t-* ID if omitted)." },
        target_id: { type: "string", description: "Goal ID (e.g. goal-3) or worker task ID (e.g. t-4)." },
        bypass_quota: { type: "boolean", description: "If true, also force-resume a quota-paused goal/worker despite quota exhaustion (use sparingly!)." },
        reason: { type: "string", description: "Brief justification for bypassing the concurrency or quota limit." },
      },
    },
  },
  {
    name: "goal_accept",
    description:
      "Explicit Director acceptance/override for a Goal based on verified evidence (e.g. when a review was obstructed or human approved). Requires a clear reason and concrete evidence_paths; recorded permanently in goal review history and auto-resolves open escalations.",
    inputSchema: {
      type: "object",
      required: ["goal_id", "reason", "evidence_paths"],
      properties: {
        goal_id: { type: "string" },
        reason: { type: "string", description: "Explicit rationale for accepting the goal." },
        evidence_paths: { type: "array", items: { type: "string" }, description: "Concrete file/artifact paths proving completion." },
      },
    },
  },
  {
    name: "steer_task",
    description: "Deliver a live steering message to any running Tier 1/2/3 task across active Goal runs in this workspace.",
    inputSchema: {
      type: "object",
      required: ["task_id", "message"],
      properties: {
        task_id: { type: "string" },
        message: { type: "string" },
      },
    },
  },
  {
    name: "write_note",
    description:
      "Logged Director note/report writer scoped strictly to `.harness/goals/`, `.harness/notes/`, or configured `extraWriteRoots` (never writes tracked repository source code).",
    inputSchema: {
      type: "object",
      required: ["path", "content", "reason"],
      properties: {
        path: { type: "string", description: "Destination file under .harness/goals/, .harness/notes/, or extraWriteRoots." },
        content: { type: "string", description: "Markdown/text content to write." },
        reason: { type: "string", description: "Concise logged reason for writing this note." },
      },
    },
  },
  {
    name: "goal_amend",
    description:
      "Dynamically amend an active or draft Goal Contract (vN -> vN+1), invalidating stale reviews and notifying the running Tier 1 Orchestrator with the exact delta.",
    inputSchema: {
      type: "object",
      required: ["goal_id", "rationale"],
      properties: {
        goal_id: { type: "string" },
        rationale: { type: "string" },
        title: { type: "string" },
        description: { type: "string" },
        add_criteria: { type: "array", items: { type: "object" } },
        update_criteria: { type: "array", items: { type: "object" } },
        remove_criteria_ids: { type: "array", items: { type: "string" } },
        non_goals: { type: "array", items: { type: "string" } },
      },
    },
  },
  {
    name: "goal_list",
    description:
      "List Goals in this workspace (or across all projects when all_projects is true) with their version, status, review cycles, and linked run IDs. Pass the returned presentation_id to present_result to show the Goals table.",
    inputSchema: {
      type: "object",
      properties: {
        all_projects: { type: "boolean", description: "When true, list goals across all registered projects." },
      },
    },
  },
  {
    name: "project_list",
    description: "List all registered projects on this daemon host with their paths, git status/branch, and active session/goal counts.",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "project_switch",
    description: "Rebind this chat session to another registered project (by project ID, name, or path), preserving conversation history while switching workspace cwd.",
    inputSchema: {
      type: "object",
      properties: {
        project: { type: "string", description: "Project ID (e.g. proj-1), name, or path on the daemon host." },
      },
      required: ["project"],
    },
  },
  {
    name: "goal_get",
    description:
      "Inspect a Goal's criteria, latest review, linked run tree, and open escalations. Pass the returned presentation_id to present_result to show the Goal detail card.",
    inputSchema: {
      type: "object",
      required: ["goal_id"],
      properties: { goal_id: { type: "string" } },
    },
  },
  {
    name: "present_result",
    description:
      "Show one native card or table in the user's chat using a presentation_id from a result in this turn. Select the desired result from a batch and pass its ID directly. The original operation is already complete, and each ID can be used once.",
    inputSchema: {
      type: "object",
      required: ["presentation_id"],
      properties: { presentation_id: { type: "string" } },
    },
  },
  {
    name: "report_goal_progress",
    description:
      "Record live progress, verification status, or checked criterion results on a Goal so the Goals table and detail cards dynamically reflect actual work.",
    inputSchema: {
      type: "object",
      required: ["goal_id", "summary"],
      properties: {
        goal_id: { type: "string" },
        summary: { type: "string" },
        phase: { type: "string" },
        verification_status: { type: "string", enum: ["not_needed", "in_progress", "passed", "failed"] },
        criteria_results: {
          type: "array",
          items: {
            type: "object",
            required: ["criterion_id", "passed", "evidence"],
            properties: {
              criterion_id: { type: "string" },
              passed: { type: "boolean" },
              evidence: { type: "string" },
              remediation: { type: "string" },
            },
          },
        },
      },
    },
  },
  {
    name: "goal_wait",
    description:
      "Event-driven wait for a Goal's linked autonomous run to settle (or raise a P0 escalation) without token polling. Only call when the user explicitly asks to block until completion — normally end your turn after goal_commit so the user stays unblocked and the daemon wakes you automatically. Any new user message immediately unblocks this wait without stopping background subagents.",
    inputSchema: {
      type: "object",
      required: ["goal_id"],
      properties: {
        goal_id: { type: "string" },
        fallback_interval_s: {
          type: "number",
          description: "Optional fallback wakeup interval in seconds in case the run dies silently (default: config.events.fallbackWakeupIntervalSec, 600s).",
        },
      },
    },
  },
  {
    name: "goal_trigger_review",
    description:
      "Verify and settle a Goal at the chosen verification depth (defaults to the Goal's verification_mode: 'auto' runs deterministic checks and only spawns an independent LLM reviewer when required behavioral/architectural criteria exist; 'deterministic' runs checks only; 'none' approves directly).",
    inputSchema: {
      type: "object",
      required: ["goal_id"],
      properties: {
        goal_id: { type: "string" },
        verification_mode: { type: "string", enum: ["auto", "none", "deterministic", "independent_review"] },
        summary: { type: "string", description: "Optional concise completion summary when verification_mode is 'none'." },
      },
    },
  },
  {
    name: "goal_pause",
    description: "Pause an active Goal and stop its in-flight execution cleanly without discarding artifacts.",
    inputSchema: {
      type: "object",
      required: ["goal_id"],
      properties: { goal_id: { type: "string" } },
    },
  },
  {
    name: "goal_cancel",
    description: "Cancel a Goal permanently and stop its linked run.",
    inputSchema: {
      type: "object",
      required: ["goal_id"],
      properties: { goal_id: { type: "string" } },
    },
  },
  ...toolsForTier(1).filter(tool => ["spawn_task", "wait_tasks", "wait_children", "cancel_task", "artifact_read"].includes(tool.name)),
  {
    name: "read_file",
    description: "Read up to 4096 bytes of workspace text as a logged exception. Delegate normal inspection to spawn_task.",
    inputSchema: {
      type: "object",
      required: ["path", "exception_reason"],
      properties: { path: { type: "string" }, exception_reason: { type: "string", minLength: 1 }, max_chars: { type: "number" } },
    },
  },
  {
    name: "kb_search",
    description: "Search the workspace Knowledge Base for architectural invariants, decisions, and gotchas.",
    inputSchema: {
      type: "object",
      properties: { query: { type: "string" }, path_globs: { type: "array", items: { type: "string" } } },
    },
  },
  {
    name: "list_models",
    description:
      "Inspect enabled latest-per-family models with coding rankings, cost/time/task, live subscription quota capacity, recommendedByKind, and any per-session model selection context.",
    inputSchema: { type: "object", properties: { exclude_exhausted: { type: "boolean" } } },
  },
  {
    name: "set_model_selection_context",
    description:
      "Set or clear concise per-session model selection context based on the user's preferences (e.g. using a specific model for a kind of task or utilizing a model family more). Empty by default; returned to users and spawned agents when listing models.",
    inputSchema: {
      type: "object",
      required: ["context"],
      properties: {
        context: {
          type: "string",
          description: "Concise per-session model routing guidance, or an empty string to clear.",
        },
      },
    },
  },
  {
    name: "run_status",
    description:
      "Inspect run and subagent telemetry. Pass the returned presentation_id to present_result to show the Run Activity card.",
    inputSchema: {
      type: "object",
      properties: {
        run_id: { type: "string", description: "Run ID to inspect." },
        goal_id: { type: "string", description: "Goal ID whose linked run should be inspected." },
      },
    },
  },
  {
    name: "steer_run",
    description: "Forward human steering guidance to an active Goal's Tier 1 Orchestrator.",
    inputSchema: {
      type: "object",
      required: ["goal_id", "message"],
      properties: { goal_id: { type: "string" }, message: { type: "string" } },
    },
  },
  {
    name: "resume_task",
    description:
      "Resume a failed, crashed, or stopped task in-place (preserving its worktree, WIP commits, artifacts, and deterministic recovery handoff) instead of spawning a new duplicate task.",
    inputSchema: {
      type: "object",
      required: ["task_id"],
      properties: {
        task_id: { type: "string", description: "Task ID to resume (e.g. t-2)." },
        note: { type: "string", description: "Optional guidance for the resumed task." },
        model: { type: "string", description: "Optional replacement model ID from list_models." },
        effort: { type: "string", description: "Optional reasoning effort override." },
        profile_id: { type: "string", description: "Optional ACP/Pi profile override." },
      },
    },
  },
  {
    name: "resume_run",
    description:
      "Resume a failed, crashed, stopped, or quota-paused run (or Goal's linked run) in-place, restarting interrupted tasks with their preserved worktrees/handoffs while keeping completed work untouched.",
    inputSchema: {
      type: "object",
      properties: {
        run_id: { type: "string", description: "Run ID to resume (e.g. run-1)." },
        goal_id: { type: "string", description: "Goal ID whose linked run should be resumed (e.g. goal-1)." },
        note: { type: "string", description: "Optional guidance for resumed tasks." },
      },
    },
  },
  {
    name: "schedule_timer",
    description:
      "Schedule a durable non-blocking timer (survives daemon restarts) to wake the Director after a delay or at an ISO timestamp (e.g. a model quota reset time), optionally pausing a Goal now and auto-resuming it when the timer fires. Work productively by default — use timers for self-pacing around usage/reset limits when instructed by the user or sessionModelContext, or for explicit reminders. End your turn after scheduling so the user stays unblocked.",
    inputSchema: {
      type: "object",
      required: ["reason"],
      properties: {
        delay_seconds: { type: "number", description: "Seconds from now until the timer fires." },
        wake_at: { type: "string", description: "ISO-8601 UTC timestamp to wake at (e.g. quota resetsAt)." },
        reason: { type: "string", description: "Concise description of what to resume or check when the timer fires." },
        goal_id: { type: "string", description: "Optional Goal ID (e.g. goal-1) associated with this timer." },
        pause_goal: {
          type: "boolean",
          description: "If true (with goal_id), cleanly pause the Goal now while waiting for the timer.",
        },
        resume_goal: {
          type: "boolean",
          description: "If true (with goal_id), automatically resume the Goal's run when the timer fires.",
        },
        wake_on_event: {
          type: "string",
          enum: ["none", "user_message", "goal_settled", "any"],
          description: "Optional early-wake/cancel trigger (default: 'none').",
        },
      },
    },
  },
  {
    name: "list_timers",
    description: "List pending or recent durable timers for this Director session.",
    inputSchema: {
      type: "object",
      properties: { status: { type: "string", enum: ["pending", "fired", "cancelled"] } },
    },
  },
  {
    name: "cancel_timer",
    description: "Cancel a pending durable timer by ID (e.g. timer-1).",
    inputSchema: {
      type: "object",
      required: ["timer_id"],
      properties: { timer_id: { type: "string" }, reason: { type: "string" } },
    },
  },
  {
    name: "remote_list",
    description: "List configured and connected remote peer hosts, their connection statuses, and queued outbound message counts.",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "session_list",
    description: "List active Director chat sessions across this host AND all connected remote peer hosts. Each entry includes a unified `address` (`local:chat-1` or `gpu-box:chat-4`) usable directly in `session_send` and `session_read`.",
    inputSchema: {
      type: "object",
      properties: {
        host: { type: "string", description: "Optional filter: 'local' or a specific remote host alias." },
      },
    },
  },
  {
    name: "session_create",
    description: "Create a new Director chat session on this host or a connected remote peer host (optionally bound to a project on that host).",
    inputSchema: {
      type: "object",
      properties: {
        host: { type: "string", description: "Target host ('local' or a connected remote host alias; defaults to local)." },
        project: { type: "string", description: "Optional project ID, name, or path on the target host." },
        title: { type: "string", description: "Optional session title." },
      },
    },
  },
  {
    name: "session_send",
    description: "Unified cross-session messaging primitive. Send a message to another local session (`chat-2` or `local:chat-2`) or a remote peer host session (`gpu-box:chat-4`). Delivers immediately and wakes the recipient if online, or queues durably in SQLite if the remote host is offline.",
    inputSchema: {
      type: "object",
      required: ["target", "message"],
      properties: {
        target: { type: "string", description: "Target session address: `chat-2`, `local:chat-2`, or `<remote-host>:<chat-id>`." },
        message: { type: "string", description: "Message body to deliver to the target session." },
        expect_reply: { type: "boolean", description: "Set to false when sending a final answer/update so the recipient does not ping-pong back." },
        reply_to_id: { type: "string", description: "Optional message ID being replied to." },
      },
    },
  },
  {
    name: "session_read",
    description: "Read recent transcript turns and peer message history from a local (`chat-2`) or remote (`gpu-box:chat-4`) session.",
    inputSchema: {
      type: "object",
      required: ["target"],
      properties: {
        target: { type: "string", description: "Target session address: `chat-2`, `local:chat-2`, or `<remote-host>:<chat-id>`." },
        limit: { type: "number", description: "Max recent turns to return (default 12, max 50)." },
      },
    },
  },
];

export interface DirectorPeerBridge {
  listRemotes(): unknown[];
  listSessions(host?: string): Promise<unknown[]>;
  createSession(input: { host?: string; project?: string; title?: string }): Promise<unknown>;
  sendMessage(input: {
    fromSessionId: string;
    target: string;
    body: string;
    expectReply?: boolean;
    replyToId?: string;
  }): Promise<unknown>;
  readSession(input: { target: string; limit?: number }): Promise<unknown>;
}

interface ChatSelection {
  repoRoot: string;
  projectId?: string | null;
  profileId?: string;
  model?: string | null;
  effort?: string | null;
  title?: string;
}

interface ActiveChat {
  abort: AbortController;
  settled: Promise<void>;
  partial: string;
  flushedText: string;
  handle?: RunnerExecutionHandle;
  executor: ReturnType<typeof createCodeModeExecutor>;
  onToolEvent: RunnerCallbacks["onToolEvent"];
  waitInterrupts: Set<(message: string, role: "user" | "system_notice") => void>;
}

/**
 * Tier 0 Interactive Director Runtime + Independent Goal Verification & Done-Review Coordinator.
 * Keeps conversational human UX completely decoupled from Tier 1 autonomous DAG rollovers.
 */
export class DirectorRuntime {
  readonly chats: DirectorSessionStore;
  readonly goals: GoalEngine;
  private activeChats = new Map<string, ActiveChat>();
  private activeReviews = new Map<string, AbortController>();
  private activeReviewerTools = new Map<
    string,
    { tools: ToolDefinition[]; execute: (name: string, args: Record<string, unknown>) => Promise<unknown> }
  >();
  private unsubRunEvents: (() => void) | null = null;
  private unsubGoalQueue: (() => void) | null = null;
  private disposed = false;
  private syncingQueuedProgress = false;
  private wakeHandler: ((chatSessionId: string, event: ChatControlEvent) => void) | null = null;
  private peerBridge: DirectorPeerBridge | null = null;
  private notifiedSettledGoals = new Set<string>();
  /** Goals between their launch decision and `linkRunAndStart`; they already hold a slot. */
  private launchingGoals = new Set<string>();
  private pendingDrainRepos = new Set<string>();
  private pendingDrainAll = false;

  constructor(
    private readonly db: ControlPlaneDb,
    private config: HarnessConfig,
    private catalog: ModelCatalogCompiler,
    private quota: QuotaClient,
    private readonly orchestrator: HarnessOrchestrator,
    private readonly customRunnerFactory?: CustomRunnerFactory,
  ) {
    this.chats = new DirectorSessionStore(db.db);
    this.goals = new GoalEngine(
      db.db,
      (id) => this.activeReviews.get(id)?.abort(new Error("Goal changed during review")),
      config.goals,
      () => {
        // Startup reconciliation frees slots held by settled runs; refresh queued progress and admit waiting goals.
        if (this.disposed) return;
        this.syncQueuedGoalProgress();
        void this.drainGoalQueue().catch(() => {});
      },
    );
    this.unsubRunEvents = this.db.onEvent((ev) => this.handleControlPlaneEvent(ev));
    this.unsubGoalQueue = this.db.admissionQueue.onChange((lane) => {
      if (!this.disposed && (!lane || lane === "goal")) this.syncQueuedGoalProgress();
    });
    this.db.timers.setOnFire((timer, wakeReason) => {
      void this.handleTimerFired(timer, wakeReason);
    });
  }

  setWakeHandler(handler: (chatSessionId: string, event: ChatControlEvent) => void): void {
    this.wakeHandler = handler;
  }

  setPeerBridge(bridge: DirectorPeerBridge): void {
    this.peerBridge = bridge;
  }

  hasActiveReviewer(taskId: string): boolean {
    return this.activeReviewerTools.has(taskId);
  }

  async executeReviewerMcpTool(taskId: string, name: string, args: Record<string, unknown>): Promise<unknown> {
    const entry = this.activeReviewerTools.get(taskId);
    if (!entry) throw new Error(`No active Done-Reviewer tool executor for ${taskId}`);
    if (name === "__list_tools") return entry.tools;
    return await entry.execute(name, args);
  }

  dispose(): void {
    this.disposed = true;
    this.unsubRunEvents?.();
    this.unsubRunEvents = null;
    this.unsubGoalQueue?.();
    this.unsubGoalQueue = null;
    for (const [chatId, active] of [...this.activeChats.entries()]) {
      active.abort.abort(new Error("Daemon shutting down"));
      void active.handle?.cancel("daemon_shutdown").catch(() => {});
      this.activeChats.delete(chatId);
    }
    for (const [goalId, abort] of [...this.activeReviews.entries()]) {
      abort.abort(new Error("Daemon shutting down"));
      this.activeReviews.delete(goalId);
    }
    this.activeReviewerTools.clear();
  }

  private async handleTimerFired(timer: TimerRecord, wakeReason: string): Promise<void> {
    if (timer.ownerKind === "task") {
      // Deliver scheduled timer wakeup to the owning Tier 1/2 agent
      const msg = `[SCHEDULED TIMER ${timer.id} FIRED (${wakeReason})]: ${timer.reason}`;
      await this.orchestrator.steerTask(timer.ownerId, msg).catch(() => {});
      return;
    }
    if (timer.ownerKind !== "chat") return;
    let resumedGoal = false;
    if (timer.resumeGoal && timer.goalId) {
      try {
        const g = this.goals.getGoal(timer.goalId);
        if (g && g.status === "paused") {
          const linkedRun = g.linkedRunId ? this.db.getRun(g.linkedRunId) : null;
          if (linkedRun?.status === "paused_quota") {
            await this.orchestrator.resumeRunFromQuota(linkedRun.id, { trigger: "user_timer" });
            this.goals.setStatus(g.id, "executing");
            resumedGoal = true;
          } else {
            await this.commitGoal(g.id);
            resumedGoal = true;
          }
        }
      } catch {
        resumedGoal = false;
      }
    }
    const event: ChatControlEvent = {
      kind: "timer.fired",
      timerId: timer.id,
      reason: timer.reason,
      wakeReason,
      ...(timer.goalId ? { goalId: timer.goalId } : {}),
      ...(resumedGoal ? { resumedGoal: true } : {}),
    };
    this.wakeHandler?.(timer.ownerId, event);
  }

  private handleControlPlaneEvent(ev: { runId: string; taskId?: string | null; type: string; payload: unknown }): void {
    // Wake any subagent task timers waiting early on a child settlement
    if (ev.type === "task.status" && ev.taskId) {
      const payload = ev.payload as { to?: string; status?: string } | null;
      const st = String(payload?.to ?? payload?.status ?? "");
      if (["completed", "failed", "cancelled", "blocked", "returned_for_rescope", "paused_quota"].includes(st)) {
        const child = this.db.getTask(ev.taskId);
        if (child?.parentId) {
          this.db.timers.triggerEarly(
            (t) =>
              t.ownerKind === "task" &&
              t.ownerId === child.parentId &&
              (t.wakeOnEvent === "child_settled" || t.wakeOnEvent === "any"),
            `child_settled:${child.id}`,
          );
        }
      }
    }

    // Goal settlement and queue admission always run; only Director wakeups are gated by this setting.
    const wakeOnSettle = Boolean(this.config.events?.wakeDirectorOnGoalSettle);

    // Wake Director immediately on P0 escalations or manual quota pauses for any linked Goal
    if (ev.type === "escalation.created") {
      if (!wakeOnSettle) return;
      const p = ev.payload as { severity?: string; title?: string } | null;
      if (p?.severity !== "P0") return;
      const row = this.db.db
        .prepare("SELECT id FROM goals WHERE linked_run_id = ? ORDER BY updated_at DESC LIMIT 1")
        .get(ev.runId) as { id: string } | undefined;
      if (!row) return;
      const goal = this.goals.getGoal(row.id);
      if (!goal?.chatSessionId || ["achieved", "cancelled"].includes(goal.status)) return;
      const openEsc = this.db.listEscalations(ev.runId, "open");
      this.wakeHandler?.(goal.chatSessionId, {
        kind: "goal.settled",
        goalId: goal.id,
        goalVersion: goal.version,
        goalTitle: goal.title,
        runId: ev.runId,
        runStatus: "failed",
        openEscalations: openEsc.length,
        detail: `P0 escalation: ${p.title ?? "human input required"}`,
      });
      return;
    }

    if (ev.type === "quota.run_paused") {
      if (!wakeOnSettle) return;
      const p = ev.payload as { mode?: string; reason?: string } | null;
      if (p?.mode !== "manual") return;
      const row = this.db.db
        .prepare("SELECT id FROM goals WHERE linked_run_id = ? ORDER BY updated_at DESC LIMIT 1")
        .get(ev.runId) as { id: string } | undefined;
      if (!row) return;
      const goal = this.goals.getGoal(row.id);
      if (!goal?.chatSessionId || ["achieved", "cancelled", "paused"].includes(goal.status)) return;
      this.wakeHandler?.(goal.chatSessionId, {
        kind: "goal.settled",
        goalId: goal.id,
        goalVersion: goal.version,
        goalTitle: goal.title,
        runId: ev.runId,
        runStatus: "stopped",
        openEscalations: this.db.listEscalations(ev.runId, "open").length,
        detail: `paused_quota (manual): ${p.reason ?? "quota limit reached"}`,
      });
      return;
    }

    if (ev.type !== "run.status") return;
    const status = String((ev.payload as { status?: string } | null)?.status ?? "");
    if (status === "active" || status === "paused_quota") {
      // A resumed run starts a new execution attempt whose settlement must be handled again.
      if (status === "active") {
        for (const key of this.notifiedSettledGoals) if (key.includes(`:${ev.runId}:`)) this.notifiedSettledGoals.delete(key);
      }
      this.syncQueuedGoalProgress();
      return;
    }
    if (status !== "completed" && status !== "failed" && status !== "stopped") return;

    // Find any active Goal linked to this run and either run deterministic verification automatically
    // or wake its owning Director session!
    const row = this.db.db
      .prepare("SELECT id FROM goals WHERE linked_run_id = ? ORDER BY updated_at DESC LIMIT 1")
      .get(ev.runId) as { id: string } | undefined;
    if (!row) return;
    const goal = this.goals.getGoal(row.id);
    if (!goal) return;
    if (["achieved", "cancelled", "paused", "verifying", "reviewing"].includes(goal.status)) return;

    // Cancel any chat timers subscribed to `goal_settled` / `any` for this goal (we're handling settlement now)
    if (goal.chatSessionId) {
      for (const t of this.db.timers.list({ ownerKind: "chat", ownerId: goal.chatSessionId, status: "pending" })) {
        if ((t.wakeOnEvent === "goal_settled" || t.wakeOnEvent === "any") && (!t.goalId || t.goalId === goal.id)) {
          this.db.timers.cancel(t.id, `goal_settled:${goal.id}`);
        }
      }
    }

    const dedupeKey = `${goal.id}:v${goal.version}:c${goal.reviewCycle}:${ev.runId}:${status}`;
    // Duplicate events still settle the goal and drain the queue; only verification launch and wakeups are deduplicated.
    const firstNotice = !this.notifiedSettledGoals.has(dedupeKey);
    this.notifiedSettledGoals.add(dedupeKey);

    // Control-plane-driven automatic verification on run completion (removes LLM single point of failure)
    if (
      firstNotice &&
      status === "completed" &&
      this.config.goals?.autoVerifyOnSettle !== false &&
      goal.verificationMode !== "none" &&
      !this.activeReviews.has(goal.id)
    ) {
      if (!this.unsubRunEvents) return; // daemon disposed
      void this.verifyAndReviewGoal(goal.id, undefined, { autoTriggered: true }).catch((err) => {
        if (!this.unsubRunEvents || !goal.chatSessionId || !this.config.events?.wakeDirectorOnGoalSettle) return;
        const msg = err instanceof Error ? err.message : String(err);
        let openEscCount = 0;
        try {
          openEscCount = this.db.listEscalations(ev.runId, "open").length;
        } catch {
          return;
        }
        this.wakeHandler?.(goal.chatSessionId, {
          kind: "goal.settled",
          goalId: goal.id,
          goalVersion: goal.version,
          goalTitle: goal.title,
          runId: ev.runId,
          runStatus: "failed",
          openEscalations: openEscCount,
          detail: `verification_error: ${msg.slice(0, 160)}`,
        });
      });
      return;
    }

    // A settled run no longer occupies a goal slot: hand the goal to a human and admit queued goals.
    // Open escalations on the run already carry the human hand-off, so the goal keeps its status for them.
    const openEscalations = this.db.listEscalations(ev.runId, "open").length;
    if ((status === "failed" || status === "stopped") && goal.status === "executing" && openEscalations === 0) {
      this.goals.setStatus(goal.id, "awaiting_human", `Linked run ${ev.runId} ${status} before goal verification; resume or review.`);
    } else if (
      status === "completed" &&
      this.config.goals?.autoVerifyOnSettle !== false &&
      goal.verificationMode === "none" &&
      goal.status === "executing"
    ) {
      this.goals.setStatus(goal.id, "awaiting_human", "Linked run completed (verification_mode=none); review or accept to finish.");
    }
    void this.drainGoalQueue(goal.repoRoot).catch(() => {});
    this.syncQueuedGoalProgress();

    if (!firstNotice || !wakeOnSettle || !goal.chatSessionId) return;

    const event: ChatControlEvent = {
      kind: "goal.settled",
      goalId: goal.id,
      goalVersion: goal.version,
      goalTitle: goal.title,
      runId: ev.runId,
      runStatus: status,
      openEscalations,
    };
    this.wakeHandler?.(goal.chatSessionId, event);
  }

  updateConfig(next: HarnessConfig, catalog = this.orchestrator.catalog, quota = this.orchestrator.quota): void {
    this.config = next;
    this.catalog = catalog;
    this.quota = quota;
    this.goals.updateConfig(next.goals);
    void this.drainGoalQueue().catch(() => {});
  }

  private async resolveChatSelection(input: ChatSelection) {
    const { profile, model } = await resolveLaunchRoute(this.config, {
      tier: 0, cwd: input.repoRoot, profileId: input.profileId, model: input.model, catalog: this.catalog, quota: this.quota,
    });
    return { ...input, profileId: profile.id, model };
  }

  async createChat(input: ChatSelection) {
    const project = input.projectId
      ? this.db.projects.resolveProject(input.projectId)
      : this.db.projects.ensureProjectForPath(input.repoRoot);
    const repoRoot = project?.rootPath ?? input.repoRoot;
    const projectId = project?.id ?? input.projectId ?? null;
    if (project) this.db.projects.touchProject(project.id);
    return this.chats.createSession(
      await this.resolveChatSelection({
        ...input,
        repoRoot,
        projectId,
        profileId: input.profileId ?? project?.defaultProfileId ?? undefined,
        model: input.model !== undefined ? input.model : project?.defaultModel ?? undefined,
        effort: input.effort !== undefined ? input.effort : project?.defaultEffort ?? undefined,
      }),
    );
  }

  /**
   * Reconciles a persisted chat session's `model` against the current compiled catalog.
   * When an unambiguous migration exists on the same profile (e.g. legacy `anthropic/opus` -> `anthropic/claude-opus-5-5`,
   * renamed canonical prefix for the same model slug, or vendor-documented successor), persists the updated model and
   * appends a one-line system notice. When no unambiguous match exists and `strict` is true, throws an actionable `/model` error.
   */
  async resolveOrMigrateSessionModel(
    session: ChatSessionRecord,
    opts?: { strict?: boolean },
  ): Promise<ChatSessionRecord> {
    const oldModel = session.model?.trim();
    if (!oldModel) return session;

    const profile = this.config.profiles.find((p) => p.enabled !== false && p.id === session.profileId);
    if (!profile) return session;

    const catalog = await this.catalog.getCatalog({ cwd: session.repoRoot });
    if (catalog.models.length === 0) return session;

    const profileEntries = catalog.models
      .map((entry) => {
        const routes = [entry.preferredRoute, ...entry.alternateRoutes].filter((r) => r?.profileId === session.profileId);
        return routes.length > 0 ? { entry, routes } : null;
      })
      .filter((x): x is { entry: (typeof catalog.models)[number]; routes: Array<(typeof catalog.models)[number]["preferredRoute"]> } => x !== null);

    const alreadyValid = profileEntries.some(({ entry, routes }) => {
      const bareId = entry.id.slice(entry.id.lastIndexOf("/") + 1);
      return oldModel === entry.id || oldModel === bareId || routes.some((r) => r.wireModelId === oldModel);
    });
    if (alreadyValid) return session;

    let migrated: string | null = null;

    // 1. Bare Claude family alias (`anthropic/opus`, `opus`, `claude-opus`) -> highest-versioned `anthropic/claude-<family>-*` on same profile
    const claudeAlias = oldModel.match(/^(?:anthropic\/)?(?:claude-)?(opus|sonnet|haiku|fable)$/i);
    if (claudeAlias) {
      const family = claudeAlias[1]!.toLowerCase();
      const familyCandidates = profileEntries
        .map(({ entry }) => ({ entry, parsed: parseClaudeFamilyVersion(entry.id) }))
        .filter(
          (x): x is { entry: (typeof catalog.models)[number]; parsed: NonNullable<ReturnType<typeof parseClaudeFamilyVersion>> } =>
            x.parsed !== null && x.parsed.family === family,
        )
        .sort((a, b) => compareSemanticVersions(b.parsed.version, a.parsed.version));
      if (familyCandidates.length > 0) {
        migrated = familyCandidates[0]!.entry.id;
      }
    }

    // 2. Same model slug under a renamed canonical prefix (or dot/hyphen normalized slug) matching uniquely on same profile
    if (!migrated) {
      const oldSlug = (oldModel.includes("/") ? oldModel.slice(oldModel.lastIndexOf("/") + 1) : oldModel).toLowerCase();
      const oldSlugDash = oldSlug.replace(/\./g, "-");
      const slugMatches = profileEntries.filter(({ entry, routes }) => {
        const entrySlug = entry.id.slice(entry.id.lastIndexOf("/") + 1).toLowerCase();
        return (
          entrySlug === oldSlug ||
          entrySlug.replace(/\./g, "-") === oldSlugDash ||
          routes.some((r) => r.wireModelId.toLowerCase() === oldSlug)
        );
      });
      if (slugMatches.length === 1) {
        migrated = slugMatches[0]!.entry.id;
      }
    }

    // 3. Vendor-documented successor chain on the same profile
    if (!migrated) {
      const byLowerId = new Map(profileEntries.map(({ entry }) => [entry.id.toLowerCase(), entry.id]));
      let cursor = oldModel.toLowerCase();
      const visited = new Set<string>([cursor]);
      while (true) {
        const next = vendorSuccessors.get(cursor);
        if (!next || visited.has(next.toLowerCase())) break;
        visited.add(next.toLowerCase());
        const hit = byLowerId.get(next.toLowerCase());
        if (hit) {
          migrated = hit;
          break;
        }
        cursor = next.toLowerCase();
      }
    }

    if (migrated) {
      const updated = this.chats.updateSession(session.id, { model: migrated, resetAcpSession: true });
      this.chats.appendTurn(updated.id, "system_notice", `Migrated session model from ${oldModel} to ${migrated}`);
      return updated;
    }

    if (opts?.strict) {
      throw new Error(
        `Session model "${oldModel}" is no longer available in the current catalog on profile "${session.profileId}". Run /model to select an available model.`,
      );
    }
    return session;
  }

  async openDefaultChat(input: ChatSelection) {
    const project = input.projectId
      ? this.db.projects.resolveProject(input.projectId)
      : this.db.projects.ensureProjectForPath(input.repoRoot);
    const repoRoot = project?.rootPath ?? input.repoRoot;
    const projectId = project?.id ?? input.projectId ?? null;
    if (project) this.db.projects.touchProject(project.id);
    const existing = (projectId ? this.chats.getProjectSession(projectId) : null) ?? this.chats.getWorkspaceSession(repoRoot);
    if (existing) {
      const bound =
        projectId && existing.projectId !== projectId
          ? this.chats.updateSession(existing.id, { projectId, repoRoot })
          : existing;
      return this.resolveOrMigrateSessionModel(bound);
    }
    return this.chats.getOrCreateDefault(
      await this.resolveChatSelection({
        ...input,
        repoRoot,
        projectId,
        profileId: input.profileId ?? project?.defaultProfileId ?? undefined,
        model: input.model !== undefined ? input.model : project?.defaultModel ?? undefined,
        effort: input.effort !== undefined ? input.effort : project?.defaultEffort ?? undefined,
      }),
    );
  }

  switchChatProject(chatSessionId: string, projectRef: string): { session: ChatSessionRecord; project: ReturnType<ControlPlaneDb["projects"]["resolveProject"]> } {
    const session = this.chats.getSession(chatSessionId);
    if (!session) throw new Error(`Unknown chat session ${chatSessionId}`);
    const project = this.db.projects.resolveProject(projectRef);
    if (!project) throw new Error(`Unknown project "${projectRef}". Use /projects or \`pi-harness project add <path>\` first.`);
    this.db.projects.touchProject(project.id);
    const updated = this.chats.updateSession(session.id, {
      projectId: project.id,
      repoRoot: project.rootPath,
      resetAcpSession: true,
    });
    this.chats.appendTurn(
      updated.id,
      "system_notice",
      `Switched session project → ${project.name} (${project.rootPath})`,
    );
    return { session: updated, project };
  }

  activeChatCount(): number {
    return this.activeChats.size;
  }

  activeReviewCount(): number {
    return this.activeReviews.size;
  }

  isChatBusy(chatSessionId: string): boolean {
    return this.activeChats.has(chatSessionId);
  }

  private isRunOccupyingGoalSlot(runId: string | null): boolean {
    if (!runId) return false;
    const run = this.orchestrator.getEffectiveRun(runId);
    return run !== null && (run.status === "active" || run.status === "paused_quota");
  }

  /** A goal holds a concurrency slot while it is launching, an in-process review is running for it, or its run is live. */
  private isGoalOccupyingSlot(goal: GoalRecord): boolean {
    if (this.activeReviews.has(goal.id) || this.launchingGoals.has(goal.id)) return true;
    if (goal.status !== "executing") return false;
    return this.isRunOccupyingGoalSlot(goal.linkedRunId);
  }

  private countActiveGoals(repoRoot: string): number {
    return this.goals.listGoals({ repoRoot }).filter((g) => this.isGoalOccupyingSlot(g)).length;
  }

  /**
   * Rewrites queued goals' progress from the live queue order and slot usage; clears it for goals no longer queued.
   * Covers every repo so callers holding a non-canonical repo path never miss or wrongly clear entries.
   */
  syncQueuedGoalProgress(): void {
    if (this.disposed || this.syncingQueuedProgress) return;
    this.syncingQueuedProgress = true;
    try {
      const limit = Math.max(1, this.config.goals.maxConcurrentGoals ?? 4);
      const entries = this.db.admissionQueue.list({ lane: "goal" });
      const activeByRepo = new Map<string, number>();
      for (const entry of entries) {
        const goal = this.goals.getGoal(entry.targetId);
        if (!goal) continue;
        let activeCount = activeByRepo.get(entry.repoRoot);
        if (activeCount === undefined) {
          activeCount = this.countActiveGoals(entry.repoRoot);
          activeByRepo.set(entry.repoRoot, activeCount);
        }
        const summary = `Queued at #${entry.position} (${activeCount}/${limit} concurrent goals active)`;
        if (goal.liveProgress?.phase !== "queued" || goal.liveProgress?.summary !== summary) {
          this.goals.reportProgress(goal.id, { phase: "queued", summary });
        }
      }
      const queuedIds = new Set(entries.map((e) => e.targetId));
      // Drafts keep a launch error so the user sees why the goal left the queue.
      for (const g of this.goals.listGoals()) {
        if (queuedIds.has(g.id)) continue;
        const phase = g.liveProgress?.phase;
        if (phase === "queued" || (phase === "queue_launch_error" && g.status !== "draft")) this.goals.clearQueuedProgress(g.id);
      }
    } finally {
      this.syncingQueuedProgress = false;
    }
  }

  private async launchCommittedGoal(
    goal: GoalRecord,
    opts?: { profileId?: string; model?: string; effort?: string },
  ): Promise<GoalRecord> {
    if (this.launchingGoals.has(goal.id)) return this.goals.getGoal(goal.id) ?? goal;
    // Claim the slot before the first await so concurrent admission decisions see it.
    this.launchingGoals.add(goal.id);
    let run: { id: string };
    let started: GoalRecord;
    try {
      this.db.admissionQueue.remove("goal", goal.id);
      const criteriaSummary = goal.criteria
        .map((c) => `- [${c.id}] (${c.kind}${c.required ? "*" : ""}) ${c.description}${c.command ? ` [cmd: ${c.command}]` : ""}`)
        .join("\n");
      const runObjective = [
        `Goal ${goal.id} v${goal.version}: ${goal.title}`,
        goal.description,
        "",
        "Acceptance Criteria:",
        criteriaSummary,
        ...(goal.nonGoals.length ? ["", "Non-Goals:", ...goal.nonGoals.map((n) => `- ${n}`)] : []),
      ].join("\n");

      ({ run } = await this.orchestrator.startRun({
        goal: runObjective,
        repoRoot: goal.repoRoot,
        profileId: opts?.profileId,
        model: opts?.model,
        effort: opts?.effort,
      }));

      started = this.goals.linkRunAndStart(goal.id, run.id);
    } finally {
      this.launchingGoals.delete(goal.id);
    }
    this.syncQueuedGoalProgress();
    if (started.chatSessionId) {
      const committedEvent: ChatControlEvent = {
        kind: "goal.committed",
        goalId: started.id,
        goalVersion: started.version,
        goalTitle: started.title,
        runId: run.id,
        criteriaCount: started.criteria.length,
      };
      this.chats.appendTurn(
        started.chatSessionId,
        "system_notice",
        formatChatEventSummary(committedEvent),
        undefined,
        committedEvent,
      );
    }
    this.db.emitEvent(run.id, null, "goal.committed", { goalId: started.id, version: started.version, title: started.title });
    return started;
  }

  async commitGoal(
    goalId: string,
    opts?: { profileId?: string; model?: string; effort?: string; position?: QueuePositionInput; forceStart?: boolean },
  ): Promise<GoalRecord> {
    const goal = this.goals.getGoal(goalId);
    if (!goal) throw new Error(`Unknown goal ${goalId}`);
    if (this.launchingGoals.has(goal.id)) return this.goals.getGoal(goal.id) ?? goal;

    const limit = Math.max(1, this.config.goals.maxConcurrentGoals ?? 4);
    let activeCount = this.countActiveGoals(goal.repoRoot);
    const listQueuedAhead = () =>
      this.db.admissionQueue.list({ lane: "goal", repoRoot: goal.repoRoot }).filter((e) => e.targetId !== goal.id);
    let queuedAhead = listQueuedAhead();
    if (!opts?.forceStart && activeCount < limit && queuedAhead.length > 0) {
      // Free slots go to goals already waiting (FIFO) before this goal decides whether to queue.
      await this.drainGoalQueue(goal.repoRoot);
      // The drain may have launched this goal itself if it was already queued.
      const fresh = this.goals.getGoal(goal.id);
      if (!fresh) throw new Error(`Unknown goal ${goalId}`);
      if (this.launchingGoals.has(fresh.id) || ["executing", "verifying", "reviewing"].includes(fresh.status)) return fresh;
      activeCount = this.countActiveGoals(goal.repoRoot);
      queuedAhead = listQueuedAhead();
    }

    if (!opts?.forceStart && (activeCount >= limit || queuedAhead.length > 0)) {
      this.db.admissionQueue.enqueue({
        lane: "goal",
        repoRoot: goal.repoRoot,
        targetId: goal.id,
        position: opts?.position,
        forceStart: false,
        launchOpts: {
          ...(opts?.profileId ? { profileId: opts.profileId } : {}),
          ...(opts?.model ? { model: opts.model } : {}),
          ...(opts?.effort ? { effort: opts.effort } : {}),
        },
        note: goal.title,
      });
      this.syncQueuedGoalProgress();
      return this.goals.getGoal(goal.id)!;
    }

    return await this.launchCommittedGoal(goal, opts);
  }

  private drainingGoals = false;

  /** Admits queued goals in strict queue order whenever capacity opens or `maxConcurrentGoals` increases. */
  async drainGoalQueue(repoRoot?: string): Promise<string[]> {
    if (this.disposed) return [];
    if (this.drainingGoals) {
      // Run this request after the current drain instead of dropping it.
      if (repoRoot) this.pendingDrainRepos.add(canonicalPath(repoRoot));
      else this.pendingDrainAll = true;
      return [];
    }
    this.drainingGoals = true;
    const startedIds: string[] = [];
    try {
      const limit = Math.max(1, this.config.goals.maxConcurrentGoals ?? 4);
      const repos = repoRoot
        ? [repoRoot]
        : [...new Set(this.db.admissionQueue.list({ lane: "goal" }).map((e) => e.repoRoot))];
      for (const repo of repos) {
        while (!this.disposed) {
          const next = this.db.admissionQueue.peekNext("goal", repo);
          if (!next) break;
          const goal = this.goals.getGoal(next.targetId);
          if (
            !goal ||
            this.launchingGoals.has(goal.id) ||
            ["achieved", "cancelled", "executing", "verifying", "reviewing"].includes(goal.status)
          ) {
            this.db.admissionQueue.remove("goal", next.targetId);
            continue;
          }
          const activeCount = this.countActiveGoals(repo);
          if (!next.forceStart && activeCount >= limit) break;
          const launchOpts = (next.launchOpts ?? {}) as { profileId?: string; model?: string; effort?: string };
          try {
            await this.launchCommittedGoal(goal, launchOpts);
            startedIds.push(goal.id);
          } catch (err) {
            this.db.admissionQueue.remove("goal", next.targetId);
            this.goals.reportProgress(goal.id, {
              phase: "queue_launch_error",
              summary: `Failed to launch from queue: ${err instanceof Error ? err.message : String(err)}`,
            });
          }
        }
        this.syncQueuedGoalProgress();
      }
    } finally {
      this.drainingGoals = false;
      if (!this.disposed && (this.pendingDrainAll || this.pendingDrainRepos.size > 0)) {
        const drainAll = this.pendingDrainAll;
        const repos = [...this.pendingDrainRepos];
        this.pendingDrainAll = false;
        this.pendingDrainRepos.clear();
        void (drainAll ? this.drainGoalQueue() : Promise.all(repos.map((r) => this.drainGoalQueue(r)))).catch(() => {});
      }
    }
    return startedIds;
  }

  /** Emergency force-start for a queued/paused goal or worker (optionally bypassing quota pause). */
  async forceStartQueuedItem(input: {
    lane?: AdmissionLane;
    targetId: string;
    bypassQuota?: boolean;
    reason?: string;
  }): Promise<Record<string, unknown>> {
    const targetId = input.targetId.trim();
    const lane: AdmissionLane =
      input.lane ?? (targetId.startsWith("goal-") || this.goals.getGoal(targetId) ? "goal" : "worker");
    const warning = "Force-start bypasses admission/quota protections; use sparingly for urgent work only.";

    if (lane === "worker") {
      return await this.orchestrator.forceStartWorker(targetId, {
        bypassQuota: input.bypassQuota,
        reason: input.reason,
      });
    }

    const goal = this.goals.getGoal(targetId);
    if (!goal) throw new Error(`Unknown goal ${targetId}`);

    // If the goal is queued in the admission queue, mark forced and launch immediately!
    const qEntry = this.db.admissionQueue.get("goal", goal.id);
    if (qEntry) {
      this.db.admissionQueue.markForceStart("goal", goal.id);
      const launched = await this.launchCommittedGoal(
        goal,
        (qEntry.launchOpts ?? {}) as { profileId?: string; model?: string; effort?: string },
      );
      return { goal: launched, action: "force_started_queued_goal", warning };
    }

    // If the goal's linked run is quota-paused and bypassQuota is true, force-resume it immediately!
    if (goal.linkedRunId) {
      const run = this.db.getRun(goal.linkedRunId);
      const quotaSt = this.orchestrator.getRunQuotaStatus(goal.linkedRunId);
      if (run?.status === "paused_quota" || quotaSt) {
        if (!input.bypassQuota) {
          throw new Error(`Goal ${goal.id} (${goal.linkedRunId}) is paused on quota; pass bypass_quota: true to force-start despite quota.`);
        }
        const resumed = await this.orchestrator.resumeRunFromQuota(goal.linkedRunId, {
          trigger: "manual",
          userMessage: input.reason ?? "Force-started bypassing quota pause",
        });
        return { goal: this.goals.getGoal(goal.id)!, resumed, action: "force_resumed_goal_from_quota", warning };
      }
    }

    throw new Error(`Goal ${goal.id} (status: ${goal.status}) is neither queued nor paused on quota.`);
  }

  async amendGoal(
    goalId: string,
    patch: Parameters<GoalEngine["amendGoal"]>[1],
  ): Promise<{ goal: GoalRecord; delta: ReturnType<GoalEngine["amendGoal"]>["delta"] }> {
    const { goal, delta } = this.goals.amendGoal(goalId, patch);
    let amendedGoal = goal;

    if (goal.linkedRunId) {
      // Persist updated Goal Contract text onto runs.goal and Blackboard so rollovers/resumes never see stale v1 requirements
      const criteriaSummary = goal.criteria
        .map((c) => `- [${c.id}] (${c.kind}${c.required ? ", required" : ""}): ${c.description}${c.command ? ` [cmd: ${c.command}]` : ""}`)
        .join("\n");
      const updatedObjective = [
        `GOAL ${goal.id} (v${goal.version}): ${goal.title}`,
        goal.description,
        "",
        "Acceptance Criteria:",
        criteriaSummary,
        ...(goal.nonGoals.length ? ["", "Non-Goals:", ...goal.nonGoals.map((n) => `- ${n}`)] : []),
      ].join("\n");
      this.db.updateRunGoal(goal.linkedRunId, updatedObjective);

      this.db.emitEvent(goal.linkedRunId, null, "goal.amended", { goalId: goal.id, delta });
      // Steer active Tier 1 Orchestrator (or relaunch in-place if settled!) with the structured vN -> vN+1 delta
      const orch = this.db.listTasks(goal.linkedRunId).find((t) => t.tier === 1);
      if (orch) {
        // Refresh Tier 1's title, objective, and stored handoff goalInvariants so rollovers never replay stale v1 constraints
        let updatedHandoffJson = orch.handoffJson;
        if (orch.handoffJson) {
          try {
            const parsed = JSON.parse(orch.handoffJson) as {
              goalInvariants?: string[];
              architecturalDecisions?: Array<{ decision: string; rationale: string }>;
              activeRisks?: string[];
              nextActions?: string[];
            };
            parsed.goalInvariants = [updatedObjective];
            updatedHandoffJson = JSON.stringify(parsed);
          } catch {
            // keep existing handoffJson if unparseable
          }
        }
        this.db.updateTask(orch.id, {
          title: `Goal ${goal.id} v${goal.version}: ${goal.title}`,
          objective: updatedObjective,
          ...(updatedHandoffJson !== orch.handoffJson ? { handoffJson: updatedHandoffJson } : {}),
        });
        this.db.blackboardSet(
          goal.linkedRunId,
          `goal_contract_v${goal.version}`,
          { version: goal.version, title: goal.title, description: goal.description, criteria: goal.criteria, nonGoals: goal.nonGoals, delta },
          orch.id,
        );
        const steerMsg = [
          `[GOAL AMENDED: ${goal.id} v${delta.fromVersion} -> v${delta.toVersion}]`,
          `Rationale: ${delta.rationale}`,
          delta.addedCriteria.length
            ? `Added criteria: ${delta.addedCriteria.map((c) => `${c.id}: ${c.description}`).join("; ")}`
            : null,
          delta.removedCriteriaIds.length ? `Removed criteria: ${delta.removedCriteriaIds.join(", ")}` : null,
          delta.modifiedCriteria.length
            ? `Modified criteria: ${delta.modifiedCriteria.map((c) => `${c.id}: ${c.description}`).join("; ")}`
            : null,
          "Reconcile active Tier 2 Workers: KEEP unaffected workers, STEER adjustable workers, CANCEL_AND_REPLACE invalidated workers.",
        ]
          .filter(Boolean)
          .join("\n");
        let delivered = await this.orchestrator.steerTask(orch.id, steerMsg).catch(() => false);
        if (!delivered && goal.status === "executing") {
          delivered = await this.orchestrator
            .resumeTask(orch.id, { allowCompleted: true, note: steerMsg })
            .then(() => true, () => false);
        }
        if (!delivered) amendedGoal = this.handOffStrandedGoal(goal, "amendment could not be delivered to its run");
      } else {
        amendedGoal = this.handOffStrandedGoal(goal, "linked run has no orchestrator to apply the amendment");
      }
    } else {
      amendedGoal = this.handOffStrandedGoal(goal, "no linked run to apply the amendment");
    }

    if (goal.chatSessionId) {
      const amendedEvent: ChatControlEvent = {
        kind: "goal.amended",
        goalId: goal.id,
        fromVersion: delta.fromVersion,
        toVersion: delta.toVersion,
        rationale: delta.rationale,
      };
      this.chats.appendTurn(
        goal.chatSessionId,
        "system_notice",
        formatChatEventSummary(amendedEvent),
        undefined,
        amendedEvent,
      );
    }

    return { goal: amendedGoal, delta };
  }

  /** Moves an `executing` goal whose run is not live to `awaiting_human` and releases its slot to the queue. */
  private handOffStrandedGoal(goal: GoalRecord, reason: string): GoalRecord {
    // Re-read so a concurrent pause, cancel, or amendment is never overwritten.
    const current = this.goals.getGoal(goal.id);
    if (!current || current.version !== goal.version || current.status !== "executing") return current ?? goal;
    if (this.isRunOccupyingGoalSlot(current.linkedRunId)) return current;
    const updated = this.goals.setStatus(goal.id, "awaiting_human", `Goal ${goal.id} needs attention: ${reason}; resume or review.`);
    void this.drainGoalQueue(goal.repoRoot).catch(() => {});
    return updated;
  }

  /**
   * Executes the full deterministic Verification Gates -> Independent Skeptical Done-Review pipeline.
   * Never lets the generator self-approve: invokes a fresh read-only Reviewer session (or fails fast at $0 LLM cost on deterministic gates).
   */
  async verifyAndReviewGoal(
    goalId: string,
    signal?: AbortSignal,
    opts?: { verificationMode?: VerificationMode; summary?: string; autoTriggered?: boolean },
  ): Promise<{
    goal: GoalRecord;
    review: GoalReviewVerdict;
    circuitBreakerTripped?: string;
  }> {
    const goal = this.goals.getGoal(goalId);
    if (!goal) throw new Error(`Unknown goal ${goalId}`);
    if (this.activeReviews.has(goal.id)) throw new Error(`Review already in progress for ${goal.id}`);
    if (goal.status === "paused" || goal.status === "cancelled") throw new Error(`Goal is ${goal.status}`);
    const abort = new AbortController();
    this.activeReviews.set(goal.id, abort);
    const reviewSignal = signal ? AbortSignal.any([abort.signal, signal]) : abort.signal;
    const unsubscribe = this.db.onEvent(ev => {
      if (ev.runId !== goal.linkedRunId || ev.type !== "run.status") return;
      if ((ev.payload as { status: string }).status === "stopped") abort.abort(new Error("Run stopped during review"));
    });
    try {
      return await this.verifyAndReviewGoalInner(goal, reviewSignal, opts);
    } catch (error) {
      if (!this.disposed) {
        try {
          const current = this.goals.getGoal(goal.id);
          if (current?.version === goal.version && ["executing", "verifying", "reviewing"].includes(current.status)) {
            if (this.isRunOccupyingGoalSlot(current.linkedRunId)) {
              if (current.status !== goal.status) this.goals.setStatus(goal.id, goal.status);
            } else {
              const msg = error instanceof Error ? error.message : String(error);
              this.goals.setStatus(goal.id, "awaiting_human", `Verification failed: ${msg.slice(0, 240)}`);
            }
          }
        } catch {
          // DB may already be closing during shutdown
        }
      }
      throw error;
    } finally {
      unsubscribe();
      this.activeReviews.delete(goal.id);
      if (!this.disposed) {
        this.syncQueuedGoalProgress();
        const latest = this.goals.getGoal(goal.id);
        if (latest && !this.isGoalOccupyingSlot(latest)) void this.drainGoalQueue(latest.repoRoot).catch(() => {});
      }
    }
  }

  private async verifyAndReviewGoalInner(
    goal: GoalRecord,
    signal: AbortSignal,
    opts?: { verificationMode?: VerificationMode; summary?: string; autoTriggered?: boolean },
  ): Promise<{
    goal: GoalRecord;
    review: GoalReviewVerdict;
    circuitBreakerTripped?: string;
  }> {
    const run = goal.linkedRunId ? this.db.getRun(goal.linkedRunId) : null;
    const assertCurrent = () => {
      signal.throwIfAborted();
      const current = this.goals.getGoal(goal.id);
      if (current?.version !== goal.version || current.status === "paused" || current.status === "cancelled" ||
          (run && this.db.getRun(run.id)?.status === "stopped")) {
        throw new Error(`Goal ${goal.id} changed or stopped during review`);
      }
    };
    assertCurrent();

    const committedMode: VerificationMode = goal.verificationMode ?? "auto";
    if (opts?.verificationMode === "none" && committedMode !== "none") {
      throw new Error(
        `Cannot downgrade verification_mode to 'none' on goal ${goal.id} (committed mode is '${committedMode}'). Amend the goal if criteria are no longer required.`,
      );
    }
    const effectiveMode: VerificationMode = opts?.verificationMode ?? committedMode;
    if (effectiveMode === "none") {
      const directResults: CriterionEvaluation[] = goal.criteria.map((c) => ({
        criterionId: c.id,
        passed: true,
        evidence: opts?.summary?.trim() || "Marked complete without separate verification pass (verification_mode=none).",
      }));
      const outcome = this.goals.recordReviewVerdict(goal.id, {
        verdict: "approved",
        criteriaResults: directResults,
        blockingIssues: [],
        summary: opts?.summary?.trim() || "Goal completed directly (verification_mode=none).",
        diffHash: `direct-${Date.now()}`,
      });
      await this.handlePostReviewTransition(outcome, { autoTriggered: opts?.autoTriggered });
      return outcome;
    }

    const targetRef = run?.baseBranch || "HEAD";
    // Prepare an isolated detached verification worktree at exact targetSha whenever verifying a git branch,
    // so gates and Done-Reviewers (which can run `bun test`, `tmux`, etc.) always test the real target branch
    // without colliding with dirty edits or a different checked-out branch in `goal.repoRoot`.
    let verifyCwd = goal.repoRoot;
    let verifyRef = targetRef;
    let verifyWorktreePath: string | null = null;
    if (run?.baseBranch) {
      const targetSha = await runGit(goal.repoRoot, ["rev-parse", "--verify", `${run.baseBranch}^{commit}`])
        .then((r) => r.stdout)
        .catch(() => null);
      if (targetSha) {
        const wtMgr = this.orchestrator.getWorktreeManager(goal.repoRoot);
        const candidatePath = join(wtMgr.baseDir, "_verify", `${goal.id}-${randomUUID().slice(0, 8)}`);
        const { mkdirSync } = await import("node:fs");
        mkdirSync(join(wtMgr.baseDir, "_verify"), { recursive: true });
        await runGit(goal.repoRoot, ["worktree", "add", "--detach", candidatePath, targetSha]);
        verifyWorktreePath = candidatePath;
        verifyCwd = candidatePath;
        verifyRef = targetSha;
        // Do NOT silently swallow bootstrap failures — fail verification cleanly if setupCommand fails
        await wtMgr.bootstrapWorktree(candidatePath);
      } else {
        // Fail closed when targetBranch cannot be resolved and host checkout is on a different branch
        const headBranch = await runGit(goal.repoRoot, ["rev-parse", "--abbrev-ref", "HEAD"])
          .then((r) => r.stdout)
          .catch(() => null);
        if (headBranch && headBranch !== run.baseBranch) {
          throw new Error(
            `Cannot verify goal ${goal.id}: target branch '${run.baseBranch}' does not resolve (host checkout is '${headBranch}').`,
          );
        }
      }
    }

    const cleanupVerifyWorktree = async () => {
      if (!verifyWorktreePath) return;
      const p = verifyWorktreePath;
      verifyWorktreePath = null;
      await runGit(goal.repoRoot, ["worktree", "remove", "--force", p]).catch(async () => {
        const { rmSync } = await import("node:fs");
        rmSync(p, { recursive: true, force: true });
      });
      await runGit(goal.repoRoot, ["worktree", "prune"]).catch(() => {});
    };

    try {
      const gateResult = await this.goals.runDeterministicGates(goal.id, run?.baseSha, signal, {
        cwd: verifyCwd,
        targetRef: verifyRef,
      });
      assertCurrent();

      // If deterministic gates failed, record immediately without burning LLM review tokens!
      if (!gateResult.allPassed) {
        const outcome = this.goals.recordReviewVerdict(goal.id, {
          verdict: "changes_requested",
          criteriaResults: gateResult.results,
          blockingIssues: gateResult.results.filter((r) => !r.passed).map((r) => r.evidence),
          summary: "Deterministic verification gates failed; bouncing back to Tier 1 before LLM review.",
          diffHash: gateResult.diffHash,
          spentUsd: this.sumRunCostUsd(goal.linkedRunId),
        });
        await this.handlePostReviewTransition(outcome, { autoTriggered: opts?.autoTriggered });
        return outcome;
      }

      // Agent-chosen verification depth: when deterministic checks already cover the goal (or mode='deterministic'),
      // approve cleanly at $0 LLM cost without spawning a redundant independent reviewer session!
      if (!shouldRunIndependentReview(goal, effectiveMode)) {
        const coveredIds = new Set(gateResult.results.map((r) => r.criterionId));
        const allResults: CriterionEvaluation[] = [
          ...gateResult.results,
          ...goal.criteria
            .filter((c) => !coveredIds.has(c.id))
            .map((c) => ({
              criterionId: c.id,
              passed: true,
              evidence: opts?.summary?.trim() || `Verified without separate LLM review (${effectiveMode}).`,
            })),
        ];
        const outcome = this.goals.recordReviewVerdict(goal.id, {
          verdict: "approved",
          criteriaResults: allResults,
          blockingIssues: [],
          summary:
            opts?.summary?.trim() ||
            (gateResult.results.length > 0
              ? `All ${gateResult.results.length} deterministic check(s) passed; approved without redundant LLM review.`
              : "Approved cleanly without redundant LLM review."),
          diffHash: gateResult.diffHash,
        });
        await this.handlePostReviewTransition(outcome, { autoTriggered: opts?.autoTriggered });
        return outcome;
      }

      // All deterministic checks passed -> spawn fresh read-only Independent Skeptical Reviewer
      let diffStat = "";
      let diffPatch = "";
      try {
        diffStat = (
          await runGit(verifyCwd, run?.baseSha ? ["diff", "--stat", `${run.baseSha}..${verifyRef}`] : ["status", "--short"])
        ).stdout;
        if (run?.baseSha) {
          const rawPatch = (await runGit(verifyCwd, ["diff", `${run.baseSha}..${verifyRef}`])).stdout;
          diffPatch = rawPatch.length > 24_000 ? `${rawPatch.slice(0, 24_000)}\n... [diff truncated at 24KB]` : rawPatch;
        }
      } catch {
        diffStat = "";
        diffPatch = "";
      }

      const reviewerPrompt = this.goals.buildReviewerPrompt(goal, gateResult, diffStat, diffPatch);
      // Prefer an independent model family different from the authoring run's Tier 1/Tier 2 model
      const runTasks = run ? this.db.listTasks(run.id) : [];
      const authorModel =
        runTasks.find((t) => t.tier === 2 && t.model)?.model ??
        runTasks.find((t) => t.tier === 1 && t.model)?.model ??
        undefined;
      const cachedCat = this.catalog.loadCached();
      const recVerify = cachedCat
        ? recommendModelsByTaskKind(cachedCat.models, { avoidModelId: authorModel }).verify
        : null;
      const { profile, model, wireModel } = await resolveLaunchRoute(this.config, {
        tier: 3,
        cwd: verifyCwd,
        profileId: recVerify?.profile,
        model: recVerify?.model,
        catalog: this.catalog,
        quota: this.quota,
      });
      const reviewerEffort = recVerify?.effort && recVerify.effort !== "default" ? recVerify.effort : undefined;
      const runner = this.customRunnerFactory
        ? this.customRunnerFactory(profile)
        : profile.kind === "acp"
          ? new AcpRunner(profile)
          : new PiRunner(profile);

      type SubmittedVerdict = {
        verdict: "approved" | "changes_requested" | "escalate_human";
        criteriaResults: CriterionEvaluation[];
        blockingIssues?: string[];
        summary: string;
      };
      const verdictBox: { value: SubmittedVerdict | null } = { value: null };

      const submitTool: ToolDefinition = {
        name: "submit_review_verdict",
        description: "Submit the structured Independent Done-Review verdict.",
        inputSchema: {
          type: "object",
          required: ["verdict", "criteriaResults", "summary"],
          properties: {
            verdict: { type: "string", enum: ["approved", "changes_requested", "escalate_human"] },
            criteriaResults: { type: "array", items: { type: "object" } },
            blockingIssues: { type: "array", items: { type: "string" } },
            summary: { type: "string" },
          },
        },
      };

      // Persist the reviewer as a Tier 3 verify task under the goal's linked run so it shows up in
      // runs.trees and its transcript is replayable / watchable like any orchestrator agent.
      const cycle = goal.reviewCycle + 1;
      const reviewRun = run ?? await this.orchestrator.ensureDirectorTaskRun(goal.chatSessionId ?? goal.id, goal.repoRoot);
      assertCurrent();
      this.goals.setStatus(goal.id, "reviewing");
      const reviewerTask = this.db.createTask({
        runId: reviewRun.id,
        parentId: this.db.listTasks(reviewRun.id).find((t) => t.tier === 1)?.id ?? null,
        tier: 3,
        kind: "verify",
        title: `Done-Review: ${goal.title} (cycle ${cycle})`,
        objective: reviewerPrompt,
        readOnly: true,
        profileId: profile.id,
        model,
        maxTurns: this.config.rollover.reviewerMaxTurns,
      });
      const abort = new AbortController();
      const reviewerSignal = AbortSignal.any([signal, abort.signal]);
      const unsubscribe = this.db.onEvent(ev => {
        if (ev.runId !== reviewRun.id) return;
        if ((ev.taskId === reviewerTask.id && ev.type === "task.status" && (ev.payload as { to: string }).to === "cancelled") ||
            (ev.type === "run.status" && (ev.payload as { status: string }).status === "stopped")) {
          abort.abort(new Error("Reviewer cancelled"));
        }
      });
      const emit = (type: string, payload: unknown) => {
        if (!reviewerSignal.aborted) this.db.emitEvent(reviewRun.id, reviewerTask.id, type, payload);
      };

      const failedSummary = "Independent reviewer failed before completing verification.";
      const failReviewer = (msg: string) => {
        if (this.db.getTask(reviewerTask.id)?.status !== "running") return;
        if (reviewerSignal.aborted) {
          this.db.updateTask(reviewerTask.id, { status: "cancelled", summary: "Reviewer cancelled" });
          return;
        }
        emit("agent.error", { error: msg });
        this.db.updateTask(reviewerTask.id, { status: "failed", summary: failedSummary });
      };

      let outcome: ReturnType<GoalEngine["recordReviewVerdict"]>;
      const bridgeExt = import.meta.url.endsWith(".js") ? "mcp-bridge.js" : "mcp-bridge.ts";
      const mcpBridgeEntrypoint = join(dirname(fileURLToPath(import.meta.url)), "..", "runners", bridgeExt);
      const reviewerExecuteTool = async (name: string, args: Record<string, unknown>) => {
        if (name === "submit_review_verdict") {
          verdictBox.value = {
            verdict: String(args.verdict) as SubmittedVerdict["verdict"],
            criteriaResults: Array.isArray(args.criteriaResults) ? (args.criteriaResults as CriterionEvaluation[]) : [],
            blockingIssues: Array.isArray(args.blockingIssues) ? (args.blockingIssues as string[]) : [],
            summary: String(args.summary ?? ""),
          };
          return { recorded: true };
        }
        throw new Error(`Unknown reviewer tool ${name}`);
      };
      this.activeReviewerTools.set(reviewerTask.id, { tools: [submitTool], execute: reviewerExecuteTool });
      const reviewerTimeout = setTimeout(() => {
        abort.abort(new Error("Independent Done-Reviewer timed out after 10 minutes"));
      }, 600_000);
      reviewerTimeout.unref?.();
      try {
        if (this.db.getTask(reviewerTask.id)?.status === "cancelled") abort.abort(new Error("Reviewer cancelled"));
        this.db.updateTask(reviewerTask.id, { status: reviewerSignal.aborted ? "cancelled" : "running" });
        reviewerSignal.throwIfAborted();
        const cost = recordCostInvocation(this.db, {
          chatSessionId: goal.chatSessionId ?? undefined,
          runId: reviewRun.id,
          taskId: reviewerTask.id,
          activity: "done_review",
          profile,
          model,
          wireModel,
        });
        let res;
        try {
          res = await runner.run({
            runId: reviewRun.id,
            taskId: reviewerTask.id,
            tier: 3,
            kind: "verify",
            cwd: verifyCwd,
            toolPolicy: "read_only_workspace",
            allowShellInReadOnly: Boolean(verifyWorktreePath),
            model: wireModel,
            effort: reviewerEffort,
            systemPrompt:
              "You are an independent read-only Done-Reviewer operating in a disposable verification checkout of the target branch. Inspect the implementation, diffs, and verification evidence for each required criterion — you may run tests, linters, git diff, rg, sg, or tmux checks freely to verify behavior, but never edit tracked repository code or delegate. Call submit_review_verdict if available, or return the requested JSON verdict with concrete sources for every required criterion.",
            initialPrompt: reviewerPrompt,
            maxTurns: this.config.rollover.reviewerMaxTurns,
            tools: [submitTool],
            executeTool: reviewerExecuteTool,
            daemonSocketPath: this.config.socketPath,
            mcpBridgeEntrypoint,
            externalMcpServers: resolveExternalMcpServers(this.config.stateDir, this.config.externalMcpServers),
            extraPathDirs: nativeToolsPathDirs(this.config.stateDir),
            extraReadRoots: [
              join(goal.repoRoot, ".harness", "goals"),
              join(goal.repoRoot, ".harness", "artifacts"),
              ...(this.config.worktree.extraReadRoots ?? []),
            ],
            signal: reviewerSignal,
            callbacks: {
              onSessionId: (sid) => {
                this.db.updateTask(reviewerTask.id, { sessionId: sid });
                cost.onSessionId(sid);
              },
              onTextDelta: (text) => emit("agent.text_delta", { text }),
              onThoughtDelta: (text) => emit("agent.thought_delta", { text }),
              onToolEvent: (ev) => emit("agent.tool", ev),
              onUsage: (usage) => {
                this.db.updateTask(reviewerTask.id, { contextUsed: usage.used, contextSize: usage.size });
                emit("agent.usage", usage);
                cost.onUsage(usage);
              },
              onPassiveQuota: (sig) => this.quota.ingestPassiveSignal(sig),
            },
          });
        } finally {
          cost.finish();
        }
        reviewerSignal.throwIfAborted();
        assertCurrent();
        if (res.stopReason !== "completed" && res.stopReason !== "max_turns") {
          throw new Error(`Reviewer stopped: ${res.stopReason}. ${res.text}`);
        }

        // Fallback JSON parse if reviewer returned JSON text instead of calling submit_review_verdict
        if (!verdictBox.value) {
          verdictBox.value = parseReviewerVerdictJson<SubmittedVerdict>(res.text);
        }

        const submittedVerdict = verdictBox.value;

        // Merge deterministic gate pass results with reviewer evaluations so deterministic criteria stay evidenced
        const mergedResults: CriterionEvaluation[] = [...gateResult.results];
        for (const r of submittedVerdict?.criteriaResults ?? []) {
          if (!mergedResults.some((m) => m.criterionId === r.criterionId)) {
            mergedResults.push(r);
          }
        }

        outcome = this.db.transaction(() => {
          const recorded = this.goals.recordReviewVerdict(goal.id, {
            verdict: submittedVerdict?.verdict ?? "changes_requested",
            criteriaResults: mergedResults,
            blockingIssues:
              submittedVerdict?.blockingIssues ?? (submittedVerdict ? [] : ["Reviewer did not return a structured verdict"]),
            summary: submittedVerdict?.summary ?? "Incomplete review output",
            diffHash: gateResult.diffHash,
            spentUsd: this.sumRunCostUsd(goal.linkedRunId),
          });
          const summary = `${recorded.review.verdict}: ${recorded.review.summary}`.slice(0, 2000);
          const dossier = writeTaskDossier(this.db, reviewRun, reviewerTask.id, "review.json",
            JSON.stringify(recorded.review, null, 2), `Independent review of ${goal.id} v${goal.version}`);
          this.db.updateTask(reviewerTask.id, {
            status: "completed", summary,
            resultJson: JSON.stringify({
              summary, sources: [{ path: dossier.path, symbol: "criteriaResults" }], artifact_paths: [dossier.path],
            }),
          });
          return recorded;
        });
      } catch (err) {
        const detail = err instanceof Error ? err.message : String(err);
        failReviewer(detail);
        reviewerSignal.throwIfAborted();
        throw new Error(`${failedSummary} (${detail})`);
      } finally {
        clearTimeout(reviewerTimeout);
        this.activeReviewerTools.delete(reviewerTask.id);
        unsubscribe();
      }

      await this.handlePostReviewTransition(outcome, { autoTriggered: opts?.autoTriggered });
      return outcome;
    } finally {
      await cleanupVerifyWorktree();
    }
  }

  private async handlePostReviewTransition(
    outcome: {
      goal: GoalRecord;
      review: GoalReviewVerdict;
      circuitBreakerTripped?: string;
    },
    opts?: { autoTriggered?: boolean },
  ): Promise<void> {
    const { review, circuitBreakerTripped } = outcome;
    let goal = outcome.goal;
    if (goal.status === "executing") {
      // Changes requested: steer Tier 1, relaunch it in place if it already settled, or hand the goal to a human.
      let settled = goal;
      const orch = goal.linkedRunId ? this.db.listTasks(goal.linkedRunId).find((t) => t.tier === 1) : undefined;
      if (!goal.linkedRunId) {
        settled = this.handOffStrandedGoal(goal, "no linked run to apply requested changes");
      } else if (orch) {
        const unmet = review.criteriaResults.filter((c) => !c.passed);
        const msg = [
          `[INDEPENDENT REVIEW #${review.cycle}: CHANGES REQUESTED for ${goal.id} v${goal.version}]`,
          `Summary: ${review.summary}`,
          ...unmet.map((u) => `- ${u.criterionId}: ${u.evidence}${u.remediation ? ` | Fix: ${u.remediation}` : ""}`),
          ...review.blockingIssues.map((b) => `- Blocker: ${b}`),
        ].join("\n");
        let delivered = await this.orchestrator.steerTask(orch.id, msg).catch(() => false);
        if (!delivered) {
          delivered = await this.orchestrator
            .resumeTask(orch.id, { allowCompleted: true, note: msg })
            .then(() => true, () => false);
        }
        if (!delivered) settled = this.handOffStrandedGoal(goal, "requested changes could not be delivered to its run");
      } else {
        settled = this.handOffStrandedGoal(goal, "linked run has no orchestrator to apply requested changes");
      }
      if (settled.status === "awaiting_human") {
        goal = settled;
        outcome.goal = settled;
      }
    }

    if (goal.linkedRunId) {
      this.db.emitEvent(goal.linkedRunId, null, "goal.reviewed", {
        goalId: goal.id,
        version: goal.version,
        cycle: review.cycle,
        verdict: review.verdict,
        status: goal.status,
        circuitBreakerTripped,
        summary: review.summary,
      });

      if (goal.status === "achieved") {
        this.db.updateRunStatus(goal.linkedRunId, "completed");
        this.autoResolveGoalEscalations(goal.linkedRunId, `Auto-resolved: goal ${goal.id} achieved`);
        void this.drainGoalQueue(goal.repoRoot).catch(() => {});
      } else if (goal.status === "awaiting_human") {
        void this.drainGoalQueue(goal.repoRoot).catch(() => {});
        const orch = this.db.listTasks(goal.linkedRunId).find((t) => t.tier === 1);
        if (orch) {
          this.db.createEscalation({
            runId: goal.linkedRunId,
            fromTaskId: orch.id,
            toTaskId: null,
            severity: "P0",
            category: "verification_failed",
            title: circuitBreakerTripped ?? `Goal ${goal.id} needs human review`,
            evidencePaths: [goal.repoRoot],
            blocking: true,
          });
        }
      }
    }

    if (goal.chatSessionId) {
      const reviewedEvent: ChatControlEvent = {
        kind: "goal.reviewed",
        goalId: goal.id,
        goalVersion: goal.version,
        cycle: review.cycle,
        verdict: review.verdict,
        status: goal.status,
        summary: review.summary,
        ...(circuitBreakerTripped ? { circuitBreakerTripped } : {}),
      };
      const willWakeDirector = Boolean(
        opts?.autoTriggered &&
          this.config.events?.wakeDirectorOnGoalSettle !== false &&
          (goal.status === "achieved" || goal.status === "awaiting_human"),
      );
      if (willWakeDirector && this.wakeHandler) {
        // Let the wakeup turn record the single canonical `system_notice` turn so `goal.reviewed` is never delivered twice!
        this.wakeHandler(goal.chatSessionId, reviewedEvent);
      } else {
        this.chats.appendTurn(
          goal.chatSessionId,
          "system_notice",
          formatChatEventSummary(reviewedEvent),
          undefined,
          reviewedEvent,
        );
      }
    }
  }

  /** Resolves all open/acked escalations on a settled goal's run so stale escalations never linger. */
  autoResolveGoalEscalations(runId: string | null | undefined, resolution: string): number {
    if (!runId) return 0;
    const open = this.db.listEscalations(runId).filter((e) => e.status === "open" || e.status === "acked");
    for (const esc of open) {
      this.db.ackEscalation(esc.id, resolution, true);
    }
    return open.length;
  }

  /** Cancels all active Director turns and in-flight Done-Reviews (used by `/stop`). */
  async cancelAllActive(reason = "Stopped by user"): Promise<{ cancelledChats: number; cancelledReviews: number }> {
    let cancelledChats = 0;
    for (const chatId of [...this.activeChats.keys()]) {
      if (await this.cancelChat(chatId)) cancelledChats++;
    }
    let cancelledReviews = 0;
    for (const [goalId, abort] of [...this.activeReviews.entries()]) {
      abort.abort(new Error(reason));
      this.activeReviews.delete(goalId);
      cancelledReviews++;
    }
    return { cancelledChats, cancelledReviews };
  }

  /** Cancels an in-flight Tier 0 Director turn immediately (e.g. when user presses Esc). */
  async cancelChat(chatSessionId: string): Promise<boolean> {
    const active = this.activeChats.get(chatSessionId);
    if (!active) return false;
    active.abort.abort();
    await active.settled;
    return true;
  }

  async steerChat(
    chatSessionId: string,
    message: string,
    opts?: { role?: "user" | "system_notice"; event?: ChatControlEvent },
  ): Promise<boolean> {
    const active = this.activeChats.get(chatSessionId);
    if (!active || active.abort.signal.aborted) return false;
    const role = opts?.event ? "system_notice" : (opts?.role ?? "user");
    const modelText = opts?.event ? formatChatEventForModel(opts.event) : message;
    const storedText = opts?.event ? formatChatEventSummary(opts.event) : message;

    // If the Director is currently blocked waiting on subagents (`goal_wait` / `wait_tasks`),
    // unblock the wait immediately so the Director receives the user's message in milliseconds
    // while background subagents continue running uninterrupted!
    if (active.waitInterrupts.size > 0) {
      this.flushChatPartial(chatSessionId, active);
      this.chats.appendTurn(chatSessionId, role, storedText, undefined, opts?.event);
      for (const interrupt of [...active.waitInterrupts]) {
        interrupt(modelText, role);
      }
      return true;
    }

    if (!active.handle) return false;
    const accepted = await active.handle.steer(modelText);
    if (accepted && this.activeChats.get(chatSessionId) === active && !active.abort.signal.aborted) {
      this.flushChatPartial(chatSessionId, active);
      this.chats.appendTurn(chatSessionId, role, storedText, undefined, opts?.event);
      return true;
    }
    return false;
  }

  getChatPartial(chatSessionId: string): string {
    return this.activeChats.get(chatSessionId)?.partial ?? "";
  }

  private flushChatPartial(chatSessionId: string, active: ActiveChat | undefined): ChatTurnRecord | null {
    if (!active?.partial) return null;
    const turn = this.chats.appendTurn(chatSessionId, "assistant", active.partial);
    active.flushedText += active.partial;
    active.partial = "";
    return turn;
  }

  private sumRunCostUsd(runId: string | null): number {
    if (!runId) return 0;
    const snap = summarizeCost(
      this.db,
      { chatSessionId: null, runIds: [runId], snapshotScope: { kind: "run", id: runId, label: runId } },
      this.config.profiles,
    );
    return snap.totals.find((t) => t.currency === "USD")?.amount ?? 0;
  }

  private directorExecutor(session: { id: string; repoRoot: string }, signal: AbortSignal, onToolEvent?: RunnerCallbacks["onToolEvent"]) {
    const presentations = new Map<string, { toolName: string; build: () => ToolDisplayPayload }>();
    return createCodeModeExecutor({
      ...this.config.codeExec,
      mode: effectiveCodeMode(this.config.codeMode),
      allowedTools: () => DIRECTOR_TOOLS,
      execute: async (name, args, execSignal) => {
        const effectiveSignal = execSignal ?? signal;
        if (name === "present_result") {
          effectiveSignal.throwIfAborted();
          const id = String(args.presentation_id);
          const selected = presentations.get(id);
          if (!selected) throw new Error("Unknown or already presented result in this turn");
          this.emitToolDisplay(session.id, selected.toolName, selected.build());
          presentations.delete(id);
          return { presented: true };
        }
        return this.executeDirectorTool(session.id, session.repoRoot, name, args, effectiveSignal, (build) => {
          const id = randomUUID();
          presentations.set(id, { toolName: name, build });
          return id;
        });
      },
      artifactsDir: resolve(session.repoRoot, this.config.worktree.artifactsDir ?? ".harness/artifacts"),
      pythonCacheDir: join(this.config.stateDir, "code-mode", "python"),
      saveOutput: async (name, json) => {
        signal.throwIfAborted();
        const run = await this.orchestrator.ensureDirectorTaskRun(session.id, session.repoRoot);
        signal.throwIfAborted();
        const [coordinator] = this.db.listTasks(run.id, { parentId: null });
        return writeCodeOutput(this.db, run, coordinator!.id, name, json);
      },
      signal,
      onToolEvent: onToolEvent && codeExecEventSink(onToolEvent),
    });
  }

  /** Reuse the active turn's executor so MCP and in-process calls share cancellation and tool events. */
  async executeDirectorMcpTool(chatSessionId: string, name: string, args: Record<string, unknown>): Promise<unknown> {
    let executor = this.activeChats.get(chatSessionId)?.executor;
    if (!executor) {
      const session = this.chats.getSession(chatSessionId);
      if (!session) throw new Error(`Unknown chat session ${chatSessionId}`);
      // Idle chats support discovery and legacy direct calls, but cannot launch a script outside a turn.
      executor = this.directorExecutor(session, AbortSignal.abort());
    }
    return name === "__list_tools" ? executor.tools() : executor.execute(name, args);
  }

  /** Executes a single interactive Tier 0 conversational turn and persists both user & assistant messages. */
  async sendChatMessage(
    chatSessionId: string,
    userMessage: string,
    onDelta?: (delta: string) => void,
    onToolEvent?: RunnerCallbacks["onToolEvent"],
    opts?: { turnRole?: "user" | "system_notice"; event?: ChatControlEvent },
  ): Promise<{ userTurn: ChatTurnRecord; assistantTurn: ChatTurnRecord | null }> {
    let session = this.chats.getSession(chatSessionId);
    if (!session) throw new Error(`Unknown chat session ${chatSessionId}`);

    if (this.activeChats.has(chatSessionId)) throw new Error(`Chat ${chatSessionId} already has an active turn`);
    // Reserve cancellable ActiveChat BEFORE awaiting cold catalog migration so Esc during migration cancels cleanly
    const abort = new AbortController();
    const { promise: settled, resolve: markSettled } = Promise.withResolvers<void>();
    const preExecutor = this.directorExecutor(session, abort.signal, onToolEvent);
    const active: ActiveChat = {
      abort,
      executor: preExecutor,
      onToolEvent,
      settled,
      partial: "",
      flushedText: "",
      waitInterrupts: new Set(),
    };
    this.activeChats.set(chatSessionId, active);

    let userTurn!: ChatTurnRecord;
    const finishCancelled = () => ({
      userTurn,
      assistantTurn: this.flushChatPartial(chatSessionId, active),
    });
    try {
      session = await this.resolveOrMigrateSessionModel(session, { strict: true });
      const typedEvent = opts?.event ?? null;
      if (!typedEvent) {
        // A fresh user message naturally supersedes any pending timer subscribed to `user_message` / `any`.
        for (const t of this.db.timers.list({ ownerKind: "chat", ownerId: chatSessionId, status: "pending" })) {
          if (t.wakeOnEvent === "user_message" || t.wakeOnEvent === "any") {
            this.db.timers.cancel(t.id, "superseded_by_user_message");
          }
        }
      }
      const turnRole = typedEvent ? "system_notice" : (opts?.turnRole ?? "user");
      const storedContent = typedEvent ? formatChatEventSummary(typedEvent) : userMessage;
      userTurn = this.chats.appendTurn(chatSessionId, turnRole, storedContent, undefined, typedEvent);
      if (abort.signal.aborted) {
        return { userTurn, assistantTurn: null };
      }
      const priorHistory = this.chats.getPromptHistory(chatSessionId, {
        conversationLimit: 24,
        systemLimit: 16,
        excludeSeq: userTurn.seq,
      });
      const allSessionGoals = this.goals.listGoals({ repoRoot: session.repoRoot, chatSessionId });
      const nonArchivedGoals = allSessionGoals.filter((g) => g.status !== "achieved" && g.status !== "cancelled");
      const archivedGoals = allSessionGoals.filter((g) => g.status === "achieved" || g.status === "cancelled");
      const bannerGoals = [...nonArchivedGoals, ...archivedGoals.slice(0, 6)];
      const omittedArchived = Math.max(0, archivedGoals.length - 6);

      const contextTranscript = priorHistory
        .map((t) => {
          const speaker = t.event
            ? `Event (${t.event.kind})`
            : t.role === "user"
              ? "User"
              : t.role === "assistant"
                ? "Director"
                : t.role === "tool_output"
                  ? "Tool output"
                  : "System";
          // tool_output turns contribute only their short summary; the display JSON stays out of the model context.
          return `${speaker}: ${t.content}`;
        })
        .join("\n\n");

      const goalBanner =
        bannerGoals.length > 0
          ? `\n\nActive/Recent Goals:\n${bannerGoals
              .map((g) => {
                const queued = g.status === "draft" && g.liveProgress?.phase === "queued" ? g.liveProgress.summary : undefined;
                const ver = queued ?? (g.reviewCycle > 0 ? `${g.reviewCycle} rev` : g.liveProgress?.verificationStatus ?? g.status);
                return `- ${g.id} v${g.version} [${g.status}] "${g.title}" (${ver})`;
              })
              .join("\n")}${
              omittedArchived > 0
                ? `\n- ... and ${omittedArchived} older completed/cancelled goal${omittedArchived === 1 ? "" : "s"} (use goal_list for all)`
                : ""
            }`
          : "";

      const currentTurnBlock = typedEvent
        ? `Control-plane event (${typedEvent.kind}):\n${formatChatEventForModel(typedEvent)}`
        : `User: ${userMessage}`;
      const prompt = `${contextTranscript ? `Prior conversation:\n${contextTranscript}\n\n` : ""}Workspace: ${session.repoRoot}${goalBanner}\n\n${currentTurnBlock}`;

      const mode = effectiveCodeMode(this.config.codeMode);
      const executor = this.directorExecutor(session, abort.signal, onToolEvent);
      active.executor = executor;
      const bridgeExt = import.meta.url.endsWith(".js") ? "mcp-bridge.js" : "mcp-bridge.ts";
      const mcpBridgeEntrypoint = join(dirname(fileURLToPath(import.meta.url)), "..", "runners", bridgeExt);

      const { profile, model, wireModel } = await resolveLaunchRoute(this.config, {
        tier: 0, cwd: session.repoRoot, profileId: session.profileId, model: session.model, catalog: this.catalog, quota: this.quota,
      });
      if (abort.signal.aborted) return finishCancelled();
      this.chats.updateSession(chatSessionId, { profileId: profile.id, model });
      const runner = this.customRunnerFactory
        ? this.customRunnerFactory(profile)
        : profile.kind === "acp" ? new AcpRunner(profile) : new PiRunner(profile);
      const cost = recordCostInvocation(this.db, {
        chatSessionId,
        activity: "director",
        profile,
        model,
        wireModel,
      });
      let res;
      try {
        res = await runner.run({
          runId: chatSessionId,
          taskId: chatSessionId,
          tier: 1,
          kind: "orchestrator",
          cwd: session.repoRoot,
          toolPolicy: "control_plane_only",
          systemPrompt: buildDirectorSystemPrompt(mode, this.config.codeExec),
          initialPrompt: prompt,
          model: wireModel,
          effort: session.effort,
          maxTurns: 0,
          quotaRetryTimeoutMs: this.config.quotaRetry.nativeRetryTimeoutMs,
          tools: executor.tools(),
          executeTool: executor.execute,
          daemonSocketPath: this.config.socketPath,
          mcpBridgeEntrypoint,
          signal: abort.signal,
          onHandle: (handle) => { if (!(runner instanceof PiRunner)) active.handle = handle; },
          callbacks: {
            onSessionId: (sid) => cost.onSessionId(sid),
            onTextDelta: (delta) => {
              if (abort.signal.aborted) return;
              active.partial += delta;
              onDelta?.(delta);
            },
            onToolEvent,
            onUsage: (usage) => cost.onUsage(usage),
            onPassiveQuota: (sig) => this.quota.ingestPassiveSignal(sig),
          },
        });
      } finally {
        cost.finish();
      }

      // ACP returns the whole turn. Pi can return only its last provider response.
      const finalSegment = res.text.startsWith(active.flushedText) ? res.text.slice(active.flushedText.length) : res.text;
      if (abort.signal.aborted || res.stopReason === "cancelled") {
        // On abort Pi can return the previous completed provider response while the
        // current response exists only in deltas. Only recover new text consistent with those deltas.
        const alreadyFlushed = active.flushedText.endsWith(res.text);
        if (res.stopReason !== "error" && !alreadyFlushed && finalSegment.startsWith(active.partial)) {
          active.partial = finalSegment;
        }
        return finishCancelled();
      }
      if (res.stopReason === "error") throw new Error(res.text.trim() || "Director runner failed");
      let content = (res.text ? finalSegment : active.partial).trim();
      if (res.stopReason === "quota_paused") {
        const pauseLine = "Paused for quota. Retry later or switch models with /model.";
        content = content ? `${content}\n\n${pauseLine}` : pauseLine;
      } else if (!content && res.toolFailures?.length) {
        content = `Stopped after tool failure without a final response:\n${res.toolFailures.map((f) => `- ${f}`).join("\n")}`;
      } else if (!content && !active.flushedText) {
        content = "(done)";
      }

      active.partial = content;
      const assistantTurn = this.flushChatPartial(chatSessionId, active);
      return { userTurn, assistantTurn };
    } catch (error) {
      if (abort.signal.aborted) return finishCancelled();
      this.flushChatPartial(chatSessionId, active);
      throw error;
    } finally {
      abort.abort();
      if (this.activeChats.get(chatSessionId) === active) {
        this.activeChats.delete(chatSessionId);
      }
      markSettled();
    }
  }

  /** Persists a tool_output turn and forwards the display to the chat's active UI sink, if any. */
  private emitToolDisplay(chatSessionId: string, toolName: string, display: ToolDisplayPayload): void {
    this.flushChatPartial(chatSessionId, this.activeChats.get(chatSessionId));
    this.chats.appendTurn(chatSessionId, "tool_output", summarizeToolDisplay(toolName, display), { toolName, display });
    this.activeChats.get(chatSessionId)?.onToolEvent?.({ id: `display-${randomUUID()}`, name: toolName, status: "completed", display });
  }

  private goalDisplayData(goal: GoalRecord) {
    const runId = goal.linkedRunId;
    return {
      goal,
      reviews: this.goals.listReviews(goal.id),
      tasks: runId ? this.db.listTasks(runId) : [],
      openEscalations: runId ? this.db.listEscalations(runId, "open") : [],
      activity: runId ? buildRunActivitySummary(this.db, runId) : null,
    };
  }

  private async executeDirectorTool(
    chatSessionId: string,
    repoRoot: string,
    name: string,
    args: Record<string, unknown>,
    signal: AbortSignal,
    offerPresentation: (build: () => ToolDisplayPayload) => string,
  ): Promise<unknown> {
    const presentGoal = (goal: GoalRecord) => offerPresentation(() => buildGoalDetailDisplay(this.goalDisplayData(goal)));
    if (args.goal_id !== undefined) {
      const goal = this.goals.getGoal(String(args.goal_id));
      if (!goal || realpathSync(goal.repoRoot) !== realpathSync(repoRoot)) throw new Error("Unknown goal in this workspace");
    }
    if (name === "goal_commit" || (name === "goal_create" && args.commit_immediately)) {
      if (typeof args.model !== "string" || !args.model.trim()) throw new Error("Select a model from list_models to commit this goal");
      if (args.profile_id !== undefined && typeof args.profile_id !== "string") throw new Error("profile_id must be a string");
    }
    switch (name) {
      case "wait_children":
      case "wait_tasks": {
        const active = this.activeChats.get(chatSessionId);
        if (!active) {
          return this.orchestrator.executeDirectorTaskTool(chatSessionId, repoRoot, "wait_tasks", args, signal);
        }
        const waitAbort = new AbortController();
        const combinedSignal = signal ? AbortSignal.any([signal, waitAbort.signal]) : waitAbort.signal;
        let userInterruption: { message: string; role: "user" | "system_notice" } | null = null;
        const onInterrupt = (message: string, role: "user" | "system_notice") => {
          userInterruption = { message, role };
          waitAbort.abort(new Error("user_message"));
        };
        active.waitInterrupts.add(onInterrupt);
        try {
          return await this.orchestrator.executeDirectorTaskTool(chatSessionId, repoRoot, "wait_tasks", args, combinedSignal);
        } catch (err) {
          if (userInterruption) {
            const inter = userInterruption as { message: string; role: "user" | "system_notice" };
            return {
              wakeReason: inter.role === "user" ? "user_message" : "system_event",
              userMessage: inter.message,
              backgroundStillRunning: true,
              hint: "A new message arrived while waiting on background subagent(s). Those subagents are STILL running uninterrupted. Handle the user's message directly (or ask for details / adjust work) and end your turn cleanly.",
            };
          }
          throw err;
        } finally {
          active.waitInterrupts.delete(onInterrupt);
        }
      }
      case "spawn_task":
      case "artifact_read":
        return this.orchestrator.executeDirectorTaskTool(chatSessionId, repoRoot, name, args, signal);
      case "cancel_task": {
        const targetId = String(args.task_id ?? "").trim();
        if (!targetId) throw new Error("task_id is required");
        const targetTask = this.db.getTask(targetId);
        if (targetTask) {
          const targetRun = this.db.getRun(targetTask.runId);
          // Allow cancelling ANY task in this workspace/goal run, not just synthetic Director tasks!
          if (targetRun && targetRun.repoRoot === repoRoot) {
            await this.orchestrator.cancelTask(targetTask.id, typeof args.reason === "string" ? args.reason : "Cancelled by Director");
            return { cancelled: targetTask.id, runId: targetTask.runId };
          }
        }
        return this.orchestrator.executeDirectorTaskTool(chatSessionId, repoRoot, name, args, signal);
      }
      case "steer_task": {
        const targetId = String(args.task_id ?? "").trim();
        const message = String(args.message ?? "").trim();
        if (!targetId || !message) throw new Error("task_id and message are required");
        const targetTask = this.db.getTask(targetId);
        if (!targetTask) throw new Error(`Unknown task ${targetId}`);
        const steered = await this.orchestrator.steerTask(targetTask.id, message);
        return { steered, taskId: targetTask.id, runId: targetTask.runId };
      }
      case "goal_accept": {
        const goalId = String(args.goal_id ?? "").trim();
        const reason = String(args.reason ?? "").trim();
        const evidencePaths = Array.isArray(args.evidence_paths)
          ? (args.evidence_paths as unknown[]).map((p) => String(p).trim()).filter(Boolean)
          : [];
        if (!goalId || !reason) throw new Error("goal_accept requires goal_id and reason");
        if (evidencePaths.length === 0) throw new Error("goal_accept requires non-empty evidence_paths");
        const goal = this.goals.getGoal(goalId);
        if (!goal) throw new Error(`Unknown goal ${goalId}`);
        const evidenceSummary = `Evidence: ${evidencePaths.join(", ")}`;
        const outcome = this.goals.recordReviewVerdict(goal.id, {
          verdict: "approved",
          criteriaResults: goal.criteria.map((c) => ({
            criterionId: c.id,
            passed: true,
            evidence: `[DIRECTOR ACCEPTED] ${reason} (${evidenceSummary})`,
          })),
          blockingIssues: [],
          summary: `[DIRECTOR ACCEPTED OVERRIDE] ${reason} (${evidenceSummary})`,
          diffHash: `director-accept-${Date.now()}`,
          spentUsd: this.sumRunCostUsd(goal.linkedRunId),
        });
        await this.handlePostReviewTransition(outcome, { autoTriggered: false });
        return { ...outcome, presentation_id: presentGoal(outcome.goal) };
      }
      case "write_note": {
        const rawPath = String(args.path ?? "").trim();
        const content = String(args.content ?? "");
        const reason = String(args.reason ?? "").trim();
        if (!rawPath || !reason) throw new Error("write_note requires path and reason");
        const root = realpathSync(repoRoot);
        const expandRoot = (r: string) => {
          const t = r.trim();
          const exp = t === "~" ? homedir() : t.startsWith("~/") ? resolve(homedir(), t.slice(2)) : resolve(t);
          return existsSync(exp) ? realpathSync(exp) : exp;
        };
        const allowedWriteRoots = [
          join(root, ".harness", "goals"),
          join(root, ".harness", "notes"),
          ...(this.config.worktree.extraWriteRoots ?? []).map(expandRoot),
        ];
        const expandedTarget = rawPath.startsWith("~/") ? resolve(homedir(), rawPath.slice(2)) : resolve(root, rawPath);
        const matchedRoot = allowedWriteRoots.find((allowed) => {
          const rel = relative(allowed, expandedTarget);
          return rel !== "" && rel !== ".." && !rel.startsWith(`..${sep}`) && !isAbsolute(rel);
        });
        if (!matchedRoot) {
          throw new Error("write_note path must be inside .harness/goals/, .harness/notes/, or a configured extraWriteRoots directory");
        }
        mkdirSync(dirname(expandedTarget), { recursive: true });
        const realParent = realpathSync(dirname(expandedTarget));
        const parentRel = relative(existsSync(matchedRoot) ? realpathSync(matchedRoot) : matchedRoot, realParent);
        if (parentRel === ".." || parentRel.startsWith(`..${sep}`) || isAbsolute(parentRel)) {
          throw new Error("write_note parent symlink escapes allowed write root");
        }
        writeFileSync(expandedTarget, content, "utf8");
        this.chats.appendTurn(
          chatSessionId,
          "system_notice",
          `Director write_note: ${rawPath} (${Buffer.byteLength(content, "utf8")} bytes). Reason: ${reason}`,
        );
        return { written: true, path: expandedTarget, bytes: Buffer.byteLength(content, "utf8") };
      }
      case "goal_create": {
        const criteria = (Array.isArray(args.criteria) ? args.criteria : []) as Array<
          Omit<GoalCriterion, "id" | "required"> & { id?: string; required?: boolean }
        >;
        const created = this.goals.createGoal({
          repoRoot,
          chatSessionId,
          title: String(args.title),
          description: String(args.description),
          criteria,
          nonGoals: Array.isArray(args.non_goals) ? (args.non_goals as string[]) : [],
          verificationMode:
            typeof args.verification_mode === "string" &&
            ["auto", "none", "deterministic", "independent_review"].includes(args.verification_mode)
              ? (args.verification_mode as VerificationMode)
              : undefined,
          budgets:
            typeof args.max_review_cycles === "number" ? { maxReviewCycles: args.max_review_cycles } : undefined,
        });
        if (args.commit_immediately) {
          const committed = await this.commitGoal(created.id, {
            model: String(args.model), profileId: args.profile_id as string | undefined,
            effort: typeof args.effort === "string" ? args.effort : undefined,
          });
          const qEntry = this.db.admissionQueue.get("goal", committed.id);
          return {
            ...committed,
            ...(qEntry ? { queued: true, queuePosition: qEntry.position } : {}),
            presentation_id: presentGoal(committed),
          };
        }
        return { ...created, presentation_id: presentGoal(created) };
      }

      case "goal_commit": {
        const committed = await this.commitGoal(String(args.goal_id), {
          profileId: typeof args.profile_id === "string" ? args.profile_id : undefined,
          model: typeof args.model === "string" ? args.model : undefined,
          effort: typeof args.effort === "string" ? args.effort : undefined,
          position: args.position as QueuePositionInput | undefined,
          forceStart: Boolean(args.force_start),
        });
        const qEntry = this.db.admissionQueue.get("goal", committed.id);
        return {
          ...committed,
          ...(qEntry ? { queued: true, queuePosition: qEntry.position } : {}),
          presentation_id: presentGoal(committed),
        };
      }

      case "queue_list": {
        const lane = args.lane === "goal" || args.lane === "worker" ? (args.lane as AdmissionLane) : undefined;
        this.syncQueuedGoalProgress();
        const entries = this.db.admissionQueue.list({ lane, repoRoot });
        return {
          repoRoot,
          activeGoals: this.countActiveGoals(repoRoot),
          limits: {
            maxConcurrentGoals: this.config.goals.maxConcurrentGoals ?? 4,
            maxConcurrentWorkers: this.config.worktree.maxConcurrentWorkers ?? 4,
          },
          entries,
        };
      }

      case "queue_reorder": {
        const targetId = String(args.target_id ?? "").trim();
        if (!targetId) throw new Error("target_id is required");
        const lane: AdmissionLane =
          args.lane === "goal" || args.lane === "worker"
            ? args.lane
            : targetId.startsWith("goal-") || this.goals.getGoal(targetId)
              ? "goal"
              : "worker";
        const updated = this.db.admissionQueue.reorder(lane, targetId, args.position as QueuePositionInput);
        return { reordered: updated, queue: this.db.admissionQueue.list({ lane, repoRoot: updated.repoRoot }) };
      }

      case "queue_force_start": {
        const targetId = String(args.target_id ?? "").trim();
        if (!targetId) throw new Error("target_id is required");
        const lane = args.lane === "goal" || args.lane === "worker" ? (args.lane as AdmissionLane) : undefined;
        return await this.forceStartQueuedItem({
          lane,
          targetId,
          bypassQuota: Boolean(args.bypass_quota),
          reason: typeof args.reason === "string" ? args.reason : undefined,
        });
      }

      case "goal_amend": {
        const amended = await this.amendGoal(String(args.goal_id), {
          rationale: String(args.rationale),
          title: typeof args.title === "string" ? args.title : undefined,
          description: typeof args.description === "string" ? args.description : undefined,
          addCriteria: Array.isArray(args.add_criteria) ? (args.add_criteria as GoalCriterion[]) : undefined,
          updateCriteria: Array.isArray(args.update_criteria) ? (args.update_criteria as GoalCriterion[]) : undefined,
          removeCriteriaIds: Array.isArray(args.remove_criteria_ids) ? (args.remove_criteria_ids as string[]) : undefined,
          nonGoals: Array.isArray(args.non_goals) ? (args.non_goals as string[]) : undefined,
        });
        return { ...amended, presentation_id: presentGoal(amended.goal) };
      }

      case "goal_list": {
        const allProjects = Boolean(args.all_projects);
        this.syncQueuedGoalProgress();
        const goals = allProjects ? this.goals.listGoals({}) : this.goals.listGoals({ repoRoot, chatSessionId });
        const presentation_id = offerPresentation(() => buildGoalsTableDisplay(goals.map((goal) => this.goalDisplayData(goal))));
        return { goals, presentation_id };
      }

      case "project_list": {
        const projects = await this.db.projects.listProjects();
        const curSess = this.chats.getSession(chatSessionId);
        return { currentProjectId: curSess?.projectId ?? null, currentPath: repoRoot, projects };
      }

      case "project_switch": {
        const ref = String(args.project ?? "").trim();
        if (!ref) throw new Error("project is required");
        const switched = this.switchChatProject(chatSessionId, ref);
        return {
          switched: true,
          project: switched.project,
          sessionId: switched.session.id,
          repoRoot: switched.session.repoRoot,
        };
      }

      case "remote_list": {
        if (!this.peerBridge) return { remotes: [] };
        return { remotes: this.peerBridge.listRemotes() };
      }

      case "session_list": {
        if (!this.peerBridge) {
          const chats = this.chats.listSessions(25).map((c) => ({
            host: "local",
            address: `local:${c.id}`,
            id: c.id,
            title: c.title,
            projectId: c.projectId,
            repoRoot: c.repoRoot,
            updatedAt: c.updatedAt,
          }));
          return { sessions: chats };
        }
        const host = typeof args.host === "string" && args.host.trim() ? args.host.trim() : undefined;
        return { sessions: await this.peerBridge.listSessions(host) };
      }

      case "session_create": {
        if (!this.peerBridge) {
          const created = await this.createChat({
            repoRoot,
            title: typeof args.title === "string" ? args.title : undefined,
          });
          return { host: "local", address: `local:${created.id}`, session: created };
        }
        return await this.peerBridge.createSession({
          host: typeof args.host === "string" && args.host.trim() ? args.host.trim() : undefined,
          project: typeof args.project === "string" && args.project.trim() ? args.project.trim() : undefined,
          title: typeof args.title === "string" && args.title.trim() ? args.title.trim() : undefined,
        });
      }

      case "session_send": {
        if (!this.peerBridge) throw new Error("Cross-session messaging is unavailable in this runtime");
        const target = String(args.target ?? "").trim();
        const body = String(args.message ?? "").trim();
        if (!target) throw new Error("target is required (e.g. 'chat-2', 'local:chat-2', or 'gpu-box:chat-4')");
        if (!body) throw new Error("message is required");
        return await this.peerBridge.sendMessage({
          fromSessionId: chatSessionId,
          target,
          body,
          expectReply: args.expect_reply === undefined ? true : Boolean(args.expect_reply),
          replyToId: typeof args.reply_to_id === "string" && args.reply_to_id.trim() ? args.reply_to_id.trim() : undefined,
        });
      }

      case "session_read": {
        if (!this.peerBridge) throw new Error("Cross-session reading is unavailable in this runtime");
        const target = String(args.target ?? "").trim();
        if (!target) throw new Error("target is required");
        return await this.peerBridge.readSession({
          target,
          limit: typeof args.limit === "number" ? args.limit : undefined,
        });
      }

      case "report_goal_progress": {
        const g = this.goals.reportProgress(String(args.goal_id), {
          summary: String(args.summary ?? "").trim() || undefined,
          phase: typeof args.phase === "string" ? args.phase.trim() || undefined : undefined,
          verificationStatus:
            typeof args.verification_status === "string" &&
            ["not_needed", "in_progress", "passed", "failed"].includes(args.verification_status)
              ? (args.verification_status as "not_needed" | "in_progress" | "passed" | "failed")
              : undefined,
          criteriaResults: Array.isArray(args.criteria_results)
            ? (args.criteria_results as Array<Record<string, unknown>>)
                .filter((r) => typeof r?.criterion_id === "string")
                .map((r) => ({
                  criterionId: String(r.criterion_id),
                  passed: Boolean(r.passed),
                  evidence: String(r.evidence ?? ""),
                  ...(typeof r.remediation === "string" ? { remediation: r.remediation } : {}),
                }))
            : undefined,
          reportedByTaskId: chatSessionId,
        });
        if (g.linkedRunId) {
          this.db.emitEvent(g.linkedRunId, null, "goal.progress", { goalId: g.id, ...g.liveProgress });
        }
        return { goal: g };
      }

      case "goal_get": {
        this.syncQueuedGoalProgress();
        const g = this.goals.getGoal(String(args.goal_id));
        if (!g) throw new Error(`Unknown goal ${args.goal_id}`);
        const detail = this.goalDisplayData(g);
        return {
          ...detail,
          tasks: detail.tasks.map((t) => compactTaskHandoff(t, detail.openEscalations)),
          presentation_id: offerPresentation(() => buildGoalDetailDisplay(detail)),
        };
      }

      case "goal_wait": {
        const goalId = String(args.goal_id);
        const goal = this.goals.getGoal(goalId);
        if (!goal) throw new Error(`Unknown goal ${goalId}`);
        const defaultSec = this.config.events?.fallbackWakeupIntervalSec ?? 600;
        const fallbackMs = Math.max(
          1,
          typeof args.fallback_interval_s === "number" ? args.fallback_interval_s : defaultSec,
        ) * 1000;
        return await this.waitForGoalEvent(goalId, fallbackMs, signal, chatSessionId);
      }

      case "goal_trigger_review": {
        // Decouple Done-Reviewer lifecycle from the Director chat turn's AbortSignal so ending the turn
        // or a Code Mode script timeout NEVER kills an in-flight verification review (cancel only via goal amend/stop/shutdown).
        const reviewed = await this.verifyAndReviewGoal(String(args.goal_id), undefined, {
          verificationMode:
            typeof args.verification_mode === "string" &&
            ["auto", "none", "deterministic", "independent_review"].includes(args.verification_mode)
              ? (args.verification_mode as VerificationMode)
              : undefined,
          summary: typeof args.summary === "string" ? args.summary : undefined,
        });
        return { ...reviewed, presentation_id: presentGoal(reviewed.goal) };
      }

      case "goal_pause": {
        const gid = String(args.goal_id);
        this.db.admissionQueue.remove("goal", gid);
        const g = this.goals.setStatus(gid, "paused", "Paused by Director");
        if (g.linkedRunId) await this.orchestrator.stopRun(g.linkedRunId, "Goal paused");
        void this.drainGoalQueue(g.repoRoot).catch(() => {});
        return { ...g, presentation_id: presentGoal(g) };
      }

      case "goal_cancel": {
        const gid = String(args.goal_id);
        this.db.admissionQueue.remove("goal", gid);
        const g = this.goals.setStatus(gid, "cancelled", "Cancelled by Director");
        if (g.linkedRunId) {
          await this.orchestrator.stopRun(g.linkedRunId, "Goal cancelled");
          this.autoResolveGoalEscalations(g.linkedRunId, `Auto-resolved: goal ${g.id} cancelled`);
        }
        void this.drainGoalQueue(g.repoRoot).catch(() => {});
        return { ...g, presentation_id: presentGoal(g) };
      }

      case "read_file": {
        const reason = typeof args.exception_reason === "string" ? args.exception_reason.trim() : "";
        if (!reason) throw new Error("read_file requires exception_reason");
        const rawPath = String(args.path ?? "").trim();
        const root = realpathSync(repoRoot);
        const expandRoot = (r: string) => {
          const t = r.trim();
          const exp = t === "~" ? homedir() : t.startsWith("~/") ? resolve(homedir(), t.slice(2)) : resolve(t);
          return existsSync(exp) ? realpathSync(exp) : exp;
        };
        const candidate = rawPath.startsWith("~/") ? resolve(homedir(), rawPath.slice(2)) : resolve(root, rawPath);
        const target = realpathSync(candidate);
        const extraReadRoots = (this.config.worktree.extraReadRoots ?? []).map(expandRoot);
        const relToRepo = relative(root, target);
        const inRepo = relToRepo !== ".." && !relToRepo.startsWith(`..${sep}`) && !isAbsolute(relToRepo);
        const path = inRepo ? relToRepo : target;
        const inExtraRoot = extraReadRoots.some((r) => {
          const rel = relative(r, target);
          return rel !== ".." && !rel.startsWith(`..${sep}`) && !isAbsolute(rel);
        });
        if (!inRepo && !inExtraRoot) {
          throw new Error("File outside the permitted workspace or extraReadRoots");
        }
        if (inRepo) {
          const parts = relToRepo.split(sep).map((p) => p.toLowerCase());
          // Block .git and .harness/worktrees, while permitting .harness/goals, .harness/artifacts, and .harness/notes!
          if (parts.includes(".git")) {
            throw new Error("File outside the permitted workspace");
          }
          if (parts[0] === ".harness" && !["artifacts", "goals", "notes"].includes(parts[1] ?? "")) {
            throw new Error("File outside the permitted workspace");
          }
        }
        const fd = openSync(target, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
        try {
          const stat = fstatSync(fd);
          if (!stat.isFile()) throw new Error("Expected a regular file");
          const bytes = Buffer.alloc(4096);
          const count = readSync(fd, bytes, 0, bytes.length, 0);
          const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, count), { stream: stat.size > count });
          const limit = typeof args.max_chars === "number" ? Math.min(4096, Math.max(0, args.max_chars)) : 4096;
          const content = text.slice(0, limit);
          const readEvent: ChatControlEvent = { kind: "director.read_file", path, bytes: count, reason };
          this.chats.appendTurn(chatSessionId, "system_notice", formatChatEventSummary(readEvent), undefined, readEvent);
          return { path, truncated: stat.size > count || content.length < text.length, content };
        } finally {
          closeSync(fd);
        }
      }

      case "kb_search":
        return {
          cards: this.db.kb.search({
            query: typeof args.query === "string" ? args.query : undefined,
            repoScope: repoRoot,
            pathGlobs: Array.isArray(args.path_globs) ? (args.path_globs as string[]) : undefined,
          }),
        };

      case "list_models": {
        const session = this.chats.getSession(chatSessionId);
        const catalog = await this.catalog.getCatalog({
          cwd: repoRoot,
          excludeExhausted: Boolean(args.exclude_exhausted),
        });
        return {
          ...catalog,
          sessionModelContext: session?.modelSelectionContext ?? null,
          recommendedByKind: recommendModelsByTaskKind(catalog.models, { avoidModelId: session?.model }),
        };
      }

      case "set_model_selection_context": {
        const raw = typeof args.context === "string" ? args.context.trim() : "";
        const updated = this.chats.updateSession(chatSessionId, {
          modelSelectionContext: raw || null,
        });
        return {
          chatSessionId,
          sessionModelContext: updated.modelSelectionContext,
        };
      }

      case "run_status": {
        let runId = typeof args.run_id === "string" && args.run_id.trim() ? args.run_id.trim() : "";
        if (!runId && typeof args.goal_id === "string" && args.goal_id.trim()) {
          const g = this.goals.getGoal(args.goal_id.trim());
          if (!g) throw new Error(`Unknown goal ${args.goal_id}`);
          if (!g.linkedRunId) throw new Error(`Goal ${g.id} has no linked run yet`);
          runId = g.linkedRunId;
        }
        if (!runId) throw new Error("run_status requires run_id or goal_id");
        const run = this.orchestrator.getEffectiveRun(runId);
        if (!run) throw new Error(`Unknown run ${runId}`);
        const activity = buildRunActivitySummary(this.db, runId);
        return { run, activity, presentation_id: offerPresentation(() => buildRunActivityDisplay(activity)) };
      }

      case "steer_run": {
        const g = this.goals.getGoal(String(args.goal_id));
        if (!g?.linkedRunId) throw new Error(`Goal ${args.goal_id} has no active run`);
        const orch = this.db.listTasks(g.linkedRunId).find((t) => t.tier === 1);
        if (!orch) throw new Error("Orchestrator task not found");
        return { steered: await this.orchestrator.steerTask(orch.id, String(args.message)) };
      }

      case "resume_task": {
        const resumed = await this.orchestrator.resumeTask(String(args.task_id), {
          note: typeof args.note === "string" ? args.note : undefined,
          model: typeof args.model === "string" ? args.model : undefined,
          effort: typeof args.effort === "string" ? args.effort : undefined,
          profileId: typeof args.profile_id === "string" && args.profile_id.trim() ? args.profile_id.trim() : undefined,
        });
        return { resumed: true, task: resumed };
      }

      case "resume_run": {
        let runId = typeof args.run_id === "string" && args.run_id.trim() ? args.run_id.trim() : "";
        if (!runId && typeof args.goal_id === "string" && args.goal_id.trim()) {
          const g = this.goals.getGoal(args.goal_id.trim());
          if (!g) throw new Error(`Unknown goal ${args.goal_id}`);
          if (!g.linkedRunId) throw new Error(`Goal ${g.id} has no linked run yet`);
          runId = g.linkedRunId;
        }
        if (!runId) throw new Error("resume_run requires run_id or goal_id");
        const res = await this.orchestrator.resumeRun(runId, {
          trigger: "manual",
          note: typeof args.note === "string" ? args.note : undefined,
        });
        return {
          ...res,
          presentation_id: offerPresentation(() => buildRunActivityDisplay(buildRunActivitySummary(this.db, res.run.id))),
        };
      }

      case "schedule_timer": {
        let goalId: string | null = null;
        let runId: string | null = null;
        let pausedGoal = false;
        if (typeof args.goal_id === "string" && args.goal_id.trim()) {
          const g = this.goals.getGoal(args.goal_id.trim());
          if (!g) throw new Error(`Unknown goal ${args.goal_id}`);
          goalId = g.id;
          runId = g.linkedRunId;
          if (args.pause_goal === true && g.status !== "paused" && g.status !== "achieved" && g.status !== "cancelled") {
            this.goals.setStatus(g.id, "paused", "Paused for scheduled timer");
            if (g.linkedRunId) await this.orchestrator.stopRun(g.linkedRunId, "Goal paused for timer");
            pausedGoal = true;
          }
        }
        const wakeOnEvent =
          args.wake_on_event === "user_message" || args.wake_on_event === "goal_settled" || args.wake_on_event === "any"
            ? args.wake_on_event
            : "none";
        const timer = this.db.timers.schedule({
          ownerKind: "chat",
          ownerId: chatSessionId,
          runId,
          goalId,
          delaySeconds: typeof args.delay_seconds === "number" ? args.delay_seconds : undefined,
          wakeAtIso: typeof args.wake_at === "string" ? args.wake_at : undefined,
          reason: String(args.reason ?? "Scheduled wakeup"),
          wakeOnEvent,
          resumeGoal: Boolean(args.resume_goal),
        });
        const schedEvent: ChatControlEvent = {
          kind: "timer.scheduled",
          timerId: timer.id,
          wakeAt: timer.wakeAt,
          reason: timer.reason,
          ...(goalId ? { goalId } : {}),
          ...(wakeOnEvent !== "none" ? { wakeOnEvent } : {}),
          ...(pausedGoal ? { pausedGoal: true } : {}),
        };
        this.chats.appendTurn(chatSessionId, "system_notice", formatChatEventSummary(schedEvent), undefined, schedEvent);
        return {
          timer,
          pausedGoal,
          wakeAtIso: new Date(timer.wakeAt).toISOString(),
        };
      }

      case "list_timers": {
        const status =
          args.status === "pending" || args.status === "fired" || args.status === "cancelled" ? args.status : undefined;
        return { timers: this.db.timers.list({ ownerKind: "chat", ownerId: chatSessionId, status }) };
      }

      case "cancel_timer": {
        const cancelled = this.db.timers.cancel(
          String(args.timer_id),
          typeof args.reason === "string" ? args.reason : "cancelled_by_director",
        );
        if (!cancelled) throw new Error(`Unknown timer ${args.timer_id}`);
        return { timer: cancelled };
      }

      default:
        throw new Error(`Unknown Director tool: ${name}`);
    }
  }

  /**
   * Event-driven wait for a Goal's linked run to settle (or raise a P0 escalation), with a
   * long configurable fallback watchdog interval that checks live execution health without polling.
   */
  async waitForGoalEvent(
    goalId: string,
    fallbackIntervalMs = (this.config.events?.fallbackWakeupIntervalSec ?? 600) * 1000,
    signal?: AbortSignal,
    chatSessionId?: string,
  ): Promise<{
    goal: GoalRecord;
    runStatus: string | null;
    wakeReason: "already_settled" | "event" | "fallback_interval" | "run_inactive" | "user_message" | "system_event";
    userMessage?: string;
    backgroundStillRunning?: boolean;
    hint?: string;
    openEscalations: ReturnType<ControlPlaneDb["listEscalations"]>;
  }> {
    signal?.throwIfAborted();
    const initial = this.goals.getGoal(goalId);
    if (!initial) throw new Error(`Unknown goal ${goalId}`);

    const snapshot = () => {
      const g = this.goals.getGoal(goalId) ?? initial;
      const run = g.linkedRunId ? this.orchestrator.getEffectiveRun(g.linkedRunId) : null;
      const openEscalations = g.linkedRunId ? this.db.listEscalations(g.linkedRunId, "open") : [];
      const settledGoal = ["achieved", "awaiting_human", "paused", "cancelled"].includes(g.status);
      const settledRun = !run || ["completed", "failed", "stopped", "paused_quota"].includes(run.status);
      const hasP0 = openEscalations.some((e) => e.severity === "P0");
      return { g, run, openEscalations, done: settledGoal || settledRun || hasP0 };
    };

    const first = snapshot();
    if (first.done) {
      return {
        goal: first.g,
        runStatus: first.run?.status ?? null,
        wakeReason: "already_settled",
        openEscalations: first.openEscalations,
      };
    }

    const activeChat = chatSessionId ? this.activeChats.get(chatSessionId) : undefined;
    return new Promise((resolve, reject) => {
      let done = false;
      const finish = (
        wakeReason: "event" | "fallback_interval" | "run_inactive" | "user_message" | "system_event",
        extra?: { userMessage?: string; backgroundStillRunning?: boolean; hint?: string },
      ) => {
        if (done) return;
        done = true;
        unsub();
        clearTimeout(timer);
        if (activeChat) activeChat.waitInterrupts.delete(onInterrupt);
        signal?.removeEventListener("abort", onAbort);
        if (signal?.aborted) {
          reject(signal.reason);
          return;
        }
        const cur = snapshot();
        resolve({
          goal: cur.g,
          runStatus: cur.run?.status ?? null,
          wakeReason,
          ...(extra ?? {}),
          openEscalations: cur.openEscalations,
        });
      };
      const onInterrupt = (message: string, role: "user" | "system_notice") => {
        finish(role === "user" ? "user_message" : "system_event", {
          userMessage: message,
          backgroundStillRunning: true,
          hint: "The user sent a message while you were waiting on this Goal. Its background run is STILL executing uninterrupted — respond to the user directly (or ask for details / adjust work) and end your turn cleanly; the daemon will wake you automatically when the Goal finishes.",
        });
      };
      const onAbort = () => finish("event");
      if (activeChat) activeChat.waitInterrupts.add(onInterrupt);
      const unsub = this.db.onEvent((ev) => {
        const cur = snapshot();
        if (ev.runId && cur.g.linkedRunId && ev.runId !== cur.g.linkedRunId) return;
        if (cur.done) finish("event");
      });
      const timer = setTimeout(() => {
        const cur = snapshot();
        if (cur.done) {
          finish("event");
          return;
        }
        const runId = cur.g.linkedRunId;
        if (runId && !this.orchestrator.isRunActiveInMemory(runId)) {
          this.orchestrator.reconcileRunStatus(runId);
          finish("run_inactive");
          return;
        }
        finish("fallback_interval");
      }, Math.max(50, fallbackIntervalMs));
      timer.unref?.();
      signal?.addEventListener("abort", onAbort, { once: true });
      if (signal?.aborted) onAbort();
    });
  }
}

/**
 * Extracts a structured verdict JSON object from reviewer prose without greedy regex bugs
 * when the reviewer quotes code braces outside the JSON block.
 */
export function parseReviewerVerdictJson<T extends { verdict?: unknown }>(text: string): T | null {
  const isValid = (parsed: unknown): parsed is T =>
    Boolean(
      parsed &&
        typeof parsed === "object" &&
        !Array.isArray(parsed) &&
        typeof (parsed as { verdict?: unknown }).verdict === "string" &&
        ["approved", "changes_requested", "escalate_human"].includes(String((parsed as { verdict: string }).verdict)),
    );

  // 1. Prefer the last fenced ```json ... ``` (or ``` ... ```) block containing a valid verdict object
  const fenceMatches = [...text.matchAll(/```(?:json)?\s*\n([\s\S]*?)```/gi)];
  for (let i = fenceMatches.length - 1; i >= 0; i--) {
    try {
      const parsed = JSON.parse(fenceMatches[i]![1]!.trim());
      if (isValid(parsed)) return parsed;
    } catch {
      // keep scanning earlier fences
    }
  }

  // 2. Fall back to scanning balanced top-level `{ ... }` objects from the end of the text
  const candidates: string[] = [];
  let depth = 0;
  let start = -1;
  let inString = false;
  let escaped = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i]!;
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === "\\") escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') {
      inString = true;
      continue;
    }
    if (ch === "{") {
      if (depth === 0) start = i;
      depth++;
    } else if (ch === "}" && depth > 0) {
      depth--;
      if (depth === 0 && start >= 0) {
        candidates.push(text.slice(start, i + 1));
        start = -1;
      }
    }
  }
  for (let i = candidates.length - 1; i >= 0; i--) {
    try {
      const parsed = JSON.parse(candidates[i]!);
      if (isValid(parsed)) return parsed;
    } catch {
      // ignore non-JSON brace blocks (e.g. code snippets)
    }
  }
  return null;
}
