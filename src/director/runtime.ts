import { randomUUID } from "node:crypto";
import { accessSync, closeSync, constants, existsSync, fstatSync, ftruncateSync, lstatSync, mkdirSync, openSync, readSync, realpathSync, statSync, writeFileSync } from "node:fs";
import { open as openFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import {
  compareSemanticVersions,
  parseClaudeFamilyVersion,
  recommendModelsByTaskKind,
  type CompiledModelEntry,
  type ModelCatalogCompiler,
} from "../catalog/compiler.js";
import { vendorSuccessors } from "../catalog/lifecycle.js";
import { createCodeModeExecutor, effectiveCodeMode } from "../code-mode/index.js";
import type { HarnessConfig } from "../config.js";
import { resolveLaunchRoute } from "../control-plane/launch-route.js";
import { compactTaskHandoff, toolsForTier } from "../control-plane/tools.js";
import { findRunDossier, readRunDossier, writeCodeOutput, writeTaskDossier } from "../control-plane/dossiers.js";
import type { AdmissionLane, QueuePositionInput } from "../control-plane/admission-queue.js";
import type { ArtifactRecord, ControlPlaneDb, EscalationRecord, EventRecord, RunRecord } from "../control-plane/db.js";
import { RunStartStoppedError, type CustomRunnerFactory, type HarnessOrchestrator } from "../control-plane/orchestrator.js";
import { directorTaskRunOwner } from "../control-plane/ids.js";
import { canonicalProjectPath, type ProjectRecord } from "../control-plane/projects.js";
import { buildRunActivitySummary } from "../control-plane/telemetry.js";
import { recordCostInvocation, summarizeCost } from "../cost/ledger.js";
import { timerMatchesGoalSettlement, type TimerRecord } from "../control-plane/timers.js";
import {
  ensureGoalEvidenceDir,
  GoalEngine,
  parseGoalCriterionPatches,
  shouldRunIndependentReview,
  type CriterionEvaluation,
  type GoalAcceptOverrideAudit,
  type GoalCriterion,
  type GoalRecord,
  type GoalReviewVerdict,
  type VerificationMode,
} from "../goals/engine.js";
import { isAuthoritativeSource, isBlockedPhase, isCurrentClaim, type GoalLiveProgress, type GoalProgressSource } from "../goals/live-progress.js";
import {
  formatChatEventForModel,
  formatChatEventSummary,
  isReviewInvalidated,
  type ChatControlEvent,
} from "../protocol/chat-events.js";
import {
  buildGoalDetailDisplay,
  buildGoalsTableDisplay,
  buildRunActivityDisplay,
  codeExecEventSink,
  deriveGoalVerificationLabel,
  reconcileGoalWithLiveRun,
  summarizeToolDisplay,
  type ToolDisplayPayload,
} from "../protocol/tool-display.js";
import type { QuotaClient } from "../quota/client.js";
import { AcpRunner } from "../runners/acp-runner.js";
import { PiRunner } from "../runners/pi-runner.js";
import { isProtectedWritePath } from "../runners/protected-paths.js";
import type { RunnerCallbacks, RunnerExecutionHandle, ToolDefinition } from "../runners/types.js";
import { nativeToolsPathDirs, resolveExternalMcpServers } from "../tools/native-tools.js";
import { runGit } from "../worktree/manager.js";
import {
  buildDirectorSystemPrompt,
  DIRECTOR_DIRECT_TOOLS,
  DirectorSessionStore,
  type ChatSessionRecord,
  type ChatTurnRecord,
} from "./session.js";

const READ_FILE_DEFAULT_PAGE_BYTES = 32 * 1024;
// One page must fit a 16 MiB code_exec IPC frame even if JSON escaping expands every byte sixfold.
const READ_FILE_MAX_PAGE_BYTES = 2 * 1024 * 1024;
const READ_FILE_SCAN_BYTES = 1024 * 1024;

/** Thrown when a failed run's required deterministic gates still fail; the goal goes to a human without a review cycle. */
class FailedRunGatesError extends Error {
  constructor(
    readonly failedIds: string[],
    readonly evidenceTail: string,
  ) {
    super(`required deterministic gate(s) still fail after the run failed (${failedIds.join(", ")})${evidenceTail ? `: ${evidenceTail}` : ""}`);
    this.name = "FailedRunGatesError";
  }
}

/** Thrown when a review is requested while an amendment steer is still unacknowledged; the goal state is left untouched. */
class PendingAmendmentError extends Error {
  constructor(goalId: string, version: number, reason: string) {
    super(`Goal ${goalId} v${version} has an unacknowledged amendment (${reason})`);
    this.name = "PendingAmendmentError";
  }
}

/** Thrown by a wake turn whose event was invalidated before it was persisted or delivered; nothing was written. */
export class StaleWakeEventError extends Error {
  constructor(message = "Wake event invalidated before delivery") {
    super(message);
    this.name = "StaleWakeEventError";
  }
}

/** Where deterministic criterion commands run; shared by the goal_create and goal_amend schemas. */
const GATE_COMMAND_CONTEXT =
  "In a git repository it runs in a clean detached checkout of the goal's target branch, not in the main project checkout, so untracked and gitignored files are absent (except a linked node_modules or whatever worktree.setupCommand creates). Use committed relative paths, and $HARNESS_REPO_ROOT (the main checkout) for local-only files. $HARNESS_WORKTREE_PATH (the checkout it runs in), $HARNESS_GOAL_EVIDENCE_DIR and $HARNESS_GOAL_ID are also set.";

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
              command: { type: "string", description: `Shell command for a deterministic criterion; exit 0 passes. ${GATE_COMMAND_CONTEXT}` },
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
      "Commit a draft Goal Contract and launch its autonomous Tier 1 Run-Until-Done Orchestrator. If maxConcurrentGoals is full (and force_start is not set), enqueues the goal cleanly in the unified admission queue at `position` (defaults to back) and starts it automatically as soon as capacity opens. Safe to repeat and never creates a second run: a launching, running or queued goal returns its current state (`outcome`, `runId`, `goalStatus`), and a paused or stopped goal resumes its existing linked run in place.",
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
      "Explicit Director acceptance/override for a Goal based on verified evidence (e.g. when a review was obstructed or human approved). Requires a clear reason and concrete, readable evidence_paths (files or directories, e.g. under the goal's evidenceDir; .git, task worktrees and harness-internal state are refused). Required deterministic gates are re-run first and a failure rejects the accept unless override_deterministic is true. Recorded permanently with an audit trail in goal review history; auto-resolves open escalations.",
    inputSchema: {
      type: "object",
      required: ["goal_id", "reason", "evidence_paths"],
      properties: {
        goal_id: { type: "string" },
        reason: { type: "string", minLength: 1, description: "Explicit rationale for accepting the goal." },
        evidence_paths: {
          type: "array",
          minItems: 1,
          items: { type: "string" },
          description: "Concrete readable file/directory paths proving completion (repo-relative, absolute, or ~/).",
        },
        override_deterministic: {
          type: "boolean",
          description: "Accept even if required deterministic gates still fail (default false). The failing ids are recorded in the audit.",
        },
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
      "Amend an active or draft Goal Contract (vN -> vN+1), invalidating stale reviews and notifying the running Tier 1 Orchestrator with the exact delta. description REPLACES the entire description; use append_description to add instructions while keeping the existing text. add_criteria adds, update_criteria merges by id, remove_criteria_ids removes. Amending supersedes this goal's open verification_failed escalations only; other escalations stay open.",
    inputSchema: {
      type: "object",
      required: ["goal_id", "rationale"],
      properties: {
        goal_id: { type: "string" },
        rationale: { type: "string" },
        title: { type: "string" },
        description: {
          type: "string",
          description:
            "FULL REPLACEMENT of the goal description; anything not repeated here is discarded. A replacement shorter than the current description is rejected unless confirm_replace_description is true. To add instructions use append_description.",
        },
        append_description: {
          type: "string",
          description: "Text appended to the existing description after a blank line; the existing description is kept. Cannot be combined with description.",
        },
        confirm_replace_description: {
          type: "boolean",
          description: "Confirms that description intentionally replaces a longer existing description.",
        },
        add_criteria: { type: "array", description: "Criteria to add, shaped like goal_create criteria; their commands run under the same conditions.", items: { type: "object" } },
        update_criteria: {
          type: "array",
          description: "Partial updates merged into existing criteria by id; omitted fields keep their current values. An unknown id is rejected.",
          items: {
            type: "object",
            required: ["id"],
            properties: {
              id: { type: "string", description: "Id of an existing criterion, such as c1." },
              description: { type: "string" },
              kind: { type: "string", enum: ["deterministic", "behavioral", "architectural"] },
              command: { type: "string", description: `Shell command; required when the resulting kind is deterministic. ${GATE_COMMAND_CONTEXT}` },
              required: { type: "boolean" },
            },
          },
        },
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
    description:
      "Rebind this chat session to another registered project (by project ID, name, or path), preserving conversation history while switching workspace cwd. " +
      "Takes effect immediately for later tool calls in the same turn. Goals and delegated tasks created earlier stay with the project they were created in.",
    inputSchema: {
      type: "object",
      properties: {
        project: { type: "string", description: "Project ID (e.g. proj-1), name, or path on the daemon host." },
      },
      required: ["project"],
    },
  },
  {
    name: "project_remove",
    description:
      "Unregister a project from this daemon host's local project registry (by project ID, name, or path). Only the registry entry is removed: " +
      "files, chat history, goals, runs and tasks are untouched and stay tied to the path. The project's name, description and default profile/model/effort are lost. " +
      "The path is registered again (with a new ID) the next time a chat is opened or created there. Removing this chat's own project keeps the chat working in the same path.",
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
      "Record live progress, verification status, or checked criterion results on a Goal so the Goals table and detail cards dynamically reflect actual work. Phase 'blocked' means the goal cannot proceed without a human: automatic verification is skipped when its run settles and the Director is woken with the summary and remediation; use it only for genuine blockers. verification_status 'failed' prevents automatic approval of this revision until progress is reported again.",
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
  {
    name: "resolve_escalation",
    description:
      "Resolve one open or acknowledged escalation on a run owned by this chat's goals and record the reason in the audit trail. Works after the run settled or its orchestrator exited. Use it only when the finding is handled or no longer applies; it does not resume the run.",
    inputSchema: {
      type: "object",
      required: ["escalation_id", "reason"],
      properties: {
        escalation_id: { type: "string", description: "Escalation id, such as esc-29." },
        reason: { type: "string", description: "Why the escalation is handled or no longer applies." },
      },
    },
  },
  ...toolsForTier(1).filter(tool => ["spawn_task", "wait_tasks", "wait_children", "cancel_task", "artifact_read"].includes(tool.name)),
  {
    name: "read_file",
    description: "Read a UTF-8 text file: any workspace file, harness goals, notes, and artifacts, or any other file on this machine that the operator's account can read (absolute or ~/ path). Returns one page (default 32768 bytes, up to 2097152 with max_bytes) that ends on a line boundary when possible. Select lines with start_line and end_line, or page by bytes with offset. When hasMore is true, call again with offset set to nextOffset to continue to end of file. .git internals, task worktrees, and harness-internal state are refused. Binary files and directories are not supported.",
    inputSchema: {
      type: "object",
      required: ["path"],
      properties: {
        path: { type: "string", description: "Workspace-relative, absolute, or ~/ path." },
        start_line: { type: "number", description: "1-based first line to return. Cannot be combined with offset." },
        end_line: { type: "number", description: "1-based last line to return, inclusive. Defaults to as many lines as fit in the page." },
        offset: { type: "number", description: "Byte offset to start from (default 0). Pass the previous result's nextOffset to continue. A negative value counts back from end of file." },
        max_bytes: { type: "number", description: "Page size in bytes (default 32768, maximum 2097152)." },
        max_chars: { type: "number", description: "Optional: trim the page to at most this many UTF-16 characters (minimum 1; never splits a surrogate pair)." },
        exception_reason: { type: "string", description: "Optional note recorded with the read in the session log." },
      },
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
      "Resume a failed, crashed, cancelled, or stopped task in-place (same task ID, artifacts, and a deterministic recovery handoff) instead of spawning a new duplicate task. A Worker keeps its worktree and WIP commits while that worktree still exists; a cancelled Worker's worktree was archived, so it restarts on a fresh worktree from the base branch and the result message names the archived ref. Safe to repeat: a task that is already running, queued or completed returns its current state (`outcome`) and nothing is restarted.",
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
      "Resume a failed, crashed, stopped, or quota-paused run (or Goal's linked run) in-place, restarting the orchestrator and interrupted tasks with their preserved worktrees/handoffs while keeping completed work untouched. Tasks cancelled by a stop are not revived. Safe to repeat: an already running run returns its current state (`outcome`).",
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
        goal_id: {
          type: "string",
          description:
            "Goal ID (e.g. goal-1) this timer is bound to. Required for wake_on_event 'goal_settled' and for pause_goal/resume_goal. With 'any' it limits the goal-settlement trigger to this Goal.",
        },
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
          description:
            "Early cancel trigger (default: 'none'). 'user_message': a new user message supersedes the timer. 'goal_settled': requires goal_id; the timer is cancelled when that Goal's run settles, and settlement handling takes over. 'any': a user message, or goal_id settling; without goal_id, ANY Goal of this chat settling.",
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
        project: {
          type: "string",
          description: "Optional project ID, name, or path on the target host. A local session without one opens in this chat's current workspace.",
        },
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
  /** `localFallbackRepoRoot` is used only for a local session without a project; it is never sent to a peer. */
  createSession(input: { host?: string; project?: string; title?: string; localFallbackRepoRoot?: string }): Promise<unknown>;
  sendMessage(input: {
    fromSessionId: string;
    target: string;
    body: string;
    expectReply?: boolean;
    replyToId?: string;
  }): Promise<unknown>;
  readSession(input: { target: string; limit?: number }): Promise<unknown>;
}

/** Result of a start-or-resume goal commit; `goal.linkedRunId` is the one run the goal uses. */
export interface GoalCommitResult {
  goal: GoalRecord;
  outcome:
    | "started"
    | "queued"
    | "already_active"
    | "resumed"
    | "already_settled"
    | "already_completed"
    | "nothing_to_resume"
    | "stopped";
  message: string;
  /** Set when the goal's linked run was resumed (or found live) in place. */
  runStatus?: RunRecord["status"];
  resumedTaskIds?: string[];
  queuePosition?: number;
}

/** Stop and pause epochs captured when a commit began; a later /stop, pause or cancel stops its launch. */
interface GoalLaunchFence {
  stop: { global: number; repo: number };
  pauseEpoch: number;
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
  /** Open P0 escalations per run whose arrival already woke the Director; consumed only by the blocked settlement they cause. */
  private notifiedP0Escalations = new Map<string, Set<string>>();
  /** Runs whose escalation is being created by a path that wakes the Director itself; skips the P0 wake. */
  private suppressEscalationWakeRuns = new Set<string>();
  /** In-flight launches; joiners receive the launch's actual outcome (started, stopped, already_active ...). */
  private launchingGoals = new Map<string, Promise<GoalCommitResult>>();
  /** Bumped by every explicit goal pause or cancel, so a pause issued during a launch wins over it. */
  private goalPauseEpoch = new Map<string, number>();
  /** Bumped on amend, pause, cancel, or settled-run resume/relaunch so resumed goals cannot resurrect older review transitions or queued notices. */
  private goalReviewEpoch = new Map<string, number>();

  getReviewInvalidationEpoch(goalId: string): number {
    return this.goalReviewEpoch.get(goalId) ?? 0;
  }

  private bumpReviewInvalidationEpoch(goalId: string): void {
    this.goalReviewEpoch.set(goalId, (this.goalReviewEpoch.get(goalId) ?? 0) + 1);
  }
  /**
   * Amendments whose steer has not been acknowledged by the run's Tier 1; recovered once the run settles.
   * `inFlight` marks a live steer still awaiting its answer: a settlement arriving meanwhile is deferred until it resolves.
   */
  private readonly undeliveredAmendments = new Map<
    string,
    { attemptId: number; version: number; steerMsg: string; reason: string; inFlight?: boolean; deferredSettlementEvent?: EventRecord }
  >();
  private nextAmendmentAttemptId = 1;
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
      (id) => {
        this.bumpReviewInvalidationEpoch(id);
        this.activeReviews.get(id)?.abort(new Error("Goal changed during review"));
      },
      config.goals,
      () => {
        // Startup reconciliation frees slots held by settled runs; refresh queued progress and admit waiting goals.
        if (this.disposed) return;
        this.syncQueuedGoalProgress();
        void this.drainGoalQueue().catch(() => {});
      },
      {
        stateDir: config.stateDir,
        onGoalSettled: (goal) => {
          if (!goal.linkedRunId) return;
          this.db.resolveOpenEscalationsForRun(goal.linkedRunId, `Auto-resolved: goal ${goal.id} ${goal.status}`, {
            resolvedBy: "goal_settled",
            goalId: goal.id,
            goalStatus: goal.status,
          });
        },
        onGoalAmended: (goal, delta) => {
          if (!goal.linkedRunId) return [];
          return this.db
            .resolveOpenEscalationsForRun(
              goal.linkedRunId,
              `Superseded by goal ${goal.id} amendment v${delta.fromVersion} -> v${delta.toVersion}: ${delta.rationale.slice(0, 200)}`,
              {
                supersededByAmendment: true,
                resolvedBy: "amendment",
                goalId: goal.id,
                fromVersion: delta.fromVersion,
                toVersion: delta.toVersion,
              },
              { categories: ["verification_failed"] },
            )
            .map((e) => e.id);
        },
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
            const pauseEpochBefore = this.goalPauseEpoch.get(g.id) ?? 0;
            const stopFence = this.orchestrator.launchStopFence(g.repoRoot);
            await this.orchestrator.resumeRunFromQuota(linkedRun.id, { trigger: "user_timer" });
            // A pause, cancel or /stop issued while resuming wins: the goal keeps that status.
            const fresh = this.goals.getGoal(g.id);
            const superseded =
              (this.goalPauseEpoch.get(g.id) ?? 0) !== pauseEpochBefore ||
              this.orchestrator.launchFenceBroken(g.repoRoot, stopFence) ||
              fresh?.status !== "paused";
            if (!superseded) {
              this.goals.setStatus(g.id, "executing");
              resumedGoal = true;
            }
          } else {
            const commitRes = await this.commitGoalDetailed(g.id);
            resumedGoal = commitRes.outcome === "started" || commitRes.outcome === "resumed";
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

  private handleControlPlaneEvent(ev: EventRecord): void {
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
      // The caller creating this escalation wakes the Director itself (blocked settle or review transition).
      if (this.suppressEscalationWakeRuns.has(ev.runId)) return;
      const p = ev.payload as { id?: string; severity?: string; title?: string } | null;
      if (p?.severity !== "P0") return;
      const row = this.db.db
        .prepare("SELECT id FROM goals WHERE linked_run_id = ? ORDER BY updated_at DESC LIMIT 1")
        .get(ev.runId) as { id: string } | undefined;
      if (!row) return;
      const goal = this.goals.getGoal(row.id);
      if (!goal?.chatSessionId || ["achieved", "cancelled"].includes(goal.status)) return;
      const openEsc = this.db.listEscalations(ev.runId, "open");
      // This wake already reports the P0; a "failed" settlement blocked on this escalation must not wake again.
      // Only that settlement is matched: no generic settlement is marked processed.
      if (p.id) {
        const ids = this.notifiedP0Escalations.get(ev.runId) ?? new Set<string>();
        ids.add(String(p.id));
        this.notifiedP0Escalations.set(ev.runId, ids);
      }
      // An escalation is not a settlement: report the run's real status (a plain read; getEffectiveRun reconciles and emits).
      this.wakeHandler?.(goal.chatSessionId, {
        kind: "goal.escalated",
        goalId: goal.id,
        goalVersion: goal.version,
        goalTitle: goal.title,
        runId: ev.runId,
        runStatus: this.db.getRun(ev.runId)?.status ?? "unknown",
        escalationId: String(p.id ?? "unknown"),
        severity: "P0",
        title: p.title ?? "human input required",
        openEscalations: openEsc.length,
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
        detail: `paused_quota (manual): ${p.reason ?? this.orchestrator.getRunQuotaStatus(ev.runId)?.reason ?? "quota limit reached"}`,
        cause: "quota_paused",
      });
      return;
    }

    if (ev.type === "task.resumed") {
      // The run may already have been active, so no run.status event refreshes queued progress.
      // Deferred like the run.status case below.
      queueMicrotask(() => this.syncQueuedGoalProgress());
      return;
    }
    if (ev.type !== "run.status") return;
    const status = String((ev.payload as { status?: string } | null)?.status ?? "");
    const cause = (ev.payload as { cause?: string } | null)?.cause;
    if (status === "active" || status === "paused_quota") {
      // A resumed run can settle again with the same goal version; let that settlement through.
      if (status === "active") {
        for (const key of this.notifiedSettledGoals) if (key.includes(`:${ev.runId}:`)) this.notifiedSettledGoals.delete(key);
        this.notifiedP0Escalations.delete(ev.runId);
      }
      // Deferred: a resume reactivates the run before its task is relaunched, and counting slots
      // in between would derive the run as settled again from its not-yet-live tasks.
      queueMicrotask(() => this.syncQueuedGoalProgress());
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
    if (["achieved", "cancelled", "paused"].includes(goal.status)) this.undeliveredAmendments.delete(goal.id);
    if (["achieved", "cancelled", "paused", "verifying", "reviewing"].includes(goal.status)) return;

    // Cancel this chat's timers whose settlement subscription covers this goal (we're handling settlement now).
    // A timer bound to a Goal matches only that Goal; only an unbound `any` timer matches every Goal of the chat.
    // Timers are cancelled only when settlement wakes are enabled; otherwise the timer remains the chat's wake.
    if (wakeOnSettle && goal.chatSessionId) {
      for (const t of this.db.timers.list({ ownerKind: "chat", ownerId: goal.chatSessionId, status: "pending" })) {
        if (timerMatchesGoalSettlement(t, goal)) {
          this.db.timers.cancel(t.id, `goal_settled:${goal.id}`);
        }
      }
    }

    // An explicit blocker (blocked phase or blocked task) goes to a human without running verification gates.
    const blocker =
      (status === "completed" || status === "failed") && goal.status === "executing" && cause !== "daemon_restart"
        ? this.detectExplicitBlocker(goal, ev.runId)
        : null;

    // The run settled on a contract its Tier 1 never acknowledged: relaunch it on the amendment instead of verifying stale work.
    // Checked before deduplication so the relaunched run's own settlement (same key) still counts as a first notice.
    const pendingAmend = this.undeliveredAmendments.get(goal.id);
    if (
      pendingAmend &&
      pendingAmend.version === goal.version &&
      goal.status === "executing" &&
      !blocker &&
      cause !== "daemon_restart" &&
      (status === "completed" || status === "failed")
    ) {
      // A live steer still awaiting its answer decides: accepted replays this settlement, refused relaunches Tier 1.
      if (pendingAmend.inFlight) pendingAmend.deferredSettlementEvent = ev;
      else queueMicrotask(() => void this.recoverUndeliveredAmendment(goal.id, goal.version).catch(() => {}));
      return;
    }

    const dedupeKey = `${goal.id}:v${goal.version}:c${goal.reviewCycle}:${ev.runId}:${status}`;
    // Deduplicates verification launches and wakeups only; slot hand-off and queue admission always run.
    // A settlement blocked on a P0 whose arrival already woke the Director is reported by that wake.
    const notifiedP0 = this.notifiedP0Escalations.get(ev.runId);
    // Matched by identity: every escalation actually blocking the run must be a P0 already reported, so an unrelated
    // open P0 never hides a distinct blocking escalation.
    const blockedOnNotifiedP0 =
      status === "failed" && cause === "blocked_escalation" && Boolean(notifiedP0) &&
      (() => {
        const blockedIds = new Set(
          this.db
            .listTasks(ev.runId)
            .filter((t) => t.profileId !== "director" && (t.status === "blocked" || t.status === "returned_for_rescope"))
            .map((t) => t.id),
        );
        const blocking = this.db.listEscalations(ev.runId, "open").filter((e) => e.blocking && blockedIds.has(e.fromTaskId));
        return blocking.length > 0 && blocking.every((e) => e.severity === "P0" && notifiedP0!.has(e.id));
      })();
    const firstNotice = !blockedOnNotifiedP0 && !this.notifiedSettledGoals.has(dedupeKey);
    this.notifiedSettledGoals.add(dedupeKey);

    // A daemon restart interrupted the run: not a failure. Hand the goal to a human (non-terminal, frees the slot)
    // without verification; resume continues the same run in place.
    if (status === "failed" && cause === "daemon_restart") {
      const reason = `Run ${ev.runId} was interrupted by a daemon restart; resume continues it in place`;
      if (goal.status === "executing" && !this.activeReviews.has(goal.id)) this.goals.setStatus(goal.id, "awaiting_human", reason);
      void this.drainGoalQueue(goal.repoRoot).catch(() => {});
      this.syncQueuedGoalProgress();
      if (!firstNotice || !wakeOnSettle || !goal.chatSessionId) return;
      this.wakeHandler?.(goal.chatSessionId, {
        kind: "goal.settled",
        goalId: goal.id,
        goalVersion: goal.version,
        goalTitle: goal.title,
        runId: ev.runId,
        runStatus: "failed",
        openEscalations: this.db.listEscalations(ev.runId, "open").length,
        detail: reason,
        cause: "daemon_restart",
      });
      return;
    }

    if (blocker && firstNotice) this.parkBlockedGoal(goal, ev.runId, blocker);

    // Control-plane-driven automatic verification on run completion (removes LLM single point of failure)
    if (
      !blocker &&
      status === "completed" &&
      this.config.goals?.autoVerifyOnSettle !== false &&
      goal.verificationMode !== "none" &&
      !this.activeReviews.has(goal.id)
    ) {
      if (!this.unsubRunEvents || !firstNotice) return; // daemon disposed or already launched
      void this.verifyAndReviewGoal(goal.id, undefined, { autoTriggered: true }).catch((err) =>
        this.notifyAutoVerifyFailure(goal, ev.runId, err),
      );
      return;
    }

    // A failed run whose required deterministic gates may already pass is checked once; failing gates go to a human
    // without a review cycle. Never over a pending P0 (a human decision) or an explicit blocked phase.
    if (
      !blocker &&
      status === "failed" &&
      cause !== "blocked_escalation" &&
      this.config.goals?.autoVerifyOnSettle !== false &&
      goal.status === "executing" &&
      goal.verificationMode !== "none" &&
      goal.criteria.some((c) => c.required && c.kind === "deterministic") &&
      goal.liveProgress?.phase !== "blocked" &&
      !this.activeReviews.has(goal.id) &&
      !this.db.listEscalations(ev.runId, "unresolved").some((e) => e.severity === "P0")
    ) {
      if (!this.unsubRunEvents || !firstNotice) return;
      void this.verifyAndReviewGoal(goal.id, undefined, { autoTriggered: true, settledRunStatus: "failed" }).catch((err) =>
        this.notifyAutoVerifyFailure(goal, ev.runId, err),
      );
      return;
    }

    // A settled run no longer occupies a goal slot: hand the goal to a human and admit queued goals.
    // Open escalations stay open here; they are counted in the hand-off and disposed of explicitly.
    const openEscalations = this.db.listEscalations(ev.runId, "unresolved").length;
    // The recorded stop/failure cause travels with the hand-off note and the Director wake.
    const recordedCause = status === "completed" ? null : this.orchestrator.describeRunCause(ev.runId);
    const explicitStopReason = status === "stopped" ? ((ev.payload as { reason?: string } | null)?.reason?.trim() || null) : null;
    if (blocker) {
      // Parked above: awaiting_human with the blocker's escalation.
    } else if (goal.status === "executing" && !this.activeReviews.has(goal.id)) {
      if (status === "failed" || status === "stopped") {
        const ended = status === "stopped" ? "was stopped" : "failed";
        this.goals.setStatus(
          goal.id,
          "awaiting_human",
          openEscalations > 0
            ? `Linked run ${ev.runId} ${ended} with ${openEscalations} open escalation(s)` +
                `${recordedCause ? ` (${recordedCause})` : ""}; resolve them, then resume or review.`
            : `Linked run ${ev.runId} ${ended} before goal verification ` +
                `(${recordedCause ?? `no cause was recorded; inspect with \`pi-harness tree ${ev.runId}\``}). ` +
                `Nothing restarts automatically; \`pi-harness resume ${ev.runId}\` or goal_commit continues the same run in place, or review the goal.`,
        );
      } else {
        // Completed and automatic verification did not launch (disabled, or verification_mode=none).
        this.goals.setStatus(
          goal.id,
          "awaiting_human",
          goal.verificationMode === "none"
            ? "Linked run completed (verification_mode=none); review or accept to finish."
            : goal.reviewCycle > 0
              ? undefined
              : "Linked run completed (auto-verify disabled); trigger review or accept to finish.",
        );
      }
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
      ...(cause === "blocked_escalation" && status === "failed"
        ? {
            // Recoverable: the run derived "failed" only because tasks are blocked on open escalations.
            detail: blocker
              ? blockerDetail(blocker)
              : (recordedCause ?? (ev.payload as { reason?: string } | null)?.reason ?? "blocked on open escalation(s)"),
            cause: "blocked_escalation",
          }
        : blocker
        ? { detail: blockerDetail(blocker), cause: "escalation" }
        : {
            ...(recordedCause ? { detail: recordedCause } : {}),
            // Only a stop with a recorded operator reason is explicit; a stop derived from settled tasks is not.
            ...(status !== "completed"
              ? { cause: status === "failed" ? "run_failed" : explicitStopReason ? "explicit_stop" : "derived_stopped" }
              : {}),
          }),
    };
    this.wakeHandler?.(goal.chatSessionId, event);
  }

  /** Wakes the Director when automatic verification of a settled run could not finish, with the run's real status. */
  private notifyAutoVerifyFailure(goal: GoalRecord, runId: string, err: unknown): void {
    // A premature review leaves the pending amendment to decide the settlement; it is not a verification failure.
    if (err instanceof PendingAmendmentError) return;
    if (!this.unsubRunEvents || !goal.chatSessionId || !this.config.events?.wakeDirectorOnGoalSettle) return;
    const runStatus = this.db.getRun(runId)?.status;
    // A resumed run notifies again when it next settles.
    if (runStatus !== "completed" && runStatus !== "failed" && runStatus !== "stopped") return;
    const latest = this.goals.getGoal(goal.id);
    if (!latest || latest.version !== goal.version || ["achieved", "cancelled", "paused"].includes(latest.status)) return;
    let openEscCount = 0;
    try {
      openEscCount = this.db.listEscalations(runId, "unresolved").length;
    } catch {
      return; // DB may be closing
    }
    const gates = err instanceof FailedRunGatesError;
    const msg = err instanceof Error ? err.message : String(err);
    this.wakeHandler?.(goal.chatSessionId, {
      kind: "goal.settled",
      goalId: goal.id,
      goalVersion: goal.version,
      goalTitle: goal.title,
      runId,
      runStatus,
      openEscalations: openEscCount,
      cause: gates ? "gates_failed" : "verification_error",
      detail: (gates ? "deterministic_gates_failed: " : "verification_error: ") + msg.slice(0, 240),
    });
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
    return (await this.createChatDetailed(input)).session;
  }

  /** createChat that also reports whether it registered the workspace as a new project (and noted it in the chat). */
  async createChatDetailed(input: ChatSelection): Promise<{ session: ChatSessionRecord; projectRegistered: boolean }> {
    const known = input.projectId ? this.db.projects.resolveProject(input.projectId) : this.db.projects.getByPath(input.repoRoot);
    const selection = await this.resolveChatSelection({
      ...input,
      repoRoot: known?.rootPath ?? canonicalProjectPath(input.repoRoot),
      projectId: known?.id ?? input.projectId ?? null,
      profileId: input.profileId ?? known?.defaultProfileId ?? undefined,
      model: input.model !== undefined ? input.model : known?.defaultModel ?? undefined,
      effort: input.effort !== undefined ? input.effort : known?.defaultEffort ?? undefined,
    });
    // Register an unknown path only after its launch route resolved, so a failed create registers nothing.
    const ensured = input.projectId ? null : this.db.projects.ensureProjectForPathWithStatus(input.repoRoot);
    const project = ensured?.project ?? known;
    if (project) this.db.projects.touchProject(project.id);
    const session = this.chats.createSession({
      ...selection,
      repoRoot: project?.rootPath ?? selection.repoRoot,
      projectId: project?.id ?? selection.projectId,
    });
    if (ensured?.created) this.noteProjectRegistered(session.id, ensured.project);
    return { session, projectRegistered: Boolean(ensured?.created) };
  }

  /** Appends the one-time receipt for a workspace that was just registered as a project. */
  noteProjectRegistered(chatSessionId: string, project: ProjectRecord): void {
    this.chats.appendTurn(
      chatSessionId,
      "system_notice",
      `Registered project ${project.name} (${project.id}) for ${project.rootPath}. Files are untouched; remove it with /projects, 'pi-harness project rm ${project.name}' or the project_remove tool.`,
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
    return (await this.openDefaultChatDetailed(input)).session;
  }

  /** openDefaultChat that also reports whether it registered the workspace as a new project (and noted it in the chat). */
  async openDefaultChatDetailed(input: ChatSelection): Promise<{ session: ChatSessionRecord; projectRegistered: boolean }> {
    const known = input.projectId ? this.db.projects.resolveProject(input.projectId) : this.db.projects.getByPath(input.repoRoot);
    const repoRoot = known?.rootPath ?? canonicalProjectPath(input.repoRoot);
    const knownId = known?.id ?? input.projectId ?? null;
    const existing = (knownId ? this.chats.getProjectSession(knownId) : null) ?? this.chats.getWorkspaceSession(repoRoot);
    const selection = existing
      ? null
      : await this.resolveChatSelection({
          ...input,
          repoRoot,
          projectId: knownId,
          profileId: input.profileId ?? known?.defaultProfileId ?? undefined,
          model: input.model !== undefined ? input.model : known?.defaultModel ?? undefined,
          effort: input.effort !== undefined ? input.effort : known?.defaultEffort ?? undefined,
        });
    // Register an unknown path only after its launch route resolved, so a failed open registers nothing.
    const ensured = input.projectId ? null : this.db.projects.ensureProjectForPathWithStatus(input.repoRoot);
    const project = ensured?.project ?? known;
    const projectId = project?.id ?? input.projectId ?? null;
    if (project) this.db.projects.touchProject(project.id);
    const projectRegistered = Boolean(ensured?.created);
    if (existing) {
      const bound =
        projectId && existing.projectId !== projectId
          ? this.chats.updateSession(existing.id, { projectId, repoRoot: project?.rootPath ?? repoRoot })
          : existing;
      // Note the registration before the model reconciliation await, so a failed reconciliation cannot lose it.
      if (projectRegistered) this.noteProjectRegistered(bound.id, ensured!.project);
      return { session: await this.resolveOrMigrateSessionModel(bound), projectRegistered };
    }
    const session = this.chats.getOrCreateDefault({ ...selection!, repoRoot: project?.rootPath ?? repoRoot, projectId });
    if (projectRegistered) this.noteProjectRegistered(session.id, ensured!.project);
    return { session, projectRegistered };
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

  /** A goal holds a concurrency slot while it is launching, its run is live, or an in-process review is running for it. */
  private isGoalOccupyingSlot(goal: GoalRecord): boolean {
    if (this.launchingGoals.has(goal.id) || this.activeReviews.has(goal.id)) return true;
    if (goal.status !== "executing") return false;
    return this.isRunOccupyingGoalSlot(goal.linkedRunId);
  }

  private countActiveGoals(repoRoot: string): number {
    // Reconcile effective runs first: settling a stale run can synchronously drain the queue and
    // reserve another goal, which a single pass over an earlier snapshot would miss.
    for (const g of this.goals.listGoals({ repoRoot })) {
      if (g.status === "executing" && g.linkedRunId) this.orchestrator.getEffectiveRun(g.linkedRunId);
    }
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
          this.goals.reportProgress(goal.id, { phase: "queued", summary }, "system");
        }
      }
      const queuedIds = new Set(entries.map((e) => e.targetId));
      for (const g of this.goals.listGoals()) {
        const phase = g.liveProgress?.phase;
        // A draft keeps its launch error so the user sees why it left the queue.
        const stale = phase === "queued" || (phase === "queue_launch_error" && g.status !== "draft");
        if (stale && !queuedIds.has(g.id)) this.goals.clearQueuedProgress(g.id);
      }
    } finally {
      this.syncingQueuedProgress = false;
    }
  }

  /**
   * Launches a committed goal's run. Single-flight per goal: concurrent callers share one in-flight
   * launch and receive the linked GoalRecord, so no second run or queue entry is ever created.
   */
  private launchCommittedGoal(
    goal: GoalRecord,
    opts?: { profileId?: string; model?: string; effort?: string },
    fromQueueDrain = false,
    entryFence?: GoalLaunchFence,
  ): Promise<GoalCommitResult> {
    const inflight = this.launchingGoals.get(goal.id);
    if (inflight) return inflight;
    let settle!: { resolve: (g: GoalCommitResult) => void; reject: (err: unknown) => void };
    const slot = new Promise<GoalCommitResult>((resolve, reject) => {
      settle = { resolve, reject };
    });
    slot.catch(() => {});
    // Claim the slot before the first await so concurrent admission decisions and commits see it.
    this.launchingGoals.set(goal.id, slot);
    this.launchCommittedGoalBody(goal, opts, fromQueueDrain, slot, entryFence).then(settle.resolve, settle.reject);
    return slot;
  }

  private async launchCommittedGoalBody(
    goal: GoalRecord,
    opts: { profileId?: string; model?: string; effort?: string } | undefined,
    fromQueueDrain: boolean,
    slot: Promise<GoalCommitResult>,
    entryFence?: GoalLaunchFence,
  ): Promise<GoalCommitResult> {
    let started: GoalRecord;
    let runId: string;
    let launched = false;
    // Captured synchronously at launch start (or at the committing call's entry, when given): only a stop, pause or
    // cancel issued after this point stops the launch; a goal that was already paused before its first launch starts cleanly.
    const stopFence = entryFence?.stop ?? this.orchestrator.launchStopFence(goal.repoRoot);
    const pauseEpochBefore = entryFence?.pauseEpoch ?? this.goalPauseEpoch.get(goal.id) ?? 0;
    try {
      this.db.admissionQueue.remove("goal", goal.id);
      // A goal another caller already launched or settled is never launched a second time.
      const current = this.goals.getGoal(goal.id) ?? goal;
      if (["executing", "verifying", "reviewing"].includes(current.status)) {
        return { goal: current, outcome: "already_active", message: `Goal ${current.id} is already ${current.status}${current.linkedRunId ? ` on run ${current.linkedRunId}` : ""}; nothing was started again.` };
      }
      if (current.status === "achieved" || current.status === "cancelled") {
        return { goal: current, outcome: "already_settled", message: `Goal ${current.id} is ${current.status}; nothing was started.` };
      }
      // Tier 1 launches with the revision current when its rows are created; its exact text becomes the blackboard
      // baseline below. An amendment during the launch's awaits is read by resolveGoal instead of being steered later.
      const initialVersion = current.version;
      let launchedGoal = current;
      let run: RunRecord;
      let orchestratorTaskId: string;
      const prevLinkedRunId = current.linkedRunId ?? null;
      let linkedEarly: string | null = null;
      try {
        let orchestratorTask: { id: string };
        ({ run, orchestratorTask } = await this.orchestrator.startRun({
          goal: goalRunObjective(launchedGoal).objective,
          repoRoot: goal.repoRoot,
          profileId: opts?.profileId,
          model: opts?.model,
          effort: opts?.effort,
          stopFence,
          // Link before Tier 1 launches so its workers see the goal (evidence dir, write scope, env) from turn one.
          // Status stays untouched here: the checks below still decide between executing, paused and cancelled.
          onRunCreated: (created) => {
            this.db.db.prepare("UPDATE goals SET linked_run_id = ?, updated_at = ? WHERE id = ?").run(created.id, Date.now(), goal.id);
            linkedEarly = created.id;
          },
          resolveGoal: () => {
            launchedGoal = this.goals.getGoal(goal.id) ?? launchedGoal;
            const { title, objective } = goalRunObjective(launchedGoal);
            return { goal: objective, taskTitle: title };
          },
        }));
        orchestratorTaskId = orchestratorTask.id;
      } catch (err) {
        if (linkedEarly) {
          this.db.db
            .prepare("UPDATE goals SET linked_run_id = ? WHERE id = ? AND linked_run_id = ?")
            .run(prevLinkedRunId, goal.id, linkedEarly);
        }
        if (!(err instanceof RunStartStoppedError)) throw err;
        // /stop arrived before the run existed: nothing was created and the goal keeps its status.
        return {
          goal: this.goals.getGoal(goal.id) ?? goal,
          outcome: "stopped",
          message: `Goal ${goal.id} was not started: a stop was issued while its run was starting, so no run was created. Commit the goal again to start it.`,
        };
      }

      // An explicit /stop, pause or cancel issued while the run was starting wins over this older launch:
      // stop the new run, keep the link so a later commit resumes it in place, and keep the goal's status.
      const after = this.goals.getGoal(goal.id);
      const stoppedAll = this.orchestrator.launchFenceBroken(goal.repoRoot, stopFence);
      const pausedSince = (this.goalPauseEpoch.get(goal.id) ?? 0) !== pauseEpochBefore;
      if (!after) {
        await this.orchestrator.stopRun(run.id, `Goal ${goal.id} was deleted while its run was starting`);
        throw new Error(`Goal ${goal.id} was deleted while its run was starting; run ${run.id} was stopped.`);
      }
      if (stoppedAll || pausedSince || after.status === "cancelled") {
        const keep = after.status === "cancelled" ? "cancelled" : "paused";
        const reason = stoppedAll
          ? `All runs were stopped while goal ${goal.id}'s run was starting`
          : `Goal ${goal.id} was ${keep} while its run was starting`;
        this.goals.linkRunAndStart(goal.id, run.id);
        started = this.goals.setStatus(
          goal.id,
          keep,
          keep === "paused"
            ? `${reason}; run ${run.id} was stopped. goal_commit or \`pi-harness resume ${run.id}\` continues it in place.`
            : `${reason}; run ${run.id} was stopped.`,
        );
        await this.orchestrator.stopRun(run.id, reason);
        return {
          goal: started,
          outcome: "stopped",
          message: `Goal ${goal.id} was ${keep === "paused" ? "paused or stopped" : "cancelled"} while its run was starting; run ${run.id} was stopped and nothing is left running.`,
        };
      }

      // A fresh run starts on the current contract; amendments pending for an older run no longer apply.
      this.undeliveredAmendments.delete(goal.id);
      started = this.goals.linkRunAndStart(goal.id, run.id);
      this.bumpReviewInvalidationEpoch(goal.id);
      runId = run.id;
      launched = true;
      const contractValue = (g: Pick<GoalRecord, "version" | "title" | "description" | "criteria" | "nonGoals">, delta: unknown) => ({
        version: g.version,
        title: g.title,
        description: g.description,
        criteria: g.criteria,
        nonGoals: g.nonGoals,
        delta,
      });
      try {
        // The launched contract is what Tier 1 received. When it was amended during the launch, earlier revisions
        // since the committed version are backfilled so the amendment's baseline stays readable.
        const revisions = launchedGoal.version > initialVersion ? this.goals.listRevisions(goal.id) : [];
        for (const rev of revisions) {
          if (rev.version < initialVersion || rev.version >= launchedGoal.version) continue;
          this.db.blackboardSet(run.id, `goal_contract_v${rev.version}`, contractValue(rev, rev.delta), orchestratorTaskId);
        }
        const launchedDelta = revisions.find((r) => r.version === launchedGoal.version)?.delta ?? null;
        this.db.blackboardSet(run.id, `goal_contract_v${launchedGoal.version}`, contractValue(launchedGoal, launchedDelta), orchestratorTaskId);
      } catch {
        // Blackboard is advisory; the run is already launched.
      }
      // An amendment that landed after the run's rows were created: deliver it to the live run, or hand the goal off.
      const latestAfterLaunch = this.goals.getGoal(goal.id);
      if (latestAfterLaunch && latestAfterLaunch.version > launchedGoal.version) {
        const { title: updatedTitle, objective: updatedObjective } = goalRunObjective(latestAfterLaunch);
        this.db.updateRunGoal(run.id, updatedObjective);
        this.db.updateTask(orchestratorTaskId, { title: updatedTitle, objective: updatedObjective });
        const latestKey = `goal_contract_v${latestAfterLaunch.version}`;
        try {
          const revisionDelta = this.goals.listRevisions(goal.id).find((r) => r.version === latestAfterLaunch.version)?.delta ?? null;
          this.db.blackboardSet(run.id, latestKey, contractValue(latestAfterLaunch, revisionDelta), orchestratorTaskId);
        } catch {
          // Blackboard is advisory; the steer below still carries the full objective.
        }
        const steerMsg = [
          `[GOAL AMENDED: ${goal.id} v${launchedGoal.version} -> v${latestAfterLaunch.version}]`,
          "The goal was amended while this run was starting; your launch objective is outdated. Current contract:",
          updatedObjective,
          `Full current contract: blackboard_get({ key: "${latestKey}" }); launch baseline: goal_contract_v${launchedGoal.version}.`,
        ].join("\n");
        const tracker = this.trackAmendmentDelivery(goal.id, latestAfterLaunch.version, run.id, orchestratorTaskId, steerMsg);
        // Armed before the await: a run settling while the steer awaits its answer must not verify the stale contract.
        if (latestAfterLaunch.status === "executing") tracker.armInFlight();
        const res = await this.orchestrator
          .deliverOrQueueLiveSteer(orchestratorTaskId, steerMsg, { onOutcome: tracker.onOutcome })
          .catch(() => ({ delivered: false, mode: "none" as const }));
        let delivered = res.delivered;
        const isStillLatestExecuting = () => {
          const g = this.goals.getGoal(goal.id);
          return g !== null && g.version === latestAfterLaunch.version && g.status === "executing";
        };
        if (delivered) {
          tracker.markDelivered();
        } else if (res.mode === "queued_before_handle") {
          tracker.armQueued();
        } else {
          tracker.markAnswered();
          if (isStillLatestExecuting()) {
            const orch = this.db.getTask(orchestratorTaskId);
            if (orch && ["completed", "failed", "cancelled", "blocked", "returned_for_rescope"].includes(orch.status)) {
              delivered = await this.orchestrator
                .resumeTaskDetailed(orchestratorTaskId, { allowCompleted: true, note: steerMsg, validateBeforeActivate: isStillLatestExecuting })
                .then((r) => r.outcome === "resumed" || (r.noteDelivered === true && r.noteDelivery?.mode === "native"), () => false);
            }
            if (delivered) this.clearUndeliveredAmendment(goal.id, latestAfterLaunch.version);
          }
        }
        if (!delivered && res.mode !== "queued_before_handle") {
          tracker.markUndelivered("mid-launch amendment could not be delivered to its run");
          started = this.handOffStrandedGoal(latestAfterLaunch, "mid-launch amendment could not be delivered to its run");
        } else {
          started = this.goals.getGoal(goal.id) ?? started;
        }
      }
    } finally {
      if (this.launchingGoals.get(goal.id) === slot) this.launchingGoals.delete(goal.id);
      if (!launched) {
        // The released slot must reach queued waiters; a queue drain handles its own failures.
        this.syncQueuedGoalProgress();
        if (!fromQueueDrain) void this.drainGoalQueue(goal.repoRoot).catch(() => {});
      }
    }
    this.syncQueuedGoalProgress();
    if (started.chatSessionId) {
      const committedEvent: ChatControlEvent = {
        kind: "goal.committed",
        goalId: started.id,
        goalVersion: started.version,
        goalTitle: started.title,
        runId,
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
    this.db.emitEvent(runId, null, "goal.committed", { goalId: started.id, version: started.version, title: started.title });
    return { goal: started, outcome: "started", message: `Goal ${started.id} started on run ${runId}.` };
  }

  async commitGoal(
    goalId: string,
    opts?: { profileId?: string; model?: string; effort?: string; position?: QueuePositionInput; forceStart?: boolean },
  ): Promise<GoalRecord> {
    return (await this.commitGoalDetailed(goalId, opts)).goal;
  }

  /**
   * Start-or-resume, safe to repeat. A launching, live, queued or settled goal returns its current state;
   * a paused, awaiting_human or stopped goal with an unfinished linked run resumes that same run in place.
   * Only a goal without a resumable linked run starts a new run (through the admission queue).
   */
  async commitGoalDetailed(
    goalId: string,
    opts?: {
      profileId?: string;
      model?: string;
      effort?: string;
      position?: QueuePositionInput;
      forceStart?: boolean;
      /** Guidance for the resumed orchestrator when the linked run is resumed in place. */
      note?: string;
    },
  ): Promise<GoalCommitResult> {
    let goal = this.goals.getGoal(goalId);
    if (!goal) throw new Error(`Unknown goal ${goalId}`);
    // Captured synchronously before any await: a /stop, pause or cancel issued after this commit began wins over it.
    const entryFence: GoalLaunchFence = {
      stop: this.orchestrator.launchStopFence(goal.repoRoot),
      pauseEpoch: this.goalPauseEpoch.get(goal.id) ?? 0,
    };
    const result = (g: GoalRecord, outcome: GoalCommitResult["outcome"], message: string): GoalCommitResult => ({
      goal: g,
      outcome,
      message,
    });
    const activeMessage = (g: GoalRecord) =>
      `Goal ${g.id} is already ${g.status}${g.linkedRunId ? ` on run ${g.linkedRunId}` : ""}; nothing was started again.`;

    // A concurrent commit shares the in-flight launch's actual outcome (started, stopped, ...), never a guessed one.
    const inflight = this.launchingGoals.get(goal.id);
    if (inflight) return inflight;
    if (goal.status === "achieved" || goal.status === "cancelled") {
      return result(
        goal,
        "already_settled",
        `Goal ${goal.id} is ${goal.status}; nothing was started. Create a new goal with goal_create for follow-up work.`,
      );
    }
    const linked = goal.linkedRunId ? this.orchestrator.getEffectiveRun(goal.linkedRunId) : null;
    // A completed linked run awaiting human review is finished work: never start a duplicate run over it.
    const completedMessage = (g: GoalRecord) =>
      `Goal ${g.id}'s linked run ${g.linkedRunId} already completed and the goal is awaiting review; nothing was started. ` +
      "Review it (goal_trigger_review), accept it (goal_accept), or amend the goal to start follow-up work.";
    if (linked?.status === "completed" && goal.status === "awaiting_human") {
      return { ...result(goal, "already_completed", completedMessage(goal)), runStatus: linked.status };
    }
    const linkedResumable = Boolean(linked && linked.status !== "completed" && linked.status !== "active");
    if (
      goal.status === "verifying" ||
      goal.status === "reviewing" ||
      (goal.status === "executing" && !linkedResumable)
    ) {
      return result(goal, "already_active", activeMessage(goal));
    }
    const queued = this.db.admissionQueue.get("goal", goal.id);
    if (queued && !opts?.position && !opts?.forceStart) {
      return { ...result(goal, "queued", `Goal ${goal.id} is already queued at #${queued.position}; nothing was enqueued again.`), queuePosition: queued.position };
    }
    if (linked && linked.status !== "completed") {
      const pauseEpochBefore = this.goalPauseEpoch.get(goal.id) ?? 0;
      const res = await this.orchestrator.resumeRun(linked.id, { trigger: "manual", ...(opts?.note?.trim() ? { note: opts.note.trim() } : {}) });
      const extra = { runStatus: res.runStatus, resumedTaskIds: res.resumedTaskIds };
      if (res.outcome === "resumed" || res.outcome === "already_active") {
        this.db.admissionQueue.remove("goal", goal.id);
        // The resumed Tier 1 reads the current contract from its run row.
        if (res.outcome === "resumed") this.undeliveredAmendments.delete(goal.id);
        // Every in-place resume (RPC, Director tool, timer) starts the new epoch here, before the goal is re-read.
        this.onRunResumed(linked.id, linked.status, res.outcome === "resumed");
        const fresh = this.goals.getGoal(goal.id)!;
        // A pause or cancel that arrived while resuming wins; its own stopRun stops the run again.
        const pausedSince = (this.goalPauseEpoch.get(goal.id) ?? 0) !== pauseEpochBefore;
        const keep = pausedSince || ["executing", "verifying", "reviewing", "cancelled", "achieved"].includes(fresh.status);
        const updated = keep ? fresh : this.goals.setStatus(goal.id, "executing", "Resumed in place");
        this.syncQueuedGoalProgress();
        return { ...result(updated, res.outcome === "resumed" ? "resumed" : "already_active", `Goal ${goal.id}: ${res.message}`), ...extra };
      }
      if (res.outcome === "nothing_to_resume") {
        return { ...result(this.goals.getGoal(goal.id) ?? goal, "nothing_to_resume", `Goal ${goal.id}: ${res.message}`), ...extra };
      }
      // already_completed: the run finished meanwhile. Finished work awaiting review is kept; otherwise start the next run.
      const fresh = this.goals.getGoal(goal.id) ?? goal;
      if (fresh.status === "awaiting_human") return { ...result(fresh, "already_completed", completedMessage(fresh)), ...extra };
    }

    const repoRoot = goal.repoRoot;
    // Counting slots can synchronously settle runs and drain the queue, which may admit this goal itself.
    const refreshUnlessAdmitted = (): GoalRecord | null => {
      const fresh = this.goals.getGoal(goalId);
      if (!fresh) throw new Error(`Unknown goal ${goalId}`);
      goal = fresh;
      return this.launchingGoals.has(fresh.id) || ["executing", "verifying", "reviewing"].includes(fresh.status) ? fresh : null;
    };
    const admittedResult = async (g: GoalRecord): Promise<GoalCommitResult> => {
      const launching = this.launchingGoals.get(g.id);
      if (launching) return launching;
      return result(g, "started", `Goal ${g.id} started${g.linkedRunId ? ` on run ${g.linkedRunId}` : ""}.`);
    };

    const limit = Math.max(1, this.config.goals.maxConcurrentGoals ?? 4);
    let activeCount = this.countActiveGoals(repoRoot);
    let admitted = refreshUnlessAdmitted();
    if (admitted) return admittedResult(admitted);
    const listQueuedAhead = () =>
      this.db.admissionQueue.list({ lane: "goal", repoRoot }).filter((e) => e.targetId !== goalId);
    let queuedAhead = listQueuedAhead();
    if (!opts?.forceStart && activeCount < limit && queuedAhead.length > 0) {
      // Free slots go to goals already waiting (FIFO) before this goal decides whether to queue.
      await this.drainGoalQueue(repoRoot);
      admitted = refreshUnlessAdmitted();
      if (admitted) return admittedResult(admitted);
      activeCount = this.countActiveGoals(repoRoot);
      admitted = refreshUnlessAdmitted();
      if (admitted) return admittedResult(admitted);
      queuedAhead = listQueuedAhead();
    }

    // Neither queue nor launch a goal whose commit was overtaken by a /stop, pause or cancel.
    const beforeAdmission = this.goals.getGoal(goal.id) ?? goal;
    if (
      (this.goalPauseEpoch.get(goal.id) ?? 0) !== entryFence.pauseEpoch ||
      this.orchestrator.launchFenceBroken(repoRoot, entryFence.stop) ||
      beforeAdmission.status === "cancelled"
    ) {
      return result(
        beforeAdmission,
        "stopped",
        `Goal ${goal.id} was not started: a stop, pause or cancel was issued while it was being committed, so no run was created and it was not queued. Commit the goal again to start it.`,
      );
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
      const entry = this.db.admissionQueue.get("goal", goal.id);
      return {
        ...result(
          this.goals.getGoal(goal.id)!,
          "queued",
          `Goal ${goal.id} is queued${entry ? ` at #${entry.position}` : ""} (${activeCount}/${limit} concurrent goals active); it starts automatically when a slot frees.`,
        ),
        ...(entry ? { queuePosition: entry.position } : {}),
      };
    }

    return this.launchCommittedGoal(goal, opts, false, entryFence);
  }

  /**
   * Shared goal resume contract (Director and daemon RPC): the same idempotent start-or-resume as a commit,
   * with an optional note for the resumed orchestrator. A settled goal reports its state instead of throwing.
   */
  async resumeGoalDetailed(goalId: string, opts?: { note?: string }): Promise<GoalCommitResult> {
    return this.commitGoalDetailed(goalId, opts?.note?.trim() ? { note: opts.note.trim() } : undefined);
  }

  /**
   * Explicit pause (or cancel) of a goal: wins over any launch or resume already in flight, cancels pending chat
   * timers that would resume it, leaves the admission queue, stops its linked run and admits queued goals.
   */
  async pauseGoal(goalId: string, reason: string, status: "paused" | "cancelled" = "paused"): Promise<GoalRecord> {
    this.noteGoalPaused(goalId, reason);
    this.undeliveredAmendments.delete(goalId);
    this.db.admissionQueue.remove("goal", goalId);
    const g = this.goals.setStatus(goalId, status, reason);
    if (g.linkedRunId) {
      await this.orchestrator.stopRun(g.linkedRunId, status === "paused" ? "Goal paused" : "Goal cancelled");
      if (status === "cancelled") this.autoResolveGoalEscalations(g.linkedRunId, `Auto-resolved: goal ${g.id} cancelled`);
    }
    void this.drainGoalQueue(g.repoRoot).catch(() => {});
    return g;
  }

  /** Bumps the goal's pause epoch and cancels earlier pending chat timers that would resume it. Returns the cancelled count. */
  noteGoalPaused(goalId: string, reason: string): number {
    this.goalPauseEpoch.set(goalId, (this.goalPauseEpoch.get(goalId) ?? 0) + 1);
    this.bumpReviewInvalidationEpoch(goalId);
    let cancelled = 0;
    for (const t of this.db.timers.list({ ownerKind: "chat", status: "pending" })) {
      if (t.resumeGoal === true && t.goalId === goalId && this.db.timers.cancel(t.id, reason)) cancelled++;
    }
    return cancelled;
  }

  /** Tool result for goal_commit / goal_create(commit_immediately): the goal plus its idempotent start-or-resume state. */
  private goalCommitToolResult(res: GoalCommitResult, presentationId: string | undefined): Record<string, unknown> {
    const qEntry = this.db.admissionQueue.get("goal", res.goal.id);
    return {
      ...res.goal,
      ...(qEntry ? { queued: true, queuePosition: qEntry.position } : {}),
      evidenceDir: goalEvidenceDirOrNull(res.goal),
      runId: res.goal.linkedRunId,
      goalStatus: res.goal.status,
      outcome: res.outcome,
      message: res.message,
      presentation_id: presentationId,
    };
  }

  private drainingGoals = false;

  /** Admits queued goals in strict queue order whenever capacity opens or `maxConcurrentGoals` increases. */
  async drainGoalQueue(repoRoot?: string): Promise<string[]> {
    if (this.disposed) return [];
    if (this.drainingGoals) {
      // Rerun once the current drain finishes so capacity freed meanwhile is not missed.
      if (repoRoot) this.pendingDrainRepos.add(repoRoot);
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
            const res = await this.launchCommittedGoal(goal, launchOpts, true);
            if (res.outcome === "started") startedIds.push(goal.id);
          } catch (err) {
            this.db.admissionQueue.remove("goal", next.targetId);
            this.goals.reportProgress(goal.id, {
              phase: "queue_launch_error",
              summary: `Failed to launch from queue: ${err instanceof Error ? err.message : String(err)}`,
            }, "system");
          }
        }
        this.syncQueuedGoalProgress();
      }
    } finally {
      this.drainingGoals = false;
      if (!this.disposed && (this.pendingDrainAll || this.pendingDrainRepos.size > 0)) {
        const repos = [...this.pendingDrainRepos];
        const drainAll = this.pendingDrainAll || repos.length > 1;
        this.pendingDrainAll = false;
        this.pendingDrainRepos.clear();
        void this.drainGoalQueue(drainAll ? undefined : repos[0]).catch(() => {});
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
      const res = await this.commitGoalDetailed(goal.id, {
        ...((qEntry.launchOpts ?? {}) as { profileId?: string; model?: string; effort?: string }),
        forceStart: true,
      });
      const started = res.outcome === "started" || res.outcome === "resumed";
      return {
        goal: res.goal,
        outcome: res.outcome,
        message: res.message,
        action: started ? "force_started_queued_goal" : `force_start_${res.outcome}`,
        warning,
      };
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

  /**
   * Refuses to resume a run whose linked goal is already achieved or cancelled: resuming would reactivate the run
   * of a finished goal. Runs with no linked goal are unaffected.
   */
  assertRunGoalNotTerminal(runId: string): void {
    const row = this.db.db
      .prepare("SELECT id FROM goals WHERE linked_run_id = ? ORDER BY updated_at DESC LIMIT 1")
      .get(runId) as { id: string } | null;
    const goal = row ? this.goals.getGoal(row.id) : null;
    if (goal && (goal.status === "achieved" || goal.status === "cancelled")) {
      throw new Error(
        `Cannot resume run ${runId}: goal ${goal.id} is already ${goal.status}. Create a new goal with goal_create for follow-up work.`,
      );
    }
  }

  async amendGoal(
    goalId: string,
    patch: Parameters<GoalEngine["amendGoal"]>[1],
    opts?: { source?: "director" | "user" | "external"; actorChatSessionId?: string },
  ): Promise<{
    goal: GoalRecord;
    delta: ReturnType<GoalEngine["amendGoal"]>["delta"];
    delivery: { delivered: boolean; via: "steer" | "resume" | "none"; reason?: string };
    supersededEscalationIds: string[];
    openEscalations: Array<Pick<EscalationRecord, "id" | "severity" | "category" | "title">>;
  }> {
    const { goal, delta, supersededEscalationIds } = this.goals.amendGoal(goalId, patch);
    const source = opts?.source ?? "external";
    let amendedGoal = goal;
    let delivery: { delivered: boolean; via: "steer" | "resume" | "none"; reason?: string } = { delivered: false, via: "none" };
    const strand = (reason: string) => {
      delivery = { delivered: false, via: "none", reason };
      amendedGoal = this.handOffStrandedGoal(goal, reason);
    };
    const cap = (text: string, max: number) => (text.length > max ? `${text.slice(0, max)}... [truncated]` : text);

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
        ...evidenceDirLines(goal),
      ].join("\n");
      this.db.updateRunGoal(goal.linkedRunId, updatedObjective);

      this.db.emitEvent(goal.linkedRunId, null, "goal.amended", { goalId: goal.id, delta, source });
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
        try {
          // Runs launched before contracts were written at launch get their previous version backfilled from revisions.
          const prevKey = `goal_contract_v${delta.fromVersion}`;
          if (!this.db.blackboardGet(goal.linkedRunId, prevKey)[prevKey]) {
            const prev = this.goals.listRevisions(goal.id).find((r) => r.version === delta.fromVersion);
            if (prev) {
              this.db.blackboardSet(
                goal.linkedRunId,
                prevKey,
                { version: prev.version, title: prev.title, description: prev.description, criteria: prev.criteria, nonGoals: prev.nonGoals, delta: prev.delta },
                orch.id,
              );
            }
          }
        } catch {
          // Backfill is best effort.
        }
        this.db.blackboardSet(
          goal.linkedRunId,
          `goal_contract_v${goal.version}`,
          { version: goal.version, title: goal.title, description: goal.description, criteria: goal.criteria, nonGoals: goal.nonGoals, delta },
          orch.id,
        );
        const steerMsg = [
          `[GOAL AMENDED: ${goal.id} v${delta.fromVersion} -> v${delta.toVersion}]`,
          `Rationale: ${delta.rationale}`,
          delta.descriptionMode === "append" && delta.appendedDescription
            ? `Description extended (existing text unchanged). Appended:\n${cap(delta.appendedDescription, 4000)}`
            : null,
          delta.descriptionMode === "replace"
            ? `Description REPLACED (${delta.previousDescriptionLength ?? "?"} -> ${delta.descriptionLength ?? goal.description.length} chars). New description:\n${cap(goal.description, 4000)}\nPrevious description: blackboard goal_contract_v${delta.fromVersion}.`
            : null,
          delta.nonGoalsChanged ? `Non-goals now: ${goal.nonGoals.length ? goal.nonGoals.join("; ") : "(none)"}` : null,
          delta.addedCriteria.length
            ? `Added criteria: ${delta.addedCriteria.map((c) => `${c.id}: ${c.description}`).join("; ")}`
            : null,
          delta.removedCriteriaIds.length ? `Removed criteria: ${delta.removedCriteriaIds.join(", ")}` : null,
          delta.modifiedCriteria.length
            ? `Modified criteria: ${delta.modifiedCriteria.map((c) => `${c.id}: ${c.description}`).join("; ")}`
            : null,
          `Full current contract: blackboard_get({ key: "goal_contract_v${goal.version}" }).`,
          "Reconcile active Tier 2 Workers: KEEP unaffected workers, STEER adjustable workers, CANCEL_AND_REPLACE invalidated workers.",
        ]
          .filter(Boolean)
          .join("\n");
        const isStillAmendedExecuting = () => {
          const latest = this.goals.getGoal(goal.id);
          return latest !== null && latest.version === goal.version && latest.status === "executing";
        };
        // A Tier 1 still launching reads the updated row in its initial prompt; one spawning gets the steer queued and
        // reports its acknowledgement later, so it is not counted as delivered here.
        const tracker = this.trackAmendmentDelivery(goal.id, goal.version, goal.linkedRunId, orch.id, steerMsg);
        // Armed before the await: a run settling while the steer awaits its answer must not verify the stale contract.
        if (goal.status === "executing") tracker.armInFlight();
        const res = await this.orchestrator
          .deliverOrQueueLiveSteer(orch.id, steerMsg, { onOutcome: tracker.onOutcome })
          .catch(() => ({ delivered: false, mode: "none" as const }));
        if (res.delivered) {
          tracker.markDelivered();
          delivery = { delivered: true, via: "steer" };
        } else if (res.mode === "queued_before_handle") {
          const reason = "queued for runner handle; awaiting acknowledgement";
          tracker.armQueued();
          delivery = { delivered: false, via: "none", reason };
        } else {
          tracker.markAnswered();
          let delivered = false;
          // A live Tier 1 that refused the steer is never resumed: its fallback only queues the note for a turn
          // the stale generation may never take. Its settlement relaunches it with the amendment instead.
          const freshOrch = this.db.getTask(orch.id);
          if (
            goal.status === "executing" &&
            isStillAmendedExecuting() &&
            freshOrch &&
            ["completed", "failed", "cancelled", "blocked", "returned_for_rescope"].includes(freshOrch.status)
          ) {
            delivered = await this.orchestrator
              .resumeTaskDetailed(orch.id, { allowCompleted: true, note: steerMsg, validateBeforeActivate: isStillAmendedExecuting })
              // An idempotent "already active" return delivers the note only when the live session took it natively.
              .then((r) => r.outcome === "resumed" || (r.noteDelivered === true && r.noteDelivery?.mode === "native"), () => false);
          }
          if (delivered) {
            this.clearUndeliveredAmendment(goal.id, goal.version);
            delivery = { delivered: true, via: "resume" };
          } else {
            tracker.markUndelivered("amendment could not be delivered to its run");
            strand("amendment could not be delivered to its run");
          }
        }
      } else {
        strand("linked run has no orchestrator to apply the amendment");
      }
    } else {
      strand("no linked run to apply the amendment");
    }

    if (goal.chatSessionId) {
      const amendedEvent: ChatControlEvent = {
        kind: "goal.amended",
        goalId: goal.id,
        fromVersion: delta.fromVersion,
        toVersion: delta.toVersion,
        rationale: delta.rationale,
        source,
      };
      // The owning Director learns of an amendment it did not make itself in a wake turn (which records the event);
      // its own amendment, or one made with wakes disabled, is only recorded in the transcript.
      const selfAmend = source === "director" && opts?.actorChatSessionId === goal.chatSessionId;
      if (this.wakeHandler && this.config.events?.wakeDirectorOnGoalSettle && !selfAmend) {
        this.wakeHandler(goal.chatSessionId, amendedEvent);
      } else {
        this.chats.appendTurn(
          goal.chatSessionId,
          "system_notice",
          formatChatEventSummary(amendedEvent),
          undefined,
          amendedEvent,
        );
      }
    }

    let openEscalations: Array<Pick<EscalationRecord, "id" | "severity" | "category" | "title">> = [];
    if (goal.linkedRunId) {
      try {
        openEscalations = this.db
          .listEscalations(goal.linkedRunId, "unresolved")
          .map((e) => ({ id: e.id, severity: e.severity, category: e.category, title: e.title }));
      } catch {
        // Escalations table may be absent.
      }
    }
    return { goal: amendedGoal, delta, delivery, supersededEscalationIds, openEscalations };
  }

  /**
   * Tracks one amendment steer of `version`: an acknowledged (or rolled-over, hence re-hydrated) steer clears the
   * pending record; a refused or never-delivered one records it, emits `goal.amendment_undelivered` and schedules
   * recovery, which relaunches Tier 1 on the amended contract once the run settles.
   */
  private trackAmendmentDelivery(goalId: string, version: number, runId: string, orchId: string, steerMsg: string) {
    const attemptId = this.nextAmendmentAttemptId++;
    let reported = false;
    /** Set once this attempt wrote the pending entry; from then on only its own still-present entry may act. */
    let armed = false;
    const isActive = () => this.undeliveredAmendments.get(goalId)?.attemptId === attemptId;
    /**
     * Whether this attempt may still record or settle the amendment. An armed attempt whose entry was consumed by
     * recovery, acknowledged, or cleared by pause, cancel or a newer amendment is finished: late outcomes are ignored.
     */
    const owns = () => {
      if (armed) return isActive();
      const entry = this.undeliveredAmendments.get(goalId);
      return !entry || entry.attemptId === attemptId || entry.version !== version;
    };
    /** A settlement deferred while the steer was in flight, taken off this attempt's entry. */
    const takeDeferred = (): EventRecord | undefined => {
      const prior = this.undeliveredAmendments.get(goalId);
      return prior?.attemptId === attemptId ? prior.deferredSettlementEvent : undefined;
    };
    const record = (reason: string, eventReason: string): boolean => {
      if (!owns()) return false;
      const current = this.goals.getGoal(goalId);
      if (!current || current.version !== version) return false;
      armed = true;
      this.undeliveredAmendments.set(goalId, { attemptId, version, steerMsg, reason, inFlight: false });
      this.db.emitEvent(runId, orchId, "goal.amendment_undelivered", { goalId, version, reason: eventReason });
      return true;
    };
    /** The steer (or initial prompt) carried the amendment: a settlement deferred meanwhile is handled now. */
    const markDelivered = () => {
      if (!owns()) return;
      const deferred = takeDeferred();
      if (isActive()) this.undeliveredAmendments.delete(goalId);
      if (deferred) queueMicrotask(() => this.handleControlPlaneEvent(deferred));
    };
    return {
      onOutcome: (accepted: boolean, reason: string) => {
        reported = true;
        if (!owns()) return;
        if (accepted || reason === "rolled_over") {
          markDelivered();
          return;
        }
        if (record(reason, reason)) {
          queueMicrotask(() => void this.recoverUndeliveredAmendment(goalId, version).catch(() => {}));
        }
      },
      /** A live steer is about to be awaited: settlements arriving before its answer are deferred, never verified. */
      armInFlight: () => {
        if (this.goals.getGoal(goalId)?.version !== version) return;
        armed = true;
        this.undeliveredAmendments.set(goalId, {
          attemptId,
          version,
          steerMsg,
          reason: "amendment delivery awaiting acknowledgement",
          inFlight: true,
        });
      },
      markDelivered,
      /** The live steer was refused; a settlement arriving during the fallback below is no longer deferred. */
      markAnswered: () => {
        const prior = this.undeliveredAmendments.get(goalId);
        if (prior?.attemptId === attemptId) prior.inFlight = false;
      },
      /** A steer queued for the runner handle: pending until its outcome arrives (unless it already did). */
      armQueued: () => {
        if (reported || !owns()) return;
        const deferred = takeDeferred();
        armed = true;
        this.undeliveredAmendments.set(goalId, {
          attemptId,
          version,
          steerMsg,
          reason: "queued for runner handle; awaiting acknowledgement",
          inFlight: false,
          ...(deferred ? { deferredSettlementEvent: deferred } : {}),
        });
        if (deferred) queueMicrotask(() => void this.recoverUndeliveredAmendment(goalId, version).catch(() => {}));
      },
      /** No live steer or resume reached the run; a still-live run recovers it when it settles. */
      markUndelivered: (reason: string) => {
        const deferred = takeDeferred();
        if (record(reason, "steer_rejected") && deferred) {
          queueMicrotask(() => void this.recoverUndeliveredAmendment(goalId, version).catch(() => {}));
        }
      },
    };
  }

  /** Clears the pending amendment of a goal, only for `version` when given, so an older one never wipes a newer one. */
  private clearUndeliveredAmendment(goalId: string, version?: number): void {
    if (version === undefined || this.undeliveredAmendments.get(goalId)?.version === version) this.undeliveredAmendments.delete(goalId);
  }

  /** Relaunches a settled run's Tier 1 with an amendment it never acknowledged, or hands the goal to a human. */
  private async recoverUndeliveredAmendment(goalId: string, version: number): Promise<void> {
    const pending = this.undeliveredAmendments.get(goalId);
    if (!pending || pending.version !== version) return;
    const current = this.goals.getGoal(goalId);
    if (!current || current.version !== version || current.status !== "executing") {
      this.clearUndeliveredAmendment(goalId, version);
      return;
    }
    // Still live: its settlement runs recovery again if the amendment is still unacknowledged.
    if (this.isRunOccupyingGoalSlot(current.linkedRunId)) return;
    this.clearUndeliveredAmendment(goalId, version);
    const orch = current.linkedRunId ? this.db.listTasks(current.linkedRunId).find((t) => t.tier === 1) : undefined;
    const resumed = orch
      ? await this.orchestrator
          .resumeTaskDetailed(orch.id, {
            allowCompleted: true,
            note: pending.steerMsg,
            validateBeforeActivate: () => {
              const g = this.goals.getGoal(goalId);
              return g !== null && g.version === version && g.status === "executing";
            },
          })
          .then((r) => r.outcome === "resumed" || (r.noteDelivered === true && r.noteDelivery?.mode === "native"), () => false)
      : false;
    if (!resumed) this.handOffStrandedGoal(current, pending.reason);
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
   * A run resumed from a settled epoch (completed, failed or stopped) starts a fresh attempt: verification
   * claims and a blocked phase from the previous epoch no longer describe it. An already-active run or a
   * quota auto-resume keeps its claims, since Tier 1 is still live and its reports are current.
   */
  onRunResumed(runId: string, priorRunStatus: RunRecord["status"] | null | undefined, resumed: boolean): void {
    if (!resumed || !priorRunStatus || !["completed", "failed", "stopped"].includes(priorRunStatus)) return;
    try {
      const rows = this.db.db.prepare("SELECT id FROM goals WHERE linked_run_id = ?").all(runId) as Array<{ id: string }>;
      for (const row of rows) {
        this.bumpReviewInvalidationEpoch(row.id);
        this.goals.clearStaleVerificationStatus(row.id, { clearBlockedPhase: true });
      }
    } catch {
      // Goals table may be absent or closing.
    }
  }

  /**
   * A current failed verification claim from the coordinator, Director or final verify task (worker claims
   * are display-only). Legacy rows without provenance count only when reported by the run's Tier 1 (or this
   * goal's chat) after the newest review and the current revision.
   */
  private detectCoordinatorFailure(goal: GoalRecord): CoordinatorFailure | null {
    const lp = goal.liveProgress;
    if (!lp || lp.verificationStatus !== "failed") return null;
    if (hasClaimProvenance(lp)) {
      if (!isCurrentClaim(goal, lp)) return null;
      if (!isAuthoritativeSource(lp.verificationSource ?? lp.source)) return null;
    } else {
      const tier1 = goal.linkedRunId
        ? this.db.listTasks(goal.linkedRunId).find((t) => t.tier === 1 && t.profileId !== "director")
        : undefined;
      const by = lp.reportedByTaskId ?? null;
      if (!by || (by !== tier1?.id && by !== goal.chatSessionId)) return null;
      const newestReview = Math.max(
        0,
        ...this.goals.listReviews(goal.id).filter((r) => r.goalVersion === goal.version).map((r) => r.createdAt),
      );
      const revisionAt = this.goals.getLatestRevisionCreatedAt(goal.id, goal.version) ?? 0;
      // Summary-only patches refresh updatedAt; claimedAt keeps when the legacy status was reported.
      const claimedAt = lp.claimedAt ?? lp.updatedAt;
      if (claimedAt <= newestReview || claimedAt <= revisionAt) return null;
    }
    return {
      source: lp.verificationSource ?? lp.source ?? "coordinator",
      summary: lp.summary,
      criteriaResults: (lp.criteriaResults ?? []).filter((c) => !c.passed),
      claimedAt: lp.claimedAt ?? lp.updatedAt,
    };
  }

  /**
   * An explicit blocker on a settled run: a current (or legacy) blocked phase, or a blocked non-director task.
   * A bare open escalation is not a blocker: an earlier breaker's P0 stays open across a manual resume.
   */
  private detectExplicitBlocker(goal: GoalRecord, runId: string): ExplicitBlocker | null {
    const lp = goal.liveProgress;
    const phaseBlocked = Boolean(lp && isBlockedPhase(lp.phase) && (!hasClaimProvenance(lp) || isCurrentClaim(goal, lp)));
    const tasks = this.db.listTasks(runId);
    const blockedTasks = tasks.filter((t) => t.profileId !== "director" && t.status === "blocked");
    if (!phaseBlocked && blockedTasks.length === 0) return null;
    const blockedIds = new Set(blockedTasks.map((t) => t.id));
    const escalation = this.db
      .listEscalations(runId, "open")
      .find((e) => e.blocking && blockedIds.has(e.fromTaskId));
    const tier1 = tasks.find((t) => t.tier === 1 && t.profileId !== "director");
    const summary =
      (phaseBlocked ? lp?.summary?.trim() : undefined) ||
      escalation?.title ||
      blockedTasks[0]?.summary?.trim() ||
      (blockedTasks[0] ? `task ${blockedTasks[0].id} is blocked` : "blocked without a summary");
    const remediation = (lp?.criteriaResults ?? [])
      .filter((c) => !c.passed && c.remediation)
      .map((c) => `${c.criterionId}: ${c.remediation}`);
    return { summary, remediation, escalationId: escalation?.id ?? null, tier1Id: tier1?.id ?? null };
  }

  /** Hands a blocked goal to a human: no gates, awaiting_human, and one blocking escalation (reused when present). */
  private parkBlockedGoal(goal: GoalRecord, runId: string, blocker: ExplicitBlocker): void {
    this.goals.setStatus(goal.id, "awaiting_human", `Blocked: ${blocker.summary.slice(0, 240)}`);
    if (blocker.escalationId || !blocker.tier1Id) return;
    const evidenceDir = goalEvidenceDirOrNull(goal);
    this.suppressEscalationWakeRuns.add(runId);
    try {
      this.db.createEscalation({
        runId,
        fromTaskId: blocker.tier1Id,
        toTaskId: null,
        severity: "P0",
        category: "blocked_dependency",
        title: `Goal ${goal.id} blocked: ${blocker.summary.slice(0, 160)}`,
        attempted: blocker.remediation,
        evidencePaths: [evidenceDir && existsSync(evidenceDir) ? evidenceDir : goal.repoRoot],
        blocking: true,
      });
    } finally {
      this.suppressEscalationWakeRuns.delete(runId);
    }
  }

  /**
   * Creates the detached `_verify` worktree at the run's base branch tip (gates and reviewers test the real
   * target branch without touching the host checkout). Without a base branch, verification runs in the repo.
   */
  private async prepareVerifyCheckout(
    goal: GoalRecord,
    run: RunRecord | null,
  ): Promise<{ verifyCwd: string; verifyRef: string; isolated: boolean; cleanup: () => Promise<void> }> {
    let verifyCwd = goal.repoRoot;
    let verifyRef = run?.baseBranch || "HEAD";
    let verifyScratch: { path: string; remove: () => Promise<void> } | null = null;
    const cleanup = async () => {
      const scratch = verifyScratch;
      verifyScratch = null;
      await scratch?.remove();
    };
    if (run?.baseBranch) {
      const targetSha = await runGit(goal.repoRoot, ["rev-parse", "--verify", `${run.baseBranch}^{commit}`])
        .then((r) => r.stdout)
        .catch(() => null);
      if (targetSha) {
        const wtMgr = this.orchestrator.getWorktreeManager(goal.repoRoot);
        const scratch = await wtMgr.addScratchWorktree("_verify", `${goal.id}-${randomUUID().slice(0, 8)}`, targetSha);
        verifyScratch = scratch;
        verifyCwd = scratch.path;
        verifyRef = targetSha;
        try {
          // Do NOT silently swallow bootstrap failures — fail verification cleanly if setupCommand fails
          await wtMgr.bootstrapWorktree(scratch.path);
        } catch (err) {
          verifyScratch = null;
          await scratch.remove();
          throw err;
        }
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
    return { verifyCwd, verifyRef, isolated: Boolean(verifyScratch), cleanup };
  }

  /**
   * Audited acceptance override (Director goal_accept or user `goal.accept`). Validates readable evidence,
   * stops an active review, re-runs required deterministic gates (a failure rejects unless overridden), then
   * records an approved `director_accept_override` review with its audit. Before the commit every failure
   * restores the goal's previous status; queue removal, run completion and wakes happen only after it.
   */
  async acceptGoalOverride(
    goalId: string,
    input: {
      reason: string;
      evidencePaths: string[];
      overrideDeterministic?: boolean;
      source: "director" | "user";
      chatSessionId?: string | null;
    },
  ): Promise<{ goal: GoalRecord; review: GoalReviewVerdict }> {
    const reason = String(input.reason ?? "").trim();
    const rawPaths = (input.evidencePaths ?? []).map((p) => String(p).trim()).filter(Boolean);
    if (!reason) throw new Error("goal_accept requires a non-empty reason");
    if (rawPaths.length === 0) throw new Error("goal_accept requires non-empty evidence_paths");
    const checkAcceptable = (g: GoalRecord | null): GoalRecord => {
      if (!g) throw new Error(`Unknown goal ${goalId}`);
      if (g.status === "draft" || g.status === "achieved" || g.status === "cancelled") {
        throw new Error(`Goal ${g.id} is ${g.status}; nothing to accept.`);
      }
      if (g.status === "paused" && input.source === "director") {
        throw new Error(`Goal ${g.id} is paused by the user; ask the user before accepting it.`);
      }
      return g;
    };
    const initial = checkAcceptable(this.goals.getGoal(goalId));
    const evidence = rawPaths.map((p) => resolveDirectorEvidencePath(initial.repoRoot, p));

    const running = this.activeReviews.get(initial.id);
    if (running) {
      running.abort(new Error(`Review superseded by goal_accept for ${initial.id}`));
      const deadline = Date.now() + 5_000;
      while (this.activeReviews.has(initial.id) && Date.now() < deadline) await new Promise((r) => setTimeout(r, 50));
      if (this.activeReviews.has(initial.id)) throw new Error(`Review of ${initial.id} is still stopping; retry goal_accept shortly.`);
    }
    // Re-read after a stopped review: it may have moved the goal (its status is what a failure restores).
    const goal = checkAcceptable(this.goals.getGoal(initial.id));
    const slot = new AbortController();
    this.activeReviews.set(goal.id, slot);
    const prevStatus = goal.status;
    let committed = false;
    try {
      const run = goal.linkedRunId ? this.db.getRun(goal.linkedRunId) : null;
      let gateResults: CriterionEvaluation[] = [];
      let diffHash = `director-accept-${Date.now()}`;
      let verifyRef: string | null = null;
      let failedDeterministic: string[] = [];
      if (goal.criteria.some((c) => c.kind === "deterministic")) {
        const checkout = await this.prepareVerifyCheckout(goal, run);
        try {
          // A pause, amend or cancel during checkout must stop before the gates write "verifying" over it.
          if (slot.signal.aborted) {
            throw slot.signal.reason instanceof Error ? slot.signal.reason : new Error(`goal_accept for ${goal.id} was interrupted`);
          }
          const fresh = checkAcceptable(this.goals.getGoal(goal.id));
          if (fresh.version !== goal.version) throw new Error(`Goal ${goal.id} changed during goal_accept`);
          verifyRef = checkout.verifyRef;
          const gate = await this.goals.runDeterministicGates(goal.id, run?.baseSha, slot.signal, {
            cwd: checkout.verifyCwd,
            targetRef: checkout.verifyRef,
          });
          gateResults = gate.results;
          diffHash = gate.diffHash;
        } finally {
          await checkout.cleanup();
        }
        const required = new Set(goal.criteria.filter((c) => c.required).map((c) => c.id));
        failedDeterministic = gateResults.filter((r) => !r.passed && required.has(r.criterionId)).map((r) => r.criterionId);
        if (failedDeterministic.length > 0 && !input.overrideDeterministic) {
          if (goal.linkedRunId) {
            this.db.emitEvent(goal.linkedRunId, null, "goal.accept_rejected", {
              goalId: goal.id,
              version: goal.version,
              failedCriteria: failedDeterministic,
              source: input.source,
            });
          }
          throw new Error(
            `goal_accept rejected for ${goal.id}: required deterministic gate(s) still fail (${failedDeterministic.join(", ")}). ` +
              "Fix them, or pass override_deterministic: true to accept anyway (recorded in the audit).",
          );
        }
      }

      const by = input.source === "user" ? "USER" : "DIRECTOR";
      const evidenceSummary = `Evidence: ${evidence.map((e) => e.canonicalPath).join(", ")}`;
      const covered = new Set(gateResults.map((r) => r.criterionId));
      const criteriaResults: CriterionEvaluation[] = [
        ...gateResults,
        ...goal.criteria
          .filter((c) => !covered.has(c.id))
          .map((c) => ({ criterionId: c.id, passed: true, evidence: `[${by} ACCEPTED] ${reason} (${evidenceSummary})` })),
      ];
      const audit: GoalAcceptOverrideAudit = {
        acceptedBy: input.source,
        chatSessionId: input.chatSessionId ?? goal.chatSessionId ?? null,
        reason,
        evidence,
        overrideDeterministic: input.overrideDeterministic === true,
        failedDeterministicCriteria: failedDeterministic,
        prevStatus,
        goalVersion: goal.version,
        verifyRef,
        acceptedAt: Date.now(),
      };
      const summary =
        `[${by} ACCEPTED OVERRIDE] ${reason} (${evidenceSummary})` +
        (failedDeterministic.length ? ` [deterministic override: ${failedDeterministic.join(", ")}]` : "");
      // A pause, amend or cancel during the gates aborts this slot: never commit over it.
      if (slot.signal.aborted) {
        throw slot.signal.reason instanceof Error ? slot.signal.reason : new Error(`goal_accept for ${goal.id} was interrupted`);
      }
      const outcome = this.goals.acceptGoalOverride(goal.id, {
        expectedVersion: goal.version,
        criteriaResults,
        summary,
        diffHash,
        audit,
      });
      committed = true;
      this.finishAcceptedGoal(outcome.goal, outcome.review, input.source);
      return outcome;
    } catch (err) {
      if (!committed) {
        try {
          // Undo only the gates' own "verifying" state; a newer pause, cancel or other transition stands.
          const current = this.goals.getGoal(goal.id);
          if (current && current.version === goal.version && current.status === "verifying" && prevStatus !== "verifying") {
            this.goals.setStatus(goal.id, prevStatus);
          }
        } catch {
          // DB may be closing during shutdown
        }
      }
      throw err;
    } finally {
      if (this.activeReviews.get(goal.id) === slot) this.activeReviews.delete(goal.id);
    }
  }

  /** Post-commit side effects of an accepted override; each step is best-effort (the goal is already achieved). */
  private finishAcceptedGoal(goal: GoalRecord, review: GoalReviewVerdict, source: "director" | "user"): void {
    const attempt = (fn: () => unknown) => {
      try {
        const r = fn();
        if (r instanceof Promise) r.catch(() => {});
      } catch {
        // best-effort
      }
    };
    this.undeliveredAmendments.delete(goal.id);
    attempt(() => this.db.admissionQueue.remove("goal", goal.id));
    const runId = goal.linkedRunId;
    if (runId) {
      attempt(async () => {
        const st = this.orchestrator.getEffectiveRun(runId)?.status;
        if (st === "active" || st === "paused_quota") await this.orchestrator.stopRun(runId, `Goal ${goal.id} accepted by ${source}`);
        this.db.updateRunStatus(runId, "completed");
      });
      attempt(() => this.autoResolveGoalEscalations(runId, `Auto-resolved: goal ${goal.id} accepted by ${source}`));
      attempt(() =>
        this.db.emitEvent(runId, null, "goal.reviewed", {
          goalId: goal.id,
          version: review.goalVersion,
          cycle: review.cycle,
          verdict: review.verdict,
          status: goal.status,
          summary: review.summary,
          source,
        }),
      );
    }
    if (goal.chatSessionId) {
      attempt(() => {
        for (const t of this.db.timers.list({ ownerKind: "chat", ownerId: goal.chatSessionId!, status: "pending" })) {
          if (timerMatchesGoalSettlement(t, goal)) this.db.timers.cancel(t.id, `goal_settled:${goal.id}`);
        }
      });
    }
    attempt(() => this.drainGoalQueue(goal.repoRoot));
    attempt(() => this.syncQueuedGoalProgress());
    if (goal.chatSessionId) {
      const event: ChatControlEvent = {
        kind: "goal.reviewed",
        goalId: goal.id,
        goalVersion: review.goalVersion,
        cycle: review.cycle,
        verdict: review.verdict,
        status: goal.status,
        summary: review.summary,
        source,
      };
      attempt(() => {
        // The Director's own accept never wakes it; a user accept wakes the owning Director.
        if (source === "user" && this.wakeHandler && this.config.events?.wakeDirectorOnGoalSettle !== false) {
          this.wakeHandler(goal.chatSessionId!, event);
        } else {
          this.chats.appendTurn(goal.chatSessionId!, "system_notice", formatChatEventSummary(event), undefined, event);
        }
      });
    }
  }

  /**
   * Executes the full deterministic Verification Gates -> Independent Skeptical Done-Review pipeline.
   * Never lets the generator self-approve: invokes a fresh read-only Reviewer session (or fails fast at $0 LLM cost on deterministic gates).
   */
  async verifyAndReviewGoal(
    goalId: string,
    signal?: AbortSignal,
    opts?: { verificationMode?: VerificationMode; summary?: string; autoTriggered?: boolean; settledRunStatus?: "failed" },
  ): Promise<{
    goal: GoalRecord;
    review: GoalReviewVerdict;
    circuitBreakerTripped?: string;
  }> {
    const goal = this.goals.getGoal(goalId);
    if (!goal) throw new Error(`Unknown goal ${goalId}`);
    if (this.activeReviews.has(goal.id)) throw new Error(`Review already in progress for ${goal.id}`);
    if (goal.status === "paused" || goal.status === "cancelled") throw new Error(`Goal is ${goal.status}`);
    // An unacknowledged amendment decides this settlement itself: reject without touching the goal or its recovery.
    const pendingAmend = this.undeliveredAmendments.get(goal.id);
    if (pendingAmend && pendingAmend.version === goal.version) throw new PendingAmendmentError(goal.id, goal.version, pendingAmend.reason);
    const abort = new AbortController();
    this.activeReviews.set(goal.id, abort);
    const reviewSignal = signal ? AbortSignal.any([abort.signal, signal]) : abort.signal;
    const unsubscribe = this.db.onEvent(ev => {
      if (ev.runId !== goal.linkedRunId) return;
      const runStatus = ev.type === "run.status" ? (ev.payload as { status?: string } | null)?.status : undefined;
      if (runStatus === "stopped") abort.abort(new Error("Run stopped during review"));
      // Only a genuine resume aborts: the Done-Reviewer running on this run, or a snapshot deriving
      // "active" from it, must never cancel the review it belongs to.
      // A real execution activating (registered in the orchestrator, which the Done-Reviewer never is) also counts.
      const taskTo = ev.type === "task.status" ? (ev.payload as { to?: string } | null)?.to : undefined;
      if (
        opts?.settledRunStatus === "failed" &&
        (ev.type === "task.resumed" ||
          ((runStatus === "active" || taskTo === "running") && this.orchestrator.isRunActiveInMemory(ev.runId)))
      ) {
        abort.abort(new Error("Run resumed during failed-run verification"));
      }
    });
    try {
      return await this.verifyAndReviewGoalInner(goal, reviewSignal, opts);
    } catch (error) {
      if (!this.disposed) {
        try {
          const current = this.goals.getGoal(goal.id);
          if (current?.version === goal.version && ["executing", "verifying", "reviewing"].includes(current.status)) {
            if (error instanceof PendingAmendmentError || this.isRunOccupyingGoalSlot(current.linkedRunId)) {
              if (current.status !== goal.status) this.goals.setStatus(goal.id, goal.status);
            } else {
              const msg = error instanceof Error ? error.message : String(error);
              this.goals.setStatus(
                goal.id,
                "awaiting_human",
                error instanceof FailedRunGatesError
                  ? `Linked run ${current.linkedRunId ?? "unknown"} failed and required deterministic gate(s) still fail (${error.failedIds.join(", ")}); resume or review.`
                  : `Verification failed: ${msg.slice(0, 240)}`,
              );
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
    opts?: { verificationMode?: VerificationMode; summary?: string; autoTriggered?: boolean; settledRunStatus?: "failed" },
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
      const pendingAmend = this.undeliveredAmendments.get(goal.id);
      if (pendingAmend && pendingAmend.version === goal.version) {
        throw new PendingAmendmentError(goal.id, goal.version, pendingAmend.reason);
      }
      if (opts?.settledRunStatus === "failed" && run && this.orchestrator.isRunActiveInMemory(run.id)) {
        throw new Error("Run resumed during failed-run verification");
      }
    };
    assertCurrent();
    // Approving a failed run resolves its escalations; an outstanding P0 (possibly reopened mid-verification) needs a human.
    const assertNoOpenP0ForFailedRunApproval = () => {
      if (opts?.settledRunStatus !== "failed") return;
      // Unresolved includes a P0 reopened (acked) after an earlier resolution.
      const pendingP0 = goal.linkedRunId
        ? this.db.listEscalations(goal.linkedRunId, "unresolved").find((e) => e.severity === "P0")
        : undefined;
      if (pendingP0) {
        throw new Error(`Cannot auto-approve failed run while P0 escalation ${pendingP0.id} (${pendingP0.title}) is ${pendingP0.status}`);
      }
    };
    // A current failed claim by the coordinator blocks approval; re-read after gates and after the reviewer.
    let coordinatorFailure = this.detectCoordinatorFailure(goal);

    const committedMode: VerificationMode = goal.verificationMode ?? "auto";
    if (opts?.verificationMode === "none" && committedMode !== "none") {
      throw new Error(
        `Cannot downgrade verification_mode to 'none' on goal ${goal.id} (committed mode is '${committedMode}'). Amend the goal if criteria are no longer required.`,
      );
    }
    const effectiveMode: VerificationMode = opts?.verificationMode ?? committedMode;
    if (effectiveMode === "none") {
      if (coordinatorFailure) {
        throw new Error(
          `Goal ${goal.id}: coordinator reported verification failed for the current revision (${(coordinatorFailure.summary ?? "no summary").slice(0, 300)}). ` +
            "Resume the run, amend the goal, or use goal_accept with evidence.",
        );
      }
      const directResults: CriterionEvaluation[] = goal.criteria.map((c) => ({
        criterionId: c.id,
        passed: true,
        evidence: opts?.summary?.trim() || "Marked complete without separate verification pass (verification_mode=none).",
      }));
      const outcome = this.db.transaction(() => {
        assertNoOpenP0ForFailedRunApproval();
        return this.goals.recordReviewVerdict(goal.id, {
          verdict: "approved",
          criteriaResults: directResults,
          blockingIssues: [],
          summary: opts?.summary?.trim() || "Goal completed directly (verification_mode=none).",
          diffHash: `direct-${Date.now()}`,
          source: "none",
        });
      });
      await this.handlePostReviewTransition(outcome, { autoTriggered: opts?.autoTriggered });
      return outcome;
    }

    const checkout = await this.prepareVerifyCheckout(goal, run);
    const verifyCwd = checkout.verifyCwd;
    const verifyRef = checkout.verifyRef;
    try {
      // Gates mark the goal's current revision verifying: a review made stale during checkout must stop first.
      assertCurrent();
      const gateResult = await this.goals.runDeterministicGates(goal.id, run?.baseSha, signal, {
        cwd: verifyCwd,
        targetRef: verifyRef,
      });
      assertCurrent();
      // The coordinator may have reported (or confirmed) while gates ran: settle on its latest claim.
      const latestGoal = this.goals.getGoal(goal.id) ?? goal;
      coordinatorFailure = this.detectCoordinatorFailure(latestGoal);

      // If deterministic gates failed, record immediately without burning LLM review tokens!
      if (!gateResult.allPassed) {
        if (opts?.settledRunStatus === "failed") {
          // A failed run is not bounced back to Tier 1: no review cycle, breaker, escalation or relaunch.
          const failed = gateResult.results.filter((r) => !r.passed);
          const requiredIds = new Set(goal.criteria.filter((c) => c.required).map((c) => c.id));
          const failedRequired = failed.filter((r) => requiredIds.has(r.criterionId));
          const reportFailed = failedRequired.length ? failedRequired : failed;
          const evidenceTail = reportFailed.map((r) => `${r.criterionId}: ${r.evidence}`).join(" | ").slice(-500);
          throw new FailedRunGatesError(reportFailed.map((r) => r.criterionId), evidenceTail);
        }
        const outcome = this.goals.recordReviewVerdict(goal.id, {
          verdict: "changes_requested",
          criteriaResults: gateResult.results,
          blockingIssues: gateResult.results.filter((r) => !r.passed).map((r) => r.evidence),
          summary: "Deterministic verification gates failed; bouncing back to Tier 1 before LLM review.",
          diffHash: gateResult.diffHash,
          spentUsd: this.sumRunCostUsd(goal.linkedRunId),
          source: "deterministic",
        });
        await this.handlePostReviewTransition(outcome, { autoTriggered: opts?.autoTriggered });
        return outcome;
      }

      // Agent-chosen verification depth: when deterministic checks already cover the goal (or mode='deterministic'),
      // approve cleanly at $0 LLM cost without spawning a redundant independent reviewer session!
      if (!shouldRunIndependentReview(goal, effectiveMode)) {
        // A coordinator-reported failure, or a silent settle after one bounced, is never approved at $0.
        const lastReview = this.goals
          .listReviews(goal.id)
          .filter((r) => r.goalVersion === goal.version)
          .at(-1);
        const lp = latestGoal.liveProgress;
        const confirmedAfterBounce =
          lp != null &&
          isCurrentClaim(goal, lp) &&
          isAuthoritativeSource(lp.verificationSource ?? lp.source) &&
          (lp.verificationStatus === "passed" || lp.verificationStatus === "not_needed");
        const unconfirmedBounce = !coordinatorFailure && lastReview?.source === "coordinator_failed" && !confirmedAfterBounce;
        if (coordinatorFailure || unconfirmedBounce) {
          const coveredIds = new Set(gateResult.results.map((r) => r.criterionId));
          const reported = coordinatorFailure?.criteriaResults ?? [];
          const criteriaResults: CriterionEvaluation[] = [
            ...gateResult.results,
            ...reported.filter((c) => !coveredIds.has(c.criterionId)),
          ];
          const head = coordinatorFailure
            ? `Coordinator reported verification failed for v${goal.version} cycle ${goal.reviewCycle}: ${(coordinatorFailure.summary ?? "no summary").slice(0, 400)}`
            : `Coordinator has not confirmed verification after its failed report in cycle ${lastReview!.cycle}.`;
          const outcome = this.goals.recordReviewVerdict(goal.id, {
            verdict: "changes_requested",
            criteriaResults,
            blockingIssues: [
              head,
              ...reported.map((c) => `${c.criterionId}: ${c.evidence}${c.remediation ? ` | Fix: ${c.remediation}` : ""}`),
            ],
            summary:
              (coordinatorFailure
                ? "Deterministic checks passed but the coordinator reported verification failed; not approving."
                : "Deterministic checks passed but the coordinator has not confirmed verification after its failed report; not approving.") +
              " Re-verify, then call report_goal_progress with verification_status passed or failed, or phase blocked.",
            diffHash: gateResult.diffHash,
            spentUsd: this.sumRunCostUsd(goal.linkedRunId),
            source: "coordinator_failed",
          });
          await this.handlePostReviewTransition(outcome, { autoTriggered: opts?.autoTriggered });
          return outcome;
        }
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
        // Re-checked in the verdict's transaction so a resume or reopened P0 cannot slip in before it is recorded.
        const outcome = this.db.transaction(() => {
          assertCurrent();
          assertNoOpenP0ForFailedRunApproval();
          return this.goals.recordReviewVerdict(goal.id, {
            verdict: "approved",
            criteriaResults: allResults,
            blockingIssues: [],
            summary:
              opts?.summary?.trim() ||
              (gateResult.results.length > 0
                ? `All ${gateResult.results.length} deterministic check(s) passed; approved without redundant LLM review.`
                : "Approved cleanly without redundant LLM review."),
            diffHash: gateResult.diffHash,
            source: "deterministic",
          });
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

      const reviewerMaxTurns = this.config.rollover.reviewerMaxTurns;
      const goalsDir = join(goal.repoRoot, ".harness", "goals");
      const artifactsDir = resolve(goal.repoRoot, this.config.worktree.artifactsDir ?? ".harness/artifacts");
      // A rejected (e.g. symlinked) evidence directory is never advertised or granted to the reviewer.
      const evidenceDir = ensureGoalEvidenceDir(goal.repoRoot, goal.id);
      const reviewerPrompt = this.goals.buildReviewerPrompt(goal, gateResult, diffStat, diffPatch, {
        reviewerMaxTurns,
        ...(evidenceDir !== null
          ? {
              evidence: {
                goalEvidenceDir: evidenceDir,
                goalsDir,
                artifactsDir,
                ...(run ? { runArtifactsDir: join(artifactsDir, run.id) } : {}),
              },
            }
          : {}),
        ...(coordinatorFailure ? { coordinatorFailure } : {}),
      });
      // Prefer an independent model family different from the authoring run's Tier 1/Tier 2 model
      const runTasks = run ? this.db.listTasks(run.id) : [];
      const authorModel =
        runTasks.find((t) => t.tier === 2 && t.model)?.model ??
        runTasks.find((t) => t.tier === 1 && t.model)?.model ??
        undefined;
      const { profile, model, wireModel, effort: reviewerEffort } = await this.resolveReviewerRoute(verifyCwd, authorModel);
      const makeRunner = () =>
        this.customRunnerFactory
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
        description:
          "Submit the structured Independent Done-Review verdict. criteriaResults items are {criterionId, passed (boolean true only when verified), evidence, remediation?}; an invalid verdict is rejected with an error you can fix and resubmit.",
        inputSchema: {
          type: "object",
          required: ["verdict", "criteriaResults", "summary"],
          properties: {
            verdict: { type: "string", enum: ["approved", "changes_requested", "escalate_human"] },
            criteriaResults: {
              type: "array",
              items: {
                type: "object",
                required: ["criterionId", "passed", "evidence"],
                properties: {
                  criterionId: { type: "string" },
                  passed: { type: "boolean" },
                  evidence: { type: "string" },
                  remediation: { type: "string" },
                },
              },
            },
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
        maxTurns: reviewerMaxTurns,
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
        if (reviewerSignal.aborted && !(timedOut && !signal.aborted)) {
          this.db.updateTask(reviewerTask.id, { status: "cancelled", summary: "Reviewer cancelled" });
          return;
        }
        emit("agent.error", { error: msg });
        this.db.updateTask(reviewerTask.id, { status: "failed", summary: failedSummary });
      };

      let outcome: ReturnType<GoalEngine["recordReviewVerdict"]>;
      let turnsUsed = 0;
      let timedOut = false;
      // O1: the wall-clock limit fired after a valid verdict was submitted: record that verdict.
      const submittedBeforeTimeout = () => timedOut && verdictBox.value !== null && !signal.aborted;
      const bridgeExt = import.meta.url.endsWith(".js") ? "mcp-bridge.js" : "mcp-bridge.ts";
      const mcpBridgeEntrypoint = join(dirname(fileURLToPath(import.meta.url)), "..", "runners", bridgeExt);
      // Each reviewer session gets its own executor; one from an ended session can never fill the verdict.
      let activeAttempt = 0;
      const makeReviewerExecuteTool = (attempt: number) => async (name: string, args: Record<string, unknown>) => {
        if (name === "submit_review_verdict") {
          if (attempt !== activeAttempt) throw new Error("submit_review_verdict: this reviewer session has ended");
          const v = normalizeReviewerVerdict<Partial<SubmittedVerdict>>(args);
          if (!v) {
            throw new Error(
              "submit_review_verdict: verdict must be approved | changes_requested | escalate_human, with criteriaResults[{criterionId, passed, evidence}]",
            );
          }
          verdictBox.value = {
            verdict: v.verdict as SubmittedVerdict["verdict"],
            criteriaResults: v.criteriaResults ?? [],
            blockingIssues: v.blockingIssues ?? [],
            summary: typeof v.summary === "string" ? v.summary : "",
          };
          return { recorded: true };
        }
        throw new Error(`Unknown reviewer tool ${name}`);
      };
      // The 10-minute wall-clock limit applies to each reviewer session.
      let reviewerTimeout: ReturnType<typeof setTimeout> | undefined;
      const armReviewerTimeout = () => {
        clearTimeout(reviewerTimeout);
        reviewerTimeout = setTimeout(() => {
          timedOut = true;
          abort.abort(new Error("Independent Done-Reviewer timed out after 10 minutes"));
        }, 600_000);
        reviewerTimeout.unref?.();
      };
      const retryNote =
        "RETRY: an earlier reviewer session for this exact review ended without a usable verdict (no accepted submit_review_verdict call and no parseable JSON verdict in its final message). " +
        "Verify independently again, then call submit_review_verdict exactly once before you end your turn; if the tool is unavailable, end with one fenced json verdict object. " +
        "Ending without a verdict fails this review.";
      try {
        if (this.db.getTask(reviewerTask.id)?.status === "cancelled") abort.abort(new Error("Reviewer cancelled"));
        this.db.updateTask(reviewerTask.id, { status: reviewerSignal.aborted ? "cancelled" : "running" });
        reviewerSignal.throwIfAborted();
        for (let attempt = 1; ; attempt++) {
          if (attempt > 1) {
            // A cancelled, stopped or otherwise settled reviewer task ends the review between sessions.
            reviewerSignal.throwIfAborted();
            if (this.db.getTask(reviewerTask.id)?.status !== "running") throw new Error("Reviewer cancelled");
          }
          activeAttempt = attempt;
          verdictBox.value = null;
          turnsUsed = 0;
          armReviewerTimeout();
          const executeTool = makeReviewerExecuteTool(attempt);
          this.activeReviewerTools.set(reviewerTask.id, { tools: [submitTool], execute: executeTool });
          const runner = makeRunner();
          const cost = recordCostInvocation(this.db, {
            chatSessionId: goal.chatSessionId ?? undefined,
            runId: reviewRun.id,
            taskId: reviewerTask.id,
            activity: "done_review",
            profile,
            model,
            wireModel,
          });
          let res: Awaited<ReturnType<typeof runner.run>> | null = null;
          try {
            res = await runner.run({
              runId: reviewRun.id,
              taskId: reviewerTask.id,
              tier: 3,
              kind: "verify",
              cwd: verifyCwd,
              toolPolicy: "read_only_workspace",
              allowShellInReadOnly: checkout.isolated,
              model: wireModel,
              effort: reviewerEffort,
              systemPrompt:
                "You are an independent read-only Done-Reviewer operating in a disposable verification checkout of the target branch. Inspect the implementation, diffs, and verification evidence for each required criterion — you may run tests, linters, git diff, rg, sg, or tmux checks freely to verify behavior, but never edit tracked repository code or delegate. Call submit_review_verdict if available, or return the requested JSON verdict with concrete sources for every required criterion. " +
                `You may get one advisory progress check-in around ${reviewerMaxTurns} tool-call turns; the harness does not stop you by turn count. The review has a hard 10-minute wall-clock limit. Submit as soon as every required criterion has evidence; a changes_requested verdict with partial evidence is better than none.`,
              initialPrompt: attempt === 1 ? reviewerPrompt : `${reviewerPrompt}\n\n${retryNote}`,
              maxTurns: reviewerMaxTurns,
              tools: [submitTool],
              executeTool,
              daemonSocketPath: this.config.socketPath,
              mcpBridgeEntrypoint,
              externalMcpServers: resolveExternalMcpServers(this.config.stateDir, this.config.externalMcpServers),
              extraPathDirs: nativeToolsPathDirs(this.config.stateDir),
              extraReadRoots: [
                goalsDir,
                join(goal.repoRoot, ".harness", "artifacts"),
                // The advertised (possibly nondefault) artifacts root must be readable too.
                ...(artifactsDir !== join(goal.repoRoot, ".harness", "artifacts") ? [artifactsDir] : []),
                ...(this.config.worktree.extraReadRoots ?? []),
              ],
              extraEnv: {
                ...(evidenceDir !== null ? { HARNESS_GOAL_EVIDENCE_DIR: evidenceDir } : {}),
                HARNESS_GOAL_ID: goal.id,
                HARNESS_GOALS_DIR: goalsDir,
                HARNESS_ARTIFACTS_DIR: artifactsDir,
              },
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
                onTurn: (n) => {
                  turnsUsed = n;
                },
                onPassiveQuota: (sig) => this.quota.ingestPassiveSignal(sig),
              },
            });
          } catch (err) {
            if (!submittedBeforeTimeout()) throw err;
          } finally {
            cost.finish();
          }
          const keepSubmitted = submittedBeforeTimeout();
          if (!keepSubmitted) {
            reviewerSignal.throwIfAborted();
            if (res && res.stopReason !== "completed") {
              throw new Error(`Reviewer stopped: ${res.stopReason}. ${res.text}`);
            }
          }
          assertCurrent();

          // Fallback JSON parse if reviewer returned JSON text instead of calling submit_review_verdict
          if (!verdictBox.value && res) {
            const parsed = parseReviewerVerdictJson<Partial<SubmittedVerdict>>(res.text);
            if (parsed) {
              verdictBox.value = {
                verdict: parsed.verdict as SubmittedVerdict["verdict"],
                criteriaResults: parsed.criteriaResults ?? [],
                blockingIssues: parsed.blockingIssues ?? [],
                summary: typeof parsed.summary === "string" ? parsed.summary : "",
              };
            }
          }
          if (verdictBox.value) break;

          // A completed session without a usable verdict is retried in a fresh session; nothing is recorded for it.
          const noVerdict =
            `Reviewer returned no usable verdict (attempt ${attempt}/${REVIEWER_VERDICT_ATTEMPTS}, stopReason=${res?.stopReason ?? "unknown"}, ` +
            `turnsUsed=${turnsUsed}, reviewerMaxTurns=${reviewerMaxTurns} advisory). Output tail: ${(res?.text ?? "").slice(-300)}`;
          if (attempt >= REVIEWER_VERDICT_ATTEMPTS) throw new Error(noVerdict);
          emit("agent.error", { error: `${noVerdict} Retrying in a fresh reviewer session.` });
        }

        const submittedVerdict = verdictBox.value;
        if (!submittedVerdict) throw new Error("Reviewer returned no usable verdict");

        // Merge deterministic gate pass results with reviewer evaluations so deterministic criteria stay evidenced
        const mergedResults: CriterionEvaluation[] = [...gateResult.results];
        for (const r of submittedVerdict.criteriaResults ?? []) {
          if (!mergedResults.some((m) => m.criterionId === r.criterionId)) {
            mergedResults.push(r);
          }
        }
        // Coordinator-failed criteria that neither the gates nor the reviewer evaluated stay failed.
        for (const c of coordinatorFailure?.criteriaResults ?? []) {
          if (!mergedResults.some((m) => m.criterionId === c.criterionId)) {
            mergedResults.push({
              criterionId: c.criterionId,
              passed: false,
              evidence: `Coordinator reported this criterion failed and the reviewer did not evaluate it: ${c.evidence}`,
              ...(c.remediation ? { remediation: c.remediation } : {}),
            });
          }
        }
        const notes = [
          coordinatorFailure && coordinatorFailure.criteriaResults.length === 0
            ? "(coordinator-reported failure was shown to the reviewer)"
            : null,
          authorModel && model === authorModel ? "(reviewer model equals author model: no independent route available)" : null,
        ].filter(Boolean);
        const baseSummary = submittedVerdict.summary;

        // A coordinator failure reported after the reviewer's prompt was built was never seen: it blocks approval.
        const postReviewGoal = this.goals.getGoal(goal.id);
        const lateFailure = postReviewGoal ? this.detectCoordinatorFailure(postReviewGoal) : null;
        const unseenFailure =
          submittedVerdict.verdict === "approved" &&
          lateFailure &&
          (!coordinatorFailure || (lateFailure.claimedAt ?? 0) > (coordinatorFailure.claimedAt ?? 0))
            ? lateFailure
            : null;
        const blockingIssues = submittedVerdict.blockingIssues ?? [];
        outcome = this.db.transaction(() => {
          if (submittedVerdict?.verdict === "approved" && !unseenFailure) {
            assertCurrent();
            assertNoOpenP0ForFailedRunApproval();
          }
          const recorded = this.goals.recordReviewVerdict(goal.id, {
            verdict: unseenFailure ? "changes_requested" : submittedVerdict.verdict,
            criteriaResults: mergedResults,
            blockingIssues: unseenFailure
              ? [
                  `Coordinator reported verification failed after the reviewer started: ${(unseenFailure.summary ?? "no summary").slice(0, 400)}`,
                  ...unseenFailure.criteriaResults.map(
                    (c) => `${c.criterionId}: ${c.evidence}${c.remediation ? ` | Fix: ${c.remediation}` : ""}`,
                  ),
                  ...blockingIssues,
                ]
              : blockingIssues,
            summary: unseenFailure
              ? `Reviewer approved, but the coordinator reported verification failed during review; not approving. ${baseSummary}`
              : notes.length ? `${baseSummary} ${notes.join(" ")}` : baseSummary,
            diffHash: gateResult.diffHash,
            spentUsd: this.sumRunCostUsd(goal.linkedRunId),
            source: unseenFailure ? "coordinator_failed" : "review",
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
        // A wall-clock timeout without a verdict is a reviewer failure with the full diagnostic, not a bare abort.
        if (timedOut && !signal.aborted) {
          throw new Error(
            `${failedSummary} (Reviewer did not return a structured verdict before the 10-minute wall-clock limit: ` +
              `stopReason=timeout, turnsUsed=${turnsUsed}, reviewerMaxTurns=${reviewerMaxTurns} advisory; ${detail})`,
          );
        }
        reviewerSignal.throwIfAborted();
        throw new Error(`${failedSummary} (${detail}; turns=${turnsUsed})`);
      } finally {
        activeAttempt = 0;
        clearTimeout(reviewerTimeout);
        this.activeReviewerTools.delete(reviewerTask.id);
        unsubscribe();
      }

      await this.handlePostReviewTransition(outcome, { autoTriggered: opts?.autoTriggered });
      return outcome;
    } finally {
      await checkout.cleanup();
    }
  }

  /**
   * Done-Reviewer route: the fresh enabled catalog's verify recommendation (independent of the author model,
   * ACP preferred), falling back to the same model on any enabled profile, then any enabled route.
   */
  private async resolveReviewerRoute(
    verifyCwd: string,
    authorModel: string | undefined,
  ): Promise<{ profile: Awaited<ReturnType<typeof resolveLaunchRoute>>["profile"]; model: string; wireModel: string; effort: string | undefined }> {
    let models: CompiledModelEntry[] = [];
    try {
      let cat = await this.catalog.getCatalog({ cwd: verifyCwd, excludeExhausted: true });
      if (cat.models.length === 0) cat = await this.catalog.getCatalog({ cwd: verifyCwd, excludeExhausted: false });
      models = cat.models;
    } catch {
      models = this.catalog.loadCached()?.models ?? [];
    }
    const enabledProfiles = this.config.profiles.filter((p) => p.enabled !== false);
    const rec = recommendModelsByTaskKind(models, { avoidModelId: authorModel, enabledProfiles, preferAcp: true }).verify;
    const rungs: Array<{ profileId?: string; model?: string }> = [];
    if (rec?.profile && enabledProfiles.some((p) => p.id === rec.profile)) rungs.push({ profileId: rec.profile, model: rec.model });
    if (rec?.model) rungs.push({ model: rec.model });
    rungs.push({});
    let lastError: unknown;
    for (const rung of rungs) {
      try {
        const route = await resolveLaunchRoute(this.config, {
          tier: 3,
          cwd: verifyCwd,
          profileId: rung.profileId,
          model: rung.model,
          catalog: this.catalog,
          quota: this.quota,
        });
        const matchesRec = rec != null && route.profile.id === rec.profile && route.model === rec.model;
        const effort = matchesRec && rec.effort && rec.effort !== "default" ? rec.effort : undefined;
        return { ...route, effort };
      } catch (err) {
        lastError = err;
      }
    }
    throw lastError;
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
    const reviewedVersion = review.goalVersion;
    let goal = outcome.goal;
    const reviewEpoch = this.getReviewInvalidationEpoch(goal.id);
    const isInvalidated = (g: GoalRecord) =>
      this.getReviewInvalidationEpoch(goal.id) !== reviewEpoch || isReviewInvalidated(g, review);
    const isStillCurrentExecuting = () => {
      const latest = this.goals.getGoal(goal.id);
      return latest !== null && !isInvalidated(latest) && latest.status === "executing";
    };
    if (goal.status === "executing" && isStillCurrentExecuting()) {
      // Wake / steer Tier 1 Orchestrator — or relaunch in-place if Tier 1 already settled!
      const orch = goal.linkedRunId ? this.db.listTasks(goal.linkedRunId).find((t) => t.tier === 1) : undefined;
      let delivered = false;
      if (orch) {
        const unmet = review.criteriaResults.filter((c) => !c.passed);
        const msg = [
          `[INDEPENDENT REVIEW #${review.cycle}: CHANGES REQUESTED for ${goal.id} v${reviewedVersion}]`,
          `Summary: ${review.summary}`,
          ...unmet.map((u) => `- ${u.criterionId}: ${u.evidence}${u.remediation ? ` | Fix: ${u.remediation}` : ""}`),
          ...review.blockingIssues.map((b) => `- Blocker: ${b}`),
        ].join("\n");
        delivered = await this.orchestrator.steerTask(orch.id, msg).catch(() => false);
        if (!delivered && isStillCurrentExecuting()) {
          delivered = await this.orchestrator
            .resumeTaskDetailed(orch.id, { allowCompleted: true, note: msg, validateBeforeActivate: isStillCurrentExecuting })
            // An idempotent "already active" return delivers the note only when it was steered in.
            .then((r) => r.outcome === "resumed" || r.noteDelivered === true, () => false);
        }
      }
      if (!delivered && isStillCurrentExecuting()) {
        const reason = !goal.linkedRunId
          ? "no linked run to apply requested changes"
          : orch
            ? "requested changes could not be delivered to its run"
            : "linked run has no orchestrator to apply requested changes";
        goal = this.handOffStrandedGoal(goal, reason);
        outcome.goal = goal;
      }
    }

    // An amendment, pause, cancel, or later review while the verdict was being delivered invalidates it. The verdict
    // stays in goal_reviews; nothing announces it, and the run, escalations, and Director are left untouched.
    const current = this.goals.getGoal(goal.id);
    if (current && isInvalidated(current)) {
      outcome.goal = current;
      return;
    }
    if (current) {
      goal = current;
      outcome.goal = current;
    }
    // Decided before the escalation below so its P0 wake can be suppressed in favour of the single review wake.
    const willWakeDirector = Boolean(
      goal.chatSessionId &&
        opts?.autoTriggered &&
        this.config.events?.wakeDirectorOnGoalSettle !== false &&
        (goal.status === "achieved" || goal.status === "awaiting_human"),
    );

    if (goal.linkedRunId) {
      this.db.emitEvent(goal.linkedRunId, null, "goal.reviewed", {
        goalId: goal.id,
        version: reviewedVersion,
        cycle: review.cycle,
        verdict: review.verdict,
        status: goal.status,
        circuitBreakerTripped,
        summary: review.summary,
      });

      if (goal.status === "achieved") {
        this.undeliveredAmendments.delete(goal.id);
        this.db.updateRunStatus(goal.linkedRunId, "completed");
        this.autoResolveGoalEscalations(goal.linkedRunId, `Auto-resolved: goal ${goal.id} achieved`);
        void this.drainGoalQueue(goal.repoRoot).catch(() => {});
      } else if (goal.status === "awaiting_human") {
        void this.drainGoalQueue(goal.repoRoot).catch(() => {});
        const orch = this.db.listTasks(goal.linkedRunId).find((t) => t.tier === 1);
        if (orch) {
          const failedIds = review.criteriaResults.filter((c) => !c.passed).map((c) => c.criterionId);
          const attempted = circuitBreakerTripped
            ? [
                ...review.blockingIssues.slice(0, 5),
                ...review.criteriaResults.filter((c) => !c.passed && c.remediation).map((c) => `${c.criterionId}: ${c.remediation}`),
              ]
            : undefined;
          const runId = goal.linkedRunId;
          if (willWakeDirector) this.suppressEscalationWakeRuns.add(runId);
          try {
            this.db.createEscalation({
              runId,
              fromTaskId: orch.id,
              toTaskId: null,
              severity: "P0",
              category: "verification_failed",
              title: circuitBreakerTripped
                ? `${circuitBreakerTripped}${failedIds.length ? ` (failed: ${failedIds.join(", ")})` : ""}`
                : `Goal ${goal.id} needs human review`,
              evidencePaths: [goal.repoRoot],
              ...(attempted ? { attempted } : {}),
              blocking: true,
            });
          } finally {
            if (willWakeDirector) this.suppressEscalationWakeRuns.delete(runId);
          }
        }
      }
    }

    if (goal.chatSessionId) {
      const reviewedEvent: ChatControlEvent = {
        kind: "goal.reviewed",
        goalId: goal.id,
        goalVersion: reviewedVersion,
        cycle: review.cycle,
        verdict: review.verdict,
        status: goal.status,
        summary: review.summary,
        ...(circuitBreakerTripped ? { circuitBreakerTripped } : {}),
      };
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
    return this.db.resolveOpenEscalationsForRun(runId, resolution, { resolvedBy: "goal_settled" }).length;
  }

  /**
   * Resolves one escalation with a recorded reason, independent of run state or a live Tier 1. With a chat
   * session, only escalations on runs that chat owns in its workspace are reachable. Repeats are no-ops.
   */
  resolveEscalation(input: {
    escalationId: string;
    reason: string;
    chatSessionId?: string;
    repoRoot?: string;
    resolvedBy: "director" | "operator";
  }): {
    escalation: EscalationRecord;
    alreadyResolved: boolean;
    goalId: string | null;
    goalStatus: string | null;
    runStatus: string | null;
    remainingOpen: number;
  } {
    const reason = typeof input.reason === "string" ? input.reason.trim().slice(0, 2000) : "";
    if (!reason) throw new Error("reason must be a non-empty string");
    const esc = this.db.getEscalation(input.escalationId);
    if (input.chatSessionId) {
      // Scoped callers get one error for missing and foreign IDs, so existence elsewhere is not revealed.
      const repoRoot = input.repoRoot ?? this.chats.getSession(input.chatSessionId)?.repoRoot;
      if (!esc || !repoRoot || !this.ownedDossierRuns(input.chatSessionId, repoRoot).some((r) => r.id === esc.runId)) {
        throw new Error("Unknown escalation in this workspace");
      }
    }
    if (!esc) throw new Error(`Unknown escalation ${input.escalationId}`);
    const row = this.db.db
      .prepare("SELECT id FROM goals WHERE linked_run_id = ? ORDER BY updated_at DESC LIMIT 1")
      .get(esc.runId) as { id: string } | undefined;
    const goal = row ? this.goals.getGoal(row.id) : null;
    const goalId = goal?.id ?? null;
    const runStatus = this.db.getRun(esc.runId)?.status ?? null;
    const remaining = () => this.db.listEscalations(esc.runId).filter((e) => e.status === "open" || e.status === "acked").length;
    if (esc.status === "resolved") {
      return { escalation: esc, alreadyResolved: true, goalId, goalStatus: goal?.status ?? null, runStatus, remainingOpen: remaining() };
    }
    // Guarded transition: a concurrent or repeated resolve loses the race without a second event or notice.
    const { escalation: updated, transitioned } = this.db.tryResolveEscalation(esc.id, reason, {
      resolvedBy: input.resolvedBy,
      chatSessionId: input.chatSessionId ?? null,
      goalId,
      goalVersion: goal?.version ?? null,
      runStatus,
    });
    if (!transitioned) {
      return { escalation: updated, alreadyResolved: true, goalId, goalStatus: goal?.status ?? null, runStatus, remainingOpen: remaining() };
    }
    const noticeSession = goal?.chatSessionId ?? input.chatSessionId;
    if (noticeSession) {
      this.chats.appendTurn(
        noticeSession,
        "system_notice",
        `Escalation ${esc.id} [${esc.severity}/${esc.category}] on run ${esc.runId} resolved by ${input.resolvedBy}: ${reason}`,
      );
    }
    return { escalation: updated, alreadyResolved: false, goalId, goalStatus: goal?.status ?? null, runStatus, remainingOpen: remaining() };
  }

  /**
   * Cancels all active Director turns, in-flight Done-Reviews, and pending chat timers that would resume a
   * goal (used by `/stop`), so nothing scheduled earlier undoes the explicit stop.
   */
  async cancelAllActive(
    reason = "Stopped by user",
    opts?: { repoRoot?: string },
  ): Promise<{ cancelledChats: number; cancelledReviews: number; cancelledTimers: number }> {
    // With repoRoot, only that repo's chats, reviews and goal-resume timers are cancelled (canonical path match).
    const root = opts?.repoRoot ? canonicalProjectPath(opts.repoRoot) : null;
    const inRepo = (repoRoot: string | null | undefined) => root === null || (repoRoot ? canonicalProjectPath(repoRoot) === root : false);
    let cancelledTimers = 0;
    for (const t of this.db.timers.list({ ownerKind: "chat", status: "pending" })) {
      if (t.resumeGoal !== true) continue;
      if (root !== null) {
        const owner = t.goalId ? this.goals.getGoal(t.goalId)?.repoRoot : this.chats.getSession(t.ownerId)?.repoRoot;
        if (!inRepo(owner)) continue;
      }
      if (this.db.timers.cancel(t.id, reason)) cancelledTimers++;
    }
    let cancelledChats = 0;
    for (const chatId of [...this.activeChats.keys()]) {
      if (!inRepo(this.chatRepoRoot(chatId))) continue;
      if (await this.cancelChat(chatId)) cancelledChats++;
    }
    let cancelledReviews = 0;
    for (const [goalId, abort] of [...this.activeReviews.entries()]) {
      if (!inRepo(this.goals.getGoal(goalId)?.repoRoot)) continue;
      abort.abort(new Error(reason));
      this.activeReviews.delete(goalId);
      cancelledReviews++;
    }
    return { cancelledChats, cancelledReviews, cancelledTimers };
  }

  /** The workspace a chat session currently works in (null when unknown). */
  chatRepoRoot(chatSessionId: string): string | null {
    return this.chats.getSession(chatSessionId)?.repoRoot ?? null;
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
    // project_switch takes effect for every later inner tool call, including later calls in the same script.
    const currentRepoRoot = () => this.chats.getSession(session.id)?.repoRoot ?? session.repoRoot;
    return createCodeModeExecutor({
      ...this.config.codeExec,
      mode: effectiveCodeMode(this.config.codeMode),
      allowedTools: () => DIRECTOR_TOOLS,
      directTools: DIRECTOR_DIRECT_TOOLS,
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
        return this.executeDirectorTool(session.id, currentRepoRoot(), name, args, effectiveSignal, (build) => {
          const id = randomUUID();
          presentations.set(id, { toolName: name, build });
          return id;
        });
      },
      artifactsDir: () => resolve(currentRepoRoot(), this.config.worktree.artifactsDir ?? ".harness/artifacts"),
      pythonCacheDir: join(this.config.stateDir, "code-mode", "python"),
      saveOutput: async (name, json) => {
        signal.throwIfAborted();
        const run = await this.orchestrator.ensureDirectorTaskRun(session.id, currentRepoRoot());
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
    opts?: {
      turnRole?: "user" | "system_notice";
      event?: ChatControlEvent;
      /** Re-checks a wake event after the async model and route resolution; null means it went stale and nothing is delivered. */
      revalidateEvent?: (event: ChatControlEvent) => ChatControlEvent | null;
    },
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
      let typedEvent = opts?.event ?? null;
      let resolvedRoute: Awaited<ReturnType<typeof resolveLaunchRoute>> | null = null;
      if (typedEvent) {
        // Resolve the route before the final freshness check so no await separates it from persisting and delivering the notice.
        resolvedRoute = await resolveLaunchRoute(this.config, {
          tier: 0, cwd: session.repoRoot, profileId: session.profileId, model: session.model, catalog: this.catalog, quota: this.quota,
        });
        if (opts?.revalidateEvent) {
          typedEvent = opts.revalidateEvent(typedEvent);
          if (!typedEvent) throw new StaleWakeEventError();
        }
      } else {
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
              .map((stored) => {
                const g = reconcileGoalWithLiveRun(stored);
                const queued = g.status === "draft" && g.liveProgress?.phase === "queued" ? g.liveProgress.summary : undefined;
                // Live verifying state outranks the review count from earlier cycles.
                const ver = queued ?? deriveGoalVerificationLabel(g);
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

      const { profile, model, wireModel } =
        resolvedRoute ??
        (await resolveLaunchRoute(this.config, {
          tier: 0, cwd: session.repoRoot, profileId: session.profileId, model: session.model, catalog: this.catalog, quota: this.quota,
        }));
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
      if (error instanceof StaleWakeEventError) throw error;
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

  private goalDisplayData(stored: GoalRecord) {
    // Read-only projection: a stale verification claim is hidden, never rewritten.
    const goal = reconcileGoalWithLiveRun(stored);
    const runId = goal.linkedRunId;
    return {
      goal,
      reviews: this.goals.listReviews(goal.id),
      tasks: runId ? this.db.listTasks(runId) : [],
      openEscalations: runId ? this.db.listEscalations(runId, "open") : [],
      activity: runId ? buildRunActivitySummary(this.db, runId) : null,
    };
  }

  /**
   * Whether this chat may cancel or steer tasks of `run`: same repository and not provably another chat's (its
   * delegation bucket, or a goal owned by a different chat). Ownerless goals and runs with no goal (CLI run.create)
   * are allowed. Returns false for another repository; throws a refusal naming the owner for another chat's run.
   */
  private directorMayControlRun(chatSessionId: string, repoRoot: string, run: RunRecord, taskId: string, action: "cancel" | "steer"): boolean {
    if (canonicalProjectPath(run.repoRoot) !== canonicalProjectPath(repoRoot)) return false;
    let ownerChat: string | null = null;
    const bucketOwner = directorTaskRunOwner(run.id);
    if (bucketOwner !== null) {
      if (bucketOwner === chatSessionId) return true;
      const goal = this.goals.getGoal(bucketOwner);
      ownerChat = goal ? goal.chatSessionId : bucketOwner;
    } else {
      const row = this.db.db
        .prepare("SELECT id FROM goals WHERE linked_run_id = ? ORDER BY updated_at DESC LIMIT 1")
        .get(run.id) as { id: string } | null;
      ownerChat = row ? (this.goals.getGoal(row.id)?.chatSessionId ?? null) : null;
    }
    if (ownerChat === null || ownerChat === chatSessionId) return true;
    const operator = action === "cancel" ? `\`pi-harness stop ${run.id}\`` : `\`pi-harness steer ${taskId} "<message>"\``;
    throw new Error(
      `Task ${taskId} belongs to run ${run.id}, owned by Director chat session ${ownerChat}; this chat may not ${action} it. ` +
        `Ask from that chat session, or the operator can use ${operator}.`,
    );
  }

  /**
   * Existing runs whose indexed dossiers this chat session may read: its synthetic task runs, its goals' linked
   * runs, and session-less goals' linked and Done-Review runs in the same repository. With `ownBucketsAnywhere`,
   * also this chat's own buckets in other workspaces (after the current workspace's runs). Creates nothing.
   */
  private ownedDossierRuns(chatSessionId: string, repoRoot: string, ownBucketsAnywhere = false): RunRecord[] {
    const ownBuckets = this.orchestrator.listDirectorTaskRuns(chatSessionId);
    const ids = new Set<string>(ownBuckets.map((r) => r.id));
    for (const goal of this.goals.listGoals({ repoRoot })) {
      if (goal.chatSessionId === chatSessionId) {
        if (goal.linkedRunId) ids.add(goal.linkedRunId);
      } else if (goal.chatSessionId === null) {
        if (goal.linkedRunId) ids.add(goal.linkedRunId);
        for (const run of this.orchestrator.listDirectorTaskRuns(goal.id)) ids.add(run.id);
      }
    }
    const runs: RunRecord[] = [];
    let realRepo: string | null = null;
    try {
      realRepo = realpathSync(repoRoot);
    } catch {
      // The current workspace no longer resolves; only this chat's own buckets can still be reached.
    }
    for (const id of realRepo === null ? [] : ids) {
      const run = this.db.getRun(id);
      if (!run) continue;
      try {
        if (realpathSync(run.repoRoot) === realRepo) runs.push(run);
      } catch {
        // Run root no longer resolves; skip it.
      }
    }
    if (ownBucketsAnywhere) for (const run of ownBuckets) if (!runs.some((r) => r.id === run.id)) runs.push(run);
    return runs;
  }

  private readDirectorArtifact(chatSessionId: string, repoRoot: string, args: Record<string, unknown>, signal?: AbortSignal) {
    if (typeof args.path === "string" && args.path.trim()) {
      const path = args.path;
      const run = this.ownedDossierRuns(chatSessionId, repoRoot, true).find((r) => findRunDossier(this.db, r, path) !== null);
      if (run) return readRunDossier(this.db, run, args);
    } else if (typeof args.task_id === "string" && args.task_id.trim()) {
      const task = this.db.getTask(args.task_id.trim());
      const run = task ? this.ownedDossierRuns(chatSessionId, repoRoot, true).find((r) => r.id === task.runId) : undefined;
      if (run) return readRunDossier(this.db, run, args);
    } else {
      const ownedRuns = this.ownedDossierRuns(chatSessionId, repoRoot);
      if (ownedRuns.length > 0) {
        const byId = new Map<string, ArtifactRecord>();
        for (const run of ownedRuns) {
          for (const artifact of this.db.listArtifacts(run.id, undefined, 50, "dossier")) byId.set(artifact.id, artifact);
        }
        const artifacts = [...byId.values()].sort((a, b) => b.createdAt - a.createdAt).slice(0, 50);
        return { artifacts };
      }
    }
    return this.orchestrator.executeDirectorTaskTool(chatSessionId, repoRoot, "artifact_read", args, signal);
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
      if (!goal) throw new Error("Unknown goal in this workspace");
      if (canonicalProjectPath(goal.repoRoot) !== canonicalProjectPath(repoRoot)) {
        throw new Error(
          `Unknown goal in this workspace: ${goal.id} belongs to ${goal.repoRoot}. Call project_switch to that project to manage it ` +
            "(register the path first if it is no longer listed by project_list).",
        );
      }
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
        return this.orchestrator.executeDirectorTaskTool(chatSessionId, repoRoot, name, args, signal);
      case "artifact_read":
        return this.readDirectorArtifact(chatSessionId, repoRoot, args, signal);
      case "cancel_task": {
        const targetId = String(args.task_id ?? "").trim();
        if (!targetId) throw new Error("task_id is required");
        const targetTask = this.db.getTask(targetId);
        if (targetTask) {
          const targetRun = this.db.getRun(targetTask.runId);
          // Throws for another chat's run; false means another repository, which is never cancellable from this workspace.
          if (!targetRun || !this.directorMayControlRun(chatSessionId, repoRoot, targetRun, targetTask.id, "cancel")) {
            throw new Error(`Task ${targetId} does not belong to this workspace; this chat may not cancel it. The operator can use \`pi-harness stop ${targetTask.runId}\`.`);
          }
          // Any task of this workspace this chat may control, not just synthetic Director tasks.
          await this.orchestrator.cancelTask(targetTask.id, typeof args.reason === "string" ? args.reason : "Cancelled by Director");
          return { cancelled: targetTask.id, runId: targetTask.runId };
        }
        return this.orchestrator.executeDirectorTaskTool(chatSessionId, repoRoot, name, args, signal);
      }
      case "steer_task": {
        const targetId = String(args.task_id ?? "").trim();
        const message = String(args.message ?? "").trim();
        if (!targetId || !message) throw new Error("task_id and message are required");
        const targetTask = this.db.getTask(targetId);
        if (!targetTask) throw new Error(`Unknown task ${targetId}`);
        const targetRun = this.db.getRun(targetTask.runId);
        // Throws for another chat's run; false means another repository, which is never steerable from this workspace.
        if (!targetRun || !this.directorMayControlRun(chatSessionId, repoRoot, targetRun, targetTask.id, "steer")) {
          throw new Error(`Task ${targetId} does not belong to this workspace; this chat may not steer it. The operator can use \`pi-harness steer ${targetId} "<message>"\`.`);
        }
        // Steering never undoes a stop: a refusal carries its reason and the recovery path.
        const result = await this.orchestrator.steerTaskDetailed(targetTask.id, message);
        return { ...result, run_id: targetTask.runId };
      }
      case "goal_accept": {
        const goalId = String(args.goal_id ?? "").trim();
        if (!goalId) throw new Error("goal_accept requires goal_id");
        const outcome = await this.acceptGoalOverride(goalId, {
          reason: String(args.reason ?? ""),
          evidencePaths: Array.isArray(args.evidence_paths) ? (args.evidence_paths as unknown[]).map(String) : [],
          overrideDeterministic: args.override_deterministic === true,
          source: "director",
          chatSessionId,
        });
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
        const checkTarget = (path: string) => {
          const rel = relative(matchedRoot, path);
          if (rel === ".." || rel.startsWith(`..${sep}`) || isAbsolute(rel)) {
            throw new Error("write_note parent symlink escapes allowed write root");
          }
          if (isProtectedWritePath(matchedRoot, path)) throw new Error("write_note target is protected");
        };
        checkTarget(expandedTarget);
        let ancestor = dirname(expandedTarget);
        while (!lstatSync(ancestor, { throwIfNoEntry: false })) ancestor = dirname(ancestor);
        const realAncestor = realpathSync(ancestor);
        // A new note root may not exist yet. Its existing prefix must not redirect creation.
        if (lstatSync(matchedRoot, { throwIfNoEntry: false })) {
          checkTarget(realAncestor);
        } else if (realAncestor !== ancestor) {
          throw new Error("write_note parent symlink escapes allowed write root");
        }
        const target = resolve(realAncestor, relative(ancestor, expandedTarget));
        checkTarget(target);
        const existing = lstatSync(target, { throwIfNoEntry: false });
        if (existing) checkTarget(realpathSync(target));
        else mkdirSync(dirname(target), { recursive: true });
        checkTarget(realpathSync(dirname(target)));
        const fd = openSync(target, constants.O_WRONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK |
          (existing ? 0 : constants.O_CREAT | constants.O_EXCL));
        try {
          const actual = realpathSync(target);
          checkTarget(actual);
          const opened = fstatSync(fd);
          const current = statSync(actual);
          if (!opened.isFile() || opened.nlink !== 1 || opened.dev !== current.dev || opened.ino !== current.ino) {
            throw new Error("write_note requires a regular file with a single link");
          }
          ftruncateSync(fd, 0);
          writeFileSync(fd, content, "utf8");
        } finally {
          closeSync(fd);
        }
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
          const res = await this.commitGoalDetailed(created.id, {
            model: String(args.model), profileId: args.profile_id as string | undefined,
            effort: typeof args.effort === "string" ? args.effort : undefined,
          });
          return this.goalCommitToolResult(res, presentGoal(res.goal));
        }
        return { ...created, evidenceDir: goalEvidenceDirOrNull(created), presentation_id: presentGoal(created) };
      }

      case "goal_commit": {
        const res = await this.commitGoalDetailed(String(args.goal_id), {
          profileId: typeof args.profile_id === "string" ? args.profile_id : undefined,
          model: typeof args.model === "string" ? args.model : undefined,
          effort: typeof args.effort === "string" ? args.effort : undefined,
          position: args.position as QueuePositionInput | undefined,
          forceStart: Boolean(args.force_start),
        });
        return this.goalCommitToolResult(res, presentGoal(res.goal));
      }

      case "queue_list": {
        const lane = args.lane === "goal" || args.lane === "worker" ? (args.lane as AdmissionLane) : undefined;
        // Read-only: queue progress is kept current by admission-queue changes, not by reads.
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
          appendDescription: typeof args.append_description === "string" ? args.append_description : undefined,
          confirmReplaceDescription: args.confirm_replace_description === true ? true : undefined,
          addCriteria: Array.isArray(args.add_criteria) ? (args.add_criteria as GoalCriterion[]) : undefined,
          updateCriteria: parseGoalCriterionPatches(args.update_criteria, "update_criteria"),
          removeCriteriaIds: Array.isArray(args.remove_criteria_ids) ? (args.remove_criteria_ids as string[]) : undefined,
          nonGoals: Array.isArray(args.non_goals) ? (args.non_goals as string[]) : undefined,
        }, { source: "director", actorChatSessionId: chatSessionId });
        return { ...amended, presentation_id: presentGoal(amended.goal) };
      }

      case "goal_list": {
        // Read-only: queue progress is kept current by admission-queue changes, not by reads.
        const allProjects = Boolean(args.all_projects);
        const goals = (allProjects ? this.goals.listGoals({}) : this.goals.listGoals({ repoRoot, chatSessionId })).map(
          reconcileGoalWithLiveRun,
        );
        const presentation_id = offerPresentation(() => buildGoalsTableDisplay(goals.map((goal) => this.goalDisplayData(goal))));
        return { goals, presentation_id };
      }

      case "project_list": {
        const projects = await this.db.projects.listProjects();
        const curSess = this.chats.getSession(chatSessionId);
        return { currentProjectId: curSess?.projectId ?? null, currentPath: curSess?.repoRoot ?? repoRoot, projects };
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

      case "project_remove": {
        const ref = String(args.project ?? "").trim();
        if (!ref) throw new Error("project is required");
        // Read the binding first: removeProject clears project_id on every chat bound to it.
        const currentProjectId = this.chats.getSession(chatSessionId)?.projectId ?? null;
        const { removed, project } = this.db.projects.removeProject(ref);
        const wasCurrentProject = currentProjectId === project.id;
        return {
          removed,
          project,
          wasCurrentProject,
          note:
            `Only the registry entry was removed; files, chat history, goals, runs and tasks under ${project.rootPath} are untouched. ` +
            "The project's name, description and default profile/model/effort are gone. The path is registered again (new ID) the next time a chat is opened or created there." +
            (wasCurrentProject ? " This chat keeps working in the same path." : ""),
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
          const { session: created, projectRegistered } = await this.createChatDetailed({
            repoRoot,
            title: typeof args.title === "string" ? args.title : undefined,
          });
          return {
            host: "local",
            address: `local:${created.id}`,
            session: created,
            ...(projectRegistered ? { projectRegistered: true } : {}),
          };
        }
        return await this.peerBridge.createSession({
          localFallbackRepoRoot: repoRoot,
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
        }, "director");
        if (g.linkedRunId) {
          this.db.emitEvent(g.linkedRunId, null, "goal.progress", { goalId: g.id, ...g.liveProgress });
        }
        return { goal: g };
      }

      case "goal_get": {
        const g = this.goals.getGoal(String(args.goal_id));
        if (!g) throw new Error(`Unknown goal ${args.goal_id}`);
        const detail = this.goalDisplayData(g);
        return {
          ...detail,
          acceptOverrides: detail.reviews.filter((r) => r.source === "director_accept_override"),
          evidenceDir: goalEvidenceDirOrNull(g),
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
        const g = await this.pauseGoal(String(args.goal_id), "Paused by Director");
        return { ...g, presentation_id: presentGoal(g) };
      }

      case "goal_cancel": {
        const g = await this.pauseGoal(String(args.goal_id), "Cancelled by Director", "cancelled");
        return { ...g, presentation_id: presentGoal(g) };
      }

      case "resolve_escalation": {
        if (typeof args.reason !== "string" || !args.reason.trim()) throw new Error("reason must be a non-empty string");
        return this.resolveEscalation({
          escalationId: String(args.escalation_id ?? ""),
          reason: args.reason,
          chatSessionId,
          repoRoot,
          resolvedBy: "director",
        });
      }

      case "read_file": {
        const rawPath = typeof args.path === "string" ? args.path.trim() : "";
        if (!rawPath) throw new Error("read_file requires path");
        const reason = typeof args.exception_reason === "string" ? args.exception_reason.trim() : "";
        const int = (v: unknown) => (typeof v === "number" && Number.isFinite(v) ? Math.floor(v) : undefined);
        const startLineArg = int(args.start_line);
        const endLine = int(args.end_line);
        const offsetArg = int(args.offset);
        const lineMode = startLineArg !== undefined || endLine !== undefined;
        if (lineMode && offsetArg !== undefined) throw new Error("read_file takes either offset or start_line/end_line, not both");
        const startLine = startLineArg ?? 1;
        if (startLine < 1) throw new Error("start_line must be 1 or greater");
        if (endLine !== undefined && endLine < startLine) throw new Error("end_line must not be less than start_line");

        const { target, display: path } = resolveDirectorReadablePath(repoRoot, rawPath);

        const pageBytes = Math.min(READ_FILE_MAX_PAGE_BYTES, Math.max(4, int(args.max_bytes) ?? READ_FILE_DEFAULT_PAGE_BYTES));
        const file = await openFile(target, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
        try {
          const stat = await file.stat();
          if (!stat.isFile()) throw new Error("Expected a regular file");
          const log = (bytes: number, at: number) => {
            const event: ChatControlEvent = { kind: "director.read_file", path, bytes, ...(reason ? { reason } : {}), ...(at > 0 ? { offset: at } : {}) };
            this.chats.appendTurn(chatSessionId, "system_notice", formatChatEventSummary(event), undefined, event);
          };

          // Find the starting byte: a caller offset (negative counts back from end of file) or the start of start_line.
          let reqOffset = offsetArg === undefined ? 0 : offsetArg < 0 ? Math.max(0, stat.size + offsetArg) : offsetArg;
          let totalLines: number | undefined;
          if (lineMode && startLine > 1) {
            const scan = Buffer.allocUnsafe(Math.max(1, Math.min(READ_FILE_SCAN_BYTES, stat.size)));
            let line = 1;
            let pos = 0;
            let last = 0x0a;
            let found = false;
            while (!found) {
              signal?.throwIfAborted();
              const { bytesRead } = await file.read(scan, 0, scan.length, pos);
              if (bytesRead === 0) break;
              const chunk = scan.subarray(0, bytesRead);
              let i = 0;
              while (line < startLine) {
                const nl = chunk.indexOf(0x0a, i);
                if (nl === -1) break;
                line++;
                i = nl + 1;
              }
              if (line === startLine) {
                reqOffset = pos + i;
                found = true;
              }
              last = chunk[bytesRead - 1]!;
              pos += bytesRead;
            }
            if (!found) {
              totalLines = line - 1 + (pos > 0 && last !== 0x0a ? 1 : 0);
              reqOffset = pos;
            }
          }
          const firstLine = lineMode ? startLine : reqOffset === 0 ? 1 : undefined;
          const empty = (size: number) => {
            log(0, reqOffset);
            return {
              path, offset: reqOffset, nextOffset: null, totalBytes: size, truncated: false, hasMore: false, content: "",
              ...(lineMode ? { totalLines: totalLines ?? startLine - 1 } : {}),
            };
          };
          if (reqOffset >= stat.size) return empty(stat.size);

          const want = Math.min(pageBytes, stat.size - reqOffset);
          const buf = Buffer.alloc(want);
          const { bytesRead: count } = await file.read(buf, 0, want, reqOffset);
          // A short read means the file ended (or was truncated) after stat; treat where it stopped as end of file.
          const effectiveSize = count < want ? reqOffset + count : stat.size;
          if (count === 0) return empty(effectiveSize);
          // Skip continuation bytes when the caller's byte offset lands inside a multi-byte character.
          let skip = 0;
          if (!lineMode && reqOffset > 0) {
            const maxSkip = Math.min(3, count);
            while (skip < maxSkip && (buf[skip]! & 0xc0) === 0x80) skip++;
          }
          const alignedOffset = reqOffset + skip;

          // End the page at end_line if reached; otherwise, when the file continues, after the last newline,
          // or else before a multi-byte character split by the page end. At end of file decode everything so truncation fails.
          let end = count;
          let rangeDone = false;
          if (endLine !== undefined) {
            let remaining = endLine - startLine + 1;
            let i = skip;
            while (remaining > 0) {
              const nl = buf.indexOf(0x0a, i);
              if (nl === -1 || nl >= count) break;
              remaining--;
              i = nl + 1;
            }
            if (remaining === 0) {
              end = i;
              rangeDone = true;
            }
          }
          if (!rangeDone && reqOffset + count < effectiveSize && count > skip) {
            const nl = buf.lastIndexOf(0x0a, count - 1);
            if (nl >= skip) {
              end = nl + 1;
            } else {
              for (let i = count - 1; i >= Math.max(skip, count - 3); i--) {
                const b = buf[i]!;
                if ((b & 0xc0) === 0x80) continue;
                const seqLen = (b & 0xe0) === 0xc0 ? 2 : (b & 0xf0) === 0xe0 ? 3 : (b & 0xf8) === 0xf0 ? 4 : 1;
                if (count - i < seqLen) end = i;
                break;
              }
            }
          }
          const slice = buf.subarray(skip, end);
          let decoded: string;
          try {
            decoded = new TextDecoder("utf-8", { fatal: true, ignoreBOM: alignedOffset > 0 }).decode(slice);
          } catch {
            throw new Error("read_file supports UTF-8 text files only");
          }
          const hasBom = alignedOffset === 0 && slice.length >= 3 && slice[0] === 0xef && slice[1] === 0xbb && slice[2] === 0xbf;
          let content = decoded;
          let trimmed = false;
          if (typeof args.max_chars === "number" && Number.isFinite(args.max_chars)) {
            let charLimit = Math.max(1, Math.floor(args.max_chars));
            if (charLimit < decoded.length) {
              const hi = decoded.charCodeAt(charLimit - 1);
              const lo = decoded.charCodeAt(charLimit);
              if (hi >= 0xd800 && hi <= 0xdbff && lo >= 0xdc00 && lo <= 0xdfff) charLimit = charLimit > 1 ? charLimit - 1 : 2;
              content = decoded.slice(0, charLimit);
              trimmed = content.length < decoded.length;
            }
          }
          if (trimmed) rangeDone = false;
          const contentBytes = trimmed ? (hasBom ? 3 : 0) + Buffer.byteLength(content, "utf8") : end - skip;
          const next = alignedOffset + contentBytes;
          const hasMore = next < effectiveSize && next > reqOffset;
          let lines: { startLine: number; endLine: number } | undefined;
          if (firstLine !== undefined && content.length > 0) {
            let newlines = 0;
            for (let i = content.indexOf("\n"); i !== -1; i = content.indexOf("\n", i + 1)) newlines++;
            lines = { startLine: firstLine, endLine: firstLine + newlines - (content.endsWith("\n") ? 1 : 0) };
          }
          log(contentBytes, alignedOffset);
          return {
            path, offset: alignedOffset, nextOffset: hasMore ? next : null, totalBytes: effectiveSize,
            truncated: hasMore && !rangeDone, hasMore, content, ...lines,
          };
        } finally {
          await file.close();
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
        const orch = this.db.listTasks(g.linkedRunId).find((t) => t.tier === 1 && t.profileId !== "director");
        if (!orch) throw new Error("Orchestrator task not found");
        const result = await this.orchestrator.steerTaskDetailed(orch.id, String(args.message));
        return { ...result, goal_id: g.id, run_id: g.linkedRunId };
      }

      case "resume_task": {
        const taskBefore = this.db.getTask(String(args.task_id));
        if (taskBefore) this.assertRunGoalNotTerminal(taskBefore.runId);
        const priorRunStatus = taskBefore ? this.orchestrator.getEffectiveRun(taskBefore.runId)?.status : undefined;
        const res = await this.orchestrator.resumeTaskDetailed(String(args.task_id), {
          note: typeof args.note === "string" ? args.note : undefined,
          model: typeof args.model === "string" ? args.model : undefined,
          effort: typeof args.effort === "string" ? args.effort : undefined,
          profileId: typeof args.profile_id === "string" && args.profile_id.trim() ? args.profile_id.trim() : undefined,
        });
        this.onRunResumed(res.task.runId, priorRunStatus, res.outcome === "resumed");
        return {
          resumed: res.outcome === "resumed",
          outcome: res.outcome,
          message: res.message,
          task: res.task,
          ...(res.queuePosition !== undefined ? { queuePosition: res.queuePosition } : {}),
          ...(res.noteDelivered !== undefined ? { noteDelivered: res.noteDelivered } : {}),
          ...(res.noteDelivery ? { noteDelivery: res.noteDelivery } : {}),
        };
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
        this.assertRunGoalNotTerminal(runId);
        const priorRunStatus = this.orchestrator.getEffectiveRun(runId)?.status;
        const res = await this.orchestrator.resumeRun(runId, {
          trigger: "manual",
          note: typeof args.note === "string" ? args.note : undefined,
        });
        this.onRunResumed(res.run.id, priorRunStatus, res.outcome === "resumed");
        return {
          ...res,
          presentation_id: offerPresentation(() => buildRunActivityDisplay(buildRunActivitySummary(this.db, res.run.id))),
        };
      }

      case "schedule_timer": {
        let goalId: string | null = null;
        let runId: string | null = null;
        let pausedGoal = false;
        // Validate before the pause below so a rejected call never pauses a Goal.
        const rawWake = args.wake_on_event ?? "none";
        if (rawWake !== "none" && rawWake !== "user_message" && rawWake !== "goal_settled" && rawWake !== "any") {
          throw new Error(
            `Unknown wake_on_event "${String(args.wake_on_event)}". Allowed values: none, user_message, goal_settled, any.`,
          );
        }
        const wakeOnEvent = rawWake;
        const goalRef = args.goal_id == null ? "" : String(args.goal_id).trim();
        if (wakeOnEvent === "goal_settled" && !goalRef) {
          throw new Error(
            'wake_on_event "goal_settled" requires goal_id (the Goal whose settlement should cancel this timer). To react to a user message or to any Goal of this chat settling, use wake_on_event "any" without goal_id.',
          );
        }
        if (goalRef) {
          const g = this.goals.getGoal(goalRef);
          if (!g) throw new Error(`Unknown goal ${args.goal_id}`);
          goalId = g.id;
          runId = g.linkedRunId;
          if (args.pause_goal === true && g.status !== "paused" && g.status !== "achieved" && g.status !== "cancelled") {
            // An explicit pause wins over a launch already in flight (the timer scheduled below resumes it).
            this.goalPauseEpoch.set(g.id, (this.goalPauseEpoch.get(g.id) ?? 0) + 1);
            this.goals.setStatus(g.id, "paused", "Paused for scheduled timer");
            if (g.linkedRunId) await this.orchestrator.stopRun(g.linkedRunId, "Goal paused for timer");
            pausedGoal = true;
          }
        }
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

interface CoordinatorFailure {
  source: GoalProgressSource;
  summary?: string;
  criteriaResults: CriterionEvaluation[];
  claimedAt?: number;
}

interface ExplicitBlocker {
  summary: string;
  remediation: string[];
  /** An open blocking escalation already raised by a blocked task; reused instead of creating another. */
  escalationId: string | null;
  tier1Id: string | null;
}

function hasClaimProvenance(lp: GoalLiveProgress): boolean {
  return lp.goalVersion !== undefined || lp.runId !== undefined || lp.reviewCycle !== undefined;
}

/**
 * The goal's canonical evidence directory (created when possible), or null when it is not a safe segment or a
 * path component is not a real directory (e.g. a symlink). A rejected path is never advertised.
 */
function goalEvidenceDirOrNull(goal: GoalRecord): string | null {
  return ensureGoalEvidenceDir(goal.repoRoot, goal.id);
}

/** The Tier 1 assignment for a goal revision: its title line and full objective (criteria, non-goals, evidence dir). */
function goalRunObjective(g: GoalRecord): { title: string; objective: string } {
  const title = `Goal ${g.id} v${g.version}: ${g.title}`;
  const criteriaSummary = g.criteria
    .map((c) => `- [${c.id}] (${c.kind}${c.required ? "*" : ""}) ${c.description}${c.command ? ` [cmd: ${c.command}]` : ""}`)
    .join("\n");
  const objective = [
    title,
    g.description,
    "",
    "Acceptance Criteria:",
    criteriaSummary,
    ...(g.nonGoals.length ? ["", "Non-Goals:", ...g.nonGoals.map((n) => `- ${n}`)] : []),
    ...evidenceDirLines(g),
  ].join("\n");
  return { title, objective };
}

function evidenceDirLines(goal: GoalRecord): string[] {
  const dir = goalEvidenceDirOrNull(goal);
  return dir
    ? ["", `Goal Evidence Directory: ${dir} (env HARNESS_GOAL_EVIDENCE_DIR). Write verification evidence for this goal there.`]
    : [];
}

function blockerDetail(blocker: ExplicitBlocker): string {
  const remediation = blocker.remediation.length ? `; remediation: ${blocker.remediation.join("; ")}` : "";
  return `blocked: ${blocker.summary}${remediation}`.slice(0, 300);
}

/**
 * Resolves a Director read path (repo-relative, absolute, or ~/) to its canonical target. Refuses `.git`,
 * task worktrees, and harness-internal state (repo `.harness/*` other than artifacts, goals and notes).
 */
export function resolveDirectorReadablePath(repoRoot: string, rawPath: string): { target: string; inRepo: boolean; display: string } {
  const root = realpathSync(repoRoot);
  const candidate = rawPath === "~" ? homedir()
    : rawPath.startsWith("~/") ? resolve(homedir(), rawPath.slice(2))
    : resolve(root, rawPath);
  const target = realpathSync(candidate);
  const relToRepo = relative(root, target);
  const inRepo = relToRepo !== ".." && !relToRepo.startsWith(`..${sep}`) && !isAbsolute(relToRepo);
  const protectedMessage = "read_file cannot read protected paths (.git, task worktrees, or harness-internal state)";
  if (inRepo) {
    const parts = relToRepo.split(sep).map((p) => p.toLowerCase());
    // Block .git and .harness/worktrees, while permitting .harness/goals, .harness/artifacts, and .harness/notes.
    if (parts.includes(".git") || (parts[0] === ".harness" && !["artifacts", "goals", "notes"].includes(parts[1] ?? ""))) {
      throw new Error(protectedMessage);
    }
  } else {
    // Check the whole canonical path so the veto holds wherever the file lives.
    const parts = target.split(sep).map((p) => p.toLowerCase());
    if (parts.includes(".git") || parts.some((p, i) => p === ".harness" && parts[i + 1] === "worktrees")) {
      throw new Error(protectedMessage);
    }
  }
  return { target, inRepo, display: inRepo ? relToRepo : target };
}

/** goal_accept evidence: a readable regular file or directory under the same policy as read_file. */
export function resolveDirectorEvidencePath(repoRoot: string, rawPath: string): GoalAcceptOverrideAudit["evidence"][number] {
  let target: string;
  try {
    ({ target } = resolveDirectorReadablePath(repoRoot, rawPath));
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === "ENOENT" || code === "ENOTDIR") throw new Error(`goal_accept evidence path does not exist: ${rawPath}`);
    throw new Error(`goal_accept evidence path refused: ${rawPath} (${err instanceof Error ? err.message : String(err)})`);
  }
  try {
    accessSync(target, constants.R_OK);
  } catch {
    throw new Error(`goal_accept evidence path is not readable: ${rawPath}`);
  }
  const st = statSync(target);
  if (!st.isFile() && !st.isDirectory()) throw new Error(`goal_accept evidence path must be a regular file or directory: ${rawPath}`);
  return {
    input: rawPath,
    canonicalPath: target,
    kind: st.isFile() ? "file" : "directory",
    ...(st.isFile() ? { sizeBytes: st.size } : {}),
    mtimeMs: st.mtimeMs,
  };
}

const REVIEWER_VERDICTS = ["approved", "changes_requested", "escalate_human"];
/** Reviewer sessions per review: a completed run without a usable verdict is retried in a fresh session. */
const REVIEWER_VERDICT_ATTEMPTS = 2;

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

/** Normalizes one reviewer criterion result (snake_case aliases, strict `passed === true`); null without an id. */
export function normalizeCriterionEvaluation(raw: unknown): CriterionEvaluation | null {
  if (!isPlainObject(raw)) return null;
  const id = raw.criterionId ?? raw.criterion_id;
  if (typeof id !== "string" || !id.trim()) return null;
  return {
    criterionId: id.trim(),
    passed: raw.passed === true,
    evidence: typeof raw.evidence === "string" ? raw.evidence : "",
    ...(typeof raw.remediation === "string" ? { remediation: raw.remediation } : {}),
  };
}

/**
 * Normalizes a reviewer verdict: unwraps one `{ review }` / `{ result }` level, accepts snake_case aliases,
 * requires a known verdict, and coerces criteria and blocking issues. Never adds a key the input lacked.
 */
export function normalizeReviewerVerdict<T extends { verdict?: unknown }>(raw: unknown): T | null {
  if (!isPlainObject(raw)) return null;
  let obj = raw;
  if (typeof obj.verdict !== "string") {
    const inner = isPlainObject(obj.review) ? obj.review : isPlainObject(obj.result) ? obj.result : null;
    if (!inner) return null;
    obj = inner;
  }
  const verdict = typeof obj.verdict === "string" ? obj.verdict.trim() : "";
  if (!REVIEWER_VERDICTS.includes(verdict)) return null;
  const { criteria_results, blocking_issues, ...rest } = obj;
  const out: Record<string, unknown> = { ...rest, verdict };
  const criteria = rest.criteriaResults ?? criteria_results;
  if (criteria !== undefined) {
    out.criteriaResults = Array.isArray(criteria)
      ? criteria.map(normalizeCriterionEvaluation).filter((c): c is CriterionEvaluation => c !== null)
      : [];
  }
  const issues = rest.blockingIssues ?? blocking_issues;
  if (issues !== undefined) {
    out.blockingIssues =
      typeof issues === "string" ? [issues] : Array.isArray(issues) ? issues.filter((i): i is string => typeof i === "string") : [];
  }
  if (rest.summary !== undefined && typeof rest.summary !== "string") out.summary = String(rest.summary);
  return out as T;
}

const VERDICT_SCAN_BYTES = 256 * 1024;
const VERDICT_SCAN_MAX_STARTS = 512;

/** End index of the balanced `{...}` object starting at `start` (string-aware), or -1. */
function balancedObjectEnd(text: string, start: number): number {
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < text.length; i++) {
    const ch = text[i]!;
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === "\\") escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') inString = true;
    else if (ch === "{") depth++;
    else if (ch === "}") {
      depth--;
      if (depth === 0) return i;
    }
  }
  return -1;
}

/**
 * Extracts a structured verdict from reviewer prose: the last fenced block holding a valid verdict, else a
 * right-to-left scan of `{` starts in the output tail. Each start begins its own string state, so a stray
 * quote or brace in earlier prose cannot hide the verdict object.
 */
export function parseReviewerVerdictJson<T extends { verdict?: unknown }>(text: string): T | null {
  const fences = [...text.matchAll(/```(?:json)?[ \t]*\r?\n?([\s\S]*?)```/gi)];
  for (let i = fences.length - 1; i >= 0; i--) {
    try {
      const parsed = normalizeReviewerVerdict<T>(JSON.parse(fences[i]![1]!.trim()));
      if (parsed) return parsed;
    } catch {
      // keep scanning earlier fences
    }
  }

  const tail = text.length > VERDICT_SCAN_BYTES ? text.slice(-VERDICT_SCAN_BYTES) : text;
  const lastVerdict = tail.lastIndexOf('"verdict"');
  if (lastVerdict === -1) return null;
  let visited = 0;
  for (let start = tail.lastIndexOf("{", lastVerdict); start >= 0 && visited < VERDICT_SCAN_MAX_STARTS; start = start > 0 ? tail.lastIndexOf("{", start - 1) : -1) {
    visited++;
    const end = balancedObjectEnd(tail, start);
    if (end < 0) continue;
    try {
      const parsed = normalizeReviewerVerdict<T>(JSON.parse(tail.slice(start, end + 1)));
      if (parsed) return parsed;
    } catch {
      // not JSON (e.g. a code snippet); keep scanning left
    }
  }
  return null;
}
