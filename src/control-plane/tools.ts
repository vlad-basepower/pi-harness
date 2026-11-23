import { rmSync } from "node:fs";
import { join, resolve } from "node:path";
import { recommendModelsByTaskKind, type ModelCatalogCompiler } from "../catalog/compiler.js";
import { CODE_EXEC_TOOL_NAME, TOOL_SEARCH_TOOL_NAME, createCodeModeExecutor, effectiveCodeMode, exposedTools } from "../code-mode/index.js";
import type { HarnessConfig } from "../config.js";
import { codeExecEventSink } from "../protocol/tool-display.js";
import type { ToolDefinition } from "../runners/types.js";
import type { KbKind } from "../kb/store.js";
import type { BisectingMergeQueue } from "../worktree/merge-queue.js";
import { runGit, type WorktreeManager } from "../worktree/manager.js";
import type { AdmissionLane, QueuePositionInput } from "./admission-queue.js";
import { directorTaskRunOwner, isDirectorTaskContainer } from "./ids.js";
import type {
  AgentTier,
  ControlPlaneDb,
  EscalationCategory,
  EscalationSeverity,
  TaskKind,
  TaskRecord,
} from "./db.js";
import { findRunDossier, readRunDossier, readWorkspaceArtifact, spillSummaryOverflow, writeCodeOutput, writeTaskDossier } from "./dossiers.js";
import { compactTaskHandoff, isReadOnlyKind, SUMMARY_LIMIT, validateTaskHandoff } from "./handoffs.js";
import type { AnchoredHandoff } from "./prompts.js";
import { mergeGoalLiveProgress, type GoalLiveProgress, type GoalProgressSource } from "../goals/live-progress.js";
import type { SteerTaskResult } from "./orchestrator.js";

export { readRunDossier } from "./dossiers.js";
export { compactTaskHandoff, isReadOnlyKind, validateTaskHandoff } from "./handoffs.js";

export interface WaitForChildrenResult {
  children: TaskRecord[];
  wakeReason: "already_settled" | "event" | "escalation" | "steered" | "fallback_interval" | "child_inactive";
  inactiveTaskIds: string[];
}

export interface ToolRuntimeHooks {
  spawnAndStartTask: (input: {
    runId: string;
    parentId: string;
    tier: AgentTier;
    kind: TaskKind;
    title: string;
    objective: string;
    criteria?: string;
    readOnly: boolean;
    profileId?: string;
    model?: string;
    effort?: string;
    maxTurns?: number;
    dependsOn?: string[];
    writeScopes?: string[];
    position?: QueuePositionInput;
    forceStart?: boolean;
    signal?: AbortSignal;
  }) => Promise<TaskRecord>;
  waitForChildren: (
    parentId: string,
    ids: string[] | undefined,
    mode: "any" | "all",
    timeoutMs: number,
    signal?: AbortSignal,
  ) => Promise<WaitForChildrenResult>;
  steerTask: (taskId: string, message: string) => Promise<boolean>;
  /** Structured steering outcome (preferred over steerTask when available). */
  steerTaskDetailed?: (taskId: string, message: string) => Promise<SteerTaskResult>;
  /** Takes the steering queued for a task; wait_children delivers it in its result. */
  drainPendingSteers?: (taskId: string) => string[];
  cancelTask: (taskId: string, reason: string) => Promise<void>;
  stopTaskForBlockingIssue?: (taskId: string, reason: string) => void;
  scheduleRollover: (taskId: string, handoff: AnchoredHandoff, target?: { profileId?: string; model?: string; effort?: string }) => Promise<void>;
  getWorktreeManager: (repoRoot: string) => WorktreeManager;
  getMergeQueue: (repoRoot: string) => BisectingMergeQueue;
  getTaskSignal: (taskId: string) => AbortSignal | undefined;
  /** Recorded cause of a run's current state (stop reason, quota pause, P0), or null when none was recorded. */
  describeRunCause?: (runId: string) => string | null;
  /** True task cancellation signal (does NOT fire on clean rollover, so in-flight merges aren't aborted). */
  getTaskCancelSignal?: (taskId: string) => AbortSignal | undefined;
  resumeTask?: (
    taskId: string,
    opts?: {
      note?: string;
      profileId?: string;
      model?: string;
      effort?: string;
      allowCompleted?: boolean;
      position?: QueuePositionInput;
      forceStart?: boolean;
    },
  ) => Promise<TaskRecord>;
  /** Idempotent resume with a structured outcome (preferred over resumeTask when available). */
  resumeTaskDetailed?: (
    taskId: string,
    opts?: { note?: string; profileId?: string; model?: string; effort?: string },
  ) => Promise<{ task: TaskRecord; outcome: string; message: string; queuePosition?: number; noteDelivered?: boolean }>;
  forceStartWorker?: (taskId: string, opts?: { bypassQuota?: boolean; reason?: string }) => Promise<unknown>;
}

function requiredModel(args: Record<string, unknown>) {
  if (typeof args.model !== "string" || !args.model.trim()) throw new Error("Choose a nonblank model from list_models.");
  return args.model.trim();
}

function optionalProfile(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || !value.trim()) throw new Error("Select an enabled runner profile.");
  return value;
}

const MAX_TURNS_DESCRIPTION =
  "Optional advisory progress-reminder threshold in tool-call turns (default: config rollover.taskMaxTurns). After this many turns the task may receive one progress reminder and keeps working; it never stops or fails the task. The task decides when its work is ready and hands it back with return_task.";

const waitChildrenTool: ToolDefinition = {
  name: "wait_children",
  description:
    "Event-driven wait for child tasks (zero polling; wakes immediately on completion or escalation). Optional fallback_interval_s configures a dead-agent watchdog wakeup (defaults to a long interval so you never thrash).",
  inputSchema: {
    type: "object",
    properties: {
      ids: { type: "array", items: { type: "string" } },
      mode: { type: "string", enum: ["any", "all"] },
      fallback_interval_s: {
        type: "number",
        description: "Optional fallback wakeup interval in seconds in case a child hangs or dies (default: config.events.fallbackWakeupIntervalSec, 600s).",
      },
      timeout_s: {
        type: "number",
        description: "Alias for fallback_interval_s.",
      },
    },
  },
};

const cancelChildTool: ToolDefinition = {
  name: "cancel_child",
  description: "Cancel a running child task cleanly and release its write leases.",
  inputSchema: {
    type: "object",
    required: ["task_id", "reason"],
    properties: {
      task_id: { type: "string" },
      reason: { type: "string" },
    },
  },
};

const ALL_TOOL_DEFS: Array<ToolDefinition & { tiers: AgentTier[] }> = [
  {
    name: "tree_view",
    tiers: [1, 2, 3],
    description: "Inspect the live hierarchical tree of tasks, statuses, models, context usage, and open escalations from SQLite.",
    inputSchema: {
      type: "object",
      properties: {
        include_settled: { type: "boolean", description: "Include completed/cancelled tasks (default true)" },
      },
    },
  },
  {
    name: "task_get",
    tiers: [1, 2, 3],
    description: "Get the compact handoff, indexed dossiers, and escalations for a task in this run.",
    inputSchema: {
      type: "object",
      required: ["task_id"],
      properties: { task_id: { type: "string" } },
    },
  },
  {
    name: "list_models",
    tiers: [1, 2],
    description:
      "List currently enabled latest-per-family models ranked by coding quality with cost/task, time/task, live subscription quota availability, recommendedByKind, and any per-session model selection context.",
    inputSchema: {
      type: "object",
      properties: {
        role: { type: "string", description: "Optional role filter (orchestrator, worker_execution, codebase_research, independent_verifier)" },
        max_api_equiv_usd: { type: "number", description: "Optional upper bound on API-equivalent USD per task" },
        exclude_exhausted: { type: "boolean", description: "Hide models whose quota bucket is currently exhausted (default false)" },
      },
    },
  },
  {
    name: "spawn_worker",
    tiers: [1],
    description:
      "Spawn a Tier 2 Worker in its own isolated git worktree with disjoint write_scopes. Workers dynamically manage sub-tasks and submit via the merge queue.",
    inputSchema: {
      type: "object",
      required: ["title", "objective", "write_scopes", "model"],
      properties: {
        title: { type: "string" },
        objective: { type: "string" },
        criteria: { type: "string" },
        write_scopes: { type: "array", items: { type: "string" }, description: "Disjoint glob patterns owned by this worker (e.g. ['src/auth/**'])" },
        profile_id: { type: "string" },
        model: { type: "string" },
        effort: { type: "string" },
        depends_on: { type: "array", items: { type: "string" } },
        position: {
          description: "Optional queue position if maxConcurrentWorkers is full: 1-based integer (1 = next up), 'front', or 'back' (default).",
        },
        force_start: {
          type: "boolean",
          description: "Emergency override: start immediately bypassing maxConcurrentWorkers. Do NOT abuse — use only for urgent blockers.",
        },
      },
    },
  },
  {
    name: "spawn_task",
    tiers: [1],
    description: "Spawn a read-only Tier 3 research, plan, or verify task. Read its handoff with wait_children.",
    inputSchema: {
      type: "object",
      required: ["kind", "title", "objective", "model"],
      properties: {
        kind: { type: "string", enum: ["research", "plan", "verify"] },
        profile_id: { type: "string" },
        title: { type: "string" },
        objective: { type: "string" },
        model: { type: "string" },
        effort: { type: "string" },
        max_turns: { type: "number", description: MAX_TURNS_DESCRIPTION },
      },
    },
  },
  {
    name: "spawn_task",
    tiers: [2],
    description:
      "Spawn a small, zero-compaction Tier 3 Task Agent (research, plan, execute, or verify) inside this Worker's worktree.",
    inputSchema: {
      type: "object",
      required: ["kind", "title", "objective", "model"],
      properties: {
        kind: { type: "string", enum: ["research", "plan", "execute", "verify"] },
        title: { type: "string" },
        objective: { type: "string" },
        criteria: { type: "string" },
        read_only: { type: "boolean", description: "Opt an execute task into read-only access. Research, plan, and verify are always read-only." },
        profile_id: { type: "string" },
        model: { type: "string" },
        effort: { type: "string" },
        max_turns: { type: "number", description: MAX_TURNS_DESCRIPTION },
        depends_on: { type: "array", items: { type: "string" } },
      },
    },
  },
  { ...waitChildrenTool, tiers: [1, 2] },
  { ...waitChildrenTool, name: "wait_tasks", tiers: [1, 2] },
  {
    name: "steer_child",
    tiers: [1],
    description: "Send mid-turn steering guidance to a running child agent. Steering never restarts a settled child; use resume_task with a note for that.",
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
    // Tier 2 has no resume_task: a settled child is replaced by delegating a fresh task.
    name: "steer_child",
    tiers: [2],
    description: "Send mid-turn steering guidance to a running child agent. Steering never restarts a settled child; spawn a new task for further work.",
    inputSchema: {
      type: "object",
      required: ["task_id", "message"],
      properties: {
        task_id: { type: "string" },
        message: { type: "string" },
      },
    },
  },
  { ...cancelChildTool, tiers: [1, 2] },
  { ...cancelChildTool, name: "cancel_task", tiers: [1] },
  {
    name: "report_goal_progress",
    tiers: [1, 2],
    description:
      "Report live goal progress, verification status, and checked criteria results back to the owning Goal so the Goals table and cards reflect what agents are actually doing.",
    inputSchema: {
      type: "object",
      required: ["summary"],
      properties: {
        summary: { type: "string", description: "Concise live progress or verification update." },
        phase: {
          type: "string",
          description:
            "Short current phase label (e.g. 'planning', 'implementing', 'verifying'). Phase 'blocked' means you cannot proceed without a human: automatic verification is skipped when the run settles and the Director is woken with your summary and remediation. Use it only for genuine blockers.",
        },
        verification_status: {
          type: "string",
          enum: ["not_needed", "in_progress", "passed", "failed"],
          description:
            "Current verification state as decided/observed by the agent. 'failed' prevents automatic approval of this goal revision until you report again (passed, failed, or phase 'blocked').",
        },
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
    name: "return_task",
    tiers: [2, 3],
    description:
      "Finish this task OR send it back up the chain if scope is too large (`scope_too_large` requires evidence_paths and >= 2 suggested_splits).",
    inputSchema: {
      type: "object",
      required: ["status", "summary"],
      properties: {
        status: { type: "string", enum: ["completed", "scope_too_large", "blocked"] },
        summary: { type: "string", description: "Compact handoff (up to 2,000 characters; longer summaries are saved as artifacts)." },
        sources: {
          type: "array",
          description: "Evidence references. Required and non-empty for research, plan, and verify tasks.",
          items: {
            type: "object",
            required: ["path"],
            properties: { path: { type: "string" }, lines: { type: "string" }, symbol: { type: "string" }, note: { type: "string" } },
          },
        },
        artifact_paths: { type: "array", items: { type: "string" } },
        evidence_paths: { type: "array", items: { type: "string" } },
        suggested_splits: { type: "array", items: { type: "string" } },
      },
    },
  },
  {
    name: "report_issue",
    tiers: [1, 2, 3],
    description: "Raise a discovered code defect, broken test, interface conflict, or ambiguity up the chain.",
    inputSchema: {
      type: "object",
      required: ["severity", "category", "title", "evidence_paths"],
      properties: {
        severity: { type: "string", enum: ["P0", "P1", "P2"] },
        category: {
          type: "string",
          enum: [
            "scope_too_large",
            "code_defect",
            "broken_test",
            "interface_conflict",
            "ambiguous_spec",
            "blocked_dependency",
            "verification_failed",
            "budget_exhausted",
            "env_failure",
            "security_risk",
          ],
        },
        title: { type: "string" },
        evidence_paths: { type: "array", items: { type: "string" } },
        attempted: { type: "array", items: { type: "string" } },
        blocking: { type: "boolean" },
      },
    },
  },
  {
    name: "ack_escalation",
    tiers: [1, 2],
    description: "Acknowledge and resolve an open escalation or scope send-back from a child agent.",
    inputSchema: {
      type: "object",
      required: ["escalation_id", "resolution"],
      properties: {
        escalation_id: { type: "string" },
        resolution: { type: "string" },
      },
    },
  },
  {
    name: "request_rollover",
    tiers: [1, 2],
    description:
      "Request a clean control-plane rollover to a fresh session (optionally switching model/effort) with an anchored handoff before context overflow.",
    inputSchema: {
      type: "object",
      required: ["goal_invariants", "next_actions"],
      properties: {
        goal_invariants: { type: "array", items: { type: "string" } },
        architectural_decisions: {
          type: "array",
          items: {
            type: "object",
            properties: { decision: { type: "string" }, rationale: { type: "string" } },
          },
        },
        active_risks: { type: "array", items: { type: "string" } },
        next_actions: { type: "array", items: { type: "string" } },
        target_profile_id: { type: "string" },
        target_model: { type: "string" },
        target_effort: { type: "string" },
      },
    },
  },
  {
    name: "query_predecessor",
    tiers: [1, 2],
    description: "Search persisted SQLite event & artifact traces from predecessor generations (Seance) without loading old context.",
    inputSchema: {
      type: "object",
      required: ["query"],
      properties: {
        query: { type: "string" },
        limit: { type: "number" },
      },
    },
  },
  {
    name: "artifact_write",
    tiers: [1, 2, 3],
    description: "Write a durable research dossier, plan, or log artifact to disk and register it in the Deterministic Artifact Index.",
    inputSchema: {
      type: "object",
      required: ["name", "content", "summary"],
      properties: {
        name: { type: "string" },
        content: { type: "string" },
        summary: { type: "string" },
      },
    },
  },
  {
    name: "artifact_read",
    tiers: [1, 2, 3],
    description: "Read a dossier by path (Tier 1: indexed dossiers in this run only) or list indexed artifacts for a task.",
    inputSchema: {
      type: "object",
      properties: {
        path: { type: "string" },
        task_id: { type: "string" },
        max_chars: { type: "number" },
        offset: { type: "number", description: "Character offset for paginating large artifacts (default 0)." },
      },
    },
  },
  {
    name: "blackboard_set",
    tiers: [1, 2],
    description: "Publish a shared interface contract, schema, or architectural decision to the run Blackboard.",
    inputSchema: {
      type: "object",
      required: ["key", "value"],
      properties: {
        key: { type: "string" },
        value: {},
      },
    },
  },
  {
    name: "blackboard_get",
    tiers: [1, 2, 3],
    description: "Read one or all shared contracts/decisions from the run Blackboard.",
    inputSchema: {
      type: "object",
      properties: { key: { type: "string" } },
    },
  },
  {
    name: "ask_agent",
    tiers: [1, 2],
    description: "Send a bounded, control-plane-mediated question to another Tier 1 or Tier 2 agent in this run (no freeform chat loops). Tier 3 agents cannot reply; steer your own Tier 3 children instead.",
    inputSchema: {
      type: "object",
      required: ["target_id", "question"],
      properties: {
        target_id: { type: "string" },
        question: { type: "string" },
      },
    },
  },
  {
    name: "reply_agent",
    tiers: [1, 2],
    description: "Reply to a mediated peer question received via `ask_agent`.",
    inputSchema: {
      type: "object",
      required: ["message_id", "answer"],
      properties: {
        message_id: { type: "string" },
        answer: { type: "string" },
      },
    },
  },
  {
    name: "worktree_status",
    tiers: [2],
    description: "Inspect git branch, dirty state, and merge-queue status for this Worker's isolated worktree.",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "submit_for_merge",
    tiers: [2],
    description: "Enqueue this Worker's committed worktree branch into the deterministic bisecting merge queue.",
    inputSchema: {
      type: "object",
      required: ["summary"],
      properties: { summary: { type: "string" } },
    },
  },
  // ---- Curated Knowledge Base (2-stage progressive disclosure) ----
  {
    name: "kb_search",
    tiers: [1, 2, 3],
    description: "Stage 1 KB discovery: search by FTS5 query and/or structured filters (path_globs, tags, kinds). Returns compact cards only.",
    inputSchema: {
      type: "object",
      properties: {
        query: { type: "string" },
        path_globs: { type: "array", items: { type: "string" } },
        tags: { type: "array", items: { type: "string" } },
        kinds: { type: "array", items: { type: "string", enum: ["invariant", "gotcha", "architecture", "decision", "convention"] } },
        include_candidates: { type: "boolean" },
        include_stale: { type: "boolean" },
        limit: { type: "number" },
      },
    },
  },
  {
    name: "kb_get",
    tiers: [1, 2, 3],
    description: "Stage 2 KB read: fetch full context_md and evidence_paths for specific KB entry IDs returned by kb_search.",
    inputSchema: {
      type: "object",
      required: ["ids"],
      properties: { ids: { type: "array", items: { type: "string" } } },
    },
  },
  {
    name: "kb_propose",
    tiers: [1, 2],
    description:
      "Propose a high-leverage non-obvious finding as an unverified KB candidate with mandatory context_md and evidence_paths. Candidates stay hidden from default kb_search results until a Tier 1 Orchestrator verifies them with kb_promote.",
    inputSchema: {
      type: "object",
      required: ["kind", "title", "summary", "context_md", "evidence_paths"],
      properties: {
        kind: { type: "string", enum: ["invariant", "gotcha", "architecture", "decision", "convention"] },
        title: { type: "string" },
        summary: { type: "string" },
        context_md: { type: "string" },
        tags: { type: "array", items: { type: "string" } },
        path_globs: { type: "array", items: { type: "string" } },
        evidence_paths: { type: "array", items: { type: "string" } },
      },
    },
  },
  {
    name: "kb_flag_stale",
    tiers: [1, 2, 3],
    description: "Immediately flag an inaccurate or outdated KB entry as stale so other agents are not misled.",
    inputSchema: {
      type: "object",
      required: ["id", "reason"],
      properties: { id: { type: "string" }, reason: { type: "string" } },
    },
  },
  {
    name: "kb_promote",
    tiers: [1],
    description: "Promote a candidate or stale KB entry in this repository to active/verified status.",
    inputSchema: {
      type: "object",
      required: ["id"],
      properties: {
        id: { type: "string" },
        title: { type: "string" },
        summary: { type: "string" },
        context_md: { type: "string" },
        verified_sha: { type: "string" },
      },
    },
  },
  {
    name: "kb_archive",
    tiers: [1],
    description: "Archive an obsolete or superseded KB entry in this repository (bi-temporal; preserves history).",
    inputSchema: {
      type: "object",
      required: ["id"],
      properties: {
        id: { type: "string" },
        reason: { type: "string" },
        superseded_by: { type: "string" },
      },
    },
  },
  // ---- Cross-Run Coordination (strictly Tier 1 Orchestrator-to-Orchestrator) ----
  {
    name: "list_peer_runs",
    tiers: [1],
    description: "Discover active sibling runs in harnessd, their coordination_group, repo_root, base_branch, and active write scopes.",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "contract_publish",
    tiers: [1],
    description: "Publish or freeze a versioned cross-run interface contract (single-writer, immutable versions, optimistic expected_version).",
    inputSchema: {
      type: "object",
      required: ["namespace", "key", "payload", "summary"],
      properties: {
        namespace: { type: "string" },
        key: { type: "string" },
        payload: {},
        summary: { type: "string" },
        status: { type: "string", enum: ["draft", "frozen", "deprecated"] },
        expected_version: { type: "number" },
      },
    },
  },
  {
    name: "contract_get",
    tiers: [1],
    description: "Read the latest or a pinned version of a cross-run contract.",
    inputSchema: {
      type: "object",
      required: ["namespace", "key"],
      properties: { namespace: { type: "string" }, key: { type: "string" }, version: { type: "number" } },
    },
  },
  {
    name: "peer_notify",
    tiers: [1],
    description: "Send a non-blocking advisory to another top-level run's Orchestrator (zero deadlock risk).",
    inputSchema: {
      type: "object",
      required: ["to_run_id", "topic_key", "message"],
      properties: { to_run_id: { type: "string" }, topic_key: { type: "string" }, message: { type: "string" } },
    },
  },
  {
    name: "peer_request",
    tiers: [1],
    description:
      "Send a bounded cross-run request to a coordinated sibling Orchestrator (enforces max_rounds=2 and DFS deadlock cycle detection).",
    inputSchema: {
      type: "object",
      required: ["to_run_id", "topic_key", "message"],
      properties: {
        to_run_id: { type: "string" },
        topic_key: { type: "string" },
        message: { type: "string" },
        timeout_s: { type: "number" },
      },
    },
  },
  {
    name: "peer_reply",
    tiers: [1],
    description: "Reply to an open cross-run peer_request and release its wait-graph edge.",
    inputSchema: {
      type: "object",
      required: ["request_id", "reply"],
      properties: { request_id: { type: "string" }, reply: { type: "string" } },
    },
  },
  {
    name: "wait_timer",
    tiers: [1, 2],
    description:
      "Explicitly sleep with zero token burn for a duration (delay_seconds) or until an ISO timestamp (wake_at, e.g. a quota reset time), optionally waking early when a subscribed event occurs (child_settled, any). Work productively by default — use this when pacing around quota limits/resets per user/session instructions or waiting for a timed condition.",
    inputSchema: {
      type: "object",
      required: ["reason"],
      properties: {
        delay_seconds: { type: "number", description: "Seconds to sleep before waking." },
        wake_at: { type: "string", description: "ISO-8601 UTC timestamp to wake at (e.g. a quota resetsAt value)." },
        reason: { type: "string", description: "Concise reason for sleeping (shown in live run telemetry)." },
        wake_on_event: {
          type: "string",
          enum: ["none", "child_settled", "any"],
          description: "Optional early-wake event trigger (default: 'none').",
        },
      },
    },
  },
  {
    name: "schedule_timer",
    tiers: [1, 2],
    description:
      "Schedule a durable non-blocking timer (survives daemon restart) for a reminder without blocking the current turn. When it fires (or a child of yours settles, with wake_on_event child_settled/any), the reason is delivered as a steering message only if this task is still running, for example inside wait_children; it does not restart a finished task.",
    inputSchema: {
      type: "object",
      required: ["reason"],
      properties: {
        delay_seconds: { type: "number", description: "Seconds from now until the timer fires." },
        wake_at: { type: "string", description: "ISO-8601 UTC timestamp when the timer should fire." },
        reason: { type: "string", description: "Concise description of what to do when the timer fires." },
        wake_on_event: { type: "string", enum: ["none", "child_settled", "any"] },
      },
    },
  },
  {
    name: "list_timers",
    tiers: [1, 2],
    description: "List pending or recent durable timers owned by this task.",
    inputSchema: {
      type: "object",
      properties: { status: { type: "string", enum: ["pending", "fired", "cancelled"] } },
    },
  },
  {
    name: "cancel_timer",
    tiers: [1, 2],
    description: "Cancel a pending durable timer by ID (e.g. timer-1).",
    inputSchema: {
      type: "object",
      required: ["timer_id"],
      properties: { timer_id: { type: "string" }, reason: { type: "string" } },
    },
  },
  {
    name: "resume_task",
    tiers: [1],
    description:
      "Resume a failed, crashed, blocked, or cancelled child task in-place with a recovery handoff instead of spawning a duplicate task. A Worker keeps its worktree and WIP commits when the worktree still exists; a cancelled Worker's worktree was archived, so it restarts in a fresh worktree from the base branch. Safe to repeat: when the task is already running, queued, waiting, or completed it returns its current state (outcome + message) without restarting anything.",
    inputSchema: {
      type: "object",
      required: ["task_id"],
      properties: {
        task_id: { type: "string", description: "Failed/crashed child task ID (e.g. t-2)." },
        note: { type: "string", description: "Optional guidance for the resumed generation." },
        model: { type: "string", description: "Optional replacement model ID from list_models." },
        effort: { type: "string", description: "Optional reasoning effort override." },
        profile_id: { type: "string", description: "Optional ACP/Pi profile override." },
      },
    },
  },
  {
    name: "queue_list",
    tiers: [1, 2],
    description: "Inspect the unified admission queue (queued goals & workers, 1-based positions, and active concurrency limits).",
    inputSchema: {
      type: "object",
      properties: {
        lane: { type: "string", enum: ["goal", "worker"], description: "Optional lane filter ('goal' or 'worker')." },
      },
    },
  },
  {
    name: "queue_reorder",
    tiers: [1, 2],
    description: "Move a queued worker (or goal) in the admission queue to any 1-based position ('front', 'back', or integer >= 1).",
    inputSchema: {
      type: "object",
      required: ["target_id", "position"],
      properties: {
        lane: { type: "string", enum: ["goal", "worker"], description: "Queue lane (defaults to 'worker' for t-* IDs, 'goal' for goal-* IDs)." },
        target_id: { type: "string", description: "Queued task ID (e.g. t-4) or goal ID (e.g. goal-2)." },
        position: { description: "Target position: 1-based integer (1 = next up), 'front', or 'back'." },
      },
    },
  },
  {
    name: "queue_force_start",
    tiers: [1],
    description:
      "Emergency override: force-start a queued worker immediately bypassing maxConcurrentWorkers (and optionally bypassing a quota pause when bypass_quota=true). WARNING: Do NOT abuse this override — use only for urgent blockers or explicit user requests.",
    inputSchema: {
      type: "object",
      required: ["target_id"],
      properties: {
        target_id: { type: "string", description: "Queued or quota-paused worker task ID (e.g. t-4)." },
        bypass_quota: { type: "boolean", description: "If true, also force-resume even when paused on quota (use sparingly!)." },
        reason: { type: "string", description: "Brief justification for bypassing the limit." },
      },
    },
  },
];

export function toolsForTier(tier: AgentTier): ToolDefinition[] {
  return ALL_TOOL_DEFS.filter((t) => t.tiers.includes(tier)).map(({ name, description, inputSchema }) => ({
    name,
    description,
    inputSchema,
  }));
}

export class ControlPlaneToolExecutor {
  constructor(
    private readonly db: ControlPlaneDb,
    private readonly getCatalogCompiler: () => ModelCatalogCompiler,
    private readonly hooks: ToolRuntimeHooks,
    private readonly config: HarnessConfig,
  ) {}

  tools(tier: AgentTier): ToolDefinition[] {
    return exposedTools(toolsForTier(tier), this.config.codeMode);
  }

  /**
   * `callerSignal` is the calling turn's abort signal (e.g. a Director chat turn). Unlike `scriptSignal`
   * it does not imply code_exec, so it never trips the active-generation gate for synthetic owners.
   */
  async execute(
    runId: string,
    taskId: string,
    name: string,
    args: Record<string, unknown>,
    scriptSignal?: AbortSignal,
    callerSignal?: AbortSignal,
  ): Promise<unknown> {
    const task = this.db.getTask(taskId);
    if (!task || task.runId !== runId) {
      throw new Error(`Task ${taskId} not found in run ${runId}`);
    }
    const mode = effectiveCodeMode(this.config.codeMode);
    if (scriptSignal || (name === CODE_EXEC_TOOL_NAME && mode !== "off")) {
      const signal = scriptSignal ?? this.hooks.getTaskSignal(taskId);
      if (!signal || signal.aborted || ["completed", "cancelled", "failed", "returned_for_rescope", "paused_quota"].includes(task.status)) {
        // Name the actual cause (whole-run stop first, then this task's cancellation) instead of a bare refusal.
        const runStatus = this.db.getRun(runId)?.status;
        const why =
          runStatus === "stopped" && !isDirectorTaskContainer(runId)
            ? `run ${runId} is stopped (${
                (this.hooks.describeRunCause?.(runId) ?? this.db.getLastRunStatusReason(runId))?.trim() ||
                `no cause was recorded; inspect with \`pi-harness tree ${runId}\``
              })`
            : task.status === "cancelled"
              ? `task ${taskId} was cancelled (${task.summary?.trim() || "no reason recorded"})`
              : signal?.aborted
                ? `the calling turn of ${taskId} was cancelled`
                : `task ${taskId} is ${task.status}`;
        throw new Error(`code_exec requires an active task generation for ${taskId}: ${why}. Nothing was executed.`);
      }
    }
    const run = this.db.getRun(runId);
    if (!run) throw new Error(`Run ${runId} not found`);

    if (name === "__list_tools") {
      return this.tools(task.tier);
    }
    if (mode !== "off" && (name === CODE_EXEC_TOOL_NAME || name === TOOL_SEARCH_TOOL_NAME)) {
      const signal = this.hooks.getTaskSignal(taskId);
      return createCodeModeExecutor({
        ...this.config.codeExec,
        mode,
        allowedTools: () => toolsForTier(task.tier),
        execute: (inner, input, execSignal) => this.execute(runId, taskId, inner, input, execSignal ?? signal),
        artifactsDir: resolve(run.repoRoot, this.config.worktree.artifactsDir ?? ".harness/artifacts"),
        pythonCacheDir: join(this.config.stateDir, "code-mode", "python"),
        saveOutput: (name, json) => writeCodeOutput(this.db, run, taskId, name, json),
        signal,
        onToolEvent: codeExecEventSink((ev) => this.db.emitEvent(runId, taskId, "agent.tool", ev)),
      }).execute(name, args);
    }

    const allowed = new Set(toolsForTier(task.tier).map((t) => t.name));
    if (!allowed.has(name)) {
      throw new Error(`Tool '${name}' is not permitted for Tier ${task.tier} agent ${taskId}`);
    }

    switch (name) {
      case "tree_view": {
        const includeSettled = args.include_settled !== false;
        const tasks = this.db
          .listTasks(runId)
          .filter((t) => includeSettled || !["completed", "cancelled", "failed"].includes(t.status));
        const escalations = this.db.listEscalations(runId, "open");
        const pendingMsgs = this.db.listOpenMessages(taskId);
        const answeredMsgs = this.db.listAnsweredMessagesFrom(taskId);
        const openPeerRequests = task.tier === 1 ? this.db.listOpenPeerExchangesForRun(runId) : [];
        return {
          run: { id: run.id, goal: run.goal, status: run.status, baseBranch: run.baseBranch },
          tasks: tasks.map((t) => ({
            id: t.id,
            parentId: t.parentId,
            tier: t.tier,
            kind: t.kind,
            title: t.title,
            status: t.status,
            model: t.model ?? t.profileId,
            effort: t.effort,
            gen: t.generation,
            ctxPct: t.contextSize > 0 ? Math.round((t.contextUsed / t.contextSize) * 100) : null,
            summary: compactTaskHandoff(t).summary,
          })),
          openEscalations: escalations,
          inboxMessages: pendingMsgs.map((m) => ({
            id: m.id,
            from: m.fromTaskId,
            question: `<untrusted_peer_input>${m.question}</untrusted_peer_input>`,
          })),
          ...(answeredMsgs.length > 0 ? { answeredMessages: answeredMsgs } : {}),
          ...(openPeerRequests.length > 0 ? { openPeerRequests } : {}),
        };
      }

      case "task_get": {
        const target = this.db.getTask(String(args.task_id));
        if (!target || target.runId !== runId) throw new Error(`Unknown task ${args.task_id}`);
        return {
          task: compactTaskHandoff(target),
          artifacts: this.db.listArtifacts(runId, target.id, 30, "dossier"),
          escalations: this.db.listEscalations(runId).filter((e) => e.fromTaskId === target.id || e.toTaskId === target.id),
        };
      }

      case "list_models": {
        const compiled = await this.getCatalogCompiler().getCatalog({
          cwd: run.repoRoot,
          role: typeof args.role === "string" ? args.role : undefined,
          maxApiEquivUsd: typeof args.max_api_equiv_usd === "number" ? args.max_api_equiv_usd : undefined,
          excludeExhausted: Boolean(args.exclude_exhausted),
        });
        return {
          syncedAt: compiled.syncedAt,
          sources: compiled.sources,
          sessionModelContext: this.resolveSessionModelContext(runId),
          recommendedByKind: recommendModelsByTaskKind(compiled.models, { avoidModelId: task.model }),
          models: compiled.models.map((m) => ({
            id: m.id,
            family: m.family,
            rank: m.codingRank,
            codingScore: m.codingScore,
            profile: m.preferredRoute.profileId,
            wireModel: m.preferredRoute.wireModelId,
            billing: m.preferredRoute.billing,
            efforts: m.efforts,
            contextK: m.contextK,
            costPerTaskUsd: m.costPerTaskUsd,
            apiEquivCostUsd: m.apiEquivCostUsd,
            timePerTaskS: m.timePerTaskSeconds,
            quota: m.quota
              ? {
                  availability: m.quota.availabilityName,
                  exhausted: m.quota.exhausted,
                  remainingPct:
                    m.quota.effectiveRemainingRatio === null ? null : Math.round(m.quota.effectiveRemainingRatio * 100),
                  bucket: m.quota.bindingBucketId,
                  nextAvailableAt: m.quota.nextAvailableAt,
                }
              : null,
            recommendedFor: m.recommendedFor,
            ...(m.alternateRoutes.length > 0
              ? {
                  alternateRoutes: m.alternateRoutes.map((r) => ({
                    profile: r.profileId,
                    wireModel: r.wireModelId,
                    billing: r.billing,
                  })),
                }
              : {}),
          })),
        };
      }

      case "spawn_worker": {
        if (task.status === "draining") {
          throw new Error("Orchestrator is DRAINING for rollover — finish handoff instead of spawning new workers.");
        }
        const writeScopes = Array.isArray(args.write_scopes) ? (args.write_scopes as string[]) : [];
        if (writeScopes.length === 0) throw new Error("spawn_worker requires non-empty write_scopes");
        // Avoid starving sibling runs with a blanket repo-root wildcard when other active runs share this target branch
        const hasBlanketWildcard = writeScopes.some((s) => {
          const n = s.trim().replace(/^\.\//, "").replace(/\/+$/, "");
          return n === "" || n === "." || n === "*" || n === "**" || n === "**/*";
        });
        if (hasBlanketWildcard) {
          const otherActiveSameBranch = this.db
            .listRuns(100)
            .some((r) => r.id !== runId && r.status === "active" && r.repoRoot === run.repoRoot && r.baseBranch === run.baseBranch);
          if (otherActiveSameBranch) {
            throw new Error(
              `Blanket write_scopes (${writeScopes.join(", ")}) are not allowed while another active run targets (${run.repoRoot}:${run.baseBranch}). Specify scoped module/directory globs instead.`,
            );
          }
        }
        const posArg = args.position as QueuePositionInput | undefined;
        const created = await this.hooks.spawnAndStartTask({
          runId,
          parentId: taskId,
          tier: 2,
          kind: "worker",
          title: String(args.title),
          objective: String(args.objective),
          criteria: typeof args.criteria === "string" ? args.criteria : "",
          readOnly: false,
          profileId: optionalProfile(args.profile_id),
          model: requiredModel(args),
          effort: typeof args.effort === "string" ? args.effort : undefined,
          dependsOn: Array.isArray(args.depends_on) ? (args.depends_on as string[]) : [],
          writeScopes,
          ...(posArg !== undefined ? { position: posArg } : {}),
          ...(args.force_start !== undefined ? { forceStart: Boolean(args.force_start) } : {}),
          signal: callerSignal ?? scriptSignal ?? this.hooks.getTaskCancelSignal?.(taskId),
        });
        const qEntry = this.db.admissionQueue.get("worker", created.id);
        return {
          spawned: created.id,
          status: created.status,
          worktreeId: created.worktreeId,
          ...(qEntry ? { queued: true, queuePosition: qEntry.position } : {}),
        };
      }

      case "queue_list": {
        const lane = args.lane === "goal" || args.lane === "worker" ? (args.lane as AdmissionLane) : undefined;
        const entries = this.db.admissionQueue.list({ lane, repoRoot: run.repoRoot });
        return {
          repoRoot: run.repoRoot,
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
            : targetId.startsWith("goal-")
              ? "goal"
              : "worker";
        const position = args.position as QueuePositionInput;
        const updated = this.db.admissionQueue.reorder(lane, targetId, position);
        return { reordered: updated, queue: this.db.admissionQueue.list({ lane, repoRoot: updated.repoRoot }) };
      }

      case "queue_force_start": {
        if (!this.hooks.forceStartWorker) throw new Error("queue_force_start is not available in this runtime");
        const targetId = String(args.target_id ?? "").trim();
        if (!targetId) throw new Error("target_id is required");
        return await this.hooks.forceStartWorker(targetId, {
          bypassQuota: Boolean(args.bypass_quota),
          reason: typeof args.reason === "string" ? args.reason : undefined,
        });
      }

      case "spawn_task": {
        const kind = String(args.kind);
        const readOnlyKind = isReadOnlyKind(kind);
        const fromOrchestrator = task.tier === 1;
        if (fromOrchestrator && !readOnlyKind) {
          throw new Error("Tier 1 spawn_task only permits research, plan, and verify; use spawn_worker for implementation.");
        }
        if (!readOnlyKind && kind !== "execute") throw new Error(`Unknown spawn_task kind: ${kind}`);
        if (task.status === "draining") throw new Error("Cannot spawn tasks while draining for rollover.");

        const created = await this.hooks.spawnAndStartTask({
          runId,
          parentId: taskId,
          tier: 3,
          kind,
          title: String(args.title),
          objective: String(args.objective),
          criteria: typeof args.criteria === "string" ? args.criteria : "",
          readOnly: readOnlyKind || args.read_only === true,
          profileId: optionalProfile(args.profile_id),
          model: requiredModel(args),
          effort: typeof args.effort === "string" ? args.effort : undefined,
          maxTurns: typeof args.max_turns === "number" ? args.max_turns : undefined,
          dependsOn: Array.isArray(args.depends_on) ? (args.depends_on as string[]) : [],
          signal: callerSignal ?? scriptSignal ?? this.hooks.getTaskCancelSignal?.(taskId),
        });
        return { spawned: created.id, kind: created.kind, readOnly: created.readOnly, status: created.status };
      }

      case "wait_tasks":
      case "wait_children": {
        const ids = Array.isArray(args.ids) ? (args.ids as string[]) : undefined;
        const mode = args.mode === "any" ? "any" : "all";
        const defaultSec = this.config.events?.fallbackWakeupIntervalSec ?? 600;
        const requestedSec =
          typeof args.fallback_interval_s === "number"
            ? args.fallback_interval_s
            : typeof args.timeout_s === "number"
              ? args.timeout_s
              : defaultSec;
        const timeoutMs = Math.max(1, requestedSec) * 1000;
        const waitSig = scriptSignal ?? this.hooks.getTaskSignal(taskId);
        const waitRes = await this.hooks.waitForChildren(taskId, ids, mode, timeoutMs, waitSig);
        const settled = waitRes.children;
        const escalations = this.db.listEscalations(runId);
        const stillRunning = settled.filter((c) => ["running", "ready", "pending", "draining", "waiting_children", "merging"].includes(c.status));
        const steeringMessages = this.hooks.drainPendingSteers?.(taskId) ?? [];
        const steering = steeringMessages.length > 0
          ? {
              wakeReason: "steered",
              steeringMessages,
              hint: "Steering received while you waited (steeringMessages, oldest first). Apply it before waiting again.",
            }
          : {};
        return {
          children: settled.map((child) => compactTaskHandoff(child, escalations)),
          ...(waitRes.wakeReason !== "event" && waitRes.wakeReason !== "already_settled"
            ? {
                wakeReason: waitRes.wakeReason,
                ...(waitRes.inactiveTaskIds.length > 0 ? { inactiveTaskIds: waitRes.inactiveTaskIds } : {}),
                ...(waitRes.wakeReason === "child_inactive"
                  ? { hint: `Inactive child task(s) detected: ${waitRes.inactiveTaskIds.join(", ")}. Inspect, then ${task.tier === 1 ? "resume_task or " : ""}cancel_child.` }
                  : waitRes.wakeReason === "escalation"
                    ? { hint: "Woke on a new escalation. Use ack_escalation only for escalations addressed to you; P0 escalations go to the human. Otherwise steer, cancel, or re-scope the affected child before waiting again." }
                    : waitRes.wakeReason === "steered"
                      ? { hint: "Woke on incoming steering or goal amendment; check Blackboard/goal state." }
                      : stillRunning.length > 0
                        ? { hint: "Fallback wakeup fired while child task(s) are still in progress; wait again with wait_children rather than short-polling." }
                        : {}),
              }
            : mode === "all" && stillRunning.length > 0
              ? {
                  wakeReason: "fallback_interval",
                  hint: "Fallback wakeup fired while child task(s) are still in progress; wait again with wait_children rather than short-polling.",
                }
              : {}),
          ...steering,
        };
      }

      case "steer_child":
      case "cancel_task":
      case "cancel_child": {
        const childId = String(args.task_id);
        const child = this.db.getTask(childId);
        if (!child || child.runId !== runId || child.parentId !== taskId) throw new Error(`Unknown child task ${childId}`);
        if (name === "steer_child") {
          return this.hooks.steerTaskDetailed
            ? await this.hooks.steerTaskDetailed(child.id, String(args.message))
            : { steered: await this.hooks.steerTask(child.id, String(args.message)) };
        }
        await this.hooks.cancelTask(child.id, String(args.reason));
        return { cancelled: child.id };
      }

      case "report_goal_progress": {
        const updated = this.updateLinkedGoalProgress(runId, taskId, {
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
        });
        return updated;
      }

      case "return_task": {
        const status = String(args.status);
        if (status !== "completed" && status !== "blocked" && status !== "scope_too_large") {
          throw new Error("return_task requires status completed, blocked, or scope_too_large");
        }
        const { summary: fullSummary, sources, artifactPaths } = validateTaskHandoff(task.kind, args);
        let summary = fullSummary;

        if (status === "completed") {
          const activeKids = this.db
            .listTasks(runId, { parentId: taskId })
            .filter((c) => ["running", "ready", "pending", "waiting_children", "merging"].includes(c.status));
          if (activeKids.length > 0) {
            throw new Error(
              `Settlement gate rejected completion: ${activeKids.length} child task(s) are still active (${activeKids.map((k) => k.id).join(", ")}).`,
            );
          }
          if (task.tier === 2 && task.worktreeId) {
            const wt = this.db.getWorktree(task.worktreeId);
            if (wt && !wt.cleanedAt) {
              const wtMgr = this.hooks.getWorktreeManager(run.repoRoot);
              if (await wtMgr.isDirty(wt.path)) {
                throw new Error(
                  `Settlement gate rejected completion: worktree ${wt.path} has uncommitted changes. Commit and call submit_for_merge first.`,
                );
              }
              if (wt.status === "queued" || wt.status === "bounced") {
                throw new Error(
                  `Settlement gate rejected completion: worktree merge status is '${wt.status}'. Resolve issues and complete submit_for_merge before returning completed.`,
                );
              }
              // Verify any commits on the worker's HEAD are actually contained in targetBranch (catches unmerged commits
              // AND post-merge commits added after an earlier submit_for_merge!).
              const wtHead = await runGit(wt.path, ["rev-parse", "HEAD"]).then((r) => r.stdout).catch(() => null);
              if (wtHead && wtHead !== wt.baseSha) {
                const contained = await runGit(run.repoRoot, ["merge-base", "--is-ancestor", wtHead, run.baseBranch]).then(
                  () => true,
                  () => false,
                );
                if (!contained) {
                  throw new Error(
                    `Settlement gate rejected completion: worktree HEAD (${wtHead.slice(0, 8)}) has unmerged commits not on ${run.baseBranch}. Call submit_for_merge first.`,
                  );
                }
              }
            }
          }
        }

        const paths = [...artifactPaths];
        let overflowPath: string | undefined;
        if (summary.length > SUMMARY_LIMIT) {
          const spill = spillSummaryOverflow(this.db, run, taskId, summary);
          summary = spill.summary;
          overflowPath = spill.overflowPath;
          if (overflowPath) paths.push(overflowPath);
        }
        const taskStatus = status === "scope_too_large" ? "returned_for_rescope" : status;
        try {
          return this.db.transaction(() => {
            let escalationId: string | undefined;
            let suggestedSplits: string[] | undefined;
            if (status === "scope_too_large") {
              suggestedSplits = Array.isArray(args.suggested_splits) ? (args.suggested_splits as string[]) : [];
              escalationId = this.db.createEscalation({
                runId,
                fromTaskId: taskId,
                toTaskId: task.parentId,
                severity: task.tier === 2 ? "P1" : "P2",
                category: "scope_too_large",
                title: `Scope too large for ${task.title}: ${fullSummary.slice(0, 140)}`,
                evidencePaths: Array.isArray(args.evidence_paths) ? (args.evidence_paths as string[]) : paths,
                suggestedSplits,
                blocking: true,
              }).id;
            }
            if (overflowPath) {
              this.db.recordArtifact({ runId, taskId, kind: "dossier", path: overflowPath, summary: "Full task summary" });
            }
            this.db.updateTask(taskId, {
              status: taskStatus,
              summary,
              resultJson: JSON.stringify({ status, summary, sources, artifact_paths: paths, escalationId, suggestedSplits }),
            });
            // Automatically reflect verify task outcomes onto any linked Goal without requiring extra model calls.
            // An intermediate verify (a Tier 3 probe, or a failed verify while other work is live) only updates the
            // summary: its pass/fail is not the run's final verification and must not gate settlement.
            if (task.kind === "verify") {
              const liveStatuses = ["running", "waiting_children", "ready", "pending", "draining", "merging"];
              const otherLive = this.db.listTasks(runId).some((t) => t.id !== taskId && liveStatuses.includes(t.status));
              const intermediate = (task.tier === 3 && otherLive) || (taskStatus !== "completed" && otherLive);
              this.updateLinkedGoalProgress(
                runId,
                taskId,
                intermediate
                  ? { summary: summary.slice(0, 300) }
                  : { verificationStatus: taskStatus === "completed" ? "passed" : "failed", summary: summary.slice(0, 300) },
                "verify_task",
              );
            }
            return { returned: true, status: taskStatus, ...(escalationId ? { escalationId } : {}) };
          });
        } catch (error) {
          if (overflowPath) rmSync(overflowPath, { force: true });
          throw error;
        } finally {
          // Tear down a merged Tier 2 worktree once the worker settles via return_task
          if (status === "completed" && task.tier === 2 && task.worktreeId) {
            const wt = this.db.getWorktree(task.worktreeId);
            if (wt && wt.status === "merged" && !wt.cleanedAt) {
              void this.hooks
                .getWorktreeManager(run.repoRoot)
                .teardown(wt.id, { archiveBeforeRemove: false, finalStatus: "merged" })
                .catch(() => {});
            }
          }
        }
      }

      case "report_issue": {
        const severity = String(args.severity) as EscalationSeverity;
        const category = String(args.category) as EscalationCategory;
        const evidencePaths = Array.isArray(args.evidence_paths) ? (args.evidence_paths as string[]) : [];
        const attempted = Array.isArray(args.attempted) ? (args.attempted as string[]) : [];
        const blocking = args.blocking === undefined ? severity !== "P2" : Boolean(args.blocking);

        const toTaskId = severity === "P0" ? null : task.parentId;
        const esc = this.db.createEscalation({
          runId,
          fromTaskId: taskId,
          toTaskId,
          severity,
          category,
          title: String(args.title),
          evidencePaths,
          attempted,
          blocking,
        });
        if (blocking) {
          this.db.updateTask(taskId, { status: "blocked", summary: `Blocked on ${esc.id}: ${esc.title}` });
          this.hooks.stopTaskForBlockingIssue?.(taskId, `Blocked on ${esc.id}: ${esc.title}`);
        }
        return { escalationId: esc.id, severity: esc.severity, routedTo: toTaskId ?? "human" };
      }

      case "ack_escalation": {
        const id = String(args.escalation_id);
        if (!this.db.listEscalations(runId, "open").some((e) => e.id === id && e.toTaskId === taskId)) {
          throw new Error(`Unknown incoming escalation ${id}`);
        }
        const updated = this.db.ackEscalation(id, String(args.resolution), true);
        return { escalation: updated };
      }

      case "request_rollover": {
        const handoff: AnchoredHandoff = {
          goalInvariants: Array.isArray(args.goal_invariants) ? (args.goal_invariants as string[]) : [],
          architecturalDecisions: Array.isArray(args.architectural_decisions)
            ? (args.architectural_decisions as Array<{ decision: string; rationale: string }>)
            : [],
          activeRisks: Array.isArray(args.active_risks) ? (args.active_risks as string[]) : [],
          nextActions: Array.isArray(args.next_actions) ? (args.next_actions as string[]) : [],
        };
        await this.hooks.scheduleRollover(taskId, handoff, {
          profileId: optionalProfile(args.target_profile_id),
          model: typeof args.target_model === "string" ? args.target_model : undefined,
          effort: typeof args.target_effort === "string" ? args.target_effort : undefined,
        });
        return { rolloverScheduled: true, nextGeneration: task.generation + 1 };
      }

      case "query_predecessor": {
        const rawQuery = String(args.query ?? "").trim();
        const q = rawQuery.toLowerCase();
        const limit = typeof args.limit === "number" && Number.isFinite(args.limit) ? Math.max(1, Math.min(50, Math.floor(args.limit))) : 12;
        const matches = this.db.searchPredecessorHistory(runId, taskId, rawQuery, limit);
        const tokens = q.split(/\s+/).filter(Boolean);
        const arts = this.db
          .listArtifacts(runId, taskId, 100)
          .filter((a) => {
            const hay = `${a.path} ${a.summary}`.toLowerCase();
            return tokens.length > 0 && tokens.some((tok) => hay.includes(tok));
          })
          .slice(0, limit);
        return { matchingEvents: matches, matchingArtifacts: arts };
      }

      case "artifact_write": {
        const art = writeTaskDossier(this.db, run, taskId, String(args.name), String(args.content), String(args.summary));
        return { artifactId: art.id, path: art.path };
      }

      case "artifact_read": {
        if (task.tier === 1 || typeof args.path !== "string" || !args.path || findRunDossier(this.db, run, args.path) !== null) {
          return readRunDossier(this.db, run, args);
        }
        const root = task.worktreeId ? this.db.getWorktree(task.worktreeId)?.path : run.repoRoot;
        if (!root) throw new Error("Task worktree unavailable");
        return readWorkspaceArtifact(root, args.path, args.max_chars, args.offset);
      }

      case "blackboard_set": {
        this.db.blackboardSet(runId, String(args.key), args.value, taskId);
        return { ok: true, key: String(args.key) };
      }

      case "blackboard_get": {
        return this.db.blackboardGet(runId, typeof args.key === "string" ? args.key : undefined);
      }

      case "ask_agent": {
        const targetId = String(args.target_id);
        const target = this.db.getTask(targetId);
        if (!target || target.runId !== runId) throw new Error(`Unknown target task ${targetId}`);
        if (target.tier === 3) throw new Error(`Task ${targetId} is a Tier 3 agent without reply_agent; use steer_child for your own Tier 3 children.`);
        const msg = this.db.createMessage(runId, taskId, targetId, String(args.question));
        // Best-effort steer notification to target agent
        void this.hooks
          .steerTask(
            targetId,
            `You received mediated peer question ${msg.id} from ${taskId}:\n<untrusted_peer_input>${msg.question}</untrusted_peer_input>\nUse reply_agent({ message_id: "${msg.id}", answer: "..." }) if appropriate.`,
          )
          .catch(() => {});
        return { messageId: msg.id, status: msg.status };
      }

      case "reply_agent": {
        const id = String(args.message_id);
        if (!this.db.listOpenMessages(taskId).some((message) => message.id === id)) throw new Error(`Unknown incoming message ${id}`);
        const msg = this.db.replyMessage(id, String(args.answer));
        void this.hooks
          .steerTask(
            msg.fromTaskId,
            `Peer answer to your message ${msg.id} from ${taskId}:\n${msg.answer ?? ""}`,
          )
          .catch(() => {});
        return { messageId: msg.id, status: msg.status };
      }

      case "worktree_status": {
        if (!task.worktreeId) throw new Error("This task has no assigned worktree");
        const wt = this.db.getWorktree(task.worktreeId);
        if (!wt) throw new Error(`Worktree ${task.worktreeId} missing`);
        const mgr = this.hooks.getWorktreeManager(run.repoRoot);
        const dirty = await mgr.isDirty(wt.path);
        return { worktree: wt, dirty };
      }

      case "submit_for_merge": {
        if (!task.worktreeId) throw new Error("This task has no assigned worktree");
        const wt = this.db.getWorktree(task.worktreeId);
        if (!wt) throw new Error(`Worktree ${task.worktreeId} missing`);
        const mgr = this.hooks.getWorktreeManager(run.repoRoot);
        if (await mgr.isDirty(wt.path)) {
          throw new Error("Cannot submit_for_merge while worktree has uncommitted changes. Commit first.");
        }
        const item = this.db.enqueueMerge({ runId, taskId, worktreeId: wt.id, branch: wt.branch });
        // Use the true task cancellation signal (not generation/rollover signal) so an auto-rollover never aborts an in-flight merge.
        const mergeSig = this.hooks.getTaskCancelSignal?.(taskId);
        const outcome = await this.hooks
          .getMergeQueue(run.repoRoot)
          .submitAndDrain(runId, item.id, { preserveLiveTask: true, signal: mergeSig });
        return {
          mergeId: item.id,
          status: outcome.item.status,
          mergedSha: outcome.mergedSha,
          bouncedReason: outcome.bouncedReason,
        };
      }

      case "kb_search":
        return {
          cards: this.db.kb.search({
            query: typeof args.query === "string" ? args.query : undefined,
            repoScope: run.repoRoot,
            pathGlobs: Array.isArray(args.path_globs) ? (args.path_globs as string[]) : undefined,
            tags: Array.isArray(args.tags) ? (args.tags as string[]) : undefined,
            kinds: Array.isArray(args.kinds) ? (args.kinds as KbKind[]) : undefined,
            includeCandidates: Boolean(args.include_candidates),
            includeStale: Boolean(args.include_stale),
            limit: typeof args.limit === "number" ? args.limit : 10,
          }),
        };

      case "kb_get": {
        const ids = Array.isArray(args.ids) ? (args.ids as string[]) : [];
        const visible = ids.filter((id) => {
          const scope = this.db.kb.getRaw(id)?.repoScope;
          return scope === run.repoRoot || scope === "global";
        });
        return { entries: this.db.kb.getMany(visible) };
      }

      case "kb_propose": {
        const headSha = await runGit(run.repoRoot, ["rev-parse", "HEAD"])
          .then((r) => r.stdout || run.baseSha)
          .catch(() => run.baseSha);
        const entry = this.db.kb.propose({
          repoScope: run.repoRoot,
          kind: String(args.kind) as KbKind,
          title: String(args.title),
          summary: String(args.summary),
          contextMd: String(args.context_md),
          tags: Array.isArray(args.tags) ? (args.tags as string[]) : [],
          pathGlobs: Array.isArray(args.path_globs) ? (args.path_globs as string[]) : [],
          evidencePaths: Array.isArray(args.evidence_paths) ? (args.evidence_paths as string[]) : [],
          verifiedSha: headSha,
          sourceRunId: runId,
          sourceTaskId: taskId,
        });
        return { id: entry.id, status: entry.status };
      }

      case "kb_flag_stale": {
        const id = String(args.id);
        const existing = this.db.kb.getRaw(id);
        if (!existing || existing.repoScope !== run.repoRoot) {
          throw new Error(`KB entry ${id} not found in repository scope ${run.repoRoot}`);
        }
        return this.db.kb.flagStale(existing.id, String(args.reason));
      }

      case "kb_promote": {
        const id = String(args.id);
        const existing = this.db.kb.getRaw(id);
        if (!existing || existing.repoScope !== run.repoRoot) {
          throw new Error(`KB entry ${id} not found in repository scope ${run.repoRoot}`);
        }
        const verifiedSha =
          typeof args.verified_sha === "string" && args.verified_sha.trim()
            ? args.verified_sha.trim()
            : await runGit(run.repoRoot, ["rev-parse", "HEAD"])
                .then((r) => r.stdout || run.baseSha)
                .catch(() => run.baseSha);
        return this.db.kb.promote(existing.id, {
          title: typeof args.title === "string" ? args.title : undefined,
          summary: typeof args.summary === "string" ? args.summary : undefined,
          contextMd: typeof args.context_md === "string" ? args.context_md : undefined,
          verifiedSha,
        });
      }

      case "kb_archive": {
        const id = String(args.id);
        const existing = this.db.kb.getRaw(id);
        if (!existing || existing.repoScope !== run.repoRoot) {
          throw new Error(`KB entry ${id} not found in repository scope ${run.repoRoot}`);
        }
        const supersededBy =
          typeof args.superseded_by === "string" && args.superseded_by.trim() ? args.superseded_by.trim() : undefined;
        if (supersededBy) {
          const sup = this.db.kb.getRaw(supersededBy);
          if (!sup || sup.repoScope !== run.repoRoot) {
            throw new Error(`Superseding KB entry ${supersededBy} not found in repository scope ${run.repoRoot}`);
          }
        }
        return this.db.kb.archive(existing.id, {
          reason: typeof args.reason === "string" ? args.reason : undefined,
          supersededBy,
        });
      }

      case "list_peer_runs": {
        const peers = this.db
          .listRuns(50)
          .filter((r) => r.id !== runId && r.status === "active")
          .map((r) => ({
            id: r.id,
            goal: r.goal,
            repoRoot: r.repoRoot,
            baseBranch: r.baseBranch,
            coordinationGroup: r.coordinationGroup,
            isolationMode: r.isolationMode,
            activeWriteScopes: this.db.listActiveWriteScopes(r.id),
          }));
        return { peerRuns: peers };
      }

      case "contract_publish":
        return this.db.publishContract({
          namespace: String(args.namespace),
          key: String(args.key),
          ownerRunId: runId,
          payload: args.payload,
          summary: String(args.summary),
          status: typeof args.status === "string" ? (args.status as "draft" | "frozen" | "deprecated") : "draft",
          expectedVersion: typeof args.expected_version === "number" ? args.expected_version : undefined,
        });

      case "contract_get":
        return {
          contract: this.db.getContract(
            String(args.namespace),
            String(args.key),
            typeof args.version === "number" ? args.version : undefined,
          ),
        };

      case "peer_notify":
        return this.db.sendPeerAdvisory(runId, String(args.to_run_id), String(args.topic_key), String(args.message));

      case "peer_request":
        return this.db.createPeerRequest({
          fromRunId: runId,
          toRunId: String(args.to_run_id),
          topicKey: String(args.topic_key),
          message: String(args.message),
          timeoutSec: typeof args.timeout_s === "number" ? args.timeout_s : 120,
        });

      case "peer_reply":
        return this.db.replyPeerRequest(String(args.request_id), runId, String(args.reply));

      case "wait_timer": {
        const timer = this.db.timers.schedule({
          ownerKind: "task",
          ownerId: taskId,
          runId,
          delaySeconds: typeof args.delay_seconds === "number" ? args.delay_seconds : undefined,
          wakeAtIso: typeof args.wake_at === "string" ? args.wake_at : undefined,
          reason: String(args.reason ?? "Timed wait"),
          wakeOnEvent:
            args.wake_on_event === "child_settled" || args.wake_on_event === "any" ? args.wake_on_event : "none",
        });
        this.db.emitEvent(runId, taskId, "agent.timer_sleep", {
          timerId: timer.id,
          wakeAt: timer.wakeAt,
          reason: timer.reason,
          wakeOnEvent: timer.wakeOnEvent,
        });
        const sig = scriptSignal ?? this.hooks.getTaskSignal(taskId);
        const res = await this.db.timers.waitForTimer(timer.id, sig);
        return {
          timerId: res.timer.id,
          status: res.timer.status,
          wakeReason: res.wakeReason,
          reason: res.timer.reason,
          sleptMs: Math.max(0, Date.now() - timer.createdAt),
        };
      }

      case "schedule_timer": {
        const timer = this.db.timers.schedule({
          ownerKind: "task",
          ownerId: taskId,
          runId,
          delaySeconds: typeof args.delay_seconds === "number" ? args.delay_seconds : undefined,
          wakeAtIso: typeof args.wake_at === "string" ? args.wake_at : undefined,
          reason: String(args.reason ?? "Scheduled reminder"),
          wakeOnEvent:
            args.wake_on_event === "child_settled" || args.wake_on_event === "any"
              ? args.wake_on_event
              : "none",
        });
        this.db.emitEvent(runId, taskId, "timer.scheduled", timer);
        return { timer };
      }

      case "list_timers": {
        const status =
          args.status === "pending" || args.status === "fired" || args.status === "cancelled" ? args.status : undefined;
        return { timers: this.db.timers.list({ ownerKind: "task", ownerId: taskId, status }) };
      }

      case "cancel_timer": {
        const id = String(args.timer_id);
        const existing = this.db.timers.get(id);
        if (!existing || existing.ownerKind !== "task" || existing.ownerId !== taskId) {
          throw new Error(`Unknown timer ${id} for task ${taskId}`);
        }
        const cancelled = this.db.timers.cancel(
          existing.id,
          typeof args.reason === "string" ? args.reason : "cancelled_by_agent",
        );
        if (!cancelled) throw new Error(`Unknown timer ${id}`);
        return { timer: cancelled };
      }

      case "resume_task": {
        if (!this.hooks.resumeTask) throw new Error("resume_task hook is not available");
        const target = this.db.getTask(String(args.task_id));
        if (!target || target.runId !== runId || target.parentId !== taskId) {
          throw new Error(`Unknown child task ${args.task_id}`);
        }
        const resumeOpts = {
          note: typeof args.note === "string" ? args.note : undefined,
          model: typeof args.model === "string" ? args.model : undefined,
          effort: typeof args.effort === "string" ? args.effort : undefined,
          profileId: optionalProfile(args.profile_id),
        };
        if (this.hooks.resumeTaskDetailed) {
          const res = await this.hooks.resumeTaskDetailed(target.id, resumeOpts);
          return {
            task: res.task,
            resumed: res.outcome === "resumed",
            outcome: res.outcome,
            message: res.message,
            ...(res.queuePosition !== undefined ? { queuePosition: res.queuePosition } : {}),
            ...(res.noteDelivered !== undefined ? { noteDelivered: res.noteDelivered } : {}),
          };
        }
        const resumed = await this.hooks.resumeTask(target.id, resumeOpts);
        return { task: resumed, resumed: true };
      }

      default:
        throw new Error(`Unhandled tool: ${name}`);
    }
  }

  private updateLinkedGoalProgress(
    runId: string,
    taskId: string,
    patch: {
      summary?: string;
      phase?: string;
      verificationStatus?: "not_needed" | "in_progress" | "passed" | "failed";
      criteriaResults?: Array<{ criterionId: string; passed: boolean; evidence: string; remediation?: string }>;
    },
    source?: GoalProgressSource,
  ): { recorded: boolean; goalId: string | null } {
    try {
      const row = this.db.db
        .prepare(
          "SELECT id, version, review_cycle, live_progress_json FROM goals WHERE linked_run_id = ? ORDER BY updated_at DESC LIMIT 1",
        )
        .get(runId) as { id: string; version: number; review_cycle: number | null; live_progress_json?: string | null } | undefined;
      if (!row) return { recorded: false, goalId: null };

      let prev: GoalLiveProgress | null = null;
      if (typeof row.live_progress_json === "string" && row.live_progress_json.trim()) {
        try {
          prev = JSON.parse(row.live_progress_json) as GoalLiveProgress;
        } catch {
          prev = null;
        }
      }
      const now = Date.now();
      const claimSource: GoalProgressSource = source ?? (this.db.getTask(taskId)?.tier === 1 ? "coordinator" : "worker");
      const next = mergeGoalLiveProgress(prev, { ...patch, reportedByTaskId: taskId }, {
        goalVersion: Number(row.version),
        runId,
        reviewCycle: Number(row.review_cycle ?? 0),
        source: claimSource,
        now,
      });
      this.db.db
        .prepare("UPDATE goals SET live_progress_json = ?, updated_at = ? WHERE id = ?")
        .run(JSON.stringify(next), now, row.id);
      this.db.emitEvent(runId, taskId, "goal.progress", { goalId: row.id, ...next });
      return { recorded: true, goalId: row.id };
    } catch {
      return { recorded: false, goalId: null };
    }
  }

  private resolveSessionModelContext(runId: string): string | null {
    try {
      let chatSessionId: string | null = null;
      if (isDirectorTaskContainer(runId)) {
        chatSessionId = directorTaskRunOwner(runId);
      } else {
        const row = this.db.db
          .prepare(
            "SELECT chat_session_id FROM goals WHERE linked_run_id = ? AND chat_session_id IS NOT NULL ORDER BY updated_at DESC LIMIT 1",
          )
          .get(runId) as { chat_session_id: string } | undefined;
        chatSessionId = row?.chat_session_id ?? null;
      }
      if (!chatSessionId) return null;
      const sess = this.db.db
        .prepare("SELECT model_selection_context FROM chat_sessions WHERE id = ?")
        .get(chatSessionId) as { model_selection_context: string | null } | undefined;
      const ctx = typeof sess?.model_selection_context === "string" ? sess.model_selection_context.trim() : "";
      return ctx || null;
    } catch {
      return null;
    }
  }
}
