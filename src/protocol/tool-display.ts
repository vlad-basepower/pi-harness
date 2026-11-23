import { randomUUID } from "node:crypto";
import type { CodeExecToolEvent } from "../code-mode/index.js";
import type { EscalationRecord, TaskRecord } from "../control-plane/db.js";
import type { RunActivitySummary } from "../control-plane/telemetry.js";
import type { RunnerCallbacks } from "../runners/types.js";
import type { GoalRecord, GoalReviewVerdict } from "../goals/engine.js";

export function toolEventPreview(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  const text = typeof value === "string" ? value : JSON.stringify(value);
  if (!text || text === "{}") return undefined;
  return text.length > 512 ? `${text.slice(0, 511)}…` : text;
}

/** Pair each inner call's start and end, including parallel calls of the same tool. */
export function codeExecEventSink(emit: NonNullable<RunnerCallbacks["onToolEvent"]>) {
  const ids = new WeakMap<CodeExecToolEvent["args"], string>();
  return ({ name, args, phase, ok, error, result, via }: CodeExecToolEvent) => {
    const id = ids.get(args) ?? randomUUID();
    if (phase === "start") ids.set(args, id);
    emit({
      id, name, via,
      status: phase === "start" ? "in_progress" : ok ? "completed" : "failed",
      args: phase === "start" ? toolEventPreview(args) : undefined,
      result,
      error: toolEventPreview(error),
    });
  };
}

/** Structured tool-output display (contract/tool-display-v1). Rendered by the TUI instead of a compact tool pill. */
export interface ToolDisplayPayload {
  kind: string;
  title?: string;
  data: unknown;
}

export interface GoalsTableRow {
  id: string;
  version: number;
  status: string;
  title: string;
  linkedRunId: string | null;
  reviewCycle: number;
  /** Optional explicit max review cap (null when not capped / dynamic). */
  maxReviewCycles?: number | null;
  /** Dynamic verification/review label derived from actual model activity (e.g. `verifying`, `1 verified`, `1 rev`, `skipped`, `—`). */
  verificationLabel?: string | null;
  criteriaTotal: number;
  /** Passed criteria from latest review or live model progress; null if none reported yet. */
  criteriaPassed: number | null;
  openEscalations: number;
}

export interface GoalsTableData {
  goals: GoalsTableRow[];
}

export interface GoalDetailData {
  goal: {
    id: string;
    version: number;
    status: string;
    title: string;
    description: string;
    linkedRunId: string | null;
    reviewCycle: number;
    maxReviewCycles?: number | null;
    verificationLabel?: string | null;
    liveSummary?: string | null;
    nonGoals: string[];
  };
  criteria: Array<{
    id: string;
    kind: string;
    description: string;
    command?: string;
    required: boolean;
    lastResult: "pass" | "fail" | null;
  }>;
  tasks: Array<{
    id: string;
    title: string;
    status: string;
    tier: number;
    kind?: string;
    model?: string | null;
    effort?: string | null;
    currentAction?: string | null;
    toolCalls?: number;
    filesEdited?: number;
    elapsedMs?: number;
  }>;
  activity?: {
    phase: string;
    filesEdited: number;
    elapsedMs: number;
    modelsUsed: string[];
  } | null;
  openEscalations: Array<{ id: string; taskId: string; summary: string }>;
  lastReview: { cycle: number; verdict: string; summary: string } | null;
}

/** The latest review: review cycles strictly increase per goal (amendments never reset them). */
function latestReview(reviews: GoalReviewVerdict[]): GoalReviewVerdict | null {
  let latest: GoalReviewVerdict | null = null;
  for (const r of reviews) if (!latest || r.cycle > latest.cycle) latest = r;
  return latest;
}

function criterionResults(goal: GoalRecord, review: GoalReviewVerdict | null): Map<string, "pass" | "fail"> {
  const results = new Map<string, "pass" | "fail">();
  const ids = new Set(goal.criteria.map((c) => c.id));
  // Seed from live model-reported progress first, then overlay authoritative review verdict if newer/present
  for (const r of goal.liveProgress?.criteriaResults ?? []) {
    if (ids.has(r.criterionId)) results.set(r.criterionId, r.passed ? "pass" : "fail");
  }
  if (review) {
    for (const r of review.criteriaResults) {
      if (ids.has(r.criterionId)) results.set(r.criterionId, r.passed ? "pass" : "fail");
    }
  }
  return results;
}

/** Derives a dynamic verification/review label from actual review records, live `verify` tasks, and model-reported progress. */
export function deriveGoalVerificationLabel(
  goal: GoalRecord,
  tasks: TaskRecord[] = [],
  activity?: RunActivitySummary | null,
): string {
  const verifyTasks = tasks.filter((t) => t.kind === "verify");
  const runningVerify = activity?.counts.verifyRunning ?? verifyTasks.filter((t) => ["running", "ready", "pending", "draining"].includes(t.status)).length;
  const completedVerify = activity?.counts.verifyCompleted ?? verifyTasks.filter((t) => t.status === "completed").length;

  if (goal.status === "draft" && goal.liveProgress?.phase === "queued") {
    const position = goal.liveProgress.summary?.match(/#\d+/)?.[0];
    return position ? `queued ${position}` : "queued";
  }
  if (runningVerify > 0 || goal.status === "verifying" || goal.status === "reviewing" || goal.liveProgress?.verificationStatus === "in_progress") {
    return runningVerify > 1 ? `verifying (${runningVerify})` : "verifying";
  }
  if (goal.reviewCycle > 0) {
    return `${goal.reviewCycle} rev`;
  }
  if (completedVerify > 0) {
    return completedVerify === 1 ? "1 verified" : `${completedVerify} verified`;
  }
  if (goal.liveProgress?.verificationStatus === "passed") return "verified";
  if (goal.liveProgress?.verificationStatus === "failed") return "verify failed";
  if (goal.liveProgress?.verificationStatus === "not_needed" || goal.verificationMode === "none") return "skipped";
  if (activity && ["researching", "planning", "implementing", "merging", "paused_quota"].includes(activity.phase)) {
    return activity.counts.running > 0 ? `${activity.phase} (${activity.counts.running})` : activity.phase;
  }
  if (goal.liveProgress?.phase) return goal.liveProgress.phase;
  return "—";
}

export function buildRunActivityDisplay(activity: RunActivitySummary): ToolDisplayPayload {
  return {
    kind: "run_activity",
    title: `Run ${activity.runId} (${activity.phase})`,
    data: activity,
  };
}

export function buildGoalsTableDisplay(
  entries: Array<{
    goal: GoalRecord;
    reviews: GoalReviewVerdict[];
    openEscalations: EscalationRecord[];
    tasks?: TaskRecord[];
    activity?: RunActivitySummary | null;
  }>,
): ToolDisplayPayload {
  const goals: GoalsTableRow[] = entries.map(({ goal, reviews, openEscalations, tasks, activity }) => {
    const review = latestReview(reviews);
    const results = criterionResults(goal, review);
    const hasResults = review !== null || (goal.liveProgress?.criteriaResults?.length ?? 0) > 0;
    return {
      id: goal.id,
      version: goal.version,
      status: goal.status,
      title: goal.title,
      linkedRunId: goal.linkedRunId,
      reviewCycle: goal.reviewCycle,
      maxReviewCycles: null,
      verificationLabel: deriveGoalVerificationLabel(goal, tasks ?? [], activity),
      criteriaTotal: goal.criteria.length,
      criteriaPassed: hasResults ? [...results.values()].filter((v) => v === "pass").length : null,
      openEscalations: openEscalations.length,
    };
  });
  return { kind: "goals_table", title: "Goals", data: { goals } satisfies GoalsTableData };
}

export function buildGoalDetailDisplay(input: {
  goal: GoalRecord;
  reviews: GoalReviewVerdict[];
  tasks: TaskRecord[];
  openEscalations: EscalationRecord[];
  activity?: RunActivitySummary | null;
}): ToolDisplayPayload {
  const { goal, activity } = input;
  const review = latestReview(input.reviews);
  const results = criterionResults(goal, review);
  const taskActivityById = new Map((activity?.tasks ?? []).map((t) => [t.id, t]));
  const data: GoalDetailData = {
    goal: {
      id: goal.id,
      version: goal.version,
      status: goal.status,
      title: goal.title,
      description: goal.description,
      linkedRunId: goal.linkedRunId,
      reviewCycle: goal.reviewCycle,
      maxReviewCycles: null,
      verificationLabel: deriveGoalVerificationLabel(goal, input.tasks, activity),
      liveSummary: goal.liveProgress?.summary ?? null,
      nonGoals: goal.nonGoals,
    },
    criteria: goal.criteria.map((c) => ({
      id: c.id,
      kind: c.kind,
      description: c.description,
      ...(c.command ? { command: c.command } : {}),
      required: c.required,
      lastResult: results.get(c.id) ?? null,
    })),
    tasks: input.tasks.map((t) => {
      const snap = taskActivityById.get(t.id);
      return {
        id: t.id,
        title: t.title,
        status: t.status,
        tier: t.tier,
        kind: t.kind,
        model: t.model,
        effort: t.effort,
        currentAction: snap?.currentAction ?? null,
        toolCalls: snap?.toolCalls ?? 0,
        filesEdited: snap?.filesEdited ?? 0,
        elapsedMs: snap?.elapsedMs ?? 0,
      };
    }),
    activity: activity
      ? {
          phase: activity.phase,
          filesEdited: activity.filesEdited,
          elapsedMs: activity.elapsedMs,
          modelsUsed: activity.modelsUsed,
        }
      : null,
    openEscalations: input.openEscalations.map((e) => ({ id: e.id, taskId: e.fromTaskId, summary: e.title })),
    lastReview: review ? { cycle: review.cycle, verdict: review.verdict, summary: review.summary } : null,
  };
  return { kind: "goal_detail", title: `Goal ${goal.id} v${goal.version}`, data };
}

/** Short plain-text summary stored as a tool_output turn's content (and shown to the model in place of the display). */
export function summarizeToolDisplay(toolName: string, display: ToolDisplayPayload): string {
  if (display.kind === "goals_table") {
    const n = (display.data as GoalsTableData).goals.length;
    return `${toolName} → Goals table (${n} goal${n === 1 ? "" : "s"})`;
  }
  if (display.kind === "goal_detail") {
    const g = (display.data as GoalDetailData).goal;
    return `${toolName} → Goal ${g.id} v${g.version} detail (${g.status})`;
  }
  return `${toolName} → ${display.title ?? display.kind}`;
}
