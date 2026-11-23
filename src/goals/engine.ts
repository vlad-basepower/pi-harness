import { createHash } from "node:crypto";
import { lstatSync, mkdirSync, realpathSync } from "node:fs";
import { join, resolve } from "node:path";
import type { Database } from "bun:sqlite";
import type { GoalsConfig } from "../config.js";
import { friendlyIdCandidates, nextFriendlyId } from "../control-plane/ids.js";
import { type ResolvedGateEnv, resolveGateEnv, runGateCommand } from "../tools/gate-env.js";
import { runGit } from "../worktree/manager.js";
import {
  type GoalLiveProgress,
  type GoalLiveProgressPatch,
  type GoalProgressSource,
  mergeGoalLiveProgress,
  stripStaleLiveProgress,
} from "./live-progress.js";

export {
  type GoalLiveProgress,
  type GoalLiveProgressPatch,
  type GoalProgressSource,
  isAuthoritativeSource,
  isBlockedPhase,
  isCurrentClaim,
  mergeGoalLiveProgress,
  stripStaleLiveProgress,
} from "./live-progress.js";

function canonicalRepoPath(p: string): string {
  try {
    return realpathSync(p);
  } catch {
    return resolve(p);
  }
}

const GOAL_ID_SEGMENT = /^[a-zA-Z0-9._-]+$/;

/** Per-goal evidence directory `<repo>/.harness/goals/<goalId>`; rejects ids that are not a single safe path segment. */
export function goalEvidenceDir(repoRoot: string, goalId: string): string {
  if (!GOAL_ID_SEGMENT.test(goalId) || goalId === "." || goalId === "..") {
    throw new Error(`Invalid goal id for evidence directory: ${JSON.stringify(goalId)}`);
  }
  return join(canonicalRepoPath(repoRoot), ".harness", "goals", goalId);
}

/** Creates the goal evidence directory segment by segment, refusing symlinks or files; returns null on failure. */
export function ensureGoalEvidenceDir(repoRoot: string, goalId: string): string | null {
  try {
    const target = goalEvidenceDir(repoRoot, goalId);
    let p = canonicalRepoPath(repoRoot);
    for (const segment of [".harness", "goals", goalId]) {
      p = join(p, segment);
      try {
        mkdirSync(p);
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code !== "EEXIST") throw err;
      }
      if (!lstatSync(p).isDirectory()) return null;
    }
    return target;
  } catch {
    return null;
  }
}

export type GoalStatus =
  | "draft"
  | "executing"
  | "verifying"
  | "reviewing"
  | "awaiting_human"
  | "achieved"
  | "paused"
  | "cancelled";

export type CriterionKind = "deterministic" | "behavioral" | "architectural";

/**
 * Agent-selected verification depth for a Goal:
 * - `"auto"` (default): runs deterministic checks; spawns an independent LLM reviewer only if required
 *   `behavioral` or `architectural` criteria exist.
 * - `"none"`: skips automated gates and independent LLM review (agent decides no separate review pass is needed).
 * - `"deterministic"`: runs deterministic shell checks only ($0 LLM cost) and approves immediately on pass.
 * - `"independent_review"`: always runs deterministic gates followed by an independent read-only LLM Done-Reviewer.
 */
export type VerificationMode = "auto" | "none" | "deterministic" | "independent_review";

export interface GoalCriterion {
  id: string;
  description: string;
  kind: CriterionKind;
  /** Shell command for `deterministic` criteria (executed at $0 LLM cost). Runs in the verification checkout, which is an isolated worktree of the target branch when one can be created, so it may not be `repoRoot`. */
  command?: string;
  required: boolean;
}

/** Partial criterion update merged by `id`; omitted fields keep their current values. */
export type GoalCriterionPatch = Partial<Omit<GoalCriterion, "id">> & { id: string };

const CRITERION_KINDS: readonly CriterionKind[] = ["deterministic", "behavioral", "architectural"];

/** Validates untrusted criterion patches (tool or RPC input), dropping unknown keys. */
export function parseGoalCriterionPatches(raw: unknown, field = "updateCriteria"): GoalCriterionPatch[] | undefined {
  if (raw === undefined || raw === null) return undefined;
  if (!Array.isArray(raw)) throw new Error(`${field} must be an array`);
  return raw.map((entry: unknown, i) => {
    if (typeof entry !== "object" || entry === null || Array.isArray(entry)) {
      throw new Error(`${field}[${i}].id must be a non-empty string`);
    }
    const e = entry as Record<string, unknown>;
    if (typeof e.id !== "string" || !e.id.trim()) throw new Error(`${field}[${i}].id must be a non-empty string`);
    const patch: GoalCriterionPatch = { id: e.id };
    if (e.description !== undefined) {
      if (typeof e.description !== "string") throw new Error(`${field}[${i}].description must be a string`);
      patch.description = e.description;
    }
    if (e.kind !== undefined) {
      if (typeof e.kind !== "string" || !CRITERION_KINDS.includes(e.kind as CriterionKind)) {
        throw new Error(`${field}[${i}].kind must be one of deterministic, behavioral, architectural`);
      }
      patch.kind = e.kind as CriterionKind;
    }
    if (e.command !== undefined) {
      if (typeof e.command !== "string") throw new Error(`${field}[${i}].command must be a string`);
      patch.command = e.command;
    }
    if (e.required !== undefined) {
      if (typeof e.required !== "boolean") throw new Error(`${field}[${i}].required must be a boolean`);
      patch.required = e.required;
    }
    return patch;
  });
}

export interface GoalBudgets {
  maxReviewCycles: number;
  maxNoProgressCycles: number;
  maxWallClockMin?: number;
  maxApiEquivUsd?: number;
}

export interface GoalRecord {
  id: string;
  chatSessionId: string | null;
  repoRoot: string;
  version: number;
  title: string;
  description: string;
  criteria: GoalCriterion[];
  nonGoals: string[];
  budgets: GoalBudgets;
  verificationMode?: VerificationMode;
  liveProgress?: GoalLiveProgress | null;
  status: GoalStatus;
  linkedRunId: string | null;
  reviewCycle: number;
  noProgressStreak: number;
  lastProgressFingerprint: string | null;
  lastReviewSummary: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface GoalAmendmentDelta {
  fromVersion: number;
  toVersion: number;
  rationale: string;
  addedCriteria: GoalCriterion[];
  removedCriteriaIds: string[];
  modifiedCriteria: GoalCriterion[];
  descriptionChanged: boolean;
  nonGoalsChanged: boolean;
  /** Absent on deltas recorded before append/replace tracking existed. */
  descriptionMode?: "append" | "replace";
  appendedDescription?: string;
  previousDescriptionLength?: number;
  descriptionLength?: number;
}

export interface GoalRevisionRecord {
  goalId: string;
  version: number;
  title: string;
  description: string;
  criteria: GoalCriterion[];
  nonGoals: string[];
  rationale: string;
  delta: GoalAmendmentDelta | null;
  createdAt: number;
}

export interface CriterionEvaluation {
  criterionId: string;
  passed: boolean;
  evidence: string;
  remediation?: string;
}

/** What produced a goal_reviews row. Legacy rows read as "review". */
export type GoalReviewSource = "review" | "deterministic" | "none" | "coordinator_failed" | "director_accept_override";

/** Audit record stored with a `director_accept_override` review. */
export interface GoalAcceptOverrideAudit {
  acceptedBy: "director" | "user";
  chatSessionId: string | null;
  reason: string;
  evidence: Array<{ input: string; canonicalPath: string; kind: "file" | "directory"; sizeBytes?: number; mtimeMs: number }>;
  overrideDeterministic: boolean;
  failedDeterministicCriteria: string[];
  prevStatus: GoalStatus;
  goalVersion: number;
  verifyRef: string | null;
  acceptedAt: number;
}

export interface GoalReviewVerdict {
  id: string;
  goalId: string;
  goalVersion: number;
  cycle: number;
  verdict: "approved" | "changes_requested" | "escalate_human";
  criteriaResults: CriterionEvaluation[];
  blockingIssues: string[];
  summary: string;
  progressFingerprint: string;
  createdAt: number;
  source?: GoalReviewSource;
  audit?: GoalAcceptOverrideAudit | null;
}

export interface DeterministicGateResult {
  allPassed: boolean;
  results: CriterionEvaluation[];
  diffHash: string;
}

export const GOALS_SCHEMA = `
CREATE TABLE IF NOT EXISTS goals (
  id TEXT PRIMARY KEY,
  chat_session_id TEXT,
  repo_root TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  title TEXT NOT NULL,
  description TEXT NOT NULL,
  criteria_json TEXT NOT NULL,
  non_goals_json TEXT NOT NULL DEFAULT '[]',
  budgets_json TEXT NOT NULL,
  verification_mode TEXT NOT NULL DEFAULT 'auto',
  live_progress_json TEXT,
  status TEXT NOT NULL DEFAULT 'draft',
  linked_run_id TEXT,
  review_cycle INTEGER NOT NULL DEFAULT 0,
  no_progress_streak INTEGER NOT NULL DEFAULT 0,
  last_progress_fingerprint TEXT,
  last_review_summary TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_goals_repo_status ON goals(repo_root, status);

CREATE TABLE IF NOT EXISTS goal_revisions (
  goal_id TEXT NOT NULL REFERENCES goals(id),
  version INTEGER NOT NULL,
  title TEXT NOT NULL,
  description TEXT NOT NULL,
  criteria_json TEXT NOT NULL,
  non_goals_json TEXT NOT NULL,
  rationale TEXT NOT NULL,
  delta_json TEXT,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (goal_id, version)
);

CREATE TABLE IF NOT EXISTS goal_reviews (
  id TEXT PRIMARY KEY,
  goal_id TEXT NOT NULL REFERENCES goals(id),
  goal_version INTEGER NOT NULL,
  cycle INTEGER NOT NULL,
  verdict TEXT NOT NULL,
  criteria_results_json TEXT NOT NULL,
  blocking_issues_json TEXT NOT NULL,
  summary TEXT NOT NULL,
  progress_fingerprint TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  source TEXT NOT NULL DEFAULT 'review',
  audit_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_goal_reviews_goal ON goal_reviews(goal_id, cycle);
`;

function goalId(db: Database): string {
  return nextFriendlyId(db, "goal", "goals");
}

function reviewId(db: Database): string {
  return nextFriendlyId(db, "rev", "goal_reviews");
}

/**
 * Dynamic "Run-Until-Done" Goal Engine:
 * - Versioned Goal Contracts (`v1 -> v2 -> ...`) with explicit deterministic & behavioral criteria
 * - $0-token deterministic verification gates (`check_cmds`)
 * - Strict mechanical validation of Independent Skeptical Done-Review verdicts (blocks premature approval!)
 * - Anti-spin circuit breakers (`maxReviewCycles` + `noProgressStreak` diff-hash/failure fingerprint)
 */
export function shouldRunIndependentReview(goal: GoalRecord, overrideMode?: VerificationMode): boolean {
  const mode: VerificationMode = overrideMode ?? goal.verificationMode ?? "auto";
  if (mode === "none" || mode === "deterministic") return false;
  if (mode === "independent_review") return true;
  // Auto: only burn an independent LLM review when required behavioral/architectural criteria exist.
  return goal.criteria.some((c) => c.required && c.kind !== "deterministic");
}

/** The operator gate keys present in `config`, omitting unset ones. */
function gateKeys(config?: Partial<GoalsConfig>): Pick<GoalsConfig, "gateShell" | "gateEnv"> {
  return {
    ...(config?.gateShell !== undefined ? { gateShell: config.gateShell } : {}),
    ...(config?.gateEnv !== undefined ? { gateEnv: config.gateEnv } : {}),
  };
}

export class GoalEngine {
  private cfg: GoalsConfig;
  private readonly stateDir?: string;
  private readonly onGoalSettled?: (goal: GoalRecord) => void;
  private readonly onGoalAmended?: (goal: GoalRecord, delta: GoalAmendmentDelta) => string[];

  constructor(
    private readonly db: Database,
    private readonly onReviewInvalidated?: (goalId: string) => void,
    config?: Partial<GoalsConfig>,
    private readonly onReconciled?: () => void,
    opts?: {
      stateDir?: string;
      /** Called after a goal becomes achieved or cancelled; resolves escalations on its linked run. */
      onGoalSettled?: (goal: GoalRecord) => void;
      /** Called after an amendment; supersedes verification_failed escalations and returns their ids. */
      onGoalAmended?: (goal: GoalRecord, delta: GoalAmendmentDelta) => string[];
    },
  ) {
    this.stateDir = opts?.stateDir;
    this.onGoalSettled = opts?.onGoalSettled;
    this.onGoalAmended = opts?.onGoalAmended;
    this.cfg = {
      defaultMaxReviewCycles: config?.defaultMaxReviewCycles ?? 4,
      defaultMaxNoProgressCycles: config?.defaultMaxNoProgressCycles ?? 2,
      gateTimeoutMs: config?.gateTimeoutMs ?? 180_000,
      autoVerifyOnSettle: config?.autoVerifyOnSettle ?? true,
      maxConcurrentGoals: config?.maxConcurrentGoals ?? 4,
      ...gateKeys(config),
    };
    this.db.exec(GOALS_SCHEMA);
    this.migrateGoals();
  }

  updateConfig(config?: Partial<GoalsConfig>): void {
    this.cfg = {
      defaultMaxReviewCycles: config?.defaultMaxReviewCycles ?? this.cfg.defaultMaxReviewCycles,
      defaultMaxNoProgressCycles: config?.defaultMaxNoProgressCycles ?? this.cfg.defaultMaxNoProgressCycles,
      gateTimeoutMs: config?.gateTimeoutMs ?? this.cfg.gateTimeoutMs,
      autoVerifyOnSettle: config?.autoVerifyOnSettle ?? this.cfg.autoVerifyOnSettle,
      maxConcurrentGoals: config?.maxConcurrentGoals ?? this.cfg.maxConcurrentGoals ?? 4,
      // Gate keys are taken verbatim (validated when gates run) so removing them from config clears them.
      ...gateKeys(config ?? this.cfg),
    };
  }

  private migrateGoals(): void {
    const cols = new Set(
      (this.db.prepare("PRAGMA table_info(goals)").all() as Array<{ name: string }>).map((c) => c.name),
    );
    if (!cols.has("verification_mode")) {
      this.db.exec("ALTER TABLE goals ADD COLUMN verification_mode TEXT NOT NULL DEFAULT 'auto'");
    }
    if (!cols.has("live_progress_json")) {
      this.db.exec("ALTER TABLE goals ADD COLUMN live_progress_json TEXT");
    }
    const reviewCols = new Set(
      (this.db.prepare("PRAGMA table_info(goal_reviews)").all() as Array<{ name: string }>).map((c) => c.name),
    );
    if (!reviewCols.has("source")) {
      this.db.exec("ALTER TABLE goal_reviews ADD COLUMN source TEXT NOT NULL DEFAULT 'review'");
    }
    if (!reviewCols.has("audit_json")) {
      this.db.exec("ALTER TABLE goal_reviews ADD COLUMN audit_json TEXT");
    }
  }

  private mapGoal(r: Record<string, unknown>): GoalRecord {
    const rawMode = typeof r.verification_mode === "string" ? r.verification_mode : "auto";
    const verificationMode: VerificationMode =
      rawMode === "none" || rawMode === "deterministic" || rawMode === "independent_review" ? rawMode : "auto";
    let liveProgress: GoalLiveProgress | null = null;
    if (typeof r.live_progress_json === "string" && r.live_progress_json.trim()) {
      try {
        liveProgress = JSON.parse(r.live_progress_json) as GoalLiveProgress;
      } catch {
        liveProgress = null;
      }
    }
    return {
      id: String(r.id),
      chatSessionId: r.chat_session_id ? String(r.chat_session_id) : null,
      repoRoot: String(r.repo_root),
      version: Number(r.version),
      title: String(r.title),
      description: String(r.description),
      criteria: JSON.parse(String(r.criteria_json)) as GoalCriterion[],
      nonGoals: JSON.parse(String(r.non_goals_json)) as string[],
      budgets: JSON.parse(String(r.budgets_json)) as GoalBudgets,
      verificationMode,
      liveProgress,
      status: String(r.status) as GoalStatus,
      linkedRunId: r.linked_run_id ? String(r.linked_run_id) : null,
      reviewCycle: Number(r.review_cycle),
      noProgressStreak: Number(r.no_progress_streak),
      lastProgressFingerprint: r.last_progress_fingerprint ? String(r.last_progress_fingerprint) : null,
      lastReviewSummary: r.last_review_summary ? String(r.last_review_summary) : null,
      createdAt: Number(r.created_at),
      updatedAt: Number(r.updated_at),
    };
  }

  createGoal(input: {
    repoRoot: string;
    chatSessionId?: string | null;
    title: string;
    description: string;
    criteria: Array<Omit<GoalCriterion, "id" | "required"> & { id?: string; required?: boolean }>;
    nonGoals?: string[];
    budgets?: Partial<GoalBudgets>;
    verificationMode?: VerificationMode;
  }): GoalRecord {
    if (!input.title.trim()) throw new Error("Goal title is required");
    const verificationMode: VerificationMode = input.verificationMode ?? "auto";
    if (input.criteria.length === 0 && verificationMode !== "none") {
      throw new Error("Goal requires at least one acceptance criterion unless verificationMode is 'none'");
    }

    const id = goalId(this.db);
    const now = Date.now();
    const criteria: GoalCriterion[] = input.criteria.map((c, idx) => ({
      id: c.id ?? `c${idx + 1}`,
      description: c.description,
      kind: c.kind,
      command: c.command,
      required: c.required !== false,
    }));
    for (const c of criteria) {
      if (c.kind === "deterministic" && !c.command?.trim()) {
        throw new Error(
          `Deterministic criterion '${c.id}' requires a non-empty shell 'command' (or use kind 'behavioral'/'architectural').`,
        );
      }
    }
    const nonGoals = input.nonGoals ?? [];
    const budgets: GoalBudgets = {
      maxReviewCycles: input.budgets?.maxReviewCycles ?? this.cfg.defaultMaxReviewCycles,
      maxNoProgressCycles: input.budgets?.maxNoProgressCycles ?? this.cfg.defaultMaxNoProgressCycles,
      maxWallClockMin: input.budgets?.maxWallClockMin,
      maxApiEquivUsd: input.budgets?.maxApiEquivUsd,
    };

    this.db
      .prepare(
        `INSERT INTO goals(
          id, chat_session_id, repo_root, version, title, description,
          criteria_json, non_goals_json, budgets_json, verification_mode, status, created_at, updated_at
        ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, 'draft', ?, ?)`,
      )
      .run(
        id,
        input.chatSessionId ?? null,
        canonicalRepoPath(input.repoRoot),
        input.title.trim(),
        input.description.trim(),
        JSON.stringify(criteria),
        JSON.stringify(nonGoals),
        JSON.stringify(budgets),
        verificationMode,
        now,
        now,
      );

    this.db
      .prepare(
        `INSERT INTO goal_revisions(goal_id, version, title, description, criteria_json, non_goals_json, rationale, created_at)
         VALUES (?, 1, ?, ?, ?, ?, 'Initial goal contract', ?)`,
      )
      .run(id, input.title.trim(), input.description.trim(), JSON.stringify(criteria), JSON.stringify(nonGoals), now);

    const created = this.getGoal(id)!;
    ensureGoalEvidenceDir(created.repoRoot, created.id);
    return created;
  }

  getGoal(id: string): GoalRecord | null {
    for (const candidate of friendlyIdCandidates(id, "goal")) {
      const r = this.db.prepare("SELECT * FROM goals WHERE id = ?").get(candidate) as Record<string, unknown> | undefined;
      if (r) return this.mapGoal(r);
    }
    return null;
  }

  listGoals(opts?: { repoRoot?: string; chatSessionId?: string; status?: GoalStatus }): GoalRecord[] {
    let sql = "SELECT * FROM goals WHERE 1=1";
    const params: string[] = [];
    if (opts?.repoRoot) {
      sql += " AND repo_root = ?";
      params.push(canonicalRepoPath(opts.repoRoot));
    }
    if (opts?.chatSessionId) {
      sql += " AND chat_session_id = ?";
      params.push(opts.chatSessionId);
    }
    if (opts?.status) {
      sql += " AND status = ?";
      params.push(opts.status);
    }
    sql += " ORDER BY updated_at DESC";
    return (this.db.prepare(sql).all(...params) as Record<string, unknown>[]).map((r) => this.mapGoal(r));
  }

  linkRunAndStart(goalIdStr: string, runId: string): GoalRecord {
    const g = this.getGoal(goalIdStr);
    if (!g) throw new Error(`Unknown goal ${goalIdStr}`);
    this.db
      .prepare("UPDATE goals SET linked_run_id = ?, status = 'executing', updated_at = ? WHERE id = ?")
      .run(runId, Date.now(), g.id);
    this.clearQueuedProgress(g.id);
    this.clearStaleVerificationStatus(g.id, { clearBlockedPhase: true });
    ensureGoalEvidenceDir(g.repoRoot, g.id);
    return this.getGoal(g.id)!;
  }

  /**
   * Removes verification claims (and, with `clearBlockedPhase`, a blocked phase) from live progress.
   * Writes only when something changed and never touches `status`.
   */
  clearStaleVerificationStatus(goalIdStr: string, opts?: { clearBlockedPhase?: boolean }): GoalRecord {
    const g = this.getGoal(goalIdStr);
    if (!g) throw new Error(`Unknown goal ${goalIdStr}`);
    const { next, changed } = stripStaleLiveProgress(g.liveProgress, opts);
    if (!changed) return g;
    this.db
      .prepare("UPDATE goals SET live_progress_json = ?, updated_at = ? WHERE id = ?")
      .run(next ? JSON.stringify(next) : null, Date.now(), g.id);
    return this.getGoal(g.id)!;
  }

  /** `created_at` of the given goal revision, or null when it does not exist. */
  getLatestRevisionCreatedAt(goalIdStr: string, version: number): number | null {
    const resolvedId = this.getGoal(goalIdStr)?.id ?? goalIdStr;
    const row = this.db
      .prepare("SELECT created_at FROM goal_revisions WHERE goal_id = ? AND version = ?")
      .get(resolvedId, version) as { created_at: number } | undefined;
    return row ? Number(row.created_at) : null;
  }

  /** Drops admission-queue progress (`queued`/`queue_launch_error`) once a goal is no longer waiting for a slot. */
  clearQueuedProgress(goalIdStr: string): void {
    const g = this.getGoal(goalIdStr);
    const lp = g?.liveProgress;
    if (!g || !lp || (lp.phase !== "queued" && lp.phase !== "queue_launch_error")) return;
    const { phase: _phase, summary: _summary, ...rest } = lp;
    const next = !rest.verificationStatus && !rest.criteriaResults ? null : JSON.stringify(rest);
    this.db.prepare("UPDATE goals SET live_progress_json = ?, updated_at = ? WHERE id = ?").run(next, Date.now(), g.id);
  }

  /** Persists live model-reported progress, verification status, and criterion checks onto the Goal. */
  reportProgress(
    goalIdStr: string,
    input: GoalLiveProgressPatch,
    source?: GoalProgressSource,
  ): GoalRecord {
    const g = this.getGoal(goalIdStr);
    if (!g) throw new Error(`Unknown goal ${goalIdStr}`);
    const now = Date.now();
    const next = mergeGoalLiveProgress(g.liveProgress, input, {
      goalVersion: g.version,
      runId: g.linkedRunId,
      reviewCycle: g.reviewCycle,
      source: source ?? "director",
      now,
    });
    this.db
      .prepare("UPDATE goals SET live_progress_json = ?, updated_at = ? WHERE id = ?")
      .run(JSON.stringify(next), now, g.id);
    return this.getGoal(g.id)!;
  }

  setStatus(goalIdStr: string, status: GoalStatus, summary?: string): GoalRecord {
    const g = this.getGoal(goalIdStr);
    if (!g) throw new Error(`Unknown goal ${goalIdStr}`);
    this.db
      .prepare("UPDATE goals SET status = ?, last_review_summary = COALESCE(?, last_review_summary), updated_at = ? WHERE id = ?")
      .run(status, summary ?? null, Date.now(), g.id);
    if (status === "paused" || status === "cancelled") {
      this.clearQueuedProgress(g.id);
      this.onReviewInvalidated?.(g.id);
    }
    const updated = this.getGoal(g.id)!;
    if (status === "achieved" || status === "cancelled") this.settleEscalations(updated, status);
    return updated;
  }

  /** Resolves escalations on a settled goal's linked run; a failure here never fails the committed transition. */
  private settleEscalations(goal: GoalRecord, status: "achieved" | "cancelled"): void {
    if (!goal.linkedRunId) return;
    if (this.onGoalSettled) {
      try {
        this.onGoalSettled(goal);
      } catch {
        // Audit hook failures must not undo the status change.
      }
      return;
    }
    this.resolveLinkedEscalationsFallback(goal.linkedRunId, `Auto-resolved: goal ${goal.id} ${status}`);
  }

  /** Direct SQL fallback when no hook is wired; the escalations table is absent in standalone engines. */
  private resolveLinkedEscalationsFallback(runId: string, resolution: string, category?: string): string[] {
    try {
      const categorySql = category !== undefined ? " AND category = ?" : "";
      const params = category !== undefined ? [runId, category] : [runId];
      const ids = (
        this.db
          .prepare(`SELECT id FROM escalations WHERE run_id = ? AND status IN ('open', 'acked')${categorySql}`)
          .all(...params) as Array<{ id: string }>
      ).map((r) => r.id);
      if (ids.length === 0) return [];
      const now = Date.now();
      const update = this.db.prepare(
        "UPDATE escalations SET status = 'resolved', resolution = ?, acked_at = ? WHERE id = ?",
      );
      for (const id of ids) update.run(resolution, now, id);
      return ids;
    } catch {
      return [];
    }
  }

  /**
   * Dynamic Mid-Flight Goal Mutation (`vN -> vN+1`):
   * Computes an exact structured Amendment Delta (`added`/`removed`/`modified`),
   * resets stale `reviewing`/`achieved`/`awaiting_human` states back to `executing`
   * (preserving `draft` if not yet committed), and resets `noProgressStreak` because
   * requirements genuinely changed.
   */
  amendGoal(
    goalIdStr: string,
    patch: {
      title?: string;
      /** Full replacement of the description. */
      description?: string;
      /** Text appended to the existing description after a blank line. */
      appendDescription?: string;
      /** Required to replace the description with shorter text. */
      confirmReplaceDescription?: boolean;
      addCriteria?: Array<Omit<GoalCriterion, "id" | "required"> & { id?: string; required?: boolean }>;
      removeCriteriaIds?: string[];
      updateCriteria?: GoalCriterionPatch[];
      nonGoals?: string[];
      rationale: string;
    },
  ): { goal: GoalRecord; delta: GoalAmendmentDelta; supersededEscalationIds: string[] } {
    const cur = this.getGoal(goalIdStr);
    if (!cur) throw new Error(`Unknown goal ${goalIdStr}`);
    if (cur.status === "cancelled") throw new Error("Cannot amend a cancelled goal");

    const removedSet = new Set(patch.removeCriteriaIds ?? []);
    let nextCriteria = cur.criteria.filter((c) => !removedSet.has(c.id));

    const modifiedCriteria: GoalCriterion[] = [];
    for (const upd of patch.updateCriteria ?? []) {
      const idx = nextCriteria.findIndex((c) => c.id === upd.id);
      if (idx >= 0) {
        const existing = nextCriteria[idx]!;
        // Partial merge: omitted fields preserve existing values so updating `command` never wipes `kind`/`description`/`required`!
        const merged: GoalCriterion = {
          id: existing.id,
          description: upd.description !== undefined ? upd.description : existing.description,
          kind: upd.kind !== undefined ? upd.kind : existing.kind,
          command: upd.command !== undefined ? upd.command : existing.command,
          required: upd.required !== undefined ? upd.required : existing.required,
        };
        nextCriteria[idx] = merged;
        modifiedCriteria.push(merged);
      } else {
        throw new Error(
          `Unknown criterion id '${upd.id}' in updateCriteria (current: ${nextCriteria.map((c) => c.id).join(", ")})`,
        );
      }
    }

    const addedCriteria: GoalCriterion[] = (patch.addCriteria ?? []).map((c, idx) => ({
      id: c.id ?? `c${cur.version + 1}_${idx + 1}`,
      description: c.description,
      kind: c.kind,
      command: c.command,
      required: c.required !== false,
    }));
    nextCriteria = [...nextCriteria, ...addedCriteria];

    if (nextCriteria.length === 0 && (cur.verificationMode !== "none" || cur.criteria.length > 0)) {
      throw new Error("Goal amendment cannot remove all acceptance criteria");
    }
    for (const c of nextCriteria) {
      if (c.kind === "deterministic" && !c.command?.trim()) {
        throw new Error(
          `Deterministic criterion '${c.id}' requires a non-empty shell 'command' (or change kind to 'behavioral'/'architectural').`,
        );
      }
    }

    const nextVersion = cur.version + 1;
    const nextTitle = patch.title?.trim() ?? cur.title;
    if (patch.description !== undefined && patch.appendDescription !== undefined) {
      throw new Error("Provide either description (full replacement) or appendDescription (extend), not both.");
    }
    let nextDesc: string;
    let descriptionMode: "append" | "replace" | undefined;
    let appendedDescription: string | undefined;
    if (patch.appendDescription !== undefined) {
      const appended = patch.appendDescription.trim();
      if (!appended) throw new Error("appendDescription must be a non-empty string.");
      nextDesc = cur.description ? `${cur.description.trimEnd()}\n\n${appended}` : appended;
      descriptionMode = "append";
      appendedDescription = appended;
    } else if (patch.description !== undefined) {
      const replaced = patch.description.trim();
      if (!replaced) {
        throw new Error("description replacement cannot be empty; omit it to keep the current description.");
      }
      if (replaced !== cur.description && replaced.length < cur.description.length && !patch.confirmReplaceDescription) {
        throw new Error(
          `Refusing to replace goal ${cur.id} description (${cur.description.length} chars) with shorter text (${replaced.length} chars). "description" replaces the whole description. Use append_description/appendDescription to add instructions, or pass confirm_replace_description/confirmReplaceDescription: true.`,
        );
      }
      nextDesc = replaced;
      if (nextDesc !== cur.description) descriptionMode = "replace";
    } else {
      nextDesc = cur.description;
    }
    const nextNonGoals = patch.nonGoals ?? cur.nonGoals;
    const nextStatus: GoalStatus =
      cur.status === "draft" || cur.status === "paused" ? cur.status : "executing";

    const delta: GoalAmendmentDelta = {
      fromVersion: cur.version,
      toVersion: nextVersion,
      rationale: patch.rationale,
      addedCriteria,
      removedCriteriaIds: [...removedSet],
      modifiedCriteria,
      descriptionChanged: nextDesc !== cur.description,
      nonGoalsChanged: JSON.stringify(nextNonGoals) !== JSON.stringify(cur.nonGoals),
      ...(descriptionMode ? { descriptionMode } : {}),
      ...(appendedDescription !== undefined ? { appendedDescription } : {}),
    };
    if (delta.descriptionChanged) {
      delta.previousDescriptionLength = cur.description.length;
      delta.descriptionLength = nextDesc.length;
    }

    const now = Date.now();
    const clearedProgress = stripStaleLiveProgress(cur.liveProgress, { clearBlockedPhase: true });
    const nextLiveProgressJson = clearedProgress.changed
      ? clearedProgress.next
        ? JSON.stringify(clearedProgress.next)
        : null
      : cur.liveProgress
        ? JSON.stringify(cur.liveProgress)
        : null;
    this.db
      .prepare(
        `UPDATE goals
         SET version = ?, title = ?, description = ?, criteria_json = ?, non_goals_json = ?,
             status = ?, review_cycle = 0, no_progress_streak = 0, last_progress_fingerprint = NULL,
             live_progress_json = ?, updated_at = ?
         WHERE id = ?`,
      )
      .run(
        nextVersion,
        nextTitle,
        nextDesc,
        JSON.stringify(nextCriteria),
        JSON.stringify(nextNonGoals),
        nextStatus,
        nextLiveProgressJson,
        now,
        cur.id,
      );

    this.db
      .prepare(
        `INSERT INTO goal_revisions(goal_id, version, title, description, criteria_json, non_goals_json, rationale, delta_json, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        cur.id,
        nextVersion,
        nextTitle,
        nextDesc,
        JSON.stringify(nextCriteria),
        JSON.stringify(nextNonGoals),
        patch.rationale,
        JSON.stringify(delta),
        now,
      );

    // An amendment disposes of vN's verification state only: supersede verification_failed escalations, keep the rest.
    const updatedGoal = this.getGoal(cur.id)!;
    let supersededEscalationIds: string[] = [];
    if (updatedGoal.linkedRunId) {
      if (this.onGoalAmended) {
        try {
          supersededEscalationIds = this.onGoalAmended(updatedGoal, delta);
        } catch {
          supersededEscalationIds = [];
        }
      } else {
        supersededEscalationIds = this.resolveLinkedEscalationsFallback(
          updatedGoal.linkedRunId,
          `Superseded by goal ${updatedGoal.id} amendment v${delta.fromVersion} -> v${delta.toVersion}: ${delta.rationale.slice(0, 200)}`,
          "verification_failed",
        );
      }
    }

    this.onReviewInvalidated?.(cur.id);
    return { goal: updatedGoal, delta, supersededEscalationIds };
  }

  /**
   * Reconciles goals stranded in `verifying`/`reviewing` or `executing` with settled, missing, or dead runs after a
   * daemon restart. A completed run is not acceptance, so those goals wait for human review instead of `achieved`.
   */
  reconcileStaleGoalsOnStartup(): number {
    const now = Date.now();
    const res1 = this.db
      .prepare(
        `UPDATE goals
         SET status = 'awaiting_human',
             last_review_summary = COALESCE(last_review_summary, 'Interrupted during verification by daemon restart'),
             updated_at = ?
         WHERE status IN ('verifying', 'reviewing')`,
      )
      .run(now);
    let res2Changes = 0;
    try {
      res2Changes += this.db
        .prepare(
          `UPDATE goals
           SET status = 'awaiting_human',
               last_review_summary = COALESCE(last_review_summary, 'Linked run completed before goal verification; review or accept to finish.'),
               updated_at = ?
           WHERE status = 'executing'
             AND linked_run_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM runs r WHERE r.id = goals.linked_run_id AND r.status = 'completed')`,
        )
        .run(now).changes;
      const res2 = this.db
        .prepare(
          `UPDATE goals
           SET status = 'awaiting_human',
               last_review_summary = COALESCE(last_review_summary, 'Linked run ended before goal verification; resume or review.'),
               updated_at = ?
           WHERE status = 'executing'
             AND (linked_run_id IS NULL
               OR NOT EXISTS (SELECT 1 FROM runs r WHERE r.id = goals.linked_run_id)
               OR EXISTS (SELECT 1 FROM runs r WHERE r.id = goals.linked_run_id AND r.status IN ('failed', 'stopped')))`,
        )
        .run(now);
      res2Changes += res2.changes;
    } catch {
      // runs table may not exist in standalone GoalEngine unit tests
    }
    this.onReconciled?.();
    return res1.changes + res2Changes;
  }

  /** Executes all `deterministic` criteria commands at $0 LLM cost and hashes repo diff for progress tracking. */
  async runDeterministicGates(
    goalIdStr: string,
    baseSha?: string,
    signal?: AbortSignal,
    opts?: { cwd?: string; targetRef?: string },
  ): Promise<DeterministicGateResult> {
    const goal = this.getGoal(goalIdStr);
    if (!goal) throw new Error(`Unknown goal ${goalIdStr}`);
    // An already-stopped review or acceptance never writes "verifying" over a newer pause or cancel.
    if (signal?.aborted) {
      throw signal.reason instanceof Error ? signal.reason : new Error(`Verification of ${goal.id} was interrupted`);
    }
    this.setStatus(goal.id, "verifying");
    // A rejected (e.g. symlinked) evidence directory is never advertised to gate commands.
    const verifiedEvDir = ensureGoalEvidenceDir(goal.repoRoot, goal.id);

    const gateCwd = opts?.cwd ?? goal.repoRoot;
    const repoRootEnv = canonicalRepoPath(goal.repoRoot);
    const worktreeEnv = canonicalRepoPath(gateCwd);
    const cwdNote =
      worktreeEnv !== repoRootEnv
        ? "\n[gate-cwd] isolated checkout of the target commit; untracked and gitignored files are absent (main checkout: $HARNESS_REPO_ROOT)"
        : "";
    const targetRef = opts?.targetRef ?? "HEAD";
    const results: CriterionEvaluation[] = [];
    let allPassed = true;
    let resolved: ResolvedGateEnv | undefined;

    for (const c of goal.criteria) {
      if (c.kind !== "deterministic") continue;
      if (!c.command?.trim()) {
        if (c.required) allPassed = false;
        results.push({
          criterionId: c.id,
          passed: false,
          evidence: `Deterministic criterion '${c.id}' has no shell command configured.`,
          remediation: "Amend the goal to provide a runnable deterministic command or change criterion kind.",
        });
        continue;
      }
      try {
        resolved ??= await resolveGateEnv({
          cwd: gateCwd,
          config: { gateShell: this.cfg.gateShell, gateEnv: this.cfg.gateEnv, stateDir: this.stateDir },
          extraEnv: {
            HARNESS_REPO_ROOT: repoRootEnv,
            HARNESS_WORKTREE_PATH: worktreeEnv,
            ...(verifiedEvDir !== null ? { HARNESS_GOAL_EVIDENCE_DIR: verifiedEvDir } : {}),
            HARNESS_GOAL_ID: goal.id,
          },
          signal,
        });
        const { stdout, stderr } = await runGateCommand(c.command, {
          cwd: gateCwd,
          resolved,
          timeoutMs: this.cfg.gateTimeoutMs,
          maxOutputBytes: 8 * 1024 * 1024,
          signal,
        });
        results.push({
          criterionId: c.id,
          passed: true,
          evidence: `$ ${c.command} -> exit 0${stdout.trim() ? `: ${stdout.trim().slice(-300)}` : stderr.trim() ? `: ${stderr.trim().slice(-200)}` : ""}`,
        });
      } catch (err: unknown) {
        signal?.throwIfAborted();
        const message = err instanceof Error ? err.message : String(err);
        if (c.required) allPassed = false;
        results.push({
          criterionId: c.id,
          passed: false,
          evidence: `$ ${c.command} -> FAILED\n${message.slice(-600)}${resolved ? `\n[gate-env] ${resolved.provenance}${cwdNote}` : ""}`,
          remediation: `Fix failures in deterministic check: ${c.command}`,
        });
      }
    }

    let diffText = "";
    try {
      const args = baseSha ? ["diff", `${baseSha}..${targetRef}`] : ["rev-parse", `${targetRef}^{tree}`];
      const committed = (await runGit(gateCwd, args)).stdout;
      const dirty = (await runGit(gateCwd, ["status", "--porcelain"])).stdout;
      diffText = `${committed}\n---WORKTREE---\n${dirty}`;
    } catch {
      diffText = `git-unavailable:${goal.id}:v${goal.version}`;
    }
    const diffHash = createHash("sha256").update(diffText).digest("hex").slice(0, 16);

    return { allPassed, results, diffHash };
  }

  /**
   * Records an Independent Done-Review verdict with **strict mechanical validation**
   * and **anti-spin circuit breakers**:
   *  - Mechanically rejects `verdict: "approved"` if ANY `required` criterion is missing, `passed: false`, or lacks evidence!
   *  - Computes progress fingerprint `sha256(diffHash + failedIds)`: trips circuit breaker to `awaiting_human`
   *    when `noProgressStreak >= maxNoProgressCycles` OR `reviewCycle >= maxReviewCycles`.
   */
  recordReviewVerdict(
    goalIdStr: string,
    input: {
      verdict: "approved" | "changes_requested" | "escalate_human";
      criteriaResults: CriterionEvaluation[];
      blockingIssues?: string[];
      summary: string;
      diffHash: string;
      spentUsd?: number;
      /** What produced this verdict; defaults to "review". */
      source?: GoalReviewSource;
      audit?: GoalAcceptOverrideAudit | null;
    },
  ): { goal: GoalRecord; review: GoalReviewVerdict; circuitBreakerTripped?: string } {
    const goal = this.getGoal(goalIdStr);
    if (!goal) throw new Error(`Unknown goal ${goalIdStr}`);

    const byId = new Map(input.criteriaResults.map((r) => [r.criterionId, r]));
    const unmetRequired: string[] = [];
    for (const c of goal.criteria) {
      if (!c.required) continue;
      const ev = byId.get(c.id);
      if (!ev || ev.passed !== true || typeof ev.evidence !== "string" || !ev.evidence.trim()) {
        unmetRequired.push(c.id);
      }
    }

    // Mechanical anti-sycophancy guard: cannot approve while required criteria are unmet/unevidenced,
    // any criterion explicitly failed, or non-empty blockingIssues were reported!
    let effectiveVerdict = input.verdict;
    const blockingIssues = (input.blockingIssues ?? []).filter((b) => typeof b === "string" && b.trim().length > 0);
    const anyReportedFailed = input.criteriaResults.some((r) => r.passed !== true);
    if (effectiveVerdict === "approved" && unmetRequired.length > 0) {
      effectiveVerdict = "changes_requested";
      blockingIssues.push(`Mechanical gate rejected premature approval; unmet/unevidenced required criteria: ${unmetRequired.join(", ")}`);
    } else if (effectiveVerdict === "approved" && (blockingIssues.length > 0 || anyReportedFailed)) {
      effectiveVerdict = "changes_requested";
      blockingIssues.push("Mechanical gate rejected contradictory approval with blocking issues or non-passing criteria.");
    }

    const nextCycle = goal.reviewCycle + 1;
    const failedIds = input.criteriaResults
      .filter((r) => !r.passed)
      .map((r) => r.criterionId)
      .concat(unmetRequired)
      .sort();
    const fingerprint = createHash("sha256")
      .update(`${goal.version}|${input.diffHash}|${[...new Set(failedIds)].join(",")}`)
      .digest("hex")
      .slice(0, 16);

    const sameAsLast = goal.lastProgressFingerprint !== null && goal.lastProgressFingerprint === fingerprint;
    const priorReviews = this.listReviews(goal.id).filter((r) => r.goalVersion === goal.version);
    const seenOscillation =
      effectiveVerdict !== "approved" && priorReviews.some((r) => r.progressFingerprint === fingerprint && !sameAsLast);
    const nextNoProgress = effectiveVerdict === "approved" ? 0 : sameAsLast ? goal.noProgressStreak + 1 : 0;

    let nextStatus: GoalStatus = "executing";
    let circuitBreakerTripped: string | undefined;

    if (effectiveVerdict === "approved") {
      nextStatus = "achieved";
    } else if (effectiveVerdict === "escalate_human") {
      nextStatus = "awaiting_human";
      circuitBreakerTripped = "Reviewer requested human decision";
    } else if (seenOscillation) {
      nextStatus = "awaiting_human";
      circuitBreakerTripped = `Oscillation circuit breaker tripped (repeated earlier diff/failure state fingerprint=${fingerprint})`;
    } else if (nextNoProgress >= goal.budgets.maxNoProgressCycles) {
      nextStatus = "awaiting_human";
      circuitBreakerTripped = `No-progress circuit breaker tripped (${nextNoProgress} identical diff/failure cycles, fingerprint=${fingerprint})`;
    } else if (nextCycle >= goal.budgets.maxReviewCycles) {
      nextStatus = "awaiting_human";
      circuitBreakerTripped = `Max review cycles reached (${nextCycle}/${goal.budgets.maxReviewCycles})`;
    } else if (
      typeof goal.budgets.maxWallClockMin === "number" &&
      goal.budgets.maxWallClockMin > 0 &&
      Date.now() - goal.createdAt >= goal.budgets.maxWallClockMin * 60_000
    ) {
      nextStatus = "awaiting_human";
      circuitBreakerTripped = `Wall-clock budget exceeded (${goal.budgets.maxWallClockMin} min)`;
    } else if (
      typeof goal.budgets.maxApiEquivUsd === "number" &&
      goal.budgets.maxApiEquivUsd > 0 &&
      typeof input.spentUsd === "number" &&
      input.spentUsd >= goal.budgets.maxApiEquivUsd
    ) {
      nextStatus = "awaiting_human";
      circuitBreakerTripped = `API-equivalent cost budget exceeded ($${input.spentUsd.toFixed(2)}/$${goal.budgets.maxApiEquivUsd.toFixed(2)})`;
    } else {
      nextStatus = "executing";
    }

    const revId = reviewId(this.db);
    const now = Date.now();
    const source: GoalReviewSource = input.source ?? "review";
    const audit = input.audit ?? null;
    this.insertReviewRow({
      id: revId,
      goalId: goal.id,
      goalVersion: goal.version,
      cycle: nextCycle,
      verdict: effectiveVerdict,
      criteriaResults: input.criteriaResults,
      blockingIssues,
      summary: input.summary,
      progressFingerprint: fingerprint,
      createdAt: now,
      source,
      audit,
    });

    // A bounce back to executing starts a new verification epoch: drop the old claim in the same write.
    // Achieved and awaiting_human keep the evidence on screen.
    let liveProgressJson: string | null = goal.liveProgress ? JSON.stringify(goal.liveProgress) : null;
    if (nextStatus === "executing") {
      const cleared = stripStaleLiveProgress(goal.liveProgress, { clearBlockedPhase: false });
      if (cleared.changed) liveProgressJson = cleared.next ? JSON.stringify(cleared.next) : null;
    }
    this.db
      .prepare(
        `UPDATE goals
         SET status = ?, review_cycle = ?, no_progress_streak = ?,
             last_progress_fingerprint = ?, last_review_summary = ?, live_progress_json = ?, updated_at = ?
         WHERE id = ?`,
      )
      .run(
        nextStatus,
        nextCycle,
        nextNoProgress,
        fingerprint,
        circuitBreakerTripped ? `${circuitBreakerTripped} — ${input.summary}` : input.summary,
        liveProgressJson,
        now,
        goal.id,
      );

    const review: GoalReviewVerdict = {
      id: revId,
      goalId: goal.id,
      goalVersion: goal.version,
      cycle: nextCycle,
      verdict: effectiveVerdict,
      criteriaResults: input.criteriaResults,
      blockingIssues,
      summary: input.summary,
      progressFingerprint: fingerprint,
      createdAt: now,
      source,
      audit,
    };

    const updatedGoal = this.getGoal(goal.id)!;
    if (nextStatus === "achieved") this.settleEscalations(updatedGoal, "achieved");
    return { goal: updatedGoal, review, circuitBreakerTripped };
  }

  private insertReviewRow(r: GoalReviewVerdict & { source: GoalReviewSource; audit: GoalAcceptOverrideAudit | null }): void {
    this.db
      .prepare(
        `INSERT INTO goal_reviews(
          id, goal_id, goal_version, cycle, verdict, criteria_results_json,
          blocking_issues_json, summary, progress_fingerprint, created_at, source, audit_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        r.id,
        r.goalId,
        r.goalVersion,
        r.cycle,
        r.verdict,
        JSON.stringify(r.criteriaResults),
        JSON.stringify(r.blockingIssues),
        r.summary,
        r.progressFingerprint,
        r.createdAt,
        r.source,
        r.audit ? JSON.stringify(r.audit) : null,
      );
  }

  /**
   * Audited override: marks the goal achieved with an approved review tagged `director_accept_override`.
   * Bypasses the mechanical gate in `recordReviewVerdict` on purpose so honest failed gate results can be stored.
   * The caller owns evidence validation and deterministic gates; this only re-checks version and status atomically.
   */
  acceptGoalOverride(
    goalIdStr: string,
    input: {
      expectedVersion: number;
      criteriaResults: CriterionEvaluation[];
      summary: string;
      diffHash: string;
      audit: GoalAcceptOverrideAudit;
    },
  ): { goal: GoalRecord; review: GoalReviewVerdict } {
    const review = this.db.transaction((): GoalReviewVerdict => {
      const goal = this.getGoal(goalIdStr);
      if (!goal) throw new Error(`Unknown goal ${goalIdStr}`);
      if (goal.version !== input.expectedVersion) {
        throw new Error(`Goal ${goal.id} changed to v${goal.version} (expected v${input.expectedVersion}); not accepting.`);
      }
      if (goal.status === "draft" || goal.status === "achieved" || goal.status === "cancelled") {
        throw new Error(`Goal ${goal.id} is ${goal.status}; cannot accept.`);
      }
      if (goal.status === "paused" && input.audit.acceptedBy === "director") {
        throw new Error(`Goal ${goal.id} was paused by the user; the Director cannot accept a paused goal.`);
      }
      const nextCycle = goal.reviewCycle + 1;
      const failedIds = input.criteriaResults.filter((r) => !r.passed).map((r) => r.criterionId).sort();
      const fingerprint = createHash("sha256")
        .update(`${goal.version}|${input.diffHash}|${[...new Set(failedIds)].join(",")}`)
        .digest("hex")
        .slice(0, 16);
      const now = Date.now();
      const row: GoalReviewVerdict & { source: GoalReviewSource; audit: GoalAcceptOverrideAudit | null } = {
        id: reviewId(this.db),
        goalId: goal.id,
        goalVersion: goal.version,
        cycle: nextCycle,
        verdict: "approved",
        criteriaResults: input.criteriaResults,
        blockingIssues: [],
        summary: input.summary,
        progressFingerprint: fingerprint,
        createdAt: now,
        source: "director_accept_override",
        audit: input.audit,
      };
      this.insertReviewRow(row);
      this.db
        .prepare(
          `UPDATE goals
           SET status = 'achieved', review_cycle = ?, no_progress_streak = 0,
               last_progress_fingerprint = ?, last_review_summary = ?, updated_at = ?
           WHERE id = ?`,
        )
        .run(nextCycle, fingerprint, input.summary, now, goal.id);
      return row;
    })();
    const achieved = this.getGoal(review.goalId)!;
    this.settleEscalations(achieved, "achieved");
    return { goal: achieved, review };
  }

  listRevisions(goalIdStr: string): GoalRevisionRecord[] {
    const resolvedId = this.getGoal(goalIdStr)?.id ?? goalIdStr;
    const rows = this.db
      .prepare("SELECT * FROM goal_revisions WHERE goal_id = ? ORDER BY version DESC")
      .all(resolvedId) as Record<string, unknown>[];
    return rows.map((r) => ({
      goalId: String(r.goal_id),
      version: Number(r.version),
      title: String(r.title),
      description: String(r.description),
      criteria: JSON.parse(String(r.criteria_json)) as GoalCriterion[],
      nonGoals: JSON.parse(String(r.non_goals_json)) as string[],
      rationale: String(r.rationale),
      delta: r.delta_json ? (JSON.parse(String(r.delta_json)) as GoalAmendmentDelta) : null,
      createdAt: Number(r.created_at),
    }));
  }

  listReviews(goalIdStr: string): GoalReviewVerdict[] {
    const resolvedId = this.getGoal(goalIdStr)?.id ?? goalIdStr;
    const rows = this.db
      .prepare("SELECT * FROM goal_reviews WHERE goal_id = ? ORDER BY cycle ASC")
      .all(resolvedId) as Record<string, unknown>[];
    return rows.map((r) => ({
      id: String(r.id),
      goalId: String(r.goal_id),
      goalVersion: Number(r.goal_version),
      cycle: Number(r.cycle),
      verdict: String(r.verdict) as GoalReviewVerdict["verdict"],
      criteriaResults: JSON.parse(String(r.criteria_results_json)) as CriterionEvaluation[],
      blockingIssues: JSON.parse(String(r.blocking_issues_json)) as string[],
      summary: String(r.summary),
      progressFingerprint: String(r.progress_fingerprint),
      createdAt: Number(r.created_at),
      source: (typeof r.source === "string" && r.source ? r.source : "review") as GoalReviewSource,
      audit: parseAudit(r.audit_json),
    }));
  }

  buildReviewerPrompt(
    goal: GoalRecord,
    gateResult: DeterministicGateResult,
    diffStat: string,
    diffPatch?: string,
    ctx?: {
      reviewerMaxTurns?: number;
      evidence?: { goalEvidenceDir: string; goalsDir: string; artifactsDir: string; runArtifactsDir?: string };
      coordinatorFailure?: {
        source: GoalProgressSource;
        summary?: string;
        criteriaResults: CriterionEvaluation[];
        claimedAt?: number;
      };
    },
  ): string {
    const lines = [
      `# Independent Skeptical Done-Review (Goal ${goal.id} v${goal.version}, Cycle ${goal.reviewCycle + 1}/${goal.budgets.maxReviewCycles})`,
      `Title: ${goal.title}`,
      `Description: ${goal.description}`,
      "",
      "You are an INDEPENDENT, SKEPTICAL REVIEWER — NOT the generator.",
      "Inspect actual code, diffs, and test outputs. Focus on actionable correctness and contract failures; never approve stubbed implementations or unverified claims.",
      "",
      "## Acceptance Criteria (EVERY required criterion must have concrete evidence)",
      ...goal.criteria.map(
        (c) => `- [${c.id}] (${c.kind}${c.required ? ", REQUIRED" : ", optional"}): ${c.description}${c.command ? ` [cmd: ${c.command}]` : ""}`,
      ),
    ];
    if (goal.nonGoals.length > 0) {
      lines.push("", "## Non-Goals (Out of Scope)", ...goal.nonGoals.map((ng) => `- ${ng}`));
    }
    lines.push("", "## Deterministic Gate Results");
    for (const r of gateResult.results) {
      lines.push(`- [${r.criterionId}] ${r.passed ? "PASS" : "FAIL"}: ${r.evidence}`);
    }
    const failure = ctx?.coordinatorFailure;
    if (failure) {
      lines.push(
        "",
        "## Coordinator-Reported Verification Failure (current revision)",
        `Reported by: ${failure.source}${failure.claimedAt ? ` at ${new Date(failure.claimedAt).toISOString()}` : ""}`,
        `Summary: ${failure.summary?.trim() || "(no summary)"}`,
      );
      for (const c of failure.criteriaResults.filter((r) => !r.passed)) {
        lines.push(`- [${c.criterionId}] FAILED: ${c.evidence}${c.remediation ? ` (remediation: ${c.remediation})` : ""}`);
      }
      lines.push(
        "Confirm or refute each reported failure with your own evidence. Do not approve unless every reported failure is refuted or fixed; say which in the summary.",
      );
    }
    const ev = ctx?.evidence;
    if (ev) {
      lines.push(
        "",
        "## Evidence Locations (Read-Only)",
        `- Goal Evidence Directory: ${ev.goalEvidenceDir} (env HARNESS_GOAL_EVIDENCE_DIR)`,
        `- Goals Root: ${ev.goalsDir} (env HARNESS_GOALS_DIR)`,
        `- Artifacts Root: ${ev.artifactsDir} (env HARNESS_ARTIFACTS_DIR)`,
      );
      if (ev.runArtifactsDir) lines.push(`- Run Artifacts Directory: ${ev.runArtifactsDir}`);
    }
    lines.push("", "## Git Diff Summary", diffStat || "(no diff)");
    if (diffPatch?.trim()) {
      lines.push("", "## Git Diff Patch (truncated if large)", "```diff", diffPatch.trim(), "```");
    }
    if (ctx?.reviewerMaxTurns !== undefined) {
      lines.push(
        "",
        `You may get one advisory progress check-in around ${ctx.reviewerMaxTurns} tool-call turns; the harness does not stop you by turn count. The review has a hard 10-minute wall-clock limit. Submit as soon as every required criterion has evidence; a changes_requested verdict with partial evidence is better than none.`,
      );
    }
    lines.push(
      "",
      "Return a strict JSON verdict (or call submit_review_verdict) matching:",
      `{ "verdict": "approved" | "changes_requested" | "escalate_human", "criteriaResults": [{ "criterionId": "...", "passed": true|false, "evidence": "...", "remediation": "..." }], "blockingIssues": [], "summary": "..." }`,
    );
    return lines.join("\n");
  }
}

function parseAudit(raw: unknown): GoalAcceptOverrideAudit | null {
  if (typeof raw !== "string" || !raw.trim()) return null;
  try {
    return JSON.parse(raw) as GoalAcceptOverrideAudit;
  } catch {
    return null;
  }
}
