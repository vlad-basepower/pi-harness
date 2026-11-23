import type { WorktreeConfig } from "../config.js";
import type { ControlPlaneDb, MergeQueueRecord } from "../control-plane/db.js";
import { KnowledgeMaintainer } from "../kb/maintainer.js";
import {
  GateCommandError,
  GateConfigError,
  type GateRuntimeConfig,
  type ResolvedGateEnv,
  resolveGateEnv,
  runGateCommand,
} from "../tools/gate-env.js";
import { canonicalPath, runGit, type WorktreeManager } from "./manager.js";

export interface MergeOutcome {
  item: MergeQueueRecord;
  mergedSha?: string;
  bouncedReason?: string;
}

export interface MergeProcessOptions {
  preserveLiveTask?: boolean;
  signal?: AbortSignal;
}

const ALLOWED_SHARED_METADATA_FILES = new Set([
  "package.json",
  "bun.lock",
  "bun.lockb",
  "package-lock.json",
  "pnpm-lock.yaml",
  "yarn.lock",
  "Cargo.lock",
  "go.sum",
]);

const TEST_WEAKENING_RE =
  /\b(?:it|test|describe)\.(?:skip|only)\s*\(|\b(?:it|test)\.todo\s*\(|\bx(?:it|test|describe)\s*\(|\bskipIf\s*\(|@pytest\.mark\.skip\b|\bpytest\.skip\s*\(|#\[\s*ignore\b|\bt\.Skip\s*\(/;

/**
 * Deterministic test-integrity guard: detects if a branch weakens or skips tests
 * (e.g., adding `.skip` / `.only` / `.todo` / `xit` / `#[ignore]` / `t.Skip(`) without explicit authorization.
 */
export async function checkTestIntegrity(repoRoot: string, baseBranch: string, branch: string): Promise<string[]> {
  const { stdout } = await runGit(repoRoot, ["diff", "-U0", `${baseBranch}...${branch}`, "--", "*test*", "*spec*"]);
  const warnings: string[] = [];
  let currentFile = "";
  for (const line of stdout.split("\n")) {
    if (line.startsWith("+++ b/")) {
      currentFile = line.slice(6);
      continue;
    }
    if (line.startsWith("+") && !line.startsWith("+++")) {
      const trimmed = line.slice(1).trim();
      // Ignore comment-only lines, while preserving Rust attributes like `#[ignore]`
      if (
        trimmed.startsWith("//") ||
        trimmed.startsWith("/*") ||
        trimmed.startsWith("*") ||
        (trimmed.startsWith("#") && !trimmed.startsWith("#["))
      ) {
        continue;
      }
      if (TEST_WEAKENING_RE.test(trimmed)) {
        warnings.push(`${currentFile}: added test skip/filter (${trimmed.slice(0, 80)})`);
      }
    }
  }
  return warnings;
}

function isWildcardWriteScope(scope: string): boolean {
  const norm = scope.trim().replace(/^\.\//, "").replace(/\/+$/, "");
  return norm === "*" || norm === "**" || norm === "**/*";
}

async function checkWriteScopes(
  repoRoot: string,
  targetBranch: string,
  branch: string,
  writeScopes: string[],
): Promise<string[]> {
  const scopes = writeScopes.map((s) => s.trim()).filter(Boolean);
  if (scopes.length === 0 || scopes.some(isWildcardWriteScope)) {
    return [];
  }

  const { stdout } = await runGit(repoRoot, ["diff", "--no-renames", "--name-only", "-z", `${targetBranch}...${branch}`]);
  const changedFiles = stdout
    .split("\0")
    .map((f) => f.trim().replace(/^\.\//, ""))
    .filter(Boolean);
  if (changedFiles.length === 0) return [];

  const matchers = scopes.map((raw) => {
    const norm = raw.replace(/^\.\//, "");
    if (norm.endsWith("/")) {
      const glob = new Bun.Glob(`${norm}**`);
      return (file: string) => glob.match(file);
    }
    const glob = new Bun.Glob(norm);
    const isPlainPath = !/[*?[{]/.test(norm);
    return (file: string) => glob.match(file) || (isPlainPath && file.startsWith(`${norm}/`));
  });

  const outOfScope: string[] = [];
  for (const file of changedFiles) {
    if (ALLOWED_SHARED_METADATA_FILES.has(file)) continue;
    if (!matchers.some((match) => match(file))) {
      outOfScope.push(file);
    }
  }
  return outOfScope;
}

/**
 * Bors-style deterministic merge queue:
 * Serializes integration of parallel Tier 2 Worker branches onto the run's target branch
 * in an ephemeral integration worktree, runs computational CI gates + test-integrity checks,
 * and bounces conflicts or CI failures directly back to the owning Worker (no LLM integrator bottleneck).
 */
export class BisectingMergeQueue {
  private lockChains = new Map<string, Promise<void>>();
  private completedOutcomes = new Map<string, MergeOutcome>();

  constructor(
    private readonly db: ControlPlaneDb,
    private readonly worktrees: WorktreeManager,
    private readonly repoRoot: string,
    private config: WorktreeConfig,
    private gate: GateRuntimeConfig = {},
  ) {}

  updateConfig(next: WorktreeConfig, gate: GateRuntimeConfig = this.gate): void {
    this.config = next;
    this.gate = gate;
  }

  private async withLock<T>(lockKey: string, fn: () => Promise<T>): Promise<T> {
    const prev = this.lockChains.get(lockKey) ?? Promise.resolve();
    let release!: () => void;
    const next = new Promise<void>((resolve) => {
      release = resolve;
    });
    const chained = prev.then(() => next);
    this.lockChains.set(lockKey, chained);

    await prev;
    try {
      return await fn();
    } finally {
      release();
      if (this.lockChains.get(lockKey) === chained) {
        this.lockChains.delete(lockKey);
      }
    }
  }

  private rememberOutcome(mergeId: string, outcome: MergeOutcome): void {
    this.completedOutcomes.set(mergeId, outcome);
    if (this.completedOutcomes.size > 500) {
      const oldest = this.completedOutcomes.keys().next().value;
      if (oldest !== undefined) this.completedOutcomes.delete(oldest);
    }
  }

  private listPendingMergesForTarget(primaryRunId: string, targetBranch: string): MergeQueueRecord[] {
    const targetRepo = canonicalPath(this.repoRoot);
    const matchingRunIds = new Set<string>([primaryRunId]);

    let cursor: { createdAt: number; id: string } | undefined;
    for (;;) {
      const batch = this.db.listRuns(200, cursor);
      for (const r of batch) {
        if (r.baseBranch === targetBranch && (r.repoRoot === this.repoRoot || canonicalPath(r.repoRoot) === targetRepo)) {
          matchingRunIds.add(r.id);
        }
      }
      if (batch.length < 200) break;
      const last = batch[batch.length - 1]!;
      cursor = { createdAt: last.createdAt, id: last.id };
    }

    const byId = new Map<string, MergeQueueRecord>();
    for (const rid of matchingRunIds) {
      for (const item of this.db.listPendingMerges(rid)) {
        byId.set(item.id, item);
      }
    }

    return [...byId.values()].sort((a, b) => {
      if (a.createdAt !== b.createdAt) return a.createdAt - b.createdAt;
      const aNum = /^mq-(\d+)$/.exec(a.id);
      const bNum = /^mq-(\d+)$/.exec(b.id);
      if (aNum && bNum) return Number(aNum[1]) - Number(bNum[1]);
      return a.id.localeCompare(b.id);
    });
  }

  async processNext(runId: string, opts?: MergeProcessOptions): Promise<MergeOutcome | null> {
    const run = this.db.getRun(runId);
    if (!run) return null;

    // Serialize merge queue by `(repoRoot, targetBranch)` — NOT `runId` — so concurrent
    // same-repo runs targeting the same branch never race on `refs/heads/<targetBranch>`!
    const lockKey = `${this.repoRoot}::${run.baseBranch}`;
    return this.withLock(lockKey, async () => {
      const pending = this.db.listPendingMerges(runId);
      const next = pending[0];
      if (!next) return null;
      const outcome = await this.processMergeItem(next, run.baseBranch, opts);
      this.rememberOutcome(next.id, outcome);
      return outcome;
    });
  }

  async submitAndDrain(runId: string, mergeId: string, opts?: MergeProcessOptions): Promise<MergeOutcome> {
    const run = this.db.getRun(runId);
    if (!run) {
      throw new Error(`Unknown run ${runId}`);
    }

    const targetBranch = run.baseBranch;
    const lockKey = `${this.repoRoot}::${targetBranch}`;

    return this.withLock(lockKey, async () => {
      const cached = this.completedOutcomes.get(mergeId);
      if (cached) return cached;

      const existing = this.db.getMergeQueueItem(mergeId);
      if (!existing) {
        throw new Error(`Merge queue item ${mergeId} not found`);
      }
      if (existing.status !== "queued") {
        return {
          item: existing,
          ...(existing.status === "merged" ? {} : { bouncedReason: existing.log ?? undefined }),
        };
      }

      const pending = this.listPendingMergesForTarget(runId, targetBranch);
      for (const item of pending) {
        const itemRun = item.runId === runId ? run : this.db.getRun(item.runId);
        // Only apply caller's AbortSignal to caller's own merge item so cancelling one worker never aborts another run's merge!
        const itemOpts = item.id === mergeId ? opts : { ...opts, signal: undefined };
        const outcome = await this.processMergeItem(item, itemRun?.baseBranch ?? targetBranch, itemOpts);
        this.rememberOutcome(item.id, outcome);
        if (item.id === mergeId) {
          return outcome;
        }
      }

      const fallbackCached = this.completedOutcomes.get(mergeId);
      if (fallbackCached) return fallbackCached;
      const after = this.db.getMergeQueueItem(mergeId);
      if (after && after.status !== "queued") {
        return {
          item: after,
          ...(after.status === "merged" ? {} : { bouncedReason: after.log ?? undefined }),
        };
      }

      const directOutcome = await this.processMergeItem(existing, targetBranch, opts);
      this.rememberOutcome(existing.id, directOutcome);
      return directOutcome;
    });
  }

  private async processMergeItem(
    next: MergeQueueRecord,
    targetBranch: string,
    opts?: MergeProcessOptions,
  ): Promise<MergeOutcome> {
    let integration: { path: string; remove: () => Promise<void> } | undefined;
    try {
      const { stdout: targetSha } = await runGit(this.repoRoot, ["rev-parse", targetBranch]);

      const isTaskCancelledOrTerminal = (): boolean => {
        if (opts?.signal?.aborted) return true;
        const curTask = this.db.getTask(next.taskId);
        return Boolean(curTask && ["cancelled", "failed", "completed"].includes(curTask.status));
      };

      const abortMerge = async (reason: string): Promise<MergeOutcome> => {
        const curTask = this.db.getTask(next.taskId);
        const isTerminal = Boolean(curTask && ["cancelled", "failed", "completed"].includes(curTask.status));
        if (!isTerminal) {
          return this.bounce(next, "ci_failed", reason, targetBranch, opts);
        }
        const finished = this.db.finishMergeItem(next.id, "ci_failed", reason);
        await this.worktrees
          .teardown(next.worktreeId, { archiveBeforeRemove: true, finalStatus: "archived" })
          .catch(() => {
            this.db.updateWorktree(next.worktreeId, { status: "bounced" });
          });
        return { item: finished, bouncedReason: reason };
      };

      if (isTaskCancelledOrTerminal()) {
        return await abortMerge("Merge aborted: task was cancelled or already settled.");
      }

      // Verify test integrity before touching integration branch
      const integrityWarnings = await checkTestIntegrity(this.repoRoot, targetBranch, next.branch);
      if (integrityWarnings.length > 0) {
        const log = `Test-integrity check failed:\n${integrityWarnings.join("\n")}`;
        return this.bounce(next, "ci_failed", log, targetBranch, opts);
      }

      // Enforce assigned write_leases mechanically before running CI (uses stored scopes even if leases were deactivated on bounce/block)
      const assignedWriteScopes = this.db.listAssignedWriteScopes(next.runId, next.taskId);
      const outOfScopeFiles = await checkWriteScopes(this.repoRoot, targetBranch, next.branch, assignedWriteScopes);
      if (outOfScopeFiles.length > 0) {
        const log = `Write-scope violation: modified files outside assigned write_scopes (${assignedWriteScopes.join(", ")}):\n${outOfScopeFiles.join("\n")}`;
        return this.bounce(next, "ci_failed", log, targetBranch, opts);
      }

      // Create detached ephemeral integration worktree on latest targetSha
      integration = await this.worktrees.addScratchWorktree("_integration", `${next.runId}-${next.id}`, targetSha);
      const intPath = integration.path;
      try {
        await this.worktrees.bootstrapWorktree(intPath);
      } catch (bootErr) {
        const detail = bootErr instanceof Error ? bootErr.message : String(bootErr);
        return this.bounce(next, "ci_failed", `Integration worktree bootstrap failed: ${detail}`, targetBranch, opts);
      }

      // Attempt clean no-ff merge of the worker branch
      try {
        await runGit(intPath, [
          "-c",
          `user.name=${this.config.gitAuthorName ?? "pi-harness"}`,
          "-c",
          `user.email=${this.config.gitAuthorEmail ?? "harness@local"}`,
          "merge",
          "--no-ff",
          "--no-edit",
          next.branch,
        ]);
      } catch (mergeErr) {
        const { stdout: conflictFiles } = await runGit(intPath, ["diff", "--name-only", "--diff-filter=U"]).catch(() => ({
          stdout: "",
        }));
        await runGit(intPath, ["merge", "--abort"]).catch(() => {});
        const log = `Merge conflict integrating ${next.branch} into ${targetBranch}:\nFiles:\n${conflictFiles}\nDetail: ${
          mergeErr instanceof Error ? mergeErr.message : String(mergeErr)
        }`;
        return this.bounce(next, "conflict", log, targetBranch, opts);
      }

      // Run deterministic computational CI gates (abortable on task cancellation)
      const ciLogs: string[] = [];
      let resolved: ResolvedGateEnv | undefined;
      for (const cmd of this.config.ciCommands ?? []) {
        if (isTaskCancelledOrTerminal()) {
          return await abortMerge("Merge aborted before CI: task cancelled.");
        }
        try {
          resolved ??= await resolveGateEnv({
            cwd: intPath,
            config: this.gate,
            extraEnv: { HARNESS_REPO_ROOT: canonicalPath(this.repoRoot), HARNESS_WORKTREE_PATH: intPath },
            signal: opts?.signal,
          });
          const { stdout, stderr } = await runGateCommand(cmd, {
            cwd: intPath,
            resolved,
            timeoutMs: this.config.ciTimeoutMs ?? 300_000,
            maxOutputBytes: 16 * 1024 * 1024,
            label: "CI gate",
            signal: opts?.signal,
          });
          ciLogs.push(`$ ${cmd} -> OK\n${stdout.slice(-1000)}${stderr ? "\n" + stderr.slice(-500) : ""}`);
        } catch (err: unknown) {
          if (isTaskCancelledOrTerminal()) {
            return await abortMerge("Merge aborted during CI: task cancelled.");
          }
          const e = err as { stdout?: string; stderr?: string; message?: string };
          // Config errors and timeouts/overflows often leave stderr empty; keep the reason visible.
          const reason =
            err instanceof GateConfigError || (err instanceof GateCommandError && err.reason !== "exit")
              ? `\n${err.message}`
              : "";
          const stderrTail = e.stderr ?? (reason ? "" : (e.message ?? ""));
          const log = `CI gate failed: ${cmd}${reason}\n${(e.stdout ?? "").slice(-2000)}\n${stderrTail.slice(-2000)}${
            resolved ? `\n[gate-env] ${resolved.provenance}` : ""
          }`;
          return this.bounce(next, "ci_failed", log, targetBranch, opts);
        }
      }

      // Re-check cancellation right before advancing targetBranch
      if (isTaskCancelledOrTerminal()) {
        return await abortMerge("Merge aborted before ref promotion: task cancelled.");
      }

      // Advance target branch safely without ever leaving the checked-out working tree out of sync with HEAD!
      const { stdout: headBranch } = await runGit(this.repoRoot, ["rev-parse", "--abbrev-ref", "HEAD"]);
      const { stdout: mergedSha } = await runGit(intPath, ["rev-parse", "HEAD"]);

      if (headBranch === targetBranch) {
        // `git merge --ff-only` atomically updates HEAD, index, and working tree while preserving
        // untracked files (like `bun.lock`) and unrelated dirty tracked edits. If a dirty tracked file
        // genuinely collides with a merged path, it refuses cleanly instead of corrupting HEAD with staged deletions.
        const { stdout: curHead } = await runGit(this.repoRoot, ["rev-parse", "HEAD"]);
        if (curHead !== targetSha) {
          return this.bounce(
            next,
            "conflict",
            `Target branch ${targetBranch} moved concurrently (${targetSha} -> ${curHead}); re-submit.`,
            targetBranch,
            opts,
          );
        }
        try {
          await runGit(this.repoRoot, ["merge", "--ff-only", mergedSha]);
        } catch (ffErr) {
          const detail = ffErr instanceof Error ? ffErr.message : String(ffErr);
          return this.bounce(
            next,
            "conflict",
            `Host repoRoot has conflicting local changes on ${targetBranch} preventing fast-forward merge: ${detail}`,
            targetBranch,
            opts,
          );
        }
      } else {
        // Target branch is not currently checked out — atomic CAS update-ref is safe
        try {
          await runGit(this.repoRoot, ["update-ref", `refs/heads/${targetBranch}`, mergedSha, targetSha]);
        } catch (refErr) {
          const detail = refErr instanceof Error ? refErr.message : String(refErr);
          return this.bounce(
            next,
            "conflict",
            `Target branch ${targetBranch} moved concurrently during ref update (${targetSha.slice(0, 8)}): ${detail}`,
            targetBranch,
            opts,
          );
        }
      }

      const finished = this.db.finishMergeItem(next.id, "merged", ciLogs.join("\n"));
      const curTaskAfterMerge = this.db.getTask(next.taskId);
      const alreadyTerminal = Boolean(curTaskAfterMerge && ["cancelled", "failed", "completed"].includes(curTaskAfterMerge.status));
      if (opts?.preserveLiveTask === true) {
        this.db.updateWorktree(next.worktreeId, { status: "merged" });
        if (!alreadyTerminal) {
          this.db.updateTask(next.taskId, { status: "running" });
        }
      } else {
        if (!alreadyTerminal) {
          this.db.updateTask(next.taskId, { status: "completed", summary: `Merged ${next.branch} at ${mergedSha.slice(0, 8)}` });
        }
        // A refused teardown must not reject a landed merge; the tree stays for the next sweep.
        await this.worktrees.teardown(next.worktreeId, { archiveBeforeRemove: false, finalStatus: "merged" }).catch(() => {
          this.db.updateWorktree(next.worktreeId, { status: "merged" });
        });
      }

      // Lightweight $0-token KB staleness check after targetBranch advances; never breaks merges.
      const run = this.db.getRun(next.runId);
      const repoScope = run?.repoRoot ?? this.repoRoot;
      await new KnowledgeMaintainer(this.db.kb)
        .runDeterministicPreCheck(this.repoRoot, repoScope, mergedSha)
        .catch(() => {});

      return { item: finished, mergedSha };
    } finally {
      await integration?.remove();
    }
  }

  private bounce(
    next: MergeQueueRecord,
    status: "conflict" | "ci_failed",
    log: string,
    targetBranch: string,
    opts?: MergeProcessOptions,
  ): MergeOutcome {
    const finalLog =
      opts?.preserveLiveTask === true && status === "conflict"
        ? `${log}\nRun \`git merge ${targetBranch}\` or \`git rebase ${targetBranch}\` in your worktree, resolve conflicts, re-run verification, and call submit_for_merge again.`
        : log;
    const finished = this.db.finishMergeItem(next.id, status, finalLog);
    this.db.updateWorktree(next.worktreeId, { status: "bounced" });
    this.db.updateTask(next.taskId, {
      status: opts?.preserveLiveTask === true ? "running" : "ready",
      summary: `Merge bounced (${status}): needs fix & re-submit`,
    });
    this.db.emitEvent(next.runId, next.taskId, "merge.bounced", { mergeId: next.id, status, log: finalLog });
    return { item: finished, bouncedReason: finalLog };
  }
}
