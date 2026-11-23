import { execFile } from "node:child_process";
import {
  type BigIntStats,
  appendFileSync,
  existsSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  realpathSync,
  statSync,
  symlinkSync,
} from "node:fs";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { promisify } from "node:util";
import type { WorktreeConfig } from "../config.js";
import type { ControlPlaneDb, WorktreeRecord } from "../control-plane/db.js";
import { type GateRuntimeConfig, resolveGateEnv, runGateCommand } from "../tools/gate-env.js";

export function canonicalPath(p: string): string {
  const abs = resolve(p);
  try {
    return realpathSync(abs);
  } catch {
    // Resolve the deepest existing ancestor so a deleted leaf keeps the identity Git recorded.
    const parent = dirname(abs);
    return parent === abs ? abs : join(canonicalPath(parent), basename(abs));
  }
}

function isWithin(parent: string, child: string): boolean {
  const rel = relative(parent, child);
  return rel !== "" && rel !== ".." && !rel.startsWith(`..${sep}`) && !isAbsolute(rel);
}

const ACTIVE_TASK_STATUSES = new Set(["running", "draining", "waiting_children", "merging", "paused_quota"]);
const SCRATCH_KINDS = new Set(["_verify", "_integration"]);

const execFileAsync = promisify(execFile);

export interface GitResult {
  stdout: string;
  stderr: string;
}

export async function runGit(cwd: string, args: string[], timeoutMs = 30_000): Promise<GitResult> {
  const { stdout, stderr } = await execFileAsync("git", args, {
    cwd,
    timeout: timeoutMs,
    maxBuffer: 16 * 1024 * 1024,
  });
  return { stdout: stdout.trim(), stderr: stderr.trim() };
}

export interface LiveWorktreeInfo {
  record: WorktreeRecord | null;
  path: string;
  branch: string | null;
  head: string | null;
  existsOnDisk: boolean;
  dirty: boolean;
  orphaned: boolean;
}

export interface CleanupReport {
  removed: string[];
  archivedRefs: string[];
  expiredArchiveRefs: string[];
  /** Registrations under the managed root that cleanup left untouched, and why. */
  skipped: Array<{ target: string; reason: string }>;
  errors: Array<{ target: string; error: string }>;
}

export class WorktreeConcurrencyLimitError extends Error {
  constructor(
    readonly activeCount: number,
    readonly limit: number,
  ) {
    super(`Worktree concurrency cap reached (${activeCount}/${limit}). Finish or clean a worker first.`);
    this.name = "WorktreeConcurrencyLimitError";
  }
}

export class WorktreeManager {
  constructor(
    private readonly db: ControlPlaneDb,
    private readonly repoRoot: string,
    private config: WorktreeConfig,
    private gate: GateRuntimeConfig = {},
  ) {}

  /** Applies live worktree config updates (e.g. `maxConcurrentWorkers`, `setupCommand`, `ciTimeoutMs`) and gate env settings. */
  updateConfig(next: WorktreeConfig, gate: GateRuntimeConfig = this.gate): void {
    this.config = next;
    this.gate = gate;
  }

  get maxConcurrentWorkers(): number {
    return Math.max(1, this.config.maxConcurrentWorkers ?? 4);
  }

  get baseDir(): string {
    return resolve(canonicalPath(this.repoRoot), this.config.dir ?? ".harness/worktrees");
  }

  isGitRepo(): boolean {
    if (!existsSync(this.repoRoot)) return false;
    let cur = canonicalPath(this.repoRoot);
    for (let i = 0; i < 32; i++) {
      if (existsSync(join(cur, ".git"))) return true;
      const parent = dirname(cur);
      if (parent === cur) break;
      cur = parent;
    }
    return false;
  }

  async resolveRepoInfo(): Promise<{ baseBranch: string; baseSha: string }> {
    if (!this.isGitRepo()) {
      return { baseBranch: "workspace", baseSha: "none" };
    }
    const [branchRes, shaRes] = await Promise.all([
      runGit(this.repoRoot, ["rev-parse", "--abbrev-ref", "HEAD"]),
      runGit(this.repoRoot, ["rev-parse", "HEAD"]),
    ]);
    if (branchRes.stdout === "HEAD") {
      throw new Error("Cannot start harness run from detached HEAD; checkout a branch first.");
    }
    return { baseBranch: branchRes.stdout, baseSha: shaRes.stdout };
  }

  async bootstrapWorktree(wtPath: string): Promise<void> {
    // Optional fast workspace setup hook (e.g., APFS clone-on-write node_modules).
    // Snapshot command and gate config together so a live reload mid-bootstrap cannot mix revisions.
    const setupCommand = this.config.setupCommand;
    const gate = this.gate;
    if (setupCommand) {
      const resolved = await resolveGateEnv({
        cwd: wtPath,
        config: gate,
        extraEnv: { HARNESS_REPO_ROOT: canonicalPath(this.repoRoot), HARNESS_WORKTREE_PATH: wtPath },
      });
      await runGateCommand(setupCommand, {
        cwd: wtPath,
        resolved,
        timeoutMs: 120_000,
        maxOutputBytes: 16 * 1024 * 1024,
        label: "Worktree setup",
      });
    } else {
      await this.linkIgnoredNodeModules(wtPath);
    }
  }

  async createForWorker(
    runId: string,
    taskId: string,
    baseSha?: string,
    opts?: { force?: boolean },
  ): Promise<WorktreeRecord> {
    const repoCanon = canonicalPath(this.repoRoot);
    const TERMINAL_RUN_STATUSES = new Set(["completed", "failed", "stopped"]);
    const LIVE_OR_PAUSED_TASK_STATUSES = new Set(["running", "draining", "waiting_children", "merging", "paused_quota"]);
    const INACTIVE_OWNER_TASK_STATUSES = new Set(["failed", "cancelled", "completed", "blocked", "returned_for_rescope"]);
    const active = this.db.listWorktrees().filter((w) => {
      if (w.status !== "active" && w.status !== "queued" && w.status !== "bounced") return false;
      const run = this.db.getRun(w.runId);
      // A worktree whose run row is gone cannot be attributed to any repo, so it must not hold a slot.
      if (!run) return false;
      if (canonicalPath(run.repoRoot) !== repoCanon) return false;
      const ownerTask = this.db.getTask(w.taskId);
      if (ownerTask && INACTIVE_OWNER_TASK_STATUSES.has(ownerTask.status)) return false;
      // Terminal runs free their slots unless the owner task is still live or quota-paused
      // (a resumed task can run before its run's stored status flips back to active).
      if (TERMINAL_RUN_STATUSES.has(run.status) && (!ownerTask || !LIVE_OR_PAUSED_TASK_STATUSES.has(ownerTask.status))) {
        return false;
      }
      return existsSync(w.path);
    });
    const limit = this.maxConcurrentWorkers;
    if (!opts?.force && active.length >= limit) {
      throw new WorktreeConcurrencyLimitError(active.length, limit);
    }

    const run = this.db.getRun(runId);
    const sha =
      baseSha ??
      (run?.baseBranch
        ? await runGit(this.repoRoot, ["rev-parse", "--verify", `${run.baseBranch}^{commit}`])
            .then((r) => r.stdout)
            .catch(async () => (await runGit(this.repoRoot, ["rev-parse", "HEAD"])).stdout)
        : (await runGit(this.repoRoot, ["rev-parse", "HEAD"])).stdout);
    const baseBranchName = `harness/${runId}/${taskId}`;
    let branch = baseBranchName;
    let suffix = 1;
    while (
      await runGit(this.repoRoot, ["show-ref", "--verify", "--quiet", `refs/heads/${branch}`]).then(
        () => true,
        () => false,
      )
    ) {
      suffix++;
      branch = `${baseBranchName}-${suffix}`;
    }
    const wtPath = join(this.baseDir, runId, suffix === 1 ? taskId : `${taskId}-${suffix}`);

    mkdirSync(join(this.baseDir, runId), { recursive: true });
    await runGit(this.repoRoot, ["worktree", "add", "-b", branch, wtPath, sha]);
    const identity = captureCreationIdentity(wtPath, await this.commonDir());

    try {
      await this.bootstrapWorktree(wtPath);
    } catch (err) {
      // Roll back only the exact registration this call created; anything else is left in place.
      let removed = false;
      try {
        const entries = await this.listRegistrations();
        const reg = matchRegistration(entries, wtPath);
        if (reg && reg.locked === null && reg.branch === branch) {
          verifyCreationIdentity(reg, identity);
          assertNoNestedCheckouts(canonicalPath(reg.path), entries);
          await runGit(this.repoRoot, ["worktree", "remove", "--force", reg.path]);
          removed = true;
        }
      } catch {
        // Leave the tree for inspection.
      }
      if (removed) await runGit(this.repoRoot, ["branch", "-D", branch]).catch(() => {});
      throw err;
    }

    return this.db.recordWorktree({
      runId,
      taskId,
      path: wtPath,
      branch,
      baseSha: sha,
      creationIdentityJson: serializeCreationIdentity(identity),
    });
  }

  /**
   * Adds a detached scratch worktree at <baseDir>/<kind>/<name>. remove() runs at most once, targets only the
   * registration this call created (by admin-entry and .git file identity, not path), and never rejects.
   */
  async addScratchWorktree(
    kind: "_verify" | "_integration",
    name: string,
    sha: string,
  ): Promise<{ path: string; remove: () => Promise<void> }> {
    const path = join(this.baseDir, kind, name);
    mkdirSync(join(this.baseDir, kind), { recursive: true });
    await runGit(this.repoRoot, ["worktree", "add", "--detach", path, sha]);
    const identity = captureCreationIdentity(path, await this.commonDir());
    let consumed = false;
    return {
      path,
      remove: async () => {
        if (consumed) return;
        consumed = true;
        try {
          const entries = await this.listRegistrations();
          const reg = matchRegistration(entries, path);
          if (!reg || reg.locked !== null || reg.branch !== null) return;
          verifyCreationIdentity(reg, identity);
          assertNoNestedCheckouts(canonicalPath(reg.path), entries);
          await runGit(this.repoRoot, ["worktree", "remove", "--force", reg.path]);
        } catch {
          // Removal is best effort; a refused tree stays registered.
        }
      },
    };
  }

  /** Canonical absolute path of this repository's common Git directory. */
  private async commonDir(): Promise<string> {
    const { stdout } = await runGit(this.repoRoot, ["rev-parse", "--git-common-dir"]);
    return canonicalPath(resolve(this.repoRoot, stdout));
  }

  /** All Git worktree registrations, parsed from NUL-delimited porcelain output so any path bytes survive. */
  private async listRegistrations(): Promise<PorcelainWorktree[]> {
    const { stdout } = await execFileAsync("git", ["worktree", "list", "--porcelain", "-z"], {
      cwd: this.repoRoot,
      timeout: 30_000,
      maxBuffer: 16 * 1024 * 1024,
    });
    return parsePorcelainWorktrees(stdout);
  }

  /** An owned row is retired when its task settled, or it merged and its task is no longer live. */
  private isRetired(row: WorktreeRecord): boolean {
    const task = this.db.getTask(row.taskId);
    if (task && ["completed", "failed", "cancelled"].includes(task.status)) return true;
    return row.status === "merged" && (!task || !ACTIVE_TASK_STATUSES.has(task.status));
  }

  /** Un-cleaned rows whose run belongs to this repo. Cleaned rows never authorise removal: paths and branch names are reused. */
  private ownedRows(runId?: string): WorktreeRecord[] {
    const repoCanon = canonicalPath(this.repoRoot);
    return this.db.listWorktrees(runId).filter((r) => {
      if (r.cleanedAt != null) return false;
      const run = this.db.getRun(r.runId);
      return Boolean(run && canonicalPath(run.repoRoot) === repoCanon);
    });
  }

  /**
   * Automatically symlinks `<repoRoot>/node_modules` into a fresh worktree when gitignored,
   * so workers and CI gates (`tsc`, `bun test`) work on turn 1 without redundant installs.
   */
  async linkIgnoredNodeModules(wtPath: string): Promise<void> {
    const srcNm = join(this.repoRoot, "node_modules");
    const dstNm = join(wtPath, "node_modules");
    if (!existsSync(srcNm) || existsSync(dstNm)) return;
    try {
      await runGit(this.repoRoot, ["check-ignore", "-q", "node_modules"]);
      symlinkSync(srcNm, dstNm, "dir");
      // If `.gitignore` uses `node_modules/` (directory-only), git treats a symlink named `node_modules`
      // as untracked (`?? node_modules`)! Add `/node_modules` to local `.git/info/exclude` (untracked)
      // so the worktree stays 100% clean for `git status` and `submit_for_merge`.
      const isIgnoredInWt = await runGit(wtPath, ["check-ignore", "-q", "node_modules"]).then(
        () => true,
        () => false,
      );
      if (!isIgnoredInWt) {
        const { stdout: commonDir } = await runGit(this.repoRoot, ["rev-parse", "--git-common-dir"]);
        const excludePath = resolve(this.repoRoot, commonDir, "info", "exclude");
        mkdirSync(resolve(excludePath, ".."), { recursive: true });
        const existing = existsSync(excludePath) ? readFileSync(excludePath, "utf8") : "";
        if (!existing.split("\n").some((line) => line.trim() === "/node_modules" || line.trim() === "node_modules")) {
          const prefix = existing.length > 0 && !existing.endsWith("\n") ? "\n" : "";
          appendFileSync(excludePath, `${prefix}/node_modules\n`, "utf8");
        }
      }
    } catch {
      // Not gitignored or symlink unsupported — leave untouched
    }
  }

  async isDirty(wtPath: string, opts?: { ignoreHarnessDir?: boolean }): Promise<boolean> {
    if (!existsSync(wtPath)) return false;
    const status = ["--no-optional-locks", "status", "--porcelain"];
    const args = opts?.ignoreHarnessDir ? [...status, "--", ".", ":!.harness"] : status;
    const { stdout } = await runGit(wtPath, args);
    return stdout.length > 0;
  }

  /**
   * Captures any uncommitted or unmerged commits into a lightweight safety git ref
   * before tearing down a worktree so agent work is never silently lost.
   */
  async snapshotToArchiveRef(wt: WorktreeRecord): Promise<string | null> {
    if (!existsSync(wt.path)) return null;
    const ref = `refs/harness/archive/${wt.runId}/${wt.taskId}`;
    try {
      const dirty = await this.isDirty(wt.path);
      if (dirty) {
        await runGit(wt.path, ["add", "-A"]);
        const { stdout: treeSha } = await runGit(wt.path, ["write-tree"]);
        const { stdout: headSha } = await runGit(wt.path, ["rev-parse", "HEAD"]);
        const { stdout: commitSha } = await runGit(wt.path, [
          "commit-tree",
          treeSha,
          "-p",
          headSha,
          "-m",
          `harness safety snapshot for ${wt.runId}/${wt.taskId}`,
        ]);
        await runGit(this.repoRoot, ["update-ref", ref, commitSha]);
      } else {
        const { stdout: headSha } = await runGit(wt.path, ["rev-parse", "HEAD"]);
        if (headSha === wt.baseSha) return null; // nothing to archive
        await runGit(this.repoRoot, ["update-ref", ref, headSha]);
      }
      this.db.updateWorktree(wt.id, { archiveRef: ref });
      return ref;
    } catch {
      return null;
    }
  }

  async teardown(
    worktreeId: string,
    opts?: { archiveBeforeRemove?: boolean; requireArchive?: boolean; finalStatus?: WorktreeRecord["status"] },
  ): Promise<string | null> {
    const wt = this.db.getWorktree(worktreeId);
    if (!wt) return null;
    // Idempotent, and a cleaned row never touches a reused path or branch.
    if (wt.cleanedAt != null) return wt.archiveRef;
    const run = this.db.getRun(wt.runId);
    if (!run || canonicalPath(run.repoRoot) !== canonicalPath(this.repoRoot)) {
      throw new Error(`Worktree ${worktreeId} does not belong to a run in ${this.repoRoot}`);
    }
    // Ownership, lock, and containment checks precede the archive snapshot, which stages files in the tree.
    if (!this.isRetired(wt) && !existsSync(wt.path)) {
      throw new Error(`Worktree ${worktreeId} is active and offline (checkout missing); left in place`);
    }
    const targetCanon = canonicalPath(wt.path);
    const owner = this.ownedRows().find((r) => canonicalPath(r.path) === targetCanon);
    if (owner && owner.id !== wt.id) {
      throw new Error(`Worktree ${worktreeId} is superseded by newer worktree ${owner.id} at ${wt.path}; left in place`);
    }
    const entries = await this.listRegistrations();
    const reg = matchRegistration(entries, wt.path);
    if (reg) {
      if (reg.locked !== null) {
        throw new Error(`Worktree ${worktreeId} is locked${reg.locked ? ` (${reg.locked})` : ""}; left in place`);
      }
      if (reg.branch !== null && reg.branch !== wt.branch) {
        throw new Error(`Worktree ${worktreeId} is on branch ${reg.branch}, not ${wt.branch}; left in place`);
      }
      verifyCheckoutBacklink(reg, await this.commonDir());
      // Common dir and backlink also describe a later registration at this path; only the creation identity proves it is ours.
      if (wt.creationIdentityJson != null) {
        verifyCreationIdentity(reg, parseCreationIdentity(wt.creationIdentityJson));
      } else if (reg.branch === null) {
        throw new Error(`Worktree ${worktreeId} is detached with no recorded creation identity; left in place`);
      }
    } else if (existsSync(wt.path)) {
      throw new Error(`Worktree ${worktreeId} at ${wt.path} is not registered with Git; left in place`);
    }
    assertNoNestedCheckouts(targetCanon, entries);

    let archiveRef: string | null = wt.archiveRef;
    if (opts?.archiveBeforeRemove) {
      archiveRef = (await this.snapshotToArchiveRef(wt)) ?? (existsSync(wt.path) ? null : wt.archiveRef);
      const mustArchive = opts.requireArchive ?? true;
      if (mustArchive && !archiveRef && existsSync(wt.path)) {
        // runGit throws on a non-zero exit; an uninspectable tree fails closed instead of reading as clean.
        const statusRes = await runGit(wt.path, ["status", "--porcelain"]).catch(() => null);
        const aheadRes = await runGit(this.repoRoot, ["rev-list", "--count", `${wt.baseSha}..${wt.branch}`]).catch(() => null);
        if (!statusRes || !aheadRes) {
          throw new Error(`Refusing to remove worktree ${worktreeId}: archiving failed and Git could not inspect its state.`);
        }
        const hasChanges = Boolean(statusRes.stdout.trim()) || Number(aheadRes.stdout.trim()) > 0;
        if (hasChanges) {
          throw new Error(`Refusing to remove worktree ${worktreeId}: archiving unmerged/dirty work failed.`);
        }
      }
    }

    // Path-targeted: Git's printed path matches its stored entry even when the checkout is missing.
    // The branch is deleted only when the removed registration still had it checked out; otherwise it may be a later one.
    if (reg) {
      await runGit(this.repoRoot, ["worktree", "remove", "--force", reg.path]);
      if (reg.branch === wt.branch && wt.branch !== run.baseBranch) {
        await runGit(this.repoRoot, ["branch", "-D", wt.branch]).catch(() => {});
      }
    }

    this.db.updateWorktree(wt.id, {
      status: opts?.finalStatus ?? "removed",
      archiveRef,
      cleanedAt: Date.now(),
    });
    this.db.emitEvent(wt.runId, wt.taskId, "worktree.removed", { worktreeId: wt.id, branch: wt.branch, archiveRef });
    return archiveRef;
  }

  /**
   * Lists registrations under the managed root, excluding scratch trees. `orphaned` means owned and retired.
   * With runId, only that run's owned trees are returned.
   */
  async listLive(runId?: string): Promise<LiveWorktreeInfo[]> {
    // Rows are newest first; on a reused path the newest un-cleaned row owns it.
    const byPath = new Map<string, WorktreeRecord>();
    for (const r of this.ownedRows()) {
      const key = canonicalPath(r.path);
      if (!byPath.has(key)) byPath.set(key, r);
    }
    const base = canonicalPath(this.baseDir);

    const results: LiveWorktreeInfo[] = [];
    for (const entry of await this.listRegistrations()) {
      const abs = canonicalPath(entry.path);
      if (!isWithin(base, abs)) continue;
      if (SCRATCH_KINDS.has(relative(base, abs).split(sep)[0] ?? "")) continue;
      const rec = byPath.get(abs) ?? null;
      if (runId && (!rec || rec.runId !== runId)) continue;
      const existsOnDisk = existsSync(abs);
      results.push({
        record: rec,
        path: abs,
        branch: entry.branch,
        head: entry.head,
        existsOnDisk,
        // A tree Git cannot read counts as dirty so it is archived, never skipped silently. Unmanaged trees are not probed.
        dirty: rec && existsOnDisk ? await this.isDirty(abs).catch(() => true) : false,
        orphaned: rec ? this.isRetired(rec) : false,
      });
    }
    return results;
  }

  /**
   * Retires owned worktrees whose tasks settled, finalises retired rows whose checkout is gone,
   * and expires old safety archive refs. Never prunes, deletes unowned trees, or deletes branches of cleaned rows.
   */
  async sweep(opts?: { runId?: string; forceAll?: boolean }): Promise<CleanupReport> {
    const report: CleanupReport = {
      removed: [],
      archivedRefs: [],
      expiredArchiveRefs: [],
      skipped: [],
      errors: [],
    };
    const errMsg = (err: unknown) => (err instanceof Error ? err.message : String(err));
    const attempted = new Set<string>();

    for (const item of await this.listLive(opts?.runId)) {
      const rec = item.record;
      if (!rec) {
        report.skipped.push({ target: item.path, reason: "unmanaged worktree" });
        continue;
      }
      if (!item.orphaned && !(opts?.forceAll && item.existsOnDisk)) {
        if (opts?.forceAll) report.skipped.push({ target: item.path, reason: "offline active worktree" });
        continue;
      }
      attempted.add(rec.id);
      try {
        const ref = await this.teardown(rec.id, {
          archiveBeforeRemove: item.dirty || rec.status !== "merged",
          finalStatus: rec.status === "merged" ? "merged" : "archived",
        });
        if (ref) report.archivedRefs.push(ref);
        report.removed.push(item.path);
      } catch (err) {
        report.errors.push({ target: item.path, error: errMsg(err) });
      }
    }

    // Finalise retired rows whose checkout is already gone; active rows with a missing checkout stay untouched.
    for (const rec of this.ownedRows(opts?.runId)) {
      if (attempted.has(rec.id) || !this.isRetired(rec)) continue;
      if (existsSync(rec.path)) {
        report.errors.push({ target: rec.path, error: "Retired worktree is not a Git worktree under the managed root; left in place" });
        continue;
      }
      try {
        await this.teardown(rec.id, {
          archiveBeforeRemove: false,
          finalStatus: rec.status === "merged" ? "merged" : "removed",
        });
      } catch (err) {
        report.errors.push({ target: rec.path, error: errMsg(err) });
      }
    }

    // Expire old safety archive refs past retentionDays; never the snapshots this sweep just took.
    // Deletion is guarded by the OID read here, so a concurrently refreshed ref survives.
    const retentionMs = (this.config.archiveRetentionDays ?? 7) * 86_400_000;
    const cutoffSec = Math.floor((Date.now() - retentionMs) / 1000);
    const runPrefix = opts?.runId ? `refs/harness/archive/${opts.runId}/` : "refs/harness/archive/";
    const justArchived = new Set(report.archivedRefs);
    try {
      const { stdout } = await runGit(this.repoRoot, [
        "for-each-ref",
        "--format=%(refname) %(objectname) %(committerdate:unix)",
        opts?.runId ? `refs/harness/archive/${opts.runId}` : "refs/harness/archive",
      ]);
      for (const line of stdout.split("\n")) {
        if (!line.trim()) continue;
        const [ref, oid, tsStr] = line.trim().split(/\s+/);
        if (!ref || !oid || !ref.startsWith(runPrefix) || justArchived.has(ref)) continue;
        const ts = Number(tsStr ?? 0);
        if (opts?.forceAll || (ts > 0 && ts < cutoffSec)) {
          try {
            await runGit(this.repoRoot, ["update-ref", "-d", ref, oid]);
            report.expiredArchiveRefs.push(ref);
          } catch (err) {
            report.errors.push({ target: ref, error: errMsg(err) });
          }
        }
      }
    } catch {
      // No archive refs yet
    }

    return report;
  }
}

interface PorcelainWorktree {
  path: string;
  head: string | null;
  branch: string | null;
  /** null when unlocked, "" for a bare lock, otherwise the lock reason. */
  locked: string | null;
}

/** Parses `git worktree list --porcelain -z`: NUL-terminated attributes, with an empty field ending each entry. */
function parsePorcelainWorktrees(stdout: string): PorcelainWorktree[] {
  const entries: PorcelainWorktree[] = [];
  let cur: PorcelainWorktree | null = null;
  for (const field of stdout.split("\0")) {
    if (field === "") {
      if (cur) entries.push(cur);
      cur = null;
      continue;
    }
    cur ??= { path: "", head: null, branch: null, locked: null };
    if (field.startsWith("worktree ")) cur.path = field.slice("worktree ".length);
    else if (field.startsWith("HEAD ")) cur.head = field.slice("HEAD ".length);
    else if (field.startsWith("branch ")) cur.branch = field.slice("branch ".length).replace(/^refs\/heads\//, "");
    else if (field === "locked") cur.locked = "";
    else if (field.startsWith("locked ")) cur.locked = field.slice("locked ".length);
  }
  if (cur) entries.push(cur);
  return entries;
}

/** Returns the single registration whose canonical path equals targetPath, or null. Throws when ambiguous. */
function matchRegistration(entries: PorcelainWorktree[], targetPath: string): PorcelainWorktree | null {
  const target = canonicalPath(targetPath);
  const matches = entries.filter((e) => canonicalPath(e.path) === target);
  if (matches.length > 1) throw new Error(`Multiple Git worktree registrations match ${targetPath}`);
  return matches[0] ?? null;
}

/** Reads a one-line path file Git wrote (`.git` link, `commondir`, `gitdir`) and resolves it relative to its directory. */
function readPathFile(file: string, prefix = ""): string {
  const raw = readFileSync(file, "utf8").replace(/\r?\n$/, "");
  if (!raw.startsWith(prefix) || raw.length === prefix.length) throw new Error(`Malformed ${file}`);
  return canonicalPath(resolve(dirname(file), raw.slice(prefix.length)));
}

/**
 * Resolves the admin entry a checkout's `.git` file points at and proves it is a linked worktree of
 * expectedCommonDir whose backlink names this checkout. Throws on any mismatch.
 */
function resolveAdminDir(wtPath: string, expectedCommonDir: string): string {
  const dotGit = join(wtPath, ".git");
  if (!lstatSync(dotGit).isFile()) throw new Error(`${dotGit} is not a linked-worktree .git file`);
  const adminDir = readPathFile(dotGit, "gitdir: ");
  if (dirname(adminDir) !== join(expectedCommonDir, "worktrees")) {
    throw new Error(`${wtPath} belongs to ${adminDir}, not a worktree of ${expectedCommonDir}`);
  }
  if (readPathFile(join(adminDir, "commondir")) !== expectedCommonDir) {
    throw new Error(`${adminDir} does not share common directory ${expectedCommonDir}`);
  }
  if (readPathFile(join(adminDir, "gitdir")) !== canonicalPath(dotGit)) {
    throw new Error(`${adminDir} does not point back to ${wtPath}`);
  }
  return adminDir;
}

/** Refuses before any read or staging when an on-disk checkout is not this registration of this repository. */
function verifyCheckoutBacklink(reg: PorcelainWorktree, expectedCommonDir: string): void {
  if (!existsSync(reg.path)) return;
  let adminDir: string;
  try {
    adminDir = resolveAdminDir(reg.path, expectedCommonDir);
  } catch (err) {
    throw new Error(`Checkout at ${reg.path} does not link back to its registration (${err instanceof Error ? err.message : err}); left in place`);
  }
  if (existsSync(join(adminDir, "locked"))) throw new Error(`Worktree at ${reg.path} is locked; left in place`);
}

interface FileIdentity {
  dev: bigint;
  ino: bigint;
  birthtimeNs: bigint;
}

interface CreationIdentity {
  adminDir: string;
  adminStat: FileIdentity;
  dotGitStat: FileIdentity;
}

function fileIdentity(s: BigIntStats): FileIdentity {
  return { dev: s.dev, ino: s.ino, birthtimeNs: s.birthtimeNs };
}

function sameIdentity(a: FileIdentity, b: FileIdentity): boolean {
  return a.dev === b.dev && a.ino === b.ino && a.birthtimeNs === b.birthtimeNs;
}

function serializeCreationIdentity(id: CreationIdentity): string {
  const file = (f: FileIdentity) => ({ dev: f.dev.toString(), ino: f.ino.toString(), birthtimeNs: f.birthtimeNs.toString() });
  return JSON.stringify({ adminDir: id.adminDir, adminStat: file(id.adminStat), dotGitStat: file(id.dotGitStat) });
}

/** Parses serializeCreationIdentity output; throws on anything malformed so removal fails closed. */
function parseCreationIdentity(raw: string): CreationIdentity {
  const bad = () => new Error("Recorded creation identity is malformed; left in place");
  let v: unknown;
  try {
    v = JSON.parse(raw);
  } catch {
    throw bad();
  }
  const int = (x: unknown): bigint => {
    if (typeof x !== "string" || !/^\d+$/.test(x)) throw bad();
    return BigInt(x);
  };
  const file = (x: unknown): FileIdentity => {
    if (!x || typeof x !== "object") throw bad();
    const f = x as Record<string, unknown>;
    return { dev: int(f.dev), ino: int(f.ino), birthtimeNs: int(f.birthtimeNs) };
  };
  if (!v || typeof v !== "object") throw bad();
  const o = v as Record<string, unknown>;
  if (typeof o.adminDir !== "string" || !isAbsolute(o.adminDir)) throw bad();
  return { adminDir: o.adminDir, adminStat: file(o.adminStat), dotGitStat: file(o.dotGitStat) };
}

/** Records the admin entry and `.git` file a just-added worktree got, so later removal targets only that creation. */
function captureCreationIdentity(wtPath: string, expectedCommonDir: string): CreationIdentity {
  const adminDir = resolveAdminDir(wtPath, expectedCommonDir);
  return {
    adminDir,
    adminStat: fileIdentity(statSync(adminDir, { bigint: true })),
    dotGitStat: fileIdentity(lstatSync(join(wtPath, ".git"), { bigint: true })),
  };
}

/** Throws unless reg is still the exact, unlocked registration and checkout recorded by captureCreationIdentity. */
function verifyCreationIdentity(reg: PorcelainWorktree, identity: CreationIdentity): void {
  const { adminDir } = identity;
  if (!existsSync(adminDir) || !sameIdentity(fileIdentity(statSync(adminDir, { bigint: true })), identity.adminStat)) {
    throw new Error(`Admin entry ${adminDir} was replaced`);
  }
  const dotGit = join(reg.path, ".git");
  if (readPathFile(join(adminDir, "gitdir")) !== canonicalPath(dotGit)) {
    throw new Error(`Admin entry ${adminDir} no longer points at ${reg.path}`);
  }
  if (existsSync(join(adminDir, "locked"))) throw new Error(`Worktree at ${reg.path} is locked`);
  if (!existsSync(reg.path)) return;
  const st = lstatSync(dotGit, { bigint: true });
  if (!st.isFile() || !sameIdentity(fileIdentity(st), identity.dotGitStat)) throw new Error(`${dotGit} was replaced`);
  if (readPathFile(dotGit, "gitdir: ") !== adminDir) throw new Error(`${dotGit} no longer points at ${adminDir}`);
}

/**
 * Refuses when removing targetCanon would also destroy another checkout: a registered worktree below it,
 * or any `.git` entry below it on disk in any letter case (foreign repositories, submodules). Symlinks are not followed.
 */
function assertNoNestedCheckouts(targetCanon: string, entries: PorcelainWorktree[]): void {
  for (const e of entries) {
    if (isWithin(targetCanon, canonicalPath(e.path))) {
      throw new Error(`${targetCanon} contains nested registered worktree ${e.path}; left in place`);
    }
  }
  if (!existsSync(targetCanon)) return;
  const stack = [targetCanon];
  while (stack.length > 0) {
    const dir = stack.pop()!;
    if (dir !== targetCanon) {
      // Ask the filesystem, which resolves `.git` the way Git does (case-insensitively on such volumes).
      let found = true;
      try {
        lstatSync(join(dir, ".git"));
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
        found = false;
      }
      if (found) throw new Error(`${targetCanon} contains nested git checkout ${dir}; left in place`);
    }
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      if (entry.name.toLowerCase() === ".git") {
        if (dir !== targetCanon) throw new Error(`${targetCanon} contains nested git checkout ${dir}; left in place`);
        continue;
      }
      if (entry.isDirectory() && !entry.isSymbolicLink()) stack.push(join(dir, entry.name));
    }
  }
}
