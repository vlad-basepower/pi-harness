import { describe, expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ModelCatalogCompiler } from "../src/catalog/compiler.js";
import { loadConfig } from "../src/config.js";
import { INBOX_TRIAGE_TABLES } from "../src/control-plane/inbox-triage-store.js";
import type { TriageClock, TriageScheduler } from "../src/control-plane/inbox-triage.js";
import { DaemonClient } from "../src/daemon/remote.js";
import { HarnessDaemon } from "../src/daemon/server.js";
import type { InboxItem, InboxTriageListResult, InboxTriageRearmResult, InboxTriageReportResult } from "../src/protocol/daemon-rpc.js";

function initTempGitRepo(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  execFileSync("git", ["init", "-b", "main"], { cwd: dir, stdio: "ignore" });
  execFileSync("git", ["config", "user.email", "test@example.com"], { cwd: dir, stdio: "ignore" });
  execFileSync("git", ["config", "user.name", "Test"], { cwd: dir, stdio: "ignore" });
  writeFileSync(join(dir, "README.md"), "# test\n");
  execFileSync("git", ["add", "."], { cwd: dir, stdio: "ignore" });
  execFileSync("git", ["commit", "-m", "init"], { cwd: dir, stdio: "ignore" });
  return dir;
}

function makeTestConfig(repoDir: string, stateDir: string) {
  const cfg = loadConfig(repoDir);
  cfg.stateDir = stateDir;
  cfg.socketPath = join(stateDir, "d.sock");
  cfg.quotaEndpoint = undefined;
  cfg.catalog = { ttlHours: 24 };
  cfg.worktree = { dir: ".harness/worktrees", archiveRetentionDays: 7, maxConcurrentWorkers: 2 };
  cfg.codeMode = "off";
  cfg.events.wakeDirectorOnGoalSettle = false;
  cfg.profiles = [
    { id: "pi:local", kind: "pi", provider: "fixture", billing: "local", models: ["coordinator"] },
  ];
  cfg.defaultOrchestratorProfile = "pi:local";
  cfg.defaultWorkerProfile = "pi:local";
  cfg.defaultTaskProfile = "pi:local";
  const compiler = new ModelCatalogCompiler(cfg);
  const catalog = compiler.compileFromData({
    enabledProfiles: cfg.profiles,
    inventory: new Map([["pi:local", [{ wireModelId: "coordinator" }]]]),
    profileStatuses: [{ profileId: "pi:local", status: "ok", modelCount: 1 }],
    modelsDev: [],
    openRouter: [],
    litellmDep: new Map(),
    benchmarks: [],
    sources: [],
  });
  writeFileSync(compiler.cachePath, JSON.stringify(catalog));
  return cfg;
}

class FakeTimer {
  nowMs: number;
  private nextId = 1;
  private timers = new Map<number, { fireAt: number; intervalMs: number | null; fn: () => void }>();

  constructor(startMs = 1_795_455_600_000) {
    this.nowMs = startMs;
  }

  readonly clock: TriageClock = { now: () => this.nowMs };

  readonly scheduler: TriageScheduler = {
    after: (ms, fn) => {
      const id = this.nextId++;
      this.timers.set(id, { fireAt: this.nowMs + ms, intervalMs: null, fn });
      return () => {
        this.timers.delete(id);
      };
    },
    every: (ms, fn) => {
      const id = this.nextId++;
      this.timers.set(id, { fireAt: this.nowMs + ms, intervalMs: ms, fn });
      return () => {
        this.timers.delete(id);
      };
    },
  };

  async advance(ms: number): Promise<void> {
    const target = this.nowMs + ms;
    while (true) {
      let nextId: number | null = null;
      let nextAt = Infinity;
      for (const [id, t] of this.timers) {
        if (t.fireAt <= target && t.fireAt < nextAt) {
          nextAt = t.fireAt;
          nextId = id;
        }
      }
      if (nextId === null) break;
      const entry = this.timers.get(nextId)!;
      this.nowMs = entry.fireAt;
      if (entry.intervalMs !== null) {
        entry.fireAt += entry.intervalMs;
      } else {
        this.timers.delete(nextId);
      }
      entry.fn();
      await new Promise((r) => setTimeout(r, 10));
    }
    this.nowMs = target;
    await new Promise((r) => setTimeout(r, 10));
  }
}

function listTriageTables(daemon: HarnessDaemon): string[] {
  const rows = daemon.db.db
    .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'inbox_triage_%' ORDER BY name ASC")
    .all() as Array<{ name: string }>;
  return rows.map((r) => r.name);
}

function listTriageEvents(daemon: HarnessDaemon): Array<{ type: string; run_id: string }> {
  return daemon.db.db
    .prepare("SELECT type, run_id FROM events WHERE type LIKE 'inbox.triage.%' ORDER BY seq ASC")
    .all() as Array<{ type: string; run_id: string }>;
}

/** Seed a mixed set of inbox items directly in the disposable DB and compute the exact legacy expectation. */
function seedMixedInbox(daemon: HarnessDaemon, repoDir: string): InboxItem[] {
  const run = daemon.db.createRun({ goal: "Legacy parity run\nsecond line", repoRoot: repoDir, baseBranch: "main", baseSha: "abc123" });
  const t1 = daemon.db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "Orch", objective: "o", profileId: "pi:local" });
  const t2 = daemon.db.createTask({ runId: run.id, parentId: t1.id, tier: 2, kind: "worker", title: "Worker", objective: "o", profileId: "pi:local" });
  daemon.db.updateTask(t1.id, { status: "completed" });
  daemon.db.updateTask(t2.id, { status: "blocked", summary: "Blocked waiting for answer" });
  daemon.db.updateRunStatus(run.id, "failed", "Task blocked");

  const goal = daemon.director.goals.createGoal({
    repoRoot: repoDir,
    title: "Parity Goal",
    description: "Verify legacy snapshot parity",
    criteria: [{ id: "c1", kind: "behavioral", description: "done", required: true }],
  });
  daemon.director.goals.linkRunAndStart(goal.id, run.id);
  daemon.director.goals.setStatus(goal.id, "awaiting_human", "Linked run failed before goal verification");

  const escP0 = daemon.db.createEscalation({
    runId: run.id,
    fromTaskId: t2.id,
    toTaskId: null,
    severity: "P0",
    category: "blocked_dependency",
    title: "Need OAuth token",
    evidencePaths: ["docs/auth.md"],
    blocking: true,
  });
  const escAcked = daemon.db.createEscalation({
    runId: run.id,
    fromTaskId: t2.id,
    toTaskId: null,
    severity: "P1",
    category: "code_defect",
    title: "Acked escalation hidden in legacy snapshot",
    evidencePaths: ["src/a.ts"],
    blocking: false,
  });
  daemon.db.ackEscalation(escAcked.id, "Investigating", false);

  const wt = daemon.db.recordWorktree({
    runId: run.id,
    taskId: t2.id,
    path: join(repoDir, ".harness/worktrees/wt-1"),
    branch: "harness/t2",
    baseSha: "abc123",
  });
  const mq = daemon.db.enqueueMerge({ runId: run.id, taskId: t2.id, worktreeId: wt.id, branch: "harness/t2" });
  daemon.db.finishMergeItem(mq.id, "ci_failed", "Test failed on line 1\nstack trace");
  // Restore t2 to blocked after enqueueMerge set it to merging
  daemon.db.updateTask(t2.id, { status: "blocked", summary: "Blocked waiting for answer" });

  const msg = daemon.db.createMessage(run.id, t2.id, t1.id, "Which config key?");

  // Re-read exact stored timestamps to compute the exact legacy expectation verbatim from the old server.ts logic.
  const gRow = daemon.director.goals.getGoal(goal.id)!;
  const rRow = daemon.db.getRun(run.id)!;
  const t2Row = daemon.db.getTask(t2.id)!;
  const eRow = daemon.db.getEscalation(escP0.id)!;
  const mqRow = daemon.db.getMergeQueueItem(mq.id)!;
  const msgRow = daemon.db.getMessage(msg.id)!;

  const expected: InboxItem[] = [
    {
      id: `inbox:goal:${gRow.id}`,
      severity: "P0",
      kind: "goal_awaiting_human",
      title: `${gRow.id} (v${gRow.version}) awaits input: ${gRow.title}`,
      detail: gRow.lastReviewSummary ?? "Goal paused awaiting human review or decision",
      ref: { kind: "goal", id: gRow.id, runId: gRow.linkedRunId ?? undefined, label: `${gRow.id} ${gRow.title}` },
      goalId: gRow.id,
      runId: gRow.linkedRunId,
      createdAt: gRow.updatedAt,
    },
    {
      id: `inbox:esc:${eRow.id}`,
      severity: eRow.severity,
      kind: "escalation",
      title: `[${eRow.severity}/${eRow.category}] ${eRow.title}`,
      detail: `from ${eRow.fromTaskId} in ${eRow.runId}${eRow.blocking ? " · blocking" : ""}`,
      ref: { kind: "escalation", id: eRow.id, runId: eRow.runId, goalId: gRow.id, label: `${eRow.id} ${eRow.title}` },
      goalId: gRow.id,
      runId: eRow.runId,
      taskId: eRow.fromTaskId,
      createdAt: eRow.createdAt,
    },
    {
      id: `inbox:run:${rRow.id}`,
      severity: "P1",
      kind: "run_failed",
      title: `${rRow.id} failed`,
      detail: "Legacy parity run",
      ref: { kind: "run", id: rRow.id, goalId: gRow.id, label: rRow.id },
      goalId: gRow.id,
      runId: rRow.id,
      createdAt: rRow.updatedAt,
    },
    {
      id: `inbox:task:${t2Row.id}`,
      severity: "P1",
      kind: "task_blocked",
      title: `${t2Row.id} (blocked): ${t2Row.title}`,
      detail: t2Row.summary!,
      ref: { kind: "task", id: t2Row.id, runId: rRow.id, goalId: gRow.id, label: `${t2Row.id} ${t2Row.title}` },
      goalId: gRow.id,
      runId: rRow.id,
      taskId: t2Row.id,
      createdAt: t2Row.updatedAt,
    },
    {
      id: `inbox:merge:${mqRow.id}`,
      severity: "P1",
      kind: "merge_bounced",
      title: `Merge ${mqRow.id} (ci_failed): ${mqRow.branch}`,
      detail: "Test failed on line 1",
      ref: { kind: "merge", id: mqRow.id, runId: mqRow.runId, goalId: gRow.id, label: mqRow.id },
      goalId: gRow.id,
      runId: mqRow.runId,
      taskId: mqRow.taskId,
      createdAt: mqRow.finishedAt ?? mqRow.createdAt,
    },
    {
      id: `inbox:msg:${msgRow.id}`,
      severity: "P2",
      kind: "open_message",
      title: `Open message ${msgRow.id} (${msgRow.fromTaskId} -> ${msgRow.toTaskId})`,
      detail: msgRow.question.slice(0, 140),
      ref: { kind: "message", id: msgRow.id, runId: msgRow.runId, label: msgRow.id },
      runId: msgRow.runId,
      taskId: msgRow.toTaskId,
      createdAt: msgRow.createdAt,
    },
  ];
  const sevRank = { P0: 0, P1: 1, P2: 2, info: 3 };
  const kindRank = (item: InboxItem) => (item.kind === "escalation" ? 0 : 1);
  expected.sort((a, b) => sevRank[a.severity] - sevRank[b.severity] || kindRank(a) - kindRank(b) || b.createdAt - a.createdAt);
  return expected;
}

describe("Inbox triage daemon gating (off / report / host authority)", () => {
  test("default mode 'off': zero tables, zero inbox.triage.* events, work.snapshot.inbox deep-equals legacy projection", async () => {
    const repoDir = initTempGitRepo("pi-triage-gate-off-");
    const stateDir = mkdtempSync(join(tmpdir(), "pi-triage-gate-off-st-"));
    const timer = new FakeTimer();
    const cfg = makeTestConfig(repoDir, stateDir);
    expect(cfg.inbox.mode).toBe("off");

    const daemon = new HarnessDaemon(cfg, {
      customRunnerFactory: () => ({ run: async () => ({ text: "unused", turnsUsed: 1, stopReason: "completed" as const }) }),
      inboxClock: timer.clock,
      inboxScheduler: timer.scheduler,
      inboxHostname: "host-a",
    });

    let client: DaemonClient | null = null;
    try {
      const expectedLegacyInbox = seedMixedInbox(daemon, repoDir);
      const sock = await daemon.start();
      client = new DaemonClient(sock);
      await client.connect();

      await timer.advance(120_000);

      expect(daemon.triage.isAuthoritative()).toBe(false);
      expect(listTriageTables(daemon)).toEqual([]);
      expect(listTriageEvents(daemon)).toEqual([]);

      const snap = (await client.request("work.snapshot", { repoRoot: repoDir })) as {
        inbox: InboxItem[];
        inboxAuto?: InboxItem[];
      };
      expect(snap.inboxAuto).toBeUndefined();
      expect(snap.inbox).toEqual(expectedLegacyInbox);

      const listed = (await client.request("inbox.triage.list", { journal: true })) as InboxTriageListResult;
      expect(listed.causes).toEqual([]);
      expect(listed.journal).toEqual([]);

      const rearm = (await client.request("inbox.triage.rearm", { causeId: "escalation:esc-1" })) as InboxTriageRearmResult;
      expect(rearm.rearmed).toBe(false);

      // Still zero triage tables after all read/rearm RPCs in off mode
      expect(listTriageTables(daemon)).toEqual([]);
      expect(listTriageEvents(daemon)).toEqual([]);
    } finally {
      client?.close();
      await daemon.stop();
      rmSync(repoDir, { recursive: true, force: true });
      rmSync(stateDir, { recursive: true, force: true });
    }
  });

  test("mode 'report': inbox.triage.report returns non-empty routing plan with zero writes including schema", async () => {
    const repoDir = initTempGitRepo("pi-triage-gate-rep-");
    const stateDir = mkdtempSync(join(tmpdir(), "pi-triage-gate-rep-st-"));
    const timer = new FakeTimer();
    const cfg = makeTestConfig(repoDir, stateDir);
    cfg.inbox.mode = "report";
    cfg.inbox.schedulerHost = "host-a";

    const daemon = new HarnessDaemon(cfg, {
      customRunnerFactory: () => ({ run: async () => ({ text: "unused", turnsUsed: 1, stopReason: "completed" as const }) }),
      inboxClock: timer.clock,
      inboxScheduler: timer.scheduler,
      inboxHostname: "host-a",
    });

    let client: DaemonClient | null = null;
    try {
      const expectedLegacyInbox = seedMixedInbox(daemon, repoDir);
      const sock = await daemon.start();
      client = new DaemonClient(sock);
      await client.connect();

      await timer.advance(90_000);

      const report = (await client.request("inbox.triage.report", { repoRoot: repoDir })) as InboxTriageReportResult;
      expect(report.mode).toBe("report");
      expect(report.authoritative).toBe(false);
      expect(report.rows.length).toBeGreaterThanOrEqual(6);
      expect(report.countsByRule.R02).toBe(1);
      expect(report.countsByRule.R18).toBe(1);
      // Includes acked escalation in report plan (unlike legacy work.snapshot)
      expect(report.rows.some((r) => r.rule === "R05")).toBe(true);

      // Zero writes including schema/DDL
      expect(listTriageTables(daemon)).toEqual([]);
      expect(listTriageEvents(daemon)).toEqual([]);

      // work.snapshot remains legacy in report mode
      const snap = (await client.request("work.snapshot", { repoRoot: repoDir })) as {
        inbox: InboxItem[];
        inboxAuto?: InboxItem[];
      };
      expect(snap.inboxAuto).toBeUndefined();
      expect(snap.inbox).toEqual(expectedLegacyInbox);
    } finally {
      client?.close();
      await daemon.stop();
      rmSync(repoDir, { recursive: true, force: true });
      rmSync(stateDir, { recursive: true, force: true });
    }
  });

  test("mode 'on' with schedulerHost mismatch stays disarmed with zero writes; live config reload arms & disarms cleanly", async () => {
    const repoDir = initTempGitRepo("pi-triage-gate-host-");
    const stateDir = mkdtempSync(join(tmpdir(), "pi-triage-gate-host-st-"));
    const timer = new FakeTimer();
    const cfg = makeTestConfig(repoDir, stateDir);
    cfg.inbox.mode = "on";
    cfg.inbox.schedulerHost = "other-host.local";

    const daemon = new HarnessDaemon(cfg, {
      customRunnerFactory: () => ({ run: async () => ({ text: "unused", turnsUsed: 1, stopReason: "completed" as const }) }),
      inboxClock: timer.clock,
      inboxScheduler: timer.scheduler,
      inboxHostname: "this-host.local",
    });

    let client: DaemonClient | null = null;
    try {
      seedMixedInbox(daemon, repoDir);
      const sock = await daemon.start();
      client = new DaemonClient(sock);
      await client.connect();

      await timer.advance(120_000);

      expect(daemon.triage.isAuthoritative()).toBe(false);
      expect(listTriageTables(daemon)).toEqual([]);
      expect(listTriageEvents(daemon)).toEqual([]);

      // Report still works read-only with zero writes
      const rep = (await client.request("inbox.triage.report", { repoRoot: repoDir })) as InboxTriageReportResult;
      expect(rep.mode).toBe("on");
      expect(rep.authoritative).toBe(false);
      expect(rep.rows.length).toBeGreaterThanOrEqual(6);
      expect(listTriageTables(daemon)).toEqual([]);

      // Live config.update setting schedulerHost to this host activates triage & creates schema lazily
      await client.request("config.update", {
        patch: { inbox: { schedulerHost: "this-host.local" } },
      });
      expect(daemon.triage.isAuthoritative()).toBe(true);
      expect(listTriageTables(daemon).sort()).toEqual([...INBOX_TRIAGE_TABLES].sort());

      await timer.advance(5_000);
      const listed = (await client.request("inbox.triage.list", {})) as InboxTriageListResult;
      expect(listed.causes.length).toBeGreaterThan(0);

      // Live config.update flipping mode back to 'off' disarms synchronously
      await client.request("config.update", {
        patch: { inbox: { mode: "off" } },
      });
      expect(daemon.triage.isAuthoritative()).toBe(false);
    } finally {
      client?.close();
      await daemon.stop();
      rmSync(repoDir, { recursive: true, force: true });
      rmSync(stateDir, { recursive: true, force: true });
    }
  });
});
