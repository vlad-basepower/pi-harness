import { describe, expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ModelCatalogCompiler } from "../src/catalog/compiler.js";
import { loadConfig } from "../src/config.js";
import { makeCauseId } from "../src/control-plane/inbox-triage-store.js";
import type { TriageClock, TriageScheduler } from "../src/control-plane/inbox-triage.js";
import { DaemonClient } from "../src/daemon/remote.js";
import { HarnessDaemon } from "../src/daemon/server.js";
import type { InboxItem } from "../src/protocol/daemon-rpc.js";

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

  constructor(startMs = Date.now()) {
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

  async advance(ms: number, settleMs = 15): Promise<void> {
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
      if (settleMs > 0) await new Promise((r) => setTimeout(r, settleMs));
    }
    this.nowMs = target;
    if (settleMs > 0) await new Promise((r) => setTimeout(r, settleMs));
  }
}

async function waitUntil(pred: () => boolean, timeoutMs = 4000, stepMs = 20): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (pred()) return;
    await new Promise((r) => setTimeout(r, stepMs));
  }
  throw new Error(`Timed out after ${timeoutMs}ms waiting for condition`);
}

function blockingRunnerFactory(onStart?: (taskId: string) => void) {
  const releases: Array<() => void> = [];
  const factory = () => ({
    run: async (req: { taskId: string; signal?: AbortSignal }) => {
      onStart?.(req.taskId);
      await new Promise<void>((resolve) => {
        releases.push(resolve);
        req.signal?.addEventListener("abort", () => resolve(), { once: true });
      });
      return { text: "aborted", turnsUsed: 0, stopReason: "cancelled" as const };
    },
  });
  return {
    factory,
    releaseAll: () => {
      for (const r of releases) r();
    },
  };
}

describe("Inbox triage restart recovery & crash durability (c2, c4.2 — real HarnessDaemon)", () => {
  test("owner dies on daemon stop -> detect <= 60s, recover <= 120s, attempts survive restarts to budget 3 then park + 1 ask, +2h adds nothing", async () => {
    const repoDir = initTempGitRepo("pi-harness-triage-restart-repo-");
    const stateDir = mkdtempSync(join(tmpdir(), "pi-harness-triage-restart-state-"));
    const timer = new FakeTimer(Date.now());
    const makeCfg = () => {
      const cfg = makeTestConfig(repoDir, stateDir);
      cfg.inbox = {
        ...cfg.inbox,
        mode: "on",
        schedulerHost: "host-restart",
        batchProfileId: "pi:local",
        batchModel: "coordinator",
        maxAutoAttempts: 3,
      };
      return cfg;
    };

    const startedTasks: string[] = [];
    const runnerA = blockingRunnerFactory((tid) => startedTasks.push(tid));
    const daemonA = new HarnessDaemon(makeCfg(), {
      customRunnerFactory: runnerA.factory,
      inboxClock: timer.clock,
      inboxScheduler: timer.scheduler,
      inboxHostname: "host-restart",
    });
    let daemonB: HarnessDaemon | null = null;
    let daemonC: HarnessDaemon | null = null;
    let daemonD: HarnessDaemon | null = null;

    try {
      // Seed a settled run with a nonblocking P1 security_risk escalation (R05 -> rung-B batch)
      const r1 = daemonA.db.createRun({ goal: "Settled security run", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const t1 = daemonA.db.createTask({ runId: r1.id, tier: 1, kind: "orchestrator", title: "O1", objective: "o", profileId: "pi:local" });
      daemonA.db.updateTask(t1.id, { status: "completed" });
      daemonA.db.updateRunStatus(r1.id, "completed");
      const esc = daemonA.db.createEscalation({
        runId: r1.id,
        fromTaskId: t1.id,
        toTaskId: null,
        severity: "P1",
        category: "security_risk",
        title: "Verify token expiry header",
        evidencePaths: ["README.md"],
        blocking: false,
      });
      const cid = makeCauseId("escalation", esc.id);

      // Daemon A: start and advance 5s -> batch #1 owns the cause (attempts = 1)
      await daemonA.start();
      await timer.advance(5_000);
      await waitUntil(() => startedTasks.length === 1);
      const cA = daemonA.triageStore.getCause(cid)!;
      expect(cA.state).toBe("owned");
      expect(cA.ownerKind).toBe("batch");
      expect(cA.attempts).toBe(1);
      const firstBatchTaskId = cA.ownerId!;
      expect(firstBatchTaskId).toBeTruthy();

      // Kill Daemon A while batch #1 is still running (simulates daemon restart)
      await daemonA.stop();
      runnerA.releaseAll();

      // Daemon B starts on the same stateDir (same SQLite DB)
      const restart1At = timer.nowMs;
      const runnerB = blockingRunnerFactory((tid) => startedTasks.push(tid));
      daemonB = new HarnessDaemon(makeCfg(), {
        customRunnerFactory: runnerB.factory,
        inboxClock: timer.clock,
        inboxScheduler: timer.scheduler,
        inboxHostname: "host-restart",
      });
      await daemonB.start();
      // Startup reconciliation marked batch #1 task as failed ("Interrupted by daemon restart")
      expect(daemonB.db.getTask(firstBatchTaskId)?.status).toBe("failed");

      // Advance 5s: owner loss detected <= 60s AND cause durably reassigned <= 120s
      await timer.advance(5_000);
      await waitUntil(() => startedTasks.length === 2);
      const detectSec = (timer.nowMs - restart1At) / 1000;
      const recoverSec = (timer.nowMs - restart1At) / 1000;
      expect(detectSec).toBeLessThanOrEqual(60);
      expect(recoverSec).toBeLessThanOrEqual(120);

      const cB = daemonB.triageStore.getCause(cid)!;
      expect(cB.state).toBe("owned");
      expect(cB.ownerKind).toBe("batch");
      expect(cB.ownerId).not.toBe(firstBatchTaskId);
      expect(cB.attempts).toBe(2); // attempts counted across restarts!

      const lostJournalsB = daemonB.triageStore.listJournal({ causeId: cid, type: "owner_lost" });
      expect(lostJournalsB.length).toBe(1);
      const effectsB = daemonB.triageStore.listEffectsForCause(cid);
      expect(effectsB).toHaveLength(2); // 1 from A + exactly 1 new recovery effect from B
      const assignsB = daemonB.db.db
        .prepare("SELECT COUNT(*) AS cnt FROM inbox_triage_assignment")
        .get() as { cnt: number };
      expect(assignsB.cnt).toBe(2);

      // Kill Daemon B -> Daemon C launches attempt #3
      await daemonB.stop();
      runnerB.releaseAll();
      const runnerC = blockingRunnerFactory((tid) => startedTasks.push(tid));
      daemonC = new HarnessDaemon(makeCfg(), {
        customRunnerFactory: runnerC.factory,
        inboxClock: timer.clock,
        inboxScheduler: timer.scheduler,
        inboxHostname: "host-restart",
      });
      await daemonC.start();
      await timer.advance(5_000);
      await waitUntil(() => startedTasks.length === 3);
      const cC = daemonC.triageStore.getCause(cid)!;
      expect(cC.state).toBe("owned");
      expect(cC.attempts).toBe(3);

      // Kill Daemon C -> Daemon D detects loss at budget 3 -> PARKED + exactly 1 scope_decision ask!
      await daemonC.stop();
      runnerC.releaseAll();
      const runnerD = blockingRunnerFactory((tid) => startedTasks.push(tid));
      daemonD = new HarnessDaemon(makeCfg(), {
        customRunnerFactory: runnerD.factory,
        inboxClock: timer.clock,
        inboxScheduler: timer.scheduler,
        inboxHostname: "host-restart",
      });
      await daemonD.start();
      await timer.advance(5_000);

      const cD = daemonD.triageStore.getCause(cid)!;
      expect(cD.state).toBe("parked");
      expect(cD.attempts).toBe(3);
      expect(cD.lastActionResult).toContain("attempts_exhausted(3/3)");
      expect(startedTasks.length).toBe(3); // no 4th attempt

      const asksAfterPark = daemonD.triageStore.listActiveAsks();
      expect(asksAfterPark).toHaveLength(1);
      expect(asksAfterPark[0]!.kind).toBe("scope_decision");

      const effectsBefore2h = daemonD.triageStore.listEffectsForCause(cid).length;
      const asksBefore2h = (
        daemonD.db.db.prepare("SELECT COUNT(*) AS cnt FROM inbox_triage_ask").get() as { cnt: number }
      ).cnt;
      const notifiedBefore2h = daemonD.triageStore.listJournal({ type: "notified" }).length;

      // Advance +2 hours (7,200,000 ms) in fast ticks: adds NOTHING!
      await timer.advance(2 * 60 * 60 * 1000, 0);
      await new Promise((r) => setTimeout(r, 30));

      expect(startedTasks.length).toBe(3);
      expect(daemonD.triageStore.listEffectsForCause(cid).length).toBe(effectsBefore2h);
      expect(
        (daemonD.db.db.prepare("SELECT COUNT(*) AS cnt FROM inbox_triage_ask").get() as { cnt: number }).cnt,
      ).toBe(asksBefore2h);
      expect(daemonD.triageStore.listJournal({ type: "notified" }).length).toBe(notifiedBefore2h);
      runnerD.releaseAll();
    } finally {
      await daemonD?.stop();
      await daemonC?.stop();
      await daemonB?.stop();
      await daemonA.stop();
      rmSync(repoDir, { recursive: true, force: true });
      rmSync(stateDir, { recursive: true, force: true });
    }
  });

  test("restartRecovery 'handoff' folds goal + failed run + interrupted tasks into ONE human ask and resumes nothing", async () => {
    const repoDir = initTempGitRepo("pi-harness-triage-handoff-repo-");
    const stateDir = mkdtempSync(join(tmpdir(), "pi-harness-triage-handoff-state-"));
    const timer = new FakeTimer(Date.now());
    const cfg = makeTestConfig(repoDir, stateDir);
    cfg.inbox = {
      ...cfg.inbox,
      mode: "on",
      schedulerHost: "host-handoff",
      restartRecovery: "handoff",
      batchProfileId: "pi:local",
      batchModel: "coordinator",
    };

    let runnerStarts = 0;
    const daemon = new HarnessDaemon(cfg, {
      customRunnerFactory: () => ({
        run: async () => {
          runnerStarts += 1;
          return { text: "should not run", turnsUsed: 0, stopReason: "completed" as const };
        },
      }),
      inboxClock: timer.clock,
      inboxScheduler: timer.scheduler,
      inboxHostname: "host-handoff",
    });
    let client: DaemonClient | null = null;

    try {
      // Seed goal + active run + 3 running tasks simulating pre-restart state
      const goal = daemon.director.goals.createGoal({
        repoRoot: repoDir,
        title: "Handoff Goal",
        description: "Complete feature before restart",
        criteria: [{ id: "c1", kind: "behavioral", description: "Done" }],
      });
      const run = daemon.db.createRun({ goal: goal.title, repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      daemon.director.goals.linkRunAndStart(goal.id, run.id);
      const tOrch = daemon.db.createTask({
        runId: run.id,
        tier: 1,
        kind: "orchestrator",
        title: "Orchestrator",
        objective: "coord",
        profileId: "pi:local",
      });
      const tW1 = daemon.db.createTask({
        runId: run.id,
        parentId: tOrch.id,
        tier: 2,
        kind: "worker",
        title: "Worker 1",
        objective: "w1",
        profileId: "pi:local",
      });
      const tW2 = daemon.db.createTask({
        runId: run.id,
        parentId: tOrch.id,
        tier: 2,
        kind: "worker",
        title: "Worker 2",
        objective: "w2",
        profileId: "pi:local",
      });
      daemon.db.updateTask(tOrch.id, { status: "running" });
      daemon.db.updateTask(tW1.id, { status: "running" });
      daemon.db.updateTask(tW2.id, { status: "running" });

      // Start daemon -> startup reconciliation marks all 3 tasks failed ("Interrupted by daemon restart") & run failed (cause: daemon_restart)
      const sock = await daemon.start();
      client = new DaemonClient(sock);
      await client.connect();

      await timer.advance(5_000);
      await timer.advance(30_000);

      // Nothing resumed!
      expect(runnerStarts).toBe(0);
      const resumeEffects = daemon.db.db
        .prepare("SELECT COUNT(*) AS cnt FROM inbox_triage_effect WHERE primitive IN ('resume_run', 'resume_goal', 'resume_task')")
        .get() as { cnt: number };
      expect(resumeEffects.cnt).toBe(0);

      // Exactly ONE human ask in snapshot.inbox folding goal + failed run + interrupted tasks
      const snap = (await client.request("work.snapshot", { repoRoot: repoDir })) as {
        inbox: InboxItem[];
      };
      expect(snap.inbox).toHaveLength(1);
      const askItem = snap.inbox[0]!;
      expect(askItem.ask).toBeTruthy();
      expect(askItem.blockedWork).toBeTruthy();
      const refIds = (askItem.sourceRefs ?? []).map((r) => r.id);
      expect(refIds).toContain(run.id);
      expect(refIds).toContain(goal.id);
    } finally {
      client?.close();
      await daemon.stop();
      rmSync(repoDir, { recursive: true, force: true });
      rmSync(stateDir, { recursive: true, force: true });
    }
  });

  test("restartRecovery 'resume' performs exactly one in-place resume of the same run, never a fresh run", async () => {
    const repoDir = initTempGitRepo("pi-harness-triage-resume-repo-");
    const stateDir = mkdtempSync(join(tmpdir(), "pi-harness-triage-resume-state-"));
    const timer = new FakeTimer(Date.now());
    const cfg = makeTestConfig(repoDir, stateDir);
    cfg.inbox = {
      ...cfg.inbox,
      mode: "on",
      schedulerHost: "host-resume",
      restartRecovery: "resume",
      batchProfileId: "pi:local",
      batchModel: "coordinator",
    };

    const resumedRuns: string[] = [];
    const daemon = new HarnessDaemon(cfg, {
      customRunnerFactory: () => ({
        run: async (req) => {
          if (req.runId.startsWith("director-tasks-inbox-triage")) {
            const causeIds = [...req.initialPrompt.matchAll(/^- ([a-z_]+:[^\s(]+)/gm)].map((m) => m[1]!);
            const outcomes = causeIds.map((cid) => ({
              cause_id: cid,
              outcome: "undetermined",
              evidence: ["inspected"],
              next_action: "review",
            }));
            const text = `<triage_outcomes>${JSON.stringify(outcomes)}</triage_outcomes>`;
            await req.executeTool("return_task", {
              status: "completed",
              summary: text,
              sources: [{ path: "README.md", note: "batch" }],
            });
            return { text, turnsUsed: 1, stopReason: "completed" as const };
          }
          resumedRuns.push(req.runId);
          return { text: "Resumed in-place successfully", turnsUsed: 1, stopReason: "completed" as const };
        },
      }),
      inboxClock: timer.clock,
      inboxScheduler: timer.scheduler,
      inboxHostname: "host-resume",
    });

    try {
      // Case A: resumable restart-interrupted goal + run (same run must be resumed in place once)
      const gRes = daemon.director.goals.createGoal({
        repoRoot: repoDir,
        title: "Resumable Goal",
        description: "Continue in place after restart",
        criteria: [{ id: "c1", kind: "behavioral", description: "Done" }],
      });
      const rRes = daemon.db.createRun({ goal: gRes.title, repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      daemon.director.goals.linkRunAndStart(gRes.id, rRes.id);
      const tOrch = daemon.db.createTask({
        runId: rRes.id,
        tier: 1,
        kind: "orchestrator",
        title: "O-Res",
        objective: "resume me",
        profileId: "pi:local",
        model: "coordinator",
      });
      daemon.db.updateTask(tOrch.id, { status: "running" });

      // Case B: goal linked to a failed run whose tasks are all completed (no resumable task -> fresh run must be refused!)
      const gNoFresh = daemon.director.goals.createGoal({
        repoRoot: repoDir,
        title: "No Fresh Run Goal",
        description: "Must never start a fresh run",
        criteria: [{ id: "c1", kind: "behavioral", description: "Done" }],
      });
      const rDoneTasks = daemon.db.createRun({ goal: gNoFresh.title, repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      daemon.director.goals.linkRunAndStart(gNoFresh.id, rDoneTasks.id);
      const tDone = daemon.db.createTask({
        runId: rDoneTasks.id,
        tier: 1,
        kind: "orchestrator",
        title: "O-Done",
        objective: "already done",
        profileId: "pi:local",
        model: "coordinator",
      });
      daemon.db.updateTask(tDone.id, { status: "completed" });
      daemon.db.updateRunStatus(rDoneTasks.id, "failed", "Interrupted by daemon restart", "daemon_restart");

      const nonBatchRunsBefore = (
        daemon.db.db.prepare("SELECT COUNT(*) AS cnt FROM runs WHERE id NOT LIKE 'director-tasks-inbox-triage%'").get() as { cnt: number }
      ).cnt;

      await daemon.start();
      await timer.advance(5_000);
      await waitUntil(() => resumedRuns.length === 1 && daemon.db.getTask(tOrch.id)?.status === "completed", 4000);
      await timer.advance(60_000);

      // Scripted runner started ONCE on the SAME run rRes; task generation incremented 1 -> 2
      expect(resumedRuns).toEqual([rRes.id]);
      expect(daemon.db.getTask(tOrch.id)?.generation).toBe(2);

      // Zero fresh work runs created!
      const nonBatchRunsAfter = (
        daemon.db.db.prepare("SELECT COUNT(*) AS cnt FROM runs WHERE id NOT LIKE 'director-tasks-inbox-triage%'").get() as { cnt: number }
      ).cnt;
      expect(nonBatchRunsAfter).toBe(nonBatchRunsBefore);
      expect(daemon.director.goals.getGoal(gNoFresh.id)?.linkedRunId).toBe(rDoneTasks.id);

      // Exactly 1 resume_goal effect for rRes
      const resEffects = daemon.triageStore.listEffectsForCause(makeCauseId("run_failed", rRes.id));
      expect(resEffects.filter((e) => e.primitive === "resume_goal")).toHaveLength(1);
    } finally {
      await daemon.stop();
      rmSync(repoDir, { recursive: true, force: true });
      rmSync(stateDir, { recursive: true, force: true });
    }
  });

  test("crash between effect intent and dispatch, and between dispatch and receipt -> zero duplicate dispatches", async () => {
    const repoDir = initTempGitRepo("pi-harness-triage-crash-repo-");
    const stateDirA = mkdtempSync(join(tmpdir(), "pi-harness-triage-crash-a-"));
    const stateDirB = mkdtempSync(join(tmpdir(), "pi-harness-triage-crash-b-"));

    // --- Sub-scenario A: Crash between intent and dispatch (onAfterClaim throws) ---
    const timerA = new FakeTimer(Date.now());
    const makeCfgA = () => {
      const cfg = makeTestConfig(repoDir, stateDirA);
      cfg.inbox = {
        ...cfg.inbox,
        mode: "on",
        schedulerHost: "host-crash-a",
        restartRecovery: "resume",
        batchProfileId: "pi:local",
        batchModel: "coordinator",
      };
      return cfg;
    };

    let runnerStartsA = 0;
    const dA1 = new HarnessDaemon(makeCfgA(), {
      customRunnerFactory: () => ({
        run: async () => {
          runnerStartsA += 1;
          return { text: "Resumed cleanly after intent crash", turnsUsed: 1, stopReason: "completed" as const };
        },
      }),
      inboxClock: timerA.clock,
      inboxScheduler: timerA.scheduler,
      inboxHostname: "host-crash-a",
      inboxHooks: {
        onAfterClaim: () => {
          throw new Error("CRASH_BETWEEN_INTENT_AND_DISPATCH");
        },
      },
    });
    let dA2: HarnessDaemon | null = null;

    // --- Sub-scenario B: Crash between dispatch and receipt (onBeforeReceipt throws) ---
    const timerB = new FakeTimer(Date.now());
    const makeCfgB = () => {
      const cfg = makeTestConfig(repoDir, stateDirB);
      cfg.inbox = {
        ...cfg.inbox,
        mode: "on",
        schedulerHost: "host-crash-b",
        restartRecovery: "resume",
        batchProfileId: "pi:local",
        batchModel: "coordinator",
      };
      return cfg;
    };

    let resumeStartsB = 0;
    let batchStartsB = 0;
    const dB1 = new HarnessDaemon(makeCfgB(), {
      customRunnerFactory: () => ({
        run: async (req) => {
          if (req.runId.startsWith("director-tasks-inbox-triage")) {
            batchStartsB += 1;
            const causeIds = [...req.initialPrompt.matchAll(/^- ([a-z_]+:[^\s(]+)/gm)].map((m) => m[1]!);
            const outcomes = causeIds.map((cid) => ({
              cause_id: cid,
              outcome: "remediation",
              evidence: ["inspected"],
              next_action: "done",
            }));
            const text = `<triage_outcomes>${JSON.stringify(outcomes)}</triage_outcomes>`;
            await req.executeTool("return_task", {
              status: "completed",
              summary: text,
              sources: [{ path: "README.md", note: "batch ok" }],
            });
            return { text, turnsUsed: 1, stopReason: "completed" as const };
          }
          resumeStartsB += 1;
          return { text: "Resumed once before receipt crash", turnsUsed: 1, stopReason: "completed" as const };
        },
      }),
      inboxClock: timerB.clock,
      inboxScheduler: timerB.scheduler,
      inboxHostname: "host-crash-b",
      inboxHooks: {
        onBeforeReceipt: () => {
          throw new Error("CRASH_BETWEEN_DISPATCH_AND_RECEIPT");
        },
      },
    });
    let dB2: HarnessDaemon | null = null;

    try {
      // Sub-scenario A: seed standalone restart-interrupted run
      const rA = dA1.db.createRun({ goal: "Crash intent run", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const tA = dA1.db.createTask({
        runId: rA.id,
        tier: 1,
        kind: "orchestrator",
        title: "OA",
        objective: "o",
        profileId: "pi:local",
        model: "coordinator",
      });
      dA1.db.updateTask(tA.id, { status: "running" });

      await dA1.start();
      timerA.nowMs += 5_000;
      await expect(dA1.triage.sweepOnce()).rejects.toThrow("CRASH_BETWEEN_INTENT_AND_DISPATCH");
      // Crash happened before dispatch: 1 'intent' row, 0 runner executions
      expect(runnerStartsA).toBe(0);
      const effectsA1 = dA1.triageStore.listEffectsForCause(makeCauseId("run_failed", rA.id));
      expect(effectsA1).toHaveLength(1);
      expect(effectsA1[0]!.status).toBe("intent");
      await dA1.stop();

      // Restart Daemon A2 without crash hook
      dA2 = new HarnessDaemon(makeCfgA(), {
        customRunnerFactory: () => ({
          run: async () => {
            runnerStartsA += 1;
            return { text: "Resumed cleanly after intent crash", turnsUsed: 1, stopReason: "completed" as const };
          },
        }),
        inboxClock: timerA.clock,
        inboxScheduler: timerA.scheduler,
        inboxHostname: "host-crash-a",
      });
      await dA2.start();
      // First tick reconciles 'intent' -> 'failed' ("interrupted_before_dispatch"); backoff holds retry for 60s
      await timerA.advance(5_000);
      const effAfterReconcile = dA2.triageStore.listEffectsForCause(makeCauseId("run_failed", rA.id));
      expect(effAfterReconcile[0]!.status).toBe("failed");
      expect(effAfterReconcile[0]!.receipt).toBe("interrupted_before_dispatch");
      expect(runnerStartsA).toBe(0);

      // Advance past 1m backoff -> exactly ONE real dispatch executes and completes!
      await timerA.advance(65_000);
      await waitUntil(() => runnerStartsA === 1 && dA2!.db.getTask(tA.id)?.status === "completed", 4000);
      await timerA.advance(60_000);
      expect(runnerStartsA).toBe(1);

      // Sub-scenario B1: crash between dispatch and receipt on resume_run
      const rB1 = dB1.db.createRun({ goal: "Crash receipt resume run", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const tB1 = dB1.db.createTask({
        runId: rB1.id,
        tier: 1,
        kind: "orchestrator",
        title: "OB1",
        objective: "o",
        profileId: "pi:local",
        model: "coordinator",
      });
      dB1.db.updateTask(tB1.id, { status: "running" });

      await dB1.start();
      timerB.nowMs += 5_000;
      await expect(dB1.triage.sweepOnce()).rejects.toThrow("CRASH_BETWEEN_DISPATCH_AND_RECEIPT");
      // resume_run dispatched for real and completed rB1 even though onBeforeReceipt threw!
      await waitUntil(() => resumeStartsB === 1 && dB1.db.getRun(rB1.id)?.status === "completed", 4000);
      const effResumeB1 = dB1.triageStore.listEffectsForCause(makeCauseId("run_failed", rB1.id));
      expect(effResumeB1).toHaveLength(1);
      expect(effResumeB1[0]!.status).toBe("dispatched");

      // Now also seed a settled-run escalation (launch_batch) and trigger crash-before-receipt on it
      const rB2 = dB1.db.createRun({ goal: "Crash receipt batch run", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const tB2 = dB1.db.createTask({ runId: rB2.id, tier: 1, kind: "orchestrator", title: "OB2", objective: "o", profileId: "pi:local" });
      dB1.db.updateTask(tB2.id, { status: "completed" });
      dB1.db.updateRunStatus(rB2.id, "completed");
      const escB = dB1.db.createEscalation({
        runId: rB2.id,
        fromTaskId: tB2.id,
        toTaskId: null,
        severity: "P1",
        category: "security_risk",
        title: "Batch receipt crash test",
        evidencePaths: ["README.md"],
        blocking: false,
      });

      timerB.nowMs += 30_000;
      await expect(dB1.triage.sweepOnce()).rejects.toThrow("CRASH_BETWEEN_DISPATCH_AND_RECEIPT");
      await waitUntil(() => batchStartsB === 1 && dB1.triageStore.listOpenAssignments("batch").length === 0, 4000);
      const effBatchB1 = dB1.triageStore.listEffectsForCause(makeCauseId("escalation", escB.id));
      expect(effBatchB1).toHaveLength(1);
      expect(effBatchB1[0]!.status).toBe("dispatched");
      await dB1.stop();

      // Restart Daemon B2 without crash hook
      dB2 = new HarnessDaemon(makeCfgB(), {
        customRunnerFactory: () => ({
          run: async () => {
            resumeStartsB += 100; // would fail assertion immediately if duplicated!
            batchStartsB += 100;
            return { text: "DUPLICATE!", turnsUsed: 1, stopReason: "completed" as const };
          },
        }),
        inboxClock: timerB.clock,
        inboxScheduler: timerB.scheduler,
        inboxHostname: "host-crash-b",
      });
      await dB2.start();
      await timerB.advance(5_000);
      await timerB.advance(60_000);

      // Postcondition verification confirmed BOTH dispatched effects -> ZERO duplicate dispatches!
      expect(resumeStartsB).toBe(1);
      expect(batchStartsB).toBe(1);
      const effResumeB2 = dB2.triageStore.listEffectsForCause(makeCauseId("run_failed", rB1.id));
      expect(effResumeB2).toHaveLength(1);
      expect(effResumeB2[0]!.status).toBe("confirmed");
      const effBatchB2 = dB2.triageStore.listEffectsForCause(makeCauseId("escalation", escB.id));
      expect(effBatchB2).toHaveLength(1);
      expect(effBatchB2[0]!.status).toBe("confirmed");
    } finally {
      await dB2?.stop();
      await dB1.stop();
      await dA2?.stop();
      await dA1.stop();
      rmSync(repoDir, { recursive: true, force: true });
      rmSync(stateDirA, { recursive: true, force: true });
      rmSync(stateDirB, { recursive: true, force: true });
    }
  });
});
