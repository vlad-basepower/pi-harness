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
import type { InboxItem, InboxTriageListResult } from "../src/protocol/daemon-rpc.js";

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
      let nextAt = target + 1;
      for (const [id, entry] of this.timers) {
        if (entry.fireAt <= target && entry.fireAt < nextAt) {
          nextAt = entry.fireAt;
          nextId = id;
        }
      }
      if (nextId === null) break;
      const entry = this.timers.get(nextId)!;
      this.nowMs = entry.fireAt;
      if (entry.intervalMs !== null) {
        entry.fireAt = this.nowMs + entry.intervalMs;
      } else {
        this.timers.delete(nextId);
      }
      entry.fn();
      for (let i = 0; i < 6; i++) await Promise.resolve();
      await new Promise((r) => setTimeout(r, 15));
    }
    this.nowMs = target;
    for (let i = 0; i < 6; i++) await Promise.resolve();
    await new Promise((r) => setTimeout(r, 15));
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

describe("Inbox triage human surface, RPC resolution, Amendment 1, windowing & stopped runs (c3, c4.3 — real HarnessDaemon)", () => {
  test("P0 blocker + blocked task: surfaces ask/blockedWork/sourceRefs, stays through 10 ticks + restart with 1 ask & 1 notified, resolve without continue clears <= 60s and holds task", async () => {
    const repoDir = initTempGitRepo("pi-triage-human-1-");
    const stateDir = mkdtempSync(join(tmpdir(), "pi-triage-human-1-st-"));
    const timer = new FakeTimer();
    const makeCfg = () => {
      const cfg = makeTestConfig(repoDir, stateDir);
      cfg.inbox = {
        ...cfg.inbox,
        mode: "on",
        schedulerHost: "host-human",
        batchProfileId: "pi:local",
        batchModel: "coordinator",
      };
      return cfg;
    };

    let nonBatchStarts = 0;
    let batchStarts = 0;
    const daemonA = new HarnessDaemon(makeCfg(), {
      customRunnerFactory: () => ({
        run: async (req) => {
          if (req.runId.startsWith("director-tasks-inbox-triage")) {
            batchStarts += 1;
          } else {
            nonBatchStarts += 1;
          }
          return { text: "unexpected run", turnsUsed: 1, stopReason: "completed" as const };
        },
      }),
      inboxClock: timer.clock,
      inboxScheduler: timer.scheduler,
      inboxHostname: "host-human",
    });

    let clientA: DaemonClient | null = null;
    let daemonB: HarnessDaemon | null = null;
    let clientB: DaemonClient | null = null;

    try {
      const goal = daemonA.director.goals.createGoal({
        repoRoot: repoDir,
        title: "Schema Migration Goal",
        description: "Migrate billing schema safely",
        criteria: [{ id: "c1", kind: "behavioral", description: "Migration verified", required: true }],
      });
      const run = daemonA.db.createRun({ goal: goal.title, repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      daemonA.director.goals.linkRunAndStart(goal.id, run.id);

      const tBlocked = daemonA.db.createTask({
        runId: run.id,
        tier: 1,
        kind: "orchestrator",
        title: "Billing Migrator",
        objective: "Run migration",
        profileId: "pi:local",
        model: "coordinator",
      });
      const esc = daemonA.db.createEscalation({
        runId: run.id,
        fromTaskId: tBlocked.id,
        toTaskId: null,
        severity: "P0",
        category: "ambiguous_spec",
        title: "Approve online vs lock-table migration strategy",
        evidencePaths: ["README.md"],
        blocking: true,
      });
      daemonA.db.updateTask(tBlocked.id, {
        status: "blocked",
        summary: `Blocked on ${esc.id}: awaiting migration strategy`,
      });
      daemonA.db.updateRunStatus(run.id, "failed", `Blocked on ${esc.id}`, "blocked_escalation");
      daemonA.director.goals.setStatus(goal.id, "awaiting_human", `Linked run ${run.id} failed before goal verification`);

      const sockA = await daemonA.start();
      clientA = new DaemonClient(sockA);
      await clientA.connect();

      const startAt = timer.nowMs;
      await timer.advance(5_000);
      const ackDeltaMs = timer.nowMs - startAt;
      expect(ackDeltaMs).toBeLessThanOrEqual(60_000);

      // Snapshot surfaces exact P0 blocker with ask, blockedWork, sourceRefs
      const snap1 = (await clientA.request("work.snapshot", { repoRoot: repoDir })) as {
        inbox: InboxItem[];
        inboxAuto?: InboxItem[];
      };
      const escCauseId = makeCauseId("escalation", esc.id);
      const blockerItem = snap1.inbox.find((i) => i.triage?.causeId === escCauseId);
      expect(blockerItem).toBeDefined();
      expect(blockerItem!.severity).toBe("P0");
      expect(blockerItem!.triage?.state).toBe("surfaced");
      expect(blockerItem!.triage?.disposition).toBe("human_blocker");
      expect(typeof blockerItem!.ask).toBe("string");
      expect(blockerItem!.ask!.length).toBeGreaterThan(0);
      expect(typeof blockerItem!.blockedWork).toBe("string");
      expect(blockerItem!.blockedWork!.length).toBeGreaterThan(0);
      expect(blockerItem!.sourceRefs && blockerItem!.sourceRefs.length).toBeGreaterThanOrEqual(1);
      expect(blockerItem!.sourceRefs!.some((r) => r.id === esc.id)).toBe(true);
      expect(blockerItem!.sourceRefs!.some((r) => r.id === tBlocked.id)).toBe(true);

      // Verify exactly 1 ask row & 1 notified journal row
      const asksInitial = daemonA.triageStore.listActiveAsks("blocker");
      expect(asksInitial).toHaveLength(1);
      const askId = asksInitial[0]!.askId;
      const notifiedInitial = daemonA.triageStore
        .listJournal({ causeId: escCauseId, limit: 100 })
        .filter((j) => j.type === "notified");
      expect(notifiedInitial).toHaveLength(1);

      // Advance 10 full sweep ticks (300s): never auto-resolved, goal never verified/accepted, still 1 ask & 1 notified
      for (let tick = 0; tick < 10; tick++) {
        await timer.advance(30_000);
      }
      expect(daemonA.db.getEscalation(esc.id)?.status).toBe("open");
      expect(daemonA.db.getTask(tBlocked.id)?.status).toBe("blocked");
      expect(daemonA.director.goals.getGoal(goal.id)?.status).toBe("awaiting_human");
      expect(daemonA.director.goals.getGoal(goal.id)?.reviewCycle).toBe(0);
      expect(daemonA.triageStore.listActiveAsks("blocker")).toHaveLength(1);
      expect(daemonA.triageStore.listActiveAsks("blocker")[0]!.askId).toBe(askId);
      expect(
        daemonA.triageStore.listJournal({ causeId: escCauseId, limit: 100 }).filter((j) => j.type === "notified"),
      ).toHaveLength(1);
      expect(nonBatchStarts).toBe(0);
      expect(batchStarts).toBe(0);

      // Restart daemon on same stateDir -> still 1 ask row & 1 notified journal row
      clientA.close();
      clientA = null;
      await daemonA.stop();

      daemonB = new HarnessDaemon(makeCfg(), {
        customRunnerFactory: () => ({
          run: async (req) => {
            if (req.runId.startsWith("director-tasks-inbox-triage")) {
              batchStarts += 1;
            } else {
              nonBatchStarts += 1;
            }
            return { text: "unexpected run after restart", turnsUsed: 1, stopReason: "completed" as const };
          },
        }),
        inboxClock: timer.clock,
        inboxScheduler: timer.scheduler,
        inboxHostname: "host-human",
      });
      const sockB = await daemonB.start();
      clientB = new DaemonClient(sockB);
      await clientB.connect();

      await timer.advance(60_000);
      expect(daemonB.triageStore.listActiveAsks("blocker")).toHaveLength(1);
      expect(daemonB.triageStore.listActiveAsks("blocker")[0]!.askId).toBe(askId);
      expect(
        daemonB.triageStore.listJournal({ causeId: escCauseId, limit: 100 }).filter((j) => j.type === "notified"),
      ).toHaveLength(1);
      expect(daemonB.db.getEscalation(esc.id)?.status).toBe("open");
      expect(daemonB.director.goals.getGoal(goal.id)?.status).toBe("awaiting_human");
      expect(daemonB.director.goals.getGoal(goal.id)?.reviewCycle).toBe(0);
      expect(nonBatchStarts).toBe(0);
      expect(batchStarts).toBe(0);

      // Resolve via RPC WITHOUT continue -> ask cleared <= 60s, task NOT resumed (held)
      const resolvedAt = timer.nowMs;
      const resNoContinue = (await clientB.request("escalation.resolve", {
        escalationId: esc.id,
        reason: "Use online dual-write migration",
      })) as { alreadyResolved: boolean; escalation: { status: string } };
      expect(resNoContinue.alreadyResolved).toBe(false);
      expect(resNoContinue.escalation.status).toBe("resolved");

      // Event nudge fires within 250ms -> advance 5s (well within <= 60s bound)
      await timer.advance(5_000);
      const clearDeltaMs = timer.nowMs - resolvedAt;
      expect(clearDeltaMs).toBeLessThanOrEqual(60_000);

      const snapCleared = (await clientB.request("work.snapshot", { repoRoot: repoDir })) as {
        inbox: InboxItem[];
      };
      expect(snapCleared.inbox.find((i) => i.triage?.causeId === escCauseId)).toBeUndefined();
      expect(daemonB.triageStore.listActiveAsks("blocker")).toHaveLength(0);
      expect(daemonB.triageStore.getCause(escCauseId)?.state).toBe("resolved");
      expect(daemonB.triageStore.getCause(escCauseId)?.outcomeKind).toBe("answered");

      // Task stays blocked/held (NOT resumed) even after 120s more; goal never accepted/verified by triage
      await timer.advance(120_000);
      expect(daemonB.db.getTask(tBlocked.id)?.status).toBe("blocked");
      expect(daemonB.db.getTask(tBlocked.id)?.generation).toBe(1);
      const taskCause = daemonB.triageStore.getCause(makeCauseId("task_blocked", tBlocked.id));
      expect(taskCause?.state).toBe("held");
      expect(daemonB.director.goals.getGoal(goal.id)?.status).toBe("awaiting_human");
      expect(daemonB.director.goals.getGoal(goal.id)?.reviewCycle).toBe(0);
      expect(nonBatchStarts).toBe(0);
    } finally {
      clientA?.close();
      clientB?.close();
      await daemonA.stop();
      if (daemonB) await daemonB.stop();
      rmSync(repoDir, { recursive: true, force: true });
      rmSync(stateDir, { recursive: true, force: true });
    }
  });

  test("RPC escalation.resolve with continue resumes blocked task once at expected generation; Amendment-1 reopen revokes grant and replay grants nothing", async () => {
    const repoDir = initTempGitRepo("pi-triage-human-2-");
    const stateDir = mkdtempSync(join(tmpdir(), "pi-triage-human-2-st-"));
    const timer = new FakeTimer();
    const cfg = makeTestConfig(repoDir, stateDir);
    cfg.inbox = {
      ...cfg.inbox,
      mode: "on",
      schedulerHost: "host-human-2",
      batchProfileId: "pi:local",
      batchModel: "coordinator",
    };

    const resumedTasks: Array<{ taskId: string; generation: number }> = [];
    const daemon: HarnessDaemon = new HarnessDaemon(cfg, {
      customRunnerFactory: () => ({
        run: async (req) => {
          const gen: number = daemon.db.getTask(req.taskId)?.generation ?? 0;
          resumedTasks.push({ taskId: req.taskId, generation: gen });
          return { text: `Resumed ${req.taskId} at gen ${gen}`, turnsUsed: 1, stopReason: "completed" as const };
        },
      }),
      inboxClock: timer.clock,
      inboxScheduler: timer.scheduler,
      inboxHostname: "host-human-2",
    });

    let client: DaemonClient | null = null;
    try {
      // Case A: resolve with continue: true -> resumes t1 exactly once at generation 1 -> 2
      const r1 = daemon.db.createRun({ goal: "Continue run", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const t1 = daemon.db.createTask({
        runId: r1.id,
        tier: 1,
        kind: "orchestrator",
        title: "Worker1",
        objective: "Finish feature",
        profileId: "pi:local",
        model: "coordinator",
      });
      const esc1 = daemon.db.createEscalation({
        runId: r1.id,
        fromTaskId: t1.id,
        toTaskId: null,
        severity: "P0",
        category: "ambiguous_spec",
        title: "Choose auth token format",
        evidencePaths: ["README.md"],
        blocking: true,
      });
      daemon.db.updateTask(t1.id, { status: "blocked", summary: `Blocked on ${esc1.id}` });

      // Case B: Amendment-1 reopen flow on r2 / t2 / esc2
      const r2 = daemon.db.createRun({ goal: "Reopen run", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const t2 = daemon.db.createTask({
        runId: r2.id,
        tier: 1,
        kind: "orchestrator",
        title: "Worker2",
        objective: "Await decision",
        profileId: "pi:local",
        model: "coordinator",
      });
      const esc2 = daemon.db.createEscalation({
        runId: r2.id,
        fromTaskId: t2.id,
        toTaskId: null,
        severity: "P0",
        category: "ambiguous_spec",
        title: "Choose billing plan",
        evidencePaths: ["README.md"],
        blocking: true,
      });
      daemon.db.updateTask(t2.id, { status: "blocked", summary: `Blocked on ${esc2.id}` });

      const sock = await daemon.start();
      client = new DaemonClient(sock);
      await client.connect();

      await timer.advance(5_000);
      expect(daemon.triageStore.listActiveAsks("blocker")).toHaveLength(2);

      // Part A: resolve esc1 with continue: true -> resumes t1 once (generation 1 -> 2)
      const res1 = (await client.request("escalation.resolve", {
        escalationId: esc1.id,
        reason: "Use JWT RS256",
        continue: true,
      })) as { alreadyResolved: boolean };
      expect(res1.alreadyResolved).toBe(false);

      await timer.advance(5_000);
      await waitUntil(() => resumedTasks.some((r) => r.taskId === t1.id) && daemon.db.getTask(t1.id)?.status === "completed", 4000);

      const t1Resumes = resumedTasks.filter((r) => r.taskId === t1.id);
      expect(t1Resumes).toHaveLength(1);
      expect(t1Resumes[0]!.generation).toBe(2); // resumed from gen 1 -> gen 2

      // Further sweeps do not re-resume t1
      await timer.advance(120_000);
      expect(resumedTasks.filter((r) => r.taskId === t1.id)).toHaveLength(1);
      expect(daemon.triageStore.getCause(makeCauseId("escalation", esc1.id))?.state).toBe("resolved");

      // Part B (Amendment 1 via RPC):
      // 1. resolve esc2 WITH continue: true (before any triage sweep fires!)
      const bRes1 = (await client.request("escalation.resolve", {
        escalationId: esc2.id,
        reason: "Initial answer",
        continue: true,
      })) as { alreadyResolved: boolean };
      expect(bRes1.alreadyResolved).toBe(false);

      // 2. reopen esc2 BEFORE dispatch via escalation.ack(resolve: false)
      const bReopen = (await client.request("escalation.ack", {
        escalationId: esc2.id,
        resolve: false,
        resolution: "Hold on, reopening",
      })) as { status: string };
      expect(bReopen.status).toBe("acked");

      // 3. resolve esc2 again WITHOUT continue
      const bRes2 = (await client.request("escalation.resolve", {
        escalationId: esc2.id,
        reason: "Final answer without continue",
      })) as { alreadyResolved: boolean; escalation: { status: string } };
      expect(bRes2.alreadyResolved).toBe(false);
      expect(bRes2.escalation.status).toBe("resolved");

      // 4. Sweep -> ZERO resumes of t2!
      await timer.advance(60_000);
      expect(resumedTasks.filter((r) => r.taskId === t2.id)).toHaveLength(0);
      expect(daemon.db.getTask(t2.id)?.status).toBe("blocked");
      expect(daemon.db.getTask(t2.id)?.generation).toBe(1);

      // 5. Repeat the last resolve WITH continue: true (replay -> alreadyResolved: true -> grants nothing)
      const bReplay = (await client.request("escalation.resolve", {
        escalationId: esc2.id,
        reason: "Replay with continue",
        continue: true,
      })) as { alreadyResolved: boolean };
      expect(bReplay.alreadyResolved).toBe(true);

      // 6. Sweep again -> STILL ZERO resumes of t2!
      await timer.advance(60_000);
      expect(resumedTasks.filter((r) => r.taskId === t2.id)).toHaveLength(0);
      expect(daemon.db.getTask(t2.id)?.status).toBe("blocked");
      expect(daemon.db.getTask(t2.id)?.generation).toBe(1);
    } finally {
      client?.close();
      await daemon.stop();
      rmSync(repoDir, { recursive: true, force: true });
      rmSync(stateDir, { recursive: true, force: true });
    }
  });

  test("goal cancel supersedes & clears blocker; surfaced blocker outside 50-run window still appears; operator-stopped run vetoes routine actions while keeping independent P0 ask surfaced", async () => {
    const repoDir = initTempGitRepo("pi-triage-human-3-");
    const stateDir = mkdtempSync(join(tmpdir(), "pi-triage-human-3-st-"));
    const timer = new FakeTimer();
    const cfg = makeTestConfig(repoDir, stateDir);
    cfg.inbox = {
      ...cfg.inbox,
      mode: "on",
      schedulerHost: "host-human-3",
      batchProfileId: "pi:local",
      batchModel: "coordinator",
    };

    let runnerStarts = 0;
    const daemon = new HarnessDaemon(cfg, {
      customRunnerFactory: () => ({
        run: async () => {
          runnerStarts += 1;
          return { text: "unexpected run", turnsUsed: 1, stopReason: "completed" as const };
        },
      }),
      inboxClock: timer.clock,
      inboxScheduler: timer.scheduler,
      inboxHostname: "host-human-3",
    });

    let client: DaemonClient | null = null;
    try {
      // Subcase A: goal-linked P0 blocker -> goal.cancel supersedes and clears it
      const gCancel = daemon.director.goals.createGoal({
        repoRoot: repoDir,
        title: "Cancellable Goal",
        description: "Goal to be cancelled",
        criteria: [{ id: "c1", kind: "behavioral", description: "Done", required: true }],
      });
      const rCancel = daemon.db.createRun({ goal: gCancel.title, repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      daemon.director.goals.linkRunAndStart(gCancel.id, rCancel.id);
      const tCancel = daemon.db.createTask({
        runId: rCancel.id,
        tier: 1,
        kind: "orchestrator",
        title: "CancelOrch",
        objective: "o",
        profileId: "pi:local",
        model: "coordinator",
      });
      const escCancel = daemon.db.createEscalation({
        runId: rCancel.id,
        fromTaskId: tCancel.id,
        toTaskId: null,
        severity: "P0",
        category: "ambiguous_spec",
        title: "Need direction for cancellable goal",
        evidencePaths: ["README.md"],
        blocking: true,
      });
      daemon.db.updateTask(tCancel.id, { status: "blocked", summary: `Blocked on ${escCancel.id}` });

      // Subcase B: surfaced blocker on an old run strictly outside the 50-run window
      const rOld = daemon.db.createRun({ goal: "Old out-of-window run", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const tOld = daemon.db.createTask({
        runId: rOld.id,
        tier: 1,
        kind: "orchestrator",
        title: "OldOrch",
        objective: "o",
        profileId: "pi:local",
        model: "coordinator",
      });
      const escOld = daemon.db.createEscalation({
        runId: rOld.id,
        fromTaskId: tOld.id,
        toTaskId: null,
        severity: "P0",
        category: "blocked_dependency",
        title: "Out-of-window blocker ask",
        evidencePaths: ["README.md"],
        blocking: true,
      });
      daemon.db.updateTask(tOld.id, { status: "blocked", summary: `Blocked on ${escOld.id}` });

      const sock = await daemon.start();
      client = new DaemonClient(sock);
      await client.connect();

      // Surface both escCancel and escOld first
      await timer.advance(5_000);
      const snapBefore = (await client.request("work.snapshot", { repoRoot: repoDir })) as { inbox: InboxItem[] };
      expect(snapBefore.inbox.some((i) => i.triage?.causeId === makeCauseId("escalation", escCancel.id))).toBe(true);
      expect(snapBefore.inbox.some((i) => i.triage?.causeId === makeCauseId("escalation", escOld.id))).toBe(true);

      // Push rOld strictly outside the 50-run window by inserting 55 newer completed runs
      for (let i = 1; i <= 55; i++) {
        const newer = daemon.db.createRun({ goal: `Newer run ${i}`, repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
        daemon.db.updateRunStatus(newer.id, "completed");
      }
      expect(daemon.db.listRuns(50).some((r) => r.id === rOld.id)).toBe(false);

      // Cancel gCancel via RPC
      await client.request("goal.cancel", { goalId: gCancel.id });

      // Subcase C: operator-stopped run with routine items + independent P0 ask
      const rStopped = daemon.db.createRun({ goal: "Operator stopped run", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const tStoppedOrch = daemon.db.createTask({
        runId: rStopped.id,
        tier: 1,
        kind: "orchestrator",
        title: "StoppedOrch",
        objective: "o",
        profileId: "pi:local",
        model: "coordinator",
      });
      const tStoppedWorker = daemon.db.createTask({
        runId: rStopped.id,
        parentId: tStoppedOrch.id,
        tier: 2,
        kind: "worker",
        title: "StoppedWorker",
        objective: "o",
        profileId: "pi:local",
        model: "coordinator",
      });
      daemon.db.updateTask(tStoppedWorker.id, { status: "failed", summary: "transient failure" });
      const escStoppedP1 = daemon.db.createEscalation({
        runId: rStopped.id,
        fromTaskId: tStoppedWorker.id,
        toTaskId: tStoppedOrch.id,
        severity: "P1",
        category: "code_defect",
        title: "Routine P1 on stopped run",
        evidencePaths: ["README.md"],
        blocking: false,
      });
      const escStoppedP0 = daemon.db.createEscalation({
        runId: rStopped.id,
        fromTaskId: tStoppedOrch.id,
        toTaskId: null,
        severity: "P0",
        category: "security_risk",
        title: "Independent P0 on stopped run",
        evidencePaths: ["README.md"],
        blocking: false,
      });
      await client.request("run.stop", { runId: rStopped.id, reason: "Stopped by operator" });

      // Advance sweeps
      await timer.advance(60_000);
      await timer.advance(60_000);

      const snapAfter = (await client.request("work.snapshot", { repoRoot: repoDir })) as {
        inbox: InboxItem[];
        inboxAuto?: InboxItem[];
      };

      // A: cancelled goal blocker is superseded and cleared from inbox
      expect(snapAfter.inbox.some((i) => i.triage?.causeId === makeCauseId("escalation", escCancel.id))).toBe(false);
      expect(daemon.triageStore.getCause(makeCauseId("escalation", escCancel.id))?.state).toBe("resolved");
      expect(daemon.db.getEscalation(escCancel.id)?.status).toBe("resolved");

      // B: surfaced blocker on rOld (outside 50-run window!) STILL appears in work.snapshot.inbox
      const oldBlocker = snapAfter.inbox.find((i) => i.triage?.causeId === makeCauseId("escalation", escOld.id));
      expect(oldBlocker).toBeDefined();
      expect(oldBlocker!.triage?.state).toBe("surfaced");
      expect(oldBlocker!.ask).toBeDefined();

      // C: operator-stopped run: ZERO routine dispatches/runners, P1 is held/vetoed, independent P0 IS surfaced!
      expect(runnerStarts).toBe(0);
      const stoppedEffects = [
        ...daemon.triageStore.listEffectsForCause(makeCauseId("escalation", escStoppedP1.id)),
        ...daemon.triageStore.listEffectsForCause(makeCauseId("task_failed", tStoppedWorker.id)),
      ];
      expect(stoppedEffects).toHaveLength(0);
      const stoppedP1Cause = daemon.triageStore.getCause(makeCauseId("escalation", escStoppedP1.id));
      expect(stoppedP1Cause?.state).toBe("held");
      const stoppedP0Item = snapAfter.inbox.find((i) => i.triage?.causeId === makeCauseId("escalation", escStoppedP0.id));
      expect(stoppedP0Item).toBeDefined();
      expect(stoppedP0Item!.triage?.state).toBe("surfaced");

      // Inspect via inbox.triage.list RPC
      const listed = (await client.request("inbox.triage.list", {
        repoRoot: repoDir,
        state: "surfaced",
        journal: true,
      })) as InboxTriageListResult;
      const surfacedIds = listed.causes.map((c) => c.causeId);
      expect(surfacedIds).toContain(makeCauseId("escalation", escOld.id));
      expect(surfacedIds).toContain(makeCauseId("escalation", escStoppedP0.id));
      expect(surfacedIds).not.toContain(makeCauseId("escalation", escCancel.id));
      expect(listed.journal && listed.journal.length).toBeGreaterThan(0);
    } finally {
      client?.close();
      await daemon.stop();
      rmSync(repoDir, { recursive: true, force: true });
      rmSync(stateDir, { recursive: true, force: true });
    }
  });
});
