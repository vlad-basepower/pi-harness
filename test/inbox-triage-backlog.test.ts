import { describe, expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ModelCatalogCompiler } from "../src/catalog/compiler.js";
import { loadConfig } from "../src/config.js";
import { makeCauseId } from "../src/control-plane/inbox-triage-store.js";
import {
  isTriageBatchRun,
  type TriageClock,
  type TriageScheduler,
} from "../src/control-plane/inbox-triage.js";
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
      await new Promise((r) => setTimeout(r, 15));
    }
    this.nowMs = target;
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

describe("Inbox triage backlog drainage (c2, c4.1 — real HarnessDaemon)", () => {
  test("all 7 census classes drain within ackMaxSec, zero routine notifications/Director turns, R16/R18 proofs, idempotent ticks & event replay", async () => {
    const repoDir = initTempGitRepo("pi-triage-backlog-");
    const stateDir = mkdtempSync(join(tmpdir(), "pi-triage-backlog-st-"));
    const timer = new FakeTimer(1_795_455_600_000);
    const startAt = timer.nowMs;
    const OLD = startAt - 25 * 3_600_000;
    const RECENT = startAt - 60_000;

    const cfg = makeTestConfig(repoDir, stateDir);
    cfg.inbox.mode = "on";
    cfg.inbox.schedulerHost = "host-backlog";
    cfg.inbox.batchModel = "coordinator";
    cfg.inbox.batchProfileId = "pi:local";

    const batchPrompts: string[] = [];
    const daemon = new HarnessDaemon(cfg, {
      customRunnerFactory: () => ({
        run: async (req) => {
          batchPrompts.push(req.initialPrompt);
          // Parse member cause IDs listed in the batch brief (`- <cause_id> (...)`) and return structured outcomes
          const causeIds = [...req.initialPrompt.matchAll(/^- ([a-z_]+:[^\s(]+)/gm)].map((m) => m[1]!);
          const outcomes = causeIds.map((cid) => ({
            cause_id: cid,
            outcome: cid.startsWith("escalation:") ? "undetermined" : "remediation",
            evidence: ["inspected"],
            next_action: `follow up on ${cid}`,
          }));
          const text = `Batch diagnosis complete.\n<triage_outcomes>\n${JSON.stringify(outcomes)}\n</triage_outcomes>`;
          await req.executeTool("return_task", {
            status: "completed",
            summary: text,
            sources: [{ path: "README.md", note: "read-only batch inspection" }],
          });
          return { text, turnsUsed: 1, stopReason: "completed" as const };
        },
      }),
      inboxClock: timer.clock,
      inboxScheduler: timer.scheduler,
      inboxHostname: "host-backlog",
    });

    let client: DaemonClient | null = null;
    try {
      // Create a Director chat session to prove routine triage triggers ZERO Director chat turns.
      const chat = daemon.director.chats.createSession({ repoRoot: repoDir, title: "Backlog Chat", profileId: "pi:local" });
      const initialChatTurns = daemon.director.chats.getHistory(chat.id).length;

      // 1. owner-dead: M1 open message from W1 -> W2, W2 completed
      const rMsg = daemon.db.createRun({ goal: "Message run", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const w1 = daemon.db.createTask({ runId: rMsg.id, tier: 1, kind: "orchestrator", title: "W1", objective: "o", profileId: "pi:local" });
      const w2 = daemon.db.createTask({ runId: rMsg.id, parentId: w1.id, tier: 2, kind: "worker", title: "W2", objective: "o", profileId: "pi:local" });
      daemon.db.updateTask(w1.id, { status: "completed" });
      daemon.db.updateTask(w2.id, { status: "completed" });
      daemon.db.updateRunStatus(rMsg.id, "completed");
      const m1 = daemon.db.createMessage(rMsg.id, w1.id, w2.id, "Did migration finish?");
      daemon.db.db.prepare("UPDATE messages SET created_at = ? WHERE id = ?").run(OLD, m1.id);

      // 2. resolved-in-fact-never-closed: Q1 ci_failed at OLD, Q2 merged at RECENT same (run, task, worktree, branch)
      //    PLUS negative control: Q3 ci_failed + Q4 merged on a DIFFERENT branch (must NOT proof-resolve Q3!)
      const rMerge = daemon.db.createRun({ goal: "Merge run", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const tMerge = daemon.db.createTask({ runId: rMerge.id, tier: 2, kind: "worker", title: "MergeWorker", objective: "o", profileId: "pi:local" });
      daemon.db.updateTask(tMerge.id, { status: "completed" });
      daemon.db.updateRunStatus(rMerge.id, "completed");
      const wtSame = daemon.db.recordWorktree({ runId: rMerge.id, taskId: tMerge.id, path: join(repoDir, ".wt-same"), branch: "harness/feat-a", baseSha: "aaa" });
      const q1 = daemon.db.enqueueMerge({ runId: rMerge.id, taskId: tMerge.id, worktreeId: wtSame.id, branch: "harness/feat-a" });
      daemon.db.finishMergeItem(q1.id, "ci_failed", "flake");
      daemon.db.db.prepare("UPDATE merge_queue SET created_at = ?, finished_at = ? WHERE id = ?").run(OLD, OLD + 1000, q1.id);
      const q2 = daemon.db.enqueueMerge({ runId: rMerge.id, taskId: tMerge.id, worktreeId: wtSame.id, branch: "harness/feat-a" });
      daemon.db.finishMergeItem(q2.id, "merged", "ok");
      daemon.db.db.prepare("UPDATE merge_queue SET created_at = ?, finished_at = ? WHERE id = ?").run(RECENT, RECENT + 1000, q2.id);

      const q3 = daemon.db.enqueueMerge({ runId: rMerge.id, taskId: tMerge.id, worktreeId: wtSame.id, branch: "harness/feat-b" });
      daemon.db.finishMergeItem(q3.id, "ci_failed", "real test failure");
      daemon.db.db.prepare("UPDATE merge_queue SET created_at = ?, finished_at = ? WHERE id = ?").run(OLD, OLD + 1000, q3.id);
      const q4 = daemon.db.enqueueMerge({ runId: rMerge.id, taskId: tMerge.id, worktreeId: wtSame.id, branch: "harness/feat-other" });
      daemon.db.finishMergeItem(q4.id, "merged", "other branch merged");
      daemon.db.db.prepare("UPDATE merge_queue SET created_at = ?, finished_at = ? WHERE id = ?").run(RECENT, RECENT + 1000, q4.id);
      daemon.db.updateTask(tMerge.id, { status: "completed" });

      // 3. duplicate-or-derived-symptom: R1 failed, G1 linked awaiting_human with recorded run failure reason
      const r1 = daemon.db.createRun({ goal: "Failed run R1", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const tR1 = daemon.db.createTask({ runId: r1.id, tier: 1, kind: "orchestrator", title: "T-R1", objective: "o", profileId: "pi:local" });
      daemon.db.updateTask(tR1.id, { status: "failed", summary: "Compilation error" });
      daemon.db.updateRunStatus(r1.id, "failed", "Compilation error");
      const g1 = daemon.director.goals.createGoal({
        chatSessionId: chat.id,
        repoRoot: repoDir,
        title: "Goal G1",
        description: "Derived goal",
        criteria: [{ id: "c1", kind: "behavioral", description: "ok", required: true }],
      });
      daemon.director.goals.linkRunAndStart(g1.id, r1.id);
      daemon.director.goals.setStatus(g1.id, "awaiting_human", `Linked run ${r1.id} failed before goal verification`);

      // 4. restart-casualty (detached legacy without daemon_restart cause -> R12 batch)
      const r2 = daemon.db.createRun({ goal: "Legacy restart R2", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const tR2 = daemon.db.createTask({ runId: r2.id, tier: 1, kind: "orchestrator", title: "T-R2", objective: "o", profileId: "pi:local" });
      daemon.db.updateTask(tR2.id, { status: "failed", summary: "Interrupted by daemon restart" });
      daemon.db.updateRunStatus(r2.id, "failed", "failed only");

      // 5. needs-rescope: G2 awaiting_human, completed R3, no verdict, reviewer stopped on quota
      const r3 = daemon.db.createRun({ goal: "Completed R3", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const tR3 = daemon.db.createTask({ runId: r3.id, tier: 1, kind: "orchestrator", title: "T-R3", objective: "o", profileId: "pi:local" });
      daemon.db.updateTask(tR3.id, { status: "completed" });
      daemon.db.updateRunStatus(r3.id, "completed");
      const g2 = daemon.director.goals.createGoal({
        chatSessionId: chat.id,
        repoRoot: repoDir,
        title: "Goal G2",
        description: "Needs review diagnosis",
        criteria: [{ id: "c1", kind: "behavioral", description: "ok", required: true }],
      });
      daemon.director.goals.linkRunAndStart(g2.id, r3.id);
      daemon.director.goals.setStatus(g2.id, "awaiting_human", "Reviewer stopped: quota_paused");

      // 6. genuine-human-decision: TWO independent P0 escalations E1 and E2 in the SAME run!
      const rHuman = daemon.db.createRun({ goal: "Human asks run", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const tH = daemon.db.createTask({ runId: rHuman.id, tier: 1, kind: "orchestrator", title: "TH", objective: "o", profileId: "pi:local" });
      daemon.db.updateTask(tH.id, { status: "blocked", summary: "Waiting on user" });
      daemon.db.updateRunStatus(rHuman.id, "failed", "Blocked on human");
      const e1 = daemon.db.createEscalation({
        runId: rHuman.id,
        fromTaskId: tH.id,
        toTaskId: null,
        severity: "P0",
        category: "blocked_dependency",
        title: "Interactive OAuth login required",
        evidencePaths: ["docs/login.md"],
        blocking: true,
      });
      const e2 = daemon.db.createEscalation({
        runId: rHuman.id,
        fromTaskId: tH.id,
        toTaskId: null,
        severity: "P0",
        category: "ambiguous_spec",
        title: "Choose merge vs rebase vs hold",
        evidencePaths: ["docs/strategy.md"],
        blocking: false,
      });

      // 7. undetermined: E3 open nonblocking P1 policy escalation, no addressee, settled run
      const rUndet = daemon.db.createRun({ goal: "Undetermined run", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const tU = daemon.db.createTask({ runId: rUndet.id, tier: 1, kind: "orchestrator", title: "TU", objective: "o", profileId: "pi:local" });
      daemon.db.updateTask(tU.id, { status: "completed" });
      daemon.db.updateRunStatus(rUndet.id, "completed");
      const e3 = daemon.db.createEscalation({
        runId: rUndet.id,
        fromTaskId: tU.id,
        toTaskId: null,
        severity: "P1",
        category: "security_risk",
        title: "Deferred access-default policy question",
        evidencePaths: ["docs/policy.md"],
        blocking: false,
      });

      const sock = await daemon.start();
      client = new DaemonClient(sock);
      await client.connect();

      // First tick fires at +5s (<= ackMaxSec 60s)
      await timer.advance(5_000);
      const ackElapsedSec = (timer.nowMs - startAt) / 1000;
      expect(ackElapsedSec).toBeLessThanOrEqual(cfg.inbox.ackMaxSec);

      // Wait for the spawned rung-B batch task to finish and ingest its outcomes
      await waitUntil(() => {
        const assigns = daemon.triageStore.listOpenAssignments("batch");
        return batchPrompts.length >= 1 && assigns.length === 0;
      }, 4000);

      // Advance one more 30s sweep so any remaining due items settle
      await timer.advance(30_000);

      // Verify M1 (owner-dead) expired in DB
      const m1After = daemon.db.getMessage(m1.id)!;
      expect(m1After.status).toBe("expired");
      expect(m1After.expiredReason).toContain(w2.id);

      // Verify Q1 proof-resolved (R16) while Q3 (different branch) is NOT proof-resolved!
      const cQ1 = daemon.triageStore.getCause(makeCauseId("merge_bounced", q1.id))!;
      expect(cQ1.state).toBe("resolved");
      expect(cQ1.outcomeKind).toBe("proof");
      expect(cQ1.outcome).toContain(q2.id);

      const cQ3 = daemon.triageStore.getCause(makeCauseId("merge_bounced", q3.id))!;
      expect(cQ3.state).not.toBe("resolved");
      expect(cQ3.state).toBe("parked");

      // Verify G2 (R14x) was NEVER auto-accepted or verified
      expect(daemon.director.goals.getGoal(g2.id)?.status).toBe("awaiting_human");
      expect(daemon.director.goals.listReviews(g2.id)).toHaveLength(0);

      // Verify E3 (undetermined) parked via batch with nextAction, NOT surfaced as a per-item blocker
      const cE3 = daemon.triageStore.getCause(makeCauseId("escalation", e3.id))!;
      expect(cE3.state).toBe("parked");
      expect(cE3.nextAction).toContain(cE3.causeId);

      // Verify ZERO per-item notifications for routine causes (only E1, E2 + ONE coalesced scope_decision ask)
      const blockerAsks = daemon.triageStore.listActiveAsks("blocker");
      expect(blockerAsks.map((a) => a.causeId).sort()).toEqual(
        [makeCauseId("escalation", e1.id), makeCauseId("escalation", e2.id)].sort(),
      );
      expect(daemon.triageStore.listActiveAsks("scope_decision")).toHaveLength(1);

      const routineCauseIds = [
        makeCauseId("open_message", m1.id),
        makeCauseId("merge_bounced", q1.id),
        makeCauseId("merge_bounced", q3.id),
        makeCauseId("run_failed", r1.id),
        makeCauseId("run_failed", r2.id),
        makeCauseId("goal_awaiting_human", g2.id),
        makeCauseId("escalation", e3.id),
      ];
      for (const cid of routineCauseIds) {
        const c = daemon.triageStore.getCause(cid)!;
        expect(["resolved", "owned", "held", "parked"]).toContain(c.state);
        expect(c.ackedAt - startAt).toBeLessThanOrEqual(cfg.inbox.ackMaxSec * 1000);
        expect(c.nextAction.length).toBeGreaterThan(0);
        expect(daemon.triageStore.getActiveAskForCause(cid)).toBeNull();
        // No per-item blocker notified journal for routine causes
        const blockerNotified = daemon.triageStore
          .listJournal({ causeId: cid, type: "notified" })
          .filter((j) => (j.payload as { kind?: string })?.kind !== "scope_decision");
        expect(blockerNotified).toHaveLength(0);
      }

      // ZERO Director chat turns created for routine causes
      expect(daemon.director.chats.getHistory(chat.id).length).toBe(initialChatTurns);

      // work.snapshot.inbox contains ONLY genuine blockers (E1 and E2 stay TWO separate entries!) + 1 scope_decision ask
      const snap = (await client.request("work.snapshot", { repoRoot: repoDir })) as {
        inbox: InboxItem[];
        inboxAuto?: InboxItem[];
      };
      const humanEscIds = snap.inbox
        .filter((i) => i.ref.kind === "escalation" && i.triage?.rule !== "SCOPE_DECISION")
        .map((i) => i.ref.id)
        .sort();
      expect(humanEscIds).toEqual([e1.id, e2.id].sort());
      const scopeAsks = snap.inbox.filter((i) => i.triage?.rule === "SCOPE_DECISION");
      expect(scopeAsks).toHaveLength(1);
      const scopeRefIds = scopeAsks[0]!.sourceRefs?.map((r) => r.id) ?? [];
      expect(scopeRefIds).toContain(e3.id);
      expect(scopeRefIds).toContain(q3.id);
      expect(scopeRefIds).not.toContain(q1.id);
      expect(snap.inboxAuto && snap.inboxAuto.length).toBeGreaterThan(0);

      // Journal + inbox.triage.* event history present via RPC
      const listed = (await client.request("inbox.triage.list", { journal: true })) as InboxTriageListResult;
      expect(listed.causes.length).toBeGreaterThanOrEqual(routineCauseIds.length + 2);
      expect(listed.journal && listed.journal.length).toBeGreaterThan(0);
      const triageEvents = daemon.db.db
        .prepare("SELECT COUNT(*) AS cnt FROM events WHERE type LIKE 'inbox.triage.%'")
        .get() as { cnt: number };
      expect(triageEvents.cnt).toBeGreaterThan(0);

      // No triage of triage: batch task itself never became a cause
      const batchTasks = daemon.db.db
        .prepare("SELECT id, run_id FROM tasks")
        .all() as Array<{ id: string; run_id: string }>;
      const triageBatchTasks = batchTasks.filter((t) => isTriageBatchRun(t.run_id));
      expect(triageBatchTasks.length).toBeGreaterThanOrEqual(1);
      for (const bt of triageBatchTasks) {
        expect(daemon.triageStore.getCause(makeCauseId("task_failed", bt.id))).toBeNull();
        expect(daemon.triageStore.getCause(makeCauseId("task_blocked", bt.id))).toBeNull();
      }

      // Repeated ticks + replayed events create ZERO additional effect rows!
      const effectCountBefore = (
        daemon.db.db.prepare("SELECT COUNT(*) AS cnt FROM inbox_triage_effect").get() as { cnt: number }
      ).cnt;
      await timer.advance(150_000);
      const seqRows = daemon.db.db
        .prepare("SELECT seq FROM events ORDER BY seq ASC LIMIT 500")
        .all() as Array<{ seq: number }>;
      for (const r of seqRows) {
        const ev = daemon.db.getEventBySeq(r.seq);
        if (ev) daemon.triage.onEvent(ev);
      }
      await timer.advance(30_000);
      const effectCountAfter = (
        daemon.db.db.prepare("SELECT COUNT(*) AS cnt FROM inbox_triage_effect").get() as { cnt: number }
      ).cnt;
      expect(effectCountAfter).toBe(effectCountBefore);
    } finally {
      client?.close();
      await daemon.stop();
      rmSync(repoDir, { recursive: true, force: true });
      rmSync(stateDir, { recursive: true, force: true });
    }
  });

  test("99-item burst with >8 restart casualties and >200 old runs: all acked <= 60s, maxDispatchPerTick & batchMaxItems enforced", async () => {
    const repoDir = initTempGitRepo("pi-triage-burst-");
    const stateDir = mkdtempSync(join(tmpdir(), "pi-triage-burst-st-"));
    const timer = new FakeTimer(1_795_455_600_000);
    const startAt = timer.nowMs;

    const cfg = makeTestConfig(repoDir, stateDir);
    cfg.inbox.mode = "on";
    cfg.inbox.schedulerHost = "host-burst";
    cfg.inbox.batchModel = "coordinator";
    cfg.inbox.batchProfileId = "pi:local";
    cfg.inbox.maxDispatchPerTick = 2;
    cfg.inbox.batchMaxItems = 20;

    // Hold batch runner open until released so we can assert batchMaxItems + overflow queuing deterministically
    let releaseBatch!: () => void;
    const batchGate = new Promise<void>((resolve) => {
      releaseBatch = resolve;
    });
    let batchRunsStarted = 0;

    const daemon = new HarnessDaemon(cfg, {
      customRunnerFactory: () => ({
        run: async (req) => {
          batchRunsStarted += 1;
          await batchGate;
          const causeIds = [...req.initialPrompt.matchAll(/^- ([a-z_]+:[^\s(]+)/gm)].map((m) => m[1]!);
          const outcomes = causeIds.map((cid) => ({
            cause_id: cid,
            outcome: "undetermined",
            evidence: ["ok"],
            next_action: "review",
          }));
          const text = `<triage_outcomes>${JSON.stringify(outcomes)}</triage_outcomes>`;
          await req.executeTool("return_task", {
            status: "completed",
            summary: text,
            sources: [{ path: "README.md", note: "burst batch" }],
          });
          return { text, turnsUsed: 1, stopReason: "completed" as const };
        },
      }),
      inboxClock: timer.clock,
      inboxScheduler: timer.scheduler,
      inboxHostname: "host-burst",
    });

    let client: DaemonClient | null = null;
    try {
      // Seed 210 old completed runs (well beyond the 50-run snapshot window)
      daemon.db.transaction(() => {
        const ins = daemon.db.db.prepare(
          "INSERT INTO runs(id, goal, repo_root, base_branch, base_sha, status, isolation_mode, created_at, updated_at) VALUES (?, ?, ?, 'main', 'aaa', 'completed', 'standalone', ?, ?)",
        );
        for (let i = 1; i <= 210; i++) {
          const ts = startAt - (1_000_000 - i * 100);
          ins.run(`run-old-${i}`, `Old completed run ${i}`, repoDir, ts, ts);
        }
      });

      // Seed 99 inbox items:
      // - 10 daemon_restart casualties (R10 handoff)
      for (let i = 1; i <= 10; i++) {
        const r = daemon.db.createRun({ goal: `Restart casualty ${i}`, repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
        const t = daemon.db.createTask({ runId: r.id, tier: 1, kind: "orchestrator", title: `RT-${i}`, objective: "o", profileId: "pi:local" });
        daemon.db.updateTask(t.id, { status: "failed", summary: "Interrupted by daemon restart" });
        daemon.db.updateRunStatus(r.id, "failed", "Interrupted by daemon restart", "daemon_restart");
      }
      // - 40 open messages to completed tasks (R18)
      const rMsgs = daemon.db.createRun({ goal: "Msg burst", repoRoot: repoDir, baseBranch: "main", baseSha: "aaa" });
      const tFrom = daemon.db.createTask({ runId: rMsgs.id, tier: 1, kind: "orchestrator", title: "From", objective: "o", profileId: "pi:local" });
      const tTo = daemon.db.createTask({ runId: rMsgs.id, parentId: tFrom.id, tier: 2, kind: "worker", title: "To", objective: "o", profileId: "pi:local" });
      daemon.db.updateTask(tFrom.id, { status: "completed" });
      daemon.db.updateTask(tTo.id, { status: "completed" });
      daemon.db.updateRunStatus(rMsgs.id, "completed");
      for (let i = 1; i <= 40; i++) {
        daemon.db.createMessage(rMsgs.id, tFrom.id, tTo.id, `Stale question ${i}`);
      }
      // - 35 P2 escalations on settled run (R05 -> rung B batch: 20 attached + 15 overflow!)
      for (let i = 1; i <= 35; i++) {
        daemon.db.createEscalation({
          runId: rMsgs.id,
          fromTaskId: tTo.id,
          toTaskId: null,
          severity: "P2",
          category: "code_defect",
          title: `Settled defect ${i}`,
          evidencePaths: ["src/x.ts"],
          blocking: false,
        });
      }
      // - 10 superseded bounced merges (R16)
      const wt = daemon.db.recordWorktree({ runId: rMsgs.id, taskId: tTo.id, path: join(repoDir, ".wt-burst"), branch: "harness/burst", baseSha: "aaa" });
      for (let i = 1; i <= 10; i++) {
        const mb = daemon.db.enqueueMerge({ runId: rMsgs.id, taskId: tTo.id, worktreeId: wt.id, branch: "harness/burst" });
        daemon.db.finishMergeItem(mb.id, "ci_failed", `fail ${i}`);
        daemon.db.db.prepare("UPDATE merge_queue SET created_at = ?, finished_at = ? WHERE id = ?").run(startAt - 60_000 + i, startAt - 50_000 + i, mb.id);
      }
      const mMerged = daemon.db.enqueueMerge({ runId: rMsgs.id, taskId: tTo.id, worktreeId: wt.id, branch: "harness/burst" });
      daemon.db.finishMergeItem(mMerged.id, "merged", "merged");
      daemon.db.db.prepare("UPDATE merge_queue SET created_at = ?, finished_at = ? WHERE id = ?").run(startAt - 1000, startAt - 500, mMerged.id);
      daemon.db.updateTask(tTo.id, { status: "completed" });

      // - 4 genuine P0 blockers (R02)
      for (let i = 1; i <= 4; i++) {
        daemon.db.createEscalation({
          runId: rMsgs.id,
          fromTaskId: tTo.id,
          toTaskId: null,
          severity: "P0",
          category: "blocked_dependency",
          title: `Genuine P0 #${i}`,
          evidencePaths: [`docs/p0-${i}.md`],
          blocking: true,
        });
      }

      const sock = await daemon.start();
      client = new DaemonClient(sock);
      await client.connect();

      // Advance 5s -> first tick acks ALL 99 items in one control pass!
      await timer.advance(5_000);
      expect((timer.nowMs - startAt) / 1000).toBeLessThanOrEqual(cfg.inbox.ackMaxSec);

      const allCauses = daemon.triageStore.listCauses();
      expect(allCauses.length).toBe(99);
      for (const c of allCauses) {
        expect(c.ackedAt - startAt).toBeLessThanOrEqual(cfg.inbox.ackMaxSec * 1000);
      }

      // All 40 obsolete messages expired, all 10 superseded merges proof-resolved
      const expiredMsgs = daemon.db.db
        .prepare("SELECT COUNT(*) AS cnt FROM messages WHERE status = 'expired'")
        .get() as { cnt: number };
      expect(expiredMsgs.cnt).toBe(40);
      expect(allCauses.filter((c) => c.originKind === "merge_bounced" && c.state === "resolved")).toHaveLength(10);

      // Exactly ONE open batch for this scope with <= 20 members; remaining 15 durably queued for next batch
      await waitUntil(() => batchRunsStarted === 1, 4000);
      const openBatch = daemon.triageStore.getOpenBatchForScope(repoDir);
      expect(openBatch).not.toBeNull();
      expect(daemon.triageStore.listAssignmentMembers(openBatch!.assignmentId)).toHaveLength(20);
      expect(batchRunsStarted).toBe(1);
      const queuedOverflow = allCauses.filter((c) => c.nextAction.startsWith("queued for next batch"));
      expect(queuedOverflow.length).toBe(15);

      // Human inbox shows ONLY the 4 P0 blockers + 10 restart handoffs (14 total), ZERO routine items!
      const snap = (await client.request("work.snapshot", { repoRoot: repoDir })) as {
        inbox: InboxItem[];
        inboxAuto?: InboxItem[];
      };
      expect(snap.inbox).toHaveLength(14);

      // Release batch and let it finish cleanly before stop
      releaseBatch();
      await waitUntil(() => daemon.triageStore.listOpenAssignments("batch").length === 0, 4000);
    } finally {
      releaseBatch();
      client?.close();
      await daemon.stop();
      rmSync(repoDir, { recursive: true, force: true });
      rmSync(stateDir, { recursive: true, force: true });
    }
  });
});
