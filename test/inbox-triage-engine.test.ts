import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  DEFAULT_INBOX_TRIAGE_CONFIG,
  type InboxTriageConfig,
  resolveConfig,
  validateConfigRaw,
} from "../src/config.js";
import { ControlPlaneDb, type EventRecord } from "../src/control-plane/db.js";
import {
  INBOX_TRIAGE_TABLES,
  InboxTriageStore,
  makeCauseId,
} from "../src/control-plane/inbox-triage-store.js";
import {
  InboxTriage,
  type TriageClock,
  type TriageDeps,
  type TriageDispatcher,
  type TriageScheduler,
} from "../src/control-plane/inbox-triage.js";
import type { RunQuotaPauseStatus } from "../src/control-plane/orchestrator.js";
import { GOALS_SCHEMA } from "../src/goals/engine.js";

// ============================================================================
// Deterministic Fake Clock + Virtual Timer Scheduler
// ============================================================================

interface ScheduledTimer {
  id: number;
  at: number;
  intervalMs: number | null;
  fn: () => void;
  cancelled: boolean;
}

class FakeClockAndScheduler implements TriageClock, TriageScheduler {
  private currentMs: number;
  private nextId = 1;
  private timers: ScheduledTimer[] = [];

  constructor(startMs = 1_800_000_000_000) {
    this.currentMs = startMs;
  }

  now(): number {
    return this.currentMs;
  }

  setNow(ms: number): void {
    this.currentMs = ms;
  }

  every(ms: number, fn: () => void): () => void {
    const t: ScheduledTimer = { id: this.nextId++, at: this.currentMs + ms, intervalMs: ms, fn, cancelled: false };
    this.timers.push(t);
    return () => {
      t.cancelled = true;
    };
  }

  after(ms: number, fn: () => void): () => void {
    const t: ScheduledTimer = { id: this.nextId++, at: this.currentMs + ms, intervalMs: null, fn, cancelled: false };
    this.timers.push(t);
    return () => {
      t.cancelled = true;
    };
  }

  async flushMicrotasks(): Promise<void> {
    for (let i = 0; i < 8; i++) {
      await Promise.resolve();
    }
  }

  /** Advances virtual time by `deltaMs`, firing due timers in chronological order and draining microtasks after each. */
  async advanceBy(deltaMs: number): Promise<void> {
    const target = this.currentMs + deltaMs;
    while (true) {
      const due = this.timers
        .filter((t) => !t.cancelled && t.at <= target)
        .sort((a, b) => a.at - b.at || a.id - b.id);
      const next = due[0];
      if (!next) break;
      this.currentMs = next.at;
      if (next.intervalMs !== null) {
        next.at += next.intervalMs;
      } else {
        next.cancelled = true;
      }
      next.fn();
      await this.flushMicrotasks();
    }
    this.currentMs = target;
    await this.flushMicrotasks();
  }
}

interface HarnessRig {
  db: ControlPlaneDb;
  store: InboxTriageStore;
  clock: FakeClockAndScheduler;
  config: InboxTriageConfig;
  liveTasks: Set<string>;
  quotaStatuses: Map<string, RunQuotaPauseStatus | null>;
  emitted: Array<{ runId: string; type: string; payload: unknown }>;
  calls: {
    steer: Array<{ taskId: string; message: string }>;
    resumeTask: Array<{ taskId: string; expectedGeneration: number }>;
    resumeRun: string[];
    resumeGoal: string[];
    launchBatch: Array<{ assignmentId: string; scope: string; brief: string }>;
  };
  dispatcher: TriageDispatcher;
  host: { name: string };
  frozen: { value: boolean };
  triage: InboxTriage;
  createEngine: (overrides?: Partial<TriageDeps>) => InboxTriage;
}

function createRig(opts?: {
  dbPath?: string;
  config?: Partial<InboxTriageConfig>;
  withGoalsSchema?: boolean;
  startMs?: number;
  dispatcherOverrides?: Partial<TriageDispatcher>;
  onAfterClaim?: (effectKey: string) => void | Promise<void>;
  onBeforeReceipt?: (effectKey: string) => void | Promise<void>;
}): HarnessRig {
  const db = new ControlPlaneDb(opts?.dbPath ?? ":memory:");
  if (opts?.withGoalsSchema !== false) {
    db.db.exec(GOALS_SCHEMA);
  }
  const store = new InboxTriageStore(db.db);
  const clock = new FakeClockAndScheduler(opts?.startMs ?? 1_800_000_000_000);
  const config: InboxTriageConfig = {
    ...DEFAULT_INBOX_TRIAGE_CONFIG,
    mode: "on",
    schedulerHost: "node-1",
    batchProfileId: "acp:claude",
    ...(opts?.config ?? {}),
  };
  const liveTasks = new Set<string>();
  const quotaStatuses = new Map<string, RunQuotaPauseStatus | null>();
  const emitted: HarnessRig["emitted"] = [];
  const host = { name: "node-1" };
  const frozen = { value: false };
  const calls: HarnessRig["calls"] = {
    steer: [],
    resumeTask: [],
    resumeRun: [],
    resumeGoal: [],
    launchBatch: [],
  };

  let batchSeq = 1;
  const dispatcher: TriageDispatcher = {
    async steerTask(taskId, message) {
      calls.steer.push({ taskId, message });
      return { steered: true };
    },
    async resumeTask(taskId, expectedGeneration) {
      calls.resumeTask.push({ taskId, expectedGeneration });
      const task = db.getTask(taskId);
      if (task) {
        const nextGen = task.generation + 1;
        db.updateTask(taskId, { status: "running", generation: nextGen });
        liveTasks.add(taskId);
        return { outcome: "resumed", generation: nextGen };
      }
      return { outcome: "resumed", generation: expectedGeneration + 1 };
    },
    async resumeRun(runId) {
      calls.resumeRun.push(runId);
      db.updateRunStatus(runId, "active");
      return { outcome: "resumed" };
    },
    async resumeGoal(goalId) {
      calls.resumeGoal.push(goalId);
      return { outcome: "resumed" };
    },
    async launchBatch(assignmentId, scope, brief) {
      calls.launchBatch.push({ assignmentId, scope, brief });
      const taskId = `t-batch-${batchSeq++}`;
      return { taskId, generation: 1 };
    },
    ...(opts?.dispatcherOverrides ?? {}),
  };

  const createEngine = (overrides?: Partial<TriageDeps>) => {
    const freshStore = overrides?.store ?? new InboxTriageStore(db.db);
    return new InboxTriage({
      db,
      store: freshStore,
      config: () => config,
      clock,
      scheduler: clock,
      hostname: () => host.name,
      isTaskExecuting: (tid) => liveTasks.has(tid),
      getRunQuotaStatus: (rid) => quotaStatuses.get(rid) ?? null,
      dispatcher,
      emit: (runId, type, payload) => {
        emitted.push({ runId, type, payload });
        db.emitEvent(runId, null, type, payload);
      },
      isFrozen: () => frozen.value,
      onAfterClaim: opts?.onAfterClaim,
      onBeforeReceipt: opts?.onBeforeReceipt,
      ...overrides,
    });
  };

  const triage = createEngine({ store });
  return { db, store, clock, config, liveTasks, quotaStatuses, emitted, calls, dispatcher, host, frozen, triage, createEngine };
}

function insertGoalRow(
  db: ControlPlaneDb,
  g: {
    id: string;
    repoRoot?: string;
    chatSessionId?: string | null;
    version?: number;
    title?: string;
    status: string;
    linkedRunId?: string | null;
    reviewCycle?: number;
    verificationMode?: string;
    lastReviewSummary?: string | null;
    now?: number;
  },
): void {
  const now = g.now ?? 1_799_900_000_000;
  db.db
    .prepare(
      `INSERT INTO goals(
         id, chat_session_id, repo_root, version, title, description, criteria_json, non_goals_json,
         budgets_json, verification_mode, status, linked_run_id, review_cycle, last_review_summary, created_at, updated_at
       ) VALUES (?, ?, ?, ?, ?, 'desc', '[]', '[]', '{}', ?, ?, ?, ?, ?, ?, ?)`,
    )
    .run(
      g.id,
      g.chatSessionId ?? "chat-1",
      g.repoRoot ?? "/repo-a",
      g.version ?? 1,
      g.title ?? `Goal ${g.id}`,
      g.verificationMode ?? "auto",
      g.status,
      g.linkedRunId ?? null,
      g.reviewCycle ?? 0,
      g.lastReviewSummary ?? null,
      now,
      now,
    );
}

function listTriageTables(db: ControlPlaneDb): string[] {
  const rows = db.db
    .prepare(`SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'inbox_triage_%' ORDER BY name`)
    .all() as Array<{ name: string }>;
  return rows.map((r) => r.name);
}

describe("InboxTriage engine — contract Section 12 verification", () => {
  test("1. census backlog of all 7 classes acked in first control pass (<= 60s) with zero routine notifications; repeated sweeps & replayed events add zero effects", async () => {
    const rig = createRig();
    try {
      const { db, store, clock, liveTasks, calls, triage, emitted } = rig;
      const t0 = clock.now();

      // Class 1: owner-dead message M1 (W1 -> W2 completed)
      const rMsg = db.createRun({ goal: "Msg run", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const w1 = db.createTask({ runId: rMsg.id, tier: 1, kind: "orchestrator", title: "W1", objective: "o", profileId: "acp:claude" });
      const w2 = db.createTask({ runId: rMsg.id, parentId: w1.id, tier: 2, kind: "worker", title: "W2", objective: "o", profileId: "acp:claude" });
      db.updateTask(w1.id, { status: "running" });
      liveTasks.add(w1.id);
      db.updateTask(w2.id, { status: "completed" });
      const m1 = db.createMessage(rMsg.id, w1.id, w2.id, "Status check?");

      // Class 2: resolved-in-fact merge Q1 ci_failed + later Q2 merged same lineage
      const wt = db.recordWorktree({ runId: rMsg.id, taskId: w2.id, path: "/tmp/wt1", branch: "feat-1", baseSha: "aaa" });
      const q1 = db.enqueueMerge({ runId: rMsg.id, taskId: w2.id, worktreeId: wt.id, branch: "feat-1" });
      db.db.prepare("UPDATE merge_queue SET status = 'ci_failed', created_at = ?, finished_at = ? WHERE id = ?").run(t0 - 100_000, t0 - 99_000, q1.id);
      const q2 = db.enqueueMerge({ runId: rMsg.id, taskId: w2.id, worktreeId: wt.id, branch: "feat-1" });
      db.db.prepare("UPDATE merge_queue SET status = 'merged', created_at = ?, finished_at = ? WHERE id = ?").run(t0 - 10_000, t0 - 9_000, q2.id);
      // enqueueMerge transitions task to 'merging'; mark w2 completed after merge settled
      db.updateTask(w2.id, { status: "completed" });

      // Class 3: duplicate/derived R1 failed + G1 awaiting_human
      const r1 = db.createRun({ goal: "R1", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const r1t = db.createTask({ runId: r1.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
      db.updateTask(r1t.id, { status: "failed", summary: "ACP exit 1" });
      db.updateRunStatus(r1.id, "failed");
      insertGoalRow(db, {
        id: "goal-G1",
        status: "awaiting_human",
        linkedRunId: r1.id,
        lastReviewSummary: `Linked run ${r1.id} failed before goal verification`,
      });

      // Class 4: restart-casualty (handoff default)
      const r2 = db.createRun({ goal: "R2", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const r2t = db.createTask({ runId: r2.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
      db.updateTask(r2t.id, { status: "failed", summary: "Interrupted by daemon restart" });
      db.updateRunStatus(r2.id, "failed", "Interrupted by daemon restart", "daemon_restart");

      // Class 5: needs-rescope G2 awaiting_human + completed R3 + no verdict
      const r3 = db.createRun({ goal: "R3", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const r3t = db.createTask({ runId: r3.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
      db.updateTask(r3t.id, { status: "completed" });
      db.updateRunStatus(r3.id, "completed");
      insertGoalRow(db, {
        id: "goal-G2",
        status: "awaiting_human",
        linkedRunId: r3.id,
        lastReviewSummary: "Verification failed: Reviewer stopped: quota_paused",
      });

      // Class 6: genuine-human-decision E1 + E2 (same run, stay two separate asks!)
      const escE1 = db.createEscalation({
        runId: rMsg.id,
        fromTaskId: w1.id,
        toTaskId: null,
        severity: "P0",
        category: "blocked_dependency",
        title: "Run 'amp login'",
        evidencePaths: ["auth.log"],
        blocking: true,
      });
      const escE2 = db.createEscalation({
        runId: rMsg.id,
        fromTaskId: w1.id,
        toTaskId: null,
        severity: "P0",
        category: "ambiguous_spec",
        title: "Reply merge, rebase, or hold",
        evidencePaths: ["git.log"],
        blocking: false,
      });

      // Class 7: undetermined E3 nonblocking P1 policy escalation on settled run
      const escE3 = db.createEscalation({
        runId: r3.id,
        fromTaskId: r3t.id,
        toTaskId: null,
        severity: "P1",
        category: "ambiguous_spec",
        title: "Deferred policy questions Q1-Q8",
        evidencePaths: ["policy.md"],
        blocking: false,
      });

      triage.start();
      // First tick fires at <= 5s (well inside ackMaxSec = 60s)
      await clock.advanceBy(5_000);

      // Every primary cause was acked within ackMaxSec (5s <= 60s)
      const causes = store.listCauses();
      expect(causes.length).toBeGreaterThanOrEqual(7);
      for (const c of causes) {
        expect(c.ackedAt - t0).toBeLessThanOrEqual(rig.config.ackMaxSec * 1_000);
      }

      // M1 expired by R18; Q1 resolved by R16 proof
      expect(db.getMessage(m1.id)?.status).toBe("expired");
      expect(store.getCause(makeCauseId("open_message", m1.id))?.state).toBe("resolved");
      expect(store.getCause(makeCauseId("merge_bounced", q1.id))?.state).toBe("resolved");
      expect(store.getCause(makeCauseId("merge_bounced", q1.id))?.outcome).toBe(`submission superseded by ${q2.id}`);

      // R1, G2, E3 grouped into ONE rung-B batch for /repo-a (never notified as human blockers!)
      expect(calls.launchBatch).toHaveLength(1);
      expect(store.getCause(makeCauseId("run_failed", r1.id))?.state).toBe("owned");
      expect(store.getCause(makeCauseId("goal_awaiting_human", "goal-G2"))?.state).toBe("owned");
      expect(store.getCause(makeCauseId("escalation", escE3.id))?.state).toBe("owned");

      // ZERO notified journal entries for routine causes (only E1, E2, and R2 handoff!)
      const notifiedJournal = store.listJournal({ type: "notified" });
      const notifiedCauses = new Set(notifiedJournal.map((j) => j.causeId));
      expect(notifiedCauses).toEqual(
        new Set([
          makeCauseId("escalation", escE1.id),
          makeCauseId("escalation", escE2.id),
          makeCauseId("run_failed", r2.id),
        ]),
      );

      // Repeating 10 sweeps and replaying every event through onEvent adds ZERO effect rows or notifications
      const effectsBefore = store.listCauses().reduce((sum, c) => sum + store.listEffectsForCause(c.causeId).length, 0);
      const emittedBefore = emitted.length;
      const allEvents = db.db.prepare("SELECT * FROM events ORDER BY seq ASC").all() as Array<Record<string, unknown>>;
      for (const raw of allEvents) {
        triage.onEvent({
          seq: Number(raw.seq),
          runId: String(raw.run_id),
          taskId: raw.task_id ? String(raw.task_id) : null,
          type: String(raw.type),
          payload: JSON.parse(String(raw.payload_json)),
          createdAt: Number(raw.created_at),
        });
      }
      for (let i = 0; i < 10; i++) {
        await triage.sweepOnce();
      }
      const effectsAfter = store.listCauses().reduce((sum, c) => sum + store.listEffectsForCause(c.causeId).length, 0);
      expect(effectsAfter).toBe(effectsBefore);
      expect(store.listJournal({ type: "notified" })).toHaveLength(3);
      expect(emitted.length).toBe(emittedBefore);
    } finally {
      rig.triage.stop();
      rig.db.close();
    }
  });

  test("2. owner death detected <= 60s and cause durably reassigned / action issued <= 120s of fake time", async () => {
    const rig = createRig();
    try {
      const { db, store, clock, liveTasks, calls, triage } = rig;
      const run = db.createRun({ goal: "Owner loss", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
      const t2 = db.createTask({ runId: run.id, parentId: t1.id, tier: 2, kind: "worker", title: "W2", objective: "o", profileId: "acp:claude" });
      const t3 = db.createTask({ runId: run.id, parentId: t2.id, tier: 3, kind: "verify", title: "V3", objective: "o", profileId: "acp:claude" });
      db.updateTask(t1.id, { status: "running" });
      db.updateTask(t2.id, { status: "running" });
      db.updateTask(t3.id, { status: "completed" });
      liveTasks.add(t1.id);
      liveTasks.add(t2.id);

      const esc = db.createEscalation({
        runId: run.id,
        fromTaskId: t3.id,
        toTaskId: t2.id,
        severity: "P1",
        category: "code_defect",
        title: "Need fix",
        evidencePaths: ["src/a.ts"],
      });

      triage.start();
      await clock.advanceBy(5_000);
      const causeId = makeCauseId("escalation", esc.id);
      expect(store.getCause(causeId)?.state).toBe("owned");
      expect(store.getCause(causeId)?.ownerId).toBe(t2.id);
      expect(calls.steer).toHaveLength(0);

      // Owner t2 dies at tDeath
      const tDeath = clock.now();
      db.updateTask(t2.id, { status: "failed" });
      liveTasks.delete(t2.id);

      // Advance 30s (one periodic tick): owner loss detected <= 60s AND steer issued to live Tier 1 <= 120s!
      await clock.advanceBy(30_000);

      const ownerLostEvents = store.listJournal({ causeId, type: "owner_lost" });
      expect(ownerLostEvents).toHaveLength(1);
      expect(ownerLostEvents[0]!.at - tDeath).toBeLessThanOrEqual(60_000);

      const effectIntents = store.listJournal({ causeId, type: "effect_intent" });
      expect(effectIntents).toHaveLength(1);
      expect(effectIntents[0]!.at - tDeath).toBeLessThanOrEqual(120_000);

      expect(calls.steer).toHaveLength(1);
      expect(calls.steer[0]!.taskId).toBe(t1.id);
      expect(store.getCause(causeId)?.state).toBe("owned");
      expect(store.getCause(causeId)?.ownerId).toBe(t1.id);
    } finally {
      rig.triage.stop();
      rig.db.close();
    }
  });

  test("3. restart mid-effect at each barrier (intent before dispatch, dispatched before receipt, ambiguous steer, notify commit)", async () => {
    const dir = mkdtempSync(join(tmpdir(), "pi-harness-it-barriers-"));
    const dbPath = join(dir, "cp.sqlite");
    try {
      // --- Barrier A: crash after intent BEFORE dispatch ---
      {
        const rig = createRig({ dbPath, config: { restartRecovery: "resume" } });
        const { db, clock } = rig;
        const run = db.createRun({ goal: "Crash intent", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
        const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
        db.updateTask(t1.id, { status: "failed", summary: "Interrupted by daemon restart" });
        db.updateRunStatus(run.id, "failed", "restart", "daemon_restart");

        // Engine A crashes inside onAfterClaim (after intent committed, BEFORE dispatch!)
        const engineA = rig.createEngine({
          onAfterClaim: () => {
            throw new Error("SIMULATED_CRASH_BEFORE_DISPATCH");
          },
        });
        await expect(engineA.sweepOnce()).rejects.toThrow("SIMULATED_CRASH_BEFORE_DISPATCH");
        expect(rig.calls.resumeRun).toHaveLength(0);

        const causeId = makeCauseId("run_failed", run.id);
        const storeCheck = new InboxTriageStore(db.db);
        expect(storeCheck.getCause(causeId)?.attempts).toBe(1);
        expect(storeCheck.getEffect(`${causeId}#a1`)?.status).toBe("intent");

        // Engine B restarts on same DB file
        const engineB = rig.createEngine();
        await engineB.sweepOnce();
        // Reconciled as interrupted_before_dispatch, attempt stays 1, still in 1m backoff so no immediate retry
        expect(storeCheck.getEffect(`${causeId}#a1`)?.status).toBe("failed");
        expect(storeCheck.getEffect(`${causeId}#a1`)?.receipt).toBe("interrupted_before_dispatch");
        expect(rig.calls.resumeRun).toHaveLength(0);

        // Advance 1m backoff -> engine B dispatches attempt 2 cleanly
        await clock.advanceBy(60_000);
        await engineB.sweepOnce();
        expect(rig.calls.resumeRun).toEqual([run.id]);
        expect(storeCheck.getCause(causeId)?.attempts).toBe(2);
        db.close();
      }

      // --- Barrier B1: crash after dispatched resumeTask BEFORE receipt (postcondition HOLDS -> no duplicate resume!) ---
      {
        const dbPath2 = join(dir, "cp2.sqlite");
        const rig = createRig({ dbPath: dbPath2 });
        const { db } = rig;
        const run = db.createRun({ goal: "Crash receipt", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
        const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
        const esc = db.createEscalation({ runId: run.id, fromTaskId: t1.id, toTaskId: null, severity: "P0", category: "blocked_dependency", title: "Login", evidencePaths: ["a"], blocking: true });
        db.updateTask(t1.id, { status: "blocked", summary: `Blocked on ${esc.id}: Login` });
        db.tryResolveEscalation(esc.id, "done");

        const engineA = rig.createEngine({
          onBeforeReceipt: () => {
            throw new Error("SIMULATED_CRASH_BEFORE_RECEIPT");
          },
        });
        engineA.recordContinuation(esc.id);
        await expect(engineA.sweepOnce()).rejects.toThrow("SIMULATED_CRASH_BEFORE_RECEIPT");
        expect(rig.calls.resumeTask).toHaveLength(1);
        // Task generation already bumped 1 -> 2 by dispatcher before crash!
        expect(db.getTask(t1.id)?.generation).toBe(2);

        const causeId = makeCauseId("task_blocked", t1.id);
        const storeCheck = new InboxTriageStore(db.db);
        expect(storeCheck.getEffect(`${causeId}#a1`)?.status).toBe("dispatched");

        // Engine B restarts: verifies postcondition (gen bumped + live) -> marks confirmed, ZERO duplicate resumeTask!
        const engineB = rig.createEngine();
        await engineB.sweepOnce();
        expect(storeCheck.getEffect(`${causeId}#a1`)?.status).toBe("confirmed");
        expect(rig.calls.resumeTask).toHaveLength(1);
        db.close();
      }

      // --- Barrier B2: crash after dispatched steerTask BEFORE receipt -> ambiguous steer NEVER replayed, routes to rung B ---
      {
        const dbPath3 = join(dir, "cp3.sqlite");
        const rig = createRig({ dbPath: dbPath3 });
        const { db, liveTasks } = rig;
        const run = db.createRun({ goal: "Crash steer", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
        const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
        const t2 = db.createTask({ runId: run.id, parentId: t1.id, tier: 2, kind: "worker", title: "W2", objective: "o", profileId: "acp:claude" });
        db.updateTask(t1.id, { status: "running" });
        liveTasks.add(t1.id);
        db.updateTask(t2.id, { status: "completed" });
        const esc = db.createEscalation({ runId: run.id, fromTaskId: t2.id, toTaskId: null, severity: "P1", category: "code_defect", title: "Bug", evidencePaths: ["a"] });

        const engineA = rig.createEngine({
          onBeforeReceipt: () => {
            throw new Error("SIMULATED_CRASH_STEER");
          },
        });
        await expect(engineA.sweepOnce()).rejects.toThrow("SIMULATED_CRASH_STEER");
        expect(rig.calls.steer).toHaveLength(1);

        const causeId = makeCauseId("escalation", esc.id);
        const storeCheck = new InboxTriageStore(db.db);

        // Engine B restarts: marks steer ambiguous, does NOT replay steer (stays 1), immediately routes to rung B batch!
        const engineB = rig.createEngine();
        await engineB.sweepOnce();
        expect(storeCheck.getEffect(`${causeId}#a1`)?.status).toBe("ambiguous");
        expect(rig.calls.steer).toHaveLength(1); // NEVER replayed!
        expect(rig.calls.launchBatch).toHaveLength(1); // routed to rung B!
        expect(storeCheck.getCause(causeId)?.ownerKind).toBe("batch");
        db.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("4. lifetime budget: 3 dispatches max across restarts, spacing 1/2m + 4m grace, then parked + ONE scope_decision ask; +2h adds nothing", async () => {
    const dir = mkdtempSync(join(tmpdir(), "pi-harness-it-budget-"));
    const dbPath = join(dir, "cp.sqlite");
    try {
      const dispatchTimes: number[] = [];
      const rig = createRig({
        dbPath,
        dispatcherOverrides: {
          async launchBatch(assignmentId, scope, brief) {
            rig.calls.launchBatch.push({ assignmentId, scope, brief });
            dispatchTimes.push(rig.clock.now());
            return null; // always fails!
          },
        },
      });
      const { db, clock } = rig;
      const run = db.createRun({ goal: "Failing batch", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
      db.updateTask(t1.id, { status: "completed" });
      db.updateRunStatus(run.id, "completed");
      const esc1 = db.createEscalation({ runId: run.id, fromTaskId: t1.id, toTaskId: null, severity: "P2", category: "code_defect", title: "Finding 1", evidencePaths: ["a"] });
      const esc2 = db.createEscalation({ runId: run.id, fromTaskId: t1.id, toTaskId: null, severity: "P2", category: "code_defect", title: "Finding 2", evidencePaths: ["b"] });

      const tStart = clock.now();
      let engine = rig.createEngine();
      engine.start();

      // First tick at +5s -> attempt 1
      await clock.advanceBy(5_000);
      expect(dispatchTimes).toHaveLength(1);
      expect(rig.store.getCause(makeCauseId("escalation", esc1.id))?.attempts).toBe(1);

      // Simulate restart between attempt 1 and attempt 2!
      engine.stop();
      engine = rig.createEngine();
      engine.start();

      // At +35s (only 30s since attempt 1): nudges/ticks must NOT bypass 1m backoff
      await clock.advanceBy(30_000);
      expect(dispatchTimes).toHaveLength(1);

      // Advance to +65s (1m elapsed since attempt 1) -> attempt 2
      await clock.advanceBy(35_000);
      expect(dispatchTimes).toHaveLength(2);
      expect(dispatchTimes[1]! - dispatchTimes[0]!).toBeGreaterThanOrEqual(60_000);

      // Advance +90s (only 1.5m since attempt 2) -> still waiting 2m backoff
      await clock.advanceBy(90_000);
      expect(dispatchTimes).toHaveLength(2);

      // Advance +60s (> 2m since attempt 2) -> attempt 3
      await clock.advanceBy(60_000);
      expect(dispatchTimes).toHaveLength(3);
      expect(dispatchTimes[2]! - dispatchTimes[1]!).toBeGreaterThanOrEqual(120_000);

      // During the 4m grace window: NO 4th dispatch and NOT parked yet
      await clock.advanceBy(180_000);
      expect(dispatchTimes).toHaveLength(3);
      expect(rig.store.getCause(makeCauseId("escalation", esc1.id))?.state).toBe("acknowledged");

      // After 4m grace elapses (<= 9m from first dispatch): both parked + EXACTLY ONE scope_decision ask!
      await clock.advanceBy(90_000);
      expect(dispatchTimes).toHaveLength(3);
      const store = new InboxTriageStore(db.db);
      expect(store.getCause(makeCauseId("escalation", esc1.id))?.state).toBe("parked");
      expect(store.getCause(makeCauseId("escalation", esc2.id))?.state).toBe("parked");
      expect(store.getCause(makeCauseId("escalation", esc1.id))!.updatedAt - dispatchTimes[0]!).toBeLessThanOrEqual(
        9 * 60_000 + 30_000,
      );

      const scopeAsks = store.listActiveAsks("scope_decision");
      expect(scopeAsks).toHaveLength(1);
      expect(scopeAsks[0]!.ask).toContain("2 findings need");
      expect(store.listActiveAsks("blocker")).toHaveLength(0); // 3 failed attempts NEVER become N human blockers!
      const notifiedCount = store.listJournal({ type: "notified" }).length;
      expect(notifiedCount).toBe(1);

      // Advancing clock 2 hours adds NOTHING
      await clock.advanceBy(2 * 60 * 60 * 1_000);
      expect(dispatchTimes).toHaveLength(3);
      expect(store.listActiveAsks("scope_decision")).toHaveLength(1);
      expect(store.listJournal({ type: "notified" })).toHaveLength(1);
      expect(clock.now() - tStart).toBeGreaterThan(2 * 60 * 60 * 1_000);

      engine.stop();
      db.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("5. silent owner hits the absolute 20 min assignment expiry despite noisy unrelated events", async () => {
    const rig = createRig();
    try {
      const { db, store, clock, liveTasks, triage } = rig;
      const run = db.createRun({ goal: "Silent owner", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
      const t2 = db.createTask({ runId: run.id, parentId: t1.id, tier: 2, kind: "worker", title: "W2", objective: "o", profileId: "acp:claude" });
      const tNoise = db.createTask({ runId: run.id, parentId: t1.id, tier: 2, kind: "worker", title: "Noise", objective: "o", profileId: "acp:claude" });
      db.updateTask(t1.id, { status: "running" });
      db.updateTask(t2.id, { status: "running" });
      liveTasks.add(t1.id);
      liveTasks.add(t2.id);

      const esc = db.createEscalation({ runId: run.id, fromTaskId: t2.id, toTaskId: t1.id, severity: "P1", category: "code_defect", title: "Silent", evidencePaths: ["a"] });
      const causeId = makeCauseId("escalation", esc.id);

      triage.start();
      await clock.advanceBy(5_000);
      expect(store.getCause(causeId)?.state).toBe("owned");
      expect(store.getCause(causeId)?.ownerId).toBe(t1.id);

      // Emit noisy unrelated events (run-level, other task, own triage events) AND even task chatter every 4 minutes
      for (let m = 0; m < 4; m++) {
        await clock.advanceBy(4 * 60_000);
        db.emitEvent(run.id, null, "chat.delta", { text: "unrelated chatter" });
        db.emitEvent(run.id, tNoise.id, "task.progress", { note: "other task" });
        db.emitEvent(run.id, t1.id, "task.heartbeat", { note: "noisy task progress" });
      }
      // At 16 min: still owned (lease renewed), not yet expired
      expect(store.getCause(causeId)?.state).toBe("owned");
      expect(store.listJournal({ causeId, type: "owner_lost" })).toHaveLength(0);

      // Advance past 20 min mark -> hits unconditional assignment_expires_at despite noisy progress!
      await clock.advanceBy(5 * 60_000);
      const lostEvents = store.listJournal({ causeId, type: "owner_lost" });
      expect(lostEvents.length).toBeGreaterThanOrEqual(1);
      expect((lostEvents[0]!.payload as { reason?: string }).reason).toBe("assignment_expired");
    } finally {
      rig.triage.stop();
      rig.db.close();
    }
  });

  test("6. batch outcome ingestion: needs_human surfaced once, unverifiable proof parked, verified proof resolved, unknown ignored, unparsable counted", async () => {
    const rig = createRig();
    try {
      const { db, store, clock, triage } = rig;
      const run = db.createRun({ goal: "Batch ingest", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
      db.updateTask(t1.id, { status: "completed" });
      db.updateRunStatus(run.id, "completed");

      const escHuman = db.createEscalation({ runId: run.id, fromTaskId: t1.id, toTaskId: null, severity: "P1", category: "ambiguous_spec", title: "Need human", evidencePaths: ["a"] });
      const escFakeProof = db.createEscalation({ runId: run.id, fromTaskId: t1.id, toTaskId: null, severity: "P2", category: "code_defect", title: "Still open", evidencePaths: ["b"] });
      const escRem = db.createEscalation({ runId: run.id, fromTaskId: t1.id, toTaskId: null, severity: "P2", category: "code_defect", title: "Needs follow-up", evidencePaths: ["c"] });

      await triage.sweepOnce();
      const batch = store.getOpenBatchForScope("/repo-a")!;
      expect(batch).toBeTruthy();

      const cHuman = makeCauseId("escalation", escHuman.id);
      const cFake = makeCauseId("escalation", escFakeProof.id);
      const cRem = makeCauseId("escalation", escRem.id);

      const summary = `Investigation finished.\n<triage_outcomes>\n${JSON.stringify([
        { cause_id: cHuman, outcome: "needs_human", evidence: ["a"], ask: "Choose option X or Y" },
        { cause_id: cFake, outcome: "proof", evidence: ["claimed resolved"], next_action: "verify" },
        { cause_id: cRem, outcome: "remediation", evidence: ["c"], next_action: "file follow-up" },
        { cause_id: "escalation:esc-unknown-999", outcome: "needs_human", evidence: [], ask: "Injected fake ask" },
      ])}\n</triage_outcomes>`;

      const res = triage.ingestBatchResult(batch.assignmentId, summary);
      expect(res.parsed).toBe(true);
      expect(res.surfaced).toBe(1);
      expect(res.unverifiedProofs).toBe(1);
      expect(res.parked).toBe(2);
      expect(res.ignoredUnknown).toBe(1);

      // needs_human surfaced once
      expect(store.getCause(cHuman)?.state).toBe("surfaced");
      expect(store.getActiveAskForCause(cHuman)?.ask).toBe("Choose option X or Y");
      // unverifiable proof was NOT closed and source escalation stays open; cause parked
      expect(db.getEscalation(escFakeProof.id)?.status).toBe("open");
      expect(store.getCause(cFake)?.state).toBe("parked");
      // remediation parked
      expect(store.getCause(cRem)?.state).toBe("parked");
      // unknown cause ignored (never created)
      expect(store.getCause("escalation:esc-unknown-999")).toBeNull();

      // Test unparsable batch output -> counted attempt
      const escBad = db.createEscalation({ runId: run.id, fromTaskId: t1.id, toTaskId: null, severity: "P2", category: "code_defect", title: "Bad batch", evidencePaths: ["d"] });
      await clock.advanceBy(60_000);
      await triage.sweepOnce();
      const batch2 = store.getOpenBatchForScope("/repo-a")!;
      expect(batch2).toBeTruthy();
      const cBad = makeCauseId("escalation", escBad.id);
      expect(store.getCause(cBad)?.attempts).toBe(1);

      const badRes = triage.ingestBatchResult(batch2.assignmentId, "No outcomes block here at all!");
      expect(badRes.parsed).toBe(false);
      expect(store.getAssignment(batch2.assignmentId)?.status).toBe("failed");
      expect(store.getCause(cBad)?.attempts).toBe(1); // counted once at batch launch, not zero and not double-counted
      expect(store.getCause(cBad)?.state).toBe("acknowledged");
    } finally {
      rig.triage.stop();
      rig.db.close();
    }
  });

  test("7. blocker stays surfaced across 10 sweeps + restart with 1 ask & 1 notified; answer without continue holds; with continue resumes once; supersession clears", async () => {
    const dir = mkdtempSync(join(tmpdir(), "pi-harness-it-human-"));
    const dbPath = join(dir, "cp.sqlite");
    try {
      const rig = createRig({ dbPath });
      const { db, calls } = rig;
      const run = db.createRun({ goal: "Human blocker", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
      const t2 = db.createTask({ runId: run.id, parentId: t1.id, tier: 2, kind: "worker", title: "W2", objective: "o", profileId: "acp:claude" });
      db.updateTask(t1.id, { status: "running" });
      rig.liveTasks.add(t1.id);

      const esc = db.createEscalation({
        runId: run.id,
        fromTaskId: t2.id,
        toTaskId: null,
        severity: "P0",
        category: "blocked_dependency",
        title: "Run amp login",
        evidencePaths: ["auth.log"],
        blocking: true,
      });
      db.updateTask(t2.id, { status: "blocked", summary: `Blocked on ${esc.id}: Run amp login` });

      let engine = rig.createEngine();
      for (let i = 0; i < 5; i++) await engine.sweepOnce();
      // Restart new engine instance on same DB and sweep 5 more times
      engine = rig.createEngine();
      for (let i = 0; i < 5; i++) await engine.sweepOnce();

      const store = new InboxTriageStore(db.db);
      const escCauseId = makeCauseId("escalation", esc.id);
      expect(store.getCause(escCauseId)?.state).toBe("surfaced");
      expect(store.listActiveAsks("blocker")).toHaveLength(1);
      expect(store.listJournal({ causeId: escCauseId, type: "notified" })).toHaveLength(1);
      // Derived task_blocked folds into ONE human item with both refs
      const projected = engine.project([]);
      expect(projected.human).toHaveLength(1);
      expect(projected.human[0]!.sourceRefs?.map((r) => r.id).sort()).toEqual([esc.id, t2.id].sort());

      // Answer WITHOUT continue permission -> ask clears, t2 held (NO resumeTask!)
      db.tryResolveEscalation(esc.id, "Logged in");
      await engine.sweepOnce();
      expect(store.getCause(escCauseId)?.state).toBe("resolved");
      expect(store.listActiveAsks("blocker")).toHaveLength(0);
      expect(store.getCause(makeCauseId("task_blocked", t2.id))?.state).toBe("held");
      expect(store.getCause(makeCauseId("task_blocked", t2.id))?.holdReason).toBe("answered; awaiting explicit resume");
      expect(calls.resumeTask).toHaveLength(0);

      // Now record explicit continue permission -> EXACTLY ONE resumeTask at expected generation 1!
      engine.recordContinuation(esc.id);
      await engine.sweepOnce();
      expect(calls.resumeTask).toEqual([{ taskId: t2.id, expectedGeneration: 1 }]);
      expect(store.getContinuation(esc.id)?.consumedAt).not.toBeNull();
      // Further sweeps do not resume again
      await engine.sweepOnce();
      expect(calls.resumeTask).toHaveLength(1);

      // Supersession clears: create a goal + P0 blocker, cancel goal -> R00 reconciles & clears ask!
      const run2 = db.createRun({ goal: "Superseded run", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const tSup = db.createTask({ runId: run2.id, tier: 1, kind: "orchestrator", title: "Tsup", objective: "o", profileId: "acp:claude" });
      insertGoalRow(db, { id: "goal-sup", status: "executing", linkedRunId: run2.id });
      const escSup = db.createEscalation({ runId: run2.id, fromTaskId: tSup.id, toTaskId: null, severity: "P0", category: "blocked_dependency", title: "Blocker", evidencePaths: ["x"] });
      await engine.sweepOnce();
      expect(store.getCause(makeCauseId("escalation", escSup.id))?.state).toBe("surfaced");

      db.db.prepare("UPDATE goals SET status = 'cancelled' WHERE id = 'goal-sup'").run();
      await engine.sweepOnce();
      expect(db.getEscalation(escSup.id)?.status).toBe("resolved");
      expect(store.getCause(makeCauseId("escalation", escSup.id))?.state).toBe("resolved");
      expect(store.getActiveAskForCause(makeCauseId("escalation", escSup.id))).toBeNull();

      db.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("8. reopened acked P0 reopens same cause without fresh budget; rearm grants fresh budget", async () => {
    const rig = createRig();
    try {
      const { db, store, triage } = rig;
      const run = db.createRun({ goal: "Reopen", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
      const esc = db.createEscalation({ runId: run.id, fromTaskId: t1.id, toTaskId: null, severity: "P0", category: "blocked_dependency", title: "Reopenable", evidencePaths: ["a"] });
      const causeId = makeCauseId("escalation", esc.id);

      await triage.sweepOnce();
      expect(store.getCause(causeId)?.state).toBe("surfaced");
      // Simulate prior attempt count on the cause before resolution
      db.db.prepare("UPDATE inbox_triage_cause SET attempts = 2 WHERE cause_id = ?").run(causeId);

      // Resolve escalation -> cause resolves & ask clears
      db.tryResolveEscalation(esc.id, "Temporary fix");
      await triage.sweepOnce();
      expect(store.getCause(causeId)?.state).toBe("resolved");
      expect(store.getCause(causeId)?.attempts).toBe(2);

      // Reopen escalation (resolved -> acked via db.ackEscalation(id, ..., false))
      db.ackEscalation(esc.id, "Still broken", false);
      await triage.sweepOnce();
      expect(store.getCause(causeId)?.state).toBe("surfaced");
      // Same cause reopened, attempts preserved (NO fresh budget!)
      expect(store.getCause(causeId)?.attempts).toBe(2);
      expect(store.listJournal({ causeId, type: "reopened" })).toHaveLength(1);

      // Re-scanning does NOT emit a second reopened entry
      await triage.sweepOnce();
      expect(store.listJournal({ causeId, type: "reopened" })).toHaveLength(1);

      // Explicit operator rearm grants fresh budget (attempts = 0)
      const rearmed = triage.rearm(causeId, "operator rearm");
      expect(rearmed.rearmed).toBe(true);
      expect(store.getCause(causeId)?.attempts).toBe(0);
      expect(store.listJournal({ causeId, type: "rearmed" })).toHaveLength(1);
    } finally {
      rig.triage.stop();
      rig.db.close();
    }
  });

  test("9. mode off and report: zero writes including schema/DDL; report returns plan; project returns legacy items", async () => {
    for (const mode of ["off", "report"] as const) {
      const rig = createRig({ config: { mode } });
      try {
        const { db, triage, clock } = rig;
        const run = db.createRun({ goal: `Mode ${mode}`, repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
        const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
        db.createEscalation({ runId: run.id, fromTaskId: t1.id, toTaskId: null, severity: "P0", category: "blocked_dependency", title: "Ask", evidencePaths: ["a"] });

        triage.start();
        await clock.advanceBy(120_000);
        const sweep = await triage.sweepOnce();
        expect(sweep.control.ran).toBe(false);
        expect(sweep.action.ran).toBe(false);

        // ZERO triage tables created (lazy DDL!)
        expect(listTriageTables(db)).toEqual([]);

        // report() works in every mode with ZERO writes/DDL
        const plan = triage.report();
        expect(plan.length).toBeGreaterThanOrEqual(1);
        expect(plan[0]!.rule).toBe("R02");
        expect(listTriageTables(db)).toEqual([]);

        // project() returns legacy items unchanged with empty auto
        const legacyItems = [
          {
            id: "inbox:dummy",
            severity: "P1" as const,
            kind: "run_failed" as const,
            title: "Dummy",
            detail: "Dummy",
            ref: { kind: "run" as const, id: run.id },
            createdAt: 1,
          },
        ];
        expect(triage.project(legacyItems)).toEqual({ human: legacyItems, auto: [] });
        expect(listTriageTables(db)).toEqual([]);
      } finally {
        rig.triage.stop();
        rig.db.close();
      }
    }
  });

  test("10. authority lost (hostname mismatch or disarm) between claim and activation -> ZERO dispatch", async () => {
    // Case A: hostname flips between claim and activation
    {
      let flipHost = true;
      const rig = createRig({
        config: { restartRecovery: "resume" },
        onAfterClaim: () => {
          if (flipHost) {
            rig.host.name = "other-host";
            flipHost = false;
          }
        },
      });
      try {
        const { db, store, calls, triage } = rig;
        const run = db.createRun({ goal: "Auth flip", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
        const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
        db.updateTask(t1.id, { status: "failed", summary: "Interrupted by daemon restart" });
        db.updateRunStatus(run.id, "failed", "restart", "daemon_restart");

        const res = await triage.sweepOnce();
        expect(calls.resumeRun).toHaveLength(0);
        expect(res.action.skippedVeto).toBe(1);
        const eff = store.getEffect(`${makeCauseId("run_failed", run.id)}#a1`);
        expect(eff?.status).toBe("failed");
        expect(eff?.receipt).toBe("authority_lost_before_activation");
      } finally {
        rig.triage.stop();
        rig.db.close();
      }
    }

    // Case B: synchronous disarm() called between claim and activation
    {
      const rig = createRig({
        config: { restartRecovery: "resume" },
        onAfterClaim: () => {
          rig.triage.disarm();
        },
      });
      try {
        const { db, calls, triage } = rig;
        const run = db.createRun({ goal: "Disarm mid-flight", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
        const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
        db.updateTask(t1.id, { status: "failed", summary: "Interrupted by daemon restart" });
        db.updateRunStatus(run.id, "failed", "restart", "daemon_restart");

        await triage.sweepOnce();
        expect(calls.resumeRun).toHaveLength(0);
        expect(triage.isAuthoritative()).toBe(false);
      } finally {
        rig.triage.stop();
        rig.db.close();
      }
    }
  });

  test("11. out-of-window surfaced ask (>60 runs) stays visible in project() with strict repo scoping", async () => {
    const rig = createRig();
    try {
      const { db, triage } = rig;
      // Oldest run #1 in /repo-a has a real P0 ask (will fall outside a 50-run snapshot window)
      const oldRunA = db.createRun({ goal: "Old A", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const oldTaskA = db.createTask({ runId: oldRunA.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
      const escA = db.createEscalation({ runId: oldRunA.id, fromTaskId: oldTaskA.id, toTaskId: null, severity: "P0", category: "blocked_dependency", title: "Old repo-a blocker", evidencePaths: ["a"] });

      // Another repo /repo-b has its own P0 ask (must NOT leak into /repo-a scope!)
      const runB = db.createRun({ goal: "Run B", repoRoot: "/repo-b", baseBranch: "main", baseSha: "bbb" });
      const taskB = db.createTask({ runId: runB.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
      db.createEscalation({ runId: runB.id, fromTaskId: taskB.id, toTaskId: null, severity: "P0", category: "blocked_dependency", title: "Repo-b blocker", evidencePaths: ["b"] });

      // Add 60 newer runs in /repo-a to push oldRunA beyond the 50-run window
      for (let i = 0; i < 60; i++) {
        const r = db.createRun({ goal: `Newer ${i}`, repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
        db.updateRunStatus(r.id, "completed");
      }

      // Even BEFORE sweepOnce (un-acked inline classification), project() keeps /repo-a scoped and shows escA without leaking repo-b
      const preSweep = triage.project([], { repoRoot: "/repo-a" });
      expect(preSweep.human.map((i) => i.ref.id)).toEqual([escA.id]);

      // After sweepOnce, out-of-window surfaced ask stays in /repo-a human view with zero leakage from /repo-b
      await triage.sweepOnce();
      const postSweepA = triage.project([], { repoRoot: "/repo-a" });
      expect(postSweepA.human.map((i) => i.ref.id)).toEqual([escA.id]);
      expect(postSweepA.human[0]!.ask).toContain("Old repo-a blocker");
    } finally {
      rig.triage.stop();
      rig.db.close();
    }
  });

  test("12. 99-item burst with >8 restart casualties and >200 old runs: all acked in first pass, maxDispatchPerTick enforced, overflow durably tracked", async () => {
    const rig = createRig({ config: { restartRecovery: "resume", maxDispatchPerTick: 2, batchMaxItems: 20 } });
    try {
      const { db, store, clock, calls, triage } = rig;
      const t0 = clock.now();

      // Seed 205 old completed runs first (beyond any 50- or 200-run window)
      for (let i = 0; i < 205; i++) {
        const r = db.createRun({ goal: `Historical ${i}`, repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
        db.updateRunStatus(r.id, "completed");
      }

      // Seed 10 restart casualties (>8)
      const casualtyRuns: string[] = [];
      for (let i = 0; i < 10; i++) {
        const r = db.createRun({ goal: `Restart casualty ${i}`, repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
        const t = db.createTask({ runId: r.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
        db.updateTask(t.id, { status: "failed", summary: "Interrupted by daemon restart" });
        db.updateRunStatus(r.id, "failed", "Interrupted by daemon restart", "daemon_restart");
        casualtyRuns.push(r.id);
      }

      // Seed 89 routine settled-run escalations (total = 99 inbox items!)
      const hostRun = db.createRun({ goal: "Escalations host", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const hostTask = db.createTask({ runId: hostRun.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
      db.updateTask(hostTask.id, { status: "completed" });
      db.updateRunStatus(hostRun.id, "completed");
      for (let i = 0; i < 89; i++) {
        db.createEscalation({
          runId: hostRun.id,
          fromTaskId: hostTask.id,
          toTaskId: null,
          severity: "P2",
          category: "code_defect",
          title: `Routine finding #${i}`,
          evidencePaths: [`src/f${i}.ts`],
        });
      }

      // Pre-sweep projection must NOT flash the 99 routine items into human inbox!
      const preProject = triage.project([], { repoRoot: "/repo-a" });
      expect(preProject.human).toHaveLength(0);
      expect(preProject.auto).toHaveLength(99);

      triage.start();
      await clock.advanceBy(5_000);

      // All 99 items acked in the first control pass (<= 5s <= 60s ackMaxSec)
      const causes = store.listCauses();
      expect(causes).toHaveLength(99);
      for (const c of causes) {
        expect(c.ackedAt - t0).toBeLessThanOrEqual(rig.config.ackMaxSec * 1_000);
      }
      // First tick respected maxDispatchPerTick = 2
      expect(calls.resumeRun.length + calls.launchBatch.length).toBeLessThanOrEqual(2);
      // One open batch created for /repo-a capped at batchMaxItems = 20, with overflow durably queued
      const openBatch = store.getOpenBatchForScope("/repo-a");
      expect(openBatch).toBeTruthy();
      expect(store.listAssignmentMembers(openBatch!.assignmentId)).toHaveLength(20);
      // Zero routine human notifications
      expect(store.listJournal({ type: "notified" })).toHaveLength(0);
    } finally {
      rig.triage.stop();
      rig.db.close();
    }
  });

  test("13. config section 'inbox': defaults, global-only mode/schedulerHost security guard, and validation", () => {
    const cfgDefault = resolveConfig({}, {}, { stateDir: "/tmp/state" });
    expect(cfgDefault.inbox.mode).toBe("off");
    expect(cfgDefault.inbox.restartRecovery).toBe("handoff");
    expect(cfgDefault.inbox.schedulerHost).toBeNull();
    expect(cfgDefault.inbox.sweepIntervalSec).toBe(30);
    expect(cfgDefault.inbox.backoffMin).toEqual([1, 2, 4]);

    // Untrusted repo .pi-harness.json MUST NOT be able to flip inbox.mode or inbox.schedulerHost
    const cfgRepoAttempt = resolveConfig(
      {},
      { inbox: { mode: "on", schedulerHost: "evil-host", sweepIntervalSec: 15 } },
      { stateDir: "/tmp/state" },
    );
    expect(cfgRepoAttempt.inbox.mode).toBe("off");
    expect(cfgRepoAttempt.inbox.schedulerHost).toBeNull();
    expect(cfgRepoAttempt.inbox.sweepIntervalSec).toBe(15);

    expect(
      validateConfigRaw({
        inbox: {
          mode: "report",
          schedulerHost: "mac-studio",
          restartRecovery: "handoff",
          backoffMin: [1, 2, 4],
          maxAutoAttempts: 3,
          batchMaxItems: 20,
        },
      }),
    ).toEqual([]);

    const badErrors = validateConfigRaw({
      inbox: {
        mode: "invalid",
        restartRecovery: "yolo",
        backoffMin: [],
        maxAutoAttempts: 0,
      },
    });
    expect(badErrors.length).toBeGreaterThanOrEqual(4);
  });
});
