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
  findLatestEscalationUpdatedEvent,
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
        const resEv = findLatestEscalationUpdatedEvent(db, run.id, esc.id)!;
        engineA.recordContinuation({
          escalationId: esc.id,
          resolutionEventSeq: resEv.seq,
          taskId: t1.id,
          taskGeneration: t1.generation,
        });
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

      // Now record explicit continue permission bound to the winning resolution event & blocked task -> EXACTLY ONE resumeTask at gen 1!
      const resEv = findLatestEscalationUpdatedEvent(db, run.id, esc.id)!;
      engine.recordContinuation({
        escalationId: esc.id,
        resolutionEventSeq: resEv.seq,
        taskId: t2.id,
        taskGeneration: t2.generation,
      });
      await engine.sweepOnce();
      expect(calls.resumeTask).toEqual([{ taskId: t2.id, expectedGeneration: 1 }]);
      expect(store.getContinuation(esc.id, resEv.seq)?.consumedAt).not.toBeNull();
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

  test("10. MF-1: authority lost (report / freeze / host mismatch / disarm) at claim & dispatch barriers -> ZERO writes, mirrors, or secondary steers, then recovers after rearm", async () => {
    const snapshotDbState = (db: ControlPlaneDb, emitted: HarnessRig["emitted"]) => ({
      tables: listTriageTables(db),
      causes: db.db.query("SELECT * FROM inbox_triage_cause ORDER BY cause_id").all(),
      items: db.db.query("SELECT * FROM inbox_triage_item ORDER BY item_kind, source_id, source_epoch").all(),
      effects: db.db.query("SELECT * FROM inbox_triage_effect ORDER BY effect_key").all(),
      assignments: db.db.query("SELECT * FROM inbox_triage_assignment ORDER BY assignment_id").all(),
      members: db.db.query("SELECT * FROM inbox_triage_assignment_member ORDER BY assignment_id, cause_id").all(),
      asks: db.db.query("SELECT * FROM inbox_triage_ask ORDER BY ask_id").all(),
      journal: db.db.query("SELECT * FROM inbox_triage_journal ORDER BY seq").all(),
      events: db.db.query("SELECT seq, run_id, type, payload_json FROM events ORDER BY seq").all(),
      emittedCount: emitted.length,
    });

    type LossMode = "report" | "freeze" | "host_mismatch" | "disarm";
    const lossModes: LossMode[] = ["report", "freeze", "host_mismatch", "disarm"];

    // Barrier 1: loss between claim and activation (onAfterClaim) on cause action AND batch launch
    for (const mode of lossModes) {
      let snapAtLoss: ReturnType<typeof snapshotDbState> | null = null;
      let triggered = false;
      const rig = createRig({
        config: { restartRecovery: "resume" },
        onAfterClaim: () => {
          if (triggered) return;
          triggered = true;
          if (mode === "report") rig.triage.applyConfig({ ...rig.config, mode: "report" });
          else if (mode === "freeze") rig.frozen.value = true;
          else if (mode === "host_mismatch") rig.host.name = "other-host";
          else rig.triage.disarm();
          snapAtLoss = snapshotDbState(rig.db, rig.emitted);
        },
      });
      try {
        const { db, store, calls, triage, clock, config, host, frozen } = rig;
        const run = db.createRun({ goal: `Auth loss claim ${mode}`, repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
        const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
        db.updateTask(t1.id, { status: "failed", summary: "Interrupted by daemon restart" });
        db.updateRunStatus(run.id, "failed", "restart", "daemon_restart");

        const res = await triage.sweepOnce();
        expect(triggered).toBe(true);
        expect(calls.resumeRun).toHaveLength(0);
        expect(res.action.skippedVeto).toBe(1);
        // ZERO writes, ZERO journal rows, ZERO events, ZERO mirrors after loss!
        expect(snapshotDbState(db, rig.emitted)).toEqual(snapAtLoss!);
        // Effect stays in 'intent' for the next authorized reconciler (NOT written as failed)
        const eff = store.getEffect(`${makeCauseId("run_failed", run.id)}#a1`);
        expect(eff?.status).toBe("intent");
        expect(eff?.receipt).toBeNull();

        // Restore authority -> reconciler cleanly recovers the interrupted intent and retries after backoff
        if (mode === "report" || mode === "disarm") triage.applyConfig({ ...config, mode: "on" });
        else if (mode === "freeze") frozen.value = false;
        else host.name = "node-1";

        const recSweep = await triage.sweepOnce();
        expect(recSweep.control.reconciledEffects).toBe(1);
        expect(store.getEffect(`${makeCauseId("run_failed", run.id)}#a1`)?.status).toBe("failed");
        expect(store.getEffect(`${makeCauseId("run_failed", run.id)}#a1`)?.receipt).toBe("interrupted_before_dispatch");

        clock.setNow(clock.now() + 65_000);
        const retrySweep = await triage.sweepOnce();
        expect(calls.resumeRun).toHaveLength(1);
        expect(retrySweep.action.confirmed).toBe(1);
      } finally {
        rig.triage.stop();
        rig.db.close();
      }
    }

    // Barrier 2: loss during dispatch / before receipt (onBeforeReceipt) on expire_message (also tests secondary steer suppression)
    for (const mode of lossModes) {
      let snapAtLoss: ReturnType<typeof snapshotDbState> | null = null;
      let triggered = false;
      const rig = createRig({
        onBeforeReceipt: () => {
          if (triggered) return;
          triggered = true;
          if (mode === "report") rig.triage.applyConfig({ ...rig.config, mode: "report" });
          else if (mode === "freeze") rig.frozen.value = true;
          else if (mode === "host_mismatch") rig.host.name = "other-host";
          else rig.triage.disarm();
          snapAtLoss = snapshotDbState(rig.db, rig.emitted);
        },
      });
      try {
        const { db, store, calls, triage, config, host, frozen, liveTasks } = rig;
        const run = db.createRun({ goal: `Auth loss receipt ${mode}`, repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
        const asker = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "Asker", objective: "o", profileId: "acp:claude" });
        const recipient = db.createTask({ runId: run.id, tier: 2, kind: "worker", title: "Recip", objective: "o", profileId: "acp:claude" });
        db.updateTask(asker.id, { status: "running" });
        liveTasks.add(asker.id);
        db.updateTask(recipient.id, { status: "failed" });
        const msg = db.createMessage(run.id, asker.id, recipient.id, "Dead?");

        await triage.sweepOnce();
        expect(triggered).toBe(true);
        // Zero writes, zero journal entries, zero mirrors, AND zero secondary steer to asker after loss!
        expect(calls.steer).toHaveLength(0);
        expect(snapshotDbState(db, rig.emitted)).toEqual(snapAtLoss!);
        const eff = store.getEffect(`${makeCauseId("open_message", msg.id)}#a1`);
        expect(eff?.status).toBe("dispatched");
        expect(eff?.receipt).toBeNull();

        // Restore authority -> next authorized reconciler confirms via durable postcondition (message expired) and resolves cause
        if (mode === "report" || mode === "disarm") triage.applyConfig({ ...config, mode: "on" });
        else if (mode === "freeze") frozen.value = false;
        else host.name = "node-1";

        const recSweep = await triage.sweepOnce();
        expect(recSweep.control.reconciledEffects).toBe(1);
        expect(store.getEffect(`${makeCauseId("open_message", msg.id)}#a1`)?.status).toBe("confirmed");
        expect(store.getCause(makeCauseId("open_message", msg.id))?.state).toBe("resolved");
      } finally {
        rig.triage.stop();
        rig.db.close();
      }
    }

    // Barrier 1 & 2 on batch launch: zero writes/assignment closure on loss, then recovers after restore
    for (const barrier of ["claim", "receipt"] as const) {
      let snapAtLoss: ReturnType<typeof snapshotDbState> | null = null;
      let triggered = false;
      const rig = createRig({
        onAfterClaim: barrier === "claim" ? () => {
          if (triggered) return;
          triggered = true;
          rig.triage.applyConfig({ ...rig.config, mode: "report" });
          snapAtLoss = snapshotDbState(rig.db, rig.emitted);
        } : undefined,
        onBeforeReceipt: barrier === "receipt" ? () => {
          if (triggered) return;
          triggered = true;
          rig.frozen.value = true;
          snapAtLoss = snapshotDbState(rig.db, rig.emitted);
        } : undefined,
      });
      try {
        const { db, store, calls, triage, clock, config, frozen } = rig;
        const run = db.createRun({ goal: `Batch loss ${barrier}`, repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
        db.updateRunStatus(run.id, "failed", "crash");

        await triage.sweepOnce();
        expect(triggered).toBe(true);
        expect(snapshotDbState(db, rig.emitted)).toEqual(snapAtLoss!);
        if (barrier === "claim") {
          expect(calls.launchBatch).toHaveLength(0);
          expect(store.getOpenBatchForScope("/repo-a")).not.toBeNull();
          triage.applyConfig({ ...config, mode: "on" });
          const rec = await triage.sweepOnce();
          expect(rec.control.reconciledEffects).toBe(1);
          clock.setNow(clock.now() + 65_000);
          await triage.sweepOnce();
          expect(calls.launchBatch).toHaveLength(1);
        } else {
          expect(calls.launchBatch).toHaveLength(1);
          expect(store.getEffect(`${makeCauseId("run_failed", run.id)}#a1`)?.status).toBe("dispatched");
          frozen.value = false;
          const rec = await triage.sweepOnce();
          expect(rec.control.reconciledEffects).toBe(1);
        }
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

  test("Amendment 1: resolve+continue, reopen before dispatch, resolve without continue, sweep => 0 resumes; replay resolve+continue => still 0 resumes", async () => {
    const rig = createRig();
    const { db, store, calls } = rig;
    try {
      const run = db.createRun({ goal: "Amendment 1 test", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
      const esc = db.createEscalation({
        runId: run.id,
        fromTaskId: t1.id,
        toTaskId: null,
        severity: "P0",
        category: "blocked_dependency",
        title: "Need approval",
        evidencePaths: ["docs/a.md"],
        blocking: true,
      });
      db.updateTask(t1.id, { status: "blocked", summary: `Blocked on ${esc.id}: Need approval` });

      const engine = rig.createEngine();
      engine.start();
      await engine.sweepOnce();

      // 1. Winning resolve with continue === true grants continuation for seq S1
      const r1 = db.tryResolveEscalation(esc.id, "Proceed");
      expect(r1.transitioned).toBe(true);
      const ev1 = findLatestEscalationUpdatedEvent(db, run.id, esc.id)!;
      expect(ev1.status).toBe("resolved");
      const rec1 = engine.recordContinuation({
        escalationId: esc.id,
        resolutionEventSeq: ev1.seq,
        taskId: t1.id,
        taskGeneration: t1.generation,
      });
      expect(rec1.recorded).toBe(true);

      // 2. Reopen BEFORE dispatch -> revokes unconsumed grant
      db.ackEscalation(esc.id, "Reopened for further review", false);
      const evReopen = findLatestEscalationUpdatedEvent(db, run.id, esc.id)!;
      expect(evReopen.status).toBe("acked");

      // 3. Resolve again WITHOUT continue -> winning transition seq S3, no grant recorded
      const r2 = db.tryResolveEscalation(esc.id, "Resolved without continue");
      expect(r2.transitioned).toBe(true);
      const ev2 = findLatestEscalationUpdatedEvent(db, run.id, esc.id)!;
      expect(ev2.status).toBe("resolved");
      expect(ev2.seq).toBeGreaterThan(ev1.seq);

      // 4. Sweep -> grant S1 is revoked / seq mismatch -> ZERO resumes! Task is held.
      await engine.sweepOnce();
      expect(calls.resumeTask).toHaveLength(0);
      expect(store.getContinuation(esc.id, ev1.seq)?.revokedAt).not.toBeNull();
      expect(store.getCause(makeCauseId("task_blocked", t1.id))?.state).toBe("held");
      expect(store.getCause(makeCauseId("task_blocked", t1.id))?.attempts).toBe(0);

      // 5. Repeat the last resolve with continue: true -> transitioned === false (replay), grants nothing -> STILL zero resumes!
      const replay = db.tryResolveEscalation(esc.id, "Resolved again with continue");
      expect(replay.transitioned).toBe(false);
      // Even if a caller mistakenly tried to re-record the old seq, it is already revoked & seq mismatch!
      await engine.sweepOnce();
      expect(calls.resumeTask).toHaveLength(0);
      expect(store.getCause(makeCauseId("task_blocked", t1.id))?.attempts).toBe(0);
    } finally {
      db.close();
    }
  });

  test("MF-2: second connection races (restore recipient, relink goal, steer gen drift, reconciled intent, reopen before late already_completed)", async () => {
    const dir = mkdtempSync(join(tmpdir(), "inbox-triage-mf2-"));
    try {
      // 1. Second connection restores message recipient between claim & activation -> message NOT expired
      {
        const dbPath = join(dir, "case1-restore-recipient.db");
        let conn2: ControlPlaneDb | null = null;
        let recipId = "";
        const rig = createRig({
          dbPath,
          onAfterClaim: () => {
            conn2!.updateTask(recipId, { status: "running" });
            rig.liveTasks.add(recipId);
          },
        });
        conn2 = new ControlPlaneDb(dbPath);
        try {
          const { db, store, triage, liveTasks } = rig;
          const run = db.createRun({ goal: "Restore recipient", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
          const asker = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "Asker", objective: "o", profileId: "acp:claude" });
          const recip = db.createTask({ runId: run.id, tier: 2, kind: "worker", title: "Recip", objective: "o", profileId: "acp:claude" });
          recipId = recip.id;
          db.updateTask(asker.id, { status: "running" });
          liveTasks.add(asker.id);
          db.updateTask(recip.id, { status: "failed" });
          const msg = db.createMessage(run.id, asker.id, recip.id, "Still alive?");

          const res = await triage.sweepOnce();
          expect(res.action.dispatched).toBe(0);
          expect(res.action.skippedVeto).toBeGreaterThanOrEqual(1);
          expect(db.getMessage(msg.id)?.status).toBe("open");
          expect(store.getCause(makeCauseId("open_message", msg.id))?.state).not.toBe("resolved");
        } finally {
          rig.triage.stop();
          conn2.close();
          rig.db.close();
        }
      }

      // 2. Second connection relinks goal to a new run between claim & activation -> zero resumeGoal
      {
        const dbPath = join(dir, "case2-relink-goal.db");
        let conn2: ControlPlaneDb | null = null;
        let goalId = "";
        let newRunId = "";
        const rig = createRig({
          dbPath,
          config: { restartRecovery: "resume" },
          onAfterClaim: () => {
            conn2!.db.prepare(`UPDATE goals SET linked_run_id = ? WHERE id = ?`).run(newRunId, goalId);
          },
        });
        conn2 = new ControlPlaneDb(dbPath);
        try {
          const { db, calls, triage } = rig;
          const run1 = db.createRun({ goal: "Old run", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
          const run2 = db.createRun({ goal: "New run", repoRoot: "/repo-a", baseBranch: "main", baseSha: "bbb" });
          newRunId = run2.id;
          const t1 = db.createTask({ runId: run1.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
          db.updateTask(t1.id, { status: "failed", summary: "Interrupted by daemon restart" });
          db.updateRunStatus(run1.id, "failed", "restart", "daemon_restart");
          goalId = "goal-relink";
          insertGoalRow(db, { id: goalId, status: "executing", linkedRunId: run1.id });

          const res = await triage.sweepOnce();
          expect(calls.resumeGoal).toHaveLength(0);
          expect(res.action.skippedVeto).toBe(1);
        } finally {
          rig.triage.stop();
          conn2.close();
          rig.db.close();
        }
      }

      // 3. Second connection changes steer target generation between claim & activation -> zero steerTask
      {
        const dbPath = join(dir, "case3-steer-gen.db");
        let conn2: ControlPlaneDb | null = null;
        let targetTaskId = "";
        const rig = createRig({
          dbPath,
          onAfterClaim: () => {
            conn2!.updateTask(targetTaskId, { generation: 2 });
          },
        });
        conn2 = new ControlPlaneDb(dbPath);
        try {
          const { db, calls, triage, liveTasks } = rig;
          const run = db.createRun({ goal: "Steer gen", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
          const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
          const w1 = db.createTask({ runId: run.id, tier: 2, kind: "worker", title: "W1", objective: "o", profileId: "acp:claude" });
          targetTaskId = t1.id;
          db.updateTask(t1.id, { status: "running" });
          liveTasks.add(t1.id);
          db.updateTask(w1.id, { status: "failed" });
          db.createEscalation({ runId: run.id, fromTaskId: w1.id, toTaskId: w1.id, severity: "P1", category: "blocked_dependency", title: "Orphan P1", evidencePaths: ["ev"] });

          const res = await triage.sweepOnce();
          expect(calls.steer).toHaveLength(0);
          expect(res.action.skippedVeto).toBe(1);
        } finally {
          rig.triage.stop();
          conn2.close();
          rig.db.close();
        }
      }

      // 4. Second connection reconciles the intent to 'failed' before activation -> activation refuses, zero dispatch, receipt preserved
      {
        const dbPath = join(dir, "case4-reconcile-intent.db");
        let store2: InboxTriageStore | null = null;
        let conn2: ControlPlaneDb | null = null;
        const rig = createRig({
          dbPath,
          config: { restartRecovery: "resume" },
          onAfterClaim: (effectKey) => {
            store2!.recordEffectReceipt(effectKey, "failed", "reconciled_by_other_conn", rig.clock.now());
          },
        });
        conn2 = new ControlPlaneDb(dbPath);
        store2 = new InboxTriageStore(conn2.db);
        try {
          const { db, store, calls, triage } = rig;
          const run = db.createRun({ goal: "Reconcile intent", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
          const t1 = db.createTask({ runId: run.id, tier: 1, kind: "orchestrator", title: "T1", objective: "o", profileId: "acp:claude" });
          db.updateTask(t1.id, { status: "failed", summary: "Interrupted by daemon restart" });
          db.updateRunStatus(run.id, "failed", "restart", "daemon_restart");

          const res = await triage.sweepOnce();
          expect(calls.resumeRun).toHaveLength(0);
          expect(res.action.dispatched).toBe(0);
          const eff = store.getEffect(`${makeCauseId("run_failed", run.id)}#a1`);
          expect(eff?.status).toBe("failed");
          expect(eff?.receipt).toBe("reconciled_by_other_conn");
        } finally {
          rig.triage.stop();
          conn2.close();
          rig.db.close();
        }
      }

      // 5. Reopen cause before late already_completed receipt -> reopened cause stays open (NOT closed by stale revision)
      {
        const dbPath = join(dir, "case5-reopen-late-receipt.db");
        let conn2: ControlPlaneDb | null = null;
        let store2: InboxTriageStore | null = null;
        let blockedTaskId = "";
        let causeId = "";
        const rig = createRig({
          dbPath,
          dispatcherOverrides: {
            async resumeTask() {
              return { outcome: "already_completed" };
            },
          },
          onBeforeReceipt: () => {
            const cur = store2!.getCause(causeId)!;
            store2!.resolveCause(
              {
                causeId,
                expectedRevision: cur.revision,
                outcomeKind: "source_resolved",
                outcome: "prior episode closed",
              },
              rig.clock.now(),
            );
            const closed = store2!.getCause(causeId)!;
            store2!.reopenCause(
              {
                causeId,
                expectedRevision: closed.revision,
                rule: "R07",
                disposition: "routine_auto",
                nextAction: "reopened episode",
                reason: "reopened_in_test",
              },
              rig.clock.now(),
            );
            conn2!.updateTask(blockedTaskId, { status: "blocked" });
          },
        });
        conn2 = new ControlPlaneDb(dbPath);
        store2 = new InboxTriageStore(conn2.db);
        try {
          const { db, store, triage } = rig;
          const run = db.createRun({ goal: "Late receipt", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
          const task = db.createTask({ runId: run.id, tier: 2, kind: "worker", title: "W1", objective: "o", profileId: "acp:claude" });
          blockedTaskId = task.id;
          causeId = makeCauseId("task_blocked", task.id);
          const esc = db.createEscalation({ runId: run.id, fromTaskId: task.id, toTaskId: null, severity: "P1", category: "blocked_dependency", title: "Need input", evidencePaths: ["ev"] });
          db.updateTask(task.id, { status: "blocked", summary: `Blocked on ${esc.id}` });
          db.ackEscalation(esc.id, "go ahead", true);
          const ev = findLatestEscalationUpdatedEvent(db, run.id, esc.id)!;
          triage.recordContinuation({
            escalationId: esc.id,
            resolutionEventSeq: ev.seq,
            taskId: task.id,
            taskGeneration: task.generation,
          });

          await triage.sweepOnce();
          const finalCause = store.getCause(causeId)!;
          expect(finalCause.state).toBe("acknowledged");
          expect(finalCause.resolvedAt).toBeNull();
        } finally {
          rig.triage.stop();
          conn2.close();
          rig.db.close();
        }
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("MF-3: durable settlement vs RAM-derived status (dead sender/live recipient, synthesized stop, RAM handle on failed run, paused goal, batch proof)", async () => {
    const rig = createRig();
    try {
      const { db, store, triage, liveTasks } = rig;

      // 1. Dead sender + live recipient -> message NOT expired, and batch proof claiming R18 is rejected
      const rMsg = db.createRun({ goal: "Dead sender", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const deadSender = db.createTask({ runId: rMsg.id, tier: 2, kind: "worker", title: "DeadSender", objective: "o", profileId: "acp:claude" });
      const liveRecip = db.createTask({ runId: rMsg.id, tier: 1, kind: "orchestrator", title: "LiveRecip", objective: "o", profileId: "acp:claude" });
      db.updateTask(deadSender.id, { status: "failed" });
      db.updateTask(liveRecip.id, { status: "running" });
      liveTasks.add(liveRecip.id);
      const msg1 = db.createMessage(rMsg.id, deadSender.id, liveRecip.id, "Can you still answer?");

      // 2. Synthesized stop: raw run status = 'active' with pending tasks and NO live RAM handle -> deriveRunStatus='stopped', MUST NOT expire message
      const rSynth = db.createRun({ goal: "Synth stop", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const q1 = db.createTask({ runId: rSynth.id, tier: 1, kind: "orchestrator", title: "Q1", objective: "o", profileId: "acp:claude" });
      const q2 = db.createTask({ runId: rSynth.id, tier: 2, kind: "worker", title: "Q2", objective: "o", profileId: "acp:claude" });
      db.updateTask(q1.id, { status: "pending" });
      db.updateTask(q2.id, { status: "pending" });
      const msg2 = db.createMessage(rSynth.id, q1.id, q2.id, "Queued run question");

      // 3. Failed run (restart handoff) -> surfaced human ask; then a task handle appears in RAM while runs.status stays 'failed' -> ask MUST NOT clear!
      const rFail = db.createRun({ goal: "Failed run", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const fTask = db.createTask({ runId: rFail.id, tier: 1, kind: "orchestrator", title: "FT1", objective: "o", profileId: "acp:claude" });
      db.updateTask(fTask.id, { status: "failed", summary: "Interrupted by daemon restart" });
      db.updateRunStatus(rFail.id, "failed", "restart", "daemon_restart");

      // 4. Goal awaiting_human -> surfaced human ask; then paused -> ask MUST NOT clear!
      const rGoal = db.createRun({ goal: "Goal pause", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      db.updateRunStatus(rGoal.id, "completed");
      insertGoalRow(db, {
        id: "goal-pause-mf3",
        status: "awaiting_human",
        linkedRunId: rGoal.id,
        verificationMode: "none",
        lastReviewSummary: "Verification verdict: needs human decision",
      });

      await triage.sweepOnce();

      // Assertions after initial sweep:
      expect(db.getMessage(msg1.id)?.status).toBe("open");
      expect(store.getCause(makeCauseId("open_message", msg1.id))?.rule).toBe("R19");
      expect(db.getMessage(msg2.id)?.status).toBe("open");
      expect(store.getCause(makeCauseId("open_message", msg2.id))?.rule).not.toBe("R18");

      const failCauseId = makeCauseId("run_failed", rFail.id);
      const goalCauseId = makeCauseId("goal_awaiting_human", "goal-pause-mf3");
      expect(store.getCause(failCauseId)?.state).toBe("surfaced");
      expect(store.getCause(goalCauseId)?.state).toBe("surfaced");
      const failAskId = store.getCause(failCauseId)!.askId!;
      const goalAskId = store.getCause(goalCauseId)!.askId!;

      // Inject RAM handle on unchanged failed run + pause the awaiting_human goal
      liveTasks.add(fTask.id);
      db.db.prepare(`UPDATE goals SET status = 'paused' WHERE id = 'goal-pause-mf3'`).run();

      await triage.sweepOnce();

      // Neither ask nor cause may be cleared!
      expect(store.getCause(failCauseId)?.state).toBe("surfaced");
      expect(store.getCause(failCauseId)?.outcomeKind).toBeNull();
      expect(store.getActiveAskForCause(failCauseId)?.askId).toBe(failAskId);

      expect(store.getCause(goalCauseId)?.state).toBe("surfaced");
      expect(store.getCause(goalCauseId)?.outcomeKind).toBeNull();
      expect(store.getActiveAskForCause(goalCauseId)?.askId).toBe(goalAskId);

      // Batch proof trying to close msg1 (dead sender) or msg2 (synthesized stop) MUST fail reverification!
      const cMsg1 = makeCauseId("open_message", msg1.id);
      const cMsg2 = makeCauseId("open_message", msg2.id);
      const created = store.createAssignment(
        {
          assignmentId: "batch-mf3-proof",
          scope: "/repo-mf3-proof",
          kind: "batch",
          ownerTaskId: null,
          ownerGeneration: null,
          expiresAt: rig.clock.now() + 60_000,
          causeIds: [cMsg1, cMsg2],
        },
        rig.clock.now(),
      );
      expect(created.created).toBe(true);
      const ingest = triage.ingestBatchResult(
        "batch-mf3-proof",
        `<triage_outcomes>${JSON.stringify([
          { cause_id: cMsg1, outcome: "proof", evidence: ["sender dead"] },
          { cause_id: cMsg2, outcome: "proof", evidence: ["derived stop"] },
        ])}</triage_outcomes>`,
      );
      expect(ingest.resolved).toBe(0);
      expect(ingest.unverifiedProofs).toBe(2);
      expect(db.getMessage(msg1.id)?.status).toBe("open");
      expect(db.getMessage(msg2.id)?.status).toBe("open");
    } finally {
      rig.triage.stop();
      rig.db.close();
    }
  });

  test("MF-4: separate persisted ask IDs and source refs for independent P0s around review, R13 gate/other-run waits, and ambiguous-gen blocks", async () => {
    const rig = createRig();
    try {
      const { db, store, triage } = rig;
      const now = rig.clock.now();

      // 1. Two independent P0s around the same review -> separate persisted ask IDs & source refs
      const rRev = db.createRun({ goal: "Review fold", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const orch = db.createTask({ runId: rRev.id, tier: 1, kind: "orchestrator", title: "Orch", objective: "o", profileId: "acp:claude" });
      const worker = db.createTask({ runId: rRev.id, parentId: orch.id, tier: 2, kind: "worker", title: "Worker", objective: "o", profileId: "acp:claude" });
      db.updateRunStatus(rRev.id, "completed");
      insertGoalRow(db, {
        id: "goal-mf4-rev",
        repoRoot: "/repo-a",
        status: "awaiting_human",
        linkedRunId: rRev.id,
        version: 1,
        reviewCycle: 1,
        lastReviewSummary: "Reviewer requested human decision — failing tests",
        now,
      });
      db.db.prepare(
        `INSERT INTO goal_reviews(id, goal_id, goal_version, cycle, verdict, summary, criteria_results_json, blocking_issues_json, progress_fingerprint, created_at)
         VALUES ('rev-mf4-1', 'goal-mf4-rev', 1, 1, 'escalate_human', 'failing tests', '[]', '[]', 'fp-1', ?)`,
      ).run(now);
      // Independent P0 from worker (different task) and independent P0 with custom title
      const escIndep1 = db.createEscalation({
        runId: rRev.id,
        fromTaskId: worker.id,
        toTaskId: null,
        severity: "P0",
        category: "verification_failed",
        title: "Goal goal-mf4-rev needs human review",
        evidencePaths: ["/repo-a"],
        blocking: true,
      });
      const escIndep2 = db.createEscalation({
        runId: rRev.id,
        fromTaskId: orch.id,
        toTaskId: null,
        severity: "P0",
        category: "verification_failed",
        title: "Independent secret missing during check",
        evidencePaths: ["src/env.ts"],
        blocking: true,
      });

      // 2. Failed-run wait mentioning another run + failed-gate wait -> separate persisted asks (NOT folded under R13)
      const rFail = db.createRun({ goal: "Fail R13", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      db.updateRunStatus(rFail.id, "failed", "crash");
      insertGoalRow(db, {
        id: "goal-mf4-other-run",
        repoRoot: "/repo-a",
        status: "awaiting_human",
        linkedRunId: rFail.id,
        lastReviewSummary: "Linked run run-OTHER failed before goal verification",
        now,
      });
      insertGoalRow(db, {
        id: "goal-mf4-gate",
        repoRoot: "/repo-a",
        status: "awaiting_human",
        linkedRunId: rFail.id,
        lastReviewSummary: `Linked run ${rFail.id} failed and required deterministic gate(s) still fail (lint); resume or review.`,
        now,
      });

      // 3. Ambiguous generation blocked task (gen 2, no task.resumed seq proof) -> R08 investigation, NOT derived under esc
      const rAmb = db.createRun({ goal: "Ambiguous gen", repoRoot: "/repo-a", baseBranch: "main", baseSha: "aaa" });
      const tAmb = db.createTask({ runId: rAmb.id, tier: 2, kind: "worker", title: "Amb", objective: "o", profileId: "acp:claude" });
      const escAmb = db.createEscalation({
        runId: rAmb.id,
        fromTaskId: tAmb.id,
        toTaskId: null,
        severity: "P0",
        category: "blocked_dependency",
        title: "Gen 1 blocker",
        evidencePaths: ["ev"],
        blocking: true,
      });
      // Bump generation directly without a task.resumed event after escalation -> ambiguous generation!
      db.updateTask(tAmb.id, { generation: 2, status: "blocked", summary: `Blocked on ${escAmb.id}: old` });

      await triage.sweepOnce();

      const askGoalRev = store.getActiveAskForCause(makeCauseId("goal_awaiting_human", "goal-mf4-rev"));
      const askIndep1 = store.getActiveAskForCause(makeCauseId("escalation", escIndep1.id));
      const askIndep2 = store.getActiveAskForCause(makeCauseId("escalation", escIndep2.id));
      expect(askGoalRev).not.toBeNull();
      expect(askIndep1).not.toBeNull();
      expect(askIndep2).not.toBeNull();
      // All three MUST have distinct persisted ask IDs and distinct source refs!
      expect(new Set([askGoalRev!.askId, askIndep1!.askId, askIndep2!.askId]).size).toBe(3);
      expect(askGoalRev!.sourceRefs.map((r) => r.id)).not.toContain(escIndep1.id);
      expect(askGoalRev!.sourceRefs.map((r) => r.id)).not.toContain(escIndep2.id);

      const askOtherRun = store.getActiveAskForCause(makeCauseId("goal_awaiting_human", "goal-mf4-other-run"));
      const askGate = store.getActiveAskForCause(makeCauseId("goal_awaiting_human", "goal-mf4-gate"));
      expect(askOtherRun).not.toBeNull();
      expect(askGate).not.toBeNull();
      expect(askOtherRun!.askId).not.toBe(askGate!.askId);

      // Ambiguous generation blocked task must NOT be folded under escAmb — must become its own R08 cause!
      const ambTaskCause = store.getCause(makeCauseId("task_blocked", tAmb.id));
      expect(ambTaskCause).not.toBeNull();
      expect(ambTaskCause?.rule).toBe("R08");
      const askAmbEsc = store.getActiveAskForCause(makeCauseId("escalation", escAmb.id));
      expect(askAmbEsc).not.toBeNull();
      expect(askAmbEsc!.sourceRefs.map((r) => r.id)).not.toContain(tAmb.id);
    } finally {
      rig.triage.stop();
      rig.db.close();
    }
  });
});
