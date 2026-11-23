import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import {
  INBOX_TRIAGE_TABLES,
  InboxTriageStore,
  makeCauseId,
} from "../src/control-plane/inbox-triage-store.js";

function listTriageTables(db: Database): string[] {
  const rows = db
    .prepare(`SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'inbox_triage_%' ORDER BY name`)
    .all() as Array<{ name: string }>;
  return rows.map((r) => r.name);
}

describe("InboxTriageStore ledger invariants", () => {
  test("lazy DDL: constructor and read-only queries create zero tables before ensureSchema", () => {
    const db = new Database(":memory:");
    try {
      const store = new InboxTriageStore(db);
      expect(store.hasSchema()).toBe(false);
      expect(listTriageTables(db)).toEqual([]);

      // All read methods must stay side-effect free before activation
      expect(store.getCause("escalation:esc-1")).toBeNull();
      expect(store.listCauses()).toEqual([]);
      expect(store.listDueCauses(1_000_000)).toEqual([]);
      expect(store.listOwnedCauses()).toEqual([]);
      expect(store.listOpenItems()).toEqual([]);
      expect(store.listOutstandingEffects()).toEqual([]);
      expect(store.listOpenAssignments()).toEqual([]);
      expect(store.listActiveAsks()).toEqual([]);
      expect(store.listJournal()).toEqual([]);
      expect(store.getContinuation("esc-1")).toBeNull();
      expect(listTriageTables(db)).toEqual([]);

      store.ensureSchema();
      expect(store.hasSchema()).toBe(true);
      expect(listTriageTables(db).sort()).toEqual([...INBOX_TRIAGE_TABLES].sort());
    } finally {
      db.close();
    }
  });

  test("double ack creates one cause row and one acked journal entry", () => {
    const db = new Database(":memory:");
    try {
      const store = new InboxTriageStore(db);
      const causeId = makeCauseId("escalation", "esc-1");
      const first = store.ackCause(
        {
          causeId,
          originKind: "escalation",
          originSourceId: "esc-1",
          runId: "run-1",
          goalId: "goal-1",
          repoRoot: "/repo",
          rule: "R04",
          disposition: "routine_owned",
          nextAction: "assign owner",
        },
        10_000,
      );
      expect(first.inserted).toBe(true);
      expect(first.cause.revision).toBe(1);
      expect(first.journal?.type).toBe("acked");

      const second = store.ackCause(
        {
          causeId,
          originKind: "escalation",
          originSourceId: "esc-1",
          runId: "run-1",
          goalId: "goal-1",
          repoRoot: "/repo",
          rule: "R05",
          disposition: "routine_owned",
          nextAction: "different action",
        },
        20_000,
      );
      expect(second.inserted).toBe(false);
      expect(second.journal).toBeNull();
      expect(second.cause.rule).toBe("R04");
      expect(second.cause.ackedAt).toBe(10_000);
      expect(store.listCauses()).toHaveLength(1);
      expect(store.listJournal({ causeId })).toHaveLength(1);
    } finally {
      db.close();
    }
  });

  test("CAS revision semantics across two connections on a file-backed DB", () => {
    const dir = mkdtempSync(join(tmpdir(), "pi-harness-it-cas-"));
    const dbPath = join(dir, "triage.sqlite");
    try {
      const dbA = new Database(dbPath);
      const dbB = new Database(dbPath);
      dbA.exec("PRAGMA busy_timeout = 5000;");
      dbB.exec("PRAGMA busy_timeout = 5000;");
      try {
        const storeA = new InboxTriageStore(dbA);
        const storeB = new InboxTriageStore(dbB);
        storeA.ensureSchema();

        const causeId = makeCauseId("task_failed", "t-1");
        const ack = storeA.ackCause(
          {
            causeId,
            originKind: "task_failed",
            originSourceId: "t-1",
            runId: "run-1",
            goalId: null,
            repoRoot: "/repo",
            rule: "R09",
            disposition: "routine_owned",
            nextAction: "assign parent",
          },
          1000,
        );

        // Both connections saw revision 1; only the first CAS write succeeds.
        const win = storeA.setOwner(
          {
            causeId,
            expectedRevision: ack.cause.revision,
            ownerKind: "task",
            ownerId: "t-parent",
            ownerEpoch: "g1",
            nextAction: "wait parent",
            nextCheckAt: 2000,
            leaseExpiresAt: 301_000,
            assignmentExpiresAt: 1_201_000,
          },
          1001,
        );
        expect(win.updated).toBe(true);
        expect(win.cause?.revision).toBe(2);

        const lose = storeB.setOwner(
          {
            causeId,
            expectedRevision: ack.cause.revision,
            ownerKind: "task",
            ownerId: "t-other",
            ownerEpoch: "g2",
            nextAction: "wait other",
            nextCheckAt: 2000,
            leaseExpiresAt: 301_000,
            assignmentExpiresAt: 1_201_000,
          },
          1002,
        );
        expect(lose.updated).toBe(false);
        expect(lose.cause?.ownerId).toBe("t-parent");
        expect(lose.cause?.revision).toBe(2);
      } finally {
        dbA.close();
        dbB.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("unique outstanding effect per target across causes and late receipt cannot resurrect resolved cause", () => {
    const db = new Database(":memory:");
    try {
      const store = new InboxTriageStore(db);
      const c1 = store.ackCause(
        {
          causeId: makeCauseId("task_blocked", "t-1"),
          originKind: "task_blocked",
          originSourceId: "t-1",
          runId: "run-1",
          goalId: null,
          repoRoot: "/repo",
          rule: "R07",
          disposition: "routine_auto",
          nextAction: "resume task",
        },
        10_000,
      ).cause;
      const c2 = store.ackCause(
        {
          causeId: makeCauseId("task_blocked", "t-2"),
          originKind: "task_blocked",
          originSourceId: "t-2",
          runId: "run-1",
          goalId: null,
          repoRoot: "/repo",
          rule: "R07",
          disposition: "routine_auto",
          nextAction: "resume shared run",
        },
        10_000,
      ).cause;

      // c1 claims intent targeting ("run", "run-1")
      const claim1 = store.claimEffectIntent(
        {
          causeId: c1.causeId,
          expectedRevision: c1.revision,
          maxAutoAttempts: 3,
          primitive: "resumeRun",
          targetKind: "run",
          targetId: "run-1",
          targetEpoch: "s1",
          nextCheckAt: 70_000,
        },
        10_000,
      );
      expect(claim1.claimed).toBe(true);
      expect(claim1.effect?.effectKey).toBe("task_blocked:t-1#a1");
      expect(claim1.cause?.attempts).toBe(1);

      // c2 targeting the SAME ("run", "run-1") is fenced by unique partial index
      const claim2 = store.claimEffectIntent(
        {
          causeId: c2.causeId,
          expectedRevision: c2.revision,
          maxAutoAttempts: 3,
          primitive: "resumeRun",
          targetKind: "run",
          targetId: "run-1",
          targetEpoch: "s1",
          nextCheckAt: 70_000,
        },
        10_000,
      );
      expect(claim2.claimed).toBe(false);
      expect(claim2.reason).toBe("target_busy");
      expect(store.getCause(c2.causeId)?.attempts).toBe(0);

      // Mark dispatched, then resolve c1 BEFORE receipt arrives
      expect(store.markEffectDispatched(claim1.effect!.effectKey, 10_500)).toBe(true);
      const resolved = store.resolveCause(
        {
          causeId: c1.causeId,
          expectedRevision: claim1.cause!.revision,
          outcomeKind: "source_resolved",
          outcome: "task completed",
        },
        11_000,
      );
      expect(resolved.resolved).toBe(true);
      expect(resolved.cause?.state).toBe("resolved");

      // Late receipt arrives attempting to patch c1 back to 'owned'
      const late = store.recordEffectReceipt(
        claim1.effect!.effectKey,
        "confirmed",
        "resumed",
        12_000,
        {
          expectedRevision: claim1.cause!.revision,
          state: "owned",
          ownerKind: "task",
          ownerId: "t-1",
          ownerEpoch: "g2",
        },
      );
      expect(late.recorded).toBe(true);
      expect(late.causeUpdated).toBe(false);
      expect(store.getCause(c1.causeId)?.state).toBe("resolved");
      expect(store.getCause(c1.causeId)?.outcome).toBe("task completed");

      // Once c1's effect reached terminal status, c2 can now claim ("run", "run-1")
      const claim2Retry = store.claimEffectIntent(
        {
          causeId: c2.causeId,
          expectedRevision: c2.revision,
          maxAutoAttempts: 3,
          primitive: "resumeRun",
          targetKind: "run",
          targetId: "run-1",
          targetEpoch: "s1",
          nextCheckAt: 72_000,
        },
        12_500,
      );
      expect(claim2Retry.claimed).toBe(true);
    } finally {
      db.close();
    }
  });

  test("notify-once blocker ask, coalesced scope_decision ask, and journal without run_id", () => {
    const db = new Database(":memory:");
    try {
      const store = new InboxTriageStore(db);
      // Goal without a linked run (runId = null)
      const gCause = store.ackCause(
        {
          causeId: makeCauseId("goal_awaiting_human", "goal-99"),
          originKind: "goal_awaiting_human",
          originSourceId: "goal-99",
          runId: null,
          goalId: "goal-99",
          repoRoot: "/repo-a",
          rule: "R15",
          disposition: "human_blocker",
          nextAction: "surface to human",
        },
        5000,
      ).cause;

      const s1 = store.surfaceCause(
        {
          causeId: gCause.causeId,
          expectedRevision: gCause.revision,
          askId: "ask-goal-99",
          ask: "Review verdict requires decision: accept | amend | cancel",
          blockedWork: "goal-99 v1 (awaiting_human)",
          sourceRefs: [{ kind: "goal", id: "goal-99" }],
        },
        5001,
      );
      expect(s1.surfaced).toBe(true);
      expect(s1.notified).toBe(true);
      expect(s1.ask?.notifiedAt).toBe(5001);
      expect(s1.journals.map((j) => j.type)).toEqual(["surfaced", "notified"]);

      // Re-surfacing updates in place without a second notified journal entry
      const s2 = store.surfaceCause(
        {
          causeId: gCause.causeId,
          expectedRevision: s1.cause!.revision,
          askId: "ask-goal-99-new",
          ask: "Updated ask text",
          blockedWork: "goal-99 v1 (awaiting_human)",
          sourceRefs: [{ kind: "goal", id: "goal-99" }],
        },
        6000,
      );
      expect(s2.surfaced).toBe(false);
      expect(s2.notified).toBe(false);
      expect(s2.ask?.askId).toBe("ask-goal-99");
      expect(s2.ask?.ask).toBe("Updated ask text");

      // Journal works cleanly with runId = null
      const journal = store.listJournal({ causeId: gCause.causeId });
      expect(journal.map((j) => j.type)).toEqual(["acked", "surfaced", "notified"]);
      expect(journal.every((j) => j.runId === null && j.goalId === "goal-99")).toBe(true);

      // Coalesced scope_decision ask across 2 parked causes in same scope notifies ONCE
      const p1 = store.ackCause(
        {
          causeId: makeCauseId("escalation", "esc-10"),
          originKind: "escalation",
          originSourceId: "esc-10",
          runId: "run-10",
          goalId: null,
          repoRoot: "/repo-a",
          rule: "R05",
          disposition: "routine_owned",
          nextAction: "batch investigate",
        },
        7000,
      ).cause;
      const p2 = store.ackCause(
        {
          causeId: makeCauseId("escalation", "esc-11"),
          originKind: "escalation",
          originSourceId: "esc-11",
          runId: "run-10",
          goalId: null,
          repoRoot: "/repo-a",
          rule: "R05",
          disposition: "routine_owned",
          nextAction: "batch investigate",
        },
        7000,
      ).cause;

      const pk1 = store.parkCause(
        {
          causeId: p1.causeId,
          expectedRevision: p1.revision,
          reason: "budget exhausted",
          nextAction: "create follow-up",
          scope: "/repo-a",
          scopeAskId: "ask-scope-a-1",
          scopeSourceRefs: [{ kind: "escalation", id: "esc-10" }],
        },
        8000,
      );
      expect(pk1.parked).toBe(true);
      expect(pk1.scopeNotified).toBe(true);
      expect(pk1.scopeAsk?.ask).toContain("1 finding needs");

      const pk2 = store.parkCause(
        {
          causeId: p2.causeId,
          expectedRevision: p2.revision,
          reason: "budget exhausted",
          nextAction: "create follow-up",
          scope: "/repo-a",
          scopeAskId: "ask-scope-a-2",
          scopeSourceRefs: [
            { kind: "escalation", id: "esc-10" },
            { kind: "escalation", id: "esc-11" },
          ],
        },
        9000,
      );
      expect(pk2.parked).toBe(true);
      expect(pk2.scopeNotified).toBe(false);
      expect(pk2.scopeAsk?.askId).toBe("ask-scope-a-1");
      expect(pk2.scopeAsk?.ask).toContain("2 findings need");
      expect(pk2.scopeAsk?.sourceRefs).toHaveLength(2);
      expect(store.listActiveAsks("scope_decision")).toHaveLength(1);
    } finally {
      db.close();
    }
  });

  test("reopen preserves attempt budget; rearm grants fresh budget; one open batch per scope", () => {
    const db = new Database(":memory:");
    try {
      const store = new InboxTriageStore(db);
      const causeId = makeCauseId("escalation", "esc-50");
      const ack = store.ackCause(
        {
          causeId,
          originKind: "escalation",
          originSourceId: "esc-50",
          runId: "run-50",
          goalId: null,
          repoRoot: "/repo",
          rule: "R04",
          disposition: "routine_owned",
          nextAction: "steer",
        },
        1000,
      ).cause;

      const claim = store.claimEffectIntent(
        {
          causeId,
          expectedRevision: ack.revision,
          maxAutoAttempts: 3,
          primitive: "steerTask",
          targetKind: "task",
          targetId: "t-1",
          targetEpoch: "g1",
          nextCheckAt: 61_000,
        },
        1000,
      );
      expect(claim.claimed).toBe(true);

      const res = store.resolveCause(
        {
          causeId,
          expectedRevision: claim.cause!.revision,
          outcomeKind: "answered",
          outcome: "fixed",
        },
        2000,
      );
      expect(res.resolved).toBe(true);

      // Reopen preserves attempts=1 and history; second reopen is no-op
      const r1 = store.reopenCause(
        {
          causeId,
          expectedRevision: res.cause!.revision,
          rule: "R04",
          disposition: "routine_owned",
          nextAction: "re-triage",
          reason: "escalation reopened",
        },
        3000,
      );
      expect(r1.reopened).toBe(true);
      expect(r1.cause?.attempts).toBe(1);
      expect(r1.cause?.state).toBe("acknowledged");

      const r2 = store.reopenCause(
        {
          causeId,
          expectedRevision: r1.cause!.revision,
          rule: "R04",
          disposition: "routine_owned",
          nextAction: "re-triage",
          reason: "duplicate scan",
        },
        3500,
      );
      expect(r2.reopened).toBe(false);

      // Rearm resets attempts to 0 and records rearmed
      const rearmed = store.rearmCause(causeId, 4000, "operator retry");
      expect(rearmed.rearmed).toBe(true);
      expect(rearmed.cause?.attempts).toBe(0);
      expect(rearmed.cause?.firstDispatchAt).toBeNull();
      expect(rearmed.journal?.type).toBe("rearmed");

      // One open batch per scope enforced by partial unique index
      const b1 = store.createAssignment(
        {
          assignmentId: "batch-1",
          scope: "/repo",
          kind: "batch",
          ownerTaskId: null,
          ownerGeneration: null,
          expiresAt: 1_204_000,
          causeIds: [causeId],
        },
        4000,
      );
      expect(b1.created).toBe(true);
      expect(store.listAssignmentMembers("batch-1")).toEqual([causeId]);

      const b2 = store.createAssignment(
        {
          assignmentId: "batch-2",
          scope: "/repo",
          kind: "batch",
          ownerTaskId: null,
          ownerGeneration: null,
          expiresAt: 1_204_000,
          causeIds: [causeId],
        },
        4100,
      );
      expect(b2.created).toBe(false);
      expect(b2.assignment?.assignmentId).toBe("batch-1");

      // Continuation grant (Amendment 1) is bound to (escalationId, resolutionEventSeq, taskId, taskGeneration),
      // revocable on reopen, and consumed at most once
      const cont = store.recordContinuation(
        { escalationId: "esc-50", resolutionEventSeq: 101, taskId: "t-blocked", taskGeneration: 1 },
        4200,
        { runId: "run-50" },
      );
      expect(cont.recorded).toBe(true);
      expect(store.getContinuation("esc-50", 101)?.consumedAt).toBeNull();
      expect(store.getContinuation("esc-50", 101)?.revokedAt).toBeNull();
      // Duplicate grant on the same (escalationId, resolutionEventSeq) is a no-op
      const dupCont = store.recordContinuation(
        { escalationId: "esc-50", resolutionEventSeq: 101, taskId: "t-blocked", taskGeneration: 1 },
        4250,
        { runId: "run-50" },
      );
      expect(dupCont.recorded).toBe(false);
      // Wrong generation or wrong seq cannot consume
      expect(store.consumeContinuation("esc-50", 101, "t-blocked", 2, 4280)).toBe(false);
      expect(store.consumeContinuation("esc-50", 999, "t-blocked", 1, 4290)).toBe(false);
      expect(store.consumeContinuation("esc-50", 101, "t-blocked", 1, 4300)).toBe(true);
      expect(store.consumeContinuation("esc-50", 101, "t-blocked", 1, 4400)).toBe(false);
      expect(store.getContinuation("esc-50", 101)?.consumedAt).toBe(4300);

      // Reopen revokes unconsumed grants
      const cont2 = store.recordContinuation(
        { escalationId: "esc-51", resolutionEventSeq: 201, taskId: "t-blocked", taskGeneration: 1 },
        4500,
        { runId: "run-50" },
      );
      expect(cont2.recorded).toBe(true);
      const rev = store.revokeContinuations("esc-51", 4600, { runId: "run-50" });
      expect(rev.revoked).toBe(1);
      expect(store.getContinuation("esc-51", 201)?.revokedAt).toBe(4600);
      expect(store.consumeContinuation("esc-51", 201, "t-blocked", 1, 4700)).toBe(false);
    } finally {
      db.close();
    }
  });
});
