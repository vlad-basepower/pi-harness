import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { ControlPlaneDb, type EventRecord } from "../src/control-plane/db.js";

function seedRunAndTasks(db: ControlPlaneDb) {
  const run = db.createRun({ goal: "Test message expiry", repoRoot: "/tmp/repo", baseBranch: "main", baseSha: "abc123" });
  const t1 = db.createTask({
    runId: run.id,
    tier: 1,
    kind: "orchestrator",
    title: "Orch",
    objective: "Coord",
    readOnly: true,
    profileId: "acp:claude",
  });
  const t2 = db.createTask({
    runId: run.id,
    parentId: t1.id,
    tier: 2,
    kind: "worker",
    title: "Worker",
    objective: "Work",
    profileId: "acp:claude",
  });
  return { run, t1, t2 };
}

describe("guarded message expiry and reply mutual exclusion", () => {
  test("expireMessage transitions once, keeps reason separate from answer, and emits message.expired once", () => {
    const db = new ControlPlaneDb(":memory:");
    try {
      const { run, t1, t2 } = seedRunAndTasks(db);
      const events: EventRecord[] = [];
      db.onEvent((ev) => events.push(ev));

      const msg = db.createMessage(run.id, t1.id, t2.id, "Are you done?");
      events.length = 0;

      const first = db.expireMessage(msg.id, "recipient task completed", 1700000001000);
      expect(first.transitioned).toBe(true);
      expect(first.message?.status).toBe("expired");
      expect(first.message?.answer).toBeNull();
      expect(first.message?.expiredReason).toBe("recipient task completed");
      expect(first.message?.answeredAt).toBe(1700000001000);
      expect(events.map((e) => e.type)).toEqual(["message.expired"]);
      expect(events[0]?.payload).toEqual({
        id: msg.id,
        runId: run.id,
        fromTaskId: t1.id,
        toTaskId: t2.id,
        reason: "recipient task completed",
      });

      // Second expire is a no-op
      const second = db.expireMessage(msg.id, "duplicate reason", 1700000002000);
      expect(second.transitioned).toBe(false);
      expect(second.message?.status).toBe("expired");
      expect(second.message?.expiredReason).toBe("recipient task completed");
      expect(second.message?.answer).toBeNull();
      expect(events.map((e) => e.type)).toEqual(["message.expired"]);

      // Unknown message returns null/false
      const missing = db.expireMessage("msg-nonexistent", "missing");
      expect(missing).toEqual({ message: null, transitioned: false });
    } finally {
      db.close();
    }
  });

  test("replyMessage after expireMessage reports expired state and emits no second event", () => {
    const db = new ControlPlaneDb(":memory:");
    try {
      const { run, t1, t2 } = seedRunAndTasks(db);
      const events: EventRecord[] = [];
      db.onEvent((ev) => events.push(ev));

      const msg = db.createMessage(run.id, t1.id, t2.id, "Need status");
      events.length = 0;

      const exp = db.expireMessage(msg.id, "recipient failed", 1700000010000);
      expect(exp.transitioned).toBe(true);

      const lateReply = db.replyMessage(msg.id, "Late answer");
      expect(lateReply.status).toBe("expired");
      expect(lateReply.answer).toBeNull();
      expect(lateReply.expiredReason).toBe("recipient failed");
      expect(events.map((e) => e.type)).toEqual(["message.expired"]);

      const stored = db.getMessage(msg.id);
      expect(stored?.status).toBe("expired");
      expect(stored?.answer).toBeNull();
      expect(stored?.expiredReason).toBe("recipient failed");
    } finally {
      db.close();
    }
  });

  test("expireMessage after replyMessage does not overwrite answer; duplicate replyMessage is idempotent", () => {
    const db = new ControlPlaneDb(":memory:");
    try {
      const { run, t1, t2 } = seedRunAndTasks(db);
      const events: EventRecord[] = [];
      db.onEvent((ev) => events.push(ev));

      const msg = db.createMessage(run.id, t1.id, t2.id, "Which branch?");
      events.length = 0;

      const firstReply = db.replyMessage(msg.id, "main");
      expect(firstReply.status).toBe("answered");
      expect(firstReply.answer).toBe("main");
      expect(firstReply.expiredReason).toBeUndefined();
      expect(events.map((e) => e.type)).toEqual(["message.answered"]);

      // Same winning answer is idempotent and emits no second event
      const sameReply = db.replyMessage(msg.id, "main");
      expect(sameReply.status).toBe("answered");
      expect(sameReply.answer).toBe("main");
      expect(events.map((e) => e.type)).toEqual(["message.answered"]);

      // Different late answer does not overwrite winning answer or emit
      const conflictingReply = db.replyMessage(msg.id, "develop");
      expect(conflictingReply.status).toBe("answered");
      expect(conflictingReply.answer).toBe("main");
      expect(events.map((e) => e.type)).toEqual(["message.answered"]);

      // Late expire loses cleanly and emits nothing
      const lateExpire = db.expireMessage(msg.id, "recipient completed");
      expect(lateExpire.transitioned).toBe(false);
      expect(lateExpire.message?.status).toBe("answered");
      expect(lateExpire.message?.answer).toBe("main");
      expect(lateExpire.message?.expiredReason).toBeUndefined();
      expect(events.map((e) => e.type)).toEqual(["message.answered"]);
    } finally {
      db.close();
    }
  });

  test("legacy messages table without expired_reason gets no DDL on open; two connections enforce mutual exclusion", () => {
    const dir = mkdtempSync(join(tmpdir(), "pi-harness-msg-exp-"));
    const dbPath = join(dir, "cp.sqlite");
    try {
      // Seed an older schema where messages has no expired_reason column
      const initDb = new ControlPlaneDb(dbPath);
      const { run, t1, t2 } = seedRunAndTasks(initDb);
      const msg1 = initDb.createMessage(run.id, t1.id, t2.id, "Q1");
      const msg2 = initDb.createMessage(run.id, t1.id, t2.id, "Q2");
      initDb.close();

      // Drop expired_reason by recreating legacy messages table
      const raw = new Database(dbPath);
      raw.exec(`
        PRAGMA foreign_keys = OFF;
        CREATE TABLE messages_old (
          id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          from_task_id TEXT NOT NULL,
          to_task_id TEXT NOT NULL,
          question TEXT NOT NULL,
          answer TEXT,
          status TEXT NOT NULL DEFAULT 'open',
          created_at INTEGER NOT NULL,
          answered_at INTEGER
        );
        INSERT INTO messages_old SELECT id, run_id, from_task_id, to_task_id, question, answer, status, created_at, answered_at FROM messages;
        DROP TABLE messages;
        ALTER TABLE messages_old RENAME TO messages;
        PRAGMA foreign_keys = ON;
      `);
      raw.close();

      // Reopen via two ControlPlaneDb connections: constructor must NOT add expired_reason eagerly
      const connA = new ControlPlaneDb(dbPath);
      const connB = new ControlPlaneDb(dbPath);
      try {
        const colsBefore = (connA.db.prepare("PRAGMA table_info(messages)").all() as Array<{ name: string }>).map(
          (c) => c.name,
        );
        expect(colsBefore.includes("expired_reason")).toBe(false);

        // Conn A expires msg1; Conn B tries to reply msg1 -> sees expired
        const exp1 = connA.expireMessage(msg1.id, "run settled", 1700000100000);
        expect(exp1.transitioned).toBe(true);
        const rep1 = connB.replyMessage(msg1.id, "too late");
        expect(rep1.status).toBe("expired");
        expect(rep1.answer).toBeNull();
        expect(rep1.expiredReason).toBe("run settled");

        // Conn B replies msg2; Conn A tries to expire msg2 -> sees answered
        const rep2 = connB.replyMessage(msg2.id, "on time");
        expect(rep2.status).toBe("answered");
        const exp2 = connA.expireMessage(msg2.id, "too late", 1700000200000);
        expect(exp2.transitioned).toBe(false);
        expect(exp2.message?.status).toBe("answered");
        expect(exp2.message?.answer).toBe("on time");
      } finally {
        connA.close();
        connB.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
