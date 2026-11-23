import { describe, expect, test } from "bun:test";
import { DEFAULT_INBOX_TRIAGE_CONFIG, type InboxTriageConfig } from "../src/config.js";
import type {
  EscalationRecord,
  MergeQueueRecord,
  MessageRecord,
  RunRecord,
  TaskRecord,
} from "../src/control-plane/db.js";
import { makeCauseId, type TriageContinuationRecord } from "../src/control-plane/inbox-triage-store.js";
import {
  type TriageFacts,
  type TriageGoalReviewSummary,
  type TriageGoalSummary,
  type TriageRunStatusEvent,
  buildInboxItems,
  evaluateVeto,
  routeItem,
} from "../src/control-plane/inbox-triage.js";
import type { RunQuotaPauseStatus } from "../src/control-plane/orchestrator.js";
import type { InboxItem } from "../src/protocol/daemon-rpc.js";

const NOW = 1_800_000_000_000;
const OLD = NOW - 25 * 60 * 60 * 1000;
const RECENT = NOW - 60 * 1000;

function makeRun(overrides: Partial<RunRecord> & { id: string }): RunRecord {
  return {
    goal: "Synthetic goal",
    repoRoot: "/tmp/synth-repo",
    baseBranch: "main",
    baseSha: "0000000",
    status: "active",
    coordinationGroup: null,
    isolationMode: "standalone",
    createdAt: OLD,
    updatedAt: RECENT,
    ...overrides,
  };
}

function makeTask(overrides: Partial<TaskRecord> & { id: string; runId: string }): TaskRecord {
  return {
    parentId: null,
    tier: 2,
    kind: "worker",
    title: `Task ${overrides.id}`,
    objective: "Do synthetic work",
    criteria: "",
    status: "running",
    profileId: "acp:claude",
    model: null,
    effort: null,
    readOnly: false,
    maxTurns: 40,
    generation: 1,
    turnsUsed: 1,
    contextUsed: 0,
    contextSize: 0,
    worktreeId: null,
    sessionId: null,
    summary: null,
    handoffJson: null,
    resultJson: null,
    createdAt: OLD,
    updatedAt: RECENT,
    ...overrides,
  };
}

function makeEsc(overrides: Partial<EscalationRecord> & { id: string; runId: string; fromTaskId: string }): EscalationRecord {
  return {
    toTaskId: null,
    severity: "P1",
    category: "code_defect",
    title: `Escalation ${overrides.id}`,
    evidencePaths: ["src/a.ts"],
    attempted: ["checked logs"],
    suggestedSplits: [],
    blocking: true,
    status: "open",
    resolution: null,
    createdAt: OLD,
    ackedAt: null,
    ...overrides,
  };
}

function makeGoal(overrides: Partial<TriageGoalSummary> & { id: string }): TriageGoalSummary {
  return {
    chatSessionId: "chat-1",
    repoRoot: "/tmp/synth-repo",
    version: 1,
    title: `Goal ${overrides.id}`,
    status: "executing",
    linkedRunId: null,
    reviewCycle: 0,
    verificationMode: "auto",
    lastReviewSummary: null,
    createdAt: OLD,
    updatedAt: RECENT,
    ...overrides,
  };
}

function makeMerge(overrides: Partial<MergeQueueRecord> & { id: string; runId: string; taskId: string }): MergeQueueRecord {
  return {
    worktreeId: "wt-1",
    branch: "harness/run-1/t-1",
    status: "ci_failed",
    log: "tests failed",
    createdAt: OLD,
    finishedAt: OLD + 1000,
    ...overrides,
  };
}

function makeMsg(overrides: Partial<MessageRecord> & { id: string; runId: string; fromTaskId: string; toTaskId: string }): MessageRecord {
  return {
    question: "Are you done?",
    answer: null,
    status: "open",
    createdAt: OLD,
    answeredAt: null,
    ...overrides,
  };
}

function buildFacts(input: {
  now?: number;
  config?: Partial<InboxTriageConfig>;
  authoritative?: boolean;
  runs?: RunRecord[];
  tasks?: TaskRecord[];
  liveTaskIds?: string[];
  escalations?: EscalationRecord[];
  merges?: MergeQueueRecord[];
  messages?: MessageRecord[];
  goals?: TriageGoalSummary[];
  runsEverLinkedToGoal?: string[];
  reviews?: TriageGoalReviewSummary[];
  runStatusEvents?: Array<TriageRunStatusEvent & { runId: string }>;
  quotaStatuses?: Record<string, RunQuotaPauseStatus | null>;
  continuations?: TriageContinuationRecord[];
  missingQuotaSinceByCauseId?: Record<string, number>;
  triageOwnedTaskIds?: string[];
  escalationGeneration?: Record<string, number>;
  escalationCreatedSeq?: Record<string, number>;
  taskCurrentGenStartedSeq?: Record<string, number>;
}): TriageFacts {
  const runs = new Map<string, RunRecord>();
  const rawRunStatus = new Map<string, RunRecord["status"]>();
  for (const r of input.runs ?? []) {
    runs.set(r.id, r);
    rawRunStatus.set(r.id, r.status);
  }
  const tasks = new Map<string, TaskRecord>();
  const tasksByRun = new Map<string, TaskRecord[]>();
  for (const t of input.tasks ?? []) {
    tasks.set(t.id, t);
    const list = tasksByRun.get(t.runId) ?? [];
    list.push(t);
    tasksByRun.set(t.runId, list);
  }
  const escalations = new Map<string, EscalationRecord>();
  const escalationsByRun = new Map<string, EscalationRecord[]>();
  for (const e of input.escalations ?? []) {
    escalations.set(e.id, e);
    const list = escalationsByRun.get(e.runId) ?? [];
    list.push(e);
    escalationsByRun.set(e.runId, list);
  }
  const merges = new Map<string, MergeQueueRecord>();
  const mergesByRun = new Map<string, MergeQueueRecord[]>();
  for (const m of input.merges ?? []) {
    merges.set(m.id, m);
    const list = mergesByRun.get(m.runId) ?? [];
    list.push(m);
    mergesByRun.set(m.runId, list);
  }
  const messages = new Map<string, MessageRecord>();
  const messagesByRun = new Map<string, MessageRecord[]>();
  for (const m of input.messages ?? []) {
    messages.set(m.id, m);
    const list = messagesByRun.get(m.runId) ?? [];
    list.push(m);
    messagesByRun.set(m.runId, list);
  }
  const goals = new Map<string, TriageGoalSummary>();
  const goalsByRunId = new Map<string, TriageGoalSummary[]>();
  const runsEverLinkedToGoal = new Set<string>(input.runsEverLinkedToGoal ?? []);
  for (const g of input.goals ?? []) {
    goals.set(g.id, g);
    if (g.linkedRunId) {
      runsEverLinkedToGoal.add(g.linkedRunId);
      const list = goalsByRunId.get(g.linkedRunId) ?? [];
      list.push(g);
      goalsByRunId.set(g.linkedRunId, list);
    }
  }
  const reviewsByGoalId = new Map<string, TriageGoalReviewSummary[]>();
  for (const rev of input.reviews ?? []) {
    const list = reviewsByGoalId.get(rev.goalId) ?? [];
    list.push(rev);
    reviewsByGoalId.set(rev.goalId, list);
  }
  const latestRunStatusEvent = new Map<string, TriageRunStatusEvent>();
  for (const ev of input.runStatusEvents ?? []) {
    latestRunStatusEvent.set(ev.runId, ev);
  }
  const quotaStatusByRunId = new Map<string, RunQuotaPauseStatus | null>(
    Object.entries(input.quotaStatuses ?? {}),
  );
  const continuations = new Map<string, TriageContinuationRecord>(
    (input.continuations ?? []).map((c) => [c.escalationId, c]),
  );

  return {
    now: input.now ?? NOW,
    config: { ...DEFAULT_INBOX_TRIAGE_CONFIG, mode: "on", schedulerHost: "host-a", ...(input.config ?? {}) },
    authoritative: input.authoritative ?? true,
    runs,
    rawRunStatus,
    tasks,
    tasksByRun,
    liveTaskIds: new Set(input.liveTaskIds ?? []),
    escalations,
    escalationsByRun,
    merges,
    mergesByRun,
    messages,
    messagesByRun,
    goals,
    goalsByRunId,
    runsEverLinkedToGoal,
    reviewsByGoalId,
    latestRunStatusEvent,
    quotaStatusByRunId,
    continuations,
    missingQuotaSinceByCauseId: new Map(Object.entries(input.missingQuotaSinceByCauseId ?? {})),
    triageOwnedTaskIds: new Set(input.triageOwnedTaskIds ?? []),
    latestEventSeqByTask: new Map(),
    latestEventSeqByRun: new Map(),
    steerUndeliveredAtByTask: new Map(),
    escalationGeneration: input.escalationGeneration ? new Map(Object.entries(input.escalationGeneration)) : undefined,
    escalationCreatedSeq: input.escalationCreatedSeq ? new Map(Object.entries(input.escalationCreatedSeq)) : undefined,
    taskCurrentGenStartedSeq: input.taskCurrentGenStartedSeq
      ? new Map(Object.entries(input.taskCurrentGenStartedSeq))
      : undefined,
  };
}

function itemsFromFacts(facts: TriageFacts, includeAckedEscalations = true): InboxItem[] {
  const goalByRunId = new Map<string, TriageGoalSummary>();
  for (const [runId, list] of facts.goalsByRunId) {
    if (list[0]) goalByRunId.set(runId, list[0]);
  }
  return buildInboxItems({
    goals: [...facts.goals.values()],
    runEntries: [...facts.runs.values()].map((run) => ({
      run,
      goalId: goalByRunId.get(run.id)?.id ?? null,
      tasks: facts.tasksByRun.get(run.id) ?? [],
      quotaPause: facts.quotaStatusByRunId.get(run.id) ?? null,
    })),
    escalations: [...facts.escalations.values()],
    merges: [...facts.merges.values()],
    messages: [...facts.messages.values()],
    goalByRunId,
    includeAckedEscalations,
  });
}

describe("buildInboxItems parity", () => {
  test("matches server.ts sorting, ids, and open-vs-acked escalation behavior", () => {
    const run = makeRun({ id: "run-1", status: "active" });
    const g = makeGoal({ id: "goal-1", status: "awaiting_human", linkedRunId: "run-1", updatedAt: RECENT + 10 });
    const escOpen = makeEsc({ id: "esc-1", runId: "run-1", fromTaskId: "t-1", severity: "P0", status: "open", createdAt: OLD });
    const escAcked = makeEsc({ id: "esc-2", runId: "run-1", fromTaskId: "t-1", severity: "P1", status: "acked", createdAt: RECENT });
    const facts = buildFacts({ runs: [run], goals: [g], escalations: [escOpen, escAcked] });

    // Legacy snapshot projection excludes acked escalations and orders P0 escalation before P0 goal
    const legacy = itemsFromFacts(facts, false);
    expect(legacy.map((i) => i.id)).toEqual(["inbox:esc:esc-1", "inbox:goal:goal-1"]);

    // Triage sweep enumeration includes acked escalations
    const triageItems = itemsFromFacts(facts, true);
    expect(triageItems.map((i) => i.id)).toEqual(["inbox:esc:esc-1", "inbox:goal:goal-1", "inbox:esc:esc-2"]);
  });
});

describe("evaluateVeto (Contract Section 6)", () => {
  test("enforces mode, authority, stopped run, paused goal, quota pause, Done-Review, container, target drift, and blocking escalation", () => {
    const runStopped = makeRun({ id: "run-stop", status: "stopped" });
    const runQuota = makeRun({ id: "run-q", status: "paused_quota" });
    const runActive = makeRun({ id: "run-a", status: "active" });
    const runContainer = makeRun({ id: "director-tasks-chat-1", status: "active" });
    const pausedGoal = makeGoal({ id: "goal-p", status: "paused", linkedRunId: "run-a" });
    const doneReview = makeTask({ id: "t-dr", runId: "run-a", tier: 3, kind: "verify", title: "Done-Review: goal-1" });
    const worker = makeTask({ id: "t-w", runId: "run-a", status: "blocked", generation: 2 });
    const esc = makeEsc({ id: "esc-b", runId: "run-a", fromTaskId: "t-w", blocking: true, status: "open" });

    const baseFacts = buildFacts({
      runs: [runStopped, runQuota, runActive, runContainer],
      tasks: [doneReview, worker],
      escalations: [esc],
      goals: [pausedGoal],
    });

    expect(evaluateVeto({ runId: "run-a" }, { ...baseFacts, config: { ...baseFacts.config, mode: "off" } }).code).toBe("mode_not_on");
    expect(evaluateVeto({ runId: "run-a" }, { ...baseFacts, authoritative: false }).code).toBe("authority_lost");
    expect(evaluateVeto({ runId: "director-tasks-chat-1" }, baseFacts).code).toBe("director_container");
    expect(evaluateVeto({ runId: "run-stop" }, baseFacts).code).toBe("run_stopped");
    expect(evaluateVeto({ runId: "run-q" }, baseFacts).code).toBe("run_paused_quota");
    expect(evaluateVeto({ runId: "run-a" }, baseFacts).code).toBe("goal_paused");

    const cleanFacts = buildFacts({ runs: [runActive], tasks: [doneReview, worker], escalations: [esc] });
    expect(evaluateVeto({ runId: "run-a", taskId: "t-dr" }, cleanFacts).code).toBe("done_review_task");
    expect(evaluateVeto({ runId: "run-a", taskId: "t-w", expectedGeneration: 1 }, cleanFacts).code).toBe("target_changed");
    expect(evaluateVeto({ runId: "run-a", taskId: "t-w", expectedStatus: "running" }, cleanFacts).code).toBe("target_changed");
    expect(evaluateVeto({ runId: "run-a", taskId: "t-w", expectedGeneration: 2, expectedStatus: "blocked" }, cleanFacts).code).toBe(
      "blocking_escalation",
    );
    expect(
      evaluateVeto(
        {
          runId: "run-a",
          taskId: "t-w",
          expectedGeneration: 2,
          expectedStatus: "blocked",
          ignoreBlockingEscalationIds: new Set(["esc-b"]),
        },
        cleanFacts,
      ).vetoed,
    ).toBe(false);
  });
});

describe("routeItem rules R00-R22 and negative controls", () => {
  test("R00: supersedes only when ALL linked goals are achieved/cancelled; mixed goals do NOT supersede", () => {
    const runAllDone = makeRun({ id: "run-done", status: "completed" });
    const g1 = makeGoal({ id: "g-1", status: "achieved", linkedRunId: "run-done" });
    const g2 = makeGoal({ id: "g-2", status: "cancelled", linkedRunId: "run-done" });
    const esc = makeEsc({ id: "esc-r00", runId: "run-done", fromTaskId: "t-1", severity: "P2" });
    const msg = makeMsg({ id: "msg-r00", runId: "run-done", fromTaskId: "t-1", toTaskId: "t-2" });
    const merge = makeMerge({ id: "mq-r00", runId: "run-done", taskId: "t-1" });

    const factsAllDone = buildFacts({ runs: [runAllDone], goals: [g1, g2], escalations: [esc], messages: [msg], merges: [merge] });
    const itemsAllDone = itemsFromFacts(factsAllDone);

    const dEsc = routeItem(itemsAllDone.find((i) => i.ref.id === "esc-r00")!, factsAllDone);
    expect(dEsc.rule).toBe("R00");
    expect(dEsc.action?.primitive).toBe("reconcile_settled_goal_escalations");

    const dMsg = routeItem(itemsAllDone.find((i) => i.ref.id === "msg-r00")!, factsAllDone);
    expect(dMsg.rule).toBe("R00");
    expect(dMsg.action?.primitive).toBe("expire_message");

    const dMerge = routeItem(itemsAllDone.find((i) => i.ref.id === "mq-r00")!, factsAllDone);
    expect(dMerge.rule).toBe("R00");
    expect(dMerge.action?.primitive).toBe("ledger_supersede");

    // Negative control: mixed goals (one achieved, one still executing) must NOT trigger R00
    const runMixed = makeRun({ id: "run-mixed", status: "active" });
    const gm1 = makeGoal({ id: "gm-1", status: "achieved", linkedRunId: "run-mixed" });
    const gm2 = makeGoal({ id: "gm-2", status: "executing", linkedRunId: "run-mixed" });
    const escMixed = makeEsc({ id: "esc-mixed", runId: "run-mixed", fromTaskId: "t-1", severity: "P0" });
    const factsMixed = buildFacts({ runs: [runMixed], goals: [gm1, gm2], escalations: [escMixed] });
    const dMixed = routeItem(itemsFromFacts(factsMixed)[0]!, factsMixed);
    expect(dMixed.rule).toBe("R02");
    expect(dMixed.disposition).toBe("human_blocker");
  });

  test("R01 + R02: stopped run vetoes routine actions, while independent P0 asks stay surfaced; two asks in one run stay two; nonblocking P0 surfaced; acked routed", () => {
    const runStopped = makeRun({ id: "run-stop", status: "stopped" });
    const t1 = makeTask({ id: "t-1", runId: "run-stop", tier: 1, kind: "orchestrator", status: "cancelled" });
    const escRoutine = makeEsc({ id: "esc-p2", runId: "run-stop", fromTaskId: "t-1", severity: "P2", status: "open" });
    // Census genuine-human-decision: E1 interactive login + E2 nonblocking publication choice in same run
    const escE1 = makeEsc({
      id: "esc-E1",
      runId: "run-stop",
      fromTaskId: "t-1",
      severity: "P0",
      category: "blocked_dependency",
      title: "Run 'amp login' on node",
      blocking: true,
      status: "open",
    });
    const escE2 = makeEsc({
      id: "esc-E2",
      runId: "run-stop",
      fromTaskId: "t-1",
      severity: "P0",
      category: "ambiguous_spec",
      title: "Reply merge, rebase, or hold for publication",
      blocking: false,
      status: "acked", // acked P0 must STILL be routed!
    });

    const facts = buildFacts({ runs: [runStopped], tasks: [t1], escalations: [escRoutine, escE1, escE2] });
    const items = itemsFromFacts(facts, true);

    const dP2 = routeItem(items.find((i) => i.ref.id === "esc-p2")!, facts);
    expect(dP2.rule).toBe("R01");
    expect(dP2.disposition).toBe("held");
    expect(dP2.action).toBeNull();

    const dE1 = routeItem(items.find((i) => i.ref.id === "esc-E1")!, facts);
    const dE2 = routeItem(items.find((i) => i.ref.id === "esc-E2")!, facts);
    expect(dE1.rule).toBe("R02");
    expect(dE1.disposition).toBe("human_blocker");
    expect(dE1.ask).toContain("Run 'amp login' on node");
    expect(dE1.ask).not.toContain("Resolve with a reason");
    expect(dE2.rule).toBe("R02");
    expect(dE2.disposition).toBe("human_blocker");
    expect(dE2.ask).toContain("nonblocking");
    // Two asks in one run stay two separate causes
    expect(dE1.causeId).toBe("escalation:esc-E1");
    expect(dE2.causeId).toBe("escalation:esc-E2");
    expect(dE1.causeId).not.toBe(dE2.causeId);
  });

  test("R02: verification_failed P0 folds under goal_awaiting_human ONLY with proven same version/cycle", () => {
    const run = makeRun({ id: "run-v", status: "completed" });
    const g = makeGoal({ id: "goal-v", status: "awaiting_human", linkedRunId: "run-v", version: 2, reviewCycle: 3 });
    const rev = {
      id: "rev-3",
      goalId: "goal-v",
      goalVersion: 2,
      cycle: 3,
      verdict: "escalate_human",
      summary: "Circuit breaker tripped",
      createdAt: RECENT - 1000,
    };
    const escVerif = makeEsc({
      id: "esc-vf",
      runId: "run-v",
      fromTaskId: "t-1",
      toTaskId: null,
      severity: "P0",
      category: "verification_failed",
      title: "Goal goal-v needs human review",
      createdAt: RECENT,
    });

    const provenFacts = buildFacts({ runs: [run], goals: [g], reviews: [rev], escalations: [escVerif] });
    const dFold = routeItem(itemsFromFacts(provenFacts).find((i) => i.ref.id === "esc-vf")!, provenFacts);
    expect(dFold.rule).toBe("R02");
    expect(dFold.role).toBe("derived");
    expect(dFold.causeId).toBe("goal_awaiting_human:goal-v");

    // Without matching review for current version/cycle -> stays primary human_blocker
    const unprovenFacts = buildFacts({ runs: [run], goals: [g], reviews: [], escalations: [escVerif] });
    const dPrimary = routeItem(itemsFromFacts(unprovenFacts).find((i) => i.ref.id === "esc-vf")!, unprovenFacts);
    expect(dPrimary.rule).toBe("R02");
    expect(dPrimary.role).toBe("primary");
    expect(dPrimary.disposition).toBe("human_blocker");
  });

  test("R03, R04, R05: P1/P2 escalation ownership across live addressee, active run ladder A, and settled run ladder B (E3)", () => {
    const runActive = makeRun({ id: "run-a", status: "active" });
    const t1 = makeTask({ id: "t-1", runId: "run-a", tier: 1, kind: "orchestrator", status: "waiting_children", generation: 2 });
    const t2 = makeTask({ id: "t-2", runId: "run-a", parentId: "t-1", tier: 2, kind: "worker", status: "completed" });
    const escR03 = makeEsc({ id: "esc-r03", runId: "run-a", fromTaskId: "t-2", toTaskId: "t-1", severity: "P1" });
    const escR04 = makeEsc({ id: "esc-r04", runId: "run-a", fromTaskId: "t-2", toTaskId: null, severity: "P2" });

    // Census E3: nonblocking policy escalation on settled run without addressee -> R05 ladder B (not human_blocker!)
    const runSettled = makeRun({ id: "run-s", status: "completed" });
    const escE3 = makeEsc({
      id: "esc-E3",
      runId: "run-s",
      fromTaskId: "t-old",
      toTaskId: null,
      severity: "P1",
      category: "ambiguous_spec",
      blocking: false,
      status: "acked",
      title: "Deferred access-default questions Q1-Q8",
    });

    const facts = buildFacts({
      runs: [runActive, runSettled],
      tasks: [t1, t2],
      liveTaskIds: ["t-1"],
      escalations: [escR03, escR04, escE3],
    });
    const items = itemsFromFacts(facts, true);

    const d3 = routeItem(items.find((i) => i.ref.id === "esc-r03")!, facts);
    expect(d3.rule).toBe("R03");
    expect(d3.disposition).toBe("routine_owned");
    expect(d3.ownerId).toBe("t-1");
    expect(d3.ownerEpoch).toBe("g2");
    expect(d3.action).toBeNull(); // no steer sent to live addressee

    const d4 = routeItem(items.find((i) => i.ref.id === "esc-r04")!, facts);
    expect(d4.rule).toBe("R04");
    expect(d4.disposition).toBe("routine_owned");
    expect(d4.action?.primitive).toBe("steer_task");
    if (d4.action?.primitive === "steer_task") expect(d4.action.targetId).toBe("t-1");

    const d5 = routeItem(items.find((i) => i.ref.id === "esc-E3")!, facts);
    expect(d5.rule).toBe("R05");
    expect(d5.disposition).toBe("routine_owned");
    expect(d5.action?.primitive).toBe("batch_investigate");
  });

  test("R06, R07, R08: proven blocking escalation, wrong-generation/nonblocking negative controls, continue vs hold, rescope", () => {
    const run = makeRun({ id: "run-b", status: "active" });
    const t1 = makeTask({ id: "t-1", runId: "run-b", tier: 1, kind: "orchestrator", status: "running" });

    // R06 positive: exact Blocked on marker, blocking=true, matching generation
    const tR06 = makeTask({ id: "t-r06", runId: "run-b", parentId: "t-1", status: "blocked", generation: 1, summary: "Blocked on esc-open: need fix" });
    const escOpen = makeEsc({ id: "esc-open", runId: "run-b", fromTaskId: "t-r06", blocking: true, status: "open" });

    // Negative control 1: nonblocking escalation is NOT a blocked-task cause -> R08
    const tNonBlock = makeTask({ id: "t-nb", runId: "run-b", parentId: "t-1", status: "blocked", generation: 1, summary: "Blocked on esc-nb: advisory" });
    const escNb = makeEsc({ id: "esc-nb", runId: "run-b", fromTaskId: "t-nb", blocking: false, status: "open" });

    // Negative control 2: wrong-generation escalation is NOT a blocked-task cause -> R08
    const tWrongGen = makeTask({ id: "t-wg", runId: "run-b", parentId: "t-1", status: "blocked", generation: 2, summary: "Blocked on esc-g1: old block" });
    const escG1 = makeEsc({ id: "esc-g1", runId: "run-b", fromTaskId: "t-wg", blocking: true, status: "open" });

    // R07 without continue permission -> held
    const tR07Hold = makeTask({ id: "t-r07h", runId: "run-b", parentId: "t-1", status: "blocked", generation: 1, summary: "Blocked on esc-res-no-cont: wait" });
    const escResNoCont = makeEsc({ id: "esc-res-no-cont", runId: "run-b", fromTaskId: "t-r07h", blocking: true, status: "resolved", resolution: "Answered" });

    // R07 with continue permission -> routine_auto resume_task
    const tR07Go = makeTask({ id: "t-r07g", runId: "run-b", parentId: "t-1", status: "blocked", generation: 3, summary: "Blocked on esc-res-go: wait" });
    const escResGo = makeEsc({ id: "esc-res-go", runId: "run-b", fromTaskId: "t-r07g", blocking: true, status: "resolved", resolution: "Approved to proceed" });

    // R07 with continue permission BUT resolution says hold -> held
    const tR07HoldText = makeTask({ id: "t-r07ht", runId: "run-b", parentId: "t-1", status: "blocked", generation: 1, summary: "Blocked on esc-res-hold: wait" });
    const escResHoldText = makeEsc({ id: "esc-res-hold", runId: "run-b", fromTaskId: "t-r07ht", blocking: true, status: "resolved", resolution: "Please hold for now" });

    // R08 returned_for_rescope even with resolved escalationId -> ladder A/B (never R07 replay)
    const tRescope = makeTask({
      id: "t-rescope",
      runId: "run-b",
      parentId: "t-1",
      status: "returned_for_rescope",
      generation: 1,
      resultJson: JSON.stringify({ escalationId: "esc-rescope" }),
    });
    const escRescope = makeEsc({ id: "esc-rescope", runId: "run-b", fromTaskId: "t-rescope", blocking: true, status: "resolved", resolution: "Split it" });

    const facts = buildFacts({
      runs: [run],
      tasks: [t1, tR06, tNonBlock, tWrongGen, tR07Hold, tR07Go, tR07HoldText, tRescope],
      liveTaskIds: ["t-1"],
      escalations: [escOpen, escNb, escG1, escResNoCont, escResGo, escResHoldText, escRescope],
      escalationCreatedSeq: { "esc-g1": 10 },
      taskCurrentGenStartedSeq: { "t-wg": 25 },
      continuations: [
        { escalationId: "esc-res-go", recordedAt: RECENT, consumedAt: null, consumedTaskId: null, consumedGeneration: null },
        { escalationId: "esc-res-hold", recordedAt: RECENT, consumedAt: null, consumedTaskId: null, consumedGeneration: null },
        { escalationId: "esc-rescope", recordedAt: RECENT, consumedAt: null, consumedTaskId: null, consumedGeneration: null },
      ],
    });
    const items = itemsFromFacts(facts);

    const d6 = routeItem(items.find((i) => i.ref.id === "t-r06")!, facts);
    expect(d6.rule).toBe("R06");
    expect(d6.disposition).toBe("derived");
    expect(d6.causeId).toBe("escalation:esc-open");

    const dNb = routeItem(items.find((i) => i.ref.id === "t-nb")!, facts);
    expect(dNb.rule).toBe("R08");
    expect(dNb.disposition).toBe("routine_owned");

    const dWg = routeItem(items.find((i) => i.ref.id === "t-wg")!, facts);
    expect(dWg.rule).toBe("R08");
    expect(dWg.disposition).toBe("routine_owned");

    const d7h = routeItem(items.find((i) => i.ref.id === "t-r07h")!, facts);
    expect(d7h.rule).toBe("R07");
    expect(d7h.disposition).toBe("held");
    expect(d7h.holdReason).toBe("answered; awaiting explicit resume");

    const d7g = routeItem(items.find((i) => i.ref.id === "t-r07g")!, facts);
    expect(d7g.rule).toBe("R07");
    expect(d7g.disposition).toBe("routine_auto");
    expect(d7g.action?.primitive).toBe("resume_task");
    if (d7g.action?.primitive === "resume_task") {
      expect(d7g.action.targetId).toBe("t-r07g");
      expect(d7g.action.expectedGeneration).toBe(3);
    }

    const d7ht = routeItem(items.find((i) => i.ref.id === "t-r07ht")!, facts);
    expect(d7ht.rule).toBe("R07");
    expect(d7ht.disposition).toBe("held");

    // Directly test returned_for_rescope item routed as task_blocked
    const rescopeItem: InboxItem = {
      id: "inbox:task:t-rescope",
      severity: "P1",
      kind: "task_blocked",
      title: "t-rescope (returned_for_rescope)",
      detail: "too big",
      ref: { kind: "task", id: "t-rescope", runId: "run-b" },
      runId: "run-b",
      taskId: "t-rescope",
      createdAt: RECENT,
    };
    const dRescope = routeItem(rescopeItem, facts);
    expect(dRescope.rule).toBe("R08");
    expect(dRescope.action?.primitive).toBe("steer_task");
  });

  test("R09: task_failed in active run assigns live parent passively, else ladder A/B", () => {
    const run = makeRun({ id: "run-tf", status: "active" });
    const t1 = makeTask({ id: "t-1", runId: "run-tf", tier: 1, kind: "orchestrator", status: "waiting_children", generation: 2 });
    const t2Dead = makeTask({ id: "t-2", runId: "run-tf", parentId: "t-1", tier: 2, kind: "worker", status: "completed" });
    const tChildA = makeTask({ id: "t-fa", runId: "run-tf", parentId: "t-1", tier: 2, kind: "worker", status: "failed" });
    const tChildB = makeTask({ id: "t-fb", runId: "run-tf", parentId: "t-2", tier: 3, kind: "verify", status: "failed" });

    const facts = buildFacts({ runs: [run], tasks: [t1, t2Dead, tChildA, tChildB], liveTaskIds: ["t-1"] });
    const items = itemsFromFacts(facts);

    const dA = routeItem(items.find((i) => i.ref.id === "t-fa")!, facts);
    expect(dA.rule).toBe("R09");
    expect(dA.ownerId).toBe("t-1");
    expect(dA.action).toBeNull();

    const dB = routeItem(items.find((i) => i.ref.id === "t-fb")!, facts);
    expect(dB.rule).toBe("R09");
    expect(dB.action?.primitive).toBe("steer_task");
    if (dB.action?.primitive === "steer_task") expect(dB.action.targetId).toBe("t-1");
  });

  test("R10: restart casualty handoff vs resume policy, standalone vs goal-linked vs detached legacy", () => {
    const runGoal = makeRun({ id: "run-rg", status: "failed" });
    const g = makeGoal({
      id: "goal-rg",
      status: "awaiting_human",
      linkedRunId: "run-rg",
      lastReviewSummary: "Run run-rg was interrupted by a daemon restart; resume continues it in place",
    });
    const tG = makeTask({ id: "t-rg", runId: "run-rg", tier: 1, kind: "orchestrator", status: "failed", summary: "Interrupted by daemon restart" });

    const runStandalone = makeRun({ id: "run-rs", status: "failed" });
    const tS = makeTask({ id: "t-rs", runId: "run-rs", tier: 1, kind: "orchestrator", status: "failed", summary: "Interrupted by daemon restart" });

    const runDetached = makeRun({ id: "run-rd", status: "failed" });
    const tD = makeTask({ id: "t-rd", runId: "run-rd", tier: 1, kind: "orchestrator", status: "failed", summary: "Interrupted by daemon restart" });

    // Policy = handoff (default)
    const handoffFacts = buildFacts({
      config: { restartRecovery: "handoff" },
      runs: [runGoal, runStandalone, runDetached],
      goals: [g],
      tasks: [tG, tS, tD],
      runsEverLinkedToGoal: ["run-rd"],
      runStatusEvents: [
        { runId: "run-rg", seq: 10, status: "failed", cause: "daemon_restart", reason: "restart", createdAt: RECENT },
        { runId: "run-rs", seq: 11, status: "failed", cause: "daemon_restart", reason: "restart", createdAt: RECENT },
        { runId: "run-rd", seq: 12, status: "failed", cause: "daemon_restart", reason: "restart", createdAt: RECENT },
      ],
    });
    const handoffItems = itemsFromFacts(handoffFacts);

    const dHGoal = routeItem(handoffItems.find((i) => i.ref.id === "run-rg")!, handoffFacts);
    expect(dHGoal.rule).toBe("R10");
    expect(dHGoal.disposition).toBe("human_blocker");
    expect(dHGoal.ask).toBe("run-rg of goal-rg was interrupted by a daemon restart: resume in place | cancel");

    // Linked goal_awaiting_human folds into run-rg under R13
    const dGoalFold = routeItem(handoffItems.find((i) => i.ref.id === "goal-rg")!, handoffFacts);
    expect(dGoalFold.rule).toBe("R13");
    expect(dGoalFold.causeId).toBe("run_failed:run-rg");

    // Detached legacy run -> ladder B
    const dDetached = routeItem(handoffItems.find((i) => i.ref.id === "run-rd")!, handoffFacts);
    expect(dDetached.rule).toBe("R10");
    expect(dDetached.disposition).toBe("routine_owned");
    expect(dDetached.action?.primitive).toBe("batch_investigate");

    // Policy = resume
    const resumeFacts = buildFacts({
      config: { restartRecovery: "resume" },
      runs: [runGoal, runStandalone, runDetached],
      goals: [g],
      tasks: [tG, tS, tD],
      runsEverLinkedToGoal: ["run-rd"],
      runStatusEvents: [
        { runId: "run-rg", seq: 10, status: "failed", cause: "daemon_restart", reason: "restart", createdAt: RECENT },
        { runId: "run-rs", seq: 11, status: "failed", cause: "daemon_restart", reason: "restart", createdAt: RECENT },
        { runId: "run-rd", seq: 12, status: "failed", cause: "daemon_restart", reason: "restart", createdAt: RECENT },
      ],
    });
    const resumeItems = itemsFromFacts(resumeFacts);

    const dRGoal = routeItem(resumeItems.find((i) => i.ref.id === "run-rg")!, resumeFacts);
    expect(dRGoal.rule).toBe("R10");
    expect(dRGoal.disposition).toBe("routine_auto");
    expect(dRGoal.action?.primitive).toBe("resume_goal");

    const dRStand = routeItem(resumeItems.find((i) => i.ref.id === "run-rs")!, resumeFacts);
    expect(dRStand.rule).toBe("R10");
    expect(dRStand.disposition).toBe("routine_auto");
    expect(dRStand.action?.primitive).toBe("resume_run");

    const dRDetached = routeItem(resumeItems.find((i) => i.ref.id === "run-rd")!, resumeFacts);
    expect(dRDetached.rule).toBe("R10");
    expect(dRDetached.action?.primitive).toBe("batch_investigate");
  });

  test("R11, R12, R13, R14x, R15: blocked run derivation, failed run diagnosis, goal without verdict -> diagnosis not verification", () => {
    // R11: run failed with two blocked tasks on two independent blocking escalations
    const runBlocked = makeRun({ id: "run-11", status: "failed" });
    const t11a = makeTask({ id: "t-11a", runId: "run-11", status: "blocked", summary: "Blocked on esc-11a: A" });
    const t11b = makeTask({ id: "t-11b", runId: "run-11", status: "blocked", summary: "Blocked on esc-11b: B" });
    const esc11a = makeEsc({ id: "esc-11a", runId: "run-11", fromTaskId: "t-11a", severity: "P0", createdAt: OLD });
    const esc11b = makeEsc({ id: "esc-11b", runId: "run-11", fromTaskId: "t-11b", severity: "P0", createdAt: OLD + 100 });

    // R12 + R13: census R1 + G1
    const runR1 = makeRun({ id: "R1", status: "failed" });
    const gG1 = makeGoal({
      id: "G1",
      status: "awaiting_human",
      linkedRunId: "R1",
      lastReviewSummary: "Linked run R1 failed before goal verification",
    });

    // R14x: census G2 awaiting_human + completed R3 + no verdict ("Reviewer stopped: quota_paused")
    const runR3 = makeRun({ id: "R3", status: "completed" });
    const gG2 = makeGoal({
      id: "G2",
      status: "awaiting_human",
      linkedRunId: "R3",
      version: 4,
      lastReviewSummary: "Verification failed: Independent reviewer failed (Reviewer stopped: quota_paused)",
    });

    // R15: goal with verdict for current version -> human_blocker
    const runR4 = makeRun({ id: "R4", status: "completed" });
    const gG4 = makeGoal({
      id: "G4",
      status: "awaiting_human",
      linkedRunId: "R4",
      version: 1,
      reviewCycle: 1,
      lastReviewSummary: "Deterministic gate failed on test suite",
    });
    const revG4 = {
      id: "rev-1",
      goalId: "G4",
      goalVersion: 1,
      cycle: 1,
      verdict: "escalate_human",
      summary: "Deterministic gate failed on test suite",
      createdAt: RECENT,
    };

    const facts = buildFacts({
      runs: [runBlocked, runR1, runR3, runR4],
      tasks: [t11a, t11b],
      escalations: [esc11a, esc11b],
      goals: [gG1, gG2, gG4],
      reviews: [revG4],
    });
    const items = itemsFromFacts(facts);

    const d11 = routeItem(items.find((i) => i.ref.id === "run-11")!, facts);
    expect(d11.rule).toBe("R11");
    expect(d11.disposition).toBe("derived");
    expect(d11.causeId).toBe("escalation:esc-11a");
    expect(d11.additionalCauseIds).toEqual(["escalation:esc-11b"]);

    const d12 = routeItem(items.find((i) => i.ref.id === "R1")!, facts);
    expect(d12.rule).toBe("R12");
    expect(d12.action?.primitive).toBe("batch_investigate");

    const d13 = routeItem(items.find((i) => i.ref.id === "G1")!, facts);
    expect(d13.rule).toBe("R13");
    expect(d13.disposition).toBe("derived");
    expect(d13.causeId).toBe("run_failed:R1");

    // Goal without verdict goes to R14x diagnosis (batch), NEVER verification/acceptance
    const d14x = routeItem(items.find((i) => i.ref.id === "G2")!, facts);
    expect(d14x.rule).toBe("R14x");
    expect(d14x.disposition).toBe("routine_owned");
    expect(d14x.action?.primitive).toBe("batch_investigate");

    const d15 = routeItem(items.find((i) => i.ref.id === "G4")!, facts);
    expect(d15.rule).toBe("R15");
    expect(d15.disposition).toBe("human_blocker");
    expect(d15.ask).toContain("Deterministic gate failed");
    expect(d15.ask).not.toContain("accept with evidence");
  });

  test("R16 and R17: merge_bounced superseded proof vs negative controls (different branch, different worktree, equal timestamp)", () => {
    const run = makeRun({ id: "run-m", status: "active" });
    const tLive = makeTask({ id: "t-live", runId: "run-m", status: "running", generation: 1 });
    const tDead = makeTask({ id: "t-dead", runId: "run-m", status: "completed", generation: 1 });

    // R16 positive: same runId, taskId, worktreeId, branch, strictly later
    const q1 = makeMerge({ id: "Q1", runId: "run-m", taskId: "t-dead", worktreeId: "wt-1", branch: "feat-1", status: "ci_failed", createdAt: OLD, finishedAt: OLD + 100 });
    const q2 = makeMerge({ id: "Q2", runId: "run-m", taskId: "t-dead", worktreeId: "wt-1", branch: "feat-1", status: "merged", createdAt: RECENT, finishedAt: RECENT + 100 });

    // Negative control 1: different branch -> R17
    const qDiffBranch = makeMerge({ id: "Q3", runId: "run-m", taskId: "t-dead", worktreeId: "wt-2", branch: "feat-2", status: "ci_failed", createdAt: OLD, finishedAt: OLD + 100 });
    const qOtherBranchMerged = makeMerge({ id: "Q4", runId: "run-m", taskId: "t-dead", worktreeId: "wt-2", branch: "feat-other", status: "merged", createdAt: RECENT, finishedAt: RECENT + 100 });

    // Negative control 2: recreated worktree (different worktreeId) -> R17
    const qDiffWt = makeMerge({ id: "Q5", runId: "run-m", taskId: "t-live", worktreeId: "wt-old", branch: "feat-3", status: "conflict", createdAt: OLD, finishedAt: OLD + 100 });
    const qNewWtMerged = makeMerge({ id: "Q6", runId: "run-m", taskId: "t-live", worktreeId: "wt-new", branch: "feat-3", status: "merged", createdAt: RECENT, finishedAt: RECENT + 100 });

    const facts = buildFacts({
      runs: [run],
      tasks: [tLive, tDead],
      liveTaskIds: ["t-live"],
      merges: [q1, q2, qDiffBranch, qOtherBranchMerged, qDiffWt, qNewWtMerged],
    });
    const items = itemsFromFacts(facts);

    const d16 = routeItem(items.find((i) => i.ref.id === "Q1")!, facts);
    expect(d16.rule).toBe("R16");
    expect(d16.disposition).toBe("routine_auto");
    expect(d16.action?.primitive).toBe("ledger_proof");
    if (d16.action?.primitive === "ledger_proof") expect(d16.action.proof).toBe("submission superseded by Q2");

    const d17Branch = routeItem(items.find((i) => i.ref.id === "Q3")!, facts);
    expect(d17Branch.rule).toBe("R17");
    expect(d17Branch.disposition).toBe("routine_owned");

    const d17Wt = routeItem(items.find((i) => i.ref.id === "Q5")!, facts);
    expect(d17Wt.rule).toBe("R17");
    expect(d17Wt.ownerId).toBe("t-live");
    expect(d17Wt.action).toBeNull();
  });

  test("R18 and R19: open_message expired ONLY when durably terminal or run settled; blocked/missing-RAM recipient stays R19", () => {
    const run = makeRun({ id: "run-msg", status: "active" });
    const w1 = makeTask({ id: "W1", runId: "run-msg", status: "running", generation: 1 });
    const w2Done = makeTask({ id: "W2", runId: "run-msg", status: "completed", generation: 1 });
    const w3Live = makeTask({ id: "W3", runId: "run-msg", status: "running", generation: 2 });
    const w4Blocked = makeTask({ id: "W4", runId: "run-msg", status: "blocked", generation: 1 });
    const w5MissingRam = makeTask({ id: "W5", runId: "run-msg", status: "running", generation: 1 });

    // Census M1: W1 -> W2 (completed)
    const m1 = makeMsg({ id: "M1", runId: "run-msg", fromTaskId: "W1", toTaskId: "W2" });
    const m2 = makeMsg({ id: "M2", runId: "run-msg", fromTaskId: "W1", toTaskId: "W3" });
    const m3 = makeMsg({ id: "M3", runId: "run-msg", fromTaskId: "W1", toTaskId: "W4" });
    const m4 = makeMsg({ id: "M4", runId: "run-msg", fromTaskId: "W1", toTaskId: "W5" });

    const facts = buildFacts({
      runs: [run],
      tasks: [w1, w2Done, w3Live, w4Blocked, w5MissingRam],
      liveTaskIds: ["W1", "W3"], // W5 is running in DB, missing in RAM -> NOT durably terminal!
      messages: [m1, m2, m3, m4],
    });
    const items = itemsFromFacts(facts);

    const d18 = routeItem(items.find((i) => i.ref.id === "M1")!, facts);
    expect(d18.rule).toBe("R18");
    expect(d18.disposition).toBe("routine_auto");
    expect(d18.action?.primitive).toBe("expire_message");
    if (d18.action?.primitive === "expire_message") expect(d18.action.notifyAskerTaskId).toBe("W1");

    const d19Live = routeItem(items.find((i) => i.ref.id === "M2")!, facts);
    expect(d19Live.rule).toBe("R19");
    expect(d19Live.ownerId).toBe("W3");
    expect(d19Live.action).toBeNull();

    // Negative controls: blocked or missing-RAM recipient must NOT be expired
    const d19Blocked = routeItem(items.find((i) => i.ref.id === "M3")!, facts);
    expect(d19Blocked.rule).toBe("R19");
    expect(d19Blocked.action?.primitive).not.toBe("expire_message");

    const d19MissingRam = routeItem(items.find((i) => i.ref.id === "M4")!, facts);
    expect(d19MissingRam.rule).toBe("R19");
    expect(d19MissingRam.action?.primitive).not.toBe("expire_message");
  });

  test("R20 and R21: quota_paused auto_timer, user_timer, manual, and null missing-since progression", () => {
    const rAuto = makeRun({ id: "run-qa", status: "paused_quota" });
    const rUser = makeRun({ id: "run-qu", status: "paused_quota" });
    const rMan = makeRun({ id: "run-qm", status: "paused_quota" });
    const rNull1 = makeRun({ id: "run-qn1", status: "paused_quota" });
    const rNull2 = makeRun({ id: "run-qn2", status: "paused_quota" });

    const facts = buildFacts({
      now: NOW,
      runs: [rAuto, rUser, rMan, rNull1, rNull2],
      quotaStatuses: {
        "run-qa": { runId: "run-qa", pausedTaskIds: ["t-1"], mode: "auto_timer", source: "quota_endpoint", resumeAt: "2026-11-23T18:00:00Z", reason: "5h bucket", pausedAt: RECENT },
        "run-qu": { runId: "run-qu", pausedTaskIds: ["t-2"], mode: "user_timer", source: "user_timer", resumeAt: "2026-11-23T22:00:00Z", reason: "user scheduled", pausedAt: RECENT },
        "run-qm": { runId: "run-qm", pausedTaskIds: ["t-3"], mode: "manual", source: "none", resumeAt: null, reason: "no auto-resume configured", pausedAt: RECENT },
        "run-qn1": null,
        "run-qn2": null,
      },
      missingQuotaSinceByCauseId: {
        [makeCauseId("quota_paused", "run-qn2")]: NOW - 125_000, // > recoverMaxSec (120s)
      },
    });
    const items = itemsFromFacts(facts);

    const dAuto = routeItem(items.find((i) => i.ref.id === "run-qa")!, facts);
    expect(dAuto.rule).toBe("R20");
    expect(dAuto.disposition).toBe("held");
    expect(dAuto.ownerId).toBe("quota-auto-resume");

    const dUser = routeItem(items.find((i) => i.ref.id === "run-qu")!, facts);
    expect(dUser.rule).toBe("R20");
    expect(dUser.disposition).toBe("held");
    expect(dUser.ownerId).toBe("quota-user-timer");

    const dMan = routeItem(items.find((i) => i.ref.id === "run-qm")!, facts);
    expect(dMan.rule).toBe("R21");
    expect(dMan.disposition).toBe("human_blocker");
    expect(dMan.ask).toContain("no auto-resume configured");

    // Null first observation -> held with recordMissingSince=true
    const dNull1 = routeItem(items.find((i) => i.ref.id === "run-qn1")!, facts);
    expect(dNull1.rule).toBe("R21");
    expect(dNull1.disposition).toBe("held");
    expect(dNull1.recordMissingSince).toBe(true);

    // Null after recoverMaxSec -> human_blocker
    const dNull2 = routeItem(items.find((i) => i.ref.id === "run-qn2")!, facts);
    expect(dNull2.rule).toBe("R21");
    expect(dNull2.disposition).toBe("human_blocker");
  });

  test("R22: unmatched/missing source falls back to ladder B diagnosis, never generic human fallback", () => {
    const facts = buildFacts({});
    const ghostItem: InboxItem = {
      id: "inbox:esc:esc-ghost",
      severity: "P2",
      kind: "escalation",
      title: "Ghost",
      detail: "Missing row",
      ref: { kind: "escalation", id: "esc-ghost", runId: "run-x" },
      runId: "run-x",
      createdAt: RECENT,
    };
    const d = routeItem(ghostItem, facts);
    expect(d.rule).toBe("R22");
    expect(d.disposition).toBe("routine_owned");
    expect(d.action?.primitive).toBe("batch_investigate");
  });
});
