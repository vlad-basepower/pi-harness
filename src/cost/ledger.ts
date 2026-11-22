import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { basename, join } from "node:path";
import { Database } from "bun:sqlite";
import type { RunnerProfileConfig } from "../config.js";
import type { ControlPlaneDb, CostInvocationRecord } from "../control-plane/db.js";
import type { CostAmount, CostModelAmount, CostRoute, CostSnapshot } from "../protocol/daemon-rpc.js";
import type { RunnerUsage } from "../runners/types.js";

export interface CostLaunch {
  chatSessionId?: string;
  runId?: string;
  taskId?: string;
  activity: CostInvocationRecord["activity"];
  profile: { id: string; billing: CostInvocationRecord["billing"] };
  model?: string | null;
  wireModel?: string | null;
}

/** Invocations whose runner has not returned yet; lives in memory so a crashed daemon leaves nothing "running". */
const liveInvocations = new Set<string>();

/**
 * Records one runner launch and returns the callbacks that feed its reported cost into the ledger.
 * Billing is snapshotted here, so later config changes never reclassify the launch; subscription
 * launches are recorded but never charged. Call `finish` when the runner returns.
 */
export function recordCostInvocation(
  db: ControlPlaneDb,
  launch: CostLaunch,
): { onSessionId: (sessionId: string) => void; onUsage: (usage: RunnerUsage) => void; finish: () => void } {
  const inv = db.insertCostInvocation({
    chatSessionId: launch.chatSessionId ?? null,
    runId: launch.runId ?? null,
    taskId: launch.taskId ?? null,
    activity: launch.activity,
    profileId: launch.profile.id,
    billing: launch.profile.billing,
    model: launch.model ?? null,
    wireModel: launch.wireModel ?? null,
  });
  liveInvocations.add(inv.id);
  let externalSessionId: string | null = null;
  let reported = false;
  return {
    onSessionId: (sessionId) => {
      externalSessionId = sessionId;
      db.setCostInvocationSession(inv.id, sessionId);
    },
    onUsage: ({ cost }) => {
      if (!cost || inv.billing === "subscription" || !Number.isFinite(cost.amount) || cost.amount < 0) return;
      const model = cost.model ?? null;
      if (cost.basis === "delta") {
        // A delta charge counts once per invocation however often it is delivered.
        db.insertCostCharge({
          invocationId: inv.id,
          dedupKey: `delta:${inv.id}:${cost.chargeId ?? ""}`,
          amount: cost.amount,
          currency: cost.currency,
          model,
          source: "pi_provider",
        });
        return;
      }
      // The baseline follows the external session, so a re-attached session never counts its total twice.
      // A total below the baseline means the adapter restarted its counter: nothing is subtracted and
      // the lower total becomes the new baseline.
      const key = externalSessionId ?? `invocation:${inv.id}`;
      db.db.transaction(() => {
        const baseline = db.getCostBaseline(inv.profileId, key, cost.currency);
        const increase = cost.amount - (baseline ?? 0);
        db.setCostBaseline(inv.profileId, key, cost.currency, cost.amount);
        // The first total of an invocation is recorded even when it adds nothing, so the invocation counts as having reported.
        if (increase > 0 || !reported) {
          reported = true;
          db.insertCostCharge({
            invocationId: inv.id,
            dedupKey: null,
            amount: Math.max(increase, 0),
            currency: cost.currency,
            model,
            source: "acp_usage_update",
          });
        }
      })();
    },
    finish: () => {
      liveInvocations.delete(inv.id);
    },
  };
}

function opencodeDbPath(): string {
  return join(process.env.XDG_DATA_HOME || join(homedir(), ".local", "share"), "opencode", "opencode.db");
}

interface OpencodeMessage {
  rootSessionId: string;
  model: string | null;
  amount: number;
}

/**
 * Per-message costs OpenCode wrote for these sessions and their sub-agent sessions, keyed by message id.
 * Read-only; `null` when the database is missing or unreadable.
 */
function readOpencodeMessages(sessionIds: string[]): Map<string, OpencodeMessage> | null {
  const path = opencodeDbPath();
  if (sessionIds.length === 0 || !existsSync(path)) return null;
  let db: Database | undefined;
  try {
    db = new Database(path, { readonly: true });
    const marks = sessionIds.map(() => "?").join(",");
    const rows = db
      .prepare(
        `WITH RECURSIVE tree(id, root) AS (
           SELECT id, id FROM session WHERE id IN (${marks})
           UNION ALL SELECT s.id, tree.root FROM session s JOIN tree ON s.parent_id = tree.id
         )
         SELECT m.id AS id, tree.root AS root, json_extract(m.data, '$.modelID') AS model, json_extract(m.data, '$.cost') AS cost
         FROM message m JOIN tree ON m.session_id = tree.id
         WHERE json_extract(m.data, '$.role') = 'assistant' AND typeof(json_extract(m.data, '$.cost')) IN ('integer', 'real')`,
      )
      .all(...sessionIds) as Array<{ id: string; root: string; model: string | null; cost: number }>;
    return new Map(rows.map((r) => [r.id, { rootSessionId: r.root, model: r.model, amount: r.cost }]));
  } catch {
    return null;
  } finally {
    db?.close();
  }
}

function isOpencode(profile: RunnerProfileConfig | undefined): boolean {
  return profile?.kind === "acp" && basename(profile.command) === "opencode";
}

/** Why a route reports no charges at all, when that is known in advance. */
function unsupportedReason(profile: RunnerProfileConfig | undefined, billing: CostRoute["billing"]): string | null {
  if (billing === "local") return "Local models have no provider charges.";
  if (profile?.kind === "pi") return profile.provider === "openrouter" ? null : "This provider does not report charges.";
  if (profile?.kind === "acp") {
    if (profile.adapter === "codex") return "Codex reports token counts only, not charges.";
    if ([profile.command, ...(profile.args ?? [])].some((a) => a.includes("amp-acp"))) return "Amp reports usage without charges.";
  }
  return null;
}

const round = (n: number) => Math.round(n * 1e9) / 1e9;

/** Known spend of the non-subscription invocations owned by the chat session or the runs. */
export function summarizeCost(
  db: ControlPlaneDb,
  scope: { chatSessionId: string | null; runIds: string[]; snapshotScope: CostSnapshot["scope"] },
  profiles: RunnerProfileConfig[],
): CostSnapshot {
  const invocations = db
    .listCostInvocations(scope.chatSessionId, scope.runIds)
    .filter((i): i is CostInvocationRecord & { billing: CostRoute["billing"] } => i.billing !== "subscription");
  const empty = { complete: false, routes: [], unattributed: [] };
  if (invocations.length === 0) return { scope: scope.snapshotScope, available: false, totals: [], byModel: [], coverage: empty };

  // OpenCode's own per-message records supersede the wire total of the sessions they cover.
  const opencodeProfiles = new Set(profiles.filter(isOpencode).map((p) => p.id));
  const externalIds = [
    ...new Set(invocations.flatMap((i) => (i.externalSessionId && opencodeProfiles.has(i.profileId) ? [i.externalSessionId] : []))),
  ];
  const messages = readOpencodeMessages(externalIds);
  const recorded = new Set([...(messages?.values() ?? [])].map((m) => m.rootSessionId));

  const lines = new Map<string, CostModelAmount>();
  const add = (model: string | null, currency: string, amount: number) => {
    const key = `${currency}\0${model ?? ""}\0${model === null}`;
    const line = lines.get(key) ?? { model, currency, amount: 0 };
    line.amount += amount;
    lines.set(key, line);
  };
  const withCost = new Set<string>();
  const unattributedInvocations = new Set<string>();
  const isRecorded = (inv: CostInvocationRecord) => inv.externalSessionId !== null && recorded.has(inv.externalSessionId);
  const byId = new Map(invocations.map((i) => [i.id, i]));
  for (const c of db.listCostChargeTotals(invocations.map((i) => i.id))) {
    const inv = byId.get(c.invocationId);
    if (!inv || isRecorded(inv)) continue;
    withCost.add(inv.id);
    if (c.model === null) unattributedInvocations.add(inv.id);
    add(c.model, c.currency, c.amount);
  }
  for (const m of messages?.values() ?? []) add(m.model, "USD", m.amount);
  for (const inv of invocations) {
    if (!isRecorded(inv)) continue;
    withCost.add(inv.id);
    if ([...(messages?.values() ?? [])].some((m) => m.rootSessionId === inv.externalSessionId && m.model === null)) {
      unattributedInvocations.add(inv.id);
    }
  }

  const byModel = [...lines.values()]
    .map((l) => ({ ...l, amount: round(l.amount) }))
    .sort((a, b) => a.currency.localeCompare(b.currency) || b.amount - a.amount);
  const sum = (rows: CostModelAmount[]): CostAmount[] => {
    const per = new Map<string, number>();
    for (const r of rows) per.set(r.currency, (per.get(r.currency) ?? 0) + r.amount);
    return [...per].map(([currency, amount]) => ({ currency, amount: round(amount) }));
  };

  const routes: CostRoute[] = [];
  for (const [key, group] of Map.groupBy(invocations, (i) => `${i.profileId}\0${i.billing}`)) {
    const [profileId, billing] = key.split("\0") as [string, CostRoute["billing"]];
    const profile = profiles.find((p) => p.id === profileId);
    const running = group.filter((i) => liveInvocations.has(i.id)).length;
    const reporting = group.filter((i) => withCost.has(i.id));
    const missing = group.length - reporting.length;
    const unattributed = group.filter((i) => unattributedInvocations.has(i.id)).length;
    const notes: string[] = [];
    let status: CostRoute["status"];
    if (reporting.length === 0) {
      const reason = unsupportedReason(profile, billing);
      status = reason ? "unsupported" : "unavailable";
      notes.push(reason ?? "No cost has been reported yet.");
    } else {
      status = missing === 0 && unattributed === 0 ? "reported" : "partial";
      if (missing > 0) notes.push(`${missing} of ${group.length} runs reported no cost.`);
      if (unattributed > 0) notes.push("Some charges do not say which model incurred them.");
    }
    if (opencodeProfiles.has(profileId)) {
      const fellBack = reporting.filter((i) => !isRecorded(i)).length;
      if (fellBack > 0) notes.push("OpenCode's session records were unavailable, so the agent's session total is used without per-model detail.");
    }
    if (profile?.kind === "acp" && profile.adapter === "claude" && reporting.length > 0) {
      notes.push("Sub-agent spend on another model is credited to the main model unless the agent lists the turn's models.");
    }
    if (running > 0) notes.push(`${running} still running; the total may grow.`);
    routes.push({ profileId, billing, invocations: group.length, running, status, note: notes.join(" ") });
  }

  return {
    scope: scope.snapshotScope,
    available: true,
    totals: sum(byModel),
    byModel,
    coverage: {
      complete: routes.every((r) => r.status === "reported" && r.running === 0),
      routes,
      unattributed: sum(byModel.filter((l) => l.model === null)),
    },
  };
}
