import { createHash, randomBytes } from "node:crypto";
import { mkdirSync } from "node:fs";
import { basename, dirname } from "node:path";
import { Database } from "bun:sqlite";
import { AdmissionQueue } from "./admission-queue.js";
import { advanceSequenceFloor, friendlyIdCandidates, nextFriendlyId, nextSequenceValue } from "./ids.js";
import { ProjectStore } from "./projects.js";
import { SessionMessageStore } from "./session-messages.js";
import { TimerManager } from "./timers.js";
import { KnowledgeBaseStore } from "../kb/store.js";

/** A requested write scope overlaps an active lease on the same (repo root, target branch). */
export class WriteScopeConflictError extends Error {
  constructor(
    message: string,
    readonly requested: string,
    readonly holders: Array<{ runId: string; taskId: string; glob: string }>,
  ) {
    super(message);
    this.name = "WriteScopeConflictError";
  }
}

export type AgentTier = 1 | 2 | 3;

export type TaskStatus =
  | "pending"
  | "ready"
  | "running"
  | "draining"
  | "waiting_children"
  | "blocked"
  | "paused_quota"
  | "returned_for_rescope"
  | "merging"
  | "completed"
  | "failed"
  | "cancelled";

export type TaskKind = "orchestrator" | "worker" | "research" | "plan" | "execute" | "verify";

export type EscalationSeverity = "P0" | "P1" | "P2";

export type EscalationCategory =
  | "scope_too_large"
  | "code_defect"
  | "broken_test"
  | "interface_conflict"
  | "ambiguous_spec"
  | "blocked_dependency"
  | "verification_failed"
  | "budget_exhausted"
  | "env_failure"
  | "security_risk";

export interface RunRecord {
  id: string;
  goal: string;
  repoRoot: string;
  baseBranch: string;
  baseSha: string;
  coordinationGroup: string | null;
  isolationMode: "standalone" | "coordinated";
  status: "active" | "paused_quota" | "completed" | "failed" | "stopped";
  createdAt: number;
  updatedAt: number;
}

export interface SharedContractRecord {
  namespace: string;
  key: string;
  version: number;
  ownerRunId: string;
  status: "draft" | "frozen" | "deprecated";
  schemaHash: string;
  payload: unknown;
  summary: string;
  createdAt: number;
}

export interface PeerExchangeRecord {
  id: string;
  fromRunId: string;
  toRunId: string;
  kind: "advisory" | "request";
  topicKey: string;
  round: number;
  message: string;
  reply: string | null;
  status: "delivered" | "open" | "answered" | "timed_out";
  createdAt: number;
  answeredAt: number | null;
}

export interface TaskRecord {
  id: string;
  runId: string;
  parentId: string | null;
  tier: AgentTier;
  kind: TaskKind;
  title: string;
  objective: string;
  criteria: string;
  readOnly: boolean;
  profileId: string;
  model: string | null;
  effort: string | null;
  status: TaskStatus;
  generation: number;
  maxTurns: number;
  turnsUsed: number;
  contextUsed: number;
  contextSize: number;
  worktreeId: string | null;
  sessionId: string | null;
  summary: string | null;
  handoffJson: string | null;
  resultJson: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface ArtifactRecord {
  id: string;
  runId: string;
  taskId: string;
  kind: "read" | "edit" | "write" | "delete" | "execute" | "dossier";
  path: string;
  summary: string;
  gitSha: string | null;
  exitCode: number | null;
  createdAt: number;
}

export interface EscalationRecord {
  id: string;
  runId: string;
  fromTaskId: string;
  toTaskId: string | null;
  severity: EscalationSeverity;
  category: EscalationCategory;
  title: string;
  evidencePaths: string[];
  attempted: string[];
  suggestedSplits: string[];
  blocking: boolean;
  status: "open" | "acked" | "resolved";
  resolution: string | null;
  createdAt: number;
  ackedAt: number | null;
}

export interface MessageRecord {
  id: string;
  runId: string;
  fromTaskId: string;
  toTaskId: string;
  question: string;
  answer: string | null;
  expiredReason?: string | null;
  status: "open" | "answered" | "expired";
  createdAt: number;
  answeredAt: number | null;
}

export interface WorktreeRecord {
  id: string;
  runId: string;
  taskId: string;
  path: string;
  branch: string;
  baseSha: string;
  status: "active" | "queued" | "merged" | "bounced" | "archived" | "removed";
  archiveRef: string | null;
  createdAt: number;
  cleanedAt: number | null;
  /** Serialized identity of the Git registration created for this row; null for rows recorded before it was kept. */
  creationIdentityJson?: string | null;
}

export interface MergeQueueRecord {
  id: string;
  runId: string;
  taskId: string;
  worktreeId: string;
  branch: string;
  status: "queued" | "running" | "merged" | "conflict" | "ci_failed";
  log: string | null;
  createdAt: number;
  finishedAt: number | null;
}

export interface EventRecord {
  seq: number;
  runId: string;
  taskId: string | null;
  type: string;
  payload: unknown;
  createdAt: number;
}

export interface ReplayCursor {
  sinceSeq: number;
  payloadOffset: number;
}

export interface ReplayPage {
  replay: EventRecord[];
  throughSeq: number;
  next?: ReplayCursor;
  fragment?: { event: EventRecord; data: string; final: boolean };
}

export interface CostInvocationRecord {
  id: string;
  chatSessionId: string | null;
  runId: string | null;
  taskId: string | null;
  activity: "director" | "done_review" | "task";
  profileId: string;
  /** Billing mode of the profile when the runner was launched. */
  billing: "subscription" | "api" | "local";
  model: string | null;
  wireModel: string | null;
  externalSessionId: string | null;
  startedAt: number;
}

export interface CostChargeTotal {
  invocationId: string;
  model: string | null;
  currency: string;
  amount: number;
}

const REPLAY_PAGE_BYTES = 1024 * 1024;

export function shortHashId(prefix: string): string {
  return `${prefix}-${randomBytes(4).toString("hex")}`;
}

/** Path-segment-aware glob-overlap detector for write-scope leasing. */
export function globsOverlap(a: string, b: string): boolean {
  const norm = (raw: string): string => {
    const cleaned = raw.trim().replaceAll("\\", "/").replace(/^\.\//, "").replace(/\/+$/, "");
    const parts: string[] = [];
    for (const seg of cleaned.split("/")) {
      if (!seg || seg === ".") continue;
      if (seg === ".." && parts.length > 0 && parts[parts.length - 1] !== "..") {
        parts.pop();
      } else {
        parts.push(seg);
      }
    }
    return parts.join("/");
  };
  const normA = norm(a);
  const normB = norm(b);
  if (!normA || !normB) return true;
  if (normA === normB) return true;
  if (normA === "*" || normA === "**" || normA === "**/*" || normB === "*" || normB === "**" || normB === "**/*") return true;

  const staticSegments = (pattern: string): { segs: string[]; isExactFile: boolean } => {
    const rawSegs = pattern.split("/");
    const segs: string[] = [];
    for (const s of rawSegs) {
      if (/[*?[{]/.test(s)) return { segs, isExactFile: false };
      segs.push(s);
    }
    return { segs, isExactFile: true };
  };

  const pA = staticSegments(normA);
  const pB = staticSegments(normB);
  if (pA.segs.length === 0 || pB.segs.length === 0) return true;

  const minLen = Math.min(pA.segs.length, pB.segs.length);
  for (let i = 0; i < minLen; i++) {
    if (pA.segs[i] !== pB.segs[i]) return false;
  }
  // If both are exact paths of different lengths, only overlap if the shorter is a directory prefix of the longer;
  // an exact file like `src/app.ts` cannot overlap `src/app.ts/foo` (impossible), and `src/app` vs `src/app/x.ts` DOES overlap.
  return true;
}

const SCHEMA = `
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  goal TEXT NOT NULL,
  repo_root TEXT NOT NULL,
  base_branch TEXT NOT NULL,
  base_sha TEXT NOT NULL,
  coordination_group TEXT,
  isolation_mode TEXT NOT NULL DEFAULT 'standalone',
  status TEXT NOT NULL DEFAULT 'active',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id),
  parent_id TEXT REFERENCES tasks(id),
  tier INTEGER NOT NULL,
  kind TEXT NOT NULL,
  title TEXT NOT NULL,
  objective TEXT NOT NULL,
  criteria TEXT NOT NULL,
  read_only INTEGER NOT NULL DEFAULT 0,
  profile_id TEXT NOT NULL,
  model TEXT,
  effort TEXT,
  status TEXT NOT NULL DEFAULT 'ready',
  generation INTEGER NOT NULL DEFAULT 1,
  max_turns INTEGER NOT NULL DEFAULT 20,
  turns_used INTEGER NOT NULL DEFAULT 0,
  context_used INTEGER NOT NULL DEFAULT 0,
  context_size INTEGER NOT NULL DEFAULT 0,
  worktree_id TEXT,
  session_id TEXT,
  summary TEXT,
  handoff_json TEXT,
  result_json TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_run ON tasks(run_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks(parent_id);

CREATE TABLE IF NOT EXISTS task_dependencies (
  task_id TEXT NOT NULL REFERENCES tasks(id),
  depends_on TEXT NOT NULL REFERENCES tasks(id),
  PRIMARY KEY (task_id, depends_on)
);

CREATE TABLE IF NOT EXISTS write_leases (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id),
  repo_root TEXT NOT NULL,
  target_branch TEXT NOT NULL,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  glob_pattern TEXT NOT NULL,
  active INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_leases_active ON write_leases(run_id, active);
CREATE INDEX IF NOT EXISTS idx_leases_repo_active ON write_leases(repo_root, active);

CREATE TABLE IF NOT EXISTS shared_contracts (
  namespace TEXT NOT NULL,
  key TEXT NOT NULL,
  version INTEGER NOT NULL,
  owner_run_id TEXT NOT NULL REFERENCES runs(id),
  status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','frozen','deprecated')),
  schema_hash TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  summary TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (namespace, key, version)
);
CREATE INDEX IF NOT EXISTS idx_contracts_ns ON shared_contracts(namespace, key, version);

CREATE TABLE IF NOT EXISTS peer_exchanges (
  id TEXT PRIMARY KEY,
  from_run_id TEXT NOT NULL REFERENCES runs(id),
  to_run_id TEXT NOT NULL REFERENCES runs(id),
  kind TEXT NOT NULL CHECK (kind IN ('advisory','request')),
  topic_key TEXT NOT NULL,
  round INTEGER NOT NULL DEFAULT 1,
  message TEXT NOT NULL,
  reply TEXT,
  status TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  answered_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_peer_to ON peer_exchanges(to_run_id, status);

CREATE TABLE IF NOT EXISTS run_wait_edges (
  waiter_run_id TEXT NOT NULL REFERENCES runs(id),
  target_run_id TEXT NOT NULL REFERENCES runs(id),
  reason TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (waiter_run_id, target_run_id, reason)
);

CREATE TABLE IF NOT EXISTS artifacts (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id),
  task_id TEXT NOT NULL REFERENCES tasks(id),
  kind TEXT NOT NULL,
  path TEXT NOT NULL,
  summary TEXT NOT NULL,
  git_sha TEXT,
  exit_code INTEGER,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_artifacts_task ON artifacts(task_id, created_at);

CREATE TABLE IF NOT EXISTS escalations (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id),
  from_task_id TEXT NOT NULL REFERENCES tasks(id),
  to_task_id TEXT REFERENCES tasks(id),
  severity TEXT NOT NULL,
  category TEXT NOT NULL,
  title TEXT NOT NULL,
  evidence_paths_json TEXT NOT NULL,
  attempted_json TEXT NOT NULL,
  suggested_splits_json TEXT NOT NULL,
  blocking INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'open',
  resolution TEXT,
  created_at INTEGER NOT NULL,
  acked_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_escalations_open ON escalations(run_id, status);

CREATE TABLE IF NOT EXISTS blackboard (
  run_id TEXT NOT NULL REFERENCES runs(id),
  key TEXT NOT NULL,
  value_json TEXT NOT NULL,
  author_task_id TEXT NOT NULL REFERENCES tasks(id),
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (run_id, key)
);

CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id),
  from_task_id TEXT NOT NULL REFERENCES tasks(id),
  to_task_id TEXT NOT NULL REFERENCES tasks(id),
  question TEXT NOT NULL,
  answer TEXT,
  expired_reason TEXT,
  status TEXT NOT NULL DEFAULT 'open',
  created_at INTEGER NOT NULL,
  answered_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_messages_to ON messages(to_task_id, status);

CREATE TABLE IF NOT EXISTS worktrees (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id),
  task_id TEXT NOT NULL REFERENCES tasks(id),
  path TEXT NOT NULL,
  branch TEXT NOT NULL,
  base_sha TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  archive_ref TEXT,
  created_at INTEGER NOT NULL,
  cleaned_at INTEGER,
  creation_identity_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_worktrees_run ON worktrees(run_id, status);

CREATE TABLE IF NOT EXISTS merge_queue (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id),
  task_id TEXT NOT NULL REFERENCES tasks(id),
  worktree_id TEXT NOT NULL REFERENCES worktrees(id),
  branch TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  log TEXT,
  created_at INTEGER NOT NULL,
  finished_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_merge_queue_status ON merge_queue(run_id, status);

CREATE TABLE IF NOT EXISTS events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(id),
  task_id TEXT,
  type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_run_seq ON events(run_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_task_seq ON events(task_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_run_task_type_seq ON events(run_id, task_id, type, seq);

CREATE TABLE IF NOT EXISTS cost_invocations (
  id TEXT PRIMARY KEY,
  chat_session_id TEXT,
  run_id TEXT,
  task_id TEXT,
  activity TEXT NOT NULL,
  profile_id TEXT NOT NULL,
  billing TEXT NOT NULL,
  model TEXT,
  wire_model TEXT,
  external_session_id TEXT,
  started_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cost_invocations_chat ON cost_invocations(chat_session_id);
CREATE INDEX IF NOT EXISTS idx_cost_invocations_run ON cost_invocations(run_id);

CREATE TABLE IF NOT EXISTS cost_charges (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  invocation_id TEXT NOT NULL REFERENCES cost_invocations(id),
  dedup_key TEXT UNIQUE,
  amount REAL NOT NULL,
  currency TEXT NOT NULL,
  model TEXT,
  source TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cost_charges_invocation ON cost_charges(invocation_id);

CREATE TABLE IF NOT EXISTS cost_baselines (
  profile_id TEXT NOT NULL,
  session_key TEXT NOT NULL,
  currency TEXT NOT NULL,
  total REAL NOT NULL,
  PRIMARY KEY (profile_id, session_key, currency)
);
`;

export class ControlPlaneDb {
  readonly db: Database;
  readonly kb: KnowledgeBaseStore;
  readonly timers: TimerManager;
  readonly projects: ProjectStore;
  readonly sessionMessages: SessionMessageStore;
  readonly admissionQueue: AdmissionQueue;
  private listeners = new Set<(event: EventRecord) => void>();

  constructor(dbPath: string) {
    if (dbPath !== ":memory:") {
      mkdirSync(dirname(dbPath), { recursive: true });
    }
    this.db = new Database(dbPath);
    this.db.exec("PRAGMA busy_timeout = 5000;");
    this.db.exec(SCHEMA);
    const worktreeColumns = new Set(
      (this.db.prepare("PRAGMA table_info(worktrees)").all() as Array<{ name: string }>).map((c) => c.name),
    );
    if (!worktreeColumns.has("creation_identity_json")) {
      this.db.exec("ALTER TABLE worktrees ADD COLUMN creation_identity_json TEXT;");
    }
    this.kb = new KnowledgeBaseStore(this.db);
    this.timers = new TimerManager(this.db);
    this.projects = new ProjectStore(this.db);
    this.sessionMessages = new SessionMessageStore(this.db);
    this.admissionQueue = new AdmissionQueue(this.db);
  }

  close(): void {
    this.timers.dispose();
    this.db.close();
  }

  onEvent(fn: (event: EventRecord) => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  transaction<T>(fn: () => T): T {
    return this.db.transaction(fn).immediate();
  }

  emitEvent(runId: string, taskId: string | null, type: string, payload: unknown): EventRecord {
    const now = Date.now();
    const res = this.db
      .prepare("INSERT INTO events(run_id, task_id, type, payload_json, created_at) VALUES (?, ?, ?, ?, ?)")
      .run(runId, taskId, type, JSON.stringify(payload ?? null), now);
    const event: EventRecord = {
      seq: Number(res.lastInsertRowid),
      runId,
      taskId,
      type,
      payload,
      createdAt: now,
    };
    for (const listener of this.listeners) {
      try {
        listener(event);
      } catch {
        // Never let subscriber errors break durable commits
      }
    }
    return event;
  }

  getEventsSince(runId: string, sinceSeq: number, taskId?: string | null, limit = 500): EventRecord[] {
    const rows = taskId
      ? (this.db
          .prepare("SELECT * FROM events WHERE run_id = ? AND task_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?")
          .all(runId, taskId, sinceSeq, limit) as Record<string, unknown>[])
      : (this.db
          .prepare("SELECT * FROM events WHERE run_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?")
          .all(runId, sinceSeq, limit) as Record<string, unknown>[]);
    return rows.map((r) => ({
      seq: Number(r.seq),
      runId: String(r.run_id),
      taskId: r.task_id ? String(r.task_id) : null,
      type: String(r.type),
      payload: JSON.parse(String(r.payload_json)),
      createdAt: Number(r.created_at),
    }));
  }

  /** Read one bounded page of the snapshot. A single oversized payload travels in byte slices. */
  getReplayPage(runId: string, taskId: string | null, cursor: ReplayCursor, throughSeq: number, runNoticesAfter?: number): ReplayPage {
    // Focused attachments also receive run-wide notices that arrive after attachment.
    const taskFilter = !taskId ? "" : runNoticesAfter === undefined
      ? "AND task_id = ?"
      : "AND (task_id = ? OR (task_id IS NULL AND seq > ?))";
    const args: Array<string | number> = [runId, cursor.sinceSeq, throughSeq];
    if (taskId) args.push(taskId);
    if (taskId && runNoticesAfter !== undefined) args.push(runNoticesAfter);
    using statement = this.db.prepare(`
      SELECT seq, task_id, type, created_at, length(CAST(payload_json AS BLOB)) AS bytes,
             substr(CAST(payload_json AS BLOB), ?, ?) AS data
      FROM events WHERE run_id = ? AND seq > ? AND seq <= ? ${taskFilter}
      ORDER BY seq LIMIT 2049
    `);
    const rows = statement.iterate(cursor.payloadOffset + 1, REPLAY_PAGE_BYTES, ...args) as Iterable<{
      seq: number; task_id: string | null; type: string; created_at: number; bytes: number; data: Uint8Array;
    }>;
    const page: ReplayPage = { replay: [], throughSeq };
    let bytes = 0;
    let sinceSeq = cursor.sinceSeq;
    for (const row of rows) {
      const event: EventRecord = { seq: row.seq, taskId: row.task_id, runId, type: row.type, createdAt: row.created_at, payload: null };
      const size = Buffer.byteLength(JSON.stringify(event)) + row.bytes + 1;
      if (page.replay.length && (bytes + size > REPLAY_PAGE_BYTES || page.replay.length === 2048)) {
        page.next = { sinceSeq, payloadOffset: 0 };
        break;
      }
      if (size > REPLAY_PAGE_BYTES) {
        const offset = cursor.payloadOffset;
        const final = offset + row.data.length >= row.bytes;
        page.fragment = { event, data: Buffer.from(row.data).toString("base64"), final };
        if (!final) page.next = { sinceSeq, payloadOffset: offset + row.data.length };
        else if (row.seq < throughSeq) page.next = { sinceSeq: row.seq, payloadOffset: 0 };
        break;
      }
      bytes += size;
      event.payload = JSON.parse(Buffer.from(row.data).toString("utf8"));
      page.replay.push(event);
      sinceSeq = row.seq;
    }
    return page;
  }

  /** Highest event seq persisted so far (0 when there are no events). */
  getLatestEventSeq(): number {
    const r = this.db.prepare("SELECT MAX(seq) AS seq FROM events").get() as { seq: number | null } | undefined;
    return Number(r?.seq ?? 0);
  }

  /**
   * Searches predecessor event history for a task ("Seance"):
   *  - Scans the 500 most recent non-delta events for `(runId, taskId)`.
   *  - Coalesces up to the 20,000 most recent `agent.text_delta` rows into contiguous turn blocks
   *    delimited by non-delta event boundaries so multi-word phrases match across streaming chunks
   *    and Generation 2+ traces remain searchable past thousands of deltas.
   *  - Ranks matches by distinct query-token overlap count, then recency (`seq DESC`), returning
   *    compact snippets (<= 400 chars centered around the match).
   */
  searchPredecessorHistory(
    runId: string,
    taskId: string,
    query: string,
    limit = 12,
  ): Array<{ seq: number; type: string; snippet: string; createdAt: number }> {
    const rawQuery = query.trim().toLowerCase();
    if (!rawQuery) return [];
    const tokens = [...new Set(rawQuery.split(/\s+/).filter(Boolean))];
    if (tokens.length === 0) return [];
    const maxResults = Math.max(1, Math.floor(limit));

    const extractSnippet = (text: string, maxLen = 400): string => {
      const clean = text.replace(/\s+/g, " ").trim();
      if (clean.length <= maxLen) return clean;
      const lower = clean.toLowerCase();
      let idx = lower.indexOf(rawQuery);
      let matchLen = rawQuery.length;
      if (idx < 0) {
        for (const tok of tokens) {
          const pos = lower.indexOf(tok);
          if (pos >= 0) {
            idx = pos;
            matchLen = tok.length;
            break;
          }
        }
      }
      if (idx < 0) return clean.slice(0, maxLen);
      const half = Math.floor((maxLen - matchLen) / 2);
      let start = Math.max(0, idx - half);
      let end = start + maxLen;
      if (end > clean.length) {
        end = clean.length;
        start = Math.max(0, end - maxLen);
      }
      const prefix = start > 0 ? "…" : "";
      const suffix = end < clean.length ? "…" : "";
      const budget = maxLen - prefix.length - suffix.length;
      return `${prefix}${clean.slice(start, start + budget)}${suffix}`;
    };

    const candidates: Array<{
      seq: number;
      type: string;
      text: string;
      searchText: string;
      createdAt: number;
    }> = [];

    // 1. Most recent 500 non-delta events
    const nonDeltaRows = this.db
      .prepare(
        `SELECT seq, type, payload_json, created_at FROM events
         WHERE run_id = ? AND task_id = ? AND type NOT IN ('agent.text_delta', 'agent.thought_delta')
         ORDER BY seq DESC LIMIT 500`,
      )
      .all(runId, taskId) as Array<{ seq: number; type: string; payload_json: string; created_at: number }>;

    for (const r of nonDeltaRows) {
      const payloadStr = String(r.payload_json ?? "");
      const typeStr = String(r.type);
      candidates.push({
        seq: Number(r.seq),
        type: typeStr,
        text: payloadStr,
        searchText: `${typeStr} ${payloadStr}`.toLowerCase(),
        createdAt: Number(r.created_at),
      });
    }

    // 2. Coalesce up to the 20,000 most recent agent.text_delta rows (from the tail, ordered ASC)
    const deltaRows = this.db
      .prepare(
        `SELECT seq, payload_json, created_at FROM (
           SELECT seq, payload_json, created_at FROM events
           WHERE run_id = ? AND task_id = ? AND type = 'agent.text_delta'
           ORDER BY seq DESC LIMIT 20000
         ) ORDER BY seq ASC`,
      )
      .all(runId, taskId) as Array<{ seq: number; payload_json: string; created_at: number }>;

    if (deltaRows.length > 0) {
      const minDeltaSeq = Number(deltaRows[0]!.seq);
      const boundarySeqs = (
        this.db
          .prepare(
            `SELECT seq FROM events
             WHERE run_id = ? AND task_id = ? AND type NOT IN ('agent.text_delta', 'agent.thought_delta') AND seq >= ?
             ORDER BY seq ASC`,
          )
          .all(runId, taskId, minDeltaSeq) as Array<{ seq: number }>
      ).map((b) => Number(b.seq));

      let bIdx = 0;
      let buf: string[] = [];
      let blockSeq = 0;
      let blockCreatedAt = 0;

      const flush = () => {
        if (buf.length === 0) return;
        const combined = buf.join("");
        if (combined.trim()) {
          candidates.push({
            seq: blockSeq,
            type: "agent.text",
            text: combined,
            searchText: combined.toLowerCase(),
            createdAt: blockCreatedAt,
          });
        }
        buf = [];
      };

      for (const row of deltaRows) {
        const seq = Number(row.seq);
        const createdAt = Number(row.created_at);
        let chunk = "";
        try {
          const parsed = JSON.parse(String(row.payload_json)) as { text?: unknown } | string | null;
          if (typeof parsed === "string") chunk = parsed;
          else if (parsed && typeof parsed.text === "string") chunk = parsed.text;
        } catch {
          continue;
        }
        if (buf.length > 0) {
          let crossedBoundary = false;
          while (bIdx < boundarySeqs.length && boundarySeqs[bIdx]! < seq) {
            crossedBoundary = true;
            bIdx++;
          }
          if (crossedBoundary) flush();
        } else {
          while (bIdx < boundarySeqs.length && boundarySeqs[bIdx]! < seq) bIdx++;
        }
        buf.push(chunk);
        blockSeq = seq;
        blockCreatedAt = createdAt;
      }
      flush();
    }

    const scored: Array<{
      seq: number;
      type: string;
      snippet: string;
      createdAt: number;
      overlap: number;
      phraseHit: number;
    }> = [];

    for (const c of candidates) {
      let overlap = 0;
      for (const tok of tokens) {
        if (c.searchText.includes(tok)) overlap++;
      }
      if (overlap === 0) continue;
      const phraseHit = tokens.length > 1 && c.searchText.includes(rawQuery) ? 1 : 0;
      scored.push({
        seq: c.seq,
        type: c.type,
        snippet: extractSnippet(c.text, 400),
        createdAt: c.createdAt,
        overlap,
        phraseHit,
      });
    }

    scored.sort((a, b) => (b.overlap - a.overlap) || (b.phraseHit - a.phraseHit) || (b.seq - a.seq));
    return scored.slice(0, maxResults).map(({ seq, type, snippet, createdAt }) => ({
      seq,
      type,
      snippet,
      createdAt,
    }));
  }

  // ---- Runs ----
  createRun(input: {
    id?: string;
    goal: string;
    repoRoot: string;
    baseBranch: string;
    baseSha: string;
    coordinationGroup?: string | null;
    isolationMode?: "standalone" | "coordinated";
  }): RunRecord {
    const id = input.id ?? nextFriendlyId(this.db, "run", "runs");
    const now = Date.now();
    const isolation = input.isolationMode ?? (input.coordinationGroup ? "coordinated" : "standalone");
    this.db
      .prepare(
        "INSERT INTO runs(id, goal, repo_root, base_branch, base_sha, coordination_group, isolation_mode, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)",
      )
      .run(id, input.goal, input.repoRoot, input.baseBranch, input.baseSha, input.coordinationGroup ?? null, isolation, now, now);
    const run = this.getRun(id)!;
    this.emitEvent(id, null, "run.created", {
      goal: input.goal,
      baseBranch: input.baseBranch,
      baseSha: input.baseSha,
      coordinationGroup: run.coordinationGroup,
      isolationMode: run.isolationMode,
    });
    return run;
  }

  getRun(id: string): RunRecord | null {
    let r: Record<string, unknown> | undefined;
    for (const candidate of friendlyIdCandidates(id, "run")) {
      r = this.db.prepare("SELECT * FROM runs WHERE id = ?").get(candidate) as Record<string, unknown> | undefined;
      if (r) break;
    }
    if (!r) return null;
    return {
      id: String(r.id),
      goal: String(r.goal),
      repoRoot: String(r.repo_root),
      baseBranch: String(r.base_branch),
      baseSha: String(r.base_sha),
      coordinationGroup: r.coordination_group ? String(r.coordination_group) : null,
      isolationMode: (r.isolation_mode ? String(r.isolation_mode) : "standalone") as RunRecord["isolationMode"],
      status: String(r.status) as RunRecord["status"],
      createdAt: Number(r.created_at),
      updatedAt: Number(r.updated_at),
    };
  }

  listRuns(limit = 50, before?: { createdAt: number; id: string }): RunRecord[] {
    const rows = this.db
      .prepare(`SELECT * FROM runs ${before ? "WHERE (created_at, id) < (?, ?)" : ""} ORDER BY created_at DESC, id DESC LIMIT ?`)
      .all(...(before ? [before.createdAt, before.id, limit] : [limit])) as Record<string, unknown>[];
    return rows.map((r) => ({
      id: String(r.id),
      goal: String(r.goal),
      repoRoot: String(r.repo_root),
      baseBranch: String(r.base_branch),
      baseSha: String(r.base_sha),
      coordinationGroup: r.coordination_group ? String(r.coordination_group) : null,
      isolationMode: (r.isolation_mode ? String(r.isolation_mode) : "standalone") as RunRecord["isolationMode"],
      status: String(r.status) as RunRecord["status"],
      createdAt: Number(r.created_at),
      updatedAt: Number(r.updated_at),
    }));
  }

  updateRunStatus(id: string, status: RunRecord["status"], reason?: string, cause?: string): void {
    const now = Date.now();
    const prev = this.db.prepare("SELECT status FROM runs WHERE id = ?").get(id) as { status: string } | undefined;
    this.db.prepare("UPDATE runs SET status = ?, updated_at = ? WHERE id = ?").run(status, now, id);
    const terminal = ["completed", "failed", "stopped"];
    if (prev && !terminal.includes(prev.status) && terminal.includes(status)) {
      // Entering a terminal state releases leases held by tasks that can no longer write;
      // live and quota-paused tasks keep theirs so a later resume stays conflict-free.
      this.db
        .prepare(
          `UPDATE write_leases SET active = 0
           WHERE run_id = ? AND active = 1
             AND NOT EXISTS (
               SELECT 1 FROM tasks t
               WHERE t.id = write_leases.task_id
                 AND t.status IN ('running', 'draining', 'waiting_children', 'merging', 'paused_quota')
             )`,
        )
        .run(id);
    }
    this.emitEvent(id, null, "run.status", { status, ...(reason ? { reason } : {}), ...(cause ? { cause } : {}) });
  }

  /** Active and quota-paused runs, newest first, without a row cap (unlike listRuns). */
  listActiveOrQuotaPausedRuns(): RunRecord[] {
    const rows = this.db
      .prepare("SELECT id FROM runs WHERE status IN ('active', 'paused_quota') ORDER BY created_at DESC, id DESC")
      .all() as { id: string }[];
    return rows.map((r) => this.getRun(r.id)).filter((r): r is RunRecord => r !== null);
  }

  /**
   * Reason recorded on the latest run.status / quota.run_paused event for a run. When that event has no
   * reason, falls back to the summary of a failed or cancelled task (root tasks first, newest first) that
   * says more than its bare status. Null when no cause was recorded anywhere.
   */
  getLastRunStatusReason(runId: string): string | null {
    const row = this.db
      .prepare(
        "SELECT payload_json FROM events WHERE run_id = ? AND type IN ('run.status', 'quota.run_paused') ORDER BY seq DESC LIMIT 1",
      )
      .get(runId) as { payload_json: string } | undefined;
    if (row) {
      try {
        const reason = (JSON.parse(row.payload_json) as { reason?: unknown } | null)?.reason;
        if (typeof reason === "string" && reason.trim()) return reason.trim();
      } catch {
        // Fall through to task summaries.
      }
    }
    const tasks = this.db
      .prepare(
        `SELECT id, status, summary FROM tasks WHERE run_id = ? AND status IN ('failed', 'cancelled') AND summary IS NOT NULL
         ORDER BY (parent_id IS NULL) DESC, updated_at DESC, created_at DESC`,
      )
      .all(runId) as { id: string; status: string; summary: string }[];
    for (const t of tasks) {
      const summary = t.summary.trim();
      if (!summary || /^(cancelled|failed|stopped)\.?$/i.test(summary)) continue;
      return `task ${t.id} ${t.status}: ${summary.slice(0, 400)}`;
    }
    return null;
  }

  /** Re-points a run at another repository (used when a Director chat switches projects). */
  updateRunRepoInfo(runId: string, repoRoot: string, baseBranch: string, baseSha: string): void {
    this.db
      .prepare("UPDATE runs SET repo_root = ?, base_branch = ?, base_sha = ?, updated_at = ? WHERE id = ?")
      .run(repoRoot, baseBranch, baseSha, Date.now(), runId);
  }

  // ---- Tasks & Write-Scope Leases ----
  createTask(input: {
    runId: string;
    parentId?: string | null;
    tier: AgentTier;
    kind: TaskKind;
    title: string;
    objective: string;
    criteria?: string;
    readOnly?: boolean;
    profileId: string;
    model?: string | null;
    effort?: string | null;
    maxTurns?: number;
    dependsOn?: string[];
    writeScopes?: string[];
  }): TaskRecord {
    return this.transaction(() => {
      const id = nextFriendlyId(this.db, "t", "tasks");
      const now = Date.now();
      const deps = input.dependsOn ?? [];
      const scopes = input.writeScopes ?? [];

      // Validate write-scope disjointness across ALL active leases in the same `repo_root`:
      //  - Same `(repo_root, target_branch)`: HARD conflict (prevents sibling/cross-run collisions)
      //  - Different `target_branch` in same `repo_root`: allowed, emits non-blocking overlap advisories!
      const run = this.getRun(input.runId);
      if (!run) throw new Error(`Unknown run ${input.runId}`);
      const crossBranchOverlaps: Array<{ otherRunId: string; otherTaskId: string; glob: string }> = [];

      // Resolve dependencies (supporting transitive dependency exemption for sequential write leases)
      const resolvedDeps: TaskRecord[] = [];
      for (const depId of deps) {
        const depTask = this.getTask(depId);
        if (!depTask) throw new Error(`Unknown dependency task ${depId}`);
        if (["failed", "cancelled", "returned_for_rescope"].includes(depTask.status)) {
          throw new Error(`Cannot depend on task ${depTask.id} because its status is '${depTask.status}'`);
        }
        resolvedDeps.push(depTask);
      }
      const depAncestors = new Set<string>();
      const depQueue = resolvedDeps.map((d) => d.id);
      while (depQueue.length > 0) {
        const curId = depQueue.shift()!;
        if (depAncestors.has(curId)) continue;
        depAncestors.add(curId);
        const parents = this.db
          .prepare("SELECT depends_on FROM task_dependencies WHERE task_id = ?")
          .all(curId) as Array<{ depends_on: string }>;
        for (const p of parents) depQueue.push(p.depends_on);
      }

      if (!input.readOnly && scopes.length > 0) {
        this.purgeNonLiveWriteLeases(run.repoRoot);
        const activeLeases = this.db
          .prepare(
            `SELECT wl.run_id, wl.target_branch, wl.task_id, wl.glob_pattern
             FROM write_leases wl
             JOIN tasks t ON t.id = wl.task_id
             LEFT JOIN runs r ON r.id = wl.run_id
             WHERE wl.repo_root = ?
               AND (wl.active = 1
                    OR (t.status = 'pending' AND r.status NOT IN ('completed', 'failed', 'stopped')))`,
          )
          .all(run.repoRoot) as Array<{ run_id: string; target_branch: string; task_id: string; glob_pattern: string }>;
        for (const requested of scopes) {
          for (const existing of activeLeases) {
            if (depAncestors.has(existing.task_id)) continue; // Sequential dependent task will wait until predecessor completes
            if (!globsOverlap(requested, existing.glob_pattern)) continue;
            if (existing.target_branch === run.baseBranch) {
              throw new WriteScopeConflictError(
                `Write-scope conflict on (${run.repoRoot}:${run.baseBranch}): '${requested}' overlaps active lease '${existing.glob_pattern}' held by task ${existing.task_id} (run ${existing.run_id})`,
                requested,
                [{ runId: existing.run_id, taskId: existing.task_id, glob: existing.glob_pattern }],
              );
            }
            crossBranchOverlaps.push({
              otherRunId: existing.run_id,
              otherTaskId: existing.task_id,
              glob: existing.glob_pattern,
            });
          }
        }
      }

      const unresolvedDeps = resolvedDeps.filter((d) => d.status !== "completed");
      const initialStatus: TaskStatus = unresolvedDeps.length > 0 ? "pending" : "ready";
      this.db
        .prepare(
          `INSERT INTO tasks(
            id, run_id, parent_id, tier, kind, title, objective, criteria,
            read_only, profile_id, model, effort, status, generation, max_turns,
            created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)`,
        )
        .run(
          id,
          input.runId,
          input.parentId ?? null,
          input.tier,
          input.kind,
          input.title,
          input.objective,
          input.criteria ?? "",
          input.readOnly ? 1 : 0,
          input.profileId,
          input.model ?? null,
          input.effort ?? null,
          initialStatus,
          input.maxTurns ?? (input.tier === 3 ? 20 : 120),
          now,
          now,
        );

      for (const dep of deps) {
        this.db.prepare("INSERT INTO task_dependencies(task_id, depends_on) VALUES (?, ?)").run(id, dep);
      }
      if (!input.readOnly) {
        const leaseActive = initialStatus === "pending" ? 0 : 1;
        for (const glob of scopes) {
          this.db
            .prepare(
              "INSERT INTO write_leases(id, run_id, repo_root, target_branch, task_id, glob_pattern, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .run(nextFriendlyId(this.db, "lease", "write_leases"), input.runId, run.repoRoot, run.baseBranch, id, glob, leaseActive, now);
        }
      }

      for (const ov of crossBranchOverlaps) {
        this.emitEvent(ov.otherRunId, null, "peer.scope_overlap_warning", {
          otherRunId: input.runId,
          otherTaskId: id,
          overlappingGlob: ov.glob,
        });
        this.emitEvent(input.runId, id, "peer.scope_overlap_warning", {
          otherRunId: ov.otherRunId,
          otherTaskId: ov.otherTaskId,
          overlappingGlob: ov.glob,
        });
      }

      const task = this.getTask(id)!;
      this.emitEvent(input.runId, id, "task.created", {
        tier: task.tier,
        kind: task.kind,
        title: task.title,
        parentId: task.parentId,
        profileId: task.profileId,
        model: task.model,
        effort: task.effort,
        writeScopes: scopes,
        dependsOn: deps,
      });
      return task;
    });
  }

  private mapTask(r: Record<string, unknown>): TaskRecord {
    return {
      id: String(r.id),
      runId: String(r.run_id),
      parentId: r.parent_id ? String(r.parent_id) : null,
      tier: Number(r.tier) as AgentTier,
      kind: String(r.kind) as TaskKind,
      title: String(r.title),
      objective: String(r.objective),
      criteria: String(r.criteria),
      readOnly: Boolean(r.read_only),
      profileId: String(r.profile_id),
      model: r.model ? String(r.model) : null,
      effort: r.effort ? String(r.effort) : null,
      status: String(r.status) as TaskStatus,
      generation: Number(r.generation),
      maxTurns: Number(r.max_turns),
      turnsUsed: Number(r.turns_used),
      contextUsed: Number(r.context_used),
      contextSize: Number(r.context_size),
      worktreeId: r.worktree_id ? String(r.worktree_id) : null,
      sessionId: r.session_id ? String(r.session_id) : null,
      summary: r.summary ? String(r.summary) : null,
      handoffJson: r.handoff_json ? String(r.handoff_json) : null,
      resultJson: r.result_json ? String(r.result_json) : null,
      createdAt: Number(r.created_at),
      updatedAt: Number(r.updated_at),
    };
  }

  getTask(id: string): TaskRecord | null {
    for (const candidate of friendlyIdCandidates(id, "t")) {
      const r = this.db.prepare("SELECT * FROM tasks WHERE id = ?").get(candidate) as Record<string, unknown> | undefined;
      if (r) return this.mapTask(r);
    }
    return null;
  }

  listTasks(runId: string, filters?: { parentId?: string | null; status?: TaskStatus }): TaskRecord[] {
    let sql = "SELECT * FROM tasks WHERE run_id = ?";
    const params: (string | number)[] = [runId];
    if (filters?.parentId !== undefined) {
      if (filters.parentId === null) sql += " AND parent_id IS NULL";
      else {
        sql += " AND parent_id = ?";
        params.push(filters.parentId);
      }
    }
    if (filters?.status) {
      sql += " AND status = ?";
      params.push(filters.status);
    }
    sql += " ORDER BY created_at ASC";
    return (this.db.prepare(sql).all(...params) as Record<string, unknown>[]).map((r) => this.mapTask(r));
  }

  updateTask(
    id: string,
    patch: Partial<{
      title: string;
      objective: string;
      status: TaskStatus;
      profileId: string;
      model: string | null;
      effort: string | null;
      generation: number;
      turnsUsed: number;
      contextUsed: number;
      contextSize: number;
      worktreeId: string | null;
      sessionId: string | null;
      summary: string | null;
      handoffJson: string | null;
      resultJson: string | null;
    }>,
  ): TaskRecord {
    const existing = this.getTask(id);
    if (!existing) throw new Error(`Unknown task ${id}`);
    const now = Date.now();

    const sets: string[] = ["updated_at = ?"];
    const values: (string | number | null)[] = [now];
    const map: Record<string, string> = {
      title: "title",
      objective: "objective",
      status: "status",
      profileId: "profile_id",
      model: "model",
      effort: "effort",
      generation: "generation",
      turnsUsed: "turns_used",
      contextUsed: "context_used",
      contextSize: "context_size",
      worktreeId: "worktree_id",
      sessionId: "session_id",
      summary: "summary",
      handoffJson: "handoff_json",
      resultJson: "result_json",
    };

    for (const [k, col] of Object.entries(map)) {
      if (k in patch) {
        sets.push(`${col} = ?`);
        values.push((patch as Record<string, string | number | null>)[k] ?? null);
      }
    }
    values.push(id);
    this.db.prepare(`UPDATE tasks SET ${sets.join(", ")} WHERE id = ?`).run(...values);

    // Release write leases when task reaches a terminal, returned, or blocked state
    if (patch.status && ["completed", "failed", "cancelled", "returned_for_rescope", "blocked"].includes(patch.status)) {
      this.db.prepare("UPDATE write_leases SET active = 0 WHERE task_id = ?").run(id);
    }

    // Automatically unblock dependent pending tasks when a task completes, or cascade failure/cancellation
    if (patch.status === "completed") {
      this.unblockDependents(existing.runId, id);
    } else if (patch.status && ["failed", "cancelled", "returned_for_rescope"].includes(patch.status)) {
      this.cascadeDependencyFailure(existing.runId, id, patch.status);
    }

    const updated = this.getTask(id)!;
    if (patch.status && patch.status !== existing.status) {
      this.emitEvent(existing.runId, id, "task.status", { from: existing.status, to: patch.status, summary: patch.summary });
    }
    return updated;
  }

  private unblockDependents(runId: string, completedTaskId: string): void {
    const dependents = this.db
      .prepare("SELECT task_id FROM task_dependencies WHERE depends_on = ?")
      .all(completedTaskId) as { task_id: string }[];
    const now = Date.now();
    for (const { task_id } of dependents) {
      const remaining = this.db
        .prepare(
          `SELECT COUNT(*) AS cnt FROM task_dependencies d
           JOIN tasks t ON t.id = d.depends_on
           WHERE d.task_id = ? AND t.status != 'completed'`,
        )
        .get(task_id) as { cnt: number };
      if (remaining.cnt === 0) {
        this.db.prepare("UPDATE tasks SET status = 'ready', updated_at = ? WHERE id = ? AND status = 'pending'").run(now, task_id);
        this.emitEvent(runId, task_id, "task.unblocked", { by: completedTaskId });
      }
    }
  }

  private cascadeDependencyFailure(runId: string, failedTaskId: string, reasonStatus: TaskStatus): void {
    const queue = [failedTaskId];
    const visited = new Set<string>([failedTaskId]);
    const now = Date.now();
    while (queue.length > 0) {
      const depId = queue.shift()!;
      const dependents = this.db
        .prepare("SELECT task_id FROM task_dependencies WHERE depends_on = ?")
        .all(depId) as { task_id: string }[];
      for (const { task_id } of dependents) {
        if (visited.has(task_id)) continue;
        visited.add(task_id);
        const target = this.getTask(task_id);
        if (!target || target.status !== "pending") continue;
        const summary = `Cancelled because dependency ${failedTaskId} ended with status '${reasonStatus}'`;
        this.db
          .prepare("UPDATE tasks SET status = 'cancelled', summary = ?, updated_at = ? WHERE id = ? AND status = 'pending'")
          .run(summary, now, task_id);
        this.db.prepare("UPDATE write_leases SET active = 0 WHERE task_id = ?").run(task_id);
        this.emitEvent(runId, task_id, "task.status", { from: "pending", to: "cancelled", summary });
        queue.push(task_id);
      }
    }
  }

  updateRunGoal(runId: string, goal: string): void {
    this.db.prepare("UPDATE runs SET goal = ?, updated_at = ? WHERE id = ?").run(goal, Date.now(), runId);
  }

  releaseWriteLeasesForRun(runId: string): void {
    this.db.prepare("UPDATE write_leases SET active = 0 WHERE run_id = ? AND active = 1").run(runId);
  }

  /**
   * Deactivates active leases in `repoRoot` whose holder can no longer write: the holder task is missing or not
   * pending, ready, live or quota-paused, or its run is terminal and the holder is neither live nor quota-paused.
   * Quota-paused holders always keep their leases.
   */
  purgeNonLiveWriteLeases(repoRoot: string): number {
    const res = this.db
      .prepare(
        `UPDATE write_leases SET active = 0
         WHERE repo_root = ? AND active = 1
           AND (
             NOT EXISTS (
               SELECT 1 FROM tasks t WHERE t.id = write_leases.task_id
                 AND t.status IN ('pending', 'ready', 'running', 'draining', 'waiting_children', 'merging', 'paused_quota')
             )
             OR (
               EXISTS (SELECT 1 FROM runs r WHERE r.id = write_leases.run_id AND r.status IN ('completed', 'failed', 'stopped'))
               AND NOT EXISTS (
                 SELECT 1 FROM tasks t WHERE t.id = write_leases.task_id
                   AND t.status IN ('running', 'draining', 'waiting_children', 'merging', 'paused_quota')
               )
             )
           )`,
      )
      .run(repoRoot);
    return res.changes;
  }

  /**
   * Deactivates every active lease not owned by a quota-paused task, across all runs.
   * Only safe at daemon startup while no task is live in memory.
   */
  purgeStaleWriteLeasesOnStartup(): number {
    const res = this.db
      .prepare(
        `UPDATE write_leases SET active = 0
         WHERE active = 1
           AND NOT EXISTS (
             SELECT 1 FROM tasks t
             WHERE t.id = write_leases.task_id
               AND t.status = 'paused_quota'
           )`,
      )
      .run();
    return res.changes;
  }

  failStaleMergesForRun(runId: string, reason: string): number {
    const now = Date.now();
    const res = this.db
      .prepare("UPDATE merge_queue SET status = 'ci_failed', log = ?, finished_at = ? WHERE run_id = ? AND status IN ('queued', 'running')")
      .run(reason, now, runId);
    return res.changes;
  }

  listPendingMergesForTarget(repoRoot: string, targetBranch: string): MergeQueueRecord[] {
    const rows = this.db
      .prepare(
        `SELECT mq.* FROM merge_queue mq
         JOIN runs r ON r.id = mq.run_id
         WHERE r.repo_root = ? AND r.base_branch = ? AND mq.status = 'queued'
         ORDER BY mq.created_at ASC`,
      )
      .all(repoRoot, targetBranch) as Record<string, unknown>[];
    return rows.map((r) => this.getMergeQueueItem(String(r.id))!);
  }

  listAnsweredMessagesFrom(taskId: string): MessageRecord[] {
    const rows = this.db
      .prepare("SELECT * FROM messages WHERE from_task_id = ? AND status = 'answered' ORDER BY answered_at DESC LIMIT 20")
      .all(taskId) as Record<string, unknown>[];
    return rows.map((r) => ({
      id: String(r.id),
      runId: String(r.run_id),
      fromTaskId: String(r.from_task_id),
      toTaskId: String(r.to_task_id),
      question: String(r.question),
      answer: r.answer ? String(r.answer) : null,
      status: String(r.status) as MessageRecord["status"],
      createdAt: Number(r.created_at),
      answeredAt: r.answered_at == null ? null : Number(r.answered_at),
    }));
  }

  listOpenPeerExchangesForRun(runId: string): PeerExchangeRecord[] {
    const rows = this.db
      .prepare("SELECT * FROM peer_exchanges WHERE to_run_id = ? AND status = 'open' ORDER BY created_at ASC LIMIT 20")
      .all(runId) as Record<string, unknown>[];
    return rows.map((r) => ({
      id: String(r.id),
      fromRunId: String(r.from_run_id),
      toRunId: String(r.to_run_id),
      kind: String(r.kind) as PeerExchangeRecord["kind"],
      topicKey: String(r.topic_key),
      round: Number(r.round),
      message: String(r.message),
      reply: r.reply ? String(r.reply) : null,
      status: String(r.status) as PeerExchangeRecord["status"],
      createdAt: Number(r.created_at),
      answeredAt: r.answered_at == null ? null : Number(r.answered_at),
    }));
  }

  // ---- Deterministic Artifact Index ----
  recordArtifact(input: {
    runId: string;
    taskId: string;
    kind: ArtifactRecord["kind"];
    path: string;
    summary: string;
    gitSha?: string | null;
    exitCode?: number | null;
  }): ArtifactRecord {
    const id = nextFriendlyId(this.db, "art", "artifacts");
    const now = Date.now();
    this.db
      .prepare(
        "INSERT INTO artifacts(id, run_id, task_id, kind, path, summary, git_sha, exit_code, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
      )
      .run(id, input.runId, input.taskId, input.kind, input.path, input.summary.slice(0, 1000), input.gitSha ?? null, input.exitCode ?? null, now);
    const art: ArtifactRecord = {
      id,
      runId: input.runId,
      taskId: input.taskId,
      kind: input.kind,
      path: input.path,
      summary: input.summary.slice(0, 1000),
      gitSha: input.gitSha ?? null,
      exitCode: input.exitCode ?? null,
      createdAt: now,
    };
    this.emitEvent(input.runId, input.taskId, "artifact.recorded", { id, kind: art.kind, path: art.path });
    return art;
  }

  hasDossier(runId: string, path: string): boolean {
    return !!this.db.prepare("SELECT 1 FROM artifacts WHERE run_id = ? AND path = ? AND kind = 'dossier' LIMIT 1").get(runId, path);
  }

  /** Durable monotonic sequence for spill file indices, so a deleted file's name is never reissued across restarts. */
  nextArtifactSeq(scopeKey: string): number {
    return nextSequenceValue(this.db, `artseq:${scopeKey}`);
  }

  /** Raises the spill sequence for scopeKey so the next value is above floor (e.g. the highest index on disk). */
  advanceArtifactSeqFloor(scopeKey: string, floor: number): void {
    advanceSequenceFloor(this.db, `artseq:${scopeKey}`, floor);
  }

  /** Highest N among indexed dossier paths named `<stem>-<N><ext>` (optionally for one task); 0 when none. */
  maxIndexedArtifactSeq(stem: string, ext: string, taskId?: string): number {
    const rows = (
      taskId
        ? this.db.prepare("SELECT path FROM artifacts WHERE kind = 'dossier' AND task_id = ?").all(taskId)
        : this.db.prepare("SELECT path FROM artifacts WHERE kind = 'dossier'").all()
    ) as Array<{ path: string }>;
    const prefix = `${stem}-`;
    let max = 0;
    for (const row of rows) {
      const name = basename(String(row.path));
      if (name.length <= prefix.length + ext.length || !name.startsWith(prefix) || !name.endsWith(ext)) continue;
      const mid = name.slice(prefix.length, name.length - ext.length);
      if (!/^[1-9][0-9]{0,9}$/.test(mid)) continue;
      const n = Number.parseInt(mid, 10);
      if (n < 0x80000000 && n > max) max = n;
    }
    return max;
  }

  /** Every distinct path indexed as a dossier by any run. */
  listDistinctDossierPaths(): string[] {
    const rows = this.db
      .prepare("SELECT DISTINCT path FROM artifacts WHERE kind = 'dossier'")
      .all() as Array<{ path: string }>;
    return rows.map(r => String(r.path));
  }

  /** Number of distinct runs that index any of paths as a dossier. */
  countDossierOwners(paths: string[]): number {
    const unique = [...new Set(paths)];
    if (unique.length === 0) return 0;
    const placeholders = unique.map(() => "?").join(", ");
    const row = this.db
      .prepare(`SELECT COUNT(DISTINCT run_id) AS n FROM artifacts WHERE kind = 'dossier' AND path IN (${placeholders})`)
      .get(...unique) as { n: number };
    return Number(row?.n ?? 0);
  }

  listArtifacts(runId: string, taskId?: string, limit = 100, kind?: ArtifactRecord["kind"]): ArtifactRecord[] {
    let sql = "SELECT * FROM artifacts WHERE run_id = ?";
    const params: string[] = [runId];
    if (taskId) {
      sql += " AND task_id = ?";
      params.push(taskId);
    }
    if (kind) {
      sql += " AND kind = ?";
      params.push(kind);
    }
    const rows = this.db.prepare(sql + " ORDER BY created_at DESC LIMIT ?").all(...params, limit) as Record<string, unknown>[];
    return rows.map((r) => ({
      id: String(r.id),
      runId: String(r.run_id),
      taskId: String(r.task_id),
      kind: String(r.kind) as ArtifactRecord["kind"],
      path: String(r.path),
      summary: String(r.summary),
      gitSha: r.git_sha ? String(r.git_sha) : null,
      exitCode: r.exit_code == null ? null : Number(r.exit_code),
      createdAt: Number(r.created_at),
    }));
  }

  // ---- Escalations & Scope Send-Backs ----
  createEscalation(input: {
    runId: string;
    fromTaskId: string;
    toTaskId: string | null;
    severity: EscalationSeverity;
    category: EscalationCategory;
    title: string;
    evidencePaths: string[];
    attempted?: string[];
    suggestedSplits?: string[];
    blocking?: boolean;
  }): EscalationRecord {
    if (input.evidencePaths.length === 0) {
      throw new Error("Escalations require at least one concrete evidence path or artifact reference");
    }
    if (input.category === "scope_too_large" && (input.suggestedSplits?.length ?? 0) < 2) {
      throw new Error("scope_too_large requires at least 2 concrete suggested_splits so parent can re-scope cleanly");
    }
    const id = nextFriendlyId(this.db, "esc", "escalations");
    const now = Date.now();
    this.db
      .prepare(
        `INSERT INTO escalations(
          id, run_id, from_task_id, to_task_id, severity, category, title,
          evidence_paths_json, attempted_json, suggested_splits_json, blocking, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)`,
      )
      .run(
        id,
        input.runId,
        input.fromTaskId,
        input.toTaskId,
        input.severity,
        input.category,
        input.title,
        JSON.stringify(input.evidencePaths),
        JSON.stringify(input.attempted ?? []),
        JSON.stringify(input.suggestedSplits ?? []),
        input.blocking === false ? 0 : 1,
        now,
      );
    const esc = this.getEscalation(id)!;
    this.emitEvent(input.runId, input.fromTaskId, "escalation.created", esc);
    return esc;
  }

  getEscalation(id: string): EscalationRecord | null {
    let r: Record<string, unknown> | undefined;
    for (const candidate of friendlyIdCandidates(id, "esc")) {
      r = this.db.prepare("SELECT * FROM escalations WHERE id = ?").get(candidate) as Record<string, unknown> | undefined;
      if (r) break;
    }
    if (!r) return null;
    return {
      id: String(r.id),
      runId: String(r.run_id),
      fromTaskId: String(r.from_task_id),
      toTaskId: r.to_task_id ? String(r.to_task_id) : null,
      severity: String(r.severity) as EscalationSeverity,
      category: String(r.category) as EscalationCategory,
      title: String(r.title),
      evidencePaths: JSON.parse(String(r.evidence_paths_json)) as string[],
      attempted: JSON.parse(String(r.attempted_json)) as string[],
      suggestedSplits: JSON.parse(String(r.suggested_splits_json)) as string[],
      blocking: Boolean(r.blocking),
      status: String(r.status) as EscalationRecord["status"],
      resolution: r.resolution ? String(r.resolution) : null,
      createdAt: Number(r.created_at),
      ackedAt: r.acked_at == null ? null : Number(r.acked_at),
    };
  }

  listEscalations(runId: string, status?: EscalationRecord["status"] | "unresolved"): EscalationRecord[] {
    const rows = status === "unresolved"
      ? (this.db
          .prepare("SELECT * FROM escalations WHERE run_id = ? AND status IN ('open', 'acked') ORDER BY created_at DESC")
          .all(runId) as Record<string, unknown>[])
      : status
      ? (this.db
          .prepare("SELECT * FROM escalations WHERE run_id = ? AND status = ? ORDER BY created_at DESC")
          .all(runId, status) as Record<string, unknown>[])
      : (this.db
          .prepare("SELECT * FROM escalations WHERE run_id = ? ORDER BY created_at DESC")
          .all(runId) as Record<string, unknown>[]);
    return rows.map((r) => this.getEscalation(String(r.id))!);
  }

  listAllEscalations(opts?: {
    runIds?: string[];
    status?: EscalationRecord["status"] | "unresolved";
    limit?: number;
  }): EscalationRecord[] {
    const where: string[] = [];
    const params: (string | number)[] = [];
    if (opts?.runIds) {
      if (opts.runIds.length === 0) return [];
      where.push(`run_id IN (${opts.runIds.map(() => "?").join(",")})`);
      params.push(...opts.runIds);
    }
    if (opts?.status === "unresolved") {
      // Filter before LIMIT so older open/acked rows are never hidden behind newer resolved ones.
      where.push("status IN ('open', 'acked')");
    } else if (opts?.status) {
      where.push("status = ?");
      params.push(opts.status);
    }
    const sql = `SELECT id FROM escalations${where.length ? ` WHERE ${where.join(" AND ")}` : ""} ORDER BY created_at DESC LIMIT ?`;
    params.push(opts?.limit ?? 100);
    const rows = this.db.prepare(sql).all(...params) as Array<{ id: string }>;
    return rows.map((r) => this.getEscalation(r.id)!).filter(Boolean);
  }

  listMergeQueue(opts?: { runId?: string; runIds?: string[]; limit?: number }): MergeQueueRecord[] {
    const where: string[] = [];
    const params: (string | number)[] = [];
    if (opts?.runId) {
      where.push("run_id = ?");
      params.push(opts.runId);
    } else if (opts?.runIds) {
      if (opts.runIds.length === 0) return [];
      where.push(`run_id IN (${opts.runIds.map(() => "?").join(",")})`);
      params.push(...opts.runIds);
    }
    const sql = `SELECT id FROM merge_queue${where.length ? ` WHERE ${where.join(" AND ")}` : ""} ORDER BY created_at DESC LIMIT ?`;
    params.push(opts?.limit ?? 100);
    const rows = this.db.prepare(sql).all(...params) as Array<{ id: string }>;
    return rows.map((r) => this.getMergeQueueItem(r.id)!).filter(Boolean);
  }

  listMessagesForRun(runId: string, limit = 50): MessageRecord[] {
    const rows = this.db
      .prepare("SELECT * FROM messages WHERE run_id = ? ORDER BY created_at DESC LIMIT ?")
      .all(runId, limit) as Record<string, unknown>[];
    return rows.map((r) => this.mapMessageRow(r));
  }

  getEventBySeq(seq: number): EventRecord | null {
    const r = this.db.prepare("SELECT * FROM events WHERE seq = ?").get(seq) as Record<string, unknown> | undefined;
    if (!r) return null;
    return {
      seq: Number(r.seq),
      runId: String(r.run_id),
      taskId: r.task_id ? String(r.task_id) : null,
      type: String(r.type),
      payload: JSON.parse(String(r.payload_json)),
      createdAt: Number(r.created_at),
    };
  }

  ackEscalation(
    id: string,
    resolution: string,
    resolve = true,
    auditExtra?: Record<string, unknown>,
  ): EscalationRecord {
    if (resolve) return this.tryResolveEscalation(id, resolution, auditExtra).escalation;
    return this.transaction(() => {
      const esc = this.getEscalation(id);
      if (!esc) throw new Error(`Unknown escalation ${id}`);
      const status = "acked";
      this.db
        .prepare("UPDATE escalations SET status = ?, resolution = ?, acked_at = ? WHERE id = ?")
        .run(status, resolution, Date.now(), esc.id);
      const updated = this.getEscalation(esc.id)!;
      this.emitEvent(esc.runId, esc.fromTaskId, "escalation.updated", {
        ...(auditExtra ?? {}),
        id: esc.id,
        status,
        resolution,
        previousStatus: esc.status,
      });
      return updated;
    });
  }

  /**
   * Atomically resolves one escalation only if it is still open or acked. A row that is already resolved keeps its
   * existing resolution, and no second escalation.updated event is emitted.
   */
  tryResolveEscalation(
    id: string,
    resolution: string,
    auditExtra?: Record<string, unknown>,
  ): { escalation: EscalationRecord; transitioned: boolean } {
    return this.transaction(() => {
      const cur = this.getEscalation(id);
      if (!cur) throw new Error(`Unknown escalation ${id}`);
      const res = this.db
        .prepare(
          "UPDATE escalations SET status = 'resolved', resolution = ?, acked_at = ? WHERE id = ? AND status IN ('open', 'acked')",
        )
        .run(resolution, Date.now(), cur.id);
      if (res.changes === 0) return { escalation: this.getEscalation(cur.id)!, transitioned: false };
      const updated = this.getEscalation(cur.id)!;
      this.emitEvent(cur.runId, cur.fromTaskId, "escalation.updated", {
        ...(auditExtra ?? {}),
        id: cur.id,
        status: "resolved",
        resolution,
        previousStatus: cur.status,
      });
      return { escalation: updated, transitioned: true };
    });
  }

  /** Resolves every open or acked escalation on a run, optionally limited to some categories. */
  resolveOpenEscalationsForRun(
    runId: string,
    resolution: string,
    auditExtra?: Record<string, unknown>,
    filter?: { categories?: EscalationCategory[] },
  ): EscalationRecord[] {
    const categories = filter?.categories;
    if (categories && categories.length === 0) return [];
    const categorySql = categories ? ` AND category IN (${categories.map(() => "?").join(", ")})` : "";
    return this.transaction(() => {
      const rows = this.db
        .prepare(
          `SELECT id FROM escalations WHERE run_id = ? AND status IN ('open', 'acked')${categorySql} ORDER BY created_at ASC`,
        )
        .all(runId, ...(categories ?? [])) as Array<{ id: string }>;
      const resolved: EscalationRecord[] = [];
      for (const row of rows) {
        const r = this.tryResolveEscalation(row.id, resolution, auditExtra);
        if (r.transitioned) resolved.push(r.escalation);
      }
      return resolved;
    });
  }

  /**
   * Idempotently resolves open or acked escalations on runs whose linked goals are all achieved or cancelled.
   * Runs with any goal in another status (awaiting_human, executing, paused, ...) are left untouched.
   * Selection and every guarded transition run in one BEGIN IMMEDIATE transaction; only rows that actually
   * transitioned are counted and announced.
   */
  reconcileSettledGoalEscalations(): {
    resolved: number;
    goals: number;
    runs: number;
    items: Array<{
      escalationId: string;
      runId: string;
      goalId: string;
      goalStatus: "achieved" | "cancelled";
      previousStatus: "open" | "acked";
    }>;
  } {
    type Row = {
      escalationId: string;
      runId: string;
      goalId: string;
      goalStatus: "achieved" | "cancelled";
      previousStatus: "open" | "acked";
    };
    return this.transaction(() => {
      let rows: Row[];
      try {
        rows = this.db
          .prepare(
            `SELECT e.id AS escalationId, e.run_id AS runId, g.id AS goalId, g.status AS goalStatus, e.status AS previousStatus
             FROM escalations e
             JOIN goals g ON g.linked_run_id = e.run_id
             WHERE e.status IN ('open', 'acked')
               AND g.status IN ('achieved', 'cancelled')
               AND NOT EXISTS (
                 SELECT 1 FROM goals g2
                 WHERE g2.linked_run_id = e.run_id AND g2.status NOT IN ('achieved', 'cancelled')
               )
             ORDER BY g.updated_at DESC, e.created_at ASC`,
          )
          .all() as Row[];
      } catch (err) {
        // Standalone ControlPlaneDb without a GoalEngine has no goals table; every other error is real.
        if (err instanceof Error && /no such table:\s*goals/i.test(err.message)) {
          return { resolved: 0, goals: 0, runs: 0, items: [] };
        }
        throw err;
      }
      const update = this.db.prepare(
        `UPDATE escalations SET status = 'resolved', resolution = ?, acked_at = ?
         WHERE id = ? AND status IN ('open', 'acked')
           AND EXISTS (
             SELECT 1 FROM goals g WHERE g.linked_run_id = escalations.run_id AND g.status IN ('achieved', 'cancelled')
           )
           AND NOT EXISTS (
             SELECT 1 FROM goals g2 WHERE g2.linked_run_id = escalations.run_id AND g2.status NOT IN ('achieved', 'cancelled')
           )`,
      );
      const seen = new Set<string>();
      const items: Row[] = [];
      for (const row of rows) {
        if (seen.has(row.escalationId)) continue;
        seen.add(row.escalationId);
        const cur = this.getEscalation(row.escalationId);
        if (!cur) continue;
        const resolution = `Auto-resolved: goal ${row.goalId} ${row.goalStatus} (reconciled)`;
        const res = update.run(resolution, Date.now(), cur.id);
        if (res.changes === 0) continue;
        this.emitEvent(cur.runId, cur.fromTaskId, "escalation.updated", {
          reconciled: true,
          resolvedBy: "reconcile",
          goalId: row.goalId,
          goalStatus: row.goalStatus,
          id: cur.id,
          status: "resolved",
          resolution,
          previousStatus: cur.status,
        });
        items.push({ ...row, previousStatus: cur.status as Row["previousStatus"] });
      }
      return {
        resolved: items.length,
        goals: new Set(items.map((i) => i.goalId)).size,
        runs: new Set(items.map((i) => i.runId)).size,
        items,
      };
    });
  }

  // ---- Blackboard & Mediated Messaging ----
  blackboardSet(runId: string, key: string, value: unknown, authorTaskId: string): void {
    const now = Date.now();
    this.db
      .prepare(
        `INSERT INTO blackboard(run_id, key, value_json, author_task_id, updated_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(run_id, key) DO UPDATE SET value_json = excluded.value_json, author_task_id = excluded.author_task_id, updated_at = excluded.updated_at`,
      )
      .run(runId, key, JSON.stringify(value), authorTaskId, now);
    this.emitEvent(runId, authorTaskId, "blackboard.updated", { key });
  }

  blackboardGet(runId: string, key?: string): Record<string, { value: unknown; authorTaskId: string; updatedAt: number }> {
    const rows = key
      ? (this.db.prepare("SELECT * FROM blackboard WHERE run_id = ? AND key = ?").all(runId, key) as Record<string, unknown>[])
      : (this.db.prepare("SELECT * FROM blackboard WHERE run_id = ? ORDER BY key ASC").all(runId) as Record<string, unknown>[]);
    const out: Record<string, { value: unknown; authorTaskId: string; updatedAt: number }> = {};
    for (const r of rows) {
      out[String(r.key)] = {
        value: JSON.parse(String(r.value_json)),
        authorTaskId: String(r.author_task_id),
        updatedAt: Number(r.updated_at),
      };
    }
    return out;
  }

  private hasMessageExpiredReasonColumn = false;

  private ensureMessageExpiredReasonColumn(): void {
    if (this.hasMessageExpiredReasonColumn) return;
    const cols = new Set(
      (this.db.prepare("PRAGMA table_info(messages)").all() as Array<{ name: string }>).map((c) => c.name),
    );
    if (!cols.has("expired_reason")) {
      try {
        this.db.exec("ALTER TABLE messages ADD COLUMN expired_reason TEXT;");
      } catch (err) {
        if (!(err instanceof Error && /duplicate column name/i.test(err.message))) throw err;
      }
    }
    this.hasMessageExpiredReasonColumn = true;
  }

  private mapMessageRow(r: Record<string, unknown>): MessageRecord {
    const expiredReason = r.expired_reason == null ? null : String(r.expired_reason);
    return {
      id: String(r.id),
      runId: String(r.run_id),
      fromTaskId: String(r.from_task_id),
      toTaskId: String(r.to_task_id),
      question: String(r.question),
      answer: r.answer == null ? null : String(r.answer),
      ...(expiredReason !== null ? { expiredReason } : {}),
      status: String(r.status) as MessageRecord["status"],
      createdAt: Number(r.created_at),
      answeredAt: r.answered_at == null ? null : Number(r.answered_at),
    };
  }

  getMessage(id: string): MessageRecord | null {
    const r = this.db.prepare("SELECT * FROM messages WHERE id = ?").get(id) as Record<string, unknown> | undefined;
    return r ? this.mapMessageRow(r) : null;
  }

  createMessage(runId: string, fromTaskId: string, toTaskId: string, question: string): MessageRecord {
    const id = nextFriendlyId(this.db, "msg", "messages");
    const now = Date.now();
    this.db
      .prepare("INSERT INTO messages(id, run_id, from_task_id, to_task_id, question, status, created_at) VALUES (?, ?, ?, ?, ?, 'open', ?)")
      .run(id, runId, fromTaskId, toTaskId, question, now);
    const msg: MessageRecord = {
      id,
      runId,
      fromTaskId,
      toTaskId,
      question,
      answer: null,
      status: "open",
      createdAt: now,
      answeredAt: null,
    };
    this.emitEvent(runId, toTaskId, "message.received", msg);
    return msg;
  }

  replyMessage(id: string, answer: string): MessageRecord {
    const { msg, transitioned } = this.transaction(() => {
      const existing = this.db.prepare("SELECT * FROM messages WHERE id = ?").get(id) as
        | Record<string, unknown>
        | undefined;
      if (!existing) throw new Error(`Unknown message ${id}`);
      const now = Date.now();
      const res = this.db
        .prepare("UPDATE messages SET answer = ?, status = 'answered', answered_at = ? WHERE id = ? AND status = 'open'")
        .run(answer, now, id);
      const current = this.db.prepare("SELECT * FROM messages WHERE id = ?").get(id) as Record<string, unknown>;
      return { msg: this.mapMessageRow(current), transitioned: res.changes === 1 };
    });
    if (transitioned) {
      this.emitEvent(msg.runId, msg.fromTaskId, "message.answered", msg);
    }
    return msg;
  }

  expireMessage(id: string, reason: string, now = Date.now()): { message: MessageRecord | null; transitioned: boolean } {
    this.ensureMessageExpiredReasonColumn();
    const { message, transitioned } = this.transaction(() => {
      const res = this.db
        .prepare(
          "UPDATE messages SET status = 'expired', expired_reason = ?, answered_at = ? WHERE id = ? AND status = 'open'",
        )
        .run(reason, now, id);
      const row = this.db.prepare("SELECT * FROM messages WHERE id = ?").get(id) as
        | Record<string, unknown>
        | undefined;
      if (!row) return { message: null, transitioned: false };
      return { message: this.mapMessageRow(row), transitioned: res.changes === 1 };
    });
    if (transitioned && message) {
      this.emitEvent(message.runId, message.fromTaskId, "message.expired", {
        id: message.id,
        runId: message.runId,
        fromTaskId: message.fromTaskId,
        toTaskId: message.toTaskId,
        reason,
      });
    }
    return { message, transitioned };
  }

  listOpenMessages(toTaskId: string): MessageRecord[] {
    const rows = this.db
      .prepare("SELECT * FROM messages WHERE to_task_id = ? AND status = 'open' ORDER BY created_at ASC")
      .all(toTaskId) as Record<string, unknown>[];
    return rows.map((r) => ({
      id: String(r.id),
      runId: String(r.run_id),
      fromTaskId: String(r.from_task_id),
      toTaskId: String(r.to_task_id),
      question: String(r.question),
      answer: null,
      status: "open",
      createdAt: Number(r.created_at),
      answeredAt: null,
    }));
  }

  // ---- Cost ledger ----
  insertCostInvocation(input: Omit<CostInvocationRecord, "id" | "externalSessionId" | "startedAt">): CostInvocationRecord {
    const id = shortHashId("cinv");
    const now = Date.now();
    this.db
      .prepare(
        "INSERT INTO cost_invocations(id, chat_session_id, run_id, task_id, activity, profile_id, billing, model, wire_model, started_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      )
      .run(id, input.chatSessionId, input.runId, input.taskId, input.activity, input.profileId, input.billing, input.model, input.wireModel, now);
    return { ...input, id, externalSessionId: null, startedAt: now };
  }

  setCostInvocationSession(id: string, externalSessionId: string): void {
    this.db.prepare("UPDATE cost_invocations SET external_session_id = ? WHERE id = ?").run(externalSessionId, id);
  }

  /** Invocations owned by the chat session or by any of the runs. */
  listCostInvocations(chatSessionId: string | null, runIds: string[]): CostInvocationRecord[] {
    const clauses: string[] = [];
    const params: Array<string> = [];
    if (chatSessionId !== null) {
      clauses.push("chat_session_id = ?");
      params.push(chatSessionId);
    }
    if (runIds.length > 0) {
      clauses.push(`run_id IN (${runIds.map(() => "?").join(",")})`);
      params.push(...runIds);
    }
    if (clauses.length === 0) return [];
    const rows = this.db
      .prepare(`SELECT * FROM cost_invocations WHERE ${clauses.join(" OR ")} ORDER BY started_at, rowid`)
      .all(...params) as Record<string, unknown>[];
    return rows.map((r) => ({
      id: String(r.id),
      chatSessionId: r.chat_session_id == null ? null : String(r.chat_session_id),
      runId: r.run_id == null ? null : String(r.run_id),
      taskId: r.task_id == null ? null : String(r.task_id),
      activity: String(r.activity) as CostInvocationRecord["activity"],
      profileId: String(r.profile_id),
      billing: String(r.billing) as CostInvocationRecord["billing"],
      model: r.model == null ? null : String(r.model),
      wireModel: r.wire_model == null ? null : String(r.wire_model),
      externalSessionId: r.external_session_id == null ? null : String(r.external_session_id),
      startedAt: Number(r.started_at),
    }));
  }

  getCostBaseline(profileId: string, sessionKey: string, currency: string): number | null {
    const r = this.db
      .prepare("SELECT total FROM cost_baselines WHERE profile_id = ? AND session_key = ? AND currency = ?")
      .get(profileId, sessionKey, currency) as { total: number } | undefined;
    return r ? Number(r.total) : null;
  }

  setCostBaseline(profileId: string, sessionKey: string, currency: string, total: number): void {
    this.db
      .prepare(
        "INSERT INTO cost_baselines(profile_id, session_key, currency, total) VALUES (?, ?, ?, ?) ON CONFLICT(profile_id, session_key, currency) DO UPDATE SET total = excluded.total",
      )
      .run(profileId, sessionKey, currency, total);
  }

  /** Records one charge; a charge whose `dedupKey` was already recorded is ignored. */
  insertCostCharge(input: { invocationId: string; dedupKey: string | null; amount: number; currency: string; model: string | null; source: string }): void {
    this.db
      .prepare(
        "INSERT OR IGNORE INTO cost_charges(invocation_id, dedup_key, amount, currency, model, source, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
      )
      .run(input.invocationId, input.dedupKey, input.amount, input.currency, input.model, input.source, Date.now());
  }

  listCostChargeTotals(invocationIds: string[]): CostChargeTotal[] {
    if (invocationIds.length === 0) return [];
    const rows = this.db
      .prepare(
        `SELECT invocation_id, model, currency, SUM(amount) AS amount FROM cost_charges WHERE invocation_id IN (${invocationIds.map(() => "?").join(",")}) GROUP BY invocation_id, model, currency`,
      )
      .all(...invocationIds) as Record<string, unknown>[];
    return rows.map((r) => ({
      invocationId: String(r.invocation_id),
      model: r.model == null ? null : String(r.model),
      currency: String(r.currency),
      amount: Number(r.amount),
    }));
  }

  // ---- Worktrees & Merge Queue ----
  recordWorktree(input: {
    runId: string;
    taskId: string;
    path: string;
    branch: string;
    baseSha: string;
    creationIdentityJson?: string | null;
  }): WorktreeRecord {
    const id = nextFriendlyId(this.db, "wt", "worktrees");
    const now = Date.now();
    this.db
      .prepare(
        "INSERT INTO worktrees(id, run_id, task_id, path, branch, base_sha, status, created_at, creation_identity_json) VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?)",
      )
      .run(id, input.runId, input.taskId, input.path, input.branch, input.baseSha, now, input.creationIdentityJson ?? null);
    this.updateTask(input.taskId, { worktreeId: id });
    const wt = this.getWorktree(id)!;
    this.emitEvent(input.runId, input.taskId, "worktree.created", wt);
    return wt;
  }

  getWorktree(id: string): WorktreeRecord | null {
    const r = this.db.prepare("SELECT * FROM worktrees WHERE id = ?").get(id) as Record<string, unknown> | undefined;
    if (!r) return null;
    return {
      id: String(r.id),
      runId: String(r.run_id),
      taskId: String(r.task_id),
      path: String(r.path),
      branch: String(r.branch),
      baseSha: String(r.base_sha),
      status: String(r.status) as WorktreeRecord["status"],
      archiveRef: r.archive_ref ? String(r.archive_ref) : null,
      createdAt: Number(r.created_at),
      cleanedAt: r.cleaned_at == null ? null : Number(r.cleaned_at),
      creationIdentityJson: r.creation_identity_json == null ? null : String(r.creation_identity_json),
    };
  }

  listWorktrees(runId?: string): WorktreeRecord[] {
    const rows = runId
      ? (this.db.prepare("SELECT * FROM worktrees WHERE run_id = ? ORDER BY created_at DESC").all(runId) as Record<string, unknown>[])
      : (this.db.prepare("SELECT * FROM worktrees ORDER BY created_at DESC").all() as Record<string, unknown>[]);
    return rows.map((r) => this.getWorktree(String(r.id))!);
  }

  updateWorktree(id: string, patch: Partial<{ status: WorktreeRecord["status"]; archiveRef: string | null; cleanedAt: number | null }>): void {
    const sets: string[] = [];
    const values: (string | number | null)[] = [];
    if (patch.status !== undefined) {
      sets.push("status = ?");
      values.push(patch.status);
    }
    if (patch.archiveRef !== undefined) {
      sets.push("archive_ref = ?");
      values.push(patch.archiveRef);
    }
    if (patch.cleanedAt !== undefined) {
      sets.push("cleaned_at = ?");
      values.push(patch.cleanedAt);
    }
    if (sets.length === 0) return;
    values.push(id);
    this.db.prepare(`UPDATE worktrees SET ${sets.join(", ")} WHERE id = ?`).run(...values);
  }

  enqueueMerge(input: { runId: string; taskId: string; worktreeId: string; branch: string }): MergeQueueRecord {
    const id = nextFriendlyId(this.db, "mq", "merge_queue");
    const now = Date.now();
    this.db
      .prepare(
        "INSERT INTO merge_queue(id, run_id, task_id, worktree_id, branch, status, created_at) VALUES (?, ?, ?, ?, ?, 'queued', ?)",
      )
      .run(id, input.runId, input.taskId, input.worktreeId, input.branch, now);
    this.updateWorktree(input.worktreeId, { status: "queued" });
    this.updateTask(input.taskId, { status: "merging" });
    const rec = this.getMergeQueueItem(id)!;
    this.emitEvent(input.runId, input.taskId, "merge.queued", rec);
    return rec;
  }

  getMergeQueueItem(id: string): MergeQueueRecord | null {
    const r = this.db.prepare("SELECT * FROM merge_queue WHERE id = ?").get(id) as Record<string, unknown> | undefined;
    if (!r) return null;
    return {
      id: String(r.id),
      runId: String(r.run_id),
      taskId: String(r.task_id),
      worktreeId: String(r.worktree_id),
      branch: String(r.branch),
      status: String(r.status) as MergeQueueRecord["status"],
      log: r.log ? String(r.log) : null,
      createdAt: Number(r.created_at),
      finishedAt: r.finished_at == null ? null : Number(r.finished_at),
    };
  }

  listPendingMerges(runId: string): MergeQueueRecord[] {
    const rows = this.db
      .prepare("SELECT * FROM merge_queue WHERE run_id = ? AND status = 'queued' ORDER BY created_at ASC")
      .all(runId) as Record<string, unknown>[];
    return rows.map((r) => this.getMergeQueueItem(String(r.id))!);
  }

  finishMergeItem(id: string, status: Exclude<MergeQueueRecord["status"], "queued" | "running">, log: string): MergeQueueRecord {
    const now = Date.now();
    this.db
      .prepare("UPDATE merge_queue SET status = ?, log = ?, finished_at = ? WHERE id = ?")
      .run(status, log.slice(-16000), now, id);
    const rec = this.getMergeQueueItem(id)!;
    this.emitEvent(rec.runId, rec.taskId, "merge.finished", { id, status, branch: rec.branch });
    return rec;
  }

  // ---- Cross-Run Coordination: Versioned Contracts, Advisories & Deadlock-Safe Requests ----
  listActiveWriteScopes(runId: string, taskId?: string): string[] {
    const query = "SELECT glob_pattern FROM write_leases WHERE run_id = ? AND active = 1";
    const rows = (taskId
      ? this.db.prepare(query + " AND task_id = ?").all(runId, taskId)
      : this.db.prepare(query).all(runId)) as Array<{ glob_pattern: string }>;
    return rows.map((r) => r.glob_pattern);
  }

  /** Returns ALL assigned write scopes ever stored for a task (active or deactivated), for merge-time enforcement. */
  listAssignedWriteScopes(runId: string, taskId: string): string[] {
    const rows = this.db
      .prepare("SELECT DISTINCT glob_pattern FROM write_leases WHERE run_id = ? AND task_id = ?")
      .all(runId, taskId) as Array<{ glob_pattern: string }>;
    return rows.map((r) => r.glob_pattern);
  }

  /**
   * Atomically validates and re-activates a task's assigned write leases before launching or resuming it
   * (with `dryRun`, only validates). Throws a clear conflict error if another active task on `(repo_root, target_branch)` now owns an overlapping scope.
   */
  reacquireWriteLeasesForTask(runId: string, taskId: string, opts?: { dryRun?: boolean }): string[] {
    return this.transaction(() => {
      const run = this.getRun(runId);
      if (!run) throw new Error(`Unknown run ${runId}`);
      const scopes = this.listAssignedWriteScopes(runId, taskId);
      if (scopes.length === 0) return [];

      this.purgeNonLiveWriteLeases(run.repoRoot);
      const activeLeases = this.db
        .prepare("SELECT run_id, target_branch, task_id, glob_pattern FROM write_leases WHERE repo_root = ? AND active = 1 AND task_id != ?")
        .all(run.repoRoot, taskId) as Array<{ run_id: string; target_branch: string; task_id: string; glob_pattern: string }>;
      for (const requested of scopes) {
        for (const existing of activeLeases) {
          if (existing.target_branch !== run.baseBranch) continue;
          if (!globsOverlap(requested, existing.glob_pattern)) continue;
          throw new WriteScopeConflictError(
            `Write-scope conflict re-acquiring lease on (${run.repoRoot}:${run.baseBranch}): '${requested}' overlaps active lease '${existing.glob_pattern}' held by task ${existing.task_id} (run ${existing.run_id})`,
            requested,
            [{ runId: existing.run_id, taskId: existing.task_id, glob: existing.glob_pattern }],
          );
        }
      }
      // A dry run only checks for conflicts (fast-fail before slow setup); nothing is acquired.
      if (!opts?.dryRun) this.db.prepare("UPDATE write_leases SET active = 1 WHERE run_id = ? AND task_id = ?").run(runId, taskId);
      return scopes;
    });
  }

  publishContract(input: {
    namespace: string;
    key: string;
    ownerRunId: string;
    payload: unknown;
    summary: string;
    status?: "draft" | "frozen" | "deprecated";
    expectedVersion?: number;
  }): SharedContractRecord {
    return this.transaction(() => {
      const latest = this.db
        .prepare("SELECT * FROM shared_contracts WHERE namespace = ? AND key = ? ORDER BY version DESC LIMIT 1")
        .get(input.namespace, input.key) as Record<string, unknown> | undefined;

      if (latest) {
        if (String(latest.owner_run_id) !== input.ownerRunId) {
          throw new Error(
            `Contract '${input.namespace}/${input.key}' is owned by run ${latest.owner_run_id}; run ${input.ownerRunId} cannot overwrite it`,
          );
        }
        if (String(latest.status) === "frozen" && input.status !== "deprecated") {
          throw new Error(
            `Contract '${input.namespace}/${input.key}' v${latest.version} is FROZEN. Publish under a new key or request human unlock.`,
          );
        }
        const curVer = Number(latest.version);
        if (input.expectedVersion !== undefined && input.expectedVersion !== curVer) {
          throw new Error(`Optimistic concurrency conflict: expected v${input.expectedVersion}, actual v${curVer}`);
        }
      } else if (input.expectedVersion !== undefined && input.expectedVersion !== 0) {
        throw new Error(`Optimistic concurrency conflict: expected v${input.expectedVersion}, contract does not exist`);
      }

      const nextVersion = latest ? Number(latest.version) + 1 : 1;
      const payloadJson = JSON.stringify(input.payload);
      const schemaHash = createHash("sha256").update(payloadJson).digest("hex").slice(0, 16);
      const now = Date.now();
      const status = input.status ?? "draft";

      this.db
        .prepare(
          `INSERT INTO shared_contracts(namespace, key, version, owner_run_id, status, schema_hash, payload_json, summary, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(input.namespace, input.key, nextVersion, input.ownerRunId, status, schemaHash, payloadJson, input.summary.slice(0, 240), now);

      const rec: SharedContractRecord = {
        namespace: input.namespace,
        key: input.key,
        version: nextVersion,
        ownerRunId: input.ownerRunId,
        status,
        schemaHash,
        payload: input.payload,
        summary: input.summary.slice(0, 240),
        createdAt: now,
      };
      this.emitEvent(input.ownerRunId, null, "contract.published", {
        namespace: rec.namespace,
        key: rec.key,
        version: rec.version,
        status: rec.status,
        schemaHash: rec.schemaHash,
      });
      return rec;
    });
  }

  getContract(namespace: string, key: string, version?: number): SharedContractRecord | null {
    const r = (
      version !== undefined
        ? this.db
            .prepare("SELECT * FROM shared_contracts WHERE namespace = ? AND key = ? AND version = ?")
            .get(namespace, key, version)
        : this.db
            .prepare("SELECT * FROM shared_contracts WHERE namespace = ? AND key = ? ORDER BY version DESC LIMIT 1")
            .get(namespace, key)
    ) as Record<string, unknown> | undefined;
    if (!r) return null;
    return {
      namespace: String(r.namespace),
      key: String(r.key),
      version: Number(r.version),
      ownerRunId: String(r.owner_run_id),
      status: String(r.status) as SharedContractRecord["status"],
      schemaHash: String(r.schema_hash),
      payload: JSON.parse(String(r.payload_json)),
      summary: String(r.summary),
      createdAt: Number(r.created_at),
    };
  }

  sendPeerAdvisory(fromRunId: string, toRunId: string, topicKey: string, message: string): PeerExchangeRecord {
    const target = this.getRun(toRunId);
    if (!target) throw new Error(`Unknown target run ${toRunId}`);
    const id = nextFriendlyId(this.db, "padv", "peer_exchanges");
    const now = Date.now();
    this.db
      .prepare(
        `INSERT INTO peer_exchanges(id, from_run_id, to_run_id, kind, topic_key, round, message, status, created_at)
         VALUES (?, ?, ?, 'advisory', ?, 1, ?, 'delivered', ?)`,
      )
      .run(id, fromRunId, toRunId, topicKey, message, now);
    const rec: PeerExchangeRecord = {
      id,
      fromRunId,
      toRunId,
      kind: "advisory",
      topicKey,
      round: 1,
      message,
      reply: null,
      status: "delivered",
      createdAt: now,
      answeredAt: null,
    };
    this.emitEvent(toRunId, null, "peer.advisory", rec);
    return rec;
  }

  /**
   * Bounded Tier-1-to-Tier-1 cross-run request with:
   *  1. Coordination eligibility check (must share coordination_group OR same repo with coordinated mode)
   *  2. Hard round cap (`maxRounds = 2` per `(run_pair, topicKey)`)
   *  3. Transactional DFS cycle check over `run_wait_edges` (`ERR_CROSS_RUN_DEADLOCK`)
   */
  createPeerRequest(input: {
    fromRunId: string;
    toRunId: string;
    topicKey: string;
    message: string;
    timeoutSec?: number;
    maxRounds?: number;
  }): PeerExchangeRecord {
    return this.transaction(() => {
      const from = this.getRun(input.fromRunId);
      const to = this.getRun(input.toRunId);
      if (!from || !to) throw new Error("Source or target run not found");
      if (from.id === to.id) throw new Error("Cannot send cross-run peer_request to self");
      if (to.status !== "active") throw new Error(`Target run ${to.id} is not active (status=${to.status})`);

      const sameGroup = Boolean(from.coordinationGroup && from.coordinationGroup === to.coordinationGroup);
      const coordinatedSameRepo =
        from.repoRoot === to.repoRoot && from.isolationMode === "coordinated" && to.isolationMode === "coordinated";
      if (!sameGroup && !coordinatedSameRepo) {
        throw new Error(
          `Cross-run request denied: runs ${from.id} and ${to.id} are isolated (not in same coordination_group or coordinated same-repo mode). Use non-blocking peer_notify instead.`,
        );
      }

      const maxRounds = input.maxRounds ?? 2;
      const pairCount = this.db
        .prepare(
          `SELECT COUNT(*) AS cnt FROM peer_exchanges
           WHERE kind = 'request' AND topic_key = ?
             AND ((from_run_id = ? AND to_run_id = ?) OR (from_run_id = ? AND to_run_id = ?))`,
        )
        .get(input.topicKey, from.id, to.id, to.id, from.id) as { cnt: number };
      const nextRound = Number(pairCount.cnt) + 1;
      if (nextRound > maxRounds) {
        throw new Error(
          `ERR_CROSS_RUN_MAX_ROUNDS: topic '${input.topicKey}' between ${from.id} and ${to.id} exceeded max_rounds=${maxRounds}. Escalate to human P0.`,
        );
      }

      // Prune expired wait edges and run DFS from `to.id` to detect any cycle reaching `from.id`
      const now = Date.now();
      this.db.prepare("DELETE FROM run_wait_edges WHERE expires_at < ?").run(now);
      const edges = this.db.prepare("SELECT waiter_run_id, target_run_id FROM run_wait_edges").all() as Array<{
        waiter_run_id: string;
        target_run_id: string;
      }>;
      const adj = new Map<string, string[]>();
      for (const e of edges) {
        const list = adj.get(e.waiter_run_id) ?? [];
        list.push(e.target_run_id);
        adj.set(e.waiter_run_id, list);
      }

      const visited = new Set<string>();
      const path: string[] = [from.id];
      const dfs = (cur: string): boolean => {
        path.push(cur);
        if (cur === from.id) return true;
        visited.add(cur);
        for (const next of adj.get(cur) ?? []) {
          if (next === from.id) {
            path.push(next);
            return true;
          }
          if (!visited.has(next) && dfs(next)) return true;
        }
        path.pop();
        return false;
      };

      if (dfs(to.id)) {
        throw new Error(`ERR_CROSS_RUN_DEADLOCK: wait cycle detected (${path.join(" -> ")})`);
      }

      const id = nextFriendlyId(this.db, "preq", "peer_exchanges");
      const ttlMs = Math.min(input.timeoutSec ?? 120, 300) * 1000;
      this.db
        .prepare(
          `INSERT INTO peer_exchanges(id, from_run_id, to_run_id, kind, topic_key, round, message, status, created_at)
           VALUES (?, ?, ?, 'request', ?, ?, ?, 'open', ?)`,
        )
        .run(id, from.id, to.id, input.topicKey, nextRound, input.message, now);

      this.db
        .prepare("INSERT INTO run_wait_edges(waiter_run_id, target_run_id, reason, expires_at, created_at) VALUES (?, ?, ?, ?, ?)")
        .run(from.id, to.id, `peer_request:${id}`, now + ttlMs, now);

      const rec: PeerExchangeRecord = {
        id,
        fromRunId: from.id,
        toRunId: to.id,
        kind: "request",
        topicKey: input.topicKey,
        round: nextRound,
        message: input.message,
        reply: null,
        status: "open",
        createdAt: now,
        answeredAt: null,
      };
      this.emitEvent(to.id, null, "peer.request", rec);
      return rec;
    });
  }

  replyPeerRequest(requestId: string, responderRunId: string, reply: string): PeerExchangeRecord {
    return this.transaction(() => {
      const r = this.db.prepare("SELECT * FROM peer_exchanges WHERE id = ?").get(requestId) as Record<string, unknown> | undefined;
      if (!r) throw new Error(`Unknown peer request ${requestId}`);
      if (String(r.to_run_id) !== responderRunId) {
        throw new Error(`Run ${responderRunId} cannot reply to request directed at ${r.to_run_id}`);
      }
      const now = Date.now();
      this.db
        .prepare("UPDATE peer_exchanges SET reply = ?, status = 'answered', answered_at = ? WHERE id = ?")
        .run(reply, now, requestId);
      this.db.prepare("DELETE FROM run_wait_edges WHERE reason = ?").run(`peer_request:${requestId}`);

      const rec: PeerExchangeRecord = {
        id: requestId,
        fromRunId: String(r.from_run_id),
        toRunId: String(r.to_run_id),
        kind: "request",
        topicKey: String(r.topic_key),
        round: Number(r.round),
        message: String(r.message),
        reply,
        status: "answered",
        createdAt: Number(r.created_at),
        answeredAt: now,
      };
      this.emitEvent(rec.fromRunId, null, "peer.replied", rec);
      return rec;
    });
  }
}
