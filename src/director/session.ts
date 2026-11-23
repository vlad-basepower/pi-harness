import { realpathSync } from "node:fs";
import { resolve } from "node:path";
import type { Database } from "bun:sqlite";
import { codeModePromptSection, type CodeMode } from "../code-mode/index.js";
import { friendlyIdCandidates, nextFriendlyId } from "../control-plane/ids.js";
import { parseChatControlEvent, type ChatControlEvent } from "../protocol/chat-events.js";
import type { ToolDisplayPayload } from "../protocol/tool-display.js";

/** Director tools that stay directly callable in collapsed Code Mode. */
export const DIRECTOR_DIRECT_TOOLS = ["read_file"] as const;

/** Leaves Director read receipts out of the prompt window's system turns. */
const EXCLUDE_READ_RECEIPTS_SQL =
  "AND (event_json IS NULL OR NOT json_valid(event_json) OR json_extract(event_json, '$.kind') IS NOT 'director.read_file')";

function canonicalRepoPath(p: string): string {
  try {
    return realpathSync(p);
  } catch {
    return resolve(p);
  }
}

export interface ChatSessionRecord {
  id: string;
  projectId: string | null;
  repoRoot: string;
  title: string;
  profileId: string;
  model: string | null;
  effort: string | null;
  acpSessionId: string | null;
  modelSelectionContext: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface ChatTurnRecord {
  seq: number;
  chatSessionId: string;
  role: "user" | "assistant" | "system_notice" | "tool_output";
  /** Short plain-text summary for tool_output / event turns, or message text for user/assistant turns. */
  content: string;
  createdAt: number;
  /** Only set on tool_output turns. */
  toolName?: string | null;
  /** Only set on tool_output turns. */
  display?: ToolDisplayPayload | null;
  /** Typed control-plane event payload (rendered distinctly in the TUI without string-prefix parsing). */
  event?: ChatControlEvent | null;
}

export const DIRECTOR_SCHEMA = `
CREATE TABLE IF NOT EXISTS chat_sessions (
  id TEXT PRIMARY KEY,
  project_id TEXT,
  repo_root TEXT NOT NULL,
  title TEXT NOT NULL,
  profile_id TEXT NOT NULL,
  model TEXT,
  effort TEXT,
  acp_session_id TEXT,
  model_selection_context TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS chat_turns (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  chat_session_id TEXT NOT NULL REFERENCES chat_sessions(id),
  role TEXT NOT NULL CHECK (role IN ('user','assistant','system_notice','tool_output')),
  content TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  tool_name TEXT,
  display_json TEXT,
  event_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_chat_turns_session ON chat_turns(chat_session_id, seq);
`;

export function buildDirectorSystemPrompt(
  mode: CodeMode = "off",
  codeExecOpts?: { defaultTimeoutMs?: number; maxTimeoutMs?: number },
): string {
  const codeMode = codeModePromptSection(mode, { ...codeExecOpts, directTools: DIRECTOR_DIRECT_TOOLS });
  const largeFiles = codeMode
    ? 'For a large file, find the relevant part first: in a code_exec script, loop read_file from "harness" over the file and output only the matching lines. Direct node:fs or open() calls inside code_exec fail with EPERM.'
    : "For a large file, read the range you need, not the whole file.";
  return [
    "You are the Tier 0 INTERACTIVE DIRECTOR of pi-harness — the user's persistent engineering partner.",
    "You sit ABOVE the autonomous Tier 1 Run Orchestrators so the user always has an instant, responsive chat.",
    "",
    "YOUR RESPONSIBILITIES:",
    "1. Brainstorm designs, discuss tradeoffs, explore requirements, and answer progress questions immediately using `goal_list`, `goal_get`, `list_models`, and `kb_search`.",
    "Read files directly with read_file whenever their contents help you answer, plan, or check work. It reads workspace files, harness goals, notes, and artifacts, and any other file on this machine that the operator's account can read (use an absolute or ~/ path). No reason or approval is needed. .git internals, task worktrees, and harness-internal state are refused, and the OS may refuse other files; report a refusal instead of working around it.",
    "Read what the request needs, not everything nearby. read_file returns UTF-8 text one page at a time (default 32768 bytes, ending on a line boundary): select lines with start_line and end_line, or continue with offset set to nextOffset until hasMore is false. Size the output to the question, and raise max_bytes only when you need more in a single result. " + largeFiles,
    "Never edit code or run shell commands yourself; code changes go through goals. Delegate lookups that span many files, need codebase search, or need shell commands or tests to a narrow read-only research, plan, or verify worker via spawn_task. Select a concrete model from list_models. Read its summary and sources via wait_tasks; open authorized artifacts on demand. Delegation adds value when you do not know where to look, when the reading would flood your context, or when a conclusion needs independent verification; for one or a few known files a direct read is faster. write_note saves notes and reports under .harness/goals/ or .harness/notes/.",
    "Treat credentials, private keys, tokens, and .env-style files as sensitive: read them only when the user's request requires it, and never copy their contents into chat, notes, goals, or task prompts. If a file you opened for another reason contains secrets, do not repeat them.",
    "2. When the user is ready to execute work, draft a structured **Goal Contract** via `goal_create` (with `criteria`, optional `non_goals`, and `verification_mode`: `auto` | `deterministic` | `none` | `independent_review`), commit it immediately via `goal_commit` for a clear action request so an autonomous Tier 1 Orchestrator executes it cleanly. Scale criteria and verification depth proportionally to task risk: for straightforward operational or low-risk tasks, use lean deterministic checks (or `verification_mode: 'none'` / `'deterministic'`) instead of inventing heavy behavioral/architectural review layers; reserve `independent_review` and multi-layer criteria for non-trivial code or architectural changes.",
    "Each goal has a canonical Goal Evidence Directory, `<repo>/.harness/goals/<goalId>` (returned as `evidenceDir` by goal_create, goal_commit and goal_get). Its run's workers and the Done-Reviewer get it as env `HARNESS_GOAL_EVIDENCE_DIR`; point goal criteria and goal_accept evidence there.",
    "Deterministic criterion commands run in a clean detached checkout of the goal's target branch, not in the main project checkout: untracked and gitignored files are absent there. Write them against committed relative paths, and use `$HARNESS_REPO_ROOT` for files that exist only in the main checkout.",
    "Use sensible defaults. Ask only when a genuinely ambiguous or irreversible decision blocks action. Create separate goals for distinct requests or amend the relevant active goal — prefer cohesive vertical goals and avoid spawning many overlapping concurrent goals that edit the same files.",
    "3. When the user changes requirements mid-flight, call `goal_amend` so a clean `vN -> vN+1` amendment delta propagates to the running Tier 1 Orchestrator without losing unaffected work. To add instructions pass `append_description` (the existing description is kept); `description` replaces the whole description, and a shorter replacement needs `confirm_replace_description: true`. `update_criteria` merges by `id`. Check `delivery` in the result to confirm the amendment reached the run.",
    "4. Surface `P0` escalations, circuit-breaker trips, and Independent Done-Review verdicts clearly to the user with actionable options. When a task or run fails or crashes (for example after a daemon restart), prefer `resume_task` or `resume_run` to continue in-place from preserved worktrees and handoffs instead of spawning duplicate tasks or goals. Delegated tasks are never stopped by a turn count: they keep working until they are ready to return (they may get an advisory progress reminder), so wait for their results instead of re-delegating. Explicit stops stay stopped: a status question never requires a resume, and steering does not restart stopped work. When the user asks to continue stopped, paused or failed work, call `resume_run`, `resume_task` or `goal_commit` for that exact work; they continue it in place and are safe to repeat, returning the current state without creating a second run when it is already running. A fresh `spawn_task` works in a new turn even after earlier work in this chat was stopped, and never restarts the stopped tasks.",
    "   Escalations stay open after a run ends until someone disposes of them. Amending a goal supersedes only its `verification_failed` escalations; close any other escalation you have handled with `resolve_escalation({ escalation_id, reason })`, and never resolve one only to clear the count.",
    "5. Keep responses crisp, direct, and grounded in actual tool state — never guess progress or invent statuses. Drive by events instead of token polling: dispatch all independent goals, amendments, or lookups requested in the user's message, then report what started and end your turn so the user stays in an instant, unblocked chat — the daemon automatically wakes you when Goals settle or escalate. Do NOT call `goal_wait` after committing a goal unless the user explicitly asks you to block until completion, and never poll `goal_get` in a loop.",
    "6. Whenever the user states per-session model, quota pacing, or routing preferences (for example, using a specific model for a kind of task, utilizing a particular model family more, or pacing usage around reset windows), call `set_model_selection_context` to store concise session-scoped guidance (empty by default; pass an empty string to clear). By default, work in the most productive manner without artificially throttling for limits; when the user or `sessionModelContext` instructs pacing around usage/reset windows (or you want a scheduled follow-up), use `schedule_timer` (non-blocking, durable across restarts, supports `delay_seconds` or ISO `wake_at`, optional `pause_goal`/`resume_goal` and early `wake_on_event`) and end your turn.",
    "7. Cross-project and remote-host collaboration uses one unified session messaging primitive: `remote_list`, `session_list`, `session_create`, `session_send`, and `session_read`. Target any local session (`chat-2` or `local:chat-2`) or peered remote host session (`gpu-box:chat-4`). `session_send` wakes the recipient automatically (or queues durably if offline); when answering a peer message and no further reply is needed, set `expect_reply: false` to avoid reply loops. `project_remove` is different: it only unregisters a project from this host's local project registry and leaves its files, chat history, goals and runs untouched.",
    "",
    "RICH TOOL OUTPUT:",
    "- Goal and run operations return structured data. Use it directly for internal checks. Card-capable results include a presentation_id.",
    "- When a native card or table answers the user's request, call `present_result` once with the selected result's presentation_id. For a batch, choose the result to show and pass its ID in the same turn. Keep any accompanying commentary brief. Never duplicate a rendered table or card in prose.",
    ...(codeMode ? ["", codeMode] : []),
  ].join("\n");
}

/**
 * Persistent Tier 0 Interactive Director chat store (survives terminal close & remote reattach in `harnessd`).
 */
export class DirectorSessionStore {
  constructor(private readonly db: Database) {
    this.db.exec(DIRECTOR_SCHEMA);
    this.migrateChatSessions();
    this.migrateChatTurns();
  }

  /** Safely upgrades existing chat_sessions tables created before model_selection_context existed. */
  private migrateChatSessions(): void {
    const cols = new Set(
      (this.db.prepare("PRAGMA table_info(chat_sessions)").all() as Array<{ name: string }>).map((c) => c.name),
    );
    if (!cols.has("model_selection_context")) {
      this.db.exec("ALTER TABLE chat_sessions ADD COLUMN model_selection_context TEXT");
    }
    if (!cols.has("project_id")) {
      this.db.exec("ALTER TABLE chat_sessions ADD COLUMN project_id TEXT");
    }
  }

  /** Upgrades chat_turns created before tool_output turns existed: rebuilds it when its role CHECK lacks 'tool_output' (keeping rows and seq), then adds missing columns. */
  private migrateChatTurns(): void {
    const table = this.db
      .prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'chat_turns'")
      .get() as { sql: string } | undefined;
    if (table && !table.sql.includes("'tool_output'")) {
      const cols = new Set((this.db.prepare("PRAGMA table_info(chat_turns)").all() as Array<{ name: string }>).map((c) => c.name));
      const copied = ["seq", "chat_session_id", "role", "content", "created_at", "tool_name", "display_json"].filter((c) => cols.has(c));
      this.db.transaction(() => {
        this.db.exec(`
          CREATE TABLE chat_turns_migrated (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_session_id TEXT NOT NULL REFERENCES chat_sessions(id),
            role TEXT NOT NULL CHECK (role IN ('user','assistant','system_notice','tool_output')),
            content TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            tool_name TEXT,
            display_json TEXT
          );
          INSERT INTO chat_turns_migrated(${copied.join(", ")}) SELECT ${copied.join(", ")} FROM chat_turns;
          DELETE FROM sqlite_sequence WHERE name = 'chat_turns_migrated';
          INSERT INTO sqlite_sequence(name, seq) SELECT 'chat_turns_migrated', seq FROM sqlite_sequence WHERE name = 'chat_turns';
          DROP TABLE chat_turns;
          ALTER TABLE chat_turns_migrated RENAME TO chat_turns;
          CREATE INDEX IF NOT EXISTS idx_chat_turns_session ON chat_turns(chat_session_id, seq);
        `);
      })();
    }
    const cols = new Set((this.db.prepare("PRAGMA table_info(chat_turns)").all() as Array<{ name: string }>).map((c) => c.name));
    if (!cols.has("tool_name")) this.db.exec("ALTER TABLE chat_turns ADD COLUMN tool_name TEXT");
    if (!cols.has("display_json")) this.db.exec("ALTER TABLE chat_turns ADD COLUMN display_json TEXT");
    if (!cols.has("event_json")) this.db.exec("ALTER TABLE chat_turns ADD COLUMN event_json TEXT");
  }

  private lastTs = 0;

  private nextTimestamp(): number {
    const now = Date.now();
    this.lastTs = now > this.lastTs ? now : this.lastTs + 1;
    return this.lastTs;
  }

  getOrCreateDefault(input: {
    repoRoot: string;
    projectId?: string | null;
    profileId: string;
    model?: string | null;
    effort?: string | null;
  }): ChatSessionRecord {
    const canon = canonicalRepoPath(input.repoRoot);
    const existing =
      (input.projectId ? this.getProjectSession(input.projectId) : null) ??
      this.getWorkspaceSession(canon);
    if (existing) {
      if (input.projectId && existing.projectId !== input.projectId) {
        return this.updateSession(existing.id, { projectId: input.projectId, repoRoot: canon });
      }
      return existing;
    }

    return this.createSession({ ...input, repoRoot: canon });
  }

  createSession(input: {
    repoRoot: string;
    projectId?: string | null;
    profileId: string;
    title?: string;
    model?: string | null;
    effort?: string | null;
  }): ChatSessionRecord {
    const canon = canonicalRepoPath(input.repoRoot);
    const id = nextFriendlyId(this.db, "chat", "chat_sessions");
    const now = this.nextTimestamp();
    this.db
      .prepare(
        "INSERT INTO chat_sessions(id, project_id, repo_root, title, profile_id, model, effort, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
      )
      .run(
        id,
        input.projectId ?? null,
        canon,
        input.title?.trim() || "Director Chat",
        input.profileId,
        input.model ?? null,
        input.effort ?? null,
        now,
        now,
      );
    return this.getSession(id)!;
  }

  touchSession(id: string): void {
    this.db.prepare("UPDATE chat_sessions SET updated_at = ? WHERE id = ?").run(this.nextTimestamp(), id);
  }

  getProjectSession(projectId: string): ChatSessionRecord | null {
    const row = this.db
      .prepare("SELECT * FROM chat_sessions WHERE project_id = ? ORDER BY updated_at DESC, rowid DESC LIMIT 1")
      .get(projectId) as Record<string, unknown> | undefined;
    return row ? this.mapSession(row) : null;
  }

  getWorkspaceSession(repoRoot: string): ChatSessionRecord | null {
    const row = this.db
      .prepare("SELECT * FROM chat_sessions WHERE repo_root = ? ORDER BY updated_at DESC, rowid DESC LIMIT 1")
      .get(canonicalRepoPath(repoRoot)) as Record<string, unknown> | undefined;
    return row ? this.mapSession(row) : null;
  }

  getMostRecentSession(): ChatSessionRecord | null {
    const r = this.db
      .prepare("SELECT * FROM chat_sessions ORDER BY updated_at DESC, rowid DESC LIMIT 1")
      .get() as Record<string, unknown> | undefined;
    return r ? this.mapSession(r) : null;
  }

  getSession(id: string): ChatSessionRecord | null {
    for (const candidate of friendlyIdCandidates(id, "chat")) {
      const r = this.db.prepare("SELECT * FROM chat_sessions WHERE id = ?").get(candidate) as Record<string, unknown> | undefined;
      if (r) return this.mapSession(r);
    }
    return null;
  }

  listSessions(limit = 20): ChatSessionRecord[] {
    const rows = this.db
      .prepare("SELECT * FROM chat_sessions ORDER BY updated_at DESC LIMIT ?")
      .all(limit) as Record<string, unknown>[];
    return rows.map((r) => this.mapSession(r));
  }

  appendTurn(
    chatSessionId: string,
    role: ChatTurnRecord["role"],
    content: string,
    tool?: { toolName: string; display: ToolDisplayPayload | null },
    event?: ChatControlEvent | null,
  ): ChatTurnRecord {
    const now = Date.now();
    const res = this.db
      .prepare(
        "INSERT INTO chat_turns(chat_session_id, role, content, created_at, tool_name, display_json, event_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
      )
      .run(
        chatSessionId,
        role,
        content,
        now,
        tool?.toolName ?? null,
        tool?.display ? JSON.stringify(tool.display) : null,
        event ? JSON.stringify(event) : null,
      );
    this.db.prepare("UPDATE chat_sessions SET updated_at = ? WHERE id = ?").run(now, chatSessionId);
    return {
      seq: Number(res.lastInsertRowid),
      chatSessionId,
      role,
      content,
      createdAt: now,
      ...(tool ? { toolName: tool.toolName, display: tool.display } : {}),
      ...(event ? { event } : {}),
    };
  }

  updateSession(
    chatSessionId: string,
    patch: Partial<{
      projectId: string | null;
      repoRoot: string;
      profileId: string;
      model: string | null;
      effort: string | null;
      title: string;
      modelSelectionContext: string | null;
      resetAcpSession: boolean;
    }>,
  ): ChatSessionRecord {
    const existing = this.getSession(chatSessionId);
    if (!existing) throw new Error(`Unknown chat session ${chatSessionId}`);
    const now = this.nextTimestamp();
    const nextModelContext =
      patch.modelSelectionContext !== undefined
        ? typeof patch.modelSelectionContext === "string" && patch.modelSelectionContext.trim()
          ? patch.modelSelectionContext.trim()
          : null
        : existing.modelSelectionContext;
    const nextRepoRoot = patch.repoRoot ? canonicalRepoPath(patch.repoRoot) : existing.repoRoot;
    const nextProjectId = patch.projectId !== undefined ? patch.projectId : existing.projectId;
    const nextAcpSessionId = patch.resetAcpSession ? null : existing.acpSessionId;
    this.db
      .prepare(
        "UPDATE chat_sessions SET project_id = ?, repo_root = ?, profile_id = ?, model = ?, effort = ?, title = ?, acp_session_id = ?, model_selection_context = ?, updated_at = ? WHERE id = ?",
      )
      .run(
        nextProjectId,
        nextRepoRoot,
        patch.profileId ?? existing.profileId,
        patch.model !== undefined ? patch.model : existing.model,
        patch.effort !== undefined ? patch.effort : existing.effort,
        patch.title ?? existing.title,
        nextAcpSessionId,
        nextModelContext,
        now,
        existing.id,
      );
    return this.getSession(existing.id)!;
  }

  getModelSelectionContext(chatSessionId: string): string | null {
    return this.getSession(chatSessionId)?.modelSelectionContext ?? null;
  }

  getHistory(chatSessionId: string, sinceSeq = 0, limit = 100): ChatTurnRecord[] {
    const rows = this.db
      .prepare("SELECT * FROM chat_turns WHERE chat_session_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?")
      .all(chatSessionId, sinceSeq, limit) as Record<string, unknown>[];
    return this.mapTurns(rows);
  }

  getRecentHistory(chatSessionId: string, limit = 100): ChatTurnRecord[] {
    const rows = this.db
      .prepare("SELECT * FROM chat_turns WHERE chat_session_id = ? ORDER BY seq DESC LIMIT ?")
      .all(chatSessionId, limit) as Record<string, unknown>[];
    return this.mapTurns(rows.reverse());
  }

  /**
   * Builds a balanced prompt-context history window: up to `conversationLimit` most-recent
   * `user`/`assistant` turns PLUS up to `systemLimit` most-recent `system_notice`/`tool_output`
   * turns, interleaved in chronological `seq` order so heavy tool/event bursts never evict
   * user requirements or earlier steering. Director read receipts are operator-facing audit
   * records and are left out of the system window so they cannot displace goal or timer events.
   */
  getPromptHistory(
    chatSessionId: string,
    opts?: { conversationLimit?: number; systemLimit?: number; excludeSeq?: number },
  ): ChatTurnRecord[] {
    const convLimit = opts?.conversationLimit ?? 24;
    const sysLimit = opts?.systemLimit ?? 16;
    const excludeSeq = opts?.excludeSeq;
    const convRows = (
      excludeSeq !== undefined
        ? this.db
            .prepare(
              "SELECT * FROM chat_turns WHERE chat_session_id = ? AND role IN ('user', 'assistant') AND seq != ? ORDER BY seq DESC LIMIT ?",
            )
            .all(chatSessionId, excludeSeq, convLimit)
        : this.db
            .prepare(
              "SELECT * FROM chat_turns WHERE chat_session_id = ? AND role IN ('user', 'assistant') ORDER BY seq DESC LIMIT ?",
            )
            .all(chatSessionId, convLimit)
    ) as Record<string, unknown>[];
    const sysRows = (
      excludeSeq !== undefined
        ? this.db
            .prepare(
              `SELECT * FROM chat_turns WHERE chat_session_id = ? AND role IN ('system_notice', 'tool_output') AND seq != ? ${EXCLUDE_READ_RECEIPTS_SQL} ORDER BY seq DESC LIMIT ?`,
            )
            .all(chatSessionId, excludeSeq, sysLimit)
        : this.db
            .prepare(
              `SELECT * FROM chat_turns WHERE chat_session_id = ? AND role IN ('system_notice', 'tool_output') ${EXCLUDE_READ_RECEIPTS_SQL} ORDER BY seq DESC LIMIT ?`,
            )
            .all(chatSessionId, sysLimit)
    ) as Record<string, unknown>[];
    const merged = [...this.mapTurns(convRows), ...this.mapTurns(sysRows)];
    merged.sort((a, b) => a.seq - b.seq);
    return merged;
  }

  private mapTurns(rows: Record<string, unknown>[]): ChatTurnRecord[] {
    return rows.map((r) => {
      let parsedEvent: ChatControlEvent | null = null;
      if (typeof r.event_json === "string" && r.event_json) {
        try {
          parsedEvent = parseChatControlEvent(JSON.parse(r.event_json));
        } catch {
          parsedEvent = null;
        }
      }
      return {
        seq: Number(r.seq),
        chatSessionId: String(r.chat_session_id),
        role: String(r.role) as ChatTurnRecord["role"],
        content: String(r.content),
        createdAt: Number(r.created_at),
        ...(r.role === "tool_output"
          ? {
              toolName: r.tool_name ? String(r.tool_name) : null,
              display: r.display_json ? (JSON.parse(String(r.display_json)) as ToolDisplayPayload) : null,
            }
          : {}),
        ...(parsedEvent ? { event: parsedEvent } : {}),
      };
    });
  }

  private mapSession(r: Record<string, unknown>): ChatSessionRecord {
    const rawCtx = typeof r.model_selection_context === "string" ? r.model_selection_context.trim() : "";
    return {
      id: String(r.id),
      projectId: r.project_id ? String(r.project_id) : null,
      repoRoot: String(r.repo_root),
      title: String(r.title),
      profileId: String(r.profile_id),
      model: r.model ? String(r.model) : null,
      effort: r.effort ? String(r.effort) : null,
      acpSessionId: r.acp_session_id ? String(r.acp_session_id) : null,
      modelSelectionContext: rawCtx || null,
      createdAt: Number(r.created_at),
      updatedAt: Number(r.updated_at),
    };
  }
}
