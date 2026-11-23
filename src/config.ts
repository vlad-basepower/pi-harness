import { accessSync, constants, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { delimiter, isAbsolute, join, resolve } from "node:path";
import type { CodeMode } from "./code-mode/sdk.js";

export type AcpAdapterKind = "claude" | "codex" | "gemini" | "generic";

export interface AcpProfileConfig {
  id: string;
  kind: "acp";
  adapter: AcpAdapterKind;
  command: string;
  args?: string[];
  env?: Record<string, string>;
  billing: "subscription" | "api" | "local";
  enabled?: boolean;
  /** Optional model ID override list if user wants to pin/skip probing */
  models?: string[];
  /** Optional extra CLI args appended when running strictly read-only */
  readOnlyArgs?: string[];
}

export interface PiProfileConfig {
  id: string;
  kind: "pi";
  provider: string;
  api?: "openai-completions" | "openai-responses" | "anthropic-messages" | "google-generative-ai";
  baseUrl?: string;
  apiKeyEnv?: string;
  billing: "api" | "subscription" | "local";
  enabled?: boolean;
  models?: string[];
}

export type RunnerProfileConfig = AcpProfileConfig | PiProfileConfig;

export interface QuotaEndpointConfig {
  /** HTTP(S) URL or `unix:///path/to/sock#/quota` endpoint returning ProtoJSON or binary QuotaSnapshot */
  url: string;
  /** Optional HTTP headers (supports `${ENV_VAR}` interpolation) */
  headers?: Record<string, string>;
  /** Poll interval in seconds (default 60) */
  pollIntervalSec?: number;
  /** Request timeout in ms (default 5000) */
  timeoutMs?: number;
  /** Wire format: protojson (default) or protobuf */
  format?: "protojson" | "protobuf";
}

export interface WorktreeConfig {
  /** Base dir relative to repo root (default `.harness/worktrees`) */
  dir?: string;
  /** Durable artifact dossier dir relative to repo root (default `.harness/artifacts`) */
  artifactsDir?: string;
  /** Optional fast bootstrap command run inside newly created worktree */
  setupCommand?: string;
  /** Deterministic CI gate commands run by the bisecting merge queue */
  ciCommands?: string[];
  /** Per-command CI timeout in ms for the merge queue (default 300_000) */
  ciTimeoutMs?: number;
  /** Git author name used for automated merge commits (default `pi-harness`) */
  gitAuthorName?: string;
  /** Git author email used for automated merge commits (default `harness@local`) */
  gitAuthorEmail?: string;
  /** Days to retain safety archive git refs before GC (default 7) */
  archiveRetentionDays?: number;
  /** Max concurrent active Tier 2 worker worktrees (default 4) */
  maxConcurrentWorkers?: number;
  /** Additional host directories readable by read-only agents, reviewers, and workers (e.g. `~/vanolyze-work`). */
  extraReadRoots?: string[];
  /** Additional host directories writable by workers and Director notes (e.g. `~/vanolyze-work`). */
  extraWriteRoots?: string[];
}

export interface GoalsConfig {
  /** Default max review cycles before escalating to human (default 4) */
  defaultMaxReviewCycles: number;
  /** Default max consecutive no-progress review cycles before escalating (default 2) */
  defaultMaxNoProgressCycles: number;
  /** Timeout in ms for each deterministic goal verification shell gate (default 180_000) */
  gateTimeoutMs: number;
  /** Automatically run deterministic gates + independent Done-Review when a Goal's linked run settles completed (default true). */
  autoVerifyOnSettle?: boolean;
  /** Max concurrent in-progress goals per repository (default 4; additional committed goals wait in the admission queue). */
  maxConcurrentGoals?: number;
  /**
   * Absolute path of the shell that runs verification gates with `-c` (default `/bin/sh`).
   * Global `config.json` / `config.update` only; ignored in repo `.pi-harness.json`. Invalid values fail gates closed.
   */
  gateShell?: string;
  /**
   * Extra environment for verification gates (goal criteria, merge-queue CI, worktree setup).
   * Global `config.json` / `config.update` only; ignored in repo `.pi-harness.json`. `PATH` is prepended to the
   * resolved PATH, never a replacement. `HARNESS_*` names are reserved. Invalid values fail gates closed.
   */
  gateEnv?: Record<string, string>;
}

export interface EventsConfig {
  /**
   * Fallback wakeup interval (seconds) for event-driven waits (`wait_children`, `wait_tasks`, `goal_wait`)
   * in case a child agent process hangs or dies without emitting a terminal event.
   * Default is intentionally long (600s = 10m) so agents rely on events and never thrash/token-poll.
   */
  fallbackWakeupIntervalSec: number;
  /** Automatically wake an idle Director chat session when one of its Goal runs settles (default true). */
  wakeDirectorOnGoalSettle: boolean;
}

export type InboxTriageMode = "off" | "report" | "on";
export type InboxRestartRecoveryPolicy = "handoff" | "resume";

export interface InboxTriageConfig {
  /** Rollout gate: "off" (default, zero writes/DDL), "report" (read-only plan), or "on" (active triage). */
  mode: InboxTriageMode;
  /** Hostname authorized to run active inbox triage; must match `os.hostname()` for authority to hold (default null). */
  schedulerHost: string | null;
  /** Policy for daemon_restart casualties: "handoff" (default, single human ask) or "resume" (fenced in-place). */
  restartRecovery: InboxRestartRecoveryPolicy;
  /** Fixed periodic control/action sweep interval in seconds (default 30). */
  sweepIntervalSec: number;
  /** Upper bound in seconds for acknowledging new/backlog items (default 60). */
  ackMaxSec: number;
  /** Upper bound in seconds for detecting lost owners (default 60). */
  lostOwnerDetectSec: number;
  /** Upper bound in seconds for recovery/reassignment and stale intent detection (default 120). */
  recoverMaxSec: number;
  /** Lifetime maximum counted auto-dispatch attempts per cause (default 3). */
  maxAutoAttempts: number;
  /** Retry spacing and final grace window in minutes (default [1, 2, 4]). */
  backoffMin: number[];
  /** Owner lease duration in minutes (default 5). */
  leaseMin: number;
  /** Maximum consecutive lease renewals without cause-specific progress (default 3). */
  maxNoProgressRenewals: number;
  /** Maximum expensive action dispatches per sweep tick (default 2). */
  maxDispatchPerTick: number;
  /** Maximum causes attached to a single rung-B investigation batch (default 20). */
  batchMaxItems: number;
  /** Optional model for rung-B read-only investigation batch tasks; rung B is disabled when unset. */
  batchModel?: string | null;
  /** Optional runner profile for rung-B read-only investigation batch tasks; rung B is disabled when unset. */
  batchProfileId?: string | null;
  /** Optional max turns advisory cap for rung-B batch tasks. */
  batchMaxTurns?: number;
}

export const DEFAULT_INBOX_TRIAGE_CONFIG: Readonly<InboxTriageConfig> = {
  mode: "off",
  schedulerHost: null,
  restartRecovery: "handoff",
  sweepIntervalSec: 30,
  ackMaxSec: 60,
  lostOwnerDetectSec: 60,
  recoverMaxSec: 120,
  maxAutoAttempts: 3,
  backoffMin: [1, 2, 4],
  leaseMin: 5,
  maxNoProgressRenewals: 3,
  maxDispatchPerTick: 2,
  batchMaxItems: 20,
};

export interface RolloverConfig {
  /** Context ratio triggering Tier 1 Orchestrator clean relay (default 0.72) */
  orchestratorThreshold: number;
  /** Context ratio triggering Tier 2 Worker clean worktree rollover (default 0.78) */
  workerThreshold: number;
  /** Advisory progress-reminder threshold in tool-call turns for Tier 1 Orchestrators (default 200); never stops the agent */
  orchestratorMaxTurns: number;
  /** Advisory progress-reminder threshold in tool-call turns for Tier 2 Workers (default 120); never stops the agent */
  workerMaxTurns: number;
  /** Advisory progress-reminder threshold in tool-call turns for Tier 3 Task Agents (default 40); never stops the agent */
  taskMaxTurns: number;
  /** Advisory progress-reminder threshold in tool-call turns for Goal Done-Review agents (default 30); never stops the agent */
  reviewerMaxTurns: number;
  /** Tool output byte threshold above which output is externalized to artifact file (default 2048) */
  externalizeBytes: number;
  /** Hard generation cap per task to prevent unbounded rollover loops (default 12) */
  maxGenerations?: number;
}

export interface CatalogConfig {
  /** Cache TTL in hours (default 24) */
  ttlHours: number;
  /** Optional Artificial Analysis Pro API key env var name (default `ARTIFICIAL_ANALYSIS_API_KEY`) */
  artificialAnalysisKeyEnv?: string;
  /** Allow -preview models to beat older GA versions in same family (default true) */
  allowPreview?: boolean;
  /** Prefer subscription routes over API routes for same canonical model (default true) */
  preferSubscription?: boolean;
  /**
   * User model filter mode:
   * - `"all"` (default): all ranked catalog models enabled except those in `disabledModels`.
   * - `"allowlist"`: only models listed in `enabledModels` are active (used after "Toggle All Off").
   */
  modelFilterMode?: "all" | "allowlist";
  /** Canonical model IDs disabled when `modelFilterMode === "all"`. */
  disabledModels?: string[];
  /** Canonical model IDs enabled when `modelFilterMode === "allowlist"`. */
  enabledModels?: string[];
}

export interface QuotaRetryConfig {
  /**
   * How long to let an ACP/pi runner natively retry 429 / rate-limit errors (e.g. while a
   * subscription pool proxy switches accounts) before stopping the agent cleanly (default 45_000 ms).
   */
  nativeRetryTimeoutMs: number;
  /**
   * Fallback cooldown in seconds when neither Quota Endpoint nor passive headers report a reset time.
   * Exponentially backs off across consecutive auto-resumes up to maxCooldownSec (default 60s).
   */
  fallbackCooldownSec: number;
  /** Maximum fallback backoff cooldown in seconds (default 900s = 15m). */
  maxCooldownSec: number;
  /** Automatically restart quota-paused runs once quota becomes available (default true). */
  autoResumeEnabled: boolean;
  /** While paused on quota, poll interval (ms) to detect early recovery from Quota Endpoint (default 30_000 ms). */
  earlyWakePollMs: number;
}

export interface ExternalMcpServerConfig {
  /** Stable MCP server name exposed to all ACP agents (e.g. `ast-grep`, `octofs`, `serena`) */
  name: string;
  /** Executable command (e.g. `uvx`, `npx`, `octofs`, `/path/to/bin`) */
  command: string;
  args?: string[];
  env?: Record<string, string>;
  /** Set false to disable without removing */
  enabled?: boolean;
}

export interface RemoteHostConfig {
  /** Short unique alias for this remote host (e.g. `gpu-box`, `dev-linux`). */
  name: string;
  /** SSH target (`ssh://[user@]host[:port]` or local socket path for tests). */
  sshTarget: string;
  /** Optional remote bridge command override (defaults to `pi-harness bridge`). */
  bridgeCommand?: string;
  /** Whether the local daemon auto-connects a persistent PeerLink on startup (default: true). */
  autoConnect?: boolean;
  /**
   * Reverse peer capability policy for RPCs initiated by this remote host back to us:
   * - `"messages"` (default): allow session messaging, read-only project/session discovery, and outbox sync only.
   * - `"full"`: also allow remote-initiated `project.create/update/remove/browse` and `chat.create` on this machine.
   */
  reverseAccess?: "messages" | "full";
}

export interface HarnessConfig {
  stateDir: string;
  socketPath: string;
  profiles: RunnerProfileConfig[];
  /** Configured remote harnessd hosts for federated projects, remote sessions, and bidirectional peering. */
  remotes?: RemoteHostConfig[];
  /** Optional external stdio MCP servers forwarded automatically to shell-enabled ACP agents (Tier 2 workers, Tier 3 verify/execute, and isolated Done-Reviewers). */
  externalMcpServers?: ExternalMcpServerConfig[];
  codeMode: CodeMode;
  codeExec?: { defaultTimeoutMs?: number; maxTimeoutMs?: number; maxOutputChars?: number };
  defaultOrchestratorProfile?: string;
  defaultWorkerProfile?: string;
  defaultTaskProfile?: string;
  rollover: RolloverConfig;
  worktree: WorktreeConfig;
  goals: GoalsConfig;
  events: EventsConfig;
  inbox: InboxTriageConfig;
  catalog: CatalogConfig;
  quotaRetry: QuotaRetryConfig;
  quotaEndpoint?: QuotaEndpointConfig;
}

/** Top-level keys whose value is a nested section (neither a string nor a list). */
type ConfigSection = {
  [K in keyof HarnessConfig]-?: NonNullable<HarnessConfig[K]> extends string | readonly unknown[] ? never : K;
}[keyof HarnessConfig];

/** Every key each section supports. Adding or removing a key in a section interface fails typecheck here. */
const CONFIG_SECTION_KEYS: { [S in ConfigSection]: { [K in keyof NonNullable<HarnessConfig[S]>]-?: true } } = {
  codeExec: { defaultTimeoutMs: true, maxTimeoutMs: true, maxOutputChars: true },
  rollover: {
    orchestratorThreshold: true,
    workerThreshold: true,
    orchestratorMaxTurns: true,
    workerMaxTurns: true,
    taskMaxTurns: true,
    reviewerMaxTurns: true,
    externalizeBytes: true,
    maxGenerations: true,
  },
  worktree: {
    dir: true,
    artifactsDir: true,
    setupCommand: true,
    ciCommands: true,
    ciTimeoutMs: true,
    gitAuthorName: true,
    gitAuthorEmail: true,
    archiveRetentionDays: true,
    maxConcurrentWorkers: true,
    extraReadRoots: true,
    extraWriteRoots: true,
  },
  goals: {
    defaultMaxReviewCycles: true,
    defaultMaxNoProgressCycles: true,
    gateTimeoutMs: true,
    autoVerifyOnSettle: true,
    maxConcurrentGoals: true,
    gateShell: true,
    gateEnv: true,
  },
  events: { fallbackWakeupIntervalSec: true, wakeDirectorOnGoalSettle: true },
  inbox: {
    mode: true,
    schedulerHost: true,
    restartRecovery: true,
    sweepIntervalSec: true,
    ackMaxSec: true,
    lostOwnerDetectSec: true,
    recoverMaxSec: true,
    maxAutoAttempts: true,
    backoffMin: true,
    leaseMin: true,
    maxNoProgressRenewals: true,
    maxDispatchPerTick: true,
    batchMaxItems: true,
    batchModel: true,
    batchProfileId: true,
    batchMaxTurns: true,
  },
  catalog: {
    ttlHours: true,
    artificialAnalysisKeyEnv: true,
    allowPreview: true,
    preferSubscription: true,
    modelFilterMode: true,
    disabledModels: true,
    enabledModels: true,
  },
  quotaRetry: {
    nativeRetryTimeoutMs: true,
    fallbackCooldownSec: true,
    maxCooldownSec: true,
    autoResumeEnabled: true,
    earlyWakePollMs: true,
  },
  quotaEndpoint: { url: true, headers: true, pollIntervalSec: true, timeoutMs: true, format: true },
};

/** Top-level keys a config file may set; stateDir always comes from the environment. */
const CONFIG_TOP_LEVEL_KEYS: { [K in Exclude<keyof HarnessConfig, "stateDir">]-?: true } = {
  socketPath: true,
  profiles: true,
  remotes: true,
  externalMcpServers: true,
  codeMode: true,
  codeExec: true,
  defaultOrchestratorProfile: true,
  defaultWorkerProfile: true,
  defaultTaskProfile: true,
  rollover: true,
  worktree: true,
  goals: true,
  events: true,
  inbox: true,
  catalog: true,
  quotaRetry: true,
  quotaEndpoint: true,
};

/** Honored only in the global config.json; resolveConfig strips these from the repo layer. */
const GLOBAL_ONLY_CONFIG_KEYS: ReadonlySet<string> = new Set([
  "remotes",
  "worktree.extraReadRoots",
  "worktree.extraWriteRoots",
  "goals.gateShell",
  "goals.gateEnv",
  "inbox.mode",
  "inbox.schedulerHost",
]);

const CONFIG_SECTIONS = Object.keys(CONFIG_SECTION_KEYS) as ConfigSection[];
const SECTION_KEY_SETS: Record<ConfigSection, ReadonlySet<string>> = Object.fromEntries(
  CONFIG_SECTIONS.map((section) => [section, new Set(Object.keys(CONFIG_SECTION_KEYS[section]))]),
) as unknown as Record<ConfigSection, ReadonlySet<string>>;
const TOP_LEVEL_KEYS: ReadonlySet<string> = new Set(Object.keys(CONFIG_TOP_LEVEL_KEYS));
/** Each nested key mapped to its one section; keys with two homes or equal to a top-level key are left out. */
const SECTION_OF_KEY: ReadonlyMap<string, ConfigSection> = (() => {
  const map = new Map<string, ConfigSection>();
  const ambiguous = new Set<string>();
  for (const section of CONFIG_SECTIONS) {
    for (const key of Object.keys(CONFIG_SECTION_KEYS[section])) {
      if (map.has(key) || TOP_LEVEL_KEYS.has(key)) ambiguous.add(key);
      else map.set(key, section);
    }
  }
  for (const key of ambiguous) map.delete(key);
  return map;
})();

export interface MisplacedConfigKey {
  /** Where the key was found, such as `extraReadRoots` or `goals.maxConcurrentWorkers`. */
  path: string;
  /** Where the key is supported, as display text. */
  supported: string;
}

function isMisplacedIn(section: ConfigSection, key: string): boolean {
  if (SECTION_KEY_SETS[section].has(key)) return false;
  const home = SECTION_OF_KEY.get(key);
  return home !== undefined ? home !== section : TOP_LEVEL_KEYS.has(key);
}

/** Known config keys found where they are not supported in a raw layer document. Never reads or returns values. */
export function findMisplacedConfigKeys(raw: unknown, layer: "global" | "repo"): MisplacedConfigKey[] {
  if (!isPlainObject(raw)) return [];
  const out: MisplacedConfigKey[] = [];
  const supported = (target: string) =>
    layer === "repo" && GLOBAL_ONLY_CONFIG_KEYS.has(target) ? `"${target}" in the global config.json` : `"${target}"`;
  for (const key of Object.keys(raw)) {
    const home = SECTION_OF_KEY.get(key);
    if (home !== undefined) {
      out.push({ path: key, supported: supported(`${home}.${key}`) });
    } else if (layer === "repo" && TOP_LEVEL_KEYS.has(key) && GLOBAL_ONLY_CONFIG_KEYS.has(key)) {
      out.push({ path: key, supported: supported(key) });
    }
  }
  for (const section of CONFIG_SECTIONS) {
    const value = raw[section];
    if (!Object.hasOwn(raw, section) || !isPlainObject(value)) continue;
    for (const key of Object.keys(value)) {
      const path = `${section}.${key}`;
      if (SECTION_KEY_SETS[section].has(key)) {
        if (layer === "repo" && GLOBAL_ONLY_CONFIG_KEYS.has(path)) out.push({ path, supported: supported(path) });
        continue;
      }
      const home = SECTION_OF_KEY.get(key);
      if (home !== undefined) {
        out.push({ path, supported: supported(`${home}.${key}`) });
      } else if (TOP_LEVEL_KEYS.has(key)) {
        out.push({ path, supported: supported(key) });
      }
    }
  }
  return out;
}

/** Copy of a raw layer whose sections hold no keys that belong elsewhere; omits `quotaEndpoint` when stripping empties it. */
function dropMisplacedSectionKeys(raw: Record<string, unknown>): Record<string, unknown> {
  let out = raw;
  for (const section of CONFIG_SECTIONS) {
    const sec = raw[section];
    if (!Object.hasOwn(raw, section) || !isPlainObject(sec)) continue;
    if (!Object.keys(sec).some((key) => isMisplacedIn(section, key))) continue;
    const remaining = Object.entries(sec).filter(([key]) => !isMisplacedIn(section, key));
    if (section === "quotaEndpoint" && remaining.length === 0) {
      out = Object.fromEntries(Object.entries(out).filter(([k]) => k !== section));
    } else {
      out = { ...out, [section]: Object.fromEntries(remaining) };
    }
  }
  return out;
}

export function defaultStateDir(): string {
  return process.env.PI_HARNESS_HOME || join(homedir(), ".pi-harness");
}

export function defaultSocketPath(stateDir: string): string {
  const runtime = process.env.XDG_RUNTIME_DIR;
  if (runtime && existsSync(runtime)) {
    return join(runtime, "pi-harness", "daemon.sock");
  }
  // Keep Unix socket paths short (<104 bytes on macOS) while avoiding world-writable /tmp root races
  const candidate = join(stateDir, "daemon.sock");
  if (candidate.length < 96) return candidate;
  const uidDir = join(ensureTempDir(), `pi-harness-${process.getuid?.() ?? "u"}`);
  mkdirSync(uidDir, { recursive: true, mode: 0o700 });
  return join(uidDir, "daemon.sock");
}

/** Resolves this host's writable temp directory and exports it as TMPDIR so child processes inherit it. */
export function ensureTempDir(): string {
  const names = ["TMPDIR", "TMP", "TEMP"] as const;
  const saved = names.map((name) => process.env[name]);
  const restore = (name: (typeof names)[number], value: string | undefined) => {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  };
  const candidates = saved.filter((value): value is string => !!value?.trim());
  // Bun's tmpdir() reads TMPDIR, TMP and TEMP and returns blank values verbatim, so all three are cleared to get the platform default.
  for (const name of names) delete process.env[name];
  try {
    candidates.push(tmpdir());
  } finally {
    restore("TMP", saved[1]);
    restore("TEMP", saved[2]);
  }
  const failures: string[] = [];
  for (const dir of new Set(candidates)) {
    try {
      mkdirSync(dir, { recursive: true, mode: 0o700 });
      accessSync(dir, constants.W_OK | constants.X_OK);
    } catch (err) {
      failures.push(`${dir}: ${err instanceof Error ? err.message : String(err)}`);
      continue;
    }
    process.env.TMPDIR = dir;
    if (failures.length) console.error(`[pi-harness] TMPDIR ${failures.join("; ")}; using ${dir} instead.`);
    return dir;
  }
  restore("TMPDIR", saved[0]);
  throw new Error(`No usable temp directory (${failures.join("; ")}). Set TMPDIR to a writable directory.`);
}

export function defaultProfiles(): RunnerProfileConfig[] {
  return [
    {
      id: "acp:claude",
      kind: "acp",
      adapter: "claude",
      command: "npx",
      args: ["-y", "@agentclientprotocol/claude-agent-acp"],
      billing: "subscription",
      enabled: true,
    },
    {
      id: "acp:codex",
      kind: "acp",
      adapter: "codex",
      command: "npx",
      args: ["-y", "@agentclientprotocol/codex-acp"],
      billing: "subscription",
      enabled: true,
    },
    {
      id: "acp:gemini",
      kind: "acp",
      adapter: "gemini",
      command: "gemini",
      args: ["--acp"],
      billing: "subscription",
      enabled: false,
    },
  ];
}

/** Reads a JSON object file; a missing file or a non-object root (number, string, array, null) yields `undefined`. */
function readJsonIfExists(path: string): Record<string, unknown> | undefined {
  if (!existsSync(path)) return undefined;
  const parsed: unknown = JSON.parse(readFileSync(path, "utf8"));
  return isPlainObject(parsed) ? parsed : undefined;
}

export function interpolateEnv(value: string): string {
  return value.replace(/\$\{([A-Z0-9_]+)\}/g, (_, name: string) => process.env[name] ?? "");
}

export function normalizeWorktreeRoots(worktree: unknown) {
  const fields = worktree as Record<string, unknown> | undefined;
  const invalid: string[] = [];
  function roots(key: "extraReadRoots" | "extraWriteRoots"): string[] | undefined {
    const value = fields?.[key];
    if (value === undefined) return undefined;
    if (!Array.isArray(value)) {
      invalid.push(key);
      return undefined;
    }
    const valid = value.filter((root): root is string =>
      typeof root === "string" && root === root.trim() && !root.includes("\0") &&
      (isAbsolute(root) || root === "~" || root.startsWith("~/")),
    );
    if (valid.length !== value.length) invalid.push(key);
    return valid;
  }
  const normalized = { extraReadRoots: roots("extraReadRoots"), extraWriteRoots: roots("extraWriteRoots") };
  return { roots: normalized, warning: invalid.length ? `Invalid worktree roots in ${invalid.join(", ")}` : undefined };
}

const GATE_ENV_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
const GATE_CONTROL_RE = /[\u0000-\u001f\u007f]/;
const MAX_GATE_ENV_VALUE_BYTES = 32 * 1024;

/**
 * Pure validation of `goals.gateShell` / `goals.gateEnv` (contract pi-harness/goals-gate-config-v1).
 * `null` / missing (and `""` for gateShell) mean unset. Issues name offending keys only, never values.
 */
export function normalizeGateConfig(goals: unknown): {
  gateShell?: string;
  gateEnv?: Record<string, string>;
  issues: string[];
} {
  const issues: string[] = [];
  const out: { gateShell?: string; gateEnv?: Record<string, string>; issues: string[] } = { issues };
  if (!goals || typeof goals !== "object") return out;
  const { gateShell, gateEnv } = goals as Record<string, unknown>;

  if (gateShell !== undefined && gateShell !== null && gateShell !== "") {
    if (
      typeof gateShell === "string" &&
      gateShell.trim() === gateShell &&
      !GATE_CONTROL_RE.test(gateShell) &&
      isAbsolute(gateShell)
    ) {
      out.gateShell = gateShell;
    } else {
      issues.push("goals.gateShell must be an absolute path string");
    }
  }

  if (gateEnv !== undefined && gateEnv !== null) {
    if (!isPlainObject(gateEnv)) {
      issues.push("goals.gateEnv must be an object of string values");
    } else {
      const clean: Record<string, string> = {};
      const bad: string[] = [];
      for (const [key, val] of Object.entries(gateEnv)) {
        const name = GATE_ENV_NAME_RE.test(key) ? key : JSON.stringify(key.slice(0, 64));
        if (!GATE_ENV_NAME_RE.test(key)) bad.push(`${name} (invalid name)`);
        else if (key === "__proto__") bad.push(`${name} (reserved name)`);
        else if (key.startsWith("HARNESS_")) bad.push(`${name} (HARNESS_* names are reserved)`);
        else if (typeof val !== "string" || val.includes("\0")) bad.push(`${name} (value must be a string without NUL)`);
        else if (Buffer.byteLength(val, "utf8") > MAX_GATE_ENV_VALUE_BYTES) bad.push(`${name} (value exceeds 32 KiB)`);
        else if (
          key === "PATH" &&
          (val === "" || val.split(delimiter).some((seg) => !seg || !isAbsolute(seg) || GATE_CONTROL_RE.test(seg)))
        ) {
          bad.push(`${name} (must be absolute directories joined by '${delimiter}')`);
        } else clean[key] = val;
      }
      if (bad.length > 0) issues.push(`goals.gateEnv has invalid entries: ${bad.join(", ")}`);
      else out.gateEnv = clean;
    }
  }
  return out;
}

/** Reads the repository overlay `<repoRoot>/.pi-harness.json`, or `{}` when absent. */
export function readRepoConfig(repoRoot?: string): Record<string, unknown> {
  return (repoRoot ? readJsonIfExists(join(repoRoot, ".pi-harness.json")) : undefined) ?? {};
}

export function loadConfig(repoRoot?: string): HarnessConfig {
  const stateDir = resolve(defaultStateDir());
  mkdirSync(stateDir, { recursive: true });
  return resolveConfig(readJsonIfExists(join(stateDir, "config.json")) ?? {}, readRepoConfig(repoRoot), { stateDir });
}

/**
 * Builds the effective config from layers: `base` (or built-in defaults) < global `config.json` < repo `.pi-harness.json`.
 * Nested sections merge per key; arrays and scalars replace whole. `remotes` and worktree access roots never come
 * from the repo layer. When `base` is given it replaces the built-in defaults, so a removed global key falls back
 * to the base value. Known keys inside a section they do not belong to are dropped, never honored.
 */
export function resolveConfig(
  globalCfg: Record<string, unknown>,
  repoCfg: Record<string, unknown>,
  runtime: { stateDir: string; socketPath?: string },
  base?: HarnessConfig,
): HarnessConfig {
  // Startup stays tolerant of non-object roots; the reload and config.update boundaries reject them.
  if (!isPlainObject(globalCfg)) globalCfg = {};
  if (!isPlainObject(repoCfg)) repoCfg = {};
  globalCfg = dropMisplacedSectionKeys(globalCfg);
  repoCfg = dropMisplacedSectionKeys(repoCfg);
  const stateDir = runtime.stateDir;
  const merged = { ...globalCfg, ...repoCfg } as Partial<HarnessConfig>;
  const rollover: RolloverConfig = {
    ...(base
      ? base.rollover
      : {
          orchestratorThreshold: 0.72,
          workerThreshold: 0.78,
          orchestratorMaxTurns: 200,
          workerMaxTurns: 120,
          taskMaxTurns: 40,
          reviewerMaxTurns: 30,
          externalizeBytes: 2048,
          maxGenerations: 12,
        }),
    ...(globalCfg.rollover as Partial<RolloverConfig> | undefined),
    ...(repoCfg.rollover as Partial<RolloverConfig> | undefined),
  };

  // Only the operator's config may grant access to additional host directories.
  const globalWorktree = globalCfg.worktree as Partial<WorktreeConfig> | undefined;
  const { extraReadRoots: _repoRead, extraWriteRoots: _repoWrite, ...repoWorktree } =
    (repoCfg.worktree as Partial<WorktreeConfig> | undefined) ?? {};
  const { roots, warning } = normalizeWorktreeRoots(globalWorktree);
  if (warning) console.error(`[harness] ${warning}; ignoring invalid entries`);
  const worktree: WorktreeConfig = base
    ? {
        ...base.worktree,
        ...globalWorktree,
        ...repoWorktree,
        extraReadRoots:
          globalWorktree && "extraReadRoots" in globalWorktree ? roots.extraReadRoots : base.worktree.extraReadRoots,
        extraWriteRoots:
          globalWorktree && "extraWriteRoots" in globalWorktree ? roots.extraWriteRoots : base.worktree.extraWriteRoots,
      }
    : {
        dir: ".harness/worktrees",
        artifactsDir: ".harness/artifacts",
        ciTimeoutMs: 300_000,
        gitAuthorName: "pi-harness",
        gitAuthorEmail: "harness@local",
        archiveRetentionDays: 7,
        maxConcurrentWorkers: 4,
        ...globalWorktree,
        ...repoWorktree,
        ...roots,
      };

  // Security: gate shell/env run commands with operator credentials, so they MUST only come from trusted user
  // global config, never from untrusted repository `.pi-harness.json`. They are file-authoritative: `base` never
  // supplies them, so removing them from `config.json` clears them on reload. Invalid values are kept here and fail
  // gates closed at run time (no throw: loadConfig runs for every CLI command).
  const globalGoals = globalCfg.goals as Record<string, unknown> | undefined;
  const { gateShell: globalShell, gateEnv: globalEnv, ...globalGoalsRest } = globalGoals ?? {};
  const { gateShell: _repoShell, gateEnv: _repoEnv, ...repoGoals } =
    (repoCfg.goals as Record<string, unknown> | undefined) ?? {};
  const { gateShell: _baseShell, gateEnv: _baseEnv, ...baseGoals } = base?.goals ?? ({} as Partial<GoalsConfig>);
  const goals: GoalsConfig = {
    ...(base
      ? (baseGoals as GoalsConfig)
      : {
          defaultMaxReviewCycles: 4,
          defaultMaxNoProgressCycles: 2,
          gateTimeoutMs: 180_000,
          autoVerifyOnSettle: true,
          maxConcurrentGoals: 4,
        }),
    ...(globalGoalsRest as Partial<GoalsConfig>),
    ...(repoGoals as Partial<GoalsConfig>),
  };
  if (globalShell !== undefined && globalShell !== null && globalShell !== "") goals.gateShell = globalShell as string;
  if (globalEnv !== undefined && globalEnv !== null) goals.gateEnv = globalEnv as Record<string, string>;

  const events: EventsConfig = {
    ...(base ? base.events : { fallbackWakeupIntervalSec: 600, wakeDirectorOnGoalSettle: true }),
    ...(globalCfg.events as Partial<EventsConfig> | undefined),
    ...(repoCfg.events as Partial<EventsConfig> | undefined),
  };

  // Security: `inbox.mode` and `inbox.schedulerHost` control host-local opt-in and scheduler authority, so they MUST
  // only come from trusted user global config, never from untrusted repository `.pi-harness.json`.
  const {
    mode: _repoInboxMode,
    schedulerHost: _repoInboxHost,
    ...repoInboxRest
  } = (repoCfg.inbox as Record<string, unknown> | undefined) ?? {};
  const inboxRawMerged: Partial<InboxTriageConfig> = {
    ...(base ? base.inbox : { ...DEFAULT_INBOX_TRIAGE_CONFIG, backoffMin: [...DEFAULT_INBOX_TRIAGE_CONFIG.backoffMin] }),
    ...(globalCfg.inbox as Partial<InboxTriageConfig> | undefined),
    ...(repoInboxRest as Partial<InboxTriageConfig>),
  };
  const inbox: InboxTriageConfig = {
    ...(inboxRawMerged as InboxTriageConfig),
    backoffMin: Array.isArray(inboxRawMerged.backoffMin)
      ? [...inboxRawMerged.backoffMin]
      : [...DEFAULT_INBOX_TRIAGE_CONFIG.backoffMin],
  };

  const catalog: CatalogConfig = {
    ...(base
      ? base.catalog
      : {
          ttlHours: 24,
          artificialAnalysisKeyEnv: "ARTIFICIAL_ANALYSIS_API_KEY",
          allowPreview: true,
          preferSubscription: true,
          modelFilterMode: "all" as const,
          disabledModels: [],
          enabledModels: [],
        }),
    ...(globalCfg.catalog as Partial<CatalogConfig> | undefined),
    ...(repoCfg.catalog as Partial<CatalogConfig> | undefined),
  };

  const quotaRetry: QuotaRetryConfig = {
    ...(base
      ? base.quotaRetry
      : {
          nativeRetryTimeoutMs: 45_000,
          fallbackCooldownSec: 60,
          maxCooldownSec: 900,
          autoResumeEnabled: true,
          earlyWakePollMs: 30_000,
        }),
    ...(globalCfg.quotaRetry as Partial<QuotaRetryConfig> | undefined),
    ...(repoCfg.quotaRetry as Partial<QuotaRetryConfig> | undefined),
  };

  const profiles =
    Array.isArray(merged.profiles) && merged.profiles.length > 0 ? merged.profiles : (base?.profiles ?? defaultProfiles());

  // Security: `remotes` (SSH peer trust & auto-connect targets) MUST only come from trusted user global config,
  // never from untrusted repository `.pi-harness.json`.
  const rawRemotes = "remotes" in globalCfg ? globalCfg.remotes : base?.remotes;
  const remotes = Array.isArray(rawRemotes) ? (rawRemotes as RemoteHostConfig[]) : undefined;
  // An explicit `[]` clears servers inherited from `base`.
  const rawExternalMcp = "externalMcpServers" in merged ? merged.externalMcpServers : base?.externalMcpServers;
  const externalMcpServers = Array.isArray(rawExternalMcp) ? rawExternalMcp : undefined;

  const codeMode = merged.codeMode ?? base?.codeMode ?? "collapsed";
  if (!["collapsed", "hybrid", "off"].includes(codeMode)) throw new Error(`Unknown codeMode: ${codeMode}`);
  const firstEnabledProfileId = profiles.find(
    (p) =>
      Boolean(p) &&
      typeof p === "object" &&
      typeof (p as { id?: unknown }).id === "string" &&
      (p as { id: string }).id.length > 0 &&
      p.enabled !== false,
  )?.id;
  return {
    stateDir,
    socketPath: runtime.socketPath ?? (merged.socketPath ? resolve(merged.socketPath) : defaultSocketPath(stateDir)),
    profiles,
    ...(remotes?.length ? { remotes } : {}),
    ...(externalMcpServers?.length ? { externalMcpServers } : {}),
    codeMode,
    codeExec: {
      ...(base ? base.codeExec : { defaultTimeoutMs: 300_000, maxTimeoutMs: 1_800_000 }),
      ...(globalCfg.codeExec as HarnessConfig["codeExec"]),
      ...(repoCfg.codeExec as HarnessConfig["codeExec"]),
    },
    defaultOrchestratorProfile: merged.defaultOrchestratorProfile ?? base?.defaultOrchestratorProfile ?? firstEnabledProfileId,
    defaultWorkerProfile: merged.defaultWorkerProfile ?? base?.defaultWorkerProfile ?? firstEnabledProfileId,
    defaultTaskProfile: merged.defaultTaskProfile ?? base?.defaultTaskProfile ?? firstEnabledProfileId,
    rollover,
    worktree,
    goals,
    events,
    inbox,
    catalog,
    quotaRetry,
    quotaEndpoint:
      "quotaEndpoint" in merged
        ? ((merged.quotaEndpoint as QuotaEndpointConfig | null | undefined) ?? undefined)
        : base?.quotaEndpoint,
  };
}

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/**
 * Deep JSON equality: plain-object member order is ignored, array order is significant, and members whose value is
 * `undefined` count as absent (as in `JSON.stringify`).
 */
export function stableEqual(a: unknown, b: unknown): boolean {
  if (a === b || Object.is(a, b)) return true;
  if (Array.isArray(a) || Array.isArray(b)) {
    return Array.isArray(a) && Array.isArray(b) && a.length === b.length && a.every((x, i) => stableEqual(x, b[i]));
  }
  if (!isPlainObject(a) || !isPlainObject(b)) return false;
  const keysA = Object.keys(a).filter((k) => a[k] !== undefined);
  const keysB = Object.keys(b).filter((k) => b[k] !== undefined);
  return keysA.length === keysB.length && keysA.every((k) => b[k] !== undefined && stableEqual(a[k], b[k]));
}

/**
 * Validates the shape of a global `config.json` document or a `config.update` patch. Unknown keys are ignored,
 * and sections emptied only by misplaced keys are omitted before validation.
 * Returns one message per problem, naming the path and expectation; values are never echoed (they may be secrets).
 */
export function validateConfigRaw(raw: unknown): string[] {
  const errors: string[] = [];
  if (!isPlainObject(raw)) return ["config must be a JSON object"];
  const doc = dropMisplacedSectionKeys(raw);

  const isString = (v: unknown) => typeof v === "string";
  const isNonEmptyString = (v: unknown) => typeof v === "string" && v.trim().length > 0;
  const isBoolean = (v: unknown) => typeof v === "boolean";
  const isStringArray = (v: unknown) => Array.isArray(v) && v.every((x) => typeof x === "string");
  const isStringRecord = (v: unknown) => isPlainObject(v) && Object.values(v).every((x) => typeof x === "string");
  const isPositiveInt = (v: unknown) => typeof v === "number" && Number.isInteger(v) && v > 0;
  const isFiniteNonNegative = (v: unknown) => typeof v === "number" && Number.isFinite(v) && v >= 0;
  const isFinitePositive = (v: unknown) => typeof v === "number" && Number.isFinite(v) && v > 0;
  const oneOf = (...values: string[]) => (v: unknown) => typeof v === "string" && values.includes(v);

  type Rule = [check: (v: unknown) => boolean, expectation: string];
  const STRING: Rule = [isString, "a string"];
  const NON_EMPTY: Rule = [isNonEmptyString, "a non-empty string"];
  const BOOLEAN: Rule = [isBoolean, "a boolean"];
  const STRINGS: Rule = [isStringArray, "an array of strings"];
  const STRING_MAP: Rule = [isStringRecord, "an object of string values"];
  const POS_INT: Rule = [isPositiveInt, "a positive integer"];
  const NON_NEG: Rule = [isFiniteNonNegative, "a finite number >= 0"];
  const POSITIVE: Rule = [isFinitePositive, "a finite number > 0"];

  const checkFields = (obj: Record<string, unknown>, path: string, rules: Record<string, Rule>, required = false) => {
    for (const [key, [check, expectation]] of Object.entries(rules)) {
      if (!required && !(key in obj)) continue;
      if (!check(obj[key])) errors.push(`${path}.${key} must be ${expectation}`);
    }
  };
  const section = (key: string, rules: Record<string, Rule>): Record<string, unknown> | undefined => {
    if (!(key in doc)) return undefined;
    const value = doc[key];
    if (!isPlainObject(value)) {
      errors.push(`${key} must be an object`);
      return undefined;
    }
    checkFields(value, key, rules);
    return value;
  };
  const list = (key: string): unknown[] | undefined => {
    if (!(key in doc)) return undefined;
    if (!Array.isArray(doc[key])) {
      errors.push(`${key} must be an array`);
      return undefined;
    }
    return doc[key] as unknown[];
  };

  checkFields(doc, "config", { stateDir: NON_EMPTY, socketPath: NON_EMPTY });

  const profileIds = new Set<string>();
  list("profiles")?.forEach((profile, i) => {
    const path = `profiles[${i}]`;
    if (!isPlainObject(profile)) {
      errors.push(`${path} must be an object`);
      return;
    }
    if (!isNonEmptyString(profile.id)) errors.push(`${path}.id must be a non-empty string`);
    else if (profileIds.has(profile.id as string)) errors.push(`${path}.id must be unique`);
    else profileIds.add(profile.id as string);
    checkFields(profile, path, { billing: [oneOf("subscription", "api", "local"), '"subscription", "api", or "local"'] }, true);
    checkFields(profile, path, { enabled: BOOLEAN, models: STRINGS });
    if (profile.kind === "acp") {
      checkFields(
        profile,
        path,
        {
          adapter: [oneOf("claude", "codex", "gemini", "generic"), '"claude", "codex", "gemini", or "generic"'],
          command: NON_EMPTY,
        },
        true,
      );
      checkFields(profile, path, {
        args: STRINGS,
        readOnlyArgs: STRINGS,
        env: STRING_MAP,
      });
    } else if (profile.kind === "pi") {
      checkFields(profile, path, { provider: NON_EMPTY }, true);
      checkFields(profile, path, {
        api: [
          oneOf("openai-completions", "openai-responses", "anthropic-messages", "google-generative-ai"),
          "a supported pi api",
        ],
        baseUrl: STRING,
        apiKeyEnv: STRING,
      });
    } else {
      errors.push(`${path}.kind must be "acp" or "pi"`);
    }
  });

  const remoteNames = new Set<string>();
  list("remotes")?.forEach((remote, i) => {
    const path = `remotes[${i}]`;
    if (!isPlainObject(remote)) {
      errors.push(`${path} must be an object`);
      return;
    }
    const name = remote.name;
    if (typeof name !== "string" || !/^[A-Za-z0-9._-]+$/.test(name)) {
      errors.push(`${path}.name must contain only letters, digits, '.', '_', or '-'`);
    } else if (name.toLowerCase() === "local") {
      errors.push(`${path}.name must not be "local"`);
    } else if (remoteNames.has(name.toLowerCase())) {
      errors.push(`${path}.name must be unique (case-insensitive)`);
    } else {
      remoteNames.add(name.toLowerCase());
    }
    if (!isNonEmptyString(remote.sshTarget)) errors.push(`${path}.sshTarget must be a non-empty string`);
    checkFields(remote, path, {
      bridgeCommand: STRING,
      autoConnect: BOOLEAN,
      reverseAccess: [oneOf("messages", "full"), '"messages" or "full"'],
    });
  });

  list("externalMcpServers")?.forEach((server, i) => {
    const path = `externalMcpServers[${i}]`;
    if (!isPlainObject(server)) {
      errors.push(`${path} must be an object`);
      return;
    }
    if (!isNonEmptyString(server.name)) errors.push(`${path}.name must be a non-empty string`);
    checkFields(server, path, { enabled: BOOLEAN, args: STRINGS, env: STRING_MAP });
    if (server.enabled === false) checkFields(server, path, { command: STRING });
    else if (!isNonEmptyString(server.command)) errors.push(`${path}.command must be a non-empty string`);
  });

  checkFields(doc, "config", {
    codeMode: [oneOf("collapsed", "hybrid", "off"), '"collapsed", "hybrid", or "off"'],
    defaultOrchestratorProfile: NON_EMPTY,
    defaultWorkerProfile: NON_EMPTY,
    defaultTaskProfile: NON_EMPTY,
  });
  section("codeExec", { defaultTimeoutMs: POS_INT, maxTimeoutMs: POS_INT, maxOutputChars: POS_INT });
  const ratio: Rule = [(v) => typeof v === "number" && v > 0 && v <= 1, "a number in (0, 1]"];
  section("rollover", {
    orchestratorThreshold: ratio,
    workerThreshold: ratio,
    orchestratorMaxTurns: POS_INT,
    workerMaxTurns: POS_INT,
    taskMaxTurns: POS_INT,
    reviewerMaxTurns: POS_INT,
    externalizeBytes: POS_INT,
    maxGenerations: POS_INT,
  });
  const worktree = section("worktree", {
    dir: NON_EMPTY,
    artifactsDir: NON_EMPTY,
    gitAuthorName: NON_EMPTY,
    gitAuthorEmail: NON_EMPTY,
    setupCommand: STRING,
    ciCommands: STRINGS,
    ciTimeoutMs: POS_INT,
    maxConcurrentWorkers: POS_INT,
    archiveRetentionDays: NON_NEG,
  });
  if (worktree) {
    const { warning } = normalizeWorktreeRoots(worktree);
    if (warning) errors.push(warning);
  }
  const goalsSection = section("goals", {
    defaultMaxReviewCycles: POS_INT,
    defaultMaxNoProgressCycles: POS_INT,
    gateTimeoutMs: POS_INT,
    maxConcurrentGoals: POS_INT,
    autoVerifyOnSettle: BOOLEAN,
  });
  if (goalsSection) errors.push(...normalizeGateConfig(goalsSection).issues);
  section("events", { fallbackWakeupIntervalSec: POSITIVE, wakeDirectorOnGoalSettle: BOOLEAN });
  section("inbox", {
    mode: [oneOf("off", "report", "on"), '"off", "report", or "on"'],
    schedulerHost: [(v) => v === null || isNonEmptyString(v), "a non-empty string or null"],
    restartRecovery: [oneOf("handoff", "resume"), '"handoff" or "resume"'],
    sweepIntervalSec: POSITIVE,
    ackMaxSec: POSITIVE,
    lostOwnerDetectSec: POSITIVE,
    recoverMaxSec: POSITIVE,
    maxAutoAttempts: POS_INT,
    backoffMin: [
      (v) => Array.isArray(v) && v.length > 0 && v.every((x) => isFinitePositive(x)),
      "a non-empty array of finite numbers > 0",
    ],
    leaseMin: POSITIVE,
    maxNoProgressRenewals: [(v) => typeof v === "number" && Number.isInteger(v) && v >= 0, "an integer >= 0"],
    maxDispatchPerTick: POS_INT,
    batchMaxItems: POS_INT,
    batchModel: [(v) => v === null || isNonEmptyString(v), "a non-empty string or null"],
    batchProfileId: [(v) => v === null || isNonEmptyString(v), "a non-empty string or null"],
    batchMaxTurns: POS_INT,
  });
  section("catalog", {
    ttlHours: NON_NEG,
    artificialAnalysisKeyEnv: STRING,
    allowPreview: BOOLEAN,
    preferSubscription: BOOLEAN,
    modelFilterMode: [oneOf("all", "allowlist"), '"all" or "allowlist"'],
    disabledModels: STRINGS,
    enabledModels: STRINGS,
  });
  section("quotaRetry", {
    nativeRetryTimeoutMs: NON_NEG,
    fallbackCooldownSec: NON_NEG,
    maxCooldownSec: NON_NEG,
    earlyWakePollMs: POSITIVE,
    autoResumeEnabled: BOOLEAN,
  });
  if ("quotaEndpoint" in doc && doc.quotaEndpoint !== null) {
    const endpoint = doc.quotaEndpoint;
    if (!isPlainObject(endpoint)) {
      errors.push("quotaEndpoint must be an object or null");
    } else {
      if (!isNonEmptyString(endpoint.url)) errors.push("quotaEndpoint.url must be a non-empty string");
      checkFields(endpoint, "quotaEndpoint", {
        headers: STRING_MAP,
        pollIntervalSec: POSITIVE,
        timeoutMs: POSITIVE,
        format: [oneOf("protojson", "protobuf"), '"protojson" or "protobuf"'],
      });
    }
  }
  return errors;
}

/** One secret-safe difference between two effective configs. List entries carry ids/names only, never values. */
export interface ConfigChange {
  path: string;
  from?: unknown;
  to?: unknown;
  added?: string[];
  removed?: string[];
  changed?: string[];
}

/**
 * Reports what changed between two configs without exposing secret-bearing values (env, args, commands, URLs, headers).
 * Tolerates malformed data on either side (e.g. a startup config holding `[null]`), reporting a path-level change
 * instead of throwing, so a valid replacement can always be applied.
 */
export function diffHarnessConfig(prev: HarnessConfig, next: HarnessConfig): ConfigChange[] {
  const changes: ConfigChange[] = [];
  const isPrintable = (v: unknown) => v === undefined || typeof v === "number" || typeof v === "boolean";

  for (const key of ["codeMode", "defaultOrchestratorProfile", "defaultWorkerProfile", "defaultTaskProfile"] as const) {
    const a: unknown = prev[key];
    const b: unknown = next[key];
    if (stableEqual(a, b)) continue;
    const showValues = [a, b].every((v) => isPrintable(v) || typeof v === "string");
    changes.push(showValues ? { path: key, from: a, to: b } : { path: key });
  }

  for (const key of ["codeExec", "rollover", "worktree", "goals", "events", "inbox", "catalog", "quotaRetry"] as const) {
    const rawA: unknown = prev[key] ?? {};
    const rawB: unknown = next[key] ?? {};
    if (!isPlainObject(rawA) || !isPlainObject(rawB)) {
      if (!stableEqual(rawA, rawB)) changes.push({ path: key });
      continue;
    }
    for (const sub of new Set([...Object.keys(rawA), ...Object.keys(rawB)])) {
      if (stableEqual(rawA[sub], rawB[sub])) continue;
      const path = `${key}.${sub}`;
      const neverShowValues = path === "goals.gateShell" || path === "goals.gateEnv";
      const showValues =
        !neverShowValues &&
        ((isPrintable(rawA[sub]) && isPrintable(rawB[sub])) ||
          ((path === "catalog.modelFilterMode" || path === "inbox.mode" || path === "inbox.restartRecovery") &&
            typeof rawA[sub] !== "object" &&
            typeof rawB[sub] !== "object"));
      changes.push(showValues ? { path, from: rawA[sub], to: rawB[sub] } : { path });
    }
  }

  const diffList = (path: string, a: unknown, b: unknown, key: string) => {
    const entries = (list: unknown) => {
      const map = new Map<string, unknown>();
      if (Array.isArray(list)) {
        for (const item of list) {
          if (isPlainObject(item) && typeof item[key] === "string") map.set(item[key] as string, item);
        }
      }
      return map;
    };
    const before = entries(a);
    const after = entries(b);
    const added = [...after.keys()].filter((k) => !before.has(k));
    const removed = [...before.keys()].filter((k) => !after.has(k));
    const changed = [...after.keys()].filter((k) => before.has(k) && !stableEqual(before.get(k), after.get(k)));
    if (added.length || removed.length || changed.length) changes.push({ path, added, removed, changed });
    else if (!stableEqual(a ?? [], b ?? [])) changes.push({ path, changed: [] }); // order, duplicate, or malformed entries
  };
  diffList("profiles", prev.profiles, next.profiles, "id");
  diffList("remotes", prev.remotes, next.remotes, "name");
  diffList("externalMcpServers", prev.externalMcpServers, next.externalMcpServers, "name");

  const pe: unknown = prev.quotaEndpoint;
  const ne: unknown = next.quotaEndpoint;
  const pePresent = pe !== undefined && pe !== null;
  const nePresent = ne !== undefined && ne !== null;
  if (pePresent !== nePresent) {
    changes.push({ path: "quotaEndpoint", from: pePresent ? "set" : "unset", to: nePresent ? "set" : "unset" });
  } else if (pePresent && nePresent && !stableEqual(pe, ne)) {
    if (isPlainObject(pe) && isPlainObject(ne)) {
      const keys = new Set([...Object.keys(pe), ...Object.keys(ne)]);
      const changed = [...keys].filter((k) => !stableEqual(pe[k], ne[k]));
      changes.push({ path: "quotaEndpoint", changed });
    } else {
      changes.push({ path: "quotaEndpoint", changed: [] });
    }
  }
  return changes;
}

/**
 * Persists mutable user settings & external harness CLI profiles to `~/.pi-harness/config.json`
 * so changes made in the `/settings` TUI overlay survive restarts and hot-reload inside `harnessd`.
 */
export function saveGlobalConfig(config: HarnessConfig): string {
  mkdirSync(config.stateDir, { recursive: true });
  const filePath = join(config.stateDir, "config.json");
  writeFileSync(filePath, serializeGlobalConfig(config), "utf8");
  return filePath;
}

/** The exact `config.json` text `saveGlobalConfig` writes for `config`. */
export function serializeGlobalConfig(config: HarnessConfig): string {
  const serialized = {
    profiles: config.profiles,
    ...(config.remotes?.length ? { remotes: config.remotes } : {}),
    ...(config.externalMcpServers?.length ? { externalMcpServers: config.externalMcpServers } : {}),
    codeMode: config.codeMode,
    codeExec: config.codeExec,
    defaultOrchestratorProfile: config.defaultOrchestratorProfile,
    defaultWorkerProfile: config.defaultWorkerProfile,
    defaultTaskProfile: config.defaultTaskProfile,
    rollover: config.rollover,
    worktree: config.worktree,
    goals: config.goals,
    events: config.events,
    inbox: config.inbox,
    catalog: config.catalog,
    quotaRetry: config.quotaRetry,
    ...(config.socketPath !== defaultSocketPath(config.stateDir) ? { socketPath: config.socketPath } : {}),
    ...(config.quotaEndpoint ? { quotaEndpoint: config.quotaEndpoint } : {}),
  };
  return JSON.stringify(serialized, null, 2) + "\n";
}

export const ACP_PROFILE_PRESETS: Record<string, Omit<AcpProfileConfig, "id">> = {
  claude: {
    kind: "acp",
    adapter: "claude",
    command: "npx",
    args: ["-y", "@agentclientprotocol/claude-agent-acp"],
    billing: "subscription",
    enabled: true,
  },
  codex: {
    kind: "acp",
    adapter: "codex",
    command: "npx",
    args: ["-y", "@agentclientprotocol/codex-acp"],
    billing: "subscription",
    enabled: true,
  },
  gemini: {
    kind: "acp",
    adapter: "gemini",
    command: "gemini",
    args: ["--acp"],
    billing: "subscription",
    enabled: true,
  },
  opencode: {
    kind: "acp",
    adapter: "generic",
    command: "opencode",
    args: ["acp"],
    billing: "subscription",
    enabled: true,
  },
  amp: {
    kind: "acp",
    adapter: "generic",
    command: "npx",
    args: ["-y", "amp-acp"],
    billing: "subscription",
    enabled: true,
  },
  antigravity: {
    kind: "acp",
    adapter: "generic",
    command: "agy",
    args: ["acp"],
    billing: "subscription",
    enabled: true,
  },
};

/** Optional presets for battle-tested external MCP code tools forwarded to all non-control-plane ACP agents. */
export const EXTERNAL_MCP_PRESETS: Record<string, ExternalMcpServerConfig> = {
  octofs: {
    name: "octofs",
    command: "octofs",
    args: ["mcp"],
    enabled: true,
  },
  "ast-grep": {
    name: "ast-grep",
    command: "uvx",
    args: ["--from", "git+https://github.com/ast-grep/ast-grep-mcp", "ast-grep-server"],
    enabled: true,
  },
};
