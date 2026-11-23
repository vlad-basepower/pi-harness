import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { constants } from "node:fs";
import { access, stat } from "node:fs/promises";
import { homedir, userInfo } from "node:os";
import { basename, delimiter, dirname, isAbsolute, join } from "node:path";
import { normalizeGateConfig } from "../config.js";
import { nativeToolsPathDirs } from "./native-tools.js";

export { normalizeGateConfig } from "../config.js";

/**
 * Shared environment and runner for verification gates (goal criteria, merge-queue CI, worktree setup).
 *
 * PATH = dedupe([goals.gateEnv.PATH, mise PATH, login-shell PATH, daemon PATH, static fallbacks]).
 * Commands run under `/bin/sh -c` (or `goals.gateShell -c`) in their own process group; the user's
 * login shell is only used to discover PATH. Nothing in this module blocks the event loop.
 */

export interface GateRuntimeConfig {
  gateShell?: unknown;
  gateEnv?: unknown;
  stateDir?: string;
}

export interface ResolvedGateEnv {
  shell: string;
  env: NodeJS.ProcessEnv;
  /** One line describing where PATH came from. Never contains env values. */
  provenance: string;
}

/** Invalid operator gate configuration (`goals.gateShell` / `goals.gateEnv`). */
export class GateConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GateConfigError";
  }
}

export type GateCommandFailure = "exit" | "timeout" | "cancelled" | "output_limit" | "spawn";

export class GateCommandError extends Error {
  constructor(
    message: string,
    readonly stdout: string,
    readonly stderr: string,
    readonly exitCode: number | null,
    readonly reason: GateCommandFailure,
  ) {
    super(message);
    this.name = "GateCommandError";
  }
}

const ENV_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
const CONTROL_RE = /[\u0000-\u001f\u007f]/;
const MAX_PATH_ENTRIES = 256;
const OPERATOR_ERROR = "operator config error, not a code defect";

function isBlank(v: unknown): boolean {
  return v === undefined || v === null || v === "";
}

async function isExecutableFile(p: string): Promise<boolean> {
  try {
    if (!(await stat(p)).isFile()) return false;
    await access(p, constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

async function isDirectory(p: string): Promise<boolean> {
  try {
    return (await stat(p)).isDirectory();
  } catch {
    return false;
  }
}

/** Absolute, control-free, deduped PATH entries (syntax only). */
function pathSegments(raw: string | undefined): string[] {
  const out: string[] = [];
  for (const seg of (raw ?? "").split(delimiter)) {
    if (seg && isAbsolute(seg) && !CONTROL_RE.test(seg) && !out.includes(seg)) out.push(seg);
  }
  return out;
}

/** Like pathSegments, but keeps only existing directories and caps the list. */
async function sanitizePath(raw: string): Promise<string[]> {
  const candidates = pathSegments(raw).slice(0, MAX_PATH_ENTRIES * 2);
  const exists = await Promise.all(candidates.map(isDirectory));
  return candidates.filter((_, i) => exists[i]).slice(0, MAX_PATH_ENTRIES);
}

function dedupe(lists: string[][]): string[] {
  return [...new Set(lists.flat())];
}

function abortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("This operation was aborted", "AbortError");
}

/** Rejects as soon as `signal` aborts; the underlying promise keeps running for other waiters. */
function raceSignal<T>(p: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return p;
  if (signal.aborted) return Promise.reject(abortReason(signal));
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(abortReason(signal));
    signal.addEventListener("abort", onAbort, { once: true });
    p.then(resolve, reject).finally(() => signal.removeEventListener("abort", onAbort));
  });
}

interface SpawnResult {
  stdout: string;
  stderr: string;
  exitCode: number | null;
  /** Set when the harness stopped the process, or it could not start. */
  stopped?: Exclude<GateCommandFailure, "exit">;
  spawnError?: Error;
  /** Value returned by `settleOn` when it ended the process early. */
  settled?: string;
}

/**
 * Spawns `bin args` in its own process group and collects output. Never rejects. On timeout, abort,
 * overflow or early settle the whole group gets SIGKILL, so background children cannot outlive it,
 * and the result settles at once instead of waiting for pipe EOF.
 */
function spawnGroup(
  bin: string,
  args: string[],
  o: {
    cwd: string;
    env: NodeJS.ProcessEnv;
    timeoutMs: number;
    maxOutputBytes: number;
    signal?: AbortSignal;
    captureStderr: boolean;
    settleOn?: (stdout: string) => string | undefined;
  },
): Promise<SpawnResult> {
  return new Promise<SpawnResult>((resolve) => {
    const out: Buffer[] = [];
    const err: Buffer[] = [];
    let stopped: SpawnResult["stopped"];
    let done = false;
    const text = (chunks: Buffer[]) => Buffer.concat(chunks).toString();
    let child: ReturnType<typeof spawn>;
    try {
      child = spawn(bin, args, {
        cwd: o.cwd,
        env: o.env,
        detached: true,
        stdio: ["ignore", "pipe", o.captureStderr ? "pipe" : "ignore"],
      });
    } catch (e) {
      resolve({ stdout: "", stderr: "", exitCode: null, stopped: "spawn", spawnError: e as Error });
      return;
    }
    /** Best-effort SIGKILL of the whole group, then the leader; never throws (ESRCH/EPERM ignored). */
    const killGroup = () => {
      if (!child.pid) return;
      try {
        process.kill(-child.pid, "SIGKILL");
        return;
      } catch {
        // Group already gone or not signalable; fall back to the leader.
      }
      try {
        child.kill("SIGKILL");
      } catch {
        // Already exited.
      }
    };
    /**
     * Settles exactly once. With `kill`, also kills the group and releases the pipes so the result
     * never waits for EOF from a helper in another process group that inherited stdout/stderr.
     */
    const finish = (r: Omit<SpawnResult, "stdout" | "stderr">, kill: boolean) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      o.signal?.removeEventListener("abort", onAbort);
      if (kill) {
        killGroup();
        child.stdout?.destroy();
        child.stderr?.destroy();
        child.unref?.();
      }
      resolve({ stdout: text(out), stderr: text(err), ...r });
    };
    const stop = (reason: NonNullable<SpawnResult["stopped"]>) => {
      if (done) return;
      stopped = reason;
      finish({ exitCode: null, stopped: reason }, true);
    };
    const onAbort = () => stop("cancelled");
    const timer = setTimeout(() => stop("timeout"), o.timeoutMs);
    o.signal?.addEventListener("abort", onAbort, { once: true });
    if (o.signal?.aborted) stop("cancelled");

    const collect = (stream: NodeJS.ReadableStream | null, chunks: Buffer[], watch: boolean) => {
      let bytes = 0;
      stream?.on("data", (chunk: Buffer) => {
        if (done) return;
        bytes += chunk.length;
        if (bytes > o.maxOutputBytes) return stop("output_limit");
        chunks.push(chunk);
        if (watch && o.settleOn) {
          const settled = o.settleOn(text(chunks));
          // Do not wait for `close`: rc-file helpers (e.g. gitstatusd) may keep the pipe open.
          if (settled !== undefined) finish({ exitCode: null, settled }, true);
        }
      });
    };
    collect(child.stdout, out, true);
    collect(child.stderr, err, false);
    child.once("error", (e) => finish({ exitCode: null, stopped: stopped ?? "spawn", spawnError: e }, true));
    // With settleOn, reap leftover group members (backgrounded jobs) once the leader has exited.
    child.once("close", (code) => finish({ exitCode: code, stopped }, Boolean(o.settleOn)));
  });
}

// ---------------------------------------------------------------------------------------------
// Login-shell PATH probe
// ---------------------------------------------------------------------------------------------

interface LoginResult {
  dirs: string[];
  desc: string;
}

interface ProbeEntry {
  result?: LoginResult;
  lastGood?: LoginResult;
  expiresAt: number;
  inflight?: Promise<LoginResult>;
}

const LOGIN_OK_TTL_MS = 15 * 60_000;
const LOGIN_FAIL_TTL_MS = 5 * 60_000;
const probeCache = new Map<string, ProbeEntry>();
const fallbackCache = new Map<string, Promise<string[]>>();
/** Bumped by invalidateGateEnvCache; probes started in an older generation never write the cache. */
let cacheGeneration = 0;

/**
 * Marks cached login PATHs stale (keeping last-good values), detaches in-flight probes so the next
 * caller starts a fresh one, and drops cached fallback dirs.
 */
export function invalidateGateEnvCache(): void {
  cacheGeneration += 1;
  fallbackCache.clear();
  for (const entry of probeCache.values()) {
    entry.expiresAt = 0;
    entry.inflight = undefined;
  }
}

async function pickProbeShell(): Promise<string> {
  let userShell: string | undefined;
  try {
    userShell = userInfo().shell ?? undefined;
  } catch {
    userShell = undefined;
  }
  const candidates = [
    process.env.SHELL,
    userShell,
    process.platform === "darwin" ? "/bin/zsh" : undefined,
    "/bin/bash",
  ];
  for (const c of candidates) {
    if (c && isAbsolute(c) && !CONTROL_RE.test(c) && (await isExecutableFile(c))) return c;
  }
  return "/bin/sh";
}

function probeAttempts(shell: string): Array<{ bin: string; args: string[]; timeoutMs: number }> {
  const name = basename(shell);
  if (name === "zsh" || name === "bash") {
    return [
      { bin: shell, args: ["-l", "-i", "-c"], timeoutMs: 5000 },
      { bin: shell, args: ["-l", "-c"], timeoutMs: 3000 },
    ];
  }
  if (["sh", "dash", "ash", "ksh", "mksh", "fish"].includes(name)) {
    return [{ bin: shell, args: ["-l", "-c"], timeoutMs: 3000 }];
  }
  return [{ bin: "/bin/sh", args: ["-l", "-c"], timeoutMs: 3000 }];
}

async function probeLoginPath(shell: string): Promise<LoginResult | null> {
  for (const attempt of probeAttempts(shell)) {
    const hex = randomBytes(8).toString("hex");
    const begin = `__PIH_${hex}_B__`;
    const end = `__PIH_${hex}_E__`;
    const r = await spawnGroup(attempt.bin, [...attempt.args, `printf '%s%s%s' '${begin}' "$PATH" '${end}'`], {
      cwd: homedir(),
      env: { ...process.env, PI_HARNESS_RESOLVING_ENV: "1", DISABLE_AUTO_UPDATE: "true" },
      timeoutMs: attempt.timeoutMs,
      maxOutputBytes: 256 * 1024,
      captureStderr: false,
      settleOn: (stdout) => {
        const b = stdout.lastIndexOf(begin);
        if (b < 0) return undefined;
        const e = stdout.indexOf(end, b + begin.length);
        return e < 0 ? undefined : stdout.slice(b + begin.length, e);
      },
    });
    if (r.settled === undefined) continue;
    const dirs = await sanitizePath(r.settled);
    if (dirs.length > 0) {
      return { dirs, desc: `login=ok(${basename(attempt.bin)},${attempt.args.slice(0, -1).join(" ")})` };
    }
  }
  return null;
}

/** Login-shell PATH for this daemon user: cached, single-flight, raced against `signal`. */
async function loginPath(signal?: AbortSignal): Promise<LoginResult> {
  if (process.env.PI_HARNESS_GATE_LOGIN_PROBE === "0") return { dirs: [], desc: "login=disabled" };
  const shell = await pickProbeShell();
  let entry = probeCache.get(shell);
  if (!entry) {
    entry = { expiresAt: 0 };
    probeCache.set(shell, entry);
  }
  if (entry.result && entry.expiresAt > Date.now()) return entry.result;
  let inflight = entry.inflight;
  if (!inflight) {
    const e = entry;
    const myGen = cacheGeneration;
    const flight: Promise<LoginResult> = probeLoginPath(shell)
      .catch(() => null)
      .then((res) => {
        const current = myGen === cacheGeneration;
        let result: LoginResult;
        if (res) {
          result = res;
          if (current) {
            e.lastGood = res;
            e.expiresAt = Date.now() + LOGIN_OK_TTL_MS;
          }
        } else {
          result = e.lastGood
            ? { dirs: e.lastGood.dirs, desc: e.lastGood.desc.replace("login=ok", "login=stale") }
            : { dirs: [], desc: `login=failed(${basename(shell)})` };
          console.error(
            e.lastGood
              ? `[gate-env] login PATH probe failed for ${shell}; gates keep the previous cached login PATH`
              : `[gate-env] login PATH probe failed for ${shell}; gates use daemon PATH and fallbacks`,
          );
          if (current) e.expiresAt = Date.now() + LOGIN_FAIL_TTL_MS;
        }
        if (current) e.result = result;
        if (e.inflight === flight) e.inflight = undefined;
        return result;
      });
    inflight = e.inflight = flight;
  }
  return raceSignal(inflight, signal);
}

// ---------------------------------------------------------------------------------------------
// Static fallback dirs
// ---------------------------------------------------------------------------------------------

function staticFallbackDirs(stateDir?: string): Promise<string[]> {
  const key = stateDir ?? "";
  let cached = fallbackCache.get(key);
  if (!cached) {
    cached = (async () => {
      const home = homedir();
      let native: string[] = [];
      if (stateDir) {
        try {
          native = nativeToolsPathDirs(stateDir);
        } catch {
          native = [];
        }
      }
      const candidates = [
        join(home, ".local", "share", "mise", "shims"),
        join(home, ".mise", "shims"),
        join(home, ".local", "bin"),
        join(home, ".bun", "bin"),
        join(home, ".cargo", "bin"),
        "/opt/homebrew/bin",
        "/opt/homebrew/sbin",
        "/home/linuxbrew/.linuxbrew/bin",
        "/usr/local/bin",
        dirname(process.execPath),
        ...native,
        "/usr/bin",
        "/bin",
        "/usr/sbin",
        "/sbin",
      ];
      const exists = await Promise.all(candidates.map(isDirectory));
      return dedupe([candidates.filter((_, i) => exists[i])]);
    })();
    fallbackCache.set(key, cached);
  }
  return cached;
}

// ---------------------------------------------------------------------------------------------
// mise
// ---------------------------------------------------------------------------------------------

const MISE_LOG_INTERVAL_MS = 5 * 60_000;
const miseLoggedAt = new Map<string, number>();

async function findExecutable(name: string, dirs: string[]): Promise<string | undefined> {
  const found = await Promise.all(dirs.map((d) => isExecutableFile(join(d, name))));
  const dir = dirs[found.indexOf(true)];
  return dir === undefined ? undefined : join(dir, name);
}

async function miseEnv(
  bin: string,
  cwd: string,
  env: NodeJS.ProcessEnv,
  signal?: AbortSignal,
): Promise<{ vars: Record<string, string>; path: string[] } | { failed: string }> {
  const r = await spawnGroup(bin, ["env", "--json"], {
    cwd,
    env,
    timeoutMs: 5000,
    maxOutputBytes: 1024 * 1024,
    signal,
    captureStderr: false,
  });
  if (r.stopped === "cancelled" && signal?.aborted) throw abortReason(signal);
  if (r.stopped) return { failed: r.stopped };
  if (r.exitCode !== 0) return { failed: `exit:${r.exitCode}` };
  let parsed: unknown;
  try {
    parsed = JSON.parse(r.stdout);
  } catch {
    return { failed: "json" };
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return { failed: "json" };
  const entries = Object.entries(parsed as Record<string, unknown>);
  if (entries.some(([, v]) => typeof v !== "string")) return { failed: "json" };
  const vars: Record<string, string> = {};
  let path: string[] = [];
  for (const [k, v] of entries as Array<[string, string]>) {
    if (k === "PATH") path = await sanitizePath(v);
    else if (ENV_NAME_RE.test(k) && k !== "__proto__" && !k.startsWith("HARNESS_") && !v.includes("\0")) vars[k] = v;
  }
  return { vars, path };
}

function logMiseFailure(reason: string, cwd: string): void {
  const now = Date.now();
  if (now - (miseLoggedAt.get(reason) ?? 0) < MISE_LOG_INTERVAL_MS) return;
  miseLoggedAt.set(reason, now);
  console.error(`[gate-env] 'mise env --json' failed (${reason}) in ${cwd}; continuing without mise`);
}

// ---------------------------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------------------------

/** Daemon environment without `HARNESS_*`: gate commands get only the harness variables their caller passes as `extraEnv`. */
function inheritedEnv(): NodeJS.ProcessEnv {
  return Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith("HARNESS_")));
}

/**
 * Resolves the shell and environment for one batch of gate commands. Throws GateConfigError when
 * `goals.gateShell` / `goals.gateEnv` are invalid (fail closed).
 */
export async function resolveGateEnv(o: {
  cwd: string;
  config?: GateRuntimeConfig;
  extraEnv?: Record<string, string>;
  signal?: AbortSignal;
}): Promise<ResolvedGateEnv> {
  o.signal?.throwIfAborted();
  const config = o.config ?? {};
  const baseEnv = inheritedEnv();
  const norm = normalizeGateConfig({ gateShell: config.gateShell, gateEnv: config.gateEnv });

  if (!isBlank(config.gateShell) && (!norm.gateShell || !(await isExecutableFile(norm.gateShell)))) {
    const raw = config.gateShell;
    // Never echo non-string input: an object could embed arbitrary fields or secrets.
    const shown =
      typeof raw === "string"
        ? `'${(CONTROL_RE.test(raw) ? JSON.stringify(raw) : raw).slice(0, 200)}'`
        : `of type ${raw === null ? "null" : Array.isArray(raw) ? "array" : typeof raw}`;
    throw new GateConfigError(`goals.gateShell ${shown} is not an executable absolute path (${OPERATOR_ERROR})`);
  }
  const envIssues = norm.issues.filter((i) => i.startsWith("goals.gateEnv"));
  if (envIssues.length > 0) throw new GateConfigError(`${envIssues.join("; ")} (${OPERATOR_ERROR})`);

  const shell = norm.gateShell ?? "/bin/sh";
  const { PATH: gatePath, ...gateVars } = norm.gateEnv ?? {};
  const gateDirs = pathSegments(gatePath);
  const [login, fallbacks] = await Promise.all([loginPath(o.signal), staticFallbackDirs(config.stateDir)]);
  const daemonDirs = pathSegments(process.env.PATH);
  const basePath = dedupe([gateDirs, login.dirs, daemonDirs, fallbacks]);

  let miseDesc = "absent";
  let miseVars: Record<string, string> = {};
  let miseDirs: string[] = [];
  const miseBin = await findExecutable("mise", basePath);
  if (miseBin) {
    const r = await miseEnv(miseBin, o.cwd, { ...baseEnv, ...gateVars, PATH: basePath.join(delimiter) }, o.signal);
    if ("failed" in r) {
      miseDesc = `failed:${r.failed}`;
      logMiseFailure(r.failed, o.cwd);
    } else {
      miseDesc = "ok";
      miseVars = r.vars;
      miseDirs = r.path;
    }
  }

  const finalPath = dedupe([gateDirs, miseDirs, login.dirs, daemonDirs, fallbacks]);
  return {
    shell,
    env: { ...baseEnv, ...miseVars, ...gateVars, ...o.extraEnv, PATH: finalPath.join(delimiter) },
    provenance: `shell=${shell} ${login.desc} mise=${miseDesc} dirs=${finalPath.length}`,
  };
}

/**
 * Runs `command` with `<resolved.shell> -c` in its own process group (stdin closed). Throws
 * GateCommandError on non-zero exit, timeout, cancellation, output overflow or spawn failure.
 */
export async function runGateCommand(
  command: string,
  o: {
    cwd: string;
    resolved: ResolvedGateEnv;
    timeoutMs: number;
    maxOutputBytes: number;
    signal?: AbortSignal;
    label?: string;
  },
): Promise<{ stdout: string; stderr: string }> {
  o.signal?.throwIfAborted();
  const label = o.label ?? "Verification gate";
  const r = await spawnGroup(o.resolved.shell, ["-c", command], {
    cwd: o.cwd,
    env: o.resolved.env,
    timeoutMs: o.timeoutMs,
    maxOutputBytes: o.maxOutputBytes,
    signal: o.signal,
    captureStderr: true,
  });
  if (r.stopped) {
    const message =
      r.stopped === "cancelled"
        ? `${label} cancelled`
        : r.stopped === "timeout"
          ? `${label} timed out`
          : r.stopped === "output_limit"
            ? `${label} exceeded its output limit`
            : (r.spawnError?.message ?? `${label} could not start`);
    throw new GateCommandError(message, r.stdout, r.stderr, r.exitCode, r.stopped);
  }
  if (r.exitCode !== 0) {
    const message = r.stderr.trim() || r.stdout.trim() || `Gate exited with status ${r.exitCode}`;
    throw new GateCommandError(message, r.stdout, r.stderr, r.exitCode, "exit");
  }
  return { stdout: r.stdout, stderr: r.stderr };
}
