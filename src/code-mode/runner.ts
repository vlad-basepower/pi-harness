import { spawn, type ChildProcess } from "node:child_process";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { createServer, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { nextUniqueFileName } from "../control-plane/ids.js";
import type { ToolDefinition } from "../runners/types.js";
import { providePython, pythonBridge, pythonEntry, pythonGuard, pythonModule, type PythonRuntime } from "./python.js";
import { guard, pythonSandboxCommand, pythonSandboxEnv, sandboxCommand, sandboxEnv } from "./sandbox.js";
import { generatePythonDeclarations, generateSdkDeclarations, pythonTools } from "./sdk.js";

export type CodeLanguage = "typescript" | "python";

export interface CodeExecToolEvent {
  phase: "start" | "end"; name: string; args: Record<string, unknown>; ok?: boolean; error?: string; via: "code_exec";
}
export interface CodeExecOptions {
  code: string; tools: ToolDefinition[];
  /** Default TypeScript. Python runs a harness-managed CPython under the same sandbox and IPC. */
  language?: CodeLanguage;
  /** Python only: exact `name==version` wheel pins. */
  packages?: string[];
  pythonCacheDir?: string;
  callTool: (name: string, args: Record<string, unknown>) => Promise<unknown>;
  signal?: AbortSignal; timeoutMs?: number; maxOutputChars?: number;
  artifactsDir: string; scratchRoot?: string;
  saveOutput?: (name: string, json: string) => string | Promise<string>;
  onToolEvent?: (event: CodeExecToolEvent) => void;
}
export interface CodeExecResult {
  ok: boolean; output?: unknown; preview: string; artifactPath?: string; logs: string; toolCalls: number;
  error?: string; timedOut?: boolean; cancelled?: boolean;
}

export function timeoutMs(ms: number | undefined, max = 600_000): number {
  return Math.max(1, Math.min(Number.isFinite(ms) ? ms! : 60_000, max, 600_000));
}

// A referenced socket keeps Bun alive until all pending requests (including synchronous output()) are acknowledged.
function bridge(socket: string): string {
  return `import { createConnection } from 'node:net';
const sock = createConnection(${JSON.stringify(socket)});
await new Promise((res,rej) => {sock.once('connect',res); sock.once('error',rej)});
const pending = new Map(); let next = 0, buf = '';
sock.setEncoding('utf8'); sock.unref();
sock.on('close', () => { for (const p of pending.values()) p.reject(new Error('Code Mode IPC closed')); pending.clear() });
sock.on('data', chunk => { buf += chunk; let n;
  while ((n = buf.indexOf('\\n')) >= 0) {
    const r = JSON.parse(buf.slice(0,n)); buf = buf.slice(n+1);
    const p = pending.get(r.id); pending.delete(r.id);
    if (r.ok) p.resolve(r.value); else p.reject(new Error(r.error));
  } if (!pending.size) sock.unref();
});
function send(msg) {
  const id = ++next, line = JSON.stringify({id,...msg})+'\\n';
  sock.ref(); return new Promise((resolve,reject) => {pending.set(id,{resolve,reject});sock.write(line)});
}
export const call = (name,args={}) => send({name,args});
export const output = value => { void send({kind:'output',value}) };
globalThis.output = output;
`;
}

const message = (err: unknown) => err instanceof Error ? err.message : String(err);
const IPC_BYTES = 16 * 1024 * 1024;

export async function runCodeExec(opts: CodeExecOptions): Promise<CodeExecResult> {
  const result: CodeExecResult = { ok: false, preview: "", logs: "", toolCalls: 0 };
  let python: PythonRuntime | undefined;
  if (opts.language === "python") {
    // Provisioning happens before the script timer starts: a cold first call downloads pinned tools and wheels.
    try { python = await providePython(opts.packages ?? [], opts.pythonCacheDir, opts.signal); }
    catch (e) {
      if (opts.signal?.aborted) result.cancelled = true;
      result.error = opts.signal?.aborted ? "Code Mode cancelled" : `Python provisioning failed: ${message(e)}`;
      return result;
    }
  }
  const names = new Set(opts.tools.map(t => t.name));
  const sockets = new Set<Socket>();
  let pending = 0, pendingBytes = 0;
  let child: ChildProcess | undefined, dir: string | undefined, output: unknown, stopped = false;
  function kill() {
    if (child?.pid) try { process.kill(-child.pid, "SIGKILL"); } catch { child.kill("SIGKILL"); }
  }
  function stop(reason: string) {
    if (stopped) return;
    stopped = true; result.error = reason; kill();
    for (const s of sockets) s.destroy();
  }
  const abort = () => { if (!stopped) { result.cancelled = true; stop("Code Mode cancelled"); } };
  const timer = setTimeout(() => { if (!stopped) { result.timedOut = true; stop("Code Mode timed out"); } }, timeoutMs(opts.timeoutMs));
  opts.signal?.addEventListener("abort", abort, { once: true });
  if (opts.signal?.aborted) abort();

  const server = createServer(socket => {
    if (stopped || sockets.size) { socket.destroy(); return; }
    sockets.add(socket);
    socket.on("error", () => socket.destroy());
    socket.once("close", () => sockets.delete(socket));
    let parts: Buffer[] = [], bytes = 0;
    socket.on("data", (chunk: Buffer) => {
      let start = 0;
      while (start < chunk.length && !stopped) {
        const end = chunk.indexOf(10, start);
        const part = chunk.subarray(start, end < 0 ? chunk.length : end);
        bytes += part.length;
        if (bytes > IPC_BYTES) { stop("Code Mode IPC message exceeds 16 MiB"); return; }
        parts.push(part);
        if (end < 0) return;
        const size = bytes;
        if (pending >= 64 || pendingBytes + size > IPC_BYTES) { stop("Too many outstanding Code Mode requests"); return; }
        pending++; pendingBytes += size;
        void dispatch(Buffer.concat(parts, bytes).toString()).catch(e => stop(message(e)))
          .finally(() => { pending--; pendingBytes -= size; });
        parts = []; bytes = 0; start = end + 1;
      }
    });

    async function dispatch(line: string) {
      const { id, name, args, kind, value } = JSON.parse(line);
      function reply(payload: Record<string, unknown>) {
        if (stopped || socket.destroyed) return;
        const json = JSON.stringify({ id, ...payload }) + "\n";
        if (Buffer.byteLength(json) + socket.writableLength > IPC_BYTES) throw new Error("Code Mode IPC reply exceeds 16 MiB buffer");
        socket.write(json);
      }
      if (!Number.isSafeInteger(id)) throw new Error("Invalid IPC request id");
      if (kind === "output") { output = value; reply({ ok: true }); return; }
      if (typeof name !== "string" || !args || Array.isArray(args) || typeof args !== "object") throw new Error("Invalid tool call");
      const event = { name, args, via: "code_exec" as const };
      result.toolCalls++;
      opts.onToolEvent?.({ ...event, phase: "start" });
      try {
        if (name === "code_exec" || name === "tool_search") throw new Error(`Recursive ${name} calls are not allowed inside code_exec`);
        if (!names.has(name)) throw new Error(`Tool not allowed: ${name}`);
        const value = await opts.callTool(name, args);
        reply({ ok: true, value });
        opts.onToolEvent?.({ ...event, phase: "end", ok: true });
      } catch (e) {
        const error = message(e);
        opts.onToolEvent?.({ ...event, phase: "end", ok: false, error });
        reply({ ok: false, error });
      }
    }
  });

  try {
    if (stopped) return result;
    dir = await realpath(await mkdtemp(join(opts.scratchRoot ?? tmpdir(), "cm-")));
    const socket = join(dir, "ipc");
    if (Buffer.byteLength(socket) >= 104) throw new Error("Code Mode scratch path too long for Unix socket; use a shorter scratchRoot");
    let cmd: string | undefined, args: string[], env: Record<string, string>;
    if (python) {
      [cmd, ...args] = pythonSandboxCommand(python.exe, python.roots, dir, socket);
      env = pythonSandboxEnv(dir);
      const { module, stubs } = pythonModule(pythonTools(opts.tools).map(t => t.name), generatePythonDeclarations(opts.tools));
      await Promise.all([
        writeFile(join(dir, "_cm_bridge.py"), pythonBridge(socket)), writeFile(join(dir, "_cm_guard.py"), pythonGuard),
        writeFile(join(dir, "_cm_entry.py"), pythonEntry(python.sitePackages)), writeFile(join(dir, "script.py"), opts.code),
        writeFile(join(dir, "harness.py"), module), writeFile(join(dir, "harness.pyi"), stubs),
      ]);
    } else {
      [cmd, ...args] = sandboxCommand(await realpath(process.execPath), dir, socket);
      env = sandboxEnv(dir);
      const pkg = join(dir, "node_modules/harness");
      await mkdir(pkg, { recursive: true });
      const exports = [...new Set([...names, "code_exec", "tool_search"])].filter(n => n !== "output")
        .map((n, i) => `const t${i} = args => call(${JSON.stringify(n)}, args); export { t${i} as ${JSON.stringify(n)} };`);
      await Promise.all([
        writeFile(join(dir, "bridge.mjs"), bridge(socket)), writeFile(join(dir, "guard.mjs"), guard),
        writeFile(join(dir, "script.ts"), opts.code),
        writeFile(join(pkg, "package.json"), '{"name":"harness","type":"module","main":"index.mjs","types":"index.d.ts"}'),
        writeFile(join(pkg, "index.d.ts"), generateSdkDeclarations(opts.tools)),
        writeFile(join(pkg, "index.mjs"), `import {call,output} from '../../bridge.mjs'; export {output};\n${exports.join("\n")}`),
      ]);
    }
    await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(socket, resolve); });
    if (stopped) return result;
    child = spawn(cmd!, args, { cwd: dir, env, detached: true, stdio: ["ignore", "pipe", "pipe"] });
    let tail = "";
    for (const stream of [child.stdout!, child.stderr!]) {
      stream.setEncoding("utf8");
      stream.on("data", (s: string) => { result.logs += s.slice(0, Math.max(0, 8192 - result.logs.length)); tail = (tail + s).slice(-2048); });
    }
    const status = await new Promise<number | null>((resolve, reject) => { child!.once("error", reject); child!.once("close", resolve); });
    child = undefined;
    if (stopped) return result;
    clearTimeout(timer); opts.signal?.removeEventListener("abort", abort);
    if (status !== 0) throw new Error(tail.trim() || `${python ? "Python" : "Bun"} exited with status ${status}`);
    const json = JSON.stringify(output) ?? "";
    const limit = Math.max(0, opts.maxOutputChars ?? 20_000);
    result.preview = json.slice(0, limit);
    if (json.length > limit) {
      const name = nextUniqueFileName(opts.saveOutput ? null : opts.artifactsDir, "code-exec", ".json");
      if (opts.saveOutput) {
        result.artifactPath = await opts.saveOutput(name, json);
      } else {
        await mkdir(opts.artifactsDir, { recursive: true });
        const path = join(opts.artifactsDir, name);
        await writeFile(path, json, { flag: "wx", mode: 0o600 });
        result.artifactPath = path;
      }
    } else if (output !== undefined) result.output = output;
    result.ok = true;
  } catch (e) { result.error ??= message(e); }
  finally {
    stopped = true; clearTimeout(timer); opts.signal?.removeEventListener("abort", abort); kill();
    for (const socket of sockets) socket.destroy();
    if (server.listening) await new Promise<void>(resolve => server.close(() => resolve()));
    if (dir) await rm(dir, { recursive: true, force: true });
  }
  return result;
}
