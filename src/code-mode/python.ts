import { spawn } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, readdir, realpath, rename, rm, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { defaultStateDir } from "../config.js";

// Supply chain: every executable we run unsandboxed is downloaded from upstream over HTTPS and
// checked against these SHA-256 pins before it is unpacked. Bump the pins together (see docs/code-mode-python.md).
const UV_VERSION = "0.13.9";
const PYTHON_VERSION = "3.13.18";
const PBS_RELEASE = "20261110";
const TARGETS: Record<string, { triple: string; uv: string; python: string }> = {
  arm64: {
    triple: "aarch64-apple-darwin",
    uv: "5aeca67f2283f75235de40d0bd9c0a5bbc5fce23b6d48b3bf9fa1b2db08382ca",
    python: "1afd3983237c2a10277345f235be730e9cb49a123e0574ed252dbf3a841a24e4",
  },
  x64: {
    triple: "x86_64-apple-darwin",
    uv: "617a3211261b0fc739562f0071aa34e9739797c510208ede8e2977c30903d041",
    python: "a46bcdeecd28ba253522b321dcb3806da1b0ff581479fc1d27e9613e3ac44764",
  },
};
const PYPI_INDEX = "https://pypi.org/simple";
const PROVISION_TIMEOUT_MS = 10 * 60_000;
const STALE_STAGING_MS = 60 * 60_000;
const MAX_PACKAGES = 32;
const TAR = "/usr/bin/tar";

export interface PythonRuntime {
  /** Absolute path of the managed interpreter. */
  exe: string;
  /** Read-only trees the sandboxed interpreter may read and map: the CPython install and the package environment. */
  roots: string[];
  sitePackages?: string;
}

export function pythonCacheRoot(override?: string): string {
  return override ?? join(defaultStateDir(), "code-mode", "python");
}

const PACKAGE_PIN = /^([A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?)==([0-9A-Za-z][0-9A-Za-z.+!_-]*)$/;

// Only exact `name==version` pins: no URLs, extras, ranges or wildcards.
function normalizePackages(packages: string[]): string[] {
  if (packages.length > MAX_PACKAGES) throw new Error(`At most ${MAX_PACKAGES} packages are allowed`);
  const pins = new Map<string, string>();
  for (const spec of packages) {
    const match = PACKAGE_PIN.exec(spec.trim());
    if (!match) throw new Error(`Package "${spec}" must be an exact pin like "name==1.2.3"`);
    const name = match[1]!.toLowerCase().replace(/[-_.]+/g, "-");
    const version = match[2]!;
    if (pins.has(name) && pins.get(name) !== version) throw new Error(`Conflicting pins for ${name}`);
    pins.set(name, version);
  }
  return [...pins].map(([name, version]) => `${name}==${version}`).sort();
}

function run(cmd: string, args: string[], opts: { cwd: string; env: Record<string, string>; signal: AbortSignal }): Promise<string> {
  return new Promise((resolve, reject) => {
    const aborted = () => opts.signal.reason ?? new Error("aborted");
    if (opts.signal.aborted) return reject(aborted());
    const child = spawn(cmd, args, { cwd: opts.cwd, env: opts.env, detached: true, stdio: ["ignore", "pipe", "pipe"] });
    const kill = () => { try { process.kill(-child.pid!, "SIGKILL"); } catch { child.kill("SIGKILL"); } };
    opts.signal.addEventListener("abort", kill, { once: true });
    let tail = "";
    for (const stream of [child.stdout!, child.stderr!]) {
      stream.setEncoding("utf8");
      stream.on("data", (s: string) => { tail = (tail + s).slice(-4096); });
    }
    child.once("error", reject);
    child.once("close", code => {
      opts.signal.removeEventListener("abort", kill);
      if (opts.signal.aborted) reject(aborted());
      else if (code === 0) resolve(tail);
      else reject(new Error(`${cmd.split("/").pop()} ${args[0] ?? ""} exited with status ${code}: ${tail.trim()}`));
    });
  });
}

async function download(url: string, sha256: string, dest: string, signal: AbortSignal): Promise<void> {
  const response = await fetch(url, { signal });
  if (!response.ok) throw new Error(`Download failed (${response.status}): ${url}`);
  const bytes = new Uint8Array(await response.arrayBuffer());
  const digest = createHash("sha256").update(bytes).digest("hex");
  if (digest !== sha256) throw new Error(`Checksum mismatch for ${url}: expected ${sha256}, got ${digest}`);
  await writeFile(dest, bytes, { mode: 0o600 });
}

// Build in a private staging directory and rename into place: the final path only ever holds a complete tree,
// concurrent builders race harmlessly (the loser discards its copy) and a killed build leaves only staging debris.
async function materialize(root: string, final: string, complete: string[], build: (staging: string) => Promise<void>): Promise<void> {
  const isComplete = () => complete.every(file => existsSync(join(final, file)));
  if (isComplete()) return;
  if (existsSync(final)) await rm(final, { recursive: true, force: true }); // damaged out of band; builds never leave partial finals
  const staging = join(root, ".staging", `${final.split("/").pop()}-${process.pid}-${randomBytes(4).toString("hex")}`);
  await mkdir(staging, { recursive: true, mode: 0o700 });
  try {
    await build(staging);
    await mkdir(join(final, ".."), { recursive: true, mode: 0o700 });
    try { await rename(staging, final); }
    catch (e) { if (!isComplete()) throw e; }
  } finally {
    await rm(staging, { recursive: true, force: true });
  }
}

async function cleanStaging(root: string): Promise<void> {
  const dir = join(root, ".staging");
  for (const name of await readdir(dir).catch(() => [] as string[])) {
    const info = await stat(join(dir, name)).catch(() => undefined);
    if (info && Date.now() - info.mtimeMs > STALE_STAGING_MS) await rm(join(dir, name), { recursive: true, force: true });
  }
}

function toolEnv(root: string): Record<string, string> {
  const env: Record<string, string> = {
    PATH: "/usr/bin:/bin", HOME: join(root, "home"), TMPDIR: join(root, "tmp"),
    UV_CACHE_DIR: join(root, "uv-cache"), UV_NO_CONFIG: "1", UV_PYTHON_DOWNLOADS: "never",
    UV_NO_PROGRESS: "1", UV_NO_ENV_FILE: "1",
  };
  for (const key of ["HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY", "ALL_PROXY", "SSL_CERT_FILE", "SSL_CERT_DIR"]) {
    const value = process.env[key] ?? process.env[key.toLowerCase()];
    if (value) env[key] = value;
  }
  return env;
}

async function provision(root: string, packages: string[], signal: AbortSignal): Promise<PythonRuntime> {
  const target = TARGETS[process.arch];
  if (process.platform !== "darwin" || !target) throw new Error(`Managed Python is not supported on ${process.platform}/${process.arch}`);
  await Promise.all(["home", "tmp", "locks"].map(name => mkdir(join(root, name), { recursive: true, mode: 0o700 })));
  await cleanStaging(root);
  const env = toolEnv(root);
  const exec = (cmd: string, args: string[], cwd: string) => run(cmd, args, { cwd, env, signal });

  const uvDir = join(root, "uv", `${UV_VERSION}-${target.triple}`);
  await materialize(root, uvDir, ["uv"], async staging => {
    const archive = join(staging, "uv.tar.gz");
    await download(`https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-${target.triple}.tar.gz`, target.uv, archive, signal);
    await exec(TAR, ["-xzf", archive, "-C", staging, "--strip-components=1"], staging);
    await rm(archive);
    const version = await exec(join(staging, "uv"), ["--version"], staging);
    if (!version.includes(UV_VERSION)) throw new Error(`Unexpected uv version: ${version.trim()}`);
  });

  const pyDir = join(root, "python", `${PYTHON_VERSION}+${PBS_RELEASE}-${target.triple}`);
  await materialize(root, pyDir, ["bin/python3"], async staging => {
    const archive = join(staging, "python.tar.gz");
    const url = `https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_RELEASE}/cpython-${PYTHON_VERSION}%2B${PBS_RELEASE}-${target.triple}-install_only.tar.gz`;
    await download(url, target.python, archive, signal);
    await exec(TAR, ["-xzf", archive, "-C", staging, "--strip-components=1"], staging);
    await rm(archive);
    const version = await exec(join(staging, "bin/python3"), ["-I", "-S", "-c", "import sys; print('.'.join(map(str, sys.version_info[:3])))"], staging);
    if (version.trim() !== PYTHON_VERSION) throw new Error(`Unexpected CPython version: ${version.trim()}`);
  });

  const uv = await realpath(join(uvDir, "uv"));
  const runtime = await realpath(pyDir);
  const exe = await realpath(join(runtime, "bin/python3"));
  if (!packages.length) return { exe, roots: [runtime] };

  const key = createHash("sha256").update([UV_VERSION, PYTHON_VERSION, PBS_RELEASE, target.triple, ...packages].join("\n")).digest("hex").slice(0, 32);
  const lock = join(root, "locks", `${key}.txt`);
  const common = ["--python", exe, "--no-config", "--only-binary", ":all:", "--index-url", PYPI_INDEX];
  // Resolution reads wheel metadata only: no build backend, setup.py or package code runs here (wheels only, no sdist builds).
  if (!existsSync(lock)) {
    const staging = join(root, ".staging", `lock-${key}-${process.pid}-${randomBytes(4).toString("hex")}`);
    await mkdir(staging, { recursive: true, mode: 0o700 });
    try {
      await writeFile(join(staging, "requirements.in"), packages.join("\n") + "\n");
      await exec(uv, ["pip", "compile", ...common, "--generate-hashes", "--no-header", "--output-file", "requirements.txt", "requirements.in"], staging);
      await rename(join(staging, "requirements.txt"), lock);
    } finally {
      await rm(staging, { recursive: true, force: true });
    }
  }

  const envDir = join(root, "envs", key);
  await materialize(root, envDir, ["site-packages", "requirements.txt"], async staging => {
    // The lock pins every wheel by hash, so installing it unpacks files and never runs package code.
    await exec(uv, ["pip", "install", ...common, "--require-hashes", "--no-deps", "--link-mode", "copy", "--target", join(staging, "site-packages"), "-r", lock], staging);
    await writeFile(join(staging, "requirements.txt"), await readFile(lock, "utf8"));
  });
  const resolved = await realpath(envDir);
  return { exe, roots: [runtime, resolved], sitePackages: join(resolved, "site-packages") };
}

interface Job { promise: Promise<PythonRuntime>; ctrl: AbortController; waiters: number }
const jobs = new Map<string, Job>();

/**
 * Returns a provisioned CPython (and package environment). Identical concurrent requests share one build, which
 * is cancelled only when every caller has aborted; completed work is reused from the on-disk cache.
 */
export async function providePython(packages: string[], cacheDir?: string, signal?: AbortSignal): Promise<PythonRuntime> {
  const specs = normalizePackages(packages);
  const base = pythonCacheRoot(cacheDir);
  await mkdir(base, { recursive: true, mode: 0o700 });
  const root = await realpath(base);
  const id = `${root}\0${specs.join("\n")}`;
  let job = jobs.get(id);
  if (!job) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(new Error("Python provisioning timed out")), PROVISION_TIMEOUT_MS);
    const promise = provision(root, specs, ctrl.signal).finally(() => { clearTimeout(timer); if (jobs.get(id)?.ctrl === ctrl) jobs.delete(id); });
    promise.catch(() => {});
    job = { promise, ctrl, waiters: 0 };
    jobs.set(id, job);
  }
  const current = job;
  current.waiters++;
  return new Promise<PythonRuntime>((resolve, reject) => {
    let left = false;
    const leave = () => {
      if (left) return;
      left = true;
      signal?.removeEventListener("abort", onAbort);
      if (--current.waiters === 0 && !current.ctrl.signal.aborted) {
        if (jobs.get(id) === current) jobs.delete(id); // a cancelled build must not be joined by later callers
        current.ctrl.abort(new Error("Python provisioning cancelled"));
      }
    };
    const onAbort = () => { leave(); reject(new Error("Code Mode cancelled")); };
    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted) return onAbort();
    current.promise.then(value => { leave(); resolve(value); }, error => { leave(); reject(error); });
  });
}

// Python side of the Code Mode IPC (newline-delimited JSON over the one authorized Unix socket).
export const pythonBridge = (socket: string) => `import json, socket
class ToolError(RuntimeError):
    pass
_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
_sock.connect(${JSON.stringify(socket)})
_reader = _sock.makefile('rb')
_next = 0
def _send(message):
    global _next
    _next += 1
    line = json.dumps({'id': _next, **message}, allow_nan=False) + '\\n'
    _sock.sendall(line.encode('utf-8'))
    reply = _reader.readline()
    if not reply:
        raise ToolError('Code Mode IPC closed')
    reply = json.loads(reply)
    if reply.get('ok'):
        return reply.get('value')
    raise ToolError(reply.get('error'))
def call(name, args=None, /, **fields):
    return _send({'name': name, 'args': {**(args or {}), **fields}})
def output(value):
    _send({'kind': 'output', 'value': value})
`;

// Defence in depth only: the OS sandbox profile is the real boundary and applies to every child process.
export const pythonGuard = `import builtins, io, os, socket, subprocess
def _deny(*args, **kwargs):
    raise PermissionError('Blocked by Code Mode sandbox')
for _name in ['system', 'fork', 'forkpty', 'popen', 'posix_spawn', 'posix_spawnp', 'spawnl', 'spawnle', 'spawnlp',
              'spawnlpe', 'spawnv', 'spawnve', 'spawnvp', 'spawnvpe', 'execl', 'execle', 'execlp', 'execlpe', 'execv',
              'execve', 'execvp', 'execvpe', 'chmod', 'chown', 'link', 'mkdir', 'makedirs', 'mkfifo', 'mknod', 'remove',
              'removedirs', 'rename', 'renames', 'replace', 'rmdir', 'symlink', 'truncate', 'unlink', 'utime']:
    if hasattr(os, _name):
        setattr(os, _name, _deny)
subprocess.Popen = _deny
for _name in ['connect', 'connect_ex', 'bind', 'listen']:
    setattr(socket.socket, _name, _deny)
socket.create_connection = _deny
socket.create_server = _deny
_open = builtins.open
_os_open = os.open
def _checked_open(file, mode='r', *args, **kwargs):
    if any(flag in str(mode) for flag in 'wax+'):
        _deny()
    return _open(file, mode, *args, **kwargs)
def _checked_os_open(path, flags, *args, **kwargs):
    if flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
        _deny()
    return _os_open(path, flags, *args, **kwargs)
builtins.open = _checked_open
io.open = _checked_open
os.open = _checked_os_open
`;

export const pythonEntry = (sitePackages: string | undefined) => `import site, sys, traceback
import _cm_bridge, _cm_guard
${sitePackages ? `site.addsitedir(${JSON.stringify(sitePackages)})` : ""}
with open('script.py', encoding='utf-8') as _file:
    _source = _file.read()
try:
    exec(compile(_source, 'script.py', 'exec'), {'__name__': '__main__', '__file__': 'script.py'})
except SystemExit:
    raise
except BaseException as _error:
    traceback.print_exception(type(_error), _error, _error.__traceback__.tb_next)
    sys.exit(1)
`;

export function pythonModule(names: string[], declarations: string): { module: string; stubs: string } {
  const functions = names.map(name => `def ${name}(input=None, /, **fields):\n    return call(${JSON.stringify(name)}, input, **fields)\n`);
  const exported = [...names, "call", "output", "ToolError"].map(name => JSON.stringify(name)).join(", ");
  return {
    module: `from _cm_bridge import call, output, ToolError\n__all__ = [${exported}]\n${functions.join("")}`,
    stubs: declarations,
  };
}
