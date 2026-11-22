import { spawnSync } from "node:child_process";
import { mkdtempSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const launcher = "/usr/bin/sandbox-exec";

// `roots` are extra read-only runtime trees (e.g. a managed CPython and its site-packages).
function sandboxProfile(exe: string, dir: string, socket: string, roots: string[] = []): string {
  const q = JSON.stringify;
  return `(version 1)
    (deny default)
    (import "dyld-support.sb")
    (allow sysctl-read file-read-metadata)
    (allow file-read* file-map-executable (subpath "/System/Library") (subpath "/usr/lib") (literal ${q(exe)})
      ${roots.map(root => `(subpath ${q(root)})`).join(" ")})
    (allow file-read* (subpath "/usr/share/icu")
      (literal "/dev/null") (literal "/dev/urandom") (literal "/dev/random")
      (subpath ${q(dir)}))
    (allow process-exec (literal ${q(exe)}))
    (allow signal (target self))
    (allow network-outbound (remote unix-socket (path-literal ${q(socket)})))`;
}

function sandboxArgs(bun: string, dir: string, socket: string): string[] {
  return ["-p", sandboxProfile(bun, dir, socket), bun, "--no-install", "--no-env-file"];
}

export function sandboxEnv(dir: string) {
  return { HOME: dir, TMPDIR: dir, PATH: "/usr/bin:/bin" };
}

// Run before installing the JS guard so these checks exercise the OS profile.
const probe = (port: number, socketPath: string) => `
import { writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { connect } from "node:net";
function denied(error) {
  return error?.code === "EPERM" || error?.code === "EACCES";
}
function mustDeny(action) {
  try { action(); } catch (error) { if (denied(error)) return; throw error; }
  throw new Error("Sandbox permitted a restricted operation");
}
mustDeny(() => writeFileSync("write-probe", ""));
mustDeny(() => execFileSync(process.execPath, ["--version"]));
await new Promise((resolve, reject) => {
  const socket = connect(${port}, "127.0.0.1");
  socket.on("connect", () => { socket.destroy(); reject(new Error("TCP permitted")); });
  socket.on("error", error => {
    // Bun can report a denied connect to the listening parent as ECONNREFUSED.
    if (denied(error) || error.code === "ECONNREFUSED") resolve();
    else reject(error);
  });
});
await new Promise((resolve, reject) => {
  const socket = connect(${JSON.stringify(socketPath)});
  socket.on("connect", () => { socket.destroy(); resolve(); });
  socket.on("error", reject);
});
console.log("sandbox probe complete");
`;

let available: boolean | undefined;
export function sandboxAvailable(): boolean {
  if (available !== undefined) return available;
  // Bubblewrap namespaces alone permit spawning children and using loopback.
  // Linux (including NixOS) needs an additional verified kernel policy.
  if (process.platform !== "darwin") return false;
  let dir: string | undefined;
  let listener: Bun.UnixSocketListener<undefined> | undefined;
  let tcp: Bun.TCPSocketListener<undefined> | undefined;
  try {
    dir = realpathSync(mkdtempSync(join(tmpdir(), "cm-")));
    const socket = join(dir, "ipc");
    if (Buffer.byteLength(socket) >= 104) throw new Error("Code Mode IPC path too long");
    listener = Bun.listen({ unix: socket, socket: { data() {} } });
    tcp = Bun.listen({ hostname: "127.0.0.1", port: 0, socket: { data() {} } });
    const args = sandboxArgs(realpathSync(process.execPath), dir, socket);
    const result = spawnSync(launcher, [...args, "-e", probe(tcp.port, socket)], {
      cwd: dir, env: sandboxEnv(dir), timeout: 3000, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"],
    });
    available = result.status === 0 && result.stdout.trim() === "sandbox probe complete";
  } catch {
    available = false;
  } finally {
    tcp?.stop(true);
    listener?.stop(true);
    if (dir) rmSync(dir, { recursive: true, force: true });
  }
  return available;
}

export function sandboxCommand(bun: string, dir: string, socket: string): string[] {
  if (!sandboxAvailable()) throw new Error("Code Mode sandbox unavailable on this host");
  return [launcher, ...sandboxArgs(bun, dir, socket), "--preload", "./guard.mjs", "./script.ts"];
}

// The same OS profile and probe checks as Bun, run against the managed CPython: the profile is inherited by every
// child process, and fork/exec of anything but the interpreter is denied, so Python gets no extra capabilities.
const pythonProbe = (port: number, socketPath: string) => `
import os, socket, subprocess, sys
def denied(error):
    return isinstance(error, PermissionError) or getattr(error, "errno", None) in (1, 13)
def must_deny(action):
    try:
        action()
    except OSError as error:
        if denied(error):
            return
        raise
    raise RuntimeError("Sandbox permitted a restricted operation")
must_deny(lambda: open("write-probe", "w"))
must_deny(lambda: subprocess.run([sys.executable, "--version"]))
must_deny(lambda: os.fork())
tcp = socket.socket()
try:
    tcp.connect(("127.0.0.1", ${port}))
except OSError as error:
    if not (denied(error) or error.errno == 61):
        raise
else:
    raise RuntimeError("TCP permitted")
unix = socket.socket(socket.AF_UNIX)
unix.connect(${JSON.stringify(socketPath)})
print("sandbox probe complete")
`;

export const pythonSandboxEnv = (dir: string) => ({ ...sandboxEnv(dir), PYTHONDONTWRITEBYTECODE: "1" });
const pythonFlags = ["-E", "-s", "-B", "-u", "-X", "utf8"];
const pythonVerified = new Map<string, string | undefined>();

export function pythonSandboxCommand(exe: string, roots: string[], dir: string, socket: string): string[] {
  if (!sandboxAvailable()) throw new Error("Code Mode sandbox unavailable on this host");
  if (!pythonVerified.has(exe)) {
    let probeDir: string | undefined, listener: Bun.UnixSocketListener<undefined> | undefined, tcp: Bun.TCPSocketListener<undefined> | undefined;
    try {
      probeDir = realpathSync(mkdtempSync(join(tmpdir(), "cm-")));
      const probeSocket = join(probeDir, "ipc");
      listener = Bun.listen({ unix: probeSocket, socket: { data() {} } });
      tcp = Bun.listen({ hostname: "127.0.0.1", port: 0, socket: { data() {} } });
      const result = spawnSync(launcher, ["-p", sandboxProfile(exe, probeDir, probeSocket, roots), exe, ...pythonFlags, "-c", pythonProbe(tcp.port, probeSocket)], {
        cwd: probeDir, env: pythonSandboxEnv(probeDir), timeout: 10_000, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"],
      });
      pythonVerified.set(exe, result.status === 0 && result.stdout.trim() === "sandbox probe complete" ? undefined : (result.stderr || result.error?.message || "probe failed").trim().slice(-500));
    } catch (e) {
      pythonVerified.set(exe, e instanceof Error ? e.message : String(e));
    } finally {
      tcp?.stop(true);
      listener?.stop(true);
      if (probeDir) rmSync(probeDir, { recursive: true, force: true });
    }
  }
  const failure = pythonVerified.get(exe);
  if (failure) throw new Error(`Code Mode sandbox probe failed for managed Python: ${failure}`);
  return [launcher, "-p", sandboxProfile(exe, dir, socket, roots), exe, ...pythonFlags, "./_cm_entry.py"];
}

// Import the bridge first: it opens the one authorized socket before networking is disabled.
export const guard = String.raw`
import './bridge.mjs';
import fs from 'node:fs';
import cp from 'node:child_process';
import net from 'node:net';
import tls from 'node:tls';
import http from 'node:http';
import https from 'node:https';
import http2 from 'node:http2';
import dgram from 'node:dgram';
import { syncBuiltinESMExports } from 'node:module';
const deny = () => { throw new Error('Blocked by Code Mode sandbox'); };
function block(obj, names) {
  for (const key of names) if (key in obj) obj[key] = deny;
}
const writeOps = ['appendFile','chmod','chown','copyFile','cp','fchmod','fchown','fdatasync',
  'fsync','ftruncate','futimes','lchmod','lchown','link','lutimes','mkdir','mkdtemp','rename',
  'rm','rmdir','symlink','truncate','unlink','utimes','write','writeFile','writev'];
block(fs, [...writeOps, ...writeOps.map(k => k + 'Sync'), 'createWriteStream','WriteStream']);
block(fs.promises, writeOps);
for (const [obj, key] of [[fs,'open'],[fs,'openSync'],[fs.promises,'open']]) {
  const open = obj[key];
  obj[key] = function(path, flags, ...rest) {
    if (flags != null && !['r','rs','sr',0].includes(flags)) deny();
    return open.call(this,path,flags,...rest);
  };
}
block(cp, ['spawn','spawnSync','exec','execSync','execFile','execFileSync','fork']);
block(net, ['connect','createConnection','createServer']); block(net.Socket.prototype, ['connect']);
block(tls, ['connect','createServer']); block(dgram, ['createSocket']);
for (const obj of [http,https,http2]) block(obj, ['connect','request','get','createServer','createSecureServer']);
block(Bun, ['write','spawn','spawnSync','connect','listen','serve','$','which']);
block(globalThis, ['fetch','WebSocket','Worker']);
syncBuiltinESMExports();
`;
