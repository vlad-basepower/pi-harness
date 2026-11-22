# Python in Code Mode

`code_exec` runs TypeScript on Bun by default. Set `language: "python"` to run Python instead. Python uses the
same host dispatch, IPC, permissions, timeout, logs, error flag, artifacts and tool events as TypeScript; only the
interpreter differs. Omitting `language` is exactly the old behaviour, and Python needs no installer changes beyond
the Bun the harness already runs under.

## Writing a Python script

```json
{ "language": "python", "packages": ["pyyaml==6.0.3"], "timeout_s": 60,
  "code": "import yaml\nfrom harness import echo, output\noutput(yaml.safe_load('a: 1'))" }
```

- Import allowed tools with `from harness import name, output`. Call with a dict or keywords: `echo({"text": "x"})`
  or `echo(text="x")`. `harness.call(name, args)` reaches any allowed tool by exact name (including names that are
  not valid Python identifiers).
- `output(value)` returns JSON. Results keep the existing shape: `ok`, `error`, `logs` (stdout/stderr), `toolCalls`,
  `preview`/artifact path for oversized output. An uncaught exception is `ok: false` with the traceback as `error`.
  A denied or failing tool raises `harness.ToolError`.
- Discover signatures with `tool_search` and `language: "python"` (same `query`/`names` filters, same
  `mcp__harness__` prefix handling). It returns Python stubs and the import note. The code_exec tool description and
  the Code Mode prompt section mention the option too.

## Declaring dependencies

`packages` is an optional list of **exact `name==version` pins**, wheels only (no ranges, extras, URLs, or source
distributions; at most 32). It is only valid with `language: "python"`. Transitive dependencies are resolved and
pinned automatically. Without `packages` you get the standard library only.

## Where the runtime and packages come from

Everything lives in a harness-private cache, `$PI_HARNESS_HOME/code-mode/python` (default
`~/.pi-harness/code-mode/python`), and is used by absolute path. Nothing on `PATH`, no system Python, and no `uv`
install is required, and `PATH`/`HOME`/Python variables are never inherited.

| Piece | Pin | Source |
| --- | --- | --- |
| uv | 0.13.9 | `github.com/astral-sh/uv/releases` (`uv-<triple>.tar.gz`) |
| CPython | 3.13.18 (python-build-standalone `20261110`, `install_only`) | `github.com/astral-sh/python-build-standalone/releases` |
| Packages | your pins, wheels from `pypi.org/simple` | locked with hashes |

Supported hosts are macOS arm64 and x86_64 (`aarch64-apple-darwin`, `x86_64-apple-darwin`), the same hosts where
the Code Mode sandbox is available. Elsewhere Python fails closed like TypeScript does.

Provisioning is **not sandboxed but runs no package code**:

1. uv and CPython archives are downloaded over HTTPS and verified against SHA-256 pins compiled into
   `src/code-mode/python.ts` before they are unpacked (and the unpacked uv/CPython versions are checked).
2. `uv pip compile --only-binary :all: --generate-hashes` resolves the pins from wheel metadata, writing a
   repeatable lock file (`locks/<key>.txt`, hashes included). The key covers uv, CPython and the sorted pins.
3. `uv pip install --require-hashes --no-deps --only-binary :all:` unpacks the locked wheels into
   `envs/<key>/site-packages`. No sdist builds, `setup.py`, or build backends run.

Scripts and packages then run only in the sandbox, which has no network and no writes (see below). Provisioning
happens before the script timeout starts, so a cold first call can take several seconds; later calls reuse the
cache (about 0.1 s overhead).

Cache behaviour:

- Each artifact is built in `.staging/` and renamed into place, so a final path is always complete. A crashed or
  aborted build leaves only staging debris (stale entries are removed after an hour) and the next call rebuilds.
- Concurrent identical requests share one build; different packages build independently. A build is cancelled only
  when every waiting caller has aborted, and a later call starts a fresh build rather than joining a cancelled one.
  Several processes racing on the same cache are safe: the loser discards its copy.
- A provisioning failure (offline, checksum mismatch, unknown or sdist-only package) returns
  `Python provisioning failed: ...` with `ok: false`; it never silently falls back to unpinned code.

## Sandbox

The interpreter is launched through the same macOS `sandbox-exec` profile as Bun: deny by default, no network except
the single IPC Unix socket, no filesystem writes, reads limited to the scratch directory, system libraries and the
read-only managed CPython and package trees, and exec of only the interpreter itself. The profile is inherited by
child processes, so `subprocess`, `os.system`, `fork` and so on are denied for them too. A startup probe checks
those denials (write, fork/exec, TCP, Unix socket) against the managed interpreter before first use, and Python
code is also guarded in-process as defence in depth. The interpreter runs with `-E -s -B -u`, so environment
variables and user site-packages are ignored.

## Refreshing

- **Rebuild a package set:** delete `locks/<key>.txt` and `envs/<key>` (or the whole cache). The next call
  resolves and installs again. Changing a pin changes the key, so old environments are merely unused.
- **Bump uv, CPython or the release:** edit `UV_VERSION`, `PYTHON_VERSION`, `PBS_RELEASE` and the SHA-256 values in
  `src/code-mode/python.ts` together. Take hashes from the upstream `*.sha256` sidecar for uv and the release's
  `SHA256SUMS` for CPython, and verify them by downloading. Old versions stay in the cache and can be deleted.
- **Reset everything:** remove `$PI_HARNESS_HOME/code-mode/python`.
