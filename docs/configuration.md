# Configuration

This reference covers `pi-harness` configuration files, precedence, extra filesystem roots, live reload, effective-settings inspection, CLI commands, and Director tools. For runtime staging and `octofs` installation layout see [Runtime install](./runtime-install.md); for the Code Mode sandbox see [Python in Code Mode](./code-mode-python.md#sandbox); for model ranking see [Model catalog ranking](./model-catalog-ranking.md).

## Config files and precedence

- **Global file**: `$PI_HARNESS_HOME/config.json` (default `~/.pi-harness/config.json`). The state directory itself comes from the `PI_HARNESS_HOME` environment variable; a `stateDir` key in `config.json` does not set it (though a non-string value is still rejected on reload).
- **Repository overlay**: `<repoRoot>/.pi-harness.json`.
- **Precedence** (lowest to highest): built-in defaults, global file, repository overlay.
- **Merge rules**: `codeExec`, `rollover`, `worktree`, `goals`, `events`, `catalog`, and `quotaRetry` merge key by key. Scalars, arrays, and `quotaEndpoint` replace whole (`"quotaEndpoint": null` clears an inherited endpoint). An empty `profiles` array falls back to the built-in default profiles.
- **Overlay snapshot**: `harnessd` loads one repository overlay once at startup (one overlay per daemon, not per project). `pi-harness daemon serve` reads it from its `--repo` path, or else its working directory; a daemon auto-spawned by any other command reads it from that command's working directory, not from the command's `--repo` flag. Editing `.pi-harness.json` requires a daemon restart from the intended directory.
- **Global-only keys**: the repository overlay cannot set these five keys; values placed there are ignored:
  - `worktree.extraReadRoots`
  - `worktree.extraWriteRoots`
  - `goals.gateShell`
  - `goals.gateEnv`
  - `remotes`

Every other key can come from `.pi-harness.json`, including `profiles`, `externalMcpServers`, `worktree.setupCommand`, and `worktree.ciCommands`, which execute commands; start `harnessd` only from repositories whose overlay you trust.

## Key placement

```json
{
  "worktree": {
    "maxConcurrentWorkers": 4,
    "extraReadRoots": ["~/reference-docs"],
    "extraWriteRoots": ["~/harness-reports"]
  },
  "goals": { "maxConcurrentGoals": 4 },
  "rollover": { "reviewerMaxTurns": 30 }
}
```

- A key takes effect only at the path shown in the reference below. `extraReadRoots` and `extraWriteRoots` belong under `worktree`; at the top level they are not read.
- Known keys at an unsupported location (such as top-level `extraReadRoots` or `extraWriteRoots`, a key in the wrong section, or a global-only key in `.pi-harness.json`) are ignored without taking effect. The scan covers the top level and the immediate fields of recognized sections, not array entries or deeper objects. `harnessd` logs a value-free `config warning:` naming the file, key path, and supported location: for the repository overlay at startup, and for the global file at startup and after each accepted reload (a rejected reload leaves the previous warnings in place). It logs `config warnings cleared:` when global warnings clear, and lists active warnings in `configWarnings` from `pi-harness daemon status`.
- Unrecognized keys outside the schema are ignored without warning. After editing, confirm active settings with `pi-harness daemon status` (see [Inspecting effective settings](#inspecting-effective-settings)).

## Extra read and write roots

- Entries are absolute paths, `~`, or `~/...`. On live reload an invalid list is rejected and previous settings stay active; at daemon startup invalid entries are dropped with a message on stderr.
- These keys govern harness-side checks in ACP sessions: structured text reads and writes, permission requests from an agent's native read, search, write, edit, move, and delete tools that the harness classifies, and the restricted-shell path check for read-only agents without shell execution (such as research and plan tasks). Done-Reviewers receive `extraReadRoots`. The Director's `write_note` accepts `extraWriteRoots`.
- These keys are not checked against: shell commands of agents with shell execution enabled (write-capable workers, verify tasks, and Done-Reviewers running in a verification worktree); tools of external MCP servers, including `octofs`; and adapter tools the harness does not classify.
- An agent that receives write roots can also read them. Read-only task agents cannot perform harness-mediated structured writes, and Done-Reviewers receive `extraReadRoots` only.
- Protected paths: harness-mediated structured writes refuse any path containing a `.git` segment and any path under `.harness/worktrees` relative to the admitting root, even inside a configured root. For read-only agents, structured reads, read and search permission checks, and restricted-shell paths refuse the same paths under extra roots, and under the agent's own workspace unless that workspace has a regular `.git` file (as in a linked git worktree); read and search permission checks inspect reported locations and search prefixes, and restricted shell checks the command's working directory and existing canonical operands. A custom `worktree.dir` is not covered by the `.harness/worktrees` rule.
- Claude adapter sessions that are not control-plane-only also receive the resolved roots as Claude Code `additionalDirectories`. If an agent CLI applies its own OS sandbox, that sandbox is configured in that CLI.
- Director `read_file` does not use these roots; its path rules are under [Director tools](#director-tools). Code Mode `code_exec` scripts never receive them: they run in the macOS sandbox described in [Python in Code Mode](./code-mode-python.md#sandbox) and cannot read workspace files directly.
- Reload: read for each newly started worker, task agent, and Done-Reviewer, and on each `write_note` call; no daemon restart is required. Sessions already running keep the roots they started with.
- Least privilege:
  - List the narrowest directory that works.
  - Prefer a read root over a write root.
  - Do not list your home directory, `/`, credential or shell-startup directories, or directories on `PATH`.
  - Remove a root when the work is done and confirm with `pi-harness daemon status`.
- Manual worktrees: the harness runs `git worktree prune` when cleaning up its own worktrees, which drops metadata for any worktree whose directory is missing at that moment. Protect manual worktrees of the same repository that can be temporarily absent with `git worktree lock <path>`.

## Reference

### Top level

| Key | Default | Notes |
| --- | --- | --- |
| `socketPath` | `$XDG_RUNTIME_DIR/pi-harness/daemon.sock` if `$XDG_RUNTIME_DIR` is set and exists; else `<stateDir>/daemon.sock` if that path is shorter than 96 characters; else `daemon.sock` in a per-user temp directory | Fixed at daemon startup. |
| `codeMode` | `collapsed` | `collapsed`, `hybrid`, or `off`. |
| `defaultOrchestratorProfile` | First enabled profile | Default profile ID for Tier 0/1 sessions. |
| `defaultWorkerProfile` | First enabled profile | Default profile ID for Tier 2 workers. |
| `defaultTaskProfile` | First enabled profile | Default profile ID for Tier 3 task agents. |

### `codeExec`

| Key | Default | Notes |
| --- | --- | --- |
| `codeExec.defaultTimeoutMs` | `300000` | Default per-script timeout in ms (positive integer). |
| `codeExec.maxTimeoutMs` | `1800000` | Upper bound for per-script timeout in ms (positive integer). |
| `codeExec.maxOutputChars` | `20000` | Inline output character threshold before spilling to an artifact (positive integer). |

### `rollover`

| Key | Default | Notes |
| --- | --- | --- |
| `rollover.orchestratorThreshold` | `0.72` | Context utilization ratio in `(0, 1]` triggering Tier 1 rollover. |
| `rollover.workerThreshold` | `0.78` | Context utilization ratio in `(0, 1]` triggering Tier 2 rollover. |
| `rollover.orchestratorMaxTurns` | `200` | Tool-call turns before an advisory progress reminder on continuing work (once per runner session when supported; never stops an agent; positive integer). |
| `rollover.workerMaxTurns` | `120` | Tool-call turns before an advisory progress reminder on continuing work (once per runner session when supported; never stops an agent; positive integer). |
| `rollover.taskMaxTurns` | `40` | Tool-call turns before an advisory progress reminder on continuing work (once per runner session when supported; never stops an agent; positive integer). |
| `rollover.reviewerMaxTurns` | `30` | Tool-call turns before an advisory progress reminder on continuing work (once per runner session when supported; never stops an agent; positive integer). |
| `rollover.externalizeBytes` | `2048` | Validated (positive integer) but read by no runtime component; Code Mode spilling uses `codeExec.maxOutputChars`. |
| `rollover.maxGenerations` | `12` | Generation cap per task to stop unbounded rollover loops (positive integer). |

A value in your config file overrides the default; an older file that sets `reviewerMaxTurns` (for example to `12`) keeps that value until you remove the key.

### `worktree`

| Key | Default | Notes |
| --- | --- | --- |
| `worktree.dir` | `.harness/worktrees` | Base worktree directory relative to repo root. |
| `worktree.artifactsDir` | `.harness/artifacts` | Relative to repo root; used only for `code_exec` output spills when no indexed artifact writer is available. Task dossiers and indexed spills always go to `<repoRoot>/.harness/artifacts`. |
| `worktree.setupCommand` | Unset | Bootstrap command run inside each newly created worktree (worker, merge-queue integration, and goal verification checkouts), with `HARNESS_REPO_ROOT` (canonical path of the main checkout) and `HARNESS_WORKTREE_PATH` (the new worktree) set. Use it to copy or link untracked files those checkouts need. When unset, a gitignored `node_modules` is symlinked from the main checkout. |
| `worktree.ciCommands` | Unset | Array of deterministic CI commands run by the merge queue in a detached integration worktree under `<worktree.dir>/_integration/` that holds the merged result, not in the main checkout. `HARNESS_REPO_ROOT` and `HARNESS_WORKTREE_PATH` are set as for `worktree.setupCommand`. |
| `worktree.ciTimeoutMs` | `300000` | Per-command CI timeout in ms (positive integer). |
| `worktree.gitAuthorName` | `pi-harness` | Git author name for automated merge commits. |
| `worktree.gitAuthorEmail` | `harness@local` | Git author email for automated merge commits. |
| `worktree.archiveRetentionDays` | `7` | Days to retain archive git refs before GC (`>= 0`). |
| `worktree.maxConcurrentWorkers` | `4` | Active Tier 2 worker worktree concurrency limit (positive integer); further workers wait in the admission queue. |
| `worktree.extraReadRoots` | Unset | Global-only; see [Extra read and write roots](#extra-read-and-write-roots). |
| `worktree.extraWriteRoots` | Unset | Global-only; see [Extra read and write roots](#extra-read-and-write-roots). |

### `goals`

| Key | Default | Notes |
| --- | --- | --- |
| `goals.defaultMaxReviewCycles` | `4` | Review cycles before escalating to a human (positive integer); copied into each new goal contract. |
| `goals.defaultMaxNoProgressCycles` | `2` | Consecutive no-progress review cycles before escalating (positive integer); copied into each new goal contract. |
| `goals.gateTimeoutMs` | `180000` | Timeout in ms per verification shell gate (positive integer). |
| `goals.autoVerifyOnSettle` | `true` | Automatically start the configured verification workflow when a goal's linked run completes (not when it stops or fails, and not for verification mode `none`; whether Done-Review runs depends on verification mode, criteria, and gate results). |
| `goals.maxConcurrentGoals` | `4` | Active goals per repository (positive integer); further committed goals wait in the admission queue. |
| `goals.gateShell` | `/bin/sh` | Global-only; absolute path of an executable invoked with `-c`. A syntactically invalid live edit is rejected on reload and the previous valid settings stay active. An invalid value at startup, or an accepted absolute path that is not executable, makes gates fail at execution instead of falling back to another shell. Used for goal criteria gates, merge-queue CI commands, and `worktree.setupCommand`. |
| `goals.gateEnv` | Unset | Global-only map of string values. `PATH` is prepended to the resolved gate `PATH` (never a replacement) and must contain absolute directories. Names starting with `HARNESS_` are rejected. A syntactically invalid live edit is rejected on reload; an invalid value at startup makes gates fail at execution. Remove the key from `config.json` to clear it. |

#### Where goal gates run

Deterministic goal criteria commands run with `goals.gateShell -c` in a clean detached checkout of the goal's target branch tip under `<worktree.dir>/_verify/`, prepared by `worktree.setupCommand`. This tests the committed result, so untracked and gitignored files of the main checkout are absent unless the setup command provides them. The main checkout itself is used only when the target branch cannot be resolved, for example outside a git repository.

| Variable | Value |
| --- | --- |
| `HARNESS_REPO_ROOT` | Canonical path of the main project checkout. |
| `HARNESS_WORKTREE_PATH` | Canonical path of the checkout the command runs in; equal to `HARNESS_REPO_ROOT` when verification is not isolated. |
| `HARNESS_GOAL_EVIDENCE_DIR` | The goal's evidence directory; omitted when it cannot be created safely. |
| `HARNESS_GOAL_ID` | The goal id. |

Setup, CI and gate commands receive only the `HARNESS_*` variables listed for them; `HARNESS_*` variables in the daemon's own environment are not passed on.

### `events`, `catalog`, and `quotaRetry`

| Key | Default | Notes |
| --- | --- | --- |
| `events.fallbackWakeupIntervalSec` | `600` | Fallback wakeup interval in seconds for event-driven waits (`> 0`). |
| `events.wakeDirectorOnGoalSettle` | `true` | Wake an idle Director chat when one of its goal runs settles. |
| `catalog.ttlHours` | `24` | Model catalog cache TTL in hours (`>= 0`). |
| `catalog.artificialAnalysisKeyEnv` | `ARTIFICIAL_ANALYSIS_API_KEY` | Validated (string) but read by no runtime component; setting it selects no credential. |
| `catalog.allowPreview` | `true` | Allow preview models to outrank older GA releases in the same family. |
| `catalog.preferSubscription` | `true` | Prefer subscription routes over API routes for the same canonical model. |
| `catalog.modelFilterMode` | `all` | `all` or `allowlist`. |
| `catalog.disabledModels` | `[]` | Canonical model IDs disabled when `modelFilterMode` is `all`. |
| `catalog.enabledModels` | `[]` | Canonical model IDs enabled when `modelFilterMode` is `allowlist`. |
| `quotaRetry.nativeRetryTimeoutMs` | `45000` | Milliseconds to let a runner natively retry rate-limit errors (`>= 0`). |
| `quotaRetry.fallbackCooldownSec` | `60` | Base backoff in seconds when repeated quota pauses report recovery within 15 s (accepts `>= 0`; runtime floor 15 s). |
| `quotaRetry.maxCooldownSec` | `900` | Backoff ceiling in seconds (accepts `>= 0`; at runtime never below the base). |
| `quotaRetry.autoResumeEnabled` | `true` | Automatically resume quota-paused runs at the scheduled time. |
| `quotaRetry.earlyWakePollMs` | `30000` | Poll interval in ms for early recovery during a scheduled quota wait (accepts `> 0`; runtime floor 5 s). Polls only with a configured `quotaEndpoint` and a wait longer than the interval. |

When neither a reset time nor confirmed current capacity provides a schedule (or when `autoResumeEnabled` is `false`), no fallback timer is started and the run waits for a manual resume (or `pi-harness resume <run-id> --in <dur>`).

### `quotaEndpoint`, `profiles`, `remotes`, and `externalMcpServers`

| Key | Default | Notes |
| --- | --- | --- |
| `quotaEndpoint` | Unset | Fields: `url`, `headers` (`${VAR}` names of uppercase letters, digits, and underscores expand from the daemon environment; unset names expand to empty), `pollIntervalSec` (`60`), `timeoutMs` (`5000`), `format` (`protojson`, the default, or `protobuf`). See the example in [README](../README.md) and [`proto/quota.proto`](../proto/quota.proto). |
| `profiles` | `acp:claude` (enabled), `acp:codex` (enabled), `acp:gemini` (disabled) | Managed with `pi-harness profiles` or `/settings`. |
| `remotes` | Unset | Global-only; required fields `name` and `sshTarget`, optional `bridgeCommand`, `autoConnect`, and `reverseAccess`. Managed with `pi-harness remote add\|rm\|connect\|disconnect`. |
| `externalMcpServers` | Unset | Entries have `name`, `command`, `args`, `env`, and `enabled`; forwarded to ACP agents that have workspace access (not control-plane-only agents). The daemon adds `octofs` (`octofs mcp`) when it resolves a binary: the one staged by the installer (see [Runtime install](./runtime-install.md)), one on `PATH` (used without checksum verification), or pinned release `0.26.0` downloaded on demand into `$PI_HARNESS_HOME/tools/octofs/` after a SHA-256 check. Opt out with `{ "name": "octofs", "enabled": false }`; your own `octofs` entry replaces the default. |

## Live reload

- `harnessd` watches the state directory and applies edits to `config.json` about 120 ms after the last change, including editors that save by atomic rename.
- A missing, empty, or unchanged file changes nothing. Deleting `config.json` keeps the previous settings; write `{}` to return to defaults plus the startup overlay.
- Invalid JSON or any value that fails validation is rejected as a whole and previous settings stay active. For a validation failure the daemon log line names the failing keys, never values; a JSON syntax error names no key.
- An applied reload logs the changed keys; values of roots, gate shell and environment, commands, URLs, and headers are not printed.
- Reload log entries use the prefixes `config reload applied`, `config reload rejected`, `config warning:`, and `config warnings cleared:` on the daemon's stderr (redirected to `daemon.log` next to the socket, `$PI_HARNESS_HOME/daemon.log` with the default state-dir socket, when auto-spawned).
- `/settings` in the TUI writes the effective config to `config.json` through the daemon, including values that come from the startup overlay and unknown fields inside sections, and dropping unrecognized top-level keys and misplaced known section keys.

| Takes effect | Settings |
| --- | --- |
| Immediately | `worktree` keys other than the roots (raising `maxConcurrentWorkers` starts queued workers); `goals.gateTimeoutMs`, `gateShell`, `gateEnv`, `autoVerifyOnSettle`, and `maxConcurrentGoals` (raising it starts queued goals); `remotes`; `quotaEndpoint`; `catalog`; `profiles`; `worktree.extraWriteRoots` for Director `write_note`; `events` for Director settlement and wait handlers (a wait already in progress keeps the value read when it started); `codeMode` and `codeExec` for an existing task's next MCP tool discovery (`tools/list`) and Code Mode dispatch; `externalMcpServers` for daemon config, `daemon status`, and background `octofs` staging; `quotaRetry.fallbackCooldownSec`, `maxCooldownSec`, `autoResumeEnabled`, and `earlyWakePollMs` when the next quota pause is handled (timers already armed are not reconfigured) |
| When a task or goal is created | `rollover.orchestratorMaxTurns`, `workerMaxTurns`, and `taskMaxTurns` (copied into the task record and kept for its later generations); `goals.defaultMaxReviewCycles` and `defaultMaxNoProgressCycles` (copied into the new goal contract; existing goals keep theirs) |
| At agent launch or generation (running sessions keep theirs) | `worktree.extraReadRoots`, `worktree.extraWriteRoots`; `externalMcpServers` entries for newly started ACP sessions; `codeMode` and `codeExec` initial prompt and runner tool list (later MCP tool discovery and Code Mode dispatches read the current mode and limits); `quotaRetry.nativeRetryTimeoutMs`; `rollover.reviewerMaxTurns`, `orchestratorThreshold`, `workerThreshold`, and `maxGenerations`; default profiles |
| Only after a daemon restart | state directory (`PI_HARNESS_HOME`), `socketPath`, the repository overlay |

To change `socketPath` or `PI_HARNESS_HOME`, run `pi-harness daemon stop` while the old setting is still in effect, change the setting, then start the daemon at the new address: `pi-harness daemon restart` targets the newly configured address and leaves the old daemon running if the effective socket path changes. Start or restart from the intended overlay directory.

## Inspecting effective settings

`pi-harness daemon status` (or plain `pi-harness daemon`) prints the daemon's `status` response as JSON. It reports a subset of the settings; alongside `pid`, `socketPath`, and `uptimeMs`, check:

- `configPath`: path to the global `config.json` the daemon watches.
- `configWarnings`: array of value-free warnings for known keys at unsupported locations in `config.json` or the startup `.pi-harness.json` overlay (including global-only keys in the overlay).
- `effectiveLimits`: `maxConcurrentGoals`, `maxConcurrentWorkers`, `reviewerMaxTurns`, `extraReadRoots`, and `extraWriteRoots`.
- `effectiveExternalMcpServers`: array of `{ name, enabled }` entries. The default `octofs` appears only when a binary is resolved; an explicitly disabled entry appears as `{ name: "octofs", enabled: false }`.

An empty `extraReadRoots` after you set roots can mean the key is misplaced, it is set in a repository overlay where it is global-only, the reload was rejected, invalid entries were dropped at startup, the file sets an explicit `[]`, or you are reading a different config or daemon. Check `configPath`, `configWarnings`, and the daemon's stderr.

## CLI

- `pi-harness daemon serve | status | stop | restart [--remote <alias|ssh://host>]`: `serve` runs in the foreground on the local host. `restart` updates a running daemon after an install (a fresh install needs no restart, and an idle daemon from an older build is replaced automatically) and applies startup-fixed settings.
- `pi-harness resume <run-id> [--in 15m | --message "<msg>"]`: resumes a run immediately; `--in <dur>` instead schedules a delayed resume of a quota-paused run (clamped to 1 s to 24 h). `--in` and `--message` cannot be combined.
- `pi-harness run resume <run-id> ...` is an alias for `pi-harness resume` and rejects `--profile`, `--model`, and `--effort`. To start a new run whose goal begins with `resume`, quote a multi-word goal, or use `pi-harness run -- resume ...`, which works for any such goal, including the single word.
- `pi-harness --remote <alias|ssh://host>` on a TTY, without `-p`, `--project`, or `--repo`, opens a picker of that host's registered projects when it has at least one.
- `pi-harness --help` lists the primary commands.

## Director tools

| Tool | What it does |
| --- | --- |
| `read_file` | Reads UTF-8 text from the workspace, harness goals, notes, and artifacts, or any file the operator's account can read (absolute or `~/`). Returns one page per call (32 KiB by default, up to 2 MiB with `max_bytes`). Select lines with `start_line`/`end_line` or page with `offset` and the returned `nextOffset` (line range and offset cannot be combined). `exception_reason` is optional. Path rules apply to the canonical path, case-insensitively: inside the current repository it refuses any `.git` segment and top-level `.harness/*` except `.harness/artifacts`, `.harness/goals`, and `.harness/notes`; outside it refuses any `.git` segment and any literal `.harness/worktrees` segment pair (it does not compare against `$PI_HARNESS_HOME` or a custom `worktree.dir`). Refuses directories and other non-regular files, and fails when the returned byte slice is not valid UTF-8 (not a whole-file binary check). Available directly in collapsed Code Mode. |
| `write_note` | Logged writer scoped to `.harness/goals/`, `.harness/notes/`, or a configured `extraWriteRoots` directory; requires `path`, `content`, and `reason`. |
| `goal_accept` | Director override that marks a goal achieved. Requires `goal_id`, `reason`, and `evidence_paths`; recorded in the goal's review history as an approved review and completes the linked run. It does not run the goal's gates or a Done-Review. |
| `steer_task` | Looks up a task of any tier on the daemon by ID and attempts to steer a live task (returns `steered: false` when the task has no live runner session or the runner does not accept the message, and refuses settled tasks or stopped runs); unlike `cancel_task`, it does not check that the task's run belongs to this repository. |
| `cancel_task` | Cancels any task whose run belongs to this repository. |
| `schedule_timer` | Schedules a durable wakeup timer; `wake_on_event: "goal_settled"` requires `goal_id`, while `"any"` without `goal_id` reacts to any goal of the chat settling. When settlement wakes are enabled (`events.wakeDirectorOnGoalSettle`), a settling goal cancels the chat's matching timer. |

Open escalations on a goal's run are resolved automatically when the goal is achieved (including by `goal_accept`) or cancelled.
