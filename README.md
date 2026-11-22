# pi-harness

CLI-first, daemonized **Tier 0 Interactive Director + 3-Tier Multi-Agent Coding Harness** built on **Bun**, **`@earendil-works/pi`**, and **ACP (Agent Client Protocol)**.

## Architecture Highlights

- **Tier 0 Interactive Director & Dynamic Goal Engine (`src/director/`, `src/goals/`)**:
  - Zero-arg `pi-harness` opens a persistent conversational chat to design, steer, and launch versioned **Goal Contracts** (`v1 → v2` mid-flight amendments without discarding unaffected work).
  - Run-until-done loop combining `$0`-token deterministic shell gates, **Independent Skeptical Done-Review** with mechanical anti-sycophancy validation, and diff-hash / oscillation circuit breakers.
- **3-Tier Dynamic Hierarchy (`src/control-plane/`)**:
  - **Tier 1 Orchestrator**: Decomposes goals into disjoint write-scope Worker slices, handles `P1` escalations and `scope_too_large` send-backs, and performs **Clean Relay Rollovers** at 72% context utilization by default (`rollover.orchestratorThreshold`) (hydrated deterministically from SQLite + Anchored Handoff + `query_predecessor` Seance).
  - **Tier 2 Workers**: Own isolated git worktrees, delegate dynamically to Tier 3 Task Agents or execute tightly-coupled edits directly, choose models/efforts via `list_models`, roll over cleanly at 78% context by default (`rollover.workerThreshold`), and integrate via a deterministic **Bors-style bisecting merge queue**.
  - **Tier 3 Task Agents**: Small, zero-compaction windows (`research`, `plan`, `execute`, `verify`) returning pass-by-reference artifacts + compact summaries, with mandatory `return_task("scope_too_large")` and `report_issue(...)` tooling.
- **ACP-First External CLIs + Native `pi` Lane (`src/runners/`)**:
  - Manage your own CLIs & subscription auth out of band (`claude-agent-acp`, `codex-acp`, `gemini --acp`, `opencode acp`, `amp --acp`, `agy acp`, or custom ACP CLIs configured live in `/settings`).
  - Implements verified per-adapter ACP shims (`src/runners/acp-shims.ts`), live `usage_update` rollover telemetry, passive quota harvesting, and universal Harness MCP tool bridging (`src/runners/mcp-bridge.ts`).
- **Curated Knowledge Base & Cross-Run Coordination (`src/kb/`, `src/control-plane/db.ts`)**:
  - `bun:sqlite` FTS5 BM25 2-stage discovery + `$0`-token `git diff` staleness pre-check for Maintainer GC.
  - Repo-scoped write leases, versioned single-writer shared contracts, target-branch-serialized merge queues, and DFS deadlock detection for bounded peer requests.
- **Model catalog** (`src/catalog/`)
  - The catalog combines routes from enabled profiles with models.dev, OpenRouter, and LiteLLM metadata to select current coding models. The CLI, picker, and automatic selection share one ranking from fresh Vals Terminal-Bench 4.0 results. Eligible models without a comparable measurement remain available with no rank. [Ranking evidence and offline reproduction](docs/model-catalog-ranking.md)
- **Native Quota Endpoint (`proto/quota.proto`, `src/gen/quota_pb.ts`, `src/quota/`)**:
  - Generated directly via `@bufbuild/protoc-gen-es` (`bun run proto:gen`). Models shared vs. separate multi-window quota buckets (e.g., Antigravity Gemini vs. Opus 5.5, Claude 5h ∩ 7d ∩ 7d-Opus, Copilot multipliers, API spend caps) merged with passive ACP rate-limit signals.
- **Native-Scrollback TUI Primitives & Zero-Port Remote Daemon (`src/tui/primitives/`, `src/daemon/`)**:
  - Persistent `harnessd` survives terminal close; thin `@earendil-works/pi-tui` (`TuiMainScreen`) client renders width-safe, memoized native-scrollback cards (`< 4ms` daemon handshake, `< 0.02ms`/frame memoized redraws) and attaches remotely over `ssh://host`.
  - Interactive overlays: `/model` (searchable tabular catalog), `/effort`, `/quota`, `/goals`, `/runs`, `/worktrees`, `/kb`, `/settings` (`Ctrl+,`), `Ctrl+O` Command Palette, `Ctrl+T` DFS Agent Tree Switcher, `Ctrl+C`/`Ctrl+D` clean exit, and `Ctrl+]` tmux-style detach.

## Install

```bash
sh install.sh                 # clean host: pinned Bun, locked deps, launcher; re-run to update
~/.local/bin/pi-harness --help
```

This stages a checksum-verified upstream Bun and the frozen `bun.lock` dependencies under `~/.pi-harness/install`, so no global Bun, Node or Python is needed. Prerequisites, layout, platform support (Code Mode is macOS-only), update steps and the Nix/mise/pixi comparison: [docs/runtime-install.md](docs/runtime-install.md).

## Quick Start (development checkout)

```bash
bun install
bun run proto:gen
bun test

# Launch Tier 0 Interactive Director Chat (auto-spawns detached harnessd)
bun run start

# Sync live model catalog (latest per family across enabled CLIs)
bun run src/cli.ts models sync

# Launch or attach to an autonomous run directly (local or over SSH)
bun run src/cli.ts run "Implement OAuth token refresh locking"
bun run src/cli.ts attach <run-id>
bun run src/cli.ts attach <run-id> --remote ssh://devbox

# Inspect or clean stale worktrees
bun run src/cli.ts worktrees list
bun run src/cli.ts worktrees clean
```

## Optional Quota Endpoint Configuration (`~/.pi-harness/config.json`)

```json
{
  "quotaEndpoint": {
    "url": "http://127.0.0.1:9090/v1/quota",
    "headers": { "Authorization": "Bearer ${QUOTA_TOKEN}" },
    "pollIntervalSec": 60,
    "format": "protojson"
  }
}
```

See [`proto/quota.proto`](./proto/quota.proto) for the complete Protobuf v3 / ProtoJSON schema.
