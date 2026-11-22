import type { SlashCommand as TuiSlashCommand } from "@earendil-works/pi-tui";
import type { PaletteCommand } from "./overlays.js";
import { COMPOSER_SHORTCUT_HELP } from "./primitives/composer.js";
import type { HarnessTheme } from "./primitives/index.js";

/**
 * Single slash-command registry. It is the only source for slash autocomplete, input dispatch,
 * `/help`, and the `Ctrl+O` Command Palette in both Director Chat (`chat`) and Attached-Run (`run`) modes.
 * Commands resolve by exact name only; there are no aliases.
 */
export type CommandMode = "chat" | "run";

export interface SlashCommand {
  /** Command name without the leading slash. */
  name: string;
  args?: string;
  description: string;
  modes: Array<CommandMode>;
  /** Keyboard shortcut that runs the same command, e.g. `Ctrl+T`. */
  key?: string;
}

export const COMMANDS: readonly SlashCommand[] = [
  { name: "runs", description: "Goals & runs tree: inspect agents; resume (r), wait (w), stop (x)", modes: ["chat", "run"], key: "Ctrl+T" },
  { name: "goals", description: "Same Goals & runs view: goal review (r), pause (p), cancel (x)", modes: ["chat"] },
  { name: "sessions", args: "[id]", description: "List & switch sessions (takes over the other terminal)", modes: ["chat", "run"] },
  { name: "new", args: "[title]", description: "Start a new concurrent Director Chat session", modes: ["chat"] },
  { name: "model", description: "Pick a model & ACP CLI route, or a CLI's default model", modes: ["chat"] },
  { name: "effort", args: "[level]", description: "Pick reasoning effort (low / medium / high / xhigh / max / ultra)", modes: ["chat"] },
  { name: "quota", description: "Subscription & API quota buckets with reset timers", modes: ["chat"] },
  { name: "worktrees", description: "List & clean stale/orphaned git worktrees", modes: ["chat"] },
  { name: "kb", args: "[query]", description: "Browse the 2-stage Knowledge Base", modes: ["chat"] },
  { name: "settings", description: "Configure defaults, Quota Endpoint & external ACP CLIs", modes: ["chat"], key: "Ctrl+," },
  { name: "status", description: "Daemon health, active model/effort, CLIs & goals summary", modes: ["chat"] },
  { name: "clear", description: "Clear the local transcript", modes: ["chat"] },
  { name: "stop", description: "Stop this autonomous run", modes: ["run"] },
  { name: "help", description: "List commands & keyboard shortcuts", modes: ["chat", "run"] },
  { name: "quit", description: "Leave (background agents keep running; harnessd stops if idle)", modes: ["chat", "run"], key: "Ctrl+]" },
];

export function commandsFor(mode: CommandMode): SlashCommand[] {
  return COMMANDS.filter((c) => c.modes.includes(mode));
}

/** Resolves `/name [arg]` to a command available in `mode`; exact names only. */
export function resolveCommand(mode: CommandMode, input: string): { cmd: SlashCommand; arg: string } | null {
  const match = /^\/(\S+)(?:\s+([\s\S]*))?$/.exec(input.trim());
  if (!match) return null;
  const cmd = commandsFor(mode).find((c) => c.name === match[1]);
  return cmd ? { cmd, arg: (match[2] ?? "").trim() } : null;
}

export function completions(mode: CommandMode, prefix: string): SlashCommand[] {
  const p = prefix.trim().replace(/^\//, "");
  return commandsFor(mode).filter((c) => c.name.startsWith(p));
}

/** Composer `/` autocomplete entries for `mode`. */
export function autocompleteCommands(mode: CommandMode): TuiSlashCommand[] {
  return completions(mode, "").map((c) => ({
    name: c.name,
    description: c.key ? `${c.description} (${c.key})` : c.description,
    ...(c.args ? { argumentHint: c.args } : {}),
  }));
}

/** `Ctrl+O` Command Palette entries for `mode`; `run` dispatches the chosen command with no argument. */
export function paletteCommands(mode: CommandMode, run: (cmd: SlashCommand) => void): PaletteCommand[] {
  return commandsFor(mode).map((c) => ({
    id: c.name,
    label: `/${c.name}`,
    ...(c.key ? { shortcut: c.key } : {}),
    description: c.description,
    run: () => run(c),
  }));
}

const SHORTCUTS: Record<CommandMode, Array<[string, string]>> = {
  chat: [
    ["Enter", "Send message immediately (steers active turn if busy)"],
    ["Ctrl+Q / Alt+Enter", "Enqueue message for after the active turn"],
    ["Ctrl+S", "Send queued message immediately"],
    ["Ctrl+R", "Recall queued message into input"],
    ["Esc", "Press twice to stop active turn / close overlay"],
    ["Ctrl+O", "Command palette (keeps your draft)"],
    ["Ctrl+C", "Clear input / cancel turn; twice to quit"],
  ],
  run: [
    ["Ctrl+O", "Command palette (keeps your draft)"],
    ["Ctrl+C", "Clear input; twice to quit"],
  ],
};

export function renderHelp(mode: CommandMode, theme?: HarnessTheme): string[] {
  const bold = theme?.bold ?? ((s: string) => s);
  const accent = theme?.accent ?? ((s: string) => s);
  const dim = theme?.dim ?? ((s: string) => s);
  const cmds = commandsFor(mode);
  const usage = (c: SlashCommand) => `/${c.name}${c.args ? ` ${c.args}` : ""}`;
  const shortcuts = [...SHORTCUTS[mode], ...COMPOSER_SHORTCUT_HELP];
  const colW = Math.max(...cmds.map((c) => usage(c).length), ...shortcuts.map(([key]) => key.length)) + 2;
  const lines = [bold(mode === "chat" ? "Commands & Shortcuts" : "Attached-Run Commands")];
  for (const c of cmds) {
    const u = usage(c);
    lines.push(`${accent(`/${c.name}`)}${c.args ? ` ${c.args}` : ""}${" ".repeat(colW - u.length)}${c.description}${c.key ? ` ${dim(`(${c.key})`)}` : ""}`);
  }
  for (const [key, desc] of shortcuts) {
    lines.push(`${accent(key)}${" ".repeat(Math.max(1, colW - key.length))}${desc}`);
  }
  if (mode === "run") lines.push(dim("Type text to steer the focused agent (or resume if quota-paused)."));
  return lines;
}
