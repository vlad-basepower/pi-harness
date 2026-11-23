import { accessSync, constants, lstatSync, mkdtempSync, realpathSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { ensureTempDir, type AcpAdapterKind, type AcpProfileConfig } from "../config.js";
import type { ArtifactRecord } from "../control-plane/db.js";
import { isProtectedWritePath } from "./protected-paths.js";
import type { ToolPolicy } from "./types.js";

export interface PreparedAcpSpawn {
  command: string;
  args: string[];
  env: Record<string, string>;
  cleanup: () => void;
  /** True if system prompt was NOT injected via env/native and must be prepended to turn 1 */
  prependSystemPromptToTurn1: boolean;
}

export interface AcpMcpServerEntry {
  name: string;
  command: string;
  args: string[];
  env: Array<{ name: string; value: string }>;
}

const AMP_READ_ONLY_DISABLED_TOOLS = [
  "edit_file",
  "create_file",
  "delete_file",
  "apply_patch",
  "mcp__octofs__text_editor",
  "mcp__octofs__batch_edit",
  "mcp__octofs__shell",
  "Task",
  "create_thread",
  "send_thread_message",
  "wait_for_threads",
  "finder",
  "oracle",
  "librarian",
];

export const OCTOFS_READ_ONLY_TOOLS = new Set([
  "view",
  "view_signatures",
  "list_files",
  "extract_lines",
  "ast_grep",
]);

export const OCTOFS_MUTATING_TOOLS = new Set([
  "text_editor",
  "batch_edit",
  "shell",
]);

const AST_GREP_READ_ONLY_TOOLS = new Set([
  "find_code",
  "find_code_by_rule",
  "dump_syntax_tree",
  "test_match_code_rule",
]);

/**
 * Checks whether an external MCP server tool is safe to invoke in `read_only_workspace` mode.
 * `octofs` exposes both read-only (`view`, `view_signatures`, `list_files`, `extract_lines`, `ast_grep`)
 * and mutating (`text_editor`, `batch_edit`, `shell`) tools — only the read-only subset is permitted.
 */
export function isReadOnlyExternalMcpTool(serverName: string, toolName: string): boolean {
  const srv = serverName.trim().toLowerCase();
  const tool = toolName.trim().toLowerCase();
  if (!tool) return false;
  if (srv === "octofs") return OCTOFS_READ_ONLY_TOOLS.has(tool);
  if (srv === "ast-grep" || srv === "ast_grep") return AST_GREP_READ_ONLY_TOOLS.has(tool);
  if (/(?:write|edit|delete|remove|create|update|patch|apply|exec|shell|run|kill|set_|put_|post_|rewrite)/.test(tool)) {
    return false;
  }
  return /^(?:read|get|list|search|find|view|dump|inspect|describe|query)(?:_|$)/.test(tool);
}

/** Git subcommands that only inspect (subject to the option checks in `isReadOnlyGitCommand`). */
const READ_ONLY_GIT_SUBCOMMANDS = new Set([
  "status",
  "log",
  "show",
  "diff",
  "rev-parse",
  "ls-files",
  "ls-tree",
  "grep",
  "blame",
  "describe",
  "cat-file",
  "merge-base",
  "shortlog",
  "rev-list",
  "show-ref",
  "for-each-ref",
  "name-rev",
  "check-ignore",
  "whatchanged",
  "count-objects",
]);

/** Readers with no option that executes, writes, or follows anything beyond their operands. */
const PLAIN_READ_ONLY_BINS = new Set([
  "cat",
  "head",
  "tail",
  "nl",
  "ls",
  "stat",
  "readlink",
  "realpath",
  "basename",
  "dirname",
  "cmp",
  "comm",
  "cut",
  "tr",
  "echo",
  "printf",
  "test",
  "[",
  "true",
  "false",
  "pwd",
  "which",
  "type",
]);

/** Leading assignments a read-only command may carry; values must not name paths. */
const SAFE_ENV_ASSIGNMENT = /^(?:LANG|LC_[A-Z]+|TZ|NO_COLOR|COLUMNS|TERM)=[^/]*$/;

const CONTROL_CHARS = /[\u0000-\u0008\u000b-\u001f\u007f]/;
/** Whitespace other than space, tab, and newline (which the shell does not treat as a word separator). */
const NON_SHELL_WHITESPACE = /[^\S \t\n]/;

/**
 * Splits a command line into simple-command segments, or returns null when the line uses any shell
 * feature whose effect the validator cannot see in the token list: redirections (except stderr to
 * /dev/null or stdout), command/process substitution, parameter/arithmetic/ANSI expansion, tilde and
 * brace expansion, subshells and zsh glob qualifiers, unquoted glob characters, zsh `=cmd`
 * expansion, line continuations, background jobs, and control characters.
 */
function splitShellSegments(cmd: string): string[] | null {
  if (CONTROL_CHARS.test(cmd)) return null;
  const segments: string[] = [];
  let cur = "";
  let inSingle = false;
  let inDouble = false;
  for (let i = 0; i < cmd.length; i++) {
    const ch = cmd[i]!;
    const next = cmd[i + 1];
    if (inSingle) {
      cur += ch;
      if (ch === "'") inSingle = false;
      continue;
    }
    if (inDouble) {
      if (ch === "\\" && next !== undefined) {
        if (next === "\n") return null;
        cur += ch + next;
        i++;
        continue;
      }
      if (ch === "`") return null;
      if (ch === "$" && next !== undefined && next !== '"' && !/\s/.test(next)) return null;
      cur += ch;
      if (ch === '"') inDouble = false;
      continue;
    }
    const wordStart = cur.length === 0 || /\s/.test(cur[cur.length - 1]!);
    if (ch === "\\" && next !== undefined) {
      if (next === "\n") return null;
      cur += ch + next;
      i++;
      continue;
    }
    if (ch === "'") {
      inSingle = true;
      cur += ch;
      continue;
    }
    if (ch === '"') {
      inDouble = true;
      cur += ch;
      continue;
    }
    // Permit ONLY stderr-to-null (`2>/dev/null`) and stderr-to-stdout (`2>&1`) as whole words; reject all other redirections
    if (ch === "2" && next === ">") {
      if (!wordStart) return null;
      if (cmd.slice(i).startsWith("2>/dev/null") && (i + 11 === cmd.length || /[\s|;&\n]/.test(cmd[i + 11]!))) {
        i += 10;
        continue;
      }
      if (cmd.slice(i).startsWith("2>&1") && (i + 4 === cmd.length || /[\s|;&\n]/.test(cmd[i + 4]!))) {
        i += 3;
        continue;
      }
      return null;
    }
    if (ch === ">" || ch === "<" || ch === "`") return null;
    if (ch === "$" && next !== undefined && !/\s/.test(next)) return null;
    if ("{}()".includes(ch)) return null;
    if (ch === "~" && (wordStart || cur.endsWith("=") || cur.endsWith(":"))) return null;
    if (wordStart && (ch === "=" || ch === "#")) return null; // zsh `=cmd` expansion; comments hide words
    // Unquoted glob characters expand to names the validator cannot see; only a lone `[` (the test command) is literal.
    if (ch === "*" || ch === "?" || (ch === "[" && !(wordStart && (next === undefined || /\s/.test(next))))) return null;
    if (ch === "&" && next === ">") return null;

    // Segment separators outside quotes: |, ||, &&, ;, \n
    if (ch === "|" || ch === ";" || ch === "\n") {
      if (cur.trim()) segments.push(cur);
      cur = "";
      if (ch === "|" && next === "|") i++;
      continue;
    }
    if (ch === "&" && next === "&") {
      if (cur.trim()) segments.push(cur);
      cur = "";
      i++;
      continue;
    }
    if (ch === "&") return null; // background execution
    cur += ch;
  }
  if (inSingle || inDouble) return null;
  if (cur.trim()) segments.push(cur);
  return segments;
}

/**
 * Splits one segment into the argument vector the shell passes, applying POSIX quote removal. A word is
 * an `assignment` only when its name and `=` are unquoted, as the shell requires.
 */
function tokenizeSegment(seg: string): Array<{ text: string; assignment: boolean }> {
  const words: Array<{ text: string; assignment: boolean }> = [];
  let cur = "";
  let inSingle = false;
  let inDouble = false;
  let hasToken = false;
  let quotedAt = -1; // length of `cur` when the first quote or escape in this word began
  const quote = () => {
    hasToken = true;
    if (quotedAt < 0) quotedAt = cur.length;
  };
  const push = () => {
    const eq = cur.indexOf("=");
    const assignment = /^[A-Za-z_][A-Za-z0-9_]*=/.test(cur) && (quotedAt < 0 || quotedAt > eq);
    words.push({ text: cur, assignment });
    cur = "";
    hasToken = false;
    quotedAt = -1;
  };
  for (let i = 0; i < seg.length; i++) {
    const ch = seg[i]!;
    const next = seg[i + 1];
    if (inSingle) {
      if (ch === "'") inSingle = false;
      else cur += ch;
      continue;
    }
    if (inDouble) {
      if (ch === "\\" && next !== undefined) {
        // Inside double quotes a backslash only escapes $, `, ", and \; otherwise it stays literal.
        cur += "$`\"\\".includes(next) ? next : ch + next;
        i++;
      } else if (ch === '"') {
        inDouble = false;
      } else {
        cur += ch;
      }
      continue;
    }
    if (ch === "\\" && next !== undefined) {
      quote();
      cur += next;
      i++;
      continue;
    }
    if (ch === "'") {
      quote();
      inSingle = true;
      continue;
    }
    if (ch === '"') {
      quote();
      inDouble = true;
      continue;
    }
    if (ch === " " || ch === "\t") {
      if (hasToken) push();
      continue;
    }
    cur += ch;
    hasToken = true;
  }
  if (hasToken) push();
  return words;
}

/** Long option name of `--name[=value]`, or undefined for anything else. */
function longOptionName(arg: string): string | undefined {
  return arg.startsWith("--") && arg.length > 2 ? arg.slice(2).split("=")[0]! : undefined;
}

/**
 * True when `arg` names a blocked long option exactly or by abbreviation (git and getopt_long accept
 * unique prefixes). Names in `exact` are distinct options that happen to prefix a blocked one.
 */
function isBlockedLongOption(arg: string, blocked: string[], exact: string[] = []): boolean {
  const name = longOptionName(arg);
  if (name === undefined || exact.includes(name)) return false;
  return blocked.some((b) => b.startsWith(name));
}

/** True when `arg` is a short-option cluster containing any of `letters` (attached values included). */
function hasShortFlag(arg: string, letters: string): boolean {
  return /^-[^-]/.test(arg) && [...arg.slice(1)].some((c) => letters.includes(c));
}

const TREE_SHORT_FLAGS = "adfxqNQpugshDFvtcUriASnCXJ";
const TREE_LONG_FLAGS = new Set(["gitignore", "ignore-case", "matchdirs", "prune", "noreport", "charset", "filelimit", "si", "du", "timefmt", "inodes", "device", "dirsfirst", "filesfirst", "sort"]);
const TREE_LONG_WITH_VALUE = new Set(["charset", "filelimit", "timefmt", "sort"]);

/** tree: allowlisted flags only (no output file, HTML rerun, base HREF, or symlink following). */
function isSafeTreeArgs(args: string[]): boolean {
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]!;
    const name = longOptionName(arg);
    if (name !== undefined) {
      if (!TREE_LONG_FLAGS.has(name)) return false;
      if (!arg.includes("=") && TREE_LONG_WITH_VALUE.has(name)) i++;
      continue;
    }
    if (!arg.startsWith("-") || arg === "-") continue;
    const withValue = /^-[LPI](.*)$/.exec(arg);
    if (withValue) {
      if (!withValue[1]) i++;
      continue;
    }
    if (![...arg.slice(1)].every((c) => TREE_SHORT_FLAGS.includes(c))) return false;
  }
  return true;
}

/** uniq: a second operand is an output file. */
function isSafeUniqArgs(args: string[]): boolean {
  let operands = 0;
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]!;
    if (arg === "--") return false;
    if (/^-[fsw]$/.test(arg) || /^--(?:skip-fields|skip-chars|check-chars)$/.test(arg)) {
      i++;
      continue;
    }
    if (!arg.startsWith("-") || arg === "-") operands++;
  }
  return operands <= 1;
}

const SED_ADDRESS = String.raw`(?:\d+|\$|/(?:[^/\\\n]|\\.)*/I?)`;
const SED_PRINT_CLAUSE = String.raw`\s*${SED_ADDRESS}(?:\s*,\s*${SED_ADDRESS})?\s*p\s*`;
const SED_PRINT_SCRIPT = new RegExp(String.raw`^${SED_PRINT_CLAUSE}(?:;${SED_PRINT_CLAUSE})*;?\s*$`);

/** sed: `-n` with scripts made only of address-print clauses (no in-place, w/r/e verbs, or script files). */
function isSafeSedArgs(args: string[]): boolean {
  const scripts: string[] = [];
  const files: string[] = [];
  let quiet = false;
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]!;
    if (arg === "--quiet" || arg === "--silent") {
      quiet = true;
      continue;
    }
    if (arg === "--regexp-extended") continue;
    if (arg.startsWith("--expression=")) {
      scripts.push(arg.slice("--expression=".length));
      continue;
    }
    if (arg === "--expression") {
      if (i + 1 >= args.length) return false;
      scripts.push(args[++i]!);
      continue;
    }
    if (arg.startsWith("--")) return false;
    if (arg.startsWith("-") && arg !== "-") {
      const m = /^-([nEr]*)(e?)([\s\S]*)$/.exec(arg);
      if (!m) return false;
      if (m[1]!.includes("n")) quiet = true;
      if (m[2]) {
        const script = m[3] || args[++i];
        if (script === undefined) return false;
        scripts.push(script);
      } else if (m[3]) {
        return false;
      }
      continue;
    }
    files.push(arg);
  }
  if (!quiet) return false;
  if (scripts.length === 0) {
    const first = files.shift();
    if (first === undefined) return false;
    scripts.push(first);
  }
  return scripts.every((s) => SED_PRINT_SCRIPT.test(s));
}

const SQLITE_FLAGS = new Set(["-readonly", "-safe", "-header", "-noheader", "-csv", "-json", "-line", "-list", "-column", "-box", "-table", "-markdown", "-html", "-quote", "-tabs", "-ascii", "-bail", "-batch"]);
const SQLITE_FLAGS_WITH_VALUE = new Set(["-separator", "-newline", "-nullvalue"]);

/** sqlite3: `-readonly`, output switches, one plain database path, and one SELECT/WITH/EXPLAIN statement. */
function isSafeSqliteArgs(args: string[]): boolean {
  const positional: string[] = [];
  let readonly = false;
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]!;
    if (!arg.startsWith("-")) {
      positional.push(arg);
      continue;
    }
    const flag = arg.replace(/^--/, "-");
    if (flag === "-readonly") readonly = true;
    if (SQLITE_FLAGS.has(flag)) continue;
    if (SQLITE_FLAGS_WITH_VALUE.has(flag) && i + 1 < args.length) {
      i++;
      continue;
    }
    return false;
  }
  if (!readonly || positional.length !== 2) return false;
  const [db, sql] = positional as [string, string];
  if (db.includes(":")) return false; // URI filenames and :memory: carry their own options
  return (
    /^\s*(?:SELECT|WITH|EXPLAIN)\b/i.test(sql) &&
    !/^\s*\./m.test(sql) &&
    !/;\s*\S/.test(sql) &&
    !/\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|VACUUM|ATTACH|DETACH|PRAGMA)\b/i.test(sql) &&
    !/\b(?:readfile|writefile|edit|fsdir|lsdir|load_extension|zipfile|sqlar_\w+|fts3_tokenizer)\s*\(/i.test(sql)
  );
}

/** ast-grep: default/run/scan only, never applying rewrites or prompting. */
function isSafeAstGrepArgs(args: string[]): boolean {
  const sub = args[0];
  if (sub !== undefined && !sub.startsWith("-") && sub !== "run" && sub !== "scan") return false;
  return !args.some((a) => hasShortFlag(a, "Uic") || isBlockedLongOption(a, ["update-all", "interactive", "rewrite", "config", "follow"]));
}

const GIT_BRANCH_FLAGS = new Set(["-l", "--list", "-a", "--all", "-r", "--remotes", "-v", "--verbose", "--show-current", "--contains", "--no-contains", "--merged", "--no-merged", "--points-at", "--format", "--sort", "--color", "--no-color", "--column", "--no-column", "-i", "--ignore-case"]);
const GIT_TAG_FLAGS = new Set(["-l", "--list", "-n", "--contains", "--no-contains", "--merged", "--no-merged", "--points-at", "--format", "--sort", "--color", "--no-color", "--column", "--no-column", "-i", "--ignore-case"]);
const GIT_LIST_FLAGS = new Set(["-l", "--list", "-n", "--contains", "--no-contains", "--merged", "--no-merged", "--points-at"]);

/** git branch / git tag: listing flags only; positional words only alongside a list-mode flag. */
function isGitListQuery(args: string[], flags: Set<string>): boolean {
  let positional = false;
  let list = false;
  for (const arg of args) {
    if (!arg.startsWith("-")) {
      positional = true;
      continue;
    }
    const names = arg.startsWith("--") ? [arg.split("=")[0]!] : /^-n\d*$/.test(arg) ? ["-n"] : [...arg.slice(1)].map((c) => `-${c}`);
    for (const name of names) {
      if (!flags.has(name)) return false;
      if (GIT_LIST_FLAGS.has(name)) list = true;
    }
  }
  return !positional || list;
}

function isReadOnlyGitCommand(args: string[]): boolean {
  let g = 0;
  while (g < args.length && args[g]!.startsWith("-")) {
    const flag = args[g]!;
    if (flag === "--no-pager" || flag === "-P" || flag === "--no-optional-locks") {
      g += 1;
      continue;
    }
    // -C (relative operands would resolve elsewhere), -c, --git-dir, --work-tree, --bare, --exec-path, ...
    return false;
  }
  const sub = args[g];
  if (!sub) return false;
  const subArgs = args.slice(g + 1);
  if (subArgs.some((a) =>
    hasShortFlag(a, "O") ||
    (!(sub === "grep" && a === "--or") && isBlockedLongOption(a, ["output", "ext-diff", "textconv", "no-index", "orderfile"], ["text"]))
  )) return false;
  if (sub === "grep" && subArgs.some((a) => isBlockedLongOption(a, ["open-files-in-pager"]))) return false;
  if (sub === "cat-file" && subArgs.some((a) => isBlockedLongOption(a, ["filters"]))) return false;
  if (READ_ONLY_GIT_SUBCOMMANDS.has(sub)) return true;
  if (sub === "branch") return isGitListQuery(subArgs, GIT_BRANCH_FLAGS);
  if (sub === "tag") return isGitListQuery(subArgs, GIT_TAG_FLAGS);
  if (sub === "remote") {
    if (subArgs.length === 0 || (subArgs.length === 1 && (subArgs[0] === "-v" || subArgs[0] === "--verbose"))) return true;
    if (subArgs[0] !== "get-url") return false;
    const rest = subArgs.slice(1);
    return rest.every((a) => !a.startsWith("-") || a === "--push" || a === "--all") && rest.filter((a) => !a.startsWith("-")).length === 1;
  }
  return false;
}

/** Per-command argument policy for commands that are readers only with restricted options. */
const READ_ONLY_ARG_CHECKS: Record<string, (args: string[]) => boolean> = {
  grep: (args) => !args.some((a) => hasShortFlag(a, "R") || isBlockedLongOption(a, ["dereference-recursive"])),
  egrep: (args) => READ_ONLY_ARG_CHECKS.grep!(args),
  fgrep: (args) => READ_ONLY_ARG_CHECKS.grep!(args),
  rg: (args) =>
    !args.some((a) => hasShortFlag(a, "zL") || ["pre", "pre-glob", "hostname-bin", "search-zip", "follow"].includes(longOptionName(a) ?? "")),
  sort: (args) =>
    !args.some((a) => hasShortFlag(a, "oT") || isBlockedLongOption(a, ["output", "compress-program", "temporary-directory", "files0-from"])),
  uniq: isSafeUniqArgs,
  tree: isSafeTreeArgs,
  // Modes that read a list of file names from a file (manifests) or follow links inside operand directories
  // reach paths the operand check never sees.
  wc: (args) => !args.some((a) => isBlockedLongOption(a, ["files0-from"])),
  diff: (args) => !args.some((a) => hasShortFlag(a, "r") || isBlockedLongOption(a, ["recursive"])),
  shasum: (args) => !args.some((a) => hasShortFlag(a, "c") || isBlockedLongOption(a, ["check"])),
  sha256sum: (args) => READ_ONLY_ARG_CHECKS.shasum!(args),
  md5sum: (args) => READ_ONLY_ARG_CHECKS.shasum!(args),
  md5: (args) => READ_ONLY_ARG_CHECKS.shasum!(args),
  file: (args) => !args.some((a) => hasShortFlag(a, "Cf") || isBlockedLongOption(a, ["compile", "files-from"])),
  find: (args) =>
    !args.some((a) =>
      ["-exec", "-execdir", "-ok", "-okdir", "-delete", "-fprint", "-fprint0", "-fprintf", "-fls", "-L", "-H", "-follow", "-files0-from"].includes(a),
    ),
  sed: isSafeSedArgs,
  sqlite3: isSafeSqliteArgs,
  jq: (args) =>
    !args.some(
      (a) =>
        hasShortFlag(a, "Lf") ||
        isBlockedLongOption(a, ["library-path", "from-file"]) ||
        /\$ENV\b|\benv\b|\bimport\b|\binclude\b/.test(a),
    ),
  sg: isSafeAstGrepArgs,
  "ast-grep": isSafeAstGrepArgs,
  git: isReadOnlyGitCommand,
};

/** Filesystem scope for path operands: the shell's working directory and the canonical allowed read roots. */
export interface ReadOnlyShellScope {
  cwd: string;
  readRoots: string[];
  /** Read roots that refuse `.git` and `.harness/worktrees` when they are the first root admitting a path. */
  protectedRoots?: string[];
}

function isInsideRoot(root: string, path: string): boolean {
  const rel = relative(root, path);
  return !isAbsolute(rel) && rel !== ".." && !rel.startsWith(`..${sep}`);
}

/**
 * Every token is treated as a possible path: the whole token, the text after its first `=`, for options
 * the text from the first `/`, every attached short-option value, and for (quoted) glob characters
 * also the directory before the first glob. A `..` segment denies (the shell resolves it physically,
 * after symlinks). An existing candidate must
 * canonicalise inside a read root; a missing one reads nothing and is allowed.
 */
function pathOperandsInsideRoots(args: string[], scope: ReadOnlyShellScope): boolean {
  for (const arg of args) {
    if (arg.startsWith("-") && /\.\.(?:\/|$)/.test(arg)) return false;
    const candidates = [arg];
    const eq = arg.indexOf("=");
    if (eq >= 0) candidates.push(arg.slice(eq + 1));
    const slash = arg.indexOf("/");
    if (arg.startsWith("-") && slash >= 0) candidates.push(arg.slice(slash));
    // A short-option cluster may end in an attached value at any position (`-nfFILE`).
    if (/^-[^-]/.test(arg)) for (let k = 2; k < arg.length; k++) candidates.push(arg.slice(k));
    // A word with glob characters reaching here was quoted: a tool may treat it as a pattern (check the
    // directory before the glob) or as a literal name (check the whole word).
    for (const word of [...candidates]) {
      const glob = word.search(/[*?[]/);
      if (glob >= 0) candidates.push(word.slice(0, word.lastIndexOf("/", glob) + 1) || ".");
    }
    for (const candidate of candidates) {
      if (!candidate) continue;
      if (candidate.split("/").includes("..")) return false;
      const absolute = resolve(scope.cwd, candidate);
      try {
        lstatSync(absolute);
      } catch (error) {
        const code = (error as NodeJS.ErrnoException).code;
        if (code === "ENOENT" || code === "ENOTDIR") continue;
        return false;
      }
      let canonical: string;
      try {
        canonical = realpathSync(absolute);
      } catch {
        return false;
      }
      const admitting = scope.readRoots.find((root) => isInsideRoot(root, canonical));
      if (!admitting) return false;
      if (scope.protectedRoots?.includes(admitting) && isProtectedWritePath(admitting, canonical)) return false;
    }
  }
  return true;
}

/**
 * Validator for read-only shell inspection commands run by `read_only_workspace` ACP agents that send
 * permission callbacks (Codex, Gemini, Antigravity, OpenCode, Claude). Approves only when the token list
 * is exactly the argument vector the shell will run, every command word is a bare, known reader, and no
 * option that executes, writes, or redirects identity is present. With `scope`, every path operand must
 * also stay inside the allowed read roots. Unknown means deny. This gates harness approval only; it is
 * not an OS sandbox.
 */
export function isSafeReadOnlyShellCommand(rawCommand: string, scope?: ReadOnlyShellScope): boolean {
  // The shell splits words only on space, tab, and newline; JavaScript's \s and trim() also match other
  // whitespace, so any other whitespace or control character would make the token list differ from argv.
  if (CONTROL_CHARS.test(rawCommand) || NON_SHELL_WHITESPACE.test(rawCommand)) return false;
  if (!rawCommand.trim()) return false;
  const segments = splitShellSegments(rawCommand);
  if (!segments || segments.length === 0) return false;

  for (const seg of segments) {
    const words = tokenizeSegment(seg);
    let idx = 0;
    // Only a word whose name and `=` are unquoted is an assignment; a quoted one is the command word.
    while (idx < words.length && words[idx]!.assignment) {
      if (!SAFE_ENV_ASSIGNMENT.test(words[idx]!.text)) return false;
      idx++;
    }
    const tokens = words.map((w) => w.text);
    const bin = tokens[idx];
    if (bin === undefined || bin.includes("/") || /^[A-Za-z_][A-Za-z0-9_]*=/.test(bin)) return false;
    const args = tokens.slice(idx + 1);
    if (!PLAIN_READ_ONLY_BINS.has(bin)) {
      const check = Object.hasOwn(READ_ONLY_ARG_CHECKS, bin) ? READ_ONLY_ARG_CHECKS[bin] : undefined;
      if (!check?.(args)) return false;
    }
    if (scope && !pathOperandsInsideRoots(args, scope)) return false;
  }
  return true;
}

// Amp's static permission rules cannot check shell operands against the workspace roots.
const AMP_RESTRICTED_DISABLED_TOOLS = [
  ...AMP_READ_ONLY_DISABLED_TOOLS,
  "Bash",
  "shell_command",
  "shell_command_status",
  "shell_command_kill",
];

function isExecutableBinary(filePath: string): boolean {
  try {
    if (!statSync(filePath).isFile()) return false;
    accessSync(filePath, process.platform === "win32" ? constants.F_OK : constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

function findBinaryInDirs(binaryName: string, dirs: Iterable<string>): string | null {
  const candidates = process.platform === "win32" ? [binaryName, `${binaryName}.exe`] : [binaryName];
  const seen = new Set<string>();
  for (const rawDir of dirs) {
    const dir = rawDir.trim();
    if (!dir || seen.has(dir)) continue;
    seen.add(dir);
    for (const name of candidates) {
      const full = join(dir, name);
      if (isExecutableBinary(full)) return full;
    }
  }
  return null;
}

/**
 * Checks whether `codex-code-mode-host` is discoverable on `PATH` or alongside the Codex ACP binary
 * (including the binary's resolved symlink target directory).
 */
export function hasCodexCodeModeHost(command: string, env: Record<string, string | undefined>): boolean {
  const pathDirs = (env.PATH ?? "").split(delimiter).filter(Boolean);
  const searchDirs: string[] = [];

  const trimmed = command.trim();
  if (trimmed) {
    let resolvedCmd: string | null = null;
    if (trimmed.includes("/") || trimmed.includes("\\")) {
      searchDirs.push(dirname(trimmed));
      if (isExecutableBinary(trimmed)) resolvedCmd = trimmed;
    } else {
      resolvedCmd = findBinaryInDirs(trimmed, pathDirs);
      if (resolvedCmd) searchDirs.push(dirname(resolvedCmd));
    }
    if (resolvedCmd) {
      try {
        const real = realpathSync(resolvedCmd);
        searchDirs.push(dirname(real));
      } catch {
        // ignore symlink resolution errors
      }
    }
  }

  searchDirs.push(...pathDirs);
  return findBinaryInDirs("codex-code-mode-host", searchDirs) !== null;
}

export function isAmpAcpProfile(profile: Pick<AcpProfileConfig, "id" | "adapter" | "command" | "args">): boolean {
  if (profile.adapter !== "generic") return false;
  const hay = [profile.id, profile.command, ...(profile.args ?? [])].join(" ").toLowerCase();
  return hay.includes("amp-acp") || /\bamp\b/.test(profile.id.toLowerCase());
}

export function prepareAcpSpawn(
  profile: AcpProfileConfig,
  opts: {
    systemPrompt: string;
    toolPolicy: ToolPolicy;
    allowShellInReadOnly?: boolean;
  },
): PreparedAcpSpawn {
  if (profile.enabled === false) throw new Error(`Runner profile "${profile.id}" is disabled.`);
  const env: Record<string, string> = {};
  for (const [k, v] of Object.entries(process.env)) {
    if (v !== undefined) env[k] = v;
  }
  if (profile.env) {
    Object.assign(env, profile.env);
  }
  // A profile env can blank TMPDIR; agents always get this host's resolved temp directory instead.
  if (!env.TMPDIR?.trim()) env.TMPDIR = ensureTempDir();

  const args = [...(profile.args ?? [])];
  const tempDirs: string[] = [];
  let prependSystemPromptToTurn1 = false;

  if (profile.adapter === "claude") {
    // Prevent silent auto-compaction so harness controls clean rollover via usage_update
    env.DISABLE_COMPACT = "1";
  } else if (profile.adapter === "codex") {
    // Block Codex's internal multi_agent subagents via BOTH CLI flag and CODEX_CONFIG JSON
    // (codex-acp v2.9+ reads features from CODEX_CONFIG and can otherwise shadow harness MCP from ~/.codex/config.toml).
    args.push("-c", "features.multi_agent=false");
    let existingCodexCfg: Record<string, unknown> = {};
    if (env.CODEX_CONFIG) {
      try {
        const parsed = JSON.parse(env.CODEX_CONFIG);
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
          existingCodexCfg = parsed as Record<string, unknown>;
        }
      } catch {
        // ignore invalid user CODEX_CONFIG JSON
      }
    }
    const existingFeatures =
      existingCodexCfg.features && typeof existingCodexCfg.features === "object" && !Array.isArray(existingCodexCfg.features)
        ? (existingCodexCfg.features as Record<string, unknown>)
        : {};
    const hasExplicitCodeModeHost =
      existingFeatures.code_mode_host !== undefined ||
      args.some((a) => /(?:^|\s)features\.code_mode_host\s*=/.test(a));
    const disableCodeModeHost = !hasExplicitCodeModeHost && !hasCodexCodeModeHost(profile.command, env);
    if (disableCodeModeHost) {
      // Codex CLI 0.170+ fails closed on MCP tools if `codex-code-mode-host` is absent and code_mode_host stays enabled.
      args.push("-c", "features.code_mode_host=false");
    }
    env.CODEX_CONFIG = JSON.stringify({
      ...existingCodexCfg,
      features: {
        ...existingFeatures,
        multi_agent: false,
        ...(disableCodeModeHost ? { code_mode_host: false } : {}),
      },
    });
    env.DISABLE_MCP_CONFIG_FILTERING = "true";
    // Only lock Codex into OS Seatbelt read-only sandbox when shell verification is NOT requested;
    // disposable verification checkouts (`allowShellInReadOnly: true`) need `/tmp` and test runners to work.
    if (opts.toolPolicy !== "read_write_workspace" && !opts.allowShellInReadOnly) {
      env.INITIAL_AGENT_MODE = "read-only";
    }
  } else if (profile.adapter === "gemini") {
    // Shim 1: gemini --acp ignores NewSessionRequest.systemPrompt; use GEMINI_SYSTEM_MD
    const dir = mkdtempSync(join(tmpdir(), "pi-harness-gemini-"));
    tempDirs.push(dir);
    const mdPath = join(dir, "system.md");
    writeFileSync(mdPath, opts.systemPrompt, "utf8");
    env.GEMINI_SYSTEM_MD = mdPath;
    if (
      opts.toolPolicy === "read_write_workspace" &&
      !args.some((a) => a === "--yolo" || a === "-y" || a.startsWith("--approval-mode"))
    ) {
      args.push("--approval-mode=yolo");
    }
  } else {
    prependSystemPromptToTurn1 = true;
    // Amp ACP (`amp-acp`) never sends `session/request_permission` callbacks; configure unattended permissions
    // and control-plane / read-only boundaries cleanly via a temporary AMP_SETTINGS_FILE!
    if (isAmpAcpProfile(profile)) {
      const dir = mkdtempSync(join(tmpdir(), "pi-harness-amp-"));
      tempDirs.push(dir);
      const settingsPath = join(dir, "settings.json");
      const ampSettings: Record<string, unknown> =
        opts.toolPolicy === "read_write_workspace"
          ? {
              "amp.dangerouslyAllowAll": true,
            }
          : opts.toolPolicy === "control_plane_only"
            ? {
                "amp.dangerouslyAllowAll": false,
                "amp.tools.enable": ["mcp__harness__*"],
                "amp.tools.disable": [
                  "Bash",
                  "shell_command",
                  "shell_command_status",
                  "shell_command_kill",
                  "Read",
                  "Grep",
                  "glob",
                  ...AMP_READ_ONLY_DISABLED_TOOLS,
                ],
                "amp.permissions": [{ tool: "mcp__harness__*", action: "allow" }],
              }
            : opts.allowShellInReadOnly
              ? {
                  // Disposable verification checkout (verify / Done-Review): keep shell and harness MCP unrestricted
                  // for tests/linters/tmux while hiding and rejecting file-editing and subagent tools.
                  "amp.dangerouslyAllowAll": false,
                  "amp.tools.disable": AMP_READ_ONLY_DISABLED_TOOLS,
                  "amp.permissions": [
                    { tool: "mcp__harness__*", action: "allow" },
                    ...[...OCTOFS_READ_ONLY_TOOLS].map((t) => ({ tool: `mcp__octofs__${t}`, action: "allow" })),
                    { tool: "mcp__ast-grep__*", action: "allow" },
                    ...AMP_READ_ONLY_DISABLED_TOOLS.map((tool) => ({ tool, action: "reject" })),
                    { tool: "shell_command", action: "allow" },
                    { tool: "Bash", action: "allow" },
                  ],
                }
              : {
                  "amp.dangerouslyAllowAll": false,
                  "amp.tools.disable": AMP_RESTRICTED_DISABLED_TOOLS,
                  "amp.permissions": [
                    { tool: "mcp__harness__*", action: "allow" },
                    ...[...OCTOFS_READ_ONLY_TOOLS].map((t) => ({ tool: `mcp__octofs__${t}`, action: "allow" })),
                    { tool: "mcp__ast-grep__*", action: "allow" },
                    ...AMP_RESTRICTED_DISABLED_TOOLS.map((tool) => ({ tool, action: "reject" })),
                  ],
                };
      writeFileSync(settingsPath, JSON.stringify(ampSettings, null, 2), "utf8");
      env.AMP_SETTINGS_FILE = settingsPath;
    }
  }

  if (opts.toolPolicy !== "read_write_workspace" && profile.readOnlyArgs?.length) {
    args.push(...profile.readOnlyArgs);
  }

  return {
    command: profile.command,
    args,
    env,
    prependSystemPromptToTurn1,
    cleanup: () => {
      for (const dir of tempDirs) {
        try {
          rmSync(dir, { recursive: true, force: true });
        } catch {
          // ignore cleanup failures
        }
      }
    },
  };
}

export function buildNewSessionParams(
  adapter: AcpAdapterKind,
  opts: {
    cwd: string;
    systemPrompt: string;
    toolPolicy: ToolPolicy;
    allowShellInReadOnly?: boolean;
    model: string;
    effort?: string | null;
    mcpServers: AcpMcpServerEntry[];
    agentCapabilities?: Record<string, unknown>;
    /** Resolved absolute directories outside `cwd` the agent may access (extra read and write roots). */
    additionalDirectories?: string[];
  },
): { params: Record<string, unknown>; needsTurn1SystemPrompt: boolean } {
  const params: Record<string, unknown> = {
    cwd: opts.cwd,
    mcpServers: opts.mcpServers,
  };

  const sessionCaps = (opts.agentCapabilities?.sessionCapabilities ?? {}) as Record<string, unknown>;
  let needsTurn1SystemPrompt = false;

  // Shim 1: System Prompt
  if (adapter === "claude" || adapter === "codex") {
    params.systemPrompt = {
      mode: "append",
      content: [{ type: "text", text: opts.systemPrompt }],
    };
  } else if (adapter === "generic") {
    if (sessionCaps.systemPrompt) {
      params.systemPrompt = {
        mode: "append",
        content: [{ type: "text", text: opts.systemPrompt }],
      };
    } else {
      needsTurn1SystemPrompt = true;
    }
  }

  // Shim 2 & 3 for Claude: initial configOptions + disallowedTools via _meta.claudeCode.options
  if (adapter === "claude") {
    const disallowed = ["Agent", "Task"];
    if (opts.toolPolicy !== "read_write_workspace") {
      disallowed.push(
        "Write",
        "Edit",
        "MultiEdit",
        "NotebookEdit",
        ...[...OCTOFS_MUTATING_TOOLS].map((t) => `mcp__octofs__${t}`),
      );
      if (!opts.allowShellInReadOnly) disallowed.push("Bash");
    }
    if (opts.toolPolicy === "control_plane_only") {
      disallowed.push("Read", "Grep", "Glob", "WebFetch", "WebSearch", "Skill");
      if (!disallowed.includes("Bash")) disallowed.push("Bash");
    }
    const effectiveEffort =
      opts.effort && opts.effort.toLowerCase() !== "default" ? opts.effort : undefined;
    const claudeOptions: Record<string, unknown> = {
      disallowedTools: disallowed,
      ...(opts.toolPolicy !== "read_write_workspace" ? {
        settingSources: [],
        strictMcpConfig: true,
        allowDangerouslySkipPermissions: false,
      } : {}),
      // Keep ToolSearch available in control_plane_only so Claude Code can load deferred MCP tools (mcp__harness__*)!
      ...(opts.toolPolicy === "control_plane_only" ? {
        tools: ["ToolSearch"],
      } : {}),
      settings: { autoCompactEnabled: false },
    };
    claudeOptions.model = opts.model;
    if (effectiveEffort) claudeOptions.effort = effectiveEffort;
    // claude-agent-acp merges this SDK pass-through with ACP `additionalDirectories` into the SDK options.
    if (opts.toolPolicy !== "control_plane_only" && opts.additionalDirectories?.length) {
      claudeOptions.additionalDirectories = [...new Set(opts.additionalDirectories)];
    }

    params._meta = {
      claudeCode: {
        options: claudeOptions,
      },
    };

    if (sessionCaps.options) {
      const cfg: Array<{ configId: string; value: string }> = [];
      cfg.push({ configId: "model", value: opts.model });
      if (effectiveEffort) cfg.push({ configId: "effort", value: effectiveEffort });
      if (cfg.length > 0) params.configOptions = cfg;
    }
  }

  if (adapter === "codex" && opts.mcpServers.length > 0) {
    // Ensure codex-acp awaits harness MCP bridge startup before Turn 1 instead of racing.
    const envAwait = Number(process.env.PI_HARNESS_MCP_AWAIT_MS);
    params._meta = {
      mcpStartupAwaitTimeoutMs: Number.isFinite(envAwait) && envAwait > 0 ? envAwait : 15_000,
    };
  }

  return { params, needsTurn1SystemPrompt };
}

export interface PostSessionAction {
  method: "session/set_config_option" | "session/set_model" | "session/set_mode";
  params: Record<string, unknown>;
  /** Prompting is unsafe unless this setter succeeds for the exact wire ID. */
  requiredModel?: string;
}

interface SelectValue { value: string; name?: string; description?: string }

const CLAUDE_VERSIONED_MODEL_RE = /^claude-(opus|sonnet|haiku|fable)-(\d+(?:[.-]\d+)*)(?:-\d{8})?$/i;
const CLAUDE_EXPLICIT_METADATA_ID_RE = /\b(?:anthropic\/)?(claude-(opus|sonnet|haiku|fable)-(\d+(?:[.-]\d+)*)(?:-\d{8})?)\b/i;
const CLAUDE_OPTION_NAME_VERSION_RE = /\b(?:claude\s+)?(opus|sonnet|haiku|fable)\s+(\d+(?:[.-]\d+)*)\b/i;

/**
 * Checks whether a Claude ACP configOption entry (`{ value: "opus", name: "Opus 5.5" }`)
 * proves exact compatibility with a specific requested wire model ID (`"claude-opus-5-5"`).
 * Never matches an unversioned display name (`"Opus"`) or a different version (`"Opus 5.0"`).
 */
export function claudeOptionMatchesExactModel(opt: SelectValue, requestedModel: string): boolean {
  if (opt.value === requestedModel) return true;
  const reqMatch = requestedModel.trim().match(CLAUDE_VERSIONED_MODEL_RE);
  if (!reqMatch) return false;
  const reqFamily = reqMatch[1]!.toLowerCase();
  const reqVer = reqMatch[2]!.replace(/\./g, "-");
  for (const field of [opt.description, opt.name]) {
    if (!field) continue;
    const explicit = field.match(CLAUDE_EXPLICIT_METADATA_ID_RE);
    if (explicit) {
      // Once explicit versioned metadata is present, it is authoritative — never fall through to stale display text
      const expFamily = explicit[2]!.toLowerCase();
      const expVer = explicit[3]!.replace(/\./g, "-");
      return expFamily === reqFamily && expVer === reqVer;
    }
  }
  if (!opt.name) return false;
  const optMatch = opt.name.match(CLAUDE_OPTION_NAME_VERSION_RE);
  if (!optMatch) return false;
  const optFamily = optMatch[1]!.toLowerCase();
  const optVer = optMatch[2]!.replace(/\./g, "-");
  return reqFamily === optFamily && reqVer === optVer;
}

/** Checks whether a reported current model value satisfies `requestedModel` given advertised options. */
export function reportedModelSatisfies(
  adapter: AcpAdapterKind,
  state: AcpModelState,
  reportedValue: unknown,
  requestedModel: string,
  fallbackState?: AcpModelState,
): boolean {
  if (reportedValue === requestedModel) return true;
  if (adapter !== "claude" || typeof reportedValue !== "string") return false;
  const findOpt = (st?: AcpModelState) =>
    selectValues(st?.configOptions?.find((c) => c.category === "model" || c.id === "model"))?.find(
      (o) => o.value === reportedValue,
    );
  const opt = findOpt(state) ?? findOpt(fallbackState);
  return opt ? claudeOptionMatchesExactModel(opt, requestedModel) : false;
}

export interface AcpConfigOption {
  id: string;
  category?: string;
  currentValue?: unknown;
  options?: Array<SelectValue | { options: SelectValue[] }>;
}

export function selectValues(option?: AcpConfigOption): SelectValue[] | undefined {
  return option?.options?.flatMap((item) => "options" in item ? item.options : [item]);
}

export interface AcpModelState {
  configOptions?: AcpConfigOption[];
  models?: { currentModelId?: string; availableModels?: Array<{ modelId: string; name?: string; description?: string }> };
  modes?: { currentModeId?: string; availableModes?: Array<{ id: string; name?: string }> };
}

export function reportedModels(state: AcpModelState, modelConfigIds = new Set(["model"])): unknown[] {
  for (const option of state.configOptions ?? []) {
    if (option.category === "model") modelConfigIds.add(option.id);
  }
  const values = state.configOptions
    ?.filter((option) => modelConfigIds.has(option.id))
    .map((option) => option.currentValue) ?? [];
  const legacyId = state.models?.currentModelId;
  // Codex legacy `models.currentModelId` appends `[effort]` (e.g. `gpt-6.2-astra[medium]`) while
  // `configOptions` reports the bare model ID (`gpt-6.2-astra`). Do not treat that as a mismatch.
  const normalizedLegacy =
    typeof legacyId === "string" && values.some((v) => typeof v === "string" && legacyId.startsWith(`${v}[`))
      ? undefined
      : legacyId;
  return [...values, normalizedLegacy].filter((value) => value !== undefined);
}

/**
 * Computes post-`session/new` configuration calls (model and effort)
 * tailored to each adapter's verified ACP behavior.
 */
export function buildPostSessionActions(
  adapter: AcpAdapterKind,
  sessionId: string,
  newSessionResponse: AcpModelState,
  opts: { toolPolicy: ToolPolicy; model: string; effort?: string | null },
  modelConfigIds = new Set(["model"]),
): PostSessionAction[] {
  const actions: PostSessionAction[] = [];
  const configOptions = newSessionResponse.configOptions ?? [];
  const legacyModels = newSessionResponse.models;

  const modelOpt = configOptions.find((c) => c.category === "model" || c.id === "model");
  const effortOpt = configOptions.find(
    (c) => c.category === "thought_level" || c.id === "reasoning_effort" || c.id === "effort",
  );

  const model = opts.model;
  if (!model?.trim()) throw new Error(`${adapter}: a nonblank wire model ID is required`);
  // Never translate the catalog's selected wire ID. Check the list belonging to
  // the setter we will use, not the union of two potentially different lists.
  const useConfig = modelOpt !== undefined || adapter === "codex" || (adapter === "claude" && !legacyModels);
  const configOptionsList = selectValues(modelOpt);
  const configIds = configOptionsList?.map((o) => o.value);
  const legacyIds = legacyModels?.availableModels?.map((m) => m.modelId);
  const advertised = useConfig ? configIds ?? legacyIds : legacyIds ?? configIds;
  const claudeExactOptionMatch =
    adapter === "claude" && useConfig && configOptionsList
      ? configOptionsList.some((o) => claudeOptionMatchesExactModel(o, model))
      : false;
  if (advertised && !advertised.includes(model) && !claudeExactOptionMatch) {
    throw new Error(`${adapter}: requested model '${model}' is not advertised by this session`);
  }
  const current = reportedModels(newSessionResponse, modelConfigIds);
  if (!current.length || current.some((value) => !reportedModelSatisfies(adapter, newSessionResponse, value, model))) {
    actions.push(useConfig
      ? { method: "session/set_config_option", params: { sessionId, configId: modelOpt?.id ?? "model", value: model }, requiredModel: model }
      : { method: "session/set_model", params: { sessionId, modelId: model }, requiredModel: model });
  }

  // Effort selection: only send a value the adapter advertises; skip cleanly otherwise
  if (opts.effort && opts.effort.toLowerCase() !== "default") {
    const wanted = opts.effort.toLowerCase();
    const efforts = selectValues(effortOpt);
    if (effortOpt && efforts?.length) {
      const match = efforts.find(
        (o) => o.value.toLowerCase() === wanted || o.name?.toLowerCase() === wanted,
      );
      if (match && match.value !== effortOpt.currentValue) {
        actions.push({
          method: "session/set_config_option",
          params: { sessionId, configId: effortOpt.id, value: match.value },
        });
      }
    } else if (adapter === "codex") {
      // Codex advertises nothing to validate against before set_config_option: keep its verified config id
      actions.push({
        method: "session/set_config_option",
        params: { sessionId, configId: effortOpt?.id ?? "reasoning_effort", value: opts.effort },
      });
    }
  }

  // Unattended approval / yolo mode for writable workers (`read_write_workspace`):
  // if the ACP adapter advertises a `mode` option with a yolo/bypass value (Claude, Gemini, Antigravity, Codex),
  // switch into it automatically so unattended runs never stall waiting on manual permission prompts.
  if (opts.toolPolicy === "read_write_workspace") {
    const YOLO_PRIORITY = ["bypasspermissions", "yolo", "full-access", "full_access", "dontask", "auto"];
    const modeOpt = configOptions.find((c) => c.category === "mode" || c.id === "mode");
    const modeValues = selectValues(modeOpt);
    if (modeOpt && modeValues?.length) {
      const currentMode = String(modeOpt.currentValue ?? "").toLowerCase();
      if (!YOLO_PRIORITY.includes(currentMode)) {
        const picked = YOLO_PRIORITY.map((p) => modeValues.find((o) => o.value.toLowerCase() === p)).find(Boolean);
        if (picked && picked.value !== modeOpt.currentValue) {
          actions.push({
            method: "session/set_config_option",
            params: { sessionId, configId: modeOpt.id, value: picked.value },
          });
        }
      }
    } else if (newSessionResponse.modes?.availableModes?.length) {
      const currentMode = String(newSessionResponse.modes.currentModeId ?? "").toLowerCase();
      if (!YOLO_PRIORITY.includes(currentMode)) {
        const picked = YOLO_PRIORITY.map((p) =>
          newSessionResponse.modes!.availableModes!.find((m) => m.id.toLowerCase() === p),
        ).find(Boolean);
        if (picked && picked.id !== newSessionResponse.modes.currentModeId) {
          actions.push({
            method: "session/set_mode",
            params: { sessionId, modeId: picked.id },
          });
        }
      }
    }
  }

  return actions;
}

/**
 * Shim 4: Extracts deterministic Artifact Index entries from ACP tool_call / tool_call_update
 * strictly relying on `kind`, `locations`, and `content` (`diff`) — never requiring `rawInput`/`rawOutput`
 * (since `gemini --acp` omits both).
 */
export function extractArtifactsFromToolUpdate(
  update: Record<string, unknown>,
): Array<Omit<ArtifactRecord, "id" | "runId" | "taskId" | "createdAt">> {
  const results: Array<Omit<ArtifactRecord, "id" | "runId" | "taskId" | "createdAt">> = [];
  const status = String(update.status ?? "");
  if (status !== "completed" && status !== "failed") return results;

  const acpKind = String(update.kind ?? "other");
  const title = String(update.title ?? update.name ?? "tool");
  const locations = (update.locations ?? []) as Array<{ path?: string; line?: number }>;
  const content = (update.content ?? []) as Array<Record<string, unknown>>;

  const mappedKind: ArtifactRecord["kind"] | null =
    acpKind === "read" || acpKind === "search"
      ? "read"
      : acpKind === "edit" || acpKind === "move"
        ? "edit"
        : acpKind === "delete"
          ? "delete"
          : acpKind === "execute"
            ? "execute"
            : null;

  if (!mappedKind) return results;

  // Prefer structured diff content paths first
  const seenPaths = new Set<string>();
  for (const item of content) {
    if (item.type === "diff" && typeof item.path === "string" && item.path) {
      seenPaths.add(item.path);
      results.push({
        kind: "edit",
        path: item.path,
        summary: `${title} (${status})`,
        gitSha: null,
        exitCode: null,
      });
    }
  }

  for (const loc of locations) {
    if (typeof loc.path === "string" && loc.path && !seenPaths.has(loc.path)) {
      seenPaths.add(loc.path);
      results.push({
        kind: mappedKind,
        path: loc.path,
        summary: `${title}${loc.line ? `:${loc.line}` : ""} (${status})`,
        gitSha: null,
        exitCode: null,
      });
    }
  }

  if (mappedKind === "execute" && results.length === 0) {
    results.push({
      kind: "execute",
      path: title,
      summary: `${title} (${status})`,
      gitSha: null,
      exitCode: status === "completed" ? 0 : 1,
    });
  }

  return results;
}
