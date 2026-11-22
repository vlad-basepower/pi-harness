import type { ToolDefinition } from "../runners/types.js";

import { sandboxAvailable } from "./sandbox.js";

export type CodeMode = "collapsed" | "hybrid" | "off";

export function effectiveCodeMode(requested: CodeMode): CodeMode {
  return requested !== "off" && sandboxAvailable() ? requested : "off";
}

export const CODE_EXEC_TOOL_NAME = "code_exec";
export const TOOL_SEARCH_TOOL_NAME = "tool_search";

export const CODE_EXEC_TOOL: ToolDefinition = {
  name: CODE_EXEC_TOOL_NAME,
  description: 'Chain allowed tools in a Bun TypeScript script with top-level await. Use import { ... } from "harness" and output(value) to return JSON. Find signatures with tool_search. Each serialized IPC message must fit in 16 MiB. Cancellation stops the script, not tool actions already started. Set language: "python" to run Python instead: use `from harness import name, output` (find Python signatures with tool_search language "python") and list third-party wheels in packages as "name==version".',
  inputSchema: {
    type: "object",
    required: ["code"],
    properties: {
      code: { type: "string", description: 'TypeScript source with top-level await and output(value) support (Python source when language is "python").' },
      language: { type: "string", enum: ["typescript", "python"], description: "Script language. Omit for TypeScript." },
      packages: {
        type: "array", items: { type: "string" },
        description: 'Python only: exact wheel pins such as "pyyaml==6.0.3". Installed from PyPI (wheels only) into a cached, hash-locked environment; the script itself has no network.',
      },
      timeout_s: { type: "number", description: "Execution timeout in seconds. Omit to use the configured default." },
    },
  },
};

export const TOOL_SEARCH_TOOL: ToolDefinition = {
  name: TOOL_SEARCH_TOOL_NAME,
  description: "Find allowed harness tools and return their TypeScript signatures for code_exec. Omit filters to list all script tools.",
  inputSchema: {
    type: "object",
    properties: {
      query: { type: "string", description: "Keywords to match against tool names and descriptions." },
      names: { type: "array", items: { type: "string" }, description: "Exact tool names to look up." },
      language: { type: "string", enum: ["typescript", "python"], description: 'Signature language. Omit for TypeScript; "python" returns the Python import surface.' },
    },
  },
};

export function exposedTools(allowed: ToolDefinition[], mode: CodeMode): ToolDefinition[] {
  if (effectiveCodeMode(mode) === "off") return allowed;
  if (mode === "collapsed") return [TOOL_SEARCH_TOOL, CODE_EXEC_TOOL];
  return [...allowed.filter((tool) => !isCodeModeTool(tool.name)), TOOL_SEARCH_TOOL, CODE_EXEC_TOOL];
}

function isCodeModeTool(name: string): boolean {
  return name === CODE_EXEC_TOOL_NAME || name === TOOL_SEARCH_TOOL_NAME;
}

// Function declarations need a strict-mode binding name, not just a JS property name.
const reservedBindings = new Set(
  ("abstract arguments await break case catch class const continue debugger default delete do else enum eval export extends " +
    "false finally for function if implements import in instanceof interface let new null package private protected public " +
    "return static super switch this throw true try typeof var void while with yield").split(" "),
);

function scriptTools(tools: ToolDefinition[]): ToolDefinition[] {
  return tools.filter(({ name }) =>
    !isCodeModeTool(name) && name !== "output" && /^[A-Za-z_$][\w$]*$/.test(name) && !reservedBindings.has(name),
  );
}

function schemaObject(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function comment(value: unknown): string {
  if (typeof value !== "string" || !value) return "";
  return `/** ${value.replace(/[\r\n\u2028\u2029]+/g, " ").replaceAll("*/", "* /")} */ `;
}

function quote(value: string): string {
  return JSON.stringify(value).replaceAll("\u2028", "\\u2028").replaceAll("\u2029", "\\u2029");
}

function schemaType(value: unknown): string {
  const schema = schemaObject(value);
  if (Array.isArray(schema.enum) && schema.enum.length > 0) {
    return schema.enum.map((item) => {
      if (typeof item === "string") return quote(item);
      if (item === null || typeof item === "boolean" || Number.isFinite(item)) return String(item);
      return "unknown";
    }).join(" | ");
  }
  switch (schema.type) {
    case "string":
    case "boolean":
    case "null":
    case "number": return schema.type;
    case "integer": return "number";
    case "array": return `Array<${schemaType(schema.items)}>`;
    case "object": {
      const properties = schemaObject(schema.properties);
      const required = new Set(Array.isArray(schema.required) ? schema.required : []);
      const keys = [...new Set([...Object.keys(properties), ...[...required].filter((key) => typeof key === "string")])];
      if (!keys.length) return "Record<string, unknown>";
      const fields = keys.map((key) => {
        const property = Object.hasOwn(properties, key) ? schemaObject(properties[key]) : {};
        const name = /^[A-Za-z_$][\w$]*$/.test(key) ? key : quote(key);
        return `${comment(property.description)}${name}${required.has(key) ? "" : "?"}: ${schemaType(property)};`;
      });
      return `{ ${fields.join(" ")} }`;
    }
    default: return "unknown";
  }
}

export function generateSdkDeclarations(tools: ToolDefinition[]): string {
  const signatures = scriptTools(tools).map((tool) =>
    `  ${comment(tool.description)}export function ${tool.name}(input: ${schemaType(tool.inputSchema)}): Promise<unknown>;`,
  );
  return ['declare module "harness" {', ...signatures, "  export function output(value: unknown): void;", "}", ""].join("\n");
}

const pythonKeywords = new Set(
  ("False None True and as assert async await break class continue def del elif else except finally for from global if import " +
    "in is lambda nonlocal not or pass raise return try while with yield").split(" "),
);

// Tools whose names are valid Python identifiers; others stay reachable through `harness.call(name, args)`.
export function pythonTools(tools: ToolDefinition[]): ToolDefinition[] {
  return scriptTools(tools).filter(({ name }) =>
    /^[A-Za-z]\w*$/.test(name) && !pythonKeywords.has(name) && name !== "call",
  );
}

function pythonType(value: unknown): string {
  const schema = schemaObject(value);
  if (Array.isArray(schema.enum) && schema.enum.length > 0) return `Literal[${schema.enum.map((item) => JSON.stringify(item) ?? "None").join(", ")}]`;
  switch (schema.type) {
    case "string": return "str";
    case "boolean": return "bool";
    case "null": return "None";
    case "number": return "float";
    case "integer": return "int";
    case "array": return `list[${pythonType(schema.items)}]`;
    case "object": {
      const properties = schemaObject(schema.properties);
      const required = new Set(Array.isArray(schema.required) ? schema.required : []);
      const keys = [...new Set([...Object.keys(properties), ...[...required].filter((key) => typeof key === "string")])];
      if (!keys.length) return "dict[str, Any]";
      return `{ ${keys.map((key) => `${quote(key)}${required.has(key) ? "" : "?"}: ${pythonType(properties[key])}`).join(", ")} }`;
    }
    default: return "Any";
  }
}

export function generatePythonDeclarations(tools: ToolDefinition[]): string {
  const stubs = pythonTools(tools).map((tool) => {
    const doc = `${(tool.description ?? "").replace(/[\r\n\u2028\u2029]+/g, " ")} Input: ${pythonType(tool.inputSchema)}`
      .replaceAll("\\", "/").replaceAll('"', "'").trim();
    return `def ${tool.name}(input: dict[str, Any] | None = None, /, **fields: Any) -> Any:\n    """${doc}"""`;
  });
  return [
    "# module: harness (from harness import <name>, output)",
    "from typing import Any, Literal",
    "",
    ...stubs,
    "def call(name: str, input: dict[str, Any] | None = None, /, **fields: Any) -> Any: ...  # any allowed tool by exact name",
    "def output(value: Any) -> None: ...  # return a JSON-serializable value from the script",
    "",
  ].join("\n");
}

const PYTHON_FUNCTION_NOTE =
  'Python code_exec scripts (language: "python") import these as `from harness import name, output`, call them with a dict or keyword arguments, and return JSON with output(value). Use `harness.call(name, args)` for tools not listed. They are direct tools only if your own tool list contains that exact name.';

// Clients such as Amp expose MCP tools as `mcp__<server>__<tool>`; script functions use the bare name.
const clientPrefix = /^mcp__harness__/;

const SCRIPT_FUNCTION_NOTE =
  'These are functions for code_exec scripts: `import { name } from "harness"`. They are callable as direct tools only if your own tool list contains that exact name. Otherwise call them only inside a code_exec script.';

export function toolSearch(
  allowed: ToolDefinition[],
  args: { query?: string; names?: string[]; language?: string },
): { signatures: string; matched: string[]; note: string } {
  const terms = args.query?.trim().toLowerCase().split(/\s+/).filter(Boolean) ?? [];
  const names = new Set(args.names?.map((name) => name.replace(clientPrefix, "")));
  const tools = scriptTools(allowed).filter((tool) => {
    const text = `${tool.name} ${tool.description}`.toLowerCase();
    return (!names.size || names.has(tool.name)) && terms.every((term) => text.includes(term));
  });
  if (args.language === "python") {
    const py = pythonTools(tools);
    return { signatures: generatePythonDeclarations(py), matched: py.map(({ name }) => name), note: PYTHON_FUNCTION_NOTE };
  }
  return { signatures: generateSdkDeclarations(tools), matched: tools.map(({ name }) => name), note: SCRIPT_FUNCTION_NOTE };
}

export function codeModePromptSection(mode: CodeMode): string {
  if (effectiveCodeMode(mode) === "off") return "";
  return [
    "## Code Mode",
    mode === "hybrid" ? "Call harness tools directly for single calls, or use code_exec to combine calls in a script." :
      "The only callable harness tools are tool_search and code_exec. Every other harness tool named in these instructions is a function inside a code_exec script. Calling it as a tool fails because it is not in your tool list.",
    'Find allowed tools with tool_search. In code_exec, use `import { ... } from "harness"`, top-level await, and output(value) for the final JSON result.',
    'Your client may prefix tool names (for example mcp__harness__code_exec) or wrap this server behind its own tool_search and code_exec. Then `harness` in the client\'s own script exports only code_exec and tool_search, so importing any other harness function there fails. Call the harness code_exec with `{ code }` and put the `import { ... } from "harness"` line inside that code string.',
    'In the Amp native wrapper, use `text(result)` to display the result of the nested call. Use `output(value)` inside the harness code_exec script to return a value to Amp.',
    'code_exec also accepts language: "python" (default TypeScript): `from harness import name, output`; tool_search with language "python" lists Python signatures; packages: ["name==version"] installs pinned wheels.',
    "Set timeout_s for long waits (default 300s, maximum 1800s unless configured otherwise). Results over the inline limit (default 20,000 characters) return a preview and artifact path.",
  ].join("\n\n");
}
