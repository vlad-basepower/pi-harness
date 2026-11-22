import { toolEventPreview } from "../protocol/tool-display.js";
import {
  createModels,
  createProvider,
  getSupportedThinkingLevels,
  type Api,
  type Context,
  type Message,
  type Provider,
} from "@earendil-works/pi-ai";
import { anthropicMessagesApi } from "@earendil-works/pi-ai/api/anthropic-messages.lazy";
import { googleGenerativeAIApi } from "@earendil-works/pi-ai/api/google-generative-ai.lazy";
import { openAICompletionsApi } from "@earendil-works/pi-ai/api/openai-completions.lazy";
import { openAIResponsesApi } from "@earendil-works/pi-ai/api/openai-responses.lazy";
import { anthropicProvider } from "@earendil-works/pi-ai/providers/anthropic";
import { googleProvider } from "@earendil-works/pi-ai/providers/google";
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai";
import type { PiProfileConfig } from "../config.js";
import { detectQuotaErrorText } from "./quota-detect.js";
import { validateRunPolicy } from "./types.js";
import type { ProbedModelInfo, RunAgentRequest, RunAgentResult, RunnerUsage } from "./types.js";

const BUILTIN_PROVIDERS = new Map<string, [() => Provider<Api>, string]>([
  ["openai", [openaiProvider as () => Provider<Api>, "OPENAI_API_KEY"]],
  ["anthropic", [anthropicProvider as () => Provider<Api>, "ANTHROPIC_API_KEY"]],
  ["google", [googleProvider as () => Provider<Api>, "GEMINI_API_KEY"]],
]);

const API_FACTORIES = new Map([
  ["openai-completions", openAICompletionsApi],
  ["openai-responses", openAIResponsesApi],
  ["anthropic-messages", anthropicMessagesApi],
  ["google-generative-ai", googleGenerativeAIApi],
]);

/**
 * Native `@earendil-works/pi-ai` lane for direct API keys & local OpenAI-compatible servers.
 * Executes harness tools directly in-process with zero MCP subprocess overhead, strict
 * rollover threshold checking, and exact normalized token usage tracking.
 */
export class PiRunner {
  constructor(private readonly profile: PiProfileConfig) {}

  probeModels(): ProbedModelInfo[] {
    const builtin = BUILTIN_PROVIDERS.get(this.profile.provider);
    if (this.profile.models && this.profile.models.length > 0) {
      return this.profile.models.map((wireModelId) => ({ wireModelId }));
    }
    if (!builtin) return [];
    const catalog = builtin[0]();
    return catalog.getModels().map((m) => ({
      wireModelId: m.id,
      name: m.name,
      supportedEfforts: getSupportedThinkingLevels(m),
    }));
  }

  async run(req: RunAgentRequest): Promise<RunAgentResult> {
    validateRunPolicy(req);
    if (this.profile.enabled === false) throw new Error(`Pi profile ${this.profile.id} is disabled`);
    const modelId = req.model;
    const builtin = BUILTIN_PROVIDERS.get(this.profile.provider);
    const catalog = builtin?.[0]();
    const builtinModel = catalog?.getModels().find((m) => m.id === modelId);
    if (!(this.profile.models?.length ? this.profile.models.includes(modelId) : builtinModel)) {
      throw new Error(`Model ${modelId} is unavailable on Pi profile ${this.profile.id}. Select a current catalog route.`);
    }
    const baseUrl = this.profile.baseUrl ?? catalog?.baseUrl;
    if (!baseUrl) {
      throw new Error(`Custom pi profile ${this.profile.id} requires baseUrl`);
    }

    // Strict credential isolation: never leak vendor keys to custom baseUrl endpoints
    const keyEnv = this.profile.apiKeyEnv ?? (!this.profile.baseUrl ? builtin?.[1] : undefined);
    const apiKey = keyEnv ? process.env[keyEnv] : undefined;
    if (keyEnv && !apiKey && this.profile.billing !== "local") {
      throw new Error(`Missing API credential in env var ${keyEnv} for profile ${this.profile.id}`);
    }

    const apiName = this.profile.api ?? builtinModel?.api ?? "openai-completions";
    const apiFactory = API_FACTORIES.get(apiName);
    if (!apiFactory) {
      throw new Error(`Unsupported pi wire API: ${apiName}`);
    }

    const contextWindow = builtinModel?.contextWindow ?? 128_000;
    const model = {
      name: modelId,
      input: ["text"] as ("text" | "image")[],
      reasoning: builtinModel?.reasoning ?? false,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      ...builtinModel,
      id: modelId,
      provider: this.profile.provider,
      api: apiName,
      baseUrl,
      contextWindow,
      maxTokens: builtinModel?.maxTokens ?? 8192,
    };

    const models = createModels();
    models.setProvider(
      createProvider({
        id: this.profile.provider,
        baseUrl,
        api: apiFactory(),
        models: [model as never],
        auth: {
          apiKey: {
            name: keyEnv ?? "local",
            resolve: async () => ({ auth: { apiKey: apiKey ?? "local" } }),
          },
        },
      }),
    );

    const resolvedModel = models.getModel(this.profile.provider, modelId);
    if (!resolvedModel) {
      throw new Error(`Failed to resolve model ${this.profile.provider}/${modelId}`);
    }

    const messages: Message[] = [{ role: "user", content: req.initialPrompt, timestamp: Date.now() }];
    const steeringQueue: string[] = [];
    let cancelled = false;
    let lastText = "";
    let lastUsage: RunnerUsage | undefined;

    req.onHandle?.({
      steer: async (msg: string) => {
        steeringQueue.push(msg);
        return true;
      },
      cancel: async () => {
        cancelled = true;
      },
    });

    const piTools = req.tools.map((t) => ({
      name: t.name,
      description: t.description,
      parameters: t.inputSchema as never,
    }));

    for (let turn = 0; turn < req.maxTurns; turn++) {
      if (cancelled || req.signal.aborted) {
        return { text: lastText, stopReason: "cancelled", usage: lastUsage };
      }

      if (steeringQueue.length > 0) {
        const injected = steeringQueue.splice(0, steeringQueue.length).join("\n\n");
        messages.push({ role: "user", content: `[Steering Update]\n${injected}`, timestamp: Date.now() });
      }

      const context: Context = {
        systemPrompt: req.systemPrompt,
        messages,
        tools: piTools,
      };

      const quotaTimeoutMs = Math.max(0, req.quotaRetryTimeoutMs ?? 45_000);
      const retryStartMs = Date.now();
      let retryCount = 0;
      let finalMsg: Awaited<ReturnType<ReturnType<typeof models.streamSimple>["result"]>> | null = null;

      while (true) {
        try {
          const s = models.streamSimple(resolvedModel, context, {
            apiKey: apiKey ?? "local",
            signal: req.signal,
            reasoning: (req.effort as never) ?? undefined,
          });

          for await (const ev of s) {
            if (ev.type === "text_delta") {
              req.callbacks.onTextDelta?.(ev.delta);
            } else if (ev.type === "thinking_delta") {
              req.callbacks.onThoughtDelta?.(ev.delta);
            }
          }

          finalMsg = await s.result();
          if (finalMsg.stopReason === "error") {
            throw new Error(finalMsg.errorMessage || `Pi provider rejected the request for ${modelId}`);
          }
          if (finalMsg.stopReason === "aborted") {
            return { text: lastText, stopReason: "cancelled", usage: lastUsage };
          }
          if (retryCount > 0) {
            req.callbacks.onQuotaRetryState?.({
              state: "recovered",
              retryCount,
              timeoutMs: quotaTimeoutMs,
            });
          }
          break;
        } catch (err) {
          if (cancelled || req.signal.aborted) {
            return { text: lastText, stopReason: "cancelled", usage: lastUsage };
          }
          const msg = err instanceof Error ? err.message : String(err);
          const detected = detectQuotaErrorText(msg);
          if (!detected.isQuota) throw err;

          retryCount += 1;
          const elapsed = Date.now() - retryStartMs;
          const remaining = quotaTimeoutMs - elapsed;
          // For transient 429s (e.g. subscription pool rotation), retry within quotaTimeoutMs
          if (!detected.isHardExhaustion && remaining > 500) {
            if (retryCount === 1) {
              req.callbacks.onQuotaRetryState?.({
                state: "retrying",
                retryCount,
                timeoutMs: quotaTimeoutMs,
                reason: detected.summary,
                resetAt: detected.resetAtIso ?? null,
              });
            }
            const delayMs = Math.min(remaining, Math.min(8_000, 1_000 * 2 ** (retryCount - 1)));
            await new Promise((r) => setTimeout(r, delayMs));
            continue;
          }

          req.callbacks.onPassiveQuota?.({
            profileId: this.profile.id,
            model: modelId,
            raw: {
              sessionFailure: detected.isHardExhaustion ? "quota_exhausted" : "rate_limited",
              ...(detected.resetAtIso ? { resetAt: detected.resetAtIso } : {}),
            },
            observedAt: Date.now(),
          });
          return {
            text: lastText,
            stopReason: "quota_paused",
            usage: lastUsage,
            quotaPause: {
              reason: detected.summary,
              resetAt: detected.resetAtIso ?? null,
              retryCount,
              watchdogTimedOut: !detected.isHardExhaustion && remaining <= 500,
              isHardExhaustion: detected.isHardExhaustion,
            },
          };
        }
      }

      if (!finalMsg) {
        return { text: lastText, stopReason: "cancelled", usage: lastUsage };
      }
      messages.push(finalMsg);

      const used = (finalMsg.usage?.input ?? 0) + (finalMsg.usage?.cacheRead ?? 0) + (finalMsg.usage?.output ?? 0);
      lastUsage = {
        used,
        size: contextWindow,
        inputTokens: finalMsg.usage?.input,
        outputTokens: finalMsg.usage?.output,
        cachedReadTokens: finalMsg.usage?.cacheRead,
        cachedWriteTokens: finalMsg.usage?.cacheWrite,
        // Only a provider-billed total (OpenRouter) is a reported cost; catalog estimates are not.
        cost: finalMsg.usage?.cost?.totalFromProvider
          ? {
              amount: finalMsg.usage.cost.total,
              currency: "USD",
              basis: "delta",
              model: finalMsg.responseModel ?? finalMsg.model,
              chargeId: finalMsg.responseId ?? `turn-${turn}`,
            }
          : undefined,
      };
      req.callbacks.onUsage?.(lastUsage);

      const textParts = finalMsg.content
        .filter((c: { type: string }): c is { type: "text"; text: string } => c.type === "text")
        .map((c: { text: string }) => c.text);
      if (textParts.length > 0) {
        lastText = textParts.join("\n");
      }

      // Check clean rollover threshold before starting another turn
      if (req.rolloverThreshold && contextWindow > 0 && used / contextWindow >= req.rolloverThreshold) {
        return { text: lastText, stopReason: "rollover_requested", usage: lastUsage };
      }

      const toolCalls = finalMsg.content.filter((c) => c.type === "toolCall");

      if (toolCalls.length === 0) {
        return { text: lastText, stopReason: "completed", usage: lastUsage };
      }

      // Execute harness tool calls directly in-process
      for (const call of toolCalls) {
        req.callbacks.onToolEvent?.({ id: call.id, name: call.name, status: "in_progress", args: toolEventPreview(call.arguments) });
        try {
          if (!piTools.some((tool) => tool.name === call.name)) {
            throw new Error(`Tool ${call.name} is unavailable to this agent.`);
          }
          const out = await req.executeTool(call.name, call.arguments ?? {});
          req.callbacks.onToolEvent?.({ id: call.id, name: call.name, status: "completed", result: toolEventPreview(out) });
          messages.push({
            role: "toolResult",
            toolCallId: call.id,
            toolName: call.name,
            content: [{ type: "text", text: typeof out === "string" ? out : JSON.stringify(out) }],
            isError: false,
            timestamp: Date.now(),
          });
        } catch (err) {
          req.callbacks.onToolEvent?.({ id: call.id, name: call.name, status: "failed", error: toolEventPreview(err instanceof Error ? err.message : String(err)) });
          messages.push({
            role: "toolResult",
            toolCallId: call.id,
            toolName: call.name,
            content: [{ type: "text", text: err instanceof Error ? err.message : String(err) }],
            isError: true,
            timestamp: Date.now(),
          });
        }
      }
    }

    return { text: lastText, stopReason: "max_turns", usage: lastUsage };
  }
}
