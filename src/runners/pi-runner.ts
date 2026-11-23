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
import { MAX_RUNNER_STEER_CONTINUATIONS, turnReminderText, validateRunPolicy } from "./types.js";
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
    if (this.profile.enabled === false) return [];
    const builtin = BUILTIN_PROVIDERS.get(this.profile.provider);
    if (this.profile.billing !== "local") {
      const keyEnv = this.profile.apiKeyEnv ?? (!this.profile.baseUrl ? builtin?.[1] : undefined);
      if (keyEnv && !process.env[keyEnv]?.trim()) {
        return [];
      }
    }
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

    const contextWindow = builtinModel?.contextWindow ?? req.contextWindow ?? 128_000;
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
    const runAbort = new AbortController();
    const onReqAbort = () => runAbort.abort(req.signal.reason);
    if (req.signal.aborted) {
      runAbort.abort(req.signal.reason);
    } else {
      req.signal.addEventListener("abort", onReqAbort, { once: true });
    }
    let cancelled = false;
    let cancelReason: string | undefined;
    let rolloverWarningSent = false;
    let lastText = "";
    let lastUsage: RunnerUsage | undefined;
    const idleTimeoutMs = Math.max(0, req.idleTimeoutMs ?? 600_000);
    const effectiveEffort =
      req.effort && req.effort.toLowerCase() !== "default" ? req.effort : undefined;

    const cancelledStopReason = (): RunAgentResult["stopReason"] =>
      cancelReason === "clean_rollover" ? "rollover_requested" : "cancelled";

    req.onHandle?.({
      steer: async (msg: string) => {
        steeringQueue.push(msg);
        return true;
      },
      cancel: async (reason?: string) => {
        cancelled = true;
        cancelReason = reason;
        if (!runAbort.signal.aborted) {
          runAbort.abort(reason ? new Error(reason) : undefined);
        }
      },
    });

    const piTools = req.tools.map((t) => ({
      name: t.name,
      description: t.description,
      parameters: t.inputSchema as never,
    }));

    try {
      // maxTurns is an advisory reminder threshold; the loop ends only when the agent stops calling tools.
      let turnReminderSent = false;
      // One missing-return wrap-up prompt when the agent stops without settling its task, then at most 3 more turns.
      let wrapUpAttempted = false;
      let wrapUpTurnsLeft = 0;
      let preWrapUpText = "";
      // Distinct substantive text of each wrap-up turn, in order, so intermediate findings survive a closing "ok".
      const wrapUpTexts: string[] = [];
      // Loop guard for turns opened only to deliver queued steering.
      let steerContinuations = 0;
      for (let turn = 0; ; turn++) {
        req.callbacks.onTurn?.(turn + 1);
        if (cancelled || runAbort.signal.aborted) {
          return { text: lastText, stopReason: cancelledStopReason(), usage: lastUsage };
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
          const attemptAbort = new AbortController();
          const onRunAbort = () => attemptAbort.abort(runAbort.signal.reason);
          if (runAbort.signal.aborted) {
            attemptAbort.abort(runAbort.signal.reason);
          } else {
            runAbort.signal.addEventListener("abort", onRunAbort, { once: true });
          }
          let idleTimedOut = false;
          let idleTimer: ReturnType<typeof setTimeout> | null = null;
          const resetIdleTimer = () => {
            if (idleTimer) {
              clearTimeout(idleTimer);
              idleTimer = null;
            }
            if (idleTimeoutMs > 0) {
              idleTimer = setTimeout(() => {
                idleTimedOut = true;
                attemptAbort.abort(new Error(`Pi stream timed out after ${idleTimeoutMs}ms of inactivity`));
              }, idleTimeoutMs);
              idleTimer.unref?.();
            }
          };

          try {
            resetIdleTimer();
            const s = models.streamSimple(resolvedModel, context, {
              apiKey: apiKey ?? "local",
              signal: attemptAbort.signal,
              reasoning: (effectiveEffort as never) ?? undefined,
            });

            for await (const ev of s) {
              resetIdleTimer();
              if (ev.type === "text_delta") {
                req.callbacks.onTextDelta?.(ev.delta);
              } else if (ev.type === "thinking_delta") {
                req.callbacks.onThoughtDelta?.(ev.delta);
              }
            }

            finalMsg = await s.result();
            if (idleTimedOut) {
              throw new Error(`Pi runner (${this.profile.id}) timed out after ${idleTimeoutMs}ms of inactivity`);
            }
            if (finalMsg.stopReason === "error") {
              throw new Error(finalMsg.errorMessage || `Pi provider rejected the request for ${modelId}`);
            }
            if (finalMsg.stopReason === "aborted") {
              return { text: lastText, stopReason: cancelledStopReason(), usage: lastUsage };
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
            if (idleTimedOut) {
              throw new Error(`Pi runner (${this.profile.id}) timed out after ${idleTimeoutMs}ms of inactivity`);
            }
            if (cancelled || runAbort.signal.aborted) {
              return { text: lastText, stopReason: cancelledStopReason(), usage: lastUsage };
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
              await new Promise<void>((resolve) => {
                if (runAbort.signal.aborted) {
                  resolve();
                  return;
                }
                const onSleepAbort = () => {
                  clearTimeout(t);
                  resolve();
                };
                const t = setTimeout(() => {
                  runAbort.signal.removeEventListener("abort", onSleepAbort);
                  resolve();
                }, delayMs);
                t.unref?.();
                runAbort.signal.addEventListener("abort", onSleepAbort, { once: true });
              });
              if (cancelled || runAbort.signal.aborted) {
                return { text: lastText, stopReason: cancelledStopReason(), usage: lastUsage };
              }
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
          } finally {
            if (idleTimer) {
              clearTimeout(idleTimer);
              idleTimer = null;
            }
            runAbort.signal.removeEventListener("abort", onRunAbort);
          }
        }

        if (!finalMsg) {
          return { text: lastText, stopReason: cancelledStopReason(), usage: lastUsage };
        }
        messages.push(finalMsg);

        const used =
          (finalMsg.usage?.input ?? 0) +
          (finalMsg.usage?.output ?? 0) +
          (finalMsg.usage?.cacheRead ?? 0) +
          (finalMsg.usage?.cacheWrite ?? 0);
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
          const turnText = textParts.join("\n");
          // Wrap-up turns add to the answer that triggered the wrap-up; an acknowledgement never replaces it.
          if (!wrapUpAttempted) lastText = turnText;
          else {
            const t = turnText.trim();
            if (t && t !== preWrapUpText.trim() && !wrapUpTexts.some((w) => w.trim() === t)) wrapUpTexts.push(turnText);
            lastText = [preWrapUpText, ...wrapUpTexts].filter((x) => x.trim()).join("\n\n");
          }
        }

        const toolCalls = finalMsg.content.filter((c) => c.type === "toolCall");

        // Execute harness tool calls directly in-process BEFORE checking rolloverThreshold so
        // terminal tools (return_task, request_rollover, submit_for_merge) on the threshold turn run cleanly.
        for (const call of toolCalls) {
          req.callbacks.onToolEvent?.({
            id: call.id,
            name: call.name,
            status: "in_progress",
            args: toolEventPreview(call.arguments ?? {}),
          });
          try {
            if (!piTools.some((tool) => tool.name === call.name)) {
              throw new Error(`Tool ${call.name} is unavailable to this agent.`);
            }
            const out = await req.executeTool(call.name, call.arguments ?? {});
            req.callbacks.onToolEvent?.({
              id: call.id,
              name: call.name,
              status: "completed",
              result: toolEventPreview(out),
            });
            messages.push({
              role: "toolResult",
              toolCallId: call.id,
              toolName: call.name,
              content: [{ type: "text", text: typeof out === "string" ? out : JSON.stringify(out) }],
              isError: false,
              timestamp: Date.now(),
            });
          } catch (err) {
            req.callbacks.onToolEvent?.({
              id: call.id,
              name: call.name,
              status: "failed",
              error: toolEventPreview(err instanceof Error ? err.message : String(err)),
            });
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

        if (cancelled || runAbort.signal.aborted) {
          return { text: lastText, stopReason: cancelledStopReason(), usage: lastUsage };
        }

        const ratio = contextWindow > 0 ? used / contextWindow : 0;

        // Check clean rollover threshold before starting another turn
        if (req.rolloverThreshold && contextWindow > 0 && ratio >= req.rolloverThreshold) {
          return { text: lastText, stopReason: "rollover_requested", usage: lastUsage };
        }

        if (toolCalls.length === 0) {
          if (steerContinuations < MAX_RUNNER_STEER_CONTINUATIONS) {
            const late = req.drainPendingSteers?.() ?? [];
            if (late.length > 0) steeringQueue.push(...late);
            if (steeringQueue.length > 0) {
              // Steering that arrived during the turn opens the next one instead of being dropped.
              steerContinuations += 1;
              continue;
            }
          }
          if (!wrapUpAttempted && req.missingReturnWrapUpPrompt && !req.isTaskSettled?.()) {
            wrapUpAttempted = true;
            wrapUpTurnsLeft = 3;
            preWrapUpText = lastText;
            messages.push({ role: "user", content: req.missingReturnWrapUpPrompt, timestamp: Date.now() });
            continue;
          }
          return { text: lastText, stopReason: "completed", usage: lastUsage, ...(wrapUpAttempted ? { wrapUpAttempted: true } : {}) };
        }
        if (wrapUpAttempted && (req.isTaskSettled?.() || --wrapUpTurnsLeft <= 0)) {
          return { text: lastText, stopReason: "completed", usage: lastUsage, wrapUpAttempted: true };
        }

        // Work continues (tool calls pending): send ONE advisory progress reminder at the threshold.
        if (!turnReminderSent && req.maxTurns > 0 && turn + 1 >= req.maxTurns) {
          turnReminderSent = true;
          steeringQueue.push(turnReminderText(req.tier, req.tools.some((t) => t.name === "submit_review_verdict")));
        }

        // Inject soft rollover warning once when approaching rolloverThreshold
        const softWarnThreshold = req.rolloverThreshold ? Math.max(0.5, req.rolloverThreshold - 0.12) : undefined;
        if (
          !rolloverWarningSent &&
          softWarnThreshold &&
          req.rolloverThreshold &&
          ratio >= softWarnThreshold &&
          ratio < req.rolloverThreshold
        ) {
          rolloverWarningSent = true;
          const pct = Math.round(ratio * 100);
          const limitPct = Math.round(req.rolloverThreshold * 100);
          steeringQueue.push(
            `Context usage is at ${pct}% (hard rollover at ${limitPct}%). Finish your current atomic step and call \`request_rollover\` with a thorough anchored handoff (goal_invariants, architectural_decisions, active_risks, next_actions) so no context is lost.`,
          );
        }
      }
    } finally {
      req.signal.removeEventListener("abort", onReqAbort);
    }
  }
}
