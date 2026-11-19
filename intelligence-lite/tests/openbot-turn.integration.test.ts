import { afterAll, beforeAll, expect, test } from "bun:test";
import type { RunAgentInput } from "@ag-ui/client";
import { LLMock } from "@copilotkit/aimock";
import { createActorAgentResolver } from "../../server/src/agents/agent-resolver";
import { createAgentFetch } from "../../server/src/agents/endpoint";
import { loadConfig } from "../../server/src/config";
import {
  mountCopilotRuntime,
  registeredAgentFromRow,
} from "../../server/src/copilot";
import { reportIntelligenceEntitlement } from "../../server/src/intelligence-entitlement-check";
import { testEnvironment } from "../../server/tests/support/environment";
import { boot, frameQueue } from "./harness";

/**
 * Boots the OpenBot copilot runtime — the same `mountCopilotRuntime` the server
 * process mounts — against intelligence-lite and a fake OpenAI. One turn has to
 * create the thread, stream events to a client socket, replay them on a new
 * connection, and accept `stop_run`.
 */
let lite: Awaited<ReturnType<typeof boot>>;
let model: LLMock;
let server: ReturnType<typeof Bun.serve>;
const previousBase = process.env.OPENAI_BASE_URL;
const previousTelemetry = process.env.COPILOTKIT_TELEMETRY_DISABLED;

type Frame = [string | null, string | null, string, string, unknown];

const queues = new WeakMap<WebSocket, ReturnType<typeof frameQueue>>();

function framesOf(ws: WebSocket): ReturnType<typeof frameQueue> {
  let queue = queues.get(ws);
  if (!queue) {
    queue = frameQueue(ws);
    queues.set(ws, queue);
  }
  return queue;
}

beforeAll(async () => {
  process.env.COPILOTKIT_TELEMETRY_DISABLED = "true";
  lite = await boot("turn");
  model = new LLMock();
  const modelUrl = await model.start();
  process.env.OPENAI_BASE_URL = `${modelUrl}/v1`;
  model.onMessage(
    /.*/,
    { content: `Hello from lite. ${"word ".repeat(80)}` },
    { latency: 40, chunkSize: 12 },
  );

  const builtIn = registeredAgentFromRow({
    id: "general-assistant",
    name: "General Assistant",
    type: "built_in",
    configuration: { systemPrompt: "Be helpful." },
    title: "Everyday Work",
    roleDescription: "Help with everyday work.",
  });
  if (!builtIn) throw new Error("the built-in Bot did not parse");
  const runtime = mountCopilotRuntime(
    loadConfig(
      testEnvironment({
        INTELLIGENCE_API_URL: lite.url,
        INTELLIGENCE_GATEWAY_WS_URL: lite.wsUrl,
        INTELLIGENCE_API_KEY: lite.apiKey,
        CPK_INTELLIGENCE_API_KEY: "",
      }),
    ),
    createActorAgentResolver({
      model: { provider: "openai", defaultModel: "gpt-test" },
      loadAgents: async () => [builtIn],
      resolveModelApiKey: async () => "model-key",
      agentFetch: createAgentFetch({ allowPrivateHosts: true }),
    }),
    async () => ({ id: "person", name: "Person" }),
    async () => ({ id: "person", role: "user" }),
  );
  server = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    fetch: (request) => runtime.handler.fetch(request),
  });
});

afterAll(async () => {
  server?.stop(true);
  await model?.stop();
  await lite?.close();
  if (previousBase === undefined) delete process.env.OPENAI_BASE_URL;
  else process.env.OPENAI_BASE_URL = previousBase;
  if (previousTelemetry === undefined)
    delete process.env.COPILOTKIT_TELEMETRY_DISABLED;
  else process.env.COPILOTKIT_TELEMETRY_DISABLED = previousTelemetry;
});

function connect(url: string): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    ws.addEventListener("open", () => resolve(ws), { once: true });
    ws.addEventListener(
      "error",
      () => reject(new Error(`socket failed for ${url}`)),
      { once: true },
    );
  });
}

async function framesUntil(
  ws: WebSocket,
  done: (frame: Frame) => boolean,
): Promise<Frame[]> {
  const frames: Frame[] = [];
  const queue = framesOf(ws);
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    frames.push(await queue.next(20_000));
    const frame = frames.at(-1);
    if (frame && done(frame)) return frames;
  }
  throw new Error("the client socket did not finish the turn");
}

test("one chat turn streams, replays, and stops", async () => {
  const warnings: string[] = [];
  const { createIntelligenceClient } = await import(
    "../../server/src/intelligence-client"
  );
  const intelligence = createIntelligenceClient({
    apiUrl: lite.url,
    gatewayWsUrl: lite.wsUrl,
    apiKey: lite.apiKey,
  });
  await reportIntelligenceEntitlement({
    entitlements: () => intelligence.getRuntimeEntitlements(),
    log: (message) => warnings.push(message),
    note: () => {},
    windowMs: 1_000,
    sleep: async () => {},
  });
  expect(warnings).toEqual([]);

  const threadId = "thread-turn";
  const runId = "run-turn";
  const response = await fetch(
    `http://127.0.0.1:${server.port}/api/copilotkit/agent/general-assistant/run`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        threadId,
        runId,
        state: {},
        messages: [{ id: "ask-1", role: "user", content: "Say hello." }],
        tools: [],
        context: [],
        forwardedProps: {},
      } satisfies RunAgentInput),
    },
  );
  expect(response.status).toBe(200);
  const started = (await response.json()) as {
    threadId: string;
    runId: string;
    joinToken: string;
    realtime: { topic: string };
  };
  expect(started).toMatchObject({
    threadId,
    runId,
    realtime: { topic: `thread:${threadId}` },
  });

  const live = await connect(
    `${lite.wsUrl}/client/websocket?vsn=2.0.0&join_token=${started.joinToken}`,
  );
  const collected = framesUntil(
    live,
    (frame) =>
      frame[3] === "ag_ui_event" &&
      typeof frame[4] === "object" &&
      frame[4] !== null &&
      (frame[4] as { type?: string }).type === "TEXT_MESSAGE_CONTENT",
  );
  live.send(JSON.stringify(["1", "1", `thread:${threadId}`, "phx_join", {}]));
  const untilText = await collected;
  expect(
    untilText.some(
      (frame) =>
        frame[3] === "ag_ui_event" &&
        (frame[4] as { delta?: string }).delta?.includes("Hello"),
    ),
  ).toBe(true);

  const stopAck = framesUntil(
    live,
    (frame) => frame[3] === "phx_reply" && frame[1] === "2",
  );
  live.send(
    JSON.stringify([
      "1",
      "2",
      `thread:${threadId}`,
      "stop_run",
      { run_id: runId },
    ]),
  );
  const acked = await stopAck;
  expect(acked.at(-1)?.[4]).toMatchObject({ status: "ok" });
  const terminal = await framesUntil(live, (frame) => {
    const type =
      frame[3] === "ag_ui_event" && frame[4] && typeof frame[4] === "object"
        ? (frame[4] as { type?: string }).type
        : undefined;
    return type === "RUN_FINISHED" || type === "RUN_ERROR";
  });
  expect(terminal.length).toBeGreaterThan(0);
  live.close();

  const replayToken = await fetch(
    `${lite.url}/api/threads/${threadId}/connect`,
    {
      method: "POST",
      headers: {
        authorization: `Bearer ${lite.apiKey}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ userId: "person", agentId: "general-assistant" }),
    },
  );
  expect(replayToken.status).toBe(200);
  const replayBody = (await replayToken.json()) as { joinToken: string };
  const replay = await connect(
    `${lite.wsUrl}/client/websocket?vsn=2.0.0&join_token=${replayBody.joinToken}`,
  );
  const replayed = framesUntil(replay, (frame) => {
    const type =
      frame[3] === "ag_ui_event" && frame[4] && typeof frame[4] === "object"
        ? (frame[4] as { type?: string }).type
        : undefined;
    return type === "RUN_FINISHED" || type === "RUN_ERROR";
  });
  replay.send(JSON.stringify(["9", "9", `thread:${threadId}`, "phx_join", {}]));
  const history = await replayed;
  expect(
    history.some(
      (frame) =>
        frame[3] === "ag_ui_event" &&
        (frame[4] as { delta?: string }).delta?.includes("Hello"),
    ),
  ).toBe(true);
  replay.close();

  const messages = await fetch(
    `${lite.url}/api/threads/${threadId}/messages?userId=person`,
    { headers: { authorization: `Bearer ${lite.apiKey}` } },
  );
  expect(messages.status).toBe(200);
  const body = (await messages.json()) as {
    messages: { id: string; content: unknown }[];
  };
  expect(body.messages.map((message) => message.id)).toContain("ask-1");
  expect(
    body.messages.some(
      (message) =>
        typeof message.content === "string" &&
        message.content.includes("Hello"),
    ),
  ).toBe(true);
}, 40_000);
