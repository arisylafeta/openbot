import { afterAll, beforeAll, expect, test } from "bun:test";
import { AG_UI_CHANNEL_EVENT } from "@copilotkit/shared";
import { AG_UI_EVENT, RUNNER_CHANNEL_EVENT } from "../src/phoenix";
import { auth, boot, frameQueue, type PhoenixFrame } from "./harness";

const queues = new WeakMap<WebSocket, ReturnType<typeof frameQueue>>();

function nextFrame(ws: WebSocket): Promise<PhoenixFrame> {
  let queue = queues.get(ws);
  if (!queue) {
    queue = frameQueue(ws);
    queues.set(ws, queue);
  }
  return queue.next();
}

let lite: Awaited<ReturnType<typeof boot>>;

beforeAll(async () => {
  lite = await boot("channels");
});

afterAll(async () => {
  await lite?.close();
});

function connect(url: string, protocols?: string[]): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url, protocols);
    const timer = setTimeout(
      () => reject(new Error(`timed out opening ${url}`)),
      3_000,
    );
    let opened = false;
    ws.addEventListener(
      "open",
      () => {
        opened = true;
        clearTimeout(timer);
        resolve(ws);
      },
      { once: true },
    );
    const fail = () => {
      if (opened) return;
      clearTimeout(timer);
      reject(new Error(`socket failed for ${url}`));
    };
    ws.addEventListener("error", fail, { once: true });
    ws.addEventListener("close", fail, { once: true });
  });
}

function phoenixToken(apiKey: string): string {
  return `base64url.bearer.phx.${Buffer.from(apiKey).toString("base64").replace(/=/g, "")}`;
}

test("the stop event name matches the runtime constant", () => {
  expect(RUNNER_CHANNEL_EVENT).toBe(AG_UI_CHANNEL_EVENT);
  expect(AG_UI_EVENT).toBe("ag_ui_event");
});

test("rejects a runner that does not present the API key", async () => {
  await expect(connect(`${lite.wsUrl}/runner/websocket`)).rejects.toThrow();
});

test("joins ingestion, fans events out, replays them, and relays stop_run", async () => {
  const headers = auth(lite.apiKey);
  const created = await fetch(`${lite.url}/api/threads`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      threadId: "thread-live",
      userId: "person",
      agentId: "general-assistant",
    }),
  });
  expect(created.status).toBe(200);

  const locked = await fetch(`${lite.url}/api/threads/thread-live/lock`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      runId: "run-live",
      userId: "person",
      agentId: "general-assistant",
      ttlSeconds: 30,
    }),
  });
  expect(locked.status).toBe(200);
  const lock = (await locked.json()) as { joinToken: string };
  expect(lock.joinToken).toBeTruthy();

  const runner = await connect(`${lite.wsUrl}/runner/websocket?vsn=2.0.0`, [
    "phoenix",
    phoenixToken(lite.apiKey),
  ]);
  const joined = nextFrame(runner);
  runner.send(
    JSON.stringify([
      "1",
      "1",
      "ingestion:run-live",
      "phx_join",
      { thread_id: "thread-live", run_id: "run-live" },
    ]),
  );
  const joinReply = await joined;
  expect(joinReply[3]).toBe("phx_reply");
  expect(joinReply[4]).toMatchObject({
    status: "ok",
    response: { capabilities: ["runner_event_batch_v1"] },
  });

  const browser = await connect(
    `${lite.wsUrl}/client/websocket?vsn=2.0.0&join_token=${lock.joinToken}`,
  );
  const browserJoin = nextFrame(browser);
  browser.send(
    JSON.stringify(["3", "3", "thread:thread-live", "phx_join", {}]),
  );
  expect((await browserJoin)[4]).toMatchObject({ status: "ok" });

  const pushed = nextFrame(runner);
  const seen = nextFrame(browser);
  const event = {
    type: "TEXT_MESSAGE_CONTENT",
    messageId: "m1",
    delta: "Hello",
    threadId: "thread-live",
    runId: "run-live",
    metadata: { cpki_event_id: "evt-1", cpki_event_seq: 1 },
  };
  runner.send(
    JSON.stringify([
      "1",
      "2",
      "ingestion:run-live",
      "events",
      { events: [event] },
    ]),
  );
  expect((await pushed)[4]).toMatchObject({ status: "ok" });
  const fanout = await seen;
  expect(fanout[2]).toBe("thread:thread-live");
  expect(fanout[3]).toBe("ag_ui_event");
  expect(fanout[4]).toMatchObject({ delta: "Hello" });

  const replayToken = await fetch(
    `${lite.url}/api/threads/thread-live/connect`,
    {
      method: "POST",
      headers,
      body: JSON.stringify({ userId: "person", agentId: "general-assistant" }),
    },
  );
  const replayBody = (await replayToken.json()) as { joinToken: string };
  const replay = await connect(
    `${lite.wsUrl}/client/websocket?vsn=2.0.0&join_token=${replayBody.joinToken}`,
  );
  const replayJoin = nextFrame(replay);
  const replayed = nextFrame(replay);
  replay.send(JSON.stringify(["4", "4", "thread:thread-live", "phx_join", {}]));
  expect((await replayJoin)[3]).toBe("phx_reply");
  expect((await replayed)[4]).toMatchObject({
    type: "TEXT_MESSAGE_CONTENT",
    delta: "Hello",
  });

  const stopped = nextFrame(runner);
  const stopAck = nextFrame(replay);
  replay.send(
    JSON.stringify([
      "4",
      "5",
      "thread:thread-live",
      "stop_run",
      { run_id: "run-live" },
    ]),
  );
  expect((await stopAck)[4]).toMatchObject({ status: "ok" });
  const relayed = await stopped;
  expect(relayed[2]).toBe("ingestion:run-live");
  expect(relayed[3]).toBe(RUNNER_CHANNEL_EVENT);
  expect(relayed[4]).toMatchObject({ type: "CUSTOM", name: "stop" });

  const messages = await fetch(
    `${lite.url}/api/threads/thread-live/messages?userId=person`,
    { headers },
  );
  expect(messages.status).toBe(200);
  expect(await messages.json()).toMatchObject({
    messages: [{ id: "m1", role: "assistant", content: "Hello" }],
  });

  for (const ws of [runner, browser, replay]) ws.close();
});

test("refuses ingestion when the run does not hold the lock", async () => {
  const runner = await connect(`${lite.wsUrl}/runner/websocket?vsn=2.0.0`, [
    "phoenix",
    phoenixToken(lite.apiKey),
  ]);
  const joined = nextFrame(runner);
  runner.send(
    JSON.stringify([
      "1",
      "1",
      "ingestion:missing-run",
      "phx_join",
      { thread_id: "thread-live", run_id: "missing-run" },
    ]),
  );
  expect(await joined).toMatchObject([
    "1",
    "1",
    "ingestion:missing-run",
    "phx_reply",
    {
      status: "error",
      response: { reason: "active_lock_mismatch", retryable: false },
    },
  ]);
  runner.close();
});
