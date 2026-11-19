import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";
import { type RawData, type WebSocket, WebSocketServer } from "ws";
import type { JoinToken, Store } from "./store";

/**
 * Phoenix Channels serializer V2: `[join_ref, ref, topic, event, payload]`.
 *
 * The MIT runner connects to `/runner/websocket`, authenticates with the API
 * key in the Phoenix auth subprotocol, and joins `ingestion:{runId}`. The join
 * is accepted only while that run holds the thread lock (`active_lock_mismatch`
 * otherwise). The join reply advertises `runner_event_batch_v1`, so the runner
 * pushes `events` batches. Each new event is stored and then fan-out as
 * `ag_ui_event` on `thread:{threadId}`.
 *
 * A browser connects to `/client/websocket?join_token=` (single use) and joins
 * `thread:{threadId}`. After the join reply, stored events are replayed oldest
 * first. `stop_run` on that topic is acknowledged and relayed to the runner as
 * `ag_ui_event` `{type:"CUSTOM", name:"stop"}`, which is the frame the runner
 * aborts on.
 */
const RUNNER_BATCH = "runner_event_batch_v1";
/** What a thread subscriber receives. The browser fixture and the runner's `connect` both use this name. */
export const AG_UI_EVENT = "ag_ui_event";
/**
 * What the runner listens for on its ingestion channel (`AG_UI_CHANNEL_EVENT`
 * in `@copilotkit/shared`). A `stop_run` is relayed as this event, not as
 * `ag_ui_event`: the constant is `ag-ui`.
 */
export const RUNNER_CHANNEL_EVENT = "ag-ui";

type Frame = [string | null, string | null, string, string, unknown];
type SocketKind = "runner" | "client" | "channels";

type Channel = {
  joinRef: string | null;
  threadId: string | null;
  runId: string | null;
};

type Conn = {
  ws: WebSocket;
  kind: SocketKind;
  token: JoinToken | null;
  channels: Map<string, Channel>;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asRef(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function send(ws: WebSocket, frame: Frame): void {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(frame));
}

function reply(
  ws: WebSocket,
  joinRef: string | null,
  ref: string | null,
  topic: string,
  status: "ok" | "error",
  response: unknown,
): void {
  send(ws, [joinRef, ref, topic, "phx_reply", { status, response }]);
}

export type PhoenixSockets = {
  handleUpgrade(
    request: IncomingMessage,
    socket: Duplex,
    head: Buffer,
    kind: SocketKind,
    token: JoinToken | null,
  ): void;
  close(): void;
};

export function createPhoenix(store: Store): PhoenixSockets {
  const wss = new WebSocketServer({ noServer: true });
  const conns = new Map<WebSocket, Conn>();
  const topics = new Map<string, Set<WebSocket>>();
  const pending = new WeakMap<
    IncomingMessage,
    { kind: SocketKind; token: JoinToken | null }
  >();

  const subscribe = (conn: Conn, topic: string) => {
    let set = topics.get(topic);
    if (!set) {
      set = new Set();
      topics.set(topic, set);
    }
    set.add(conn.ws);
  };

  const unsubscribe = (conn: Conn, topic: string) => {
    conn.channels.delete(topic);
    const set = topics.get(topic);
    set?.delete(conn.ws);
    if (set && set.size === 0) topics.delete(topic);
  };

  const broadcast = (topic: string, event: string, payload: unknown) => {
    for (const ws of topics.get(topic) ?? []) {
      const conn = conns.get(ws);
      if (!conn) continue;
      send(ws, [
        conn.channels.get(topic)?.joinRef ?? null,
        null,
        topic,
        event,
        payload,
      ]);
    }
  };

  const onJoin = async (
    conn: Conn,
    joinRef: unknown,
    ref: unknown,
    topic: string,
    payload: unknown,
  ) => {
    const join = asRef(joinRef);
    const messageRef = asRef(ref);
    if (conn.kind === "runner") {
      if (!topic.startsWith("ingestion:")) {
        reply(conn.ws, join, messageRef, topic, "error", {
          reason: "unmatched topic",
        });
        return;
      }
      const runId = topic.slice("ingestion:".length);
      const body = isRecord(payload) ? payload : {};
      const threadId = typeof body.thread_id === "string" ? body.thread_id : "";
      const claimed = typeof body.run_id === "string" ? body.run_id : runId;
      if (
        !threadId ||
        claimed !== runId ||
        !(await store.hasActiveLock(threadId, runId))
      ) {
        reply(conn.ws, join, messageRef, topic, "error", {
          reason: "active_lock_mismatch",
          retryable: false,
        });
        return;
      }
      conn.channels.set(topic, { joinRef: join, threadId, runId });
      subscribe(conn, topic);
      reply(conn.ws, join, messageRef, topic, "ok", {
        capabilities: [RUNNER_BATCH],
      });
      return;
    }

    if (!(await clientMayJoin(store, conn, topic))) {
      reply(conn.ws, join, messageRef, topic, "error", {
        reason: "unauthorized",
      });
      return;
    }
    const threadId = topic.startsWith("thread:")
      ? topic.slice("thread:".length)
      : null;
    conn.channels.set(topic, { joinRef: join, threadId, runId: null });
    subscribe(conn, topic);
    reply(conn.ws, join, messageRef, topic, "ok", {});
    if (threadId) {
      const events = await store.eventsForThread(threadId);
      for (const event of events) {
        send(conn.ws, [join, null, topic, AG_UI_EVENT, event]);
      }
    }
  };

  const onRunnerEvents = async (
    conn: Conn,
    joinRef: unknown,
    ref: unknown,
    topic: string,
    event: string,
    payload: unknown,
  ) => {
    const channel = conn.channels.get(topic);
    const events =
      event === "events" && isRecord(payload) && Array.isArray(payload.events)
        ? payload.events
        : event === "event"
          ? [payload]
          : null;
    if (!channel?.threadId || !channel.runId || !events) {
      reply(conn.ws, asRef(joinRef), asRef(ref), topic, "error", {
        reason: "invalid_batch",
        retryable: false,
      });
      return;
    }
    let stored: Record<string, unknown>[];
    try {
      const appended = await store.appendEvents(
        channel.threadId,
        channel.runId,
        events,
      );
      if (!appended.ok) {
        reply(conn.ws, asRef(joinRef), asRef(ref), topic, "error", {
          reason: "active_lock_mismatch",
          retryable: false,
        });
        return;
      }
      stored = appended.stored;
    } catch {
      reply(conn.ws, asRef(joinRef), asRef(ref), topic, "error", {
        reason: "storage_unavailable",
        retryable: true,
      });
      return;
    }
    reply(conn.ws, asRef(joinRef), asRef(ref), topic, "ok", {});
    for (const item of stored) {
      broadcast(`thread:${channel.threadId}`, AG_UI_EVENT, item);
    }
  };

  const onStop = (
    conn: Conn,
    joinRef: unknown,
    ref: unknown,
    topic: string,
    payload: unknown,
  ) => {
    const body = isRecord(payload) ? payload : {};
    const runId =
      typeof body.run_id === "string"
        ? body.run_id
        : typeof body.runId === "string"
          ? body.runId
          : "";
    reply(conn.ws, asRef(joinRef), asRef(ref), topic, "ok", {});
    if (!runId) return;
    broadcast(`ingestion:${runId}`, RUNNER_CHANNEL_EVENT, {
      type: "CUSTOM",
      name: "stop",
      threadId: topic.slice("thread:".length),
      runId,
    });
  };

  const onMessage = async (conn: Conn, data: RawData) => {
    let frame: unknown;
    try {
      const text = Buffer.isBuffer(data)
        ? data.toString("utf8")
        : Array.isArray(data)
          ? Buffer.concat(data).toString("utf8")
          : Buffer.from(data).toString("utf8");
      frame = JSON.parse(text);
    } catch {
      return;
    }
    if (!Array.isArray(frame) || frame.length !== 5) return;
    const [joinRef, ref, topic, event, payload] = frame as Frame;
    if (typeof topic !== "string" || typeof event !== "string") return;

    if (topic === "phoenix" && event === "heartbeat") {
      reply(conn.ws, null, asRef(ref), "phoenix", "ok", {});
      return;
    }
    if (event === "phx_join") {
      await onJoin(conn, joinRef, ref, topic, payload);
      return;
    }
    if (!conn.channels.has(topic)) {
      reply(conn.ws, asRef(joinRef), asRef(ref), topic, "error", {
        reason: "unmatched topic",
      });
      return;
    }
    if (event === "phx_leave") {
      reply(conn.ws, asRef(joinRef), asRef(ref), topic, "ok", {});
      send(conn.ws, [asRef(joinRef), null, topic, "phx_close", {}]);
      unsubscribe(conn, topic);
      return;
    }
    if (
      topic.startsWith("ingestion:") &&
      (event === "events" || event === "event")
    ) {
      await onRunnerEvents(conn, joinRef, ref, topic, event, payload);
      return;
    }
    if (topic.startsWith("thread:") && event === "stop_run") {
      onStop(conn, joinRef, ref, topic, payload);
      return;
    }
    reply(conn.ws, asRef(joinRef), asRef(ref), topic, "error", {
      reason: "unsupported_event",
    });
  };

  wss.on("connection", (ws: WebSocket, request: IncomingMessage) => {
    const context = pending.get(request);
    pending.delete(request);
    if (!context) {
      ws.close(1008, "unauthorized");
      return;
    }
    const conn: Conn = {
      ws,
      kind: context.kind,
      token: context.token,
      channels: new Map(),
    };
    conns.set(ws, conn);
    ws.on("message", (data) => {
      void onMessage(conn, data).catch(() => {
        try {
          ws.close(1011, "internal");
        } catch {
          // Already closing.
        }
      });
    });
    ws.on("close", () => {
      for (const topic of [...conn.channels.keys()]) unsubscribe(conn, topic);
      conns.delete(ws);
    });
  });

  return {
    handleUpgrade(request, socket, head, kind, token) {
      pending.set(request, { kind, token });
      wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit("connection", ws, request);
      });
    },
    close() {
      for (const ws of conns.keys()) {
        try {
          ws.close(1001, "going away");
        } catch {
          // Already gone.
        }
      }
      wss.close();
    },
  };
}

async function clientMayJoin(
  store: Store,
  conn: Conn,
  topic: string,
): Promise<boolean> {
  const token = conn.token;
  if (!token) return false;
  if (topic.startsWith("thread:")) {
    return (
      token.kind === "thread" &&
      token.threadId === topic.slice("thread:".length)
    );
  }
  if (topic.startsWith("user_meta:memories:") && token.kind === "memories") {
    if (!token.userId) return false;
    const code = await store.joinCodeFor(token.userId, "memories");
    return code !== null && topic === `user_meta:memories:${code}`;
  }
  if (token.kind === "threads" && token.userId) {
    const code = await store.joinCodeFor(token.userId, "threads");
    return code !== null && topic.endsWith(code);
  }
  return false;
}
