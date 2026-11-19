import { timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";
import type { Config } from "./config";
import { handleHttp, writeResult } from "./http";
import { createPhoenix, type PhoenixSockets } from "./phoenix";
import { type JoinToken, Store } from "./store";

const AUTH_PREFIX = "base64url.bearer.phx.";

export type Started = {
  url: string;
  wsUrl: string;
  port: number;
  apiKey: string;
  close: () => Promise<void>;
};

/**
 * One HTTP server for the REST API and the Phoenix sockets.
 *
 * Phoenix appends `/websocket` to the runner and client addresses the runtime
 * derives from `INTELLIGENCE_GATEWAY_WS_URL`, so the sockets are
 * `/runner/websocket` and `/client/websocket`. The runner authenticates with
 * the API key carried in the Phoenix auth subprotocol. The browser authenticates
 * with the single-use `join_token` query parameter.
 */
export async function startServer(config: Config): Promise<Started> {
  const store = await Store.open(config.databaseUrl, config.schema);
  const phoenix = createPhoenix(store);
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(
        request.url ?? "/",
        `http://${request.headers.host ?? "localhost"}`,
      );
      const result = await handleHttp(store, config.apiKey, request, url);
      writeResult(response, result);
    } catch (error) {
      response.writeHead(500, { "content-type": "application/json" });
      response.end(
        JSON.stringify({
          error: {
            code: "INTERNAL_SERVER_ERROR",
            message: "Unexpected server error.",
            category: "internal",
            retryable: true,
          },
        }),
      );
      console.error(
        JSON.stringify({
          type: "intelligence-lite-error",
          message: error instanceof Error ? error.message : "request failed",
        }),
      );
    }
  });

  server.on("upgrade", (request, socket, head) => {
    void upgrade(store, phoenix, config.apiKey, request, socket, head);
  });

  await new Promise<void>((resolve) => {
    server.listen(config.port, config.host, () => resolve());
  });
  const address = server.address();
  const port =
    typeof address === "object" && address ? address.port : config.port;
  const host = config.host === "0.0.0.0" ? "127.0.0.1" : config.host;

  return {
    url: `http://${host}:${port}`,
    wsUrl: `ws://${host}:${port}`,
    port,
    apiKey: config.apiKey,
    async close() {
      phoenix.close();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
      await store.close();
    },
  };
}

async function upgrade(
  store: Store,
  phoenix: PhoenixSockets,
  apiKey: string,
  request: IncomingMessage,
  socket: Duplex,
  head: Buffer,
): Promise<void> {
  const url = new URL(request.url ?? "/", "http://localhost");
  const kind = socketKind(url.pathname);
  if (!kind) {
    refuse(socket, 404);
    return;
  }
  try {
    if (kind === "runner") {
      if (
        !protocolKeyMatches(request.headers["sec-websocket-protocol"], apiKey)
      ) {
        refuse(socket, 401);
        return;
      }
      phoenix.handleUpgrade(request, socket, head, kind, null);
      return;
    }
    const token = url.searchParams.get("join_token");
    const consumed = token ? await store.consumeToken(token) : null;
    if (
      !consumed &&
      !protocolKeyMatches(request.headers["sec-websocket-protocol"], apiKey)
    ) {
      refuse(socket, 401);
      return;
    }
    phoenix.handleUpgrade(request, socket, head, kind, consumed);
  } catch {
    refuse(socket, 500);
  }
}

function socketKind(pathname: string): "runner" | "client" | "channels" | null {
  if (pathname === "/runner/websocket") return "runner";
  if (pathname === "/client/websocket") return "client";
  if (pathname === "/channels/websocket") return "channels";
  return null;
}

function protocolKeyMatches(
  header: string | string[] | undefined,
  apiKey: string,
): boolean {
  const value = Array.isArray(header) ? header.join(",") : header;
  if (!value) return false;
  for (const part of value.split(",")) {
    const trimmed = part.trim();
    if (!trimmed.startsWith(AUTH_PREFIX)) continue;
    const encoded = trimmed
      .slice(AUTH_PREFIX.length)
      .replace(/-/g, "+")
      .replace(/_/g, "/");
    const padded = encoded + "=".repeat((4 - (encoded.length % 4)) % 4);
    const decoded = Buffer.from(padded, "base64").toString("utf8");
    const left = Buffer.from(decoded);
    const right = Buffer.from(apiKey);
    if (left.length === right.length && timingSafeEqual(left, right))
      return true;
  }
  return false;
}

function refuse(socket: Duplex, status: number): void {
  socket.write(
    `HTTP/1.1 ${status} ${status === 401 ? "Unauthorized" : "Error"}\r\nConnection: close\r\n\r\n`,
  );
  socket.destroy();
}

export type { JoinToken };
