import { type Started, startServer } from "../src/server";

export function databaseUrl(): string {
  const url = process.env.TEST_DATABASE_URL || process.env.DATABASE_URL;
  if (!url) {
    throw new Error(
      "TEST_DATABASE_URL or DATABASE_URL is required to test intelligence-lite.",
    );
  }
  return url;
}

export async function boot(
  name: string,
): Promise<Started & { schema: string }> {
  const schema = `il_${name}_${Math.random().toString(36).slice(2, 8)}`.slice(
    0,
    63,
  );
  const started = await startServer({
    databaseUrl: databaseUrl(),
    apiKey: "cpk-test-key",
    schema,
    host: "127.0.0.1",
    port: 0,
  });
  return { ...started, schema };
}

export function auth(apiKey: string): HeadersInit {
  return {
    authorization: `Bearer ${apiKey}`,
    "content-type": "application/json",
  };
}

export type PhoenixFrame = [
  string | null,
  string | null,
  string,
  string,
  unknown,
];

/** One queue per socket, so two waiters never both observe the same frame. */
export function frameQueue(ws: WebSocket): {
  next: (timeoutMs?: number) => Promise<PhoenixFrame>;
} {
  const queue: PhoenixFrame[] = [];
  const waiters: ((frame: PhoenixFrame) => void)[] = [];
  ws.addEventListener("message", (event) => {
    const data = event.data;
    const frame = JSON.parse(
      typeof data === "string" ? data : String(data),
    ) as PhoenixFrame;
    const waiter = waiters.shift();
    if (waiter) waiter(frame);
    else queue.push(frame);
  });
  return {
    next(timeoutMs = 5_000) {
      const queued = queue.shift();
      if (queued) return Promise.resolve(queued);
      return new Promise((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error("timed out waiting for a frame")),
          timeoutMs,
        );
        waiters.push((frame) => {
          clearTimeout(timer);
          resolve(frame);
        });
      });
    },
  };
}
