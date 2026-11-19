import { timingSafeEqual } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";
import { errorBody, NOT_ENABLED } from "./http-error";
import type { Store } from "./store";

const MAX_BODY = 1_000_000;

export type HttpResult = {
  status: number;
  body?: unknown;
  raw?: Buffer;
  contentType?: string;
};

function keysMatch(left: string, right: string): boolean {
  const a = Buffer.from(left);
  const b = Buffer.from(right);
  if (a.length !== b.length) return false;
  return timingSafeEqual(a, b);
}

export function bearer(header: string | undefined, apiKey: string): boolean {
  if (!header) return false;
  const match = /^Bearer\s+(\S+)\s*$/.exec(header);
  if (!match?.[1]) return false;
  return keysMatch(match[1], apiKey);
}

type Grant = {
  user: "none" | "read" | "read-write";
  project: "none" | "read" | "read-write";
};

function parseGrant(header: string | undefined): Grant | null | "invalid" {
  if (!header) return null;
  try {
    const value = JSON.parse(header) as Partial<Grant>;
    if (
      (value.user === "none" ||
        value.user === "read" ||
        value.user === "read-write") &&
      (value.project === "none" ||
        value.project === "read" ||
        value.project === "read-write")
    ) {
      return { user: value.user, project: value.project };
    }
    return "invalid";
  } catch {
    return "invalid";
  }
}

function allows(grant: Grant | null, scope: string, write: boolean): boolean {
  if (!grant) return true;
  const level = scope === "project" ? grant.project : grant.user;
  if (level === "none") return false;
  if (write && level !== "read-write") return false;
  return true;
}

async function readBody(request: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > MAX_BODY) {
      throw Object.assign(new Error("body too large"), { status: 413 });
    }
    chunks.push(buffer);
  }
  if (chunks.length === 0) return {};
  const text = Buffer.concat(chunks).toString("utf8");
  if (!text.trim()) return {};
  return JSON.parse(text) as unknown;
}

async function readRaw(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > MAX_BODY) {
      throw Object.assign(new Error("body too large"), { status: 413 });
    }
    chunks.push(buffer);
  }
  return Buffer.concat(chunks);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringField(body: Record<string, unknown>, key: string): string {
  const value = body[key];
  return typeof value === "string" ? value : "";
}

function userHeader(request: IncomingMessage): string {
  const value = request.headers["x-cpki-user-id"];
  return typeof value === "string" ? value : "";
}

/**
 * The REST surface the MIT `CopilotKitIntelligence` client calls.
 * Learning, inspector and trajectories answer a stable not-enabled envelope.
 */
export async function handleHttp(
  store: Store,
  apiKey: string,
  request: IncomingMessage,
  url: URL,
): Promise<HttpResult> {
  const path = url.pathname.replace(/\/+$/, "") || "/";
  const method = request.method ?? "GET";

  if (method === "GET" && path === "/healthz") {
    return { status: 200, body: { ok: true } };
  }

  if (!bearer(request.headers.authorization, apiKey)) {
    return {
      status: 401,
      body: errorBody(
        "AUTH_UNAUTHENTICATED",
        "Authentication is required.",
        "auth",
      ),
    };
  }

  if (method === "GET" && path === "/api/entitlements/runtime") {
    return {
      status: 200,
      body: {
        status: "ready",
        entitlement: {
          active: true,
          source: "selfHostedDeploymentLicense",
          features: { memory: true, learning: false, trajectories: false },
          limits: {},
          planCode: "free",
        },
      },
    };
  }

  if (
    path.startsWith("/api/v1/learning") ||
    path.startsWith("/api/v1/learning/")
  ) {
    return { status: 404, body: NOT_ENABLED.learning };
  }
  if (
    path === "/api/inspector/metadata" ||
    path === "/api/inspector/learning"
  ) {
    return { status: 404, body: NOT_ENABLED.inspector };
  }
  if (path.startsWith("/api/_inspect/")) {
    return { status: 404, body: NOT_ENABLED.inspector };
  }
  if (method === "POST" && path === "/api/trajectories/join") {
    return { status: 403, body: NOT_ENABLED.trajectories };
  }

  try {
    return await route(store, request, url, path, method);
  } catch (error) {
    if (error instanceof SyntaxError) {
      return {
        status: 400,
        body: errorBody(
          "VALIDATION_ERROR",
          "The request body is not JSON.",
          "validation",
        ),
      };
    }
    const status =
      error && typeof error === "object" && "status" in error
        ? Number((error as { status: unknown }).status)
        : 500;
    if (status === 413) {
      return {
        status: 413,
        body: errorBody(
          "VALIDATION_ERROR",
          "The request body is too large.",
          "validation",
        ),
      };
    }
    throw error;
  }
}

async function route(
  store: Store,
  request: IncomingMessage,
  url: URL,
  path: string,
  method: string,
): Promise<HttpResult> {
  if (path === "/api/threads" && method === "GET") {
    const limit = Number(url.searchParams.get("limit") ?? "50");
    const listed = await store.listThreads({
      userId: url.searchParams.get("userId") ?? "",
      agentId: url.searchParams.get("agentId") ?? "",
      includeArchived: url.searchParams.get("includeArchived") === "true",
      limit: Number.isFinite(limit) ? limit : 50,
      ...(url.searchParams.get("cursor")
        ? { cursor: url.searchParams.get("cursor") ?? "" }
        : {}),
    });
    if (!listed.ok) return failure(listed);
    return {
      status: 200,
      body: {
        threads: listed.threads,
        joinCode: listed.joinCode,
        joinToken: listed.joinToken,
        nextCursor: listed.nextCursor,
      },
    };
  }

  if (path === "/api/threads" && method === "POST") {
    const body = record(await readBody(request));
    const created = await store.createThread({
      threadId: stringField(body, "threadId"),
      userId: stringField(body, "userId"),
      agentId: stringField(body, "agentId"),
      ...(typeof body.name === "string" ? { name: body.name } : {}),
      ...(typeof body.learningContainerId === "string"
        ? { learningContainerId: body.learningContainerId }
        : {}),
    });
    if (!created.ok) return failure(created);
    return { status: 200, body: { thread: created.thread } };
  }

  if (path === "/api/threads/subscribe" && method === "POST") {
    const body = record(await readBody(request));
    const userId = stringField(body, "userId");
    if (!userId) {
      return {
        status: 400,
        body: errorBody(
          "VALIDATION_ERROR",
          "userId is required.",
          "validation",
        ),
      };
    }
    return { status: 200, body: await store.subscribeThreads(userId) };
  }

  const thread = path.match(/^\/api\/threads\/([^/]+)$/);
  if (thread?.[1]) {
    const threadId = decodeURIComponent(thread[1]);
    if (method === "GET") {
      const found = await store.getThread(
        threadId,
        url.searchParams.get("userId") ?? "",
      );
      if (!found.ok) return failure(found);
      return { status: 200, body: { thread: found.thread } };
    }
    if (method === "PATCH") {
      const body = record(await readBody(request));
      const updated = await store.updateThread({
        threadId,
        userId: stringField(body, "userId"),
        agentId: stringField(body, "agentId"),
        ...(typeof body.name === "string" ? { name: body.name } : {}),
        ...(typeof body.archived === "boolean"
          ? { archived: body.archived }
          : {}),
      });
      if (!updated.ok) return failure(updated);
      return { status: 200, body: { thread: updated.thread } };
    }
    if (method === "DELETE") {
      const body = record(await readBody(request));
      const removed = await store.deleteThread(
        threadId,
        stringField(body, "userId"),
        stringField(body, "agentId"),
      );
      if (!removed.ok) return failure(removed);
      return { status: 204 };
    }
  }

  const messages = path.match(/^\/api\/threads\/([^/]+)\/messages$/);
  if (messages?.[1] && method === "GET") {
    const found = await store.messages(
      decodeURIComponent(messages[1]),
      url.searchParams.get("userId") ?? "",
    );
    if (!found.ok) return failure(found);
    return {
      status: 200,
      body: {
        messages: found.messages.map((message) => ({
          id: message.id,
          role: message.role,
          content: message.content,
          ...(message.activityType
            ? { activityType: message.activityType }
            : {}),
          ...(message.toolCalls ? { toolCalls: message.toolCalls } : {}),
          ...(message.toolCallId ? { toolCallId: message.toolCallId } : {}),
        })),
      },
    };
  }

  const lock = path.match(/^\/api\/threads\/([^/]+)\/lock$/);
  if (lock?.[1]) {
    const threadId = decodeURIComponent(lock[1]);
    if (method === "POST") {
      const body = record(await readBody(request));
      const acquired = await store.acquireLock({
        threadId,
        runId: stringField(body, "runId"),
        userId: stringField(body, "userId"),
        agentId: stringField(body, "agentId"),
        ...(typeof body.supportsBackendThreadId === "boolean"
          ? { supportsBackendThreadId: body.supportsBackendThreadId }
          : {}),
        ...(typeof body.learningContainerId === "string"
          ? { learningContainerId: body.learningContainerId }
          : {}),
        ...(typeof body.lockKeyPrefix === "string"
          ? { lockKeyPrefix: body.lockKeyPrefix }
          : {}),
        ...(typeof body.ttlSeconds === "number"
          ? { ttlSeconds: body.ttlSeconds }
          : {}),
      });
      if (!acquired.ok) return failure(acquired);
      const { ok: _acquired, ...lockBody } = acquired;
      return { status: 200, body: lockBody };
    }
    if (method === "PATCH") {
      const body = record(await readBody(request));
      const renewed = await store.renewLock({
        threadId,
        runId: stringField(body, "runId"),
        ttlSeconds: typeof body.ttlSeconds === "number" ? body.ttlSeconds : 20,
        ...(typeof body.lockKeyPrefix === "string"
          ? { lockKeyPrefix: body.lockKeyPrefix }
          : {}),
      });
      if (!renewed.ok) return failure(renewed);
      const { ok: _renewed, ...renewedBody } = renewed;
      return { status: 200, body: renewedBody };
    }
    if (method === "DELETE") {
      const body = record(await readBody(request));
      await store.releaseLock(threadId, stringField(body, "runId"));
      return { status: 204 };
    }
  }

  const joinCode = path.match(/^\/api\/threads\/([^/]+)\/join-code$/);
  if (joinCode?.[1] && method === "GET") {
    const connected = await store.connectThread({
      threadId: decodeURIComponent(joinCode[1]),
      userId: url.searchParams.get("userId") ?? "",
    });
    if (!connected.ok) return failure(connected);
    if (connected.empty || !("joinToken" in connected)) {
      return {
        status: 404,
        body: errorBody(
          "THREAD_NOT_FOUND",
          "The thread was not found.",
          "not_found",
        ),
      };
    }
    const { ok: _joined, empty: _blank, ...credentials } = connected;
    return { status: 200, body: credentials };
  }

  const connect = path.match(/^\/api\/threads\/([^/]+)\/connect$/);
  if (connect?.[1] && method === "POST") {
    const body = record(await readBody(request));
    const connected = await store.connectThread({
      threadId: decodeURIComponent(connect[1]),
      userId: stringField(body, "userId"),
      agentId: stringField(body, "agentId"),
    });
    if (!connected.ok) return failure(connected);
    if (connected.empty) return { status: 204 };
    const { ok: _connected, empty: _empty, ...connection } = connected;
    return { status: 200, body: connection };
  }

  if (path === "/api/memories" && (method === "GET" || method === "POST")) {
    return memoriesCollection(store, request, url, method);
  }
  if (path === "/api/memories/recall" && method === "POST") {
    return recall(store, request);
  }
  if (path === "/api/memories/subscribe" && method === "POST") {
    const userId = userHeader(request);
    if (!userId) {
      return {
        status: 400,
        body: errorBody(
          "VALIDATION_ERROR",
          "x-cpki-user-id is required.",
          "validation",
        ),
      };
    }
    return { status: 200, body: await store.subscribeMemories(userId) };
  }
  const memory = path.match(/^\/api\/memories\/([^/]+)$/);
  if (memory?.[1]) {
    return oneMemory(store, request, decodeURIComponent(memory[1]), method);
  }

  const file = path.match(/^\/api\/channels\/files\/([^/]+)$/);
  if (file?.[1] && method === "GET") {
    const found = await store.getFile(decodeURIComponent(file[1]));
    if (!found) {
      return {
        status: 404,
        body: errorBody(
          "FILE_NOT_FOUND",
          "The file was not found.",
          "not_found",
        ),
      };
    }
    return {
      status: 200,
      raw: found.bytes,
      contentType: found.mimeType ?? "application/octet-stream",
    };
  }
  if (file?.[1] && method === "PUT") {
    const bytes = await readRaw(request);
    const mime = request.headers["content-type"];
    await store.putFile(
      decodeURIComponent(file[1]),
      bytes,
      typeof mime === "string" ? mime : null,
    );
    return { status: 204 };
  }

  const annotate = path.match(/^\/connector\/annotate\/([^/]+)$/);
  if (annotate?.[1] && method === "PUT") {
    const body = await readBody(request);
    const saved = await store.annotate(decodeURIComponent(annotate[1]), body);
    return { status: 200, body: saved };
  }

  return {
    status: 404,
    body: errorBody("ROUTE_NOT_FOUND", "Route not found.", "not_found"),
  };
}

function record(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {};
}

function failure(result: {
  status: number;
  code: string;
  message: string;
  category: string;
  retryable: boolean;
}): HttpResult {
  return {
    status: result.status,
    body: errorBody(
      result.code,
      result.message,
      result.category,
      result.retryable,
    ),
  };
}

async function memoriesCollection(
  store: Store,
  request: IncomingMessage,
  url: URL,
  method: string,
): Promise<HttpResult> {
  const userId = userHeader(request);
  const grant = parseGrant(
    typeof request.headers["x-cpki-memory-grant"] === "string"
      ? request.headers["x-cpki-memory-grant"]
      : undefined,
  );
  if (grant === "invalid") {
    return {
      status: 400,
      body: errorBody(
        "VALIDATION_ERROR",
        "x-cpki-memory-grant is not valid.",
        "validation",
      ),
    };
  }
  if (!userId) {
    return {
      status: 400,
      body: errorBody(
        "VALIDATION_ERROR",
        "x-cpki-user-id is required.",
        "validation",
      ),
    };
  }
  if (method === "GET") {
    if (!allows(grant, "user", false)) {
      return {
        status: 403,
        body: errorBody(
          "MEMORY_NOT_ENTITLED",
          "Memory is not granted.",
          "permission",
        ),
      };
    }
    const memories = await store.listMemories({
      userId,
      scope: null,
      includeInvalidated: url.searchParams.get("includeInvalidated") === "true",
    });
    return {
      status: 200,
      body: {
        memories: memories.filter(
          (memory) =>
            memory.scope === "user" || allows(grant, memory.scope, false),
        ),
      },
    };
  }
  const body = record(await readBody(request));
  const scope = typeof body.scope === "string" ? body.scope : "user";
  if (!allows(grant, scope, true)) {
    return {
      status: 403,
      body: errorBody(
        "MEMORY_NOT_ENTITLED",
        "Memory is not granted.",
        "permission",
      ),
    };
  }
  if (typeof body.content !== "string" || typeof body.kind !== "string") {
    return {
      status: 400,
      body: errorBody(
        "VALIDATION_ERROR",
        "content and kind are required.",
        "validation",
      ),
    };
  }
  const created = await store.createMemory({
    userId,
    content: body.content,
    kind: body.kind,
    scope,
    sourceThreadIds: strings(body.sourceThreadIds),
  });
  return { status: 200, body: created };
}

async function recall(
  store: Store,
  request: IncomingMessage,
): Promise<HttpResult> {
  const userId = userHeader(request);
  const grant = parseGrant(
    typeof request.headers["x-cpki-memory-grant"] === "string"
      ? request.headers["x-cpki-memory-grant"]
      : undefined,
  );
  if (grant === "invalid") {
    return {
      status: 400,
      body: errorBody(
        "VALIDATION_ERROR",
        "x-cpki-memory-grant is not valid.",
        "validation",
      ),
    };
  }
  if (!userId) {
    return {
      status: 400,
      body: errorBody(
        "VALIDATION_ERROR",
        "x-cpki-user-id is required.",
        "validation",
      ),
    };
  }
  const body = record(await readBody(request));
  const scope = typeof body.scope === "string" ? body.scope : "user";
  if (!allows(grant, scope, false)) {
    return {
      status: 403,
      body: errorBody(
        "MEMORY_NOT_ENTITLED",
        "Memory is not granted.",
        "permission",
      ),
    };
  }
  if (typeof body.query !== "string") {
    return {
      status: 400,
      body: errorBody("VALIDATION_ERROR", "query is required.", "validation"),
    };
  }
  const memories = await store.recallMemories({
    userId,
    query: body.query,
    limit: typeof body.limit === "number" ? body.limit : 8,
    scope,
  });
  return { status: 200, body: { memories } };
}

async function oneMemory(
  store: Store,
  request: IncomingMessage,
  id: string,
  method: string,
): Promise<HttpResult> {
  const userId = userHeader(request);
  const grant = parseGrant(
    typeof request.headers["x-cpki-memory-grant"] === "string"
      ? request.headers["x-cpki-memory-grant"]
      : undefined,
  );
  if (!userId) {
    return {
      status: 400,
      body: errorBody(
        "VALIDATION_ERROR",
        "x-cpki-user-id is required.",
        "validation",
      ),
    };
  }
  if (method === "DELETE") {
    const removed = await store.removeMemory(userId, id);
    if (!removed.ok) return failure(removed);
    return { status: 204 };
  }
  if (method === "PATCH") {
    const body = record(await readBody(request));
    const scope = typeof body.scope === "string" ? body.scope : "user";
    if (grant === "invalid" || !allows(grant, scope, true)) {
      return {
        status: 403,
        body: errorBody(
          "MEMORY_NOT_ENTITLED",
          "Memory is not granted.",
          "permission",
        ),
      };
    }
    if (typeof body.content !== "string" || typeof body.kind !== "string") {
      return {
        status: 400,
        body: errorBody(
          "VALIDATION_ERROR",
          "content and kind are required.",
          "validation",
        ),
      };
    }
    const updated = await store.updateMemory({
      userId,
      id,
      content: body.content,
      kind: body.kind,
      scope,
      sourceThreadIds: strings(body.sourceThreadIds),
    });
    if ("ok" in updated) return failure(updated);
    return { status: 200, body: updated };
  }
  return {
    status: 404,
    body: errorBody("ROUTE_NOT_FOUND", "Route not found.", "not_found"),
  };
}

function strings(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === "string");
}

export function writeResult(
  response: ServerResponse,
  result: HttpResult,
): void {
  if (result.raw) {
    response.writeHead(result.status, {
      "content-type": result.contentType ?? "application/octet-stream",
    });
    response.end(result.raw);
    return;
  }
  if (result.status === 204 || result.body === undefined) {
    response.writeHead(result.status);
    response.end();
    return;
  }
  response.writeHead(result.status, { "content-type": "application/json" });
  response.end(JSON.stringify(result.body));
}
