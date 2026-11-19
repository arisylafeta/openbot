import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import postgres, { type Sql, type TransactionSql } from "postgres";
import { quoteIdent } from "./config";
import {
  isRecordEvent,
  projectMessages,
  type StoredMessage,
  terminalType,
} from "./messages";

const MIGRATION_ID = "001_init";
const TOKEN_TTL_MS = 10 * 60_000;
const DEFAULT_LOCK_TTL = 20;
const MAX_LOCK_TTL = 3600;
const LIST_CAP = 100;
const MEMORY_CAP = 200;

export type ThreadSummary = {
  id: string;
  name: string | null;
  createdAt: string;
  updatedAt: string;
  lastUpdatedAt: string;
  lastRunAt?: string;
  archived: boolean;
  agentId: string;
  createdById: string;
  learningContainerId: string | null;
};

type ThreadRow = {
  id: string;
  user_id: string;
  agent_id: string;
  name: string | null;
  archived: boolean;
  learning_container_id: string | null;
  created_at: Date;
  updated_at: Date;
  last_run_at: Date | null;
};

type LockRow = {
  thread_id: string;
  run_id: string;
  user_id: string;
  agent_id: string;
  lock_key_prefix: string | null;
  expires_at: Date;
  completed: boolean;
};

export type JoinToken = {
  token: string;
  kind: string;
  threadId: string | null;
  userId: string | null;
  runId: string | null;
};

export type MemoryRow = {
  id: string;
  kind: string;
  scope: string;
  content: string;
  sourceThreadIds: string[];
  invalidatedAt: string | null;
  score?: number;
  absorbed?: boolean;
  retiredId?: string;
};

export type Refusal = {
  ok: false;
  status: number;
  code: string;
  message: string;
  category: string;
  retryable: boolean;
};

function refusal(
  status: number,
  code: string,
  message: string,
  category: string,
  retryable = false,
): Refusal {
  return { ok: false, status, code, message, category, retryable };
}

function ttlSeconds(value: unknown): number {
  const parsed = typeof value === "number" ? value : DEFAULT_LOCK_TTL;
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_LOCK_TTL;
  return Math.min(Math.floor(parsed), MAX_LOCK_TTL);
}

function summary(row: ThreadRow): ThreadSummary {
  return {
    id: row.id,
    name: row.name,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
    lastUpdatedAt: row.updated_at.toISOString(),
    ...(row.last_run_at ? { lastRunAt: row.last_run_at.toISOString() } : {}),
    archived: row.archived,
    agentId: row.agent_id,
    createdById: row.user_id,
    learningContainerId: row.learning_container_id,
  };
}

function memoryOf(row: {
  id: string;
  kind: string;
  scope: string;
  content: string;
  source_thread_ids: unknown;
  invalidated_at: Date | null;
}): MemoryRow {
  return {
    id: row.id,
    kind: row.kind,
    scope: row.scope,
    content: row.content,
    sourceThreadIds: Array.isArray(row.source_thread_ids)
      ? row.source_thread_ids.filter(
          (id): id is string => typeof id === "string",
        )
      : [],
    invalidatedAt: row.invalidated_at ? row.invalidated_at.toISOString() : null,
  };
}

export class Store {
  private constructor(
    private readonly sql: Sql,
    private readonly schema: string,
  ) {}

  static async open(databaseUrl: string, schema: string): Promise<Store> {
    const admin = postgres(databaseUrl, { max: 1, onnotice: () => {} });
    await admin.unsafe(`CREATE SCHEMA IF NOT EXISTS ${quoteIdent(schema)}`);
    await admin.end({ timeout: 5 });
    const sql = postgres(databaseUrl, {
      max: 10,
      onnotice: () => {},
      connection: { search_path: schema },
    });
    const store = new Store(sql, schema);
    await store.migrate();
    return store;
  }

  async close(): Promise<void> {
    await this.sql.end({ timeout: 5 });
  }

  async dropSchema(): Promise<void> {
    await this.sql.unsafe(
      `DROP SCHEMA IF EXISTS ${quoteIdent(this.schema)} CASCADE`,
    );
  }

  private async migrate(): Promise<void> {
    const source = await readFile(
      new URL("../migrations/001_init.sql", import.meta.url),
      "utf8",
    );
    await this.tx(async (tx) => {
      await tx.unsafe(source);
      await tx`INSERT INTO schema_migrations (id) VALUES (${MIGRATION_ID}) ON CONFLICT DO NOTHING`;
    });
  }

  private tx<T>(fn: (tx: TransactionSql) => Promise<T>): Promise<T> {
    return this.sql.begin(async (tx) => {
      await tx.unsafe(`SET LOCAL search_path TO ${quoteIdent(this.schema)}`);
      return fn(tx);
    }) as Promise<T>;
  }

  async createThread(input: {
    threadId: string;
    userId: string;
    agentId: string;
    name?: string;
    learningContainerId?: string;
  }): Promise<{ ok: true; thread: ThreadSummary } | Refusal> {
    if (!input.threadId || !input.userId || !input.agentId) {
      return refusal(
        400,
        "VALIDATION_ERROR",
        "threadId, userId and agentId are required.",
        "validation",
      );
    }
    return this.tx(async (tx) => {
      const inserted = await tx<ThreadRow[]>`
        INSERT INTO threads (id, user_id, agent_id, name, learning_container_id)
        VALUES (
          ${input.threadId},
          ${input.userId},
          ${input.agentId},
          ${input.name ?? null},
          ${input.learningContainerId ?? null}
        )
        ON CONFLICT (id) DO NOTHING
        RETURNING *
      `;
      const row = inserted[0];
      if (!row) {
        return refusal(
          409,
          "THREAD_ALREADY_EXISTS",
          "A thread with this id already exists.",
          "conflict",
        );
      }
      return { ok: true as const, thread: summary(row) };
    });
  }

  async getThread(
    threadId: string,
    userId: string,
  ): Promise<{ ok: true; thread: ThreadSummary } | Refusal> {
    const rows = await this.sql<ThreadRow[]>`
      SELECT * FROM threads WHERE id = ${threadId} AND user_id = ${userId}
    `;
    const row = rows[0];
    if (!row) {
      return refusal(
        404,
        "THREAD_NOT_FOUND",
        "The thread was not found.",
        "not_found",
      );
    }
    return { ok: true, thread: summary(row) };
  }

  async listThreads(input: {
    userId: string;
    agentId: string;
    includeArchived: boolean;
    limit: number;
    cursor?: string;
  }): Promise<
    | {
        ok: true;
        threads: ThreadSummary[];
        joinCode: string;
        joinToken: string;
        nextCursor: string | null;
      }
    | Refusal
  > {
    if (!input.userId || !input.agentId) {
      return refusal(
        400,
        "VALIDATION_ERROR",
        "userId and agentId are required.",
        "validation",
      );
    }
    const limit = Math.min(Math.max(input.limit || 50, 1), LIST_CAP);
    let cursorTime: string | null = null;
    let cursorId: string | null = null;
    if (input.cursor) {
      const split = input.cursor.indexOf("|");
      if (split <= 0) {
        return refusal(
          400,
          "VALIDATION_ERROR",
          "The cursor is not valid.",
          "validation",
        );
      }
      cursorTime = input.cursor.slice(0, split);
      cursorId = input.cursor.slice(split + 1);
      if (Number.isNaN(Date.parse(cursorTime)) || !cursorId) {
        return refusal(
          400,
          "VALIDATION_ERROR",
          "The cursor is not valid.",
          "validation",
        );
      }
    }
    return this.tx(async (tx) => {
      const rows = cursorTime
        ? await tx<ThreadRow[]>`
            SELECT * FROM threads
            WHERE user_id = ${input.userId}
              AND agent_id = ${input.agentId}
              AND (${input.includeArchived} OR archived = false)
              AND (updated_at, id) < (${cursorTime}::timestamptz, ${cursorId})
            ORDER BY updated_at DESC, id DESC
            LIMIT ${limit}
          `
        : await tx<ThreadRow[]>`
            SELECT * FROM threads
            WHERE user_id = ${input.userId}
              AND agent_id = ${input.agentId}
              AND (${input.includeArchived} OR archived = false)
            ORDER BY updated_at DESC, id DESC
            LIMIT ${limit}
          `;
      const joinCode = await this.code(tx, input.userId, "threads");
      const joinToken = await this.insertToken(tx, {
        kind: "threads",
        userId: input.userId,
      });
      const last = rows.at(-1);
      return {
        ok: true as const,
        threads: rows.map(summary),
        joinCode,
        joinToken,
        nextCursor:
          rows.length === limit && last
            ? `${last.updated_at.toISOString()}|${last.id}`
            : null,
      };
    });
  }

  async updateThread(input: {
    threadId: string;
    userId: string;
    agentId: string;
    name?: string;
    archived?: boolean;
  }): Promise<{ ok: true; thread: ThreadSummary } | Refusal> {
    return this.tx(async (tx) => {
      const current = await tx<ThreadRow[]>`
        SELECT * FROM threads WHERE id = ${input.threadId} AND user_id = ${input.userId} FOR UPDATE
      `;
      const row = current[0];
      if (!row || row.agent_id !== input.agentId) {
        return refusal(
          404,
          "THREAD_NOT_FOUND",
          "The thread was not found.",
          "not_found",
        );
      }
      const updated = await tx<ThreadRow[]>`
        UPDATE threads
        SET name = ${input.name === undefined ? row.name : input.name},
            archived = ${input.archived === undefined ? row.archived : input.archived},
            updated_at = now()
        WHERE id = ${input.threadId}
        RETURNING *
      `;
      const next = updated[0];
      if (!next) {
        return refusal(
          404,
          "THREAD_NOT_FOUND",
          "The thread was not found.",
          "not_found",
        );
      }
      return { ok: true as const, thread: summary(next) };
    });
  }

  async deleteThread(
    threadId: string,
    userId: string,
    agentId: string,
  ): Promise<{ ok: true } | Refusal> {
    const removed = await this.sql`
      DELETE FROM threads
      WHERE id = ${threadId} AND user_id = ${userId} AND agent_id = ${agentId}
      RETURNING id
    `;
    if (removed.length === 0) {
      return refusal(
        404,
        "THREAD_NOT_FOUND",
        "The thread was not found.",
        "not_found",
      );
    }
    return { ok: true };
  }

  async messages(
    threadId: string,
    userId: string,
  ): Promise<{ ok: true; messages: StoredMessage[] } | Refusal> {
    const thread = await this.getThread(threadId, userId);
    if (!thread.ok) return thread;
    const rows = await this.sql<StoredMessage[]>`
      SELECT id, role, content, activity_type AS "activityType",
             tool_calls AS "toolCalls", tool_call_id AS "toolCallId", position
      FROM messages
      WHERE thread_id = ${threadId}
      ORDER BY position ASC
    `;
    return { ok: true, messages: rows };
  }

  async acquireLock(input: {
    threadId: string;
    runId: string;
    userId: string;
    agentId: string;
    supportsBackendThreadId?: boolean;
    learningContainerId?: string;
    lockKeyPrefix?: string;
    ttlSeconds?: number;
  }): Promise<
    | {
        ok: true;
        threadId: string;
        runId: string;
        joinToken: string;
        ttlSeconds: number;
        backendThreadId?: string;
        lock: { key: string; ttlSeconds: number };
      }
    | Refusal
  > {
    if (!input.threadId || !input.runId || !input.userId || !input.agentId) {
      return refusal(
        400,
        "VALIDATION_ERROR",
        "threadId, runId, userId and agentId are required.",
        "validation",
      );
    }
    const ttl = ttlSeconds(input.ttlSeconds);
    return this.tx(async (tx) => {
      const threads = await tx<ThreadRow[]>`
        SELECT * FROM threads WHERE id = ${input.threadId} FOR UPDATE
      `;
      const thread = threads[0];
      if (!thread || thread.user_id !== input.userId) {
        return refusal(
          404,
          "THREAD_NOT_FOUND",
          "The thread was not found.",
          "not_found",
        );
      }
      if (
        input.learningContainerId &&
        thread.learning_container_id &&
        thread.learning_container_id !== input.learningContainerId
      ) {
        return refusal(
          409,
          "LEARNING_CONTAINER_CONFLICT",
          "This thread already belongs to another Learning container.",
          "conflict",
        );
      }
      if (input.learningContainerId && !thread.learning_container_id) {
        await tx`
          UPDATE threads
          SET learning_container_id = ${input.learningContainerId}, updated_at = now()
          WHERE id = ${input.threadId}
        `;
      }
      const locks = await tx<LockRow[]>`
        SELECT * FROM locks WHERE thread_id = ${input.threadId} FOR UPDATE
      `;
      const lock = locks[0];
      const held =
        lock &&
        !lock.completed &&
        lock.expires_at.getTime() > Date.now() &&
        lock.run_id !== input.runId;
      if (held) {
        return refusal(
          409,
          "THREAD_LOCK_FAILED",
          "Another run holds this thread.",
          "conflict",
          true,
        );
      }
      const prefix = input.lockKeyPrefix ?? lock?.lock_key_prefix ?? null;
      if (lock) {
        await tx`
          UPDATE locks
          SET run_id = ${input.runId},
              user_id = ${input.userId},
              agent_id = ${input.agentId},
              lock_key_prefix = ${prefix},
              expires_at = now() + (${ttl} * interval '1 second'),
              completed = false
          WHERE thread_id = ${input.threadId}
        `;
      } else {
        await tx`
          INSERT INTO locks (thread_id, run_id, user_id, agent_id, lock_key_prefix, expires_at)
          VALUES (
            ${input.threadId},
            ${input.runId},
            ${input.userId},
            ${input.agentId},
            ${prefix},
            now() + (${ttl} * interval '1 second')
          )
        `;
      }
      await tx`
        UPDATE threads SET last_run_at = now(), updated_at = now() WHERE id = ${input.threadId}
      `;
      const joinToken = await this.insertToken(tx, {
        kind: "thread",
        threadId: input.threadId,
        userId: input.userId,
        runId: input.runId,
      });
      return {
        ok: true as const,
        threadId: input.threadId,
        runId: input.runId,
        joinToken,
        ttlSeconds: ttl,
        ...(input.supportsBackendThreadId
          ? { backendThreadId: input.threadId }
          : {}),
        lock: {
          key: `${prefix ?? "thread"}:${input.threadId}`,
          ttlSeconds: ttl,
        },
      };
    });
  }

  async renewLock(input: {
    threadId: string;
    runId: string;
    ttlSeconds: number;
    lockKeyPrefix?: string;
  }): Promise<
    | {
        ok: true;
        ttlSeconds: number;
        status: "renewed" | "completed";
        threadId: string;
        runId: string;
      }
    | Refusal
  > {
    const ttl = ttlSeconds(input.ttlSeconds);
    return this.tx(async (tx) => {
      const locks = await tx<LockRow[]>`
        SELECT * FROM locks WHERE thread_id = ${input.threadId} FOR UPDATE
      `;
      const lock = locks[0];
      const finished = await tx<{ present: boolean }[]>`
        SELECT true AS present FROM events
        WHERE thread_id = ${input.threadId}
          AND run_id = ${input.runId}
          AND type IN ('RUN_FINISHED', 'RUN_ERROR')
        LIMIT 1
      `;
      if (lock?.completed || finished.length > 0) {
        return {
          ok: true as const,
          ttlSeconds: 0,
          status: "completed" as const,
          threadId: input.threadId,
          runId: input.runId,
        };
      }
      if (
        !lock ||
        lock.run_id !== input.runId ||
        lock.expires_at.getTime() <= Date.now() ||
        (input.lockKeyPrefix !== undefined &&
          (lock.lock_key_prefix ?? undefined) !== input.lockKeyPrefix)
      ) {
        return refusal(
          409,
          "THREAD_LOCK_RENEWAL_FAILED",
          "The thread lock is no longer held by this run.",
          "conflict",
        );
      }
      await tx`
        UPDATE locks
        SET expires_at = now() + (${ttl} * interval '1 second')
        WHERE thread_id = ${input.threadId}
      `;
      return {
        ok: true as const,
        ttlSeconds: ttl,
        status: "renewed" as const,
        threadId: input.threadId,
        runId: input.runId,
      };
    });
  }

  async releaseLock(threadId: string, runId: string): Promise<void> {
    await this.sql`
      DELETE FROM locks WHERE thread_id = ${threadId} AND run_id = ${runId}
    `;
  }

  async connectThread(input: {
    threadId: string;
    userId: string;
    agentId?: string;
  }): Promise<
    | { ok: true; empty: true }
    | {
        ok: true;
        empty: false;
        threadId: string;
        joinToken: string;
        runId?: string;
      }
    | Refusal
  > {
    return this.tx(async (tx) => {
      const threads = await tx<ThreadRow[]>`
        SELECT * FROM threads
        WHERE id = ${input.threadId}
          AND user_id = ${input.userId}
          AND (${input.agentId ?? null}::text IS NULL OR agent_id = ${input.agentId ?? null})
      `;
      const thread = threads[0];
      if (!thread) return { ok: true as const, empty: true as const };
      const locks = await tx<LockRow[]>`
        SELECT * FROM locks
        WHERE thread_id = ${input.threadId}
          AND completed = false
          AND expires_at > now()
      `;
      const joinToken = await this.insertToken(tx, {
        kind: "thread",
        threadId: input.threadId,
        userId: input.userId,
        runId: locks[0]?.run_id ?? null,
      });
      return {
        ok: true as const,
        empty: false as const,
        threadId: input.threadId,
        joinToken,
        ...(locks[0] ? { runId: locks[0].run_id } : {}),
      };
    });
  }

  async subscribeThreads(userId: string): Promise<{ joinToken: string }> {
    return this.tx(async (tx) => ({
      joinToken: await this.insertToken(tx, { kind: "threads", userId }),
    }));
  }

  async consumeToken(token: string): Promise<JoinToken | null> {
    const rows = await this.sql<
      {
        token: string;
        kind: string;
        thread_id: string | null;
        user_id: string | null;
        run_id: string | null;
      }[]
    >`
      UPDATE join_tokens
      SET used_at = now()
      WHERE token = ${token} AND used_at IS NULL AND expires_at > now()
      RETURNING token, kind, thread_id, user_id, run_id
    `;
    const row = rows[0];
    if (!row) return null;
    return {
      token: row.token,
      kind: row.kind,
      threadId: row.thread_id,
      userId: row.user_id,
      runId: row.run_id,
    };
  }

  async joinCodeFor(userId: string, kind: string): Promise<string | null> {
    const rows = await this.sql<{ code: string }[]>`
      SELECT code FROM join_codes WHERE user_id = ${userId} AND kind = ${kind}
    `;
    return rows[0]?.code ?? null;
  }

  async hasActiveLock(threadId: string, runId: string): Promise<boolean> {
    const rows = await this.sql<{ present: boolean }[]>`
      SELECT true AS present FROM locks
      WHERE thread_id = ${threadId}
        AND run_id = ${runId}
        AND completed = false
        AND expires_at > now()
    `;
    return rows.length > 0;
  }

  /**
   * Persist runner events in order. Duplicates of `metadata.cpki_event_id` are
   * acknowledged and not returned, so a retried batch is not broadcast again.
   */
  async appendEvents(
    threadId: string,
    runId: string,
    events: unknown[],
  ): Promise<Record<string, unknown>[]> {
    return this.tx(async (tx) => {
      const fresh: Record<string, unknown>[] = [];
      for (const event of events) {
        if (!isRecordEvent(event)) continue;
        const metadata = isRecordEvent(event.metadata) ? event.metadata : {};
        const eventId =
          typeof metadata.cpki_event_id === "string"
            ? metadata.cpki_event_id
            : randomUUID();
        const seq =
          typeof metadata.cpki_event_seq === "number"
            ? metadata.cpki_event_seq
            : 0;
        const type = typeof event.type === "string" ? event.type : "UNKNOWN";
        const inserted = await tx<{ payload: Record<string, unknown> }[]>`
          INSERT INTO events (thread_id, run_id, event_id, seq, type, payload)
          VALUES (
            ${threadId},
            ${runId},
            ${eventId},
            ${seq},
            ${type},
            ${tx.json(event as postgres.JSONValue)}
          )
          ON CONFLICT (thread_id, event_id) DO NOTHING
          RETURNING payload
        `;
        const row = inserted[0];
        if (row) fresh.push(row.payload);
      }
      if (fresh.length > 0) {
        const current = await tx<StoredMessage[]>`
          SELECT id, role, content, activity_type AS "activityType",
                 tool_calls AS "toolCalls", tool_call_id AS "toolCallId", position
          FROM messages WHERE thread_id = ${threadId}
          ORDER BY position ASC
        `;
        const projected = projectMessages(current, fresh);
        for (const message of projected) {
          await tx`
            INSERT INTO messages (
              thread_id, id, role, content, activity_type, tool_calls, tool_call_id, position
            )
            VALUES (
              ${threadId},
              ${message.id},
              ${message.role},
              ${tx.json(message.content as postgres.JSONValue)},
              ${message.activityType},
              ${message.toolCalls ? tx.json(message.toolCalls as postgres.JSONValue) : null},
              ${message.toolCallId},
              ${message.position}
            )
            ON CONFLICT (thread_id, id) DO UPDATE
            SET role = EXCLUDED.role,
                content = EXCLUDED.content,
                activity_type = EXCLUDED.activity_type,
                tool_calls = EXCLUDED.tool_calls,
                tool_call_id = EXCLUDED.tool_call_id
          `;
        }
        if (fresh.some(terminalType)) {
          await tx`
            UPDATE locks SET completed = true
            WHERE thread_id = ${threadId} AND run_id = ${runId}
          `;
        }
        await tx`
          UPDATE threads SET updated_at = now(), last_run_at = now() WHERE id = ${threadId}
        `;
      }
      return fresh;
    });
  }

  async eventsForThread(threadId: string): Promise<Record<string, unknown>[]> {
    const rows = await this.sql<{ payload: Record<string, unknown> }[]>`
      SELECT payload FROM events WHERE thread_id = ${threadId} ORDER BY id ASC
    `;
    return rows.map((row) => row.payload);
  }

  async listMemories(input: {
    userId: string;
    scope: string | null;
    includeInvalidated: boolean;
  }): Promise<MemoryRow[]> {
    const rows = await this.sql<
      {
        id: string;
        kind: string;
        scope: string;
        content: string;
        source_thread_ids: unknown;
        invalidated_at: Date | null;
      }[]
    >`
      SELECT id, kind, scope, content, source_thread_ids, invalidated_at
      FROM memories
      WHERE user_id = ${input.userId}
        AND (${input.scope}::text IS NULL OR scope = ${input.scope})
        AND (${input.includeInvalidated} OR invalidated_at IS NULL)
      ORDER BY created_at DESC
      LIMIT ${MEMORY_CAP}
    `;
    return rows.map(memoryOf);
  }

  async createMemory(input: {
    userId: string;
    content: string;
    kind: string;
    scope: string;
    sourceThreadIds: string[];
  }): Promise<MemoryRow> {
    return this.tx(async (tx) => {
      const existing = await tx<
        {
          id: string;
          kind: string;
          scope: string;
          content: string;
          source_thread_ids: unknown;
          invalidated_at: Date | null;
        }[]
      >`
        SELECT id, kind, scope, content, source_thread_ids, invalidated_at
        FROM memories
        WHERE user_id = ${input.userId}
          AND scope = ${input.scope}
          AND invalidated_at IS NULL
          AND lower(content) = lower(${input.content})
        LIMIT 1
      `;
      const absorbed = existing[0];
      if (absorbed) return { ...memoryOf(absorbed), absorbed: true };
      const id = randomUUID();
      const inserted = await tx<
        {
          id: string;
          kind: string;
          scope: string;
          content: string;
          source_thread_ids: unknown;
          invalidated_at: Date | null;
        }[]
      >`
        INSERT INTO memories (id, user_id, scope, kind, content, source_thread_ids)
        VALUES (
          ${id},
          ${input.userId},
          ${input.scope},
          ${input.kind},
          ${input.content},
          ${tx.json(input.sourceThreadIds)}
        )
        RETURNING id, kind, scope, content, source_thread_ids, invalidated_at
      `;
      const row = inserted[0];
      if (!row) throw new Error("memory insert returned no row");
      return memoryOf(row);
    });
  }

  async updateMemory(input: {
    userId: string;
    id: string;
    content: string;
    kind: string;
    scope: string;
    sourceThreadIds: string[];
  }): Promise<MemoryRow | Refusal> {
    return this.tx(async (tx) => {
      const current = await tx<{ id: string; scope: string }[]>`
        SELECT id, scope FROM memories
        WHERE id = ${input.id}
          AND user_id = ${input.userId}
          AND invalidated_at IS NULL
        FOR UPDATE
      `;
      const row = current[0];
      if (!row || row.scope !== input.scope) {
        return refusal(
          404,
          "MEMORY_NOT_FOUND",
          "The memory was not found.",
          "not_found",
        );
      }
      await tx`
        UPDATE memories SET invalidated_at = now() WHERE id = ${input.id}
      `;
      const id = randomUUID();
      const inserted = await tx<
        {
          id: string;
          kind: string;
          scope: string;
          content: string;
          source_thread_ids: unknown;
          invalidated_at: Date | null;
        }[]
      >`
        INSERT INTO memories (id, user_id, scope, kind, content, source_thread_ids)
        VALUES (
          ${id},
          ${input.userId},
          ${input.scope},
          ${input.kind},
          ${input.content},
          ${tx.json(input.sourceThreadIds)}
        )
        RETURNING id, kind, scope, content, source_thread_ids, invalidated_at
      `;
      const created = inserted[0];
      if (!created) throw new Error("memory insert returned no row");
      return { ...memoryOf(created), retiredId: input.id };
    });
  }

  async removeMemory(
    userId: string,
    id: string,
  ): Promise<{ ok: true } | Refusal> {
    const updated = await this.sql`
      UPDATE memories SET invalidated_at = COALESCE(invalidated_at, now())
      WHERE id = ${id} AND user_id = ${userId}
      RETURNING id
    `;
    if (updated.length === 0) {
      return refusal(
        404,
        "MEMORY_NOT_FOUND",
        "The memory was not found.",
        "not_found",
      );
    }
    return { ok: true };
  }

  /**
   * v1 recall: score live memories by how many query words they contain.
   * Not embeddings. Enough for OpenBot's memory context on a single user.
   */
  async recallMemories(input: {
    userId: string;
    query: string;
    limit: number;
    scope: string;
  }): Promise<MemoryRow[]> {
    const rows = await this.listMemories({
      userId: input.userId,
      scope: input.scope,
      includeInvalidated: false,
    });
    const terms = input.query
      .toLowerCase()
      .split(/[^\p{L}\p{N}]+/u)
      .filter((term) => term.length > 1);
    const scored = rows
      .map((memory) => {
        const haystack = memory.content.toLowerCase();
        if (terms.length === 0) return { memory, score: 0 };
        const hits = terms.filter((term) => haystack.includes(term)).length;
        return { memory, score: hits / terms.length };
      })
      .filter((item) => terms.length === 0 || item.score > 0)
      .sort((left, right) => right.score - left.score)
      .slice(0, Math.min(Math.max(input.limit, 1), 50));
    return scored.map((item) => ({ ...item.memory, score: item.score }));
  }

  async subscribeMemories(
    userId: string,
  ): Promise<{ joinToken: string; joinCode: string }> {
    return this.tx(async (tx) => ({
      joinToken: await this.insertToken(tx, { kind: "memories", userId }),
      joinCode: await this.code(tx, userId, "memories"),
    }));
  }

  async putFile(
    id: string,
    bytes: Buffer,
    mimeType: string | null,
  ): Promise<void> {
    await this.sql`
      INSERT INTO files (id, bytes, mime_type)
      VALUES (${id}, ${bytes}, ${mimeType})
      ON CONFLICT (id) DO UPDATE SET bytes = EXCLUDED.bytes, mime_type = EXCLUDED.mime_type
    `;
  }

  async getFile(
    id: string,
  ): Promise<{ bytes: Buffer; mimeType: string | null } | null> {
    const rows = await this.sql<{ bytes: Buffer; mime_type: string | null }[]>`
      SELECT bytes, mime_type FROM files WHERE id = ${id}
    `;
    const row = rows[0];
    if (!row) return null;
    return { bytes: row.bytes, mimeType: row.mime_type };
  }

  async annotate(
    clientEventId: string,
    body: unknown,
  ): Promise<{ id: string; duplicate: boolean }> {
    return this.tx(async (tx) => {
      const existing = await tx<{ id: string }[]>`
        SELECT id FROM annotations WHERE client_event_id = ${clientEventId}
      `;
      const prior = existing[0];
      if (prior) return { id: prior.id, duplicate: true };
      const id = randomUUID();
      await tx`
        INSERT INTO annotations (client_event_id, id, body)
        VALUES (${clientEventId}, ${id}, ${tx.json(body as postgres.JSONValue)})
      `;
      return { id, duplicate: false };
    });
  }

  private async code(
    tx: TransactionSql,
    userId: string,
    kind: string,
  ): Promise<string> {
    const existing = await tx<{ code: string }[]>`
      SELECT code FROM join_codes WHERE user_id = ${userId} AND kind = ${kind}
    `;
    const found = existing[0];
    if (found) return found.code;
    const code = randomUUID();
    await tx`
      INSERT INTO join_codes (user_id, kind, code) VALUES (${userId}, ${kind}, ${code})
      ON CONFLICT DO NOTHING
    `;
    const again = await tx<{ code: string }[]>`
      SELECT code FROM join_codes WHERE user_id = ${userId} AND kind = ${kind}
    `;
    return again[0]?.code ?? code;
  }

  private async insertToken(
    tx: TransactionSql,
    input: {
      kind: string;
      threadId?: string | null;
      userId?: string | null;
      runId?: string | null;
    },
  ): Promise<string> {
    const token = randomUUID();
    await tx`
      INSERT INTO join_tokens (token, kind, thread_id, user_id, run_id, expires_at)
      VALUES (
        ${token},
        ${input.kind},
        ${input.threadId ?? null},
        ${input.userId ?? null},
        ${input.runId ?? null},
        now() + (${TOKEN_TTL_MS} * interval '1 millisecond')
      )
    `;
    return token;
  }
}
