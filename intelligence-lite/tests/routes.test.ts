import { afterAll, beforeAll, expect, test } from "bun:test";
import { CopilotKitIntelligence } from "@copilotkit/runtime/v2";
import { auth, boot } from "./harness";

let lite: Awaited<ReturnType<typeof boot>>;
let client: CopilotKitIntelligence;

beforeAll(async () => {
  lite = await boot("routes");
  client = new CopilotKitIntelligence({
    apiUrl: lite.url,
    wsUrl: lite.wsUrl,
    apiKey: lite.apiKey,
  });
});

afterAll(async () => {
  await lite?.close();
});

test("refuses a missing key and reports a ready entitlement", async () => {
  const anonymous = await fetch(`${lite.url}/api/entitlements/runtime`);
  expect(anonymous.status).toBe(401);

  const entitlements = await client.getRuntimeEntitlements();
  expect(entitlements.status).toBe("ready");
  if (entitlements.status !== "ready") return;
  expect(entitlements.entitlement.active).toBe(true);
  expect(entitlements.entitlement.features.memory).toBe(true);
});

test("learning, inspector and trajectories answer not enabled", async () => {
  const headers = auth(lite.apiKey);
  const learning = await fetch(
    `${lite.url}/api/v1/learning/containers/openbot`,
    {
      headers,
    },
  );
  expect(learning.status).toBe(404);
  expect(await learning.json()).toMatchObject({
    error: { code: "ROUTE_NOT_FOUND", retryable: false },
  });

  const inspector = await fetch(`${lite.url}/api/inspector/metadata`, {
    headers,
  });
  expect(inspector.status).toBe(404);

  const trajectories = await fetch(`${lite.url}/api/trajectories/join`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      trajectoryId: "00000000-0000-4000-8000-000000000001",
      appUserId: "person",
    }),
  });
  expect(trajectories.status).toBe(403);
  expect(await trajectories.json()).toMatchObject({
    error: { code: "TRAJECTORIES_NOT_ENABLED", retryable: false },
  });
});

test("creates a thread, locks it, and refuses a second run", async () => {
  const thread = await client.createThread({
    threadId: "thread-1",
    userId: "person",
    agentId: "general-assistant",
  });
  expect(thread.id).toBe("thread-1");

  const missing = client.getThread({
    threadId: "thread-1",
    userId: "someone-else",
  });
  await expect(missing).rejects.toMatchObject({ status: 404 });

  const lock = await client.ɵacquireThreadLock({
    threadId: "thread-1",
    runId: "run-1",
    userId: "person",
    agentId: "general-assistant",
    supportsBackendThreadId: true,
    ttlSeconds: 30,
  });
  expect(lock).toMatchObject({
    threadId: "thread-1",
    runId: "run-1",
    ttlSeconds: 30,
    backendThreadId: "thread-1",
  });
  expect(lock.joinToken.length).toBeGreaterThan(0);

  const conflict = client.ɵacquireThreadLock({
    threadId: "thread-1",
    runId: "run-2",
    userId: "person",
    agentId: "general-assistant",
  });
  await expect(conflict).rejects.toMatchObject({
    status: 409,
    retryable: true,
  });

  const renewed = await client.ɵrenewThreadLock({
    threadId: "thread-1",
    runId: "run-1",
    ttlSeconds: 30,
  });
  expect(renewed.status).toBe("renewed");
  expect(renewed.ttlSeconds).toBe(30);

  await client.ɵcleanupThreadLock({ threadId: "thread-1", runId: "run-1" });
  const again = await client.ɵacquireThreadLock({
    threadId: "thread-1",
    runId: "run-3",
    userId: "person",
    agentId: "general-assistant",
    ttlSeconds: 20,
  });
  expect(again.runId).toBe("run-3");
});

test("stores and recalls a memory with a basic text match", async () => {
  const grant = { user: "read-write", project: "none" } as const;
  const saved = await client.createMemory({
    userId: "person",
    memoryGrant: grant,
    content: "The warehouse door code is 1942.",
    kind: "topical",
    scope: "user",
  });
  expect(saved.content).toContain("1942");

  const again = await client.createMemory({
    userId: "person",
    memoryGrant: grant,
    content: "The warehouse door code is 1942.",
    kind: "topical",
    scope: "user",
  });
  expect(again.absorbed).toBe(true);
  expect(again.id).toBe(saved.id);

  const recalled = await client.recallMemories({
    userId: "person",
    memoryGrant: grant,
    query: "warehouse door",
    limit: 5,
    scope: "user",
  });
  expect(recalled.memories.map((memory) => memory.id)).toContain(saved.id);
  expect(recalled.memories[0]?.score).toBeGreaterThan(0);

  const listed = await client.listMemories({
    userId: "person",
    memoryGrant: grant,
  });
  expect(listed.memories.some((memory) => memory.id === saved.id)).toBe(true);

  const replaced = await client.updateMemory({
    userId: "person",
    memoryGrant: grant,
    id: saved.id,
    content: "The warehouse door code is 1943.",
    kind: "topical",
    scope: "user",
  });
  expect(replaced.retiredId).toBe(saved.id);
  expect(replaced.id).not.toBe(saved.id);

  await client.removeMemory({
    userId: "person",
    memoryGrant: grant,
    id: replaced.id,
  });
  const after = await client.listMemories({
    userId: "person",
    memoryGrant: grant,
  });
  expect(after.memories.some((memory) => memory.id === replaced.id)).toBe(
    false,
  );
});

test("serves a channel file that was stored", async () => {
  const put = await fetch(`${lite.url}/api/channels/files/file-1`, {
    method: "PUT",
    headers: {
      authorization: `Bearer ${lite.apiKey}`,
      "content-type": "text/plain",
    },
    body: "hello file",
  });
  expect(put.status).toBe(204);
  const got = await fetch(`${lite.url}/api/channels/files/file-1`, {
    headers: { authorization: `Bearer ${lite.apiKey}` },
  });
  expect(got.status).toBe(200);
  expect(await got.text()).toBe("hello file");
});
