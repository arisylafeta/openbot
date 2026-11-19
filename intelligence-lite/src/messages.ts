/** A persisted chat message, in the shape `getThreadMessages` returns. */
export type StoredMessage = {
  id: string;
  role: string;
  content: unknown;
  activityType: string | null;
  toolCalls: { id: string; name: string; args: string }[] | null;
  toolCallId: string | null;
  position: number;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function toolCallsOf(
  value: unknown,
): { id: string; name: string; args: string }[] | null {
  if (!Array.isArray(value)) return null;
  const calls: { id: string; name: string; args: string }[] = [];
  for (const item of value) {
    if (!isRecord(item) || typeof item.id !== "string") continue;
    const name =
      typeof item.name === "string"
        ? item.name
        : isRecord(item.function) && typeof item.function.name === "string"
          ? item.function.name
          : "";
    const args =
      typeof item.args === "string"
        ? item.args
        : isRecord(item.function) && typeof item.function.arguments === "string"
          ? item.function.arguments
          : "";
    calls.push({ id: item.id, name, args });
  }
  return calls.length > 0 ? calls : null;
}

/**
 * Fold AG-UI events into the thread transcript the runtime reads back.
 *
 * `RUN_STARTED.input.messages` carries the history the run was given, including
 * the new user message. Text and tool events append the assistant's reply.
 * A message id that is already stored is left where it is, so a later run that
 * resends history does not duplicate it. That is what lets the runtime's
 * `fields=id` read skip messages it has already persisted.
 */
export function projectMessages(
  existing: StoredMessage[],
  events: Record<string, unknown>[],
): StoredMessage[] {
  const byId = new Map<string, StoredMessage>(
    existing.map((message) => [
      message.id,
      {
        ...message,
        toolCalls: message.toolCalls
          ? message.toolCalls.map((call) => ({ ...call }))
          : null,
      },
    ]),
  );
  let position =
    existing.reduce((max, message) => Math.max(max, message.position), -1) + 1;

  const ensure = (id: string, role: string): StoredMessage => {
    const found = byId.get(id);
    if (found) return found;
    const created: StoredMessage = {
      id,
      role,
      content: "",
      activityType: null,
      toolCalls: null,
      toolCallId: null,
      position: position++,
    };
    byId.set(id, created);
    return created;
  };

  for (const event of events) {
    const type = event.type;
    if (type === "RUN_STARTED" && isRecord(event.input)) {
      const messages = event.input.messages;
      if (!Array.isArray(messages)) continue;
      for (const raw of messages) {
        if (!isRecord(raw) || typeof raw.id !== "string" || byId.has(raw.id)) {
          continue;
        }
        byId.set(raw.id, {
          id: raw.id,
          role: typeof raw.role === "string" ? raw.role : "user",
          content: raw.content ?? "",
          activityType:
            typeof raw.activityType === "string" ? raw.activityType : null,
          toolCalls: toolCallsOf(raw.toolCalls),
          toolCallId:
            typeof raw.toolCallId === "string" ? raw.toolCallId : null,
          position: position++,
        });
      }
      continue;
    }
    if (type === "TEXT_MESSAGE_START" && typeof event.messageId === "string") {
      const message = ensure(
        event.messageId,
        typeof event.role === "string" ? event.role : "assistant",
      );
      if (typeof event.role === "string") message.role = event.role;
      continue;
    }
    if (
      type === "TEXT_MESSAGE_CONTENT" &&
      typeof event.messageId === "string" &&
      typeof event.delta === "string"
    ) {
      const message = ensure(event.messageId, "assistant");
      message.content =
        typeof message.content === "string"
          ? message.content + event.delta
          : event.delta;
      continue;
    }
    if (type === "TOOL_CALL_START" && typeof event.toolCallId === "string") {
      const parent =
        typeof event.parentMessageId === "string"
          ? event.parentMessageId
          : event.toolCallId;
      const message = ensure(parent, "assistant");
      message.toolCalls ??= [];
      if (!message.toolCalls.some((call) => call.id === event.toolCallId)) {
        message.toolCalls.push({
          id: event.toolCallId,
          name:
            typeof event.toolCallName === "string" ? event.toolCallName : "",
          args: "",
        });
      }
      continue;
    }
    if (
      type === "TOOL_CALL_ARGS" &&
      typeof event.toolCallId === "string" &&
      typeof event.delta === "string"
    ) {
      for (const message of byId.values()) {
        const call = message.toolCalls?.find(
          (item) => item.id === event.toolCallId,
        );
        if (call) call.args += event.delta;
      }
      continue;
    }
    if (type === "TOOL_CALL_RESULT") {
      const id =
        typeof event.messageId === "string"
          ? event.messageId
          : typeof event.toolCallId === "string"
            ? `result:${event.toolCallId}`
            : null;
      if (!id) continue;
      const message = ensure(id, "tool");
      if (typeof event.toolCallId === "string") {
        message.toolCallId = event.toolCallId;
      }
      if ("content" in event) message.content = event.content;
      else if (typeof event.delta === "string") {
        message.content =
          typeof message.content === "string"
            ? message.content + event.delta
            : event.delta;
      }
    }
  }

  return [...byId.values()].sort(
    (left, right) => left.position - right.position,
  );
}

export function isRecordEvent(
  value: unknown,
): value is Record<string, unknown> {
  return isRecord(value);
}

export function terminalType(event: Record<string, unknown>): boolean {
  return event.type === "RUN_FINISHED" || event.type === "RUN_ERROR";
}
