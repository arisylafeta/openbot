# intelligence-lite

A small self-hosted stand-in for the CopilotKit Intelligence service OpenBot
talks to. It implements the REST and Phoenix Channels surface of the MIT
`@copilotkit/runtime` client (`CopilotKitIntelligence` in
`dist/v2/runtime/intelligence-platform/client.mjs`, 1.85.0) well enough for
OpenBot to boot and run a chat turn without CopilotKit's hosted Intelligence.

It is not the proprietary Intelligence product. Learning, the inspector, and
trajectories answer a stable "not enabled" error. Memory recall is a word
match, not embeddings.

## Run

Required environment:

| Variable | Meaning |
| --- | --- |
| `DATABASE_URL` | Postgres URL. The same database as OpenBot is fine. Tables live in `INTELLIGENCE_SCHEMA` (default `intelligence_lite`). |
| `CPK_INTELLIGENCE_API_KEY` or `INTELLIGENCE_API_KEY` | Bearer token OpenBot sends. The two names follow OpenBot: a non-empty `CPK_INTELLIGENCE_API_KEY` wins. |
| `PORT` | HTTP and websocket port. Default `7100`. |
| `HOST` | Bind address. Default `0.0.0.0`. |
| `INTELLIGENCE_SCHEMA` | Postgres schema. Default `intelligence_lite`. |

```sh
DATABASE_URL=postgres://openbot:openbot@127.0.0.1:5432/openbot \
INTELLIGENCE_API_KEY=openbot-dev-intelligence-key \
bun intelligence-lite/src/index.ts
```

Point OpenBot at it:

```
INTELLIGENCE_API_URL=http://127.0.0.1:7100
INTELLIGENCE_GATEWAY_WS_URL=ws://127.0.0.1:7100
INTELLIGENCE_API_KEY=openbot-dev-intelligence-key
```

`openbot-dev-intelligence-key` is the development credential Compose uses when
neither key is set. It is not a production secret. Set a real key before
exposing the port.

`docker compose --profile openbot up` starts Postgres, this service, and the
OpenBot server. The server container is given `http://intelligence-lite:7100`
and `ws://intelligence-lite:7100`. A host process ( `scripts/start.sh` ) uses
the published loopback port. Set `INTELLIGENCE_CONTAINER_API_URL` and
`INTELLIGENCE_CONTAINER_GATEWAY_WS_URL` when the server runs in Compose and
the browser runs on the host; `.env.example` does this.

`GET /healthz` does not require the key.

## What is implemented

- `GET /api/entitlements/runtime` returns `status: "ready"` and an active
  self-hosted entitlement with `features.memory: true`, so OpenBot's boot
  check passes and memory calls are attempted.
- Thread create, get, list, update, archive, and delete.
- `POST /lock`, `PATCH /lock`, `DELETE /lock`. A second run gets `409`
  `THREAD_LOCK_FAILED` with `retryable: true`. Renewal after a terminal event
  returns `status: "completed"`. A lost lock is `409` `THREAD_LOCK_RENEWAL_FAILED`
  with `retryable: false`.
- `POST /connect`, `GET /join-code`, and `POST /api/threads/subscribe` mint a
  single-use `joinToken`.
- Messages projected from stored AG-UI events (`GET .../messages`, including
  `fields=id`, which the client reads from `messages[].id`). A
  `MESSAGES_SNAPSHOT` replaces that transcript, which is how a remote agent
  such as ag-ui-langgraph reports its final messages.
- A runner event is stored only while that run still holds the lock. The check
  and the insert are one transaction. A batch whose events were already stored
  is still acknowledged, so a retry after the lock is released can finish.
- Memories: list, create (exact-text duplicate returns `absorbed: true`),
  update (retires the old row), delete, recall, subscribe.
- `PUT` and `GET /api/channels/files/:id`. The MIT client only defines the GET.
  The PUT stores the raw body so a later GET can hydrate `cpki-asset://` ids.
- `PUT /connector/annotate/:clientEventId`, idempotent on that id.
- Phoenix serializer V2 on `/runner/websocket`, `/client/websocket`, and
  `/channels/websocket`: join, leave, heartbeat, `phx_reply`.
- Runner topic `ingestion:{runId}`. The socket must present the API key as the
  Phoenix auth subprotocol (`base64url.bearer.phx.` + base64, padding stripped).
  The join is accepted only while that run holds the lock, and the reply
  includes `runner_event_batch_v1`. Pushes named `events` or `event` are stored
  and fan out as `ag_ui_event` on `thread:{threadId}`.
- Client topic `thread:{threadId}`, authorized by the single-use `join_token`
  query parameter. Stored events are replayed after the join reply.
- `stop_run` (`{run_id}` or `{runId}`) is acknowledged and relayed to the
  runner as `ag-ui` `{type:"CUSTOM", name:"stop"}`. That event name is
  `AG_UI_CHANNEL_EVENT` in `@copilotkit/shared`. Thread subscribers still
  receive streamed events as `ag_ui_event`.

## What is stubbed

These answers are the ones OpenBot already treats as "this deployment does not
offer the feature", not as "Intelligence is down":

- Learning routes under `/api/v1/learning` return `404` with
  `error.code: "ROUTE_NOT_FOUND"` and `retryable: false`. The runtime client's
  learned-skills read does not know that code, so it falls through to the
  container read, which classifies this as learning switched off.
- `GET /api/inspector/metadata`, `GET /api/inspector/learning`, and
  `/api/_inspect/...` return the same `404` `ROUTE_NOT_FOUND`. The metadata
  read treats `404` as "no inspector".
- `POST /api/trajectories/join` returns `403` `TRAJECTORIES_NOT_ENABLED`.

## Protocol gaps

- Recall is a case-insensitive word overlap over the person's recent memories,
  not hybrid retrieval. Scores are the fraction of query words that appear.
- Memory and thread-list sockets accept `user_meta:memories:{joinCode}` and a
  topic that ends with the thread-list join code, and then send nothing.
  OpenBot reads memory and threads over REST. Live invalidation is not pushed.
- `analyticsContext`, `requestId`, and `X-Cpki-Channel-Delivery-Id` are
  accepted and not stored. Channel delivery does not change who may read the
  thread.
- A runner event without `metadata.cpki_event_id` is stored under a generated
  id. The MIT runner always sets that id.
- Invalid websocket frames are ignored. The test fake of the trajectory gateway
  closes the socket instead. Chat frames are JSON arrays, which is what the
  MIT client sends for V2.
- Managed channel activation (`cpk-{projectId}_...` keys, the channels control
  connection) is not implemented. OpenBot's chat runtime does not open it.
- There is no multi-node fan-out. Sockets and locks on one Postgres are local
  to this process; a second replica would not see the other's websocket
  subscribers. Run one replica.
