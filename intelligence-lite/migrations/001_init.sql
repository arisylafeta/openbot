CREATE TABLE IF NOT EXISTS schema_migrations (
  id text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS threads (
  id text PRIMARY KEY,
  user_id text NOT NULL,
  agent_id text NOT NULL,
  name text,
  archived boolean NOT NULL DEFAULT false,
  learning_container_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_run_at timestamptz
);

CREATE INDEX IF NOT EXISTS threads_owner_idx
  ON threads (user_id, agent_id, updated_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS events (
  id bigserial PRIMARY KEY,
  thread_id text NOT NULL REFERENCES threads (id) ON DELETE CASCADE,
  run_id text NOT NULL,
  event_id text NOT NULL,
  seq integer NOT NULL DEFAULT 0,
  type text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (thread_id, event_id)
);

CREATE INDEX IF NOT EXISTS events_thread_idx ON events (thread_id, id);

CREATE TABLE IF NOT EXISTS messages (
  thread_id text NOT NULL REFERENCES threads (id) ON DELETE CASCADE,
  id text NOT NULL,
  role text NOT NULL,
  content jsonb,
  activity_type text,
  tool_calls jsonb,
  tool_call_id text,
  position integer NOT NULL,
  PRIMARY KEY (thread_id, id)
);

CREATE TABLE IF NOT EXISTS locks (
  thread_id text PRIMARY KEY REFERENCES threads (id) ON DELETE CASCADE,
  run_id text NOT NULL,
  user_id text NOT NULL,
  agent_id text NOT NULL,
  lock_key_prefix text,
  expires_at timestamptz NOT NULL,
  completed boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS join_tokens (
  token text PRIMARY KEY,
  kind text NOT NULL,
  thread_id text,
  user_id text,
  run_id text,
  expires_at timestamptz NOT NULL,
  used_at timestamptz
);

CREATE TABLE IF NOT EXISTS join_codes (
  user_id text NOT NULL,
  kind text NOT NULL,
  code text NOT NULL,
  PRIMARY KEY (user_id, kind)
);

CREATE TABLE IF NOT EXISTS memories (
  id text PRIMARY KEY,
  user_id text NOT NULL,
  scope text NOT NULL,
  kind text NOT NULL,
  content text NOT NULL,
  source_thread_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  invalidated_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS memories_user_idx
  ON memories (user_id, scope, created_at DESC);

CREATE TABLE IF NOT EXISTS files (
  id text PRIMARY KEY,
  bytes bytea NOT NULL,
  mime_type text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS annotations (
  client_event_id text PRIMARY KEY,
  id text NOT NULL,
  body jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
