-- Core schema for asyncflow. Idempotent: safe to run on every startup.

CREATE TABLE IF NOT EXISTS tasks (
    id               TEXT PRIMARY KEY,
    idempotency_key  TEXT,
    type             TEXT NOT NULL,
    payload          JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority         TEXT NOT NULL,
    status           TEXT NOT NULL,
    max_retries      INTEGER NOT NULL DEFAULT 0,
    attempts         INTEGER NOT NULL DEFAULT 0,
    timeout_seconds  INTEGER NOT NULL DEFAULT 60,
    retry_kind       TEXT NOT NULL DEFAULT 'exponential',
    retry_base       INTEGER NOT NULL DEFAULT 5,
    retry_cron       TEXT NOT NULL DEFAULT '',
    callback_url     TEXT NOT NULL DEFAULT '',
    execute_after    TIMESTAMPTZ,
    worker_id        TEXT NOT NULL DEFAULT '',
    lease_expires_at TIMESTAMPTZ,
    dag_id           TEXT NOT NULL DEFAULT '',
    dag_node_id      TEXT NOT NULL DEFAULT '',
    last_error       TEXT NOT NULL DEFAULT '',
    error_category   TEXT NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS tasks_idem_uniq
    ON tasks(idempotency_key) WHERE idempotency_key <> '';
CREATE INDEX IF NOT EXISTS tasks_status_idx ON tasks(status);
CREATE INDEX IF NOT EXISTS tasks_priority_idx ON tasks(priority);
CREATE INDEX IF NOT EXISTS tasks_type_idx ON tasks(type);
CREATE INDEX IF NOT EXISTS tasks_created_idx ON tasks(created_at);
CREATE INDEX IF NOT EXISTS tasks_dag_idx ON tasks(dag_id);
CREATE INDEX IF NOT EXISTS tasks_dlq_idx ON tasks(status, updated_at) WHERE status = 'dead';

CREATE TABLE IF NOT EXISTS task_attempts (
    id             BIGSERIAL PRIMARY KEY,
    task_id        TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_no     INTEGER NOT NULL,
    worker_id      TEXT NOT NULL DEFAULT '',
    started_at     TIMESTAMPTZ NOT NULL,
    ended_at       TIMESTAMPTZ,
    status         TEXT NOT NULL,
    error          TEXT NOT NULL DEFAULT '',
    error_category TEXT NOT NULL DEFAULT '',
    result         JSONB
);
CREATE INDEX IF NOT EXISTS attempts_task_idx ON task_attempts(task_id, attempt_no);

CREATE TABLE IF NOT EXISTS workers (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    capabilities     TEXT[] NOT NULL DEFAULT '{}',
    total_slots      INTEGER NOT NULL,
    used_slots       INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL DEFAULT 'online',
    last_heartbeat   TIMESTAMPTZ NOT NULL,
    current_tasks    TEXT[] NOT NULL DEFAULT '{}',
    completed_count  BIGINT NOT NULL DEFAULT 0,
    failed_count     BIGINT NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dags (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    failure_policy TEXT NOT NULL,
    status         TEXT NOT NULL,
    definition     JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS dag_nodes (
    dag_id        TEXT NOT NULL REFERENCES dags(id) ON DELETE CASCADE,
    node_id       TEXT NOT NULL,
    task_id       TEXT NOT NULL DEFAULT '',
    state         TEXT NOT NULL,
    dependencies  TEXT[] NOT NULL DEFAULT '{}',
    attempts      INTEGER NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dag_id, node_id)
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id          BIGSERIAL PRIMARY KEY,
    entity      TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    action      TEXT NOT NULL,
    from_state  TEXT NOT NULL DEFAULT '',
    to_state    TEXT NOT NULL DEFAULT '',
    actor       TEXT NOT NULL DEFAULT '',
    detail      TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS audit_entity_idx ON audit_logs(entity, entity_id);
CREATE INDEX IF NOT EXISTS audit_created_idx ON audit_logs(created_at);

CREATE TABLE IF NOT EXISTS metrics_snapshots (
    id           BIGSERIAL PRIMARY KEY,
    taken_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    queue_depths JSONB NOT NULL,
    dlq_count    INTEGER NOT NULL DEFAULT 0,
    running      INTEGER NOT NULL DEFAULT 0,
    online       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS metrics_snap_time_idx ON metrics_snapshots(taken_at);
