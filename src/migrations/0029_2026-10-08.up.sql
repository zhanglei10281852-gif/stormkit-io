-- Frozen, per-deployment/per-trigger delivery records for the deployment
-- completion outbound webhooks (on_deploy_success / on_deploy_failed).
--
-- Before this table existed the completion event was dispatched synchronously
-- once and forgotten: a network timeout or a process restart meant the external
-- system never learned the outcome, and a failed "save result" step could send
-- the same event twice on retry. Each row freezes the request URL, method,
-- headers and the already-rendered body at event creation time, so later edits
-- to the webhook configuration never change what a retry sends, and tracks the
-- delivery state machine (pending -> sending -> succeeded | failed) with
-- attempt counts and backoff scheduling.
CREATE TABLE IF NOT EXISTS deployment_outbound_webhook_deliveries (
    delivery_id     bigserial PRIMARY KEY,
    app_id          bigint NOT NULL,
    deployment_id   bigint NOT NULL,
    wh_id           integer NOT NULL,
    trigger_when    text NOT NULL,
    request_url     text NOT NULL,
    request_method  text NOT NULL,
    request_headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    request_body    text NOT NULL DEFAULT '',
    -- pending: waiting for (the next) attempt
    -- sending: claimed by a worker; reclaimed when locked_at goes stale
    -- succeeded: terminal, 2xx acknowledged
    -- failed: terminal, permanent non-retryable rejection (e.g. 4xx) or exhausted
    status          text NOT NULL DEFAULT 'pending',
    attempts        integer NOT NULL DEFAULT 0,
    max_attempts    integer NOT NULL DEFAULT 12,
    last_error      text,
    response_status integer,
    response_body   text,
    next_attempt_at timestamptz NOT NULL DEFAULT NOW(),
    locked_at       timestamptz,
    last_attempt_at timestamptz,
    succeeded_at    timestamptz,
    created_at      timestamptz NOT NULL DEFAULT NOW(),
    updated_at      timestamptz NOT NULL DEFAULT NOW()
);

-- Idempotency boundary: a single frozen event per deployment and trigger, no
-- matter how often the completion path runs.
CREATE UNIQUE INDEX IF NOT EXISTS idx_dowd_deployment_wh_unique
    ON deployment_outbound_webhook_deliveries (deployment_id, wh_id);

-- Worker's due-queue lookup: due pending rows plus stale sending rows left
-- behind by a crashed or restarted process.
CREATE INDEX IF NOT EXISTS idx_dowd_status_next_attempt
    ON deployment_outbound_webhook_deliveries (status, next_attempt_at);
