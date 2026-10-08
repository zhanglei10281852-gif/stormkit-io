-- Persistent inbox for inbound deploy webhooks that carry a provider delivery
-- identity (GitHub X-GitHub-Delivery, GitLab X-Gitlab-Event-UUID and Bitbucket
-- X-Request-UUID). Duplicate deliveries and concurrent requests are claimed
-- exactly once per matched application/environment, and a crash between the
-- claim and the queue dispatch is recoverable from these rows.
--
-- webhook_deliveries stores the original event, keyed by the provider plus its
-- delivery id: the same GUID from two different providers is two events.
-- First write wins for the payload, so a redelivery never rewrites history.
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    delivery_id           BIGSERIAL PRIMARY KEY,
    provider              TEXT NOT NULL,
    provider_delivery_id  TEXT NOT NULL,
    repo                  TEXT,
    checkout_repo         TEXT,
    branch                TEXT,
    message               TEXT,
    event_type            TEXT,
    commit_sha            TEXT,
    pull_request_number   BIGINT,
    is_fork               BOOLEAN,
    changes_complete      BOOLEAN,
    changed_files         TEXT[],
    payload               JSONB,
    created_at            TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC') NOT NULL,
    updated_at            TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC') NOT NULL,
    CONSTRAINT webhook_deliveries_provider_delivery_key UNIQUE (provider, provider_delivery_id)
);

-- One row per matched application/environment of a delivery. The same commit
-- legitimately reaches several environments, so results are never merged: the
-- unique key is the delivery together with the app and the environment.
--
-- status is one of:
--   processing - claimed, deployment creation/dispatch in flight (claimed_at is the lease)
--   succeeded  - deployment created and dispatched (deployment_id is set)
--   retryable  - a recoverable failure; next_retry_at says when background retry is due
CREATE TABLE IF NOT EXISTS webhook_delivery_results (
    result_id      BIGSERIAL PRIMARY KEY,
    delivery_id    BIGINT NOT NULL REFERENCES webhook_deliveries(delivery_id) ON DELETE CASCADE,
    app_id         BIGINT NOT NULL,
    env_id         BIGINT NOT NULL,
    env_name       TEXT,
    status         TEXT NOT NULL,
    deployment_id  BIGINT REFERENCES deployments(deployment_id) ON DELETE SET NULL,
    attempts       INTEGER NOT NULL DEFAULT 0,
    last_error     TEXT,
    claimed_at     TIMESTAMP WITHOUT TIME ZONE,
    finished_at    TIMESTAMP WITHOUT TIME ZONE,
    next_retry_at  TIMESTAMP WITHOUT TIME ZONE,
    created_at     TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC') NOT NULL,
    updated_at     TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC') NOT NULL,
    CONSTRAINT webhook_delivery_results_delivery_app_env_key UNIQUE (delivery_id, app_id, env_id)
);

-- Recovery scans for expired leases and due retries.
CREATE INDEX IF NOT EXISTS idx_webhook_delivery_results_status_retry
    ON webhook_delivery_results (status, next_retry_at);

-- Links an auto deployment back to the claim that created it. The partial
-- unique index is the database-level guarantee that one claim never produces
-- two live deployments, even when the process dies between the insert and the
-- queue dispatch and recovery has to decide whether a deployment already
-- exists. Soft-deleted deployments do not block the claim from deploying again.
ALTER TABLE deployments
    ADD COLUMN IF NOT EXISTS delivery_result_id BIGINT
    REFERENCES webhook_delivery_results(result_id) ON DELETE SET NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_deployments_delivery_result_unique
    ON deployments (delivery_result_id)
    WHERE delivery_result_id IS NOT NULL AND deleted_at IS NULL;
