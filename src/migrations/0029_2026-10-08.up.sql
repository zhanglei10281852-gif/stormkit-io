-- Volume file lifecycle operations.
--
-- Uploads (including same-name replacements) and deletes cross two systems
-- that cannot share a transaction: the object backend (filesystem/S3) and the
-- volumes table. Writing the object first and upserting the row afterwards
-- leaves a window in which list/download/public-file/size disagree about the
-- current version, a failed commit orphans the new object, and a crash leaves
-- no record of what was in flight.
--
-- Every upload or delete therefore registers its operation here before
-- touching the backend. The row carries the exact object locator the backend
-- wrote (or is about to delete), so that after a restart a reconciliation job
-- can reach a deterministic outcome: finish the pending object removal, or
-- abort the interrupted upload. An object is only ever deleted once it is
-- known to be unreferenced by a live volumes row and by another pending
-- operation, which is what keeps a still-published object safe.
CREATE TABLE IF NOT EXISTS volumes_ops (
    op_id       bigserial PRIMARY KEY,
    env_id      bigint NOT NULL REFERENCES apps_build_conf(env_id) ON DELETE CASCADE,
    op_type     text NOT NULL CHECK (op_type IN ('upload', 'delete')),
    status      text NOT NULL CHECK (status IN ('pending', 'committed', 'failed')),
    file_id     bigint,
    file_name   text,
    file_size   bigint NOT NULL DEFAULT 0,
    mount_type  text NOT NULL,
    -- Object locator, shaped like a volumes row's (file_path, file_name) pair
    -- so the exact same backend code can download or remove it:
    --   {"path": "<absolute dir or bucket/key-dir>", "name": "<logical name>"}
    object      jsonb NOT NULL,
    -- Set when a committed replacement displaced an older object. The object
    -- is removed after commit (and retried by reconciliation); until then the
    -- old bytes remain recoverable.
    replaced    jsonb,
    error       text,
    created_at  timestamp without time zone NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'::text),
    updated_at  timestamp without time zone
);

CREATE INDEX IF NOT EXISTS idx_volumes_ops_status ON volumes_ops (status);
CREATE INDEX IF NOT EXISTS idx_volumes_ops_env_id ON volumes_ops (env_id);
