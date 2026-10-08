package webhookdelivery

var (
	tableDeliveries = "webhook_deliveries"
	tableResults    = "webhook_delivery_results"
)

type statement struct {
	upsertDelivery    string
	selectDelivery    string
	selectResultByKey string
	claimResult       string
	reclaimResult     string
	markSucceeded     string
	markRetryable     string
	dueResults        string
}

// resultColumns lists every persisted Result field, in the order scanResult
// expects them.
const resultColumns = `
	result_id, delivery_id, app_id, env_id, COALESCE(env_name, ''),
	status, COALESCE(deployment_id, 0), attempts,
	last_error, claimed_at, finished_at, next_retry_at,
	created_at, updated_at
`

var stmt = &statement{
	// First write wins for the event itself: a redelivery never rewrites the
	// original payload or normalized fields; only updated_at is touched.
	upsertDelivery: `
		INSERT INTO webhook_deliveries (
			provider, provider_delivery_id, repo, checkout_repo, branch, message,
			event_type, commit_sha, pull_request_number, is_fork, changes_complete,
			changed_files, payload
		)
		VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11,
			$12, $13
		)
		ON CONFLICT (provider, provider_delivery_id)
		DO UPDATE SET updated_at = NOW() AT TIME ZONE 'UTC'
		RETURNING delivery_id, created_at, updated_at;
	`,

	selectDelivery: `
		SELECT
			delivery_id, provider, provider_delivery_id,
			COALESCE(repo, ''), COALESCE(checkout_repo, ''), COALESCE(branch, ''),
			COALESCE(message, ''), COALESCE(event_type, ''), COALESCE(commit_sha, ''),
			COALESCE(pull_request_number, 0), COALESCE(is_fork, false),
			COALESCE(changes_complete, false), COALESCE(changed_files, '{}'),
			COALESCE(payload, '{}'::jsonb), created_at, updated_at
		FROM webhook_deliveries
		WHERE delivery_id = $1;
	`,

	selectResultByKey: `
		SELECT ` + resultColumns + `
		FROM webhook_delivery_results
		WHERE delivery_id = $1 AND app_id = $2 AND env_id = $3;
	`,

	// The unique index turns concurrent claims into one insert: the loser of
	// the race gets no row back and has to read the existing claim.
	claimResult: `
		INSERT INTO webhook_delivery_results (
			delivery_id, app_id, env_id, env_name, status, claimed_at, attempts
		)
		VALUES ($1, $2, $3, $4, 'processing', NOW() AT TIME ZONE 'UTC', 0)
		ON CONFLICT (delivery_id, app_id, env_id) DO NOTHING
		RETURNING ` + resultColumns + `;
	`,

	// A claim can only be taken over while it is still retryable or while its
	// processing lease has expired. Succeeded rows and freshly held processing
	// rows are untouchable.
	reclaimResult: `
		UPDATE webhook_delivery_results
		SET
			status = 'processing',
			claimed_at = NOW() AT TIME ZONE 'UTC',
			attempts = attempts + 1,
			last_error = NULL,
			next_retry_at = NULL,
			updated_at = NOW() AT TIME ZONE 'UTC'
		WHERE
			result_id = $1
			AND (
				status = 'retryable'
				OR (status = 'processing' AND claimed_at < $2)
			)
		RETURNING ` + resultColumns + `;
	`,

	// Only the current processing owner may close the claim, so a late
	// callback from an expired lease cannot overwrite a newer attempt.
	markSucceeded: `
		UPDATE webhook_delivery_results
		SET
			status = 'succeeded',
			deployment_id = CASE WHEN $2 = 0 THEN NULL ELSE $2 END,
			last_error = NULL,
			next_retry_at = NULL,
			finished_at = NOW() AT TIME ZONE 'UTC',
			updated_at = NOW() AT TIME ZONE 'UTC'
		WHERE result_id = $1 AND status = 'processing';
	`,

	// A zero deployment id means no deployment was created on this attempt;
	// keep whatever a previous attempt may have linked.
	markRetryable: `
		UPDATE webhook_delivery_results
		SET
			status = 'retryable',
			deployment_id = CASE WHEN $2 = 0 THEN deployment_id ELSE $2 END,
			last_error = $3,
			next_retry_at = $4,
			finished_at = NULL,
			updated_at = NOW() AT TIME ZONE 'UTC'
		WHERE result_id = $1 AND status <> 'succeeded';
	`,

	// Rows are locked skip-already-locked while the statement runs; because the
	// store autocommits, that lock does not outlive the SELECT. Ownership is
	// really taken by reclaimResult's conditional UPDATE, which is what
	// prevents two workers from resuming the same claim.
	dueResults: `
		SELECT ` + resultColumns + `
		FROM webhook_delivery_results
		WHERE
			(status = 'processing' AND claimed_at < $1)
			OR (
				status = 'retryable'
				AND next_retry_at IS NOT NULL
				AND next_retry_at <= $2
				AND attempts < $3
			)
		ORDER BY claimed_at
		LIMIT $4
		FOR UPDATE SKIP LOCKED;
	`,
}
