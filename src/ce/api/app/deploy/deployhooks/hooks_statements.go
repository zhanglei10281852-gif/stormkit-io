package deployhooks

type statement struct {
	appDetailsForHooks string

	insertOutboundWebhookDelivery             string
	claimDueOutboundWebhookDeliveries         string
	markOutboundWebhookDeliverySucceeded      string
	markOutboundWebhookDeliveryRetry          string
	markOutboundWebhookDeliveryFailed         string
	selectOutboundWebhookDeliveries           string
	selectOutboundWebhookDeliveryByDeployment string
}

var stmt = &statement{
	appDetailsForHooks: `
		SELECT
			d.app_id, COALESCE(d.is_auto_deploy, false),
			COALESCE(d.pull_request_number, 0),
			COALESCE(app.repo, ''), app.display_name,
			app.user_id, COALESCE(d.auto_publish, FALSE)
		FROM deployments d
		INNER JOIN apps app ON app.app_id = d.app_id
		WHERE d.deployment_id = $1;
	`,

	insertOutboundWebhookDelivery: `
		INSERT INTO deployment_outbound_webhook_deliveries (
			app_id, deployment_id, wh_id, trigger_when,
			request_url, request_method, request_headers, request_body,
			status, max_attempts, next_attempt_at
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8,
			'pending', $9, NOW()
		)
		ON CONFLICT (deployment_id, wh_id) DO NOTHING;
	`,

	// Claims rows that are due, as well as sending rows whose lock went stale
	// after a crash or restart. The attempts value returned with each row is
	// used as an optimistic concurrency token when the attempt is acknowledged,
	// so a late response from a reclaimed attempt cannot overwrite a newer one.
	claimDueOutboundWebhookDeliveries: `
		WITH due AS (
			SELECT delivery_id
			FROM deployment_outbound_webhook_deliveries
			WHERE
				(status = 'pending' AND next_attempt_at <= NOW())
				OR (status = 'sending' AND locked_at < $2)
			ORDER BY next_attempt_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE deployment_outbound_webhook_deliveries d
		SET
			status = 'sending',
			locked_at = NOW(),
			attempts = d.attempts + 1,
			last_attempt_at = NOW(),
			updated_at = NOW()
		FROM due
		WHERE d.delivery_id = due.delivery_id
		RETURNING
			d.delivery_id, d.app_id, d.deployment_id, d.wh_id, d.trigger_when,
			d.request_url, d.request_method, d.request_headers, d.request_body,
			d.status, d.attempts, d.max_attempts,
			d.last_error, d.response_status, d.response_body,
			d.next_attempt_at, d.locked_at, d.last_attempt_at, d.succeeded_at,
			d.created_at, d.updated_at;
	`,

	markOutboundWebhookDeliverySucceeded: `
		UPDATE deployment_outbound_webhook_deliveries
		SET
			status = 'succeeded',
			response_status = $3,
			response_body = $4,
			last_error = NULL,
			succeeded_at = NOW(),
			updated_at = NOW()
		WHERE delivery_id = $1 AND status = 'sending' AND attempts = $2;
	`,

	markOutboundWebhookDeliveryRetry: `
		UPDATE deployment_outbound_webhook_deliveries
		SET
			status = 'pending',
			last_error = $3,
			response_status = $4,
			response_body = $5,
			next_attempt_at = $6,
			updated_at = NOW()
		WHERE delivery_id = $1 AND status = 'sending' AND attempts = $2;
	`,

	markOutboundWebhookDeliveryFailed: `
		UPDATE deployment_outbound_webhook_deliveries
		SET
			status = 'failed',
			last_error = $3,
			response_status = $4,
			response_body = $5,
			updated_at = NOW()
		WHERE delivery_id = $1 AND status = 'sending' AND attempts = $2;
	`,

	selectOutboundWebhookDeliveries: `
		SELECT
			delivery_id, app_id, deployment_id, wh_id, trigger_when,
			request_url, request_method, request_headers, request_body,
			status, attempts, max_attempts,
			last_error, response_status, response_body,
			next_attempt_at, locked_at, last_attempt_at, succeeded_at,
			created_at, updated_at
		FROM deployment_outbound_webhook_deliveries
		WHERE deployment_id = $1
		ORDER BY delivery_id ASC;
	`,

	selectOutboundWebhookDeliveryByDeployment: `
		SELECT
			delivery_id, app_id, deployment_id, wh_id, trigger_when,
			request_url, request_method, request_headers, request_body,
			status, attempts, max_attempts,
			last_error, response_status, response_body,
			next_attempt_at, locked_at, last_attempt_at, succeeded_at,
			created_at, updated_at
		FROM deployment_outbound_webhook_deliveries
		WHERE deployment_id = $1 AND wh_id = $2;
	`,
}
