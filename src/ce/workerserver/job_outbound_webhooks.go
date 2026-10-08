package jobs

import (
	"context"

	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deploy/deployhooks"
)

// DeliverOutboundWebhooks processes due deployment completion outbound webhook
// deliveries: retryable failures (network errors, timeouts, 429 and 5xx) are
// retried on their exponential backoff schedule, and records stuck in the
// sending state after a process crash or restart are safely reclaimed.
func DeliverOutboundWebhooks(ctx context.Context) error {
	return deployhooks.ProcessDueDeliveries(ctx)
}
