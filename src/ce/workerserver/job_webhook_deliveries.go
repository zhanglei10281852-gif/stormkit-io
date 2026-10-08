package jobs

import (
	"context"

	"github.com/stormkit-io/stormkit-io/src/ce/api/app/apphandlers"
)

// RecoverWebhookDeliveries resumes inbound deploy webhook claims that never
// finished: processing claims whose lease expired after a crash and retryable
// claims whose backoff is due. It runs on the leader, but every transition is
// claim-gated so duplicate runs stay safe.
func RecoverWebhookDeliveries(ctx context.Context) error {
	return apphandlers.RecoverPendingWebhookDeliveries(ctx)
}
