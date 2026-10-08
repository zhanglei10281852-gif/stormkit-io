package app

import (
	"time"

	"github.com/stormkit-io/stormkit-io/src/lib/types"
	null "gopkg.in/guregu/null.v3"
)

// Delivery lifecycle states.
const (
	// DeliveryStatusPending waits for its first or its next scheduled attempt.
	DeliveryStatusPending = "pending"

	// DeliveryStatusSending was claimed by a worker and is in flight.
	// Rows left in this state past the stale threshold are safely reclaimed.
	DeliveryStatusSending = "sending"

	// DeliveryStatusSucceeded is a terminal state reached on a 2xx response.
	DeliveryStatusSucceeded = "succeeded"

	// DeliveryStatusFailed is a terminal state reached on a permanent rejection
	// (e.g. 4xx other than 429) or after the attempt budget is exhausted.
	DeliveryStatusFailed = "failed"
)

const (
	// DeliveryMaxAttempts is the default attempt budget for one frozen event.
	DeliveryMaxAttempts = 12

	// DeliveryIDHeader carries the stable delivery identifier on every attempt,
	// including retries. Receivers can use it as an idempotency key so a retried
	// completion event never causes a second external side effect.
	DeliveryIDHeader = "X-Stormkit-Delivery-Id"
)

// OutboundWebhookDelivery is a frozen, persistently tracked delivery of a
// deployment completion outbound webhook (on_deploy_success /
// on_deploy_failed). The request URL, method, headers and rendered body are
// snapshots taken when the completion event is produced, so later edits to the
// webhook configuration never change what a retry sends.
type OutboundWebhookDelivery struct {
	DeliveryID     types.ID
	AppID          types.ID
	DeploymentID   types.ID
	WebhookID      types.ID
	TriggerWhen    string
	RequestURL     string
	RequestMethod  string
	RequestHeaders map[string]string
	RequestBody    string
	Status         string
	Attempts       int
	MaxAttempts    int
	LastError      null.String
	ResponseStatus null.Int
	ResponseBody   null.String
	NextAttemptAt  time.Time
	LockedAt       null.Time
	LastAttemptAt  null.Time
	SucceededAt    null.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// NewOutboundWebhookDelivery freezes the webhook configuration and renders the
// payload for a deployment completion event. The rendered body (including the
// expanded $SK_* variables) is frozen as well, so retries send exactly what
// the first attempt sent.
func NewOutboundWebhookDelivery(wh OutboundWebhook, settings OutboundWebhookSettings) *OutboundWebhookDelivery {
	headers := make(map[string]string, len(wh.RequestHeaders))

	for key, value := range wh.RequestHeaders {
		headers[key] = value
	}

	return &OutboundWebhookDelivery{
		AppID:          settings.AppID,
		DeploymentID:   settings.DeploymentID,
		WebhookID:      wh.WebhookID,
		TriggerWhen:    wh.TriggerWhen,
		RequestURL:     wh.RequestURL,
		RequestMethod:  wh.RequestMethod,
		RequestHeaders: headers,
		RequestBody:    wh.RenderPayload(settings),
		Status:         DeliveryStatusPending,
		MaxAttempts:    DeliveryMaxAttempts,
	}
}
