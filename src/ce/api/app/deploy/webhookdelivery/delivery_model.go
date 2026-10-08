// Package webhookdelivery persists the claim state of inbound deploy webhooks
// that carry a provider delivery identity. One webhook_deliveries row stores
// the original event, and one webhook_delivery_results row stores the outcome
// for each application/environment the event matched. The rows are the only
// source of truth for deduplication: a duplicate delivery or a process restart
// after the claim can be resolved without starting a second deployment.
package webhookdelivery

import (
	"encoding/json"
	"time"

	"gopkg.in/guregu/null.v3"

	"github.com/stormkit-io/stormkit-io/src/lib/types"
)

const (
	ProviderGitHub    = "github"
	ProviderGitLab    = "gitlab"
	ProviderBitbucket = "bitbucket"
)

// Result statuses.
const (
	// StatusProcessing means the result row is claimed and the deployment is
	// being created and dispatched. ClaimedAt is the lease: once it expires
	// another delivery or the background job may reclaim the row.
	StatusProcessing = "processing"

	// StatusSucceeded means the deployment was created and dispatched exactly
	// once. Redeliveries become no-ops.
	StatusSucceeded = "succeeded"

	// StatusRetryable means a recoverable failure happened while creating or
	// dispatching the deployment. NextRetryAt says when the background job is
	// allowed to try again; a provider redelivery reclaims immediately.
	StatusRetryable = "retryable"
)

// Delivery is the original provider event, keyed by provider and the
// provider's delivery GUID.
type Delivery struct {
	ID                types.ID
	Provider          string
	ProviderDelivery  string
	Repo              string
	CheckoutRepo      string
	Branch            string
	Message           string
	EventType         string
	CommitSha         string
	PullRequestNumber int64
	IsFork            bool
	ChangesComplete   bool
	ChangedFiles      []string
	Payload           json.RawMessage

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Result is the per application/environment outcome of a delivery.
type Result struct {
	ID         types.ID
	DeliveryID types.ID
	AppID      types.ID
	EnvID      types.ID
	EnvName    string

	Status string

	// DeploymentID links the deployment the claim created. It stays zero
	// until the insert succeeds.
	DeploymentID types.ID

	Attempts   int
	LastError  null.String
	ClaimedAt  null.Time
	FinishedAt null.Time
	NextRetryAt null.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}
