package webhookdelivery

import (
	"context"
	"database/sql"
	"time"

	"github.com/lib/pq"

	"github.com/stormkit-io/stormkit-io/src/lib/database"
	"github.com/stormkit-io/stormkit-io/src/lib/types"
)

// Store persists inbound webhook deliveries and their per-environment results.
type Store struct {
	*database.Store
}

// NewStore returns a store instance.
func NewStore() *Store {
	return &Store{database.NewStore()}
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanDelivery(row rowScanner) (*Delivery, error) {
	d := &Delivery{}

	err := row.Scan(
		&d.ID, &d.Provider, &d.ProviderDelivery,
		&d.Repo, &d.CheckoutRepo, &d.Branch, &d.Message,
		&d.EventType, &d.CommitSha, &d.PullRequestNumber,
		&d.IsFork, &d.ChangesComplete, pq.Array(&d.ChangedFiles),
		&d.Payload, &d.CreatedAt, &d.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return d, nil
}

func scanResult(row rowScanner) (*Result, error) {
	r := &Result{}

	err := row.Scan(
		&r.ID, &r.DeliveryID, &r.AppID, &r.EnvID, &r.EnvName,
		&r.Status, &r.DeploymentID, &r.Attempts,
		&r.LastError, &r.ClaimedAt, &r.FinishedAt, &r.NextRetryAt,
		&r.CreatedAt, &r.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return r, nil
}

// UpsertDelivery stores the event on its first receipt and returns its id.
// Redeliveries keep the original fields and only touch updated_at.
func (s *Store) UpsertDelivery(ctx context.Context, d *Delivery) (types.ID, error) {
	var payload any

	if len(d.Payload) > 0 {
		payload = []byte(d.Payload)
	}

	row, err := s.QueryRow(ctx, stmt.upsertDelivery,
		d.Provider, d.ProviderDelivery, d.Repo, d.CheckoutRepo, d.Branch, d.Message,
		d.EventType, d.CommitSha, d.PullRequestNumber, d.IsFork, d.ChangesComplete,
		pq.Array(d.ChangedFiles), payload,
	)

	if err != nil {
		return 0, err
	}

	err = row.Scan(&d.ID, &d.CreatedAt, &d.UpdatedAt)

	if err != nil {
		return 0, err
	}

	return d.ID, nil
}

// DeliveryByID returns the stored event, or nil when it does not exist.
func (s *Store) DeliveryByID(ctx context.Context, id types.ID) (*Delivery, error) {
	row, err := s.QueryRow(ctx, stmt.selectDelivery, id)

	if err != nil {
		return nil, err
	}

	d, err := scanDelivery(row)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	return d, err
}

// ResultByKey returns the result row for a delivery/app/environment triple, or
// nil when it has not been claimed yet.
func (s *Store) ResultByKey(ctx context.Context, deliveryID, appID, envID types.ID) (*Result, error) {
	row, err := s.QueryRow(ctx, stmt.selectResultByKey, deliveryID, appID, envID)

	if err != nil {
		return nil, err
	}

	r, err := scanResult(row)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	return r, err
}

// ClaimResult inserts a processing result for a delivery/app/environment. The
// boolean is true when this call created the claim and therefore owns it; a
// false result carries the pre-existing row untouched, so the caller can tell
// succeeded, in-flight and retryable claims apart.
func (s *Store) ClaimResult(ctx context.Context, deliveryID, appID, envID types.ID, envName string) (*Result, bool, error) {
	row, err := s.QueryRow(ctx, stmt.claimResult, deliveryID, appID, envID, envName)

	if err != nil {
		return nil, false, err
	}

	r, err := scanResult(row)

	if err == nil {
		return r, true, nil
	}

	if err != sql.ErrNoRows {
		return nil, false, err
	}

	// The unique index rejected the insert: read back the row that won.
	existing, err := s.ResultByKey(ctx, deliveryID, appID, envID)

	if err != nil {
		return nil, false, err
	}

	return existing, false, nil
}

// ReclaimResult takes over a retryable claim or a processing claim whose lease
// expired before leaseExpiry. It returns false when another processor still
// owns the row or the claim already succeeded.
func (s *Store) ReclaimResult(ctx context.Context, resultID types.ID, leaseExpiry time.Time) (bool, *Result, error) {
	row, err := s.QueryRow(ctx, stmt.reclaimResult, resultID, leaseExpiry)

	if err != nil {
		return false, nil, err
	}

	r, err := scanResult(row)

	if err == sql.ErrNoRows {
		return false, nil, nil
	}

	if err != nil {
		return false, nil, err
	}

	return true, r, nil
}

// MarkSucceeded closes a claim with the deployment it produced.
func (s *Store) MarkSucceeded(ctx context.Context, resultID, deploymentID types.ID) error {
	_, err := s.Exec(ctx, stmt.markSucceeded, resultID, deploymentID)
	return err
}

// MarkRetryable records a recoverable failure and the earliest time the
// background job is allowed to try again. A zero deploymentID preserves a link
// established by an earlier attempt.
func (s *Store) MarkRetryable(ctx context.Context, resultID, deploymentID types.ID, errMessage string, nextRetryAt *time.Time) error {
	var nextRetry any

	if nextRetryAt != nil {
		nextRetry = *nextRetryAt
	}

	_, err := s.Exec(ctx, stmt.markRetryable, resultID, deploymentID, errMessage, nextRetry)
	return err
}

// DueResults returns claims the recovery job should take over: processing
// rows whose lease expired before leaseExpiry and retryable rows whose backoff
// is due at now, up to limit. Rows are locked skip-already-locked so multiple
// workers never pick the same claim.
func (s *Store) DueResults(ctx context.Context, leaseExpiry, now time.Time, maxAttempts, limit int) ([]*Result, error) {
	rows, err := s.Query(ctx, stmt.dueResults, leaseExpiry, now, maxAttempts, limit)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	results := []*Result{}

	for rows.Next() {
		r, err := scanResult(rows)

		if err != nil {
			return nil, err
		}

		results = append(results, r)
	}

	return results, rows.Err()
}
