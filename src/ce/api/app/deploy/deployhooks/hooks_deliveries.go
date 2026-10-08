package deployhooks

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/stormkit-io/stormkit-io/src/ce/api/app"
	"github.com/stormkit-io/stormkit-io/src/lib/shttp"
	"github.com/stormkit-io/stormkit-io/src/lib/slog"
	"github.com/stormkit-io/stormkit-io/src/lib/types"
	null "gopkg.in/guregu/null.v3"
)

const (
	// deliveryClaimBatchSize bounds how many rows one processing pass claims.
	deliveryClaimBatchSize = 50

	// deliveryRequestTimeout bounds a single HTTP attempt so a hanging target
	// cannot hold a delivery row in the sending state indefinitely.
	deliveryRequestTimeout = 15 * time.Second

	// deliveryStaleThreshold is how long a row may stay in the sending state
	// before another worker assumes the previous process crashed or restarted
	// mid-flight and safely reclaims it.
	deliveryStaleThreshold = 5 * time.Minute

	// deliveryBackoffBase/deliveryBackoffMax define the exponential backoff
	// used for retryable failures (network errors, timeouts, 429, 5xx).
	deliveryBackoffBase = 30 * time.Second
	deliveryBackoffMax  = 30 * time.Minute

	// deliveryMaxBodyLength keeps response bodies bounded when persisted.
	deliveryMaxBodyLength = 4096
)

// deliveryInFlight ensures a single background processing loop runs per
// process, even when several deployments complete at the same time.
var deliveryInFlight atomic.Bool

// TriggerDeliveries kicks off asynchronous processing of due deliveries. It
// never blocks the caller: deployment status transitions, status checks, pull
// request previews and Discord notifications proceed even when a webhook
// target is slow or unreachable. It is a variable so tests can replace it
// with a synchronous implementation.
var TriggerDeliveries = func() {
	if !deliveryInFlight.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer deliveryInFlight.Store(false)

		if err := ProcessDueDeliveries(context.Background()); err != nil {
			slog.Errorf("error while processing outbound webhook deliveries: %s", err.Error())
		}
	}()
}

// ProcessDueDeliveries claims due (or stale) deliveries and performs their HTTP
// attempt, persisting the outcome of each one. It returns once there are no
// more due deliveries. It is invoked both just after events are enqueued and by
// the periodic worker job, which is also the recovery path after timeouts and
// process restarts.
func ProcessDueDeliveries(ctx context.Context) error {
	store := NewStore()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		deliveries, err := store.ClaimDueOutboundWebhookDeliveries(ctx, deliveryClaimBatchSize, deliveryStaleThreshold)

		if err != nil {
			return err
		}

		for i := range deliveries {
			deliver(ctx, store, deliveries[i])
		}

		if len(deliveries) < deliveryClaimBatchSize {
			return nil
		}
	}
}

// deliveryAttemptResult is the classified outcome of one HTTP attempt.
type deliveryAttemptResult struct {
	// status is the received HTTP status code, or zero when no response was
	// received (network error, timeout or unreadable response).
	status int

	// body is the truncated response body.
	body string

	// error is the transport error message, empty when a response arrived.
	error string

	// retryAfter is the raw Retry-After header of a 429 response.
	retryAfter string
}

// deliver performs a single attempt and advances the delivery state machine.
func deliver(ctx context.Context, store *Store, d app.OutboundWebhookDelivery) {
	result := sendDelivery(d)

	responseStatus := null.NewInt(0, false)
	responseBody := null.NewString("", false)

	if result.status != 0 {
		responseStatus = null.IntFrom(int64(result.status))
		responseBody = null.StringFrom(result.body)
	}

	switch {
	case result.status >= 200 && result.status < 300:
		if err := store.MarkOutboundWebhookDeliverySucceeded(ctx, d.DeliveryID, d.Attempts, result.status, result.body); err != nil {
			slog.Errorf("error while marking webhook delivery %d succeeded: %s", d.DeliveryID, err.Error())
		}

	case isPermanentDeliveryError(result):
		reason := fmt.Sprintf("delivery failed permanently with HTTP status %d", result.status)

		if result.body != "" {
			reason = fmt.Sprintf("%s: %s", reason, result.body)
		}

		if err := store.MarkOutboundWebhookDeliveryFailed(ctx, d.DeliveryID, d.Attempts, reason, responseStatus, responseBody); err != nil {
			slog.Errorf("error while marking webhook delivery %d failed: %s", d.DeliveryID, err.Error())
		}

	default:
		// Retryable: no response (network error/timeout), 429 or 5xx.
		reason := result.error

		if reason == "" {
			reason = fmt.Sprintf("delivery failed with retryable HTTP status %d", result.status)
		}

		if d.Attempts >= d.MaxAttempts {
			exhausted := fmt.Sprintf("max delivery attempts (%d) exceeded: %s", d.MaxAttempts, reason)

			if err := store.MarkOutboundWebhookDeliveryFailed(ctx, d.DeliveryID, d.Attempts, exhausted, responseStatus, responseBody); err != nil {
				slog.Errorf("error while marking webhook delivery %d failed: %s", d.DeliveryID, err.Error())
			}

			return
		}

		delay := deliveryBackoff(d.Attempts)

		if result.status == http.StatusTooManyRequests {
			if retryAfter, ok := parseRetryAfter(result.retryAfter); ok {
				delay = retryAfter
			}
		}

		nextAttemptAt := time.Now().Add(delay)

		if err := store.MarkOutboundWebhookDeliveryRetry(ctx, d.DeliveryID, d.Attempts, reason, responseStatus, responseBody, nextAttemptAt); err != nil {
			slog.Errorf("error while rescheduling webhook delivery %d: %s", d.DeliveryID, err.Error())
		}
	}
}

// sendDelivery performs the frozen HTTP request. A stable delivery identifier
// header is attached so receivers can deduplicate retries of the same event.
func sendDelivery(d app.OutboundWebhookDelivery) deliveryAttemptResult {
	headers := shttp.HeadersFromMap(d.RequestHeaders)
	headers.Set(app.DeliveryIDHeader, d.DeliveryID.String())

	req := shttp.NewRequestV2(d.RequestMethod, d.RequestURL).
		WithTimeout(deliveryRequestTimeout).
		Headers(headers)

	if d.RequestBody != "" {
		req.Payload(d.RequestBody)
	}

	res, err := req.Do()

	if err != nil {
		return deliveryAttemptResult{error: err.Error()}
	}

	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)

	if err != nil {
		// Headers arrived but the body did not: do not acknowledge delivery.
		return deliveryAttemptResult{error: err.Error()}
	}

	return deliveryAttemptResult{
		status:     res.StatusCode,
		body:       truncateDeliveryBody(string(body)),
		retryAfter: res.Header.Get("Retry-After"),
	}
}

// isPermanentDeliveryError reports whether the attempt must not be retried.
// Network errors and timeouts (no response), 429 rate limiting and 5xx server
// errors are recoverable; everything else with a response (notably explicit
// 4xx rejections and exhausted redirects) is terminal.
func isPermanentDeliveryError(result deliveryAttemptResult) bool {
	if result.status == 0 {
		return false
	}

	if result.status == http.StatusTooManyRequests {
		return false
	}

	if result.status >= 500 {
		return false
	}

	return true
}

// deliveryBackoff returns the delay before the attempt after the given
// (1-based) attempt number: 30s, 1m, 2m, ... capped at 30m.
func deliveryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := float64(deliveryBackoffBase) * math.Pow(2, float64(attempt-1))

	if delay > float64(deliveryBackoffMax) {
		delay = float64(deliveryBackoffMax)
	}

	return time.Duration(delay)
}

// parseRetryAfter parses an RFC 9110 Retry-After header, either as a number of
// seconds or an HTTP date. It is clamped to the backoff ceiling.
func parseRetryAfter(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)

	if value == "" {
		return 0, false
	}

	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			seconds = 0
		}

		delay := time.Duration(seconds) * time.Second

		if delay > deliveryBackoffMax {
			delay = deliveryBackoffMax
		}

		return delay, true
	}

	if when, err := http.ParseTime(value); err == nil {
		delay := time.Until(when)

		if delay < 0 {
			delay = 0
		}

		if delay > deliveryBackoffMax {
			delay = deliveryBackoffMax
		}

		return delay, true
	}

	return 0, false
}

func truncateDeliveryBody(body string) string {
	if len(body) > deliveryMaxBodyLength {
		return body[:deliveryMaxBodyLength]
	}

	return body
}

// rowScanner is satisfied by both *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanOutboundWebhookDelivery(scanner rowScanner) (app.OutboundWebhookDelivery, error) {
	var (
		d       app.OutboundWebhookDelivery
		headers []byte
	)

	err := scanner.Scan(
		&d.DeliveryID, &d.AppID, &d.DeploymentID, &d.WebhookID, &d.TriggerWhen,
		&d.RequestURL, &d.RequestMethod, &headers, &d.RequestBody,
		&d.Status, &d.Attempts, &d.MaxAttempts,
		&d.LastError, &d.ResponseStatus, &d.ResponseBody,
		&d.NextAttemptAt, &d.LockedAt, &d.LastAttemptAt, &d.SucceededAt,
		&d.CreatedAt, &d.UpdatedAt,
	)

	if err != nil {
		return d, err
	}

	d.RequestHeaders = map[string]string{}

	if len(headers) > 0 {
		if err := json.Unmarshal(headers, &d.RequestHeaders); err != nil {
			slog.Errorf("failed while unmarshaling webhook delivery headers: %s", err.Error())
		}
	}

	return d, nil
}

// EnqueueOutboundWebhookDeliveries persists the frozen completion events. The
// unique index on (deployment_id, wh_id) makes enqueueing idempotent: running
// the completion path twice never creates a second event for the same
// deployment and trigger. It returns the number of newly created rows.
func (s *Store) EnqueueOutboundWebhookDeliveries(ctx context.Context, deliveries []app.OutboundWebhookDelivery) (int, error) {
	inserted := 0

	for _, d := range deliveries {
		headers, err := json.Marshal(d.RequestHeaders)

		if err != nil {
			return inserted, err
		}

		result, err := s.Exec(ctx, stmt.insertOutboundWebhookDelivery,
			d.AppID, d.DeploymentID, d.WebhookID, d.TriggerWhen,
			d.RequestURL, d.RequestMethod, headers, d.RequestBody,
			d.MaxAttempts,
		)

		if err != nil {
			return inserted, err
		}

		if affected, err := result.RowsAffected(); err == nil {
			inserted += int(affected)
		}
	}

	return inserted, nil
}

// ClaimDueOutboundWebhookDeliveries atomically transitions due pending rows
// (and stale sending rows older than staleThreshold) to the sending state and
// returns them with an incremented attempt counter.
func (s *Store) ClaimDueOutboundWebhookDeliveries(ctx context.Context, limit int, staleThreshold time.Duration) ([]app.OutboundWebhookDelivery, error) {
	rows, err := s.Query(ctx, stmt.claimDueOutboundWebhookDeliveries, limit, time.Now().Add(-staleThreshold))

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	deliveries := []app.OutboundWebhookDelivery{}

	for rows.Next() {
		d, err := scanOutboundWebhookDelivery(rows)

		if err != nil {
			slog.Errorf("failed while scanning webhook delivery: %s", err.Error())
			continue
		}

		deliveries = append(deliveries, d)
	}

	return deliveries, nil
}

// MarkOutboundWebhookDeliverySucceeded records a terminal 2xx acknowledgement.
// The attempt guard ensures a late response from a reclaimed attempt cannot
// overwrite newer state.
func (s *Store) MarkOutboundWebhookDeliverySucceeded(ctx context.Context, deliveryID types.ID, attempt, statusCode int, body string) error {
	_, err := s.Exec(ctx, stmt.markOutboundWebhookDeliverySucceeded, deliveryID, attempt, statusCode, body)
	return err
}

// MarkOutboundWebhookDeliveryRetry returns the row to pending with the failure
// reason and the next scheduled attempt.
func (s *Store) MarkOutboundWebhookDeliveryRetry(ctx context.Context, deliveryID types.ID, attempt int, errMsg string, statusCode null.Int, body null.String, nextAttemptAt time.Time) error {
	_, err := s.Exec(ctx, stmt.markOutboundWebhookDeliveryRetry, deliveryID, attempt, errMsg, statusCode, body, nextAttemptAt)
	return err
}

// MarkOutboundWebhookDeliveryFailed records the terminal failure and preserves
// its reason.
func (s *Store) MarkOutboundWebhookDeliveryFailed(ctx context.Context, deliveryID types.ID, attempt int, reason string, statusCode null.Int, body null.String) error {
	_, err := s.Exec(ctx, stmt.markOutboundWebhookDeliveryFailed, deliveryID, attempt, reason, statusCode, body)
	return err
}

// OutboundWebhookDeliveries returns all delivery records for a deployment,
// making the delivery outcome per deployment and trigger queryable.
func (s *Store) OutboundWebhookDeliveries(ctx context.Context, deploymentID types.ID) ([]app.OutboundWebhookDelivery, error) {
	rows, err := s.Query(ctx, stmt.selectOutboundWebhookDeliveries, deploymentID)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	deliveries := []app.OutboundWebhookDelivery{}

	for rows.Next() {
		d, err := scanOutboundWebhookDelivery(rows)

		if err != nil {
			return nil, err
		}

		deliveries = append(deliveries, d)
	}

	return deliveries, nil
}

// OutboundWebhookDelivery returns the delivery record for one deployment and
// trigger. It returns nil without error when no record exists.
func (s *Store) OutboundWebhookDelivery(ctx context.Context, deploymentID, webhookID types.ID) (*app.OutboundWebhookDelivery, error) {
	row, err := s.QueryRow(ctx, stmt.selectOutboundWebhookDeliveryByDeployment, deploymentID, webhookID)

	if err != nil {
		return nil, err
	}

	d, err := scanOutboundWebhookDelivery(row)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return &d, nil
}
