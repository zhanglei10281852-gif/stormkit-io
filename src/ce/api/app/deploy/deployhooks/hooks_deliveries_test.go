package deployhooks_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stormkit-io/stormkit-io/src/ce/api/app"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deploy"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deploy/deployhooks"
	"github.com/stormkit-io/stormkit-io/src/lib/database/databasetest"
	"github.com/stormkit-io/stormkit-io/src/lib/factory"
	"github.com/stormkit-io/stormkit-io/src/lib/shttp"
	"github.com/stormkit-io/stormkit-io/src/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	null "gopkg.in/guregu/null.v3"
)

type HooksDeliveriesSuite struct {
	suite.Suite
	*factory.Factory

	conn databasetest.TestDB
}

func (s *HooksDeliveriesSuite) BeforeTest(suiteName, _ string) {
	s.conn = databasetest.InitTx(suiteName)
	s.Factory = factory.New(s.conn)
}

func (s *HooksDeliveriesSuite) AfterTest(_, _ string) {
	s.conn.CloseTx()
}

// fakeWebhookServer replies with firstStatus on the first call and with
// laterStatus on every subsequent call, recording call count, bodies and the
// delivery id headers it received.
type fakeWebhookServer struct {
	server       *httptest.Server
	mu           sync.Mutex
	calls        int
	bodies       []string
	deliveryIDs  []string
	contentTypes []string

	firstStatus int
	laterStatus int
	retryAfter  string
}

func newFakeWebhookServer(firstStatus, laterStatus int, retryAfter string) *fakeWebhookServer {
	f := &fakeWebhookServer{
		firstStatus: firstStatus,
		laterStatus: laterStatus,
		retryAfter:  retryAfter,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)

		f.mu.Lock()
		f.calls++
		f.bodies = append(f.bodies, string(body))
		f.deliveryIDs = append(f.deliveryIDs, req.Header.Get(app.DeliveryIDHeader))
		f.contentTypes = append(f.contentTypes, req.Header.Get("Content-Type"))

		status := f.firstStatus

		if f.calls > 1 {
			status = f.laterStatus
		}

		if status == http.StatusTooManyRequests && f.retryAfter != "" {
			w.Header().Set("Retry-After", f.retryAfter)
		}

		f.mu.Unlock()

		w.WriteHeader(status)
		_, _ = w.Write([]byte("response-body"))
	})

	f.server = httptest.NewServer(mux)

	return f
}

func (f *fakeWebhookServer) URL() string {
	return f.server.URL
}

func (f *fakeWebhookServer) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

func (f *fakeWebhookServer) DeliveryIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, len(f.deliveryIDs))
	copy(out, f.deliveryIDs)

	return out
}

func (f *fakeWebhookServer) Close() {
	f.server.Close()
}

// processSynchronously replaces the asynchronous delivery trigger with a
// synchronous implementation and returns a restore function.
func (s *HooksDeliveriesSuite) processSynchronously() func() {
	original := deployhooks.TriggerDeliveries
	deployhooks.TriggerDeliveries = func() {
		s.Require().NoError(deployhooks.ProcessDueDeliveries(context.Background()))
	}

	return func() {
		deployhooks.TriggerDeliveries = original
	}
}

// disableTrigger enqueues events without delivering them so the test can
// manipulate the rows before driving processing manually.
func (s *HooksDeliveriesSuite) disableTrigger() func() {
	original := deployhooks.TriggerDeliveries
	deployhooks.TriggerDeliveries = func() {}

	return func() {
		deployhooks.TriggerDeliveries = original
	}
}

func (s *HooksDeliveriesSuite) insertWebhook(triggerWhen, url string, payload *string) types.ID {
	body := null.String{}

	if payload != nil {
		body = null.StringFrom(*payload)
	}

	err := app.NewStore().InsertOutboundWebhook(context.Background(), s.GetApp().ID, app.OutboundWebhook{
		TriggerWhen:    triggerWhen,
		RequestURL:     url,
		RequestMethod:  shttp.MethodPost,
		RequestPayload: body,
		RequestHeaders: map[string]string{"Content-Type": "application/json"},
	})

	s.Require().NoError(err)

	var whID types.ID
	err = s.conn.QueryRow(
		`SELECT wh_id FROM app_outbound_webhooks WHERE app_id = $1 ORDER BY wh_id DESC LIMIT 1`,
		s.GetApp().ID,
	).Scan(&whID)

	s.Require().NoError(err)

	return whID
}

func (s *HooksDeliveriesSuite) delivery(deploymentID, webhookID types.ID) *app.OutboundWebhookDelivery {
	d, err := deployhooks.NewStore().OutboundWebhookDelivery(context.Background(), deploymentID, webhookID)
	s.Require().NoError(err)
	s.Require().NotNil(d)

	return d
}

func (s *HooksDeliveriesSuite) successfulDeployment() *factory.MockDeployment {
	return s.MockDeployment(nil, map[string]interface{}{
		"ExitCode":          null.NewInt(0, true),
		"PullRequestNumber": null.NewInt(0, true),
		"ShouldPublish":     true,
	})
}

func (s *HooksDeliveriesSuite) failedDeployment() *factory.MockDeployment {
	return s.MockDeployment(nil, map[string]interface{}{
		"ExitCode":          null.NewInt(1, true),
		"PullRequestNumber": null.NewInt(0, true),
		"ShouldPublish":     false,
		"Error":             null.StringFrom("build failed"),
	})
}

func (s *HooksDeliveriesSuite) execCompletion(d *deploy.Deployment) {
	deployhooks.Exec(context.Background(), d)
}

// Test_Success_FreezesPayloadAndTarget verifies that a successful deployment
// delivers the webhook exactly once with the rendered payload and a stable
// delivery id header, and records a succeeded delivery.
func (s *HooksDeliveriesSuite) Test_Success_FreezesPayloadAndTarget() {
	a := assert.New(s.T())
	server := newFakeWebhookServer(http.StatusOK, http.StatusOK, "")
	defer server.Close()

	payload := `{"deployment_id":"$SK_DEPLOYMENT_ID","status":"$SK_DEPLOYMENT_STATUS"}`
	whID := s.insertWebhook(app.TriggerOnDeploySuccess, server.URL(), &payload)

	depl := s.successfulDeployment()

	restore := s.processSynchronously()
	defer restore()

	s.execCompletion(&deploy.Deployment{
		ID:       depl.ID,
		Env:      "production",
		Branch:   "master",
		EnvID:    s.GetEnv().ID,
		AppID:    s.GetApp().ID,
		ExitCode: null.NewInt(0, true),
	})

	a.Equal(1, server.Calls())
	a.JSONEq(`{"deployment_id":"1","status":"success"}`, server.bodies[0])
	a.Equal([]string{"application/json"}, server.contentTypes)

	d := s.delivery(depl.ID, whID)
	a.Equal(app.DeliveryStatusSucceeded, d.Status)
	a.Equal(1, d.Attempts)
	a.Equal(int64(http.StatusOK), d.ResponseStatus.ValueOrZero())
	a.JSONEq(`{"deployment_id":"1","status":"success"}`, d.RequestBody)
	a.Equal(server.URL(), d.RequestURL)
	a.Equal(shttp.MethodPost, d.RequestMethod)
	a.True(d.SucceededAt.Valid)
	a.Equal(d.DeliveryID.String(), server.DeliveryIDs()[0])
}

// Test_Idempotent_DuplicateCompletion verifies that running the completion
// path twice still creates exactly one event and causes one external call.
func (s *HooksDeliveriesSuite) Test_Idempotent_DuplicateCompletion() {
	a := assert.New(s.T())
	server := newFakeWebhookServer(http.StatusOK, http.StatusOK, "")
	defer server.Close()

	payload := `{"deployment_id":"$SK_DEPLOYMENT_ID"}`
	whID := s.insertWebhook(app.TriggerOnDeploySuccess, server.URL(), &payload)
	depl := s.successfulDeployment()

	restore := s.processSynchronously()
	defer restore()

	completion := func() {
		s.execCompletion(&deploy.Deployment{
			ID:       depl.ID,
			Env:      "production",
			Branch:   "master",
			EnvID:    s.GetEnv().ID,
			AppID:    s.GetApp().ID,
			ExitCode: null.NewInt(0, true),
		})
	}

	completion()
	completion()

	a.Equal(1, server.Calls())

	rows, err := deployhooks.NewStore().OutboundWebhookDeliveries(context.Background(), depl.ID)
	s.Require().NoError(err)
	a.Len(rows, 1)
	a.Equal(1, rows[0].Attempts)
	a.Equal(app.DeliveryStatusSucceeded, rows[0].Status)
	a.Equal(whID, rows[0].WebhookID)
	a.NotEmpty(server.DeliveryIDs()[0])
	a.Equal(rows[0].DeliveryID.String(), server.DeliveryIDs()[0])
}

// Test_FailedDeployment_TriggersFailedWebhook verifies that a failed build
// fires on_deploy_failed with the rendered failure payload.
func (s *HooksDeliveriesSuite) Test_FailedDeployment_TriggersFailedWebhook() {
	a := assert.New(s.T())
	server := newFakeWebhookServer(http.StatusOK, http.StatusOK, "")
	defer server.Close()

	payload := `{"status":"$SK_DEPLOYMENT_STATUS","error":"$SK_DEPLOYMENT_ERROR"}`
	whID := s.insertWebhook(app.TriggerOnDeployFailed, server.URL(), &payload)
	depl := s.failedDeployment()

	restore := s.processSynchronously()
	defer restore()

	s.execCompletion(&deploy.Deployment{
		ID:       depl.ID,
		Env:      "production",
		Branch:   "master",
		EnvID:    s.GetEnv().ID,
		AppID:    s.GetApp().ID,
		ExitCode: null.NewInt(1, true),
		Error:    null.StringFrom("build failed"),
	})

	a.Equal(1, server.Calls())
	a.JSONEq(`{"status":"failed","error":"build failed"}`, server.bodies[0])

	d := s.delivery(depl.ID, whID)
	a.Equal(app.DeliveryStatusSucceeded, d.Status)
	a.JSONEq(`{"status":"failed","error":"build failed"}`, d.RequestBody)
}

// Test_Permanent4xx_Terminal verifies that an explicit 4xx rejection ends in
// the failed terminal state, preserving the reason, and is never retried.
func (s *HooksDeliveriesSuite) Test_Permanent4xx_Terminal() {
	a := assert.New(s.T())
	server := newFakeWebhookServer(http.StatusNotFound, http.StatusNotFound, "")
	defer server.Close()

	whID := s.insertWebhook(app.TriggerOnDeploySuccess, server.URL(), nil)
	depl := s.successfulDeployment()

	restore := s.processSynchronously()
	defer restore()

	s.execCompletion(&deploy.Deployment{
		ID:       depl.ID,
		Env:      "production",
		Branch:   "master",
		EnvID:    s.GetEnv().ID,
		AppID:    s.GetApp().ID,
		ExitCode: null.NewInt(0, true),
	})

	a.Equal(1, server.Calls())

	d := s.delivery(depl.ID, whID)
	a.Equal(app.DeliveryStatusFailed, d.Status)
	a.Equal(1, d.Attempts)
	a.Equal(int64(http.StatusNotFound), d.ResponseStatus.ValueOrZero())
	a.Contains(d.LastError.String, "404")
	a.Contains(d.LastError.String, "response-body")
	a.False(d.SucceededAt.Valid)

	// The worker must not pick terminal rows up again.
	s.Require().NoError(deployhooks.ProcessDueDeliveries(context.Background()))
	a.Equal(1, server.Calls())
}

// Test_5xx_RetriesWithBackoff verifies that a 5xx is retried on an exponential
// schedule and succeeds once the target recovers.
func (s *HooksDeliveriesSuite) Test_5xx_RetriesWithBackoff() {
	a := assert.New(s.T())
	server := newFakeWebhookServer(http.StatusServiceUnavailable, http.StatusOK, "")
	defer server.Close()

	whID := s.insertWebhook(app.TriggerOnDeploySuccess, server.URL(), nil)
	depl := s.successfulDeployment()

	restore := s.processSynchronously()
	defer restore()

	before := time.Now()

	s.execCompletion(&deploy.Deployment{
		ID:       depl.ID,
		Env:      "production",
		Branch:   "master",
		EnvID:    s.GetEnv().ID,
		AppID:    s.GetApp().ID,
		ExitCode: null.NewInt(0, true),
	})

	a.Equal(1, server.Calls())

	d := s.delivery(depl.ID, whID)
	a.Equal(app.DeliveryStatusPending, d.Status)
	a.Equal(1, d.Attempts)
	a.Equal(int64(http.StatusServiceUnavailable), d.ResponseStatus.ValueOrZero())
	a.Contains(d.LastError.String, "503")
	a.True(d.NextAttemptAt.After(before.Add(20*time.Second)), "first backoff should be ~30s, got %v", d.NextAttemptAt.Sub(before))
	a.True(d.NextAttemptAt.Before(before.Add(45 * time.Second)))

	// Simulate the worker tick arriving after the backoff window.
	_, err := s.conn.Exec(
		`UPDATE deployment_outbound_webhook_deliveries SET next_attempt_at = NOW() - INTERVAL '1 minute' WHERE delivery_id = $1`,
		d.DeliveryID,
	)
	s.Require().NoError(err)

	s.Require().NoError(deployhooks.ProcessDueDeliveries(context.Background()))

	a.Equal(2, server.Calls())

	d = s.delivery(depl.ID, whID)
	a.Equal(app.DeliveryStatusSucceeded, d.Status)
	a.Equal(2, d.Attempts)
	a.Equal(int64(http.StatusOK), d.ResponseStatus.ValueOrZero())

	// Both attempts carry the same stable delivery id.
	ids := server.DeliveryIDs()
	a.Len(ids, 2)
	a.Equal(ids[0], ids[1])
}

// Test_429_HonorsRetryAfter verifies that a 429 is retried and the Retry-After
// header drives the next attempt delay.
func (s *HooksDeliveriesSuite) Test_429_HonorsRetryAfter() {
	a := assert.New(s.T())
	server := newFakeWebhookServer(http.StatusTooManyRequests, http.StatusOK, "1")
	defer server.Close()

	whID := s.insertWebhook(app.TriggerOnDeploySuccess, server.URL(), nil)
	depl := s.successfulDeployment()

	restore := s.processSynchronously()
	defer restore()

	before := time.Now()

	s.execCompletion(&deploy.Deployment{
		ID:       depl.ID,
		Env:      "production",
		Branch:   "master",
		EnvID:    s.GetEnv().ID,
		AppID:    s.GetApp().ID,
		ExitCode: null.NewInt(0, true),
	})

	d := s.delivery(depl.ID, whID)
	a.Equal(app.DeliveryStatusPending, d.Status)
	a.True(d.NextAttemptAt.Before(before.Add(3*time.Second)), "Retry-After: 1 should schedule ~1s out, got %v", d.NextAttemptAt.Sub(before))
	a.True(d.NextAttemptAt.After(before.Add(-1 * time.Second)))
	a.Equal(int64(http.StatusTooManyRequests), d.ResponseStatus.ValueOrZero())

	_, err := s.conn.Exec(
		`UPDATE deployment_outbound_webhook_deliveries SET next_attempt_at = NOW() - INTERVAL '1 minute' WHERE delivery_id = $1`,
		d.DeliveryID,
	)
	s.Require().NoError(err)

	s.Require().NoError(deployhooks.ProcessDueDeliveries(context.Background()))

	a.Equal(2, server.Calls())
	a.Equal(app.DeliveryStatusSucceeded, s.delivery(depl.ID, whID).Status)
}

// Test_StaleSending_Reclaimed verifies that a record stuck in the sending
// state after a crash or restart is reclaimed and delivered.
func (s *HooksDeliveriesSuite) Test_StaleSending_Reclaimed() {
	a := assert.New(s.T())
	server := newFakeWebhookServer(http.StatusOK, http.StatusOK, "")
	defer server.Close()

	whID := s.insertWebhook(app.TriggerOnDeploySuccess, server.URL(), nil)
	depl := s.successfulDeployment()

	restore := s.disableTrigger()
	defer restore()

	s.execCompletion(&deploy.Deployment{
		ID:       depl.ID,
		Env:      "production",
		Branch:   "master",
		EnvID:    s.GetEnv().ID,
		AppID:    s.GetApp().ID,
		ExitCode: null.NewInt(0, true),
	})

	a.Zero(server.Calls())

	// Simulate a process that claimed the row and died mid-request.
	_, err := s.conn.Exec(
		`UPDATE deployment_outbound_webhook_deliveries
			SET status = 'sending', attempts = 1, locked_at = NOW() - INTERVAL '10 minutes'
			WHERE delivery_id = (SELECT delivery_id FROM deployment_outbound_webhook_deliveries WHERE deployment_id = $1)`,
		depl.ID,
	)
	s.Require().NoError(err)

	s.Require().NoError(deployhooks.ProcessDueDeliveries(context.Background()))

	a.Equal(1, server.Calls())

	d := s.delivery(depl.ID, whID)
	a.Equal(app.DeliveryStatusSucceeded, d.Status)
	a.Equal(2, d.Attempts)
}

// Test_MaxAttempts_Terminal verifies that retries end in a terminal failed
// state with a preserved reason once the attempt budget is exhausted.
func (s *HooksDeliveriesSuite) Test_MaxAttempts_Terminal() {
	a := assert.New(s.T())
	server := newFakeWebhookServer(http.StatusBadGateway, http.StatusBadGateway, "")
	defer server.Close()

	whID := s.insertWebhook(app.TriggerOnDeploySuccess, server.URL(), nil)
	depl := s.successfulDeployment()

	restore := s.disableTrigger()
	defer restore()

	s.execCompletion(&deploy.Deployment{
		ID:       depl.ID,
		Env:      "production",
		Branch:   "master",
		EnvID:    s.GetEnv().ID,
		AppID:    s.GetApp().ID,
		ExitCode: null.NewInt(0, true),
	})

	// One attempt short of the budget: the next claim reaches max attempts.
	_, err := s.conn.Exec(
		`UPDATE deployment_outbound_webhook_deliveries
			SET attempts = $2, next_attempt_at = NOW() - INTERVAL '1 minute'
			WHERE deployment_id = $1`,
		depl.ID, app.DeliveryMaxAttempts-1,
	)
	s.Require().NoError(err)

	s.Require().NoError(deployhooks.ProcessDueDeliveries(context.Background()))

	a.Equal(1, server.Calls())

	d := s.delivery(depl.ID, whID)
	a.Equal(app.DeliveryStatusFailed, d.Status)
	a.Equal(app.DeliveryMaxAttempts, d.Attempts)
	a.Contains(d.LastError.String, "max delivery attempts")
	a.Equal(int64(http.StatusBadGateway), d.ResponseStatus.ValueOrZero())
}

// Test_ConfigChangeAfterEnqueue_KeepsFrozenTarget verifies that editing the
// webhook after the event was created changes neither the target URL nor the
// payload used by retries.
func (s *HooksDeliveriesSuite) Test_ConfigChangeAfterEnqueue_KeepsFrozenTarget() {
	a := assert.New(s.T())
	server := newFakeWebhookServer(http.StatusServiceUnavailable, http.StatusOK, "")
	defer server.Close()

	payload := `{"version":"v1"}`
	whID := s.insertWebhook(app.TriggerOnDeploySuccess, server.URL(), &payload)
	depl := s.successfulDeployment()

	restore := s.processSynchronously()
	defer restore()

	s.execCompletion(&deploy.Deployment{
		ID:       depl.ID,
		Env:      "production",
		Branch:   "master",
		EnvID:    s.GetEnv().ID,
		AppID:    s.GetApp().ID,
		ExitCode: null.NewInt(0, true),
	})

	a.Equal(1, server.Calls())

	// Edit the trigger configuration after the completion event exists.
	configured := app.NewStore().OutboundWebhook(context.Background(), s.GetApp().ID, whID)
	s.Require().NotNil(configured)
	configured.RequestURL = "http://127.0.0.1:1/unreachable"
	configured.RequestPayload = null.StringFrom(`{"version":"v2"}`)
	s.Require().NoError(app.NewStore().UpdateOutboundWebhook(context.Background(), s.GetApp().ID, configured))

	_, err := s.conn.Exec(
		`UPDATE deployment_outbound_webhook_deliveries SET next_attempt_at = NOW() - INTERVAL '1 minute' WHERE deployment_id = $1`,
		depl.ID,
	)
	s.Require().NoError(err)

	s.Require().NoError(deployhooks.ProcessDueDeliveries(context.Background()))

	// The retry hit the original frozen target again, not the edited URL.
	a.Equal(2, server.Calls())

	d := s.delivery(depl.ID, whID)
	a.Equal(app.DeliveryStatusSucceeded, d.Status)
	a.Equal(server.URL(), d.RequestURL)
	a.JSONEq(`{"version":"v1"}`, d.RequestBody)
}

// Test_NetworkError_Retries verifies that a connection error is retryable and
// never blocks processing.
func (s *HooksDeliveriesSuite) Test_NetworkError_Retries() {
	a := assert.New(s.T())

	// Nothing listens on this port.
	whID := s.insertWebhook(app.TriggerOnDeploySuccess, "http://127.0.0.1:1", nil)
	depl := s.successfulDeployment()

	restore := s.processSynchronously()
	defer restore()

	s.execCompletion(&deploy.Deployment{
		ID:       depl.ID,
		Env:      "production",
		Branch:   "master",
		EnvID:    s.GetEnv().ID,
		AppID:    s.GetApp().ID,
		ExitCode: null.NewInt(0, true),
	})

	d := s.delivery(depl.ID, whID)
	a.Equal(app.DeliveryStatusPending, d.Status)
	a.Equal(1, d.Attempts)
	a.False(d.ResponseStatus.Valid)
	a.NotEmpty(d.LastError.String)
	a.True(d.NextAttemptAt.After(time.Now()))
}

// Test_NoWebhookConfigured_KeepsDefaultBehavior verifies that without any
// outbound webhook, no delivery records are created and Exec still completes.
func (s *HooksDeliveriesSuite) Test_NoWebhookConfigured_KeepsDefaultBehavior() {
	a := assert.New(s.T())
	depl := s.successfulDeployment()

	s.execCompletion(&deploy.Deployment{
		ID:       depl.ID,
		Env:      "production",
		Branch:   "master",
		EnvID:    s.GetEnv().ID,
		AppID:    s.GetApp().ID,
		ExitCode: null.NewInt(0, true),
	})

	rows, err := deployhooks.NewStore().OutboundWebhookDeliveries(context.Background(), depl.ID)
	s.Require().NoError(err)
	a.Empty(rows)
}

func TestHooksDeliveries(t *testing.T) {
	suite.Run(t, &HooksDeliveriesSuite{})
}
