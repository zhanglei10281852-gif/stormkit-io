package apphandlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/stormkit-io/stormkit-io/src/ce/api/app"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/apphandlers"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deploy"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deploy/webhookdelivery"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deployservice"
	"github.com/stormkit-io/stormkit-io/src/lib/database/databasetest"
	"github.com/stormkit-io/stormkit-io/src/lib/factory"
	"github.com/stormkit-io/stormkit-io/src/lib/shttp"
	"github.com/stormkit-io/stormkit-io/src/lib/shttp/shttptest"
	"github.com/stormkit-io/stormkit-io/src/mocks"
)

// deliveryRows are the persisted result rows for one provider delivery.
type deliveryRow struct {
	status       string
	appID        int64
	envID        int64
	deploymentID int64
}

// pushDelivery posts the github push payload with a delivery GUID.
func (s *InboundGithubSuite) pushDelivery(payload map[string]any, guid string) shttptest.Response {
	headers := map[string]string{
		"X-GitHub-Event":      "push",
		"X-Hub-Signature-256": githubSignature(payload),
		"X-GitHub-Delivery":   guid,
	}

	return shttptest.RequestWithHeaders(
		shttp.NewRouter().RegisterService(apphandlers.Services).Router().Handler(),
		shttp.MethodPost,
		"/app/webhooks/github/deploy",
		payload,
		headers,
	)
}

// deliveryResultsFor returns the persisted result rows of one provider delivery.
func deliveryResultsFor(conn databasetest.TestDB, provider, guid string) []deliveryRow {
	rows, err := conn.Query(`
		SELECT r.status, r.app_id, r.env_id, COALESCE(r.deployment_id, 0)
		FROM webhook_delivery_results r
		JOIN webhook_deliveries wd ON wd.delivery_id = r.delivery_id
		WHERE wd.provider = $1 AND wd.provider_delivery_id = $2
		ORDER BY r.env_id
	`, provider, guid)
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	results := []deliveryRow{}

	for rows.Next() {
		var row deliveryRow
		if err := rows.Scan(&row.status, &row.appID, &row.envID, &row.deploymentID); err != nil {
			panic(err)
		}

		results = append(results, row)
	}

	return results
}

// deliveryResults returns the persisted result rows for the current test.
func (s *InboundGithubSuite) deliveryResults(provider, guid string) []deliveryRow {
	return deliveryResultsFor(s.conn, provider, guid)
}

// pushExamplePayload unmarshals the simple push example used by the delivery
// tests.
func (s *InboundGithubSuite) pushExamplePayload() map[string]any {
	payload := map[string]any{}
	s.Require().NoError(json.Unmarshal([]byte(githubPushExample), &payload))

	return payload
}

// staleClaim ages a processing claim so its lease has expired.
func (s *InboundGithubSuite) staleClaim(resultID int64) {
	_, err := s.conn.Exec(`
		UPDATE webhook_delivery_results
		SET claimed_at = NOW() AT TIME ZONE 'UTC' - INTERVAL '10 minutes'
		WHERE result_id = $1
	`, resultID)
	s.Require().NoError(err)
}

// Test_Delivery_DuplicateRequestDeploysOnce verifies two identical deliveries
// (provider retry or redelivery) start a single deployment and both answer 200.
func (s *InboundGithubSuite) Test_Delivery_DuplicateRequestDeploysOnce() {
	appl := s.app(nil)
	payload := s.pushExamplePayload()

	first := s.pushDelivery(payload, "github-delivery-1")
	s.Equal(http.StatusOK, first.Code)

	second := s.pushDelivery(payload, "github-delivery-1")
	s.Equal(http.StatusOK, second.Code)

	s.mockDeployer.AssertNumberOfCalls(s.T(), "Deploy", 1)
	s.mockDeployer.AssertCalled(s.T(), "Deploy",
		mock.Anything,
		mock.MatchedBy(func(a *app.App) bool { return a.ID == appl.ID }),
		mock.MatchedBy(func(d *deploy.Deployment) bool { return d.DeliveryResultID != 0 }),
	)

	results := s.deliveryResults(webhookdelivery.ProviderGitHub, "github-delivery-1")
	s.Len(results, 1)
	s.Equal("succeeded", results[0].status)
	s.Equal(int64(appl.ID), results[0].appID)
}

// Test_Delivery_ConcurrentRequestAccepted verifies that a second request while
// the first is still processing gets 202 and starts nothing.
func (s *InboundGithubSuite) Test_Delivery_ConcurrentRequestAccepted() {
	appl := s.app(nil)
	env := s.GetEnv()

	deliveryID, err := webhookdelivery.NewStore().UpsertDelivery(context.Background(), &webhookdelivery.Delivery{
		Provider:         webhookdelivery.ProviderGitHub,
		ProviderDelivery: "github-delivery-2",
		Repo:             "github/stormkit-test-acc/test-repo",
		Branch:           "main",
	})
	s.Require().NoError(err)

	_, _, err = webhookdelivery.NewStore().ClaimResult(context.Background(), deliveryID, appl.ID, env.ID, "production")
	s.Require().NoError(err)

	response := s.pushDelivery(s.pushExamplePayload(), "github-delivery-2")
	s.Equal(http.StatusAccepted, response.Code)
	s.mockDeployer.AssertNotCalled(s.T(), "Deploy")

	results := s.deliveryResults(webhookdelivery.ProviderGitHub, "github-delivery-2")
	s.Len(results, 1)
	s.Equal("processing", results[0].status)
}

// Test_Delivery_MultipleEnvironmentsFanOut verifies one delivery matching two
// environments deploys and records both, never merging them into one result.
func (s *InboundGithubSuite) Test_Delivery_MultipleEnvironmentsFanOut() {
	appl := s.app(nil)
	firstEnv := s.GetEnv()
	secondEnv := s.MockEnv(appl, map[string]any{"Name": "staging", "Branch": "main"})

	response := s.pushDelivery(s.pushExamplePayload(), "github-delivery-3")
	s.Equal(http.StatusOK, response.Code)

	s.mockDeployer.AssertNumberOfCalls(s.T(), "Deploy", 2)

	envIDs := map[int64]bool{}

	for _, call := range s.mockDeployer.Calls {
		d := call.Arguments.Get(2).(*deploy.Deployment)
		envIDs[int64(d.EnvID)] = true
	}

	s.True(envIDs[int64(firstEnv.ID)])
	s.True(envIDs[int64(secondEnv.ID)])

	results := s.deliveryResults(webhookdelivery.ProviderGitHub, "github-delivery-3")
	s.Len(results, 2)

	for _, row := range results {
		s.Equal("succeeded", row.status)
		s.Equal(int64(appl.ID), row.appID)
	}
}

// Test_Delivery_DeployFailureThenRedelivery verifies a failed start stays
// retryable and the redelivery resumes successfully, with one success attempt.
func (s *InboundGithubSuite) Test_Delivery_DeployFailureThenRedelivery() {
	s.app(nil)

	failing := &mocks.Deployer{}
	failing.On("Deploy", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("queue is down"))
	deployservice.MockDeployer = failing

	first := s.pushDelivery(s.pushExamplePayload(), "github-delivery-4")
	s.Equal(http.StatusServiceUnavailable, first.Code)
	failing.AssertNumberOfCalls(s.T(), "Deploy", 1)

	results := s.deliveryResults(webhookdelivery.ProviderGitHub, "github-delivery-4")
	s.Len(results, 1)
	s.Equal("retryable", results[0].status)

	// Redelivery: restore the healthy deployer and resend the same GUID.
	deployservice.MockDeployer = s.mockDeployer

	second := s.pushDelivery(s.pushExamplePayload(), "github-delivery-4")
	s.Equal(http.StatusOK, second.Code)
	s.mockDeployer.AssertNumberOfCalls(s.T(), "Deploy", 1)

	results = s.deliveryResults(webhookdelivery.ProviderGitHub, "github-delivery-4")
	s.Len(results, 1)
	s.Equal("succeeded", results[0].status)
}

// Test_Delivery_NoCandidates verifies events that match nothing stay 204 and
// leave only the inbox row, without result claims.
func (s *InboundGithubSuite) Test_Delivery_NoCandidates() {
	response := s.pushDelivery(s.pushExamplePayload(), "github-delivery-5")
	s.Equal(http.StatusNoContent, response.Code)
	s.mockDeployer.AssertNotCalled(s.T(), "Deploy")
	s.Empty(s.deliveryResults(webhookdelivery.ProviderGitHub, "github-delivery-5"))
}

// Test_RecoverStaleClaim_DispatchesExistingDeployment covers the crash window
// where the deployment exists but the queue dispatch may not have happened:
// recovery only dispatches, never inserts again.
func (s *InboundGithubSuite) Test_RecoverStaleClaim_DispatchesExistingDeployment() {
	appl := s.app(nil)
	env := s.GetEnv()
	depl := s.MockDeployment(env, nil)

	deliveryID, err := webhookdelivery.NewStore().UpsertDelivery(context.Background(), &webhookdelivery.Delivery{
		Provider:         webhookdelivery.ProviderGitHub,
		ProviderDelivery: "github-delivery-7",
		Repo:             appl.Repo,
		Branch:           "main",
	})
	s.Require().NoError(err)

	result, _, err := webhookdelivery.NewStore().ClaimResult(context.Background(), deliveryID, appl.ID, env.ID, "production")
	s.Require().NoError(err)

	_, err = s.conn.Exec(`UPDATE webhook_delivery_results SET deployment_id = $2 WHERE result_id = $1`, result.ID, depl.ID)
	s.Require().NoError(err)

	s.staleClaim(int64(result.ID))

	s.mockDeployer.On("Dispatch", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	s.Require().NoError(apphandlers.RecoverPendingWebhookDeliveries(context.Background()))

	s.mockDeployer.AssertNotCalled(s.T(), "Deploy")
	s.mockDeployer.AssertNumberOfCalls(s.T(), "Dispatch", 1)

	results := s.deliveryResults(webhookdelivery.ProviderGitHub, "github-delivery-7")
	s.Len(results, 1)
	s.Equal("succeeded", results[0].status)
	s.Equal(int64(depl.ID), results[0].deploymentID)

	// A redelivery after recovery is a stable 200 with no further dispatch.
	response := s.pushDelivery(s.pushExamplePayload(), "github-delivery-7")
	s.Equal(http.StatusOK, response.Code)
	s.mockDeployer.AssertNumberOfCalls(s.T(), "Dispatch", 1)
}

// Test_Delivery_RedeliveryWithDeploymentDispatchesOnly verifies the HTTP
// redelivery of a claim whose deployment was already inserted (enqueue failed
// before the crash) does not insert again: it re-dispatches the existing
// deployment and answers 200.
func (s *InboundGithubSuite) Test_Delivery_RedeliveryWithDeploymentDispatchesOnly() {
	appl := s.app(nil)
	env := s.GetEnv()
	depl := s.MockDeployment(env, nil)

	store := webhookdelivery.NewStore()
	ctx := context.Background()

	deliveryID, err := store.UpsertDelivery(ctx, &webhookdelivery.Delivery{
		Provider:         webhookdelivery.ProviderGitHub,
		ProviderDelivery: "github-delivery-10",
		Repo:             appl.Repo,
		Branch:           "main",
	})
	s.Require().NoError(err)

	result, _, err := store.ClaimResult(ctx, deliveryID, appl.ID, env.ID, "production")
	s.Require().NoError(err)

	// Simulate "insert succeeded, enqueue failed": the deployment exists and is
	// linked to the retryable claim with an elapsed backoff.
	past := time.Now().Add(-time.Minute)
	s.Require().NoError(store.MarkRetryable(ctx, result.ID, depl.ID, "queue down", &past))

	s.mockDeployer.On("Dispatch", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	response := s.pushDelivery(s.pushExamplePayload(), "github-delivery-10")
	s.Equal(http.StatusOK, response.Code)
	s.mockDeployer.AssertNotCalled(s.T(), "Deploy")
	s.mockDeployer.AssertNumberOfCalls(s.T(), "Dispatch", 1)

	results := s.deliveryResults(webhookdelivery.ProviderGitHub, "github-delivery-10")
	s.Len(results, 1)
	s.Equal("succeeded", results[0].status)
	s.Equal(int64(depl.ID), results[0].deploymentID)
}

// Test_RecoverStaleClaim_CreatesMissingDeployment covers the crash window where
// nothing was inserted: recovery replays the stored event for the claim.
func (s *InboundGithubSuite) Test_RecoverStaleClaim_CreatesMissingDeployment() {
	appl := s.app(nil)
	env := s.GetEnv()

	deliveryID, err := webhookdelivery.NewStore().UpsertDelivery(context.Background(), &webhookdelivery.Delivery{
		Provider:         webhookdelivery.ProviderGitHub,
		ProviderDelivery: "github-delivery-8",
		Repo:             appl.Repo,
		CheckoutRepo:     appl.Repo,
		Branch:           "main",
		EventType:        "commit",
		ChangesComplete:  true,
		Payload:          []byte(`{"ref":"refs/heads/main"}`),
	})
	s.Require().NoError(err)

	result, _, err := webhookdelivery.NewStore().ClaimResult(context.Background(), deliveryID, appl.ID, env.ID, "production")
	s.Require().NoError(err)
	s.staleClaim(int64(result.ID))

	s.Require().NoError(apphandlers.RecoverPendingWebhookDeliveries(context.Background()))

	s.mockDeployer.AssertNumberOfCalls(s.T(), "Deploy", 1)
	s.mockDeployer.AssertCalled(s.T(), "Deploy",
		mock.Anything, mock.Anything,
		mock.MatchedBy(func(d *deploy.Deployment) bool { return d.DeliveryResultID == result.ID }),
	)

	results := s.deliveryResults(webhookdelivery.ProviderGitHub, "github-delivery-8")
	s.Len(results, 1)
	s.Equal("succeeded", results[0].status)
}

// Test_Recover_SkipsFreshSucceededAndFutureRetry verifies the recovery job only
// touches expired processing and due retryable claims.
func (s *InboundGithubSuite) Test_Recover_SkipsFreshSucceededAndFutureRetry() {
	mkAppWithEnv := func() (*factory.MockApp, *factory.MockEnv) {
		appl := s.MockApp(nil, nil)
		env := s.MockEnv(appl, nil)

		return appl, env
	}

	appl, env := mkAppWithEnv()
	s.MockDeployment(env, nil)

	store := webhookdelivery.NewStore()
	ctx := context.Background()

	deliveryID, err := store.UpsertDelivery(ctx, &webhookdelivery.Delivery{
		Provider:         webhookdelivery.ProviderGitHub,
		ProviderDelivery: "github-delivery-9",
		Repo:             appl.Repo,
		Branch:           "main",
	})
	s.Require().NoError(err)

	// Fresh processing: must stay processing.
	fresh, _, err := store.ClaimResult(ctx, deliveryID, appl.ID, env.ID, "production")
	s.Require().NoError(err)

	// Succeeded claim with a deployment: must stay succeeded.
	secondApp, secondEnv := mkAppWithEnv()
	succeededDepl := s.MockDeployment(secondEnv, nil)

	succeeded, _, err := store.ClaimResult(ctx, deliveryID, secondApp.ID, secondEnv.ID, "production")
	s.Require().NoError(err)
	s.Require().NoError(store.MarkSucceeded(ctx, succeeded.ID, succeededDepl.ID))

	// Retryable claim with a future backoff and an existing deployment: left alone.
	thirdApp, thirdEnv := mkAppWithEnv()
	retryDepl := s.MockDeployment(thirdEnv, nil)

	future := time.Now().Add(time.Hour)

	retry, _, err := store.ClaimResult(ctx, deliveryID, thirdApp.ID, thirdEnv.ID, "production")
	s.Require().NoError(err)
	s.Require().NoError(store.MarkRetryable(ctx, retry.ID, retryDepl.ID, "queue down", &future))

	s.Require().NoError(apphandlers.RecoverPendingWebhookDeliveries(ctx))

	s.mockDeployer.AssertNotCalled(s.T(), "Deploy")
	s.mockDeployer.AssertNotCalled(s.T(), "Dispatch")

	r, err := store.ResultByKey(ctx, deliveryID, appl.ID, env.ID)
	s.Require().NoError(err)
	s.Equal("processing", r.Status)
	s.Equal(fresh.ID, r.ID)

	r, err = store.ResultByKey(ctx, deliveryID, secondApp.ID, secondEnv.ID)
	s.Require().NoError(err)
	s.Equal("succeeded", r.Status)

	r, err = store.ResultByKey(ctx, deliveryID, thirdApp.ID, thirdEnv.ID)
	s.Require().NoError(err)
	s.Equal("retryable", r.Status)
}
