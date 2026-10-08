package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deploy/webhookdelivery"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deployservice"
	jobs "github.com/stormkit-io/stormkit-io/src/ce/workerserver"
	"github.com/stormkit-io/stormkit-io/src/lib/database/databasetest"
	"github.com/stormkit-io/stormkit-io/src/lib/factory"
	"github.com/stormkit-io/stormkit-io/src/mocks"
)

type JobWebhookDeliveriesSuite struct {
	suite.Suite
	*factory.Factory
	conn         databasetest.TestDB
	mockDeployer *mocks.Deployer
}

func (s *JobWebhookDeliveriesSuite) BeforeTest(suiteName, _ string) {
	s.conn = databasetest.InitTx(suiteName)
	s.Factory = factory.New(s.conn)

	s.mockDeployer = &mocks.Deployer{}
	s.mockDeployer.On("Deploy", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	deployservice.MockDeployer = s.mockDeployer
}

func (s *JobWebhookDeliveriesSuite) AfterTest(_, _ string) {
	deployservice.MockDeployer = nil
	s.conn.CloseTx()
}

// Test_RecoverWebhookDeliveries verifies the scheduled entry point resumes a
// stale claim and leaves fresh claims untouched.
func (s *JobWebhookDeliveriesSuite) Test_RecoverWebhookDeliveries() {
	appl := s.MockApp(nil, nil)
	env := s.MockEnv(appl, nil)

	store := webhookdelivery.NewStore()
	ctx := context.Background()

	deliveryID, err := store.UpsertDelivery(ctx, &webhookdelivery.Delivery{
		Provider:         webhookdelivery.ProviderGitHub,
		ProviderDelivery: "job-recovery-1",
		Repo:             appl.Repo,
		CheckoutRepo:     appl.Repo,
		Branch:           "main",
		EventType:        "commit",
		ChangesComplete:  true,
		Payload:          []byte(`{"ref":"refs/heads/main"}`),
	})
	s.Require().NoError(err)

	stale, _, err := store.ClaimResult(ctx, deliveryID, appl.ID, env.ID, "production")
	s.Require().NoError(err)

	_, err = s.conn.Exec(`
		UPDATE webhook_delivery_results
		SET claimed_at = NOW() AT TIME ZONE 'UTC' - INTERVAL '10 minutes'
		WHERE result_id = $1
	`, stale.ID)
	s.Require().NoError(err)

	// A second, still fresh claim that must not be touched.
	otherApp := s.MockApp(nil, nil)
	otherEnv := s.MockEnv(otherApp, nil)

	fresh, _, err := store.ClaimResult(ctx, deliveryID, otherApp.ID, otherEnv.ID, "production")
	s.Require().NoError(err)

	s.Require().NoError(jobs.RecoverWebhookDeliveries(ctx))

	s.mockDeployer.AssertNumberOfCalls(s.T(), "Deploy", 1)

	recovered, err := store.ResultByKey(ctx, deliveryID, appl.ID, env.ID)
	s.Require().NoError(err)
	s.Equal(webhookdelivery.StatusSucceeded, recovered.Status)

	unchanged, err := store.ResultByKey(ctx, deliveryID, otherApp.ID, otherEnv.ID)
	s.Require().NoError(err)
	s.Equal(webhookdelivery.StatusProcessing, unchanged.Status)
	s.Equal(fresh.ID, unchanged.ID)
}

// Test_RecoverDueRetryable verifies a retryable claim whose backoff elapsed is
// resumed and closed.
func (s *JobWebhookDeliveriesSuite) Test_RecoverDueRetryable() {
	appl := s.MockApp(nil, nil)
	env := s.MockEnv(appl, nil)

	store := webhookdelivery.NewStore()
	ctx := context.Background()

	deliveryID, err := store.UpsertDelivery(ctx, &webhookdelivery.Delivery{
		Provider:         webhookdelivery.ProviderGitHub,
		ProviderDelivery: "job-recovery-2",
		Repo:             appl.Repo,
		CheckoutRepo:     appl.Repo,
		Branch:           "main",
		EventType:        "commit",
		ChangesComplete:  true,
		Payload:          []byte(`{"ref":"refs/heads/main"}`),
	})
	s.Require().NoError(err)

	result, _, err := store.ClaimResult(ctx, deliveryID, appl.ID, env.ID, "production")
	s.Require().NoError(err)

	past := time.Now().Add(-time.Minute)
	s.Require().NoError(store.MarkRetryable(ctx, result.ID, 0, "queue down", &past))

	s.Require().NoError(jobs.RecoverWebhookDeliveries(ctx))

	s.mockDeployer.AssertNumberOfCalls(s.T(), "Deploy", 1)

	r, err := store.ResultByKey(ctx, deliveryID, appl.ID, env.ID)
	s.Require().NoError(err)
	s.Equal(webhookdelivery.StatusSucceeded, r.Status)
}

// Test_RecoverSkipsExhaustedRetry verifies claims at the attempt cap are not
// retried automatically any more, even with an elapsed backoff.
func (s *JobWebhookDeliveriesSuite) Test_RecoverSkipsExhaustedRetry() {
	appl := s.MockApp(nil, nil)
	env := s.MockEnv(appl, nil)

	store := webhookdelivery.NewStore()
	ctx := context.Background()

	deliveryID, err := store.UpsertDelivery(ctx, &webhookdelivery.Delivery{
		Provider:         webhookdelivery.ProviderGitHub,
		ProviderDelivery: "job-recovery-3",
		Repo:             appl.Repo,
		Branch:           "main",
	})
	s.Require().NoError(err)

	result, _, err := store.ClaimResult(ctx, deliveryID, appl.ID, env.ID, "production")
	s.Require().NoError(err)

	past := time.Now().Add(-time.Minute)
	s.Require().NoError(store.MarkRetryable(ctx, result.ID, 0, "queue down", &past))
	_, err = s.conn.Exec(`UPDATE webhook_delivery_results SET attempts = 10 WHERE result_id = $1`, result.ID)
	s.Require().NoError(err)

	s.Require().NoError(jobs.RecoverWebhookDeliveries(ctx))

	s.mockDeployer.AssertNotCalled(s.T(), "Deploy")

	r, err := store.ResultByKey(ctx, deliveryID, appl.ID, env.ID)
	s.Require().NoError(err)
	s.Equal(webhookdelivery.StatusRetryable, r.Status)
	s.Equal(10, r.Attempts)
}

// Test_Recover_BadClaimDoesNotStopBatch verifies a claim whose stored event no
// longer matches any candidate is parked retryable while the remaining due
// claims in the same batch are still recovered.
func (s *JobWebhookDeliveriesSuite) Test_Recover_BadClaimDoesNotStopBatch() {
	store := webhookdelivery.NewStore()
	ctx := context.Background()

	// Claim with an unknown repository: replay cannot find a candidate.
	badDeliveryID, err := store.UpsertDelivery(ctx, &webhookdelivery.Delivery{
		Provider:         webhookdelivery.ProviderGitHub,
		ProviderDelivery: "job-recovery-bad",
		Repo:             "github/does-not-exist/repo",
		Branch:           "main",
	})
	s.Require().NoError(err)

	bad, _, err := store.ClaimResult(ctx, badDeliveryID, 999001, 999002, "production")
	s.Require().NoError(err)
	_, err = s.conn.Exec(`
		UPDATE webhook_delivery_results
		SET claimed_at = NOW() AT TIME ZONE 'UTC' - INTERVAL '10 minutes'
		WHERE result_id = $1
	`, bad.ID)
	s.Require().NoError(err)

	// Healthy claim that must still be recovered in the same run.
	appl := s.MockApp(nil, nil)
	env := s.MockEnv(appl, nil)

	goodDeliveryID, err := store.UpsertDelivery(ctx, &webhookdelivery.Delivery{
		Provider:         webhookdelivery.ProviderGitHub,
		ProviderDelivery: "job-recovery-good",
		Repo:             appl.Repo,
		CheckoutRepo:     appl.Repo,
		Branch:           "main",
		EventType:        "commit",
		ChangesComplete:  true,
		Payload:          []byte(`{"ref":"refs/heads/main"}`),
	})
	s.Require().NoError(err)

	good, _, err := store.ClaimResult(ctx, goodDeliveryID, appl.ID, env.ID, "production")
	s.Require().NoError(err)
	_, err = s.conn.Exec(`
		UPDATE webhook_delivery_results
		SET claimed_at = NOW() AT TIME ZONE 'UTC' - INTERVAL '10 minutes'
		WHERE result_id = $1
	`, good.ID)
	s.Require().NoError(err)

	s.Require().NoError(jobs.RecoverWebhookDeliveries(ctx))

	s.mockDeployer.AssertNumberOfCalls(s.T(), "Deploy", 1)

	badResult, err := store.ResultByKey(ctx, badDeliveryID, 999001, 999002)
	s.Require().NoError(err)
	s.Equal(webhookdelivery.StatusRetryable, badResult.Status)

	goodResult, err := store.ResultByKey(ctx, goodDeliveryID, appl.ID, env.ID)
	s.Require().NoError(err)
	s.Equal(webhookdelivery.StatusSucceeded, goodResult.Status)
}

func TestJobWebhookDeliveriesSuite(t *testing.T) {
	suite.Run(t, &JobWebhookDeliveriesSuite{})
}
