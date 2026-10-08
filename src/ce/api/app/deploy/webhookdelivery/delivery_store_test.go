package webhookdelivery_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deploy/webhookdelivery"
	"github.com/stormkit-io/stormkit-io/src/lib/database"
	"github.com/stormkit-io/stormkit-io/src/lib/database/databasetest"
	"github.com/stormkit-io/stormkit-io/src/lib/factory"
	"github.com/stormkit-io/stormkit-io/src/lib/types"
)

type DeliveryStoreSuite struct {
	suite.Suite
	*factory.Factory
	conn  databasetest.TestDB
	store *webhookdelivery.Store
	ctx   context.Context
}

func (s *DeliveryStoreSuite) BeforeTest(suiteName, _ string) {
	s.conn = databasetest.InitTx(suiteName)
	s.Factory = factory.New(s.conn)
	s.store = webhookdelivery.NewStore()
	s.ctx = context.Background()
}

// newDeploymentID creates a real deployment so the results table foreign key
// is satisfied.
func (s *DeliveryStoreSuite) newDeploymentID() types.ID {
	appl := s.MockApp(nil, nil)
	env := s.MockEnv(appl, nil)
	return s.MockDeployment(env, nil).ID
}

func (s *DeliveryStoreSuite) AfterTest(_, _ string) {
	s.conn.CloseTx()
}

func (s *DeliveryStoreSuite) sampleDelivery(provider, guid string) *webhookdelivery.Delivery {
	return &webhookdelivery.Delivery{
		Provider:          provider,
		ProviderDelivery:  guid,
		Repo:              provider + "/stormkit-io/test-repo",
		CheckoutRepo:      provider + "/stormkit-io/test-repo",
		Branch:            "main",
		Message:           "chore: first delivery",
		EventType:         "commit",
		CommitSha:         "abc123",
		PullRequestNumber: 0,
		IsFork:            false,
		ChangesComplete:   true,
		ChangedFiles:      []string{"apps/web/main.go", "README.md"},
		Payload:           []byte(`{"ref":"refs/heads/main"}`),
	}
}

func (s *DeliveryStoreSuite) Test_UpsertDelivery_FirstWriteWins() {
	d := s.sampleDelivery(webhookdelivery.ProviderGitHub, "guid-1")

	id, err := s.store.UpsertDelivery(s.ctx, d)
	s.Require().NoError(err)
	s.NotZero(id)

	stored, err := s.store.DeliveryByID(s.ctx, id)
	s.Require().NoError(err)
	s.Equal("github/stormkit-io/test-repo", stored.Repo)
	s.Equal("main", stored.Branch)
	s.Equal("abc123", stored.CommitSha)
	s.Equal([]string{"apps/web/main.go", "README.md"}, stored.ChangedFiles)
	s.JSONEq(`{"ref":"refs/heads/main"}`, string(stored.Payload))

	// A redelivery carries different content, but the first receipt wins.
	redelivery := s.sampleDelivery(webhookdelivery.ProviderGitHub, "guid-1")
	redelivery.Branch = "main-changed"
	redelivery.Message = "chore: rewritten"
	redelivery.Payload = []byte(`{"ref":"other"}`)

	sameID, err := s.store.UpsertDelivery(s.ctx, redelivery)
	s.Require().NoError(err)
	s.Equal(id, sameID)

	stored, err = s.store.DeliveryByID(s.ctx, id)
	s.Require().NoError(err)
	s.Equal("main", stored.Branch)
	s.Equal("chore: first delivery", stored.Message)
	s.JSONEq(`{"ref":"refs/heads/main"}`, string(stored.Payload))
}

// Test_UpsertDelivery_ProviderScopedKey verifies that the same GUID from two
// different providers is two independent events.
func (s *DeliveryStoreSuite) Test_UpsertDelivery_ProviderScopedKey() {
	first := s.sampleDelivery(webhookdelivery.ProviderGitHub, "shared-guid")
	second := s.sampleDelivery(webhookdelivery.ProviderGitLab, "shared-guid")

	firstID, err := s.store.UpsertDelivery(s.ctx, first)
	s.Require().NoError(err)

	secondID, err := s.store.UpsertDelivery(s.ctx, second)
	s.Require().NoError(err)

	s.NotEqual(firstID, secondID)
}

func (s *DeliveryStoreSuite) Test_ClaimResult_NewAndExisting() {
	d := s.sampleDelivery(webhookdelivery.ProviderGitHub, "claim-1")
	deliveryID, err := s.store.UpsertDelivery(s.ctx, d)
	s.Require().NoError(err)

	claimed, created, err := s.store.ClaimResult(s.ctx, deliveryID, 10, 20, "production")
	s.Require().NoError(err)
	s.True(created)
	s.Equal(webhookdelivery.StatusProcessing, claimed.Status)
	s.Zero(claimed.DeploymentID)
	s.Zero(claimed.Attempts)
	s.True(claimed.ClaimedAt.Valid)

	// Concurrent loser: the existing row comes back untouched.
	again, created, err := s.store.ClaimResult(s.ctx, deliveryID, 10, 20, "production")
	s.Require().NoError(err)
	s.False(created)
	s.Equal(claimed.ID, again.ID)
	s.Equal(webhookdelivery.StatusProcessing, again.Status)
	s.Zero(again.Attempts)

	missing, err := s.store.ResultByKey(s.ctx, deliveryID, 99, 99)
	s.Require().NoError(err)
	s.Nil(missing)

	// The (delivery, app, env) triple is unique at the database level. Done
	// last: the failed insert aborts the surrounding test transaction.
	_, err = s.conn.Exec(`
		INSERT INTO webhook_delivery_results (delivery_id, app_id, env_id, status)
		VALUES ($1, 10, 20, 'processing')
	`, deliveryID)
	s.Require().Error(err)
	s.True(database.IsDuplicate(err))
}

func (s *DeliveryStoreSuite) Test_ClaimResult_ReturnsSucceededRow() {
	d := s.sampleDelivery(webhookdelivery.ProviderGitLab, "claim-2")
	deliveryID, err := s.store.UpsertDelivery(s.ctx, d)
	s.Require().NoError(err)

	claimed, _, err := s.store.ClaimResult(s.ctx, deliveryID, 10, 20, "production")
	s.Require().NoError(err)

	deploymentID := s.newDeploymentID()
	s.Require().NoError(s.store.MarkSucceeded(s.ctx, claimed.ID, deploymentID))

	again, _, err := s.store.ClaimResult(s.ctx, deliveryID, 10, 20, "production")
	s.Require().NoError(err)
	s.Equal(webhookdelivery.StatusSucceeded, again.Status)
	s.Equal(deploymentID, again.DeploymentID)
}

func (s *DeliveryStoreSuite) Test_ReclaimResult_LeaseAndStatusGuards() {
	d := s.sampleDelivery(webhookdelivery.ProviderGitHub, "reclaim-1")
	deliveryID, err := s.store.UpsertDelivery(s.ctx, d)
	s.Require().NoError(err)

	claimed, _, err := s.store.ClaimResult(s.ctx, deliveryID, 10, 20, "production")
	s.Require().NoError(err)

	lease := time.Now().Add(-2 * time.Minute)

	// Fresh processing rows cannot be taken over.
	acquired, _, err := s.store.ReclaimResult(s.ctx, claimed.ID, lease)
	s.Require().NoError(err)
	s.False(acquired)

	// Expired the lease.
	_, err = s.conn.Exec(`
		UPDATE webhook_delivery_results
		SET claimed_at = NOW() AT TIME ZONE 'UTC' - INTERVAL '10 minutes'
		WHERE result_id = $1`, claimed.ID)
	s.Require().NoError(err)

	acquired, retaken, err := s.store.ReclaimResult(s.ctx, claimed.ID, lease)
	s.Require().NoError(err)
	s.True(acquired)
	s.Equal(1, retaken.Attempts)
	s.Equal(webhookdelivery.StatusProcessing, retaken.Status)
	s.False(retaken.NextRetryAt.Valid)

	// Retryable claims can always be taken; another attempt is counted.
	nextRetry := time.Now().Add(time.Minute)
	s.Require().NoError(s.store.MarkRetryable(s.ctx, claimed.ID, 0, "queue down", &nextRetry))

	acquired, retaken, err = s.store.ReclaimResult(s.ctx, claimed.ID, lease)
	s.Require().NoError(err)
	s.True(acquired)
	s.Equal(2, retaken.Attempts)

	// Succeeded claims are final.
	s.Require().NoError(s.store.MarkSucceeded(s.ctx, claimed.ID, s.newDeploymentID()))

	acquired, _, err = s.store.ReclaimResult(s.ctx, claimed.ID, lease)
	s.Require().NoError(err)
	s.False(acquired)
}

func (s *DeliveryStoreSuite) Test_MarkRetryable_PersistsFailureAndKeepsDeployment() {
	d := s.sampleDelivery(webhookdelivery.ProviderBitbucket, "retry-1")
	deliveryID, err := s.store.UpsertDelivery(s.ctx, d)
	s.Require().NoError(err)

	claimed, _, err := s.store.ClaimResult(s.ctx, deliveryID, 10, 20, "production")
	s.Require().NoError(err)

	deploymentID := s.newDeploymentID()

	nextRetry := time.Now().Add(2 * time.Minute)
	s.Require().NoError(s.store.MarkRetryable(s.ctx, claimed.ID, deploymentID, "enqueue failed", &nextRetry))

	r, err := s.store.ResultByKey(s.ctx, deliveryID, 10, 20)
	s.Require().NoError(err)
	s.Equal(webhookdelivery.StatusRetryable, r.Status)
	s.Equal(deploymentID, r.DeploymentID)
	s.Equal("enqueue failed", r.LastError.String)
	s.True(r.NextRetryAt.Valid)
	s.False(r.FinishedAt.Valid)

	// A later attempt without a deployment id must not erase the link.
	future := time.Now().Add(4 * time.Minute)
	s.Require().NoError(s.store.MarkRetryable(s.ctx, claimed.ID, 0, "still failing", &future))

	r, err = s.store.ResultByKey(s.ctx, deliveryID, 10, 20)
	s.Require().NoError(err)
	s.Equal(deploymentID, r.DeploymentID)

	// A redelivery reclaims the retryable claim (back to processing) before a
	// successful attempt can close it.
	acquired, _, err := s.store.ReclaimResult(s.ctx, claimed.ID, time.Now())
	s.Require().NoError(err)
	s.True(acquired)

	s.Require().NoError(s.store.MarkSucceeded(s.ctx, claimed.ID, deploymentID))

	// Terminal rows are never moved back to retryable.
	s.NoError(s.store.MarkRetryable(s.ctx, claimed.ID, deploymentID, "late failure", nil))

	r, err = s.store.ResultByKey(s.ctx, deliveryID, 10, 20)
	s.Require().NoError(err)
	s.Equal(webhookdelivery.StatusSucceeded, r.Status)
}

func (s *DeliveryStoreSuite) Test_DueResults_Selection() {
	d := s.sampleDelivery(webhookdelivery.ProviderGitHub, "due-1")
	deliveryID, err := s.store.UpsertDelivery(s.ctx, d)
	s.Require().NoError(err)

	// Helper to create a claim at arbitrary app/env triples.
	claim := func(appID, envID types.ID) types.ID {
		r, _, err := s.store.ClaimResult(s.ctx, deliveryID, appID, envID, "production")
		s.Require().NoError(err)
		return r.ID
	}

	staleProcessing := claim(1, 1)
	freshProcessing := claim(1, 2)
	dueRetryable := claim(1, 3)
	futureRetryable := claim(1, 4)
	exhaustedRetryable := claim(1, 5)
	succeeded := claim(1, 6)

	_, err = s.conn.Exec(`
		UPDATE webhook_delivery_results SET claimed_at = NOW() AT TIME ZONE 'UTC' - INTERVAL '10 minutes'
		WHERE result_id IN ($1, $2, $3, $4, $5)`,
		staleProcessing, dueRetryable, futureRetryable, exhaustedRetryable, succeeded)
	s.Require().NoError(err)

	past := time.Now().Add(-time.Minute)
	future := time.Now().Add(time.Hour)

	s.Require().NoError(s.store.MarkRetryable(s.ctx, dueRetryable, 0, "err", &past))
	s.Require().NoError(s.store.MarkRetryable(s.ctx, futureRetryable, 0, "err", &future))
	s.Require().NoError(s.store.MarkRetryable(s.ctx, exhaustedRetryable, 0, "err", &past))
	s.Require().NoError(s.store.MarkSucceeded(s.ctx, succeeded, s.newDeploymentID()))

	// exhaustedRetryable has already spent its attempts; bump it to the cap.
	_, err = s.conn.Exec(`UPDATE webhook_delivery_results SET attempts = 10 WHERE result_id = $1`, exhaustedRetryable)
	s.Require().NoError(err)

	due, err := s.store.DueResults(s.ctx, time.Now().Add(-2*time.Minute), time.Now(), 10, 50)
	s.Require().NoError(err)

	dueIDs := map[types.ID]bool{}
	for _, r := range due {
		dueIDs[r.ID] = true
	}

	s.True(dueIDs[staleProcessing], "stale processing claim is due")
	s.True(dueIDs[dueRetryable], "retryable claim with elapsed backoff is due")
	s.False(dueIDs[freshProcessing], "fresh processing claim is not due")
	s.False(dueIDs[futureRetryable], "retryable claim with future backoff is not due")
	s.False(dueIDs[exhaustedRetryable], "retryable claim at the attempt cap is not due")
	s.False(dueIDs[succeeded], "succeeded claim is never due")
	s.Len(due, 2)
}

func TestDeliveryStore(t *testing.T) {
	suite.Run(t, &DeliveryStoreSuite{})
}
