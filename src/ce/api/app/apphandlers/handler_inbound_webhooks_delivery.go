package apphandlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/stormkit-io/stormkit-io/src/ce/api/admin"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deploy"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deploy/webhookdelivery"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deployservice"
	"github.com/stormkit-io/stormkit-io/src/ce/api/oauth/github"
	"github.com/stormkit-io/stormkit-io/src/lib/shttp"
	"github.com/stormkit-io/stormkit-io/src/lib/slog"
	"github.com/stormkit-io/stormkit-io/src/lib/types"
)

const (
	// webhookClaimLease is how long a processing claim is owned before a
	// duplicate delivery or the background job may take it over. HTTP
	// processing only inserts a row and enqueues a task, so this is well
	// beyond its normal duration.
	webhookClaimLease = 2 * time.Minute

	// webhookMaxAttempts caps automatic background retries. A provider
	// redelivery is always allowed to reclaim regardless of the cap.
	webhookMaxAttempts = 10

	// webhookRecoveryBatchSize bounds how many claims one recovery run takes.
	webhookRecoveryBatchSize = 50
)

// webhookNextRetry computes the backoff of an attempt: 1m, 2m, 4m, ... capped
// at one hour.
func webhookNextRetry(attempts int) time.Time {
	backoff := time.Minute

	for i := 0; i < attempts && backoff < time.Hour; i++ {
		backoff *= 2
	}

	if backoff > time.Hour {
		backoff = time.Hour
	}

	return time.Now().UTC().Add(backoff)
}

func webhookAccepted() *shttp.Response {
	return &shttp.Response{Status: http.StatusAccepted}
}

func webhookRetryable() *shttp.Response {
	return &shttp.Response{
		Status: http.StatusServiceUnavailable,
		Data: map[string]any{
			"error": "deployment is being processed or temporarily unavailable; redeliver to retry",
		},
	}
}

// triggerDeployWithClaims is the idempotent path for events carrying a
// provider delivery identity. Every matched application/environment gets its
// own persistent claim, and deployments are created at most once per claim.
func triggerDeployWithClaims(ctx context.Context, input TriggerDeployInput) *shttp.Response {
	claimStore := webhookdelivery.NewStore()

	deliveryID, err := claimStore.UpsertDelivery(ctx, deliveryFromInput(input))

	if err != nil {
		slog.Errorf("could not persist webhook delivery %s/%s: %v", input.Provider, input.deliveryID, err)
		return webhookRetryable()
	}

	// Cross-delivery guard: GitLab can deliver the same commit under a new
	// event UUID (for example an event triggered by an earlier webhook), which
	// the per-delivery claims cannot dedupe. The guard keeps the legacy
	// semantics; within a single delivery the multi-environment fan-out below
	// happens before any deployment exists.
	if alreadyBuilt, err := commitHasBeenBuilt(ctx, input); err != nil {
		return shttp.Error(err)
	} else if alreadyBuilt {
		return &shttp.Response{Status: http.StatusAlreadyReported}
	}

	candidates, err := app.NewStore().DeployCandidates(ctx, input.Repo)

	if err != nil {
		return shttp.Error(err, fmt.Sprintf("error while fetching deploy candidates: %s", err.Error()))
	}

	outcome := processDeployCandidates(ctx, input, claimStore, deliveryID, FilterDeployCandidates(input, candidates), time.Now().UTC())

	return outcome.response()
}

// claimOutcome aggregates what happened to the matched claims of one delivery.
type claimOutcome struct {
	succeeded  int
	inProgress int
	failed     int
}

func (o claimOutcome) response() *shttp.Response {
	if o.failed > 0 {
		return webhookRetryable()
	}

	if o.inProgress > 0 {
		return webhookAccepted()
	}

	if o.succeeded > 0 {
		return shttp.OK()
	}

	return shttp.NoContent()
}

// processDeployCandidates claims and deploys every matched candidate. It is
// shared verbatim by the HTTP path and the background recovery path when a
// missing deployment has to be recreated.
func processDeployCandidates(ctx context.Context, input TriggerDeployInput, claimStore *webhookdelivery.Store, deliveryID types.ID, candidates []*app.DeployCandidate, now time.Time) claimOutcome {
	var outcome claimOutcome

	for _, candidate := range candidates {
		if input.AppID != 0 && candidate.ID != input.AppID {
			continue
		}

		result, created, err := claimStore.ClaimResult(ctx, deliveryID, candidate.ID, candidate.EnvID, candidate.EnvName)

		if err != nil {
			slog.Errorf("could not claim delivery %d for app %d env %d: %v", deliveryID, candidate.ID, candidate.EnvID, err)
			outcome.failed++

			continue
		}

		// This request inserted the claim, so it owns it regardless of the
		// processing status it was created with.
		if created {
			if err := processClaim(ctx, input, claimStore, candidate, result); err != nil {
				outcome.failed++

				continue
			}

			outcome.succeeded++

			continue
		}

		if result.Status == webhookdelivery.StatusSucceeded {
			outcome.succeeded++

			continue
		}

		// A fresh processing claim is owned by another in-flight request.
		if result.Status == webhookdelivery.StatusProcessing && claimIsFresh(result, now) {
			outcome.inProgress++

			continue
		}

		// Retryable claims and expired leases are taken over here.
		acquired, retaken, err := claimStore.ReclaimResult(ctx, result.ID, now.Add(-webhookClaimLease))

		if err != nil {
			slog.Errorf("could not reclaim result %d: %v", result.ID, err)
			outcome.failed++

			continue
		}

		if !acquired {
			outcome.inProgress++

			continue
		}

		if err := resumeClaimedResult(ctx, claimStore, retaken, &input, candidate); err != nil {
			outcome.failed++

			continue
		}

		outcome.succeeded++
	}

	return outcome
}

// claimIsFresh reports whether a processing claim is still within its lease.
func claimIsFresh(result *webhookdelivery.Result, now time.Time) bool {
	return result.ClaimedAt.Valid && result.ClaimedAt.Time.After(now.Add(-webhookClaimLease))
}

// processClaim creates the deployment for one claimed candidate and dispatches
// it. The claim is the only thing the caller owns: on failure it becomes
// retryable with whatever deployment id was obtained, so a retry never inserts
// a second deployment.
func processClaim(ctx context.Context, input TriggerDeployInput, claimStore *webhookdelivery.Store, candidate *app.DeployCandidate, result *webhookdelivery.Result) error {
	depl := deploy.New(candidate.App)
	depl.PopulateFromDeployCandidate(candidate, deploy.DeployCandidatePayload{
		Branch:            input.Branch,
		CommitSha:         input.CommitSha,
		WebhookEvent:      input.payload,
		CheckoutRepo:      input.CheckoutRepo,
		IsFork:            input.IsFork,
		PullRequestNumber: input.PullRequestNumber,
	})

	if candidate.EnvDefaultBranch != input.Branch {
		depl.ShouldPublish = false
	}

	depl.DeliveryResultID = result.ID

	if err := deployservice.New().Deploy(ctx, candidate.App, depl); err != nil {
		markClaimRetryable(ctx, claimStore, result, depl.ID, err)

		return err
	}

	if err := claimStore.MarkSucceeded(ctx, result.ID, depl.ID); err != nil {
		// The deployment exists and was dispatched; the lease reaper closes
		// the claim. Tell the provider to redeliver so this request is not
		// acknowledged as finished.
		markClaimRetryable(ctx, claimStore, result, depl.ID, err)

		return err
	}

	createGithubPendingStatus(ctx, candidate.App, depl)

	return nil
}

// markClaimRetryable records a recoverable failure. When the deployment insert
// is already linked, or discoverable through the claim column, that id is kept
// so recovery only re-dispatches instead of inserting again.
func markClaimRetryable(ctx context.Context, claimStore *webhookdelivery.Store, result *webhookdelivery.Result, deploymentID types.ID, cause error) {
	slog.Errorf("auto deployment failed for app %d env %d claim %d: %v", result.AppID, result.EnvID, result.ID, cause)

	if deploymentID == 0 {
		if existing, err := deploy.NewStore().DeploymentByDeliveryResult(ctx, result.ID); err == nil && existing != nil {
			deploymentID = existing.ID
		}
	}

	nextRetry := webhookNextRetry(result.Attempts)

	if err := claimStore.MarkRetryable(ctx, result.ID, deploymentID, cause.Error(), &nextRetry); err != nil {
		slog.Errorf("could not mark claim %d retryable: %v", result.ID, err)
	}
}

// createGithubPendingStatus posts the pending commit status for GitHub repos,
// best-effort and only when the deployment was actually dispatched.
func createGithubPendingStatus(ctx context.Context, a *app.App, depl *deploy.Deployment) {
	cnf := admin.MustConfig()

	if a.IsGithub() && cnf.IsGithubEnabled() {
		if err := github.CreateStatus(a.Repo, depl.Branch, cnf.DeploymentLogsURL(depl.AppID, depl.ID), github.StatusPending); err != nil {
			slog.Errorf("error while updating github status: %s", err.Error())
		}
	}
}

func deliveryFromInput(input TriggerDeployInput) *webhookdelivery.Delivery {
	return &webhookdelivery.Delivery{
		Provider:          input.Provider,
		ProviderDelivery:  input.deliveryID,
		Repo:              input.Repo,
		CheckoutRepo:      input.CheckoutRepo,
		Branch:            input.Branch,
		Message:           input.Message,
		EventType:         input.EventType,
		CommitSha:         input.CommitSha,
		PullRequestNumber: input.PullRequestNumber,
		IsFork:            input.IsFork,
		ChangesComplete:   input.ChangesComplete,
		ChangedFiles:      input.ChangedFiles,
		Payload:           input.payloadRaw,
	}
}

func triggerInputFromDelivery(d *webhookdelivery.Delivery) TriggerDeployInput {
	var event any

	if len(d.Payload) > 0 {
		var parsed any

		if err := json.Unmarshal(d.Payload, &parsed); err == nil {
			event = parsed
		}
	}

	return TriggerDeployInput{
		Provider:          d.Provider,
		Repo:              d.Repo,
		CheckoutRepo:      d.CheckoutRepo,
		Branch:            d.Branch,
		Message:           d.Message,
		EventType:         d.EventType,
		CommitSha:         d.CommitSha,
		PullRequestNumber: d.PullRequestNumber,
		IsFork:            d.IsFork,
		ChangesComplete:   d.ChangesComplete,
		ChangedFiles:      d.ChangedFiles,
		payload:           event,
		deliveryID:        d.ProviderDelivery,
		payloadRaw:        d.Payload,
	}
}

// RecoverPendingWebhookDeliveries resumes stale processing claims and due
// retryable claims. It runs on the leader worker every minute but is safe to
// invoke from a redelivery too: every transition is claim-gated.
func RecoverPendingWebhookDeliveries(ctx context.Context) error {
	claimStore := webhookdelivery.NewStore()
	now := time.Now().UTC()

	results, err := claimStore.DueResults(ctx, now.Add(-webhookClaimLease), now, webhookMaxAttempts, webhookRecoveryBatchSize)

	if err != nil {
		return err
	}

	for _, result := range results {
		if err := recoverClaim(ctx, claimStore, result, now); err != nil {
			slog.Errorf("could not recover webhook claim %d: %v", result.ID, err)
		}
	}

	return nil
}

// recoverClaim handles a single due claim. A deployment that already exists is
// only re-dispatched; otherwise the stored event is rebuilt and replayed for
// the claim's app/environment.
func recoverClaim(ctx context.Context, claimStore *webhookdelivery.Store, due *webhookdelivery.Result, now time.Time) error {
	acquired, result, err := claimStore.ReclaimResult(ctx, due.ID, now.Add(-webhookClaimLease))

	if err != nil {
		return err
	}

	if !acquired {
		return nil
	}

	// Background recovery passes no candidate: resumeClaimedResult rebuilds the
	// stored event only when the deployment does not exist yet.
	return resumeClaimedResult(ctx, claimStore, result, nil, nil)
}

// resumeClaimedResult finishes a claim after ownership was taken. When the
// deployment already exists (the insert succeeded before a crash or a failed
// dispatch), it is re-dispatched only and no second deployment is ever
// inserted. Otherwise the candidate's event is deployed. The HTTP path passes
// the already-resolved candidate and input; recovery passes nil for both and
// the stored event is rebuilt lazily.
func resumeClaimedResult(ctx context.Context, claimStore *webhookdelivery.Store, result *webhookdelivery.Result, input *TriggerDeployInput, candidate *app.DeployCandidate) error {
	depl, err := deploymentForClaim(ctx, result)

	if err != nil {
		return err
	}

	if depl != nil {
		a := candidateApp(candidate, depl.AppID)

		if a == nil {
			a, err = app.NewStore().AppByID(ctx, depl.AppID)

			if err != nil {
				markClaimRetryable(ctx, claimStore, result, depl.ID, err)

				return err
			}
		}

		if a == nil {
			markClaimRetryable(ctx, claimStore, result, depl.ID, errors.New("application no longer exists"))

			return nil
		}

		if err := deployservice.New().Dispatch(ctx, a, depl); err != nil {
			markClaimRetryable(ctx, claimStore, result, depl.ID, err)

			return err
		}

		return claimStore.MarkSucceeded(ctx, result.ID, depl.ID)
	}

	if candidate == nil {
		recoveredInput, recoveredCandidate, err := resolveRecoveryCandidate(ctx, claimStore, result)

		if err != nil {
			return err
		}

		if recoveredCandidate == nil {
			// The app/environment no longer matches the stored event (deleted,
			// auto deploy disabled, branch config changed). Retry with backoff
			// until the attempt cap; a provider redelivery can still reclaim.
			markClaimRetryable(ctx, claimStore, result, 0, errors.New("stored webhook event no longer matches an application/environment"))

			return nil
		}

		input = recoveredInput
		candidate = recoveredCandidate
	}

	return processClaim(ctx, *input, claimStore, candidate, result)
}

// candidateApp returns the candidate's app when it belongs to appID, so the
// dispatch reuses the app loaded for the current request instead of reloading.
func candidateApp(candidate *app.DeployCandidate, appID types.ID) *app.App {
	if candidate != nil && candidate.ID == appID {
		return candidate.App
	}

	return nil
}

// resolveRecoveryCandidate rebuilds the stored event and finds the claim's
// current candidate. A nil candidate with no error means the event no longer
// matches anything.
func resolveRecoveryCandidate(ctx context.Context, claimStore *webhookdelivery.Store, result *webhookdelivery.Result) (*TriggerDeployInput, *app.DeployCandidate, error) {
	delivery, err := claimStore.DeliveryByID(ctx, result.DeliveryID)

	if err != nil {
		return nil, nil, err
	}

	if delivery == nil {
		return nil, nil, errors.New("webhook delivery disappeared for a due claim")
	}

	input := triggerInputFromDelivery(delivery)

	candidates, err := app.NewStore().DeployCandidates(ctx, input.Repo)

	if err != nil {
		return nil, nil, err
	}

	candidate := findCandidate(FilterDeployCandidates(input, candidates), result.AppID, result.EnvID)

	return &input, candidate, nil
}

// deploymentForClaim loads the deployment linked to a claim, checking the
// claim column first and the deployment column second to cover the crash
// window between the insert and the claim update.
func deploymentForClaim(ctx context.Context, result *webhookdelivery.Result) (*deploy.Deployment, error) {
	store := deploy.NewStore()

	if result.DeploymentID != 0 {
		depl, err := store.DeploymentForDispatch(ctx, result.DeploymentID)

		if err != nil {
			return nil, err
		}

		if depl != nil {
			return depl, nil
		}
	}

	return store.DeploymentByDeliveryResult(ctx, result.ID)
}

// findCandidate returns the deploy candidate for a given app/environment pair.
func findCandidate(candidates []*app.DeployCandidate, appID, envID types.ID) *app.DeployCandidate {
	for _, candidate := range candidates {
		if candidate.ID == appID && candidate.EnvID == envID {
			return candidate
		}
	}

	return nil
}
