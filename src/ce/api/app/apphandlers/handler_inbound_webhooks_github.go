package apphandlers

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/stormkit-io/stormkit-io/src/ce/api/admin"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deploy/webhookdelivery"
	"github.com/stormkit-io/stormkit-io/src/lib/shttp"
	"github.com/stormkit-io/stormkit-io/src/lib/slog"
	"github.com/stormkit-io/stormkit-io/src/lib/utils"
	"gopkg.in/go-playground/webhooks.v5/github"
)

var whiteList = []string{
	string(github.PushEvent),
	string(github.PullRequestEvent),
	string(github.IssueCommentEvent),
	string(github.CheckSuiteEvent),
}

const githubPushCommitLimit = 2048

// githubMaxPayloadSize is the largest payload GitHub delivers.
const githubMaxPayloadSize = 25 << 20

// ErrGithubWebhookSecretMissing is returned when no webhook secret is
// configured, since the payload cannot be verified without one.
var ErrGithubWebhookSecretMissing = errors.New("github webhook secret is not configured")

// readAndRestoreBody reads the verified request body and restores it for the
// provider parser, returning the exact bytes that were signed.
func readAndRestoreBody(req *shttp.RequestContext) ([]byte, error) {
	body, err := io.ReadAll(req.Body)

	if err != nil {
		return nil, err
	}

	req.Body = io.NopCloser(bytes.NewReader(body))

	return body, nil
}

// deliveryHeader trims the provider delivery identity. An empty (or
// whitespace-only) header is treated as a missing identity.
func deliveryHeader(value string) string {
	return strings.TrimSpace(value)
}

// github verifies the X-Hub-Signature-256 header against the configured
// webhook secret and restores the request body for parsing. It returns the
// verified body so callers persist the exact bytes that were signed.
func (v webhookVerifier) github() ([]byte, error) {
	secret := admin.MustConfig().GithubWebhookSecret()

	if secret == "" {
		return nil, ErrGithubWebhookSecretMissing
	}

	signature := v.req.Header.Get("X-Hub-Signature-256")

	// Unsigned requests are rejected before their body is read.
	if !strings.HasPrefix(signature, "sha256=") {
		return nil, ErrInvalidWebhookSecret
	}

	body, err := io.ReadAll(io.LimitReader(v.req.Body, githubMaxPayloadSize))

	if err != nil {
		return nil, err
	}

	v.req.Body = io.NopCloser(bytes.NewReader(body))

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)

	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(signature), []byte(expected)) {
		return nil, ErrInvalidWebhookSecret
	}

	return body, nil
}

// processGithubPayload processes a github payload and starts a new deployment.
func processGithubPayload(req *shttp.RequestContext) (*TriggerDeployInput, error) {
	body, err := (webhookVerifier{req: req}).github()

	if err != nil {
		slog.Infof("github webhook rejected: %s", err.Error())
		return nil, err
	}

	hook, _ := github.New()
	eventType := req.Header.Get("X-GitHub-Event")

	// Not sure why we receive this hook but it's not required for this endpoint
	if !utils.InSliceString(whiteList, eventType) {
		return nil, nil
	}

	payload, err := hook.Parse(
		req.Request,
		github.PushEvent,
		github.PullRequestEvent,
		github.IssueCommentEvent,
		// until we get answer from github
		// we will parse this and no-op
		github.CheckSuiteEvent,
	)

	if err != nil {
		slog.Errorf("Github request failed for event type=%s, err=%s", eventType, err.Error())
		return nil, nil
	}

	input := TriggerDeployInput{
		Provider:   webhookdelivery.ProviderGitHub,
		deliveryID: deliveryHeader(req.Header.Get("X-GitHub-Delivery")),
		payloadRaw: body,
		payload:    payload,
	}

	switch event := payload.(type) {

	// Commit event
	case github.PushPayload:
		input.Branch = strings.Replace(event.Ref, "refs/heads/", "", 1)
		input.Message = event.HeadCommit.Message
		input.Repo = fmt.Sprintf("github/%s", event.Repository.FullName)
		input.CheckoutRepo = fmt.Sprintf("github/%s", event.Repository.FullName)
		input.EventType = typeCommit
		input.IsFork = false
		input.ChangesComplete = len(event.Commits) > 0 && len(event.Commits) < githubPushCommitLimit

		for _, commit := range event.Commits {
			input.ChangedFiles = append(input.ChangedFiles, commit.Added...)
			input.ChangedFiles = append(input.ChangedFiles, commit.Modified...)
			input.ChangedFiles = append(input.ChangedFiles, commit.Removed...)
		}

		// Pushed something else: for instance a tag.
		if input.Message == "" {
			return nil, nil
		}

	// Pull request event
	case github.PullRequestPayload:
		input.Message = event.PullRequest.Title
		input.EventType = typePullRequest
		input.Repo = fmt.Sprintf("github/%s", event.Repository.FullName)
		input.CheckoutRepo = fmt.Sprintf("github/%s", event.PullRequest.Head.Repo.FullName)
		input.IsFork = !strings.EqualFold(event.PullRequest.Head.Repo.FullName, event.PullRequest.Base.Repo.FullName)
		input.PullRequestNumber = event.PullRequest.Number
		input.Branch = event.PullRequest.Head.Ref

		if event.Action != "opened" && event.Action != "synchronize" {
			return nil, nil
		}

	default:
		return nil, nil
	}

	if strings.Contains(input.Branch, "refs/tags/") {
		return nil, nil
	}

	return &input, nil
}
