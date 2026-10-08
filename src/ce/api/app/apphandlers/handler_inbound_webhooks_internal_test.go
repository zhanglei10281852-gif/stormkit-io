package apphandlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deploy/webhookdelivery"
	"github.com/stormkit-io/stormkit-io/src/lib/shttp"
)

func newWebhookRequest(t *testing.T, eventHeader string, body string) *shttp.RequestContext {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/app/webhooks/gitlab", strings.NewReader(body))

	if eventHeader != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	return shttp.NewRequestContext(req)
}

// Test_ProcessGitlabPayload_DeliveryIdentity verifies the GitLab event UUID is
// captured together with the exact request body, and a missing header means no
// delivery identity (legacy path).
func Test_ProcessGitlabPayload_DeliveryIdentity(t *testing.T) {
	const push = `{
		"object_kind": "push",
		"ref": "refs/heads/main",
		"project": {"path_with_namespace": "stormkit-test-acc/test-repo", "default_branch": "main"},
		"commits": [{"message": "Update something"}]
	}`

	t.Run("with event uuid", func(t *testing.T) {
		req := newWebhookRequest(t, "", push)
		req.Header.Set("X-Gitlab-Event", "Push Hook")
		req.Header.Set("X-Gitlab-Event-UUID", "  gitlab-guid-123  ")

		input, err := processGitlabPayload(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if input.Provider != webhookdelivery.ProviderGitLab {
			t.Fatalf("expected provider gitlab, got %q", input.Provider)
		}

		if input.deliveryID != "gitlab-guid-123" {
			t.Fatalf("expected trimmed delivery id, got %q", input.deliveryID)
		}

		if string(input.payloadRaw) != push {
			t.Fatal("payloadRaw must be the exact verified request body")
		}
	})

	t.Run("without event uuid", func(t *testing.T) {
		req := newWebhookRequest(t, "", push)
		req.Header.Set("X-Gitlab-Event", "Push Hook")

		input, err := processGitlabPayload(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if input.deliveryID != "" {
			t.Fatalf("expected empty delivery id, got %q", input.deliveryID)
		}

		if string(input.payloadRaw) != push {
			t.Fatal("payloadRaw must be captured even without a delivery id")
		}
	})
}

// Test_ProcessBitbucketPayload_DeliveryIdentity verifies the Bitbucket request
// UUID is captured as the delivery identity.
func Test_ProcessBitbucketPayload_DeliveryIdentity(t *testing.T) {
	const push = `{
		"repository": {"full_name": "stormkit-test/test-repo"},
		"push": {"changes": [{"new": {"type": "branch", "name": "main", "target": {"type": "commit", "message": "msg"}}}]}
	}`

	t.Run("with request uuid", func(t *testing.T) {
		req := newWebhookRequest(t, "", push)
		req.Header.Set("X-Event-Key", "repo:push")
		req.Header.Set("X-Request-UUID", "bitbucket-guid-456")

		input, err := processBitbucketPayload(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if input.Provider != webhookdelivery.ProviderBitbucket {
			t.Fatalf("expected provider bitbucket, got %q", input.Provider)
		}

		if input.deliveryID != "bitbucket-guid-456" {
			t.Fatalf("expected delivery id bitbucket-guid-456, got %q", input.deliveryID)
		}

		if string(input.payloadRaw) != push {
			t.Fatal("payloadRaw must be the exact request body")
		}
	})

	t.Run("without request uuid", func(t *testing.T) {
		req := newWebhookRequest(t, "", push)
		req.Header.Set("X-Event-Key", "repo:push")

		input, err := processBitbucketPayload(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if input.deliveryID != "" {
			t.Fatalf("expected empty delivery id, got %q", input.deliveryID)
		}
	})
}
