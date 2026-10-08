package apphandlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/stormkit-io/stormkit-io/src/ce/api/app/apphandlers"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deploy"
	"github.com/stormkit-io/stormkit-io/src/lib/shttp"
	"github.com/stormkit-io/stormkit-io/src/lib/shttp/shttptest"
	"gopkg.in/guregu/null.v3"
)

func gitlabRouter() http.Handler {
	return shttp.NewRouter().RegisterService(apphandlers.Services).Router().Handler()
}

// Test_Delivery_DuplicateDeploysOnce verifies GitLab deliveries carrying
// X-Gitlab-Event-UUID are deployed exactly once.
func (s *InboundGitlabSuite) Test_Delivery_DuplicateDeploysOnce() {
	appl := s.app(true)
	payload := s.pushPayload()

	post := func() shttptest.Response {
		return shttptest.RequestWithHeaders(
			gitlabRouter(),
			shttp.MethodPost,
			fmt.Sprintf("/app/webhooks/gitlab/%s", appl.Secret()),
			payload,
			map[string]string{
				"X-Gitlab-Event":      "Push Hook",
				"X-Gitlab-Event-UUID": "gitlab-delivery-1",
			},
		)
	}

	s.Equal(http.StatusOK, post().Code)
	s.Equal(http.StatusOK, post().Code)
	s.mockDeployer.AssertNumberOfCalls(s.T(), "Deploy", 1)
}

// Test_Delivery_AlreadyBuiltCommitReturns208 verifies the cross-delivery commit
// guard stays in effect on the claim path when a delivery UUID is present.
func (s *InboundGitlabSuite) Test_Delivery_AlreadyBuiltCommitReturns208() {
	appl := s.app(true)

	s.MockDeployment(s.GetEnv(), map[string]any{
		"Commit": deploy.CommitInfo{ID: null.NewString("123abc456FGH", true)},
	})

	payload := map[string]any{}
	s.Require().NoError(json.Unmarshal([]byte(gitlabMergeExample("opened")), &payload))

	response := shttptest.RequestWithHeaders(
		gitlabRouter(),
		shttp.MethodPost,
		fmt.Sprintf("/app/webhooks/gitlab/%s", appl.Secret()),
		payload,
		map[string]string{
			"X-Gitlab-Event":      "Merge Request Hook",
			"X-Gitlab-Event-UUID": "gitlab-delivery-2",
		},
	)

	s.Equal(http.StatusAlreadyReported, response.Code)
	s.mockDeployer.AssertNotCalled(s.T(), "Deploy")
	s.Empty(deliveryResultsFor(s.conn, "gitlab", "gitlab-delivery-2"))
}

// Test_Delivery_DuplicateDeploysOnce_Bitbucket verifies Bitbucket deliveries
// carrying X-Request-UUID are deployed exactly once.
func (s *InboundBitbucketSuite) Test_Delivery_DuplicateDeploysOnce() {
	appl := s.app(true)
	payload := s.pushPayload()

	post := func() shttptest.Response {
		return shttptest.RequestWithHeaders(
			shttp.NewRouter().RegisterService(apphandlers.Services).Router().Handler(),
			shttp.MethodPost,
			fmt.Sprintf("/app/webhooks/bitbucket/%s", appl.Secret()),
			payload,
			map[string]string{
				"X-Event-Key":   bitbucketPushEvent,
				"X-Request-UUID": "bitbucket-delivery-1",
			},
		)
	}

	s.Equal(http.StatusOK, post().Code)
	s.Equal(http.StatusOK, post().Code)
	s.mockDeployer.AssertNumberOfCalls(s.T(), "Deploy", 1)
}

// Test_Delivery_WithoutIdentityKeepsLegacy verifies a Bitbucket request without
// X-Request-UUID is unaffected by the claim path (legacy behaviour, no claims).
func (s *InboundBitbucketSuite) Test_Delivery_WithoutIdentityKeepsLegacy() {
	appl := s.app(true)

	response := s.post(fmt.Sprintf("/app/webhooks/bitbucket/%s", appl.Secret()), bitbucketPushEvent, s.pushPayload())

	s.Equal(http.StatusOK, response.Code)
	s.mockDeployer.AssertNumberOfCalls(s.T(), "Deploy", 1)
	s.Empty(deliveryResultsFor(s.conn, "bitbucket", "bitbucket-delivery-1"))
}
