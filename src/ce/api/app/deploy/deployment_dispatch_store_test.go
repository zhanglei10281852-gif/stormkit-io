package deploy_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deploy"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deploy/webhookdelivery"
	"github.com/stormkit-io/stormkit-io/src/lib/database"
	"github.com/stormkit-io/stormkit-io/src/lib/database/databasetest"
	"github.com/stormkit-io/stormkit-io/src/lib/factory"
	"github.com/stormkit-io/stormkit-io/src/lib/types"
)

type DeploymentDispatchSuite struct {
	suite.Suite
	*factory.Factory
	conn databasetest.TestDB
}

func (s *DeploymentDispatchSuite) BeforeTest(suiteName, _ string) {
	s.conn = databasetest.InitTx(suiteName)
	s.Factory = factory.New(s.conn)
}

func (s *DeploymentDispatchSuite) AfterTest(_, _ string) {
	s.conn.CloseTx()
}

// claim creates a delivery with one result row and returns the result id.
func (s *DeploymentDispatchSuite) claim(appID, envID types.ID) types.ID {
	deliveryID, err := webhookdelivery.NewStore().UpsertDelivery(context.Background(), &webhookdelivery.Delivery{
		Provider:         webhookdelivery.ProviderGitHub,
		ProviderDelivery: "dispatch-delivery",
		Repo:             "github/stormkit-test-acc/test-repo",
	})
	s.Require().NoError(err)

	result, _, err := webhookdelivery.NewStore().ClaimResult(context.Background(), deliveryID, appID, envID, "production")
	s.Require().NoError(err)

	return result.ID
}

// Test_InsertDeployment_DeliveryResultUnique verifies the partial unique index
// enforces a single deployment per delivery claim at the database level.
func (s *DeploymentDispatchSuite) Test_InsertDeployment_DeliveryResultUnique() {
	a := assert.New(s.T())
	appl := s.MockApp(nil, nil)
	env := s.MockEnv(appl, nil)

	resultID := s.claim(appl.ID, env.ID)

	first := deploy.New(appl.App)
	first.EnvID = env.ID
	first.Env = env.Name
	first.Branch = "main"
	first.CheckoutRepo = appl.Repo
	first.IsAutoDeploy = true
	first.DeliveryResultID = resultID

	a.NoError(deploy.NewStore().InsertDeployment(context.Background(), first))

	second := deploy.New(appl.App)
	second.EnvID = env.ID
	second.Env = env.Name
	second.Branch = "main"
	second.CheckoutRepo = appl.Repo
	second.IsAutoDeploy = true
	second.DeliveryResultID = resultID

	err := deploy.NewStore().InsertDeployment(context.Background(), second)
	a.Error(err)
	a.True(database.IsDuplicate(err))
}

// Test_InsertDeployment_SoftDeletedFreesClaim verifies the partial unique
// index ignores soft-deleted deployments, so a claim may deploy again after a
// deployment was deleted.
func (s *DeploymentDispatchSuite) Test_InsertDeployment_SoftDeletedFreesClaim() {
	appl := s.MockApp(nil, nil)
	env := s.MockEnv(appl, nil)

	resultID := s.claim(appl.ID, env.ID)

	first := deploy.New(appl.App)
	first.EnvID = env.ID
	first.Env = env.Name
	first.Branch = "main"
	first.CheckoutRepo = appl.Repo
	first.IsAutoDeploy = true
	first.DeliveryResultID = resultID
	s.Require().NoError(deploy.NewStore().InsertDeployment(context.Background(), first))

	_, err := s.conn.Exec(`
		UPDATE deployments SET deleted_at = NOW() AT TIME ZONE 'UTC'
		WHERE deployment_id = $1
	`, first.ID)
	s.Require().NoError(err)

	second := deploy.New(appl.App)
	second.EnvID = env.ID
	second.Env = env.Name
	second.Branch = "main"
	second.CheckoutRepo = appl.Repo
	second.IsAutoDeploy = true
	second.DeliveryResultID = resultID

	s.Require().NoError(deploy.NewStore().InsertDeployment(context.Background(), second))
	s.NotEqual(first.ID, second.ID)
}

// Test_InsertDeployment_WithoutClaim verifies that deployments not tied to a
// webhook claim keep a null delivery result (manual deploys, factories).
func (s *DeploymentDispatchSuite) Test_InsertDeployment_WithoutClaim() {
	appl := s.MockApp(nil, nil)
	env := s.MockEnv(appl, nil)
	d := s.MockDeployment(env, nil)

	var deliveryResultID any
	err := s.conn.QueryRow(`
		SELECT delivery_result_id FROM deployments WHERE deployment_id = $1
	`, d.ID).Scan(&deliveryResultID)

	s.Require().NoError(err)
	s.Nil(deliveryResultID)
}

// Test_DeploymentForDispatch restores every field the queue message needs,
// including the build config frozen in the config snapshot.
func (s *DeploymentDispatchSuite) Test_DeploymentForDispatch() {
	appl := s.MockApp(nil, nil)
	env := s.MockEnv(appl, nil)
	d := s.MockDeployment(env, map[string]any{
		"Branch":        "release/x",
		"ShouldPublish": true,
	})

	// is_priority is not part of the factory insert; set it directly.
	_, err := s.conn.Exec(`UPDATE deployments SET is_priority = TRUE WHERE deployment_id = $1`, d.ID)
	s.Require().NoError(err)

	loaded, err := deploy.NewStore().DeploymentForDispatch(context.Background(), d.ID)
	s.Require().NoError(err)
	s.Require().NotNil(loaded)

	s.Equal(d.ID, loaded.ID)
	s.Equal(appl.ID, loaded.AppID)
	s.Equal(env.ID, loaded.EnvID)
	s.Equal(env.Name, loaded.Env)
	s.Equal("release/x", loaded.Branch)
	s.True(loaded.IsPriority)
	s.True(loaded.ShouldPublish)
	s.Require().NotNil(loaded.BuildConfig)
	s.Equal("npm run build", loaded.BuildConfig.BuildCmd)

	missing, err := deploy.NewStore().DeploymentForDispatch(context.Background(), types.ID(999999))
	s.Require().NoError(err)
	s.Nil(missing)
}

// Test_DeploymentByDeliveryResult finds the deployment through its claim, which
// is how crash recovery detects that an insert already happened.
func (s *DeploymentDispatchSuite) Test_DeploymentByDeliveryResult() {
	appl := s.MockApp(nil, nil)
	env := s.MockEnv(appl, nil)
	d := s.MockDeployment(env, nil)

	resultID := s.claim(appl.ID, env.ID)

	// The factory insert predates the claim, so the link is established here,
	// mirroring what happened when InsertDeployment carried the claim id.
	_, err := s.conn.Exec(`
		UPDATE deployments SET delivery_result_id = $1 WHERE deployment_id = $2
	`, resultID, d.ID)
	s.Require().NoError(err)

	loaded, err := deploy.NewStore().DeploymentByDeliveryResult(context.Background(), resultID)
	s.Require().NoError(err)
	s.Require().NotNil(loaded)
	s.Equal(d.ID, loaded.ID)
	s.Equal(appl.ID, loaded.AppID)

	missing, err := deploy.NewStore().DeploymentByDeliveryResult(context.Background(), resultID+99999)
	s.Require().NoError(err)
	s.Nil(missing)
}

func TestDeploymentDispatchSuite(t *testing.T) {
	suite.Run(t, new(DeploymentDispatchSuite))
}
