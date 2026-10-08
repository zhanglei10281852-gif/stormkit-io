package deployservice

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/hibiken/asynq"
	"github.com/stormkit-io/stormkit-io/src/ce/api/admin"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/buildconf"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/deploy"
	"github.com/stormkit-io/stormkit-io/src/ce/api/user"
	"github.com/stormkit-io/stormkit-io/src/lib/config"
	"github.com/stormkit-io/stormkit-io/src/lib/slog"
	"github.com/stormkit-io/stormkit-io/src/lib/tasks"
	"github.com/stormkit-io/stormkit-io/src/lib/utils"
	"go.uber.org/zap"
	"gopkg.in/guregu/null.v3"
)

var ErrBuildMinutesExceeded = fmt.Errorf("build minutes limit exceeded")

type Deployer interface {
	Deploy(context.Context, *app.App, *deploy.Deployment) error

	// Dispatch sends an already persisted deployment to the build queue. It is
	// the recovery half of Deploy: webhook delivery claims call it when the
	// deployment row exists but the queue dispatch may not have happened.
	Dispatch(context.Context, *app.App, *deploy.Deployment) error
}

type DefaultDeployer struct {
}

var MockDeployer Deployer

func New() Deployer {
	if MockDeployer != nil {
		return MockDeployer
	}

	return &DefaultDeployer{}
}

// Deploy creates the deployment and dispatches it to the build queue.
func (dd *DefaultDeployer) Deploy(ctx context.Context, a *app.App, d *deploy.Deployment) error {
	if err := dd.createDeployment(ctx, a, d); err != nil {
		return err
	}

	return dd.dispatchDeployment(ctx, a, d)
}

// Dispatch re-dispatches an existing deployment without inserting a new row.
func (dd *DefaultDeployer) Dispatch(ctx context.Context, a *app.App, d *deploy.Deployment) error {
	return dd.dispatchDeployment(ctx, a, d)
}

// createDeployment runs every check the deploy needs, snapshots the config and
// inserts the deployment row. Nothing here reaches the build queue yet.
func (dd *DefaultDeployer) createDeployment(ctx context.Context, a *app.App, d *deploy.Deployment) error {
	// Build minutes are enforced on Stormkit Cloud before anything occupies a
	// runner or a queue slot.
	if config.IsStormkitCloud() {
		store := user.NewStore()
		usr, err := store.UserMetrics(ctx, user.UserMetricsArgs{AppID: a.ID})

		if err != nil {
			return err
		}

		if usr != nil && !usr.HasBuildMinutes() {
			return ErrBuildMinutesExceeded
		}
	}

	// Rejected here rather than on the build host: the provider already knows
	// how large the repository is, so an oversized one never occupies a
	// runner, a slot in the queue, or any bandwidth.
	if err := (repoSizeChecker{app: a}).check(); err != nil {
		return err
	}

	d.ConfigCopy, _ = d.MarshalConfigSnapshot()

	if d.BuildConfig == nil {
		d.BuildConfig = &buildconf.BuildConf{}
	}

	d.APIPathPrefix = null.NewString(
		utils.TrimPath(
			utils.GetString(
				d.BuildConfig.APIPathPrefix,
				d.BuildConfig.APIFolder,
				"api",
			),
		),
		true,
	)

	if !d.IsRestart {
		if d.IsAutoDeploy && d.BuildConfig != nil && d.BuildConfig.PriorityPattern != "" {
			if matched, _ := regexp.MatchString(d.BuildConfig.PriorityPattern, d.Commit.Message.ValueOrZero()); matched {
				d.IsPriority = true
			}
		}

		store := deploy.NewStore()

		// Insert the deployment first, so we can have an ID.
		if err := store.InsertDeployment(ctx, d); err != nil {
			return err
		}
	}

	return nil
}

// dispatchDeployment builds the runner message for an already persisted
// deployment and enqueues it. It is self-contained so a crash-recovery
// re-dispatch does not have to replay the create-phase checks or insert a
// second deployment row.
func (dd *DefaultDeployer) dispatchDeployment(ctx context.Context, a *app.App, d *deploy.Deployment) error {
	// Get git credentials
	gitCreds, err := a.GitCreds(ctx)

	if err != nil {
		return err
	}

	// The credentials grant access to the app's repository. Never hand them to
	// a build that checks out a different one, such as a fork.
	if d.CheckoutRepo != "" && !strings.EqualFold(d.CheckoutRepo, a.Repo) {
		gitCreds = ""
	}

	if d.BuildConfig == nil {
		d.BuildConfig = &buildconf.BuildConf{}
	}

	// When caching is not allowed the runner receives no cache directories,
	// which is how it decides whether to restore and snapshot the cache.
	cacheDirs := d.BuildConfig.CacheDirs

	if !cacheEnabledForDeploy(ctx, a) {
		cacheDirs = nil
	}

	payload := DeploymentMessage{
		Client: ClientConfig{
			Repo:        d.RepoCloneURL(),
			Slug:        d.RepoSlug(),
			AccessToken: gitCreds,
		},

		Build: BuildConfig{
			Env:              d.Env,
			Branch:           d.Branch,
			ShouldPublish:    d.ShouldPublish,
			BuildCmd:         d.BuildConfig.BuildCmd,
			ServerCmd:        d.BuildConfig.ServerCmd,
			InstallCmd:       d.BuildConfig.InstallCmd,
			WorkDir:          d.BuildConfig.WorkDir,
			DistFolder:       d.BuildConfig.DistFolder,
			DeploymentID:     d.ID.String(),
			EnvID:            d.EnvID.String(),
			AppID:            d.AppID.String(),
			HeadersFile:      d.BuildConfig.HeadersFile,
			RedirectsFile:    d.BuildConfig.RedirectsFile,
			APIFolder:        utils.GetString(d.BuildConfig.APIFolder, "/api"),
			StatusChecks:     d.BuildConfig.StatusChecks,
			MigrationsFolder: d.MigrationsFolder.ValueOrZero(),
			CacheDirs:        cacheDirs,
			Vars: d.BuildConfig.InterpolatedVars(
				buildconf.InterpolatedVarsOpts{
					DeploymentID: d.ID.String(),
					AppID:        d.AppID.String(),
					EnvID:        d.EnvID.String(),
					Env:          d.Env,
					DisplayName:  a.DisplayName,
				},
			),
		},

		Config: &RunnerSettings{
			RunnerConfig: *config.Get().Runner,
			AutoInstall:  admin.MustConfig().IsAutoInstallEnabled(),
		},
	}

	queue := tasks.QueueDeployService

	if d.IsPriority {
		queue = tasks.QueueDeployServicePriority
	}

	return dd.sendPayloadToRedis(ctx, sendPayloadToRedisParams{message: payload, queue: queue})
}

// cacheEnabledForDeploy reports whether the deployment may use build caching.
// Everyone on self-hosted may; on Stormkit Cloud it is a premium and ultimate
// feature. A failed entitlement lookup disables caching instead of failing the
// deployment, because caching is best-effort.
func cacheEnabledForDeploy(ctx context.Context, a *app.App) bool {
	if !config.IsStormkitCloud() {
		return true
	}

	store := user.NewStore()
	tier := ""

	if usr, err := store.UserMetrics(ctx, user.UserMetricsArgs{AppID: a.ID}); err != nil {
		slog.Errorf("could not resolve package for app %d: %v", a.ID, err)
	} else if usr != nil {
		tier = usr.Metadata.PackageName
	}

	if tier == "" {
		if t, err := store.PackageNameByAppID(ctx, a.ID); err != nil {
			slog.Errorf("could not resolve package for app %d: %v", a.ID, err)
		} else {
			tier = t
		}
	}

	return tier == config.PackagePremium || tier == config.PackageUltimate
}

type sendPayloadToRedisParams struct {
	message DeploymentMessage
	queue   string
}

func (dd *DefaultDeployer) sendPayloadToRedis(ctx context.Context, p sendPayloadToRedisParams) error {
	encrypted, err := p.message.Encrypt()

	if err != nil {
		return err
	}

	if p.queue == "" {
		p.queue = tasks.QueueDeployService
	}

	info, err := tasks.Enqueue(ctx, tasks.DeploymentStart, encrypted, &tasks.EnqueueOptions{
		MaxRetry:  10,
		QueueName: p.queue,
		TaskID:    fmt.Sprintf("deployment-%s", p.message.Build.DeploymentID),
	})

	// A task with this deployment id is already queued or running, so the
	// dispatch happened on an earlier attempt. Treat it as success rather than
	// starting anything twice.
	if errors.Is(err, asynq.ErrTaskIDConflict) {
		return nil
	}

	if err != nil {
		slog.Errorf("could not enqueue task: %v", err)
	}

	if info != nil {
		slog.Debug(slog.LogOpts{
			Msg:   "enqueued task",
			Level: slog.DL2,
			Payload: []zap.Field{
				zap.String("task_id", info.ID),
				zap.String("queue", info.Queue),
			},
		})
	}

	return err
}
