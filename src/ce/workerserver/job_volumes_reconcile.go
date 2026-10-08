package jobs

import (
	"context"

	"github.com/stormkit-io/stormkit-io/src/ce/api/admin"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/volumes"
)

// ReconcileVolumeOps finishes volume file operations interrupted by a
// crashed or failed request: aborted uploads have their staged objects
// reaped, committed replacements have their displaced objects removed and
// failed deletes are retried. Runs on the elected master so it is safe with
// live uploads and deletes.
func ReconcileVolumeOps(ctx context.Context) error {
	cfg, err := admin.Store().Config(ctx)

	if err != nil {
		return err
	}

	if cfg.VolumesConfig == nil {
		return nil
	}

	return volumes.ReconcileOps(ctx, cfg.VolumesConfig)
}
