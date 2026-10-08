package volumes

import (
	"context"
	"time"

	"github.com/stormkit-io/stormkit-io/src/ce/api/admin"
	"github.com/stormkit-io/stormkit-io/src/lib/slog"
	"github.com/stormkit-io/stormkit-io/src/lib/types"
	"go.uber.org/zap"
)

var (
	// reconcilePendingGrace is how long an upload may remain in progress
	// before reconciliation treats it as interrupted. It must comfortably
	// exceed the slowest allowed upload (50MB); it is a variable so tests do
	// not wait for it.
	reconcilePendingGrace = 15 * time.Minute

	// reconcileBatchSize bounds how many rows one reconciliation pass handles.
	reconcileBatchSize = 100

	// reconcileMaxPasses bounds a single run; the scheduler re-runs regularly
	// so leftovers are picked up on the next tick.
	reconcileMaxPasses = 10
)

// ReconcileOps drives interrupted volume operations to a deterministic state
// after failures or restarts:
//
//   - uploads whose process died before the metadata transaction committed are
//     aborted: no row ever referenced their staged object, so the bytes are
//     removed and any reader keeps seeing the previous version;
//   - committed replacements whose displaced object could not be removed
//     inline get it removed now, provided no current metadata references it;
//   - failed uploads have their staged bytes reaped;
//   - pending/failed deletes are retried: object first, metadata row second.
//
// It is safe to run repeatedly and concurrently with live traffic.
func ReconcileOps(ctx context.Context, vc *admin.VolumesConfig) error {
	if vc == nil {
		return nil
	}

	store := Ops()

	for pass := 0; pass < reconcileMaxPasses; pass++ {
		progress := false

		aborted, err := abortStaleUploads(ctx, store, vc)
		if err != nil {
			return err
		}

		retried, err := retryDeletes(ctx, store, vc)
		if err != nil {
			return err
		}

		cleaned, err := cleanCommittedUploads(ctx, store, vc)
		if err != nil {
			return err
		}

		reaped, err := reapFailedUploads(ctx, store, vc)
		if err != nil {
			return err
		}

		progress = aborted || retried || cleaned || reaped

		if !progress {
			return nil
		}
	}

	return nil
}

// abortStaleUploads fails pending uploads older than the grace period (their
// request is gone) and removes their now-unprotected staged object.
func abortStaleUploads(ctx context.Context, store *opsStore, vc *admin.VolumesConfig) (bool, error) {
	ops, err := store.StalePendingUploadOps(ctx, time.Now().Add(-reconcilePendingGrace), reconcileBatchSize)

	if err != nil {
		return false, err
	}

	progress := false

	for _, op := range ops {
		// Flip out of pending first: that releases the protection the object
		// receives as an in-flight upload.
		if err := store.FailOp(ctx, op.ID, "interrupted upload aborted after restart"); err != nil {
			slog.Errorf("cannot abort stale volume upload op %d: %s", op.ID, err.Error())
			continue
		}

		gone, err := removeIfUnreferenced(ctx, store, vc, op.Object)

		if err != nil {
			slog.Errorf("cannot verify staged object of stale volume upload op %d: %s", op.ID, err.Error())
			continue
		}

		if !gone {
			// The object is referenced by current metadata; leave it in
			// place and keep the row for the next run.
			continue
		}

		if err := store.DeleteOp(ctx, op.ID); err != nil {
			slog.Errorf("cannot delete aborted volume upload op %d: %s", op.ID, err.Error())
			continue
		}

		slog.Debug(slog.LogOpts{
			Msg:   "aborted stale volume upload",
			Level: slog.DL1,
			Payload: []zap.Field{
				zap.Int64("op_id", int64(op.ID)),
				zap.Int64("env_id", int64(op.EnvID)),
				zap.String("name", op.FileName),
			},
		})

		progress = true
	}

	return progress, nil
}

// retryDeletes completes pending and failed delete operations: the object is
// removed first and the row is only consumed afterwards, so a failing backend
// never strands an undownloadable empty row.
func retryDeletes(ctx context.Context, store *opsStore, vc *admin.VolumesConfig) (bool, error) {
	ops, err := store.RetryableDeleteOps(ctx, reconcileBatchSize)

	if err != nil {
		return false, err
	}

	progress := false

	for _, op := range ops {
		if err := RemoveObject(vc, op.Object); err != nil {
			slog.Errorf("cannot remove object of pending volume delete op %d: %s", op.ID, err.Error())
			continue
		}

		if err := store.CommitDelete(ctx, op); err != nil {
			slog.Errorf("cannot commit pending volume delete op %d: %s", op.ID, err.Error())
			continue
		}

		progress = true
	}

	return progress, nil
}

// cleanCommittedUploads removes objects displaced by committed replacements
// and consumes the finished op rows. Objects still referenced by current
// metadata are left for a later run.
func cleanCommittedUploads(ctx context.Context, store *opsStore, vc *admin.VolumesConfig) (bool, error) {
	ops, err := store.CommittedUploadOps(ctx, reconcileBatchSize)

	if err != nil {
		return false, err
	}

	progress := false

	for _, op := range ops {
		if op.Replaced != nil {
			gone, err := removeIfUnreferenced(ctx, store, vc, *op.Replaced)

			if err != nil {
				slog.Errorf("cannot verify displaced object of volume upload op %d: %s", op.ID, err.Error())
				continue
			}

			if !gone {
				continue
			}
		}

		if err := store.DeleteOp(ctx, op.ID); err != nil {
			slog.Errorf("cannot delete committed volume upload op %d: %s", op.ID, err.Error())
			continue
		}

		progress = true
	}

	return progress, nil
}

// reapFailedUploads removes staged objects left behind by failed uploads and
// consumes their op rows.
func reapFailedUploads(ctx context.Context, store *opsStore, vc *admin.VolumesConfig) (bool, error) {
	ops, err := store.FailedUploadOps(ctx, reconcileBatchSize)

	if err != nil {
		return false, err
	}

	progress := false

	for _, op := range ops {
		gone, err := removeIfUnreferenced(ctx, store, vc, op.Object)

		if err != nil {
			slog.Errorf("cannot verify staged object of failed volume upload op %d: %s", op.ID, err.Error())
			continue
		}

		if !gone {
			continue
		}

		if err := store.DeleteOp(ctx, op.ID); err != nil {
			slog.Errorf("cannot delete failed volume upload op %d: %s", op.ID, err.Error())
			continue
		}

		progress = true
	}

	return progress, nil
}

// PurgeEnvOps removes every operation object of a soft-deleted environment
// and consumes its op rows. It runs after the environment's volumes rows were
// purged, so no live metadata can reference these objects anymore.
func PurgeEnvOps(ctx context.Context, vc *admin.VolumesConfig, envID types.ID) error {
	store := Ops()

	for {
		ops, err := store.OpsByEnv(ctx, envID, reconcileBatchSize)

		if err != nil {
			return err
		}

		if len(ops) == 0 {
			return nil
		}

		for _, op := range ops {
			refs := []ObjectRef{op.Object}

			if op.Replaced != nil {
				refs = append(refs, *op.Replaced)
			}

			for _, ref := range refs {
				if err := RemoveObject(vc, ref); err != nil {
					return err
				}
			}

			if err := store.DeleteOp(ctx, op.ID); err != nil {
				return err
			}
		}

		if len(ops) < reconcileBatchSize {
			return nil
		}
	}
}
