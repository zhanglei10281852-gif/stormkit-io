package volumes

import (
	"context"

	"github.com/stormkit-io/stormkit-io/src/ce/api/admin"
	"github.com/stormkit-io/stormkit-io/src/lib/slog"
	"github.com/stormkit-io/stormkit-io/src/lib/types"
	"github.com/stormkit-io/stormkit-io/src/lib/utils"
)

// UploadItem is one file of a multipart batch upload.
type UploadItem struct {
	AppID              types.ID
	EnvID              types.ID
	FileHeader         FileHeader
	ContentDisposition map[string]string
}

// UploadBatch stages and publishes uploads one file at a time. Each file is
// an independent operation: an invalid name, a failed object write or a
// failed metadata commit is recorded under failed, while every other file
// still publishes (partial-success semantics). Same-name uploads inside one
// batch resolve to the last version, exactly like the old API contract.
//
// The physical object is registered before it is written and the volumes row
// only starts referencing it inside a transaction, so at any observable
// point list, download, public-file and size describe the same version.
func UploadBatch(ctx context.Context, vc *admin.VolumesConfig, items []UploadItem) ([]*File, map[string]string) {
	store := Ops()
	files := []*File{}
	failed := map[string]string{}
	indexByName := map[string]int{}

	for _, item := range items {
		displayName := utils.GetString(item.ContentDisposition["filename"], item.FileHeader.Name())

		reader, fileName, err := OpenUpload(UploadArgs{
			AppID:              item.AppID,
			EnvID:              item.EnvID,
			FileHeader:         item.FileHeader,
			ContentDisposition: item.ContentDisposition,
		})

		if err != nil {
			failed[displayName] = err.Error()
			continue
		}

		token := utils.RandomToken(stagingTokenLength)
		ref := StagingRef(vc, item.AppID, item.EnvID, token, fileName)

		op := &Op{
			EnvID:     item.EnvID,
			Type:      OpUpload,
			Status:    OpPending,
			FileName:  fileName,
			FileSize:  item.FileHeader.Size(),
			MountType: vc.MountType,
			Object:    ref,
		}

		if err := store.CreateOp(ctx, op); err != nil {
			reader.Close()
			failed[displayName] = err.Error()
			continue
		}

		staged, err := StageUpload(vc, UploadArgs{
			AppID:              item.AppID,
			EnvID:              item.EnvID,
			FileHeader:         item.FileHeader,
			ContentDisposition: item.ContentDisposition,
		}, token, fileName, reader)

		reader.Close()

		if err != nil {
			abortUpload(ctx, store, vc, op, err.Error())
			failed[displayName] = err.Error()
			continue
		}

		if staged == nil {
			reason := "file storage mount type is not supported"
			abortUpload(ctx, store, vc, op, reason)
			failed[displayName] = reason
			continue
		}

		if _, replaced, err := store.PublishUpload(ctx, op, staged); err != nil {
			abortUpload(ctx, store, vc, op, err.Error())
			failed[displayName] = err.Error()
			continue
		} else {
			finishUpload(ctx, store, vc, op, replaced)
		}

		// Last upload of a logical name wins; keep its position in the
		// response stable.
		if idx, ok := indexByName[fileName]; ok {
			files[idx] = staged
		} else {
			indexByName[fileName] = len(files)
			files = append(files, staged)
		}
	}

	return files, failed
}

// DeleteBatch removes objects first and consumes their metadata rows only
// after the physical removal succeeded. A failed object removal leaves the
// row (and its object) untouched and retryable instead of producing a row
// whose download 404s. Missing objects are consumed normally.
func DeleteBatch(ctx context.Context, vc *admin.VolumesConfig, files []*File) ([]*File, map[string]string) {
	store := Ops()
	removed := []*File{}
	failed := map[string]string{}

	for _, file := range files {
		op := &Op{
			EnvID:     file.EnvID,
			Type:      OpDelete,
			Status:    OpPending,
			FileID:    file.ID,
			FileName:  file.Name,
			FileSize:  file.Size,
			MountType: MountTypeOf(file),
			Object:    RefOf(file),
		}

		if err := store.CreateOp(ctx, op); err != nil {
			failed[file.ID.String()] = err.Error()
			continue
		}

		if err := RemoveObject(vc, op.Object); err != nil {
			if failErr := store.FailOp(ctx, op.ID, err.Error()); failErr != nil {
				slog.Errorf("cannot mark volume delete op %d failed: %s", op.ID, failErr.Error())
			}

			failed[file.ID.String()] = err.Error()
			continue
		}

		if err := store.CommitDelete(ctx, op); err != nil {
			failed[file.ID.String()] = err.Error()
			continue
		}

		removed = append(removed, file)
	}

	return removed, failed
}

// abortUpload drives an upload to a definite failed state: its staged object
// is removed and the op consumed. If either step cannot finish now, the op
// row stays behind and ReconcileOps reaches the same state after restart.
func abortUpload(ctx context.Context, store *opsStore, vc *admin.VolumesConfig, op *Op, reason string) {
	if err := store.FailOp(ctx, op.ID, reason); err != nil {
		slog.Errorf("cannot mark volume upload op %d failed: %s", op.ID, err.Error())
		return
	}

	if err := RemoveObject(vc, op.Object); err != nil {
		slog.Errorf("cannot remove staged object of failed volume upload op %d: %s", op.ID, err.Error())
		return
	}

	if err := store.DeleteOp(ctx, op.ID); err != nil {
		slog.Errorf("cannot delete failed volume upload op %d: %s", op.ID, err.Error())
	}
}

// finishUpload removes the object displaced by a committed replacement and
// consumes the op. When the old object cannot be removed yet, the committed
// op row is retained so ReconcileOps retries without touching the live
// object.
func finishUpload(ctx context.Context, store *opsStore, vc *admin.VolumesConfig, op *Op, replaced *ObjectRef) {
	if replaced != nil {
		gone, err := removeIfUnreferenced(ctx, store, vc, *replaced)

		if err != nil {
			slog.Errorf("cannot verify displaced object of volume upload op %d: %s", op.ID, err.Error())
			return
		}

		if !gone {
			return
		}
	}

	if err := store.DeleteOp(ctx, op.ID); err != nil {
		slog.Errorf("cannot delete finished volume upload op %d: %s", op.ID, err.Error())
	}
}

// removeIfUnreferenced removes the object unless a published volumes row or a
// pending upload still references it. gone is true when the object is absent
// afterwards (already missing objects count as gone).
func removeIfUnreferenced(ctx context.Context, store *opsStore, vc *admin.VolumesConfig, ref ObjectRef) (gone bool, err error) {
	protected, err := store.IsObjectProtected(ctx, vc.MountType, ref)

	if err != nil {
		return false, err
	}

	if protected {
		return false, nil
	}

	if err := RemoveObject(vc, ref); err != nil {
		return false, err
	}

	return true, nil
}
