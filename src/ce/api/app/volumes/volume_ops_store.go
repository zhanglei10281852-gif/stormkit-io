package volumes

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/stormkit-io/stormkit-io/src/lib/database"
	"github.com/stormkit-io/stormkit-io/src/lib/types"
	"github.com/stormkit-io/stormkit-io/src/lib/utils"
)

const (
	OpUpload = "upload"
	OpDelete = "delete"

	OpPending   = "pending"
	OpCommitted = "committed"
	OpFailed    = "failed"
)

// ObjectRef locates a physical object in the configured backend. It mirrors
// the (file_path, file_name) pair stored on volumes rows so the same backend
// code can download or remove it.
type ObjectRef struct {
	Path string `json:"path"`
	Name string `json:"name"`
}

func (r ObjectRef) file() *File {
	return &File{Path: r.Path, Name: r.Name}
}

func (r ObjectRef) jsonValue() any {
	b, _ := json.Marshal(r)
	return string(b)
}

// Op is a registered upload or delete operation. The row exists before the
// backend is touched and is only removed once the operation and every cleanup
// it implies have completed, which is what makes crash recovery deterministic.
type Op struct {
	ID        types.ID
	EnvID     types.ID
	Type      string
	Status    string
	FileID    types.ID
	FileName  string
	FileSize  int64
	MountType string
	Object    ObjectRef
	Replaced  *ObjectRef
	Error     sql.NullString
	CreatedAt utils.Unix
	UpdatedAt utils.Unix
}

const opColumns = `
	op_id, env_id, op_type, status, file_id, file_name,
	file_size, mount_type, object, replaced, error,
	created_at, updated_at
`

type opsStore struct {
	*database.Store
}

// Ops returns a store for volume lifecycle operations.
func Ops() *opsStore {
	return &opsStore{Store: database.NewStore()}
}

func scanOp(s interface {
	Scan(dest ...any) error
}) (*Op, error) {
	var (
		op          Op
		fileID      sql.NullInt64
		fileName    sql.NullString
		errMsg      sql.NullString
		objectRaw   []byte
		replacedRaw []byte
	)

	if err := s.Scan(
		&op.ID, &op.EnvID, &op.Type, &op.Status, &fileID, &fileName,
		&op.FileSize, &op.MountType, &objectRaw, &replacedRaw, &errMsg,
		&op.CreatedAt, &op.UpdatedAt,
	); err != nil {
		return nil, err
	}

	if fileID.Valid {
		op.FileID = types.ID(fileID.Int64)
	}

	op.FileName = fileName.String
	op.Error = errMsg

	if err := json.Unmarshal(objectRaw, &op.Object); err != nil {
		return nil, err
	}

	if len(replacedRaw) > 0 {
		var replaced ObjectRef

		if err := json.Unmarshal(replacedRaw, &replaced); err != nil {
			return nil, err
		}

		op.Replaced = &replaced
	}

	return &op, nil
}

// CreateOp registers an operation before the backend is touched.
func (s *opsStore) CreateOp(ctx context.Context, op *Op) error {
	row := s.Conn.QueryRowContext(ctx, `
		INSERT INTO volumes_ops (
			env_id, op_type, status, file_id, file_name,
			file_size, mount_type, object
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING op_id, created_at
	`,
		op.EnvID, op.Type, op.Status, nullableID(op.FileID), nullableString(op.FileName),
		op.FileSize, op.MountType, op.Object.jsonValue(),
	)

	if err := row.Scan(&op.ID, &op.CreatedAt); err != nil {
		return err
	}

	return nil
}

// DeleteOp removes the operation row.
func (s *opsStore) DeleteOp(ctx context.Context, opID types.ID) error {
	_, err := s.Conn.ExecContext(ctx, `DELETE FROM volumes_ops WHERE op_id = $1`, opID)
	return err
}

// FailOp marks the operation as failed and stores the error message. Failed
// uploads leave no metadata behind; their staged object is reaped by
// reconciliation. Failed deletes are retried because their rows are still
// downloadable.
func (s *opsStore) FailOp(ctx context.Context, opID types.ID, errMsg string) error {
	_, err := s.Conn.ExecContext(ctx, `
		UPDATE volumes_ops
		SET status = $2, error = $3, updated_at = now() AT TIME ZONE 'UTC'
		WHERE op_id = $1
	`, opID, OpFailed, truncateErr(errMsg))
	return err
}

// PublishUpload flips an upload to its single observable state: within one
// transaction the volumes row starts pointing at the staged object and the
// operation becomes committed, recording the object it displaced (if any).
// Concurrent readers see either the old row or the new row, never a mix.
// It returns the published file id (stable across replacements so public
// links keep working) and the displaced object (nil for new files).
func (s *opsStore) PublishUpload(ctx context.Context, op *Op, file *File) (types.ID, *ObjectRef, error) {
	tx, err := s.Conn.BeginTx(ctx, nil)

	if err != nil {
		return 0, nil, err
	}

	defer tx.Rollback()

	var (
		fileID   types.ID
		replaced *ObjectRef
	)

	// Two uploads of the same new name can both find no row before either
	// inserts; the loser of the unique constraint retries as an update so the
	// request still publishes instead of surfacing a conflict.
	for attempt := 0; attempt < 2; attempt++ {
		var (
			oldPath sql.NullString
			oldName sql.NullString
			oldMeta utils.Map
		)

		scanErr := tx.QueryRowContext(ctx, `
			SELECT file_path, file_name, file_metadata
			FROM volumes
			WHERE env_id = $1 AND file_name = $2
			FOR UPDATE
		`, file.EnvID, file.Name).Scan(&oldPath, &oldName, &oldMeta)

		if scanErr == sql.ErrNoRows {
			scanErr = tx.QueryRowContext(ctx, `
				INSERT INTO volumes (
					file_name, file_path, file_size, file_metadata,
					is_public, env_id, created_at
				)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
				RETURNING file_id
			`,
				file.Name, file.Path, file.Size, metadataValue(file.Metadata),
				file.IsPublic, file.EnvID, file.CreatedAt,
			).Scan(&fileID)

			if scanErr != nil && database.IsDuplicate(scanErr) && attempt == 0 {
				continue
			}
		} else if scanErr == nil {
			if oldPath.Valid && oldName.Valid {
				r := ObjectRefFromRow(oldPath.String, oldName.String, oldMeta)
				replaced = &r
			}

			scanErr = tx.QueryRowContext(ctx, `
				UPDATE volumes
				SET file_path = $1,
					file_size = $2,
					file_metadata = $3,
					updated_at = $4
				WHERE env_id = $5 AND file_name = $6
				RETURNING file_id
			`,
				file.Path, file.Size, metadataValue(file.Metadata),
				file.CreatedAt, file.EnvID, file.Name,
			).Scan(&fileID)
		}

		if scanErr != nil {
			return 0, nil, scanErr
		}

		break
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE volumes_ops
		SET status = $2,
			file_id = $3,
			replaced = $4,
			updated_at = $5
		WHERE op_id = $1
	`,
		op.ID, OpCommitted, fileID, jsonRef(replaced), file.CreatedAt,
	); err != nil {
		return 0, nil, err
	}

	if err := tx.Commit(); err != nil {
		return 0, nil, err
	}

	op.Status = OpCommitted
	op.FileID = fileID
	op.Replaced = replaced
	file.ID = fileID

	return fileID, replaced, nil
}

// CommitDelete finishes a delete operation. The volumes row is deleted only
// while it still points at the exact object the operation removed, so a
// replacement that committed concurrently (repointing the row at a new
// object) is never taken down. The op row is consumed in the same
// transaction.
func (s *opsStore) CommitDelete(ctx context.Context, op *Op) error {
	tx, err := s.Conn.BeginTx(ctx, nil)

	if err != nil {
		return err
	}

	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		DELETE FROM volumes
		WHERE file_id = $1 AND env_id = $2 AND file_path = $3 AND file_name = $4
	`, op.FileID, op.EnvID, op.Object.Path, op.FileName); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM volumes_ops WHERE op_id = $1`, op.ID); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *opsStore) queryOps(ctx context.Context, query string, args ...any) ([]*Op, error) {
	rows, err := s.Conn.QueryContext(ctx, query, args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	ops := []*Op{}

	for rows.Next() {
		op, err := scanOp(rows)

		if err != nil {
			return nil, err
		}

		ops = append(ops, op)
	}

	return ops, rows.Err()
}

// StalePendingUploadOps returns upload operations still pending after the
// given timestamp. Younger rows belong to requests that may still be in
// flight on another node and must not be touched.
func (s *opsStore) StalePendingUploadOps(ctx context.Context, before time.Time, limit int) ([]*Op, error) {
	return s.queryOps(ctx, `
		SELECT `+opColumns+`
		FROM volumes_ops
		WHERE op_type = $1 AND status = $2 AND created_at < $3
		ORDER BY op_id ASC
		LIMIT $4
	`, OpUpload, OpPending, before, limit)
}

// RetryableDeleteOps returns delete operations that still need their physical
// object and row removed. Pending rows may be in flight; repeating the
// idempotent remove + conditional delete is harmless.
func (s *opsStore) RetryableDeleteOps(ctx context.Context, limit int) ([]*Op, error) {
	return s.queryOps(ctx, `
		SELECT `+opColumns+`
		FROM volumes_ops
		WHERE op_type = $1 AND status IN ($2, $3)
		ORDER BY op_id ASC
		LIMIT $4
	`, OpDelete, OpPending, OpFailed, limit)
}

// CommittedUploadOps returns committed uploads whose displaced object still
// has to be cleaned up (replaced IS NOT NULL), followed by rows that are
// fully done and only need to be removed.
func (s *opsStore) CommittedUploadOps(ctx context.Context, limit int) ([]*Op, error) {
	return s.queryOps(ctx, `
		SELECT `+opColumns+`
		FROM volumes_ops
		WHERE op_type = $1 AND status = $2
		ORDER BY op_id ASC
		LIMIT $3
	`, OpUpload, OpCommitted, limit)
}

// FailedUploadOps returns aborted uploads whose staged object must be reaped.
func (s *opsStore) FailedUploadOps(ctx context.Context, limit int) ([]*Op, error) {
	return s.queryOps(ctx, `
		SELECT `+opColumns+`
		FROM volumes_ops
		WHERE op_type = $1 AND status = $2
		ORDER BY op_id ASC
		LIMIT $3
	`, OpUpload, OpFailed, limit)
}

// OpsByEnv returns every operation registered for an environment, used while
// purging a deleted environment.
func (s *opsStore) OpsByEnv(ctx context.Context, envID types.ID, limit int) ([]*Op, error) {
	return s.queryOps(ctx, `
		SELECT `+opColumns+`
		FROM volumes_ops
		WHERE env_id = $1
		ORDER BY op_id ASC
		LIMIT $2
	`, envID, limit)
}

// DeleteOpsByEnv removes all operation rows of an environment.
func (s *opsStore) DeleteOpsByEnv(ctx context.Context, envID types.ID) error {
	_, err := s.Conn.ExecContext(ctx, `DELETE FROM volumes_ops WHERE env_id = $1`, envID)
	return err
}

// IsObjectProtected reports whether the object is either staged by an
// operation that may still commit or referenced by a published volumes row.
// Recovery never removes a protected object. Rows sharing the object's
// directory are resolved to their physical key in Go so that legacy layouts
// and the flattened staged names compare correctly; the check is deliberately
// conservative (an extra directory match only delays cleanup, it never
// deletes a live object).
func (s *opsStore) IsObjectProtected(ctx context.Context, mountType string, ref ObjectRef) (bool, error) {
	var pending int

	if err := s.Conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM volumes_ops
		WHERE op_type = $1 AND status = $2 AND object @> $3::jsonb
	`, OpUpload, OpPending, ref.jsonValue()).Scan(&pending); err != nil {
		return false, err
	}

	if pending > 0 {
		return true, nil
	}

	rows, err := s.Conn.QueryContext(ctx, `
		SELECT file_name, file_metadata
		FROM volumes
		WHERE file_path = $1
	`, ref.Path)

	if err != nil {
		return false, err
	}

	defer rows.Close()

	targetKey := PhysicalKey(mountType, ref)

	for rows.Next() {
		var (
			logicalName string
			metadata    utils.Map
		)

		if err := rows.Scan(&logicalName, &metadata); err != nil {
			return false, err
		}

		rowKey := PhysicalKey(mountType, ObjectRefFromRow(ref.Path, logicalName, metadata))

		if rowKey != "" && rowKey == targetKey {
			return true, nil
		}
	}

	return false, rows.Err()
}

func nullableID(id types.ID) any {
	if id == 0 {
		return nil
	}

	return id
}

func nullableString(v string) any {
	if v == "" {
		return nil
	}

	return v
}

func jsonRef(ref *ObjectRef) any {
	if ref == nil {
		return nil
	}

	return ref.jsonValue()
}

func truncateErr(msg string) string {
	const max = 2000

	if len(msg) > max {
		return msg[:max]
	}

	return msg
}
