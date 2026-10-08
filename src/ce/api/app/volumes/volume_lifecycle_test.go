package volumes_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stormkit-io/stormkit-io/src/ce/api/admin"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/volumes"
	"github.com/stormkit-io/stormkit-io/src/lib/database/databasetest"
	"github.com/stormkit-io/stormkit-io/src/lib/factory"
	"github.com/stormkit-io/stormkit-io/src/lib/types"
	"github.com/stretchr/testify/suite"
)

var (
	_ multipart.File     = failingReader{}
	_ volumes.FileHeader = (*stubFileHeader)(nil)
)

// stubFileHeader is an in-memory FileHeader for lifecycle tests.
type stubFileHeader struct {
	name    string
	data    []byte
	readErr error
}

type stubMultipartFile struct {
	*bytes.Reader
	io.Closer
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error)          { return 0, errors.New("broken pipe") }
func (failingReader) ReadAt([]byte, int64) (int, error) { return 0, errors.New("broken pipe") }
func (failingReader) Seek(int64, int) (int64, error)    { return 0, errors.New("broken pipe") }
func (failingReader) Close() error                      { return nil }

func (h *stubFileHeader) Open() (multipart.File, error) {
	if h.readErr != nil {
		return failingReader{}, nil
	}

	return &stubMultipartFile{
		Reader: bytes.NewReader(h.data),
		Closer: io.NopCloser(nil),
	}, nil
}

func (h *stubFileHeader) Name() string { return h.name }
func (h *stubFileHeader) Size() int64  { return int64(len(h.data)) }

type VolumeLifecycleSuite struct {
	suite.Suite
	*factory.Factory
	conn   databasetest.TestDB
	tmpdir string
	envID  types.ID
	cfg    *admin.VolumesConfig
}

func (s *VolumeLifecycleSuite) BeforeTest(suiteName, _ string) {
	s.conn = databasetest.InitTx(suiteName)
	s.Factory = factory.New(s.conn)

	volumes.CachedFileSys = nil

	tmpdir, err := os.MkdirTemp("", "tmp-volume-lifecycle-")
	s.Require().NoError(err)
	s.tmpdir = tmpdir

	app := s.MockApp(nil)
	env := s.MockEnv(app)
	s.envID = env.ID

	s.cfg = &admin.VolumesConfig{
		MountType: volumes.FileSys,
		RootPath:  s.tmpdir,
	}
}

func (s *VolumeLifecycleSuite) AfterTest(_, _ string) {
	s.conn.CloseTx()
	volumes.CachedFileSys = nil
	os.RemoveAll(s.tmpdir)
}

func (s *VolumeLifecycleSuite) item(name, content string) volumes.UploadItem {
	return volumes.UploadItem{
		EnvID: s.envID,
		FileHeader: &stubFileHeader{
			name: name,
			data: []byte(content),
		},
		ContentDisposition: map[string]string{"filename": name},
	}
}

func (s *VolumeLifecycleSuite) countOps() int {
	var count int

	s.NoError(s.conn.QueryRow(`SELECT COUNT(*) FROM volumes_ops`).Scan(&count))

	return count
}

func (s *VolumeLifecycleSuite) countFiles() int {
	var count int

	s.NoError(s.conn.QueryRow(`SELECT COUNT(*) FROM volumes WHERE env_id = $1`, s.envID).Scan(&count))

	return count
}

// physicalFiles returns every regular object left under the volume root.
func (s *VolumeLifecycleSuite) physicalFiles() []string {
	found := []string{}

	s.NoError(filepath.WalkDir(s.tmpdir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			found = append(found, p)
		}

		return nil
	}))

	return found
}

func (s *VolumeLifecycleSuite) download(file *volumes.File) string {
	reader, err := volumes.Download(s.cfg, file)
	s.Require().NoError(err)
	s.Require().NotNil(reader)

	data, err := io.ReadAll(reader)
	s.Require().NoError(err)

	return string(data)
}

// Test_UploadBatch_PublishesOneVersion verifies the complete upload and
// replacement lifecycle: every reader resolves to the committed version and
// the displaced object is removed once nothing references it.
func (s *VolumeLifecycleSuite) Test_UploadBatch_PublishesOneVersion() {
	ctx := context.Background()

	files, failed := volumes.UploadBatch(ctx, s.cfg, []volumes.UploadItem{
		s.item("docs/readme.txt", "v1"),
	})

	s.Empty(failed)
	s.Len(files, 1)

	first := files[0]
	s.Equal("docs/readme.txt", first.Name)
	s.Equal(int64(2), first.Size)
	s.FileExists(volumes.PhysicalKey(volumes.FileSys, volumes.RefOf(first)))
	s.Equal("v1", s.download(first))

	size, err := volumes.Store().VolumeSize(ctx, s.envID)
	s.NoError(err)
	s.Equal(int64(2), size)
	s.Equal(0, s.countOps(), "finished upload must not leave ops behind")

	// Replace with a different size. Until commit old version is served;
	// after commit list/download/size must all describe the new bytes.
	files, failed = volumes.UploadBatch(ctx, s.cfg, []volumes.UploadItem{
		s.item("docs/readme.txt", "version-two"),
	})

	s.Empty(failed)
	s.Len(files, 1)

	replaced := files[0]
	s.Equal(first.ID, replaced.ID, "file id stays stable across replacements")
	s.Equal(int64(11), replaced.Size)

	rows, err := volumes.Store().SelectFiles(ctx, volumes.SelectFilesArgs{EnvID: s.envID})
	s.NoError(err)
	s.Len(rows, 1)

	current := rows[0]
	s.Equal(int64(11), current.Size)
	s.True(current.UpdatedAt.Valid, "row records the replacement time")
	s.Equal("version-two", s.download(current))
	s.Equal(first.ID.String(), current.ID.String())

	size, err = volumes.Store().VolumeSize(ctx, s.envID)
	s.NoError(err)
	s.Equal(int64(11), size, "capacity stat follows the committed version")

	s.Len(s.physicalFiles(), 1, "displaced object must be removed")
	s.FileExists(volumes.PhysicalKey(volumes.FileSys, volumes.RefOf(current)))
	s.Equal(0, s.countOps())
}

// Test_UploadBatch_NestedName verifies nested logical names flatten into
// unique object directories while preserving the logical name in metadata.
func (s *VolumeLifecycleSuite) Test_UploadBatch_NestedName() {
	ctx := context.Background()

	files, failed := volumes.UploadBatch(ctx, s.cfg, []volumes.UploadItem{
		s.item("a/b/c.txt", "deep"),
	})

	s.Empty(failed)
	s.Len(files, 1)
	s.Equal("a/b/c.txt", files[0].Name)
	s.Equal("deep", s.download(files[0]))
}

// Test_UploadBatch_PartialSuccess keeps the existing semantics: an invalid
// file is reported under failed while the rest of the batch publishes.
func (s *VolumeLifecycleSuite) Test_UploadBatch_PartialSuccess() {
	ctx := context.Background()

	files, failed := volumes.UploadBatch(ctx, s.cfg, []volumes.UploadItem{
		s.item("../escape.txt", "nope"),
		s.item("good.txt", "yes"),
	})

	s.Len(files, 1)
	s.Equal("good.txt", files[0].Name)
	s.Contains(failed, "../escape.txt")
	s.Equal(1, s.countFiles())
	s.Equal(0, s.countOps())
}

// Test_UploadBatch_ObjectWriteFailure leaves a definite failure: no metadata,
// no op row and no staged bytes, so the request can simply be retried.
func (s *VolumeLifecycleSuite) Test_UploadBatch_ObjectWriteFailure() {
	ctx := context.Background()

	files, failed := volumes.UploadBatch(ctx, s.cfg, []volumes.UploadItem{
		{
			EnvID: s.envID,
			FileHeader: &stubFileHeader{
				name:    "broken.txt",
				data:    []byte("x"),
				readErr: errors.New("broken pipe"),
			},
			ContentDisposition: map[string]string{"filename": "broken.txt"},
		},
		s.item("good.txt", "yes"),
	})

	s.Len(files, 1)
	s.Equal("good.txt", files[0].Name)
	s.Contains(failed, "broken.txt")
	s.Equal(1, s.countFiles())
	s.Equal(0, s.countOps(), "failed upload must be fully aborted")

	for _, p := range s.physicalFiles() {
		s.NotContains(p, "broken.txt")
	}
}

// Test_DeleteBatch_PhysicalFailureKeepsRow verifies that a failing object
// removal keeps a downloadable, retryable row instead of an empty one, and
// that reconciliation completes the delete once the backend recovers.
func (s *VolumeLifecycleSuite) Test_DeleteBatch_PhysicalFailureKeepsRow() {
	ctx := context.Background()

	files, failed := volumes.UploadBatch(ctx, s.cfg, []volumes.UploadItem{
		s.item("doc.txt", "data"),
	})

	s.Empty(failed)
	file := files[0]
	originalPath := file.Path

	// Repoint the row at a location where object removal deterministically
	// fails (ENOTDIR on /dev/null), independently of process privileges.
	_, err := s.conn.Exec(`UPDATE volumes SET file_path = $2 WHERE file_id = $1`, file.ID, "/dev/null")
	s.NoError(err)

	file.Path = "/dev/null"

	removed, delFailed := volumes.DeleteBatch(ctx, s.cfg, []*volumes.File{file})
	s.Empty(removed)
	s.Contains(delFailed, file.ID.String())
	s.Equal(1, s.countFiles(), "row must stay while the object cannot be removed")
	s.Equal(1, s.countOps(), "retryable delete op must stay")

	s.Require().NoError(volumes.ReconcileOps(ctx, s.cfg))
	s.Equal(1, s.countFiles(), "still broken: row and op persist")
	s.Equal(1, s.countOps())

	// Backend recovers: metadata and the pending op point back at the real
	// object, so the retried remove succeeds and both rows are consumed.
	_, err = s.conn.Exec(`UPDATE volumes SET file_path = $2 WHERE file_id = $1`, file.ID, originalPath)
	s.NoError(err)
	_, err = s.conn.Exec(`
		UPDATE volumes_ops
		SET object = jsonb_set(object, '{path}', to_jsonb($2::text))
		WHERE file_id = $1 AND op_type = 'delete'
	`, file.ID, originalPath)
	s.NoError(err)
	s.Require().NoError(volumes.ReconcileOps(ctx, s.cfg))
	s.Equal(0, s.countFiles(), "row consumed after the object is gone")
	s.Equal(0, s.countOps())
}

// Test_DeleteBatch_MissingObjectConsumesRow treats an already-missing object
// as success, matching the legacy contract.
func (s *VolumeLifecycleSuite) Test_DeleteBatch_MissingObjectConsumesRow() {
	ctx := context.Background()

	files, failed := volumes.UploadBatch(ctx, s.cfg, []volumes.UploadItem{
		s.item("doc.txt", "data"),
	})
	s.Empty(failed)
	file := files[0]

	s.NoError(os.Remove(volumes.PhysicalKey(volumes.FileSys, volumes.RefOf(file))))

	removed, delFailed := volumes.DeleteBatch(ctx, s.cfg, []*volumes.File{file})
	s.Empty(delFailed)
	s.Len(removed, 1)
	s.Equal(0, s.countFiles())
	s.Equal(0, s.countOps())
}

// Test_Reconcile_AbortsInterruptedUpload simulates a crash after the object
// was written but before metadata committed: the orphan staged object is
// reaped and published data is untouched.
func (s *VolumeLifecycleSuite) Test_Reconcile_AbortsInterruptedUpload() {
	ctx := context.Background()

	live, failed := volumes.UploadBatch(ctx, s.cfg, []volumes.UploadItem{
		s.item("live.txt", "live"),
	})
	s.Empty(failed)

	// Orphan staged object on disk with an old pending op, as if the process
	// died right after writing it.
	orphanDir := filepath.Join(s.tmpdir, "a0e"+s.envID.String(), "objects", "orphantoken")
	s.Require().NoError(os.MkdirAll(orphanDir, 0775))
	orphanPath := filepath.Join(orphanDir, "orphan.txt")
	s.Require().NoError(os.WriteFile(orphanPath, []byte("orphan"), 0664))

	_, err := s.conn.Exec(`
		INSERT INTO volumes_ops (
			env_id, op_type, status, file_name, file_size, mount_type,
			object, created_at
		)
		VALUES ($1, 'upload', 'pending', 'orphan.txt', 6, $2, $3, $4)
	`,
		s.envID,
		volumes.FileSys,
		`{"path": "`+orphanDir+`", "name": "orphan.txt"}`,
		time.Now().Add(-time.Hour),
	)
	s.NoError(err)

	s.Require().NoError(volumes.ReconcileOps(ctx, s.cfg))

	s.NoFileExists(orphanPath, "orphan object reaped")
	s.Equal(1, s.countFiles(), "published row untouched")
	s.Equal("live", s.download(live[0]))
	s.Equal(0, s.countOps())
}

// Test_Reconcile_NeverDeletesReferencedObject verifies the safety rule: a
// displaced object that still matches current metadata is preserved; the op
// is retried later and only then completes.
func (s *VolumeLifecycleSuite) Test_Reconcile_NeverDeletesReferencedObject() {
	ctx := context.Background()

	files, failed := volumes.UploadBatch(ctx, s.cfg, []volumes.UploadItem{
		s.item("doc.txt", "live"),
	})
	s.Empty(failed)
	file := files[0]
	livePath := volumes.PhysicalKey(volumes.FileSys, volumes.RefOf(file))

	// A committed replacement op that claims the currently live object as
	// displaced. Recovery must trust the metadata, not the op.
	_, err := s.conn.Exec(`
		INSERT INTO volumes_ops (
			env_id, op_type, status, file_id, file_name, file_size,
			mount_type, object, replaced, created_at
		)
		VALUES ($1, 'upload', 'committed', $2, 'doc.txt', 4, $3, $4, $5, now())
	`,
		s.envID, file.ID,
		volumes.FileSys,
		`{"path": "`+filepath.Dir(livePath)+`", "name": "new.txt"}`,
		`{"path": "`+file.Path+`", "name": "doc.txt"}`,
	)
	s.NoError(err)

	s.Require().NoError(volumes.ReconcileOps(ctx, s.cfg))
	s.FileExists(livePath, "referenced object must survive recovery")
	s.Equal(1, s.countOps(), "op retained while its object is protected")

	// Metadata moves away (row deleted): the object is now unreferenced and
	// the op completes on the next run.
	s.NoError(volumes.Store().RemoveFiles(ctx, []*volumes.File{file}, s.envID))
	s.Require().NoError(volumes.ReconcileOps(ctx, s.cfg))
	s.NoFileExists(livePath)
	s.Equal(0, s.countOps())
}

// Test_Reconcile_RetryPendingDelete completes an interrupted delete after
// restart: object removal first, metadata consumption second.
func (s *VolumeLifecycleSuite) Test_Reconcile_RetryPendingDelete() {
	ctx := context.Background()

	files, failed := volumes.UploadBatch(ctx, s.cfg, []volumes.UploadItem{
		s.item("doc.txt", "data"),
	})
	s.Empty(failed)
	file := files[0]
	objectPath := volumes.PhysicalKey(volumes.FileSys, volumes.RefOf(file))

	_, err := s.conn.Exec(`
		INSERT INTO volumes_ops (
			env_id, op_type, status, file_id, file_name, file_size,
			mount_type, object, created_at
		)
		VALUES ($1, 'delete', 'pending', $2, 'doc.txt', 4, $3, $4, now())
	`,
		s.envID, file.ID,
		volumes.FileSys,
		`{"path": "`+file.Path+`", "name": "doc.txt"}`,
	)
	s.NoError(err)

	s.Require().NoError(volumes.ReconcileOps(ctx, s.cfg))

	s.NoFileExists(objectPath)
	s.Equal(0, s.countFiles())
	s.Equal(0, s.countOps())
}

// Test_UploadBatch_IsPublicPreservedOnReplace verifies visibility is a
// property of the row, not the upload: making a file public and replacing it
// keeps it public under the same id and link.
func (s *VolumeLifecycleSuite) Test_UploadBatch_IsPublicPreservedOnReplace() {
	ctx := context.Background()

	files, failed := volumes.UploadBatch(ctx, s.cfg, []volumes.UploadItem{
		s.item("doc.txt", "v1"),
	})
	s.Empty(failed)
	file := files[0]

	s.NoError(volumes.Store().ChangeVisibility(ctx, file.ID, true))

	files, failed = volumes.UploadBatch(ctx, s.cfg, []volumes.UploadItem{
		s.item("doc.txt", "v2"),
	})
	s.Empty(failed)

	rows, err := volumes.Store().SelectFiles(ctx, volumes.SelectFilesArgs{EnvID: s.envID})
	s.NoError(err)
	s.Len(rows, 1)
	s.True(rows[0].IsPublic)
	s.Equal(file.ID, rows[0].ID)
	s.Equal("v2", s.download(rows[0]))
}

func TestVolumeLifecycleSuite(t *testing.T) {
	suite.Run(t, &VolumeLifecycleSuite{})
}
