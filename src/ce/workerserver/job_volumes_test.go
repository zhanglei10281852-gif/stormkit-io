package jobs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stormkit-io/stormkit-io/src/ce/api/admin"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/volumes"
	"github.com/stormkit-io/stormkit-io/src/lib/database/databasetest"
	"github.com/stormkit-io/stormkit-io/src/lib/factory"
	"github.com/stormkit-io/stormkit-io/src/lib/types"
	"github.com/stormkit-io/stormkit-io/src/lib/utils"
	"github.com/stretchr/testify/suite"
)

type JobVolumesSuite struct {
	suite.Suite
	*factory.Factory
	conn   databasetest.TestDB
	tmpdir string
}

func (s *JobVolumesSuite) BeforeTest(suiteName, _ string) {
	s.conn = databasetest.InitTx(suiteName)
	s.Factory = factory.New(s.conn)

	tmpdir, err := os.MkdirTemp("", "tmp-stale-volumes-")
	s.NoError(err)
	s.tmpdir = tmpdir

	s.NoError(admin.Store().UpsertConfig(context.Background(), admin.InstanceConfig{
		VolumesConfig: &admin.VolumesConfig{
			MountType: volumes.FileSys,
			RootPath:  tmpdir,
		},
	}))
}

func (s *JobVolumesSuite) AfterTest(_, _ string) {
	s.conn.CloseTx()
	os.RemoveAll(s.tmpdir)
}

func (s *JobVolumesSuite) writeFile(envID types.ID, name string) *volumes.File {
	content := []byte("Hello world!")

	file := &volumes.File{
		EnvID:     envID,
		Name:      name,
		Size:      int64(len(content)),
		Path:      s.tmpdir,
		IsPublic:  true,
		CreatedAt: utils.NewUnix(),
	}

	s.NoError(os.WriteFile(file.FullPath(), content, 0664))
	s.NoError(volumes.Store().Insert(context.Background(), []*volumes.File{file}, envID))

	return file
}

func (s *JobVolumesSuite) countVolumes(envID types.ID) int {
	var count int

	s.NoError(s.conn.
		QueryRow("SELECT COUNT(*) FROM volumes WHERE env_id = $1", envID).
		Scan(&count),
	)

	return count
}

func (s *JobVolumesSuite) countOps(envID types.ID) int {
	var count int

	s.NoError(s.conn.
		QueryRow("SELECT COUNT(*) FROM volumes_ops WHERE env_id = $1", envID).
		Scan(&count),
	)

	return count
}

// writeOp writes a physical object and a matching lifecycle op row for the
// environment. It returns the object's physical path.
func (s *JobVolumesSuite) writeOp(envID types.ID, status, name, content string) string {
	dir := filepath.Join(s.tmpdir, fmt.Sprintf("a0e%s", envID), "objects", "tok-"+name)
	s.NoError(os.MkdirAll(dir, 0775))

	objectPath := filepath.Join(dir, name)
	s.NoError(os.WriteFile(objectPath, []byte(content), 0664))

	_, err := s.conn.Exec(`
		INSERT INTO volumes_ops (
			env_id, op_type, status, file_name, file_size,
			mount_type, object, created_at
		)
		VALUES ($1, 'upload', $2, $3, $4, $5, $6, now())
	`,
		envID, status, name, len(content), volumes.FileSys,
		fmt.Sprintf(`{"path": "%s", "name": "%s"}`, dir, name),
	)
	s.NoError(err)

	return objectPath
}

func (s *JobVolumesSuite) Test_RemoveStaleVolumes() {
	app := s.MockApp(nil)

	deletedEnv := s.MockEnv(app, map[string]any{
		"DeletedAt": utils.Unix{Time: time.Now(), Valid: true},
	})

	liveEnv := s.MockEnv(app)

	staleFile := s.writeFile(deletedEnv.ID, "stale.txt")
	liveFile := s.writeFile(liveEnv.ID, "live.txt")

	// Staged object left behind by an interrupted upload of the deleted env.
	staleOpObject := s.writeOp(deletedEnv.ID, "pending", "staged.txt", "bytes")
	// Ops belonging to a live env must be left untouched.
	liveOpObject := s.writeOp(liveEnv.ID, "committed", "committed.txt", "bytes")

	s.NoError(removeStaleVolumes(context.Background(), NewStore()))

	s.Equal(0, s.countVolumes(deletedEnv.ID), "volume rows for the deleted env should be removed")
	s.NoFileExists(staleFile.FullPath(), "physical file for the deleted env should be removed")
	s.Equal(0, s.countOps(deletedEnv.ID), "ops rows for the deleted env should be removed")
	s.NoFileExists(staleOpObject, "staged object for the deleted env should be removed")

	s.Equal(1, s.countVolumes(liveEnv.ID), "volume rows for the live env must be preserved")
	s.FileExists(liveFile.FullPath(), "physical file for the live env must be preserved")
	s.Equal(1, s.countOps(liveEnv.ID), "ops rows for the live env must be preserved")
	s.FileExists(liveOpObject, "physical object for the live env must be preserved")
}

// Test_RemoveStaleVolumes_OpsOnlyEnv purges a deleted environment that only
// has in-flight op rows left (its volumes rows were deleted earlier).
func (s *JobVolumesSuite) Test_RemoveStaleVolumes_OpsOnlyEnv() {
	app := s.MockApp(nil)

	deletedEnv := s.MockEnv(app, map[string]any{
		"DeletedAt": utils.Unix{Time: time.Now(), Valid: true},
	})

	staleOpObject := s.writeOp(deletedEnv.ID, "failed", "staged.txt", "bytes")

	s.NoError(removeStaleVolumes(context.Background(), NewStore()))

	s.Equal(0, s.countOps(deletedEnv.ID))
	s.NoFileExists(staleOpObject)
}

// Test_ReconcileVolumeOps aborts an interrupted upload (old pending op with an
// orphan staged object) through the scheduled job entry point.
func (s *JobVolumesSuite) Test_ReconcileVolumeOps() {
	app := s.MockApp(nil)
	env := s.MockEnv(app)
	liveFile := s.writeFile(env.ID, "live.txt")

	orphanDir := filepath.Join(s.tmpdir, fmt.Sprintf("a0e%s", env.ID), "objects", "orphan")
	s.NoError(os.MkdirAll(orphanDir, 0775))
	orphanPath := filepath.Join(orphanDir, "orphan.txt")
	s.NoError(os.WriteFile(orphanPath, []byte("orphan"), 0664))

	_, err := s.conn.Exec(`
		INSERT INTO volumes_ops (
			env_id, op_type, status, file_name, file_size,
			mount_type, object, created_at
		)
		VALUES ($1, 'upload', 'pending', 'orphan.txt', 6, $2, $3, $4)
	`,
		env.ID, volumes.FileSys,
		fmt.Sprintf(`{"path": "%s", "name": "orphan.txt"}`, orphanDir),
		time.Now().Add(-time.Hour),
	)
	s.NoError(err)

	s.NoError(ReconcileVolumeOps(context.Background()))

	s.NoFileExists(orphanPath, "orphan staged object reaped")
	s.Equal(0, s.countOps(env.ID))
	s.FileExists(liveFile.FullPath(), "live object untouched")
	s.Equal(1, s.countVolumes(env.ID))
}

func (s *JobVolumesSuite) Test_RemoveStaleVolumes_NoVolumesConfig() {
	s.NoError(admin.Store().UpsertConfig(context.Background(), admin.InstanceConfig{}))

	app := s.MockApp(nil)

	deletedEnv := s.MockEnv(app, map[string]any{
		"DeletedAt": utils.Unix{Time: time.Now(), Valid: true},
	})

	s.writeFile(deletedEnv.ID, "stale.txt")

	// Without a configured volume backend the physical bytes cannot be removed,
	// so the rows must be left intact for a later run rather than orphaning files.
	s.NoError(removeStaleVolumes(context.Background(), NewStore()))
	s.Equal(1, s.countVolumes(deletedEnv.ID))
}

func TestJobVolumesSuite(t *testing.T) {
	suite.Run(t, &JobVolumesSuite{})
}
