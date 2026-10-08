package volumeshandlers_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stormkit-io/stormkit-io/src/ce/api/admin"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/volumes"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/volumes/volumeshandlers"
	"github.com/stormkit-io/stormkit-io/src/ce/api/user"
	"github.com/stormkit-io/stormkit-io/src/ce/api/user/usertest"
	"github.com/stormkit-io/stormkit-io/src/lib/database/databasetest"
	"github.com/stormkit-io/stormkit-io/src/lib/factory"
	"github.com/stormkit-io/stormkit-io/src/lib/shttp"
	"github.com/stormkit-io/stormkit-io/src/lib/shttp/shttptest"
	"github.com/stormkit-io/stormkit-io/src/lib/types"
	"github.com/stormkit-io/stormkit-io/src/lib/utils"
	"github.com/stretchr/testify/suite"
)

// HandlerVolumesLifecycleSuite covers the end-to-end invariant of the volume
// file lifecycle: after a same-name replacement commits, list, download,
// public-file and capacity stat all resolve to the same version, and a
// failing physical delete keeps a retryable row instead of an empty one.
type HandlerVolumesLifecycleSuite struct {
	suite.Suite
	*factory.Factory
	conn   databasetest.TestDB
	tmpdir string
}

func (s *HandlerVolumesLifecycleSuite) BeforeTest(suiteName, _ string) {
	s.conn = databasetest.InitTx(suiteName)
	s.Factory = factory.New(s.conn)
	admin.ResetCache(context.Background())
	admin.SetMockLicense()
	volumes.CachedFileSys = nil

	// Sequences reset per transaction, so env ids repeat across tests; keep
	// the physical backing isolated too.
	tmpDir, err := os.MkdirTemp("", "tmp-volumes-lifecycle-")
	s.Require().NoError(err)
	s.tmpdir = tmpDir

	s.NoError(admin.Store().UpsertConfig(context.Background(), admin.InstanceConfig{
		VolumesConfig: &admin.VolumesConfig{
			MountType: volumes.FileSys,
			RootPath:  s.tmpdir,
		},
	}))
}

func (s *HandlerVolumesLifecycleSuite) AfterTest(_, _ string) {
	s.conn.CloseTx()
	admin.ResetMockLicense()
	os.RemoveAll(s.tmpdir)
}

func (s *HandlerVolumesLifecycleSuite) handler() http.Handler {
	return shttp.NewRouter().RegisterService(volumeshandlers.Services).Router().Handler()
}

func (s *HandlerVolumesLifecycleSuite) upload(usrID, appID, envID types.ID, name, content string) map[string]any {
	requestBody, contentType, err := shttptest.MultipartForm(map[string][]byte{
		"appId": []byte(appID.String()),
		"envId": []byte(envID.String()),
	}, map[string][]shttptest.UploadFile{
		"files": {{Name: name, Data: content}},
	})
	s.Require().NoError(err)

	response := shttptest.RequestWithHeaders(
		s.handler(), shttp.MethodPost, "/volumes", requestBody,
		map[string]string{
			"Content-Type":  contentType,
			"Authorization": usertest.Authorization(usrID),
		},
	)
	if response.Code != http.StatusOK {
		s.T().Fatalf("upload failed: code=%d body=%s", response.Code, response.String())
	}

	files := response.Map()["files"].([]any)
	s.Require().Len(files, 1)

	return files[0].(map[string]any)
}

func (s *HandlerVolumesLifecycleSuite) setVisibility(usrID, appID, envID, fileID types.ID, visibility string) {
	response := shttptest.RequestWithHeaders(
		s.handler(), shttp.MethodPost,
		"/volumes/visibility",
		map[string]any{
			"appId":      appID.String(),
			"envId":      envID.String(),
			"fileId":     fileID.String(),
			"visibility": visibility,
		},
		map[string]string{"Authorization": usertest.Authorization(usrID)},
	)
	s.Require().Equal(http.StatusOK, response.Code, response.String())
}

func (s *HandlerVolumesLifecycleSuite) fileByID(envID, fileID types.ID) *volumes.File {
	rows, err := volumes.Store().SelectFiles(context.Background(), volumes.SelectFilesArgs{
		EnvID:  envID,
		FileID: []types.ID{fileID},
	})
	s.Require().NoError(err)
	s.Require().Len(rows, 1)

	return rows[0]
}

// countRegularFiles walks the environment's object prefix and counts the
// physical objects it still contains.
func (s *HandlerVolumesLifecycleSuite) countRegularFiles(appID, envID types.ID) int {
	count := 0
	root := filepath.Join(s.tmpdir, fmt.Sprintf("a%se%s", appID, envID))

	s.NoError(filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			count++
		}

		return nil
	}))

	return count
}

// Test_Replacement_AllViewsPointAtSameVersion verifies the single-version
// publication guarantee across every read path.
func (s *HandlerVolumesLifecycleSuite) Test_Replacement_AllViewsPointAtSameVersion() {
	usr := s.MockUser()
	app := s.MockApp(usr)
	env := s.MockEnv(app)

	first := s.upload(usr.ID, app.ID, env.ID, "doc.txt", "v1")
	fileID := utils.StringToID(first["id"].(string))

	// Public links derive from the stable file id and must survive replacement.
	s.setVisibility(usr.ID, app.ID, env.ID, fileID, "public")

	v1 := s.fileByID(env.ID, fileID)
	v1Path := volumes.PhysicalKey(volumes.FileSys, volumes.RefOf(v1))
	s.FileExists(v1Path)
	linkV1 := v1.PublicLink()

	// Replace with content of a different length so a stale size would be
	// observable immediately.
	second := s.upload(usr.ID, app.ID, env.ID, "doc.txt", "second-version")
	s.Equal(first["id"], second["id"], "file id must stay stable")

	// List points at the new version.
	response := shttptest.RequestWithHeaders(
		s.handler(), shttp.MethodGet,
		fmt.Sprintf("/volumes?appId=%s&envId=%s", app.ID.String(), env.ID.String()),
		nil,
		map[string]string{"Authorization": usertest.Authorization(usr.ID)},
	)
	s.Equal(http.StatusOK, response.Code)

	listed := response.Map()["files"].([]any)[0].(map[string]any)
	s.Equal(float64(len("second-version")), listed["size"])
	s.Equal(true, listed["isPublic"], "visibility is preserved by replacement")

	// Encryption uses a random nonce, so compare the decoded token: the
	// stable file id keeps the public link pointing at the same file.
	listedLink, ok := listed["publicLink"].(string)
	s.Require().True(ok)
	s.Equal(
		utils.DecryptToString(strings.TrimPrefix(linkV1, admin.MustConfig().ApiURL("/volumes/file/"))),
		utils.DecryptToString(strings.TrimPrefix(listedLink, admin.MustConfig().ApiURL("/volumes/file/"))),
		"public link must reference the same file id after replacement",
	)

	// Capacity stat points at the new version.
	response = shttptest.RequestWithHeaders(
		s.handler(), shttp.MethodGet,
		fmt.Sprintf("/volumes/size?appId=%s&envId=%s", app.ID.String(), env.ID.String()),
		nil,
		map[string]string{"Authorization": usertest.Authorization(usr.ID)},
	)
	s.Equal(http.StatusOK, response.Code)
	s.JSONEq(fmt.Sprintf(`{"size": %d}`, len("second-version")), response.String())

	// Authenticated download serves the new bytes.
	token, err := user.JWT(user.JWTParams{Purpose: user.PurposeVolumeDownload, Claims: jwt.MapClaims{
		"token":  strings.Replace(usertest.Authorization(usr.ID), "Bearer ", "", 1),
		"appId":  app.ID.String(),
		"envId":  env.ID.String(),
		"fileId": fileID.String(),
	}})
	s.NoError(err)

	response = shttptest.RequestWithHeaders(
		s.handler(), shttp.MethodGet,
		fmt.Sprintf("/volumes/download?token=%s", token),
		nil, nil,
	)
	downloadBody := response.String()
	s.Equal(http.StatusOK, response.Code, downloadBody)
	s.Equal("second-version", downloadBody)

	// Public file serves the new bytes through the same stable link.
	parsed, err := url.Parse(linkV1)
	s.NoError(err)

	response = shttptest.RequestWithHeaders(s.handler(), shttp.MethodGet, parsed.Path, nil, nil)
	publicBody := response.String()
	s.Equal(http.StatusOK, response.Code, publicBody)
	s.Equal("second-version", publicBody)

	// The displaced object is gone, only the live version occupies space.
	s.Equal(1, s.countRegularFiles(app.ID, env.ID), "old object must be removed after replacement")
	s.NoFileExists(v1Path)
}

// Test_Delete_PhysicalFailureIsRetryable verifies a failed physical delete
// reports the file under "failed" and leaves its row downloadable; a later
// recovery attempt consumes both object and row.
func (s *HandlerVolumesLifecycleSuite) Test_Delete_PhysicalFailureIsRetryable() {
	usr := s.MockUser()
	app := s.MockApp(usr)
	env := s.MockEnv(app)

	uploaded := s.upload(usr.ID, app.ID, env.ID, "doc.txt", "v1")
	fileID := utils.StringToID(uploaded["id"].(string))
	originalPath := s.fileByID(env.ID, fileID).Path

	// Repoint at a location where physical removal deterministically fails
	// (ENOTDIR on /dev/null), independently of process privileges.
	_, err := s.conn.Exec(`UPDATE volumes SET file_path = $2 WHERE file_id = $1`, fileID, "/dev/null")
	s.Require().NoError(err)

	response := shttptest.RequestWithHeaders(
		s.handler(), shttp.MethodDelete,
		fmt.Sprintf("/volumes?appId=%s&envId=%s&id=%s", app.ID.String(), env.ID.String(), fileID.String()),
		nil,
		map[string]string{"Authorization": usertest.Authorization(usr.ID)},
	)
	body := response.Map()

	if response.Code != http.StatusOK {
		s.T().Fatalf("delete failed: code=%d body=%v", response.Code, body)
	}

	s.Contains(body["failed"].(map[string]any), fileID.String())
	s.Empty(body["removed"])

	// Row remains: metadata is still retryable instead of an empty row.
	s.NotNil(s.fileByID(env.ID, fileID))

	// Backend recovers: metadata and the pending op point back at the real
	// object and reconciliation completes the delete.
	_, err = s.conn.Exec(`UPDATE volumes SET file_path = $2 WHERE file_id = $1`, fileID, originalPath)
	s.Require().NoError(err)
	_, err = s.conn.Exec(`
		UPDATE volumes_ops
		SET object = jsonb_set(object, '{path}', to_jsonb($2::text))
		WHERE file_id = $1 AND op_type = 'delete'
	`, fileID, originalPath)
	s.Require().NoError(err)

	cfg, err := admin.Store().Config(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(volumes.ReconcileOps(context.Background(), cfg.VolumesConfig))

	rows, err := volumes.Store().SelectFiles(context.Background(), volumes.SelectFilesArgs{
		EnvID:  env.ID,
		FileID: []types.ID{fileID},
	})
	s.NoError(err)
	s.Empty(rows)
}

func TestHandlerVolumesLifecycle(t *testing.T) {
	suite.Run(t, &HandlerVolumesLifecycleSuite{})
}
