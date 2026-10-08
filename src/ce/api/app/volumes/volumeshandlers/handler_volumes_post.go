package volumeshandlers

import (
	"mime"
	"net/http"

	"github.com/stormkit-io/stormkit-io/src/ce/api/admin"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/volumes"
	"github.com/stormkit-io/stormkit-io/src/lib/shttp"
	"github.com/stormkit-io/stormkit-io/src/lib/slog"
)

func HandlerVolumesPost(req *app.RequestContext) *shttp.Response {
	if req.MultipartForm == nil {
		return &shttp.Response{
			Status: http.StatusBadRequest,
			Data: map[string]string{
				"error": "Invalid request: expected multipart/form-data with files under the \"files\" field.",
			},
		}
	}

	cfg, err := admin.Store().Config(req.Context())

	if err != nil {
		return shttp.Error(err)
	}

	if cfg.VolumesConfig == nil {
		return volumesNotConfigured()
	}

	items := []volumes.UploadItem{}

	for _, fileHeader := range req.MultipartForm.File["files"] {
		_, params, err := mime.ParseMediaType(fileHeader.Header.Get("Content-Disposition"))

		if err != nil {
			slog.Errorf("cannot parse content-disposition: %s", err.Error())
		}

		if params == nil {
			params = map[string]string{}
		}

		items = append(items, volumes.UploadItem{
			AppID:              req.App.ID,
			EnvID:              req.EnvID,
			FileHeader:         volumes.FromFileHeader(fileHeader),
			ContentDisposition: params,
		})
	}

	uploadedFiles, failedFiles := volumes.UploadBatch(req.Context(), cfg.VolumesConfig, items)

	return &shttp.Response{
		Status: http.StatusOK,
		Data: map[string]any{
			"files":  toJSON(uploadedFiles),
			"failed": failedFiles,
		},
	}
}
