package publicapiv1

import (
	"mime"
	"net/http"

	"github.com/stormkit-io/stormkit-io/src/ce/api/admin"
	"github.com/stormkit-io/stormkit-io/src/ce/api/app/volumes"
	"github.com/stormkit-io/stormkit-io/src/lib/shttp"
	"github.com/stormkit-io/stormkit-io/src/lib/slog"
)

func handlerVolumesPost(req *RequestContext) *shttp.Response {
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
		return &shttp.Response{
			Status: http.StatusBadRequest,
			Data: map[string]string{
				"error": "File storage is not yet configured.",
			},
		}
	}

	files := req.MultipartForm.File["files"]

	if len(files) == 0 {
		return &shttp.Response{
			Status: http.StatusBadRequest,
			Data: map[string]string{
				"error": "At least one file is required. Send files under the \"files\" field.",
			},
		}
	}

	items := []volumes.UploadItem{}

	for _, fileHeader := range files {
		_, params, err := mime.ParseMediaType(fileHeader.Header.Get("Content-Disposition"))

		if err != nil {
			slog.Errorf("cannot parse content-disposition: %s", err.Error())
		}

		if params == nil {
			params = map[string]string{}
		}

		items = append(items, volumes.UploadItem{
			AppID:              req.App.ID,
			EnvID:              req.Env.ID,
			FileHeader:         volumes.FromFileHeader(fileHeader),
			ContentDisposition: params,
		})
	}

	uploadedFiles, failedFiles := volumes.UploadBatch(req.Context(), cfg.VolumesConfig, items)

	return &shttp.Response{
		Status: http.StatusOK,
		Data: map[string]any{
			"files":  filesToJSON(uploadedFiles),
			"failed": failedFiles,
		},
	}
}

func filesToJSON(files []*volumes.File) []map[string]any {
	result := make([]map[string]any, 0, len(files))

	for _, file := range files {
		data := map[string]any{
			"id":        file.ID.String(),
			"name":      file.Name,
			"size":      file.Size,
			"isPublic":  file.IsPublic,
			"createdAt": file.CreatedAt.Unix(),
		}

		if file.IsPublic {
			data["publicLink"] = file.PublicLink()
		}

		if file.UpdatedAt.Valid {
			data["updatedAt"] = file.UpdatedAt.Unix()
		}

		result = append(result, data)
	}

	return result
}
