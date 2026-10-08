package volumes

import (
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/stormkit-io/stormkit-io/src/ce/api/admin"
	"github.com/stormkit-io/stormkit-io/src/lib/shttp"
	"github.com/stormkit-io/stormkit-io/src/lib/types"
	"github.com/stormkit-io/stormkit-io/src/lib/utils"
)

const (
	FileSys    = "filesys"
	AWSS3      = "aws:s3"
	AlibabaOSS = "alibaba:oss"
	HCloudOSS  = "hcloud:oss"
)

const (
	// objectsDir holds staged and published upload objects. Each upload gets
	// a unique sub-directory, so replacing a file never overwrites the bytes
	// a still-published row points at until the metadata transaction commits.
	objectsDir = "objects"

	// stagingTokenLength is the entropy of a staged object directory.
	stagingTokenLength = 24
)

var (
	MaxUploadSize     = int64(50 << 20)  // 50 MB
	UploadMemoryLimit = int64(100 << 20) // 100 MB
)

func init() {
	if v := os.Getenv("STORMKIT_VOLUMES_MAX_UPLOAD_SIZE"); v != "" {
		if size, err := strconv.ParseInt(v, 10, 64); err == nil {
			MaxUploadSize = size
		}
	}

	if v := os.Getenv("STORMKIT_VOLUMES_UPLOAD_MEMORY_LIMIT"); v != "" {
		if size, err := strconv.ParseInt(v, 10, 64); err == nil {
			UploadMemoryLimit = size
		}
	}
}

type File struct {
	ID        types.ID
	EnvID     types.ID
	Name      string // The file name with the maintained structure (e.g. test-folder/file.txt)
	Path      string // The absolute path to the file including relative path (/shared/volumes/test-folder)
	Size      int64
	IsPublic  bool
	CreatedAt utils.Unix
	UpdatedAt utils.Unix
	Metadata  utils.Map
}

// FullPath returns the absolute path of the file.
func (f *File) FullPath() string {
	return filepath.Join(f.Path, filepath.Base(f.Name))
}

// PublicLink returns the link to access the file.
func (f *File) PublicLink() string {
	token := utils.EncryptToString(f.ID.String() + ":" + f.EnvID.String())
	return admin.MustConfig().ApiURL(fmt.Sprintf("/volumes/file/%s", token))
}

type UploadArgs struct {
	AppID              types.ID
	EnvID              types.ID
	FileHeader         FileHeader
	ContentDisposition map[string]string
}

// LimitRequestBody returns a middleware that wraps the request body with
// http.MaxBytesReader so that multipart parsing (e.g. triggered inside
// WithAPIKey via req.FormValue) cannot buffer an unbounded body to disk.
// For multipart requests the form is parsed eagerly so that an oversized
// body is caught here and returns a 413 with a clear message, rather than
// surfacing as a nil-dereference later in the handler.
func LimitRequestBody() shttp.RequestFunc {
	return func(req *shttp.RequestContext) *shttp.Response {
		if req.Body == nil || req.Writer() == nil {
			return nil
		}

		req.Body = http.MaxBytesReader(req.Writer(), req.Body, MaxUploadSize)

		if !strings.HasPrefix(strings.ToLower(req.Header.Get("Content-Type")), "multipart/form-data") {
			return nil
		}

		if err := req.ParseMultipartForm(UploadMemoryLimit); err != nil {
			var maxBytesErr *http.MaxBytesError

			if errors.As(err, &maxBytesErr) {
				return &shttp.Response{
					Status: http.StatusRequestEntityTooLarge,
					Data: map[string]string{
						"error": fmt.Sprintf("Request body too large. You can upload up to %dMB at a time.", maxBytesErr.Limit/(1024*1024)),
					},
				}
			}

			return &shttp.Response{
				Status: http.StatusBadRequest,
				Data: map[string]string{
					"error": err.Error(),
				},
			}
		}

		return nil
	}
}

// SanitizeUploadFilename validates and normalizes an uploaded filename to prevent
// path traversal attacks that could write files outside the intended volume root.
func SanitizeUploadFilename(name string) (string, error) {
	name = strings.TrimSpace(name)

	if name == "" {
		return "", fmt.Errorf("filename must not be empty")
	}

	cleaned := filepath.Clean(name)

	if filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("absolute paths are not allowed")
	}

	if cleaned == "." || cleaned == ".." {
		return "", fmt.Errorf("invalid filename")
	}

	sep := string(filepath.Separator)

	if strings.HasPrefix(cleaned, ".."+sep) || strings.Contains(cleaned, sep+".."+sep) {
		return "", fmt.Errorf("path traversal segments are not allowed")
	}

	return cleaned, nil
}

// OpenUpload opens the multipart file and returns its sanitized logical name.
// The caller is responsible for closing the returned reader.
func OpenUpload(args UploadArgs) (multipart.File, string, error) {
	file, err := args.FileHeader.Open()

	if err != nil {
		return nil, "", err
	}

	fileName, err := SanitizeUploadFilename(utils.GetString(args.ContentDisposition["filename"], args.FileHeader.Name()))

	if err != nil {
		file.Close()
		return nil, "", err
	}

	return file, fileName, nil
}

// StagingRef returns the locator of a staged upload object before it is
// written. Objects are flattened into a unique directory per upload so the
// bytes a published row currently points at are never overwritten in place.
func StagingRef(vc *admin.VolumesConfig, appID, envID types.ID, token, fileName string) ObjectRef {
	objectDir := path.Join(constructFilePath(appID, envID), objectsDir, token)

	switch vc.MountType {
	case AWSS3:
		return ObjectRef{
			Path: path.Join(vc.BucketName, objectDir),
			Name: physicalName(fileName),
		}
	default:
		return ObjectRef{
			Path: filepath.Join(vc.RootPath, filepath.FromSlash(objectDir)),
			Name: physicalName(fileName),
		}
	}
}

// StageUpload writes the uploaded bytes to the given unpublished object
// location. It does not create any metadata: callers must have registered an
// upload op first and publish the returned file through Ops().PublishUpload.
// Until that transaction commits, readers keep seeing the previous version.
func StageUpload(vc *admin.VolumesConfig, args UploadArgs, token, fileName string, file multipart.File) (*File, error) {
	opts := uploadArgs{
		file:       file,
		size:       args.FileHeader.Size(),
		envID:      args.EnvID,
		token:      token,
		logicalName: fileName,
		ref:        StagingRef(vc, args.AppID, args.EnvID, token, fileName),
	}

	switch vc.MountType {
	case FileSys:
		return clientFilesys(vc).stage(opts)
	case AWSS3:
		return clientAWS(vc).stage(opts)
	}

	return nil, nil
}

// Download downloads a file from the source.
func Download(vc *admin.VolumesConfig, file *File) (io.ReadSeeker, error) {
	switch vc.MountType {
	case FileSys:
		return clientFilesys(vc).download(file)
	case AWSS3:
		return clientAWS(vc).download(file)
	}

	return nil, nil
}

// RemoveObject removes a single physical object. A missing object is treated
// as success so deletes can be retried safely after a crash.
func RemoveObject(vc *admin.VolumesConfig, ref ObjectRef) error {
	file := ref.file()

	switch vc.MountType {
	case FileSys:
		return clientFilesys(vc).removeOne(file)
	case AWSS3:
		return clientAWS(vc).removeOne(file)
	}

	return nil
}

// RemoveFiles removes files from the source.
// This function returns a list of files that are successfully removed.
// When encountered an error other than os.IsNotExist, returns immediately.
func RemoveFiles(vc *admin.VolumesConfig, files []*File) ([]*File, error) {
	switch vc.MountType {
	case FileSys:
		return clientFilesys(vc).removeFiles(files)
	case AWSS3:
		return clientAWS(vc).removeFiles(files)
	}

	return nil, nil
}

// MountTypeOf returns the backend mount type recorded on a volumes row,
// defaulting to the filesystem backend for legacy rows without metadata.
func MountTypeOf(f *File) string {
	if f.Metadata != nil {
		if mountType, ok := f.Metadata["mountType"].(string); ok && mountType != "" {
			return mountType
		}
	}

	return FileSys
}

// RefOf returns the exact physical object locator of a published volumes row:
// staged rows record their flattened object name in metadata, while legacy
// rows encoded the whole (possibly nested) name in file_name.
func RefOf(f *File) ObjectRef {
	name := f.Name

	if f.Metadata != nil {
		if objectName, ok := f.Metadata["objectName"].(string); ok && objectName != "" {
			name = objectName
		}
	}

	return ObjectRef{Path: f.Path, Name: name}
}

// ObjectRefFromRow builds an object locator from raw volumes columns.
func ObjectRefFromRow(filePath, logicalName string, metadata utils.Map) ObjectRef {
	name := logicalName

	if metadata != nil {
		if objectName, ok := metadata["objectName"].(string); ok && objectName != "" {
			name = objectName
		}
	}

	return ObjectRef{Path: filePath, Name: name}
}

// physicalName flattens a logical name to its base component. Every staged
// upload lives in its own unique directory, so nested logical names do not
// need to preserve their folder structure in the object backend.
func physicalName(fileName string) string {
	return filepath.Base(filepath.FromSlash(fileName))
}

// PhysicalKey returns the backend-specific unique key of an object: the
// absolute path for the filesystem mount and the bucket-relative key for S3.
// Recovery compares keys to make sure it never removes a published object.
func PhysicalKey(mountType string, ref ObjectRef) string {
	switch mountType {
	case AWSS3:
		pieces := strings.SplitN(ref.Path, "/", 2)

		if len(pieces) != 2 {
			return ""
		}

		return path.Join(pieces[1], ref.Name)
	default:
		return filepath.Join(ref.Path, filepath.Base(ref.Name))
	}
}

// metadataValue serialises row metadata for the jsonb column as a JSON string;
// a []byte would be sent as bytea.
func metadataValue(m utils.Map) any {
	v, err := m.Value()

	if err != nil {
		return nil
	}

	if b, ok := v.([]byte); ok {
		return string(b)
	}

	return v
}

func constructFilePath(appID, envID types.ID) string {
	return path.Join(fmt.Sprintf("a%se%s", appID, envID))
}

type FileHeader interface {
	Open() (multipart.File, error)
	Name() string
	Size() int64
}

type fileHeader struct {
	original *multipart.FileHeader
}

func (fh *fileHeader) Open() (multipart.File, error) {
	return fh.original.Open()
}

func (fh *fileHeader) Size() int64 {
	return fh.original.Size
}

func (fh *fileHeader) Name() string {
	return fh.original.Filename
}

func FromFileHeader(file *multipart.FileHeader) FileHeader {
	return &fileHeader{
		original: file,
	}
}
