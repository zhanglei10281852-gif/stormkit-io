package volumes

import (
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"sync"

	"github.com/stormkit-io/stormkit-io/src/ce/api/admin"
	"github.com/stormkit-io/stormkit-io/src/lib/types"
	"github.com/stormkit-io/stormkit-io/src/lib/utils"
)

type uploadArgs struct {
	file        multipart.File
	size        int64
	envID       types.ID
	token       string
	logicalName string
	ref         ObjectRef
}

type vmFileSys struct {
	RootPath string
}

var CachedFileSys *vmFileSys
var mux sync.Mutex

// clientFilesys is a singleton function to return either the cached
// file system volume manager or create one from scratch.
func clientFilesys(c *admin.VolumesConfig) *vmFileSys {
	mux.Lock()
	defer mux.Unlock()

	if CachedFileSys == nil {
		CachedFileSys = &vmFileSys{
			RootPath: c.RootPath,
		}
	}

	return CachedFileSys
}

func (fs *vmFileSys) download(file *File) (io.ReadSeeker, error) {
	f, err := os.Open(fs.location(file))

	if os.IsNotExist(err) {
		return nil, nil
	}

	return f, nil
}

// location returns the on-disk path of a file. Rows created through the
// staging lifecycle store their flattened object name in metadata; legacy
// rows fall back to the file name encoded in the row.
func (fs *vmFileSys) location(file *File) string {
	if file.Metadata != nil {
		if objectName, ok := file.Metadata["objectName"].(string); ok && objectName != "" {
			return filepath.Join(file.Path, filepath.Base(objectName))
		}
	}

	return file.FullPath()
}

// stage writes a file into its unique staging directory. Bytes go to a
// temporary file first and are renamed into place, so a crashed or
// interrupted write never leaves a half-written object behind.
// This function assumes that the file has already been sanitized and validated by the caller.
func (fs *vmFileSys) stage(args uploadArgs) (*File, error) {
	destination, err := filepath.Abs(args.ref.Path)

	if err != nil {
		return nil, err
	}

	// Make sure that the folder exists
	if err := os.MkdirAll(destination, 0775); err != nil {
		return nil, err
	}

	objectName := filepath.Base(args.ref.Name)
	finalPath := filepath.Join(destination, objectName)
	tmpPath := filepath.Join(destination, "."+objectName+".tmp-"+args.token)

	dst, err := os.Create(tmpPath)

	if err != nil {
		return nil, err
	}

	size, copyErr := io.Copy(dst, args.file)
	closeErr := dst.Close()

	if copyErr != nil {
		os.Remove(tmpPath)
		return nil, copyErr
	}

	if closeErr != nil {
		os.Remove(tmpPath)
		return nil, closeErr
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return nil, err
	}

	return &File{
		Size:      size,
		Path:      destination,
		Name:      args.logicalName,
		EnvID:     args.envID,
		CreatedAt: utils.NewUnix(),
		Metadata: utils.Map{
			"mountType":  FileSys,
			"objectName": objectName,
		},
	}, nil
}

// removeOne removes a single object. A missing object is not an error.
func (fs *vmFileSys) removeOne(file *File) error {
	if err := os.Remove(fs.location(file)); err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}

	return nil
}

func (fs *vmFileSys) removeFiles(files []*File) ([]*File, error) {
	success := []*File{}

	for _, file := range files {
		if err := fs.removeOne(file); err != nil {
			return success, err
		}

		success = append(success, file)
	}

	return success, nil
}
