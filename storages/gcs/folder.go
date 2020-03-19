package gcs

import (
	"context"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/storages/storage"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const (
	ContextTimeout        = "GCS_CONTEXT_TIMEOUT"
	NormalizePrefix       = "GCS_NORMALIZE_PREFIX"
	defaultContextTimeout = 60 * 60 // 1 hour
)

var SettingList = []string{
	ContextTimeout,
	NormalizePrefix,
}

func NewError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "GCS", format, args...)
}

func NewFolder(bucket *gcs.BucketHandle, path string, contextTimeout int, normalizePrefix bool) *Folder {
	return &Folder{bucket, path, contextTimeout, normalizePrefix}
}

func ConfigureFolder(prefix string, settings map[string]string) (storage.Folder, error) {
	normalizePrefix := true

	if normalizePrefixStr, ok := settings[NormalizePrefix]; ok {
		var err error
		normalizePrefix, err = strconv.ParseBool(normalizePrefixStr)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse %s", NormalizePrefix)
		}
	}

	ctx := context.Background()

	client, err := gcs.NewClient(ctx)
	if err != nil {
		return nil, NewError(err, "Unable to create client")
	}

	var bucketName, path string
	if normalizePrefix {
		bucketName, path, err = storage.GetPathFromPrefix(prefix)
	} else {
		// Special mode for WAL-E compatibility with strange prefixes
		bucketName, path, err = storage.ParsePrefixAsURL(prefix)
		if err == nil && path[0] == '/' {
			path = path[1:]
		}
	}

	if err != nil {
		return nil, NewError(err, "Unable to parse prefix %v", prefix)
	}

	bucket := client.Bucket(bucketName)

	path = storage.AddDelimiterToPath(path)

	contextTimeout := defaultContextTimeout
	if contextTimeoutStr, ok := settings[ContextTimeout]; ok {
		contextTimeout, err = strconv.Atoi(contextTimeoutStr)
		if err != nil {
			return nil, NewError(err, "Unable to parse Context Timeout %v", prefix)
		}
	}
	return NewFolder(bucket, path, contextTimeout, normalizePrefix), nil
}

// Folder represents folder in GCP
type Folder struct {
	bucket          *gcs.BucketHandle
	path            string
	contextTimeout  int
	normalizePrefix bool
}

func (folder *Folder) GetPath() string {
	return folder.path
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	prefix := storage.AddDelimiterToPath(folder.path)
	ctx, cancel := folder.createTimeoutContext()
	defer cancel()
	it := folder.bucket.Objects(ctx, &gcs.Query{Delimiter: "/", Prefix: prefix})
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, NewError(err, "Unable to iterate %v", folder.path)
		}
		if objAttrs.Prefix != "" {

			if objAttrs.Prefix != prefix+"/" {
				// Sometimes GCS returns "//" folder - skip it
				subFolders = append(subFolders, NewFolder(folder.bucket, objAttrs.Prefix, folder.contextTimeout, folder.normalizePrefix))
			}
		} else {
			objName := strings.TrimPrefix(objAttrs.Name, prefix)
			objects = append(objects, storage.NewLocalObject(objName, objAttrs.Updated))
		}
	}
	return
}

func (folder *Folder) createTimeoutContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Second*time.Duration(folder.contextTimeout))
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	for _, objectRelativePath := range objectRelativePaths {
		path := folder.joinPath(folder.path, objectRelativePath)
		object := folder.bucket.Object(path)
		tracelog.DebugLogger.Printf("Delete %v\n", path)
		ctx, cancel := folder.createTimeoutContext()
		defer cancel()
		err := object.Delete(ctx)
		if err != nil && err != gcs.ErrObjectNotExist {
			return NewError(err, "Unable to delete object %v", path)
		}
	}
	return nil
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	path := folder.joinPath(folder.path, objectRelativePath)
	object := folder.bucket.Object(path)
	ctx, cancel := folder.createTimeoutContext()
	defer cancel()
	_, err := object.Attrs(ctx)
	if err == gcs.ErrObjectNotExist {
		return false, nil
	}
	if err != nil {
		return false, NewError(err, "Unable to stat object %v", path)
	}
	return true, nil
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(folder.bucket, folder.joinPath(folder.path, subFolderRelativePath), folder.contextTimeout, folder.normalizePrefix)
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	path := folder.joinPath(folder.path, objectRelativePath)
	object := folder.bucket.Object(path)
	reader, err := object.NewReader(context.Background())
	if err == gcs.ErrObjectNotExist {
		return nil, storage.NewObjectNotFoundError(path)
	}
	return ioutil.NopCloser(reader), err
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.path)
	object := folder.bucket.Object(folder.joinPath(folder.path, name))
	ctx, cancel := folder.createTimeoutContext()
	defer cancel()
	writer := object.NewWriter(ctx)
	_, err := io.Copy(writer, content)
	if err != nil {
		return NewError(err, "Unable to copy to object")
	}
	tracelog.DebugLogger.Printf("Put %v done\n", name)
	err = writer.Close()
	if err != nil {
		return NewError(err, "Unable to Close object")
	}
	return nil
}

func (folder *Folder) joinPath(one string, another string) string {
	if folder.normalizePrefix {
		return storage.JoinPath(one, another)
	}
	if one[len(one)-1] == '/' {
		one = one[:len(one)-1]
	}
	if another[0] == '/' {
		another = another[1:]
	}
	return one + "/" + another
}
