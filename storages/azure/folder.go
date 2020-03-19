package azure

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/wal-g/wal-g/storages/storage"
	"github.com/wal-g/tracelog"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/pkg/errors"
)

const (
	AccountSetting    = "AZURE_STORAGE_ACCOUNT"
	AccessKeySetting  = "AZURE_STORAGE_ACCESS_KEY"
	BufferSizeSetting = "AZURE_BUFFER_SIZE"
	MaxBuffersSetting = "AZURE_MAX_BUFFERS"
	TryTimeoutSetting = "AZURE_TRY_TIMEOUT"
	minBufferSize     = 1024
	defaultBufferSize = 64 * 1024 * 1024
	minBuffers        = 1
	defaultBuffers    = 3
	defaultTryTimeout = 5
)

var SettingList = []string{
	AccountSetting,
	AccessKeySetting,
	BufferSizeSetting,
	MaxBuffersSetting,
}

func NewFolderError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "Azure", format, args...)
}

func NewCredentialError(settingName string) storage.Error {
	return NewFolderError(errors.New("Credential error"),
		"%s setting is not set", settingName)
}

func NewFolder(
	uploadStreamToBlockBlobOptions azblob.UploadStreamToBlockBlobOptions,
	containerURL azblob.ContainerURL,
	path string) *Folder {
	return &Folder{uploadStreamToBlockBlobOptions, containerURL, path}
}

func ConfigureFolder(prefix string, settings map[string]string) (storage.Folder, error) {
	var accountName, accountKey string
	var ok bool
	if accountName, ok = settings[AccountSetting]; !ok {
		return nil, NewCredentialError(AccountSetting)
	}
	if accountKey, ok = settings[AccessKeySetting]; !ok {
		return nil, NewCredentialError(AccessKeySetting)
	}
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, NewFolderError(err, "Unable to create credentials")
	}

	var tryTimeout int
	if strTryTimeout, ok := settings[TryTimeoutSetting]; ok {
		tryTimeout, err = strconv.Atoi(strTryTimeout)
		if err != nil {
			return nil, NewFolderError(err, "Invalid azure try timeout setting")
		}
	} else {
		tryTimeout = defaultTryTimeout
	}

	pipeLine := azblob.NewPipeline(credential, azblob.PipelineOptions{Retry: azblob.RetryOptions{TryTimeout: time.Duration(tryTimeout) * time.Minute}})
	containerName, path, err := storage.GetPathFromPrefix(prefix)
	if err != nil {
		return nil, NewFolderError(err, "Unable to create container")
	}
	serviceURL, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))
	if err != nil {
		return nil, NewFolderError(err, "Unable to parse service URL")
	}
	containerURL := azblob.NewContainerURL(*serviceURL, pipeLine)
	path = storage.AddDelimiterToPath(path)
	return NewFolder(getUploadStreamToBlockBlobOptions(settings), containerURL, path), nil
}

type Folder struct {
	uploadStreamToBlockBlobOptions azblob.UploadStreamToBlockBlobOptions
	containerURL                   azblob.ContainerURL
	path                           string
}

func (folder *Folder) GetPath() string {
	return folder.path
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	path := storage.JoinPath(folder.path, objectRelativePath)
	ctx := context.Background()
	blobURL := folder.containerURL.NewBlockBlobURL(path)
	_, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
	if stgErr, ok := err.(azblob.StorageError); ok && stgErr.ServiceCode() == azblob.ServiceCodeBlobNotFound {
		return false, nil
	}
	if err != nil {
		return false, NewFolderError(err, "Unable to stat object %v", path)
	}
	return true, nil
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	//Marker is used for segmented iteration.
	for marker := (azblob.Marker{}); marker.NotDone(); {

		blobs, err := folder.containerURL.ListBlobsHierarchySegment(context.Background(), marker, "/", azblob.ListBlobsSegmentOptions{Prefix: folder.path})
		if err != nil {
			return nil, nil, NewFolderError(err, "Unable to iterate %v", folder.path)
		}
		//add blobs to the list of storage objects
		for _, blob := range blobs.Segment.BlobItems {
			objName := strings.TrimPrefix(blob.Name, folder.path)
			updated := time.Time(blob.Properties.LastModified)

			objects = append(objects, storage.NewLocalObject(objName, updated))
		}

		marker = blobs.NextMarker
		//Get subFolder names
		blobPrefixes := blobs.Segment.BlobPrefixes
		//add subFolders to the list of storage folders
		for _, blobPrefix := range blobPrefixes {
			subFolderPath := blobPrefix.Name

			subFolders = append(subFolders, NewFolder(folder.uploadStreamToBlockBlobOptions, folder.containerURL, subFolderPath))
		}

	}
	return
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(
		folder.uploadStreamToBlockBlobOptions,
		folder.containerURL,
		storage.AddDelimiterToPath(storage.JoinPath(folder.path, subFolderRelativePath)))
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	//Download blob using blobURL obtained from full path to blob
	path := storage.JoinPath(folder.path, objectRelativePath)
	blobURL := folder.containerURL.NewBlockBlobURL(path)
	downloadResponse, err := blobURL.Download(context.Background(), 0, 0, azblob.BlobAccessConditions{}, false)
	if stgErr, ok := err.(azblob.StorageError); ok && stgErr.ServiceCode() == azblob.ServiceCodeBlobNotFound {
		return nil, storage.NewObjectNotFoundError(path)
	}

	if err != nil {
		return nil, NewFolderError(err, "Unable to download blob %s.", path)
	}
	//retrieve and return the downloaded content
	content := downloadResponse.Body(azblob.RetryReaderOptions{})
	return content, nil
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.path)
	//Upload content to a block blob using full path
	path := storage.JoinPath(folder.path, name)
	blobURL := folder.containerURL.NewBlockBlobURL(path)
	_, err := azblob.UploadStreamToBlockBlob(context.Background(), content, blobURL, folder.uploadStreamToBlockBlobOptions)
	if err != nil {
		return NewFolderError(err, "Unable to upload blob %v", name)
	}

	tracelog.DebugLogger.Printf("Put %v done\n", name)
	return nil
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	for _, objectRelativePath := range objectRelativePaths {
		//Delete blob using blobURL obtained from full path to blob
		path := storage.JoinPath(folder.path, objectRelativePath)
		blobURL := folder.containerURL.NewBlockBlobURL(path)
		tracelog.DebugLogger.Printf("Delete %v\n", path)
		_, err := blobURL.Delete(context.Background(), azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
		if stgErr, ok := err.(azblob.StorageError); ok && stgErr.ServiceCode() == azblob.ServiceCodeBlobNotFound {
			continue
		}
		if err != nil {
			return NewFolderError(err, "Unable to delete object %v", path)
		} else {
			//blob is deleted
		}
	}
	return nil
}

func getUploadStreamToBlockBlobOptions(settings map[string]string) azblob.UploadStreamToBlockBlobOptions {
	// Configure the size of the rotating buffers
	bufSizeS := settings[BufferSizeSetting]
	bufferSize, err := strconv.Atoi(bufSizeS)
	if err != nil || bufferSize < minBufferSize {
		bufferSize = defaultBufferSize
	}
	// Configure the size of the rotating buffers and number of buffers
	maxBufS := settings[MaxBuffersSetting]
	maxBuffers, err := strconv.Atoi(maxBufS)
	if err != nil || maxBuffers < minBuffers {
		maxBuffers = defaultBuffers
	}
	return azblob.UploadStreamToBlockBlobOptions{MaxBuffers: maxBuffers, BufferSize: bufferSize}
}
