package internal

import (
	"io"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/tinsane/storages/storage"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/utility"
)

// Uploader contains fields associated with uploading tarballs.
// Multiple tarballs can share one uploader.
type Uploader struct {
	UploadingFolder      storage.Folder
	Compressor           compression.Compressor
	waitGroup            *sync.WaitGroup
	deltaFileManager     *DeltaFileManager
	archiveStatusManager DataFolder
	Failed               atomic.Value
}

// UploadObject
type UploadObject struct {
	Path    string
	Content io.Reader
}

func (uploader *Uploader) getUseWalDelta() (useWalDelta bool) {
	return uploader.deltaFileManager != nil
}

func NewUploader(
	compressor compression.Compressor,
	uploadingLocation storage.Folder,
	deltaFileManager *DeltaFileManager,
	archiveStatusManager DataFolder,
) *Uploader {
	uploader := &Uploader{
		UploadingFolder:      uploadingLocation,
		Compressor:           compressor,
		waitGroup:            &sync.WaitGroup{},
		deltaFileManager:     deltaFileManager,
		archiveStatusManager: archiveStatusManager,
	}
	uploader.Failed.Store(false)
	return uploader
}

// finish waits for all waiting parts to be uploaded. If an error occurs,
// prints alert to stderr.
func (uploader *Uploader) finish() {
	uploader.waitGroup.Wait()
	if uploader.Failed.Load().(bool) {
		tracelog.ErrorLogger.Printf("WAL-G could not complete upload.\n")
	}
}

// Clone creates similar Uploader with new WaitGroup
func (uploader *Uploader) Clone() *Uploader {
	return &Uploader{
		uploader.UploadingFolder,
		uploader.Compressor,
		&sync.WaitGroup{},
		uploader.deltaFileManager,
		uploader.archiveStatusManager,
		uploader.Failed,
	}
}

// TODO : unit tests
func (uploader *Uploader) UploadWalFile(file NamedReader) error {
	var walFileReader io.Reader

	filename := path.Base(file.Name())
	if uploader.getUseWalDelta() && isWalFilename(filename) {
		recordingReader, err := NewWalDeltaRecordingReader(file, filename, uploader.deltaFileManager)
		if err != nil {
			walFileReader = file
		} else {
			walFileReader = recordingReader
			defer utility.LoggedClose(recordingReader, "")
		}
	} else {
		walFileReader = file
	}

	return uploader.UploadFile(NewNamedReaderImpl(walFileReader, file.Name()))
}

// TODO : unit tests
// UploadFile compresses a file and uploads it.
func (uploader *Uploader) UploadFile(file NamedReader) error {
	compressedFile := CompressAndEncrypt(file, uploader.Compressor, ConfigureCrypter())
	dstPath := utility.SanitizePath(filepath.Base(file.Name()) + "." + uploader.Compressor.FileExtension())

	err := uploader.Upload(dstPath, compressedFile)
	tracelog.InfoLogger.Println("FILE PATH:", dstPath)
	return err
}

// TODO : unit tests
func (uploader *Uploader) Upload(path string, content io.Reader) error {
	err := uploader.UploadingFolder.PutObject(path, content)
	if err == nil {
		return nil
	}
	uploader.Failed.Store(true)
	tracelog.ErrorLogger.PrintError(err)
	return err
}

// UploadMultiple uploads multiple objects from the start of the slice,
// returning the first error if any. Note that this operation is not atomic
// TODO : unit tests
func (uploader *Uploader) UploadMultiple(objects []UploadObject) error {
	for _, object := range objects {
		err := uploader.Upload(object.Path, object.Content)
		if err != nil {
			// possibly do a retry here
			return err
		}
	}
	return nil
}
