package archive

import (
	"fmt"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/utility"
	"io"

	"github.com/wal-g/wal-g/storages/storage"
)

// ErrWaiter
type ErrWaiter interface {
	Wait() error
}

// Uploader defines interface to store mongodb backups and oplog archives
type Uploader interface {
	UploadOplogArchive(stream io.Reader, firstTS, lastTS models.Timestamp) error // TODO: rename firstTS
	UploadBackup(stream io.Reader, cmd ErrWaiter, metaProvider BackupMetaProvider) error
	FileExtension() string
}

// Downloader defines interface to fetch mongodb oplog archives
type Downloader interface {
	Sentinel(name string) (StreamSentinelDto, error) // TODO: reformat backup json, we use text for now
	DownloadOplogArchive(arch models.Archive, writeCloser io.WriteCloser) error
	ListOplogArchives() ([]models.Archive, error)
}

// StorageDownloader extends base folder with mongodb specific.
type StorageDownloader struct {
	folder storage.Folder
}

// NewStorageDownloader builds mongodb downloader.
func NewStorageDownloader(path string) (*StorageDownloader, error) {
	folder, err := internal.ConfigureFolder()
	if err != nil {
		return nil, err
	}
	if path != "" {
		folder = folder.GetSubFolder(path)
	}
	return &StorageDownloader{folder}, nil
}

// Sentinel downloads sentinel contents.
func (sd *StorageDownloader) Sentinel(name string) (StreamSentinelDto, error) {
	backup := internal.NewBackup(sd.folder.GetSubFolder(utility.BaseBackupPath), name)
	var sentinel StreamSentinelDto
	err := internal.FetchStreamSentinel(backup, &sentinel)
	if err != nil {
		return StreamSentinelDto{}, fmt.Errorf("can not fetch stream sentinel: %w", err)
	}
	return sentinel, nil
}

// DownloadOplogArchive downloads, decompresses and decrypts (if needed) oplog archive.
func (sd *StorageDownloader) DownloadOplogArchive(arch models.Archive, writeCloser io.WriteCloser) error {
	return internal.DownloadFile(sd.folder, arch.Filename(), arch.Extension(), writeCloser)
}

// ListOplogArchives fetches all oplog archives existed in storage.
func (sd *StorageDownloader) ListOplogArchives() ([]models.Archive, error) {
	objects, _, err := sd.folder.ListFolder()
	if err != nil {
		return nil, fmt.Errorf("can not list archive folder: %w", err)
	}

	archives := make([]models.Archive, 0, len(objects))
	for _, key := range objects {
		archName := key.GetName()
		arch, err := models.ArchFromFilename(archName)
		if err != nil {
			return nil, fmt.Errorf("can not convert retrieve timestamps since oplog archive Ext '%s': %w", archName, err)
		}
		archives = append(archives, arch)
	}
	return archives, nil
}

// StorageUploader extends base uploader with mongodb specific.
type StorageUploader struct {
	*internal.Uploader
}

// NewStorageUploader builds mongodb uploader.
func NewStorageUploader(path string) (*StorageUploader, error) {
	uploader, err := internal.ConfigureUploader()
	if err != nil {
		return nil, err
	}
	if path != "" {
		uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(path)
	}
	return &StorageUploader{uploader}, nil
}

// UploadOplogArchive compresses a stream and uploads it with given archive name.
func (su *StorageUploader) UploadOplogArchive(stream io.Reader, firstTS, lastTS models.Timestamp) error {
	arch, err := models.NewArchive(firstTS, lastTS, su.FileExtension())
	if err != nil {
		return fmt.Errorf("can not build archive: %w", err)
	}

	if err := su.PushStreamToDestination(stream, arch.Filename()); err != nil {
		return fmt.Errorf("error while uploading stream: %w", err)
	}
	return nil
}

// UploadBackup compresses a stream and uploads it.
func (su *StorageUploader) UploadBackup(stream io.Reader, cmd ErrWaiter, metaProvider BackupMetaProvider) error {
	timeStart := utility.TimeNowCrossPlatformLocal()
	backupName, err := su.PushStream(stream)
	if err != nil {
		return err
	}

	if err := metaProvider.Finalize(); err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	currentBackupSentinelDto := &StreamSentinelDto{
		StartLocalTime:  timeStart,
		FinishLocalTime: utility.TimeNowCrossPlatformLocal(),
		UserData:        internal.GetSentinelUserData(),
		MongoMeta:       metaProvider.Meta(),
	}
	return internal.UploadSentinel(su.Uploader, currentBackupSentinelDto, backupName)
}

// FileExtension returns current file extension (based on configured compression)
func (su *StorageUploader) FileExtension() string {
	return su.Compressor.FileExtension()
}
