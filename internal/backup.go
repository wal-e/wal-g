package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

//region errors
type SentinelMarshallingError struct {
	error
}

func NewSentinelMarshallingError(sentinelName string, err error) SentinelMarshallingError {
	return SentinelMarshallingError{errors.Wrapf(err, "Failed to marshall sentinel file: '%s'", sentinelName)}
}

func (err SentinelMarshallingError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

//endregion

// Backup provides basic functionality
// to fetch backup-related information from storage
//
// WAL-G stores information about single backup in the following files:
//
// Sentinel file - contains useful information, such as backup start time, backup size, etc.
// see FetchSentinel, UploadSentinel
//
// Metadata file (only in Postgres) - Postgres sentinel files can be quite large (> 1GB),
// so the metadata file is useful for the quick fetch of backup-related information.
// see FetchMetadata, UploadMetadata
type Backup struct {
	Name string
	// base backup folder or catchup backup folder
	Folder storage.Folder
}

func NewBackup(folder storage.Folder, name string) Backup {
	return Backup{
		Name:   name,
		Folder: folder,
	}
}

// getStopSentinelPath returns sentinel path.
func (backup *Backup) getStopSentinelPath() string {
	return SentinelNameFromBackup(backup.Name)
}

func (backup *Backup) getMetadataPath() string {
	return backup.Name + "/" + utility.MetadataFileName
}

// SentinelExists checks that the sentinel file of the specified backup exists.
func (backup *Backup) SentinelExists() (bool, error) {
	return backup.Folder.Exists(backup.getStopSentinelPath())
}

// TODO : unit tests
func (backup *Backup) FetchSentinel(sentinelDto interface{}) error {
	sentinelDtoData, err := backup.fetchStorageBytes(backup.getStopSentinelPath())
	if err != nil {
		return errors.Wrap(err, "failed to fetch sentinel")
	}
	err = json.Unmarshal(sentinelDtoData, sentinelDto)
	return errors.Wrap(err, "failed to unmarshal sentinel")
}

// TODO : unit tests
func (backup *Backup) FetchMetadata(metadataDto interface{}) error {
	sentinelDtoData, err := backup.fetchStorageBytes(backup.getMetadataPath())
	if err != nil {
		return errors.Wrap(err, "failed to fetch metadata")
	}
	err = json.Unmarshal(sentinelDtoData, metadataDto)
	return errors.Wrap(err, "failed to unmarshal metadata")
}

func (backup *Backup) fetchStorageBytes(path string) ([]byte, error) {
	backupReaderMaker := NewStorageReaderMaker(backup.Folder, path)
	backupReader, err := backupReaderMaker.Reader()
	if err != nil {
		return make([]byte, 0), err
	}
	metadata, err := ioutil.ReadAll(backupReader)
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

func (backup *Backup) UploadMetadata(metadataDto interface{}) error {
	metaFilePath := backup.getMetadataPath()
	dtoBody, err := json.Marshal(metadataDto)
	if err != nil {
		return err
	}
	return backup.Folder.PutObject(metaFilePath, bytes.NewReader(dtoBody))
}

func (backup *Backup) UploadSentinel(sentinelDto interface{}) error {
	sentinelPath := backup.getStopSentinelPath()
	dtoBody, err := json.Marshal(sentinelDto)
	if err != nil {
		return err
	}
	return backup.Folder.PutObject(sentinelPath, bytes.NewReader(dtoBody))
}

func (backup *Backup) CheckExistence() (bool, error) {
	exists, err := backup.SentinelExists()
	if err != nil {
		return false, errors.Wrap(err, "failed to check if backup sentinel exists")
	}
	return exists, nil
}

// AssureExists is similar to CheckExistence, but returns
// an error in two cases:
// 1. Backup does not exist
// 2. Failed to check if backup exist
func (backup *Backup) AssureExists() error {
	exists, err := backup.CheckExistence()
	if err != nil {
		return err
	}
	if !exists {
		return NewBackupNonExistenceError(backup.Name)
	}
	return nil
}

func GetBackupByName(backupName, subfolder string, folder storage.Folder) (Backup, error) {
	baseBackupFolder := folder.GetSubFolder(subfolder)

	var backup Backup
	if backupName == LatestString {
		latest, err := GetLatestBackupName(baseBackupFolder)
		if err != nil {
			return Backup{}, err
		}
		tracelog.InfoLogger.Printf("LATEST backup is: '%s'\n", latest)

		backup = NewBackup(baseBackupFolder, latest)
	} else {
		backup = NewBackup(baseBackupFolder, backupName)
		if err := backup.AssureExists(); err != nil {
			return Backup{}, err
		}
	}
	return backup, nil
}

func SelectBackup(folder storage.Folder,
	targetBackupSelector BackupSelector) (Backup, error) {
	backupName, err := targetBackupSelector.Select(folder)
	if err != nil {
		return Backup{}, err
	}
	tracelog.DebugLogger.Printf("SelectBackup(%s, folder,)\n", backupName)
	return GetBackupByName(backupName, utility.BaseBackupPath, folder)
}

// TODO : unit tests
func UploadSentinel(uploader UploaderProvider, sentinelDto interface{}, backupName string) error {
	sentinelName := SentinelNameFromBackup(backupName)

	dtoBody, err := json.Marshal(sentinelDto)
	if err != nil {
		return NewSentinelMarshallingError(sentinelName, err)
	}

	return uploader.Upload(sentinelName, bytes.NewReader(dtoBody))
}

type ErrWaiter interface {
	Wait() error
}

// MetaConstructor - interface that helps with building meta-info about backup and generate MetaInfo
// see MongoMetaConstructor
// see RedisMetaConstructor
type MetaConstructor interface {
	Init() error
	Finalize(backupName string) error
	MetaInfo() interface{}
}
