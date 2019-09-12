package internal

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"

	"github.com/pkg/errors"
	"github.com/tinsane/storages/storage"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/utility"
)

const (
	TarPartitionFolderName = "/tar_partitions/"
	PgControlPath          = "/global/pg_control"
)

var UnwrapAll map[string]bool = nil

var UtilityFilePaths = map[string]bool{
	PgControlPath:         true,
	BackupLabelFilename:   true,
	TablespaceMapFilename: true,
}

type NoBackupsFoundError struct {
	error
}

func NewNoBackupsFoundError() NoBackupsFoundError {
	return NoBackupsFoundError{errors.New("No backups found")}
}

func (err NoBackupsFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// Backup contains information about a valid backup
// generated and uploaded by WAL-G.
type Backup struct {
	Folder      storage.Folder
	Name        string
	SentinelDto *BackupSentinelDto // used for storage query caching
	Metadata    *ExtendedMetadataDto
}

func NewBackup(baseBackupFolder storage.Folder, name string) *Backup {
	return &Backup{baseBackupFolder, name, nil, nil}
}

func (backup *Backup) GetStopSentinelPath() string {
	return backup.Name + utility.SentinelSuffix
}

func (backup *Backup) GetMetadataPath() string {
	return backup.Name + "/" + utility.MetadataFileName
}

func (backup *Backup) getTarPartitionFolder() storage.Folder {
	return backup.Folder.GetSubFolder(backup.Name + TarPartitionFolderName)
}

// CheckExistence checks that the specified backup exists.
func (backup *Backup) CheckExistence() (bool, error) {
	return backup.Folder.Exists(backup.GetStopSentinelPath())
}

func (backup *Backup) GetTarNames() ([]string, error) {
	tarPartitionFolder := backup.getTarPartitionFolder()
	objects, _, err := tarPartitionFolder.ListFolder()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to list backup '%s' for deletion", backup.Name)
	}
	result := make([]string, len(objects))
	for id, object := range objects {
		result[id] = object.GetName()
	}
	return result, nil
}

func (backup *Backup) GetSentinel() (BackupSentinelDto, error) {
	if backup.SentinelDto != nil {
		return *backup.SentinelDto, nil
	}
	sentinelDto := BackupSentinelDto{}
	sentinelDtoData, err := backup.FetchSentinelData()
	if err != nil {
		return sentinelDto, err
	}

	err = json.Unmarshal(sentinelDtoData, &sentinelDto)
	if err != nil {
		return sentinelDto, errors.Wrap(err, "failed to unmarshal sentinel")
	}
	backup.SentinelDto = &sentinelDto
	return sentinelDto, nil
}

// TODO : unit tests
func (backup *Backup) FetchSentinelData() ([]byte, error) {
	backupReader, err := backup.Folder.ReadObject(backup.GetStopSentinelPath())
	if err != nil {
		return make([]byte, 0), err
	}
	sentinelDtoData, err := ioutil.ReadAll(backupReader)
	if err != nil {
		return sentinelDtoData, errors.Wrap(err, "failed to fetch sentinel")
	}
	return sentinelDtoData, nil
}

func (backup *Backup) GetMeta() (ExtendedMetadataDto, error) {
	if backup.Metadata != nil {
		return *backup.Metadata, nil
	}
	metadata, err := backup.fetchMeta()
	if err != nil {
		return metadata, err
	}
	backup.Metadata = &metadata
	return metadata, nil
}

func (backup *Backup) fetchMeta() (ExtendedMetadataDto, error) {
	extendedMetadataDto := ExtendedMetadataDto{}
	backupReader, err := backup.Folder.ReadObject(backup.GetMetadataPath())
	if err != nil {
		return extendedMetadataDto, err
	}
	extendedMetadataDtoData, err := ioutil.ReadAll(backupReader)
	if err != nil {
		return extendedMetadataDto, errors.Wrap(err, "failed to fetch metadata")
	}

	err = json.Unmarshal(extendedMetadataDtoData, &extendedMetadataDto)
	return extendedMetadataDto, errors.Wrap(err, "failed to unmarshal metadata")
}

func checkDbDirectoryForUnwrap(dbDataDirectory string, sentinelDto BackupSentinelDto) error {
	if !sentinelDto.IsIncremental() {
		isEmpty, err := IsDirectoryEmpty(dbDataDirectory)
		if err != nil {
			return err
		}
		if !isEmpty {
			return NewNonEmptyDbDataDirectoryError(dbDataDirectory)
		}
	} else {
		tracelog.DebugLogger.Println("DB data directory before increment:")
		_ = filepath.Walk(dbDataDirectory,
			func(path string, info os.FileInfo, err error) error {
				if !info.IsDir() {
					tracelog.DebugLogger.Println(path)
				}
				return nil
			})

		for fileName, fileDescription := range sentinelDto.Files {
			if fileDescription.IsSkipped {
				tracelog.DebugLogger.Printf("Skipped file %v\n", fileName)
			}
		}
	}
	return nil
}

// TODO : unit tests
// Do the job of unpacking Backup object
func (backup *Backup) unwrap(dbDataDirectory string, sentinelDto BackupSentinelDto, filesToUnwrap map[string]bool) error {
	err := checkDbDirectoryForUnwrap(dbDataDirectory, sentinelDto)
	if err != nil {
		return err
	}

	tarInterpreter := NewFileTarInterpreter(dbDataDirectory, sentinelDto, filesToUnwrap)
	tarsToExtract, pgControlKey, err := backup.getTarsToExtract(sentinelDto, filesToUnwrap)
	if err != nil {
		return err
	}

	// Check name for backwards compatibility. Will check for `pg_control` if WALG version of backup.
	needPgControl := IsPgControlRequired(backup, sentinelDto)

	if pgControlKey == "" && needPgControl {
		return NewPgControlNotFoundError()
	}

	err = ExtractAll(tarInterpreter, tarsToExtract)
	if err != nil {
		return err
	}

	if needPgControl {
		err = ExtractAll(tarInterpreter, []ReaderMaker{NewStorageReaderMaker(backup.getTarPartitionFolder(), pgControlKey)})
		if err != nil {
			return errors.Wrap(err, "failed to extract pg_control")
		}
	}

	tracelog.InfoLogger.Print("\nBackup extraction complete.\n")
	return nil
}

func IsPgControlRequired(backup *Backup, sentinelDto BackupSentinelDto) bool {
	re := regexp.MustCompile(`^([^_]+._{1}[^_]+._{1})`)
	walgBasebackupName := re.FindString(backup.Name) == ""
	needPgControl := walgBasebackupName || sentinelDto.IsIncremental()
	return needPgControl
}

// TODO : unit tests
func IsDirectoryEmpty(directoryPath string) (bool, error) {
	var isEmpty = true
	searchLambda := func(path string, info os.FileInfo, err error) error {
		if path != directoryPath {
			isEmpty = false
			tracelog.InfoLogger.Printf("found file '%s' in directory: '%s'\n", path, directoryPath)
		}
		return nil
	}
	err := filepath.Walk(directoryPath, searchLambda)
	return isEmpty, errors.Wrapf(err, "can't check, that directory: '%s' is empty", directoryPath)
}

// TODO : init tests
func (backup *Backup) getTarsToExtract(sentinelDto BackupSentinelDto, filesToUnwrap map[string]bool) (tarsToExtract []ReaderMaker, pgControlKey string, err error) {
	tarNames, err := backup.GetTarNames()
	if err != nil {
		return nil, "", err
	}
	tracelog.DebugLogger.Printf("Tars to extract: '%+v'\n", tarNames)
	tarsToExtract = make([]ReaderMaker, 0, len(tarNames))

	pgControlRe := regexp.MustCompile(`^.*?pg_control\.tar(\..+$|$)`)
	for _, tarName := range tarNames {
		// Separate the pg_control tarName from the others to
		// extract it at the end, as to prevent server startup
		// with incomplete backup restoration.  But only if it
		// exists: it won't in the case of WAL-E backup
		// backwards compatibility.
		if pgControlRe.MatchString(tarName) {
			if pgControlKey != "" {
				panic("expect only one pg_control tar name match")
			}
			pgControlKey = tarName
			continue
		}

		if !shouldUnwrapTar(tarName, sentinelDto, filesToUnwrap) {
			continue
		}

		tarToExtract := NewStorageReaderMaker(backup.getTarPartitionFolder(), tarName)
		tarsToExtract = append(tarsToExtract, tarToExtract)
	}
	return
}

func (backup *Backup) GetFilesToUnwrap(fileMask string) (map[string]bool, error) {
	sentinelDto, err := backup.GetSentinel()
	if err != nil {
		return nil, err
	}
	if sentinelDto.Files == nil { // in case of WAL-E of old WAL-G backup
		return UnwrapAll, nil
	}
	filesToUnwrap := make(map[string]bool)
	for file := range sentinelDto.Files {
		filesToUnwrap[file] = true
	}
	for utilityFilePath := range UtilityFilePaths {
		filesToUnwrap[utilityFilePath] = true
	}
	return utility.SelectMatchingFiles(fileMask, filesToUnwrap)
}

func shouldUnwrapTar(tarName string, sentinelDto BackupSentinelDto, filesToUnwrap map[string]bool) bool {
	if len(sentinelDto.TarFileSets) == 0 {
		return true
	}

	tarFiles := sentinelDto.TarFileSets[tarName]

	for _, file := range tarFiles {
		if filesToUnwrap[file] {
			return true
		}
	}

	return false
}
