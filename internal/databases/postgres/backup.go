package postgres

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/pkg/errors"
	"github.com/wal-g/storages/fs"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const (
	PgControlPath = "/global/pg_control"
)

var UnwrapAll map[string]bool = nil

var UtilityFilePaths = map[string]bool{
	PgControlPath:         true,
	BackupLabelFilename:   true,
	TablespaceMapFilename: true,
}

var patternPgBackupName = fmt.Sprintf("base_%[1]s(_D_%[1]s)?", PatternTimelineAndLogSegNo)
var regexpPgBackupName = regexp.MustCompile(patternPgBackupName)

// Backup contains information about a valid Postgres backup
// generated and uploaded by WAL-G.
type Backup struct {
	internal.Backup
	SentinelDto *BackupSentinelDto // used for storage query caching
}

func ToPgBackup(source internal.Backup) (output Backup) {
	return Backup{
		Backup: source,
	}
}

func NewBackup(baseBackupFolder storage.Folder, name string) Backup {
	return Backup{
		Backup: internal.NewBackup(baseBackupFolder, name),
	}
}

func (backup *Backup) getTarPartitionFolder() storage.Folder {
	return backup.Folder.GetSubFolder(backup.Name + internal.TarPartitionFolderName)
}

func GetBackupByName(backupName, subfolder string, folder storage.Folder) (Backup, error) {
	defaultBackup, err := internal.GetBackupByName(backupName, subfolder, folder)
	if err != nil {
		return Backup{}, err
	}
	backup := Backup{defaultBackup, nil}

	_, err = backup.GetSentinel()
	if err != nil {
		return Backup{}, err
	}
	return backup, nil
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
	err := backup.FetchSentinel(&sentinelDto)
	if err != nil {
		return sentinelDto, err
	}

	backup.SentinelDto = &sentinelDto
	return sentinelDto, nil
}

func (backup *Backup) FetchMeta() (ExtendedMetadataDto, error) {
	extendedMetadataDto := ExtendedMetadataDto{}
	err := backup.FetchMetadata(&extendedMetadataDto)
	if err != nil {
		return ExtendedMetadataDto{}, errors.Wrap(err, "failed to unmarshal metadata")
	}

	return extendedMetadataDto, nil
}

func checkDBDirectoryForUnwrap(dbDataDirectory string, sentinelDto BackupSentinelDto) error {
	if !sentinelDto.IsIncremental() {
		isEmpty, err := isDirectoryEmpty(dbDataDirectory)
		if err != nil {
			return err
		}
		if !isEmpty {
			return NewNonEmptyDBDataDirectoryError(dbDataDirectory)
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

	if sentinelDto.TablespaceSpec != nil && !sentinelDto.TablespaceSpec.empty() {
		err := setTablespacePaths(*sentinelDto.TablespaceSpec)
		if err != nil {
			return err
		}
	}

	return nil
}

func setTablespacePaths(spec TablespaceSpec) error {
	basePrefix, ok := spec.BasePrefix()
	if !ok {
		return fmt.Errorf("tablespace specification base path is not set")
	}
	err := fs.NewFolder(basePrefix, TablespaceFolder).EnsureExists()
	if err != nil {
		return fmt.Errorf("error creating pg_tblspc folder %v", err)
	}
	for _, location := range spec.tablespaceLocations() {
		err := fs.NewFolder(location.Location, "").EnsureExists()
		if err != nil {
			return fmt.Errorf("error creating folder for tablespace %v", err)
		}
		err = os.Symlink(location.Location, filepath.Join(basePrefix, location.Symlink))
		if err != nil {
			return fmt.Errorf("error creating tablespace symkink %v", err)
		}
	}
	return nil
}

// check that directory is empty before unwrap
func (backup *Backup) unwrapToEmptyDirectory(
	dbDataDirectory string, sentinelDto BackupSentinelDto, filesToUnwrap map[string]bool, createIncrementalFiles bool,
) error {
	err := checkDBDirectoryForUnwrap(dbDataDirectory, sentinelDto)
	if err != nil {
		return err
	}

	return backup.unwrapOld(dbDataDirectory, sentinelDto, filesToUnwrap, createIncrementalFiles)
}

// TODO : unit tests
// Do the job of unpacking Backup object
func (backup *Backup) unwrapOld(
	dbDataDirectory string, sentinelDto BackupSentinelDto, filesToUnwrap map[string]bool, createIncrementalFiles bool,
) error {
	tarInterpreter := NewFileTarInterpreter(dbDataDirectory, sentinelDto, filesToUnwrap, createIncrementalFiles)
	tarsToExtract, pgControlKey, err := backup.getTarsToExtract(sentinelDto, filesToUnwrap, false)
	if err != nil {
		return err
	}

	// Check name for backwards compatibility. Will check for `pg_control` if WALG version of backup.
	needPgControl := IsPgControlRequired(*backup, sentinelDto)

	if pgControlKey == "" && needPgControl {
		return newPgControlNotFoundError()
	}

	err = internal.ExtractAll(tarInterpreter, tarsToExtract)
	if err != nil {
		return err
	}

	if needPgControl {
		err = internal.ExtractAll(tarInterpreter, []internal.ReaderMaker{
			internal.NewStorageReaderMaker(backup.getTarPartitionFolder(), pgControlKey)})
		if err != nil {
			return errors.Wrap(err, "failed to extract pg_control")
		}
	}

	tracelog.InfoLogger.Print("\nBackup extraction complete.\n")
	return nil
}

func IsPgControlRequired(backup Backup, sentinelDto BackupSentinelDto) bool {
	re := regexp.MustCompile(`^([^_]+._{1}[^_]+._{1})`)
	walgBasebackupName := re.FindString(backup.Name) == ""
	needPgControl := walgBasebackupName || sentinelDto.IsIncremental()
	return needPgControl
}

func isDirectoryEmpty(directoryPath string) (bool, error) {
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
func (backup *Backup) getTarsToExtract(sentinelDto BackupSentinelDto, filesToUnwrap map[string]bool,
	skipRedundantTars bool) (tarsToExtract []internal.ReaderMaker, pgControlKey string, err error) {
	tarNames, err := backup.GetTarNames()
	if err != nil {
		return nil, "", err
	}
	tracelog.DebugLogger.Printf("Tars to extract: '%+v'\n", tarNames)
	tarsToExtract = make([]internal.ReaderMaker, 0, len(tarNames))

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

		if skipRedundantTars && !shouldUnwrapTar(tarName, sentinelDto, filesToUnwrap) {
			continue
		}

		tarToExtract := internal.NewStorageReaderMaker(backup.getTarPartitionFolder(), tarName)
		tarsToExtract = append(tarsToExtract, tarToExtract)
	}
	return tarsToExtract, pgControlKey, nil
}

func (backup *Backup) GetFilesToUnwrap(fileMask string) (map[string]bool, error) {
	sentinelDto, err := backup.GetSentinel()
	if err != nil {
		return nil, err
	}
	// in case of WAL-E of old WAL-G backup -or-
	// base backup created with WALG_USE_SIMPLE_COMPOSER
	if sentinelDto.Files == nil || len(sentinelDto.Files) == 0 {
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
	// in case of base backup created with WALG_USE_SIMPLE_COMPOSER
	if len(sentinelDto.TarFileSets) == 0 {
		return true
	}

	tarFiles := sentinelDto.TarFileSets[tarName]

	for _, file := range tarFiles {
		if filesToUnwrap[file] {
			return true
		}
	}

	tracelog.DebugLogger.Printf("Skipping archive '%s'\n", tarName)
	return false
}

func GetLastWalFilename(backup Backup) (string, error) {
	meta, err := backup.FetchMeta()
	if err != nil {
		tracelog.InfoLogger.Print("No meta found.")
		return "", err
	}
	timelineID, err := ParseTimelineFromBackupName(backup.Name)
	if err != nil {
		tracelog.InfoLogger.FatalError(err)
		return "", err
	}
	endWalSegmentNo := newWalSegmentNo(meta.FinishLsn - 1)
	return endWalSegmentNo.getFilename(timelineID), nil
}

func FetchPgBackupName(object storage.Object) string {
	return regexpPgBackupName.FindString(object.GetName())
}
