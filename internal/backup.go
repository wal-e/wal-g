package internal

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/storages/fs"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
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

var patternPgBackupName = fmt.Sprintf("base_%[1]s(_D_%[1]s)?", PatternTimelineAndLogSegNo)
var regexpPgBackupName = regexp.MustCompile(patternPgBackupName)

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
	BaseBackupFolder storage.Folder
	Name             string
	SentinelDto      *BackupSentinelDto // used for storage query caching
}

func NewBackup(baseBackupFolder storage.Folder, name string) *Backup {
	return &Backup{baseBackupFolder, name, nil}
}

// GetStopSentinelPath returns sentinel path.
func (backup *Backup) GetStopSentinelPath() string {
	return SentinelNameFromBackup(backup.Name)
}

func (backup *Backup) GetMetadataPath() string {
	return backup.Name + "/" + utility.MetadataFileName
}

func (backup *Backup) getTarPartitionFolder() storage.Folder {
	return backup.BaseBackupFolder.GetSubFolder(backup.Name + TarPartitionFolderName)
}

// CheckExistence checks that the specified backup exists.
func (backup *Backup) CheckExistence() (bool, error) {
	return backup.BaseBackupFolder.Exists(backup.GetStopSentinelPath())
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
	sentinelDtoData, err := backup.fetchSentinelData()
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
func (backup *Backup) fetchSentinelData() ([]byte, error) {
	backupReaderMaker := newStorageReaderMaker(backup.BaseBackupFolder, backup.GetStopSentinelPath())
	backupReader, err := backupReaderMaker.Reader()
	if err != nil {
		return make([]byte, 0), err
	}
	sentinelDtoData, err := ioutil.ReadAll(backupReader)
	if err != nil {
		return sentinelDtoData, errors.Wrap(err, "failed to fetch sentinel")
	}
	return sentinelDtoData, nil
}

func (backup *Backup) FetchMeta() (ExtendedMetadataDto, error) {
	extendedMetadataDto := ExtendedMetadataDto{}
	backupReaderMaker := newStorageReaderMaker(backup.BaseBackupFolder, backup.GetMetadataPath())

	backupReader, err := backupReaderMaker.Reader()
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
		isEmpty, err := isDirectoryEmpty(dbDataDirectory)
		if err != nil {
			return err
		}
		if !isEmpty {
			return newNonEmptyDbDataDirectoryError(dbDataDirectory)
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
		return fmt.Errorf("Tablespace specification base path is not set\n")
	}
	err := fs.NewFolder(basePrefix, TablespaceFolder).EnsureExists()
	if err != nil {
		return fmt.Errorf("Error creating pg_tblspc folder %v\n", err)
	}
	for _, location := range spec.tablespaceLocations() {
		symlinkName := filepath.Join(basePrefix, location.Symlink)
		err := fs.NewFolder(location.Location, "").EnsureExists()
		if err != nil {
			return fmt.Errorf("Error creating folder for tablespace %v\n", err)
		}
		if FileInfo, err := os.Lstat(symlinkName); err == nil {
			if FileInfo.Mode()&os.ModeSymlink != os.ModeSymlink {
				return fmt.Errorf("Error, %v exists, but is not a symlink\n", symlinkName)
			}
			symlinkTarget, err := os.Readlink(symlinkName)
			if err != nil {
				return fmt.Errorf("Error reading symlink %v\n", err)
			}
			if symlinkTarget != location.Location {
				return fmt.Errorf("Error: Symlink %v points to target %v, instead of %v", symlinkName, symlinkTarget, location.Location)
			}
		} else {
			err = os.Symlink(location.Location, symlinkName)
			if err != nil {
				return fmt.Errorf("Error creating tablespace symlink %v\n", err)
			}
		}
	}
	return nil
}

// check that directory is empty before unwrap
func (backup *Backup) unwrapToEmptyDirectory(
	dbDataDirectory string, sentinelDto BackupSentinelDto, filesToUnwrap map[string]bool, createIncrementalFiles bool,
) error {
	err := checkDbDirectoryForUnwrap(dbDataDirectory, sentinelDto)
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
	needPgControl := IsPgControlRequired(backup, sentinelDto)

	if pgControlKey == "" && needPgControl {
		return newPgControlNotFoundError()
	}

	err = ExtractAll(tarInterpreter, tarsToExtract)
	if err != nil {
		return err
	}

	if needPgControl {
		err = ExtractAll(tarInterpreter, []ReaderMaker{newStorageReaderMaker(backup.getTarPartitionFolder(), pgControlKey)})
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
func (backup *Backup) getTarsToExtract(sentinelDto BackupSentinelDto, filesToUnwrap map[string]bool, skipRedundantTars bool) (tarsToExtract []ReaderMaker, pgControlKey string, err error) {
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

		if skipRedundantTars && !shouldUnwrapTar(tarName, sentinelDto, filesToUnwrap) {
			continue
		}

		tarToExtract := newStorageReaderMaker(backup.getTarPartitionFolder(), tarName)
		tarsToExtract = append(tarsToExtract, tarToExtract)
	}
	return tarsToExtract, pgControlKey, nil
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

	tracelog.DebugLogger.Printf("Skipping archive '%s'\n", tarName)
	return false
}

func GetLastWalFilename(backup *Backup) (string, error) {
	meta, err := backup.FetchMeta()
	if err != nil {
		tracelog.InfoLogger.Print("No meta found.")
		return "", err
	}
	timelineId, err := ParseTimelineFromBackupName(backup.Name)
	if err != nil {
		tracelog.InfoLogger.FatalError(err)
		return "", err
	}
	endWalSegmentNo := newWalSegmentNo(meta.FinishLsn - 1)
	return endWalSegmentNo.getFilename(timelineId), nil
}

// TODO : unit tests
func getLatestBackupName(folder storage.Folder) (string, error) {
	sortTimes, err := GetBackups(folder)
	if err != nil {
		return "", err
	}

	return sortTimes[0].BackupName, nil
}

func GetBackupSentinelObjects(folder storage.Folder) ([]storage.Object, error) {
	objects, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return nil, err
	}
	sentinelObjects := make([]storage.Object, 0, len(objects))
	for _, object := range objects {
		if !strings.HasSuffix(object.GetName(), utility.SentinelSuffix) {
			continue
		}
		sentinelObjects = append(sentinelObjects, object)
	}

	return sentinelObjects, nil
}

// TODO : unit tests
// GetBackups receives backup descriptions and sorts them by time
func GetBackups(folder storage.Folder) (backups []BackupTime, err error) {
	return GetBackupsWithTarget(folder, utility.BaseBackupPath)
}

func GetBackupsWithTarget(folder storage.Folder, targetPath string) (backups []BackupTime, err error) {
	backups, _, err = GetBackupsAndGarbageWithTarget(folder, targetPath)
	if err != nil {
		return nil, err
	}

	count := len(backups)
	if count == 0 {
		return nil, NewNoBackupsFoundError()
	}
	return
}

func GetBackupsAndGarbage(folder storage.Folder) (backups []BackupTime, garbage []string, err error) {
	return GetBackupsAndGarbageWithTarget(folder, utility.BaseBackupPath)
}

// TODO : unit tests
func GetBackupsAndGarbageWithTarget(folder storage.Folder, targetPath string) (backups []BackupTime, garbage []string, err error) {
	backupObjects, subFolders, err := folder.GetSubFolder(targetPath).ListFolder()
	if err != nil {
		return nil, nil, err
	}

	sortTimes := GetBackupTimeSlices(backupObjects)
	garbage = getGarbageFromPrefix(subFolders, sortTimes)

	return sortTimes, garbage, nil
}

// TODO : unit tests
func GetBackupTimeSlices(backups []storage.Object) []BackupTime {
	sortTimes := make([]BackupTime, len(backups))
	for i, object := range backups {
		key := object.GetName()
		if !strings.HasSuffix(key, utility.SentinelSuffix) {
			continue
		}
		time := object.GetLastModified()
		sortTimes[i] = BackupTime{utility.StripRightmostBackupName(key), time,
			utility.StripWalFileName(key)}
	}
	sort.Slice(sortTimes, func(i, j int) bool {
		return sortTimes[i].Time.After(sortTimes[j].Time)
	})
	return sortTimes
}

// TODO : unit tests
func getGarbageFromPrefix(folders []storage.Folder, nonGarbage []BackupTime) []string {
	garbage := make([]string, 0)
	var keyFilter = make(map[string]string)
	for _, k := range nonGarbage {
		keyFilter[k.BackupName] = k.BackupName
	}
	for _, folder := range folders {
		backupName := utility.StripPrefixName(folder.GetPath())
		if _, ok := keyFilter[backupName]; ok {
			continue
		}
		garbage = append(garbage, backupName)
	}
	return garbage
}

func FetchPgBackupName(object storage.Object) string {
	return regexpPgBackupName.FindString(object.GetName())
}
