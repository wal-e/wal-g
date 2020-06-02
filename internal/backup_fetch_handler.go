package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"

	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

const LatestString = "LATEST"

type BackupNonExistenceError struct {
	error
}

func NewBackupNonExistenceError(backupName string) BackupNonExistenceError {
	return BackupNonExistenceError{errors.Errorf("Backup '%s' does not exist.", backupName)}
}

func (err BackupNonExistenceError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type NonEmptyDbDataDirectoryError struct {
	error
}

func newNonEmptyDbDataDirectoryError(dbDataDirectory string) NonEmptyDbDataDirectoryError {
	return NonEmptyDbDataDirectoryError{errors.Errorf("Directory %v for delta base must be empty", dbDataDirectory)}
}

func (err NonEmptyDbDataDirectoryError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type PgControlNotFoundError struct {
	error
}

func newPgControlNotFoundError() PgControlNotFoundError {
	return PgControlNotFoundError{errors.Errorf("Expect pg_control archive, but not found")}
}

func (err PgControlNotFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

func readRestoreSpec(path string, spec *TablespaceSpec) (err error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("Unable to read file: %v\n", err)
	}
	err = json.Unmarshal(data, spec)
	if err != nil {
		return fmt.Errorf("Unable to unmarshal json: %v\n Full json data:\n %s", err, data)
	}

	return nil
}

func GetPgFetcher(dbDataDirectory, fileMask, restoreSpecPath string) func(folder storage.Folder, backup Backup) {
	return func(folder storage.Folder, backup Backup) {
		filesToUnwrap, err := backup.GetFilesToUnwrap(fileMask)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)

		var spec *TablespaceSpec
		if restoreSpecPath != "" {
			spec = &TablespaceSpec{}
			err := readRestoreSpec(restoreSpecPath, spec)
			errMessege := fmt.Sprintf("Invalid restore specification path %s\n", restoreSpecPath)
			tracelog.ErrorLogger.FatalfOnError(errMessege, err)
		}
		err = deltaFetchRecursion(backup.Name, folder, utility.ResolveSymlink(dbDataDirectory), spec, filesToUnwrap)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	}
}

func GetStreamFetcher(writeCloser io.WriteCloser) func(folder storage.Folder, backup Backup) {
	return func(folder storage.Folder, backup Backup) {
		err := DownloadAndDecompressStream(&backup, writeCloser)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	}
}

func GetCommandStreamFetcher(cmd *exec.Cmd) func(folder storage.Folder, backup Backup) {
	return func(folder storage.Folder, backup Backup) {
		stdin, err := cmd.StdinPipe()
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
		stderr := &bytes.Buffer{}
		cmd.Stderr = stderr
		err = cmd.Start()
		tracelog.ErrorLogger.FatalfOnError("Failed to start restore command: %v\n", err)
		err = DownloadAndDecompressStream(&backup, stdin)
		cmdErr := cmd.Wait()
		if cmdErr != nil {
			tracelog.ErrorLogger.Printf("Restore command output:\n%s", stderr.String())
			err = cmdErr
		}
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	}
}

// TODO : unit tests
// HandleBackupFetch is invoked to perform wal-g backup-fetch
func HandleBackupFetch(folder storage.Folder, backupName string, fetcher func(folder storage.Folder, backup Backup)) {
	tracelog.DebugLogger.Printf("HandleBackupFetch(%s, folder,)\n", backupName)
	backup, err := GetBackupByName(backupName, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)

	fetcher(folder, *backup)
}

func GetBackupByName(backupName, subfolder string, folder storage.Folder) (*Backup, error) {
	baseBackupFolder := folder.GetSubFolder(subfolder)

	var backup *Backup
	if backupName == LatestString {
		latest, err := GetLatestBackupName(folder)
		if err != nil {
			return nil, err
		}
		tracelog.InfoLogger.Printf("LATEST backup is: '%s'\n", latest)

		backup = NewBackup(baseBackupFolder, latest)

	} else {
		backup = NewBackup(baseBackupFolder, backupName)

		exists, err := backup.CheckExistence()
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, NewBackupNonExistenceError(backupName)
		}
	}
	return backup, nil
}

// If specified - choose specified, else choose from latest sentinelDto
func chooseTablespaceSpecification(sentinelDto BackupSentinelDto, spec *TablespaceSpec) {
	if spec != nil {
		sentinelDto.TablespaceSpec = spec
	} else {
		if sentinelDto.TablespaceSpec == nil {
			sentinelDto.TablespaceSpec = &TablespaceSpec{}
		}
		spec = sentinelDto.TablespaceSpec
	}
}

// TODO : unit tests
// deltaFetchRecursion function composes Backup object and recursively searches for necessary base backup
func deltaFetchRecursion(backupName string, folder storage.Folder, dbDataDirectory string,
	tablespaceSpec *TablespaceSpec, filesToUnwrap map[string]bool) error {
	backup, err := GetBackupByName(backupName, utility.BaseBackupPath, folder)
	if err != nil {
		return err
	}
	sentinelDto, err := backup.GetSentinel()
	if err != nil {
		return err
	}
	chooseTablespaceSpecification(sentinelDto, tablespaceSpec)

	if sentinelDto.IsIncremental() {
		tracelog.InfoLogger.Printf("Delta from %v at LSN %x \n", *(sentinelDto.IncrementFrom), *(sentinelDto.IncrementFromLSN))
		baseFilesToUnwrap, err := GetBaseFilesToUnwrap(sentinelDto.Files, filesToUnwrap)
		if err != nil {
			return err
		}
		err = deltaFetchRecursion(*sentinelDto.IncrementFrom, folder, dbDataDirectory, tablespaceSpec, baseFilesToUnwrap)
		if err != nil {
			return err
		}
		tracelog.InfoLogger.Printf("%v fetched. Upgrading from LSN %x to LSN %x \n", *(sentinelDto.IncrementFrom), *(sentinelDto.IncrementFromLSN), *(sentinelDto.BackupStartLSN))
	}

	return backup.unwrapToEmptyDirectory(dbDataDirectory, sentinelDto, filesToUnwrap, false)
}

func GetBaseFilesToUnwrap(backupFileStates BackupFileList, currentFilesToUnwrap map[string]bool) (map[string]bool, error) {
	baseFilesToUnwrap := make(map[string]bool)
	for file := range currentFilesToUnwrap {
		fileDescription, hasDescription := backupFileStates[file]
		if !hasDescription {
			if _, ok := UtilityFilePaths[file]; !ok {
				tracelog.ErrorLogger.Panicf("Wanted to fetch increment for file: '%s', but didn't find one in base", file)
			}
			continue
		}
		if fileDescription.IsSkipped || fileDescription.IsIncremented {
			baseFilesToUnwrap[file] = true
		}
	}
	return baseFilesToUnwrap, nil
}
