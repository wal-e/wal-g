package mysql

import (
	"bytes"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const (
	XtrabackupApplyLogOnly   = "--apply-log-only"
	XtrabackupIncrementalDir = "--incremental-dir"
)

func HandleBackupFetch(folder storage.Folder, backupName string, restoreCmd *exec.Cmd, prepareCmd *exec.Cmd) {
	var err error
	if isXtrabackupCmd(restoreCmd) {
		tempDeltaDir := path.Join("/tmp", "delta")
		if _, err = os.Stat(tempDeltaDir); os.IsNotExist(err) {
			err = os.MkdirAll(tempDeltaDir, 0755)
		}
		tracelog.ErrorLogger.FatalOnError(err)

		internal.HandleBackupFetch(folder, backupName, getMysqlFetcher(restoreCmd, prepareCmd, tempDeltaDir))
		err := os.Remove(tempDeltaDir)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to delete temp dir: %v", err)
		}
		return
	}

	internal.HandleBackupFetch(folder, backupName, internal.GetCommandStreamFetcher(restoreCmd))
	if prepareCmd != nil {
		err = prepareCmd.Run()
		tracelog.ErrorLogger.FatalfOnError("failed to prepare fetched backup: %v", err)
	}
}

func getMysqlFetcher(restoreCmd, prepareCmd *exec.Cmd, tempDeltaDir string) func(folder storage.Folder, backup internal.Backup) {
	return func(folder storage.Folder, backup internal.Backup) {
		internal.AppendCommandArgument(prepareCmd, XtrabackupApplyLogOnly)

		err := deltaFetchMysqlRecursion(backup.Name, tempDeltaDir, folder, restoreCmd, prepareCmd, true)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	}
}

func deltaFetchMysqlRecursion(
	backupName, tempDeltaDir string,
	folder storage.Folder,
	restoreCmd, prepareCmd *exec.Cmd, isLast bool) error {
	var isFull bool
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	if err != nil {
		return err
	}
	sentinelDto, err := backup.GetSentinel()
	if err != nil {
		return err
	}

	if sentinelDto.IsIncremental() {
		tracelog.InfoLogger.Printf("Delta from %v at LSN %x \n", *sentinelDto.IncrementFrom, *sentinelDto.IncrementFromLSN)
		err = deltaFetchMysqlRecursion(*sentinelDto.IncrementFrom, tempDeltaDir, folder, restoreCmd, prepareCmd, false)
		if err != nil {
			return err
		}
	} else {
		isFull = true
	}

	if !isFull {
		restoreCmd = internal.ForkCommand(restoreCmd)
		restoreArgs := strings.Split(restoreCmd.Args[len(restoreCmd.Args)-1], " ")
		internal.ReplaceCommandArgument(restoreCmd, restoreArgs[len(restoreArgs)-1], tempDeltaDir)

		prepareCmd = internal.ForkCommand(prepareCmd)
		internal.AppendCommandArgument(prepareCmd, XtrabackupIncrementalDir+"="+tempDeltaDir)
	}
	if isLast {
		internal.ReplaceCommandArgument(prepareCmd, XtrabackupApplyLogOnly, "")
	}

	stdin, err := restoreCmd.StdinPipe()
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	stderr := &bytes.Buffer{}
	restoreCmd.Stderr = stderr

	tracelog.InfoLogger.Printf("Restoring %s with cmd %v", backupName, restoreCmd.Args)
	err = restoreCmd.Start()
	if err != nil {
		return err
	}
	err = internal.DownloadAndDecompressStream(backup, stdin)
	if err != nil {
		return err
	}
	cmdErr := restoreCmd.Wait()
	if cmdErr != nil {
		tracelog.ErrorLogger.Printf("Restore command output:\n%s", stderr.String())
		err = cmdErr
	}
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("Restored %s", backupName)

	tracelog.InfoLogger.Printf("Preparing %s with cmd %v", backupName, prepareCmd.Args)
	if prepareCmd != nil {
		err = prepareCmd.Run()
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to prepare fetched backup: %v", err)
			return err
		}
		tracelog.InfoLogger.Printf("Prepared %s", backupName)
	}

	return utility.RemoveContents(tempDeltaDir)
}
