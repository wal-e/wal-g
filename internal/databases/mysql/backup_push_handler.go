package mysql

import (
	"bufio"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const (
	XtrabackupExtraLsnDir    = "--extra-lsndir"
	XtrabackupIncrementalLSN = "--incremental-lsn"
	XtrabackupCheckpoints    = "xtrabackup_checkpoints"
)

func HandleBackupPush(uploader *internal.Uploader, backupCmd *exec.Cmd, isFullBackup bool) {
	maxDeltas := viper.GetInt(internal.DeltaMaxStepsSetting)
	var err error
	var previousBackupName string
	previousBackupSentinelDto := internal.BackupSentinelDto{}
	incrementCount := 1

	folder := uploader.UploadingFolder
	uploader.UploadingFolder = folder.GetSubFolder(utility.BaseBackupPath)
	if maxDeltas > 0 && !isFullBackup && isXtrabackupCmd(backupCmd) {
		previousBackupName, err = internal.GetLatestBackupName(folder)
		if err != nil {
			if _, ok := err.(internal.NoBackupsFoundError); ok {
				tracelog.InfoLogger.Println("Couldn't find previous backup. Doing full backup.")
			} else {
				tracelog.ErrorLogger.FatalError(err)
			}
		} else {
			previousBackup := internal.NewBackup(uploader.UploadingFolder, previousBackupName)
			previousBackupSentinelDto, err = previousBackup.GetSentinel()
			tracelog.ErrorLogger.FatalOnError(err)

			if previousBackupSentinelDto.IncrementCount != nil {
				incrementCount = *previousBackupSentinelDto.IncrementCount + 1
			}

			if incrementCount > maxDeltas {
				tracelog.InfoLogger.Println("Reached max delta steps. Doing full backup.")
				previousBackupSentinelDto = internal.BackupSentinelDto{}
				incrementCount = 1
			} else if previousBackupSentinelDto.BackupFinishLSN == nil {
				tracelog.InfoLogger.Println("LATEST backup was made without support for delta feature. Fallback to full backup with LSN marker for future deltas.")
				previousBackupSentinelDto = internal.BackupSentinelDto{}
				incrementCount = 1
			} else {
				tracelog.InfoLogger.Printf("Delta backup from LSN %v. \n", *previousBackupSentinelDto.BackupFinishLSN)
			}
		}
	} else {
		tracelog.InfoLogger.Println("Doing full backup.")
	}

	createAndPushBackup(uploader, backupCmd, previousBackupSentinelDto, incrementCount)
}

func createAndPushBackup(
	uploader *internal.Uploader, backupCmd *exec.Cmd,
	previousBackupSentinelDto internal.BackupSentinelDto,
	incrementCount int,
) {
	extraLsnDir := path.Join("/tmp", "extra_lsn")
	isXtrabackup := isXtrabackupCmd(backupCmd)
	fromLSN, previousBackupName := previousBackupSentinelDto.BackupFinishLSN, previousBackupSentinelDto.IncrementFullName

	if isXtrabackup {
		if _, err := os.Stat(extraLsnDir); os.IsNotExist(err) {
			err = os.MkdirAll(extraLsnDir, 0755)
		}

		internal.AppendCommandArgument(backupCmd, XtrabackupExtraLsnDir+"="+extraLsnDir)
		if fromLSN != nil {
			internal.AppendCommandArgument(backupCmd, XtrabackupIncrementalLSN+"="+strconv.FormatUint(*fromLSN, 10))
		}
	}

	db, err := getMySQLConnection()
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(db, "")

	binlogStart := getMySQLCurrentBinlogFile(db)
	tracelog.DebugLogger.Println("Binlog start file", binlogStart)
	timeStart := utility.TimeNowCrossPlatformLocal()

	stdout, stderr, err := utility.StartCommandWithStdoutStderr(backupCmd)
	tracelog.ErrorLogger.FatalfOnError("Failed to start backup create command: %v", err)

	fileName, err := uploader.PushStream(stdout)
	tracelog.ErrorLogger.FatalfOnError("Failed to push backup: %v", err)

	err = backupCmd.Wait()
	if err != nil {
		tracelog.ErrorLogger.Printf("Backup command output:\n%s", stderr.String())
		tracelog.ErrorLogger.Fatalf("Backup create command failed: %v", err)
	}

	var toLSN *uint64
	if isXtrabackup {
		toLSN, err = readToLSN(extraLsnDir)

		if err != nil {
			tracelog.WarningLogger.Printf("Failed to read to_lsn. Future deltas cannot be done on top of the current backup: %v", err)
		}
	}

	binlogEnd := getMySQLCurrentBinlogFile(db)
	tracelog.DebugLogger.Println("Binlog end file", binlogEnd)

	sentinel := BackupSentinelDto{
		BackupSentinelDto: internal.BackupSentinelDto{
			IncrementFromLSN: fromLSN, BackupFinishLSN: toLSN,
			IncrementFrom: previousBackupName, IncrementFullName: &fileName, IncrementCount: &incrementCount,
		},
		StreamSentinelDto: StreamSentinelDto{
			BinLogStart: binlogStart, BinLogEnd: binlogEnd,
			StartLocalTime: timeStart,
		},
	}
	err = internal.UploadSentinel(uploader, &sentinel, fileName)
	tracelog.ErrorLogger.FatalOnError(err)

	if isXtrabackup {
		err = removePushTempFiles(extraLsnDir)
		if err != nil {
			tracelog.WarningLogger.Printf("Couldn't delete data in temp dir: %v", err)
		}
	}
}

func readToLSN(dir string) (*uint64, error) {
	file, err := os.Open(path.Join(dir, XtrabackupCheckpoints))
	if err != nil {
		return nil, err
	}

	values := make(map[string]string)
	fileScanner := bufio.NewScanner(file)
	for fileScanner.Scan() {
		pair := strings.Split(fileScanner.Text(), " = ")
		values[pair[0]] = pair[1]
	}

	err = file.Close()
	if err != nil {
		return nil, err
	}

	toLSN, err := strconv.ParseUint(values["to_lsn"], 10, 64)
	if err != nil {
		return nil, err
	}

	return &toLSN, fileScanner.Err()
}

func removePushTempFiles(dir string) error {
	err := utility.RemoveContents(dir)
	if err != nil {
		return err
	}
	return os.Remove(dir)
}
