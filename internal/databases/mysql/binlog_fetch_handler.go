package mysql

import (
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/wal-g/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func HandleBinlogFetch(folder storage.Folder, backupName string, untilDT string) {
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Unable to get backup %v", err)

	startTs, err := getBinlogStartTs(folder, backup)
	tracelog.ErrorLogger.FatalOnError(err)

	endTs, err := configureEndTs(untilDT)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Printf("Fetching binlogs since %s until %s", startTs, endTs)
	fetchedLogs, err := fetchLogs(folder, startTs, endTs, fetchHandler)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch logs: %v", err)

	err = createIndexFile(fetchedLogs)
	tracelog.ErrorLogger.FatalfOnError("Failed to create binlog index file: %v", err)
}

func fetchHandler(logFolder storage.Folder, logName string, endTs time.Time) (needAbortFetch bool, err error) {
	dstPathFolder, err := internal.GetLogsDstSettings(internal.MysqlBinlogDstSetting)
	if err != nil {
		tracelog.ErrorLogger.Println(err)
		return true, err
	}
	pathToLog := path.Join(dstPathFolder, logName)
	tracelog.InfoLogger.Printf("downloading %s into %s", logName, pathToLog)
	if err = internal.DownloadWALFileTo(logFolder, logName, pathToLog); err != nil {
		tracelog.ErrorLogger.Print(err)
		return true, err
	}
	timestamp, err := GetBinlogStartTimestamp(pathToLog)
	if err != nil {
		return true, err
	}
	return timestamp.After(endTs), nil
}

func createIndexFile(fetchedBinlogs []storage.Object) error {
	logsFolder, err := internal.GetLogsDstSettings(internal.MysqlBinlogDstSetting)
	if err != nil {
		return err
	}
	indexFile, err := os.Create(filepath.Join(logsFolder, "binlogs_order"))
	if err != nil {
		return err
	}
	for _, binlogObject := range fetchedBinlogs {
		_, err = indexFile.WriteString(utility.TrimFileExtension(binlogObject.GetName()) + "\n")
		if err != nil {
			return err
		}
	}
	return indexFile.Close()
}
