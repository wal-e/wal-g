package internal

import (
	"github.com/wal-g/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

// HandleCatchupFetch is invoked to perform wal-g catchup-fetch
func HandleCatchupFetch(folder storage.Folder, dbDirectory, backupName string) {
	dbDirectory = utility.ResolveSymlink(dbDirectory)

	backup, err := GetBackupByName(backupName, utility.CatchupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Failed get backup by name: %v", err)

	filesToUnwrap, err := backup.GetFilesToUnwrap("")
	tracelog.ErrorLogger.FatalfOnError("Failed get files to unwrap from backup: %v", err)

	sentinelDto, err := backup.GetSentinel()
	tracelog.ErrorLogger.FatalfOnError("Failed get backup sentinel: %v", err)

	err = backup.unwrap(dbDirectory, sentinelDto, filesToUnwrap, true)
	tracelog.ErrorLogger.FatalfOnError("Failed unwrap backup: %v", err)
}
