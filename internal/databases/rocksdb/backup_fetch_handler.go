package rocksdb

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func HandleBackupFetch(folder storage.Folder, dbOptions DatabaseOptions, restoreOptions RestoreOptions) error {
	backup, err := internal.GetBackupByName(restoreOptions.BackupName, utility.BaseBackupPath, folder)
	if err != nil {
		return err
	}
	metaData := BackupInfo{}
	if err := backup.FetchSentinel(&metaData); err != nil {
		return err
	}
	tracelog.DebugLogger.Println("Directory to restore backup is ", dbOptions.DbPath)

	if _, err = os.Stat(dbOptions.DbPath); !os.IsNotExist(err) {
		return errors.New("this file or direcrtory exists. Can restore only to nonExistentDirectory")
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// Create parent path if it doesnot exist
	parentPath := filepath.Clean(filepath.Join(dbOptions.DbPath, ".."))
	tracelog.DebugLogger.Printf("Create parent dictionary %v\n", parentPath)
	if err = os.MkdirAll(parentPath, 0755); err != nil {
		return err
	}

	folder = folder.GetSubFolder(utility.BaseBackupPath)
	reader, err := folder.ReadObject(backup.Name)
	if err != nil {
		return err
	}
	if err = unpackStreamToDirectory(dbOptions.DbPath, reader); err != nil {
		return err
	}
	tracelog.DebugLogger.Printf("Restored backup to %v\n", dbOptions.DbPath)
	return nil
}
