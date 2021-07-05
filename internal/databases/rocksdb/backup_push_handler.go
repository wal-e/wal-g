package rocksdb

import (
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

// Creates backup of Database with dbOptions to Uploader subfolder
func HandleBackupPush(uploader *internal.Uploader, dbOptions DatabaseOptions) error {
	tempDir, err := os.MkdirTemp("", internal.ROCKSDB)
	if err != nil {
		return err
	}
	tracelog.DebugLogger.Println("Temporaly backup to ", tempDir)
	tempDir = filepath.Join(tempDir, "backup") // hack: rocksdb requires nonexistent directory for checkpoint, so I dont delete temp directory
	defer os.RemoveAll(tempDir)

	if err = saveBackupToLocalDirectory(tempDir, dbOptions); err != nil {
		return err
	}

	backupName := utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat)
	uploadBackup(uploader, backupName, tempDir)

	return internal.UploadSentinel(uploader, generateBackupInfo(uploader, backupName), backupName)
}

func saveBackupToLocalDirectory(checkpointPath string, dbOptions DatabaseOptions) error {
	db, err := OpenDatabase(dbOptions)
	if err != nil {
		return errors.Wrapf(err, "Error when open database (%s)", dbOptions.DbPath)
	}
	defer db.CloseDb()

	checkpoint, err := db.CreateCheckpointObject()
	if err != nil {
		return errors.Wrapf(err, "Error when creating Checkpoint Object by path: %s ", checkpointPath)
	}
	defer checkpoint.DestroyCheckpointObject()

	return checkpoint.CreateCheckpoint(checkpointPath, 100)
}

func uploadBackup(uploader *internal.Uploader, backupName string, backupDirectory string) (TarFileSets, error) {
	bundle := NewBundle(backupDirectory, internal.ConfigureCrypter(), viper.GetInt64(internal.TarSizeThresholdSetting))

	// Start a new tar bundle, walk the backupDirectory and upload everything there.
	tracelog.InfoLogger.Println("Starting a new tar bundle")
	if err := bundle.StartQueue(internal.NewStorageTarBallMaker(backupName, uploader)); err != nil {
		return nil, err
	}

	tarBallComposerMaker := NewTarBallComposerMaker()

	if err := bundle.SetupComposer(tarBallComposerMaker); err != nil {
		return nil, err
	}

	tracelog.InfoLogger.Println("Walking ...")
	if err := filepath.Walk(backupDirectory, bundle.HandleWalkedFSObject); err != nil {
		return nil, err
	}

	tracelog.InfoLogger.Println("Packing ...")
	var tarFileSets TarFileSets
	var err error
	if tarFileSets, err = bundle.PackTarballs(); err != nil {
		return nil, err
	}

	tracelog.DebugLogger.Println("Finishing queue ...")
	if err = bundle.FinishQueue(); err != nil {
		return nil, err
	}

	uploader.Finish()

	return tarFileSets, nil
}

func generateBackupInfo(uploader *internal.Uploader, backupName string) BackupInfo {
	rawSize, _ := uploader.RawDataSize()
	backupSize, _ := uploader.UploadedDataSize()
	return BackupInfo{
		uint64(rawSize),
		uint64(backupSize),
		utility.TimeNowCrossPlatformUTC().Unix(),
		backupName,
	}
}
