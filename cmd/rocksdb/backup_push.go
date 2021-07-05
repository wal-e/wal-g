package rocksdb

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/tmp/rocksdb"
	"github.com/wal-g/wal-g/utility"
)

const (
	backupPushShortDescription = "Makes backup of database and uploads it to storage"

	walDirectoryFlag        = "wal-directory"
	walDirectoryDescription = "If WAL directory is different of DB directory, set this parameter (full path to wal directory)"
)

var (
	// backupPushCmd represents the backupPush command
	backupPushCmd = &cobra.Command{
		Use:   "backup-push db_directory",
		Short: backupPushShortDescription, // TODO : improve description
		Args:  cobra.MaximumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var dataDirectory string

			if len(args) > 0 {
				dataDirectory = args[0]
			}

			if walDirectory == "" {
				walDirectory = dataDirectory
			}

			uploader, err := internal.ConfigureUploader()
			tracelog.ErrorLogger.FatalOnError(err)
			uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
			tracelog.ErrorLogger.FatalOnError(err)
			dbOptions := rocksdb.NewDatabaseOptions(dataDirectory, walDirectory)
			err = rocksdb.HandleBackupPush(uploader, dbOptions)
			tracelog.ErrorLogger.FatalfOnError("Backup creation failed: %v\n", err)
		},
	}
	walDirectory = ""
)

func init() {
	cmd.AddCommand(backupPushCmd)
	backupPushCmd.Flags().StringVar(&walDirectory, walDirectoryFlag,
		"", walDirectoryDescription)
}
