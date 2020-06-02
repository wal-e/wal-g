package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
)

const (
	backupPushShortDescription = "Creates new backup and pushes it to storage"

	FullBackupFlag      = "full"
	FullBackupShorthand = "f"
)

var (
	// backupPushCmd represents the streamPush command
	backupPushCmd = &cobra.Command{
		Use:   "backup-push",
		Short: backupPushShortDescription,
		Run: func(cmd *cobra.Command, args []string) {
			uploader, err := internal.ConfigureUploader()
			tracelog.ErrorLogger.FatalOnError(err)
			backupCmd, err := internal.GetCommandSetting(internal.NameStreamCreateCmd)
			tracelog.ErrorLogger.FatalOnError(err)
			mysql.HandleBackupPush(uploader, backupCmd, fullBackup)
		},
	}
	fullBackup = false
)

func init() {
	Cmd.AddCommand(backupPushCmd)

	backupPushCmd.Flags().BoolVarP(&fullBackup, FullBackupFlag, FullBackupShorthand, false, "Make full backup-push")
}
