package pg

import (
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"

	"github.com/spf13/cobra"
)

const (
	BackupPushShortDescription     = "Makes backup and uploads it to storage"
	PermanentFlag                  = "permanent"
	FullBackupFlag                 = "full"
	VerifyPagesFlag                = "verify"
	StoreAllCorruptBlocksFlag      = "store-all-corrupt"
	PermanentShorthand             = "p"
	FullBackupShorthand            = "f"
	VerifyPagesShorthand           = "v"
	StoreAllCorruptBlocksShorthand = "s"
)

var (
	// backupPushCmd represents the backupPush command
	backupPushCmd = &cobra.Command{
		Use:   "backup-push db_directory",
		Short: BackupPushShortDescription, // TODO : improve description
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			uploader, err := internal.ConfigureWalUploader()
			tracelog.ErrorLogger.FatalOnError(err)
			verifyPageChecksums = verifyPageChecksums || viper.GetBool(internal.VerifyPageChecksumsSetting)
			storeAllCorruptBlocks = storeAllCorruptBlocks || viper.GetBool(internal.StoreAllCorruptBlocksSetting)
			internal.HandleBackupPush(uploader, args[0], permanent, fullBackup, verifyPageChecksums, storeAllCorruptBlocks)
		},
	}
	permanent             = false
	fullBackup            = false
	verifyPageChecksums   = false
	storeAllCorruptBlocks = false
)

func init() {
	Cmd.AddCommand(backupPushCmd)

	backupPushCmd.Flags().BoolVarP(&permanent, PermanentFlag, PermanentShorthand, false, "Pushes permanent backup")
	backupPushCmd.Flags().BoolVarP(&fullBackup, FullBackupFlag, FullBackupShorthand, false, "Make full backup-push")
	backupPushCmd.Flags().BoolVarP(&verifyPageChecksums, VerifyPagesFlag, VerifyPagesShorthand, false, "Verify page checksums")
	backupPushCmd.Flags().BoolVarP(&storeAllCorruptBlocks, StoreAllCorruptBlocksFlag, StoreAllCorruptBlocksShorthand,
		false, "Store all corrupt blocks found during page checksum verification")
}
