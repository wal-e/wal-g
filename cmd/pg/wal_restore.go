package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/storages/fs"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	WalRestoreUsage            = "wal-restore"
	WalRestoreShortDescription = "Restores WAL segments from storage."
	WalRestoreLongDescription  = "Restores the missing WAL segments that will be needed to perform pg_rewind with storage."
)

// walRestoreCmd represents the walRestore command
var walRestoreCmd = &cobra.Command{
	Use:   WalRestoreUsage,
	Short: WalRestoreShortDescription,
	Long:  WalRestoreLongDescription,
	Run: func(cmd *cobra.Command, checks []string) {
		localDir := internal.GetPgDataFolderPath()
		localFolder, err := fs.ConfigureFolder(localDir, nil)
		tracelog.ErrorLogger.FatalfOnError("Error on configure local folder %v\n", err)
		externalFolder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalfOnError("Error on configure external folder %v\n", err)

		internal.HandleWALRestore(externalFolder, localFolder)
	},
}

func init() {
	cmd.AddCommand(walRestoreCmd)
}
