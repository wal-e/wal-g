package rocksdb

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

var confirmed = false

const (
	deleteBackupTargetUsage = "target backup_name"
)

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: internal.DeleteShortDescription, // TODO : improve description
}

var deleteBeforeCmd = &cobra.Command{
	Use:  internal.DeleteBeforeUsageExample, // TODO : improve description
	Args: internal.DeleteBeforeArgsValidator,
	Run:  runDeleteBefore,
}

var deleteRetainCmd = &cobra.Command{
	Use:       internal.DeleteRetainUsageExample, // TODO : improve description
	ValidArgs: internal.StringModifiers,
	Run:       runDeleteRetain,
}

var deleteEverythingCmd = &cobra.Command{
	Use:       internal.DeleteEverythingUsageExample, // TODO : improve description
	ValidArgs: internal.StringModifiersDeleteEverything,
	Args:      internal.DeleteEverythingArgsValidator,
	Run:       runDeleteEverything,
}

var deleteTargetCmd = &cobra.Command{
	Use:  deleteBackupTargetUsage, // TODO : improve description
	Args: internal.DeleteTargetArgsValidator,
	Run:  runDeleteTarget,
}

func runDeleteBefore(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	objects, err := internal.GetBackupSentinelObjects(folder)
	tracelog.ErrorLogger.FatalOnError(err)
	deleteHandler := newDeleteHandler(folder, createBackupObjects(objects))
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteBefore(args, confirmed)
}

func runDeleteRetain(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	objects, err := internal.GetBackupSentinelObjects(folder)
	tracelog.ErrorLogger.FatalOnError(err)
	deleteHandler := newDeleteHandler(folder, createBackupObjects(objects))
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteRetain(args, confirmed)
}

func runDeleteEverything(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	objects, err := internal.GetBackupSentinelObjects(folder)
	tracelog.ErrorLogger.FatalOnError(err)
	deleteHandler := newDeleteHandler(folder, createBackupObjects(objects))
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteEverything(args, make(map[string]bool), confirmed)
}

func runDeleteTarget(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	objects, err := internal.GetBackupSentinelObjects(folder)
	tracelog.ErrorLogger.FatalOnError(err)
	deleteHandler := newDeleteHandler(folder, createBackupObjects(objects))
	tracelog.ErrorLogger.FatalOnError(err)
	targetBackupSelector, err := internal.NewTargetBackupSelector("", args[0], nil)
	tracelog.ErrorLogger.FatalOnError(err)
	deleteHandler.HandleDeleteTarget(targetBackupSelector, confirmed, false)
}

func init() {
	cmd.AddCommand(deleteCmd)

	deleteCmd.AddCommand(deleteRetainCmd, deleteBeforeCmd, deleteEverythingCmd, deleteTargetCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
}

func newDeleteHandler(folder storage.Folder, backups []internal.BackupObject) *internal.DeleteHandler {
	return internal.NewDeleteHandler(folder, backups, lessFunc)
}

func lessFunc(object1 storage.Object, object2 storage.Object) bool {
	time1 := object1.GetLastModified()
	time2 := object2.GetLastModified()
	return time1.Before(time2) || time1.Equal(time2)
}

func createBackupObjects(objects []storage.Object) []internal.BackupObject {
	backupObjects := make([]internal.BackupObject, 0)
	for _, object := range objects {
		backupObjects = append(backupObjects, internal.NewDefaultBackupObject(object))
	}
	return backupObjects
}
