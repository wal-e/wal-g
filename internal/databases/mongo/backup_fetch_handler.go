package mongo

import (
	"context"
	"github.com/wal-g/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"
	"os/exec"
)

func HandleBackupFetch(ctx context.Context, folder storage.Folder, backupName string, restoreCmd *exec.Cmd) {
	internal.HandleBackupFetch(folder, backupName, internal.GetCommandStreamFetcher(restoreCmd))
}
