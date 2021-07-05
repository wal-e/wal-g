package rocksdb

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jedib0t/go-pretty/table"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

// TODO : unit tests
func HandleDetailedBackupList(folder storage.Folder, pretty bool, json bool) {
	backups, err := internal.GetBackups(folder)
	if len(backups) == 0 {
		tracelog.InfoLogger.Println("No backups found")
		return
	}
	tracelog.ErrorLogger.FatalOnError(err)
	// if details are requested we append content of metadata.json to each line

	backupDetails, err := GetBackupsDetails(folder, backups)
	tracelog.ErrorLogger.FatalOnError(err)

	switch {
	case json:
		err = internal.WriteAsJSON(backupDetails, os.Stdout, pretty)
	case pretty:
		writePrettyBackupListDetails(backupDetails, os.Stdout)
	default:
		err = writeBackupListDetails(backupDetails, os.Stdout)
	}
	tracelog.ErrorLogger.FatalOnError(err)
}

func GetBackupsDetails(folder storage.Folder, backups []internal.BackupTime) ([]BackupInfo, error) {
	backupsDetails := make([]BackupInfo, 0, len(backups))
	for i := len(backups) - 1; i >= 0; i-- {
		details, err := GetBackupDetails(folder, backups[i])
		if err != nil {
			return nil, err
		}
		backupsDetails = append(backupsDetails, details)
	}
	return backupsDetails, nil
}

func GetBackupDetails(folder storage.Folder, backupTime internal.BackupTime) (BackupInfo, error) {
	backup := internal.NewBackup(folder, backupTime.BackupName)

	metaData := BackupInfo{}
	err := backup.FetchSentinel(&metaData)
	if err != nil {
		return BackupInfo{}, err
	}
	return metaData, nil
}

// TODO : unit tests
func writeBackupListDetails(backupDetails []BackupInfo, output io.Writer) error {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer func() { _ = writer.Flush() }()
	_, err := fmt.Fprintln(writer, "BackupName\traw_backup_size\tuploaded_backup_size\tbackuptime") //nolint:lll
	if err != nil {
		return err
	}
	for i, count := 0, len(backupDetails); i < count; i++ {
		b := backupDetails[i]
		_, err = fmt.Fprintf(writer, "%v\t%v\t%v\t%v\n",
			b.BackupName, b.RawSize, b.BackupSize, time.Unix(int64(b.Timestamp), 0).Format(time.RFC850)) //nolint:lll
		if err != nil {
			return err
		}
	}
	return nil
}

//TODO : unit tests
func writePrettyBackupListDetails(backupDetails []BackupInfo, output io.Writer) {
	writer := table.NewWriter()
	writer.SetOutputMirror(output)
	defer writer.Render()
	writer.AppendHeader(table.Row{"#", "BackupName", "Raw size", "Uploaded backup size", "BackupDate"}) //nolint:lll
	for idx := range backupDetails {
		b := &backupDetails[idx]
		writer.AppendRow(table.Row{idx + 1, b.BackupName, b.RawSize, b.BackupSize, time.Unix(int64(b.Timestamp), 0).Format(time.RFC850)}) //nolint:lll
	}
}
