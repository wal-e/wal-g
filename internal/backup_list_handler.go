package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jedib0t/go-pretty/table"
	"github.com/wal-g/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type InfoLogger interface {
	Println(v ...interface{})
}

type ErrorLogger interface {
	FatalOnError(err error)
}

type Logging struct {
	InfoLogger  InfoLogger
	ErrorLogger ErrorLogger
}

func DefaultHandleBackupList(folder storage.Folder) {
	getBackupsFunc := func() ([]BackupTime, error) {
		return getBackups(folder)
	}
	writeBackupListFunc := func(backups []BackupTime) {
		WriteBackupList(backups, os.Stdout)
	}
	logging := Logging{
		InfoLogger:  tracelog.InfoLogger,
		ErrorLogger: tracelog.ErrorLogger,
	}

	HandleBackupList(getBackupsFunc, writeBackupListFunc, logging)
}

func HandleBackupList(
	getBackupsFunc func() ([]BackupTime, error),
	writeBackupListFunc func([]BackupTime),
	logging Logging,
) {
	backups, err := getBackupsFunc()
	if len(backups) == 0 {
		logging.InfoLogger.Println("No backups found")
		return
	}
	logging.ErrorLogger.FatalOnError(err)

	writeBackupListFunc(backups)
}

// TODO : unit tests
func HandleBackupListWithFlags(folder storage.Folder, pretty bool, json bool, detail bool) {
	backups, err := getBackups(folder)
	if len(backups) == 0 {
		tracelog.InfoLogger.Println("No backups found")
		return
	}
	tracelog.ErrorLogger.FatalOnError(err)
	// if details are requested we append content of metadata.json to each line
	if detail {
		backupDetails, err := getBackupDetails(folder, backups)
		tracelog.ErrorLogger.FatalOnError(err)
		if json {
			err = WriteAsJson(backupDetails, os.Stdout, pretty)
			tracelog.ErrorLogger.FatalOnError(err)
		} else if pretty {
			writePrettyBackupListDetails(backupDetails, os.Stdout)
		} else {
			writeBackupListDetails(backupDetails, os.Stdout)
		}
	} else {
		if json {
			err = WriteAsJson(backups, os.Stdout, pretty)
			tracelog.ErrorLogger.FatalOnError(err)
		} else if pretty {
			WritePrettyBackupList(backups, os.Stdout)
		} else {
			WriteBackupList(backups, os.Stdout)
		}
	}
}

func getBackupDetails(folder storage.Folder, backups []BackupTime) ([]BackupDetail, error) {
	backupDetails := make([]BackupDetail, len(backups))
	for i := len(backups) - 1; i >= 0; i-- {
		backup, err := GetBackupByName(backups[i].BackupName, utility.BaseBackupPath, folder)
		if err != nil {
			return nil, err
		} else {
			metaData, err := backup.fetchMeta()
			if err != nil {
				return nil, err
			}
			backupDetails[i] = BackupDetail{backups[i], metaData}
		}
	}
	return backupDetails, nil
}

// TODO : unit tests
func WriteBackupList(backups []BackupTime, output io.Writer) {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer writer.Flush()
	fmt.Fprintln(writer, "name\tlast_modified\twal_segment_backup_start")
	for i := len(backups) - 1; i >= 0; i-- {
		b := backups[i]
		fmt.Fprintln(writer, fmt.Sprintf("%v\t%v\t%v", b.BackupName, b.Time.Format(time.RFC3339), b.WalFileName))
	}
}

// TODO : unit tests
func writeBackupListDetails(backupDetails []BackupDetail, output io.Writer) {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer writer.Flush()
	fmt.Fprintln(writer, "name\tlast_modified\twal_segment_backup_start\tstart_time\tfinish_time\thostname\tdata_dir\tpg_version\tstart_lsn\tfinish_lsn\tis_permanent")
	for i := len(backupDetails) - 1; i >= 0; i-- {
		b := backupDetails[i]
		fmt.Fprintln(writer, fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v", b.BackupName, b.Time.Format(time.RFC3339), b.WalFileName, b.StartTime.Format(time.RFC850), b.FinishTime.Format(time.RFC850), b.Hostname, b.DataDir, b.PgVersion, b.StartLsn, b.FinishLsn, b.IsPermanent))
	}
}

// TODO : unit tests
func WritePrettyBackupList(backups []BackupTime, output io.Writer) {
	writer := table.NewWriter()
	writer.SetOutputMirror(output)
	defer writer.Render()
	writer.AppendHeader(table.Row{"#", "Name", "Last modified", "WAL segment backup start"})
	for i, b := range backups {
		writer.AppendRow(table.Row{i, b.BackupName, b.Time.Format(time.RFC850), b.WalFileName})
	}
}

// TODO : unit tests
func writePrettyBackupListDetails(backupDetails []BackupDetail, output io.Writer) {
	writer := table.NewWriter()
	writer.SetOutputMirror(output)
	defer writer.Render()
	writer.AppendHeader(table.Row{"#", "Name", "Last modified", "WAL segment backup start", "Start time", "Finish time", "Hostname", "Datadir", "PG Version", "Start LSN", "Finish LSN", "Permanent"})
	for i, b := range backupDetails {
		writer.AppendRow(table.Row{i, b.BackupName, b.Time.Format(time.RFC850), b.WalFileName, b.StartTime.Format(time.RFC850), b.FinishTime.Format(time.RFC850), b.Hostname, b.DataDir, b.PgVersion, b.StartLsn, b.FinishLsn, b.IsPermanent})
	}
}

func WriteAsJson(data interface{}, output io.Writer, pretty bool) error {
	var bytes []byte
	var err error
	if pretty {
		bytes, err = json.MarshalIndent(data, "", "    ")
	} else {
		bytes, err = json.Marshal(data)
	}
	if err != nil {
		return err
	}
	_, err = output.Write(bytes)
	return err
}
