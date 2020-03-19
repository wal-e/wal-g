package mysql

import (
	"bufio"
	"github.com/wal-g/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
	"io"
	"time"
)

const pipeChunkSize = 64 * 1024

func copyFixed(dst io.Writer, src io.Reader, chunk int) error {
	buf := make([]byte, chunk)
	for {
		n, err := io.ReadFull(src, buf)
		if n > 0 {
			_, err2 := dst.Write(buf[:n])
			if err2 != nil {
				return err2
			}
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func replayHandler(logFolder storage.Folder, logName string, endTs time.Time) (needAbortFetch bool, err error) {
	reader, err := internal.DownloadAndDecompressWALFile(logFolder, logName)
	bufReader := bufio.NewReaderSize(reader, utility.Mebibyte)
	binlogReader := NewBinlogReader(bufReader, time.Unix(0, 0), endTs)
	cmd, err := internal.GetCommandSetting(internal.MysqlBinlogReplayCmd)
	if err != nil {
		return true, err
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return true, err
	}
	defer stdin.Close()
	tracelog.InfoLogger.Printf("replaying %s ...", logName)
	err = cmd.Start()
	if err != nil {
		return true, err
	}
	err = copyFixed(stdin, binlogReader, pipeChunkSize)
	stdin.Close()
	cmdErr := cmd.Wait()
	if cmdErr != nil {
		return true, cmdErr
	}
	if err != nil {
		return true, err
	}
	return binlogReader.NeedAbort(), nil
}

func HandleBinlogReplay(folder storage.Folder, backupName string, untilDT string) {
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Unable to get backup %v", err)

	startTs, err := getBinlogStartTs(folder, backup)
	tracelog.ErrorLogger.FatalOnError(err)

	endTs, err := configureEndTs(untilDT)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Printf("Fetching binlogs since %s until %s", startTs, endTs)
	_, err = fetchLogs(folder, startTs, endTs, replayHandler)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch logs: %v", err)
}
