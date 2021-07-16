package internal

import (
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/utility"
)

func ParseTS(endTSEnvVar string) (endTS *time.Time, err error) {
	endTSStr, ok := GetSetting(endTSEnvVar)
	if ok {
		t, err := time.Parse(time.RFC3339, endTSStr)
		if err != nil {
			return nil, err
		}
		endTS = &t
	}
	return endTS, nil
}

// TODO : unit tests
// GetLogsDstSettings reads from the environment variables fetch settings
func GetLogsDstSettings(operationLogsDstEnvVariable string) (dstFolder string, err error) {
	dstFolder, ok := GetSetting(operationLogsDstEnvVariable)
	if !ok {
		return dstFolder, NewUnsetRequiredSettingError(operationLogsDstEnvVariable)
	}
	return dstFolder, nil
}

// TODO : unit tests
// downloadAndDecompressStream downloads, decompresses and writes stream to stdout
func downloadAndDecompressStream(backup Backup, writeCloser io.WriteCloser) error {
	defer utility.LoggedClose(writeCloser, "")

	for _, decompressor := range compression.Decompressors {
		archiveReader, exists, err := TryDownloadFile(
			backup.Folder, GetStreamName(backup.Name, decompressor.FileExtension()))
		if err != nil {
			return errors.Wrapf(err, "failed to dowload file")
		}
		if !exists {
			continue
		}

		tracelog.DebugLogger.Printf("Found file: %s.%s", backup.Name, decompressor.FileExtension())
		err = DecompressDecryptBytes(&EmptyWriteIgnorer{WriteCloser: writeCloser}, archiveReader, decompressor)
		if err != nil {
			return errors.Wrapf(err, "failed to decompress and decrypt file")
		}
		return nil
	}
	return newArchiveNonExistenceError(fmt.Sprintf("Archive '%s' does not exist.\n", backup.Name))
}

func downloadAndDecompressStreamParts(backup Backup, writeCloser io.WriteCloser,
	fileNames []string, fetchedFilesCnt int) error {
	defer utility.LoggedClose(writeCloser, "")

	decompressor := compression.FindDecompressor(filepath.Ext(fileNames[0])[1:])
	if decompressor == nil {
		return newUnknownCompressionMethodError()
	}
	filesCh := make(chan FileResult, fetchedFilesCnt)
	go TryDownloadFiles(backup.Folder, fileNames, filesCh)
	for _, fileName := range fileNames {
		file := <-filesCh
		if file.err != nil {
			return file.err
		}
		if !file.exists {
			return newArchiveNonExistenceError(fmt.Sprintf("Archive '%s' does not exist.\n", fileName))
		}
		tracelog.DebugLogger.Printf("Found file: %s", fileName)
		err := DecompressDecryptBytes(&EmptyWriteIgnorer{WriteCloser: writeCloser}, file.readCloser, decompressor)
		if err != nil {
			return err
		}
	}
	return nil
}
