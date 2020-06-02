package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"

	"github.com/wal-g/storages/storage"
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
// DownloadAndDecompressStream downloads, decompresses and writes stream to stdout
func DownloadAndDecompressStream(backup *Backup, writeCloser io.WriteCloser) error {
	for _, decompressor := range compression.Decompressors {
		archiveReader, exists, err := TryDownloadFile(backup.BaseBackupFolder, getStreamName(backup.Name, decompressor.FileExtension()))
		if err != nil {
			return err
		}
		if !exists {
			continue
		}

		err = DecompressDecryptBytes(&EmptyWriteIgnorer{WriteCloser: writeCloser}, archiveReader, decompressor)
		if err != nil {
			return err
		}
		utility.LoggedClose(writeCloser, "")
		return nil
	}
	return newArchiveNonExistenceError(fmt.Sprintf("Archive '%s' does not exist.\n", backup.Name))
}

// TODO : unit tests
func FetchStreamSentinel(backup *Backup, sentinelDto interface{}) error {
	sentinelDtoData, err := backup.fetchSentinelData()
	if err != nil {
		return errors.Wrap(err, "failed to fetch sentinel")
	}
	err = json.Unmarshal(sentinelDtoData, sentinelDto)
	return errors.Wrap(err, "failed to unmarshal sentinel")
}

// DownloadFile downloads, decompresses and decrypts
func DownloadFile(folder storage.Folder, filename, ext string, writeCloser io.WriteCloser) error {
	decompressor := compression.FindDecompressor(ext)
	if decompressor == nil {
		return fmt.Errorf("decompressor for extension '%s' was not found", ext)
	}
	archiveReader, exists, err := TryDownloadFile(folder, filename)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("File '%s' does not exist.\n", filename)
	}

	err = DecompressDecryptBytes(&EmptyWriteIgnorer{WriteCloser: writeCloser}, archiveReader, decompressor)
	if err != nil {
		return err
	}
	utility.LoggedClose(writeCloser, "")
	return nil
}
