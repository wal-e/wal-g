package internal

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type CantOverwriteWalFileError struct {
	error
}

func newCantOverwriteWalFileError(walFilePath string) CantOverwriteWalFileError {
	return CantOverwriteWalFileError{errors.Errorf("WAL file '%s' already archived, contents differ, unable to overwrite", walFilePath)}
}

func (err CantOverwriteWalFileError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TODO : unit tests
// HandleWALPush is invoked to perform wal-g wal-push
func HandleWALPush(uploader *WalUploader, walFilePath string) {
	if !utility.FileExists(walFilePath) {
		tracelog.ErrorLogger.Printf("walFilePath: %s doesn't exist", walFilePath)
	}
	if uploader.ArchiveStatusManager.isWalAlreadyUploaded(walFilePath) {
		err := uploader.ArchiveStatusManager.unmarkWalFile(walFilePath)

		if err != nil {
			tracelog.ErrorLogger.Printf("unmark wal-g status for %s file failed due following error %+v", walFilePath, err)
		}
		return
	}

	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.WalPath)

	concurrency, err := getMaxUploadConcurrency()
	tracelog.ErrorLogger.FatalOnError(err)

	preventWalOverwrite := viper.GetBool(PreventWalOverwriteSetting)

	bgUploader := NewBgUploader(walFilePath, int32(concurrency-1), uploader, preventWalOverwrite)
	// Look for new WALs while doing main upload
	bgUploader.Start()
	err = uploadWALFile(uploader, walFilePath, bgUploader.preventWalOverwrite)
	tracelog.ErrorLogger.FatalOnError(err)

	bgUploader.Stop()
	if uploader.getUseWalDelta() {
		uploader.FlushFiles()
	}
} //

// TODO : unit tests
// uploadWALFile from FS to the cloud
func uploadWALFile(uploader *WalUploader, walFilePath string, preventWalOverwrite bool) error {
	if preventWalOverwrite {
		overwriteAttempt, err := checkWALOverwrite(uploader, walFilePath)
		if overwriteAttempt {
			return err
		} else if err != nil {
			return errors.Wrap(err, "Couldn't check whether there is an overwrite attempt due to inner error")
		}
	}
	walFile, err := os.Open(walFilePath)
	if err != nil {
		return errors.Wrapf(err, "upload: could not open '%s'\n", walFilePath)
	}
	err = uploader.UploadWalFile(walFile)
	return errors.Wrapf(err, "upload: could not Upload '%s'\n", walFilePath)
}

// TODO : unit tests
func checkWALOverwrite(uploader *WalUploader, walFilePath string) (overwriteAttempt bool, err error) {
	walFileReader, err := DownloadAndDecompressWALFile(uploader.UploadingFolder, filepath.Base(walFilePath))
	if err != nil {
		if _, ok := err.(ArchiveNonExistenceError); ok {
			err = nil
		}
		return false, err
	}

	archived, err := ioutil.ReadAll(walFileReader)
	if err != nil {
		return false, err
	}

	localBytes, err := ioutil.ReadFile(walFilePath)
	if err != nil {
		return false, err
	}

	if !bytes.Equal(archived, localBytes) {
		return true, newCantOverwriteWalFileError(walFilePath)
	} else {
		tracelog.InfoLogger.Printf("WAL file '%s' already archived with equal content, skipping", walFilePath)
		return true, nil
	}
}
