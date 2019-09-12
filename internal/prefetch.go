package internal

import (
	"archive/tar"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/tinsane/storages/storage"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/utility"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// TODO : unit tests
// HandleWALPrefetch is invoked by wal-fetch command to speed up database restoration
func HandleWALPrefetch(uploader *Uploader, walFileName string, location string) {
	folder := uploader.UploadingFolder.GetSubFolder(utility.WalPath)
	var fileName = walFileName
	location = path.Dir(location)
	waitGroup := &sync.WaitGroup{}
	concurrency, err := GetMaxDownloadConcurrency()
	tracelog.ErrorLogger.FatalOnError(err)

	for i := 0; i < concurrency; i++ {
		fileName, err = GetNextWalFilename(fileName)
		if err != nil {
			tracelog.ErrorLogger.Println("WAL-prefetch failed: ", err, " file: ", fileName)
		}
		waitGroup.Add(1)
		go prefetchFile(location, folder, fileName, waitGroup)

		prefaultStartLsn, shouldPrefault, timelineId, err := ShouldPrefault(fileName)
		if err != nil {
			tracelog.ErrorLogger.Println("ShouldPrefault failed: ", err, " file: ", fileName)
		}
		if shouldPrefault {
			waitGroup.Add(1)
			go prefaultData(prefaultStartLsn, timelineId, waitGroup, uploader)
		}

		time.Sleep(10 * time.Millisecond) // ramp up in order
	}

	go CleanupPrefetchDirectories(walFileName, location, FileSystemCleaner{})

	waitGroup.Wait()
}

// TODO : unit tests
func prefaultData(prefaultStartLsn uint64, timelineId uint32, waitGroup *sync.WaitGroup, uploader *Uploader) {
	defer func() {
		if r := recover(); r != nil {
			tracelog.ErrorLogger.Println("Prefault unsuccessful ", prefaultStartLsn)
		}
		waitGroup.Done()
	}()

	if !uploader.getUseWalDelta() {
		return
	}

	archiveDirectory := uploader.deltaFileManager.dataFolder.(*DiskDataFolder).path
	archiveDirectory = filepath.Dir(archiveDirectory)
	archiveDirectory = filepath.Dir(archiveDirectory)
	uploadPooler, err := NewUploadPooler(NewNopTarBallMaker(), viper.GetInt64(TarSizeThresholdSetting), nil)
	tracelog.ErrorLogger.FatalOnError(err)
	bundle := NewBundle(uploadPooler, archiveDirectory, &prefaultStartLsn, nil, timelineId)
	err = bundle.DownloadDeltaMap(uploader.UploadingFolder.GetSubFolder(utility.WalPath), prefaultStartLsn+WalSegmentSize*WalFileInDelta)
	if err != nil {
		tracelog.ErrorLogger.Printf("Error during loading delta map: '%+v'.", err)
		return
	}

	// Start a new tar bundle, walk the archiveDirectory and upload everything there.
	tracelog.InfoLogger.Println("Walking for prefault...")
	err = filepath.Walk(archiveDirectory, bundle.PrefaultWalkedFSObject)
	tracelog.ErrorLogger.FatalOnError(err)
}

// TODO : unit tests
func (bundle *Bundle) PrefaultWalkedFSObject(path string, info os.FileInfo, err error) error {
	if err != nil {
		if os.IsNotExist(err) {
			tracelog.WarningLogger.Println(path, " deleted during filepath walk")
			return nil
		}
		return err
	}

	if info.Name() != PgControl {
		err = bundle.prefaultHandleTar(path, info)
		if err != nil {
			if err == filepath.SkipDir {
				return err
			}
			return errors.Wrap(err, "HandleWalkedFSObject: handle tar failed")
		}
	}
	return nil
}

// TODO : unit tests
func (bundle *Bundle) prefaultHandleTar(path string, info os.FileInfo) error {
	fileName := info.Name()
	_, excluded := ExcludedFilenames[fileName]
	isDir := info.IsDir()

	if excluded && !isDir {
		return nil
	}

	fileInfoHeader, err := tar.FileInfoHeader(info, fileName)
	if err != nil {
		return errors.Wrap(err, "handleTar: could not grab header info")
	}

	fileInfoHeader.Name = bundle.GetFileRelPath(path)

	if !excluded && info.Mode().IsRegular() {
		tarBall := bundle.UploadPooler.Deque()
		go func() {
			err := bundle.prefaultFile(path, info, fileInfoHeader)
			if err != nil {
				panic(err)
			}
			err = bundle.UploadPooler.CheckSizeAndEnqueueBack(tarBall)
			if err != nil {
				panic(err)
			}
		}()
	} else {
		if excluded && isDir {
			return filepath.SkipDir
		}
	}

	return nil
}

// TODO : unit tests
func (bundle *Bundle) prefaultFile(path string, info os.FileInfo, fileInfoHeader *tar.Header) error {
	incrementBaseLsn := bundle.GetIncrementBaseLsn()
	isIncremented := isPagedFile(info, path)
	if !isIncremented {
		return nil
	}
	bitmap, err := bundle.getDeltaBitmapFor(path)
	if _, ok := err.(NoBitmapFoundError); ok { // this file has not changed after the start of backup, so just skip it
		return nil
	} else if err != nil {
		return errors.Wrapf(err, "packFileIntoTar: failed to find corresponding bitmap '%s'\n", path)
	}
	tracelog.InfoLogger.Println("Prefaulting ", path)
	var fileReader io.ReadCloser
	fileReader, fileInfoHeader.Size, err = ReadIncrementalFile(path, info.Size(), *incrementBaseLsn, bitmap)
	if _, ok := err.(InvalidBlockError); ok {
		return nil
	} else if err != nil {
		return errors.Wrapf(err, "packFileIntoTar: failed reading incremental file '%s'\n", path)
	}
	defer utility.LoggedClose(fileReader, "")

	_, err = io.Copy(ioutil.Discard, fileReader)
	return errors.Wrap(err, "packFileIntoTar: operation failed")
}

// TODO : unit tests
func prefetchFile(location string, folder storage.Folder, walFileName string, waitGroup *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			tracelog.ErrorLogger.Println("Prefetch unsuccessful ", walFileName, r)
		}
		waitGroup.Done()
	}()

	_, runningLocation, oldPath, newPath := GetPrefetchLocations(location, walFileName)
	_, errO := os.Stat(oldPath)
	_, errN := os.Stat(newPath)

	if (errO == nil || !os.IsNotExist(errO)) || (errN == nil || !os.IsNotExist(errN)) {
		// Seems someone is doing something about this file
		return
	}

	tracelog.InfoLogger.Println("WAL-prefetch file: ", walFileName)
	os.MkdirAll(runningLocation, 0755)

	err := DownloadWALFileTo(folder, walFileName, oldPath)
	tracelog.ErrorLogger.FatalOnError(err)

	_, errO = os.Stat(oldPath)
	_, errN = os.Stat(newPath)
	if errO == nil && os.IsNotExist(errN) {
		os.Rename(oldPath, newPath)
	} else {
		os.Remove(oldPath) // error is ignored
	}
}

func GetPrefetchLocations(location string, walFileName string) (prefetchLocation string, runningLocation string, runningFile string, fetchedFile string) {
	prefetchLocation = path.Join(location, ".wal-g", "prefetch")
	runningLocation = path.Join(prefetchLocation, "running")
	oldPath := path.Join(runningLocation, walFileName)
	newPath := path.Join(prefetchLocation, walFileName)
	return prefetchLocation, runningLocation, oldPath, newPath
}

// TODO : unit tests
func forkPrefetch(walFileName string, location string) {
	concurrency, err := GetMaxDownloadConcurrency()
	if err != nil {
		tracelog.ErrorLogger.Println("WAL-prefetch failed: ", err)
	}
	if strings.Contains(walFileName, "history") ||
		strings.Contains(walFileName, "partial") ||
		concurrency == 1 {
		return // There will be nothing ot prefetch anyway
	}
	cmd := exec.Command(os.Args[0], "wal-prefetch", walFileName, location)
	cmd.Env = os.Environ()
	err = cmd.Start()

	if err != nil {
		tracelog.ErrorLogger.Println("WAL-prefetch failed: ", err)
	}
}
