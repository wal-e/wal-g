package internal

import (
	"archive/tar"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

// TODO : unit tests
// HandleWALPrefetch is invoked by wal-fetch command to speed up database restoration
func HandleWALPrefetch(uploader *WalUploader, walFileName string, location string) {
	folder := uploader.UploadingFolder.GetSubFolder(utility.WalPath)
	var fileName = walFileName
	location = path.Dir(location)
	waitGroup := &sync.WaitGroup{}
	concurrency, err := getMaxDownloadConcurrency()
	tracelog.ErrorLogger.FatalOnError(err)

	for i := 0; i < concurrency; i++ {
		fileName, err = GetNextWalFilename(fileName)
		if err != nil {
			tracelog.ErrorLogger.Println("WAL-prefetch failed: ", err, " file: ", fileName)
		}
		waitGroup.Add(1)
		go prefetchFile(location, folder, fileName, waitGroup)

		prefaultStartLsn, shouldPrefault, timelineId, err := shouldPrefault(fileName)
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
func prefaultData(prefaultStartLsn uint64, timelineId uint32, waitGroup *sync.WaitGroup, uploader *WalUploader) {
	defer func() {
		if r := recover(); r != nil {
			tracelog.ErrorLogger.Println("Prefault unsuccessful ", prefaultStartLsn)
		}
		waitGroup.Done()
	}()

	if !uploader.getUseWalDelta() {
		return
	}

	archiveDirectory := uploader.DeltaFileManager.dataFolder.(*DiskDataFolder).path
	archiveDirectory = filepath.Dir(archiveDirectory)
	archiveDirectory = filepath.Dir(archiveDirectory)
	bundle := newBundle(archiveDirectory, nil, &prefaultStartLsn, nil, false)
	bundle.Timeline = timelineId
	err := bundle.DownloadDeltaMap(uploader.UploadingFolder.GetSubFolder(utility.WalPath), prefaultStartLsn+WalSegmentSize*WalFileInDelta)
	if err != nil {
		tracelog.ErrorLogger.Printf("Error during loading delta map: '%+v'.", err)
		return
	}
	bundle.TarBallMaker = newNopTarBallMaker()

	// Start a new tar bundle, walk the archiveDirectory and upload everything there.
	bundle.StartQueue()
	tracelog.InfoLogger.Println("Walking for prefault...")
	err = filepath.Walk(archiveDirectory, bundle.prefaultWalkedFSObject)
	tracelog.ErrorLogger.FatalOnError(err)
	err = bundle.FinishQueue()
}

// TODO : unit tests
func (bundle *Bundle) prefaultWalkedFSObject(path string, info os.FileInfo, err error) error {
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

	fileInfoHeader.Name = bundle.getFileRelPath(path)

	if !excluded && info.Mode().IsRegular() {
		tarBall := bundle.Deque()
		tarBall.SetUp(nil)
		go func() {
			err := bundle.prefaultFile(path, info, fileInfoHeader)
			if err != nil {
				panic(err)
			}
			err = bundle.CheckSizeAndEnqueueBack(tarBall)
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
	incrementBaseLsn := bundle.getIncrementBaseLsn()
	isIncremented := isPagedFile(info, path)
	var fileReader io.ReadCloser
	if isIncremented {
		bitmap, err := bundle.getDeltaBitmapFor(path)
		if _, ok := err.(NoBitmapFoundError); !ok { // this file has changed after the start of backup, so just skip it
			if err != nil {
				return errors.Wrapf(err, "packFileIntoTar: failed to find corresponding bitmap '%s'\n", path)
			}
			tracelog.InfoLogger.Println("Prefaulting ", path)
			fileReader, fileInfoHeader.Size, err = ReadIncrementalFile(path, info.Size(), *incrementBaseLsn, bitmap)
			if _, ok := err.(InvalidBlockError); ok {
				return nil
			} else if err != nil {
				return errors.Wrapf(err, "packFileIntoTar: failed reading incremental file '%s'\n", path)
			}

			_, err := io.Copy(ioutil.Discard, fileReader)

			if err != nil {
				return errors.Wrap(err, "packFileIntoTar: operation failed")
			}
			fileReader.Close()
		}
	}

	return nil
}

// TODO : unit tests
func prefetchFile(location string, folder storage.Folder, walFileName string, waitGroup *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			tracelog.ErrorLogger.Println("Prefetch unsuccessful ", walFileName, r)
		}
		waitGroup.Done()
	}()

	_, runningLocation, oldPath, newPath := getPrefetchLocations(location, walFileName)
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

func getPrefetchLocations(location string, walFileName string) (prefetchLocation string, runningLocation string, runningFile string, fetchedFile string) {
	prefetchLocation = path.Join(location, ".wal-g", "prefetch")
	runningLocation = path.Join(prefetchLocation, "running")
	oldPath := path.Join(runningLocation, walFileName)
	newPath := path.Join(prefetchLocation, walFileName)
	return prefetchLocation, runningLocation, oldPath, newPath
}

// TODO : unit tests
func forkPrefetch(walFileName string, location string) {
	concurrency, err := getMaxDownloadConcurrency()
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
