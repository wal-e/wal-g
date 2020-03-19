package internal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/utility"
)

type InvalidWalFileMagicError struct {
	error
}

func newInvalidWalFileMagicError() InvalidWalFileMagicError {
	return InvalidWalFileMagicError{errors.New("WAL-G: WAL file magic is invalid ")}
}

func (err InvalidWalFileMagicError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type ArchiveNonExistenceError struct {
	error
}

func newArchiveNonExistenceError(archiveName string) ArchiveNonExistenceError {
	return ArchiveNonExistenceError{errors.Errorf("Archive '%s' does not exist.\n", archiveName)}
}

func (err ArchiveNonExistenceError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TODO : unit tests
// HandleWALFetch is invoked to performa wal-g wal-fetch
func HandleWALFetch(folder storage.Folder, walFileName string, location string, triggerPrefetch bool) {
	tracelog.DebugLogger.Printf("HandleWALFetch(folder, %s, %s, %v)\n", walFileName, location, triggerPrefetch)
	folder = folder.GetSubFolder(utility.WalPath)
	location = utility.ResolveSymlink(location)
	if triggerPrefetch {
		defer forkPrefetch(walFileName, location)
	}

	_, _, running, prefetched := getPrefetchLocations(path.Dir(location), walFileName)
	seenSize := int64(-1)

	for {
		if stat, err := os.Stat(prefetched); err == nil {
			if stat.Size() != int64(WalSegmentSize) {
				tracelog.ErrorLogger.Println("WAL-G: Prefetch error: wrong file size of prefetched file ", stat.Size())
				break
			}

			err = os.Rename(prefetched, location)
			tracelog.ErrorLogger.FatalOnError(err)

			err := checkWALFileMagic(location)
			if err != nil {
				tracelog.ErrorLogger.Println("Prefetched file contain errors", err)
				_ = os.Remove(location)
				break
			}

			return
		} else if !os.IsNotExist(err) {
			tracelog.ErrorLogger.FatalError(err)
		}

		// We have race condition here, if running is renamed here, but it's OK

		if runStat, err := os.Stat(running); err == nil {
			observedSize := runStat.Size() // If there is no progress in 50 ms - start downloading myself
			if observedSize <= seenSize {
				defer func() {
					_ = os.Remove(running) // we try to clean up and ignore here any error
					_ = os.Remove(prefetched)
				}()
				break
			}
			seenSize = observedSize
		} else if os.IsNotExist(err) {
			break // Normal startup path
		} else {
			break // Abnormal path. Permission denied etc. Yes, I know that previous 'else' can be eliminated.
		}
		time.Sleep(50 * time.Millisecond)
	}

	err := DownloadWALFileTo(folder, walFileName, location)
	tracelog.ErrorLogger.FatalOnError(err)
}

// TODO : unit tests
func checkWALFileMagic(prefetched string) error {
	file, err := os.Open(prefetched)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(file, "")
	magic := make([]byte, 4)
	_, err = file.Read(magic)
	if err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(magic) < 0xD061 {
		return newInvalidWalFileMagicError()
	}

	return nil
}

func TryDownloadFile(folder storage.Folder, path string) (walFileReader io.ReadCloser, exists bool, err error) {
	walFileReader, err = folder.ReadObject(path)
	if err == nil {
		exists = true
		return
	}
	if _, ok := errors.Cause(err).(storage.ObjectNotFoundError); ok {
		err = nil
	}
	return
}

// TODO : unit tests
func DecompressDecryptBytes(dst io.Writer, archiveReader io.ReadCloser, decompressor compression.Decompressor) error {
	crypter := ConfigureCrypter()
	if crypter != nil {
		reader, err := crypter.Decrypt(archiveReader)
		if err != nil {
			return err
		}
		archiveReader = ioextensions.ReadCascadeCloser{
			Reader: reader,
			Closer: archiveReader,
		}
	}

	err := decompressor.Decompress(dst, archiveReader)
	return err
}

// CachedDecompressor is the file extension describing decompressor
type CachedDecompressor struct {
	FileExtension string
}

func GetLastDecompressor() (compression.Decompressor, error) {
	var cache CachedDecompressor
	var cacheFilename string

	usr, err := user.Current()
	if err == nil {
		cacheFilename = filepath.Join(usr.HomeDir, ".walg_decompressor_cache")
		file, err := ioutil.ReadFile(cacheFilename)
		if err == nil {
			err = json.Unmarshal(file, &cache)
			if err != nil {
				return nil, err
			}
			return compression.FindDecompressor(cache.FileExtension), nil
		}
		return nil, err
	}

	return nil, nil
}

func SetLastDecompressor(decompressor compression.Decompressor) error {
	var cache CachedDecompressor
	usr, err := user.Current()

	if err != nil {
		return err
	}

	cacheFilename := filepath.Join(usr.HomeDir, ".walg_decompressor_cache")
	cache.FileExtension = decompressor.FileExtension()

	marshal, err := json.Marshal(&cache)
	if err == nil {
		return ioutil.WriteFile(cacheFilename, marshal, 0644)
	}

	return err
}

func convertDecompressorList(decompressors []compression.Decompressor,
	lastDecompressor compression.Decompressor) []compression.Decompressor {
	ret := append(make([]compression.Decompressor, 0, len(decompressors)), lastDecompressor)

	for _, elem := range decompressors {
		if elem != lastDecompressor {
			ret = append(ret, elem)
		}
	}

	return ret
}

func putCachedDecompressorInFirstPlace(decompressors []compression.Decompressor) []compression.Decompressor {
	lastDecompressor, _ := GetLastDecompressor()

	if lastDecompressor != nil && lastDecompressor != decompressors[0] {
		return convertDecompressorList(decompressors, lastDecompressor)
	}

	return decompressors
}

// TODO : unit tests
func DownloadAndDecompressWALFile(folder storage.Folder, walFileName string) (io.ReadCloser, error) {
	for _, decompressor := range putCachedDecompressorInFirstPlace(compression.Decompressors) {
		archiveReader, exists, err := TryDownloadFile(folder, walFileName+"."+decompressor.FileExtension())
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		_ = SetLastDecompressor(decompressor)
		reader, writer := io.Pipe()
		go func() {
			err = DecompressDecryptBytes(&EmptyWriteIgnorer{writer}, archiveReader, decompressor)
			_ = writer.CloseWithError(err)
		}()
		return reader, nil
	}
	return nil, newArchiveNonExistenceError(walFileName)
}

// TODO : unit tests
// downloadWALFileTo downloads a file and writes it to local file
func DownloadWALFileTo(folder storage.Folder, walFileName string, dstPath string) error {
	reader, err := DownloadAndDecompressWALFile(folder, walFileName)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(reader, "")
	return ioextensions.CreateFileWith(dstPath, reader)
}
