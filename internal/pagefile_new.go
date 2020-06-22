//
// This file holds new functionality for pagefile.go
//

package internal

import (
	"bytes"
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/walparser/parsingutil"
	"io"
	"io/ioutil"
	"os"
)

type ReadWriterAt interface {
	io.ReaderAt
	io.WriterAt
	Stat() (os.FileInfo, error)
	Name() string
}

// RestoreMissingPages restores missing pages (zero blocks)
// of local file with their base backup version
func RestoreMissingPages(base io.Reader, target ReadWriterAt) error {
	tracelog.DebugLogger.Printf("Restoring missing pages from base backup: %s\n", target.Name())

	targetPageCount, err := getPageCount(target)
	if err != nil {
		return err
	}
	for i := int64(0); i < targetPageCount; i++ {
		err = writePage(target, i, base, false)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	// check if some extra pages left in base reader
	if isEmpty := isTarReaderEmpty(base); !isEmpty {
		tracelog.DebugLogger.Printf("Skipping pages after end of the local target %s, " +
			"possibly the pagefile was truncated.\n", target.Name())
	}
	return nil
}

// CreateFileFromIncrement writes the pages from the increment to local file
// and write empty blocks in place of pages which are not present in the increment
func CreateFileFromIncrement(increment io.Reader, target ReadWriterAt) error {
	tracelog.DebugLogger.Printf("Creating from increment: %s\n", target.Name())

	fileSize, diffBlockCount, diffMap, err := getIncrementHeaderFields(increment)
	if err != nil {
		return err
	}

	// set represents all block numbers with non-empty pages
	deltaBlockNumbers := make(map[int64]bool, diffBlockCount)
	for i := uint32(0); i < diffBlockCount; i++ {
		blockNo := binary.LittleEndian.Uint32(diffMap[i*sizeofInt32 : (i+1)*sizeofInt32])
		deltaBlockNumbers[int64(blockNo)] = true
	}
	pageCount := int64(fileSize / uint64(DatabasePageSize))
	emptyPage := make([]byte, DatabasePageSize)
	for i := int64(0); i < pageCount; i++ {
		if deltaBlockNumbers[i] {
			err = writePage(target, i, increment, true)
			if err != nil {
				return err
			}
		} else {
			_, err = target.WriteAt(emptyPage, i*DatabasePageSize)
			if err != nil {
				return err
			}
		}
	}
	// check if some extra delta blocks left in increment
	if isEmpty := isTarReaderEmpty(increment); !isEmpty {
		tracelog.DebugLogger.Printf("Skipping extra increment blocks, target: %s\n", target.Name())
	}
	return nil
}

// WritePagesFromIncrement writes pages from delta backup according to diffMap
func WritePagesFromIncrement(increment io.Reader, target ReadWriterAt, overwriteExisting bool) error {
	tracelog.DebugLogger.Printf("Writing pages from increment: %s\n", target.Name())

	_, diffBlockCount, diffMap, err := getIncrementHeaderFields(increment)
	if err != nil {
		return err
	}
	targetPageCount, err := getPageCount(target)
	if err != nil {
		return err
	}

	for i := uint32(0); i < diffBlockCount; i++ {
		blockNo := int64(binary.LittleEndian.Uint32(diffMap[i*sizeofInt32 : (i+1)*sizeofInt32]))
		if blockNo >= targetPageCount {
			_, err := io.CopyN(ioutil.Discard, increment, DatabasePageSize)
			if err != nil {
				return err
			}
			continue
		}
		err = writePage(target, blockNo, increment, overwriteExisting)
		if err != nil {
			return err
		}
	}
	// at this point, we should have empty increment reader
	if isEmpty := isTarReaderEmpty(increment); !isEmpty {
		return newUnexpectedTarDataError()
	}
	return nil
}

// write page to local file
func writePage(target ReadWriterAt, blockNo int64, content io.Reader, overwrite bool) error {
	page := make([]byte, DatabasePageSize)
	_, err := io.ReadFull(content, page)
	if err != nil {
		return err
	}

	if !overwrite {
		isMissingPage, err := checkIfMissingPage(target, blockNo)
		if err != nil {
			return err
		}
		if !isMissingPage {
			return nil
		}
	}
	_, err = target.WriteAt(page, blockNo*DatabasePageSize)
	if err != nil {
		return err
	}
	return nil
}

// check if page is missing (block of zeros) in local file
func checkIfMissingPage(target ReadWriterAt, blockNo int64) (bool, error) {
	emptyPageHeader := make([]byte, headerSize)
	pageHeader := make([]byte, headerSize)
	_, err := target.ReadAt(pageHeader, blockNo*DatabasePageSize)
	if err != nil {
		return false, err
	}

	return bytes.Equal(pageHeader, emptyPageHeader), nil
}

// check that tar reader is empty
func isTarReaderEmpty(reader io.Reader) bool {
	all, _ := reader.Read(make([]byte, 1))
	return all == 0
}

func getPageCount(target ReadWriterAt) (int64, error) {
	targetFileInfo, err := target.Stat()
	if err != nil {
		return 0, errors.Wrap(err, "error getting fileInfo")
	}
	return targetFileInfo.Size() / DatabasePageSize, nil
}

func getIncrementHeaderFields(increment io.Reader) (uint64, uint32, []byte, error) {
	err := ReadIncrementFileHeader(increment)
	if err != nil {
		return 0, 0, nil, err
	}

	var fileSize uint64
	var diffBlockCount uint32
	err = parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		{Field: &fileSize, Name: "fileSize"},
		{Field: &diffBlockCount, Name: "diffBlockCount"},
	}, increment)
	if err != nil {
		return 0, 0, nil, err
	}

	diffMap := make([]byte, diffBlockCount*sizeofInt32)

	_, err = io.ReadFull(increment, diffMap)
	if err != nil {
		return 0, 0, nil, err
	}
	return fileSize, diffBlockCount, diffMap, nil
}