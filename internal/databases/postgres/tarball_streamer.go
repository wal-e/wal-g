package postgres

import (
	"archive/tar"
	"bytes"
	"io"
	"regexp"
	"strings"

	"github.com/wal-g/wal-g/internal"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

const (
	ioBufSize = 33554432
)

var (
	errTarStreamerOutputEOF     = errors.New("tarStreamer end of output tar")
	errTarInputIsDir            = errors.New("tar input is a Directory")
	errTarInputHeaderAlreadySet = errors.New("Not reading tar input, header already set")
)

type TarballStreamerRemap struct {
	from *regexp.Regexp
	to   string
}

type TarballStreamerRemaps []TarballStreamerRemap

func NewTarballStreamerRemap(from string, to string) (tsr *TarballStreamerRemap, err error) {
	fromRe, err := regexp.Compile(from)
	if err != nil {
		return nil, err
	}
	return &TarballStreamerRemap{from: fromRe, to: to}, nil
}

// TarballStreamer is used to modify tar files which are received streaming.
// Two modifications are:
// * remap: change (some of) the paths for files in the tar file, and
// * tee: copy some of the files to a second tar file
// In addition TarballStreamer maintains a list of files with their info
type TarballStreamer struct {
	// The tar stream we read from
	inputTar *tar.Reader
	// Number of bytes read from current tar file
	tarFileReadIndex int64
	// Bytes to write to one tar file. After exceeding we will split the tar file
	maxTarSize int64
	// The current header of the file in the tar stream we are currently streaming
	curHeader *tar.Header
	// Number of bytes read from current file from tar
	fileReadIndex int64
	// Buffer to read data from current file from tar
	inputBuf []byte
	// Index in buffer until where we have already forwarded to outputTar
	bufReadIndex int
	// Number of bytes in buffer
	bufDataSize int
	// tar writer to output the tar
	outputTar *tar.Writer
	// io buffer where writer writes data to
	outputIo *bytes.Buffer
	// files to write to extra tar (teeTar)
	Tee []string
	// writer that creates tee tar
	teeTar *tar.Writer
	// io buffer where writer writes data to
	TeeIo *bytes.Buffer
	// status if current file should also be written to teeTar
	teeing bool
	// list of remaps, remapping input file names to output file names
	Remaps TarballStreamerRemaps
	// list of processed files
	Files internal.BackupFileList
}

func NewTarballStreamer(input io.Reader, maxTarSize int64) (streamer *TarballStreamer) {
	streamer = &TarballStreamer{
		maxTarSize: maxTarSize,
		inputTar:   tar.NewReader(input),
		inputBuf:   make([]byte, ioBufSize),
		Files:      make(internal.BackupFileList),
		outputIo:   &(bytes.Buffer{}),
	}
	streamer.TeeIo = &(bytes.Buffer{})
	streamer.teeTar = tar.NewWriter(streamer.TeeIo)
	return streamer
}

//NextFile is what makes the TarballStreamer move to the next file.
func (streamer *TarballStreamer) NextInputFile() (err error) {
	// First output tar, or switching to next
	if streamer.outputTar == nil {
		streamer.outputTar = tar.NewWriter(streamer.outputIo)
		streamer.tarFileReadIndex = 0
		if streamer.curHeader != nil {
			err = streamer.outputTar.WriteHeader(streamer.curHeader)
			if err != nil {
				return err
			}
		}
	}

	if streamer.curHeader != nil {
		return errTarInputHeaderAlreadySet
	}

	streamer.curHeader, err = streamer.inputTar.Next()
	if err != nil {
		return err
	}
	streamer.fileReadIndex = 0

	streamer.remap()

	if streamer.tarFileReadIndex == 0 && streamer.curHeader.Size >= streamer.maxTarSize {
		tracelog.WarningLogger.Printf("This file %s is larger than max tar size. "+
			"I will create a separate tar for just this file.", streamer.curHeader.Name)
	} else if streamer.tarFileReadIndex+streamer.curHeader.Size >= streamer.maxTarSize {
		// Seems outputfile is going to exceed maxTarSize. Next file.
		return errTarStreamerOutputEOF
	}
	return streamer.addFile()
}

//addFile adds the new file to the stream
func (streamer *TarballStreamer) addFile() (err error) {
	tracelog.DebugLogger.Printf("Adding file %s", streamer.curHeader.Name)
	err = streamer.outputTar.WriteHeader(streamer.curHeader)
	if err != nil {
		return err
	}
	streamer.teeing = false
	for _, t := range streamer.Tee {
		if t == streamer.curHeader.Name {
			streamer.teeing = true
			err = streamer.teeTar.WriteHeader(streamer.curHeader)
			if err != nil {
				return err
			}
			break
		}
	}
	if !streamer.curHeader.FileInfo().IsDir() {
		if !isSimpleComposerTarBallType {
			filePath := streamer.curHeader.Name
			filePath = strings.TrimPrefix(filePath, "./")
			streamer.Files[filePath] = internal.BackupFileDescription{MTime: streamer.curHeader.ModTime}
		}
		streamer.tarFileReadIndex += streamer.curHeader.Size
	}
	return nil
}

//remap rebuilds the name of the file according to remapping rules
func (streamer *TarballStreamer) remap() {
	for _, remap := range streamer.Remaps {
		streamer.curHeader.Name = remap.from.ReplaceAllString(streamer.curHeader.Name, remap.to)
	}
}

//readFileData reads the data from a tarred file
func (streamer *TarballStreamer) readFileData() (err error) {
	err = streamer.NextInputFile()
	if err != nil && err != errTarInputHeaderAlreadySet {
		return err
	} else if streamer.curHeader.FileInfo().IsDir() {
		streamer.curHeader = nil
		return errTarInputIsDir
	}

	if streamer.bufReadIndex < streamer.bufDataSize {
		return nil
	}
	// read index is at last byte. All is read. Read next block.
	streamer.bufDataSize, err = streamer.inputTar.Read(streamer.inputBuf)
	streamer.bufReadIndex = 0
	// Update index as read from file
	streamer.fileReadIndex += int64(streamer.bufDataSize)
	if streamer.fileReadIndex > streamer.curHeader.Size {
		// Issue. We are reading more bytes than size in header.
		return tar.ErrWriteTooLong
	} else if streamer.fileReadIndex == streamer.curHeader.Size {
		// Seems we have read all from this file. Next file.
		streamer.curHeader = nil
		return nil
	}
	if err == io.EOF && streamer.bufDataSize > 0 {
		// stream reached end of file. Bytes where read, but . Lets ignore on this pass.
		return nil
	}
	return err
}

//pipeFileData calls readFileData to read file data from tar and then writes it to output tar writer
func (streamer *TarballStreamer) pipeFileData() (err error) {
	if streamer.outputIo.Len() > 0 {
		// There is still data in the buffer. Just stream that.
		return nil
	}
	err = streamer.readFileData()
	if err != nil && err != errTarInputHeaderAlreadySet {
		return err
	}

	// Write from streamer.inputBuf to outputTar
	if streamer.bufReadIndex < streamer.bufDataSize {
		nTar, err := streamer.outputTar.Write(streamer.inputBuf[streamer.bufReadIndex : streamer.bufDataSize-
			streamer.bufReadIndex])
		if err != nil {
			return err
		}
		if streamer.teeing {
			// Also tee to teeTar
			teeN, err := streamer.teeTar.Write(streamer.inputBuf[streamer.bufReadIndex : streamer.bufReadIndex+nTar])
			if err != nil {
				return err
			}
			if teeN != nTar {
				return errors.Errorf("Could not stream to tee tar file (%d out of %d only).", teeN, nTar)
			}
		}
		streamer.bufReadIndex += nTar
	}
	return nil
}

//Read is what makes the TarballStreamer an io.Reader, which can be handled by WalUploader.UploadFile.
func (streamer *TarballStreamer) Read(p []byte) (n int, err error) {
	// Handle next file header if needed, read file data if needed, and write to output tar writer
	err = streamer.pipeFileData()
	if err == errTarStreamerOutputEOF {
		tracelog.InfoLogger.Printf("maxTarSize exceeded. Closing this tar.")
		closeErr := streamer.outputTar.Close()
		if closeErr != nil {
			return 0, closeErr
		}
		n, readErr := streamer.outputIo.Read(p)
		if readErr != nil && readErr != io.EOF {
			return 0, readErr
		}
		if streamer.outputIo.Len() > 0 {
			tracelog.ErrorLogger.Printf("maxTarSize exceeded, but could not write buffer in one run.")
		}
		streamer.outputTar = nil
		streamer.outputIo.Reset()
		streamer.tarFileReadIndex = 0
		return n, io.EOF
	}
	// If err == errTarStreamerOutputEOF, we received end of output tar.
	// streamer.outputIo.Read will probably return all data and return io.EOF which will close this output tar file.
	if err != nil && err != errTarInputIsDir {
		return 0, err
	}

	// Write output of outputTar to p array passed by caller of Read()
	//streamer.outputTar.Flush()
	n, err = streamer.outputIo.Read(p)
	if err == io.EOF && streamer.curHeader.Size > streamer.fileReadIndex {
		pipeErr := streamer.pipeFileData()
		if pipeErr != nil {
			return 0, pipeErr
		}
		n, readErr := streamer.outputIo.Read(p)
		if readErr != nil {
			return n, readErr
		}
		return n, errTarStreamerOutputEOF
	}
	if err != nil {
		return n, err
	}

	return n, nil
}
