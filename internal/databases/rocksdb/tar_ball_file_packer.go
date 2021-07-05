package rocksdb

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/wal-g/wal-g/internal"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
)

type FileNotExistError struct {
	error
}

func newFileNotExistError(path string) FileNotExistError {
	return FileNotExistError{errors.Errorf(
		"%s does not exist, probably deleted during the backup creation\n", path)}
}

func (err FileNotExistError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TarBallFilePacker is used to pack bundle file into tarball.
type TarBallFilePacker struct {
	files BundleFiles
}

func newTarBallFilePacker(files BundleFiles) *TarBallFilePacker {
	return &TarBallFilePacker{
		files: files,
	}
}

// TODO : unit tests
func (p *TarBallFilePacker) PackFileIntoTar(cfi *ComposeFileInfo, tarBall internal.TarBall) error {
	fileReadCloser, err := p.createFileReadCloser(cfi)
	if err != nil {
		switch err.(type) {
		case FileNotExistError:
			// File was deleted before opening.
			// We should ignore file here as if it did not exist.
			tracelog.WarningLogger.Println(err)
			return nil
		default:
			return err
		}
	}
	errorGroup, _ := errgroup.WithContext(context.Background())

	p.files.AddFile(cfi.header, cfi.fileInfo)

	errorGroup.Go(func() error {
		defer utility.LoggedClose(fileReadCloser, "")
		packedFileSize, err := internal.PackFileTo(tarBall, cfi.header, fileReadCloser)
		if err != nil {
			return errors.Wrap(err, "PackFileIntoTar: operation failed")
		}
		if packedFileSize != cfi.header.Size {
			return newTarSizeError(packedFileSize, cfi.header.Size)
		}
		return nil
	})

	return errorGroup.Wait()
}

func (p *TarBallFilePacker) createFileReadCloser(cfi *ComposeFileInfo) (io.ReadCloser, error) {
	var err error
	fileReadCloser, err := startReadingFile(cfi.header, cfi.fileInfo, cfi.path)
	if err != nil {
		return nil, err
	}
	return fileReadCloser, nil
}

// TODO : unit tests
func startReadingFile(fileInfoHeader *tar.Header, info os.FileInfo, path string) (io.ReadCloser, error) {
	fileInfoHeader.Size = info.Size()
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, newFileNotExistError(path)
		}
		return nil, errors.Wrapf(err, "startReadingFile: failed to open file '%s'\n", path)
	}
	diskLimitedFileReader := limiters.NewDiskLimitReader(file)
	fileReader := &ioextensions.ReadCascadeCloser{
		Reader: &io.LimitedReader{
			R: io.MultiReader(diskLimitedFileReader, &ioextensions.ZeroReader{}),
			N: fileInfoHeader.Size,
		},
		Closer: file,
	}
	return fileReader, nil
}
