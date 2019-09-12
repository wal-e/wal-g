package internal

import (
	"archive/tar"
	"io"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/crypto"
)

// A TarBall represents one tar file.
type TarBall interface {
	SetUp(crypter crypto.Crypter, args ...string)
	CloseTar() error
	Size() int64
	AddSize(int64)
	TarWriter() *tar.Writer
	AwaitUploads()
}

func PackFileTo(tarBall TarBall, fileInfoHeader *tar.Header, fileContent io.Reader) (fileSize int64, err error) {
	tarWriter := tarBall.TarWriter()
	err = tarWriter.WriteHeader(fileInfoHeader)
	if err != nil {
		return 0, errors.Wrap(err, "PackFileTo: failed to write header")
	}

	fileSize, err = io.Copy(tarWriter, fileContent)
	if err != nil {
		return fileSize, errors.Wrap(err, "PackFileTo: copy failed")
	}

	tarBall.AddSize(fileInfoHeader.Size)
	return fileSize, nil
}
