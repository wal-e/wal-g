package internal

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/utility"
)

// CompressAndEncryptError is used to catch specific errors from CompressAndEncrypt
// when uploading to Storage. Will not retry upload if this error occurs.
type CompressAndEncryptError struct {
	error
}

func newCompressingPipeWriterError(reason string) CompressAndEncryptError {
	return CompressAndEncryptError{errors.New(reason)}
}

func (err CompressAndEncryptError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// CompressAndEncrypt compresses input to a pipe reader. Output must be used or
// pipe will block.
func CompressAndEncrypt(source io.Reader, compressor compression.Compressor, crypter crypto.Crypter) io.Reader {
	compressedReader, dstWriter := io.Pipe()

	var writeCloser io.WriteCloser = dstWriter
	if crypter != nil {
		var err error
		writeCloser, err = crypter.Encrypt(dstWriter)

		if err != nil {
			panic(err)
		}
	}

	writeIgnorer := &EmptyWriteIgnorer{writeCloser}
	compressedWriter := compressor.NewWriter(writeIgnorer)

	go func() {
		_, err := utility.FastCopy(compressedWriter, source)

		if err != nil {
			e := newCompressingPipeWriterError("CompressAndEncrypt: compression failed")
			_ = dstWriter.CloseWithError(e)
		}

		if err := compressedWriter.Close(); err != nil {
			e := newCompressingPipeWriterError("CompressAndEncrypt: writer close failed")
			_ = dstWriter.CloseWithError(e)
			return
		}
		if crypter != nil {
			err := writeCloser.Close()

			if err != nil {
				e := newCompressingPipeWriterError("CompressAndEncrypt: encryption failed")
				_ = dstWriter.CloseWithError(e)
				return
			}
		}
		_ = dstWriter.Close()
	}()
	return compressedReader
}
