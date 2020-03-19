package internal

import (
	"io"

	"github.com/wal-g/wal-g/storages/storage"
)

// StorageReaderMaker creates readers for downloading from storage
type StorageReaderMaker struct {
	Folder       storage.Folder
	RelativePath string
}

func newStorageReaderMaker(folder storage.Folder, relativePath string) *StorageReaderMaker {
	return &StorageReaderMaker{folder, relativePath}
}

func (readerMaker *StorageReaderMaker) Path() string { return readerMaker.RelativePath }

func (readerMaker *StorageReaderMaker) Reader() (io.ReadCloser, error) {
	return readerMaker.Folder.ReadObject(readerMaker.RelativePath)
}
