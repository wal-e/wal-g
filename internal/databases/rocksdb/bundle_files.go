package rocksdb

import (
	"archive/tar"
	"os"
	"sync"

	"github.com/wal-g/wal-g/internal"
)

// BundleFiles represents the files in the backup that is going to be created
type BundleFiles interface {
	AddFile(tarHeader *tar.Header, fileInfo os.FileInfo)
}

type RegularBundleFiles struct {
	sync.Map
}

func (files *RegularBundleFiles) AddFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	files.Store(tarHeader.Name,
		internal.BackupFileDescription{IsSkipped: false, MTime: fileInfo.ModTime()})
}
