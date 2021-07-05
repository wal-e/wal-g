package rocksdb

import (
	"archive/tar"
	"os"
)

// TarBallComposer is used to compose files into tarballs.
type TarBallComposer interface {
	AddFile(info *ComposeFileInfo)
	PackTarballs() (TarFileSets, error)
}

// ComposeFileInfo holds data which is required to pack a file to some tarball
type ComposeFileInfo struct {
	path     string
	fileInfo os.FileInfo
	header   *tar.Header
}

type TarFileSets map[string][]string

func NewComposeFileInfo(path string, fileInfo os.FileInfo, header *tar.Header) *ComposeFileInfo {
	return &ComposeFileInfo{path: path, fileInfo: fileInfo, header: header}
}

// TarBallComposerMaker is used to make an instance of TarBallComposer
type TarBallComposerMaker interface {
	Make(bundle *Bundle) (TarBallComposer, error)
}

func NewTarBallComposerMaker() TarBallComposerMaker {
	return &RegularTarBallComposerMaker{}
}
