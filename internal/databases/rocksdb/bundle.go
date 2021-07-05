package rocksdb

import (
	"archive/tar"
	"fmt"
	"os"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/utility"

	"github.com/pkg/errors"
)

type TarSizeError struct {
	error
}

func newTarSizeError(packedFileSize, expectedSize int64) TarSizeError {
	return TarSizeError{errors.Errorf("packed wrong numbers of bytes %d instead of %d", packedFileSize, expectedSize)}
}

func (err TarSizeError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type Bundle struct {
	Directory string

	TarBallComposer TarBallComposer
	TarBallQueue    *internal.TarBallQueue

	Crypter  crypto.Crypter
	Timeline uint32

	TarSizeThreshold int64
}

func NewBundle(directory string, crypter crypto.Crypter, tarSizeThreshold int64) *Bundle {
	return &Bundle{
		Directory:        directory,
		Crypter:          crypter,
		TarSizeThreshold: tarSizeThreshold,
	}
}

func (bundle *Bundle) StartQueue(tarBallMaker internal.TarBallMaker) error {
	bundle.TarBallQueue = internal.NewTarBallQueue(bundle.TarSizeThreshold, tarBallMaker)
	return bundle.TarBallQueue.StartQueue()
}

func (bundle *Bundle) SetupComposer(composerMaker TarBallComposerMaker) (err error) {
	tarBallComposer, err := composerMaker.Make(bundle)
	if err != nil {
		return err
	}
	bundle.TarBallComposer = tarBallComposer
	return nil
}

func (bundle *Bundle) HandleWalkedFSObject(path string, info os.FileInfo, err error) error {
	if err != nil {
		if os.IsNotExist(err) {
			tracelog.WarningLogger.Println(path, " deleted during filepath walk")
			return nil
		}
		return errors.Wrap(err, "HandleWalkedFSObject: walk failed")
	}

	if info.IsDir() {
		return nil
	}

	err = bundle.addToBundle(path, info)
	if err != nil {
		return errors.Wrap(err, "HandleWalkedFSObject: handle tar failed")
	}

	return nil
}

func (bundle *Bundle) addToBundle(path string, info os.FileInfo) error {
	fileName := info.Name()

	fileInfoHeader, err := tar.FileInfoHeader(info, fileName)
	if err != nil {
		return errors.Wrap(err, "addToBundle: could not grab header info")
	}

	fileInfoHeader.Name = bundle.getFileRelPath(path)
	tracelog.DebugLogger.Println(fileInfoHeader.Name)

	bundle.TarBallComposer.AddFile(NewComposeFileInfo(path, info, fileInfoHeader))

	return nil
}

func (bundle *Bundle) getFileRelPath(fileAbsPath string) string {
	return utility.PathSeparator + utility.GetSubdirectoryRelativePath(fileAbsPath, bundle.Directory)
}

func (bundle *Bundle) PackTarballs() (TarFileSets, error) {
	return bundle.TarBallComposer.PackTarballs()
}

func (bundle *Bundle) FinishQueue() error {
	return bundle.TarBallQueue.FinishQueue()
}
