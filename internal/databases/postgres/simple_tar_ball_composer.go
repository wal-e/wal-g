package postgres

import (
	"archive/tar"
	"context"
	"os"

	"github.com/wal-g/wal-g/internal"

	"github.com/wal-g/wal-g/internal/crypto"
	"golang.org/x/sync/errgroup"
)

type SimpleTarBallComposer struct {
	tarBallQueue  *internal.TarBallQueue
	tarFilePacker *TarBallFilePacker
	crypter       crypto.Crypter
	files         *SimpleBundleFiles
	tarFileSets   TarFileSets
	errorGroup    *errgroup.Group
	ctx           context.Context
}

func NewSimpleTarBallComposer(
	tarBallQueue *internal.TarBallQueue,
	tarBallFilePacker *TarBallFilePacker,
	files *SimpleBundleFiles,
	crypter crypto.Crypter,
) *SimpleTarBallComposer {
	errorGroup, ctx := errgroup.WithContext(context.Background())
	return &SimpleTarBallComposer{
		tarBallQueue:  tarBallQueue,
		tarFilePacker: tarBallFilePacker,
		crypter:       crypter,
		files:         files,
		tarFileSets:   make(TarFileSets),
		errorGroup:    errorGroup,
		ctx:           ctx,
	}
}

type SimpleTarBallComposerMaker struct {
	filePackerOptions TarBallFilePackerOptions
}

func NewSimpleTarBallComposerMaker(filePackerOptions TarBallFilePackerOptions) *SimpleTarBallComposerMaker {
	return &SimpleTarBallComposerMaker{filePackerOptions: filePackerOptions}
}

func (maker *SimpleTarBallComposerMaker) Make(bundle *Bundle) (TarBallComposer, error) {
	bundleFiles := &SimpleBundleFiles{}
	tarBallFilePacker := newTarBallFilePacker(bundle.DeltaMap,
		bundle.IncrementFromLsn, bundleFiles, maker.filePackerOptions)
	return NewSimpleTarBallComposer(bundle.TarBallQueue, tarBallFilePacker, bundleFiles, bundle.Crypter), nil
}

func (c *SimpleTarBallComposer) AddFile(info *ComposeFileInfo) {
	tarBall, err := c.tarBallQueue.DequeCtx(c.ctx)
	if err != nil {
		return
	}
	tarBall.SetUp(c.crypter)
	c.errorGroup.Go(func() error {
		err := c.tarFilePacker.PackFileIntoTar(info, tarBall)
		if err != nil {
			return err
		}
		return c.tarBallQueue.CheckSizeAndEnqueueBack(tarBall)
	})
}

func (c *SimpleTarBallComposer) AddLabelFiles(tarFileSets *TarFileSets, tarBallName string, labelFiles []string) {
}

func (c *SimpleTarBallComposer) AddHeader(fileInfoHeader *tar.Header, info os.FileInfo) error {
	tarBall, err := c.tarBallQueue.DequeCtx(c.ctx)
	if err != nil {
		return c.errorGroup.Wait()
	}
	tarBall.SetUp(c.crypter)
	defer c.tarBallQueue.EnqueueBack(tarBall)
	c.files.AddFile(fileInfoHeader, info, false)
	return tarBall.TarWriter().WriteHeader(fileInfoHeader)
}

func (c *SimpleTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	c.files.AddSkippedFile(tarHeader, fileInfo)
}

func (c *SimpleTarBallComposer) PackTarballs() (TarFileSets, error) {
	err := c.errorGroup.Wait()
	if err != nil {
		return nil, err
	}
	return c.tarFileSets, nil
}

func (c *SimpleTarBallComposer) GetFiles() BundleFiles {
	return c.files
}
