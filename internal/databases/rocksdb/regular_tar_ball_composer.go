package rocksdb

import (
	"context"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto"

	"golang.org/x/sync/errgroup"
)

type RegularTarBallComposer struct {
	tarBallQueue  *internal.TarBallQueue
	tarFilePacker *TarBallFilePacker
	crypter       crypto.Crypter
	files         *RegularBundleFiles
	tarFileSets   TarFileSets
	errorGroup    *errgroup.Group
	ctx           context.Context
}

func NewRegularTarBallComposer(
	tarBallQueue *internal.TarBallQueue,
	tarBallFilePacker *TarBallFilePacker,
	files *RegularBundleFiles,
	crypter crypto.Crypter,
) *RegularTarBallComposer {
	errorGroup, ctx := errgroup.WithContext(context.Background())
	return &RegularTarBallComposer{
		tarBallQueue:  tarBallQueue,
		tarFilePacker: tarBallFilePacker,
		crypter:       crypter,
		files:         files,
		tarFileSets:   make(TarFileSets),
		errorGroup:    errorGroup,
		ctx:           ctx,
	}
}

type RegularTarBallComposerMaker struct {
}

func (maker *RegularTarBallComposerMaker) Make(bundle *Bundle) (TarBallComposer, error) {
	bundleFiles := &RegularBundleFiles{}
	tarBallFilePacker := newTarBallFilePacker(bundleFiles)
	return NewRegularTarBallComposer(bundle.TarBallQueue, tarBallFilePacker, bundleFiles, bundle.Crypter), nil
}

func (c *RegularTarBallComposer) AddFile(info *ComposeFileInfo) {
	tarBall, err := c.tarBallQueue.DequeCtx(c.ctx)
	if err != nil {
		return
	}
	tarBall.SetUp(c.crypter)
	c.tarFileSets[tarBall.Name()] = append(c.tarFileSets[tarBall.Name()], info.header.Name)
	c.errorGroup.Go(func() error {
		err := c.tarFilePacker.PackFileIntoTar(info, tarBall)
		if err != nil {
			return err
		}
		return c.tarBallQueue.CheckSizeAndEnqueueBack(tarBall)
	})
}

func (c *RegularTarBallComposer) PackTarballs() (TarFileSets, error) {
	err := c.errorGroup.Wait()
	if err != nil {
		return nil, err
	}
	return c.tarFileSets, nil
}
