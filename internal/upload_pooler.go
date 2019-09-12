package internal

import (
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/crypto"
	"sync"
)

type UploadPooler struct {
	tarBallMaker     TarBallMaker
	tarSizeThreshold int64

	Crypter          crypto.Crypter
	tarballQueue     chan TarBall
	uploadQueue      chan TarBall
	parallelTarballs int
	maxUploadQueue   int
	mutex            sync.Mutex
}

func NewUploadPooler(tarBallMaker TarBallMaker, tarSizeThreshold int64, crypter crypto.Crypter) (*UploadPooler, error) {
	var err error
	parallelTarballs, err := GetMaxUploadDiskConcurrency()
	if err != nil {
		return nil, err
	}
	maxUploadQueue, err := GetMaxUploadQueue()
	if err != nil {
		return nil, err
	}

	tarballQueue := make(chan TarBall, parallelTarballs)
	uploadQueue := make(chan TarBall, parallelTarballs+maxUploadQueue)
	for i := 0; i < parallelTarballs; i++ {
		tarballQueue <- tarBallMaker.Make(true)
	}
	return &UploadPooler{
		tarBallMaker,
		tarSizeThreshold,
		crypter,
		tarballQueue,
		uploadQueue,
		parallelTarballs,
		maxUploadQueue,
		sync.Mutex{},
	}, nil
}

func (pooler *UploadPooler) FinishQueue() error {
	// We have to deque exactly this count of workers
	for i := 0; i < pooler.parallelTarballs; i++ {
		tarBall := <-pooler.tarballQueue
		if tarBall.TarWriter() == nil {
			// This had written nothing
			continue
		}
		err := tarBall.CloseTar()
		if err != nil {
			return errors.Wrap(err, "HandleWalkedFSObject: failed to close tarball")
		}
		tarBall.AwaitUploads()
	}

	// At this point no new tarballs should be put into uploadQueue
	for len(pooler.uploadQueue) > 0 {
		select {
		case otb := <-pooler.uploadQueue:
			otb.AwaitUploads()
		default:
		}
	}

	return nil
}

func (pooler *UploadPooler) CheckSizeAndEnqueueBack(tarBall TarBall) error {
	if tarBall.Size() > pooler.tarSizeThreshold {
		pooler.mutex.Lock()
		defer pooler.mutex.Unlock()

		err := tarBall.CloseTar()
		if err != nil {
			return errors.Wrap(err, "HandleWalkedFSObject: failed to close tarball")
		}

		pooler.uploadQueue <- tarBall
		for len(pooler.uploadQueue) > pooler.maxUploadQueue {
			select {
			case otb := <-pooler.uploadQueue:
				otb.AwaitUploads()
			default:
			}
		}

		tarBall = pooler.tarBallMaker.Make(true)
	}
	pooler.tarballQueue <- tarBall
	return nil
}

func (pooler *UploadPooler) EnqueueBack(tarBall TarBall) {
	pooler.tarballQueue <- tarBall
}

func (pooler *UploadPooler) Deque() TarBall {
	tarBall := <- pooler.tarballQueue
	tarBall.SetUp(pooler.Crypter)
	return tarBall
}
