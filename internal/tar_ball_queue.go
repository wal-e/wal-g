package internal

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/abool"
)

// TarBallQueue is used to process multiple tarballs concurrently
type TarBallQueue struct {
	tarsToFillQueue  chan TarBall
	uploadQueue      chan TarBall
	parallelTarballs int
	maxUploadQueue   int
	mutex            sync.Mutex
	started          *abool.AtomicBool

	TarSizeThreshold   int64
	AllTarballsSize    *int64
	TarBallMaker       TarBallMaker
	LastCreatedTarball TarBall
}

func NewTarBallQueue(tarSizeThreshold int64, tarBallMaker TarBallMaker) *TarBallQueue {
	return &TarBallQueue{
		TarSizeThreshold: tarSizeThreshold,
		TarBallMaker:     tarBallMaker,
		AllTarballsSize:  new(int64),
		started:          abool.New(),
	}
}

func (tarQueue *TarBallQueue) StartQueue() error {
	if tarQueue.started.IsSet() {
		panic("Trying to start already started Queue")
	}
	var err error
	tarQueue.parallelTarballs, err = GetMaxUploadDiskConcurrency()
	if err != nil {
		return err
	}
	tarQueue.maxUploadQueue, err = getMaxUploadQueue()
	if err != nil {
		return err
	}

	tarQueue.tarsToFillQueue = make(chan TarBall, tarQueue.parallelTarballs)
	tarQueue.uploadQueue = make(chan TarBall, tarQueue.parallelTarballs+tarQueue.maxUploadQueue)
	for i := 0; i < tarQueue.parallelTarballs; i++ {
		tarQueue.NewTarBall(true)
		tarQueue.tarsToFillQueue <- tarQueue.LastCreatedTarball
	}

	tarQueue.started.Set()
	return nil
}

// DequeCtx returns a TarBall from the queue. If the context finishes before it
// can do so, it returns the result of ctx.Err().
func (tarQueue *TarBallQueue) DequeCtx(ctx context.Context) (TarBall, error) {
	if tarQueue.started.IsNotSet() {
		panic("Trying to deque from not started Queue")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case tarball := <-tarQueue.tarsToFillQueue:
		return tarball, nil
	}
}

func (tarQueue *TarBallQueue) Deque() TarBall {
	// The error can be ignored, since context.Background will never finish, so
	// DequeCtx will never return an error.
	tarball, _ := tarQueue.DequeCtx(context.Background())
	return tarball
}

func (tarQueue *TarBallQueue) FinishQueue() error {
	if tarQueue.started.IsNotSet() {
		panic("Trying to stop not started Queue")
	}
	tarQueue.started.UnSet()

	// We have to deque exactly this count of workers
	for i := 0; i < tarQueue.parallelTarballs; i++ {
		tarBall := <-tarQueue.tarsToFillQueue
		if tarBall.TarWriter() == nil {
			// This had written nothing
			continue
		}
		err := tarQueue.CloseTarball(tarBall)
		if err != nil {
			return errors.Wrap(err, "HandleWalkedFSObject: failed to close tarball")
		}
		tarBall.AwaitUploads()
	}

	// At this point no new tarballs should be put into uploadQueue
	for len(tarQueue.uploadQueue) > 0 {
		select {
		case otb := <-tarQueue.uploadQueue:
			otb.AwaitUploads()
		default:
		}
	}

	return nil
}

func (tarQueue *TarBallQueue) EnqueueBack(tarBall TarBall) {
	tarQueue.tarsToFillQueue <- tarBall
}

func (tarQueue *TarBallQueue) SkipTarBall(tarBall TarBall) error {
	tarQueue.mutex.Lock()
	defer tarQueue.mutex.Unlock()
	err := tarQueue.CloseTarball(tarBall)
	if err != nil {
		return errors.Wrap(err, "HandleWalkedFSObject: failed to close tarball")
	}

	tarQueue.NewTarBall(true)
	tarQueue.tarsToFillQueue <- tarQueue.LastCreatedTarball
	return nil
}

func (tarQueue *TarBallQueue) FinishTarBall(tarBall TarBall) error {
	tarQueue.mutex.Lock()
	defer tarQueue.mutex.Unlock()

	err := tarQueue.CloseTarball(tarBall)
	if err != nil {
		return errors.Wrap(err, "HandleWalkedFSObject: failed to close tarball")
	}

	tarQueue.uploadQueue <- tarBall
	for len(tarQueue.uploadQueue) > tarQueue.maxUploadQueue {
		select {
		case otb := <-tarQueue.uploadQueue:
			otb.AwaitUploads()
		default:
		}
	}

	tarQueue.NewTarBall(true)
	tarQueue.tarsToFillQueue <- tarQueue.LastCreatedTarball
	return nil
}

func (tarQueue *TarBallQueue) CheckSizeAndEnqueueBack(tarBall TarBall) error {
	if tarBall.Size() > tarQueue.TarSizeThreshold {
		return tarQueue.FinishTarBall(tarBall)
	}

	tarQueue.tarsToFillQueue <- tarBall
	return nil
}

// NewTarBall starts writing new tarball
func (tarQueue *TarBallQueue) NewTarBall(dedicatedUploader bool) TarBall {
	tarQueue.LastCreatedTarball = tarQueue.TarBallMaker.Make(dedicatedUploader)
	return tarQueue.LastCreatedTarball
}

func (tarQueue *TarBallQueue) CloseTarball(tarBall TarBall) error {
	atomic.AddInt64(tarQueue.AllTarballsSize, tarBall.Size())
	return tarBall.CloseTar()
}
