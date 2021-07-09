package internal

import (
	"archive/tar"
	"io/ioutil"
	"sync/atomic"

	"github.com/wal-g/wal-g/internal/crypto"
)

// NOPTarBall mocks a tarball. Used for prefault logic.
type NOPTarBall struct {
	number    int
	partSize  *int64
	tarWriter *tar.Writer
}

func (tarBall *NOPTarBall) Name() string {
	return "NOPTarBall"
}

func (tarBall *NOPTarBall) SetUp(crypter crypto.Crypter, params ...string) {}
func (tarBall *NOPTarBall) CloseTar() error                                { return nil }

func (tarBall *NOPTarBall) Size() int64            { return atomic.LoadInt64(tarBall.partSize) }
func (tarBall *NOPTarBall) AddSize(i int64)        { atomic.AddInt64(tarBall.partSize, i) }
func (tarBall *NOPTarBall) TarWriter() *tar.Writer { return tarBall.tarWriter }
func (tarBall *NOPTarBall) AwaitUploads()          {}

// NOPTarBallMaker creates a new NOPTarBall. Used
// for testing purposes.
type NOPTarBallMaker struct {
	number   int
	partSize *int64
}

// Make creates a new NOPTarBall.
func (tarBallMaker *NOPTarBallMaker) Make(inheritState bool) TarBall {
	tarBallMaker.number++
	return &NOPTarBall{
		number:    tarBallMaker.number,
		partSize:  tarBallMaker.partSize,
		tarWriter: tar.NewWriter(ioutil.Discard),
	}
}

func (tarBallMaker *NOPTarBallMaker) AddCopiedTarName(tarName string) {
	panic("AddCopiedTarName is not implemented for NOPTarBallMaker")
}

func NewNopTarBallMaker() TarBallMaker {
	size := int64(0)
	return &NOPTarBallMaker{0, &size}
}
