package internal

// TarBallMaker is used to allow for
// flexible creation of different TarBalls.
type TarBallMaker interface {
	Make(dedicatedUploader bool) TarBall
	AddCopiedTarName(tarName string)
}
