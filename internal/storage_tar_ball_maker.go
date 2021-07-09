package internal

// StorageTarBallMaker creates tarballs that are uploaded to storage.
type StorageTarBallMaker struct {
	partCount       int
	backupName      string
	uploader        *Uploader
	tarNameResolver *TarCopiesNameResolver
}

func NewStorageTarBallMaker(backupName string, uploader *Uploader) *StorageTarBallMaker {
	return &StorageTarBallMaker{0, backupName, uploader, NewTarCopiesNameResolver()}
}

// Make returns a tarball with required storage fields.
func (tarBallMaker *StorageTarBallMaker) Make(dedicatedUploader bool) TarBall {
	tarBallMaker.partCount++
	uploader := tarBallMaker.uploader
	if dedicatedUploader {
		uploader = uploader.Clone()
	}
	size := int64(0)
	return &StorageTarBall{
		partNumber:   tarBallMaker.partCount,
		backupName:   tarBallMaker.backupName,
		uploader:     uploader,
		partSize:     &size,
		resolver:     tarBallMaker.tarNameResolver,
	}
}

func (tarBallMaker *StorageTarBallMaker) AddCopiedTarName(tarName string) {
	tarBallMaker.tarNameResolver.copiedTarNames[tarName] = true
}