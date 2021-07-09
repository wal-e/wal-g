package postgres

import (
	"archive/tar"
	"context"
	"os"
	
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
)

type copyStatus int

const (
	possibleCopy copyStatus = iota // Mark files in previous tarball when it unclear whether tarball could be copied
	doNotCopy
	fromNew
	processed
)

type fileInfo struct {
	status copyStatus
	info   *ComposeFileInfo
}

type headerInfo struct {
	status         copyStatus
	fileInfoHeader *tar.Header
	info           os.FileInfo
}

type CopyTarBallComposer struct {
	tarBallQueue  *internal.TarBallQueue
	tarFilePacker *TarBallFilePacker
	crypter       crypto.Crypter
	files         *RegularBundleFiles
	ctx           context.Context
	errorGroup    *errgroup.Group

	prevBackup             Backup
	newBackupName          string
	tarFileSets            TarFileSets
	tarUnchangedFilesCount map[string]int
	prevFileTar            map[string]string
	prevTarFileSets        TarFileSets
	fileInfo               map[string]*fileInfo
	headerInfos            map[string]*headerInfo
	copiedTars             map[string]bool
}

type CopyTarBallComposerMaker struct {
	previousBackup    Backup
	newBackupName     string
	filePackerOptions TarBallFilePackerOptions
}

func NewCopyTarBallComposerMaker(previousBackup Backup, newBackupName string,
	filePackerOptions TarBallFilePackerOptions) *CopyTarBallComposerMaker {
	return &CopyTarBallComposerMaker{previousBackup, newBackupName, filePackerOptions}
}

func NewCopyTarBallComposer(
	tarBallQueue *internal.TarBallQueue,
	tarBallFilePacker *TarBallFilePacker,
	files *RegularBundleFiles,
	crypter crypto.Crypter,
	prevBackup Backup,
	newBackupName string,
	tarUnchangedFilesCount map[string]int,
	prevFileTar map[string]string,
	prevTarFileSets TarFileSets,
) *CopyTarBallComposer {
	errorGroup, ctx := errgroup.WithContext(context.Background())
	prevBackup.GetSentinel()
	return &CopyTarBallComposer{
		tarBallQueue:           tarBallQueue,
		tarFilePacker:          tarBallFilePacker,
		crypter:                crypter,
		files:                  files,
		ctx:                    ctx,
		errorGroup:             errorGroup,
		prevBackup:             prevBackup,
		newBackupName:          newBackupName,
		tarFileSets:            make(TarFileSets),
		tarUnchangedFilesCount: tarUnchangedFilesCount,
		prevFileTar:            prevFileTar,
		prevTarFileSets:        prevTarFileSets,
		fileInfo:               make(map[string]*fileInfo),
		headerInfos:            make(map[string]*headerInfo),
		copiedTars:             make(map[string]bool),
	}
}

func (maker *CopyTarBallComposerMaker) Make(bundle *Bundle) (TarBallComposer, error) {
	prevFileTar := make(map[string]string)
	prevTarFileSets := make(map[string][]string)
	tarUnchangedFilesCount := make(map[string]int)
	for tarName, fileSet := range maker.previousBackup.SentinelDto.TarFileSets {
		for _, fileName := range fileSet {
			prevFileTar[fileName] = tarName
			prevTarFileSets[tarName] = append(prevTarFileSets[tarName], fileName)
		}
		tarUnchangedFilesCount[tarName] = len(fileSet)
	}
	files := &RegularBundleFiles{}
	tarBallFilePacker := newTarBallFilePacker(bundle.DeltaMap,
		bundle.IncrementFromLsn, files, maker.filePackerOptions)
	return NewCopyTarBallComposer(bundle.TarBallQueue, tarBallFilePacker, files,
		bundle.Crypter, maker.previousBackup, maker.newBackupName, tarUnchangedFilesCount,
		prevFileTar, prevTarFileSets), nil
}

func (c *CopyTarBallComposer) AddFile(info *ComposeFileInfo) {
	var fileName = info.header.Name
	var currFile = fileInfo{}
	if _, exists := c.prevFileTar[fileName]; exists {
		if !c.prevBackup.SentinelDto.Files[fileName].MTime.Equal(info.header.ModTime) {
			c.tarUnchangedFilesCount[c.prevFileTar[fileName]] = -1
			currFile.status = doNotCopy
		} else { 
			c.tarUnchangedFilesCount[c.prevFileTar[fileName]] -= 1
			currFile.status = possibleCopy
		}
	} else {
		currFile.status = fromNew
	}
	currFile.info = info
	c.fileInfo[fileName] = &currFile
}

func (c *CopyTarBallComposer) AddHeader(fileInfoHeader *tar.Header, info os.FileInfo) error {
	var fileName = fileInfoHeader.Name
	var currHeader = headerInfo{}
	if _, exists := c.prevFileTar[fileName]; exists {
		if !c.prevBackup.SentinelDto.Files[fileName].MTime.Equal(info.ModTime()) {
			c.tarUnchangedFilesCount[c.prevFileTar[fileName]] = -1
			currHeader.status = doNotCopy
		} else { 
			c.tarUnchangedFilesCount[c.prevFileTar[fileName]] -= 1
			currHeader.status = possibleCopy
		}
	} else {
		currHeader.status = fromNew
	}
	currHeader.info = info
	currHeader.fileInfoHeader = fileInfoHeader
	c.files.AddFile(fileInfoHeader, info, false)
	c.headerInfos[fileName] = &currHeader
	return nil
}

func (c *CopyTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	c.files.AddSkippedFile(tarHeader, fileInfo)
}

func (c *CopyTarBallComposer) copyTar(tarName string) {
	tracelog.InfoLogger.Printf("Copying %s ...\n", tarName)
	c.prevBackup.Folder.CopyObject(
		utility.BaseBackupPath,
		c.prevBackup.Name+internal.TarPartitionFolderName+tarName,
		c.newBackupName+internal.TarPartitionFolderName+tarName)
	c.copiedTars[tarName] = true
	c.tarBallQueue.TarBallMaker.AddCopiedTarName(tarName)
	for _, fileName := range c.prevTarFileSets[tarName] {
		if file, exists := c.fileInfo[fileName]; exists {
			file.status = processed
			c.tarFileSets[tarName] = append(c.tarFileSets[tarName], fileName)
			c.files.AddFile(file.info.header, file.info.fileInfo, file.info.isIncremented)
		} else if header, exists := c.headerInfos[fileName]; exists {
			header.status = processed
			c.tarFileSets[tarName] = append(c.tarFileSets[tarName], fileName)
			c.files.AddFile(header.fileInfoHeader, header.info, false)
		}
	}
}

func (c *CopyTarBallComposer) getTarBall() internal.TarBall {
	tarBall := c.tarBallQueue.Deque()
	tarBall.SetUp(c.crypter)
	return tarBall
}

func (c *CopyTarBallComposer) copyUnchangedTars() {
	for tarName, cnt := range c.tarUnchangedFilesCount {
		if cnt != 0 {
			for _, fileName := range c.prevTarFileSets[tarName] {
				if _, exists := c.fileInfo[fileName]; exists {
					c.fileInfo[fileName].status = doNotCopy
				} else if _, exists := c.headerInfos[fileName]; exists {
					c.headerInfos[fileName].status = doNotCopy
				}
			}
		}
	}

	for fileName := range c.fileInfo {
		file := c.fileInfo[fileName]
		if file.status == possibleCopy {
			c.copyTar(c.prevFileTar[fileName])
		}
	}
	for headerName := range c.headerInfos {
		header := c.headerInfos[headerName]
		if header.status == possibleCopy {
			c.copyTar(c.prevFileTar[headerName])
		}
	}
}

func (c *CopyTarBallComposer) PackTarballs() (TarFileSets, error) {
	c.copyUnchangedTars()	

	var tarBall internal.TarBall = nil
	for fileName := range c.fileInfo {
		file := c.fileInfo[fileName]
		if file.status == doNotCopy || file.status == fromNew {
			tarBall = c.getTarBall()
			c.errorGroup.Go(func() error {
				err := c.tarFilePacker.PackFileIntoTar(file.info, tarBall)
				if err != nil {
					return err
				}
				return c.tarBallQueue.CheckSizeAndEnqueueBack(tarBall)
			})
			c.tarFileSets[tarBall.Name()] = append(c.tarFileSets[tarBall.Name()], fileName)
			file.status = processed
		}
	}

	for headerName := range c.headerInfos {
		header := c.headerInfos[headerName]
		if header.status == doNotCopy || header.status == fromNew {
			tarBall = c.getTarBall()
			tarBall.TarWriter().WriteHeader(header.fileInfoHeader)
			c.tarFileSets[tarBall.Name()] = append(c.tarFileSets[tarBall.Name()], headerName)
			c.files.AddFile(header.fileInfoHeader, header.info, false)
			c.tarBallQueue.EnqueueBack(tarBall)
		}
		header.status = processed
	}
	err := c.errorGroup.Wait()
	if err != nil {
		return nil, err
	}
	return c.tarFileSets, nil
}

func (c *CopyTarBallComposer) GetFiles() BundleFiles {
	return c.files
}
