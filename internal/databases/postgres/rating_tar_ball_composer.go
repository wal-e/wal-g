package postgres

import (
	"archive/tar"
	"context"
	"os"
	"sort"
	"sync"

	"github.com/wal-g/wal-g/internal"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/crypto"
	"golang.org/x/sync/errgroup"
)

type RatingTarBallComposerMaker struct {
	fileStats         RelFileStatistics
	bundleFiles       BundleFiles
	filePackerOptions TarBallFilePackerOptions
}

func NewRatingTarBallComposerMaker(relFileStats RelFileStatistics,
	filePackerOptions TarBallFilePackerOptions) (*RatingTarBallComposerMaker, error) {
	bundleFiles := newStatBundleFiles(relFileStats)
	return &RatingTarBallComposerMaker{
		fileStats:         relFileStats,
		bundleFiles:       bundleFiles,
		filePackerOptions: filePackerOptions,
	}, nil
}

func (maker *RatingTarBallComposerMaker) Make(bundle *Bundle) (TarBallComposer, error) {
	composeRatingEvaluator := internal.NewDefaultComposeRatingEvaluator(bundle.IncrementFromFiles)
	filePacker := newTarBallFilePacker(bundle.DeltaMap, bundle.IncrementFromLsn, maker.bundleFiles, maker.filePackerOptions)
	return NewRatingTarBallComposer(uint64(bundle.TarSizeThreshold),
		composeRatingEvaluator,
		bundle.IncrementFromLsn,
		bundle.DeltaMap,
		bundle.TarBallQueue,
		bundle.Crypter,
		maker.fileStats,
		maker.bundleFiles,
		filePacker)
}

type RatedComposeFileInfo struct {
	ComposeFileInfo
	updateRating uint64
	updatesCount uint64
	// for regular files this value should match their size on the disk
	// for increments this value is the estimated size of the increment that is going to be created
	expectedSize uint64
}

// TarFilesCollection stores the files which are going to be written
// to the same tarball
type TarFilesCollection struct {
	files        []*RatedComposeFileInfo
	expectedSize uint64
}

func newTarFilesCollection() *TarFilesCollection {
	return &TarFilesCollection{files: make([]*RatedComposeFileInfo, 0), expectedSize: 0}
}

func (collection *TarFilesCollection) AddFile(file *RatedComposeFileInfo) {
	collection.files = append(collection.files, file)
	collection.expectedSize += file.expectedSize
}

// RatingTarBallComposer receives all files and tar headers
// that are going to be written to the backup,
// and composes the tarballs by placing the files
// with similar update rating in the same tarballs
type RatingTarBallComposer struct {
	filesToCompose         []*RatedComposeFileInfo
	filesToComposeMutex    sync.Mutex
	headersToCompose       []*tar.Header
	composeRatingEvaluator internal.ComposeRatingEvaluator

	tarBallQueue  *internal.TarBallQueue
	tarFilePacker *TarBallFilePacker
	crypter       crypto.Crypter

	addFileQueue     chan *ComposeFileInfo
	addFileWaitGroup sync.WaitGroup

	fileStats   RelFileStatistics
	bundleFiles BundleFiles

	incrementBaseLsn *uint64
	tarSizeThreshold uint64

	deltaMap         PagedFileDeltaMap
	deltaMapMutex    sync.RWMutex
	deltaMapComplete bool

	errorGroup *errgroup.Group
}

func NewRatingTarBallComposer(
	tarSizeThreshold uint64, updateRatingEvaluator internal.ComposeRatingEvaluator,
	incrementBaseLsn *uint64, deltaMap PagedFileDeltaMap, tarBallQueue *internal.TarBallQueue,
	crypter crypto.Crypter, fileStats RelFileStatistics, bundleFiles BundleFiles, packer *TarBallFilePacker,
) (*RatingTarBallComposer, error) {
	errorGroup, _ := errgroup.WithContext(context.Background())
	deltaMapComplete := true
	if deltaMap == nil {
		deltaMapComplete = false
		deltaMap = NewPagedFileDeltaMap()
	}

	composer := &RatingTarBallComposer{
		headersToCompose:       make([]*tar.Header, 0),
		filesToCompose:         make([]*RatedComposeFileInfo, 0),
		tarSizeThreshold:       tarSizeThreshold,
		incrementBaseLsn:       incrementBaseLsn,
		composeRatingEvaluator: updateRatingEvaluator,
		deltaMapComplete:       deltaMapComplete,
		deltaMap:               deltaMap,
		tarBallQueue:           tarBallQueue,
		crypter:                crypter,
		fileStats:              fileStats,
		bundleFiles:            bundleFiles,
		tarFilePacker:          packer,
		errorGroup:             errorGroup,
	}

	maxUploadDiskConcurrency, err := internal.GetMaxUploadDiskConcurrency()
	if err != nil {
		return nil, err
	}
	composer.addFileQueue = make(chan *ComposeFileInfo, maxUploadDiskConcurrency)
	for i := 0; i < maxUploadDiskConcurrency; i++ {
		composer.addFileWaitGroup.Add(1)
		composer.errorGroup.Go(func() error {
			return composer.addFileWorker(composer.addFileQueue)
		})
	}
	return composer, nil
}

func (c *RatingTarBallComposer) AddFile(info *ComposeFileInfo) {
	c.addFileQueue <- info
}

func (c *RatingTarBallComposer) AddLabelFiles(tarFileSets *TarFileSets, tarBallName string, labelFiles []string) {
	(*tarFileSets)[tarBallName] = append((*tarFileSets)[tarBallName], labelFiles...)
}

func (c *RatingTarBallComposer) AddHeader(fileInfoHeader *tar.Header, info os.FileInfo) error {
	c.headersToCompose = append(c.headersToCompose, fileInfoHeader)
	c.bundleFiles.AddFile(fileInfoHeader, info, false)
	return nil
}

func (c *RatingTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	c.bundleFiles.AddSkippedFile(tarHeader, fileInfo)
}

func (c *RatingTarBallComposer) PackTarballs() (TarFileSets, error) {
	close(c.addFileQueue)
	err := c.errorGroup.Wait()
	if err != nil {
		return nil, err
	}
	c.addFileWaitGroup.Wait()

	c.tarFilePacker.UpdateDeltaMap(c.deltaMap)
	headers, tarFilesCollections := c.composeFiles()
	headersTarName, headersNames, err := c.writeHeaders(headers)
	if err != nil {
		return nil, err
	}

	tarFileSets := make(map[string][]string)
	tarFileSets[headersTarName] = headersNames

	for _, tarFilesCollection := range tarFilesCollections {
		tarBall := c.tarBallQueue.Deque()
		tarBall.SetUp(c.crypter)
		for _, composeFileInfo := range tarFilesCollection.files {
			tarFileSets[tarBall.Name()] = append(tarFileSets[tarBall.Name()], composeFileInfo.header.Name)
		}
		// tarFilesCollection closure
		tarFilesCollectionLocal := tarFilesCollection
		go func() {
			for _, fileInfo := range tarFilesCollectionLocal.files {
				err := c.tarFilePacker.PackFileIntoTar(&fileInfo.ComposeFileInfo, tarBall)
				if err != nil {
					panic(err)
				}
			}
			err := c.tarBallQueue.FinishTarBall(tarBall)
			if err != nil {
				panic(err)
			}
		}()
	}

	return tarFileSets, nil
}

func (c *RatingTarBallComposer) GetFiles() BundleFiles {
	return c.bundleFiles
}

func (c *RatingTarBallComposer) addFileWorker(tasks <-chan *ComposeFileInfo) error {
	for task := range tasks {
		err := c.addFile(task)
		if err != nil {
			return err
		}
	}
	c.addFileWaitGroup.Done()
	return nil
}

func (c *RatingTarBallComposer) addFile(cfi *ComposeFileInfo) error {
	expectedFileSize, err := c.getExpectedFileSize(cfi)
	if err != nil {
		return err
	}
	updatesCount := c.fileStats.getFileUpdateCount(cfi.path)
	updateRating := c.composeRatingEvaluator.Evaluate(cfi.path, updatesCount, cfi.wasInBase)
	ratedComposeFileInfo := &RatedComposeFileInfo{*cfi, updateRating, updatesCount, expectedFileSize}
	c.filesToComposeMutex.Lock()
	defer c.filesToComposeMutex.Unlock()
	c.filesToCompose = append(c.filesToCompose, ratedComposeFileInfo)
	return nil
}

func (c *RatingTarBallComposer) sortFiles() {
	sort.Slice(c.filesToCompose, func(i, j int) bool {
		return c.filesToCompose[i].updateRating < c.filesToCompose[j].updateRating
	})
}

func (c *RatingTarBallComposer) composeFiles() ([]*tar.Header, []*TarFilesCollection) {
	c.sortFiles()
	tarFilesCollections := make([]*TarFilesCollection, 0)
	currentFilesCollection := newTarFilesCollection()
	prevUpdateRating := uint64(0)

	for _, file := range c.filesToCompose {
		// if the estimated size of the current collection exceeds the threshold,
		// or if the updateRating just went to non-zero from zero,
		// start packing to the new tar files collection
		if currentFilesCollection.expectedSize > c.tarSizeThreshold ||
			prevUpdateRating == 0 && file.updateRating > 0 {
			tarFilesCollections = append(tarFilesCollections, currentFilesCollection)
			currentFilesCollection = newTarFilesCollection()
		}
		currentFilesCollection.AddFile(file)
		prevUpdateRating = file.updateRating
	}

	tarFilesCollections = append(tarFilesCollections, currentFilesCollection)
	return c.headersToCompose, tarFilesCollections
}

func (c *RatingTarBallComposer) getExpectedFileSize(cfi *ComposeFileInfo) (uint64, error) {
	if !cfi.isIncremented {
		return uint64(cfi.fileInfo.Size()), nil
	}

	if !c.deltaMapComplete {
		err := c.scanDeltaMapFor(cfi.path, cfi.fileInfo.Size())
		if err != nil {
			return 0, err
		}
	}
	c.deltaMapMutex.RLock()
	defer c.deltaMapMutex.RUnlock()
	bitmap, err := c.deltaMap.GetDeltaBitmapFor(cfi.path)
	if _, ok := err.(NoBitmapFoundError); ok {
		// this file has changed after the start of backup and will be skipped
		// so the expected size in tar is zero
		return 0, nil
	}
	if err != nil {
		return 0, errors.Wrapf(err, "getExpectedFileSize: failed to find corresponding bitmap '%s'\n", cfi.path)
	}
	incrementBlocksCount := bitmap.GetCardinality()
	// expected header size =
	// length(IncrementFileHeader) + sizeOf(fileSize) + sizeOf(diffBlockCount) + sizeOf(blockNo)*incrementBlocksCount
	incrementHeaderSize := uint64(len(IncrementFileHeader)) +
		sizeofInt64 + sizeofInt32 + (incrementBlocksCount * sizeofInt32)
	incrementPageDataSize := incrementBlocksCount * uint64(DatabasePageSize)
	return incrementHeaderSize + incrementPageDataSize, nil
}

func (c *RatingTarBallComposer) scanDeltaMapFor(filePath string, fileSize int64) error {
	locations, err := ReadIncrementLocations(filePath, fileSize, *c.incrementBaseLsn)
	if err != nil {
		return err
	}
	c.deltaMapMutex.Lock()
	defer c.deltaMapMutex.Unlock()
	if len(locations) == 0 {
		return nil
	}
	c.deltaMap.AddLocationsToDelta(locations)
	return nil
}

func (c *RatingTarBallComposer) writeHeaders(headers []*tar.Header) (string, []string, error) {
	headersTarBall := c.tarBallQueue.Deque()
	headersTarBall.SetUp(c.crypter)
	headersNames := make([]string, 0, len(headers))
	for _, header := range headers {
		err := headersTarBall.TarWriter().WriteHeader(header)
		headersNames = append(headersNames, header.Name)
		if err != nil {
			return "", nil, errors.Wrap(err, "addToBundle: failed to write header")
		}
	}
	c.tarBallQueue.EnqueueBack(headersTarBall)
	return headersTarBall.Name(), headersNames, nil
}
