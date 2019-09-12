package internal

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"github.com/tinsane/storages/storage"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/utility"
)

const (
	PgControl             = "pg_control"
	BackupLabelFilename   = "backup_label"
	TablespaceMapFilename = "tablespace_map"
)

type TarSizeError struct {
	error
}

func NewTarSizeError(packedFileSize, expectedSize int64) TarSizeError {
	return TarSizeError{errors.Errorf("packed wrong numbers of bytes %d instead of %d", packedFileSize, expectedSize)}
}

func (err TarSizeError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// ExcludedFilenames is a list of excluded members from the bundled backup.
var ExcludedFilenames = make(map[string]utility.Empty)

func init() {
	filesToExclude := []string{
		"log", "pg_log", "pg_xlog", "pg_wal", // Directories
		"pgsql_tmp", "postgresql.auto.conf.tmp", "postmaster.pid", "postmaster.opts", "recovery.conf", // Files
		"pg_dynshmem", "pg_notify", "pg_replslot", "pg_serial", "pg_stat_tmp", "pg_snapshots", "pg_subtrans", // Directories
	}

	for _, filename := range filesToExclude {
		ExcludedFilenames[filename] = utility.Empty{}
	}
}

// A Bundle represents the directory to
// be walked. Contains at least one TarBall
// if walk has started. Each TarBall except for the last one will be at least
// TarSizeThreshold bytes. The Sentinel is used to ensure complete
// uploaded backups; in this case, pg_control is used as
// the sentinel.
type Bundle struct {
	ArchiveDirectory   string
	Sentinel           *Sentinel
	Timeline           uint32
	IncrementFromLsn   *uint64
	IncrementFromFiles BackupFileList
	DeltaMap           PagedFileDeltaMap

	UploadPooler *UploadPooler

	Files *sync.Map
}

// TODO: use DiskDataFolder
func NewBundle(uploadPooler *UploadPooler, archiveDirectory string, incrementFromLsn *uint64, incrementFromFiles BackupFileList, timeline uint32) *Bundle {
	return &Bundle{
		ArchiveDirectory:   archiveDirectory,
		IncrementFromLsn:   incrementFromLsn,
		IncrementFromFiles: incrementFromFiles,
		Files:              &sync.Map{},
		Timeline:           timeline,
		UploadPooler:       uploadPooler,
	}
}

func (bundle *Bundle) GetFiles() BackupFileList {
	files := make(BackupFileList)
	bundle.Files.Range(func(k, v interface{}) bool {
		key := k.(string)
		description := v.(BackupFileDescription)
		files[key] = description
		return true
	})
	return files
}

func (bundle *Bundle) GetFileRelPath(fileAbsPath string) string {
	return utility.GetFileRelativePath(fileAbsPath, bundle.ArchiveDirectory)
}

// GetIncrementBaseLsn returns LSN of previous backup
func (bundle *Bundle) GetIncrementBaseLsn() *uint64 { return bundle.IncrementFromLsn }

// GetIncrementBaseFiles returns list of Files from previous backup
func (bundle *Bundle) GetIncrementBaseFiles() BackupFileList { return bundle.IncrementFromFiles }

// TODO : unit tests
// HandleWalkedFSObject walks files provided by the passed in directory
// and creates compressed tar members labeled as `part_00i.tar.*`, where '*' is compressor file extension.
//
// To see which files and directories are Skipped, please consult
// ExcludedFilenames. Excluded directories will be created but their
// contents will not be included in the tar bundle.
func (bundle *Bundle) HandleWalkedFSObject(path string, info os.FileInfo, err error) error {
	if err != nil {
		if os.IsNotExist(err) {
			tracelog.WarningLogger.Println(path, " deleted during filepath walk")
			return nil
		}
		return errors.Wrap(err, "HandleWalkedFSObject: walk failed")
	}

	if info.Name() == PgControl {
		bundle.Sentinel = &Sentinel{info, path}
	} else {
		err = bundle.handleTar(path, info)
		if err != nil {
			if err == filepath.SkipDir {
				return err
			}
			return errors.Wrap(err, "HandleWalkedFSObject: handle tar failed")
		}
	}
	return nil
}

// TODO : unit tests
// handleTar creates underlying tar writer and handles one given file.
// Does not follow symlinks. If file is in ExcludedFilenames, will not be included
// in the final tarball. EXCLUDED directories are created
// but their contents are not written to local disk.
func (bundle *Bundle) handleTar(path string, info os.FileInfo) error {
	fileName := info.Name()
	_, excluded := ExcludedFilenames[fileName]
	isDir := info.IsDir()

	if excluded && !isDir {
		return nil
	}

	fileInfoHeader, err := tar.FileInfoHeader(info, fileName)
	if err != nil {
		return errors.Wrap(err, "handleTar: could not grab header info")
	}

	fileInfoHeader.Name = bundle.GetFileRelPath(path)
	tracelog.DebugLogger.Println(fileInfoHeader.Name)

	if !excluded && info.Mode().IsRegular() {
		baseFiles := bundle.GetIncrementBaseFiles()
		baseFile, wasInBase := baseFiles[fileInfoHeader.Name]
		// It is important to take MTime before ReadIncrementalFile()
		time := info.ModTime()

		// We do not rely here on monotonic time, instead we backup file if MTime changed somehow
		// For details see
		// https://www.postgresql.org/message-id/flat/F0627DEB-7D0D-429B-97A9-D321450365B4%40yandex-team.ru#F0627DEB-7D0D-429B-97A9-D321450365B4@yandex-team.ru

		if wasInBase && (time.Equal(baseFile.MTime)) {
			// File was not changed since previous backup
			tracelog.DebugLogger.Println("Skipped due to unchanged modification time")
			bundle.Files.Store(fileInfoHeader.Name, BackupFileDescription{IsSkipped: true, IsIncremented: false, MTime: time})
			return nil
		}

		tarBall := bundle.UploadPooler.Deque()
		go func() {
			// TODO: Refactor this functional mess
			// And maybe do a better error handling
			err := bundle.packFileIntoTar(path, info, fileInfoHeader, wasInBase, tarBall)
			tracelog.ErrorLogger.PanicOnError(err)
			err = bundle.UploadPooler.CheckSizeAndEnqueueBack(tarBall)
			tracelog.ErrorLogger.PanicOnError(err)
		}()
	} else {
		tarBall := bundle.UploadPooler.Deque()
		defer bundle.UploadPooler.EnqueueBack(tarBall)
		err = tarBall.TarWriter().WriteHeader(fileInfoHeader)
		if err != nil {
			return errors.Wrap(err, "handleTar: failed to write header")
		}
		if excluded && isDir {
			return filepath.SkipDir
		}
	}

	return nil
}

// TODO : unit tests
// UploadPgControl should only be called
// after the rest of the backup is successfully uploaded to S3.
func (bundle *Bundle) UploadPgControl() error {
	fileName := bundle.Sentinel.Info.Name()
	info := bundle.Sentinel.Info
	path := bundle.Sentinel.path

	tarBall := bundle.UploadPooler.tarBallMaker.Make(false)
	tarBall.SetUp(bundle.UploadPooler.Crypter, "pg_control")
	tarWriter := tarBall.TarWriter()

	fileInfoHeader, err := tar.FileInfoHeader(info, fileName)
	if err != nil {
		return errors.Wrap(err, "UploadPgControl: failed to grab header info")
	}

	fileInfoHeader.Name = bundle.GetFileRelPath(path)
	tracelog.InfoLogger.Println(fileInfoHeader.Name)

	err = tarWriter.WriteHeader(fileInfoHeader) // TODO : what happens in case of irregular pg_control?
	if err != nil {
		return errors.Wrap(err, "UploadPgControl: failed to write header")
	}

	if info.Mode().IsRegular() {
		file, err := os.Open(path)
		if err != nil {
			return errors.Wrapf(err, "UploadPgControl: failed to open file %s\n", path)
		}

		lim := &io.LimitedReader{
			R: file,
			N: int64(fileInfoHeader.Size),
		}

		_, err = io.Copy(tarWriter, lim)
		if err != nil {
			return errors.Wrap(err, "UploadPgControl: copy failed")
		}

		tarBall.AddSize(fileInfoHeader.Size)
		utility.LoggedClose(file, "")
	}

	err = tarBall.CloseTar()
	return errors.Wrap(err, "UploadPgControl: failed to close tarball")
}

// TODO : unit tests
// UploadLabelFiles creates the `backup_label` and `tablespace_map` files by stopping the backup
// and uploads them to S3.
func (bundle *Bundle) UploadLabelFiles(label, offsetMap string) error {
	tarBall := bundle.UploadPooler.tarBallMaker.Make(false)
	tarBall.SetUp(bundle.UploadPooler.Crypter)

	labelHeader := &tar.Header{
		Name:     BackupLabelFilename,
		Mode:     int64(0600),
		Size:     int64(len(label)),
		Typeflag: tar.TypeReg,
	}

	_, err := PackFileTo(tarBall, labelHeader, strings.NewReader(label))
	if err != nil {
		return errors.Wrapf(err, "UploadLabelFiles: failed to put %s to tar", labelHeader.Name)
	}
	tracelog.InfoLogger.Println(labelHeader.Name)

	offsetMapHeader := &tar.Header{
		Name:     TablespaceMapFilename,
		Mode:     int64(0600),
		Size:     int64(len(offsetMap)),
		Typeflag: tar.TypeReg,
	}

	_, err = PackFileTo(tarBall, offsetMapHeader, strings.NewReader(offsetMap))
	if err != nil {
		return errors.Wrapf(err, "UploadLabelFiles: failed to put %s to tar", offsetMapHeader.Name)
	}
	tracelog.InfoLogger.Println(offsetMapHeader.Name)

	err = tarBall.CloseTar()
	return errors.Wrap(err, "UploadLabelFiles: failed to close tarball")
}

func (bundle *Bundle) getDeltaBitmapFor(filePath string) (*roaring.Bitmap, error) {
	if bundle.DeltaMap == nil {
		return nil, nil
	}
	return bundle.DeltaMap.GetDeltaBitmapFor(filePath)
}

func (bundle *Bundle) DownloadDeltaMap(folder storage.Folder, backupStartLSN uint64) error {
	deltaMap, err := GetDeltaMap(folder, bundle.Timeline, *bundle.IncrementFromLsn, backupStartLSN)
	if err != nil {
		return err
	}
	bundle.DeltaMap = deltaMap
	return nil
}

func (bundle *Bundle) uploadBackup() error {
	tracelog.InfoLogger.Println("Walking ...")
	err := filepath.Walk(bundle.ArchiveDirectory, bundle.HandleWalkedFSObject)
	if err != nil {
		return err
	}
	err = bundle.UploadPooler.FinishQueue()
	if err != nil {
		return err
	}
	return bundle.UploadPgControl()
}

// TODO : unit tests
func (bundle *Bundle) packFileIntoTar(path string, info os.FileInfo, fileInfoHeader *tar.Header, wasInBase bool, tarBall TarBall) error {
	incrementBaseLsn := bundle.GetIncrementBaseLsn()
	isIncremented := incrementBaseLsn != nil && wasInBase && isPagedFile(info, path)
	var fileReader io.ReadCloser
	if isIncremented {
		bitmap, err := bundle.getDeltaBitmapFor(path)
		if _, ok := err.(NoBitmapFoundError); ok { // this file has changed after the start of backup, so just skip it
			bundle.Files.Store(fileInfoHeader.Name, BackupFileDescription{IsSkipped: true, IsIncremented: false, MTime: info.ModTime()})
			return nil
		} else if err != nil {
			return errors.Wrapf(err, "packFileIntoTar: failed to find corresponding bitmap '%s'\n", path)
		}
		fileReader, fileInfoHeader.Size, err = ReadIncrementalFile(path, info.Size(), *incrementBaseLsn, bitmap)
		switch err.(type) {
		case nil:
			fileReader = &ioextensions.ReadCascadeCloser{
				Reader: &io.LimitedReader{
					R: io.MultiReader(fileReader, &ioextensions.ZeroReader{}),
					N: int64(fileInfoHeader.Size),
				},
				Closer: fileReader,
			}
		case InvalidBlockError: // fallback to full file backup
			tracelog.WarningLogger.Printf("failed to read file '%s' as incremented\n", fileInfoHeader.Name)
			isIncremented = false
			fileReader, err = startReadingFile(fileInfoHeader, info, path, fileReader)
			if err != nil {
				return err
			}
		default:
			return errors.Wrapf(err, "packFileIntoTar: failed reading incremental file '%s'\n", path)
		}
	} else {
		var err error
		fileReader, err = startReadingFile(fileInfoHeader, info, path, fileReader)
		if err != nil {
			return err
		}
	}
	defer utility.LoggedClose(fileReader, "")

	bundle.Files.Store(fileInfoHeader.Name, BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented, MTime: info.ModTime()})

	packedFileSize, err := PackFileTo(tarBall, fileInfoHeader, fileReader)
	if err != nil {
		return errors.Wrap(err, "packFileIntoTar: operation failed")
	}

	if packedFileSize != fileInfoHeader.Size {
		return NewTarSizeError(packedFileSize, fileInfoHeader.Size)
	}

	return nil
}

// TODO : unit tests
func startReadingFile(fileInfoHeader *tar.Header, info os.FileInfo, path string, fileReader io.ReadCloser) (io.ReadCloser, error) {
	fileInfoHeader.Size = info.Size()
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "packFileIntoTar: failed to open file '%s'\n", path)
	}
	diskLimitedFileReader := NewDiskLimitReader(file)
	fileReader = &ioextensions.ReadCascadeCloser{
		Reader: &io.LimitedReader{
			R: io.MultiReader(diskLimitedFileReader, &ioextensions.ZeroReader{}),
			N: int64(fileInfoHeader.Size),
		},
		Closer: file,
	}
	return fileReader, nil
}
