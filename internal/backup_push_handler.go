package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type SentinelMarshallingError struct {
	error
}

func newSentinelMarshallingError(sentinelName string, err error) SentinelMarshallingError {
	return SentinelMarshallingError{errors.Wrapf(err, "Failed to marshall sentinel file: '%s'", sentinelName)}
}

func (err SentinelMarshallingError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type BackupFromFuture struct {
	error
}

func newBackupFromFuture(backupName string) BackupFromFuture {
	return BackupFromFuture{errors.Errorf("Finish LSN of backup %v greater than current LSN", backupName)}
}

func (err BackupFromFuture) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type BackupFromOtherBD struct {
	error
}

func newBackupFromOtherBD() BackupFromOtherBD {
	return BackupFromOtherBD{errors.Errorf("Current database and database of base backup are not equal.")}
}

func (err BackupFromOtherBD) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type BackupConfig struct {
	uploader                  *WalUploader
	archiveDirectory          string
	backupsFolder             string
	previousBackupName        string
	previousBackupSentinelDto BackupSentinelDto
	isPermanent               bool
	forceIncremental          bool
	incrementCount            int
	verifyPageChecksums       bool
	storeAllCorruptBlocks     bool
	tarBallComposerType       TarBallComposerType
	userData                  string
}

// TODO : unit tests
func getDeltaConfig() (maxDeltas int, fromFull bool) {
	maxDeltas = viper.GetInt(DeltaMaxStepsSetting)
	if origin, hasOrigin := GetSetting(DeltaOriginSetting); hasOrigin {
		switch origin {
		case LatestString:
		case "LATEST_FULL":
			fromFull = true
		default:
			tracelog.ErrorLogger.Fatalf("Unknown %s: %s\n", DeltaOriginSetting, origin)
		}
	}
	return
}

func createAndPushBackup(bc *BackupConfig) {
	folder := bc.uploader.UploadingFolder
	// TODO: AB: this subfolder switch look ugly.
	// I think typed storage folders could be better (i.e. interface BasebackupStorageFolder, WalStorageFolder etc)
	bc.uploader.UploadingFolder = folder.GetSubFolder(bc.backupsFolder)
	crypter := ConfigureCrypter()
	bundle := NewBundle(bc.archiveDirectory, crypter, bc.previousBackupSentinelDto.BackupStartLSN,
		bc.previousBackupSentinelDto.Files, bc.forceIncremental, viper.GetInt64(TarSizeThresholdSetting))

	var meta ExtendedMetadataDto
	meta.StartTime = utility.TimeNowCrossPlatformUTC()
	meta.Hostname, _ = os.Hostname()
	meta.IsPermanent = bc.isPermanent

	// Connect to postgres and start/finish a nonexclusive backup.
	conn, err := Connect()
	tracelog.ErrorLogger.FatalOnError(err)
	backupName, backupStartLSN, pgVersion, dataDir, systemIdentifier, err := bundle.StartBackup(conn,
		utility.CeilTimeUpToMicroseconds(time.Now()).String())
	meta.DataDir = dataDir
	if dataDir != bc.archiveDirectory {
		warning := fmt.Sprintf("Data directory '%s' is not equal to backup-push argument '%s'", dataDir, bc.archiveDirectory)
		tracelog.WarningLogger.Println(warning)
	}
	tracelog.ErrorLogger.FatalOnError(err)

	if len(bc.previousBackupName) > 0 && bc.previousBackupSentinelDto.BackupStartLSN != nil {
		if *bc.previousBackupSentinelDto.BackupFinishLSN > backupStartLSN {
			tracelog.ErrorLogger.FatalOnError(newBackupFromFuture(bc.previousBackupName))
		}
		if bc.previousBackupSentinelDto.SystemIdentifier != nil &&
			systemIdentifier != nil &&
			*systemIdentifier != *bc.previousBackupSentinelDto.SystemIdentifier {
			tracelog.ErrorLogger.FatalOnError(newBackupFromOtherBD())
		}
		if bc.uploader.getUseWalDelta() {
			err = bundle.DownloadDeltaMap(folder.GetSubFolder(utility.WalPath), backupStartLSN)
			if err == nil {
				tracelog.InfoLogger.Println("Successfully loaded delta map, delta backup will be made with provided delta map")
			} else {
				tracelog.WarningLogger.Printf("Error during loading delta map: '%v'. Fallback to full scan delta backup\n", err)
			}
		}
		backupName = backupName + "_D_" + utility.StripWalFileName(bc.previousBackupName)
	}

	uncompressedSize, compressedSize, finishLsn, tarFileSets := uploadBackup(bundle, bc, conn, backupName)

	var tablespaceSpec *TablespaceSpec
	if !bundle.TablespaceSpec.empty() {
		tablespaceSpec = &bundle.TablespaceSpec
	}
	currentBackupSentinelDto := NewBackupSentinelDto(
		backupStartLSN, finishLsn, bc, pgVersion, tablespaceSpec, systemIdentifier,
		uncompressedSize, compressedSize, bundle.GetFiles(), tarFileSets)

	// If pushing permanent delta backup, mark all previous backups permanent
	// Do this before uploading current meta to ensure that backups are marked in increasing order
	if bc.isPermanent && currentBackupSentinelDto.IsIncremental() {
		markBackup(bc.uploader.Uploader, folder, bc.previousBackupName, true)
	}

	err = uploadMetadata(bc.uploader.Uploader, currentBackupSentinelDto, backupName, meta)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload metadata file for backup: %s %v", backupName, err)
		tracelog.ErrorLogger.FatalError(err)
	}
	err = UploadSentinel(bc.uploader.Uploader, currentBackupSentinelDto, backupName)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload sentinel file for backup: %s", backupName)
		tracelog.ErrorLogger.FatalError(err)
	}
	// logging backup set name
	tracelog.InfoLogger.Println("Wrote backup with name " + backupName)
}

func uploadBackup(
	bundle *Bundle, bc *BackupConfig, conn *pgx.Conn, backupName string) (int64, int64, uint64, TarFileSets) {
	// Start a new tar bundle, walk the archiveDirectory and upload everything there.
	err := bundle.StartQueue(NewStorageTarBallMaker(backupName, bc.uploader.Uploader))
	tracelog.ErrorLogger.FatalOnError(err)
	tarBallComposerMaker, err := NewTarBallComposerMaker(bc.tarBallComposerType, conn,
		NewTarBallFilePackerOptions(bc.verifyPageChecksums, bc.storeAllCorruptBlocks))
	tracelog.ErrorLogger.FatalOnError(err)
	err = bundle.SetupComposer(tarBallComposerMaker)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Println("Walking ...")
	err = filepath.Walk(bc.archiveDirectory, bundle.HandleWalkedFSObject)
	tracelog.ErrorLogger.FatalOnError(err)
	tarFileSets, err := bundle.PackTarballs()
	tracelog.ErrorLogger.FatalOnError(err)
	err = bundle.FinishQueue()
	tracelog.ErrorLogger.FatalOnError(err)
	err = bundle.UploadPgControl(bc.uploader.Compressor.FileExtension())
	tracelog.ErrorLogger.FatalOnError(err)
	// Stops backup and write/upload postgres `backup_label` and `tablespace_map` Files
	labelFilesTarBallName, labelFilesList, finishLsn, err := bundle.uploadLabelFiles(conn)
	tracelog.ErrorLogger.FatalOnError(err)
	uncompressedSize := atomic.LoadInt64(bundle.TarBallQueue.AllTarballsSize)
	compressedSize := atomic.LoadInt64(bc.uploader.tarSize)
	tarFileSets[labelFilesTarBallName] = append(tarFileSets[labelFilesTarBallName], labelFilesList...)
	timelineChanged := bundle.checkTimelineChanged(conn)

	// Wait for all uploads to finish.
	bc.uploader.finish()
	if bc.uploader.Failed.Load().(bool) {
		tracelog.ErrorLogger.Fatalf("Uploading failed during '%s' backup.\n", backupName)
	}
	if timelineChanged {
		tracelog.ErrorLogger.Fatalf("Cannot finish backup because of changed timeline.")
	}
	return uncompressedSize, compressedSize, finishLsn, tarFileSets
}

// TODO : unit tests
// HandleBackupPush is invoked to perform a wal-g backup-push
func HandleBackupPush(uploader *WalUploader, archiveDirectory string, isPermanent, isFullBackup,
	verifyPageChecksums, storeAllCorruptBlocks bool, tarBallComposerType TarBallComposerType,
	deltaBaseSelector BackupSelector, userData string) {
	archiveDirectory = utility.ResolveSymlink(archiveDirectory)
	checkPgVersionAndPgControl(archiveDirectory)

	var previousBackupSentinelDto BackupSentinelDto
	var previousBackupName string
	var incrementCount int

	if isFullBackup {
		tracelog.InfoLogger.Println("Doing full backup.")
	} else {
		previousBackupName,
			previousBackupSentinelDto,
			incrementCount = configureDeltaBackup(uploader.UploadingFolder, deltaBaseSelector, isPermanent)
	}

	backupConfig := BackupConfig{
		uploader:                  uploader,
		archiveDirectory:          archiveDirectory,
		backupsFolder:             utility.BaseBackupPath,
		previousBackupName:        previousBackupName,
		previousBackupSentinelDto: previousBackupSentinelDto,
		isPermanent:               isPermanent,
		forceIncremental:          false,
		incrementCount:            incrementCount,
		verifyPageChecksums:       verifyPageChecksums,
		storeAllCorruptBlocks:     storeAllCorruptBlocks,
		tarBallComposerType:       tarBallComposerType,
		userData:                  userData,
	}
	createAndPushBackup(&backupConfig)
}

func configureDeltaBackup(folder storage.Folder, deltaBaseSelector BackupSelector, isPermanent bool,
) (string, BackupSentinelDto, int) {
	maxDeltas, fromFull := getDeltaConfig()
	if maxDeltas == 0 {
		return "", BackupSentinelDto{}, 0
	}

	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	previousBackupName, err := deltaBaseSelector.Select(folder)
	if err != nil {
		if _, ok := err.(NoBackupsFoundError); ok {
			tracelog.InfoLogger.Println("Couldn't find previous backup. Doing full backup.")
			return "", BackupSentinelDto{}, 0
		}
		tracelog.ErrorLogger.FatalError(err)
	}

	previousBackup := NewBackup(baseBackupFolder, previousBackupName)
	previousBackupSentinelDto, err := previousBackup.GetSentinel()
	tracelog.ErrorLogger.FatalOnError(err)

	incrementCount := 1
	if previousBackupSentinelDto.IncrementCount != nil {
		incrementCount = *previousBackupSentinelDto.IncrementCount + 1
	}

	if incrementCount > maxDeltas {
		tracelog.InfoLogger.Println("Reached max delta steps. Doing full backup.")
		return "", BackupSentinelDto{}, 0
	}

	if previousBackupSentinelDto.BackupStartLSN == nil {
		tracelog.InfoLogger.Println("LATEST backup was made without support for delta feature. " +
			"Fallback to full backup with LSN marker for future deltas.")
		return "", BackupSentinelDto{}, 0
	}

	previousBackupMeta, err := previousBackup.FetchMeta()
	if err != nil {
		tracelog.InfoLogger.Printf(
			"Failed to get previous backup metadata: %s. Doing full backup.\n", err.Error())
		return "", BackupSentinelDto{}, 0
	}

	if !isPermanent && !fromFull && previousBackupMeta.IsPermanent {
		tracelog.InfoLogger.Println("Can't do a delta backup from permanent backup. Doing full backup.")
		return "", BackupSentinelDto{}, 0
	}

	if fromFull {
		tracelog.InfoLogger.Println("Delta will be made from full backup.")

		if previousBackupSentinelDto.IncrementFullName != nil {
			previousBackupName = *previousBackupSentinelDto.IncrementFullName
		}

		previousBackup := NewBackup(baseBackupFolder, previousBackupName)
		previousBackupSentinelDto, err = previousBackup.GetSentinel()
		tracelog.ErrorLogger.FatalOnError(err)
	}
	tracelog.InfoLogger.Printf("Delta backup from %v with LSN %x. \n",
		previousBackupName, *previousBackupSentinelDto.BackupStartLSN)
	return previousBackupName, previousBackupSentinelDto, incrementCount
}

// TODO : unit tests
func uploadMetadata(uploader *Uploader,
	sentinelDto *BackupSentinelDto,
	backupName string, meta ExtendedMetadataDto) error {
	// BackupSentinelDto struct allows nil field for backward compatiobility
	// We do not expect here nil dto since it is new dto to upload
	meta.DatetimeFormat = "%Y-%m-%dT%H:%M:%S.%fZ"
	meta.FinishTime = utility.TimeNowCrossPlatformUTC()
	meta.StartLsn = *sentinelDto.BackupStartLSN
	meta.FinishLsn = *sentinelDto.BackupFinishLSN
	meta.PgVersion = sentinelDto.PgVersion
	meta.SystemIdentifier = sentinelDto.SystemIdentifier
	meta.UserData = sentinelDto.UserData
	meta.UncompressedSize = sentinelDto.UncompressedSize
	meta.CompressedSize = sentinelDto.CompressedSize

	metaFile := storage.JoinPath(backupName, utility.MetadataFileName)
	dtoBody, err := json.Marshal(meta)
	if err != nil {
		return newSentinelMarshallingError(metaFile, err)
	}
	return uploader.Upload(metaFile, bytes.NewReader(dtoBody))
}

// TODO : unit tests
func UploadSentinel(uploader UploaderProvider, sentinelDto interface{}, backupName string) error {
	sentinelName := SentinelNameFromBackup(backupName)

	// TODO
	// Marshalling can be memory consuming operation.
	// Hopefully by this time we had done all the main job and need
	// to write only sentinel to the store. Currently I did not found
	// proper JSON lib that can stream constructed JSON to network.
	// Enventualy we will fix this. For now let's force garbage collection.

	runtime.GC()

	dtoBody, err := json.Marshal(sentinelDto)
	if err != nil {
		return newSentinelMarshallingError(sentinelName, err)
	}

	return uploader.Upload(sentinelName, bytes.NewReader(dtoBody))
}

func SentinelNameFromBackup(backupName string) string {
	return backupName + utility.SentinelSuffix
}

func checkPgVersionAndPgControl(archiveDirectory string) {
	_, err := ioutil.ReadFile(filepath.Join(archiveDirectory, PgControlPath))
	tracelog.ErrorLogger.FatalfOnError(
		"It looks like you are trying to backup not pg_data. PgControl file not found: %v\n", err)
	_, err = ioutil.ReadFile(filepath.Join(archiveDirectory, "PG_VERSION"))
	tracelog.ErrorLogger.FatalfOnError(
		"It looks like you are trying to backup not pg_data. PG_VERSION file not found: %v\n", err)
}
