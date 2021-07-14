package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/wal-g/wal-g/internal"

	"github.com/jackc/pgconn"

	"github.com/jackc/pgx"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type backupFromFuture struct {
	error
}

func newBackupFromFuture(backupName string) backupFromFuture {
	return backupFromFuture{errors.Errorf("Finish LSN of backup %v greater than current LSN", backupName)}
}

func (err backupFromFuture) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type backupFromOtherBD struct {
	error
}

func newBackupFromOtherBD() backupFromOtherBD {
	return backupFromOtherBD{errors.Errorf("Current database and database of base backup are not equal.")}
}

func (err backupFromOtherBD) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// BackupArguments holds all arguments parsed from cmd to this handler class
type BackupArguments struct {
	isPermanent           bool
	verifyPageChecksums   bool
	storeAllCorruptBlocks bool
	tarBallComposerType   TarBallComposerType
	userData              string
	forceIncremental      bool
	backupsFolder         string
	pgDataDirectory       string
	isFullBackup          bool
	deltaBaseSelector     internal.BackupSelector
}

// CurBackupInfo holds all information that is harvest during the backup process
type CurBackupInfo struct {
	name             string
	startTime        time.Time
	startLSN         uint64
	endLSN           uint64
	uncompressedSize int64
	compressedSize   int64
	incrementCount   int
}

// PrevBackupInfo holds all information that is harvest during the backup process
type PrevBackupInfo struct {
	name        string
	sentinelDto BackupSentinelDto
}

// BackupWorkers holds the external objects that the handler uses to get the backup data / write the backup data
type BackupWorkers struct {
	uploader *WalUploader
	bundle   *Bundle
	conn     *pgx.Conn
}

// BackupPgInfo holds the PostgreSQL info that the handler queries before running the backup
type BackupPgInfo struct {
	pgVersion        int
	pgDataDirectory  string
	systemIdentifier *uint64
}

// BackupHandler is the main struct which is handling the backup process
type BackupHandler struct {
	curBackupInfo  CurBackupInfo
	prevBackupInfo PrevBackupInfo
	arguments      BackupArguments
	workers        BackupWorkers
	pgInfo         BackupPgInfo
}

var isSimpleComposerTarBallType bool

// NewBackupArguments creates a BackupArgument object to hold the arguments from the cmd
func NewBackupArguments(pgDataDirectory string, backupsFolder string, isPermanent bool, verifyPageChecksums bool,
	isFullBackup bool, storeAllCorruptBlocks bool, tarBallComposerType TarBallComposerType,
	deltaBaseSelector internal.BackupSelector, userData string) BackupArguments {
	return BackupArguments{
		pgDataDirectory:       pgDataDirectory,
		backupsFolder:         backupsFolder,
		isPermanent:           isPermanent,
		verifyPageChecksums:   verifyPageChecksums,
		isFullBackup:          isFullBackup,
		storeAllCorruptBlocks: storeAllCorruptBlocks,
		tarBallComposerType:   tarBallComposerType,
		deltaBaseSelector:     deltaBaseSelector,
		userData:              userData,
	}
}

// TODO : unit tests
func getDeltaConfig() (maxDeltas int, fromFull bool) {
	maxDeltas = viper.GetInt(internal.DeltaMaxStepsSetting)
	if origin, hasOrigin := internal.GetSetting(internal.DeltaOriginSetting); hasOrigin {
		switch origin {
		case internal.LatestString:
		case "LATEST_FULL":
			fromFull = true
		default:
			tracelog.ErrorLogger.Fatalf("Unknown %s: %s\n", internal.DeltaOriginSetting, origin)
		}
	}
	return
}

func (bh *BackupHandler) createAndPushBackup() {
	var err error
	folder := bh.workers.uploader.UploadingFolder
	// TODO: AB: this subfolder switch look ugly.
	// I think typed storage folders could be better (i.e. interface BasebackupStorageFolder, WalStorageFolder etc)
	bh.workers.uploader.UploadingFolder = folder.GetSubFolder(bh.arguments.backupsFolder)
	tracelog.DebugLogger.Printf("Uploading folder: %s", bh.workers.uploader.UploadingFolder)

	arguments := bh.arguments
	crypter := internal.ConfigureCrypter()
	bh.workers.bundle = NewBundle(bh.pgInfo.pgDataDirectory, crypter, bh.prevBackupInfo.sentinelDto.BackupStartLSN,
		bh.prevBackupInfo.sentinelDto.Files, arguments.forceIncremental,
		viper.GetInt64(internal.TarSizeThresholdSetting))

	err = bh.startBackup()
	tracelog.ErrorLogger.FatalOnError(err)
	bh.handleDeltaBackup(folder)
	tarFileSets := bh.uploadBackup()
	sentinelDto := bh.setupDTO(tarFileSets)
	bh.markBackups(folder, sentinelDto)
	bh.uploadMetadata(sentinelDto)

	// logging backup set name
	tracelog.InfoLogger.Printf("Wrote backup with name %s", bh.curBackupInfo.name)
}

func (bh *BackupHandler) startBackup() (err error) {
	// Connect to postgres and start/finish a nonexclusive backup.
	tracelog.DebugLogger.Println("Connecting to Postgres.")
	bh.workers.conn, err = Connect()
	if err != nil {
		return
	}

	tracelog.DebugLogger.Println("Running StartBackup.")
	backupName, backupStartLSN, err := bh.workers.bundle.StartBackup(
		bh.workers.conn, utility.CeilTimeUpToMicroseconds(time.Now()).String())
	if err != nil {
		return
	}
	bh.curBackupInfo.startLSN = backupStartLSN
	bh.curBackupInfo.name = backupName
	tracelog.DebugLogger.Printf("Backup name: %s\nBackup start LSN: %d", backupName, backupStartLSN)

	return
}

func (bh *BackupHandler) handleDeltaBackup(folder storage.Folder) {
	if len(bh.prevBackupInfo.name) > 0 && bh.prevBackupInfo.sentinelDto.BackupStartLSN != nil {
		tracelog.InfoLogger.Println("Delta backup enabled")
		tracelog.DebugLogger.Printf("Previous backup: %s\nBackup start LSN: %d", bh.prevBackupInfo.name,
			bh.prevBackupInfo.sentinelDto.BackupStartLSN)
		if *bh.prevBackupInfo.sentinelDto.BackupFinishLSN > bh.curBackupInfo.startLSN {
			tracelog.ErrorLogger.FatalOnError(newBackupFromFuture(bh.prevBackupInfo.name))
		}
		if bh.prevBackupInfo.sentinelDto.SystemIdentifier != nil &&
			bh.pgInfo.systemIdentifier != nil &&
			*bh.pgInfo.systemIdentifier != *bh.prevBackupInfo.sentinelDto.SystemIdentifier {
			tracelog.ErrorLogger.FatalOnError(newBackupFromOtherBD())
		}
		if bh.workers.uploader.getUseWalDelta() {
			err := bh.workers.bundle.DownloadDeltaMap(folder.GetSubFolder(utility.WalPath), bh.curBackupInfo.startLSN)
			if err == nil {
				tracelog.InfoLogger.Println("Successfully loaded delta map, delta backup will be made with provided " +
					"delta map")
			} else {
				tracelog.WarningLogger.Printf("Error during loading delta map: '%v'. "+
					"Fallback to full scan delta backup\n", err)
			}
		}
		bh.curBackupInfo.name = bh.curBackupInfo.name + "_D_" + utility.StripWalFileName(bh.prevBackupInfo.name)
		tracelog.DebugLogger.Printf("Suffixing Backup name with Delta info: %s", bh.curBackupInfo.name)
	}
}

func (bh *BackupHandler) setupDTO(tarFileSets TarFileSets) (sentinelDto BackupSentinelDto) {
	var tablespaceSpec *TablespaceSpec
	if !bh.workers.bundle.TablespaceSpec.empty() {
		tablespaceSpec = &bh.workers.bundle.TablespaceSpec
	}
	sentinelDto = NewBackupSentinelDto(bh, tablespaceSpec, tarFileSets)
	sentinelDto.setFiles(bh.workers.bundle.GetFiles())
	return sentinelDto
}

func (bh *BackupHandler) markBackups(folder storage.Folder, sentinelDto BackupSentinelDto) {
	// If pushing permanent delta backup, mark all previous backups permanent
	// Do this before uploading current meta to ensure that backups are marked in increasing order
	if bh.arguments.isPermanent && sentinelDto.IsIncremental() {
		markBackupHandler := internal.NewBackupMarkHandler(NewGenericMetaInteractor(), folder)
		markBackupHandler.MarkBackup(bh.prevBackupInfo.name, true)
	}
}

func (bh *BackupHandler) uploadBackup() TarFileSets {
	bundle := bh.workers.bundle
	// Start a new tar bundle, walk the pgDataDirectory and upload everything there.
	tracelog.InfoLogger.Println("Starting a new tar bundle")
	err := bundle.StartQueue(internal.NewStorageTarBallMaker(bh.curBackupInfo.name, bh.workers.uploader.Uploader))
	tracelog.ErrorLogger.FatalOnError(err)

	tarBallComposerMaker, err := NewTarBallComposerMaker(bh.arguments.tarBallComposerType, bh.workers.conn,
		NewTarBallFilePackerOptions(bh.arguments.verifyPageChecksums, bh.arguments.storeAllCorruptBlocks))
	tracelog.ErrorLogger.FatalOnError(err)

	err = bundle.SetupComposer(tarBallComposerMaker)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Walking ...")
	err = filepath.Walk(bh.pgInfo.pgDataDirectory, bundle.HandleWalkedFSObject)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Packing ...")
	tarFileSets, err := bundle.PackTarballs()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.DebugLogger.Println("Finishing queue ...")
	err = bundle.FinishQueue()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.DebugLogger.Println("Uploading pg_control ...")
	err = bundle.UploadPgControl(bh.workers.uploader.Compressor.FileExtension())
	tracelog.ErrorLogger.FatalOnError(err)

	// Stops backup and write/upload postgres `backup_label` and `tablespace_map` Files
	tracelog.DebugLogger.Println("Stop backup and upload backup_label and tablespace_map")
	labelFilesTarBallName, labelFilesList, finishLsn, err := bundle.uploadLabelFiles(bh.workers.conn)
	tracelog.ErrorLogger.FatalOnError(err)
	bh.curBackupInfo.endLSN = finishLsn
	bh.curBackupInfo.uncompressedSize = atomic.LoadInt64(bundle.TarBallQueue.AllTarballsSize)
	bh.curBackupInfo.compressedSize, err = bh.workers.uploader.UploadedDataSize()
	tracelog.ErrorLogger.FatalOnError(err)
	bundle.TarBallComposer.AddLabelFiles(&tarFileSets, labelFilesTarBallName, labelFilesList)
	timelineChanged := bundle.checkTimelineChanged(bh.workers.conn)
	tracelog.DebugLogger.Printf("Labelfiles tarball name: %s", labelFilesTarBallName)
	tracelog.DebugLogger.Printf("Number of label files: %d", len(labelFilesList))
	tracelog.DebugLogger.Printf("Finish LSN: %d", bh.curBackupInfo.endLSN)
	tracelog.DebugLogger.Printf("Uncompressed size: %d", bh.curBackupInfo.uncompressedSize)
	tracelog.DebugLogger.Printf("Compressed size: %d", bh.curBackupInfo.compressedSize)

	// Wait for all uploads to finish.
	tracelog.DebugLogger.Println("Waiting for all uploads to finish")
	bh.workers.uploader.Finish()
	if bh.workers.uploader.Failed.Load().(bool) {
		tracelog.ErrorLogger.Fatalf("Uploading failed during '%s' backup.\n", bh.curBackupInfo.name)
	}
	if timelineChanged {
		tracelog.ErrorLogger.Fatalf("Cannot finish backup because of changed timeline.")
	}
	return tarFileSets
}

// HandleBackupPush handles the backup being read from Postgres or filesystem and being pushed to the repository
// TODO : unit tests
func (bh *BackupHandler) HandleBackupPush() {
	folder := bh.workers.uploader.UploadingFolder
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	tracelog.DebugLogger.Printf("Base backup folder: %s", baseBackupFolder)

	bh.curBackupInfo.startTime = utility.TimeNowCrossPlatformUTC()

	if bh.arguments.pgDataDirectory == "" {
		if bh.arguments.forceIncremental {
			tracelog.ErrorLogger.Println("Delta backup not available for remote backup.")
			tracelog.ErrorLogger.Fatal("To run delta backup, supply [db_directory].")
		}
		// If no arg is parsed, try to run remote backup using pglogrepl's BASE_BACKUP functionality
		tracelog.InfoLogger.Println("Running remote backup through Postgres connection.")
		tracelog.InfoLogger.Println("Features like delta backup are disabled, there might be a performance impact.")
		tracelog.InfoLogger.Println("To run with local backup functionalities, supply [db_directory].")
		if bh.pgInfo.pgVersion < 110000 && !bh.arguments.verifyPageChecksums {
			tracelog.InfoLogger.Println("VerifyPageChecksums=false is only supported for streaming backup since PG11")
			bh.arguments.verifyPageChecksums = true
		}
		bh.createAndPushRemoteBackup()
		return
	}

	if utility.ResolveSymlink(bh.arguments.pgDataDirectory) != bh.pgInfo.pgDataDirectory {
		tracelog.ErrorLogger.Panicf("Data directory read from Postgres (%s) is different then as parsed (%s).",
			bh.arguments.pgDataDirectory, bh.pgInfo.pgDataDirectory)
	}
	bh.checkPgVersionAndPgControl()

	if bh.arguments.isFullBackup {
		tracelog.InfoLogger.Println("Doing full backup.")
	} else {
		err := bh.configureDeltaBackup()
		tracelog.ErrorLogger.FatalOnError(err)
	}

	bh.createAndPushBackup()
}

func (bh *BackupHandler) createAndPushRemoteBackup() {
	var err error
	uploader := *bh.workers.uploader
	isSimpleComposerTarBallType = bh.arguments.tarBallComposerType == SimpleComposer
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	tracelog.DebugLogger.Printf("Uploading folder: %s", uploader.UploadingFolder)

	baseBackup := bh.runRemoteBackup()
	tracelog.InfoLogger.Println("Updating metadata")
	bh.curBackupInfo.startLSN = uint64(baseBackup.StartLSN)
	bh.curBackupInfo.endLSN = uint64(baseBackup.EndLSN)

	bh.curBackupInfo.uncompressedSize = baseBackup.UncompressedSize
	bh.curBackupInfo.compressedSize, err = bh.workers.uploader.UploadedDataSize()
	tracelog.ErrorLogger.FatalOnError(err)
	sentinelDto := NewBackupSentinelDto(bh, baseBackup.GetTablespaceSpec(), TarFileSets{})
	sentinelDto.Files = baseBackup.Files
	bh.curBackupInfo.name = baseBackup.BackupName()
	tracelog.InfoLogger.Println("Uploading metadata")
	bh.uploadMetadata(sentinelDto)
	// logging backup set name
	tracelog.InfoLogger.Printf("Wrote backup with name %s", bh.curBackupInfo.name)
}

func (bh *BackupHandler) uploadMetadata(sentinelDto BackupSentinelDto) {
	curBackupName := bh.curBackupInfo.name
	err := bh.uploadExtendedMetadata(sentinelDto)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload metadata file for backup: %s %v", curBackupName, err)
		tracelog.ErrorLogger.FatalError(err)
	}
	err = internal.UploadSentinel(bh.workers.uploader, sentinelDto, bh.curBackupInfo.name)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload sentinel file for backup: %s", curBackupName)
		tracelog.ErrorLogger.FatalError(err)
	}
}

// NewBackupHandler returns a backup handler object, which can handle the backup
func NewBackupHandler(arguments BackupArguments) (bh *BackupHandler, err error) {
	// RemoteBackup is triggered by not passing PGDATA to wal-g,
	// and version cannot be read easily using replication connection.
	// Retrieve both with this helper function which uses a temp connection to postgres.

	uploader, err := ConfigureWalUploader()
	if err != nil {
		return bh, err
	}
	pgInfo, err := getPgServerInfo()
	if err != nil {
		return bh, err
	}

	if arguments.pgDataDirectory != "" && arguments.pgDataDirectory != pgInfo.pgDataDirectory {
		warning := fmt.Sprintf("Data directory for postgres '%s' is not equal to backup-push argument '%s'",
			arguments.pgDataDirectory, pgInfo.pgDataDirectory)
		tracelog.WarningLogger.Println(warning)
	}

	bh = &BackupHandler{
		arguments: arguments,
		workers: BackupWorkers{
			uploader: uploader,
		},
		pgInfo: pgInfo,
	}

	return bh, err
}

func (bh *BackupHandler) runRemoteBackup() *StreamingBaseBackup {
	var diskLimit int32
	if viper.IsSet(internal.DiskRateLimitSetting) {
		// Note that BASE_BACKUP (pg protocol) allows to limit in kb/sec
		// Also note that the basebackup class  only enables this when set > 32kb/s
		diskLimit = int32(viper.GetInt64(internal.DiskRateLimitSetting)) / 1024
		if diskLimit > 32 {
			tracelog.InfoLogger.Printf("DiskIO limited to %d kb/s", diskLimit)
		}
	}
	// Connect to postgres and start/finish a nonexclusive backup.
	tracelog.DebugLogger.Println("Connecting to Postgres (replication connection)")
	conn, err := pgconn.Connect(context.Background(), "replication=yes")
	tracelog.ErrorLogger.FatalOnError(err)

	baseBackup := NewStreamingBaseBackup(bh.pgInfo.pgDataDirectory, viper.GetInt64(internal.TarSizeThresholdSetting), conn)
	tracelog.InfoLogger.Println("Starting remote backup")
	err = baseBackup.Start(bh.arguments.verifyPageChecksums, diskLimit)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Streaming remote backup")
	err = baseBackup.Upload(bh.workers.uploader)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Finishing backup")
	tracelog.InfoLogger.Println("If wal-g hangs during this step, please Postgres log file for details.")
	err = baseBackup.Finish()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.DebugLogger.Println("Closing Postgres connection (replication connection)")
	err = conn.Close(context.Background())
	tracelog.ErrorLogger.FatalOnError(err)
	return baseBackup
}

func getPgServerInfo() (pgInfo BackupPgInfo, err error) {
	// Creating a temporary connection to read slot info and wal_segment_size
	tracelog.DebugLogger.Println("Initializing tmp connection to read Postgres info")
	tmpConn, err := Connect()
	if err != nil {
		return pgInfo, err
	}

	queryRunner, err := NewPgQueryRunner(tmpConn)
	if err != nil {
		return pgInfo, err
	}

	pgInfo.pgDataDirectory, err = queryRunner.GetDataDir()
	if err != nil {
		return pgInfo, err
	}
	tracelog.DebugLogger.Printf("Datadir: %s", pgInfo.pgDataDirectory)

	err = queryRunner.getVersion()
	if err != nil {
		return pgInfo, err
	}
	pgInfo.pgVersion = queryRunner.Version
	tracelog.DebugLogger.Printf("Postgres version: %d", queryRunner.Version)

	err = queryRunner.getSystemIdentifier()
	if err != nil {
		return pgInfo, err
	}
	pgInfo.systemIdentifier = queryRunner.SystemIdentifier
	tracelog.DebugLogger.Printf("Postgres SystemIdentifier: %d", queryRunner.Version)

	err = tmpConn.Close()
	if err != nil {
		return pgInfo, err
	}

	return pgInfo, err
}

func (bh *BackupHandler) configureDeltaBackup() (err error) {
	maxDeltas, fromFull := getDeltaConfig()
	if maxDeltas == 0 {
		return nil
	}

	folder := bh.workers.uploader.UploadingFolder
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	previousBackupName, err := bh.arguments.deltaBaseSelector.Select(folder)
	if err != nil {
		if _, ok := err.(internal.NoBackupsFoundError); ok {
			tracelog.InfoLogger.Println("Couldn't find previous backup. Doing full backup.")
			return nil
		}
		return err
	}

	previousBackup := NewBackup(baseBackupFolder, previousBackupName)
	prevBackupSentinelDto, err := previousBackup.GetSentinel()
	tracelog.ErrorLogger.FatalOnError(err)

	if prevBackupSentinelDto.IncrementCount != nil {
		bh.curBackupInfo.incrementCount = *prevBackupSentinelDto.IncrementCount + 1
	} else {
		bh.curBackupInfo.incrementCount = 1
	}

	if bh.curBackupInfo.incrementCount > maxDeltas {
		tracelog.InfoLogger.Println("Reached max delta steps. Doing full backup.")
		return nil
	}

	if prevBackupSentinelDto.BackupStartLSN == nil {
		tracelog.InfoLogger.Println("LATEST backup was made without support for delta feature. " +
			"Fallback to full backup with LSN marker for future deltas.")
		return nil
	}

	previousBackupMeta, err := previousBackup.FetchMeta()
	if err != nil {
		tracelog.InfoLogger.Printf(
			"Failed to get previous backup metadata: %s. Doing full backup.\n", err.Error())
		return nil
	}

	if !bh.arguments.isPermanent && !fromFull && previousBackupMeta.IsPermanent {
		tracelog.InfoLogger.Println("Can't do a delta backup from permanent backup. Doing full backup.")
		return nil
	}

	if fromFull {
		tracelog.InfoLogger.Println("Delta will be made from full backup.")

		if prevBackupSentinelDto.IncrementFullName != nil {
			previousBackupName = *prevBackupSentinelDto.IncrementFullName
		}

		previousBackup := NewBackup(baseBackupFolder, previousBackupName)
		prevBackupSentinelDto, err = previousBackup.GetSentinel()
		if err != nil {
			return err
		}
	}
	tracelog.InfoLogger.Printf("Delta backup from %v with LSN %x.\n", previousBackupName,
		*prevBackupSentinelDto.BackupStartLSN)
	bh.prevBackupInfo.name = previousBackupName
	bh.prevBackupInfo.sentinelDto = prevBackupSentinelDto
	return nil
}

// TODO : unit tests
func (bh *BackupHandler) uploadExtendedMetadata(sentinelDto BackupSentinelDto) (err error) {
	meta := NewExtendedMetadataDto(bh.arguments.isPermanent, bh.pgInfo.pgDataDirectory,
		bh.curBackupInfo.startTime, sentinelDto)

	metaFile := storage.JoinPath(bh.curBackupInfo.name, utility.MetadataFileName)
	dtoBody, err := json.Marshal(meta)
	if err != nil {
		return internal.NewSentinelMarshallingError(metaFile, err)
	}
	tracelog.DebugLogger.Printf("Uploading metadata file (%s):\n%s", metaFile, dtoBody)
	return bh.workers.uploader.Upload(metaFile, bytes.NewReader(dtoBody))
}

func (bh *BackupHandler) checkPgVersionAndPgControl() {
	_, err := ioutil.ReadFile(filepath.Join(bh.pgInfo.pgDataDirectory, PgControlPath))
	tracelog.ErrorLogger.FatalfOnError(
		"It looks like you are trying to backup not pg_data. PgControl file not found: %v\n", err)
	_, err = ioutil.ReadFile(filepath.Join(bh.pgInfo.pgDataDirectory, "PG_VERSION"))
	tracelog.ErrorLogger.FatalfOnError(
		"It looks like you are trying to backup not pg_data. PG_VERSION file not found: %v\n", err)
}
