package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx"
	"os"
	"time"

	"github.com/spf13/viper"

	"github.com/pkg/errors"
	"github.com/tinsane/storages/storage"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type SentinelMarshallingError struct {
	error
}

func NewSentinelMarshallingError(sentinelName string, err error) SentinelMarshallingError {
	return SentinelMarshallingError{errors.Wrapf(err, "Failed to marshall sentinel file: '%s'", sentinelName)}
}

func (err SentinelMarshallingError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
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

func getPreviousBackup(folder storage.Folder) (*Backup, error) {
	maxDeltas, fromFull := getDeltaConfig()
	basebackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	name, err := GetLatestBackupName(folder)
	if err != nil {
		if _, ok := err.(NoBackupsFoundError); ok {
			tracelog.InfoLogger.Println("Couldn't find previous backup. Doing full backup.")
			return nil, nil
		}
		return nil, err
	}
	backup := NewBackup(basebackupFolder, name)
	sentinelDto, err := backup.GetSentinel()
	if err != nil {
		return nil, err
	}
	incrementCount := 1
	if sentinelDto.IsIncremental() {
		incrementCount = *sentinelDto.GetIncrementCount() + 1
	}

	if incrementCount > maxDeltas {
		tracelog.InfoLogger.Println("Reached max delta steps. Doing full backup.")
		return nil, nil
	}
	if fromFull {
		tracelog.InfoLogger.Println("Delta will be made from full backup.")
		name = *sentinelDto.GetIncrementFullName()
		backup = NewBackup(basebackupFolder, name)
	}

	_, err = backup.GetMeta()
	if err != nil {
		if _, ok := err.(storage.ObjectNotFoundError); ok {
			tracelog.InfoLogger.Println("Increment from backup was made without support for delta feature. Fallback to full backup with LSN marker for future deltas.")
			return nil, nil
		}
		return nil, err
	}

	return backup, nil
}

// TODO : unit tests
// HandleBackupPush is invoked to perform a wal-g backup-push
func HandleBackupPush(uploader *Uploader, archiveDirectory string, isPermanent bool, isFullBackup bool) {
	if viper.GetInt(DeltaMaxStepsSetting) <= 0 {
		isFullBackup = true
	}

	folder := uploader.UploadingFolder
	var previousBackup *Backup
	var err error
	if isFullBackup {
		tracelog.InfoLogger.Println("Doing full backup.")
	} else {
		previousBackup, err = getPreviousBackup(folder)
		tracelog.ErrorLogger.FatalOnError(err)
	}
	// TODO: AB: this subfolder switch look ugly. I think typed storage folders could be better (i.e. interface BasebackupFolder, WalFolder etc)
	uploader.UploadingFolder = folder.GetSubFolder(utility.BaseBackupPath)

	backupStartTimeLocal := utility.TimeNowCrossPlatformLocal()
	backupStartTime := backupStartTimeLocal.In(time.UTC)
	// TODO : replace
	backupName, meta, sentinelDto, err := doBackup(previousBackup, uploader, folder, backupStartTimeLocal, archiveDirectory)
	tracelog.ErrorLogger.FatalOnError(err)
	// If pushing permanent delta backup, mark all previous backups permanent
	// Do this before uploading current meta to ensure that backups are marked in increasing order
	if isPermanent && previousBackup != nil {
		MarkBackup(uploader, folder, previousBackup.Name, true)
	}
	hostname, err := os.Hostname()
	tracelog.ErrorLogger.PrintOnError(errors.Wrapf(err, "failed to get hostname"))
	meta.SetCommonMetadata(CommonMetadataDto{
		backupStartTime,
		utility.TimeNowCrossPlatformUTC(),
		"%Y-%m-%dT%H:%M:%S.%fZ",
		hostname,
		isPermanent,
	})
	err = UploadMetadata(uploader, meta, backupName)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload metadata file for backup: %s", backupName)
		tracelog.ErrorLogger.FatalError(err)
	}
	err = UploadSentinel(uploader, sentinelDto, backupName)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload sentinel file for backup: %s", backupName)
		tracelog.ErrorLogger.FatalError(err)
	}
	// logging backup set name
	tracelog.InfoLogger.Println("Wrote backup with name " + backupName)
}

func doBackup(previousBackup *Backup, uploader *Uploader, folder storage.Folder, backupStartTime time.Time, archiveDirectory string) (
	backupName string, metadata MetadataDto, sentinelDto SentinelDto, err error) {
	if previousBackup != nil {
		previousMeta, err := previousBackup.GetMeta()
		if err != nil {
			return "", nil, nil, err
		}
		tracelog.InfoLogger.Printf("Delta backup from %v with LSN %x. \n", previousBackup.Name, previousMeta.StartLsn)
	}

	// Connect to postgres and start/finish a nonexclusive backup.
	conn, err := Connect()
	if err != nil {
		return "", nil, nil, err
	}
	backupName, backupStartLSN, pgVersion, dataDir, isReplica, timeline, err := StartBackup(conn, backupStartTime.String())
	if err != nil {
		return "", nil, nil, err
	}
	if previousBackup != nil {
		backupName = backupName + "_D_" + utility.StripWalFileName(previousBackup.Name)
	}
	bundle, err := createBundle(previousBackup, backupName, archiveDirectory, timeline, uploader, folder, backupStartLSN)
	if err != nil {
		return "", nil, nil, err
	}
	// Start a new tar bundle, walk the archiveDirectory and upload everything there.
	err = bundle.uploadBackup()
	if err != nil {
		return "", nil, nil, err
	}
	// Stops backup and write/upload postgres `backup_label` and `tablespace_map` Files
	label, offsetMap, finishLsn, err := StopBackup(conn)
	if err != nil {
		return "", nil, nil, err
	}
	if pgVersion >= 90600 {
		err = bundle.UploadLabelFiles(label, offsetMap)
		tracelog.ErrorLogger.FatalOnError(err)
	}
	timelineChanged := checkTimelineChanged(conn, isReplica, timeline)
	// Wait for all uploads to finish
	uploader.finish()
	if uploader.Failed.Load().(bool) {
		return "", nil, nil, errors.Errorf("Uploading failed during '%s' backup.\n", backupName)
	}
	if timelineChanged {
		return "", nil, nil, errors.Errorf("Cannot finish backup because of changed timeline.")
	}
	meta := ExtendedMetadataDto{
		DataDir:   dataDir,
		PgVersion: pgVersion,
		StartLsn:  backupStartLSN,
		FinishLsn: finishLsn,
	}
	return backupName, &meta, createSentinel(previousBackup, bundle.GetFiles()), nil
}

func createBundle(previousBackup *Backup, backupName string, archiveDirectory string, timeline uint32, uploader *Uploader, folder storage.Folder, backupStartLSN uint64) (*Bundle, error) {
	archiveDirectory = utility.ResolveSymlink(archiveDirectory)
	uploadPooler, err := NewUploadPooler(NewStorageTarBallMaker(backupName, uploader), viper.GetInt64(TarSizeThresholdSetting), ConfigureCrypter())
	if err != nil {
		return nil, err
	}
	var incrementFromLsn *uint64 = nil
	var previousBackupSentinelDto BackupSentinelDto
	if previousBackup != nil {
		incrementFromLsn = &previousBackup.Metadata.StartLsn
		previousBackupSentinelDto = *previousBackup.SentinelDto
	}
	bundle := NewBundle(uploadPooler, archiveDirectory, incrementFromLsn, previousBackupSentinelDto.Files, timeline)
	if previousBackup != nil && uploader.getUseWalDelta() {
		err := bundle.DownloadDeltaMap(folder.GetSubFolder(utility.WalPath), backupStartLSN)
		if err == nil {
			tracelog.InfoLogger.Println("Successfully loaded delta map, delta backup will be made with provided delta map")
		} else {
			tracelog.WarningLogger.Printf("Error during loading delta map: '%v'. Fallback to full scan delta backup\n", err)
		}
	}
	return bundle, nil
}

func createSentinel(previousBackup *Backup, files BackupFileList) *BackupSentinelDto {
	currentBackupSentinelDto := &BackupSentinelDto{
		UserData: GetSentinelUserData(),
		Files:    files,
	}
	if previousBackup == nil {
		return currentBackupSentinelDto
	}
	currentBackupSentinelDto.IncrementFromLSN = &previousBackup.Metadata.StartLsn
	previousBackupSentinelDto := previousBackup.SentinelDto
	currentBackupSentinelDto.IncrementFrom = &previousBackup.Name
	if previousBackupSentinelDto.IsIncremental() {
		currentBackupSentinelDto.IncrementFullName = previousBackupSentinelDto.IncrementFullName
	} else {
		currentBackupSentinelDto.IncrementFullName = &previousBackup.Name
	}
	currentIncrementCount := 1
	if previousBackupSentinelDto.IncrementCount != nil {
		currentIncrementCount = *previousBackupSentinelDto.IncrementCount + 1
	}
	currentBackupSentinelDto.IncrementCount = &currentIncrementCount
	return currentBackupSentinelDto
}

// TODO : unit tests
func UploadMetadata(uploader *Uploader, meta MetadataDto, backupName string) error {
	// BackupSentinelDto struct allows nil field for backward compatibility
	// We do not expect here nil dto since it is new dto to upload
	return UploadDto(uploader, meta, storage.JoinPath(backupName, utility.MetadataFileName))
}

// TODO : unit tests
func UploadSentinel(uploader *Uploader, sentinelDto interface{}, backupName string) error {
	return UploadDto(uploader, sentinelDto, backupName+utility.SentinelSuffix)
}

func UploadDto(uploader *Uploader, dto interface{}, dtoName string) error {
	dtoBody, err := json.Marshal(dto)
	if err != nil {
		return NewSentinelMarshallingError(dtoName, err)
	}
	return uploader.Upload(dtoName, bytes.NewReader(dtoBody))
}

// TODO : unit tests
// StartBackup starts a non-exclusive base backup immediately. When finishing the backup,
// `backup_label` and `tablespace_map` contents are not immediately written to
// a file but returned instead. Returns empty string and an error if backup
// fails.
func StartBackup(conn *pgx.Conn, backup string) (backupName string, lsn uint64, version int, dataDir string, isReplica bool, timeline uint32, err error) {
	var name, lsnStr string
	queryRunner, err := NewPgQueryRunner(conn)
	if err != nil {
		return "", 0, queryRunner.Version, "", false, 0, errors.Wrap(err, "StartBackup: Failed to build query runner.")
	}
	name, lsnStr, isReplica, dataDir, err = queryRunner.StartBackup(backup)

	if err != nil {
		return "", 0, queryRunner.Version, "", false, 0, err
	}
	lsn, err = pgx.ParseLSN(lsnStr)

	if isReplica {
		name, timeline, err = getWalFilename(lsn, conn)
		if err != nil {
			return "", 0, queryRunner.Version, "", false, 0, err
		}
	} else {
		timeline, err = readTimeline(conn)
		if err != nil {
			tracelog.WarningLogger.Printf("Couldn't get current timeline because of error: '%v'\n", err)
		}
	}
	return "base_" + name, lsn, queryRunner.Version, dataDir, isReplica, timeline, nil

}

func StopBackup(conn *pgx.Conn) (label, offsetMap string, lsn uint64, err error) {
	queryRunner, err := NewPgQueryRunner(conn)
	if err != nil {
		return "", "", 0, errors.Wrap(err, "UploadLabelFiles: Failed to build query runner.")
	}
	var lsnStr string
	label, offsetMap, lsnStr, err = queryRunner.StopBackup()
	if err != nil {
		return "", "", 0, errors.Wrap(err, "UploadLabelFiles: failed to stop backup")
	}

	lsn, err = pgx.ParseLSN(lsnStr)
	if err != nil {
		return "", "", 0, errors.Wrap(err, "UploadLabelFiles: failed to parse finish LSN")
	}
	return
}

// TODO : unit tests
// checkTimelineChanged compares timelines of pg_backup_start() and pg_backup_stop()
func checkTimelineChanged(conn *pgx.Conn, isReplica bool, oldTimeline uint32) bool {
	if isReplica {
		timeline, err := readTimeline(conn)
		if err != nil {
			tracelog.ErrorLogger.Printf("Unable to check timeline change. Sentinel for the backup will not be uploaded.")
			return true
		}

		// Per discussion in
		// https://www.postgresql.org/message-id/flat/BF2AD4A8-E7F5-486F-92C8-A6959040DEB6%40yandex-team.ru#BF2AD4A8-E7F5-486F-92C8-A6959040DEB6@yandex-team.ru
		// Following check is the very pessimistic approach on replica backup invalidation
		if timeline != oldTimeline {
			tracelog.ErrorLogger.Printf("Timeline has changed since backup start. Sentinel for the backup will not be uploaded.")
			return true
		}
	}
	return false
}
