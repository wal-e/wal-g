package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/crypto/awskms"
	"github.com/wal-g/wal-g/internal/crypto/openpgp"
	"golang.org/x/time/rate"
)

const (
	DefaultDataBurstRateLimit = 8 * int64(DatabasePageSize)
	DefaultDataFolderPath     = "/tmp"
	WaleFileHost              = "file://localhost"
)

const MinAllowedConcurrency = 1

var DeprecatedExternalGpgMessage = fmt.Sprintf(
	`You are using deprecated functionality that uses an external gpg library.
It will be removed in next major version.
Please set GPG key using environment variables %s or %s.
`, PgpKeySetting, PgpKeyPathSetting)

type UnconfiguredStorageError struct {
	error
}

func newUnconfiguredStorageError(storagePrefixVariants []string) UnconfiguredStorageError {
	return UnconfiguredStorageError{errors.Errorf("No storage is configured now, please set one of following settings: %v", storagePrefixVariants)}
}

func (err UnconfiguredStorageError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type UnknownCompressionMethodError struct {
	error
}

func newUnknownCompressionMethodError() UnknownCompressionMethodError {
	return UnknownCompressionMethodError{errors.Errorf("Unknown compression method, supported methods are: %v", compression.CompressingAlgorithms)}
}

func (err UnknownCompressionMethodError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type UnsetRequiredSettingError struct {
	error
}

func NewUnsetRequiredSettingError(settingName string) UnsetRequiredSettingError {
	return UnsetRequiredSettingError{errors.Errorf("%v is required to be set, but it isn't", settingName)}
}

func (err UnsetRequiredSettingError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type InvalidConcurrencyValueError struct {
	error
}

func newInvalidConcurrencyValueError(concurrencyType string, value int) InvalidConcurrencyValueError {
	return InvalidConcurrencyValueError{errors.Errorf("%v value is expected to be positive but is: %v", concurrencyType, value)}
}

func (err InvalidConcurrencyValueError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type UnmarshallingError struct {
	error
}

func newUnmarshallingError(subject string, err error) UnmarshallingError {
	return UnmarshallingError{errors.Errorf("Failed to unmarshal %s: %v", subject, err)}
}

func (err UnmarshallingError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TODO : unit tests
func configureLimiters() {
	if viper.IsSet(DiskRateLimitSetting) {
		diskLimit := viper.GetInt64(DiskRateLimitSetting)
		DiskLimiter = rate.NewLimiter(rate.Limit(diskLimit), int(diskLimit+DefaultDataBurstRateLimit)) // Add 8 pages to possible bursts
	}

	if viper.IsSet(NetworkRateLimitSetting) {
		netLimit := viper.GetInt64(NetworkRateLimitSetting)
		NetworkLimiter = rate.NewLimiter(rate.Limit(netLimit), int(netLimit+DefaultDataBurstRateLimit)) // Add 8 pages to possible bursts
	}
}

// TODO : unit tests
func ConfigureFolder() (storage.Folder, error) {
	skippedPrefixes := make([]string, 0)
	for _, adapter := range StorageAdapters {
		prefix, ok := getWaleCompatibleSetting(adapter.prefixName)
		if !ok {
			skippedPrefixes = append(skippedPrefixes, "WALG_"+adapter.prefixName)
			continue
		}
		if adapter.prefixPreprocessor != nil {
			prefix = adapter.prefixPreprocessor(prefix)
		}

		settings, err := adapter.loadSettings()
		if err != nil {
			return nil, err
		}
		return adapter.configureFolder(prefix, settings)
	}
	return nil, newUnconfiguredStorageError(skippedPrefixes)
}

func getWalFolderPath() string {
	if !viper.IsSet(PgDataSetting) {
		return DefaultDataFolderPath
	}
	pgdata := viper.GetString(PgDataSetting)
	dataFolderPath := filepath.Join(pgdata, "pg_wal")
	if _, err := os.Stat(dataFolderPath); err == nil {
		return dataFolderPath
	}

	dataFolderPath = filepath.Join(pgdata, "pg_xlog")
	if _, err := os.Stat(dataFolderPath); err == nil {
		return dataFolderPath
	}

	return DefaultDataFolderPath
}

func GetDataFolderPath() string {
	return filepath.Join(getWalFolderPath(), "walg_data")
}

// TODO : unit tests
func configureWalDeltaUsage() (useWalDelta bool, deltaDataFolder DataFolder, err error) {
	useWalDelta = viper.GetBool(UseWalDeltaSetting)
	if !useWalDelta {
		return
	}
	dataFolderPath := GetDataFolderPath()
	deltaDataFolder, err = newDiskDataFolder(dataFolderPath)
	if err != nil {
		useWalDelta = false
		tracelog.WarningLogger.Printf("can't use wal delta feature because can't open delta data folder '%s'"+
			" due to error: '%v'\n", dataFolderPath, err)
		err = nil
	}
	return
}

// TODO : unit tests
func configureCompressor() (compression.Compressor, error) {
	compressionMethod := viper.GetString(CompressionMethodSetting)
	if _, ok := compression.Compressors[compressionMethod]; !ok {
		return nil, newUnknownCompressionMethodError()
	}
	return compression.Compressors[compressionMethod], nil
}

func ConfigureLogging() error {
	if viper.IsSet(LogLevelSetting) {
		return tracelog.UpdateLogLevel(viper.GetString(LogLevelSetting))
	}
	return nil
}

func getArchiveDataFolderPath() string {
	return filepath.Join(GetDataFolderPath(), "walg_archive_status")
}

// TODO : unit tests
func ConfigureArchiveStatusManager() (DataFolder, error) {
	return newDiskDataFolder(getArchiveDataFolderPath())
}

// ConfigureUploader connects to storage and creates an uploader. It makes sure
// that a valid session has started; if invalid, returns AWS error
// and `<nil>` values.
func ConfigureUploader() (uploader *Uploader, err error) {
	uploader, err = ConfigureUploaderWithoutCompressMethod()
	if err != nil {
		return nil, err
	}

	folder := uploader.UploadingFolder

	compressor, err := configureCompressor()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure compression")
	}

	uploader = NewUploader(compressor, folder)
	return uploader, err
}

// ConfigureWalUploader connects to storage and creates an uploader. It makes sure
// that a valid session has started; if invalid, returns AWS error
// and `<nil>` values.
func ConfigureWalUploader() (uploader *WalUploader, err error) {
	uploader, err = ConfigureWalUploaderWithoutCompressMethod()
	if err != nil {
		return nil, err
	}

	folder := uploader.UploadingFolder
	deltaFileManager := uploader.DeltaFileManager

	compressor, err := configureCompressor()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure compression")
	}

	uploader = NewWalUploader(compressor, folder, deltaFileManager)
	return uploader, err
}

func ConfigureUploaderWithoutCompressMethod() (uploader *Uploader, err error) {
	folder, err := ConfigureFolder()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure folder")
	}

	uploader = NewUploader(nil, folder)
	return uploader, err
}

func ConfigureWalUploaderWithoutCompressMethod() (uploader *WalUploader, err error) {
	folder, err := ConfigureFolder()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure folder")
	}

	useWalDelta, deltaDataFolder, err := configureWalDeltaUsage()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure WAL Delta usage")
	}

	var deltaFileManager *DeltaFileManager = nil
	if useWalDelta {
		deltaFileManager = NewDeltaFileManager(deltaDataFolder)
	}

	uploader = NewWalUploader(nil, folder, deltaFileManager)
	return uploader, err
}

// ConfigureCrypter uses environment variables to create and configure a crypter.
// In case no configuration in environment variables found, return `<nil>` value.
func ConfigureCrypter() crypto.Crypter {
	loadPassphrase := func() (string, bool) {
		return GetSetting(PgpKeyPassphraseSetting)
	}

	// key can be either private (for download) or public (for upload)
	if viper.IsSet(PgpKeySetting) {
		return openpgp.CrypterFromKey(viper.GetString(PgpKeySetting), loadPassphrase)
	}

	// key can be either private (for download) or public (for upload)
	if viper.IsSet(PgpKeyPathSetting) {
		return openpgp.CrypterFromKeyPath(viper.GetString(PgpKeyPathSetting), loadPassphrase)
	}

	if keyRingID, ok := getWaleCompatibleSetting(GpgKeyIDSetting); ok {
		tracelog.WarningLogger.Printf(DeprecatedExternalGpgMessage)
		return openpgp.CrypterFromKeyRingID(keyRingID, loadPassphrase)
	}

	if viper.IsSet(CseKmsIDSetting) {
		return awskms.CrypterFromKeyID(viper.GetString(CseKmsIDSetting), viper.GetString(CseKmsRegionSetting))
	}

	if crypter := configureLibsodiumCrypter(); crypter != nil {
		return crypter
	}

	return nil
}

func getMaxDownloadConcurrency() (int, error) {
	return GetMaxConcurrency(DownloadConcurrencySetting)
}

func getMaxUploadConcurrency() (int, error) {
	return GetMaxConcurrency(UploadConcurrencySetting)
}

// This setting is intentionally undocumented in README. Effectively, this configures how many prepared tar Files there
// may be in uploading state during backup-push.
func getMaxUploadQueue() (int, error) {
	return GetMaxConcurrency(UploadQueueSetting)
}

func getMaxUploadDiskConcurrency() (int, error) {
	return GetMaxConcurrency(UploadDiskConcurrencySetting)
}

func GetMaxConcurrency(concurrencyType string) (int, error) {
	concurrency := viper.GetInt(concurrencyType)

	if concurrency < MinAllowedConcurrency {
		return MinAllowedConcurrency, newInvalidConcurrencyValueError(concurrencyType, concurrency)
	}
	return concurrency, nil
}

func GetSentinelUserData() interface{} {
	dataStr, ok := GetSetting(SentinelUserDataSetting)
	if !ok || len(dataStr) == 0 {
		return nil
	}
	var out interface{}
	err := json.Unmarshal([]byte(dataStr), &out)
	if err != nil {
		tracelog.WarningLogger.PrintError(newUnmarshallingError(SentinelUserDataSetting, err))
		return dataStr
	}
	return out
}

func GetCommandSettingContext(ctx context.Context, variableName string) (*exec.Cmd, error) {
	dataStr, ok := GetSetting(variableName)
	if !ok {
		tracelog.InfoLogger.Printf("command %s not configured", variableName)
		return nil, errors.New("command not configured")
	}
	if len(dataStr) == 0 {
		tracelog.ErrorLogger.Print(variableName + " expected.")
		return nil, errors.New(variableName + " not configured")
	}
	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/sh"
	}
	cmd := exec.CommandContext(ctx, shell, "-c", dataStr)
	// do not shut up subcommands by default
	cmd.Stderr = os.Stderr
	return cmd, nil
}

func GetCommandSetting(variableName string) (*exec.Cmd, error) {
	return GetCommandSettingContext(context.Background(), variableName)
}

func GetOplogArchiveTimeout() (time.Duration, error) {
	oplogArchiveTimeoutStr, _ := GetSetting(OplogArchiveTimeoutSetting)
	oplogArchiveTimeout, err := strconv.Atoi(oplogArchiveTimeoutStr)
	if err != nil {
		return 0, fmt.Errorf("integer expected for %s setting but given '%s': %w", OplogArchiveTimeoutSetting, oplogArchiveTimeoutStr, err)
	}
	return time.Duration(oplogArchiveTimeout) * time.Second, nil
}

func GetOplogArchiveAfterSize() (int, error) {
	oplogArchiveAfterSizeStr, _ := GetSetting(OplogArchiveAfterSize)
	oplogArchiveAfterSize, err := strconv.Atoi(oplogArchiveAfterSizeStr)
	if err != nil {
		return 0, fmt.Errorf("integer expected for %s setting but given '%s': %w", OplogArchiveAfterSize, oplogArchiveAfterSizeStr, err)
	}
	return oplogArchiveAfterSize, nil
}

func GetLastWriteUpdateInterval() (time.Duration, error) {
	intervalStr, _ := GetSetting(MongoDBLastWriteUpdateSeconds)
	interval, err := strconv.Atoi(intervalStr)
	if err != nil {
		return 0, fmt.Errorf("integer(seconds) expected for %s setting but given '%s': %w", MongoDBLastWriteUpdateSeconds, intervalStr, err)
	}
	return time.Duration(interval) * time.Second, nil
}

func GetRequiredSetting(setting string) (string, error) {
	val, ok := GetSetting(setting)
	if !ok {
		return "", NewUnsetRequiredSettingError(setting)
	}
	return val, nil
}
