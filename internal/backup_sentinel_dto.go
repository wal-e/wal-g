package internal

import (
	"github.com/tinsane/tracelog"
	"time"
)

// BackupSentinelDto describes file structure of json sentinel
type BackupSentinelDto struct {
	IncrementFromLSN  *uint64 `json:"DeltaFromLSN,omitempty"`
	IncrementFrom     *string `json:"DeltaFrom,omitempty"`
	IncrementFullName *string `json:"DeltaFullName,omitempty"`
	IncrementCount    *int    `json:"DeltaCount,omitempty"`

	Files       BackupFileList      `json:"Files"`
	TarFileSets map[string][]string `json:"TarFileSets"`

	UserData interface{} `json:"UserData,omitempty"`
}

func (sentinel *BackupSentinelDto) GetIncrementCount() *int {
	return sentinel.IncrementCount
}

func (sentinel *BackupSentinelDto) GetIncrementFullName() *string {
	return sentinel.IncrementFullName
}

func (sentinel *BackupSentinelDto) GetIncrementFrom() *string {
	return sentinel.IncrementFrom
}

// TODO : unit tests
// TODO : get rid of panic here
// IsIncremental checks that sentinel represents delta backup
func (sentinel *BackupSentinelDto) IsIncremental() bool {
	// If we have increment base, we must have all the rest properties.
	if sentinel.IncrementFrom != nil {
		if sentinel.IncrementFromLSN == nil || sentinel.IncrementFullName == nil || sentinel.IncrementCount == nil {
			tracelog.ErrorLogger.Panic("Inconsistent BackupSentinelDto")
		}
	}
	return sentinel.IncrementFrom != nil
}

type SentinelDto interface {
	GetIncrementCount() *int
	GetIncrementFullName() *string
	GetIncrementFrom() *string
}

// Extended metadata should describe backup in more details, but be small enough to be downloaded often
type ExtendedMetadataDto struct {
	CommonMetadataDto
	DataDir   string `json:"data_dir"`
	PgVersion int    `json:"pg_version"`
	StartLsn  uint64 `json:"start_lsn"`
	FinishLsn uint64 `json:"finish_lsn"`
}

func (metadataDto *ExtendedMetadataDto) SetCommonMetadata(commonMetadataDto CommonMetadataDto) {
	metadataDto.CommonMetadataDto = commonMetadataDto
}

type MetadataDto interface {
	SetCommonMetadata(CommonMetadataDto)
}

type CommonMetadataDto struct {
	StartTime      time.Time `json:"start_time"`
	FinishTime     time.Time `json:"finish_time"`
	DatetimeFormat string    `json:"date_fmt"`
	Hostname       string    `json:"hostname"`
	IsPermanent    bool      `json:"is_permanent"`
}
