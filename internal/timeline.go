package internal

import (
	"fmt"
	"github.com/wal-g/wal-g/utility"
	"regexp"
	"strconv"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

const PatternTLILogSegNo = "[0-9A-F]{24}"

var regexpWalFileName = regexp.MustCompile(PatternTLILogSegNo)

const maxCountOfLSN = 2

type BytesPerWalSegmentError struct {
	error
}

func newBytesPerWalSegmentError() BytesPerWalSegmentError {
	return BytesPerWalSegmentError{errors.New("bytes_per_wal_segment of the server does not match expected value, you may need to set WALG_PG_WAL_SIZE")}
}

func (err BytesPerWalSegmentError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type IncorrectLogSegNoError struct {
	error
}

func newIncorrectLogSegNoError(name string) IncorrectLogSegNoError {
	return IncorrectLogSegNoError{errors.Errorf("Incorrect logSegNoLo in WAL file name: %s", name)}
}

func (err IncorrectLogSegNoError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type IncorrectBackupNameError struct {
	error
}

func newIncorrectBackupNameError(name string) IncorrectBackupNameError {
	return IncorrectBackupNameError{errors.Errorf("Incorrect backup name: %s", name)}
}

func (err IncorrectBackupNameError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

func readTimeline(conn *pgx.Conn) (timeline uint32, err error) {
	var bytesPerWalSegment uint32

	// TODO: Check if this logic can be moved to queryRunner or abstracted away somehow
	err = conn.QueryRow("select timeline_id, bytes_per_wal_segment from pg_control_checkpoint(), pg_control_init()").Scan(&timeline, &bytesPerWalSegment)
	if err == nil && uint64(bytesPerWalSegment) != WalSegmentSize {
		return 0, newBytesPerWalSegmentError()
	}
	return
}

const (
	sizeofInt32bits = sizeofInt32 * 8
	hexadecimal     = 16
)

var (
	// WalSegmentSize is the size of one WAL file
	WalSegmentSize        = uint64(16 * 1024 * 1024)
	xLogSegmentsPerXLogId = 0x100000000 / WalSegmentSize // xlog_internal.h line 101
)

const (
	walFileFormat        = "%08X%08X%08X" // xlog_internal.h line 155
	walHistoryFileFormat = "%08X.history"
)

func SetWalSize(sizeMb uint64) {
	WalSegmentSize = sizeMb * 1024 * 1024
	xLogSegmentsPerXLogId = 0x100000000 / WalSegmentSize
}

// getWalFilename formats WAL file name using PostgreSQL connection. Essentially reads timeline of the server.
func getWalFilename(lsn uint64, conn *pgx.Conn) (walFilename string, timeline uint32, err error) {
	timeline, err = readTimeline(conn)
	if err != nil {
		return "", 0, err
	}

	walSegmentNo := newWalSegmentNo(lsn - 1)

	return walSegmentNo.getFilename(timeline), timeline, nil
}

func formatWALFileName(timeline uint32, logSegNo uint64) string {
	return fmt.Sprintf(walFileFormat, timeline, logSegNo/xLogSegmentsPerXLogId, logSegNo%xLogSegmentsPerXLogId)
}

// ParseWALFilename extracts numeric parts from WAL file name
func ParseWALFilename(name string) (timelineID uint32, logSegNo uint64, err error) {
	if len(name) != 24 {
		err = newNotWalFilenameError(name)
		return
	}
	timelineID64, err0 := strconv.ParseUint(name[0:8], 0x10, sizeofInt32bits)
	timelineID = uint32(timelineID64)
	if err0 != nil {
		err = err0
		return
	}
	logSegNoHi, err0 := strconv.ParseUint(name[8:16], 0x10, sizeofInt32bits)
	if err0 != nil {
		err = err0
		return
	}
	logSegNoLo, err0 := strconv.ParseUint(name[16:24], 0x10, sizeofInt32bits)
	if err0 != nil {
		err = err0
		return
	}
	if logSegNoLo >= xLogSegmentsPerXLogId {
		err = newIncorrectLogSegNoError(name)
		return
	}

	logSegNo = logSegNoHi*xLogSegmentsPerXLogId + logSegNoLo
	return
}

func TryFetchLogSegNo(objectName string) (uint64, bool) {
	foundLsn := regexpWalFileName.FindAllString(objectName, maxCountOfLSN)
	if len(foundLsn) > 0 {
		// Remove timeline id: Timeline is resetted during pg_upgrade. This still can cause problems
		// with overlapping WALs, but at least will prevent us from deleting new backups
		_, logSegNo, err := ParseWALFilename(foundLsn[0])

		if err != nil {
			return 0, false
		}
		return logSegNo, true
	}
	return 0, false
}

func isWalFilename(filename string) bool {
	_, _, err := ParseWALFilename(filename)
	return err == nil
}

func ParseTimelineFromBackupName(backupName string) (uint32, error) {
	if len(backupName) == 0 {
		return 0, newIncorrectBackupNameError(backupName)
	}
	prefixLength := len(utility.BackupNamePrefix)
	timelineId64, err := strconv.ParseUint(backupName[prefixLength:prefixLength+8], hexadecimal, sizeofInt32bits)
	if err != nil {
		return 0, err
	}
	return uint32(timelineId64), nil
}

// GetNextWalFilename computes name of next WAL segment
func GetNextWalFilename(name string) (string, error) {
	timelineId, logSegNo, err := ParseWALFilename(name)
	if err != nil {
		return "", err
	}
	logSegNo++
	return formatWALFileName(uint32(timelineId), logSegNo), nil
}

func shouldPrefault(name string) (lsn uint64, shouldPrefault bool, timelineId uint32, err error) {
	timelineId, logSegNo, err := ParseWALFilename(name)
	if err != nil {
		return 0, false, 0, err
	}
	if logSegNo%WalFileInDelta != 0 {
		return 0, false, 0, nil
	}
	logSegNo += WalFileInDelta

	return logSegNo * WalSegmentSize, true, timelineId, nil
}
