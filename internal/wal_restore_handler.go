package internal

import (
	"errors"
	"github.com/spf13/viper"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"os"
	"path/filepath"
	"sort"
)

// Types for sorting
type TimelineSlice []uint32

func (p TimelineSlice) Len() int { return len(p) }

// Because we need sorted slice in descending order
func (p TimelineSlice) Less(i, j int) bool { return p[i] > p[j] }
func (p TimelineSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type WalSegmentNumbers []WalSegmentNo

func (p WalSegmentNumbers) Len() int { return len(p) }

// Because we need sorted slice in descending order
func (p WalSegmentNumbers) Less(i, j int) bool { return p[i] > p[j] }
func (p WalSegmentNumbers) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// HandleWALRestore is invoked to perform wal-g wal-restore
func HandleWALRestore(externalFolder storage.Folder, localFolder storage.Folder) {
	externalHistoryRecs, err := getTimeLineHistoryRecords(1, externalFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get the external WAL history records %v\n", err)
	localHistoryRecs, err := getTimeLineHistoryRecords(1, localFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get the local WAL history records %v\n", err)

	lastCommonWALNo, err := findLastCommonSegmentNo(externalHistoryRecs, localHistoryRecs)
	tracelog.ErrorLogger.FatalfOnError("Failed to find last common segment number: %v\n", err)

	externalWALFolder := externalFolder.GetSubFolder(utility.WalPath)
	extFolderFilenames, err := getFolderFilenames(externalWALFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get the WAL external folder filenames %v\n", err)
	externalWALs := getSegmentsFromFiles(extFolderFilenames)
	externalWALsByTimelines := groupSegmentsByTimelines(externalWALs)

	walDirName, err := getWALDirName()
	tracelog.ErrorLogger.FatalfOnError("Failed to get the WAL directory name: %v\n", err)
	localWALFolder := localFolder.GetSubFolder(walDirName)

	filenamesToRestore, err := getFilenamesToRestore(externalWALsByTimelines, lastCommonWALNo)
	tracelog.ErrorLogger.FatalfOnError("Failed to get the needed to restore WAL filenames %v\n", err)

	if len(filenamesToRestore) == 0 {
		tracelog.InfoLogger.Print("No WAL files to restore")
		return
	}

	for _, walFilename := range filenamesToRestore {
		if err = DownloadWALFileTo(externalFolder, walFilename, localWALFolder.GetPath()); err != nil {
			tracelog.ErrorLogger.Printf("Failed to download WAL file %v\n", walFilename)
		} else {
			tracelog.InfoLogger.Printf("Successfully download WAL file %v\n", walFilename)
		}
	}
}

func getFilenamesToRestore(externalWALsByTimeline map[uint32]*WalSegmentsSequence,
	lastCommonSegmentNo WalSegmentNo) (filenames []string, err error) {
	// MaxUint64
	currentSegmentNo := uint64(1<<64 - 1)
	extSortedTimelines := getSortedTimelines(externalWALsByTimeline)

	for _, timeline := range extSortedTimelines {
		segmentNums := getSortWalSegmentNumbers(externalWALsByTimeline[timeline].walSegmentNumbers)
		for _, segmentNum := range segmentNums {
			if currentSegmentNo > uint64(segmentNum) {
				continue
			}
			filenames = append(filenames, segmentNum.getFilename(timeline))
			currentSegmentNo = uint64(segmentNum)
			if segmentNum <= lastCommonSegmentNo {
				return
			}
		}
	}

	return
}

func findLastCommonSegmentNo(external, local []*TimelineHistoryRecord) (WalSegmentNo, error) {
	var i int
	for i = range external {
		if external[i].lsn != local[i].lsn || external[i].timeline != local[i].timeline {
			break
		}
	}
	if i > 0 {
		return newWalSegmentNo(external[i-1].lsn), nil
	}
	return 0, errors.New("no common ancestors")
}

func getSortedTimelines(segmentsByTimeline map[uint32]*WalSegmentsSequence) (timelines []uint32) {
	for timeline := range segmentsByTimeline {
		timelines = append(timelines, timeline)
	}
	sort.Sort(TimelineSlice(timelines))
	return
}

func getSortWalSegmentNumbers(walSegmentNumbers map[WalSegmentNo]bool) (result []WalSegmentNo) {
	for walSegmentNo := range walSegmentNumbers {
		result = append(result, walSegmentNo)
	}
	sort.Sort(WalSegmentNumbers(result))
	return
}

func GetPgDataFolderPath() string {
	if !viper.IsSet(PgDataSetting) {
		return DefaultDataFolderPath
	}
	return viper.GetString(PgDataSetting)
}

func getWALDirName() (string, error) {
	pgData := viper.GetString(PgDataSetting)
	dataFolderPath := filepath.Join(pgData, "pg_wal")
	if _, err := os.Stat(dataFolderPath); err == nil {
		return "pg_wal", nil
	}

	dataFolderPath = filepath.Join(pgData, "pg_xlog")
	if _, err := os.Stat(dataFolderPath); err == nil {
		return "pg_xlog", nil
	}
	return "", errors.New("directory for WAL files doesn't exist in " + pgData)
}
