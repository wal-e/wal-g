package internal

import (
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
)

func getDeltaMap(folder storage.Folder, timeline uint32, firstUsedLSN, firstNotUsedLSN uint64) (PagedFileDeltaMap, error) {
	tracelog.InfoLogger.Printf("Timeline: %d, FirstUsedLsn: %d, FirstNotUsedLsn: %d\n", timeline, firstUsedLSN, firstNotUsedLSN)
	tracelog.InfoLogger.Printf("First WAL should participate in building delta map: %s", newWalSegmentNo(firstUsedLSN).getFilename(timeline))
	tracelog.InfoLogger.Printf("First WAL shouldn't participate in building delta map: %s", newWalSegmentNo(firstNotUsedLSN).getFilename(timeline))
	deltaMap := NewPagedFileDeltaMap()
	firstUsedDeltaNo, firstNotUsedDeltaNo := getDeltaRange(firstUsedLSN, firstNotUsedLSN)
	// Get locations from [firstUsedDeltaNo, lastUsedDeltaNo). We use lastUsedDeltaNo in next step
	err := deltaMap.getLocationsFromDeltas(folder, timeline, firstUsedDeltaNo, firstNotUsedDeltaNo.previous())
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during fetch locations from delta files.\n")
	}

	// Handle last delta file separately for fetch locations and walParser from it
	lastDeltaFile, err := getDeltaFile(folder, firstNotUsedDeltaNo.previous().getFilename(timeline))
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during downloading last delta file.\n")
	}
	deltaMap.AddLocationsToDelta(lastDeltaFile.Locations)

	firstUsedWalSegmentNo, firstNotUsedWalSegmentNo := getWalSegmentRange(firstNotUsedDeltaNo, firstNotUsedLSN)
	// we handle WAL files from [firstUsedWalSegmentNo, lastUsedWalSegmentNo]
	err = deltaMap.getLocationsFromWals(folder, timeline, firstUsedWalSegmentNo, firstNotUsedWalSegmentNo, lastDeltaFile.WalParser)
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during fetch locations from wal segments.\n")
	}
	return deltaMap, nil
}

func getDeltaRange(firstUsedLsn, firstNotUsedLsn uint64) (DeltaNo, DeltaNo) {
	firstUsedDeltaNo := newDeltaNoFromLsn(firstUsedLsn)
	firstNotUsedDeltaNo := newDeltaNoFromLsn(firstNotUsedLsn)
	return firstUsedDeltaNo, firstNotUsedDeltaNo
}

func getWalSegmentRange(firstNotUsedDeltaNo DeltaNo, firstNotUsedLsn uint64) (WalSegmentNo, WalSegmentNo) {
	firstUsedWalSegmentNo := firstNotUsedDeltaNo.firstWalSegmentNo()
	lastUsedLsn := firstNotUsedLsn - 1
	lastUsedWalSegmentNo := newWalSegmentNo(lastUsedLsn)
	return firstUsedWalSegmentNo, lastUsedWalSegmentNo.next()
}
