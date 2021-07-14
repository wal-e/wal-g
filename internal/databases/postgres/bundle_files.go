package postgres

import (
	"archive/tar"
	"os"
	"sync"

	"github.com/wal-g/wal-g/internal"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/walparser"
)

// BundleFiles represents the files in the backup that is going to be created
type BundleFiles interface {
	AddSkippedFile(tarHeader *tar.Header, fileInfo os.FileInfo)
	AddFile(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool)
	AddFileWithCorruptBlocks(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool,
		corruptedBlocks []uint32, storeAllBlocks bool)
	GetUnderlyingMap() *sync.Map
}

type RegularBundleFiles struct {
	sync.Map
}

func (files *RegularBundleFiles) AddSkippedFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	files.Store(tarHeader.Name,
		internal.BackupFileDescription{IsSkipped: true, IsIncremented: false, MTime: fileInfo.ModTime()})
}

func (files *RegularBundleFiles) AddFile(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool) {
	files.Store(tarHeader.Name,
		internal.BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented, MTime: fileInfo.ModTime()})
}

func (files *RegularBundleFiles) AddFileWithCorruptBlocks(tarHeader *tar.Header, fileInfo os.FileInfo,
	isIncremented bool, corruptedBlocks []uint32, storeAllBlocks bool) {
	fileDescription := internal.BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented, MTime: fileInfo.ModTime()}
	fileDescription.SetCorruptBlocks(corruptedBlocks, storeAllBlocks)
	files.Store(tarHeader.Name, fileDescription)
}

func (files *RegularBundleFiles) GetUnderlyingMap() *sync.Map {
	return &files.Map
}

func newStatBundleFiles(fileStat RelFileStatistics) *StatBundleFiles {
	return &StatBundleFiles{fileStats: fileStat}
}

// StatBundleFiles contains the bundle files.
// Additionally, it calculates and stores the updates count for each added file
type StatBundleFiles struct {
	sync.Map
	fileStats RelFileStatistics
}

func (files *StatBundleFiles) AddFileWithCorruptBlocks(tarHeader *tar.Header,
	fileInfo os.FileInfo,
	isIncremented bool,
	corruptedBlocks []uint32,
	storeAllBlocks bool) {
	updatesCount := files.fileStats.getFileUpdateCount(tarHeader.Name)
	fileDescription := internal.BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented, MTime: fileInfo.ModTime(),
		UpdatesCount: updatesCount}
	fileDescription.SetCorruptBlocks(corruptedBlocks, storeAllBlocks)
	files.Store(tarHeader.Name, fileDescription)
}

func (files *StatBundleFiles) AddSkippedFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	updatesCount := files.fileStats.getFileUpdateCount(tarHeader.Name)
	files.Store(tarHeader.Name,
		internal.BackupFileDescription{IsSkipped: true, IsIncremented: false,
			MTime: fileInfo.ModTime(), UpdatesCount: updatesCount})
}

func (files *StatBundleFiles) AddFile(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool) {
	updatesCount := files.fileStats.getFileUpdateCount(tarHeader.Name)
	files.Store(tarHeader.Name,
		internal.BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented,
			MTime: fileInfo.ModTime(), UpdatesCount: updatesCount})
}

func (files *StatBundleFiles) GetUnderlyingMap() *sync.Map {
	return &files.Map
}

type SimpleBundleFiles struct {
	sync.Map
}

func (files *SimpleBundleFiles) AddSkippedFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
}

func (files *SimpleBundleFiles) AddFile(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool) {
}

func (files *SimpleBundleFiles) AddFileWithCorruptBlocks(tarHeader *tar.Header, fileInfo os.FileInfo,
	isIncremented bool, corruptedBlocks []uint32, storeAllBlocks bool) {
}

func (files *SimpleBundleFiles) GetUnderlyingMap() *sync.Map {
	return &files.Map
}

type RelFileStatistics map[walparser.RelFileNode]PgRelationStat

func (relStat *RelFileStatistics) getFileUpdateCount(filePath string) uint64 {
	if relStat == nil {
		return 0
	}
	relFileNode, err := GetRelFileNodeFrom(filePath)
	if err != nil {
		return 0
	}
	fileStat, ok := (*relStat)[*relFileNode]
	if !ok {
		return 0
	}
	return fileStat.deletedTuplesCount + fileStat.updatedTuplesCount + fileStat.insertedTuplesCount
}

func newRelFileStatistics(conn *pgx.Conn) (RelFileStatistics, error) {
	databases, err := getDatabaseInfos(conn)
	if err != nil {
		return nil, errors.Wrap(err, "CollectStatistics: Failed to get db names.")
	}

	result := make(map[walparser.RelFileNode]PgRelationStat)
	// CollectStatistics collects statistics for each relFileNode
	for _, db := range databases {
		dbName := db.name
		databaseOption := func(c *pgx.ConnConfig) error {
			c.Database = dbName
			return nil
		}
		dbConn, err := Connect(databaseOption)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to collect statistics for database: %s\n'%v'\n", db.name, err)
			continue
		}

		queryRunner, err := NewPgQueryRunner(dbConn)
		if err != nil {
			return nil, errors.Wrap(err, "CollectStatistics: Failed to build query runner.")
		}
		pgStatRows, err := queryRunner.getStatistics(db)
		if err != nil {
			return nil, errors.Wrap(err, "CollectStatistics: Failed to collect statistics.")
		}
		for relFileNode, statRow := range pgStatRows {
			result[relFileNode] = statRow
		}
		err = dbConn.Close()
		tracelog.WarningLogger.PrintOnError(err)
	}
	return result, nil
}

func getDatabaseInfos(conn *pgx.Conn) ([]PgDatabaseInfo, error) {
	queryRunner, err := NewPgQueryRunner(conn)
	if err != nil {
		return nil, errors.Wrap(err, "getDatabaseInfos: Failed to build query runner.")
	}
	return queryRunner.getDatabaseInfos()
}
