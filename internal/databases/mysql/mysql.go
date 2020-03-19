package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"github.com/wal-g/wal-g/storages/storage"
	"io/ioutil"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const BinlogPath = "binlog_" + utility.VersionStr + "/"

// not really the maximal value, but high enough.
// NOTE: can't use MaxInt64, due to time.Time implementation issues (it adds some value to it)
var MaxTime = time.Unix(math.MaxInt64/2, 0)

var MinTime = time.Unix(0, 0)

func scanToMap(rows *sql.Rows, dst map[string]interface{}) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	args := make([]interface{}, len(columns))
	var garbage interface{}
	for i, field := range columns {
		if v, ok := dst[field]; ok {
			args[i] = v
		} else {
			args[i] = &garbage
		}
	}
	return rows.Scan(args...)
}

func getMySQLCurrentBinlogFile(db *sql.DB) (fileName string) {
	rows, err := db.Query("SHOW MASTER STATUS")
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(rows, "")
	var logFileName string
	for rows.Next() {
		err = scanToMap(rows, map[string]interface{}{"File": &logFileName})
		tracelog.ErrorLogger.FatalOnError(err)
		return logFileName
	}
	tracelog.ErrorLogger.Fatalf("Failed to obtain current binlog file")
	return ""
}

func getMySQLConnection() (*sql.DB, error) {
	datasourceName, err := internal.GetRequiredSetting(internal.MysqlDatasourceNameSetting)
	db, err := getMySqlConnectionFromDatasource(datasourceName)
	if err != nil {
		fallbackDatasourceName := replaceHostInDatasourceName(datasourceName, "localhost")
		if fallbackDatasourceName != datasourceName {
			tracelog.ErrorLogger.Println(err.Error())
			tracelog.ErrorLogger.Println("Failed to connect using provided host, trying localhost")

			db, err = getMySqlConnectionFromDatasource(datasourceName)
		}
	}
	return db, err
}

func getMySqlConnectionFromDatasource(datasourceName string) (*sql.DB, error) {
	if caFile, ok := internal.GetSetting(internal.MysqlSslCaSetting); ok {
		rootCertPool := x509.NewCertPool()
		pem, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return nil, fmt.Errorf("Failed to load certificate from %s", caFile)
		}
		err = mysql.RegisterTLSConfig("custom", &tls.Config{
			RootCAs: rootCertPool,
		})
		if err != nil {
			return nil, err
		}
		if strings.Contains(datasourceName, "?tls=") || strings.Contains(datasourceName, "&tls=") {
			return nil, fmt.Errorf("MySQL datasource string contains tls option. It can't be used with %v option", internal.MysqlSslCaSetting)
		}
		if strings.Contains(datasourceName, "?") {
			datasourceName += "&tls=custom"
		} else {
			datasourceName += "?tls=custom"
		}
	}
	db, err := sql.Open("mysql", datasourceName)
	return db, err
}

func replaceHostInDatasourceName(datasourceName string, newHost string) string {
	var userData, dbNameAndParams string

	splitName := strings.SplitN(datasourceName, "@", 2)
	if len(splitName) == 2 {
		userData = splitName[0]
	} else {
		userData = ""
	}
	splitName = strings.SplitN(datasourceName, "/", 2)
	if len(splitName) == 2 {
		dbNameAndParams = splitName[1]
	} else {
		dbNameAndParams = ""
	}

	return userData + "@" + newHost + "/" + dbNameAndParams
}

type StreamSentinelDto struct {
	BinLogStart    string `json:"BinLogStart,omitempty"`
	BinLogEnd      string `json:"BinLogEnd,omitempty"`
	StartLocalTime time.Time
}

type BinlogHandler func(logFolder storage.Folder, logName string, endTs time.Time) (needAbortFetch bool, err error)

func fetchLogs(folder storage.Folder, startTs time.Time, endTs time.Time, handler BinlogHandler) ([]storage.Object, error) {
	logFolder := folder.GetSubFolder(BinlogPath)
	logsToFetch, err := getLogsCoveringInterval(logFolder, startTs)
	if err != nil {
		return nil, err
	}
	var fetchedLogs []storage.Object
	for _, logFile := range logsToFetch {
		logName := utility.TrimFileExtension(logFile.GetName())
		if needAbortFetch, err := handler(logFolder, logName, endTs); err != nil {
			return nil, err
		} else if needAbortFetch {
			break
		}
		fetchedLogs = append(fetchedLogs, logFile)
	}
	return fetchedLogs, nil
}

func configureEndTs(untilDt string) (time.Time, error) {
	if untilDt != "" {
		dt, err := time.Parse(time.RFC3339, untilDt)
		if err != nil {
			return time.Time{}, err
		}
		return dt, nil
	}
	// far future
	return MaxTime, nil
}

func getBinlogStartTs(folder storage.Folder, backup *internal.Backup) (time.Time, error) {
	startTs := MaxTime // far future
	var streamSentinel StreamSentinelDto
	err := internal.FetchStreamSentinel(backup, &streamSentinel)
	if err != nil {
		return time.Time{}, err
	}
	// case when backup was uploaded before first binlog
	sentinels, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return time.Time{}, err
	}
	for _, sentinel := range sentinels {
		if strings.HasPrefix(sentinel.GetName(), backup.Name) {
			tracelog.InfoLogger.Printf("Backup sentinel: %s (%s)", sentinel.GetName(), sentinel.GetLastModified())
			if sentinel.GetLastModified().Before(startTs) {
				startTs = sentinel.GetLastModified()
			}
		}
	}
	// case when binlog was uploaded before backup
	binlogs, _, err := folder.GetSubFolder(BinlogPath).ListFolder()
	if err != nil {
		return time.Time{}, err
	}
	for _, binlog := range binlogs {
		if strings.HasPrefix(binlog.GetName(), streamSentinel.BinLogStart) {
			tracelog.InfoLogger.Printf("Backup start binlog: %s (%s)", binlog.GetName(), binlog.GetLastModified())
			if binlog.GetLastModified().Before(startTs) {
				startTs = binlog.GetLastModified()
			}
		}
	}
	return startTs, nil
}

// getLogsCoveringInterval lists the operation logs that cover the interval
func getLogsCoveringInterval(folder storage.Folder, start time.Time) ([]storage.Object, error) {
	logFiles, _, err := folder.ListFolder()
	if err != nil {
		return nil, err
	}
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].GetLastModified().Before(logFiles[j].GetLastModified())
	})
	var logsToFetch []storage.Object
	for _, logFile := range logFiles {
		if start.Before(logFile.GetLastModified()) || start.Equal(logFile.GetLastModified()) {
			logsToFetch = append(logsToFetch, logFile)
		}
	}
	return logsToFetch, nil
}
