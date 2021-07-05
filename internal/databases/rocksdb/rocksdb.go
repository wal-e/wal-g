package rocksdb

// #cgo CFLAGS: -I../../../submodules/rocksdb/include
// #cgo LDFLAGS: -L../../../submodules/rocksdb -lrocksdb -lpthread -lrt -ldl -lgflags -lbz2 -lm -lstdc++
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
)

type DatabaseOptions struct {
	DbPath  string
	WalPath string
}

type BackupInfo struct {
	RawSize    uint64 `json:"RawSize"`
	BackupSize uint64 `json:"BackupSize"`
	Timestamp  int64  `json:"Timestamp"`
	BackupName string `json:"BackupName"`
}

type RestoreOptions struct {
	BackupName string
}

func NewDatabaseOptions(dbPath string, walDirectory string) DatabaseOptions {
	return DatabaseOptions{dbPath, walDirectory}
}

func NewRestoreOptions(backupName string) RestoreOptions {
	return RestoreOptions{backupName}
}

func OpenDatabase(dbOptions DatabaseOptions) (db *C.rocksdb_t, err error) {
	dbPathC := C.CString(dbOptions.DbPath)
	options := C.rocksdb_options_create()
	C.rocksdb_options_set_wal_dir(options, C.CString(dbOptions.WalPath))
	var cErr *C.char = nil
	db = C.rocksdb_open(options, dbPathC, &cErr)
	C.rocksdb_options_destroy(options)
	if cErr != nil {
		err = errors.New(C.GoString(cErr))
	}
	return
}

func (db *C.rocksdb_t) CreateCheckpointObject() (*C.rocksdb_checkpoint_t, error) {
	var cErr *C.char = nil
	checkpoint := C.rocksdb_checkpoint_object_create(db, &cErr)
	if cErr != nil {
		return nil, errors.New(C.GoString(cErr))
	}

	return checkpoint, nil
}

func (checkpoint *C.rocksdb_checkpoint_t) CreateCheckpoint(checkpointPath string, size int) error {
	var cErr *C.char = nil
	C.rocksdb_checkpoint_create(checkpoint, C.CString(checkpointPath), C.size_t(size), &cErr)
	if cErr != nil {
		return errors.New(C.GoString(cErr))
	}

	return nil
}

func (chkpnt *C.rocksdb_checkpoint_t) DestroyCheckpointObject() {
	C.rocksdb_checkpoint_object_destroy(chkpnt)
}

func (db *C.rocksdb_t) CloseDb() {
	C.rocksdb_close(db)
}
