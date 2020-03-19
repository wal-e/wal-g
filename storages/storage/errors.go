package storage

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type ObjectNotFoundError struct {
	error
}

func NewObjectNotFoundError(path string) ObjectNotFoundError {
	return ObjectNotFoundError{errors.Errorf("object '%s' not found in storage", path)}
}

func (err ObjectNotFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type Error struct {
	error
}

func NewError(err error, storageName, format string, args ...interface{}) Error {
	return Error{errors.Wrapf(err, storageName+" error : "+format, args...)}
}

func (err Error) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}
