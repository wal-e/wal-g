package storage

import (
	"time"
)

type Object interface {
	GetName() string
	GetLastModified() time.Time
}
