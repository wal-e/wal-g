package storage

import "time"

type LocalObject struct {
	name         string
	lastModified time.Time
}

func NewLocalObject(name string, lastModified time.Time) *LocalObject {
	return &LocalObject{name, lastModified}
}

func (object LocalObject) GetName() string {
	return object.name
}

func (object LocalObject) GetLastModified() time.Time {
	return object.lastModified
}
