package memory

import (
	"bytes"
	"sync"
	"time"
)

// This function is needed for being cross-platform
func CeilTimeUpToMicroseconds(timeToCeil time.Time) time.Time {
	if timeToCeil.Nanosecond()%1000 != 0 {
		timeToCeil = timeToCeil.Add(time.Microsecond)
		timeToCeil = timeToCeil.Add(-time.Duration(timeToCeil.Nanosecond() % 1000))
	}
	return timeToCeil
}

type TimeStampedData struct {
	Data      bytes.Buffer
	Timestamp time.Time
}

func TimeStampData(data bytes.Buffer) TimeStampedData {
	return TimeStampedData{data, CeilTimeUpToMicroseconds(time.Now())}
}

// Storage is supposed to be used for tests. It doesn't guarantee data safety!
type Storage struct {
	underlying *sync.Map
}

func NewStorage() *Storage {
	return &Storage{&sync.Map{}}
}

func (storage *Storage) Load(key string) (value TimeStampedData, exists bool) {
	valueInterface, ok := storage.underlying.Load(key)
	if !ok {
		return TimeStampedData{}, ok
	}
	return valueInterface.(TimeStampedData), ok
}

func (storage *Storage) Store(key string, value bytes.Buffer) {
	storage.underlying.Store(key, TimeStampData(value))
}

func (storage *Storage) Delete(key string) {
	storage.underlying.Delete(key)
}

func (storage *Storage) Range(callback func(key string, value TimeStampedData) bool) {
	storage.underlying.Range(func(iKey, iValue interface{}) bool {
		return callback(iKey.(string), iValue.(TimeStampedData))
	})
}
