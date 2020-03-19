package storage_test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/storages/memory"
	"github.com/wal-g/wal-g/storages/storage"
	"strings"
	"testing"
)

func TestListFolderRecursively(t *testing.T) {
	var folder = memory.NewFolder("in_memory/", memory.NewStorage())
	paths := []string{
		"a",
		"subfolder1/b",
		"subfolder1/subfolder11/c",
		"subfolder2/d",
	}
	for _, relativePath := range paths {
		err := folder.PutObject(relativePath, &bytes.Buffer{})
		assert.NoError(t, err)
	}
	fullPathObjects, err := storage.ListFolderRecursively(folder)
	assert.NoError(t, err)
	for _, relativePath := range paths {
		found := false
		for _, object := range fullPathObjects {
			if object.GetName() == relativePath {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
}

func CreateMockStorageFolder() storage.Folder {
	var folder = memory.NewFolder("in_memory/", memory.NewStorage())
	subFolder := folder.GetSubFolder("basebackups_005/")
	subFolder.PutObject("base_123_backup_stop_sentinel.json", &bytes.Buffer{})
	subFolder.PutObject("base_456_backup_stop_sentinel.json", strings.NewReader("{}"))
	subFolder.PutObject("base_000_backup_stop_sentinel.json", &bytes.Buffer{}) // last put
	subFolder.PutObject("base_123312", &bytes.Buffer{})                        // not a sentinel
	subFolder.PutObject("base_321/nop", &bytes.Buffer{})
	subFolder.PutObject("folder123/nop", &bytes.Buffer{})
	subFolder.PutObject("base_456/tar_partitions/1", &bytes.Buffer{})
	subFolder.PutObject("base_456/tar_partitions/2", &bytes.Buffer{})
	subFolder.PutObject("base_456/tar_partitions/3", &bytes.Buffer{})
	return folder
}

func TestDeleteOldObjects(t *testing.T) {
	folder := CreateMockStorageFolder()
	expectedOnlyOneSavedObjectName := "basebackups_005/base_123312"
	filter := func(object storage.Object) bool {
		return object.GetName() != expectedOnlyOneSavedObjectName
	}
	err := storage.DeleteObjectsWhere(folder, true, filter)
	assert.NoError(t, err)
	savedObjects, err := storage.ListFolderRecursively(folder)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(savedObjects))
	assert.Equal(t, expectedOnlyOneSavedObjectName, savedObjects[0].GetName())
}
