package internal_test

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
	"testing"
)

type backupInfo map[string]struct {
	meta     internal.ExtendedMetadataDto
	sentinel internal.BackupSentinelDto
}

func TestGetBackupMetadataToUpload_markSeveralBackups(t *testing.T) {
	backups := backupInfo{
		"base_000000010000000000000002": {
			meta: internal.ExtendedMetadataDto{
				CommonMetadataDto: internal.CommonMetadataDto{
					IsPermanent: false,
				},
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom: nil,
			},
		},
		"base_000000010000000000000004_D_000000010000000000000002": {
			meta: internal.ExtendedMetadataDto{
				CommonMetadataDto: internal.CommonMetadataDto{
					IsPermanent: false,
				},
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000002"),
				IncrementFromLSN:  func(i uint64) *uint64 { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
		"base_000000010000000000000006_D_000000010000000000000004": {
			meta: internal.ExtendedMetadataDto{
				CommonMetadataDto: internal.CommonMetadataDto{
					IsPermanent: false,
				},
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000004_D_000000010000000000000002"),
				IncrementFromLSN:  func(i uint64) *uint64 { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
	}
	toMark := "base_000000010000000000000006_D_000000010000000000000004"
	expectUploadObjectLen := 3
	expectUploadObjectPaths := map[int]string{
		0: "base_000000010000000000000002" + "/" + utility.MetadataFileName,
		1: "base_000000010000000000000004_D_000000010000000000000002" + "/" + utility.MetadataFileName,
		2: "base_000000010000000000000006_D_000000010000000000000004" + "/" + utility.MetadataFileName,
	}

	testGetBackupMetadataToUpload(backups, true, false, toMark, expectUploadObjectLen, expectUploadObjectPaths, t)
}

func TestGetBackupMetadataToUpload_markOneBackup(t *testing.T) {
	backups := backupInfo{
		"base_000000010000000000000002": {
			meta: internal.ExtendedMetadataDto{
				CommonMetadataDto: internal.CommonMetadataDto{
					IsPermanent: true,
				},
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom: nil,
			},
		},
		"base_000000010000000000000004_D_000000010000000000000002": {
			meta: internal.ExtendedMetadataDto{
				CommonMetadataDto: internal.CommonMetadataDto{
					IsPermanent: true,
				},
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000002"),
				IncrementFromLSN:  func(i uint64) *uint64 { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
		"base_000000010000000000000006_D_000000010000000000000004": {
			meta: internal.ExtendedMetadataDto{
				CommonMetadataDto: internal.CommonMetadataDto{
					IsPermanent: false,
				},
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000004_D_000000010000000000000002"),
				IncrementFromLSN:  func(i uint64) *uint64 { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
	}
	toMark := "base_000000010000000000000006_D_000000010000000000000004"
	expectUploadObjectLen := 1
	expectUploadObjectPaths := map[int]string{
		0: "base_000000010000000000000006_D_000000010000000000000004" + "/" + utility.MetadataFileName,
	}
	testGetBackupMetadataToUpload(backups, true, false, toMark, expectUploadObjectLen, expectUploadObjectPaths, t)
}

func TestGetBackupMetadataToUpload_unmarkOneBackupWithIncrementBackups(t *testing.T) {
	backups := backupInfo{
		"base_000000010000000000000002": {
			meta: internal.ExtendedMetadataDto{
				CommonMetadataDto: internal.CommonMetadataDto{
					IsPermanent: true,
				},
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom: nil,
			},
		},
		"base_000000010000000000000004_D_000000010000000000000002": {
			meta: internal.ExtendedMetadataDto{
				CommonMetadataDto: internal.CommonMetadataDto{
					IsPermanent: false,
				},
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000002"),
				IncrementFromLSN:  func(i uint64) *uint64 { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
		"base_000000010000000000000006_D_000000010000000000000004": {
			meta: internal.ExtendedMetadataDto{
				CommonMetadataDto: internal.CommonMetadataDto{
					IsPermanent: false,
				},
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000004_D_000000010000000000000002"),
				IncrementFromLSN:  func(i uint64) *uint64 { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
	}
	toMark := "base_000000010000000000000002"
	expectUploadObjectLen := 1
	expectUploadObjectPaths := map[int]string{
		0: "base_000000010000000000000002" + "/" + utility.MetadataFileName,
	}

	testGetBackupMetadataToUpload(backups, false, false, toMark, expectUploadObjectLen, expectUploadObjectPaths, t)
}

func TestGetBackupMetadataToUpload_unmarkOneBackupWithoutIncrementBackups(t *testing.T) {
	backups := backupInfo{
		"base_000000010000000000000002": {
			meta: internal.ExtendedMetadataDto{
				CommonMetadataDto: internal.CommonMetadataDto{
					IsPermanent: true,
				},
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom: nil,
			},
		},
		"base_000000010000000000000004_D_000000010000000000000002": {
			meta: internal.ExtendedMetadataDto{
				CommonMetadataDto: internal.CommonMetadataDto{
					IsPermanent: true,
				},
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000002"),
				IncrementFromLSN:  func(i uint64) *uint64 { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
	}
	toMark := "base_000000010000000000000004_D_000000010000000000000002"
	expectUploadObjectLen := 1
	expectUploadObjectPaths := map[int]string{
		0: "base_000000010000000000000004_D_000000010000000000000002" + "/" + utility.MetadataFileName,
	}

	testGetBackupMetadataToUpload(backups, false, false, toMark, expectUploadObjectLen, expectUploadObjectPaths, t)
}

func TestGetBackupMetadataToUpload_tryToMarkAlreadyMarkedBackup(t *testing.T) {
	backups := backupInfo{
		"base_000000010000000000000002": {
			meta: internal.ExtendedMetadataDto{
				CommonMetadataDto: internal.CommonMetadataDto{
					IsPermanent: true,
				},
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom: nil,
			},
		},
	}
	toMark := "base_000000010000000000000002"
	expectUploadObjectLen := 0
	expectUploadObjectPaths := map[int]string{}

	testGetBackupMetadataToUpload(backups, true, true, toMark, expectUploadObjectLen, expectUploadObjectPaths, t)
}

func TestGetBackupMetadataToUpload_tryToUnmarkAlreadyUnmarkedBackup(t *testing.T) {
	backups := backupInfo{
		"base_000000010000000000000002": {
			meta: internal.ExtendedMetadataDto{
				CommonMetadataDto: internal.CommonMetadataDto{
					IsPermanent: false,
				},
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom: nil,
			},
		},
	}
	toMark := "base_000000010000000000000002"
	expectUploadObjectLen := 0
	expectUploadObjectPaths := map[int]string{}

	testGetBackupMetadataToUpload(backups, false, true, toMark, expectUploadObjectLen, expectUploadObjectPaths, t)
}

func TestGetBackupMetadataToUpload_tryToUnmarkBackupWithMarkedIncrementBackups(t *testing.T) {
	backups := backupInfo{
		"base_000000010000000000000002": {
			meta: internal.ExtendedMetadataDto{
				CommonMetadataDto: internal.CommonMetadataDto{
					IsPermanent: true,
				},
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom: nil,
			},
		},
		"base_000000010000000000000004_D_000000010000000000000002": {
			meta: internal.ExtendedMetadataDto{
				CommonMetadataDto: internal.CommonMetadataDto{
					IsPermanent: true,
				},
			},
			sentinel: internal.BackupSentinelDto{
				IncrementFrom:     func(s string) *string { return &s }("base_000000010000000000000002"),
				IncrementFromLSN:  func(i uint64) *uint64 { return &i }(1),
				IncrementFullName: func(s string) *string { return &s }(""),
				IncrementCount:    func(i int) *int { return &i }(1),
			},
		},
	}
	toMark := "base_000000010000000000000002"
	expectUploadObjectLen := 0
	expectUploadObjectPaths := map[int]string{}
	testGetBackupMetadataToUpload(backups, false, true, toMark, expectUploadObjectLen, expectUploadObjectPaths, t)
}

func testGetBackupMetadataToUpload(
	backups backupInfo,
	toPermanent,
	isErrorExpect bool,
	toMark string,
	expectUploadObjectLen int,
	expectUploadObjectPaths map[int]string,
	t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	for backupName, backupData := range backups {
		sentinelBytes, err := json.Marshal(backupData.sentinel)
		assert.NoError(t, err)
		err = baseBackupFolder.PutObject(backupName+utility.SentinelSuffix, bytes.NewReader(sentinelBytes))
		assert.NoError(t, err)
		metaBytes, err := json.Marshal(backupData.meta)
		assert.NoError(t, err)
		err = baseBackupFolder.PutObject(backupName+"/"+utility.MetadataFileName, bytes.NewReader(metaBytes))
		assert.NoError(t, err)
	}
	uploadObjects, err := internal.GetMarkedBackupMetadataToUpload(folder, toMark, toPermanent)
	if !isErrorExpect {
		assert.NoError(t, err)
		assert.Equal(t, len(uploadObjects), expectUploadObjectLen)
		for idx, path := range expectUploadObjectPaths {
			assert.Equal(t, uploadObjects[idx].Path, path)
		}
	} else {
		assert.Error(t, err)
	}
}
