package internal_test

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/test/mocks"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestFindTargetBeforeName_ReturnsBackup_Without_Modifier(t *testing.T) {
	targetDelta := "base_000000010000000000000005_D_000000010000000000000003"
	expected := targetDelta + utility.SentinelSuffix
	testFindTargetBeforeName(t, expected, targetDelta, internal.NoDeleteModifier)
}

func TestFindTargetBeforeName_ReturnsForbiddenActionError_With_FULL_Modifier(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	_, err := internal.FindTargetBeforeName(mocks.NewMockFolder(controller), "",
		internal.FullDeleteModifier, isFullBackup, greaterByName)
	assert.Error(t, err)
	assert.IsType(t, utility.ForbiddenActionError{}, err)
}

func TestFindTargetBeforeName_ReturnsFullBackup_With_FIND_FULL(t *testing.T) {
	targetDelta := "base_000000010000000000000009_D_000000010000000000000007"
	expected := "base_000000010000000000000007" + utility.SentinelSuffix
	testFindTargetBeforeName(t, expected, targetDelta, internal.FindFullDeleteModifier)
}

func testFindTargetBeforeName(t *testing.T, expected, targetName string, modifier int) {
	folder := createMockStorageFolderWithDeltaBackups(t)
	target, err := internal.FindTargetBeforeName(folder, targetName, modifier, isFullBackup, greaterByName)
	assert.NoError(t, err)
	assert.Equal(t, expected, target.GetName())
}

func TestFindTargetRetain_Without_Modifier(t *testing.T) {
	expectedName := "base_000000010000000000000003_D_000000010000000000000002"
	testTargetRetain(t, expectedName, 2, internal.NoDeleteModifier)
}

func TestFindTargetRetain_With_FULL_Modifier(t *testing.T) {
	expectedName := "base_000000010000000000000002"
	testTargetRetain(t, expectedName, 2, internal.FullDeleteModifier)
}

func TestFindTargetRetain_With_FIND_FULL_Modifier(t *testing.T) {
	expectedName := "base_000000010000000000000000"
	testTargetRetain(t, expectedName, 4, internal.FindFullDeleteModifier)
}

func testTargetRetain(t *testing.T, expectedName string, retentionCount, modifier int) {
	mockFolder := createMockFolderWithTime(t, utility.TimeNowCrossPlatformLocal())

	target, err := internal.FindTargetRetain(mockFolder, retentionCount, modifier, isFullBackup, greaterByTime)
	assert.NoError(t, err)
	assert.Equal(t, expectedName, target.GetName())
}

func getBoundedValue(leftBound, value, rightBound int) int {
	if value < leftBound {
		return leftBound
	} else if value > rightBound {
		return rightBound
	} else {
		return value
	}
}

func intMin(value1, value2 int) int {
	if value1 > value2 {
		return value2
	} else {
		return value1
	}
}

func TestFindTargetRetainAfter_Without_Modifier(t *testing.T) {
	backupNames := []string{
		"base_000000010000000000000000",
		"base_000000010000000000000001_D_000000010000000000000000",
		"base_000000010000000000000002",
		"base_000000010000000000000003_D_000000010000000000000002",
		"base_000000010000000000000004",
	}
	for retentionCount := 1; retentionCount <= 5; retentionCount++ {
		for minutesCount := 0; minutesCount < 5; minutesCount++ {
			expectedIndex := intMin(getBoundedValue(0, minutesCount, 4), 5-retentionCount)
			expectedName := backupNames[expectedIndex]
			duration := time.Duration(minutesCount * int(time.Minute))
			testTargetRetainAfterTime(t, duration, expectedName, retentionCount, internal.NoDeleteModifier)
			testTargetRetainAfterName(t, backupNames[minutesCount], expectedName, retentionCount, internal.NoDeleteModifier)
		}
	}
}

func TestFindTargetRetainAfter_With_FULL_Modifier(t *testing.T) {
	backupNames := []string{
		"base_000000010000000000000000",
		"base_000000010000000000000001_D_000000010000000000000000",
		"base_000000010000000000000002",
		"base_000000010000000000000003_D_000000010000000000000002",
		"base_000000010000000000000004",
	}
	for retentionCount := 1; retentionCount <= 3; retentionCount++ {
		for minutesCount := 1; minutesCount < 5; minutesCount++ {
			expectedIndex := intMin(((getBoundedValue(0, minutesCount, 4)+1)/2)*2, 6-retentionCount*2)
			expectedName := backupNames[expectedIndex]
			duration := time.Duration(minutesCount * int(time.Minute))
			testTargetRetainAfterTime(t, duration, expectedName, retentionCount, internal.FullDeleteModifier)
			testTargetRetainAfterName(t, backupNames[minutesCount], expectedName, retentionCount, internal.FullDeleteModifier)
		}
	}
}

func TestFindTargetRetainAfter_With_FIND_FULL_Modifier(t *testing.T) {
	backupNames := []string{
		"base_000000010000000000000000",
		"base_000000010000000000000001_D_000000010000000000000000",
		"base_000000010000000000000002",
		"base_000000010000000000000003_D_000000010000000000000002",
		"base_000000010000000000000004",
	}
	for retentionCount := 1; retentionCount <= 5; retentionCount++ {
		for minutesCount := 0; minutesCount < 5; minutesCount++ {
			expectedIndex := intMin(((getBoundedValue(0, minutesCount, 4)+1)/2)*2, 4-(retentionCount/2)*2)
			expectedName := backupNames[expectedIndex]
			duration := time.Duration(minutesCount * int(time.Minute))
			testTargetRetainAfterTime(t, duration, expectedName, retentionCount, internal.FindFullDeleteModifier)
			testTargetRetainAfterName(t, backupNames[minutesCount], expectedName, retentionCount, internal.FindFullDeleteModifier)
		}
	}
}

func testTargetRetainAfterTime(t *testing.T, duration time.Duration, expectedName string, retentionCount, modifier int) {
	baseTime := utility.TimeNowCrossPlatformLocal()
	mockFolder := createMockFolderWithTime(t, baseTime)

	target, err := internal.FindTargetRetainAfterTime(mockFolder, retentionCount, baseTime.Add(duration), modifier, isFullBackup, greaterByTime)
	assert.NoError(t, err)
	assert.Equal(t, expectedName, target.GetName())
}

func testTargetRetainAfterName(t *testing.T, name string, expectedName string, retentionCount, modifier int) {
	mockFolder := createMockFolderWithTime(t, utility.TimeNowCrossPlatformLocal())

	target, err := internal.FindTargetRetainAfterName(mockFolder, retentionCount, name, modifier, isFullBackup, greaterByTime)
	assert.NoError(t, err)
	assert.Equal(t, expectedName, target.GetName())
}

func TestFindTargetBeforeTime_ReturnBackup_Without_Modifier(t *testing.T) {
	expected := "base_000000010000000000000001_D_000000010000000000000000"
	target, err := testFindTargetBeforeTime(t, 1, internal.NoDeleteModifier)
	assert.NoError(t, err)
	assert.Equal(t, expected, target.GetName())
}

func TestFindTargetBeforeTime_ReturnsForbiddenActionError_With_FULL_Modifier(t *testing.T) {
	_, err := testFindTargetBeforeTime(t, 2, internal.FullDeleteModifier)
	assert.Error(t, err)
	assert.IsType(t, utility.ForbiddenActionError{}, err)
}

func TestFindTargetBeforeTime_With_FIND_FULL_Modifier(t *testing.T) {
	expected := "base_000000010000000000000002"
	target, err := testFindTargetBeforeTime(t, 3, internal.FindFullDeleteModifier)
	assert.NoError(t, err)
	assert.Equal(t, expected, target.GetName())
}

func testFindTargetBeforeTime(t *testing.T, minute int, modifier int) (storage.Object, error) {
	baseTime := utility.TimeNowCrossPlatformLocal()
	mockFolder := createMockFolderWithTime(t, baseTime)

	timeLine := baseTime.Add(time.Duration(minute * int(time.Minute)))
	return internal.FindTargetBeforeTime(mockFolder, timeLine, modifier, isFullBackup, lessByTime)
}

func verifyThatExistBackupsAndWals(t *testing.T, expectBackupExistAfterDelete, expectWalExistAfterDelete map[string]bool, folder storage.Folder) {
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	walBackupFolder := folder.GetSubFolder(utility.WalPath)
	for backupName, expect := range expectBackupExistAfterDelete {
		exists, err := baseBackupFolder.Exists(backupName + "/" + utility.MetadataFileName)
		assert.NoError(t, err)
		assert.Equal(t, expect, exists, "errored on "+backupName+"/"+utility.MetadataFileName)
	}
	for walName, expect := range expectWalExistAfterDelete {
		exists, err := walBackupFolder.Exists(walName + ".lz4")
		assert.NoError(t, err)
		assert.Equal(t, expect, exists, "errored on "+walName+".lz4")
	}
}

func TestDeleteBeforeTargetWithPermanentBackups(t *testing.T) {
	folder := createMockStorageFolderWithPermanentBackups(t)

	expectBackupExistBeforeDelete := map[string]bool{
		"base_000000010000000000000002":                            true,
		"base_000000010000000000000004_D_000000010000000000000002": true,
		"base_000000010000000000000006_D_000000010000000000000004": true,
	}
	expectWalExistBeforeDelete := map[string]bool{
		"000000010000000000000001": true,
		"000000010000000000000002": true,
		"000000010000000000000003": true,
	}

	expectBackupExistAfterDelete := map[string]bool{
		"base_000000010000000000000002":                            true,
		"base_000000010000000000000004_D_000000010000000000000002": true,
		"base_000000010000000000000006_D_000000010000000000000004": false,
	}
	expectWalExistAfterDelete := map[string]bool{
		"000000010000000000000001": true,
		"000000010000000000000002": true,
		"000000010000000000000003": false,
	}

	// verify that they exist initially
	verifyThatExistBackupsAndWals(t, expectBackupExistBeforeDelete, expectWalExistBeforeDelete, folder)

	// attempt delete
	target := storage.NewLocalObject("", utility.TimeNowCrossPlatformLocal().Add(time.Duration(1*int(time.Minute))))
	err := internal.DeleteBeforeTarget(folder, target, true, isFullBackup, lessByTime)
	assert.NoError(t, err)

	// verify expected permanent still exists
	verifyThatExistBackupsAndWals(t, expectBackupExistAfterDelete, expectWalExistAfterDelete, folder)
}

func createMockFolderWithTime(t *testing.T, baseTime time.Time) *mocks.MockFolder {
	baseNamePrefix := "base_"
	deltaMark := "_D_"
	lsnPrefix := "00000001000000000000000"
	objects := make([]storage.Object, 5)
	var lastLSN, name string
	for i := 0; i < 5; i++ {
		iDuration := time.Duration(i * int(time.Minute))
		if i%2 == 0 {
			lastLSN = lsnPrefix + strconv.Itoa(i)
			name = baseNamePrefix + lastLSN
		} else {
			name = baseNamePrefix + lsnPrefix + strconv.Itoa(i) + deltaMark + lastLSN
		}
		objects[i] = storage.NewLocalObject(name, baseTime.Add(iDuration))
	}

	controller := gomock.NewController(t)
	defer controller.Finish()

	mockBaseBackupFolder := mocks.NewMockFolder(controller)

	mockBaseBackupFolder.
		EXPECT().
		ListFolder().
		Return(objects, nil, nil).
		AnyTimes()

	mockFolder := mocks.NewMockFolder(controller)

	mockFolder.
		EXPECT().
		GetSubFolder(utility.BaseBackupPath).
		Return(mockBaseBackupFolder).
		AnyTimes()
	return mockFolder
}

func isFullBackup(object storage.Object) bool {
	return !strings.Contains(object.GetName(), "D")
}

func greaterByName(object1, object2 storage.Object) bool {
	return object1.GetName() > object2.GetName()
}

func lessByTime(object1, object2 storage.Object) bool {
	return object1.GetLastModified().Before(object2.GetLastModified())
}

func greaterByTime(object1, object2 storage.Object) bool {
	return object1.GetLastModified().After(object2.GetLastModified())
}

func createMockStorageFolderWithDeltaBackups(t *testing.T) storage.Folder {
	var folder = testtools.MakeDefaultInMemoryStorageFolder()
	subFolder := folder.GetSubFolder(utility.BaseBackupPath)
	sentinelData := map[string]interface{}{
		"DeltaFrom":     "",
		"DeltaFullName": "base_000000010000000000000007",
		"DeltaFromLSN":  0,
		"DeltaCount":    0,
	}
	emptySentinelData := map[string]interface{}{}
	backupNames := map[string]interface{}{
		"base_000000010000000000000003":                            emptySentinelData,
		"base_000000010000000000000005_D_000000010000000000000003": sentinelData,
		"base_000000010000000000000007":                            emptySentinelData,
		"base_000000010000000000000009_D_000000010000000000000007": sentinelData}
	for backupName, sentinelD := range backupNames {
		bytesSentinel, err := json.Marshal(&sentinelD)
		assert.NoError(t, err)
		sentinelString := string(bytesSentinel)
		err = subFolder.PutObject(backupName+utility.SentinelSuffix, strings.NewReader(sentinelString))
		assert.NoError(t, err)
	}
	return folder
}

func createMockStorageFolderWithPermanentBackups(t *testing.T) storage.Folder {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	walBackupFolder := folder.GetSubFolder(utility.WalPath)
	emptyData := map[string]interface{}{}
	backupNames := map[string]interface{}{
		"base_000000010000000000000002": map[string]interface{}{
			"start_time":   utility.TimeNowCrossPlatformLocal().Format(time.RFC3339),
			"finish_time":  utility.TimeNowCrossPlatformLocal().Format(time.RFC3339),
			"hostname":     "",
			"data_dir":     "",
			"pg_version":   0,
			"start_lsn":    16777216, // logSegNo = 1
			"finish_lsn":   33554432, // logSegNo = 2
			"is_permanent": true,
		},
		"base_000000010000000000000004_D_000000010000000000000002": map[string]interface{}{
			"start_time":   utility.TimeNowCrossPlatformLocal().Format(time.RFC3339),
			"finish_time":  utility.TimeNowCrossPlatformLocal().Format(time.RFC3339),
			"hostname":     "",
			"data_dir":     "",
			"pg_version":   0,
			"start_lsn":    16777217, // logSegNo = 1
			"finish_lsn":   33554433, // logSegNo = 2
			"is_permanent": true,
		},
		"base_000000010000000000000006_D_000000010000000000000004": emptyData,
	}
	walNames := map[string]interface{}{
		"000000010000000000000001": emptyData,
		"000000010000000000000002": emptyData,
		"000000010000000000000003": emptyData,
	}
	for backupName, metadata := range backupNames {
		// empty sentinel
		empty, err := json.Marshal(&emptyData)
		assert.NoError(t, err)
		sentinelString := string(empty)
		err = baseBackupFolder.PutObject(backupName+utility.SentinelSuffix, strings.NewReader(sentinelString))

		// metadata
		assert.NoError(t, err)
		bytesMetadata, err := json.Marshal(&metadata)
		assert.NoError(t, err)
		metadataString := string(bytesMetadata)
		err = baseBackupFolder.PutObject(backupName+"/"+utility.MetadataFileName, strings.NewReader(metadataString))
		assert.NoError(t, err)
	}
	for walName, data := range walNames {
		bytes, err := json.Marshal(&data)
		assert.NoError(t, err)
		walString := string(bytes)
		err = walBackupFolder.PutObject(walName+".lz4", strings.NewReader(walString))
		assert.NoError(t, err)
	}
	return folder
}
