package internal

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/tinsane/storages/storage"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/utility"
)

const (
	NoDeleteModifier = iota
	FullDeleteModifier
	FindFullDeleteModifier
	ConfirmFlag            = "confirm"
	DeleteShortDescription = "Clears old backups and WALs"

	DeleteRetainExamples = `  retain 5                      keep 5 backups
  retain FULL 5                 keep 5 full backups and all deltas of them
  retain FIND_FULL 5            find necessary full for 5th and keep everything after it`

	DeleteBeforeExamples = `  before base_0123              keep everything after base_0123 including itself
  before FIND_FULL base_0123    keep everything after the base of base_0123`
)

var StringModifiers = []string{"FULL", "FIND_FULL"}

// TODO : unit tests
func GetLatestBackupName(folder storage.Folder) (string, error) {
	sortTimes, err := getBackups(folder)
	if err != nil {
		return "", err
	}

	return sortTimes[0].BackupName, nil
}

// TODO : unit tests
// getBackups receives backup descriptions and sorts them by time
func getBackups(folder storage.Folder) (backups []BackupTime, err error) {
	backups, _, err = getBackupsAndGarbage(folder)
	if err != nil {
		return nil, err
	}

	count := len(backups)
	if count == 0 {
		return nil, NewNoBackupsFoundError()
	}
	return
}

// TODO : unit tests
func getBackupsAndGarbage(folder storage.Folder) (backups []BackupTime, garbage []string, err error) {
	backupObjects, subFolders, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return nil, nil, err
	}

	sortTimes := getBackupTimeSlices(backupObjects)
	garbage = getGarbageFromPrefix(subFolders, sortTimes)

	return sortTimes, garbage, nil
}

// TODO : unit tests
func getBackupTimeSlices(backups []storage.Object) []BackupTime {
	sortTimes := make([]BackupTime, len(backups))
	for i, object := range backups {
		key := object.GetName()
		if !strings.HasSuffix(key, utility.SentinelSuffix) {
			continue
		}
		time := object.GetLastModified()
		sortTimes[i] = BackupTime{utility.StripBackupName(key), time, utility.StripWalFileName(key)}
	}
	sort.Slice(sortTimes, func(i, j int) bool {
		return sortTimes[i].Time.After(sortTimes[j].Time)
	})
	return sortTimes
}

// TODO : unit tests
func getGarbageFromPrefix(folders []storage.Folder, nonGarbage []BackupTime) []string {
	garbage := make([]string, 0)
	var keyFilter = make(map[string]string)
	for _, k := range nonGarbage {
		keyFilter[k.BackupName] = k.BackupName
	}
	for _, folder := range folders {
		backupName := utility.StripPrefixName(folder.GetPath())
		if _, ok := keyFilter[backupName]; ok {
			continue
		}
		garbage = append(garbage, backupName)
	}
	return garbage
}

func FindTargetBeforeName(folder storage.Folder,
	name string, modifier int,
	isFullBackup func(object storage.Object) bool,
	greater func(object1, object2 storage.Object) bool) (storage.Object, error) {

	choiceFunc := GetBeforeChoiceFunc(name, modifier, isFullBackup)
	if choiceFunc == nil {
		return nil, utility.NewForbiddenActionError("Not allowed modifier for 'delete before'")
	}
	return FindTarget(folder, greater, choiceFunc)
}

func FindTargetBeforeTime(folder storage.Folder,
	timeLine time.Time, modifier int,
	isFullBackup func(object storage.Object) bool,
	less func(object1, object2 storage.Object) bool) (storage.Object, error) {

	potentialTarget, err := FindTarget(folder, less, func(object storage.Object) bool {
		return timeLine.Before(object.GetLastModified()) || timeLine.Equal(object.GetLastModified())
	})
	if err != nil {
		return nil, err
	}
	greater := func(object1, object2 storage.Object) bool {
		return less(object2, object1)
	}
	return FindTargetBeforeName(folder, potentialTarget.GetName(), modifier, isFullBackup, greater)
}

func FindTargetRetain(folder storage.Folder,
	retentionCount, modifier int,
	isFullBackup func(object storage.Object) bool,
	greater func(object1, object2 storage.Object) bool) (storage.Object, error) {

	choiceFunc := GetRetainChoiceFunc(retentionCount, modifier, isFullBackup)
	if choiceFunc == nil {
		return nil, utility.NewForbiddenActionError("Not allowed modifier for 'delete retain'")
	}
	return FindTarget(folder, greater, choiceFunc)
}

func FindTarget(folder storage.Folder,
	compare func(object1, object2 storage.Object) bool,
	isTarget func(object storage.Object) bool) (storage.Object, error) {

	objects, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return nil, err
	}
	sort.Slice(objects, func(i, j int) bool {
		return compare(objects[i], objects[j])
	})
	for _, object := range objects {
		if isTarget(object) {
			return object, nil
		}
	}
	return nil, nil
}

func GetBeforeChoiceFunc(name string, modifier int,
	isFullBackup func(object storage.Object) bool) func(object storage.Object) bool {

	meetName := false
	switch modifier {
	case NoDeleteModifier:
		return func(object storage.Object) bool {
			return strings.HasPrefix(object.GetName(), name)
		}
	case FindFullDeleteModifier:
		return func(object storage.Object) bool {
			meetName = meetName || strings.HasPrefix(object.GetName(), name)
			return meetName && isFullBackup(object)
		}
	}
	return nil
}

func GetRetainChoiceFunc(retentionCount, modifier int,
	isFullBackup func(object storage.Object) bool) func(object storage.Object) bool {

	count := 0
	switch modifier {
	case NoDeleteModifier:
		return func(object storage.Object) bool {
			count++
			if count == retentionCount {
				return true
			}
			return false
		}
	case FullDeleteModifier:
		return func(object storage.Object) bool {
			if isFullBackup(object) {
				count++
			}
			if count == retentionCount {
				return true
			}
			return false
		}
	case FindFullDeleteModifier:
		return func(object storage.Object) bool {
			count++
			if count >= retentionCount && isFullBackup(object) {
				return true
			}
			return false
		}
	}
	return nil
}

func DeleteBeforeTarget(folder storage.Folder, target storage.Object,
	confirmed bool,
	isFullBackup func(object storage.Object) bool,
	less func(object1, object2 storage.Object) bool) error {

	if !isFullBackup(target) {
		errorMessage := "%v is incremental and it's predecessors cannot be deleted. Consider FIND_FULL option."
		return utility.NewForbiddenActionError(fmt.Sprintf(errorMessage, target.GetName()))
	}
	tracelog.InfoLogger.Println("Start delete")
	permanentBackups, permanentWals := getPermanentObjects(folder)
	if len(permanentBackups) > 0 {
		tracelog.InfoLogger.Printf("Found permanent objects: backups=%v, wals=%v\n", permanentBackups, permanentWals)
	}
	return storage.DeleteObjectsWhere(folder, confirmed, func(object storage.Object) bool {
		return less(object, target) && !isPermanent(object.GetName(), permanentBackups, permanentWals)
	})
}

func getPermanentObjects(folder storage.Folder) (map[string]bool, map[string]bool) {
	tracelog.InfoLogger.Println("retrieving permanent objects")
	backupTimes, err := getBackups(folder)
	if err != nil {
		return map[string]bool{}, map[string]bool{}
	}

	permanentBackups := map[string]bool{}
	permanentWals := map[string]bool{}
	for _, backupTime := range backupTimes {
		backup, err := GetBackupByName(backupTime.BackupName, folder)
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to get backup by name with error %s, ignoring...", err.Error())
			continue
		}
		meta, err := backup.GetMeta()
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to fetch backup meta for backup %s with error %s, ignoring...", backupTime.BackupName, err.Error())
			continue
		}
		if meta.IsPermanent {
			timelineID64, err := strconv.ParseUint(backupTime.BackupName[len(utility.BackupNamePrefix):len(utility.BackupNamePrefix)+8], 0x10, sizeofInt32bits)
			if err != nil {
				tracelog.ErrorLogger.Printf("failed to parse backup timeline for backup %s with error %s, ignoring...", backupTime.BackupName, err.Error())
				continue
			}
			timelineID := uint32(timelineID64)

			startWalSegmentNo := NewWalSegmentNo(meta.StartLsn - 1)
			endWalSegmentNo := NewWalSegmentNo(meta.FinishLsn - 1)
			for walSegmentNo := startWalSegmentNo; walSegmentNo <= endWalSegmentNo; walSegmentNo = walSegmentNo.Next() {
				permanentWals[walSegmentNo.GetFilename(timelineID)] = true
			}
			permanentBackups[backupTime.BackupName[len(utility.BackupNamePrefix):len(utility.BackupNamePrefix)+24]] = true
		}
	}
	return permanentBackups, permanentWals
}

func isPermanent(objectName string, permanentBackups map[string]bool, permanentWals map[string]bool) bool {
	if objectName[:len(utility.WalPath)] == utility.WalPath {
		wal := objectName[len(utility.WalPath) : len(utility.WalPath)+24]
		return permanentWals[wal]
	}
	if objectName[:len(utility.BaseBackupPath)] == utility.BaseBackupPath {
		backup := objectName[len(utility.BaseBackupPath)+len(utility.BackupNamePrefix) : len(utility.BaseBackupPath)+len(utility.BackupNamePrefix)+24]
		return permanentBackups[backup]
	}
	// should not reach here, default to false
	return false
}

func HandleDeleteBefore(folder storage.Folder, args []string, confirmed bool,
	isFullBackup func(object storage.Object) bool,
	less func(object1, object2 storage.Object) bool) {

	modifier, beforeStr := extractDeleteModifierFromArgs(args)
	timeLine, err := time.Parse(time.RFC3339, beforeStr)
	var target storage.Object
	if err == nil {
		target, err = FindTargetBeforeTime(folder, timeLine, modifier, isFullBackup, less)
	} else {
		greater := func(object1, object2 storage.Object) bool { return less(object2, object1) }
		target, err = FindTargetBeforeName(folder, beforeStr, modifier, isFullBackup, greater)
	}
	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}
	err = DeleteBeforeTarget(folder, target, confirmed, isFullBackup, less)
	tracelog.ErrorLogger.FatalOnError(err)
}

func HandleDeleteRetain(folder storage.Folder, args []string, confirmed bool,
	isFullBackup func(object storage.Object) bool,
	less func(object1, object2 storage.Object) bool) {

	modifier, retantionStr := extractDeleteModifierFromArgs(args)
	retentionCount, err := strconv.Atoi(retantionStr)
	tracelog.ErrorLogger.FatalOnError(err)
	greater := func(object1, object2 storage.Object) bool { return less(object2, object1) }
	target, err := FindTargetRetain(folder, retentionCount, modifier, isFullBackup, greater)
	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}
	err = DeleteBeforeTarget(folder, target, confirmed, isFullBackup, less)
	tracelog.ErrorLogger.FatalOnError(err)
}

func extractDeleteModifierFromArgs(args []string) (int, string) {
	if len(args) == 1 {
		return NoDeleteModifier, args[0]
	} else if args[0] == StringModifiers[FullDeleteModifier-1] {
		return FullDeleteModifier, args[1]
	} else {
		return FindFullDeleteModifier, args[1]
	}
}

func DeleteBeforeArgsValidator(cmd *cobra.Command, args []string) error {
	err := deleteArgsValidator(cmd, args)
	if err != nil {
		return err
	}
	modifier, beforeStr := extractDeleteModifierFromArgs(args)
	if modifier == FullDeleteModifier {
		return fmt.Errorf("unsupported moodifier for delete before command")
	}
	if before, err := time.Parse(time.RFC3339, beforeStr); err == nil {
		if before.After(utility.TimeNowCrossPlatformUTC()) {
			return fmt.Errorf("cannot delete before future date")
		}
	}
	return nil
}

func DeleteRetainArgsValidator(cmd *cobra.Command, args []string) error {
	err := deleteArgsValidator(cmd, args)
	if err != nil {
		return err
	}
	_, retantionStr := extractDeleteModifierFromArgs(args)
	retantionNumber, err := strconv.Atoi(retantionStr)
	if err != nil {
		return errors.Wrapf(err, "expected to get a number as retantion count, but got: '%s'", retantionStr)
	}
	if retantionNumber <= 0 {
		return fmt.Errorf("cannot retain less than one backup") // TODO : Consider allowing to delete everything
	}
	return nil
}

func deleteArgsValidator(cmd *cobra.Command, args []string) error {
	if len(args) != 1 && len(args) != 2 {
		return fmt.Errorf("accepts between 1 and 2 arg(s), received %d", len(args))
	}
	if len(args) == 2 {
		expectedModifier := args[0]
		if expectedModifier != StringModifiers[0] && expectedModifier != StringModifiers[1] {
			return fmt.Errorf("expected to get one of modifiers: %v as first argument", StringModifiers)
		}
	}
	return nil
}
