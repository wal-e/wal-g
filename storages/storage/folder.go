package storage

import (
	"github.com/wal-g/tracelog"
	"io"
	"path"
	"strings"
)

type Folder interface {
	// Path should always ends with '/'
	GetPath() string

	// Should return objects with relative paths
	ListFolder() (objects []Object, subFolders []Folder, err error)

	// Delete object, if exists
	DeleteObjects(objectRelativePaths []string) error

	Exists(objectRelativePath string) (bool, error)

	// Returns handle to subfolder. Does not have to instantiate subfolder in any material form
	GetSubFolder(subFolderRelativePath string) Folder

	// Should return ObjectNotFoundError in case, there is no such object
	ReadObject(objectRelativePath string) (io.ReadCloser, error)

	PutObject(name string, content io.Reader) error
}

func DeleteObjectsWhere(folder Folder, confirm bool, filter func(object1 Object) bool) error {
	relativePathObjects, err := ListFolderRecursively(folder)
	if err != nil {
		return err
	}
	filteredRelativePaths := make([]string, 0)
	tracelog.InfoLogger.Println("Objects in folder:")
	for _, object := range relativePathObjects {
		if filter(object) {
			tracelog.InfoLogger.Println("\twill be deleted: " + object.GetName())
			filteredRelativePaths = append(filteredRelativePaths, object.GetName())
		} else {
			tracelog.DebugLogger.Println("\tskipped: " + object.GetName())
		}
	}
	if len(filteredRelativePaths) == 0 {
		return nil
	}
	if confirm {
		return folder.DeleteObjects(filteredRelativePaths)
	} else {
		tracelog.InfoLogger.Println("Dry run, nothing were deleted")
	}
	return nil
}

func ListFolderRecursively(folder Folder) (relativePathObjects []Object, err error) {
	queue := make([]Folder, 0)
	queue = append(queue, folder)
	for len(queue) > 0 {
		subFolder := queue[0]
		queue = queue[1:]
		objects, subFolders, err := subFolder.ListFolder()
		folderPrefix := strings.TrimPrefix(subFolder.GetPath(), folder.GetPath())
		relativePathObjects = append(relativePathObjects, addPrefixToNames(objects, folderPrefix)...)
		if err != nil {
			return nil, err
		}
		queue = append(queue, subFolders...)
	}
	return relativePathObjects, nil
}

func addPrefixToNames(objects []Object, folderPrefix string) []Object {
	relativePathObjects := make([]Object, len(objects))
	for i, object := range objects {
		relativePath := path.Join(folderPrefix, object.GetName())
		relativePathObjects[i] = NewLocalObject(relativePath, object.GetLastModified())
	}
	return relativePathObjects
}
