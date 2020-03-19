package fs

import (
	"github.com/wal-g/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"io"
	"io/ioutil"
	"os"
	"path"
)

const dirDefaultMode = 0755

func NewError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "FS", format, args...)
}

// Folder represents folder of file system
type Folder struct {
	rootPath string
	subpath  string
}

func NewFolder(rootPath string, subPath string) *Folder {
	return &Folder{rootPath, subPath}
}

func ConfigureFolder(path string, settings map[string]string) (storage.Folder, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, NewError(err, "Folder not exists or is inaccessible")
	}
	return NewFolder(path, ""), nil
}

func (folder *Folder) GetPath() string {
	return folder.subpath
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	files, err := ioutil.ReadDir(path.Join(folder.rootPath, folder.subpath))
	if err != nil {
		return nil, nil, NewError(err, "Unable to read folder")
	}
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			// I do not use GetSubfolder() intentially
			subPath := path.Join(folder.subpath, fileInfo.Name()) + "/"
			subFolders = append(subFolders, NewFolder(folder.rootPath, subPath))
		} else {
			objects = append(objects, storage.NewLocalObject(fileInfo.Name(), fileInfo.ModTime()))
		}
	}
	return
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	for _, fileName := range objectRelativePaths {
		err := os.RemoveAll(folder.GetFilePath(fileName))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return NewError(err, "Unable to delete object %v", fileName)
		}
	}
	return nil
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	_, err := os.Stat(folder.GetFilePath(objectRelativePath))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, NewError(err, "Unable to stat object %v", objectRelativePath)
	}
	return true, nil

}
func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	sf := Folder{folder.rootPath, path.Join(folder.subpath, subFolderRelativePath)}
	_ = sf.EnsureExists()

	// This is something unusual when we cannot be sure that our subfolder exists in FS
	// But we do not have to guarantee folder persistence, but any subsequent calls will fail
	// Just like in all other Storage Folders
	return &sf
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	filePath := folder.GetFilePath(objectRelativePath)
	file, err := os.Open(filePath)
	if os.IsNotExist(err) {
		return nil, storage.NewObjectNotFoundError(filePath)
	}
	if err != nil {
		return nil, NewError(err, "Unable to read object %v", filePath)
	}
	return file, nil
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.subpath)
	filePath := folder.GetFilePath(name)
	file, err := OpenFileWithDir(filePath)
	if err != nil {
		return NewError(err, "Unable to open file %v", filePath)
	}
	_, err = io.Copy(file, content)
	if err != nil {
		return NewError(err, "Unable to copy data to %v", filePath)
	}
	err = file.Close()
	if err != nil {
		return NewError(err, "Unable to close %v", filePath)
	}
	return nil
}

func OpenFileWithDir(filePath string) (*os.File, error) {
	file, err := os.Create(filePath)
	if os.IsNotExist(err) {
		err = os.MkdirAll(path.Dir(filePath), dirDefaultMode)
		if err != nil {
			return nil, err
		}
		file, err = os.Create(filePath)
	}
	return file, err
}

func (folder *Folder) GetFilePath(objectRelativePath string) string {
	return path.Join(folder.rootPath, folder.subpath, objectRelativePath)
}

func (folder *Folder) EnsureExists() error {
	dirname := path.Join(folder.rootPath, folder.subpath)
	_, err := os.Stat(dirname)
	if os.IsNotExist(err) {
		return os.MkdirAll(dirname, dirDefaultMode)

	}
	return err
}
