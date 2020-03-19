package sh

import (
	"github.com/pkg/sftp"
	"github.com/wal-g/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"golang.org/x/crypto/ssh"
	"io"
)

type Folder struct {
	client SftpClient
	path string
}

func NewFolderError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "SSH", format, args...)
}

func ConfigureFolder(prefix string, settings map[string]string) (storage.Folder, error) {
	host, path, err := storage.GetPathFromPrefix(prefix)

	if err != nil {
		return nil, err
	}

	// TODO parse from settings
	user := "user"
	pass := ""
	port := ":22"

	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(pass),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	address := host + port
	sshClient, err := ssh.Dial("tcp", address, config)
	if err != nil {
		return nil, NewFolderError(err, "Fail connect via ssh. Address: %s", address)
	}

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		return nil, NewFolderError(err, "Fail connect via sftp. Address: %s", address)
	}

	return &Folder{
		extend(sftpClient), path,
	}, nil
}

// TODO close ssh and sftp connection
func closeConnection(client io.Closer)  {
	err := client.Close()
	if err != nil {
		tracelog.WarningLogger.FatalOnError(err)
	}
}

func (folder *Folder) GetPath() string {
	return folder.path
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	client := folder.client
	path := folder.path

	filesInfo, err := client.ReadDir(folder.path)

	if err != nil {
		return nil, nil,
			NewFolderError(err, "Fail read folder '%s'", path)
	}

	for _, fileInfo := range filesInfo {
		if fileInfo.IsDir() {
			folder := &Folder{
				folder.client,
				client.Join(path, fileInfo.Name()),
			}
			subFolders = append(subFolders, folder)
		}

		object := storage.NewLocalObject(
			fileInfo.Name(),
			fileInfo.ModTime(),
		)
		objects = append(objects, object)
	}

	return
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	client := folder.client

	for _, relativePath := range objectRelativePaths {
		path := client.Join(folder.path, relativePath)

		err := client.Remove(path)
		if err != nil {
			return NewFolderError(err, "Fail delete object '%s'", path)
		}
	}

	return nil
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error)  {
	path := folder.client.Join()
	_, err := folder.client.Stat(path)

	if err != nil {
		return false, NewFolderError(
			err, "Fail check object existence '%s'", path,
		)
	}

	return true, nil
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return &Folder{
		folder.client,
		folder.client.Join(folder.path, subFolderRelativePath),
	}
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	path := folder.client.Join(folder.path, objectRelativePath)
	file, err := folder.client.Open(path)

	if err != nil {
		return nil, NewFolderError(err, "Fail open file '%s'", path)
	}

	return file, nil
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	client := folder.client
	path := client.Join(folder.path, name)

	file, err := client.Create(path)
	if err != nil {
		return NewFolderError(err, "Fail create file '%s'", path)
	}

	_, err = io.Copy(file, content)
	if err != nil {
		return NewFolderError(err, "Fail write content to file '%s'", path)
	}

	return nil
}




