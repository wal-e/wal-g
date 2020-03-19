package sh

import (
	"github.com/pkg/sftp"
	"io"
	"os"
)

type SftpClient interface {
	Lock()
	Unlock()
	ReadDir(path string) ([]os.FileInfo, error)
	Join(elem ...string) string
	Remove(path string) error
	Stat(p string) (os.FileInfo, error)
	Open(path string) (io.ReadCloser, error)
	Create(path string) (io.Writer, error)
}

type extendedSftpClient struct {
	*sftp.Client
}

func (client *extendedSftpClient) Open(path string) (io.ReadCloser, error) {
	return client.Open(path)
}

func (client *extendedSftpClient) Create(path string) (io.Writer, error) {
	return client.Create(path)
}

func extend(client *sftp.Client) *extendedSftpClient {
	return &extendedSftpClient{client}
}