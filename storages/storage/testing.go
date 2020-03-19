package storage

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"strings"
	"testing"
)

func RunFolderTest(storageFolder Folder, t *testing.T) {
	sub1 := storageFolder.GetSubFolder("Sub1")

	token := make([]byte, 1024*1024) //Send 1 Mb
	rand.Read(token)

	err := storageFolder.PutObject("file0", bytes.NewBuffer(token))
	assert.NoError(t, err)

	readCloser, err := storageFolder.ReadObject("file0")
	assert.NoError(t, err)
	all, err := ioutil.ReadAll(readCloser)
	assert.NoError(t, err)
	assert.Equal(t, all, token)

	err = sub1.PutObject("file1", strings.NewReader("data1"))
	assert.NoError(t, err)

	b, err := storageFolder.Exists("file0")
	assert.NoError(t, err)
	assert.True(t, b)
	b, err = sub1.Exists("file1")
	assert.NoError(t, err)
	assert.True(t, b)

	objects, subFolders, err := storageFolder.ListFolder()
	assert.NoError(t, err)
	t.Log(subFolders[0].GetPath())
	assert.Equal(t, objects[0].GetName(), "file0")
	assert.True(t, strings.HasSuffix(subFolders[0].GetPath(), "Sub1/"))

	sublist, subFolders, err := sub1.ListFolder()
	assert.NoError(t, err)
	assert.Equal(t, len(subFolders), 0)
	assert.Equal(t, len(sublist), 1)
	assert.Equal(t, sublist[0].GetName(), "file1")

	data, err := sub1.ReadObject("file1")
	assert.NoError(t, err)
	data0Str, err := ioutil.ReadAll(data)
	assert.NoError(t, err)
	assert.Equal(t, "data1", string(data0Str))
	err = data.Close()
	assert.NoError(t, err)

	err = sub1.DeleteObjects([]string{"file1"})
	assert.NoError(t, err)
	err = storageFolder.DeleteObjects([]string{"Sub1"})
	assert.NoError(t, err)
	err = storageFolder.DeleteObjects([]string{"file0"})
	assert.NoError(t, err)

	b, err = storageFolder.Exists("file0")
	assert.NoError(t, err)
	assert.False(t, b)
	b, err = sub1.Exists("file1")
	assert.NoError(t, err)
	assert.False(t, b)

	_, err = sub1.ReadObject("Tumba Yumba")
	assert.Error(t, err.(ObjectNotFoundError))
}
