package testtools

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/wal-g/wal-g/storages/memory"
)

type mockMultiFailureError struct {
	s3manager.MultiUploadFailure
	err awserr.Error
}

func (err mockMultiFailureError) UploadID() string {
	return "mock ID"
}

func (err mockMultiFailureError) Error() string {
	return err.err.Error()
}

// Mock out uploader client for S3. Includes these methods:
// Upload(*UploadInput, ...func(*s3manager.Uploader))
type mockS3Uploader struct {
	s3manageriface.UploaderAPI
	multiErr bool
	err      bool
	storage  *memory.Storage
}

func NewMockS3Uploader(multiErr, err bool, storage *memory.Storage) *mockS3Uploader {
	return &mockS3Uploader{multiErr: multiErr, err: err, storage: storage}
}

func (uploader *mockS3Uploader) Upload(input *s3manager.UploadInput, f ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	if uploader.err {
		return nil, awserr.New("UploadFailed", "mock Upload error", nil)
	}

	if uploader.multiErr {
		e := mockMultiFailureError{
			err: awserr.New("UploadFailed", "multiupload failure error", nil),
		}
		return nil, e
	}

	output := &s3manager.UploadOutput{
		Location:  *input.Bucket,
		VersionID: input.Key,
	}

	var err error
	if uploader.storage == nil {
		// Discard bytes to unblock pipe.
		_, err = io.Copy(ioutil.Discard, input.Body)
	} else {
		var buf bytes.Buffer
		_, err = io.Copy(&buf, input.Body)
		uploader.storage.Store(*input.Bucket+*input.Key, buf)
	}
	if err != nil {
		return nil, err
	}

	return output, nil
}
