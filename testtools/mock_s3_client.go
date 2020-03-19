package testtools

import (
	"io/ioutil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	walgs3 "github.com/wal-g/wal-g/storages/s3"
)

// Mock out S3 client. Includes these methods:
// ListObjects(*ListObjectsV2Input)
// GetObject(*GetObjectInput)
// HeadObject(*HeadObjectInput)
type mockS3Client struct {
	s3iface.S3API
	err      bool
	notFound bool
}

func NewMockS3Client(err, notFound bool) *mockS3Client {
	return &mockS3Client{err: err, notFound: notFound}
}

func (client *mockS3Client) ListObjectsV2Pages(input *s3.ListObjectsV2Input, callback func(*s3.ListObjectsV2Output, bool) bool) error {
	if client.err {
		return awserr.New("MockListObjects", "mock ListObjects errors", nil)
	}

	contents := fakeContents()
	output := &s3.ListObjectsV2Output{
		Contents: contents,
		Name:     input.Bucket,
	}

	callback(output, true)
	return nil
}

func (client *mockS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if client.err {
		return nil, awserr.New("MockGetObject", "mock GetObject error", nil)
	}

	output := &s3.GetObjectOutput{
		Body: ioutil.NopCloser(strings.NewReader("mock content")),
	}

	return output, nil
}

func (client *mockS3Client) HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	if client.err {
		return nil, awserr.New("MockHeadObject", "mock HeadObject error", nil)
	} else if client.notFound {
		return nil, awserr.New(walgs3.NotFoundAWSErrorCode, "mock HeadObject error", nil)
	}

	return &s3.HeadObjectOutput{}, nil
}

// Creates 5 fake S3 objects with Key and LastModified field.
func fakeContents() []*s3.Object {
	c := make([]*s3.Object, 5)

	ob := &s3.Object{
		Key:          aws.String("mockServer/base_backup/second.nop"),
		LastModified: aws.Time(time.Date(2017, 2, 2, 30, 48, 39, 651387233, time.UTC)),
	}
	c[0] = ob

	ob = &s3.Object{
		Key:          aws.String("mockServer/base_backup/fourth.nop"),
		LastModified: aws.Time(time.Date(2009, 2, 27, 20, 8, 33, 651387235, time.UTC)),
	}
	c[1] = ob

	ob = &s3.Object{
		Key:          aws.String("mockServer/base_backup/fifth.nop"),
		LastModified: aws.Time(time.Date(2008, 11, 20, 16, 34, 58, 651387232, time.UTC)),
	}
	c[2] = ob

	ob = &s3.Object{
		Key:          aws.String("mockServer/base_backup/first.nop"),
		LastModified: aws.Time(time.Date(2020, 11, 31, 20, 3, 58, 651387237, time.UTC)),
	}
	c[3] = ob

	ob = &s3.Object{
		Key:          aws.String("mockServer/base_backup/third.nop"),
		LastModified: aws.Time(time.Date(2009, 3, 13, 4, 2, 42, 651387234, time.UTC)),
	}
	c[4] = ob

	return c
}
