package s3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/storages/storage"
	"io"
	"strings"
)

const (
	NotFoundAWSErrorCode  = "NotFound"
	NoSuchKeyAWSErrorCode = "NoSuchKey"

	EndpointSetting          = "AWS_ENDPOINT"
	RegionSetting            = "AWS_REGION"
	ForcePathStyleSetting    = "AWS_S3_FORCE_PATH_STYLE"
	AccessKeyIdSetting       = "AWS_ACCESS_KEY_ID"
	AccessKeySetting         = "AWS_ACCESS_KEY"
	SecretAccessKeySetting   = "AWS_SECRET_ACCESS_KEY"
	SecretKeySetting         = "AWS_SECRET_KEY"
	SessionTokenSetting      = "AWS_SESSION_TOKEN"
	SseSetting               = "S3_SSE"
	SseKmsIdSetting          = "S3_SSE_KMS_ID"
	StorageClassSetting      = "S3_STORAGE_CLASS"
	UploadConcurrencySetting = "UPLOAD_CONCURRENCY"
	s3CertFile               = "S3_CA_CERT_FILE"
	MaxPartSize              = "S3_MAX_PART_SIZE"
	EndpointSourceSetting    = "S3_ENDPOINT_SOURCE"
	EndpointPortSetting      = "S3_ENDPOINT_PORT"
)

var (
	// MaxRetries limit upload and download retries during interaction with S3
	MaxRetries  = 15
	SettingList = []string{
		EndpointPortSetting,
		EndpointSetting,
		EndpointSourceSetting,
		RegionSetting,
		ForcePathStyleSetting,
		AccessKeyIdSetting,
		AccessKeySetting,
		SecretAccessKeySetting,
		SecretKeySetting,
		SessionTokenSetting,
		SseSetting,
		SseKmsIdSetting,
		StorageClassSetting,
		UploadConcurrencySetting,
		s3CertFile,
		MaxPartSize,
	}
)

func getFirstSettingOf(settings map[string]string, keys []string) string {
	for _, key := range keys {
		if value, ok := settings[key]; ok {
			return value
		}
	}
	return ""
}

func NewFolderError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "S3", format, args...)
}

func NewConfiguringError(settingName string) storage.Error {
	return NewFolderError(errors.New("Configuring error"),
		"%s setting is not set", settingName)
}

type Folder struct {
	uploader Uploader
	S3API    s3iface.S3API
	Bucket   *string
	Path     string
	settings map[string]string
}

func NewFolder(uploader Uploader, s3API s3iface.S3API, bucket, path string) *Folder {
	return &Folder{
		uploader: uploader,
		S3API:    s3API,
		Bucket:   aws.String(bucket),
		Path:     storage.AddDelimiterToPath(path),
	}
}

func ConfigureFolder(prefix string, settings map[string]string) (storage.Folder, error) {
	bucket, path, err := storage.GetPathFromPrefix(prefix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure S3 path")
	}
	sess, err := createSession(bucket, settings)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new session")
	}
	client := s3.New(sess)
	uploader, err := configureUploader(client, settings)
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure S3 uploader")
	}
	folder := NewFolder(*uploader, client, bucket, path)
	folder.settings = settings
	return folder, nil
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	objectPath := folder.Path + objectRelativePath
	stopSentinelObjectInput := &s3.HeadObjectInput{
		Bucket: folder.Bucket,
		Key:    aws.String(objectPath),
	}

	_, err := folder.S3API.HeadObject(stopSentinelObjectInput)
	if err != nil {
		if isAwsNotExist(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "failed to check s3 object '%s' existance", objectPath)
	}
	return true, nil
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	return folder.uploader.upload(*folder.Bucket, folder.Path+name, content)
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	objectPath := folder.Path + objectRelativePath
	input := &s3.GetObjectInput{
		Bucket: folder.Bucket,
		Key:    aws.String(objectPath),
	}

	object, err := folder.S3API.GetObject(input)
	if err != nil {
		if isAwsNotExist(err) {
			return nil, storage.NewObjectNotFoundError(objectPath)
		}
		return nil, errors.Wrapf(err, "failed to read object: '%s' from S3", objectPath)
	}
	return object.Body, nil
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(folder.uploader, folder.S3API, *folder.Bucket, storage.JoinPath(folder.Path, subFolderRelativePath)+"/")
}

func (folder *Folder) GetPath() string {
	return folder.Path
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	s3Objects := &s3.ListObjectsV2Input{
		Bucket:    folder.Bucket,
		Prefix:    aws.String(folder.Path),
		Delimiter: aws.String("/"),
	}

	err = folder.S3API.ListObjectsV2Pages(s3Objects, func(files *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, prefix := range files.CommonPrefixes {
			subFolders = append(subFolders, NewFolder(folder.uploader, folder.S3API, *folder.Bucket, *prefix.Prefix))
		}
		for _, object := range files.Contents {
			// Some storages return root tar_partitions folder as a Key.
			// We do not want to fail restoration due to this fact.
			// Keep in mind that skipping files is very dangerous and any decision here must be weighted.
			if *object.Key == folder.Path {
				continue
			}
			objectRelativePath := strings.TrimPrefix(*object.Key, folder.Path)
			objects = append(objects, storage.NewLocalObject(objectRelativePath, *object.LastModified))
		}
		return true
	})
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to list s3 folder: '%s'", folder.Path)
	}
	return objects, subFolders, nil
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	parts := partitionStrings(objectRelativePaths, 1000)
	for _, part := range parts {
		input := &s3.DeleteObjectsInput{Bucket: folder.Bucket, Delete: &s3.Delete{
			Objects: folder.partitionToObjects(part),
		}}
		_, err := folder.S3API.DeleteObjects(input)
		if err != nil {
			return errors.Wrapf(err, "failed to delete s3 object: '%s'", part)
		}
	}
	return nil
}

func (folder *Folder) partitionToObjects(keys []string) []*s3.ObjectIdentifier {
	objects := make([]*s3.ObjectIdentifier, len(keys))
	for id, key := range keys {
		objects[id] = &s3.ObjectIdentifier{Key: aws.String(folder.Path + key)}
	}
	return objects
}

func isAwsNotExist(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		if awsErr.Code() == NotFoundAWSErrorCode || awsErr.Code() == NoSuchKeyAWSErrorCode {
			return true
		}
	}
	return false
}
