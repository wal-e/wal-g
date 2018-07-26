package walg

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"io"
	"log"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

// Uploader contains fields associated with uploading tarballs.
// Multiple tarballs can share one uploader. Must call CreateUploader()
// in 'upload.go'.
type Uploader struct {
	UploaderApi          s3manageriface.UploaderAPI
	UploadingLocation    *S3Folder
	ServerSideEncryption string
	SSEKMSKeyId          string
	StorageClass         string
	Success              bool
	compressor           Compressor
	waitGroup            *sync.WaitGroup
}

// NewUploader creates a new tar uploader without the actual
// S3 uploader. CreateUploader() is used to configure byte size and
// concurrency streams for the uploader.
func NewUploader(compressionMethod string, uploadingLocation *S3Folder) *Uploader {
	return &Uploader{
		UploadingLocation: uploadingLocation,
		StorageClass:      "STANDARD",
		compressor:        Compressors[compressionMethod],
		waitGroup:         &sync.WaitGroup{},
	}
}

// Finish waits for all waiting parts to be uploaded. If an error occurs,
// prints alert to stderr.
func (uploader *Uploader) Finish() {
	uploader.waitGroup.Wait()
	if !uploader.Success {
		log.Printf("WAL-G could not complete upload.\n")
	}
}

// Clone creates similar Uploader with new WaitGroup
func (uploader *Uploader) Clone() *Uploader {
	return &Uploader{
		uploader.UploaderApi,
		uploader.UploadingLocation,
		uploader.ServerSideEncryption,
		uploader.SSEKMSKeyId,
		uploader.StorageClass,
		uploader.Success,
		uploader.compressor,
		&sync.WaitGroup{},
	}
}

// UploadWal compresses a WAL file and uploads to S3. Returns
// the first error encountered and an empty string upon failure.
func (uploader *Uploader) UploadWal(file NamedReader, s3Prefix *S3Folder, verify bool) (string, error) {
	var walFileReader io.Reader

	filename := path.Base(file.Name())
	if isWalFilename(filename) {
		recordingReader, err := NewWalDeltaRecordingReader(file, filename, s3Prefix, uploader.Clone())
		if err != nil {
			walFileReader = file
		} else {
			walFileReader = recordingReader
			defer recordingReader.Close()
		}
	} else {
		walFileReader = file
	}

	pipeWriter := &CompressingPipeWriter{
		Input:                walFileReader,
		NewCompressingWriter: uploader.compressor.NewWriter,
	}

	pipeWriter.Compress(&OpenPGPCrypter{})

	dstPath := sanitizePath(*uploader.UploadingLocation.Server + WalPath + filepath.Base(file.Name()) + "." + uploader.compressor.FileExtension())
	reader := pipeWriter.Output

	if verify {
		reader = newMd5Reader(reader)
	}

	input := uploader.CreateUploadInput(dstPath, reader)

	var err error
	uploader.waitGroup.Add(1)
	go func() {
		defer uploader.waitGroup.Done()
		err = uploader.upload(input, file.Name())
	}()

	uploader.Finish()
	fmt.Println("WAL PATH:", dstPath)
	if verify {
		sum := reader.(*MD5Reader).Sum()
		archive := &Archive{
			Prefix:  s3Prefix,
			Archive: aws.String(dstPath),
		}
		eTag, err := archive.GetETag()
		if err != nil {
			log.Fatalf("Unable to verify WAL %s", err)
		}
		if eTag == nil {
			log.Fatalf("Unable to verify WAL: nil ETag ")
		}

		trimETag := strings.Trim(*eTag, "\"")
		if sum != trimETag {
			log.Fatalf("WAL verification failed: md5 %s ETag %s", sum, trimETag)
		}
		fmt.Println("ETag ", trimETag)
	}
	return dstPath, err
}

// CreateUploadInput creates a s3manager.UploadInput for a Uploader using
// the specified path and reader.
func (uploader *Uploader) CreateUploadInput(path string, reader io.Reader) *s3manager.UploadInput {
	uploadInput := &s3manager.UploadInput{
		Bucket:       uploader.UploadingLocation.Bucket,
		Key:          aws.String(path),
		Body:         reader,
		StorageClass: aws.String(uploader.StorageClass),
	}

	if uploader.ServerSideEncryption != "" {
		uploadInput.ServerSideEncryption = aws.String(uploader.ServerSideEncryption)

		if uploader.SSEKMSKeyId != "" {
			// Only aws:kms implies sseKmsKeyId, checked during validation
			uploadInput.SSEKMSKeyId = aws.String(uploader.SSEKMSKeyId)
		}
	}

	return uploadInput
}

// Helper function to upload to S3. If an error occurs during upload, retries will
// occur in exponentially incremental seconds.
func (uploader *Uploader) upload(input *s3manager.UploadInput, path string) (err error) {
	upl := uploader.UploaderApi

	_, e := upl.Upload(input)
	if e == nil {
		uploader.Success = true
		return nil
	}

	if multierr, ok := e.(s3manager.MultiUploadFailure); ok {
		log.Printf("upload: failed to upload '%s' with UploadID '%s'.", path, multierr.UploadID())
	} else {
		log.Printf("upload: failed to upload '%s': %s.", path, e.Error())
	}
	return e
}
