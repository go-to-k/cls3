//go:generate mockgen -source=$GOFILE -destination=s3_mock.go -package=$GOPACKAGE -write_package_comment=false
package client

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	S3DeleteObjectsSizeLimit = 1000

	// S3 API can achieve at least 3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD requests per second per partitioned prefix.
	// Values above that threshold cause many 503 errors.
	// So limit DeleteObjects to 3 parallels of 1000 objects at a time.
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
	MaxS3DeleteObjectsParallelsCount = 3
)

var SleepTimeSecForS3 = 10

type IS3 interface {
	DeleteBucket(ctx context.Context, bucketName *string, region string) error
	DeleteObjects(ctx context.Context, bucketName *string, objects []types.ObjectIdentifier, region string, quiet bool) ([]types.Error, error)
	ListObjectVersions(ctx context.Context, bucketName *string, region string) ([]types.ObjectIdentifier, error)
	CheckBucketExists(ctx context.Context, bucketName *string) (bool, error)
	ListBuckets(ctx context.Context) ([]types.Bucket, error)
	GetBucketLocation(ctx context.Context, bucketName *string) (string, error)
}

var _ IS3 = (*S3)(nil)

type S3 struct {
	client *s3.Client
}

func NewS3(client *s3.Client) *S3 {
	return &S3{
		client,
	}
}

func (s *S3) DeleteBucket(ctx context.Context, bucketName *string, region string) error {
	input := &s3.DeleteBucketInput{
		Bucket: bucketName,
	}

	_, err := s.client.DeleteBucket(ctx, input, func(o *s3.Options) {
		o.Region = region
	})
	if err != nil {
		return &ClientError{
			ResourceName: bucketName,
			Err:          err,
		}
	}
	return nil
}

func (s *S3) DeleteObjects(ctx context.Context, bucketName *string, objects []types.ObjectIdentifier, region string, quiet bool) ([]types.Error, error) {
	errors := []types.Error{}
	if len(objects) == 0 {
		return errors, nil
	}

	eg, ctx := errgroup.WithContext(ctx)
	outputsCh := make(chan *s3.DeleteObjectsOutput, MaxS3DeleteObjectsParallelsCount)
	sem := semaphore.NewWeighted(int64(MaxS3DeleteObjectsParallelsCount))
	wg := sync.WaitGroup{}

	var bar *progressbar.ProgressBar
	if !quiet {
		bar = progressbar.NewOptions64(
			int64(len(objects)),
			progressbar.OptionSetWriter(os.Stderr),
			progressbar.OptionSetWidth(50),
			progressbar.OptionThrottle(65*time.Millisecond),
			progressbar.OptionShowCount(),
			progressbar.OptionOnCompletion(func() {
				fmt.Fprint(os.Stderr, "\n")
			}),
			progressbar.OptionSpinnerType(14),
			progressbar.OptionSetRenderBlankState(true),
		)
	}

	nextObjects := make([]types.ObjectIdentifier, len(objects))
	copy(nextObjects, objects)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for outputErrors := range outputsCh {
			outputErrors := outputErrors
			if len(outputErrors.Errors) > 0 {
				errors = append(errors, outputErrors.Errors...)
			}
		}
	}()

	for {
		inputObjects := []types.ObjectIdentifier{}

		if len(nextObjects) > S3DeleteObjectsSizeLimit {
			inputObjects = append(inputObjects, nextObjects[:S3DeleteObjectsSizeLimit]...)
			nextObjects = nextObjects[S3DeleteObjectsSizeLimit:]
		} else {
			inputObjects = append(inputObjects, nextObjects...)
			nextObjects = nil
		}

		input := &s3.DeleteObjectsInput{
			Bucket: bucketName,
			Delete: &types.Delete{
				Objects: inputObjects,
				Quiet:   *aws.Bool(true),
			},
		}

		if err := sem.Acquire(ctx, 1); err != nil {
			return errors, &ClientError{
				ResourceName: bucketName,
				Err:          err,
			}
		}
		eg.Go(func() error {
			defer sem.Release(1)

			retryable := func(err error) bool {
				return strings.Contains(err.Error(), "api error SlowDown")
			}
			optFn := func(o *s3.Options) {
				o.Retryer = NewRetryer(retryable, SleepTimeSecForS3)
				o.Region = region
			}

			output, err := s.client.DeleteObjects(ctx, input, optFn)
			if err != nil {
				return err // return non wrapping error because wrap after eg.Wait()
			}

			if !quiet {
				bar.Add(len(inputObjects))
			}

			outputsCh <- output
			return nil
		})

		if len(nextObjects) == 0 {
			break
		}
	}

	go func() {
		eg.Wait()
		close(outputsCh)
	}()

	if err := eg.Wait(); err != nil {
		return nil, &ClientError{
			ResourceName: bucketName,
			Err:          err,
		}
	}

	// wait errors set before access an errors var at below return (for race)
	wg.Wait()

	return errors, nil
}

func (s *S3) ListObjectVersions(ctx context.Context, bucketName *string, region string) ([]types.ObjectIdentifier, error) {
	var keyMarker *string
	var versionIdMarker *string
	objectIdentifiers := []types.ObjectIdentifier{}

	for {
		select {
		case <-ctx.Done():
			return objectIdentifiers, &ClientError{
				ResourceName: bucketName,
				Err:          ctx.Err(),
			}
		default:
		}

		input := &s3.ListObjectVersionsInput{
			Bucket:          bucketName,
			KeyMarker:       keyMarker,
			VersionIdMarker: versionIdMarker,
		}

		output, err := s.client.ListObjectVersions(ctx, input, func(o *s3.Options) {
			o.Region = region
		})
		if err != nil {
			return nil, &ClientError{
				ResourceName: bucketName,
				Err:          err,
			}
		}

		for _, version := range output.Versions {
			objectIdentifier := types.ObjectIdentifier{
				Key:       version.Key,
				VersionId: version.VersionId,
			}
			objectIdentifiers = append(objectIdentifiers, objectIdentifier)
		}

		for _, deleteMarker := range output.DeleteMarkers {
			objectIdentifier := types.ObjectIdentifier{
				Key:       deleteMarker.Key,
				VersionId: deleteMarker.VersionId,
			}
			objectIdentifiers = append(objectIdentifiers, objectIdentifier)
		}

		keyMarker = output.NextKeyMarker
		versionIdMarker = output.NextVersionIdMarker

		if keyMarker == nil && versionIdMarker == nil {
			break
		}
	}

	return objectIdentifiers, nil
}

func (s *S3) CheckBucketExists(ctx context.Context, bucketName *string) (bool, error) {
	buckets, err := s.ListBuckets(ctx)
	if err != nil {
		return false, err
	}

	for _, bucket := range buckets {
		if *bucket.Name == *bucketName {
			return true, nil
		}
	}

	return false, nil
}

func (s *S3) ListBuckets(ctx context.Context) ([]types.Bucket, error) {
	input := &s3.ListBucketsInput{}

	output, err := s.client.ListBuckets(ctx, input)
	if err != nil {
		return []types.Bucket{}, &ClientError{
			Err: err,
		}
	}

	return output.Buckets, nil
}

func (s *S3) GetBucketLocation(ctx context.Context, bucketName *string) (string, error) {
	input := &s3.GetBucketLocationInput{
		Bucket: bucketName,
	}

	output, err := s.client.GetBucketLocation(ctx, input)
	if err != nil {
		return "", &ClientError{
			ResourceName: bucketName,
			Err:          err,
		}
	}
	if output.LocationConstraint == "" {
		return "us-east-1", nil
	}

	return string(output.LocationConstraint), nil
}
