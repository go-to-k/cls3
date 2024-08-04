//go:generate mockgen -source=$GOFILE -destination=s3_mock.go -package=$GOPACKAGE -write_package_comment=false
package client

import (
	"context"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var SleepTimeSecForS3 = 10

type IS3 interface {
	DeleteBucket(ctx context.Context, bucketName *string, region string) error
	DeleteObjects(
		ctx context.Context,
		bucketName *string,
		objects []types.ObjectIdentifier,
		region string,
	) ([]types.Error, error)
	ListObjectVersionsByPage(
		ctx context.Context,
		bucketName *string,
		region string,
		oldVersionsOnly bool,
		keyMarker *string,
		versionIdMarker *string,
	) (
		objectIdentifiers []types.ObjectIdentifier,
		nextKeyMarker *string,
		nextVersionIdMarker *string,
		err error,
	)
	ListObjectsByPage(
		ctx context.Context,
		bucketName *string,
		region string,
		marker *string,
	) (
		objectIdentifiers []types.ObjectIdentifier,
		nextMarker *string,
		err error,
	)
	CheckBucketExists(ctx context.Context, bucketName *string, directoryBucketsMode bool) (bool, error)
	ListBuckets(ctx context.Context) ([]types.Bucket, error)
	ListDirectoryBuckets(ctx context.Context) ([]types.Bucket, error)
	GetBucketLocation(ctx context.Context, bucketName *string) (string, error)
}

var _ IS3 = (*S3)(nil)

type S3 struct {
	client  *s3.Client
	retryer *Retryer
}

func NewS3(client *s3.Client) *S3 {
	retryable := func(err error) bool {
		isErrorRetryable := strings.Contains(err.Error(), "api error SlowDown")
		return isErrorRetryable
	}
	retryer := NewRetryer(retryable, SleepTimeSecForS3)

	return &S3{
		client,
		retryer,
	}
}

func (s *S3) DeleteBucket(ctx context.Context, bucketName *string, region string) error {
	input := &s3.DeleteBucketInput{
		Bucket: bucketName,
	}

	optFn := func(o *s3.Options) {
		o.Retryer = s.retryer
		if region != "" {
			o.Region = region
		}
	}

	_, err := s.client.DeleteBucket(ctx, input, optFn)
	if err != nil {
		return &ClientError{
			ResourceName: bucketName,
			Err:          err,
		}
	}
	return nil
}

func (s *S3) DeleteObjects(
	ctx context.Context,
	bucketName *string,
	objects []types.ObjectIdentifier,
	region string,
) ([]types.Error, error) {
	errors := []types.Error{}
	retryCounts := 0

	for {
		// Assuming that the number of objects received as an argument does not
		// exceed 1000, so no slice splitting and validation whether exceeds
		// 1000 or not are good.
		if len(objects) == 0 {
			break
		}

		input := &s3.DeleteObjectsInput{
			Bucket: bucketName,
			Delete: &types.Delete{
				Objects: objects,
				Quiet:   aws.Bool(true),
			},
		}

		optFn := func(o *s3.Options) {
			o.Retryer = s.retryer
			if region != "" {
				o.Region = region
			}
		}

		output, err := s.client.DeleteObjects(ctx, input, optFn)
		if err != nil {
			return []types.Error{}, &ClientError{
				ResourceName: bucketName,
				Err:          err,
			}
		}

		if len(output.Errors) == 0 {
			break
		}

		retryCounts++

		if retryCounts > s.retryer.MaxAttempts() {
			errors = append(errors, output.Errors...)
			break
		}

		objects = []types.ObjectIdentifier{}
		for _, err := range output.Errors {
			// Error example:
			// 	 Code: InternalError
			// 	 Message: We encountered an internal error. Please try again.
			if strings.Contains(*err.Message, "Please try again") {
				objects = append(objects, types.ObjectIdentifier{
					Key:       err.Key,
					VersionId: err.VersionId,
				})
			} else {
				errors = append(errors, err)
			}
		}
		// random sleep
		if len(objects) > 0 {
			sleepTime, _ := s.retryer.RetryDelay(0, nil)
			time.Sleep(sleepTime)
		}
	}

	return errors, nil
}

func (s *S3) ListObjectVersionsByPage(
	ctx context.Context,
	bucketName *string,
	region string,
	oldVersionsOnly bool,
	keyMarker *string,
	versionIdMarker *string,
) (
	objectIdentifiers []types.ObjectIdentifier,
	nextKeyMarker *string,
	nextVersionIdMarker *string,
	err error,
) {
	objectIdentifiers = []types.ObjectIdentifier{}
	input := &s3.ListObjectVersionsInput{
		Bucket:          bucketName,
		KeyMarker:       keyMarker,
		VersionIdMarker: versionIdMarker,
	}

	optFn := func(o *s3.Options) {
		o.Retryer = s.retryer
		if region != "" {
			o.Region = region
		}
	}

	output, err := s.client.ListObjectVersions(ctx, input, optFn)
	if err != nil {
		return nil, nextKeyMarker, nextVersionIdMarker, &ClientError{
			ResourceName: bucketName,
			Err:          err,
		}
	}

	for _, version := range output.Versions {
		if oldVersionsOnly && (version.IsLatest == nil || *version.IsLatest) {
			continue
		}
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

	nextKeyMarker = output.NextKeyMarker
	nextVersionIdMarker = output.NextVersionIdMarker

	return objectIdentifiers, nextKeyMarker, nextVersionIdMarker, nil
}

func (s *S3) ListObjectsByPage(
	ctx context.Context,
	bucketName *string,
	region string,
	token *string,
) (
	objectIdentifiers []types.ObjectIdentifier,
	nextMarker *string,
	err error,
) {
	objectIdentifiers = []types.ObjectIdentifier{}
	input := &s3.ListObjectsV2Input{
		Bucket:            bucketName,
		ContinuationToken: token,
	}

	optFn := func(o *s3.Options) {
		o.Retryer = s.retryer
		if region != "" {
			o.Region = region
		}
	}

	output, err := s.client.ListObjectsV2(ctx, input, optFn)
	if err != nil {
		return nil, nextMarker, &ClientError{
			ResourceName: bucketName,
			Err:          err,
		}
	}

	for _, object := range output.Contents {
		objectIdentifier := types.ObjectIdentifier{
			Key: object.Key,
		}
		objectIdentifiers = append(objectIdentifiers, objectIdentifier)
	}

	return objectIdentifiers, output.NextContinuationToken, nil
}

func (s *S3) CheckBucketExists(ctx context.Context, bucketName *string, directoryBucketsMode bool) (bool, error) {
	var listBucketsFunc func(ctx context.Context) ([]types.Bucket, error)
	if directoryBucketsMode {
		listBucketsFunc = s.ListDirectoryBuckets
	} else {
		listBucketsFunc = s.ListBuckets
	}

	buckets, err := listBucketsFunc(ctx)
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

	optFn := func(o *s3.Options) {
		o.Retryer = s.retryer
	}

	output, err := s.client.ListBuckets(ctx, input, optFn)
	if err != nil {
		return []types.Bucket{}, &ClientError{
			Err: err,
		}
	}

	return output.Buckets, nil
}

func (s *S3) ListDirectoryBuckets(ctx context.Context) ([]types.Bucket, error) {
	buckets := []types.Bucket{}
	var continuationToken *string

	for {
		select {
		case <-ctx.Done():
			return buckets, &ClientError{
				Err: ctx.Err(),
			}
		default:
		}

		input := &s3.ListDirectoryBucketsInput{
			ContinuationToken: continuationToken,
		}

		optFn := func(o *s3.Options) {
			o.Retryer = s.retryer
		}

		output, err := s.client.ListDirectoryBuckets(ctx, input, optFn)
		if err != nil {
			return buckets, &ClientError{
				Err: err,
			}
		}

		buckets = append(buckets, output.Buckets...)

		if output.ContinuationToken == nil {
			break
		}
		continuationToken = output.ContinuationToken
	}

	return buckets, nil
}

func (s *S3) GetBucketLocation(ctx context.Context, bucketName *string) (string, error) {
	input := &s3.GetBucketLocationInput{
		Bucket: bucketName,
	}

	optFn := func(o *s3.Options) {
		o.Retryer = s.retryer
	}

	output, err := s.client.GetBucketLocation(ctx, input, optFn)
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
