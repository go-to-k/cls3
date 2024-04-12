//go:generate mockgen -source=$GOFILE -destination=s3_mock.go -package=$GOPACKAGE -write_package_comment=false
package client

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var SleepTimeSecForS3 = 10

type IS3 interface {
	DeleteBucket(ctx context.Context, bucketName *string, region string) error
	DeleteObjects(ctx context.Context, bucketName *string, objects []types.ObjectIdentifier, region string, quiet bool) ([]types.Error, error)
	ListObjectVersions(ctx context.Context, bucketName *string, region string, oldVersionsOnly bool) ([]types.ObjectIdentifier, error)
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
	if len(objects) == 0 {
		return []types.Error{}, nil
	}

	input := &s3.DeleteObjectsInput{
		Bucket: bucketName,
		Delete: &types.Delete{
			Objects: objects,
			Quiet:   aws.Bool(true),
		},
	}

	retryable := func(err error) bool {
		return strings.Contains(err.Error(), "api error SlowDown")
	}
	optFn := func(o *s3.Options) {
		o.Retryer = NewRetryer(retryable, SleepTimeSecForS3)
		o.Region = region
	}

	output, err := s.client.DeleteObjects(ctx, input, optFn)
	if err != nil {
		return []types.Error{}, &ClientError{
			ResourceName: bucketName,
			Err:          err,
		}
	}

	return output.Errors, nil
}

func (s *S3) ListObjectVersions(ctx context.Context, bucketName *string, region string, oldVersionsOnly bool) ([]types.ObjectIdentifier, error) {
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

		keyMarker = output.NextKeyMarker
		versionIdMarker = output.NextVersionIdMarker

		if keyMarker == nil && versionIdMarker == nil {
			break
		}
	}

	return objectIdentifiers, nil
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
	input := &s3.ListObjectVersionsInput{
		Bucket:          bucketName,
		KeyMarker:       keyMarker,
		VersionIdMarker: versionIdMarker,
	}

	output, err := s.client.ListObjectVersions(ctx, input, func(o *s3.Options) {
		o.Region = region
	})
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
