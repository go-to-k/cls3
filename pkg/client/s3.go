//go:generate mockgen -source=$GOFILE -destination=mock_$GOFILE -package=$GOPACKAGE -write_package_comment=false
package client

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-to-k/cls3/pkg/endpoint"
)

var SleepTimeSecForS3 = 20

type ListObjectsOrVersionsByPageOutput struct {
	ObjectIdentifiers   []types.ObjectIdentifier
	NextKeyMarker       *string
	NextVersionIdMarker *string
}
type listObjectVersionsByPageOutput struct {
	ObjectIdentifiers   []types.ObjectIdentifier
	NextKeyMarker       *string
	NextVersionIdMarker *string
}
type listObjectsByPageOutput struct {
	ObjectIdentifiers []types.ObjectIdentifier
	NextToken         *string
}

type IS3 interface {
	DeleteBucket(ctx context.Context, bucketName *string, region string) error
	DeleteObjects(
		ctx context.Context,
		bucketName *string,
		objects []types.ObjectIdentifier,
		region string,
	) ([]types.Error, error)
	ListObjectsOrVersionsByPage(
		ctx context.Context,
		bucketName *string,
		region string,
		oldVersionsOnly bool,
		keyMarker *string,
		versionIdMarker *string,
		keyPrefix *string,
	) (*ListObjectsOrVersionsByPageOutput, error)
	ListBucketsOrDirectoryBuckets(ctx context.Context) ([]types.Bucket, error)
	GetBucketLocation(ctx context.Context, bucketName *string) (string, error)
}

var _ IS3 = (*S3)(nil)

type S3 struct {
	client               *s3.Client
	directoryBucketsMode bool
	retryer              *Retryer
}

func NewS3(client *s3.Client, directoryBucketsMode bool) *S3 {
	retryable := func(err error) bool {
		isRetryable :=
			strings.Contains(err.Error(), "api error SlowDown") ||
				// See: https://github.com/go-to-k/cls3/issues/194
				strings.Contains(err.Error(), "https response error StatusCode: 0") ||
				// ex: ERR [resource par-cls-019] operation error S3: DeleteObjects, https response error StatusCode: 0, RequestID: , HostID: , request send failed, Post "https://par-cls-019.s3.us-east-1.amazonaws.com/?delete=": EOF
				// but one condition above, it didn't catch on.
				strings.Contains(err.Error(), "EOF") ||
				// ex: ERR [resource cls3-test-xxx] operation error S3: ListObjectVersions, https response error StatusCode: 500, RequestID: xxxxxx, HostID: xxxxxx=, api error InternalError: We encountered an internal error. Please try again.
				strings.Contains(err.Error(), "Please try again")

		return isRetryable
	}
	retryer := NewRetryer(retryable, SleepTimeSecForS3)

	return &S3{
		client,
		directoryBucketsMode,
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
		select {
		case <-ctx.Done():
			return errors, &ClientError{
				ResourceName: bucketName,
				Err:          ctx.Err(),
			}
		default:
		}

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
			return errors, &ClientError{
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

func (s *S3) ListObjectsOrVersionsByPage(
	ctx context.Context,
	bucketName *string,
	region string,
	oldVersionsOnly bool,
	keyMarker *string,
	versionIdMarker *string,
	keyPrefix *string,
) (*ListObjectsOrVersionsByPageOutput, error) {
	var objectIdentifiers []types.ObjectIdentifier
	var nextKeyMarker *string
	var nextVersionIdMarker *string

	if !s.supportsVersions() {
		output, err := s.listObjectsByPage(ctx, bucketName, region, keyMarker, keyPrefix)
		if err != nil {
			return nil, err
		}

		objectIdentifiers = output.ObjectIdentifiers
		nextKeyMarker = output.NextToken
	} else {
		output, err := s.listObjectVersionsByPage(ctx, bucketName, region, oldVersionsOnly, keyMarker, versionIdMarker, keyPrefix)
		if err != nil {
			return nil, err
		}

		objectIdentifiers = output.ObjectIdentifiers
		nextKeyMarker = output.NextKeyMarker
		nextVersionIdMarker = output.NextVersionIdMarker
	}

	return &ListObjectsOrVersionsByPageOutput{
		ObjectIdentifiers:   objectIdentifiers,
		NextKeyMarker:       nextKeyMarker,
		NextVersionIdMarker: nextVersionIdMarker,
	}, nil
}

func (s *S3) listObjectVersionsByPage(
	ctx context.Context,
	bucketName *string,
	region string,
	oldVersionsOnly bool,
	keyMarker *string,
	versionIdMarker *string,
	keyPrefix *string,
) (*listObjectVersionsByPageOutput, error) {
	objectIdentifiers := []types.ObjectIdentifier{}
	input := &s3.ListObjectVersionsInput{
		Bucket:          bucketName,
		KeyMarker:       keyMarker,
		VersionIdMarker: versionIdMarker,
		Prefix:          keyPrefix,
	}

	optFn := func(o *s3.Options) {
		o.Retryer = s.retryer
		if region != "" {
			o.Region = region
		}
	}

	output, err := s.client.ListObjectVersions(ctx, input, optFn)
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

	return &listObjectVersionsByPageOutput{
		ObjectIdentifiers:   objectIdentifiers,
		NextKeyMarker:       output.NextKeyMarker,
		NextVersionIdMarker: output.NextVersionIdMarker,
	}, nil
}

func (s *S3) listObjectsByPage(
	ctx context.Context,
	bucketName *string,
	region string,
	token *string,
	keyPrefix *string,
) (*listObjectsByPageOutput, error) {
	objectIdentifiers := []types.ObjectIdentifier{}
	input := &s3.ListObjectsV2Input{
		Bucket:            bucketName,
		ContinuationToken: token,
		Prefix:            keyPrefix,
	}

	optFn := func(o *s3.Options) {
		o.Retryer = s.retryer
		if region != "" {
			o.Region = region
		}
	}

	output, err := s.client.ListObjectsV2(ctx, input, optFn)
	if err != nil {
		return nil, &ClientError{
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

	return &listObjectsByPageOutput{
		ObjectIdentifiers: objectIdentifiers,
		NextToken:         output.NextContinuationToken,
	}, nil
}

func (s *S3) ListBucketsOrDirectoryBuckets(ctx context.Context) ([]types.Bucket, error) {
	var listBucketsFunc func(ctx context.Context) ([]types.Bucket, error)

	if s.directoryBucketsMode {
		listBucketsFunc = s.listDirectoryBuckets
	} else {
		listBucketsFunc = s.listBuckets
	}

	buckets, err := listBucketsFunc(ctx)
	if err != nil {
		return []types.Bucket{}, err
	}
	return buckets, nil
}

func (s *S3) listBuckets(ctx context.Context) ([]types.Bucket, error) {
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

		input := &s3.ListBucketsInput{
			ContinuationToken: continuationToken,
		}

		optFn := func(o *s3.Options) {
			o.Retryer = s.retryer
		}

		output, err := s.client.ListBuckets(ctx, input, optFn)
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

func (s *S3) listDirectoryBuckets(ctx context.Context) ([]types.Bucket, error) {
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

	// sort by bucket name
	sort.Slice(buckets, func(i, j int) bool {
		return *buckets[i].Name < *buckets[j].Name
	})

	return buckets, nil
}

func (s *S3) GetBucketLocation(ctx context.Context, bucketName *string) (string, error) {
	// The return string value allows buckets outside the specified region to be deleted.
	// If the `directoryBucketsMode` is true, the value is empty because only one region's
	// buckets can be operated on.
	if s.directoryBucketsMode {
		return "", nil
	}

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

func (s *S3) supportsVersions() bool {
	if s.directoryBucketsMode {
		return false
	}

	baseEndpoint := s.client.Options().BaseEndpoint
	if baseEndpoint == nil || endpoint.IsAWSS3Endpoint(*baseEndpoint) {
		// Standard AWS S3 supports versioning
		return true
	}

	if endpoint.IsCloudflareR2Endpoint(*baseEndpoint) {
		return false
	}

	// For other custom endpoints, assume versioning is supported
	return true
}
