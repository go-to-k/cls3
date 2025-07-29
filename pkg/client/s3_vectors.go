//go:generate mockgen -source=$GOFILE -destination=mock_$GOFILE -package=$GOPACKAGE -write_package_comment=false
package client

import (
	"context"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/types"
)

var SleepTimeSecForS3Vectors = 20

type ListIndexesByPageOutput struct {
	Indexes   []types.IndexSummary
	NextToken *string
}

type IS3Vectors interface {
	DeleteVectorBucket(ctx context.Context, vectorBucketName *string) error
	DeleteIndex(ctx context.Context, indexName *string, vectorBucketName *string) error
	ListVectorBuckets(ctx context.Context) ([]types.VectorBucketSummary, error)
	ListIndexesByPage(ctx context.Context, vectorBucketName *string, nextToken *string, keyPrefix *string) (*ListIndexesByPageOutput, error)
}

var _ IS3Vectors = (*S3Vectors)(nil)

type S3Vectors struct {
	client  *s3vectors.Client
	retryer *Retryer
}

func NewS3Vectors(client *s3vectors.Client) *S3Vectors {
	retryable := func(err error) bool {
		isRetryable :=
			strings.Contains(err.Error(), "api error SlowDown") ||
				strings.Contains(err.Error(), "An internal error occurred. Try again.") ||
				strings.Contains(err.Error(), "StatusCode: 429") ||
				strings.Contains(err.Error(), "StatusCode: 503") ||
				strings.Contains(err.Error(), "Please try again")

		return isRetryable
	}
	retryer := NewRetryer(retryable, SleepTimeSecForS3Vectors)

	return &S3Vectors{
		client,
		retryer,
	}
}

func (s *S3Vectors) DeleteVectorBucket(ctx context.Context, vectorBucketName *string) error {
	input := &s3vectors.DeleteVectorBucketInput{
		VectorBucketName: vectorBucketName,
	}

	optFn := func(o *s3vectors.Options) {
		o.Retryer = s.retryer
	}

	_, err := s.client.DeleteVectorBucket(ctx, input, optFn)
	if err != nil {
		return &ClientError{
			ResourceName: vectorBucketName,
			Err:          err,
		}
	}
	return nil
}

func (s *S3Vectors) DeleteIndex(ctx context.Context, indexName *string, vectorBucketName *string) error {
	input := &s3vectors.DeleteIndexInput{
		IndexName:        indexName,
		VectorBucketName: vectorBucketName,
	}

	optFn := func(o *s3vectors.Options) {
		o.Retryer = s.retryer
	}

	_, err := s.client.DeleteIndex(ctx, input, optFn)
	if err != nil {
		return &ClientError{
			ResourceName: aws.String(*vectorBucketName + "/" + *indexName),
			Err:          err,
		}
	}
	return nil
}

func (s *S3Vectors) ListVectorBuckets(ctx context.Context) ([]types.VectorBucketSummary, error) {
	buckets := []types.VectorBucketSummary{}
	var nextToken *string

	for {
		select {
		case <-ctx.Done():
			return buckets, &ClientError{
				Err: ctx.Err(),
			}
		default:
		}

		input := &s3vectors.ListVectorBucketsInput{
			NextToken: nextToken,
		}

		optFn := func(o *s3vectors.Options) {
			o.Retryer = s.retryer
		}

		output, err := s.client.ListVectorBuckets(ctx, input, optFn)
		if err != nil {
			return buckets, &ClientError{
				Err: err,
			}
		}

		buckets = append(buckets, output.VectorBuckets...)

		if output.NextToken == nil {
			break
		}
		nextToken = output.NextToken
	}

	// sort by bucket name
	sort.Slice(buckets, func(i, j int) bool {
		return *buckets[i].VectorBucketName < *buckets[j].VectorBucketName
	})

	return buckets, nil
}

func (s *S3Vectors) ListIndexesByPage(ctx context.Context, vectorBucketName *string, nextToken *string, keyPrefix *string) (*ListIndexesByPageOutput, error) {
	input := &s3vectors.ListIndexesInput{
		VectorBucketName: vectorBucketName,
		NextToken:        nextToken,
	}

	if keyPrefix != nil && *keyPrefix != "" {
		input.Prefix = keyPrefix
	}

	optFn := func(o *s3vectors.Options) {
		o.Retryer = s.retryer
	}

	output, err := s.client.ListIndexes(ctx, input, optFn)
	if err != nil {
		return nil, &ClientError{
			ResourceName: vectorBucketName,
			Err:          err,
		}
	}

	return &ListIndexesByPageOutput{
		Indexes:   output.Indexes,
		NextToken: output.NextToken,
	}, nil
}
