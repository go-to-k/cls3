//go:generate mockgen -source=$GOFILE -destination=mock_$GOFILE -package=$GOPACKAGE -write_package_comment=false
package wrapper

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors"
	"github.com/go-to-k/cls3/pkg/client"
)

const SDKRetryMaxAttempts = 3

type IWrapper interface {
	ClearBucket(ctx context.Context, input ClearBucketInput) error
	OutputClearedMessage(bucket string, count int64) error
	OutputDeletedMessage(bucket string) error
	OutputCheckingMessage(bucket string) error
	GetLiveClearingMessage(bucket string, count int64) (string, error)
	GetLiveClearedMessage(bucket string, count int64, isCompleted bool) (string, error)
	ListBucketNamesFilteredByKeyword(ctx context.Context, keyword *string) ([]ListBucketNamesFilteredByKeywordOutput, error)
	CheckAllBucketsExist(ctx context.Context, bucketNames []string) ([]string, error)
}

type ClearBucketInput struct {
	TargetBucket    string // bucket name for S3 and S3Vectors, bucket arn for S3Tables
	ForceMode       bool
	OldVersionsOnly bool
	QuietMode       bool
	ClearingCountCh chan int64
	Prefix          *string // not used for S3Tables
}

type ListBucketNamesFilteredByKeywordOutput struct {
	BucketName   string
	TargetBucket string // bucket name for S3 and S3Vectors, bucket arn for S3Tables
}

type CreateS3WrapperInput struct {
	Region               string
	Profile              string
	EndpointUrl          string
	PathStyle            bool
	TableBucketsMode     bool
	DirectoryBucketsMode bool
	VectorBucketsMode    bool
}

func CreateS3Wrapper(ctx context.Context, input CreateS3WrapperInput) (IWrapper, error) {
	config, err := client.LoadAWSConfig(ctx, input.Region, input.Profile, input.EndpointUrl)
	if err != nil {
		return nil, err
	}

	if input.TableBucketsMode {
		client := client.NewS3Tables(
			s3tables.NewFromConfig(config, func(o *s3tables.Options) {
				o.RetryMaxAttempts = SDKRetryMaxAttempts
				o.RetryMode = aws.RetryModeStandard
			}),
		)
		return NewS3TablesWrapper(client), nil
	}

	if input.VectorBucketsMode {
		client := client.NewS3Vectors(
			s3vectors.NewFromConfig(config, func(o *s3vectors.Options) {
				o.RetryMaxAttempts = SDKRetryMaxAttempts
				o.RetryMode = aws.RetryModeStandard
			}),
		)
		return NewS3VectorsWrapper(client), nil
	}

	client := client.NewS3(
		s3.NewFromConfig(config, func(o *s3.Options) {
			o.RetryMaxAttempts = SDKRetryMaxAttempts
			o.RetryMode = aws.RetryModeStandard
			o.UsePathStyle = input.PathStyle
		}),
		input.DirectoryBucketsMode,
	)
	return NewS3Wrapper(client), nil
}
