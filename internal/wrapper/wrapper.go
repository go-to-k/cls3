//go:generate mockgen -source=$GOFILE -destination=mock_$GOFILE -package=$GOPACKAGE -write_package_comment=false
package wrapper

import (
	"context"
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
	TargetBucket    string // bucket name for S3, bucket arn for S3Tables
	ForceMode       bool
	OldVersionsOnly bool
	QuietMode       bool
	ClearingCountCh chan int64
}

type ListBucketNamesFilteredByKeywordOutput struct {
	BucketName   string
	TargetBucket string // bucket name for S3, bucket arn for S3Tables
}

type CreateS3WrapperInput struct {
	Region               string
	Profile              string
	TableBucketsMode     bool
	DirectoryBucketsMode bool
}

func CreateS3Wrapper(ctx context.Context, input CreateS3WrapperInput) (IWrapper, error) {
	if input.TableBucketsMode {
		wrapper, err := NewS3TablesWrapper(ctx, S3TablesWrapperInput{
			Region:  input.Region,
			Profile: input.Profile,
		})
		if err != nil {
			return nil, err
		}
		return wrapper, nil
	}

	wrapper, err := NewS3Wrapper(ctx, S3WrapperInput{
		Region:               input.Region,
		Profile:              input.Profile,
		DirectoryBucketsMode: input.DirectoryBucketsMode,
	})
	if err != nil {
		return nil, err
	}
	return wrapper, nil
}
