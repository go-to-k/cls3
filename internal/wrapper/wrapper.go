package wrapper

import (
	"context"
)

type ClearBucketInput struct {
	TargetBucket    string // bucket name for S3, bucket arn for S3Tables
	ForceMode       bool
	OldVersionsOnly bool
	QuietMode       bool
}

type ListBucketNamesFilteredByKeywordOutput struct {
	BucketName   string
	TargetBucket string // bucket name for S3, bucket arn for S3Tables
}

type IWrapper interface {
	ClearBucket(ctx context.Context, input ClearBucketInput) error
	ListBucketNamesFilteredByKeyword(ctx context.Context, keyword *string) ([]ListBucketNamesFilteredByKeywordOutput, error)
	CheckAllBucketsExist(ctx context.Context, bucketNames []string) ([]string, error)
}
