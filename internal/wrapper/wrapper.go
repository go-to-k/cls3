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

type IWrapper interface {
	ClearBucket(ctx context.Context, input ClearBucketInput) error
	ListBucketNamesFilteredByKeyword(ctx context.Context, keyword *string) ([]string, error)
	CheckAllBucketsExist(ctx context.Context, bucketNames []string) ([]string, error)
}
