package wrapper

import (
	"context"
)

type ClearBucketInput struct {
	BucketName      string
	ForceMode       bool
	OldVersionsOnly bool
	QuietMode       bool
}

type IWrapper interface {
	ClearBucket(ctx context.Context, input ClearBucketInput) error
	ListBucketNamesFilteredByKeyword(ctx context.Context, keyword *string) ([]string, error)
	CheckAllBucketsExist(ctx context.Context, bucketNames []string) error
}
