package wrapper

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/pkg/client"
	"github.com/gosuri/uilive"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Too Many Requests error often occurs, so limit the value
const SemaphoreWeight = 5

var _ IWrapper = (*S3TablesWrapper)(nil)

type S3TablesWrapper struct {
	client client.IS3Tables
}

func NewS3TablesWrapper(client client.IS3Tables) *S3TablesWrapper {
	return &S3TablesWrapper{
		client: client,
	}
}

func (s *S3TablesWrapper) deleteNamespace(ctx context.Context, bucketArn string, namespace string) (int, error) {
	eg := errgroup.Group{}

	sem := semaphore.NewWeighted(SemaphoreWeight)

	deletedTablesCount := 0
	var continuationToken *string
	for {
		output, err := s.client.ListTablesByPage(ctx, aws.String(bucketArn), aws.String(namespace), continuationToken)
		if err != nil {
			return 0, err
		}
		if len(output.Tables) == 0 {
			break
		}

		for _, table := range output.Tables {
			if err := sem.Acquire(ctx, 1); err != nil {
				return 0, err
			}
			eg.Go(func() error {
				defer sem.Release(1)
				return s.client.DeleteTable(ctx, aws.String(*table.Name), aws.String(namespace), aws.String(bucketArn))
			})
		}
		deletedTablesCount += len(output.Tables)

		continuationToken = output.ContinuationToken
		if continuationToken == nil {
			break
		}
	}

	if err := eg.Wait(); err != nil {
		return 0, err
	}

	if err := s.client.DeleteNamespace(ctx, aws.String(namespace), aws.String(bucketArn)); err != nil {
		return 0, err
	}

	return deletedTablesCount, nil
}

func (s *S3TablesWrapper) ClearBucket(
	ctx context.Context,
	input ClearBucketInput,
) error {
	eg := errgroup.Group{}

	sem := semaphore.NewWeighted(SemaphoreWeight)

	deletedTablesCount := 0
	deletedTablesCountMtx := sync.Mutex{}

	var writer *uilive.Writer
	if !input.QuietMode {
		writer = uilive.New()
		writer.Start()
		defer writer.Stop()
	}

	bucketArn := input.TargetBucket
	parts := strings.Split(bucketArn, "/")
	if len(parts) < 2 {
		return &client.ClientError{
			Err: fmt.Errorf("InvalidBucketArnError: %v, got %v", "invalid bucket ARN format without a slash", bucketArn),
		}
	}
	bucketName := parts[1]

	io.Logger.Info().Msgf("%v Checking...", bucketName)

	var continuationToken *string
	for {
		output, err := s.client.ListNamespacesByPage(
			ctx,
			aws.String(bucketArn),
			continuationToken,
		)
		if err != nil {
			return err
		}
		if len(output.Namespaces) == 0 {
			break
		}

		for _, summary := range output.Namespaces {
			for _, namespace := range summary.Namespace {
				if err := sem.Acquire(ctx, 1); err != nil {
					return err
				}
				eg.Go(func() error {
					defer sem.Release(1)
					tableCounts, err := s.deleteNamespace(ctx, bucketArn, namespace)
					if err != nil {
						return err
					}
					deletedTablesCountMtx.Lock()
					deletedTablesCount += tableCounts
					if !input.QuietMode {
						fmt.Fprintf(writer, "Clearing... %d tables\n", deletedTablesCount)
					}
					deletedTablesCountMtx.Unlock()
					return nil
				})
			}
		}

		continuationToken = output.ContinuationToken
		if continuationToken == nil {
			break
		}
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	if !input.QuietMode {
		if err := writer.Flush(); err != nil {
			return err
		}
	}

	if deletedTablesCount == 0 {
		io.Logger.Info().Msgf("%v No tables.", bucketName)
	} else {
		io.Logger.Info().Msgf("%v Cleared!!: %v tables.", bucketName, deletedTablesCount)
	}

	if input.ForceMode {
		if err := s.client.DeleteTableBucket(ctx, aws.String(bucketArn)); err != nil {
			return err
		}
		io.Logger.Info().Msgf("%v Deleted!!", bucketName)
	}

	return nil
}

func (s *S3TablesWrapper) ListBucketNamesFilteredByKeyword(ctx context.Context, keyword *string) ([]ListBucketNamesFilteredByKeywordOutput, error) {
	filteredBuckets := []ListBucketNamesFilteredByKeywordOutput{}
	buckets, err := s.client.ListTableBuckets(ctx)
	if err != nil {
		return filteredBuckets, err
	}

	// Bucket names are lowercase so that we need to convert keyword to lowercase for case-insensitive search.
	lowerKeyword := strings.ToLower(*keyword)

	for _, bucket := range buckets {
		if strings.Contains(*bucket.Name, lowerKeyword) {
			filteredBuckets = append(filteredBuckets, ListBucketNamesFilteredByKeywordOutput{
				BucketName:   *bucket.Name,
				TargetBucket: *bucket.Arn,
			})
		}
	}

	if len(filteredBuckets) == 0 {
		errMsg := fmt.Sprintf("No buckets matching the keyword %s.", *keyword)
		return filteredBuckets, &client.ClientError{
			Err: fmt.Errorf("NotExistsError: %v", errMsg),
		}
	}

	return filteredBuckets, nil
}

func (s *S3TablesWrapper) CheckAllBucketsExist(ctx context.Context, bucketNames []string) ([]string, error) {
	targetBucketArns := []string{}
	nonExistingBucketNames := []string{}

	outputBuckets, err := s.client.ListTableBuckets(ctx)
	if err != nil {
		return targetBucketArns, err
	}

	for _, name := range bucketNames {
		found := false
		for _, bucket := range outputBuckets {
			if *bucket.Name == name {
				found = true
				targetBucketArns = append(targetBucketArns, *bucket.Arn)
				break
			}
		}
		if !found {
			nonExistingBucketNames = append(nonExistingBucketNames, name)
		}
	}

	if len(nonExistingBucketNames) > 0 {
		errMsg := fmt.Sprintf("The following buckets do not exist: %v", strings.Join(nonExistingBucketNames, ", "))
		return targetBucketArns, &client.ClientError{
			Err: fmt.Errorf("NotExistsError: %v", errMsg),
		}
	}
	return targetBucketArns, nil
}
