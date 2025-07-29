package wrapper

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/pkg/client"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Too Many Requests error often occurs, so limit the value
const S3VectorsSemaphoreWeight = 8

var _ IWrapper = (*S3VectorsWrapper)(nil)

type S3VectorsWrapper struct {
	client client.IS3Vectors
}

func NewS3VectorsWrapper(client client.IS3Vectors) *S3VectorsWrapper {
	return &S3VectorsWrapper{
		client: client,
	}
}

func (s *S3VectorsWrapper) ClearBucket(
	ctx context.Context,
	input ClearBucketInput,
) error {
	bucketName := input.TargetBucket

	var deletedIndexesCount atomic.Int64
	progressCh := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range progressCh {
			count := deletedIndexesCount.Add(1)
			if !input.QuietMode {
				input.ClearingCountCh <- count
			}
		}
	}()

	eg := errgroup.Group{}
	sem := semaphore.NewWeighted(S3VectorsSemaphoreWeight)
	var nextToken *string
	for {
		select {
		case <-ctx.Done():
			close(progressCh)
			wg.Wait()
			return &client.ClientError{
				ResourceName: aws.String(bucketName),
				Err:          ctx.Err(),
			}
		default:
		}

		output, err := s.client.ListIndexesByPage(
			ctx,
			aws.String(bucketName),
			nextToken,
			input.Prefix,
		)
		if err != nil {
			close(progressCh)
			wg.Wait()
			return err
		}
		if len(output.Indexes) == 0 {
			break
		}

		for _, index := range output.Indexes {
			if err := sem.Acquire(ctx, 1); err != nil {
				close(progressCh)
				wg.Wait()
				return err
			}
			eg.Go(func() error {
				defer sem.Release(1)
				if err := s.client.DeleteIndex(ctx, index.IndexName, aws.String(bucketName)); err != nil {
					return err
				}
				progressCh <- struct{}{}
				return nil
			})
		}

		nextToken = output.NextToken
		if nextToken == nil {
			break
		}
	}

	if err := eg.Wait(); err != nil {
		close(progressCh)
		wg.Wait()
		return err
	}
	close(progressCh)
	wg.Wait()

	finalCount := deletedIndexesCount.Load()
	if input.QuietMode {
		// When not in quiet mode, the message is displayed along with other buckets in the app.go.
		if err := s.OutputClearedMessage(bucketName, finalCount); err != nil {
			return err
		}
	}

	if !input.ForceMode {
		return nil
	}

	if err := s.client.DeleteVectorBucket(ctx, aws.String(bucketName)); err != nil {
		return err
	}

	// NOTE: When not in quiet mode, the message is displayed along with other buckets in the app.go.
	if !input.QuietMode {
		return nil
	}

	if err := s.OutputDeletedMessage(bucketName); err != nil {
		return err
	}

	return nil
}

func (s *S3VectorsWrapper) OutputClearedMessage(bucket string, count int64) error {
	if count == 0 {
		io.Logger.Info().Msgf("%v No indexes.", bucket)
	} else {
		io.Logger.Info().Msgf("%v Cleared!!: %v indexes.", bucket, count)
	}
	return nil
}

func (s *S3VectorsWrapper) OutputDeletedMessage(bucket string) error {
	io.Logger.Info().Msgf("%v Deleted!!", bucket)
	return nil
}

func (s *S3VectorsWrapper) OutputCheckingMessage(bucket string) error {
	io.Logger.Info().Msgf("%v Checking...", bucket)
	return nil
}

func (s *S3VectorsWrapper) GetLiveClearingMessage(bucket string, count int64) (string, error) {
	return fmt.Sprintf("%v Clearing... %v indexes", bucket, count), nil
}

func (s *S3VectorsWrapper) GetLiveClearedMessage(bucket string, count int64, isCompleted bool) (string, error) {
	if isCompleted {
		return fmt.Sprintf("\033[32m%v Cleared!!!  %d indexes\033[0m", bucket, count), nil
	}
	return fmt.Sprintf("\033[31m%v Errors occurred!!! Cleared: %d indexes\033[0m", bucket, count), nil
}

func (s *S3VectorsWrapper) ListBucketNamesFilteredByKeyword(ctx context.Context, keyword *string) ([]ListBucketNamesFilteredByKeywordOutput, error) {
	filteredBuckets := []ListBucketNamesFilteredByKeywordOutput{}
	buckets, err := s.client.ListVectorBuckets(ctx)
	if err != nil {
		return filteredBuckets, err
	}

	lowerKeyword := strings.ToLower(*keyword)

	for _, bucket := range buckets {
		if strings.Contains(*bucket.VectorBucketName, lowerKeyword) {
			filteredBuckets = append(filteredBuckets, ListBucketNamesFilteredByKeywordOutput{
				BucketName:   *bucket.VectorBucketName,
				TargetBucket: *bucket.VectorBucketName,
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

func (s *S3VectorsWrapper) CheckAllBucketsExist(ctx context.Context, bucketNames []string) ([]string, error) {
	targetBucketArns := []string{}
	nonExistingBucketNames := []string{}

	uniqueBucketNames := make([]string, 0, len(bucketNames))
	seen := make(map[string]bool)
	for _, name := range bucketNames {
		if !seen[name] {
			seen[name] = true
			uniqueBucketNames = append(uniqueBucketNames, name)
		}
	}

	outputBuckets, err := s.client.ListVectorBuckets(ctx)
	if err != nil {
		return targetBucketArns, err
	}

	for _, name := range uniqueBucketNames {
		found := false
		for _, bucket := range outputBuckets {
			if *bucket.VectorBucketName == name {
				found = true
				targetBucketArns = append(targetBucketArns, *bucket.VectorBucketName)
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
