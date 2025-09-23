package wrapper

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/pkg/client"
	"golang.org/x/sync/errgroup"
)

var _ IWrapper = (*S3Wrapper)(nil)

type S3Wrapper struct {
	client client.IS3
}

func NewS3Wrapper(client client.IS3) *S3Wrapper {
	return &S3Wrapper{
		client: client,
	}
}

type objectDeletionState struct {
	errorStr        string
	errorsCount     int
	errorsMtx       sync.Mutex
	objectsCount    int64
	objectsCountMtx sync.Mutex
}

func (s *S3Wrapper) ClearBucket(
	ctx context.Context,
	input ClearBucketInput,
) error {
	// NOTE: This `bucketRegion` allows buckets outside the specified region to be deleted.
	// If the `directoryBucketsMode` is true, bucketRegion is empty because only one region's
	// buckets can be operated on.
	bucketRegion, err := s.client.GetBucketLocation(ctx, aws.String(input.TargetBucket))
	if err != nil {
		return err
	}

	if err := s.clearObjects(ctx, input, bucketRegion); err != nil {
		return err
	}

	if !input.ForceMode {
		return nil
	}

	if err := s.deleteBucket(ctx, input.TargetBucket, bucketRegion, input.QuietMode); err != nil {
		return err
	}

	return nil
}

func (s *S3Wrapper) clearObjects(ctx context.Context, input ClearBucketInput, bucketRegion string) error {
	state := &objectDeletionState{}

	if !input.QuietMode {
		// NOTE: Send 0 to the channel to indicate that the clearing has started.
		input.ClearingCountCh <- 0
	}

	// NOTE: Try clearing objects up to 2 times to handle eventual consistency
	// There was a case where the object deletion was completed but the object was still there.
	// So, even if all objects are deleted, it is not guaranteed that the object is deleted.
	// Therefore, we try to delete the objects again.
	maxAttempts := 2
	for attempt := 0; attempt < maxAttempts; attempt++ {
		done, err := s.processObjectDeletionAttempt(ctx, input, bucketRegion, state, attempt)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}

	if state.errorsCount > 0 {
		state.objectsCount -= int64(state.errorsCount)
		if !input.QuietMode {
			input.ClearingCountCh <- state.objectsCount
		}

		// NOTE: The error is from `DeleteObjectsOutput.Errors`, not `err`.
		// However, we want to treat it as an error, so we use `client.ClientError`.
		return &client.ClientError{
			ResourceName: aws.String(input.TargetBucket),
			Err:          fmt.Errorf("DeleteObjectsError: %v objects with errors were found. %v", state.errorsCount, state.errorStr),
		}
	}

	// NOTE: When not in quiet mode, the message is displayed along with other buckets in the app.go.
	if !input.QuietMode {
		return nil
	}

	if err := s.OutputClearedMessage(input.TargetBucket, state.objectsCount); err != nil {
		return err
	}

	return nil
}

func (s *S3Wrapper) processObjectDeletionAttempt(ctx context.Context, input ClearBucketInput, bucketRegion string, state *objectDeletionState, attempt int) (bool, error) {
	eg := errgroup.Group{}
	var keyMarker *string
	var versionIdMarker *string

	for {
		select {
		case <-ctx.Done():
			return false, &client.ClientError{
				ResourceName: aws.String(input.TargetBucket),
				Err:          ctx.Err(),
			}
		default:
		}

		// NOTE: ListObjectVersions/ListObjectsV2 API can only retrieve up to 1000 items, so it is good to pass it
		// directly to DeleteObjects, which can only delete up to 1000 items.
		output, err := s.client.ListObjectsOrVersionsByPage(
			ctx,
			aws.String(input.TargetBucket),
			bucketRegion,
			input.OldVersionsOnly,
			keyMarker,
			versionIdMarker,
			input.Prefix,
		)
		if err != nil {
			return false, err
		}

		isFirstPage := keyMarker == nil && versionIdMarker == nil
		if len(output.ObjectIdentifiers) == 0 {
			// If no objects found in the first page of a new attempt, we're done
			if isFirstPage {
				return true, nil
			}
			break
		} else if isFirstPage && attempt > 0 {
			state.errorStr = ""
			state.errorsCount = 0
			io.Logger.Debug().Msgf("%s: Retry attempt %d", input.TargetBucket, attempt)
		}

		eg.Go(func() error {
			// NOTE: This loop with the `attempt` variable is a retry process for the bug where DeleteObjects
			// was executed but objects were not deleted.
			// Therefore, it is not counted in the number of deletions if it is not the first attempt.
			if attempt == 0 {
				state.objectsCountMtx.Lock()
				state.objectsCount += int64(len(output.ObjectIdentifiers))
				if !input.QuietMode {
					input.ClearingCountCh <- state.objectsCount
				}
				state.objectsCountMtx.Unlock()
			}

			// NOTE: One DeleteObjects is executed for each loop of the List, and it usually ends during
			// the next loop. Therefore, there seems to be no throttling concern, so the number of
			// parallels is not limited by semaphore. (Throttling occurs at about 3500 deletions
			// per second.)
			gotErrors, err := s.client.DeleteObjects(ctx, aws.String(input.TargetBucket), output.ObjectIdentifiers, bucketRegion)
			if err != nil {
				return err
			}

			if len(gotErrors) > 0 {
				state.errorsMtx.Lock()
				state.errorsCount += len(gotErrors)
				for _, error := range gotErrors {
					state.errorStr += fmt.Sprintf("\nCode: %v\n", *error.Code)
					state.errorStr += fmt.Sprintf("Key: %v\n", *error.Key)
					state.errorStr += fmt.Sprintf("VersionId: %v\n", *error.VersionId)
					state.errorStr += fmt.Sprintf("Message: %v\n", *error.Message)
				}
				state.errorsMtx.Unlock()
			}

			return nil
		})

		keyMarker = output.NextKeyMarker
		versionIdMarker = output.NextVersionIdMarker

		if keyMarker == nil && versionIdMarker == nil {
			break
		}
	}

	if err := eg.Wait(); err != nil {
		return false, err
	}

	return false, nil
}

func (s *S3Wrapper) deleteBucket(ctx context.Context, bucket string, bucketRegion string, quietMode bool) error {
	if err := s.client.DeleteBucket(ctx, aws.String(bucket), bucketRegion); err != nil {
		return err
	}
	if quietMode {
		// NOTE: When not in quiet mode, the message is displayed along with other buckets in the app.go.
		if err := s.OutputDeletedMessage(bucket); err != nil {
			return err
		}
	}
	return nil
}

func (s *S3Wrapper) OutputClearedMessage(bucket string, count int64) error {
	if count == 0 {
		io.Logger.Info().Msgf("%v No objects.", bucket)
	} else {
		io.Logger.Info().Msgf("%v Cleared!!: %v objects.", bucket, count)
	}
	return nil
}

func (s *S3Wrapper) OutputDeletedMessage(bucket string) error {
	io.Logger.Info().Msgf("%v Deleted!!", bucket)
	return nil
}

func (s *S3Wrapper) OutputCheckingMessage(bucket string) error {
	io.Logger.Info().Msgf("%v Checking...", bucket)
	return nil
}

func (s *S3Wrapper) GetLiveClearingMessage(bucket string, count int64) (string, error) {
	return fmt.Sprintf("%v Clearing... %v objects", bucket, count), nil
}

func (s *S3Wrapper) GetLiveClearedMessage(bucket string, count int64, isCompleted bool) (string, error) {
	if isCompleted {
		return fmt.Sprintf("\033[32m%v Cleared!!!  %d objects\033[0m", bucket, count), nil
	}
	return fmt.Sprintf("\033[31m%v Errors occurred!!! Cleared: %d objects\033[0m", bucket, count), nil
}

func (s *S3Wrapper) ListBucketNamesFilteredByKeyword(ctx context.Context, keyword *string) ([]ListBucketNamesFilteredByKeywordOutput, error) {
	filteredBuckets := []ListBucketNamesFilteredByKeywordOutput{}
	buckets, err := s.client.ListBucketsOrDirectoryBuckets(ctx)
	if err != nil {
		return filteredBuckets, err
	}

	// NOTE: Bucket names are lowercase so that we need to convert keyword to lowercase for case-insensitive search.
	lowerKeyword := strings.ToLower(*keyword)

	for _, bucket := range buckets {
		if strings.Contains(*bucket.Name, lowerKeyword) {
			filteredBuckets = append(filteredBuckets, ListBucketNamesFilteredByKeywordOutput{
				BucketName:   *bucket.Name,
				TargetBucket: *bucket.Name,
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

func (s *S3Wrapper) CheckAllBucketsExist(ctx context.Context, bucketNames []string) ([]string, error) {
	targetBucketNames := []string{}
	nonExistingBucketNames := []string{}

	uniqueBucketNames := make([]string, 0, len(bucketNames))
	seen := make(map[string]bool)
	for _, name := range bucketNames {
		if !seen[name] {
			seen[name] = true
			uniqueBucketNames = append(uniqueBucketNames, name)
		}
	}

	outputBuckets, err := s.client.ListBucketsOrDirectoryBuckets(ctx)
	if err != nil {
		return targetBucketNames, err
	}

	for _, name := range uniqueBucketNames {
		found := false
		for _, bucket := range outputBuckets {
			if *bucket.Name == name {
				found = true
				targetBucketNames = append(targetBucketNames, *bucket.Name)
				break
			}
		}
		if !found {
			nonExistingBucketNames = append(nonExistingBucketNames, name)
		}
	}

	if len(nonExistingBucketNames) > 0 {
		errMsg := fmt.Sprintf("The following buckets do not exist: %v", strings.Join(nonExistingBucketNames, ", "))
		return targetBucketNames, &client.ClientError{
			Err: fmt.Errorf("NotExistsError: %v", errMsg),
		}
	}
	return targetBucketNames, nil
}
