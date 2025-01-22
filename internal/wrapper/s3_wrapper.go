package wrapper

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/pkg/client"
	"github.com/gosuri/uilive"
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

func (s *S3Wrapper) ClearBucket(
	ctx context.Context,
	input ClearBucketInput,
) error {
	// This `bucketRegion` allows buckets outside the specified region to be deleted.
	// If the `directoryBucketsMode` is true, bucketRegion is empty because only one region's
	// buckets can be operated on.
	bucketRegion, err := s.client.GetBucketLocation(ctx, aws.String(input.TargetBucket))
	if err != nil {
		return err
	}

	eg, ctx := errgroup.WithContext(ctx)
	errorStr := ""
	errorsCount := 0
	errorsMtx := sync.Mutex{}
	var deletedObjectsCount atomic.Int64

	var writer *uilive.Writer
	if !input.QuietMode {
		writer = uilive.New()
		writer.Start()
		defer writer.Stop()
	}

	io.Logger.Info().Msgf("%v Checking...", input.TargetBucket)

	var keyMarker *string
	var versionIdMarker *string
	for {
		select {
		case <-ctx.Done():
			return &client.ClientError{
				ResourceName: aws.String(input.TargetBucket),
				Err:          ctx.Err(),
			}
		default:
		}

		var objects []types.ObjectIdentifier

		// ListObjectVersions/ListObjectsV2 API can only retrieve up to 1000 items, so it is good to pass it
		// directly to DeleteObjects, which can only delete up to 1000 items.
		output, err := s.client.ListObjectsOrVersionsByPage(
			ctx,
			aws.String(input.TargetBucket),
			bucketRegion,
			input.OldVersionsOnly,
			keyMarker,
			versionIdMarker,
		)
		if err != nil {
			return err
		}

		objects = output.ObjectIdentifiers
		keyMarker = output.NextKeyMarker
		versionIdMarker = output.NextVersionIdMarker

		if len(objects) == 0 {
			break
		}

		eg.Go(func() error {
			count := deletedObjectsCount.Add(int64(len(objects)))
			if !input.QuietMode {
				fmt.Fprintf(writer, "Clearing... %d objects\n", count)
			}

			// One DeleteObjects is executed for each loop of the List, and it usually ends during
			// the next loop. Therefore, there seems to be no throttling concern, so the number of
			// parallels is not limited by semaphore. (Throttling occurs at about 3500 deletions
			// per second.)
			gotErrors, err := s.client.DeleteObjects(ctx, aws.String(input.TargetBucket), objects, bucketRegion)
			if err != nil {
				return err
			}

			if len(gotErrors) > 0 {
				errorsMtx.Lock()
				errorsCount += len(gotErrors)
				for _, error := range gotErrors {
					errorStr += fmt.Sprintf("\nCode: %v\n", *error.Code)
					errorStr += fmt.Sprintf("Key: %v\n", *error.Key)
					errorStr += fmt.Sprintf("VersionId: %v\n", *error.VersionId)
					errorStr += fmt.Sprintf("Message: %v\n", *error.Message)
				}
				errorsMtx.Unlock()
			}

			return nil
		})

		if keyMarker == nil && versionIdMarker == nil {
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

	if errorsCount > 0 {
		// The error is from `DeleteObjectsOutput.Errors`, not `err`.
		// However, we want to treat it as an error, so we use `client.ClientError`.
		return &client.ClientError{
			ResourceName: aws.String(input.TargetBucket),
			Err:          fmt.Errorf("DeleteObjectsError: %v objects with errors were found. %v", errorsCount, errorStr),
		}
	}

	finalCount := deletedObjectsCount.Load()
	if finalCount == 0 {
		io.Logger.Info().Msgf("%v No objects.", input.TargetBucket)
	} else {
		io.Logger.Info().Msgf("%v Cleared!!: %v objects.", input.TargetBucket, finalCount)
	}

	if input.ForceMode {
		if err := s.client.DeleteBucket(ctx, aws.String(input.TargetBucket), bucketRegion); err != nil {
			return err
		}
		io.Logger.Info().Msgf("%v Deleted!!", input.TargetBucket)
	}

	return nil
}

func (s *S3Wrapper) ListBucketNamesFilteredByKeyword(ctx context.Context, keyword *string) ([]ListBucketNamesFilteredByKeywordOutput, error) {
	filteredBuckets := []ListBucketNamesFilteredByKeywordOutput{}
	buckets, err := s.client.ListBucketsOrDirectoryBuckets(ctx)
	if err != nil {
		return filteredBuckets, err
	}

	// Bucket names are lowercase so that we need to convert keyword to lowercase for case-insensitive search.
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

	outputBuckets, err := s.client.ListBucketsOrDirectoryBuckets(ctx)
	if err != nil {
		return targetBucketNames, err
	}

	for _, name := range bucketNames {
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
