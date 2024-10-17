package wrapper

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/pkg/client"
	"github.com/gosuri/uilive"
	"golang.org/x/sync/errgroup"
)

type ListBucketsOutput struct {
	ExistingBuckets        []types.Bucket
	NonExistingBucketNames []string
}

type S3Wrapper struct {
	client client.IS3
}

func NewS3Wrapper(client client.IS3) *S3Wrapper {
	return &S3Wrapper{
		client: client,
	}
}

func (s *S3Wrapper) ClearS3Objects(
	ctx context.Context,
	bucket *types.Bucket,
	forceMode bool,
	oldVersionsOnly bool,
	quietMode bool,
) error {
	eg := errgroup.Group{}
	errorStr := ""
	errorsCount := 0
	errorsMtx := sync.Mutex{}
	deletedObjectsCount := 0
	deletedObjectsCountMtx := sync.Mutex{}

	var writer *uilive.Writer
	if !quietMode {
		writer = uilive.New()
		writer.Start()
		defer writer.Stop()
	}

	io.Logger.Info().Msgf("%v Checking...", *bucket.Name)

	var keyMarker *string
	var versionIdMarker *string
	for {
		var objects []types.ObjectIdentifier

		// ListObjectVersions/ListObjectsV2 API can only retrieve up to 1000 items, so it is good to pass it
		// directly to DeleteObjects, which can only delete up to 1000 items.
		output, err := s.client.ListObjectsOrVersionsByPage(
			ctx,
			bucket.Name,
			*bucket.BucketRegion,
			oldVersionsOnly,
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
			deletedObjectsCountMtx.Lock()
			deletedObjectsCount += len(objects)
			if !quietMode {
				fmt.Fprintf(writer, "Clearing... %d objects\n", deletedObjectsCount)
			}
			deletedObjectsCountMtx.Unlock()

			// One DeleteObjects is executed for each loop of the List, and it usually ends during
			// the next loop. Therefore, there seems to be no throttling concern, so the number of
			// parallels is not limited by semaphore. (Throttling occurs at about 3500 deletions
			// per second.)
			gotErrors, err := s.client.DeleteObjects(ctx, bucket.Name, objects, *bucket.BucketRegion)
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

	if !quietMode {
		if err := writer.Flush(); err != nil {
			return err
		}
	}

	if errorsCount > 0 {
		// The error is from `DeleteObjectsOutput.Errors`, not `err`.
		// However, we want to treat it as an error, so we use `client.ClientError`.
		return &client.ClientError{
			ResourceName: bucket.Name,
			Err:          fmt.Errorf("DeleteObjectsError: %v objects with errors were found. %v", errorsCount, errorStr),
		}
	}

	if deletedObjectsCount == 0 {
		io.Logger.Info().Msgf("%v No objects.", *bucket.Name)
	} else {
		io.Logger.Info().Msgf("%v Cleared!!: %v objects.", *bucket.Name, deletedObjectsCount)
	}

	if forceMode {
		if err := s.client.DeleteBucket(ctx, bucket.Name, *bucket.BucketRegion); err != nil {
			return err
		}
		io.Logger.Info().Msgf("%v Deleted!!", *bucket.Name)
	}

	return nil
}

func (s *S3Wrapper) ListBucketsFilteredByKeyword(ctx context.Context, keyword *string) ([]types.Bucket, error) {
	// TODO: copy ListBucketNamesFilteredByKeyword here from client
	// return s.client.ListBucketNamesFilteredByKeyword(ctx, keyword)
}

func (s *S3Wrapper) ListBucketsByNames(ctx context.Context, bucketNames []string) (ListBucketsOutput, error) {
	// TODO: get buckets and separate existing and non-existing buckets
	// return s.client.ListBucketNamesFilteredByKeyword(ctx, keyword)
}
