package wrapper

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-to-k/cls3/pkg/client"
)

var _ IWrapper = (*S3TablesWrapper)(nil)

type S3TablesWrapper struct {
	client client.IS3Tables
}

func NewS3TablesWrapper(client client.IS3Tables) *S3TablesWrapper {
	return &S3TablesWrapper{
		client: client,
	}
}

func (s *S3TablesWrapper) ClearBucket(
	ctx context.Context,
	input ClearBucketInput,
) error {
	// eg := errgroup.Group{}
	// errorStr := ""
	// errorsCount := 0
	// errorsMtx := sync.Mutex{}
	// deletedObjectsCount := 0
	// deletedObjectsCountMtx := sync.Mutex{}

	// var writer *uilive.Writer
	// if !input.QuietMode {
	// 	writer = uilive.New()
	// 	writer.Start()
	// 	defer writer.Stop()
	// }

	// io.Logger.Info().Msgf("%v Checking...", input.BucketName)

	// var keyMarker *string
	// var versionIdMarker *string
	// for {
	// 	var objects []types.ObjectIdentifier

	// 	// ListObjectVersions/ListObjectsV2 API can only retrieve up to 1000 items, so it is good to pass it
	// 	// directly to DeleteObjects, which can only delete up to 1000 items.
	// 	output, err := s.client.ListTables(
	// 		ctx,
	// 		aws.String(input.BucketName),
	// 		input.OldVersionsOnly,
	// 		keyMarker,
	// 		versionIdMarker,
	// 	)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	objects = output.ObjectIdentifiers
	// 	keyMarker = output.NextKeyMarker
	// 	versionIdMarker = output.NextVersionIdMarker

	// 	if len(objects) == 0 {
	// 		break
	// 	}

	// 	eg.Go(func() error {
	// 		deletedObjectsCountMtx.Lock()
	// 		deletedObjectsCount += len(objects)
	// 		if !input.QuietMode {
	// 			fmt.Fprintf(writer, "Clearing... %d objects\n", deletedObjectsCount)
	// 		}
	// 		deletedObjectsCountMtx.Unlock()

	// 		// One DeleteObjects is executed for each loop of the List, and it usually ends during
	// 		// the next loop. Therefore, there seems to be no throttling concern, so the number of
	// 		// parallels is not limited by semaphore. (Throttling occurs at about 3500 deletions
	// 		// per second.)
	// 		gotErrors, err := s.client.DeleteObjects(ctx, aws.String(input.BucketName), objects, bucketRegion)
	// 		if err != nil {
	// 			return err
	// 		}

	// 		if len(gotErrors) > 0 {
	// 			errorsMtx.Lock()
	// 			errorsCount += len(gotErrors)
	// 			for _, error := range gotErrors {
	// 				errorStr += fmt.Sprintf("\nCode: %v\n", *error.Code)
	// 				errorStr += fmt.Sprintf("Key: %v\n", *error.Key)
	// 				errorStr += fmt.Sprintf("VersionId: %v\n", *error.VersionId)
	// 				errorStr += fmt.Sprintf("Message: %v\n", *error.Message)
	// 			}
	// 			errorsMtx.Unlock()
	// 		}

	// 		return nil
	// 	})

	// 	if keyMarker == nil && versionIdMarker == nil {
	// 		break
	// 	}
	// }

	// if err := eg.Wait(); err != nil {
	// 	return err
	// }

	// if !input.QuietMode {
	// 	if err := writer.Flush(); err != nil {
	// 		return err
	// 	}
	// }

	// if errorsCount > 0 {
	// 	// The error is from `DeleteObjectsOutput.Errors`, not `err`.
	// 	// However, we want to treat it as an error, so we use `client.ClientError`.
	// 	return &client.ClientError{
	// 		ResourceName: aws.String(input.BucketName),
	// 		Err:          fmt.Errorf("DeleteObjectsError: %v objects with errors were found. %v", errorsCount, errorStr),
	// 	}
	// }

	// if deletedObjectsCount == 0 {
	// 	io.Logger.Info().Msgf("%v No objects.", input.BucketName)
	// } else {
	// 	io.Logger.Info().Msgf("%v Cleared!!: %v objects.", input.BucketName, deletedObjectsCount)
	// }

	// if input.ForceMode {
	// 	if err := s.client.DeleteBucket(ctx, aws.String(input.BucketName), bucketRegion); err != nil {
	// 		return err
	// 	}
	// 	io.Logger.Info().Msgf("%v Deleted!!", input.BucketName)
	// }

	return nil
}

func (s *S3TablesWrapper) ListBucketNamesFilteredByKeyword(ctx context.Context, keyword *string) ([]string, error) {
	filteredBucketNames := []string{}
	buckets, err := s.client.ListTableBuckets(ctx)
	if err != nil {
		return filteredBucketNames, err
	}

	// Bucket names are lowercase so that we need to convert keyword to lowercase for case-insensitive search.
	lowerKeyword := strings.ToLower(*keyword)

	for _, bucket := range buckets {
		if strings.Contains(*bucket.Name, lowerKeyword) {
			filteredBucketNames = append(filteredBucketNames, *bucket.Name)
		}
	}

	if len(filteredBucketNames) == 0 {
		errMsg := fmt.Sprintf("No buckets matching the keyword %s.", *keyword)
		return filteredBucketNames, &client.ClientError{
			Err: fmt.Errorf("NotExistsError: %v", errMsg),
		}
	}

	return filteredBucketNames, nil
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
