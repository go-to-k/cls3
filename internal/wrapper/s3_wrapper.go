package wrapper

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/pkg/client"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
)

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
	bucketName string,
	forceMode bool,
	quiet bool,
	oldVersionsOnly bool,
) error {
	exists, err := s.client.CheckBucketExists(ctx, aws.String(bucketName))
	if err != nil {
		return err
	}
	if !exists {
		io.Logger.Info().Msgf("A bucket does not exist: %v", bucketName)
		return nil
	}

	region, err := s.client.GetBucketLocation(ctx, aws.String(bucketName))
	if err != nil {
		return err
	}

	eg := errgroup.Group{}
	errorStr := ""
	errorsMtx := sync.Mutex{}
	deletedVersionsCount := 0
	deletedVersionsCountMtx := sync.Mutex{}

	var keyMarker *string
	var versionIdMarker *string
	var bar *progressbar.ProgressBar
	isFirstLoop := true

	for {
		var versions []types.ObjectIdentifier

		versions, keyMarker, versionIdMarker, err = s.client.ListObjectVersionsByPage(
			ctx,
			aws.String(bucketName),
			region,
			oldVersionsOnly,
			keyMarker,
			versionIdMarker,
		)
		if err != nil {
			return err
		}
		if len(versions) == 0 {
			break
		}

		if isFirstLoop {
			io.Logger.Info().Msgf("%v Clearing...", bucketName)
		}
		if isFirstLoop && !quiet {
			// dummy for the first value (because it does not work if the value is zero)
			dummyForFirstValue := 1000

			bar = progressbar.NewOptions64(
				int64(dummyForFirstValue),
				progressbar.OptionSetWriter(os.Stderr),
				progressbar.OptionSetWidth(50),
				progressbar.OptionThrottle(65*time.Millisecond),
				progressbar.OptionShowCount(),
				progressbar.OptionOnCompletion(func() {
					fmt.Fprint(os.Stderr, "\n")
				}),
				progressbar.OptionSpinnerType(14),
				progressbar.OptionSetRenderBlankState(true),
			)
			// clear the dummy for the first value
			bar.ChangeMax(bar.GetMax() - dummyForFirstValue)
		}
		if !quiet {
			bar.ChangeMax(bar.GetMax() + len(versions))
		}

		eg.Go(func() error {
			gotErrors, err := s.client.DeleteObjects(ctx, aws.String(bucketName), versions, region, quiet)
			if err != nil {
				return err
			}
			if len(gotErrors) > 0 {
				errorsMtx.Lock()
				for _, error := range gotErrors {
					errorStr += fmt.Sprintf("\nCode: %v\n", *error.Code)
					errorStr += fmt.Sprintf("Key: %v\n", *error.Key)
					errorStr += fmt.Sprintf("VersionId: %v\n", *error.VersionId)
					errorStr += fmt.Sprintf("Message: %v\n", *error.Message)
				}
				errorsMtx.Unlock()
			}

			deletedVersionsCountMtx.Lock()
			deletedVersionsCount += len(versions)
			deletedVersionsCountMtx.Unlock()

			if !quiet {
				bar.Add(len(versions))
			}

			return nil
		})

		if keyMarker == nil && versionIdMarker == nil {
			break
		}
		isFirstLoop = false
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	if errorStr != "" {
		return fmt.Errorf("DeleteObjectsError: followings %v", errorStr)
	}

	if deletedVersionsCount == 0 {
		io.Logger.Info().Msgf("%v No objects.", bucketName)
	} else {
		io.Logger.Info().Msgf("%v Cleared!!: %v objects.", bucketName, deletedVersionsCount)
	}

	if forceMode {
		if err := s.client.DeleteBucket(ctx, aws.String(bucketName), region); err != nil {
			return err
		}
		io.Logger.Info().Msgf("%v Deleted!!", bucketName)
	}

	return nil
}

func (s *S3Wrapper) ListBucketNamesFilteredByKeyword(ctx context.Context, keyword *string) ([]string, error) {
	filteredBucketNames := []string{}

	buckets, err := s.client.ListBuckets(ctx)
	if err != nil {
		return filteredBucketNames, err
	}

	// Bucket names are lowercase so that we need to convert keyword to lowercase for case-insensitive search.
	lowerKeyword := strings.ToLower(*keyword)

	// To be series to avoid throttling of S3 API
	for _, bucket := range buckets {
		if strings.Contains(*bucket.Name, lowerKeyword) {
			filteredBucketNames = append(filteredBucketNames, *bucket.Name)
		}
	}

	return filteredBucketNames, nil
}
