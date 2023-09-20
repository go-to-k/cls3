package wrapper

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/pkg/client"
)

type S3Wrapper struct {
	client client.IS3
}

func NewS3Wrapper(client client.IS3) *S3Wrapper {
	return &S3Wrapper{
		client: client,
	}
}

func (s *S3Wrapper) ClearS3Objects(ctx context.Context, bucketName string, forceMode bool, quiet bool) error {
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

	io.Logger.Info().Msgf("%v Checking...", bucketName)

	versions, err := s.client.ListObjectVersions(ctx, aws.String(bucketName), region)
	if err != nil {
		return err
	}

	if len(versions) == 0 {
		io.Logger.Info().Msgf("%v No objects.", bucketName)
	} else {
		io.Logger.Info().Msgf("%v Clearing...", bucketName)

		errors, err := s.client.DeleteObjects(ctx, aws.String(bucketName), versions, region, quiet)
		if err != nil {
			return err
		}
		if len(errors) > 0 {
			errorStr := ""
			for _, error := range errors {
				errorStr += fmt.Sprintf("\nCode: %v\n", *error.Code)
				errorStr += fmt.Sprintf("Key: %v\n", *error.Key)
				errorStr += fmt.Sprintf("VersionId: %v\n", *error.VersionId)
				errorStr += fmt.Sprintf("Message: %v\n", *error.Message)
			}
			return fmt.Errorf("DeleteObjectsError: followings %v", errorStr)
		}

		io.Logger.Info().Msgf("%v Cleared!!", bucketName)
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

	// To be series to avoid throttling of S3 API
	for _, bucket := range buckets {
		// Bucket names are lowercase only so need not be case-insensitive
		if strings.Contains(*bucket.Name, *keyword) {
			filteredBucketNames = append(filteredBucketNames, *bucket.Name)
		}
	}

	return filteredBucketNames, nil
}
