package cls3

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-to-k/delstack/pkg/client"
)

const sleepTimeSecForS3 = 10

type S3Wrapper struct {
	client client.IS3
}

func NewS3Wrapper(client client.IS3) *S3Wrapper {
	return &S3Wrapper{
		client: client,
	}
}

func (s3Wrapper *S3Wrapper) ClearS3Objects(ctx context.Context, bucketName string, forceMode bool) error {
	exists, err := s3Wrapper.client.CheckBucketExists(ctx, aws.String(bucketName))
	if err != nil {
		return err
	}
	if !exists {
		Logger.Info().Msgf("A bucket does not exist: %v", bucketName)
		return nil
	}

	versions, err := s3Wrapper.client.ListObjectVersions(ctx, aws.String(bucketName))
	if err != nil && strings.Contains(err.Error(), "api error PermanentRedirect") {
		return fmt.Errorf("PermanentRedirectError: Are you sure you are specifying the correct region?")
	}
	if err != nil {
		return err
	}

	if len(versions) > 0 {
		errors, err := s3Wrapper.client.DeleteObjects(ctx, aws.String(bucketName), versions, sleepTimeSecForS3)
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
	}

	if forceMode {
		Logger.Info().Msgf("[ForceMode] Delete the bucket as well: %v", bucketName)
		if err := s3Wrapper.client.DeleteBucket(ctx, aws.String(bucketName)); err != nil {
			return err
		}
	}

	Logger.Info().Msg("Finished.")
	return nil
}
