package cls3

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-to-k/delstack/client"
)

type S3Wrapper struct {
	client client.IS3
}

func NewS3Wrapper(client client.IS3) *S3Wrapper {
	return &S3Wrapper{
		client: client,
	}
}

func (s3Wrapper *S3Wrapper) ClearS3Objects(bucketName string, forceMode bool) error {
	exists, err := s3Wrapper.client.CheckBucketExists(aws.String(bucketName))
	if err != nil {
		return err
	}
	if !exists {
		Logger.Info().Msgf("A bucket does not exist: %v", bucketName)
		return nil
	}

	versions, err := s3Wrapper.client.ListObjectVersions(aws.String(bucketName))
	if err != nil && strings.Contains(err.Error(), "api error PermanentRedirect") {
		return fmt.Errorf("ListObjectVersionsError: Are you sure you are specifying the correct region?")
	}
	if err != nil {
		return err
	}

	if len(versions) > 0 {
		errors, err := s3Wrapper.client.DeleteObjects(aws.String(bucketName), versions, SleepTimeSecForS3)
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
		if err := s3Wrapper.client.DeleteBucket(aws.String(bucketName)); err != nil {
			return err
		}
	}

	Logger.Info().Msg("Finished.")
	return nil
}
