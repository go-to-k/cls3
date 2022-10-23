package cls3

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-to-k/delstack/client"
)

type S3 struct {
	client client.IS3
}

func NewS3(client client.IS3) *S3 {
	return &S3{
		client: client,
	}
}

func (s3 *S3) ClearS3Objects(bucketName string, forceMode bool) error {
	exists, err := s3.client.CheckBucketExists(aws.String(bucketName))
	if err != nil {
		return err
	}
	if !exists {
		Logger.Info().Msgf("A bucket does not exist: %v", bucketName)
		return nil
	}

	versions, err := s3.client.ListObjectVersions(aws.String(bucketName))
	if err != nil {
		return err
	}

	if len(versions) > 0 {
		errors, err := s3.client.DeleteObjects(aws.String(bucketName), versions, SleepTimeSecForS3)
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
		if err := s3.client.DeleteBucket(aws.String(bucketName)); err != nil {
			return err
		}
	}

	Logger.Info().Msg("Finished.")
	return nil
}
