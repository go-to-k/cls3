package cls3

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-to-k/delstack/pkg/client"
)

var _ client.IS3 = (*MockS3)(nil)
var _ client.IS3 = (*DeleteObjectsErrorMockS3)(nil)
var _ client.IS3 = (*DeleteObjectsErrorAfterZeroLengthMockS3)(nil)
var _ client.IS3 = (*DeleteObjectsOutputErrorMockS3)(nil)
var _ client.IS3 = (*DeleteObjectsOutputErrorAfterZeroLengthMockS3)(nil)
var _ client.IS3 = (*ListObjectVersionsErrorMockS3)(nil)
var _ client.IS3 = (*DeleteBucketErrorMockS3)(nil)
var _ client.IS3 = (*CheckBucketExistsErrorMockS3)(nil)
var _ client.IS3 = (*CheckBucketNotExistsMockS3)(nil)
var _ client.IS3 = (*ListObjectVersionsIncorrectRegionMockS3)(nil)

/*
	Mocks for client
*/
type MockS3 struct{}

func NewMockS3() *MockS3 {
	return &MockS3{}
}

func (m *MockS3) DeleteBucket(ctx context.Context, bucketName *string) error {
	return nil
}

func (m *MockS3) DeleteObjects(ctx context.Context, bucketName *string, objects []types.ObjectIdentifier, sleepTimeSec int) ([]types.Error, error) {
	return []types.Error{}, nil
}

func (m *MockS3) ListObjectVersions(ctx context.Context, bucketName *string) ([]types.ObjectIdentifier, error) {
	output := []types.ObjectIdentifier{
		{
			Key:       aws.String("KeyForVersions"),
			VersionId: aws.String("VersionIdForVersions"),
		},
		{
			Key:       aws.String("KeyForDeleteMarkers"),
			VersionId: aws.String("VersionIdForDeleteMarkers"),
		},
	}
	return output, nil
}

func (m *MockS3) CheckBucketExists(ctx context.Context, bucketName *string) (bool, error) {
	return true, nil
}

type DeleteBucketErrorMockS3 struct{}

func NewDeleteBucketErrorMockS3() *DeleteBucketErrorMockS3 {
	return &DeleteBucketErrorMockS3{}
}

func (m *DeleteBucketErrorMockS3) DeleteBucket(ctx context.Context, bucketName *string) error {
	return fmt.Errorf("DeleteBucketError")
}

func (m *DeleteBucketErrorMockS3) DeleteObjects(ctx context.Context, bucketName *string, objects []types.ObjectIdentifier, sleepTimeSec int) ([]types.Error, error) {
	return []types.Error{}, nil
}

func (m *DeleteBucketErrorMockS3) ListObjectVersions(ctx context.Context, bucketName *string) ([]types.ObjectIdentifier, error) {
	output := []types.ObjectIdentifier{
		{
			Key:       aws.String("KeyForVersions"),
			VersionId: aws.String("VersionIdForVersions"),
		},
		{
			Key:       aws.String("KeyForDeleteMarkers"),
			VersionId: aws.String("VersionIdForDeleteMarkers"),
		},
	}
	return output, nil
}

func (m *DeleteBucketErrorMockS3) CheckBucketExists(ctx context.Context, bucketName *string) (bool, error) {
	return true, nil
}

type DeleteObjectsErrorMockS3 struct{}

func NewDeleteObjectsErrorMockS3() *DeleteObjectsErrorMockS3 {
	return &DeleteObjectsErrorMockS3{}
}

func (m *DeleteObjectsErrorMockS3) DeleteBucket(ctx context.Context, bucketName *string) error {
	return nil
}

func (m *DeleteObjectsErrorMockS3) DeleteObjects(ctx context.Context, bucketName *string, objects []types.ObjectIdentifier, sleepTimeSec int) ([]types.Error, error) {
	return []types.Error{}, fmt.Errorf("DeleteObjectsError")
}

func (m *DeleteObjectsErrorMockS3) ListObjectVersions(ctx context.Context, bucketName *string) ([]types.ObjectIdentifier, error) {
	output := []types.ObjectIdentifier{
		{
			Key:       aws.String("KeyForVersions"),
			VersionId: aws.String("VersionIdForVersions"),
		},
		{
			Key:       aws.String("KeyForDeleteMarkers"),
			VersionId: aws.String("VersionIdForDeleteMarkers"),
		},
	}
	return output, nil
}

func (m *DeleteObjectsErrorMockS3) CheckBucketExists(ctx context.Context, bucketName *string) (bool, error) {
	return true, nil
}

type DeleteObjectsErrorAfterZeroLengthMockS3 struct{}

func NewDeleteObjectsErrorAfterZeroLengthMockS3() *DeleteObjectsErrorAfterZeroLengthMockS3 {
	return &DeleteObjectsErrorAfterZeroLengthMockS3{}
}

func (m *DeleteObjectsErrorAfterZeroLengthMockS3) DeleteBucket(ctx context.Context, bucketName *string) error {
	return nil
}

func (m *DeleteObjectsErrorAfterZeroLengthMockS3) DeleteObjects(ctx context.Context, bucketName *string, objects []types.ObjectIdentifier, sleepTimeSec int) ([]types.Error, error) {
	return []types.Error{}, fmt.Errorf("DeleteObjectsErrorAfterZeroLength")
}

func (m *DeleteObjectsErrorAfterZeroLengthMockS3) ListObjectVersions(ctx context.Context, bucketName *string) ([]types.ObjectIdentifier, error) {
	output := []types.ObjectIdentifier{}
	return output, nil
}

func (m *DeleteObjectsErrorAfterZeroLengthMockS3) CheckBucketExists(ctx context.Context, bucketName *string) (bool, error) {
	return true, nil
}

type DeleteObjectsOutputErrorMockS3 struct{}

func NewDeleteObjectsOutputErrorMockS3() *DeleteObjectsOutputErrorMockS3 {
	return &DeleteObjectsOutputErrorMockS3{}
}

func (m *DeleteObjectsOutputErrorMockS3) DeleteBucket(ctx context.Context, bucketName *string) error {
	return nil
}

func (m *DeleteObjectsOutputErrorMockS3) DeleteObjects(ctx context.Context, bucketName *string, objects []types.ObjectIdentifier, sleepTimeSec int) ([]types.Error, error) {
	output := []types.Error{
		{
			Key:       aws.String("Key"),
			Code:      aws.String("Code"),
			Message:   aws.String("Message"),
			VersionId: aws.String("VersionId"),
		},
	}
	return output, nil
}

func (m *DeleteObjectsOutputErrorMockS3) ListObjectVersions(ctx context.Context, bucketName *string) ([]types.ObjectIdentifier, error) {
	output := []types.ObjectIdentifier{
		{
			Key:       aws.String("KeyForVersions"),
			VersionId: aws.String("VersionIdForVersions"),
		},
		{
			Key:       aws.String("KeyForDeleteMarkers"),
			VersionId: aws.String("VersionIdForDeleteMarkers"),
		},
	}
	return output, nil
}

func (m *DeleteObjectsOutputErrorMockS3) CheckBucketExists(ctx context.Context, bucketName *string) (bool, error) {
	return true, nil
}

type DeleteObjectsOutputErrorAfterZeroLengthMockS3 struct{}

func NewDeleteObjectsOutputErrorAfterZeroLengthMockS3() *DeleteObjectsOutputErrorAfterZeroLengthMockS3 {
	return &DeleteObjectsOutputErrorAfterZeroLengthMockS3{}
}

func (m *DeleteObjectsOutputErrorAfterZeroLengthMockS3) DeleteBucket(ctx context.Context, bucketName *string) error {
	return nil
}

func (m *DeleteObjectsOutputErrorAfterZeroLengthMockS3) DeleteObjects(ctx context.Context, bucketName *string, objects []types.ObjectIdentifier, sleepTimeSec int) ([]types.Error, error) {
	output := []types.Error{
		{
			Key:       aws.String("Key"),
			Code:      aws.String("Code"),
			Message:   aws.String("Message"),
			VersionId: aws.String("VersionId"),
		},
	}
	return output, nil
}

func (m *DeleteObjectsOutputErrorAfterZeroLengthMockS3) ListObjectVersions(ctx context.Context, bucketName *string) ([]types.ObjectIdentifier, error) {
	output := []types.ObjectIdentifier{}
	return output, nil
}

func (m *DeleteObjectsOutputErrorAfterZeroLengthMockS3) CheckBucketExists(ctx context.Context, bucketName *string) (bool, error) {
	return true, nil
}

type ListObjectVersionsErrorMockS3 struct{}

func NewListObjectVersionsErrorMockS3() *ListObjectVersionsErrorMockS3 {
	return &ListObjectVersionsErrorMockS3{}
}

func (m *ListObjectVersionsErrorMockS3) DeleteBucket(ctx context.Context, bucketName *string) error {
	return nil
}

func (m *ListObjectVersionsErrorMockS3) DeleteObjects(ctx context.Context, bucketName *string, objects []types.ObjectIdentifier, sleepTimeSec int) ([]types.Error, error) {
	return []types.Error{}, nil
}

func (m *ListObjectVersionsErrorMockS3) ListObjectVersions(ctx context.Context, bucketName *string) ([]types.ObjectIdentifier, error) {
	return nil, fmt.Errorf("ListObjectVersionsError")
}

func (m *ListObjectVersionsErrorMockS3) CheckBucketExists(ctx context.Context, bucketName *string) (bool, error) {
	return true, nil
}

type CheckBucketExistsErrorMockS3 struct{}

func NewCheckBucketExistsErrorMockS3() *CheckBucketExistsErrorMockS3 {
	return &CheckBucketExistsErrorMockS3{}
}

func (m *CheckBucketExistsErrorMockS3) DeleteBucket(ctx context.Context, bucketName *string) error {
	return nil
}

func (m *CheckBucketExistsErrorMockS3) DeleteObjects(ctx context.Context, bucketName *string, objects []types.ObjectIdentifier, sleepTimeSec int) ([]types.Error, error) {
	return []types.Error{}, nil
}

func (m *CheckBucketExistsErrorMockS3) ListObjectVersions(ctx context.Context, bucketName *string) ([]types.ObjectIdentifier, error) {
	output := []types.ObjectIdentifier{
		{
			Key:       aws.String("KeyForVersions"),
			VersionId: aws.String("VersionIdForVersions"),
		},
		{
			Key:       aws.String("KeyForDeleteMarkers"),
			VersionId: aws.String("VersionIdForDeleteMarkers"),
		},
	}
	return output, nil
}

func (m *CheckBucketExistsErrorMockS3) CheckBucketExists(ctx context.Context, bucketName *string) (bool, error) {
	return false, fmt.Errorf("ListBucketsError")
}

type CheckBucketNotExistsMockS3 struct{}

func NewCheckBucketNotExistsMockS3() *CheckBucketNotExistsMockS3 {
	return &CheckBucketNotExistsMockS3{}
}

func (m *CheckBucketNotExistsMockS3) DeleteBucket(ctx context.Context, bucketName *string) error {
	return nil
}

func (m *CheckBucketNotExistsMockS3) DeleteObjects(ctx context.Context, bucketName *string, objects []types.ObjectIdentifier, sleepTimeSec int) ([]types.Error, error) {
	return []types.Error{}, nil
}

func (m *CheckBucketNotExistsMockS3) ListObjectVersions(ctx context.Context, bucketName *string) ([]types.ObjectIdentifier, error) {
	output := []types.ObjectIdentifier{
		{
			Key:       aws.String("KeyForVersions"),
			VersionId: aws.String("VersionIdForVersions"),
		},
		{
			Key:       aws.String("KeyForDeleteMarkers"),
			VersionId: aws.String("VersionIdForDeleteMarkers"),
		},
	}
	return output, nil
}

func (m *CheckBucketNotExistsMockS3) CheckBucketExists(ctx context.Context, bucketName *string) (bool, error) {
	return false, nil
}

type ListObjectVersionsIncorrectRegionMockS3 struct{}

func NewListObjectVersionsIncorrectRegionMockS3() *ListObjectVersionsIncorrectRegionMockS3 {
	return &ListObjectVersionsIncorrectRegionMockS3{}
}

func (m *ListObjectVersionsIncorrectRegionMockS3) DeleteBucket(ctx context.Context, bucketName *string) error {
	return nil
}

func (m *ListObjectVersionsIncorrectRegionMockS3) DeleteObjects(ctx context.Context, bucketName *string, objects []types.ObjectIdentifier, sleepTimeSec int) ([]types.Error, error) {
	return []types.Error{}, nil
}

func (m *ListObjectVersionsIncorrectRegionMockS3) ListObjectVersions(ctx context.Context, bucketName *string) ([]types.ObjectIdentifier, error) {
	return nil, fmt.Errorf("api error PermanentRedirect")
}

func (m *ListObjectVersionsIncorrectRegionMockS3) CheckBucketExists(ctx context.Context, bucketName *string) (bool, error) {
	return true, nil
}

/*
	Test Cases
*/
func TestS3Wrapper_DeleteBucket(t *testing.T) {
	NewLogger(false)
	mock := NewMockS3()
	deleteBucketErrorMock := NewDeleteBucketErrorMockS3()
	deleteObjectsErrorMock := NewDeleteObjectsErrorMockS3()
	deleteObjectsErrorAfterZeroLengthMock := NewDeleteObjectsErrorAfterZeroLengthMockS3()
	deleteObjectsOutputErrorMock := NewDeleteObjectsOutputErrorMockS3()
	deleteObjectsOutputErrorAfterZeroLengthMock := NewDeleteObjectsOutputErrorAfterZeroLengthMockS3()
	listObjectVersionsErrorMock := NewListObjectVersionsErrorMockS3()
	checkBucketExistsErrorMock := NewCheckBucketExistsErrorMockS3()
	checkBucketNotExistsMock := NewCheckBucketNotExistsMockS3()
	listObjectVersionsIncorrectRegionMock := NewListObjectVersionsIncorrectRegionMockS3()

	type args struct {
		ctx        context.Context
		bucketName string
		forceMode  bool
		client     client.IS3
	}

	cases := []struct {
		name    string
		args    args
		want    error
		wantErr bool
	}{
		{
			name: "clear objects successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				client:     mock,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "delete bucket successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  true,
				client:     mock,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "clear objects failure for check bucket exists errors",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				client:     checkBucketExistsErrorMock,
			},
			want:    fmt.Errorf("ListBucketsError"),
			wantErr: true,
		},
		{
			name: "clear objects successfully for bucket not exists",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				client:     checkBucketNotExistsMock,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "clear objects failure for list object versions errors",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				client:     listObjectVersionsErrorMock,
			},
			want:    fmt.Errorf("ListObjectVersionsError"),
			wantErr: true,
		},
		{
			name: "clear objects failure for delete objects errors",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				client:     deleteObjectsErrorMock,
			},
			want:    fmt.Errorf("DeleteObjectsError"),
			wantErr: true,
		},
		{
			name: "clear objects successfully for delete objects errors after zero length",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				client:     deleteObjectsErrorAfterZeroLengthMock,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "clear objects failure for delete objects output errors",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				client:     deleteObjectsOutputErrorMock,
			},
			want:    fmt.Errorf("DeleteObjectsError: followings \nCode: Code\nKey: Key\nVersionId: VersionId\nMessage: Message\n"),
			wantErr: true,
		},
		{
			name: "clear objects successfully for delete objects output errors after zero length",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				client:     deleteObjectsOutputErrorAfterZeroLengthMock,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "delete bucket failure for delete objects errors",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  true,
				client:     deleteBucketErrorMock,
			},
			want:    fmt.Errorf("DeleteBucketError"),
			wantErr: true,
		},
		{
			name: "clear objects failure for list object versions invalid region errors",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				client:     listObjectVersionsIncorrectRegionMock,
			},
			want:    fmt.Errorf("PermanentRedirectError: Are you sure you are specifying the correct region?"),
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			s3 := NewS3Wrapper(tt.args.client)

			err := s3.ClearS3Objects(tt.args.ctx, tt.args.bucketName, tt.args.forceMode)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err.Error(), tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.Error())
				return
			}
		})
	}
}
