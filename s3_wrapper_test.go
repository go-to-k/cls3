package cls3

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-to-k/delstack/pkg/client"
)

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
