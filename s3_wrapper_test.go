package cls3

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-to-k/delstack/pkg/client"
	"github.com/golang/mock/gomock"
)

/*
	Test Cases
*/

func TestS3Wrapper_DeleteBucket(t *testing.T) {
	NewLogger(false)

	type args struct {
		ctx        context.Context
		bucketName string
		forceMode  bool
	}

	cases := []struct {
		name          string
		args          args
		prepareMockFn func(m *client.MockIS3)
		want          error
		wantErr       bool
	}{
		{
			name: "clear objects successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().ListObjectVersions(gomock.Any(), aws.String("test")).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions"),
							VersionId: aws.String("VersionIdForVersions"),
						},
						{
							Key:       aws.String("KeyForDeleteMarkers"),
							VersionId: aws.String("VersionIdForDeleteMarkers"),
						},
					}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any()).Return([]types.Error{}, nil)
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
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().ListObjectVersions(gomock.Any(), aws.String("test")).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions"),
							VersionId: aws.String("VersionIdForVersions"),
						},
						{
							Key:       aws.String("KeyForDeleteMarkers"),
							VersionId: aws.String("VersionIdForDeleteMarkers"),
						},
					}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any()).Return([]types.Error{}, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test")).Return(nil)
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
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(false, fmt.Errorf("ListBucketsError"))
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
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(false, nil)
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
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().ListObjectVersions(gomock.Any(), aws.String("test")).Return(nil, fmt.Errorf("ListObjectVersionsError"))
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
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().ListObjectVersions(gomock.Any(), aws.String("test")).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions"),
							VersionId: aws.String("VersionIdForVersions"),
						},
						{
							Key:       aws.String("KeyForDeleteMarkers"),
							VersionId: aws.String("VersionIdForDeleteMarkers"),
						},
					}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any()).Return([]types.Error{}, fmt.Errorf("DeleteObjectsError"))
			},
			want:    fmt.Errorf("DeleteObjectsError"),
			wantErr: true,
		},
		{
			name: "clear objects failure for delete objects output errors",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().ListObjectVersions(gomock.Any(), aws.String("test")).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions"),
							VersionId: aws.String("VersionIdForVersions"),
						},
						{
							Key:       aws.String("KeyForDeleteMarkers"),
							VersionId: aws.String("VersionIdForDeleteMarkers"),
						},
					}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any()).Return(
					[]types.Error{
						{
							Key:       aws.String("Key"),
							Code:      aws.String("Code"),
							Message:   aws.String("Message"),
							VersionId: aws.String("VersionId"),
						},
					}, nil,
				)
			},
			want:    fmt.Errorf("DeleteObjectsError: followings \nCode: Code\nKey: Key\nVersionId: VersionId\nMessage: Message\n"),
			wantErr: true,
		},
		{
			name: "delete bucket failure for delete bucket errors",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  true,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().ListObjectVersions(gomock.Any(), aws.String("test")).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions"),
							VersionId: aws.String("VersionIdForVersions"),
						},
						{
							Key:       aws.String("KeyForDeleteMarkers"),
							VersionId: aws.String("VersionIdForDeleteMarkers"),
						},
					}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any()).Return([]types.Error{}, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test")).Return(fmt.Errorf("DeleteBucketError"))
			},
			want:    fmt.Errorf("DeleteBucketError"),
			wantErr: true,
		},
		{
			name: "delete bucket successfully after zero length",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  true,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().ListObjectVersions(gomock.Any(), aws.String("test")).Return([]types.ObjectIdentifier{}, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test")).Return(nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "delete bucket failure for delete bucket errors after zero length",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  true,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().ListObjectVersions(gomock.Any(), aws.String("test")).Return([]types.ObjectIdentifier{}, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test")).Return(fmt.Errorf("DeleteBucketError"))
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
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().ListObjectVersions(gomock.Any(), aws.String("test")).Return(nil, fmt.Errorf("api error PermanentRedirect"))
			},
			want:    fmt.Errorf("PermanentRedirectError: Are you sure you are specifying the correct region?"),
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			s3Mock := client.NewMockIS3(ctrl)
			tt.prepareMockFn(s3Mock)

			s3 := NewS3Wrapper(s3Mock)

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
