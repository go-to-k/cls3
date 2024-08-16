package wrapper

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/pkg/client"
	"go.uber.org/mock/gomock"
)

/*
	Test Cases
*/

func TestS3Wrapper_ClearS3Objects(t *testing.T) {
	io.NewLogger(false)

	type args struct {
		ctx        context.Context
		bucketName string
		forceMode  bool
		quietMode  bool
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
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions"),
								VersionId: aws.String("VersionIdForVersions"),
							},
							{
								Key:       aws.String("KeyForDeleteMarkers"),
								VersionId: aws.String("VersionIdForDeleteMarkers"),
							},
						},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "clear objects on quiet mode successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				quietMode:  true,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions"),
								VersionId: aws.String("VersionIdForVersions"),
							},
							{
								Key:       aws.String("KeyForDeleteMarkers"),
								VersionId: aws.String("VersionIdForDeleteMarkers"),
							},
						},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, nil)
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
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions"),
								VersionId: aws.String("VersionIdForVersions"),
							},
							{
								Key:       aws.String("KeyForDeleteMarkers"),
								VersionId: aws.String("VersionIdForDeleteMarkers"),
							},
						},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test"), "ap-northeast-1").Return(nil)
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
				quietMode:  false,
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
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(false, nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "clear objects failure for get bucket location errors",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("", fmt.Errorf("GetBucketLocationError"))
			},
			want:    fmt.Errorf("GetBucketLocationError"),
			wantErr: true,
		},
		{
			name: "clear objects failure for list object versions errors",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(nil, fmt.Errorf("ListObjectVersionsByPageError"))
			},
			want:    fmt.Errorf("ListObjectVersionsByPageError"),
			wantErr: true,
		},
		{
			name: "clear objects failure for delete objects errors",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions"),
								VersionId: aws.String("VersionIdForVersions"),
							},
							{
								Key:       aws.String("KeyForDeleteMarkers"),
								VersionId: aws.String("VersionIdForDeleteMarkers"),
							},
						},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, fmt.Errorf("DeleteObjectsError"))
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
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions"),
								VersionId: aws.String("VersionIdForVersions"),
							},
							{
								Key:       aws.String("KeyForDeleteMarkers"),
								VersionId: aws.String("VersionIdForDeleteMarkers"),
							},
						},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return(
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
			want:    fmt.Errorf("[resource test] DeleteObjectsError: 1 objects with errors were found. \nCode: Code\nKey: Key\nVersionId: VersionId\nMessage: Message\n"),
			wantErr: true,
		},
		{
			name: "delete bucket failure for delete bucket errors",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  true,
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions"),
								VersionId: aws.String("VersionIdForVersions"),
							},
							{
								Key:       aws.String("KeyForDeleteMarkers"),
								VersionId: aws.String("VersionIdForDeleteMarkers"),
							},
						},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test"), "ap-northeast-1").Return(fmt.Errorf("DeleteBucketError"))
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
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers:   []types.ObjectIdentifier{},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test"), "ap-northeast-1").Return(nil)
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
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(&client.ListObjectsOrVersionsByPageOutput{
					ObjectIdentifiers:   []types.ObjectIdentifier{},
					NextKeyMarker:       nil,
					NextVersionIdMarker: nil,
				}, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test"), "ap-northeast-1").Return(fmt.Errorf("DeleteBucketError"))
			},
			want:    fmt.Errorf("DeleteBucketError"),
			wantErr: true,
		},
		{
			name: "clear objects successfully if several loops are executed",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions1"),
								VersionId: aws.String("VersionIdForVersions1"),
							},
						},
						NextKeyMarker:       aws.String("NextKeyMarker1"),
						NextVersionIdMarker: aws.String("NextVersionIdMarker1"),
					},
					nil,
				)
				m.EXPECT().ListObjectsOrVersionsByPage(
					gomock.Any(),
					aws.String("test"),
					"ap-northeast-1",
					false,
					aws.String("NextKeyMarker1"),
					aws.String("NextVersionIdMarker1"),
				).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions2"),
								VersionId: aws.String("VersionIdForVersions2"),
							},
						},
						NextKeyMarker:       aws.String("NextKeyMarker2"),
						NextVersionIdMarker: aws.String("NextVersionIdMarker2"),
					},
					nil,
				)
				m.EXPECT().ListObjectsOrVersionsByPage(
					gomock.Any(),
					aws.String("test"),
					"ap-northeast-1",
					false,
					aws.String("NextKeyMarker2"),
					aws.String("NextVersionIdMarker2"),
				).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions3"),
								VersionId: aws.String("VersionIdForVersions3"),
							},
						},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "clear objects failure for delete objects outputs errors if several loops are executed",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions1"),
								VersionId: aws.String("VersionIdForVersions1"),
							},
						},
						NextKeyMarker:       aws.String("NextKeyMarker1"),
						NextVersionIdMarker: aws.String("NextVersionIdMarker1"),
					},
					nil,
				)
				m.EXPECT().ListObjectsOrVersionsByPage(
					gomock.Any(),
					aws.String("test"),
					"ap-northeast-1",
					false,
					aws.String("NextKeyMarker1"),
					aws.String("NextVersionIdMarker1"),
				).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions2"),
								VersionId: aws.String("VersionIdForVersions2"),
							},
						},
						NextKeyMarker:       aws.String("NextKeyMarker2"),
						NextVersionIdMarker: aws.String("NextVersionIdMarker2"),
					},
					nil,
				)
				m.EXPECT().ListObjectsOrVersionsByPage(
					gomock.Any(),
					aws.String("test"),
					"ap-northeast-1",
					false,
					aws.String("NextKeyMarker2"),
					aws.String("NextVersionIdMarker2"),
				).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions3"),
								VersionId: aws.String("VersionIdForVersions3"),
							},
						},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return(
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
			want:    fmt.Errorf("[resource test] DeleteObjectsError: 1 objects with errors were found. \nCode: Code\nKey: Key\nVersionId: VersionId\nMessage: Message\n"),
			wantErr: true,
		},
		{
			name: "clear objects failure for delete objects errors if several loops are executed",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test")).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions1"),
								VersionId: aws.String("VersionIdForVersions1"),
							},
						},
						NextKeyMarker:       aws.String("NextKeyMarker1"),
						NextVersionIdMarker: aws.String("NextVersionIdMarker1"),
					},
					nil,
				)
				m.EXPECT().ListObjectsOrVersionsByPage(
					gomock.Any(),
					aws.String("test"),
					"ap-northeast-1",
					false,
					aws.String("NextKeyMarker1"),
					aws.String("NextVersionIdMarker1"),
				).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions2"),
								VersionId: aws.String("VersionIdForVersions2"),
							},
						},
						NextKeyMarker:       aws.String("NextKeyMarker2"),
						NextVersionIdMarker: aws.String("NextVersionIdMarker2"),
					},
					nil,
				)
				m.EXPECT().ListObjectsOrVersionsByPage(
					gomock.Any(),
					aws.String("test"),
					"ap-northeast-1",
					false,
					aws.String("NextKeyMarker2"),
					aws.String("NextVersionIdMarker2"),
				).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions3"),
								VersionId: aws.String("VersionIdForVersions3"),
							},
						},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, fmt.Errorf("DeleteObjectsError"))
			},
			want:    fmt.Errorf("DeleteObjectsError"),
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			s3Mock := client.NewMockIS3(ctrl)
			tt.prepareMockFn(s3Mock)

			s3 := NewS3Wrapper(s3Mock)

			err := s3.ClearS3Objects(tt.args.ctx, tt.args.bucketName, tt.args.forceMode, false, tt.args.quietMode)
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
