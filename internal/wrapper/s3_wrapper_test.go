package wrapper

import (
	"context"
	"fmt"
	reflect "reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/pkg/client"
	"github.com/golang/mock/gomock"
)

/*
	Test Cases
*/

func TestS3Wrapper_ClearS3Objects(t *testing.T) {
	io.NewLogger(false)

	type args struct {
		ctx                  context.Context
		bucketName           string
		forceMode            bool
		quietMode            bool
		directoryBucketsMode bool
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
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            false,
				quietMode:            false,
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), false).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions"),
							VersionId: aws.String("VersionIdForVersions"),
						},
						{
							Key:       aws.String("KeyForDeleteMarkers"),
							VersionId: aws.String("VersionIdForDeleteMarkers"),
						},
					}, nil, nil, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "clear objects on quiet mode successfully",
			args: args{
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            false,
				quietMode:            true,
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), false).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions"),
							VersionId: aws.String("VersionIdForVersions"),
						},
						{
							Key:       aws.String("KeyForDeleteMarkers"),
							VersionId: aws.String("VersionIdForDeleteMarkers"),
						},
					}, nil, nil, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "clear objects on directory buckets mode successfully",
			args: args{
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            false,
				quietMode:            false,
				directoryBucketsMode: true,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), true).Return(true, nil)
				m.EXPECT().ListObjectsByPage(gomock.Any(), aws.String("test"), "", nil).Return(
					[]types.ObjectIdentifier{
						{
							Key: aws.String("Key1"),
						},
						{
							Key: aws.String("Key2"),
						},
					}, nil, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "").Return([]types.Error{}, nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "delete bucket successfully",
			args: args{
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            true,
				quietMode:            false,
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), false).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions"),
							VersionId: aws.String("VersionIdForVersions"),
						},
						{
							Key:       aws.String("KeyForDeleteMarkers"),
							VersionId: aws.String("VersionIdForDeleteMarkers"),
						},
					}, nil, nil, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test"), "ap-northeast-1").Return(nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "clear objects failure for check bucket exists errors",
			args: args{
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            false,
				quietMode:            false,
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), false).Return(false, fmt.Errorf("ListBucketsError"))
			},
			want:    fmt.Errorf("ListBucketsError"),
			wantErr: true,
		},
		{
			name: "clear objects successfully for bucket not exists",
			args: args{
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            false,
				quietMode:            false,
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), false).Return(false, nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "clear objects failure for get bucket location errors",
			args: args{
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            false,
				quietMode:            false,
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), false).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("", fmt.Errorf("GetBucketLocationError"))
			},
			want:    fmt.Errorf("GetBucketLocationError"),
			wantErr: true,
		},
		{
			name: "clear objects failure for list object versions errors",
			args: args{
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            false,
				quietMode:            false,
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), false).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(nil, nil, nil, fmt.Errorf("ListObjectVersionsByPageError"))
			},
			want:    fmt.Errorf("ListObjectVersionsByPageError"),
			wantErr: true,
		},
		{
			name: "clear objects failure for delete objects errors",
			args: args{
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            false,
				quietMode:            false,
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), false).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions"),
							VersionId: aws.String("VersionIdForVersions"),
						},
						{
							Key:       aws.String("KeyForDeleteMarkers"),
							VersionId: aws.String("VersionIdForDeleteMarkers"),
						},
					}, nil, nil, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, fmt.Errorf("DeleteObjectsError"))
			},
			want:    fmt.Errorf("DeleteObjectsError"),
			wantErr: true,
		},
		{
			name: "clear objects failure for delete objects output errors",
			args: args{
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            false,
				quietMode:            false,
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), false).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions"),
							VersionId: aws.String("VersionIdForVersions"),
						},
						{
							Key:       aws.String("KeyForDeleteMarkers"),
							VersionId: aws.String("VersionIdForDeleteMarkers"),
						},
					}, nil, nil, nil)
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
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            true,
				quietMode:            false,
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), false).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions"),
							VersionId: aws.String("VersionIdForVersions"),
						},
						{
							Key:       aws.String("KeyForDeleteMarkers"),
							VersionId: aws.String("VersionIdForDeleteMarkers"),
						},
					}, nil, nil, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "ap-northeast-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test"), "ap-northeast-1").Return(fmt.Errorf("DeleteBucketError"))
			},
			want:    fmt.Errorf("DeleteBucketError"),
			wantErr: true,
		},
		{
			name: "delete bucket successfully after zero length",
			args: args{
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            true,
				quietMode:            false,
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), false).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return([]types.ObjectIdentifier{}, nil, nil, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test"), "ap-northeast-1").Return(nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "delete bucket failure for delete bucket errors after zero length",
			args: args{
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            true,
				quietMode:            false,
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), false).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return([]types.ObjectIdentifier{}, nil, nil, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test"), "ap-northeast-1").Return(fmt.Errorf("DeleteBucketError"))
			},
			want:    fmt.Errorf("DeleteBucketError"),
			wantErr: true,
		},
		{
			name: "clear objects successfully if several loops are executed",
			args: args{
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            false,
				quietMode:            false,
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), false).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions1"),
							VersionId: aws.String("VersionIdForVersions1"),
						},
					},
					aws.String("NextKeyMarker1"),
					aws.String("NextVersionIdMarker1"),
					nil,
				)
				m.EXPECT().ListObjectVersionsByPage(
					gomock.Any(),
					aws.String("test"),
					"ap-northeast-1",
					false,
					aws.String("NextKeyMarker1"),
					aws.String("NextVersionIdMarker1"),
				).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions2"),
							VersionId: aws.String("VersionIdForVersions2"),
						},
					},
					aws.String("NextKeyMarker2"),
					aws.String("NextVersionIdMarker2"),
					nil,
				)
				m.EXPECT().ListObjectVersionsByPage(
					gomock.Any(),
					aws.String("test"),
					"ap-northeast-1",
					false,
					aws.String("NextKeyMarker2"),
					aws.String("NextVersionIdMarker2"),
				).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions3"),
							VersionId: aws.String("VersionIdForVersions3"),
						},
					}, nil, nil, nil)
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
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            false,
				quietMode:            false,
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), false).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions1"),
							VersionId: aws.String("VersionIdForVersions1"),
						},
					},
					aws.String("NextKeyMarker1"),
					aws.String("NextVersionIdMarker1"),
					nil,
				)
				m.EXPECT().ListObjectVersionsByPage(
					gomock.Any(),
					aws.String("test"),
					"ap-northeast-1",
					false,
					aws.String("NextKeyMarker1"),
					aws.String("NextVersionIdMarker1"),
				).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions2"),
							VersionId: aws.String("VersionIdForVersions2"),
						},
					},
					aws.String("NextKeyMarker2"),
					aws.String("NextVersionIdMarker2"),
					nil,
				)
				m.EXPECT().ListObjectVersionsByPage(
					gomock.Any(),
					aws.String("test"),
					"ap-northeast-1",
					false,
					aws.String("NextKeyMarker2"),
					aws.String("NextVersionIdMarker2"),
				).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions3"),
							VersionId: aws.String("VersionIdForVersions3"),
						},
					}, nil, nil, nil)
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
				ctx:                  context.Background(),
				bucketName:           "test",
				forceMode:            false,
				quietMode:            false,
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().CheckBucketExists(gomock.Any(), aws.String("test"), false).Return(true, nil)
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("ap-northeast-1", nil)
				m.EXPECT().ListObjectVersionsByPage(gomock.Any(), aws.String("test"), "ap-northeast-1", false, nil, nil).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions1"),
							VersionId: aws.String("VersionIdForVersions1"),
						},
					},
					aws.String("NextKeyMarker1"),
					aws.String("NextVersionIdMarker1"),
					nil,
				)
				m.EXPECT().ListObjectVersionsByPage(
					gomock.Any(),
					aws.String("test"),
					"ap-northeast-1",
					false,
					aws.String("NextKeyMarker1"),
					aws.String("NextVersionIdMarker1"),
				).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions2"),
							VersionId: aws.String("VersionIdForVersions2"),
						},
					},
					aws.String("NextKeyMarker2"),
					aws.String("NextVersionIdMarker2"),
					nil,
				)
				m.EXPECT().ListObjectVersionsByPage(
					gomock.Any(),
					aws.String("test"),
					"ap-northeast-1",
					false,
					aws.String("NextKeyMarker2"),
					aws.String("NextVersionIdMarker2"),
				).Return(
					[]types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions3"),
							VersionId: aws.String("VersionIdForVersions3"),
						},
					}, nil, nil, nil)
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

			err := s3.ClearS3Objects(tt.args.ctx, tt.args.bucketName, tt.args.forceMode, false, tt.args.quietMode, tt.args.directoryBucketsMode)
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

func TestS3Wrapper_ListBucketNamesFilteredByKeyword(t *testing.T) {
	io.NewLogger(false)

	type args struct {
		ctx                  context.Context
		keyword              string
		directoryBucketsMode bool
	}

	type want struct {
		output []string
		err    error
	}

	cases := []struct {
		name          string
		args          args
		prepareMockFn func(m *client.MockIS3)
		want          want
		wantErr       bool
	}{
		{
			name: "list a bucket filtered by keyword successfully",
			args: args{
				ctx:                  context.Background(),
				keyword:              "test",
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBuckets(gomock.Any()).Return([]types.Bucket{
					{Name: aws.String("test1")},
				}, nil)
			},
			want: want{
				output: []string{
					"test1",
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list a bucket filtered by keyword on directory buckets mode successfully",
			args: args{
				ctx:                  context.Background(),
				keyword:              "test",
				directoryBucketsMode: true,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListDirectoryBuckets(gomock.Any()).Return([]types.Bucket{
					{Name: aws.String("test1")},
				}, nil)
			},
			want: want{
				output: []string{
					"test1",
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list buckets filtered by keyword successfully",
			args: args{
				ctx:                  context.Background(),
				keyword:              "test",
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBuckets(gomock.Any()).Return([]types.Bucket{
					{Name: aws.String("test1")},
					{Name: aws.String("test2")},
					{Name: aws.String("other")},
				}, nil)
			},
			want: want{
				output: []string{
					"test1",
					"test2",
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list buckets filtered by keyword successfully when keyword is empty",
			args: args{
				ctx:                  context.Background(),
				keyword:              "",
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBuckets(gomock.Any()).Return([]types.Bucket{
					{Name: aws.String("test1")},
					{Name: aws.String("test2")},
					{Name: aws.String("other")},
				}, nil)
			},
			want: want{
				output: []string{
					"test1",
					"test2",
					"other",
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list buckets filtered by keyword successfully but not match",
			args: args{
				ctx:                  context.Background(),
				keyword:              "test",
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBuckets(gomock.Any()).Return([]types.Bucket{
					{Name: aws.String("other1")},
					{Name: aws.String("other2")},
					{Name: aws.String("other3")},
				}, nil)
			},
			want: want{
				output: []string{},
				err:    nil,
			},
			wantErr: false,
		},
		{
			name: "list buckets filtered by keyword successfully but not return buckets",
			args: args{
				ctx:                  context.Background(),
				keyword:              "test",
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBuckets(gomock.Any()).Return([]types.Bucket{}, nil)
			},
			want: want{
				output: []string{},
				err:    nil,
			},
			wantErr: false,
		},
		{
			name: "list buckets filtered by keyword successfully but not return buckets when keyword is empty",
			args: args{
				ctx:                  context.Background(),
				keyword:              "",
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBuckets(gomock.Any()).Return([]types.Bucket{}, nil)
			},
			want: want{
				output: []string{},
				err:    nil,
			},
			wantErr: false,
		},
		{
			name: "list buckets filtered by keyword failure",
			args: args{
				ctx:                  context.Background(),
				keyword:              "test",
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBuckets(gomock.Any()).Return([]types.Bucket{}, fmt.Errorf("ListBucketsError"))
			},
			want: want{
				output: []string{},
				err:    fmt.Errorf("ListBucketsError"),
			},
			wantErr: true,
		},
		{
			name: "list buckets filtered by keyword successfully for case-insensitive search",
			args: args{
				ctx:                  context.Background(),
				keyword:              "TEST",
				directoryBucketsMode: false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBuckets(gomock.Any()).Return([]types.Bucket{
					{Name: aws.String("test1")},
					{Name: aws.String("test2")},
					{Name: aws.String("other")},
				}, nil)
			},
			want: want{
				output: []string{
					"test1",
					"test2",
				},
				err: nil,
			},
			wantErr: false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			s3Mock := client.NewMockIS3(ctrl)
			tt.prepareMockFn(s3Mock)

			s3 := NewS3Wrapper(s3Mock)

			output, err := s3.ListBucketNamesFilteredByKeyword(tt.args.ctx, aws.String(tt.args.keyword), tt.args.directoryBucketsMode)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.err.Error() {
				t.Errorf("err = %#v, want %#v", err, tt.want)
				return
			}
			if !reflect.DeepEqual(output, tt.want.output) {
				t.Errorf("output = %#v, want %#v", output, tt.want.output)
			}
		})
	}
}
