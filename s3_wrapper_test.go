package cls3

import (
	"context"
	"fmt"
	reflect "reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
		prepareMockFn func(m *MockIS3)
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
			prepareMockFn: func(m *MockIS3) {
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
			prepareMockFn: func(m *MockIS3) {
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
			prepareMockFn: func(m *MockIS3) {
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
			prepareMockFn: func(m *MockIS3) {
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
			prepareMockFn: func(m *MockIS3) {
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
			prepareMockFn: func(m *MockIS3) {
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
			prepareMockFn: func(m *MockIS3) {
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
			prepareMockFn: func(m *MockIS3) {
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
			prepareMockFn: func(m *MockIS3) {
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
			prepareMockFn: func(m *MockIS3) {
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
			prepareMockFn: func(m *MockIS3) {
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
			s3Mock := NewMockIS3(ctrl)
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

func TestS3Wrapper_ListBucketNamesFilteredByKeyword(t *testing.T) {
	NewLogger(false)

	type args struct {
		ctx     context.Context
		keyword string
	}

	type want struct {
		output []string
		err    error
	}

	cases := []struct {
		name          string
		args          args
		prepareMockFn func(m *MockIS3)
		want          want
		wantErr       bool
	}{
		{
			name: "list a bucket filtered by keyword successfully",
			args: args{
				ctx:     context.Background(),
				keyword: "test",
			},
			prepareMockFn: func(m *MockIS3) {
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
			name: "list buckets filtered by keyword successfully",
			args: args{
				ctx:     context.Background(),
				keyword: "test",
			},
			prepareMockFn: func(m *MockIS3) {
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
				ctx:     context.Background(),
				keyword: "",
			},
			prepareMockFn: func(m *MockIS3) {
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
				ctx:     context.Background(),
				keyword: "test",
			},
			prepareMockFn: func(m *MockIS3) {
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
				ctx:     context.Background(),
				keyword: "test",
			},
			prepareMockFn: func(m *MockIS3) {
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
				ctx:     context.Background(),
				keyword: "",
			},
			prepareMockFn: func(m *MockIS3) {
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
				ctx:     context.Background(),
				keyword: "test",
			},
			prepareMockFn: func(m *MockIS3) {
				m.EXPECT().ListBuckets(gomock.Any()).Return([]types.Bucket{}, fmt.Errorf("ListBucketsError"))
			},
			want: want{
				output: []string{},
				err:    fmt.Errorf("ListBucketsError"),
			},
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			s3Mock := NewMockIS3(ctrl)
			tt.prepareMockFn(s3Mock)

			s3 := NewS3Wrapper(s3Mock)

			output, err := s3.ListBucketNamesFilteredByKeyword(tt.args.ctx, aws.String(tt.args.keyword))
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
