package wrapper

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/pkg/client"
	"github.com/rs/zerolog"
	"go.uber.org/mock/gomock"
)

/*
	Test Cases
*/

func TestS3Wrapper_ClearBucket(t *testing.T) {
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
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("us-east-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
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
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return([]types.Error{}, nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers:   []types.ObjectIdentifier{},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
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
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("us-east-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
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
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return([]types.Error{}, nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers:   []types.ObjectIdentifier{},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
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
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("us-east-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
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
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return([]types.Error{}, nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers:   []types.ObjectIdentifier{},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test"), "us-east-1").Return(nil)
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
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("us-east-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(nil, fmt.Errorf("ListObjectVersionsByPageError"))
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
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("us-east-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
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
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return([]types.Error{}, fmt.Errorf("DeleteObjectsError"))
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
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("us-east-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
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
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return(
					[]types.Error{
						{
							Key:       aws.String("Key"),
							Code:      aws.String("Code"),
							Message:   aws.String("Message"),
							VersionId: aws.String("VersionId"),
						},
					}, nil,
				)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers:   []types.ObjectIdentifier{},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
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
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("us-east-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
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
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return([]types.Error{}, nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers:   []types.ObjectIdentifier{},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test"), "us-east-1").Return(fmt.Errorf("DeleteBucketError"))
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
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("us-east-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers:   []types.ObjectIdentifier{},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test"), "us-east-1").Return(nil)
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
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("us-east-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(&client.ListObjectsOrVersionsByPageOutput{
					ObjectIdentifiers:   []types.ObjectIdentifier{},
					NextKeyMarker:       nil,
					NextVersionIdMarker: nil,
				}, nil)
				m.EXPECT().DeleteBucket(gomock.Any(), aws.String("test"), "us-east-1").Return(fmt.Errorf("DeleteBucketError"))
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
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("us-east-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
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
					"us-east-1",
					false,
					aws.String("NextKeyMarker1"),
					aws.String("NextVersionIdMarker1"),
					nil,
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
					"us-east-1",
					false,
					aws.String("NextKeyMarker2"),
					aws.String("NextVersionIdMarker2"),
					nil,
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
					},
					nil,
				)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return([]types.Error{}, nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers:   []types.ObjectIdentifier{},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
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
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("us-east-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
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
					"us-east-1",
					false,
					aws.String("NextKeyMarker1"),
					aws.String("NextVersionIdMarker1"),
					nil,
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
					"us-east-1",
					false,
					aws.String("NextKeyMarker2"),
					aws.String("NextVersionIdMarker2"),
					nil,
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
					},
					nil,
				)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return(
					[]types.Error{
						{
							Key:       aws.String("Key"),
							Code:      aws.String("Code"),
							Message:   aws.String("Message"),
							VersionId: aws.String("VersionId"),
						},
					}, nil,
				)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers:   []types.ObjectIdentifier{},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					}, nil)
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
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("us-east-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
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
					"us-east-1",
					false,
					aws.String("NextKeyMarker1"),
					aws.String("NextVersionIdMarker1"),
					nil,
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
					"us-east-1",
					false,
					aws.String("NextKeyMarker2"),
					aws.String("NextVersionIdMarker2"),
					nil,
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
					},
					nil,
				)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return([]types.Error{}, nil)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return([]types.Error{}, fmt.Errorf("DeleteObjectsError"))
			},
			want:    fmt.Errorf("DeleteObjectsError"),
			wantErr: true,
		},
		{
			name: "clear objects successfully when retry deletion loop is executed",
			args: args{
				ctx:        context.Background(),
				bucketName: "test",
				forceMode:  false,
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().GetBucketLocation(gomock.Any(), aws.String("test")).Return("us-east-1", nil)
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions1"),
								VersionId: aws.String("VersionIdForVersions1"),
							},
						},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					},
					nil,
				)
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return([]types.Error{}, nil)
				// retry loop
				m.EXPECT().ListObjectsOrVersionsByPage(gomock.Any(), aws.String("test"), "us-east-1", false, nil, nil, nil).Return(
					&client.ListObjectsOrVersionsByPageOutput{
						ObjectIdentifiers: []types.ObjectIdentifier{
							{
								Key:       aws.String("KeyForVersions1"),
								VersionId: aws.String("VersionIdForVersions1"),
							},
						},
						NextKeyMarker:       nil,
						NextVersionIdMarker: nil,
					},
					nil,
				)
				// retry deletion
				m.EXPECT().DeleteObjects(gomock.Any(), aws.String("test"), gomock.Any(), "us-east-1").Return([]types.Error{}, nil)
			},
			want:    nil,
			wantErr: false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			s3Mock := client.NewMockIS3(ctrl)
			tt.prepareMockFn(s3Mock)

			s3 := NewS3Wrapper(s3Mock)

			clearingCountCh := make(chan int64)
			if !tt.args.quietMode {
				go func() {
					for range clearingCountCh {
					}
				}()
			}

			err := s3.ClearBucket(tt.args.ctx, ClearBucketInput{
				TargetBucket:    tt.args.bucketName,
				ForceMode:       tt.args.forceMode,
				QuietMode:       tt.args.quietMode,
				ClearingCountCh: clearingCountCh,
			})

			close(clearingCountCh)

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
		ctx     context.Context
		keyword *string
	}

	type want struct {
		output []ListBucketNamesFilteredByKeywordOutput
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
			name: "list buckets filtered by keyword successfully",
			args: args{
				ctx:     context.Background(),
				keyword: aws.String("test"),
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBucketsOrDirectoryBuckets(gomock.Any()).Return(
					[]types.Bucket{
						{
							Name: aws.String("test1"),
						},
						{
							Name: aws.String("test2"),
						},
						{
							Name: aws.String("other"),
						},
					},
					nil,
				)
			},
			want: want{
				output: []ListBucketNamesFilteredByKeywordOutput{
					{
						BucketName:   "test1",
						TargetBucket: "test1",
					},
					{
						BucketName:   "test2",
						TargetBucket: "test2",
					},
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list buckets filtered by keyword successfully when keyword is empty",
			args: args{
				ctx:     context.Background(),
				keyword: aws.String(""),
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBucketsOrDirectoryBuckets(gomock.Any()).Return(
					[]types.Bucket{
						{
							Name: aws.String("test1"),
						},
						{
							Name: aws.String("test2"),
						},
						{
							Name: aws.String("other"),
						},
					},
					nil,
				)
			},
			want: want{
				output: []ListBucketNamesFilteredByKeywordOutput{
					{
						BucketName:   "test1",
						TargetBucket: "test1",
					},
					{
						BucketName:   "test2",
						TargetBucket: "test2",
					},
					{
						BucketName:   "other",
						TargetBucket: "other",
					},
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list buckets filtered by keyword successfully but not match",
			args: args{
				ctx:     context.Background(),
				keyword: aws.String("test"),
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBucketsOrDirectoryBuckets(gomock.Any()).Return(
					[]types.Bucket{
						{
							Name: aws.String("other1"),
						},
						{
							Name: aws.String("other2"),
						},
						{
							Name: aws.String("other3"),
						},
					},
					nil,
				)
			},
			want: want{
				output: []ListBucketNamesFilteredByKeywordOutput{},
				err:    fmt.Errorf("[resource -] NotExistsError: No buckets matching the keyword test."),
			},
			wantErr: true,
		},
		{
			name: "list buckets filtered by keyword successfully but not return buckets",
			args: args{
				ctx:     context.Background(),
				keyword: aws.String("test"),
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBucketsOrDirectoryBuckets(gomock.Any()).Return(
					[]types.Bucket{},
					nil,
				)
			},
			want: want{
				output: []ListBucketNamesFilteredByKeywordOutput{},
				err:    fmt.Errorf("[resource -] NotExistsError: No buckets matching the keyword test."),
			},
			wantErr: true,
		},
		{
			name: "list buckets filtered by keyword successfully but not return buckets when keyword is empty",
			args: args{
				ctx:     context.Background(),
				keyword: aws.String(""),
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBucketsOrDirectoryBuckets(gomock.Any()).Return(
					[]types.Bucket{},
					nil,
				)
			},
			want: want{
				output: []ListBucketNamesFilteredByKeywordOutput{},
				err:    fmt.Errorf("[resource -] NotExistsError: No buckets matching the keyword ."),
			},
			wantErr: true,
		},
		{
			name: "list buckets filtered by keyword failure",
			args: args{
				ctx:     context.Background(),
				keyword: aws.String("test"),
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBucketsOrDirectoryBuckets(gomock.Any()).Return(
					[]types.Bucket{},
					fmt.Errorf("ListBucketError"),
				)
			},
			want: want{
				output: []ListBucketNamesFilteredByKeywordOutput{},
				err:    fmt.Errorf("ListBucketError"),
			},
			wantErr: true,
		},
		{
			name: "list buckets filtered by keyword successfully for case-insensitive search",
			args: args{
				ctx:     context.Background(),
				keyword: aws.String("TEST"),
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBucketsOrDirectoryBuckets(gomock.Any()).Return(
					[]types.Bucket{
						{
							Name: aws.String("test1"),
						},
						{
							Name: aws.String("test2"),
						},
						{
							Name: aws.String("other"),
						},
					},
					nil,
				)
			},
			want: want{
				output: []ListBucketNamesFilteredByKeywordOutput{
					{
						BucketName:   "test1",
						TargetBucket: "test1",
					},
					{
						BucketName:   "test2",
						TargetBucket: "test2",
					},
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

			output, err := s3.ListBucketNamesFilteredByKeyword(tt.args.ctx, tt.args.keyword)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err.Error(), tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.err.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.err.Error())
				return
			}
			if !reflect.DeepEqual(output, tt.want.output) {
				t.Errorf("output = %#v, want %#v", output, tt.want.output)
			}
		})
	}
}

func TestS3Wrapper_CheckAllBucketsExist(t *testing.T) {
	io.NewLogger(false)

	type args struct {
		ctx         context.Context
		bucketNames []string
	}

	type want struct {
		bucketNames []string
		err         error
	}

	cases := []struct {
		name          string
		args          args
		prepareMockFn func(m *client.MockIS3)
		want          want
		wantErr       bool
	}{
		{
			name: "all buckets exist",
			args: args{
				ctx:         context.Background(),
				bucketNames: []string{"test1", "test2"},
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBucketsOrDirectoryBuckets(gomock.Any()).Return(
					[]types.Bucket{
						{
							Name: aws.String("test1"),
						},
						{
							Name: aws.String("test2"),
						},
					},
					nil,
				)
			},
			want: want{
				bucketNames: []string{"test1", "test2"},
				err:         nil,
			},
			wantErr: false,
		},
		{
			name: "part of bucket does not exist",
			args: args{
				ctx:         context.Background(),
				bucketNames: []string{"test1", "test2"},
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBucketsOrDirectoryBuckets(gomock.Any()).Return(
					[]types.Bucket{
						{
							Name: aws.String("test2"),
						},
					},
					nil,
				)
			},
			want: want{
				bucketNames: []string{"test2"},
				err:         fmt.Errorf("[resource -] NotExistsError: The following buckets do not exist: test1"),
			},
			wantErr: true,
		},
		{
			name: "ListBucketsOrDirectoryBuckets returns empty",
			args: args{
				ctx:         context.Background(),
				bucketNames: []string{"test1", "test2"},
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBucketsOrDirectoryBuckets(gomock.Any()).Return(
					[]types.Bucket{},
					nil,
				)
			},
			want: want{
				bucketNames: []string{},
				err:         fmt.Errorf("[resource -] NotExistsError: The following buckets do not exist: test1, test2"),
			},
			wantErr: true,
		},
		{
			name: "ListBucketsOrDirectoryBuckets returns an error",
			args: args{
				ctx:         context.Background(),
				bucketNames: []string{"test1", "test2"},
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBucketsOrDirectoryBuckets(gomock.Any()).Return(
					[]types.Bucket{},
					fmt.Errorf("ListBucketError"),
				)
			},
			want: want{
				bucketNames: []string{},
				err:         fmt.Errorf("ListBucketError"),
			},
			wantErr: true,
		},
		{
			name: "args.bucketNames is empty",
			args: args{
				ctx:         context.Background(),
				bucketNames: []string{},
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBucketsOrDirectoryBuckets(gomock.Any()).Return(
					[]types.Bucket{
						{
							Name: aws.String("test1"),
						},
						{
							Name: aws.String("test2"),
						},
					},
					nil,
				)
			},
			want: want{
				bucketNames: []string{},
				err:         nil,
			},
			wantErr: false,
		},
		{
			name: "bucket names are duplicated",
			args: args{
				ctx:         context.Background(),
				bucketNames: []string{"test1", "test1"},
			},
			prepareMockFn: func(m *client.MockIS3) {
				m.EXPECT().ListBucketsOrDirectoryBuckets(gomock.Any()).Return(
					[]types.Bucket{
						{
							Name: aws.String("test1"),
						},
					},
					nil,
				)
			},
			want: want{
				bucketNames: []string{"test1"},
				err:         nil,
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

			bucketNames, err := s3.CheckAllBucketsExist(tt.args.ctx, tt.args.bucketNames)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err.Error(), tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.err.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.err.Error())
				return
			}
			if !reflect.DeepEqual(bucketNames, tt.want.bucketNames) {
				t.Errorf("bucketNames = %#v, want %#v", bucketNames, tt.want.bucketNames)
			}
		})
	}
}

func TestS3Wrapper_OutputClearedMessage(t *testing.T) {
	io.NewLogger(false)

	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	io.Logger = &logger

	tests := []struct {
		name          string
		bucket        string
		count         int64
		wantErr       bool
		wantLogOutput string
	}{
		{
			name:          "normal clear result",
			bucket:        "test-bucket",
			count:         100,
			wantErr:       false,
			wantLogOutput: `{"level":"info","message":"test-bucket Cleared!!: 100 objects."}`,
		},
		{
			name:          "zero count clear result",
			bucket:        "test-bucket",
			count:         0,
			wantErr:       false,
			wantLogOutput: `{"level":"info","message":"test-bucket No objects."}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			s3 := NewS3Wrapper(nil)
			err := s3.OutputClearedMessage(tt.bucket, tt.count)
			if (err != nil) != tt.wantErr {
				t.Errorf("OutputClearedMessage() error = %v, wantErr %v", err, tt.wantErr)
			}
			got := buf.String()[:len(buf.String())-1] // remove trailing newline
			if got != tt.wantLogOutput {
				t.Errorf("OutputClearedMessage() log = %v, want %v", got, tt.wantLogOutput)
			}
		})
	}
}

func TestS3Wrapper_OutputDeletedMessage(t *testing.T) {
	io.NewLogger(false)

	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	io.Logger = &logger

	tests := []struct {
		name          string
		bucket        string
		wantErr       bool
		wantLogOutput string
	}{
		{
			name:          "normal delete result",
			bucket:        "test-bucket",
			wantErr:       false,
			wantLogOutput: `{"level":"info","message":"test-bucket Deleted!!"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			s3 := NewS3Wrapper(nil)
			err := s3.OutputDeletedMessage(tt.bucket)
			if (err != nil) != tt.wantErr {
				t.Errorf("OutputDeletedMessage() error = %v, wantErr %v", err, tt.wantErr)
			}
			got := buf.String()[:len(buf.String())-1] // remove trailing newline
			if got != tt.wantLogOutput {
				t.Errorf("OutputDeletedMessage() log = %v, want %v", got, tt.wantLogOutput)
			}
		})
	}
}

func TestS3Wrapper_OutputCheckingMessage(t *testing.T) {
	io.NewLogger(false)

	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	io.Logger = &logger

	tests := []struct {
		name          string
		bucket        string
		wantErr       bool
		wantLogOutput string
	}{
		{
			name:          "normal checking message",
			bucket:        "test-bucket",
			wantErr:       false,
			wantLogOutput: `{"level":"info","message":"test-bucket Checking..."}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			s3 := NewS3Wrapper(nil)
			err := s3.OutputCheckingMessage(tt.bucket)
			if (err != nil) != tt.wantErr {
				t.Errorf("OutputCheckingMessage() error = %v, wantErr %v", err, tt.wantErr)
			}
			got := buf.String()[:len(buf.String())-1] // remove trailing newline
			if got != tt.wantLogOutput {
				t.Errorf("OutputCheckingMessage() log = %v, want %v", got, tt.wantLogOutput)
			}
		})
	}
}

func TestS3Wrapper_GetLiveClearingMessage(t *testing.T) {
	io.NewLogger(false)

	tests := []struct {
		name       string
		bucket     string
		count      int64
		wantErr    bool
		wantOutput string
	}{
		{
			name:       "normal clearing message",
			bucket:     "test-bucket",
			count:      100,
			wantErr:    false,
			wantOutput: "test-bucket Clearing... 100 objects",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3 := NewS3Wrapper(nil)
			got, err := s3.GetLiveClearingMessage(tt.bucket, tt.count)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetLiveClearingMessage() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantOutput {
				t.Errorf("GetLiveClearingMessage() = %v, want %v", got, tt.wantOutput)
			}
		})
	}
}

func TestS3Wrapper_GetLiveClearedMessage(t *testing.T) {
	io.NewLogger(false)

	tests := []struct {
		name        string
		bucket      string
		count       int64
		isCompleted bool
		wantErr     bool
		wantOutput  string
	}{
		{
			name:        "normal cleared message",
			bucket:      "test-bucket",
			count:       100,
			isCompleted: true,
			wantErr:     false,
			wantOutput:  "\033[32mtest-bucket Cleared!!!  100 objects\033[0m",
		},
		{
			name:        "error occurred",
			bucket:      "test-bucket",
			count:       100,
			isCompleted: false,
			wantErr:     false,
			wantOutput:  "\033[31mtest-bucket Errors occurred!!! Cleared: 100 objects\033[0m",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3 := NewS3Wrapper(nil)
			got, err := s3.GetLiveClearedMessage(tt.bucket, tt.count, tt.isCompleted)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetLiveClearedMessage() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantOutput {
				t.Errorf("GetLiveClearedMessage() = %v, want %v", got, tt.wantOutput)
			}
		})
	}
}
