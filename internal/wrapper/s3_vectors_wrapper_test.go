package wrapper

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/types"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/pkg/client"
	"github.com/rs/zerolog"
	"go.uber.org/mock/gomock"
)

/*
	Test Cases
*/

func TestS3VectorsWrapper_ClearBucket(t *testing.T) {
	io.NewLogger(false)

	type args struct {
		ctx        context.Context
		bucketName string
		forceMode  bool
		quietMode  bool
		prefix     *string
	}

	cases := []struct {
		name          string
		args          args
		prepareMockFn func(m *client.MockIS3Vectors)
		want          error
		wantErr       bool
	}{
		{
			name: "clear indexes successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: "test-vector-bucket",
				forceMode:  false,
				quietMode:  false,
				prefix:     nil,
			},
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListIndexesByPage(
					gomock.Any(),
					aws.String("test-vector-bucket"),
					nil,
					(*string)(nil),
				).Return(
					&client.ListIndexesByPageOutput{
						Indexes: []types.IndexSummary{
							{
								IndexName: aws.String("index1"),
							},
							{
								IndexName: aws.String("index2"),
							},
						},
						NextToken: nil,
					},
					nil,
				)

				m.EXPECT().DeleteIndex(
					gomock.Any(),
					aws.String("index1"),
					aws.String("test-vector-bucket"),
				).Return(nil)

				m.EXPECT().DeleteIndex(
					gomock.Any(),
					aws.String("index2"),
					aws.String("test-vector-bucket"),
				).Return(nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "clear indexes with quiet mode successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: "test-vector-bucket",
				forceMode:  false,
				quietMode:  true,
				prefix:     nil,
			},
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListIndexesByPage(
					gomock.Any(),
					aws.String("test-vector-bucket"),
					nil,
					(*string)(nil),
				).Return(
					&client.ListIndexesByPageOutput{
						Indexes: []types.IndexSummary{
							{
								IndexName: aws.String("index1"),
							},
						},
						NextToken: nil,
					},
					nil,
				)

				m.EXPECT().DeleteIndex(
					gomock.Any(),
					aws.String("index1"),
					aws.String("test-vector-bucket"),
				).Return(nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "delete vector bucket successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: "test-vector-bucket",
				forceMode:  true,
				quietMode:  false,
				prefix:     nil,
			},
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListIndexesByPage(
					gomock.Any(),
					aws.String("test-vector-bucket"),
					nil,
					(*string)(nil),
				).Return(
					&client.ListIndexesByPageOutput{
						Indexes: []types.IndexSummary{
							{
								IndexName: aws.String("index1"),
							},
						},
						NextToken: nil,
					},
					nil,
				)

				m.EXPECT().DeleteIndex(
					gomock.Any(),
					aws.String("index1"),
					aws.String("test-vector-bucket"),
				).Return(nil)

				m.EXPECT().DeleteVectorBucket(
					gomock.Any(),
					aws.String("test-vector-bucket"),
				).Return(nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "list indexes failure",
			args: args{
				ctx:        context.Background(),
				bucketName: "test-vector-bucket",
				forceMode:  false,
				quietMode:  false,
				prefix:     nil,
			},
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListIndexesByPage(
					gomock.Any(),
					aws.String("test-vector-bucket"),
					nil,
					(*string)(nil),
				).Return(nil, fmt.Errorf("ListIndexesError"))
			},
			want:    fmt.Errorf("ListIndexesError"),
			wantErr: true,
		},
		{
			name: "delete index failure",
			args: args{
				ctx:        context.Background(),
				bucketName: "test-vector-bucket",
				forceMode:  false,
				quietMode:  false,
				prefix:     nil,
			},
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListIndexesByPage(
					gomock.Any(),
					aws.String("test-vector-bucket"),
					nil,
					(*string)(nil),
				).Return(
					&client.ListIndexesByPageOutput{
						Indexes: []types.IndexSummary{
							{
								IndexName: aws.String("index1"),
							},
						},
						NextToken: nil,
					},
					nil,
				)

				m.EXPECT().DeleteIndex(
					gomock.Any(),
					aws.String("index1"),
					aws.String("test-vector-bucket"),
				).Return(fmt.Errorf("DeleteIndexError"))
			},
			want:    fmt.Errorf("DeleteIndexError"),
			wantErr: true,
		},
		{
			name: "delete vector bucket failure",
			args: args{
				ctx:        context.Background(),
				bucketName: "test-vector-bucket",
				forceMode:  true,
				quietMode:  false,
				prefix:     nil,
			},
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListIndexesByPage(
					gomock.Any(),
					aws.String("test-vector-bucket"),
					nil,
					(*string)(nil),
				).Return(
					&client.ListIndexesByPageOutput{
						Indexes: []types.IndexSummary{
							{
								IndexName: aws.String("index1"),
							},
						},
						NextToken: nil,
					},
					nil,
				)

				m.EXPECT().DeleteIndex(
					gomock.Any(),
					aws.String("index1"),
					aws.String("test-vector-bucket"),
				).Return(nil)

				m.EXPECT().DeleteVectorBucket(
					gomock.Any(),
					aws.String("test-vector-bucket"),
				).Return(fmt.Errorf("DeleteVectorBucketError"))
			},
			want:    fmt.Errorf("DeleteVectorBucketError"),
			wantErr: true,
		},
		{
			name: "clear indexes with key prefix successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: "test-vector-bucket",
				forceMode:  false,
				quietMode:  false,
				prefix:     aws.String("test-"),
			},
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListIndexesByPage(
					gomock.Any(),
					aws.String("test-vector-bucket"),
					nil,
					aws.String("test-"),
				).Return(
					&client.ListIndexesByPageOutput{
						Indexes: []types.IndexSummary{
							{
								IndexName: aws.String("test-index1"),
							},
							{
								IndexName: aws.String("test-index2"),
							},
						},
						NextToken: nil,
					},
					nil,
				)

				m.EXPECT().DeleteIndex(
					gomock.Any(),
					aws.String("test-index1"),
					aws.String("test-vector-bucket"),
				).Return(nil)

				m.EXPECT().DeleteIndex(
					gomock.Any(),
					aws.String("test-index2"),
					aws.String("test-vector-bucket"),
				).Return(nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "clear indexes with pagination successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: "test-vector-bucket",
				forceMode:  false,
				quietMode:  false,
				prefix:     nil,
			},
			prepareMockFn: func(m *client.MockIS3Vectors) {
				// First page
				m.EXPECT().ListIndexesByPage(
					gomock.Any(),
					aws.String("test-vector-bucket"),
					nil,
					(*string)(nil),
				).Return(
					&client.ListIndexesByPageOutput{
						Indexes: []types.IndexSummary{
							{
								IndexName: aws.String("index1"),
							},
							{
								IndexName: aws.String("index2"),
							},
						},
						NextToken: aws.String("token1"),
					},
					nil,
				)

				// Second page
				m.EXPECT().ListIndexesByPage(
					gomock.Any(),
					aws.String("test-vector-bucket"),
					aws.String("token1"),
					(*string)(nil),
				).Return(
					&client.ListIndexesByPageOutput{
						Indexes: []types.IndexSummary{
							{
								IndexName: aws.String("index3"),
							},
						},
						NextToken: nil,
					},
					nil,
				)

				m.EXPECT().DeleteIndex(
					gomock.Any(),
					aws.String("index1"),
					aws.String("test-vector-bucket"),
				).Return(nil)

				m.EXPECT().DeleteIndex(
					gomock.Any(),
					aws.String("index2"),
					aws.String("test-vector-bucket"),
				).Return(nil)

				m.EXPECT().DeleteIndex(
					gomock.Any(),
					aws.String("index3"),
					aws.String("test-vector-bucket"),
				).Return(nil)
			},
			want:    nil,
			wantErr: false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			s3VectorsMock := client.NewMockIS3Vectors(ctrl)
			tt.prepareMockFn(s3VectorsMock)

			s3Vectors := NewS3VectorsWrapper(s3VectorsMock)

			clearingCountCh := make(chan int64)
			if !tt.args.quietMode {
				go func() {
					for range clearingCountCh {
					}
				}()
			}

			err := s3Vectors.ClearBucket(tt.args.ctx, ClearBucketInput{
				TargetBucket:    tt.args.bucketName,
				ForceMode:       tt.args.forceMode,
				QuietMode:       tt.args.quietMode,
				Prefix:          tt.args.prefix,
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

func TestS3VectorsWrapper_ListBucketNamesFilteredByKeyword(t *testing.T) {
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
		prepareMockFn func(m *client.MockIS3Vectors)
		want          want
		wantErr       bool
	}{
		{
			name: "list buckets filtered by keyword successfully",
			args: args{
				ctx:     context.Background(),
				keyword: aws.String("test"),
			},
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListVectorBuckets(gomock.Any()).Return(
					[]types.VectorBucketSummary{
						{
							VectorBucketName: aws.String("test1"),
						},
						{
							VectorBucketName: aws.String("test2"),
						},
						{
							VectorBucketName: aws.String("other"),
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
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListVectorBuckets(gomock.Any()).Return(
					[]types.VectorBucketSummary{
						{
							VectorBucketName: aws.String("test1"),
						},
						{
							VectorBucketName: aws.String("test2"),
						},
						{
							VectorBucketName: aws.String("other"),
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
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListVectorBuckets(gomock.Any()).Return(
					[]types.VectorBucketSummary{
						{
							VectorBucketName: aws.String("other1"),
						},
						{
							VectorBucketName: aws.String("other2"),
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
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListVectorBuckets(gomock.Any()).Return(
					[]types.VectorBucketSummary{},
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
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListVectorBuckets(gomock.Any()).Return(
					[]types.VectorBucketSummary{},
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
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListVectorBuckets(gomock.Any()).Return(
					[]types.VectorBucketSummary{},
					fmt.Errorf("ListVectorBucketsError"),
				)
			},
			want: want{
				output: []ListBucketNamesFilteredByKeywordOutput{},
				err:    fmt.Errorf("ListVectorBucketsError"),
			},
			wantErr: true,
		},
		{
			name: "list buckets filtered by keyword successfully for case-insensitive search",
			args: args{
				ctx:     context.Background(),
				keyword: aws.String("TEST"),
			},
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListVectorBuckets(gomock.Any()).Return(
					[]types.VectorBucketSummary{
						{
							VectorBucketName: aws.String("test1"),
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
				},
				err: nil,
			},
			wantErr: false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			s3VectorsMock := client.NewMockIS3Vectors(ctrl)
			tt.prepareMockFn(s3VectorsMock)

			s3Vectors := NewS3VectorsWrapper(s3VectorsMock)

			output, err := s3Vectors.ListBucketNamesFilteredByKeyword(tt.args.ctx, tt.args.keyword)
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

func TestS3VectorsWrapper_CheckAllBucketsExist(t *testing.T) {
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
		prepareMockFn func(m *client.MockIS3Vectors)
		want          want
		wantErr       bool
	}{
		{
			name: "all buckets exist",
			args: args{
				ctx:         context.Background(),
				bucketNames: []string{"test1", "test2"},
			},
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListVectorBuckets(gomock.Any()).Return(
					[]types.VectorBucketSummary{
						{
							VectorBucketName: aws.String("test1"),
						},
						{
							VectorBucketName: aws.String("test2"),
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
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListVectorBuckets(gomock.Any()).Return(
					[]types.VectorBucketSummary{
						{
							VectorBucketName: aws.String("test2"),
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
			name: "list vector buckets returns empty",
			args: args{
				ctx:         context.Background(),
				bucketNames: []string{"test1", "test2"},
			},
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListVectorBuckets(gomock.Any()).Return(
					[]types.VectorBucketSummary{},
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
			name: "list vector buckets failure",
			args: args{
				ctx:         context.Background(),
				bucketNames: []string{"test1", "test2"},
			},
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListVectorBuckets(gomock.Any()).Return(
					[]types.VectorBucketSummary{},
					fmt.Errorf("ListVectorBucketsError"),
				)
			},
			want: want{
				bucketNames: []string{},
				err:         fmt.Errorf("ListVectorBucketsError"),
			},
			wantErr: true,
		},
		{
			name: "args.bucketNames is empty",
			args: args{
				ctx:         context.Background(),
				bucketNames: []string{},
			},
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListVectorBuckets(gomock.Any()).Return(
					[]types.VectorBucketSummary{
						{
							VectorBucketName: aws.String("test1"),
						},
						{
							VectorBucketName: aws.String("test2"),
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
			prepareMockFn: func(m *client.MockIS3Vectors) {
				m.EXPECT().ListVectorBuckets(gomock.Any()).Return(
					[]types.VectorBucketSummary{
						{
							VectorBucketName: aws.String("test1"),
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
			s3VectorsMock := client.NewMockIS3Vectors(ctrl)
			tt.prepareMockFn(s3VectorsMock)

			s3Vectors := NewS3VectorsWrapper(s3VectorsMock)

			bucketNames, err := s3Vectors.CheckAllBucketsExist(tt.args.ctx, tt.args.bucketNames)
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

func TestS3VectorsWrapper_OutputClearedMessage(t *testing.T) {
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
			bucket:        "test-vector-bucket",
			count:         100,
			wantErr:       false,
			wantLogOutput: `{"level":"info","message":"test-vector-bucket Cleared!!: 100 indexes."}`,
		},
		{
			name:          "zero count clear result",
			bucket:        "test-vector-bucket",
			count:         0,
			wantErr:       false,
			wantLogOutput: `{"level":"info","message":"test-vector-bucket No indexes."}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			s3Vectors := NewS3VectorsWrapper(nil)
			err := s3Vectors.OutputClearedMessage(tt.bucket, tt.count)
			if (err != nil) != tt.wantErr {
				t.Errorf("OutputClearedMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := buf.String()[:len(buf.String())-1] // remove trailing newline
			if got != tt.wantLogOutput {
				t.Errorf("OutputClearedMessage() log = %v, want %v", got, tt.wantLogOutput)
			}
		})
	}
}

func TestS3VectorsWrapper_OutputDeletedMessage(t *testing.T) {
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
			bucket:        "test-vector-bucket",
			wantErr:       false,
			wantLogOutput: `{"level":"info","message":"test-vector-bucket Deleted!!"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			s3Vectors := NewS3VectorsWrapper(nil)
			err := s3Vectors.OutputDeletedMessage(tt.bucket)
			if (err != nil) != tt.wantErr {
				t.Errorf("OutputDeletedMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := buf.String()[:len(buf.String())-1] // remove trailing newline
			if got != tt.wantLogOutput {
				t.Errorf("OutputDeletedMessage() log = %v, want %v", got, tt.wantLogOutput)
			}
		})
	}
}

func TestS3VectorsWrapper_OutputCheckingMessage(t *testing.T) {
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
			bucket:        "test-vector-bucket",
			wantErr:       false,
			wantLogOutput: `{"level":"info","message":"test-vector-bucket Checking..."}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			s3Vectors := NewS3VectorsWrapper(nil)
			err := s3Vectors.OutputCheckingMessage(tt.bucket)
			if (err != nil) != tt.wantErr {
				t.Errorf("OutputCheckingMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := buf.String()[:len(buf.String())-1] // remove trailing newline
			if got != tt.wantLogOutput {
				t.Errorf("OutputCheckingMessage() log = %v, want %v", got, tt.wantLogOutput)
			}
		})
	}
}

func TestS3VectorsWrapper_GetLiveClearingMessage(t *testing.T) {
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
			bucket:     "test-vector-bucket",
			count:      100,
			wantErr:    false,
			wantOutput: "test-vector-bucket Clearing... 100 indexes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3Vectors := NewS3VectorsWrapper(nil)
			got, err := s3Vectors.GetLiveClearingMessage(tt.bucket, tt.count)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetLiveClearingMessage() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantOutput {
				t.Errorf("GetLiveClearingMessage() = %v, want %v", got, tt.wantOutput)
			}
		})
	}
}

func TestS3VectorsWrapper_GetLiveClearedMessage(t *testing.T) {
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
			bucket:      "test-vector-bucket",
			count:       100,
			isCompleted: true,
			wantErr:     false,
			wantOutput:  "\033[32mtest-vector-bucket Cleared!!!  100 indexes\033[0m",
		},
		{
			name:        "message when isCompleted is false",
			bucket:      "test-vector-bucket",
			count:       100,
			isCompleted: false,
			wantErr:     false,
			wantOutput:  "\033[31mtest-vector-bucket Errors occurred!!! Cleared: 100 indexes\033[0m",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3Vectors := NewS3VectorsWrapper(nil)
			got, err := s3Vectors.GetLiveClearedMessage(tt.bucket, tt.count, tt.isCompleted)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetLiveClearedMessage() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.wantOutput {
				t.Errorf("GetLiveClearedMessage() = %v, want %v", got, tt.wantOutput)
			}
		})
	}
}
