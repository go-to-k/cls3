package wrapper

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/pkg/client"
	"github.com/rs/zerolog"
	"go.uber.org/mock/gomock"
)

/*
	Test Cases
*/

func TestS3TablesWrapper_ClearBucket(t *testing.T) {
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
		prepareMockFn func(m *client.MockIS3Tables)
		want          error
		wantErr       bool
	}{
		{
			name: "clear tables successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
				forceMode:  false,
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListNamespacesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					nil,
				).Return(
					&client.ListNamespacesByPageOutput{
						Namespaces: []types.NamespaceSummary{
							{
								Namespace: []string{"namespace1", "namespace2"},
							},
						},
						ContinuationToken: nil,
					},
					nil,
				)

				// For namespace1
				m.EXPECT().ListTablesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					aws.String("namespace1"),
					nil,
				).Return(
					&client.ListTablesByPageOutput{
						Tables: []types.TableSummary{
							{
								Name: aws.String("table1"),
							},
							{
								Name: aws.String("table2"),
							},
						},
						ContinuationToken: nil,
					},
					nil,
				)

				m.EXPECT().DeleteTable(
					gomock.Any(),
					aws.String("table1"),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)

				m.EXPECT().DeleteTable(
					gomock.Any(),
					aws.String("table2"),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)

				m.EXPECT().DeleteNamespace(
					gomock.Any(),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)

				// For namespace2
				m.EXPECT().ListTablesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					aws.String("namespace2"),
					nil,
				).Return(
					&client.ListTablesByPageOutput{
						Tables: []types.TableSummary{
							{
								Name: aws.String("table3"),
							},
						},
						ContinuationToken: nil,
					},
					nil,
				)

				m.EXPECT().DeleteTable(
					gomock.Any(),
					aws.String("table3"),
					aws.String("namespace2"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)

				m.EXPECT().DeleteNamespace(
					gomock.Any(),
					aws.String("namespace2"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "clear tables with quiet mode successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
				forceMode:  false,
				quietMode:  true,
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListNamespacesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					nil,
				).Return(
					&client.ListNamespacesByPageOutput{
						Namespaces: []types.NamespaceSummary{
							{
								Namespace: []string{"namespace1"},
							},
						},
						ContinuationToken: nil,
					},
					nil,
				)

				m.EXPECT().ListTablesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					aws.String("namespace1"),
					nil,
				).Return(
					&client.ListTablesByPageOutput{
						Tables: []types.TableSummary{
							{
								Name: aws.String("table1"),
							},
						},
						ContinuationToken: nil,
					},
					nil,
				)

				m.EXPECT().DeleteTable(
					gomock.Any(),
					aws.String("table1"),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)

				m.EXPECT().DeleteNamespace(
					gomock.Any(),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "delete bucket successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
				forceMode:  true,
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListNamespacesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					nil,
				).Return(
					&client.ListNamespacesByPageOutput{
						Namespaces: []types.NamespaceSummary{
							{
								Namespace: []string{"namespace1"},
							},
						},
						ContinuationToken: nil,
					},
					nil,
				)

				m.EXPECT().ListTablesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					aws.String("namespace1"),
					nil,
				).Return(
					&client.ListTablesByPageOutput{
						Tables: []types.TableSummary{
							{
								Name: aws.String("table1"),
							},
						},
						ContinuationToken: nil,
					},
					nil,
				)

				m.EXPECT().DeleteTable(
					gomock.Any(),
					aws.String("table1"),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)

				m.EXPECT().DeleteNamespace(
					gomock.Any(),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)

				m.EXPECT().DeleteTableBucket(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "invalid bucket ARN format",
			args: args{
				ctx:        context.Background(),
				bucketName: "invalid-arn",
				forceMode:  false,
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3Tables) {},
			want: &client.ClientError{
				Err: fmt.Errorf("InvalidBucketArnError: invalid bucket ARN format without a slash, got invalid-arn"),
			},
			wantErr: true,
		},
		{
			name: "list namespaces failure",
			args: args{
				ctx:        context.Background(),
				bucketName: "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
				forceMode:  false,
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListNamespacesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					nil,
				).Return(nil, fmt.Errorf("ListNamespacesError"))
			},
			want:    fmt.Errorf("ListNamespacesError"),
			wantErr: true,
		},
		{
			name: "list tables failure in deleteNamespace",
			args: args{
				ctx:        context.Background(),
				bucketName: "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
				forceMode:  false,
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListNamespacesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					nil,
				).Return(
					&client.ListNamespacesByPageOutput{
						Namespaces: []types.NamespaceSummary{
							{
								Namespace: []string{"namespace1"},
							},
						},
						ContinuationToken: nil,
					},
					nil,
				)

				m.EXPECT().ListTablesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					aws.String("namespace1"),
					nil,
				).Return(nil, fmt.Errorf("ListTablesError"))
			},
			want:    fmt.Errorf("ListTablesError"),
			wantErr: true,
		},
		{
			name: "delete table bucket failure",
			args: args{
				ctx:        context.Background(),
				bucketName: "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
				forceMode:  true,
				quietMode:  false,
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListNamespacesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					nil,
				).Return(
					&client.ListNamespacesByPageOutput{
						Namespaces: []types.NamespaceSummary{
							{
								Namespace: []string{"namespace1"},
							},
						},
						ContinuationToken: nil,
					},
					nil,
				)

				m.EXPECT().ListTablesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					aws.String("namespace1"),
					nil,
				).Return(
					&client.ListTablesByPageOutput{
						Tables: []types.TableSummary{
							{
								Name: aws.String("table1"),
							},
						},
						ContinuationToken: nil,
					},
					nil,
				)

				m.EXPECT().DeleteTable(
					gomock.Any(),
					aws.String("table1"),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)

				m.EXPECT().DeleteNamespace(
					gomock.Any(),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)

				m.EXPECT().DeleteTableBucket(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(fmt.Errorf("DeleteTableBucketError"))
			},
			want:    fmt.Errorf("DeleteTableBucketError"),
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			s3TablesMock := client.NewMockIS3Tables(ctrl)
			tt.prepareMockFn(s3TablesMock)

			s3Tables := NewS3TablesWrapper(s3TablesMock)

			clearingCountCh := make(chan int64)
			if !tt.args.quietMode {
				go func() {
					for range clearingCountCh {
					}
				}()
			}

			err := s3Tables.ClearBucket(tt.args.ctx, ClearBucketInput{
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

func TestS3TablesWrapper_deleteNamespace(t *testing.T) {
	io.NewLogger(false)

	type args struct {
		ctx        context.Context
		bucketArn  string
		bucketName string
		namespace  string
	}

	type want struct {
		deletedCount int
		err          error
	}

	cases := []struct {
		name          string
		args          args
		prepareMockFn func(m *client.MockIS3Tables)
		want          want
		wantErr       bool
	}{
		{
			name: "delete namespace and tables successfully",
			args: args{
				ctx:        context.Background(),
				bucketArn:  "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
				bucketName: "test",
				namespace:  "namespace1",
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				// First page of tables
				m.EXPECT().ListTablesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					aws.String("namespace1"),
					nil,
				).Return(
					&client.ListTablesByPageOutput{
						Tables: []types.TableSummary{
							{
								Name: aws.String("table1"),
							},
							{
								Name: aws.String("table2"),
							},
						},
						ContinuationToken: aws.String("token1"),
					},
					nil,
				)

				// Second page of tables
				m.EXPECT().ListTablesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					aws.String("namespace1"),
					aws.String("token1"),
				).Return(
					&client.ListTablesByPageOutput{
						Tables: []types.TableSummary{
							{
								Name: aws.String("table3"),
							},
						},
						ContinuationToken: nil,
					},
					nil,
				)

				// Delete tables
				m.EXPECT().DeleteTable(
					gomock.Any(),
					aws.String("table1"),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)

				m.EXPECT().DeleteTable(
					gomock.Any(),
					aws.String("table2"),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)

				m.EXPECT().DeleteTable(
					gomock.Any(),
					aws.String("table3"),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)

				// Delete namespace
				m.EXPECT().DeleteNamespace(
					gomock.Any(),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)
			},
			want: want{
				deletedCount: 3,
				err:          nil,
			},
			wantErr: false,
		},
		{
			name: "delete empty namespace successfully",
			args: args{
				ctx:        context.Background(),
				bucketArn:  "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
				bucketName: "test",
				namespace:  "namespace1",
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTablesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					aws.String("namespace1"),
					nil,
				).Return(
					&client.ListTablesByPageOutput{
						Tables:            []types.TableSummary{},
						ContinuationToken: nil,
					},
					nil,
				)

				m.EXPECT().DeleteNamespace(
					gomock.Any(),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(nil)
			},
			want: want{
				deletedCount: 0,
				err:          nil,
			},
			wantErr: false,
		},
		{
			name: "list tables failure",
			args: args{
				ctx:        context.Background(),
				bucketArn:  "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
				bucketName: "test",
				namespace:  "namespace1",
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTablesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					aws.String("namespace1"),
					nil,
				).Return(nil, fmt.Errorf("ListTablesError"))
			},
			want: want{
				deletedCount: 0,
				err:          fmt.Errorf("ListTablesError"),
			},
			wantErr: true,
		},
		{
			name: "delete table failure",
			args: args{
				ctx:        context.Background(),
				bucketArn:  "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
				bucketName: "test",
				namespace:  "namespace1",
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTablesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					aws.String("namespace1"),
					nil,
				).Return(
					&client.ListTablesByPageOutput{
						Tables: []types.TableSummary{
							{
								Name: aws.String("table1"),
							},
						},
						ContinuationToken: nil,
					},
					nil,
				)

				m.EXPECT().DeleteTable(
					gomock.Any(),
					aws.String("table1"),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(fmt.Errorf("DeleteTableError"))
			},
			want: want{
				deletedCount: 0,
				err:          fmt.Errorf("DeleteTableError"),
			},
			wantErr: true,
		},
		{
			name: "delete namespace failure",
			args: args{
				ctx:        context.Background(),
				bucketArn:  "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
				bucketName: "test",
				namespace:  "namespace1",
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTablesByPage(
					gomock.Any(),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					aws.String("namespace1"),
					nil,
				).Return(
					&client.ListTablesByPageOutput{
						Tables:            []types.TableSummary{},
						ContinuationToken: nil,
					},
					nil,
				)

				m.EXPECT().DeleteNamespace(
					gomock.Any(),
					aws.String("namespace1"),
					aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				).Return(fmt.Errorf("DeleteNamespaceError"))
			},
			want: want{
				deletedCount: 0,
				err:          fmt.Errorf("DeleteNamespaceError"),
			},
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			s3TablesMock := client.NewMockIS3Tables(ctrl)
			tt.prepareMockFn(s3TablesMock)

			s3Tables := NewS3TablesWrapper(s3TablesMock)

			progressCh := make(chan struct{})
			var deletedCount atomic.Int64
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range progressCh {
					deletedCount.Add(1)
				}
			}()

			err := s3Tables.deleteNamespace(tt.args.ctx, tt.args.bucketArn, tt.args.bucketName, tt.args.namespace, progressCh)
			close(progressCh)
			wg.Wait()

			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err.Error(), tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.err.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.err.Error())
				return
			}
			if deletedCount.Load() != int64(tt.want.deletedCount) {
				t.Errorf("deletedCount = %d, want %d", deletedCount.Load(), tt.want.deletedCount)
			}
		})
	}
}

func TestS3TablesWrapper_ListBucketNamesFilteredByKeyword(t *testing.T) {
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
		prepareMockFn func(m *client.MockIS3Tables)
		want          want
		wantErr       bool
	}{
		{
			name: "list buckets filtered by keyword successfully",
			args: args{
				ctx:     context.Background(),
				keyword: aws.String("test"),
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTableBuckets(gomock.Any()).Return(
					[]types.TableBucketSummary{
						{
							Name: aws.String("test1"),
							Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test1"),
						},
						{
							Name: aws.String("test2"),
							Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test2"),
						},
						{
							Name: aws.String("other"),
							Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/other"),
						},
					},
					nil,
				)
			},
			want: want{
				output: []ListBucketNamesFilteredByKeywordOutput{
					{
						BucketName:   "test1",
						TargetBucket: "arn:aws:s3:us-east-1:123456789012:table-bucket/test1",
					},
					{
						BucketName:   "test2",
						TargetBucket: "arn:aws:s3:us-east-1:123456789012:table-bucket/test2",
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
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTableBuckets(gomock.Any()).Return(
					[]types.TableBucketSummary{
						{
							Name: aws.String("test1"),
							Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test1"),
						},
						{
							Name: aws.String("test2"),
							Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test2"),
						},
						{
							Name: aws.String("other"),
							Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/other"),
						},
					},
					nil,
				)
			},
			want: want{
				output: []ListBucketNamesFilteredByKeywordOutput{
					{
						BucketName:   "test1",
						TargetBucket: "arn:aws:s3:us-east-1:123456789012:table-bucket/test1",
					},
					{
						BucketName:   "test2",
						TargetBucket: "arn:aws:s3:us-east-1:123456789012:table-bucket/test2",
					},
					{
						BucketName:   "other",
						TargetBucket: "arn:aws:s3:us-east-1:123456789012:table-bucket/other",
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
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTableBuckets(gomock.Any()).Return(
					[]types.TableBucketSummary{
						{
							Name: aws.String("other1"),
							Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/other1"),
						},
						{
							Name: aws.String("other2"),
							Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/other2"),
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
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTableBuckets(gomock.Any()).Return(
					[]types.TableBucketSummary{},
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
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTableBuckets(gomock.Any()).Return(
					[]types.TableBucketSummary{},
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
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTableBuckets(gomock.Any()).Return(
					[]types.TableBucketSummary{},
					fmt.Errorf("ListTableBucketsError"),
				)
			},
			want: want{
				output: []ListBucketNamesFilteredByKeywordOutput{},
				err:    fmt.Errorf("ListTableBucketsError"),
			},
			wantErr: true,
		},
		{
			name: "list buckets filtered by keyword successfully for case-insensitive search",
			args: args{
				ctx:     context.Background(),
				keyword: aws.String("TEST"),
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTableBuckets(gomock.Any()).Return(
					[]types.TableBucketSummary{
						{
							Name: aws.String("test1"),
							Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test1"),
						},
					},
					nil,
				)
			},
			want: want{
				output: []ListBucketNamesFilteredByKeywordOutput{
					{
						BucketName:   "test1",
						TargetBucket: "arn:aws:s3:us-east-1:123456789012:table-bucket/test1",
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
			s3TablesMock := client.NewMockIS3Tables(ctrl)
			tt.prepareMockFn(s3TablesMock)

			s3Tables := NewS3TablesWrapper(s3TablesMock)

			output, err := s3Tables.ListBucketNamesFilteredByKeyword(tt.args.ctx, tt.args.keyword)
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

func TestS3TablesWrapper_CheckAllBucketsExist(t *testing.T) {
	io.NewLogger(false)

	type args struct {
		ctx         context.Context
		bucketNames []string
	}

	type want struct {
		bucketArns []string
		err        error
	}

	cases := []struct {
		name          string
		args          args
		prepareMockFn func(m *client.MockIS3Tables)
		want          want
		wantErr       bool
	}{
		{
			name: "all buckets exist",
			args: args{
				ctx:         context.Background(),
				bucketNames: []string{"test1", "test2"},
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTableBuckets(gomock.Any()).Return(
					[]types.TableBucketSummary{
						{
							Name: aws.String("test1"),
							Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test1"),
						},
						{
							Name: aws.String("test2"),
							Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test2"),
						},
					},
					nil,
				)
			},
			want: want{
				bucketArns: []string{
					"arn:aws:s3:us-east-1:123456789012:table-bucket/test1",
					"arn:aws:s3:us-east-1:123456789012:table-bucket/test2",
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "part of bucket does not exist",
			args: args{
				ctx:         context.Background(),
				bucketNames: []string{"test1", "test2"},
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTableBuckets(gomock.Any()).Return(
					[]types.TableBucketSummary{
						{
							Name: aws.String("test2"),
							Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test2"),
						},
					},
					nil,
				)
			},
			want: want{
				bucketArns: []string{"arn:aws:s3:us-east-1:123456789012:table-bucket/test2"},
				err:        fmt.Errorf("[resource -] NotExistsError: The following buckets do not exist: test1"),
			},
			wantErr: true,
		},
		{
			name: "list table buckets returns empty",
			args: args{
				ctx:         context.Background(),
				bucketNames: []string{"test1", "test2"},
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTableBuckets(gomock.Any()).Return(
					[]types.TableBucketSummary{},
					nil,
				)
			},
			want: want{
				bucketArns: []string{},
				err:        fmt.Errorf("[resource -] NotExistsError: The following buckets do not exist: test1, test2"),
			},
			wantErr: true,
		},
		{
			name: "list table buckets failure",
			args: args{
				ctx:         context.Background(),
				bucketNames: []string{"test1", "test2"},
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTableBuckets(gomock.Any()).Return(
					[]types.TableBucketSummary{},
					fmt.Errorf("ListTableBucketsError"),
				)
			},
			want: want{
				bucketArns: []string{},
				err:        fmt.Errorf("ListTableBucketsError"),
			},
			wantErr: true,
		},
		{
			name: "args.bucketNames is empty",
			args: args{
				ctx:         context.Background(),
				bucketNames: []string{},
			},
			prepareMockFn: func(m *client.MockIS3Tables) {
				m.EXPECT().ListTableBuckets(gomock.Any()).Return(
					[]types.TableBucketSummary{
						{
							Name: aws.String("test1"),
							Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test1"),
						},
						{
							Name: aws.String("test2"),
							Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test2"),
						},
					},
					nil,
				)
			},
			want: want{
				bucketArns: []string{},
				err:        nil,
			},
			wantErr: false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			s3TablesMock := client.NewMockIS3Tables(ctrl)
			tt.prepareMockFn(s3TablesMock)

			s3Tables := NewS3TablesWrapper(s3TablesMock)

			bucketArns, err := s3Tables.CheckAllBucketsExist(tt.args.ctx, tt.args.bucketNames)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err.Error(), tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.err.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.err.Error())
				return
			}
			if !reflect.DeepEqual(bucketArns, tt.want.bucketArns) {
				t.Errorf("bucketArns = %#v, want %#v", bucketArns, tt.want.bucketArns)
			}
		})
	}
}

func TestS3TablesWrapper_outputBucketName(t *testing.T) {
	io.NewLogger(false)

	tests := []struct {
		name       string
		bucket     string
		want       string
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:    "normal ARN",
			bucket:  "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
			want:    "test",
			wantErr: false,
		},
		{
			name:       "invalid ARN format",
			bucket:     "invalid-arn",
			want:       "",
			wantErr:    true,
			wantErrMsg: "[resource -] InvalidBucketArnError: invalid bucket ARN format without a slash, got invalid-arn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3Tables := NewS3TablesWrapper(nil)
			got, err := s3Tables.outputBucketName(tt.bucket)
			if (err != nil) != tt.wantErr {
				t.Errorf("OutputBucketName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.wantErrMsg {
				t.Errorf("OutputBucketName() error = %v, wantErrMsg %v", err, tt.wantErrMsg)
				return
			}
			if got != tt.want {
				t.Errorf("OutputBucketName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3TablesWrapper_OutputClearedMessage(t *testing.T) {
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
			bucket:        "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
			count:         100,
			wantErr:       false,
			wantLogOutput: `{"level":"info","message":"test Cleared!!: 100 tables."}`,
		},
		{
			name:          "zero count clear result",
			bucket:        "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
			count:         0,
			wantErr:       false,
			wantLogOutput: `{"level":"info","message":"test No tables."}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			s3Tables := NewS3TablesWrapper(nil)
			err := s3Tables.OutputClearedMessage(tt.bucket, tt.count)
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

func TestS3TablesWrapper_OutputDeletedMessage(t *testing.T) {
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
			bucket:        "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
			wantErr:       false,
			wantLogOutput: `{"level":"info","message":"test Deleted!!"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			s3Tables := NewS3TablesWrapper(nil)
			err := s3Tables.OutputDeletedMessage(tt.bucket)
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

func TestS3TablesWrapper_OutputCheckingMessage(t *testing.T) {
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
			bucket:        "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
			wantErr:       false,
			wantLogOutput: `{"level":"info","message":"test Checking..."}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			s3Tables := NewS3TablesWrapper(nil)
			err := s3Tables.OutputCheckingMessage(tt.bucket)
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

func TestS3TablesWrapper_GetLiveClearingMessage(t *testing.T) {
	io.NewLogger(false)

	tests := []struct {
		name       string
		bucket     string
		count      int64
		wantErr    bool
		wantOutput string
		wantErrMsg string
	}{
		{
			name:       "normal clearing message",
			bucket:     "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
			count:      100,
			wantErr:    false,
			wantOutput: "test Clearing... 100 tables",
			wantErrMsg: "",
		},
		{
			name:       "error occurred for invalid bucket name",
			bucket:     "invalid-bucket",
			count:      100,
			wantErr:    true,
			wantOutput: "",
			wantErrMsg: "[resource -] InvalidBucketArnError: invalid bucket ARN format without a slash, got invalid-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3Tables := NewS3TablesWrapper(nil)
			got, err := s3Tables.GetLiveClearingMessage(tt.bucket, tt.count)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetLiveClearingMessage() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err.Error() != tt.wantErrMsg {
				t.Errorf("GetLiveClearingMessage() error = %v, wantErrMsg %v", err, tt.wantErrMsg)
			}
			if got != tt.wantOutput {
				t.Errorf("GetLiveClearingMessage() = %v, want %v", got, tt.wantOutput)
			}
		})
	}
}

func TestS3TablesWrapper_GetLiveClearedMessage(t *testing.T) {
	io.NewLogger(false)

	tests := []struct {
		name        string
		bucket      string
		count       int64
		isCompleted bool
		wantErr     bool
		wantOutput  string
		wantErrMsg  string
	}{
		{
			name:        "normal cleared message",
			bucket:      "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
			count:       100,
			isCompleted: true,
			wantErr:     false,
			wantOutput:  "\033[32mtest Cleared!!!  100 tables\033[0m",
			wantErrMsg:  "",
		},
		{
			name:        "message when isCompleted is false",
			bucket:      "arn:aws:s3:us-east-1:123456789012:table-bucket/test",
			count:       100,
			isCompleted: false,
			wantErr:     false,
			wantOutput:  "\033[31mtest Errors occurred!!! Cleared: 100 tables\033[0m",
			wantErrMsg:  "",
		},
		{
			name:        "error occurred for invalid bucket name",
			bucket:      "invalid-bucket",
			count:       100,
			isCompleted: false,
			wantErr:     true,
			wantOutput:  "",
			wantErrMsg:  "[resource -] InvalidBucketArnError: invalid bucket ARN format without a slash, got invalid-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3Tables := NewS3TablesWrapper(nil)
			got, err := s3Tables.GetLiveClearedMessage(tt.bucket, tt.count, tt.isCompleted)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetLiveClearedMessage() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err.Error() != tt.wantErrMsg {
				t.Errorf("GetLiveClearedMessage() error = %v, wantErrMsg %v", err, tt.wantErrMsg)
			}
			if got != tt.wantOutput {
				t.Errorf("GetLiveClearedMessage() = %v, want %v", got, tt.wantOutput)
			}
		})
	}
}
