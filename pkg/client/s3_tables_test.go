package client

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"github.com/aws/smithy-go/middleware"
)

type tokenForListTableBuckets struct{}

func getTokenForListTableBucketsInitialize(
	ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
) (
	out middleware.InitializeOutput, metadata middleware.Metadata, err error,
) {
	//nolint:gocritic
	switch v := in.Parameters.(type) {
	case *s3tables.ListTableBucketsInput:
		ctx = middleware.WithStackValue(ctx, tokenForListTableBuckets{}, v.ContinuationToken)
	}
	return next.HandleInitialize(ctx, in)
}

/*
	Test Cases
*/

func TestS3Tables_DeleteTableBucket(t *testing.T) {
	type args struct {
		ctx                context.Context
		tableBucketARN     *string
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	cases := []struct {
		name    string
		args    args
		want    error
		wantErr bool
	}{
		{
			name: "delete table bucket successfully",
			args: args{
				ctx:            context.Background(),
				tableBucketARN: aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteTableBucketMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3tables.DeleteTableBucketOutput{},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "delete table bucket failure",
			args: args{
				ctx:            context.Background(),
				tableBucketARN: aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteTableBucketErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: nil,
								}, middleware.Metadata{}, fmt.Errorf("DeleteTableBucketError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: &ClientError{
				ResourceName: aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				Err:          fmt.Errorf("operation error S3Tables: DeleteTableBucket, DeleteTableBucketError"),
			},
			wantErr: true,
		},
		{
			name: "delete table bucket failure for api error SlowDown",
			args: args{
				ctx:            context.Background(),
				tableBucketARN: aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteTableBucketApiErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
										Result: nil,
									}, middleware.Metadata{}, &retry.MaxAttemptsError{
										Attempt: MaxRetryCount,
										Err:     fmt.Errorf("api error SlowDown"),
									}
							},
						),
						middleware.Before,
					)
				},
			},
			want: &ClientError{
				ResourceName: aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				Err:          fmt.Errorf("operation error S3Tables: DeleteTableBucket, exceeded maximum number of attempts, 20, api error SlowDown"),
			},
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("us-east-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			client := s3tables.NewFromConfig(cfg)
			s3TablesClient := NewS3Tables(client)

			err = s3TablesClient.DeleteTableBucket(tt.args.ctx, tt.args.tableBucketARN)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err.Error(), tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.Error())
			}
		})
	}
}

func TestS3Tables_DeleteNamespace(t *testing.T) {
	type args struct {
		ctx                context.Context
		namespace          *string
		tableBucketARN     *string
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	cases := []struct {
		name    string
		args    args
		want    error
		wantErr bool
	}{
		{
			name: "delete namespace successfully",
			args: args{
				ctx:            context.Background(),
				namespace:      aws.String("namespace1"),
				tableBucketARN: aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteNamespaceMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3tables.DeleteNamespaceOutput{},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "delete namespace failure",
			args: args{
				ctx:            context.Background(),
				namespace:      aws.String("namespace1"),
				tableBucketARN: aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteNamespaceErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: nil,
								}, middleware.Metadata{}, fmt.Errorf("DeleteNamespaceError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: &ClientError{
				ResourceName: aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test/namespace1"),
				Err:          fmt.Errorf("operation error S3Tables: DeleteNamespace, DeleteNamespaceError"),
			},
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("us-east-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			client := s3tables.NewFromConfig(cfg)
			s3TablesClient := NewS3Tables(client)

			err = s3TablesClient.DeleteNamespace(tt.args.ctx, tt.args.namespace, tt.args.tableBucketARN)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err.Error(), tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.Error())
			}
		})
	}
}

func TestS3Tables_DeleteTable(t *testing.T) {
	type args struct {
		ctx                context.Context
		tableName          *string
		namespace          *string
		tableBucketARN     *string
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	cases := []struct {
		name    string
		args    args
		want    error
		wantErr bool
	}{
		{
			name: "delete table successfully",
			args: args{
				ctx:            context.Background(),
				tableName:      aws.String("table1"),
				namespace:      aws.String("namespace1"),
				tableBucketARN: aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteTableMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3tables.DeleteTableOutput{},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "delete table failure",
			args: args{
				ctx:            context.Background(),
				tableName:      aws.String("table1"),
				namespace:      aws.String("namespace1"),
				tableBucketARN: aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteTableErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: nil,
								}, middleware.Metadata{}, fmt.Errorf("DeleteTableError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: &ClientError{
				ResourceName: aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test/namespace1/table1"),
				Err:          fmt.Errorf("operation error S3Tables: DeleteTable, DeleteTableError"),
			},
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("us-east-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			client := s3tables.NewFromConfig(cfg)
			s3TablesClient := NewS3Tables(client)

			err = s3TablesClient.DeleteTable(tt.args.ctx, tt.args.tableName, tt.args.namespace, tt.args.tableBucketARN)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err.Error(), tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.Error())
			}
		})
	}
}

func TestS3Tables_ListTableBuckets(t *testing.T) {
	type args struct {
		ctx                context.Context
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	type want struct {
		buckets []types.TableBucketSummary
		err     error
	}

	cases := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "list table buckets successfully",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListTableBucketsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3tables.ListTableBucketsOutput{
										TableBuckets: []types.TableBucketSummary{
											{
												Name: aws.String("test1"),
												Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test1"),
											},
											{
												Name: aws.String("test2"),
												Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test2"),
											},
										},
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				buckets: []types.TableBucketSummary{
					{
						Name: aws.String("test1"),
						Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test1"),
					},
					{
						Name: aws.String("test2"),
						Arn:  aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test2"),
					},
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list table buckets sorted successfully",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListTableBucketsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3tables.ListTableBucketsOutput{
										TableBuckets: []types.TableBucketSummary{
											{
												Name: aws.String("test2"),
											},
											{
												Name: aws.String("test3"),
											},
											{
												Name: aws.String("test"),
											},
											{
												Name: aws.String("test1"),
											},
										},
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				buckets: []types.TableBucketSummary{
					{
						Name: aws.String("test"),
					},
					{
						Name: aws.String("test1"),
					},
					{
						Name: aws.String("test2"),
					},
					{
						Name: aws.String("test3"),
					},
				},
			},
			wantErr: false,
		},
		{
			name: "list table buckets successfully but empty",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListTableBucketsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3tables.ListTableBucketsOutput{
										TableBuckets: []types.TableBucketSummary{},
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				buckets: []types.TableBucketSummary{},
				err:     nil,
			},
			wantErr: false,
		},
		{
			name: "list table buckets failure",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListTableBucketsErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: nil,
								}, middleware.Metadata{}, fmt.Errorf("ListTableBucketsError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				buckets: []types.TableBucketSummary{},
				err: &ClientError{
					Err: fmt.Errorf("operation error S3Tables: ListTableBuckets, ListTableBucketsError"),
				},
			},
			wantErr: true,
		},
		{
			name: "list table buckets with continuation token successfully",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					err := stack.Initialize.Add(
						middleware.InitializeMiddlewareFunc(
							"GetToken",
							getTokenForListTableBucketsInitialize,
						), middleware.Before,
					)
					if err != nil {
						return err
					}

					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListTableBucketsWithTokenMock",
							func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								continuationToken := middleware.GetStackValue(ctx, tokenForListTableBuckets{}).(*string)

								var token *string
								var buckets []types.TableBucketSummary
								if continuationToken == nil {
									token = aws.String("ContinuationToken")
									buckets = []types.TableBucketSummary{
										{
											Name: aws.String("test"),
										},
									}
									return middleware.FinalizeOutput{
										Result: &s3tables.ListTableBucketsOutput{
											TableBuckets:      buckets,
											ContinuationToken: token,
										},
									}, middleware.Metadata{}, nil
								} else {
									buckets = []types.TableBucketSummary{
										{
											Name: aws.String("test2"),
										},
									}
									return middleware.FinalizeOutput{
										Result: &s3tables.ListTableBucketsOutput{
											TableBuckets:      buckets,
											ContinuationToken: token,
										},
									}, middleware.Metadata{}, nil
								}
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				buckets: []types.TableBucketSummary{
					{
						Name: aws.String("test"),
					},
					{
						Name: aws.String("test2"),
					},
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list table buckets with continuation token failure",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					err := stack.Initialize.Add(
						middleware.InitializeMiddlewareFunc(
							"GetToken",
							getTokenForListTableBucketsInitialize,
						), middleware.Before,
					)
					if err != nil {
						return err
					}

					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListTableBucketsWithTokenMock",
							func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								continuationToken := middleware.GetStackValue(ctx, tokenForListTableBuckets{}).(*string)

								var token *string
								var buckets []types.TableBucketSummary
								if continuationToken == nil {
									token = aws.String("ContinuationToken")
									buckets = []types.TableBucketSummary{
										{
											Name: aws.String("test"),
										},
									}
									return middleware.FinalizeOutput{
										Result: &s3tables.ListTableBucketsOutput{
											TableBuckets:      buckets,
											ContinuationToken: token,
										},
									}, middleware.Metadata{}, nil
								} else {
									return middleware.FinalizeOutput{
										Result: &s3tables.ListTableBucketsOutput{},
									}, middleware.Metadata{}, fmt.Errorf("ListTableBucketsError")
								}
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				buckets: []types.TableBucketSummary{
					{
						Name: aws.String("test"),
					},
				},
				err: &ClientError{
					Err: fmt.Errorf("operation error S3Tables: ListTableBuckets, ListTableBucketsError"),
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("us-east-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			client := s3tables.NewFromConfig(cfg)
			s3TablesClient := NewS3Tables(client)

			buckets, err := s3TablesClient.ListTableBuckets(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err.Error(), tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.err.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.err.Error())
				return
			}
			if !reflect.DeepEqual(buckets, tt.want.buckets) {
				t.Errorf("buckets = %#v, want %#v", buckets, tt.want.buckets)
			}
		})
	}
}

func TestS3Tables_ListNamespacesByPage(t *testing.T) {
	type args struct {
		ctx                context.Context
		tableBucketARN     *string
		continuationToken  *string
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	type want struct {
		output *ListNamespacesByPageOutput
		err    error
	}

	cases := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "list namespaces successfully",
			args: args{
				ctx:               context.Background(),
				tableBucketARN:    aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				continuationToken: nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListNamespacesMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3tables.ListNamespacesOutput{
										Namespaces: []types.NamespaceSummary{
											{
												Namespace: []string{"namespace1", "namespace2"},
											},
										},
										ContinuationToken: aws.String("token1"),
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: &ListNamespacesByPageOutput{
					Namespaces: []types.NamespaceSummary{
						{
							Namespace: []string{"namespace1", "namespace2"},
						},
					},
					ContinuationToken: aws.String("token1"),
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list namespaces failure",
			args: args{
				ctx:               context.Background(),
				tableBucketARN:    aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				continuationToken: nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListNamespacesErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: nil,
								}, middleware.Metadata{}, fmt.Errorf("ListNamespacesError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: nil,
				err: &ClientError{
					ResourceName: aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
					Err:          fmt.Errorf("operation error S3Tables: ListNamespaces, ListNamespacesError"),
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("us-east-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			client := s3tables.NewFromConfig(cfg)
			s3TablesClient := NewS3Tables(client)

			output, err := s3TablesClient.ListNamespacesByPage(tt.args.ctx, tt.args.tableBucketARN, tt.args.continuationToken)
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

func TestS3Tables_ListTablesByPage(t *testing.T) {
	type args struct {
		ctx                context.Context
		tableBucketARN     *string
		namespace          *string
		continuationToken  *string
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	type want struct {
		output *ListTablesByPageOutput
		err    error
	}

	cases := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "list tables successfully",
			args: args{
				ctx:               context.Background(),
				tableBucketARN:    aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				namespace:         aws.String("namespace1"),
				continuationToken: nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListTablesMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3tables.ListTablesOutput{
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
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: &ListTablesByPageOutput{
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
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list tables failure",
			args: args{
				ctx:               context.Background(),
				tableBucketARN:    aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test"),
				namespace:         aws.String("namespace1"),
				continuationToken: nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListTablesErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: nil,
								}, middleware.Metadata{}, fmt.Errorf("ListTablesError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: nil,
				err: &ClientError{
					ResourceName: aws.String("arn:aws:s3:us-east-1:123456789012:table-bucket/test/namespace1"),
					Err:          fmt.Errorf("operation error S3Tables: ListTables, ListTablesError"),
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("us-east-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			client := s3tables.NewFromConfig(cfg)
			s3TablesClient := NewS3Tables(client)

			output, err := s3TablesClient.ListTablesByPage(tt.args.ctx, tt.args.tableBucketARN, tt.args.namespace, tt.args.continuationToken)
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
