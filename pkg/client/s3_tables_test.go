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

type tokenForListNamespaces struct{}

func getTokenForListNamespacesInitialize(
	ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
) (
	out middleware.InitializeOutput, metadata middleware.Metadata, err error,
) {
	switch v := in.Parameters.(type) {
	case *s3tables.ListNamespacesInput:
		ctx = middleware.WithStackValue(ctx, tokenForListNamespaces{}, v.ContinuationToken)
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
				Err:          fmt.Errorf("operation error S3Tables: DeleteTableBucket, exceeded maximum number of attempts, 10, api error SlowDown"),
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

// ... 他のメソッドのテストも同様に実装 ...
