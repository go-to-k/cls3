package client

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
)

type tokenForListBuckets struct{}

func getTokenForListBucketsInitialize(
	ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
) (
	out middleware.InitializeOutput, metadata middleware.Metadata, err error,
) {
	//nolint:gocritic
	switch v := in.Parameters.(type) {
	case *s3.ListBucketsInput:
		ctx = middleware.WithStackValue(ctx, tokenForListBuckets{}, v.ContinuationToken)
	}
	return next.HandleInitialize(ctx, in)
}

type tokenForListDirectoryBuckets struct{}

func getTokenForListDirectoryBucketsInitialize(
	ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
) (
	out middleware.InitializeOutput, metadata middleware.Metadata, err error,
) {
	//nolint:gocritic
	switch v := in.Parameters.(type) {
	case *s3.ListDirectoryBucketsInput:
		ctx = middleware.WithStackValue(ctx, tokenForListDirectoryBuckets{}, v.ContinuationToken)
	}
	return next.HandleInitialize(ctx, in)
}

type targetObjectsForDeleteObjects struct{}

func setTargetObjectsForDeleteObjectsInitialize(
	ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
) (
	out middleware.InitializeOutput, metadata middleware.Metadata, err error,
) {
	//nolint:gocritic
	switch v := in.Parameters.(type) {
	case *s3.DeleteObjectsInput:
		ctx = middleware.WithStackValue(ctx, targetObjectsForDeleteObjects{}, v.Delete.Objects)
	}
	return next.HandleInitialize(ctx, in)
}

/*
	Test Cases
*/

func TestS3_DeleteBucket(t *testing.T) {
	type args struct {
		ctx                context.Context
		bucketName         *string
		region             string
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	cases := []struct {
		name    string
		args    args
		want    error
		wantErr bool
	}{
		{
			name: "delete bucket successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteBucketMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.DeleteBucketOutput{},
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
			name: "delete bucket failure",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteBucketErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: nil,
								}, middleware.Metadata{}, fmt.Errorf("DeleteBucketError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: &ClientError{
				ResourceName: aws.String("test"),
				Err:          fmt.Errorf("operation error S3: DeleteBucket, DeleteBucketError"),
			},
			wantErr: true,
		},
		{
			name: "delete bucket failure for api error SlowDown",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteBucketApiErrorMock",
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
				ResourceName: aws.String("test"),
				Err:          fmt.Errorf("operation error S3: DeleteBucket, exceeded maximum number of attempts, 20, api error SlowDown"),
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

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client, false)

			err = s3Client.DeleteBucket(tt.args.ctx, tt.args.bucketName, tt.args.region)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.Error() {
				t.Errorf("err = %#v, want %#v", err, tt.want)
			}
		})
	}
}

func TestS3_DeleteObjects(t *testing.T) {
	type args struct {
		ctx                context.Context
		bucketName         *string
		region             string
		objects            []types.ObjectIdentifier
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	type want struct {
		output []types.Error
		err    error
	}

	cases := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "delete objects successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				objects: []types.ObjectIdentifier{
					{
						Key:       aws.String("Key"),
						VersionId: aws.String("VersionId"),
					},
				},
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteObjectsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.DeleteObjectsOutput{
										Errors: []types.Error{},
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: []types.Error{},
				err:    nil,
			},
			wantErr: false,
		},
		{
			name: "delete objects successfully if zero objects",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				objects:    []types.ObjectIdentifier{},
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteObjectsIfZeroObjectsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.DeleteObjectsOutput{
										Errors: []types.Error{},
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: []types.Error{},
				err:    nil,
			},
			wantErr: false,
		},
		{
			name: "delete objects failure",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				objects: []types.ObjectIdentifier{
					{
						Key:       aws.String("Key"),
						VersionId: aws.String("VersionId"),
					},
				},
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteObjectsErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.DeleteObjectsOutput{
										Errors: []types.Error{},
									},
								}, middleware.Metadata{}, fmt.Errorf("DeleteObjectsError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: []types.Error{},
				err: &ClientError{
					ResourceName: aws.String("test"),
					Err:          fmt.Errorf("operation error S3: DeleteObjects, DeleteObjectsError"),
				},
			},
			wantErr: true,
		},
		{
			name: "delete objects failure for api error SlowDown",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				objects: []types.ObjectIdentifier{
					{
						Key:       aws.String("Key"),
						VersionId: aws.String("VersionId"),
					},
				},
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteObjectsApiErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
										Result: &s3.DeleteObjectsOutput{
											Errors: []types.Error{},
										},
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
			want: want{
				output: []types.Error{},
				err: &ClientError{
					ResourceName: aws.String("test"),
					Err:          fmt.Errorf("operation error S3: DeleteObjects, exceeded maximum number of attempts, 20, api error SlowDown"),
				},
			},
			wantErr: true,
		},
		{
			name: "delete objects failure for output errors",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				objects: []types.ObjectIdentifier{
					{
						Key:       aws.String("Key"),
						VersionId: aws.String("VersionId"),
					},
				},
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteObjectsOutputErrorsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.DeleteObjectsOutput{
										Errors: []types.Error{
											{
												Key:       aws.String("Key"),
												Code:      aws.String("Code"),
												Message:   aws.String("Message"),
												VersionId: aws.String("VersionId"),
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
				output: []types.Error{
					{
						Key:       aws.String("Key"),
						Code:      aws.String("Code"),
						Message:   aws.String("Message"),
						VersionId: aws.String("VersionId"),
					},
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "does not return errors when retry succeeds",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				objects: []types.ObjectIdentifier{
					{
						Key:       aws.String("Key1"),
						VersionId: aws.String("VersionId1"),
					},
					{
						Key:       aws.String("Key2"),
						VersionId: aws.String("VersionId2"),
					},
					{
						Key:       aws.String("Key3"),
						VersionId: aws.String("VersionId3"),
					},
				},
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					err := stack.Initialize.Add(
						middleware.InitializeMiddlewareFunc(
							"SetTargetObjects",
							setTargetObjectsForDeleteObjectsInitialize,
						), middleware.Before,
					)
					if err != nil {
						return err
					}

					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteObjectsWithRetryableErrorsMock",
							func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								objects := middleware.GetStackValue(ctx, targetObjectsForDeleteObjects{}).([]types.ObjectIdentifier)
								var errors []types.Error
								// first loop
								if len(objects) == 3 {
									errors = []types.Error{
										{
											Key:       aws.String("Key1"),
											Code:      aws.String("InternalError"),
											Message:   aws.String("We encountered an internal error. Please try again."),
											VersionId: aws.String("VersionId1"),
										},
										{
											Key:       aws.String("Key2"),
											Code:      aws.String("InternalError"),
											Message:   aws.String("We encountered an internal error. Please try again."),
											VersionId: aws.String("VersionId2"),
										},
										// 3rd object is not an error
									}
								} else {
									errors = []types.Error{}
								}
								return middleware.FinalizeOutput{
									Result: &s3.DeleteObjectsOutput{
										Errors: errors,
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: []types.Error{},
				err:    nil,
			},
			wantErr: false,
		},
		{
			name: "return retryable output errors when it exceeds max attempts",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				objects: []types.ObjectIdentifier{
					{
						Key:       aws.String("Key1"),
						VersionId: aws.String("VersionId1"),
					},
					{
						Key:       aws.String("Key2"),
						VersionId: aws.String("VersionId2"),
					},
					{
						Key:       aws.String("Key3"),
						VersionId: aws.String("VersionId3"),
					},
				},
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteObjectsWithRetryableErrorsMock",
							func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.DeleteObjectsOutput{
										Errors: []types.Error{
											{
												Key:       aws.String("Key1"),
												Code:      aws.String("InternalError"),
												Message:   aws.String("We encountered an internal error. Please try again."),
												VersionId: aws.String("VersionId1"),
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
				output: []types.Error{
					{
						Key:       aws.String("Key1"),
						Code:      aws.String("InternalError"),
						Message:   aws.String("We encountered an internal error. Please try again."),
						VersionId: aws.String("VersionId1"),
					},
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "return non-retryable output errors even when retrying",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				objects: []types.ObjectIdentifier{
					{
						Key:       aws.String("Key1"),
						VersionId: aws.String("VersionId1"),
					},
					{
						Key:       aws.String("Key2"),
						VersionId: aws.String("VersionId2"),
					},
					{
						Key:       aws.String("Key3"),
						VersionId: aws.String("VersionId3"),
					},
				},
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					err := stack.Initialize.Add(
						middleware.InitializeMiddlewareFunc(
							"SetTargetObjects",
							setTargetObjectsForDeleteObjectsInitialize,
						), middleware.Before,
					)
					if err != nil {
						return err
					}

					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteObjectsWithRetryableErrorsMock",
							func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								objects := middleware.GetStackValue(ctx, targetObjectsForDeleteObjects{}).([]types.ObjectIdentifier)
								var errors []types.Error
								// first loop
								if len(objects) == 3 {
									errors = []types.Error{
										{
											Key:       aws.String("Key1"),
											Code:      aws.String("InternalError"),
											Message:   aws.String("We encountered an internal error. Please try again."),
											VersionId: aws.String("VersionId1"),
										},
										{
											Key:       aws.String("Key2"),
											Code:      aws.String("InternalError"),
											Message:   aws.String("Other Error"),
											VersionId: aws.String("VersionId2"),
										},
										// 3rd object is not an error
									}
								} else {
									errors = []types.Error{}
								}
								return middleware.FinalizeOutput{
									Result: &s3.DeleteObjectsOutput{
										Errors: errors,
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: []types.Error{
					{
						Key:       aws.String("Key2"),
						Code:      aws.String("InternalError"),
						Message:   aws.String("Other Error"),
						VersionId: aws.String("VersionId2"),
					},
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "return errors if a retryable error becomes a non-retryable error after retrying",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				objects: []types.ObjectIdentifier{
					{
						Key:       aws.String("Key1"),
						VersionId: aws.String("VersionId1"),
					},
					{
						Key:       aws.String("Key2"),
						VersionId: aws.String("VersionId2"),
					},
					{
						Key:       aws.String("Key3"),
						VersionId: aws.String("VersionId3"),
					},
				},
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					err := stack.Initialize.Add(
						middleware.InitializeMiddlewareFunc(
							"SetTargetObjects",
							setTargetObjectsForDeleteObjectsInitialize,
						), middleware.Before,
					)
					if err != nil {
						return err
					}

					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"DeleteObjectsWithRetryableErrorsMock",
							func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								objects := middleware.GetStackValue(ctx, targetObjectsForDeleteObjects{}).([]types.ObjectIdentifier)
								var errors []types.Error
								// first loop
								if len(objects) == 3 {
									errors = []types.Error{
										{
											Key:       aws.String("Key1"),
											Code:      aws.String("InternalError"),
											Message:   aws.String("We encountered an internal error. Please try again."),
											VersionId: aws.String("VersionId1"),
										},
										// 2nd and 3rd objects are not errors
									}
								} else {
									errors = []types.Error{
										{
											Key:       aws.String("Key1"),
											Code:      aws.String("InternalError"),
											Message:   aws.String("Other Error"),
											VersionId: aws.String("VersionId1"),
										},
									}
								}
								return middleware.FinalizeOutput{
									Result: &s3.DeleteObjectsOutput{
										Errors: errors,
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: []types.Error{
					{
						Key:       aws.String("Key1"),
						Code:      aws.String("InternalError"),
						Message:   aws.String("Other Error"),
						VersionId: aws.String("VersionId1"),
					},
				},
				err: nil,
			},
			wantErr: false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			SleepTimeSecForS3 = 0
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("us-east-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client, false)

			output, err := s3Client.DeleteObjects(tt.args.ctx, tt.args.bucketName, tt.args.objects, tt.args.region)
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

func TestS3_ListObjectsOrVersionsByPage(t *testing.T) {
	type args struct {
		ctx                  context.Context
		bucketName           *string
		region               string
		oldVersionsOnly      bool
		keyMarker            *string
		versionIdMarker      *string
		directoryBucketsMode bool
		keyPrefix            *string
		withAPIOptionsFunc   func(*middleware.Stack) error
	}

	type want struct {
		output *ListObjectsOrVersionsByPageOutput
		err    error
	}

	cases := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "call listObjectsByPage if directoryBucketsMode is true",
			args: args{
				ctx:                  context.Background(),
				bucketName:           aws.String("test"),
				region:               "us-east-1",
				oldVersionsOnly:      false,
				keyMarker:            nil,
				versionIdMarker:      nil,
				directoryBucketsMode: true,
				keyPrefix:            nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectsV2Mock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectsV2Output{
										Contents: []types.Object{
											{
												Key: aws.String("Key1"),
											},
											{
												Key: aws.String("Key2"),
											},
										},
										NextContinuationToken: aws.String("NextContinuationToken"),
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: &ListObjectsOrVersionsByPageOutput{
					ObjectIdentifiers: []types.ObjectIdentifier{
						{
							Key: aws.String("Key1"),
						},
						{
							Key: aws.String("Key2"),
						},
					},
					NextKeyMarker: aws.String("NextContinuationToken"),
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "call listObjectsByPage if directoryBucketsMode is false",
			args: args{
				ctx:                  context.Background(),
				bucketName:           aws.String("test"),
				region:               "us-east-1",
				oldVersionsOnly:      false,
				keyMarker:            nil,
				versionIdMarker:      nil,
				directoryBucketsMode: false,
				keyPrefix:            nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectVersionsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectVersionsOutput{
										Versions: []types.ObjectVersion{
											{
												Key:       aws.String("KeyForVersions"),
												VersionId: aws.String("VersionIdForVersions"),
											},
										},
										DeleteMarkers: []types.DeleteMarkerEntry{
											{
												Key:       aws.String("KeyForDeleteMarkers"),
												VersionId: aws.String("VersionIdForDeleteMarkers"),
											},
										},
										NextKeyMarker:       aws.String("NextKeyMarker"),
										NextVersionIdMarker: aws.String("NextVersionIdMarker"),
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: &ListObjectsOrVersionsByPageOutput{
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
					NextKeyMarker:       aws.String("NextKeyMarker"),
					NextVersionIdMarker: aws.String("NextVersionIdMarker"),
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "listObjectsByPage errors",
			args: args{
				ctx:                  context.Background(),
				bucketName:           aws.String("test"),
				region:               "us-east-1",
				oldVersionsOnly:      false,
				keyMarker:            nil,
				versionIdMarker:      nil,
				directoryBucketsMode: true,
				keyPrefix:            nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectsV2ErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectsV2Output{},
								}, middleware.Metadata{}, fmt.Errorf("ListObjectsV2Error")
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: nil,
				err:    fmt.Errorf("[resource test] operation error S3: ListObjectsV2, ListObjectsV2Error"),
			},
			wantErr: true,
		},
		{
			name: "listObjectVersionsByPage errors",
			args: args{
				ctx:                  context.Background(),
				bucketName:           aws.String("test"),
				region:               "us-east-1",
				oldVersionsOnly:      false,
				keyMarker:            nil,
				versionIdMarker:      nil,
				directoryBucketsMode: false,
				keyPrefix:            nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectVersionsErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectVersionsOutput{},
								}, middleware.Metadata{}, fmt.Errorf("ListObjectVersionsError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: nil,
				err:    fmt.Errorf("[resource test] operation error S3: ListObjectVersions, ListObjectVersionsError"),
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

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client, tt.args.directoryBucketsMode)

			output, err := s3Client.ListObjectsOrVersionsByPage(tt.args.ctx, tt.args.bucketName, tt.args.region, tt.args.oldVersionsOnly, tt.args.keyMarker, tt.args.versionIdMarker, tt.args.keyPrefix)
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
			if tt.want.output != nil && !reflect.DeepEqual(output.NextKeyMarker, tt.want.output.NextKeyMarker) {
				t.Errorf("nextKeyMarker = %#v, want %#v", output.NextKeyMarker, tt.want.output.NextKeyMarker)
			}
			if tt.want.output != nil && !reflect.DeepEqual(output.NextVersionIdMarker, tt.want.output.NextVersionIdMarker) {
				t.Errorf("nextVersionIdMarker = %#v, want %#v", output.NextVersionIdMarker, tt.want.output.NextVersionIdMarker)
			}
		})
	}
}

func TestS3_listObjectVersionsByPage(t *testing.T) {
	type args struct {
		ctx                context.Context
		bucketName         *string
		region             string
		oldVersionsOnly    bool
		keyMarker          *string
		versionIdMarker    *string
		keyPrefix          *string
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	type want struct {
		output *listObjectVersionsByPageOutput
		err    error
	}

	cases := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "list objects versions successfully",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "us-east-1",
				oldVersionsOnly: false,
				keyMarker:       nil,
				versionIdMarker: nil,
				keyPrefix:       nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectVersionsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectVersionsOutput{
										Versions: []types.ObjectVersion{
											{
												Key:       aws.String("KeyForVersions"),
												VersionId: aws.String("VersionIdForVersions"),
											},
										},
										DeleteMarkers: []types.DeleteMarkerEntry{
											{
												Key:       aws.String("KeyForDeleteMarkers"),
												VersionId: aws.String("VersionIdForDeleteMarkers"),
											},
										},
										NextKeyMarker:       aws.String("NextKeyMarker"),
										NextVersionIdMarker: aws.String("NextVersionIdMarker"),
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: &listObjectVersionsByPageOutput{
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
					NextKeyMarker:       aws.String("NextKeyMarker"),
					NextVersionIdMarker: aws.String("NextVersionIdMarker"),
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list objects versions failure",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "us-east-1",
				oldVersionsOnly: false,
				keyMarker:       nil,
				versionIdMarker: nil,
				keyPrefix:       nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectVersionsErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectVersionsOutput{},
								}, middleware.Metadata{}, fmt.Errorf("ListObjectVersionsError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: nil,
				err: &ClientError{
					ResourceName: aws.String("test"),
					Err:          fmt.Errorf("operation error S3: ListObjectVersions, ListObjectVersionsError"),
				},
			},
			wantErr: true,
		},
		{
			name: "list objects versions failure for api error SlowDown",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "us-east-1",
				oldVersionsOnly: false,
				keyMarker:       nil,
				versionIdMarker: nil,
				keyPrefix:       nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectVersionsApiErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
										Result: &s3.ListObjectVersionsOutput{},
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
			want: want{
				output: nil,
				err: &ClientError{
					ResourceName: aws.String("test"),
					Err:          fmt.Errorf("operation error S3: ListObjectVersions, exceeded maximum number of attempts, 20, api error SlowDown"),
				},
			},
			wantErr: true,
		},
		{
			name: "list objects versions successfully(empty)",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "us-east-1",
				oldVersionsOnly: false,
				keyMarker:       nil,
				versionIdMarker: nil,
				keyPrefix:       nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectVersionsEmptyMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectVersionsOutput{
										Versions:      []types.ObjectVersion{},
										DeleteMarkers: []types.DeleteMarkerEntry{},
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: &listObjectVersionsByPageOutput{
					ObjectIdentifiers:   []types.ObjectIdentifier{},
					NextKeyMarker:       nil,
					NextVersionIdMarker: nil,
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list objects versions successfully(versions only)",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "us-east-1",
				oldVersionsOnly: false,
				keyMarker:       nil,
				versionIdMarker: nil,
				keyPrefix:       nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectVersionsWithVersionsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectVersionsOutput{
										Versions: []types.ObjectVersion{
											{
												Key:       aws.String("KeyForVersions"),
												VersionId: aws.String("VersionIdForVersions"),
											},
										},
										DeleteMarkers:       []types.DeleteMarkerEntry{},
										NextKeyMarker:       aws.String("NextKeyMarker"),
										NextVersionIdMarker: aws.String("NextVersionIdMarker"),
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: &listObjectVersionsByPageOutput{
					ObjectIdentifiers: []types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions"),
							VersionId: aws.String("VersionIdForVersions"),
						},
					},
					NextKeyMarker:       aws.String("NextKeyMarker"),
					NextVersionIdMarker: aws.String("NextVersionIdMarker"),
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list objects versions successfully(delete markers only)",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "us-east-1",
				oldVersionsOnly: false,
				keyMarker:       nil,
				versionIdMarker: nil,
				keyPrefix:       nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectVersionsWithDeleteMarkersMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectVersionsOutput{
										Versions: []types.ObjectVersion{},
										DeleteMarkers: []types.DeleteMarkerEntry{
											{
												Key:       aws.String("KeyForDeleteMarkers"),
												VersionId: aws.String("VersionIdForDeleteMarkers"),
											},
										},
										NextKeyMarker:       aws.String("NextKeyMarker"),
										NextVersionIdMarker: aws.String("NextVersionIdMarker"),
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: &listObjectVersionsByPageOutput{
					ObjectIdentifiers: []types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForDeleteMarkers"),
							VersionId: aws.String("VersionIdForDeleteMarkers"),
						},
					},
					NextKeyMarker:       aws.String("NextKeyMarker"),
					NextVersionIdMarker: aws.String("NextVersionIdMarker"),
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list objects versions with markers successfully",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "us-east-1",
				oldVersionsOnly: false,
				keyMarker:       aws.String("NextKeyMarker"),
				versionIdMarker: aws.String("NextVersionIdMarker"),
				keyPrefix:       nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectVersionsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectVersionsOutput{
										Versions: []types.ObjectVersion{
											{
												Key:       aws.String("KeyForVersions"),
												VersionId: aws.String("VersionIdForVersions"),
											},
										},
										DeleteMarkers: []types.DeleteMarkerEntry{
											{
												Key:       aws.String("KeyForDeleteMarkers"),
												VersionId: aws.String("VersionIdForDeleteMarkers"),
											},
										},
										NextKeyMarker:       nil,
										NextVersionIdMarker: nil,
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: &listObjectVersionsByPageOutput{
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
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list objects versions with markers failure",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "us-east-1",
				oldVersionsOnly: false,
				keyMarker:       aws.String("NextKeyMarker"),
				versionIdMarker: aws.String("NextVersionIdMarker"),
				keyPrefix:       nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectVersionsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectVersionsOutput{},
								}, middleware.Metadata{}, fmt.Errorf("ListObjectVersionsError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: nil,
				err: &ClientError{
					ResourceName: aws.String("test"),
					Err:          fmt.Errorf("operation error S3: ListObjectVersions, ListObjectVersionsError"),
				},
			},
			wantErr: true,
		},
		{
			name: "list objects versions with old versions if oldVersionsOnly is true successfully",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "us-east-1",
				oldVersionsOnly: true,
				keyMarker:       nil,
				versionIdMarker: nil,
				keyPrefix:       nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectVersionsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectVersionsOutput{
										Versions: []types.ObjectVersion{
											{
												Key:       aws.String("KeyForVersions1"),
												VersionId: aws.String("VersionIdForVersions1"),
												IsLatest:  aws.Bool(false),
											},
											{
												Key:       aws.String("KeyForVersions2"),
												VersionId: aws.String("VersionIdForVersions2"),
												IsLatest:  aws.Bool(true),
											},
											{
												Key:       aws.String("KeyForVersions3"),
												VersionId: aws.String("VersionIdForVersions3"),
											},
										},
										DeleteMarkers: []types.DeleteMarkerEntry{
											{
												Key:       aws.String("KeyForDeleteMarkers1"),
												VersionId: aws.String("VersionIdForDeleteMarkers1"),
												IsLatest:  aws.Bool(false),
											},
											{
												Key:       aws.String("KeyForDeleteMarkers2"),
												VersionId: aws.String("VersionIdForDeleteMarkers2"),
												IsLatest:  aws.Bool(true),
											},
											{
												Key:       aws.String("KeyForDeleteMarkers3"),
												VersionId: aws.String("VersionIdForDeleteMarkers3"),
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
				output: &listObjectVersionsByPageOutput{
					ObjectIdentifiers: []types.ObjectIdentifier{
						{
							Key:       aws.String("KeyForVersions1"),
							VersionId: aws.String("VersionIdForVersions1"),
						},
						{
							Key:       aws.String("KeyForDeleteMarkers1"),
							VersionId: aws.String("VersionIdForDeleteMarkers1"),
						},
						{
							Key:       aws.String("KeyForDeleteMarkers2"),
							VersionId: aws.String("VersionIdForDeleteMarkers2"),
						},
						{
							Key:       aws.String("KeyForDeleteMarkers3"),
							VersionId: aws.String("VersionIdForDeleteMarkers3"),
						},
					},
					NextKeyMarker:       nil,
					NextVersionIdMarker: nil,
				},
				err: nil,
			},
			wantErr: false,
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

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client, false)

			output, err := s3Client.listObjectVersionsByPage(tt.args.ctx, tt.args.bucketName, tt.args.region, tt.args.oldVersionsOnly, tt.args.keyMarker, tt.args.versionIdMarker, tt.args.keyPrefix)
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
			if tt.want.output != nil && !reflect.DeepEqual(output.NextKeyMarker, tt.want.output.NextKeyMarker) {
				t.Errorf("nextKeyMarker = %#v, want %#v", output.NextKeyMarker, tt.want.output.NextKeyMarker)
			}
			if tt.want.output != nil && !reflect.DeepEqual(output.NextVersionIdMarker, tt.want.output.NextVersionIdMarker) {
				t.Errorf("nextVersionIdMarker = %#v, want %#v", output.NextVersionIdMarker, tt.want.output.NextVersionIdMarker)
			}
		})
	}
}

func TestS3_listObjectsByPage(t *testing.T) {
	type args struct {
		ctx                context.Context
		bucketName         *string
		region             string
		token              *string
		keyPrefix          *string
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	type want struct {
		output *listObjectsByPageOutput
		err    error
	}

	cases := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "list objects successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				token:      nil,
				keyPrefix:  nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectsV2Mock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectsV2Output{
										Contents: []types.Object{
											{
												Key: aws.String("Key1"),
											},
											{
												Key: aws.String("Key2"),
											},
										},
										NextContinuationToken: aws.String("NextContinuationToken"),
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: &listObjectsByPageOutput{
					ObjectIdentifiers: []types.ObjectIdentifier{
						{
							Key: aws.String("Key1"),
						},
						{
							Key: aws.String("Key2"),
						},
					},
					NextToken: aws.String("NextContinuationToken"),
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list objects failure",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				token:      nil,
				keyPrefix:  nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectsV2ErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectsV2Output{},
								}, middleware.Metadata{}, fmt.Errorf("ListObjectsV2Error")
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: nil,
				err: &ClientError{
					ResourceName: aws.String("test"),
					Err:          fmt.Errorf("operation error S3: ListObjectsV2, ListObjectsV2Error"),
				},
			},
			wantErr: true,
		},
		{
			name: "list objects failure for api error SlowDown",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				token:      nil,
				keyPrefix:  nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectsV2ApiErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
										Result: &s3.ListObjectsV2Output{},
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
			want: want{
				output: nil,
				err: &ClientError{
					ResourceName: aws.String("test"),
					Err:          fmt.Errorf("operation error S3: ListObjectsV2, exceeded maximum number of attempts, 20, api error SlowDown"),
				},
			},
			wantErr: true,
		},
		{
			name: "list objects successfully(empty)",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				token:      nil,
				keyPrefix:  nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectsV2EmptyMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectsV2Output{
										Contents: []types.Object{},
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: nil,
				err:    nil,
			},
			wantErr: false,
		},
		{
			name: "list objects with token successfully",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				token:      aws.String("Token"),
				keyPrefix:  nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectsV2Mock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectsV2Output{
										Contents: []types.Object{
											{
												Key: aws.String("Key1"),
											},
											{
												Key: aws.String("Key2"),
											},
										},
										NextContinuationToken: nil,
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: &listObjectsByPageOutput{
					ObjectIdentifiers: []types.ObjectIdentifier{
						{
							Key: aws.String("Key1"),
						},
						{
							Key: aws.String("Key2"),
						},
					},
					NextToken: nil,
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list objects with token failure",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "us-east-1",
				token:      aws.String("Token"),
				keyPrefix:  nil,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectsV2Mock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListObjectsV2Output{},
								}, middleware.Metadata{}, fmt.Errorf("ListObjectsV2Error")
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: nil,
				err: &ClientError{
					ResourceName: aws.String("test"),
					Err:          fmt.Errorf("operation error S3: ListObjectsV2, ListObjectsV2Error"),
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

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client, true)

			output, err := s3Client.listObjectsByPage(tt.args.ctx, tt.args.bucketName, tt.args.region, tt.args.token, tt.args.keyPrefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err.Error(), tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.err.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.err.Error())
				return
			}
			if tt.want.output != nil && !reflect.DeepEqual(output, tt.want.output) {
				t.Errorf("output = %#v, want %#v", output, tt.want.output)
			}
			if tt.want.output != nil && !reflect.DeepEqual(output.NextToken, tt.want.output.NextToken) {
				t.Errorf("nextToken = %#v, want %#v", output.NextToken, tt.want.output.NextToken)
			}
		})
	}
}

func TestS3_ListBucketsOrDirectoryBuckets(t *testing.T) {
	type args struct {
		ctx                  context.Context
		directoryBucketsMode bool
		withAPIOptionsFunc   func(*middleware.Stack) error
	}

	type want struct {
		output []types.Bucket
		err    error
	}

	cases := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "call listDirectoryBuckets if directoryBucketsMode is true",
			args: args{
				ctx:                  context.Background(),
				directoryBucketsMode: true,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListDirectoryBucketsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListDirectoryBucketsOutput{
										Buckets: []types.Bucket{
											{
												Name: aws.String("test"),
											},
											{
												Name: aws.String("test2"),
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
				output: []types.Bucket{
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
			name: "call listBuckets if directoryBucketsMode is false",
			args: args{
				ctx:                  context.Background(),
				directoryBucketsMode: false,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListBucketsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListBucketsOutput{
										Buckets: []types.Bucket{
											{
												Name: aws.String("test"),
											},
											{
												Name: aws.String("test2"),
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
				output: []types.Bucket{
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
			name: "listDirectoryBuckets errors",
			args: args{
				ctx:                  context.Background(),
				directoryBucketsMode: true,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListDirectoryBucketsErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: nil,
								}, middleware.Metadata{}, fmt.Errorf("ListDirectoryBucketsError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: []types.Bucket{},
				err:    fmt.Errorf("[resource -] operation error S3: ListDirectoryBuckets, ListDirectoryBucketsError"),
			},
			wantErr: true,
		},
		{
			name: "listBuckets errors",
			args: args{
				ctx:                  context.Background(),
				directoryBucketsMode: false,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListBucketsErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: nil,
								}, middleware.Metadata{}, fmt.Errorf("ListBucketsError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: []types.Bucket{},
				err:    fmt.Errorf("[resource -] operation error S3: ListBuckets, ListBucketsError"),
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

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client, tt.args.directoryBucketsMode)

			output, err := s3Client.ListBucketsOrDirectoryBuckets(tt.args.ctx)
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

func TestS3_listBuckets(t *testing.T) {
	type args struct {
		ctx                context.Context
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	type want struct {
		buckets []types.Bucket
		err     error
	}

	cases := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "list buckets successfully",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListBucketsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListBucketsOutput{
										Buckets: []types.Bucket{
											{
												Name: aws.String("test"),
											},
											{
												Name: aws.String("test2"),
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
				buckets: []types.Bucket{
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
			name: "list buckets successfully but empty",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListBucketsNotExistMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListBucketsOutput{
										Buckets: []types.Bucket{},
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				buckets: []types.Bucket{},
				err:     nil,
			},
			wantErr: false,
		},
		{
			name: "list buckets failure",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListBucketsErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: nil,
								}, middleware.Metadata{}, fmt.Errorf("ListBucketsError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				buckets: []types.Bucket{},
				err: &ClientError{
					Err: fmt.Errorf("operation error S3: ListBuckets, ListBucketsError"),
				},
			},
			wantErr: true,
		},
		{
			name: "list buckets failure for api error SlowDown",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListBucketsApiErrorMock",
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
			want: want{
				buckets: []types.Bucket{},
				err: &ClientError{
					Err: fmt.Errorf("operation error S3: ListBuckets, exceeded maximum number of attempts, 20, api error SlowDown"),
				},
			},
			wantErr: true,
		},
		{
			name: "list buckets with token successfully",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					err := stack.Initialize.Add(
						middleware.InitializeMiddlewareFunc(
							"GetToken",
							getTokenForListBucketsInitialize,
						), middleware.Before,
					)
					if err != nil {
						return err
					}

					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListBucketsWithTokenMock",
							func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								continuationToken := middleware.GetStackValue(ctx, tokenForListBuckets{}).(*string)

								var nextToken *string
								var buckets []types.Bucket
								if continuationToken == nil {
									nextToken = aws.String("NextToken")
									buckets = []types.Bucket{
										{
											Name: aws.String("test1"),
										},
									}
									return middleware.FinalizeOutput{
										Result: &s3.ListBucketsOutput{
											Buckets:           buckets,
											ContinuationToken: nextToken,
										},
									}, middleware.Metadata{}, nil
								} else {
									buckets = []types.Bucket{
										{
											Name: aws.String("test2"),
										},
									}
									return middleware.FinalizeOutput{
										Result: &s3.ListBucketsOutput{
											Buckets: buckets,
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
				buckets: []types.Bucket{
					{
						Name: aws.String("test1"),
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
			name: "list buckets with token failure",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					err := stack.Initialize.Add(
						middleware.InitializeMiddlewareFunc(
							"GetToken",
							getTokenForListBucketsInitialize,
						), middleware.Before,
					)
					if err != nil {
						return err
					}

					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListBucketsWithTokenErrorMock",
							func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								continuationToken := middleware.GetStackValue(ctx, tokenForListBuckets{}).(*string)

								var nextToken *string
								var buckets []types.Bucket
								if continuationToken == nil {
									nextToken = aws.String("NextToken")
									buckets = []types.Bucket{
										{
											Name: aws.String("test1"),
										},
									}
									return middleware.FinalizeOutput{
										Result: &s3.ListBucketsOutput{
											Buckets:           buckets,
											ContinuationToken: nextToken,
										},
									}, middleware.Metadata{}, nil
								} else {
									return middleware.FinalizeOutput{
										Result: &s3.ListBucketsOutput{},
									}, middleware.Metadata{}, fmt.Errorf("ListBucketsError")
								}
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				buckets: []types.Bucket{
					{
						Name: aws.String("test1"),
					},
				},
				err: &ClientError{
					Err: fmt.Errorf("operation error S3: ListBuckets, ListBucketsError"),
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

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client, false)

			output, err := s3Client.listBuckets(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err.Error(), tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.err.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.err.Error())
				return
			}
			if !reflect.DeepEqual(output, tt.want.buckets) {
				t.Errorf("output = %#v, want %#v", output, tt.want.buckets)
			}
		})
	}
}

func TestS3_listDirectoryBuckets(t *testing.T) {
	type args struct {
		ctx                context.Context
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	type want struct {
		buckets []types.Bucket
		err     error
	}

	cases := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "list directory buckets successfully",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListDirectoryBucketsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListDirectoryBucketsOutput{
										Buckets: []types.Bucket{
											{
												Name: aws.String("test"),
											},
											{
												Name: aws.String("test2"),
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
				buckets: []types.Bucket{
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
			name: "list directory buckets sorted successfully",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListDirectoryBucketsMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListDirectoryBucketsOutput{
										Buckets: []types.Bucket{
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
				buckets: []types.Bucket{
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
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list directory buckets successfully but empty",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListDirectoryBucketsNotExistMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListDirectoryBucketsOutput{
										Buckets: []types.Bucket{},
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				buckets: []types.Bucket{},
				err:     nil,
			},
			wantErr: false,
		},
		{
			name: "list directory buckets failure",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListDirectoryBucketsErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: nil,
								}, middleware.Metadata{}, fmt.Errorf("ListDirectoryBucketsError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				buckets: []types.Bucket{},
				err: &ClientError{
					Err: fmt.Errorf("operation error S3: ListDirectoryBuckets, ListDirectoryBucketsError"),
				},
			},
			wantErr: true,
		},
		{
			name: "list directory buckets failure for api error SlowDown",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListDirectoryBucketsApiErrorMock",
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
			want: want{
				buckets: []types.Bucket{},
				err: &ClientError{
					Err: fmt.Errorf("operation error S3: ListDirectoryBuckets, exceeded maximum number of attempts, 20, api error SlowDown"),
				},
			},
			wantErr: true,
		},
		{
			name: "list directory buckets with token successfully",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					err := stack.Initialize.Add(
						middleware.InitializeMiddlewareFunc(
							"GetToken",
							getTokenForListDirectoryBucketsInitialize,
						), middleware.Before,
					)
					if err != nil {
						return err
					}

					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListDirectoryBucketsWithTokenMock",
							func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								continuationToken := middleware.GetStackValue(ctx, tokenForListDirectoryBuckets{}).(*string)

								var nextToken *string
								var buckets []types.Bucket
								if continuationToken == nil {
									nextToken = aws.String("NextToken")
									buckets = []types.Bucket{
										{
											Name: aws.String("test"),
										},
									}
									return middleware.FinalizeOutput{
										Result: &s3.ListDirectoryBucketsOutput{
											Buckets:           buckets,
											ContinuationToken: nextToken,
										},
									}, middleware.Metadata{}, nil
								} else {
									buckets = []types.Bucket{
										{
											Name: aws.String("test2"),
										},
									}
									return middleware.FinalizeOutput{
										Result: &s3.ListDirectoryBucketsOutput{
											Buckets:           buckets,
											ContinuationToken: nextToken,
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
				buckets: []types.Bucket{
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
			name: "list directory buckets with token failure",
			args: args{
				ctx: context.Background(),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					err := stack.Initialize.Add(
						middleware.InitializeMiddlewareFunc(
							"GetToken",
							getTokenForListDirectoryBucketsInitialize,
						), middleware.Before,
					)
					if err != nil {
						return err
					}

					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListDirectoryBucketsWithTokenMock",
							func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								continuationToken := middleware.GetStackValue(ctx, tokenForListDirectoryBuckets{}).(*string)

								var nextToken *string
								var buckets []types.Bucket
								if continuationToken == nil {
									nextToken = aws.String("NextToken")
									buckets = []types.Bucket{
										{
											Name: aws.String("test1"),
										},
									}
									return middleware.FinalizeOutput{
										Result: &s3.ListDirectoryBucketsOutput{
											Buckets:           buckets,
											ContinuationToken: nextToken,
										},
									}, middleware.Metadata{}, nil
								} else {
									return middleware.FinalizeOutput{
										Result: &s3.ListDirectoryBucketsOutput{},
									}, middleware.Metadata{}, fmt.Errorf("ListDirectoryBucketsError")
								}
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				buckets: []types.Bucket{
					{
						Name: aws.String("test1"),
					},
				},
				err: &ClientError{
					Err: fmt.Errorf("operation error S3: ListDirectoryBuckets, ListDirectoryBucketsError"),
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

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client, true)

			output, err := s3Client.listDirectoryBuckets(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err.Error(), tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.err.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.err.Error())
				return
			}
			if !reflect.DeepEqual(output, tt.want.buckets) {
				t.Errorf("output = %#v, want %#v", output, tt.want.buckets)
			}
		})
	}
}

func TestS3_GetBucketLocation(t *testing.T) {
	type args struct {
		ctx                  context.Context
		bucketName           *string
		directoryBucketsMode bool
		withAPIOptionsFunc   func(*middleware.Stack) error
	}

	type want struct {
		region string
		err    error
	}

	cases := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "get bucket location successfully",
			args: args{
				ctx:                  context.Background(),
				bucketName:           aws.String("test"),
				directoryBucketsMode: false,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"GetBucketLocationMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.GetBucketLocationOutput{
										LocationConstraint: "us-east-1",
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				region: "us-east-1",
				err:    nil,
			},
			wantErr: false,
		},
		{
			name: "return empty string on directory buckets mode",
			args: args{
				ctx:                  context.Background(),
				bucketName:           aws.String("test"),
				directoryBucketsMode: true,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return nil
				},
			},
			want: want{
				region: "",
				err:    nil,
			},
			wantErr: false,
		},
		{
			name: "get bucket location successfully for us-east-1(empty)",
			args: args{
				ctx:                  context.Background(),
				bucketName:           aws.String("test"),
				directoryBucketsMode: false,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"GetBucketLocationMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.GetBucketLocationOutput{
										LocationConstraint: "",
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				region: "us-east-1",
				err:    nil,
			},
			wantErr: false,
		},
		{
			name: "get bucket location failure",
			args: args{
				ctx:                  context.Background(),
				bucketName:           aws.String("test"),
				directoryBucketsMode: false,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"GetBucketLocationErrorMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: nil,
								}, middleware.Metadata{}, fmt.Errorf("GetBucketLocationError")
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				region: "",
				err: &ClientError{
					ResourceName: aws.String("test"),
					Err:          fmt.Errorf("operation error S3: GetBucketLocation, GetBucketLocationError"),
				},
			},
			wantErr: true,
		},
		{
			name: "get bucket location failure for api error SlowDown",
			args: args{
				ctx:                  context.Background(),
				bucketName:           aws.String("test"),
				directoryBucketsMode: false,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"GetBucketLocationApiErrorMock",
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
			want: want{
				region: "",
				err: &ClientError{
					ResourceName: aws.String("test"),
					Err:          fmt.Errorf("operation error S3: GetBucketLocation, exceeded maximum number of attempts, 20, api error SlowDown"),
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

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client, tt.args.directoryBucketsMode)

			output, err := s3Client.GetBucketLocation(tt.args.ctx, tt.args.bucketName)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err.Error(), tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.err.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.err.Error())
				return
			}
			if !reflect.DeepEqual(output, tt.want.region) {
				t.Errorf("output = %#v, want %#v", output, tt.want.region)
			}
		})
	}
}

func TestS3_supportsVersions(t *testing.T) {
	type fields struct {
		directoryBucketsMode bool
		baseEndpoint         *string
	}

	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "directory buckets mode returns false",
			fields: fields{
				directoryBucketsMode: true,
				baseEndpoint:         nil,
			},
			want: false,
		},
		{
			name: "nil endpoint with standard mode returns true",
			fields: fields{
				directoryBucketsMode: false,
				baseEndpoint:         nil,
			},
			want: true,
		},
		{
			name: "empty endpoint with standard mode returns true",
			fields: fields{
				directoryBucketsMode: false,
				baseEndpoint:         aws.String(""),
			},
			want: true,
		},
		{
			name: "Cloudflare R2 endpoint returns false",
			fields: fields{
				directoryBucketsMode: false,
				baseEndpoint:         aws.String("https://account.r2.cloudflarestorage.com"),
			},
			want: false,
		},
		{
			name: "AWS S3 endpoint with standard mode returns true",
			fields: fields{
				directoryBucketsMode: false,
				baseEndpoint:         aws.String("https://s3.amazonaws.com"),
			},
			want: true,
		},
		{
			name: "LocalStack endpoint with standard mode returns true",
			fields: fields{
				directoryBucketsMode: false,
				baseEndpoint:         aws.String("http://localhost:4566"),
			},
			want: true,
		},
		{
			name: "MinIO endpoint with standard mode returns true",
			fields: fields{
				directoryBucketsMode: false,
				baseEndpoint:         aws.String("https://minio.example.com"),
			},
			want: true,
		},
		{
			name: "Custom S3-compatible endpoint with standard mode returns true",
			fields: fields{
				directoryBucketsMode: false,
				baseEndpoint:         aws.String("https://s3.custom-provider.com"),
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Create a mock S3 client with the specified baseEndpoint
			cfg, err := config.LoadDefaultConfig(
				ctx,
				config.WithRegion("us-east-1"),
			)
			if err != nil {
				t.Fatal(err)
			}

			var client *s3.Client
			if tt.fields.baseEndpoint != nil {
				client = s3.NewFromConfig(cfg, func(o *s3.Options) {
					o.BaseEndpoint = tt.fields.baseEndpoint
				})
			} else {
				client = s3.NewFromConfig(cfg)
			}

			s3Client := NewS3(client, tt.fields.directoryBucketsMode)

			got := s3Client.supportsVersions()
			if got != tt.want {
				t.Errorf("supportsVersions() = %v, want %v", got, tt.want)
			}
		})
	}
}
