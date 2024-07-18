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

type keyMarkerKeyForS3 struct{}
type versionIdMarkerKeyForS3 struct{}

func getNextMarkerForS3Initialize(
	ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
) (
	out middleware.InitializeOutput, metadata middleware.Metadata, err error,
) {
	switch v := in.Parameters.(type) {
	case *s3.ListObjectVersionsInput:
		ctx = middleware.WithStackValue(ctx, keyMarkerKeyForS3{}, v.KeyMarker)
		ctx = middleware.WithStackValue(ctx, versionIdMarkerKeyForS3{}, v.VersionIdMarker)
	}
	return next.HandleInitialize(ctx, in)
}

type targetObjectsForDeleteObjects struct{}

func setTargetObjectsForDeleteObjectsInitialize(
	ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
) (
	out middleware.InitializeOutput, metadata middleware.Metadata, err error,
) {
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
				region:     "ap-northeast-1",
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
				region:     "ap-northeast-1",
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
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("ap-northeast-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client)

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
				region:     "ap-northeast-1",
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
				region:     "ap-northeast-1",
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
				region:     "ap-northeast-1",
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
			name: "delete objects failure for api error",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "ap-northeast-1",
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
					Err:          fmt.Errorf("operation error S3: DeleteObjects, exceeded maximum number of attempts, 10, api error SlowDown"),
				},
			},
			wantErr: true,
		},
		{
			name: "delete objects failure for output errors",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				region:     "ap-northeast-1",
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
				region:     "ap-northeast-1",
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
				region:     "ap-northeast-1",
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
				region:     "ap-northeast-1",
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
				region:     "ap-northeast-1",
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
										// 2nd and 3rd object is not an error
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
				config.WithRegion("ap-northeast-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client)

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

func TestS3_ListObjectVersions(t *testing.T) {
	type args struct {
		ctx                context.Context
		bucketName         *string
		region             string
		oldVersionsOnly    bool
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	type want struct {
		output []types.ObjectIdentifier
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
				region:          "ap-northeast-1",
				oldVersionsOnly: false,
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
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: []types.ObjectIdentifier{
					{
						Key:       aws.String("KeyForVersions"),
						VersionId: aws.String("VersionIdForVersions"),
					},
					{
						Key:       aws.String("KeyForDeleteMarkers"),
						VersionId: aws.String("VersionIdForDeleteMarkers"),
					},
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
				region:          "ap-northeast-1",
				oldVersionsOnly: false,
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
			name: "list objects versions successfully(empty)",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "ap-northeast-1",
				oldVersionsOnly: false,
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
				output: []types.ObjectIdentifier{},
				err:    nil,
			},
			wantErr: false,
		},
		{
			name: "list objects versions successfully(versions only)",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "ap-northeast-1",
				oldVersionsOnly: false,
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
				output: []types.ObjectIdentifier{
					{
						Key:       aws.String("KeyForVersions"),
						VersionId: aws.String("VersionIdForVersions"),
					},
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
				region:          "ap-northeast-1",
				oldVersionsOnly: false,
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
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				output: []types.ObjectIdentifier{
					{
						Key:       aws.String("KeyForDeleteMarkers"),
						VersionId: aws.String("VersionIdForDeleteMarkers"),
					},
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list objects versions with marker successfully",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "ap-northeast-1",
				oldVersionsOnly: false,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					err := stack.Initialize.Add(
						middleware.InitializeMiddlewareFunc(
							"GetNextMarker",
							getNextMarkerForS3Initialize,
						), middleware.Before,
					)
					if err != nil {
						return err
					}

					err = stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectVersionsWithMarkerMock",
							func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								keyMarker := middleware.GetStackValue(ctx, keyMarkerKeyForS3{}).(*string)
								versionIdMarker := middleware.GetStackValue(ctx, versionIdMarkerKeyForS3{}).(*string)

								var nextKeyMarker *string
								var nextVersionIdMarker *string
								var objectVersions []types.ObjectVersion
								var objectDeleteMarkers []types.DeleteMarkerEntry
								if keyMarker == nil && versionIdMarker == nil {
									nextKeyMarker = aws.String("NextMarker")
									nextVersionIdMarker = aws.String("NextMarker")
									objectVersions = []types.ObjectVersion{
										{
											Key:       aws.String("KeyForVersions1"),
											VersionId: aws.String("VersionIdForVersions1"),
										},
									}
									objectDeleteMarkers = []types.DeleteMarkerEntry{
										{
											Key:       aws.String("KeyForDeleteMarkers1"),
											VersionId: aws.String("VersionIdForDeleteMarkers1"),
										},
									}
									return middleware.FinalizeOutput{
										Result: &s3.ListObjectVersionsOutput{
											Versions:            objectVersions,
											DeleteMarkers:       objectDeleteMarkers,
											NextKeyMarker:       nextKeyMarker,
											NextVersionIdMarker: nextVersionIdMarker,
										},
									}, middleware.Metadata{}, nil
								} else {
									objectVersions = []types.ObjectVersion{
										{
											Key:       aws.String("KeyForVersions2"),
											VersionId: aws.String("VersionIdForVersions2"),
										},
									}
									objectDeleteMarkers = []types.DeleteMarkerEntry{
										{
											Key:       aws.String("KeyForDeleteMarkers2"),
											VersionId: aws.String("VersionIdForDeleteMarkers2"),
										},
									}
									return middleware.FinalizeOutput{
										Result: &s3.ListObjectVersionsOutput{
											Versions:            objectVersions,
											DeleteMarkers:       objectDeleteMarkers,
											NextKeyMarker:       nextKeyMarker,
											NextVersionIdMarker: nextVersionIdMarker,
										},
									}, middleware.Metadata{}, nil
								}
							},
						),
						middleware.Before,
					)
					return err
				},
			},
			want: want{
				output: []types.ObjectIdentifier{
					{
						Key:       aws.String("KeyForVersions1"),
						VersionId: aws.String("VersionIdForVersions1"),
					},
					{
						Key:       aws.String("KeyForDeleteMarkers1"),
						VersionId: aws.String("VersionIdForDeleteMarkers1"),
					},
					{
						Key:       aws.String("KeyForVersions2"),
						VersionId: aws.String("VersionIdForVersions2"),
					},
					{
						Key:       aws.String("KeyForDeleteMarkers2"),
						VersionId: aws.String("VersionIdForDeleteMarkers2"),
					},
				},
				err: nil,
			},
			wantErr: false,
		},
		{
			name: "list objects versions with marker failure",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "ap-northeast-1",
				oldVersionsOnly: false,
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					err := stack.Initialize.Add(
						middleware.InitializeMiddlewareFunc(
							"GetNextMarker",
							getNextMarkerForS3Initialize,
						), middleware.Before,
					)
					if err != nil {
						return err
					}

					err = stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListObjectVersionsWithMarkerErrorMock",
							func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								keyMarker := middleware.GetStackValue(ctx, keyMarkerKeyForS3{}).(*string)
								versionIdMarker := middleware.GetStackValue(ctx, versionIdMarkerKeyForS3{}).(*string)

								var nextKeyMarker *string
								var nextVersionIdMarker *string
								var objectVersions []types.ObjectVersion
								var objectDeleteMarkers []types.DeleteMarkerEntry
								if keyMarker == nil && versionIdMarker == nil {
									nextKeyMarker = aws.String("NextMarker")
									nextVersionIdMarker = aws.String("NextMarker")
									objectVersions = []types.ObjectVersion{
										{
											Key:       aws.String("KeyForVersions1"),
											VersionId: aws.String("VersionIdForVersions1"),
										},
									}
									objectDeleteMarkers = []types.DeleteMarkerEntry{
										{
											Key:       aws.String("KeyForDeleteMarkers1"),
											VersionId: aws.String("VersionIdForDeleteMarkers1"),
										},
									}
									return middleware.FinalizeOutput{
										Result: &s3.ListObjectVersionsOutput{
											Versions:            objectVersions,
											DeleteMarkers:       objectDeleteMarkers,
											NextKeyMarker:       nextKeyMarker,
											NextVersionIdMarker: nextVersionIdMarker,
										},
									}, middleware.Metadata{}, nil
								} else {
									return middleware.FinalizeOutput{
										Result: &s3.ListObjectVersionsOutput{},
									}, middleware.Metadata{}, fmt.Errorf("ListObjectVersionsError")
								}
							},
						),
						middleware.Before,
					)
					return err
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
				region:          "ap-northeast-1",
				oldVersionsOnly: true,
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
				output: []types.ObjectIdentifier{
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
				err: nil,
			},
			wantErr: false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("ap-northeast-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client)

			output, err := s3Client.ListObjectVersions(tt.args.ctx, tt.args.bucketName, tt.args.region, tt.args.oldVersionsOnly)
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

func TestS3_ListObjectVersionsByPage(t *testing.T) {
	type args struct {
		ctx                context.Context
		bucketName         *string
		region             string
		oldVersionsOnly    bool
		keyMarker          *string
		versionIdMarker    *string
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	type want struct {
		output              []types.ObjectIdentifier
		nextKeyMarker       *string
		nextVersionIdMarker *string
		err                 error
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
				region:          "ap-northeast-1",
				oldVersionsOnly: false,
				keyMarker:       nil,
				versionIdMarker: nil,
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
				output: []types.ObjectIdentifier{
					{
						Key:       aws.String("KeyForVersions"),
						VersionId: aws.String("VersionIdForVersions"),
					},
					{
						Key:       aws.String("KeyForDeleteMarkers"),
						VersionId: aws.String("VersionIdForDeleteMarkers"),
					},
				},
				nextKeyMarker:       aws.String("NextKeyMarker"),
				nextVersionIdMarker: aws.String("NextVersionIdMarker"),
				err:                 nil,
			},
			wantErr: false,
		},
		{
			name: "list objects versions failure",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "ap-northeast-1",
				oldVersionsOnly: false,
				keyMarker:       nil,
				versionIdMarker: nil,
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
				output:              nil,
				nextKeyMarker:       nil,
				nextVersionIdMarker: nil,
				err: &ClientError{
					ResourceName: aws.String("test"),
					Err:          fmt.Errorf("operation error S3: ListObjectVersions, ListObjectVersionsError"),
				},
			},
			wantErr: true,
		},
		{
			name: "list objects versions successfully(empty)",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "ap-northeast-1",
				oldVersionsOnly: false,
				keyMarker:       nil,
				versionIdMarker: nil,
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
				output:              []types.ObjectIdentifier{},
				nextKeyMarker:       nil,
				nextVersionIdMarker: nil,
				err:                 nil,
			},
			wantErr: false,
		},
		{
			name: "list objects versions successfully(versions only)",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "ap-northeast-1",
				oldVersionsOnly: false,
				keyMarker:       nil,
				versionIdMarker: nil,
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
				output: []types.ObjectIdentifier{
					{
						Key:       aws.String("KeyForVersions"),
						VersionId: aws.String("VersionIdForVersions"),
					},
				},
				nextKeyMarker:       aws.String("NextKeyMarker"),
				nextVersionIdMarker: aws.String("NextVersionIdMarker"),
				err:                 nil,
			},
			wantErr: false,
		},
		{
			name: "list objects versions successfully(delete markers only)",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "ap-northeast-1",
				oldVersionsOnly: false,
				keyMarker:       nil,
				versionIdMarker: nil,
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
				output: []types.ObjectIdentifier{
					{
						Key:       aws.String("KeyForDeleteMarkers"),
						VersionId: aws.String("VersionIdForDeleteMarkers"),
					},
				},
				nextKeyMarker:       aws.String("NextKeyMarker"),
				nextVersionIdMarker: aws.String("NextVersionIdMarker"),
				err:                 nil,
			},
			wantErr: false,
		},
		{
			name: "list objects versions with markers successfully",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "ap-northeast-1",
				oldVersionsOnly: false,
				keyMarker:       aws.String("NextKeyMarker"),
				versionIdMarker: aws.String("NextVersionIdMarker"),
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
				output: []types.ObjectIdentifier{
					{
						Key:       aws.String("KeyForVersions"),
						VersionId: aws.String("VersionIdForVersions"),
					},
					{
						Key:       aws.String("KeyForDeleteMarkers"),
						VersionId: aws.String("VersionIdForDeleteMarkers"),
					},
				},
				nextKeyMarker:       nil,
				nextVersionIdMarker: nil,
				err:                 nil,
			},
			wantErr: false,
		},
		{
			name: "list objects versions with markers failure",
			args: args{
				ctx:             context.Background(),
				bucketName:      aws.String("test"),
				region:          "ap-northeast-1",
				oldVersionsOnly: false,
				keyMarker:       aws.String("NextKeyMarker"),
				versionIdMarker: aws.String("NextVersionIdMarker"),
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
				output:              nil,
				nextKeyMarker:       nil,
				nextVersionIdMarker: nil,
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
				region:          "ap-northeast-1",
				oldVersionsOnly: true,
				keyMarker:       nil,
				versionIdMarker: nil,
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
				output: []types.ObjectIdentifier{
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
				nextKeyMarker:       nil,
				nextVersionIdMarker: nil,
				err:                 nil,
			},
			wantErr: false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("ap-northeast-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client)

			output, nextKeyMarker, nextVersionIdMarker, err := s3Client.ListObjectVersionsByPage(tt.args.ctx, tt.args.bucketName, tt.args.region, tt.args.oldVersionsOnly, tt.args.keyMarker, tt.args.versionIdMarker)
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
			if !reflect.DeepEqual(nextKeyMarker, tt.want.nextKeyMarker) {
				t.Errorf("nextKeyMarker = %#v, want %#v", nextKeyMarker, tt.want.nextKeyMarker)
			}
			if !reflect.DeepEqual(nextVersionIdMarker, tt.want.nextVersionIdMarker) {
				t.Errorf("nextVersionIdMarker = %#v, want %#v", nextVersionIdMarker, tt.want.nextVersionIdMarker)
			}
		})
	}
}

func TestS3_CheckBucketExists(t *testing.T) {
	type args struct {
		ctx                context.Context
		bucketName         *string
		withAPIOptionsFunc func(*middleware.Stack) error
	}

	type want struct {
		exists bool
		err    error
	}

	cases := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "check bucket for bucket exists",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
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
				exists: true,
				err:    nil,
			},
			wantErr: false,
		},
		{
			name: "check bucket for bucket do not exist",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"ListBucketsNotExistMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.ListBucketsOutput{
										Buckets: []types.Bucket{
											{
												Name: aws.String("test0"),
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
				exists: false,
				err:    nil,
			},
			wantErr: false,
		},
		{
			name: "check bucket exists failure",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
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
				exists: false,
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
				config.WithRegion("ap-northeast-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client)

			output, err := s3Client.CheckBucketExists(tt.args.ctx, tt.args.bucketName)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %#v, wantErr %#v", err.Error(), tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.want.err.Error() {
				t.Errorf("err = %#v, want %#v", err.Error(), tt.want.err.Error())
				return
			}
			if !reflect.DeepEqual(output, tt.want.exists) {
				t.Errorf("output = %#v, want %#v", output, tt.want.exists)
			}
		})
	}
}

func TestS3_ListBuckets(t *testing.T) {
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
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("ap-northeast-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client)

			output, err := s3Client.ListBuckets(tt.args.ctx)
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
		ctx                context.Context
		bucketName         *string
		withAPIOptionsFunc func(*middleware.Stack) error
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
				ctx:        context.Background(),
				bucketName: aws.String("test"),
				withAPIOptionsFunc: func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc(
							"GetBucketLocationMock",
							func(context.Context, middleware.FinalizeInput, middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
								return middleware.FinalizeOutput{
									Result: &s3.GetBucketLocationOutput{
										LocationConstraint: "ap-northeast-1",
									},
								}, middleware.Metadata{}, nil
							},
						),
						middleware.Before,
					)
				},
			},
			want: want{
				region: "ap-northeast-1",
				err:    nil,
			},
			wantErr: false,
		},
		{
			name: "get bucket location successfully for us-east-1(empty)",
			args: args{
				ctx:        context.Background(),
				bucketName: aws.String("test"),
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
				ctx:        context.Background(),
				bucketName: aws.String("test"),
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
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(
				tt.args.ctx,
				config.WithRegion("ap-northeast-1"),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.args.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			client := s3.NewFromConfig(cfg)
			s3Client := NewS3(client)

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
