package client

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
)

func TestCheckErrorRetryable(t *testing.T) {
	testErr := errors.New("test error")

	tests := []struct {
		name                 string
		isErrorRetryableFunc func(error) bool
		inputErr             error
		expected             aws.Ternary
	}{
		{
			name:                 "nil error returns UnknownTernary",
			isErrorRetryableFunc: func(error) bool { return true },
			inputErr:             nil,
			expected:             aws.UnknownTernary,
		},
		{
			name:                 "retryable error returns TrueTernary",
			isErrorRetryableFunc: func(err error) bool { return errors.Is(err, testErr) },
			inputErr:             testErr,
			expected:             aws.TrueTernary,
		},
		{
			name:                 "non-retryable error returns UnknownTernary",
			isErrorRetryableFunc: func(err error) bool { return false },
			inputErr:             errors.New("non-retryable error"),
			expected:             aws.UnknownTernary,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn := checkErrorRetryable(tt.isErrorRetryableFunc)
			result := fn(tt.inputErr)

			if result != tt.expected {
				t.Errorf("result = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestCheckErrorRetryable_Integration(t *testing.T) {
	retryableErr := errors.New("retryable error")
	attemptCount := 0

	cases := []struct {
		name               string
		withAPIOptionsFunc func(*middleware.Stack) error
		expectedAttempts   int
		wantErr            bool
		expectedErrString  string
	}{
		{
			name: "retry with custom retryable error logic",
			withAPIOptionsFunc: func(stack *middleware.Stack) error {
				return stack.Finalize.Add(
					middleware.FinalizeMiddlewareFunc(
						"RetryableErrorMock",
						func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
							attemptCount++
							return middleware.FinalizeOutput{
								Result: nil,
							}, middleware.Metadata{}, retryableErr
						},
					),
					middleware.After,
				)
			},
			expectedAttempts:  MaxAttempts,
			wantErr:           true,
			expectedErrString: "retryable error",
		},
		{
			name: "no retry with non-retryable error",
			withAPIOptionsFunc: func(stack *middleware.Stack) error {
				return stack.Finalize.Add(
					middleware.FinalizeMiddlewareFunc(
						"NonRetryableErrorMock",
						func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
							attemptCount++
							return middleware.FinalizeOutput{
								Result: nil,
							}, middleware.Metadata{}, errors.New("non-retryable error")
						},
					),
					middleware.After,
				)
			},
			expectedAttempts:  1,
			wantErr:           true,
			expectedErrString: "non-retryable error",
		},
		{
			name: "success without retry",
			withAPIOptionsFunc: func(stack *middleware.Stack) error {
				return stack.Finalize.Add(
					middleware.FinalizeMiddlewareFunc(
						"SuccessMock",
						func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
							attemptCount++
							return middleware.FinalizeOutput{
								Result: &s3.ListBucketsOutput{},
							}, middleware.Metadata{}, nil
						},
					),
					middleware.After,
				)
			},
			expectedAttempts: 1,
			wantErr:          false,
		},
		{
			name: "retry then success",
			withAPIOptionsFunc: func(stack *middleware.Stack) error {
				return stack.Finalize.Add(
					middleware.FinalizeMiddlewareFunc(
						"RetryThenSuccessMock",
						func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
							attemptCount++
							if attemptCount < 3 {
								return middleware.FinalizeOutput{
									Result: nil,
								}, middleware.Metadata{}, retryableErr
							}
							return middleware.FinalizeOutput{
								Result: &s3.ListBucketsOutput{},
							}, middleware.Metadata{}, nil
						},
					),
					middleware.After,
				)
			},
			expectedAttempts: 3,
			wantErr:          false,
		},
		{
			name: "retry with default retryable error code defined in SDK",
			withAPIOptionsFunc: func(stack *middleware.Stack) error {
				return stack.Finalize.Add(
					middleware.FinalizeMiddlewareFunc(
						"RequestTimeoutMock",
						func(ctx context.Context, input middleware.FinalizeInput, handler middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
							attemptCount++
							return middleware.FinalizeOutput{
									Result: nil,
								}, middleware.Metadata{}, &smithy.GenericAPIError{
									Code:    "RequestTimeout",
									Message: "Request timeout",
								}
						},
					),
					middleware.After,
				)
			},
			expectedAttempts:  MaxAttempts,
			wantErr:           true,
			expectedErrString: "RequestTimeout",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			attemptCount = 0

			retryer := NewRetryer(func(err error) bool {
				return errors.Is(err, retryableErr)
			}, 0)

			cfg, err := config.LoadDefaultConfig(
				context.Background(),
				config.WithRegion("us-east-1"),
				config.WithCredentialsProvider(aws.AnonymousCredentials{}),
				config.WithRetryer(func() aws.Retryer { return retryer }),
				config.WithAPIOptions([]func(*middleware.Stack) error{tt.withAPIOptionsFunc}),
			)
			if err != nil {
				t.Fatal(err)
			}

			client := s3.NewFromConfig(cfg)
			_, err = client.ListBuckets(context.Background(), &s3.ListBucketsInput{})

			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr && tt.expectedErrString != "" {
				if !strings.Contains(err.Error(), tt.expectedErrString) {
					t.Errorf("error = %v, expected to contain %v", err, tt.expectedErrString)
				}
			}

			if attemptCount != tt.expectedAttempts {
				t.Errorf("attemptCount = %d, want %d", attemptCount, tt.expectedAttempts)
			}
		})
	}
}
