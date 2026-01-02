package client

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
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
			isErrorRetryableFunc: func(err error) bool { return err == testErr },
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
