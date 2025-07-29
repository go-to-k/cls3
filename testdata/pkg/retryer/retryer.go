package retryer

import (
	"context"
	"math/rand"
	"strings"
	"time"
)

// Retryer implements the aws.RetryerV2 interface
type Retryer struct {
	isErrorRetryableFunc func(error) bool
	delayTimeSec         int
	maxAttempts          int
}

// NewRetryer creates a new Retryer instance
func NewRetryer(isErrorRetryableFunc func(error) bool, delayTimeSec int, maxAttempts int) *Retryer {
	return &Retryer{
		isErrorRetryableFunc: isErrorRetryableFunc,
		delayTimeSec:         delayTimeSec,
		maxAttempts:          maxAttempts,
	}
}

// IsErrorRetryable determines if an error is retryable
func (r *Retryer) IsErrorRetryable(err error) bool {
	return r.isErrorRetryableFunc(err)
}

// MaxAttempts returns the maximum number of retry attempts
func (r *Retryer) MaxAttempts() int {
	return r.maxAttempts
}

// RetryDelay returns the wait time between retries
func (r *Retryer) RetryDelay(int, error) (time.Duration, error) {
	waitTime := 1
	if r.delayTimeSec > 1 {
		waitTime += rand.Intn(r.delayTimeSec)
	}
	return time.Duration(waitTime) * time.Second, nil
}

// GetRetryToken implements the aws.RetryerV2 interface
func (r *Retryer) GetRetryToken(context.Context, error) (func(error) error, error) {
	return func(error) error { return nil }, nil
}

// GetInitialToken implements the aws.RetryerV2 interface
func (r *Retryer) GetInitialToken() func(error) error {
	return func(error) error { return nil }
}

// GetAttemptToken implements the aws.RetryerV2 interface
func (r *Retryer) GetAttemptToken(context.Context) (func(error) error, error) {
	return func(error) error { return nil }, nil
}

// CreateS3Retryer creates a Retryer instance for S3 operations
func CreateS3Retryer() *Retryer {
	retryable := func(err error) bool {
		isRetryable :=
			// SlowDown error
			strings.Contains(err.Error(), "api error SlowDown") ||
				// no such host error
				strings.Contains(err.Error(), "lookup") && strings.Contains(err.Error(), "no such host") ||
				// StatusCode: 0 error
				strings.Contains(err.Error(), "https response error StatusCode: 0") ||
				// 503 Service Unavailable error
				strings.Contains(err.Error(), "StatusCode: 503") ||
				// EOF error
				strings.Contains(err.Error(), "EOF") ||
				// Please try again error
				strings.Contains(err.Error(), "Please try again")

		return isRetryable
	}

	return NewRetryer(retryable, 20, 20) // 20 second delay, maximum 20 retries
}
