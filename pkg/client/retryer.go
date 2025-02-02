package client

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

const MaxRetryCount = 20

var _ aws.RetryerV2 = (*Retryer)(nil)

type Retryer struct {
	isErrorRetryableFunc func(error) bool
	delayTimeSec         int
}

func NewRetryer(isErrorRetryableFunc func(error) bool, delayTimeSec int) *Retryer {
	return &Retryer{
		isErrorRetryableFunc: isErrorRetryableFunc,
		delayTimeSec:         delayTimeSec,
	}
}

func (r *Retryer) IsErrorRetryable(err error) bool {
	return r.isErrorRetryableFunc(err)
}

func (r *Retryer) MaxAttempts() int {
	return MaxRetryCount
}

func (r *Retryer) RetryDelay(int, error) (time.Duration, error) {
	waitTime := 1
	if r.delayTimeSec > 1 {
		//nolint:gosec
		waitTime += rand.IntN(r.delayTimeSec)
	}
	return time.Duration(waitTime) * time.Second, nil
}

func (r *Retryer) GetRetryToken(context.Context, error) (func(error) error, error) {
	return func(error) error { return nil }, nil
}

func (r *Retryer) GetInitialToken() func(error) error {
	return func(error) error { return nil }
}

func (r *Retryer) GetAttemptToken(context.Context) (func(error) error, error) {
	return func(error) error { return nil }, nil
}
