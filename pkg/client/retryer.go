package client

import (
	"math/rand/v2"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
)

const MaxAttempts = 20

type Retryer struct {
	aws.RetryerV2
}

func NewRetryer(isErrorRetryableFunc func(error) bool, delayTimeSec int) *Retryer {
	retryer := retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = MaxAttempts
		o.Backoff = retry.BackoffDelayerFunc(backoffDelay(delayTimeSec))
		o.Retryables = append(o.Retryables, retry.IsErrorRetryableFunc(checkErrorRetryable(isErrorRetryableFunc)))
	})

	return &Retryer{
		RetryerV2: retryer,
	}
}

func backoffDelay(delayTimeSec int) func(int, error) (time.Duration, error) {
	return func(attempt int, err error) (time.Duration, error) {
		waitTime := 1
		if delayTimeSec > 1 {
			//nolint:gosec
			waitTime += rand.IntN(delayTimeSec)
		}
		return time.Duration(waitTime) * time.Second, nil
	}
}

func checkErrorRetryable(isErrorRetryableFunc func(error) bool) func(error) aws.Ternary {
	return func(err error) aws.Ternary {
		if err == nil {
			// Return UnknownTernary instead of FalseTernary to delegate the decision to other Retryable checkers
			return aws.UnknownTernary
		}

		if isErrorRetryableFunc(err) {
			return aws.TrueTernary
		}

		return aws.UnknownTernary
	}
}
