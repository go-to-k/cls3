package client

import (
	"math/rand/v2"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
)

const MaxRetryCount = 20

var _ aws.RetryerV2 = (*Retryer)(nil)

type Retryer struct {
	aws.RetryerV2
	isErrorRetryableFunc func(error) bool
	delayTimeSec         int
}

func NewRetryer(isErrorRetryableFunc func(error) bool, delayTimeSec int) *Retryer {
	retryer := retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = MaxRetryCount

		o.Backoff = retry.BackoffDelayerFunc(
			func(attempt int, err error) (time.Duration, error) {
				waitTime := 1
				if delayTimeSec > 1 {
					//nolint:gosec
					waitTime += rand.IntN(delayTimeSec)
				}
				return time.Duration(waitTime) * time.Second, nil
			},
		)

		o.Retryables = append(o.Retryables, retry.IsErrorRetryableFunc(
			func(err error) aws.Ternary {
				if err == nil {
					// Return UnknownTernary instead of FalseTernary to delegate the decision to other Retryable checkers
					return aws.UnknownTernary
				}

				if isErrorRetryableFunc(err) {
					return aws.TrueTernary
				}

				return aws.UnknownTernary
			},
		))
	})

	return &Retryer{
		RetryerV2:            retryer,
		isErrorRetryableFunc: isErrorRetryableFunc,
		delayTimeSec:         delayTimeSec,
	}
}
