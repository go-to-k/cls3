package client

import (
	"context"

	"github.com/aws/smithy-go/middleware"
	"golang.org/x/time/rate"
)

const rateLimitMiddlewareID = "Cls3RateLimit"

// NewRateLimitAPIOptions returns SDK API options that limit the number of requests
// sent per second by a client to requestsPerSecond. A single limiter is shared by all
// operations of the client. The limiter is placed after the retry middleware, so each
// retried attempt is also counted against the limit.
// It returns no options when requestsPerSecond is 0 or less (unlimited).
func NewRateLimitAPIOptions(requestsPerSecond int) []func(*middleware.Stack) error {
	if requestsPerSecond <= 0 {
		return []func(*middleware.Stack) error{}
	}

	limiter := rate.NewLimiter(rate.Limit(requestsPerSecond), 1)
	return []func(*middleware.Stack) error{
		func(stack *middleware.Stack) error {
			return stack.Finalize.Insert(&rateLimitMiddleware{limiter: limiter}, "Retry", middleware.After)
		},
	}
}

type rateLimitMiddleware struct {
	limiter *rate.Limiter
}

var _ middleware.FinalizeMiddleware = (*rateLimitMiddleware)(nil)

func (m *rateLimitMiddleware) ID() string {
	return rateLimitMiddlewareID
}

func (m *rateLimitMiddleware) HandleFinalize(
	ctx context.Context,
	in middleware.FinalizeInput,
	next middleware.FinalizeHandler,
) (middleware.FinalizeOutput, middleware.Metadata, error) {
	if err := m.limiter.Wait(ctx); err != nil {
		return middleware.FinalizeOutput{}, middleware.Metadata{}, err
	}
	return next.HandleFinalize(ctx, in)
}
