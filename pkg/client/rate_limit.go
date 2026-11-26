package client

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	"golang.org/x/time/rate"
)

const (
	objectCountMiddlewareID = "Cls3ObjectCount"
	rateLimitMiddlewareID   = "Cls3RateLimit"
)

type objectCountContextKey struct{}

// NewRateLimitAPIOptions returns SDK API options that limit how many S3 objects per second
// the client processes. Only DeleteObjects requests are counted: each attempt consumes one token
// per object in the request. Requests that do not delete objects (e.g. list or bucket operations)
// are not limited.
//
// A single limiter is shared by all operations of the client. The limiter is placed after the
// retry middleware, so each retried attempt is counted again.
//
// It returns no options when objectsPerSecond is 0 or less (unlimited).
func NewRateLimitAPIOptions(objectsPerSecond int) []func(*middleware.Stack) error {
	if objectsPerSecond <= 0 {
		return []func(*middleware.Stack) error{}
	}

	// Burst equals the rate, so the average rate stays at objectsPerSecond.
	limiter := rate.NewLimiter(rate.Limit(objectsPerSecond), objectsPerSecond)
	return []func(*middleware.Stack) error{
		func(stack *middleware.Stack) error {
			if err := stack.Initialize.Add(&objectCountMiddleware{}, middleware.Before); err != nil {
				return err
			}
			return stack.Finalize.Insert(&rateLimitMiddleware{limiter: limiter}, "Retry", middleware.After)
		},
	}
}

// objectCountMiddleware runs in the Initialize step, where the typed operation input is still
// available, and stores the number of objects to delete in the context. The Finalize step only
// sees the serialized request, so the count has to be passed through the context.
type objectCountMiddleware struct{}

var _ middleware.InitializeMiddleware = (*objectCountMiddleware)(nil)

func (m *objectCountMiddleware) ID() string {
	return objectCountMiddlewareID
}

func (m *objectCountMiddleware) HandleInitialize(
	ctx context.Context,
	in middleware.InitializeInput,
	next middleware.InitializeHandler,
) (middleware.InitializeOutput, middleware.Metadata, error) {
	if n, ok := deleteObjectsCount(in.Parameters); ok {
		ctx = context.WithValue(ctx, objectCountContextKey{}, n)
	}
	return next.HandleInitialize(ctx, in)
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
	if n, ok := ctx.Value(objectCountContextKey{}).(int); ok {
		if err := m.waitForObjects(ctx, n); err != nil {
			return middleware.FinalizeOutput{}, middleware.Metadata{}, err
		}
	}
	return next.HandleFinalize(ctx, in)
}

// deleteObjectsCount returns the number of objects in a DeleteObjects input.
// ok is false for any other operation input.
func deleteObjectsCount(params interface{}) (int, bool) {
	input, ok := params.(*s3.DeleteObjectsInput)
	if !ok || input.Delete == nil {
		return 0, false
	}
	return len(input.Delete.Objects), true
}

// waitForObjects takes n tokens from the limiter. The limiter rejects requests larger than its
// burst, so the wait is split into chunks of at most the burst size.
func (m *rateLimitMiddleware) waitForObjects(ctx context.Context, n int) error {
	burst := m.limiter.Burst()
	for n > 0 {
		chunk := min(n, burst)
		if err := m.limiter.WaitN(ctx, chunk); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}
