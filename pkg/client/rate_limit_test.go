package client

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

const listObjectsV2EmptyBody = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><IsTruncated>false</IsTruncated></ListBucketResult>`

func newFakeS3Client(t *testing.T, requestsPerSecond int) (*s3.Client, *[]time.Time, *sync.Mutex) {
	t.Helper()

	var mu sync.Mutex
	var requestTimes []time.Time

	httpClient := smithyhttp.ClientDoFunc(func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		requestTimes = append(requestTimes, time.Now())
		mu.Unlock()

		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/xml"}},
			Body:       io.NopCloser(strings.NewReader(listObjectsV2EmptyBody)),
			Request:    r,
		}, nil
	})

	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: aws.AnonymousCredentials{},
		HTTPClient:  httpClient,
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, NewRateLimitAPIOptions(requestsPerSecond)...)
	})
	return client, &requestTimes, &mu
}

func TestNewRateLimitAPIOptions_Unlimited(t *testing.T) {
	if got := NewRateLimitAPIOptions(0); len(got) != 0 {
		t.Errorf("expected no API options for 0, got %d", len(got))
	}
	if got := NewRateLimitAPIOptions(-1); len(got) != 0 {
		t.Errorf("expected no API options for -1, got %d", len(got))
	}
}

func TestRateLimitAPIOptions_LimitsRequestRate(t *testing.T) {
	const requestsPerSecond = 5
	const requests = 4

	client, requestTimes, mu := newFakeS3Client(t, requestsPerSecond)

	for i := 0; i < requests; i++ {
		_, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket: aws.String("bucket"),
		})
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(*requestTimes) != requests {
		t.Fatalf("expected %d requests, got %d", requests, len(*requestTimes))
	}

	// With 5 requests/sec, 4 requests must span at least 3 intervals of 200ms.
	// Allow some slack for scheduling jitter.
	elapsed := (*requestTimes)[requests-1].Sub((*requestTimes)[0])
	minElapsed := 3 * time.Second / requestsPerSecond
	if elapsed < minElapsed-20*time.Millisecond {
		t.Errorf("requests were not throttled: %v elapsed, expected at least %v", elapsed, minElapsed)
	}
}

func TestRateLimitAPIOptions_CancelledContext(t *testing.T) {
	// Burst is 1, so the second request must wait ~1s. A cancelled context must abort the wait.
	client, _, _ := newFakeS3Client(t, 1)

	if _, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("bucket")}); err != nil {
		t.Fatalf("first request failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("bucket")})
	if err == nil {
		t.Fatal("expected an error from the cancelled context, got nil")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("cancelled request waited too long: %v", elapsed)
	}
}
