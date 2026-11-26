package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

const (
	listObjectsV2EmptyBody = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><IsTruncated>false</IsTruncated></ListBucketResult>`
	deleteObjectsEmptyBody = `<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`
)

func newFakeS3Client(objectsPerSecond int) *s3.Client {
	httpClient := smithyhttp.ClientDoFunc(func(r *http.Request) (*http.Response, error) {
		body := listObjectsV2EmptyBody
		if r.Method == http.MethodPost {
			body = deleteObjectsEmptyBody
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/xml"}},
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    r,
		}, nil
	})

	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: aws.AnonymousCredentials{},
		HTTPClient:  httpClient,
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, NewRateLimitAPIOptions(objectsPerSecond)...)
	})
}

func deleteObjectsInput(n int) *s3.DeleteObjectsInput {
	objects := make([]types.ObjectIdentifier, 0, n)
	for i := 0; i < n; i++ {
		objects = append(objects, types.ObjectIdentifier{Key: aws.String(fmt.Sprintf("key-%d", i))})
	}
	return &s3.DeleteObjectsInput{
		Bucket: aws.String("bucket"),
		Delete: &types.Delete{Objects: objects},
	}
}

func TestNewRateLimitAPIOptions_Unlimited(t *testing.T) {
	if got := NewRateLimitAPIOptions(0); len(got) != 0 {
		t.Errorf("expected no API options for 0, got %d", len(got))
	}
	if got := NewRateLimitAPIOptions(-1); len(got) != 0 {
		t.Errorf("expected no API options for -1, got %d", len(got))
	}
}

func TestRateLimitAPIOptions_ListingIsNotLimited(t *testing.T) {
	client := newFakeS3Client(1)

	start := time.Now()
	for i := 0; i < 3; i++ {
		if _, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("bucket")}); err != nil {
			t.Fatalf("list request %d failed: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Errorf("list requests should not be throttled, took %v", elapsed)
	}
}

func TestRateLimitAPIOptions_DeletesArePacedByObjects(t *testing.T) {
	const objectsPerSecond = 10
	client := newFakeS3Client(objectsPerSecond)

	// The first 10 objects use the initial burst. The next 10 must wait about 1 second.
	start := time.Now()
	for i := 0; i < 2; i++ {
		if _, err := client.DeleteObjects(context.Background(), deleteObjectsInput(10)); err != nil {
			t.Fatalf("delete request %d failed: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	if elapsed < 900*time.Millisecond {
		t.Errorf("20 objects at 10/s were not throttled: took %v, expected at least ~1s", elapsed)
	}
}

func TestRateLimitAPIOptions_LargeDeleteBatchIsChunked(t *testing.T) {
	const objectsPerSecond = 10
	client := newFakeS3Client(objectsPerSecond)

	// 25 objects in one request: 10 use the burst, the other 15 take about 1.5 seconds.
	// The limiter rejects requests larger than its burst, so this checks the chunking.
	start := time.Now()
	if _, err := client.DeleteObjects(context.Background(), deleteObjectsInput(25)); err != nil {
		t.Fatalf("delete request failed: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 1400*time.Millisecond {
		t.Errorf("25 objects at 10/s were not throttled: took %v, expected at least ~1.5s", elapsed)
	}
}

func TestRateLimitAPIOptions_CancelledContext(t *testing.T) {
	client := newFakeS3Client(1)

	// The first request uses the single token. The second must wait ~1s, and cancellation must stop it.
	if _, err := client.DeleteObjects(context.Background(), deleteObjectsInput(1)); err != nil {
		t.Fatalf("first request failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := client.DeleteObjects(ctx, deleteObjectsInput(1))
	if err == nil {
		t.Fatal("expected an error from the cancelled context, got nil")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("cancelled request waited too long: %v", elapsed)
	}
}
