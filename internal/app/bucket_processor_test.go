package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_determineConcurrencyNumber(t *testing.T) {
	tests := []struct {
		name           string
		processor      *bucketProcessor
		expectedNumber int
	}{
		{
			name: "return 1 when concurrent mode is off",
			processor: &bucketProcessor{
				concurrentMode:    false,
				concurrencyNumber: UnspecifiedConcurrencyNumber,
				targetBuckets:     []string{"bucket1", "bucket2"},
			},
			expectedNumber: 1,
		},
		{
			name: "return number of target buckets when concurrency number is not specified",
			processor: &bucketProcessor{
				concurrentMode:    true,
				concurrencyNumber: UnspecifiedConcurrencyNumber,
				targetBuckets:     []string{"bucket1", "bucket2", "bucket3"},
			},
			expectedNumber: 3,
		},
		{
			name: "return specified concurrency number when set",
			processor: &bucketProcessor{
				concurrentMode:    true,
				concurrencyNumber: 2,
				targetBuckets:     []string{"bucket1", "bucket2", "bucket3"},
			},
			expectedNumber: 2,
		},
		{
			name: "return 1 when concurrent mode is off regardless of concurrency number",
			processor: &bucketProcessor{
				concurrentMode:    false,
				concurrencyNumber: 2,
				targetBuckets:     []string{"bucket1", "bucket2"},
			},
			expectedNumber: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.processor.determineConcurrencyNumber()
			assert.Equal(t, tt.expectedNumber, result)
		})
	}
}
