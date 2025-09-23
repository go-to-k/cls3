package wrapper

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateS3Wrapper(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		input    CreateS3WrapperInput
		wantType string
	}{
		{
			name:     "default mode creates S3Wrapper",
			input:    CreateS3WrapperInput{},
			wantType: "*wrapper.S3Wrapper",
		},
		{
			name: "TableBucketsMode creates S3TablesWrapper",
			input: CreateS3WrapperInput{
				TableBucketsMode: true,
			},
			wantType: "*wrapper.S3TablesWrapper",
		},
		{
			name: "VectorBucketsMode creates S3VectorsWrapper",
			input: CreateS3WrapperInput{
				VectorBucketsMode: true,
			},
			wantType: "*wrapper.S3VectorsWrapper",
		},
		{
			name: "DirectoryBucketsMode creates S3Wrapper",
			input: CreateS3WrapperInput{
				DirectoryBucketsMode: true,
			},
			wantType: "*wrapper.S3Wrapper",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrapper, err := CreateS3Wrapper(ctx, tt.input)

			// Verify no error occurred
			require.NoError(t, err)
			require.NotNil(t, wrapper)

			// Verify the correct wrapper type was created
			wrapperType := fmt.Sprintf("%T", wrapper)
			assert.Equal(t, tt.wantType, wrapperType)
		})
	}
}
