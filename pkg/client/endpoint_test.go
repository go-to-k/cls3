package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsCloudflareR2Endpoint(t *testing.T) {
	tests := []struct {
		name        string
		endpointUrl string
		want        bool
	}{
		{
			name:        "Cloudflare R2 storage endpoint",
			endpointUrl: "https://account.r2.cloudflarestorage.com",
			want:        true,
		},
		{
			name:        "AWS S3 endpoint",
			endpointUrl: "https://s3.amazonaws.com",
			want:        false,
		},
		{
			name:        "AWS S3 regional endpoint",
			endpointUrl: "https://s3.us-west-2.amazonaws.com",
			want:        false,
		},
		{
			name:        "LocalStack endpoint",
			endpointUrl: "http://localhost:4566",
			want:        false,
		},
		{
			name:        "MinIO endpoint",
			endpointUrl: "https://minio.example.com",
			want:        false,
		},
		{
			name:        "Empty endpoint",
			endpointUrl: "",
			want:        false,
		},
		{
			name:        "Generic S3-compatible endpoint",
			endpointUrl: "https://s3.custom-provider.com",
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isCloudflareR2Endpoint(tt.endpointUrl)
			assert.Equal(t, tt.want, got, "isCloudflareR2Endpoint(%v) = %v, want %v", tt.endpointUrl, got, tt.want)
		})
	}
}
