package endpoint

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
			got := IsCloudflareR2Endpoint(tt.endpointUrl)
			assert.Equal(t, tt.want, got, "IsCloudflareR2Endpoint(%v) = %v, want %v", tt.endpointUrl, got, tt.want)
		})
	}
}

func TestIsAWSS3Endpoint(t *testing.T) {
	tests := []struct {
		name        string
		endpointUrl string
		want        bool
	}{
		{
			name:        "AWS S3 global endpoint",
			endpointUrl: "https://s3.amazonaws.com",
			want:        true,
		},
		{
			name:        "AWS S3 regional endpoint",
			endpointUrl: "https://s3.us-west-2.amazonaws.com",
			want:        true,
		},
		{
			name:        "AWS S3 legacy regional endpoint",
			endpointUrl: "https://s3-us-west-2.amazonaws.com",
			want:        true,
		},
		{
			name:        "AWS S3 virtual hosted-style endpoint",
			endpointUrl: "https://bucket.s3.amazonaws.com",
			want:        true,
		},
		{
			name:        "AWS S3 virtual hosted-style regional endpoint",
			endpointUrl: "https://bucket.s3.us-west-2.amazonaws.com",
			want:        true,
		},
		{
			name:        "AWS S3 with HTTPS and path",
			endpointUrl: "https://s3.eu-central-1.amazonaws.com/bucket/key",
			want:        true,
		},
		{
			name:        "Cloudflare R2 storage endpoint",
			endpointUrl: "https://account.r2.cloudflarestorage.com",
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
			name:        "Generic S3-compatible endpoint",
			endpointUrl: "https://s3.custom-provider.com",
			want:        false,
		},
		{
			name:        "Empty endpoint",
			endpointUrl: "",
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsAWSS3Endpoint(tt.endpointUrl)
			assert.Equal(t, tt.want, got, "IsAWSS3Endpoint(%v) = %v, want %v", tt.endpointUrl, got, tt.want)
		})
	}
}
