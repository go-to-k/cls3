package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadAWSConfig(t *testing.T) {
	ctx := context.Background()

	// Disable AWS config file to ensure predictable behavior
	t.Setenv("AWS_CONFIG_FILE", "/tmp/nonexistent-aws-config")

	tests := []struct {
		name        string
		region      string
		endpointUrl string
		wantRegion  string
	}{
		{
			name:        "region specified - overrides default",
			region:      "us-west-2",
			endpointUrl: "",
			wantRegion:  "us-west-2",
		},
		{
			name:        "no region specified - uses DefaultAwsRegion",
			region:      "",
			endpointUrl: "",
			wantRegion:  DefaultAwsRegion,
		},
		{
			name:        "region with endpoint URL",
			region:      "ap-southeast-1",
			endpointUrl: "https://custom.endpoint.com",
			wantRegion:  "ap-southeast-1",
		},
		{
			name:        "endpoint URL with localhost",
			region:      "eu-central-1",
			endpointUrl: "http://localhost:4566",
			wantRegion:  "eu-central-1",
		},
		{
			name:        "endpoint URL only - uses DefaultAwsRegion",
			region:      "",
			endpointUrl: "https://s3.custom.endpoint.com",
			wantRegion:  DefaultAwsRegion,
		},
		{
			name:        "Cloudflare R2 endpoint with no region - uses auto",
			region:      "",
			endpointUrl: "https://account.r2.cloudflarestorage.com",
			wantRegion:  "auto",
		},
		{
			name:        "Cloudflare R2 endpoint with region - uses specified region",
			region:      "region",
			endpointUrl: "https://account.r2.cloudflarestorage.com",
			wantRegion:  "region",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadAWSConfig(ctx, tt.region, "", tt.endpointUrl)

			// Verify no error occurred
			require.NoError(t, err)

			// Verify region is set correctly
			assert.Equal(t, tt.wantRegion, cfg.Region)

			// Verify endpoint URL is set when provided
			if tt.endpointUrl != "" {
				assert.NotNil(t, cfg.BaseEndpoint)
				assert.Equal(t, tt.endpointUrl, *cfg.BaseEndpoint)
			}
		})
	}
}
