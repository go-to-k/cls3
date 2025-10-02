package client

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/go-to-k/cls3/pkg/endpoint"
)

const DefaultAwsRegion = "us-east-1"

func LoadAWSConfig(ctx context.Context, region string, profile string, endpointUrl string) (aws.Config, error) {
	var (
		options []func(*config.LoadOptions) error
		cfg     aws.Config
		err     error
	)

	if profile != "" {
		options = append(options, config.WithSharedConfigProfile(profile))
	}

	if endpointUrl != "" {
		options = append(options, config.WithBaseEndpoint(endpointUrl))
	}

	cfg, err = config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return cfg, err
	}

	if region != "" {
		cfg.Region = region
	}
	if cfg.Region == "" {
		cfg.Region = defineDefaultRegion(endpointUrl)
	}

	return cfg, nil
}

func defineDefaultRegion(endpointUrl string) string {
	if endpoint.IsAWSS3Endpoint(endpointUrl) {
		return DefaultAwsRegion
	}

	if endpoint.IsCloudflareR2Endpoint(endpointUrl) {
		return "auto"
	}
	return DefaultAwsRegion
}
