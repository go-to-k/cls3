package client

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

const SDKRetryMaxAttempts = 3

// TODO: change to us-east-1 (and README and blogs)
const DefaultAwsRegion = "ap-northeast-1"

func loadAWSConfig(ctx context.Context, region string, profile string) (aws.Config, error) {
	var (
		cfg aws.Config
		err error
	)

	if profile != "" {
		cfg, err = config.LoadDefaultConfig(ctx, config.WithSharedConfigProfile(profile))
	} else {
		cfg, err = config.LoadDefaultConfig(ctx)
	}

	if err != nil {
		return cfg, err
	}

	if region != "" {
		cfg.Region = region
	}
	if cfg.Region == "" {
		cfg.Region = DefaultAwsRegion
	}

	return cfg, nil
}
