package cls3

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

const DefaultAwsRegion = "us-east-1"

func LoadAWSConfig(ctx context.Context, region string, profile string) (aws.Config, error) {
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
