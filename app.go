package cls3

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-to-k/delstack/client"
	"github.com/urfave/cli/v2"
)

type App struct {
	Cli        *cli.App
	BucketName string
	Profile    string
	Region     string
	ForceMode  bool
}

func NewApp(version string) *App {
	app := App{}

	app.Cli = &cli.App{
		Name:  AppName,
		Usage: "A CLI tool to clear all objects in a S3 Bucket.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "bucketName",
				Aliases:     []string{"b"},
				Usage:       "S3 bucket name",
				Required:    true,
				Destination: &app.BucketName,
			},
			&cli.StringFlag{
				Name:        "profile",
				Aliases:     []string{"p"},
				Usage:       "AWS profile name",
				Destination: &app.Profile,
			},
			&cli.StringFlag{
				Name:        "region",
				Aliases:     []string{"r"},
				Value:       "ap-northeast-1",
				Usage:       "AWS region",
				Destination: &app.Region,
			},
			&cli.BoolFlag{
				Name:        "force",
				Aliases:     []string{"f"},
				Value:       false,
				Usage:       "Delete a bucket together",
				Destination: &app.ForceMode,
			},
		},
	}

	app.Cli.Version = version
	app.Cli.Action = app.getAction()
	app.Cli.HideHelpCommand = true

	return &app
}

func (app *App) Run(ctx context.Context) error {
	return app.Cli.RunContext(ctx, os.Args)
}

func (app *App) getAction() func(c *cli.Context) error {
	return func(c *cli.Context) error {
		config, err := app.loadAwsConfig()
		if err != nil {
			return err
		}

		client := client.NewS3(
			s3.NewFromConfig(config),
		)
		s3Wrapper := NewS3Wrapper(client)

		if err := s3Wrapper.ClearS3Objects(app.BucketName, app.ForceMode); err != nil {
			return err
		}

		return nil
	}
}

func (app *App) loadAwsConfig() (aws.Config, error) {
	var (
		cfg aws.Config
		err error
	)

	if app.Profile != "" {
		cfg, err = config.LoadDefaultConfig(context.TODO(), config.WithRegion(app.Region), config.WithSharedConfigProfile(app.Profile))
	} else {
		cfg, err = config.LoadDefaultConfig(context.TODO(), config.WithRegion(app.Region))
	}

	return cfg, err
}
