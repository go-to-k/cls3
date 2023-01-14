package cls3

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-to-k/delstack/pkg/client"
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
		Name:  "cls3",
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

func (a *App) Run(ctx context.Context) error {
	return a.Cli.RunContext(ctx, os.Args)
}

func (a *App) getAction() func(c *cli.Context) error {
	return func(c *cli.Context) error {
		config, err := LoadAWSConfig(c.Context, a.Region, a.Profile)
		if err != nil {
			return err
		}

		client := client.NewS3(
			s3.NewFromConfig(config),
		)
		s3Wrapper := NewS3Wrapper(client)

		if err := s3Wrapper.ClearS3Objects(c.Context, a.BucketName, a.ForceMode); err != nil {
			return err
		}

		return nil
	}
}
