package app

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/internal/wrapper"
	"github.com/go-to-k/cls3/pkg/client"
	"github.com/urfave/cli/v2"
)

const SDKRetryMaxAttempts = 3

type App struct {
	Cli                  *cli.App
	BucketNames          *cli.StringSlice
	Profile              string
	Region               string
	ForceMode            bool
	InteractiveMode      bool
	OldVersionsOnly      bool
	QuietMode            bool
	DirectoryBucketsMode bool
}

func NewApp(version string) *App {
	app := App{}

	app.BucketNames = cli.NewStringSlice()

	app.Cli = &cli.App{
		Name:  "cls3",
		Usage: "A CLI tool to clear all objects in S3 Buckets or delete Buckets.",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:        "bucketName",
				Aliases:     []string{"b"},
				Usage:       "S3 bucket names(one or more)",
				Destination: app.BucketNames,
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
			&cli.BoolFlag{
				Name:        "interactive",
				Aliases:     []string{"i"},
				Value:       false,
				Usage:       "Interactive Mode",
				Destination: &app.InteractiveMode,
			},
			&cli.BoolFlag{
				Name:        "oldVersionsOnly",
				Aliases:     []string{"o"},
				Value:       false,
				Usage:       "Delete old version objects only (including all delete-markers)",
				Destination: &app.OldVersionsOnly,
			},
			&cli.BoolFlag{
				Name:        "quietMode",
				Aliases:     []string{"q"},
				Value:       false,
				Usage:       "Hide live display of number of deletions",
				Destination: &app.QuietMode,
			},
			&cli.BoolFlag{
				Name:        "directoryBucketsMode",
				Aliases:     []string{"d"},
				Value:       false,
				Usage:       "Clear Directory Buckets for S3 Express One Zone",
				Destination: &app.DirectoryBucketsMode,
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
		io.Logger.Debug().Msg("Debug mode...")

		err := a.validateOptions()
		if err != nil {
			return err
		}

		s3Wrapper, err := a.createS3Wrapper(c.Context)
		if err != nil {
			return err
		}

		if a.InteractiveMode {
			continuation, err := a.doInteractiveMode(c.Context, s3Wrapper)
			if err != nil {
				return err
			}
			if !continuation {
				return nil
			}
		} else {
			err := s3Wrapper.CheckAllBucketsExist(c.Context, a.BucketNames.Value())
			if err != nil {
				return err
			}
		}

		for _, bucketName := range a.BucketNames.Value() {
			if err := s3Wrapper.ClearS3Objects(c.Context, bucketName, a.ForceMode, a.OldVersionsOnly, a.QuietMode); err != nil {
				return err
			}
		}

		return nil
	}
}

func (a *App) createS3Wrapper(ctx context.Context) (*wrapper.S3Wrapper, error) {
	config, err := client.LoadAWSConfig(ctx, a.Region, a.Profile)
	if err != nil {
		return nil, err
	}

	client := client.NewS3(
		s3.NewFromConfig(config, func(o *s3.Options) {
			o.RetryMaxAttempts = SDKRetryMaxAttempts
			o.RetryMode = aws.RetryModeStandard
		}),
		a.DirectoryBucketsMode,
	)
	return wrapper.NewS3Wrapper(client), nil
}

func (a *App) validateOptions() error {
	if !a.InteractiveMode && len(a.BucketNames.Value()) == 0 {
		errMsg := fmt.Sprintln("At least one bucket name must be specified in command options (-b) or a flow of the interactive mode (-i).")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.InteractiveMode && len(a.BucketNames.Value()) != 0 {
		errMsg := fmt.Sprintln("When specifying -i, do not specify the -b option.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.ForceMode && a.OldVersionsOnly {
		errMsg := fmt.Sprintln("When specifying -o, do not specify the -f option.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.DirectoryBucketsMode && a.OldVersionsOnly {
		errMsg := fmt.Sprintln("When specifying -d, do not specify the -o option.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.DirectoryBucketsMode && a.Region == "" {
		io.Logger.Warn().Msg("You are in the Directory Buckets Mode `-d` to clear the Directory Buckets. In this mode, operation across regions is not possible, but only in one region. You can specify the region with the `-r` option.")
	}
	return nil
}

func (a *App) doInteractiveMode(ctx context.Context, s3Wrapper *wrapper.S3Wrapper) (bool, error) {
	keyword := io.InputKeywordForFilter("Filter a keyword of bucket names: ")
	bucketNames, err := s3Wrapper.ListBucketNamesFilteredByKeyword(ctx, aws.String(keyword))
	if err != nil {
		return false, err
	}

	label := []string{"Select buckets."}
	checkboxes, continuation, err := io.GetCheckboxes(label, bucketNames)
	if err != nil {
		return false, err
	}
	if !continuation {
		return false, nil
	}

	for _, bucket := range checkboxes {
		//nolint:errcheck
		a.BucketNames.Set(bucket)
	}
	return true, nil
}
