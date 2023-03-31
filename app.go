package cls3

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/urfave/cli/v2"
)

const SDKRetryMaxAttempts = 3

type App struct {
	Cli             *cli.App
	BucketNames     *cli.StringSlice
	Profile         string
	Region          string
	ForceMode       bool
	InteractiveMode bool
}

func NewApp(version string) *App {
	app := App{}

	app.BucketNames = cli.NewStringSlice()

	app.Cli = &cli.App{
		Name:  "cls3",
		Usage: "A CLI tool to clear all objects in a S3 Bucket.",
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
		if !a.InteractiveMode && len(a.BucketNames.Value()) == 0 {
			errMsg := fmt.Sprintln("At least one bucket name must be specified in command options (-b) or a flow of the interactive mode (-i).")
			return fmt.Errorf("InvalidOptionError: %v", errMsg)
		}
		if a.InteractiveMode && len(a.BucketNames.Value()) != 0 {
			errMsg := fmt.Sprintln("When specifying -i, do not specify the -b option.")
			return fmt.Errorf("InvalidOptionError: %v", errMsg)
		}

		config, err := LoadAWSConfig(c.Context, a.Region, a.Profile)
		if err != nil {
			return err
		}

		client := NewS3(
			s3.NewFromConfig(config, func(o *s3.Options) {
				o.RetryMaxAttempts = SDKRetryMaxAttempts
				o.RetryMode = aws.RetryModeStandard
			}),
		)
		s3Wrapper := NewS3Wrapper(client)

		if a.InteractiveMode {
			buckets, continuation, err := a.doInteractiveMode(c.Context, s3Wrapper)
			if err != nil {
				return err
			}
			if !continuation {
				return nil
			}

			for _, bucket := range buckets {
				a.BucketNames.Set(bucket)
			}
		}

		for _, bucket := range a.BucketNames.Value() {
			if err := s3Wrapper.ClearS3Objects(c.Context, bucket, a.ForceMode); err != nil {
				return err
			}
		}

		return nil
	}
}

func (a *App) doInteractiveMode(ctx context.Context, s3Wrapper *S3Wrapper) ([]string, bool, error) {
	var checkboxes []string
	var keyword string

	BucketNameLabel := "Filter a keyword of bucket names(case-insensitive): "
	keyword = InputKeywordForFilter(BucketNameLabel)

	label := "Select buckets." + "\n"
	bucketNames, err := s3Wrapper.ListBucketNamesFilteredByKeyword(ctx, aws.String(keyword))
	if err != nil {
		return checkboxes, false, err
	}
	if len(bucketNames) == 0 {
		errMsg := fmt.Sprintf("No buckets matching the keyword %s.", keyword)
		return checkboxes, false, fmt.Errorf("NotExistsError: %v", errMsg)
	}

	for {
		checkboxes = GetCheckboxes(label, bucketNames)

		if len(checkboxes) == 0 {
			// The case for interruption(Ctrl + C)
			ok := GetYesNo("Do you want to finish?")
			if ok {
				Logger.Info().Msg("Finished...")
				return checkboxes, false, nil
			}
			continue
		}

		ok := GetYesNo("OK?")
		if ok {
			return checkboxes, true, nil
		}
	}
}
