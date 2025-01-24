package app

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/internal/wrapper"
	"github.com/go-to-k/cls3/pkg/client"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	ForbiddenConcurrencyNumber = -999
)

type App struct {
	Cli                  *cli.App
	BucketNames          *cli.StringSlice
	Profile              string
	Region               string
	ForceMode            bool
	InteractiveMode      bool
	OldVersionsOnly      bool
	QuietMode            bool
	ConcurrentMode       bool
	ConcurrencyNumber    int
	DirectoryBucketsMode bool
	TableBucketsMode     bool
	targetBuckets        []string // bucket names for S3, bucket arns for S3Tables
}

func NewApp(version string) *App {
	app := App{}

	app.BucketNames = cli.NewStringSlice()
	app.targetBuckets = []string{}

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
				Usage:       "Delete a bucket together. If you specify this option with -t (--tableBucketsMode), it will delete not only the namespaces and the tables but also the table bucket itself.",
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
				Name:        "concurrentMode",
				Aliases:     []string{"c"},
				Value:       false,
				Usage:       "Delete multiple buckets in parallel. If you want to limit the number of parallel deletions, specify the -n option.",
				Destination: &app.ConcurrentMode,
			},
			&cli.IntFlag{
				Name:        "concurrencyNumber",
				Aliases:     []string{"n"},
				Value:       ForbiddenConcurrencyNumber,
				Usage:       "Specify the number of parallel deletions. To specify this option, the -c option must be specified. The default is to delete all buckets in parallel if only the -c option is specified.",
				Destination: &app.ConcurrencyNumber,
			},
			&cli.BoolFlag{
				Name:        "directoryBucketsMode",
				Aliases:     []string{"d"},
				Value:       false,
				Usage:       "Clear Directory Buckets for S3 Express One Zone",
				Destination: &app.DirectoryBucketsMode,
			},
			&cli.BoolFlag{
				Name:        "tableBucketsMode",
				Aliases:     []string{"t"},
				Value:       false,
				Usage:       "Clear Table Buckets for S3 Tables. If you specify this option WITHOUT -f (--force), it will delete ONLY the namespaces and the tables without the table bucket itself.",
				Destination: &app.TableBucketsMode,
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
			outputBuckets, err := s3Wrapper.CheckAllBucketsExist(c.Context, a.BucketNames.Value())
			if err != nil {
				return err
			}
			a.targetBuckets = append(a.targetBuckets, outputBuckets...)
		}

		concurrencyNumber := a.determineConcurrencyNumber()

		sem := semaphore.NewWeighted(int64(concurrencyNumber))
		eg := errgroup.Group{}
		// FIXME: handle messages
		// FIXME: error occurs
		// INF par-cls-020 Checking...
		// INF par-cls-016 Checking...
		// INF par-cls-022 Checking...
		// INF par-cls-025 Checking...
		// INF par-cls-018 Checking...
		// INF par-cls-021 Checking...
		// INF par-cls-019 Checking...
		// INF par-cls-024 Checking...
		// INF par-cls-023 Checking...
		// INF par-cls-017 Checking...
		// INF par-cls-016 Cleared!!: 13000 objects.
		// INF par-cls-016 Deleted!!
		// INF par-cls-024 Cleared!!: 32000 objects.
		// INF par-cls-025 Cleared!!: 32000 objects.
		// INF par-cls-020 Cleared!!: 32000 objects.
		// INF par-cls-023 Cleared!!: 32000 objects.
		// INF par-cls-017 Cleared!!: 32000 objects.
		// INF par-cls-024 Deleted!!
		// INF par-cls-025 Deleted!!
		// INF par-cls-020 Deleted!!
		// INF par-cls-023 Deleted!!
		// INF par-cls-017 Deleted!!
		// INF par-cls-018 Cleared!!: 32000 objects.
		// INF par-cls-018 Deleted!!
		// INF par-cls-022 Cleared!!: 32000 objects.
		// INF par-cls-022 Deleted!!
		// ERR [resource par-cls-019] operation error S3: DeleteObjects, https response error StatusCode: 0, RequestID: , HostID: , request send failed, Post "https://par-cls-019.s3.us-east-1.amazonaws.com/?delete=": EOF
		// exit status 1
		for _, bucket := range a.targetBuckets {
			bucket := bucket
			if err := sem.Acquire(c.Context, 1); err != nil {
				return err
			}

			eg.Go(func() error {
				defer sem.Release(1)
				return s3Wrapper.ClearBucket(c.Context, wrapper.ClearBucketInput{
					TargetBucket:    bucket,
					ForceMode:       a.ForceMode,
					OldVersionsOnly: a.OldVersionsOnly,
					QuietMode:       a.QuietMode,
				})
			})
		}

		if err := eg.Wait(); err != nil {
			return err
		}

		return nil
	}
}

func (a *App) createS3Wrapper(ctx context.Context) (wrapper.IWrapper, error) {
	config, err := client.LoadAWSConfig(ctx, a.Region, a.Profile)
	if err != nil {
		return nil, err
	}

	return wrapper.CreateS3Wrapper(config, a.TableBucketsMode, a.DirectoryBucketsMode), nil
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
	if a.DirectoryBucketsMode && a.TableBucketsMode {
		errMsg := fmt.Sprintln("You cannot specify both -d and -t options.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.DirectoryBucketsMode && a.OldVersionsOnly {
		errMsg := fmt.Sprintln("When specifying -d, do not specify the -o option.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.DirectoryBucketsMode && a.Region == "" {
		io.Logger.Warn().Msg("You are in the Directory Buckets Mode `-d` to clear the Directory Buckets. In this mode, operation across regions is not possible, but only in one region. You can specify the region with the `-r` option.")
	}
	if a.TableBucketsMode && a.OldVersionsOnly {
		errMsg := fmt.Sprintln("When specifying -t, do not specify the -o option.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.TableBucketsMode && a.Region == "" {
		io.Logger.Warn().Msg("You are in the Table Buckets Mode `-t` to clear the Table Buckets for S3 Tables. In this mode, operation across regions is not possible, but only in one region. You can specify the region with the `-r` option.")
	}
	if !a.ConcurrentMode && a.ConcurrencyNumber != ForbiddenConcurrencyNumber {
		errMsg := fmt.Sprintln("When specifying -n, you must specify the -c option.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.ConcurrentMode && a.ConcurrencyNumber != ForbiddenConcurrencyNumber && a.ConcurrencyNumber < 1 {
		errMsg := fmt.Sprintln("You must specify a positive number for the -n option when specifying the -c option.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	return nil
}

func (a *App) doInteractiveMode(ctx context.Context, s3Wrapper wrapper.IWrapper) (bool, error) {
	keyword := io.InputKeywordForFilter("Filter a keyword of bucket names: ")
	outputs, err := s3Wrapper.ListBucketNamesFilteredByKeyword(ctx, aws.String(keyword))
	if err != nil {
		return false, err
	}

	bucketNames := []string{}
	for _, output := range outputs {
		bucketNames = append(bucketNames, output.BucketName)
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
		for _, output := range outputs {
			if output.BucketName == bucket {
				a.targetBuckets = append(a.targetBuckets, output.TargetBucket)
			}
		}
	}
	return true, nil
}

func (a *App) determineConcurrencyNumber() int {
	// Series when ConcurrentMode is off.
	if !a.ConcurrentMode {
		return 1
	}

	// No real-time deletion counts
	a.QuietMode = true

	// Cases where ConcurrencyNumber is unspecified.
	if a.ConcurrencyNumber == ForbiddenConcurrencyNumber {
		return len(a.targetBuckets)
	}

	return a.ConcurrencyNumber
}
