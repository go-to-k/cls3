package app

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/internal/wrapper"
	"github.com/go-to-k/cls3/pkg/endpoint"
	"github.com/urfave/cli/v2"
)

const (
	UnspecifiedConcurrencyNumber = 0
)

type App struct {
	Cli                  *cli.App
	BucketNames          *cli.StringSlice
	Profile              string
	Region               string
	EndpointUrl          string
	ForceMode            bool
	InteractiveMode      bool
	OldVersionsOnly      bool
	QuietMode            bool
	ConcurrentMode       bool
	ConcurrencyNumber    int
	DirectoryBucketsMode bool
	TableBucketsMode     bool
	VectorBucketsMode    bool
	KeyPrefix            string
	targetBuckets        []string // bucket names for S3 and S3Vectors, bucket arns for S3Tables
	bucketSelector       IBucketSelector
	bucketProcessor      IBucketProcessor
	s3Wrapper            wrapper.IWrapper
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
			&cli.StringFlag{
				Name:        "endpointUrl",
				Aliases:     []string{"e"},
				Usage:       "Custom endpoint URL",
				EnvVars:     []string{"CLS3_ENDPOINT_URL"},
				Destination: &app.EndpointUrl,
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
				Usage:       "Delete multiple buckets in parallel. If you want to limit the number of parallel deletions, specify the -n option. This option is not available in the Table Buckets Mode -t because the throttling threshold for S3 Tables is very low.",
				Destination: &app.ConcurrentMode,
			},
			&cli.IntFlag{
				Name:        "concurrencyNumber",
				Aliases:     []string{"n"},
				Value:       UnspecifiedConcurrencyNumber,
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
			&cli.BoolFlag{
				Name:        "vectorBucketsMode",
				Aliases:     []string{"V"},
				Value:       false,
				Usage:       "Clear Vector Buckets for S3 Vectors. If you specify this option WITHOUT -f (--force), it will delete ONLY the indexes without the vector bucket itself.",
				Destination: &app.VectorBucketsMode,
			},
			&cli.StringFlag{
				Name:        "keyPrefix",
				Aliases:     []string{"k"},
				Usage:       "Key prefix of the objects to be deleted.",
				Destination: &app.KeyPrefix,
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

		if err := a.validateOptions(); err != nil {
			return err
		}

		if err := a.initS3Wrapper(c.Context); err != nil {
			return err
		}
		if err := a.initBucketSelector(); err != nil {
			return err
		}

		selectedBuckets, continuation, err := a.bucketSelector.SelectBuckets(c.Context)
		if err != nil {
			return err
		}
		if !continuation {
			return nil
		}
		a.targetBuckets = append(a.targetBuckets, selectedBuckets...)

		if err := a.initBucketProcessor(); err != nil {
			return err
		}
		return a.bucketProcessor.Process(c.Context)
	}
}

func (a *App) initS3Wrapper(ctx context.Context) error {
	if a.s3Wrapper == nil {
		s3Wrapper, err := wrapper.CreateS3Wrapper(ctx, wrapper.CreateS3WrapperInput{
			Region:               a.Region,
			Profile:              a.Profile,
			EndpointUrl:          a.EndpointUrl,
			TableBucketsMode:     a.TableBucketsMode,
			DirectoryBucketsMode: a.DirectoryBucketsMode,
			VectorBucketsMode:    a.VectorBucketsMode,
		})
		if err != nil {
			return err
		}
		a.s3Wrapper = s3Wrapper
	}
	return nil
}

func (a *App) initBucketSelector() error {
	if a.bucketSelector == nil {
		a.bucketSelector = NewBucketSelector(a.InteractiveMode, a.BucketNames, a.s3Wrapper)
	}
	return nil
}

func (a *App) initBucketProcessor() error {
	if a.bucketProcessor == nil {
		processorConfig := BucketProcessorConfig{
			TargetBuckets:     a.targetBuckets,
			QuietMode:         a.QuietMode,
			ConcurrentMode:    a.ConcurrentMode,
			ConcurrencyNumber: a.ConcurrencyNumber,
			ForceMode:         a.ForceMode,
			OldVersionsOnly:   a.OldVersionsOnly,
			Prefix:            aws.String(a.KeyPrefix),
		}
		a.bucketProcessor = NewBucketProcessor(processorConfig, a.s3Wrapper)
	}
	return nil
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
	if a.DirectoryBucketsMode && a.VectorBucketsMode {
		errMsg := fmt.Sprintln("You cannot specify both -d and -V options.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.TableBucketsMode && a.VectorBucketsMode {
		errMsg := fmt.Sprintln("You cannot specify both -t and -V options.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if !endpoint.IsAWSS3Endpoint(a.EndpointUrl) && a.DirectoryBucketsMode {
		errMsg := fmt.Sprintln("Directory Buckets mode (-d) is not supported with non-AWS S3 endpoints.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if !endpoint.IsAWSS3Endpoint(a.EndpointUrl) && a.TableBucketsMode {
		errMsg := fmt.Sprintln("Table Buckets mode (-t) is not supported with non-AWS S3 endpoints.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if !endpoint.IsAWSS3Endpoint(a.EndpointUrl) && a.VectorBucketsMode {
		errMsg := fmt.Sprintln("Vector Buckets mode (-V) is not supported with non-AWS S3 endpoints.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if endpoint.IsCloudflareR2Endpoint(a.EndpointUrl) && a.OldVersionsOnly {
		errMsg := fmt.Sprintln("The -o option is not supported with Cloudflare R2.")
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
	if a.TableBucketsMode && a.ConcurrentMode {
		errMsg := fmt.Sprintln("When specifying -t, do not specify the -c option because the throttling threshold for S3 Tables is very low.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.TableBucketsMode && a.Region == "" {
		io.Logger.Warn().Msg("You are in the Table Buckets Mode `-t` to clear the Table Buckets for S3 Tables. In this mode, operation across regions is not possible, but only in one region. You can specify the region with the `-r` option.")
	}
	if a.VectorBucketsMode && a.OldVersionsOnly {
		errMsg := fmt.Sprintln("When specifying -V, do not specify the -o option.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.VectorBucketsMode && a.Region == "" {
		io.Logger.Warn().Msg("You are in the Vector Buckets Mode `-V` to clear the Vector Buckets for S3 Vectors. In this mode, operation across regions is not possible, but only in one region. You can specify the region with the `-r` option.")
	}
	if !a.ConcurrentMode && a.ConcurrencyNumber != UnspecifiedConcurrencyNumber {
		errMsg := fmt.Sprintln("When specifying -n, you must specify the -c option.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.ConcurrentMode && a.ConcurrencyNumber < UnspecifiedConcurrencyNumber {
		errMsg := fmt.Sprintln("You must specify a positive number for the -n option when specifying the -c option.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.KeyPrefix != "" && a.TableBucketsMode {
		errMsg := fmt.Sprintln("When specifying -t, do not specify the -k option.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.KeyPrefix != "" && a.ForceMode {
		errMsg := fmt.Sprintln("When specifying -k, do not specify the -f option.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.DirectoryBucketsMode && a.KeyPrefix != "" && !strings.HasSuffix(a.KeyPrefix, "/") {
		io.Logger.Warn().Msgf("The key prefix `%s` for the Directory Buckets does not end with a delimiter ( / ). It has been added automatically.", a.KeyPrefix)
		a.KeyPrefix += "/"
	}
	return nil
}
