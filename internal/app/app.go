package app

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/internal/wrapper"
	"github.com/go-to-k/cls3/pkg/client"
	"github.com/gosuri/uilive"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	UnspecifiedConcurrencyNumber = 0
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

		// TODO: Refactor to separate the display and the deletion process.
		writer := uilive.New()
		writer.Start()
		defer writer.Stop()

		clearingLines := make([]string, len(a.targetBuckets))
		var clearingLinesMutex sync.Mutex
		var clearingCountsMutex sync.Mutex

		clearingCountChannels := make(map[string]chan int64, len(a.targetBuckets))
		clearedCompletedChannels := make(map[string]chan bool, len(a.targetBuckets))
		clearingCounts := make(map[string]*atomic.Int64, len(a.targetBuckets))

		for _, bucket := range a.targetBuckets {
			if err := s3Wrapper.OutputCheckingMessage(bucket); err != nil {
				return err
			}
			clearingCountChannels[bucket] = make(chan int64)
			clearedCompletedChannels[bucket] = make(chan bool)
			clearingCounts[bucket] = &atomic.Int64{}
		}

		var displayEg errgroup.Group
		if !a.QuietMode {
			for i, bucket := range a.targetBuckets {
				i, bucket := i, bucket

				// Necessary to first display all bucket rows together
				clearingLinesMutex.Lock()
				message, err := s3Wrapper.GetLiveClearingMessage(bucket, 0)
				if err != nil {
					return err
				}
				clearingLines[i] = message
				clearingLinesMutex.Unlock()

				displayEg.Go(func() error {
					clearingCountsMutex.Lock()
					clearingCountCh := clearingCountChannels[bucket]
					clearedCompletedCh := clearedCompletedChannels[bucket]
					counter := clearingCounts[bucket]
					clearingCountsMutex.Unlock()

					for count := range clearingCountCh {
						counter.Store(count)
						clearingLinesMutex.Lock()
						message, err := s3Wrapper.GetLiveClearingMessage(bucket, count)
						if err != nil {
							return err
						}
						clearingLines[i] = message
						fmt.Fprintln(writer, strings.Join(clearingLines, "\n"))
						clearingLinesMutex.Unlock()
					}

					count := counter.Load()
					clearingLinesMutex.Lock()
					isCompleted := <-clearedCompletedCh
					message, err := s3Wrapper.GetLiveClearedMessage(bucket, count, isCompleted)
					if err != nil {
						return err
					}
					clearingLines[i] = message
					fmt.Fprintln(writer, strings.Join(clearingLines, "\n"))
					clearingLinesMutex.Unlock()
					return nil
				})
			}
		}

		concurrencyNumber := a.determineConcurrencyNumber()
		sem := semaphore.NewWeighted(int64(concurrencyNumber))
		clearEg := errgroup.Group{}

		for _, bucket := range a.targetBuckets {
			bucket := bucket
			if err := sem.Acquire(c.Context, 1); err != nil {
				return err
			}

			clearEg.Go(func() error {
				defer sem.Release(1)
				clearingCountsMutex.Lock()
				clearingCountCh := clearingCountChannels[bucket]
				clearedCompletedCh := clearedCompletedChannels[bucket]
				clearingCountsMutex.Unlock()

				err := s3Wrapper.ClearBucket(c.Context, wrapper.ClearBucketInput{
					TargetBucket:    bucket,
					ForceMode:       a.ForceMode,
					OldVersionsOnly: a.OldVersionsOnly,
					QuietMode:       a.QuietMode,
					ClearingCountCh: clearingCountCh,
				})
				if err != nil {
					close(clearingCountCh)
					if !a.QuietMode {
						clearedCompletedCh <- false
					}
					close(clearedCompletedCh)
					return err
				}

				close(clearingCountCh)
				if !a.QuietMode {
					clearedCompletedCh <- true
				}
				close(clearedCompletedCh)

				return nil
			})
		}

		if err := clearEg.Wait(); err != nil {
			return err
		}

		if err := displayEg.Wait(); err != nil {
			return err
		}
		if !a.QuietMode {
			if err := writer.Flush(); err != nil {
				return err
			}

			for _, bucket := range a.targetBuckets {
				if err := s3Wrapper.OutputClearedMessage(bucket, clearingCounts[bucket].Load()); err != nil {
					return err
				}
				if a.ForceMode {
					if err := s3Wrapper.OutputDeletedMessage(bucket); err != nil {
						return err
					}
				}
			}
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
	if a.TableBucketsMode && a.ConcurrentMode {
		errMsg := fmt.Sprintln("When specifying -t, do not specify the -c option because the throttling threshold for S3 Tables is very low.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.TableBucketsMode && a.Region == "" {
		io.Logger.Warn().Msg("You are in the Table Buckets Mode `-t` to clear the Table Buckets for S3 Tables. In this mode, operation across regions is not possible, but only in one region. You can specify the region with the `-r` option.")
	}
	if !a.ConcurrentMode && a.ConcurrencyNumber != UnspecifiedConcurrencyNumber {
		errMsg := fmt.Sprintln("When specifying -n, you must specify the -c option.")
		return fmt.Errorf("InvalidOptionError: %v", errMsg)
	}
	if a.ConcurrentMode && a.ConcurrencyNumber < UnspecifiedConcurrencyNumber {
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

	// Cases where ConcurrencyNumber is unspecified.
	if a.ConcurrencyNumber == UnspecifiedConcurrencyNumber {
		return len(a.targetBuckets)
	}

	return a.ConcurrencyNumber
}
