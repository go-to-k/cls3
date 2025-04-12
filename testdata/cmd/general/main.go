package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/semaphore"
)

func main() {
	var (
		profile          string
		bucketPrefix     string
		numBuckets       int
		objectsPerBucket int
		help             bool
	)

	flag.StringVar(&profile, "p", "", "AWS profile")
	flag.StringVar(&bucketPrefix, "b", "cls3-test", "Bucket prefix")
	flag.IntVar(&numBuckets, "n", 10, "Number of buckets")
	flag.IntVar(&objectsPerBucket, "o", 10000, "Number of objects per bucket")
	flag.BoolVar(&help, "h", false, "Show help message")
	flag.Parse()

	if help {
		showHelp()
		return
	}

	if bucketPrefix == "" {
		fmt.Println("bucket_prefix option (-b) is required")
		os.Exit(1)
	}

	if numBuckets <= 0 {
		fmt.Println("number of buckets (-n) must be a positive integer")
		os.Exit(1)
	}

	if objectsPerBucket <= 0 {
		fmt.Println("number of objects (-o) must be a positive integer")
		os.Exit(1)
	}

	// Calculate iterations considering 4 versions (3 copies and 1 deletion) and 100 files
	iterations := objectsPerBucket / (4 * 100)
	if iterations < 1 {
		iterations = 1
	}

	randomSuffix := rand.Intn(65536) // $RANDOM is 0-32767, but we can use a bigger range
	paddedStart := fmt.Sprintf("%04d", 1)
	paddedEnd := fmt.Sprintf("%04d", numBuckets)
	fmt.Printf("=== buckets: %s-%d-[%s-%s] ===\n", bucketPrefix, randomSuffix, paddedStart, paddedEnd)
	fmt.Printf("=== versions: %d per bucket ===\n", objectsPerBucket)

	// Configure AWS SDK
	ctx := context.Background()
	var cfg aws.Config
	var err error
	if profile != "" {
		cfg, err = config.LoadDefaultConfig(ctx, config.WithSharedConfigProfile(profile))
	} else {
		cfg, err = config.LoadDefaultConfig(ctx)
	}
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load AWS config")
	}

	s3Client := s3.NewFromConfig(cfg)

	var wg sync.WaitGroup
	// Limit to 10 concurrent bucket processes
	sem := semaphore.NewWeighted(10)

	for bucketNum := 1; bucketNum <= numBuckets; bucketNum++ {
		wg.Add(1)

		// Acquire semaphore with weight 1
		if err := sem.Acquire(ctx, 1); err != nil {
			log.Error().Err(err).Msg("Failed to acquire semaphore")
			continue
		}

		go func(bucketNum int) {
			defer wg.Done()
			defer sem.Release(1) // Release semaphore when done

			paddedNum := fmt.Sprintf("%04d", bucketNum)
			bucketName := fmt.Sprintf("%s-%d-%s", bucketPrefix, randomSuffix, paddedNum)
			lowerBucketName := bucketName // Buckets are always lowercase in S3

			// Check if bucket exists
			_, err := s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
				Bucket: aws.String(lowerBucketName),
			})
			if err != nil {
				// Bucket doesn't exist, create it
				_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(lowerBucketName),
				})
				if err != nil {
					log.Error().Err(err).Str("bucket", lowerBucketName).Msg("Failed to create bucket")
					return
				}
			}

			// Enable versioning
			_, err = s3Client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
				Bucket: aws.String(lowerBucketName),
				VersioningConfiguration: &types.VersioningConfiguration{
					Status: types.BucketVersioningStatusEnabled,
				},
			})
			if err != nil {
				log.Error().Err(err).Str("bucket", lowerBucketName).Msg("Failed to enable versioning")
				return
			}

			dir := filepath.Join("./testfiles", bucketName)
			err = os.MkdirAll(dir, 0755)
			if err != nil {
				log.Error().Err(err).Str("dir", dir).Msg("Failed to create directory")
				return
			}

			// Generate approximately objectsPerBucket versions
			// NOTE: For 1,000,000 versions per bucket, it'll cost you about $3.75 (0.005 USD / 1000 PUT)(DELETE operation is free)
			// NOTE: 1,000,000 versions = 250,000 objects × 4 versions (3 PUT operations and 1 DELETE operation per object)
			for i := 1; i <= iterations; i++ {
				// Create test files
				for j := 1; j <= 100; j++ {
					fileName := fmt.Sprintf("%d_%d_%d.txt", i, j, rand.Intn(65536))
					filePath := filepath.Join(dir, fileName)
					file, createErr := os.Create(filePath)
					if createErr != nil {
						log.Error().Err(createErr).Str("file", filePath).Msg("Failed to create file")
						continue
					}
					file.Close()
				}

				// Upload files 3 times to create versions
				var versionWg sync.WaitGroup
				for v := 0; v < 3; v++ {
					versionWg.Add(1)
					go func() {
						defer versionWg.Done()

						// Collect all files in directory first
						files := []string{}
						walkErr := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
							if err != nil {
								return err
							}
							if !info.IsDir() {
								files = append(files, path)
							}
							return nil
						})
						if walkErr != nil {
							log.Error().Err(walkErr).Msg("Failed to collect files for upload")
							return
						}

						// Create a worker pool for uploads (max 20 concurrent uploads)
						var uploadWg sync.WaitGroup
						uploadSem := semaphore.NewWeighted(20)

						// Process each file in parallel
						for _, filePath := range files {
							uploadWg.Add(1)

							// Acquire upload semaphore with weight 1
							if semErr := uploadSem.Acquire(ctx, 1); semErr != nil {
								log.Error().Err(semErr).Msg("Failed to acquire upload semaphore")
								uploadWg.Done()
								continue
							}

							go func(path string) {
								defer uploadWg.Done()
								defer uploadSem.Release(1) // Release semaphore when done

								relPath, relErr := filepath.Rel(dir, path)
								if relErr != nil {
									log.Error().Err(relErr).Str("file", path).Msg("Failed to get relative path")
									return
								}

								file, relErr := os.Open(path)
								if relErr != nil {
									log.Error().Err(relErr).Str("file", path).Msg("Failed to open file")
									return
								}
								defer file.Close()

								_, relErr = s3Client.PutObject(ctx, &s3.PutObjectInput{
									Bucket: aws.String(lowerBucketName),
									Key:    aws.String(relPath),
									Body:   file,
								})
								if relErr != nil {
									log.Error().Err(relErr).Str("file", path).Msg("Failed to upload file")
								}
							}(filePath)
						}

						// Wait for all uploads to complete
						uploadWg.Wait()
					}()
				}
				versionWg.Wait()

				// Delete all objects (create delete markers)
				files := []string{}
				err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if !info.IsDir() {
						files = append(files, path)
					}
					return nil
				})
				if err != nil {
					log.Error().Err(err).Msg("Failed to collect files for deletion")
				} else {
					// Create a worker pool for deletions (max 20 concurrent deletions)
					var deleteWg sync.WaitGroup
					deleteSem := semaphore.NewWeighted(20)

					// Process each deletion in parallel
					for _, filePath := range files {
						deleteWg.Add(1)

						// Acquire delete semaphore with weight 1
						if err := deleteSem.Acquire(ctx, 1); err != nil {
							log.Error().Err(err).Msg("Failed to acquire delete semaphore")
							deleteWg.Done()
							continue
						}

						go func(path string) {
							defer deleteWg.Done()
							defer deleteSem.Release(1) // Release semaphore when done

							relPath, err := filepath.Rel(dir, path)
							if err != nil {
								log.Error().Err(err).Str("file", path).Msg("Failed to get relative path for deletion")
								return
							}

							_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
								Bucket: aws.String(lowerBucketName),
								Key:    aws.String(relPath),
							})
							if err != nil {
								log.Error().Err(err).Str("file", relPath).Msg("Failed to delete object")
							}
						}(filePath)
					}

					// Wait for all deletions to complete
					deleteWg.Wait()
				}

				// Clean up local files
				files, err := filepath.Glob(filepath.Join(dir, "*.txt"))
				if err != nil {
					log.Error().Err(err).Msg("Failed to glob files")
				} else {
					for _, file := range files {
						os.Remove(file)
					}
				}
			}

			// Clean up the directory when done
			os.RemoveAll(dir)
		}(bucketNum)
	}

	wg.Wait()
}

func showHelp() {
	fmt.Println("Standard S3 Bucket Test Data Generator")
	fmt.Println("======================================")
	fmt.Println("This tool creates multiple standard S3 buckets with versioning enabled and populates them with objects.")
	fmt.Println("")
	fmt.Println("Options:")
	fmt.Println("  -p string  AWS profile name (optional)")
	fmt.Println("  -b string  Bucket name prefix (required)")
	fmt.Println("  -n int     Number of buckets to create (default: 10)")
	fmt.Println("  -o int     Number of objects per bucket (default: 10000)")
	fmt.Println("  -h         Show this help message")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("  Create 5 buckets with 1000 objects each:")
	fmt.Println("    go run testdata/cmd/general/main.go -b my-bucket -n 5 -o 1000")
	fmt.Println("")
	fmt.Println("  Using a specific AWS profile:")
	fmt.Println("    go run testdata/cmd/general/main.go -p my-profile -b my-bucket")
	fmt.Println("")
	fmt.Println("  Or using Make:")
	fmt.Println("    make testgen_general OPT=\"-b my-bucket -n 5 -o 1000\"")
}
