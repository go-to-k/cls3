package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-to-k/cls3/testdata/pkg/retryer"
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

	randomSuffix := rand.Intn(65536)
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

	// Create S3 client and apply retry configuration
	s3Client := s3.NewFromConfig(cfg)
	retryer := retryer.CreateS3Retryer()

	var wg sync.WaitGroup
	// Limit to 10 concurrent bucket processes
	sem := semaphore.NewWeighted(10)

	for bucketNum := 1; bucketNum <= numBuckets; bucketNum++ {
		wg.Add(1)

		// Acquire semaphore with weight 1
		if err := sem.Acquire(ctx, 1); err != nil {
			log.Error().Err(err).Msg("Failed to acquire semaphore")
			wg.Done()
			continue
		}

		go func(bucketNum int) {
			defer wg.Done()
			defer sem.Release(1) // Release semaphore when done

			paddedNum := fmt.Sprintf("%04d", bucketNum)
			bucketName := fmt.Sprintf("%s-%d-%s", bucketPrefix, randomSuffix, paddedNum)
			lowerBucketName := strings.ToLower(bucketName)

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

			// Generate approximately objectsPerBucket versions
			// NOTE: For 1,000,000 versions per bucket, it'll cost you about $3.75 (0.005 USD / 1000 PUT)(DELETE operation is free)
			// NOTE: 1,000,000 versions = 250,000 objects × 4 versions (3 PUT operations and 1 DELETE operation per object)
			for i := 1; i <= iterations; i++ {
				// Calculate how many objects to create in this iteration
				numObjects := 100
				if i == iterations && objectsPerBucket%(4*100) != 0 {
					// Handle the remainder when the version count doesn't divide evenly
					// Example: 1000 versions ÷ 4 = 250 objects
					// Processing 400 (100 objects × 4) at a time,
					// in the last iteration: 1000 % 400 = 200 versions = 50 objects
					numObjects = (objectsPerBucket % (4 * 100)) / 4

					// Verify that the total object count matches the expected count
					totalObjects := (iterations-1)*100 + numObjects
					expectedObjects := objectsPerBucket / 4
					if totalObjects != expectedObjects {
						// Apply correction here (to handle integer division truncation)
						remainder := expectedObjects - totalObjects
						if remainder > 0 {
							numObjects += remainder
						}
					}
				}

				// Upload and delete files in batches directly
				var versionWg sync.WaitGroup

				// Generate stable object keys for this iteration
				objectKeys := make([]string, numObjects)
				for j := range numObjects {
					// Create stable object keys that will be used for all versions
					objectKeys[j] = fmt.Sprintf("%d/%d/%d.txt", i, j+1, rand.Intn(65536))
				}

				// Create 3 versions per object
				for range 3 {
					versionWg.Add(1)
					go func() {
						defer versionWg.Done()

						// Create a worker pool for uploads (max 20 concurrent uploads)
						var uploadWg sync.WaitGroup
						uploadSem := semaphore.NewWeighted(20)

						// Process each upload in parallel
						for j := range numObjects {
							uploadWg.Add(1)

							// Acquire upload semaphore with weight 1
							if semErr := uploadSem.Acquire(ctx, 1); semErr != nil {
								log.Error().Err(semErr).Msg("Failed to acquire upload semaphore")
								uploadWg.Done()
								continue
							}

							go func(objectIndex int) {
								defer uploadWg.Done()
								defer uploadSem.Release(1) // Release semaphore when done

								// Use the stable object key
								objectKey := objectKeys[objectIndex]

								// Execute PutObject with retry configuration
								optFn := func(o *s3.Options) {
									o.Retryer = retryer
								}

								_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
									Bucket: aws.String(lowerBucketName),
									Key:    aws.String(objectKey),
									Body:   strings.NewReader(""),
								}, optFn)
								if err != nil {
									log.Error().Err(err).Str("object", objectKey).Msg("Failed to upload object")
								}
							}(j)
						}

						// Wait for all uploads to complete
						uploadWg.Wait()
					}()
				}
				versionWg.Wait()

				// Delete objects to create delete markers
				var deleteWg sync.WaitGroup
				deleteSem := semaphore.NewWeighted(20)

				// Process each deletion in parallel
				for j := range numObjects {
					deleteWg.Add(1)

					// Acquire delete semaphore with weight 1
					if semErr := deleteSem.Acquire(ctx, 1); semErr != nil {
						log.Error().Err(semErr).Msg("Failed to acquire delete semaphore")
						deleteWg.Done()
						continue
					}

					go func(objectIndex int) {
						defer deleteWg.Done()
						defer deleteSem.Release(1) // Release semaphore when done

						// Use the same stable object key
						objectKey := objectKeys[objectIndex]

						// Execute DeleteObject with retry configuration
						optFn := func(o *s3.Options) {
							o.Retryer = retryer
						}

						_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
							Bucket: aws.String(lowerBucketName),
							Key:    aws.String(objectKey),
						}, optFn)
						if err != nil {
							log.Error().Err(err).Str("object", objectKey).Msg("Failed to delete object")
						}
					}(j)
				}

				// Wait for all deletions to complete
				deleteWg.Wait()
			}
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
	fmt.Println("    go run testdata/cmd/general/main.go -n 5 -o 1000")
	fmt.Println("")
	fmt.Println("  Using a specific AWS profile:")
	fmt.Println("    go run testdata/cmd/general/main.go -p my-profile")
	fmt.Println("")
	fmt.Println("  Or using Make:")
	fmt.Println("    make testgen_general OPT=\"-n 5 -o 1000\"")
}
