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

	if numBuckets > 100 {
		fmt.Println("number of buckets (-n) must be less than or equal to 100 for directory buckets")
		os.Exit(1)
	}

	if objectsPerBucket <= 0 {
		fmt.Println("number of objects (-o) must be a positive integer")
		os.Exit(1)
	}

	region := "us-east-1"
	azID := "use1-az4"

	// Calculate iterations considering 1000 files per iteration
	iterations := objectsPerBucket / 1000
	if iterations < 1 {
		iterations = 1
	}

	randomSuffix := rand.Intn(65536)
	paddedStart := fmt.Sprintf("%02d", 1)
	paddedEnd := fmt.Sprintf("%02d", numBuckets)
	fmt.Printf("=== buckets: %s-%d-[%s-%s]--%s--x-s3 ===\n", bucketPrefix, randomSuffix, paddedStart, paddedEnd, azID)
	fmt.Printf("=== objects: %d per bucket ===\n", objectsPerBucket)

	// Configure AWS SDK
	ctx := context.Background()
	var cfg aws.Config
	var err error

	if profile != "" {
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(profile),
			config.WithRegion(region),
		)
	} else {
		cfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(region))
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

			paddedNum := fmt.Sprintf("%02d", bucketNum)
			bucketName := fmt.Sprintf("%s-%d-%s", bucketPrefix, randomSuffix, paddedNum)
			// naming for S3 Express One Zone (using az 4 in us-east-1)
			lowerBucketName := strings.ToLower(fmt.Sprintf("%s--%s--x-s3", bucketName, azID))

			// Check if bucket exists and create if it doesn't
			listInput := &s3.ListDirectoryBucketsInput{}

			listDirBucketsOptFn := func(o *s3.Options) {
				o.Retryer = retryer
			}

			_, err := s3Client.ListDirectoryBuckets(ctx, listInput, listDirBucketsOptFn)
			bucketExists := false
			if err == nil {
				// Note: This is simplified. In real code, we'd check if the bucket is in the response
				// The original script used grep to check bucket name in the output
				bucketExists = false // Assume bucket doesn't exist
			}

			if !bucketExists {
				// For S3 Express One Zone bucket creation
				createBucketInput := &s3.CreateBucketInput{
					Bucket: aws.String(lowerBucketName),
					CreateBucketConfiguration: &types.CreateBucketConfiguration{
						Location: &types.LocationInfo{
							Type: types.LocationTypeAvailabilityZone,
							Name: aws.String(azID),
						},
						Bucket: &types.BucketInfo{
							DataRedundancy: types.DataRedundancySingleAvailabilityZone,
							Type:           types.BucketTypeDirectory,
						},
					},
				}

				createBucketOptFn := func(o *s3.Options) {
					o.Retryer = retryer
				}

				_, err = s3Client.CreateBucket(ctx, createBucketInput, createBucketOptFn)
				if err != nil {
					log.Error().Err(err).Str("bucket", lowerBucketName).Msg("Failed to create directory bucket")
					return
				}
			}

			// Generate approximately objectsPerBucket objects on S3 Express One Zone
			// NOTE: It'll cost you $2.5 (S3 Express One Zone: 0.0025 USD / 1000 PUT)
			// NOTE: You can create up to 10 directory buckets in each of your AWS accounts
			for i := 1; i <= iterations; i++ {
				numObjectsThisIteration := 1000
				if i == iterations && objectsPerBucket%1000 != 0 {
					numObjectsThisIteration = objectsPerBucket % 1000
				}

				// Create a worker pool for uploads (max 20 concurrent uploads)
				uploadSem := semaphore.NewWeighted(20)
				var uploadWg sync.WaitGroup

				objectKeys := make([]string, numObjectsThisIteration)

				// Process each upload in parallel
				for j := range numObjectsThisIteration {
					// Generate object keys for this iteration
					objectKeys[j] = fmt.Sprintf("%d/%d/%d.txt", i, j+1, rand.Intn(65536))

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

						objectKey := objectKeys[objectIndex]

						// Execute PutObject with retry configuration
						optFn := func(o *s3.Options) {
							o.Retryer = retryer
						}

						_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
							Bucket: aws.String(lowerBucketName),
							Key:    aws.String(objectKey),
							Body:   strings.NewReader(""),
						}, optFn)

						// FIXME: Errors often occur when uploading files to S3 Express One Zone
						// The error can be ignored as mentioned in the original script
						if err != nil {
							log.Error().Err(err).Str("object", objectKey).Msg("Failed to upload object")
						}
					}(j)
				}

				// Wait for all uploads to complete
				uploadWg.Wait()
			}
		}(bucketNum)
	}

	wg.Wait()
}

func showHelp() {
	fmt.Println("S3 Express One Zone (Directory Bucket) Test Data Generator")
	fmt.Println("======================================================")
	fmt.Println("This tool creates multiple S3 Express One Zone directory buckets and populates them with objects.")
	fmt.Println("NOTE: Directory buckets have a limit of 10 buckets per AWS account.")
	fmt.Println("")
	fmt.Println("Options:")
	fmt.Println("  -p string  AWS profile name (optional)")
	fmt.Println("  -b string  Bucket name prefix (required)")
	fmt.Println("  -n int     Number of buckets to create (default: 10, max: 100)")
	fmt.Println("  -o int     Number of objects per bucket (default: 10000)")
	fmt.Println("  -h         Show this help message")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("  Create 2 directory buckets with 500 objects each:")
	fmt.Println("    go run testdata/cmd/directory/main.go -b my-bucket -n 2 -o 500")
	fmt.Println("")
	fmt.Println("  Using a specific AWS profile:")
	fmt.Println("    go run testdata/cmd/directory/main.go -p my-profile -b my-bucket")
	fmt.Println("")
	fmt.Println("  Or using Make:")
	fmt.Println("    make testgen_directory OPT=\"-b my-bucket -n 2 -o 500\"")
}
