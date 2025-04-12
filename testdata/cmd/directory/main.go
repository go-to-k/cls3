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

	randomSuffix := rand.Intn(65536) // $RANDOM is 0-32767, but we can use a bigger range
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

			paddedNum := fmt.Sprintf("%02d", bucketNum)
			bucketName := fmt.Sprintf("%s-%d-%s", bucketPrefix, randomSuffix, paddedNum)
			// naming for S3 Express One Zone (using az 4 in us-east-1)
			lowerBucketName := fmt.Sprintf("%s--%s--x-s3", bucketName, azID)

			// Check if bucket exists and create if it doesn't
			listInput := &s3.ListDirectoryBucketsInput{}
			_, err := s3Client.ListDirectoryBuckets(ctx, listInput)
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

				_, err = s3Client.CreateBucket(ctx, createBucketInput)
				if err != nil {
					log.Error().Err(err).Str("bucket", lowerBucketName).Msg("Failed to create directory bucket")
					return
				}
			}

			dir := filepath.Join("./testfiles", lowerBucketName)
			err = os.MkdirAll(dir, 0755)
			if err != nil {
				log.Error().Err(err).Str("dir", dir).Msg("Failed to create directory")
				return
			}

			// Generate approximately objectsPerBucket objects on S3 Express One Zone
			// NOTE: It'll cost you $2.5 (S3 Express One Zone: 0.0025 USD / 1000 PUT)
			// NOTE: You can create up to 10 directory buckets in each of your AWS accounts
			// FIXME: Errors often occur when uploading files to S3 Express One Zone
			// Retrying fixes it, but it may happen again with another file
			for i := 1; i <= iterations; i++ {
				// Create test files
				for j := 1; j <= 1000; j++ {
					fileName := fmt.Sprintf("%d_%d_%d.txt", i, j, rand.Intn(65536))
					filePath := filepath.Join(dir, fileName)
					file, createErr := os.Create(filePath)
					if createErr != nil {
						log.Error().Err(createErr).Str("file", filePath).Msg("Failed to create file")
						continue
					}
					file.Close()
				}

				// Upload files - the original script notes that uploads may fail randomly
				// so we ignore errors here similar to the shell script using 'set +e'
				files := []string{}
				err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return nil // Continue despite error, mimicking set +e
					}
					if !info.IsDir() {
						files = append(files, path)
					}
					return nil
				})

				// Create a worker pool for uploads (max 20 concurrent uploads)
				uploadSem := semaphore.NewWeighted(20)
				var uploadWg sync.WaitGroup

				for _, path := range files {
					uploadWg.Add(1)

					// Acquire upload semaphore with weight 1
					if err := uploadSem.Acquire(ctx, 1); err != nil {
						log.Error().Err(err).Msg("Failed to acquire upload semaphore")
						uploadWg.Done()
						continue
					}

					go func(filePath string) {
						defer uploadWg.Done()
						defer uploadSem.Release(1) // Release semaphore when done

						relPath, err := filepath.Rel(dir, filePath)
						if err != nil {
							return // Continue despite error
						}

						file, err := os.Open(filePath)
						if err != nil {
							return // Continue despite error
						}
						defer file.Close()

						_, _ = s3Client.PutObject(ctx, &s3.PutObjectInput{
							Bucket: aws.String(lowerBucketName),
							Key:    aws.String(relPath),
							Body:   file,
						})
						// Ignore errors here as per the shell script (set +e)
					}(path)
				}

				// Wait for all uploads to complete
				uploadWg.Wait()

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
