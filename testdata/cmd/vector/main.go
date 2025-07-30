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
	"github.com/aws/aws-sdk-go-v2/service/s3vectors"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/go-to-k/cls3/testdata/pkg/retryer"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/semaphore"
)

func main() {
	var (
		profile          string
		bucketPrefix     string
		numBuckets       int
		vectorsPerIndex  int
		indexesPerBucket int
		region           string
		help             bool
	)

	flag.StringVar(&profile, "p", "", "AWS profile")
	flag.StringVar(&bucketPrefix, "b", "cls3-test", "Bucket prefix")
	flag.IntVar(&numBuckets, "n", 1, "Number of buckets")
	flag.IntVar(&vectorsPerIndex, "v", 100, "Vectors per index")
	flag.IntVar(&indexesPerBucket, "i", 100, "Indexes per bucket")
	flag.StringVar(&region, "r", "us-east-1", "AWS region")
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

	if vectorsPerIndex <= 0 {
		fmt.Println("number of vectors (-v) must be a positive integer")
		os.Exit(1)
	}

	if indexesPerBucket <= 0 {
		fmt.Println("number of indexes (-i) must be a positive integer")
		os.Exit(1)
	}

	if indexesPerBucket > 10000 {
		fmt.Println("number of indexes (-i) must be less than or equal to 10,000 for vector buckets")
		os.Exit(1)
	}

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

	// Create S3 Vectors client and apply retry configuration
	s3VectorsClient := s3vectors.NewFromConfig(cfg)
	s3Retryer := retryer.CreateS3Retryer()

	// Configure STS client
	stsClient := sts.NewFromConfig(cfg)

	// Get AWS account ID (not needed for S3 Vectors operations, but keeping for consistency)
	_, err = stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get caller identity")
	}

	randomSuffix := rand.Intn(65536)
	paddedStart := fmt.Sprintf("%02d", 1)
	paddedEnd := fmt.Sprintf("%02d", numBuckets)
	fmt.Printf("=== buckets: %s-%d-[%s-%s] ===\n", bucketPrefix, randomSuffix, paddedStart, paddedEnd)
	fmt.Printf("=== indexes: %d, vectors: %d ===\n", indexesPerBucket, vectorsPerIndex)

	// Processing buckets in parallel with a concurrency limit
	var bucketWg sync.WaitGroup
	bucketSem := semaphore.NewWeighted(10)

	for bucketNum := 1; bucketNum <= numBuckets; bucketNum++ {
		bucketWg.Add(1)

		if semErr := bucketSem.Acquire(ctx, 1); semErr != nil {
			log.Error().Err(semErr).Msg("Failed to acquire bucket semaphore")
			bucketWg.Done()
			continue
		}

		go func(bucketNum int) {
			defer bucketWg.Done()
			defer bucketSem.Release(1)

			paddedNum := fmt.Sprintf("%02d", bucketNum)
			bucketName := fmt.Sprintf("%s-%d-%s", bucketPrefix, randomSuffix, paddedNum)
			lowerBucketName := strings.ToLower(bucketName)

			// Check if bucket exists
			listBucketsOptFn := func(o *s3vectors.Options) {
				o.Retryer = s3Retryer
			}
			listBucketsOutput, err := s3VectorsClient.ListVectorBuckets(ctx, &s3vectors.ListVectorBucketsInput{}, listBucketsOptFn)
			bucketExists := false
			if err == nil {
				for _, bucket := range listBucketsOutput.VectorBuckets {
					if *bucket.VectorBucketName == lowerBucketName {
						bucketExists = true
						break
					}
				}
			}

			if !bucketExists {
				createBucketOptFn := func(o *s3vectors.Options) {
					o.Retryer = s3Retryer
				}
				_, err = s3VectorsClient.CreateVectorBucket(ctx, &s3vectors.CreateVectorBucketInput{
					VectorBucketName: aws.String(lowerBucketName),
				}, createBucketOptFn)
				if err != nil {
					log.Error().Err(err).Str("bucket", lowerBucketName).Msg("Failed to create vector bucket")
					return
				}
			}

			// Create indexes and vectors in parallel
			// NOTE: Each bucket can contain multiple indexes
			// NOTE: Up to 10,000 indexes can be created in total per bucket
			// see: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-limitations.html

			// Process indexes in parallel with a concurrency limit
			var wg sync.WaitGroup
			indexSem := semaphore.NewWeighted(16)

			for i := 1; i <= indexesPerBucket; i++ {
				wg.Add(1)

				if semErr := indexSem.Acquire(ctx, 1); semErr != nil {
					log.Error().Err(semErr).Msg("Failed to acquire index semaphore")
					wg.Done()
					continue
				}

				go func(indexNum int) {
					defer wg.Done()
					defer indexSem.Release(1)

					indexName := fmt.Sprintf("my-index-%d", indexNum)

					// Create vector index
					createIndexOptFn := func(o *s3vectors.Options) {
						o.Retryer = s3Retryer
					}
					_, err = s3VectorsClient.CreateIndex(ctx, &s3vectors.CreateIndexInput{
						VectorBucketName: aws.String(lowerBucketName),
						IndexName:        aws.String(indexName),
						DataType:         types.DataTypeFloat32,
						Dimension:        aws.Int32(128),
						DistanceMetric:   types.DistanceMetricCosine,
					}, createIndexOptFn)
					if err != nil {
						log.Error().Err(err).Str("index", indexName).Msg("Failed to create vector index")
						return
					}

					// Process vectors in batches of 500 (PutVectors API limit)
					// see: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-limitations.html
					batchSize := 500
					for batchStart := 1; batchStart <= vectorsPerIndex; batchStart += batchSize {
						batchEnd := batchStart + batchSize - 1
						if batchEnd > vectorsPerIndex {
							batchEnd = vectorsPerIndex
						}

						// Create batch of vectors
						vectors := make([]types.PutInputVector, 0, batchEnd-batchStart+1)
						for vector := batchStart; vector <= batchEnd; vector++ {
							vectorId := fmt.Sprintf("vector-%d", vector)

							// Generate sample vector data (128 dimensions)
							vectorData := make([]float32, 128)
							for j := range vectorData {
								vectorData[j] = rand.Float32()
							}

							vectors = append(vectors, types.PutInputVector{
								Key:  aws.String(vectorId),
								Data: &types.VectorDataMemberFloat32{Value: vectorData},
							})
						}

						// Upload batch
						optFn := func(o *s3vectors.Options) {
							o.Retryer = s3Retryer
						}

						_, err = s3VectorsClient.PutVectors(ctx, &s3vectors.PutVectorsInput{
							VectorBucketName: aws.String(lowerBucketName),
							IndexName:        aws.String(indexName),
							Vectors:          vectors,
						}, optFn)
						if err != nil {
							log.Error().Err(err).
								Str("index", indexName).
								Int("batch_start", batchStart).
								Int("batch_end", batchEnd).
								Msg("Failed to put vector batch")
						}
					}
				}(i)
			}

			// Wait for all indexes to be created and their vectors to be uploaded
			wg.Wait()
		}(bucketNum)
	}

	// Wait for all buckets to be processed
	bucketWg.Wait()
}

func showHelp() {
	fmt.Println("S3 Vector Bucket Test Data Generator")
	fmt.Println("===================================")
	fmt.Println("This tool creates S3 vector buckets with indexes and vectors.")
	fmt.Println("NOTE: Each vector bucket can have up to 10,000 indexes.")
	fmt.Println("")
	fmt.Println("Options:")
	fmt.Println("  -p string  AWS profile name (optional)")
	fmt.Println("  -b string  Bucket name prefix (required)")
	fmt.Println("  -n int     Number of buckets to create (default: 1, max: 10)")
	fmt.Println("  -v int     Number of vectors per index (default: 100)")
	fmt.Println("  -i int     Number of indexes per bucket (default: 100, max: 10,000)")
	fmt.Println("  -r string  AWS region (default: us-east-1)")
	fmt.Println("  -h         Show this help message")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("  Create 1 vector bucket with 50 vectors per index and 20 indexes:")
	fmt.Println("    go run testdata/cmd/vector/main.go -v 50 -i 20")
	fmt.Println("")
	fmt.Println("  Using a specific AWS profile and region:")
	fmt.Println("    go run testdata/cmd/vector/main.go -p my-profile -r us-east-1")
	fmt.Println("")
	fmt.Println("  Or using Make:")
	fmt.Println("    make testgen_vector OPT=\"-n 1 -v 50 -i 20\"")
}
