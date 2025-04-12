package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/semaphore"
)

func main() {
	var (
		profile            string
		bucketPrefix       string
		numBuckets         int
		tablesPerNamespace int
		namespacesPerTable int
		region             string
		help               bool
	)

	flag.StringVar(&profile, "p", "", "AWS profile")
	flag.StringVar(&bucketPrefix, "b", "cls3-test", "Bucket prefix")
	flag.IntVar(&numBuckets, "n", 1, "Number of buckets")
	flag.IntVar(&tablesPerNamespace, "t", 100, "Tables per namespace")
	flag.IntVar(&namespacesPerTable, "s", 100, "Namespaces per table")
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

	if numBuckets > 10 {
		fmt.Println("number of buckets (-n) must be less than or equal to 10 for table buckets")
		os.Exit(1)
	}

	if tablesPerNamespace <= 0 {
		fmt.Println("number of tables (-t) must be a positive integer")
		os.Exit(1)
	}

	if namespacesPerTable <= 0 {
		fmt.Println("number of namespaces (-s) must be a positive integer")
		os.Exit(1)
	}

	if tablesPerNamespace*namespacesPerTable > 10000 {
		fmt.Println("number of tables (-t) must be less than or equal to 10000")
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

	s3TablesClient := s3tables.NewFromConfig(cfg)
	stsClient := sts.NewFromConfig(cfg)

	// Get AWS account ID
	identity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get caller identity")
	}
	accountID := *identity.Account

	randomSuffix := rand.Intn(65536) // $RANDOM is 0-32767, but we can use a bigger range
	paddedStart := fmt.Sprintf("%02d", 1)
	paddedEnd := fmt.Sprintf("%02d", numBuckets)
	fmt.Printf("=== buckets: %s-%d-[%s-%s] ===\n", bucketPrefix, randomSuffix, paddedStart, paddedEnd)

	// NOTE: S3 Tables API has throttling limits
	// Processing buckets sequentially, but tables are processed in batches of 10 concurrent processes
	// This matches the original shell script implementation

	for bucketNum := 1; bucketNum <= numBuckets; bucketNum++ {
		paddedNum := fmt.Sprintf("%02d", bucketNum)
		bucketName := fmt.Sprintf("%s-%d-%s", bucketPrefix, randomSuffix, paddedNum)
		lowerBucketName := bucketName // In Go, we're not applying .toLowerCase() - assuming bucket names are case-sensitive in AWS

		// Check if bucket exists
		listBucketsOutput, err := s3TablesClient.ListTableBuckets(ctx, &s3tables.ListTableBucketsInput{})
		bucketExists := false
		if err == nil {
			for _, bucket := range listBucketsOutput.TableBuckets {
				if *bucket.Name == lowerBucketName {
					bucketExists = true
					break
				}
			}
		}

		if !bucketExists {
			_, err = s3TablesClient.CreateTableBucket(ctx, &s3tables.CreateTableBucketInput{
				Name: aws.String(lowerBucketName),
			})
			if err != nil {
				log.Error().Err(err).Str("bucket", lowerBucketName).Msg("Failed to create table bucket")
				continue
			}
		}

		tableBucketArn := fmt.Sprintf("arn:aws:s3tables:%s:%s:bucket/%s", region, accountID, lowerBucketName)

		// Create namespaces and tables
		// NOTE: Each namespace can contain multiple tables
		// NOTE: Up to 10,000 tables can be created in total per bucket
		for i := 1; i <= namespacesPerTable; i++ {
			namespaceName := fmt.Sprintf("my_namespace_%d", i)
			_, err = s3TablesClient.CreateNamespace(ctx, &s3tables.CreateNamespaceInput{
				TableBucketARN: aws.String(tableBucketArn),
				Namespace:      []string{namespaceName},
			})
			if err != nil {
				log.Error().Err(err).Str("namespace", namespaceName).Msg("Failed to create namespace")
				continue
			}

			// Process tables in batches of 10 concurrent processes
			// This matches the original shell script implementation
			var wg sync.WaitGroup
			tableSem := semaphore.NewWeighted(10) // Limit to 10 concurrent processes

			for table := 1; table <= tablesPerNamespace; table++ {
				wg.Add(1)

				// Acquire semaphore with weight 1
				if semErr := tableSem.Acquire(ctx, 1); semErr != nil {
					log.Error().Err(semErr).Msg("Failed to acquire table semaphore")
					wg.Done()
					continue
				}

				go func(table int) {
					defer wg.Done()
					defer tableSem.Release(1) // Release semaphore when done

					tableName := fmt.Sprintf("my_table_%d", table)
					// Create metadata structure for Iceberg table
					schemaField := types.SchemaField{
						Name:     aws.String("column"),
						Type:     aws.String("int"),
						Required: false,
					}

					icebergSchema := &types.IcebergSchema{
						Fields: []types.SchemaField{schemaField},
					}

					icebergMetadata := &types.IcebergMetadata{
						Schema: icebergSchema,
					}

					tableMetadata := &types.TableMetadataMemberIceberg{
						Value: *icebergMetadata,
					}

					_, err = s3TablesClient.CreateTable(ctx, &s3tables.CreateTableInput{
						TableBucketARN: aws.String(tableBucketArn),
						Namespace:      aws.String(namespaceName),
						Name:           aws.String(tableName),
						Metadata:       tableMetadata,
						Format:         types.OpenTableFormatIceberg,
					})
					if err != nil {
						log.Error().Err(err).Str("table", tableName).Msg("Failed to create table")
					}
				}(table)

				// If we've hit the limit of concurrent processes, wait for them to complete
				if table%10 == 0 {
					wg.Wait()
				}
			}

			// Wait for any remaining processes
			wg.Wait()
		}
	}
}

func showHelp() {
	fmt.Println("S3 Table Bucket Test Data Generator")
	fmt.Println("=================================")
	fmt.Println("This tool creates S3 table buckets with namespaces and tables.")
	fmt.Println("NOTE: Each table bucket can have up to 10,000 tables in total.")
	fmt.Println("")
	fmt.Println("Options:")
	fmt.Println("  -p string  AWS profile name (optional)")
	fmt.Println("  -b string  Bucket name prefix (required)")
	fmt.Println("  -n int     Number of buckets to create (default: 1, max: 10)")
	fmt.Println("  -t int     Number of tables per namespace (default: 100)")
	fmt.Println("  -s int     Number of namespaces per table (default: 100)")
	fmt.Println("  -r string  AWS region (default: us-east-1)")
	fmt.Println("  -h         Show this help message")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("  Create 1 table bucket with 50 tables per namespace and 20 namespaces:")
	fmt.Println("    go run testdata/cmd/table/main.go -b my-bucket -t 50 -s 20")
	fmt.Println("")
	fmt.Println("  Using a specific AWS profile and region:")
	fmt.Println("    go run testdata/cmd/table/main.go -p my-profile -b my-bucket -r us-west-2")
	fmt.Println("")
	fmt.Println("  Or using Make:")
	fmt.Println("    make testgen_table OPT=\"-b my-bucket -n 1 -t 50 -s 20 -r us-west-2\"")
}
