//go:generate mockgen -source=$GOFILE -destination=mock_$GOFILE -package=$GOPACKAGE -write_package_comment=false
package client

import (
	"context"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
)

var SleepTimeSecForS3Tables = 3 // NOTE: Because S3Tables is a serial operation, a low value is OK.

type ListNamespacesByPageOutput struct {
	Namespaces        []types.NamespaceSummary
	ContinuationToken *string
}

type ListTablesByPageOutput struct {
	Tables            []types.TableSummary
	ContinuationToken *string
}

type IS3Tables interface {
	DeleteTableBucket(ctx context.Context, tableBucketARN *string) error
	DeleteNamespace(ctx context.Context, namespace *string, tableBucketARN *string) error
	DeleteTable(ctx context.Context, tableName *string, namespace *string, tableBucketARN *string) error
	ListTableBuckets(ctx context.Context) ([]types.TableBucketSummary, error)
	ListNamespacesByPage(ctx context.Context, tableBucketARN *string, continuationToken *string) (*ListNamespacesByPageOutput, error)
	ListTablesByPage(ctx context.Context, tableBucketARN *string, namespace *string, continuationToken *string) (*ListTablesByPageOutput, error)
}

var _ IS3Tables = (*S3Tables)(nil)

type S3Tables struct {
	*s3tables.Client
	retryer *Retryer
}

type NewS3TablesInput struct {
	Region  string
	Profile string
}

func NewS3Tables(ctx context.Context, input NewS3TablesInput) (*S3Tables, error) {
	config, err := loadAWSConfig(ctx, input.Region, input.Profile)
	if err != nil {
		return nil, err
	}

	retryable := func(err error) bool {
		isRetryable :=
			strings.Contains(err.Error(), "api error SlowDown") ||
				strings.Contains(err.Error(), "An internal error occurred. Try again.") ||
				strings.Contains(err.Error(), "StatusCode: 429")

		return isRetryable
	}
	retryer := NewRetryer(retryable, SleepTimeSecForS3Tables)

	client := s3tables.NewFromConfig(config, func(o *s3tables.Options) {
		o.RetryMaxAttempts = SDKRetryMaxAttempts
		o.RetryMode = aws.RetryModeStandard
	})

	return &S3Tables{
		client,
		retryer,
	}, nil
}

func (s *S3Tables) DeleteTableBucket(ctx context.Context, tableBucketARN *string) error {
	input := &s3tables.DeleteTableBucketInput{
		TableBucketARN: tableBucketARN,
	}

	optFn := func(o *s3tables.Options) {
		o.Retryer = s.retryer
	}

	_, err := s.Client.DeleteTableBucket(ctx, input, optFn)
	if err != nil {
		return &ClientError{
			ResourceName: tableBucketARN,
			Err:          err,
		}
	}
	return nil
}

func (s *S3Tables) DeleteNamespace(ctx context.Context, namespace *string, tableBucketARN *string) error {
	input := &s3tables.DeleteNamespaceInput{
		Namespace:      namespace,
		TableBucketARN: tableBucketARN,
	}

	optFn := func(o *s3tables.Options) {
		o.Retryer = s.retryer
	}

	_, err := s.Client.DeleteNamespace(ctx, input, optFn)
	if err != nil {
		return &ClientError{
			ResourceName: aws.String(*tableBucketARN + "/" + *namespace),
			Err:          err,
		}
	}
	return nil
}

func (s *S3Tables) DeleteTable(ctx context.Context, tableName *string, namespace *string, tableBucketARN *string) error {
	input := &s3tables.DeleteTableInput{
		Name:           tableName,
		Namespace:      namespace,
		TableBucketARN: tableBucketARN,
	}

	optFn := func(o *s3tables.Options) {
		o.Retryer = s.retryer
	}

	_, err := s.Client.DeleteTable(ctx, input, optFn)
	if err != nil {
		return &ClientError{
			ResourceName: aws.String(*tableBucketARN + "/" + *namespace + "/" + *tableName),
			Err:          err,
		}
	}
	return nil
}

func (s *S3Tables) ListTableBuckets(ctx context.Context) ([]types.TableBucketSummary, error) {
	buckets := []types.TableBucketSummary{}
	var continuationToken *string

	for {
		select {
		case <-ctx.Done():
			return buckets, &ClientError{
				Err: ctx.Err(),
			}
		default:
		}

		input := &s3tables.ListTableBucketsInput{
			ContinuationToken: continuationToken,
		}

		optFn := func(o *s3tables.Options) {
			o.Retryer = s.retryer
		}

		output, err := s.Client.ListTableBuckets(ctx, input, optFn)
		if err != nil {
			return buckets, &ClientError{
				Err: err,
			}
		}

		buckets = append(buckets, output.TableBuckets...)

		if output.ContinuationToken == nil {
			break
		}
		continuationToken = output.ContinuationToken
	}

	// sort by bucket name
	sort.Slice(buckets, func(i, j int) bool {
		return *buckets[i].Name < *buckets[j].Name
	})

	return buckets, nil
}

func (s *S3Tables) ListNamespacesByPage(ctx context.Context, tableBucketARN *string, continuationToken *string) (*ListNamespacesByPageOutput, error) {
	namespaces := []types.NamespaceSummary{}

	input := &s3tables.ListNamespacesInput{
		TableBucketARN:    tableBucketARN,
		ContinuationToken: continuationToken,
	}

	optFn := func(o *s3tables.Options) {
		o.Retryer = s.retryer
	}

	output, err := s.Client.ListNamespaces(ctx, input, optFn)
	if err != nil {
		return nil, &ClientError{
			ResourceName: tableBucketARN,
			Err:          err,
		}
	}

	namespaces = append(namespaces, output.Namespaces...)

	return &ListNamespacesByPageOutput{
		Namespaces:        namespaces,
		ContinuationToken: output.ContinuationToken,
	}, nil
}

func (s *S3Tables) ListTablesByPage(ctx context.Context, tableBucketARN *string, namespace *string, continuationToken *string) (*ListTablesByPageOutput, error) {
	tables := []types.TableSummary{}

	input := &s3tables.ListTablesInput{
		Namespace:         namespace,
		TableBucketARN:    tableBucketARN,
		ContinuationToken: continuationToken,
	}

	optFn := func(o *s3tables.Options) {
		o.Retryer = s.retryer
	}

	output, err := s.Client.ListTables(ctx, input, optFn)
	if err != nil {
		return nil, &ClientError{
			ResourceName: aws.String(*tableBucketARN + "/" + *namespace),
			Err:          err,
		}
	}

	tables = append(tables, output.Tables...)

	return &ListTablesByPageOutput{
		Tables:            tables,
		ContinuationToken: output.ContinuationToken,
	}, nil
}
