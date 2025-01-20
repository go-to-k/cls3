//go:generate mockgen -source=$GOFILE -destination=s3_tables_mock.go -package=$GOPACKAGE -write_package_comment=false
package client

import (
	"context"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
)

type IS3Tables interface {
	DeleteTableBucket(ctx context.Context, tableBucketARN *string) error
	DeleteNamespace(ctx context.Context, namespace *string, tableBucketARN *string) error
	DeleteTable(ctx context.Context, tableName *string, namespace *string, tableBucketARN *string) error
	ListTableBuckets(ctx context.Context) ([]types.TableBucketSummary, error)
	ListNamespaces(ctx context.Context, tableBucketARN *string) ([]types.NamespaceSummary, error)
	ListTables(ctx context.Context, tableBucketARN *string, namespace *string) ([]types.TableSummary, error)
}

var _ IS3Tables = (*S3Tables)(nil)

type S3Tables struct {
	client  *s3tables.Client
	retryer *Retryer
}

func NewS3Tables(client *s3tables.Client) *S3Tables {
	retryable := func(err error) bool {
		return strings.Contains(err.Error(), "api error SlowDown")
	}
	retryer := NewRetryer(retryable, SleepTimeSecForS3)

	return &S3Tables{
		client,
		retryer,
	}
}

func (s *S3Tables) DeleteTableBucket(ctx context.Context, tableBucketARN *string) error {
	input := &s3tables.DeleteTableBucketInput{
		TableBucketARN: tableBucketARN,
	}

	optFn := func(o *s3tables.Options) {
		o.Retryer = s.retryer
	}

	_, err := s.client.DeleteTableBucket(ctx, input, optFn)
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

	_, err := s.client.DeleteNamespace(ctx, input, optFn)
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

	_, err := s.client.DeleteTable(ctx, input, optFn)
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

		output, err := s.client.ListTableBuckets(ctx, input, optFn)
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

func (s *S3Tables) ListNamespaces(ctx context.Context, tableBucketARN *string) ([]types.NamespaceSummary, error) {
	namespaces := []types.NamespaceSummary{}
	var continuationToken *string

	for {
		select {
		case <-ctx.Done():
			return namespaces, &ClientError{
				ResourceName: tableBucketARN,
				Err:          ctx.Err(),
			}
		default:
		}

		input := &s3tables.ListNamespacesInput{
			TableBucketARN:    tableBucketARN,
			ContinuationToken: continuationToken,
		}

		optFn := func(o *s3tables.Options) {
			o.Retryer = s.retryer
		}

		output, err := s.client.ListNamespaces(ctx, input, optFn)
		if err != nil {
			return namespaces, &ClientError{
				ResourceName: tableBucketARN,
				Err:          err,
			}
		}

		namespaces = append(namespaces, output.Namespaces...)

		if output.ContinuationToken == nil {
			break
		}
		continuationToken = output.ContinuationToken
	}

	return namespaces, nil
}

func (s *S3Tables) ListTables(ctx context.Context, tableBucketARN *string, namespace *string) ([]types.TableSummary, error) {
	tables := []types.TableSummary{}
	var continuationToken *string

	for {
		select {
		case <-ctx.Done():
			return tables, &ClientError{
				ResourceName: aws.String(*tableBucketARN + "/" + *namespace),
				Err:          ctx.Err(),
			}
		default:
		}

		input := &s3tables.ListTablesInput{
			Namespace:         namespace,
			TableBucketARN:    tableBucketARN,
			ContinuationToken: continuationToken,
		}

		optFn := func(o *s3tables.Options) {
			o.Retryer = s.retryer
		}

		output, err := s.client.ListTables(ctx, input, optFn)
		if err != nil {
			return tables, &ClientError{
				ResourceName: aws.String(*tableBucketARN + "/" + *namespace),
				Err:          err,
			}
		}

		tables = append(tables, output.Tables...)

		if output.ContinuationToken == nil {
			break
		}
		continuationToken = output.ContinuationToken
	}

	return tables, nil
}
