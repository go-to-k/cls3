package app

import (
	"bytes"
	"flag"
	"fmt"
	"strings"
	"testing"

	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/internal/wrapper"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v2"
	"go.uber.org/mock/gomock"
)

func Test_validateOptions(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	io.Logger = &logger

	tests := []struct {
		name            string
		app             *App
		expectedErr     string
		expectedWarning string
	}{
		{
			name: "error when no bucket names specified in non-interactive mode",
			app: &App{
				InteractiveMode:   false,
				BucketNames:       cli.NewStringSlice(),
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: At least one bucket name must be specified in command options (-b) or a flow of the interactive mode (-i).\n",
		},
		{
			name: "error when bucket names specified in interactive mode",
			app: &App{
				InteractiveMode:   true,
				BucketNames:       cli.NewStringSlice("bucket1"),
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: When specifying -i, do not specify the -b option.\n",
		},
		{
			name: "error when both force mode and old versions only specified",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				ForceMode:         true,
				OldVersionsOnly:   true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: When specifying -o, do not specify the -f option.\n",
		},
		{
			name: "error when both directory buckets mode and table buckets mode specified",
			app: &App{
				BucketNames:          cli.NewStringSlice("bucket1"),
				DirectoryBucketsMode: true,
				TableBucketsMode:     true,
				ConcurrencyNumber:    UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: You cannot specify both -d and -t options.\n",
		},
		{
			name: "error when both directory buckets mode and vector buckets mode specified",
			app: &App{
				BucketNames:          cli.NewStringSlice("bucket1"),
				DirectoryBucketsMode: true,
				VectorBucketsMode:    true,
				ConcurrencyNumber:    UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: You cannot specify both -d and -V options.\n",
		},
		{
			name: "error when both table buckets mode and vector buckets mode specified",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				TableBucketsMode:  true,
				VectorBucketsMode: true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: You cannot specify both -t and -V options.\n",
		},
		{
			name: "error when both directory buckets mode and old versions only specified",
			app: &App{
				BucketNames:          cli.NewStringSlice("bucket1"),
				DirectoryBucketsMode: true,
				OldVersionsOnly:      true,
				ConcurrencyNumber:    UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: When specifying -d, do not specify the -o option.\n",
		},
		{
			name: "error when concurrency number specified without concurrent mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				ConcurrencyNumber: 2,
				ConcurrentMode:    false,
			},
			expectedErr: "InvalidOptionError: When specifying -n, you must specify the -c option.\n",
		},
		{
			name: "error when non positive concurrency number specified with concurrent mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				ConcurrencyNumber: -1,
				ConcurrentMode:    true,
			},
			expectedErr: "InvalidOptionError: You must specify a positive number for the -n option when specifying the -c option.\n",
		},
		{
			name: "warn when directory buckets mode without region",
			app: &App{
				BucketNames:          cli.NewStringSlice("bucket1"),
				DirectoryBucketsMode: true,
				Region:               "",
				ConcurrencyNumber:    UnspecifiedConcurrencyNumber,
			},
			expectedErr:     "",
			expectedWarning: "{\"level\":\"warn\",\"message\":\"You are in the Directory Buckets Mode `-d` to clear the Directory Buckets. In this mode, operation across regions is not possible, but only in one region. You can specify the region with the `-r` option.\"}",
		},
		{
			name: "error when both table buckets mode and old versions only specified",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				TableBucketsMode:  true,
				OldVersionsOnly:   true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: When specifying -t, do not specify the -o option.\n",
		},
		{
			name: "error when table buckets mode with concurrent mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				TableBucketsMode:  true,
				ConcurrentMode:    true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: When specifying -t, do not specify the -c option because the throttling threshold for S3 Tables is very low.\n",
		},
		{
			name: "error when both vector buckets mode and old versions only specified",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				VectorBucketsMode: true,
				OldVersionsOnly:   true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: When specifying -V, do not specify the -o option.\n",
		},
		{
			name: "error when path style with directory buckets mode",
			app: &App{
				BucketNames:          cli.NewStringSlice("bucket1"),
				PathStyle:            true,
				DirectoryBucketsMode: true,
				Region:               "us-east-1",
				ConcurrencyNumber:    UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: When specifying -P (--pathStyle), do not specify the -d option.\n",
		},
		{
			name: "error when path style with table buckets mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				PathStyle:         true,
				TableBucketsMode:  true,
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: When specifying -P (--pathStyle), do not specify the -t option.\n",
		},
		{
			name: "error when path style with vector buckets mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				PathStyle:         true,
				VectorBucketsMode: true,
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: When specifying -P (--pathStyle), do not specify the -V option.\n",
		},
		{
			name: "error when key prefix specified with table buckets mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				TableBucketsMode:  true,
				KeyPrefix:         "prefix",
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: When specifying -t, do not specify the -k option.\n",
		},
		{
			name: "error when key prefix specified with force mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				ForceMode:         true,
				KeyPrefix:         "prefix",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: When specifying -k, do not specify the -f option.\n",
		},
		{
			name: "a delimiter is added automatically when key prefix does not end with delimiter in directory buckets mode",
			app: &App{
				BucketNames:          cli.NewStringSlice("bucket1"),
				DirectoryBucketsMode: true,
				KeyPrefix:            "prefix",
				Region:               "us-east-1",
				ConcurrencyNumber:    UnspecifiedConcurrencyNumber,
			},
			expectedWarning: "{\"level\":\"warn\",\"message\":\"The key prefix `prefix` for the Directory Buckets does not end with a delimiter ( / ). It has been added automatically.\"}",
		},
		{
			name: "warn when table buckets mode without region",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				TableBucketsMode:  true,
				Region:            "",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr:     "",
			expectedWarning: "{\"level\":\"warn\",\"message\":\"You are in the Table Buckets Mode `-t` to clear the Table Buckets for S3 Tables. In this mode, operation across regions is not possible, but only in one region. You can specify the region with the `-r` option.\"}",
		},
		{
			name: "warn when vector buckets mode without region",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				VectorBucketsMode: true,
				Region:            "",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr:     "",
			expectedWarning: "{\"level\":\"warn\",\"message\":\"You are in the Vector Buckets Mode `-V` to clear the Vector Buckets for S3 Vectors. In this mode, operation across regions is not possible, but only in one region. You can specify the region with the `-r` option.\"}",
		},
		{
			name: "succeed with valid options - basic case",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - interactive mode",
			app: &App{
				InteractiveMode:   true,
				BucketNames:       cli.NewStringSlice(),
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - multiple bucket names",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1", "bucket2"),
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - bucket names with force mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				ForceMode:         true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - interactive mode with force mode",
			app: &App{
				InteractiveMode:   true,
				ForceMode:         true,
				BucketNames:       cli.NewStringSlice(),
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - table buckets mode with region",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				TableBucketsMode:  true,
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - bucket names with force mode and table buckets mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				ForceMode:         true,
				TableBucketsMode:  true,
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - interactive mode with force mode and table buckets mode",
			app: &App{
				InteractiveMode:   true,
				ForceMode:         true,
				TableBucketsMode:  true,
				BucketNames:       cli.NewStringSlice(),
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - directory buckets mode with region",
			app: &App{
				BucketNames:          cli.NewStringSlice("bucket1"),
				DirectoryBucketsMode: true,
				Region:               "us-east-1",
				ConcurrencyNumber:    UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - bucket names with force mode and directory buckets mode",
			app: &App{
				BucketNames:          cli.NewStringSlice("bucket1"),
				ForceMode:            true,
				DirectoryBucketsMode: true,
				Region:               "us-east-1",
				ConcurrencyNumber:    UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - interactive mode with force mode and directory buckets mode",
			app: &App{
				InteractiveMode:      true,
				ForceMode:            true,
				DirectoryBucketsMode: true,
				BucketNames:          cli.NewStringSlice(),
				Region:               "us-east-1",
				ConcurrencyNumber:    UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - bucket names with old versions only",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				OldVersionsOnly:   true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - interactive mode with old versions only",
			app: &App{
				InteractiveMode:   true,
				OldVersionsOnly:   true,
				BucketNames:       cli.NewStringSlice(),
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - bucket names with quiet mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				QuietMode:         true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - interactive mode with quiet mode",
			app: &App{
				InteractiveMode:   true,
				QuietMode:         true,
				BucketNames:       cli.NewStringSlice(),
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - bucket names with old versions only and quiet mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				OldVersionsOnly:   true,
				QuietMode:         true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - interactive mode with old versions only and quiet mode",
			app: &App{
				InteractiveMode:   true,
				OldVersionsOnly:   true,
				QuietMode:         true,
				BucketNames:       cli.NewStringSlice(),
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - bucket names with concurrent mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				ConcurrentMode:    true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - interactive mode with concurrent mode",
			app: &App{
				InteractiveMode:   true,
				ConcurrentMode:    true,
				BucketNames:       cli.NewStringSlice(),
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - bucket names with concurrent mode and concurrency number",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				ConcurrentMode:    true,
				ConcurrencyNumber: 2,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - interactive mode with concurrent mode and concurrency number",
			app: &App{
				InteractiveMode:   true,
				ConcurrentMode:    true,
				ConcurrencyNumber: 2,
				BucketNames:       cli.NewStringSlice(),
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - concurrency number can be 1",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				ConcurrentMode:    true,
				ConcurrencyNumber: 1,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - directory buckets mode with concurrent mode",
			app: &App{
				BucketNames:          cli.NewStringSlice("bucket1"),
				DirectoryBucketsMode: true,
				ConcurrentMode:       true,
				ConcurrencyNumber:    UnspecifiedConcurrencyNumber,
				Region:               "us-east-1",
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - key prefix with directory buckets mode",
			app: &App{
				BucketNames:          cli.NewStringSlice("bucket1"),
				DirectoryBucketsMode: true,
				KeyPrefix:            "test-prefix/",
				Region:               "us-east-1",
				ConcurrencyNumber:    UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - key prefix does not end with delimiter not in directory buckets mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				KeyPrefix:         "test-prefix",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - key prefix with interactive mode",
			app: &App{
				InteractiveMode:   true,
				KeyPrefix:         "prefix",
				BucketNames:       cli.NewStringSlice(),
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - key prefix with old versions only",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				OldVersionsOnly:   true,
				KeyPrefix:         "prefix",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - vector buckets mode with region",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				VectorBucketsMode: true,
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - bucket names with force mode and vector buckets mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				ForceMode:         true,
				VectorBucketsMode: true,
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - interactive mode with force mode and vector buckets mode",
			app: &App{
				InteractiveMode:   true,
				ForceMode:         true,
				VectorBucketsMode: true,
				BucketNames:       cli.NewStringSlice(),
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - vector buckets mode with concurrent mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				VectorBucketsMode: true,
				ConcurrentMode:    true,
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - vector buckets mode with key prefix",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				VectorBucketsMode: true,
				KeyPrefix:         "prefix",
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - endpoint URL with bucket names",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				EndpointUrl:       "https://custom.endpoint.com",
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - endpoint URL with interactive mode",
			app: &App{
				InteractiveMode:   true,
				EndpointUrl:       "https://custom.endpoint.com",
				Region:            "us-east-1",
				BucketNames:       cli.NewStringSlice(),
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - endpoint URL with force mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				EndpointUrl:       "https://custom.endpoint.com",
				ForceMode:         true,
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - endpoint URL with old versions only",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				EndpointUrl:       "https://custom.endpoint.com",
				OldVersionsOnly:   true,
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - endpoint URL with concurrent mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1", "bucket2"),
				EndpointUrl:       "https://custom.endpoint.com",
				ConcurrentMode:    true,
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - endpoint URL with key prefix",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				EndpointUrl:       "https://custom.endpoint.com",
				KeyPrefix:         "test-prefix/",
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - path style with endpoint URL",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				EndpointUrl:       "https://ceph.cluster.com",
				PathStyle:         true,
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - path style without endpoint URL",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				PathStyle:         true,
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - AWS S3 endpoint with directory buckets mode",
			app: &App{
				BucketNames:          cli.NewStringSlice("bucket1"),
				EndpointUrl:          "https://s3.us-west-2.amazonaws.com",
				DirectoryBucketsMode: true,
				Region:               "us-west-2",
				ConcurrencyNumber:    UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - AWS S3 endpoint with table buckets mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				EndpointUrl:       "https://s3.amazonaws.com",
				TableBucketsMode:  true,
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - AWS S3 endpoint with vector buckets mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				EndpointUrl:       "https://s3.eu-central-1.amazonaws.com",
				VectorBucketsMode: true,
				Region:            "eu-central-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - empty endpoint with directory buckets mode",
			app: &App{
				BucketNames:          cli.NewStringSlice("bucket1"),
				EndpointUrl:          "",
				DirectoryBucketsMode: true,
				Region:               "us-west-2",
				ConcurrencyNumber:    UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - empty endpoint with table buckets mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				EndpointUrl:       "",
				TableBucketsMode:  true,
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - empty endpoint with vector buckets mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				EndpointUrl:       "",
				VectorBucketsMode: true,
				Region:            "eu-central-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "error when Cloudflare R2 endpoint URL with old versions only",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				EndpointUrl:       "https://account.r2.cloudflarestorage.com",
				OldVersionsOnly:   true,
				Region:            "us-east-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: The -o option is not supported with Cloudflare R2.\n",
		},
		{
			name: "error when non-AWS S3 endpoint specified with directory buckets mode",
			app: &App{
				BucketNames:          cli.NewStringSlice("bucket1"),
				EndpointUrl:          "https://custom.endpoint.com",
				DirectoryBucketsMode: true,
				ConcurrencyNumber:    UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: Directory Buckets mode (-d) is not supported with non-AWS S3 endpoints.\n",
		},
		{
			name: "error when non-AWS S3 endpoint specified with table buckets mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				EndpointUrl:       "https://custom.endpoint.com",
				TableBucketsMode:  true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: Table Buckets mode (-t) is not supported with non-AWS S3 endpoints.\n",
		},
		{
			name: "error when non-AWS S3 endpoint specified with vector buckets mode",
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				EndpointUrl:       "https://custom.endpoint.com",
				VectorBucketsMode: true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "InvalidOptionError: Vector Buckets mode (-V) is not supported with non-AWS S3 endpoints.\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()

			originalKeyPrefix := tt.app.KeyPrefix
			err := tt.app.validateOptions()

			if tt.expectedErr != "" {
				assert.EqualError(t, err, tt.expectedErr)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectedWarning != "" {
				assert.Equal(t, tt.expectedWarning, buf.String()[:len(buf.String())-1])
			} else {
				assert.Empty(t, buf.String())
			}

			// Check if the delimiter is added automatically when the key prefix does not end with delimiter in directory buckets mode
			if originalKeyPrefix != "" && !strings.HasSuffix(originalKeyPrefix, "/") && tt.app.DirectoryBucketsMode {
				assert.True(t, strings.HasSuffix(tt.app.KeyPrefix, "/"))
			}
			// Check if the delimiter is NOT added automatically when the key prefix ends with delimiter in directory buckets mode
			if strings.HasSuffix(originalKeyPrefix, "/") && tt.app.DirectoryBucketsMode {
				assert.Equal(t, tt.app.KeyPrefix, originalKeyPrefix)
			}
			// Check if the delimiter is NOT added automatically when the key prefix is not specified
			if originalKeyPrefix == "" {
				assert.Equal(t, tt.app.KeyPrefix, "")
			}
		})
	}
}

func TestApp_getAction(t *testing.T) {
	tests := []struct {
		name                  string
		prepareMockFn         func(m *wrapper.MockIWrapper, ms *MockIBucketSelector, mp *MockIBucketProcessor)
		app                   *App
		wantErr               bool
		expectedErr           string
		expectedTargetBuckets []string
	}{
		{
			name: "successfully process buckets",
			prepareMockFn: func(m *wrapper.MockIWrapper, ms *MockIBucketSelector, mp *MockIBucketProcessor) {
				ms.EXPECT().SelectBuckets(gomock.Any()).Return([]string{"bucket1", "bucket2"}, true, nil)
				mp.EXPECT().Process(gomock.Any()).Return(nil)
			},
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1", "bucket2"),
				targetBuckets:     []string{},
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			wantErr:               false,
			expectedTargetBuckets: []string{"bucket1", "bucket2"},
		},
		{
			name: "error when select buckets fails",
			prepareMockFn: func(m *wrapper.MockIWrapper, ms *MockIBucketSelector, mp *MockIBucketProcessor) {
				ms.EXPECT().SelectBuckets(gomock.Any()).Return(nil, false, fmt.Errorf("SelectBucketsError"))
			},
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				targetBuckets:     []string{},
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			wantErr:               true,
			expectedErr:           "SelectBucketsError",
			expectedTargetBuckets: []string{},
		},
		{
			name: "no error when select buckets returns no continuation",
			prepareMockFn: func(m *wrapper.MockIWrapper, ms *MockIBucketSelector, mp *MockIBucketProcessor) {
				ms.EXPECT().SelectBuckets(gomock.Any()).Return(nil, false, nil)
			},
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				targetBuckets:     []string{},
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			wantErr:               false,
			expectedTargetBuckets: []string{},
		},
		{
			name: "error when process buckets fails",
			prepareMockFn: func(m *wrapper.MockIWrapper, ms *MockIBucketSelector, mp *MockIBucketProcessor) {
				ms.EXPECT().SelectBuckets(gomock.Any()).Return([]string{"bucket1"}, true, nil)
				mp.EXPECT().Process(gomock.Any()).Return(fmt.Errorf("ProcessError"))
			},
			app: &App{
				BucketNames:       cli.NewStringSlice("bucket1"),
				targetBuckets:     []string{},
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			wantErr:               true,
			expectedErr:           "ProcessError",
			expectedTargetBuckets: []string{"bucket1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockWrapper := wrapper.NewMockIWrapper(ctrl)
			mockSelector := NewMockIBucketSelector(ctrl)
			mockProcessor := NewMockIBucketProcessor(ctrl)

			// Set up the mocks before calling prepareMockFn
			tt.app.s3Wrapper = mockWrapper
			tt.app.bucketSelector = mockSelector
			tt.app.bucketProcessor = mockProcessor

			// Set up the mock expectations
			tt.prepareMockFn(mockWrapper, mockSelector, mockProcessor)

			action := tt.app.getAction()
			err := action(cli.NewContext(tt.app.Cli, &flag.FlagSet{}, nil))

			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				assert.EqualError(t, err, tt.expectedErr)
			}

			// Verify targetBuckets
			assert.Equal(t, tt.expectedTargetBuckets, tt.app.targetBuckets, "targetBuckets mismatch")
		})
	}
}
