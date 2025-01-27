package app

import (
	"bytes"
	"testing"

	"github.com/go-to-k/cls3/internal/io"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v2"
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
				Region:            "ap-northeast-1",
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
				Region:            "ap-northeast-1",
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
				Region:            "ap-northeast-1",
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			expectedErr: "",
		},
		{
			name: "succeed with valid options - directory buckets mode with region",
			app: &App{
				BucketNames:          cli.NewStringSlice("bucket1"),
				DirectoryBucketsMode: true,
				Region:               "ap-northeast-1",
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
				Region:               "ap-northeast-1",
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
				Region:               "ap-northeast-1",
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()

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
		})
	}
}

func Test_determineConcurrencyNumber(t *testing.T) {
	tests := []struct {
		name           string
		app            *App
		expectedNumber int
	}{
		{
			name: "return 1 when concurrent mode is off",
			app: &App{
				ConcurrentMode:    false,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
				targetBuckets:     []string{"bucket1", "bucket2"},
			},
			expectedNumber: 1,
		},
		{
			name: "return number of target buckets when concurrency number is not specified",
			app: &App{
				ConcurrentMode:    true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
				targetBuckets:     []string{"bucket1", "bucket2", "bucket3"},
			},
			expectedNumber: 3,
		},
		{
			name: "return specified concurrency number when set",
			app: &App{
				ConcurrentMode:    true,
				ConcurrencyNumber: 2,
				targetBuckets:     []string{"bucket1", "bucket2", "bucket3"},
			},
			expectedNumber: 2,
		},
		{
			name: "return 1 when concurrent mode is off regardless of concurrency number",
			app: &App{
				ConcurrentMode:    false,
				ConcurrencyNumber: 2,
				targetBuckets:     []string{"bucket1", "bucket2"},
			},
			expectedNumber: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.app.determineConcurrencyNumber()
			assert.Equal(t, tt.expectedNumber, result)
		})
	}
}
