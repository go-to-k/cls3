# cls3 Test Data Generation Tool

This directory contains Go programs for generating test data for various types of AWS S3 buckets. These scripts create AWS resources, which may incur costs when executed.

## Features

The tool supports generating test data for the following three bucket types:

1. **Standard S3 Buckets** - Versioning-enabled test data generation
2. **S3 Express One Zone (Directory Buckets)** - Test data for directory buckets
3. **S3 Tables** - Test data for S3 tables

## Prerequisites

- Go 1.23 or later
- AWS account and appropriate permissions
- AWS credentials properly configured (e.g., in ~/.aws/credentials)

## Usage

You can run each script from the project root directory as follows:

```bash
# Standard S3 bucket
make testgen_general OPT="-b bucket-prefix -n 5 -o 1000"

# S3 Express One Zone (Directory bucket)
make testgen_directory OPT="-b bucket-prefix -n 2 -o 500"

# S3 table
make testgen_table OPT="-b bucket-prefix -n 1 -t 50 -s 20 -r us-west-2"
```

To display help:

```bash
make testgen_help
```

### Command Options

#### Standard S3 Bucket
- `-p` - AWS profile name (optional)
- `-b` - Bucket name prefix (required)
- `-n` - Number of buckets to create (default: 10)
- `-o` - Number of objects per bucket (default: 10000)

#### S3 Express One Zone (Directory Bucket)
- `-p` - AWS profile name (optional)
- `-b` - Bucket name prefix (required)
- `-n` - Number of buckets to create (default: 10, max: 100)
- `-o` - Number of objects per bucket (default: 10000)

#### S3 Table
- `-p` - AWS profile name (optional)
- `-b` - Bucket name prefix (required)
- `-n` - Number of buckets to create (default: 1, max: 10)
- `-t` - Number of tables per namespace (default: 100)
- `-s` - Number of namespaces per table (default: 100)
- `-r` - AWS region (default: us-east-1)

### Cleanup

To remove test files:

```bash
make testgen_clean
```
