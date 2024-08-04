# cls3

[![Go Report Card](https://goreportcard.com/badge/github.com/go-to-k/cls3)](https://goreportcard.com/report/github.com/go-to-k/cls3) ![GitHub](https://img.shields.io/github/license/go-to-k/cls3) ![GitHub](https://img.shields.io/github/v/release/go-to-k/cls3) [![ci](https://github.com/go-to-k/cls3/actions/workflows/ci.yml/badge.svg)](https://github.com/go-to-k/cls3/actions/workflows/ci.yml)

The description in **English** is available on the following page. -> [Blog](https://dev.to/aws-builders/tool-for-fast-deletion-and-emptying-of-s3-buckets-versioning-supported-6dn)

The description in **Japanese** is available on the following page. -> [Blog](https://go-to-k.hatenablog.com/entry/cls3)

The description in **Spanish** is available on the following page. -> [Blog](https://dev.to/aws-espanol/cls3-busqueda-y-eliminacion-masiva-de-buckets-s3-1gb)

## What is

The CLI tool **"cls3"** is to <ins>**CL**</ins>ear Amazon <ins>**S3**</ins> Buckets(AWS).

It <ins>**empties**</ins> (so **deletes all objects and versions/delete-markers** in) S3 Buckets **or** <ins>**deletes**</ins> the buckets **themselves**.

This tool allows you to **search for bucket names** and empty or delete **multiple buckets**.

## Features

### Bucket deletion option

Initially, the tool was intended to "empty the bucket", but since I was going to go through the trouble, I also added an option (`-f|--force`) to **"delete the bucket as well"**.

### Search for bucket names and delete multiple buckets

As described below ([Interactive Mode](#interactive-mode)), you can **search for bucket names** and delete or empty **multiple buckets at once**.

### Cross-region

In deleting multiple buckets, you can list and delete them all at once, even if they are in **multiple regions**.

(In the **Directory Buckets** Mode for **S3 Express One Zone** (`-d` option), operation across regions is not possible, but only in **one region**. You can specify the region with the `-r` option.)

### Deletion of buckets with versioning enabled

**Even if versioning is turned on**, you can empty it just as if it were turned off. Therefore, you can use it **without** being aware of the versioning settings.

### Deletion of old version objects only

The `-o | --oldVersionsOnly` option allows you to **delete only old versions** and all delete-markers **without new versions and a bucket itself**.

**So you can retain the latest version objects only.**

This option cannot be specified with the `-f | --force` option.

### Deletion of Directory Buckets for S3 Express One Zone

The `-d | --directoryBucketsMode` option allows you to delete **the Directory Buckets** for **S3 Express One Zone**.

In this mode, operation across regions is not possible, but only in **one region**. You can specify the region with the `-r` option.

### Number of objects that can be deleted

The delete-objects API provided by the CLI and SDK has a limit of **"the number of objects that can be deleted in one command is limited to 1000"**, but **This tool has no limit on the number**.

### Retry on 503 error

When there are tens of thousands of objects, a SlowDown error (503 error) may occur on the S3 api side in rare cases when deleting them all at once using the CLI or SDK.

When this occurs, cls3 responds by adding a mechanism that waits a few seconds and retries automatically several times.

## Install

- Homebrew

  ```bash
  brew install go-to-k/tap/cls3
  ```

- Linux, Darwin (macOS) and Windows

  ```bash
  curl -fsSL https://raw.githubusercontent.com/go-to-k/cls3/main/install.sh | sh
  cls3 -h

  # To install a specific version of cls3
  # e.g. version 0.13.2
  curl -fsSL https://raw.githubusercontent.com/go-to-k/cls3/main/install.sh | sh -s "v0.13.2"
  cls3 -h
  ```

- aqua

  ```bash
  aqua g -i go-to-k/cls3
  ```

- Binary
  - [Releases](https://github.com/go-to-k/cls3/releases)
- Git Clone and install(for developers)

  ```bash
  git clone https://github.com/go-to-k/cls3.git
  cd cls3
  make install
  ```

## How to use

  ```bash
  cls3 -b <bucketName> [-b <bucketName>] [-p <profile>] [-r <region>] [-f|--force] [-i|--interactive] [-o|--oldVersionsOnly] [-q|--quietMode] [-d|--directoryBucketsMode]
  ```

- -b, --bucketName: optional
  - Bucket name
    - Must be specified in **not** interactive mode
    - Otherwise (so in the interactive mode), you **can not** specify this!
  - **Multiple specifications are possible.**
    - `cls3 -b test1 -b test2`
- -p, --profile: optional
  - AWS profile name
- -r, --region: optional(default: `ap-northeast-1`)
  - AWS Region
    - If this option is not specified and your AWS profile is tied to a region, the region is used instead of the default region.
  - It is not necessary to be aware of this as it can be used **across regions**.
    - But in the **Directory Buckets** Mode for **S3 Express One Zone** (with `-d` option), you should specify the region. The mode is not available across regions.
- -f, --force: optional
  - ForceMode (Delete the bucket together)
- -i, --interactive: optional
  - Interactive Mode for buckets selection
- -o, --oldVersionsOnly: optional
  - Delete old version objects only (including all delete-markers)
  - Do not specify the `-f` option if you specify this option.
- -q, --quietMode: optional
  - Hide live display of number of deletions
  - It would be good to use in CI
- -d, --directoryBucketsMode: optional
  - Directory Buckets Mode for S3 Express One Zone
  - Operation across regions is not possible, but only in one region.
  - You can specify the region with the `-r` option.

## Interactive Mode

### BucketName Selection

In the interactive mode(`-i` option), you can search bucket names and select buckets.

It is designed to be searchable and deletable **across regions**, so it can be used **without being region-aware**.

It can be **empty**.

```bash
❯ cls3 -i
Filter a keyword of bucket names: test-goto
```

Then you select bucket names in the UI.

```bash
? Select buckets.
  [Use arrows to move, space to select, <right> to all, <left> to none, type to filter]
> [x]  test-goto-bucket-1
  [ ]  test-goto-bucket-2
  [x]  test-goto-bucket-3
```

## GitHub Actions

You can use cls3 in GitHub Actions Workflow.

The "quiet" allows you to hive live display of number of deletions (**default: true in GitHub Actions ONLY**).

Basically, you do not need to specify a "region" parameter, since you can delete buckets across regions. However,
in Directory Buckets mode (`directory-buckets-mode`) for S3 Express One Zone, the region must be specified. This mode cannot
be used across regions.

To delete multiple buckets, specify bucket names separated by commas.

```yaml
jobs:
  cls3:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v3
        with:
          role-to-assume: arn:aws:iam::123456789100:role/my-github-actions-role
          # Or specify access keys.
          # aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          # aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1
      - name: Delete bucket
        uses: go-to-k/cls3@main # Or specify the version instead of main
        with:
          bucket-name: YourBucket
          # bucket-name: YourBucket1, YourBucket2, YourBucket3 # To delete multiple buckets
          force: true # Whether to delete the bucket itself, not just the object (default: false)
          quiet: false # Hide live display of number of deletions (default: true in GitHub Actions ONLY.)
          old-versions-only: false # Delete old version objects only (including all delete-markers) (default: false)
          directory-buckets-mode: false # Directory Buckets Mode for S3 Express One Zone (default: false)
          region: us-east-1 # Specify the region in the Directory Buckets Mode for S3 Express One Zone
```

You can also run raw commands after installing the cls3 binary.

```yaml
jobs:
  cls3:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v3
        with:
          role-to-assume: arn:aws:iam::123456789100:role/my-github-actions-role
          # Or specify access keys.
          # aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          # aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1
      - name: Install cls3
        uses: go-to-k/cls3@main # Or specify the version instead of main
      - name: Run cls3
        run: |
          echo "cls3"
          cls3 -v
          cls3 -b YourBucket1 -b YourBucket2
```
