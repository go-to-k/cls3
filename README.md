# cls3

[![Go Report Card](https://goreportcard.com/badge/github.com/go-to-k/cls3)](https://goreportcard.com/report/github.com/go-to-k/cls3) ![GitHub](https://img.shields.io/github/license/go-to-k/cls3) ![GitHub](https://img.shields.io/github/v/release/go-to-k/cls3) [![ci](https://github.com/go-to-k/cls3/actions/workflows/ci.yml/badge.svg)](https://github.com/go-to-k/cls3/actions/workflows/ci.yml)

The description in **English** is available on the following blog page. -> [Blog](https://dev.to/aws-builders/tool-for-fast-deletion-and-emptying-of-s3-buckets-versioning-supported-6dn)

The description in **Japanese** is available on the following blog page. -> [Blog](https://go-to-k.hatenablog.com/entry/cls3)

## What is

The CLI tool **"cls3"** is to <ins>**CL**</ins>ear Amazon <ins>**S3**</ins> Buckets(AWS).

It <ins>**empties**</ins> (so **deletes all objects and versions/delete-markers** in) S3 Buckets **or** <ins>**deletes**</ins> the buckets **themselves**.

This tool allows you to **search for bucket names** and empty or delete **multiple buckets**.

## Install

- Homebrew
  ```
  brew install go-to-k/tap/cls3
  ```
- Binary
  - [Releases](https://github.com/go-to-k/cls3/releases)
- Git Clone and install(for developers)
  ```
  git clone https://github.com/go-to-k/cls3.git
  cd cls3
  make install
  ```

## How to use
  ```
  cls3 -b <bucketName> [-b <bucketName>] [-p <profile>] [-r <region>] [-f|--force] [-i|--interactive]
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
  - It is not necessary to be aware of this as it can be used **across regions**.
- -f, --force: optional
  - ForceMode (Delete the bucket together)
- -i, --interactive: optional
  - Interactive Mode for buckets selection

## Interactive Mode

### BucketName Selection

In the interactive mode(`-i` option), you can search bucket names and select buckets.

It is designed to be searchable and deletable **across regions**, so it can be used **without being region-aware**.

It can be **empty**.

```sh
❯ cls3 -i
Filter a keyword of bucket names: test-goto
```

Then you select bucket names in the UI.

```sh
? Select buckets.
  [Use arrows to move, space to select, <right> to all, <left> to none, type to filter]
> [x]  test-goto-bucket-1
  [ ]  test-goto-bucket-2
  [x]  test-goto-bucket-3
```
