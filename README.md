# cls3

[![Go Report Card](https://goreportcard.com/badge/github.com/go-to-k/cls3)](https://goreportcard.com/report/github.com/go-to-k/cls3) ![GitHub](https://img.shields.io/github/license/go-to-k/cls3) ![GitHub](https://img.shields.io/github/v/release/go-to-k/cls3)

The description in **Japanese** is available on the following blog page. -> [Blog](https://go-to-k.hatenablog.com/entry/cls3)

## What is

CLI tool to clear (so delete all objects and versions/delete-markers in) Amazon S3 Buckets(AWS). The bucket itself can also be deleted by the option.

## Install

- Homebrew
  ```
  brew tap go-to-k/tap
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
  cls3 -b <bucketName> [-p <profile>] [-r <region>] [-f|--force]
  ```

- -b, --bucketName: **required**
  - Bucket name
- -p, --profile: optional
  - AWS profile name
- -r, --region: optional(default: `ap-northeast-1`)
  - AWS Region
- -f, --force: optional
  - ForceMode (Delete the bucket together)