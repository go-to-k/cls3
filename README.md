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

### Delete bucket option

Initially, the tool was intended to "empty the bucket", but since I was going to go through the trouble, I also added an option (`-f|--force`) to **"delete the bucket as well"**.

### Search for bucket names and delete multiple buckets

As described below ([Interactive Mode](#interactive-mode)), you can **search for bucket names** and delete or empty **multiple buckets at once**.

### Cross-region

In deleting multiple buckets, you can delete them all at once, even if they are in **multiple regions**.

### Versioning

**Even if versioning is turned on**, you can empty it just as if it were turned off. Therefore, you can use it **without** being aware of the versioning settings.

### Delete Old Version Objects Only

The `-o | --oldObjectsOnly` option allows you to **delete only old versions** and all delete-markers without new versions and a bucket itself.

**So you can retain the latest version objects only.**

This option cannot be specified with the `-f | --force` option.

### Number of objects that can be deleted

The delete-objects API provided by the CLI and SDK has a limit of **"the number of objects that can be deleted in one command is limited to 1000"**, but **This tool has no limit on the number**.

### Parallel Processing

When there are more than 1000 object versions, object deletion is performed in **Parallel Processing**.

As mentioned above, up to 1,000 objects can be deleted at once using the SDK's 1 API, so each unit is executed in parallel.

Therefore, it is quite fast.

As a test, I prepared two buckets with the same 10,000 objects (very small in size) and tried deleting them with the "Empty" button on the console and the tool I created (local environment/logical CPU core count 8) respectively, and it took **22 seconds on the console and 8 seconds** with this tool. That is **3 times faster** than the console.

Also, with **10 million objects (including version and delete markers)**, the deletion was completed in **about 100 minutes**. (This depends on the operating environment, so I can't make any guarantees.)

### Retry on 503 error

When there are tens of thousands of objects, a SlowDown error (503 error) may occur on the S3 api side in rare cases when deleting them all at once using the CLI or SDK.

When this occurs, cls3 responds by adding a mechanism that waits a few seconds and retries automatically several times.

### Progress bar

A progress bar will appear by default. This gives you the following information

- Number of all objects
- Number of objects deleted
- Estimated completion time

```bash
❯ cls3 -i
Filter a keyword of bucket names: testgoto

? Select buckets.
 testgoto1
OK? (Y/n)

INF testgoto1 Checking...
INF testgoto1 Clearing...
  52% |██████████████████████████                        | (12000/22882) [4s:4s]
```

`-q | --quiet` option to hide the progress bar.

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
  cls3 -b <bucketName> [-b <bucketName>] [-p <profile>] [-r <region>] [-f|--force] [-i|--interactive] [-q|--quiet] [-o|--oldObjectsOnly]
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
- -q, --quiet: optional
  - Not to display a progress bar
- -o, --oldObjectsOnly: optional
  - Delete old version objects only (including all delete-markers)
  - Do not specify the `-f` option if you specify this option.

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
