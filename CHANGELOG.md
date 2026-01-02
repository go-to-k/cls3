# Changelog

## [v0.32.1](https://github.com/go-to-k/cls3/compare/v0.32.0...v0.32.1) - 2026-01-02
- chore: fix mismatch for version number in changelog by @go-to-k in https://github.com/go-to-k/cls3/pull/421
- fix: deserialization failed by @go-to-k in https://github.com/go-to-k/cls3/pull/424

## [v0.32.0](https://github.com/go-to-k/cls3/compare/v0.31.2...v0.32.0) - 2026-01-01
- fix: mismatch for version number in changelog by @go-to-k in https://github.com/go-to-k/cls3/pull/418

## [v0.31.2](https://github.com/go-to-k/cls3/compare/v0.31.1...v0.31.2) - 2026-01-01
- feat: support path-style URL by @go-to-k in https://github.com/go-to-k/cls3/pull/416

## [v0.31.1](https://github.com/go-to-k/cls3/compare/v0.31.0...v0.31.1) - 2025-11-29
- chore: change go and bubbletea version by @go-to-k in https://github.com/go-to-k/cls3/pull/406

## [v0.31.0](https://github.com/go-to-k/cls3/compare/v0.30.1...v0.31.0) - 2025-11-29
- chore: change reviewdog settings in CI by @go-to-k in https://github.com/go-to-k/cls3/pull/402
- chore(deps): bump actions/checkout from 5 to 6 by @dependabot[bot] in https://github.com/go-to-k/cls3/pull/404
- chore(deps): bump actions/upload-artifact from 4 to 5 by @dependabot[bot] in https://github.com/go-to-k/cls3/pull/400
- chore(deps): bump go.uber.org/mock from 0.4.0 to 0.6.0 by @dependabot[bot] in https://github.com/go-to-k/cls3/pull/393
- feat: support `aws login` by @go-to-k in https://github.com/go-to-k/cls3/pull/405
- chore(deps): bump github.com/rs/zerolog from 1.33.0 to 1.34.0 by @dependabot[bot] in https://github.com/go-to-k/cls3/pull/391

## [v0.30.1](https://github.com/go-to-k/cls3/compare/v0.30.0...v0.30.1) - 2025-10-15
- ci: enable immutable releases by @go-to-k in https://github.com/go-to-k/cls3/pull/395

## [v0.30.0](https://github.com/go-to-k/cls3/compare/v0.29.0...v0.30.0) - 2025-10-02
- feat: change default region to us-east-1 by @go-to-k in https://github.com/go-to-k/cls3/pull/385
- chore(deps): bump actions/checkout from 4 to 5 by @dependabot[bot] in https://github.com/go-to-k/cls3/pull/372
- chore(deps): bump actions/setup-go from 5 to 6 by @dependabot[bot] in https://github.com/go-to-k/cls3/pull/375
- ci: add 'endpoint' to scopes in PR title lint by @go-to-k in https://github.com/go-to-k/cls3/pull/387
- chore(deps): bump amannn/action-semantic-pull-request from 5 to 6 by @dependabot[bot] in https://github.com/go-to-k/cls3/pull/374
- chore(deps): bump github.com/charmbracelet/bubbletea from 1.1.1 to 1.3.10 by @dependabot[bot] in https://github.com/go-to-k/cls3/pull/373
- revert: "chore(deps): bump github.com/charmbracelet/bubbletea (#373)" by @go-to-k in https://github.com/go-to-k/cls3/pull/388
- chore(deps): bump github.com/aws/aws-sdk-go-v2/service/s3vectors from 1.0.1 to 1.4.8 by @dependabot[bot] in https://github.com/go-to-k/cls3/pull/380

## [v0.29.0](https://github.com/go-to-k/cls3/compare/v0.28.1...v0.29.0) - 2025-09-24
- feat: allow S3 endpoint to use -d, -t and -V options by @go-to-k in https://github.com/go-to-k/cls3/pull/370

## [v0.28.1](https://github.com/go-to-k/cls3/compare/v0.28.0...v0.28.1) - 2025-09-23
- chore: remove warning if specifying -o and -e by @go-to-k in https://github.com/go-to-k/cls3/pull/367
- chore(deps): bump golang.org/x/sync from 0.11.0 to 0.14.0 by @dependabot[bot] in https://github.com/go-to-k/cls3/pull/355
- revert: chore(deps): bump golang.org/x/sync from 0.11.0 to 0.14.0 (#355) by @go-to-k in https://github.com/go-to-k/cls3/pull/369

## [v0.28.0](https://github.com/go-to-k/cls3/compare/v0.27.1...v0.28.0) - 2025-09-23
- test: modify validation in testdata script for vector by @go-to-k in https://github.com/go-to-k/cls3/pull/361
- test(version): comment out version tests by @go-to-k in https://github.com/go-to-k/cls3/pull/365
- feat: custom endpoint url by @go-to-k in https://github.com/go-to-k/cls3/pull/366

## [v0.27.1](https://github.com/go-to-k/cls3/compare/v0.27.0...v0.27.1) - 2025-07-29
- chore: fix .gitignore for testdata by @go-to-k in https://github.com/go-to-k/cls3/pull/358
- refactor: early return in wrapper by @go-to-k in https://github.com/go-to-k/cls3/pull/360

## [v0.27.0](https://github.com/go-to-k/cls3/compare/v0.26.0...v0.27.0) - 2025-07-29
- feat: support S3 Vectors by @go-to-k in https://github.com/go-to-k/cls3/pull/356

## [v0.26.0](https://github.com/go-to-k/cls3/compare/v0.25.4...v0.26.0) - 2025-04-15
- feat(app): add `-k | --keyPrefix` option to delete objects with a specific key prefix by @go-to-k in https://github.com/go-to-k/cls3/pull/350

## [v0.25.4](https://github.com/go-to-k/cls3/compare/v0.25.3...v0.25.4) - 2025-04-13
- test: create deploy scripts for tests with golang instead of shell by @go-to-k in https://github.com/go-to-k/cls3/pull/345
- test: increase number of retries and waiting time in test script by @go-to-k in https://github.com/go-to-k/cls3/pull/347
- revert: "refactor(wrapper): improve object deletion speed  (#342)" by @go-to-k in https://github.com/go-to-k/cls3/pull/348

## [v0.25.3](https://github.com/go-to-k/cls3/compare/v0.25.2...v0.25.3) - 2025-04-11
- chore: migrate golangci to v2 by @go-to-k in https://github.com/go-to-k/cls3/pull/336
- refactor(wrapper): improve object deletion speed  by @go-to-k in https://github.com/go-to-k/cls3/pull/342

## [v0.25.2](https://github.com/go-to-k/cls3/compare/v0.25.1...v0.25.2) - 2025-02-07
- fix: failed in ListObjectVersions with an error "Please try again" when so many buckets are specified by @go-to-k in https://github.com/go-to-k/cls3/pull/322

## [v0.25.1](https://github.com/go-to-k/cls3/compare/v0.25.0...v0.25.1) - 2025-02-06
- docs: update gif in README by @go-to-k in https://github.com/go-to-k/cls3/pull/312
- docs: change gif image in README by @go-to-k in https://github.com/go-to-k/cls3/pull/314
- refactor: avoid making app module dependent on client module by @go-to-k in https://github.com/go-to-k/cls3/pull/317
- chore(deps): bump github.com/aws/aws-sdk-go-v2/service/s3tables from 1.0.4 to 1.1.1 by @dependabot in https://github.com/go-to-k/cls3/pull/310
- chore(deps): bump golang.org/x/sync from 0.10.0 to 0.11.0 by @dependabot in https://github.com/go-to-k/cls3/pull/315
- chore(deps): bump github.com/stretchr/testify from 1.8.0 to 1.10.0 by @dependabot in https://github.com/go-to-k/cls3/pull/298

## [v0.25.0](https://github.com/go-to-k/cls3/compare/v0.24.2...v0.25.0) - 2025-02-03
- test: create tables with schema in deploy_table.sh by @go-to-k in https://github.com/go-to-k/cls3/pull/307
- feat(app): concurrent mode for parallel deletion of multiple buckets by @go-to-k in https://github.com/go-to-k/cls3/pull/288

## [v0.24.2](https://github.com/go-to-k/cls3/compare/v0.24.1...v0.24.2) - 2025-01-29
- chore(wrapper): change handling errgroup with context by @go-to-k in https://github.com/go-to-k/cls3/pull/289
- revert: "chore(wrapper): change handling errgroup with context (#289)" by @go-to-k in https://github.com/go-to-k/cls3/pull/292
- chore(client): add debug log for retryable errors by @go-to-k in https://github.com/go-to-k/cls3/pull/293
- test: improve test deploy shells by @go-to-k in https://github.com/go-to-k/cls3/pull/294
- test: increase the number of buckets created in deploy_directory.sh by @go-to-k in https://github.com/go-to-k/cls3/pull/295
- test(app): add tests for app module by @go-to-k in https://github.com/go-to-k/cls3/pull/296
- test: wait for asynchronous loops in the test deployment shells for each constant process by @go-to-k in https://github.com/go-to-k/cls3/pull/300
- test: fix loop handling in deploy.sh by @go-to-k in https://github.com/go-to-k/cls3/pull/301
- fix(wrapper): S3 Tables Wrapper is loading without waiting for the addition process of deletedTablesCount by @go-to-k in https://github.com/go-to-k/cls3/pull/302

## [v0.24.1](https://github.com/go-to-k/cls3/compare/v0.24.0...v0.24.1) - 2025-01-22
- refactor(wrapper): use atomic instead of mutex for deletion counts in S3 wrapper by @go-to-k in https://github.com/go-to-k/cls3/pull/285
- chore(deps): bump golang.org/x/sync from 0.8.0 to 0.10.0 by @dependabot in https://github.com/go-to-k/cls3/pull/268
- chore(deps): bump github.com/fatih/color from 1.17.0 to 1.18.0 by @dependabot in https://github.com/go-to-k/cls3/pull/255
- chore: handle loop cancel when context done by @go-to-k in https://github.com/go-to-k/cls3/pull/286

## [v0.24.0](https://github.com/go-to-k/cls3/compare/v0.23.0...v0.24.0) - 2025-01-21
- docs: add command option sample for -t in README by @go-to-k in https://github.com/go-to-k/cls3/pull/280
- fix(wrapper): TooManyRequestsException often occurs by @go-to-k in https://github.com/go-to-k/cls3/pull/283
- feat(wrapper): count the number of table deletions more finely in the s3 tables wrapper by @go-to-k in https://github.com/go-to-k/cls3/pull/282
- feat(client): handle ContinuationToken for ListBuckets by @go-to-k in https://github.com/go-to-k/cls3/pull/284

## [v0.23.0](https://github.com/go-to-k/cls3/compare/v0.22.0...v0.23.0) - 2025-01-20
- docs: add how to install with asdf in README by @go-to-k in https://github.com/go-to-k/cls3/pull/271
- docs: add gif in README by @go-to-k in https://github.com/go-to-k/cls3/pull/277
- feat(app): add Table Buckets Mode for clearing Table Buckets of S3 Tables by @go-to-k in https://github.com/go-to-k/cls3/pull/279

## [v0.22.0](https://github.com/go-to-k/cls3/compare/v0.21.0...v0.22.0) - 2024-10-19
- ci: change PR label names for release by @go-to-k in https://github.com/go-to-k/cls3/pull/248
- docs: improve style for README by @go-to-k in https://github.com/go-to-k/cls3/pull/247
- feat: check for existence of buckets early by @go-to-k in https://github.com/go-to-k/cls3/pull/251
- chore(deps): bump github.com/charmbracelet/bubbletea from 0.27.0 to 1.1.1 by @go-to-k in https://github.com/go-to-k/cls3/pull/253

## [v0.21.0](https://github.com/go-to-k/cls3/compare/v0.20.5...v0.21.0) - 2024-08-27
- ci: tweak for pr-lint by @go-to-k in https://github.com/go-to-k/cls3/pull/208
- ci: Manage labels in PR lint by @go-to-k in https://github.com/go-to-k/cls3/pull/210
- ci: tweak for semantic-pull-request workflow by @go-to-k in https://github.com/go-to-k/cls3/pull/211
- ci: fix bug that labels are not created by @go-to-k in https://github.com/go-to-k/cls3/pull/212
- ci: ignore lint on tagpr PR by @go-to-k in https://github.com/go-to-k/cls3/pull/213
- ci: add revert type in prlint by @go-to-k in https://github.com/go-to-k/cls3/pull/215
- ci: change token for tagpr by @go-to-k in https://github.com/go-to-k/cls3/pull/218
- ci: don't run CI in PR actions by @go-to-k in https://github.com/go-to-k/cls3/pull/219
- ci: add error linters by @go-to-k in https://github.com/go-to-k/cls3/pull/216
- ci: change token for tagpr by @go-to-k in https://github.com/go-to-k/cls3/pull/221
- feat(io): redesign UI implementation with a new library by @go-to-k in https://github.com/go-to-k/cls3/pull/214

## [v0.20.5](https://github.com/go-to-k/cls3/compare/v0.20.4...v0.20.5) - 2024-08-16
- ci(deps): upgrade to goreleaser-action@v6 by @go-to-k in https://github.com/go-to-k/cls3/pull/204
- ci: PR-Lint for PR titles by @go-to-k in https://github.com/go-to-k/cls3/pull/206
- ci: add main scope in PR-Lint by @go-to-k in https://github.com/go-to-k/cls3/pull/207

## [v0.20.3](https://github.com/go-to-k/cls3/compare/v0.20.2...v0.20.3) - 2024-08-16
- chore: use math/rand/v2 by @go-to-k in https://github.com/go-to-k/cls3/pull/197
- chore: use new gomock by @go-to-k in https://github.com/go-to-k/cls3/pull/198
- ci: add linter by @go-to-k in https://github.com/go-to-k/cls3/pull/199
- ci: use tagpr by @go-to-k in https://github.com/go-to-k/cls3/pull/200

## [v0.20.2](https://github.com/go-to-k/cls3/compare/v0.20.1...v0.20.2) - 2024-08-15
- chore(deps): bump goreleaser/goreleaser-action from 5 to 6 by @dependabot in https://github.com/go-to-k/cls3/pull/177
- chore(deps): bump golang.org/x/sync from 0.5.0 to 0.8.0 by @dependabot in https://github.com/go-to-k/cls3/pull/190
- chore(deps): bump github.com/urfave/cli/v2 from 2.25.0 to 2.27.4 by @dependabot in https://github.com/go-to-k/cls3/pull/193
- fix: DeleteBucket error on Directory Buckets by @go-to-k in https://github.com/go-to-k/cls3/pull/195

## [v0.20.1](https://github.com/go-to-k/cls3/compare/v0.20.0...v0.20.1) - 2024-08-07
- test: do not finish even in the event of errors in testdata/deploy_directory.sh by @go-to-k in https://github.com/go-to-k/cls3/pull/191
- refactor: add directory buckets mode property in client object by @go-to-k in https://github.com/go-to-k/cls3/pull/192

## [v0.20.0](https://github.com/go-to-k/cls3/compare/v0.19.0...v0.20.0) - 2024-08-04
- feat(app): add Directory Mode for clearing Directory Buckets for S3 Express One Zone by @go-to-k in https://github.com/go-to-k/cls3/pull/189

## [v0.19.0](https://github.com/go-to-k/cls3/compare/v0.18.0...v0.19.0) - 2024-08-03
- docs: add description for quiet mode in GitHub Actions by @go-to-k in https://github.com/go-to-k/cls3/pull/186
- feat(client): remove ListObjectVersions method by @go-to-k in https://github.com/go-to-k/cls3/pull/188

## [v0.18.0](https://github.com/go-to-k/cls3/compare/v0.17.0...v0.18.0) - 2024-08-01
- fix(deps): cannot start on IMDS-v2 credentials by @go-to-k in https://github.com/go-to-k/cls3/pull/181
- feat: add GitHub custom actions by @go-to-k in https://github.com/go-to-k/cls3/pull/183
- feat(app): quiet mode option to hide live display of number of deletions by @go-to-k in https://github.com/go-to-k/cls3/pull/184
- test: increase the number of buckets in deploy.sh for test data by 10 times by @go-to-k in https://github.com/go-to-k/cls3/pull/185
- fix(client): retry is not executed on APIs other than DeleteObjects by @go-to-k in https://github.com/go-to-k/cls3/pull/180

## [v0.17.0](https://github.com/go-to-k/cls3/compare/v0.16.0...v0.17.0) - 2024-07-20
- chore: change config of brews in .goreleaser.yaml by @go-to-k in https://github.com/go-to-k/cls3/pull/169
- feat(wrapper): output the number of objects with an error by @go-to-k in https://github.com/go-to-k/cls3/pull/170
- chore(deps): bump github.com/aws/aws-sdk-go-v2 from 1.23.5 to 1.25.2 by @dependabot in https://github.com/go-to-k/cls3/pull/163
- chore(deps): bump actions/cache from 3 to 4 by @dependabot in https://github.com/go-to-k/cls3/pull/158
- chore(deps): bump actions/upload-artifact from 3 to 4 by @dependabot in https://github.com/go-to-k/cls3/pull/147
- chore(deps): bump go.uber.org/goleak from 1.2.1 to 1.3.0 by @dependabot in https://github.com/go-to-k/cls3/pull/128
- feat(client): retry internal retryable errors by @go-to-k in https://github.com/go-to-k/cls3/pull/171

## [v0.16.0](https://github.com/go-to-k/cls3/compare/v0.15.0...v0.16.0) - 2024-04-15
- chore: add PR template by @go-to-k in https://github.com/go-to-k/cls3/pull/150
- docs: aqua install in README by @go-to-k in https://github.com/go-to-k/cls3/pull/154
- test(testdata): separate testdata dir into buckets and increase number of objects by @go-to-k in https://github.com/go-to-k/cls3/pull/165
- test(testdata): change test logic with background and amount of versions by @go-to-k in https://github.com/go-to-k/cls3/pull/166
- feat: BREAKING CHANGE: deletion logic by each page and removing the progress bar and `-q` option by @go-to-k in https://github.com/go-to-k/cls3/pull/168

## [v0.15.0](https://github.com/go-to-k/cls3/compare/v0.14.0...v0.15.0) - 2023-12-22
- feat(io): keep filter for bucket selection active in interactive mode by @go-to-k in https://github.com/go-to-k/cls3/pull/148

## [v0.14.0](https://github.com/go-to-k/cls3/compare/v0.13.2...v0.14.0) - 2023-12-12
- feat(app): add OldObjectsOnly option by @go-to-k in https://github.com/go-to-k/cls3/pull/146

## [v0.13.2](https://github.com/go-to-k/cls3/compare/v0.13.1...v0.13.2) - 2023-12-07
- docs: change sample code in README by @go-to-k in https://github.com/go-to-k/cls3/pull/143

## [v0.13.1](https://github.com/go-to-k/cls3/compare/v0.13.0...v0.13.1) - 2023-12-07
- docs: improve style in README by @go-to-k in https://github.com/go-to-k/cls3/pull/142

## [v0.13.0](https://github.com/go-to-k/cls3/compare/v0.12.1...v0.13.0) - 2023-12-07
- chore(deps): bump actions/setup-go from 4 to 5 by @dependabot in https://github.com/go-to-k/cls3/pull/139
- feat(install): Use Script Install by @watany-dev in https://github.com/go-to-k/cls3/pull/138
- chore(client): upgrade aws-sdk-go-v2/service/s3 to v1.47.3 and fix a breaking change by the version by @go-to-k in https://github.com/go-to-k/cls3/pull/141

## [v0.12.1](https://github.com/go-to-k/cls3/compare/v0.12.0...v0.12.1) - 2023-11-17
- docs: add features to README by @go-to-k in https://github.com/go-to-k/cls3/pull/124
- chore(deps): bump github.com/aws/aws-sdk-go-v2 from 1.20.2 to 1.23.0 by @dependabot in https://github.com/go-to-k/cls3/pull/127
- chore(deps): bump golang.org/x/sync from 0.3.0 to 0.5.0 by @dependabot in https://github.com/go-to-k/cls3/pull/121
- chore(deps): bump goreleaser/goreleaser-action from 4 to 5 by @dependabot in https://github.com/go-to-k/cls3/pull/99
- chore(deps): bump actions/checkout from 3 to 4 by @dependabot in https://github.com/go-to-k/cls3/pull/98

## [v0.12.0](https://github.com/go-to-k/cls3/compare/v0.11.0...v0.12.0) - 2023-10-23
- feat: case insensitive search by @go-to-k in https://github.com/go-to-k/cls3/pull/115

## [v0.11.0](https://github.com/go-to-k/cls3/compare/v0.10.0...v0.11.0) - 2023-10-14
- feat: add number of objects on cleared message by @go-to-k in https://github.com/go-to-k/cls3/pull/114

## [v0.10.0](https://github.com/go-to-k/cls3/compare/v0.9.0...v0.10.0) - 2023-10-06
- docs: change a command example for a quiet option by @go-to-k in https://github.com/go-to-k/cls3/pull/102
- docs: Update README.md by @go-to-k in https://github.com/go-to-k/cls3/pull/106
- chore: go version to 1.21 by @go-to-k in https://github.com/go-to-k/cls3/pull/108

## [v0.9.0](https://github.com/go-to-k/cls3/compare/v0.8.0...v0.9.0) - 2023-09-20
- docs: tool description by @go-to-k in https://github.com/go-to-k/cls3/pull/93
- ci: fix coverage report path by @go-to-k in https://github.com/go-to-k/cls3/pull/96
- test: add goleak by @go-to-k in https://github.com/go-to-k/cls3/pull/95
- Added Spanish Translation for cls3 by @iaasgeek in https://github.com/go-to-k/cls3/pull/97
- feat: add a progress bar and quiet option by @go-to-k in https://github.com/go-to-k/cls3/pull/101

## [v0.8.0](https://github.com/go-to-k/cls3/compare/v0.7.1...v0.8.0) - 2023-08-18
- docs: README.md for blog link by @go-to-k in https://github.com/go-to-k/cls3/pull/85
- chore(deps): bump golang.org/x/sync from 0.1.0 to 0.3.0 by @dependabot in https://github.com/go-to-k/cls3/pull/71
- chore(deps): bump github.com/rs/zerolog from 1.29.0 to 1.30.0 by @dependabot in https://github.com/go-to-k/cls3/pull/79
- chore(deps): bump github.com/aws/smithy-go from 1.13.5 to 1.14.1 by @dependabot in https://github.com/go-to-k/cls3/pull/87
- chore(deps): bump github.com/aws/aws-sdk-go-v2 from 1.17.7 to 1.20.2 by @dependabot in https://github.com/go-to-k/cls3/pull/89
- chore(deps): bump github.com/aws/aws-sdk-go-v2/service/s3 from 1.31.0 to 1.38.3 by @dependabot in https://github.com/go-to-k/cls3/pull/90
- test: correct comments by @go-to-k in https://github.com/go-to-k/cls3/pull/91
- chore: change S3 DeleteObjects parallels count by @go-to-k in https://github.com/go-to-k/cls3/pull/92

## [v0.7.1](https://github.com/go-to-k/cls3/commits/v0.7.1) - 2023-08-06
- ci: upgrade goreleaser version by @go-to-k in https://github.com/go-to-k/cls3/pull/84

## [v0.7.0](https://github.com/go-to-k/cls3/compare/v0.6.3...v0.7.0) - 2023-04-01
- feat: cross region delete by @go-to-k in https://github.com/go-to-k/cls3/pull/59

## [v0.6.3](https://github.com/go-to-k/cls3/compare/v0.6.1...v0.6.3) - 2023-03-31
- feat: more page size for interactive mode by @go-to-k in https://github.com/go-to-k/cls3/pull/57
