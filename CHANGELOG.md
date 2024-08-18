# Changelog

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