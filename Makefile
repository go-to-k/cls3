RED=\033[31m
GREEN=\033[32m
RESET=\033[0m

COLORIZE_PASS = sed "s/^\([- ]*\)\(PASS\)/\1$$(printf "$(GREEN)")\2$$(printf "$(RESET)")/g"
COLORIZE_FAIL = sed "s/^\([- ]*\)\(FAIL\)/\1$$(printf "$(RED)")\2$$(printf "$(RESET)")/g"

VERSION := $(shell git describe --tags --abbrev=0)
REVISION := $(shell git rev-parse --short HEAD)
LDFLAGS := -s -w \
			-X 'github.com/go-to-k/cls3/internal/version.Version=$(VERSION)' \
			-X 'github.com/go-to-k/cls3/internal/version.Revision=$(REVISION)'
GO_FILES := $(shell find . -type f -name '*.go' -print)

DIFF_FILE := "$$(git diff --name-only --diff-filter=ACMRT | grep .go$ | xargs -I{} dirname {} | sort | uniq | xargs -I{} echo ./{})"

TEST_DIFF_RESULT := "$$(go test -race -cover -v $$(echo $(DIFF_FILE)) -coverpkg=./...)"
TEST_FULL_RESULT := "$$(go test -race -cover -v ./... -coverpkg=./...)"
TEST_COV_RESULT := "$$(go test -race -cover -v ./... -coverpkg=./... -coverprofile=cover.out.tmp)"

FAIL_CHECK := "^[^\s\t]*FAIL[^\s\t]*$$"

.PHONY: test_diff test test_view lint lint_diff mockgen deadcode shadow cognit run build install clean testgen_general testgen_directory testgen_table testgen_vector testgen_help

test_diff:
	@! echo $(TEST_DIFF_RESULT) | $(COLORIZE_PASS) | $(COLORIZE_FAIL) | tee /dev/stderr | grep $(FAIL_CHECK) > /dev/null
test:
	@! echo $(TEST_FULL_RESULT) | $(COLORIZE_PASS) | $(COLORIZE_FAIL) | tee /dev/stderr | grep $(FAIL_CHECK) > /dev/null
test_view:
	@! echo $(TEST_COV_RESULT) | $(COLORIZE_PASS) | $(COLORIZE_FAIL) | tee /dev/stderr | grep $(FAIL_CHECK) > /dev/null
	cat cover.out.tmp | grep -v "**_mock.go" > cover.out
	rm cover.out.tmp
	go tool cover -func=cover.out
	go tool cover -html=cover.out -o cover.html
lint:
	golangci-lint run
lint_diff:
	golangci-lint run $$(echo $(DIFF_FILE))
mockgen:
	go generate ./...
deadcode:
	deadcode ./...
shadow:
	find . -type f -name '*.go' | sed -e "s/\/[^\.\/]*\.go//g" | uniq | xargs shadow
cognit:
	gocognit -top 10 ./ | grep -v "_test.go"
run:
	go mod tidy
	go run -ldflags "$(LDFLAGS)" cmd/cls3/main.go $${OPT}
build: $(GO_FILES)
	go mod tidy
	go build -ldflags "$(LDFLAGS)" -o cls3 cmd/cls3/main.go
install:
	go install -ldflags "$(LDFLAGS)" github.com/go-to-k/cls3/cmd/cls3
clean:
	go clean
	rm -f cls3

# Test data generation commands
# ==================================

# Run standard S3 bucket test data generator
testgen_general:
	@echo "Running standard S3 bucket test data generator..."
	@cd testdata && go mod tidy && go run cmd/general/main.go $(OPT)

# Run S3 Express One Zone directory bucket test data generator
testgen_directory:
	@echo "Running S3 Express One Zone directory bucket test data generator..."
	@cd testdata && go mod tidy && go run cmd/directory/main.go $(OPT)

# Run S3 table test data generator
testgen_table:
	@echo "Running S3 table test data generator..."
	@cd testdata && go mod tidy && go run cmd/table/main.go $(OPT)

# Run S3 vector test data generator
testgen_vector:
	@echo "Running S3 vector test data generator..."
	@cd testdata && go mod tidy && go run cmd/vector/main.go $(OPT)

# Help for test data generation
testgen_help:
	@echo "Test data generation targets:"
	@echo "  testgen_general      - Run the standard S3 bucket test data generator"
	@echo "  testgen_directory    - Run the S3 Express One Zone directory bucket test data generator"
	@echo "  testgen_table        - Run the S3 table test data generator"
	@echo "  testgen_vector       - Run the S3 vector test data generator"
	@echo ""
	@echo "Example usage:"
	@echo "  make testgen_general OPT=\"-b my-bucket -n 5 -o 1000\""
	@echo "  make testgen_directory OPT=\"-b my-bucket -n 2 -o 500\""
	@echo "  make testgen_table OPT=\"-b my-bucket -n 1 -t 50 -s 20 -r us-west-2\""
	@echo "  make testgen_vector OPT=\"-b my-bucket -n 1 -v 50 -i 20 -r us-west-2\""