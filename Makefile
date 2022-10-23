RED=\033[31m
GREEN=\033[32m
RESET=\033[0m

COLORIZE_PASS=sed ''/PASS/s//$$(printf "$(GREEN)PASS$(RESET)")/''
COLORIZE_FAIL=sed ''/FAIL/s//$$(printf "$(RED)FAIL$(RESET)")/''

VERSION := $(shell git describe --tags --abbrev=0)
REVISION := $(shell git rev-parse --short HEAD)
LDFLAGS := -s -w \
			-X 'github.com/go-to-k/cls3/option.Version=$(VERSION)' \
			-X 'github.com/go-to-k/cls3/option.Revision=$(REVISION)'
GO_FILES:=$(shell find . -type f -name '*.go' -print)

TEST_RESULT := "$$(go test -race -cover -v ./... -coverpkg=./...)"
TEST_COV_RESULT := "$$(go test -race -cover -v ./... -coverpkg=./... -coverprofile=cover_file.out)"
FAIL_CHECK := "^[^\s\t]*FAIL[^\s\t]*$$"

test:
	@! echo $(TEST_RESULT) | $(COLORIZE_PASS) | $(COLORIZE_FAIL) | tee /dev/stderr | grep $(FAIL_CHECK) > /dev/null
test_view:
	@! echo $(TEST_COV_RESULT) | $(COLORIZE_PASS) | $(COLORIZE_FAIL) | tee /dev/stderr | grep $(FAIL_CHECK) > /dev/null
	go tool cover -html=cover_file.out -o cover_file.html
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