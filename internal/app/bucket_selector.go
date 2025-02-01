//go:generate mockgen -source=$GOFILE -destination=mock_$GOFILE -package=$GOPACKAGE -write_package_comment=false
package app

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/internal/wrapper"
	"github.com/urfave/cli/v2"
)

type IBucketSelector interface {
	SelectBuckets(ctx context.Context) ([]string, bool, error)
}

var _ IBucketSelector = (*BucketSelector)(nil)

// BucketSelector handles the selection of buckets through either interactive mode or command line arguments
type BucketSelector struct {
	interactiveMode bool
	bucketNames     *cli.StringSlice
	s3Wrapper       wrapper.IWrapper
	inputManager    io.IInputManager
}

// NewBucketSelector creates a new BucketSelector instance
func NewBucketSelector(interactiveMode bool, bucketNames *cli.StringSlice, s3Wrapper wrapper.IWrapper) *BucketSelector {
	return &BucketSelector{
		interactiveMode: interactiveMode,
		bucketNames:     bucketNames,
		s3Wrapper:       s3Wrapper,
		inputManager:    io.NewInputManager(),
	}
}

// SelectBuckets selects buckets based on the mode (interactive or command line)
// Returns the selected buckets, a continuation flag, and any error that occurred
func (s *BucketSelector) SelectBuckets(ctx context.Context) ([]string, bool, error) {
	if s.interactiveMode {
		return s.selectInteractively(ctx)
	}
	return s.selectFromCommandLine(ctx)
}

// selectInteractively handles bucket selection in interactive mode
// Allows users to filter and select buckets through a command line interface
func (s *BucketSelector) selectInteractively(ctx context.Context) ([]string, bool, error) {
	keyword := s.inputManager.InputKeywordForFilter("Filter a keyword of bucket names: ")
	outputs, err := s.s3Wrapper.ListBucketNamesFilteredByKeyword(ctx, aws.String(keyword))
	if err != nil {
		return nil, false, err
	}

	bucketNames := []string{}
	for _, output := range outputs {
		bucketNames = append(bucketNames, output.BucketName)
	}

	label := []string{"Select buckets."}
	checkboxes, continuation, err := s.inputManager.GetCheckboxes(label, bucketNames)
	if err != nil {
		return nil, false, err
	}
	if !continuation {
		return nil, false, nil
	}

	selectedBuckets := []string{}
	for _, bucket := range checkboxes {
		for _, output := range outputs {
			if output.BucketName == bucket {
				selectedBuckets = append(selectedBuckets, output.TargetBucket)
			}
		}
	}
	return selectedBuckets, true, nil
}

// selectFromCommandLine handles bucket selection from command line arguments
// Validates that all specified buckets exist
func (s *BucketSelector) selectFromCommandLine(ctx context.Context) ([]string, bool, error) {
	outputBuckets, err := s.s3Wrapper.CheckAllBucketsExist(ctx, s.bucketNames.Value())
	if err != nil {
		return nil, false, err
	}
	return outputBuckets, true, nil
}
