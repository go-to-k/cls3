//go:generate mockgen -source=$GOFILE -destination=mock_$GOFILE -package=$GOPACKAGE -write_package_comment=false
package app

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/internal/wrapper"
	"golang.org/x/sync/errgroup"
)

type IClearingState interface {
	StartDisplayRoutines(targetBuckets []string, writer *io.Writer) *errgroup.Group
	OutputFinalMessages(targetBuckets []string) error
	GetChannelsForBucket(bucket string) (chan int64, chan bool)
}

var _ IClearingState = (*ClearingState)(nil)

// ClearingState manages the state of bucket clearing operations
type ClearingState struct {
	lines             []string
	linesMutex        sync.Mutex
	countChannels     map[string]chan int64
	completedChannels map[string]chan bool
	counts            map[string]*atomic.Int64
	countsMutex       sync.Mutex
	s3Wrapper         wrapper.IWrapper
	forceMode         bool
}

// NewClearingState initializes a new ClearingState instance
func NewClearingState(targetBuckets []string, s3Wrapper wrapper.IWrapper, forceMode bool) *ClearingState {
	state := &ClearingState{
		lines:             make([]string, len(targetBuckets)),
		countChannels:     make(map[string]chan int64, len(targetBuckets)),
		completedChannels: make(map[string]chan bool, len(targetBuckets)),
		counts:            make(map[string]*atomic.Int64, len(targetBuckets)),
		s3Wrapper:         s3Wrapper,
		forceMode:         forceMode,
	}

	for _, bucket := range targetBuckets {
		state.countChannels[bucket] = make(chan int64)
		state.completedChannels[bucket] = make(chan bool)
		state.counts[bucket] = &atomic.Int64{}
	}

	return state
}

// StartDisplayRoutines initializes and starts the display monitoring routines
func (s *ClearingState) StartDisplayRoutines(targetBuckets []string, writer *io.Writer) *errgroup.Group {
	displayEg := &errgroup.Group{}

	for i, bucket := range targetBuckets {
		i, bucket := i, bucket
		displayEg.Go(func() error {
			return s.monitorBucketProgress(writer, i, bucket)
		})
	}

	return displayEg
}

// monitorBucketProgress monitors the progress of a single bucket clearing operation
func (s *ClearingState) monitorBucketProgress(writer *io.Writer, index int, bucket string) error {
	getNonEmptyLines := func(lines []string) []string {
		nonEmptyLines := []string{}
		for _, line := range lines {
			if line != "" {
				nonEmptyLines = append(nonEmptyLines, line)
			}
		}
		return nonEmptyLines
	}

	// Lock to access to slices safely
	s.countsMutex.Lock()
	clearingCountCh := s.countChannels[bucket]
	clearingCompletedCh := s.completedChannels[bucket]
	counter := s.counts[bucket]
	s.countsMutex.Unlock()

	for count := range clearingCountCh {
		counter.Store(count)
		message, err := s.s3Wrapper.GetLiveClearingMessage(bucket, count)
		if err != nil {
			return err
		}
		s.linesMutex.Lock()
		s.lines[index] = message
		nonEmptyLines := getNonEmptyLines(s.lines)
		fmt.Fprintln(writer, strings.Join(nonEmptyLines, "\n"))
		s.linesMutex.Unlock()
	}

	isCompleted := <-clearingCompletedCh
	count := counter.Load()
	message, err := s.s3Wrapper.GetLiveClearedMessage(bucket, count, isCompleted)
	if err != nil {
		return err
	}
	s.linesMutex.Lock()
	s.lines[index] = message
	nonEmptyLines := getNonEmptyLines(s.lines)
	fmt.Fprintln(writer, strings.Join(nonEmptyLines, "\n"))
	s.linesMutex.Unlock()
	return nil
}

// GetChannelsForBucket returns the channels associated with a specific bucket
func (s *ClearingState) GetChannelsForBucket(bucket string) (chan int64, chan bool) {
	// Lock to access to slices safely
	s.countsMutex.Lock()
	defer s.countsMutex.Unlock()
	return s.countChannels[bucket], s.completedChannels[bucket]
}

// OutputFinalMessages displays the final status messages for all buckets
func (s *ClearingState) OutputFinalMessages(targetBuckets []string) error {
	for _, bucket := range targetBuckets {
		count := s.getCount(bucket)
		if err := s.s3Wrapper.OutputClearedMessage(bucket, count); err != nil {
			return err
		}
		if s.forceMode {
			if err := s.s3Wrapper.OutputDeletedMessage(bucket); err != nil {
				return err
			}
		}
	}
	return nil
}

// getCount returns the current count for a specific bucket
func (s *ClearingState) getCount(bucket string) int64 {
	// Lock to access to slices safely
	s.countsMutex.Lock()
	defer s.countsMutex.Unlock()
	return s.counts[bucket].Load()
}
