package app

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-to-k/cls3/internal/wrapper"
	"github.com/gosuri/uilive"
)

// clearingState manages the state of bucket clearing operations for display live deletion progress
type clearingState struct {
	lines             []string
	linesMutex        sync.Mutex
	countChannels     map[string]chan int64
	completedChannels map[string]chan bool
	counts            map[string]*atomic.Int64
	countsMutex       sync.Mutex
}

// newClearingState initializes a new clearingState
func newClearingState(targetBuckets []string, s3Wrapper wrapper.IWrapper) (*clearingState, error) {
	state := &clearingState{
		lines:             make([]string, len(targetBuckets)),
		countChannels:     make(map[string]chan int64, len(targetBuckets)),
		completedChannels: make(map[string]chan bool, len(targetBuckets)),
		counts:            make(map[string]*atomic.Int64, len(targetBuckets)),
	}

	for _, bucket := range targetBuckets {
		if err := s3Wrapper.OutputCheckingMessage(bucket); err != nil {
			return nil, err
		}
		state.countChannels[bucket] = make(chan int64)
		state.completedChannels[bucket] = make(chan bool)
		state.counts[bucket] = &atomic.Int64{}
	}

	return state, nil
}

// prepareInitialDisplay prepares the initial display lines for each bucket
func (s *clearingState) prepareInitialDisplay(targetBuckets []string, s3Wrapper wrapper.IWrapper) error {
	for i, bucket := range targetBuckets {
		i, bucket := i, bucket
		// Necessary to first display all bucket rows together
		message, err := s3Wrapper.GetLiveClearingMessage(bucket, 0)
		if err != nil {
			return err
		}
		s.linesMutex.Lock()
		s.lines[i] = message
		s.linesMutex.Unlock()
	}
	return nil
}

// monitorBucketProgress monitors the progress of a single bucket clearing operation
func (s *clearingState) monitorBucketProgress(
	s3Wrapper wrapper.IWrapper,
	writer *uilive.Writer,
	index int,
	bucket string,
) error {
	// Lock to access to slices safely
	s.countsMutex.Lock()
	clearingCountCh := s.countChannels[bucket]
	clearingCompletedCh := s.completedChannels[bucket]
	counter := s.counts[bucket]
	s.countsMutex.Unlock()

	for count := range clearingCountCh {
		counter.Store(count)
		message, err := s3Wrapper.GetLiveClearingMessage(bucket, count)
		if err != nil {
			return err
		}
		s.linesMutex.Lock()
		s.lines[index] = message
		fmt.Fprintln(writer, strings.Join(s.lines, "\n"))
		s.linesMutex.Unlock()
	}

	isCompleted := <-clearingCompletedCh
	count := counter.Load()
	message, err := s3Wrapper.GetLiveClearedMessage(bucket, count, isCompleted)
	if err != nil {
		return err
	}
	s.linesMutex.Lock()
	s.lines[index] = message
	fmt.Fprintln(writer, strings.Join(s.lines, "\n"))
	s.linesMutex.Unlock()
	return nil
}

// getChannelsForBucket returns the channels associated with a specific bucket
func (s *clearingState) getChannelsForBucket(bucket string) (chan int64, chan bool) {
	// Lock to access to slices safely
	s.countsMutex.Lock()
	defer s.countsMutex.Unlock()
	return s.countChannels[bucket], s.completedChannels[bucket]
}

// getCount returns the current count for a specific bucket
func (s *clearingState) getCount(bucket string) int64 {
	// Lock to access to slices safely
	s.countsMutex.Lock()
	defer s.countsMutex.Unlock()
	return s.counts[bucket].Load()
}
