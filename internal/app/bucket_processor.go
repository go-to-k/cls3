package app

import (
	"context"

	"github.com/go-to-k/cls3/internal/wrapper"
	"github.com/gosuri/uilive"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// bucketProcessor handles all bucket processing operations
type bucketProcessor struct {
	targetBuckets     []string
	quietMode         bool
	concurrentMode    bool
	concurrencyNumber int
	forceMode         bool
	oldVersionsOnly   bool
}

// newBucketProcessor creates a new bucketProcessor instance
func newBucketProcessor(
	targetBuckets []string,
	quietMode,
	concurrentMode,
	forceMode,
	oldVersionsOnly bool,
	concurrencyNumber int,
) *bucketProcessor {
	return &bucketProcessor{
		targetBuckets:     targetBuckets,
		quietMode:         quietMode,
		concurrentMode:    concurrentMode,
		concurrencyNumber: concurrencyNumber,
		forceMode:         forceMode,
		oldVersionsOnly:   oldVersionsOnly,
	}
}

// Process executes the bucket processing workflow
func (p *bucketProcessor) Process(ctx context.Context, s3Wrapper wrapper.IWrapper) error {
	state, err := newClearingState(p.targetBuckets, s3Wrapper)
	if err != nil {
		return err
	}

	writer := uilive.New()
	if !p.quietMode {
		writer.Start()
		defer writer.Stop()
	}

	displayEg, err := p.startDisplayRoutines(s3Wrapper, state, writer)
	if err != nil {
		return err
	}

	if err := p.clearBuckets(ctx, s3Wrapper, state); err != nil {
		return err
	}

	if !p.quietMode {
		if err := displayEg.Wait(); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
		if err := p.outputFinalMessages(s3Wrapper, state); err != nil {
			return err
		}
	}

	return nil
}

// startDisplayRoutines initializes and starts the display monitoring routines
func (p *bucketProcessor) startDisplayRoutines(s3Wrapper wrapper.IWrapper, state *clearingState, writer *uilive.Writer) (*errgroup.Group, error) {
	displayEg := &errgroup.Group{}
	if p.quietMode {
		return displayEg, nil
	}

	if err := state.prepareInitialDisplay(p.targetBuckets, s3Wrapper); err != nil {
		return nil, err
	}

	for i, bucket := range p.targetBuckets {
		i, bucket := i, bucket
		displayEg.Go(func() error {
			return state.monitorBucketProgress(s3Wrapper, writer, i, bucket)
		})
	}

	return displayEg, nil
}

// clearBuckets processes all buckets with the specified concurrency
func (p *bucketProcessor) clearBuckets(ctx context.Context, s3Wrapper wrapper.IWrapper, state *clearingState) error {
	concurrencyNumber := p.determineConcurrencyNumber()
	sem := semaphore.NewWeighted(int64(concurrencyNumber))
	clearEg := errgroup.Group{}

	for _, bucket := range p.targetBuckets {
		bucket := bucket
		if err := sem.Acquire(ctx, 1); err != nil {
			return err
		}

		clearEg.Go(func() error {
			defer sem.Release(1)
			return p.clearSingleBucket(ctx, s3Wrapper, state, bucket)
		})
	}

	return clearEg.Wait()
}

// determineConcurrencyNumber calculates the appropriate concurrency number
func (p *bucketProcessor) determineConcurrencyNumber() int {
	// Series when ConcurrentMode is off.
	if !p.concurrentMode {
		return 1
	}

	// Cases where ConcurrencyNumber is unspecified.
	if p.concurrencyNumber == UnspecifiedConcurrencyNumber {
		return len(p.targetBuckets)
	}

	// Cases where ConcurrencyNumber is specified.
	return p.concurrencyNumber
}

// clearSingleBucket processes a single bucket
func (p *bucketProcessor) clearSingleBucket(ctx context.Context, s3Wrapper wrapper.IWrapper, state *clearingState, bucket string) error {
	clearingCountCh, clearingCompletedCh := state.getChannelsForBucket(bucket)

	err := s3Wrapper.ClearBucket(ctx, wrapper.ClearBucketInput{
		TargetBucket:    bucket,
		ForceMode:       p.forceMode,
		OldVersionsOnly: p.oldVersionsOnly,
		QuietMode:       p.quietMode,
		ClearingCountCh: clearingCountCh,
	})

	close(clearingCountCh)
	if !p.quietMode {
		clearingCompletedCh <- err == nil
	}
	close(clearingCompletedCh)

	return err
}

// outputFinalMessages displays the final status messages for all buckets
func (p *bucketProcessor) outputFinalMessages(s3Wrapper wrapper.IWrapper, state *clearingState) error {
	for _, bucket := range p.targetBuckets {
		bucket := bucket
		count := state.getCount(bucket)
		if err := s3Wrapper.OutputClearedMessage(bucket, count); err != nil {
			return err
		}
		if p.forceMode {
			if err := s3Wrapper.OutputDeletedMessage(bucket); err != nil {
				return err
			}
		}
	}
	return nil
}
