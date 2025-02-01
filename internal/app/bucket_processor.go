//go:generate mockgen -source=$GOFILE -destination=mock_$GOFILE -package=$GOPACKAGE -write_package_comment=false
package app

import (
	"context"

	"github.com/go-to-k/cls3/internal/wrapper"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type IBucketProcessor interface {
	Process(ctx context.Context) error
}

var _ IBucketProcessor = (*BucketProcessor)(nil)

// BucketProcessorConfig contains all configuration parameters for bucket processing operations
type BucketProcessorConfig struct {
	TargetBuckets     []string
	QuietMode         bool
	ConcurrentMode    bool
	ConcurrencyNumber int
	ForceMode         bool
	OldVersionsOnly   bool
}

// BucketProcessor handles all bucket processing operations
type BucketProcessor struct {
	config    BucketProcessorConfig
	s3Wrapper wrapper.IWrapper
	state     IClearingState
	display   IDisplayManager
}

// NewBucketProcessor creates a new BucketProcessor instance
func NewBucketProcessor(
	config BucketProcessorConfig,
	s3Wrapper wrapper.IWrapper,
) (*BucketProcessor, error) {
	state, err := NewClearingState(config.TargetBuckets, s3Wrapper, config.ForceMode)
	if err != nil {
		return nil, err
	}

	display := NewDisplayManager(state, config.QuietMode)

	return &BucketProcessor{
		config:    config,
		s3Wrapper: s3Wrapper,
		state:     state,
		display:   display,
	}, nil
}

// Process executes the bucket processing workflow
func (p *BucketProcessor) Process(ctx context.Context) error {
	if err := p.display.Start(p.config.TargetBuckets); err != nil {
		return err
	}

	if err := p.clearBuckets(ctx); err != nil {
		return err
	}

	return p.display.Finish(p.config.TargetBuckets)
}

// clearBuckets processes all buckets with the specified concurrency
func (p *BucketProcessor) clearBuckets(ctx context.Context) error {
	concurrencyNumber := p.determineConcurrencyNumber()
	sem := semaphore.NewWeighted(int64(concurrencyNumber))
	clearEg := errgroup.Group{}

	for _, bucket := range p.config.TargetBuckets {
		bucket := bucket
		if err := sem.Acquire(ctx, 1); err != nil {
			return err
		}

		clearEg.Go(func() error {
			defer sem.Release(1)
			return p.clearSingleBucket(ctx, bucket)
		})
	}

	return clearEg.Wait()
}

// determineConcurrencyNumber calculates the appropriate concurrency number
func (p *BucketProcessor) determineConcurrencyNumber() int {
	// Series when ConcurrentMode is off.
	if !p.config.ConcurrentMode {
		return 1
	}

	// Cases where ConcurrencyNumber is unspecified.
	if p.config.ConcurrencyNumber == UnspecifiedConcurrencyNumber {
		return len(p.config.TargetBuckets)
	}

	// Cases where ConcurrencyNumber is specified.
	return p.config.ConcurrencyNumber
}

// clearSingleBucket processes a single bucket
func (p *BucketProcessor) clearSingleBucket(ctx context.Context, bucket string) error {
	clearingCountCh, clearingCompletedCh := p.state.GetChannelsForBucket(bucket)

	err := p.s3Wrapper.ClearBucket(ctx, wrapper.ClearBucketInput{
		TargetBucket:    bucket,
		ForceMode:       p.config.ForceMode,
		OldVersionsOnly: p.config.OldVersionsOnly,
		QuietMode:       p.config.QuietMode,
		ClearingCountCh: clearingCountCh,
	})

	close(clearingCountCh)
	if !p.config.QuietMode {
		clearingCompletedCh <- err == nil
	}
	close(clearingCompletedCh)

	return err
}
