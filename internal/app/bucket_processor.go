//go:generate mockgen -source=$GOFILE -destination=mock_$GOFILE -package=$GOPACKAGE -write_package_comment=false
package app

import (
	"context"

	"github.com/go-to-k/cls3/internal/io"
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
	Prefix            *string // not used for S3Tables
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
) *BucketProcessor {
	state := NewClearingState(config.TargetBuckets, s3Wrapper, config.ForceMode)

	display := NewDisplayManager(state, config.QuietMode)

	return &BucketProcessor{
		config:    config,
		s3Wrapper: s3Wrapper,
		state:     state,
		display:   display,
	}
}

// Process executes the bucket processing workflow
func (p *BucketProcessor) Process(ctx context.Context) error {
	concurrencyNumber := p.determineConcurrencyNumber()
	io.Logger.Info().Msgf("Number of buckets:  %v", len(p.config.TargetBuckets))
	io.Logger.Info().Msgf("Concurrency number: %v", concurrencyNumber)
	if p.config.Prefix != nil {
		io.Logger.Info().Msgf("Key prefix: %v", *p.config.Prefix)
	}

	for _, bucket := range p.config.TargetBuckets {
		if err := p.s3Wrapper.OutputCheckingMessage(bucket); err != nil {
			return err
		}
	}

	p.display.Start(p.config.TargetBuckets)

	if err := p.clearBuckets(ctx, concurrencyNumber); err != nil {
		return err
	}

	return p.display.Finish(p.config.TargetBuckets)
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

// clearBuckets processes all buckets with the specified concurrency
func (p *BucketProcessor) clearBuckets(ctx context.Context, concurrencyNumber int) error {
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

// clearSingleBucket processes a single bucket
func (p *BucketProcessor) clearSingleBucket(ctx context.Context, bucket string) error {
	clearingCountCh, clearingCompletedCh := p.state.GetChannelsForBucket(bucket)

	err := p.s3Wrapper.ClearBucket(ctx, wrapper.ClearBucketInput{
		TargetBucket:    bucket,
		ForceMode:       p.config.ForceMode,
		OldVersionsOnly: p.config.OldVersionsOnly,
		QuietMode:       p.config.QuietMode,
		ClearingCountCh: clearingCountCh,
		Prefix:          p.config.Prefix,
	})

	close(clearingCountCh)
	if !p.config.QuietMode {
		clearingCompletedCh <- err == nil
	}
	close(clearingCompletedCh)

	return err
}
