package app

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-to-k/cls3/internal/wrapper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func Test_determineConcurrencyNumber(t *testing.T) {
	tests := []struct {
		name           string
		processor      *BucketProcessor
		expectedNumber int
	}{
		{
			name: "return 1 when concurrent mode is off",
			processor: &BucketProcessor{
				config: BucketProcessorConfig{
					ConcurrentMode:    false,
					ConcurrencyNumber: UnspecifiedConcurrencyNumber,
					TargetBuckets:     []string{"bucket1", "bucket2"},
				},
			},
			expectedNumber: 1,
		},
		{
			name: "return number of target buckets when concurrency number is not specified",
			processor: &BucketProcessor{
				config: BucketProcessorConfig{
					ConcurrentMode:    true,
					ConcurrencyNumber: UnspecifiedConcurrencyNumber,
					TargetBuckets:     []string{"bucket1", "bucket2", "bucket3"},
				},
			},
			expectedNumber: 3,
		},
		{
			name: "return specified concurrency number when set",
			processor: &BucketProcessor{
				config: BucketProcessorConfig{
					ConcurrentMode:    true,
					ConcurrencyNumber: 2,
					TargetBuckets:     []string{"bucket1", "bucket2", "bucket3"},
				},
			},
			expectedNumber: 2,
		},
		{
			name: "return 1 when concurrent mode is off regardless of concurrency number",
			processor: &BucketProcessor{
				config: BucketProcessorConfig{
					ConcurrentMode:    false,
					ConcurrencyNumber: 2,
					TargetBuckets:     []string{"bucket1", "bucket2"},
				},
			},
			expectedNumber: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.processor.determineConcurrencyNumber()
			assert.Equal(t, tt.expectedNumber, result)
		})
	}
}

func TestBucketProcessor_Process(t *testing.T) {
	tests := []struct {
		name          string
		prepareMockFn func(m *wrapper.MockIWrapper, mc *MockIClearingState, md *MockIDisplayManager)
		config        BucketProcessorConfig
		wantErr       bool
		expectedErr   string
	}{
		{
			name: "successfully process buckets",
			prepareMockFn: func(m *wrapper.MockIWrapper, mc *MockIClearingState, md *MockIDisplayManager) {
				m.EXPECT().OutputCheckingMessage("bucket1").Return(nil)
				m.EXPECT().OutputCheckingMessage("bucket2").Return(nil)
				md.EXPECT().Start([]string{"bucket1", "bucket2"})
				md.EXPECT().Finish([]string{"bucket1", "bucket2"}).Return(nil)

				countCh1 := make(chan int64)
				completedCh1 := make(chan bool)
				countCh2 := make(chan int64)
				completedCh2 := make(chan bool)

				mc.EXPECT().GetChannelsForBucket("bucket1").Return(countCh1, completedCh1)
				mc.EXPECT().GetChannelsForBucket("bucket2").Return(countCh2, completedCh2)
				m.EXPECT().ClearBucket(
					gomock.Any(),
					wrapper.ClearBucketInput{
						TargetBucket:    "bucket1",
						ForceMode:       false,
						OldVersionsOnly: false,
						QuietMode:       false,
						ClearingCountCh: countCh1,
					},
				).Return(nil)
				m.EXPECT().ClearBucket(
					gomock.Any(),
					wrapper.ClearBucketInput{
						TargetBucket:    "bucket2",
						ForceMode:       false,
						OldVersionsOnly: false,
						QuietMode:       false,
						ClearingCountCh: countCh2,
					},
				).Return(nil)
				go func() {
					completed := <-completedCh1
					assert.True(t, completed, "value from completedCh should be true")
				}()
				go func() {
					completed := <-completedCh2
					assert.True(t, completed, "value from completedCh should be true")
				}()
			},
			config: BucketProcessorConfig{
				TargetBuckets:     []string{"bucket1", "bucket2"},
				QuietMode:         false,
				ConcurrentMode:    false,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
				ForceMode:         false,
				OldVersionsOnly:   false,
			},
			wantErr: false,
		},
		{
			name: "error when output checking message fails",
			prepareMockFn: func(m *wrapper.MockIWrapper, mc *MockIClearingState, md *MockIDisplayManager) {
				m.EXPECT().OutputCheckingMessage("bucket1").Return(fmt.Errorf("OutputCheckingMessageError"))
			},
			config: BucketProcessorConfig{
				TargetBuckets:     []string{"bucket1"},
				QuietMode:         false,
				ConcurrentMode:    false,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			wantErr:     true,
			expectedErr: "OutputCheckingMessageError",
		},
		{
			name: "error when clear bucket fails",
			prepareMockFn: func(m *wrapper.MockIWrapper, mc *MockIClearingState, md *MockIDisplayManager) {
				m.EXPECT().OutputCheckingMessage("bucket1").Return(nil)
				md.EXPECT().Start([]string{"bucket1"})
				countCh := make(chan int64)
				completedCh := make(chan bool)
				mc.EXPECT().GetChannelsForBucket("bucket1").Return(countCh, completedCh)
				m.EXPECT().ClearBucket(
					gomock.Any(),
					wrapper.ClearBucketInput{
						TargetBucket:    "bucket1",
						ForceMode:       false,
						OldVersionsOnly: false,
						QuietMode:       false,
						ClearingCountCh: countCh,
					},
				).Return(fmt.Errorf("ClearBucketError"))
				go func() {
					completed := <-completedCh
					assert.False(t, completed, "value from completedCh should be false")
				}()
			},
			config: BucketProcessorConfig{
				TargetBuckets:     []string{"bucket1"},
				QuietMode:         false,
				ConcurrentMode:    false,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			wantErr:     true,
			expectedErr: "ClearBucketError",
		},
		{
			name: "error when display finish fails",
			prepareMockFn: func(m *wrapper.MockIWrapper, mc *MockIClearingState, md *MockIDisplayManager) {
				m.EXPECT().OutputCheckingMessage("bucket1").Return(nil)
				md.EXPECT().Start([]string{"bucket1"})
				countCh := make(chan int64)
				completedCh := make(chan bool)
				mc.EXPECT().GetChannelsForBucket("bucket1").Return(countCh, completedCh)
				m.EXPECT().ClearBucket(
					gomock.Any(),
					wrapper.ClearBucketInput{
						TargetBucket:    "bucket1",
						ForceMode:       false,
						OldVersionsOnly: false,
						QuietMode:       false,
						ClearingCountCh: countCh,
					},
				).Return(nil)
				md.EXPECT().Finish([]string{"bucket1"}).Return(fmt.Errorf("FinishError"))
				go func() {
					completed := <-completedCh
					assert.True(t, completed, "value from completedCh should be true")
				}()
			},
			config: BucketProcessorConfig{
				TargetBuckets:     []string{"bucket1"},
				QuietMode:         false,
				ConcurrentMode:    false,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
				ForceMode:         false,
				OldVersionsOnly:   false,
			},
			wantErr:     true,
			expectedErr: "FinishError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockWrapper := wrapper.NewMockIWrapper(ctrl)
			mockClearingState := NewMockIClearingState(ctrl)
			mockDisplayManager := NewMockIDisplayManager(ctrl)
			tt.prepareMockFn(mockWrapper, mockClearingState, mockDisplayManager)

			processor := &BucketProcessor{
				config:    tt.config,
				s3Wrapper: mockWrapper,
				state:     mockClearingState,
				display:   mockDisplayManager,
			}

			err := processor.Process(context.Background())
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				assert.EqualError(t, err, tt.expectedErr)
			}
		})
	}
}

func TestBucketProcessor_clearBuckets(t *testing.T) {
	tests := []struct {
		name              string
		prepareMockFn     func(m *wrapper.MockIWrapper, mc *MockIClearingState)
		config            BucketProcessorConfig
		concurrencyNumber int
		wantErr           bool
		expectedErr       string
	}{
		{
			name: "successfully clear single bucket",
			prepareMockFn: func(m *wrapper.MockIWrapper, mc *MockIClearingState) {
				countCh := make(chan int64)
				completedCh := make(chan bool)
				mc.EXPECT().GetChannelsForBucket("bucket1").Return(countCh, completedCh)
				m.EXPECT().ClearBucket(
					gomock.Any(),
					wrapper.ClearBucketInput{
						TargetBucket:    "bucket1",
						ForceMode:       false,
						OldVersionsOnly: false,
						QuietMode:       false,
						ClearingCountCh: countCh,
					},
				).Return(nil)
				go func() {
					completed := <-completedCh
					assert.True(t, completed, "value from completedCh should be true")
				}()
			},
			config: BucketProcessorConfig{
				TargetBuckets:     []string{"bucket1"},
				QuietMode:         false,
				ConcurrentMode:    false,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			concurrencyNumber: 1,
			wantErr:           false,
		},
		{
			name: "successfully clear multiple buckets",
			prepareMockFn: func(m *wrapper.MockIWrapper, mc *MockIClearingState) {
				for _, bucket := range []string{"bucket1", "bucket2"} {
					countCh := make(chan int64)
					completedCh := make(chan bool)
					mc.EXPECT().GetChannelsForBucket(bucket).Return(countCh, completedCh)
					m.EXPECT().ClearBucket(
						gomock.Any(),
						wrapper.ClearBucketInput{
							TargetBucket:    bucket,
							ForceMode:       false,
							OldVersionsOnly: false,
							QuietMode:       false,
							ClearingCountCh: countCh,
						},
					).Return(nil)
					go func() {
						completed := <-completedCh
						assert.True(t, completed, "value from completedCh should be true")
					}()
				}
			},
			config: BucketProcessorConfig{
				TargetBuckets:     []string{"bucket1", "bucket2"},
				QuietMode:         false,
				ConcurrentMode:    true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			concurrencyNumber: 1,
			wantErr:           false,
		},
		{
			name: "successfully clear multiple buckets concurrently",
			prepareMockFn: func(m *wrapper.MockIWrapper, mc *MockIClearingState) {
				for _, bucket := range []string{"bucket1", "bucket2"} {
					countCh := make(chan int64)
					completedCh := make(chan bool)
					mc.EXPECT().GetChannelsForBucket(bucket).Return(countCh, completedCh)
					m.EXPECT().ClearBucket(
						gomock.Any(),
						wrapper.ClearBucketInput{
							TargetBucket:    bucket,
							ForceMode:       false,
							OldVersionsOnly: false,
							QuietMode:       false,
							ClearingCountCh: countCh,
						},
					).Return(nil)
					go func() {
						completed := <-completedCh
						assert.True(t, completed, "value from completedCh should be true")
					}()
				}
			},
			config: BucketProcessorConfig{
				TargetBuckets:     []string{"bucket1", "bucket2"},
				QuietMode:         false,
				ConcurrentMode:    true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			concurrencyNumber: 2,
			wantErr:           false,
		},
		{
			name: "successfully clear many buckets concurrently",
			prepareMockFn: func(m *wrapper.MockIWrapper, mc *MockIClearingState) {
				for _, bucket := range []string{"bucket1", "bucket2", "bucket3", "bucket4", "bucket5"} {
					countCh := make(chan int64)
					completedCh := make(chan bool)
					mc.EXPECT().GetChannelsForBucket(bucket).Return(countCh, completedCh)
					m.EXPECT().ClearBucket(
						gomock.Any(),
						wrapper.ClearBucketInput{
							TargetBucket:    bucket,
							ForceMode:       false,
							OldVersionsOnly: false,
							QuietMode:       false,
							ClearingCountCh: countCh,
						},
					).Return(nil)
					go func() {
						completed := <-completedCh
						assert.True(t, completed, "value from completedCh should be true")
					}()
				}
			},
			config: BucketProcessorConfig{
				TargetBuckets:     []string{"bucket1", "bucket2", "bucket3", "bucket4", "bucket5"},
				QuietMode:         false,
				ConcurrentMode:    true,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			concurrencyNumber: 2,
			wantErr:           false,
		},
		{
			name: "successfully clear single bucket with quiet mode",
			prepareMockFn: func(m *wrapper.MockIWrapper, mc *MockIClearingState) {
				countCh := make(chan int64)
				completedCh := make(chan bool)
				mc.EXPECT().GetChannelsForBucket("bucket1").Return(countCh, completedCh)
				m.EXPECT().ClearBucket(
					gomock.Any(),
					wrapper.ClearBucketInput{
						TargetBucket:    "bucket1",
						ForceMode:       false,
						OldVersionsOnly: false,
						QuietMode:       true,
						ClearingCountCh: countCh,
					},
				).Return(nil)
			},
			config: BucketProcessorConfig{
				TargetBuckets:     []string{"bucket1"},
				QuietMode:         true,
				ConcurrentMode:    false,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			concurrencyNumber: 1,
			wantErr:           false,
		},
		{
			name: "error when clear bucket fails",
			prepareMockFn: func(m *wrapper.MockIWrapper, mc *MockIClearingState) {
				countCh := make(chan int64)
				completedCh := make(chan bool)
				mc.EXPECT().GetChannelsForBucket("bucket1").Return(countCh, completedCh)
				m.EXPECT().ClearBucket(
					gomock.Any(),
					wrapper.ClearBucketInput{
						TargetBucket:    "bucket1",
						ForceMode:       false,
						OldVersionsOnly: false,
						QuietMode:       false,
						ClearingCountCh: countCh,
					},
				).Return(fmt.Errorf("ClearBucketError"))
				go func() {
					completed := <-completedCh
					assert.False(t, completed, "value from completedCh should be false")
				}()
			},
			config: BucketProcessorConfig{
				TargetBuckets:     []string{"bucket1"},
				QuietMode:         false,
				ConcurrentMode:    false,
				ConcurrencyNumber: UnspecifiedConcurrencyNumber,
			},
			concurrencyNumber: 1,
			wantErr:           true,
			expectedErr:       "ClearBucketError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockWrapper := wrapper.NewMockIWrapper(ctrl)
			mockClearingState := NewMockIClearingState(ctrl)
			tt.prepareMockFn(mockWrapper, mockClearingState)

			processor := &BucketProcessor{
				config:    tt.config,
				s3Wrapper: mockWrapper,
				state:     mockClearingState,
			}

			err := processor.clearBuckets(context.Background(), tt.concurrencyNumber)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				assert.EqualError(t, err, tt.expectedErr)
			}
		})
	}
}
