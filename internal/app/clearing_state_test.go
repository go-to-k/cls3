package app

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/internal/wrapper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestClearingState_NewClearingState(t *testing.T) {
	tests := []struct {
		name          string
		targetBuckets []string
		forceMode     bool
	}{
		{
			name:          "create clearing state",
			targetBuckets: []string{"bucket1", "bucket2"},
			forceMode:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockWrapper := wrapper.NewMockIWrapper(ctrl)

			state := NewClearingState(tt.targetBuckets, mockWrapper, tt.forceMode)
			assert.NotNil(t, state)

			assert.Equal(t, len(tt.targetBuckets), len(state.lines))
			assert.Equal(t, len(tt.targetBuckets), len(state.countChannels))
			assert.Equal(t, len(tt.targetBuckets), len(state.completedChannels))
			assert.Equal(t, len(tt.targetBuckets), len(state.counts))
			assert.Equal(t, tt.forceMode, state.forceMode)
		})
	}
}

func TestClearingState_StartDisplayRoutines(t *testing.T) {
	tests := []struct {
		name          string
		prepareMockFn func(m *wrapper.MockIWrapper)
		targetBuckets []string
		wantEgErr     bool
		expectedErr   string
	}{
		{
			name: "successfully start display routines",
			prepareMockFn: func(m *wrapper.MockIWrapper) {
				m.EXPECT().GetLiveClearedMessage("bucket1", int64(0), true).Return("Cleared bucket1", nil)
				m.EXPECT().GetLiveClearedMessage("bucket2", int64(0), true).Return("Cleared bucket2", nil)
			},
			targetBuckets: []string{"bucket1", "bucket2"},
			wantEgErr:     false,
		},
		{
			name: "error when get live cleared message fails",
			prepareMockFn: func(m *wrapper.MockIWrapper) {
				m.EXPECT().GetLiveClearedMessage("bucket1", int64(0), true).Return("", fmt.Errorf("GetLiveClearedMessageError"))
			},
			targetBuckets: []string{"bucket1"},
			wantEgErr:     true,
			expectedErr:   "GetLiveClearedMessageError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockWrapper := wrapper.NewMockIWrapper(ctrl)
			tt.prepareMockFn(mockWrapper)

			state := &ClearingState{
				lines:             make([]string, len(tt.targetBuckets)),
				countChannels:     make(map[string]chan int64),
				completedChannels: make(map[string]chan bool),
				counts:            make(map[string]*atomic.Int64),
				s3Wrapper:         mockWrapper,
			}

			for _, bucket := range tt.targetBuckets {
				state.countChannels[bucket] = make(chan int64)
				state.completedChannels[bucket] = make(chan bool)
				state.counts[bucket] = &atomic.Int64{}
			}

			writer := io.NewWriter()
			eg := state.StartDisplayRoutines(tt.targetBuckets, writer)

			assert.NotNil(t, eg)

			for _, bucket := range tt.targetBuckets {
				close(state.countChannels[bucket])
				state.completedChannels[bucket] <- true
				close(state.completedChannels[bucket])
			}

			err := eg.Wait()
			assert.Equal(t, tt.wantEgErr, err != nil)
			if tt.wantEgErr {
				assert.EqualError(t, err, tt.expectedErr)
			}
		})
	}
}

func TestClearingState_OutputFinalMessages(t *testing.T) {
	tests := []struct {
		name          string
		prepareMockFn func(m *wrapper.MockIWrapper)
		targetBuckets []string
		forceMode     bool
		wantErr       bool
		expectedErr   string
	}{
		{
			name: "successfully output final messages without force mode",
			prepareMockFn: func(m *wrapper.MockIWrapper) {
				m.EXPECT().OutputClearedMessage("bucket1", int64(0)).Return(nil)
				m.EXPECT().OutputClearedMessage("bucket2", int64(0)).Return(nil)
			},
			targetBuckets: []string{"bucket1", "bucket2"},
			forceMode:     false,
			wantErr:       false,
		},
		{
			name: "successfully output final messages with force mode",
			prepareMockFn: func(m *wrapper.MockIWrapper) {
				m.EXPECT().OutputClearedMessage("bucket1", int64(0)).Return(nil)
				m.EXPECT().OutputDeletedMessage("bucket1").Return(nil)
			},
			targetBuckets: []string{"bucket1"},
			forceMode:     true,
			wantErr:       false,
		},
		{
			name: "error when output cleared message fails",
			prepareMockFn: func(m *wrapper.MockIWrapper) {
				m.EXPECT().OutputClearedMessage("bucket1", int64(0)).Return(fmt.Errorf("OutputClearedMessageError"))
			},
			targetBuckets: []string{"bucket1"},
			forceMode:     false,
			wantErr:       true,
			expectedErr:   "OutputClearedMessageError",
		},
		{
			name: "error when output deleted message fails",
			prepareMockFn: func(m *wrapper.MockIWrapper) {
				m.EXPECT().OutputClearedMessage("bucket1", int64(0)).Return(nil)
				m.EXPECT().OutputDeletedMessage("bucket1").Return(fmt.Errorf("OutputDeletedMessageError"))
			},
			targetBuckets: []string{"bucket1"},
			forceMode:     true,
			wantErr:       true,
			expectedErr:   "OutputDeletedMessageError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockWrapper := wrapper.NewMockIWrapper(ctrl)
			tt.prepareMockFn(mockWrapper)

			state := &ClearingState{
				counts:    make(map[string]*atomic.Int64),
				s3Wrapper: mockWrapper,
				forceMode: tt.forceMode,
			}

			for _, bucket := range tt.targetBuckets {
				state.counts[bucket] = &atomic.Int64{}
			}

			err := state.OutputFinalMessages(tt.targetBuckets)
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
