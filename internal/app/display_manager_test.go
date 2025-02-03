package app

import (
	"fmt"
	"testing"

	"github.com/go-to-k/cls3/internal/io"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"
)

func TestDisplayManager_Start(t *testing.T) {
	tests := []struct {
		name          string
		prepareMockFn func(m *MockIClearingState)
		quietMode     bool
		targetBuckets []string
	}{
		{
			name: "successfully start display manager",
			prepareMockFn: func(m *MockIClearingState) {
				eg := &errgroup.Group{}
				m.EXPECT().StartDisplayRoutines(
					[]string{"bucket1"},
					gomock.Any(),
				).Return(eg)
			},
			quietMode:     false,
			targetBuckets: []string{"bucket1"},
		},
		{
			name: "skip display in quiet mode",
			prepareMockFn: func(m *MockIClearingState) {
				// No expectations because quiet mode skips display
			},
			quietMode:     true,
			targetBuckets: []string{"bucket1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockClearingState := NewMockIClearingState(ctrl)
			tt.prepareMockFn(mockClearingState)

			manager := NewDisplayManager(mockClearingState, tt.quietMode)

			manager.Start(tt.targetBuckets)

			if !tt.quietMode {
				assert.NotNil(t, manager.writer)
				assert.NotNil(t, manager.displayEg)
			} else {
				assert.Nil(t, manager.writer)
				assert.Nil(t, manager.displayEg)
			}
		})
	}
}

func TestDisplayManager_Finish(t *testing.T) {
	tests := []struct {
		name          string
		prepareMockFn func(m *MockIClearingState)
		quietMode     bool
		targetBuckets []string
		wantErr       bool
		expectedErr   string
	}{
		{
			name: "successfully finish display manager",
			prepareMockFn: func(m *MockIClearingState) {
				eg := &errgroup.Group{}
				m.EXPECT().StartDisplayRoutines(
					[]string{"bucket1"},
					gomock.Any(),
				).Return(eg)
				m.EXPECT().OutputFinalMessages([]string{"bucket1"}).Return(nil)
			},
			quietMode:     false,
			targetBuckets: []string{"bucket1"},
			wantErr:       false,
		},
		{
			name: "skip finish in quiet mode",
			prepareMockFn: func(m *MockIClearingState) {
				// No expectations because quiet mode skips display
			},
			quietMode:     true,
			targetBuckets: []string{"bucket1"},
			wantErr:       false,
		},
		{
			name: "error when errgroup wait fails",
			prepareMockFn: func(m *MockIClearingState) {
				eg := &errgroup.Group{}
				m.EXPECT().StartDisplayRoutines(
					[]string{"bucket1"},
					gomock.Any(),
				).Return(eg)
				eg.Go(func() error {
					return fmt.Errorf("errgroup wait error")
				})
			},
			quietMode:     false,
			targetBuckets: []string{"bucket1"},
			wantErr:       true,
			expectedErr:   "errgroup wait error",
		},
		{
			name: "error when output final messages fails",
			prepareMockFn: func(m *MockIClearingState) {
				eg := &errgroup.Group{}
				m.EXPECT().StartDisplayRoutines(
					[]string{"bucket1"},
					gomock.Any(),
				).Return(eg)
				m.EXPECT().OutputFinalMessages([]string{"bucket1"}).Return(fmt.Errorf("OutputFinalMessagesError"))
			},
			quietMode:     false,
			targetBuckets: []string{"bucket1"},
			wantErr:       true,
			expectedErr:   "OutputFinalMessagesError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockClearingState := NewMockIClearingState(ctrl)
			tt.prepareMockFn(mockClearingState)

			manager := NewDisplayManager(mockClearingState, tt.quietMode)

			if !tt.quietMode {
				manager.writer = io.NewWriter()
				manager.displayEg = mockClearingState.StartDisplayRoutines(tt.targetBuckets, manager.writer)
			}

			err := manager.Finish(tt.targetBuckets)
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
