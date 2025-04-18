// Code generated by MockGen. DO NOT EDIT.
// Source: clearing_state.go
//
// Generated by this command:
//
//	mockgen -source=clearing_state.go -destination=mock_clearing_state.go -package=app -write_package_comment=false
//

package app

import (
	reflect "reflect"

	io "github.com/go-to-k/cls3/internal/io"
	gomock "go.uber.org/mock/gomock"
	errgroup "golang.org/x/sync/errgroup"
)

// MockIClearingState is a mock of IClearingState interface.
type MockIClearingState struct {
	ctrl     *gomock.Controller
	recorder *MockIClearingStateMockRecorder
	isgomock struct{}
}

// MockIClearingStateMockRecorder is the mock recorder for MockIClearingState.
type MockIClearingStateMockRecorder struct {
	mock *MockIClearingState
}

// NewMockIClearingState creates a new mock instance.
func NewMockIClearingState(ctrl *gomock.Controller) *MockIClearingState {
	mock := &MockIClearingState{ctrl: ctrl}
	mock.recorder = &MockIClearingStateMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockIClearingState) EXPECT() *MockIClearingStateMockRecorder {
	return m.recorder
}

// GetChannelsForBucket mocks base method.
func (m *MockIClearingState) GetChannelsForBucket(bucket string) (chan int64, chan bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetChannelsForBucket", bucket)
	ret0, _ := ret[0].(chan int64)
	ret1, _ := ret[1].(chan bool)
	return ret0, ret1
}

// GetChannelsForBucket indicates an expected call of GetChannelsForBucket.
func (mr *MockIClearingStateMockRecorder) GetChannelsForBucket(bucket any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetChannelsForBucket", reflect.TypeOf((*MockIClearingState)(nil).GetChannelsForBucket), bucket)
}

// OutputFinalMessages mocks base method.
func (m *MockIClearingState) OutputFinalMessages(targetBuckets []string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OutputFinalMessages", targetBuckets)
	ret0, _ := ret[0].(error)
	return ret0
}

// OutputFinalMessages indicates an expected call of OutputFinalMessages.
func (mr *MockIClearingStateMockRecorder) OutputFinalMessages(targetBuckets any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OutputFinalMessages", reflect.TypeOf((*MockIClearingState)(nil).OutputFinalMessages), targetBuckets)
}

// StartDisplayRoutines mocks base method.
func (m *MockIClearingState) StartDisplayRoutines(targetBuckets []string, writer *io.Writer) *errgroup.Group {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StartDisplayRoutines", targetBuckets, writer)
	ret0, _ := ret[0].(*errgroup.Group)
	return ret0
}

// StartDisplayRoutines indicates an expected call of StartDisplayRoutines.
func (mr *MockIClearingStateMockRecorder) StartDisplayRoutines(targetBuckets, writer any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartDisplayRoutines", reflect.TypeOf((*MockIClearingState)(nil).StartDisplayRoutines), targetBuckets, writer)
}
