// Code generated by MockGen. DO NOT EDIT.
// Source: display_manager.go
//
// Generated by this command:
//
//	mockgen -source=display_manager.go -destination=mock_display_manager.go -package=app -write_package_comment=false
//

package app

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockIDisplayManager is a mock of IDisplayManager interface.
type MockIDisplayManager struct {
	ctrl     *gomock.Controller
	recorder *MockIDisplayManagerMockRecorder
	isgomock struct{}
}

// MockIDisplayManagerMockRecorder is the mock recorder for MockIDisplayManager.
type MockIDisplayManagerMockRecorder struct {
	mock *MockIDisplayManager
}

// NewMockIDisplayManager creates a new mock instance.
func NewMockIDisplayManager(ctrl *gomock.Controller) *MockIDisplayManager {
	mock := &MockIDisplayManager{ctrl: ctrl}
	mock.recorder = &MockIDisplayManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockIDisplayManager) EXPECT() *MockIDisplayManagerMockRecorder {
	return m.recorder
}

// Finish mocks base method.
func (m *MockIDisplayManager) Finish(targetBuckets []string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Finish", targetBuckets)
	ret0, _ := ret[0].(error)
	return ret0
}

// Finish indicates an expected call of Finish.
func (mr *MockIDisplayManagerMockRecorder) Finish(targetBuckets any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Finish", reflect.TypeOf((*MockIDisplayManager)(nil).Finish), targetBuckets)
}

// Start mocks base method.
func (m *MockIDisplayManager) Start(targetBuckets []string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start", targetBuckets)
}

// Start indicates an expected call of Start.
func (mr *MockIDisplayManagerMockRecorder) Start(targetBuckets any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockIDisplayManager)(nil).Start), targetBuckets)
}
