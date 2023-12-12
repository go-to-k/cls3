// Code generated by MockGen. DO NOT EDIT.
// Source: s3.go

package client

import (
	context "context"
	reflect "reflect"

	types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	gomock "github.com/golang/mock/gomock"
)

// MockIS3 is a mock of IS3 interface.
type MockIS3 struct {
	ctrl     *gomock.Controller
	recorder *MockIS3MockRecorder
}

// MockIS3MockRecorder is the mock recorder for MockIS3.
type MockIS3MockRecorder struct {
	mock *MockIS3
}

// NewMockIS3 creates a new mock instance.
func NewMockIS3(ctrl *gomock.Controller) *MockIS3 {
	mock := &MockIS3{ctrl: ctrl}
	mock.recorder = &MockIS3MockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockIS3) EXPECT() *MockIS3MockRecorder {
	return m.recorder
}

// CheckBucketExists mocks base method.
func (m *MockIS3) CheckBucketExists(ctx context.Context, bucketName *string) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckBucketExists", ctx, bucketName)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CheckBucketExists indicates an expected call of CheckBucketExists.
func (mr *MockIS3MockRecorder) CheckBucketExists(ctx, bucketName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckBucketExists", reflect.TypeOf((*MockIS3)(nil).CheckBucketExists), ctx, bucketName)
}

// DeleteBucket mocks base method.
func (m *MockIS3) DeleteBucket(ctx context.Context, bucketName *string, region string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteBucket", ctx, bucketName, region)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteBucket indicates an expected call of DeleteBucket.
func (mr *MockIS3MockRecorder) DeleteBucket(ctx, bucketName, region interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteBucket", reflect.TypeOf((*MockIS3)(nil).DeleteBucket), ctx, bucketName, region)
}

// DeleteObjects mocks base method.
func (m *MockIS3) DeleteObjects(ctx context.Context, bucketName *string, objects []types.ObjectIdentifier, region string, quiet bool) ([]types.Error, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteObjects", ctx, bucketName, objects, region, quiet)
	ret0, _ := ret[0].([]types.Error)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteObjects indicates an expected call of DeleteObjects.
func (mr *MockIS3MockRecorder) DeleteObjects(ctx, bucketName, objects, region, quiet interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteObjects", reflect.TypeOf((*MockIS3)(nil).DeleteObjects), ctx, bucketName, objects, region, quiet)
}

// GetBucketLocation mocks base method.
func (m *MockIS3) GetBucketLocation(ctx context.Context, bucketName *string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBucketLocation", ctx, bucketName)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetBucketLocation indicates an expected call of GetBucketLocation.
func (mr *MockIS3MockRecorder) GetBucketLocation(ctx, bucketName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBucketLocation", reflect.TypeOf((*MockIS3)(nil).GetBucketLocation), ctx, bucketName)
}

// ListBuckets mocks base method.
func (m *MockIS3) ListBuckets(ctx context.Context) ([]types.Bucket, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListBuckets", ctx)
	ret0, _ := ret[0].([]types.Bucket)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListBuckets indicates an expected call of ListBuckets.
func (mr *MockIS3MockRecorder) ListBuckets(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListBuckets", reflect.TypeOf((*MockIS3)(nil).ListBuckets), ctx)
}

// ListObjectVersions mocks base method.
func (m *MockIS3) ListObjectVersions(ctx context.Context, bucketName *string, region string, oldObjectsOnly bool) ([]types.ObjectIdentifier, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListObjectVersions", ctx, bucketName, region, oldObjectsOnly)
	ret0, _ := ret[0].([]types.ObjectIdentifier)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListObjectVersions indicates an expected call of ListObjectVersions.
func (mr *MockIS3MockRecorder) ListObjectVersions(ctx, bucketName, region, oldObjectsOnly interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListObjectVersions", reflect.TypeOf((*MockIS3)(nil).ListObjectVersions), ctx, bucketName, region, oldObjectsOnly)
}
