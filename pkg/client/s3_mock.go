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
func (m *MockIS3) CheckBucketExists(ctx context.Context, bucketName *string, directoryBucketsMode bool) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckBucketExists", ctx, bucketName, directoryBucketsMode)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CheckBucketExists indicates an expected call of CheckBucketExists.
func (mr *MockIS3MockRecorder) CheckBucketExists(ctx, bucketName, directoryBucketsMode interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckBucketExists", reflect.TypeOf((*MockIS3)(nil).CheckBucketExists), ctx, bucketName, directoryBucketsMode)
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
func (m *MockIS3) DeleteObjects(ctx context.Context, bucketName *string, objects []types.ObjectIdentifier, region string) ([]types.Error, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteObjects", ctx, bucketName, objects, region)
	ret0, _ := ret[0].([]types.Error)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteObjects indicates an expected call of DeleteObjects.
func (mr *MockIS3MockRecorder) DeleteObjects(ctx, bucketName, objects, region interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteObjects", reflect.TypeOf((*MockIS3)(nil).DeleteObjects), ctx, bucketName, objects, region)
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

// ListDirectoryBuckets mocks base method.
func (m *MockIS3) ListDirectoryBuckets(ctx context.Context) ([]types.Bucket, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListDirectoryBuckets", ctx)
	ret0, _ := ret[0].([]types.Bucket)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListDirectoryBuckets indicates an expected call of ListDirectoryBuckets.
func (mr *MockIS3MockRecorder) ListDirectoryBuckets(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListDirectoryBuckets", reflect.TypeOf((*MockIS3)(nil).ListDirectoryBuckets), ctx)
}

// ListObjectVersions mocks base method.
func (m *MockIS3) ListObjectVersions(ctx context.Context, bucketName *string, region string, oldVersionsOnly bool) ([]types.ObjectIdentifier, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListObjectVersions", ctx, bucketName, region, oldVersionsOnly)
	ret0, _ := ret[0].([]types.ObjectIdentifier)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListObjectVersions indicates an expected call of ListObjectVersions.
func (mr *MockIS3MockRecorder) ListObjectVersions(ctx, bucketName, region, oldVersionsOnly interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListObjectVersions", reflect.TypeOf((*MockIS3)(nil).ListObjectVersions), ctx, bucketName, region, oldVersionsOnly)
}

// ListObjectVersionsByPage mocks base method.
func (m *MockIS3) ListObjectVersionsByPage(ctx context.Context, bucketName *string, region string, oldVersionsOnly bool, keyMarker, versionIdMarker *string) ([]types.ObjectIdentifier, *string, *string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListObjectVersionsByPage", ctx, bucketName, region, oldVersionsOnly, keyMarker, versionIdMarker)
	ret0, _ := ret[0].([]types.ObjectIdentifier)
	ret1, _ := ret[1].(*string)
	ret2, _ := ret[2].(*string)
	ret3, _ := ret[3].(error)
	return ret0, ret1, ret2, ret3
}

// ListObjectVersionsByPage indicates an expected call of ListObjectVersionsByPage.
func (mr *MockIS3MockRecorder) ListObjectVersionsByPage(ctx, bucketName, region, oldVersionsOnly, keyMarker, versionIdMarker interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListObjectVersionsByPage", reflect.TypeOf((*MockIS3)(nil).ListObjectVersionsByPage), ctx, bucketName, region, oldVersionsOnly, keyMarker, versionIdMarker)
}

// ListObjectsByPage mocks base method.
func (m *MockIS3) ListObjectsByPage(ctx context.Context, bucketName *string, region string, marker *string) ([]types.ObjectIdentifier, *string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListObjectsByPage", ctx, bucketName, region, marker)
	ret0, _ := ret[0].([]types.ObjectIdentifier)
	ret1, _ := ret[1].(*string)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// ListObjectsByPage indicates an expected call of ListObjectsByPage.
func (mr *MockIS3MockRecorder) ListObjectsByPage(ctx, bucketName, region, marker interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListObjectsByPage", reflect.TypeOf((*MockIS3)(nil).ListObjectsByPage), ctx, bucketName, region, marker)
}
