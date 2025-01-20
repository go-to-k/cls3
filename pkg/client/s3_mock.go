// Code generated by MockGen. DO NOT EDIT.
// Source: s3.go
//
// Generated by this command:
//
//	mockgen -source=s3.go -destination=s3_mock.go -package=client -write_package_comment=false
package client

import (
	context "context"
	reflect "reflect"

	types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	gomock "go.uber.org/mock/gomock"
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

// DeleteBucket mocks base method.
func (m *MockIS3) DeleteBucket(ctx context.Context, bucketName *string, region string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteBucket", ctx, bucketName, region)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteBucket indicates an expected call of DeleteBucket.
func (mr *MockIS3MockRecorder) DeleteBucket(ctx, bucketName, region any) *gomock.Call {
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
func (mr *MockIS3MockRecorder) DeleteObjects(ctx, bucketName, objects, region any) *gomock.Call {
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
func (mr *MockIS3MockRecorder) GetBucketLocation(ctx, bucketName any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBucketLocation", reflect.TypeOf((*MockIS3)(nil).GetBucketLocation), ctx, bucketName)
}

// ListBucketsOrDirectoryBuckets mocks base method.
func (m *MockIS3) ListBucketsOrDirectoryBuckets(ctx context.Context) ([]types.Bucket, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListBucketsOrDirectoryBuckets", ctx)
	ret0, _ := ret[0].([]types.Bucket)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListBucketsOrDirectoryBuckets indicates an expected call of ListBucketsOrDirectoryBuckets.
func (mr *MockIS3MockRecorder) ListBucketsOrDirectoryBuckets(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListBucketsOrDirectoryBuckets", reflect.TypeOf((*MockIS3)(nil).ListBucketsOrDirectoryBuckets), ctx)
}

// ListObjectsOrVersionsByPage mocks base method.
func (m *MockIS3) ListObjectsOrVersionsByPage(ctx context.Context, bucketName *string, region string, oldVersionsOnly bool, keyMarker, versionIdMarker *string) (*ListObjectsOrVersionsByPageOutput, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListObjectsOrVersionsByPage", ctx, bucketName, region, oldVersionsOnly, keyMarker, versionIdMarker)
	ret0, _ := ret[0].(*ListObjectsOrVersionsByPageOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListObjectsOrVersionsByPage indicates an expected call of ListObjectsOrVersionsByPage.
func (mr *MockIS3MockRecorder) ListObjectsOrVersionsByPage(ctx, bucketName, region, oldVersionsOnly, keyMarker, versionIdMarker any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListObjectsOrVersionsByPage", reflect.TypeOf((*MockIS3)(nil).ListObjectsOrVersionsByPage), ctx, bucketName, region, oldVersionsOnly, keyMarker, versionIdMarker)
}
