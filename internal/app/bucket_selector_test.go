package app

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/internal/wrapper"
	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v2"
	"go.uber.org/mock/gomock"
)

func Test_SelectBuckets(t *testing.T) {
	tests := []struct {
		name          string
		prepareMockFn func(m *wrapper.MockIWrapper, mi *io.MockIInputManager)
		selector      *BucketSelector
		want          []string
		wantContinue  bool
		wantErr       bool
		expectedErr   string
	}{
		{
			name: "successfully select buckets from command line",
			prepareMockFn: func(m *wrapper.MockIWrapper, mi *io.MockIInputManager) {
				m.EXPECT().CheckAllBucketsExist(
					gomock.Any(),
					[]string{"bucket1", "bucket2"},
				).Return(
					[]string{"bucket1", "bucket2"},
					nil,
				)
			},
			selector: &BucketSelector{
				interactiveMode: false,
				bucketNames:     cli.NewStringSlice("bucket1", "bucket2"),
			},
			want:         []string{"bucket1", "bucket2"},
			wantContinue: true,
			wantErr:      false,
		},
		{
			name: "error when bucket does not exist in command line mode",
			prepareMockFn: func(m *wrapper.MockIWrapper, mi *io.MockIInputManager) {
				m.EXPECT().CheckAllBucketsExist(
					gomock.Any(),
					[]string{"non-existent-bucket"},
				).Return(
					[]string{},
					fmt.Errorf("[resource -] NotExistsError: The following buckets do not exist: non-existent-bucket"),
				)
			},
			selector: &BucketSelector{
				interactiveMode: false,
				bucketNames:     cli.NewStringSlice("non-existent-bucket"),
			},
			want:         nil,
			wantContinue: false,
			wantErr:      true,
			expectedErr:  "[resource -] NotExistsError: The following buckets do not exist: non-existent-bucket",
		},
		{
			name: "successfully select buckets in interactive mode",
			prepareMockFn: func(m *wrapper.MockIWrapper, mi *io.MockIInputManager) {
				mi.EXPECT().InputKeywordForFilter("Filter a keyword of bucket names: ").Return("bucket")
				m.EXPECT().ListBucketNamesFilteredByKeyword(
					gomock.Any(),
					aws.String("bucket"),
				).Return(
					[]wrapper.ListBucketNamesFilteredByKeywordOutput{
						{BucketName: "bucket1", TargetBucket: "targetBucket1"},
						{BucketName: "bucket2", TargetBucket: "targetBucket2"},
					},
					nil,
				)
				mi.EXPECT().GetCheckboxes(
					[]string{"Select buckets."},
					[]string{"bucket1", "bucket2"},
				).Return(
					[]string{"bucket1"},
					true,
					nil,
				)
			},
			selector: &BucketSelector{
				interactiveMode: true,
				bucketNames:     cli.NewStringSlice(),
			},
			want:         []string{"targetBucket1"},
			wantContinue: true,
			wantErr:      false,
		},
		{
			name: "error when listing buckets fails in interactive mode",
			prepareMockFn: func(m *wrapper.MockIWrapper, mi *io.MockIInputManager) {
				mi.EXPECT().InputKeywordForFilter("Filter a keyword of bucket names: ").Return("test")
				m.EXPECT().ListBucketNamesFilteredByKeyword(
					gomock.Any(),
					aws.String("test"),
				).Return(
					nil,
					fmt.Errorf("ListBucketNamesFilteredByKeywordError"),
				)
			},
			selector: &BucketSelector{
				interactiveMode: true,
				bucketNames:     cli.NewStringSlice(),
			},
			want:         nil,
			wantContinue: false,
			wantErr:      true,
			expectedErr:  "ListBucketNamesFilteredByKeywordError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockWrapper := wrapper.NewMockIWrapper(ctrl)
			mockInputManager := io.NewMockIInputManager(ctrl)
			tt.prepareMockFn(mockWrapper, mockInputManager)

			tt.selector.s3Wrapper = mockWrapper
			tt.selector.inputManager = mockInputManager

			got, cont, err := tt.selector.SelectBuckets(context.Background())
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.expectedErr {
				t.Errorf("err = %v, want %v", err, tt.expectedErr)
				return
			}
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantContinue, cont)
		})
	}
}
