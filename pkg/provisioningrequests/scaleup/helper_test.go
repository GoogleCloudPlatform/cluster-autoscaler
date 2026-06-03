// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scaleup

import (
	"testing"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
)

func TestAggregateAutoscalerErrors(t *testing.T) {
	tests := []struct {
		name             string
		autoscalerErrors []errors.AutoscalerError
		wantErrorMessage string
		wantErrorType    errors.AutoscalerErrorType
	}{
		{
			name: "Messages are aggregated. Type is the same.",
			autoscalerErrors: []errors.AutoscalerError{
				errors.NewAutoscalerError(errors.ApiCallError, "msg1"),
				errors.NewAutoscalerError(errors.ApiCallError, "msg2"),
			},
			wantErrorMessage: "msg1; msg2; ",
			wantErrorType:    errors.ApiCallError,
		},
		{
			name: "Messages are aggregated. Type is not the same.",
			autoscalerErrors: []errors.AutoscalerError{
				errors.NewAutoscalerError(errors.ApiCallError, "msg1"),
				errors.NewAutoscalerError(errors.CloudProviderError, "msg2"),
			},
			wantErrorMessage: "msg1; msg2; ",
			wantErrorType:    errors.InternalError,
		},
		{
			name: "Messages are aggregated. Last message not shown. Type is not the same.",
			autoscalerErrors: []errors.AutoscalerError{
				errors.NewAutoscalerError(errors.ApiCallError, "msg1"),
				errors.NewAutoscalerError(errors.CloudProviderError, "msg2"),
				errors.NewAutoscalerError(errors.CloudProviderError, "msg3"),
				errors.NewAutoscalerError(errors.CloudProviderError, "msg4"),
			},
			wantErrorMessage: "msg1; msg2; msg3; ...",
			wantErrorType:    errors.InternalError,
		},
		{
			name: "Messages are aggregated. Last message not shown. Type is the same in the shown messages.",
			autoscalerErrors: []errors.AutoscalerError{
				errors.NewAutoscalerError(errors.CloudProviderError, "msg1"),
				errors.NewAutoscalerError(errors.CloudProviderError, "msg2"),
				errors.NewAutoscalerError(errors.CloudProviderError, "msg3"),
				errors.NewAutoscalerError(errors.ApiCallError, "msg4"),
			},
			wantErrorMessage: "msg1; msg2; msg3; ...",
			wantErrorType:    errors.CloudProviderError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotError := aggregateAutoscalerErrors(tt.autoscalerErrors)
			if gotError.Type() != tt.wantErrorType {
				t.Errorf("gotError.Type() = %v, wantErrorType: %v", gotError.Type(), tt.wantErrorType)
			}
			if gotError.Error() != tt.wantErrorMessage {
				t.Errorf("gotError.Error() = %v, wantErrorMessage: %v", gotError.Error(), tt.wantErrorMessage)
			}
		})
	}
}

func TestAggregateMigIds(t *testing.T) {
	tests := []struct {
		name        string
		migs        int
		limit       int
		wantMessage string
	}{
		{
			name:        "Ids are aggregated.",
			migs:        2,
			limit:       5,
			wantMessage: "https://www.googleapis.com/compute/v1/projects//zones//instanceGroups/; https://www.googleapis.com/compute/v1/projects//zones//instanceGroups/; ",
		},
		{
			name:        "Ids are aggregated. Last Id not shown.",
			migs:        2,
			limit:       1,
			wantMessage: "https://www.googleapis.com/compute/v1/projects//zones//instanceGroups/; ...",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var migs []*gke.GkeMig
			for i := 0; i < tt.migs; i++ {
				migs = append(migs, &gke.GkeMig{})
			}
			gotMessage := aggregateMigIds(migs, tt.limit)
			if gotMessage != tt.wantMessage {
				t.Errorf("gotMessage = %v, wantMessage: %v", gotMessage, tt.wantMessage)
			}
		})
	}
}

func TestRegisterScaleUp(t *testing.T) {
	type param struct {
		ngID string
		size int
		want int
	}
	tests := []struct {
		name   string
		params []param
	}{
		{
			name: "simple test case",
			params: []param{
				{"test-1", 4, 0},
				{"test-2", 4, 0},
				{"test-3", 4, 0},
				{"test-1", 4, 4},
			},
		},
		{
			name: "one ng multiple calls",
			params: []param{
				{"test-1", 4, 0},
				{"test-1", 4, 4},
				{"test-1", 4, 8},
				{"test-1", 4, 12},
			},
		},
		{
			name: "all ngs different",
			params: []param{
				{"test-1", 4, 0},
				{"test-2", 4, 0},
				{"test-3", 4, 0},
				{"test-4", 4, 0},
			},
		},
		{
			name: "multiple ngs",
			params: []param{
				{"test-1", 4, 0},
				{"test-2", 4, 0},
				{"test-3", 4, 0},
				{"test-2", 4, 4},
				{"test-3", 4, 4},
				{"test-4", 4, 0},
				{"test-1", 4, 4},
				{"test-4", 4, 4},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newScaleUpState()
			for _, p := range tt.params {
				if got := s.registerScaleUp(p.ngID, p.size); got != p.want {
					t.Errorf("scaleUpState.registerScaleUp() = %v, want %v", got, p.want)
				}
			}
		})
	}
}
