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

package processor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

type fakeResizableVmLimitProvider struct {
	limit int
}

func (f *fakeResizableVmLimitProvider) provide() int {
	return f.limit
}

func TestLimit(t *testing.T) {
	tests := []struct {
		name     string
		requests map[string]map[string]apiv1.ResourceList
		limits   map[string]int
		want     map[string]map[string]apiv1.ResourceList
	}{
		{
			name:     "empty requests",
			requests: map[string]map[string]apiv1.ResourceList{},
			want:     map[string]map[string]apiv1.ResourceList{},
		},
		{
			name: "default doesn't exist",
			requests: map[string]map[string]apiv1.ResourceList{
				machinetypes.EK.Name(): {
					"a": {apiv1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)},
				},
			},
			want: map[string]map[string]apiv1.ResourceList{
				machinetypes.EK.Name(): {},
			},
		},
		{
			name: "default workload ID is always included",
			requests: map[string]map[string]apiv1.ResourceList{
				machinetypes.EK.Name(): {
					"":  {apiv1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)},
					"a": {apiv1.ResourceCPU: *resource.NewMilliQuantity(200, resource.DecimalSI)},
				},
			},
			want: map[string]map[string]apiv1.ResourceList{
				machinetypes.EK.Name(): {
					"": {apiv1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)},
				},
			},
		},
		{
			name: "default workload ID is prioritized over non-default",
			requests: map[string]map[string]apiv1.ResourceList{
				machinetypes.EK.Name(): {
					"":  {apiv1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)},
					"a": {apiv1.ResourceCPU: *resource.NewMilliQuantity(200, resource.DecimalSI)},
					"b": {apiv1.ResourceCPU: *resource.NewMilliQuantity(300, resource.DecimalSI)},
				},
			},
			limits: map[string]int{
				machinetypes.EK.Name(): 1,
			},
			want: map[string]map[string]apiv1.ResourceList{
				machinetypes.EK.Name(): {
					"":  {apiv1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)},
					"b": {apiv1.ResourceCPU: *resource.NewMilliQuantity(300, resource.DecimalSI)},
				},
			},
		},
		{
			name: "under limit",
			requests: map[string]map[string]apiv1.ResourceList{
				machinetypes.EK.Name(): {
					"a": {apiv1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)},
					"b": {apiv1.ResourceCPU: *resource.NewMilliQuantity(200, resource.DecimalSI)},
				},
			},
			limits: map[string]int{
				machinetypes.EK.Name(): 10,
			},
			want: map[string]map[string]apiv1.ResourceList{
				machinetypes.EK.Name(): {
					"a": {apiv1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)},
					"b": {apiv1.ResourceCPU: *resource.NewMilliQuantity(200, resource.DecimalSI)},
				},
			},
		},
		{
			name: "equal cpu, fallback to sorting by name",
			requests: map[string]map[string]apiv1.ResourceList{
				machinetypes.EK.Name(): {
					"a": {apiv1.ResourceCPU: *resource.NewMilliQuantity(200, resource.DecimalSI)},
					"b": {apiv1.ResourceCPU: *resource.NewMilliQuantity(200, resource.DecimalSI)},
				},
			},
			limits: map[string]int{
				machinetypes.EK.Name(): 1,
			},
			want: map[string]map[string]apiv1.ResourceList{
				machinetypes.EK.Name(): {
					"a": {apiv1.ResourceCPU: *resource.NewMilliQuantity(200, resource.DecimalSI)},
				},
			},
		},
		{
			name: "multiple machine families with separate limits",
			requests: map[string]map[string]apiv1.ResourceList{
				machinetypes.EK.Name(): {
					"":     {apiv1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI)},
					"ek-1": {apiv1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)},
					"ek-2": {apiv1.ResourceCPU: *resource.NewMilliQuantity(200, resource.DecimalSI)},
				},
				machinetypes.E4A.Name(): {
					"e4a-1": {apiv1.ResourceCPU: *resource.NewMilliQuantity(300, resource.DecimalSI)},
					"e4a-2": {apiv1.ResourceCPU: *resource.NewMilliQuantity(400, resource.DecimalSI)},
				},
			},
			limits: map[string]int{
				machinetypes.EK.Name():  1,
				machinetypes.E4A.Name(): 2,
			},
			want: map[string]map[string]apiv1.ResourceList{
				machinetypes.EK.Name(): {
					"":     {apiv1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI)},
					"ek-2": {apiv1.ResourceCPU: *resource.NewMilliQuantity(200, resource.DecimalSI)},
				},
				machinetypes.E4A.Name(): {
					"e4a-1": {apiv1.ResourceCPU: *resource.NewMilliQuantity(300, resource.DecimalSI)},
					"e4a-2": {apiv1.ResourceCPU: *resource.NewMilliQuantity(400, resource.DecimalSI)},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resizableVmLimiters := map[string]resizableVmLimitProvider{}
			for family, limit := range tt.limits {
				resizableVmLimiters[family] = &fakeResizableVmLimitProvider{limit: limit}
			}
			w := &workloadSeparationLimiter{
				resizableVmLimiters: resizableVmLimiters,
			}
			got := w.Limit(tt.requests)
			assert.Equal(t, tt.want, got)
		})
	}
}
