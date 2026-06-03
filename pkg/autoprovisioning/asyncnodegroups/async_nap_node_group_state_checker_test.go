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

package asyncnodegroups

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
)

func TestAsyncNapNodeGroupStateChecker(t *testing.T) {
	tests := []struct {
		name      string
		nodeGroup func() cloudprovider.NodeGroup
		expected  bool
	}{
		{
			name: "GkeMig without gkeManager",
			nodeGroup: func() cloudprovider.NodeGroup {
				return &gke.GkeMig{}
			},
			expected: false,
		},
		{
			name: "nil NodeGroup",
			nodeGroup: func() cloudprovider.NodeGroup {
				return nil
			},
			expected: false,
		},
		{
			name: "NodeGroup that is not a GkeMig",
			nodeGroup: func() cloudprovider.NodeGroup {
				return &gke.TestGkeNodeGroup{}
			},
			expected: false,
		},
		{
			name: "GkeMig with gkeManager identifying mig as upcoming",
			nodeGroup: func() cloudprovider.NodeGroup {
				gkeManager := &gke.GkeManagerMock{MockIsUpcoming: true}
				mig := gke.NewTestGkeMigBuilder().
					SetGkeManager(gkeManager).
					Build()
				gkeManager.On("IsUpcoming", mig).Return(true)
				return mig
			},
			expected: true,
		},
		{
			name: "GkeMig with gkeManager identifying mig as not upcoming",
			nodeGroup: func() cloudprovider.NodeGroup {
				gkeManager := &gke.GkeManagerMock{MockIsUpcoming: true}
				mig := gke.NewTestGkeMigBuilder().
					SetGkeManager(gkeManager).
					Build()
				gkeManager.On("IsUpcoming", mig).Return(false)
				return mig
			},
			expected: false,
		},
	}
	checker := NewAsyncNapNodeGroupStateChecker()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodeGroup := test.nodeGroup()
			result := checker.IsUpcoming(nodeGroup)
			assert.Equal(t, test.expected, result)
		})
	}
}
