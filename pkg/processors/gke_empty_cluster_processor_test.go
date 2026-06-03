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

package processors

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/callbacks"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

func TestNotEnoughTimeHasPassedSinceCreateCluster(t *testing.T) {
	server := test.NewHttpServerMock()
	defer server.Close()
	gkeManagerMock := &gke.GkeManagerMock{}
	gkeManagerMock.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.N1)

	gkeCloudProvider, err := gke.BuildGkeCloudProvider(gkeManagerMock, nil, nil, false, "us-test1", nil, false, false, nil, "", nil, nil, nil, 1000)
	assert.NoError(t, err)
	startTime := time.Now()

	testCases := map[string]struct {
		AllNodes          int
		ReadyNodes        int
		CurrentTimeDelay  time.Duration
		ClusterStarted    bool
		ScaleFromZeroFlag bool
		ExpectedOutput    bool
	}{
		"Scale from zero; no nodes, cluster started": {
			AllNodes:          0,
			ReadyNodes:        0,
			ScaleFromZeroFlag: true,
			ExpectedOutput:    false,
			ClusterStarted:    true,
		},
		"Scale from zero; no ready; no cluster started": {
			AllNodes:          1,
			ReadyNodes:        0,
			ScaleFromZeroFlag: true,
			ExpectedOutput:    true,
			ClusterStarted:    false,
		},
		"Scale from zero; no ready; cluster started": {
			AllNodes:          1,
			ReadyNodes:        0,
			ScaleFromZeroFlag: true,
			ExpectedOutput:    false,
			ClusterStarted:    true,
			CurrentTimeDelay:  1 * time.Minute,
		},
		"Scale from zero; no ready; cluster started recently": {
			AllNodes:          1,
			ReadyNodes:        0,
			ScaleFromZeroFlag: true,
			ExpectedOutput:    true,
			ClusterStarted:    true,
			CurrentTimeDelay:  1 * time.Second,
		},
		"Not Scale from zero; no nodes": {
			AllNodes:          0,
			ReadyNodes:        0,
			ScaleFromZeroFlag: false,
			ExpectedOutput:    true,
		},
		"Not Scale from zero; no ready; cluster started": {
			AllNodes:          1,
			ReadyNodes:        0,
			ScaleFromZeroFlag: false,
			ExpectedOutput:    false,
			ClusterStarted:    true,
			CurrentTimeDelay:  1 * time.Minute,
		},
		"Not Scale from zero; no ready; cluster started recently": {
			AllNodes:          1,
			ReadyNodes:        0,
			ScaleFromZeroFlag: false,
			ExpectedOutput:    true,
			ClusterStarted:    true,
			CurrentTimeDelay:  1 * time.Second,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			emptyClusterProcessor := NewGkeEmptyClusterProcessor()
			emptyClusterProcessor.provisioningTime = startTime
			if strings.Contains(tn, "cluster started") {
				gkeManagerMock.On("ClusterStarted").Return(tc.ClusterStarted, nil).Once()
			}
			autoscalingContext := &context.AutoscalingContext{
				CloudProvider:      gkeCloudProvider,
				AutoscalingOptions: config.AutoscalingOptions{ScaleUpFromZero: tc.ScaleFromZeroFlag},
				ProcessorCallbacks: callbacks.NewTestProcessorCallbacks(),
			}

			var allNodes []*apiv1.Node
			for i := 0; i < tc.AllNodes; i++ {
				allNodes = append(allNodes, &apiv1.Node{})
			}
			var readyNodes []*apiv1.Node
			for i := 0; i < tc.ReadyNodes; i++ {
				readyNodes = append(readyNodes, &apiv1.Node{})
			}
			abort, _ := emptyClusterProcessor.ShouldAbort(autoscalingContext, allNodes, readyNodes, startTime.Add(tc.CurrentTimeDelay))
			assert.EqualValues(t, tc.ExpectedOutput, abort)

		})
	}
}
