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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/pods"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

func TestFilterQueuedNodeGroupListProcessorProcess(t *testing.T) {
	podWithoutProvReq := &apiv1.Pod{}
	podWithProvReq := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				pods.DeprecatedProvisioningRequestPodAnnotationKey: "test",
			},
		},
	}

	tests := []struct {
		name              string
		nodeGroups        []cloudprovider.NodeGroup
		unschedulablePods []*apiv1.Pod
		wantNodeGroups    []cloudprovider.NodeGroup
	}{
		{
			name: "simple test case",
			nodeGroups: []cloudprovider.NodeGroup{
				buildMig("np-1", false),
				buildMig("np-2", false),
				buildMig("np-3", false),
				buildMig("np-4", false),
			},
			unschedulablePods: []*apiv1.Pod{podWithoutProvReq},
			wantNodeGroups: []cloudprovider.NodeGroup{
				buildMig("np-1", false),
				buildMig("np-2", false),
				buildMig("np-3", false),
				buildMig("np-4", false),
			},
		},
		{
			name: "some queued node groups",
			nodeGroups: []cloudprovider.NodeGroup{
				buildMig("np-1", true),
				buildMig("np-2", false),
				buildMig("np-3", true),
				buildMig("np-4", false),
			},
			unschedulablePods: []*apiv1.Pod{podWithoutProvReq},
			wantNodeGroups: []cloudprovider.NodeGroup{
				buildMig("np-2", false),
				buildMig("np-4", false),
			},
		},
		{
			name: "all queued node groups",
			nodeGroups: []cloudprovider.NodeGroup{
				buildMig("np-1", true),
				buildMig("np-2", true),
				buildMig("np-3", true),
				buildMig("np-4", true),
			},
			unschedulablePods: []*apiv1.Pod{podWithoutProvReq},
			wantNodeGroups:    []cloudprovider.NodeGroup{},
		},
		{
			name: "a lot of node groups some are queued node groups",
			nodeGroups: []cloudprovider.NodeGroup{
				buildMig("np-1", true),
				buildMig("np-2", false),
				buildMig("np-3", true),
				buildMig("np-4", false),
				buildMig("np-5", false),
				buildMig("np-6", false),
				buildMig("np-7", true),
				buildMig("np-8", true),
				buildMig("np-9", true),
			},
			unschedulablePods: []*apiv1.Pod{podWithoutProvReq},
			wantNodeGroups: []cloudprovider.NodeGroup{
				buildMig("np-2", false),
				buildMig("np-4", false),
				buildMig("np-5", false),
				buildMig("np-6", false),
			},
		},
		{
			name: "a lot of node groups some are queued node groups, pods consume provreq nodegroups are not filtered",
			nodeGroups: []cloudprovider.NodeGroup{
				buildMig("np-1", true),
				buildMig("np-2", false),
				buildMig("np-3", true),
				buildMig("np-4", false),
				buildMig("np-5", false),
				buildMig("np-6", false),
				buildMig("np-7", true),
				buildMig("np-8", true),
				buildMig("np-9", true),
			},
			unschedulablePods: []*apiv1.Pod{podWithProvReq},
			wantNodeGroups: []cloudprovider.NodeGroup{
				buildMig("np-1", true),
				buildMig("np-2", false),
				buildMig("np-3", true),
				buildMig("np-4", false),
				buildMig("np-5", false),
				buildMig("np-6", false),
				buildMig("np-7", true),
				buildMig("np-8", true),
				buildMig("np-9", true),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewFilterQueuedNodeGroupListProcessor(&mockNodeGroupListProcessor{})
			gotNodeGroups, _, err := p.Process(nil, tt.nodeGroups, nil, tt.unschedulablePods)
			assert.Nil(t, err)
			if !reflect.DeepEqual(gotNodeGroups, tt.wantNodeGroups) {
				t.Errorf("filterQueuedNodeGroupListProcessor.Process() got = %v, want %v", gotNodeGroups, tt.wantNodeGroups)
			}
		})
	}
}

func buildMig(nodePoolName string, queuedProvisioning bool) *gke.GkeMig {
	nodepoolSpec := &gkeclient.NodePoolSpec{MachineType: "e2-micro", QueuedProvisioning: queuedProvisioning}
	return gke.NewTestGkeMigBuilder().
		SetSpec(nodepoolSpec).
		SetNodePoolName(nodePoolName).
		SetGceRefName(nodePoolName).
		SetQueuedProvisioning(queuedProvisioning).
		Build()
}

type mockNodeGroupListProcessor struct {
}

func (p *mockNodeGroupListProcessor) Process(ctx *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo, unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	return nodeGroups, nodeInfos, nil
}

func (m *mockNodeGroupListProcessor) CleanUp() {
}
