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

package tpu

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
)

func TestFilterOutNodesWithUnreadyResources(t *testing.T) {
	ready := buildTpuNode("n1", TpuResource{gkelabels.TpuV4PodsliceValue, "2x2x4", 4})
	unready := kubernetes.GetUnreadyNodeCopy(buildTpuNode("n2", TpuResource{gkelabels.TpuV4PodsliceValue, "2x2x4", 0}), kubernetes.ResourceUnready)
	withoutTpu := buildTpuNode("n3", TpuResource{gkelabels.TpuV4PodsliceValue, "2x2x4", 0})
	withoutTpuAsUnready := kubernetes.GetUnreadyNodeCopy(withoutTpu, kubernetes.ResourceUnready)

	p := TpuCustomResourcesProcessor{}
	gotAll, gotReady := p.FilterOutNodesWithUnreadyResources(&context.AutoscalingContext{}, []*v1.Node{ready, unready, withoutTpu}, []*v1.Node{ready, withoutTpu}, drasnapshot.NewEmptySnapshot())

	assert.ElementsMatch(t, gotAll, []*v1.Node{ready, unready, withoutTpuAsUnready})
	assert.ElementsMatch(t, gotReady, []*v1.Node{ready})
}

func TestFilterOutDRANodesWithUnreadyResources(t *testing.T) {
	node := test.BuildTestNode("dra-node", 1000, 1000)
	node.Labels[gkelabels.TPULabel] = gkelabels.TpuV4PodsliceValue
	node.Labels[gkelabels.TPUTopologyLabel] = "2x2x4"
	node.Labels[gkelabels.DraTpuNodeLabel] = "true"

	p := TpuCustomResourcesProcessor{}
	gotAll, gotReady := p.FilterOutNodesWithUnreadyResources(&context.AutoscalingContext{}, []*v1.Node{node}, []*v1.Node{node}, drasnapshot.NewEmptySnapshot())

	assert.ElementsMatch(t, gotAll, []*v1.Node{node})
	assert.ElementsMatch(t, gotReady, []*v1.Node{node})
}

func TestGetNodeResourceTargetsIgnoresDRANodes(t *testing.T) {
	node := test.BuildTestNode("dra-node", 1000, 1000)
	node.Labels[gkelabels.TPULabel] = gkelabels.TpuV4PodsliceValue
	node.Labels[gkelabels.TPUTopologyLabel] = "2x2x4"
	node.Labels[gkelabels.DraTpuNodeLabel] = "true"
	node.Status.Allocatable[tpu.ResourceGoogleTPU] = *resource.NewQuantity(4, resource.DecimalSI)

	p := TpuCustomResourcesProcessor{}
	targets, err := p.GetNodeResourceTargets(&context.AutoscalingContext{}, node, nil)

	assert.NoError(t, err)
	assert.Empty(t, targets)
}
