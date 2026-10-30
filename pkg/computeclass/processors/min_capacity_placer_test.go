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
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestMinCapacityPodListProcessor_PlaceInSnapshot(t *testing.T) {
	crds := []crd.CRD{minCapacityCRD("my-ccc", 1)}
	ccLister := lister.NewMockCrdListerWithLabel(crds, labels.ComputeClassLabel)
	testCases := []struct {
		name           string
		ccLister       lister.Lister
		flags          map[string]bool
		wantErr        bool
		wantMinCapPods int
	}{
		{
			name:           "defrag-only placement enabled",
			ccLister:       ccLister,
			flags:          map[string]bool{},
			wantMinCapPods: 1,
		},
		{
			name:     "defrag-only placement disabled: the pod list processor places the pods",
			ccLister: ccLister,
			flags:    map[string]bool{experiments.ComputeClassMinCapacityDefragOnlyFakePodsEnabledFlag: false},
		},
		{
			name:     "min capacity disabled",
			ccLister: ccLister,
			flags:    map[string]bool{experiments.ComputeClassMinCapacityEnabledFlag: false},
		},
		{
			name:     "listing ComputeClasses fails",
			ccLister: failingCRDLister{ccLister},
			flags:    map[string]bool{},
			wantErr:  true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := experiments.NewMockManagerWithOptions(version.Version{}, tc.flags, map[string]string{})
			processor := NewMinCapacityPodListProcessor(tc.ccLister, nil, manager, nil, nil)
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			if err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(buildNodeWithCCC("n1", "my-ccc"))); err != nil {
				t.Fatalf("AddNodeInfo(\"n1\") failed: %v", err)
			}

			autoscalingCtx := &ca_context.AutoscalingContext{ClusterSnapshot: snapshot, CloudProvider: &minCapacityMockCloudProvider{}}

			remove, err := processor.PlaceInSnapshot(autoscalingCtx)

			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Errorf("PlaceInSnapshot() error = %v, want error: %t", err, tc.wantErr)
			}
			assert.Equal(t, tc.wantMinCapPods, mustCountMinCapPods(t, snapshot, "n1"), "min capacity pods on n1 after PlaceInSnapshot()")
			assert.NotNil(t, remove, "PlaceInSnapshot() remove func")
		})
	}
}

func mustCountMinCapPods(t *testing.T, snapshot clustersnapshot.ClusterSnapshot, nodeName string) int {
	t.Helper()
	nodeInfo, err := snapshot.GetNodeInfo(nodeName)
	if err != nil {
		t.Fatalf("GetNodeInfo(%q) failed: %v", nodeName, err)
	}
	return countMinCapacityPods(nodeInfo)
}

func TestMinCapacityPodListProcessor_PlaceInSnapshotRemoveDeletesOnlyThePodsItPlaced(t *testing.T) {
	ccLister := lister.NewMockCrdListerWithLabel([]crd.CRD{minCapacityCRD("my-ccc", 2)}, labels.ComputeClassLabel)
	manager := experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{}, map[string]string{})
	processor := NewMinCapacityPodListProcessor(ccLister, nil, manager, nil, nil)
	realPod := test.BuildTestPod("real", 100, 100)
	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	for _, nodeInfo := range []*framework.NodeInfo{
		framework.NewTestNodeInfo(buildNodeWithCCC("n1", "my-ccc"), realPod),
		framework.NewTestNodeInfo(buildNodeWithCCC("n2", "my-ccc")),
		framework.NewTestNodeInfo(buildNodeWithCCC("elsewhere", "")),
	} {
		if err := snapshot.AddNodeInfo(nodeInfo); err != nil {
			t.Fatalf("AddNodeInfo(%q) failed: %v", nodeInfo.Node().Name, err)
		}
	}
	autoscalingCtx := &ca_context.AutoscalingContext{ClusterSnapshot: snapshot, CloudProvider: &minCapacityMockCloudProvider{}}
	remove, err := processor.PlaceInSnapshot(autoscalingCtx)
	if err != nil {
		t.Fatalf("PlaceInSnapshot() failed: %v", err)
	}
	// A simulation copied the pod placed on n1 to a node without a
	// ComputeClass, keeping its name.
	n1, err := snapshot.GetNodeInfo("n1")
	if err != nil {
		t.Fatalf("GetNodeInfo(\"n1\") failed: %v", err)
	}
	var copied *apiv1.Pod
	for _, podInfo := range n1.Pods() {
		if crd.IsMinCapacityFakePod(podInfo.Pod) {
			copied = podInfo.Pod.DeepCopy()
		}
	}
	if copied == nil {
		t.Fatalf("PlaceInSnapshot() placed no min capacity pod on n1")
	}
	if err := snapshot.ForceAddPod(copied, "elsewhere"); err != nil {
		t.Fatalf("ForceAddPod(%q, \"elsewhere\") failed: %v", copied.Name, err)
	}
	// A min capacity pod that this call did not place.
	other := buildFakePod("other-ccc", nil, 0)
	if err := snapshot.ForceAddPod(other, "elsewhere"); err != nil {
		t.Fatalf("ForceAddPod(%q, \"elsewhere\") failed: %v", other.Name, err)
	}

	remove()

	assert.Equal(t, 0, mustCountMinCapPods(t, snapshot, "n1"), "min capacity pods on n1 after remove()")
	assert.Equal(t, 0, mustCountMinCapPods(t, snapshot, "n2"), "min capacity pods on n2 after remove()")
	assert.Equal(t, 1, mustCountMinCapPods(t, snapshot, "elsewhere"), "min capacity pods on elsewhere after remove()")
	n1, err = snapshot.GetNodeInfo("n1")
	if err != nil {
		t.Fatalf("GetNodeInfo(\"n1\") failed: %v", err)
	}
	if assert.Len(t, n1.Pods(), 1, "remove() must keep the real pod on n1") {
		assert.Equal(t, realPod.Name, n1.Pods()[0].Pod.Name)
	}
}
