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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/scaledowncandidates"
	"k8s.io/autoscaler/cluster-autoscaler/processors/scaledowncandidates/emptycandidates"
	"k8s.io/autoscaler/cluster-autoscaler/processors/scaledowncandidates/previouscandidates"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
)

func BenchmarkScaleDownSorting(b *testing.B) {
	cp := NewMockCloudProvider()
	snapshot := testsnapshot.NewTestSnapshotOrDie(b)
	cp.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.N1)
	cp.On("IsAutopilotEnabled").Return(false)

	nodeInfos := make(map[string]*framework.NodeInfo)
	benchmarkManager := &BenchmarkGkeManager{
		FakeGkeManager: gke.NewFakeGkeManager(nil),
		nodeInfos:      nodeInfos,
	}

	nodeGroups := 10
	nodesPerGroup := 500
	totalNodes := nodeGroups * nodesPerGroup

	allNodes := make([]*apiv1.Node, 0, totalNodes)
	crds := make([]crd.CRD, 0)

	// Create a CRD that matches even node groups
	testCrd := icCrd // Use the one from _test.go
	crds = append(crds, testCrd)

	for i := 0; i < nodeGroups; i++ {
		groupName := fmt.Sprintf("ng-%d", i)
		machineType := "n2-standard-4"
		if i%2 == 0 {
			machineType = "c3-standard-4" // Matches CRD priority
		}

		labels := map[string]string{}
		if i%2 == 0 {
			labels[testCrd.Label()] = testCrd.Name()
		}

		mig := gke.NewTestGkeMigBuilder().
			SetNodePoolName(groupName).
			SetGceRefName(groupName).
			SetSpec(&gkeclient.NodePoolSpec{
				Labels:      labels,
				MachineType: machineType,
				Spot:        true}).
			SetGkeManager(benchmarkManager).
			SetMinSize(-1).
			Build()

		cp.InsertNodeGroup(mig)

		// Create template node info for the MIG
		templateNode := &apiv1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   fmt.Sprintf("%s-template", groupName),
				Labels: labels,
			},
		}
		nodeInfos[mig.Id()] = framework.NewTestNodeInfo(templateNode)

		for j := 0; j < nodesPerGroup; j++ {
			nodeName := fmt.Sprintf("%s-node-%d", groupName, j)
			node := &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: nodeName},
			}
			cp.AddNode(mig.Id(), node)
			err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(node))
			assert.NoError(b, err)
			allNodes = append(allNodes, node)
		}
	}

	crdLister := lister.NewMockCrdLister(crds)
	crdLister.SetCrdLabel("crd-1")

	// 1. EmptySorting
	deleteOpts := options.NodeDeleteOptions{
		SkipNodesWithSystemPods:           true,
		SkipNodesWithLocalStorage:         true,
		SkipNodesWithCustomControllerPods: true,
	}
	drainabilityRules := rules.Default(deleteOpts)
	emptySorter := emptycandidates.NewEmptySortingProcessor(emptycandidates.NewNodeInfoGetter(snapshot), deleteOpts, drainabilityRules)

	// 2. PreviousCandidates
	previousCandidates := previouscandidates.NewPreviousCandidates()
	// No previous candidates for this benchmark

	// 3. crdScaleDownSortingProcessor
	crdSorter := NewCrdScaleDownSortingProcessor(crdLister, cp)

	scaleDownComparers := []scaledowncandidates.CandidatesComparer{
		emptySorter,
		previousCandidates,
		crdSorter,
	}

	sortingProcessor := scaledowncandidates.NewScaleDownCandidatesSortingProcessor(scaleDownComparers)
	ctx := &context.AutoscalingContext{
		CloudProvider:   cp,
		ClusterSnapshot: snapshot,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// GetScaleDownCandidates modifies the slice in place or returns a new sorted one.
		// To be safe and mimic real behavior where input list is fresh, we copy the slice.
		nodesCopy := make([]*apiv1.Node, len(allNodes))
		copy(nodesCopy, allNodes)

		_, err := sortingProcessor.GetScaleDownCandidates(ctx, nodesCopy)
		assert.NoError(b, err)
	}
}

type BenchmarkGkeManager struct {
	*gke.FakeGkeManager
	nodeInfos map[string]*framework.NodeInfo
}

func (m *BenchmarkGkeManager) GetMigTemplateNodeInfo(mig *gke.GkeMig) (*framework.NodeInfo, error) {
	if info, ok := m.nodeInfos[mig.Id()]; ok {
		return info, nil
	}
	return nil, fmt.Errorf("node info not found for %s", mig.Id())
}

func (m *BenchmarkGkeManager) GetNumberOfSurgeNodesInMig(mig *gke.GkeMig) int {
	return 0
}
