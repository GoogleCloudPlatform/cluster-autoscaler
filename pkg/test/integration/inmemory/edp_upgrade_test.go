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

package inmemory

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

const (
	edpCpuRequestMillicores = 1200
	edpMemoryRequestBytes   = units.GiB
	edpCpuLabelValue        = "1200m"
)

type nodeState struct {
	version string
	tainted bool
}

// TestEdpUpgradeNodeTaintingAndProvisioning verifies Cluster Autoscaler's Extended Duration Pod (EDP)
// upgrade tainting (UpgradeNodeTaintingProcessor) and MIG scale-up blocking (BlockedMigsSource)
// across 1-step and 2-step (rollback-safe) GKE Control Plane upgrades (b/565692761, omg/103836).
func TestEdpUpgradeNodeTaintingAndProvisioning(t *testing.T) {
	testCases := []struct {
		name                   string
		currentMasterVersion   string
		currentEmulatedVersion string
		initialNodePools       []*gke_api_beta.NodePool
		podsToSchedule         []*apiv1.Pod
		wantNodePoolCount      int
		wantNodes              []nodeState
	}{
		{
			name:                 "no upgrade",
			currentMasterVersion: "1.34.9-gke.1655001",
			initialNodePools:     []*gke_api_beta.NodePool{edpNodePool("1.34.9-gke.1655001")},
			podsToSchedule:       []*apiv1.Pod{newUnschedulableEdpPod("edp-pod-2")},
			wantNodePoolCount:    1,
			wantNodes: []nodeState{
				{version: "1.34.9-gke.1655001", tainted: false},
				{version: "1.34.9-gke.1655001", tainted: false},
			},
		},
		{
			name:                 "upgrade complete",
			currentMasterVersion: "1.35.6-gke.1250000",
			initialNodePools:     []*gke_api_beta.NodePool{edpNodePool("1.34.9-gke.1655001")},
			podsToSchedule:       []*apiv1.Pod{newUnschedulableEdpPod("edp-pod-2")},
			wantNodePoolCount:    2,
			wantNodes: []nodeState{
				{version: "1.34.9-gke.1655001", tainted: true},
				{version: "1.35.6-gke.1250000", tainted: false},
			},
		},
		{
			name:                   "rollback-safe: scales existing pool",
			currentMasterVersion:   "1.35.6-gke.1250000",
			currentEmulatedVersion: "1.34",
			initialNodePools:       []*gke_api_beta.NodePool{edpNodePool("1.34.9-gke.1655001")},
			podsToSchedule:         []*apiv1.Pod{newUnschedulableEdpPod("edp-pod-2")},
			wantNodePoolCount:      1,
			wantNodes: []nodeState{
				{version: "1.34.9-gke.1655001", tainted: false},
				{version: "1.34.9-gke.1655001", tainted: false},
			},
		},
		{
			name:                   "rollback-safe: NAP pool not tainted",
			currentMasterVersion:   "1.35.6-gke.1250000",
			currentEmulatedVersion: "1.34",
			initialNodePools:       nil,
			podsToSchedule:         []*apiv1.Pod{newUnschedulableEdpPod("edp-pod-1"), newUnschedulableEdpPod("edp-pod-2")},
			wantNodePoolCount:      1,
			wantNodes: []nodeState{
				{version: "1.34.0-gke.0", tainted: false},
				{version: "1.34.0-gke.0", tainted: false},
			},
		},
		{
			name:                   "rollback-safe: pool older than emulated minor",
			currentMasterVersion:   "1.35.6-gke.1250000",
			currentEmulatedVersion: "1.34",
			initialNodePools:       []*gke_api_beta.NodePool{edpNodePool("1.33.9-gke.1000000")},
			podsToSchedule:         []*apiv1.Pod{newUnschedulableEdpPod("edp-pod-2")},
			wantNodePoolCount:      2,
			wantNodes: []nodeState{
				{version: "1.33.9-gke.1000000", tainted: true},
				{version: "1.34.0-gke.0", tainted: false},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testConfig := integration.NewTestConfig().
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
					integration.WithEdpAutopilotOptions(),
				).
				WithClusterOverrides(
					integration.WithClusterAutoProvisioningEnabled(),
					integration.WithAutoprovisioningLocations("us-central1-a"),
					integration.WithClusterZones("us-central1-a"),
					integration.WithCurrentMasterVersion(tc.currentMasterVersion),
					integration.WithCurrentEmulatedVersion(tc.currentEmulatedVersion),
				).
				WithNodePools(tc.initialNodePools...)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

				// If an initial EDP node pool exists, occupy its initial node with an EDP pod
				// so that scheduling a subsequent EDP pod requires either scaling up that pool
				// or provisioning a new node pool.
				if len(tc.initialNodePools) > 0 {
					infra.Fakes.K8s.AddPod(newUnschedulableEdpPod("edp-pod-initial"))
					infra.Fakes.RunScheduler(ctx, t)
				}

				for _, p := range tc.podsToSchedule {
					infra.Fakes.K8s.AddPod(p)
					// Trigger scale-up / node-pool creation and UpgradeNodeTaintingProcessor.
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 15*time.Second)

					// Schedule the pending pod onto the newly available untainted node,
					// and run one more CA loop so UpgradeNodeTaintingProcessor evaluates any newly created nodes.
					infra.Fakes.RunScheduler(ctx, t)
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 15*time.Second)
				}

				assert.Equal(t, tc.wantNodePoolCount, len(infra.Fakes.GkeService.GetAutoprovisionedNodePools()), "unexpected number of autoprovisioned EDP node pools")
				assert.ElementsMatch(t, tc.wantNodes, listNodeStates(ctx, t, infra), "unexpected node states")
				assert.Empty(t, listUnscheduledPods(ctx, t, infra), "expected all pods to be scheduled")
			})
		})
	}
}

func edpNodePool(version string) *gke_api_beta.NodePool {
	np := integration.DefaultNodePool(
		integration.WithNodePoolMachineType("n1-highcpu-2"),
		integration.WithNodePoolLocations("us-central1-a"),
		integration.WithNodePoolSize(1),
		integration.WithNodePoolLabels(map[string]string{
			gkelabels.ExtendedDurationPodsLabel: edpCpuLabelValue,
		}),
	)
	np.Version = version
	np.Autoscaling.Autoprovisioned = true
	return np
}

func newUnschedulableEdpPod(name string) *apiv1.Pod {
	return tu.BuildTestPod(
		name,
		edpCpuRequestMillicores,
		edpMemoryRequestBytes,
		tu.MarkUnschedulable(),
		pod.WithAnnotation("cluster-autoscaler.kubernetes.io/safe-to-evict", "false"),
		pod.WithNodeSelectorEntry(gkelabels.ExtendedDurationPodsLabel, edpCpuLabelValue),
	)
}

func listNodeStates(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure) []nodeState {
	t.Helper()
	nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Failed to list nodes: %v", err)
	}
	var states []nodeState
	for _, node := range nodes.Items {
		states = append(states, nodeState{
			version: strings.TrimPrefix(node.Status.NodeInfo.KubeletVersion, "v"),
			tainted: taints.HasTaint(&node, gkelabels.NotTargetGkeVersionLabel),
		})
	}
	return states
}

func listUnscheduledPods(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure) []string {
	t.Helper()
	pods, err := infra.Fakes.KubeClient.CoreV1().Pods(apiv1.NamespaceDefault).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Failed to list pods: %v", err)
	}
	var unscheduled []string
	for _, p := range pods.Items {
		if p.Spec.NodeName == "" {
			unscheduled = append(unscheduled, p.Name)
		}
	}
	return unscheduled
}
