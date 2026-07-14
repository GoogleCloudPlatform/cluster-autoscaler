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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/filter"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	pod_util "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
)

const (
	namespaceDefault = "default"
	namespaceCustom  = "hilbert"
)

func buildTestPod(namespace, name string, opts ...podOption) *apiv1.Pod {
	pod := BuildTestPod(name, 10, 10)
	pod.Namespace = namespace
	for _, opt := range opts {
		opt(pod)
	}
	return pod
}

type podOption func(*apiv1.Pod)

func withDaemonSetOwnerRef() podOption {
	return func(pod *apiv1.Pod) {
		pod.OwnerReferences = GenerateOwnerReferences(fmt.Sprintf("%s-ds", pod.Name), "DaemonSet", "apps/v1", "")
	}
}

func buildTestNodeInfo(name string, pods []*apiv1.Pod, opts ...nodeOption) *framework.NodeInfo {
	n := BuildTestNode(name, 1000, 1000)
	n.Name = name
	for _, opt := range opts {
		opt(n)
	}
	nodeInfo := framework.NewTestNodeInfo(n, pods...)
	return nodeInfo
}

type nodeOption func(*apiv1.Node)

func withNodeLabels(labels map[string]string) nodeOption {
	return func(node *apiv1.Node) {
		for k, v := range labels {
			node.Labels[k] = v
		}
	}
}

func withTaint(taint apiv1.Taint) nodeOption {
	return func(node *apiv1.Node) {
		node.Spec.Taints = append(node.Spec.Taints, taint)
	}
}

var (
	emptyNode        = buildTestNodeInfo("void", []*apiv1.Pod{})
	workloadNoDsNode = buildTestNodeInfo("mostly-void", []*apiv1.Pod{
		buildTestPod(namespaceDefault, "boltzman-brain"),
	})
	userDsNode = buildTestNodeInfo("bye-bye-entropy", []*apiv1.Pod{
		buildTestPod(namespaceDefault, "maxwelld", withDaemonSetOwnerRef()),
		buildTestPod(metav1.NamespaceSystem, "fluentd", withDaemonSetOwnerRef()),
		buildTestPod(metav1.NamespaceSystem, "metrics-server"),
	})
	mixedNode = buildTestNodeInfo("newtonian-gravity", []*apiv1.Pod{
		buildTestPod(metav1.NamespaceSystem, "heapster"),
		buildTestPod(metav1.NamespaceSystem, "metrics-server"),
		buildTestPod(namespaceDefault, "cannonball"),
		buildTestPod(metav1.NamespaceSystem, "fluentd", withDaemonSetOwnerRef()),
	})
	threeNamespacesNode = buildTestNodeInfo("triumvirate", []*apiv1.Pod{
		buildTestPod(metav1.NamespaceSystem, "caesar"),
		buildTestPod(namespaceCustom, "pompeius"),
		buildTestPod(namespaceDefault, "crassus"),
	})
	systemOnlyNode = buildTestNodeInfo("boring", []*apiv1.Pod{
		buildTestPod(metav1.NamespaceSystem, "metrics-server"),
		buildTestPod(metav1.NamespaceSystem, "fluentd", withDaemonSetOwnerRef()),
		buildTestPod(metav1.NamespaceSystem, "kube-dns"),
	}, withNodeLabels(map[string]string{
		nodePoolLabel:                 "default-pool",
		apiv1.LabelInstanceTypeStable: "e2-medium",
	}))
	quickRemoveNode = buildTestNodeInfo("quick-remove", []*apiv1.Pod{
		buildTestPod(metav1.NamespaceSystem, "metrics-server"),
		buildTestPod(metav1.NamespaceSystem, "fluentd", withDaemonSetOwnerRef()),
		buildTestPod(metav1.NamespaceSystem, "kube-dns"),
	}, withNodeLabels(map[string]string{
		nodePoolLabel: "default-pool",
	}), withTaint(apiv1.Taint{
		Key:    quickRemoveTaint,
		Value:  "true",
		Effect: apiv1.TaintEffectNoSchedule,
	}))
)

func TestNonSystemPodsPresent(t *testing.T) {
	testCases := map[string]struct {
		nodeInfos              []*framework.NodeInfo
		unschedulablePods      []*apiv1.Pod
		customSystemNamespaces []string
		wantUserPods           bool
	}{
		"empty cluster has no user pods": {},
		"user ds don't count": {
			nodeInfos: []*framework.NodeInfo{
				userDsNode,
			},
		},
		"multiple nodes, system pods and user ds": {
			nodeInfos: []*framework.NodeInfo{
				systemOnlyNode,
				userDsNode,
				emptyNode,
			},
			unschedulablePods: []*apiv1.Pod{
				buildTestPod(metav1.NamespaceSystem, "kube-dns"),
				buildTestPod(namespaceDefault, "laplaced", withDaemonSetOwnerRef()),
			},
		},
		"single node with user pod": {
			nodeInfos: []*framework.NodeInfo{
				workloadNoDsNode,
			},
			wantUserPods: true,
		},
		"single node with multiple system pods and users pod": {
			nodeInfos: []*framework.NodeInfo{
				mixedNode,
			},
			wantUserPods: true,
		},
		"multiple nodes, just one has user pod": {
			nodeInfos: []*framework.NodeInfo{
				emptyNode,
				mixedNode,
				systemOnlyNode,
			},
			unschedulablePods: []*apiv1.Pod{
				buildTestPod(metav1.NamespaceSystem, "kube-dns"),
			},
			wantUserPods: true,
		},
		"no user pods scheduled, but some are pending": {
			nodeInfos: []*framework.NodeInfo{
				emptyNode,
				systemOnlyNode,
				userDsNode,
			},
			unschedulablePods: []*apiv1.Pod{
				buildTestPod(metav1.NamespaceSystem, "kube-dns"),
				buildTestPod(namespaceDefault, "schrodinger-cat"),
			},
			wantUserPods: true,
		},
		"custom system namespace used": {
			nodeInfos: []*framework.NodeInfo{
				threeNamespacesNode,
			},
			customSystemNamespaces: []string{
				namespaceCustom,
			},
			wantUserPods: true,
		},
		"all namespaces ignored": {
			nodeInfos: []*framework.NodeInfo{
				threeNamespacesNode,
				mixedNode,
				userDsNode,
			},
			customSystemNamespaces: []string{
				namespaceCustom,
				namespaceDefault,
			},
		},
	}
	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			ignoredNamespaces := []string{metav1.NamespaceSystem}
			ignoredNamespaces = append(ignoredNamespaces, tc.customSystemNamespaces...)
			p := NewScaleToZeroPodListProcessor(nil, time.Second, systempods.NewClassifier(ignoredNamespaces))
			assert.Equal(t, tc.wantUserPods, p.blockingPodsPresent(tc.nodeInfos, tc.unschedulablePods))
		})
	}
}

type podMap map[string]map[string]*apiv1.Pod // node_name -> pod_name -> pod
func nodeToPodMap(infos []*framework.NodeInfo) podMap {
	result := podMap{}
	for _, ni := range infos {
		nodeName := ni.Node().Name
		result[nodeName] = map[string]*apiv1.Pod{}
		for _, podInfo := range ni.Pods() {
			result[nodeName][podInfo.Pod.Name] = podInfo.Pod
		}
	}
	return result
}

func validateSnapshotState(t *testing.T, before []*framework.NodeInfo, snapshot clustersnapshot.ClusterSnapshot, expectSystemPodsFiltered bool) {
	after, err := snapshot.ListNodeInfos()
	assert.NoError(t, err)
	beforeMap := nodeToPodMap(before)
	afterMap := nodeToPodMap(after)

	for nodeName, beforePods := range beforeMap {
		afterPods, found := afterMap[nodeName]
		assert.True(t, found)
		for _, bp := range beforePods {
			isSystem := bp.Namespace == metav1.NamespaceSystem
			isDs := pod_util.IsDaemonSetPod(bp)
			expectFiltered := expectSystemPodsFiltered && isSystem && !isDs
			if _, found := afterPods[bp.Name]; found == expectFiltered {
				sb := strings.Builder{}
				sb.WriteString("Node: ")
				sb.WriteString(nodeName)
				sb.WriteString(", pods before: [")
				for bn := range beforePods {
					sb.WriteString(bn)
					sb.WriteRune(' ')
				}
				sb.WriteString("], pods after: [")
				for an := range afterPods {
					sb.WriteString(an)
					sb.WriteRune(' ')
				}
				sb.WriteString("].")
				if expectFiltered {
					t.Fatalf("Expected pod %s to be filtered out, but found it in snapshot. %s", bp.Name, sb.String())
				} else {
					t.Fatalf("Pod %s missing from snapshot. %s", bp.Name, sb.String())
				}
			}
		}
	}
}

func TestScaleToZeroProcessor(t *testing.T) {
	testCases := map[string]struct {
		nodeInfos                   []*framework.NodeInfo
		unschedulablePods           []*apiv1.Pod
		emptyFor                    time.Duration
		expectClusterEmptyTimer     bool
		expectSystemPodsFiltered    bool
		expectedDrainabilityOutcome drainability.OutcomeType
	}{
		// Note: this must be true regardless of emptyFor,
		// otherwise CA restart would scale-up empty cluster
		"empty cluster stays empty": {
			unschedulablePods: []*apiv1.Pod{
				buildTestPod(metav1.NamespaceSystem, "kube-dns"),
			},
			expectClusterEmptyTimer:     true,
			expectSystemPodsFiltered:    true,
			expectedDrainabilityOutcome: drainability.UndefinedOutcome,
		},
		"empty cluster will scale-up if user pod is pending": {
			unschedulablePods: []*apiv1.Pod{
				buildTestPod(metav1.NamespaceSystem, "kube-dns"),
				buildTestPod(namespaceDefault, "schrodinger-cat"),
			},
			emptyFor:                    time.Hour,
			expectedDrainabilityOutcome: drainability.UndefinedOutcome,
		},
		"cluster with scheduled user pod is not empty": {
			nodeInfos: []*framework.NodeInfo{
				systemOnlyNode,
				mixedNode,
			},
			emptyFor:                    time.Hour,
			expectedDrainabilityOutcome: drainability.UndefinedOutcome,
		},
		"cluster with pending user pod is not empty": {
			nodeInfos: []*framework.NodeInfo{
				systemOnlyNode,
			},
			unschedulablePods: []*apiv1.Pod{
				buildTestPod(namespaceDefault, "schrodinger-cat"),
			},
			emptyFor:                    time.Hour,
			expectedDrainabilityOutcome: drainability.UndefinedOutcome,
		},
		"cluster with only system pods is empty, but not scaled to 0 before grace period": {
			nodeInfos: []*framework.NodeInfo{
				systemOnlyNode,
				emptyNode,
			},
			unschedulablePods: []*apiv1.Pod{
				buildTestPod(metav1.NamespaceSystem, "kube-dns"),
			},
			expectClusterEmptyTimer:     true,
			expectedDrainabilityOutcome: drainability.UndefinedOutcome,
		},
		"cluster with only system pods and user ds is empty, but not scaled to 0 before grace period": {
			nodeInfos: []*framework.NodeInfo{
				systemOnlyNode,
				userDsNode,
			},
			expectClusterEmptyTimer:     true,
			expectedDrainabilityOutcome: drainability.UndefinedOutcome,
		},
		"scheduled system pods are filtered after grace period": {
			nodeInfos: []*framework.NodeInfo{
				systemOnlyNode,
				userDsNode,
			},
			emptyFor:                    6 * time.Minute,
			expectClusterEmptyTimer:     true,
			expectSystemPodsFiltered:    true,
			expectedDrainabilityOutcome: drainability.SkipDrain,
		},
		"pending system pods are filtered out after grace period": {
			nodeInfos: []*framework.NodeInfo{
				emptyNode,
			},
			unschedulablePods: []*apiv1.Pod{
				buildTestPod(metav1.NamespaceSystem, "kube-dns"),
			},
			emptyFor:                    6 * time.Minute,
			expectClusterEmptyTimer:     true,
			expectSystemPodsFiltered:    true,
			expectedDrainabilityOutcome: drainability.SkipDrain,
		},
		"pending used ds are filtered out along system pods": {
			nodeInfos: []*framework.NodeInfo{
				systemOnlyNode,
				userDsNode,
			},
			unschedulablePods: []*apiv1.Pod{
				buildTestPod(metav1.NamespaceSystem, "kube-dns"),
				buildTestPod(namespaceDefault, "maxwelld", withDaemonSetOwnerRef()),
			},
			emptyFor:                    6 * time.Minute,
			expectClusterEmptyTimer:     true,
			expectSystemPodsFiltered:    true,
			expectedDrainabilityOutcome: drainability.SkipDrain,
		},
		"e2-medium default node respects grace period": {
			nodeInfos: []*framework.NodeInfo{
				systemOnlyNode,
			},
			unschedulablePods: []*apiv1.Pod{
				buildTestPod(metav1.NamespaceSystem, "kube-dns"),
				buildTestPod(namespaceDefault, "maxwelld", withDaemonSetOwnerRef()),
			},
			expectClusterEmptyTimer:     true,
			expectedDrainabilityOutcome: drainability.UndefinedOutcome,
		},
		"mixed node configuration respects grace period": {
			nodeInfos: []*framework.NodeInfo{
				systemOnlyNode,
				quickRemoveNode,
			},
			unschedulablePods: []*apiv1.Pod{
				buildTestPod(metav1.NamespaceSystem, "kube-dns"),
				buildTestPod(namespaceDefault, "maxwelld", withDaemonSetOwnerRef()),
			},
			expectClusterEmptyTimer:     true,
			expectedDrainabilityOutcome: drainability.UndefinedOutcome,
		},
		"quick remove node filtered out immediately": {
			nodeInfos: []*framework.NodeInfo{
				quickRemoveNode,
			},
			unschedulablePods: []*apiv1.Pod{
				buildTestPod(metav1.NamespaceSystem, "kube-dns"),
				buildTestPod(namespaceDefault, "maxwelld", withDaemonSetOwnerRef()),
			},
			expectClusterEmptyTimer:     true,
			expectSystemPodsFiltered:    true,
			expectedDrainabilityOutcome: drainability.SkipDrain,
		},
	}
	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for _, ni := range tc.nodeInfos {
				err := snapshot.AddNodeInfo(ni)
				assert.NoError(t, err)
			}
			context := &context.AutoscalingContext{
				ClusterSnapshot: snapshot,
			}

			metricsFilter := filter.NewMetricsFilter()
			proc := NewScaleToZeroPodListProcessor(metricsFilter, 5*time.Minute, systempods.NewClassifier([]string{metav1.NamespaceSystem}))
			proc.emptySince = time.Now().Add(-1 * tc.emptyFor)

			newUnschedulable, err := proc.Process(context, tc.unschedulablePods)
			assert.NoError(t, err)

			metricsFilteredPods := metricsFilter.FilterOutPods(tc.unschedulablePods)

			if tc.expectClusterEmptyTimer {
				assert.NotZero(t, proc.emptySince)
			} else {
				assert.Zero(t, proc.emptySince)
			}

			if tc.expectSystemPodsFiltered {
				assert.Empty(t, newUnschedulable)
				assert.Empty(t, metricsFilteredPods)
			} else {
				assert.ElementsMatch(t, tc.unschedulablePods, newUnschedulable)
				assert.ElementsMatch(t, tc.unschedulablePods, metricsFilteredPods)
			}
			validateSnapshotState(t, tc.nodeInfos, context.ClusterSnapshot, tc.expectSystemPodsFiltered)

			for _, nodeInfo := range tc.nodeInfos {
				drainabilityStatus := proc.Drainable(nil, nil, nodeInfo)
				assert.Equal(t, tc.expectedDrainabilityOutcome, drainabilityStatus.Outcome)
			}
		})
	}
}
