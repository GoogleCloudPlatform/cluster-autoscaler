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

package podtopologyspread

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/annotator"
	clockutils "k8s.io/utils/clock/testing"
)

const timeFormat = "2006-01-02T15:04:05-0700"

type ptsEligibilityForTest struct {
	domainNames         []string
	domainDiscoveryName string
	constraintIdx       *int32
	podName             string
}

type domainDiscoveryConfig struct {
	name        string
	domainNames []string
	topologyKey string
}

type domainAssignment struct {
	domainKey         string
	domainName        string
	whenUnsatisfiable apiv1.UnsatisfiableConstraintAction
}

func TestProcess(t *testing.T) {
	now := time.Now()
	domainDiscovery1 := domainDiscoveryConfig{
		name:        "domain-discovery-1",
		topologyKey: "domain-dimension-1",
		domainNames: []string{
			"domain-1-A",
			"domain-1-B",
			"domain-1-C",
		},
	}
	domainDiscovery2 := domainDiscoveryConfig{
		name:        "domain-discovery-2",
		topologyKey: "domain-dimension-2",
		domainNames: []string{
			"domain-2-A",
			"domain-2-B",
			"domain-2-C",
		},
	}

	testCases := []struct {
		description                string
		pods                       []*apiv1.Pod
		nodeInfos                  []*framework.NodeInfo
		domainDiscoveries          []domainDiscoveryConfig
		expectedPodDomains         map[string][]domainAssignment
		expectedNewlyScheduledPods []string
	}{
		{
			description: "Normal Pod",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
					test.BuildTestPod("pod-1", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-2", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-2", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1, domainDiscovery2},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod": nil,
			},
		},
		{
			description: "Single PTS Pod",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
					test.BuildTestPod("pod-1", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-2", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-2", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1, domainDiscovery2},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-C", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
			},
		},
		{
			description: "Single PTS Pod - already has node selector",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
					withNodeSelector(domainDiscovery1.topologyKey, "random-value"),
				),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
					test.BuildTestPod("pod-1", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-2", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-2", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1, domainDiscovery2},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "random-value", whenUnsatisfiable: apiv1.DoNotSchedule},
				},
			},
		},
		{
			description: "Single PTS Pod - conflicting domain discoveries",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       "topologyKey",
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{"topologyKey": "domain-1-A"},
					),
					test.BuildTestPod("pod-1", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-2", 10, 10),
						map[string]string{"topologyKey": "domain-1-B"},
					),
					test.BuildTestPod("pod-2", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{
				{
					name:        "domain-discovery-1",
					topologyKey: "topologyKey",
					domainNames: []string{
						"domain-1-A",
						"domain-1-B",
						"domain-1-C",
					},
				},
				{
					name:        "domain-discovery-2",
					topologyKey: "topologyKey",
					domainNames: []string{
						"domain-2-A",
						"domain-2-B",
						"domain-2-C",
					},
				},
			},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod": {
					{domainKey: "topologyKey", domainName: "domain-1-C", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
			},
		},
		{
			description: "Single PTS Pod - new pod with ScheduleAnyway",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.ScheduleAnyway,
						MaxSkew:           1,
					}),
					test.WithCreationTimestamp(now.Add(-time.Minute)),
				),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
					test.BuildTestPod("pod-1", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-2", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-2", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1, domainDiscovery2},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-C", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
			},
		},
		{
			description: "Single PTS Pod - old pod with ScheduleAnyway",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.ScheduleAnyway,
						MaxSkew:           1,
					}),
					test.WithCreationTimestamp(now.Add(-6*time.Minute)),
				),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
					test.BuildTestPod("pod-1", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-2", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-2", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1, domainDiscovery2},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod": nil,
			},
		},
		{
			description: "Single PTS Pod - unknown age pod with ScheduleAnyway",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.ScheduleAnyway,
						MaxSkew:           1,
					}),
				),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
					test.BuildTestPod("pod-1", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-2", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-2", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1, domainDiscovery2},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod": nil,
			},
		},
		{
			description: "Many PTS Pods",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod-1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-pod-2", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-pod-3", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-pod-4", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-normal-pod-5", 1, 1),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
					test.BuildTestPod("pod-1", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-2", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-2", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1, domainDiscovery2},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod-1": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-C", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-2": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-A", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-3": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-B", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-4": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-C", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-normal-pod-5": nil,
			},
			expectedNewlyScheduledPods: []string{
				"new-pod-2",
				"new-pod-3",
			},
		},
		{
			description: "Many PTS Pods - different domain discoveries",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod-1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-pod-2", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery2.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-pod-3", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery2.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-pod-4", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-normal-pod-5", 1, 1),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
					test.BuildTestPod("pod-1", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-2", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-2", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1, domainDiscovery2},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod-1": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-C", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-2": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-A", whenUnsatisfiable: apiv1.ScheduleAnyway},
					{domainKey: domainDiscovery2.topologyKey, domainName: "domain-2-A", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-3": {
					{domainKey: domainDiscovery2.topologyKey, domainName: "domain-2-B", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-4": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-B", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-normal-pod-5": nil,
			},
			expectedNewlyScheduledPods: []string{
				"new-pod-4",
			},
		},
		{
			description: "Many PTS Pods with defrag and scaled-down nodes",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod-1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-pod-2", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-pod-3", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-pod-4", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-normal-pod-5", 1, 1),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
					test.BuildTestPod("pod-1", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-2", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-2", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
				framework.NewTestNodeInfo(
					withTaints(
						withNodeLabels(
							test.BuildTestNode("node-3", 10, 10),
							map[string]string{domainDiscovery1.topologyKey: "domain-1-C"},
						),
						apiv1.Taint{
							Key:    taints.ToBeDeletedTaint,
							Effect: apiv1.TaintEffectNoSchedule,
						},
					),
					test.BuildTestPod("pod-3", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
				framework.NewTestNodeInfo(
					withTaints(
						withNodeLabels(
							test.BuildTestNode("node-4", 10, 10),
							map[string]string{domainDiscovery1.topologyKey: "domain-1-C"},
						),
						apiv1.Taint{
							Key:    defrag.HardTaint,
							Effect: apiv1.TaintEffectNoSchedule,
						},
					),
					test.BuildTestPod("pod-4", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1, domainDiscovery2},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod-1": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-C", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-2": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-A", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-3": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-B", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-4": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-C", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-normal-pod-5": nil,
			},
			expectedNewlyScheduledPods: []string{
				"new-pod-2",
				"new-pod-3",
			},
		},
		{
			description: "Single PTS Pod with MatchLabels that are not matched",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"env": "prod"},
						},
					}),
				),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
					test.BuildTestPod("pod-1", 1, 1),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-2", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-2", 1, 1),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1, domainDiscovery2},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-A", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
			},
			expectedNewlyScheduledPods: []string{"new-pod"},
		},
		{
			description: "Many PTS Pods with MatchLabels that are not matched",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod-1", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"env": "prod"},
						},
					}),
				),
				test.BuildTestPod("new-pod-2", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"env": "prod"},
						},
					}),
				),
				test.BuildTestPod("new-pod-3", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"env": "prod"},
						},
					}),
				),
				test.BuildTestPod("new-pod-4", 1, 1,
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"env": "prod"},
						},
					}),
				),
				test.BuildTestPod("new-normal-pod-5", 1, 1),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
					test.BuildTestPod("pod-1", 1, 1),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-2", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-2", 1, 1),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1, domainDiscovery2},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod-1": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-A", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-2": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-A", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-3": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-A", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-4": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-A", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-normal-pod-5": nil,
			},
			expectedNewlyScheduledPods: []string{
				"new-pod-1",
				"new-pod-2",
				"new-pod-3",
				"new-pod-4",
			},
		},
		{
			description: "Many PTS Pods with MatchLabels that are matched",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod-1", 1, 1,
					test.WithLabels(map[string]string{"env": "prod"}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"env": "prod"},
						},
					}),
				),
				test.BuildTestPod("new-pod-2", 1, 1,
					test.WithLabels(map[string]string{"env": "prod"}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"env": "prod"},
						},
					}),
				),
				test.BuildTestPod("new-pod-3", 1, 1,
					test.WithLabels(map[string]string{"env": "prod"}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"env": "prod"},
						},
					}),
				),
				test.BuildTestPod("new-pod-4", 1, 1,
					test.WithLabels(map[string]string{"env": "prod"}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"env": "prod"},
						},
					}),
				),
				test.BuildTestPod("new-normal-pod-5", 1, 1),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
					test.BuildTestPod("pod-1", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-2", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-2", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-3", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-3", 1, 1, test.WithLabels(map[string]string{"env": "staging"})), // unmatched pod.
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-4", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-C"},
					),
					test.BuildTestPod("pod-4", 1, 1), // unmatched pod.
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1, domainDiscovery2},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod-1": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-C", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-2": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-A", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-3": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-B", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-4": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-C", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-normal-pod-5": nil,
			},
			expectedNewlyScheduledPods: []string{
				"new-pod-1",
				"new-pod-2",
				"new-pod-3",
				"new-pod-4",
			},
		},
		{
			description: "Many PTS Pods with MatchLabelKeys, MatchLabels and MatchExpressions that are matched",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod-1", 1, 1,
					test.WithLabels(map[string]string{
						"env":  "prod",
						"app":  "cluster-autoscaler",
						"tier": "backend",
					}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "cluster-autoscaler"},
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "tier",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"backend"},
								},
							},
						},
						MatchLabelKeys: []string{"env", "abc"},
					}),
				),
				test.BuildTestPod("new-pod-2", 1, 1,
					test.WithLabels(map[string]string{
						"env":  "prod",
						"app":  "cluster-autoscaler",
						"tier": "backend",
					}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "cluster-autoscaler"},
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "tier",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"backend"},
								},
							},
						},
						MatchLabelKeys: []string{"env", "abc"},
					}),
				),
				test.BuildTestPod("new-pod-3", 1, 1,
					test.WithLabels(map[string]string{
						"env":  "prod",
						"app":  "cluster-autoscaler",
						"tier": "backend",
					}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "cluster-autoscaler"},
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "tier",
									Operator: metav1.LabelSelectorOpNotIn,
									Values:   []string{"frontend"},
								},
							},
						},
						MatchLabelKeys: []string{"env", "abc"},
					}),
				),
				test.BuildTestPod("new-pod-4", 1, 1,
					test.WithLabels(map[string]string{
						"env":  "prod",
						"app":  "cluster-autoscaler",
						"tier": "backend",
					}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "cluster-autoscaler"},
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "tier",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"backend"},
								},
							},
						},
						MatchLabelKeys: []string{"env", "abc"},
					}),
				),
				test.BuildTestPod("new-normal-pod-5", 1, 1),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
					test.BuildTestPod("pod-1", 1, 1, test.WithLabels(map[string]string{
						"env":  "prod",
						"app":  "cluster-autoscaler",
						"tier": "backend",
					})),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-2", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-2", 1, 1,
						test.WithLabels(map[string]string{
							"env":  "prod",
							"app":  "cluster-autoscaler",
							"tier": "backend",
						}),
					),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-3", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-3", 1, 1,
						test.WithLabels(map[string]string{"env": "staging"}),
					), // Unmatched pod.
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-4", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-C"},
					),
					test.BuildTestPod("pod-4", 1, 1), // Unmatched pod.
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-5", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-C"},
					),
					test.BuildTestPod("pod-5", 1, 1,
						test.WithLabels(map[string]string{
							"env":  "staging", // Wrong env.
							"app":  "cluster-autoscaler",
							"tier": "backend",
						}),
					),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-6", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-C"},
					),
					test.BuildTestPod("pod-6", 1, 1,
						test.WithLabels(map[string]string{
							"env":  "prod",
							"app":  "vertical-autoscaler", // Wrong app.
							"tier": "backend",
						}),
					),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-7", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-C"},
					),
					test.BuildTestPod("pod-7", 1, 1,
						test.WithLabels(map[string]string{
							"env":  "prod",
							"app":  "cluster-autoscaler",
							"tier": "frontend", // Wrong tier.
						}),
					),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1, domainDiscovery2},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod-1": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-C", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-2": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-A", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-3": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-B", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-4": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-C", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-normal-pod-5": nil,
			},
			expectedNewlyScheduledPods: []string{
				"new-pod-1",
				"new-pod-2",
				"new-pod-3",
				"new-pod-4",
			},
		},
		{
			description: "Many PTS Pods in different namespaces with Zonal Spread (Verify Namespace scoping)",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod-ns-b-1", 1, 1,
					test.WithNamespace("ns-b"),
					test.WithLabels(map[string]string{"app": "my-app"}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "my-app"},
						},
					}),
				),
				test.BuildTestPod("new-pod-ns-b-2", 1, 1,
					test.WithNamespace("ns-b"),
					test.WithLabels(map[string]string{"app": "my-app"}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "my-app"},
						},
					}),
				),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
					test.BuildTestPod("pod-ns-a-1", 1, 1,
						test.WithNamespace("ns-a"),
						test.WithLabels(map[string]string{"app": "my-app"}),
					),
					test.BuildTestPod("pod-ns-a-2", 1, 1,
						test.WithNamespace("ns-a"),
						test.WithLabels(map[string]string{"app": "my-app"}),
					),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-2", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-B"},
					),
					test.BuildTestPod("pod-ns-a-3", 1, 1,
						test.WithNamespace("ns-a"),
						test.WithLabels(map[string]string{"app": "my-app"}),
					),
					test.BuildTestPod("pod-ns-a-4", 1, 1,
						test.WithNamespace("ns-a"),
						test.WithLabels(map[string]string{"app": "my-app"}),
					),
				),
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-3", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-C"},
					),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod-ns-b-1": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-A", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-ns-b-2": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-B", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
			},
			expectedNewlyScheduledPods: []string{
				"new-pod-ns-b-1",
				"new-pod-ns-b-2",
			},
		},
		{
			description: "Unhelpable PTS pod triggers backoff for all pods from same controller - pods are ignored by PTS processor",
			pods: func() []*apiv1.Pod {
				pod1 := test.BuildTestPod("new-pod-1", 1, 1,
					withOwnerReference("controller-uid"),
				)

				pod2 := test.BuildTestPod("new-pod-2", 1, 1,
					withUnhelpable(),
					withOwnerReference("controller-uid"),
				)

				pod3 := test.BuildTestPod("new-pod-3", 1, 1,
					withOwnerReference("controller-uid"),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       "domain-dimension-1",
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				)

				return []*apiv1.Pod{pod1, pod2, pod3}
			}(),
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{"domain-dimension-1": "domain-1-A"},
					),
					test.BuildTestPod("pod-1", 1, 1, test.WithLabels(map[string]string{"env": "prod"})),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{
				{
					name:        "domain-discovery-1",
					topologyKey: "domain-dimension-1",
					domainNames: []string{"domain-1-A", "domain-1-B"},
				},
			},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod-1": nil,
				"new-pod-2": nil,
				"new-pod-3": nil,
			},
			expectedNewlyScheduledPods: nil,
		},
		{
			description: "PTS pods not in backoff - resolved unhelpable annotation does not trigger backoff",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod-1", 1, 1,
					withOwnerReference("controller-uid"),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-pod-2", 1, 1,
					withOwnerReference("controller-uid"),
					withAnnotations(map[string]string{annotator.UnhelpableUntilAnnotation: now.Format(timeFormat)}),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod-1": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-A", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
				"new-pod-2": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-B", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
			},
			expectedNewlyScheduledPods: []string{"new-pod-1"},
		},
		{
			description: "Unhelpable PTS pod triggers backoff - does not affect other controllers",
			pods: []*apiv1.Pod{
				test.BuildTestPod("new-pod-1", 1, 1,
					withOwnerReference("controller-a"),
					withUnhelpable(),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-pod-2", 1, 1,
					withOwnerReference("controller-a"),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
				test.BuildTestPod("new-pod-3", 1, 1,
					withOwnerReference("controller-b"),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1},
			expectedPodDomains: map[string][]domainAssignment{
				"new-pod-1": nil,
				"new-pod-2": nil,
				"new-pod-3": {
					{domainKey: domainDiscovery1.topologyKey, domainName: "domain-1-A", whenUnsatisfiable: apiv1.ScheduleAnyway},
				},
			},
			expectedNewlyScheduledPods: []string{"new-pod-3"},
		},
		{
			description: "Unhelpable bare PTS pod triggers backoff for itself",
			pods: []*apiv1.Pod{
				test.BuildTestPod("bare-pod", 1, 1,
					withUnhelpable(),
					withPodTopologySpread(apiv1.TopologySpreadConstraint{
						TopologyKey:       domainDiscovery1.topologyKey,
						WhenUnsatisfiable: apiv1.DoNotSchedule,
						MaxSkew:           1,
					}),
				),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					withNodeLabels(
						test.BuildTestNode("node-1", 10, 10),
						map[string]string{domainDiscovery1.topologyKey: "domain-1-A"},
					),
				),
			},
			domainDiscoveries: []domainDiscoveryConfig{domainDiscovery1},
			expectedPodDomains: map[string][]domainAssignment{
				"bare-pod": nil,
			},
			expectedNewlyScheduledPods: nil,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			var unschedulablePods []*apiv1.Pod
			for _, pod := range tc.pods {
				unschedulablePods = append(unschedulablePods, pod)
			}

			alreadyScheduledPods := make(map[string]bool)
			for _, nodeInfo := range tc.nodeInfos {
				for _, podInfo := range nodeInfo.Pods() {
					alreadyScheduledPods[podInfo.Pod.Name] = true
				}
			}

			var domainKeys []string
			var domainDiscoveries []PTSDomainDiscovery
			for _, domainDiscoveryConfig := range tc.domainDiscoveries {
				domainKeys = append(domainKeys, domainDiscoveryConfig.topologyKey)
				domainDiscoveries = append(domainDiscoveries, newFakeDomainDiscovery(domainDiscoveryConfig, tc.pods))
			}
			p := NewPodTopologySpreadProcessor(domainDiscoveries)
			p.clock = clockutils.NewFakePassiveClock(now)

			clusterSnapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for _, nodeInfo := range tc.nodeInfos {
				err := clusterSnapshot.AddNodeInfo(nodeInfo)
				if err != nil {
					t.Fatalf("failed to add node info: %v", err)
				}
			}
			autoscalingContext := &context.AutoscalingContext{
				ClusterSnapshot: clusterSnapshot,
			}

			p.Preprocess(unschedulablePods)
			unschedulablePods, err := p.Process(autoscalingContext, unschedulablePods)

			if err != nil {
				t.Fatalf("failed to process pods: %v", err)
			}

			allPodsAfterProcessing := make(map[string]*apiv1.Pod)
			for _, pod := range unschedulablePods {
				allPodsAfterProcessing[pod.Name] = pod
			}
			nodeInfos, err := clusterSnapshot.NodeInfos().List()
			if err != nil {
				t.Fatalf("failed to list node infos: %v", err)
			}
			var newlyScheduledPods []string
			for _, nodeInfo := range nodeInfos {
				for _, podInfo := range nodeInfo.GetPods() {
					pod := podInfo.GetPod()
					allPodsAfterProcessing[pod.Name] = pod
					if !alreadyScheduledPods[pod.Name] {
						newlyScheduledPods = append(newlyScheduledPods, pod.Name)
					}
				}
			}

			assert.ElementsMatch(t, tc.expectedNewlyScheduledPods, newlyScheduledPods)

			for _, originalPod := range tc.pods {
				pod, found := allPodsAfterProcessing[originalPod.Name]
				if !found {
					t.Errorf("pod %q disappeared after processing", originalPod.Name)
					continue
				}
				assignmentsByDomain := make(map[string]domainAssignment)
				for _, domain := range tc.expectedPodDomains[pod.Name] {
					assignmentsByDomain[domain.domainKey] = domain
				}
				for _, domainKey := range domainKeys {
					assignment, found := assignmentsByDomain[domainKey]
					if !found {
						if got, found := pod.Spec.NodeSelector[domainKey]; found {
							t.Errorf("got pod %q assigned to domain %q (for key %q), want no domain", pod.Name, got, domainKey)
						}
					} else {
						if got, want := pod.Spec.NodeSelector[domainKey], assignment.domainName; got != want {
							t.Errorf("got pod %q assigned to domain %q (for key %q), want %q", pod.Name, got, domainKey, want)
						}
						for _, constraint := range pod.Spec.TopologySpreadConstraints {
							if constraint.TopologyKey != domainKey {
								continue
							}
							if got, want := constraint.WhenUnsatisfiable, assignment.whenUnsatisfiable; got != want {
								t.Errorf("got pod %q constraint WhenUnsatisfiable %q, want %q", pod.Name, got, want)
							}
						}
					}
				}
			}
		})
	}
}

func TestFilterPTSConfigs(t *testing.T) {
	now := time.Now()

	testCases := []struct {
		name        string
		configs     []PTSConfig
		wantConfigs []PTSConfig
	}{
		{
			name: "ScheduleAnyway - old and unknown age pods are filtered out",
			configs: []PTSConfig{
				{
					pod: test.BuildTestPod("p1", 1, 1, test.WithCreationTimestamp(now.Add(-time.Minute))),
					constraint: &apiv1.TopologySpreadConstraint{
						WhenUnsatisfiable: apiv1.ScheduleAnyway,
					},
				},
				{
					pod: test.BuildTestPod("p2", 1, 1, test.WithCreationTimestamp(now.Add(-6*time.Minute))),
					constraint: &apiv1.TopologySpreadConstraint{
						WhenUnsatisfiable: apiv1.ScheduleAnyway,
					},
				},
				{
					pod: test.BuildTestPod("p3", 1, 1),
					constraint: &apiv1.TopologySpreadConstraint{
						WhenUnsatisfiable: apiv1.ScheduleAnyway,
					},
				},
			},
			wantConfigs: []PTSConfig{
				{
					pod: test.BuildTestPod("p1", 1, 1, test.WithCreationTimestamp(now.Add(-time.Minute))),
					constraint: &apiv1.TopologySpreadConstraint{
						WhenUnsatisfiable: apiv1.ScheduleAnyway,
					},
				},
			},
		},
		{
			name: "pods with node selector for topology key are filtered out",
			configs: []PTSConfig{
				{
					pod: test.BuildTestPod("p1", 1, 1),
					constraint: &apiv1.TopologySpreadConstraint{
						TopologyKey: "key",
					},
				},
				{
					pod: test.BuildTestPod("p1", 1, 1, withNodeSelector("key", "value")),
					constraint: &apiv1.TopologySpreadConstraint{
						TopologyKey: "key",
					},
				},
				{
					pod: test.BuildTestPod("p1", 1, 1, withNodeSelector("other-key", "value")),
					constraint: &apiv1.TopologySpreadConstraint{
						TopologyKey: "key",
					},
				},
			},
			wantConfigs: []PTSConfig{
				{
					pod: test.BuildTestPod("p1", 1, 1),
					constraint: &apiv1.TopologySpreadConstraint{
						TopologyKey: "key",
					},
				},
				{
					pod: test.BuildTestPod("p1", 1, 1, withNodeSelector("other-key", "value")),
					constraint: &apiv1.TopologySpreadConstraint{
						TopologyKey: "key",
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPodTopologySpreadProcessor(nil)
			p.clock = clockutils.NewFakePassiveClock(now)
			assert.Equal(t, tc.wantConfigs, p.filterPTSConfigs(tc.configs))
		})
	}
}

type fakeDomainDiscovery struct {
	configs []PTSConfig
}

func TestStatefulBackoff(t *testing.T) {
	testStart := time.Now()

	type preprocessEvent struct {
		timeSinceStart    time.Duration
		unschedulablePods []*apiv1.Pod
		expectedBackoffs  map[types.UID]time.Time
	}

	testCases := []struct {
		description string
		steps       []preprocessEvent
	}{
		{
			description: "Controller-wide backoff - applied, persisted and eventually expires",
			steps: []preprocessEvent{
				{
					timeSinceStart: 0,
					unschedulablePods: []*apiv1.Pod{
						test.BuildTestPod("older-pod", 1, 1,
							test.WithCreationTimestamp(testStart.Add(-10*time.Minute)),
							withOwnerReference("controller-uid"),
						),
						test.BuildTestPod("unhelpable-pod", 1, 1,
							test.WithCreationTimestamp(testStart.Add(-5*time.Minute)),
							withUnhelpable(),
							withOwnerReference("controller-uid"),
						),
					},
					expectedBackoffs: map[types.UID]time.Time{types.UID("controller-uid"): testStart},
				},
				{
					timeSinceStart: 10 * time.Second,
					unschedulablePods: []*apiv1.Pod{
						test.BuildTestPod("older-pod", 1, 1,
							test.WithCreationTimestamp(testStart.Add(-10*time.Minute)),
							withOwnerReference("controller-uid"),
						),
					},
					expectedBackoffs: map[types.UID]time.Time{types.UID("controller-uid"): testStart},
				},
				{
					timeSinceStart: 5 * time.Minute,
					unschedulablePods: []*apiv1.Pod{
						test.BuildTestPod("new-pod", 1, 1,
							test.WithCreationTimestamp(testStart.Add(5*time.Minute)),
							withOwnerReference("controller-uid"),
						),
					},
					expectedBackoffs: map[types.UID]time.Time{},
				},
				{
					timeSinceStart:    5*time.Minute + 10*time.Second,
					unschedulablePods: []*apiv1.Pod{},
					expectedBackoffs:  map[types.UID]time.Time{},
				},
			},
		},
		{
			description: "Old pod becomes helpable, but there are pods created before cutoff - still in backoff",
			steps: []preprocessEvent{
				{
					timeSinceStart: 0,
					unschedulablePods: []*apiv1.Pod{
						test.BuildTestPod("older-pod", 1, 1,
							test.WithCreationTimestamp(testStart.Add(-10*time.Minute)),
							withOwnerReference("controller-uid"),
						),
						test.BuildTestPod("unhelpable-pod", 1, 1,
							test.WithCreationTimestamp(testStart.Add(-5*time.Minute)),
							withUnhelpable(),
							withOwnerReference("controller-uid"),
						),
					},
					expectedBackoffs: map[types.UID]time.Time{types.UID("controller-uid"): testStart},
				},
				{
					timeSinceStart: 10 * time.Second,
					unschedulablePods: []*apiv1.Pod{
						test.BuildTestPod("older-pod", 1, 1,
							test.WithCreationTimestamp(testStart.Add(-10*time.Minute)),
							withOwnerReference("controller-uid"),
						),
						test.BuildTestPod("unhelpable-pod", 1, 1,
							test.WithCreationTimestamp(testStart.Add(-5*time.Minute)),
							withOwnerReference("controller-uid"),
						),
					},
					expectedBackoffs: map[types.UID]time.Time{types.UID("controller-uid"): testStart},
				},
			},
		},
		{
			description: "Fake pods with empty creation timestamps do not extend backoff",
			steps: []preprocessEvent{
				{
					timeSinceStart: 0,
					unschedulablePods: []*apiv1.Pod{
						test.BuildTestPod("fake-pod-1", 1, 1,
							withOwnerReference("controller-uid"),
						),
						test.BuildTestPod("fake-pod-2", 1, 1,
							withUnhelpable(),
							withOwnerReference("controller-uid"),
						),
					},
					expectedBackoffs: map[types.UID]time.Time{types.UID("controller-uid"): testStart},
				},
				{
					timeSinceStart: 10 * time.Second,
					unschedulablePods: []*apiv1.Pod{
						test.BuildTestPod("fake-pod-1", 1, 1,
							withOwnerReference("controller-uid"),
						),
						test.BuildTestPod("fake-pod-2", 1, 1,
							withUnhelpable(),
							withOwnerReference("controller-uid"),
						),
					},
					expectedBackoffs: map[types.UID]time.Time{types.UID("controller-uid"): testStart.Add(10 * time.Second)},
				},
				{
					timeSinceStart: 20 * time.Second,
					unschedulablePods: []*apiv1.Pod{
						test.BuildTestPod("fake-pod-1", 1, 1,
							withOwnerReference("controller-uid"),
						),
					},
					expectedBackoffs: map[types.UID]time.Time{},
				},
			},
		},
		{
			description: "Pods without controller have their own backoffs",
			steps: []preprocessEvent{
				{
					timeSinceStart: 0,
					unschedulablePods: []*apiv1.Pod{
						test.BuildTestPod("raw-pod-1", 1, 1),
						test.BuildTestPod("raw-pod-2", 1, 1,
							withUnhelpable(),
						),
						test.BuildTestPod("controlled-pod", 1, 1,
							withOwnerReference("controller-uid"),
						),
					},
					expectedBackoffs: map[types.UID]time.Time{types.UID("raw-pod-2"): testStart},
				},
				{
					timeSinceStart: 10 * time.Second,
					unschedulablePods: []*apiv1.Pod{
						test.BuildTestPod("raw-pod-1", 1, 1),
						test.BuildTestPod("controlled-pod", 1, 1,
							withOwnerReference("controller-uid"),
						),
					},
					expectedBackoffs: map[types.UID]time.Time{},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			p := NewPodTopologySpreadProcessor(nil)
			fakeClock := clockutils.NewFakePassiveClock(testStart)
			p.clock = fakeClock

			for i, step := range tc.steps {
				fakeClock.SetTime(testStart.Add(step.timeSinceStart))
				p.Preprocess(step.unschedulablePods)

				// Verify backoff state (UIDs and trigger timestamps)
				assert.Equal(t, step.expectedBackoffs, p.backoffs, "Step %d: Backoffs mismatch", i)
			}
		})
	}
}

func (f *fakeDomainDiscovery) EligiblePTSPods(pods []*apiv1.Pod) []PTSConfig {
	return f.configs
}

func newFakeDomainDiscovery(config domainDiscoveryConfig, pods []*apiv1.Pod) *fakeDomainDiscovery {
	var configs []PTSConfig
	for _, pod := range pods {
		idx := slices.IndexFunc(pod.Spec.TopologySpreadConstraints, func(c apiv1.TopologySpreadConstraint) bool {
			return c.TopologyKey == config.topologyKey
		})
		if idx == -1 {
			continue
		}
		configs = append(configs, ptsEligibilityFromTestOne(ptsEligibilityForTest{
			domainNames:         config.domainNames,
			domainDiscoveryName: config.name,
			constraintIdx:       proto.Int32(int32(idx)),
			podName:             pod.Name,
		}, pod))
	}
	return &fakeDomainDiscovery{
		configs: configs,
	}
}

func ptsEligibilityFromTestOnes(t *testing.T, eligibilities []ptsEligibilityForTest, pods []*apiv1.Pod) []PTSConfig {
	if len(eligibilities) == 0 {
		return nil
	}

	podsByName := make(map[string]*apiv1.Pod)
	for _, pod := range pods {
		podsByName[pod.Name] = pod
	}

	var ptsConfigs []PTSConfig
	for _, eligibility := range eligibilities {
		pod := podsByName[eligibility.podName]
		if pod == nil {
			t.Fatalf("Pod %q not found. All pods: %v", eligibility.podName, slices.Collect(maps.Keys(podsByName)))
		}
		ptsConfigs = append(ptsConfigs, ptsEligibilityFromTestOne(eligibility, pod))
	}
	return ptsConfigs
}

func ptsEligibilityFromTestOne(eligibility ptsEligibilityForTest, pod *apiv1.Pod) PTSConfig {
	var constraint *apiv1.TopologySpreadConstraint
	if eligibility.constraintIdx != nil {
		constraint = &pod.Spec.TopologySpreadConstraints[*eligibility.constraintIdx]
	}
	return PTSConfig{
		domainNames:         eligibility.domainNames,
		domainDiscoveryName: eligibility.domainDiscoveryName,
		constraint:          constraint,
		pod:                 pod,
	}
}

// withNodeSelector sets a node selector to the pod.
func withNodeSelector(k, v string) func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		if pod.Spec.NodeSelector == nil {
			pod.Spec.NodeSelector = make(map[string]string)
		}
		pod.Spec.NodeSelector[k] = v
	}
}

// withComputeClass sets a compute class to the pod.
func withComputeClass(computeClass string) func(*apiv1.Pod) {
	return withNodeSelector(gkelabels.ComputeClassLabel, computeClass)
}

// withPodTopologySpread adds a topology spread constraint to the pod.
func withPodTopologySpread(constraint apiv1.TopologySpreadConstraint) func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		pod.Spec.TopologySpreadConstraints = append(pod.Spec.TopologySpreadConstraints, constraint)
	}
}

// withNodeLabels sets node labels to the node.
func withNodeLabels(node *apiv1.Node, labels map[string]string) *apiv1.Node {
	for k, v := range labels {
		node.ObjectMeta.Labels[k] = v
	}
	return node
}

func withTaints(node *apiv1.Node, taints ...apiv1.Taint) *apiv1.Node {
	node.Spec.Taints = append(node.Spec.Taints, taints...)
	return node
}

// withAnnotations adds annotations to the pod.
func withAnnotations(annotations map[string]string) func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		for k, v := range annotations {
			pod.Annotations[k] = v
		}
	}
}

// withOwnerReference adds an owner reference to the pod.
func withOwnerReference(uid types.UID) func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		pod.OwnerReferences = append(pod.OwnerReferences, metav1.OwnerReference{
			UID:        uid,
			Controller: proto.Bool(true),
			Kind:       "ReplicaSet",
			Name:       "my-repset",
			APIVersion: "apps/v1",
		})
	}
}

// withUnhelpable marks the pod as unhelpable forever.
func withUnhelpable() func(*apiv1.Pod) {
	return withAnnotations(map[string]string{
		annotator.UnhelpableUntilAnnotation: annotator.UnhelpableForever,
	})
}
