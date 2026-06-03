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

	"github.com/stretchr/testify/assert"

	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability"
	pod_util "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	test_util "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/filter"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/multitenancy"
)

const (
	fooTenantSystemNamespace   = "t123-foo-system"
	fooTenantWorkloadNamespace = "t123-foo-workload"
	fooSupervisorNamespace     = "t123-foo-supervisor"
	fooUID                     = "foo-uid"

	barTenantSystemNamespace   = "t456-bar-system"
	barTenantWorkloadNamespace = "t456-bar-workload"
	barSupervisorNamespace     = "t456-bar-supervisor"
	barUID                     = "bar-uid"
)

var (
	fooUserWorkload = buildTestPod(fooTenantWorkloadNamespace, "workload", test_util.WithLabels(map[string]string{
		multitenancy.TenantUIDLabel:    fooUID,
		multitenancy.TenantAccessLabel: multitenancy.TenantAccessValue,
	}))
	fooSystemWorkload = buildTestPod(fooTenantSystemNamespace, "system", test_util.WithLabels(map[string]string{
		multitenancy.TenantUIDLabel:    fooUID,
		multitenancy.TenantAccessLabel: multitenancy.TenantAccessValue,
	}))
	fooSupervisorWorkload = buildTestPod(fooSupervisorNamespace, "supervisor", test_util.WithLabels(map[string]string{
		multitenancy.TenantUIDLabel:    fooUID,
		multitenancy.TenantAccessLabel: multitenancy.SupervisorAccessValue,
	}))
	fooSupervisorSystemWorkload = buildTestPod(metav1.NamespaceSystem, "supervisor-system")

	barUserWorkload = buildTestPod(barTenantWorkloadNamespace, "workload", test_util.WithLabels(map[string]string{
		multitenancy.TenantUIDLabel:    barUID,
		multitenancy.TenantAccessLabel: multitenancy.TenantAccessValue,
	}))
	barSystemWorkload = buildTestPod(barTenantSystemNamespace, "system", test_util.WithLabels(map[string]string{
		multitenancy.TenantUIDLabel:    barUID,
		multitenancy.TenantAccessLabel: multitenancy.TenantAccessValue,
	}))

	fooTenantNodeLabels = map[string]string{
		multitenancy.TenantUIDLabel:    fooUID,
		multitenancy.TenantAccessLabel: multitenancy.TenantAccessValue,
	}
	supervisorNodeLabels = map[string]string{
		multitenancy.TenantAccessLabel: multitenancy.SupervisorAccessValue,
	}
	barTenantNodeLabels = map[string]string{
		multitenancy.TenantUIDLabel:    barUID,
		multitenancy.TenantAccessLabel: multitenancy.TenantAccessValue,
	}
)

func TestMultitenantScaleToZeroProcessor(t *testing.T) {
	testCases := map[string]struct {
		nodeInfos             []*framework.NodeInfo
		unschedulablePods     []*apiv1.Pod
		wantUnschedulablePods []*apiv1.Pod
		tenantsFilteredOut    map[string]bool
		supervisorFilteredOut bool
	}{
		"2 tenants, 1 tenant with user workloads and 1 tenant with only system workload": {
			unschedulablePods: []*apiv1.Pod{
				fooSystemWorkload, fooUserWorkload, barSystemWorkload,
			},
			wantUnschedulablePods: []*apiv1.Pod{
				fooSystemWorkload, fooUserWorkload,
			},
			tenantsFilteredOut: map[string]bool{
				barUID: true,
			},
		},
		"2 tenants, 1 tenant supervisor and system workload and 1 tenant with only system workload": {
			unschedulablePods: []*apiv1.Pod{
				fooSystemWorkload, fooSupervisorWorkload, barSystemWorkload,
			},
			wantUnschedulablePods: []*apiv1.Pod{
				fooSupervisorWorkload,
			},
			tenantsFilteredOut: map[string]bool{
				barUID: true,
				fooUID: true,
			},
		},
		"1 tenant, supervisor with system workload": {
			unschedulablePods: []*apiv1.Pod{
				fooUserWorkload, fooSupervisorSystemWorkload,
			},
			wantUnschedulablePods: []*apiv1.Pod{
				fooUserWorkload,
			},
			supervisorFilteredOut: true,
		},
		"1 tenant with scheduled user pod": {
			nodeInfos: []*framework.NodeInfo{
				buildTestNodeInfo("system", []*apiv1.Pod{fooSystemWorkload, fooSystemWorkload}, withNodeLabels(fooTenantNodeLabels)),
				buildTestNodeInfo("workload", []*apiv1.Pod{fooUserWorkload}, withNodeLabels(fooTenantNodeLabels)),
			},
		},
		"1 tenant with pending user pod": {
			nodeInfos: []*framework.NodeInfo{
				buildTestNodeInfo("system", []*apiv1.Pod{fooSystemWorkload, fooSystemWorkload}, withNodeLabels(fooTenantNodeLabels)),
			},
			unschedulablePods: []*apiv1.Pod{
				fooUserWorkload,
			},
		},
		"2 tenants, 1 tenant with scheduled system and supervisor pods and 1 tenant with scheduled user pods": {
			nodeInfos: []*framework.NodeInfo{
				buildTestNodeInfo("system", []*apiv1.Pod{fooSystemWorkload, fooSystemWorkload}, withNodeLabels(fooTenantNodeLabels)),
				buildTestNodeInfo("supervisor", []*apiv1.Pod{fooSupervisorWorkload}, withNodeLabels(supervisorNodeLabels)),
				buildTestNodeInfo("workload", []*apiv1.Pod{barUserWorkload}, withNodeLabels(barTenantNodeLabels)),
			},
			tenantsFilteredOut: map[string]bool{
				fooUID: true,
			},
		},
		"1 tenant with scheduled supervisor system pods": {
			nodeInfos: []*framework.NodeInfo{
				buildTestNodeInfo("supervisor-system", []*apiv1.Pod{fooSupervisorSystemWorkload, fooSupervisorSystemWorkload}, withNodeLabels(supervisorNodeLabels)),
			},
			unschedulablePods: []*apiv1.Pod{
				fooUserWorkload,
			},
			supervisorFilteredOut: true,
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

			allPods := tc.unschedulablePods
			for _, n := range tc.nodeInfos {
				for _, p := range n.Pods() {
					allPods = append(allPods, p.Pod)
				}
			}

			metricsFilter := filter.NewMetricsFilter()
			experimentsManager := experiments.NewMockManager(experiments.MultitenancyScaleToZeroProcessorFlag)
			// no grace period for testing purposes
			proc := NewMultitenantScaleToZeroPodListProcessor(
				metricsFilter, 0, systempods.NewMultitenantClassifier([]string{metav1.NamespaceSystem}), experimentsManager, "cluster_hash",
			)

			// ensure that pod drainability is undefinded outcome if the processor hasn't seen the tenant
			for _, pod := range allPods {
				drainStatus := proc.Drainable(nil, pod, nil)
				assert.Equal(t, drainability.UndefinedOutcome, drainStatus.Outcome)
			}

			newUnschedulable, err := proc.Process(context, tc.unschedulablePods)
			assert.NoError(t, err)
			if tc.wantUnschedulablePods != nil {
				assert.Equal(t, newUnschedulable, tc.wantUnschedulablePods)
			} else {
				assert.Equal(t, newUnschedulable, tc.unschedulablePods)
			}

			validateMultitenantSnapshotState(t, tc.nodeInfos, snapshot, tc.tenantsFilteredOut, tc.supervisorFilteredOut)

			for _, pod := range allPods {
				drainStatus := proc.Drainable(nil, pod, nil)
				tenantUID := pod.Labels[multitenancy.TenantUIDLabel]

				var shouldBeFiltered bool
				if multitenancy.IsSupervisorPod(pod) {
					shouldBeFiltered = tc.supervisorFilteredOut
				} else {
					shouldBeFiltered = tc.tenantsFilteredOut[tenantUID]
				}

				expectedOutcome := drainability.UndefinedOutcome
				if shouldBeFiltered {
					expectedOutcome = drainability.SkipDrain
				}
				assert.Equal(t, expectedOutcome, drainStatus.Outcome)

			}
		})
	}
}

func validateMultitenantSnapshotState(t *testing.T, before []*framework.NodeInfo, snapshot clustersnapshot.ClusterSnapshot, expectTenantsFilteredOut map[string]bool, expectSupervisorFilteredOut bool) {
	after, err := snapshot.ListNodeInfos()
	assert.NoError(t, err)
	beforeMap := nodeToPodMap(before)
	afterMap := nodeToPodMap(after)
	systemClassifier := systempods.NewMultitenantClassifier([]string{metav1.NamespaceSystem})
	for nodeName, beforePods := range beforeMap {
		afterPods, found := afterMap[nodeName]
		assert.True(t, found)
		for _, bp := range beforePods {
			isSystem := systemClassifier.IsSystemPod(bp)
			isDs := pod_util.IsDaemonSetPod(bp)
			isSupervisor := multitenancy.IsSupervisorPod(bp)
			expectFiltered := isSystem && !isDs
			if isSupervisor {
				expectFiltered = expectFiltered && expectSupervisorFilteredOut
			} else {
				tenant := bp.Labels[multitenancy.TenantUIDLabel]
				expectFiltered = expectFiltered && expectTenantsFilteredOut[tenant]
			}
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
