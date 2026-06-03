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

package reasons

import (
	"reflect"
	"testing"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/orchestrator"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/resourcequotas"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
)

func TestGetReasonAndMessage(t *testing.T) {
	tests := []struct {
		name              string
		skippedNodeGroups map[cloudprovider.NodeGroup]status.Reasons
		wantReason        string
		wantMessage       string
	}{
		{
			name: "provisioningRequestSingleZoneInfeasible",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-1", "nodepool-1"):          CouldNotScheduleAllPodsInSingleZone,
				testNodeGroup("test-mig-name-nodepool-2", "nodepool-2"):          ClusterSizeReachedSkippedReason,
				testNodeGroup("test-mig-name-nodepool-3", "nodepool-3"):          orchestrator.MaxLimitReachedReason,
				testNodeGroup("test-mig-name-nodepool-4", "nodepool-4"):          orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-5", "nodepool-5"):          orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-6", "nodepool-6"):          orchestrator.NotReadyReason,
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"):          NewCouldNotScheduleAnyPodsInNodePool([]string{"Insufficient memory"}),
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-9", "nodepool-9"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "ProvisioningRequestSingleZoneInfeasible",
			wantMessage: "Provisioning Request cannot be scheduled in a single zone of a single nodepool, affected nodepools: nodepool-1",
		},
		{
			name: "clusterSizeReachedReason",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-2", "nodepool-2"):          ClusterSizeReachedSkippedReason,
				testNodeGroup("test-mig-name-nodepool-3", "nodepool-3"):          orchestrator.MaxLimitReachedReason,
				testNodeGroup("test-mig-name-nodepool-4", "nodepool-4"):          orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-5", "nodepool-5"):          orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-6", "nodepool-6"):          orchestrator.NotReadyReason,
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"):          NewCouldNotScheduleAnyPodsInNodePool([]string{"Insufficient memory"}),
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-9", "nodepool-9"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "ClusterNodeLimitExceeded",
			wantMessage: "Max cluster size reached, affected nodepools: nodepool-2",
		},
		{
			name: "MaxLimitReachedReason",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-3", "nodepool-3"):          orchestrator.MaxLimitReachedReason,
				testNodeGroup("test-mig-name-nodepool-4", "nodepool-4"):          orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-5", "nodepool-5"):          orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-6", "nodepool-6"):          orchestrator.NotReadyReason,
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"):          NewCouldNotScheduleAnyPodsInNodePool([]string{"Insufficient memory"}),
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-9", "nodepool-9"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "NodepoolSizeReached",
			wantMessage: "Max nodepool size reached, affected nodepools: nodepool-3",
		},
		{
			name: "NewMaxResourceLimitReached",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-4", "nodepool-4"):          orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-5", "nodepool-5"):          orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-6", "nodepool-6"):          orchestrator.NotReadyReason,
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"):          NewCouldNotScheduleAnyPodsInNodePool([]string{"Insufficient memory"}),
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-9", "nodepool-9"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "OutOfResources",
			wantMessage: "Max cluster limit reached, nodepools out of resources: nodepool-4 (cpu)",
		},
		{
			name: "BackoffReason",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-5", "nodepool-5"):          orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-6", "nodepool-6"):          orchestrator.NotReadyReason,
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"):          NewCouldNotScheduleAnyPodsInNodePool([]string{"Insufficient memory"}),
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-9", "nodepool-9"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "NodepoolInBackoff",
			wantMessage: "Nodepool in backoff after failed scale-up, affected nodepools: nodepool-5",
		},
		{
			name: "NotReadyReason",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-6", "nodepool-6"):          orchestrator.NotReadyReason,
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"):          NewCouldNotScheduleAnyPodsInNodePool([]string{"Insufficient memory"}),
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-9", "nodepool-9"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "NodepoolNotReady",
			wantMessage: "Nodepool not ready for scale-up, affected nodepools: nodepool-6",
		},
		{
			name: "podsUnschedulableReason",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"):          NewCouldNotScheduleAnyPodsInNodePool([]string{"Insufficient memory"}),
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-9", "nodepool-9"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: nodepool-7 (Insufficient memory)",
		},
		{
			name: "podsUnschedulableDueToInsufficientMemory",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"):          NewCouldNotScheduleAnyPodsInNodePool([]string{"Insufficient memory"}),
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-9", "nodepool-9"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: nodepool-7 (Insufficient memory)",
		},
		{
			name: "podsUnschedulableDueToInsufficientMemoryAndGPU",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"):          NewCouldNotScheduleAnyPodsInNodePool([]string{"Insufficient memory", "Insufficient nvidia.com/gpu"}),
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-9", "nodepool-9"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: nodepool-7 (Insufficient memory, Insufficient nvidia.com/gpu)",
		},
		{
			name: "podsUnschedulableDueToToleration",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"):          NewCouldNotScheduleAnyPodsInNodePool([]string{"node(s) had untolerated taint {nvidia.com/gpu: present}"}),
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-9", "nodepool-9"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: nodepool-7 (node(s) had untolerated taint {nvidia.com/gpu: present})",
		},
		{
			name: "podsUnschedulableDueToInsufficientMemoryAndGPUMultipleNodes",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"):          NewCouldNotScheduleAnyPodsInNodePool([]string{"Insufficient memory", "Insufficient nvidia.com/gpu"}),
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-9", "nodepool-9"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
				testNodeGroup("test-mig-name-nodepool-10", "nodepool-10"):        NewCouldNotScheduleAnyPodsInNodePool([]string{"Insufficient memory"}),
				testNodeGroup("test-mig-name-nodepool-11", "nodepool-11"):        NewCouldNotScheduleAnyPodsInNodePool([]string{"Insufficient nvidia.com/gpu"}),
			},
			wantReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: nodepool-10 (Insufficient memory), nodepool-11 (Insufficient nvidia.com/gpu), nodepool-7 (Insufficient memory, Insufficient nvidia.com/gpu)",
		},
		{
			name: "podsUnschedulableDueToMismatchedNodeSelector",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"):          NewCouldNotScheduleAnyPodsInNodePool([]string{"node(s) didn't match Pod's node affinity/selector"}),
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-9", "nodepool-9"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: nodepool-7 (node(s) didn't match Pod's node affinity/selector)",
		},
		{
			name: "podsUnschedulableDueToMismatchedNodeSelectorAndToleration",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"):          NewCouldNotScheduleAnyPodsInNodePool([]string{"node(s) didn't match Pod's node affinity/selector"}),
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-9", "nodepool-9"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
				testNodeGroup("test-mig-name-nodepool-10", "nodepool-10"):        NewCouldNotScheduleAnyPodsInNodePool([]string{"node(s) had untolerated taint {nvidia.com/gpu: present}"}),
			},
			wantReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: nodepool-10 (node(s) had untolerated taint {nvidia.com/gpu: present}), nodepool-7 (node(s) didn't match Pod's node affinity/selector)",
		},
		{
			name: "unrecognizedReason",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-9", "nodepool-9"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "InternalErrorSkippedNodepool",
			wantMessage: "Unrecognized reasons for skipping nodepools, i.e. \"test reason\", affected nodepools: nodepool-8",
		},
		{
			name: "unrecognizedReasonMultipleNodepools",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):            orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-9", "nodepool-9"):            orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-10", "nodepool-10"):          orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-11", "nodepool-11"):          orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-12", "nodepool-12"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-13", "nodepool-13"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "InternalErrorSkippedNodepool",
			wantMessage: "Unrecognized reasons for skipping nodepools, i.e. \"test reason\", affected nodepools: nodepool-10, nodepool-11, nodepool-12, nodepool-8, nodepool-9",
		},
		{
			name: "unrecognizedReasonMultipleNodepools - cap the number of nodepools in message",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):            orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-9", "nodepool-9"):            orchestrator.NewSkippedReasons("test reason"), // omitted in message - latest nodepool alphabetically
				testNodeGroup("test-mig-name-nodepool-10", "nodepool-10"):          orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-11", "nodepool-11"):          orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-12", "nodepool-12"):          orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-13", "nodepool-13"):          orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-14", "nodepool-14"):          orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-15", "nodepool-15"):          orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-16", "nodepool-16"):          orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-17", "nodepool-17"):          orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-18", "nodepool-18"):          orchestrator.NewSkippedReasons("test reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-19", "nodepool-19"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "InternalErrorSkippedNodepool",
			wantMessage: "Unrecognized reasons for skipping nodepools, i.e. \"test reason\", affected nodepools: nodepool-10, nodepool-11, nodepool-12, nodepool-13, nodepool-14, nodepool-15, nodepool-16, nodepool-17, nodepool-18, nodepool-8, ...",
		},
		{
			name: "multipleUnrecognizedReasons - cap the number of reasons in message",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):            orchestrator.NewSkippedReasons("test8 reason"),
				testNodeGroup("test-mig-name-nodepool-9", "nodepool-9"):            orchestrator.NewSkippedReasons("test9 reason"), // omitted in message - latest reason alphabetically
				testNodeGroup("test-mig-name-nodepool-10", "nodepool-10"):          orchestrator.NewSkippedReasons("test10 reason"),
				testNodeGroup("test-mig-name-nodepool-11", "nodepool-11"):          orchestrator.NewSkippedReasons("test11 reason"),
				testNodeGroup("test-mig-name-nodepool-12", "nodepool-12"):          orchestrator.NewSkippedReasons("test12 reason"),
				testNodeGroup("test-mig-name-nodepool-13", "nodepool-13"):          orchestrator.NewSkippedReasons("test13 reason"),
				testNodeGroup("test-mig-name-nodepool-14", "nodepool-14"):          orchestrator.NewSkippedReasons("test14 reason"),
				testNodeGroup("test-mig-name-nodepool-15", "nodepool-15"):          orchestrator.NewSkippedReasons("test15 reason"),
				testNodeGroup("test-mig-name-nodepool-16", "nodepool-16"):          orchestrator.NewSkippedReasons("test16 reason"),
				testNodeGroup("test-mig-name-nodepool-17", "nodepool-17"):          orchestrator.NewSkippedReasons("test17 reason"),
				testNodeGroup("test-mig-name-nodepool-18", "nodepool-18"):          orchestrator.NewSkippedReasons("test18 reason"),
				nonQueuedTestNodeGroup("test-mig-name-nodepool-19", "nodepool-19"): orchestrator.NewSkippedReasons("test19 reason, non-queued np"),
			},
			wantReason:  "InternalErrorSkippedNodepool",
			wantMessage: "Unrecognized reasons for skipping nodepools, i.e. \"test10 reason\", affected nodepools: nodepool-10; \"test11 reason\", affected nodepools: nodepool-11; \"test12 reason\", affected nodepools: nodepool-12; \"test13 reason\", affected nodepools: nodepool-13; \"test14 reason\", affected nodepools: nodepool-14; \"test15 reason\", affected nodepools: nodepool-15; \"test16 reason\", affected nodepools: nodepool-16; \"test17 reason\", affected nodepools: nodepool-17; \"test18 reason\", affected nodepools: nodepool-18; \"test8 reason\", affected nodepools: nodepool-8; ...",
		},
		{
			name: "multipleUnrecognizedReasonMultipleNodepools - cap the number of nodepools in message",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):            orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-9", "nodepool-9"):            orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-10", "nodepool-10"):          orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-11", "nodepool-11"):          orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-12", "nodepool-12"):          orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-13", "nodepool-13"):          orchestrator.NewSkippedReasons("test reason"),
				testNodeGroup("test-mig-name-nodepool-14", "nodepool-14"):          orchestrator.NewSkippedReasons("test2 reason"),
				testNodeGroup("test-mig-name-nodepool-15", "nodepool-15"):          orchestrator.NewSkippedReasons("test2 reason"),
				testNodeGroup("test-mig-name-nodepool-16", "nodepool-16"):          orchestrator.NewSkippedReasons("test2 reason"),
				testNodeGroup("test-mig-name-nodepool-17", "nodepool-17"):          orchestrator.NewSkippedReasons("test2 reason"),
				testNodeGroup("test-mig-name-nodepool-18", "nodepool-18"):          orchestrator.NewSkippedReasons("test2 reason"), // omitted in message - latest nodepool alphabetically for latest reason
				nonQueuedTestNodeGroup("test-mig-name-nodepool-19", "nodepool-19"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "InternalErrorSkippedNodepool",
			wantMessage: "Unrecognized reasons for skipping nodepools, i.e. \"test reason\", affected nodepools: nodepool-10, nodepool-11, nodepool-12, nodepool-13, nodepool-8, nodepool-9; \"test2 reason\", affected nodepools: nodepool-14, nodepool-15, nodepool-16, nodepool-17, ...",
		},
		{
			name: "noQueuedNodepoolsReason",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				nonQueuedTestNodeGroup("test-mig-name-nodepool-9", "nodepool-9"): orchestrator.NewSkippedReasons("test reason, non-queued np"),
			},
			wantReason:  "NoQueuedNodepoolAvailable",
			wantMessage: "No nodepool with QueuedProvisioning enabled is available for scale up",
		},
		{
			name: "MaxLimitReachedReason - three nodegroups are affected, but only two nodepools",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-3", "nodepool-3"): orchestrator.MaxLimitReachedReason,
				testNodeGroup("test-mig-name-nodepool-4", "nodepool-4"): orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-5", "nodepool-3"): orchestrator.MaxLimitReachedReason,
				testNodeGroup("test-mig-name-nodepool-6", "nodepool-6"): orchestrator.NotReadyReason,
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"): orchestrator.MaxLimitReachedReason,
			},
			wantReason:  "NodepoolSizeReached",
			wantMessage: "Max nodepool size reached, affected nodepools: nodepool-3, nodepool-7",
		},
		{
			name: "BackoffReason - cap the number of nodepools in message",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-0", "nodepool-0"):   orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-1", "nodepool-1"):   orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-2", "nodepool-2"):   orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-3", "nodepool-3"):   orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-4", "nodepool-4"):   orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-5", "nodepool-5"):   orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-6", "nodepool-6"):   orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"):   orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):   orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-9", "nodepool-9"):   orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-91", "nodepool-91"): orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-92", "nodepool-92"): orchestrator.BackoffReason,
			},
			wantReason:  "NodepoolInBackoff",
			wantMessage: "Nodepool in backoff after failed scale-up, affected nodepools: nodepool-0, nodepool-1, nodepool-2, nodepool-3, nodepool-4, nodepool-5, nodepool-6, nodepool-7, nodepool-8, nodepool-9, ...",
		},
		{
			name: "NewMaxResourceLimitReached - cap the number of nodepools in message",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-0", "nodepool-0"):   orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-1", "nodepool-1"):   orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-2", "nodepool-2"):   orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-3", "nodepool-3"):   orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-4", "nodepool-4"):   orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-5", "nodepool-5"):   orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-6", "nodepool-6"):   orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-7"):   orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-8", "nodepool-8"):   orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-9", "nodepool-9"):   orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-91", "nodepool-91"): orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-92", "nodepool-92"): orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
			},
			wantReason:  "OutOfResources",
			wantMessage: "Max cluster limit reached, nodepools out of resources: nodepool-0 (cpu), nodepool-1 (cpu), nodepool-2 (cpu), nodepool-3 (cpu), nodepool-4 (cpu), nodepool-5 (cpu), nodepool-6 (cpu), nodepool-7 (cpu), nodepool-8 (cpu), nodepool-9 (cpu), ...",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReason, gotMessage := GetReasonAndMessage(tt.skippedNodeGroups)
			if gotReason != tt.wantReason {
				t.Errorf("getReasonAndMessage() gotReason = %s, want %s", gotReason, tt.wantReason)
			}
			if gotMessage != tt.wantMessage {
				t.Errorf("getReasonAndMessage() gotMessage = %s, want %s", gotMessage, tt.wantMessage)
			}
		})
	}
}

func TestResourceLimitEntryFindMatch(t *testing.T) {
	tests := []struct {
		name              string
		skippedNodeGroups map[cloudprovider.NodeGroup]status.Reasons
		wantFound         bool
		wantReason        string
		wantMessage       string
	}{
		{
			name: "only cpu missing for one nodegroup",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-1", "nodepool-1"): orchestrator.NewSkippedReasons("example reasons"),
				testNodeGroup("test-mig-name-nodepool-2", "nodepool-2"): orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
			},
			wantFound:   true,
			wantReason:  "example response",
			wantMessage: "example message prefix, nodepools out of resources: nodepool-2 (cpu)",
		},
		{
			name: "each nodegroup missing different resource",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-1", "nodepool-1"): orchestrator.NewSkippedReasons("example reasons"),
				testNodeGroup("test-mig-name-nodepool-2", "nodepool-2"): orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-3", "nodepool-3"): orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"memory"}}}),
			},
			wantFound:   true,
			wantReason:  "example response",
			wantMessage: "example message prefix, nodepools out of resources: nodepool-2 (cpu), nodepool-3 (memory)",
		},
		{
			name: "nodegroups missing multiple resources",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-1", "nodepool-1"): orchestrator.NewSkippedReasons("example reasons"),
				testNodeGroup("test-mig-name-nodepool-2", "nodepool-2"): orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu", "memory"}}}),
				testNodeGroup("test-mig-name-nodepool-3", "nodepool-3"): orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"tpu-v4-podslice"}}}),
			},
			wantFound:   true,
			wantReason:  "example response",
			wantMessage: "example message prefix, nodepools out of resources: nodepool-2 (cpu, memory), nodepool-3 (tpu-v4-podslice)",
		},
		{
			name: "nothing matching",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-1", "nodepool-1"): orchestrator.NewSkippedReasons("example reasons"),
				testNodeGroup("test-mig-name-nodepool-2", "nodepool-2"): orchestrator.NewSkippedReasons("max cluster is not happy"),
			},
			wantFound:   false,
			wantReason:  "",
			wantMessage: "",
		},
		{
			name: "two nodegroups missing multiple resources, but both are from the same nodepool",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-1", "nodepool-1"): orchestrator.NewSkippedReasons("example reasons"),
				testNodeGroup("test-mig-name-nodepool-2", "nodepool-2"): orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu", "memory"}}}),
				testNodeGroup("test-mig-name-nodepool-3", "nodepool-2"): orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"tpu-v4-podslice"}}}),
			},
			wantFound:   true,
			wantReason:  "example response",
			wantMessage: "example message prefix, nodepools out of resources: nodepool-2 (cpu, memory, tpu-v4-podslice)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createResourceLimitEntry("example response", "example message prefix")
			gotFound, gotReason, gotMessage := s.findMatch(tt.skippedNodeGroups)
			if gotFound != tt.wantFound {
				t.Errorf("resourceLimitEntry.findMatch() gotFound = %t, wantFound %t", gotFound, tt.wantFound)
			}
			if gotReason != tt.wantReason {
				t.Errorf("resourceLimitEntry.findMatch() gotReason = %s, wantReason %s", gotReason, tt.wantReason)
			}
			if gotMessage != tt.wantMessage {
				t.Errorf("resourceLimitEntry.findMatch() gotMessage = %s, wantMessage %s", gotMessage, tt.wantMessage)
			}
		})
	}
}

func TestTranslateKeysToNames(t *testing.T) {
	tests := []struct {
		name              string
		skippedNodeGroups map[cloudprovider.NodeGroup]status.Reasons
		want              map[string]status.Reasons
	}{
		{
			name: "simple test",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-1", "nodepool-1"): orchestrator.NewSkippedReasons("example reasons"),
				testNodeGroup("test-mig-name-nodepool-2", "nodepool-2"): orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
			},
			want: map[string]status.Reasons{
				"https://www.googleapis.com/compute/v1/projects/test-project/zones/test-zone1-a/instanceGroups/test-mig-name-nodepool-1": orchestrator.NewSkippedReasons("example reasons"),
				"https://www.googleapis.com/compute/v1/projects/test-project/zones/test-zone1-a/instanceGroups/test-mig-name-nodepool-2": orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
			},
		},
		{
			name: "some node groups come from the same nodepool",
			skippedNodeGroups: map[cloudprovider.NodeGroup]status.Reasons{
				testNodeGroup("test-mig-name-nodepool-1", "nodepool-1"): orchestrator.NewSkippedReasons("example reasons"),
				testNodeGroup("test-mig-name-nodepool-2", "nodepool-2"): orchestrator.MaxLimitReachedReason,
				testNodeGroup("test-mig-name-nodepool-3", "nodepool-3"): orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				testNodeGroup("test-mig-name-nodepool-4", "nodepool-2"): orchestrator.BackoffReason,
				testNodeGroup("test-mig-name-nodepool-5", "nodepool-5"): orchestrator.NotReadyReason,
				testNodeGroup("test-mig-name-nodepool-7", "nodepool-2"): orchestrator.NewSkippedReasons("test reason"),
			},
			want: map[string]status.Reasons{
				"https://www.googleapis.com/compute/v1/projects/test-project/zones/test-zone1-a/instanceGroups/test-mig-name-nodepool-1": orchestrator.NewSkippedReasons("example reasons"),
				"https://www.googleapis.com/compute/v1/projects/test-project/zones/test-zone1-a/instanceGroups/test-mig-name-nodepool-2": orchestrator.MaxLimitReachedReason,
				"https://www.googleapis.com/compute/v1/projects/test-project/zones/test-zone1-a/instanceGroups/test-mig-name-nodepool-3": orchestrator.NewMaxResourceLimitReached([]resourcequotas.ExceededQuota{{ID: "test", ExceededResources: []string{"cpu"}}}),
				"https://www.googleapis.com/compute/v1/projects/test-project/zones/test-zone1-a/instanceGroups/test-mig-name-nodepool-4": orchestrator.BackoffReason,
				"https://www.googleapis.com/compute/v1/projects/test-project/zones/test-zone1-a/instanceGroups/test-mig-name-nodepool-5": orchestrator.NotReadyReason,
				"https://www.googleapis.com/compute/v1/projects/test-project/zones/test-zone1-a/instanceGroups/test-mig-name-nodepool-7": orchestrator.NewSkippedReasons("test reason"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TranslateKeysToNames(tt.skippedNodeGroups); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TranslateKeysToNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func testNodeGroup(migName, nodepoolName string) cloudprovider.NodeGroup {
	return gke.NewTestGkeMigBuilder().
		SetGceRef(gce.GceRef{
			Project: "test-project",
			Zone:    "test-zone1-a",
			Name:    migName,
		}).
		SetNodePoolName(nodepoolName).
		SetQueuedProvisioning(true).
		Build()
}

func nonQueuedTestNodeGroup(migName, nodepoolName string) cloudprovider.NodeGroup {
	return gke.NewTestGkeMigBuilder().
		SetGceRef(gce.GceRef{
			Project: "test-project",
			Zone:    "test-zone1-a",
			Name:    migName,
		}).
		SetNodePoolName(nodepoolName).
		SetQueuedProvisioning(false).
		Build()
}
