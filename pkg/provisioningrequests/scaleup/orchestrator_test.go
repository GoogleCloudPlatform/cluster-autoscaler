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

package scaleup

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"
	"k8s.io/autoscaler/cluster-autoscaler/resourcequotas"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/orchestrator"
	. "k8s.io/autoscaler/cluster-autoscaler/core/test"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/processors/callbacks"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupconfig"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodeinfosprovider"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	. "k8s.io/autoscaler/cluster-autoscaler/processors/test"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podsharding"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	pr_pods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	provreq_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	"k8s.io/utils/ptr"
)

const (
	defaultZone   = "us-central1-f"
	testNamespace = "test-namespace"
	testName      = "test-name"
)

type injectedMigConfig struct {
	migConfig
	extraCreatedMigs []migConfig
}

type migConfig struct {
	nodePoolName    string
	migName         string
	zone            string
	queued          bool
	maxSize         int
	useTotalMaxSize bool
	paginated       bool
	nodes           []NodeConfig
	spec            *gkeclient.NodePoolSpec
	templateNode    *NodeConfig
}

type scaleUpInfo struct {
	ID          string
	CurrentSize int
	NewSize     int
	MaxSize     int
}

func TestOrchestrator_ScaleUp(t *testing.T) {
	t.Parallel()

	exampleCurrentTime := time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC)
	nonRetriableProvReqCreationTime := exampleCurrentTime.Add(-pendingProvisioningRequestRetryPeriod - time.Minute)
	retriableProvReqCreationTime := exampleCurrentTime.Add(-10 * time.Second)
	supportedZones := []string{"us-central1-a", "us-central1-b", "us-central1-c", "us-central1-f"}

	defaultMachineTypes := []string{"n1-standard-1"}
	defaultOptions := config.AutoscalingOptions{
		BalanceSimilarNodeGroups: true,
		EstimatorName:            estimator.BinpackingEstimatorName,
		MaxCoresTotal:            config.DefaultMaxClusterCores,
		MaxMemoryTotal:           config.DefaultMaxClusterMemory * units.GiB,
		MinCoresTotal:            0,
		MinMemoryTotal:           0,
	}

	tests := []struct {
		name                    string
		migs                    []migConfig
		napInjectedMigs         []injectedMigConfig
		pods                    []PodConfig
		podOptions              []func(*apiv1.Pod)
		ossScaleUp              bool
		options                 config.AutoscalingOptions
		bestOption              GroupSizeChange
		prs                     []*provreqwrapper.ProvisioningRequest
		want                    ScaleUpStatusInfo
		disabledExperimentFlags []string
		disableRLAForTPU        bool
		shardOptions            []shardOption
		wantPodShards           int
		wantScaleUpInfos        []scaleUpInfo
		wantIncreaseSizeCalls   []GroupSizeChange
		prIdxOverrides          []int
		wantOptions             *[]GroupSizeChange
		wantPRToFail            bool
		wantPRFailReason        string
		wantPRFailMessage       string
		wantPRAcceptedReason    string
		wantPRAcceptedMessage   string
		wantPRCommittedMigs     []string
		wantPRCommittedZones    []string
	}{
		{
			name: "simple test",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{cpuNode(1)})),
				testMig(2, withNodes([]NodeConfig{cpuNode(2)})),
				testMig(3, withNodes([]NodeConfig{cpuNode(3)}), withNonQueued()),
				testMig(4, withNodes([]NodeConfig{cpuNode(4)}), withMaxSize(1)),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(2), SizeChange: 1},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(2), SizeChange: 1},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(2), 1, 2, 10},
			},
		},
		{
			name: "simple gpu test",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{gpuNode(1)})),
				testMig(2, withNodes([]NodeConfig{cpuNode(2)})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1), SizeChange: 3},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 3},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "1", 3, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1), 1, 4, 10},
			},
		},
		{
			name: "gpu test where the pods have a taint toleration to queued taint, oss scale-up is run no non-queued-nodepool available",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{gpuNode(1)})),
				testMig(2, withNodes([]NodeConfig{gpuNode(2)})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 1, ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 1, ToleratesGpu: true},
			},
			podOptions:   []func(*apiv1.Pod){pods.PopulatePodToleration},
			ossScaleUp:   true,
			shardOptions: []shardOption{withOssScaleUp},
			options:      defaultOptions,
			prs:          []*provreqwrapper.ProvisioningRequest{},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{podName(1), podName(2)},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{},
		},
		{
			name: "gpu test where the pods have a taint toleration to queued taint, oss scale-up is run and non-queued-nodepool can accommodate the scale-up",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{gpuNode(1)})),
				testMig(2, withNodes([]NodeConfig{gpuNode(2)}), withNonQueued()),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 1, ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 1, ToleratesGpu: true},
			},
			podOptions:   []func(*apiv1.Pod){pods.PopulatePodToleration},
			ossScaleUp:   true,
			shardOptions: []shardOption{withOssScaleUp},
			options:      defaultOptions,
			bestOption:   GroupSizeChange{GroupName: migUrl(2), SizeChange: 2},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(2), SizeChange: 2},
			},
			prs: []*provreqwrapper.ProvisioningRequest{},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{podName(1), podName(2)},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(2), 1, 3, 10},
			},
		},
		{
			name: "max size breached after scale up",
			migs: []migConfig{
				testMig(2, withBasicCpuNodes(4)),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "1000m", "10M", "", 8, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "NodepoolSizeReached",
			wantPRFailMessage: "Max nodepool size reached, affected nodepools: np2",
		},
		{
			name: "Provisioning Request requires nodes to match the max size",
			migs: []migConfig{
				testMig(2, withNodes([]NodeConfig{
					cpuNode(1, withCpu(1000))}), withMaxSize(1000))},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
			},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(2), SizeChange: 999},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(2), SizeChange: 999},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "1000m", "10M", "", 999, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(2), 1, 1000, 1000},
			},
		},
		{
			name: "Provisioning Request requires nodes to breach the max size by one",
			migs: []migConfig{
				testMig(2, withNodes([]NodeConfig{
					cpuNode(1, withCpu(1000))}), withMaxSize(1000))},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "1000m", "10M", "", 1000, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{},
			wantPRToFail:     true,
		},
		{
			name: "Provisioning Request has anti-affinity to itself, one pod can be scheduled",
			migs: []migConfig{
				testMig(2, withBasicCpuNodes(4)),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
			},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(2), SizeChange: 1},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(2), SizeChange: 1},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "1000m", "10M", "", 1, true, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(2), 4, 5, 10},
			},
		},
		{
			name: "Provisioning Request has anti-affinity to itself, two pod cannot be scheduled",
			migs: []migConfig{
				testMig(2, withBasicCpuNodes(4)),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "1000m", "10M", "", 2, true, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{},
			wantPRToFail:     true,
		},
		{
			name: "gpu test, no migs provide gpus",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					cpuNode(1, withCpu(1000))})),
				testMig(2, withNodes([]NodeConfig{
					cpuNode(2, withCpu(10*1000))})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "1", 3, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: podNamesForProvReq(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "1", 3, false, nonRetriableProvReqCreationTime)),
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantPRFailMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: np1 (Insufficient nvidia.com/gpu), np2 (Insufficient nvidia.com/gpu)",
		},
		{
			name: "all migs have gpus, provreq does not tolerate them",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					gpuNode(1, withCpu(1000))})),
				testMig(2, withNodes([]NodeConfig{
					gpuNode(2, withCpu(10*1000))})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: true},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: podNamesForProvReq(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime)),
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantPRFailMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: np1 (node(s) had untolerated taint(s)), np2 (node(s) had untolerated taint(s))",
		},
		{
			name: "current nodes breach the cpu limit",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					cpuNode(1, withCpu(1000))})),
				testMig(2, withNodes([]NodeConfig{
					cpuNode(2, withCpu(10*1000))})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options: config.AutoscalingOptions{
				EstimatorName:  estimator.BinpackingEstimatorName,
				MaxCoresTotal:  10,
				MaxMemoryTotal: config.DefaultMaxClusterMemory * units.GiB,
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 6, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: podNamesForProvReq(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 6, false, nonRetriableProvReqCreationTime)),
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "OutOfResources",
			wantPRFailMessage: "Max cluster limit reached, nodepools out of resources: np1 (cpu), np2 (cpu)",
		},
		{
			name: "new nodes added breach the cpu limit",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					cpuNode(1, withCpu(1000))})),
				testMig(2, withNodes([]NodeConfig{
					cpuNode(2, withCpu(10*1000))})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options: config.AutoscalingOptions{
				EstimatorName:  estimator.BinpackingEstimatorName,
				MaxCoresTotal:  13,
				MaxMemoryTotal: config.DefaultMaxClusterMemory * units.GiB,
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "OutOfResources",
			wantPRFailMessage: "Max cluster limit reached, nodepools out of resources: np1 (cpu), np2 (cpu)",
		},
		{
			name: "newPRsBreachGpuLimit_groupsNotSimilarBecauseNg2CannotScheduleAllPartialOptions",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					gpuNode(1, withCpu(10*1000), withGpu(4))}), withMaxSize(9)),
				testMig(2, withNodes([]NodeConfig{
					gpuNode(2, withCpu(10*1000), withGpu(4))}), withMaxSize(3)),
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, "pr-1", "700m", "10M", "4", 2, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)),
				buildProvisioningRequest(testNamespace, "pr-2", "700m", "10M", "4", 2, false, nonRetriableProvReqCreationTime.Add(-10*time.Second)),
				buildProvisioningRequest(testNamespace, "pr-3", "700m", "10M", "4", 2, false, nonRetriableProvReqCreationTime.Add(-5*time.Second)),
			},
			bestOption: GroupSizeChange{GroupName: migUrl(1), SizeChange: 6},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 2},
				{GroupName: migUrl(1), SizeChange: 2},
				{GroupName: migUrl(1), SizeChange: 2},
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1), 1, 3, 9},
				{migUrl(1), 3, 5, 9},
				{migUrl(1), 5, 7, 9},
			},
		},
		{
			name: "no QueuedProvisioning MIGs - don't fail recently created ProvReq",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					gpuNode(1, withCpu(1000))}), withNonQueued()),
				testMig(2, withNodes([]NodeConfig{
					gpuNode(2, withCpu(10*1000))}), withNonQueued()),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: true},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, retriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: podNamesForProvReq(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, retriableProvReqCreationTime)),
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:      []scaleUpInfo{},
			wantPRToFail:          false,
			wantPRAcceptedReason:  "NoQueuedNodepoolAvailable",
			wantPRAcceptedMessage: "No nodepool with QueuedProvisioning enabled is available for scale up",
		},
		{
			name: "current nodes breach the cpu limit - don't fail recently created ProvReq",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					cpuNode(1, withCpu(1000))})),
				testMig(2, withNodes([]NodeConfig{
					cpuNode(2, withCpu(10*1000))})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options: config.AutoscalingOptions{
				EstimatorName:  estimator.BinpackingEstimatorName,
				MaxCoresTotal:  10,
				MaxMemoryTotal: config.DefaultMaxClusterMemory * units.GiB,
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 6, false, retriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: podNamesForProvReq(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 6, false, retriableProvReqCreationTime)),
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:      []scaleUpInfo{},
			wantPRToFail:          false,
			wantPRAcceptedReason:  "OutOfResources",
			wantPRAcceptedMessage: "Max cluster limit reached, nodepools out of resources: np1 (cpu), np2 (cpu)",
		},
		{
			name: "all migs have gpus, provreq does not tolerate them - don't fail recently created ProvReq",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					gpuNode(1, withCpu(1000))})),
				testMig(2, withNodes([]NodeConfig{
					gpuNode(2, withCpu(10*1000))})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: true},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, retriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: podNamesForProvReq(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, retriableProvReqCreationTime)),
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:      []scaleUpInfo{},
			wantPRToFail:          false,
			wantPRAcceptedReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantPRAcceptedMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: np1 (node(s) had untolerated taint(s)), np2 (node(s) had untolerated taint(s))",
		},
		{
			name: "best location recommended",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					cpuNode(1, withCpu(10*1000))}), withZone("us-central1-c"), withNodePool(npName(1))),
				testMig(2, withNodes([]NodeConfig{
					cpuNode(2, withCpu(10*1000))}), withZone("us-central1-b"), withNodePool(npName(1))),
				testMig(3, withNodes([]NodeConfig{
					cpuNode(3, withCpu(10*1000))}), withZone("us-central1-a"), withNodePool(npName(1))),
			},
			pods:       []PodConfig{{Name: podName(1), Cpu: 1}, {Name: podName(2), Cpu: 8}},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1, "us-central1-c"), SizeChange: 1},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(3, "us-central1-a"), SizeChange: 1},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(3, "us-central1-a"), 1, 2, 10},
			},
		},
		{
			name: "TPU RLA disabled by experiment flag",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					cpuNode(1, withCpu(10*1000))}), withZone("us-central1-c"), withNodePool(npName(1)), withTpu()),
				testMig(2, withNodes([]NodeConfig{
					cpuNode(2, withCpu(10*1000))}), withZone("us-central1-b"), withNodePool(npName(1)), withTpu()),
				testMig(3, withNodes([]NodeConfig{
					cpuNode(3, withCpu(10*1000))}), withZone("us-central1-a"), withNodePool(npName(1)), withTpu()),
			},
			disableRLAForTPU: true,
			pods:             []PodConfig{{Name: podName(1), Cpu: 1}, {Name: podName(2), Cpu: 8}},
			options:          defaultOptions,
			bestOption:       GroupSizeChange{GroupName: migUrl(1, "us-central1-c"), SizeChange: 1},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1, "us-central1-c"), SizeChange: 1},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1, "us-central1-c"), 1, 2, 10},
			},
		},
		{
			name: "locations without MIGs not recommended",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					cpuNode(1, withCpu(10*1000))}), withZone("us-central1-c")),
			},
			pods:       []PodConfig{{Name: podName(1), Cpu: 1}, {Name: podName(2), Cpu: 8}},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1, "us-central1-c"), SizeChange: 1},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1, "us-central1-c"), SizeChange: 1},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1, "us-central1-c"), 1, 2, 10},
			},
		},
		{
			name: "slightly different MIG not recommended",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					cpuNode(1, withCpu(10*1000))}), withZone("us-central1-c")),
				testMig(2, withNodes([]NodeConfig{
					cpuNode(2, withCpu(11*1000))}), withZone("us-central1-b")),
			},
			pods:       []PodConfig{{Name: podName(1), Cpu: 1}, {Name: podName(2), Cpu: 8}},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1, "us-central1-c"), SizeChange: 1},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1, "us-central1-c"), SizeChange: 1},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1, "us-central1-c"), 1, 2, 10},
			},
		},
		{
			name: "defaults to no recommendation if RecLoc call fails (unsupported zones)",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					cpuNode(1, withCpu(10*1000))}), withZone("us-central2-c")),
				testMig(2, withNodes([]NodeConfig{
					cpuNode(2, withCpu(10*1000))}), withZone("us-central2-b")),
			},
			pods:       []PodConfig{{Name: podName(1), Cpu: 1}, {Name: podName(2), Cpu: 8}},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1, "us-central2-c"), SizeChange: 1},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1, "us-central2-c"), SizeChange: 1},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1, "us-central2-c"), 1, 2, 10},
			},
		},
		{
			name: "many ProvReqs, simple test",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					cpuNode(1, withCpu(1000))}), withMaxSize(100)),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 2, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
			},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1), SizeChange: 7},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 1},
				{GroupName: migUrl(1), SizeChange: 2},
				{GroupName: migUrl(1), SizeChange: 4},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, "test-name1", "700m", "10M", "", 1, false, nonRetriableProvReqCreationTime.Add(-2*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name2", "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name3", "700m", "10M", "", 4, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1), 1, 2, 100},
				{migUrl(1), 2, 4, 100},
				{migUrl(1), 4, 8, 100},
			},
		},
		{
			name: "many ProvReqs, 5 oldest ones are processed",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					cpuNode(1, withCpu(1000))}), withMaxSize(100)),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 2, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
			},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1), SizeChange: 31},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 1},
				{GroupName: migUrl(1), SizeChange: 2},
				{GroupName: migUrl(1), SizeChange: 4},
				{GroupName: migUrl(1), SizeChange: 8},
				{GroupName: migUrl(1), SizeChange: 16},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, "test-name1", "700m", "10M", "", 1, false, nonRetriableProvReqCreationTime.Add(-5*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name2", "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-4*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name3", "700m", "10M", "", 4, false, nonRetriableProvReqCreationTime.Add(-3*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name4", "700m", "10M", "", 8, false, nonRetriableProvReqCreationTime.Add(-2*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name5", "700m", "10M", "", 16, false, nonRetriableProvReqCreationTime.Add(-time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name6", "700m", "10M", "", 32, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1), 1, 2, 100},
				{migUrl(1), 2, 4, 100},
				{migUrl(1), 4, 8, 100},
				{migUrl(1), 8, 16, 100},
				{migUrl(1), 16, 32, 100},
			},
		},
		{
			name: "many ProvReqs, no capacity for the newest",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					cpuNode(1, withCpu(1000))}), withMaxSize(15)),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 2, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
			},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1), SizeChange: 7},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 1},
				{GroupName: migUrl(1), SizeChange: 2},
				{GroupName: migUrl(1), SizeChange: 4},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, "test-name1", "700m", "10M", "", 1, false, nonRetriableProvReqCreationTime.Add(-3*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name2", "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-2*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name3", "700m", "10M", "", 4, false, nonRetriableProvReqCreationTime.Add(-time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name4", "700m", "10M", "", 8, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1), 1, 2, 15},
				{migUrl(1), 2, 4, 15},
				{migUrl(1), 4, 8, 15},
			},
		},
		{
			name: "many ProvReqs, no capacity for the oldest",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					cpuNode(1, withCpu(1000))}), withMaxSize(4)),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 2, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, "test-name1", "700m", "10M", "", 4, false, nonRetriableProvReqCreationTime.Add(-2*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name2", "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name3", "700m", "10M", "", 1, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "NodepoolSizeReached",
			wantPRFailMessage: "Max nodepool size reached, affected nodepools: np1",
		},
		{
			name: "many ProvReqs, no capacity for the oldest within 2 minutes",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{cpuNode(1, withCpu(1000))}), withMaxSize(4)),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 2, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, "test-name1", "700m", "10M", "", 4, false, retriableProvReqCreationTime.Add(-20*time.Second)),
				buildProvisioningRequest(testNamespace, "test-name2", "700m", "10M", "", 2, false, retriableProvReqCreationTime.Add(-10*time.Second)),
				buildProvisioningRequest(testNamespace, "test-name3", "700m", "10M", "", 1, false, retriableProvReqCreationTime),
			},
			bestOption: GroupSizeChange{GroupName: migUrl(1), SizeChange: 3},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 0},
				{GroupName: migUrl(1), SizeChange: 2},
				{GroupName: migUrl(1), SizeChange: 1},
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1), 1, 3, 4},
				{migUrl(1), 3, 4, 4},
			},
			wantPRToFail: false,
		},
		{
			name: "many ProvReqs, best location recommended without all PRs handled",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					cpuNode(1, withCpu(1000))}), withMaxSize(20), withZone("us-central1-c"), withNodePool(npName(1))),
				testMig(2, withNodes([]NodeConfig{
					cpuNode(2, withCpu(1000))}), withZone("us-central1-b"), withNodePool(npName(1))),
				testMig(3, withNodes([]NodeConfig{
					cpuNode(3, withCpu(1000))}), withZone("us-central1-a"), withNodePool(npName(1))),
			},
			pods:       []PodConfig{{Name: podName(1), Cpu: 1}, {Name: podName(2), Cpu: 1}},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1, "us-central1-c"), SizeChange: 15},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(3, "us-central1-a"), SizeChange: 8},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, "test-name1", "700m", "10M", "", 8, false, nonRetriableProvReqCreationTime.Add(-3*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name2", "700m", "10M", "", 4, false, nonRetriableProvReqCreationTime.Add(-2*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name3", "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name4", "700m", "10M", "", 1, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(3, "us-central1-a"), 1, 9, 10},
			},
		},
		{
			name: "many ProvReqs, best location lacking necessary capacity",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{
					cpuNode(1, withCpu(1000))}), withMaxSize(20), withZone("us-central1-c")),
				testMig(2, withNodes([]NodeConfig{
					cpuNode(2, withCpu(1000))}), withMaxSize(5), withZone("us-central1-b")),
				testMig(3, withNodes([]NodeConfig{
					cpuNode(3, withCpu(1000))}), withMaxSize(5), withZone("us-central1-a")),
			},
			pods:       []PodConfig{{Name: podName(1), Cpu: 1}, {Name: podName(2), Cpu: 1}},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1, "us-central1-c"), SizeChange: 15},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1, "us-central1-c"), SizeChange: 8},
				{GroupName: migUrl(1, "us-central1-c"), SizeChange: 4},
				{GroupName: migUrl(1, "us-central1-c"), SizeChange: 2},
				{GroupName: migUrl(1, "us-central1-c"), SizeChange: 1},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, "test-name1", "700m", "10M", "", 8, false, nonRetriableProvReqCreationTime.Add(-3*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name2", "700m", "10M", "", 4, false, nonRetriableProvReqCreationTime.Add(-2*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name3", "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name4", "700m", "10M", "", 1, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1, "us-central1-c"), 1, 9, 20},
				{migUrl(1, "us-central1-c"), 9, 13, 20},
				{migUrl(1, "us-central1-c"), 13, 15, 20},
				{migUrl(1, "us-central1-c"), 15, 16, 20},
			},
		},
		// Test cases based on go/gke-dws-batching-qa
		{
			name: "many ProvReqs, best location recommended with total_max_size setting",
			migs: []migConfig{
				testMig(1,
					withNodes(genNodes(1, 2, cpuNode, withCpu(1000))),
					withMaxSize(26), withZone("us-central1-c"), withNodePool(npName(1)), withTotalMaxSize()),
				testMig(2,
					withNodes(genNodes(2, 4, cpuNode, withCpu(1000))),
					withMaxSize(26), withZone("us-central1-b"), withNodePool(npName(1)), withTotalMaxSize()),
				testMig(3,
					withNodes(genNodes(3, 8, cpuNode, withCpu(1000))),
					withMaxSize(26), withZone("us-central1-a"), withNodePool(npName(1)), withTotalMaxSize()),
			},
			pods:       []PodConfig{{Name: podName(1), Cpu: 1}, {Name: podName(2), Cpu: 1}},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1, "us-central1-c"), SizeChange: 10},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(3, "us-central1-a"), SizeChange: 2},
				{GroupName: migUrl(3, "us-central1-a"), SizeChange: 2},
				{GroupName: migUrl(3, "us-central1-a"), SizeChange: 2},
				{GroupName: migUrl(3, "us-central1-a"), SizeChange: 2},
				{GroupName: migUrl(3, "us-central1-a"), SizeChange: 2},
				{GroupName: migUrl(3, "us-central1-a"), SizeChange: 2},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, "test-name1", "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-6*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name2", "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-5*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name3", "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-4*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name4", "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-3*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name5", "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-2*time.Minute)),
				buildProvisioningRequest(testNamespace, "test-name6", "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-time.Minute)),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(3, "us-central1-a"), 8, 10, 20},
				{migUrl(3, "us-central1-a"), 10, 12, 20},
				{migUrl(3, "us-central1-a"), 12, 14, 20},
				{migUrl(3, "us-central1-a"), 14, 16, 20},
				{migUrl(3, "us-central1-a"), 16, 18, 20},
			},
		},
		{
			name: "ProvReq requesting more than allowed by the pageless mig limit - existing mig is pageless",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{gpuNode(1, withCpu(10000))}), withMaxSize(gceclient.PagelessMigInstanceLimit+500)),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 2, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, "test-name1", "100m", "10M", "1", gceclient.PagelessMigInstanceLimit+1, false, nonRetriableProvReqCreationTime.Add(-time.Minute)),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{},
			wantPRToFail:     true,
		},
		{
			name: "ProvReq requesting more than allowed by the pageless mig limit - existing mig is paginated",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{gpuNode(1, withCpu(10000))}), withMaxSize(gceclient.PagelessMigInstanceLimit+500), withPaginated()),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 2, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: false},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, "test-name1", "100m", "10M", "1", gceclient.PagelessMigInstanceLimit+1, false, retriableProvReqCreationTime),
			},
			bestOption: GroupSizeChange{GroupName: migUrl(1), SizeChange: gceclient.PagelessMigInstanceLimit + 1},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: gceclient.PagelessMigInstanceLimit + 1},
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1), 1, 1002, 1500},
			},
			wantPRToFail: false,
		},
		{
			name: "ProvReq with NAP enabled. NAP-injected node group is the best option",
			migs: []migConfig{
				testMig(1, withNodes(genNodes(1, 2, cpuNode, withCpu(1000))), withMaxSize(4), withNonQueued()),
			},
			napInjectedMigs: []injectedMigConfig{
				{migConfig: testMig(2, withNodes([]NodeConfig{cpuNode(2, withCpu(1000))}), withMaxSize(12))},
			},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(2), SizeChange: 1},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(2), SizeChange: 1},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, "prov-req-1", "700m", "10M", "", 1, false, retriableProvReqCreationTime.Add(-20*time.Second)),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(2), 0, 1, 12},
			},
		},
		{
			name: "Provision request can't be scheduled due to insufficient memory",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{cpuNode(1, withCpu(10*1000), withMemory(500))}))},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "0", 3, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: podNamesForProvReq(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "0", 3, false, nonRetriableProvReqCreationTime)),
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantPRFailMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: np1 (Insufficient memory)",
		},
		{
			name: "Provision request can't be scheduled due to insufficient gpu",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{cpuNode(2, withCpu(10*1000))})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "1", 3, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: podNamesForProvReq(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "1", 3, false, nonRetriableProvReqCreationTime)),
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantPRFailMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: np1 (Insufficient nvidia.com/gpu)",
		},
		{
			name: "Provision request can't be scheduled due to missing toleration",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{gpuNode(2, withCpu(10*1000))})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-time.Minute)),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: podNamesForProvReq(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-time.Minute))),
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantPRFailMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: np1 (node(s) had untolerated taint(s))",
		},
		{
			name: "Provision request can't be scheduled due to insufficient memory and gpu",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{cpuNode(1, withCpu(1000), withMemory(100*1000*1000))})),
				testMig(2, withNodes([]NodeConfig{gpuNode(2, withCpu(10*1000), withMemory(500))})),
				testMig(3, withNodes([]NodeConfig{cpuNode(3, withCpu(10*1000), withMemory(500))})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "1", 3, false, nonRetriableProvReqCreationTime),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: podNamesForProvReq(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "1", 3, false, nonRetriableProvReqCreationTime)),
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantPRFailMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: np1 (Insufficient nvidia.com/gpu), np2 (Insufficient memory), np3 (Insufficient memory, Insufficient nvidia.com/gpu)",
		},
		{
			name: "Provision request can't be scheduled due to insufficient memory and missing toleration",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{gpuNode(1, withCpu(10*1000), withMemory(100*1000*1000))})),
				testMig(2, withNodes([]NodeConfig{gpuNode(2, withCpu(10*1000), withMemory(500))})),
				testMig(3, withNodes([]NodeConfig{cpuNode(3, withCpu(10*1000), withMemory(500))})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-time.Minute)),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: podNamesForProvReq(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-time.Minute))),
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantPRFailMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: np1 (node(s) had untolerated taint(s)), np2 (node(s) had untolerated taint(s)), np3 (Insufficient memory)",
		},
		{
			name: "napEventFlagEnabled_ProvReqPodsUnschedulable",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{gpuNode(1, withCpu(10*1000), withMemory(100*1000*1000))})),
				testMig(2, withNodes([]NodeConfig{gpuNode(2, withCpu(10*1000), withMemory(500))})),
				testMig(3, withNodes([]NodeConfig{cpuNode(3, withCpu(10*1000), withMemory(500))})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-time.Minute)),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: podNamesForProvReq(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 2, false, nonRetriableProvReqCreationTime.Add(-time.Minute))),
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantPRFailMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: np1 (node(s) had untolerated taint(s)), np2 (node(s) had untolerated taint(s)), np3 (Insufficient memory)",
		},
		{
			name: "MaxRunDurationChecks_bulkMigExperimentNotActive_doesNotRunMRDChecksAndSchedulesAll",
			migs: []migConfig{
				testMig(1, withDefaultCpuMemNode(1), withMaxSize(100), withBulkA4XSpec("100")),
			},
			disabledExperimentFlags: []string{experiments.ProvisioningRequestBulkMigsFlag, experiments.ProvisioningRequestsZeroOrMaxRoundUpEnabledFlag},
			pods:                    []PodConfig{},
			options:                 defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, "mrd-pr-1", "100m", "10M", "", 1, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)),
				withMaxRunDuration(buildProvisioningRequest(testNamespace, "mrd-pr-2", "100m", "10M", "", 1, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)), "10000"),
				withMaxRunDuration(buildProvisioningRequest(testNamespace, "mrd-pr-3", "100m", "10M", "", 1, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)), "100"),
				withMaxRunDuration(buildProvisioningRequest(testNamespace, "mrd-pr-4", "100m", "10M", "", 1, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)), "invalidValue"),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			bestOption: GroupSizeChange{GroupName: migUrl(1), SizeChange: 4},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 1},
				{GroupName: migUrl(1), SizeChange: 1},
				{GroupName: migUrl(1), SizeChange: 1},
				{GroupName: migUrl(1), SizeChange: 1},
			},
			wantOptions: &[]GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 4},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1), 1, 2, 100},
				{migUrl(1), 2, 3, 100},
				{migUrl(1), 3, 4, 100},
				{migUrl(1), 4, 5, 100},
			},
		},
		{
			name:                    "MaxRunDurationChecks_provReqDoesNotHaveMRD_usesDefaultMRDValue",
			disabledExperimentFlags: []string{experiments.ProvisioningRequestsZeroOrMaxRoundUpEnabledFlag},
			migs: []migConfig{
				testMig(1, withDefaultCpuMemNode(1), withMaxSize(100), withBulkA4XSpec("100")),
			},
			pods:    []PodConfig{},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, "mrd-pr-1", "700m", "10M", "", 1, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantPRToFail:      true,
			wantPRFailReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantPRFailMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: np1 (unschedulable due to MaxRunDuration mismatch)",
		},
		{
			name:                    "MaxRunDurationChecks_provReqDoesNotHaveMRD_usesDefaultMRDValueAndSchedules",
			disabledExperimentFlags: []string{experiments.ProvisioningRequestsZeroOrMaxRoundUpEnabledFlag},
			migs: []migConfig{
				testMig(1, withDefaultCpuMemNode(1), withMaxSize(100),
					withBulkA4XSpec(strconv.FormatInt(int64(queuedwrapper.DefaultMaxRunDuration.Seconds()), 10))),
			},
			pods:    []PodConfig{},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, "mrd-pr-1", "700m", "10M", "", 1, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			bestOption: GroupSizeChange{GroupName: migUrl(1), SizeChange: 1},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 1},
			},
			wantOptions: &[]GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 1},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1), 1, 2, 100},
			},
		},
		{
			name:                    "MaxRunDurationChecks_migDoesNotHaveMRD_doesNotSchedule",
			disabledExperimentFlags: []string{experiments.ProvisioningRequestsZeroOrMaxRoundUpEnabledFlag},
			migs: []migConfig{
				testMig(1, withDefaultCpuMemNode(1), withMaxSize(100), withBulkA4XSpec("")),
			},
			pods:    []PodConfig{},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				withMaxRunDuration(buildProvisioningRequest(testNamespace, "mrd-pr-1", "700m", "10M", "", 1, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)), "100"),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantPRToFail:      true,
			wantPRFailReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantPRFailMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: np1 (unschedulable due to MaxRunDuration mismatch)",
		},
		{
			name:                    "MaxRunDurationChecks_invalidProvReqMRDFormat_doesNotSchedule",
			disabledExperimentFlags: []string{experiments.ProvisioningRequestsZeroOrMaxRoundUpEnabledFlag},
			migs: []migConfig{
				testMig(1, withDefaultCpuMemNode(1), withMaxSize(100), withBulkA4XSpec("100")),
			},
			pods:    []PodConfig{},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				withMaxRunDuration(buildProvisioningRequest(testNamespace, "mrd-pr-1", "700m", "10M", "", 1, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)), "10seconds"),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantPRToFail:      true,
			wantPRFailReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantPRFailMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: np1 (unschedulable due to MaxRunDuration mismatch)",
		},
		{
			name:                    "MaxRunDurationChecks_invalidMigMRDFormat_doesNotSchedule",
			disabledExperimentFlags: []string{experiments.ProvisioningRequestsZeroOrMaxRoundUpEnabledFlag},
			migs: []migConfig{
				testMig(1, withDefaultCpuMemNode(1), withMaxSize(100), withBulkA4XSpec("100seconds")),
			},
			pods:    []PodConfig{},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				withMaxRunDuration(buildProvisioningRequest(testNamespace, "mrd-pr-1", "700m", "10M", "", 1, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)), "10"),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantPRToFail:      true,
			wantPRFailReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantPRFailMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: np1 (unschedulable due to MaxRunDuration mismatch)",
		},
		{
			name:                    "MaxRunDurationChecks_provReqAndMigHaveMismatchedMRDs_doesNotSchedule",
			disabledExperimentFlags: []string{experiments.ProvisioningRequestsZeroOrMaxRoundUpEnabledFlag},
			migs: []migConfig{
				testMig(1, withDefaultCpuMemNode(1), withMaxSize(100), withBulkA4XSpec("100")),
			},
			pods:    []PodConfig{},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				withMaxRunDuration(buildProvisioningRequest(testNamespace, "mrd-pr-1", "700m", "10M", "", 1, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)), "200"),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantPRFailMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: np1 (unschedulable due to MaxRunDuration mismatch)",
		},
		{
			name:                    "MaxRunDurationChecks_migsWithAndWithoutBulkMode_validatesMRDOnlyOnBulkProvisioningMachinesAndSchedules",
			disabledExperimentFlags: []string{experiments.ProvisioningRequestsZeroOrMaxRoundUpEnabledFlag},
			migs: []migConfig{
				testMig(1, withDefaultCpuMemNode(1), withMaxSize(100), withBulkA4XSpec("100")),
				testMig(2, withDefaultCpuMemNode(2), withMaxSize(100), withBulkA4XSpec("200")),
				testMig(3, withDefaultCpuMemNode(3), withMaxSize(100), withA4XSpec("10", false, placement.Spec{Policy: "a4x-policy"})),
				testMig(4, withDefaultCpuMemNode(4), withMaxSize(100), withA4XSpec("10", true, placement.Spec{})),
			},
			pods:    []PodConfig{},
			options: defaultOptions,
			prs: []*provreqwrapper.ProvisioningRequest{
				withMaxRunDuration(buildProvisioningRequest(testNamespace, "mrd-pr-1", "700m", "10M", "", 1, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)), "200"),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			bestOption: GroupSizeChange{GroupName: migUrl(4), SizeChange: 1},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(4), SizeChange: 1},
			},
			wantOptions: &[]GroupSizeChange{
				{GroupName: migUrl(2), SizeChange: 1},
				{GroupName: migUrl(3), SizeChange: 1},
				{GroupName: migUrl(4), SizeChange: 1},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(4), 1, 2, 100},
			},
		},
		{
			name:                    "bulkMig_roundUpExpDisabled_scaleUp",
			disabledExperimentFlags: []string{experiments.ProvisioningRequestsZeroOrMaxRoundUpEnabledFlag},
			migs: []migConfig{
				testMig(1, withTemplateNode(ptr.To(gpuNode(1, withCpu(10*1000), withMemory(1000*1000*1000)))), withMaxSize(10), withBulkA4XSpec("100")),
			},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1), SizeChange: 3},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 3},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				withMaxRunDuration(buildProvisioningRequest("default", "pr1", "700m", "10M", "1", 3, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)), "100"),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1), 0, 3, 10},
			},
		},
		{
			name: "nonBulkNonAtomicMig_noRoundUpToMaxSize",
			migs: []migConfig{
				testMig(1, withTemplateNode(ptr.To(gpuNode(1, withCpu(10*1000), withMemory(1000*1000*1000)))), withMaxSize(10))},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1), SizeChange: 3},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 3},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest("default", "pr1", "700m", "10M", "1", 3, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1), 0, 3, 10},
			},
		},
		{
			name: "bulkMig_roundUpToMaxSize",
			migs: []migConfig{
				testMig(1, withTemplateNode(ptr.To(gpuNode(1, withCpu(10*1000), withMemory(1000*1000*1000)))), withMaxSize(10), withBulkA4XSpec("100")),
			},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1), SizeChange: 10},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 10},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				withMaxRunDuration(buildProvisioningRequest("default", "pr1", "700m", "10M", "1", 3, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)), "100"),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1), 0, 10, 10},
			},
		},
		{
			name: "bulkMig_existingNodes_noScaleUp",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{(gpuNode(1, withCpu(10*1000), withMemory(1000*1000*1000)))}), withMaxSize(10), withBulkA4XSpec("100")),
			},
			options:               defaultOptions,
			bestOption:            GroupSizeChange{},
			wantIncreaseSizeCalls: []GroupSizeChange{},
			prs: []*provreqwrapper.ProvisioningRequest{
				withMaxRunDuration(buildProvisioningRequest("default", "pr1", "700m", "10M", "1", 3, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)), "100"),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "NodepoolSizeReached",
			wantPRFailMessage: "Max nodepool size reached, affected nodepools: np1",
		},
		{
			name: "bulkMig_requestMoreThanMaxSize_noScaleUp",
			migs: []migConfig{
				testMig(1, withTemplateNode(ptr.To(gpuNode(1, withCpu(10*1000), withMemory(1000*1000*1000)))), withMaxSize(10), withBulkA4XSpec("100")),
			},
			options:               defaultOptions,
			bestOption:            GroupSizeChange{},
			wantIncreaseSizeCalls: []GroupSizeChange{},
			prs: []*provreqwrapper.ProvisioningRequest{
				withMaxRunDuration(buildProvisioningRequest("default", "pr1", "700m", "10M", "1", 30, false, nonRetriableProvReqCreationTime.Add(-15*time.Second)), "100"),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "NodepoolSizeReached",
			wantPRFailMessage: "Max nodepool size reached, affected nodepools: np1",
		},
		{
			name: "multiplePodSets_SamePodSets_scaleUp",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{gpuNode(1)})),
				testMig(2, withNodes([]NodeConfig{cpuNode(2)})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1), SizeChange: 3},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 3},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "1", 3, false, nonRetriableProvReqCreationTime, []podSetOpts{
					{replicasPerPodSet: 1, containersOverride: []apiv1.Container{container("", "", nil, "700m", "10M", ptr.To(resource.MustParse("1")))}},
					{replicasPerPodSet: 1, containersOverride: []apiv1.Container{container("", "", nil, "700m", "10M", ptr.To(resource.MustParse("1")))}},
					{replicasPerPodSet: 1, containersOverride: []apiv1.Container{container("", "", nil, "700m", "10M", ptr.To(resource.MustParse("1")))}},
				}...),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1), 1, 4, 10},
			},
		},
		{
			name: "multiplePodSets_DifferentContainers_scaleUp",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{gpuNode(1)})),
				testMig(2, withNodes([]NodeConfig{cpuNode(2)})),
				testMig(3, withNodes([]NodeConfig{gpuNode(3)})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1), SizeChange: 2},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 2},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "1", 2, false, nonRetriableProvReqCreationTime, []podSetOpts{
					// Different containers/images, same resources, 1 GPU
					{replicasPerPodSet: 1, containersOverride: []apiv1.Container{container("", "", nil, "700m", "10M", ptr.To(resource.MustParse("1")))}},
					{replicasPerPodSet: 1, containersOverride: []apiv1.Container{container("test-container", "test-img", []string{"test", "cmd"}, "700m", "10M", ptr.To(resource.MustParse("1")))}},
				}...),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1), 1, 3, 10},
			},
		},
		{
			name: "multiplePodSets_DifferentGpuCountPodSets_scaleUp",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{gpuNode(1, withGpu(2))})),
				testMig(2, withNodes([]NodeConfig{gpuNode(2)})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1), SizeChange: 2},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1), SizeChange: 2},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "2", 2, false, nonRetriableProvReqCreationTime, []podSetOpts{
					{replicasPerPodSet: 1, containersOverride: []apiv1.Container{container("", "", nil, "700m", "10M", ptr.To(resource.MustParse("2")))}}, // 2 GPU request
					{replicasPerPodSet: 1, containersOverride: []apiv1.Container{container("", "", nil, "700m", "10M", ptr.To(resource.MustParse("1")))}}, // 1 GPU request
				}...),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1), 1, 3, 10},
			},
		},
		{
			name: "multiplePodSets_DifferentGpuCountPodSets_noOptions",
			migs: []migConfig{
				testMig(1, withNodes([]NodeConfig{gpuNode(1)})),
				testMig(2, withNodes([]NodeConfig{gpuNode(2)})),
			},
			pods: []PodConfig{
				{Name: podName(1), Cpu: 1, Memory: 0, Gpu: 0, Node: nodeName(1), ToleratesGpu: true},
				{Name: podName(2), Cpu: 8, Memory: 0, Gpu: 0, Node: nodeName(2), ToleratesGpu: false},
			},
			options:               defaultOptions,
			bestOption:            GroupSizeChange{},
			wantIncreaseSizeCalls: []GroupSizeChange{},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(testNamespace, testName, "700m", "10M", "2", 2, false, nonRetriableProvReqCreationTime, []podSetOpts{
					{replicasPerPodSet: 1, containersOverride: []apiv1.Container{container("", "", nil, "700m", "10M", ptr.To(resource.MustParse("2")))}}, // 2 GPU request
					{replicasPerPodSet: 1, containersOverride: []apiv1.Container{container("", "", nil, "700m", "10M", ptr.To(resource.MustParse("1")))}}, // 1 GPU request
				}...),
			},
			want: ScaleUpStatusInfo{
				Result:               status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp: []string{},
				// Only 2-GPU-PodSet's Pods remain unschedulable
				PodsRemainUnschedulable: podNamesForProvReq(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "2", 2, false, nonRetriableProvReqCreationTime, []podSetOpts{
					{replicasPerPodSet: 1, containersOverride: []apiv1.Container{container("", "", nil, "700m", "10M", ptr.To(resource.MustParse("2")))}},
				}...)),
				PodsAwaitEvaluation: []string{},
			},
			wantScaleUpInfos:  []scaleUpInfo{},
			wantPRToFail:      true,
			wantPRFailReason:  "ProvisioningRequestNotSchedulableInNodepool",
			wantPRFailMessage: "Provisioning Request's pods cannot be scheduled in the nodepool. Predicate checking errors: np1 (Insufficient nvidia.com/gpu), np2 (Insufficient nvidia.com/gpu)",
		},
		{
			name: "nap_with_zone_selector",
			napInjectedMigs: []injectedMigConfig{
				{
					migConfig: testMig(1, withNodes([]NodeConfig{cpuNode(1, withCpu(1000))}), withZone("us-central1-c"), withNodePool(npName(1))),
					extraCreatedMigs: []migConfig{
						testMig(2, withNodes([]NodeConfig{cpuNode(2, withCpu(1000))}), withZone("us-central1-b"), withNodePool(npName(1))),
						testMig(3, withNodes([]NodeConfig{cpuNode(3, withCpu(1000))}), withZone("us-central1-a"), withNodePool(npName(1))),
					},
				},
			},
			options:    defaultOptions,
			bestOption: GroupSizeChange{GroupName: migUrl(1, "us-central1-c"), SizeChange: 1},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1, "us-central1-c"), SizeChange: 1},
			},
			prs: []*provreqwrapper.ProvisioningRequest{
				buildProvisioningRequest(
					testNamespace,
					"test-name3",
					"700m",
					"10M",
					"",
					1,
					false,
					nonRetriableProvReqCreationTime,
					podSetOpts{replicasPerPodSet: 1, podTemplateOptions: []podTemplateOption{withZoneSelector(("us-central1-c"))}},
				),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(1, "us-central1-c"), 0, 1, 10},
			},
		},
		{
			name: "parallelQueueing_happyPath",
			migs: []migConfig{
				testMig(1, withZone("us-central1-c"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(1)})),
				testMig(2, withZone("us-central1-b"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(2)})),
				testMig(3, withZone("us-central1-a"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(3)})),
			},
			options:      defaultOptions,
			shardOptions: []shardOption{withObtainabilityStrategyShard},
			bestOption:   GroupSizeChange{GroupName: migUrl(1, "us-central1-c"), SizeChange: 2},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1, "us-central1-c"), SizeChange: 1},
				{GroupName: migUrl(2, "us-central1-b"), SizeChange: 1},
				{GroupName: migUrl(3, "us-central1-a"), SizeChange: 1},
				{GroupName: migUrl(1, "us-central1-c"), SizeChange: 1},
				{GroupName: migUrl(2, "us-central1-b"), SizeChange: 1},
				{GroupName: migUrl(3, "us-central1-a"), SizeChange: 1},
			},
			prIdxOverrides: []int{0, 0, 0, 1, 1, 1},
			prs: []*provreqwrapper.ProvisioningRequest{
				withObtainabilityStrategyPR(buildProvisioningRequest(testNamespace, "wiktorkpr1", "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime)),
				withObtainabilityStrategyPR(buildProvisioningRequest(testNamespace, "wiktorkpr2", "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime)),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(3, "us-central1-a"), 1, 2, 10},
				{migUrl(3, "us-central1-a"), 2, 3, 10},

				{migUrl(2, "us-central1-b"), 1, 2, 10},
				{migUrl(2, "us-central1-b"), 2, 3, 10},

				{migUrl(1, "us-central1-c"), 1, 2, 10},
				{migUrl(1, "us-central1-c"), 2, 3, 10},
			},
			wantPRCommittedZones: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			wantPRCommittedMigs:  []string{migName(1), migName(2), migName(3)},
		},
		{
			name: "parallelQueueing_parallelizationBreaksClusterNodeLimits",
			migs: []migConfig{
				testMig(1, withZone("us-central1-c"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(1)})),
				testMig(2, withZone("us-central1-b"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(2)})),
				testMig(3, withZone("us-central1-a"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(3)})),
			},
			options: config.AutoscalingOptions{
				BalanceSimilarNodeGroups: true,
				EstimatorName:            estimator.BinpackingEstimatorName,
				MaxCoresTotal:            config.DefaultMaxClusterCores,
				MaxMemoryTotal:           config.DefaultMaxClusterMemory * units.GiB,
				MaxNodesTotal:            4, // 3 for existing nodes, 1 for another node, non-parallel scaleup would fit
			},
			shardOptions: []shardOption{withObtainabilityStrategyShard},
			prs: []*provreqwrapper.ProvisioningRequest{
				withObtainabilityStrategyPR(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime)),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantPRToFail:      true,
			wantPRFailReason:  "CannotExecuteObtainabilityStrategy",
			wantPRFailMessage: "Could not execute OBTAINABILITY capacitySearchStrategy in nodepool np1. max cluster size reached",
		},
		{
			name: "parallelQueueing_parallelizationBreaksClusterResourceLimits",
			migs: []migConfig{
				testMig(1, withZone("us-central1-c"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(1)})),
				testMig(2, withZone("us-central1-b"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(2)})),
				testMig(3, withZone("us-central1-a"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(3)})),
			},
			options: config.AutoscalingOptions{
				BalanceSimilarNodeGroups: true,
				EstimatorName:            estimator.BinpackingEstimatorName,
				MaxCoresTotal:            40, // 30 for existing nodes, 10 for another node, non-parallel scaleup would fit
				MaxMemoryTotal:           config.DefaultMaxClusterMemory * units.GiB,
			},
			shardOptions: []shardOption{withObtainabilityStrategyShard},
			prs: []*provreqwrapper.ProvisioningRequest{
				withObtainabilityStrategyPR(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime)),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantPRToFail:      true,
			wantPRFailReason:  "CannotExecuteObtainabilityStrategy",
			wantPRFailMessage: "Could not execute OBTAINABILITY capacitySearchStrategy in nodepool np1. exceeded quota: \"cluster-wide\", resources: cpu",
		},
		{
			name: "parallelQueueing_parallelizationBreaksMaxSize",
			migs: []migConfig{
				testMig(1, withZone("us-central1-c"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(1), cpuNode(4)}), withMaxSize(2)),
				testMig(2, withZone("us-central1-b"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(2)}), withMaxSize(2)),
				testMig(3, withZone("us-central1-a"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(3)}), withMaxSize(2)),
			},
			options:      defaultOptions,
			shardOptions: []shardOption{withObtainabilityStrategyShard},
			prs: []*provreqwrapper.ProvisioningRequest{
				withObtainabilityStrategyPR(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime)),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantPRToFail:      true,
			wantPRFailReason:  "NodepoolSizeReached",
			wantPRFailMessage: "Max nodepool size reached, affected nodepools: np1",
		},
		{
			name: "parallelQueueing_parallelizationBreaksNodePoolTotalMaxSize",
			migs: []migConfig{
				testMig(1, withZone("us-central1-c"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(1)}), withMaxSize(4), withTotalMaxSize()),
				testMig(2, withZone("us-central1-b"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(2)}), withMaxSize(4), withTotalMaxSize()),
				testMig(3, withZone("us-central1-a"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(3)}), withMaxSize(4), withTotalMaxSize()),
			},
			options:      defaultOptions,
			shardOptions: []shardOption{withObtainabilityStrategyShard},
			prs: []*provreqwrapper.ProvisioningRequest{
				withObtainabilityStrategyPR(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime)),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpNoOptionsAvailable,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantPRToFail:      true,
			wantPRFailReason:  "CannotExecuteObtainabilityStrategy",
			wantPRFailMessage: "Could not execute OBTAINABILITY capacitySearchStrategy in nodepool np1. max node group size reached",
		},
		{
			name: "parallelQueueing_parallelizationPossibleOnlyInOneNP",
			migs: []migConfig{
				testMig(1, withZone("us-central1-c"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(1)}), withMaxSize(4), withTotalMaxSize()),
				testMig(2, withZone("us-central1-b"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(2)}), withMaxSize(4), withTotalMaxSize()),
				testMig(3, withZone("us-central1-a"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(3)}), withMaxSize(4), withTotalMaxSize()),
				testMig(4, withZone("us-central1-c"), withNodePool(npName(2)), withNodes([]NodeConfig{cpuNode(4)}), withMaxSize(2)),
				testMig(5, withZone("us-central1-b"), withNodePool(npName(2)), withNodes([]NodeConfig{cpuNode(5)}), withMaxSize(2)),
				testMig(6, withZone("us-central1-a"), withNodePool(npName(2)), withNodes([]NodeConfig{cpuNode(6)}), withMaxSize(2)),
			},
			bestOption:   GroupSizeChange{GroupName: migUrl(4, "us-central1-c"), SizeChange: 1},
			options:      defaultOptions,
			shardOptions: []shardOption{withObtainabilityStrategyShard},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(4, "us-central1-c"), SizeChange: 1},
				{GroupName: migUrl(5, "us-central1-b"), SizeChange: 1},
				{GroupName: migUrl(6, "us-central1-a"), SizeChange: 1},
			},
			prIdxOverrides: []int{0, 0, 0},
			prs: []*provreqwrapper.ProvisioningRequest{
				withObtainabilityStrategyPR(buildProvisioningRequest(testNamespace, testName, "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime)),
			},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(6, "us-central1-a"), 1, 2, 2},
				{migUrl(5, "us-central1-b"), 1, 2, 2},
				{migUrl(4, "us-central1-c"), 1, 2, 2},
			},
			wantPRCommittedZones: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			wantPRCommittedMigs:  []string{migName(4), migName(5), migName(6)},
		},
		{
			name: "parallelQueueing_parallelizationPossibleOnlyForOnePR",
			migs: []migConfig{
				testMig(1, withZone("us-central1-c"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(1)}), withMaxSize(2)),
				testMig(2, withZone("us-central1-b"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(2)}), withMaxSize(2)),
				testMig(3, withZone("us-central1-a"), withNodePool(npName(1)), withNodes([]NodeConfig{cpuNode(3)}), withMaxSize(2)),
			},
			options:      defaultOptions,
			shardOptions: []shardOption{withObtainabilityStrategyShard},
			prs: []*provreqwrapper.ProvisioningRequest{
				withObtainabilityStrategyPR(buildProvisioningRequest(testNamespace, "pr1", "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime)),
				// Give pr2 a slightly later creation time to ensure stable sorting in buildProvReqGroups.
				withObtainabilityStrategyPR(buildProvisioningRequest(testNamespace, "pr2", "700m", "10M", "", 3, false, nonRetriableProvReqCreationTime.Add(time.Second))),
			},
			bestOption: GroupSizeChange{GroupName: migUrl(1, "us-central1-c"), SizeChange: 1},
			wantIncreaseSizeCalls: []GroupSizeChange{
				{GroupName: migUrl(1, "us-central1-c"), SizeChange: 1},
				{GroupName: migUrl(2, "us-central1-b"), SizeChange: 1},
				{GroupName: migUrl(3, "us-central1-a"), SizeChange: 1},
			},
			prIdxOverrides: []int{0, 0, 0},
			want: ScaleUpStatusInfo{
				Result:                  status.ScaleUpSuccessful,
				PodsTriggeredScaleUp:    []string{},
				PodsRemainUnschedulable: []string{},
				PodsAwaitEvaluation:     []string{},
			},
			wantScaleUpInfos: []scaleUpInfo{
				{migUrl(3, "us-central1-a"), 1, 2, 2},
				{migUrl(2, "us-central1-b"), 1, 2, 2},
				{migUrl(1, "us-central1-c"), 1, 2, 2},
			},
			wantPRCommittedZones: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			wantPRCommittedMigs:  []string{migName(1), migName(2), migName(3)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			now := time.Now()
			ctx := context.Background()

			enabledExperimentFlags := sets.New(experiments.ProvisioningRequestsRLAEnabledFlag, experiments.ProvisioningRequestBulkMigsFlag, experiments.ProvisioningRequestsZeroOrMaxRoundUpEnabledFlag)
			for _, disabledFlag := range tt.disabledExperimentFlags {
				enabledExperimentFlags.Delete(disabledFlag)
			}
			if tt.disableRLAForTPU {
				enabledExperimentFlags.Insert(experiments.RecommendLocationsDisabledForTPUFlag)
			}
			experimentsManager := experiments.NewMockManager(enabledExperimentFlags.UnsortedList()...)

			// Prepare mocked cloud provider and nodes.
			nodes, gkeCloudProvider := prepareNodes(t, tt.migs, tt.napInjectedMigs, tt.wantIncreaseSizeCalls, tt.prs, tt.ossScaleUp, now, tt.prIdxOverrides)
			gkeCloudProvider.On("GetAvailableMachineTypes").Return(defaultMachineTypes, nil)
			resourceLimiter := cloudprovider.NewResourceLimiter(
				map[string]int64{cloudprovider.ResourceNameCores: tt.options.MinCoresTotal, cloudprovider.ResourceNameMemory: tt.options.MinMemoryTotal},
				map[string]int64{cloudprovider.ResourceNameCores: tt.options.MaxCoresTotal, cloudprovider.ResourceNameMemory: tt.options.MaxMemoryTotal},
			)
			gkeCloudProvider.On("GetResourceLimiter").Return(resourceLimiter, nil)

			// Prepare pods and pod lister.
			pods, extraPods := preparePods(t, tt.pods, tt.podOptions, tt.prs, now, gkeCloudProvider, experimentsManager)
			podLister := kube_util.NewTestPodLister(pods)
			listers := kube_util.NewListerRegistry(nil, nil, podLister, nil, nil, nil, nil, nil, nil)
			if tt.ossScaleUp {
				extraPods = pods
			}

			// Prepare processor and set pod shard.
			processors, registry := NewTestProcessors(tt.options)
			// Create context with non-random expander strategy.
			context, err := NewScaleTestAutoscalingContext(tt.options, &fake.Clientset{}, listers, gkeCloudProvider, callbacks.NewTestProcessorCallbacks(), nil, registry)
			assert.NoError(t, err)
			context.ExpanderStrategy = mockExpander{
				optionToChoose: tt.bestOption,
				wantOptions:    tt.wantOptions,
				t:              t,
			}
			err = context.ClusterSnapshot.SetClusterState(nodes, kube_util.ScheduledPods(pods), drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot())
			assert.NoError(t, err)

			if len(tt.napInjectedMigs) > 0 {
				processors.NodeGroupListProcessor = &MockAutoprovisioningNodeGroupListProcessor{T: t}
				em := experiments.NewMockManager()
				processors.NodeGroupManager = autoprovisioning.NewAutoprovisioningNodeGroupManager(autoprovisioning.AutoprovisioningNodeGroupManagerOptions{
					CloudProvider:      gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineTypes(defaultMachineTypes...).WithAutoprovisioningEnabled(true).Build(),
					OptionsTracker:     tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
					ExperimentsManager: em,
				})
			}
			processors.NodeGroupListProcessor = provreq_processors.NewFilterQueuedNodeGroupListProcessor(processors.NodeGroupListProcessor)
			podShard := buildPodShard(extraPods, tt.shardOptions)
			podsharding.TestSetPodShard(&context, podShard)
			nodeGroupConfigProcessor := nodegroupconfig.NewDefaultNodeGroupConfigProcessor(config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 15 * time.Minute})

			// Prepare node infos and templates.
			nodeInfoProvider := nodeinfosprovider.NewDefaultTemplateNodeInfoProvider(nil, false)
			templateNodeInfoRegistry := nodeinfosprovider.NewTemplateNodeInfoRegistry(nodeInfoProvider)
			nodeInfos, _ := nodeInfoProvider.Process(&context, nodes, []*appsv1.DaemonSet{}, taints.TaintConfig{}, now)
			clusterState := clusterstate.NewClusterStateRegistry(gkeCloudProvider, context.LogRecorder, NewBackoff(), nodeGroupConfigProcessor, templateNodeInfoRegistry, clusterstate.WithAsyncNodeGroupStateChecker(processors.AsyncNodeGroupStateChecker), clusterstate.WithScaleStateNotifier(processors.ScaleStateNotifier))
			err = clusterState.UpdateNodes(nodes, time.Now())
			assert.NoError(t, err)

			// Create oss orchestrator.
			orchestratorRaw := orchestrator.New()

			// Create ProvReq client and orchestrator.
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, tt.prs...)
			recommender := &alphabeticRecommender{
				zones: supportedZones,
			}
			prCache := provreqcache.NewQueuedProvisioningCache(fakeClient)
			o := NewOrchestrator(orchestratorRaw, recommender, fakeClient, prCache, 4*time.Minute, false, experimentsManager, nil)

			estimatorBuilder, err := estimator.NewEstimatorBuilder(
				estimator.BinpackingEstimatorName,
				estimator.NewThresholdBasedEstimationLimiter(nil),
				estimator.NewDecreasingPodOrderer(),
				nil, false)
			assert.NoError(t, err)

			quotasProvider := resourcequotas.NewCloudQuotasProvider(gkeCloudProvider)
			quotasTrackerFactory := resourcequotas.NewTrackerFactory(resourcequotas.TrackerOptions{
				CustomResourcesProcessor: processors.CustomResourcesProcessor,
				QuotaProvider:            quotasProvider,
			})
			gkeCloudProvider.On("GPULabel").Return("cloud.google.com/gke-accelerator")

			o.Initialize(&context, processors, clusterState, estimatorBuilder, taints.TaintConfig{}, quotasTrackerFactory)
			o.(*Orchestrator).now = func() time.Time { return exampleCurrentTime }

			// Run scale up check expected out-out.
			prCache.Refresh()
			scaleUpStatus, gotAErr := o.ScaleUp(extraPods, nodes, []*appsv1.DaemonSet{}, nodeInfos, true)
			got := simplifyScaleUpStatus(scaleUpStatus)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Orchestrator.ScaleUp() (-want +got):\n%s\nScaleUpStatus: %+v\ngotAErr: %+v", diff, scaleUpStatus, gotAErr)
				for k, v := range scaleUpStatus.ConsideredNodeGroups {
					t.Errorf("ConsideredNodeGroups[%v]: %+v", k, v)
				}
			}
			gotScaleUpInfos := simplifyScaleUpInfos(scaleUpStatus.ScaleUpInfos)
			if tt.wantScaleUpInfos == nil {
				tt.wantScaleUpInfos = []scaleUpInfo{}
			}
			if diff := cmp.Diff(tt.wantScaleUpInfos, gotScaleUpInfos); diff != "" {
				t.Errorf("Orchestrator.ScaleUp().ScaleUpInfos (-want +got):\n%s", diff)
			}
			assert.NoError(t, gotAErr)

			// Check if the state of ProvisioningRequests match.
			prState := func(t *testing.T, ns, name string) (provreqstate.ProvisioningRequestState, *provreqwrapper.ProvisioningRequest) {
				gotPR, err := fakeClient.ProvisioningRequest(ns, name)
				assert.NoError(t, err)
				return provreqstate.StateOfProvisioningRequest(gotPR), gotPR
			}
			if tt.wantPRToFail {
				gotPRState, pr := prState(t, tt.prs[0].Namespace, tt.prs[0].Name)
				if gotPRState != provreqstate.FailedState {
					t.Errorf("ProvisioningRequest.StateOfProvisioningRequest() got = %+v, wantPRToFail: %+v", gotPRState, tt.wantPRToFail)
				} else if tt.wantPRFailReason != "" {
					cond := meta.FindStatusCondition(pr.Status.Conditions, "Failed")
					assert.NotNil(t, cond)
					if cond.Reason != tt.wantPRFailReason {
						t.Errorf("ProvisioningRequest failed reason got = %v, wantPRFailReason: %v", cond.Reason, tt.wantPRFailReason)
					}
					if cond.Message != tt.wantPRFailMessage {
						t.Errorf("ProvisioningRequest failed message got = %v, wantPRFailMessage: %v", cond.Message, tt.wantPRFailMessage)
					}
				}
			} else if !tt.ossScaleUp {
				for i := range tt.wantIncreaseSizeCalls {
					prIdx := i
					if i < len(tt.prIdxOverrides) {
						prIdx = tt.prIdxOverrides[i]
					}
					gotPRState, gotPR := prState(t, tt.prs[prIdx].Namespace, tt.prs[prIdx].Name)
					if gotPRState == provreqstate.FailedState {
						t.Errorf("ProvisioningRequest.StateOfProvisioningRequest() got = %+v, wantPRToFail: %+v", gotPRState, tt.wantPRToFail)
					}
					expectStringSliceDetail(t, gotPR, "CommittedZones", tt.wantPRCommittedZones)
					expectStringSliceDetail(t, gotPR, "CommittedNodeGroups", tt.wantPRCommittedMigs)
				}
				if tt.wantPRAcceptedReason != "" {
					_, pr := prState(t, tt.prs[0].Namespace, tt.prs[0].Name)
					cond := meta.FindStatusCondition(pr.Status.Conditions, "Accepted")
					assert.NotNil(t, cond)
					if cond.Reason != tt.wantPRAcceptedReason {
						t.Errorf("ProvisioningRequest accepted reason got = %v, wantPRAcceptedReason: %v", cond.Reason, tt.wantPRAcceptedReason)
					}
					if cond.Message != tt.wantPRAcceptedMessage {
						t.Errorf("ProvisioningRequest accepted message got = %v, wantPRAcceptedMessage: %v", cond.Message, tt.wantPRAcceptedMessage)
					}
				}
			}
		})
	}
}

func TestOrchestrator_highestMaxRunDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		prDurations  map[string]*string // prName -> durationInSeconds
		wantDuration time.Duration
	}{
		{
			name: "MaxRunDuration is set to 3 and 5 days. Doesn't default to 7 days. Uses the highest value.",
			prDurations: map[string]*string{
				"prov-req-1": proto.String(strconv.FormatInt(3*24*3600, 10)),
				"prov-req-2": proto.String(strconv.FormatInt(5*24*3600, 10)),
			},
			wantDuration: 5 * 24 * time.Hour,
		},
		{
			name: "MaxRunDuration is unset in 1 ProvReq. Defaults to 7 days.",
			prDurations: map[string]*string{
				"prov-req-1": proto.String(strconv.FormatInt(3*24*3600, 10)),
				"prov-req-2": nil,
			},
			wantDuration: 7 * 24 * time.Hour,
		},
		{
			name: "MaxRunDuration is invalid in 1 ProvReq. Defaults to 7 days.",
			prDurations: map[string]*string{
				"prov-req-1": proto.String(strconv.FormatInt(3*24*3600, 10)),
				"prov-req-2": proto.String("invalid"),
			},
			wantDuration: 7 * 24 * time.Hour,
		},
		{
			name: "MaxRunDuration is unset in 1 ProvReq. Uses the highest value.",
			prDurations: map[string]*string{
				"prov-req-1": proto.String(strconv.FormatInt(12*24*3600, 10)),
				"prov-req-2": proto.String(strconv.FormatInt(10*24*3600, 10)),
				"prov-req-3": nil,
			},
			wantDuration: 12 * 24 * time.Hour,
		},
		{
			name: "MaxRunDuration is invalid in 1 ProvReq. Uses the highest value.",
			prDurations: map[string]*string{
				"prov-req-1": proto.String(strconv.FormatInt(12*24*3600, 10)),
				"prov-req-2": proto.String(strconv.FormatInt(10*24*3600, 10)),
				"prov-req-3": proto.String("invalid"),
			},
			wantDuration: 12 * 24 * time.Hour,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			var prs []*provreqwrapper.ProvisioningRequest
			var partialOptions []PartialOption
			for prName, durationInSeconds := range tt.prDurations {
				pr := provreqstate.ProvisioningRequestInStateForTests("default", prName, "", "", provreqstate.PendingState, time.Now(), time.Second)
				if durationInSeconds != nil {
					provreqstate.WithMaxRunDuration(*durationInSeconds)(queuedwrapper.ToQueuedProvisioningRequest(*pr))
				}
				prs = append(prs, pr)
				partialOptions = append(partialOptions, PartialOption{ProvReqID: pods.ProvReqID{Name: pr.Name, Namespace: pr.Namespace}})
			}

			orchestrator := Orchestrator{}
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, prs...)
			orchestrator.prCache = provreqcache.NewQueuedProvisioningCache(fakeClient)
			orchestrator.prCache.Refresh()
			gotDuration := orchestrator.highestMaxRunDuration(&CompositeOption{partialOptions: partialOptions})

			if gotDuration == nil || *gotDuration != tt.wantDuration {
				t.Errorf("gotDuration = %v, wantDuration: %v", gotDuration, tt.wantDuration)
			}
		})
	}
}

// preparePods prepares pods for the test.
func preparePods(t *testing.T, podConfigs []PodConfig, podOptions []func(*apiv1.Pod), prs []*provreqwrapper.ProvisioningRequest, now time.Time, gkeCloudProvider *gke.GkeCloudProviderMock, experimentsManager experiments.Manager) ([]*apiv1.Pod, []*apiv1.Pod) {
	t.Helper()
	pods := make([]*apiv1.Pod, 0, len(podConfigs))
	for _, p := range podConfigs {
		pod := BuildTestPod(p.Name, p.Cpu, p.Memory, podOptions...)
		if p.Gpu > 0 {
			RequestGpuForPod(pod, p.Gpu)
		}
		if p.ToleratesGpu {
			TolerateGpuForPod(pod)
		}
		if p.Node != "" {
			pod.Spec.NodeName = p.Node
		}
		pods = append(pods, pod)
	}
	extraPods := []*apiv1.Pod{}
	for _, pr := range prs {
		prPods, err := pr_pods.PodsForProvisioningRequest(gkeCloudProvider, experimentsManager, pr)
		assert.NoError(t, err)
		extraPods = append(extraPods, prPods...)
	}
	return pods, extraPods
}

// prepareNodes prepares gke mocks. Returns mocked cloud provider and list of nodes.
func prepareNodes(t *testing.T, migConfigs []migConfig, napInjectedMigConfigs []injectedMigConfig, wantIncreaseSizeCalls []GroupSizeChange, prs []*provreqwrapper.ProvisioningRequest, standardNodes bool, now time.Time, prIdxOverrides []int) ([]*apiv1.Node, *gke.GkeCloudProviderMock) {
	t.Helper()
	gkeCloudProvider := &gke.GkeCloudProviderMock{}
	gkeManagerMock := &gke.GkeManagerMock{}

	nodes := make([]*apiv1.Node, 0)
	nodeGroups := make([]cloudprovider.NodeGroup, 0)
	migs := make([]*gke.GkeMig, 0)
	migRefsPerNp := map[string][]gce.GceRef{}
	migTargetSizesPerNp := map[string]int64{}
	migsPerId := make(map[string]*gke.GkeMig)
	for _, mc := range migConfigs {
		mig, migNodes := prepareMig(gkeCloudProvider, gkeManagerMock, mc, false, migRefsPerNp, migTargetSizesPerNp, now)
		migs = append(migs, mig)
		nodeGroups = append(nodeGroups, mig)
		nodes = append(nodes, migNodes...)
		migsPerId[mig.Id()] = mig

		if mc.templateNode != nil {
			gkeManagerMock.On("GetMigTemplateNodeInfo", mig).Return(framework.NewTestNodeInfo(buildNode(*mc.templateNode, mc)), nil)
		}
	}

	// Link MIGs by node pool name to ensure they share the same GkeNodePool object.
	// This is required for optimizations that rely on mig.NodePool().Migs().
	migsByNodePool := make(map[string][]*gke.GkeMig)
	for _, m := range migs {
		migsByNodePool[m.NodePoolName()] = append(migsByNodePool[m.NodePoolName()], m)
	}
	for name, ms := range migsByNodePool {
		gke.AddMigsToNodePool(name, ms...)
	}

	for _, mc := range napInjectedMigConfigs {
		mig, migNodes := prepareMig(gkeCloudProvider, gkeManagerMock, mc.migConfig, true, migRefsPerNp, migTargetSizesPerNp, now)
		gkeCloudProvider.On("NewNodeGroup", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mig, nil).Once()
		gkeManagerMock.On("GetMigTemplateNodeInfo", mig).Return(framework.NewTestNodeInfo(migNodes[0]), nil)
		migsPerId[mig.Id()] = mig
		nodeGroups = append(nodeGroups, mig)

		var extraCreatedMigs []*gke.GkeMig
		for _, extraCreatedMig := range mc.extraCreatedMigs {
			extraPreparedMig, extraPreparedMigNodes := prepareMig(gkeCloudProvider, gkeManagerMock, extraCreatedMig, true, migRefsPerNp, migTargetSizesPerNp, now)
			extraCreatedMigs = append(extraCreatedMigs, extraPreparedMig)
			gkeManagerMock.On("GetMigTemplateNodeInfo", extraPreparedMig).Return(framework.NewTestNodeInfo(extraPreparedMigNodes[0]), nil)
			nodeGroups = append(nodeGroups, extraPreparedMig)
			migsPerId[extraPreparedMig.Id()] = extraPreparedMig
		}
		gkeManagerMock.On("CreateNodePool", mig).Return(nil, extraCreatedMigs)
	}
	for i, o := range wantIncreaseSizeCalls {
		prIdx := i
		if i < len(prIdxOverrides) {
			prIdx = prIdxOverrides[i]
		}
		if mig, found := migsPerId[o.GroupName]; found {
			if standardNodes {
				gkeManagerMock.On("CreateInstances", mig, int64(o.SizeChange)).Return(nil)
			} else {
				gkeManagerMock.On("CreateQueuedInstances", pods.GetProvReqID(prs[prIdx]), mig, int64(o.SizeChange)).Return(nil).Once()
			}
		}
	}

	for np, targetSize := range migTargetSizesPerNp {
		gkeManagerMock.On("GetMigsTargetSize", migRefsPerNp[np]).Return(targetSize, nil)
	}
	gkeManagerMock.On("GetGkeMigs").Return(migs)
	gkeManagerMock.On("IsDataplaneV2Enabled").Return(true) // When set to false, GkeMig.TemplateNodeInfo adds a pod, and node groups aren't similar.
	gkeCloudProvider.On("NodeGroups").Return(nodeGroups)
	gkeCloudProvider.On("GetAvailableGPUTypes").Return(map[string]struct{}{})
	gkeCloudProvider.On("GetNodeGpuConfig", mock.AnythingOfType("*v1.Node")).Return(&cloudprovider.GpuConfig{})

	return nodes, gkeCloudProvider
}

func prepareMig(gkeCloudProvider *gke.GkeCloudProviderMock, gkeManagerMock *gke.GkeManagerMock, mc migConfig, autoprovisioned bool, migRefsPerNp map[string][]gce.GceRef, migTargetSizesPerNp map[string]int64, now time.Time) (*gke.GkeMig, []*apiv1.Node) {
	migBuilder := gke.NewTestGkeMigBuilder().
		SetGkeManager(gkeManagerMock).
		SetQueuedProvisioning(mc.queued).
		SetNodePoolName(mc.nodePoolName).
		SetAutoprovisioned(false).
		SetExist(true).
		SetGceRef(gce.GceRef{Project: "project", Zone: mc.zone, Name: mc.migName}).
		SetSpec(mc.spec)
	if autoprovisioned {
		migBuilder.SetAutoprovisioned(true).SetExist(false)
	}
	if mc.useTotalMaxSize {
		migBuilder.SetTotalMaxSize(mc.maxSize)
	} else {
		migBuilder.SetMaxSize(mc.maxSize)
	}
	mig := migBuilder.Build()
	instances := make([]gce.GceInstance, 0, len(mc.nodes))
	nodes := make([]*apiv1.Node, 0)
	for _, nc := range mc.nodes {
		node := buildNode(nc, mc)
		SetNodeReadyState(node, nc.Ready, now.Add(-2*time.Minute))
		gkeCloudProvider.On("NodeGroupForNode", mock.MatchedBy(func(n *apiv1.Node) bool { return n != nil && n.Name == node.Name })).Return(mig, nil)
		gkeCloudProvider.On("HasInstance", node).Return(true, nil)
		nodes = append(nodes, node)
		instances = append(instances, gce.GceInstance{
			Instance: cloudprovider.Instance{
				Id: node.Name,
				Status: &cloudprovider.InstanceStatus{
					State: cloudprovider.InstanceRunning,
				},
			},
		})
	}
	migListManagedResults := gke.MigPageless
	if mc.paginated {
		migListManagedResults = gke.MigPaginated
	}
	gkeManagerMock.On("GetListManagedInstancesResults", mig.GceRef()).Return(migListManagedResults, nil)
	gkeManagerMock.On("GetMigNodes", mig).Return(instances, nil)
	gkeManagerMock.On("GetMigSize", mig).Return(int64(len(instances)), nil)
	gkeManagerMock.On("ScaleDownUnreadyTimeOverride", mig).Return(time.Duration(0), false).Maybe()
	migRefsPerNp[mig.NodePoolName()] = append(migRefsPerNp[mig.NodePoolName()], mig.GceRef())
	migTargetSizesPerNp[mig.NodePoolName()] = migTargetSizesPerNp[mig.NodePoolName()] + int64(len(instances))
	return mig, nodes
}

func buildNode(nc NodeConfig, mc migConfig) *apiv1.Node {
	node := BuildTestNode(nc.Name, nc.Cpu, nc.Memory)
	if nc.Gpu > 0 {
		AddGpusToNode(node, nc.Gpu)
	}
	node.Labels["failure-domain.beta.kubernetes.io/zone"] = mc.zone
	node.Labels[apiv1.LabelZoneFailureDomainStable] = mc.zone
	node.Labels[labels.GkeNodePoolLabel] = mc.nodePoolName
	return node
}

type podTemplateOption func(*apiv1.PodTemplate)

func withZoneSelector(zone string) podTemplateOption {
	return func(pt *apiv1.PodTemplate) {
		if pt.Template.Spec.NodeSelector == nil {
			pt.Template.Spec.NodeSelector = make(map[string]string)
		}
		pt.Template.Spec.NodeSelector["topology.kubernetes.io/zone"] = zone
	}
}

type podSetOpts struct {
	replicasPerPodSet  int
	containersOverride []apiv1.Container
	podTemplateOptions []podTemplateOption
}

func buildProvisioningRequest(namespace, name, cpu, memory, gpu string, podCount int32, antiAffinity bool, creationTimestamp time.Time, podSetsOverride ...podSetOpts) *provreqwrapper.ProvisioningRequest {
	gpuResource := resource.Quantity{}
	tolerations := []apiv1.Toleration{}
	if len(gpu) > 0 {
		gpuResource = resource.MustParse(gpu)
		tolerations = append(tolerations, apiv1.Toleration{Key: "nvidia.com/gpu", Operator: apiv1.TolerationOpExists})
	}

	affinity := &apiv1.Affinity{}
	if antiAffinity {
		affinity.PodAntiAffinity = &apiv1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []apiv1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "app",
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{"test-app"},
							},
						},
					},
					TopologyKey: "failure-domain.beta.kubernetes.io/zone",
				},
			},
		}
	}

	defaultContainers := []apiv1.Container{
		container("", "", nil, cpu, memory, &gpuResource),
	}
	podSets := []prv1.PodSet{}
	podTemplates := []*apiv1.PodTemplate{}
	if podSetsOverride != nil {
		for i, ps := range podSetsOverride {
			podSets = append(podSets, prv1.PodSet{
				Count: int32(ps.replicasPerPodSet),
				PodTemplateRef: prv1.Reference{
					Name: fmt.Sprintf("pt-%s-%d", name, i),
				},
			})

			containers := ps.containersOverride
			if containers == nil {
				containers = defaultContainers
			}
			podTemplate := podTemplate(fmt.Sprintf("pt-%s-%d", name, i), namespace, tolerations, affinity, containers)
			for _, o := range ps.podTemplateOptions {
				o(podTemplate)
			}
			podTemplates = append(podTemplates, podTemplate)
		}
	} else {
		podSets = append(podSets, prv1.PodSet{
			Count: podCount,
			PodTemplateRef: prv1.Reference{
				Name: fmt.Sprintf("pt-%s", name),
			},
		})
		podTemplates = []*apiv1.PodTemplate{
			podTemplate(fmt.Sprintf("pt-%s", name), namespace, tolerations, affinity, defaultContainers),
		}
	}

	return provreqwrapper.NewProvisioningRequest(&prv1.ProvisioningRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			CreationTimestamp: metav1.NewTime(creationTimestamp),
			UID:               types.UID(fmt.Sprintf("pr/%s/%s", namespace, name)),
		},
		Spec: prv1.ProvisioningRequestSpec{
			ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
			PodSets:               podSets,
			Parameters:            map[string]prv1.Parameter{},
		},
		Status: prv1.ProvisioningRequestStatus{
			Conditions: []metav1.Condition{
				{Type: prv1.Accepted, Status: metav1.ConditionFalse},
				{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
				{Type: prv1.Failed, Status: metav1.ConditionFalse},
			},
		},
	},
		podTemplates)
}

func podTemplate(name, namespace string, tolerations []apiv1.Toleration, affinity *apiv1.Affinity, containers []apiv1.Container) *apiv1.PodTemplate {
	return &apiv1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Template: apiv1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					"app": "test-app",
				},
			},
			Spec: apiv1.PodSpec{
				Tolerations: tolerations,
				Affinity:    affinity,
				Containers:  containers,
			},
		},
	}
}

func container(name, image string, command []string, cpu, memory string, gpuResource *resource.Quantity) apiv1.Container {
	if name == "" {
		name = "pi"
	}
	if image == "" {
		image = "perl"
	}
	if command == nil {
		command = []string{"/bin/sh"}
	}

	container := apiv1.Container{
		Name:    "pi",
		Image:   "perl",
		Command: []string{"/bin/sh"},
		Resources: apiv1.ResourceRequirements{
			Limits: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse(cpu),
				apiv1.ResourceMemory: resource.MustParse(memory),
			},
			Requests: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse(cpu),
				apiv1.ResourceMemory: resource.MustParse(memory),
			},
		},
	}
	if gpuResource != nil {
		container.Resources.Limits["nvidia.com/gpu"] = *gpuResource
		container.Resources.Requests["nvidia.com/gpu"] = *gpuResource
	}
	return container
}

func withMaxRunDuration(pr *provreqwrapper.ProvisioningRequest, maxRunDurationInSeconds string) *provreqwrapper.ProvisioningRequest {
	pr.Spec.Parameters[queuedwrapper.MaxRunDurationSecondsKey] = prv1.Parameter(maxRunDurationInSeconds)
	return pr
}

func withObtainabilityStrategyPR(pr *provreqwrapper.ProvisioningRequest) *provreqwrapper.ProvisioningRequest {
	pr.Spec.Parameters[queuedwrapper.CapacitySearchStrategyKey] = prv1.Parameter(queuedwrapper.CapacitySearchStrategyObtainability)
	return pr
}

// mockExpander is a mocked expander, which picks given node group to scale up.
type mockExpander struct {
	optionToChoose GroupSizeChange
	wantOptions    *[]GroupSizeChange
	t              *testing.T
}

// BestOption picks a defined node group to scale up.
func (r mockExpander) BestOption(options []expander.Option, nodeInfo map[string]*framework.NodeInfo) *expander.Option {
	if len(r.optionToChoose.GroupName) == 0 {
		if len(options) > 0 {
			assert.Failf(r.t, "received options, when none were expected", "expected to receive no options, but got: %+v", simplifyExpanderOptions(options))
		}
		return nil
	}

	if r.wantOptions != nil {
		gotOptions := simplifyExpanderOptions(options)
		if diff := cmp.Diff(*r.wantOptions, gotOptions); diff != "" {
			r.t.Errorf("expander proposed options do not match wantOptions (-want +got):\n%s", diff)
		}
	}

	inputOptions := simplifyExpanderOptions(options)
	for i, option := range inputOptions {
		if option == r.optionToChoose {
			return &options[i]
		}
	}
	assert.Failf(r.t, "did not find optionToChoose", "expected: %v, but got %v", r.optionToChoose, inputOptions)
	return nil
}

type alphabeticRecommender struct {
	zones []string
}

func (p *alphabeticRecommender) GetMigInstanceTemplateSelfLink(*gke.GkeMig) (string, error) {
	return "mock_template", nil
}

func (p *alphabeticRecommender) RecommendLocations(ctx context.Context, request gceclient.RecommendLocationsRequest) (*gceclient.RecommendLocationsResponse, error) {
	sort.Strings(p.zones)
	for _, zone := range p.zones {
		preference, defined := request.LocationSettings[zone]
		if defined {
			if preference.ZonePreference == gceclient.ZonePreferenceDeny {
				continue
			}
			if preference.MaxScaleUpSize > 0 && preference.MaxScaleUpSize > int64(request.Count) {
				continue
			}
		}
		// return the first zone after zones that didn't match the request were excluded
		return &gceclient.RecommendLocationsResponse{
			Recommendation: map[string]int{zone: request.Count},
		}, nil
	}
	return nil, fmt.Errorf("No valid zone to recommend")
}

func (p *alphabeticRecommender) GetAllZones() ([]string, error) {
	return p.zones, nil
}

func simplifyExpanderOptions(options []expander.Option) []GroupSizeChange {
	groupSizeChanges := make([]GroupSizeChange, 0, len(options))
	for _, option := range options {
		groupName := option.NodeGroup.Id()
		groupSizeIncrement := option.NodeCount
		groupSizeChanges = append(groupSizeChanges, GroupSizeChange{GroupName: groupName, SizeChange: groupSizeIncrement})
	}
	return groupSizeChanges
}

func simplifyScaleUpInfos(scaleUpInfos []nodegroupset.ScaleUpInfo) []scaleUpInfo {
	result := []scaleUpInfo{}
	for _, info := range scaleUpInfos {
		result = append(result, scaleUpInfo{
			ID:          info.Group.Id(),
			CurrentSize: info.CurrentSize,
			NewSize:     info.NewSize,
			MaxSize:     info.MaxSize,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ID == result[j].ID {
			return result[i].CurrentSize < result[j].CurrentSize
		}
		return result[i].ID < result[j].ID
	})
	return result
}

func simplifyScaleUpStatus(scaleUpStatus *status.ScaleUpStatus) ScaleUpStatusInfo {
	podsTriggeredScaleUp := []string{}
	for _, pod := range scaleUpStatus.PodsTriggeredScaleUp {
		podsTriggeredScaleUp = append(podsTriggeredScaleUp, pod.Name)
	}
	sort.Strings(podsTriggeredScaleUp)

	remainUnschedulable := []string{}
	for _, nsi := range scaleUpStatus.PodsRemainUnschedulable {
		remainUnschedulable = append(remainUnschedulable, nsi.Pod.Name)
	}
	sort.Strings(remainUnschedulable)

	podsAwaitEvaluation := []string{}
	for _, pod := range scaleUpStatus.PodsAwaitEvaluation {
		podsAwaitEvaluation = append(podsAwaitEvaluation, pod.Name)
	}
	sort.Strings(podsAwaitEvaluation)

	return ScaleUpStatusInfo{
		Result:                  scaleUpStatus.Result,
		PodsTriggeredScaleUp:    podsTriggeredScaleUp,
		PodsRemainUnschedulable: remainUnschedulable,
		PodsAwaitEvaluation:     podsAwaitEvaluation,
	}
}

func podNamesForProvReq(pr *provreqwrapper.ProvisioningRequest) []string {
	var names []string
	pods, err := pr_pods.PodsForProvisioningRequest(nil, nil, pr)
	if err != nil {
		return names
	}
	for _, pod := range pods {
		names = append(names, pod.Name)
	}
	return names
}

func buildPodShard(pods []*apiv1.Pod, opts []shardOption) *podsharding.PodShard {
	podShard := &podsharding.PodShard{
		PodUids: map[types.UID]bool{},
		NodeGroupDescriptor: podsharding.NodeGroupDescriptor{
			ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
		},
	}
	for _, p := range pods {
		podShard.PodUids[p.UID] = true
	}
	for _, o := range opts {
		o(podShard)
	}
	return podShard
}

type shardOption func(*podsharding.PodShard)

func withOssScaleUp(shard *podsharding.PodShard) {
	shard.NodeGroupDescriptor.ProvisioningClassName = ""
}

func withObtainabilityStrategyShard(shard *podsharding.PodShard) {
	shard.NodeGroupDescriptor.ProvisioningCapacitySearchStrategy = queuedwrapper.CapacitySearchStrategyObtainability
}

type migOption func(*migConfig)

func withNonQueued() migOption {
	return func(m *migConfig) {
		m.queued = false
	}
}

func withZone(zone string) migOption {
	return func(m *migConfig) {
		m.zone = zone
	}
}

func withMaxSize(size int) migOption {
	return func(m *migConfig) {
		m.maxSize = size
	}
}

func withPaginated() migOption {
	return func(m *migConfig) {
		m.paginated = true
	}
}

func withTemplateNode(node *NodeConfig) migOption {
	return func(m *migConfig) {
		m.templateNode = node
	}
}

func withNodes(nodes []NodeConfig) migOption {
	return func(m *migConfig) {
		m.nodes = nodes
	}
}

func withDefaultCpuMemNode(index int) migOption {
	return withNodes([]NodeConfig{cpuNode(index, withCpu(1000*700))})
}

func withBasicCpuNodes(n int) migOption {
	nodes := make([]NodeConfig, n)
	for i := range n {
		nodes[i] = cpuNode(i, withCpu(1000))
	}
	return withNodes(nodes)
}

func withNodePool(np string) migOption {
	return func(m *migConfig) {
		m.nodePoolName = np
	}
}

func withTotalMaxSize() migOption {
	return func(m *migConfig) {
		m.useTotalMaxSize = true
	}
}

func withSpec(spec *gkeclient.NodePoolSpec) migOption {
	return func(m *migConfig) {
		m.spec = spec
	}
}

func withTpu() migOption {
	return func(m *migConfig) {
		if m.spec == nil {
			m.spec = &gkeclient.NodePoolSpec{}
		}
		m.spec.TpuType = "tpu-v5-lite-podslice"
	}
}

func withBulkA4XSpec(mrd string) migOption {
	return func(m *migConfig) {
		if m.spec == nil {
			m.spec = &gkeclient.NodePoolSpec{}
		}
		m.spec.MachineType = "a4x-highgpu-4g"
		m.spec.FlexStart = true
		m.spec.PlacementGroup = placement.Spec{Policy: "a4x-policy"}
		m.spec.MaxRunDurationInSeconds = mrd
	}
}

func withA4XSpec(mrd string, flexStart bool, placement placement.Spec) migOption {
	return func(m *migConfig) {
		if m.spec == nil {
			m.spec = &gkeclient.NodePoolSpec{}
		}
		m.spec.MachineType = "a4x-highgpu-4g"
		m.spec.FlexStart = flexStart
		m.spec.PlacementGroup = placement
		m.spec.MaxRunDurationInSeconds = mrd
	}
}

func testMig(i int, opts ...migOption) migConfig {
	defaultMig := migConfig{
		nodePoolName: npName(i),
		migName:      migName(i),
		zone:         "us-central1-f",
		queued:       true,
		maxSize:      10,
		nodes:        []NodeConfig{},
	}
	for _, opt := range opts {
		opt(&defaultMig)
	}
	return defaultMig
}

type nodeOption func(*NodeConfig)

func withGpu(gpu int) nodeOption {
	return func(nc *NodeConfig) {
		nc.Gpu = int64(gpu)
	}
}

func withCpu(cpu int) nodeOption {
	return func(nc *NodeConfig) {
		nc.Cpu = int64(cpu)
	}
}

func withMemory(mem int) nodeOption {
	return func(nc *NodeConfig) {
		nc.Memory = int64(mem)
	}
}

func gpuNode(i int, opts ...nodeOption) NodeConfig {
	node := NodeConfig{Name: nodeName(i), Cpu: 1000, Memory: 100 * 1000 * 1000, Gpu: 1, Ready: true}
	for _, opt := range opts {
		opt(&node)
	}
	return node
}

func cpuNode(i int, opts ...nodeOption) NodeConfig {
	node := NodeConfig{Name: nodeName(i), Cpu: 10 * 1000, Memory: 1000 * 1000 * 1000, Gpu: 0, Ready: true}
	for _, opt := range opts {
		opt(&node)
	}
	return node
}

func genNodes(i, count int, nodeFn func(i int, opts ...nodeOption) NodeConfig, opts ...nodeOption) []NodeConfig {
	nodes := make([]NodeConfig, count)
	for j := 0; j < count; j++ {
		nodes[j] = nodeFn(i*10+j, opts...)
	}
	return nodes
}

func npName(i int) string {
	return fmt.Sprintf("np%d", i)
}

func migName(i int) string {
	return fmt.Sprintf("ng%d", i)
}

func nodeName(i int) string {
	return fmt.Sprintf("n%d", i)
}

func podName(i int) string {
	return fmt.Sprintf("p%d", i)
}

func migUrl(i int, zone ...string) string {
	if zone == nil || len(zone) == 0 {
		zone = []string{defaultZone}
	}
	return fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/project/zones/%s/instanceGroups/%s", zone[0], migName(i))
}

func expectStringSliceDetail(t *testing.T, pr *provreqwrapper.ProvisioningRequest, detail string, want []string) {
	t.Helper()
	if len(want) > 0 {
		got := pr.ProvisioningRequest.Status.ProvisioningClassDetails[detail]
		split := strings.Split(string(got), ",")
		sort.Strings(split)
		if diff := cmp.Diff(want, split); diff != "" {
			t.Errorf("%s (-want +got):\n%s", detail, diff)
		}
	}
}
