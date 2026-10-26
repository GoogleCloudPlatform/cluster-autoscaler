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

package flexadvisor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	compute "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	ekvmstypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/core"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// This file contains the Big Unit Tests (BUT) for the fleet-efficiency allocation strategy applied
// automatically to pay-per-pod workloads (go/gke-ca-obt-fleet-efficiency-ap, b/561477622).
//
// Every scenario runs a full autoscaling cycle against an empty cluster with Node Auto-Provisioning
// enabled, so the fleet-efficiency expander has to choose between the NAP candidates generated for
// the workload. The test pod requests 3 vCPUs and 10 GiB of memory, so the lowest-cost candidate that
// fits it is the 4-vCPU standard shape of the machine family (highcpu shapes are too small). The fake
// Flex Advisor returns DefaultZonalScore (0.5) for every machine type without an explicit guidance, so
// the scenarios pin a low score (0.2) on that lowest-cost candidate and a high score (0.9) on a more
// expensive one: fleet-efficiency picks the high-score candidate, while lowest-cost (or any fallback
// to it) picks the cheapest one.

const (
	payPerPodTestPodName = "pay-per-pod-test-pod"

	payPerPodTestPodCpuMillis = 3000
	payPerPodTestPodMemory    = 10 * 1024 * 1024 * 1024

	lowPreferenceScore  = 0.2
	highPreferenceScore = 0.9
)

// payPerPodExperimentMode describes how the PayPerPodFleetEfficiency experiment is configured in a test.
type payPerPodExperimentMode int

const (
	payPerPodExperimentEnabled payPerPodExperimentMode = iota
	payPerPodExperimentDisabledByKillSwitch
	payPerPodExperimentDisabledByVersionGate
)

// payPerPodExperimentFlags returns the experiment overrides for the given mode of the
// PayPerPodFleetEfficiency experiment, along with AutopilotArmMachineFallbacks enabled so that the
// general-purpose-arm podFamily rule can provision classic ARM machine families (n4a, c4a).
func payPerPodExperimentFlags(mode payPerPodExperimentMode) (map[string]bool, map[string]string) {
	boolFlags := map[string]bool{
		experiments.AutopilotArmMachineFallbacksEnabledFlag: true,
	}
	stringFlags := map[string]string{
		experiments.AutopilotArmMachineFallbacksMinCAVersionFlag: "0.0.0",
	}
	switch mode {
	case payPerPodExperimentEnabled:
		boolFlags[experiments.PayPerPodFleetEfficiencyEnabledFlag] = true
		stringFlags[experiments.PayPerPodFleetEfficiencyMinCAVersionFlag] = "0.0.0"
	case payPerPodExperimentDisabledByKillSwitch:
		boolFlags[experiments.PayPerPodFleetEfficiencyEnabledFlag] = false
		stringFlags[experiments.PayPerPodFleetEfficiencyMinCAVersionFlag] = "0.0.0"
	case payPerPodExperimentDisabledByVersionGate:
		// Leave MinCAVersion unset so the evaluator falls back to its failsafe ("disabled").
		boolFlags[experiments.PayPerPodFleetEfficiencyEnabledFlag] = true
	default:
		panic(fmt.Sprintf("unexpected payPerPodExperimentMode: %v", mode))
	}
	return boolFlags, stringFlags
}

// withClassicMachineFamilies disables resizable VM autoprovisioning (EK, E4, E4A), so that Autopilot and
// podFamily workloads are served by classic machine families (e2, n4a, c4a) which the test infrastructure supports.
func withClassicMachineFamilies() integration.Option[*internalopts.AutoscalingOptions] {
	return func(o *internalopts.AutoscalingOptions) *internalopts.AutoscalingOptions {
		o.InternalOptions.EkAutoprovisioning = string(ekvmstypes.EkAutoprovisioningDisabled)
		o.InternalOptions.E4Autoprovisioning = string(ekvmstypes.E4AutoprovisioningDisabled)
		o.InternalOptions.E4aAutoprovisioning = string(ekvmstypes.E4aAutoprovisioningDisabled)
		return o
	}
}

// withAutopilotCluster configures the autoscaler options of a GKE Autopilot cluster with e2 as the default
// NAP machine family.
func withAutopilotCluster() integration.Option[*internalopts.AutoscalingOptions] {
	return func(o *internalopts.AutoscalingOptions) *internalopts.AutoscalingOptions {
		o.AutopilotEnabled = true
		o.NapDefaultMachineTypeFamily = "e2"
		return o
	}
}

// newPayPerPodTestConfig returns a test config of an empty single-zone cluster with Node Auto-Provisioning
// and Flex Advisor enabled, and the PayPerPodFleetEfficiency experiment configured according to mode.
func newPayPerPodTestConfig(autopilot bool, mode payPerPodExperimentMode) *integration.TestConfig {
	boolFlags, stringFlags := payPerPodExperimentFlags(mode)
	overrides := []integration.Option[*internalopts.AutoscalingOptions]{
		integration.WithMaxMemoryTotal(140 * 1024 * 1024 * 1024),
		integration.WithFlexAdvisorEnabled(),
		integration.WithAutoProvisioningEnabled(),
		withClassicMachineFamilies(),
	}
	if autopilot {
		overrides = append(overrides, withAutopilotCluster())
	}
	return integration.NewTestConfig().
		WithExperimentOverrides(boolFlags, stringFlags).
		WithOverrides(overrides...).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithAutoprovisioningLocations(ZoneB),
		)
}

// withTolerationForTaintKey tolerates every NoSchedule taint with the given key. Autopilot pods get such
// tolerations (e.g. for the compute class, machine family or architecture taints) from the Autopilot webhook.
func withTolerationForTaintKey(key string) func(*apiv1.Pod) {
	return pod.WithToleration(key, apiv1.TolerationOpExists, apiv1.TaintEffectNoSchedule)
}

// withAutopilotManagedComputeClass selects the given Autopilot-managed compute class on a Standard cluster
// and tolerates the taint of the Autopilot-managed node pools, like the GKE webhook does.
func withAutopilotManagedComputeClass(cccName string) func(*apiv1.Pod) {
	return func(p *apiv1.Pod) {
		pod.WithCCC(cccName)(p)
		withTolerationForTaintKey(gkelabels.ManagedNodeLabel)(p)
	}
}

// podFamilyComputeClass builds an Autopilot-managed compute class with node pool auto-creation and a single
// podFamily priority rule using the given pod family and allocation strategy (nil to omit the strategy).
func podFamilyComputeClass(name, podFamily string, strategy *v1.AllocationStrategy) *v1.ComputeClass {
	return ccc.NewComputeClassBuilder(name).
		WithAutopilot(true).
		WithNodePoolAutoCreation(true).
		WithPriorities(v1.Priority{
			PodFamily:          ptr.To(podFamily),
			PriorityScore:      ptr.To(100),
			AllocationStrategy: strategy,
		}).
		Build()
}

// buildPayPerPodTestPod builds an unschedulable pod requesting 3 vCPUs and 10 GiB of memory, so that
// among the machine types of a family, the 4-vCPU standard shape is the cheapest one that fits it.
func buildPayPerPodTestPod(options ...func(*apiv1.Pod)) *apiv1.Pod {
	options = append([]func(*apiv1.Pod){tu.MarkUnschedulable()}, options...)
	return tu.BuildTestPod(payPerPodTestPodName, payPerPodTestPodCpuMillis, payPerPodTestPodMemory, options...)
}

// runPayPerPodScaleUp adds the pod to the cluster, runs a single autoscaling cycle followed by the fake
// scheduler and returns the machine type of the single node pool autoprovisioned for the pod.
func runPayPerPodScaleUp(ctx context.Context, t *testing.T, autoscaler core.Autoscaler, infra *integration.TestInfrastructure, testPod *apiv1.Pod) string {
	t.Helper()
	infra.Fakes.K8s.AddPod(testPod)

	integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
	infra.Fakes.RunScheduler(ctx, t)

	nodePool := infra.Fakes.GkeService.MustGetSingleAutoprovisionedNodePool(t)

	updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods(testPod.Namespace).Get(ctx, testPod.Name, metav1.GetOptions{})
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod %s to be scheduled", testPod.Name)
	assert.Contains(t, updatedPod.Spec.NodeName, nodePool.Name, "Expected pod %s to be scheduled on the autoprovisioned node pool %s", testPod.Name, nodePool.Name)
	return nodePool.Config.MachineType
}

// assertAllocationStrategyMetrics verifies that exactly one scale-up was recorded with the given
// allocation strategy labels, both in scaleups_with_allocation_strategy and nodes_with_allocation_strategy.
func assertAllocationStrategyMetrics(t *testing.T, requestedStrategy string, fallbackReason metrics.AllocationStrategyFallbackReason, source metrics.AllocationStrategySource, machineType string) {
	t.Helper()
	scaleupCount, err := metrics.GetScaleupsWithAllocationStrategyCountForTest(requestedStrategy, fallbackReason, source)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), scaleupCount, "Expected scaleups_with_allocation_strategy{strategy=%s, reason=%q, source=%s} to be 1", requestedStrategy, fallbackReason, source)
	nodesCount, err := metrics.GetNodesWithAllocationStrategyCountForTest(requestedStrategy, fallbackReason, machineType)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), nodesCount, "Expected nodes_with_allocation_strategy{strategy=%s, reason=%q, machine_type=%s} to be 1", requestedStrategy, fallbackReason, machineType)
}

// assertNoStrategyMetrics verifies that no scale-up or node was recorded with the given allocation
// strategy, for any fallback reason, any allocation strategy source, and the given machine type.
func assertNoStrategyMetrics(t *testing.T, strategy v1.AllocationStrategy, machineType string) {
	t.Helper()
	reasons := []metrics.AllocationStrategyFallbackReason{
		metrics.AllocationStrategyFallbackNone,
		metrics.AllocationStrategyFallbackMissingScore,
		metrics.AllocationStrategyFallbackTieBreak,
		metrics.AllocationStrategyFallbackReservationPresent,
		metrics.AllocationStrategyFallbackError,
		metrics.AllocationStrategyFallbackFlexAdvisorNotSupported,
		metrics.AllocationStrategyFallbackUnsupported,
	}
	sources := []metrics.AllocationStrategySource{
		metrics.AllocationStrategySourceUnknown,
		metrics.AllocationStrategySourceCluster,
		metrics.AllocationStrategySourceCustomComputeClass,
		metrics.AllocationStrategySourcePredefinedComputeClass,
		metrics.AllocationStrategySourceAutopilotWorkload,
	}
	for _, reason := range reasons {
		nodesCount, err := metrics.GetNodesWithAllocationStrategyCountForTest(string(strategy), reason, machineType)
		assert.NoError(t, err)
		assert.Equal(t, float64(0), nodesCount, "Expected no %s nodes_with_allocation_strategy to be recorded (reason=%q, machine_type=%s)", strategy, reason, machineType)
		for _, source := range sources {
			scaleupCount, err := metrics.GetScaleupsWithAllocationStrategyCountForTest(string(strategy), reason, source)
			assert.NoError(t, err)
			assert.Equal(t, float64(0), scaleupCount, "Expected no %s scaleups_with_allocation_strategy to be recorded (reason=%q, source=%s)", strategy, reason, source)
		}
	}
}

// assertNoFleetEfficiencyMetrics verifies that no scale-up or node was evaluated with the
// fleet-efficiency allocation strategy.
func assertNoFleetEfficiencyMetrics(t *testing.T, machineType string) {
	t.Helper()
	assertNoStrategyMetrics(t, v1.AllocationStrategyFleetEfficiency, machineType)
}

// assertNoAllocationStrategyMetrics verifies that no allocation strategy (neither fleet-efficiency
// nor lowest-cost) was evaluated for the scale-up.
func assertNoAllocationStrategyMetrics(t *testing.T, machineType string) {
	t.Helper()
	assertNoStrategyMetrics(t, v1.AllocationStrategyFleetEfficiency, machineType)
	assertNoStrategyMetrics(t, v1.AllocationStrategyLowestCost, machineType)
}

// TestFleetEfficiency_PayPerPod_AutopilotWorkloads verifies that pay-per-pod workloads on Autopilot
// clusters are scaled up with the fleet-efficiency allocation strategy, i.e. that the autoprovisioned node
// pool uses the machine type with the highest Flex Advisor preference score among the machine families of
// the workload:
//   - default x86 workloads (no compute class) are evaluated against the general-purpose pod family,
//   - default ARM workloads are evaluated against the general-purpose-arm pod family,
//   - Balanced and Scale-Out predefined compute class workloads are evaluated against the machine families
//     of the compute class.
func TestFleetEfficiency_PayPerPod_AutopilotWorkloads(t *testing.T) {
	testCases := map[string]struct {
		podOptions          []func(*apiv1.Pod)
		guidances           []fake.CapacityGuidance
		expectedMachineType string
		expectedSource      metrics.AllocationStrategySource
	}{
		"default x86 workload - highest scored general-purpose machine type": {
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(lowPreferenceScore),
				fake.NewGuidance("e2-standard-8").WithScore(highPreferenceScore),
			},
			expectedMachineType: "e2-standard-8",
			expectedSource:      metrics.AllocationStrategySourceAutopilotWorkload,
		},
		"default ARM workload - highest scored general-purpose-arm machine type": {
			podOptions: []func(*apiv1.Pod){
				pod.WithNodeSelectorEntry(apiv1.LabelArchStable, "arm64"),
				withTolerationForTaintKey(apiv1.LabelArchStable),
				withTolerationForTaintKey(gkelabels.MachineFamilyLabel),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("c4a-standard-4").WithScore(lowPreferenceScore),
				fake.NewGuidance("c4a-standard-8").WithScore(highPreferenceScore),
			},
			expectedMachineType: "c4a-standard-8",
			expectedSource:      metrics.AllocationStrategySourceAutopilotWorkload,
		},
		"Balanced compute class workload - highest scored Balanced machine type": {
			podOptions: []func(*apiv1.Pod){
				pod.WithCCC("Balanced"),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n2-standard-4").WithScore(lowPreferenceScore),
				fake.NewGuidance("n2d-standard-4").WithScore(lowPreferenceScore),
				fake.NewGuidance("n2-standard-8").WithScore(highPreferenceScore),
			},
			expectedMachineType: "n2-standard-8",
			expectedSource:      metrics.AllocationStrategySourcePredefinedComputeClass,
		},
		"Scale-Out compute class workload - highest scored Scale-Out machine type": {
			podOptions: []func(*apiv1.Pod){
				pod.WithCCC("Scale-Out"),
				withTolerationForTaintKey(apiv1.LabelArchStable),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("t2d-standard-4").WithScore(lowPreferenceScore),
				fake.NewGuidance("t2d-standard-8").WithScore(highPreferenceScore),
			},
			expectedMachineType: "t2d-standard-8",
			expectedSource:      metrics.AllocationStrategySourcePredefinedComputeClass,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			testConfig := newPayPerPodTestConfig(true, payPerPodExperimentEnabled)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(tc.guidances...)

				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)
				defer integration_synctest.TearDown(cancel)

				PrimeFlexAdvisorCacheForPod(ctx, t, autoscaler, infra, tc.podOptions...)
				metrics.ResetAllForTest()

				machineType := runPayPerPodScaleUp(ctx, t, autoscaler, infra, buildPayPerPodTestPod(tc.podOptions...))
				assert.Equal(t, tc.expectedMachineType, machineType, "Expected fleet-efficiency to choose the highest scored machine type")
				assert.Greater(t, infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls(), 0, "Expected the workload to be registered in Flex Advisor")
				assertAllocationStrategyMetrics(t, string(v1.AllocationStrategyFleetEfficiency), metrics.AllocationStrategyFallbackNone, tc.expectedSource, tc.expectedMachineType)
			})
		})
	}
}

// TestFleetEfficiency_PayPerPod_PodFamilyComputeClass verifies that on Standard clusters pods matching a
// general-purpose or general-purpose-arm podFamily priority rule of a Custom Compute Class (pre-installed,
// custom or default) are scaled up with the fleet-efficiency allocation strategy, unless the rule explicitly
// requests lowest-cost.
//
// A namespace-level default compute class is applied by the GKE webhook, which injects the compute class
// node selector into the pod, so from the Cluster Autoscaler's point of view it is identical to the pod
// explicitly selecting the (pre-installed or custom) compute class. The cluster-level default compute class
// is resolved by the Cluster Autoscaler itself for pods without a compute class selector.
func TestFleetEfficiency_PayPerPod_PodFamilyComputeClass(t *testing.T) {
	testCases := map[string]struct {
		computeClass        *v1.ComputeClass
		defaultComputeClass bool
		podOptions          []func(*apiv1.Pod)
		expectedMachineType string
		expectedStrategy    v1.AllocationStrategy
	}{
		"pre-installed autopilot compute class with general-purpose podFamily rule - fleet efficiency": {
			computeClass:        podFamilyComputeClass("autopilot", rules.GeneralPurposePodFamily, nil),
			podOptions:          []func(*apiv1.Pod){withAutopilotManagedComputeClass("autopilot")},
			expectedMachineType: "e2-standard-8",
			expectedStrategy:    v1.AllocationStrategyFleetEfficiency,
		},
		"pre-installed autopilot-arm compute class with general-purpose-arm podFamily rule - fleet efficiency": {
			computeClass: podFamilyComputeClass("autopilot-arm", rules.GeneralPurposeArmPodFamily, nil),
			podOptions: []func(*apiv1.Pod){
				withAutopilotManagedComputeClass("autopilot-arm"),
				withTolerationForTaintKey(apiv1.LabelArchStable),
			},
			expectedMachineType: "n4a-standard-8",
			expectedStrategy:    v1.AllocationStrategyFleetEfficiency,
		},
		"custom compute class with podFamily rule - fleet efficiency": {
			computeClass:        podFamilyComputeClass("custom-gp-ccc", rules.GeneralPurposePodFamily, nil),
			podOptions:          []func(*apiv1.Pod){withAutopilotManagedComputeClass("custom-gp-ccc")},
			expectedMachineType: "e2-standard-8",
			expectedStrategy:    v1.AllocationStrategyFleetEfficiency,
		},
		"custom compute class with podFamily rule and explicit lowest-cost - lowest cost": {
			computeClass:        podFamilyComputeClass("custom-gp-ccc", rules.GeneralPurposePodFamily, ptr.To(v1.AllocationStrategyLowestCost)),
			podOptions:          []func(*apiv1.Pod){withAutopilotManagedComputeClass("custom-gp-ccc")},
			expectedMachineType: "e2-standard-4",
			expectedStrategy:    v1.AllocationStrategyLowestCost,
		},
		"default compute class with podFamily rule - fleet efficiency": {
			computeClass:        podFamilyComputeClass(gkelabels.DefaultCCCName, rules.GeneralPurposePodFamily, nil),
			defaultComputeClass: true,
			podOptions:          []func(*apiv1.Pod){withTolerationForTaintKey(gkelabels.ManagedNodeLabel)},
			expectedMachineType: "e2-standard-8",
			expectedStrategy:    v1.AllocationStrategyFleetEfficiency,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			testConfig := newPayPerPodTestConfig(false, payPerPodExperimentEnabled).
				WithCccCrds(tc.computeClass)
			if tc.defaultComputeClass {
				testConfig = testConfig.WithDefaultComputeClassEnabled()
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
					fake.NewGuidance("e2-standard-4").WithScore(lowPreferenceScore),
					fake.NewGuidance("e2-standard-8").WithScore(highPreferenceScore),
					fake.NewGuidance("n4a-standard-4").WithScore(lowPreferenceScore),
					fake.NewGuidance("n4a-standard-8").WithScore(highPreferenceScore),
				)

				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)
				defer integration_synctest.TearDown(cancel)

				PrimeFlexAdvisorCacheForPod(ctx, t, autoscaler, infra, tc.podOptions...)
				metrics.ResetAllForTest()

				machineType := runPayPerPodScaleUp(ctx, t, autoscaler, infra, buildPayPerPodTestPod(tc.podOptions...))
				assert.Equal(t, tc.expectedMachineType, machineType)
				assert.Greater(t, infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls(), 0, "Expected the workload to be registered in Flex Advisor")
				assertAllocationStrategyMetrics(t, string(tc.expectedStrategy), metrics.AllocationStrategyFallbackNone, metrics.AllocationStrategySourceCustomComputeClass, tc.expectedMachineType)
			})
		})
	}
}

// TestFleetEfficiency_PayPerPod_NonPayPerPodWorkloads verifies that workloads on Autopilot clusters which
// are not billed per pod (GPU and TPU accelerator workloads, Performance compute class / Slice of Hardware
// workloads, and workloads pinning a machine family) do not use the fleet-efficiency allocation strategy:
// GPU and machine-family workloads ignore Flex Advisor preference scores and scale up the lowest-cost
// candidate without registering in Flex Advisor; TPU workloads (whose required gke-accelerator-count
// selector pins a single machine type per TPU type) likewise do not register in Flex Advisor or record
// fleet-efficiency metrics; and Performance compute class workloads still register a flexibility scope in
// Flex Advisor for the predefined compute class, while the expander ignores the preference scores because
// the workload is billed per node.
func TestFleetEfficiency_PayPerPod_NonPayPerPodWorkloads(t *testing.T) {
	testCases := map[string]struct {
		podOptions                 []func(*apiv1.Pod)
		clusterOverrides           []integration.Option[*gke_api_beta.Cluster]
		setupInfra                 func(infra *integration.TestInfrastructure)
		guidances                  []fake.CapacityGuidance
		expectedMachineType        string
		expectedFlexAdvisorQueried bool
	}{
		"GPU workload - lowest cost": {
			podOptions: []func(*apiv1.Pod){
				pod.WithNodeSelectorEntry(gkelabels.GPULabel, "nvidia-l4"),
				pod.WithResource("nvidia.com/gpu", "1", "1"),
				withTolerationForTaintKey("nvidia.com/gpu"),
			},
			clusterOverrides: []integration.Option[*gke_api_beta.Cluster]{
				// NAP requires a resource limit for the requested GPU type.
				integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
					{ResourceType: "cpu", Maximum: integration.DefaultMaxCoresResourceLimit},
					{ResourceType: "memory", Maximum: integration.DefaultMaxMemoryResourceLimit},
					{ResourceType: "nvidia-l4", Maximum: 10},
				}),
			},
			setupInfra: func(infra *integration.TestInfrastructure) {
				// NAP validates the GPU count against the maximum number of cards per instance in the zone.
				infra.Fakes.GceService.WithAcceleratorTypes(&compute.AcceleratorTypeList{
					Items: []*compute.AcceleratorType{
						{Name: "nvidia-l4", Zone: ZoneB, MaximumCardsPerInstance: 8},
					},
				})
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("g2-standard-4").WithScore(lowPreferenceScore),
				fake.NewGuidance("g2-standard-8").WithScore(highPreferenceScore),
			},
			expectedMachineType:        "g2-standard-4",
			expectedFlexAdvisorQueried: false,
		},
		"TPU workload - not evaluated with fleet-efficiency": {
			// NAP requires gke-tpu-accelerator and gke-accelerator-count on TPU pods, which uniquely
			// determines a single machine type (ct5l-hightpu-1t); this subtest verifies that the TPU
			// workload is not registered in Flex Advisor and records no fleet-efficiency metrics.
			podOptions: []func(*apiv1.Pod){
				pod.WithNodeSelectorEntry(gkelabels.TPULabel, gkelabels.TpuV5LiteDeviceValue),
				pod.WithNodeSelectorEntry(gkelabels.AcceleratorCountLabel, "1"),
				pod.WithResource("google.com/tpu", "1", "1"),
				withTolerationForTaintKey("google.com/tpu"),
			},
			clusterOverrides: []integration.Option[*gke_api_beta.Cluster]{
				// NAP requires a resource limit for the requested TPU type.
				integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
					{ResourceType: "cpu", Maximum: integration.DefaultMaxCoresResourceLimit},
					{ResourceType: "memory", Maximum: integration.DefaultMaxMemoryResourceLimit},
					{ResourceType: gkelabels.TpuV5LiteDeviceValue, Maximum: 10},
				}),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("ct5l-hightpu-1t").WithScore(lowPreferenceScore),
				fake.NewGuidance("ct5l-hightpu-4t").WithScore(highPreferenceScore),
			},
			expectedMachineType:        "ct5l-hightpu-1t",
			expectedFlexAdvisorQueried: false,
		},
		"Performance compute class (Slice of Hardware) workload - lowest cost": {
			podOptions: []func(*apiv1.Pod){
				pod.WithCCC("Performance"),
				pod.WithNodeSelectorEntry(gkelabels.MachineFamilyLabel, "c3"),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("c3-standard-4").WithScore(lowPreferenceScore),
				fake.NewGuidance("c3-standard-8").WithScore(highPreferenceScore),
			},
			expectedMachineType: "c3-standard-4",
			// PodListProcessor still registers a flexibility scope for the Performance predefined compute class,
			// while fleetEfficiencyFilter ignores the preference scores because GetBillingModel is node-based.
			expectedFlexAdvisorQueried: true,
		},
		"workload pinning a machine family - lowest cost": {
			podOptions: []func(*apiv1.Pod){
				pod.WithNodeSelectorEntry(gkelabels.MachineFamilyLabel, "n2"),
				withTolerationForTaintKey(gkelabels.MachineFamilyLabel),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n2-standard-4").WithScore(lowPreferenceScore),
				fake.NewGuidance("n2-standard-8").WithScore(highPreferenceScore),
			},
			expectedMachineType:        "n2-standard-4",
			expectedFlexAdvisorQueried: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			testConfig := newPayPerPodTestConfig(true, payPerPodExperimentEnabled).
				WithClusterOverrides(tc.clusterOverrides...)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(tc.guidances...)

				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)
				defer integration_synctest.TearDown(cancel)
				// setupInfra must run after MustSetupAutoscaler because SetupAutoscaler calls
				// WithDefaultAccelerators() on GceService and would otherwise overwrite custom accelerators.
				if tc.setupInfra != nil {
					tc.setupInfra(infra)
				}

				PrimeFlexAdvisorCacheForPod(ctx, t, autoscaler, infra, tc.podOptions...)
				metrics.ResetAllForTest()

				machineType := runPayPerPodScaleUp(ctx, t, autoscaler, infra, buildPayPerPodTestPod(tc.podOptions...))
				assert.Equal(t, tc.expectedMachineType, machineType, "Expected the lowest-cost machine type to be chosen regardless of Flex Advisor preference scores")
				assertNoAllocationStrategyMetrics(t, tc.expectedMachineType)
				if tc.expectedFlexAdvisorQueried {
					assert.Greater(t, infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls(), 0, "Expected the workload to be registered in Flex Advisor")
				} else {
					assert.Equal(t, 0, infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls(), "Expected the workload not to be registered in Flex Advisor")
				}
			})
		})
	}
}

// TestFleetEfficiency_PayPerPod_FlexAdvisorDataUnavailable verifies that when Flex Advisor preference scores
// are not available or invalid for a pay-per-pod Autopilot workload (the capacity guidance omits the machine
// type, the Flex Advisor call fails, or a score is out of range), the scale-up still succeeds by falling back
// to the lowest-cost expander and the fallback is recorded in the allocation strategy metrics.
func TestFleetEfficiency_PayPerPod_FlexAdvisorDataUnavailable(t *testing.T) {
	testCases := map[string]struct {
		guidances              []fake.CapacityGuidance
		expectedFallbackReason metrics.AllocationStrategyFallbackReason
	}{
		"preference scores missing": {
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithOmit(true),
				fake.NewGuidance("e2-standard-8").WithScore(highPreferenceScore),
			},
			expectedFallbackReason: metrics.AllocationStrategyFallbackMissingScore,
		},
		"Flex Advisor call fails": {
			// When the background FetchCapacityGuidance call fails during cache priming, no snapshot is
			// stored in the Flex Advisor cache; during scale-up, fleetEfficiencyFilter sees
			// ErrSnapshotNotFound from the cache and maps it to AllocationStrategyFallbackMissingScore.
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithError(errors.New("flex advisor unavailable")),
			},
			expectedFallbackReason: metrics.AllocationStrategyFallbackMissingScore,
		},
		"invalid preference score": {
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("e2-standard-4").WithScore(2.0),
				fake.NewGuidance("e2-standard-8").WithScore(highPreferenceScore),
			},
			expectedFallbackReason: metrics.AllocationStrategyFallbackError,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			testConfig := newPayPerPodTestConfig(true, payPerPodExperimentEnabled)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(tc.guidances...)

				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)
				defer integration_synctest.TearDown(cancel)

				PrimeFlexAdvisorCacheForPod(ctx, t, autoscaler, infra)
				metrics.ResetAllForTest()

				machineType := runPayPerPodScaleUp(ctx, t, autoscaler, infra, buildPayPerPodTestPod())
				assert.Equal(t, "e2-standard-4", machineType, "Expected fallback to lowest-cost to choose the cheapest machine type")
				assert.Greater(t, infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls(), 0, "Expected the workload to be registered in Flex Advisor")
				assertAllocationStrategyMetrics(t, string(v1.AllocationStrategyFleetEfficiency), tc.expectedFallbackReason, metrics.AllocationStrategySourceAutopilotWorkload, "e2-standard-4")
			})
		})
	}
}

// TestFleetEfficiency_PayPerPod_ExperimentDisabled verifies that when the PayPerPodFleetEfficiency
// experiment is disabled (by either of its two keys: the Enabled kill switch or the MinCAVersion gate),
// pay-per-pod workloads keep the pre-existing behavior:
//   - default Autopilot workloads are not registered in Flex Advisor and are scaled up with the lowest-cost
//     expander without any allocation strategy being evaluated,
//   - Predefined Compute Class (Balanced / Scale-Out) workloads on Autopilot are still registered in Flex
//     Advisor, but are scaled up with the lowest-cost expander without any allocation strategy being evaluated,
//   - podFamily priority rules without an explicit allocation strategy inherit the cluster default
//     allocation strategy (lowest-cost) instead of defaulting to fleet-efficiency.
func TestFleetEfficiency_PayPerPod_ExperimentDisabled(t *testing.T) {
	testCases := map[string]struct {
		autopilot      bool
		experimentMode payPerPodExperimentMode
		computeClass   *v1.ComputeClass
		podOptions     []func(*apiv1.Pod)
		// expectedMachineType is the machine type expected to be autoprovisioned; defaults to "e2-standard-4".
		expectedMachineType        string
		expectedFlexAdvisorQueried bool
		// expectedStrategy is the allocation strategy expected in the metrics; empty when no strategy is evaluated.
		expectedStrategy string
		expectedSource   metrics.AllocationStrategySource
	}{
		"default Autopilot workload, kill switch - lowest cost without Flex Advisor": {
			autopilot:      true,
			experimentMode: payPerPodExperimentDisabledByKillSwitch,
		},
		"default Autopilot workload, version gate not met - lowest cost without Flex Advisor": {
			autopilot:      true,
			experimentMode: payPerPodExperimentDisabledByVersionGate,
		},
		"Scale-Out compute class on Autopilot, kill switch - lowest cost": {
			autopilot:                  true,
			experimentMode:             payPerPodExperimentDisabledByKillSwitch,
			podOptions:                 []func(*apiv1.Pod){pod.WithCCC("Scale-Out"), withTolerationForTaintKey(apiv1.LabelArchStable)},
			expectedMachineType:        "t2d-standard-4",
			expectedFlexAdvisorQueried: true,
		},
		"Scale-Out compute class on Autopilot, version gate not met - lowest cost": {
			autopilot:                  true,
			experimentMode:             payPerPodExperimentDisabledByVersionGate,
			podOptions:                 []func(*apiv1.Pod){pod.WithCCC("Scale-Out"), withTolerationForTaintKey(apiv1.LabelArchStable)},
			expectedMachineType:        "t2d-standard-4",
			expectedFlexAdvisorQueried: true,
		},
		"Standard cluster compute class with podFamily rule, kill switch - cluster default lowest cost": {
			experimentMode:             payPerPodExperimentDisabledByKillSwitch,
			computeClass:               podFamilyComputeClass("autopilot", rules.GeneralPurposePodFamily, nil),
			podOptions:                 []func(*apiv1.Pod){withAutopilotManagedComputeClass("autopilot")},
			expectedFlexAdvisorQueried: true,
			expectedStrategy:           string(v1.AllocationStrategyLowestCost),
			expectedSource:             metrics.AllocationStrategySourceCluster,
		},
		"Standard cluster compute class with podFamily rule, version gate not met - cluster default lowest cost": {
			experimentMode:             payPerPodExperimentDisabledByVersionGate,
			computeClass:               podFamilyComputeClass("autopilot", rules.GeneralPurposePodFamily, nil),
			podOptions:                 []func(*apiv1.Pod){withAutopilotManagedComputeClass("autopilot")},
			expectedFlexAdvisorQueried: true,
			expectedStrategy:           string(v1.AllocationStrategyLowestCost),
			expectedSource:             metrics.AllocationStrategySourceCluster,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			testConfig := newPayPerPodTestConfig(tc.autopilot, tc.experimentMode)
			if tc.computeClass != nil {
				testConfig = testConfig.WithCccCrds(tc.computeClass)
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
					fake.NewGuidance("e2-standard-4").WithScore(lowPreferenceScore),
					fake.NewGuidance("e2-standard-8").WithScore(highPreferenceScore),
					fake.NewGuidance("t2d-standard-4").WithScore(lowPreferenceScore),
					fake.NewGuidance("t2d-standard-8").WithScore(highPreferenceScore),
				)

				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)
				defer integration_synctest.TearDown(cancel)

				PrimeFlexAdvisorCacheForPod(ctx, t, autoscaler, infra, tc.podOptions...)
				metrics.ResetAllForTest()

				expectedMachineType := tc.expectedMachineType
				if expectedMachineType == "" {
					expectedMachineType = "e2-standard-4"
				}

				machineType := runPayPerPodScaleUp(ctx, t, autoscaler, infra, buildPayPerPodTestPod(tc.podOptions...))
				assert.Equal(t, expectedMachineType, machineType, "Expected the lowest-cost machine type to be chosen when the experiment is disabled")
				if tc.expectedFlexAdvisorQueried {
					assert.Greater(t, infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls(), 0, "Expected the workload to be registered in Flex Advisor")
				} else {
					assert.Equal(t, 0, infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls(), "Expected the workload not to be registered in Flex Advisor")
				}
				if tc.expectedStrategy != "" {
					assertNoFleetEfficiencyMetrics(t, expectedMachineType)
					assertAllocationStrategyMetrics(t, tc.expectedStrategy, metrics.AllocationStrategyFallbackNone, tc.expectedSource, expectedMachineType)
				} else {
					assertNoAllocationStrategyMetrics(t, expectedMachineType)
				}
			})
		})
	}
}
