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

package noscaleup

import (
	stderrors "errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/orchestrator"
	core_test "k8s.io/autoscaler/cluster-autoscaler/core/test"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/processors"
	"k8s.io/autoscaler/cluster-autoscaler/processors/callbacks"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodeinfosprovider"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/resourcequotas"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/machineselection"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	npc_crd "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/gkedebuggingsnapshot"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	internal_customresources "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/customresources"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/zonetypes"
)

func TestGetNewReasons(t *testing.T) {
	now := time.Now()
	nsu := NewNoScaleUp(time.Minute)

	mig1 := &vistypes.GkeMig{Id: "mig1", Name: "mig1", NodePoolName: "np1", Zone: "z1", Exists: true}
	noScaleUpInfo := &vistypes.NoScaleUpInfo{
		Pod: &vistypes.Pod{Name: "pod1", Uid: "puid1"},
		SkippedNodeGroups: map[string]status.Reasons{
			"mig1": &mockFailureReasons{reasons: []string{"Reason1", "Reason2"}},
		},
	}
	scaleUpStatus := &vistypes.ScaleUpStatus{
		Result:         status.ScaleUpInCooldown,
		ConsideredMigs: []*vistypes.GkeMig{mig1},
		NoScaleUpInfos: []*vistypes.NoScaleUpInfo{noScaleUpInfo},
	}
	napStatus := &vistypes.NapStatus{
		Result: autoprovisioning.NapDisabled,
	}

	// Check that correct reasons are returned when there are unschedulable pods.
	reasons := nsu.GetNewReasons(scaleUpStatus, napStatus, now)
	assert.Equal(t, &Reasons{
		TopLevel:    vistypes.NewNoScaleUpInBackoffMsg(),
		TopLevelNap: vistypes.NewNoScaleUpNapDisabledMsg(),
		SkippedMigs: []*vistypes.MigExplanation{
			{Mig: mig1, Reason: vistypes.NewNoScaleUpMigSkippedMsg([]string{"Reason1", "Reason2"})},
		},
		PodGroups: []*vistypes.PodGroupExplanation{
			{
				SamplePod:  noScaleUpInfo.Pod,
				PodCount:   1,
				MigReasons: map[string]*vistypes.MigExplanation{},
				NapReasons: []*vistypes.Message{},
			},
		},
	}, reasons)

	// Check that if some top-level reason changes, it is still returned even though
	// the pod group is throttled (but the unschedulable pods are still present).
	nsu.MarkReasonsReported(reasons, now.Add(time.Second))
	noScaleUpInfo.SkippedNodeGroups = map[string]status.Reasons{
		"mig1": &mockFailureReasons{reasons: []string{"NewReason"}},
	}
	reasons = nsu.GetNewReasons(scaleUpStatus, napStatus, now.Add(10*time.Second))
	assert.Equal(t, &Reasons{
		SkippedMigs: []*vistypes.MigExplanation{
			{Mig: mig1, Reason: vistypes.NewNoScaleUpMigSkippedMsg([]string{"NewReason"})},
		},
	}, reasons)

	// Check that if there are no unschedulable pods, no reasons are returned even if there is
	// some top-level reason present.
	scaleUpStatus.NoScaleUpInfos = []*vistypes.NoScaleUpInfo{}
	reasons = nsu.GetNewReasons(scaleUpStatus, napStatus, now.Add(time.Hour))
	assert.True(t, reasons.IsEmpty())
}

func TestTopLevelReason(t *testing.T) {
	for _, testCase := range []struct {
		scaleUpResult  status.ScaleUpResult
		expectedReason *vistypes.Message
	}{
		{status.ScaleUpError, vistypes.NewNoScaleUpUnexpectedErrorMsg()},
		{status.ScaleUpNotTried, vistypes.NewNoScaleUpNotTriedMsg()},
		{status.ScaleUpInCooldown, vistypes.NewNoScaleUpInBackoffMsg()},
		{status.ScaleUpNoOptionsAvailable, nil},
		{status.ScaleUpNotNeeded, nil},
		{status.ScaleUpSuccessful, nil},
	} {
		noScaleUp := throttledNoScaleUp{}
		assert.Equal(t, testCase.expectedReason, noScaleUp.computeTopLevel(testCase.scaleUpResult))
	}
}

func TestTopLevelNapReason(t *testing.T) {
	for _, testCase := range []struct {
		napProcessingResult autoprovisioning.ProcessingResult
		expectedReason      *vistypes.Message
	}{
		{autoprovisioning.ProcessingOk, nil},
		{autoprovisioning.NapDisabled, vistypes.NewNoScaleUpNapDisabledMsg()},
		{autoprovisioning.ResourceLimiterNotAvailable, vistypes.NewNoScaleUpNapUnexpectedErrorMsg()},
		{autoprovisioning.MaxAutoprovisionedNodeGroupsLimitReached, vistypes.NewNoScaleUpNapNodeGroupsLimitReachedMsg()},
		{autoprovisioning.NoAutoprovisioningLocationsAvailable, vistypes.NewNoScaleUpNapNoLocationsAvailableMsg()},
	} {
		noScaleUp := throttledNoScaleUp{}
		assert.Equal(t, testCase.expectedReason, noScaleUp.computeTopLevelNap(&vistypes.NapStatus{Result: testCase.napProcessingResult}))
	}
}

func TestSkippedMigReasons(t *testing.T) {
	mig1 := &vistypes.GkeMig{Id: "mig1", Name: "mig1", NodePoolName: "np1", Zone: "z1", Exists: true}
	mig2 := &vistypes.GkeMig{Id: "mig2", Name: "mig2", NodePoolName: "np2", Zone: "z1", Exists: true}
	mig3 := &vistypes.GkeMig{Id: "mig3", Name: "mig3", NodePoolName: "np3", Zone: "z1", Exists: false}
	consideredMigs := []*vistypes.GkeMig{mig1, mig2, mig3}

	noScaleUpInfo := &vistypes.NoScaleUpInfo{
		Pod:                &vistypes.Pod{Name: "pod1"},
		RejectedNodeGroups: nil,
		SkippedNodeGroups: map[string]status.Reasons{
			"mig1": &mockFailureReasons{reasons: []string{"Reason1", "Reason2"}},
			"mig2": &mockFailureReasons{reasons: []string{"Reason3"}},
			"mig3": &mockFailureReasons{reasons: []string{"Shouldn't be emitted because it doesn't exist."}},
			"mig4": &mockFailureReasons{reasons: []string{"Shouldn't work, missing MIG."}},
		},
	}

	noScaleUp := throttledNoScaleUp{}
	skippedReasons := noScaleUp.computeSkippedMigs(&vistypes.ScaleUpStatus{NoScaleUpInfos: []*vistypes.NoScaleUpInfo{noScaleUpInfo}, ConsideredMigs: consideredMigs})
	assert.ElementsMatch(t, []*vistypes.MigExplanation{
		{Mig: mig1, Reason: vistypes.NewNoScaleUpMigSkippedMsg([]string{"Reason1", "Reason2"})},
		{Mig: mig2, Reason: vistypes.NewNoScaleUpMigSkippedMsg([]string{"Reason3"})},
	}, skippedReasons)

	skippedReasons = noScaleUp.computeSkippedMigs(&vistypes.ScaleUpStatus{NoScaleUpInfos: []*vistypes.NoScaleUpInfo{}, ConsideredMigs: consideredMigs})
	assert.Nil(t, skippedReasons)
}

func TestPodGroupReasons(t *testing.T) {
	migsById := map[string]*vistypes.GkeMig{
		"mig1": {Id: "mig1", Name: "mig1", NodePoolName: "np1", Zone: "z1", Exists: true},
		"mig2": {Id: "mig2", Name: "mig2", NodePoolName: "np2", Zone: "z1", Exists: true},
		"mig3": {Id: "mig3", Name: "mig3", NodePoolName: "np3", Zone: "z1", Exists: true},
		"mig4": {Id: "mig4", Name: "mig4", NodePoolName: "np4", Zone: "z1", Exists: false},
	}
	apiPod := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}}
	pod := &vistypes.Pod{Name: "pod1"}

	noScaleUpInfo := &vistypes.NoScaleUpInfo{
		Pod: pod,
		RejectedNodeGroups: map[string]status.Reasons{
			"mig1": clustersnapshot.NewFailingPredicateError(apiPod, "Predicate1", []string{"Reason1", "Reason2"}, "", ""),
			"mig2": clustersnapshot.NewFailingPredicateError(apiPod, "Predicate2", []string{}, "", ""),
			"mig3": &mockFailureReasons{[]string{"This should default to unknown reason, wrong Reasons type."}},
			"mig4": clustersnapshot.NewFailingPredicateError(apiPod, "ThisShouldntBeEmittedDoesntExist", []string{}, "", ""),
			"mig5": clustersnapshot.NewFailingPredicateError(apiPod, "ThisShouldntWorkMissingMig", []string{}, "", ""),
		},
		SkippedNodeGroups: nil,
	}

	noScaleUp := throttledNoScaleUp{}
	migReasons := noScaleUp.computePodLevelMigReasons(noScaleUpInfo, migsById)
	assert.Equal(t, map[string]*vistypes.MigExplanation{
		"mig1": {Mig: migsById["mig1"], Reason: vistypes.NewNoScaleUpMigFailingPredicateMsg("Predicate1", []string{"Reason1", "Reason2"})},
		"mig2": {Mig: migsById["mig2"], Reason: vistypes.NewNoScaleUpMigFailingPredicateMsg("Predicate2", []string{})},
		"mig3": {Mig: migsById["mig3"], Reason: vistypes.NewNoScaleUpMigUnknownReasonMsg()},
	}, migReasons)
}

func TestPodLevelNapReasons(t *testing.T) {
	for tn, tc := range map[string]struct {
		pod                            *apiv1.Pod
		autoprovisioningLocations      []string
		unavailableMachineTypesByZone  map[string]map[string]bool
		machineTypesWithInternalErrors map[string]bool
		expectedReasons                []*vistypes.Message
	}{
		// Single message, easy trigger scenarios.
		"explicit NAP pod error -> appropriate global message": {
			pod: buildPodWithNodeSelector("p1", 5000, 5000000000,
				map[string]string{gkelabels.MachineFamilyLabel: "n2", gkelabels.RequestedMinCpuPlatformLabel: "AMD_Milan"}),
			autoprovisioningLocations: []string{"ss-moon-1", "ss-moon-2"},
			expectedReasons: []*vistypes.Message{
				vistypes.NewNoScaleUpNapPodMinCpuPlatformInvalidMsg(`machine family "n2"`, "AMD Milan"),
			},
		},
		"single zone, only resource constraints -> resource exceeded zonal message": {
			pod:                       test.BuildTestPod("p1", 10000, 10000000000),
			autoprovisioningLocations: []string{"ss-moon-1"},
			expectedReasons: []*vistypes.Message{
				vistypes.NewNoScaleUpNapPodZonalResourcesExceededMsg("ss-moon-1"),
			},
		},
		"single zone, non-resource-related predicate failing -> failing predicate zonal message": {
			pod:                       buildPodWithNodeSelector("p1", 5000, 5000000000, map[string]string{gkelabels.GkeNodePoolLabel: "some-name"}),
			autoprovisioningLocations: []string{"ss-moon-1"},
			expectedReasons: []*vistypes.Message{
				vistypes.NewNoScaleUpNapPodZonalFailingPredicatesMsg("ss-moon-1", []string{"node(s) didn't match Pod's node affinity/selector"}),
			},
		},
		"single zone, internal errors -> unexpected error message": {
			pod:                       test.BuildTestPod("p1", 1, 5000),
			autoprovisioningLocations: []string{"ss-moon-1"},
			machineTypesWithInternalErrors: map[string]bool{
				machineTypes[0]: true,
				machineTypes[1]: true,
				machineTypes[2]: true,
				machineTypes[3]: true,
			},
			expectedReasons: []*vistypes.Message{
				vistypes.NewNoScaleUpNapPodZonalUnexpectedErrorMsg("ss-moon-1"),
			},
		},
		"single zone, unable to build any node group -> illegal config message": {
			pod:                       test.BuildTestPod("p1", 1, 1000),
			autoprovisioningLocations: []string{"ss-moon-1"},
			unavailableMachineTypesByZone: map[string]map[string]bool{
				"ss-moon-1": {
					machineTypes[0]: true,
					machineTypes[1]: true,
					machineTypes[2]: true,
					machineTypes[3]: true,
				},
			},
			expectedReasons: []*vistypes.Message{
				vistypes.NewNoScaleUpNapPodZonalIllegalConfigMsg("ss-moon-1"),
			},
		},
		// Failing predicate message emitted with other messages + less obvious trigger scenarios.
		"failing predicate and unexpected error messages can be emitted together": {
			pod:                            buildPodWithNodeSelector("p1", 5000, 5000000000, map[string]string{gkelabels.GkeNodePoolLabel: "some-name"}),
			autoprovisioningLocations:      []string{"ss-moon-1"},
			machineTypesWithInternalErrors: map[string]bool{machineTypes[0]: true},
			expectedReasons: []*vistypes.Message{
				vistypes.NewNoScaleUpNapPodZonalFailingPredicatesMsg("ss-moon-1", []string{"node(s) didn't match Pod's node affinity/selector"}),
				vistypes.NewNoScaleUpNapPodZonalUnexpectedErrorMsg("ss-moon-1"),
			},
		},
		"failing predicate zonal message is produced even if some node groups can't be built": {
			pod:                       buildPodWithNodeSelector("p1", 5000, 5000000000, map[string]string{gkelabels.GkeNodePoolLabel: "some-name"}),
			autoprovisioningLocations: []string{"ss-moon-1"},
			unavailableMachineTypesByZone: map[string]map[string]bool{
				"ss-moon-1": {machineTypes[0]: true},
			},
			expectedReasons: []*vistypes.Message{
				vistypes.NewNoScaleUpNapPodZonalFailingPredicatesMsg("ss-moon-1", []string{"node(s) didn't match Pod's node affinity/selector"}),
			},
		},
		// Resources exceeded message emitted with other messages + less obvious trigger scenarios.
		"resources exceeded and unexpected error messages can be emitted together": {
			pod:                            test.BuildTestPod("p1", 10000, 10000000000),
			autoprovisioningLocations:      []string{"ss-moon-1"},
			machineTypesWithInternalErrors: map[string]bool{machineTypes[0]: true},
			expectedReasons: []*vistypes.Message{
				vistypes.NewNoScaleUpNapPodZonalResourcesExceededMsg("ss-moon-1"),
				vistypes.NewNoScaleUpNapPodZonalUnexpectedErrorMsg("ss-moon-1"),
			},
		},
		"resource exceeded zonal message is produced even if some node groups can't be built": {
			pod:                       test.BuildTestPod("p1", 10000, 10000000000),
			autoprovisioningLocations: []string{"ss-moon-1"},
			unavailableMachineTypesByZone: map[string]map[string]bool{
				"ss-moon-1": {machineTypes[0]: true},
			},
			expectedReasons: []*vistypes.Message{
				vistypes.NewNoScaleUpNapPodZonalResourcesExceededMsg("ss-moon-1"),
			},
		},
		// Multiple zones.
		"multiple zones, same issue in every zone": {
			pod:                       buildPodWithNodeSelector("p1", 5000, 5000000000, map[string]string{gkelabels.GkeNodePoolLabel: "some-name"}),
			autoprovisioningLocations: []string{"ss-moon-1", "ss-moon-2"},
			expectedReasons: []*vistypes.Message{
				vistypes.NewNoScaleUpNapPodZonalFailingPredicatesMsg("ss-moon-1", []string{"node(s) didn't match Pod's node affinity/selector"}),
				vistypes.NewNoScaleUpNapPodZonalFailingPredicatesMsg("ss-moon-2", []string{"node(s) didn't match Pod's node affinity/selector"}),
			},
		},
		"multiple zones, different issues in different zones": {
			pod:                       buildPodWithNodeSelector("p1", 5000, 5000000000, map[string]string{apiv1.LabelZoneFailureDomain: "ss-moon-1"}),
			autoprovisioningLocations: []string{"ss-moon-1", "ss-moon-2"},
			unavailableMachineTypesByZone: map[string]map[string]bool{
				"ss-moon-1": {
					machineTypes[0]: true,
					machineTypes[1]: true,
					machineTypes[2]: true,
					machineTypes[3]: true,
				},
			},
			expectedReasons: []*vistypes.Message{
				vistypes.NewNoScaleUpNapPodZonalIllegalConfigMsg("ss-moon-1"),
				vistypes.NewNoScaleUpNapPodZonalFailingPredicatesMsg("ss-moon-2", []string{"node(s) didn't match Pod's node affinity/selector"}),
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			scaleUpStatus, ctx, err := performNapScaleUp(t, tc.pod, tc.autoprovisioningLocations, tc.unavailableMachineTypesByZone, tc.machineTypesWithInternalErrors)
			assert.NoError(t, err)
			vizStatus, err := vistypes.ConvertScaleUpStatus(scaleUpStatus)
			assert.NoError(t, err)
			napStatus := vistypes.GetNapStatus(ctx)

			noScaleUp := NewNoScaleUp(time.Minute)
			reasons := noScaleUp.GetNewReasons(vizStatus, napStatus, time.Now())
			assert.Equal(t, 1, len(reasons.PodGroups))
			assert.ElementsMatch(t, tc.expectedReasons, reasons.PodGroups[0].NapReasons)
		})
	}
}

func TestNapPodErrToReasons(t *testing.T) {
	for tn, tc := range map[string]struct {
		napErr          errors.AutoscalerError
		expectedReasons []*vistypes.Message
	}{
		"invalid workload separation error": {
			napErr:          podrequirements.NewInvalidWorkloadSeparationError("key"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodWorkloadSeparationInvalidMsg("key")},
		},
		"private node affinity in a cluster without PSC infrastructure": {
			napErr:          autoprovisioning.ErrNoPSCInfrastructure,
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodNoPSCInfrastructureMsg()},
		},
		"invalid value for label": {
			napErr:          podrequirements.NewInvalidLabelValueError("example-label", "example-value"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodInvalidLabelValueMsg("example-label", "example-value")},
		},
		"machine family unknown error": {
			napErr:          machineselection.NewMachineFamilyUnknownError("x7"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodMachineFamilyUnknownMsg("x7")},
		},
		"machine family not supported error": {
			napErr:          machineselection.NewMachineFamilyNotSupportedError("x7"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodMachineFamilyNotSupportedMsg("x7")},
		},
		"compute class non Autopilot error": {
			napErr:          machineselection.NewComputeClassNonAutopilotError("x7"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodComputeClassNonAutopilotMsg("x7")},
		},
		"arch specified without required compute class in Autopilot error": {
			napErr:          machineselection.NewAutopilotArchNoComputeClassError("x7"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodAutopilotArchNoComputeClassMsg("x7")},
		},
		"arch unknown error": {
			napErr:          machineselection.NewSystemArchitectureUnknownError("x7"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodArchUnknownMsg("x7")},
		},
		"arch and machine group not compatible error": {
			napErr:          machineselection.NewSystemArchitectureIncompatibleError("x7", "arm42"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodArchInvalidMsg("x7", "arm42")},
		},
		"compute class with machine family error": {
			napErr:          machineselection.NewComputeClassWithMachineFamilyError("x7"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodComputeClassWithMachineFamilyMsg("x7")},
		},
		"compute class without machine family error": {
			napErr:          machineselection.NewComputeClassWithoutMachineFamilyError("Performance"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodComputeClassWithoutMachineFamilyMsg("Performance")},
		},
		"compute class with invalid machine family error": {
			napErr:          machineselection.NewComputeClassWithInvalidMachineFamilyError("Performance", "x7"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodComputeClassWithInvalidMachineFamilyMsg("Performance", "x7")},
		},
		"compute class without accelerator error": {
			napErr:          machineselection.NewComputeClassWithoutAcceleratorError("Accelerator"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodComputeClassWithoutAcceleratorMsg("Accelerator")},
		},
		"confidential nodes not supported error": {
			napErr:          machineselection.NewConfidentialNodesIncompatibleError("x7"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodConfidentialNodesIncompatibleMsg("x7")},
		},
		"GPU and family not compatible error": {
			napErr:          machineselection.NewGpuIncompatibleError("x7", "nvidia-eth-1000"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodGpuIncompatibleMsg("x7", "nvidia-eth-1000")},
		},
		"GPU and min_cpu_platform not compatible error": {
			napErr:          machineselection.NewGpuMinCpuPlatformIncompatibleError("Intel Inside", "nvidia-eth-1000"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodGpuMinCpuPlatformIncompatibleMsg("Intel Inside", "nvidia-eth-1000")},
		},
		"min_cpu_platform invalid error": {
			napErr:          machineselection.NewMinCpuPlatformInvalidError("x7", "Intel Inside"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodMinCpuPlatformInvalidMsg("x7", "Intel Inside")},
		},
		"min_cpu_platform unknown error": {
			napErr:          machineselection.NewMinCpuPlatformUnknownError("Intel Outside"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodMinCpuPlatformUnknownMsg("Intel Outside")},
		},
		"multiple min_cpu_platform values error": {
			napErr:          machineselection.NewMultipleMinCpuPlatformsError(),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodMultipleMinCpuPlatformsMsg()},
		},
		"machine config invalid error": {
			napErr:          machineselection.NewMachineConfigInvalidError("some config", "some error"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodMachineConfigInvalidMsg("some config", "some error")},
		},
		"machine with DWS disabled requested": {
			napErr:          autoprovisioning.NewInvalidDwsMachineFamilyError([]string{"some-machine-family"}),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodMachineFamiliesDoNotSupportDws([]string{"some-machine-family"})},
		},
		"GPU limit not defined error": {
			napErr:          autoprovisioning.NewGpuTypeNoLimitDefinedError("nvidia-eth-1000"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodGpuNoLimitDefinedMsg("nvidia-eth-1000")},
		},
		"GPU not supported error": {
			napErr:          autoprovisioning.NewGpuTypeNotSupportedError("nvidia-eth-1000"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodGpuTypeNotSupportedMsg("nvidia-eth-1000")},
		},
		"GPU request invalid error": {
			napErr:          autoprovisioning.NewGpuRequestInvalidError("some reason"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodGpuRequestInvalidMsg("some reason")},
		},
		"GPU request invalid (GPU type is not specified) error": {
			napErr:          autoprovisioning.NewGpuRequestInvalidError("GPU type is not specified"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodGpuRequestInvalidMsg("GPU type is not specified")},
		},
		"GPU failing predicates error": {
			napErr:          autoprovisioning.NewGpuRequestFailingPredicatesError([]string{"reason1", "reason2"}),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodGpuFailingPredicatesMsg([]string{"reason1", "reason2"})},
		},
		"TPU and machine family incompatible error": {
			napErr:          machineselection.NewTpuIncompatibleError("x7", "tpu-v42"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodTpuIncompatibleMsg("x7", "tpu-v42")},
		},
		"TPU not supported error": {
			napErr:          autoprovisioning.NewTpuTypeNotSupportedError("tpu-v42"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodTpuTypeNotSupportedMsg("tpu-v42")},
		},
		"TPU limit not defined error": {
			napErr:          autoprovisioning.NewTpuTypeNoLimitDefinedError("tpu-v42"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodTpuNoLimitDefinedMsg("tpu-v42")},
		},
		"TPU invald accelerator count error:": {
			napErr:          autoprovisioning.NewTpuTypeInvalidAcceleratorCount("tpu-v42", 111),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodTpuAcceleratorCountInvalid("tpu-v42", 111)},
		},
		"Incorrect extended duration pod cpu req error": {
			napErr:          autoprovisioning.NewInvalidExtendedDurationPodCPUReq("ab"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapExtendedDurationPodCPUReqInvalid("ab")},
		},
		"Extended duration pod defined outside of autopilot error": {
			napErr:          autoprovisioning.NewExtendedDurationPodNonAutopilotError(),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapExtendedDurationPodNonAutopilotError()},
		},
		"Incorrect isolated pod cpu req error": {
			napErr:          autoprovisioning.NewInvalidIsolatedPodCPUReq("ab"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapIsolatedPodCPUReqInvalid("ab")},
		},
		"Incorrect isolated pod capacity value error": {
			napErr:          autoprovisioning.NewIsolatedPodCapacityError("ab"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapIsolatedPodCapacityInvalid("ab")},
		},
		"Isolated pod defined outside of autopilot error": {
			napErr:          autoprovisioning.NewIsolatedPodNonAutopilotError(),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapIsolatedPodNonAutopilotError()},
		},
		"CCC not found error without reason": {
			napErr:          autoprovisioning.NewComputeClassNotFoundError("ccc", "CCC", nil),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapCccNotFound("ccc", "")},
		},
		"CCC not found error with reason": {
			napErr:          autoprovisioning.NewComputeClassNotFoundError("ccc", "CCC", stderrors.New("reason1")),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapCccNotFound("ccc", "reason1")},
		},
		"CCC fetching error": {
			napErr:          autoprovisioning.NewComputeClassFetchingError("ccc", "CCC"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapCccFetchingErr("ccc")},
		},
		"CCC autoprovisioning disabled": {
			napErr:          autoprovisioning.NewComputeClassAutoprovisioningDisabled("ccc", "CCC"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapCccAutoprovisioningDisabled("ccc")},
		},
		"CCC pod incompatible error": {
			napErr:          autoprovisioning.NewComputeClassPodIncompatibleError("ccc", "CCC"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapCccPodIncompatible("ccc")},
		},
		"CCC and NPC both defined error": {
			napErr:          autoprovisioning.NewComputeClassPodMultipleDefinitionsError(),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapNpcCccBothDefined()},
		},
		"Flex Start misconfigured error": {
			napErr:          autoprovisioning.NewFlexStartMisconfiguredError("misconfig"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapFlexStartMisconfiguredError("misconfig")},
		},
		"unexpected error type": {
			napErr:          errors.NewAutoscalerError(errors.CloudProviderError, "some error"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodUnexpectedErrorMsg()},
		},
		"unexpected machineselection.Error.ErrType": {
			napErr:          &machineselection.Error{ErrType: errors.CloudProviderError},
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodUnexpectedErrorMsg()},
		},
		"InternalError": {
			napErr:          errors.NewAutoscalerErrorf(errors.InternalError, "Some %s", "message"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodUnexpectedErrorMsg()},
		},
		"Unusable reservation error": {
			napErr: reservations.NewErrUnusableReservation(gceclient.ReservationRef{
				Name:         "res-name",
				Project:      "some-project",
				BlockName:    "block-name",
				SubBlockName: "sub-block-name",
			}, "this reservation is unusable for some reason"),
			expectedReasons: []*vistypes.Message{vistypes.NewNoScaleUpNapPodUnusableReservation(
				"reservation 'res-name' in project 'some-project' with block 'block-name' with sub-block 'sub-block-name' is unusable, this reservation is unusable for some reason",
				"projects/some-project/reservations/res-name/reservationBlocks/block-name/reservationSubBlocks/sub-block-name",
			)},
		},
		"missing placement policy": {
			napErr: placement.NewInvalidPlacementPolicy("policy", "policy does not exist"),
			expectedReasons: []*vistypes.Message{
				vistypes.NewNoScaleUpNapPodInvalidPlacementPolicy("policy does not exist", "policy"),
			},
		},
		"missing ai zones": {
			napErr: zonetypes.NewErrNoAIZones(),
			expectedReasons: []*vistypes.Message{
				vistypes.NewNoScaleUpNapPodMissingAIZones("zoneTypes: no AI zones were found in the cluster region"),
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			reasons := napPodErrToReasons(&vistypes.Pod{Name: "pod", Namespace: "ns"}, tc.napErr)
			assert.Equal(t, tc.expectedReasons, reasons)
		})
	}
}

func performNapScaleUp(t *testing.T, unschedulablePod *apiv1.Pod, autoprovisioningLocations []string, unavailableMachineTypesByZone map[string]map[string]bool, machineTypesWithInternalErrors map[string]bool) (*status.ScaleUpStatus, *context.AutoscalingContext, error) {
	provider := newVizNapTestCloudProvider(autoprovisioningLocations, unavailableMachineTypesByZone, machineTypesWithInternalErrors)
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(false)
	processor.SetContext(&context.AutoscalingContext{CloudProvider: provider, DebuggingSnapshotter: debuggingSnapshotter})
	opts := config.AutoscalingOptions{
		EstimatorName: estimator.BinpackingEstimatorName,
	}
	dsLister, err := kube_util.NewTestDaemonSetLister([]*appsv1.DaemonSet{})
	assert.NoError(t, err)
	listers := kube_util.NewListerRegistry(nil, nil, kube_util.NewTestPodLister([]*apiv1.Pod{}), nil, dsLister, nil, nil, nil, nil)
	templateNodeInfoProvider := nodeinfosprovider.NewDefaultTemplateNodeInfoProvider(nil, false)
	templateNodeInfoRegistry := nodeinfosprovider.NewTemplateNodeInfoRegistry(templateNodeInfoProvider)
	ctx, err := core_test.NewScaleTestAutoscalingContext(opts, &fake.Clientset{}, listers, provider, callbacks.NewTestProcessorCallbacks(), debuggingSnapshotter, templateNodeInfoRegistry)
	assert.NoError(t, err)

	proc := processors.DefaultProcessors(opts)
	npcLister := npc_lister.NewMockCrdLister([]npc_crd.CRD{})
	em := experiments.NewMockManager()
	options := autoprovisioning.AutoprovisioningNodeGroupManagerOptions{
		CloudProvider:                    provider,
		Backoff:                          backoff.NewGkeBackoff(backoff.Config{CustomResourceProcessor: processor, NpcLister: npcLister}),
		MaxAutoprovisionedNodeGroupCount: 1000,
		Lister:                           npcLister,
		ExperimentsManager:               em,
		ResourcePolicyPuller:             &placement.FakeResourcePolicyPullerProvider{},
		OptionsTracker:                   tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
	}
	proc.NodeGroupListProcessor = autoprovisioning.NewAutoprovisioningNodeGroupManager(options)

	clusterState := clusterstate.NewClusterStateRegistry(provider, ctx.LogRecorder, backoff.NewGkeBackoff(backoff.Config{CustomResourceProcessor: processor}), proc.NodeGroupConfigProcessor, nil, clusterstate.WithAsyncNodeGroupStateChecker(proc.AsyncNodeGroupStateChecker), clusterstate.WithScaleStateNotifier(proc.ScaleStateNotifier))

	estimatorBuilder, err := estimator.NewEstimatorBuilder(
		estimator.BinpackingEstimatorName,
		estimator.NewThresholdBasedEstimationLimiter(nil),
		estimator.NewDecreasingPodOrderer(),
		nil, false)
	assert.NoError(t, err)

	quotasProvider := resourcequotas.NewCloudQuotasProvider(provider)
	quotasTrackerFactory := resourcequotas.NewTrackerFactory(resourcequotas.TrackerOptions{
		QuotaProvider:            quotasProvider,
		CustomResourcesProcessor: processor,
	})

	scaleUpOrchestrator := orchestrator.New()
	scaleUpOrchestrator.Initialize(&ctx, proc, clusterState, estimatorBuilder, taints.TaintConfig{}, quotasTrackerFactory)
	scaleUpStatus, err := scaleUpOrchestrator.ScaleUp([]*apiv1.Pod{unschedulablePod}, []*apiv1.Node{}, []*appsv1.DaemonSet{}, map[string]*framework.NodeInfo{}, false)
	return scaleUpStatus, &ctx, err
}

func buildPodWithNodeSelector(name string, millicpu, mem int64, nodeSelector map[string]string) *apiv1.Pod {
	pod := test.BuildTestPod(name, millicpu, mem)
	pod.Spec.NodeSelector = nodeSelector
	return pod
}
