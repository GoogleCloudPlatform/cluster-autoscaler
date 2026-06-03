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
	"errors"
	"reflect"
	"sort"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	caerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/machineselection"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/zonetypes"
)

// NoScaleUp describes an interface used to obtain information about the reasons behind a lack of scale-up.
type NoScaleUp interface {
	GetNewReasons(scaleUpStatus *vistypes.ScaleUpStatus, napStatus *vistypes.NapStatus, now time.Time) *Reasons
	MarkReasonsReported(reasons *Reasons, reportTime time.Time)
}

type throttledNoScaleUp struct {
	recentlyReportedReasons *reasonsReportTracker
}

// MarkReasonsReported is used to indicate that the specified reasons were reported to the user.
func (ns *throttledNoScaleUp) MarkReasonsReported(reasons *Reasons, reportTime time.Time) {
	ns.recentlyReportedReasons.markReported(reasons, reportTime)
}

// GetNewReasons returns reasons that should be reported to the user in the current autoscaling iteration.
func (ns *throttledNoScaleUp) GetNewReasons(scaleUpStatus *vistypes.ScaleUpStatus, napStatus *vistypes.NapStatus, now time.Time) *Reasons {
	// Garbage collect old entries.
	ns.recentlyReportedReasons.removeOld(now)

	if len(scaleUpStatus.NoScaleUpInfos) == 0 {
		// If there are no unschedulable pods, don't provide any reasons, they won't make sense anyway.
		return &Reasons{}
	}

	allCurrentReasons := &Reasons{
		TopLevel:    ns.computeTopLevel(scaleUpStatus.Result),
		TopLevelNap: ns.computeTopLevelNap(napStatus),
		SkippedMigs: ns.computeSkippedMigs(scaleUpStatus),
		PodGroups:   ns.computePodGroups(scaleUpStatus, napStatus),
	}
	return ns.recentlyReportedReasons.filterOutAlreadyTrackedReasons(allCurrentReasons, now)
}

func (ns *throttledNoScaleUp) computeTopLevel(result status.ScaleUpResult) *vistypes.Message {
	if result == status.ScaleUpError {
		return vistypes.NewNoScaleUpUnexpectedErrorMsg()
	} else if result == status.ScaleUpNotTried {
		return vistypes.NewNoScaleUpNotTriedMsg()
	} else if result == status.ScaleUpInCooldown {
		return vistypes.NewNoScaleUpInBackoffMsg()
	}
	return nil
}

func (ns *throttledNoScaleUp) computeTopLevelNap(napStatus *vistypes.NapStatus) *vistypes.Message {
	if napStatus.Result == autoprovisioning.ProcessingOk {
		return nil
	} else if napStatus.Result == autoprovisioning.NapDisabled {
		return vistypes.NewNoScaleUpNapDisabledMsg()
	} else if napStatus.Result == autoprovisioning.NoAutoprovisioningLocationsAvailable {
		return vistypes.NewNoScaleUpNapNoLocationsAvailableMsg()
	} else if napStatus.Result == autoprovisioning.MaxAutoprovisionedNodeGroupsLimitReached {
		return vistypes.NewNoScaleUpNapNodeGroupsLimitReachedMsg()
	} else {
		return vistypes.NewNoScaleUpNapUnexpectedErrorMsg()
	}
}

func (ns *throttledNoScaleUp) computeSkippedMigs(scaleUpStatus *vistypes.ScaleUpStatus) []*vistypes.MigExplanation {
	if len(scaleUpStatus.NoScaleUpInfos) == 0 {
		return nil
	}

	migsById := scaleUpStatus.GetMigsById()

	// The skipped migs should be the same for all pods.
	skippedMigs := scaleUpStatus.NoScaleUpInfos[0].SkippedNodeGroups

	var result []*vistypes.MigExplanation
	for migId, skippedMigReasons := range skippedMigs {
		mig, found := migsById[migId]
		if !found {
			klog.Errorf("CA Viz NoScaleUp: Mig %s marked as skipped in ScaleUpResult, but wasn't provided in ConsideredNodeGroups.", migId)
			continue
		}
		if !mig.Exists {
			continue
		}

		result = append(result, &vistypes.MigExplanation{
			Mig:    mig,
			Reason: vistypes.NewNoScaleUpMigSkippedMsg(skippedMigReasons.Reasons()),
		})
	}

	return result
}

func (ns *throttledNoScaleUp) computePodGroups(scaleUpStatus *vistypes.ScaleUpStatus, napStatus *vistypes.NapStatus) []*vistypes.PodGroupExplanation {
	migsById := scaleUpStatus.GetMigsById()
	pgm := NewPodGroupMap()
	for _, noScaleUpInfo := range scaleUpStatus.NoScaleUpInfos {
		migReasons := ns.computePodLevelMigReasons(noScaleUpInfo, migsById)
		napReasons := ns.computePodLevelNapReasons(noScaleUpInfo, napStatus, migsById)
		pgm.AddPod(noScaleUpInfo.Pod, migReasons, napReasons)
	}
	return pgm.GetPodGroups()
}

func (ns *throttledNoScaleUp) computePodLevelMigReasons(noScaleUpInfo *vistypes.NoScaleUpInfo, migsById map[string]*vistypes.GkeMig) map[string]*vistypes.MigExplanation {
	result := make(map[string]*vistypes.MigExplanation)

	for migId, reasons := range noScaleUpInfo.RejectedNodeGroups {
		mig, found := migsById[migId]
		if !found {
			klog.Errorf("CA Viz NoScaleUp: Mig %s marked as rejected in ScaleUpResult, but wasn't provided in ConsideredNodeGroups.", migId)
			continue
		}
		if !mig.Exists {
			continue
		}

		schedErr, ok := reasons.(clustersnapshot.SchedulingError)
		if !ok {
			klog.Errorf("CA Viz NoScaleUp: unexpected rejected MIG reason, got %s, want: something implementing clustersnapshot.SchedulingError.", reflect.TypeOf(reasons))
			result[mig.Id] = &vistypes.MigExplanation{Mig: mig, Reason: vistypes.NewNoScaleUpMigUnknownReasonMsg()}
			continue
		}
		if schedErr.Type() != clustersnapshot.FailingPredicateError {
			klog.Errorf("CA Viz NoScaleUp: unexpected SchedulingErrorType %v", schedErr.Type())
			result[mig.Id] = &vistypes.MigExplanation{Mig: mig, Reason: vistypes.NewNoScaleUpMigUnknownReasonMsg()}
			continue
		}

		result[mig.Id] = &vistypes.MigExplanation{Mig: mig, Reason: vistypes.NewNoScaleUpMigFailingPredicateMsg(schedErr.FailingPredicateName(), schedErr.FailingPredicateReasons())}
	}

	return result
}

func (ns *throttledNoScaleUp) computePodLevelNapReasons(noScaleUpInfo *vistypes.NoScaleUpInfo, napStatus *vistypes.NapStatus, migsById map[string]*vistypes.GkeMig) []*vistypes.Message {
	if napStatus.Result != autoprovisioning.ProcessingOk {
		return []*vistypes.Message{}
	}

	podStatus := napStatus.PodStatuses[noScaleUpInfo.Pod.Uid]
	if podStatus.Err != nil {
		return napPodErrToReasons(noScaleUpInfo.Pod, podStatus.Err)
	}
	if !podStatus.Picked {
		// The logic below tries to guess the reason based on CA scale-up logic. We know that NAP didn't inject any node groups
		// that would help this pod, so trying to guess based on the scale-up logic doesn't make sense. The status doesn't
		// indicate a pod-specific error, so NAP should be able to help eventually - just not this loop. We could have a transient
		// message in the meantime, but historically such messages have just introduced unnecessary noise.
		return []*vistypes.Message{}
	}

	// Filter out existing MIGs and group the skipped and rejected theoretical NAP MIGs by zone.
	skippedMigInfosByZone := extractNapMigsAndGroupByZone(noScaleUpInfo.SkippedNodeGroups, migsById)
	rejectedMigInfosByZone := extractNapMigsAndGroupByZone(noScaleUpInfo.RejectedNodeGroups, migsById)
	disregardedMigInfosByZone := groupDisregardedNapMigsByZone(napStatus.DisregardedMigs)

	// Other reasons, zone-by-zone.
	var reasons []*vistypes.Message
	zones := getAllSortedZonesFromReasonsByZone(skippedMigInfosByZone, rejectedMigInfosByZone, disregardedMigInfosByZone)
	for _, zone := range zones {
		skippedMigsDiv := divideSkippedMigs(skippedMigInfosByZone[zone])
		rejectedMigsDiv := divideRejectedMigs(rejectedMigInfosByZone[zone])
		disregardedMigsDiv := divideDisregardedMigs(disregardedMigInfosByZone[zone])

		initialReasonsLen := len(reasons)

		if skippedMigsDiv.hasInternalErrors() || disregardedMigsDiv.hasInternalErrors() {
			// This means that there were "shouldn't happen" errors for some migs.
			reasons = append(reasons, vistypes.NewNoScaleUpNapPodZonalUnexpectedErrorMsg(zone))
		}

		onlyResourceConstraintsInInjectedOrEmpty := skippedMigsDiv.resourceConstraintsOnlyOrEmpty() && rejectedMigsDiv.resourceConstraintsOnlyOrEmpty()
		resourceConstraintsPresent := skippedMigsDiv.hasResourceConstraints() || rejectedMigsDiv.hasResourceConstraints() || disregardedMigsDiv.hasResourceConstraints()
		if resourceConstraintsPresent && onlyResourceConstraintsInInjectedOrEmpty {
			// Only resource-related reasons in this zone, so report that. This might mean that the pod is too big for any machine, the resources it requests
			// are in resource-based backoff, or that the smallest machine which fits the pod would exceed cluster-wide limits. It's not really straightforward
			// how to distinguish between these cases and the message should be enough to point the user to check the pod's resource requests, quotas, etc..
			reasons = append(reasons, vistypes.NewNoScaleUpNapPodZonalResourcesExceededMsg(zone))
		}

		if skippedMigsDiv.empty() && rejectedMigsDiv.empty() && disregardedMigsDiv.unableToBuildNodeGroupOnly() {
			// NAP couldn't build any node groups for this zone - this probably means something really is wrong with pod's configuration,
			// e.g. requesting a GPU which isn't present in this zone.
			reasons = append(reasons, vistypes.NewNoScaleUpNapPodZonalIllegalConfigMsg(zone))
		}

		if rejectedMigsDiv.hasRemaining() {
			// There are NAP node groups which were rejected because of a predicate other than PodFitsResources - this means that something really is wrong with pod's
			// configuration, e.g. affinity-related stuff. Gather all the reasons and include them in the message.

			// Don't duplicate reasons.
			failureReasonsSet := make(map[string]bool)
			for _, info := range rejectedMigsDiv.remaining {
				for _, reason := range info.reasons.Reasons() {
					failureReasonsSet[reason] = true
				}
			}
			var failureReasons []string
			for reason := range failureReasonsSet {
				failureReasons = append(failureReasons, reason)
			}
			// Make sure that the reasons are sorted to make sure that the same predicates will yield the same visibility message,
			// for throttling purposes.
			sort.Strings(failureReasons)

			reasons = append(reasons, vistypes.NewNoScaleUpNapPodZonalFailingPredicatesMsg(zone, failureReasons))
		}

		if len(reasons) == initialReasonsLen {
			// The cases above don't cover all possible combinations of MIG reasons. If none match, just fall back to a generic message for this zone.
			reasons = append(reasons, vistypes.NewNoScaleUpNapPodZonalOtherErrorMsg(zone))
		}
	}

	return reasons
}

// NewNoScaleUp returns a new default instance implementing NoScaleUp.
func NewNoScaleUp(stalenessThreshold time.Duration) NoScaleUp {
	return &throttledNoScaleUp{
		recentlyReportedReasons: newReasonsReportTracker(stalenessThreshold),
	}
}

func napPodErrToReasons(pod *vistypes.Pod, napErr caerrors.AutoscalerError) []*vistypes.Message {
	invalidWorkloadSeparationErr := &podrequirements.ErrInvalidWorkloadSeparation{}
	if errors.As(napErr, &invalidWorkloadSeparationErr) {
		return []*vistypes.Message{vistypes.NewNoScaleUpNapPodWorkloadSeparationInvalidMsg(invalidWorkloadSeparationErr.Label)}
	}
	if errors.Is(napErr, autoprovisioning.ErrNoPSCInfrastructure) {
		return []*vistypes.Message{vistypes.NewNoScaleUpNapPodNoPSCInfrastructureMsg()}
	}
	invalidPlacementGroupNameErr := &placement.ErrInvalidPlacementGroupName{}
	if errors.As(napErr, &invalidPlacementGroupNameErr) {
		return []*vistypes.Message{vistypes.NewNoScaleUpNapPodInvalidPlacementGroupNameMsg(invalidPlacementGroupNameErr.PlacementGroup)}
	}
	invalidCompactPlacementMachineFamily := &placement.ErrInvalidMachineFamily{}
	if errors.As(napErr, &invalidCompactPlacementMachineFamily) {
		return []*vistypes.Message{vistypes.NewNoScaleUpNapPodInvalidCompactPlacementMachineFamilyMsg(invalidCompactPlacementMachineFamily.PlacementGroup, invalidCompactPlacementMachineFamily.Msg)}
	}
	compactPlacementNodeGroupAlreadyExists := &placement.ErrNodeGroupAlreadyExists{}
	if errors.As(napErr, &compactPlacementNodeGroupAlreadyExists) {
		return []*vistypes.Message{vistypes.NewNoScaleUpNapPodCompactPlacementNodeGroupAlreadyExistsMsg(compactPlacementNodeGroupAlreadyExists.NodeGroup)}
	}
	invalidLabelValueErr := &podrequirements.ErrInvalidLabelValue{}
	if errors.As(napErr, &invalidLabelValueErr) {
		return []*vistypes.Message{vistypes.NewNoScaleUpNapPodInvalidLabelValueMsg(invalidLabelValueErr.Label, invalidLabelValueErr.Value)}
	}
	selectionErr := &machineselection.Error{}
	if errors.As(napErr, &selectionErr) {
		switch selectionErr.ErrType {
		case machineselection.MachineFamilyUnknownError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodMachineFamilyUnknownMsg(selectionErr.MachineGroupName)}
		case machineselection.MachineFamilyNotSupportedError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodMachineFamilyNotSupportedMsg(selectionErr.MachineGroupName)}
		case machineselection.ComputeClassNonAutopilotError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodComputeClassNonAutopilotMsg(selectionErr.ComputeClassName)}
		case machineselection.ComputeClassWithMachineFamilyError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodComputeClassWithMachineFamilyMsg(selectionErr.ComputeClassName)}
		case machineselection.ComputeClassWithInvalidMachineFamilyError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodComputeClassWithInvalidMachineFamilyMsg(selectionErr.ComputeClassName, selectionErr.MachineGroupName)}
		case machineselection.ComputeClassWithoutMachineFamilyError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodComputeClassWithoutMachineFamilyMsg(selectionErr.ComputeClassName)}
		case machineselection.ComputeClassWithoutAcceleratorError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodComputeClassWithoutAcceleratorMsg(selectionErr.ComputeClassName)}
		case machineselection.ConfidentialNodesIncompatibleError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodConfidentialNodesIncompatibleMsg(selectionErr.MachineGroupName)}
		case machineselection.GpuIncompatibleError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodGpuIncompatibleMsg(selectionErr.MachineGroupName, selectionErr.GpuName)}
		case machineselection.GpuMinCpuPlatformIncompatibleError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodGpuMinCpuPlatformIncompatibleMsg(selectionErr.MinCpuPlatformName, selectionErr.GpuName)}
		case machineselection.TpuIncompatibleError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodTpuIncompatibleMsg(selectionErr.MachineGroupName, selectionErr.TpuName)}
		case machineselection.MinCpuPlatformInvalidError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodMinCpuPlatformInvalidMsg(selectionErr.MachineGroupName, selectionErr.MinCpuPlatformName)}
		case machineselection.MinCpuPlatformUnknownError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodMinCpuPlatformUnknownMsg(selectionErr.MinCpuPlatformName)}
		case machineselection.MultipleMinCpuPlatformsError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodMultipleMinCpuPlatformsMsg()}
		case machineselection.AutopilotArchNoComputeClassError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodAutopilotArchNoComputeClassMsg(selectionErr.SystemArch)}
		case machineselection.SystemArchitectureUnknownError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodArchUnknownMsg(selectionErr.SystemArch)}
		case machineselection.SystemArchitectureIncompatibleError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodArchInvalidMsg(selectionErr.MachineGroupName, selectionErr.SystemArch)}
		case machineselection.MachineConfigInvalidError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodMachineConfigInvalidMsg(selectionErr.MachineConfigDesc, selectionErr.MachineConfigErr)}
		default:
			klog.Errorf("NAP provided machineselection.Error with unexpected ErrType %q to CA Viz for pod %s/%s, err: %v", selectionErr.ErrType, pod.Namespace, pod.Name, selectionErr)
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodUnexpectedErrorMsg()}
		}
	}
	autoprovisioningErr := &autoprovisioning.Error{}
	if errors.As(napErr, &autoprovisioningErr) {
		switch autoprovisioningErr.ErrType {
		case autoprovisioning.GpuTypeNotSupportedError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodGpuTypeNotSupportedMsg(autoprovisioningErr.GpuType)}
		case autoprovisioning.GpuTypeNoLimitDefinedError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodGpuNoLimitDefinedMsg(autoprovisioningErr.GpuType)}
		case autoprovisioning.GpuRequestInvalidError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodGpuRequestInvalidMsg(autoprovisioningErr.GpuRequestInvalidReason)}
		case autoprovisioning.GpuFailingPredicatesError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodGpuFailingPredicatesMsg(autoprovisioningErr.GpuPredicateFailureReasons)}
		case autoprovisioning.TpuTypeNotSupportedError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodTpuTypeNotSupportedMsg(autoprovisioningErr.TpuType)}
		case autoprovisioning.TpuTypeNoLimitDefinedError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodTpuNoLimitDefinedMsg(autoprovisioningErr.TpuType)}
		case autoprovisioning.TpuTypeInvalidAcceleratorCount:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodTpuAcceleratorCountInvalid(autoprovisioningErr.TpuType, autoprovisioningErr.AcceleratorCount)}
		case autoprovisioning.InvalidExtendedDurationPodCPUReqError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapExtendedDurationPodCPUReqInvalid(autoprovisioningErr.ExtendedDurationCPUReq)}
		case autoprovisioning.ExtendedDurationPodNonAutopilotError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapExtendedDurationPodNonAutopilotError()}
		case autoprovisioning.ComputeClassFetchingError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapCccFetchingErr(autoprovisioningErr.ComputeClassName)}
		case autoprovisioning.ComputeClassNotFoundError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapCccNotFound(autoprovisioningErr.ComputeClassName, autoprovisioningErr.ComputeClassNotFoundReason)}
		case autoprovisioning.ComputeClassAutoprovisioningDisabled:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapCccAutoprovisioningDisabled(autoprovisioningErr.ComputeClassName)}
		case autoprovisioning.ComputeClassPodIncompatibleError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapCccPodIncompatible(autoprovisioningErr.ComputeClassName)}
		case autoprovisioning.ComputeClassPodMultipleDefinitionsError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapNpcCccBothDefined()}
		case autoprovisioning.InvalidIsolatedPodCPUReqError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapIsolatedPodCPUReqInvalid(autoprovisioningErr.IsolatedPodCPUReq)}
		case autoprovisioning.IsolatedPodNonAutopilotError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapIsolatedPodNonAutopilotError()}
		case autoprovisioning.IsolatedPodCapacityError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapIsolatedPodCapacityInvalid(autoprovisioningErr.IsolatedPodCapacity)}
		case autoprovisioning.FlexStartMisconfiguredError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapFlexStartMisconfiguredError(autoprovisioningErr.FlexStartMisconfiguredReason)}
		case autoprovisioning.InvalidConfidentialNodeTypeError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapInvalidConfidentialNodeType(autoprovisioningErr.ConfidentialNodeType)}
		case autoprovisioning.InvalidMachineFamilyForConfidentialNodeTypeError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapInvalidMachineFamilyForConfidentialNodeType(autoprovisioningErr.InvalidMachineFamilyForConfidentialNodeTypeReason)}
		case autoprovisioning.MachineFamiliesDoNotSupportDwsError:
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodMachineFamiliesDoNotSupportDws(autoprovisioningErr.MachineFamilies)}
		default:
			klog.Errorf("NAP provided autoprovisioning.Error with unexpected ErrType %q to CA Viz for pod %s/%s, err: %v", autoprovisioningErr.ErrType, pod.Namespace, pod.Name, autoprovisioningErr)
			return []*vistypes.Message{vistypes.NewNoScaleUpNapPodUnexpectedErrorMsg()}
		}
	}
	resErr := &reservations.ErrUnusableReservation{}
	if errors.As(napErr, &resErr) {
		return []*vistypes.Message{vistypes.NewNoScaleUpNapPodUnusableReservation(resErr.Error(), resErr.ReservationRef.Path())}
	}
	placementErr := &placement.ErrInvalidPlacementPolicy{}
	if errors.As(napErr, &placementErr) {
		return []*vistypes.Message{vistypes.NewNoScaleUpNapPodInvalidPlacementPolicy(placementErr.Msg, placementErr.PlacementPolicy)}
	}
	zoneTypesErr := &zonetypes.ErrNoAIZones{}
	if errors.As(napErr, &zoneTypesErr) {
		return []*vistypes.Message{vistypes.NewNoScaleUpNapPodMissingAIZones(zoneTypesErr.Msg)}
	}

	if napErr.Type() == caerrors.InternalError {
		// InternalError means that we don't want to expose the details of what went wrong - log the exact error here, but
		// only issue a generic message.
		klog.Errorf("Internal error while processing pod %s/%s by NAP: %s", pod.Namespace, pod.Name, napErr.Error())
		return []*vistypes.Message{vistypes.NewNoScaleUpNapPodUnexpectedErrorMsg()}
	}

	klog.Errorf("NAP provided an unexpected pod error type %q to CA Viz for pod %s/%s, err: %v", reflect.TypeOf(napErr), pod.Namespace, pod.Name, napErr)
	return []*vistypes.Message{vistypes.NewNoScaleUpNapPodUnexpectedErrorMsg()}
}
