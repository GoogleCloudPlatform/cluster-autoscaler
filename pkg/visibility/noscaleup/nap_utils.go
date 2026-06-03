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
	"sort"

	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/orchestrator"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
)

type migReasonsInfo struct {
	mig     *vistypes.GkeMig
	reasons status.Reasons
}

type skippedMigsDivision struct {
	all               []migReasonsInfo
	resourceViolating []migReasonsInfo
	notReady          []migReasonsInfo
}

func (div skippedMigsDivision) empty() bool {
	return len(div.all) == 0
}

func (div skippedMigsDivision) hasInternalErrors() bool {
	return len(div.notReady) > 0
}

func (div skippedMigsDivision) hasResourceConstraints() bool {
	return len(div.resourceViolating) > 0
}

func (div skippedMigsDivision) resourceConstraintsOnlyOrEmpty() bool {
	return len(div.all) == len(div.resourceViolating)
}

type rejectedMigsDivision struct {
	all        []migReasonsInfo
	notFitting []migReasonsInfo
	remaining  []migReasonsInfo
}

func (div rejectedMigsDivision) empty() bool {
	return len(div.all) == 0
}

func (div rejectedMigsDivision) hasRemaining() bool {
	return len(div.remaining) > 0
}

func (div rejectedMigsDivision) hasResourceConstraints() bool {
	return len(div.notFitting) > 0
}
func (div rejectedMigsDivision) resourceConstraintsOnlyOrEmpty() bool {
	return len(div.all) == len(div.notFitting)
}

type disregardedMigsDivision struct {
	all                    []vistypes.DisregardedMigInfo
	resourceBackedOff      []vistypes.DisregardedMigInfo
	standardBackedOff      []vistypes.DisregardedMigInfo
	unableToBuildNodeGroup []vistypes.DisregardedMigInfo
	internalError          []vistypes.DisregardedMigInfo
}

func (div disregardedMigsDivision) empty() bool {
	return len(div.all) == 0
}

func (div disregardedMigsDivision) hasInternalErrors() bool {
	return len(div.internalError) > 0
}

func (div disregardedMigsDivision) hasResourceConstraints() bool {
	return len(div.resourceBackedOff) > 0
}

func (div disregardedMigsDivision) unableToBuildNodeGroupOnly() bool {
	return len(div.all) == len(div.unableToBuildNodeGroup) && !div.empty()
}

func divideSkippedMigs(skippedMigInfos []migReasonsInfo) skippedMigsDivision {
	resourceViolating := make([]migReasonsInfo, 0)
	notReady := make([]migReasonsInfo, 0)

	for _, info := range skippedMigInfos {
		if reasonsContainMaxResourceLimitReached(info.reasons) {
			resourceViolating = append(resourceViolating, info)
		} else {
			notReady = append(notReady, info)
		}
	}

	return skippedMigsDivision{
		all:               skippedMigInfos,
		resourceViolating: resourceViolating,
		notReady:          notReady,
	}
}

func divideRejectedMigs(rejectedMigInfos []migReasonsInfo) rejectedMigsDivision {
	notFitting := make([]migReasonsInfo, 0)
	remaining := make([]migReasonsInfo, 0)

	for _, info := range rejectedMigInfos {
		schedErr, ok := info.reasons.(clustersnapshot.SchedulingError)
		if ok && schedErr.Type() == clustersnapshot.FailingPredicateError {
			if schedErr.FailingPredicateName() == noderesources.Name {
				notFitting = append(notFitting, info)
				continue
			}
		}
		remaining = append(remaining, info)
	}

	return rejectedMigsDivision{
		all:        rejectedMigInfos,
		notFitting: notFitting,
		remaining:  remaining,
	}
}

func divideDisregardedMigs(disregardedMigInfos []vistypes.DisregardedMigInfo) disregardedMigsDivision {
	resourceBackedOff := make([]vistypes.DisregardedMigInfo, 0)
	standardBackedOff := make([]vistypes.DisregardedMigInfo, 0)
	unableToBuildNodeGroup := make([]vistypes.DisregardedMigInfo, 0)
	internalError := make([]vistypes.DisregardedMigInfo, 0)

	for _, disregardedMigInfo := range disregardedMigInfos {
		switch disregardedMigInfo.Reason {
		case autoprovisioning.InResourceBasedBackoff:
			resourceBackedOff = append(resourceBackedOff, disregardedMigInfo)
		case autoprovisioning.InStandardBackoff:
			standardBackedOff = append(standardBackedOff, disregardedMigInfo)
		case autoprovisioning.UnableToBuildNodeGroup:
			unableToBuildNodeGroup = append(unableToBuildNodeGroup, disregardedMigInfo)
		case autoprovisioning.InternalError:
			internalError = append(internalError, disregardedMigInfo)
		case autoprovisioning.NoReason:
			klog.Errorf("CA Viz NoScaleUp: disregarded MIG reason NoReason encountered, this shouldn't happen.")
			internalError = append(internalError, disregardedMigInfo)
		default:
			klog.Errorf("CA Viz NoScaleUp: disregarded MIG unknown reason encountered: %v.", disregardedMigInfo.Reason)
			internalError = append(internalError, disregardedMigInfo)
		}
	}

	return disregardedMigsDivision{
		all:                    disregardedMigInfos,
		resourceBackedOff:      resourceBackedOff,
		standardBackedOff:      standardBackedOff,
		unableToBuildNodeGroup: unableToBuildNodeGroup,
		internalError:          internalError,
	}
}

func extractNapMigsAndGroupByZone(migReasons map[string]status.Reasons, migsById map[string]*vistypes.GkeMig) map[string][]migReasonsInfo {
	migsByZone := make(map[string][]migReasonsInfo)
	for migId, reasons := range migReasons {
		mig, found := migsById[migId]
		if !found {
			klog.Errorf("CA Viz NoScaleUp: MIG %s present in migReasons, but wasn't provided in migsById.", migId)
			continue
		}
		if mig.Exists {
			continue
		}

		migsByZone[mig.Zone] = append(migsByZone[mig.Zone], migReasonsInfo{mig: mig, reasons: reasons})
	}
	return migsByZone
}

func groupDisregardedNapMigsByZone(disregardedMigs map[autoprovisioning.NodeGroupOptions]autoprovisioning.NodeGroupDisregardedReason) map[string][]vistypes.DisregardedMigInfo {
	result := make(map[string][]vistypes.DisregardedMigInfo)
	for key, reason := range disregardedMigs {
		result[key.Zone] = append(result[key.Zone], vistypes.DisregardedMigInfo{Key: key, Reason: reason})
	}
	return result
}

func getAllSortedZonesFromReasonsByZone(skippedMigInfosByZone, rejectedMigInfosByZone map[string][]migReasonsInfo, disregardedMigInfosByZone map[string][]vistypes.DisregardedMigInfo) []string {
	allZones := make(map[string]bool)
	for zone := range skippedMigInfosByZone {
		allZones[zone] = true
	}
	for zone := range rejectedMigInfosByZone {
		allZones[zone] = true
	}
	for zone := range disregardedMigInfosByZone {
		allZones[zone] = true
	}

	var result []string
	for zone := range allZones {
		result = append(result, zone)
	}
	sort.Strings(result)
	return result
}

func reasonsContainMaxResourceLimitReached(reasons status.Reasons) bool {
	_, ok := reasons.(*orchestrator.MaxResourceLimitReached)
	return ok
}
