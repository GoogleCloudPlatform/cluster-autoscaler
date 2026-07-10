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

package types

import (
	"fmt"
	"strconv"
	"strings"

	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
)

// MessageId is a type representing the id of a parameterized message.
type MessageId int

const (
	// ScaleUpErrorOutOfResources - scale-up failed because some of the MIGs couldn't be increased due to a stockout.
	ScaleUpErrorOutOfResources MessageId = iota
	// ScaleUpErrorQuotaExceeded - scale-up failed because some of the MIGs couldn't be increased due to exceeded quota.
	ScaleUpErrorQuotaExceeded
	// ScaleUpErrorWaitingForInstancesTimeout - scale-up failed because instances in some of the MIGs failed to appear in time.
	ScaleUpErrorWaitingForInstancesTimeout
	// ScaleUpErrorIPSpaceExhausted  - scale-up failed because instances in some of the MIGs ran out of IPs.
	ScaleUpErrorIPSpaceExhausted
	// ScaleUpErrorServiceAccountDeleted - scale-up failed because the service account was deleted.
	ScaleUpErrorServiceAccountDeleted
	// ScaleUpErrorUnsupportedCompactPlacementConfig - compact placement was requested with unsupported set of options
	ScaleUpErrorUnsupportedCompactPlacementConfig
	// ScaleUpErrorCompactPlacementNodeGroupAlreadyExists - pod requests a node group that already exists
	ScaleUpErrorCompactPlacementNodeGroupAlreadyExists
	// ScaleUpErrorTpuTopologyInvalid - scale-up failed because the specified TPU topology is invalid.
	ScaleUpErrorTpuTopologyInvalid
	// ScaleUpErrorTpuConfigurationInvalid - scale-up failed because the specified TPU configuration is invalid.
	ScaleUpErrorTpuConfigurationInvalid
	// ScaleUpErrorInvalidReservation - scale-up failed becase the reservation is invalid.
	ScaleUpErrorInvalidReservation
	// ScaleUpErrorReservationNotReady - scale-up failed because the reservation is not in ready state.
	ScaleUpErrorReservationNotReady
	//ScaleUpErrorReservationCapacityExceeded - scale-up failed because the reservation's capacity is exceeded.
	ScaleUpErrorReservationCapacityExceeded
	// ScaleUpErrorReservationNotFound - scale-up failed because the reservation was not found or incorrectly shared.
	ScaleUpErrorReservationNotFound
	// ScaleUpErrorReservationIncompatible - scale-up failed because the reservation is not compatible with the node group.
	ScaleUpErrorReservationIncompatible
	// ScaleUpErrorAnyAffinityReservationsNotAvailable - scale-up failed because any affinity reservations are not available.
	ScaleUpErrorAnyAffinityReservationsNotAvailable
	// ScaleUpErrorAnyAffinityReservationsNoCapacity - scale-up failed because any affinity reservations have no capacity.
	ScaleUpErrorAnyAffinityReservationsNoCapacity
	// ScaleUpErrorOther - scale-up failed because some of the MIGs couldn't be increased due to an unspecified error.
	ScaleUpErrorOther

	// ScaleDownErrorFailedToMarkToBeDeleted - scale-down failed because a node couldn't be marked to be deleted.
	ScaleDownErrorFailedToMarkToBeDeleted
	// ScaleDownErrorFailedToEvictPods - scale-down failed because some of the pods couldn't be evicted from a node.
	ScaleDownErrorFailedToEvictPods
	// ScaleDownErrorFailedToDeleteNodeMinSizeReached - scale-down failed because a node couldn't be deleted due to the cluster already being at minimal size.
	ScaleDownErrorFailedToDeleteNodeMinSizeReached
	// ScaleDownErrorFailedToDeleteNodeOther - scale down failed because a node couldn't be deleted due to an unspecified error.
	ScaleDownErrorFailedToDeleteNodeOther

	// NoScaleUpMigFailingPredicate - can't scale up a mig because a predicate failed for it.
	NoScaleUpMigFailingPredicate
	// NoScaleUpMigSkipped - can't scale up a mig because it was skipped during the simulation.
	NoScaleUpMigSkipped
	// NoScaleUpMigUnknownReason - can't scale up a mig because of an unknown reason.
	NoScaleUpMigUnknownReason

	// NoScaleUpUnexpectedError - no scale-up because of an unexpected error.
	NoScaleUpUnexpectedError
	// NoScaleUpNotTried - no scale-up because the scale-up wasn't even attempted.
	NoScaleUpNotTried
	// NoScaleUpInBackoff - no scale-up because scaling-up is in a backoff period.
	NoScaleUpInBackoff

	// NoScaleUpNapDisabled - NAP didn't provision any node groups because it was disabled.
	NoScaleUpNapDisabled
	// NoScaleUpNapUnexpectedError - NAP didn't provision any node groups because of an unexpected error.
	NoScaleUpNapUnexpectedError
	// NoScaleUpNapNoLocationsAvailable - NAP didn't provision any node groups because there weren't any NAP locations available.
	NoScaleUpNapNoLocationsAvailable
	// NoScaleUpNapNodeGroupsLimitReached - NAP didn't provision any node groups because maximut count of autoprovisioned node groups has been reached.
	NoScaleUpNapNodeGroupsLimitReached

	// NoScaleUpNapPodInvalidLabelValue - pod requests an invalid value for label.
	NoScaleUpNapPodInvalidLabelValue
	// NoScaleUpNapPodMachineFamilyUnknown - pod requests an unknown machine family.
	NoScaleUpNapPodMachineFamilyUnknown
	// NoScaleUpNapPodMachineFamilyNotSupported - pod requests a machine family that is not supported in NAP.
	NoScaleUpNapPodMachineFamilyNotSupported
	// NoScaleUpNapPodComputeClassUnknown - pod requests an unknown compute class.
	NoScaleUpNapPodComputeClassUnknown
	// NoScaleUpNapPodComputeClassNonAutopilot - pod requests a compute class available only in Autopilot.
	NoScaleUpNapPodComputeClassNonAutopilot
	// NoScaleUpNapPodAutopilotArchNoComputeClass - pod requests an arch without required compute class in Autopilot.
	NoScaleUpNapPodAutopilotArchNoComputeClass
	// NoScaleUpNapPodComputeClassWithMachineFamily - pod requests both the compute class and a machine family.
	NoScaleUpNapPodComputeClassWithMachineFamily
	// NoScaleUpNapPodComputeClassWithInvalidMachineFamily - pod requests compute class with invalid machine family.
	NoScaleUpNapPodComputeClassWithInvalidMachineFamily
	// NoScaleUpNapPodComputeClassWithoutMachineFamily - pod requests a compute class, which requires machine family, without a machine family.
	NoScaleUpNapPodComputeClassWithoutMachineFamily
	// NoScaleUpNapPodComputeClassWithoutAccelerator - pod requests a compute class, which requires accelerator, without a accelerator tpu,gpu specified.
	NoScaleUpNapPodComputeClassWithoutAccelerator
	// NoScaleUpNapPodMinCpuPlatformUnknown - pod requests an unknown min_cpu_platform.
	NoScaleUpNapPodMinCpuPlatformUnknown
	// NoScaleUpNapPodMinCpuPlatformInvalid - pod requests an invalid machines and min_cpu_platform combination.
	NoScaleUpNapPodMinCpuPlatformInvalid
	// NoScaleUpNapPodMultipleMinCpuPlatforms - pod requests multiple min_cpu_platform values.
	NoScaleUpNapPodMultipleMinCpuPlatforms
	// NoScaleUpNapPodGpuIncompatible - pod requests an incompatible machines and GPU type combination.
	NoScaleUpNapPodGpuIncompatible
	// NoScaleUpNapPodInvalidPlacementGroupName - pod requests an invalid placement group.
	NoScaleUpNapPodInvalidPlacementGroupName
	// NoScaleUpNapPodInvalidCompactPlacementMachineFamily - pod requests compact placement without specifying a supported machine family.
	NoScaleUpNapPodInvalidCompactPlacementMachineFamily
	// NoScaleUpNapPodCompactPlacementNodeGroupAlreadyExists - pod requests a node group that already exists
	NoScaleUpNapPodCompactPlacementNodeGroupAlreadyExists
	// NoScaleUpNapPodGpuMinCpuPlatformIncompatible - pod requests a GPU and min_cpu_platform which are incompatible.
	NoScaleUpNapPodGpuMinCpuPlatformIncompatible
	// NoScaleUpNapPodConfidentialNodesIncompatible - Confidential Nodes are enabled, and the pod requests machines incompatible with them.
	NoScaleUpNapPodConfidentialNodesIncompatible
	// NoScaleUpNapPodWorkloadSeparationInvalid - pod requires a non-system label, and doesn't have a matching toleration.
	NoScaleUpNapPodWorkloadSeparationInvalid
	// NoScaleUpNapPodNoPSCInfrastructure - pod has private node affinity in a cluster without PSC infrastructure.
	NoScaleUpNapPodNoPSCInfrastructure
	// NoScaleUpNapPodArchUnknown - pod requests an unknown architecture.
	NoScaleUpNapPodArchUnknown
	// NoScaleUpNapPodArchInvalid - pod requests an invalid machines and architecture combination.
	NoScaleUpNapPodArchInvalid
	// NoScaleUpNapPodMachineConfigInvalid - pod requests a machine config that is invalid in some way not detected explicitly.
	NoScaleUpNapPodMachineConfigInvalid
	// NoScaleUpNapPodUnexpectedError - NAP provided an unexpected error type for the pod.
	NoScaleUpNapPodUnexpectedError
	// NoScaleUpNapPodUnusableReservation - pod requests an unusable reservation.
	NoScaleUpNapPodUnusableReservation
	// NoScaleUpNapPodInvalidPlacementPolicy - pod requests a missing resource policy
	NoScaleUpNapPodInvalidPlacementPolicy
	// NoScaleUpNapMissingAIZones - pod requests AI zones which were not found in the cluster region.
	NoScaleUpNapPodMissingAIZones

	// NoScaleUpNapPodGpuNoLimitDefined - NAP didn't provision any node group for the pod because the pod has a GPU request and the GPU doesn't have a limit defined.
	NoScaleUpNapPodGpuNoLimitDefined
	// NoScaleUpNapPodGpuTypeNotSupported - NAP didn't provision any node group for the pod because it specifies a GPU which isn't supported.
	NoScaleUpNapPodGpuTypeNotSupported
	// NoScaleUpNapPodGpuRequestInvalid - NAP didn't provision any node group for the pod because its GPU request is invalid.
	NoScaleUpNapPodGpuRequestInvalid
	// NoScaleUpNapPodMachineFamiliesDoNotSupportDws - NAP didn't provision any node group for the pod because the machine family associated with requested accelerator does not support DWS.
	NoScaleUpNapPodMachineFamiliesDoNotSupportDws
	// NoScaleUpNapPodGpuFailingPredicates - NAP didn't provision any node group for the pod because it wouldn't pass scheduler predicates in any of the injected node groups.
	NoScaleUpNapPodGpuFailingPredicates

	// NoScaleUpNapPodTpuIncompatible - pod requests an incompatible machines and GPU type combination
	NoScaleUpNapPodTpuIncompatible
	// NoScaleUpNapPodTpuTypeNotSupported - NAP didn't provision any node group for the pod because it specifies a TPU which isn't supported.
	NoScaleUpNapPodTpuTypeNotSupported
	// NoScaleUpNapPodTpuNoLimitDefined  - NAP didn't provision any node group for the pod because the pod has a TPU request and the TPU doesn't have a limit defined.
	NoScaleUpNapPodTpuNoLimitDefined
	// NoScaleUpNapPodTpuAcceleratorCountInvalid  - NAP didn't provision any node group for the pod because no machine type supports the specified accelerator count.
	NoScaleUpNapPodTpuAcceleratorCountInvalid

	// NoScaleUpNapPodZonalResourcesExceeded - NAP didn't provision any node group for the pod because some resources would exceeded.
	NoScaleUpNapPodZonalResourcesExceeded
	// NoScaleUpNapPodZonalIllegalConfig - NAP didn't provision any node group for the pod because it requests an illegal MIG configuration.
	NoScaleUpNapPodZonalIllegalConfig
	// NoScaleUpNapPodZonalFailingPredicates - NAP didn't provision any node group for the pod because of failing predicates.
	NoScaleUpNapPodZonalFailingPredicates
	// NoScaleUpNapPodZonalUnexpectedError - NAP didn't provision any node group for the pod because of an unexpected error.
	NoScaleUpNapPodZonalUnexpectedError
	// NoScaleUpNapPodZonalOtherError - NAP didn't provision any node group for the pod because of an other, unspecified error.
	NoScaleUpNapPodZonalOtherError

	// NoScaleUpNapExtendedDurationPodCPUReqInvalid - NAP didn't provision any node group for the pod because of incorrect extended duration cpuReq config.
	NoScaleUpNapExtendedDurationPodCPUReqInvalid
	// NoScaleUpNapExtendedDurationPodNonAutopilotError - NAP didn't provision any node group for the pod because of extended duration pods being created in non autopilot cluster.
	NoScaleUpNapExtendedDurationPodNonAutopilotError

	// NoScaleUpNapNpcNotFound - NAP didn't provision any node group for the pod because of pod requesting non-existing NPC.
	NoScaleUpNapNpcNotFound
	// NoScaleUpNapCccNotFound - NAP didn't provision any node group for the pod because of pod requesting non-existing CCC.
	NoScaleUpNapCccNotFound
	// NoScaleUpNapNpcFetchError - NAP didn't provision any node group for the pod because it failed to fetch NPC.
	NoScaleUpNapNpcFetchError
	// NoScaleUpNapCccFetchError - NAP didn't provision any node group for the pod because it failed to fetch CCC.
	NoScaleUpNapCccFetchError

	// NoScaleUpNapNpcAutoprovisioningDisabled - NAP didn't provision any node group because it is disabled for NPC.
	NoScaleUpNapNpcAutoprovisioningDisabled
	// NoScaleUpNapCccAutoprovisioningDisabled - NAP didn't provision any node group because it is disabled for CCC.
	NoScaleUpNapCccAutoprovisioningDisabled
	// NoScaleUpNapNpcPodIncompatible - NAP didn't provision any node group for the pod because pod being incompatible with NPC.
	NoScaleUpNapNpcPodIncompatible
	// NoScaleUpNapCccPodIncompatible - NAP didn't provision any node group for the pod because pod being incompatible with CCC.
	NoScaleUpNapCccPodIncompatible

	// NoScaleUpNapNpcCccBothDefined - NAP didn't provision any node group for the pod because pod has both NPC and CCC configured.
	NoScaleUpNapNpcCccBothDefined

	// NoScaleUpNapIsolatedPodCPUReqInvalid - NAP didn't provision any node group for the pod because of incorrect isolated pod cpuReq config.
	NoScaleUpNapIsolatedPodCPUReqInvalid
	// NoScaleUpNapIsolatedPodCapacityInvalid - NAP didn't provision any node group for the pod because of incorrect isolated pod capacity config.
	NoScaleUpNapIsolatedPodCapacityInvalid
	// NoScaleUpNapIsolatedPodNonAutopilotError - NAP didn't provision any node group for the pod because of isolated pods being created in non autopilot cluster.
	NoScaleUpNapIsolatedPodNonAutopilotError

	// NoScaleUpNapFlexStartMisconfiguredError - NAP didn't provision any node group for the pod because of Flex Start node selectors misconfiguration.
	NoScaleUpNapFlexStartMisconfiguredError

	// NoScaleUpNapInvalidConfidentialNodeType - NAP didn't provision any node group for the pod because the confidential node type is invalid
	NoScaleUpNapInvalidConfidentialNodeType
	// NoScaleUpNapInvalidMachineFamilyForConfidentialNodeType - NAP didn't provision any node group for the pod because the machine family is invalid for the confidential node type
	NoScaleUpNapInvalidMachineFamilyForConfidentialNodeType

	// NoScaleDownUnexpectedError - no scale-down because of an unexpected error.
	NoScaleDownUnexpectedError
	// NoScaleDownNotTried - no scale-down because the scale-down wasn't even attempted.
	NoScaleDownNotTried
	// NoScaleDownInBackoff - no scale-down because scaling-down is in a backoff period.
	NoScaleDownInBackoff
	// NoScaleDownInProgress - no scale-down because a previous scale-down was still in progress.
	NoScaleDownInProgress

	// NoScaleDownNodeScaleDownDisabledAnnotation - node can't be removed because it has a "scale down disabled" annotation.
	NoScaleDownNodeScaleDownDisabledAnnotation
	// NoScaleDownNodeNodeGroupMinSizeReached - node can't be removed because its node group is at its minimal size already.
	NoScaleDownNodeNodeGroupMinSizeReached
	// NoScaleDownNodeMinimalResourceLimitsExceeded - node can't be removed because it would violate cluster-wide minimal resource limits.
	NoScaleDownNodeMinimalResourceLimitsExceeded
	// NoScaleDownNodeNoPlaceToMovePods - node can't be removed because there's no place to move its pods to.
	NoScaleDownNodeNoPlaceToMovePods
	// NoScaleDownNodeUnexpectedError - node can't be removed because of an unexpected error.
	NoScaleDownNodeUnexpectedError

	// NoScaleDownNodePodControllerNotFound - pod is blocking scale down because its controller can't be found.
	NoScaleDownNodePodControllerNotFound
	// NoScaleDownNodePodMinReplicasReached - pod is blocking scale down because its controller already has the minimum number of replicas.
	NoScaleDownNodePodMinReplicasReached
	// NoScaleDownNodePodNotBackedByController - pod is blocking scale down because it's not backed by a controller.
	NoScaleDownNodePodNotBackedByController
	// NoScaleDownNodePodHasLocalStorage - pod is blocking scale down because it has local storage.
	NoScaleDownNodePodHasLocalStorage
	// NoScaleDownNodePodNotSafeToEvictAnnotation - pod is blocking scale down because it has a "not safe to evict" annotation.
	NoScaleDownNodePodNotSafeToEvictAnnotation
	// NoScaleDownNodePodKubeSystemUnmovable - pod is blocking scale down because it's a non-daemonset, non-mirrored, non-pdb-assigned kube-system pod.
	NoScaleDownNodePodKubeSystemUnmovable
	// NoScaleDownNodePodNotEnoughPdb - pod is blocking scale down because it doesn't have enough PDB left.
	NoScaleDownNodePodNotEnoughPdb
	// NoScaleDownNodePodUnexpectedError - pod is blocking scale down because of an unexpected error.
	NoScaleDownNodePodUnexpectedError
	// Just for testing if all massageIds have correspoding value in MessageIdToStringMap - it needs to be last!
	_maxMessageId
)

// MessageIdToStringMap maps enum IDs to their textual representation.
var MessageIdToStringMap = map[MessageId]string{
	ScaleUpErrorOutOfResources:                         "scale.up.error.out.of.resources",
	ScaleUpErrorQuotaExceeded:                          "scale.up.error.quota.exceeded",
	ScaleUpErrorWaitingForInstancesTimeout:             "scale.up.error.waiting.for.instances.timeout",
	ScaleUpErrorIPSpaceExhausted:                       "scale.up.error.ip.space.exhausted",
	ScaleUpErrorServiceAccountDeleted:                  "scale.up.error.service.account.deleted",
	ScaleUpErrorUnsupportedCompactPlacementConfig:      "scale.up.error.unsupportred.compact.placement.config",
	ScaleUpErrorCompactPlacementNodeGroupAlreadyExists: "scale.up.error.compact.placement.node.group.already.exists",
	ScaleUpErrorTpuTopologyInvalid:                     "scale.up.error.tpu.topology.invalid",
	ScaleUpErrorTpuConfigurationInvalid:                "scale.up.error.tpu.configuration.invalid",
	ScaleUpErrorInvalidReservation:                     "scale.up.error.invalid.reservation",
	ScaleUpErrorReservationNotReady:                    "scale.up.error.reservation.not.ready",
	ScaleUpErrorReservationCapacityExceeded:            "scale.up.error.reservation.capacity.exceeded",
	ScaleUpErrorReservationNotFound:                    "scale.up.error.reservation.not.found",
	ScaleUpErrorReservationIncompatible:                "scale.up.error.reservation.incompatible",
	ScaleUpErrorAnyAffinityReservationsNotAvailable:    "scale.up.error.any.affinity.reservations.not.available",
	ScaleUpErrorAnyAffinityReservationsNoCapacity:      "scale.up.error.any.affinity.reservations.no.capacity",
	ScaleUpErrorOther:                                  "scale.up.error.other",

	ScaleDownErrorFailedToMarkToBeDeleted:          "scale.down.error.failed.to.mark.to.be.deleted",
	ScaleDownErrorFailedToEvictPods:                "scale.down.error.failed.to.evict.pods",
	ScaleDownErrorFailedToDeleteNodeMinSizeReached: "scale.down.error.failed.to.delete.node.min.size.reached",
	ScaleDownErrorFailedToDeleteNodeOther:          "scale.down.error.failed.to.delete.node.other",

	NoScaleUpMigSkipped:          "no.scale.up.mig.skipped",
	NoScaleUpMigFailingPredicate: "no.scale.up.mig.failing.predicate",
	NoScaleUpMigUnknownReason:    "no.scale.up.mig.unknown.reason",

	NoScaleUpUnexpectedError: "no.scale.up.unexpected.error",
	NoScaleUpNotTried:        "no.scale.up.not.tried",
	NoScaleUpInBackoff:       "no.scale.up.in.backoff",

	NoScaleUpNapDisabled:               "no.scale.up.nap.disabled",
	NoScaleUpNapUnexpectedError:        "no.scale.up.nap.unexpected.error",
	NoScaleUpNapNoLocationsAvailable:   "no.scale.up.nap.no.locations.available",
	NoScaleUpNapNodeGroupsLimitReached: "no.scale.up.nap.node.groups.limit.reached",

	NoScaleUpNapPodInvalidLabelValue:                        "no.scale.up.nap.pod.invalid.label.value",
	NoScaleUpNapPodMachineFamilyUnknown:                     "no.scale.up.nap.pod.machine.family.unknown",
	NoScaleUpNapPodMachineFamilyNotSupported:                "no.scale.up.nap.pod.machine.family.not.supported",
	NoScaleUpNapPodComputeClassUnknown:                      "no.scale.up.nap.pod.compute.class.unknown",
	NoScaleUpNapPodComputeClassNonAutopilot:                 "no.scale.up.nap.pod.compute.class.non.autopilot",
	NoScaleUpNapPodAutopilotArchNoComputeClass:              "no.scale.up.nap.pod.autopilot.arch.no.compute.class",
	NoScaleUpNapPodComputeClassWithMachineFamily:            "no.scale.up.nap.pod.compute.class.with.machine.family",
	NoScaleUpNapPodComputeClassWithInvalidMachineFamily:     "no.scale.up.nap.pod.compute.class.with.invalid.machine.family",
	NoScaleUpNapPodComputeClassWithoutMachineFamily:         "no.scale.up.nap.pod.compute.class.without.machine.family",
	NoScaleUpNapPodComputeClassWithoutAccelerator:           "no.scale.up.nap.pod.compute.class.without.accelerator",
	NoScaleUpNapPodMinCpuPlatformUnknown:                    "no.scale.up.nap.pod.min.cpu.platform.unknown",
	NoScaleUpNapPodMinCpuPlatformInvalid:                    "no.scale.up.nap.pod.min.cpu.platform.invalid",
	NoScaleUpNapPodMultipleMinCpuPlatforms:                  "no.scale.up.nap.pod.multiple.min.cpu.platforms",
	NoScaleUpNapPodGpuIncompatible:                          "no.scale.up.nap.pod.gpu.incompatible",
	NoScaleUpNapPodInvalidPlacementGroupName:                "no.scale.up.nap.pod.invalid.placement.group.name",
	NoScaleUpNapPodInvalidCompactPlacementMachineFamily:     "no.scale.up.nap.pod.invalid.compact.placement.machine.family",
	NoScaleUpNapPodCompactPlacementNodeGroupAlreadyExists:   "no.scale.up.nap.pod.compact.placement.node.group.already.exists",
	NoScaleUpNapPodGpuMinCpuPlatformIncompatible:            "no.scale.up.nap.pod.gpu.min.cpu.platform.incompatible",
	NoScaleUpNapPodConfidentialNodesIncompatible:            "no.scale.up.nap.pod.confidential.nodes.incompatible",
	NoScaleUpNapInvalidConfidentialNodeType:                 "no.scale.up.nap.pod.invalid.confidential.node.type",
	NoScaleUpNapInvalidMachineFamilyForConfidentialNodeType: "no.scale.up.nap.pod.invalid.machine.family.for.confidential.node.type",
	NoScaleUpNapPodWorkloadSeparationInvalid:                "no.scale.up.nap.pod.workload.separation.invalid",
	NoScaleUpNapPodNoPSCInfrastructure:                      "no.scale.up.nap.pod.no.psc.infrastructure",
	NoScaleUpNapPodArchUnknown:                              "no.scale.up.nap.pod.arch.unknown",
	NoScaleUpNapPodArchInvalid:                              "no.scale.up.nap.pod.arch.invalid",
	NoScaleUpNapPodMachineConfigInvalid:                     "no.scale.up.nap.pod.machine.config.invalid",
	NoScaleUpNapPodUnexpectedError:                          "no.scale.up.nap.pod.unexpected.error",
	NoScaleUpNapPodUnusableReservation:                      "no.scale.up.nap.pod.unusable.reservation",
	NoScaleUpNapPodInvalidPlacementPolicy:                   "no.scale.up.nap.pod.missing.placement.policy",
	NoScaleUpNapPodMissingAIZones:                           "no.scale.up.nap.pod.missing.ai.zones",

	NoScaleUpNapPodGpuNoLimitDefined:              "no.scale.up.nap.pod.gpu.no.limit.defined",
	NoScaleUpNapPodGpuTypeNotSupported:            "no.scale.up.nap.pod.gpu.type.not.supported",
	NoScaleUpNapPodGpuRequestInvalid:              "no.scale.up.nap.pod.gpu.request.invalid",
	NoScaleUpNapPodMachineFamiliesDoNotSupportDws: "no.scale.up.nap.pod.machine.families.do.not.support.dws",
	NoScaleUpNapPodGpuFailingPredicates:           "no.scale.up.nap.pod.gpu.failing.predicates",

	NoScaleUpNapPodTpuIncompatible:            "no.scale.up.nap.pod.tpu.incompatible",
	NoScaleUpNapPodTpuTypeNotSupported:        "no.scale.up.nap.pod.tpu.type.not.supported",
	NoScaleUpNapPodTpuNoLimitDefined:          "no.scale.up.nap.pod.tpu.no.limit.defined",
	NoScaleUpNapPodTpuAcceleratorCountInvalid: "no.scale.up.nap.pod.tpu.accelerator.count.invalid",

	NoScaleUpNapPodZonalResourcesExceeded: "no.scale.up.nap.pod.zonal.resources.exceeded",
	NoScaleUpNapPodZonalUnexpectedError:   "no.scale.up.nap.pod.zonal.unexpected.error",
	NoScaleUpNapPodZonalIllegalConfig:     "no.scale.up.nap.pod.zonal.illegal.config",
	NoScaleUpNapPodZonalFailingPredicates: "no.scale.up.nap.pod.zonal.failing.predicates",
	NoScaleUpNapPodZonalOtherError:        "no.scale.up.nap.pod.zonal.other.error",

	NoScaleUpNapExtendedDurationPodCPUReqInvalid:     "no.scale.up.extended.duration.pod.cpu.req.invalid",
	NoScaleUpNapExtendedDurationPodNonAutopilotError: "no.scale.up.extended.duration.pod.non.autopilot.error",

	NoScaleUpNapNpcNotFound:                 "no.scale.up.nap.npc.not.found",
	NoScaleUpNapCccNotFound:                 "no.scale.up.nap.ccc.not.found",
	NoScaleUpNapNpcFetchError:               "no.scale.up.nap.npc.fetch.error",
	NoScaleUpNapCccFetchError:               "no.scale.up.nap.ccc.fetch.error",
	NoScaleUpNapNpcAutoprovisioningDisabled: "no.scale.up.nap.npc.autoprovisioning.disabled",
	NoScaleUpNapCccAutoprovisioningDisabled: "no.scale.up.nap.ccc.autoprovisioning.disabled",
	NoScaleUpNapNpcPodIncompatible:          "no.scale.up.nap.npc.pod.incompatible",
	NoScaleUpNapCccPodIncompatible:          "no.scale.up.nap.ccc.pod.incompatible",

	NoScaleUpNapNpcCccBothDefined: "no.scale.up.nap.npc.ccc.both.defined",

	NoScaleUpNapIsolatedPodCPUReqInvalid:     "no.scale.up.isolated.pod.cpu.req.invalid",
	NoScaleUpNapIsolatedPodCapacityInvalid:   "no.scale.up.isolated.pod.capacity.invalid",
	NoScaleUpNapIsolatedPodNonAutopilotError: "no.scale.up.isolated.pod.non.autopilot.error",
	NoScaleUpNapFlexStartMisconfiguredError:  "no.scale.up.flex.start.misconfigured.error",

	NoScaleDownUnexpectedError: "no.scale.down.unexpected.error",
	NoScaleDownNotTried:        "no.scale.down.not.tried",
	NoScaleDownInBackoff:       "no.scale.down.in.backoff",
	NoScaleDownInProgress:      "no.scale.down.in.progress",

	NoScaleDownNodeScaleDownDisabledAnnotation:   "no.scale.down.node.scale.down.disabled.annotation",
	NoScaleDownNodeNodeGroupMinSizeReached:       "no.scale.down.node.node.group.min.size.reached",
	NoScaleDownNodeMinimalResourceLimitsExceeded: "no.scale.down.node.minimal.resource.limits.exceeded",
	NoScaleDownNodeNoPlaceToMovePods:             "no.scale.down.node.no.place.to.move.pods",
	NoScaleDownNodeUnexpectedError:               "no.scale.down.node.unexpected.error",

	NoScaleDownNodePodControllerNotFound:       "no.scale.down.node.pod.controller.not.found",
	NoScaleDownNodePodMinReplicasReached:       "no.scale.down.node.pod.min.replicas.reached",
	NoScaleDownNodePodNotBackedByController:    "no.scale.down.node.pod.not.backed.by.controller",
	NoScaleDownNodePodHasLocalStorage:          "no.scale.down.node.pod.has.local.storage",
	NoScaleDownNodePodNotSafeToEvictAnnotation: "no.scale.down.node.pod.not.safe.to.evict.annotation",
	NoScaleDownNodePodKubeSystemUnmovable:      "no.scale.down.node.pod.kube.system.unmovable",
	NoScaleDownNodePodNotEnoughPdb:             "no.scale.down.node.pod.not.enough.pdb",
	NoScaleDownNodePodUnexpectedError:          "no.scale.down.node.pod.unexpected.error",
}

// String converts message's numerical id to its textual representation.
func (id MessageId) String() string {
	return MessageIdToStringMap[id]
}

// Message represents a parameterized message that can be transformed into a visibility event proto representation.
type Message struct {
	Id     MessageId
	Params []string
}

// MessageSignature is a unique identifier for a message along with its params.
type MessageSignature string

// Proto transforms the message into a visibility event proto representation.
func (m *Message) Proto() *vispb.ParametrizedMessage {
	return &vispb.ParametrizedMessage{
		MessageId:  m.Id.String(),
		Parameters: m.Params,
	}
}

// Signature computes a string uniquely identifying the message.
func (m *Message) Signature() MessageSignature {
	idStr := strconv.Itoa(int(m.Id))
	sigParts := append([]string{idStr}, m.Params...)
	return MessageSignature(strings.Join(sigParts, ","))
}

// MessagePerPodGroupSignature is a unique identifier for a message within a PodGroup.
type MessagePerPodGroupSignature string

// PerPodGroupSignature computes a unique identifier for a message within a provided PodGroup.
func (m *Message) PerPodGroupSignature(pgUID string) MessagePerPodGroupSignature {
	return MessagePerPodGroupSignature(fmt.Sprintf("%v/%v", pgUID, m.Signature()))
}

// NewScaleUpErrorOutOfResourcesMsg creates and returns a "scale-up failed because some of the MIGs couldn't be increased due to a stockout" message.
func NewScaleUpErrorOutOfResourcesMsg(failingMig string) *Message {
	return &Message{Id: ScaleUpErrorOutOfResources, Params: []string{failingMig}}
}

// NewScaleUpErrorQuotaExceededMsg creates and returns a "scale-up failed because some of the MIGs couldn't be increased due to exceeded quota" message.
func NewScaleUpErrorQuotaExceededMsg(failingMig string) *Message {
	return &Message{Id: ScaleUpErrorQuotaExceeded, Params: []string{failingMig}}
}

// NewScaleUpErrorIPSpaceExhaustedMsg creates and returns a "scale-up failed because some of the MIGs couldn't be increased due to ip space exhausted" message.
func NewScaleUpErrorIPSpaceExhaustedMsg(failingMig string) *Message {
	return &Message{Id: ScaleUpErrorIPSpaceExhausted, Params: []string{failingMig}}
}

// NewScaleUpErrorServiceAccountDeletedMsg creates and returns a "scale-up failed because some of the MIGs couldn't be increased due to service account deleted" message.
func NewScaleUpErrorServiceAccountDeletedMsg(failingMig string) *Message {
	return &Message{Id: ScaleUpErrorServiceAccountDeleted, Params: []string{failingMig}}
}

// NewScaleUpErrorWaitingForInstancesTimeoutMsg creates and returns a "scale-up failed because instances in some of the MIGs failed to appear in time" message.
func NewScaleUpErrorWaitingForInstancesTimeoutMsg(failingMig string) *Message {
	return &Message{Id: ScaleUpErrorWaitingForInstancesTimeout, Params: []string{failingMig}}
}

// NewScaleUpErrorUnsupportedCompactPlacementConfigMsg creates and returns a "pod requests compact placement with unsupported set of options" message.
func NewScaleUpErrorUnsupportedCompactPlacementConfigMsg(reason string) *Message {
	return &Message{Id: ScaleUpErrorUnsupportedCompactPlacementConfig, Params: []string{reason}}
}

// NewScaleUpErrorCompactPlacementNodeGroupAlreadyExistsMsg creates and returns a "compact placement node group requested by the pod already exists, but doesn't match pod requirements" message.
func NewScaleUpErrorCompactPlacementNodeGroupAlreadyExistsMsg(nodeGroupId string) *Message {
	return &Message{Id: ScaleUpErrorCompactPlacementNodeGroupAlreadyExists, Params: []string{nodeGroupId}}
}

func NewScaleUpErrorTpuTopologyInvalid(topology string) *Message {
	return &Message{Id: ScaleUpErrorTpuTopologyInvalid, Params: []string{topology}}
}

func NewScaleUpErrorTpuConfigurationInvalid(failingMig string) *Message {
	return &Message{Id: ScaleUpErrorTpuConfigurationInvalid, Params: []string{failingMig}}
}

// NewScaleUpErrorInvalidReservationMsg creates and returns an "Invalid Reservation" message.
func NewScaleUpErrorInvalidReservationMsg(failingMig string) *Message {
	return &Message{Id: ScaleUpErrorInvalidReservation, Params: []string{failingMig}}
}

// NewScaleUpErrorReservationNotReadyMsg creates and returns a "Reservation not ready" message.
func NewScaleUpErrorReservationNotReadyMsg(failingMig string) *Message {
	return &Message{Id: ScaleUpErrorReservationNotReady, Params: []string{failingMig}}
}

// NewScaleUpErrorReservationCapacityExceededMsg creates and returns a "Reservation capacity exceeded" message.
func NewScaleUpErrorReservationCapacityExceededMsg(nodeGroupId string) *Message {
	return &Message{Id: ScaleUpErrorReservationCapacityExceeded, Params: []string{nodeGroupId}}
}

// NewScaleUpErrorReservationNotFoundMsg creates and returns a "Reservation not found or incorrectly shared" message.
func NewScaleUpErrorReservationNotFoundMsg(nodeGroupId string) *Message {
	return &Message{Id: ScaleUpErrorReservationNotFound, Params: []string{nodeGroupId}}
}

// NewScaleUpErrorReservationIncompatibleMsg creates and returns a "Reservation incompatible with node group" message.
func NewScaleUpErrorReservationIncompatibleMsg(nodeGroupId string) *Message {
	return &Message{Id: ScaleUpErrorReservationIncompatible, Params: []string{nodeGroupId}}
}

// NewScaleUpErrorAnyAffinityReservationsNotAvailableMsg creates and returns an "any affinity reservations not available" message.
func NewScaleUpErrorAnyAffinityReservationsNotAvailableMsg(nodeGroupId string) *Message {
	return &Message{Id: ScaleUpErrorAnyAffinityReservationsNotAvailable, Params: []string{nodeGroupId, "There are no any affinity reservations matching the instance in your project"}}
}

// NewScaleUpErrorAnyAffinityReservationsNoCapacityMsg creates and returns an "any affinity reservations no capacity" message.
func NewScaleUpErrorAnyAffinityReservationsNoCapacityMsg(nodeGroupId string) *Message {
	return &Message{Id: ScaleUpErrorAnyAffinityReservationsNoCapacity, Params: []string{nodeGroupId, "All any affinity reservations in your project, or shared with your project, are fully consumed"}}
}

// NewScaleUpErrorOtherMsg creates and returns a "scale-up failed because some of the MIGs couldn't be increased due to an unspecified error" message.
func NewScaleUpErrorOtherMsg(failingMig string) *Message {
	return &Message{Id: ScaleUpErrorOther, Params: []string{failingMig}}
}

// NewScaleDownErrorFailedToMarkToBeDeletedMsg creates and returns a "scale-down failed because a node couldn't be marked to be deleted" message.
func NewScaleDownErrorFailedToMarkToBeDeletedMsg(failingNode string) *Message {
	return &Message{Id: ScaleDownErrorFailedToMarkToBeDeleted, Params: []string{failingNode}}
}

// NewScaleDownErrorFailedToEvictPodsMsg creates and returns a "scale-down failed because some of the pods couldn't be evicted from a node" message.
func NewScaleDownErrorFailedToEvictPodsMsg(failingNode string, failingPods []string) *Message {
	return &Message{Id: ScaleDownErrorFailedToEvictPods, Params: append([]string{failingNode}, failingPods...)}
}

// NewScaleDownErrorFailedToDeleteNodeMinSizeReachedMsg creates and returns a "scale-down failed because a node couldn't be deleted due to the cluster already being at minimal size" message.
func NewScaleDownErrorFailedToDeleteNodeMinSizeReachedMsg(failingNode string) *Message {
	return &Message{Id: ScaleDownErrorFailedToDeleteNodeMinSizeReached, Params: []string{failingNode}}
}

// NewScaleDownErrorFailedToDeleteNodeOtherMsg creates and returns a "scale down failed because a node couldn't be deleted to to an unspecified error" message.
func NewScaleDownErrorFailedToDeleteNodeOtherMsg(failingNode string) *Message {
	return &Message{Id: ScaleDownErrorFailedToDeleteNodeOther, Params: []string{failingNode}}
}

// NewNoScaleUpMigSkippedMsg creates and returns a "can't scale up a MIG because it was skipped during the simulation" message.
func NewNoScaleUpMigSkippedMsg(skippedReasons []string) *Message {
	return &Message{Id: NoScaleUpMigSkipped, Params: skippedReasons}
}

// NewNoScaleUpMigFailingPredicateMsg creates and returns a "can't scale up a MIG because some predicate failed for it" message.
func NewNoScaleUpMigFailingPredicateMsg(predicateName string, predicateFailureReasons []string) *Message {
	return &Message{Id: NoScaleUpMigFailingPredicate, Params: append([]string{predicateName}, predicateFailureReasons...)}
}

// NewNoScaleUpMigUnknownReasonMsg creates and returns a "can't scale up a MIG because of an unknown reason" message.
func NewNoScaleUpMigUnknownReasonMsg() *Message {
	return &Message{Id: NoScaleUpMigUnknownReason}
}

// NewNoScaleUpUnexpectedErrorMsg creates and returns a "no scale-up because of an unexpected error" message.
func NewNoScaleUpUnexpectedErrorMsg() *Message {
	return &Message{Id: NoScaleUpUnexpectedError}
}

// NewNoScaleUpNotTriedMsg creates and returns a "no scale-up because the scale-up wasn't even attempted" message.
func NewNoScaleUpNotTriedMsg() *Message {
	return &Message{Id: NoScaleUpNotTried}
}

// NewNoScaleUpInBackoffMsg creates and returns a "no scale-up because scaling-up is in a backoff period" message.
func NewNoScaleUpInBackoffMsg() *Message {
	return &Message{Id: NoScaleUpInBackoff}
}

// NewNoScaleUpNapDisabledMsg creates and returns a "NAP didn't provision any node groups because it was disabled" message.
func NewNoScaleUpNapDisabledMsg() *Message {
	return &Message{Id: NoScaleUpNapDisabled}
}

// NewNoScaleUpNapNoLocationsAvailableMsg creates and returns a "NAP didn't provision any node groups because there weren't any NAP locations available" message.
func NewNoScaleUpNapNoLocationsAvailableMsg() *Message {
	return &Message{Id: NoScaleUpNapNoLocationsAvailable}
}

// NewNoScaleUpNapNodeGroupsLimitReachedMsg creates and returns a "NAP didn't provision any node groups because maximut count of autoprovisioned node groups has been reached" message.
func NewNoScaleUpNapNodeGroupsLimitReachedMsg() *Message {
	return &Message{Id: NoScaleUpNapNodeGroupsLimitReached}
}

// NewNoScaleUpNapUnexpectedErrorMsg creates and returns a "NAP didn't provision any node groups because of an unexpected error" message.
func NewNoScaleUpNapUnexpectedErrorMsg() *Message {
	return &Message{Id: NoScaleUpNapUnexpectedError}
}

// NewNoScaleUpNapPodInvalidLabelValueMsg creates and returns a "pod requests an invalid value for label" message.
func NewNoScaleUpNapPodInvalidLabelValueMsg(label, value string) *Message {
	return &Message{Id: NoScaleUpNapPodInvalidLabelValue, Params: []string{label, value}}
}

// NewNoScaleUpNapPodMachineFamilyUnknownMsg creates and returns a "pod requests an unknown machine family" message.
func NewNoScaleUpNapPodMachineFamilyUnknownMsg(familyName string) *Message {
	return &Message{Id: NoScaleUpNapPodMachineFamilyUnknown, Params: []string{familyName}}
}

// NewNoScaleUpNapPodMachineFamilyNotSupportedMsg creates and returns a "pod requests machine family that is not supported in NAP" message.
func NewNoScaleUpNapPodMachineFamilyNotSupportedMsg(familyName string) *Message {
	return &Message{Id: NoScaleUpNapPodMachineFamilyNotSupported, Params: []string{familyName}}
}

// NewNoScaleUpNapPodComputeClassNonAutopilotMsg creates and returns a "pod requests a compute class available in Autopilot only" message.
func NewNoScaleUpNapPodComputeClassNonAutopilotMsg(className string) *Message {
	return &Message{Id: NoScaleUpNapPodComputeClassNonAutopilot, Params: []string{className}}
}

// NewNoScaleUpNapPodAutopilotArchNoComputeClassMsg creates and returns a "pod requests an arch without required compute class in Autopilot" message.
func NewNoScaleUpNapPodAutopilotArchNoComputeClassMsg(archName string) *Message {
	return &Message{Id: NoScaleUpNapPodAutopilotArchNoComputeClass, Params: []string{archName}}
}

// NewNoScaleUpNapPodComputeClassWithMachineFamilyMsg creates and returns a "pod requests both compute class and machine family" message.
func NewNoScaleUpNapPodComputeClassWithMachineFamilyMsg(className string) *Message {
	return &Message{Id: NoScaleUpNapPodComputeClassWithMachineFamily, Params: []string{className}}
}

// NewNoScaleUpNapPodComputeClassWithInvalidMachineFamilyMsg creates and returns a "pod requests compute class with invalid machine family" message.
func NewNoScaleUpNapPodComputeClassWithInvalidMachineFamilyMsg(className, machineGroupName string) *Message {
	return &Message{Id: NoScaleUpNapPodComputeClassWithInvalidMachineFamily, Params: []string{className, machineGroupName}}
}

// NewNoScaleUpNapPodComputeClassWithoutMachineFamilyMsg creates and returns a "pod requests compute class, requiring machine family, without a machine family" message.
func NewNoScaleUpNapPodComputeClassWithoutMachineFamilyMsg(className string) *Message {
	return &Message{Id: NoScaleUpNapPodComputeClassWithoutMachineFamily, Params: []string{className}}
}

// NewNoScaleUpNapPodComputeClassWithoutAcceleratorMsg creates and returns a "pod requests compute class, requiring accelerator, without an accelerator" message.
func NewNoScaleUpNapPodComputeClassWithoutAcceleratorMsg(className string) *Message {
	return &Message{Id: NoScaleUpNapPodComputeClassWithoutAccelerator, Params: []string{className}}
}

// NewNoScaleUpNapPodMinCpuPlatformUnknownMsg creates and returns a "pod requests an unknown min_cpu_platform" message.
func NewNoScaleUpNapPodMinCpuPlatformUnknownMsg(platformName string) *Message {
	return &Message{Id: NoScaleUpNapPodMinCpuPlatformUnknown, Params: []string{platformName}}
}

// NewNoScaleUpNapPodMinCpuPlatformInvalidMsg creates and returns a "pod requests an invalid machines and min_cpu_platform combination" message.
func NewNoScaleUpNapPodMinCpuPlatformInvalidMsg(machineGroupName, platformName string) *Message {
	return &Message{Id: NoScaleUpNapPodMinCpuPlatformInvalid, Params: []string{machineGroupName, platformName}}
}

// NewNoScaleUpNapPodMultipleMinCpuPlatformsMsg creates and returns a "pod requests multiple min_cpu_platform values" message.
func NewNoScaleUpNapPodMultipleMinCpuPlatformsMsg() *Message {
	return &Message{Id: NoScaleUpNapPodMultipleMinCpuPlatforms}
}

// NewNoScaleUpNapPodGpuIncompatibleMsg creates and returns a "pod requests an incompatible machines and GPU type combination" message.
func NewNoScaleUpNapPodGpuIncompatibleMsg(machineGroupName, gpuType string) *Message {
	return &Message{Id: NoScaleUpNapPodGpuIncompatible, Params: []string{machineGroupName, gpuType}}
}

// NewNoScaleUpNapPodInvalidPlacementGroupNameMsg creates and returns a "pod requests and invalid placement group" message.
func NewNoScaleUpNapPodInvalidPlacementGroupNameMsg(placementGroup string) *Message {
	return &Message{Id: NoScaleUpNapPodInvalidPlacementGroupName, Params: []string{placementGroup}}
}

// NewNoScaleUpNapPodInvalidCompactPlacementMachineFamilyMsg creates and returns a "pod requests compact placement without specifying
// a supported machine family" message.
func NewNoScaleUpNapPodInvalidCompactPlacementMachineFamilyMsg(placementGroup, msg string) *Message {
	return &Message{Id: NoScaleUpNapPodInvalidCompactPlacementMachineFamily, Params: []string{placementGroup, msg}}
}

// NewNoScaleUpNapPodCompactPlacementNodeGroupAlreadyExistsMsg creates and returns a "compact placement node group requested by the pod already exists, but doesn't match pod requirements" message.
func NewNoScaleUpNapPodCompactPlacementNodeGroupAlreadyExistsMsg(placementGroup string) *Message {
	return &Message{Id: NoScaleUpNapPodCompactPlacementNodeGroupAlreadyExists, Params: []string{placementGroup}}
}

// NewNoScaleUpNapPodGpuMinCpuPlatformIncompatibleMsg creates and returns a "pod requests a GPU type and min_cpu_platform which are incompatible" message.
func NewNoScaleUpNapPodGpuMinCpuPlatformIncompatibleMsg(platformName, gpuType string) *Message {
	return &Message{Id: NoScaleUpNapPodGpuMinCpuPlatformIncompatible, Params: []string{platformName, gpuType}}
}

// NewNoScaleUpNapPodConfidentialNodesIncompatibleMsg creates and returns a "Confidential Nodes are enabled, and the pod requests machines incompatible with them" message.
func NewNoScaleUpNapPodConfidentialNodesIncompatibleMsg(machineGroupName string) *Message {
	return &Message{Id: NoScaleUpNapPodConfidentialNodesIncompatible, Params: []string{machineGroupName}}
}

// NewNoScaleUpNapPodWorkloadSeparationInvalidMsg creates and returns a "pod requires a non-system label, and doesn't have a matching toleration" message.
func NewNoScaleUpNapPodWorkloadSeparationInvalidMsg(label string) *Message {
	return &Message{Id: NoScaleUpNapPodWorkloadSeparationInvalid, Params: []string{label}}
}

// NewNoScaleUpNapPodNoPSCInfrastructureMsg creates and returns a "pod specifies private node affinity in a cluster without PSC infrastructure" message.
func NewNoScaleUpNapPodNoPSCInfrastructureMsg() *Message {
	return &Message{Id: NoScaleUpNapPodNoPSCInfrastructure}
}

// NewNoScaleUpNapPodArchUnknownMsg creates and returns a "pod requests an unknown architecture" message.
func NewNoScaleUpNapPodArchUnknownMsg(archName string) *Message {
	return &Message{Id: NoScaleUpNapPodArchUnknown, Params: []string{archName}}
}

// NewNoScaleUpNapPodArchInvalidMsg creates and returns a "pod requests an invalid machines and architecture combination" message.
func NewNoScaleUpNapPodArchInvalidMsg(machineGroupName, archName string) *Message {
	return &Message{Id: NoScaleUpNapPodArchInvalid, Params: []string{machineGroupName, archName}}
}

// NewNoScaleUpNapPodMachineConfigInvalidMsg creates and returns a "pod requests a machine config that is invalid in some way not detected explicitly" message.
func NewNoScaleUpNapPodMachineConfigInvalidMsg(machineConfigDesc, errMsg string) *Message {
	return &Message{Id: NoScaleUpNapPodMachineConfigInvalid, Params: []string{machineConfigDesc, errMsg}}
}

// NewNoScaleUpNapPodUnexpectedErrorMsg creates and returns a "NAP provided an unexpected error type for the pod" message.
func NewNoScaleUpNapPodUnexpectedErrorMsg() *Message {
	return &Message{Id: NoScaleUpNapPodUnexpectedError}
}

// NewNoScaleUpNapPodGpuNoLimitDefinedMsg creates and returns a "NAP didn't provision any node group for the pod because the pod has a GPU request and the GPU doesn't have a limit defined" message.
func NewNoScaleUpNapPodGpuNoLimitDefinedMsg(gpuType string) *Message {
	return &Message{Id: NoScaleUpNapPodGpuNoLimitDefined, Params: []string{gpuType}}
}

// NewNoScaleUpNapPodGpuTypeNotSupportedMsg creates and returns a "NAP didn't provision any node group for the pod because it specifies a GPU which isn't supported" message.
func NewNoScaleUpNapPodGpuTypeNotSupportedMsg(gpuType string) *Message {
	return &Message{Id: NoScaleUpNapPodGpuTypeNotSupported, Params: []string{gpuType}}
}

// NewNoScaleUpNapPodGpuRequestInvalidMsg creates and returns a "NAP didn't provision any node group for the pod because its GPU request is invalid" message.
func NewNoScaleUpNapPodGpuRequestInvalidMsg(reason string) *Message {
	return &Message{Id: NoScaleUpNapPodGpuRequestInvalid, Params: []string{reason}}
}

// NewNoScaleUpNapPodGpuFailingPredicatesMsg creates and returns a "NAP didn't provision any node group for the pod because it wouldn't pass scheduler predicates in any of the injected node groups" message.
func NewNoScaleUpNapPodGpuFailingPredicatesMsg(predicatesFailureReasons []string) *Message {
	return &Message{Id: NoScaleUpNapPodGpuFailingPredicates, Params: predicatesFailureReasons}
}

// NewNoScaleUpNapPodTpuIncompatibleMsg creates and returns a "pod requests an incompatible machines and TPU type combination" message
func NewNoScaleUpNapPodTpuIncompatibleMsg(machineGroupName, tpuType string) *Message {
	return &Message{Id: NoScaleUpNapPodTpuIncompatible, Params: []string{machineGroupName, tpuType}}
}

// NewNoScaleUpNapPodTpuTypeNotSupportedMsg creates and returns a "NAP didn't provision any node group for the pod because it specifies a TPU which isn't supported" message.
func NewNoScaleUpNapPodTpuTypeNotSupportedMsg(tpuType string) *Message {
	return &Message{Id: NoScaleUpNapPodTpuTypeNotSupported, Params: []string{tpuType}}
}

// NewNoScaleUpNapPodTpuNoLimitDefinedMsg creates and returns a "NAP didn't provision any node group for the pod because the pod has a TPU request and the TPU doesn't have a limit defined" message.
func NewNoScaleUpNapPodTpuNoLimitDefinedMsg(tpuType string) *Message {
	return &Message{Id: NoScaleUpNapPodTpuNoLimitDefined, Params: []string{tpuType}}
}

func NewNoScaleUpNapPodTpuAcceleratorCountInvalid(tpuType string, acceleratorCount int) *Message {
	return &Message{Id: NoScaleUpNapPodTpuAcceleratorCountInvalid, Params: []string{tpuType, fmt.Sprintf("%v", acceleratorCount)}}
}

// NewNoScaleUpNapPodZonalResourcesExceededMsg creates and returns a "NAP didn't provision any node group for the pod because some resources would exceeded" message.
func NewNoScaleUpNapPodZonalResourcesExceededMsg(zone string) *Message {
	return &Message{Id: NoScaleUpNapPodZonalResourcesExceeded, Params: []string{zone}}
}

// NewNoScaleUpNapPodZonalUnexpectedErrorMsg creates and returns a "NAP didn't provision any node group for the pod because of an unexpected error" message.
func NewNoScaleUpNapPodZonalUnexpectedErrorMsg(zone string) *Message {
	return &Message{Id: NoScaleUpNapPodZonalUnexpectedError, Params: []string{zone}}
}

// NewNoScaleUpNapPodZonalIllegalConfigMsg creates and returns a "NAP didn't provision any node group for the pod because it requests an illegal MIG configuration" message.
func NewNoScaleUpNapPodZonalIllegalConfigMsg(zone string) *Message {
	return &Message{Id: NoScaleUpNapPodZonalIllegalConfig, Params: []string{zone}}
}

// NewNoScaleUpNapPodZonalFailingPredicatesMsg creates and returns a "NAP didn't provision any node group for the pod because of failing predicates" message.
func NewNoScaleUpNapPodZonalFailingPredicatesMsg(zone string, predicatesFailureReasons []string) *Message {
	return &Message{Id: NoScaleUpNapPodZonalFailingPredicates, Params: append([]string{zone}, predicatesFailureReasons...)}
}

// NewNoScaleUpNapPodZonalOtherErrorMsg creates and returns a "NAP didn't provision any node group for the pod because of an other, unspecified error" message.
func NewNoScaleUpNapPodZonalOtherErrorMsg(zone string) *Message {
	return &Message{Id: NoScaleUpNapPodZonalOtherError, Params: []string{zone}}
}

// NewNoScaleUpNapExtendedDurationPodCPUReqInvalid creates and returns a "NAP didn't provision any node group for the pod because it specifies incorrect extended duration pod cpu request value" message.
func NewNoScaleUpNapExtendedDurationPodCPUReqInvalid(cpuReq string) *Message {
	return &Message{Id: NoScaleUpNapExtendedDurationPodCPUReqInvalid, Params: []string{cpuReq}}
}

// NewNoScaleUpNapExtendedDurationPodNonAutopilotError creates and returns a "NAP didn't provision any node group for the pod because extended duration pods aren't supported outside of autopilot clusters" message.
func NewNoScaleUpNapExtendedDurationPodNonAutopilotError() *Message {
	return &Message{Id: NoScaleUpNapExtendedDurationPodNonAutopilotError}
}

// NewNoScaleUpNapNpcFetchingErr creates and returns a "NAP didn't provision any node group for the pod because it failed to fetch NPC." message.
func NewNoScaleUpNapNpcFetchingErr(npcName string) *Message {
	return &Message{Id: NoScaleUpNapNpcFetchError, Params: []string{npcName}}
}

// NewNoScaleUpNapCccFetchingErr creates and returns a "NAP didn't provision any node group for the pod because it failed to fetch CCC." message.
func NewNoScaleUpNapCccFetchingErr(cccName string) *Message {
	return &Message{Id: NoScaleUpNapCccFetchError, Params: []string{cccName}}
}

// NewNoScaleUpNapNpcNotFound creates and returns a "NAP didn't provision any node group for the pod because of pod requesting non-existing NPC." message.
func NewNoScaleUpNapNpcNotFound(npcName, reason string) *Message {
	if reason != "" {
		return &Message{Id: NoScaleUpNapNpcNotFound, Params: []string{npcName, reason}}
	}
	return &Message{Id: NoScaleUpNapNpcNotFound, Params: []string{npcName}}
}

// NewNoScaleUpNapCccNotFound creates and returns a "NAP didn't provision any node group for the pod because of pod requesting non-existing CCC." message.
func NewNoScaleUpNapCccNotFound(cccName, reason string) *Message {
	if reason != "" {
		return &Message{Id: NoScaleUpNapCccNotFound, Params: []string{cccName, reason}}
	}
	return &Message{Id: NoScaleUpNapCccNotFound, Params: []string{cccName}}
}

// NewNoScaleUpNapNpcAutoprovisioningDisabled creates and returns a "NAP is disabled" message.
func NewNoScaleUpNapNpcAutoprovisioningDisabled(npcName string) *Message {
	return &Message{Id: NoScaleUpNapNpcAutoprovisioningDisabled, Params: []string{npcName}}
}

// NewNoScaleUpNapCccAutoprovisioningDisabled creates and returns a "NAP is disabled" message.
func NewNoScaleUpNapCccAutoprovisioningDisabled(cccName string) *Message {
	return &Message{Id: NoScaleUpNapCccAutoprovisioningDisabled, Params: []string{cccName}}
}

// NewNoScaleUpNapNpcPodIncompatible creates and returns a "NAP didn't provision any node group for the pod because pod being incompatible with NPC." message.
func NewNoScaleUpNapNpcPodIncompatible(npcName string) *Message {
	return &Message{Id: NoScaleUpNapNpcPodIncompatible, Params: []string{npcName}}
}

// NewNoScaleUpNapCccPodIncompatible creates and returns a "NAP didn't provision any node group for the pod because pod being incompatible with CCC." message.
func NewNoScaleUpNapCccPodIncompatible(cccName string) *Message {
	return &Message{Id: NoScaleUpNapCccPodIncompatible, Params: []string{cccName}}
}

// NewNoScaleUpNapNpcCccBothDefined creates and returns a "NAP didn't provision any node group for the pod because pod has both NPC and CCC configured" message.
func NewNoScaleUpNapNpcCccBothDefined() *Message {
	return &Message{Id: NoScaleUpNapNpcCccBothDefined, Params: []string{}}
}

// NewNoScaleUpNapIsolatedPodCPUReqInvalid creates and returns a "NAP didn't provision any node group for the pod because it specifies incorrect isolated pod cpu request value" message.
func NewNoScaleUpNapIsolatedPodCPUReqInvalid(cpuReq string) *Message {
	return &Message{Id: NoScaleUpNapIsolatedPodCPUReqInvalid, Params: []string{cpuReq}}
}

// NewNoScaleUpNapIsolatedPodCapacityInvalid creates and returns a "NAP didn't provision any node group for the pod because it specifies incorrect isolated pod capacity value" message.
func NewNoScaleUpNapIsolatedPodCapacityInvalid(podCap string) *Message {
	return &Message{Id: NoScaleUpNapIsolatedPodCapacityInvalid, Params: []string{podCap}}
}

// NewNoScaleUpNapIsolatedPodNonAutopilotError creates and returns a "NAP didn't provision any node group for the pod because isolated pods aren't supported outside of autopilot clusters" message.
func NewNoScaleUpNapIsolatedPodNonAutopilotError() *Message {
	return &Message{Id: NoScaleUpNapIsolatedPodNonAutopilotError, Params: []string{}}
}

// NewNoScaleUpNapFlexStartMisconfiguredError creates and returns a "NAP didn't provision any node group for the pod because of Flex Start node selectors misconfiguration" message.
func NewNoScaleUpNapFlexStartMisconfiguredError(reason string) *Message {
	return &Message{Id: NoScaleUpNapFlexStartMisconfiguredError, Params: []string{reason}}
}

// NewNoScaleUpNapInvalidConfidentialNodeType creates and returns a "NAP didn't provision any node group for the pod because the confidential node type is invalid" message.
func NewNoScaleUpNapInvalidConfidentialNodeType(confidentialNodeType string) *Message {
	return &Message{Id: NoScaleUpNapInvalidConfidentialNodeType, Params: []string{confidentialNodeType}}
}

func NewNoScaleUpNapInvalidMachineFamilyForConfidentialNodeType(reason string) *Message {
	return &Message{Id: NoScaleUpNapInvalidMachineFamilyForConfidentialNodeType, Params: []string{reason}}
}

func NewNoScaleUpNapPodMachineFamiliesDoNotSupportDws(machineFamilies []string) *Message {
	return &Message{Id: NoScaleUpNapPodMachineFamiliesDoNotSupportDws, Params: machineFamilies}
}

func NewNoScaleUpNapPodUnusableReservation(reason string, path string) *Message {
	return &Message{Id: NoScaleUpNapPodUnusableReservation, Params: []string{reason, path}}
}

func NewNoScaleUpNapPodInvalidPlacementPolicy(reason string, policyName string) *Message {
	return &Message{Id: NoScaleUpNapPodInvalidPlacementPolicy, Params: []string{reason}}
}

func NewNoScaleUpNapPodMissingAIZones(message string) *Message {
	return &Message{Id: NoScaleUpNapPodMissingAIZones, Params: []string{message}}
}

// NewNoScaleDownUnexpectedErrorMsg creates and returns a "no scale-down because of an unexpected error" message.
func NewNoScaleDownUnexpectedErrorMsg() *Message {
	return &Message{Id: NoScaleDownUnexpectedError}
}

// NewNoScaleDownNotTriedMsg creates and returns a "no scale-down because the scale-down wasn't even attempted" message.
func NewNoScaleDownNotTriedMsg() *Message {
	return &Message{Id: NoScaleDownNotTried}
}

// NewNoScaleDownInBackoffMsg creates and returns a "no scale-down because scaling-down is in a backoff period" message.
func NewNoScaleDownInBackoffMsg() *Message {
	return &Message{Id: NoScaleDownInBackoff}
}

// NewNoScaleDownInProgressMsg creates and returns a "no scale-down because a previous scale-down was still in progress" message.
func NewNoScaleDownInProgressMsg() *Message {
	return &Message{Id: NoScaleDownInProgress}
}

// NewNoScaleDownNodeScaleDownDisabledAnnotationMsg creates and returns a "node can't be removed because it has a 'scale down disabled' annotation" message.
func NewNoScaleDownNodeScaleDownDisabledAnnotationMsg() *Message {
	return &Message{Id: NoScaleDownNodeScaleDownDisabledAnnotation}
}

// NewNoScaleDownNodeNodeGroupMinSizeReachedMsg creates and returns a "node can't be removed because its node group is at its minimal size already" message;
func NewNoScaleDownNodeNodeGroupMinSizeReachedMsg() *Message {
	return &Message{Id: NoScaleDownNodeNodeGroupMinSizeReached}
}

// NewNoScaleDownNodeMinimalResourceLimitsExceededMsg creates and returns a "node can't be removed because it would violate cluster-wide minimal resource limits" message;
func NewNoScaleDownNodeMinimalResourceLimitsExceededMsg() *Message {
	return &Message{Id: NoScaleDownNodeMinimalResourceLimitsExceeded}
}

// NewNoScaleDownNodeNoPlaceToMovePodsMsg creates and returns a "node can't be removed because there's no place to move its pods to" message.
func NewNoScaleDownNodeNoPlaceToMovePodsMsg() *Message {
	return &Message{Id: NoScaleDownNodeNoPlaceToMovePods}
}

// NewNoScaleDownNodeUnexpectedErrorMsg creates and returns a "node can't be removed because of an unexpected error" message.
func NewNoScaleDownNodeUnexpectedErrorMsg() *Message {
	return &Message{Id: NoScaleDownNodeUnexpectedError}
}

// NewNoScaleDownNodePodControllerNotFoundMsg creates and returns a "pod is blocking scale down because its controller can't be found" message.
func NewNoScaleDownNodePodControllerNotFoundMsg(podName string) *Message {
	return &Message{Id: NoScaleDownNodePodControllerNotFound, Params: []string{podName}}
}

// NewNoScaleDownNodePodMinReplicasReachedMsg creates and returns a "pod is blocking scale down because its controller already has the minimum number of replicas" message.
func NewNoScaleDownNodePodMinReplicasReachedMsg(podName string) *Message {
	return &Message{Id: NoScaleDownNodePodMinReplicasReached, Params: []string{podName}}
}

// NewNoScaleDownNodePodNotBackedByControllerMsg creates and returns a "pod is blocking scale down because it's not backed by a controller" message.
func NewNoScaleDownNodePodNotBackedByControllerMsg(podName string) *Message {
	return &Message{Id: NoScaleDownNodePodNotBackedByController, Params: []string{podName}}
}

// NewNoScaleDownNodePodHasLocalStorageMsg creates and returns a "pod is blocking scale down because it has local storage" message.
func NewNoScaleDownNodePodHasLocalStorageMsg(podName string) *Message {
	return &Message{Id: NoScaleDownNodePodHasLocalStorage, Params: []string{podName}}
}

// NewNoScaleDownNodePodNotSafeToEvictAnnotationMsg creates and returns a "pod is blocking scale down because it has a "not safe to evict" annotation" message.
func NewNoScaleDownNodePodNotSafeToEvictAnnotationMsg(podName string) *Message {
	return &Message{Id: NoScaleDownNodePodNotSafeToEvictAnnotation, Params: []string{podName}}
}

// NewNoScaleDownNodePodKubeSystemUnmovableMsg creates and returns a "pod is blocking scale down because it's a non-daemonset, non-mirrored, non-pdb-assigned kube-system pod" message.
func NewNoScaleDownNodePodKubeSystemUnmovableMsg(podName string) *Message {
	return &Message{Id: NoScaleDownNodePodKubeSystemUnmovable, Params: []string{podName}}
}

// NewNoScaleDownNodePodNotEnoughPdbMsg creates and returns a "pod is blocking scale down because it doesn't have enough PDB left" message.
func NewNoScaleDownNodePodNotEnoughPdbMsg(podName string) *Message {
	return &Message{Id: NoScaleDownNodePodNotEnoughPdb, Params: []string{podName}}
}

// NewNoScaleDownNodePodUnexpectedErrorMsg creates and returns a "pod is blocking scale down because of an unexpected error" message.
func NewNoScaleDownNodePodUnexpectedErrorMsg(podName string) *Message {
	return &Message{Id: NoScaleDownNodePodUnexpectedError, Params: []string{podName}}
}
