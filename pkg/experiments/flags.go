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

package experiments

const ProvisioningRequestsRLAEnabledFlag = "ProvisioningRequest::RecommendLocationsMinCAVersion"
const ProvisioningRequestsScaleDownUnreadyFlag = "ProvisioningRequest::ScaleDownUnreadyTimeSeconds"
const ProvisioningRequestsZeroOrMaxRoundUpEnabledFlag = "ProvisioningRequest::ZeroOrMaxRoundUpMinCAVersion"

const ResourcePolicyPullerFlag = "ResourcePolicyPuller::MinCAVersion"

const ProvisioningRequestBulkMigsFlag = "ProvisioningRequestBulkMigs::MinCAVersion"
const ProvisioningRequestMultiplePodSetsMinCAVersionFlag = "ProvisioningRequestMultiplePodSets::MinCAVersion"
const ProvisioningRequestMultiplePodSetsEnabledFlag = "ProvisioningRequestMultiplePodSets::Enabled"

const ProvisioningRequestObtainabilityStrategyMinCAVersionFlag = "ProvisioningRequestObtainabilityStrategy::MinCAVersion"
const ProvisioningRequestObtainabilityStrategyEnabledFlag = "ProvisioningRequestObtainabilityStrategy::Enabled"

const ProvisioningRequestAcceptedUpdatesPerLoopLimitFlag = "ProvisioningRequest::AcceptedUpdatesPerLoopLimit"

const FlexStartNonQueuedEnabledFlag = "FlexStartNonQueued::EnabledMinCAVersion"
const FlexStartNonQueuedNAPEnabledFlag = "FlexStartNonQueuedNAP::NAPMinCAVersion"
const FlexStartNonQueuedBulkMigsFlag = "FlexStartNonQueuedBulkMigs::MinCAVersion"
const FlexStartNonQueuedIgnoreStockoutErrorsEnabledFlag = "FlexStartNonQueuedIgnoreBulkStockout::EnabledMinCAVersion"
const FlexStartNonQueuedTrickleModeMinCAVersionFlag = "FlexStartNonQueuedTrickleMode::MinCAVersion"
const FlexStartNonQueuedTrickleModeEnabledFlag = "FlexStartNonQueuedTrickleMode::Enabled"

const RecommendLocationsDisabledForTPUFlag = "RecommendLocations::DisabledForTPUMinCAVersion"
const RecommendLocationsResourcePolicyFlag = "RecommendLocationsResourcePolicy::EnabledMinCAVersion"
const RecommendLocationsFlexAddInstancesFlag = "RecommendLocationsFlexAddInstances::EnabledMinCAVersion"

const RelaxedNodeGroupCreationPenalty = "RelaxedNodeGroupCreationPenalty::MinCAVersion"

const ReservationSubblocksTargetingEnabledFlag = "ReservationSubblocksTargeting::EnabledMinCAVersion"

const CapacityCheckWaitTimeSecondsFlexStartEnabledFlag = "CapacityCheckWaitTimeSecondsFlexStart::EnabledMinCAVersion"
const CapacityCheckWaitTimeSecondsMultiHostTpuEnabledFlag = "CapacityCheckWaitTimeSecondsMultiHostTpu::EnabledMinCAVersion"
const CapacityCheckWaitTimeSecondsDefaultValueGpuFlag = "CapacityCheckWaitTimeSecondsGPU::DefaultValue"
const CapacityCheckWaitTimeSecondsNonFlexValueMultiHostTpuFlag = "CapacityCheckWaitTimeSecondsMultiHostTpu::NonFlexValue"
const CapacityCheckWaitTimeSecondsFlexValueMultiHostTpuFlag = "CapacityCheckWaitTimeSecondsMultiHostTpu::FlexValue"
const NodeProvisionTimeSingleHostTPU = "NodeProvisionTime::SingleHostTPU"

const EkLookaheadPodsV1Flag = "AutopilotEk::LookaheadPodsV1"
const EkLookaheadMaxWorkloadSeparationsFlag = "AutopilotEk::LookaheadMaxWorkloadSeparations"
const ConsumableReservationExperimentName = "ClusterAutoscaler::UseConsumableReservationsApi"
const EkSpotFlag = "ClusterAutoscaler::EkSpotEnabled"
const EnableEkEdpMinGKEVersionFlag = "AutopilotExtendedDuration::EnableCAMinGKEVersion"
const EkMachineTypesFlag = "AutopilotEk::MachineTypes"
const EkUasUpsizabilityBufferFlag = "AutopilotEk::UasUpsizabilityBuffer"
const EkDPV1SupportedFlag = "AutopilotEk::DPV1Supported"
const EkForcefulMigrationFromEkToE2MinCAVersionFlag = "AutopilotEk::ForcefulMigrationFromEkToE2MinCAVersion"
const EkDownsizeConfigFlag = "AutopilotEk::DownsizeConfig"
const EkDownsizeNonResizableFlag = "AutopilotEk::DownsizeNonResizableEk"
const EKOnManagedNodesMinCAVersionFlag = "EKOnManagedNodes::MinCaVersion"
const ResizableClusterBackoffCustomThresholdsPerErrorTypeFlag = "EkClusterBackoff::CustomThresholdsPerErrorType"

const PodTopologySpreadCCCMinCAVersionFlag = "PodTopologySpreadCCC::MinCAVersion"
const PodTopologySpreadNodeBasedMinCAVersionFlag = "PodTopologySpreadNodeBased::MinCAVersion"

const ComputeClassMinCapacityEnabledFlag = "ComputeClassMinCapacity::Enabled"
const ComputeClassMinCapacityMinCAVersionFlag = "ComputeClassMinCapacity::MinCAVersion"

const MultitenancyScaleToZeroProcessorFlag = "Multitenancy::EnablePerTenantScaleToZero"
const MultitenancyEnablePerTenantP4SAFlag = "Multitenancy::EnablePerTenantP4SAInClusterAutoscaler"
const MultitenancyEnableLazyReservationGCEClientFlag = "Multitenancy::EnableLazyReservationGCEClient"
const MultitenancyKCPEnableServerAcceptNetworkApiFieldFlag = "Multitenancy::KCPEnableServerAcceptNetworkApiField"

const NodePoolConsolidationDelayMinCAVersionFlag = "NodePoolConsolidationDelay::MinCAVersion"

const ScaleToZeroLateRunFlag = "ScaleToZero::LateRun" // Direct launch.

const CapacityBuffersPrivatePreviewMinCAVersion = "CapacityBuffersPrivatePreview::EnabledMinCAVersion"
const CapacityBuffersMinCAVersion = "CapacityBuffers::MinCAVersion"
const CapacityBuffersEnabled = "CapacityBuffers::Enabled"

const CapacityBuffersMetricProcessor = "CapacityBuffers::MetricProcessor" // Direct launch.

const ColdStandbyNodesInternalMinCAVersionFlag = "ColdStandbyNodes::InternalMinCAVersion"
const ColdStandbyNodesControllerConfigV1Flag = "ColdStandbyNodes::NodeControllerConfigV1"
const ColdStandbyNodesMinCAVersionFlag = "ColdStandbyNodes::MinCAVersion"
const ColdStandbyNodesAutopilotSoHWFlag = "ColdStandbyNodes::AutopilotSoHWMinCAVersion"                       // CSN in Autopilot on Slice of Hardware (SoHW)
const ColdStandbyNodesPreventCSNScaleUpForNonCSNPodsFlag = "ColdStandbyNodes::PreventCSNScaleUpForNonCSNPods" // Direct launch.
const ColdStandbyNodesCheckPodsOnSuspendedNodes = "ColdStandbyNodes::CheckPodsOnSuspendedNodes"               // Direct launch.
const ColdStandbyNodesProcessTemplateNodeInfosFlag = "ColdStandbyNodes::ProcessTemplateNodeInfos"             // Direct launch.
const ColdStandbyNodesMinCAVersionGuardForCAFlag = "ColdStandbyNodes::MinCAVersionGuardForCAFlag"             // Direct launch style flag.
const ColdStandbyNodesWaitForInstanceStatus = "ColdStandbyNodes::WaitForInstanceStatus"                       // Direct launch.

const SliceOfHardwareReservationSteerLocalSSDFlag = "AutopilotSliceOfHardware::ReservationSteerLocalSSD"
const SliceOfHardwareReservationSteerLocalSSD2Flag = "AutopilotSliceOfHardware::ReservationSteerLocalSSD2"

const DraMinCAVersionFlag = "DRA::MinCAVersion"
const DraEnabledFlag = "DRA::Enabled"

const HtnapEnabledFlag = "HTNAP::Enabled"
const HtnapMinCAVersionFlag = "HTNAP::MinCAVersion"

const IncreasedNapMaxNodesEnabledFlag = "IncreasedNapMaxNodes::Enabled"
const IncreasedNapMaxNodesMinCAVersionFlag = "IncreasedNapMaxNodes::MinCAVersion"

const EnablePartialDefragFlag = "ClusterAutoscalerDefrag::EnablePartialDefrag"
const EkPreventScheduleOnLookaheadNodes = "AutopilotEk::PreventScheduleOnLookaheadNodes"

const AutopilotE4MinVersionFlag = "AutopilotE4::MinCAVersion"
const AutopilotE4NoResizeEnabledFlag = "AutopilotE4::NoResizeEnabled"

const AutopilotE4ExtendedFallbacksMinCAVersionFlag = "AutopilotE4ExtendedFallbacks::MinCAVersion"
const E4OnManagedNodesMinCAVersionFlag = "E4OnManagedNodes::MinCaVersion"

const AutopilotE4aNoResizeEnabledFlag = "AutopilotE4A::EnableAllowlistFeature"
const AutopilotE4aNoResizeMinVersionFlag = "AutopilotE4A::MinCAVersion"
const AutopilotE4aWithResizeEnabledFlag = "AutopilotE4A::CoarseGrainedResizeEnabled"
const AutopilotE4aWithResizeMinVersionFlag = "AutopilotE4A::CoarseGrainedResizeMinCAVersion"
const E4aMachineTypesFlag = "AutopilotE4A::MachineTypes"
const E4aDownsizeConfigFlag = "AutopilotE4A::DownsizeConfig"
const E4aUasUpsizabilityBufferFlag = "AutopilotE4A::UasUpsizabilityBuffer"
const E4AOnManagedNodesMinCAVersionFlag = "E4AOnManagedNodes::MinCaVersion"
const E4AOnManagedNodesEnabledFlag = "E4AOnManagedNodes::Enabled"

const AutopilotArmMachineFallbacksMinCAVersionFlag = "AutopilotArmPodFamilyMachineFallbacks::MinCAVersion"
const AutopilotArmMachineFallbacksEnabledFlag = "AutopilotArmPodFamilyMachineFallbacks::Enabled"

const ZoneTypesEnabledFlag = "ZoneTypes::Enabled"
const ZoneTypesMinCAVersionFlag = "ZoneTypes::MinCAVersion"

const FastpathBinpackingEnabledFlag = "FastpathBinpacking::Enabled"
const FastpathBinpackingMinCAVersionFlag = "FastpathBinpacking::MinCAVersion"

const IncreasedMaxNodesPerScaleUpEnabledFlag = "IncreasedMaxNodesPerScaleUp::Enabled"
const IncreasedMaxNodesPerScaleUpMinCAVersionFlag = "IncreasedMaxNodesPerScaleUp::MinCAVersion"

const FlexAdvisorDWSEnabledFlag = "FlexAdvisorDWS::Enabled"
const FlexAdvisorDWSMinCAVersionFlag = "FlexAdvisorDWS::MinCAVersion"

const FlexAdvisorTPUEnabledFlag = "FlexAdvisorTPU::Enabled"
const FlexAdvisorTPUMinCAVersionFlag = "FlexAdvisorTPU::MinCAVersion"

const FlexAdvisorZoneTypesEnabledFlag = "FlexAdvisorZoneTypes::Enabled"
const FlexAdvisorZoneTypesMinCAVersionFlag = "FlexAdvisorZoneTypes::MinCAVersion"
const FlexAdvisorMinCpuPlatformEnabledFlag = "FlexAdvisorMinCpuPlatform::Enabled"
const FlexAdvisorMinCpuPlatformMinCAVersionFlag = "FlexAdvisorMinCpuPlatform::MinCAVersion"

const FlexAdvisorPCCSupportEnabledFlag = "FlexAdvisorPCCSupport::Enabled"
const FlexAdvisorPCCSupportMinCAVersionFlag = "FlexAdvisorPCCSupport::MinCAVersion"

const FlexAdvisorProcessingEnabledFlag = "FlexAdvisorProcessing::Enabled"
const FlexAdvisorProcessingMinCAVersionFlag = "FlexAdvisorProcessing::MinCAVersion"
const FlexAdvisorLateRegistrationEnabledFlag = "FlexAdvisorLateRegistration::Enabled"
const FlexAdvisorLateRegistrationMinCAVersionFlag = "FlexAdvisorLateRegistration::MinCAVersion"

const FlexAdvisorEnableDebugLogsFlag = "FlexAdvisor::EnableDebugLogs"

const FlexAdvisorAwaitInstanceAvailabilityTimeoutSecondsFlag = "FlexAdvisor::AwaitInstanceAvailabilityTimeoutSeconds"

const FlexAdvisorMaxActiveScopes = "FlexAdvisor::MaxActiveScopes"

const FlexAdvisorGeneratorMachineErrorsCacheEnabledFlag = "FlexAdvisorGeneratorMachineErrorsCache::Enabled"
const FlexAdvisorGeneratorMachineErrorsCacheMinCAVersionFlag = "FlexAdvisorGeneratorMachineErrorsCache::MinCAVersion"

const SalvoScaleUpEnabledFlag = "SalvoScaleUp::Enabled"
const SalvoScaleUpMinCAVersionFlag = "SalvoScaleUp::MinCAVersion"
const SalvoScaleUpBudgetSecondsFlag = "SalvoScaleUp::BudgetSeconds"

const AnyThenFailReservationAffinityThresholdEnabledFlag = "AnyThenFailReservationAffinityThreshold::Enabled"

const FleetEfficiencyStrategyEnabledFlag = "FleetEfficiencyStrategy::Enabled"
const FleetEfficiencyStrategyMinCAVersionFlag = "FleetEfficiencyStrategy::MinCAVersion"

const ClusterDefaultAllocationStrategyFlag = "AllocationStrategy::DefaultStrategy"

const SimshipAutomationApplyCRDMinCAVersionFlag = "SimshipAutomation::ApplyCRDMinCAVersion"
const SimshipAutomationApplyCRDEnabledFlag = "SimshipAutomation::ApplyCRDEnabled"
const SimshipAutomationBigRedButtonFlag = "SimshipAutomation::BigRedButton"

const DemandFungibilityImpactTrackingEnabledFlag = "DemandFungibilityImpactTracking::Enabled"
const DemandFungibilityImpactTrackingMinCAVersionFlag = "DemandFungibilityImpactTracking::MinCAVersion"

const ScaleUpSimulationForSkippedNodeGroupsMinCAVersionFlag = "ScaleUpSimulationForSkippedNodeGroups::MinCAVersion"
const ScaleUpSimulationForSkippedNodeGroupsEnabledFlag = "ScaleUpSimulationForSkippedNodeGroups::Enabled"

const DaemonSetMutationEnabledFlag = "DaemonSetMutation::Enabled"
const DaemonSetMutationMinCAVersionFlag = "DaemonSetMutation::MinCAVersion"
