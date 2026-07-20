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

package ccc

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/selfservice"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/client"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/klog/v2"
)

const (
	// CrdType is the CCC CRD type name
	CrdType           = "CCC"
	scaleUpAnyway     = "ScaleUpAnyway"
	doNotScaleUp      = "DoNotScaleUp"
	clientCallTimeout = 5 * time.Second
)

type cccCrd struct {
	*v1.ComputeClass

	projectId        string
	autopilotEnabled bool

	rulesOnce     sync.Once
	computedRules []rules.Rule

	groupedOnce     sync.Once
	computedGrouped [][]rules.Rule

	optionsTracker *optstracking.OptionsTracker
	provider       DataProvider
}

// DataProvider defines GKE cloud provider methods required by compute class CRD.
type DataProvider interface {
	MachineConfigProvider() *machinetypes.MachineConfigProvider
	GetAIZones() ([]string, error)
	GetStandardZones() ([]string, error)
	GetAutoprovisioningLocations() []string
	TrimLocationsForMachineConfig(locations []string, machineType string, acceleratorConfig *gke_api_beta.AcceleratorConfig, minCpuPlatform string, diskType string) []string
}

// NewCccCrd returns new crd based on ComputeClass CRD
func NewCccCrd(ccc *v1.ComputeClass, projectId string, autopilotEnabled bool, provider DataProvider, optionsTracker *optstracking.OptionsTracker) crd.CRD {
	return &cccCrd{
		ComputeClass:     ccc,
		projectId:        projectId,
		autopilotEnabled: autopilotEnabled,
		optionsTracker:   optionsTracker,
		provider:         provider,
	}
}

// ResourceVersion returns resource version
func (ccc *cccCrd) ResourceVersion() string {
	return ccc.ComputeClass.ResourceVersion
}

// Label is the node label used for specifying the CCC.
func (ccc *cccCrd) Label() string {
	return gkelabels.ComputeClassLabel
}

// CrdType returns the name of the wantCrd type (CCC for cccCrd).
func (ccc *cccCrd) CrdType() string {
	return CrdType
}

// Name returns the name of the CCC.
func (ccc *cccCrd) Name() string {
	if ccc == nil || ccc.ComputeClass == nil {
		return ""
	}
	return ccc.ComputeClass.Name
}

// Rules returns list of CCC rules, computed lazily.
func (ccc *cccCrd) Rules() []rules.Rule {
	ccc.rulesOnce.Do(func() {
		if len(ccc.priorities()) == 0 {
			ccc.computedRules = nil
			return
		}
		var result []rules.Rule
		for idx, p := range ccc.priorities() {
			rule := ccc.buildRuleFromPriority(p, idx)
			result = append(result, rule)
		}
		ccc.computedRules = result
	})
	return ccc.computedRules
}

// GroupedRules returns list of CCC rules grouped by priorityScore, computed lazily.
func (ccc *cccCrd) GroupedRules() [][]rules.Rule {
	ccc.groupedOnce.Do(func() {
		if ccc.priorities() == nil {
			ccc.computedGrouped = nil
			return
		}

		priorityScoreBased := ccc.withPriorityScore()

		if !priorityScoreBased {
			// Not priorityScore based, each priority is its own group.
			var groupedRules [][]rules.Rule
			for _, rule := range ccc.Rules() {
				groupedRules = append(groupedRules, []rules.Rule{rule})
			}
			ccc.computedGrouped = groupedRules
			return
		}

		// PriorityScore based, group by priorityScore.
		rulesByPriorityScore := make(map[int][]rules.Rule)
		for idx, p := range ccc.priorities() {
			rule := ccc.buildRuleFromPriority(p, idx)
			if p.PriorityScore == nil {
				// this should never happen as it is already checked in withPriorityScore().
				continue
			}
			rulesByPriorityScore[*p.PriorityScore] = append(rulesByPriorityScore[*p.PriorityScore], rule)
		}
		var priorityScores []int
		for priorityScore := range rulesByPriorityScore {
			priorityScores = append(priorityScores, priorityScore)
		}
		sort.Slice(priorityScores, func(i, j int) bool {
			return priorityScores[i] > priorityScores[j] // Sort in descending order.
		})
		var groupedRules [][]rules.Rule
		for _, priorityScore := range priorityScores {
			groupedRules = append(groupedRules, rulesByPriorityScore[priorityScore])
		}
		ccc.computedGrouped = groupedRules
	})
	return ccc.computedGrouped
}

// withPriorityScore returns true if all the priorities in CCC are priorityScore based.
func (ccc *cccCrd) withPriorityScore() bool {
	if ccc.priorities() == nil || len(ccc.priorities()) == 0 {
		return false
	}

	priorityScoreCount := 0

	for _, p := range ccc.priorities() {
		if p.PriorityScore != nil {
			priorityScoreCount += 1
		}
	}

	if priorityScoreCount == len(ccc.priorities()) {
		return true
	}

	if priorityScoreCount > 0 && priorityScoreCount < len(ccc.priorities()) {
		klog.Errorf("Found mixed priorities (priorities with priorityScore and without priorityScore) in CCC %s. This should never happen. Conidering this CCC as index based.", ccc.Name())
		return false
	}

	return false
}

// buildRuleFromPriority builds Rule from CCC priority.
func (ccc *cccCrd) buildRuleFromPriority(p v1.Priority, idx int) rules.Rule {
	if p.Nodepools != nil {
		return rules.NewRule(rules.WithNodePoolsRule(p.Nodepools), rules.WithAllocationStrategyRule(p.AllocationStrategy))
	}

	generalPurposeFamilies := ccc.getGeneralPurposeFamilies(p.PodFamily)

	ruleOpts := []rules.RuleOption{
		rules.WithMachineFamilyRule(p.MachineFamily),
		rules.WithSpotRule(p.Spot),
		rules.WithMinCoresRule(p.MinCores),
		rules.WithMinMemoryGbRule(p.MinMemoryGb),
		rules.WithMachineTypeRule(p.MachineType),
		rules.WithMaxPodsPerNodeRule(p.MaxPodsPerNode),
		rules.WithPodFamilyRule(p.PodFamily, generalPurposeFamilies...),
		rules.WithMinCpuPlatformRule(p.MinCpuPlatform),
		rules.WithLabelsRule(p.NodeLabels),
		rules.WithAllocationStrategyRule(p.AllocationStrategy),
	}

	var taints []apiv1.Taint
	for _, t := range p.Taints {
		taints = append(taints, apiv1.Taint{
			Key:    t.Key,
			Value:  t.Value,
			Effect: apiv1.TaintEffect(t.Effect),
		})
	}
	ruleOpts = append(ruleOpts, rules.WithTaintsRule(taints))

	if ccc.AutopilotManaged() || ccc.autopilotEnabled {
		ruleOpts = append(ruleOpts, rules.WithAutopilotModeRule())
	}

	gpuRequest := ccc.getGPURequest(p.MachineType, p.Gpu)
	if gpuRequest != nil {
		ruleOpts = append(ruleOpts, rules.WithGpuRule(gpuRequest))
	}
	if p.Tpu != nil {
		ruleOpts = append(ruleOpts, rules.WithTpuRule(p.Tpu.Type, p.Tpu.Count, p.Tpu.Topology))
	}

	if p.MaxRunDurationSeconds != nil {
		ruleOpts = append(ruleOpts, rules.WithMaxRunDurationRule(p.MaxRunDurationSeconds))
	}

	if p.FlexStart != nil {
		ruleOpts = append(ruleOpts, rules.WithFlexStartRule(p.FlexStart.Enabled, p.FlexStart.NodeRecycling))
	}

	if p.Storage != nil {
		// TODO(go/ccc-static-local-ssd): Add data cache and NVME block count.
		storageOpt := rules.WithStorageRule(
			p.Storage.BootDiskType,
			p.Storage.BootDiskSize,
			p.Storage.BootDiskKMSKey,
			p.Storage.LocalSSDCount,
		)
		ruleOpts = append(ruleOpts, storageOpt)
		ruleOpts = checkSecondaryBootDiskRules(p, ccc, ruleOpts)
	}

	if p.Location != nil {
		if p.Location.Zones != nil {
			locationOpt := rules.WithLocationRule(p.Location.Zones)
			ruleOpts = append(ruleOpts, locationOpt)
		}
		if p.Location.ZoneTypes != nil && ccc.optionsTracker.Options().ZoneTypesEnabled {
			zoneTypesOpt := rules.WithLocationZoneTypesRule(p.Location.ZoneTypes, ccc.provider)
			ruleOpts = append(ruleOpts, zoneTypesOpt)
		}
	}

	if p.Placement != nil && p.Placement.PolicyName != "" {
		ppOpt := rules.WithPlacementPolicyRule(p.Placement.PolicyName)
		ruleOpts = append(ruleOpts, ppOpt)
	}

	if p.Reservations != nil {
		if p.Reservations.Affinity == v1.SpecificAffinity {
			for _, reservation := range p.Reservations.Specific {
				blockName, subBlockName := getReservationBlockAndSubBlockName(&reservation)
				path := gceclient.ReservationRef{
					Project:      reservation.Project,
					Name:         reservation.Name,
					BlockName:    blockName,
					SubBlockName: subBlockName,
				}.RelativePath(ccc.projectId)
				reservationOpt := rules.WithReservationsRule(
					rules.NewReservation().
						WithReservationName(reservation.Name).
						WithReservationAffinity(reservations.SpecificAffinity).
						WithReservationProject(reservation.Project).
						WithReservationZones(reservation.Zones).
						WithReservationPath(path).
						WithReservationBlock(blockName).
						WithReservationSubBlock(subBlockName),
				)
				ruleOpts = append(ruleOpts, reservationOpt)
			}
		}

		if p.Reservations.Affinity == v1.AnyBestEffortAffinity {
			reservationOpt := rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.AnyAffinity))
			ruleOpts = append(ruleOpts, reservationOpt)
		}

		if p.Reservations.Affinity == v1.NoneAffinity {
			reservationOpt := rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.NoneAffinity))
			ruleOpts = append(ruleOpts, reservationOpt)
		}

		if p.Reservations.Affinity == v1.AnyThenFail {
			reservationOpt := rules.WithReservationsRule(rules.NewReservation().WithReservationAffinity(reservations.AnyThenFail))
			ruleOpts = append(ruleOpts, reservationOpt)
		}
	}

	nodeSystemConfigRuleOpts, err := ruleOptsForNodeSystemConfig(p.NodeSystemConfig)
	if err != nil {
		klog.Errorf("error generating node config rule options for node system config. CCC: %s, priority: %d, err: %v", ccc.Name(), idx+1, err)
	}
	if len(nodeSystemConfigRuleOpts) != 0 {
		ruleOpts = append(ruleOpts, nodeSystemConfigRuleOpts...)
	}

	// Add minimum capacity spec
	if p.MinimumCapacity != nil {
		ruleOpts = append(ruleOpts, rules.WithTargetNodeCountRule(p.MinimumCapacity.TargetNodeCount))
	}

	// Add self-service metadata
	if metadata := selfservice.PriorityMetadata(p); len(metadata) > 0 {
		ruleOpts = append(ruleOpts, rules.WithSelfServiceRule(metadata))
	}

	return rules.NewRule(ruleOpts...)
}

func (ccc *cccCrd) getGeneralPurposeFamilies(podFamily *string) []machinetypes.MachineFamily {
	if podFamily == nil || *podFamily != rules.GeneralPurposePodFamily {
		return nil
	}

	familyNames := ccc.optionsTracker.Options().GeneralPurposeMachineFamilies
	if len(familyNames) == 0 {
		return nil
	}

	var generalPurposeFamilies []machinetypes.MachineFamily
	for _, name := range familyNames {
		family, err := ccc.provider.MachineConfigProvider().ToMachineFamily(name)
		if err != nil {
			klog.Errorf("Unexpected invalid machine family %q (should have been validated at startup): %v", name, err)
			continue
		}
		generalPurposeFamilies = append(generalPurposeFamilies, family)
	}

	// TODO: Temporary debugging log, delete after RCA
	klog.Infof("getGeneralPurposeFamilies: generalPurposeMachineFamilies flag values: %v resolved to families: %v", familyNames, generalPurposeFamilies)
	return generalPurposeFamilies
}

// priorities returns list of priorities.
func (ccc *cccCrd) priorities() []v1.Priority {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.Priorities == nil {
		return nil
	}

	result := []v1.Priority{}
	for i := range ccc.Spec.Priorities {
		priority := ccc.Spec.Priorities[i]
		applyDefaultsToPriority(&priority, ccc.Spec.PriorityDefaults, ccc.Spec.AllocationStrategyDefaults)
		result = append(result, priority)
	}

	return result
}

// applyDefaultsToPriority applies the default field to priority if it is not present in priority.
func applyDefaultsToPriority(priority *v1.Priority, defaults *v1.PriorityDefaults, allocationStrategyDefaults *v1.AllocationStrategyDefaults) {
	if defaults != nil {
		// Check for Node System Config.
		if priority.NodeSystemConfig == nil {
			priority.NodeSystemConfig = defaults.NodeSystemConfig
		}

		if priority.Location == nil {
			priority.Location = defaults.Location
		}
	}

	if priority.AllocationStrategy == nil && allocationStrategyDefaults != nil {
		if priority.FlexStart != nil && priority.FlexStart.Enabled {
			priority.AllocationStrategy = allocationStrategyDefaults.FlexStart
		} else if priority.Spot != nil && *priority.Spot {
			priority.AllocationStrategy = allocationStrategyDefaults.Spot
		} else {
			priority.AllocationStrategy = allocationStrategyDefaults.OnDemand
		}
	}
}

func checkSecondaryBootDiskRules(priority v1.Priority, ccc *cccCrd, ruleOpts []rules.RuleOption) []rules.RuleOption {
	if priority.Storage.SecondaryBootDisks == nil {
		return ruleOpts
	}
	for _, secondaryBootDisk := range priority.Storage.SecondaryBootDisks {
		var project string
		if secondaryBootDisk.Project == nil {
			project = ccc.projectId
		} else {
			project = *secondaryBootDisk.Project
		}
		var mode string
		if secondaryBootDisk.Mode == nil {
			mode = ""
		} else {
			mode = *secondaryBootDisk.Mode
		}

		secondaryBootDiskOpt := rules.WithSecondaryBootDiskRule(
			secondaryBootDisk.DiskImageName,
			project,
			mode,
		)

		ruleOpts = append(ruleOpts, secondaryBootDiskOpt)
	}
	return ruleOpts
}

// AutoprovisioningEnabled checks if NAP is enabled for this CCC.
func (ccc *cccCrd) AutoprovisioningEnabled() bool {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.NodePoolAutoCreation == nil {
		return false
	}
	return ccc.Spec.NodePoolAutoCreation.Enabled
}

// DynamicMaxPodsPerNodeEnabled checks if dynamic max pods per node is enabled for this CCC.
// This setting only makes sense for managed aka AutopilotManaged CCCs. Always false for standard ones.
func (ccc *cccCrd) DynamicMaxPodsPerNodeEnabled() bool {
	if ccc == nil {
		return false
	}

	// Always false for standard CCCs
	if !ccc.AutopilotManaged() {
		return false
	}

	// If there is no data specified for DynamicMaxPodsPerNode flag we consider it to be set to true
	// for AutopilotManaged CCCs
	if ccc.ComputeClass == nil ||
		ccc.Spec.NodePoolAutoCreation == nil ||
		ccc.Spec.NodePoolAutoCreation.DynamicMaxPodsPerNode == nil {
		return true
	}

	return *ccc.Spec.NodePoolAutoCreation.DynamicMaxPodsPerNode
}

// DynamicBootDiskSizeEnabled checks if dynamic boot disk size is enabled for this CCC.
// This setting only makes sense for managed aka AutopilotManaged CCCs. Always false for standard ones.
func (ccc *cccCrd) DynamicBootDiskSizeEnabled() bool {
	if ccc == nil {
		return false
	}

	// Always false for standard CCCs
	if !ccc.AutopilotManaged() {
		return false
	}

	// If there is no data specified for DynamicBootDiskSize flag we consider it to be set to true
	// for AutopilotManaged CCCs
	if ccc.ComputeClass == nil ||
		ccc.Spec.NodePoolAutoCreation == nil ||
		ccc.Spec.NodePoolAutoCreation.DynamicBootDiskSize == nil {
		return true
	}

	return *ccc.Spec.NodePoolAutoCreation.DynamicBootDiskSize
}

// AutopilotManaged checks if this CCC is Autopilot managed.
func (ccc *cccCrd) AutopilotManaged() bool {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.Autopilot == nil {
		return false
	}
	return ccc.Spec.Autopilot.Enabled
}

// ScaleUpAnyway checks if NAP should default to standard logic if no rule is available.
func (ccc *cccCrd) ScaleUpAnyway() bool {
	if ccc == nil || ccc.ComputeClass == nil {
		return false
	}
	return ccc.Spec.WhenUnsatisfiable == scaleUpAnyway
}

// ServiceAccount returns the name of the ServiceAccount
func (ccc *cccCrd) ServiceAccount() string {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.NodePoolConfig == nil {
		return ""
	}
	return ccc.Spec.NodePoolConfig.ServiceAccount
}

// ImageType returns the image type to be used by the node pools
func (ccc *cccCrd) ImageType() string {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.NodePoolConfig == nil {
		return ""
	}
	return ccc.Spec.NodePoolConfig.ImageType
}

// NodeVersion returns the node version to be used by the node pools
func (ccc *cccCrd) NodeVersion() string {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.NodePoolConfig == nil {
		return ""
	}
	return ccc.Spec.NodePoolConfig.NodeVersion
}

// OptimizeRulePriority checks if defrag should be enabled for CCC.
func (ccc *cccCrd) OptimizeRulePriority() bool {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.ActiveMigration == nil {
		return false
	}
	return ccc.Spec.ActiveMigration.OptimizeRulePriority
}

// ArchitectureTaintBehavior returns the architecture taint behavior for CCC.
func (ccc *cccCrd) ArchitectureTaintBehavior() string {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.NodePoolConfig == nil || ccc.Spec.NodePoolConfig.TaintConfig == nil {
		return ""
	}
	return ccc.Spec.NodePoolConfig.TaintConfig.ArchitectureTaintBehavior
}

// Conditions returns conditions of CCC.
func (ccc *cccCrd) Conditions() []metav1.Condition {
	if ccc == nil || ccc.ComputeClass == nil {
		return nil
	}
	return ccc.Status.Conditions
}

// UpdateConditions updates conditions of CCC.
func (ccc *cccCrd) UpdateConditions(client client.Client, conditions []metav1.Condition) error {
	ctx, cancel := context.WithTimeout(context.Background(), clientCallTimeout)
	defer cancel()

	ccc.Status.Conditions = conditions
	cccCopy := ccc.ComputeClass.DeepCopy()
	cccCopy.Spec = v1.ComputeClassSpec{}
	_, err := client.CccClient().CloudV1().ComputeClasses().UpdateStatus(ctx, cccCopy, metav1.UpdateOptions{})

	return err
}

// GetRuleCondition returns conditions of CCC rule.
func (ccc *cccCrd) GetRuleCondition(ruleIdx string) []metav1.Condition {
	if ccc == nil || ccc.ComputeClass == nil {
		return nil
	}
	for _, status := range ccc.Status.PriorityStatuses {
		if status.Identifier == ruleIdx {
			return status.Conditions
		}
	}
	return nil
}

func (ccc *cccCrd) ConsolidationDelay() *time.Duration {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.AutoscalingPolicy == nil {
		return nil
	}

	if ccc.Spec.AutoscalingPolicy.ConsolidationDelayMinutes == nil {
		return nil
	}

	duration := time.Duration(*ccc.Spec.AutoscalingPolicy.ConsolidationDelayMinutes) * time.Minute
	return &duration
}

func (ccc *cccCrd) ConsolidationThreshold() *int {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.AutoscalingPolicy == nil {
		return nil
	}

	return ccc.Spec.AutoscalingPolicy.ConsolidationThreshold
}

func (ccc *cccCrd) GPUConsolidationThreshold() *int {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.AutoscalingPolicy == nil {
		return nil
	}

	return ccc.Spec.AutoscalingPolicy.GPUConsolidationThreshold
}

func (ccc *cccCrd) EnsureAllDaemonSetPodsRunning() bool {
	if ccc.Spec.ActiveMigration != nil && ccc.Spec.ActiveMigration.EnsureAllDaemonSetPodsRunning != nil {
		return *ccc.Spec.ActiveMigration.EnsureAllDaemonSetPodsRunning
	}
	return ccc.Spec.Autopilot != nil && ccc.Spec.Autopilot.Enabled
}

// SelfServiceMetadata returns labels used for self-service features.
// These labels are automatically generated based on the CCC spec and supportedCccFeatures.
func (ccc *cccCrd) SelfServiceMetadata() map[string]string {
	if ccc == nil || ccc.ComputeClass == nil {
		return nil
	}
	return selfservice.ComputeClassSpecMetadata(ccc.Spec)
}

// UserDefinedLabels returns labels which are defined by the user in the CCC spec.nodePoolConfig.nodeLabels
// These labels are applied to the node pools created by the NAP. System labels are excluded.
func (ccc *cccCrd) UserDefinedLabels() map[string]string {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.NodePoolConfig == nil {
		return nil
	}
	labels := make(map[string]string)
	for k, v := range ccc.Spec.NodePoolConfig.NodeLabels {
		if !gkelabels.IsSystemLabel(k) {
			labels[k] = v
		}
	}
	return labels
}

// UserDefinedTaints returns taints which are defined by the user in the CCC spec.nodePoolConfig.taints.
// These taints are applied to the created node pools
func (ccc *cccCrd) UserDefinedTaints() []apiv1.Taint {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.NodePoolConfig == nil {
		return nil
	}
	var taints []apiv1.Taint
	for _, taint := range ccc.Spec.NodePoolConfig.Taints {
		if !gkelabels.IsSystemLabel(taint.Key) {
			taints = append(taints, apiv1.Taint{
				Key:    taint.Key,
				Value:  taint.Value,
				Effect: apiv1.TaintEffect(taint.Effect),
			})
		}
	}

	return taints
}

func (ccc *cccCrd) ResourceManagerTags() []crd.Tag {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.NodePoolConfig == nil {
		return nil
	}
	var tags []crd.Tag
	for _, tag := range ccc.Spec.NodePoolConfig.ResourceManagerTags {
		tags = append(tags, crd.Tag{
			Key:   tag.Key,
			Value: tag.Value,
		})
	}
	return tags
}

// TpuDriverMode returns the TPU driver mode for the CCC, defaults to device plugin
// when unable to determine or there's an error during conversion.
func (ccc *cccCrd) TpuDriverMode() crd.TpuDriverMode {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.NodePoolConfig == nil {
		return crd.TpuDriverModeDevicePlugin
	}

	switch ccc.Spec.NodePoolConfig.Tpu.DriverMode {
	case v1.TpuDriverModeDevicePlugin:
		return crd.TpuDriverModeDevicePlugin
	case v1.TpuDriverModeDynamicResourceAllocation:
		return crd.TpuDriverModeDynamicResourceAllocation
	case "":
		// Default to device plugin when not specified as server-side
		// defaulting only works in cases when the block field is located
		// in is present in the custom resource spec.
		return crd.TpuDriverModeDevicePlugin
	default:
		klog.Warningf("Unknown TPU driver mode for CCC (%s): %v, defaulting to device plugin", ccc.Name(), ccc.Spec.NodePoolConfig.Tpu.DriverMode)
		return crd.TpuDriverModeDevicePlugin
	}
}

func (ccc *cccCrd) getGPURequest(machineType *string, gpu *v1.GPU) (gpuRequest *machinetypes.GpuRequest) {
	if gpu != nil {
		gpuRequest = &machinetypes.GpuRequest{
			Config: machinetypes.GpuConfig{
				GpuType:       gpu.Type,
				DriverVersion: gpu.DriverVersion,
			},
		}
		if gpu.GpuSharing != nil {
			if gpu.GpuSharing.SharingStrategy != "" {
				// MaxSharedClientsPerGPU only makes sense when used with some strategy
				if gpu.GpuSharing.MaxSharedClientsPerGPU != 0 {
					gpuRequest.Config.MaxSharedClients = fmt.Sprintf("%d", gpu.GpuSharing.MaxSharedClientsPerGPU)
				}
				gpuRequest.Config.SharingStrategy = gkelabels.ConvertGpuSharingStrategyToLabelEnum(gpu.GpuSharing.SharingStrategy)
			}
			gpuRequest.Config.PartitionSize = gpu.GpuSharing.GpuPartitionSize
		}
		gpuRequest.PhysicalGPUCount = machinetypes.PhysicalGpuCount(gpu.Count)
	}
	if machineType != nil {
		gpuType, count, found := ccc.provider.MachineConfigProvider().FixedGPUTypeAndCountForMachineType(*machineType)
		if found {
			if gpuRequest == nil {
				gpuRequest = &machinetypes.GpuRequest{}
			}
			if gpuRequest.Config.GpuType == "" {
				gpuRequest.Config.GpuType = gpuType
			}
			if gpuRequest.PhysicalGPUCount == 0 {
				gpuRequest.PhysicalGPUCount = count
			}
		}
	}
	if gpuRequest != nil {
		var err error
		gpuRequest.Count, err = ccc.provider.MachineConfigProvider().PhysicalToAllocatableWithGpuName(gpuRequest.PhysicalGPUCount, gpuRequest.Config.GpuType, gpuRequest.Config.PartitionSize, gpuRequest.Config.MaxSharedClients)
		if err != nil {
			klog.Errorf("Unable to evaluate the allocatable gpu units for gpu "+
				"requirements: %v. Falled back to allocatableGpus = %v (physical gpu count)",
				err, gpuRequest.PhysicalGPUCount)
			// should not happen, fallback to physical
			gpuRequest.Count = machinetypes.AllocatableGpuCount(gpuRequest.PhysicalGPUCount)
		}
	}
	return
}

func getReservationBlockAndSubBlockName(reservation *v1.SpecificReservation) (string, string) {
	var blockName, subBlockName string
	if reservation == nil {
		return blockName, subBlockName
	}
	if reservation.ReservationBlock != nil {
		blockName = reservation.ReservationBlock.Name
		if reservation.ReservationBlock.ReservationSubBlock != nil {
			subBlockName = reservation.ReservationBlock.ReservationSubBlock.Name
		}
	}
	return blockName, subBlockName
}

// TargetNodeCount returns the TargetNodeCount for the ComputeClass.
func (ccc *cccCrd) TargetNodeCount() *int {
	if ccc == nil || ccc.ComputeClass == nil || ccc.Spec.MinimumCapacity == nil {
		return nil
	}
	return ccc.Spec.MinimumCapacity.TargetNodeCount
}
