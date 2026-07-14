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

package placement

import (
	"fmt"
	"regexp"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	caerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/klog/v2"
)

// Spec specifies the type of VM placement group. Empty value leaves unspecified placement type,
// any other value corresponds to a name of a compact placement group.
type Spec struct {
	// GroupId, when specified, is the name of the node group, if NAP beforehand created a node pool for this group.
	GroupId string
	// Policy is the name of the placement policy.
	Policy string
	// ResourcePolicy is the fetched GCE resource policy object
	// Note: It's currently only fetched in NAP injection.
	ResourcePolicy *gceclient.GceResourcePolicy
}

const (
	// Unspecified is an unspecified type of node placement.
	Unspecified = "TYPE_UNSPECIFIED"
	// Compact places nodes close to each other in order to reduce the communication latency.
	Compact = "COMPACT"
	// TODO(b/517097125): adjust the validation if necessary after the discussion in cl/444512634 gets resolved.
	// maxLen is a maximum placement group length
	maxLen = 40
)

var (
	nameRegexp = regexp.MustCompile(`^[a-z]([-a-z0-9]*[a-z0-9])?$`)
	// placementPolicyRegexp matches the following projects/{project}/regions/{region}/resourcePolicies/{group}
	placementPolicyRegexp = regexp.MustCompile(`^projects/([a-z0-9\-]+)/regions/([a-z0-9\-]+)/resourcePolicies/([a-z0-9\-]+)$`)
)

// UsesPlacement returns whether the specified placement group requires compact placement.
func (p Spec) UsesPlacement() bool {
	return p.GroupId != "" || p.Policy != ""
}

// UsesSlice returns whether the specified placement group requires slice placement.
func (p Spec) UsesSlice() bool {
	return p.Policy != "" && hasTopology(p.ResourcePolicy)
}

// Type returns the type of specified placement.
func (p Spec) Type() string {
	if p.isCompact() {
		return Compact
	}
	return Unspecified
}

// isCompact returns whether the specified placement group requires compact placement.
func (p Spec) isCompact() bool {
	return p.GroupId != "" && p.Policy == ""
}

// Strings converts the specified Placement to string.
func (p Spec) String() string {
	fullName := ""
	if p.GroupId != "" {
		fullName += fmt.Sprintf("Group: '%s'", p.GroupId)
	}
	if p.GroupId != "" && p.Policy != "" {
		fullName += ", "
	}
	if p.Policy != "" {
		fullName += fmt.Sprintf("Policy: '%s'", p.Policy)
	}
	return fullName
}

// FromLabels returns the placement type the labels indicate.
func FromLabels(labels map[string]string) Spec {
	var spec Spec
	if val, ok := labels[gkelabels.PlacementGroupLabel]; ok {
		spec.GroupId = val
	}
	if val, ok := labels[gkelabels.PolicyLabel]; ok {
		spec.Policy = val
	}
	return spec
}

// FromRequirements returns placement type specified by pod requirements.
func FromRequirements(req podrequirements.LabelRequirements) Spec {
	var spec Spec
	valueGroup, foundGroup := req.GetSingleValue(gkelabels.PlacementGroupLabel)
	if foundGroup {
		spec.GroupId = valueGroup
	}
	valuePolicy, foundPolicy := req.GetSingleValue(gkelabels.PolicyLabel)
	if foundPolicy {
		spec.Policy = valuePolicy
	}
	return spec
}

func FromReservationResourcePolices(reservationResourcePolices map[string]string) (Spec, errors.AutoscalerError) {
	var resourcePolicy string
	if placementName, hasPlacement := reservationResourcePolices[gkelabels.ReservationResourcePoliciesPlacementKey]; hasPlacement {
		resourcePolicy = placementName
	} else if policyName, hasPolicy := reservationResourcePolices[gkelabels.ReservationResourcePoliciesPolicyKey]; hasPolicy {
		resourcePolicy = policyName
	} else {
		return Spec{}, nil
	}
	// projects/{project}/regions/{region}/resourcePolicies/{group}
	placementPolicyParts := placementPolicyRegexp.FindStringSubmatch(resourcePolicy)
	if len(placementPolicyParts) != 4 {
		klog.Errorf("Invalid placement policy '%s' provided in the reservation", resourcePolicy)
		return Spec{}, NewInvalidPlacementPolicy(resourcePolicy, fmt.Sprintf("it should follow %s regexp", placementPolicyRegexp.String()))
	}
	return Spec{Policy: placementPolicyParts[3]}, nil
}

// Validate returns an error if placement group is invalid based on user requirements and cluster configuration.
func (p Spec) Validate(existingNodePools sets.Set[string]) errors.AutoscalerError {
	if !p.UsesPlacement() || p.GroupId == "" {
		return nil
	}
	if err := p.validateNodePoolName(); err != nil {
		return err
	}
	if err := p.validateDoesntAlreadyExist(existingNodePools); err != nil {
		return err
	}
	return nil
}

func (p Spec) validateNodePoolName() errors.AutoscalerError {
	if len(p.GroupId) > maxLen {
		return NewInvalidPlacementGroupNameError(p.GroupId, fmt.Sprintf("should be at most %v characters long", maxLen))
	}
	if !nameRegexp.MatchString(p.GroupId) && p.GroupId != "" {
		return NewInvalidPlacementGroupNameError(p.GroupId, "should consist of alphanumerics and '-', start with a letter and end with an alphanumeric")
	}
	return nil
}

// UpdateMachineSpec updates machine spec - if only some machine families support placement and they were implied from other configuration,
// this function narrows down this set only to those supporting placement. If there is no machine family supporting placement function will return error.
func (p Spec) UpdateMachineSpec(machineSpec *machinetypes.MachineSpec, selectionType machinetypes.SelectionType) errors.AutoscalerError {
	if !p.UsesPlacement() {
		return nil
	}
	selectedMachineFamilies := []string{}
	for _, f := range machineSpec.Families {
		selectedMachineFamilies = append(selectedMachineFamilies, f.Name())
	}
	if selectionType == machinetypes.SelectionTypeImplied {
		filtered := p.filterSupportedMachineFamilies(machineSpec.Families)
		if len(filtered) == 0 {
			return NewInvalidMachineFamilyError(p.String(), fmt.Sprintf("pod configuration implies machine families %q, none of which supports %s",
				selectedMachineFamilies, p.errPlacementTypeString()))
		}
		machineSpec.Families = filtered
	}
	return nil
}

// ValidateMachineSpec returns an error if provided machine spec is incompatible with any placement.
func (p Spec) ValidateMachineSpec(machineSpec *machinetypes.MachineSpec, selectionType machinetypes.SelectionType, tpuTopology string, tpuChipsPerNode int64, machineConfigProvider *machinetypes.MachineConfigProvider) errors.AutoscalerError {
	if !p.UsesPlacement() {
		return nil
	}
	selectedMachineFamilies := []string{}
	for _, f := range machineSpec.Families {
		selectedMachineFamilies = append(selectedMachineFamilies, f.Name())
	}
	if selectionType == machinetypes.SelectionTypeDefault {
		return NewInvalidMachineFamilyError(p.String(), "compact and non-compact placement requires machine family or compute class to be specified explicitly")
	}
	if !p.allMachineFamiliesSupported(machineSpec.Families) {
		if machineSpec.ComputeClassName != "" {
			return NewInvalidMachineFamilyError(p.String(), fmt.Sprintf("specified compute class %q doesn't support %s", machineSpec.ComputeClassName, p.errPlacementTypeString()))
		}
		return NewInvalidMachineFamilyError(p.String(), fmt.Sprintf("specified machine families %q don't support %s", selectedMachineFamilies, p.errPlacementTypeString()))
	}
	isMultiHostTpu, err := p.isTpuMultiHost(machineSpec, tpuTopology, tpuChipsPerNode, machineConfigProvider)
	if err != nil {
		return err
	}
	var errs []*ErrInvalidPlacementPolicy
	for _, f := range machineSpec.Families {
		if !isMultiHostTpu && machineSpec.TpuType != "" {
			continue
		}
		err := p.validateWorkloadPolicy(f)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return newInvalidPlacementPolicyFromErrors(errs)
	}
	return nil
}

func (p Spec) isTpuMultiHost(machineSpec *machinetypes.MachineSpec, tpuTopology string, tpuChipsPerNode int64, machineConfigProvider *machinetypes.MachineConfigProvider) (bool, errors.AutoscalerError) {
	if tpuTopology == "" || machineSpec.TpuType == "" || tpuChipsPerNode == 0 {
		return false, nil
	}
	isMultiHost, err := machineConfigProvider.IsMultiHostTpuPodslice(machineSpec.TpuType, tpuTopology, tpuChipsPerNode)
	if err != nil {
		metrics.Metrics.RegisterUnexpectedPod(metrics.ReasonInvalidTPUTopology)
		return false, machinetypes.NewInvalidMachineSpecError(machineSpec.String(), err.Error())
	}
	return isMultiHost, nil
}

// validateWorkloadPolicy checks if resourece policy exists for machine families that require it
func (p Spec) validateWorkloadPolicy(machineFamily machinetypes.MachineFamily) *ErrInvalidPlacementPolicy {
	if p.ResourcePolicy != nil || !machineFamily.RequiresBYOResourcePolicy() {
		if machineFamily.RequiresBYOResourcePolicy() && p.ResourcePolicy.WorkloadPolicy.AcceleratorTopology == "" {
			return &ErrInvalidPlacementPolicy{PlacementPolicy: p.String(), Msg: fmt.Sprintf("machine family %s requires workload policy with topology to determine the slice size", machineFamily.Name())}
		}
		return nil
	}
	return &ErrInvalidPlacementPolicy{PlacementPolicy: p.String(), Msg: "could not find workload policy"}
}

// MaxNodes returns the maximum node limit for machine type and resource policy
func MaxNodes(provider *machinetypes.MachineConfigProvider, machineType string, resourcePolicy *gceclient.GceResourcePolicy) (int64, error) {
	machineFamily, err := provider.GetMachineFamilyFromMachineName(machineType)
	if err != nil {
		return 0, caerrors.NewAutoscalerErrorf(caerrors.InternalError, "unknown machine family for machine type %q: %v", machineType, err)
	}
	if hasTopology(resourcePolicy) && machineFamily.IsAcceleratorSliceSupported() {
		nodes, err := maxNodesFromTopology(provider, machineType, resourcePolicy)
		if err == nil {
			return nodes, nil
		}
		// TODO(b/474571251): add additional error handling
		klog.Infof("Invalid topology %v", err)
	}
	return machineFamily.MaxCompactPlacementNodes()
}

// SupportsMachineFamily returns whether the specified machine family
// is supported by the given type of placement, or no placement at all.
func (p Spec) SupportsMachineFamily(family machinetypes.MachineFamily) bool {
	if !p.UsesPlacement() {
		return true
	}
	if family.IsCompactPlacementSupported() {
		return true
	}
	// for non-compact placement (accelerator slice has dense placement), the policy name must be specified
	if family.IsAcceleratorSliceSupported() && p.Policy != "" {
		return true
	}
	return false
}

func (p Spec) validateDoesntAlreadyExist(existingNodePools sets.Set[string]) errors.AutoscalerError {
	if existingNodePools.Has(p.GroupId) {
		return NewNodeGroupAlreadyExistsError(p.GroupId)
	}
	return nil
}

func (p Spec) allMachineFamiliesSupported(families []machinetypes.MachineFamily) bool {
	for _, f := range families {
		if !p.SupportsMachineFamily(f) {
			return false
		}
	}
	return true
}

func (p Spec) filterSupportedMachineFamilies(families []machinetypes.MachineFamily) []machinetypes.MachineFamily {
	filtered := []machinetypes.MachineFamily{}
	for _, f := range families {
		if p.SupportsMachineFamily(f) {
			filtered = append(filtered, f)
		}
	}
	return filtered
}

func (p Spec) errPlacementTypeString() string {
	if p.isCompact() {
		return "compact placement"
	} else {
		return "any type of placement"
	}
}

// hasTopology returns true if the Resource Policy has a topology.
func hasTopology(resourcePolicy *gceclient.GceResourcePolicy) bool {
	return resourcePolicy != nil && resourcePolicy.WorkloadPolicy.AcceleratorTopology != ""
}

func maxNodesFromTopology(provider *machinetypes.MachineConfigProvider, machineType string, resourcePolicy *gceclient.GceResourcePolicy) (int64, error) {
	if resourcePolicy == nil {
		return 0, fmt.Errorf("resource policy is not set")
	}
	topology := resourcePolicy.WorkloadPolicy.AcceleratorTopology

	nodes, err := provider.NumNodesFromTopology(machineType, topology)
	if err == nil {
		return nodes, nil
	}
	return 0, fmt.Errorf("couldn't find number of nodes for machine type %q and topology %q: %q", machineType, topology, err)
}
