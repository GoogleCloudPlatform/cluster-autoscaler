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
	"errors"
	"fmt"
	"slices"
	"strconv"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

type instanceConfigCloudProvider interface {
	GetAutoprovisioningLocations() []string
	GetGkeMigs() []*gke.GkeMig
	ExistingMigsInNodePool(nodePoolName string) []*gke.GkeMig
	GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily
	GetMachineType(machineType string, zone string) (gce.MachineType, error)
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

// instanceConfigGenerator generates all possible instance configurations(InstanceConfig) for a flexibility Scope (a.k.a. CCC)
type instanceConfigGenerator struct {
	cccLister          lister.Lister
	provider           instanceConfigCloudProvider
	optionsTracker     *optstracking.OptionsTracker
	maxInstanceConfigs int
}

type generatorOption func(*instanceConfigGenerator)

// NewInstanceConfigGenerator returns an instance of instanceConfigGenerator
func NewInstanceConfigGenerator(cccLister lister.Lister, provider instanceConfigCloudProvider, optionsTracker *optstracking.OptionsTracker, opts ...generatorOption) *instanceConfigGenerator {
	g := &instanceConfigGenerator{
		cccLister:          cccLister,
		provider:           provider,
		optionsTracker:     optionsTracker,
		maxInstanceConfigs: defaultMaxInstanceConfigs,
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// GeneratorArtifacts wraps generated instance configs and a map indicating capped keys.
type GeneratorArtifacts struct {
	Configs       map[string]*api.InstanceConfig
	CappedKeysMap map[string]bool
}

// generateInstanceConfigs returns all possible instance configurations(InstanceConfig) for a flexibility Scope (a.k.a. CCC) wrapped in GeneratorArtifacts, and errors occurred during the generation process.
func (g *instanceConfigGenerator) generateInstanceConfigs(flexibilityScopeKey string) (*GeneratorArtifacts, []error) {
	crd, err := g.matchingCrd(flexibilityScopeKey)
	if err != nil {
		return nil, []error{err}
	}
	allInstanceConfigs := make(map[string]*api.InstanceConfig)
	var allErrors []error

	for idx, ruleGroup := range crd.GroupedRules() {
		for _, rule := range ruleGroup {
			rank := idx + 1

			// Rule specifies a set of node pools
			if len(rule.NodePoolNames()) > 0 {
				instanceConfigsForRule, errors := g.instanceConfigsForNodePools(rule, rank)
				allErrors = append(allErrors, errors...)
				g.validateGeneratedConfigsForRule(flexibilityScopeKey, instanceConfigsForRule, errors)
				allInstanceConfigs = mergeNewInstanceConfigs(allInstanceConfigs, instanceConfigsForRule)
				continue
			}

			instanceConfigsForRule, err := g.deriveMachineConfigsFromRule(rule, rank)
			if err != nil {
				allErrors = append(allErrors, fmt.Errorf("error when generating instance configs for flexibilityScopeKey: %s err: %v", flexibilityScopeKey, err))
				g.validateGeneratedConfigsForRule(flexibilityScopeKey, nil, []error{err})
				continue
			}
			instanceConfigsForRule = g.expandConfigsByProvisioningMode(rule, instanceConfigsForRule)
			instanceConfigsForRule = g.expandConfigsByMaxRunDuration(rule, instanceConfigsForRule)
			instanceConfigsForRule, zoneErrors := g.assignZonesToConfigs(rule, instanceConfigsForRule)
			allErrors = append(allErrors, zoneErrors...)

			g.validateGeneratedConfigsForRule(flexibilityScopeKey, instanceConfigsForRule, zoneErrors)

			allInstanceConfigs = mergeNewInstanceConfigs(allInstanceConfigs, instanceConfigsForRule)
		}
	}

	metrics.Metrics.UpdateFlexAdvisorGeneratedInstanceConfigCount(flexibilityScopeKey, len(allInstanceConfigs))

	klog.V(4).Infof("FlexAdvisor[async-worker]: generated %d instance configs for flexibility scope: %s, rulesCount=%d", len(allInstanceConfigs), flexibilityScopeKey, len(crd.GroupedRules()))

	allInstanceConfigs, cappedKeysMap := g.capGeneratedInstanceConfigs(allInstanceConfigs, flexibilityScopeKey)

	return &GeneratorArtifacts{
		Configs:       allInstanceConfigs,
		CappedKeysMap: cappedKeysMap,
	}, allErrors
}

func (g *instanceConfigGenerator) validateGeneratedConfigsForRule(flexibilityScopeKey string, configs []*api.InstanceConfig, errorsForRule []error) {
	if len(configs) > 0 {
		return
	}

	onlyZoneAvailabilityErrors := len(errorsForRule) > 0
	for _, err := range errorsForRule {
		var zoneErr *ErrMachineUnavailableInZones
		if !errors.As(err, &zoneErr) {
			onlyZoneAvailabilityErrors = false
			break
		}
	}

	if !onlyZoneAvailabilityErrors {
		klog.Warningf("FlexAdvisor[async-worker]: 0 instance configurations generated for rule under ccc %q", flexibilityScopeKey)
		metrics.Metrics.RegisterFlexAdvisorGenerationError(metrics.ZeroConfigsGeneratedForRule)
	}
}

// capGeneratedInstanceConfigs returns input array limited to maxInstanceConfigs elements and cappedKeysMap (map indicating for each generated key whether it was capped from final array)
func (g *instanceConfigGenerator) capGeneratedInstanceConfigs(allInstanceConfigs map[string]*api.InstanceConfig, flexibilityScopeKey string) (map[string]*api.InstanceConfig, map[string]bool) {
	var instanceConfigSlice []*api.InstanceConfig
	var cappedKeysMap = make(map[string]bool, len(allInstanceConfigs))
	for _, config := range allInstanceConfigs {
		instanceConfigSlice = append(instanceConfigSlice, config)
	}
	slices.SortFunc(instanceConfigSlice, func(a, b *api.InstanceConfig) int {
		if a.Rank() != b.Rank() {
			return a.Rank() - b.Rank()
		}
		// If rank is equal to make capping deterministic we sort by these as well, besides that there's no use is sorting by signature
		if a.Signature() < b.Signature() {
			return -1
		}
		if a.Signature() > b.Signature() {
			return 1
		}
		return 0
	})

	finalInstanceConfigs := make(map[string]*api.InstanceConfig)

	n := min(len(instanceConfigSlice), g.maxInstanceConfigs)
	for i := 0; i < n; i += 1 {
		finalInstanceConfigs[instanceConfigSlice[i].Signature()] = instanceConfigSlice[i]
		cappedKeysMap[instanceConfigSlice[i].Signature()] = false
	}
	for i := n; i < len(instanceConfigSlice); i++ {
		cappedKeysMap[instanceConfigSlice[i].Signature()] = true
	}
	if len(allInstanceConfigs) > g.maxInstanceConfigs {
		klog.Infof("FlexAdvisor[async-worker]: capping generated instance configs for %v from %d to %d", flexibilityScopeKey, len(allInstanceConfigs), g.maxInstanceConfigs)
	}
	return finalInstanceConfigs, cappedKeysMap
}

// Since we are merging new instanceConfigs to existing instanceConfig with higher ranks (rank 1 > rank 2 ...)
// we are preserving the rank of the highest priority as the rank of the instanceConfig
func mergeNewInstanceConfigs(allInstanceConfigs map[string]*api.InstanceConfig, newInstanceConfigs []*api.InstanceConfig) map[string]*api.InstanceConfig {
	for _, instanceConfig := range newInstanceConfigs {
		signature := instanceConfig.Signature()
		if existingConfig, found := allInstanceConfigs[signature]; found {
			existingConfig.MergeZones(instanceConfig.Zones())
			continue
		}
		allInstanceConfigs[signature] = instanceConfig
	}
	return allInstanceConfigs
}

func (g *instanceConfigGenerator) getRuleConstraintsForMachineDerivation(rule rules.Rule) (*machinetypes.Constraints, int64, error) {
	var constraint machinetypes.Constraints
	var requestedTpuCount int64

	constraint = machinetypes.NoConstraints
	constraint.DiskType = rule.BootDiskType()

	if rule.MinCpuPlatformString() != "" && isFlexAdvisorMinCpuPlatformSupportEnabled(g.optionsTracker.ExperimentsManager()) {
		platform, err := rule.MinCpuPlatform()

		if err != nil {
			return nil, 0, fmt.Errorf("could not determine min cpu platform err=%v", err)
		}
		constraint.CpuPlatform = platform
	}

	if rule.MachineType() != "" {
		if _, err := g.provider.MachineConfigProvider().ToMachineType(rule.MachineType()); err != nil {
			return nil, 0, fmt.Errorf("machine type not found for %s", rule.MachineType())
		}
		constraint.ExplicitMachineTypes = []string{rule.MachineType()}
	}

	if rule.GpuRequest().Config.GpuType != "" {
		constraint.GpuType = rule.GpuRequest().Config.GpuType
	}

	if rule.TpuType() != "" {
		if isFlexAdvisorTPUEnabled(g.optionsTracker.ExperimentsManager()) {
			constraint.TpuType = rule.TpuType()
		} else {
			return nil, 0, errors.New("tpu rules are not supported")
		}
	}

	if rule.TpuCount() != 0 {
		if isFlexAdvisorTPUEnabled(g.optionsTracker.ExperimentsManager()) {
			requestedTpuCount = rule.TpuCount()
		} else {
			return nil, 0, errors.New("tpu rules are not supported")
		}
	}

	return &constraint, requestedTpuCount, nil
}

func (g *instanceConfigGenerator) deriveMachineConfigsFromRule(rule rules.Rule, rank int) ([]*api.InstanceConfig, error) {
	var instanceConfigs []*api.InstanceConfig
	constraint, requestedTpuCount, err := g.getRuleConstraintsForMachineDerivation(rule)
	if err != nil {
		return nil, err
	}
	machineFamilies, err := g.machineFamiliesForRule(rule)
	if err != nil {
		return nil, fmt.Errorf("machine family not found %v", err)
	}
	for _, machineFamily := range machineFamilies {
		for _, machineTypeInfo := range filterByCoresAndMemoryRequirements(machineFamily.AllMachineTypes(*constraint), rule) {
			// Note: requestedTpuCount can only be non-zero when the TPU experiment is enabled
			// (otherwise getRuleConstraintsForMachineDerivation would return an error).
			if requestedTpuCount != 0 {
				machineTpuCount, err := g.provider.MachineConfigProvider().GetTpuCountForMachineType(machineTypeInfo.Name)
				if err != nil || machineTpuCount != requestedTpuCount {
					// we are iterating over many families not always connected to tpu, don't report err
					continue
				}
			}
			if machineFamily.Name() == "n1" {
				instanceConfigs = append(instanceConfigs, g.instanceConfigsForMachineTypeFromN1Family(rule, machineTypeInfo, machineFamily, rank)...)
			} else {
				instanceConfigs = append(instanceConfigs, g.buildInstanceConfig(rule, machineTypeInfo, rank, nil, nil))
			}
		}
	}
	return instanceConfigs, nil
}

func (g *instanceConfigGenerator) expandConfigsByProvisioningMode(rule rules.Rule, instanceConfigs []*api.InstanceConfig) []*api.InstanceConfig {

	if rule.FlexStartEnabled() {
		if isFlexAdvisorDWSEnabled(g.optionsTracker.ExperimentsManager()) {
			for _, config := range instanceConfigs {
				config.SetProvisioningMode(instanceavailability.FlexStart)
				config.SetMaxRunDurationInSeconds(strconv.Itoa(int(queuedwrapper.DefaultMaxRunDuration.Seconds())))
			}
			return instanceConfigs
		} else {
			return nil
		}
	}

	var allInstanceConfigs []*api.InstanceConfig
	for _, baseConfig := range instanceConfigs {
		configCopy := api.DeepCopyInstanceConfig(baseConfig)
		configCopy.SetProvisioningMode(instanceavailability.Spot)
		allInstanceConfigs = append(allInstanceConfigs, configCopy)
	}
	if rule.Spot() {
		return allInstanceConfigs
	}
	for _, baseConfig := range instanceConfigs {
		configCopy := api.DeepCopyInstanceConfig(baseConfig)
		configCopy.SetProvisioningMode(instanceavailability.Standard)
		allInstanceConfigs = append(allInstanceConfigs, configCopy)
	}
	return allInstanceConfigs
}

func (g *instanceConfigGenerator) expandConfigsByMaxRunDuration(rule rules.Rule, instanceConfigs []*api.InstanceConfig) []*api.InstanceConfig {
	// MaxRunDuration field support is part of DWS rollout
	if !isFlexAdvisorDWSEnabled(g.optionsTracker.ExperimentsManager()) {
		return instanceConfigs
	}

	if rule.MaxRunDurationSeconds() == nil {
		return instanceConfigs
	}

	ruleMRD := strconv.Itoa(*rule.MaxRunDurationSeconds())
	for _, config := range instanceConfigs {
		// Note: `nodepool` rules cannot have MRD specified in the CCC, for them we set MRD directly in `instanceConfigsForNodePools`
		config.SetMaxRunDurationInSeconds(ruleMRD)
	}
	return instanceConfigs
}

// assignZonesToConfigs assigns zones to instanceConfigs. Filter out instance configs with invalid (machine type, zone) combinations.
func (g *instanceConfigGenerator) assignZonesToConfigs(rule rules.Rule, instanceConfigs []*api.InstanceConfig) ([]*api.InstanceConfig, []error) {
	var zones []string
	var instanceConfigsWithZones []*api.InstanceConfig
	var allErrors []error
	if len(rule.Zones()) > 0 {
		zones = rule.Zones()
	} else if len(rule.ZoneTypes()) > 0 && isFlexAdvisorZoneTypesEnabled(g.optionsTracker.ExperimentsManager()) && g.optionsTracker.Options().ZoneTypesEnabled {
		zoneTypesZones, err := rule.GetZoneTypesZones()
		if err != nil {
			klog.Errorf("zoneTypes: failed to get zones from zone types: %v", err)
			zones = g.provider.GetAutoprovisioningLocations()
		} else {
			zones = zoneTypesZones
		}
	} else {
		zones = g.provider.GetAutoprovisioningLocations()
	}
	for _, instanceConfig := range instanceConfigs {
		for _, zone := range zones {
			_, err := g.provider.GetMachineType(instanceConfig.MachineType(), zone)
			if err != nil {
				continue
			}
			instanceConfig.InsertZone(zone)
		}
		if len(instanceConfig.Zones()) == 0 {
			allErrors = append(allErrors, &ErrMachineUnavailableInZones{MachineType: instanceConfig.MachineType()})
			continue
		}
		instanceConfigsWithZones = append(instanceConfigsWithZones, instanceConfig)
	}
	return instanceConfigsWithZones, allErrors
}

func (g *instanceConfigGenerator) instanceConfigsForNodePools(rule rules.Rule, rank int) ([]*api.InstanceConfig, []error) {
	var allInstanceConfigs []*api.InstanceConfig
	var errors []error
	for _, nodePoolName := range rule.NodePoolNames() {
		mig := g.findGkeMig(nodePoolName)
		if mig == nil {
			errors = append(errors, fmt.Errorf("mig not found for node pool: %s", nodePoolName))
			continue
		}
		if mig.Spec() == nil {
			errors = append(errors, fmt.Errorf("mig spec is undefined for node pool: %s", nodePoolName))
			continue
		}
		instanceConfig, err := buildInstanceConfigFromNodePoolSpec(mig.MachineType(), mig.Spec(), rank, mig.Spec().Locations, g.optionsTracker.ExperimentsManager())
		if err != nil {
			errors = append(errors, err)
			continue
		}
		allInstanceConfigs = append(allInstanceConfigs, instanceConfig)
	}
	return allInstanceConfigs, errors
}

func (g *instanceConfigGenerator) instanceConfigsForMachineTypeFromN1Family(rule rules.Rule, machineTypeInfo machinetypes.MachineType, machineFamily machinetypes.MachineFamily, rank int) []*api.InstanceConfig {
	var instanceConfigs []*api.InstanceConfig
	for _, gpuType := range machineFamily.SupportedGpuTypes() {
		if rule.GpuRequest().Config.GpuType != "" && rule.GpuRequest().Config.GpuType != gpuType.Name() {
			continue
		}
		for gpuCount, maxCpuCount := range gpuType.MaxCpuCount() {
			if rule.GpuRequest().Count > 0 && rule.GpuRequest().PhysicalGPUCount != gpuCount {
				continue
			}
			if machineTypeInfo.CPU > int64(maxCpuCount) {
				continue
			}
			instanceConfigs = append(instanceConfigs, g.buildInstanceConfig(rule, machineTypeInfo, rank, ptr.To(gpuType.Name()), ptr.To(int(gpuCount))))
		}
	}
	if rule.GpuRequest().Config.GpuType == "" {
		instanceConfigs = append(instanceConfigs, g.buildInstanceConfig(rule, machineTypeInfo, rank, nil, nil))
	}
	return instanceConfigs
}

// buildInstanceConfig builds InstanceConfig from passed machineType and rule. GpuType & count is taken from rule unless overriden with overrideGpuType and overrideGpuCount
func (g *instanceConfigGenerator) buildInstanceConfig(rule rules.Rule, machineType machinetypes.MachineType, rank int, overrideGpuType *string, overrideGpuCount *int) *api.InstanceConfig {
	gpuType := machineType.GpuType()
	gpuCount := int(machineType.FixedGpuCount())
	if overrideGpuType != nil {
		gpuType = *overrideGpuType
	}
	if overrideGpuCount != nil {
		gpuCount = *overrideGpuCount
	}
	instanceConfig := api.NewInstanceConfig(
		machineType.Name,
		gpuType,
		gpuCount,
		rank,
		instanceavailability.Standard,
		api.EmptyMaxRunDuration,
	)

	if rule.TpuTopology() != "" && isFlexAdvisorTPUEnabled(g.optionsTracker.ExperimentsManager()) {
		// instanceConfigGenerator is "dumb". Validation is done on GCW/K8S and NAP levels, here we just pass through all the data from a rule.
		instanceConfig.SetWorkloadPolicies(api.WorkloadPolicies{
			AcceleratorTopology: rule.TpuTopology(),
		})
	}
	return instanceConfig
}

func (g *instanceConfigGenerator) findGkeMig(nodePoolName string) *gke.GkeMig {
	migs := g.provider.ExistingMigsInNodePool(nodePoolName)
	if len(migs) > 0 {
		return migs[0]
	}
	return nil
}

func (g *instanceConfigGenerator) matchingCrd(flexibilityScopeKey string) (crd.CRD, error) {
	crd, err := g.cccLister.GetCrd(flexibilityScopeKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get CRD for flexibilityScopeKey %q: %w", flexibilityScopeKey, err)
	}
	if crd == nil {
		// TODO(b/514250091): GetCrd returns nil for predefined compute class.
		return nil, fmt.Errorf("no CRD found for flexibilityScopeKey %q", flexibilityScopeKey)
	}
	return crd, nil
}

func (g *instanceConfigGenerator) machineFamiliesForRule(rule rules.Rule) ([]machinetypes.MachineFamily, error) {
	// TODO(b/491088027): consider whether this is the best order of ifs
	if rule.MachineFamily() != "" {
		machineFamily, err := g.provider.MachineConfigProvider().ToMachineFamily(rule.MachineFamily())
		if err != nil {
			return nil, err
		}
		return []machinetypes.MachineFamily{machineFamily}, nil
	} else if rule.PodFamilyName() != "" {
		podFamilyMachineFamilies, err := rule.PodFamilyMachineFamilies()
		if err != nil {
			return nil, err
		}
		if len(podFamilyMachineFamilies) == 0 {
			return nil, fmt.Errorf("pod family %q does not map to any machine families", rule.PodFamilyName())
		}
		return podFamilyMachineFamilies, nil
	} else if rule.MachineType() != "" {
		machineFamily, err := g.provider.MachineConfigProvider().GetMachineFamilyFromMachineName(rule.MachineType())
		if err != nil {
			return nil, fmt.Errorf("machine type not found for %s", rule.MachineType())
		}
		return []machinetypes.MachineFamily{machineFamily}, nil
	} else if rule.GpuRequest().Config.GpuType != "" || rule.TpuType() != "" {
		return g.provider.MachineConfigProvider().AllMachineFamilies(), nil
	}
	return []machinetypes.MachineFamily{g.provider.GetAutoprovisioningDefaultFamily()}, nil
}

func filterByCoresAndMemoryRequirements(machineTypes map[string]machinetypes.MachineType, rule rules.Rule) map[string]machinetypes.MachineType {
	filtered := make(map[string]machinetypes.MachineType)
	for machineType, machineTypeInfo := range machineTypes {
		if machineTypeInfo.CPU < rule.MinCores() {
			continue
		}
		if machineTypeInfo.Memory < rule.MinMemoryGb()*units.GiB {
			continue
		}
		filtered[machineType] = machineTypeInfo
	}
	return filtered
}

// ErrMachineUnavailableInZones represents an error when a machine type is not available in any of the targeted zones.
type ErrMachineUnavailableInZones struct {
	MachineType string
}

func (e *ErrMachineUnavailableInZones) Error() string {
	return fmt.Sprintf("machineType=%s was removed due to not being available in any of the zones", e.MachineType)
}
