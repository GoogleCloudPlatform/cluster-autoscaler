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

package autoprovisioning

import (
	"math"
	"math/rand"
	"reflect"
	"slices"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	gce_cloudprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	klog "k8s.io/klog/v2"
	"k8s.io/utils/set"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/interfaces"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/machineselection"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/napcloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	computeclass "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	computeclass_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/gkedebuggingsnapshot"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleblocking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
)

const (
	// StartupScriptUrlConflict caused scale-up to fail
	StartupScriptUrlConflict metrics.FailedScaleUpReason = "startupScriptUrlConflict"
	// GKEServiceAccountPermissionError caused failure to create NodePools
	GKEServiceAccountPermissionError metrics.FailedScaleUpReason = "GKE_SA_PERMISSIONS_ERROR"
)

const (
	// groupTypeQueuedProvisioning is the value of the group_type label set in the
	// created_node_groups_total and deleted_node_groups_total metrics.
	groupTypeQueuedProvisioning = "queued-provisioning"
)

type ReportingInitializerWrapper struct {
	nodegroups.AsyncNodeGroupInitializer
	InitializeFunc func(result interfaces.AsyncCreateNodePoolResult)
}

func (w *ReportingInitializerWrapper) InitializeNodeGroup(result interfaces.AsyncCreateNodePoolResult) {
	w.InitializeFunc(result)
}

// AutoprovisioningNodeGroupManager is responsible for creating/deleting autoprovisioned node groups.
type AutoprovisioningNodeGroupManager struct {
	cloudProvider           napcloudprovider.AutoprovisioningCloudProvider
	nodeGroupBackoff        backoff.CompositeBackoff
	scaleBlockingProcessor  *scaleblocking.Processor
	reservationsPuller      *gceclient.ReservationsPuller
	reservationBlocksPuller *reservations.BlocksPuller
	machineSelector         machineselection.Selector
	computeClassLister      computeclass_lister.Lister

	provisioningLabelEnabled         bool
	tpuAutoprovisioningEnabled       bool
	asyncNodeGroupDeletionEnabled    bool
	enableUserAnyZoneSelection       bool
	maxAutoprovisionedNodeGroupCount int

	specGenerators                  []NodePoolSpecGenerator
	nodeGroupRequirementsGenerators []NodeGroupRequirementsGenerator
	nodeGroupOptionsGenerators      []NodeGroupOptionsGenerator

	randInt func(max int) int
}

// AutoprovisioningNodeGroupManagerOptions defines arguments used to create new
// instance of Autoprovisoning node group manager.
type AutoprovisioningNodeGroupManagerOptions struct {
	CloudProvider                    napcloudprovider.AutoprovisioningCloudProvider
	Backoff                          backoff.CompositeBackoff
	ScaleBlockingProcessor           *scaleblocking.Processor
	ReservationsPuller               *gceclient.ReservationsPuller
	ReservationBlocksPuller          *reservations.BlocksPuller
	ResourcePolicyPuller             placement.ResourcePolicyPuller
	Lister                           computeclass_lister.Lister
	Matcher                          networking.Matcher
	AllowlistedSystemLabelsMatcher   *labels.Matcher
	ExperimentsManager               experiments.Manager
	PodLister                        kubernetes.PodLister
	ResizableMachineTypesProvider    config.Provider[sets.Set[string]]
	MaxAutoprovisionedNodeGroupCount int
	Flags                            AutoprovisioningNodeGroupManagerFlags
	OptionsTracker                   *optstracking.OptionsTracker
}

// AutoprovisioningNodeGroupManagerFlags defines flags responsible for enablement of autoprovisioning features.
type AutoprovisioningNodeGroupManagerFlags struct {
	ReservationFlags
	ProvisioningLabelEnabled       bool
	TpuAutoprovisioningEnabled     bool
	MultiNetworkingEnabled         bool
	BootDiskConfigEnabled          bool
	AsyncNodeGroupsDeletionEnabled bool
	EnableUserAnyZoneSelection     bool
	EnableComputeClassMinCapacity  bool
}

// ReservationFlags defines flags impacting autoprovisioning with reservations.
type ReservationFlags struct {
	SpecificTypeReservationMatchEnabled bool
	SpecificTypeReservationsEnabled     bool
	// ReservationsAnyLocationPolicyOverride  Whether to enforce location policy ANY in NAP managed node groups using reservations.
	// It is used e.g. to query Recommend Locations API before every scale-up, as it has the knowledge about all (even cross org) reservations."
	ReservationsAnyLocationPolicyOverride bool
}

// NewAutoprovisioningNodeGroupManager creates a new instance of AutoprovisioningNodeGroupManager.
func NewAutoprovisioningNodeGroupManager(opts AutoprovisioningNodeGroupManagerOptions) *AutoprovisioningNodeGroupManager {
	machineSelector := machineselection.Selector{
		CloudProvider:      opts.CloudProvider,
		ExperimentsManager: opts.ExperimentsManager,
	}

	machineSelectionGenerator := NewMachineSelectionGenerator(opts.CloudProvider, machineSelector, opts.ResizableMachineTypesProvider)
	preemptionGenerator := NewPreemeptionOptionGenerator(opts.Flags.ProvisioningLabelEnabled)
	computeClassGenerator := NewComputeClassGenerator(opts.CloudProvider, opts.Lister, opts.Flags.EnableComputeClassMinCapacity, opts.ExperimentsManager)
	// initialGenerators is a slice with generators that need to be run before others.
	// For example - GPU request generator needs to be run before machine selection generator,
	// so that GPU is known before we select machine.
	initialGenerators := []NodePoolSpecGenerator{
		NewGpuRequestGenerator(opts.CloudProvider),
	}
	specGenerators := []NodePoolSpecGenerator{
		NewWorkloadSeparationGenerator(opts.AllowlistedSystemLabelsMatcher),
		NewCSNGenerator(opts.OptionsTracker.Options().CSNEnabled),
		machineSelectionGenerator,
		NewPlacementGroupGenerator(opts.CloudProvider, opts.ResourcePolicyPuller),
		NewSandboxTypeGenerator(),
		preemptionGenerator,
		NewConsolidationDelayGenerator(opts.ExperimentsManager),
	}
	mppnGenerator := NewMaxPodsPerNodeGenerator(opts.CloudProvider, opts.PodLister)
	var nodeGroupRequirementsGenerators []NodeGroupRequirementsGenerator
	nodeGroupOptionsGenerators := []NodeGroupOptionsGenerator{
		machineSelectionGenerator,
		preemptionGenerator,
		mppnGenerator,
		// This generator only filters invalid options, therefore it must be at the end to make sure no invalid options get generated afterwards
		// The responsibility of generating and filtering options should be separated in the future
		NewDWSSupportFilteringGenerator(opts.CloudProvider.MachineConfigProvider()),
	}

	projectId, _, _ := opts.CloudProvider.GetClusterInfo()
	rg := NewReservationGenerator(opts.ReservationsPuller, opts.Flags.ReservationFlags, projectId, opts.ExperimentsManager, opts.ReservationBlocksPuller)
	// b/355142536 relies on the fact that reservation generator is added to the front of the list.
	// Order of the list determines in which order the generators are executed, for example, in
	// func (m *AutoprovisioningNodeGroupManager) extractRequirements
	specGenerators = append([]NodePoolSpecGenerator{rg}, specGenerators...)
	if opts.Flags.SpecificTypeReservationMatchEnabled {
		nodeGroupOptionsGenerators = append(nodeGroupOptionsGenerators, rg)
	}
	if opts.Flags.TpuAutoprovisioningEnabled {
		// we want TPURequestGenerator to be run before others - e.g. reservations
		// generator, so that TPU config is known upfront.
		initialGenerators = append(initialGenerators, NewTpuRequestGenerator(opts.CloudProvider))
	}
	generator := NewExtendedDurationPodGenerator(opts.CloudProvider)
	specGenerators = append(specGenerators, generator)
	nodeGroupOptionsGenerators = append(nodeGroupOptionsGenerators, generator)
	specGenerators = append(specGenerators, computeClassGenerator)
	nodeGroupRequirementsGenerators = append(nodeGroupRequirementsGenerators, computeClassGenerator)
	selfServiceGenerator := NewSelfServiceGenerator()
	specGenerators = append(specGenerators, selfServiceGenerator)
	podIsolationCPULabelGenerator := NewPodIsolationLabelGenerator(opts.CloudProvider)
	podCapacityLabelGenerator := NewPodCapacityLabelGenerator(opts.CloudProvider)
	specGenerators = append(specGenerators, podIsolationCPULabelGenerator, podCapacityLabelGenerator)
	nodeGroupOptionsGenerators = append(nodeGroupOptionsGenerators, podIsolationCPULabelGenerator, podCapacityLabelGenerator)
	localSSDConfigGenerator := NewLocalSSDConfigGenerator(opts.CloudProvider)
	specGenerators = append(specGenerators, localSSDConfigGenerator)
	if opts.Flags.MultiNetworkingEnabled {
		generator := NewMultiNetworkingGenerator(opts.Matcher)
		specGenerators = append(specGenerators, generator)
	}
	if opts.AllowlistedSystemLabelsMatcher != nil {
		generator := NewSystemLabelsGenerator(opts.AllowlistedSystemLabelsMatcher)
		specGenerators = append(specGenerators, generator)
	}
	specGenerators = append(specGenerators, NewFlexStartGenerator(opts.ExperimentsManager))
	specGenerators = append(specGenerators, NewMaxRunDurationGenerator(opts.ExperimentsManager, opts.CloudProvider))
	specGenerators = append(specGenerators, NewLinuxNodeConfigGenerator())
	specGenerators = append(specGenerators, NewKubeletConfigGenerator())
	specGenerators = append(specGenerators, mppnGenerator)

	szGenerator := NewSpecifiedZonesGenerator(opts.CloudProvider, opts.Flags.EnableUserAnyZoneSelection, opts.OptionsTracker)
	specGenerators = append(specGenerators, szGenerator)
	nodeGroupOptionsGenerators = append(nodeGroupOptionsGenerators, szGenerator)

	specGenerators = append(specGenerators, NewResourceLabelsGenerator())
	specGenerators = append(specGenerators, NewProvisioningRequestGenerator(opts.ExperimentsManager, opts.CloudProvider))
	specGenerators = append(specGenerators, NewAcceleratorSliceGenerator(opts.CloudProvider))
	specGenerators = append(specGenerators, NewNodeVersionGenerator())

	cnGenerator := NewConfidentialNodeGenerator(opts.CloudProvider)
	specGenerators = append(specGenerators, cnGenerator)
	nodeGroupOptionsGenerators = append(nodeGroupOptionsGenerators, cnGenerator)

	bootDiskGenerator := NewBootDiskConfigGenerator(opts.CloudProvider)
	initialGenerators = append(initialGenerators, bootDiskGenerator)
	specGenerators = append(initialGenerators, specGenerators...)

	nodePoolBuilders := make([]napcloudprovider.NodePoolSpecBuilder, len(specGenerators))
	for i := range specGenerators {
		nodePoolBuilders[i] = specGenerators[i]
	}
	opts.CloudProvider.RegisterNodePoolSpecBuilders(nodePoolBuilders)
	return &AutoprovisioningNodeGroupManager{
		cloudProvider:                    opts.CloudProvider,
		nodeGroupBackoff:                 opts.Backoff,
		scaleBlockingProcessor:           opts.ScaleBlockingProcessor,
		reservationsPuller:               opts.ReservationsPuller,
		machineSelector:                  machineSelector,
		provisioningLabelEnabled:         opts.Flags.ProvisioningLabelEnabled,
		tpuAutoprovisioningEnabled:       opts.Flags.TpuAutoprovisioningEnabled,
		asyncNodeGroupDeletionEnabled:    opts.Flags.AsyncNodeGroupsDeletionEnabled,
		enableUserAnyZoneSelection:       opts.Flags.EnableUserAnyZoneSelection,
		specGenerators:                   specGenerators,
		nodeGroupRequirementsGenerators:  nodeGroupRequirementsGenerators,
		nodeGroupOptionsGenerators:       nodeGroupOptionsGenerators,
		computeClassLister:               opts.Lister,
		maxAutoprovisionedNodeGroupCount: opts.MaxAutoprovisionedNodeGroupCount,
		randInt:                          rand.Intn,
	}
}

// Process adds autoprovisioned node groups based on unschedulable pods.
func (m *AutoprovisioningNodeGroupManager) Process(ctx *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo, unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	// NAP reports its status via ProcessorCallbacks to e.g. the visibility processors.
	status := NewProcessingStatus()
	ctx.ProcessorCallbacks.SetExtraValue(ProcessingStatusContextKey, status)

	// WATCH OUT: This is called and returned even if NAP is disabled.
	if m.scaleBlockingProcessor != nil {
		nodeGroups = m.scaleBlockingProcessor.FilterNoScaleUpNodeGroups(ctx, nodeGroups)
	}

	// Exit early if NAP is disabled.
	if !m.cloudProvider.IsNodeAutoprovisioningEnabled() {
		status.SetResult(NapDisabled)
		return nodeGroups, nodeInfos, nil
	}

	// Update/clean up internal state.
	m.nodeGroupBackoff.RemoveStaleBackoffData(time.Now())

	// TODO: Do we still need this if we decided to set MaxAutoprovisionedNodeGroupCount to 999 (~infinity) anyway?
	if autoprovisionedNodeGroupsCount(nodeGroups) >= m.maxAutoprovisionedNodeGroupCount {
		klog.V(4).Infof("Max autoprovisioned node group count reached")
		status.SetResult(MaxAutoprovisionedNodeGroupsLimitReached)
		return nodeGroups, nodeInfos, nil
	}

	// Prepare all common data and components required for injecting new node groups.
	injectionCtx, err := m.prepareInjectionContext(ctx, nodeGroups, nodeInfos, status)
	if err != nil {
		// prepareInjectionContext sets its own result on the status object in case of errors.
		return nil, nil, err
	}

	// TODO: consider refactoring this part as discussed in b/483284226
	totalInjected := 0
	for _, nonGpuRequirements := range m.nonGpuPodsRequirements(injectionCtx, unschedulablePods) {
		injected := m.injectNodeGroups(injectionCtx, nonGpuRequirements)
		if injected == 0 {
			klog.Infof("NAP: didn't inject any node groups for requirements=<%s>", nonGpuRequirements.String())
		} else {
			klog.Infof("NAP: injected node groups for requirements count=%d requirements=<%s>", injected, nonGpuRequirements.String())
		}
		totalInjected += injected
	}
	for _, gpuRequirements := range m.gpuPodsRequirements(injectionCtx, unschedulablePods) {
		injected := m.injectNodeGroups(injectionCtx, gpuRequirements)
		if injected == 0 {
			klog.Infof("NAP: didn't inject any node groups for requirements=<%s>", gpuRequirements.String())
		} else {
			klog.Infof("NAP: injected node groups for requirements count=%d requirements=<%s>", injected, gpuRequirements.String())
		}
		totalInjected += injected
	}
	if m.tpuAutoprovisioningEnabled {
		for _, tpuRequirements := range m.tpuPodsRequirements(injectionCtx, unschedulablePods) {
			injected := m.injectNodeGroups(injectionCtx, tpuRequirements)
			if injected == 0 {
				klog.Infof("NAP: didn't inject any node groups for requirements=<%s>", tpuRequirements.String())
			} else {
				klog.Infof("NAP: injected node groups for requirements count=%d requirements=<%s>", injected, tpuRequirements.String())
			}
			totalInjected += injected
		}
	}

	snapshotter := ctx.DebuggingSnapshotter.(*gkedebuggingsnapshot.GkeDebuggingSnapshotter)
	if !snapshotter.IsSnapshotterDisabled() {
		snapshotter.CacheTemplateNodeLastUsedByNAP(virtualNodeInfos(injectionCtx.allNodeGroups(), injectionCtx.nodeInfos))
	}

	klog.V(4).Infof("NAP: injected node groups total_count=%d", totalInjected)
	return injectionCtx.allNodeGroups(), injectionCtx.nodeInfos, nil
}

// CreateNodeGroup creates autoprovisioned node group.
func (m *AutoprovisioningNodeGroupManager) CreateNodeGroup(context *context.AutoscalingContext, nodeGroup cloudprovider.NodeGroup) (nodegroups.CreateNodeGroupResult, errors.AutoscalerError) {
	if !m.cloudProvider.IsNodeAutoprovisioningEnabled() {
		return nodegroups.CreateNodeGroupResult{}, errors.NewAutoscalerErrorf(errors.InternalError, "tried to create a node group, but autoprovisioning is disabled node_group=%s", nodeGroup.Id())
	}

	var mig interfaces.AutoprovisionedNodeGroup
	var ok bool
	if mig, ok = nodeGroup.(interfaces.AutoprovisionedNodeGroup); !ok {
		return nodegroups.CreateNodeGroupResult{}, errors.NewAutoscalerErrorf(errors.InternalError, "Unsupported nodeGroup of type %T", nodeGroup)
	}

	// Node group id may change when we create node group and we need to update
	// our data structures
	oldId := nodeGroup.Id()

	createResult, err := mig.AutoprovisionedCreate()
	return m.reportNodePoolCreation(context, oldId, mig, createResult, err)
}

// CreateNodeGroupAsync asynchronously created autoprovisioned node group.
func (m *AutoprovisioningNodeGroupManager) CreateNodeGroupAsync(context *context.AutoscalingContext, nodeGroup cloudprovider.NodeGroup, initializer nodegroups.AsyncNodeGroupInitializer) (nodegroups.CreateNodeGroupResult, errors.AutoscalerError) {
	if !m.cloudProvider.IsNodeAutoprovisioningEnabled() {
		return nodegroups.CreateNodeGroupResult{}, errors.NewAutoscalerErrorf(errors.InternalError, "tried to create node group, but autoprovisioning is disabled node_group=%s", nodeGroup.Id())
	}

	mig, ok := nodeGroup.(interfaces.AutoprovisionedNodeGroup)
	if !ok {
		return nodegroups.CreateNodeGroupResult{}, errors.NewAutoscalerErrorf(errors.InternalError, "Unsupported nodeGroup of type %T", nodeGroup)
	}

	oldId := nodeGroup.Id()
	napInitializer := &ReportingInitializerWrapper{
		initializer,
		func(result interfaces.AsyncCreateNodePoolResult) {
			_, err := m.reportNodePoolCreation(context, oldId, mig, result.CreationResult, result.Error)
			processorResult := nodegroups.AsyncNodeGroupCreationResult{
				CreatedToUpcomingMapping: result.CreatedToUpcomingMapping,
				CreationResult:           convertCreateNodeGroupResult(result.CreationResult),
				Error:                    err,
			}
			initializer.InitializeNodeGroup(processorResult)
		},
	}
	klog.Infof("NAP: Initiating async node pool creation %s (mig: %s)", mig.NodePoolName(), mig.Id())
	start := time.Now()
	createResult, err := mig.CreateAsync(initializer, napInitializer)
	if err != nil {
		klog.Errorf("NAP: Failed initiating async node pool creation %s [time: %s] (mig: %s, upcomingMigs: %v): %v", mig.NodePoolName(), time.Since(start), mig.Id(), nodeGroupNames(createResult.AllCreatedNodeGroups()), err)
		return m.reportNodePoolCreation(context, oldId, mig, createResult, err)
	}
	klog.Infof("NAP: Initiated async node pool creation %s [time: %s] (mig: %s, upcomingMigs: %v)", mig.NodePoolName(), time.Since(start), mig.Id(), nodeGroupNames(createResult.AllCreatedNodeGroups()))

	// no reporting as node group is just scheduled to be created
	return convertCreateNodeGroupResult(createResult), nil
}

func mapTargetSizesToInt(sizes map[string]int64) map[string]int {
	mapped := make(map[string]int, len(sizes))
	for key, value := range sizes {
		casted := int(value)
		if int64(casted) == value {
			mapped[key] = casted
		} else {
			// TODO(b/517093780): It's already fixed in DWS integration gkecl/1130820 where int64 is passed to OSS initiator as it suppose to be.
			klog.Errorf("Target size for async created node group exceeded max int value. Using max int value instead target_size=%v max_int=%v", value, math.MaxInt)
			mapped[key] = math.MaxInt
		}
	}
	return mapped
}

func (m *AutoprovisioningNodeGroupManager) reportNodePoolCreation(
	ctx *context.AutoscalingContext,
	oldId string,
	mig interfaces.AutoprovisionedNodeGroup,
	createResult interfaces.CreateNodePoolResult,
	err error,
) (nodegroups.CreateNodeGroupResult, errors.AutoscalerError) {
	if err != nil {
		errCode := ""
		if autoscalerErr, ok := err.(errors.AutoscalerError); ok && autoscalerErr.Type() == gkeclient.GkePersistentOperationError {
			// If we support temporary type of error for gkeOperation, we may split gkeclient.GkePersistentOperationError to
			// errorClass=NodePoolCreationErrorClass and errCode = "persistent" / "temporary".
			errCode = gceclient.GkePersistentOperationError
		}
		m.nodeGroupBackoff.BackoffInAllZones(
			mig,
			m.cloudProvider.GetAutoprovisioningLocations(),
			nil,
			cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OtherErrorClass, ErrorCode: errCode, ErrorMessage: err.Error()}, time.Now(),
		)

		ctx.LogRecorder.Eventf(
			apiv1.EventTypeWarning,
			"FailedToCreateNodeGroup",
			"NodeAutoprovisioning: attempt to create node group failed node_group=%v err=%v",
			oldId,
			err)

		reason := recognizeFailedScaleUpReason(err)
		availableGPUTypes := ctx.CloudProvider.GetAvailableGPUTypes()
		gpuResource, gpuType := "", ""
		nodeInfo, templErr := mig.TemplateNodeInfo()
		if templErr != nil {
			klog.Warningf("Failed to get template node info for a node group node_group=%s", templErr)
		} else {
			gpuResource, gpuType = gpu.GetGpuInfoForMetrics(ctx.CloudProvider.GetNodeGpuConfig(nodeInfo.Node()), availableGPUTypes, nodeInfo.Node(), mig)
		}
		metrics.RegisterFailedScaleUp(reason, gpuResource, gpuType, "")

		return nodegroups.CreateNodeGroupResult{}, errors.ToAutoscalerError(errors.AutoscalerErrorType(reason), err)
	}
	newId := createResult.MainCreatedNodeGroup.Id()
	if newId != oldId {
		klog.V(2).Infof("Created node group based on template node group, will use new node group in scale-up node_group=%s template_node_group=%s", newId, oldId)
	}
	ctx.LogRecorder.Eventf(
		apiv1.EventTypeNormal,
		"CreatedNodeGroup",
		"NodeAutoprovisioning: created new node group %v",
		newId)

	for _, extraNodeGroup := range createResult.ExtraCreatedNodeGroups {
		ctx.LogRecorder.Eventf(
			apiv1.EventTypeNormal,
			"CreatedNodeGroup",
			"NodeAutoprovisioning: created new node group %v",
			extraNodeGroup.Id())
	}

	if mig.QueuedProvisioning() {
		metrics.RegisterNodeGroupCreationWithLabelValues(groupTypeQueuedProvisioning)
	} else {
		metrics.RegisterNodeGroupCreation()
	}
	return convertCreateNodeGroupResult(createResult), nil
}

func recognizeFailedScaleUpReason(err error) metrics.FailedScaleUpReason {
	if gke.IsServiceAccountDeletedError(err) {
		return metrics.FailedScaleUpReason(gkeclient.ServiceAccountDeleted)
	} else if gke.IsProjectMetadataStartupScriptUrlConflict(err) {
		return StartupScriptUrlConflict
	} else if gke.IsGKEServiceAccountPermissionError(err) {
		return metrics.FailedScaleUpReason(GKEServiceAccountPermissionError)
	} else if gke.IsOutOfQuotaError(err) {
		// this check is quite sensitive - it catches everything based on one word only - 'quota',
		// so keep it as the last one
		return metrics.FailedScaleUpReason(gce_cloudprovider.ErrorCodeQuotaExceeded)
	}
	return metrics.CloudProviderError
}

func convertCreateNodeGroupResult(createResult interfaces.CreateNodePoolResult) nodegroups.CreateNodeGroupResult {
	result := nodegroups.CreateNodeGroupResult{}
	result.MainCreatedNodeGroup = createResult.MainCreatedNodeGroup
	result.ExtraCreatedNodeGroups = make([]cloudprovider.NodeGroup, 0, len(createResult.ExtraCreatedNodeGroups))
	for _, mig := range createResult.ExtraCreatedNodeGroups {
		result.ExtraCreatedNodeGroups = append(result.ExtraCreatedNodeGroups, mig)
	}
	return result
}

// RemoveUnneededNodeGroups removes node groups that are not needed anymore.
func (m *AutoprovisioningNodeGroupManager) RemoveUnneededNodeGroups(context *context.AutoscalingContext) (removedNodeGroups []cloudprovider.NodeGroup, err error) {
	if !m.cloudProvider.IsNodeAutoprovisioningEnabled() {
		return nil, nil
	}
	unblockedNodeGroups, err := m.gkeNodeGroups(context)
	if err != nil {
		return nil, err
	}
	serverErrorNodeGroups, err := toAutoprovisionedNodeGroups(m.cloudProvider.NodeGroupsBlockedByServerError())
	if err != nil {
		return nil, err
	}
	notFoundNodeGroups, err := toAutoprovisionedNodeGroups(m.cloudProvider.NodeGroupsBlockedByNotFoundError())
	if err != nil {
		return nil, err
	}
	notFoundNodeGroupIds := nodeGroupIds(notFoundNodeGroups)
	nodePoolMigs := slices.Concat(unblockedNodeGroups, serverErrorNodeGroups, notFoundNodeGroups)

	// Find candidates for deletion.
	unneededNodeGroups := map[string]interfaces.AutoprovisionedNodeGroup{}
	for _, nodePoolMig := range nodePoolMigs {
		if !nodePoolMig.Autoprovisioned() {
			continue
		}
		if !m.cloudProvider.UseAutoprovisioningFeaturesForNodeGroup(nodePoolMig) {
			continue
		}

		// missing node groups have no targetSizes nor nodes
		if notFoundNodeGroupIds.Has(nodePoolMig.Id()) {
			unneededNodeGroups[nodePoolMig.Id()] = nodePoolMig
			continue
		}

		targetSize, err := nodePoolMig.TargetSize()
		if err != nil {
			return nil, err
		}
		if targetSize > 0 {
			continue
		}

		// We are only able to delete a MIG if there are no ongoing operations on it.
		isStable, err := nodePoolMig.IsStable()
		if err != nil {
			return nil, err
		}
		if !isStable {
			continue
		}

		unneededNodeGroups[nodePoolMig.Id()] = nodePoolMig
	}
	// If a group isn't a candidate, mark its node pool as needed.
	neededNodePools := map[string]bool{}
	for _, nodePoolMig := range nodePoolMigs {
		if _, found := unneededNodeGroups[nodePoolMig.Id()]; !found {
			neededNodePools[nodePoolMig.NodePoolName()] = true
		}
	}
	// We only remove one group per loop, so we don't need to worry about removing twice from the same unneeded node pool.
	for _, nodePoolMig := range unneededNodeGroups {
		poolName := nodePoolMig.NodePoolName()
		if _, found := neededNodePools[poolName]; found {
			continue
		}
		if m.asyncNodeGroupDeletionEnabled {
			return m.deleteNodeGroupAsync(context, nodePoolMig)
		}
		err := nodePoolMig.Delete()
		m.reportNodePoolDeletion(context, nodePoolMig, err)
		if err != nil {
			return nil, err
		}
		return []cloudprovider.NodeGroup{nodePoolMig}, nil
	}
	return nil, nil
}

func (m *AutoprovisioningNodeGroupManager) deleteNodeGroupAsync(context *context.AutoscalingContext, nodePoolMig interfaces.AutoprovisionedNodeGroup) ([]cloudprovider.NodeGroup, error) {
	finalizer := interfaces.AsyncNodeGroupFinalizerFunc(func(result interfaces.AsyncDeleteNodePoolResult) {
		m.reportNodePoolDeletion(context, nodePoolMig, result.Error)
	})
	klog.Infof("NAP: Initiating async node pool deletion %s (triggering mig: %s)", nodePoolMig.NodePoolName(), nodePoolMig.Id())
	start := time.Now()
	err := nodePoolMig.DeleteAsync(finalizer)
	if err != nil {
		klog.Errorf("NAP: Failed initiating async node pool deletion %s [time: %s] (triggering mig: %s): %v", nodePoolMig.NodePoolName(), time.Since(start), nodePoolMig.Id(), err)
		m.reportNodePoolDeletion(context, nodePoolMig, err)
		return nil, err
	}
	klog.Infof("NAP: Initiated async node pool deletion %s [time: %s] (triggering mig: %s)", nodePoolMig.NodePoolName(), time.Since(start), nodePoolMig.Id())
	return []cloudprovider.NodeGroup{nodePoolMig}, nil
}

func (m *AutoprovisioningNodeGroupManager) reportNodePoolDeletion(context *context.AutoscalingContext, nodePoolMig interfaces.AutoprovisionedNodeGroup, err error) {
	if err != nil {
		context.LogRecorder.Eventf(apiv1.EventTypeWarning, "FailedToDeleteNodeGroup",
			"NodeAutoprovisioning: attempt to delete node group failed node_group=%v err=%v", nodePoolMig.Id(), err)
		// TODO(b/517094047): add some metric here after figuring out failure scenarios
		return
	}
	context.LogRecorder.Eventf(apiv1.EventTypeNormal, "DeletedNodeGroup",
		"NodeAutoprovisioning: removed node group node_group=%v", nodePoolMig.Id())

	if nodePoolMig.QueuedProvisioning() {
		metrics.RegisterNodeGroupDeletionWithLabelValues(groupTypeQueuedProvisioning)
	} else {
		metrics.RegisterNodeGroupDeletion()
	}
}

func (m *AutoprovisioningNodeGroupManager) gkeNodeGroups(context *context.AutoscalingContext) ([]interfaces.AutoprovisionedNodeGroup, error) {
	nodeGroups := m.cloudProvider.NodeGroups()
	if m.scaleBlockingProcessor != nil {
		nodeGroups = m.scaleBlockingProcessor.FilterNoScaleDownNodeGroups(context, nodeGroups)
	}
	return toAutoprovisionedNodeGroups(nodeGroups)
}

func toAutoprovisionedNodeGroups(nodeGroups []cloudprovider.NodeGroup) ([]interfaces.AutoprovisionedNodeGroup, error) {
	nodePoolMigs := make([]interfaces.AutoprovisionedNodeGroup, len(nodeGroups))
	for i, nodeGroup := range nodeGroups {
		nodePoolMig, ok := nodeGroup.(interfaces.AutoprovisionedNodeGroup)
		if !ok {
			return nil, errors.NewAutoscalerErrorf(errors.InternalError, "Mig isn't a GkeNodeGroup want *gke.gkeMig got %v", reflect.TypeOf(nodeGroup))
		}
		nodePoolMigs[i] = nodePoolMig
	}
	return nodePoolMigs, nil
}

// CleanUp cleans up the processor's internal structures.
func (m *AutoprovisioningNodeGroupManager) CleanUp() {}

func nodeGroupIds(nodeGroups []interfaces.AutoprovisionedNodeGroup) set.Set[string] {
	result := set.New[string]()
	for _, ng := range nodeGroups {
		result.Insert(ng.Id())
	}
	return result
}

func nodeGroupNames(nodeGroups []interfaces.AutoprovisionedNodeGroup) []string {
	result := make([]string, 0, len(nodeGroups))
	for _, ng := range nodeGroups {
		result = append(result, ng.Id())
	}
	return result
}

// isDraTpuPod returns true if the pod requesting TPU through DRA APIs
func (m *AutoprovisioningNodeGroupManager) isDraTpuPod(pod *apiv1.Pod) bool {
	podCC, _, _ := m.computeClassLister.PodCrd(pod)
	if podCC == nil {
		return false
	}

	isTpuDriverMode := podCC.TpuDriverMode() == computeclass.TpuDriverModeDynamicResourceAllocation
	return isTpuDriverMode && len(pod.Spec.ResourceClaims) > 0
}

// isTpuPod returns true if the pod requesting TPU through device plugin or DRA
func (m *AutoprovisioningNodeGroupManager) isTpuPod(pod *apiv1.Pod) bool {
	return tpu.HasTpuPodRequests(pod) || m.isDraTpuPod(pod)
}

// isGpuPod returns true if the pod requesting GPU through device plugin
func (m *AutoprovisioningNodeGroupManager) isGpuPod(pod *apiv1.Pod) bool {
	return gpu.PodRequestsGpu(pod)
}
