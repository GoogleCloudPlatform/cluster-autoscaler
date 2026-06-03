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

package impostor

import (
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	cacontext "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/pdb"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	ca_processors "k8s.io/autoscaler/cluster-autoscaler/processors"
	"k8s.io/autoscaler/cluster-autoscaler/processors/callbacks"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupconfig"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodes"
	"k8s.io/autoscaler/cluster-autoscaler/processors/scaledowncandidates"
	"k8s.io/autoscaler/cluster-autoscaler/processors/scaledowncandidates/emptycandidates"
	"k8s.io/autoscaler/cluster-autoscaler/processors/scaledowncandidates/previouscandidates"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/predicate"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/store"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	base_backoff "k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	kubeutil "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	npc_crd "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	gkeoptions "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/gkedebuggingsnapshot"
	internal_customresources "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/customresources"
	scaledown_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaledown"
)

const (
	defaultMaxScaleDownParallelism    = 1
	defaultMaxDrainParallelism        = 1
	defaultScaleDownSimulationTimeout = 1 * time.Second
	defaultPredicateParallelism       = 1
)

// TestAutoscaler wraps ScaleUp and ScaleDown logic.
type TestAutoscaler struct {
	context  *cacontext.AutoscalingContext
	provider *MockCloudProvider
	sd       *ScaleDown
}

// CreateAutoscalingOptions create autoscaling options.
func CreateAutoscalingOptions(maxGracefulTerminationSec int, scaleDownSimulationTimeout, maxPodEvictionTime time.Duration, maxScaleDownParallelism int, maxDrainParallelism int) *config.AutoscalingOptions {
	return &config.AutoscalingOptions{
		MaxGracefulTerminationSec:  maxGracefulTerminationSec,
		MaxPodEvictionTime:         maxPodEvictionTime,
		MaxScaleDownParallelism:    maxScaleDownParallelism,
		MaxDrainParallelism:        maxDrainParallelism,
		ScaleDownSimulationTimeout: scaleDownSimulationTimeout,
	}
}

// CreateAutoscalingContext create autoscaling context based on provided parameters.
func CreateAutoscalingContext(opts *config.AutoscalingOptions, provider *MockCloudProvider, kubeClients cacontext.AutoscalingKubeClients) *cacontext.AutoscalingContext {
	debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(false)
	fwHandle, err := framework.NewTestFrameworkHandle()
	if err != nil {
		return nil
	}
	return &cacontext.AutoscalingContext{
		AutoscalingOptions:     *opts,
		CloudProvider:          provider,
		FrameworkHandle:        fwHandle,
		ClusterSnapshot:        predicate.NewPredicateSnapshot(store.NewBasicSnapshotStore(), fwHandle, opts.DynamicResourceAllocationEnabled, defaultPredicateParallelism, opts.CSINodeAwareSchedulingEnabled),
		ProcessorCallbacks:     callbacks.NewTestProcessorCallbacks(),
		AutoscalingKubeClients: kubeClients,
		DebuggingSnapshotter:   debuggingSnapshotter,
		RemainingPdbTracker:    pdb.NewBasicRemainingPdbTracker(),
	}
}

// CreateAutoscalingProcessors creates Autoscaling Processors to be used by autoscaler.
func CreateAutoscalingProcessors(opts *config.AutoscalingOptions, context *cacontext.AutoscalingContext, provider *MockCloudProvider, snapshot clustersnapshot.ClusterSnapshot) *ca_processors.AutoscalingProcessors {
	processors := ca_processors.AutoscalingProcessors{}
	scaleDownNodeProcessors := []nodes.ScaleDownNodeProcessor{
		nodes.NewPreFilteringScaleDownNodeProcessor(),
	}

	processors.NodeGroupManager = nodegroups.NewDefaultNodeGroupManager()
	customResourceProcessor := gke.NewMockProcessor()
	customResourceProcessor.SetContext(context)
	processors.CustomResourcesProcessor = customResourceProcessor
	processors.NodeGroupConfigProcessor = nodegroupconfig.NewDefaultNodeGroupConfigProcessor(opts.NodeGroupDefaults)

	processors.ScaleDownCandidatesNotifier = scaledowncandidates.NewObserversList()
	sdCandidatesSorting := previouscandidates.NewPreviousCandidates()
	processors.ScaleDownCandidatesNotifier.Register(sdCandidatesSorting)

	deleteOpts := options.NewNodeDeleteOptions(*opts)
	sortingProcessor := scaledowncandidates.NewScaleDownCandidatesSortingProcessor([]scaledowncandidates.CandidatesComparer{
		emptycandidates.NewEmptySortingProcessor(emptycandidates.NewNodeInfoGetter(snapshot), deleteOpts, rules.Default(deleteOpts)),
		sdCandidatesSorting,
	})
	scaleDownNodeProcessors = append(scaleDownNodeProcessors, sortingProcessor)
	processors.ScaleDownNodeProcessor = scaledown_processors.NewGkeInternalAutoscalingScaleDownNodeProcessor(scaleDownNodeProcessors)
	processors.ScaleDownSetProcessor = nodes.NewAtomicResizeFilteringProcessor()

	return &processors
}

// NewParameterizedTestAutoscaler creates a new instance of TestAutoscaler based on provided arguments.
// Initially it is used in scale down test.
func NewParameterizedTestAutoscaler(context *cacontext.AutoscalingContext, provider *MockCloudProvider, processors *ca_processors.AutoscalingProcessors, planner scaledown.Planner, actuator scaledown.Actuator) *TestAutoscaler {
	sd := NewScaleDown(context, planner, actuator, processors)
	autoscaler := &TestAutoscaler{
		provider: provider,
		context:  context,
		sd:       sd,
	}
	return autoscaler
}

// DefaultTestAutoscaler constructs instance of TestAutoscaler
func DefaultTestAutoscaler(provider *MockCloudProvider) *TestAutoscaler {
	dsLister, _ := kubeutil.NewTestDaemonSetLister([]*appsv1.DaemonSet{})
	rcLister, _ := kubeutil.NewTestReplicationControllerLister([]*apiv1.ReplicationController{})
	jobLister, _ := kubeutil.NewTestJobLister([]*batchv1.Job{})
	rsLister, _ := kubeutil.NewTestReplicaSetLister([]*appsv1.ReplicaSet{})
	ssLister, _ := kubeutil.NewTestStatefulSetLister([]*appsv1.StatefulSet{})
	aKC := cacontext.AutoscalingKubeClients{
		ListerRegistry: kubeutil.NewListerRegistry(
			kubeutil.NewTestNodeLister([]*apiv1.Node{}),
			kubeutil.NewTestNodeLister([]*apiv1.Node{}),
			kubeutil.NewTestPodLister([]*apiv1.Pod{}),
			nil,
			dsLister,
			rcLister,
			jobLister,
			rsLister,
			ssLister,
		),
	}
	opts := CreateAutoscalingOptions(0, defaultScaleDownSimulationTimeout, 0*time.Second, defaultMaxScaleDownParallelism, defaultMaxDrainParallelism)
	context := CreateAutoscalingContext(opts, provider, aKC)

	autoscaler := &TestAutoscaler{
		provider: provider,
		context:  context,
	}
	return autoscaler
}

// getNodeInfosForGroups creates NodeInfos for all existing node groups
func (a *TestAutoscaler) getNodeInfosForGroups() map[string]*framework.NodeInfo {
	nodeGroups := a.provider.NodeGroups()
	result := make(map[string]*framework.NodeInfo, len(nodeGroups))
	for _, group := range nodeGroups {
		nodeInfo, err := group.TemplateNodeInfo()
		if err != nil {
			panic(err)
		}
		result[group.Id()] = nodeInfo
	}
	return result
}

// ScaleUpOptions lists all options of scaling up (which node group, how many nodes, how many pods would fit)
func (a *TestAutoscaler) ScaleUpOptions(pods []*apiv1.Pod) ([]expander.Option, map[string]*framework.NodeInfo, error) {
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(a.context)
	resourceBackoff := backoff.NewResourceBackoff(processor, time.Second, time.Second, time.Second)
	em := experiments.NewMockManager()
	opts := autoprovisioning.AutoprovisioningNodeGroupManagerOptions{
		CloudProvider:                    a.provider,
		Backoff:                          backoff.NewCompositeBackoff([]base_backoff.Backoff{resourceBackoff}, nil),
		Lister:                           npc_lister.NewMockCrdLister([]npc_crd.CRD{}),
		PodLister:                        kubeutil.NewTestPodLister(pods),
		MaxAutoprovisionedNodeGroupCount: 50,
		ExperimentsManager:               em,
		ResourcePolicyPuller:             &placement.FakeResourcePolicyPullerProvider{},
		OptionsTracker:                   tracking.FakeOptionsTracker(gkeoptions.AutoscalingOptions{}, gkeclient.Cluster{}, em),
	}
	manager := autoprovisioning.NewAutoprovisioningNodeGroupManager(opts)
	nodeGroups, nodeInfosForGroups, err := manager.Process(
		a.context, a.provider.NodeGroups(), a.getNodeInfosForGroups(), pods)
	if err != nil {
		return nil, nil, err
	}

	options, err := ScaleUp(a.context, pods, nil, nodeGroups, nodeInfosForGroups)
	if err != nil {
		return nil, nil, err
	}
	return options, nodeInfosForGroups, nil
}

// GetNodeGroupsForScaleUp returns node groups and node group infos for scale-ups suitable for the given pods
func (a *TestAutoscaler) GetNodeGroupsForScaleUp(pods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(a.context)
	resourceBackoff := backoff.NewResourceBackoff(processor, time.Second, time.Second, time.Second)
	opts := autoprovisioning.AutoprovisioningNodeGroupManagerOptions{
		CloudProvider:                    a.provider,
		Backoff:                          backoff.NewCompositeBackoff([]base_backoff.Backoff{resourceBackoff}, nil),
		MaxAutoprovisionedNodeGroupCount: 50,
		OptionsTracker:                   tracking.FakeOptionsTracker(gkeoptions.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager()),
	}
	manager := autoprovisioning.NewAutoprovisioningNodeGroupManager(opts)
	nodeGroups, nodeInfosForGroups, err := manager.Process(
		a.context, a.provider.NodeGroups(), a.getNodeInfosForGroups(), pods)
	if err != nil {
		return nil, nil, err
	}
	return nodeGroups, nodeInfosForGroups, nil
}

// GetContext returns the AutoscalingContext of the TestAutoscaler
func (a *TestAutoscaler) GetContext() *cacontext.AutoscalingContext {
	return a.context
}

// BestScaleUpOption selects best option to scale up (which node group, how many nodes, how many pods would fit)
func (a *TestAutoscaler) BestScaleUpOption(strategy expander.Strategy, pods []*apiv1.Pod) (*expander.Option,
	map[string]*framework.NodeInfo, error) {

	options, nodeInfosForGroups, err := a.ScaleUpOptions(pods)
	if err != nil {
		return nil, nil, err
	}
	if len(options) == 0 {
		return nil, nil, errors.NewAutoscalerError(errors.InternalError, "No valid options found")
	}
	option := strategy.BestOption(options, nodeInfosForGroups)
	if option == nil {
		return nil, nil, errors.NewAutoscalerError(errors.InternalError, "No option selected by expander strategy")
	}
	return option, nodeInfosForGroups, nil
}

// ScaleDown tries to perform scale down operation on a cluster.
func (a *TestAutoscaler) ScaleDown() error {
	return a.sd.Run(time.Now())
}
