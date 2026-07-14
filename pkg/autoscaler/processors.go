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

package autoscaler

import (
	ctx "context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/client"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/fakepods"
	"k8s.io/autoscaler/cluster-autoscaler/processors/provreq"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/nodequota"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer"
	cbclient "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/client"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors"
	"k8s.io/autoscaler/cluster-autoscaler/processors/binpacking"
	cbprocessors "k8s.io/autoscaler/cluster-autoscaler/processors/capacitybuffer"
	"k8s.io/autoscaler/cluster-autoscaler/processors/customresources"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	oss_nodeinfosprovider "k8s.io/autoscaler/cluster-autoscaler/processors/nodeinfosprovider"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodes"
	"k8s.io/autoscaler/cluster-autoscaler/processors/podinjection"
	podinjectionbackoff "k8s.io/autoscaler/cluster-autoscaler/processors/podinjection/backoff"
	"k8s.io/autoscaler/cluster-autoscaler/processors/pods"
	"k8s.io/autoscaler/cluster-autoscaler/processors/scaledowncandidates"
	"k8s.io/autoscaler/cluster-autoscaler/processors/scaledowncandidates/emptycandidates"
	"k8s.io/autoscaler/cluster-autoscaler/processors/scaledowncandidates/previouscandidates"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/resourcequotas"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/client-go/informers"
	kube_client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/asyncnodegroups"
	napprovider "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/napcloudprovider"
	gke_backoff "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/bluegreen"
	cr_v1alpha1 "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/apis/internal.autoscaling.gke.io/v1alpha1"
	cr_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/processors"
	cr_utils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalcfg "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	npc_nodeinfosproviders "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/nodeinfosproviders"
	npc_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/processors"
	npc_status "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/history"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller"
	csn_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/daemonsetmutation"
	defrag_plugins "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins"
	defrag_plugins_config "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	defrag_processor "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/processor"
	des_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/dynamicephemeralstorage/processors"
	ekvms_backoff "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/backoff"
	ekvms_customthresholds "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/backoff/customthresholds"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	lookaheadbuffer_processor "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer/processor"
	ekvms_recommender "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/nodesizerecommender"
	ekvms_operationtracker "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	ekvms_processor "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/processor"
	ekvms_providers "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/providers"
	ekvms_size "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/vmreservation"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	internalestimator "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/estimator"
	edps "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/extendeddurationpods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	kubernetes_util "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/kubernetes"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/annotator"
	metricsbinpacking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/binpacking"
	cbmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/capacitybuffer"
	metricsccc "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/filter"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/podstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/nodeannotator"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/nodesnowflake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podsharding"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podtopologyspread"
	internal_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/bulkmig"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/capacitybuffers"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/customresources/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/flexstart"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/locationpolicy"
	metrics_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/nodeinfosprovider"
	nodequota_processor "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/nodequota"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleblocking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaledown"
	scaleup_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleup"
	pr_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	sohw_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/sliceofhardware/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/fairness"
	utils_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/events"
	viz_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/processors"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// GkeCloudProvider is cloud provider interface extended for GKE use cases.
type GkeCloudProvider interface {
	napprovider.AutoprovisioningCloudProvider
	internal_processors.ProcessorsCloudProvider

	GetClusterInfo() (projectId, location, clusterName string)
	GetGkeMigs() []*gke.GkeMig
	GkeMigForNode(node *apiv1.Node) (*gke.GkeMig, error)
	RegisterInitializationFunc(f gke.InitializationFunc)
	QueuedProvisioningNodeHasScaleDownImmunity(*apiv1.Node, *reconciler.QueuedProvisioningMigSpec, time.Time) bool

	ResizeVm(ctx.Context, *apiv1.Node, ekvms_size.VmSize) error
	GetCurrentResizableVmState(*apiv1.Node) (ekvmtypes.ResizableVmState, error)
	BulkFetchCurrentResizableVmStates() (map[gce.GceRef]ekvmtypes.ResizableVmState, error)
	InstanceByRef(ref gce.GceRef) *gce.GceInstance
	ResumeInstances(migRef gce.GceRef, instances []gce.GceRef) error
	SuspendInstances(migRef gce.GceRef, instances []gce.GceRef, forceSuspend bool) error
	CalculatePhysicalEphemeralStorageGiB(mig *gke.GkeMig, allocatableBytes int64) int64
	ResizingEnabled(machineFamily string) bool
	GetNodesScaleDownAllowedFromCache([]string) map[string]bool
	UpdateNodesScaleDownAllowedCache(map[string]bool)
	InvalidateNodesScaleDownAllowedCache()
	ValidateMachineTypeConfig(machineType string, zone string) error

	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

func initCapacityRequestProcessors(bgContext ctx.Context, kubeConfig *rest.Config) (*cr_processors.CapacityRequestPodListProcessor, *cr_processors.CapacityRequestScaleUpProcessor, *cr_utils.CapacityRequestState, bool) {
	crClient, err := cr_utils.NewCrClient(kubeConfig)
	if err != nil {
		klog.Warningf("Failed to create Capacity Request client, will not use Capacity Requests. Error was: %v", err)
		return nil, nil, nil, false
	}
	crLister, err := cr_utils.NewAllCrsLister(bgContext, crClient)
	if err != nil {
		klog.Warningf("Failed to create Capacity Request lister, will not use Capacity Requests. Error was: %v", err)
		return nil, nil, nil, false
	}
	// Add Capacity Requests to scheme to be able to emit events for them.
	if err := cr_v1alpha1.AddToScheme(scheme.Scheme); err != nil {
		klog.Warningf("Failed to add Capacity Request to scheme: %v", err)
	}
	crStatus := cr_utils.NewCapacityRequestState(crClient)

	podListProcessor := cr_processors.NewCapacityRequestPodListProcessor(crStatus, crLister)
	crScaleUpProcessor := cr_processors.NewCapacityRequestScaleUpProcessor(crStatus)

	return podListProcessor, crScaleUpProcessor, crStatus, true
}

func initAutoprovisioningProcessors(
	optionsTracker *optstracking.OptionsTracker,
	options internalopts.AutoscalingOptions,
	gkeCloudProvider GkeCloudProvider,
	backoff gke_backoff.CompositeBackoff,
	scaleBlockingProcessor *scaleblocking.Processor,
	reservationsPuller *gceclient.ReservationsPuller,
	npcCrdLister npc_lister.Lister,
	matcher networking.Matcher,
	allowlistedSystemLabelsMatcher *labels.Matcher,
	experimentsManager experiments.Manager,
	listerRegistry kube_util.ListerRegistry,
	resizableMachineTypesProvider internalcfg.Provider[sets.Set[string]],
	reservationBlocksPuller *reservations.BlocksPuller,
	resourcePolicyPuller placement.ResourcePolicyPuller) (nodegroups.NodeGroupListProcessor, nodegroups.NodeGroupManager) {

	opts := autoprovisioning.AutoprovisioningNodeGroupManagerOptions{
		CloudProvider:                    gkeCloudProvider,
		Backoff:                          backoff,
		ScaleBlockingProcessor:           scaleBlockingProcessor,
		ReservationBlocksPuller:          reservationBlocksPuller,
		ResourcePolicyPuller:             resourcePolicyPuller,
		ReservationsPuller:               reservationsPuller,
		Lister:                           npcCrdLister,
		Matcher:                          matcher,
		AllowlistedSystemLabelsMatcher:   allowlistedSystemLabelsMatcher,
		ExperimentsManager:               experimentsManager,
		PodLister:                        listerRegistry.AllPodLister(),
		ResizableMachineTypesProvider:    resizableMachineTypesProvider,
		MaxAutoprovisionedNodeGroupCount: options.MaxAutoprovisionedNodeGroupCount,
		Flags: autoprovisioning.AutoprovisioningNodeGroupManagerFlags{
			ProvisioningLabelEnabled:   options.ProvisioningLabelEnabled,
			TpuAutoprovisioningEnabled: options.TpuAutoprovisioningEnabled,
			ReservationFlags: autoprovisioning.ReservationFlags{
				SpecificTypeReservationMatchEnabled:   options.SpecificTypeReservationMatchEnabled,
				SpecificTypeReservationsEnabled:       options.SpecificTypeReservationMatchEnabled || options.SpecificTypeReservationWithoutMatchEnabled,
				ReservationsAnyLocationPolicyOverride: options.ReservationsAnyLocationPolicyOverride,
			},
			MultiNetworkingEnabled:         options.MultiNetworkSupportEnabled,
			BootDiskConfigEnabled:          options.BootDiskSelectorEnabled,
			AsyncNodeGroupsDeletionEnabled: options.AsyncNodeGroupsEnabled,
			EnableUserAnyZoneSelection:     options.EnableUserAnyZoneSelection,
			EnableComputeClassMinCapacity:  options.EnableComputeClassMinCapacity,
		},
		OptionsTracker: optionsTracker,
	}
	nodeGroupManager := autoprovisioning.NewAutoprovisioningNodeGroupManager(opts)
	return nodeGroupManager, nodeGroupManager
}

func initVisibilityProcessors(options internalopts.AutoscalingOptions, provider GkeCloudProvider, eventLogger visibility.EventLogger) (*viz_processors.AutoscalingStatusVisibilityProcessor, status.ScaleUpStatusProcessor, status.ScaleDownStatusProcessor, error) {
	var logger visibility.EventLogger
	if options.UseAutoscalerVisibility {
		logger = eventLogger
	}

	sharedData := viz_processors.NewSharedData()
	opts := visibility.VisibilityOptions{EmitNoScaleUpEvents: options.EmitNoScaleUpCAVizEvents, EmitNoScaleDownEvents: options.EmitNoScaleDownCAVizEvents,
		IncludePerMigStatuses: options.IncludePerMigStatusesInCAViz, EmitNapInfo: options.EmitNapInfoInCAViz,
		ScaleUpSimulationForSkippedNodeGroupsEnabled: options.ScaleUpSimulationForSkippedNodeGroupsEnabled}

	failedScaleUpEventLogger := events.NewFailedScaleUpEventLogger()
	autoscalingStatusProcessor := viz_processors.NewAutoscalingStatusVisibilityProcessor(logger, opts, sharedData, failedScaleUpEventLogger)
	scaleUpStatusProcessor := viz_processors.NewScaleUpStatusVisibilityProcessor(logger, opts, sharedData, failedScaleUpEventLogger)
	scaleDownStatusProcessor := viz_processors.NewScaleDownStatusVisibilityProcessor(logger, opts, sharedData)

	return autoscalingStatusProcessor, scaleUpStatusProcessor, scaleDownStatusProcessor, nil
}

func initCapacityBufferMetricsProcessor(experimentsManager experiments.Manager, client *client.CapacityBufferClient, registry *fakepods.Registry, buffersEnabled bool) *capacitybuffers.MetricProcessor {
	if buffersEnabled && experimentsManager.DirectLaunchBoolFlag(experiments.CapacityBuffersMetricProcessor) {
		return capacitybuffers.NewMetricProcessor(client, registry, internalmetrics.Metrics)
	}
	return nil
}

func setUpProcessors(
	context ctx.Context,
	caVersion version.Version,
	optionsTracker *optstracking.OptionsTracker,
	options *internalopts.AutoscalingOptions,
	kubeConfig *rest.Config,
	kubeClient kube_client.Interface,
	httpClient *http.Client,
	provider GkeCloudProvider,
	backoff gke_backoff.CompositeBackoff,
	fetcher kubernetes_util.UpdateInfoFetcher,
	customResourcesProcessor customresources.CustomResourcesProcessor,
	clusterSnapshot clustersnapshot.ClusterSnapshot,
	reservationsPuller *gceclient.ReservationsPuller,
	defragProcessor **defrag_processor.Processor,
	capacitybufferPodsRegistry *fakepods.Registry,
	clusterScaleToZeroProcessor internal_processors.ScaleToZeroProcessor,
	ossProvReqInjector *provreq.ProvisioningRequestPodsInjector,
	npcCrdLister npc_lister.Lister,
	deleteOptions options.NodeDeleteOptions,
	drainabilityRules rules.Rules,
	autoscalingKubeClients *context.AutoscalingKubeClients,
	matcher networking.Matcher,
	informerFactory informers.SharedInformerFactory,
	gkeReserved *gke.GkeReserved,
	napResourceTrimmer *internalestimator.NapResourceTrimmer,
	systemLabelPatterns []string,
	systemPodsClassifier systempods.Classifier,
	metricsFilter filter.MetricsFilter,
	nodeSizeRecommender ekvms_recommender.NodeSizeRecommender,
	experimentsManager experiments.Manager,
	prClient *provreqclient.ProvisioningRequestClient,
	prCache *provreqcache.QueuedProvisioningCache,
	provreqProcessor pods.PodListProcessor,
	resizableVmAutoprovisioningProvider *ekvms_providers.ResizableVmAutoprovisioningProvider,
	lookaheadBufferStrategyProvider lookaheadbuffer.StrategyProvider,
	reservationBlocksPuller *reservations.BlocksPuller,
	instanceAvailabilityProvider instanceavailability.Provider,
	resourcePolicyPuller placement.ResourcePolicyPuller,
	resizableVmCustomThresholdsProvider ekvms_customthresholds.CustomThresholdsProvider,
	snowflakeWatcher nodesnowflake.Watcher,
	nodeQuotaWatcher nodequota.Watcher,
	eventLogger visibility.EventLogger,
	updatesCh chan npc_status.UpdateMessage,
	minCapacityObserver npc_processors.MinCapacityObserver,
	minQuotasTrackerFactory *resourcequotas.TrackerFactory) (*processors.AutoscalingProcessors, error) {

	allowlistedSystemLabelsMatcher, err := labels.NewMatcher(systemLabelPatterns)
	if err != nil {
		return nil, fmt.Errorf("failed to create system labels matcher: %v", err)
	}

	limitProvider := calculator.NewResizeLimitProvider(provider)
	limitProvider.RegisterConfig(machinetypes.EK.Name(), calculator.LimitConfig{
		MinVmSize:     options.EkvmsMinVmSize,
		IncrementStep: options.EkvmsIncrementStep,
		SafetyBuffer:  options.EkvmsAllocationSafetyBuffer,
	})
	resizeCalculator := calculator.New(vmreservation.New(gkeReserved), provider, options.IsClusterUsingDPV1, limitProvider)
	autoscalingProcessors := processors.DefaultProcessors(options.AutoscalingOptions)

	// Insert the ShortLivedUpgradeNodeInfoProvider right after MixedTemplateNodeInfoProvider, which provides the initial nodeInfos.
	// As such, if needed, the nodeInfos will be replaced right at the beginning and they will go through
	// all the other nodeInfoProvider processors.
	autoscalingProcessors.TemplateNodeInfoProvider =
		oss_nodeinfosprovider.NewCustomAnnotationNodeInfoProvider(
			pr_processors.NewShortLivedUpgradeNodeInfoProvider(
				oss_nodeinfosprovider.NewMixedTemplateNodeInfoProvider(&options.NodeInfoCacheExpireTime, options.ForceDaemonSets)))

	scaleUpProcessorChain := utils_processors.NewScaleUpStatusChainProcessor()

	var capacitybufferClient *cbclient.CapacityBufferClient
	var cbFakePodStateObserver *cbmetrics.FakePodStateObserver
	var capacitybufferClientError error
	if options.CapacitybufferControllerEnabled {
		var fakePodsResolver fakepods.Resolver
		if options.CapacityBufferPodDryRunEnabled {
			fakePodsResolver = fakepods.NewDryRunResolver(kubeClient)
		} else {
			fakePodsResolver = fakepods.NewDefaultingResolver()
		}
		capacitybufferClient, capacitybufferClientError = cbclient.NewCapacityBufferClientFromConfig(kubeConfig)
		if capacitybufferClientError == nil {
			capacitybuffers.InitializeAndRunBufferController(context, capacitybufferClient, fakePodsResolver, npcCrdLister, options.AutopilotEnabled, options.CSNEnabled)
		}
	}

	if options.CapacitybufferPodInjectionEnabled {
		// Add CapacityBuffer types to the default scheme for event recording.
		if err := cbv1beta1.AddToScheme(scheme.Scheme); err != nil {
			klog.Warningf("Failed to add CapacityBuffer (v1beta1) to scheme: %v", err)
		}
		cbFakePodStateObserver = cbmetrics.NewFakePodStateObserver(systemPodsClassifier, internalmetrics.Metrics, capacitybufferPodsRegistry, clock.RealClock{}, true)
		scaleUpProcessorChain.AddProcessor(cbFakePodStateObserver)
		scaleUpProcessorChain.AddProcessor(cbprocessors.NewFakePodsScaleUpStatusProcessor(capacitybufferPodsRegistry))
	}

	// FakePodsScaleUpStatusProcessor processor needs to be the first processor in the chain as it filters out fake pods from
	// Scale Up status so that we don't emit events, visibility logs, ..etc for them.
	var podInjectionBackoffRegistry *podinjectionbackoff.ControllerRegistry
	if options.ProactiveScaleupEnabled {
		podInjectionBackoffRegistry = podinjectionbackoff.NewFakePodControllerRegistry()
		if err := scaleUpProcessorChain.AddProcessor(podinjection.NewFakePodsScaleUpStatusProcessor(podInjectionBackoffRegistry)); err != nil {
			return nil, err
		}
	}

	scaleDownProcessorChain := new(utils.ScaleDownStatusChainProcessor)
	podStatusAggregator := metrics_processors.NewPodStatusAggregator()

	var crPodListProcessor *cr_processors.CapacityRequestPodListProcessor
	eventingScaleUpStatusProcessor := status.NewDefaultScaleUpStatusProcessor()
	optionalInternalEventingScaleUpProcessor := scaleup_processors.NewInternalEventingScaleUpStatusProcessor()
	if options.UseCapacityRequests {
		klog.Infof("Enabling capacity-requests")
		if podListProcessor, scaleUpProcessor, crState, ok := initCapacityRequestProcessors(context, kubeConfig); ok {
			optionalInternalEventingScaleUpProcessor.EnableCapacityReqProcessing(crState)
			if err := scaleUpProcessorChain.AddProcessor(scaleUpProcessor); err != nil {
				return nil, err
			}
			crPodListProcessor = podListProcessor
			eventingScaleUpStatusProcessor = optionalInternalEventingScaleUpProcessor
		}
	}
	if options.ProvisioningRequestEnabled {
		optionalInternalEventingScaleUpProcessor.EnableProvReqProcessing()
		if err := scaleUpProcessorChain.AddProcessor(pr_processors.NewProvisioningRequestScaleUpStatusProcessor()); err != nil {
			return nil, err
		}
		eventingScaleUpStatusProcessor = optionalInternalEventingScaleUpProcessor
	}
	if err := scaleUpProcessorChain.AddProcessor(eventingScaleUpStatusProcessor); err != nil {
		return nil, err
	}

	klog.Infof("Enabling provisioning-requests")
	prPodListProcessor := pr_processors.NewProvisioningRequestPodListProcessor(prClient, prCache, ossProvReqInjector, options.ScanInterval, experimentsManager)

	var (
		edpNodeTaintingProcessor *edps.UpgradeNodeTaintingProcessor
		edpMetrics               *edps.Metrics
	)

	podSharder := podsharding.NewGkePodSharder(provider, options.CSNEnabled, allowlistedSystemLabelsMatcher)
	podShardSelector := podsharding.NewLruPodShardSelector()
	podShardFilter := podsharding.NewPredicatePodShardFilter(npcCrdLister, options.CSNEnabled)
	podShardingProcessor := podsharding.NewPodShardingProcessor(podSharder, podShardSelector, podShardFilter)

	snowflakeBlockedMigsSource := nodesnowflake.NewBlockedMigsSource(provider, snowflakeWatcher)
	snowflakeBlockedMigsSource.Run(context)
	blockedMigsSources := []scaleblocking.BlockedMigsSource{
		bluegreen.NewBlockedMigsSource(provider),
		snowflakeBlockedMigsSource,
	}

	if options.AutopilotEnabled {
		klog.Infof("Enabling Mig Blocking Source for Extended Duration Pods")
		blockedMigsSources = append(blockedMigsSources, edps.NewBlockedMigsSource(provider))
	}

	blockedMigsSources = append(blockedMigsSources, tpu.NewBlockedMigsSource(provider))

	// FlexStart BlockedMigsSource blocks scale ups only when `FlexStartNonQueuedEnabledFlag` experiment is disabled
	// to safely disable broken/incomplete scale up path for Flex Start Non-Queued (FSNQ) node pools.
	blockedMigsSources = append(blockedMigsSources, flexstart.NewBlockedMigsSource(provider, experimentsManager))

	// BulkMig BlockedMigsSource blocks scale ups for FSNQ Bulk Migs if FlexStartNonQueuedBulkMigs is disabled
	blockedMigsSources = append(blockedMigsSources, bulkmig.NewBlockedMigsSource(provider, experimentsManager))

	scaleBlockingProcessor := scaleblocking.NewProcessor(provider, blockedMigsSources)
	scaleDownNodeProcessors := []nodes.ScaleDownNodeProcessor{
		nodes.NewPreFilteringScaleDownNodeProcessor(),
		// scaleBlockingProcessor expects only autoscaled nodes, so it should be kept after PreFilteringScaleDownNodeProcessor.
		scaleBlockingProcessor,
	}
	if prScaleDownNodeProcessor, enabled := pr_processors.NewProvisioningRequestScaleDownNodeProcessor(provider, options.NodeGroupDefaults.ScaleDownUnneededTime); enabled {
		scaleDownNodeProcessors = append(scaleDownNodeProcessors, prScaleDownNodeProcessor)
	}
	scaleDownNodeProcessors = append(scaleDownNodeProcessors, internal_processors.NewSurgeUpgradeScaleDownNodeProcessor(fetcher))

	if options.AutopilotEnabled {
		klog.Info("Enabling extended duration pods scale down processor")
		scaleDownNodeProcessors = append(scaleDownNodeProcessors, edps.NewScaleDownProcessor())
		edpNodeTaintingProcessor = edps.NewUpgradeNodeTaintingProcessor(options.ExtendedDurationPodsUpgradeNodesTaintPerLoop)
		edpMetrics = edps.NewEdpMetrics()
	}
	sdCandidatesSorting := previouscandidates.NewPreviousCandidates()
	autoscalingProcessors.ScaleDownCandidatesNotifier.Register(sdCandidatesSorting)
	scaleDownComparers := []scaledowncandidates.CandidatesComparer{
		emptycandidates.NewEmptySortingProcessor(emptycandidates.NewNodeInfoGetter(clusterSnapshot), deleteOptions, drainabilityRules),
		sdCandidatesSorting,
	}

	npcCrdComparer := npc_processors.NewCrdScaleDownSortingProcessor(npcCrdLister, provider)
	scaleDownComparers = append(scaleDownComparers, npcCrdComparer)

	var ekvmsProcessor *ekvms_processor.ScaleUpNodeProcessor
	var resizableVmManager *ekvms_operationtracker.ManagerImpl
	var resizableMachineTypesProvider internalcfg.Provider[sets.Set[string]]

	if options.EkvmsFixerEnabled {
		resizableVmBackoffManager := ekvms_backoff.NewManager(provider, resizableVmCustomThresholdsProvider, clock.RealClock{})
		nodeStateManager := ekvms_operationtracker.NewNodeStateManager(provider, nodeSizeRecommender, resizableVmBackoffManager, resizeCalculator, clock.RealClock{})
		opTracker := ekvms_operationtracker.New(kubeClient, informerFactory, provider, nodeStateManager, internalmetrics.Metrics, resizeCalculator, options.EkvmsConcurrentResizeWorkers, options.EkvmsFixerEnabled, options.EkvmsFixerInterval)
		resizableVmManager = ekvms_operationtracker.NewManager(provider, opTracker, resizeCalculator, nodeSizeRecommender, internalmetrics.Metrics, nodeStateManager)

		// ResizableVmAutoprovisioningProvider needs to know the NodesCount
		// and ResizableVmAutoprovisioningProvider is defined in buildAutoscaler to be sent to gkeManager
		// So we had to use a callback function to register the NodesCountProvider to ResizableVmAutoprovisioningProvider.
		resizableVmAutoprovisioningProvider.RegisterNodesCountProvider(resizableVmManager)

		// Scale Up and Scale Down processors will be added by default but will be executed only if resize is enabled
		npcCrdBackoff := gke_backoff.GetNpcCrdBackoff(backoff)
		ekvmsProcessor = ekvms_processor.NewScaleUpNodeProcessor(provider, resizableVmManager, resizeCalculator, internalmetrics.Metrics, npcCrdBackoff, npcCrdLister, resizableVmCustomThresholdsProvider)

		downsizeConfigFlags := map[string]string{
			machinetypes.EK.Name():  options.EkDownsizeConfig,
			machinetypes.E4A.Name(): options.E4aDownsizeConfig,
		}
		downsizeExperimentFlags := map[string]string{
			machinetypes.EK.Name():  experiments.EkDownsizeConfigFlag,
			machinetypes.E4A.Name(): experiments.E4aDownsizeConfigFlag,
		}
		configProvider := ekvms_processor.NewDownsizeConfigProvider(provider.MachineConfigProvider(), experimentsManager, downsizeConfigFlags, downsizeExperimentFlags)
		scaleDownNodeProcessors = append(scaleDownNodeProcessors, ekvms_processor.NewScaleDownNodeProcessor(provider.MachineConfigProvider(), resizableVmManager, experimentsManager, fetcher, configProvider, resizeCalculator, internalmetrics.Metrics, clock.RealClock{}))
		scaleDownNodeProcessors = append(scaleDownNodeProcessors, lookaheadbuffer_processor.NewScaleDownNodeProcessor(experimentsManager))

		machineTypeFlags := map[string]string{
			machinetypes.EK.Name():  options.EkMachineTypes,
			machinetypes.E4A.Name(): options.E4aMachineTypes,
		}
		experimentFlags := map[string]string{
			machinetypes.EK.Name():  experiments.EkMachineTypesFlag,
			machinetypes.E4A.Name(): experiments.E4aMachineTypesFlag,
		}
		resizableMachineTypesProvider = ekvms_providers.NewAllResizableMachineTypesProvider(provider.MachineConfigProvider(), experimentsManager, machineTypeFlags, experimentFlags)

		go resizableVmBackoffManager.Run(context)
		go resizableVmManager.Run(context)
		go resizableVmAutoprovisioningProvider.Run(context)
	}

	if options.ScaleDownDelayTypeLocal {
		sdp := scaledowncandidates.NewScaleDownCandidatesDelayProcessor()
		autoscalingProcessors.ScaleStateNotifier.Register(sdp)
		scaleDownNodeProcessors = append(scaleDownNodeProcessors, sdp)
	}

	if len(scaleDownComparers) > 0 {
		// TODO(b/322309995): Use scaledowncandidates.combinedScaleDownCandidatesProcessor.
		sortingProcessor := scaledowncandidates.NewScaleDownCandidatesSortingProcessor(scaleDownComparers)
		scaleDownNodeProcessors = append(scaleDownNodeProcessors, sortingProcessor)
	}

	autoscalingProcessors.ScaleDownNodeProcessor = scaledown.NewGkeInternalAutoscalingScaleDownNodeProcessor(scaleDownNodeProcessors)

	vizAutoscalingStatusProcessor, vizScaleUpStatusProcessor, vizScaleDownStatusProcessor, err := initVisibilityProcessors(*options, provider, eventLogger)
	if err != nil {
		return nil, err
	}
	scaleDownProcessorChain.AddProcessor(vizScaleDownStatusProcessor)
	if err := scaleUpProcessorChain.AddProcessor(vizScaleUpStatusProcessor); err != nil {
		return nil, err
	}

	sharedFairnessManager := fairness.NewSharedEnforcerManager(options.MaxLoopsBeforeAdmission)

	if options.DefragEnabled {
		defragConfig := defrag_processor.Config{
			CandidateLimit:   options.DefragCandidateLimit,
			MaxDelay:         options.DefragMaxDelay,
			ScaleUpTimeout:   options.DefragScaleUpTimeout,
			ScaleDownTimeout: options.DefragScaleDownTimeout,
			ScaleDownDelay:   options.DefragScaleDownDelay,
		}
		pluginsConfig := defrag_plugins_config.New(defrag_plugins_config.Options{
			MaxCandidateNodeCount: options.DefragCandidateNodeLimit,
			NPCLister:             npcCrdLister,
			Provider:              provider,
			Autopilot:             options.AutopilotEnabled,
			ResizableVmManager:    resizableVmManager,
			ExperimentsManager:    experimentsManager,
		})
		plugins, err := defrag_plugins.BuildPlugins(strings.Split(options.DefragPlugins, ","), pluginsConfig)
		if err != nil {
			return nil, err
		}
		*defragProcessor = defrag_processor.NewProcessor(defrag_processor.Options{
			ScaleDownNodeProcessor:   autoscalingProcessors.ScaleDownNodeProcessor,
			ScaleDownStatusProcessor: scaleDownProcessorChain,
			DeleteOptions:            deleteOptions,
			DrainabilityRules:        drainabilityRules,
			Config:                   defragConfig,
			Plugins:                  plugins,
			ExperimentsManager:       experimentsManager,
			FairnessEnforcer:         sharedFairnessManager.CreateEnforcer(defrag_processor.DefragProcessorName),
			MinQuotasTrackerFactory:  minQuotasTrackerFactory,
		})
	}
	var quotaProcessor *nodequota_processor.NodeQuotaProcessor
	if nodeQuotaWatcher != nil {
		quotaProcessor = nodequota_processor.NewNodeQuotaProcessor(nodeQuotaWatcher)
		// HACK: node quota is updated after the loop; set the value explicitly for the first loop
		// note that this is actually most likely going to be 0 (no quota) after master is (re)created
		// as it may take a while for quota file to be updated.
		options.MaxNodesTotal = nodeQuotaWatcher.GetNodeQuota()
		go nodeQuotaWatcher.Run(context)
	}

	var cbPodInjectionProcessor *cbprocessors.CapacityBufferPodListProcessor
	if options.CapacitybufferPodInjectionEnabled {
		if capacitybufferClient == nil {
			capacitybufferClient, capacitybufferClientError = cbclient.NewCapacityBufferClientFromConfig(kubeConfig)
		}
		if capacitybufferClientError == nil && capacitybufferClient != nil {
			cbPodInjectionProcessor = cbprocessors.NewCapacityBufferPodListProcessor(capacitybufferClient, []string{capacitybuffer.ActiveProvisioningStrategy}, capacitybufferPodsRegistry, true)
		}
	}

	var podInjectionProcessor *podinjection.PodInjectionPodListProcessor
	var enforceFakePodsLimitProcessor *podinjection.EnforceInjectedPodsLimitProcessor
	if options.ProactiveScaleupEnabled {
		podInjectionProcessor = podinjection.NewPodInjectionPodListProcessor(podInjectionBackoffRegistry)
		enforceFakePodsLimitProcessor = podinjection.NewEnforceInjectedPodsLimitProcessor(options.PodInjectionLimit)
	}
	laPodProvider := lookaheadbuffer.NewPodProvider(lookaheadBufferStrategyProvider)
	lookaheadPodsInjectionProcessor := lookaheadbuffer_processor.NewLookaheadPodInjectionProcessor(laPodProvider, lookaheadBufferStrategyProvider, lookaheadbuffer_processor.NewWorkloadSeparationLimiter(experimentsManager, options.EkLookaheadMaxWorkloadSeparations, caVersion), systemPodsClassifier, npcCrdLister, resizeCalculator, internalmetrics.Metrics)

	psObserver, err := podstate.NewPodStateObserver(informerFactory, internalmetrics.Metrics, systemPodsClassifier, npcCrdLister, options.PendingPodsMetricEnabled, options.MetricsPerCccEnabled && options.PendingPodsPerCccMetricEnabled, options.ExpendablePodsPriorityCutoff)
	if options.MetricsPerCccEnabled && options.ScaleUpPerCccMetricsEnabled {
		autoscalingProcessors.ScaleStateNotifier.Register(metricsccc.NewNodeGroupChangePerCCCMetricsProducer(npcCrdLister))
	}
	if err != nil {
		return nil, err
	}
	go psObserver.Run(context)

	ptsDomainDiscoveries := []podtopologyspread.PTSDomainDiscovery{
		podtopologyspread.NewCCCDomainDiscovery(experimentsManager, npcCrdLister),
		podtopologyspread.NewZonalDomainDiscovery(experimentsManager, provider),
		// Node Based DD should be the last processor as it is supposed to work only for the PTS domains for which we do not have a dedicated processor.
		podtopologyspread.NewNodeBasedDomainDiscovery(experimentsManager, clusterSnapshot, provider),
	}
	podTopologySpreadProcessor := podtopologyspread.NewPodTopologySpreadProcessor(ptsDomainDiscoveries)

	var csnPodsInjectionProcessor *cbprocessors.CapacityBufferPodListProcessor
	var csnNodeReconcilationProcessor *csn_processors.NodeReconcilationProcessor
	var csnBufferConsumptionProcessor *csn_processors.BufferConsumptionProcessor
	var csnCSNPodsLifecycleProcessor *csn_processors.CSNPodsLifecycleProcessor
	if capacitybufferClient != nil && options.CSNEnabled {
		csnPodsInjectionProcessor = cbprocessors.NewCapacityBufferPodListProcessor(capacitybufferClient, []string{capacitybuffers.ColdProvisioningStrategy}, capacitybufferPodsRegistry, true)
		csnNodeController := nodecontroller.NewCSNNodeController(informerFactory, kubeClient, provider, experimentsManager)
		go csnNodeController.Run(context)
		csnNodeReconcilationProcessor = csn_processors.NewNodeReconciliationProcessor(csnNodeController, provider, experimentsManager)
		csnBufferConsumptionProcessor = csn_processors.NewBufferConsumptionProcessor(csnNodeController, experimentsManager)
		csnCSNPodsLifecycleProcessor = csn_processors.NewCSNPodsLifecycleProcessor(csnNodeController, csnPodsInjectionProcessor, cbFakePodStateObserver, capacitybufferPodsRegistry, options.CSNDefaultRefreshFrequency)
	}

	capacityBufferMetricsProcessor := initCapacityBufferMetricsProcessor(experimentsManager, capacitybufferClient, capacitybufferPodsRegistry, options.CapacitybufferPodInjectionEnabled)

	var flexAdvisorPodListProcessor *flexadvisor.PodListProcessor
	if options.GCEFlexAdvisorEnabled {
		flexAdvisorPodListProcessor = flexadvisor.NewPodListProcessor(instanceAvailabilityProvider, npcCrdLister, experimentsManager)
	}

	var cccMinCapacityProcessor *npc_processors.MinCapacityPodListProcessor
	if options.EnableComputeClassMinCapacity {
		cccMinCapacityProcessor = npc_processors.NewMinCapacityPodListProcessor(npcCrdLister, sharedFairnessManager.CreateEnforcer(npc_processors.MinCapacityPodListProcessorName), experimentsManager)
	}

	autoscalingProcessors.PodListProcessor =
		internal_processors.NewGkeInternalPodListProcessor(crPodListProcessor, prPodListProcessor, ekvmsProcessor, podStatusAggregator, podShardingProcessor, clusterScaleToZeroProcessor, *defragProcessor, podInjectionProcessor, provreqProcessor, enforceFakePodsLimitProcessor, lookaheadPodsInjectionProcessor, psObserver, podTopologySpreadProcessor, flexAdvisorPodListProcessor, cbPodInjectionProcessor, csnNodeReconcilationProcessor, csnBufferConsumptionProcessor, csnCSNPodsLifecycleProcessor, capacityBufferMetricsProcessor, cbFakePodStateObserver, cccMinCapacityProcessor, experimentsManager)

	metricsFilterProcessor := metrics_processors.NewMetricsFilterScaleUpProcessor(metricsFilter)

	autoscalingProcessors.ActionableClusterProcessor = internal_processors.NewGkeEmptyClusterProcessor()

	var crdStatusHistoryProcessor *history.AutoscalingStatusHistoryProcessor
	var crdResourcesReportingProcessor *npc_status.CrdResourceReportingProcessor
	if options.EnhancedCrdStatusReporting && updatesCh != nil {
		sharedScaleUpData := history.NewScaleUpData()
		crdHistoryProcessor := history.NewScaleUpStatusHistoryProcessor(npcCrdLister, provider, sharedScaleUpData, updatesCh, minCapacityObserver)
		crdResourcesReportingProcessor = npc_status.NewCrdResourceReportingProcessor(npcCrdLister, updatesCh, computeclass.NewMatcher(npcCrdLister, provider))
		if err := scaleUpProcessorChain.AddProcessor(crdHistoryProcessor); err != nil {
			return nil, err
		}
		crdStatusHistoryProcessor = history.NewAutoscalingStatusHistoryProcessor(sharedScaleUpData, updatesCh, minCapacityObserver)

		crdScaleDownHistoryProcessor := history.NewScaleDownStatusHistoryProcessor(npcCrdLister, provider, updatesCh)
		scaleDownProcessorChain.AddProcessor(crdScaleDownHistoryProcessor)
	}

	autoscalingProcessors.AutoscalingStatusProcessor = internal_processors.NewGkeInternalAutoscalingStatusProcessor(quotaProcessor, vizAutoscalingStatusProcessor, metricsFilterProcessor, edpNodeTaintingProcessor, edpMetrics, crdResourcesReportingProcessor, crdStatusHistoryProcessor)

	apNodeGroupListProcessor, apNodeGroupManager := initAutoprovisioningProcessors(optionsTracker, *options, provider, backoff, scaleBlockingProcessor, reservationsPuller, npcCrdLister, matcher, allowlistedSystemLabelsMatcher, experimentsManager, autoscalingKubeClients.ListerRegistry, resizableMachineTypesProvider, reservationBlocksPuller, resourcePolicyPuller)
	autoscalingProcessors.NodeGroupListProcessor = apNodeGroupListProcessor

	// autoprovisioning.NewSortedNodeGroupListProcessor sorts node groups descending by allocatable CPU.
	autoscalingProcessors.NodeGroupListProcessor = autoprovisioning.NewSortedNodeGroupListProcessor(autoscalingProcessors.NodeGroupListProcessor)
	autoscalingProcessors.NodeGroupManager = apNodeGroupManager

	// Slice of Hardware processor to filter only the smallest size node groups supporting slice of hardware.
	// This ensures that only the smallest machine-shape which can schedule pods will be scaled up.
	if options.AutopilotEnabled {
		processor := sohw_processors.NewNodeGroupListProcessor(autoscalingProcessors.NodeGroupListProcessor)
		autoscalingProcessors.NodeGroupListProcessor = processor
	}

	// Prepares EK nodegroups for binpacking by injecting balloon pods into node infos.
	if options.EkvmsFixerEnabled {
		processor := ekvms_processor.NewNodeGroupListProcessor(autoscalingProcessors.NodeGroupListProcessor, resizeCalculator, resizableMachineTypesProvider, resizableVmAutoprovisioningProvider, provider.MachineConfigProvider())
		autoscalingProcessors.NodeGroupListProcessor = processor
	}

	if !options.AutopilotEnabled {
		processor := csn_processors.NewNodeGroupListProcessor(autoscalingProcessors.NodeGroupListProcessor, experimentsManager)
		autoscalingProcessors.NodeGroupListProcessor = processor
	}

	// TODO(b/322314237): !!! HACK !!! TO BE REMOVED IN NEAR FUTURE !!!
	processor := des_processors.NewNodeGroupListProcessor(autoscalingProcessors.NodeGroupListProcessor, napResourceTrimmer)
	autoscalingProcessors.NodeGroupListProcessor = processor

	pr_processor := pr_processors.NewFilterQueuedNodeGroupListProcessor(autoscalingProcessors.NodeGroupListProcessor)
	autoscalingProcessors.NodeGroupListProcessor = pr_processor

	// This processor filters node groups by shard homogeneity before sorting/bucketing.
	shardProcessor := npc_processors.NewShardAwareNodeGroupListProcessor(autoscalingProcessors.NodeGroupListProcessor, npcCrdLister)
	autoscalingProcessors.NodeGroupListProcessor = shardProcessor

	// This processor sorts node groups, a critical step prior to scale-up. To maintain
	// processing order, it's initialized as the final NodeGroupListProcessor.
	npcProcessor := npc_processors.NewNodeGroupListProcessor(npcCrdLister, autoscalingProcessors.NodeGroupListProcessor, internalmetrics.Metrics, provider)
	autoscalingProcessors.NodeGroupListProcessor = npcProcessor
	autoscalingProcessors.BinpackingLimiter = binpacking.NewCombinedLimiter([]binpacking.BinpackingLimiter{autoscalingProcessors.BinpackingLimiter, npcProcessor})
	npcCrdScaleUpStatusProcessor := npc_processors.NewCrdScaleUpStatusProcessor(npcCrdLister, provider, internalmetrics.Metrics)
	if err := scaleUpProcessorChain.AddProcessor(npcCrdScaleUpStatusProcessor); err != nil {
		return nil, err
	}
	npcCrdScaleDownStatusProcessor := npc_processors.NewCrdScaleDownStatusProcessor(npcCrdLister, provider, internalmetrics.Metrics)
	scaleDownProcessorChain.AddProcessor(npcCrdScaleDownStatusProcessor)

	if options.AsyncNodeGroupsEnabled {
		autoscalingProcessors.AsyncNodeGroupStateChecker = asyncnodegroups.NewAsyncNapNodeGroupStateChecker()
	}

	autoscalingProcessors.BinpackingLimiter = metricsbinpacking.NewBinpackingMetricsProcessor(autoscalingProcessors.BinpackingLimiter)
	autoscalingProcessors.NodeGroupManager = apNodeGroupManager
	autoscalingProcessors.ScaleDownSetProcessor =
		nodes.NewCompositeScaleDownSetProcessor(
			[]nodes.ScaleDownSetProcessor{
				internal_processors.NewMinSizeProcessor(provider),
				nodes.NewAtomicResizeFilteringProcessor(),
			},
		)

	var nodeGroupSetProcessor nodegroupset.NodeGroupSetProcessor
	nodeGroupSetProcessor = &nodegroupset.BalancingNodeGroupSetProcessor{Comparator: internal_processors.IsGkeNodeInfoSimilar}
	nodeGroupSetProcessor = internal_processors.NewNodePoolAwareNodeGroupSetProcessor(nodeGroupSetProcessor)
	locationPolicyBalancers := make(map[gke.LocationPolicyEnum]locationpolicy.Balancer)
	locationPolicyBalancers[gke.LocationPolicyAny] = locationpolicy.NewLocationPolicyAnyBalancer(provider, experimentsManager)
	if options.GCEFlexAdvisorEnabled {
		nodeGroupSetProcessor = flexadvisor.NewScaleUpBalancer(nodeGroupSetProcessor, instanceAvailabilityProvider, npcCrdLister, experimentsManager, true)
		nodeGroupSetProcessor = locationpolicy.NewProcessor(nodeGroupSetProcessor, provider, locationPolicyBalancers, experimentsManager, true, instanceAvailabilityProvider, npcCrdLister)
		nodeGroupSetProcessor = flexadvisor.NewScaleUpBalancer(nodeGroupSetProcessor, instanceAvailabilityProvider, npcCrdLister, experimentsManager, false)
	} else {
		nodeGroupSetProcessor = locationpolicy.NewProcessor(nodeGroupSetProcessor, provider, locationPolicyBalancers, experimentsManager, false, nil, nil)
	}
	nodeGroupSetProcessor = reservations.NewReservationBalancingProcessor(nodeGroupSetProcessor, reservationsPuller, options.GCEOptions.LocalSSDDiskSizeProvider, provider)
	autoscalingProcessors.NodeGroupSetProcessor = internal_processors.NewTotalMaxSizeProcessor(nodeGroupSetProcessor)

	var mutationInjector *daemonsetmutation.Injector
	if options.DaemonSetMutationEnabled {
		dryRunResolver := fakepods.NewDryRunResolver(kubeClient)
		mutationCache := daemonsetmutation.NewMutationCache()
		mutationController := daemonsetmutation.NewController(context, mutationCache, dryRunResolver, informerFactory)
		mutationInjector = daemonsetmutation.NewInjector(mutationCache, mutationController)
		mutationController.Start()
	}

	autoscalingProcessors.TemplateNodeInfoProvider = nodeinfosprovider.NewAugmentingNodeInfoProvider(
		autoscalingProcessors.TemplateNodeInfoProvider, options.NodePoolUpdatesEnabled, mutationInjector,
	)

	if options.EnableComputeClassMinCapacity {
		autoscalingProcessors.TemplateNodeInfoProvider = npc_nodeinfosproviders.NewPriorityIdxNodeInfoProvider(
			autoscalingProcessors.TemplateNodeInfoProvider, computeclass.NewMatcher(npcCrdLister, provider), npcCrdLister, experimentsManager,
		)
	}

	autoscalingProcessors.TemplateNodeInfoProvider = csn_processors.NewNodeInfoProvider(
		autoscalingProcessors.TemplateNodeInfoProvider,
		provider,
		experimentsManager,
	)

	podAnnotator := annotator.NewPodAnnotator(kubeClient, podStatusAggregator)
	if err := scaleUpProcessorChain.AddProcessor(metrics_processors.NewScaleUpStatusMetricsProcessor(podStatusAggregator, metricsFilter, npcCrdLister)); err != nil {
		return nil, err
	}
	if err := scaleUpProcessorChain.AddProcessor(podAnnotator); err != nil {
		return nil, err
	}
	if err := scaleUpProcessorChain.AddProcessor(psObserver); err != nil {
		return nil, err
	}
	autoscalingProcessors.ScaleUpStatusProcessor = scaleUpProcessorChain
	autoscalingProcessors.ScaleDownStatusProcessor = scaleDownProcessorChain
	podAnnotator.Start(context, 1)

	nodeAnnotator := nodeannotator.NewNodeAnnotator(kubeClient, autoscalingKubeClients.AllNodeLister(), nodeannotator.Config{})
	cccNodeAnnotationPlugin := computeclass.NewCCCNodeAnnotatorPlugin(npcCrdLister, provider)
	nodeAnnotator.RegisterPlugin(cccNodeAnnotationPlugin)
	if err := nodeAnnotator.Start(context); err != nil {
		return nil, err
	}

	autoscalingProcessors.CustomResourcesProcessor = customResourcesProcessor
	return autoscalingProcessors, nil
}
