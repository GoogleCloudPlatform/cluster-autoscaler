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

	"golang.org/x/oauth2"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/autoscaler/cluster-autoscaler/builder"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/fakepods"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate/scaleupfailures"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core"
	coreoptions "k8s.io/autoscaler/cluster-autoscaler/core/options"
	"k8s.io/autoscaler/cluster-autoscaler/core/utils"
	ca_utils "k8s.io/autoscaler/cluster-autoscaler/core/utils"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/loop"
	"k8s.io/autoscaler/cluster-autoscaler/observers/loopstart"
	"k8s.io/autoscaler/cluster-autoscaler/processors/pods"
	"k8s.io/autoscaler/cluster-autoscaler/processors/provreq"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/besteffortatomic"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/checkcapacity"
	provreqorchestrator "k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/orchestrator"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/resourcequotas"
	"k8s.io/autoscaler/cluster-autoscaler/resourcequotas/capacityquota"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/predicate"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/store"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/client-go/informers"
	kube_client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/selfservice"
	gke_backoff "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/backoff"
	internalcq "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityquota"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/consumablereservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizablevms"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	npc_client "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/client"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"

	cc_resourcequota "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/resourcequota"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	npc_history "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/history"
	npc_validator "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/validator"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	defrag_processor "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/processor"
	ekvms_customthresholds "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/backoff/customthresholds"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/nodesizerecommender"
	ekvms_providers "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/providers"
	internalestimator "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/estimator"
	internalexpander "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/gkeprice"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/provider"
	edps "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/extendeddurationpods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor"
	flexadvisorapi "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/futurereservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/gkedebuggingsnapshot"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	kubernetes_util "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/kubernetes"
	informerutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/kubernetes/informers"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/filter"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/multitenancy"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/nodequota"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/nodesnowflake"
	internal_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors"
	internal_customresources "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/customresources"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"
	internal_estimator "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/estimator"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/initialization"
	prmanager "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/manager"
	scaleup_pr "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/scaleup"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/resizerequests"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/informers/externalversions"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/listers/nodemanagement.gke.io/v1alpha1"
	gke_utils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"

	ccc_api "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	informer "github.com/googlecloudplatform/compute-class-api/client/informers/externalversions"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	cc_controller "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/controller"
	cc_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/processors"
	cc_history "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/history"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Builder is responsible for constructing the StaticAutoscaler and its dependencies.
type Builder struct {
	optsTracker *optstracking.OptionsTracker
	caVersion   *version.Version
	tokenSource oauth2.TokenSource
	projectID   string
	location    string

	manager ctrl.Manager

	// Fields to hold injectable fakes (or real clients)
	kubeClient     kube_client.Interface
	kubeConfig     *rest.Config
	kubeConfigJSON *rest.Config

	nodeTemplateCache *nodetemplate.Cache
	gceCache          *gce.GceCache
	gkeCache          *gke.GkeCache

	listerRegistry     kube_util.ListerRegistry
	updateInfoLister   v1alpha1.UpdateInfoLister
	updateInfoFactory  externalversions.SharedInformerFactory
	npcCrdClient       npc_client.Client
	npcCrdLister       npc_lister.Lister
	provConfigInformer ProviderConfigManager

	httpClient                   *http.Client
	gceClient                    gceclient.AutoscalingInternalGceClient
	gkeClient                    gkeclient.AutoscalingGkeClient
	resizableVmClient            resizablevms.Client
	consumableReservationsClient consumablereservations.Client
	recommendLocationsClient     gceclient.RecommendLocationsClient
	atomicResizeRequestClient    resizerequestclient.ResizeRequestClient
	flexResizeRequestClient      resizerequestclient.ResizeRequestClient
	flexAdvisorClient            flexadvisorapi.AdviceProvider
	machineConfigProvider        *machinetypes.MachineConfigProvider

	prClient   *provreqclient.ProvisioningRequestClient
	prInjector *provreq.ProvisioningRequestPodsInjector
	prManager  prmanager.ProvisioningRequestManager
	prCache    *provreqcache.QueuedProvisioningCache

	informerFactory informers.SharedInformerFactory

	podObserver                *loop.UnschedulablePodObserver
	snowflakeWatcher           nodesnowflake.Watcher
	nodeQuotaWatcher           nodequota.Watcher
	nodeSizeRecommenderFactory nodesizerecommender.RecommenderFactory

	eventLogger visibility.EventLogger
	ctrClient   client.Client
}

// This helper struct is a way to deal with mutual dependency between
// gkeCloudProviderImpl constructor (which takes ClusterLocationsObserver)
// and NodeSizeRecommender constructor (which takes cloud provider).
// NodeSizeRecommender is the actual implementation of ClusterLocationsObserver,
// so we will pass delegatingClusterLocationsObserver to gkeCloudProviderImpl
// constructor and then set proper NodeSizeRecommender instance as delegatee in
// delegatingClusterLocationsObserver.
type delegatingClusterLocationsObserver struct {
	gke.ClusterLocationsObserver
}

func (dclo *delegatingClusterLocationsObserver) SetLocations(locations []string) {
	if dclo.ClusterLocationsObserver == nil {
		return
	}
	dclo.ClusterLocationsObserver.SetLocations(locations)
}

// FutureReservationsPuller asynchronously pulls info about Future Reservations from GCE and provides
// them through GetLocalFutureReservations() method.
type FutureReservationsPuller interface {
	// GetLocalFutureReservations returns a slice of future reservations in this cluster's GCP project
	GetLocalFutureReservations() []*gceclient.GceFutureReservation
	// Run starts asynchrounous Future Reservations pulls from GCE
	Run(bgContext ctx.Context)
}

type ProviderConfigManager interface {
	multitenancy.ProviderConfigObserver
	// Run starts asynchrounous ProviderConfigObserver.
	Run(bgContext ctx.Context)
}

func NewBuilder(optionsTracker *optstracking.OptionsTracker) *Builder {
	return &Builder{
		optsTracker: optionsTracker,
	}
}

// GetOptionsTracker returns the configured projectID.
func (b *Builder) GetOptionsTracker() *optstracking.OptionsTracker {
	return b.optsTracker
}

// WithCAVersion allows injecting a specific CA component version.
func (b *Builder) WithCAVersion(v version.Version) *Builder {
	b.caVersion = &v
	return b
}

// WithKubeConfig allows injecting a Kubernetes config.
func (b *Builder) WithKubeConfig(config *rest.Config) *Builder {
	b.kubeConfig = config
	return b
}

// WithKubeJSON allows injecting a Kubernetes json config.
func (b *Builder) WithKubeJSON(config *rest.Config) *Builder {
	b.kubeConfigJSON = config
	return b
}

// WithKubeClient allows injecting a Kubernetes client.
func (b *Builder) WithKubeClient(client kube_client.Interface) *Builder {
	b.kubeClient = client
	return b
}

// WithManager allows injecting a controller-runtime manager.
func (b *Builder) WithManager(mgr ctrl.Manager) *Builder {
	b.manager = mgr
	return b
}

// WithNodeTemplateCache allows injecting a Kubernetes client.
func (b *Builder) WithNodeTemplateCache(cache *nodetemplate.Cache) *Builder {
	b.nodeTemplateCache = cache
	return b
}

// WithListerRegistry allows injecting a fake ListerRegistry.
func (b *Builder) WithListerRegistry(registry kube_util.ListerRegistry) *Builder {
	b.listerRegistry = registry
	return b
}

// WithUpdateInfoLister allows injecting a UpdateInfoLister and optionally a factory. In big unit test factory is nil.
func (b *Builder) WithUpdateInfoLister(lister v1alpha1.UpdateInfoLister, sharedInformerFactory externalversions.SharedInformerFactory) *Builder {
	b.updateInfoLister = lister
	b.updateInfoFactory = sharedInformerFactory
	return b
}

// WithNpcCrdClient allows injecting a npc crd client.
func (b *Builder) WithNpcCrdClient(client npc_client.Client) *Builder {
	b.npcCrdClient = client
	return b
}

// WithNpcCrdLister allows injecting a npc crd lister.
func (b *Builder) WithNpcCrdLister(lister npc_lister.Lister) *Builder {
	b.npcCrdLister = lister
	return b
}

// WithTokenSource allows injecting a provided token.
func (b *Builder) WithTokenSource(token oauth2.TokenSource) *Builder {
	b.tokenSource = token
	return b
}

// GetTokenSource returns the configured TokenSource.
func (b *Builder) GetTokenSource() oauth2.TokenSource {
	return b.tokenSource
}

// WithLocation allows injecting a location.
func (b *Builder) WithLocation(location string) *Builder {
	b.location = location
	return b
}

// GetLocation returns the configured location.
func (b *Builder) GetLocation() string {
	return b.location
}

// WithProjectID allows injecting a projectID.
func (b *Builder) WithProjectID(projectID string) *Builder {
	b.projectID = projectID
	return b
}

// GetProjectID returns the configured projectID.
func (b *Builder) GetProjectID() string {
	return b.projectID
}

// WithHttpClient allows injecting http client.
func (b *Builder) WithHttpClient(httpClient *http.Client) *Builder {
	b.httpClient = httpClient
	return b
}

// GetHttpClient returns the configured HttpClient.
func (b *Builder) GetHttpClient() *http.Client {
	return b.httpClient
}

// WithGCEClient allows injecting a GCE client.
func (b *Builder) WithGCEClient(client gceclient.AutoscalingInternalGceClient) *Builder {
	b.gceClient = client
	return b
}

// WithResizableVmClient allows injecting a ResizableVmClient.
func (b *Builder) WithResizableVmClient(client resizablevms.Client) *Builder {
	b.resizableVmClient = client
	return b
}

// WithConsumableReservationsClient allows injecting a Consumable Reservations client.
func (b *Builder) WithConsumableReservationsClient(client consumablereservations.Client) *Builder {
	b.consumableReservationsClient = client
	return b
}

// WithGCECache allows injecting a GCE cache.
func (b *Builder) WithGCECache(cache *gce.GceCache) *Builder {
	b.gceCache = cache
	return b
}

// WithGKECache allows injecting a GKE cache.
func (b *Builder) WithGKECache(cache *gke.GkeCache) *Builder {
	b.gkeCache = cache
	return b
}

// WithProvReqClient allows injecting a prov req client.
func (b *Builder) WithProvReqClient(client *provreqclient.ProvisioningRequestClient) *Builder {
	b.prClient = client
	return b
}

// WithProvReqInjector allows injecting a prov req pods injector.
func (b *Builder) WithProvReqInjector(injector *provreq.ProvisioningRequestPodsInjector) *Builder {
	b.prInjector = injector
	return b
}

// WithProvReqManager allows injecting a prov req manager.
func (b *Builder) WithProvReqManager(mgr prmanager.ProvisioningRequestManager) *Builder {
	b.prManager = mgr
	return b
}

// WithProvReqCache allows injecting a prov req cache.
func (b *Builder) WithProvReqCache(cache *provreqcache.QueuedProvisioningCache) *Builder {
	b.prCache = cache
	return b
}

// WithGkeClient allows injecting a GkeClient.
func (b *Builder) WithGkeClient(client gkeclient.AutoscalingGkeClient) *Builder {
	b.gkeClient = client
	return b
}

// WithRecommendLocationsClient allows injecting a RecommendLocationsClient.
func (b *Builder) WithRecommendLocationsClient(client gceclient.RecommendLocationsClient) *Builder {
	b.recommendLocationsClient = client
	return b
}

// WithAtomicResizeRequestClient allows injecting a AtomicResizeRequestClient.
func (b *Builder) WithAtomicResizeRequestClient(client resizerequestclient.ResizeRequestClient) *Builder {
	b.atomicResizeRequestClient = client
	return b
}

// WithFlexResizeRequestClient allows injecting a FlexResizeRequestClient.
func (b *Builder) WithFlexResizeRequestClient(client resizerequestclient.ResizeRequestClient) *Builder {
	b.flexResizeRequestClient = client
	return b
}

// WithFlexAdvisorClient allows injecting a FlexAdvisorClient.
func (b *Builder) WithFlexAdvisorClient(client flexadvisorapi.AdviceProvider) *Builder {
	b.flexAdvisorClient = client
	return b
}

// WithMachineConfigProvider allows injecting a machine config provider.
func (b *Builder) WithMachineConfigProvider(machineConfigProvider *machinetypes.MachineConfigProvider) *Builder {
	b.machineConfigProvider = machineConfigProvider
	return b
}

// GetMachineConfigProvider returns configured machine config provider.
func (b *Builder) GetMachineConfigProvider() *machinetypes.MachineConfigProvider {
	return b.machineConfigProvider
}

// WithPodObserver allows injecting a fake UnschedulablePodObserver.
func (b *Builder) WithPodObserver(observer *loop.UnschedulablePodObserver) *Builder {
	b.podObserver = observer
	return b
}

// WithSnowflakeWatcher allows injecting a specific SnowflakeWatcher.
func (b *Builder) WithSnowflakeWatcher(watcher nodesnowflake.Watcher) *Builder {
	b.snowflakeWatcher = watcher
	return b
}

// WithNodeQuotaWatcher allows injecting a specific NodeQuotaWatcher.
func (b *Builder) WithNodeQuotaWatcher(watcher nodequota.Watcher) *Builder {
	b.nodeQuotaWatcher = watcher
	return b
}

// WithProviderConfigInformer allows injecting a provider config manager.
func (b *Builder) WithProviderConfigInformer(informer ProviderConfigManager) *Builder {
	b.provConfigInformer = informer
	return b
}

// GetProviderConfigInformer returns the configured provider config manager.
func (b *Builder) GetProviderConfigInformer() ProviderConfigManager {
	return b.provConfigInformer
}

// WithInformerFactory allows injecting a main informer factory.
func (b *Builder) WithInformerFactory(sharedInformerFactory informers.SharedInformerFactory) *Builder {
	b.informerFactory = sharedInformerFactory
	return b
}

// WithEventLogger allows injecting an event logger.
func (b *Builder) WithEventLogger(eventLogger visibility.EventLogger) *Builder {
	b.eventLogger = eventLogger
	return b
}

// WithControllerRuntimeClient allows injecting a Controller Runtime client.
func (b *Builder) WithControllerRuntimeClient(ctrClient client.Client) *Builder {
	b.ctrClient = ctrClient
	return b
}

func (b *Builder) WithNodeSizeRecommenderFactory(nodeSizeRecommenderFactory nodesizerecommender.RecommenderFactory) *Builder {
	b.nodeSizeRecommenderFactory = nodeSizeRecommenderFactory
	return b
}

func (b *Builder) Build(
	bgContext ctx.Context,
	gkeDebuggingSnapshotter *gkedebuggingsnapshot.GkeDebuggingSnapshotter,
	osReservedContent []byte) (*core.StaticAutoscaler, *loop.LoopTrigger, error) {

	experimentsManager := b.optsTracker.ExperimentsManager()
	if b.caVersion == nil {
		return nil, nil, fmt.Errorf("caVersion is missing: ensure WithCAVersion() is called")
	}
	caVersion := *b.caVersion
	autoscalingOptions := b.optsTracker.Options()

	// Validate that all required options have been set on the builder.
	if err := b.validateOptions(); err != nil {
		return nil, nil, err
	}

	informerFactory := b.informerFactory

	customResourcesProcessor := internal_customresources.NewProcessor(b.nodeTemplateCache)

	var surgeUpgradeResourceTracker *gke.SurgeUpgradeResourceTracker
	var updateInfoFetcher kubernetes_util.UpdateInfoFetcher
	// Use internal expander strategy if requested
	autoscalingKubeClients := context.NewAutoscalingKubeClients(bgContext, autoscalingOptions.AutoscalingOptions, b.kubeClient, informerFactory)

	// Use lister registry if provided.
	if b.listerRegistry != nil {
		autoscalingKubeClients.ListerRegistry = b.listerRegistry
	}

	if b.updateInfoFactory != nil {
		b.updateInfoFactory.Start(bgContext.Done())
		informersSynced := b.updateInfoFactory.WaitForCacheSync(bgContext.Done())
		for _, synced := range informersSynced {
			if !synced {
				klog.Fatal("can't create updateInfo lister")
			}
		}
		klog.V(2).Info("Successful initial UpdateInfo sync")
	}

	updateInfoFetcher = kubernetes_util.NewUpdateInfoFetcher(b.updateInfoLister, clock.RealClock{})
	surgeUpgradeResourceTracker = gke.NewSurgeUpgradeResourceTracker(customResourcesProcessor, autoscalingKubeClients.AllNodeLister(), updateInfoFetcher)

	gkeDebuggingSnapshotter.SetUpdateInfoFetcher(updateInfoFetcher)

	deleteOptions := options.NewNodeDeleteOptions(autoscalingOptions.AutoscalingOptions)
	drainabilityRules := rules.Default(deleteOptions)

	// We are initializing clusterScaleToZeroProcessor here to have all drainability rules in one place, as order matters and there are multiple references to it down the lines.
	var clusterScaleToZeroProcessor internal_processors.ScaleToZeroProcessor
	systemPodsClassifier := systempods.NewClassifier(autoscalingOptions.SystemNamespaces)
	metricsFilter := filter.NewMetricsFilter()
	if autoscalingOptions.MultitenancyEnabled {
		systemPodsClassifier = systempods.NewMultitenantClassifier(autoscalingOptions.SystemNamespaces)
		metricsFilter = filter.NewMultitenantMetricsFilter(experimentsManager)
	}
	if autoscalingOptions.ClusterScaleToZeroEnabled {
		klog.Infof("Enabling cluster scale-to-0 processor. Delay: %v. Ignored system namespaces: %v.", autoscalingOptions.ClusterScaleToZeroDelay, autoscalingOptions.SystemNamespaces)
		if autoscalingOptions.MultitenancyEnabled {
			clusterScaleToZeroProcessor = internal_processors.NewMultitenantScaleToZeroPodListProcessor(metricsFilter, autoscalingOptions.ClusterScaleToZeroDelay, systemPodsClassifier, experimentsManager, autoscalingOptions.ClusterHash)
		} else {
			clusterScaleToZeroProcessor = internal_processors.NewScaleToZeroPodListProcessor(metricsFilter, autoscalingOptions.ClusterScaleToZeroDelay, systemPodsClassifier)
		}
		drainabilityRules = append([]rules.Rule{clusterScaleToZeroProcessor}, drainabilityRules...)
	}

	scaleUpFailuresRegistry := scaleupfailures.NewRegistry()
	var loopStartObservers = []loopstart.Observer{scaleUpFailuresRegistry}

	rrer := resizerequests.NewErrorReporter(experimentsManager)
	loopStartObservers = append(loopStartObservers, rrer)
	var matcher networking.Matcher
	if autoscalingOptions.MultiNetworkSupportEnabled {
		clientset, err := networking.NewClientset(b.kubeConfigJSON)
		if err != nil {
			return nil, nil, err
		}
		lister, err := networking.NewLister(bgContext, clientset)
		if err != nil {
			return nil, nil, err
		}
		matcher = networking.GetMatcher(lister)
	}

	gkeReserved, err := gke.NewGkeReserved(osReservedContent)
	if err != nil {
		return nil, nil, err
	}

	// merge two flags - explicit label is also a valid regex pattern
	flagSystemLabels := strings.Split(autoscalingOptions.AllowlistedSystemLabels, ",")
	flagSystemLabelPatterns := append(strings.Split(autoscalingOptions.AllowlistedSystemLabelPatterns, ","), flagSystemLabels...)
	// filter out invalid/empty
	var systemLabelPatterns []string
	for _, pattern := range flagSystemLabelPatterns {
		if pattern != "" {
			systemLabelPatterns = append(systemLabelPatterns, pattern)
		}
	}

	// Start a ProviderConfig informer for GKE MT cluster, a ProviderConfig is 1:1 with a Tenant
	// and contains GCP specific config for managing the tenant. More at go/gke-mt-resource-model-design.
	var providerConfigObserver multitenancy.ProviderConfigObserver
	if autoscalingOptions.MultitenancyEnabled && b.provConfigInformer != nil {
		go b.provConfigInformer.Run(bgContext)
		providerConfigObserver = b.provConfigInformer
	}

	clusterLocationsObserverImpl := &delegatingClusterLocationsObserver{}
	var clusterLocationsObserver gke.ClusterLocationsObserver = clusterLocationsObserverImpl

	// We need to trim single quotes from ekLookaheadPodStrategy since it's a single-line JSON string
	// and we wrap it in single quotes for CA manifest to treat it as a string.
	flagConfig, err := lookaheadbuffer.ParsePodStrategy(strings.Trim(autoscalingOptions.EkLookaheadPodStrategy, "'"))
	if err != nil {
		klog.Errorf("Cannot parse lookahead pod strategy, error: %v", err)
		return nil, nil, err
	}
	lookaheadBufferStrategyProvider := lookaheadbuffer.NewStrategyProvider(experimentsManager, flagConfig, internalmetrics.Metrics, caVersion)
	resizableVmCustomThresholdsProvider := ekvms_customthresholds.NewCustomThresholdsProvider(experimentsManager, caVersion)

	var provreqProcessor pods.PodListProcessor
	prProcessor := provreq.NewProvReqProcessor(b.prClient, autoscalingOptions.CheckCapacityProcessorInstance)
	loopStartObservers = append(loopStartObservers, b.prCache, prProcessor)
	provreqProcessor = prProcessor

	resizableVmAutoprovisioningProvider, err := ekvms_providers.NewResizableVmAutoprovisioningProvider(b.kubeClient, b.machineConfigProvider, experimentsManager, autoscalingOptions.AutopilotEnabled, autoscalingOptions.EkOnManagedNodesEnabled, autoscalingOptions.E4aOnManagedNodesEnabled, autoscalingOptions.EkAutoprovisioning, autoscalingOptions.E4aAutoprovisioning, internalmetrics.Metrics)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating ResizableVmAutoprovisioningProvider: %v", err)
	}
	autoscalingOptsProvider := computeclass.NewAutoscalingOptionsProvider(b.npcCrdLister, experimentsManager)
	autoprovisioningEligibility := computeclass.NewAutoprovisioningEligibility(b.npcCrdLister, autoscalingOptions.CccNodeAutoprovisioningEnabled)

	klog.V(1).Infof("GCE projectId=%s location=%s", b.projectID, b.location)

	ekSpotEnabledCache := ekvms_providers.NewEkSpotEnabledCache(experimentsManager)

	// Initialize GCE Reservations Puller.
	reservationsPuller, err := gceclient.NewReservationsPuller(b.gceClient, b.consumableReservationsClient, experimentsManager, b.projectID, autoscalingOptions.EnableConsumablePuller, autoscalingOptions.Location)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create reservations puller: %v", err)
	}
	go reservationsPuller.Run(bgContext)

	// Create GKE cloud provider.
	cloudProvider, err := gke.BuildGKE(bgContext, gke.Config{
		ProjectId: b.projectID,
		Location:  b.location,
		InternalClient: gke.InternalClient{
			GCE:                        b.gceClient,
			GKE:                        b.gkeClient,
			ResizableVmClient:          b.resizableVmClient,
			ConsumableReservations:     b.consumableReservationsClient,
			RecommendLocations:         b.recommendLocationsClient,
			AtomicResizeRequest:        b.atomicResizeRequestClient,
			FlexResizeRequest:          b.flexResizeRequestClient,
			FlexAdvisor:                b.flexAdvisorClient,
			ProvisioningRequestManager: b.prManager,
			MachineConfigProvider:      b.machineConfigProvider,
		},
		GceCache:                            b.gceCache,
		Cache:                               b.gkeCache,
		AutoscalingOptionsTracker:           b.optsTracker,
		SurgeTracker:                        surgeUpgradeResourceTracker,
		TemplateCache:                       b.nodeTemplateCache,
		GkeDebuggingSnapshotter:             gkeDebuggingSnapshotter,
		Listers:                             autoscalingKubeClients.ListerRegistry,
		NetworkMatcher:                      matcher,
		GkeReserved:                         gkeReserved,
		SystemLabelPatterns:                 systemLabelPatterns,
		ClusterLocationsObserver:            clusterLocationsObserver,
		AutoscalingOptsProvider:             autoscalingOptsProvider,
		EkSpotEnabledCache:                  ekSpotEnabledCache,
		AutoprovisioningEligibility:         autoprovisioningEligibility,
		ResizableVmAutoprovisioningProvider: resizableVmAutoprovisioningProvider,
		LookaheadBufferStrategyProvider:     lookaheadBufferStrategyProvider,
		ProviderConfigObserver:              providerConfigObserver,
		ProvisioningCache:                   b.prCache,
		DraResourcePredictor:                customResourcesProcessor.GetDraResourcePredictor(),
		ReservationsPuller:                  reservationsPuller,
		ResizableVmCustomThresholdsProvider: resizableVmCustomThresholdsProvider,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error building GKE: %v", err)
	}

	if b.npcCrdLister != nil {
		b.npcCrdLister.SetCloudProvider(cloudProvider)
	}

	selfservice.InitSelfService(cloudProvider)

	// Some autoscalingOptions fields depend on the Cluster proto, and are only properly initialized after it's obtained from the API for the first time
	// as part of the BuildGKE() call above. Refresh the variable to pick up the newest state. Alternatively we could just inline optsTracker.Options()
	// everywhere instead of using a variable in the first place, but it looks pretty bad repeated everywhere.
	autoscalingOptions = b.optsTracker.Options()

	var nodeSizeRecommender nodesizerecommender.NodeSizeRecommender
	if b.nodeSizeRecommenderFactory != nil {
		nodeSizeRecommender, err = b.nodeSizeRecommenderFactory(autoscalingOptions, cloudProvider, experimentsManager)
		if err != nil {
			return nil, nil, fmt.Errorf("error building NodeSizeRecommender: %v", err)
		}
		clusterLocationsObserverImpl.ClusterLocationsObserver = nodeSizeRecommender
	}

	fwHandle, err := framework.NewHandle(bgContext, informerFactory, autoscalingOptions.SchedulerConfig, autoscalingOptions.DynamicResourceAllocationEnabled, autoscalingOptions.CSINodeAwareSchedulingEnabled)
	if err != nil {
		return nil, nil, err
	}
	var snapshotStore clustersnapshot.ClusterSnapshotStore = store.NewDeltaSnapshotStore()
	clusterSnapshot := predicate.NewPredicateSnapshot(snapshotStore, fwHandle, autoscalingOptions.DynamicResourceAllocationEnabled, autoscalingOptions.PredicateParallelism, autoscalingOptions.CSINodeAwareSchedulingEnabled)

	var futureReservationsPuller FutureReservationsPuller = futurereservations.NewNoOpPuller()
	if autoscalingOptions.FutureReservationsBackoffEnabled {
		futureReservationsPuller = futurereservations.NewFutureReservationsPuller(cloudProvider, b.projectID)
		go futureReservationsPuller.Run(bgContext)
	}

	resourcePolicyPuller := placement.NewResourcePolicyPuller(experimentsManager, cloudProvider, b.projectID)
	go resourcePolicyPuller.Run(bgContext)

	// initializing GCE Reservation Blocks Puller
	var reservationBlocksPuller *reservations.BlocksPuller
	if autoscalingOptions.ReservationBlocksEnabled {
		reservationBlocksPuller = reservations.NewBlocksPuller(cloudProvider, reservationsPuller)
		go reservationBlocksPuller.Run(bgContext)
	}

	// initializing Aggregator
	var cccStatusUpdatesCh chan status.UpdateMessage
	if autoscalingOptions.EnhancedCrdStatusReporting {
		cccStatusUpdatesCh = make(chan status.UpdateMessage, 2000)

		ctrClient := b.ctrClient
		if ctrClient == nil {
			s := runtime.NewScheme()
			_ = scheme.AddToScheme(s)
			_ = ccc_api.AddToScheme(s)

			var err error
			ctrClient, err = client.New(b.kubeConfig, client.Options{
				Scheme: s,
			})
			if err != nil {
				klog.Errorf("Cannot create Controller Runtime Client. Error: %v", err)
				return nil, nil, err
			}
		}

		aggregator := status.NewAggregator(b.npcCrdClient, b.npcCrdLister, cccStatusUpdatesCh, ctrClient)
		go aggregator.Start(bgContext)
	}

	var cccInformer cache.SharedIndexInformer
	if autoscalingOptions.EnhancedCrdStatusReporting || autoscalingOptions.EnableComputeClassMinCapacity {
		cccInformerFactory := informer.NewSharedInformerFactory(b.npcCrdClient.CccClient(), 0)
		cccInformer = cccInformerFactory.Cloud().V1().ComputeClasses().Informer()
		cccInformerFactory.Start(bgContext.Done())
	}

	if autoscalingOptions.EnhancedCrdStatusReporting {
		cc_history.SetupHistoryResetObserver(cccInformer, cccStatusUpdatesCh)
	}

	var minCapacityObserver cc_processors.MinCapacityObserver
	if autoscalingOptions.EnableComputeClassMinCapacity {
		minCapacityObserver = cc_processors.NewMinCapacityObserver(internalmetrics.Metrics, cccStatusUpdatesCh)
		cc_processors.SubscribeToComputeClassInformer(cccInformer, minCapacityObserver)

		// Initialize ComputeClass MinimumCapacity Controller.
		minCapacityController := cc_controller.NewMinCapacityController(
			time.Minute,
			b.npcCrdLister,
			autoscalingKubeClients.AllNodeLister(),
			cloudProvider,
			computeclass.NewMatcher(b.npcCrdLister, cloudProvider),
			minCapacityObserver,
			experimentsManager,
		)
		if err := minCapacityController.Start(bgContext); err != nil {
			klog.Errorf("Failed to start MinCapacityController: %v", err)
			return nil, nil, err
		}
	}

	// running NPC Crd Validator
	npcCrdValidator, err := npc_validator.NewValidator(
		b.npcCrdClient,
		b.npcCrdLister,
		cloudProvider,
		internalmetrics.Metrics,
		reservationsPuller,
		autoscalingOptions.GCEOptions.LocalSSDDiskSizeProvider,
		reservationBlocksPuller,
		autoscalingOptions.CloudConfig,
		cccStatusUpdatesCh,
		autoscalingOptions.EnhancedCrdStatusReporting,
	)
	if err != nil {
		klog.Errorf("Cannot create NPC/CCC Crd Validator. Error: %v", err)
		return nil, nil, err
	}
	go npcCrdValidator.Run(bgContext)

	var observers []gke_backoff.BackoffObserver
	if autoscalingOptions.EnhancedCrdStatusReporting {
		inf := npc_history.NewCrdBackoffObserver(cccStatusUpdatesCh, b.npcCrdLister, cloudProvider)
		observers = append(observers, inf)
	}

	// Backoff shared between NAP and core logic
	backoff := gke_backoff.NewGkeBackoff(gke_backoff.Config{
		CustomResourceProcessor: customResourcesProcessor,
		NpcLister:               b.npcCrdLister,
		CloudProvider:           cloudProvider,
		AsyncNodeGroupsEnabled:  autoscalingOptions.AsyncNodeGroupsEnabled,
		FrbConfig: &gke_backoff.FutureReservationsBackoffConfig{
			Enabled:   autoscalingOptions.FutureReservationsBackoffEnabled,
			Provider:  futureReservationsPuller,
			ProjectID: b.projectID,
		},
		Observers: observers,
	})

	// Initializing GCE Flex Advisor
	var flexAdvisor instanceavailability.Provider
	if autoscalingOptions.GCEFlexAdvisorEnabled {
		flexAdvisor, err = flexadvisor.NewFlexAdvisor(bgContext, cloudProvider, b.npcCrdLister, cloudProvider, b.optsTracker, cccStatusUpdatesCh)
		if err != nil {
			klog.Errorf("cannot create Flex Advisor. Error: %v", err)
			return nil, nil, err
		}
	}

	thresholds := []estimator.Threshold{
		estimator.NewStaticThreshold(autoscalingOptions.MaxNodesPerScaleUp, autoscalingOptions.MaxNodeGroupBinpackingDuration),
		estimator.NewSngCapacityThreshold(),
		estimator.NewClusterCapacityThreshold(),
		internal_estimator.NewReservationsThreshold(reservationsPuller, autoscalingOptions.GCEOptions.LocalSSDDiskSizeProvider, cloudProvider, b.optsTracker),
	}

	if autoscalingOptions.GCEFlexAdvisorEnabled {
		thresholds = append(thresholds, flexadvisor.NewInstanceAvailabilityThreshold(flexAdvisor,
			reservationsPuller,
			autoscalingOptions.GCEOptions.LocalSSDDiskSizeProvider,
			b.npcCrdLister,
			cloudProvider,
			experimentsManager,
		))
	}

	var napResourceTrimmer *internalestimator.NapResourceTrimmer
	var napResourceAnalyzerFunc estimator.EstimationAnalyserFunc

	// When setting max pods per node value we calculate approximate resources
	// of "future pods" to account for free space on a node that can be filled
	// with pods later. We do not want to include certain types of pods when calculating
	// approximation - Daemon set pods and pods from "system namespace", as user
	// pods are better predictors for future pods.
	ca := gkeprice.NewGroupingClusterAnalyzer(cloudProvider, autoscalingKubeClients.AllNodeLister(), autoscalingKubeClients.AllPodLister(), systemPodsClassifier)
	napResourceTrimmer = internalestimator.NewNapResourceTrimmer(ca, cloudProvider, autoscalingOptions.AutopilotEnabled)
	napResourceAnalyzerFunc = napResourceTrimmer.NapResourceAnalyzerFunc()

	estimatorBuilder, err := estimator.NewEstimatorBuilder(
		autoscalingOptions.EstimatorName,
		estimator.NewThresholdBasedEstimationLimiter(thresholds),
		edps.NewExtendedDurationPodsFirstDecreasingPodOrderer(),
		napResourceAnalyzerFunc,
		autoscalingOptions.AutoscalingOptions.FastpathBinpackingEnabled,
	)
	if err != nil {
		return nil, nil, err
	}

	capacitybufferPodsRegistry := fakepods.NewRegistry(nil)

	minQuotasTrackerOptions := resourcequotas.TrackerOptions{
		CustomResourcesProcessor: customResourcesProcessor,
		QuotaProvider: resourcequotas.NewCombinedQuotasProvider([]resourcequotas.Provider{
			resourcequotas.NewCloudMinProvider(cloudProvider),
			cc_resourcequota.NewTargetNodeCountProvider(b.npcCrdLister, false, experimentsManager),
		}),
		NodeFilter: resourcequotas.NewCombinedNodeFilter([]resourcequotas.NodeFilter{
			ca_utils.VirtualKubeletNodeFilter{},
			gke_utils.TerminatingNodeFilter{},
			surgeUpgradeResourceTracker,
		}),
	}

	defragMinQuotasTrackerOptions := resourcequotas.TrackerOptions{
		CustomResourcesProcessor: customResourcesProcessor,
		QuotaProvider: resourcequotas.NewCombinedQuotasProvider([]resourcequotas.Provider{
			resourcequotas.NewCloudMinProvider(cloudProvider),
			cc_resourcequota.NewTargetNodeCountProvider(b.npcCrdLister, true, experimentsManager),
		}),
		NodeFilter: minQuotasTrackerOptions.NodeFilter,
	}
	defragMinQuotasTrackerFactory := resourcequotas.NewTrackerFactory(defragMinQuotasTrackerOptions)

	// initialized in setUpProcessors:
	var defragProcessor *defrag_processor.Processor

	quotaProviders := []resourcequotas.Provider{resourcequotas.NewCloudQuotasProvider(cloudProvider)}
	if autoscalingOptions.CapacityQuotasEnabled {
		quotaProviders = append(quotaProviders, capacityquota.NewCapacityQuotasProvider(b.manager.GetClient()))
		cqReconciler := capacityquota.NewCapacityQuotaReconciler(b.manager.GetClient(), capacityquota.ReconcilerOptions{
			// We do not want surgeUpgradeTracker here, as it's thread unsafe and bound to a specific loop
			// This is fine, since NodeFilter in the controller is used only to calculate the usages for observability
			NodeFilter: utils.VirtualKubeletNodeFilter{},
			CustomValidators: []capacityquota.Validator{
				internalcq.NewDefaultBlocklistedLabelsValidator(),
			},
		})
		if err := cqReconciler.SetupWithManager(b.manager); err != nil {
			return nil, nil, fmt.Errorf("failed to setup CapacityQuota reconciler: %w", err)
		}
	}
	quotasTrackerOpts := resourcequotas.TrackerOptions{
		CustomResourcesProcessor: customResourcesProcessor,
		QuotaProvider:            resourcequotas.NewCombinedQuotasProvider(quotaProviders),
		NodeFilter: resourcequotas.NewCombinedNodeFilter([]resourcequotas.NodeFilter{
			utils.VirtualKubeletNodeFilter{},
			surgeUpgradeResourceTracker,
		}),
	}

	autoscalingProcessors, err := setUpProcessors(
		bgContext,
		caVersion,
		b.optsTracker,
		&autoscalingOptions,
		b.kubeConfigJSON,
		b.kubeClient,
		b.httpClient,
		cloudProvider,
		backoff,
		updateInfoFetcher,
		customResourcesProcessor,
		clusterSnapshot,
		reservationsPuller,
		&defragProcessor,
		capacitybufferPodsRegistry,
		clusterScaleToZeroProcessor,
		b.prInjector,
		b.npcCrdLister,
		deleteOptions,
		drainabilityRules,
		autoscalingKubeClients,
		matcher,
		informerFactory,
		gkeReserved,
		napResourceTrimmer,
		systemLabelPatterns,
		systemPodsClassifier,
		metricsFilter,
		nodeSizeRecommender,
		experimentsManager,
		b.prClient,
		b.prCache,
		provreqProcessor,
		resizableVmAutoprovisioningProvider,
		lookaheadBufferStrategyProvider,
		reservationBlocksPuller,
		flexAdvisor,
		resourcePolicyPuller,
		resizableVmCustomThresholdsProvider,
		b.snowflakeWatcher,
		b.nodeQuotaWatcher,
		b.eventLogger,
		cccStatusUpdatesCh,
		minCapacityObserver,
		defragMinQuotasTrackerFactory)
	if err != nil {
		return nil, nil, err
	}
	penaltyChecker := provider.NewRelaxedNodeGroupPenaltyChecker(experimentsManager, cloudProvider.IsAutopilotEnabled())
	expanderStrategy, err := internalexpander.ExpanderStrategyFromString(
		autoscalingOptions.ExpanderNames,
		cloudProvider,
		penaltyChecker,
		autoscalingKubeClients,
		b.kubeClient,
		defragProcessor,
		reservationsPuller,
		autoscalingOptions.ConfigNamespace,
		autoscalingOptions.PvmUnfitnessPenaltyEnabled,
		autoscalingOptions.AutopilotEnabled,
		autoscalingOptions.GCEOptions.LocalSSDDiskSizeProvider,
		autoscalingProcessors.AsyncNodeGroupStateChecker,
		flexAdvisor,
		b.npcCrdLister,
		autoscalingOptions.GCEFlexAdvisorEnabled,
		experimentsManager)
	if err != nil {
		return nil, nil, err
	}

	prOrchestrator := provreqorchestrator.New(b.prClient, []provreqorchestrator.ProvisioningClass{checkcapacity.New(b.prClient, b.prInjector), besteffortatomic.New(b.prClient), scaleup_pr.NewQueuedProvisioningClass(cloudProvider, b.prClient, b.prCache, autoscalingOptions.MaxProvReqBinpackingDuration, autoscalingOptions.AutoscalingOptions.FastpathBinpackingEnabled, experimentsManager, napResourceAnalyzerFunc)})
	scaleUpOrchestrator := provreqorchestrator.NewWrapperOrchestrator(prOrchestrator)

	opts := coreoptions.AutoscalerOptions{
		AutoscalingOptions:         autoscalingOptions.AutoscalingOptions,
		CloudProvider:              cloudProvider,
		AutoscalingKubeClients:     autoscalingKubeClients,
		KubeClient:                 b.kubeClient,
		ExpanderStrategy:           expanderStrategy,
		Processors:                 autoscalingProcessors,
		LoopStartObservers:         loopStartObservers,
		Backoff:                    backoff,
		ScaleUpFailuresRegistry:    scaleUpFailuresRegistry,
		EstimatorBuilder:           estimatorBuilder,
		FrameworkHandle:            fwHandle,
		ClusterSnapshot:            clusterSnapshot,
		DebuggingSnapshotter:       gkeDebuggingSnapshotter,
		ScaleUpOrchestrator:        scaleUpOrchestrator,
		DeleteOptions:              deleteOptions,
		DrainabilityRules:          drainabilityRules,
		InformerFactory:            informerFactory,
		CapacityBufferPodsRegistry: capacitybufferPodsRegistry,
		QuotasTrackerOptions:       quotasTrackerOpts,
		MinQuotasTrackerOptions:    minQuotasTrackerOptions,
	}

	if b.manager != nil {
		opts.KubeClientNew = b.manager.GetClient()
		opts.KubeCache = b.manager.GetCache()
	}

	// This metric should be published only once.
	internalmetrics.Metrics.RecordClusterType(autoscalingOptions.AutopilotEnabled)
	internalmetrics.Metrics.UpdateProfile(autoscalingOptions.Profile)
	recordFeaturesEnablementMetrics(autoscalingOptions)

	// Create autoscaler.
	autoscaler, err := builder.NewAutoscaler(bgContext, opts, informerFactory)
	if err != nil {
		return autoscaler.(*core.StaticAutoscaler), nil, err
	}

	podObserver := b.podObserver
	if podObserver == nil {
		podObserver = loop.StartPodObserver(bgContext, b.kubeClient)
	}
	// A ProvisioningRequestPodsInjector is used as provisioningRequestProcessingTimesGetter here to obtain the last time a
	// ProvisioningRequest was processed. This is because the ProvisioningRequestPodsInjector in addition to injecting pods
	// also marks the ProvisioningRequest as accepted or failed.
	trigger := loop.NewLoopTrigger(autoscaler, b.prInjector, podObserver, autoscalingOptions.ScanInterval)

	informerFactory.Start(bgContext.Done())

	// Cluster Autoscaler needs to wait until all K8s API informer caches are synced before starting normal operation - otherwise it can take wrong/suboptimal decisions based
	// on inconsistent K8s state. The set of APIs/informers that CA needs to wait for changes depending on some AutoscalingOptions fields (e.g. DRA API is needed if and only if
	// AutoscalingOptions.EnableDynamicResourceAllocation is true). The value for these AutoscalingOptions fields can be based on the Cluster proto/experiments
	// (e.g. Cluster.CurrentEmulatedVersion or DRA::Enabled experiment for DRA), which can change over time. The following call periodically refreshes the Cluster state and
	// recomputes AutoscalingOptions while waiting for the informers to sync. CA is restarted to re-initialize using the new values if AutoscalingOptions change.
	if err := informerutils.WaitForInformerSyncWithClusterRefresh(informerFactory, cloudProvider, b.optsTracker); err != nil {
		return nil, nil, fmt.Errorf("unable to start and sync resource informers: %v", err)
	}

	// We use this ugly hack because the customResourcesProcessor is used by
	// various components that do not have access to autoscaler context.
	// If they pass nil we use the context set here as a backup.
	staticAutoscaler := autoscaler.(*core.StaticAutoscaler)
	customResourcesProcessor.SetContext(staticAutoscaler.AutoscalingContext)
	// And we need this because custom resource processor needs information
	// about machine config and available resources, but also is created before cloud provider.
	customResourcesProcessor.SetCloudProvider(cloudProvider)

	cloudProvider.RegisterInitializationFunc(initialization.RecoverPendingScaleUps(staticAutoscaler.AutoscalingContext, cloudProvider))
	rrer.Init(staticAutoscaler.AutoscalingContext, cloudProvider, autoscalingProcessors.ScaleStateNotifier)
	if staticAutoscaler.AutoscalingContext != nil && staticAutoscaler.AutoscalingContext.ClusterStateRegistry != nil {
		cloudProvider.SetScaleUpTimeProvider(staticAutoscaler.AutoscalingContext.ClusterStateRegistry)
	}
	return staticAutoscaler, trigger, nil
}

func (b *Builder) validateOptions() error {
	if b.informerFactory == nil {
		return fmt.Errorf("informerFactory is missing: ensure WithInformerFactory() is called")
	}
	if b.kubeConfig == nil || b.kubeConfigJSON == nil {
		return fmt.Errorf("kubeConfig is missing: ensure WithKubeConfig() is called")
	}
	if b.tokenSource == nil || b.projectID == "" || b.location == "" {
		return fmt.Errorf("gce Configuration (Token/Project/Location) is missing")
	}
	if b.kubeClient == nil {
		return fmt.Errorf("kubeClient is missing: ensure WithKubeClient() is called")
	}
	if b.nodeTemplateCache == nil {
		return fmt.Errorf("nodeTemplateCache is missing: ensure WithNodeTemplateCache() is called")
	}
	if b.updateInfoLister == nil {
		return fmt.Errorf("updateInfoLister is missing: ensure WithUpdateInfoLister() is called")
	}
	if b.npcCrdClient == nil {
		return fmt.Errorf("npcCrdClient is missing: ensure WithNpcCrdClient() is called")
	}
	if b.npcCrdLister == nil {
		return fmt.Errorf("npcCrdLister is missing: ensure WithNpcCrdLister() is called")
	}
	if b.prClient == nil || b.prInjector == nil {
		return fmt.Errorf("ProvisioningRequest dependencies are missing: ensure WithProvReqClient() and WithProvReqInjector() are called")
	}
	if b.prCache == nil {
		return fmt.Errorf("ProvisioningRequest cache is missing: ensure WithProvReqCache() is called")
	}
	if b.httpClient == nil {
		return fmt.Errorf("http client is missing: ensure WithHttpClient() is called")
	}
	if b.gceClient == nil {
		return fmt.Errorf("gce client is missing: ensure WithGCEClient() is called")
	}
	if b.gkeClient == nil {
		return fmt.Errorf("gke client is missing: ensure WithGKEClient() is called")
	}
	if b.recommendLocationsClient == nil {
		return fmt.Errorf("recommend locations client is missing: ensure WithRecommendLocationsClient() is called")
	}
	if b.atomicResizeRequestClient == nil {
		return fmt.Errorf("atomic resize request client is missing: ensure WithAtomicResizeRequestClient() is called")
	}
	if b.flexResizeRequestClient == nil {
		return fmt.Errorf("flex resize request client is missing: ensure WithFlexResizeRequestClient() is called")
	}
	if b.flexAdvisorClient == nil {
		return fmt.Errorf("flex advisor client is missing: ensure WithFlexAdvisorClient() is called")
	}
	if b.prManager == nil {
		return fmt.Errorf("provisioning request manager is missing: ensure WithProvReqManager() is called")
	}
	if b.gceCache == nil || b.gkeCache == nil {
		return fmt.Errorf("caches are missing: ensure WithGKECache() and WithGCECache() are called")
	}
	if b.machineConfigProvider == nil {
		return fmt.Errorf("machine config provider is missing: ensure WithMachineConfigProvider() is called")
	}
	if b.snowflakeWatcher == nil {
		return fmt.Errorf("snowflake watcher is missing: ensure WithSnowflakeWatcher() is called")
	}
	if b.nodeQuotaWatcher == nil {
		return fmt.Errorf("node quota watcher is missing: ensure WithNodeQuotaWatcher() is called")
	}
	return nil
}

func recordFeaturesEnablementMetrics(options internalopts.AutoscalingOptions) {
	// The value of autoscalingOptions.DynamicResourceAllocationEnabled depends on a CLI flag, experiment value, and the Cluster proto. The option should
	// be properly initialized as part of BuildGKE() - should be good to use here.
	logAndRecordFeatureMetric(internalmetrics.DraFeatureName, "DRA integration", options.DynamicResourceAllocationEnabled)
	logAndRecordFeatureMetric(internalmetrics.CapacityBufferFeatureName, "Capacity Buffer controller", options.CapacitybufferControllerEnabled && options.CapacitybufferPodInjectionEnabled)
	logAndRecordFeatureMetric(internalmetrics.HTNAPFeatureName, "HTNAP", options.AsyncNodeGroupsEnabled)
	logAndRecordFeatureMetric(internalmetrics.FastpathBinpackingFeatureName, "Fastpath Binpacking", options.FastpathBinpackingEnabled)
	logAndRecordFeatureMetric(internalmetrics.IncreasedMaxNodesPerScaleUpFeatureName, "IncreasedMaxNodesPerScaleUp", options.MaxNodesPerScaleUp > optstracking.DecreasedMaxNodesPerScaleUp)
	logAndRecordFeatureMetric(internalmetrics.IncreasedNapMaxNodesFeatureName, "IncreasedNapMaxNodes", options.NapMaxNodes > optstracking.DecreasedNapMaxNodesCount)
}

func logAndRecordFeatureMetric(feature internalmetrics.FeatureName, featureNameForLog string, enabled bool) {
	klog.Infof("%s enabled: %v", featureNameForLog, enabled)
	internalmetrics.Metrics.UpdateFeatureEnabled(feature, enabled)
}
