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
	"context"
	"net/http"
	"time"

	"golang.org/x/oauth2"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	provreqclientset "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/clientset/versioned"
	provreqinformers "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/informers/externalversions"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/processors/provreq"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	kube_client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	mccclient "k8s.io/gke-autoscaling/cluster-autoscaler/apis/machineconfig/client/clientset/versioned"
	mccinformers "k8s.io/gke-autoscaling/cluster-autoscaler/apis/machineconfig/client/informers/externalversions"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/bulkmig"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkeapi "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	gkeutil "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util"
	npc_client "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/client"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	gcecfg "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/gce"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	http_client "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/httpclient"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/multitenancy"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"
	prmanager "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/manager"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/clientset/versioned"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/informers/externalversions"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/listers/nodemanagement.gke.io/v1alpha1"
	"k8s.io/klog/v2"
)

// KubeConfig returns the rest config and config json from AutoscalingOptions.
func KubeConfig(opts internalopts.AutoscalingOptions) (*rest.Config, *rest.Config) {
	kubeConfig := kube_util.GetKubeConfig(opts.KubeClientOpts)
	kubeConfigJSON := rest.CopyConfig(kubeConfig)
	kubeConfigJSON.ContentType = runtime.ContentTypeJSON
	return kubeConfig, kubeConfigJSON
}

// KubeClient returns the clientset for given config.
func KubeClient(config *rest.Config) *kube_client.Clientset {
	return kube_client.NewForConfigOrDie(config)
}

// NewSharedInformerFactory returns a new SharedInformerFactory with a trim transformer.
// Filters pods and nodes according to label and field selectors passed in options.
func NewSharedInformerFactory(options internalopts.AutoscalingOptions, kubeClient kube_client.Interface, defaultResync time.Duration) informers.SharedInformerFactory {
	trim := func(obj interface{}) (interface{}, error) {
		if accessor, err := meta.Accessor(obj); err == nil {
			accessor.SetManagedFields(nil)
		}
		return obj, nil
	}

	factory := informers.NewSharedInformerFactoryWithOptions(kubeClient, defaultResync, informers.WithTransform(trim))
	overridePodInformer(factory, options)
	overrideNodeInformer(factory, options)

	return factory
}

// Overrides factory's standard Pod informer with an implementation that only watches Pods
// matching the configured label and field selectors.
func overridePodInformer(factory informers.SharedInformerFactory, options internalopts.AutoscalingOptions) {
	factory.InformerFor(&apiv1.Pod{}, func(client kube_client.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
		return cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListWithContextFunc: func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
					opts.LabelSelector = options.PodWatchLabelSelector
					opts.FieldSelector = options.PodWatchFieldSelector
					return client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, opts)
				},
				WatchFuncWithContext: func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
					opts.LabelSelector = options.PodWatchLabelSelector
					opts.FieldSelector = options.PodWatchFieldSelector
					return client.CoreV1().Pods(metav1.NamespaceAll).Watch(ctx, opts)
				},
			},
			&apiv1.Pod{},
			resyncPeriod,
			cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		)
	})
}

// Overrides factory's standard Node informer with an implementation that only watches Nodes
// matching the configured label and field selectors.
func overrideNodeInformer(factory informers.SharedInformerFactory, options internalopts.AutoscalingOptions) {
	factory.InformerFor(&apiv1.Node{}, func(client kube_client.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
		return cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListWithContextFunc: func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
					opts.LabelSelector = options.NodeWatchLabelSelector
					opts.FieldSelector = options.NodeWatchFieldSelector
					return client.CoreV1().Nodes().List(ctx, opts)
				},
				WatchFuncWithContext: func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
					opts.LabelSelector = options.NodeWatchLabelSelector
					opts.FieldSelector = options.NodeWatchFieldSelector
					return client.CoreV1().Nodes().Watch(ctx, opts)
				},
			},
			&apiv1.Node{},
			resyncPeriod,
			cache.Indexers{},
		)
	})
}

// MustCreateUpdateInfoLister creates a new info lister and factory.
func MustCreateUpdateInfoLister(kubeConfigJSON *rest.Config) (externalversions.SharedInformerFactory, v1alpha1.UpdateInfoLister) {
	updateInfoClient, err := versioned.NewForConfig(kubeConfigJSON)
	if err != nil {
		klog.Fatalf("error getting update info client: %v", err)
	}
	factory := externalversions.NewSharedInformerFactory(updateInfoClient, 1*time.Hour)
	infoLister := factory.Nodemanagement().V1alpha1().UpdateInfos().Lister()
	return factory, infoLister
}

// MustCreateNpcCrdClient creates a new crd client.
func MustCreateNpcCrdClient(config *rest.Config) npc_client.Client {
	npcCrdClient, err := npc_client.NewClient(config)
	if err != nil {
		klog.Fatalf("cannot create CCC Crd client set. Error: %v", err)
	}
	return npcCrdClient
}

// MustCreateNpcCrdLister creates a new crd lister.
func MustCreateNpcCrdLister(ctx context.Context, optionsTracker *optstracking.OptionsTracker, client npc_client.Client) npc_lister.Lister {
	npcCrdLister, err := npc_lister.NewCccLister(ctx, client, optionsTracker)
	if err != nil {
		klog.Fatalf("cannot create CCC Lister. Error: %v", err)
	}
	return npcCrdLister
}

// MustCreateGCEConfig returns the ProjectID, Location, and TokenSource.
func MustCreateGCEConfig(ctx context.Context, opts internalopts.AutoscalingOptions) (string, string, oauth2.TokenSource) {
	gceProviderConfig, err := gcecfg.GceConfigProvider(opts.CloudConfig)
	if err != nil {
		klog.Fatalf("error getting GCE provider config: %v", err)
	}

	tokenSource, err := gcecfg.GetTokenSource(gceProviderConfig, ctx)
	if err != nil {
		klog.Fatalf("error getting oauth2 token source: %v", err)
	}

	projectID, location, err := gcecfg.GetProjectAndLocation(opts.Regional, gceProviderConfig, ctx)
	if err != nil {
		klog.Fatalf("error getting project ID and location: %v", err)
	}

	return projectID, location, tokenSource
}

// MustCreateProvReqClient returns the ProvisioningRequestClient.
func MustCreateProvReqClient(ctx context.Context, kubeConfigJSON *rest.Config, sharedInformerFactory informers.SharedInformerFactory) *provreqclient.ProvisioningRequestClient {
	podTemplLister := sharedInformerFactory.Core().V1().PodTemplates().Lister()
	prClient, err := provreqclientset.NewForConfig(kubeConfigJSON)
	if err != nil {
		klog.Fatalf("failed to create PR client: %v", err)
	}
	factory := provreqinformers.NewSharedInformerFactory(prClient, 1*time.Hour)
	provReqLister := factory.Autoscaling().V1().ProvisioningRequests().Lister()

	client := provreqclient.NewProvisioningRequestClient(prClient, provReqLister, podTemplLister)

	factory.Start(ctx.Done())
	klog.Info("Waiting for Provisioning Request cache to sync...")
	synced := factory.WaitForCacheSync(ctx.Done())
	for _, ok := range synced {
		if !ok {
			klog.Fatalf("failed to sync Provisioning Request informers")
		}
	}
	klog.V(2).Info("Successful initial Provisioning Request sync")

	return client
}

// MustCreateProvReqInjector returns the ProvisioningRequestPodsInjector.
func MustCreateProvReqInjector(opts internalopts.AutoscalingOptions, prClient *provreqclient.ProvisioningRequestClient) *provreq.ProvisioningRequestPodsInjector {
	ossProvReqInjector := provreq.NewProvisioningRequestPodsInjector(
		prClient,
		opts.ProvisioningRequestInitialBackoffTime,
		opts.ProvisioningRequestMaxBackoffTime,
		opts.ProvisioningRequestMaxBackoffCacheSize,
		opts.CheckCapacityBatchProcessing,
		opts.CheckCapacityProcessorInstance,
	)
	return ossProvReqInjector
}

// MustCreateHttpClient returns the http client.
func MustCreateHttpClient(tokenSource oauth2.TokenSource) *http.Client {
	return http_client.CreateHttpClient(tokenSource)
}

// MustCreateCaches returns both gce and gke caches.
func MustCreateCaches(nodeTemplateCache *nodetemplate.Cache) (*gce.GceCache, *gke.GkeCache) {
	gceCache := gce.NewGceCache()
	gkeCache := gke.NewGkeCache(gceCache, nodeTemplateCache)
	return gceCache, gkeCache
}

// MustCreateProviderConfigInformer creates the informer but DOES NOT start it yet.
func MustCreateProviderConfigInformer(opts internalopts.AutoscalingOptions, kubeConfigJSON *rest.Config) ProviderConfigManager {
	if !opts.MultitenancyEnabled {
		return nil
	}
	dc := dynamic.NewForConfigOrDie(kubeConfigJSON)
	return multitenancy.NewProviderConfigInformer(dc)
}

// MustCreateGceClient returns the gce client.
func MustCreateGceClient(
	projectID string,
	opts internalopts.AutoscalingOptions,
	httpClient *http.Client,
	cache gceclient.MigInfoProvider,
	experimentsManager experiments.Manager,
	observer multitenancy.ProviderConfigObserver,
) gceclient.AutoscalingInternalGceClient {
	region, err := gkeutil.GetRegionFromLocation(opts.Location)
	if err != nil {
		klog.Warningf("Failed to get region from location %q: %v", opts.Location, err)
	}
	gceConfig := gceclient.GceConfig{
		ProjectId:              projectID,
		ProjectNumber:          int64(opts.ProjectNumber),
		MultitenancyEnabled:    opts.MultitenancyEnabled,
		HttpClient:             httpClient,
		Cache:                  cache,
		ClusterName:            opts.ClusterName,
		UserAgent:              opts.UserAgent,
		ExperimentsManager:     experimentsManager,
		Endpoint:               opts.GceEndpoint,
		ProviderConfigObserver: observer,
		Region:                 region,
	}
	client, err := gceclient.CreateClient(gceConfig)
	if err != nil {
		klog.Fatalf("error creating GCE client: %v", err)
	}
	return client
}

// MustCreateAtomicResizeRequestClient returns the atomic rr client.
func MustCreateAtomicResizeRequestClient(httpClient *http.Client, projectID string, opts internalopts.AutoscalingOptions, observer multitenancy.ProviderConfigObserver, experimentsManager experiments.Manager) resizerequestclient.ResizeRequestClient {
	var client resizerequestclient.ResizeRequestClient
	var err error

	if opts.MultitenancyEnabled && observer != nil {
		mtClient, err := resizerequestclient.NewMultitenancyResizeRequestClientBeta(httpClient, projectID, int64(opts.ProjectNumber), opts.UserAgent, opts.GceEndpoint, resizerequestclient.ResizeRequestModeAtomic, experimentsManager)
		if err != nil {
			klog.Fatalf("Failed to create MT Atomic RR Client: %v", err)
		}
		// Register handlers immediately
		if err := observer.RegisterEventHandlers("MTResizeRequestClient", mtClient.AddProviderConfig, mtClient.DeleteProviderConfig); err != nil {
			klog.Fatalf("Failed to register Atomic RR handlers: %v", err)
		}
		client = mtClient
	} else {
		client, err = resizerequestclient.NewResizeRequestClientBeta(httpClient, projectID, opts.UserAgent, opts.GceEndpoint, resizerequestclient.ResizeRequestModeAtomic)
	}
	if err != nil {
		klog.Fatalf("Failed to create Atomic RR Client: %v", err)
	}
	return client
}

// MustCreateFlexResizeRequestClient returns the flex rr client.
func MustCreateFlexResizeRequestClient(httpClient *http.Client, projectID string, opts internalopts.AutoscalingOptions) resizerequestclient.ResizeRequestClient {
	client, err := resizerequestclient.NewResizeRequestClientBeta(httpClient, projectID, opts.UserAgent, opts.GceEndpoint, resizerequestclient.ResizeRequestModeFlex)
	if err != nil {
		klog.Fatalf("Failed to create Flex RR Client: %v", err)
	}
	return client
}

// MustCreateGKEAPIClient returns the gke api client.
func MustCreateGKEAPIClient(httpClient *http.Client, userAgent, endpoint string) gkeapi.Client {
	gkeApiClient, err := gkeapi.NewClient(httpClient, userAgent, endpoint)
	if err != nil {
		klog.Fatalf("Failed to create GKE API Client: %v", err)
	}
	return gkeApiClient
}

// MustCreateGKEClient returns the gke client.
func MustCreateGKEClient(apiClient gkeapi.Client, nodePoolTranslator gkeclient.NodePoolTranslator, projectID, location string, opts internalopts.AutoscalingOptions, observer multitenancy.ProviderConfigObserver, machineConfigProvider *machinetypes.MachineConfigProvider, experimentsManager experiments.Manager) gkeclient.AutoscalingGkeClient {
	var client gkeclient.AutoscalingGkeClient
	var err error
	if opts.MultitenancyEnabled && observer != nil {
		mtClient, err := gkeclient.NewMultitenancyGkeClientV1beta1(apiClient, nodePoolTranslator, projectID, location, opts.ClusterName, machineConfigProvider, opts.NapMaxNodes, experimentsManager)
		if err != nil {
			klog.Fatalf("Failed to create MT GKE Client: %v", err)
		}
		if err = observer.RegisterEventHandlers("MTGKEClient", mtClient.AddProviderConfig, mtClient.DeleteProviderConfig); err != nil {
			klog.Fatalf("Failed to register GKE Client handlers: %v", err)
		}
		client = mtClient
	} else {
		client, err = gkeclient.NewAutoscalingGkeClientV1beta1(apiClient, nodePoolTranslator, projectID, location, opts.ClusterName, machineConfigProvider, opts.NapMaxNodes)
		if err != nil {
			klog.Fatalf("Failed to create GKE Client: %v", err)
		}
	}
	return client
}

// MustCreatePRCache returns the provisioning request cache.
func MustCreatePRCache(prClient *provreqclient.ProvisioningRequestClient) *provreqcache.QueuedProvisioningCache {
	if prClient == nil {
		return nil
	}
	return provreqcache.NewQueuedProvisioningCache(prClient)
}

// MustCreatePRManager returns the provisioning request manager.
func MustCreatePRManager(
	prClient *provreqclient.ProvisioningRequestClient,
	httpClient *http.Client,
	projectID string,
	opts internalopts.AutoscalingOptions,
	prCache *provreqcache.QueuedProvisioningCache,
	experimentsManager experiments.Manager,
	gceClient gce.AutoscalingGceClient,
	cache *gce.GceCache,
) prmanager.ProvisioningRequestManager {
	queuedResizeRequestService, err := resizerequestclient.NewResizeRequestClientV1(httpClient, projectID, opts.UserAgent, opts.GceEndpoint, resizerequestclient.ResizeRequestModeQueued)
	if err != nil {
		klog.Fatalf("failed to create queuedResizeRequestService: %v", err)
	}
	bulkMigClient, err := bulkmig.NewBulkMigClientBeta(httpClient, projectID, opts.UserAgent, opts.GceEndpoint, gceClient, cache)
	if err != nil {
		klog.Fatalf("failed to create bulkMigClient: %v", err)
	}
	manager, err := prmanager.NewProvisioningRequestManager(
		prClient,
		queuedResizeRequestService,
		bulkMigClient,
		projectID,
		prCache,
		experimentsManager,
	)
	if err != nil {
		klog.Fatalf("failed to create ProvisioningRequestManager: %v", err)
	}
	return manager
}

// CreateMachineConfigClient returns a new clientset for MachineConfigs.
func CreateMachineConfigClient(kubeConfig *rest.Config) mccclient.Interface {
	client, err := mccclient.NewForConfig(kubeConfig)
	if err != nil {
		klog.Errorf("Failed to create MachineConfig client, will use hard-coded configuration: %v", err)
		return nil
	}
	return client
}

// CreateMachineConfigProvider returns the machine config provider.
func CreateMachineConfigProvider(ctx context.Context, opts internalopts.AutoscalingOptions, mccClient mccclient.Interface, experimentsManager experiments.Manager) *machinetypes.MachineConfigProvider {
	var source *machinetypes.Source
	if opts.MachineConfigEnabled && mccClient != nil {
		mccInformerFactory := mccinformers.NewSharedInformerFactory(mccClient, opts.MachineConfigRefreshInterval)
		source = machinetypes.NewSource(mccInformerFactory, opts.CvmMachineConfigEnabled)
		go source.Run(ctx)
	}
	provider := machinetypes.NewMachineConfigProvider(source)
	provider.SetExperimentsManager(experimentsManager)
	provider.SetMetrics(internalmetrics.Metrics)
	return provider
}
