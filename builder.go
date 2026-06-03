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

package main

import (
	"context"

	internalautoscaler "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoscaler"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/consumablereservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/flexadvisorclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizablevms"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/cli"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/nodesizerecommender"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/nodequota"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/nodesnowflake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility"
)

// initBuilder returns the CA Builder without any Google-restricted clients.
// When updating, please reflect the changes in all the initBuilder implementations.
func initBuilder(ctx context.Context, stopCh chan struct{}, optsTracker *optstracking.OptionsTracker) *internalautoscaler.Builder {
	options := optsTracker.Options()
	experimentsManager := optsTracker.ExperimentsManager()

	// K8s client and related objects
	kubeConfig, kubeConfigJSON := internalautoscaler.KubeConfig(options)
	kubeClient := internalautoscaler.KubeClient(kubeConfig)
	informerFactory := internalautoscaler.NewSharedInformerFactory(options, kubeClient, 0)

	// GCE config and HTTP client
	projectID, location, tokenSource := internalautoscaler.MustCreateGCEConfig(options, ctx)
	httpClient := internalautoscaler.MustCreateHttpClient(tokenSource)

	// Internal CA caches
	nodeTemplateCache := nodetemplate.NewCache()
	gceCache, gkeCache := internalautoscaler.MustCreateCaches(nodeTemplateCache)
	machineConfigProvider := internalautoscaler.CreateMachineConfigProvider(options, kubeConfigJSON)

	// Clients and related objects for GKE-specific CRDs
	// UpdateInfo CRD
	updateInfoFactory, updateInfoLister := internalautoscaler.MustCreateUpdateInfoLister(kubeConfigJSON)
	// NodeProvisioningConfig (NPC) / CustomProvisioningClass (CCC) CRDs
	npcCrdClient := internalautoscaler.MustCreateNpcCrdClient(kubeConfigJSON)
	npcCrdLister := internalautoscaler.MustCreateNpcCrdLister(optsTracker, stopCh, npcCrdClient)
	// ProvisioningRequest CRD
	prClient := internalautoscaler.MustCreateProvReqClient(kubeConfigJSON, informerFactory, ctx)
	prInjector := internalautoscaler.MustCreateProvReqInjector(options, prClient)
	prCache := internalautoscaler.MustCreatePRCache(prClient)
	// ProviderConfig CRD (related to MultiTenancy)
	provConfigInformer := internalautoscaler.MustCreateProviderConfigInformer(options, kubeConfigJSON)

	// Main GCE client
	gceClient := internalautoscaler.MustCreateGceClient(
		projectID, options, httpClient, gkeCache, experimentsManager, provConfigInformer,
	)
	// Dedicated GCE sub-clients
	provReqManager := internalautoscaler.MustCreatePRManager(prClient, httpClient, projectID, options, prCache, experimentsManager, gceClient, gceCache)
	atomicRRClient := internalautoscaler.MustCreateAtomicResizeRequestClient(httpClient, projectID, options, provConfigInformer, experimentsManager)
	flexRRClient := internalautoscaler.MustCreateFlexResizeRequestClient(httpClient, projectID, options)

	// TODO(b/476064174): Move initialization of IO to separate structs.
	builder := internalautoscaler.NewBuilder(optsTracker).
		WithCAVersion(cli.ComponentVersion()).
		WithKubeConfig(kubeConfig).
		WithKubeJSON(kubeConfigJSON).
		WithKubeClient(kubeClient).
		WithInformerFactory(informerFactory).
		WithProjectID(projectID).
		WithLocation(location).
		WithTokenSource(tokenSource).
		WithHttpClient(httpClient).
		WithNodeTemplateCache(nodeTemplateCache).
		WithGCECache(gceCache).
		WithGKECache(gkeCache).
		WithMachineConfigProvider(machineConfigProvider).
		WithUpdateInfoLister(updateInfoLister, updateInfoFactory).
		WithNpcCrdClient(npcCrdClient).
		WithNpcCrdLister(npcCrdLister).
		WithProvReqClient(prClient).
		WithProvReqInjector(prInjector).
		WithProvReqCache(prCache).
		WithProviderConfigInformer(provConfigInformer).
		WithGCEClient(gceClient).
		WithProvReqManager(provReqManager).
		WithAtomicResizeRequestClient(atomicRRClient).
		WithFlexResizeRequestClient(flexRRClient)

	return configureGKEInternalClients(builder, ctx, stopCh)
}

// configureGKEInternalClients is used to configure a Builder with the internal clients that are cut out from GKE Cluster Autoscaler in OSS.
// When GKE CA is built in GKE, this function is overwritten in the main pkg init() - with an implementation that builds actual non-stub clients.
var configureGKEInternalClients = configureStubClients

// configureStubClients configures the provided Builder with stub/no-op implementations of the internal clients - so that the code still compiles
// in OSS with the actual internal client code cut out.
func configureStubClients(builder *internalautoscaler.Builder, ctx context.Context, stopCh chan struct{}) *internalautoscaler.Builder {
	// WARN: All the .With() lines below have to be kept in sync with configureActualClients(). If you're adding a new line here, you should
	//       add a new one there as well.

	optsTracker := builder.GetOptionsTracker()
	options := optsTracker.Options()

	// GKE API client
	httpClient := builder.GetHttpClient()
	gkeApiClient := internalautoscaler.MustCreateGKEAPIClient(httpClient, options.UserAgent, *gkeclient.GkeAPIEndpoint)

	// GKE client
	projectID := builder.GetProjectID()
	location := builder.GetLocation()
	provConfigInformer := builder.GetProviderConfigInformer()
	machineConfigProvider := builder.GetMachineConfigProvider()
	gkeClient := internalautoscaler.MustCreateGKEClient(gkeApiClient, nil, projectID, location, options, provConfigInformer, machineConfigProvider)

	return builder.
		WithNodeSizeRecommenderFactory(nodesizerecommender.DefaultFactory).
		WithNodeQuotaWatcher(nodequota.NewNoOpWatcher()).
		WithSnowflakeWatcher(nodesnowflake.NewNoOpWatcher()).
		WithFlexAdvisorClient(flexadvisorclient.NewNoOpFlexAdvisorClient()).
		WithRecommendLocationsClient(gceclient.NewAlwaysErrorRecommendLocationsClient()).
		WithEventLogger(visibility.NewNoOpEventLogger()).
		WithResizableVmClient(resizablevms.NewNoOpClient()).
		WithConsumableReservationsClient(consumablereservations.NewNoOpClient()).
		WithGkeClient(gkeClient)
}
