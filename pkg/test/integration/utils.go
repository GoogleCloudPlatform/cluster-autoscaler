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

package integration

import (
	"context"
	"testing"
	"time"

	cccfake "github.com/googlecloudplatform/compute-class-api/client/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/core"
	"k8s.io/autoscaler/cluster-autoscaler/loop"
	"k8s.io/autoscaler/cluster-autoscaler/processors/provreq"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	fake_k8s "k8s.io/autoscaler/cluster-autoscaler/utils/fake"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	osconfig "k8s.io/gke-autoscaling/cluster-autoscaler/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoscaler"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/consumablereservations"
	fakegce "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/fake"
	fakerl "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizablevms"
	resizerequest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest/fake"
	gkeclientfake "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient/fake"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	npc_client "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/client"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	fakeflexadvisor "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/gkedebuggingsnapshot"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/nodequota"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/nodesnowflake"
	provreqtest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/provreq"

	updateinfosclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/clientset/versioned/fake"
	updateinfosinformers "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/informers/externalversions"
	schedulerconfiglatest "k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	fakeclock "k8s.io/utils/clock/testing"
)

// TestInfrastructure holds the dependencies for a test.
type TestInfrastructure struct {
	// Fakes encapsulates all the fake clients and providers.
	Fakes *FakeSet
	// Snapshotter represents the debugging snapshotter.
	Snapshotter *gkedebuggingsnapshot.GkeDebuggingSnapshotter
	// MachineConfigProvider provides GKE machine hardware configuration specs.
	MachineConfigProvider *machinetypes.MachineConfigProvider
}

// SetupInfrastructure initializes the standard set of test dependencies.
func SetupInfrastructure(ctx context.Context, t testing.TB) *TestInfrastructure {
	t.Helper()
	snapshotter, err := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(false)
	assert.NoError(t, err)
	mcp := machinetypes.NewMachineConfigProvider(nil)

	return &TestInfrastructure{
		Fakes:                 NewFakeSet(ctx, t),
		Snapshotter:           snapshotter,
		MachineConfigProvider: mcp,
	}
}

// FakeSet encapsulates all the fake clients and providers needed for
// interaction during an in-memory integration test.
type FakeSet struct {
	// KubeClient is the underlying Kubernetes fake clientset.
	KubeClient *fake.Clientset
	// K8s provides helper methods for tests.
	K8s *fake_k8s.Kubernetes
	// InformerFactory is the shared informer factory.
	InformerFactory informers.SharedInformerFactory

	UpdateInfosClient *updateinfosclient.Clientset
	GceService        *fakegce.GceClient
	GkeService        *gkeclientfake.Client
	fwHandle          *framework.Handle
	FlexAdvisorClient fakeflexadvisor.FakeFlexAdvisorClient
}

// NewFakeSet initializes a coordinated set of fakes.
func NewFakeSet(ctx context.Context, t testing.TB) *FakeSet {
	kubeClient := fake.NewClientset()
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	k8s := fake_k8s.NewKubernetes(kubeClient, informerFactory)
	gceService := fakegce.NewGceClient(t, k8s)

	fwHandle := newSimulatorHandle(ctx, t, informerFactory)

	return &FakeSet{
		KubeClient:        kubeClient,
		K8s:               k8s,
		InformerFactory:   informerFactory,
		UpdateInfosClient: updateinfosclient.NewSimpleClientset(),
		GceService:        gceService,
		GkeService:        gkeclientfake.NewClient(gceService, k8s),
		fwHandle:          fwHandle,
	}
}

// DefaultAutoscalingBuilder returns a production builder pre-configured with
// standard fakes and defaults for integration testing and an error if any.
func DefaultAutoscalingBuilder(
	ctx context.Context,
	t testing.TB,
	config *TestConfig,
	infra *TestInfrastructure,
) (*autoscaler.Builder, error) {

	fakeOptions := config.ResolveOptions()
	cluster := config.ResolveCluster()

	fakeCAVersion, err := version.FromString(config.CaVersion)
	if err != nil {
		panic(err)
	}
	// TODO(b/466598792): pass a fake evaluator.
	fakeExperimentsManager := experiments.NewManager(fakeCAVersion, experiments.NewNoopEvaluator())
	tracker := optstracking.NewOptionsTracker(fakeOptions, fakeExperimentsManager)

	fakeToken := &oauth2.Token{AccessToken: "fake-test-token"}
	fakeTokenSource := oauth2.StaticTokenSource(fakeToken)

	kubeConfig := &rest.Config{}
	fakeKubeClient := infra.Fakes.KubeClient
	fakeInformerFactory := informers.NewSharedInformerFactory(fakeKubeClient, 0)

	// TODO(b/456722077): follow dependency injection deprecating NewFakeProvisioningRequestClient usage.
	fakePR := provreqclient.ProvisioningRequestWrapperForTesting("my-namespace", "my-pr-name")
	fakePrClient := provreqtest.NewFakeClientForTB(ctx, t, fakePR)
	fakePrInjector := provreq.NewFakePodsInjector(fakePrClient, fakeclock.NewFakePassiveClock(time.Now()))
	fakePrCache := autoscaler.MustCreatePRCache(fakePrClient)
	// TODO(b/481602752): Pass a fake source after refactoring it to be an interface.
	machineConfigProvider := infra.MachineConfigProvider

	provConfigInformer := autoscaler.MustCreateProviderConfigInformer(fakeOptions, kubeConfig)
	httpClient := autoscaler.MustCreateHttpClient(fakeTokenSource)

	fakeGceClient := infra.Fakes.GceService.
		WithMachineConfigProvider(machineConfigProvider).
		WithZones(config.RegionToZones).
		WithDefaultDiskTypes().
		WithDefaultAccelerators().
		WithDefaultMachineTypes().
		WithTemplates(config.InstanceTemplates...).
		WithReservations(config.Reservations)

	if len(config.Reservations) > 0 {
		fakeGceClient.WithReservations(config.Reservations)
	}

	fakeGkeService, err := infra.Fakes.GkeService.
		WithProject(config.ProjectID).
		WithLocation(config.Location).
		WithCluster(&cluster)
	if err != nil {
		return nil, err
	}
	for _, dc := range config.DeviceClasses {
		infra.Fakes.KubeClient.ResourceV1().DeviceClasses().Create(
			context.Background(),
			&resourceapi.DeviceClass{ObjectMeta: metav1.ObjectMeta{Name: dc}},
			metav1.CreateOptions{},
		)
	}

	gkeClient := autoscaler.MustCreateGKEClient(fakeGkeService, nil, config.ProjectID, config.Location, fakeOptions, provConfigInformer, machineConfigProvider)

	fakeUpdateInfosClient := updateinfosclient.NewSimpleClientset()
	updateInfosInformerFactory := updateinfosinformers.NewSharedInformerFactory(fakeUpdateInfosClient, 0)
	updateInfoLister := updateInfosInformerFactory.Nodemanagement().V1alpha1().UpdateInfos().Lister()

	cccClient := cccfake.NewSimpleClientset()
	fakeNpcCrdClient := npc_client.NewClientFromClientsets(cccClient)

	gceCache := gce.NewGceCache()
	gkeCache := gke.NewGkeCache(gceCache, nodetemplate.NewCache())

	return autoscaler.NewBuilder(tracker).
		WithCAVersion(fakeCAVersion).
		WithKubeConfig(kubeConfig).
		WithKubeJSON(kubeConfig).
		WithKubeClient(fakeKubeClient).
		WithInformerFactory(fakeInformerFactory).
		WithNodeTemplateCache(nodetemplate.NewCache()).
		WithGCECache(gceCache).
		WithGKECache(gkeCache).
		WithUpdateInfoLister(updateInfoLister, updateInfosInformerFactory).
		WithNpcCrdLister(npc_lister.NewMockCrdListerWithLabel(config.NpcCrds, gkelabels.ComputeClassLabel)).
		WithProjectID(config.ProjectID).
		WithLocation(config.Location).
		WithTokenSource(fakeTokenSource).
		WithProvReqClient(fakePrClient).
		WithProvReqInjector(fakePrInjector).
		WithHttpClient(httpClient).
		WithGCEClient(fakeGceClient).
		WithGkeClient(gkeClient).
		WithResizableVmClient(resizablevms.NewNoOpClient()).
		WithConsumableReservationsClient(consumablereservations.NewNoOpClient()).
		WithProvReqCache(fakePrCache).
		WithNodeTemplateCache(nodetemplate.NewCache()).
		WithUpdateInfoLister(updateInfoLister, updateInfosInformerFactory).
		WithNpcCrdClient(fakeNpcCrdClient).
		WithNpcCrdLister(npc_lister.NewMockCrdListerWithLabel(config.NpcCrds, gkelabels.ComputeClassLabel)).
		WithRecommendLocationsClient(&fakerl.RecommendLocationsClient{}).
		WithAtomicResizeRequestClient(resizerequest.NewResizeRequestClient(fakeGceClient)).
		WithFlexResizeRequestClient(resizerequest.NewResizeRequestClient(fakeGceClient)).
		WithFlexAdvisorClient(&infra.Fakes.FlexAdvisorClient).
		WithProvReqManager(autoscaler.MustCreatePRManager(fakePrClient, httpClient, config.ProjectID, fakeOptions, fakePrCache, fakeExperimentsManager, fakeGceClient, gceCache)).
		WithGCECache(gceCache).
		WithGKECache(gkeCache).
		WithMachineConfigProvider(machineConfigProvider).
		WithProviderConfigInformer(provConfigInformer).
		// TODO(b/457181524): Refactor NodeSnowflakeWatcher to accept an injectable HTTP client.
		// Currently, it creates a real REST client, preventing the use of fake clients in tests.
		WithSnowflakeWatcher(nodesnowflake.NewNoOpWatcher()).
		WithNodeQuotaWatcher(nodequota.NewNoOpWatcher()).
		// TODO(b/457174984): Update PodObserver usage once StartPodObserver accepts cache.ListerWatcher.
		// This requires an OSS Kubernetes change. The current signature makes it hard to substitute with a fake informer/lister.
		WithPodObserver(&loop.UnschedulablePodObserver{}), nil
}

// SetupAutoscaler constructs a fully functional StaticAutoscaler using the provided configuration, returning the autoscaler instance and an error if any.
func SetupAutoscaler(t testing.TB, ctx context.Context, config *TestConfig, infra *TestInfrastructure, stopCh chan struct{}) (*core.StaticAutoscaler, error) {

	builder, err := DefaultAutoscalingBuilder(ctx, t, config, infra)
	if err != nil {
		return nil, err
	}

	a, _, err := builder.Build(
		ctx,
		infra.Snapshotter,
		stopCh,
		osconfig.OsReservedContent,
	)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// newSimulatorHandle creates a framework handle for testing.
func newSimulatorHandle(ctx context.Context, t testing.TB, informerFactory informers.SharedInformerFactory) *framework.Handle {
	t.Helper()
	defaultConfig, err := schedulerconfiglatest.Default()
	if err != nil {
		t.Fatalf("Failed to get default scheduler config: %v", err)
	}
	fwHandle, err := framework.NewHandle(ctx, informerFactory, defaultConfig, true, true)
	if err != nil {
		t.Fatalf("Failed to create framework handle: %v", err)
	}
	return fwHandle
}
