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
	"fmt"
	"sync"
	"testing"
	"time"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	cccfake "github.com/googlecloudplatform/compute-class-api/client/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/core"
	"k8s.io/autoscaler/cluster-autoscaler/loop"
	"k8s.io/autoscaler/cluster-autoscaler/processors/provreq"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	fake_k8s "k8s.io/autoscaler/cluster-autoscaler/utils/fake"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	mccfake "k8s.io/gke-autoscaling/cluster-autoscaler/apis/machineconfig/client/clientset/versioned/fake"
	mcv1 "k8s.io/gke-autoscaling/cluster-autoscaler/apis/machineconfig/cloud.google.com/v1"
	osconfig "k8s.io/gke-autoscaling/cluster-autoscaler/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoscaler"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/bulkmig"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/consumablereservations"
	fakegce "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/fake"
	fakerl "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizablevms"
	resizerequest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest/fake"
	gkeclientfake "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	npc_client "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/client"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	fakeflexadvisor "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/gkedebuggingsnapshot"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/nodequota"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/nodesnowflake"
	prmanager "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/manager"
	provreqtest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/provreq"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/reactors"
	updateinfov1alpha1 "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/apis/nodemanagement.gke.io/v1alpha1"
	updateinfosclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/clientset/versioned/fake"
	updateinfosinformers "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/informers/externalversions"
	schedulerconfiglatest "k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	fakeclock "k8s.io/utils/clock/testing"
)

var metricsRegisterOnce sync.Once

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
// It registers all metrics (once) and resets them to prevent cross-test contamination.
func SetupInfrastructure(ctx context.Context, t testing.TB) *TestInfrastructure {
	t.Helper()
	metricsRegisterOnce.Do(metrics.RegisterAll)
	metrics.ResetAllForTest()

	snapshotter, err := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(false)
	assert.NoError(t, err)

	fakes := NewFakeSet(ctx, t)

	mcp := machinetypes.NewMachineConfigProvider(nil)

	return &TestInfrastructure{
		Fakes:                 fakes,
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

	UpdateInfosClient   *updateinfosclient.Clientset
	GceService          *fakegce.GceClient
	GkeService          *gkeclientfake.Client
	fwHandle            *framework.Handle
	FlexAdvisorClient   fakeflexadvisor.FakeFlexAdvisorClient
	CccClient           *cccfake.Clientset
	PRClientset         *provreqtest.FakeClientset
	ProvReqClient       *provreqclient.ProvisioningRequestClient
	ResizeRequestClient *resizerequest.ResizeRequestClient
	MccClient           *mccfake.Clientset
}

// NewFakeSet initializes a coordinated set of fakes.
func NewFakeSet(ctx context.Context, t testing.TB) *FakeSet {
	kubeClient := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
	)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	k8s := fake_k8s.NewKubernetes(kubeClient, informerFactory)
	gceService := fakegce.NewGceClient(t, k8s)
	rrClient := resizerequest.NewResizeRequestClient(gceService)
	mccClient := setupFakeMachineConfigClient()
	fwHandle := newSimulatorHandle(ctx, t, informerFactory)

	return &FakeSet{
		KubeClient:          kubeClient,
		K8s:                 k8s,
		InformerFactory:     informerFactory,
		UpdateInfosClient:   updateinfosclient.NewSimpleClientset(),
		GceService:          gceService,
		GkeService:          gkeclientfake.NewClient(gceService, k8s),
		fwHandle:            fwHandle,
		CccClient:           cccfake.NewSimpleClientset(),
		PRClientset:         provreqtest.NewFakeClientset(),
		ResizeRequestClient: rrClient,
		MccClient:           mccClient,
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
		t.Fatalf("failed to parse CA version: %v", err)
	}
	fakeExperimentsManager := experiments.NewManager(fakeCAVersion, config.ExperimentEvaluator)
	tracker := optstracking.NewOptionsTracker(fakeOptions, fakeExperimentsManager)

	fakeToken := &oauth2.Token{AccessToken: "fake-test-token"}
	fakeTokenSource := oauth2.StaticTokenSource(fakeToken)

	kubeConfig := &rest.Config{}
	fakeKubeClient := infra.Fakes.KubeClient
	reactors.SimulateInitialListStreamForWatchCalls(&fakeKubeClient.Fake, fakeKubeClient.Tracker(), "deviceclasses", &resourceapi.DeviceClass{})
	fakeInformerFactory := informers.NewSharedInformerFactory(fakeKubeClient, 0)

	// TODO(b/456722077): follow dependency injection deprecating NewFakeProvisioningRequestClient usage.
	fakePR := provreqclient.ProvisioningRequestWrapperForTesting("my-namespace", "my-pr-name")
	prs := append([]*provreqwrapper.ProvisioningRequest{fakePR}, config.ProvisioningRequests...)
	fakePrClient := provreqtest.NewFakeClientForTB(ctx, t, infra.Fakes.PRClientset, fakeKubeClient, prs...)
	reactors.SimulateInitialListStreamForWatchCalls(&infra.Fakes.PRClientset.Fake, infra.Fakes.PRClientset.Tracker(), "*", &prv1.ProvisioningRequest{})
	infra.Fakes.ProvReqClient = fakePrClient
	fakePrInjector := provreq.NewFakePodsInjector(fakePrClient, fakeclock.NewFakePassiveClock(time.Now()))
	fakePrCache := autoscaler.MustCreatePRCache(fakePrClient)

	mccClient := infra.Fakes.MccClient
	machineConfigProvider := autoscaler.CreateMachineConfigProvider(ctx, fakeOptions, mccClient, fakeExperimentsManager)
	infra.MachineConfigProvider = machineConfigProvider

	provConfigInformer := autoscaler.MustCreateProviderConfigInformer(fakeOptions, kubeConfig)
	httpClient := autoscaler.MustCreateHttpClient(fakeTokenSource)

	fakeGceClient := infra.Fakes.GceService.
		WithMachineConfigProvider(machineConfigProvider).
		WithZones(config.RegionToZones, config.RegionToAiZones).
		WithDefaultDiskTypes().
		WithDefaultAccelerators().
		WithDefaultMachineTypes().
		WithTemplates(config.InstanceTemplates...).
		WithReservations(config.Reservations)

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

	gkeClient := autoscaler.MustCreateGKEClient(fakeGkeService, nil, config.ProjectID, config.Location, fakeOptions, provConfigInformer, machineConfigProvider, fakeExperimentsManager)

	fakeUpdateInfosClient := updateinfosclient.NewSimpleClientset()
	reactors.SimulateInitialListStreamForWatchCalls(&fakeUpdateInfosClient.Fake, fakeUpdateInfosClient.Tracker(), "*", &updateinfov1alpha1.UpdateInfo{})
	updateInfosInformerFactory := updateinfosinformers.NewSharedInformerFactory(fakeUpdateInfosClient, 0)
	updateInfoLister := updateInfosInformerFactory.Nodemanagement().V1alpha1().UpdateInfos().Lister()

	cccClient := infra.Fakes.CccClient
	reactors.SimulateInitialListStreamForWatchCalls(&cccClient.Fake, cccClient.Tracker(), "*", &cccv1.ComputeClass{})
	for _, cc := range config.CccCrds {
		if _, err := cccClient.CloudV1().ComputeClasses().Create(ctx, cc, metav1.CreateOptions{}); err != nil {
			panic(fmt.Sprintf("failed to create compute class: %v", err))
		}
	}

	fakeNpcCrdClient := npc_client.NewClientFromClientsets(cccClient)

	npcLister, err := npc_lister.NewCccLister(ctx, fakeNpcCrdClient, tracker)
	if err != nil {
		return nil, err
	}

	gceCache := gce.NewGceCache()
	gkeCache := gke.NewGkeCache(gceCache, nodetemplate.NewCache())

	bulkMigClient, err := bulkmig.NewBulkMigClientBeta(httpClient, config.ProjectID, fakeOptions.UserAgent, fakeOptions.GceEndpoint, fakeGceClient, gceCache)
	if err != nil {
		t.Fatalf("failed to create bulkMigClient: %v", err)
	}
	prManager, err := prmanager.NewProvisioningRequestManager(
		fakePrClient,
		infra.Fakes.ResizeRequestClient,
		bulkMigClient,
		config.ProjectID,
		fakePrCache,
		fakeExperimentsManager,
	)
	if err != nil {
		t.Fatalf("failed to create ProvisioningRequestManager: %v", err)
	}

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
		WithNpcCrdLister(npcLister).
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
		WithNpcCrdClient(fakeNpcCrdClient).
		WithRecommendLocationsClient(&fakerl.RecommendLocationsClient{}).
		WithAtomicResizeRequestClient(infra.Fakes.ResizeRequestClient).
		WithFlexResizeRequestClient(infra.Fakes.ResizeRequestClient).
		WithFlexAdvisorClient(&infra.Fakes.FlexAdvisorClient).
		WithProvReqManager(prManager).
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
func SetupAutoscaler(ctx context.Context, t testing.TB, config *TestConfig, infra *TestInfrastructure) (*core.StaticAutoscaler, error) {

	builder, err := DefaultAutoscalingBuilder(ctx, t, config, infra)
	if err != nil {
		return nil, err
	}

	a, _, err := builder.Build(
		ctx,
		infra.Snapshotter,
		osconfig.OsReservedContent,
	)
	if err != nil {
		return nil, err
	}

	return a, nil
}

// MustSetupAutoscaler constructs a fully functional StaticAutoscaler, failing the test if an error occurs.
func MustSetupAutoscaler(ctx context.Context, t testing.TB, config *TestConfig, infra *TestInfrastructure) *core.StaticAutoscaler {
	t.Helper()
	a, err := SetupAutoscaler(ctx, t, config, infra)
	if err != nil {
		t.Fatalf("Failed to setup autoscaler: %v", err)
	}
	return a
}

// GetTestCluster retrieves the cluster object from the test environment.
func GetTestCluster(t *testing.T, infra *TestInfrastructure, config *TestConfig) *gke_api_beta.Cluster {
	t.Helper()
	clusterPath := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", config.ProjectID, config.Location, config.Cluster.Name)
	cluster, err := infra.Fakes.GkeService.GetCluster(clusterPath)
	assert.NoError(t, err)
	return cluster
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

func setupFakeMachineConfigClient() *mccfake.Clientset {
	mccClient := mccfake.NewSimpleClientset()
	reactors.SimulateInitialListStreamForWatchCalls(&mccClient.Fake, mccClient.Tracker(), "*", &mcv1.MachineConfig{})
	return mccClient
}
