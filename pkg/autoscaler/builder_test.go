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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
	"k8s.io/autoscaler/cluster-autoscaler/loop"
	"k8s.io/autoscaler/cluster-autoscaler/processors/provreq"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/rest"
	fakeapi "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	nsf "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/nodesnowflake"
	fakeclock "k8s.io/utils/clock/testing"
)

const (
	project  = "test-project"
	location = "test-location"
)

func TestBuilder_Overrides(t *testing.T) {
	fakeOptions := internalopts.AutoscalingOptions{}
	fakeCAVersion, err := version.FromString("34.1.0")
	assert.NoError(t, err)
	// TODO(b/466598792): pass a fake evaluator.
	fakeExperimentManager := experiments.NewManager(fakeCAVersion, experiments.NewNoopEvaluator())
	tracker := optstracking.NewOptionsTracker(fakeOptions, fakeExperimentManager)

	fakeToken := &oauth2.Token{AccessToken: "fake-test-token"}
	fakeTokenSource := oauth2.StaticTokenSource(fakeToken)

	// TODO(b/456722077): introduce a fake client to wrap this logic.
	fakePR := provreqclient.ProvisioningRequestWrapperForTesting("my-namespace", "my-pr-name")
	fakePrClient := provreqclient.NewFakeProvisioningRequestClient(ctx.Background(), t, fakePR)
	fakePrInjector := provreq.NewFakePodsInjector(fakePrClient, fakeclock.NewFakePassiveClock(time.Now()))
	fakePrCache := MustCreatePRCache(fakePrClient)

	// TODO(b/481602752): Pass a fake source after refactoring it to be an interface.
	machineConfigProvider := machinetypes.NewMachineConfigProvider(nil)

	// Clients.
	kubeConfig := &rest.Config{}
	provConfigInformer := MustCreateProviderConfigInformer(fakeOptions, kubeConfig)
	httpClient := MustCreateHttpClient(fakeTokenSource)
	fakeGkeApiClient := fakeapi.NewClient(nil, nil).
		WithLocation(location).
		WithProject(project)

	gkeClient := MustCreateGKEClient(fakeGkeApiClient, nil, project, location, fakeOptions, provConfigInformer, machineConfigProvider)
	fakeInformerFactory := informers.NewSharedInformerFactory(nil, 0)

	// TODO(b/450181314): Refactor this test passing fake clients once are available.
	b := NewBuilder(tracker).
		WithCAVersion(fakeCAVersion).
		WithProjectID(project).
		WithLocation(location).
		WithTokenSource(fakeTokenSource).
		WithProvReqClient(fakePrClient).
		WithProvReqInjector(fakePrInjector).
		WithHttpClient(httpClient).
		WithProvReqCache(fakePrCache).
		WithMachineConfigProvider(machineConfigProvider).
		WithKubeConfig(kubeConfig).
		WithKubeJSON(kubeConfig).
		WithProviderConfigInformer(provConfigInformer).
		WithGkeClient(gkeClient).
		WithPodObserver(&loop.UnschedulablePodObserver{}).
		WithSnowflakeWatcher(nsf.NewNoOpWatcher()).
		WithInformerFactory(fakeInformerFactory)

	assert.Equal(t, fakeExperimentManager, b.optsTracker.ExperimentsManager())
	assert.Equal(t, &fakeCAVersion, b.caVersion)
	assert.Equal(t, "test-project", b.projectID)
	assert.Equal(t, "test-location", b.location)
	assert.Equal(t, fakeTokenSource, b.tokenSource)

	assert.Equal(t, fakePrClient, b.prClient, "ProvReqClient should match the injected pointer")
	assert.Equal(t, fakePrInjector, b.prInjector, "ProvReqInjector should match the injected pointer")
	assert.Equal(t, fakePrCache, b.prCache, "ProvReqCache should match the injected pointer")
	assert.Equal(t, machineConfigProvider, b.machineConfigProvider, "MachineConfigProvider should match the injected pointer")
	assert.Equal(t, provConfigInformer, b.provConfigInformer, "ProvConfigInformer client should match the injected pointer")
	assert.Equal(t, gkeClient, b.gkeClient, "GKE client should match the injected pointer")
	assert.Equal(t, fakeInformerFactory, b.informerFactory, "InformerFactory should match the injected pointer")

	assert.NotNil(t, b.podObserver, "PodObserver should be strictly injected")

	assert.NotNil(t, b.snowflakeWatcher, "SnowflakeWatcher should be strictly injected")
}
