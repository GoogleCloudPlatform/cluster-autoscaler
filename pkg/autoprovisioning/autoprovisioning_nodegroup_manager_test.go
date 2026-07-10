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
	ctx "context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	gce "google.golang.org/api/compute/v1"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	gce_cloudprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate/utils"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/autoscaler/cluster-autoscaler/processors/callbacks"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	base_backoff "k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/kubernetes/fake"
	kube_record "k8s.io/client-go/tools/record"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/klog/v2"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/machineselection"
	gke_backoff "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	computeclass "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	computeclass_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/gkedebuggingsnapshot"
	internal_customresources "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/customresources"
)

func TestProcessNodeGroupList(t *testing.T) {
	for testName, machineType := range map[string]string{
		"predefined machine type": "n1-standard-4",
		"custom machine type":     "n2-custom-48-184320",
		"n1 custom machine type":  "custom-48-184320",
	} {
		t.Run(testName, func(t *testing.T) {
			p1 := BuildTestPod("p1", 100, 100)
			n1 := BuildTestNode("ng1-xxx", 4000, 1000000)
			ni1 := framework.NewTestNodeInfo(n1)

			debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(true)

			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineTypes(machineType).
				WithAutoprovisioningLocations("us-central1-c").
				WithAllZones("us-central1-a", "us-central1-b", "us-central1-c").
				WithMachineTypesPerZone(map[string][]string{
					"us-central1-c": {machineType},
				}).
				WithAutoprovisioningEnabled(true).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			provider.AddNodeGroup("ng1", 1, 5, 3)

			processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
			processor.SetContext(&context.AutoscalingContext{CloudProvider: provider, DebuggingSnapshotter: debuggingSnapshotter})
			computeClassLister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{})
			em := experiments.NewMockManager()

			opts := AutoprovisioningNodeGroupManagerOptions{
				CloudProvider:                    provider,
				Backoff:                          gke_backoff.NewGkeBackoff(gke_backoff.Config{CustomResourceProcessor: processor, NpcLister: computeClassLister}),
				MaxAutoprovisionedNodeGroupCount: 1,
				Lister:                           computeClassLister,
				ExperimentsManager:               em,
				OptionsTracker:                   tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				ResourcePolicyPuller:             &placement.FakeResourcePolicyPullerProvider{},
			}
			manager := NewAutoprovisioningNodeGroupManager(opts)

			var err error
			dsLister, err := kube_util.NewTestDaemonSetLister([]*appsv1.DaemonSet{
				{ObjectMeta: v1.ObjectMeta{Name: "test-ds"}},
			})
			assert.NoError(t, err)

			context := &context.AutoscalingContext{
				CloudProvider:      provider,
				ProcessorCallbacks: callbacks.NewTestProcessorCallbacks(),
				ClusterSnapshot:    testsnapshot.NewTestSnapshotOrDie(t),
				AutoscalingKubeClients: context.AutoscalingKubeClients{
					ListerRegistry: kube_util.NewListerRegistry(nil, nil, nil, nil, dsLister, nil, nil, nil, nil),
				},
				DebuggingSnapshotter: debuggingSnapshotter,
			}
			nodeGroups := provider.NodeGroups()
			nodeInfos := map[string]*framework.NodeInfo{
				"ng1": ni1,
			}
			nodeGroups, nodeInfos, err = manager.Process(context, nodeGroups, nodeInfos, []*apiv1.Pod{p1})

			assert.NoError(t, err)
			assert.Equal(t, 2, len(nodeGroups))
			assert.Equal(t, 2, len(nodeInfos))

			nodeInfo := nodeInfos["autoprovisioned-"+machineType]
			assert.Equal(t, 1, len(nodeInfo.Pods()))
			assert.Contains(t, nodeInfo.Pods()[0].Pod.Name, "test-ds")
		})
	}
}

func TestProcessNodeGroupListTooMany(t *testing.T) {
	x1 := BuildTestNode("X1-cde", 4000, 1000000)
	xi1 := framework.NewTestNodeInfo(x1)
	p1 := BuildTestPod("p1", 100, 100)

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithMachineTypes("n2-standard-4", "n2-standard-8").
		WithAutoprovisioningEnabled(true).
		Build()
	provider.AddAutoprovisionedNodeGroup("autoprovisioned-X1", 0, 1000, 0, "n2-standard-4")

	debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(true)
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
	computeClassLister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{})
	em := experiments.NewMockManager()
	opts := AutoprovisioningNodeGroupManagerOptions{
		CloudProvider:                    provider,
		Backoff:                          gke_backoff.NewGkeBackoff(gke_backoff.Config{CustomResourceProcessor: processor, NpcLister: computeClassLister}),
		MaxAutoprovisionedNodeGroupCount: 1,
		Lister:                           computeClassLister,
		ExperimentsManager:               em,
		OptionsTracker:                   tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
	}
	manager := NewAutoprovisioningNodeGroupManager(opts)

	var err error
	dsLister, err := kube_util.NewTestDaemonSetLister([]*appsv1.DaemonSet{})
	assert.NoError(t, err)

	context := &context.AutoscalingContext{
		CloudProvider:      provider,
		ProcessorCallbacks: callbacks.NewTestProcessorCallbacks(),
		AutoscalingKubeClients: context.AutoscalingKubeClients{
			ListerRegistry: kube_util.NewListerRegistry(nil, nil, nil, nil, dsLister, nil, nil, nil, nil),
		},
		DebuggingSnapshotter: debuggingSnapshotter,
	}
	nodeGroups := provider.NodeGroups()
	nodeInfos := map[string]*framework.NodeInfo{"X1": xi1}
	nodeGroups, nodeInfos, err = manager.Process(context, nodeGroups, nodeInfos, []*apiv1.Pod{p1})

	assert.NoError(t, err)
	assert.Equal(t, 1, len(nodeGroups))
	assert.Equal(t, 1, len(nodeInfos))
}

func TestCreateNodeGroup(t *testing.T) {
	tests := []struct {
		name               string
		createNodeGroupErr error
		wantError          bool
	}{
		{
			name: "create node group",
		},
		{
			name:               "failed to create node group",
			createNodeGroupErr: fmt.Errorf("some error"),
			wantError:          true,
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("Sync/%s", tc.name), func(t *testing.T) {
			manager, provider, context := createNodeGroupTestContext(tc.createNodeGroupErr)
			nodeGroup, err := provider.NewNodeGroup("T1", nil, nil, nil, nil)
			assert.NoError(t, err)
			_, err = manager.CreateNodeGroup(context, nodeGroup)
			if tc.wantError {
				if err == nil {
					klog.Errorf("%s: Got no error, want error", tc.name)
				}
			} else {
				if err != nil {
					klog.Errorf("%s: Unexpected error %v", tc.name, err)
				}
				if len(provider.NodeGroups()) != 1 {
					klog.Errorf("%s: Unexpected number of node groups %d, want 1", tc.name, len(provider.NodeGroups()))
				}
			}
		})
		t.Run(fmt.Sprintf("Async/%s", tc.name), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				manager, provider, context := createNodeGroupTestContext(tc.createNodeGroupErr)
				nodeGroup, err := provider.NewNodeGroup("T1", nil, nil, nil, nil)
				assert.NoError(t, err)
				initializer := newAsyncNodeGroupInitializer(t)
				_, err = manager.CreateNodeGroupAsync(context, nodeGroup, initializer)
				synctest.Wait()
				asyncResult := initializer.AwaitResultOrFail()
				if tc.wantError {
					if err == nil {
						klog.Errorf("%s: Got no error, want error", tc.name)
					}
					if asyncResult.Error == nil {
						klog.Errorf("%s: Got no async error, want error", tc.name)
					}
				} else {
					if err != nil {
						klog.Errorf("%s: Unexpected error %v", tc.name, err)
					}
					if asyncResult.Error != nil {
						klog.Errorf("%s: Unexpected async error %v", tc.name, err)
					}
					if len(provider.NodeGroups()) != 1 {
						klog.Errorf("%s: Unexpected number of node groups %d, want 1", tc.name, len(provider.NodeGroups()))
					}
				}
			})
		})
	}
}

func createNodeGroupTestContext(createNodeGroupErr error) (*AutoprovisioningNodeGroupManager, *gke.TestAutoprovisioningCloudProvider, *context.AutoscalingContext) {
	fakeClient := &fake.Clientset{}
	fakeLogRecorder, _ := utils.NewStatusMapRecorder(fakeClient, "kube-system", kube_record.NewFakeRecorder(5), false, "test-configmap")
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithOnNodeGroupCreate(func(string) error { return createNodeGroupErr }).
		WithAutoprovisioningEnabled(true).
		Build()
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
	computeClassLister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{})
	em := experiments.NewMockManager()
	opts := AutoprovisioningNodeGroupManagerOptions{
		CloudProvider:      provider,
		Backoff:            gke_backoff.NewGkeBackoff(gke_backoff.Config{CustomResourceProcessor: processor, NpcLister: computeClassLister}),
		Lister:             computeClassLister,
		ExperimentsManager: em,
		OptionsTracker:     tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
	}
	manager := NewAutoprovisioningNodeGroupManager(opts)

	context := &context.AutoscalingContext{
		CloudProvider: provider,
		AutoscalingKubeClients: context.AutoscalingKubeClients{
			LogRecorder: fakeLogRecorder,
		},
		ProcessorCallbacks: callbacks.NewTestProcessorCallbacks(),
	}
	return manager, provider, context
}

func TestBackoffNodeGroup(t *testing.T) {
	p1 := BuildTestPod("p1", 100, 100)

	fakeClient := &fake.Clientset{}
	fakeLogRecorder, _ := utils.NewStatusMapRecorder(fakeClient, "kube-system", kube_record.NewFakeRecorder(5), false, "test-configmap")

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithOnNodeGroupCreate(func(string) error { return fmt.Errorf("some error") }).
		WithMachineTypes("n2-standard-4", "n2-standard-8").
		WithAutoprovisioningLocations("us-central1-c").
		WithMachineTypesPerZone(map[string][]string{
			"us-central1-c": {"n2-standard-4", "n2-standard-8"},
		}).
		WithAllZones("us-central1-a", "us-central1-b", "us-central1-c").
		WithAutoprovisioningEnabled(true).
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()

	// for sake of backoff test we replace backoff as the default one depends on fact that created node groups are of GkeMig type.
	testNodeGroupToBackoffKey := func(group cloudprovider.NodeGroup) string {
		return group.Id()
	}
	expBackoff := base_backoff.NewExponentialBackoff(gke_backoff.InitialNodeGroupBackoffDuration, gke_backoff.MaxNodeGroupBackoffDuration, gke_backoff.NodeGroupBackoffResetTimeout, testNodeGroupToBackoffKey)
	backoff := gke_backoff.NewCompositeBackoff([]base_backoff.Backoff{expBackoff}, nil)
	em := experiments.NewMockManager()
	opts := AutoprovisioningNodeGroupManagerOptions{
		CloudProvider:                    provider,
		Backoff:                          backoff,
		MaxAutoprovisionedNodeGroupCount: 10,
		Lister:                           computeclass_lister.NewMockCrdLister([]computeclass.CRD{}),
		ExperimentsManager:               em,
		OptionsTracker:                   tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
		ResourcePolicyPuller:             &placement.FakeResourcePolicyPullerProvider{},
	}
	manager := NewAutoprovisioningNodeGroupManager(opts)
	dsLister, err := kube_util.NewTestDaemonSetLister([]*appsv1.DaemonSet{})
	debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(true)
	assert.NoError(t, err)
	context := &context.AutoscalingContext{
		AutoscalingKubeClients: context.AutoscalingKubeClients{
			LogRecorder:    fakeLogRecorder,
			ListerRegistry: kube_util.NewListerRegistry(nil, nil, nil, nil, dsLister, nil, nil, nil, nil),
		},
		CloudProvider:        provider,
		ProcessorCallbacks:   callbacks.NewTestProcessorCallbacks(),
		DebuggingSnapshotter: debuggingSnapshotter,
	}

	nodeGroups, nodeInfos, err := manager.Process(context, []cloudprovider.NodeGroup{}, map[string]*framework.NodeInfo{}, []*apiv1.Pod{p1})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(nodeGroups))
	assert.Equal(t, 2, len(nodeInfos))
	_, err = manager.CreateNodeGroup(context, nodeGroups[0])
	assert.Error(t, err)
	nodeGroups, nodeInfos, err = manager.Process(context, []cloudprovider.NodeGroup{}, map[string]*framework.NodeInfo{}, []*apiv1.Pod{p1})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(nodeGroups))
	assert.Equal(t, 1, len(nodeInfos))
}

func TestAddDeletedNodeGroup(t *testing.T) {
	p1 := BuildTestPod("p1", 100, 100)

	fakeClient := &fake.Clientset{}
	fakeLogRecorder, _ := utils.NewStatusMapRecorder(fakeClient, "kube-system", kube_record.NewFakeRecorder(5), false, "test-configmap")

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithOnNodeGroupCreate(func(string) error { return nil }).
		WithMachineTypes("n2-standard-4", "n2-standard-8").
		WithAutoprovisioningLocations("us-central1-c").
		WithMachineTypesPerZone(map[string][]string{
			"us-central1-c": {"n2-standard-4", "n2-standard-8"},
		}).
		WithAllZones("us-central1-a", "us-central1-b", "us-central1-c").
		WithAutoprovisioningEnabled(true).
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
	debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(true)
	computeClassLister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{})
	em := experiments.NewMockManager()

	opts := AutoprovisioningNodeGroupManagerOptions{
		CloudProvider:                    provider,
		Backoff:                          gke_backoff.NewGkeBackoff(gke_backoff.Config{CustomResourceProcessor: processor, NpcLister: computeClassLister}),
		MaxAutoprovisionedNodeGroupCount: 10,
		Lister:                           computeClassLister,
		ExperimentsManager:               em,
		OptionsTracker:                   tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
		ResourcePolicyPuller:             &placement.FakeResourcePolicyPullerProvider{},
	}
	manager := NewAutoprovisioningNodeGroupManager(opts)

	dsLister, err := kube_util.NewTestDaemonSetLister([]*appsv1.DaemonSet{})
	assert.NoError(t, err)

	context := &context.AutoscalingContext{
		AutoscalingKubeClients: context.AutoscalingKubeClients{
			LogRecorder:    fakeLogRecorder,
			ListerRegistry: kube_util.NewListerRegistry(nil, nil, nil, nil, dsLister, nil, nil, nil, nil),
		},
		CloudProvider:        provider,
		ProcessorCallbacks:   callbacks.NewTestProcessorCallbacks(),
		DebuggingSnapshotter: debuggingSnapshotter,
	}

	nodeGroups, nodeInfos, err := manager.Process(context, []cloudprovider.NodeGroup{}, map[string]*framework.NodeInfo{}, []*apiv1.Pod{p1})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(nodeGroups))
	assert.Equal(t, 2, len(nodeInfos))
	_, err = manager.CreateNodeGroup(context, nodeGroups[0])
	assert.NoError(t, err)
	nodeGroups, nodeInfos, err = manager.Process(context, []cloudprovider.NodeGroup{}, map[string]*framework.NodeInfo{}, []*apiv1.Pod{p1})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(nodeGroups))
	assert.Equal(t, 2, len(nodeInfos))
}

func TestA100GPU(t *testing.T) {
	for _, gpuType := range []string{machinetypes.NvidiaTeslaA100.Name(), machinetypes.NvidiaA100_80gb.Name()} {
		t.Run(gpuType, func(t *testing.T) {
			p1 := BuildTestPod("p1", 100, 100)
			RequestGpuForPod(p1, 1)
			TolerateGpuForPod(p1)
			p1.Spec.NodeSelector = map[string]string{"cloud.google.com/gke-accelerator": gpuType}

			fakeClient := &fake.Clientset{}
			fakeLogRecorder, _ := utils.NewStatusMapRecorder(fakeClient, "kube-system", kube_record.NewFakeRecorder(5), false, "test-configmap")

			maxLimits := make(map[string]int64)
			maxLimits[gpuType] = 1000000
			allA2Machines := []string{}
			for machineType := range machinetypes.A2.AllMachineTypes(machinetypes.NoConstraints) {
				allA2Machines = append(allA2Machines, machineType)
			}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithOnNodeGroupCreate(func(string) error { return nil }).
				WithResourceLimiter(cloudprovider.NewResourceLimiter(nil, maxLimits)).
				WithAutoprovisioningLocations("us-central1-c").
				WithAllZones("us-central1-a", "us-central1-b", "us-central1-c").
				WithMachineTypesPerZone(map[string][]string{
					"us-central1-c": allA2Machines,
				}).
				WithAutoprovisioningEnabled(true).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
			processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
			debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(true)
			computeClassLister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{})
			em := experiments.NewMockManager()

			opts := AutoprovisioningNodeGroupManagerOptions{
				CloudProvider:                    provider,
				Backoff:                          gke_backoff.NewGkeBackoff(gke_backoff.Config{CustomResourceProcessor: processor, NpcLister: computeClassLister}),
				Lister:                           computeClassLister,
				MaxAutoprovisionedNodeGroupCount: 10,
				ExperimentsManager:               em,
				OptionsTracker:                   tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				ResourcePolicyPuller:             &placement.FakeResourcePolicyPullerProvider{},
			}
			manager := NewAutoprovisioningNodeGroupManager(opts)

			dsLister, err := kube_util.NewTestDaemonSetLister([]*appsv1.DaemonSet{})
			assert.NoError(t, err)

			context := &context.AutoscalingContext{
				AutoscalingKubeClients: context.AutoscalingKubeClients{
					LogRecorder:    fakeLogRecorder,
					ListerRegistry: kube_util.NewListerRegistry(nil, nil, nil, nil, dsLister, nil, nil, nil, nil),
				},
				CloudProvider:        provider,
				ProcessorCallbacks:   callbacks.NewTestProcessorCallbacks(),
				DebuggingSnapshotter: debuggingSnapshotter,
				ClusterSnapshot:      testsnapshot.NewTestSnapshotOrDie(t),
			}

			nodeGroups, nodeInfos, err := manager.Process(context, []cloudprovider.NodeGroup{}, map[string]*framework.NodeInfo{}, []*apiv1.Pod{p1})
			assert.NoError(t, err)
			wantMachineTypes := machinetypes.A2.AutoprovisionedMachineTypes(machinetypes.Constraints{CpuPlatform: machinetypes.AnyPlatform, GpuType: gpuType})
			assert.Equal(t, len(wantMachineTypes), len(nodeGroups))
			assert.Equal(t, len(wantMachineTypes), len(nodeInfos))
			for _, machineType := range wantMachineTypes {
				assert.Contains(t, nodeInfos, "autoprovisioned-"+machineType.Name)
			}
			for _, nodeGroup := range nodeGroups {
				assert.True(t, strings.Contains(nodeGroup.Id(), "a2-"))
				_, err = manager.CreateNodeGroup(context, nodeGroup)
				assert.NoError(t, err)
			}
		})
	}
}

func TestGpuPartitioning(t *testing.T) {
	for _, tc := range []struct {
		gpuType       string
		partitionSize string
	}{
		{
			gpuType:       machinetypes.NvidiaTeslaA100.Name(),
			partitionSize: "1g.5gb",
		},
		{
			gpuType:       machinetypes.NvidiaA100_80gb.Name(),
			partitionSize: "1g.10gb",
		},
	} {
		t.Run(tc.gpuType, func(t *testing.T) {
			p1 := BuildTestPod("p1", 100, 100)
			RequestGpuForPod(p1, 1)
			TolerateGpuForPod(p1)
			p1.Spec.NodeSelector = map[string]string{
				labels.GPULabel:              tc.gpuType,
				labels.GPUPartitionSizeLabel: tc.partitionSize,
			}
			p1.Spec.Tolerations = append(p1.Spec.Tolerations, apiv1.Toleration{
				Key:      labels.GPUPartitionSizeLabel,
				Operator: apiv1.TolerationOpEqual,
				Value:    tc.partitionSize,
				Effect:   apiv1.TaintEffectNoSchedule,
			})

			fakeClient := &fake.Clientset{}
			fakeLogRecorder, _ := utils.NewStatusMapRecorder(fakeClient, "kube-system", kube_record.NewFakeRecorder(5), false, "test-configmap")
			allA2Machines := []string{}
			for machineType := range machinetypes.A2.AllMachineTypes(machinetypes.NoConstraints) {
				allA2Machines = append(allA2Machines, machineType)
			}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithOnNodeGroupCreate(func(string) error { return nil }).
				WithResourceLimiter(cloudprovider.NewResourceLimiter(nil, map[string]int64{tc.gpuType: 100})).
				WithAutoprovisioningLocations("us-central1-c").
				WithMachineTypesPerZone(map[string][]string{
					"us-central1-c": allA2Machines,
				}).
				WithAllZones("us-central1-a", "us-central1-b", "us-central1-c").
				WithAutoprovisioningEnabled(true).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
			computeClassLister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{})
			em := experiments.NewMockManager()

			opts := AutoprovisioningNodeGroupManagerOptions{
				CloudProvider:                    provider,
				Backoff:                          gke_backoff.NewGkeBackoff(gke_backoff.Config{CustomResourceProcessor: processor, NpcLister: computeClassLister}),
				Lister:                           computeClassLister,
				MaxAutoprovisionedNodeGroupCount: 10,
				ExperimentsManager:               em,
				OptionsTracker:                   tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				ResourcePolicyPuller:             &placement.FakeResourcePolicyPullerProvider{},
			}
			manager := NewAutoprovisioningNodeGroupManager(opts)
			debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(true)

			dsLister, err := kube_util.NewTestDaemonSetLister([]*appsv1.DaemonSet{})
			assert.NoError(t, err)

			context := &context.AutoscalingContext{
				AutoscalingKubeClients: context.AutoscalingKubeClients{
					LogRecorder:    fakeLogRecorder,
					ListerRegistry: kube_util.NewListerRegistry(nil, nil, nil, nil, dsLister, nil, nil, nil, nil),
				},
				CloudProvider:        provider,
				ProcessorCallbacks:   callbacks.NewTestProcessorCallbacks(),
				DebuggingSnapshotter: debuggingSnapshotter,
				ClusterSnapshot:      testsnapshot.NewTestSnapshotOrDie(t),
			}
			processor.SetContext(context)

			nodeGroups, _, err := manager.Process(context, []cloudprovider.NodeGroup{}, map[string]*framework.NodeInfo{}, []*apiv1.Pod{p1})
			assert.NoError(t, err)
			wantMachineTypes := machinetypes.A2.AutoprovisionedMachineTypes(machinetypes.Constraints{CpuPlatform: machinetypes.AnyPlatform, GpuType: tc.gpuType})
			assert.Equal(t, len(wantMachineTypes), len(nodeGroups))
			for _, nodeGroup := range nodeGroups {
				mig := nodeGroup.(*gke.TestGkeNodeGroup)
				assert.Nil(t, mig.Taints())
			}
		})
	}
}

func TestTPUv4(t *testing.T) {
	for tn, tc := range map[string]struct {
		tpuType              string
		topology             string
		expectedMachineTypes map[string]machinetypes.MachineType
	}{
		"tpu-v4-lite-device": {
			tpuType:              labels.TpuV4LiteDeviceValue,
			expectedMachineTypes: machinetypes.CT4L.AutoprovisionedMachineTypes(machinetypes.Constraints{CpuPlatform: machinetypes.AnyPlatform, TpuType: labels.TpuV4LiteDeviceValue}),
		},
		"tpu-v4-podslice": {
			tpuType:              labels.TpuV4PodsliceValue,
			topology:             "2x2x4",
			expectedMachineTypes: machinetypes.CT4P.AutoprovisionedMachineTypes(machinetypes.Constraints{CpuPlatform: machinetypes.AnyPlatform, TpuType: labels.TpuV4PodsliceValue}),
		},
	} {
		t.Run(tn, func(t *testing.T) {
			p1 := buildTpuPod("p1", tc.tpuType, 4, tc.topology)
			fakeClient := &fake.Clientset{}
			fakeLogRecorder, _ := utils.NewStatusMapRecorder(fakeClient, "kube-system", kube_record.NewFakeRecorder(5), false, "test-configmap")
			allMachineTypes := []string{}
			for machineType := range machinetypes.CT4L.AllMachineTypes(machinetypes.NoConstraints) {
				allMachineTypes = append(allMachineTypes, machineType)
			}
			for machineType := range machinetypes.CT4P.AllMachineTypes(machinetypes.NoConstraints) {
				allMachineTypes = append(allMachineTypes, machineType)
			}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithOnNodeGroupCreate(func(string) error { return nil }).
				WithAutoprovisioningLocations("us-central1-c").
				WithAllZones("us-central1-a", "us-central1-b", "us-central1-c").
				WithMachineTypesPerZone(map[string][]string{
					"us-central1-c": allMachineTypes,
				}).
				WithResourceLimiter(cloudprovider.NewResourceLimiter(nil, map[string]int64{tc.tpuType: 32})).
				WithAutoprovisioningEnabled(true).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
			computeClassLister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{})
			em := experiments.NewMockManager()

			opts := AutoprovisioningNodeGroupManagerOptions{
				CloudProvider:                    provider,
				Backoff:                          gke_backoff.NewGkeBackoff(gke_backoff.Config{CustomResourceProcessor: processor, NpcLister: computeClassLister}),
				MaxAutoprovisionedNodeGroupCount: 10,
				Flags:                            AutoprovisioningNodeGroupManagerFlags{TpuAutoprovisioningEnabled: true},
				Lister:                           computeClassLister,
				ExperimentsManager:               em,
				OptionsTracker:                   tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				ResourcePolicyPuller:             &placement.FakeResourcePolicyPullerProvider{},
			}
			manager := NewAutoprovisioningNodeGroupManager(opts)
			debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(true)

			dsLister, err := kube_util.NewTestDaemonSetLister([]*appsv1.DaemonSet{})
			assert.NoError(t, err)

			context := &context.AutoscalingContext{
				AutoscalingKubeClients: context.AutoscalingKubeClients{
					LogRecorder:    fakeLogRecorder,
					ListerRegistry: kube_util.NewListerRegistry(nil, nil, nil, nil, dsLister, nil, nil, nil, nil),
				},
				CloudProvider:        provider,
				ProcessorCallbacks:   callbacks.NewTestProcessorCallbacks(),
				DebuggingSnapshotter: debuggingSnapshotter,
				ClusterSnapshot:      testsnapshot.NewTestSnapshotOrDie(t),
			}
			processor.SetContext(context)

			nodeGroups, nodeInfos, err := manager.Process(context, []cloudprovider.NodeGroup{}, map[string]*framework.NodeInfo{}, []*apiv1.Pod{p1})
			assert.NoError(t, err)
			assert.Equal(t, len(tc.expectedMachineTypes), len(nodeGroups))
			assert.Equal(t, len(tc.expectedMachineTypes), len(nodeInfos))
			for _, nodeGroup := range nodeGroups {
				mig := nodeGroup.(*gke.TestGkeNodeGroup)
				assert.Nil(t, mig.Taints())
			}
			for _, machineType := range tc.expectedMachineTypes {
				assert.Contains(t, nodeInfos, "autoprovisioned-"+machineType.Name)
			}
			for _, nodeGroup := range nodeGroups {
				assert.True(t, strings.Contains(nodeGroup.Id(), "ct4p-") || strings.Contains(nodeGroup.Id(), "ct4l-"))
				_, err = manager.CreateNodeGroup(context, nodeGroup)
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfidentialNodesEnabled(t *testing.T) {
	p1 := BuildTestPod("p1", 100, 100)

	fakeClient := &fake.Clientset{}
	fakeLogRecorder, _ := utils.NewStatusMapRecorder(fakeClient, "kube-system", kube_record.NewFakeRecorder(5), false, "test-configmap")

	allMachineTypes := []string{}
	for machineType := range machinetypes.N2D.AllMachineTypes(machinetypes.NoConstraints) {
		allMachineTypes = append(allMachineTypes, machineType)
	}

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithOnNodeGroupCreate(func(string) error { return nil }).
		WithAutoprovisioningLocations("us-central1-c").
		WithAllZones("us-central1-a", "us-central1-b", "us-central1-c").
		WithMachineTypesPerZone(map[string][]string{
			"us-central1-c": allMachineTypes,
		}).
		WithConfidentialInstanceType(labels.SEVConfidentialNodeTypeValue).
		WithAutoprovisioningEnabled(true).
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
	computeClassLister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{})
	em := experiments.NewMockManager()
	opts := AutoprovisioningNodeGroupManagerOptions{
		CloudProvider:                    provider,
		Backoff:                          gke_backoff.NewGkeBackoff(gke_backoff.Config{CustomResourceProcessor: processor, NpcLister: computeClassLister}),
		MaxAutoprovisionedNodeGroupCount: 10,
		Lister:                           computeClassLister,
		ExperimentsManager:               em,
		OptionsTracker:                   tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
		ResourcePolicyPuller:             &placement.FakeResourcePolicyPullerProvider{},
	}
	manager := NewAutoprovisioningNodeGroupManager(opts)
	debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(true)

	dsLister, err := kube_util.NewTestDaemonSetLister([]*appsv1.DaemonSet{})
	assert.NoError(t, err)

	context := &context.AutoscalingContext{
		AutoscalingKubeClients: context.AutoscalingKubeClients{
			LogRecorder:    fakeLogRecorder,
			ListerRegistry: kube_util.NewListerRegistry(nil, nil, nil, nil, dsLister, nil, nil, nil, nil),
		},
		CloudProvider:        provider,
		ProcessorCallbacks:   callbacks.NewTestProcessorCallbacks(),
		DebuggingSnapshotter: debuggingSnapshotter,
	}

	nodeGroups, nodeInfos, err := manager.Process(context, []cloudprovider.NodeGroup{}, map[string]*framework.NodeInfo{}, []*apiv1.Pod{p1})
	assert.NoError(t, err)
	wantMachineTypes := machinetypes.N2D.AutoprovisionedMachineTypes(machinetypes.NoConstraints)
	assert.Equal(t, len(wantMachineTypes), len(nodeGroups))
	assert.Equal(t, len(wantMachineTypes), len(nodeInfos))
	for _, machineType := range wantMachineTypes {
		assert.Contains(t, nodeInfos, "autoprovisioned-"+machineType.Name)
	}
	for _, ng := range nodeGroups {
		assert.Contains(t, ng.Id(), "n2d-")
		_, err = manager.CreateNodeGroup(context, ng)
		assert.NoError(t, err)
	}
}

func TestExtendedDurationPodAndPPVM(t *testing.T) {
	fakeClient := &fake.Clientset{}
	fakeLogRecorder, _ := utils.NewStatusMapRecorder(fakeClient, "kube-system", kube_record.NewFakeRecorder(5), false, "test-configmap")
	allResizableMachineTypes := []string{
		"ek-standard-2",
		"e4a-standard-8",
	}

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithOnNodeGroupCreate(func(string) error { return nil }).
		WithMachineTypes("e2-standard-2", "ek-standard-2", "e4a-standard-8").
		WithAutoprovisioningLocations("us-central1-c").
		WithMachineTypesPerZone(map[string][]string{
			"us-central1-c": {"e2-standard-2"},
		}).
		WithAllZones("us-central1-a", "us-central1-b", "us-central1-c").
		WithAutopilotEnabled(true).
		WithAutoprovisioningEnabled(true).
		WithEkEdpEnabled(true).
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
	computeClassLister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{})
	em := experiments.NewMockManager()
	opts := AutoprovisioningNodeGroupManagerOptions{
		CloudProvider:                    provider,
		Backoff:                          gke_backoff.NewGkeBackoff(gke_backoff.Config{CustomResourceProcessor: processor, NpcLister: computeClassLister}),
		MaxAutoprovisionedNodeGroupCount: 10,
		Flags:                            AutoprovisioningNodeGroupManagerFlags{},
		PodLister:                        kube_util.NewTestPodLister([]*apiv1.Pod{}),
		Lister:                           computeClassLister,
		ExperimentsManager:               em,
		ResizableMachineTypesProvider:    config.NewSimpleStringSetProvider(allResizableMachineTypes),
		OptionsTracker:                   tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
		ResourcePolicyPuller:             &placement.FakeResourcePolicyPullerProvider{},
	}
	manager := NewAutoprovisioningNodeGroupManager(opts)
	debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(true)

	dsLister, err := kube_util.NewTestDaemonSetLister([]*appsv1.DaemonSet{})
	assert.NoError(t, err)

	ctx := &context.AutoscalingContext{
		AutoscalingKubeClients: context.AutoscalingKubeClients{
			LogRecorder:    fakeLogRecorder,
			ListerRegistry: kube_util.NewListerRegistry(nil, nil, nil, nil, dsLister, nil, nil, nil, nil),
		},
		CloudProvider:        provider,
		ProcessorCallbacks:   callbacks.NewTestProcessorCallbacks(),
		DebuggingSnapshotter: debuggingSnapshotter,
	}

	for tn, tc := range map[string]struct {
		pod                         *apiv1.Pod
		expectedNodeGroups          int
		wantNodeGroupsWithLabels    []map[string]string
		wantNodeGroupsWithoutLabels [][]string
	}{
		"extended duration pods get EDP and non EDP nodegroups": {
			pod:                addExtendedDurationPodLabel(BuildTestPod("p1", 100, 100), "100m"),
			expectedNodeGroups: 6,
			wantNodeGroupsWithLabels: []map[string]string{
				{labels.ExtendedDurationPodsLabel: "100m"},
				{labels.ExtendedDurationPodsLabel: "X"},
			},
			wantNodeGroupsWithoutLabels: [][]string{
				{labels.ExtendedDurationPodsLabel},
				{labels.PodPerVMSizeLabel, labels.PodCapacityLabel},
			},
		},
		"pod-per-vms only get ppvm node group": {
			pod:                addPodPerVMInfo(BuildTestPod("p1", 100, 100), "100m"),
			expectedNodeGroups: 3,
			wantNodeGroupsWithLabels: []map[string]string{
				{labels.PodPerVMSizeLabel: "100m", labels.PodCapacityLabel: "1"},
			},
			wantNodeGroupsWithoutLabels: [][]string{
				{labels.ExtendedDurationPodsLabel},
			},
		},
		"pod-per-vms with EDP get ppvm + (ppvm edp) nodegroups": {
			pod:                addExtendedDurationPodLabel(addPodPerVMInfo(BuildTestPod("p1", 100, 100), "100m"), "100m"),
			expectedNodeGroups: 6,
			wantNodeGroupsWithLabels: []map[string]string{
				{labels.ExtendedDurationPodsLabel: "100m", labels.PodPerVMSizeLabel: "100m", labels.PodCapacityLabel: "1"},
				{labels.ExtendedDurationPodsLabel: "X", labels.PodPerVMSizeLabel: "100m", labels.PodCapacityLabel: "1"},
				{labels.PodPerVMSizeLabel: "100m", labels.PodCapacityLabel: "1"},
			},
			wantNodeGroupsWithoutLabels: [][]string{
				{labels.ExtendedDurationPodsLabel},
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			nodeGroups, _, err := manager.Process(ctx, []cloudprovider.NodeGroup{}, map[string]*framework.NodeInfo{}, []*apiv1.Pod{tc.pod})
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedNodeGroups, len(nodeGroups))
			foundNodeGroupsWithLabel := make(map[int]bool)
			foundNodeGroupsWithoutLabel := make(map[int]bool)
			for _, ng := range nodeGroups {
				templateNode, err := ng.TemplateNodeInfo()
				assert.NoError(t, err)
				assert.NotNil(t, templateNode.Node())
				// Check if this node group satisfies one of the wanted node groups with labels.
				for i, wantLabels := range tc.wantNodeGroupsWithLabels {
					foundAll := true
					for key, value := range wantLabels {
						foundValue, foundLabel := templateNode.Node().Labels[key]
						if !foundLabel || foundValue != value {
							foundAll = false
							break
						}
					}
					if foundAll {
						foundNodeGroupsWithLabel[i] = true
					}
				}
				// Check if this node group satisfies one of wanted node groups without labels.
				for i, wantWithoutLabels := range tc.wantNodeGroupsWithoutLabels {
					foundAny := false
					for _, label := range wantWithoutLabels {
						_, foundLabel := templateNode.Node().Labels[label]
						if foundLabel {
							foundAny = true
							break
						}
					}
					if !foundAny {
						foundNodeGroupsWithoutLabel[i] = true
					}
				}
				_, err = manager.CreateNodeGroup(ctx, ng)
				assert.NoError(t, err)
			}
			for i, wantLabels := range tc.wantNodeGroupsWithLabels {
				assert.Equal(t, true, foundNodeGroupsWithLabel[i], "Expected node group with labels %v", wantLabels)
			}
			for i, wantWithoutLabels := range tc.wantNodeGroupsWithoutLabels {
				assert.Equal(t, true, foundNodeGroupsWithoutLabel[i], "Expected node group without labels %v", wantWithoutLabels)
			}
		})
	}
}

func TestCSNPod(t *testing.T) {
	fakeClient := &fake.Clientset{}
	fakeLogRecorder, _ := utils.NewStatusMapRecorder(fakeClient, "kube-system", kube_record.NewFakeRecorder(5), false, "test-configmap")

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithOnNodeGroupCreate(func(string) error { return nil }).
		WithMachineTypes("e2-standard-2", "ek-standard-2").
		WithAutoprovisioningLocations("us-central1-c").
		WithMachineTypesPerZone(map[string][]string{
			"us-central1-c": {"e2-standard-2"},
		}).
		WithAllZones("us-central1-a", "us-central1-b", "us-central1-c").
		WithAutoprovisioningEnabled(true).
		WithEkEdpEnabled(true).
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
	computeClassLister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{})
	for tn, tc := range map[string]struct {
		pod                      *apiv1.Pod
		experimentDisabled       bool
		wantCSNLabelAndSoftTaint bool
	}{
		"CSN pod": {
			pod:                      createCSNPod("csn-pod", 100, 100),
			wantCSNLabelAndSoftTaint: true,
		},
		"CSN pod - experiment disabled": {
			pod:                      createCSNPod("csn-pod", 100, 100),
			experimentDisabled:       true,
			wantCSNLabelAndSoftTaint: false,
		},
		"non CSN pod": {
			pod:                      test.BuildTestPod("normal-pod", 100, 100),
			wantCSNLabelAndSoftTaint: false,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			var enabledFlags []string
			if !tc.experimentDisabled {
				enabledFlags = append(enabledFlags, experiments.ColdStandbyNodesInternalMinCAVersionFlag)
			}
			gm := experiments.NewMockManager(enabledFlags...)

			opts := AutoprovisioningNodeGroupManagerOptions{
				CloudProvider:                    provider,
				Backoff:                          gke_backoff.NewGkeBackoff(gke_backoff.Config{CustomResourceProcessor: processor, NpcLister: computeClassLister}),
				MaxAutoprovisionedNodeGroupCount: 10,
				Flags:                            AutoprovisioningNodeGroupManagerFlags{},
				PodLister:                        kube_util.NewTestPodLister([]*apiv1.Pod{}),
				Lister:                           computeClassLister,
				ExperimentsManager:               gm,
				ResizableMachineTypesProvider:    config.NewSimpleStringSetProvider(nil),
				OptionsTracker:                   tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, gm),
				ResourcePolicyPuller:             &placement.FakeResourcePolicyPullerProvider{},
			}
			manager := NewAutoprovisioningNodeGroupManager(opts)
			debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(true)

			dsLister, err := kube_util.NewTestDaemonSetLister([]*appsv1.DaemonSet{})
			assert.NoError(t, err)

			ctx := &context.AutoscalingContext{
				AutoscalingKubeClients: context.AutoscalingKubeClients{
					LogRecorder:    fakeLogRecorder,
					ListerRegistry: kube_util.NewListerRegistry(nil, nil, nil, nil, dsLister, nil, nil, nil, nil),
				},
				CloudProvider:        provider,
				ProcessorCallbacks:   callbacks.NewTestProcessorCallbacks(),
				DebuggingSnapshotter: debuggingSnapshotter,
			}
			nodeGroups, _, err := manager.Process(ctx, []cloudprovider.NodeGroup{}, map[string]*framework.NodeInfo{}, []*apiv1.Pod{tc.pod})
			assert.NoError(t, err)

			for _, ng := range nodeGroups {
				templateNodeInfo, err := ng.TemplateNodeInfo()
				assert.NoError(t, err)
				node := templateNodeInfo.Node()
				assert.NotNil(t, node, "Node shouldn't be nil")

				foundTaint := slices.ContainsFunc(node.Spec.Taints, func(t apiv1.Taint) bool {
					return t == csn.SoftWorkloadSeparationTaint
				})

				if tc.wantCSNLabelAndSoftTaint {
					assert.Equal(t, csn.SoftWorkloadSeparationValue, node.Labels[csn.SoftWorkloadSeparationKey])
					assert.True(t, foundTaint, "Expected to find taint %v")
				} else {
					assert.NotEqual(t, csn.SoftWorkloadSeparationValue, node.Labels[csn.SoftWorkloadSeparationKey])
					assert.False(t, foundTaint, "Did not expect to find taint %v")
				}

				_, err = manager.CreateNodeGroup(ctx, ng)
				assert.NoError(t, err)
			}
		})
	}
}

func TestInjectedLocations(t *testing.T) {
	for _, tc := range []struct {
		desc                       string
		enableUserAnyZoneSelection bool
		zoneAgnosticShard          bool
		podOpts                    []func(*apiv1.Pod)
		reservations               []*gce.Reservation
		apZones                    []string
		allZones                   []string
		wantInjectedZones          []string
	}{
		{
			desc:                       "injects one AP location a for zone-agnostic shard",
			enableUserAnyZoneSelection: true,
			zoneAgnosticShard:          true,
			apZones:                    []string{"us-central1-a", "us-central1-b"},
			// use autoprovisioning locations exclusive with allZones
			// to make sure that injected zone is selected only from autoprovisioning locations.
			allZones:          []string{"us-central1-x", "us-central1-y", "us-central1-z"},
			wantInjectedZones: []string{"us-central1-a"},
		},
		{
			desc:                       "injects AP locations for zone-agnostic shard with reservations",
			enableUserAnyZoneSelection: true,
			zoneAgnosticShard:          true,
			reservations: []*gce.Reservation{{
				SelfLink: "https://www.googleapis.com/compute/v1/projects/proj/zones/us-central1-a/reservations/my-res",
			}},
			apZones:           []string{"us-central1-a", "us-central1-b"},
			allZones:          []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			wantInjectedZones: []string{"us-central1-a", "us-central1-b"},
		},
		{
			desc:                       "injects AP locations for non-zone-agnostic shard with AI Zones disabled",
			enableUserAnyZoneSelection: false,
			zoneAgnosticShard:          false,
			apZones:                    []string{"us-central1-a", "us-central1-b"},
			allZones:                   []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			wantInjectedZones:          []string{"us-central1-a", "us-central1-b"},
		},
		{
			desc:                       "injects specified AI zones outside AP locations",
			enableUserAnyZoneSelection: true,
			zoneAgnosticShard:          false,
			podOpts:                    []func(*apiv1.Pod){withNodeAffinity(apiv1.LabelTopologyZone, "us-central1-aib")},
			apZones:                    []string{"us-central1-a"},
			allZones:                   []string{"us-central1-a", "us-central1-aib"},
			wantInjectedZones:          []string{"us-central1-aib"},
		},
		// TODO(b/431688185): add unit tests for other AI Zone cases, examples:
		// * AI zones disabled and pod has a node selector for zone - we may expect that NAP injects
		//   no options, but actually the current implementation defaults to injecting autoprovisioning zones.
		//   That's because SpecifiedZonesGenerator ignores apiv1.LabelTopologyZone node selector
		//   and only looks at "ccc-specified-zones" label.
	} {
		t.Run(tc.desc, func(t *testing.T) {
			p := test.BuildTestPod("test-pod", 100, 100, tc.podOpts...)
			fakeLogRecorder, _ := utils.NewStatusMapRecorder(&fake.Clientset{}, "kube-system", kube_record.NewFakeRecorder(5), false, "test-configmap")

			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineTypes("n2-standard-4").
				WithOnNodeGroupCreate(func(string) error { return nil }).
				WithAutoprovisioningLocations(tc.apZones...).
				WithAllZones(tc.allZones...).
				WithMachineTypesPerZone(map[string][]string{
					"us-central1-a":   {"n2-standard-4"},
					"us-central1-b":   {"n2-standard-4"},
					"us-central1-aib": {"n2-standard-4"},
				}).
				WithAutoprovisioningEnabled(true).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
			computeClassLister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{})

			mGceClient := gceclient.BuildAutoscalingInternalGceClientMock().
				WithFetchZones(func(region string) ([]string, error) { return []string{"us-central1-a"}, nil })
			fakeRP, err := gceclient.NewReservationsPuller(mGceClient, nil, nil, "proj", false, "us-central1")
			assert.NoError(t, err)
			fakeRP.SetReservations(tc.reservations)
			em := experiments.NewMockManager()

			opts := AutoprovisioningNodeGroupManagerOptions{
				CloudProvider:                    provider,
				Backoff:                          gke_backoff.NewGkeBackoff(gke_backoff.Config{CustomResourceProcessor: processor, NpcLister: computeClassLister}),
				MaxAutoprovisionedNodeGroupCount: 10,
				Flags: AutoprovisioningNodeGroupManagerFlags{
					EnableUserAnyZoneSelection: tc.enableUserAnyZoneSelection,
				},
				Lister:               computeClassLister,
				ExperimentsManager:   em,
				ReservationsPuller:   fakeRP,
				OptionsTracker:       tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				ResourcePolicyPuller: &placement.FakeResourcePolicyPullerProvider{},
			}
			manager := NewAutoprovisioningNodeGroupManager(opts)
			manager.randInt = func(_ int) int { return 0 }
			debuggingSnapshotter, _ := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(true)

			dsLister, err := kube_util.NewTestDaemonSetLister([]*appsv1.DaemonSet{})
			assert.NoError(t, err)

			pc := callbacks.NewTestProcessorCallbacks()
			pc.SetExtraValue("unschedulable-pods-zone-agnostic.podsharding.gke-autoscaler", tc.zoneAgnosticShard)
			context := &context.AutoscalingContext{
				AutoscalingKubeClients: context.AutoscalingKubeClients{
					LogRecorder:    fakeLogRecorder,
					ListerRegistry: kube_util.NewListerRegistry(nil, nil, nil, nil, dsLister, nil, nil, nil, nil),
				},
				CloudProvider:        provider,
				ProcessorCallbacks:   pc,
				DebuggingSnapshotter: debuggingSnapshotter,
				ClusterSnapshot:      testsnapshot.NewTestSnapshotOrDie(t),
			}
			processor.SetContext(context)

			nodeGroups, _, err := manager.Process(context, []cloudprovider.NodeGroup{}, map[string]*framework.NodeInfo{}, []*apiv1.Pod{p})
			assert.NoError(t, err)
			assert.ElementsMatch(t, tc.wantInjectedZones, getInjectedZones(t, nodeGroups))
		})
	}
}

func withNodeAffinity(key string, values ...string) func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		if pod.Spec.Affinity == nil {
			pod.Spec.Affinity = &apiv1.Affinity{}
		}
		if pod.Spec.Affinity.NodeAffinity == nil {
			pod.Spec.Affinity.NodeAffinity = &apiv1.NodeAffinity{}
		}
		if pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
			pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &apiv1.NodeSelector{}
		}
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(
			pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms,
			apiv1.NodeSelectorTerm{
				MatchExpressions: []apiv1.NodeSelectorRequirement{
					{
						Key:      key,
						Operator: apiv1.NodeSelectorOpIn,
						Values:   values,
					},
				},
			},
		)
	}
}

func getInjectedZones(t *testing.T, nodeGroups []cloudprovider.NodeGroup) []string {
	zones := []string{}
	for _, ng := range nodeGroups {
		tng, ok := ng.(*gke.TestGkeNodeGroup)
		if !ok {
			t.Fatalf("expected TestGkeNodeGroup, got %+v", ng)
		}
		zones = append(zones, tng.Zone())
	}
	return zones
}

// Helper struct since Go doesn't like nested anonymous structs.
type nodeGroupDef struct {
	nodePoolName    string
	id              string
	size            int
	autoprovisioned bool
	exist           bool
	isAutopilot     bool
	isStable        bool
}

func TestRemoveUnneededNodeGroups(t *testing.T) {
	// Node autoprovisioning disabled for one of the migs
	autoprovisioningEligibility := &gke.MockAutoprovisioningEligibility{}
	autoprovisioningEligibility.On("IsNodeAutoprovisioningEnabled").Return(true)
	autoprovisioningEligibility.On("UseAutoprovisioningFeaturesForNodeGroup", mock.MatchedBy(func(nodeGroup cloudprovider.NodeGroup) bool {
		return nodeGroup.Id() == "no-autoprovisioning"
	})).Return(false)
	autoprovisioningEligibility.On("UseAutoprovisioningFeaturesForNodeGroup", mock.Anything).Return(true)

	testCases := []struct {
		desc                             string
		nodeGroups                       []nodeGroupDef
		nodeGroupsBlockedByServerError   []nodeGroupDef
		nodeGroupsBlockedByNotFoundError []nodeGroupDef
		deleteError                      error
		allowedDeleted                   []string
		expectedDelete                   bool
	}{
		{
			desc: "Empty node pool with autoprovisioning disabled",
			nodeGroups: []nodeGroupDef{
				{nodePoolName: "no-autoprovisioning", id: "no-autoprovisioning", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
			},
			allowedDeleted: []string{},
			expectedDelete: false,
		},
		{
			desc: "Delete only one empty single-zone node pool",
			nodeGroups: []nodeGroupDef{
				{nodePoolName: "np1", id: "ng1", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
				{nodePoolName: "np2", id: "ng2", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
			},
			allowedDeleted: []string{"ng1", "ng2"},
			expectedDelete: true,
		},
		{
			desc: "One of the empty node pool with autoprovisioning disabled",
			nodeGroups: []nodeGroupDef{
				{nodePoolName: "no-autoprovisioning", id: "no-autoprovisioning", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
				{nodePoolName: "np1", id: "ng1", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
				{nodePoolName: "np2", id: "ng2", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
			},
			allowedDeleted: []string{"ng1", "ng2"},
			expectedDelete: true,
		},
		{
			desc: "Delete only one empty multi-zone node pool",
			nodeGroups: []nodeGroupDef{
				{nodePoolName: "np1", id: "ng1", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
				{nodePoolName: "np1", id: "ng2", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
				{nodePoolName: "np2", id: "ng2", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
				{nodePoolName: "np2", id: "ng2", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
			},
			allowedDeleted: []string{"ng1", "ng2", "ng3", "ng4"},
			expectedDelete: true,
		},
		{
			desc: "Not delete non-autoprovisioned node pool",
			nodeGroups: []nodeGroupDef{
				{nodePoolName: "np1", id: "ng1", size: 0, autoprovisioned: false, exist: true, isAutopilot: false, isStable: true},
			},
		},
		{
			desc: "Not delete node pool with 0 size and ongoing operations",
			nodeGroups: []nodeGroupDef{
				{nodePoolName: "np1", id: "ng1", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: false},
			},
		},
		{
			desc: "Not delete node pool with non-zero size and no nodes",
			nodeGroups: []nodeGroupDef{
				{nodePoolName: "np1", id: "ng1", size: 1, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
			},
		},
		{
			desc: "Delete blocked node pool with zero size and no nodes",
			nodeGroupsBlockedByServerError: []nodeGroupDef{
				{nodePoolName: "np1", id: "ng1", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
			},
			expectedDelete: true,
			allowedDeleted: []string{"ng1"},
		},
		{
			desc: "Delete node-pool with missing node groups",
			nodeGroupsBlockedByNotFoundError: []nodeGroupDef{
				{nodePoolName: "np1", id: "ng1", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
				{nodePoolName: "np1", id: "ng2", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
			},
			expectedDelete: true,
			allowedDeleted: []string{"ng1", "ng2"},
		},
		{
			desc: "Not delete node-pool with node group blocked by server error containing node",
			nodeGroupsBlockedByServerError: []nodeGroupDef{
				{nodePoolName: "np1", id: "ng1", size: 1, autoprovisioned: true, exist: true, isAutopilot: false, isStable: false},
			},
			nodeGroups: []nodeGroupDef{
				{nodePoolName: "np1", id: "ng2", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
			},
		},
		{
			desc: "Not delete node-pool with one healthy node group and one missing node group",
			nodeGroupsBlockedByNotFoundError: []nodeGroupDef{
				{nodePoolName: "np1", id: "ng1", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
			},
			nodeGroups: []nodeGroupDef{
				{nodePoolName: "np1", id: "ng2", size: 1, autoprovisioned: true, exist: true, isAutopilot: false, isStable: false},
			},
		},
		{
			desc: "Not delete node-pool with one missing non-autoprovisioned node group",
			nodeGroupsBlockedByNotFoundError: []nodeGroupDef{
				{nodePoolName: "np1", id: "ng1", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
				{nodePoolName: "np1", id: "ng2", size: 0, autoprovisioned: false, exist: true, isAutopilot: false, isStable: true},
			},
			expectedDelete: false,
		},
		{
			desc: "Not delete multi-zone node pool with only one empty MIG",
			nodeGroups: []nodeGroupDef{
				{nodePoolName: "np1", id: "ng1", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
				{nodePoolName: "np1", id: "ng2", size: 1, autoprovisioned: true, exist: true, isAutopilot: false, isStable: false},
			},
		},
		{
			desc: "Return original error on failed delete",
			nodeGroups: []nodeGroupDef{
				{nodePoolName: "np1", id: "ng1", size: 0, autoprovisioned: true, exist: true, isAutopilot: false, isStable: true},
			},
			allowedDeleted: []string{"ng1"},
			deleteError:    fmt.Errorf("cloud provider delete failed"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			// Set up test cloud provider.
			deleted := []string{}
			onDelete := func(id string) error {
				if tc.deleteError == nil {
					deleted = append(deleted, id)
				}
				return tc.deleteError
			}

			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithOnNodeGroupDelete(onDelete).
				WithAutoprovisioningEligibility(autoprovisioningEligibility).
				Build()
			// Set up each node group.
			for _, group := range tc.nodeGroups {
				provider.AddAutoprovisionedGkeNodeGroup(group.nodePoolName, group.id, group.size, group.exist, group.autoprovisioned, "", group.isAutopilot, group.isStable)
			}
			for _, group := range tc.nodeGroupsBlockedByServerError {
				provider.AddBlockedNodeGroupBlockedByServerError(group.nodePoolName, group.id, group.size, group.exist, group.autoprovisioned, "", group.isAutopilot, group.isStable)
			}
			for _, group := range tc.nodeGroupsBlockedByNotFoundError {
				provider.AddNodeGroupBlockedByNotFoundError(group.nodePoolName, group.id, group.size, group.exist, group.autoprovisioned, "", group.isAutopilot, group.isStable)
			}
			// Set up autoprovisioning manager.
			processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
			processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
			computeClassLister := computeclass_lister.NewMockCrdLister([]computeclass.CRD{})
			em := experiments.NewMockManager()
			opts := AutoprovisioningNodeGroupManagerOptions{
				CloudProvider:        provider,
				Backoff:              gke_backoff.NewGkeBackoff(gke_backoff.Config{CustomResourceProcessor: processor, NpcLister: computeClassLister}),
				Lister:               computeClassLister,
				ExperimentsManager:   em,
				OptionsTracker:       tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
				ResourcePolicyPuller: &placement.FakeResourcePolicyPullerProvider{},
			}
			manager := NewAutoprovisioningNodeGroupManager(opts)
			// Set up fake context.
			fakeClient := &fake.Clientset{}
			fakeRecorder := kube_util.CreateEventRecorder(ctx.TODO(), fakeClient, false)
			fakeLogRecorder, _ := utils.NewStatusMapRecorder(fakeClient, "kube-system", fakeRecorder, false, "test-configmap")
			context := &context.AutoscalingContext{
				AutoscalingKubeClients: context.AutoscalingKubeClients{
					LogRecorder: fakeLogRecorder,
				},
				ProcessorCallbacks: callbacks.NewTestProcessorCallbacks(),
			}

			removedNodeGroups, err := manager.RemoveUnneededNodeGroups(context)

			removedNodeGroupIds := make([]string, len(removedNodeGroups))
			for i, nodeGroup := range removedNodeGroups {
				removedNodeGroupIds[i] = nodeGroup.Id()
			}
			assert.ElementsMatch(t, deleted, removedNodeGroupIds)

			// Assert expected node groups are removed.
			assert.Equal(t, tc.deleteError, err)
			if tc.expectedDelete {
				if assert.Equal(t, 1, len(deleted), "Unexpected node groups deleted: %v", deleted) {
					assert.True(t, oneOf(deleted[0], tc.allowedDeleted), "Unexpected node group deleted, got: %s, want one of: %v", deleted[0], tc.allowedDeleted)
				}
			} else {
				assert.Equal(t, 0, len(deleted), "Unexpected node groups deleted, got: %v, want: []", deleted)
			}
		})
	}
}

func TestRecognizeFailedScaleUpReason(t *testing.T) {
	testCases := []struct {
		desc         string
		error        error
		wantedReason metrics.FailedScaleUpReason
	}{
		{
			desc:         "Account deleted error",
			error:        errors.New("Service account xyz does not exist"),
			wantedReason: gkeclient.ServiceAccountDeleted,
		},
		{
			desc:         "GKE Account permissions error returned by GKE API",
			error:        errors.New("The Kubernetes Engine service account is missing required permissions  on this project. See https://cloud.google.com/kubernetes-engine/docs/troubleshooting#gke_service_account_deleted for more info: required \"container.hostServiceAgent.use\" permission(s) for \"projects/xx\"., forbidden"),
			wantedReason: GKEServiceAccountPermissionError,
		},
		{
			desc:         "GKE Account permissions error returned by GCE API",
			error:        errors.New("Google Compute Engine: Required 'compute.instanceGroupManagers.create' permission for 'projects/xx'"),
			wantedReason: GKEServiceAccountPermissionError,
		},
		{
			desc:         "Out of quota error",
			error:        errors.New("CPU quota exceeded"),
			wantedReason: gce_cloudprovider.ErrorCodeQuotaExceeded,
		},
		{
			desc:         "Account named 'quota' deleted error",
			error:        errors.New("Service account quota does not exist"),
			wantedReason: gkeclient.ServiceAccountDeleted,
		},
		{
			desc:         "Project metadata conflict in statup script URL",
			error:        errors.New("Error occurred: clusters/node pools cannot be created while \"script-url\" is specified in the project metadata"),
			wantedReason: StartupScriptUrlConflict,
		},
		{
			desc:         "Any other error",
			error:        errors.New("Some error occurred while scaling up"),
			wantedReason: metrics.CloudProviderError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			actual := recognizeFailedScaleUpReason(tc.error)
			if actual != tc.wantedReason {
				t.Errorf("recognizeFailedScaleUpReason(%v) = %v, want %v", tc.error, actual, tc.wantedReason)
			}
		})
	}
}

func TestNodeGroupManagerResourceLabels(t *testing.T) {
	pod := test.BuildTestPod("test-pod", 100, 100)
	pod.Spec.NodeSelector = map[string]string{"cloud.google.com/resourcelabel_annotation": ""}
	wantSystemLabels := map[string]string{"cloud.google.com/resourcelabel_annotation": ""}

	t.Run("check resource labels", func(t *testing.T) {
		provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
			WithAutoprovisioningLocations("us-central1-c").
			WithAllZones("us-central1-a", "us-central1-b", "us-central1-c").
			WithAutoprovisioningDefaultFamily(machinetypes.E2).
			WithAutoprovisioningEnabled(true).
			Build()
		em := experiments.NewMockManager()
		manager := NewAutoprovisioningNodeGroupManager(AutoprovisioningNodeGroupManagerOptions{
			CloudProvider:        provider,
			Flags:                AutoprovisioningNodeGroupManagerFlags{},
			Lister:               computeclass_lister.NewMockCrdLister([]computeclass.CRD{}),
			ExperimentsManager:   em,
			OptionsTracker:       tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
			ResourcePolicyPuller: &placement.FakeResourcePolicyPullerProvider{},
		})

		reqs, err := manager.extractRequirements([]*apiv1.Pod{pod}, machinetypes.GpuRequest{}, TpuRequest{})
		assert.Empty(t, err)
		assert.Len(t, reqs, 1)
		assert.Equal(t, wantSystemLabels, reqs[0].systemLabels)
	})

}

// TestNewAutoprovisioningNodeGroupManagerGeneratorsOrder - logic in autoprovisioning/injection.go depends on the
// expected spec generators order specified in the tests
func TestNewAutoprovisioningNodeGroupManagerGeneratorsOrder(t *testing.T) {
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithMachineTypes("test-machine-type").
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()

	slMatcher, err := gkelabels.NewMatcher([]string{"test"})
	assert.NoError(t, err)

	expectedSpecGenerators := []NodePoolSpecGenerator{
		NewGpuRequestGenerator(provider),
		NewTpuRequestGenerator(provider),
		NewBootDiskConfigGenerator(provider),
		NewReservationGenerator(nil, ReservationFlags{
			SpecificTypeReservationMatchEnabled: true,
		}, "", nil, nil),
		NewWorkloadSeparationGenerator(slMatcher),
		NewCSNGenerator(true),
		NewMachineSelectionGenerator(nil, machineselection.Selector{}, nil),
		NewPlacementGroupGenerator(provider, placement.NewFakeResourcePolicyPullerProvider([]*gceclient.GceResourcePolicy{}, nil)),
		NewSandboxTypeGenerator(),
		NewPreemeptionOptionGenerator(true),
		NewConsolidationDelayGenerator(nil),
		NewExtendedDurationPodGenerator(provider),
		NewComputeClassGenerator(provider, computeclass_lister.NewMockCrdListerWithLabel(nil, ""), true, nil),
		NewSelfServiceGenerator(),
		NewPodIsolationLabelGenerator(provider),
		NewPodCapacityLabelGenerator(provider),
		NewLocalSSDConfigGenerator(provider),
		NewMultiNetworkingGenerator(nil),
		NewSystemLabelsGenerator(slMatcher),
		NewFlexStartGenerator(nil),
		NewMaxRunDurationGenerator(nil, provider),
		NewLinuxNodeConfigGenerator(),
		NewKubeletConfigGenerator(),
		NewMaxPodsPerNodeGenerator(provider, nil),
		NewSpecifiedZonesGenerator(provider, true, nil),
		NewResourceLabelsGenerator(),
		NewProvisioningRequestGenerator(experiments.NewMockManager(), provider),
		NewAcceleratorSliceGenerator(provider),
		NewNodeVersionGenerator(),
		NewConfidentialNodeGenerator(provider),
	}

	t.Run("check order of manager's generators", func(t *testing.T) {
		em := experiments.NewMockManager()
		manager := NewAutoprovisioningNodeGroupManager(AutoprovisioningNodeGroupManagerOptions{
			CloudProvider:      provider,
			ReservationsPuller: nil,
			Flags: AutoprovisioningNodeGroupManagerFlags{
				TpuAutoprovisioningEnabled: true,
				ReservationFlags: ReservationFlags{
					SpecificTypeReservationMatchEnabled: true,
				},
				ProvisioningLabelEnabled: true,
				MultiNetworkingEnabled:   true,
				BootDiskConfigEnabled:    true,
			},
			AllowlistedSystemLabelsMatcher: slMatcher,
			Lister:                         computeclass_lister.NewMockCrdListerWithLabel(nil, ""),
			ExperimentsManager:             em,
			OptionsTracker:                 tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
			ResourcePolicyPuller:           &placement.FakeResourcePolicyPullerProvider{},
		})
		assert.Equal(t, len(expectedSpecGenerators), len(manager.specGenerators))
		for i, generator := range manager.specGenerators {
			assert.Equal(t, reflect.TypeOf(expectedSpecGenerators[i]), reflect.TypeOf(generator), fmt.Sprintf("%s is not equal to %s", reflect.TypeOf(expectedSpecGenerators[i]), reflect.TypeOf(generator)))
		}
	})
}

func TestIsDraTpuPod(t *testing.T) {
	draTpuCC := computeclass.NewTestCrd(
		computeclass.WithLabel(gkelabels.ComputeClassLabel),
		computeclass.WithName("dra-tpu-cc"),
		computeclass.WithTpuDriverMode(computeclass.TpuDriverModeDynamicResourceAllocation),
	)

	devicePluginTpuCC := computeclass.NewTestCrd(
		computeclass.WithLabel(gkelabels.ComputeClassLabel),
		computeclass.WithName("device-plugin-tpu-cc"),
		computeclass.WithTpuDriverMode(computeclass.TpuDriverModeDevicePlugin),
	)

	draPod := buildTpuPod("dra-tpu", "", 0, "")
	draPod.Spec.ResourceClaims = []apiv1.PodResourceClaim{{Name: "tpu"}}
	draPod = addSeparation(draPod, gkelabels.ComputeClassLabel, draTpuCC.Name(), true)

	draPodNoResourceClaims := buildTpuPod("dra-tpu-no-resource-claims", "", 0, "")
	draPodNoResourceClaims = addSeparation(draPodNoResourceClaims, gkelabels.ComputeClassLabel, draTpuCC.Name(), false)

	devicePluginPod := buildTpuPod("dp-tpu", "", 4, "")
	devicePluginPod = addSeparation(devicePluginPod, gkelabels.ComputeClassLabel, devicePluginTpuCC.Name(), true)

	noCCPod := buildTpuPod("no-cc-tpu", "", 4, "")

	lister := computeclass_lister.NewMockCrdListerWithLabel([]computeclass.CRD{draTpuCC, devicePluginTpuCC}, gkelabels.ComputeClassLabel)
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithAutoprovisioningEnabled(true).Build()
	em := experiments.NewMockManager()
	manager := NewAutoprovisioningNodeGroupManager(AutoprovisioningNodeGroupManagerOptions{
		Lister:               lister,
		CloudProvider:        provider,
		ExperimentsManager:   em,
		OptionsTracker:       tracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, em),
		ResourcePolicyPuller: &placement.FakeResourcePolicyPullerProvider{},
	})

	tests := []struct {
		name    string
		pod     *apiv1.Pod
		wantDra bool
	}{
		{
			name:    "DRATpuPod",
			pod:     draPod,
			wantDra: true,
		},
		{
			name:    "DRATpuPodNoResourceClaims",
			pod:     draPodNoResourceClaims,
			wantDra: false,
		},
		{
			name:    "DevicePluginTpuPod",
			pod:     devicePluginPod,
			wantDra: false,
		},
		{
			name:    "NoCCPod",
			pod:     noCCPod,
			wantDra: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantDra, manager.isDraTpuPod(tc.pod))
		})
	}
}

func oneOf(x string, ys []string) bool {
	for _, y := range ys {
		if x == y {
			return true
		}
	}
	return false
}

// asyncNodeGroupInitializerMock implements interfaces.AsyncNodeGroupFinalizer
// Used to await node pool initialization in tests.
type asyncNodeGroupInitializerMock struct {
	t        *testing.T
	sizes    map[string]int64
	executed chan struct{}
	mutex    sync.Mutex // guards result
	result   *nodegroups.AsyncNodeGroupCreationResult
}

func newAsyncNodeGroupInitializer(t *testing.T) *asyncNodeGroupInitializerMock {
	return &asyncNodeGroupInitializerMock{
		t:        t,
		executed: make(chan struct{}, 1),
	}
}

func (m *asyncNodeGroupInitializerMock) InitializeNodeGroup(result nodegroups.AsyncNodeGroupCreationResult) {
	m.mutex.Lock()
	if m.result != nil {
		m.mutex.Unlock()
		m.t.Fatalf("initializer was already executed")
		return
	}
	m.result = &result
	m.mutex.Unlock()
	m.executed <- struct{}{}
}

func (m *asyncNodeGroupInitializerMock) GetTargetSize(nodeGroup string) int64 {
	return m.sizes[nodeGroup]
}

func (m *asyncNodeGroupInitializerMock) SetTargetSize(nodeGroup string, size int64) {
	m.sizes[nodeGroup] = size
}

func (m *asyncNodeGroupInitializerMock) ChangeTargetSize(nodeGroup string, delta int64) {
	m.sizes[nodeGroup] = m.sizes[nodeGroup] + delta
}

func (m *asyncNodeGroupInitializerMock) AwaitResultOrFail() *nodegroups.AsyncNodeGroupCreationResult {
	select {
	case x := <-m.executed:
		m.executed <- x
	default:
		m.t.Fatalf("initializer was not executed within the timeout")
		return nil
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.result
}

func createCSNPod(name string, cpu int64, mem int64, options ...func(*apiv1.Pod)) *apiv1.Pod {
	p := test.BuildTestPod(name, cpu, mem, options...)
	csn.MakePodCSN(p, "ns/buffer")
	return p
}
