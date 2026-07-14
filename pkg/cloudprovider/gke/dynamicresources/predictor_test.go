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

package dynamicresources

import (
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"

	apiv1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/utils/ptr"
)

type fakeProvider struct{}

func (p *fakeProvider) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return machinetypes.NewMachineConfigProvider(nil)
}

func TestResourcePredictorResourceSlicesForNode(t *testing.T) {
	gpuTemplateNode := test.BuildTestNode("node1", 1, 1)
	gpuTemplateNode.Labels[gkelabels.DraGpuNodeLabel] = "true"
	gpuTemplateNode.Labels[labels.GPULabel] = "exists"
	tpuTemplateNode := test.BuildTestNode("node1", 1, 1)
	tpuTemplateNode.Labels[gkelabels.DraTpuNodeLabel] = "true"
	tpuTemplateNode.Labels[labels.TPULabel] = "exists"
	dranetTemplateNode := test.BuildTestNode("node1", 1, 1)
	dranetTemplateNode.Labels[gkelabels.DraNetNodeLabel] = "true"
	dranetTemplateNode.Labels[labels.GPULabel] = "exists"
	noAcceleratorTemplateNode := test.BuildTestNode("node1", 1, 1)

	a4xGpuTemplateNode := test.BuildTestNode("node1", 1, 1)
	a4xGpuTemplateNode.Labels[gkelabels.DraGpuNodeLabel] = "true"
	a4xGpuTemplateNode.Labels[labels.GPULabel] = "exists"
	a4xGpuTemplateNode.Labels[gkelabels.MachineFamilyLabel] = machinetypes.A4X.Name()

	a4xGpuDevicePluginTemplateNode := test.BuildTestNode("node1", 1, 1)
	a4xGpuDevicePluginTemplateNode.Labels[labels.GPULabel] = "exists"
	a4xGpuDevicePluginTemplateNode.Labels[gkelabels.MachineFamilyLabel] = machinetypes.A4X.Name()

	nodeWithoutGPUButDraEnabled := test.BuildTestNode("node1", 1, 1)
	nodeWithoutGPUButDraEnabled.Labels[gkelabels.DraGpuNodeLabel] = "true"

	nodeWithoutTPUButDraEnabled := test.BuildTestNode("node1", 1, 1)
	nodeWithoutTPUButDraEnabled.Labels[gkelabels.DraTpuNodeLabel] = "true"

	tests := map[string]struct {
		templateNode      *apiv1.Node
		acceleratorConfig []*gke_api_beta.AcceleratorConfig
		machineType       string
		tpuType           string
		deviceClasses     []string

		wantResourceSlicesCount  int
		wantResourceSlicePerPool map[string]int
	}{
		"NoResourceSlices": {
			templateNode:            noAcceleratorTemplateNode,
			wantResourceSlicesCount: 0,
		},
		"OneResourceGPU": {
			templateNode: gpuTemplateNode,
			acceleratorConfig: []*gke_api_beta.AcceleratorConfig{
				{
					AcceleratorCount: int64(1),
					AcceleratorType:  "nvidia-tesla-t4",
				},
			},
			wantResourceSlicesCount: 1,
			wantResourceSlicePerPool: map[string]int{
				GpuDriver: 1,
			},
		},
		"OneResourceTPU": {
			templateNode:            tpuTemplateNode,
			machineType:             "ct5lp-hightpu-1t",
			tpuType:                 "tpu-v5-lite-podslice",
			wantResourceSlicesCount: 1,
			wantResourceSlicePerPool: map[string]int{
				TpuDriver: 1,
			},
		},
		"OneResourceNET": {
			templateNode:            dranetTemplateNode,
			machineType:             "a4x-highgpu-4g",
			wantResourceSlicesCount: 1,
			wantResourceSlicePerPool: map[string]int{
				NetworkDriver: 1,
			},
		},
		"GPUDriverEnabled_NoGPUAttached": {
			templateNode:            nodeWithoutGPUButDraEnabled,
			wantResourceSlicesCount: 0,
		},
		"TPUDriverEnabled_NoTPUAttached": {
			templateNode:            nodeWithoutTPUButDraEnabled,
			wantResourceSlicesCount: 0,
		},
		"ComputeDomainOnly": {
			templateNode:            a4xGpuDevicePluginTemplateNode,
			deviceClasses:           []string{"abcd", ComputeDomainDaemonDeviceClassName, "xyz", ComputeDomainChannelDeviceClassName},
			wantResourceSlicesCount: 1,
			wantResourceSlicePerPool: map[string]int{
				ComputeDomainDriver: 1,
			},
		},
		"ComputeDomainOnlyNoClasses": {
			templateNode:            a4xGpuDevicePluginTemplateNode,
			wantResourceSlicesCount: 0,
		},
		"ComputeDomainOnlyOneClassMissing": {
			templateNode:            a4xGpuDevicePluginTemplateNode,
			deviceClasses:           []string{"abcd", ComputeDomainDaemonDeviceClassName, "xyz"},
			wantResourceSlicesCount: 0,
		},
		"ComputeDomainWithGPU": {
			templateNode:  a4xGpuTemplateNode,
			deviceClasses: []string{"abcd", ComputeDomainDaemonDeviceClassName, "xyz", ComputeDomainChannelDeviceClassName},
			acceleratorConfig: []*gke_api_beta.AcceleratorConfig{
				{
					AcceleratorCount: int64(1),
					AcceleratorType:  "nvidia-tesla-t4",
				},
			},
			wantResourceSlicesCount: 2,
			wantResourceSlicePerPool: map[string]int{
				GpuDriver:           1,
				ComputeDomainDriver: 1,
			},
		},
		"ComputeDomainWithGPUNoClasses": {
			templateNode: a4xGpuTemplateNode,
			acceleratorConfig: []*gke_api_beta.AcceleratorConfig{
				{
					AcceleratorCount: int64(1),
					AcceleratorType:  "nvidia-tesla-t4",
				},
			},
			wantResourceSlicesCount: 1,
			wantResourceSlicePerPool: map[string]int{
				GpuDriver: 1,
			},
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			deviceClasses := map[string]*resourceapi.DeviceClass{}
			for _, className := range test.deviceClasses {
				deviceClasses[className] = &resourceapi.DeviceClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: className,
					},
				}
			}
			draSnapshot := drasnapshot.NewSnapshot(nil, nil, nil, deviceClasses)

			nodePoolSpec := &gkeclient.NodePoolSpec{
				Accelerators: test.acceleratorConfig,
				MachineType:  test.machineType,
				TpuType:      test.tpuType,
			}

			predictor := NewResourcePredictor()
			predictor.SetCloudProvider(&fakeProvider{})
			predictor.FilterOutNodesWithUnreadyResources(nil, nil, nil, draSnapshot)
			resourceSlices, err := predictor.ResourceSlicesForNode(nodePoolSpec, test.templateNode)
			assert.NoError(t, err)
			assert.EqualValues(t, len(resourceSlices), test.wantResourceSlicesCount)
			for _, slice := range resourceSlices {
				wantResourceSliceInPool, wantDriver := test.wantResourceSlicePerPool[slice.Spec.Driver]
				if !wantDriver {
					t.Fatalf("Response contains unexpected driver: %s", slice.Spec.Driver)
				}

				assert.Equal(t, wantResourceSliceInPool, int(slice.Spec.Pool.ResourceSliceCount))
			}
		})
	}
}

func TestResourcePredictorCustomResourcesProcessorMethods(t *testing.T) {
	now := time.Now()
	readyNode1 := test.BuildTestNode("readyNode1", 1, 1)
	test.SetNodeReadyState(readyNode1, true, now)
	readyNode2 := test.BuildTestNode("readyNode2", 1, 1)
	test.SetNodeReadyState(readyNode2, true, now)
	unreadyNode1 := test.BuildTestNode("unreadyNode1", 1, 1)
	test.SetNodeReadyState(unreadyNode1, false, now)
	unreadyNode2 := test.BuildTestNode("unreadyNode2", 1, 1)
	test.SetNodeReadyState(unreadyNode2, false, now)
	readyNodes := []*apiv1.Node{readyNode1, readyNode2}
	allNodes := append(readyNodes, unreadyNode1, unreadyNode2)

	predictor := NewResourcePredictor()

	// Assert that FilterOutNodesWithUnreadyResources() doesn't change the provided Node lists.
	gotAllNodes, gotReadyNodes := predictor.FilterOutNodesWithUnreadyResources(nil, allNodes, readyNodes, drasnapshot.NewSnapshot(nil, nil, nil, nil))
	assert.Equal(t, allNodes, gotAllNodes)
	assert.Equal(t, readyNodes, gotReadyNodes)

	// Assert that GetNodeResourceTargets() doesn't return anything.
	targets, err := predictor.GetNodeResourceTargets(nil, readyNode1, nil)
	assert.NoError(t, err)
	assert.Nil(t, targets)
}

// resourceSlicesForNodeResult can be used to record the result of a ResourcePredictor.ResourceSlicesForNode() call, and
// have it safely accessible from other goroutines.
type resourceSlicesForNodeResult struct {
	lock   sync.Mutex
	slices []*resourceapi.ResourceSlice
	err    error
}

func (r *resourceSlicesForNodeResult) updateResult(slices []*resourceapi.ResourceSlice, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.slices = slices
	r.err = err
}

func (r *resourceSlicesForNodeResult) getResult() (map[string]int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	slicesByDriver := map[string]int{}
	for _, slice := range r.slices {
		slicesByDriver[slice.Spec.Driver]++
	}
	return slicesByDriver, r.err
}

// waitForExpectedResourceSlicesForNodeResults repeatedly checks the provided resourceSlicesForNodeResult objects and verifies if the ResourcePredictor.ResourceSlicesForNode() results
// recorded in them have the expected number of ResourceSlices from each driver. The function returns as soon as all the provided resourceSlicesForNodeResult objects are as expected.
// If that doesn't happen within the provided timeout, t.Fatalf() is called.
func waitForExpectedResourceSlicesForNodeResults(t *testing.T, callResults []*resourceSlicesForNodeResult, wantSlicesByDriver map[string]int, timeout time.Duration) {
	timeoutChan := time.After(timeout)
	for {
		select {
		case <-timeoutChan:
			t.Fatalf("timed out while waiting for the expected slices for %v: %v", timeout, wantSlicesByDriver)
		default:
			expectedResultsFound := 0
			for _, result := range callResults {
				slicesByDriver, err := result.getResult()
				if err != nil {
					t.Fatalf("got unexpected error from ResourceSlicesForNode(): %v", err)
				}
				if diff := cmp.Diff(wantSlicesByDriver, slicesByDriver); diff == "" {
					expectedResultsFound++
				}
			}
			if expectedResultsFound == len(callResults) {
				return
			}
		}
	}
}

func TestResourcePredictorConcurrency(t *testing.T) {
	readGoroutineCount := 10
	multipleConsecutiveWritesToTest := 100
	waitingForNewResultsTimeout := 1 * time.Second

	// ResourcePredictor should predict ComputeDomain slices for this Node if and only if it can find the
	// ComputeDomain DeviceClasses in the cluster. GPU slices should always be predicted.
	a4xGpuTemplateNode := test.BuildTestNode("node1", 1, 1)
	a4xGpuTemplateNode.Labels[labels.GPULabel] = "exists"
	a4xGpuTemplateNode.Labels[gkelabels.DraGpuNodeLabel] = "true"
	a4xGpuTemplateNode.Labels[gkelabels.MachineFamilyLabel] = machinetypes.A4X.Name()
	nodePoolSpec := &gkeclient.NodePoolSpec{
		Accelerators: []*gke_api_beta.AcceleratorConfig{
			{
				AcceleratorCount: int64(1),
				AcceleratorType:  "nvidia-tesla-t4",
			},
		},
	}
	computeDomainDeviceClasses := map[string]*resourceapi.DeviceClass{
		ComputeDomainChannelDeviceClassName: {
			ObjectMeta: metav1.ObjectMeta{
				Name: ComputeDomainChannelDeviceClassName,
			},
		},
		ComputeDomainDaemonDeviceClassName: {
			ObjectMeta: metav1.ObjectMeta{
				Name: ComputeDomainDaemonDeviceClassName,
			},
		},
	}
	otherDeviceClasses := map[string]*resourceapi.DeviceClass{
		"abc": {
			ObjectMeta: metav1.ObjectMeta{
				Name: "abc",
			},
		},
	}

	// Create a single ResourcePredictor shared between multiple read goroutines and a single main write goroutine.
	// This mimics the production setup where the write happens in the main goroutine, while reads can happen
	// concurrently from other goroutines, through mig.TemplateNodeInfo().
	predictor := NewResourcePredictor()
	predictor.SetCloudProvider(&fakeProvider{})

	// Each resourceSlicesForNodeResult is safe to read (from the main test goroutine) and write (from the goroutines fired below) concurrently via its methods.
	var lastCallResults []*resourceSlicesForNodeResult
	// stopChannels are used to stop the goroutines fired below.
	var stopChannels []chan bool

	// Simulate predictor.ResourceSlicesForNode() being called concurrently from separate goroutines.
	// Each goroutine should keep making calls repeatedly, recording the last result in its entry in lastCallResults.
	t.Cleanup(func() {
		for _, stopChannel := range stopChannels {
			stopChannel <- true
		}
	})
	for range readGoroutineCount {
		stopChannel := make(chan bool)
		stopChannels = append(stopChannels, stopChannel)

		lastResult := &resourceSlicesForNodeResult{}
		lastCallResults = append(lastCallResults, lastResult)

		go func() {
			for {
				select {
				case <-stopChannel:
					return
				default:
					slices, err := predictor.ResourceSlicesForNode(nodePoolSpec, a4xGpuTemplateNode)
					lastResult.updateResult(slices, err)
				}
			}
		}()
	}

	// Before predictor.FilterOutNodesWithUnreadyResources() is called, the predictor doesn't know about any DeviceClasses
	// so it should only return the GPU slices.
	// waitForExpectedResourceSlicesForNodeResults waits until all the goroutines fired above see the expected result,
	// and fails the test if that doesn't happen within the timeout.
	waitForExpectedResourceSlicesForNodeResults(t, lastCallResults, map[string]int{GpuDriver: 1}, waitingForNewResultsTimeout)

	// This predictor.FilterOutNodesWithUnreadyResources() call should update the DeviceClasses, and predictor should
	// start returning both the GPU and the ComputeDomain slices.
	predictor.FilterOutNodesWithUnreadyResources(nil, nil, nil, drasnapshot.NewSnapshot(nil, nil, nil, computeDomainDeviceClasses))
	waitForExpectedResourceSlicesForNodeResults(t, lastCallResults, map[string]int{GpuDriver: 1, ComputeDomainDriver: 1}, waitingForNewResultsTimeout)

	// Update the device classes, the new ones don't have the ComputeDomain ones - predictor should stop predicting ComputeDomain again.
	predictor.FilterOutNodesWithUnreadyResources(nil, nil, nil, drasnapshot.NewSnapshot(nil, nil, nil, otherDeviceClasses))
	waitForExpectedResourceSlicesForNodeResults(t, lastCallResults, map[string]int{GpuDriver: 1}, waitingForNewResultsTimeout)

	// Simulate calling FilterOutNodesWithUnreadyResources() repeatedly to triple-check there aren't any race conditions between
	// reading and writing (a single write could technically align between reads and not break, this should be much less
	// probable with both sides being repeated).
	for range multipleConsecutiveWritesToTest {
		predictor.FilterOutNodesWithUnreadyResources(nil, nil, nil, drasnapshot.NewSnapshot(nil, nil, nil, computeDomainDeviceClasses))
	}
	// ComputeDomain devices should be predicted again after the previous FilterOutNodesWithUnreadyResources() call.
	waitForExpectedResourceSlicesForNodeResults(t, lastCallResults, map[string]int{GpuDriver: 1, ComputeDomainDriver: 1}, waitingForNewResultsTimeout)
}

func TestAssignDevicesIntoResourceSlices(t *testing.T) {
	tests := map[string]struct {
		deviceCount        int
		wantResourceSlices int
	}{
		"OneDevice": {
			deviceCount:        1,
			wantResourceSlices: 1,
		},
		"TwoDevices": {
			deviceCount:        2,
			wantResourceSlices: 1,
		},
		"TwoResourceSlices": {
			deviceCount:        resourceapi.ResourceSliceMaxDevices + 1,
			wantResourceSlices: 2,
		},
		"TenResourceSlices": {
			deviceCount:        resourceapi.ResourceSliceMaxDevices * 10,
			wantResourceSlices: 10,
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			devices := []resourceapi.Device{}
			for i := 0; i < test.deviceCount; i++ {
				devices = append(devices, resourceapi.Device{})
			}
			resourceSlices := assignDevicesIntoResourceSlices("node", "driver", "pool", devices)
			gotDeviceCount := 0
			for _, slice := range resourceSlices {
				gotDeviceCount += len(slice.Spec.Devices)
			}

			assert.Equal(t, len(resourceSlices), test.wantResourceSlices)
			assert.Equal(t, test.deviceCount, gotDeviceCount)
		})
	}
}

// removeSliceUIDsForAssertion clears out UUID attribute of the accelerator to make it viable for
// precise 1:1 comparison without implementing a custom comparator
func removeSliceUIDsForAssertion(resourceSlices []*resourceapi.ResourceSlice) {
	for _, resourceSlice := range resourceSlices {
		resourceSlice.UID = types.UID("")
		for _, device := range resourceSlice.Spec.Devices {
			if _, exists := device.Attributes["uuid"]; !exists {
				continue
			}

			device.Attributes["uuid"] = resourceapi.DeviceAttribute{
				StringValue: ptr.To(""),
			}
		}
	}
}

func buildResourceSlice(sliceName, nodeName, poolName, driver string, slicesCount int64, devices []resourceapi.Device) *resourceapi.ResourceSlice {
	return &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: sliceName,
			UID:  types.UID(""),
		},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: &nodeName,
			Driver:   driver,
			Pool: resourceapi.ResourcePool{
				Name:               poolName,
				Generation:         1,
				ResourceSliceCount: slicesCount,
			},
			Devices: devices,
		},
	}
}
