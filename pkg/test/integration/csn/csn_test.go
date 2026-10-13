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

package csn

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	k8stesting "k8s.io/client-go/testing"
	gceinternal "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	fakegce "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/metadata"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/capacitybuffers"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	kube_test "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const fastCSNControllerConfigJSON = `{"suspend":{"minNodeLifetime":"0s","preSuspendDelay":"1ms"},"dispatcher":{"retry":{"maxRetries":1,"maxRetriesByOp":{"CONSUME":1,"SUSPEND":1},"initialDelay":"1ms","maxDelay":"5ms"}}}`

// csnGceClient wraps fakegce.GceClient to track and simulate GCE SuspendInstances
// and ResumeInstances calls and per-instance RUNNING/SUSPENDED states.
type csnGceClient struct {
	*fakegce.GceClient
	mu               sync.Mutex
	instanceStatuses map[string]string
	suspendCalls     []gceinternal.GceRef
	resumeCalls      []gceinternal.GceRef
	suspendErr       error
	resumeErr        error
}

func newCSNGceClient(base *fakegce.GceClient) *csnGceClient {
	return &csnGceClient{
		GceClient:        base,
		instanceStatuses: make(map[string]string),
	}
}

func (c *csnGceClient) setInstanceStatus(name, status string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.instanceStatuses[name] = status
}

func (c *csnGceClient) statusForInstanceLocked(name, fallback string) string {
	if s := c.instanceStatuses[name]; s != "" {
		return s
	}
	if fallback != "" {
		return fallback
	}
	return "RUNNING"
}

func (c *csnGceClient) setSuspendError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.suspendErr = err
}

func (c *csnGceClient) setResumeError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeErr = err
}

func (c *csnGceClient) getSuspendCalls() []gceinternal.GceRef {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]gceinternal.GceRef, len(c.suspendCalls))
	copy(out, c.suspendCalls)
	return out
}

func (c *csnGceClient) getResumeCalls() []gceinternal.GceRef {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]gceinternal.GceRef, len(c.resumeCalls))
	copy(out, c.resumeCalls)
	return out
}

func (c *csnGceClient) SuspendInstances(migRef gceinternal.GceRef, instances []gceinternal.GceRef, forceSuspend bool) error {
	c.mu.Lock()
	c.suspendCalls = append(c.suspendCalls, instances...)
	err := c.suspendErr
	if err == nil {
		for _, ref := range instances {
			c.instanceStatuses[ref.Name] = "SUSPENDED"
		}
	}
	c.mu.Unlock()
	if err != nil {
		return err
	}
	return c.GceClient.SuspendInstances(migRef, instances, forceSuspend)
}

func (c *csnGceClient) ResumeInstances(migRef gceinternal.GceRef, instances []gceinternal.GceRef) error {
	c.mu.Lock()
	c.resumeCalls = append(c.resumeCalls, instances...)
	err := c.resumeErr
	if err == nil {
		for _, ref := range instances {
			c.instanceStatuses[ref.Name] = "RUNNING"
		}
	}
	c.mu.Unlock()
	if err != nil {
		return err
	}
	return c.GceClient.ResumeInstances(migRef, instances)
}

func (c *csnGceClient) FetchAllInstances(ctx context.Context, project, zone string, filter string) ([]gceinternal.GceInstance, error) {
	res, err := c.GceClient.FetchAllInstances(ctx, project, zone, filter)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range res {
		name := path.Base(res[i].Id)
		res[i].GCEStatus = c.statusForInstanceLocked(name, res[i].GCEStatus)
	}
	return res, nil
}

func (c *csnGceClient) FetchMigInstances(ctx context.Context, ref gceinternal.GceRef) ([]gceinternal.GceInstance, error) {
	res, err := c.GceClient.FetchMigInstances(ctx, ref)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range res {
		name := path.Base(res[i].Id)
		res[i].GCEStatus = c.statusForInstanceLocked(name, res[i].GCEStatus)
	}
	return res, nil
}

func (c *csnGceClient) FetchManagedInstances(migRef gceinternal.GceRef, filter string) ([]*gceclient.ManagedInstance, error) {
	res, err := c.GceClient.FetchManagedInstances(migRef, filter)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, inst := range res {
		copiedInst := *inst
		status := c.statusForInstanceLocked(inst.Name, inst.InstanceStatus)
		copiedInst.InstanceStatus = status
		copiedInst.TargetStatus = status
		res[i] = &copiedInst
	}
	return res, nil
}

// installPodNodeNameFieldSelectorReactor makes the fake Kubernetes client honor
// `spec.nodeName=<node>` field selectors on Pod List calls, which CSN's
// SuspendHandler uses via IsWorkloadPresent before suspending a node.
func installPodNodeNameFieldSelectorReactor(infra *integration.TestInfrastructure) {
	infra.Fakes.KubeClient.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		listAction, ok := action.(k8stesting.ListAction)
		if !ok {
			return false, nil, nil
		}
		fields := listAction.GetListRestrictions().Fields
		if fields == nil || fields.Empty() {
			return false, nil, nil
		}
		nodeName, found := fields.RequiresExactMatch("spec.nodeName")
		if !found {
			return false, nil, nil
		}
		objs, err := infra.Fakes.KubeClient.Tracker().List(
			apiv1.SchemeGroupVersion.WithResource("pods"),
			apiv1.SchemeGroupVersion.WithKind("Pod"),
			listAction.GetNamespace(),
		)
		if err != nil {
			return true, nil, err
		}
		podList, ok := objs.(*apiv1.PodList)
		if !ok {
			return false, nil, nil
		}
		filtered := &apiv1.PodList{
			TypeMeta: podList.TypeMeta,
			ListMeta: podList.ListMeta,
		}
		for _, pod := range podList.Items {
			if pod.Spec.NodeName == nodeName {
				filtered.Items = append(filtered.Items, pod)
			}
		}
		return true, filtered, nil
	})
}

func addCSNCapacityBuffer(t *testing.T, infra *integration.TestInfrastructure, name string, replicas int32, cpu, mem string) *cbv1beta1.CapacityBuffer {
	t.Helper()
	podTemplateName := name + "-pod-template"
	podTemplate := &apiv1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:       podTemplateName,
			Namespace:  "default",
			Generation: 1,
		},
		Template: apiv1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podTemplateName,
				Namespace: "default",
			},
			Spec: apiv1.PodSpec{
				Containers: []apiv1.Container{
					{
						Name:  "buffer-container",
						Image: "pause:latest",
						Resources: apiv1.ResourceRequirements{
							Requests: apiv1.ResourceList{
								apiv1.ResourceCPU:    resource.MustParse(cpu),
								apiv1.ResourceMemory: resource.MustParse(mem),
							},
						},
					},
				},
			},
		},
	}
	infra.Fakes.K8s.AddPodTemplate(podTemplate)

	strategy := capacitybuffers.ColdProvisioningStrategy
	buffer := &cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID("uid-" + name),
			Annotations: map[string]string{
				csn.ColdCapacityInitTimeAnnotationKey:              "0s",
				"buffer.gke.io/standby-capacity-refresh-frequency": "never",
			},
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &strategy,
			Replicas:             ptr.To(replicas),
			PodTemplateRef: &cbv1beta1.LocalObjectRef{
				Name: podTemplateName,
			},
		},
		Status: cbv1beta1.CapacityBufferStatus{
			ProvisioningStrategy:  &strategy,
			Replicas:              ptr.To(replicas),
			PodTemplateGeneration: ptr.To(int64(1)),
			PodTemplateRef: &cbv1beta1.LocalObjectRef{
				Name: podTemplateName,
			},
			Conditions: []metav1.Condition{
				{
					Type:               capacitybuffer.ReadyForProvisioningCondition,
					Status:             metav1.ConditionTrue,
					LastTransitionTime: metav1.Now(),
					Reason:             "Ready",
				},
			},
		},
	}
	assert.NoError(t, infra.Fakes.CapacityBufferIndexer.Add(buffer))
	return buffer
}

func csnNodePool(name string, initialCount, maxCount int) *gke_api_beta.NodePool {
	return integration.DefaultNodePoolBuilder(name).
		WithMachineType("n1-standard-2").
		WithSize(int64(initialCount)).
		WithMin(0).
		WithMax(int64(maxCount)).
		WithLocations("us-central1-a").
		WithLabels(map[string]string{
			metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
		}).
		Build()
}

func TestCSNScaleUpAndSuspendStandbyNodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		installPodNodeNameFieldSelectorReactor(infra)
		gceWrapper := newCSNGceClient(infra.Fakes.GceService)

		config := integration.NewTestConfig().
			WithNodePools(csnNodePool("csn-pool", 1, 5)).
			WithGceClient(gceWrapper).
			WithOverrides(integration.WithCSNEnabled(true)).
			WithExperimentOverrides(nil, map[string]string{
				experiments.ColdStandbyNodesControllerConfigV1Flag: fastCSNControllerConfigJSON,
			})

		autoscaler := integration.MustSetupAutoscaler(ctx, t, config, infra)

		// Mark the existing node in csn-pool as a chilling CSN node so it is tracked by CSNNodeController.
		nodes := infra.Fakes.K8s.Nodes()
		assert.Len(t, nodes.Items, 1)
		chillingNode := nodes.Items[0].DeepCopy()
		chillingNode.CreationTimestamp = metav1.NewTime(time.Now().Add(-10 * time.Minute))
		chillingNode, err := csn.SetNodeAs(chillingNode, csn.NodeStateChilling)
		assert.NoError(t, err)
		gceWrapper.setInstanceStatus(chillingNode.Name, "RUNNING")
		infra.Fakes.K8s.UpdateNode(chillingNode)

		// Add a CSN CapacityBuffer requesting 2 standby replicas (1 fits on the existing chilling node,
		// 1 requires scaling up csn-pool).
		addCSNCapacityBuffer(t, infra, "standby-buf", 2, "1000m", "1Gi")

		cluster := integration.GetTestCluster(t, infra, config)
		assert.Len(t, cluster.NodePools, 1)

		for range 5 {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Millisecond)
			time.Sleep(20 * time.Millisecond)
			synctest.Wait()
			if len(gceWrapper.getSuspendCalls()) >= 1 && infra.Fakes.GkeService.MustGetTargetSize(t, cluster.NodePools[0]) == 2 {
				break
			}
		}

		assert.GreaterOrEqual(t, len(gceWrapper.getSuspendCalls()), 1, "expected CSNNodeController to call GCE SuspendInstances for chilling node")
		assert.Equal(t, int64(2), infra.Fakes.GkeService.MustGetTargetSize(t, cluster.NodePools[0]), "expected csn-pool to scale up from 1 to 2")
		updatedNode, err := infra.Fakes.KubeClient.CoreV1().Nodes().Get(ctx, chillingNode.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, csn.NodeStateSuspended, csn.ClassifyNode(updatedNode), "expected chilling node to transition to Suspended")
	})
}

func TestCSNConsumeSuspendedNodeAndGceErrors(t *testing.T) {
	tests := []struct {
		name           string
		resumeErr      error
		wantFinalState csn.NodeState
	}{
		{
			name:           "ResumeSuccess",
			resumeErr:      nil,
			wantFinalState: csn.NodeStateConsumed,
		},
		{
			name:           "ResumeErrorHandledGracefully",
			resumeErr:      errors.New("simulated GCE ResumeInstances failure"),
			wantFinalState: csn.NodeStateFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)

				infra := integration.SetupInfrastructure(ctx, t)
				installPodNodeNameFieldSelectorReactor(infra)
				gceWrapper := newCSNGceClient(infra.Fakes.GceService)
				gceWrapper.setResumeError(tc.resumeErr)

				config := integration.NewTestConfig().
					WithNodePools(csnNodePool("csn-pool", 1, 5)).
					WithGceClient(gceWrapper).
					WithOverrides(integration.WithCSNEnabled(true)).
					WithExperimentOverrides(
						map[string]bool{
							experiments.ColdStandbyNodesWaitForNodeReadiness: false,
						},
						map[string]string{
							experiments.ColdStandbyNodesControllerConfigV1Flag: fastCSNControllerConfigJSON,
						},
					)

				autoscaler := integration.MustSetupAutoscaler(ctx, t, config, infra)

				nodes := infra.Fakes.K8s.Nodes()
				assert.Len(t, nodes.Items, 1)
				suspendedNode := nodes.Items[0].DeepCopy()
				suspendedNode.CreationTimestamp = metav1.NewTime(time.Now().Add(-10 * time.Minute))
				suspendedNode, err := csn.SetNodeAs(suspendedNode, csn.NodeStateSuspended)
				assert.NoError(t, err)
				gceWrapper.setInstanceStatus(suspendedNode.Name, "SUSPENDED")
				infra.Fakes.K8s.UpdateNode(suspendedNode)

				// Inject an unschedulable workload pod that can fit on the suspended CSN node.
				pod := kube_test.BuildTestPod("workload-pod-1", 1000, 100*1024*1024, kube_test.MarkUnschedulable())
				pod.Namespace = "default"
				infra.Fakes.K8s.AddPod(pod)

				for range 5 {
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Millisecond)
					time.Sleep(20 * time.Millisecond)
					synctest.Wait()
					if len(gceWrapper.getResumeCalls()) >= 1 {
						break
					}
				}
				assert.GreaterOrEqual(t, len(gceWrapper.getResumeCalls()), 1, "expected CSNNodeController to invoke ResumeInstances")

				// Run another loop after the resume attempt (including when ResumeInstances errored)
				// to verify reconciliation with non-empty GCEStatus and subsequent loops do not panic.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Minute)
				time.Sleep(20 * time.Millisecond)
				synctest.Wait()

				updated, err := infra.Fakes.KubeClient.CoreV1().Nodes().Get(ctx, suspendedNode.Name, metav1.GetOptions{})
				assert.NoError(t, err)
				assert.Equal(t, tc.wantFinalState, csn.ClassifyNode(updated))
			})
		})
	}
}

func TestCSNSuspendErrorHandledGracefully(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		installPodNodeNameFieldSelectorReactor(infra)
		gceWrapper := newCSNGceClient(infra.Fakes.GceService)
		gceWrapper.setSuspendError(errors.New("simulated GCE SuspendInstances failure"))

		config := integration.NewTestConfig().
			WithNodePools(csnNodePool("csn-pool", 1, 5)).
			WithGceClient(gceWrapper).
			WithOverrides(integration.WithCSNEnabled(true)).
			WithExperimentOverrides(nil, map[string]string{
				experiments.ColdStandbyNodesControllerConfigV1Flag: fastCSNControllerConfigJSON,
			})

		autoscaler := integration.MustSetupAutoscaler(ctx, t, config, infra)

		nodes := infra.Fakes.K8s.Nodes()
		assert.Len(t, nodes.Items, 1)
		chillingNode := nodes.Items[0].DeepCopy()
		chillingNode.CreationTimestamp = metav1.NewTime(time.Now().Add(-10 * time.Minute))
		chillingNode, err := csn.SetNodeAs(chillingNode, csn.NodeStateChilling)
		assert.NoError(t, err)
		gceWrapper.setInstanceStatus(chillingNode.Name, "RUNNING")
		infra.Fakes.K8s.UpdateNode(chillingNode)

		addCSNCapacityBuffer(t, infra, "standby-buf-err", 1, "500m", "512Mi")

		for range 5 {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Millisecond)
			time.Sleep(20 * time.Millisecond)
			synctest.Wait()
			if len(gceWrapper.getSuspendCalls()) >= 1 {
				break
			}
		}
		assert.GreaterOrEqual(t, len(gceWrapper.getSuspendCalls()), 1, "expected SuspendInstances to be attempted")

		// Subsequent loop must not panic after SuspendInstances fails.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Minute)
		time.Sleep(20 * time.Millisecond)
		synctest.Wait()

		updatedNode, err := infra.Fakes.KubeClient.CoreV1().Nodes().Get(ctx, chillingNode.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, csn.NodeStateFailed, csn.ClassifyNode(updatedNode), "expected chilling node to transition to Failed after permanent SuspendInstances failure")
	})
}

func TestCSNWithUnmanagedNodeDoesNotPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		installPodNodeNameFieldSelectorReactor(infra)
		gceWrapper := newCSNGceClient(infra.Fakes.GceService)

		config := integration.NewTestConfig().
			WithNodePools(csnNodePool("csn-pool", 1, 5)).
			WithGceClient(gceWrapper).
			WithOverrides(integration.WithCSNEnabled(true)).
			WithExperimentOverrides(
				map[string]bool{
					experiments.ColdStandbyNodesProcessTemplateNodeInfosFlag: false,
				},
				map[string]string{
					experiments.ColdStandbyNodesControllerConfigV1Flag: fastCSNControllerConfigJSON,
				},
			)

		autoscaler := integration.MustSetupAutoscaler(ctx, t, config, infra)

		// Add an unmanaged node (whose ProviderID does not belong to any registered MIG,
		// so NodeGroupForNode / GkeMigForNode returns (nil, nil)).
		// Exercise both upcoming-node reconciliation and CSN chilling-node handling on unmanaged nodes.
		unmanagedUpcoming := kube_test.BuildTestNode("unmanaged-upcoming-node", 2000, 4*1024*1024*1024)
		unmanagedUpcoming.Spec.ProviderID = "gce://test-project/us-central1-a/unmanaged-upcoming-node"
		if unmanagedUpcoming.Annotations == nil {
			unmanagedUpcoming.Annotations = make(map[string]string)
		}
		unmanagedUpcoming.Annotations[annotations.NodeUpcomingAnnotation] = "true"
		kube_test.SetNodeReadyState(unmanagedUpcoming, true, time.Now().Add(-10*time.Minute))
		infra.Fakes.K8s.AddNode(unmanagedUpcoming)

		unmanagedCSN := kube_test.BuildTestNode("unmanaged-csn-node", 2000, 4*1024*1024*1024)
		unmanagedCSN.Spec.ProviderID = fmt.Sprintf("gce://%s/us-central1-a/unmanaged-csn-node", integration.DefaultProject())
		unmanagedCSN.CreationTimestamp = metav1.NewTime(time.Now().Add(-10 * time.Minute))
		kube_test.SetNodeReadyState(unmanagedCSN, true, time.Now().Add(-10*time.Minute))
		var err error
		unmanagedCSN, err = csn.SetNodeAs(unmanagedCSN, csn.NodeStateChilling)
		assert.NoError(t, err)
		infra.Fakes.K8s.AddNode(unmanagedCSN)

		// Request 3 standby replicas of 1200m CPU: 1 schedules onto csn-pool's initial node,
		// 1 schedules onto unmanagedCSN (exercising CSN handling on an unmanaged chilling node),
		// and the 3rd triggers scale-up of csn-pool from 1 to 2.
		addCSNCapacityBuffer(t, infra, "standby-buf-unmanaged", 3, "1200m", "1Gi")

		cluster := integration.GetTestCluster(t, infra, config)
		assert.Len(t, cluster.NodePools, 1)

		for range 5 {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Millisecond)
			time.Sleep(20 * time.Millisecond)
			synctest.Wait()
			if len(gceWrapper.getSuspendCalls()) >= 1 && infra.Fakes.GkeService.MustGetTargetSize(t, cluster.NodePools[0]) == 2 {
				break
			}
		}
		assert.Equal(t, int64(2), infra.Fakes.GkeService.MustGetTargetSize(t, cluster.NodePools[0]))
		suspendCalls := gceWrapper.getSuspendCalls()
		assert.NotEmpty(t, suspendCalls, "expected managed csn-pool node to be suspended")
		for _, ref := range suspendCalls {
			assert.NotEqual(t, unmanagedCSN.Name, ref.Name, "unmanaged CSN node should not be suspended")
		}
	})
}
