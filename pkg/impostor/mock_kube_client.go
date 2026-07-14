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

package impostor

import (
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/client-go/kubernetes/fake"
	typed_appsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

// MockKubeClient is a wrapper over test Clientset overriding some of its methods for autoscaling purposes.
type MockKubeClient struct {
	*fake.Clientset
	corev1  *coreV1
	appsv1  *appsV1
	listers *listerRegistry
}

// NewMockKubeClient returns new instance of Mock Kube Client.
func NewMockKubeClient(pods *sync.Map, pdbs []*policyv1.PodDisruptionBudget) (*MockKubeClient, *listerRegistry) {
	listerSnapshot, _, err := testsnapshot.NewTestSnapshotAndHandle()
	if err != nil {
		panic(err)
	}
	snapshotMutex := &sync.Mutex{}

	listers := newListerRegistry(pods, pdbs, listerSnapshot, snapshotMutex)
	fCs := fake.NewSimpleClientset()
	client := &MockKubeClient{
		Clientset: fCs,
	}
	client.corev1 = newCoreV1(fCs.CoreV1(), pods, listerSnapshot, snapshotMutex, nil)
	client.appsv1 = newAppsV1(listers.statefulSetLister, fCs.AppsV1())
	client.listers = listers
	return client, listers
}

// AddPodCallback adds a callback for Pod events.
func (c *MockKubeClient) AddPodCallback(callback podCallback) {
	c.corev1.addPodCallback(callback)
}

// AddPods adds provided pods to kube client.
func (c *MockKubeClient) AddPods(evictionTime time.Duration, newPods []*corev1.Pod) {
	c.corev1.AddPods(evictionTime, newPods)
}

// AddNode adds provided node to kube client.
func (c *MockKubeClient) AddNode(node *corev1.Node) {
	c.corev1.AddNode(node)
}

// CoreV1 returns coreV1 interface of kube client.
func (c *MockKubeClient) CoreV1() v1.CoreV1Interface {
	return c.corev1
}

// AppsV1 returns appsV1 interface of kube client.
func (c *MockKubeClient) AppsV1() typed_appsv1.AppsV1Interface {
	return c.appsv1
}
