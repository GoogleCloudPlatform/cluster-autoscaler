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

package daemonsetmutation

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	podutils "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

const (
	testDSUID  = types.UID("ds-uid-123")
	testDSName = "test-ds"
)

type fakePodResolver struct {
	resolveFunc func(template *apiv1.PodTemplateSpec) (*apiv1.Pod, error)
}

func (f *fakePodResolver) Resolve(ctx context.Context, namespace string, template *apiv1.PodTemplateSpec) (*apiv1.Pod, error) {
	if f.resolveFunc != nil {
		return f.resolveFunc(template)
	}
	return nil, nil
}

func setUpTestPodAndDS() (*appsv1.DaemonSet, map[string]*framework.NodeInfo) {
	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-pod-12345", testDSName),
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "DaemonSet", UID: testDSUID, Name: testDSName, Controller: ptr.To(true)},
			},
		},
		Spec: apiv1.PodSpec{
			RuntimeClassName: ptr.To("mock-overhead"),
			Containers: []apiv1.Container{
				{
					Name: "main",
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							apiv1.ResourceCPU: resource.MustParse("100m"),
						},
					},
				},
			},
		},
	}
	podInfo := framework.NewPodInfo(pod, nil)
	node := &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "test-node"}}
	nodeInfo := framework.NewNodeInfo(node, nil, podInfo)
	nodeInfos := map[string]*framework.NodeInfo{"test-group": nodeInfo}

	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testDSName,
			Namespace:  "default",
			UID:        testDSUID,
			Generation: 1,
		},
		Spec: appsv1.DaemonSetSpec{
			Template: apiv1.PodTemplateSpec{
				Spec: pod.Spec,
			},
		},
	}

	return ds, nodeInfos
}

func resolverWithOverhead(cpu string) *fakePodResolver {
	return &fakePodResolver{
		resolveFunc: func(template *apiv1.PodTemplateSpec) (*apiv1.Pod, error) {
			return mutatePodOverhead(template, cpu), nil
		},
	}
}

func resolverWithError(err error) *fakePodResolver {
	return &fakePodResolver{
		resolveFunc: func(template *apiv1.PodTemplateSpec) (*apiv1.Pod, error) {
			return nil, err
		},
	}
}

func resolverWithoutChange() *fakePodResolver {
	return &fakePodResolver{
		resolveFunc: func(template *apiv1.PodTemplateSpec) (*apiv1.Pod, error) {
			return podutils.GetPodFromTemplate(template), nil
		},
	}
}

func mutatePodOverhead(template *apiv1.PodTemplateSpec, cpu string) *apiv1.Pod {
	pod := podutils.GetPodFromTemplate(template)
	updatedPod := pod.DeepCopy()
	updatedPod.Spec.Overhead = apiv1.ResourceList{
		apiv1.ResourceCPU: resource.MustParse(cpu),
	}
	return updatedPod
}

func testInformerFactory() informers.SharedInformerFactory {
	return informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0)
}

func expireCacheEntry(c *MutationCache, uid types.UID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.items[uid]; ok {
		entry.expiresAt = time.Now().Add(-1 * time.Minute)
		c.items[uid] = entry
	}
}
