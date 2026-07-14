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

package pod

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
)

func WithTPU(accelerator string, topology string, acceleratorCount string) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		if p.Spec.NodeSelector == nil {
			p.Spec.NodeSelector = make(map[string]string)
		}
		p.Spec.NodeSelector[labels.TPULabel] = accelerator
		p.Spec.NodeSelector[labels.TPUTopologyLabel] = topology
		p.Spec.NodeSelector[labels.AcceleratorCountLabel] = acceleratorCount

		if len(p.Spec.Containers) > 0 {
			if p.Spec.Containers[0].Resources.Requests == nil {
				p.Spec.Containers[0].Resources.Requests = make(apiv1.ResourceList)
			}
			if p.Spec.Containers[0].Resources.Limits == nil {
				p.Spec.Containers[0].Resources.Limits = make(apiv1.ResourceList)
			}
			qty := resource.MustParse(acceleratorCount)
			p.Spec.Containers[0].Resources.Requests[apiv1.ResourceName(tpu.ResourceGoogleTPU)] = qty
			p.Spec.Containers[0].Resources.Limits[apiv1.ResourceName(tpu.ResourceGoogleTPU)] = qty
		}

		p.Spec.Tolerations = append(p.Spec.Tolerations, apiv1.Toleration{
			Key:      tpu.ResourceGoogleTPU,
			Operator: apiv1.TolerationOpExists,
			Effect:   apiv1.TaintEffectNoSchedule,
		})
	}
}

func WithCreationTimestamp(t metav1.Time) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		p.CreationTimestamp = t
	}
}

func WithNodeSelector(s map[string]string) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		p.Spec.NodeSelector = s
	}
}

func WithCCC(cccName string) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		p.Spec.NodeSelector = map[string]string{labels.ComputeClassLabel: cccName}
		p.Spec.Tolerations = []apiv1.Toleration{
			{
				Key:      labels.ComputeClassLabel,
				Operator: apiv1.TolerationOpEqual,
				Value:    cccName,
				Effect:   apiv1.TaintEffectNoSchedule,
			},
		}
	}
}

// WithTolerations appends the given tolerations to the pod spec.
func WithTolerations(tolerations ...apiv1.Toleration) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		p.Spec.Tolerations = append(p.Spec.Tolerations, tolerations...)
	}
}

// WithNodeSelectorEntry adds or updates a single key-value pair in the pod's NodeSelector.
func WithNodeSelectorEntry(key, value string) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		if p.Spec.NodeSelector == nil {
			p.Spec.NodeSelector = make(map[string]string)
		}
		p.Spec.NodeSelector[key] = value
	}
}

func WithAnnotations(annotations map[string]string) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		if p.Annotations == nil {
			p.Annotations = make(map[string]string)
		}
		for k, v := range annotations {
			p.Annotations[k] = v
		}
	}
}

func WithResource(resourceName apiv1.ResourceName, req, lim string) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		if p.Spec.Containers[0].Resources.Requests == nil {
			p.Spec.Containers[0].Resources.Requests = make(apiv1.ResourceList)
		}
		if p.Spec.Containers[0].Resources.Limits == nil {
			p.Spec.Containers[0].Resources.Limits = make(apiv1.ResourceList)
		}
		p.Spec.Containers[0].Resources.Requests[resourceName] = resource.MustParse(req)
		p.Spec.Containers[0].Resources.Limits[resourceName] = resource.MustParse(lim)
	}
}

func WithToleration(key string, op apiv1.TolerationOperator, effect apiv1.TaintEffect) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		p.Spec.Tolerations = append(p.Spec.Tolerations, apiv1.Toleration{
			Key:      key,
			Operator: op,
			Effect:   effect,
		})
	}
}

func WithTolerationValue(key string, op apiv1.TolerationOperator, value string, effect apiv1.TaintEffect) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		p.Spec.Tolerations = append(p.Spec.Tolerations, apiv1.Toleration{
			Key:      key,
			Operator: op,
			Value:    value,
			Effect:   effect,
		})
	}
}

func WithAnnotation(key, value string) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		if p.Annotations == nil {
			p.Annotations = make(map[string]string)
		}
		p.Annotations[key] = value
	}
}

// WithFlexStart adds the Flex Start node selector and toleration to the pod.
func WithFlexStart() func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		if p.Spec.NodeSelector == nil {
			p.Spec.NodeSelector = make(map[string]string)
		}
		p.Spec.NodeSelector["cloud.google.com/gke-flex-start"] = "true"
		p.Spec.Tolerations = append(p.Spec.Tolerations, apiv1.Toleration{
			Key:      "cloud.google.com/gke-flex-start",
			Operator: apiv1.TolerationOpEqual,
			Value:    "true",
			Effect:   apiv1.TaintEffectNoSchedule,
		})
	}
}

func WithProvisioningMode(mode instanceavailability.ProvisioningMode) func(*apiv1.Pod) {
	return func(*apiv1.Pod) {
		if mode == instanceavailability.FlexStart {
			WithFlexStart()
		}
	}
}

// WithOwnerReplicaSet sets the owner reference of the pod to a ReplicaSet with the given name.
func WithOwnerReplicaSet(rsName string) func(p *apiv1.Pod) {
	return func(p *apiv1.Pod) {
		tu.SetRSPodSpec(p, rsName)
	}
}
