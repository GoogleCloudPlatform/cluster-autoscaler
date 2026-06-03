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

package processors

import (
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
)

const (
	maxRuleIndex = 1000
	// MinCapacityFakePodAnnotation is the annotation used to identify fake pods used for min capacity.
	MinCapacityFakePodAnnotation = "autoscaling.gke.io/min-nodes-fake-pod"
)

// processNodeGroup processes a nodegroup considered for scaleup.
func getRuleIndexForMetrics(nodeGroup cloudprovider.NodeGroup, lister lister.Lister, matcher computeclass.Matcher) (int, crd.CRD, error) {
	c, cName, err := lister.NodeGroupCrd(nodeGroup)
	if err != nil {
		return -1, nil, err
	}

	if c == nil || cName == "" {
		return 0, nil, nil
	}

	ruleFound, ruleIndex, _ := matcher.FirstMatchedRule(nodeGroup, c)

	// Check for no rule matching.
	if !ruleFound {
		if len(c.Rules()) > 0 && !c.ScaleUpAnyway() {
			return -1, nil, fmt.Errorf("nodepool: %v shouldn't scale scale up. crd: %v:%v", nodeGroup, c.Label(), c.Name())
		}
		return len(c.Rules()), c, nil
	}
	ruleIndex = min(ruleIndex, maxRuleIndex)
	return ruleIndex, c, nil
}

// min returns the minimum of 2 int.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// FakePodAntiAffinityHostPort is chosen to enforce a "one fake pod per node" policy
// in the scheduler simulator. Using HostPort collision is much faster to compute
// than PodAntiAffinity rules. Port 10250 is used as it's the standard Kubelet port,
// but since these fake pods are never actually created in the cluster (only used
// in simulation), it just serves as a unique lock key per node.
const FakePodAntiAffinityHostPort = 10250

func buildFakePod(cccName string, priorityIdx *int, index int) *apiv1.Pod {
	name := fmt.Sprintf("min-nodes-fake-ccc-pod-%s-%d", cccName, index)
	nodeSelector := map[string]string{
		labels.ComputeClassLabel: cccName,
	}
	tolerations := []apiv1.Toleration{{Operator: apiv1.TolerationOpExists}}

	if priorityIdx != nil {
		name = fmt.Sprintf("min-nodes-fake-priority-pod-%s-%d-%d", cccName, *priorityIdx, index)
		nodeSelector[labels.ComputeClassPriorityIdxLabel] = fmt.Sprintf("%d", *priorityIdx)
	}

	return &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "kube-system",
			UID:       types.UID(name),
			Annotations: map[string]string{
				MinCapacityFakePodAnnotation: "true",
			},
		},
		Spec: apiv1.PodSpec{
			NodeSelector: nodeSelector,
			Tolerations:  tolerations,
			Containers: []apiv1.Container{{
				Name:  "nginx",
				Image: "nginx:latest",
				Ports: []apiv1.ContainerPort{{
					ContainerPort: 80,
					HostPort:      FakePodAntiAffinityHostPort,
				}},
			}},
		},
	}
}
