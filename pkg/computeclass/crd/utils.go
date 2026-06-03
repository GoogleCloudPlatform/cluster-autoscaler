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

package crd

import (
	"errors"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

type gkeNodeGroup interface {
	Spec() *gkeclient.NodePoolSpec
	TemplateNodeInfo() (*framework.NodeInfo, error)
	TemplateNodeLabels() (map[string]string, error)
}

func NodeCrdLabel(node *v1.Node, crdLabel string) (string, bool, error) {
	if node == nil {
		return "", false, errors.New("got nil node")
	}
	if value, exists := node.Labels[crdLabel]; exists {
		return value, true, nil
	}

	return "", false, nil
}

// NodeGroupCrdLabel returns value of crd label of the nodeGroup if present.
func NodeGroupCrdLabel(nodeGroup cloudprovider.NodeGroup, crdLabel string) (string, bool, error) {
	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		return "", false, fmt.Errorf("expected GkeMig; got %v", nodeGroup)
	}
	if mig == nil {
		return "", false, errors.New("got nil mig")
	}
	if mig.Spec() != nil && mig.Spec().Labels != nil {
		if value, exists := mig.Spec().Labels[crdLabel]; exists {
			return value, true, nil
		}
	}

	labels, err := mig.TemplateNodeLabels()
	if err != nil {
		return "", false, err
	}
	if value, exists := labels[crdLabel]; exists {
		return value, true, nil
	}
	return "", false, nil
}

// NodeGroupCrdTaint returns value of crd taint if exists.
func NodeGroupCrdTaint(nodeGroup cloudprovider.NodeGroup, crdLabel string) (string, error) {
	crdTaints, err := NodeGroupCrdTaints(nodeGroup, crdLabel)
	if err != nil {
		return "", err
	}

	// If there are > 0 crd taints, return the first one.
	// Note: If there are no crd taints MigCrdTaints will return an error.
	return crdTaints[0].Value, nil
}

// NodeGroupCrdTaints returns all crd taints if exists.
func NodeGroupCrdTaints(nodeGroup cloudprovider.NodeGroup, crdLabel string) ([]v1.Taint, error) {
	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		return []v1.Taint{}, fmt.Errorf("expected GkeMig; got %v", nodeGroup)
	}

	if mig == nil {
		return []v1.Taint{}, fmt.Errorf("no taints found, nodeGroup is nil")
	}

	var crdTaints []v1.Taint

	if mig.Spec() != nil && mig.Spec().Taints != nil {
		for _, t := range mig.Spec().Taints {
			if t.Key == crdLabel {
				crdTaints = append(crdTaints, t)
			}
		}
	}
	if len(crdTaints) > 0 {
		return crdTaints, nil
	}

	// If no crd taints are found check nodeGroup template node.
	node, err := templateNodeFromMig(mig)
	if err != nil {
		return []v1.Taint{}, err
	}
	for _, t := range node.Spec.Taints {
		if t.Key == crdLabel {
			crdTaints = append(crdTaints, t)
		}
	}
	if len(crdTaints) > 0 {
		return crdTaints, nil
	}

	return []v1.Taint{}, fmt.Errorf("crd taint missing for nodeGroup: %v", mig)
}

// templateNodeFromMig returns template node for the given nodeGroup.
func templateNodeFromMig(mig gkeNodeGroup) (*v1.Node, error) {
	nodeInfo, err := mig.TemplateNodeInfo()
	if err != nil {
		return nil, err
	}
	if nodeInfo.Node() == nil {
		return nil, fmt.Errorf("failed to get node from NodeGroup node template info")
	}
	return nodeInfo.Node(), nil
}
