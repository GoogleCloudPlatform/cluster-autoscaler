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

package nodeinfosprovider

import (
	"errors"
	"reflect"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/klog/v2"
	taintsutil "k8s.io/kubernetes/pkg/util/taints"
)

const (
	lastAppliedLabelsKey = "node.gke.io/last-applied-node-labels"
	lastAppliedTaintsKey = "node.gke.io/last-applied-node-taints"
)

// HandleNodePoolUpdates handles non-disruptive node-pool updates. During the node pool updates
// nodes are processed sequentially, which means that they may not accurately represent MIG at
// all times. To handle that we simulate a 3-way merge algorithm used for labels and taints updates,
// with old settings maintained in the nodes annotations and templates being the new source of truth.
func HandleNodePoolUpdates(ctx *context.AutoscalingContext, nodeInfos map[string]*framework.NodeInfo, taintConfig taints.TaintConfig) map[string]*framework.NodeInfo {
	gkeCloudProvider, ok := ctx.CloudProvider.(processors.ProcessorsCloudProvider)
	if !ok {
		klog.Errorf("Unexpected cloudprovider.CloudProvider type, got: %s, want: ProcessorsCloudProvider", reflect.TypeOf(ctx.CloudProvider))
		return nodeInfos
	}

	nodeGroups := gkeCloudProvider.NodeGroups()
	for _, nodeGroup := range nodeGroups {
		originalTemplate, found := nodeInfos[nodeGroup.Id()]
		if !nodeGroup.Exist() || !found || !isNodeInfoReal(originalTemplate) {
			continue
		}

		mig, ok := nodeGroup.(*gke.GkeMig)
		if !ok {
			klog.Errorf("Unexpected cloudprovider.NodeGroup type, got: %s, want: *gke.GkeMig", reflect.TypeOf(nodeGroup))
			continue
		}

		desiredLabels, err := gkeCloudProvider.GetMigInstanceTemplateLabels(mig)
		if err != nil {
			klog.Errorf("Error occurred while fetching template labels for node group %v: %v", nodeGroup.Id(), err)
			continue
		}

		originalTemplate, err = updateLabels(nodeGroup, originalTemplate, desiredLabels)
		if err != nil {
			klog.Errorf("Error occurred while updating labels for node group %v: %v", nodeGroup.Id(), err)
			continue
		}

		desiredTaints, err := gkeCloudProvider.GetMigInstanceTemplateTaints(mig)
		if err != nil {
			klog.Errorf("Error occurred while fetching template taints for node group %v: %v", nodeGroup.Id(), err)
			continue
		}

		originalTemplate, err = updateTaints(nodeGroup, originalTemplate, desiredTaints)
		if err != nil {
			klog.Errorf("Error occurred while updating taints for node group %v: %v", nodeGroup.Id(), err)
			continue
		}

		// Sanitize the template node info again in case some taints need to be filtered out after updateTaints() above.
		// forceDaemonSets is set to false because the forcing should've already been done on originalTemplate - no need
		// to do it again.
		sanitizedTemplate, err := simulator.SanitizedTemplateNodeInfoFromNodeInfo(originalTemplate, mig.Id(), nil, false, taintConfig)
		if err != nil {
			klog.Errorf("Error occurred while sanitizing NodeInfo for node group %v: %v", nodeGroup.Id(), err)
		}
		nodeInfos[nodeGroup.Id()] = sanitizedTemplate
	}

	return nodeInfos
}

func updateLabels(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, desiredLabels map[string]string) (*framework.NodeInfo, error) {
	lastAppliedLabels, found, err := getLastAppliedLabels(nodeInfo)
	if err != nil {
		return nil, err
	}
	if !found {
		klog.Warningf("Last applied labels annotation not found for node group %v", nodeGroup.Id())
		return nodeInfo, nil
	}

	// Merge labels by:
	// 1. delete last applied labels
	// 2. add/update desired labels
	node := nodeInfo.Node().DeepCopy()
	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}
	for key := range lastAppliedLabels {
		delete(node.Labels, key)
	}
	for key, value := range desiredLabels {
		node.Labels[key] = value
	}

	nodeInfo.SetNode(node)
	return nodeInfo, nil
}

func updateTaints(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, desiredTaints []v1.Taint) (*framework.NodeInfo, error) {
	lastAppliedTaints, found, err := getLastAppliedTaints(nodeInfo)
	if err != nil {
		return nil, err
	}
	if !found {
		klog.Infof("Last applied taints annotation not found for node group %v", nodeGroup.Id())
		return nodeInfo, nil
	}

	// Merge taints by:
	// 1. delete last applied taints
	// 2. add/update desired taints
	node := nodeInfo.Node().DeepCopy()
	for _, taint := range lastAppliedTaints {
		node.Spec.Taints, _ = taintsutil.DeleteTaint(node.Spec.Taints, &taint)
	}
	for _, taint := range desiredTaints {
		updated := false
		for i := range node.Spec.Taints {
			if taint.MatchTaint(&node.Spec.Taints[i]) {
				node.Spec.Taints[i] = taint
				updated = true
				break
			}
		}
		if !updated {
			node.Spec.Taints = append(node.Spec.Taints, taint)
		}
	}

	nodeInfo.SetNode(node)
	return nodeInfo, nil
}

func getLastAppliedLabels(nodeInfo *framework.NodeInfo) (map[string]string, bool, error) {
	node := nodeInfo.Node()
	if node.Annotations == nil {
		return nil, false, nil
	}

	annotation, found := node.Annotations[lastAppliedLabelsKey]
	if annotation == "" {
		return nil, found, nil
	}

	labels := make(map[string]string)
	for _, label := range strings.Split(annotation, ",") {
		split := strings.Split(label, "=")
		if len(split) != 2 {
			return nil, true, errors.New("malformed last applied labels annotation")
		}
		labels[split[0]] = split[1]
	}
	return labels, true, nil
}

func getLastAppliedTaints(nodeInfo *framework.NodeInfo) ([]v1.Taint, bool, error) {
	node := nodeInfo.Node()
	if node.Annotations == nil {
		return nil, false, nil
	}
	annotation, found := node.Annotations[lastAppliedTaintsKey]
	if annotation == "" {
		return nil, found, nil
	}

	taints, _, err := taintsutil.ParseTaints(strings.Split(annotation, ","))
	return taints, true, err
}
