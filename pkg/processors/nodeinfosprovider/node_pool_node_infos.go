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
	"reflect"
	"sort"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors"
	"k8s.io/klog/v2"
)

// UpdateNodeInfosWithinNodePools updates nodeInfos generated from template nodes with
// relevant information from nodeInfos generated from real nodes from the same node pool.
// This can help solve problems with scaling a node group from 0 if there are some non-0
// node groups in the same node pool.
func UpdateNodeInfosWithinNodePools(ctx *context.AutoscalingContext, nodeInfos map[string]*framework.NodeInfo) (map[string]*framework.NodeInfo, errors.AutoscalerError) {
	gkeCloudProvider, ok := ctx.CloudProvider.(processors.ProcessorsCloudProvider)
	if !ok {
		klog.Errorf("Unexpected cloudprovider.CloudProvider type, got: %s, want: ProcessorsCloudProvider", reflect.TypeOf(ctx.CloudProvider))
		return nodeInfos, nil
	}

	nodeGroups := gkeCloudProvider.NodeGroups()
	for _, nodeGroup := range nodeGroups {
		if !nodeGroup.Exist() {
			continue
		}

		if nodeInfo, found := nodeInfos[nodeGroup.Id()]; found && isNodeInfoReal(nodeInfo) {
			continue
		}

		mig, ok := nodeGroup.(*gke.GkeMig)
		if !ok {
			return nil, errors.NewAutoscalerErrorf(errors.InternalError, "unexpected cloudprovider.NodeGroup type, got: %s, want: *gke.GkeMig", reflect.TypeOf(nodeGroup))
		}

		candidateNodeInfo, err := getCandidateNodeInfoForNodePool(gkeCloudProvider, nodeGroups, nodeInfos, mig)
		if err != nil {
			return nil, errors.NewAutoscalerErrorf(errors.InternalError, "error occureced while getting the candidate node info for node group %v: %v", nodeGroup.Id(), err)
		}

		if candidateNodeInfo != nil {
			augmentedNodeInfo, err := createAugmentedTemplateNodeInfo(nodeInfos[nodeGroup.Id()], candidateNodeInfo)
			if err != nil {
				return nil, err
			}
			nodeInfos[nodeGroup.Id()] = augmentedNodeInfo
		}
	}

	return nodeInfos, nil
}

func isNodeInfoReal(nodeInfo *framework.NodeInfo) bool {
	_, found := nodeInfo.Node().Annotations[labels.NodeGeneratedFromTemplateAnnotation]
	return !found
}

func getNodePoolNodeGroups(nodePoolName string, nodeGroups []cloudprovider.NodeGroup) ([]*gke.GkeMig, errors.AutoscalerError) {
	var result []*gke.GkeMig

	for _, nodeGroup := range nodeGroups {
		if !nodeGroup.Exist() {
			continue
		}

		mig, ok := nodeGroup.(*gke.GkeMig)
		if !ok {
			return nil, errors.NewAutoscalerErrorf(errors.InternalError, "unexpected cloudprovider.NodeGroup type, got: %s, want: *gke.GkeMig", reflect.TypeOf(nodeGroup))
		}

		if mig.NodePoolName() == nodePoolName {
			result = append(result, mig)
		}
	}

	return result, nil
}

func getCandidateNodeInfoForNodePool(provider processors.ProcessorsCloudProvider, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo, mig *gke.GkeMig) (*framework.NodeInfo, error) {
	var candidateMigs []*gke.GkeMig

	if np := mig.NodePool(); np != nil {
		candidateMigs = np.Migs()
	} else {
		klog.Warningf("GkeMig.NodePool is nil for %s, falling back to O(N) lookup. This should not happen in production.", mig.Id())
		nodePoolNodeGroups, err := getNodePoolNodeGroups(mig.NodePoolName(), nodeGroups)
		if err != nil {
			return nil, err
		}
		candidateMigs = append(candidateMigs, nodePoolNodeGroups...)
	}

	for _, otherMig := range candidateMigs {
		if !otherMig.Exist() || !areTemplatesSimilar(provider, mig, otherMig) {
			continue
		}

		if nodeInfo, found := nodeInfos[otherMig.Id()]; found && isNodeInfoReal(nodeInfo) {
			return nodeInfo, nil
		}
	}
	return nil, nil
}

func areTemplatesSimilar(provider processors.ProcessorsCloudProvider, mig *gke.GkeMig, otherMig *gke.GkeMig) bool {
	templateLabels, labelsErr := provider.GetMigInstanceTemplateLabels(mig)
	otherTemplateLabels, otherLabelsErr := provider.GetMigInstanceTemplateLabels(otherMig)
	templateTaints, taintsErr := provider.GetMigInstanceTemplateTaints(mig)
	otherTemplateTaints, otherTaintsErr := provider.GetMigInstanceTemplateTaints(otherMig)

	if labelsErr != nil || otherLabelsErr != nil || taintsErr != nil || otherTaintsErr != nil {
		return false
	}

	sort.Slice(templateTaints, func(i, j int) bool {
		return templateTaints[i].ToString() < templateTaints[j].ToString()
	})
	sort.Slice(otherTemplateTaints, func(i, j int) bool {
		return otherTemplateTaints[i].ToString() < otherTemplateTaints[j].ToString()
	})

	return reflect.DeepEqual(templateLabels, otherTemplateLabels) && reflect.DeepEqual(templateTaints, otherTemplateTaints)
}

func createAugmentedTemplateNodeInfo(templateNodeInfo *framework.NodeInfo, realNodeInfo *framework.NodeInfo) (*framework.NodeInfo, errors.AutoscalerError) {
	resultNode := realNodeInfo.Node().DeepCopy()

	// Overwrite the name, UID, annotations and labels (including e.g. the zone label) with values from the template.
	resultNode.Name = templateNodeInfo.Node().Name
	resultNode.UID = templateNodeInfo.Node().UID
	if resultNode.Labels == nil {
		resultNode.Labels = make(map[string]string)
	}
	for k, v := range templateNodeInfo.Node().Labels {
		resultNode.Labels[k] = v
	}
	if resultNode.Annotations == nil {
		resultNode.Annotations = make(map[string]string)
	}
	for k, v := range templateNodeInfo.Node().Annotations {
		resultNode.Annotations[k] = v
	}

	// It's better to use the template nodeInfo's DS pods in case some of them have a zone selector.
	var pods []*framework.PodInfo
	for _, podInfo := range templateNodeInfo.Pods() {
		pods = append(pods, framework.NewPodInfo(podInfo.Pod, nil))
	}
	result := framework.NewNodeInfo(resultNode, nil, pods...)

	return result, nil
}
