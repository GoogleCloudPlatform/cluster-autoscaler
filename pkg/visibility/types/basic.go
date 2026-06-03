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

package types

import (
	"fmt"
	"reflect"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/utilization"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
)

// PodController contains information about a pod's controller.
type PodController struct {
	Uid        string
	Name       string
	Kind       string
	ApiVersion string
}

// Pod contains information about a pod.
type Pod struct {
	Uid        string
	Name       string
	Namespace  string
	Controller *PodController
	Spec       apiv1.PodSpec
}

// Node contains information about a node.
type Node struct {
	Name     string
	Mig      *GkeMig
	UtilInfo *utilization.Info
}

// GkeMig contains information about a GKE MIG.
type GkeMig struct {
	Id           string
	Name         string
	NodePoolName string
	Zone         string
	Exists       bool
	Spec         *gkeclient.NodePoolSpec
}

// Proto converts the structure to its proto representation.
func (pc *PodController) Proto() *vispb.PodController {
	return &vispb.PodController{
		ApiVersion: pc.ApiVersion,
		Kind:       pc.Kind,
		Name:       pc.Name,
	}
}

// Proto converts the structure to its proto representation.
func (p *Pod) Proto() *vispb.Pod {
	result := &vispb.Pod{
		Name:      p.Name,
		Namespace: p.Namespace,
	}

	if p.Controller != nil {
		result.Controller = p.Controller.Proto()
	}

	return result
}

// ControllerOrPodUid returns the UID of pod's controller, if the pod has a controller,
// and the UID of the pod otherwise.
func (p *Pod) ControllerOrPodUid() string {
	if p.Controller != nil {
		return p.Controller.Uid
	}
	return p.Uid
}

// Proto converts the structure to its proto representation.
func (n *Node) Proto() *vispb.Node {
	result := &vispb.Node{
		Name: n.Name,
	}

	if n.Mig != nil {
		result.Mig = n.Mig.Proto()
	}
	if n.UtilInfo != nil {
		result.CpuRatio = int32(n.UtilInfo.CpuUtil * visibility.UtilizationRatioFloatToIntScaleFactor)
		result.MemRatio = int32(n.UtilInfo.MemUtil * visibility.UtilizationRatioFloatToIntScaleFactor)
	}

	return result
}

// Proto converts the structure to its proto representation.
func (m *GkeMig) Proto() *vispb.Mig {
	return &vispb.Mig{
		Name:     m.Name,
		Zone:     m.Zone,
		Nodepool: m.NodePoolName,
	}
}

// ConvertPod converts a k8s pod to its subset needed for visibility.
func ConvertPod(pod *apiv1.Pod) *Pod {
	result := &Pod{
		Uid:        string(pod.UID),
		Name:       pod.Name,
		Controller: nil,
		Spec:       pod.Spec,
		Namespace:  pod.Namespace,
	}

	for _, ownerRef := range pod.ObjectMeta.OwnerReferences {
		if ownerRef.Controller != nil && *ownerRef.Controller {
			result.Controller = &PodController{
				Uid:        string(ownerRef.UID),
				Name:       ownerRef.Name,
				Kind:       ownerRef.Kind,
				ApiVersion: ownerRef.APIVersion,
			}
			break
		}
	}

	return result
}

// ConvertNode converts a node to its visibility-specific counterpart.
func ConvertNode(node *apiv1.Node, utilInfo *utilization.Info, nodeGroup cloudprovider.NodeGroup) (*Node, error) {
	result := &Node{
		Name:     node.Name,
		UtilInfo: utilInfo,
	}

	if nodeGroup != nil {
		mig, err := ConvertGkeMig(nodeGroup)
		if err != nil {
			return nil, err
		}
		result.Mig = mig
	}

	return result, nil
}

// ConvertGkeMig converts a GKE MIG to its subset needed for visibility.
func ConvertGkeMig(nodeGroup cloudprovider.NodeGroup) (*GkeMig, error) {
	mig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		return nil, fmt.Errorf("unexpected cloudprovider.NodeGroup type, got: %s, want: *gke.GkeMig", reflect.TypeOf(nodeGroup))
	}

	return &GkeMig{
		Id:           mig.Id(),
		Name:         mig.GceRef().Name,
		NodePoolName: mig.NodePoolName(),
		Zone:         mig.GceRef().Zone,
		Exists:       mig.Exist(),
		Spec:         mig.Spec(),
	}, nil
}
