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

package gkedebuggingsnapshot

import (
	"encoding/json"

	"k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/debuggingsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	cr_v1 "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/apis/internal.autoscaling.gke.io/v1"
	v1alpha12 "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/apis/nodemanagement.gke.io/v1alpha1"
	"k8s.io/klog/v2"
)

// GkeDebuggingSnapshot is the data bean to capture all internal and external fields
// This should work with default setters in debuggingsnapshot.DebuggingSnapshot
// since it uses a reference of debuggingsnapshot.DebuggingSnapshotImpl
type GkeDebuggingSnapshot struct {
	// DebuggingSnapshotImpl is the external OSS Data obj which is encapsulated for json formatting
	*debuggingsnapshot.DebuggingSnapshotImpl
	// CapacityRequest stores the list of CapacityRequest CRD
	CapacityRequest []*cr_v1.CapacityRequest `json:"CapacityRequest"`
	// UpdateInfo stores the list of UpdateInfo CRD
	UpdateInfo []*v1alpha12.UpdateInfo `json:"UpdateInfo"`
	// CacheTemplateNodesUsedByNAP is the list of Template nodes last used by NAP and
	// considered for scale up
	CacheTemplateNodesUsedByNAP map[string]*debuggingsnapshot.ClusterNode `json:"TemplateNodesUsedByNAP"`
}

// SetUpdateInfo is the setter for UpdateInfo
// Since the UpdateInfo Fetcher is captured elsewhere
// We call this func at the time in the loop when we want to capture the state
// of update info
func (g *GkeDebuggingSnapshot) SetUpdateInfo(updateInfos []*v1alpha12.UpdateInfo) {
	klog.Infof("Generate UpdateInfo for debugging snapshot")
	if updateInfos == nil {
		return
	}

	g.UpdateInfo = nil
	for _, updateInfo := range updateInfos {
		g.UpdateInfo = append(g.UpdateInfo, updateInfo.DeepCopy())
	}
}

// SetCapacityRequest is the setter func for CapacityRequest
func (g *GkeDebuggingSnapshot) SetCapacityRequest(list []*cr_v1.CapacityRequest) {
	klog.Infof("Setting Capacity Request for debugging snapshot")
	if list == nil {
		return
	}

	g.CapacityRequest = nil
	for _, request := range list {
		g.CapacityRequest = append(g.CapacityRequest, request.DeepCopy())
	}
}

// CacheTemplateNodeLastUsedByNAP is the setter for CacheTemplateNodesUsedByNAP
// This captures the template nodes set last used by NAP for scale up
func (g *GkeDebuggingSnapshot) CacheTemplateNodeLastUsedByNAP(templates map[string]*framework.NodeInfo) {
	if templates == nil {
		return
	}

	g.CacheTemplateNodesUsedByNAP = make(map[string]*debuggingsnapshot.ClusterNode)
	for ng, template := range templates {
		t := debuggingsnapshot.GetClusterNodeCopy(template)
		t = cleanupClusterNode(t)
		g.CacheTemplateNodesUsedByNAP[ng] = t
	}
}

// SetClusterNodes is the override impl of debuggingsnapshot.SetClusterNodes
// This func removes fields un-needed for GKE to optimise memory
func (g *GkeDebuggingSnapshot) SetClusterNodes(nodeInfos []*framework.NodeInfo) {
	if nodeInfos == nil {
		return
	}

	var NodeInfoList []*debuggingsnapshot.ClusterNode

	for _, n := range nodeInfos {
		t := debuggingsnapshot.GetClusterNodeCopy(n)
		t = cleanupClusterNode(t)
		NodeInfoList = append(NodeInfoList, t)
	}
	g.NodeList = NodeInfoList
}

// SetTemplateNodes is the override impl of debuggingsnapshot.SetTemplateNodes
// This func removes fields un-needed for GKE to optimise memory
func (g *GkeDebuggingSnapshot) SetTemplateNodes(templates map[string]*framework.NodeInfo) {
	if templates == nil {
		return
	}

	g.TemplateNodes = make(map[string]*debuggingsnapshot.ClusterNode)
	for ng, template := range templates {
		t := debuggingsnapshot.GetClusterNodeCopy(template)
		t = cleanupClusterNode(t)
		g.TemplateNodes[ng] = t
	}
}

// GetOutputBytes processes all data bean and returns json marshalled output which is
// directly returned by the handler. This is a GKE specific override
func (g *GkeDebuggingSnapshot) GetOutputBytes() ([]byte, bool) {

	errMsgSet := false
	if g.Error != "" {
		klog.Errorf("Debugging snapshot found with error message set when GetOutputBytes() is called: %v", g.Error)
		errMsgSet = true
	}

	klog.Infof("Debugging snapshot flush ready")
	marshalOutput, err := json.Marshal(g)

	// this error captures if the snapshot couldn't be marshalled, hence we create a new object
	// and return the error message
	if err != nil {
		klog.Errorf("Unable to json marshal the debugging snapshot: %v", err)
		errorSnapshot := GkeDebuggingSnapshot{
			DebuggingSnapshotImpl: &debuggingsnapshot.DebuggingSnapshotImpl{},
		}
		errorSnapshot.SetErrorMessage("Unable to marshal the snapshot," + err.Error())
		errorSnapshot.SetEndTimestamp(g.EndTimestamp)
		errorSnapshot.SetStartTimestamp(g.StartTimestamp)
		errorMarshal, err1 := json.Marshal(errorSnapshot)
		klog.Errorf("Unable to marshal a new Debugging Snapshot Impl, with just a error message: %v", err1)
		return errorMarshal, true
	}

	return marshalOutput, errMsgSet
}

// Cleanup is the cleanup func for gke specific override
func (g *GkeDebuggingSnapshot) Cleanup() {
	// the GKE Snapshot is cleaned without changing the pointer reference to the OSS snapshot object.
	// OSS snapshotter continues to work with the older OSS snapshot pointer reference
	ossDebugSnapshot := g.DebuggingSnapshotImpl
	cacheTemplateNodesUsedByNAP := g.CacheTemplateNodesUsedByNAP
	*g = GkeDebuggingSnapshot{}
	g.DebuggingSnapshotImpl = ossDebugSnapshot
	g.CacheTemplateNodesUsedByNAP = cacheTemplateNodesUsedByNAP
	g.DebuggingSnapshotImpl.Cleanup()
}

func cleanupClusterNode(node *debuggingsnapshot.ClusterNode) *debuggingsnapshot.ClusterNode {
	node.Node.SetManagedFields(nil)
	var pods []*v1.Pod
	for _, pod := range node.Pods {
		pod.SetManagedFields(nil)
		var containers []v1.Container
		for _, c := range pod.Spec.Containers {
			containers = append(containers, cleanupPodContainer(c))
		}
		pod.Spec.Containers = containers

		var initContainers []v1.Container
		for _, c := range pod.Spec.InitContainers {
			initContainers = append(initContainers, cleanupPodContainer(c))
		}
		pod.Spec.InitContainers = initContainers
		var ephemeralContainer []v1.EphemeralContainer
		for _, c := range pod.Spec.EphemeralContainers {
			ephemeralContainer = append(ephemeralContainer, cleanupPodEphemeralContainer(c))
		}
		pod.Spec.EphemeralContainers = ephemeralContainer
		pod.Spec.RestartPolicy = ""
		pod.Spec.DNSPolicy = ""
		pod.Spec.SecurityContext = nil
		pod.Spec.ImagePullSecrets = nil
		pod.Spec.HostAliases = nil
		pod.Spec.DNSConfig = nil
		pod.Spec.ReadinessGates = nil
		pod.Spec.EnableServiceLinks = nil
		pods = append(pods, pod)
	}
	node.Pods = pods
	return node
}

func cleanupPodContainer(container v1.Container) v1.Container {
	container.Image = ""
	container.Command = nil
	container.Args = nil
	container.WorkingDir = ""
	container.Ports = nil
	container.EnvFrom = nil
	container.Env = nil
	container.VolumeDevices = nil
	container.VolumeMounts = nil
	container.LivenessProbe = nil
	container.ReadinessProbe = nil
	container.Lifecycle = nil
	container.TerminationMessagePath = ""
	container.TerminationMessagePolicy = ""
	container.ImagePullPolicy = ""
	container.SecurityContext = nil

	return container
}

func cleanupPodEphemeralContainer(container v1.EphemeralContainer) v1.EphemeralContainer {
	container.Image = ""
	container.Command = nil
	container.Args = nil
	container.WorkingDir = ""
	container.Ports = nil
	container.EnvFrom = nil
	container.Env = nil
	container.VolumeDevices = nil
	container.VolumeMounts = nil
	container.LivenessProbe = nil
	container.ReadinessProbe = nil
	container.Lifecycle = nil
	container.TerminationMessagePath = ""
	container.TerminationMessagePolicy = ""
	container.ImagePullPolicy = ""
	container.SecurityContext = nil

	return container
}
