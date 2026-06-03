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

package gke

import (
	"fmt"
	"math/rand"
	"strconv"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	labelUtils "k8s.io/autoscaler/cluster-autoscaler/utils/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	networkingutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking/util"
)

const (
	HugepageSize1gResourceName apiv1.ResourceName = "hugepages-1Gi"
	HugepageSize2mResourceName apiv1.ResourceName = "hugepages-2Mi"
)

// GkeTemplateBuilder builds templates for GKE cloud provider.
type GkeTemplateBuilder struct {
	gce.GceTemplateBuilder
	machineSerenityLabelsEnabled bool
}

// BuildNodeFromMigSpec builds node based on MIG's spec.
func (t *GkeTemplateBuilder) BuildNodeFromMigSpec(mig *GkeMig, migOsInfo *GkeMigOsInfo, cpu int64, mem int64, pods *int64,
	daemonSetConditions *DaemonSetConditions, gcfsEnabled bool, reserved gce.OsReservedCalculator, ssdSizeProvider localssdsize.LocalSSDSizeProvider,
	defaultMaxPodsPerNode int64) (*apiv1.Node, error) {
	if mig.Spec() == nil {
		return nil, fmt.Errorf("no spec in mig %s", mig.GceRef().Name)
	}

	maxPodsPerNode := defaultMaxPodsPerNode
	if mig.Spec() != nil && mig.Spec().MaxPodsPerNode != 0 {
		maxPodsPerNode = mig.Spec().MaxPodsPerNode
	}
	if pods == nil && maxPodsPerNode != 0 {
		pods = &maxPodsPerNode
	}
	node := apiv1.Node{}
	nodeName := fmt.Sprintf("%s-autoprovisioned-template-%d", mig.GceRef().Name, rand.Int63())

	node.ObjectMeta = metav1.ObjectMeta{
		Name:     nodeName,
		SelfLink: fmt.Sprintf("/api/v1/nodes/%s", nodeName),
		Labels:   map[string]string{},
	}

	var ephemeralLocalSsdCount int64
	var err error
	ephemeralStorageGiB := mig.Spec().DiskSize
	if localSSDCfg := mig.Spec().LocalSSDConfig; localSSDCfg != nil && localSSDCfg.EphemeralStorageConfig != nil {
		ephemeralLocalSsdCount = localSSDCfg.EphemeralStorageConfig.LocalSsdCount
	}
	if ephemeralLocalSsdCount > 0 {
		ssdSize := ssdSizeProvider.SSDSizeInGiB(mig.Spec().MachineType)
		ephemeralStorageGiB = ephemeralLocalSsdCount * int64(ssdSize)
	}

	labels, err := buildLabelsForAutoprovisionedMig(mig, nodeName, migOsInfo.Os(), ephemeralLocalSsdCount, maxPodsPerNode, t.machineSerenityLabelsEnabled)
	if err != nil {
		return nil, err
	}
	gkeDaemonSetLabels := getGKEDaemonSetLabels(daemonSetConditions, migOsInfo.Os())
	for k, v := range gkeDaemonSetLabels {
		labels[k] = v
	}
	node.Labels = labels

	extendedResources := make(apiv1.ResourceList)
	if hugepage1g := mig.GetHugepageSize1gBytes(); hugepage1g > 0 {
		extendedResources[HugepageSize1gResourceName] = *resource.NewQuantity(hugepage1g, resource.DecimalSI)
	}
	if hugepage2m := mig.GetHugepageSize2mBytes(); hugepage2m > 0 {
		extendedResources[HugepageSize2mResourceName] = *resource.NewQuantity(hugepage2m, resource.DecimalSI)
	}

	capacity, err := t.BuildCapacity(migOsInfo, cpu, mem, nil, ephemeralStorageGiB*GiB, ephemeralLocalSsdCount, pods, reserved, extendedResources)
	if err != nil {
		return nil, err
	}

	if !dynamicresources.GpuDraDriverEnabled(&node) {
		if gpuRequest, found := mig.extraResources[gpu.ResourceNvidiaGPU]; found {
			capacity[gpu.ResourceNvidiaGPU] = gpuRequest.DeepCopy()
		}
	}

	if !dynamicresources.TpuDraDriverEnabled(&node) {
		if tpuRequest, found := mig.extraResources[tpu.ResourceGoogleTPU]; found {
			capacity[tpu.ResourceGoogleTPU] = tpuRequest.DeepCopy()
		}
	}

	if podCapacity, found := mig.extraResources[gkelabels.PodCapacityLabel]; found {
		capacity[gkelabels.PodCapacityLabel] = podCapacity.DeepCopy()
	}

	for key, val := range mig.extraResources {
		if networkingutils.IsNetworkResource(key) {
			capacity[apiv1.ResourceName(key)] = val.DeepCopy()
		}
	}

	kubeReserved := t.BuildKubeReserved(cpu, mem, mig.Spec().MachineType, ephemeralStorageGiB, gcfsEnabled, ephemeralLocalSsdCount, maxPodsPerNode)
	evictionHard := gce.ParseEvictionHardOrGetDefault(nil)

	node.Status = apiv1.NodeStatus{
		Capacity:    capacity,
		Allocatable: t.CalculateAllocatable(capacity, kubeReserved, evictionHard),
	}
	if mig.Spec().NodeVersion != "" {
		node.Status.NodeInfo = apiv1.NodeSystemInfo{
			KubeletVersion: mig.Spec().NodeVersion,
		}
	}

	addBootDiskAnnotation(&node, mig.Spec().DiskSize, mig.Spec().DiskType)
	addEphemeralLocalSsdAnnotation(&node, ephemeralLocalSsdCount)

	node.Spec.Taints = mig.Spec().Taints

	// Ready status
	node.Status.Conditions = cloudprovider.BuildReadyConditions()
	return &node, nil
}

// BuildKubeReserved builds kube reserved resources based on node physical resources.
// See calculateReserved for more details
func (t *GkeTemplateBuilder) BuildKubeReserved(cpu, physicalMemory int64, machineType string, ephemeralStorageGiB int64,
	gcfsEnabled bool, ephemeralLocalSsdCount int64, maxPodsPerNode int64) apiv1.ResourceList {
	cpuReservedMillicores := PredictKubeReservedCpuMillicores(cpu*1000, machineType, maxPodsPerNode)
	memoryReserved := PredictKubeReservedMemory(physicalMemory, gcfsEnabled)
	reserved := apiv1.ResourceList{}
	reserved[apiv1.ResourceCPU] = *resource.NewMilliQuantity(cpuReservedMillicores, resource.DecimalSI)
	reserved[apiv1.ResourceMemory] = *resource.NewQuantity(memoryReserved, resource.BinarySI)

	var ephemeralStorageReserved int64
	if ephemeralLocalSsdCount > 0 {
		ephemeralStorageReserved = PredictEphemeralLocalSsdKubeReservedEphemeralStorage(ephemeralLocalSsdCount)
	} else if ephemeralStorageGiB > 0 {
		ephemeralStorageReserved = PredictKubeReservedEphemeralStorage(ephemeralStorageGiB)
	}
	if ephemeralStorageReserved > 0 {
		reserved[apiv1.ResourceEphemeralStorage] = *resource.NewQuantity(ephemeralStorageReserved, resource.DecimalSI)
	}

	return reserved
}

func buildLabelsForAutoprovisionedMig(mig *GkeMig, nodeName string, os gce.OperatingSystem, ephemeralLocalSsdCount, maxPodsPerNode int64, includeMachineSerenityLabels bool) (map[string]string, error) {
	// GenericLabels
	labels, err := gce.BuildGenericLabels(mig.GceRef(), mig.Spec().MachineType, nodeName, os, *mig.Spec().SystemArchitecture)
	if err != nil {
		return nil, err
	}
	labelUtils.UpdateDeprecatedLabels(labels)

	// Add deprecated arch and os labels
	gkelabels.UpdateDeprecatedLabels(labels)

	machineFamily, err := mig.MachineConfigProvider().GetMachineFamilyFromMachineName(mig.Spec().MachineType)
	if err != nil {
		return nil, fmt.Errorf("could not get machine family for %q: %v", mig.Spec().MachineType, err)
	}

	labels[gkelabels.MachineFamilyLabel] = machineFamily.Name()

	if includeMachineSerenityLabels {
		for _, diskType := range machineFamily.ListSupportedDisks(mig.IsConfidentialNode()) {
			labels[gkelabels.SupportedDiskTypeKey(diskType)] = "true"
		}
	}

	for k, v := range mig.Spec().Labels {
		if existingValue, found := labels[k]; found {
			if v != existingValue {
				return map[string]string{}, fmt.Errorf("conflict in labels requested: %s=%s  present: %s=%s",
					k, v, k, existingValue)
			}
		} else {
			labels[k] = v
		}
	}
	if ephemeralLocalSsdCount > 0 {
		labels[gkelabels.EphemeralLocalSsdLabel] = gkelabels.EphemeralLocalSsdEnabledValue
	}
	labels[gkelabels.MaxPodsPerNodeLabel] = fmt.Sprintf("%d", maxPodsPerNode)
	return labels, nil
}

func getGKEDaemonSetLabels(conditions *DaemonSetConditions, os gce.OperatingSystem) map[string]string {
	labels := make(map[string]string)
	if conditions.MetadataServerEnabled {
		labels["iam.gke.io/gke-metadata-server-enabled"] = "true"
	}
	if conditions.NodeLocalDNSEnabled && os != gce.OperatingSystemWindows {
		labels["addon.gke.io/node-local-dns-ds-ready"] = "true"
	}
	if conditions.HighThroughputLoggingEnabled {
		labels["cloud.google.com/gke-logging-variant"] = gkeclient.MaxThroughputLoggingEnabledLabel
	}
	if conditions.NetdEnabled {
		labels[gkelabels.NetdLabel] = gkelabels.NetdValue
	}
	if conditions.IpMasqAgentEnabled {
		labels[gkelabels.IpMasqAgentLabel] = gkelabels.IpMasqAgentValue
	}
	return labels
}

func addBootDiskAnnotation(node *apiv1.Node, bootDiskSize int64, bootDiskType string) {
	addAnnotation(node, gce.BootDiskSizeAnnotation, strconv.FormatInt(bootDiskSize, 10))
	if bootDiskType != "" {
		addAnnotation(node, gce.BootDiskTypeAnnotation, bootDiskType)
	}
}

func addEphemeralLocalSsdAnnotation(node *apiv1.Node, ephemeralLocalSsdCount int64) {
	if ephemeralLocalSsdCount > 0 {
		addAnnotation(node, gce.EphemeralStorageLocalSsdAnnotation, "true")
		addAnnotation(node, gce.LocalSsdCountAnnotation, strconv.FormatInt(ephemeralLocalSsdCount, 10))
	}
}

func addAnnotation(node *apiv1.Node, key, value string) {
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations[key] = value
}
