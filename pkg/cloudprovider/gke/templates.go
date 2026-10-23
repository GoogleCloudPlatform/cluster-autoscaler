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
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"

	gce_api "google.golang.org/api/compute/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"

	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"

	networkingutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking/util"
	"k8s.io/klog/v2"
	"k8s.io/utils/cpuset"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"

	labelUtils "sigs.k8s.io/cluster-autoscaler/pkg/utils/labels"
)

const (
	HugepageSize1gResourceName            apiv1.ResourceName = "hugepages-1Gi"
	HugepageSize2mResourceName            apiv1.ResourceName = "hugepages-2Mi"
	ephemeralLocalSsdCapacityGbKubeEnvVar                    = "EPHEMERAL_LOCAL_SSD_CAPACITY_GB"
)

// lssdOverheadUnitGiB is the 375 GiB unit size used to index EphemeralStorageOnLocalSSDFilesystemOverheadInBytes.
const lssdOverheadUnitGiB = 375

func mixedModeEphemeralStorageCapacityBytes(ctx context.Context, ephemeralCapacityGiB int64, osDistribution gce.OperatingSystemDistribution) int64 {
	if ephemeralCapacityGiB <= 0 {
		return 0
	}
	unitCount := int64(math.Ceil(float64(ephemeralCapacityGiB) / float64(lssdOverheadUnitGiB)))
	fullOverhead := gce.EphemeralStorageOnLocalSSDFilesystemOverheadInBytes(ctx, unitCount, osDistribution)
	partitionOverhead := fullOverhead
	if unitCount <= 8 {
		partitionOverhead = int64(math.Ceil(float64(fullOverhead) * float64(ephemeralCapacityGiB) / float64(unitCount*lssdOverheadUnitGiB)))
	}
	return max(ephemeralCapacityGiB*GiB-partitionOverhead, 0)
}

func extractEvictionHardFromKubeEnv(kubeEnv gce.KubeEnv) map[string]string {
	autoscalerVals, found := kubeEnv.Var("AUTOSCALER_ENV_VARS")
	if !found || strings.TrimSpace(autoscalerVals) == "" {
		return nil
	}
	for _, entry := range strings.Split(autoscalerVals, ";") {
		entry = strings.TrimSpace(entry)
		items := strings.SplitN(entry, "=", 2)
		if len(items) != 2 {
			return nil
		}
		if strings.TrimSpace(items[0]) == "evictionHard" {
			evictionHardStr := strings.Trim(items[1], " \"'")
			if evictionHardStr == "" {
				return make(map[string]string)
			}
			result := make(map[string]string)
			for _, keyValue := range strings.Split(evictionHardStr, ",") {
				parts := strings.SplitN(keyValue, "=", 2)
				if len(parts) != 2 {
					return nil
				}
				result[parts[0]] = parts[1]
			}
			return result
		}
	}
	return nil
}

// GkeTemplateBuilder builds templates for GKE cloud provider.
type GkeTemplateBuilder struct {
	gce.GceTemplateBuilder
	machineSerenityLabelsEnabled      bool
	defaultReservedResourcesV2Enabled bool
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

	localSSDCfg := mig.Spec().LocalSSDConfig
	var ephemeralLocalSsdCount int64
	ephemeralStorageGiB := mig.Spec().DiskSize
	switch {
	case localSSDCfg.IsBlockMode():
		// Ephemeral storage stays on the boot disk.
	case localSSDCfg != nil && localSSDCfg.EphemeralStorageLocalSsdConfig != nil:
		ephemeralLocalSsdCount = localSSDCfg.EphemeralStorageLocalSsdConfig.LocalSsdCount
		if ephemeralLocalSsdCount == 0 {
			if c, found, err := mig.MachineConfigProvider().AutomaticEphemeralLocalSsdCountByMachineType(mig.Spec().MachineType); found && err == nil {
				ephemeralLocalSsdCount = c
			}
		}
	case localSSDCfg != nil && localSSDCfg.EphemeralStorageConfig != nil:
		ephemeralLocalSsdCount = localSSDCfg.EphemeralStorageConfig.LocalSsdCount
	}
	if localSSDCfg.IsMixedMode() {
		// The node sizes the ephemeral partition in GiB from ephemeralCapacityGb.
		ephemeralStorageGiB = localSSDCfg.GetEphemeralCapacityGb()
	} else if ephemeralLocalSsdCount > 0 {
		ssdSize := ssdSizeProvider.SSDSizeInGiB(mig.Spec().MachineType)
		ephemeralStorageGiB = ephemeralLocalSsdCount * int64(ssdSize)
	}

	labels, err := buildLabelsForAutoprovisionedMig(mig, nodeName, migOsInfo.Os(), ephemeralLocalSsdCount, maxPodsPerNode, t.machineSerenityLabelsEnabled)
	if err != nil {
		return nil, err
	}
	// Determine per-MIG riptide state: check CCC self-service override, fallback to cluster default.
	riptideEnabled := gcfsEnabled
	if mig.Spec().SelfServiceMetadata != nil {
		if val, ok := mig.Spec().SelfServiceMetadata[gkelabels.ImageStreamingLabelKey]; ok {
			riptideEnabled = (val == "true")
		}
	}
	gkeDaemonSetLabels := getGKEDaemonSetLabels(daemonSetConditions, migOsInfo.Os(), mig.Version(), riptideEnabled)
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

	var version string
	var osDistribution gce.OperatingSystemDistribution
	if migOsInfo != nil {
		version = migOsInfo.NodeVersion()
		osDistribution = migOsInfo.OsDistribution()
	}

	capacityLocalSsdCount := ephemeralLocalSsdCount
	if localSSDCfg.IsMixedMode() {
		capacityLocalSsdCount = 0
	}
	capacity, err := t.BuildCapacity(context.TODO(), migOsInfo, cpu, mem, nil, ephemeralStorageGiB*GiB, capacityLocalSsdCount, pods, reserved, extendedResources)
	if err != nil {
		return nil, err
	}
	if localSSDCfg.IsMixedMode() {
		capacity[apiv1.ResourceEphemeralStorage] = *resource.NewQuantity(
			mixedModeEphemeralStorageCapacityBytes(context.TODO(), ephemeralStorageGiB, osDistribution),
			resource.DecimalSI,
		)
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

	var effectiveCpu, effectiveMemory int64
	if kc := mig.Spec().KubeletConfig; kc != nil {
		if rrc := kc.ReservedResourcesConfig; rrc != nil {
			effectiveCpu = rrc.EffectiveCpuReservedMillicore
			if effectiveCpu == 0 {
				effectiveCpu = rrc.CpuReservedMillicore
			}
			effectiveMemory = rrc.EffectiveMemoryReservedMib
			if effectiveMemory == 0 {
				effectiveMemory = rrc.MemoryReservedMib
			}
		}
		// kubelet overrides kube/system-reserved cpu with the size of --reserved-cpus (whole cores).
		if kc.ReservedSystemCpus != "" {
			if set, err := cpuset.Parse(kc.ReservedSystemCpus); err == nil {
				effectiveCpu = int64(set.Size()) * 1000
			} else {
				klog.Warningf("Ignoring unparsable reservedSystemCpus %q for %s: %v", kc.ReservedSystemCpus, mig.GceRef().Name, err)
			}
		}
	}
	// In mixed mode, GKE sizes kube-reserved from the ephemeral partition, not the disk count.
	kubeReservedLocalSsdCount := ephemeralLocalSsdCount
	if localSSDCfg.IsMixedMode() {
		kubeReservedLocalSsdCount = 0
	}
	kubeReserved := t.BuildKubeReserved(cpu, mem, mig.Spec().MachineType, ephemeralStorageGiB, riptideEnabled, kubeReservedLocalSsdCount, maxPodsPerNode, effectiveCpu, effectiveMemory, version, osDistribution)
	evictionHard := gce.ParseEvictionHardOrGetDefault(context.TODO(), nil)

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

// BuildNodeFromTemplate builds node based on GCE instance template and kube-env.
func (t *GkeTemplateBuilder) BuildNodeFromTemplate(ctx context.Context, mig gce.Mig, migOsInfo gce.MigOsInfo, template *gce_api.InstanceTemplate, kubeEnv gce.KubeEnv, cpu int64, mem int64, pods *int64, reserved gce.OsReservedCalculator, localSSDSizeProvider localssdsize.LocalSSDSizeProvider) (*apiv1.Node, error) {
	node, err := t.GceTemplateBuilder.BuildNodeFromTemplate(ctx, mig, migOsInfo, template, kubeEnv, cpu, mem, pods, reserved, localSSDSizeProvider)
	if err != nil {
		return nil, err
	}
	rawCapGb, found := kubeEnv.Var(ephemeralLocalSsdCapacityGbKubeEnvVar)
	if !found || strings.TrimSpace(rawCapGb) == "" {
		return node, nil
	}
	ephemeralCapacityGb, err := strconv.ParseInt(strings.TrimSpace(rawCapGb), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s=%q: %w", ephemeralLocalSsdCapacityGbKubeEnvVar, rawCapGb, err)
	}
	if ephemeralCapacityGb < 0 {
		return nil, fmt.Errorf("%s must be non-negative, got %d", ephemeralLocalSsdCapacityGbKubeEnvVar, ephemeralCapacityGb)
	}
	if ephemeralCapacityGb == 0 {
		return node, nil
	}

	var osDist gce.OperatingSystemDistribution
	if migOsInfo != nil {
		osDist = migOsInfo.OsDistribution()
	}
	ephemeralCapBytes := mixedModeEphemeralStorageCapacityBytes(ctx, ephemeralCapacityGb, osDist)
	node.Status.Capacity[apiv1.ResourceEphemeralStorage] = *resource.NewQuantity(ephemeralCapBytes, resource.DecimalSI)

	evictionHardMap := extractEvictionHardFromKubeEnv(kubeEnv)
	evictionHard := gce.ParseEvictionHardOrGetDefault(ctx, evictionHardMap)
	if allocatable, err := t.BuildAllocatableFromKubeEnv(ctx, node.Status.Capacity, kubeEnv, evictionHard); err == nil {
		node.Status.Allocatable = allocatable
	} else {
		node.Status.Allocatable = node.Status.Capacity.DeepCopy()
	}
	return node, nil
}

// BuildKubeReserved builds kube reserved resources based on node physical resources.
// See calculateReserved for more details
func (t *GkeTemplateBuilder) BuildKubeReserved(cpu, physicalMemory int64, machineType string, ephemeralStorageGiB int64,
	gcfsEnabled bool, ephemeralLocalSsdCount int64, maxPodsPerNode int64, effectiveCpu, effectiveMemory int64,
	version string, osDistribution gce.OperatingSystemDistribution) apiv1.ResourceList {

	var cpuReservedMillicores int64
	var memoryReserved int64

	if effectiveCpu > 0 {
		cpuReservedMillicores = effectiveCpu
	} else {
		cpuReservedMillicores = PredictKubeReservedCpuMillicores(cpu*1000, machineType, maxPodsPerNode, version, osDistribution, t.defaultReservedResourcesV2Enabled)
	}

	if effectiveMemory > 0 {
		memoryReserved = effectiveMemory * MiB
	} else {
		memoryReserved = PredictKubeReservedMemory(physicalMemory, gcfsEnabled, version, osDistribution, maxPodsPerNode, t.defaultReservedResourcesV2Enabled)
	}

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

	localSSDCfg := mig.Spec().LocalSSDConfig
	if localSSDCfg.IsBlockMode() {
		labels[gkelabels.LocalNvmeSsdBlockLabel] = gkelabels.LocalNvmeSsdBlockValue
	} else if localSSDCfg.IsMixedMode() && ephemeralLocalSsdCount > 0 {
		labels[gkelabels.EphemeralLocalSsdLabel] = gkelabels.EphemeralLocalSsdEnabledValue
		labels[gkelabels.LocalNvmeSsdPartitionLabel] = gkelabels.LocalNvmeSsdPartitionValue
	} else if ephemeralLocalSsdCount > 0 {
		labels[gkelabels.EphemeralLocalSsdLabel] = gkelabels.EphemeralLocalSsdEnabledValue
	}

	for k, v := range mig.Spec().Labels {
		if localSSDCfg.IsBlockMode() && k == gkelabels.EphemeralLocalSsdLabel {
			return map[string]string{}, fmt.Errorf("conflict in labels requested: %s=%s cannot be set in block mode", k, v)
		}
		if existingValue, found := labels[k]; found {
			if v != existingValue {
				return map[string]string{}, fmt.Errorf("conflict in labels requested: %s=%s  present: %s=%s",
					k, v, k, existingValue)
			}
		} else {
			labels[k] = v
		}
	}
	labels[gkelabels.MaxPodsPerNodeLabel] = fmt.Sprintf("%d", maxPodsPerNode)
	return labels, nil
}

func getGKEDaemonSetLabels(conditions *DaemonSetConditions, os gce.OperatingSystem, nodeVersion string, riptideEnabled bool) map[string]string {
	labels := make(map[string]string)
	if conditions.MetadataServerEnabled {
		labels["iam.gke.io/gke-metadata-server-enabled"] = "true"
	}

	if conditions.WIImagePullMinVersion != nil && !(conditions.DisableWIImagePullForRiptide && riptideEnabled) {
		nv, err := version.FromString(nodeVersion)
		if err == nil {
			if !nv.LessThan(*conditions.WIImagePullMinVersion) && conditions.WorkloadIdentityEnabled {
				labels["iam.gke.io/workload-identity-image-pull-enabled"] = "true"
			}
		}
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
