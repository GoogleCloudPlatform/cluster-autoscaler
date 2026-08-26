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

package fake

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"

	gcev1 "google.golang.org/api/compute/v1"
	gkeapibeta "google.golang.org/api/container/v1beta1"
	"google.golang.org/api/googleapi"
	gceinternal "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

const (
	defaultKubeReserved       = "cpu=100m,memory=256Mi,ephemeral-storage=1Gi"
	evictionHardForAutoscaler = "memory=100Mi,ephemeral-storage=10%"
	evictionHardForKubelet    = "memory<100Mi,ephemeral-storage<10%"
	defaultOS                 = "linux"
	defaultOSDistribution     = "cos"
	defaultArch               = "amd64"
)

// buildInstanceTemplateFromRequest constructs a GCE InstanceTemplate from a GKE NodePool request.
// It maps the GKE NodePool configuration (e.g., machine type, disk size, labels, taints, accelerators)
// into the corresponding GCE InstanceTemplate properties.
func buildInstanceTemplateFromRequest(project, templateName string, np *gkeapibeta.NodePool, zone string) (*gcev1.InstanceTemplate, error) {
	config := np.Config
	if config == nil {
		return nil, fmt.Errorf("no config available in node pool %s", np.Name)
	}

	// Resolve accelerators early to use in labels and env vars
	accelerators, err := buildAccelerators(config)
	if err != nil {
		return nil, err
	}

	machineType := config.MachineType
	diskSize := config.DiskSizeGb

	kubeLabels := buildKubeLabels(np, zone, accelerators)
	kubeLabelsStr := toKubeLabelString(kubeLabels)
	taintsStr := toKubeTaintString(config.Taints)

	var autoscalerEnvVars []string

	commonEnvVars := []string{
		fmt.Sprintf("os=%s", defaultOS),
		fmt.Sprintf("os_distribution=%s", defaultOSDistribution),
		fmt.Sprintf("arch=%s", defaultArch),
		fmt.Sprintf("node_labels=%s", kubeLabelsStr),
		fmt.Sprintf("node_taints=%s", taintsStr),
		fmt.Sprintf("kube_reserved=%s", defaultKubeReserved),
		fmt.Sprintf("evictionHard=%s", evictionHardForAutoscaler),
	}

	var tpuCount int64
	for _, acc := range accelerators {
		if !strings.HasPrefix(acc.AcceleratorType, "nvidia") {
			tpuCount += acc.AcceleratorCount
		}
	}
	if tpuCount > 0 {
		commonEnvVars = append(commonEnvVars, fmt.Sprintf("extended_resources=google.com/tpu=%d", tpuCount))
	}

	autoscalerEnvVars = append(autoscalerEnvVars, commonEnvVars...)

	var kubeletArgs []string
	kubeletArgs = append(kubeletArgs, fmt.Sprintf("--kube-reserved=%s", defaultKubeReserved))
	kubeletArgs = append(kubeletArgs, fmt.Sprintf("--eviction-hard=%s", evictionHardForKubelet))

	var kubeEnvStringBuilder strings.Builder
	kubeEnvStringBuilder.WriteString(fmt.Sprintf("AUTOSCALER_ENV_VARS: %s\n", strings.Join(autoscalerEnvVars, ";")))
	kubeEnvStringBuilder.WriteString(fmt.Sprintf("NODE_LABELS: %s\n", kubeLabelsStr))
	kubeEnvStringBuilder.WriteString(fmt.Sprintf("NODE_TAINTS: %s\n", taintsStr))
	kubeEnvStringBuilder.WriteString(fmt.Sprintf("KUBELET_ARGS: %s\n", strings.Join(kubeletArgs, " ")))

	metadataItems := []*gcev1.MetadataItems{
		{
			Key:   "kube-env",
			Value: googleapi.String(kubeEnvStringBuilder.String()),
		},
	}

	disks := []*gcev1.AttachedDisk{
		{
			Boot: true,
			InitializeParams: &gcev1.AttachedDiskInitializeParams{
				DiskSizeGb: diskSize,
			},
		},
	}
	for i := 0; i < int(config.LocalSsdCount); i++ {
		disks = append(disks, &gcev1.AttachedDisk{
			Type: "SCRATCH",
		})
	}

	serviceAccounts := []*gcev1.ServiceAccount{
		{
			Email:  "default",
			Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"},
		},
	}

	properties := &gcev1.InstanceProperties{
		MachineType:       machineType,
		Disks:             disks,
		Metadata:          &gcev1.Metadata{Items: metadataItems},
		ServiceAccounts:   serviceAccounts,
		MinCpuPlatform:    config.MinCpuPlatform,
		GuestAccelerators: accelerators,
	}

	if config.ReservationAffinity != nil {
		properties.ReservationAffinity = &gcev1.ReservationAffinity{
			ConsumeReservationType: config.ReservationAffinity.ConsumeReservationType,
			Key:                    config.ReservationAffinity.Key,
			Values:                 config.ReservationAffinity.Values,
		}
	}

	// Hash the template name to generate a deterministic unique Id.
	hash := fnv.New64a()
	hash.Write([]byte(templateName))
	templateId := hash.Sum64()

	return &gcev1.InstanceTemplate{
		Id:         templateId,
		Name:       templateName,
		SelfLink:   fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/instanceTemplates/%s", project, templateName),
		Properties: properties,
	}, nil
}

func buildKubeLabels(np *gkeapibeta.NodePool, zone string, accelerators []*gcev1.AcceleratorConfig) map[string]string {
	config := np.Config
	kubeLabels := map[string]string{
		"cloud.google.com/gke-nodepool":        np.Name,
		"cloud.google.com/gke-os-distribution": strings.ToLower(config.ImageType),
		"topology.gke.io/zone":                 zone,
		"topology.kubernetes.io/zone":          zone,
	}
	if mfName, err := gceinternal.GetMachineFamily(config.MachineType); err == nil && mfName != "" {
		kubeLabels[gkelabels.MachineFamilyLabel] = mfName
	}
	for k, v := range config.Labels {
		kubeLabels[k] = v
	}
	if np.PlacementPolicy != nil {
		kubeLabels[gkelabels.PolicyLabel] = np.PlacementPolicy.PolicyName
		if np.PlacementPolicy.TpuTopology != "" {
			kubeLabels[gkelabels.TPUTopologyLabel] = np.PlacementPolicy.TpuTopology
		}
	}
	for _, acc := range accelerators {
		if strings.HasPrefix(acc.AcceleratorType, "nvidia") {
			kubeLabels[gkelabels.GPULabel] = acc.AcceleratorType
		} else {
			kubeLabels[gkelabels.TPULabel] = acc.AcceleratorType
			if np.PlacementPolicy != nil && np.PlacementPolicy.TpuTopology != "" {
				kubeLabels[gkelabels.TPUTopologyLabel] = np.PlacementPolicy.TpuTopology
			}
		}
		kubeLabels[gkelabels.AcceleratorCountLabel] = strconv.FormatInt(acc.AcceleratorCount, 10)
	}
	return kubeLabels
}

func buildAccelerators(config *gkeapibeta.NodeConfig) ([]*gcev1.AcceleratorConfig, error) {
	accelerators := make([]*gcev1.AcceleratorConfig, 0)
	for _, acc := range config.Accelerators {
		accelerators = append(accelerators, &gcev1.AcceleratorConfig{
			AcceleratorType:  acc.AcceleratorType,
			AcceleratorCount: acc.AcceleratorCount,
		})
	}
	if len(accelerators) == 0 {
		mcp := machinetypes.NewMachineConfigProvider(nil)
		mtInfo, err := mcp.ToMachineType(config.MachineType)
		if err == nil {
			if mtInfo.HasFixedGPU() {
				accelerators = append(accelerators, &gcev1.AcceleratorConfig{
					AcceleratorType:  mtInfo.GpuType(),
					AcceleratorCount: int64(mtInfo.FixedGpuCount()),
				})
			} else {
				tpuType, tpuCount, err := mtInfo.TpuConfig()
				if err != nil {
					return nil, err
				}
				if tpuType != "" && tpuCount > 0 {
					accelerators = append(accelerators, &gcev1.AcceleratorConfig{
						AcceleratorType:  tpuType,
						AcceleratorCount: tpuCount,
					})
				}
			}
		}
	}
	return accelerators, nil
}

func toKubeLabelString(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	var parts []string
	for k, v := range labels {
		parts = append(parts, fmt.Sprintf("%s=%s", k, v))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func toKubeTaintString(taints []*gkeapibeta.NodeTaint) string {
	if len(taints) == 0 {
		return ""
	}
	var parts []string
	for _, t := range taints {
		parts = append(parts, fmt.Sprintf("%s=%s:%s", t.Key, t.Value, t.Effect))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
