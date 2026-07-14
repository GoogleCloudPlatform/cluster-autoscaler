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

package labels

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	kubeletapis "k8s.io/kubelet/pkg/apis"
)

func TestCommonSystemLabels(t *testing.T) {
	systemLabels := []string{
		"beta.kubernetes.io/fluentd-ds-ready",
		"iam.gke.io/gke-metadata-server-enabled",
		"beta.kubernetes.io/kube-proxy-ds-ready",
		"node.kubernetes.io/kube-proxy-ds-ready",
		"cloud.google.com/metadata-proxy-ready",
		"cloud.google.com/gke-netd-ready",
		"addon.gke.io/node-local-dns-ds-ready",
		"sandbox.gke.io/runtime",

		apiv1.LabelHostname,
		apiv1.LabelTopologyZone,
		apiv1.LabelTopologyRegion,
		apiv1.LabelFailureDomainBetaZone,
		apiv1.LabelFailureDomainBetaRegion,
		apiv1.LabelInstanceType,
		apiv1.LabelInstanceTypeStable,
		apiv1.LabelOSStable,
		apiv1.LabelArchStable,
		kubeletapis.LabelArch,
		kubeletapis.LabelOS,

		GkeNodePoolLabel,
		GkeOsDistributionLabel,
		MachineFamilyLabel,
		NodeGeneratedFromTemplateAnnotation,
		GPULabel,
		GPUPartitionSizeLabel,
		PreemptibleLabel,
		RequestedMinCpuPlatformLabel,
		SpotLabel,
	}

	for _, label := range systemLabels {
		t.Run(label, func(t *testing.T) {
			assert.True(t, IsSystemLabel(label))
		})
	}
}

func TestSystemLabelNamespaces(t *testing.T) {
	systemNamespaces := []string{
		"cloud.google.com",
		"kubernetes.io",
		"gke.io",
		"k8s.io",
	}
	for _, namespace := range systemNamespaces {
		t.Run(namespace, func(t *testing.T) {
			assert.True(t, IsSystemLabel(fmt.Sprintf("%s/", namespace)))
			assert.True(t, IsSystemLabel(fmt.Sprintf("%s/key", namespace)))
			assert.True(t, IsSystemLabel(fmt.Sprintf("test.%s/key", namespace)))
			assert.True(t, IsSystemLabel(fmt.Sprintf("prefix/%s/key", namespace)))

			assert.False(t, IsSystemLabel(namespace))
			assert.False(t, IsSystemLabel(fmt.Sprintf("%s.net/", namespace)))
			assert.False(t, IsSystemLabel(fmt.Sprintf("%s/", strings.Replace(namespace, ".", "-", -1))))
		})
	}
}

func TestSystemLabelsForGPU(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		gpuType                     string
		gpuDriverVersion            string
		nodeAutoprovisioningEnabled bool
		wantGpuDriverVersion        string
	}{
		{
			name:                        "empty standard",
			gpuType:                     "",
			gpuDriverVersion:            "",
			nodeAutoprovisioningEnabled: false,
			wantGpuDriverVersion:        "",
		},
		{
			name:                        "empty autopilot - do not inject driver version when no gpu type set",
			gpuType:                     "",
			gpuDriverVersion:            "",
			nodeAutoprovisioningEnabled: true,
			wantGpuDriverVersion:        "",
		},
		{
			name:                        "no driver autoinstallation in standard for autoprovisioned node pools",
			gpuType:                     "a-gpu-type",
			gpuDriverVersion:            "",
			nodeAutoprovisioningEnabled: false,
			wantGpuDriverVersion:        "",
		},
		{
			name:                        "default value - autopilot installs default drivers even when unset",
			gpuType:                     "a-gpu-type",
			gpuDriverVersion:            "",
			nodeAutoprovisioningEnabled: true,
			wantGpuDriverVersion:        DefaultGPUDriverVersionValue,
		},
		{
			name:                        "standard - set driver version is not overriden",
			gpuType:                     "a-gpu-type",
			gpuDriverVersion:            "latest",
			nodeAutoprovisioningEnabled: false,
			wantGpuDriverVersion:        "latest",
		},
		{
			name:                        "autopilot - set driver version is not overriden",
			gpuType:                     "a-gpu-type",
			gpuDriverVersion:            "latest",
			nodeAutoprovisioningEnabled: true,
			wantGpuDriverVersion:        "latest",
		},
		{
			name:                        "standard - autoinstall-disabled is not overriden",
			gpuType:                     "a-gpu-type",
			gpuDriverVersion:            DisabledGPUDriverVersionValue,
			nodeAutoprovisioningEnabled: false,
			wantGpuDriverVersion:        DisabledGPUDriverVersionValue,
		},
		{
			name:                        "autopilot - autoinstall-disabled is not overriden",
			gpuType:                     "a-gpu-type",
			gpuDriverVersion:            DisabledGPUDriverVersionValue,
			nodeAutoprovisioningEnabled: true,
			wantGpuDriverVersion:        DisabledGPUDriverVersionValue,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			labels := SystemLabelsForGPU(tc.gpuType, "", "", "", tc.gpuDriverVersion, tc.nodeAutoprovisioningEnabled)
			assert.Equal(t, tc.wantGpuDriverVersion, labels[GPUDriverVersionLabel])
		})
	}
}

func TestConvertGpuSharingStrategyToLabelEnum(t *testing.T) {
	for input, expected := range map[string]string{
		"TIME_SHARING":         GPUTimeSharingStrategy,
		GPUTimeSharingStrategy: GPUTimeSharingStrategy,
		"MPS":                  GPUMpsStrategy,
		GPUMpsStrategy:         GPUMpsStrategy,
		"other":                "other",
	} {
		t.Run(input, func(t *testing.T) {
			input := input
			expected := expected
			t.Parallel()

			res := ConvertGpuSharingStrategyToLabelEnum(input)

			assert.Equal(t, expected, res)
		})
	}
}

func TestIsAddedByClusterAutoscaler(t *testing.T) {
	testCases := []struct {
		key                   string
		bootDiskConfigEnabled bool
		want                  bool
	}{
		{
			key:  ComputeClassLabel,
			want: true,
		},
		{
			key:  "some-random-label",
			want: false,
		},
		{
			key:                   BootDiskTypeLabelKey,
			bootDiskConfigEnabled: true,
			want:                  true,
		},
		{
			key:                   BootDiskTypeLabelKey,
			bootDiskConfigEnabled: false,
			want:                  false,
		},
		{
			key:  SupportedCpuPlatformKey("test"),
			want: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			assert.Equal(t, tc.want, IsAddedByClusterAutoscaler(tc.key, tc.bootDiskConfigEnabled))
		})
	}
}
