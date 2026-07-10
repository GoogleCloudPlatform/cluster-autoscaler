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

package autoprovisioning

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"

	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/sandbox"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
)

var (
	compareAllUnexportedOpt = cmp.Exporter(func(r reflect.Type) bool { return true })
	cmpReqs                 = func(x, y nodeGroupRequirements) bool {
		return fmt.Sprintf("%v", y) < fmt.Sprintf("%v", x)
	}
	cmpPods = func(x, y *apiv1.Pod) bool {
		return fmt.Sprintf("%v", y) < fmt.Sprintf("%v", x)
	}
	requirementsIgnoreOrderOpt = cmpopts.SortSlices(cmpReqs)

	comNextedReqs = func(x, y []nodeGroupRequirements) bool {
		var xs, ys []string
		for _, xReq := range x {
			xs = append(xs, fmt.Sprintf("%v", xReq))
		}
		for _, yReq := range y {
			ys = append(ys, fmt.Sprintf("%v", yReq))
		}

		return strings.Join(ys, ";") < strings.Join(xs, ";")
	}
	nestedRequirementsIgnoreOrderOpt = cmpopts.SortSlices(comNextedReqs)

	requirementsSliceIgnoreOrderOpt = cmpopts.SortSlices(func(x, y []nodeGroupRequirements) bool {
		var xSig, ySig bytes.Buffer
		sort.Slice(x, func(i, j int) bool {
			return cmpReqs(x[i], x[j])
		})
		sort.Slice(y, func(i, j int) bool {
			return cmpReqs(y[i], y[j])
		})
		for _, el := range x {
			xSig.WriteString(fmt.Sprintf("%v", el))
		}
		for _, el := range y {
			ySig.WriteString(fmt.Sprintf("%v", el))
		}
		return ySig.String() < xSig.String()
	})
	requirementsCompareOnlyPodsAndGpuOpt = cmp.Comparer(func(x, y nodeGroupRequirements) bool {
		return cmp.Equal(x.pods, y.pods, cmpopts.SortSlices(cmpPods)) && cmp.Equal(x.gpuRequest, y.gpuRequest, compareAllUnexportedOpt)
	})
	compareErrorsByValueOpt = cmp.Comparer(func(e1, e2 error) bool {
		return e1 == e2 || cmp.Equal(e1, e2)
	})
	requirementsCompareOnlyPodsAndTpuOpt = cmp.Comparer(func(x, y nodeGroupRequirements) bool {
		return cmp.Equal(x.pods, y.pods) && cmp.Equal(x.tpuRequest, y.tpuRequest, compareAllUnexportedOpt)
	})
)

func buildGpuPod(name string, gpuType string, gpuCount int64) *apiv1.Pod {
	pod := test.BuildTestPod(name, 0, 1000)
	pod.UID = types.UID(name)
	addGpuToPod(pod, gpuType, gpuCount)
	return pod
}

func buildTpuPodGeneric(name string, tpuType string, tpuCount int64, topology string, setResourceTpu bool) *apiv1.Pod {
	pod := test.BuildTestPod(name, 0, 1000)
	pod.UID = types.UID(name)
	pod.Spec.NodeSelector = map[string]string{
		gkelabels.TPULabel:              tpuType,
		gkelabels.TPUTopologyLabel:      topology,
		gkelabels.AcceleratorCountLabel: strconv.FormatInt(tpuCount, 10),
	}
	if setResourceTpu {
		pod.Spec.Containers[0].Resources.Requests[tpu.ResourceGoogleTPU] = *resource.NewQuantity(tpuCount, resource.DecimalSI)
	}
	return pod
}

func buildTpuPod(name string, tpuType string, tpuCount int64, topology string) *apiv1.Pod {
	return buildTpuPodGeneric(name, tpuType, tpuCount, topology, true)
}

func addGpuToPod(pod *apiv1.Pod, gpuType string, gpuCount int64) {
	if gpuType != machinetypes.AnyGPU {
		pod.Spec.NodeSelector = map[string]string{gkelabels.GPULabel: gpuType}
	}
	pod.Spec.Containers[0].Resources.Requests[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(gpuCount, resource.DecimalSI)
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, apiv1.Toleration{
		Key:      gpu.ResourceNvidiaGPU,
		Operator: apiv1.TolerationOpExists,
		Effect:   apiv1.TaintEffectNoSchedule,
	})
}

func addGpuPartitioning(pod *apiv1.Pod, partitionSize string) *apiv1.Pod {
	return addRequirement(pod, gkelabels.GPUPartitionSizeLabel, partitionSize)
}

func addGpuTimeSharing(pod *apiv1.Pod, maxSharedClients, sharingStrategy string) *apiv1.Pod {
	addRequirement(pod, gkelabels.GPUMaxSharedClientsLabel, maxSharedClients)
	if sharingStrategy != "" {
		addRequirement(pod, gkelabels.GPUSharingStrategyLabel, sharingStrategy)
	}
	return pod
}

func addGpuDriverVersion(pod *apiv1.Pod, driverVersion string) *apiv1.Pod {
	return addRequirement(pod, gkelabels.GPUDriverVersionLabel, driverVersion)
}

func buildPodWithGpuCpusAndMem(name string, gpuType string, gpuCount, cpu, mem int64) *apiv1.Pod {
	pod := test.BuildTestPod(name, cpu, mem)
	addGpuToPod(pod, gpuType, gpuCount)
	return pod
}

func addSeparation(pod *apiv1.Pod, key, val string, correct bool) *apiv1.Pod {
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	pod.Spec.NodeSelector[key] = val
	if correct {
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, apiv1.Toleration{
			Key:      key,
			Operator: apiv1.TolerationOpEqual,
			Value:    val,
			Effect:   apiv1.TaintEffectNoSchedule,
		})
	}
	return pod
}

func addRequirement(pod *apiv1.Pod, label, value string) *apiv1.Pod {
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	pod.Spec.NodeSelector[label] = value
	return pod
}

func addMachineFamily(pod *apiv1.Pod, family string) *apiv1.Pod {
	return addRequirement(pod, gkelabels.MachineFamilyLabel, family)
}

func addComputeClass(pod *apiv1.Pod, class string) *apiv1.Pod {
	return addRequirement(pod, gkelabels.ComputeClassLabel, class)
}

func addInstanceType(pod *apiv1.Pod, instanceType string) *apiv1.Pod {
	pod = addRequirement(pod, apiv1.LabelInstanceTypeStable, instanceType)
	return pod
}

func addMinCpuPlatform(pod *apiv1.Pod, platform string) *apiv1.Pod {
	return addRequirement(pod, gkelabels.RequestedMinCpuPlatformLabel, platform)
}

func addArch(pod *apiv1.Pod, arch string) *apiv1.Pod {
	return addRequirement(pod, apiv1.LabelArchStable, arch)
}

func addGVisorLabel(pod *apiv1.Pod) *apiv1.Pod {
	return addRequirement(pod, sandbox.RuntimeLabelKey, sandbox.GVisorLabelValue)
}

func addCompactPlacementGroupLabel(pod *apiv1.Pod, label string) *apiv1.Pod {
	return addRequirement(pod, gkelabels.PlacementGroupLabel, label)
}

func addCompactPlacementPolicyLabel(pod *apiv1.Pod, label string) *apiv1.Pod {
	return addRequirement(pod, gkelabels.PolicyLabel, label)
}

func addBootDiskTypeLabel(pod *apiv1.Pod, value string) *apiv1.Pod {
	return addRequirement(pod, gkelabels.BootDiskTypeLabelKey, value)
}

func addBootDiskSizeLabel(pod *apiv1.Pod, value string) *apiv1.Pod {
	return addRequirement(pod, gkelabels.BootDiskSizeLabelKey, value)
}

func addBootDiskEncryptionLabel(pod *apiv1.Pod, value string) *apiv1.Pod {
	pod = addRequirement(pod, gkelabels.BootDiskEncryptionLabelKey, "encryption-key-annotation")
	pod.Annotations["encryption-key-annotation"] = value

	return pod
}

func addExtendedDurationPodLabel(pod *apiv1.Pod, label string) *apiv1.Pod {
	return addRequirement(pod, gkelabels.ExtendedDurationPodsLabel, label)
}

func addReservationLabels(pod *apiv1.Pod, resName, resProject string) *apiv1.Pod {
	pod = addRequirement(pod, gkelabels.ReservationNameLabel, resName)
	pod = addRequirement(pod, gkelabels.ReservationProjectLabel, resProject)
	return pod
}

func addAcceleratorLabels(pod *apiv1.Pod, count, tpuType string) *apiv1.Pod {
	pod = addRequirement(pod, gkelabels.AcceleratorCountLabel, count)
	pod = addRequirement(pod, gkelabels.TPULabel, tpuType)
	return pod
}

func addReservationProjectLabel(pod *apiv1.Pod, project string) *apiv1.Pod {
	pod = addRequirement(pod, gkelabels.ReservationProjectLabel, project)
	return pod
}

func addReservationAffinity(pod *apiv1.Pod, affinity string) *apiv1.Pod {
	pod = addRequirement(pod, gkelabels.ReservationAffinityLabel, affinity)
	return pod
}

func addPodPerVMLabel(pod *apiv1.Pod, label string) *apiv1.Pod {
	return addRequirement(pod, gkelabels.PodPerVMSizeLabel, label)
}

func addPodPerVMInfo(pod *apiv1.Pod, cpu string) *apiv1.Pod {
	pod = addPodPerVMLabel(pod, cpu)
	pod.Spec.Containers[0].Resources.Requests[gkelabels.PodCapacityLabel] = *resource.NewQuantity(1, resource.DecimalSI)
	return pod
}

func addSliceOfHardwarePod(pod *apiv1.Pod, cpu string) *apiv1.Pod {
	pod = addPodPerVMInfo(pod, cpu)
	pod = addComputeClass(pod, "Performance")
	pod = addMachineFamily(pod, "c3")
	return pod
}

func addSpotToleration(pod *apiv1.Pod) *apiv1.Pod {
	if pod.Spec.Tolerations == nil {
		pod.Spec.Tolerations = []apiv1.Toleration{}
	}
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, apiv1.Toleration{
		Key:      gkelabels.SpotLabel,
		Operator: apiv1.TolerationOpEqual,
		Value:    "true",
	})
	return pod
}

// buildBulkProvisioningPod conditions set in this helper should correspond to checks in pods.go UsesBulkProvisioning()
func buildBulkProvisioningPod(name string) *apiv1.Pod {
	pod := test.BuildTestPod(name, 1, 1)
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	// GB200 is known accelerator with slices support
	pod.Spec.NodeSelector[gkelabels.GPULabel] = gkelabels.NvidiaGB200
	pod.Spec.NodeSelector[gkelabels.PolicyLabel] = "policy-1"
	pod.Spec.NodeSelector[gkelabels.FlexStartLabel] = "true"
	return pod
}
