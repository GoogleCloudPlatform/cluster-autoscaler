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
	"context"
	"testing"

	gcev1 "google.golang.org/api/compute/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

func TestBuildNodeFromTemplate_Basic(t *testing.T) {
	nodeName := "node-1"
	wantLabels := make(map[string]string)
	wantAnnotations := make(map[string]string)
	wantCapacity := make(apiv1.ResourceList)

	migRef := gce.GceRef{
		Project: "test-project",
		Zone:    "us-central1-a",
		Name:    "test-nodepool",
	}
	wantLabels["cloud.google.com/gke-nodepool"] = migRef.Name

	mt := &gcev1.MachineType{
		Name:      "n1-standard-4",
		GuestCpus: 4,
		MemoryMb:  15360,
	}
	wantCapacity[apiv1.ResourceCPU] = *resource.NewQuantity(4, resource.DecimalSI)
	wantLabels[gkelabels.MachineFamilyLabel] = "n1"
	wantLabels[apiv1.LabelInstanceTypeStable] = mt.Name
	wantLabels[apiv1.LabelInstanceType] = mt.Name

	template := &gcev1.InstanceTemplate{
		Name: "test-template",
		Properties: &gcev1.InstanceProperties{
			MachineType: mt.Name,
			Disks: []*gcev1.AttachedDisk{
				{
					Boot: true,
					InitializeParams: &gcev1.AttachedDiskInitializeParams{
						DiskSizeGb: 100,
						DiskType:   "pd-standard",
					},
				},
			},
		},
	}

	ke := MustParseKubeEnv(t, nodeName, "NODE_LABELS: env=prod,team=autoscaling\nNODE_TAINTS: dedicated=critical:NoSchedule\n")
	wantLabels["env"] = "prod"
	wantLabels["team"] = "autoscaling"
	wantAnnotations["node.gke.io/last-applied-node-labels"] = "fake-node-label"

	node, err := BuildNodeFromTemplate(context.Background(), migRef, mt, template, ke, nodeName)
	if err != nil {
		t.Fatalf("BuildNodeFromTemplate failed: %v", err)
	}

	if node.Name != nodeName {
		t.Errorf("expected node name %q, got %q", nodeName, node.Name)
	}
	for k, want := range wantLabels {
		if got := node.Labels[k]; got != want {
			t.Errorf("expected label %s=%q, got %q", k, want, got)
		}
	}
	for k, want := range wantAnnotations {
		if got := node.Annotations[k]; got != want {
			t.Errorf("expected annotation %s=%q, got %q", k, want, got)
		}
	}
	for k, want := range wantCapacity {
		if got := node.Status.Capacity[k]; got.Cmp(want) != 0 {
			t.Errorf("expected capacity %s=%v, got %v", k, want.String(), got.String())
		}
	}
	if node.Status.NodeInfo.KubeletVersion == "" {
		t.Errorf("expected non-empty KubeletVersion")
	}
}

func TestBuildNodeFromTemplate_TPU(t *testing.T) {
	nodeName := "tpu-node-1"
	wantLabels := make(map[string]string)
	wantCapacity := make(apiv1.ResourceList)
	wantAllocatable := make(apiv1.ResourceList)

	migRef := gce.GceRef{
		Project: "test-project",
		Zone:    "us-central1-a",
		Name:    "tpu-pool",
	}

	tpuType := "tpu-v5-lite-podslice"
	tpuCount := int64(4)
	tpuResource := apiv1.ResourceName("google.com/tpu")
	mt := &gcev1.MachineType{
		Name:      "ct5lp-hightpu-4t",
		GuestCpus: 224,
		MemoryMb:  896000,
		Accelerators: []*gcev1.MachineTypeAccelerators{
			{
				GuestAcceleratorType:  tpuType,
				GuestAcceleratorCount: tpuCount,
			},
		},
	}
	wantCapacity[tpuResource] = *resource.NewQuantity(tpuCount, resource.DecimalSI)
	wantAllocatable[tpuResource] = *resource.NewQuantity(tpuCount, resource.DecimalSI)
	wantLabels[gkelabels.TPULabel] = tpuType
	wantLabels[apiv1.LabelInstanceTypeStable] = mt.Name
	wantLabels[apiv1.LabelInstanceType] = mt.Name

	template := &gcev1.InstanceTemplate{
		Name: "tpu-template",
		Properties: &gcev1.InstanceProperties{
			MachineType: mt.Name,
			Disks: []*gcev1.AttachedDisk{
				{
					Boot: true,
					InitializeParams: &gcev1.AttachedDiskInitializeParams{
						DiskSizeGb: 100,
						DiskType:   "pd-standard",
					},
				},
			},
		},
	}

	node, err := BuildNodeFromTemplate(context.Background(), migRef, mt, template, gce.KubeEnv{}, nodeName)
	if err != nil {
		t.Fatalf("BuildNodeFromTemplate failed: %v", err)
	}

	for k, want := range wantLabels {
		if got := node.Labels[k]; got != want {
			t.Errorf("expected label %s=%q, got %q", k, want, got)
		}
	}
	for k, want := range wantCapacity {
		if got := node.Status.Capacity[k]; got.Cmp(want) != 0 {
			t.Errorf("expected capacity %s=%v, got %v", k, want.String(), got.String())
		}
	}
	for k, want := range wantAllocatable {
		if got := node.Status.Allocatable[k]; got.Cmp(want) != 0 {
			t.Errorf("expected allocatable %s=%v, got %v", k, want.String(), got.String())
		}
	}

	wantTaint := apiv1.Taint{
		Key:    "google.com/tpu",
		Value:  "present",
		Effect: apiv1.TaintEffectNoSchedule,
	}
	if !hasTaint(node.Spec.Taints, wantTaint) {
		t.Errorf("expected taint %v to be present in %v", wantTaint, node.Spec.Taints)
	}
}

func MustParseKubeEnv(t *testing.T, instanceName, kubeEnvStr string) gce.KubeEnv {
	t.Helper()
	ke, err := gce.ParseKubeEnv(instanceName, kubeEnvStr)
	if err != nil {
		t.Fatalf("ParseKubeEnv failed: %v", err)
	}
	return ke
}

func hasTaint(taints []apiv1.Taint, expected apiv1.Taint) bool {
	for _, t := range taints {
		if t.Key == expected.Key && t.Value == expected.Value && t.Effect == expected.Effect {
			return true
		}
	}
	return false
}
