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

package reconciler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	apiv1 "k8s.io/api/core/v1"

	gce_api "google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
	fakegce "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/fake"
)

func TestReconcile(t *testing.T) {
	kubeClient := newTestKubeClient(t)

	template := buildTestInstanceTemplate(testTemplateOptions{
		Name:        "my-template",
		MachineType: "n1-standard-4",
		DiskSizeGb:  100,
	})

	fakeGce := fakegce.NewGceClient(t, nil).WithZones(map[string][]string{
		"us-central1": {"us-central1-a"},
	}, nil).WithTemplates(template)
	fakeGce.AddCustomMachineType(buildTestMachineType("us-central1-a", "n1-standard-4", 4, 15360, nil))

	instances := []*gce_api.Instance{
		buildTestInstance(testInstanceOptions{
			Project:     "my-proj",
			Zone:        "us-central1-a",
			Name:        "instance-1",
			MachineType: "n1-standard-4",
			Status:      "RUNNING",
			MigName:     "my-mig",
			TemplateURL: globalTemplateURL("my-proj", "my-template"),
			KubeEnv: buildTestKubeEnv(
				map[string]string{"label1": "val1", "label2": "val2"},
				[]string{"taint1=val1:NoSchedule"},
			),
		}),
	}

	server, gceService := newTestComputeServer(t, instances)
	defer server.Close()

	rec := NewKwokGceReconciler(kubeClient, fakeGce, gceService, "my-proj", "my-cluster", "us-central1", 10*time.Second)
	rec.Reconcile(context.Background())

	var nodeList apiv1.NodeList
	if err := kubeClient.List(context.Background(), &nodeList); err != nil {
		t.Fatalf("failed to list nodes: %v", err)
	}

	if len(nodeList.Items) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodeList.Items))
	}

	node := nodeList.Items[0]
	if node.Name != "instance-1" {
		t.Errorf("expected node name 'instance-1', got %s", node.Name)
	}

	if node.Spec.ProviderID != "gce://my-proj/us-central1-a/instance-1" {
		t.Errorf("expected provider ID 'gce://my-proj/us-central1-a/instance-1', got %s", node.Spec.ProviderID)
	}

	cpu := node.Status.Capacity[apiv1.ResourceCPU]
	if cpu.Value() != 4 {
		t.Errorf("expected CPU capacity 4, got %v", cpu.Value())
	}

	mem := node.Status.Capacity[apiv1.ResourceMemory]
	if mem.Value() <= 0 || mem.Value() > int64(15360*1024*1024) {
		t.Errorf("expected Memory capacity to be > 0 and <= %d, got %v", int64(15360*1024*1024), mem.Value())
	}

	expectedLabels := map[string]string{
		apiv1.LabelHostname:           "instance-1",
		apiv1.LabelTopologyZone:       "us-central1-a",
		apiv1.LabelTopologyRegion:     "us-central1",
		apiv1.LabelInstanceTypeStable: "n1-standard-4",
		apiv1.LabelInstanceType:       "n1-standard-4",
		"label1":                      "val1",
		"label2":                      "val2",
	}

	for k, expectedVal := range expectedLabels {
		actualVal := node.Labels[k]
		if actualVal != expectedVal {
			t.Errorf("expected label %s=%s, got %s", k, expectedVal, actualVal)
		}
	}

	if len(node.Spec.Taints) != 1 {
		t.Errorf("expected 1 taint, got %d", len(node.Spec.Taints))
	} else {
		taint := node.Spec.Taints[0]
		if taint.Key != "taint1" || taint.Value != "val1" || taint.Effect != apiv1.TaintEffectNoSchedule {
			t.Errorf("unexpected taint: %v", taint)
		}
	}

	expectedLastAppliedLabels := "label1=val1,label2=val2"
	actualLastAppliedLabels := node.Annotations["node.gke.io/last-applied-node-labels"]
	if actualLastAppliedLabels != expectedLastAppliedLabels {
		t.Errorf("expected annotation node.gke.io/last-applied-node-labels=%q, got %q", expectedLastAppliedLabels, actualLastAppliedLabels)
	}

	expectedLastAppliedTaints := "taint1=val1:NoSchedule"
	actualLastAppliedTaints := node.Annotations["node.gke.io/last-applied-node-taints"]
	if actualLastAppliedTaints != expectedLastAppliedTaints {
		t.Errorf("expected annotation node.gke.io/last-applied-node-taints=%q, got %q", expectedLastAppliedTaints, actualLastAppliedTaints)
	}

	if len(node.Status.Conditions) != 1 || node.Status.Conditions[0].Type != apiv1.NodeReady || node.Status.Conditions[0].Status != apiv1.ConditionUnknown {
		t.Errorf("expected NodeReady condition to be Unknown, got: %v", node.Status.Conditions)
	}
}

func TestReconcileTemplateFallback(t *testing.T) {
	kubeClient := newTestKubeClient(t)

	template := buildTestInstanceTemplate(testTemplateOptions{
		Name:        "my-template",
		MachineType: "n1-standard-8",
		DiskSizeGb:  100,
		KubeEnv: buildTestKubeEnv(
			map[string]string{"labelA": "valA"},
			[]string{"taintA=valA:NoSchedule"},
		),
	})

	fakeGce := fakegce.NewGceClient(t, nil).WithZones(map[string][]string{
		"us-central1": {"us-central1-a"},
	}, nil).WithTemplates(template)
	fakeGce.AddCustomMachineType(buildTestMachineType("us-central1-a", "n1-standard-8", 8, 30720, nil))

	instances := []*gce_api.Instance{
		buildTestInstance(testInstanceOptions{
			Project:     "my-proj",
			Zone:        "us-central1-a",
			Name:        "instance-2",
			MachineType: "",
			Status:      "RUNNING",
			MigName:     "my-mig",
			TemplateURL: regionalTemplateURL("my-proj", "us-central1", "my-template"),
		}),
	}

	server, gceService := newTestComputeServer(t, instances)
	defer server.Close()

	rec := NewKwokGceReconciler(kubeClient, fakeGce, gceService, "my-proj", "my-cluster", "us-central1", 10*time.Second)
	rec.Reconcile(context.Background())

	var nodeList apiv1.NodeList
	if err := kubeClient.List(context.Background(), &nodeList); err != nil {
		t.Fatalf("failed to list nodes: %v", err)
	}

	if len(nodeList.Items) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodeList.Items))
	}

	node := nodeList.Items[0]
	if node.Name != "instance-2" {
		t.Errorf("expected node name 'instance-2', got %s", node.Name)
	}

	cpu := node.Status.Capacity[apiv1.ResourceCPU]
	if cpu.Value() != 8 {
		t.Errorf("expected CPU capacity 8, got %v", cpu.Value())
	}

	if node.Labels[apiv1.LabelInstanceTypeStable] != "n1-standard-8" {
		t.Errorf("expected machine type label n1-standard-8, got %s", node.Labels[apiv1.LabelInstanceTypeStable])
	}
	if node.Labels["labelA"] != "valA" {
		t.Errorf("expected label labelA=valA, got %s", node.Labels["labelA"])
	}

	expectedLastAppliedLabels := "labelA=valA"
	actualLastAppliedLabels := node.Annotations["node.gke.io/last-applied-node-labels"]
	if actualLastAppliedLabels != expectedLastAppliedLabels {
		t.Errorf("expected annotation node.gke.io/last-applied-node-labels=%q, got %q", expectedLastAppliedLabels, actualLastAppliedLabels)
	}

	expectedLastAppliedTaints := "taintA=valA:NoSchedule"
	actualLastAppliedTaints := node.Annotations["node.gke.io/last-applied-node-taints"]
	if actualLastAppliedTaints != expectedLastAppliedTaints {
		t.Errorf("expected annotation node.gke.io/last-applied-node-taints=%q, got %q", expectedLastAppliedTaints, actualLastAppliedTaints)
	}
}

func TestReconcileWithGPU(t *testing.T) {
	kubeClient := newTestKubeClient(t)

	gpuTemplate := buildTestInstanceTemplate(testTemplateOptions{
		Name:        "gpu-template",
		MachineType: "a2-megagpu-16g",
		DiskSizeGb:  100,
	})

	fakeGce := fakegce.NewGceClient(t, nil).WithZones(map[string][]string{
		"us-central1": {"us-central1-b"},
	}, nil).WithTemplates(gpuTemplate)
	fakeGce.AddCustomMachineType(buildTestMachineType("us-central1-b", "a2-megagpu-16g", 96, 1331200, []*gce_api.MachineTypeAccelerators{
		{
			GuestAcceleratorCount: 16,
			GuestAcceleratorType:  "nvidia-tesla-a100",
		},
	}))

	instances := []*gce_api.Instance{
		buildTestInstance(testInstanceOptions{
			Project:     "my-proj",
			Zone:        "us-central1-b",
			Name:        "gpu-node-1",
			MachineType: "a2-megagpu-16g",
			Status:      "RUNNING",
			MigName:     "gpu-mig",
			TemplateURL: globalTemplateURL("my-proj", "gpu-template"),
		}),
	}

	server, gceService := newTestComputeServer(t, instances)
	defer server.Close()

	rec := NewKwokGceReconciler(kubeClient, fakeGce, gceService, "my-proj", "my-cluster", "us-central1-b", 10*time.Second)
	rec.Reconcile(context.Background())

	var nodeList apiv1.NodeList
	if err := kubeClient.List(context.Background(), &nodeList); err != nil {
		t.Fatalf("failed to list nodes: %v", err)
	}
	if len(nodeList.Items) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodeList.Items))
	}

	node := nodeList.Items[0]
	gpuRes := apiv1.ResourceName("nvidia.com/gpu")
	gpuCapacity := node.Status.Capacity[gpuRes]
	if gpuCapacity.Value() != 16 {
		t.Errorf("expected 16 GPUs, got %d", gpuCapacity.Value())
	}
}

func TestReconcileSkipsStoppingAndTerminated(t *testing.T) {
	kubeClient := newTestKubeClient(t)

	fakeGce := fakegce.NewGceClient(t, nil).WithZones(map[string][]string{
		"us-central1": {"us-central1-a"},
	}, nil)

	instances := []*gce_api.Instance{
		buildTestInstance(testInstanceOptions{
			Project:     "my-proj",
			Zone:        "us-central1-a",
			Name:        "stopping-instance",
			MachineType: "n1-standard-4",
			Status:      "STOPPING",
		}),
		buildTestInstance(testInstanceOptions{
			Project:     "my-proj",
			Zone:        "us-central1-a",
			Name:        "terminated-instance",
			MachineType: "n1-standard-4",
			Status:      "TERMINATED",
		}),
	}

	server, gceService := newTestComputeServer(t, instances)
	defer server.Close()

	rec := NewKwokGceReconciler(kubeClient, fakeGce, gceService, "my-proj", "my-cluster", "us-central1", 10*time.Second)
	rec.Reconcile(context.Background())

	var nodeList apiv1.NodeList
	if err := kubeClient.List(context.Background(), &nodeList); err != nil {
		t.Fatalf("failed to list nodes: %v", err)
	}
	if len(nodeList.Items) != 0 {
		t.Errorf("expected 0 nodes for stopping/terminated instances, got %d", len(nodeList.Items))
	}
}

func newTestComputeServer(t *testing.T, instances []*gce_api.Instance) (*httptest.Server, *gce_api.Service) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := gce_api.InstanceList{
			Items: instances,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	svc, err := gce_api.NewService(context.Background(), option.WithHTTPClient(server.Client()), option.WithEndpoint(server.URL))
	if err != nil {
		t.Fatalf("failed to create GCE service: %v", err)
	}
	return server, svc
}
