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
	"fmt"
	"path"
	"sort"
	"strings"

	gce_api "google.golang.org/api/compute/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodeprediction/fake"
)

// kwokNodeMapper constructs Kubernetes Node objects for KWOK nodes from GCE Instances.
type kwokNodeMapper struct {
	gceClient gce.AutoscalingGceClient
	projectID string
}

func newKwokNodeMapper(gceClient gce.AutoscalingGceClient, projectID string) *kwokNodeMapper {
	return &kwokNodeMapper{
		gceClient: gceClient,
		projectID: projectID,
	}
}

type instanceMetadata struct {
	createdBy   string
	templateURL string
	kubeEnv     string
}

func extractMetadata(inst *gce_api.Instance) instanceMetadata {
	var meta instanceMetadata
	if inst.Metadata == nil {
		return meta
	}
	for _, item := range inst.Metadata.Items {
		if item == nil || item.Value == nil {
			continue
		}
		switch item.Key {
		case "created-by":
			meta.createdBy = *item.Value
		case "instance-template":
			meta.templateURL = *item.Value
		case "kube-env":
			meta.kubeEnv = *item.Value
		}
	}
	return meta
}

// mapInstanceToNode constructs a K8s Node object from a GCE instance for KWOK.
func (m *kwokNodeMapper) mapInstanceToNode(ctx context.Context, inst *gce_api.Instance, zone string) (*apiv1.Node, error) {
	meta := extractMetadata(inst)

	if meta.createdBy == "" {
		return nil, fmt.Errorf("instance %s missing 'created-by' metadata", inst.Name)
	}
	migRef, err := gce.ParseIgmUrlRef(meta.createdBy)
	if err != nil {
		return nil, fmt.Errorf("failed to parse created-by URL %q for %s: %w", meta.createdBy, inst.Name, err)
	}

	if meta.templateURL == "" {
		return nil, fmt.Errorf("instance %s missing 'instance-template' metadata", inst.Name)
	}
	templateName := path.Base(meta.templateURL)
	regional := strings.Contains(meta.templateURL, "/regions/")
	template, err := m.gceClient.FetchMigTemplate(ctx, migRef, templateName, regional)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch template %s for %s: %w", templateName, inst.Name, err)
	}

	machineType := path.Base(inst.MachineType)
	if machineType == "" || machineType == "." {
		if template.Properties != nil {
			machineType = path.Base(template.Properties.MachineType)
		}
	}
	if machineType == "" || machineType == "." {
		return nil, fmt.Errorf("machine type not found for instance %s", inst.Name)
	}

	mt, err := m.gceClient.FetchMachineType(ctx, zone, machineType)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch machine type %s in zone %s: %w", machineType, zone, err)
	}

	var ke gce.KubeEnv
	if meta.kubeEnv != "" {
		ke, err = gce.ParseKubeEnv(inst.Name, meta.kubeEnv)
		if err != nil {
			return nil, fmt.Errorf("failed to parse kube-env for %s: %w", inst.Name, err)
		}
	} else if template != nil {
		if extractedKe, err := gce.ExtractKubeEnv(template); err == nil {
			ke = extractedKe
		}
	}

	node, err := fake.BuildNodeFromTemplate(ctx, migRef, mt, template, ke, inst.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to predict node for %s: %w", inst.Name, err)
	}

	// Strip internal CA simulator pseudo-annotations (e.g. cluster-autoscaler/gce/*)
	// that have invalid annotation keys for the Kubernetes API server.
	for k := range node.Annotations {
		if strings.HasPrefix(k, "cluster-autoscaler/gce/") {
			delete(node.Annotations, k)
		}
	}

	// KWOK specific annotations and configurations
	node.Annotations["kwok.x-k8s.io/node"] = "fake"

	// TODO: b/532099850 - When cloud-controller-manager handles setting ProviderID, replace setting ProviderID here with setting the node.cloudprovider.kubernetes.io/uninitialized taint.
	node.Spec.ProviderID = fmt.Sprintf("gce://%s/%s/%s", m.projectID, zone, inst.Name)

	if labelsFromKubeEnv, err := gce.GetLabelsFromKubeEnv(ke); err == nil && len(labelsFromKubeEnv) > 0 {
		var labelPairs []string
		for k, v := range labelsFromKubeEnv {
			labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", k, v))
		}
		sort.Strings(labelPairs)
		node.Annotations["node.gke.io/last-applied-node-labels"] = strings.Join(labelPairs, ",")
	}
	if taintsFromKubeEnv, err := gce.GetTaintsFromKubeEnv(ke); err == nil && len(taintsFromKubeEnv) > 0 {
		var taintPairs []string
		for _, t := range taintsFromKubeEnv {
			if t.Value == "" {
				taintPairs = append(taintPairs, fmt.Sprintf("%s:%s", t.Key, t.Effect))
			} else {
				taintPairs = append(taintPairs, fmt.Sprintf("%s=%s:%s", t.Key, t.Value, t.Effect))
			}
		}
		sort.Strings(taintPairs)
		node.Annotations["node.gke.io/last-applied-node-taints"] = strings.Join(taintPairs, ",")
	}

	builder := &gce.GceTemplateBuilder{}
	migOsInfo, _ := builder.MigOsInfo(ctx, inst.Name, ke)
	osName := "linux"
	archName := "amd64"
	if migOsInfo != nil {
		if os := string(migOsInfo.Os()); os != "" {
			osName = os
		}
		if arch := migOsInfo.Arch().Name(); arch != "" {
			archName = arch
		}
	}

	node.Status.NodeInfo = apiv1.NodeSystemInfo{
		KubeletVersion:          "v1.35.1-kwok",
		KubeProxyVersion:        "v1.35.1-kwok",
		OperatingSystem:         osName,
		Architecture:            archName,
		ContainerRuntimeVersion: "kwok-0.8.0",
	}

	node.Status.Conditions = []apiv1.NodeCondition{
		{
			Type:               apiv1.NodeReady,
			Status:             apiv1.ConditionUnknown,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "NodeStatusUnknown",
			Message:            "Just created by kwok reconciler",
		},
	}

	return node, nil
}
