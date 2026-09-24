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
	"fmt"
	"sort"
	"strings"
	"testing"

	gce_api "google.golang.org/api/compute/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TODO(b/496548045): Merge GCE test builders with pkg/cloudprovider/gke/nodeprediction/fake
// or pkg/cloudprovider/gke/gkeclient/fake/instancetemplateprediction to unify GCE API resource construction across tests.

func newTestKubeClient(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := apiv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core/v1 to scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

type testTemplateOptions struct {
	Name        string
	MachineType string
	DiskSizeGb  int64
	KubeEnv     string
}

func buildTestInstanceTemplate(opts testTemplateOptions) *gce_api.InstanceTemplate {
	diskSize := opts.DiskSizeGb
	if diskSize == 0 {
		diskSize = 100
	}
	props := &gce_api.InstanceProperties{
		MachineType: opts.MachineType,
		Disks: []*gce_api.AttachedDisk{
			{
				Boot: true,
				InitializeParams: &gce_api.AttachedDiskInitializeParams{
					DiskSizeGb: diskSize,
					DiskType:   "pd-standard",
				},
			},
		},
	}
	if opts.KubeEnv != "" {
		kubeEnvVal := opts.KubeEnv
		props.Metadata = &gce_api.Metadata{
			Items: []*gce_api.MetadataItems{
				{
					Key:   "kube-env",
					Value: &kubeEnvVal,
				},
			},
		}
	}
	return &gce_api.InstanceTemplate{
		Name:       opts.Name,
		Properties: props,
	}
}

type testInstanceOptions struct {
	Project     string
	Zone        string
	Name        string
	MachineType string
	Status      string
	MigName     string
	TemplateURL string
	KubeEnv     string
}

func buildTestInstance(opts testInstanceOptions) *gce_api.Instance {
	inst := &gce_api.Instance{
		Name:   opts.Name,
		Status: opts.Status,
	}
	if opts.Zone != "" && opts.Project != "" {
		inst.Zone = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s", opts.Project, opts.Zone)
	}
	if opts.MachineType != "" {
		if opts.Project != "" && opts.Zone != "" {
			inst.MachineType = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s/machineTypes/%s", opts.Project, opts.Zone, opts.MachineType)
		} else {
			inst.MachineType = opts.MachineType
		}
	}

	var metadataItems []*gce_api.MetadataItems
	if opts.KubeEnv != "" {
		kubeEnvVal := opts.KubeEnv
		metadataItems = append(metadataItems, &gce_api.MetadataItems{
			Key:   "kube-env",
			Value: &kubeEnvVal,
		})
	}
	if opts.MigName != "" {
		createdByVal := fmt.Sprintf("projects/%s/zones/%s/instanceGroupManagers/%s", opts.Project, opts.Zone, opts.MigName)
		metadataItems = append(metadataItems, &gce_api.MetadataItems{
			Key:   "created-by",
			Value: &createdByVal,
		})
	}
	if opts.TemplateURL != "" {
		templateURLVal := opts.TemplateURL
		metadataItems = append(metadataItems, &gce_api.MetadataItems{
			Key:   "instance-template",
			Value: &templateURLVal,
		})
	}
	if len(metadataItems) > 0 {
		inst.Metadata = &gce_api.Metadata{
			Items: metadataItems,
		}
	}
	return inst
}

func buildTestKubeEnv(labels map[string]string, taints []string) string {
	var b strings.Builder
	if len(labels) > 0 {
		var pairs []string
		for k, v := range labels {
			pairs = append(pairs, fmt.Sprintf("%s=%s", k, v))
		}
		sort.Strings(pairs)
		b.WriteString(fmt.Sprintf("NODE_LABELS: %s\n", strings.Join(pairs, ",")))
	}
	if len(taints) > 0 {
		b.WriteString(fmt.Sprintf("NODE_TAINTS: %s\n", strings.Join(taints, ",")))
	}
	return b.String()
}

func buildTestMachineType(zone, name string, cpu int64, memMb int64, accelerators []*gce_api.MachineTypeAccelerators) *gce_api.MachineType {
	return &gce_api.MachineType{
		Name:         name,
		Zone:         zone,
		GuestCpus:    cpu,
		MemoryMb:     memMb,
		Accelerators: accelerators,
	}
}

func globalTemplateURL(project, templateName string) string {
	return fmt.Sprintf("projects/%s/global/instanceTemplates/%s", project, templateName)
}

func regionalTemplateURL(project, region, templateName string) string {
	return fmt.Sprintf("projects/%s/regions/%s/instanceTemplates/%s", project, region, templateName)
}
