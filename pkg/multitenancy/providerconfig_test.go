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

package multitenancy

import (
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var providerConfigJSON = `

{
    "apiVersion": "cloud.gke.io/v1",
    "kind": "ProviderConfig",
    "metadata": {
        "creationTimestamp": "2025-01-24T12:59:51Z",
        "generation": 1,
        "labels": {
            "tenancy.gke.io/access-level": "tenant",
            "tenancy.gke.io/tenant": "t1234-tenantname",
            "tenancy.gke.io/project": "t1234"
        },
        "name": "t1234-tenantname",
        "resourceVersion": "29997",
        "uid": "e9287db7-f83c-4dea-9312-a3bf7b5ed47d"
    },
    "spec": {
        "networkConfig": {
            "network": "projects/nicknagi-gke-dev/global/networks/default",
            "subnetInfo": {
                "cidr": "10.0.0.0/16",
                "podRanges": [
                    {
                        "cidr": "10.0.0.0/16",
                        "name": "pod-range-1"
                    }
                ],
                "subnetwork": "projects/nicknagi-gke-dev/regions/us-central1/subnetworks/default"
            }
        },
        "projectID": "nicknagi-gke-dev",
        "projectNumber": 1234
    }
}
`
var providerConfigJSONEmptyPodRanges = `
{
    "spec": {
        "networkConfig": {
            "network": "projects/nicknagi-gke-dev/global/networks/default",
            "subnetInfo": {
                "cidr": "10.0.0.0/16",
                "podRanges": [],
                "subnetwork": "projects/nicknagi-gke-dev/regions/us-central1/subnetworks/default"
            }
        },
        "projectID": "nicknagi-gke-dev",
        "projectNumber": 1234
    }
}
`
var providerConfigJSONMissingPodRangeName = `
{
    "spec": {
        "networkConfig": {
            "network": "projects/nicknagi-gke-dev/global/networks/default",
            "subnetInfo": {
                "cidr": "10.0.0.0/16",
                "podRanges": [
                    {
                        "cidr": "10.0.0.0/16"
                    }
                ],
                "subnetwork": "projects/nicknagi-gke-dev/regions/us-central1/subnetworks/default"
            }
        },
        "projectID": "nicknagi-gke-dev",
        "projectNumber": 1234
    }
}
`

func TestProviderConfigFromUnstructuredHappyPath(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]interface{}{}}
	err := json.Unmarshal([]byte(providerConfigJSON), &u.Object)
	if err != nil {
		t.Errorf("unable to convert to unstructured: %v", err)
		return
	}
	pc, err := providerConfigFromUnstructured(u)
	if err != nil {
		t.Errorf("unable to convert to ProviderConfig: %v", err)
		return
	}
	if pc.Name != "t1234-tenantname" {
		t.Errorf("got providerconfig name: %s, want: %s", pc.Name, "t1234-tenantname")
	}
	if pc.ProjectID != "nicknagi-gke-dev" {
		t.Errorf("got providerconfig projectID: %s, want: %s", pc.ProjectID, "nicknagi-gke-dev")
	}
	if pc.ProjectNumber != 1234 {
		t.Errorf("got providerconfig projectNumber: %d, want: %s", pc.ProjectNumber, "1234")
	}
	if pc.NetworkConfig.Network != "projects/nicknagi-gke-dev/global/networks/default" {
		t.Errorf("got providerconfig network: %s, want: %s", pc.NetworkConfig.Network, "projects/nicknagi-gke-dev/global/networks/default")
	}
	if pc.NetworkConfig.Subnetwork != "projects/nicknagi-gke-dev/regions/us-central1/subnetworks/default" {
		t.Errorf("got providerconfig subnetwork: %s, want: %s", pc.NetworkConfig.Subnetwork, "projects/nicknagi-gke-dev/regions/us-central1/subnetworks/default")
	}
	if pc.NetworkConfig.PodRange != "pod-range-1" {
		t.Errorf("got providerconfig podRange: %s, want: %s", pc.NetworkConfig.PodRange, "pod-range-1")
	}
}
func TestProviderConfigFromUnstructuredErrors(t *testing.T) {
	tests := []struct {
		name               string
		fieldToRemove      []string
		fieldToModifyKey   []string
		fieldToModifyValue interface{}
		jsonData           string
		wantError          bool
	}{
		{
			name: "remove_projectID",
			fieldToRemove: []string{
				"spec", "projectID",
			},
			wantError: true,
		},
		{
			name: "modify_projectID_type",
			fieldToModifyKey: []string{
				"spec", "projectID",
			},
			fieldToModifyValue: float64(1),
			wantError:          true,
		},
		{
			name: "remove_projectNumber",
			fieldToRemove: []string{
				"spec", "projectNumber",
			},
			wantError: true,
		},
		{
			name: "modify_projectNumber_type",
			fieldToModifyKey: []string{
				"spec", "projectNumber",
			},
			fieldToModifyValue: "1234",
			wantError:          true,
		},
		{
			name: "remove_network_config",
			fieldToRemove: []string{
				"spec", "networkConfig",
			},
			wantError: true,
		},
		{
			name: "remove_network",
			fieldToRemove: []string{
				"spec", "networkConfig", "network",
			},
			wantError: true,
		},
		{
			name: "remove_subnetwork",
			fieldToRemove: []string{
				"spec", "networkConfig", "subnetInfo", "subnetwork",
			},
			wantError: true,
		},
		{
			name: "remove_podranges",
			fieldToRemove: []string{
				"spec", "networkConfig", "subnetInfo", "podRanges",
			},
		},
		{
			name:     "empty_podranges",
			jsonData: providerConfigJSONEmptyPodRanges,
		},
		{
			name:      "empty_podranges_name",
			jsonData:  providerConfigJSONMissingPodRangeName,
			wantError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.jsonData == "" {
				tt.jsonData = providerConfigJSON
			}
			u := &unstructured.Unstructured{Object: map[string]interface{}{}}
			err := json.Unmarshal([]byte(tt.jsonData), &u.Object)
			if err != nil {
				t.Errorf("unable to convert to unstructured: %v", err)
				return
			}
			if tt.fieldToRemove != nil {
				unstructured.RemoveNestedField(u.Object, tt.fieldToRemove...)
			}
			if tt.fieldToModifyKey != nil {
				unstructured.SetNestedField(u.Object, tt.fieldToModifyValue, tt.fieldToModifyKey...)
			}
			_, err = providerConfigFromUnstructured(u)
			if tt.wantError && err == nil {
				t.Error("got: nil, want: error")
			}
			if !tt.wantError && err != nil {
				t.Errorf("got: %v, want: nil", err)
			}
		})
	}
}
