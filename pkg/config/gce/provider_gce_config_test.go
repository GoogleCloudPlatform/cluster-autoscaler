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

package gce

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	provider_gce "k8s.io/cloud-provider-gcp/providers/gce"
)

func TestGetProjectAndLocation(t *testing.T) {
	testCases := []struct {
		name             string
		cfgProject       string
		localZone        string
		explicitLocation string
		regional         bool
		md               fakeMetadata
		wantProject      string
		wantLocation     string
	}{
		{
			name:             "managed: config project-id and explicit location, no metadata queries",
			cfgProject:       "cfg-project",
			explicitLocation: "us-central1-b",
			wantProject:      "cfg-project",
			wantLocation:     "us-central1-b",
		},
		{
			name:             "self-hosted: no config, metadata project and explicit location, no zone query",
			explicitLocation: "us-central1-b",
			md:               fakeMetadata{project: "metadata-project"},
			wantProject:      "metadata-project",
			wantLocation:     "us-central1-b",
		},
		{
			name:             "explicit location wins over config local-zone",
			cfgProject:       "cfg-project",
			localZone:        "us-central1-b",
			explicitLocation: "us-central1-c",
			wantProject:      "cfg-project",
			wantLocation:     "us-central1-c",
		},
		{
			name:             "explicit region is used as is for a regional cluster",
			cfgProject:       "cfg-project",
			localZone:        "us-central1-b",
			explicitLocation: "us-central1",
			regional:         true,
			wantProject:      "cfg-project",
			wantLocation:     "us-central1",
		},
		{
			name:         "config local-zone is used without explicit location",
			cfgProject:   "cfg-project",
			localZone:    "us-central1-b",
			wantProject:  "cfg-project",
			wantLocation: "us-central1-b",
		},
		{
			name:         "config local-zone is reduced to its region for a regional cluster",
			cfgProject:   "cfg-project",
			localZone:    "us-central1-b",
			regional:     true,
			wantProject:  "cfg-project",
			wantLocation: "us-central1",
		},
		{
			name:         "config local-zone that is already a region is kept for a regional cluster",
			cfgProject:   "cfg-project",
			localZone:    "us-central1",
			regional:     true,
			wantProject:  "cfg-project",
			wantLocation: "us-central1",
		},
		{
			name:         "TPC config local-zone is reduced to its region for a regional cluster",
			cfgProject:   "cfg-project",
			localZone:    "u-europe-central2-a",
			regional:     true,
			wantProject:  "cfg-project",
			wantLocation: "u-europe-central2",
		},
		{
			name:         "metadata project and zone are used without config and explicit location",
			md:           fakeMetadata{project: "metadata-project", zone: "us-central1-c"},
			wantProject:  "metadata-project",
			wantLocation: "us-central1-c",
		},
		{
			name:         "metadata zone is reduced to its region for a regional cluster",
			md:           fakeMetadata{project: "metadata-project", zone: "us-central1-c"},
			regional:     true,
			wantProject:  "metadata-project",
			wantLocation: "us-central1",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg *provider_gce.ConfigFile
			if tc.cfgProject != "" || tc.localZone != "" {
				cfg = &provider_gce.ConfigFile{}
				cfg.Global.ProjectID = tc.cfgProject
				cfg.Global.LocalZone = tc.localZone
			}
			project, location, err := GetProjectAndLocation(context.Background(), tc.md, tc.regional, cfg, tc.explicitLocation)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantProject, project)
			assert.Equal(t, tc.wantLocation, location)
		})
	}
}

// fakeMetadata returns the given project and zone. It fails for an empty one, so
// tests notice unexpected metadata server queries.
type fakeMetadata struct{ project, zone string }

func (f fakeMetadata) ProjectIDWithContext(context.Context) (string, error) {
	if f.project == "" {
		return "", errors.New("unexpected project query")
	}
	return f.project, nil
}

func (f fakeMetadata) ZoneWithContext(context.Context) (string, error) {
	if f.zone == "" {
		return "", errors.New("unexpected zone query")
	}
	return f.zone, nil
}
