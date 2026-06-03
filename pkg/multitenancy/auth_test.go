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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateTenantProjectTokenURL(t *testing.T) {
	tenantProjectNumber := int64(12345678)
	tenantName := "tenant-1"
	clusterProjectNumber := int64(87654321)
	clusterName := "test-cluster-1"
	location := "us-central1"
	perTenantEndpoint := "https://gkeauth.googleapis.com/v1/projects/%d/locations/%s/tenants/%s:generateTenantToken"
	perTenantProjectEndpoint := "https://gkeauth.googleapis.com/v1/projects/%d/locations/%s/clusters/%s/tenantProjects/%d:generateTenantProjectToken"

	tests := []struct {
		name                 string
		tenantTokenURL       string
		tokenBody            string
		clusterProjectNumber int64
		tenantProjectNumber  int64
		expectedURL          string
		expectError          bool
	}{
		{
			name:                 "Valid per-tenant URL",
			tenantTokenURL:       fmt.Sprintf(perTenantEndpoint, tenantProjectNumber, location, tenantName),
			tokenBody:            fmt.Sprintf(`{"clusterId":%q,"projectNumber":"%d"}`, clusterName, clusterProjectNumber),
			clusterProjectNumber: clusterProjectNumber,
			tenantProjectNumber:  tenantProjectNumber,
			expectedURL:          fmt.Sprintf(perTenantProjectEndpoint, clusterProjectNumber, location, clusterName, tenantProjectNumber),
		},
		{
			name:           "URL not matching per-tenant format (too short)",
			tenantTokenURL: "https://gkeauth.googleapis.com/v1/projects/p/tenants/t:generateTenantToken",
			tokenBody:      fmt.Sprintf(`{"clusterId":"c","projectNumber":"%d"}`, clusterProjectNumber),
			// Should return original URL
			expectedURL: "https://gkeauth.googleapis.com/v1/projects/p/tenants/t:generateTenantToken",
		},
		{
			name:           "URL not matching per-tenant format (wrong suffix)",
			tenantTokenURL: "https://gkeauth.googleapis.com/v1/projects/p/locations/l/tenants/t:somethingElse",
			tokenBody:      fmt.Sprintf(`{"clusterId":"c","projectNumber":"%d"}`, clusterProjectNumber),
			// Should return original URL
			expectedURL: "https://gkeauth.googleapis.com/v1/projects/p/locations/l/tenants/t:somethingElse",
		},
		{
			name:           "URL not matching per-tenant format (wrong prefix)",
			tenantTokenURL: fmt.Sprintf("https://gkeauth.googleapis.com/v2/projects/%d/locations/%s/tenants/%d-%s:generateTenantToken", clusterProjectNumber, location, tenantProjectNumber, tenantName),
			tokenBody:      fmt.Sprintf(`{"clusterId":"c","projectNumber":"%d"}`, clusterProjectNumber),
			// Should return original URL
			expectedURL: fmt.Sprintf("https://gkeauth.googleapis.com/v2/projects/%d/locations/%s/tenants/%d-%s:generateTenantToken", clusterProjectNumber, location, tenantProjectNumber, tenantName),
		},
		{
			name:           "Body missing clusterId",
			tenantTokenURL: "https://gkeauth.googleapis.com/v1/projects/p/locations/l/tenants/t:generateTenantToken",
			tokenBody:      `{"other": "stuff"}`,
			// Should return original URL
			expectedURL: "https://gkeauth.googleapis.com/v1/projects/p/locations/l/tenants/t:generateTenantToken",
		},
		{
			name:           "Invalid JSON body",
			tenantTokenURL: "https://gkeauth.googleapis.com/v1/projects/p/locations/l/tenants/t:generateTenantToken",
			tokenBody:      `{invalid-json}`,
			// Should return error
			expectError: true,
		},
		{
			name:           "Empty TokenURL",
			tenantTokenURL: "",
			expectedURL:    "",
		},
		{
			name:           "URL with extra segments",
			tenantTokenURL: "https://gkeauth.googleapis.com/v1/projects/p/locations/l/tenants/t/extra:generateTenantToken",
			tokenBody:      fmt.Sprintf(`{"clusterId":"c","projectNumber":"%d"}`, clusterProjectNumber),
			// Should return original URL
			expectedURL: "https://gkeauth.googleapis.com/v1/projects/p/locations/l/tenants/t/extra:generateTenantToken",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := &tenantTokenSource{
				authConfig: &AuthConfig{
					TokenURL:  tt.tenantTokenURL,
					TokenBody: tt.tokenBody,
				},
				clusterProjectNumber: tt.clusterProjectNumber,
				tenantProjectNumber:  tt.tenantProjectNumber,
			}

			actualURL, err := ts.generateTenantProjectTokenURL()

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedURL, actualURL)
			}
		})
	}
}
