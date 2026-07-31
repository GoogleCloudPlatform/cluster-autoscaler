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
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
	"k8s.io/client-go/util/flowcontrol"
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

func TestTenantTokenSource_Validation(t *testing.T) {
	tests := []struct {
		name        string
		tokenURL    string
		expectLeak  bool // true if it bypasses validation and would leak token
		expectError string
	}{
		{
			name:        "Malicious external URL",
			tokenURL:    "http://evil.com/v1/projects/12345678/locations/us-central1/tenants/tenant-1:generateTenantToken",
			expectLeak:  false,
			expectError: "is not a trusted Google API domain",
		},
		{
			name:        "Malicious IP address",
			tokenURL:    "http://192.168.1.100/v1/projects/12345678/locations/us-central1/tenants/tenant-1:generateTenantToken",
			expectLeak:  false,
			expectError: "is not a trusted Google API domain",
		},
		{
			name:        "Google APIs URL (trusted)",
			tokenURL:    "https://container.googleapis.com/v1/projects/12345678/locations/us-central1/tenants/tenant-1:generateTenantToken",
			expectLeak:  true, // It should pass validation (and then fail on actual HTTP call in test environment)
			expectError: "",   // We do not expect the "not a trusted" error
		},
		{
			name:        "Staging Google APIs URL (trusted)",
			tokenURL:    "https://staging-container.sandbox.googleapis.com/v1/projects/12345678/locations/us-central1/tenants/tenant-1:generateTenantToken",
			expectLeak:  true,
			expectError: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authConfig := &AuthConfig{
				TokenURL:  tt.tokenURL,
				TokenBody: `{"clusterId":"test-cluster"}`,
			}
			ts := NewTenantTokenSource(authConfig, 87654321, 12345678)
			_, err := ts.Token()

			if tt.expectError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			} else {
				// For trusted URLs, it should pass validation. Since there is no real metadata server or
				// internet access/GKE API in the unit test, it will fail with a different error (e.g., connection refused,
				// or oauth2 error), but it must NOT return the validation error.
				if err != nil {
					assert.NotContains(t, err.Error(), "is not a trusted Google API domain")
				}
			}
		})
	}
}

type mockRoundTripper struct {
	roundTrip func(req *http.Request) (*http.Response, error)
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTrip(req)
}

func TestTenantTokenSource_TokenErrorWrapping(t *testing.T) {
	tests := []struct {
		name          string
		roundTripFunc func(req *http.Request) (*http.Response, error)
		verifyError   func(t *testing.T, err error)
	}{
		{
			name: "Network error wrapping",
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				return nil, errors.New("network fail")
			},
			verifyError: func(t *testing.T, err error) {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "failed to fetch tenant token")
				assert.Contains(t, err.Error(), "network fail")
				var urlErr *url.Error
				if assert.True(t, errors.As(err, &urlErr)) {
					assert.Equal(t, "network fail", urlErr.Err.Error())
				}
			},
		},
		{
			name: "HTTP 404 error wrapping",
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewBufferString(`{"error":{"code":404,"message":"Not Found"}}`)),
					Header:     make(http.Header),
				}, nil
			},
			verifyError: func(t *testing.T, err error) {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "token request failed")

				var gErr *googleapi.Error
				if assert.True(t, errors.As(err, &gErr)) {
					assert.Equal(t, http.StatusNotFound, gErr.Code)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authConfig := &AuthConfig{
				TokenURL:  "https://container.googleapis.com/v1/projects/123/locations/us-central1/tenants/t1:generateTenantToken",
				TokenBody: `{"clusterId":"test-cluster"}`,
			}
			ts := &tenantTokenSource{
				authConfig:           authConfig,
				clusterProjectNumber: 87654321,
				tenantProjectNumber:  12345678,
				tokenURL:             authConfig.TokenURL,
				httpClient: &http.Client{
					Transport: &mockRoundTripper{roundTrip: tt.roundTripFunc},
				},
				throttle: flowcontrol.NewTokenBucketRateLimiter(tokenURLQPS, tokenURLBurst),
			}

			_, err := ts.Token()
			tt.verifyError(t, err)
		})
	}
}
