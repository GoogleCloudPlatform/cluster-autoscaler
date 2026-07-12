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

package gceclient

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestCreateClientAddsRoundTripper(t *testing.T) {
	const expectedRegion = "us-central1"
	const expectedHeader = "X-Compute-API-Quota-Region-Override"
	const expectedProject = "test-project"

	tests := []struct {
		name       string
		callFunc   func(client AutoscalingInternalGceClient) error
		wantHeader string
	}{
		{
			name: "global call injects header",
			callFunc: func(client AutoscalingInternalGceClient) error {
				_, err := client.FetchNetwork("test-project", "test-net")
				return err
			},
			wantHeader: expectedRegion,
		},
		{
			name: "zonal call omits header",
			callFunc: func(client AutoscalingInternalGceClient) error {
				_, err := client.FetchAcceleratorTypes("us-central1-a")
				return err
			},
			wantHeader: "",
		},
		{
			name: "regional call omits header",
			callFunc: func(client AutoscalingInternalGceClient) error {
				_, err := client.FetchResourcePolicies("test-project", "us-central1")
				return err
			},
			wantHeader: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var receivedHeader string
			var mu sync.Mutex

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				receivedHeader = r.Header.Get(expectedHeader)
				mu.Unlock()
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{}`))
			}))
			defer server.Close()

			cfg := GceConfig{
				ProjectId:          expectedProject,
				HttpClient:         server.Client(),
				Region:             expectedRegion,
				Endpoint:           server.URL,
				ExperimentsManager: experiments.NewMockManager(),
			}

			client, err := CreateClient(cfg)
			assert.NoError(t, err)
			assert.NotNil(t, client)

			_ = tt.callFunc(client)

			// Verify that the appropriate header was included (or omitted) in the request.
			mu.Lock()
			actualHeader := receivedHeader
			mu.Unlock()
			assert.Equal(t, tt.wantHeader, actualHeader)
		})
	}
}
