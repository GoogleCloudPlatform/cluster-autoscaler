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
	"time"
)

func TestIsGlobalGceApiRequest(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		// Operations list calls (should be false)
		{"/compute/v1/projects/p/zones/us-central1-a/operations", false},
		{"/compute/beta/projects/p/zones/us-central1-a/operations", false},
		{"/compute/v1/projects/p/regions/us-central1/operations", false},
		{"/compute/beta/projects/p/regions/us-central1/operations", false},
		// Zonal resource calls (should be false)
		{"/compute/v1/projects/p/zones/us-central1-a/instances/i1", false},
		{"/compute/beta/projects/p/zones/us-central1-a/instanceGroupManagers/igm1", false},
		// Regional resource calls (should be false)
		{"/compute/v1/projects/p/regions/us-central1/subnetworks/s1", false},
		{"/compute/beta/projects/p/regions/us-central1/notificationEndpoints/e1", false},
		// Global resource calls (should be true)
		{"/compute/v1/projects/p/global/networks/n1", true},
		{"/compute/v1/projects/p/global/instanceTemplates/t1", true},
		// Metadata / List calls (should be true)
		{"/compute/v1/projects/p/zones/us-central1-a", true},
		{"/compute/v1/projects/p/regions/us-central1", true},
		{"/compute/v1/projects/p/zones", true},
		{"/compute/v1/projects/p/regions", true},
		// Aggregated list calls (should be true)
		{"/compute/v1/projects/p/aggregated/reservations", true},
		{"/compute/beta/projects/p/aggregated/futureReservations", true},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := isGlobalGceApiRequest(tt.path); got != tt.want {
				t.Errorf("isGlobalGceApiRequest(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

type mockTransport struct {
	gotReq *http.Request
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	m.gotReq = req
	return &http.Response{StatusCode: http.StatusOK}, nil
}

func TestGlobalGceApiHeaderRoundTripper(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		initialHeaders map[string]string
		region         string
		wantHeader     string
	}{
		{
			name:       "injects header on global call",
			path:       "/compute/v1/projects/p/global/networks/n1",
			region:     "us-central1",
			wantHeader: "us-central1",
		},
		{
			name:       "does not inject header on zonal call",
			path:       "/compute/v1/projects/p/zones/us-central1-a/instances/i1",
			region:     "us-central1",
			wantHeader: "",
		},
		{
			name:           "does not overwrite existing header",
			path:           "/compute/v1/projects/p/global/networks/n1",
			initialHeaders: map[string]string{quotaRegionHeader: "europe-west1"},
			region:         "us-central1",
			wantHeader:     "europe-west1",
		},
		{
			name:       "does not inject header if region is empty",
			path:       "/compute/v1/projects/p/global/networks/n1",
			region:     "",
			wantHeader: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockTransport{}
			rt := &globalGceApiHeaderRoundTripper{
				delegate: mock,
				region:   tt.region,
			}

			req := httptest.NewRequest("GET", "https://compute.googleapis.com"+tt.path, nil)
			for k, v := range tt.initialHeaders {
				req.Header.Set(k, v)
			}

			_, err := rt.RoundTrip(req)
			if err != nil {
				t.Fatalf("RoundTrip failed: %v", err)
			}

			gotHeader := mock.gotReq.Header.Get(quotaRegionHeader)
			if gotHeader != tt.wantHeader {
				t.Errorf("Header %q = %q, want %q", quotaRegionHeader, gotHeader, tt.wantHeader)
			}
		})
	}
}

func TestGlobalGceApiHeaderRoundTripper_WithGceClient(t *testing.T) {
	tests := []struct {
		name       string
		callFunc   func(client *autoscalingInternalGceClient) error
		wantPath   string
		wantHeader string
		mockResp   string
	}{
		{
			name: "global network fetch injects header",
			callFunc: func(client *autoscalingInternalGceClient) error {
				_, err := client.FetchNetwork("project1", "default")
				return err
			},
			wantPath:   "/compute/v1/projects/project1/global/networks/default",
			wantHeader: "us-central1",
			mockResp:   `{"kind": "compute#network", "name": "default"}`,
		},
		{
			name: "zonal accelerator types list omits header",
			callFunc: func(client *autoscalingInternalGceClient) error {
				_, err := client.FetchAcceleratorTypes("us-central1-a")
				return err
			},
			wantPath:   "/compute/v1/projects/project1/zones/us-central1-a/acceleratorTypes",
			wantHeader: "",
			mockResp:   `{"kind": "compute#acceleratorTypeList", "items": []}`,
		},
		{
			name: "global future reservations list injects header",
			callFunc: func(client *autoscalingInternalGceClient) error {
				_, err := client.FetchFutureReservationsInProject("project1")
				return err
			},
			wantPath:   "/compute/v1/projects/project1/aggregated/futureReservations",
			wantHeader: "us-central1",
			mockResp:   `{"kind": "compute#futureReservationsAggregatedList", "items": {}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			var gotHeader string
			var gotPath string

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				mu.Lock()
				gotPath = req.URL.Path
				gotHeader = req.Header.Get(quotaRegionHeader)
				mu.Unlock()

				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tt.mockResp))
			}))
			defer server.Close()

			rt := &globalGceApiHeaderRoundTripper{
				delegate: http.DefaultTransport,
				region:   "us-central1",
			}

			client := newTestAutoscalingInternalGceClientWithCustomTransport(t, "project1", server.URL+"/compute/v1/", nil, time.Second, rt)

			if err := tt.callFunc(client); err != nil {
				t.Fatalf("callFunc failed: %v", err)
			}

			mu.Lock()
			path := gotPath
			header := gotHeader
			mu.Unlock()

			if path != tt.wantPath {
				t.Errorf("Request path = %q, want %q", path, tt.wantPath)
			}
			if header != tt.wantHeader {
				t.Errorf("Header %q = %q, want %q", quotaRegionHeader, header, tt.wantHeader)
			}
		})
	}
}
