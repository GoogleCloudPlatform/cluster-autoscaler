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

package resizerequestclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gce_api_beta "google.golang.org/api/compute/v0.beta"
	gce_api_v1 "google.golang.org/api/compute/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
)

type clientFactory struct {
	name string
	new  func(t *testing.T, serverURL string, pollInterval, waitTimeout time.Duration) (ResizeRequestClient, func(rrID uint64, opName, opType string))
}

func bothClientFactories() []clientFactory {
	return []clientFactory{
		{
			name: "v1",
			new: func(t *testing.T, serverURL string, pollInterval, waitTimeout time.Duration) (ResizeRequestClient, func(rrID uint64, opName, opType string)) {
				t.Helper()
				c, err := NewResizeRequestClientV1(&http.Client{}, "project1", "test-agent", serverURL, ResizeRequestModeQueued)
				require.NoError(t, err)
				c.operationPollInterval = pollInterval
				c.operationWaitTimeout = waitTimeout
				seedOp := func(rrID uint64, opName, opType string) {
					c.cache.setOperation(rrID, &gce_api_v1.Operation{
						Name:          opName,
						OperationType: opType,
					})
				}
				return c, seedOp
			},
		},
		{
			name: "beta",
			new: func(t *testing.T, serverURL string, pollInterval, waitTimeout time.Duration) (ResizeRequestClient, func(rrID uint64, opName, opType string)) {
				t.Helper()
				c, err := NewResizeRequestClientBeta(&http.Client{}, "project1", "test-agent", serverURL, ResizeRequestModeAtomic)
				require.NoError(t, err)
				c.operationPollInterval = pollInterval
				c.operationWaitTimeout = waitTimeout
				seedOp := func(rrID uint64, opName, opType string) {
					c.cache.setOperation(rrID, &gce_api_beta.Operation{
						Name:          opName,
						OperationType: opType,
					})
				}
				return c, seedOp
			},
		},
	}
}

func TestCreateResizeRequestWithRunDurationAndTransientPollError(t *testing.T) {
	t.Parallel()

	mig := gce.GceRef{Project: "project1", Zone: "us-west4-a", Name: "mig-name"}
	runDur := 3600*time.Second + 500*time.Millisecond

	for _, cf := range bothClientFactories() {
		t.Run(cf.name, func(t *testing.T) {
			t.Parallel()

			var insertCalls atomic.Int32
			var pollCalls atomic.Int32
			var capturedBody map[string]any

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/projects/project1/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests":
					insertCalls.Add(1)
					bodyBytes, err := io.ReadAll(r.Body)
					assert.NoError(t, err)
					assert.NoError(t, json.Unmarshal(bodyBytes, &capturedBody))
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"name":"op-create-1","operationType":"compute.instanceGroupManagerResizeRequests.insert","status":"RUNNING","targetLink":"target-rr"}`))
				case "/projects/project1/zones/us-west4-a/operations/op-create-1":
					switch pollCalls.Add(1) {
					case 1:
						// Transient ZoneOperations.Get error must be retried rather than aborting CreateResizeRequest.
						w.WriteHeader(http.StatusInternalServerError)
						_, _ = w.Write([]byte(`{"error":{"code":500,"message":"transient backend error"}}`))
					case 2:
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte(`{"name":"op-create-1","operationType":"compute.instanceGroupManagerResizeRequests.insert","status":"RUNNING"}`))
					default:
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte(`{"name":"op-create-1","operationType":"compute.instanceGroupManagerResizeRequests.insert","status":"DONE"}`))
					}
				default:
					t.Errorf("unexpected request path: %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()

			client, _ := cf.new(t, server.URL, 1*time.Millisecond, 500*time.Millisecond)
			err := client.CreateResizeRequest(context.Background(), mig, ResizeRequestCreateRequest{
				Name:                 "rr-run-dur",
				ResizeBy:             4,
				RequestedRunDuration: &runDur,
			})
			require.NoError(t, err)
			assert.Equal(t, int32(1), insertCalls.Load())
			assert.Equal(t, int32(3), pollCalls.Load())
			assert.Equal(t, "rr-run-dur", capturedBody["name"])
			assert.Equal(t, float64(4), capturedBody["resizeBy"])
			durMap, ok := capturedBody["requestedRunDuration"].(map[string]any)
			require.True(t, ok, "expected requestedRunDuration in POST body, got %v", capturedBody)
			assert.Equal(t, "3600", durMap["seconds"])
			assert.Equal(t, float64(500000000), durMap["nanos"])
		})
	}
}

func TestCreateResizeRequestErrorsAndTimeout(t *testing.T) {
	t.Parallel()

	mig := gce.GceRef{Project: "project1", Zone: "us-west4-a", Name: "mig-name"}

	for _, cf := range bothClientFactories() {
		t.Run(cf.name+"/insert_http_error", func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":{"code":400,"message":"invalid resizeBy"}}`))
			}))
			defer server.Close()

			client, _ := cf.new(t, server.URL, 1*time.Millisecond, 100*time.Millisecond)
			err := client.CreateResizeRequest(context.Background(), mig, ResizeRequestCreateRequest{Name: "rr-bad", ResizeBy: -1})
			require.Error(t, err)
			assert.ErrorContains(t, err, "ResizeRequests.Insert")
		})

		t.Run(cf.name+"/operation_done_with_errors", func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/projects/project1/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests":
					_, _ = w.Write([]byte(`{"name":"op-err","operationType":"compute.instanceGroupManagerResizeRequests.insert","status":"RUNNING","targetLink":"target-rr"}`))
				case "/projects/project1/zones/us-west4-a/operations/op-err":
					_, _ = w.Write([]byte(`{"name":"op-err","operationType":"compute.instanceGroupManagerResizeRequests.insert","status":"DONE","error":{"errors":[{"code":"QUOTA_EXCEEDED","message":"Insufficient GPU quota"}]}}`))
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()

			client, _ := cf.new(t, server.URL, 1*time.Millisecond, 200*time.Millisecond)
			err := client.CreateResizeRequest(context.Background(), mig, ResizeRequestCreateRequest{Name: "rr-quota", ResizeBy: 8})
			require.Error(t, err)
			var multiErr *ResizeRequestOperationMultiError
			require.True(t, errors.As(err, &multiErr), "expected ResizeRequestOperationMultiError, got %T: %v", err, err)
			require.Len(t, multiErr.Errors, 1)
			assert.Equal(t, "QUOTA_EXCEEDED", multiErr.Errors[0].Code)
		})

		t.Run(cf.name+"/polling_timeout", func(t *testing.T) {
			t.Parallel()
			var polls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/projects/project1/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests":
					_, _ = w.Write([]byte(`{"name":"op-slow","operationType":"compute.instanceGroupManagerResizeRequests.insert","status":"RUNNING","targetLink":"target-rr"}`))
				case "/projects/project1/zones/us-west4-a/operations/op-slow":
					polls.Add(1)
					_, _ = w.Write([]byte(`{"name":"op-slow","operationType":"compute.instanceGroupManagerResizeRequests.insert","status":"RUNNING"}`))
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()

			client, _ := cf.new(t, server.URL, 2*time.Millisecond, 15*time.Millisecond)
			err := client.CreateResizeRequest(context.Background(), mig, ResizeRequestCreateRequest{Name: "rr-slow", ResizeBy: 1})
			require.Error(t, err)
			assert.ErrorContains(t, err, "timeout while waiting for ResizeRequest")
			assert.GreaterOrEqual(t, polls.Load(), int32(1))
		})
	}
}

func TestResizeRequestGetAndListEdgeCases(t *testing.T) {
	t.Parallel()

	mig := gce.GceRef{Project: "project1", Zone: "us-central1-a", Name: "mig-1"}

	for _, cf := range bothClientFactories() {
		t.Run(cf.name+"/get_succeeded_and_cancelled", func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/projects/project1/zones/us-central1-a/instanceGroupManagers/mig-1/resizeRequests/rr-ok":
					_, _ = w.Write([]byte(`{"id":"42","name":"rr-ok","resizeBy":2,"creationTimestamp":"2026-03-01T12:00:00Z","state":"SUCCEEDED","requestedRunDuration":{"seconds":"1800","nanos":250},"status":{}}`))
				case "/projects/project1/zones/us-central1-a/instanceGroupManagers/mig-1/resizeRequests/rr-cancelled":
					_, _ = w.Write([]byte(`{"id":"43","name":"rr-cancelled","resizeBy":1,"creationTimestamp":"2026-03-01T12:05:00Z","state":"CANCELLED","status":{}}`))
				case "/projects/project1/zones/us-central1-a/instanceGroupManagers/mig-1/resizeRequests/rr-missing":
					w.WriteHeader(http.StatusNotFound)
					_, _ = w.Write([]byte(`{"error":{"code":404,"message":"not found"}}`))
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()

			client, _ := cf.new(t, server.URL, time.Millisecond, 100*time.Millisecond)

			gotOK, err := client.ResizeRequest(context.Background(), mig, "rr-ok")
			require.NoError(t, err)
			assert.Equal(t, uint64(42), gotOK.ID)
			assert.Equal(t, ResizeRequestStateSucceeded, gotOK.State)
			require.NotNil(t, gotOK.RequestedRunDuration)
			assert.Equal(t, 1800*time.Second+250*time.Nanosecond, *gotOK.RequestedRunDuration)

			gotCancelled, err := client.ResizeRequest(context.Background(), mig, "rr-cancelled")
			require.NoError(t, err)
			assert.Equal(t, ResizeRequestStateCancelled, gotCancelled.State)

			_, err = client.ResizeRequest(context.Background(), mig, "rr-missing")
			require.Error(t, err)
			assert.ErrorContains(t, err, "ResizeRequests.Get")
		})

		t.Run(cf.name+"/list_http_error", func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":{"code":500,"message":"backend unavailable"}}`))
			}))
			defer server.Close()

			client, _ := cf.new(t, server.URL, time.Millisecond, 100*time.Millisecond)
			got, err := client.ResizeRequests(context.Background(), mig)
			require.Error(t, err)
			assert.Nil(t, got)
			assert.ErrorContains(t, err, "ResizeRequests.List")
		})

		t.Run(cf.name+"/list_partial_parse_errors_still_returns_valid_items", func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{
					"items": [
						{"id":"1","name":"rr-valid","resizeBy":2,"creationTimestamp":"2026-03-01T12:00:00Z","state":"SUCCEEDED","status":{}},
						{"id":"2","name":"rr-bad-time","resizeBy":2,"creationTimestamp":"not-a-timestamp","state":"ACCEPTED","status":{}},
						{"id":"3","name":"rr-bad-state","resizeBy":2,"creationTimestamp":"2026-03-01T12:00:00Z","state":"BOGUS_STATE","status":{}}
					]
				}`))
			}))
			defer server.Close()

			client, _ := cf.new(t, server.URL, time.Millisecond, 100*time.Millisecond)
			got, err := client.ResizeRequests(context.Background(), mig)
			require.Error(t, err)
			assert.ErrorContains(t, err, "time.Parse")
			assert.ErrorContains(t, err, "unknown resize request state")
			require.Len(t, got, 1)
			assert.Equal(t, "rr-valid", got[0].Name)
		})
	}
}

func TestAdvanceResizeRequestCleanUpAdditionalBranches(t *testing.T) {
	t.Parallel()

	baseRR := ResizeRequestStatus{
		ProjectID: "project1",
		Zone:      "us-west4-a",
		MigName:   "mig-name",
		Name:      "rr-1",
		ID:        999,
	}

	for _, cf := range bothClientFactories() {
		t.Run(cf.name+"/cancel_operation_finishes_with_non_condition_not_met_error_retries_cancel", func(t *testing.T) {
			t.Parallel()
			var cancelCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/projects/project1/zones/us-west4-a/instanceGroupManagers/mig-name/resizeRequests/rr-1/cancel":
					switch cancelCalls.Add(1) {
					case 1:
						_, _ = w.Write([]byte(`{"name":"op-cancel-1","operationType":"compute.instanceGroupManagerResizeRequests.cancel","status":"RUNNING"}`))
					default:
						_, _ = w.Write([]byte(`{"name":"op-cancel-2","operationType":"compute.instanceGroupManagerResizeRequests.cancel","status":"RUNNING"}`))
					}
				case "/projects/project1/zones/us-west4-a/operations/op-cancel-1":
					_, _ = w.Write([]byte(`{"name":"op-cancel-1","operationType":"compute.instanceGroupManagerResizeRequests.cancel","status":"DONE","error":{"errors":[{"code":"INTERNAL_ERROR","message":"backend glitch"}]}}`))
				default:
					t.Errorf("unexpected path: %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()

			client, _ := cf.new(t, server.URL, time.Millisecond, 100*time.Millisecond)
			rr := baseRR
			rr.State = ResizeRequestStateAccepted

			// Loop 1: triggers initial Cancel operation.
			require.NoError(t, client.AdvanceResizeRequestCleanUp(context.Background(), rr))
			assert.Equal(t, int32(1), cancelCalls.Load())
			assert.Equal(t, UnspecifiedReportState, client.ReportState(rr))

			// Loop 2: op-cancel-1 finished with INTERNAL_ERROR (not CONDITION_NOT_MET), so client must retry Cancel and NOT mark ToBeReportedState yet.
			require.NoError(t, client.AdvanceResizeRequestCleanUp(context.Background(), rr))
			assert.Equal(t, int32(2), cancelCalls.Load())
			assert.Equal(t, UnspecifiedReportState, client.ReportState(rr))
		})

		t.Run(cf.name+"/delete_trigger_http_error", func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":{"code":500,"message":"delete failed"}}`))
			}))
			defer server.Close()

			client, _ := cf.new(t, server.URL, time.Millisecond, 100*time.Millisecond)
			rr := baseRR
			rr.State = ResizeRequestStateFailed

			err := client.AdvanceResizeRequestCleanUp(context.Background(), rr)
			require.Error(t, err)
			assert.ErrorContains(t, err, "ResizeRequests.Delete")
		})

		t.Run(cf.name+"/creating_state_takes_no_action", func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer server.Close()

			client, _ := cf.new(t, server.URL, time.Millisecond, 100*time.Millisecond)
			rr := baseRR
			rr.State = ResizeRequestStateCreating

			require.NoError(t, client.AdvanceResizeRequestCleanUp(context.Background(), rr))
			assert.Equal(t, int32(0), calls.Load())
		})

		t.Run(cf.name+"/unsupported_cached_operation_type_takes_no_action", func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path == "/projects/project1/zones/us-west4-a/operations/op-insert-1" {
					_, _ = w.Write([]byte(`{"name":"op-insert-1","operationType":"compute.instanceGroupManagerResizeRequests.insert","status":"DONE"}`))
					return
				}
				t.Errorf("unexpected cleanup RPC path: %s", r.URL.Path)
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer server.Close()

			client, seedOp := cf.new(t, server.URL, time.Millisecond, 100*time.Millisecond)
			seedOp(baseRR.ID, "op-insert-1", "compute.instanceGroupManagerResizeRequests.insert")
			rr := baseRR
			rr.State = ResizeRequestStateAccepted

			require.NoError(t, client.AdvanceResizeRequestCleanUp(context.Background(), rr))
			assert.Equal(t, int32(1), calls.Load())
		})
	}
}
