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

package cfg

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWorkQueueCfg_JSON(t *testing.T) {
	tests := []struct {
		name string
		cfg  WorkQueue
		json string
	}{
		{
			name: "valid max size",
			cfg: WorkQueue{
				MaxSize: 1000,
			},
			json: `{"maxSize":1000}`,
		},
		{
			name: "empty struct",
			cfg:  WorkQueue{},
			json: `{}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Marshal
			data, err := json.Marshal(tc.cfg)
			assert.NoError(t, err)
			assert.JSONEq(t, tc.json, string(data))

			// Unmarshal
			var actual WorkQueue
			err = json.Unmarshal([]byte(tc.json), &actual)
			assert.NoError(t, err)
			assert.Equal(t, tc.cfg, actual)
		})
	}
}

func TestRetryCfg_JSON(t *testing.T) {
	tests := []struct {
		name string
		cfg  Retry
		json string
	}{
		{
			name: "valid retry config",
			cfg: Retry{
				MaxRetries:   5,
				InitialDelay: metav1.Duration{Duration: 10 * time.Minute},
				MaxDelay:     metav1.Duration{Duration: 1 * time.Hour},
			},
			json: `{"maxRetries":5,"initialDelay": "10m0s","maxDelay": "1h0m0s"}`,
		},
		{
			name: "empty struct",
			cfg:  Retry{},
			json: `{"initialDelay":"0s", "maxDelay":"0s"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Marshal
			data, err := json.Marshal(tc.cfg)
			assert.NoError(t, err)
			assert.JSONEq(t, tc.json, string(data))

			// Unmarshal
			var actual Retry
			err = json.Unmarshal([]byte(tc.json), &actual)
			assert.NoError(t, err)
			assert.Equal(t, tc.cfg, actual)
		})
	}
}

func TestDispatcherCfg_JSON(t *testing.T) {
	tests := []struct {
		name string
		cfg  Dispatcher
		json string
	}{
		{
			name: "valid worker count",
			cfg: Dispatcher{
				WorkerCount: 10,
				Retry: Retry{
					InitialDelay: metav1.Duration{Duration: 5 * time.Hour},
				},
			},
			json: `{"workerCount":10, "retry": {"initialDelay":"5h0m0s", "maxDelay":"0s"}}`,
		},
		{
			name: "empty struct",
			cfg:  Dispatcher{},
			json: `{"retry": {"initialDelay":"0s", "maxDelay":"0s"}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Marshal
			data, err := json.Marshal(tc.cfg)
			assert.NoError(t, err)
			assert.JSONEq(t, tc.json, string(data))

			// Unmarshal
			var actual Dispatcher
			err = json.Unmarshal([]byte(tc.json), &actual)
			assert.NoError(t, err)
			assert.Equal(t, tc.cfg, actual)
		})
	}
}

func TestSuspendCfg_JSON(t *testing.T) {
	tests := []struct {
		name string
		cfg  Suspend
		json string
	}{
		{
			name: "valid durations",
			cfg: Suspend{
				MinNodeLifetime: metav1.Duration{Duration: 10 * time.Minute},
				PreSuspendDelay: metav1.Duration{Duration: 5 * time.Minute},
			},
			json: `{"minNodeLifetime":"10m0s","preSuspendDelay":"5m0s"}`,
		},
		{
			name: "zero durations",
			cfg:  Suspend{},
			// metav1.Duration marshals to "0s" even if empty/zero
			json: `{"minNodeLifetime":"0s","preSuspendDelay":"0s"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Marshal
			data, err := json.Marshal(tc.cfg)
			assert.NoError(t, err)
			assert.JSONEq(t, tc.json, string(data))

			// Unmarshal
			var actual Suspend
			err = json.Unmarshal([]byte(tc.json), &actual)
			assert.NoError(t, err)
			assert.Equal(t, tc.cfg, actual)
		})
	}
}

func TestReconciliationCfg_JSON(t *testing.T) {
	tests := []struct {
		name string
		cfg  Reconciliation
		json string
	}{
		{
			name: "valid reconciliation config",
			cfg: Reconciliation{
				MaxInvalidCount: 5,
				Interval:        metav1.Duration{Duration: 10 * time.Minute},
			},
			json: `{"maxInvalidCount":5,"interval": "10m0s"}`,
		},
		{
			name: "empty struct",
			cfg:  Reconciliation{},
			json: `{"interval":"0s"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Marshal
			data, err := json.Marshal(tc.cfg)
			assert.NoError(t, err)
			assert.JSONEq(t, tc.json, string(data))

			// Unmarshal
			var actual Reconciliation
			err = json.Unmarshal([]byte(tc.json), &actual)
			assert.NoError(t, err)
			assert.Equal(t, tc.cfg, actual)
		})
	}
}

func TestStateManagerCfg_JSON(t *testing.T) {
	tests := []struct {
		name string
		cfg  StateManager
		json string
	}{
		{
			name: "valid state manager config",
			cfg: StateManager{
				StopTrackingDelay:   metav1.Duration{Duration: 10 * time.Minute},
				MetricsSyncInterval: metav1.Duration{Duration: 5 * time.Second},
			},
			json: `{"stopTrackingDelay":"10m0s","metricsSyncInterval":"5s"}`,
		},
		{
			name: "empty struct",
			cfg:  StateManager{},
			json: `{"stopTrackingDelay":"0s","metricsSyncInterval":"0s"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Marshal
			data, err := json.Marshal(tc.cfg)
			assert.NoError(t, err)
			assert.JSONEq(t, tc.json, string(data))

			// Unmarshal
			var actual StateManager
			err = json.Unmarshal([]byte(tc.json), &actual)
			assert.NoError(t, err)
			assert.Equal(t, tc.cfg, actual)
		})
	}
}

func TestControllerCfg_JSON(t *testing.T) {
	tests := []struct {
		name string
		cfg  Controller
		json string
	}{
		{
			name: "fully populated composition",
			cfg:  ExampleControllerStruct,
			json: ExampleControllerJSON,
		},
		{
			name: "empty composition",
			cfg:  Controller{},
			json: `{"workQueue":{},
									"dispatcher":{"retry": {"initialDelay":"0s", "maxDelay":"0s"}},
									"suspend":{"minNodeLifetime":"0s","preSuspendDelay":"0s"},
									"reconciliation": {"interval":"0s"},
									"stateManager": {"stopTrackingDelay":"0s","metricsSyncInterval":"0s"}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Marshal
			data, err := json.Marshal(tc.cfg)
			assert.NoError(t, err)
			assert.JSONEq(t, tc.json, string(data))

			// Unmarshal
			var actual Controller
			err = json.Unmarshal([]byte(tc.json), &actual)
			assert.NoError(t, err)
			assert.Equal(t, tc.cfg, actual)
		})
	}
}

func TestControllerCfg_UnmarshalErrors(t *testing.T) {
	tests := []struct {
		name    string
		jsonStr string
	}{
		{
			name:    "invalid max size type",
			jsonStr: `{"workQueue": {"maxSize": "string"}}`,
		},
		{
			name:    "invalid duration format",
			jsonStr: `{"suspend": {"minNodeLifetime": "invalid"}}`,
		},
		{
			name:    "malformed json",
			jsonStr: `{"workQueue": {`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var cfg Controller
			err := json.Unmarshal([]byte(tc.jsonStr), &cfg)
			assert.Error(t, err)
		})
	}
}
