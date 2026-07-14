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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ExampleControllerJSON can be used to test integration
// with the cfg package.
// The config is set up to create a working CSN Node Controller.
const ExampleControllerJSON string = `{
				"workQueue": {"maxSize": 100},
				"dispatcher": {
					"workerCount": 10,
					"retry": {
						"maxRetries": 3,
						"initialDelay": "5ns",
						"maxDelay": "10ns"
          }
				},
				"suspend": {
					"minNodeLifetime": "10m0s",
					"preSuspendDelay": "10ms"
				},
				"reconciliation": {
					"interval": "10m0s",
					"maxInvalidCount": 1
				},
				"stateManager": {
					"stopTrackingDelay": "10m0s",
					"metricsSyncInterval": "5s"
				}
			}`

// ExampleControllerStruct should be a struct representation of
// ExampleControllerJSON
var ExampleControllerStruct Controller = Controller{
	WorkQueue: WorkQueue{
		MaxSize: 100,
	},
	Dispatcher: Dispatcher{
		WorkerCount: 10,
		Retry: Retry{
			MaxRetries:   3,
			InitialDelay: metav1.Duration{Duration: 5 * time.Nanosecond},
			MaxDelay:     metav1.Duration{Duration: 10 * time.Nanosecond},
		},
	},
	Suspend: Suspend{
		MinNodeLifetime: metav1.Duration{Duration: 10 * time.Minute},
		PreSuspendDelay: metav1.Duration{Duration: 10 * time.Millisecond},
	},
	Reconciliation: Reconciliation{
		Interval:        metav1.Duration{Duration: 10 * time.Minute},
		MaxInvalidCount: 1,
	},
	StateManager: StateManager{
		StopTrackingDelay:   metav1.Duration{Duration: 10 * time.Minute},
		MetricsSyncInterval: metav1.Duration{Duration: 5 * time.Second},
	},
}
