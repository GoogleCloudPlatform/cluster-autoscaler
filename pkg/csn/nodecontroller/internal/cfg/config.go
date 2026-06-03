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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Controller contains all configuration parameters for the CSN Node Controller.
// This config needs to be backwards-compatible as the same experiment
// will be used across different CA versions.
type Controller struct {
	// WorkQueue defines settings for the operation work queue.
	WorkQueue WorkQueue `json:"workQueue,omitempty"`
	// Dispatcher defines settings for the operation dispatcher.
	Dispatcher Dispatcher `json:"dispatcher,omitempty"`
	// Suspend defines settings for node suspension behavior.
	Suspend        Suspend        `json:"suspend,omitempty"`
	Reconciliation Reconciliation `json:"reconciliation,omitempty"`
	StateManager   StateManager   `json:"stateManager,omitempty"`
}

// WorkQueue defines the capacity and behavior of the controller's operation queue.
type WorkQueue struct {
	// MaxSize is the maximum number of operations that can be held in the queue.
	MaxSize int `json:"maxSize,omitempty"`
}

// Retry defines the parameters for retrying failed operations.
type Retry struct {
	MaxRetries   int             `json:"maxRetries,omitempty"`
	InitialDelay metav1.Duration `json:"initialDelay,omitempty"`
	MaxDelay     metav1.Duration `json:"maxDelay,omitempty"`
}

// Dispatcher defines the execution model for processing node operations.
type Dispatcher struct {
	// WorkerCount is the number of concurrent goroutines processing operations from the queue.
	WorkerCount int   `json:"workerCount,omitempty"`
	Retry       Retry `json:"retry,omitempty"`
}

// Suspend defines the constraints and timing for node suspension operations.
type Suspend struct {
	// MinNodeLifetime is the minimum age a node must reach before it can be suspended.
	// If per-buffer configuration is introduced, this will be the default.
	MinNodeLifetime metav1.Duration `json:"minNodeLifetime,omitempty"`
	// PreSuspendDelay is the duration to wait for pods that don't tolerate
	// the suspension hard taint to be evicted. After this duration passes,
	// a GCE Suspend operation can be triggered.
	PreSuspendDelay metav1.Duration `json:"preSuspendDelay,omitempty"`
}

type Reconciliation struct {
	// Interval determines how often the reconciliation loop runs.
	Interval metav1.Duration `json:"interval,omitempty"`
	// MaxInvalidCount determines the maximum amount of times a node can have
	// an incorrect status before it's consumed. This is used to mitigate the
	// issue of the CloudProvider cache being stale.
	MaxInvalidCount int `json:"maxInvalidCount,omitempty"`
}

// StateManager defines the parameters for tracking the state of CSN nodes.
type StateManager struct {
	// StopTrackingDelay determines how long a node is tracked after it's no longer a CSN node.
	StopTrackingDelay metav1.Duration `json:"stopTrackingDelay,omitempty"`
	// MetricsSyncInterval determines how often metrics are emitted.
	MetricsSyncInterval metav1.Duration `json:"metricsSyncInterval,omitempty"`
}
