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

package tpu

import (
	"fmt"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

// InvalidTpuTopologyError - specified topology is invalid
const InvalidTpuTopologyError errors.AutoscalerErrorType = "invalidTpuTopology"

// ErrInvalidTpuTopology represents an error caused by invalid topology for multi-host tpus.
type ErrInvalidTpuTopology struct {
	Prefix   string
	Topology string
	Msg      string
}

// Error returns an error message applicable for the error type.
func (e *ErrInvalidTpuTopology) Error() string {
	return e.Prefix + fmt.Sprintf("specified topology %q is invalid, %s", e.Topology, e.Msg)
}

// Type returns the type of the error.
func (e *ErrInvalidTpuTopology) Type() errors.AutoscalerErrorType {
	return InvalidTpuTopologyError
}

// AddPrefix adds a prefix to the error message, without changing the error type.
func (e *ErrInvalidTpuTopology) AddPrefix(msg string, args ...interface{}) errors.AutoscalerError {
	e.Prefix = fmt.Sprintf(msg, args...) + e.Prefix
	return e
}

// NewInvalidTpuTopologyError creates a specific error type.
func NewInvalidTpuTopologyError(topology, msg string) errors.AutoscalerError {
	return &ErrInvalidTpuTopology{Topology: topology, Msg: msg}
}
