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

package ops

import (
	"context"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/utils/set"
)

// OperationType represents the type of operation to be performed on a batch
// of nodes.
type OperationType uint8

const (
	// NoOp represents no operation.
	NoOp OperationType = 0
	// SuspendOp represents a suspend operation.
	SuspendOp OperationType = 1 << iota
	// ConsumeOp represents a resume operation.
	ConsumeOp
	// AssignBufferOp means the addition of buffer label/taints to nodes via k8s api.
	// There is no need to set Operation.MIG if this type is used. Leaving it as
	// a zero value will make the Operation be batched with any other operations
	// of the same type.
	AssignBufferOp
	// AssignSoftTaintOp means the addition of soft taints to nodes via k8s api.
	AssignSoftTaintOp
)

// String returns the string representation of the operation type.
func (ot OperationType) String() string {
	switch ot {
	case SuspendOp:
		return "SUSPEND"
	case ConsumeOp:
		return "CONSUME"
	case AssignBufferOp:
		return "ASSIGN_BUFFER"
	case AssignSoftTaintOp:
		return "ASSIGN_SOFT_TAINT"
	case NoOp:
		return "NO_OP"
	default:
		return "UNKNOWN"
	}
}

// HasAny returns true if the operation type contains any of the operations in the mask.
func (ot OperationType) HasAny(mask OperationType) bool {
	return ot&mask != 0
}

// Contains returns true if the operation type contains all the operations in the mask.
func (ot OperationType) Contains(mask OperationType) bool {
	return ot&mask == mask
}

// With returns a new OperationType with the operations in the mask added.
func (ot OperationType) With(mask OperationType) OperationType {
	return ot | mask
}

// Without returns a new OperationType with the operations in the mask removed.
func (ot OperationType) Without(mask OperationType) OperationType {
	return ot & (^mask)
}

// Operation is stored in the WorkQueue.
// It is imperative to make sure that all nodes in NodeNames
// belong to the provided MIG.
type Operation struct {
	MIG       gce.GceRef
	Type      OperationType
	NodeNames set.Set[string]
}

// Result represents the output of processing a single operation.
type Result struct {
	// Confirms successful processing of a set of node names.
	Success set.Set[string]
	// Per-node errors to reflect partial failures.
	Errs map[string]error
}

func NewResult() Result {
	return Result{Success: set.New[string](), Errs: make(map[string]error)}
}

func (r *Result) AddErrForNodeSet(err error, nodeNames set.Set[string]) {
	for nodeName := range nodeNames {
		r.Errs[nodeName] = err
	}
}

func (r *Result) AddErrForRefSlice(err error, nodeRefs []gce.GceRef) {
	for _, ref := range nodeRefs {
		r.Errs[ref.Name] = err
	}
}

func (r *Result) AddSuccessForNodeSet(nodeNames set.Set[string]) {
	for nodeName := range nodeNames {
		r.Success.Insert(nodeName)
	}
}

func (r *Result) AddSuccessForRefSlice(nodeRefs []gce.GceRef) {
	for _, ref := range nodeRefs {
		r.Success.Insert(ref.Name)
	}
}

// OperationHandler performs the logic for a specific operation type.
type OperationHandler func(ctx context.Context, op Operation) (Result, error)
