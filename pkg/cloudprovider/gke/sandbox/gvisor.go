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

// Package sandbox defines constants related to sandboxing workloads on GKE.
package sandbox

import (
	"fmt"
	"strings"
)

const (
	// GVisorRuntimeClassName is the string value used in the
	// RuntimeClassName field of the PodSpec to indicate that a given pod
	// should use gVisor as its container runtime.
	GVisorRuntimeClassName = "gvisor"

	// GVisorLabelKey is the label key attached to nodes capable of running
	// gVisor sandboxed pods.
	GVisorLabelKey = "sandbox.gke.io/runtime"

	// GVisorLabelValue is the value attached to the GVisorLabelKey.
	GVisorLabelValue = "gvisor"

	// GVisorTaintKey is the key for a taint on a node capable of running
	// gVisor workloads.
	GVisorTaintKey = "sandbox.gke.io/runtime"

	// GVisorTaintValue is the value for the GVisorTaintKey.
	GVisorTaintValue = "gvisor"
)

const (
	gVisorTypeValue = "gvisor"
)

// Type is the type of sandbox e.g None, gvisor etc.
type Type int

const (
	// None is used to indicate that the pod does not need to be sandboxed.
	None Type = iota
	// GVisor indicates that the pod must be sandboxed using gVisor.
	GVisor
	// Unsupported indicates any unsupported sandbox type that might
	// be requested in a PodSpec.
	Unsupported
)

// String implements Stringer.
func (t Type) String() string {
	switch t {
	case GVisor:
		return gVisorTypeValue
	case None:
		return ""
	default:
		return "Unsupported"
	}
}

// TypeFromString returns the equivalent sandboxType for a given string or
// Unsupported and an error if its nota supported sandboxType.
func TypeFromString(sandboxType string) (Type, error) {
	switch strings.ToLower(sandboxType) {
	case gVisorTypeValue:
		return GVisor, nil
	case "":
		return None, nil
	default:
		return Unsupported, fmt.Errorf("unexpected sandboxType specified: %s", sandboxType)
	}
}
