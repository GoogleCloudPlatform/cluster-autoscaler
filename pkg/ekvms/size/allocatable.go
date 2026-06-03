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

package size

import (
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// Allocatable represents the allocatable size of the node
type Allocatable size

// isGreaterOrEqual checks if s is greater or equal than other is all dimensions.
func (s Allocatable) isGreaterOrEqual(other Allocatable) bool {
	return s.MilliCpus >= other.MilliCpus && s.KBytes >= other.KBytes
}

// IsUpsizeFrom checks that s is greater or equal than other (in all dimensions),
// and s is greater than other in some dimension.
func (s Allocatable) IsUpsizeFrom(other Allocatable) bool {
	return s.isGreaterOrEqual(other) && s != other
}

// isLessOrEqual checks if s is less or equal than other is all dimensions.
func (s Allocatable) isLessOrEqual(other Allocatable) bool {
	return s.MilliCpus <= other.MilliCpus && s.KBytes <= other.KBytes
}

// IsDownsizeFrom checks that s is less or equal than other (in all dimensions),
// and s is less than other in some dimension.
func (s Allocatable) IsDownsizeFrom(other Allocatable) bool {
	return s.isLessOrEqual(other) && s != other
}

// Add increases the size by the other size
func (s *Allocatable) Add(other Allocatable) {
	s.MilliCpus += other.MilliCpus
	s.KBytes += other.KBytes
}

// String returns a human-readable representation of the Allocatable struct.
func (s Allocatable) String() string {
	return fmt.Sprintf(printTemplate, "Allocatable", s.MilliCpus, s.KBytes)
}

// Max returns allocatable that is max over each dimension.
func Max(s1, s2 Allocatable) Allocatable {
	return Allocatable{
		MilliCpus: max(s1.MilliCpus, s2.MilliCpus),
		KBytes:    max(s1.KBytes, s2.KBytes),
	}
}

// Min returns allocatable that is min over each dimension.
func Min(s1, s2 Allocatable) Allocatable {
	return Allocatable{
		MilliCpus: min(s1.MilliCpus, s2.MilliCpus),
		KBytes:    min(s1.KBytes, s2.KBytes),
	}
}

// ResourcesToSize converts ResourceList to EK VM Size.
func ResourcesToSize(resources v1.ResourceList) Allocatable {
	if resources == nil {
		return Allocatable{}
	}
	allocatableCpu := resources[v1.ResourceCPU]
	allocatableMemory := resources[v1.ResourceMemory]
	return Allocatable{
		MilliCpus: allocatableCpu.MilliValue(),
		KBytes:    int64(math.Ceil(float64(allocatableMemory.Value()) / float64(KiB))),
	}
}

// Subtract return (s1 - s2) Allocatable.
func Subtract(s1, s2 Allocatable) Allocatable {
	return Allocatable{
		MilliCpus: s1.MilliCpus - s2.MilliCpus,
		KBytes:    s1.KBytes - s2.KBytes,
	}
}

// Add return (s1 + s2) Allocatable.
func Add(s1, s2 Allocatable) Allocatable {
	return Allocatable{
		MilliCpus: s1.MilliCpus + s2.MilliCpus,
		KBytes:    s1.KBytes + s2.KBytes,
	}
}

// modulo calculates the modulo of x % modulo, and also handles negative values for x.
func modulo(x, mod int64) int64 {
	return ((x % mod) + mod) % mod
}

// RoundUpToIncrement rounds up a value to nearest increment.
func RoundUpToIncrement(x, increment int64) int64 {
	if increment <= 0 {
		klog.Errorf("RoundUpToIncrement: expected positive increment, got %d; continuing anyway", increment)
	}
	remainder := modulo(x, increment)
	if remainder == 0 {
		return x // already rounded
	}
	return x - remainder + increment
}
