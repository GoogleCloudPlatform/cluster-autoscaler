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

import "fmt"

// VmSize represents the size of the underlying vm.
type VmSize size

// IsUpsizeFrom returns true if no dimension is downsized.
func (s VmSize) IsUpsizeFrom(currentSize VmSize) bool {
	return s.MilliCpus >= currentSize.MilliCpus && s.KBytes >= currentSize.KBytes
}

// IsDownsizeFrom returns true if no dimension is upsized.
func (s VmSize) IsDownsizeFrom(currentSize VmSize) bool {
	return s.MilliCpus <= currentSize.MilliCpus && s.KBytes <= currentSize.KBytes
}

// String returns a human-readable representation of the VmSize struct.
func (s VmSize) String() string {
	return fmt.Sprintf(printTemplate, "VmSize", s.MilliCpus, s.KBytes)
}

// MaxSize returns the maximum size for each dimensions.
func MaxSize(s1, s2 VmSize) VmSize {
	return VmSize{
		MilliCpus: max(s1.MilliCpus, s2.MilliCpus),
		KBytes:    max(s1.KBytes, s2.KBytes),
	}
}

// MinSize returns the minimum size for each dimensions.
func MinSize(s1, s2 VmSize) VmSize {
	return VmSize{
		MilliCpus: min(s1.MilliCpus, s2.MilliCpus),
		KBytes:    min(s1.KBytes, s2.KBytes),
	}
}
