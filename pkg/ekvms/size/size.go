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

const (
	// KiB is kilobyte const.
	KiB = 1024
	// MiB is megabyte const.
	MiB = KiB * KiB
	// GiB is gigabyte const.
	GiB = MiB * KiB
	// MiBToKiB is the multiplication constant for converting from MiB to KiB
	MiBToKiB = 1024
	// GiBToKiB is the multiplication constant for converting from GiB to KiB
	GiBToKiB = 1024 * 1024
)

// size represents size in CPU and memory dimensions.
type size struct {
	MilliCpus int64
	KBytes    int64
}

// printTemplate used to print size type in addition to its values:
// SizeType{value mCPU, value KiB}
const printTemplate = "%s{%d mCPU, %d KiB}"
