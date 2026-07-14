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

package version

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/klog/v2"
)

// Version type represent node version as an array of 4 elements.
type Version [4]int

// FromString parse string representation of node version to an array of 4 elements.
func FromString(nv string) (Version, error) {
	err := fmt.Errorf("invalid node version: %s", nv)
	nv = strings.TrimPrefix(nv, "v")
	// Remove the +<Cx-name> suffix from GKE Frontier versions
	nv = strings.Split(nv, "+")[0]
	sl := strings.Split(nv, "-gke.")
	var result Version
	var patch int
	if len(sl) == 1 {
		// Check if the node version has format A.B.C-gke-rc.D
		sl = strings.Split(nv, "-gke-rc.")
	}
	if len(sl) == 0 || len(sl) > 2 {
		return Version{}, err
	}
	v := strings.Split(sl[0], ".")
	if len(v) != 3 {
		return Version{}, err
	}
	if len(sl) == 1 {
		patch = 0
	} else {
		patch, err = strconv.Atoi(sl[1])
		if err != nil {
			klog.Errorf("Couldn't parse gke-patch version, will use 0 instead, err: %v", err)
			patch = 0
		}
	}
	for i := 0; i <= 2; i++ {
		x, err := strconv.Atoi(v[i])
		if err != nil {
			return Version{}, err
		}
		result[i] = x
	}
	result[3] = patch
	return result, nil
}

// VersionToString return string representation of Version.
func (nv Version) String() string {
	return fmt.Sprintf("%d.%d.%d-gke.%d", nv[0], nv[1], nv[2], nv[3])
}

// LessThan compare current version with input version.
func (nv Version) LessThan(nv2 Version) bool {
	for i := 0; i < len(nv2); i++ {
		if nv[i] < nv2[i] {
			return true
		}
		if nv[i] > nv2[i] {
			return false
		}
	}
	return false
}
