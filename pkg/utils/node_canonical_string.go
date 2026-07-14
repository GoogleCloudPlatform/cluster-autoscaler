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

package utils

import (
	"bytes"
	"fmt"
	"sort"

	apiv1 "k8s.io/api/core/v1"
)

// LabelsToCanonicalString returns a stable string representation of a labels map.
func LabelsToCanonicalString(labels map[string]string) string {
	var buffer bytes.Buffer
	sortedLabels := make([]string, len(labels))
	i := 0
	for k, v := range labels {
		sortedLabels[i] = fmt.Sprintf("{%s:%s}", k, v)
		i++
	}
	sort.Strings(sortedLabels)
	for _, l := range sortedLabels {
		buffer.WriteString(l)
	}
	return buffer.String()
}

// TaintsToCanonicalString returns a stable string representation of a taints slice.
func TaintsToCanonicalString(taints []apiv1.Taint) string {
	var buffer bytes.Buffer
	sortedTaints := make([]string, len(taints))
	for i, t := range taints {
		sortedTaints[i] = fmt.Sprintf("{%s}", t.ToString())
	}
	sort.Strings(sortedTaints)
	for _, t := range sortedTaints {
		buffer.WriteString(t)
	}
	return buffer.String()
}
