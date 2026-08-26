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

package pod

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestGetMissingPods(t *testing.T) {
	var p = test.BuildTestPod("p1", 100, 1)
	var p2 = test.BuildTestPod("p2", 200, 2)
	var p3 = test.BuildTestPod("p3", 200, 2, func(p *v1.Pod) { p.Namespace = metav1.NamespaceSystem })

	testCases := []struct {
		desc               string
		unchedulableBefore []*v1.Pod
		unchedulableAfter  []*v1.Pod
		wantMissing        []*v1.Pod
	}{
		{
			desc:               "No missing",
			unchedulableBefore: []*v1.Pod{p, p2, p3},
			unchedulableAfter:  []*v1.Pod{p2, p, p3},
			wantMissing:        []*v1.Pod{},
		},
		{
			desc:               "All missing",
			unchedulableBefore: []*v1.Pod{p, p2, p3},
			unchedulableAfter:  []*v1.Pod{},
			wantMissing:        []*v1.Pod{p, p2, p3},
		},
		{
			desc:               "One missing",
			unchedulableBefore: []*v1.Pod{p, p2, p3},
			unchedulableAfter:  []*v1.Pod{p, p3},
			wantMissing:        []*v1.Pod{p2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			missing := GetMissingPods(tc.unchedulableBefore, tc.unchedulableAfter)
			if diff := cmp.Diff(tc.wantMissing, missing); diff != "" {
				t.Errorf("GetMissingPods returned unexpected diff (-want +got):\n%s", diff)
			}
		})
	}
}
