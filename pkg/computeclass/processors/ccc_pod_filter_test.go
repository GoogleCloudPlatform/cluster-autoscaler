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

package processors

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
)

func buildPod(name string, nodeSelector map[string]string) *apiv1.Pod {
	return &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: apiv1.PodSpec{
			NodeSelector: nodeSelector,
		},
	}
}

func TestCccAnnotationPodFilter(t *testing.T) {
	stdCrd := crd.NewTestCrd(
		crd.WithName("std-ccc"),
		crd.WithLabel(gkelabels.ComputeClassLabel),
		crd.WithAnnotations(map[string]string{options.AutoscalerNameAnnotation: options.StandardAutoscalerName}),
	)
	solverCrd := crd.NewTestCrd(
		crd.WithName("solver-ccc"),
		crd.WithLabel(gkelabels.ComputeClassLabel),
		crd.WithAnnotations(map[string]string{options.AutoscalerNameAnnotation: options.SolverAutoscalerName}),
	)

	baseLister := lister.NewMockCrdListerWithLabel([]crd.CRD{stdCrd, solverCrd}, gkelabels.ComputeClassLabel)

	stdPod := buildPod("std-pod", map[string]string{gkelabels.ComputeClassLabel: "std-ccc"})
	solverPod := buildPod("solver-pod", map[string]string{gkelabels.ComputeClassLabel: "solver-ccc"})
	unannotatedPod := buildPod("plain-pod", nil)
	missingCccPod := buildPod("missing-ccc-pod", map[string]string{gkelabels.ComputeClassLabel: "non-existent-ccc"})

	allPods := []*apiv1.Pod{stdPod, solverPod, unannotatedPod, missingCccPod}

	testCases := []struct {
		name            string
		autoscalerNames []string
		wantPodNames    []string
	}{
		{
			name:            "Solver CA instance filters pods",
			autoscalerNames: []string{options.SolverAutoscalerName},
			wantPodNames:    []string{"solver-pod"},
		},
		{
			name:            "Standard CA instance (*,standard) filters pods and claims orphans",
			autoscalerNames: []string{options.UnannotatedAutoscalerName, options.StandardAutoscalerName},
			wantPodNames:    []string{"std-pod", "plain-pod", "missing-ccc-pod"},
		},
		{
			name:            "Explicit standard-only CA instance (no *) claims only std-pod",
			autoscalerNames: []string{options.StandardAutoscalerName},
			wantPodNames:    []string{"std-pod"},
		},
		{
			name:            "Fallback mode instance accepts both",
			autoscalerNames: []string{options.UnannotatedAutoscalerName, options.StandardAutoscalerName, options.SolverAutoscalerName},
			wantPodNames:    []string{"std-pod", "solver-pod", "plain-pod", "missing-ccc-pod"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filteredLister := lister.NewFilteredCccLister(baseLister, tc.autoscalerNames)
			podFilter := NewCccAnnotationPodFilter(filteredLister)

			accepted := podFilter.FilterPodsByAutoscaler(allPods)
			var gotNames []string
			for _, p := range accepted {
				gotNames = append(gotNames, p.Name)
			}
			assert.ElementsMatch(t, tc.wantPodNames, gotNames)

			owned, unowned := podFilter.PartitionPodsByAutoscaler(allPods)
			assert.Equal(t, accepted, owned)
			var wantUnownedNames, gotUnownedNames []string
			for _, p := range allPods {
				if !slices.Contains(tc.wantPodNames, p.Name) {
					wantUnownedNames = append(wantUnownedNames, p.Name)
				}
			}
			for _, p := range unowned {
				gotUnownedNames = append(gotUnownedNames, p.Name)
			}
			assert.ElementsMatch(t, wantUnownedNames, gotUnownedNames)
		})
	}
}
