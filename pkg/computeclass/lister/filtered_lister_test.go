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

package lister

import (
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"

	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
)

func mockCrd(name string, annotations map[string]string) crd.CRD {
	return crd.NewTestCrd(crd.WithName(name), crd.WithAnnotations(annotations), crd.WithLabel(gkelabels.ComputeClassLabel))
}

func TestFilteredCccLister(t *testing.T) {
	stdCrdUnannotated := mockCrd("std-unannotated", nil)
	stdCrdAnnotated := mockCrd("std-annotated", map[string]string{
		options.AutoscalerNameAnnotation: options.StandardAutoscalerName,
	})
	solverCrd := mockCrd("solver-crd", map[string]string{
		options.AutoscalerNameAnnotation: options.SolverAutoscalerName,
	})
	customCrd := mockCrd("custom-crd", map[string]string{
		options.AutoscalerNameAnnotation: "my-custom-autoscaler",
	})

	allCrds := []crd.CRD{stdCrdUnannotated, stdCrdAnnotated, solverCrd, customCrd}
	mockBase := NewMockCrdListerWithLabel(allCrds, gkelabels.ComputeClassLabel)

	testCases := []struct {
		name                   string
		autoscalerNames        []string
		wantHandlesUnannotated bool
		wantOwnedCrds          []string
		wantUnownedCrds        []string
	}{
		{
			name:                   "Solver CA instance",
			autoscalerNames:        []string{options.SolverAutoscalerName},
			wantHandlesUnannotated: false,
			wantOwnedCrds:          []string{"solver-crd"},
			wantUnownedCrds:        []string{"std-unannotated", "std-annotated", "custom-crd"},
		},
		{
			name:                   "Empty autoscalerNames disables filtering (all CRDs owned)",
			autoscalerNames:        nil,
			wantHandlesUnannotated: true,
			wantOwnedCrds:          []string{"std-unannotated", "std-annotated", "solver-crd", "custom-crd"},
			wantUnownedCrds:        nil,
		},
		{
			name:                   "Standard CA instance (* and standard)",
			autoscalerNames:        []string{options.UnannotatedAutoscalerName, options.StandardAutoscalerName},
			wantHandlesUnannotated: true,
			wantOwnedCrds:          []string{"std-unannotated", "std-annotated"},
			wantUnownedCrds:        []string{"solver-crd", "custom-crd"},
		},
		{
			name:                   "Explicit standard-only CA instance (without *)",
			autoscalerNames:        []string{options.StandardAutoscalerName},
			wantHandlesUnannotated: false,
			wantOwnedCrds:          []string{"std-annotated"},
			wantUnownedCrds:        []string{"std-unannotated", "solver-crd", "custom-crd"},
		},
		{
			name:                   "Unannotated-only (*) CA instance",
			autoscalerNames:        []string{options.UnannotatedAutoscalerName},
			wantHandlesUnannotated: true,
			wantOwnedCrds:          []string{"std-unannotated"},
			wantUnownedCrds:        []string{"std-annotated", "solver-crd", "custom-crd"},
		},
		{
			name:                   "Fallback mode handling *, standard, and solver",
			autoscalerNames:        []string{options.UnannotatedAutoscalerName, options.StandardAutoscalerName, options.SolverAutoscalerName},
			wantHandlesUnannotated: true,
			wantOwnedCrds:          []string{"std-unannotated", "std-annotated", "solver-crd"},
			wantUnownedCrds:        []string{"custom-crd"},
		},
		{
			name:                   "Custom user autoscaler instance",
			autoscalerNames:        []string{"my-custom-autoscaler"},
			wantHandlesUnannotated: false,
			wantOwnedCrds:          []string{"custom-crd"},
			wantUnownedCrds:        []string{"std-unannotated", "std-annotated", "solver-crd"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filtered := NewFilteredCccLister(mockBase, tc.autoscalerNames)

			if f, ok := filtered.(*FilteredCccLister); ok {
				assert.Equal(t, tc.wantHandlesUnannotated, f.handlesUnannotated)
			} else {
				assert.Same(t, mockBase, filtered)
			}

			list, err := filtered.ListCrds()
			assert.NoError(t, err)
			var gotListedNames []string
			for _, c := range list {
				gotListedNames = append(gotListedNames, c.Name())
			}
			assert.ElementsMatch(t, tc.wantOwnedCrds, gotListedNames)

			for _, crdName := range tc.wantOwnedCrds {
				// Test GetCrd
				c, err := filtered.GetCrd(crdName)
				assert.NoError(t, err)
				assert.NotNil(t, c, "expected owned CRD %q to be found via GetCrd", crdName)

				// Test Crd(label, name)
				cLabel, err := filtered.Crd(gkelabels.ComputeClassLabel, crdName)
				assert.NoError(t, err)
				assert.NotNil(t, cLabel, "expected owned CRD %q to be found via Crd", crdName)

				// Test PodCrd
				pod := &apiv1.Pod{
					Spec: apiv1.PodSpec{
						NodeSelector: map[string]string{gkelabels.ComputeClassLabel: crdName},
					},
				}
				cPod, namePod, err := filtered.PodCrd(pod)
				assert.NoError(t, err)
				assert.NotNil(t, cPod, "expected owned CRD %q to be found via PodCrd", crdName)
				assert.Equal(t, crdName, namePod)
			}

			for _, crdName := range tc.wantUnownedCrds {
				// Test GetCrd
				c, err := filtered.GetCrd(crdName)
				assert.NoError(t, err)
				assert.Nil(t, c, "expected unowned CRD %q to be filtered out (nil) via GetCrd", crdName)

				// Test Crd(label, name)
				cLabel, err := filtered.Crd(gkelabels.ComputeClassLabel, crdName)
				assert.NoError(t, err)
				assert.Nil(t, cLabel, "expected unowned CRD %q to be filtered out (nil) via Crd", crdName)

				// Test PodCrd
				pod := &apiv1.Pod{
					Spec: apiv1.PodSpec{
						NodeSelector: map[string]string{gkelabels.ComputeClassLabel: crdName},
					},
				}
				cPod, namePod, err := filtered.PodCrd(pod)
				assert.NoError(t, err)
				assert.Nil(t, cPod, "expected unowned CRD %q to be filtered out (nil) via PodCrd", crdName)
				assert.Empty(t, namePod)
			}

		})
	}
}

func TestFilteredCccLister_DefaultHandling(t *testing.T) {
	testCases := []struct {
		name                string
		autoscalerNames     []string
		defaultCrdOwnerName string
		wantDefaultFound    bool
		wantDefaultName     string
	}{
		{
			name:                "Default CCC owned by Standard CA - queried by Standard CA",
			autoscalerNames:     []string{options.StandardAutoscalerName},
			defaultCrdOwnerName: options.StandardAutoscalerName,
			wantDefaultFound:    true,
			wantDefaultName:     "default-ccc",
		},
		{
			name:                "Default CCC owned by Standard CA - queried by Solver CA",
			autoscalerNames:     []string{options.SolverAutoscalerName},
			defaultCrdOwnerName: options.StandardAutoscalerName,
			wantDefaultFound:    false,
			wantDefaultName:     "",
		},
		{
			name:                "Default CCC owned by Solver CA - queried by Solver CA",
			autoscalerNames:     []string{options.SolverAutoscalerName},
			defaultCrdOwnerName: options.SolverAutoscalerName,
			wantDefaultFound:    true,
			wantDefaultName:     "default-ccc",
		},
		{
			name:                "Default CCC owned by Solver CA - queried by Standard CA",
			autoscalerNames:     []string{options.StandardAutoscalerName},
			defaultCrdOwnerName: options.SolverAutoscalerName,
			wantDefaultFound:    false,
			wantDefaultName:     "",
		},
		{
			name:                "Un-annotated Default CCC - queried by Standard CA (*,standard)",
			autoscalerNames:     []string{options.UnannotatedAutoscalerName, options.StandardAutoscalerName},
			defaultCrdOwnerName: "",
			wantDefaultFound:    true,
			wantDefaultName:     "default-ccc",
		},
		{
			name:                "Un-annotated Default CCC - queried by explicit Standard-only CA (no *)",
			autoscalerNames:     []string{options.StandardAutoscalerName},
			defaultCrdOwnerName: "",
			wantDefaultFound:    false,
			wantDefaultName:     "",
		},
		{
			name:                "Un-annotated Default CCC - queried by Solver CA",
			autoscalerNames:     []string{options.SolverAutoscalerName},
			defaultCrdOwnerName: "",
			wantDefaultFound:    false,
			wantDefaultName:     "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var annotations map[string]string
			if tc.defaultCrdOwnerName != "" {
				annotations = map[string]string{options.AutoscalerNameAnnotation: tc.defaultCrdOwnerName}
			}
			defaultCrd := mockCrd("default-ccc", annotations)
			mockBase := NewMockCrdListerWithLabel([]crd.CRD{defaultCrd}, gkelabels.ComputeClassLabel)
			mockBase.SetDefaultCrdName("default-ccc")

			filtered := NewFilteredCccLister(mockBase, tc.autoscalerNames)
			name, _, found := filtered.Default()

			assert.Equal(t, tc.wantDefaultFound, found)
			assert.Equal(t, tc.wantDefaultName, name)
		})
	}
}

func TestIsOwned(t *testing.T) {
	unannotated := map[string]string{}
	standardAnnotated := map[string]string{options.AutoscalerNameAnnotation: options.StandardAutoscalerName}
	solverAnnotated := map[string]string{options.AutoscalerNameAnnotation: options.SolverAutoscalerName}

	// Empty autoscalerNames acts as an "off" switch: all annotations are owned.
	assert.True(t, IsOwned(nil, nil))
	assert.True(t, IsOwned(unannotated, []string{}))
	assert.True(t, IsOwned(standardAnnotated, []string{}))
	assert.True(t, IsOwned(solverAnnotated, []string{}))

	// Unannotated + StandardAutoscalerName owns unannotated and standard, but not solver.
	standardNames := []string{options.UnannotatedAutoscalerName, options.StandardAutoscalerName}
	assert.True(t, IsOwned(unannotated, standardNames))
	assert.True(t, IsOwned(standardAnnotated, standardNames))
	assert.False(t, IsOwned(solverAnnotated, standardNames))

	// SolverAutoscalerName owns only solver.
	assert.False(t, IsOwned(unannotated, []string{options.SolverAutoscalerName}))
	assert.False(t, IsOwned(standardAnnotated, []string{options.SolverAutoscalerName}))
	assert.True(t, IsOwned(solverAnnotated, []string{options.SolverAutoscalerName}))
}
