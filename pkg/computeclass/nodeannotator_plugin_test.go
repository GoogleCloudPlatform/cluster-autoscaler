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

package computeclass

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

type mockLister struct {
	mock.Mock
}

func (m *mockLister) ListCrds() ([]crd.CRD, error) {
	args := m.Called()
	if crdsArg := args.Get(0); crdsArg != nil {
		if crds, ok := crdsArg.([]crd.CRD); ok {
			return crds, args.Error(1)
		}
		panic(fmt.Sprintf("mockLister ListCrds: unexpected type %T for arg 0", crdsArg))
	}
	return nil, args.Error(1)
}

type mockAnnotationCloudProvider struct {
	mock.Mock
}

func (m *mockAnnotationCloudProvider) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	args := m.Called(node)
	if ngArg := args.Get(0); ngArg != nil {
		return ngArg.(cloudprovider.NodeGroup), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *mockAnnotationCloudProvider) IsAutopilotEnabled() bool {
	args := m.Called()
	return args.Bool(0)
}

// mockMatcher mocks the Matcher interface (as defined in the matcher file)
type mockMatcher struct {
	mock.Mock
}

func (m *mockMatcher) MatchesCrdLabel(nodeGroup cloudprovider.NodeGroup, crd crd.CRD) bool {
	args := m.Called(nodeGroup, crd)
	return args.Bool(0)
}
func (m *mockMatcher) MatchesCrdConfig(nodeGroup cloudprovider.NodeGroup, crd crd.CRD) bool {
	args := m.Called(nodeGroup, crd)
	return args.Bool(0)
}

func (m *mockMatcher) FirstMatchedRule(nodeGroup cloudprovider.NodeGroup, crd crd.CRD) (bool, int, rules.Rule) {
	args := m.Called(nodeGroup, crd)
	// Handle nil rule return if necessary
	var rule rules.Rule
	if ruleArg := args.Get(2); ruleArg != nil {
		rule = ruleArg.(rules.Rule)
	}
	return args.Bool(0), args.Int(1), rule
}

func (m *mockMatcher) FirstMatchedRuleGroup(nodeGroup cloudprovider.NodeGroup, crd crd.CRD) (bool, int, rules.Rule) {
	return false, -1, nil
}

func createNodeWithLabel(name, cccValue string) *apiv1.Node {
	return &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				gkelabels.ComputeClassLabel: cccValue,
			},
		},
	}
}

func TestCCCPluginGetAnnotation(t *testing.T) {
	testNodeName := "test-node-1"
	testCCCName := "custom-ccc"
	predefinedCCName := "Scale-Out"

	listerErr := errors.New("lister failed")
	providerErr := errors.New("provider failed")

	mockNG := gke.NewTestGkeMigBuilder().
		SetNodePoolName("cool-pool").
		Build()

	existingCrd := crd.NewTestCrd(
		crd.WithLabel(gkelabels.ComputeClassLabel),
		crd.WithName(testCCCName),
	)

	existingCrdScaleUp := crd.NewTestCrd(
		crd.WithLabel(gkelabels.ComputeClassLabel),
		crd.WithName(testCCCName),
		crd.WithScaleUpAnyway(),
	)

	otherCrd := crd.NewTestCrd(
		crd.WithLabel(gkelabels.ComputeClassLabel),
		crd.WithName("other-ccc-name"),
	)

	wrongLabelCrd := crd.NewTestCrd(
		crd.WithLabel("wrong.label/key"),
		crd.WithName(testCCCName),
	)

	testCases := []struct {
		name                string
		node                *apiv1.Node
		mockListerSetup     func(ml *mockLister)
		mockProviderSetup   func(mp *mockAnnotationCloudProvider)
		mockMatcherSetup    func(mm *mockMatcher)
		expectedAnnotations map[string]string
		expectErr           bool
		expectedErrMsg      string
	}{
		{
			name:                "Node is nil",
			node:                nil,
			mockListerSetup:     func(ml *mockLister) {},
			mockProviderSetup:   func(mp *mockAnnotationCloudProvider) {},
			mockMatcherSetup:    func(mm *mockMatcher) {},
			expectedAnnotations: nil,
			expectErr:           false,
		},
		{
			name:                "Node has no labels",
			node:                &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: testNodeName}},
			mockListerSetup:     func(ml *mockLister) {},
			mockProviderSetup:   func(mp *mockAnnotationCloudProvider) {},
			mockMatcherSetup:    func(mm *mockMatcher) {},
			expectedAnnotations: nil,
			expectErr:           false,
		},
		{
			name:                "Node has no ComputeClassLabel",
			node:                &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: testNodeName, Labels: map[string]string{"other": "label"}}},
			mockListerSetup:     func(ml *mockLister) {},
			mockProviderSetup:   func(mp *mockAnnotationCloudProvider) {},
			mockMatcherSetup:    func(mm *mockMatcher) {},
			expectedAnnotations: nil,
			expectErr:           false,
		},
		{
			name:                "Node has predefined ComputeClassLabel",
			node:                createNodeWithLabel(testNodeName, predefinedCCName),
			mockListerSetup:     func(ml *mockLister) {},
			mockProviderSetup:   func(mp *mockAnnotationCloudProvider) {},
			mockMatcherSetup:    func(mm *mockMatcher) {},
			expectedAnnotations: nil,
			expectErr:           false,
		},
		{
			name: "Lister returns error",
			node: createNodeWithLabel(testNodeName, testCCCName),
			mockListerSetup: func(ml *mockLister) {
				ml.On("ListCrds").Return(nil, listerErr).Once()
			},
			mockProviderSetup:   func(mp *mockAnnotationCloudProvider) {},
			mockMatcherSetup:    func(mm *mockMatcher) {},
			expectedAnnotations: nil,
			expectErr:           true,
			expectedErrMsg:      "lister failed",
		},
		{
			name: "CRD doesn't exist (CCC Deleted)",
			node: createNodeWithLabel(testNodeName, testCCCName),
			mockListerSetup: func(ml *mockLister) {
				ml.On("ListCrds").Return([]crd.CRD{wrongLabelCrd, otherCrd}, nil).Once()
			},
			mockProviderSetup:   func(mp *mockAnnotationCloudProvider) {},
			mockMatcherSetup:    func(mm *mockMatcher) {},
			expectedAnnotations: map[string]string{cccPriorityIndexAnnotationKey: cccDeletedAnnotationValue},
			expectErr:           false,
		},
		{
			name: "Provider NodeGroupForNode returns error",
			node: createNodeWithLabel(testNodeName, testCCCName),
			mockListerSetup: func(ml *mockLister) {
				ml.On("ListCrds").Return([]crd.CRD{existingCrd}, nil).Once()
			},
			mockProviderSetup: func(mp *mockAnnotationCloudProvider) {
				mp.On("NodeGroupForNode", mock.AnythingOfType("*v1.Node")).Return(nil, providerErr).Once()
			},
			mockMatcherSetup:    func(mm *mockMatcher) {},
			expectedAnnotations: nil,
			expectErr:           true,
			expectedErrMsg:      "provider failed",
		},
		{
			name: "Provider NodeGroupForNode returns nil NodeGroup",
			node: createNodeWithLabel(testNodeName, testCCCName),
			mockListerSetup: func(ml *mockLister) {
				ml.On("ListCrds").Return([]crd.CRD{existingCrd}, nil).Once()
			},
			mockProviderSetup: func(mp *mockAnnotationCloudProvider) {
				mp.On("NodeGroupForNode", mock.AnythingOfType("*v1.Node")).Return(nil, nil).Once()
			},
			mockMatcherSetup:    func(mm *mockMatcher) {},
			expectedAnnotations: nil,
			expectErr:           true,
			expectedErrMsg:      "NodeGroup not found",
		},
		{
			name: "CRD exists, no rule matches, ScaleUpAnyway=false",
			node: createNodeWithLabel(testNodeName, testCCCName),
			mockListerSetup: func(ml *mockLister) {
				ml.On("ListCrds").Return([]crd.CRD{existingCrd}, nil).Once()
			},
			mockProviderSetup: func(mp *mockAnnotationCloudProvider) {
				mp.On("NodeGroupForNode", mock.AnythingOfType("*v1.Node")).Return(mockNG, nil).Once()
			},
			mockMatcherSetup: func(mm *mockMatcher) {
				mm.On("FirstMatchedRule", mockNG, existingCrd).Return(false, -1, nil).Once()
			},
			expectedAnnotations: map[string]string{cccPriorityIndexAnnotationKey: noRuleMatchingAnnotationValue},
			expectErr:           false,
		},
		{
			name: "CRD exists, no rule matches, ScaleUpAnyway=true",
			node: createNodeWithLabel(testNodeName, testCCCName),
			mockListerSetup: func(ml *mockLister) {
				ml.On("ListCrds").Return([]crd.CRD{existingCrdScaleUp}, nil).Once()
			},
			mockProviderSetup: func(mp *mockAnnotationCloudProvider) {
				mp.On("NodeGroupForNode", mock.AnythingOfType("*v1.Node")).Return(mockNG, nil).Once()
			},
			mockMatcherSetup: func(mm *mockMatcher) {
				mm.On("FirstMatchedRule", mockNG, existingCrdScaleUp).Return(false, -1, nil).Once()
			},
			expectedAnnotations: map[string]string{cccPriorityIndexAnnotationKey: cccScaleUpAnywayAnnotationValue},
			expectErr:           false,
		},
		{
			name: "CRD exists, rule matches at index 3",
			node: createNodeWithLabel(testNodeName, testCCCName),
			mockListerSetup: func(ml *mockLister) {
				ml.On("ListCrds").Return([]crd.CRD{existingCrd}, nil).Once()
			},
			mockProviderSetup: func(mp *mockAnnotationCloudProvider) {
				mp.On("NodeGroupForNode", mock.AnythingOfType("*v1.Node")).Return(mockNG, nil).Once()
			},
			mockMatcherSetup: func(mm *mockMatcher) {
				// Simulate the matcher returning true with index 3
				mm.On("FirstMatchedRule", mockNG, existingCrd).Return(true, 3, nil).Once()
			},
			expectedAnnotations: map[string]string{cccPriorityIndexAnnotationKey: "3"},
			expectErr:           false,
		},
		{
			name: "CRD exists, rule matches at index 0",
			node: createNodeWithLabel(testNodeName, testCCCName),
			mockListerSetup: func(ml *mockLister) {
				ml.On("ListCrds").Return([]crd.CRD{existingCrd}, nil).Once()
			},
			mockProviderSetup: func(mp *mockAnnotationCloudProvider) {
				mp.On("NodeGroupForNode", mock.AnythingOfType("*v1.Node")).Return(mockNG, nil).Once()
			},
			mockMatcherSetup: func(mm *mockMatcher) {
				mm.On("FirstMatchedRule", mockNG, existingCrd).Return(true, 0, nil).Once()
			},
			expectedAnnotations: map[string]string{cccPriorityIndexAnnotationKey: "0"},
			expectErr:           false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			mockL := new(mockLister)
			mockP := new(mockAnnotationCloudProvider)
			mockM := new(mockMatcher)

			tc.mockListerSetup(mockL)
			tc.mockProviderSetup(mockP)
			tc.mockMatcherSetup(mockM)

			plugin := &cccNodeAnnotatorPlugin{
				lister:                  mockL,
				annotationCloudprovider: mockP,
				matcher:                 mockM,
			}

			annotations, err := plugin.GetAnnotation(tc.node)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedErrMsg != "" {
					assert.Contains(t, err.Error(), tc.expectedErrMsg)
				}
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.expectedAnnotations, annotations)

			mockL.AssertExpectations(t)
			mockP.AssertExpectations(t)
			mockM.AssertExpectations(t)
		})
	}
}

func TestCCCPluginName(t *testing.T) {
	// Use mocks with no expectations, or nil if constructor allows,
	// as Name() doesn't depend on them.
	mockL := new(mockLister)
	mockP := new(mockAnnotationCloudProvider)
	mockM := new(mockMatcher)

	plugin := &cccNodeAnnotatorPlugin{lister: mockL, annotationCloudprovider: mockP, matcher: mockM}
	assert.Equal(t, cccNodeAnnotatotPluginName, plugin.Name())
}
