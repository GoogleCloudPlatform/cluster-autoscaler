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

package conditions

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	rmpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	gceapiv1 "google.golang.org/api/compute/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/resourcemanager"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
)

type mockTagClient struct {
	resourcemanager.TagClient
	tagKeys        map[string]*rmpb.TagKey
	tagValues      map[string]*rmpb.TagValue
	iamPermissions *iampb.TestIamPermissionsResponse
}

func (m *mockTagClient) GetTagKey(ctx context.Context, name string) (*rmpb.TagKey, error) {
	key, found := m.tagKeys[name]
	if !found {
		return nil, fmt.Errorf("tag key %s not found", name)
	}
	return key, nil
}

func (m *mockTagClient) GetTagValue(ctx context.Context, name string) (*rmpb.TagValue, error) {
	val, found := m.tagValues[name]
	if !found {
		return nil, fmt.Errorf("tag value %s not found", name)
	}
	return val, nil
}

func (m *mockTagClient) ValidateIamPermissions(ctx context.Context, tagValue string) (*iampb.TestIamPermissionsResponse, error) {
	if m.iamPermissions != nil {
		return m.iamPermissions, nil
	}
	return &iampb.TestIamPermissionsResponse{Permissions: resourcemanager.TagsRequiredIamPermissions}, nil
}

func TestValidateResourceManagerTags(t *testing.T) {
	networkURI := "https://www.googleapis.com/compute/v1/projects/test-project/global/networks/12345"
	testNet := &gceapiv1.Network{
		Name:           "default",
		SelfLink:       "https://www.googleapis.com/compute/v1/projects/test-project/global/networks/default",
		SelfLinkWithId: networkURI,
	}
	projectID := "test-project"
	testKey := "tagKeys/123"
	testVal := "tagValues/456"
	namespacedTestKey := fmt.Sprintf("%s/%s", projectID, "key1")
	namespacedTestVal := fmt.Sprintf("%s/%s", namespacedTestKey, "val1")
	invalidPurposeData := map[string]string{"foo": "12345"}

	testCases := []struct {
		name          string
		crd           crd.CRD
		network       *gceapiv1.Network
		tagClient     resourcemanager.TagClient
		wantCondition *metav1.Condition
	}{
		{
			name: "no tags in metadata",
			crd:  crd.NewTestCrd(),
		},
		{
			name: "empty tags map",
			crd: crd.NewTestCrd(
				crd.WithResourceManagerTags([]crd.Tag{}),
			),
		},
		{
			name: "tag key has prefix but value does not",
			crd: crd.NewTestCrd(
				crd.WithResourceManagerTags([]crd.Tag{
					{
						Key:   testKey,
						Value: "val1",
					},
				}),
			),
			network:   testNet,
			tagClient: &mockTagClient{},
			wantCondition: &metav1.Condition{
				Type:   CrdMisconfiguredCondition,
				Status: metav1.ConditionTrue,
				Reason: RmTagValidationReason,
				Message: fmt.Sprintf(
					RmTagValidationMessage,
					fmt.Sprintf("invalid format for tag key %q and value %q. If key has prefix 'tagKeys/', value must have prefix 'tagValues/' (and vice-versa)", testKey, "val1"),
				),
			},
		},
		{
			name: "tag value has prefix but key does not",
			crd: crd.NewTestCrd(
				crd.WithResourceManagerTags([]crd.Tag{
					{
						Key:   "key1",
						Value: testVal,
					},
				}),
			),
			network:   testNet,
			tagClient: &mockTagClient{},
			wantCondition: &metav1.Condition{
				Type:   CrdMisconfiguredCondition,
				Status: metav1.ConditionTrue,
				Reason: RmTagValidationReason,
				Message: fmt.Sprintf(
					RmTagValidationMessage,
					fmt.Sprintf("invalid format for tag key %q and value %q. If key has prefix 'tagKeys/', value must have prefix 'tagValues/' (and vice-versa)", "key1", testVal),
				),
			},
		},
		{
			name: "tag key not found",
			crd: crd.NewTestCrd(
				crd.WithResourceManagerTags([]crd.Tag{
					{
						Key:   "tagKeys/unknown",
						Value: testVal,
					},
				}),
			),
			network: testNet,
			tagClient: &mockTagClient{
				tagKeys: map[string]*rmpb.TagKey{},
			},
			wantCondition: &metav1.Condition{
				Type:   CrdMisconfiguredCondition,
				Status: metav1.ConditionTrue,
				Reason: RmTagValidationReason,
				Message: fmt.Sprintf(
					RmTagValidationMessage,
					fmt.Sprintf("tag key %q not found", "tagKeys/unknown"),
				),
			},
		},
		{
			name: "tag value not found",
			crd: crd.NewTestCrd(
				crd.WithResourceManagerTags([]crd.Tag{
					{
						Key:   testKey,
						Value: testVal,
					},
				}),
			),
			network: testNet,
			tagClient: &mockTagClient{
				tagKeys: map[string]*rmpb.TagKey{
					testKey: {
						Name:           testKey,
						NamespacedName: fmt.Sprintf("%s/%s", projectID, "tag1"),
						Purpose:        rmpb.Purpose_GCE_FIREWALL,
						PurposeData: map[string]string{
							"network": networkURI,
						},
					},
				},
			},
			wantCondition: &metav1.Condition{
				Type:   CrdMisconfiguredCondition,
				Status: metav1.ConditionTrue,
				Reason: RmTagValidationReason,
				Message: fmt.Sprintf(
					RmTagValidationMessage,
					fmt.Sprintf("tag value %q not found", testVal),
				),
			},
		},
		{
			name: "tag key generic purpose",
			crd: crd.NewTestCrd(
				crd.WithResourceManagerTags([]crd.Tag{
					{
						Key:   testKey,
						Value: testVal,
					},
				}),
			),
			network: testNet,
			tagClient: &mockTagClient{
				tagKeys: map[string]*rmpb.TagKey{
					testKey: {
						Name:    testKey,
						Purpose: rmpb.Purpose_PURPOSE_UNSPECIFIED,
					},
				},
				tagValues: map[string]*rmpb.TagValue{
					testVal: {
						Name: testVal,
					},
				},
			},
		},
		{
			name: "tag key invalid network",
			crd: crd.NewTestCrd(
				crd.WithResourceManagerTags([]crd.Tag{
					{
						Key:   testKey,
						Value: testVal,
					},
				}),
			),
			network: testNet,
			tagClient: &mockTagClient{
				tagKeys: map[string]*rmpb.TagKey{
					testKey: {
						Name:    testKey,
						Purpose: rmpb.Purpose_GCE_FIREWALL,
						PurposeData: map[string]string{
							"network": "https://www.googleapis.com/compute/v1/projects/host-project/global/networks/6789",
						},
					},
				},
			},
			wantCondition: &metav1.Condition{
				Type:   CrdMisconfiguredCondition,
				Status: metav1.ConditionTrue,
				Reason: RmTagValidationReason,
				Message: fmt.Sprintf(
					RmTagValidationMessage,
					fmt.Sprintf("tag key %q is for network %q, but cluster is in network %q", testKey, "https://www.googleapis.com/compute/v1/projects/host-project/global/networks/6789", networkURI),
				),
			},
		},
		{
			name: "gke managed tag",
			crd: crd.NewTestCrd(
				crd.WithResourceManagerTags([]crd.Tag{
					{
						Key:   testKey,
						Value: testVal,
					},
				}),
			),
			network: testNet,
			tagClient: &mockTagClient{
				tagKeys: map[string]*rmpb.TagKey{
					testKey: {
						Name:      testKey,
						ShortName: "gke-managed-tag",
						Purpose:   rmpb.Purpose_GCE_FIREWALL,
						PurposeData: map[string]string{
							"network": networkURI,
						},
					},
				},
			},
			wantCondition: &metav1.Condition{
				Type:   CrdMisconfiguredCondition,
				Status: metav1.ConditionTrue,
				Reason: RmTagValidationReason,
				Message: fmt.Sprintf(
					RmTagValidationMessage,
					fmt.Sprintf("tag key short name %q cannot start with 'gke-managed-'", "gke-managed-tag"),
				),
			},
		},
		{
			name: "permission denied on tag value",
			crd: crd.NewTestCrd(
				crd.WithResourceManagerTags([]crd.Tag{
					{
						Key:   testKey,
						Value: testVal,
					},
				}),
			),
			network: testNet,
			tagClient: &mockTagClient{
				tagKeys: map[string]*rmpb.TagKey{
					testKey: {
						Name:    testKey,
						Purpose: rmpb.Purpose_GCE_FIREWALL,
						PurposeData: map[string]string{
							"network": networkURI,
						},
					},
				},
				tagValues: map[string]*rmpb.TagValue{
					testVal: {
						Name: testVal,
					},
				},
				iamPermissions: &iampb.TestIamPermissionsResponse{},
			},
			wantCondition: &metav1.Condition{
				Type:   CrdMisconfiguredCondition,
				Status: metav1.ConditionTrue,
				Reason: RmTagValidationReason,
				Message: fmt.Sprintf(
					RmTagValidationMessage,
					fmt.Sprintf("service account missing the following required permissions for tag value %q: %v", testVal, resourcemanager.TagsRequiredIamPermissions),
				),
			},
		},
		{
			name: "duplicate tag",
			crd: crd.NewTestCrd(
				crd.WithResourceManagerTags([]crd.Tag{
					{
						Key:   testKey,
						Value: testVal,
					},
					{
						Key:   testKey,
						Value: testVal,
					},
				}),
			),
			network: testNet,
			tagClient: &mockTagClient{
				tagKeys: map[string]*rmpb.TagKey{
					testKey: {
						Name:    testKey,
						Purpose: rmpb.Purpose_GCE_FIREWALL,
						PurposeData: map[string]string{
							"network": networkURI,
						},
					},
				},
				tagValues: map[string]*rmpb.TagValue{
					testVal: {
						Name: testVal,
					},
				},
			},
			wantCondition: &metav1.Condition{
				Type:   CrdMisconfiguredCondition,
				Status: metav1.ConditionTrue,
				Reason: RmTagValidationReason,
				Message: fmt.Sprintf(
					RmTagValidationMessage,
					fmt.Sprintf("duplicate tag keys specified: %s", testKey),
				),
			},
		},
		{
			name: "invalid purpose data",
			crd: crd.NewTestCrd(
				crd.WithResourceManagerTags([]crd.Tag{
					{
						Key:   testKey,
						Value: testVal,
					},
				}),
			),
			network: testNet,
			tagClient: &mockTagClient{
				tagKeys: map[string]*rmpb.TagKey{
					testKey: {
						Name:        testKey,
						Purpose:     rmpb.Purpose_GCE_FIREWALL,
						PurposeData: invalidPurposeData,
					},
				},
				tagValues: map[string]*rmpb.TagValue{
					testVal: {
						Name: testVal,
					},
				},
				iamPermissions: &iampb.TestIamPermissionsResponse{
					Permissions: resourcemanager.TagsRequiredIamPermissions,
				},
			},
			wantCondition: &metav1.Condition{
				Type:   CrdMisconfiguredCondition,
				Status: metav1.ConditionTrue,
				Reason: RmTagValidationReason,
				Message: fmt.Sprintf(
					RmTagValidationMessage,
					fmt.Sprintf("tag key %q has invalid purpose data %q, purpose data key must be network or organization", testKey, invalidPurposeData),
				),
			},
		},
		{
			name: "valid tags with prefixes",
			crd: crd.NewTestCrd(
				crd.WithResourceManagerTags([]crd.Tag{
					{
						Key:   testKey,
						Value: testVal,
					},
				}),
			),
			network: testNet,
			tagClient: &mockTagClient{
				tagKeys: map[string]*rmpb.TagKey{
					testKey: {
						Name:    testKey,
						Purpose: rmpb.Purpose_GCE_FIREWALL,
						PurposeData: map[string]string{
							"network": networkURI,
						},
					},
				},
				tagValues: map[string]*rmpb.TagValue{
					testVal: {
						Name: testVal,
					},
				},
				iamPermissions: &iampb.TestIamPermissionsResponse{
					Permissions: resourcemanager.TagsRequiredIamPermissions,
				},
			},
		},
		{
			name: "valid tags with key namespaced name and val shortname",
			crd: crd.NewTestCrd(
				crd.WithResourceManagerTags([]crd.Tag{
					{
						Key:   fmt.Sprintf("%s/%s", projectID, "key1"),
						Value: "val1",
					},
				}),
			),
			network: testNet,
			tagClient: &mockTagClient{
				tagKeys: map[string]*rmpb.TagKey{
					namespacedTestKey: {
						Name:           testKey,
						NamespacedName: namespacedTestKey,
						Purpose:        rmpb.Purpose_GCE_FIREWALL,
						PurposeData: map[string]string{
							"network": networkURI,
						},
					},
				},
				tagValues: map[string]*rmpb.TagValue{
					namespacedTestVal: {
						Name:           testVal,
						NamespacedName: namespacedTestVal,
					},
				},
				iamPermissions: &iampb.TestIamPermissionsResponse{
					Permissions: resourcemanager.TagsRequiredIamPermissions,
				},
			},
		},
		{
			name: "valid org scoped tags",
			crd: crd.NewTestCrd(
				crd.WithResourceManagerTags([]crd.Tag{
					{
						Key:   testKey,
						Value: testVal,
					},
				}),
			),
			network: testNet,
			tagClient: &mockTagClient{
				tagKeys: map[string]*rmpb.TagKey{
					testKey: {
						Name:    testKey,
						Purpose: rmpb.Purpose_GCE_FIREWALL,
						PurposeData: map[string]string{
							"organization": "12345",
						},
					},
				},
				tagValues: map[string]*rmpb.TagValue{
					testVal: {
						Name: testVal,
					},
				},
				iamPermissions: &iampb.TestIamPermissionsResponse{
					Permissions: resourcemanager.TagsRequiredIamPermissions,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTestProvider().WithNetwork(tc.network).Build()
			checker := &resourceManagerTagsCheck{
				tagClient: tc.tagClient,
				provider:  provider,
				cache:     map[crd.CRD]cccData{},
			}

			got := checker.checkCrd(tc.crd, nil)

			if tc.wantCondition != nil {
				if got == nil {
					t.Errorf("Unexpected condition, got nil, want %v", tc.wantCondition)
				} else if diff := cmp.Diff(tc.wantCondition, got, cmpopts.IgnoreFields(metav1.Condition{}, "LastTransitionTime")); diff != "" {
					t.Errorf("Unexpected condition, diff (-want +got):\n%s", diff)
				}
			} else {
				assert.Nil(t, got)
			}
		})
	}
}
