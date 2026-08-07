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

package capacitybuffers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
	"k8s.io/client-go/discovery"
	kube_client "k8s.io/client-go/kubernetes"
)

func TestGetGkeBufferStrategies(t *testing.T) {
	testCases := []struct {
		name               string
		csnEnabled         bool
		expectedStrategies []string
	}{
		{
			name:               "CSN enabled",
			csnEnabled:         true,
			expectedStrategies: []string{capacitybuffer.ActiveProvisioningStrategy, "", ColdProvisioningStrategy},
		},
		{
			name:               "CSN disabled",
			csnEnabled:         false,
			expectedStrategies: []string{capacitybuffer.ActiveProvisioningStrategy, ""},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			strategies := getGkeBufferStrategies(tc.csnEnabled)
			assert.Equal(t, tc.expectedStrategies, strategies)
		})
	}
}

type mockDiscovery struct {
	discovery.DiscoveryInterface
	serverResourcesForGroupVersionFunc func(groupVersion string) (*metav1.APIResourceList, error)
}

func (m *mockDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	if m.serverResourcesForGroupVersionFunc != nil {
		return m.serverResourcesForGroupVersionFunc(groupVersion)
	}
	return nil, fmt.Errorf("not implemented")
}

type mockKubeClient struct {
	kube_client.Interface
	discoveryClient discovery.DiscoveryInterface
}

func (m *mockKubeClient) Discovery() discovery.DiscoveryInterface {
	return m.discoveryClient
}

func TestIsCapacityBufferCRDPresent(t *testing.T) {
	testCases := []struct {
		name          string
		setupMock     func() *mockDiscovery
		expectPresent bool
		expectErr     bool
	}{
		{
			name: "CRD present",
			setupMock: func() *mockDiscovery {
				return &mockDiscovery{
					serverResourcesForGroupVersionFunc: func(groupVersion string) (*metav1.APIResourceList, error) {
						if groupVersion == "autoscaling.x-k8s.io/v1beta1" {
							return &metav1.APIResourceList{
								GroupVersion: groupVersion,
								APIResources: []metav1.APIResource{
									{Name: "capacitybuffers"},
								},
							}, nil
						}
						return nil, fmt.Errorf("unexpected groupVersion: %s", groupVersion)
					},
				}
			},
			expectPresent: true,
			expectErr:     false,
		},
		{
			name: "CRD missing in group",
			setupMock: func() *mockDiscovery {
				return &mockDiscovery{
					serverResourcesForGroupVersionFunc: func(groupVersion string) (*metav1.APIResourceList, error) {
						if groupVersion == "autoscaling.x-k8s.io/v1beta1" {
							return &metav1.APIResourceList{
								GroupVersion: groupVersion,
								APIResources: []metav1.APIResource{
									{Name: "otherresources"},
								},
							}, nil
						}
						return nil, fmt.Errorf("unexpected groupVersion: %s", groupVersion)
					},
				}
			},
			expectPresent: false,
			expectErr:     false,
		},
		{
			name: "Group missing (NotFound error)",
			setupMock: func() *mockDiscovery {
				return &mockDiscovery{
					serverResourcesForGroupVersionFunc: func(groupVersion string) (*metav1.APIResourceList, error) {
						return nil, k8serrors.NewNotFound(schema.GroupResource{Group: "autoscaling.x-k8s.io", Resource: ""}, "")
					},
				}
			},
			expectPresent: false,
			expectErr:     false,
		},
		{
			name: "Other error",
			setupMock: func() *mockDiscovery {
				return &mockDiscovery{
					serverResourcesForGroupVersionFunc: func(groupVersion string) (*metav1.APIResourceList, error) {
						return nil, fmt.Errorf("internal server error")
					},
				}
			},
			expectPresent: false,
			expectErr:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			discoveryClient := tc.setupMock()
			kubeClient := &mockKubeClient{discoveryClient: discoveryClient}
			present, err := IsCapacityBufferCRDPresent(kubeClient)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectPresent, present)
			}
		})
	}
}
