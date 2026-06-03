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
	"fmt"
	"testing"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	lister "github.com/googlecloudplatform/compute-class-api/client/listers/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	corev1 "k8s.io/api/core/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/utils/ptr"
)

type fakeComputeClassLister struct {
	lister.ComputeClassLister
	items map[string]*cccv1.ComputeClass
}

func (f *fakeComputeClassLister) Get(name string) (*cccv1.ComputeClass, error) {
	if item, ok := f.items[name]; ok {
		return item, nil
	}
	return nil, fmt.Errorf("not found")
}

func (f *fakeComputeClassLister) List(selector labels.Selector) (ret []*cccv1.ComputeClass, err error) {
	for _, item := range f.items {
		ret = append(ret, item)
	}
	return ret, nil
}

type fakeCloudProvider struct {
	defaultCCCEnabled bool
}

func (f *fakeCloudProvider) GetClusterInfo() (string, string, string) {
	return "project", "location", "cluster"
}

func (f *fakeCloudProvider) IsAutopilotEnabled() bool {
	return false
}

func (f *fakeCloudProvider) IsDefaultCCCEnabled() bool {
	return f.defaultCCCEnabled
}

func (f *fakeCloudProvider) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return machinetypes.NewMachineConfigProvider(nil)
}

func (f *fakeCloudProvider) GetStandardZones() ([]string, error) {
	return []string{"zone-1", "zone-2"}, nil
}

func (f *fakeCloudProvider) GetAIZones() ([]string, error) {
	return []string{"zone-3", "zone-4"}, nil
}

func (f *fakeCloudProvider) GetAutoprovisioningLocations() []string {
	return []string{"location-1", "location-2"}
}

func (f *fakeCloudProvider) TrimLocationsForMachineConfig(locations []string, machineType string, acceleratorConfig *gke_api_beta.AcceleratorConfig, minCpuPlatform string, diskType string) []string {
	return locations
}

func TestListCrds(t *testing.T) {
	ccc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "ccc-1", UID: "uid-1", ResourceVersion: "123"},
		Spec:       cccv1.ComputeClassSpec{Priorities: []cccv1.Priority{{MachineFamily: ptr.To("e2")}}},
	}
	ccc2 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "ccc-2", UID: "uid-2", ResourceVersion: "456"},
		Spec:       cccv1.ComputeClassSpec{Priorities: []cccv1.Priority{{MachineFamily: ptr.To("n2")}}},
	}

	testCases := []struct {
		name     string
		items    map[string]*cccv1.ComputeClass
		wantLen  int
		wantRule string // Check first rule family of the first CRD if len > 0
	}{
		{
			name:    "CCC list is empty",
			items:   map[string]*cccv1.ComputeClass{},
			wantLen: 0,
		},
		{
			name:     "CCC list contains one entry",
			items:    map[string]*cccv1.ComputeClass{"ccc-1": ccc1},
			wantLen:  1,
			wantRule: "e2",
		},
		{
			name:    "CCC list contains multiple entries",
			items:   map[string]*cccv1.ComputeClass{"ccc-1": ccc1, "ccc-2": ccc2},
			wantLen: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			l := &cccLister{
				ComputeClassLister: &fakeComputeClassLister{items: tc.items},
				provider:           &fakeCloudProvider{},
				crdCache:           make(map[types.UID]cachedCrd),
			}
			crds, err := l.ListCrds()
			assert.NoError(t, err)
			assert.Len(t, crds, tc.wantLen)
			if tc.wantLen == 1 {
				assert.Equal(t, tc.wantRule, crds[0].Rules()[0].MachineFamily())
			}
		})
	}
}

func TestGetCrd(t *testing.T) {
	cccName := "test-ccc"
	ccc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: cccName, UID: "uid-1", ResourceVersion: "123"},
		Spec:       cccv1.ComputeClassSpec{Priorities: []cccv1.Priority{{MachineFamily: ptr.To("e2")}}},
	}

	fakeLister := &fakeComputeClassLister{
		items: map[string]*cccv1.ComputeClass{cccName: ccc1},
	}
	l := &cccLister{
		ComputeClassLister: fakeLister,
		provider:           &fakeCloudProvider{},
		crdCache:           make(map[types.UID]cachedCrd),
	}

	// Case 1: Exists
	crd, err := l.GetCrd(cccName)
	assert.NoError(t, err)
	assert.NotNil(t, crd)
	assert.Equal(t, "e2", crd.Rules()[0].MachineFamily())

	// Case 2: Missing
	crdMissing, errMissing := l.GetCrd("missing")
	assert.Error(t, errMissing)
	assert.Nil(t, crdMissing)
}

func TestPodCrd(t *testing.T) {
	cccName := "test-ccc"
	ccc1 := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: cccName, UID: "uid-1", ResourceVersion: "1234"},
		Spec:       cccv1.ComputeClassSpec{Priorities: []cccv1.Priority{{MachineFamily: ptr.To("e2")}}},
	}
	defaultCCC := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: gkelabels.DefaultCCCName, UID: "uid-default", ResourceVersion: "1234"},
		Spec:       cccv1.ComputeClassSpec{Priorities: []cccv1.Priority{{MachineFamily: ptr.To("n2")}}},
	}

	testCases := []struct {
		name           string
		items          map[string]*cccv1.ComputeClass
		podLabels      map[string]string
		affinity       *corev1.Affinity
		defaultEnabled bool
		wantFound      bool
		wantFamily     string
	}{
		{
			name:       "Pod has CCC label and matching CCC exists",
			items:      map[string]*cccv1.ComputeClass{cccName: ccc1},
			podLabels:  map[string]string{gkelabels.ComputeClassLabel: cccName},
			wantFound:  true,
			wantFamily: "e2",
		},
		{
			name:  "Pod has CCC in node affinity and matching CCC exists",
			items: map[string]*cccv1.ComputeClass{cccName: ccc1},
			affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      gkelabels.ComputeClassLabel,
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{cccName},
									},
								},
							},
						},
					},
				},
			},
			wantFound:  true,
			wantFamily: "e2",
		},
		{
			name:      "Pod has CCC label but matching CCC does not exist",
			items:     map[string]*cccv1.ComputeClass{},
			podLabels: map[string]string{gkelabels.ComputeClassLabel: cccName},
			wantFound: false,
		},
		{
			name:           "Pod has no CCC label, default CCC is enabled and matching CCC exists",
			items:          map[string]*cccv1.ComputeClass{gkelabels.DefaultCCCName: defaultCCC},
			podLabels:      nil,
			defaultEnabled: true,
			wantFound:      true,
			wantFamily:     "n2",
		},
		{
			name:           "Pod has no CCC label, default CCC is enabled but matching CCC does not exist",
			items:          map[string]*cccv1.ComputeClass{},
			podLabels:      nil,
			defaultEnabled: true,
			wantFound:      false,
		},
		{
			name:           "Pod has no CCC label and default CCC is disabled",
			items:          map[string]*cccv1.ComputeClass{gkelabels.DefaultCCCName: defaultCCC},
			podLabels:      nil,
			defaultEnabled: false,
			wantFound:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			l := &cccLister{
				ComputeClassLister: &fakeComputeClassLister{items: tc.items},
				provider:           &fakeCloudProvider{defaultCCCEnabled: tc.defaultEnabled},
				crdCache:           make(map[types.UID]cachedCrd),
			}
			pod := &corev1.Pod{Spec: corev1.PodSpec{NodeSelector: tc.podLabels, Affinity: tc.affinity}}
			crd, _, err := l.PodCrd(pod)

			if tc.wantFound {
				assert.NoError(t, err)
				assert.NotNil(t, crd)
				assert.Equal(t, tc.wantFamily, crd.Rules()[0].MachineFamily())
			} else {
				if err == nil {
					assert.Nil(t, crd)
				}
			}
		})
	}
}

func TestGetCrd_Updates_Cache_when_resource_version_changes(t *testing.T) {
	cccName := "test-ccc"
	cccUID := types.UID("test-uid")

	conditions := make([]metav1.Condition, 0)
	conditions = append(conditions, metav1.Condition{Type: "TestCondition1"})

	ccc := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: cccName, UID: cccUID, ResourceVersion: "213"},
		Spec:       cccv1.ComputeClassSpec{},
		Status:     cccv1.ComputeClassStatus{Conditions: conditions},
	}

	fakeLister := &fakeComputeClassLister{items: map[string]*cccv1.ComputeClass{cccName: ccc}}
	l := &cccLister{
		ComputeClassLister: fakeLister,
		provider:           &fakeCloudProvider{},
		crdCache:           make(map[types.UID]cachedCrd),
	}

	// 1. Initial Fetch
	crd, err := l.GetCrd(cccName)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(crd.Conditions()))

	// 2. Update object status & resource version
	cccUpdate := ccc.DeepCopy()
	cccUpdate.ResourceVersion = "456"
	cccUpdate.Status = cccv1.ComputeClassStatus{Conditions: append(conditions, metav1.Condition{Type: "TestCondition2"})}
	fakeLister.items[cccName] = cccUpdate

	// 3. Fetch again - should reflect update
	crdUpdated, err := l.GetCrd(cccName)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(crdUpdated.Conditions()))
}

func TestLabels(t *testing.T) {
	l := &cccLister{}
	labels := l.Labels()
	assert.Contains(t, labels, gkelabels.ComputeClassLabel)
}

func TestListCrds_CacheCleanup(t *testing.T) {
	cccName := "test-ccc"
	cccUID := types.UID("test-uid")
	ccc := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: cccName, UID: cccUID, ResourceVersion: "123"},
		Spec:       cccv1.ComputeClassSpec{Priorities: []cccv1.Priority{{MachineFamily: ptr.To("e2")}}},
	}

	fakeLister := &fakeComputeClassLister{items: map[string]*cccv1.ComputeClass{cccName: ccc}}
	l := &cccLister{
		ComputeClassLister: fakeLister,
		provider:           &fakeCloudProvider{},
		crdCache:           make(map[types.UID]cachedCrd),
	}

	// 1. Initial List - should populate cache
	crds, err := l.ListCrds()
	assert.NoError(t, err)
	assert.Len(t, crds, 1)

	l.cacheMutex.RLock()
	_, inCache := l.crdCache[cccUID]
	l.cacheMutex.RUnlock()
	assert.True(t, inCache, "UID should be in cache")

	// 2. Remove from lister
	delete(fakeLister.items, cccName)

	// 3. List again - should trigger cleanup
	crds, err = l.ListCrds()
	assert.NoError(t, err)
	assert.Len(t, crds, 0)

	l.cacheMutex.RLock()
	_, inCache = l.crdCache[cccUID]
	l.cacheMutex.RUnlock()
	assert.False(t, inCache, "UID should be removed from cache")
}
