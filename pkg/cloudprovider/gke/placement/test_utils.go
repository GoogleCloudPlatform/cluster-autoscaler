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

package placement

import (
	"maps"
	"slices"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
)

var _ resourcePolicyPullerProvider = &FakeResourcePolicyPullerProvider{}

type FakeResourcePolicyPullerProvider struct {
	resourcePolicies map[string]*gceclient.GceResourcePolicy
	err              error
}

func NewFakeResourcePolicyPullerProvider(rp []*gceclient.GceResourcePolicy, err error) *FakeResourcePolicyPullerProvider {
	rps := map[string]*gceclient.GceResourcePolicy{}
	for _, r := range rp {
		rps[r.Name] = r
	}
	return &FakeResourcePolicyPullerProvider{
		resourcePolicies: rps,
		err:              err,
	}
}

func (f *FakeResourcePolicyPullerProvider) GetResourcePolicy(name string) *gceclient.GceResourcePolicy {
	return f.resourcePolicies[name]
}

func (*FakeResourcePolicyPullerProvider) Run(_ <-chan struct{}) {
	// noop
}

func (f *FakeResourcePolicyPullerProvider) GetResourcePolicies(prj string) ([]*gceclient.GceResourcePolicy, error) {
	return slices.Collect(maps.Values(f.resourcePolicies)), f.err
}
