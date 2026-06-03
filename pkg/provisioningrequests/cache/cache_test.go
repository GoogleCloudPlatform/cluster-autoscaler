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

package provreqcache

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

type prDesc struct {
	namespace, name string
}

func TestCache(t *testing.T) {
	tests := []struct {
		name       string
		prs1       []*provreqwrapper.ProvisioningRequest
		prs2       []*provreqwrapper.ProvisioningRequest
		wantPrs    []*prDesc
		notWantPrs []*prDesc
	}{
		{
			name:       "Refresh adds and removes PRs",
			prs1:       []*provreqwrapper.ProvisioningRequest{testPr("ns1", "pr1"), testPr("ns2", "pr1")},
			prs2:       []*provreqwrapper.ProvisioningRequest{testPr("ns1", "pr2"), testPr("ns2", "pr1"), testPr("ns2", "pr2")},
			wantPrs:    []*prDesc{{"ns1", "pr2"}, {"ns2", "pr1"}, {"ns2", "pr2"}},
			notWantPrs: []*prDesc{{"ns1", "pr1"}},
		},
	}
	for _, tc := range tests {
		client := &fakeClient{prs: tc.prs1}
		cache := NewQueuedProvisioningCache(client)
		cache.Refresh()
		client.prs = tc.prs2
		cache.Refresh()
		for _, wantPr := range tc.wantPrs {
			gotPr := cache.PendingProvReq(wantPr.namespace, wantPr.name)
			if gotPr == nil {
				t.Errorf("Want ProvReq cache to contain pr %s/%s, got none", wantPr.namespace, wantPr.name)
			}
		}
		for _, notWantPr := range tc.notWantPrs {
			gotPr := cache.PendingProvReq(notWantPr.namespace, notWantPr.name)
			if gotPr != nil {
				t.Errorf("Want ProvReq cache to not contain pr %s/%s, got one", notWantPr.namespace, notWantPr.name)
			}
		}
	}
}

type fakeClient struct {
	prs []*provreqwrapper.ProvisioningRequest
}

func (c *fakeClient) ProvisioningRequestsInState(state provreqstate.ProvisioningRequestState) ([]*provreqwrapper.ProvisioningRequest, error) {
	return c.prs, nil
}

func (c *fakeClient) ProvisioningRequests() ([]*provreqwrapper.ProvisioningRequest, error) {
	return c.prs, nil
}

func testPr(namespace, name string) *provreqwrapper.ProvisioningRequest {
	return provreqwrapper.NewProvisioningRequest(&prv1.ProvisioningRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: prv1.ProvisioningRequestSpec{
			ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
			PodSets:               []prv1.PodSet{},
		},
		Status: prv1.ProvisioningRequestStatus{
			Conditions: []metav1.Condition{
				{Type: prv1.Accepted, Status: metav1.ConditionFalse},
				{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
				{Type: prv1.Failed, Status: metav1.ConditionFalse},
			},
		},
	}, []*v1.PodTemplate{})
}
