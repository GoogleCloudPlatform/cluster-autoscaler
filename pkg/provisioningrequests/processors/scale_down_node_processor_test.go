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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

func basicTestNode(name string, creationTime time.Time) *apiv1.Node {
	return &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(creationTime),
			Labels:            map[string]string{},
		},
	}
}

func basicTestNodeWithResizeRequest(name string, creationTime time.Time, resizeRequest string) *apiv1.Node {
	node := basicTestNode(name, creationTime)
	node.Labels[gkelabels.ProvisioningRequestLabelKey] = resizeRequest
	return node
}

var (
	testTime = time.Date(2023, time.Month(2), 21, 1, 10, 30, 0, time.UTC)
	testNow  = func() time.Time { return testTime }
)

func TestGetScaleDownCandidates(t *testing.T) {
	n1 := basicTestNode("n1", testNow())
	n2 := basicTestNode("n2", testNow().Add(-1*time.Minute))
	n3 := basicTestNode("n3", testNow().Add(-11*time.Minute))
	qNotImmune := basicTestNodeWithResizeRequest("qNotImmune", testNow().Add(-15*time.Minute), "qNotImmune-rr")
	qImmune := basicTestNodeWithResizeRequest("qImmune", testNow().Add(-30*time.Second), "qImmune-rr")
	unknown := basicTestNode("unknown", testNow())

	nMig1 := gke.NewTestGkeMigBuilder().SetGceRefName("nMig1").Build()
	nMig2 := gke.NewTestGkeMigBuilder().SetGceRefName("nMig2").SetQueuedProvisioning(false).Build()
	qMig := gke.NewTestGkeMigBuilder().SetGceRefName("qMig").SetQueuedProvisioning(true).Build()

	nodeToMig := map[*apiv1.Node]*gke.GkeMig{
		n1:         nMig1,
		n2:         nMig2,
		n3:         nMig2,
		qNotImmune: qMig,
		qImmune:    qMig,
	}

	tests := []struct {
		name                  string
		scaleDownUnneededTime time.Duration
		nodes                 []*apiv1.Node
		want                  []*apiv1.Node
		wantEnabled           bool
	}{
		{
			name:                  "Optimized autoscaling profile: scaleDownUnneededTime = 1 min, processor enabled, filtering immune nodes out for additional 9 min",
			scaleDownUnneededTime: 1 * time.Minute,
			nodes:                 []*apiv1.Node{n1, n2, n3, qImmune, qNotImmune},
			want:                  []*apiv1.Node{n1, n2, n3, qNotImmune},
			wantEnabled:           true,
		},
		{
			name:                  "Balanced autoscaling profile: scaleDownUnneededTime = 10 min, no processor",
			scaleDownUnneededTime: 10 * time.Minute,
			wantEnabled:           false,
		},
		{
			name:                  "Custom scaleDownUnneededTime = 15 min, no processor",
			scaleDownUnneededTime: 15 * time.Minute,
			wantEnabled:           false,
		},
		{
			name:                  "node without MIG, not filtered",
			scaleDownUnneededTime: 5 * time.Minute,
			nodes:                 []*apiv1.Node{unknown},
			want:                  []*apiv1.Node{unknown},
			wantEnabled:           true,
		},
	}
	for _, tt := range tests {
		cloudProvider := gke.GkeCloudProviderMock{}
		cloudProvider.On("QueuedProvisioningNodeHasScaleDownImmunity", qImmune, mock.AnythingOfType("*reconciler.QueuedProvisioningMigSpec"), mock.AnythingOfType("time.Time")).Return(true)
		cloudProvider.On("QueuedProvisioningNodeHasScaleDownImmunity", qNotImmune, mock.AnythingOfType("*reconciler.QueuedProvisioningMigSpec"), mock.AnythingOfType("time.Time")).Return(false)

		t.Run(tt.name, func(t *testing.T) {
			p, enabled := NewProvisioningRequestScaleDownNodeProcessor(&cloudProvider, tt.scaleDownUnneededTime)

			assert.Equal(t, enabled, tt.wantEnabled)
			if tt.wantEnabled {
				p.now = testNow
				for _, node := range tt.nodes {
					cloudProvider.On("GkeMigForNode", node).Return(nodeToMig[node], nil).Once()
				}
				got, _ := p.GetScaleDownCandidates(nil, tt.nodes)
				assert.ElementsMatch(t, tt.want, got)
			}
		})
	}
}
