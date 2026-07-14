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

package selfservice

import (
	"testing"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	container "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

type mockCloudProvider struct {
	isPSC              bool
	defaultPrivateNode bool
	isAutopilot        bool
}

func (m *mockCloudProvider) IsClusterUsingPSCInfrastructure() bool {
	return m.isPSC
}

func (m *mockCloudProvider) GetDefaultEnablePrivateNodes() bool {
	return m.defaultPrivateNode
}

func (m *mockCloudProvider) IsAutopilotEnabled() bool {
	return m.isAutopilot
}

func TestPrivateNodeFromNodepool(t *testing.T) {
	testCases := []struct {
		name         string
		np           *container.NodePool
		wantMetadata Metadata
	}{
		{
			name:         "nil nodepool",
			np:           nil,
			wantMetadata: nil,
		},
		{
			name:         "nil network config",
			np:           &container.NodePool{},
			wantMetadata: nil,
		},
		{
			name: "private nodes enabled",
			np: &container.NodePool{
				NetworkConfig: &container.NodeNetworkConfig{
					EnablePrivateNodes: true,
				},
			},
			wantMetadata: Metadata{
				privateNodeFromLabel: "true",
				privateNodeFromCcc:   "true",
			},
		},
		{
			name: "private nodes disabled",
			np: &container.NodePool{
				NetworkConfig: &container.NodeNetworkConfig{
					EnablePrivateNodes: false,
				},
			},
			wantMetadata: Metadata{
				privateNodeFromLabel: "false",
				privateNodeFromCcc:   "false",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cp := &mockCloudProvider{}
			f := newPrivateNode(cp)
			gotMetadata := f.FromNodepool(tc.np)
			assert.Equal(t, tc.wantMetadata, gotMetadata)
		})
	}
}

func TestPrivateNodeFromLabelRequirements(t *testing.T) {
	testCases := []struct {
		name         string
		req          podrequirements.LabelRequirements
		isPSC        bool
		wantMetadata Metadata
	}{
		{
			name:         "no label",
			req:          podrequirements.NewLabelRequirements(map[string]podrequirements.Values{}),
			isPSC:        true,
			wantMetadata: nil,
		},
		{
			name: "true value, PSC",
			req: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
				gkelabels.PrivateNodeLabel: podrequirements.NewValues("true"),
			}),
			isPSC:        true,
			wantMetadata: Metadata{privateNodeFromLabel: "true"},
		},
		{
			name: "false value, PSC",
			req: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
				gkelabels.PrivateNodeLabel: podrequirements.NewValues("false"),
			}),
			isPSC:        true,
			wantMetadata: Metadata{privateNodeFromLabel: "false"},
		},
		{
			name: "true value, not PSC",
			req: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
				gkelabels.PrivateNodeLabel: podrequirements.NewValues("true"),
			}),
			isPSC:        false,
			wantMetadata: nil,
		},
		{
			name: "invalid value",
			req: podrequirements.NewLabelRequirements(map[string]podrequirements.Values{
				gkelabels.PrivateNodeLabel: podrequirements.NewValues("invalid"),
			}),
			isPSC:        true,
			wantMetadata: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cp := &mockCloudProvider{isPSC: tc.isPSC}
			f := newPrivateNode(cp)
			gotMetadata := f.FromLabelRequirements(tc.req)
			assert.Equal(t, tc.wantMetadata, gotMetadata)
		})
	}
}

func TestPrivateNodeFromCccSpec(t *testing.T) {
	testCases := []struct {
		name         string
		spec         v1.ComputeClassSpec
		isPSC        bool
		wantMetadata Metadata
	}{
		{
			name:         "empty spec",
			spec:         v1.ComputeClassSpec{},
			isPSC:        true,
			wantMetadata: nil,
		},
		{
			name: "nil nodepool config",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: nil,
			},
			isPSC:        true,
			wantMetadata: nil,
		},
		{
			name: "empty ip type",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{IPType: ""},
			},
			isPSC:        true,
			wantMetadata: nil,
		},
		{
			name: "public ip type",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{IPType: "public"},
			},
			isPSC:        true,
			wantMetadata: Metadata{privateNodeFromCcc: "false"},
		},
		{
			name: "private ip type",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{IPType: "private"},
			},
			isPSC:        true,
			wantMetadata: Metadata{privateNodeFromCcc: "true"},
		},
		{
			name: "invalid ip type",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{IPType: "invalid"},
			},
			isPSC:        true,
			wantMetadata: nil,
		},
		{
			name: "private ip type, no PSC",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{IPType: "private"},
			},
			isPSC:        false,
			wantMetadata: nil,
		},
		{
			name: "public ip type, no PSC",
			spec: v1.ComputeClassSpec{
				NodePoolConfig: &v1.NodePoolConfig{IPType: "public"},
			},
			isPSC:        false,
			wantMetadata: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cp := &mockCloudProvider{isPSC: tc.isPSC}
			f := newPrivateNode(cp)
			gotMetadata := f.FromCccSpec(tc.spec)
			assert.Equal(t, tc.wantMetadata, gotMetadata)
		})
	}
}

func TestPrivateNodeToNodePoolLabels(t *testing.T) {
	testCases := []struct {
		name       string
		metadata   Metadata
		wantLabels map[string]string
	}{
		{
			name:       "empty metadata",
			metadata:   Metadata{},
			wantLabels: map[string]string{},
		},
		{
			name:       "private node label true",
			metadata:   Metadata{privateNodeFromLabel: "true"},
			wantLabels: map[string]string{gkelabels.PrivateNodeLabel: "true"},
		},
		{
			name:       "private node label false",
			metadata:   Metadata{privateNodeFromLabel: "false"},
			wantLabels: map[string]string{gkelabels.PrivateNodeLabel: "false"},
		},
		{
			name:       "private node ccc",
			metadata:   Metadata{privateNodeFromCcc: "true"},
			wantLabels: map[string]string{gkelabels.PrivateNodeLabel: "true"},
		},
		{
			name:       "public node ccc",
			metadata:   Metadata{privateNodeFromCcc: "false"},
			wantLabels: map[string]string{gkelabels.PrivateNodeLabel: "false"},
		},
		{
			name: "both label and ccc",
			metadata: Metadata{
				privateNodeFromLabel: "true",
				privateNodeFromCcc:   "false",
			},
			wantLabels: map[string]string{gkelabels.PrivateNodeLabel: "true"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cp := &mockCloudProvider{}
			f := newPrivateNode(cp)
			gotLabels := make(map[string]string)
			f.ToNodePoolLabels(gotLabels, tc.metadata)
			assert.Equal(t, tc.wantLabels, gotLabels)
		})
	}
}

func TestPrivateNodeToNodepool(t *testing.T) {
	testCases := []struct {
		name               string
		metadata           Metadata
		defaultPrivateNode bool
		isAutopilot        bool
		wantNp             *container.NodePool
	}{
		{
			name:     "empty metadata",
			metadata: Metadata{},
			wantNp:   &container.NodePool{},
		},
		{
			name:               "true, default false",
			metadata:           Metadata{privateNodeFromLabel: "true"},
			defaultPrivateNode: false,
			wantNp: &container.NodePool{
				NetworkConfig: &container.NodeNetworkConfig{
					EnablePrivateNodes: true,
					ForceSendFields:    []string{"EnablePrivateNodes"},
				},
			},
		},
		{
			name:               "false, default true",
			metadata:           Metadata{privateNodeFromLabel: "false"},
			defaultPrivateNode: true,
			wantNp: &container.NodePool{
				NetworkConfig: &container.NodeNetworkConfig{
					EnablePrivateNodes: false,
					ForceSendFields:    []string{"EnablePrivateNodes"},
				},
			},
		},
		{
			name:               "true, default true",
			metadata:           Metadata{privateNodeFromLabel: "true"},
			defaultPrivateNode: true,
			wantNp:             &container.NodePool{},
		},
		{
			name:               "false, default false",
			metadata:           Metadata{privateNodeFromLabel: "false"},
			defaultPrivateNode: false,
			wantNp:             &container.NodePool{},
		},
		{
			name:               "true, default false, autopilot",
			metadata:           Metadata{privateNodeFromLabel: "true"},
			defaultPrivateNode: false,
			isAutopilot:        true,
			wantNp: &container.NodePool{
				NetworkConfig: &container.NodeNetworkConfig{
					EnablePrivateNodes: true,
					ForceSendFields:    []string{"EnablePrivateNodes"},
				},
			},
		},
		{
			name:               "false, default true, autopilot",
			metadata:           Metadata{privateNodeFromLabel: "false"},
			defaultPrivateNode: true,
			isAutopilot:        true,
			wantNp: &container.NodePool{
				NetworkConfig: &container.NodeNetworkConfig{
					EnablePrivateNodes: false,
					ForceSendFields:    []string{"EnablePrivateNodes"},
				},
			},
		},
		{
			name:               "ccc true, default false",
			metadata:           Metadata{privateNodeFromCcc: "true"},
			defaultPrivateNode: false,
			wantNp: &container.NodePool{
				NetworkConfig: &container.NodeNetworkConfig{
					EnablePrivateNodes: true,
					ForceSendFields:    []string{"EnablePrivateNodes"},
				},
			},
		},
		{
			name:               "ccc false, default true",
			metadata:           Metadata{privateNodeFromCcc: "false"},
			defaultPrivateNode: true,
			wantNp: &container.NodePool{
				NetworkConfig: &container.NodeNetworkConfig{
					EnablePrivateNodes: false,
					ForceSendFields:    []string{"EnablePrivateNodes"},
				},
			},
		},
		{
			name: "label true, ccc false, default false",
			metadata: Metadata{
				privateNodeFromLabel: "true",
				privateNodeFromCcc:   "false",
			},
			defaultPrivateNode: false,
			wantNp: &container.NodePool{
				NetworkConfig: &container.NodeNetworkConfig{
					EnablePrivateNodes: true,
					ForceSendFields:    []string{"EnablePrivateNodes"},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cp := &mockCloudProvider{defaultPrivateNode: tc.defaultPrivateNode, isAutopilot: tc.isAutopilot}
			f := newPrivateNode(cp)
			gotNp := &container.NodePool{}
			f.ToNodepool(gotNp, tc.metadata)
			assert.Equal(t, tc.wantNp, gotNp)
		})
	}
}

type mockGkeMigSetter struct {
	taints []apiv1.Taint
}

func (m *mockGkeMigSetter) SetTaint(taint apiv1.Taint) {
	if m.taints == nil {
		m.taints = make([]apiv1.Taint, 0)
	}
	m.taints = append(m.taints, taint)
}

func (m *mockGkeMigSetter) SetLocationPolicy(_ string) {

}

func TestPrivateNodeUpdateMig(t *testing.T) {
	tests := []struct {
		name               string
		metadata           Metadata
		defaultPrivateNode bool
		isAutopilot        bool
		expectTaints       []apiv1.Taint
	}{
		{
			name:               "label not found",
			metadata:           Metadata{},
			defaultPrivateNode: false,
			isAutopilot:        true,
			expectTaints:       nil,
		},
		{
			name:               "same as default",
			metadata:           Metadata{privateNodeFromLabel: "false"},
			defaultPrivateNode: false,
			isAutopilot:        true,
			expectTaints:       nil,
		},
		{
			name:               "different from default, autopilot enabled",
			metadata:           Metadata{privateNodeFromLabel: "true"},
			defaultPrivateNode: false,
			isAutopilot:        true,
			expectTaints: []apiv1.Taint{
				{
					Key:    gkelabels.PrivateNodeLabel,
					Value:  "true",
					Effect: apiv1.TaintEffectNoSchedule,
				},
			},
		},
		{
			name:               "different from default, autopilot disabled",
			metadata:           Metadata{privateNodeFromLabel: "true"},
			defaultPrivateNode: false,
			isAutopilot:        false,
			expectTaints:       nil,
		},
		{
			name:               "different from default, true to false, autopilot enabled",
			metadata:           Metadata{privateNodeFromLabel: "false"},
			defaultPrivateNode: true,
			isAutopilot:        true,
			expectTaints: []apiv1.Taint{
				{
					Key:    gkelabels.PrivateNodeLabel,
					Value:  "false",
					Effect: apiv1.TaintEffectNoSchedule,
				},
			},
		},
		{
			name:               "from ccc, autopilot enabled",
			metadata:           Metadata{privateNodeFromCcc: "true"},
			defaultPrivateNode: false,
			isAutopilot:        true,
			expectTaints:       nil,
		},
		{
			name:               "from ccc, autopilot disabled",
			metadata:           Metadata{privateNodeFromCcc: "true"},
			defaultPrivateNode: false,
			isAutopilot:        false,
			expectTaints:       nil,
		},
		{
			name: "from ccc and label, autopilot enabled",
			metadata: Metadata{
				privateNodeFromCcc:   "true",
				privateNodeFromLabel: "true",
			},
			defaultPrivateNode: false,
			isAutopilot:        true,
			expectTaints:       nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cp := &mockCloudProvider{
				defaultPrivateNode: tc.defaultPrivateNode,
				isAutopilot:        tc.isAutopilot,
			}
			f := &privateNode{cp: cp}
			mig := &mockGkeMigSetter{}

			f.UpdateMig(mig, tc.metadata)

			assert.Equal(t, tc.expectTaints, mig.taints)
		})
	}
}
