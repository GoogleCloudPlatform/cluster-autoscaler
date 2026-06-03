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

package multitenancy

import (
	"testing"

	test_util "k8s.io/autoscaler/cluster-autoscaler/utils/test"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
)

func TestIsTenantSystemNamespace(t *testing.T) {
	testCases := []struct {
		name      string
		namespace string
		want      bool
	}{
		{
			name:      "tenant_system_namespace",
			namespace: "t1234-foo-system",
			want:      true,
		},
		{
			name:      "non_tenant_system_namespace",
			namespace: "t1234-foo-prod",
			want:      false,
		},
		{
			name:      "non_tenant_namespace",
			namespace: "kube-system",
			want:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsTenantSystemNamespace(tc.namespace)
			if tc.want != got {
				t.Errorf("Got IsTenantSystemNamespace(%v) = %v, want: %v", tc.namespace, got, tc.want)
			}
		})
	}
}

func TestIsSupervisorPod(t *testing.T) {
	tests := []struct {
		name         string
		pod          *apiv1.Pod
		isSupervisor bool
	}{
		{
			name:         "label is not present",
			pod:          BuildTestPod("supervisor-pod", 10, 10),
			isSupervisor: true,
		},
		{
			name: "label is present and is supervisor",
			pod: BuildTestPod("supervisor-pod", 10, 10, test_util.WithLabels(map[string]string{
				TenantAccessLabel: SupervisorAccessValue,
			})),
			isSupervisor: true,
		},
		{
			name: "label is present and is not supervisor",
			pod: BuildTestPod("user-pod", 10, 10, test_util.WithLabels(map[string]string{
				TenantAccessLabel: TenantAccessValue,
			})),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsSupervisorPod(tc.pod)
			if got != tc.isSupervisor {
				t.Fatalf("IsSupervisorPod(%v) got %v, want %v", tc.pod, got, tc.isSupervisor)
			}

		})
	}
}

func TestIsNonSupervisorNode(t *testing.T) {
	testCases := []struct {
		name string
		node *framework.NodeInfo
		want bool
	}{
		{
			name: "supervisor_node",
			node: buildTestNodeInfoWithLabels("supervisor-node", nil),
			want: false,
		},
		{
			name: "user_node",
			node: buildTestNodeInfoWithLabels("user-node", map[string]string{
				TenantUIDLabel: "uid",
			}),
			want: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsNonSupervisorNode(tc.node)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIsTenantNode(t *testing.T) {
	testCases := []struct {
		name      string
		node      *framework.NodeInfo
		tenantUID string
		want      bool
	}{
		{
			name:      "supervisor_node",
			node:      buildTestNodeInfoWithLabels("supervisor-node", nil),
			want:      false,
			tenantUID: "uid",
		},
		{
			name: "user_node",
			node: buildTestNodeInfoWithLabels("user-node", map[string]string{
				TenantUIDLabel: "uid",
			}),
			want:      true,
			tenantUID: "uid",
		},
		{
			name: "user_node_check_for_different_tenant",
			node: buildTestNodeInfoWithLabels("user-node", map[string]string{
				TenantUIDLabel: "uid",
			}),
			want:      false,
			tenantUID: "other-uid",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := NodeBelongsToTenant(tc.node, tc.tenantUID)
			assert.Equal(t, tc.want, got)
		})
	}
}

func buildTestNodeInfoWithLabels(name string, labels map[string]string) *framework.NodeInfo {
	n := BuildTestNode(name, 1000, 1000)
	n.Name = name
	n.Labels = labels
	nodeInfo := framework.NewTestNodeInfo(n)
	return nodeInfo
}
