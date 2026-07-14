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
	"fmt"
	"regexp"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
)

const (
	// systemNamespaceSuffix is the suffix of the system namespace.
	systemNamespaceSuffix = "system"

	// k8sTenantIDPattern is the pattern for a K8s tenant id.
	k8sTenantIDPattern = "t[0-9]{1,15}-[a-zA-Z0-9]{1,20}"

	TenantUIDLabel        = "tenancy.gke.io/tenant-uid"
	TenantAccessLabel     = "tenancy.gke.io/access-level"
	SupervisorAccessValue = "supervisor"
	TenantAccessValue     = "tenant"
)

var (
	tenantSystemNamespaceRegexp = regexp.MustCompile(
		fmt.Sprintf("^%s-%s$", k8sTenantIDPattern, systemNamespaceSuffix),
	)
)

func IsTenantSystemNamespace(namespace string) bool {
	return tenantSystemNamespaceRegexp.MatchString(namespace)
}

// IsSupervisorPod returns true if pod is owned by the supervisor
func IsSupervisorPod(pod *apiv1.Pod) bool {
	val, ok := pod.Labels[TenantAccessLabel]
	// access level label should always be present for pods
	return !ok || val == SupervisorAccessValue
}

// NodeBelongsToTenant returns true if the node is owned by the given tenant
func NodeBelongsToTenant(node *framework.NodeInfo, tenantUID string) bool {
	tenantUIDLabelValue, ok := node.Node().Labels[TenantUIDLabel]
	return ok && tenantUIDLabelValue == tenantUID
}

// IsNonSupervisorNode returns true if the node is owned by any tenant
func IsNonSupervisorNode(node *framework.NodeInfo) bool {
	_, ok := node.Node().Labels[TenantUIDLabel]
	return ok
}
