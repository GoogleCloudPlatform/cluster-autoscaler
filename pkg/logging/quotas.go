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

package logging

import (
	"k8s.io/autoscaler/cluster-autoscaler/utils/klogx"
	klog "k8s.io/klog/v2"
)

const (
	// MaxProvisioningRequestsLogged is the maximum number of ProvisioningRequests for which we will
	// log detailed information every loop at verbosity < 5.
	MaxProvisioningRequestsLogged = 5
	// MaxProvisioningRequestsLoggedV5 is the maximum number of ProvisioningRequests for which we will
	// log detailed information every loop at verbosity >= 5.
	MaxProvisioningRequestsLoggedV5 = 500
	// MaxNodeGroupsLogged is the maximum number of node groups for which we will
	// log detailed information every loop at verbosity < 5.
	MaxNodeGroupsLogged = 10
	// MaxNodeGroupsLoggedV5 is the maximum number of node groups for which we will
	// log detailed information every loop at verbosity >= 5.
	MaxNodeGroupsLoggedV5 = 500
	// Max number of pods logged at once for long reaction.
	MaxOverdueReactionsLogged = 5
	// Max number of pods logged at once for long reaction at verbosity >= 5.
	MaxOverdueReactionsLoggedV5 = 500
	// Max number of pods logged at once for PTS pod assignment.
	MaxPTSPodAssignmentLogged = 20
	// Max number of pods logged at once for PTS pod assignment at verbosity >= 5.
	MaxPTSPodAssignmentLoggedV5 = 500
	// Max number of CSN pods logged at once.
	MaxCSNPodsLogged = 20
	// Max number of CSN pods logged at once at verbosity >= 5.
	MaxCSNPodsLoggedV5 = 500
)

// ProvisioningRequestsLoggingQuota returns a new quota with default limit for ProvisioningRequests at current verbosity.
func ProvisioningRequestsLoggingQuota() *klogx.Quota {
	if klog.V(5).Enabled() {
		return klogx.NewLoggingQuota(MaxProvisioningRequestsLoggedV5)
	}
	return klogx.NewLoggingQuota(MaxProvisioningRequestsLogged)
}

// NodeGroupLoggingQuota returns a new quota with default limit for node groups at current verbosity.
func NodeGroupLoggingQuota() *klogx.Quota {
	if klog.V(5).Enabled() {
		return klogx.NewLoggingQuota(MaxNodeGroupsLoggedV5)
	}
	return klogx.NewLoggingQuota(MaxNodeGroupsLogged)
}

// OverdueReactionsLoggingQuota returns a new quota with default limit for pods at current verbosity.
func OverdueReactionsLoggingQuota() *klogx.Quota {
	if klog.V(5).Enabled() {
		return klogx.NewLoggingQuota(MaxOverdueReactionsLoggedV5)
	}
	return klogx.NewLoggingQuota(MaxOverdueReactionsLogged)
}

// PTSPodAssignmentLoggingQuota returns a new quota with default limit for pods at current verbosity.
func PTSPodAssignmentLoggingQuota() *klogx.Quota {
	if klog.V(5).Enabled() {
		return klogx.NewLoggingQuota(MaxPTSPodAssignmentLoggedV5)
	}
	return klogx.NewLoggingQuota(MaxPTSPodAssignmentLogged)
}

// CSNPodLoggingQuota returns a new quota with default limit for CSN pods at current verbosity.
func CSNPodLoggingQuota() *klogx.Quota {
	if klog.V(5).Enabled() {
		return klogx.NewLoggingQuota(MaxCSNPodsLoggedV5)
	}
	return klogx.NewLoggingQuota(MaxCSNPodsLogged)
}
