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

package provider

import (
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

// GkeExpanderCloudProvider is a CloudProvider sub-interface used in expander package.
type GkeExpanderCloudProvider interface {
	cloudprovider.CloudProvider

	GetClusterInfo() (projectId, location, clusterName string)
	IsAutopilotEnabled() bool
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}
