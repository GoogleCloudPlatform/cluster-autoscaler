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

package flexadvisor

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	"k8s.io/klog/v2"
	"k8s.io/utils/set"
)

// InstanceReference keeps information from cloudprovider.nodegroup that needs to query FlexAdvisor
type InstanceReference struct {
	FlexibilityScopeKey string
	InstanceConfigKey   string
	Zone                string
}

func (i *InstanceReference) String() string {
	return fmt.Sprintf("InstanceReference{FlexibilityScopeKey: %v, InstanceConfigKey: %v, Zone: %v}", i.FlexibilityScopeKey, i.InstanceConfigKey, i.Zone)
}

// ConstructInstanceReference returns InstanceReference for the nodeGroup.
// Returns an error if any field in InstanceReference was not found from nodeGroup.
func ConstructInstanceReference(nodeGroup cloudprovider.NodeGroup, cccLister lister.Lister, experimentsManager experiments.Manager) (*InstanceReference, error) {
	gkeNodeGroup, ok := nodeGroup.(gke.NodeGroup)
	if !ok {
		return nil, fmt.Errorf("unexpected cloudprovider.NodeGroup type, got: %s, want: gke.NodeGroup", reflect.TypeOf(nodeGroup))
	}
	if cccLister == nil {
		return nil, errors.New("lister.Lister is nil")
	}
	_, flexibilityScopeKey, err := cccLister.NodeGroupCrd(gkeNodeGroup.GetMig())
	if flexibilityScopeKey == "" {
		return nil, fmt.Errorf("ccc label/flexibility scope key not found in the nodeGroup. err: %v", err)
	}
	zone := gkeNodeGroup.GceRef().Zone
	if zone == "" {
		return nil, fmt.Errorf("zone not found in nodeGroup: %s", gkeNodeGroup.Id())
	}

	if gkeNodeGroup.Spec() == nil {
		return nil, fmt.Errorf("nodeGroup spec is nil for nodeGroup: %s", gkeNodeGroup.Id())
	}

	// It is not possible to derive rank from cloudprovider.NodeGroup. So -1 is used as a placeholder.
	// This instanceConfig is used only for the purpose of generating instanceConfigKey to read the FA cache.
	// Rank field will not be a part of instanceConfigKey so this will not be a problem to query the cache.
	rank := -1
	instanceConfig, err := buildInstanceConfigFromNodePoolSpec(gkeNodeGroup.MachineType(), gkeNodeGroup.Spec(), rank, []string{zone}, experimentsManager)
	if err != nil {
		return nil, fmt.Errorf("error when building instance config for nodeGroup: %s err: %v", gkeNodeGroup.Id(), err)
	}
	return &InstanceReference{
		Zone:                zone,
		FlexibilityScopeKey: flexibilityScopeKey,
		InstanceConfigKey:   instanceConfig.Signature(),
	}, nil
}

func buildInstanceConfigFromNodePoolSpec(machineType string, spec *gkeclient.NodePoolSpec, rank int, zones []string, experimentsManager experiments.Manager) (*api.InstanceConfig, error) {
	if spec == nil {
		return nil, errors.New("invalid spec, cannot generate instance config")
	}

	instanceConfig := api.NewInstanceConfigWithZones(machineType, "", 0, rank, "", api.EmptyMaxRunDuration, set.New(zones...))

	isTPU := spec.TpuType != "" || spec.TpuTopology != ""
	if isTPU && !isFlexAdvisorTPUEnabled(experimentsManager) {
		return nil, errors.New("tpu node pools are not supported by Flex Advisor")
	}

	if spec.FlexStart {
		if !isFlexAdvisorDWSEnabled(experimentsManager) {
			return nil, errors.New("flex start node pools are not supported by Flex Advisor")
		}
		instanceConfig.SetProvisioningMode(instanceavailability.FlexStart)
	} else if spec.Spot {
		instanceConfig.SetProvisioningMode(instanceavailability.Spot)
	} else {
		instanceConfig.SetProvisioningMode(instanceavailability.Standard)
	}

	// NOTE: Assuming only the first accelerator in the spec is relevant for defining
	// the instance configuration. GKE currently supports one accelerator type per node pool.
	if len(spec.Accelerators) > 0 && spec.TpuType == "" {
		accelerator := spec.Accelerators[0]
		instanceConfig.SetGpuType(accelerator.AcceleratorType)
		instanceConfig.SetGpuCount(int(accelerator.AcceleratorCount))
	}
	if len(spec.Accelerators) > 1 {
		// TODO(b/516429381): log whole spec.Accelerators object
		// TODO(b/516426312): buildInstanceConfigFromNodePoolSpec may be called from main or background workers. Add options to differentiate and add prefix appropriately
		klog.Warningf("FlexAdvisor: At most one accelerator is expected in the mig spec, found len(accelerators)=%v", len(spec.Accelerators))
	}

	if isFlexAdvisorDWSEnabled(experimentsManager) {
		if spec.MaxRunDurationInSeconds != "" {
			// Note: FSQ pools won't have MRD set. FSNQ will. FSQ is not supported by CCC (FSQ pools are filtered out during CCC scale up)
			instanceConfig.SetMaxRunDurationInSeconds(spec.MaxRunDurationInSeconds)
		} else if spec.FlexStart {
			// DWS configs sent to GCE >always< need to have MRD present. If spec doesn't have it we default to 7 days
			// DWS spec can have MRD missing if:
			// - it is a temporary-mig (defaulting to 7 days when sending Create request to GCE happens just before sending the request)
			// - it is a FSQ pool (this should not actually happen because CCC doesn't support FSQs and such scale up will fail anyway)
			instanceConfig.SetMaxRunDurationInSeconds(strconv.Itoa(int(queuedwrapper.DefaultMaxRunDuration.Seconds())))
		}
	}

	if spec.TpuTopology != "" && isFlexAdvisorTPUEnabled(experimentsManager) {
		instanceConfig.SetWorkloadPolicies(api.WorkloadPolicies{
			AcceleratorTopology: spec.TpuTopology,
		})
	}
	return instanceConfig, nil
}
