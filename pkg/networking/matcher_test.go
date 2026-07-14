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

package networking

import (
	"errors"
	"fmt"
	"testing"

	api "github.com/GoogleCloudPlatform/gke-networking-api/apis/network/v1"
	v1 "github.com/GoogleCloudPlatform/gke-networking-api/client/network/listers/network/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

func Test_GetNetworkingResourcesFromMigSpec(t *testing.T) {
	quantity10 := resource.NewQuantity(10, resource.DecimalSI)
	quantity1 := resource.NewQuantity(1, resource.DecimalSI)
	quantity2 := resource.NewQuantity(2, resource.DecimalSI)
	for desc, tc := range map[string]struct {
		paramSets         []*api.GKENetworkParamSet
		networkConfigs    []gkeclient.AdditionalNetworkConfig
		expectedResources map[string]resource.Quantity
	}{
		"One network config, correct paramSet exists": {
			paramSets: []*api.GKENetworkParamSet{
				paramSet("red-net", "net", "sub", []string{"range"}, nil),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfig("net", "sub", "range", 10),
			},
			expectedResources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *quantity10,
			},
		},
		"One network config, correct paramSet with multiple ranges exists": {
			paramSets: []*api.GKENetworkParamSet{
				paramSet("red-net", "net", "sub", []string{"range", "range2"}, nil),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfig("net", "sub", "range", 10),
			},
			expectedResources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *quantity10,
			},
		},
		"One network config, correct paramSet doesn't exist (no param set with matching network name)": {
			paramSets: []*api.GKENetworkParamSet{
				paramSet("red-net", "net2", "sub", []string{"range"}, nil),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfig("net", "sub", "range", 10),
			},
			expectedResources: map[string]resource.Quantity{},
		},
		"One network config, correct paramSet doesn't exist (no param set with matching subnetwork name)": {
			paramSets: []*api.GKENetworkParamSet{
				paramSet("red-net", "net", "sub2", []string{"range"}, nil),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfig("net", "sub", "range", 10),
			},
			expectedResources: map[string]resource.Quantity{},
		},
		"One network config, correct paramSet doesn't exist (no param set with matching subrange name)": {
			paramSets: []*api.GKENetworkParamSet{
				paramSet("red-net", "net", "sub2", []string{}, nil),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfig("net", "sub", "range", 10),
			},
			expectedResources: map[string]resource.Quantity{},
		},
		"One network config, high performance network dpdk": {
			paramSets: []*api.GKENetworkParamSet{
				paramSet("dpdk-net", "net", "sub", []string{}, deviceModePtr(api.DPDKVFIO)),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfig("net", "sub", "", 0),
			},
			expectedResources: map[string]resource.Quantity{
				"networking.gke.io.networks/dpdk-net.IP": *quantity1,
				"networking.gke.io.networks/dpdk-net":    *quantity1,
			},
		},
		"One network config, high performance network netdevice": {
			paramSets: []*api.GKENetworkParamSet{
				paramSet("nd-net", "net", "sub", []string{}, deviceModePtr(api.NetDevice)),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfig("net", "sub", "", 0),
			},
			expectedResources: map[string]resource.Quantity{
				"networking.gke.io.networks/nd-net.IP": *quantity1,
				"networking.gke.io.networks/nd-net":    *quantity1,
			},
		},
		"One network config, no network name specified in paramset Status": {
			paramSets: []*api.GKENetworkParamSet{
				paramSet("", "net", "sub", []string{}, nil),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfig("net", "sub", "", 0),
			},
			expectedResources: map[string]resource.Quantity{},
		},
		"Two network configs, correct paramSets exist": {
			paramSets: []*api.GKENetworkParamSet{
				paramSet("red-net", "net", "sub", []string{"range"}, nil),
				paramSet("blue-net", "net2", "sub2", []string{"range2"}, nil),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfig("net", "sub", "range", 10),
				gkeclient.TestAdditionalNetworkConfig("net2", "sub2", "range2", 10),
			},
			expectedResources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP":  *quantity10,
				"networking.gke.io.networks/blue-net.IP": *quantity10,
			},
		},
		"high performance and multi networks together, correct paramSets exist": {
			paramSets: []*api.GKENetworkParamSet{
				paramSet("red-net", "net", "sub", []string{"range"}, nil),
				paramSet("blue-net", "net2", "sub2", []string{}, deviceModePtr(api.NetDevice)),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfig("net", "sub", "range", 10),
				gkeclient.TestAdditionalNetworkConfig("net2", "sub2", "", 1),
			},
			expectedResources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP":  *quantity10,
				"networking.gke.io.networks/blue-net.IP": *quantity1,
				"networking.gke.io.networks/blue-net":    *quantity1,
			},
		},
		"Two network configs, correct paramSet exists for only one": {
			paramSets: []*api.GKENetworkParamSet{
				paramSet("red-net", "net", "sub", []string{"range"}, nil),
				paramSet("blue-net", "net3", "sub2", []string{"range2"}, nil),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfig("net", "sub", "range", 10),
				gkeclient.TestAdditionalNetworkConfig("net2", "sub2", "range2", 10),
			},
			expectedResources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *quantity10,
			},
		},
		"One network config with network attachment, correct paramSet exists": {
			paramSets: []*api.GKENetworkParamSet{
				paramSetWithAttachment("red-net", nil, "attachment1"),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfigWithAttachment(10, "attachment1"),
			},
			expectedResources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *quantity10,
			},
		},
		"Two network configs with and without network attachment, correct paramSets exist": {
			paramSets: []*api.GKENetworkParamSet{
				paramSetWithAttachment("red-net", nil, "attachment1"),
				paramSet("blue-net", "net", "sub", []string{"range"}, nil),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfigWithAttachment(10, "attachment1"),
				gkeclient.TestAdditionalNetworkConfig("net", "sub", "range", 2),
			},
			expectedResources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP":  *quantity10,
				"networking.gke.io.networks/blue-net.IP": *quantity2,
			},
		},
		"Two network configs with network attachments, correct paramSets exist only for one": {
			paramSets: []*api.GKENetworkParamSet{
				paramSetWithAttachment("red-net", nil, "attachment1"),
				paramSetWithAttachment("blue-net", nil, "attachment10"),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfigWithAttachment(10, "attachment1"),
				gkeclient.TestAdditionalNetworkConfigWithAttachment(2, "attachment2"),
			},
			expectedResources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *quantity10,
			},
		},
		"high performance and multi networks with network attachments together, correct paramSets exist": {
			paramSets: []*api.GKENetworkParamSet{
				paramSetWithAttachment("red-net", nil, "attachment1"),
				paramSetWithAttachment("blue-net", deviceModePtr(api.NetDevice), "attachment2"),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfigWithAttachment(10, "attachment1"),
				gkeclient.TestAdditionalNetworkConfigWithAttachment(2, "attachment2"),
			},
			expectedResources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP":  *quantity10,
				"networking.gke.io.networks/blue-net.IP": *quantity1,
				"networking.gke.io.networks/blue-net":    *quantity1,
			},
		},
		"high performance and multi networks with and without network attachments together, correct paramSets exist": {
			paramSets: []*api.GKENetworkParamSet{
				paramSetWithAttachment("red-net", nil, "attachment1"),
				paramSetWithAttachment("blue-net", deviceModePtr(api.NetDevice), "attachment2"),
				paramSet("green-net", "net1", "sub", []string{"range"}, deviceModePtr(api.NetDevice)),
				paramSet("yellow-net", "net2", "sub", []string{"range"}, nil),
			},
			networkConfigs: []gkeclient.AdditionalNetworkConfig{
				gkeclient.TestAdditionalNetworkConfigWithAttachment(10, "attachment1"),
				gkeclient.TestAdditionalNetworkConfigWithAttachment(2, "attachment2"),
				gkeclient.TestAdditionalNetworkConfig("net1", "sub", "range", 2),
				gkeclient.TestAdditionalNetworkConfig("net2", "sub", "range", 2),
			},
			expectedResources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP":    *quantity10,
				"networking.gke.io.networks/blue-net.IP":   *quantity1,
				"networking.gke.io.networks/blue-net":      *quantity1,
				"networking.gke.io.networks/green-net.IP":  *quantity1,
				"networking.gke.io.networks/green-net":     *quantity1,
				"networking.gke.io.networks/yellow-net.IP": *quantity2,
			},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			m := matcherImpl{&mockParamSetLister{paramSets: tc.paramSets}}
			gotResources, err := m.GetNetworkingResourcesFromNetworkConfig(tc.networkConfigs)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.expectedResources, gotResources); diff != "" {
				t.Errorf("Unexpected resources: %v", diff)
			}
		})
	}
}

func Test_GetNetworkConfigFromResources(t *testing.T) {
	for desc, tc := range map[string]struct {
		resources          map[string]resource.Quantity
		annotation         string
		paramSets          []*api.GKENetworkParamSet
		wantNetworkConfigs []gkeclient.AdditionalNetworkConfig
		wantErr            error
	}{
		"empty resources": {
			resources: map[string]resource.Quantity{},
		},
		"no networking resources": {
			resources: map[string]resource.Quantity{
				"cpu": *resource.NewQuantity(1, resource.DecimalSI),
			},
		},
		"multi networking resource l3 network paramset": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
							{"interfaceName":"eth0","network":"red-net"}
						]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						VPC:       "net",
						VPCSubnet: "sub",
						PodIPv4Ranges: &api.SecondaryRanges{
							RangeNames: []string{
								"range",
							},
						},
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "red-net"},
				},
			},
			wantNetworkConfigs: []gkeclient.AdditionalNetworkConfig{
				{
					VPCNetName:     "net",
					VPCSubnetName:  "sub",
					SubRange:       "range",
					MaxPodsPerNode: 4,
				},
			},
		},
		"interface annotation with unused networks": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
							{"interfaceName":"eth2","network":"green-net"},
							{"interfaceName":"eth1","network":"blue-net"},
							{"interfaceName":"eth0","network":"red-net"}
						]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						VPC:       "net",
						VPCSubnet: "sub",
						PodIPv4Ranges: &api.SecondaryRanges{
							RangeNames: []string{
								"range",
							},
						},
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "red-net"},
				},
			},
			wantNetworkConfigs: []gkeclient.AdditionalNetworkConfig{
				{
					VPCNetName:     "net",
					VPCSubnetName:  "sub",
					SubRange:       "range",
					MaxPodsPerNode: 4,
				},
			},
		},
		"multiple multi networking resource l3 network paramset": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/blue-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/red-net.IP":  *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
							{"interfaceName":"eth1","network":"blue-net"},
							{"interfaceName":"eth0","network":"red-net"}
						]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						VPC:       "net",
						VPCSubnet: "sub",
						PodIPv4Ranges: &api.SecondaryRanges{
							RangeNames: []string{
								"range",
							},
						},
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "red-net"},
				},
				{
					Spec: api.GKENetworkParamSetSpec{
						VPC:       "net1",
						VPCSubnet: "sub1",
						PodIPv4Ranges: &api.SecondaryRanges{
							RangeNames: []string{
								"range1",
							},
						},
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "blue-net"},
				},
			},
			wantNetworkConfigs: []gkeclient.AdditionalNetworkConfig{
				{
					VPCNetName:     "net",
					VPCSubnetName:  "sub",
					SubRange:       "range",
					MaxPodsPerNode: 4,
				},
				{
					VPCNetName:     "net1",
					VPCSubnetName:  "sub1",
					SubRange:       "range1",
					MaxPodsPerNode: 4,
				},
			},
		},
		"multi networking resource high performance network paramset": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/netdev-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/netdev-net":    *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
							{"interfaceName":"eth0","network":"netdev-net"}
						]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						VPC:        "net",
						VPCSubnet:  "sub",
						DeviceMode: api.NetDevice,
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "netdev-net"},
				},
			},
			wantNetworkConfigs: []gkeclient.AdditionalNetworkConfig{{
				VPCNetName:    "net",
				VPCSubnetName: "sub",
			}},
		},
		"multiple high performance network": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/dpdk-net.IP":   *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/dpdk-net":      *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/netdev-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/netdev-net":    *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
							{"interfaceName":"eth1","network":"dpdk-net"},
							{"interfaceName":"eth0","network":"netdev-net"}
						]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						VPC:        "net",
						VPCSubnet:  "sub",
						DeviceMode: api.NetDevice,
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "netdev-net"},
				},
				{
					Spec: api.GKENetworkParamSetSpec{
						VPC:        "net2",
						VPCSubnet:  "sub2",
						DeviceMode: api.DPDKVFIO,
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "dpdk-net"},
				},
			},
			wantNetworkConfigs: []gkeclient.AdditionalNetworkConfig{
				{
					VPCNetName:    "net",
					VPCSubnetName: "sub",
				},
				{
					VPCNetName:    "net2",
					VPCSubnetName: "sub2",
				},
			},
		},
		"multi networking resource, network missing": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
							{"interfaceName":"eth0","network":"red-net"}
						]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						VPC:       "net",
						VPCSubnet: "sub",
						PodIPv4Ranges: &api.SecondaryRanges{
							RangeNames: []string{
								"range",
							},
						},
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "red-net2"},
				},
			},
			wantErr: errors.New("GKENetworkParamSet red-net not found"),
		},
		"multi networking resource, no pod ranges": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
							{"interfaceName":"eth0","network":"red-net"}
						]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						VPC:           "net",
						VPCSubnet:     "sub",
						PodIPv4Ranges: &api.SecondaryRanges{},
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "red-net"},
				},
			},
			wantErr: errors.New("empty IP ranges for network red-net"),
		},
		"high performance network, network missing": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/dpdk-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/dpdk-net":    *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
							{"interfaceName":"eth0","network":"dpdk-net"}
						]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						VPC:       "net",
						VPCSubnet: "sub",
						PodIPv4Ranges: &api.SecondaryRanges{
							RangeNames: []string{
								"range",
							},
						},
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "dpdk-net2"},
				},
			},
			wantErr: errors.New("GKENetworkParamSet dpdk-net not found"),
		},
		"interface annotation is misconfigured": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `invalid`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						VPC:       "net",
						VPCSubnet: "sub",
						PodIPv4Ranges: &api.SecondaryRanges{
							RangeNames: []string{
								"range",
							},
						},
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "red-net"},
				},
			},
			wantErr: errors.New("failed to parse annotation: invalid character 'i' looking for beginning of value"),
		},
		"multi networking resource network attachment paramset": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
        					{"interfaceName":"eth0","network":"red-net"}
      					]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						NetworkAttachment: "attachment1",
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "red-net"},
				},
			},
			wantNetworkConfigs: []gkeclient.AdditionalNetworkConfig{
				{
					NetworkAttachment: "attachment1",
					MaxPodsPerNode:    4,
				},
			},
		},
		"multi networking resources with high performance network paramset with network attachements": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/netdev-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/netdev-net":    *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
							{"interfaceName":"eth0","network":"netdev-net"}
						]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						NetworkAttachment: "attachemnt1",
						DeviceMode:        api.NetDevice,
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "netdev-net"},
				},
			},
			wantNetworkConfigs: []gkeclient.AdditionalNetworkConfig{{
				NetworkAttachment: "attachemnt1",
			}},
		},
		"multiple high performance networks with network attachments": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/dpdk-net.IP":   *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/dpdk-net":      *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/netdev-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/netdev-net":    *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
							{"interfaceName":"eth1","network":"dpdk-net"},
							{"interfaceName":"eth0","network":"netdev-net"}
						]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						NetworkAttachment: "attachemnt1",
						DeviceMode:        api.NetDevice,
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "netdev-net"},
				},
				{
					Spec: api.GKENetworkParamSetSpec{
						NetworkAttachment: "attachemnt2",
						DeviceMode:        api.DPDKVFIO,
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "dpdk-net"},
				},
			},
			wantNetworkConfigs: []gkeclient.AdditionalNetworkConfig{
				{
					NetworkAttachment: "attachemnt1",
				},
				{
					NetworkAttachment: "attachemnt2",
				},
			},
		},
		"interface annotation with unused networks with network attachments": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
							{"interfaceName":"eth2","network":"green-net"},
							{"interfaceName":"eth1","network":"blue-net"},
							{"interfaceName":"eth0","network":"red-net"}
						]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						NetworkAttachment: "attachemnt1",
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "red-net"},
				},
			},
			wantNetworkConfigs: []gkeclient.AdditionalNetworkConfig{
				{
					NetworkAttachment: "attachemnt1",
					MaxPodsPerNode:    4,
				},
			},
		},
		"multiple multi networking resources with network attachments": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/blue-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/red-net.IP":  *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
							{"interfaceName":"eth1","network":"blue-net"},
							{"interfaceName":"eth0","network":"red-net"}
						]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						NetworkAttachment: "attachement1",
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "red-net"},
				},
				{
					Spec: api.GKENetworkParamSetSpec{
						NetworkAttachment: "attachement2",
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "blue-net"},
				},
			},
			wantNetworkConfigs: []gkeclient.AdditionalNetworkConfig{
				{
					NetworkAttachment: "attachement1",
					MaxPodsPerNode:    4,
				},
				{
					NetworkAttachment: "attachement2",
					MaxPodsPerNode:    4,
				},
			},
		},
		"multi networking resources with network attachments, network missing": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
							{"interfaceName":"eth0","network":"red-net"}
						]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						NetworkAttachment: "attachement1",
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "red-net2"},
				},
			},
			wantErr: errors.New("GKENetworkParamSet red-net not found"),
		},
		"high performance network with network attachment, network missing": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/dpdk-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/dpdk-net":    *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
							{"interfaceName":"eth0","network":"dpdk-net"}
						]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						NetworkAttachment: "attachment1",
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "dpdk-net2"},
				},
			},
			wantErr: errors.New("GKENetworkParamSet dpdk-net not found"),
		},
		"interface annotation is misconfigured, network with attachment": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/red-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `invalid`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						NetworkAttachment: "attachment1",
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "red-net"},
				},
			},
			wantErr: errors.New("failed to parse annotation: invalid character 'i' looking for beginning of value"),
		},
		"multiple multi networking resources with and without network attachments": {
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/blue-net.IP":   *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/red-net.IP":    *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/green-net.IP":  *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/yellow-net.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			annotation: `[
							{"interfaceName":"eth0","network":"blue-net"},
							{"interfaceName":"eth3","network":"red-net"},
							{"interfaceName":"eth2","network":"green-net"},
							{"interfaceName":"eth1","network":"yellow-net"}
						]`,
			paramSets: []*api.GKENetworkParamSet{
				{
					Spec: api.GKENetworkParamSetSpec{
						NetworkAttachment: "attachement1",
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "red-net"},
				},
				{
					Spec: api.GKENetworkParamSetSpec{
						NetworkAttachment: "attachement2",
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "blue-net"},
				},
				{
					Spec: api.GKENetworkParamSetSpec{
						VPC:       "net",
						VPCSubnet: "sub",
						PodIPv4Ranges: &api.SecondaryRanges{
							RangeNames: []string{
								"range",
							},
						},
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "green-net"},
				},
				{
					Spec: api.GKENetworkParamSetSpec{
						VPC:       "net1",
						VPCSubnet: "sub1",
						PodIPv4Ranges: &api.SecondaryRanges{
							RangeNames: []string{
								"range1",
							},
						},
					},
					Status: api.GKENetworkParamSetStatus{NetworkName: "yellow-net"},
				},
			},
			wantNetworkConfigs: []gkeclient.AdditionalNetworkConfig{
				{
					NetworkAttachment: "attachement2",
					MaxPodsPerNode:    4,
				},
				{
					VPCNetName:     "net1",
					VPCSubnetName:  "sub1",
					SubRange:       "range1",
					MaxPodsPerNode: 4,
				},
				{
					VPCNetName:     "net",
					VPCSubnetName:  "sub",
					SubRange:       "range",
					MaxPodsPerNode: 4,
				},
				{
					NetworkAttachment: "attachement1",
					MaxPodsPerNode:    4,
				},
			},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			m := matcherImpl{&mockParamSetLister{paramSets: tc.paramSets}}
			gotConfigs, err := m.GetNetworkConfigFromResources(tc.resources, tc.annotation)
			if tc.wantErr != nil {
				assert.Equal(t, tc.wantErr, err)
			} else {
				if diff := cmp.Diff(tc.wantNetworkConfigs, gotConfigs); diff != "" {
					t.Errorf("Unexpected resources: %v", diff)
				}
			}
		})
	}
}

func TestInterfaceNameOrder(t *testing.T) {
	testCases := []struct {
		name       string
		interfaceA string
		interfaceB string
		wantLess   bool
	}{
		{
			name:       "two arbitrary interface names, ordered alphabetically",
			interfaceA: "interface-name-A",
			interfaceB: "interface-name-B",
			wantLess:   true,
		},
		{
			name:       "two arbitrary interface names, not ordered alphabetically",
			interfaceA: "interface-name-B",
			interfaceB: "interface-name-A",
			wantLess:   false,
		},
		{
			name:       "two ethX interface names, ordered correctly",
			interfaceA: "eth5",
			interfaceB: "eth123",
			wantLess:   true,
		},
		{
			name:       "two ethX interface names, not ordered correctly",
			interfaceA: "eth123",
			interfaceB: "eth5",
			wantLess:   false,
		},
		{
			name:       "two different interface naming scheme, ethX first",
			interfaceA: "eth5",
			interfaceB: "arbitrary-name",
			wantLess:   true,
		},
		{
			name:       "two different interface naming scheme, ethX second",
			interfaceA: "whatever-name",
			interfaceB: "eth5",
			wantLess:   false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantLess, interfaceNameOrder(tc.interfaceA, tc.interfaceB))
		})
	}
}

func TestGetSortedInterfaceReferences(t *testing.T) {
	testCases := []struct {
		name              string
		resources         map[string]resource.Quantity
		annotation        string
		networkToParamSet map[string]*api.GKENetworkParamSet
		expectedOrder     []api.InterfaceRef
		expectedError     error
	}{
		{
			name: "broken annotation",
			annotation: `[
							{"interfaceName":"eth0","network":"default"},
							{"interfaceName":"eth1",
						]`,
			networkToParamSet: map[string]*api.GKENetworkParamSet{},
			expectedError:     fmt.Errorf("failed to parse annotation: invalid character ']' looking for beginning of object key string"),
		},
		{
			name: "incomplete annotation",
			annotation: `[
							{"interfaceName":"eth0","network":"default"},
							{"interfaceName":"eth1"}
						]`,
			networkToParamSet: map[string]*api.GKENetworkParamSet{},
			expectedError:     fmt.Errorf("interface annotation {InterfaceName:eth1 Network:<nil> Interface:<nil>} has no Network specified"),
		},
		{
			name: "correct annotation",
			annotation: `[
							{"interfaceName":"eth0","network":"default"}
						]`,
			networkToParamSet: map[string]*api.GKENetworkParamSet{
				"default": {Spec: api.GKENetworkParamSetSpec{}},
			},
			expectedOrder: nil,
		},
		{
			name: "default network specified in the annotation missing from the param set",
			annotation: `[
							{"interfaceName":"eth0","network":"default"}
						]`,
			networkToParamSet: map[string]*api.GKENetworkParamSet{},
			expectedError:     fmt.Errorf("network default not found"),
		},
		{
			name: "Net Devices in wrong order",
			annotation: `[
							{"interfaceName":"eth1","network":"non-default"},
							{"interfaceName":"eth0","network":"default"}
						]`,
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/non-default.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			networkToParamSet: map[string]*api.GKENetworkParamSet{
				"default":     {Spec: api.GKENetworkParamSetSpec{}},
				"non-default": {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.NetDevice}},
			},
			expectedOrder: []api.InterfaceRef{
				{InterfaceName: "eth1", Network: stringPtr("non-default"), Interface: nil},
			},
		},
		{
			name: "RDMA interface list with correct order",
			annotation: `[
							{"interfaceName":"eth0","network":"default"},
							{"interfaceName":"eth1","network":"gvnic"},
							{"interfaceName":"eth2","network":"rdma-1"}
						]`,
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/gvnic.IP":  *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/rdma-1.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			networkToParamSet: map[string]*api.GKENetworkParamSet{
				"default": {Spec: api.GKENetworkParamSetSpec{}},
				"gvnic":   {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.NetDevice}},
				"rdma-1":  {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.RDMA}},
			},
			expectedOrder: []api.InterfaceRef{
				{InterfaceName: "eth1", Network: stringPtr("gvnic"), Interface: nil},
				{InterfaceName: "eth2", Network: stringPtr("rdma-1"), Interface: nil},
			},
		},
		{
			name: "RDMA interface list with incorrect order, gvnic name > rdma name",
			annotation: `[
							{"interfaceName":"eth0","network":"default"},
							{"interfaceName":"eth2","network":"rdma-1"},
							{"interfaceName":"eth3","network":"gvnic"}
						]`,
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/gvnic.IP":  *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/rdma-1.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			networkToParamSet: map[string]*api.GKENetworkParamSet{
				"default": {Spec: api.GKENetworkParamSetSpec{}},
				"gvnic":   {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.NetDevice}},
				"rdma-1":  {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.RDMA}},
			},
			expectedOrder: []api.InterfaceRef{
				{InterfaceName: "eth3", Network: stringPtr("gvnic"), Interface: nil},
				{InterfaceName: "eth2", Network: stringPtr("rdma-1"), Interface: nil},
			},
		},
		{
			name: "RDMA interface list with incorrect order, l3 name < default name, l3 not a netdevice",
			annotation: `[
							{"interfaceName":"eth0","network":"default"},
							{"interfaceName":"eth2","network":"rdma-1"},
							{"interfaceName":"abc","network":"l3"}
						]`,
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/l3.IP":     *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/rdma-1.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			networkToParamSet: map[string]*api.GKENetworkParamSet{
				"default": {Spec: api.GKENetworkParamSetSpec{}},
				"l3":      {Spec: api.GKENetworkParamSetSpec{}},
				"rdma-1":  {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.RDMA}},
			},
			expectedOrder: []api.InterfaceRef{
				{InterfaceName: "abc", Network: stringPtr("l3"), Interface: nil},
				{InterfaceName: "eth2", Network: stringPtr("rdma-1"), Interface: nil},
			},
		},
		{
			name: "RDMA interface list with incorrect order, gvnic name < default name",
			annotation: `[
							{"interfaceName":"eth0","network":"default"},
							{"interfaceName":"eth2","network":"rdma-1"},
							{"interfaceName":"abc","network":"gvnic"}
						]`,
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/gvnic.IP":  *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/rdma-1.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			networkToParamSet: map[string]*api.GKENetworkParamSet{
				"default": {Spec: api.GKENetworkParamSetSpec{}},
				"gvnic":   {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.NetDevice}},
				"rdma-1":  {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.RDMA}},
			},
			expectedOrder: []api.InterfaceRef{
				{InterfaceName: "abc", Network: stringPtr("gvnic"), Interface: nil},
				{InterfaceName: "eth2", Network: stringPtr("rdma-1"), Interface: nil},
			},
		},
		{
			name: "RDMA interface list with incorrect order",
			annotation: `[
							{"interfaceName":"eth0","network":"default"},
							{"interfaceName":"eth2","network":"rdma-1"},
							{"interfaceName":"eth3","network":"rdma-2"},
							{"interfaceName":"eth8","network":"rdma-7"},
							{"interfaceName":"eth7","network":"rdma-6"},
							{"interfaceName":"eth6","network":"rdma-5"},
							{"interfaceName":"eth5","network":"rdma-4"},
							{"interfaceName":"eth4","network":"rdma-3"},
							{"interfaceName":"eth1","network":"gvnic"}
						]`,
			resources: map[string]resource.Quantity{
				"networking.gke.io.networks/gvnic.IP":  *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/rdma-1.IP": *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/rdma-2.IP": *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/rdma-3.IP": *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/rdma-4.IP": *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/rdma-5.IP": *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/rdma-6.IP": *resource.NewQuantity(1, resource.DecimalSI),
				"networking.gke.io.networks/rdma-7.IP": *resource.NewQuantity(1, resource.DecimalSI),
			},
			networkToParamSet: map[string]*api.GKENetworkParamSet{
				"default": {Spec: api.GKENetworkParamSetSpec{}},
				"gvnic":   {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.NetDevice}},
				"rdma-1":  {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.RDMA}},
				"rdma-2":  {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.RDMA}},
				"rdma-3":  {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.RDMA}},
				"rdma-4":  {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.RDMA}},
				"rdma-5":  {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.RDMA}},
				"rdma-6":  {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.RDMA}},
				"rdma-7":  {Spec: api.GKENetworkParamSetSpec{DeviceMode: api.RDMA}},
			},
			expectedOrder: []api.InterfaceRef{
				{InterfaceName: "eth1", Network: stringPtr("gvnic"), Interface: nil},
				{InterfaceName: "eth2", Network: stringPtr("rdma-1"), Interface: nil},
				{InterfaceName: "eth3", Network: stringPtr("rdma-2"), Interface: nil},
				{InterfaceName: "eth4", Network: stringPtr("rdma-3"), Interface: nil},
				{InterfaceName: "eth5", Network: stringPtr("rdma-4"), Interface: nil},
				{InterfaceName: "eth6", Network: stringPtr("rdma-5"), Interface: nil},
				{InterfaceName: "eth7", Network: stringPtr("rdma-6"), Interface: nil},
				{InterfaceName: "eth8", Network: stringPtr("rdma-7"), Interface: nil},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := getSortedInterfaceReferences(tc.resources, tc.annotation, tc.networkToParamSet)
			if err != nil {
				assert.Equal(t, tc.expectedError, err)
			} else {
				assert.Equal(t, tc.expectedOrder, result)
			}
		})
	}
}

type mockParamSetLister struct {
	paramSets []*api.GKENetworkParamSet
	v1.GKENetworkParamSetListerExpansion
}

func (m *mockParamSetLister) List(_ labels.Selector) ([]*api.GKENetworkParamSet, error) {
	return m.paramSets, nil
}

func (m *mockParamSetLister) Get(_ string) (*api.GKENetworkParamSet, error) {
	return nil, nil
}

func deviceModePtr(deviceMode api.DeviceModeType) *api.DeviceModeType {
	return &deviceMode
}

func paramSet(networkName, vpc, vpcSubnet string, rangeNames []string, deviceMode *api.DeviceModeType) *api.GKENetworkParamSet {
	var ranges *api.SecondaryRanges
	if len(rangeNames) > 0 {
		ranges = &api.SecondaryRanges{
			RangeNames: rangeNames,
		}
	}

	spec := api.GKENetworkParamSetSpec{
		VPC:           vpc,
		VPCSubnet:     vpcSubnet,
		PodIPv4Ranges: ranges,
	}
	if deviceMode != nil {
		spec.DeviceMode = *deviceMode
	}

	return &api.GKENetworkParamSet{
		Spec:   spec,
		Status: api.GKENetworkParamSetStatus{NetworkName: networkName},
	}
}

func paramSetWithAttachment(networkName string, deviceMode *api.DeviceModeType, networkAttachment string) *api.GKENetworkParamSet {
	spec := api.GKENetworkParamSetSpec{
		NetworkAttachment: networkAttachment,
	}
	if deviceMode != nil {
		spec.DeviceMode = *deviceMode
	}

	return &api.GKENetworkParamSet{
		Spec:   spec,
		Status: api.GKENetworkParamSetStatus{NetworkName: networkName},
	}
}

func stringPtr(s string) *string {
	return &s
}
