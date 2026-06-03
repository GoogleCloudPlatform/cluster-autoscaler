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

// Package networking contains structures and interfaces for interacting with
// networking CRDs and GKE networking related structures.
package networking

import (
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"strconv"

	api "github.com/GoogleCloudPlatform/gke-networking-api/apis/network/v1"
	v1 "github.com/GoogleCloudPlatform/gke-networking-api/client/network/listers/network/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking/util"
	"k8s.io/klog/v2"
)

var (
	ethRegexp = regexp.MustCompile("eth[0-9]+")
	numRegexp = regexp.MustCompile("[0-9]+")
)

// Matcher is an interface used for matching GKE Networking structures with
// kubernetes Networking CRDs.
type Matcher interface {
	GetNetworkingResourcesFromNetworkConfig(networkConfigs []gkeclient.AdditionalNetworkConfig) (map[string]resource.Quantity, error)
	GetNetworkConfigFromResources(resources map[string]resource.Quantity, networkAnnotation string) ([]gkeclient.AdditionalNetworkConfig, error)
}

func GetMatcher(lister v1.GKENetworkParamSetLister) Matcher {
	return &matcherImpl{lister: lister}
}

type matcherImpl struct {
	lister v1.GKENetworkParamSetLister
}

// GetNetworkingResourcesFromNetworkConfig maps given networkConfigs based on
// networking CRDs to kubernetes resources that should be present for a given config.
func (m *matcherImpl) GetNetworkingResourcesFromNetworkConfig(networkConfigs []gkeclient.AdditionalNetworkConfig) (map[string]resource.Quantity, error) {
	result := make(map[string]resource.Quantity)
	paramSetsList, err := m.lister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	paramSetsMap := getParamsSetMap(paramSetsList)
	for _, nConfig := range networkConfigs {
		id := paramsID(nConfig.VPCNetName, nConfig.VPCSubnetName, nConfig.SubRange, nConfig.NetworkAttachment)

		pSet, found := paramSetsMap[id]
		if !found {
			klog.Warningf("Didn't find corresponding GKENetworkParamSet for networking config: %v.", id)
			continue
		}
		if pSet.Status.NetworkName == "" {
			klog.Warningf("GKENetworkParamSet %v has no network name specified.", id)
			continue
		}
		// High performance networks set additional resource
		// networking.gke.io.networks/{netname}
		// in addition to
		// networking.gke.io.networks/{netname}/.IP
		if pSet.Spec.DeviceMode != "" {
			result[fmt.Sprintf(util.MultiNetworkingResourceNamePattern, pSet.Status.NetworkName)] = *util.HighPerformanceDefaultResourceValue
			result[fmt.Sprintf(util.HighPerformanceNetworkResourceNamePattern, pSet.Status.NetworkName)] = *util.HighPerformanceDefaultResourceValue
		} else {
			q := resource.NewQuantity(nConfig.MaxPodsPerNode, resource.DecimalSI)
			result[fmt.Sprintf(util.MultiNetworkingResourceNamePattern, pSet.Status.NetworkName)] = *q
		}
	}
	return result, nil
}

// GetNetworkConfigFromResources creates AdditionalNetworkConfig based on resource requests.
// If resource names match networking resource name pattern this function will
// fetch appropriate GKENetworkParamSet and based on it will construct
// AdditionalNetworkConfig.
func (m *matcherImpl) GetNetworkConfigFromResources(resources map[string]resource.Quantity, networkAnnotation string) ([]gkeclient.AdditionalNetworkConfig, error) {
	paramSetsList, err := m.lister.List(labels.NewSelector())
	if err != nil {
		return nil, err
	}
	networkToParamSet := getNetworkToParamSetMap(paramSetsList)

	// The order of the networks matters.
	// It is dependent on the network annotation and should not be changed.
	multiNetworkInterfaceRefs, err := getSortedInterfaceReferences(resources, networkAnnotation, networkToParamSet)
	if err != nil {
		return nil, err
	}

	var result []gkeclient.AdditionalNetworkConfig
	for _, ref := range multiNetworkInterfaceRefs {
		gnp, exists := networkToParamSet[*ref.Network]
		if !exists {
			return nil, fmt.Errorf("GKENetworkParamSet %s not found", *ref.Network)
		}
		if gnp.Spec.NetworkAttachment != "" {
			var maxPodsPerNode int64
			if gnp.Spec.DeviceMode != "" {
				maxPodsPerNode = 0
			} else {
				maxPodsPerNode = util.DefaultAdditionalMultiNetworkConfigMaxPodsPerNode
			}
			result = append(result, gkeclient.AdditionalNetworkConfig{NetworkAttachment: gnp.Spec.NetworkAttachment, MaxPodsPerNode: maxPodsPerNode})
			continue
		}

		// Check if network is high performance
		if gnp.Spec.DeviceMode != "" {
			result = append(result, gkeclient.AdditionalNetworkConfig{VPCNetName: gnp.Spec.VPC, VPCSubnetName: gnp.Spec.VPCSubnet})
			continue
		}
		if gnp.Spec.PodIPv4Ranges == nil || len(gnp.Spec.PodIPv4Ranges.RangeNames) == 0 {
			return nil, fmt.Errorf("empty IP ranges for network %s", *ref.Network)
		}
		// we choose range name randomly. As of now CA doesn't have a way of
		// determining which range has most IPs allocatable, so the choice is random.
		rangeName := gnp.Spec.PodIPv4Ranges.RangeNames[rand.Int31n(int32(len(gnp.Spec.PodIPv4Ranges.RangeNames)))]
		result = append(result, gkeclient.AdditionalNetworkConfig{VPCSubnetName: gnp.Spec.VPCSubnet, VPCNetName: gnp.Spec.VPC, SubRange: rangeName, MaxPodsPerNode: util.DefaultAdditionalMultiNetworkConfigMaxPodsPerNode})
	}
	return result, nil
}

func getNetworkToParamSetMap(paramSets []*api.GKENetworkParamSet) map[string]*api.GKENetworkParamSet {
	result := make(map[string]*api.GKENetworkParamSet)
	for _, p := range paramSets {
		if p.Status.NetworkName != "" {
			result[p.Status.NetworkName] = p
		}
	}
	return result
}

// getSortedInterfaceReferences returns a list of network interface references
// that are present in the resources map.
//
// The list is sorted by interface name, so that the order of networks is
// deterministic, and it maps correctly to the corresponding interfaces.
func getSortedInterfaceReferences(resources map[string]resource.Quantity, annotation string, networkToParamSet map[string]*api.GKENetworkParamSet) ([]api.InterfaceRef, error) {
	interfaceAnnotation, err := api.ParseInterfaceAnnotation(annotation)
	if err != nil {
		return nil, fmt.Errorf("failed to parse annotation: %v", err)
	}
	if err := validateInterfaceAnnotation(interfaceAnnotation); err != nil {
		return nil, err
	}

	var interfaceRefs []api.InterfaceRef
	for _, ref := range interfaceAnnotation {
		if _, found := resources[fmt.Sprintf(util.MultiNetworkingResourceNamePattern, *ref.Network)]; found {
			interfaceRefs = append(interfaceRefs, ref)
		}
	}

	if err := allGNPPresent(interfaceRefs, networkToParamSet); err != nil {
		return nil, err
	}
	sort.Slice(interfaceRefs, func(i, j int) bool {
		return interfaceOrder(interfaceRefs[i], interfaceRefs[j], networkToParamSet)
	})
	return interfaceRefs, nil
}

func allGNPPresent(multiNetworkInterfaces []api.InterfaceRef, networkToParamSet map[string]*api.GKENetworkParamSet) error {
	for _, ref := range multiNetworkInterfaces {
		if _, exists := networkToParamSet[*ref.Network]; !exists {
			return fmt.Errorf("GKENetworkParamSet %s not found", *ref.Network)
		}
	}
	return nil
}

func getParamsSetMap(paramSets []*api.GKENetworkParamSet) map[string]*api.GKENetworkParamSet {
	result := make(map[string]*api.GKENetworkParamSet)
	for _, p := range paramSets {
		if p.Spec.PodIPv4Ranges == nil || len(p.Spec.PodIPv4Ranges.RangeNames) == 0 {
			result[paramsID(p.Spec.VPC, p.Spec.VPCSubnet, "", p.Spec.NetworkAttachment)] = p
		} else {
			for _, rangeName := range p.Spec.PodIPv4Ranges.RangeNames {
				result[paramsID(p.Spec.VPC, p.Spec.VPCSubnet, rangeName, p.Spec.NetworkAttachment)] = p
			}
		}
	}
	return result
}

func paramsID(network, subnetwork, subRange, networkAttachment string) string {
	if networkAttachment == "" {
		return fmt.Sprintf("%s/%s/%s", network, subnetwork, subRange)
	}

	return networkAttachment
}

func validateInterfaceAnnotation(interfaceAnnotation api.InterfaceAnnotation) error {
	for _, ia := range interfaceAnnotation {
		if ia.InterfaceName == "" {
			return fmt.Errorf("interface annotation %+v has no InterfaceName specified", ia)
		}
		if ia.Network == nil {
			return fmt.Errorf("interface annotation %+v has no Network specified", ia)
		}
	}
	return nil
}

func interfaceOrder(a, b api.InterfaceRef, networkToParamSet map[string]*api.GKENetworkParamSet) bool {
	dma := deviceMode(a, networkToParamSet)
	dmb := deviceMode(b, networkToParamSet)
	// sort by DeviceMode only for RDMA devices present in the annotation
	if (dma != api.RDMA && dmb != api.RDMA) || dma == dmb {
		return interfaceNameOrder(a.InterfaceName, b.InterfaceName)
	}
	return dma < dmb
}

func deviceMode(netInterface api.InterfaceRef, networkToParamSet map[string]*api.GKENetworkParamSet) api.DeviceModeType {
	return networkToParamSet[*netInterface.Network].Spec.DeviceMode
}

func interfaceNameOrder(a, b string) bool {
	if isEth(a) && isEth(b) {
		return ethIndex(a) < ethIndex(b)
	} else if isEth(a) {
		return true
	} else if isEth(b) {
		return false
	} else {
		return a < b
	}
}

func isEth(s string) bool {
	return ethRegexp.MatchString(s)
}

func ethIndex(s string) int {
	index, _ := strconv.Atoi(numRegexp.FindString(s))
	return index
}
