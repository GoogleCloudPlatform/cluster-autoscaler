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

package gkeclient

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	container "google.golang.org/api/container/v1beta1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"google.golang.org/api/googleapi"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	caerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/selfservice"
	gkeapi "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	gke_metrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/sandbox"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

const (
	reasonInvalidMachineFamily       = "InvalidMachineFamily"
	MaxThroughputLoggingEnabledLabel = "MAX_THROUGHPUT"
)

var (
	// This makes me so sad
	taintEffectsMap = map[apiv1.TaintEffect]string{
		apiv1.TaintEffectNoSchedule:       "NO_SCHEDULE",
		apiv1.TaintEffectPreferNoSchedule: "PREFER_NO_SCHEDULE",
		apiv1.TaintEffectNoExecute:        "NO_EXECUTE",
	}
	regexClusterSizeLimitReachedError = regexp.MustCompile("Cluster byte size limit reached")
	regexIPSpaceExhaustedError        = regexp.MustCompile("The network \".*\" does not have available private IP space")
)

const (
	// ref: https://cloud.google.com/kubernetes-engine/quotas#limits_per_cluster
	standardMaxNodesPerCluster  = 15000
	autopilotMaxNodesPerCluster = standardMaxNodesPerCluster

	napMinNodes = 0
	// Some of errors from google.golang.org/genproto/googleapis/rpc/code
	permissionDenied  = 7
	unauthenticated   = 16
	resourceExhausted = 8

	defaultMaxSurge = 1

	strategySurge = "SURGE"

	ipRangeNotFoundError = "Cannot find pod range with enough available IPs for nodepool"

	// GkePersistentOperationError signifies that type of error that comes from gke operation is persistent.
	GkePersistentOperationError caerrors.AutoscalerErrorType = "GkePersistentOperationError"
	// GkeTooManyRequestsError the type of error for creating too many operations at given time.
	GkeTooManyRequestsError caerrors.AutoscalerErrorType = "GkeTooManyRequestsError"
)

type autoscalingGkeClientV1beta1 struct {
	apiClient          gkeapi.Client
	nodePoolTranslator NodePoolTranslator

	clusterPath   string
	nodePoolPath  string
	operationPath string

	operationWaitTimeout  time.Duration
	operationPollInterval time.Duration

	machineConfigProvider *machinetypes.MachineConfigProvider

	napMaxNodes int
}

// NodePoolTranslator is auxiliary interface that enables additional translation
// between gke_api_beta.Nodepool and their internal representation.
type NodePoolTranslator interface {
	FromGkeApi(*NodePool, *gke_api_beta.NodePool)
	ToGkeApi(*gke_api_beta.NodePool, *NodePoolSpec)
}

// NewAutoscalingGkeClientV1beta1 creates a new client for communicating with GKE v1beta1 API.
func NewAutoscalingGkeClientV1beta1(apiClient gkeapi.Client, nodePoolTranslator NodePoolTranslator, projectId, location, clusterName string, provider *machinetypes.MachineConfigProvider, napMaxNodes int) (*autoscalingGkeClientV1beta1, error) {
	autoscalingGkeClient := &autoscalingGkeClientV1beta1{
		apiClient:             apiClient,
		nodePoolTranslator:    nodePoolTranslator,
		clusterPath:           fmt.Sprintf(clusterPathPrefix, projectId, location, clusterName),
		nodePoolPath:          fmt.Sprintf(nodePoolPathPrefix, projectId, location, clusterName),
		operationPath:         fmt.Sprintf(operationPathPrefix, projectId, location),
		operationWaitTimeout:  defaultOperationWaitTimeout,
		operationPollInterval: defaultOperationPollInterval,
		machineConfigProvider: provider,
		napMaxNodes:           napMaxNodes,
	}

	return autoscalingGkeClient, nil
}

func (m *autoscalingGkeClientV1beta1) GetCluster() (Cluster, error) {
	start := time.Now()
	clusterResponse, err := m.apiClient.GetCluster(m.clusterPath)
	gke_metrics.EmitGkeLatency("clusters", "get", clusterResponse, err, start)
	if err != nil {
		return Cluster{}, err
	}

	allNodePoolNames := sets.New[string]()
	nodePools := []NodePool{}
	clusterMaxPodsPerNode := maxPodsPerNode(clusterResponse.DefaultMaxPodsConstraint)
	for _, pool := range clusterResponse.NodePools {
		allNodePoolNames.Insert(pool.Name)
		switch pool.Status {
		case "STOPPING":
			klog.V(4).Infof("Filtering out node-pool %s with stopping status", pool.Name)
			continue
		case "PROVISIONING":
			klog.V(4).Infof("Filtering out unready node-pool %s with provisioning status", pool.Name)
			continue
		case "STATUS_UNSPECIFIED":
			// We allow node pools with unspecified status.
			klog.Warningf("Node-pool %s has unspecified status", pool.Name)
		}
		bgi, err := getBlueGreenInfoV1Beta1(pool)
		if err != nil {
			// This probably means that there's an ongoing B/G update, but there's something wrong with how we consume
			// the B/G API. In that case, it seems better to skip scaling this node pool until the update finishes.
			// This means that CA won't be able to scale it beyond the original capacity if more pods
			// appear. An alternative could be treating the node pool as if the update wasn't happening (i.e. ignore the
			// error and set NodePool.BlueGreenInfo to nil), but then we risk CA scaling down blue MIGs.
			klog.Errorf("Failed to parse node pool %q, skipping (it won't be autoscaled until the next refresh): %v", pool.Name, err)
			continue
		}
		arch, err := getArchV1Beta1(pool)
		if err != nil {
			klog.Errorf("Unable to get the system architecture for node pool %q, %v", pool.Name, err)
		}
		sandboxType, err := sandbox.TypeFromString(getSandboxTypeV1Beta1(pool))
		if err != nil {
			klog.Errorf("Unable to get the sandbox type for node pool %q, %v", pool.Name, err)
		}

		tpuType, tpuTopo, tpuMultiHost := m.getTPUSpec(pool)
		var networkConfigs []AdditionalNetworkConfig
		nodePoolMppn := maxPodsPerNode(pool.MaxPodsConstraint)
		if pool.NetworkConfig != nil {
			networkConfigs = toInternalNetworkConfig(pool.NetworkConfig.AdditionalPodNetworkConfigs,
				pool.NetworkConfig.AdditionalNodeNetworkConfigs,
				nodePoolMppn,
				clusterMaxPodsPerNode,
				clusterResponse.Subnetwork,
				clusterResponse.Network)
		}

		podIpv4CidrBlock := getPodIpv4CidrBlock(pool)
		subnet := getNodePoolSubnetwork(pool)
		if subnet == "" {
			subnet = clusterResponse.Subnetwork
		}

		queuedProvisioning := pool.QueuedProvisioning != nil && pool.QueuedProvisioning.Enabled
		autopilotManaged := pool.AutopilotConfig != nil && pool.AutopilotConfig.Enabled

		confidentialType := ""
		if pool.Config.ConfidentialNodes != nil {
			confidentialType = pool.Config.ConfidentialNodes.ConfidentialInstanceType
		}

		if pool.Autoscaling == nil {
			pool.Autoscaling = &gke_api_beta.NodePoolAutoscaling{}
		}
		np := NodePool{
			Name:                     pool.Name,
			InstanceGroupUrls:        pool.InstanceGroupUrls,
			Autoscaled:               pool.Autoscaling.Enabled,
			MinNodeCount:             pool.Autoscaling.MinNodeCount,
			MaxNodeCount:             pool.Autoscaling.MaxNodeCount,
			TotalMinNodeCount:        pool.Autoscaling.TotalMinNodeCount,
			TotalMaxNodeCount:        pool.Autoscaling.TotalMaxNodeCount,
			LocationPolicy:           pool.Autoscaling.LocationPolicy,
			Autoprovisioned:          pool.Autoscaling.Autoprovisioned,
			ThreadsPerCore:           getThreadsPerCoreV1Beta1(pool),
			Version:                  getNodeVersionV1Beta1(pool),
			BlueGreenInfo:            bgi,
			ConfidentialNodesEnabled: IsConfidentialNodesEnabled(pool.Config.ConfidentialNodes),
			Spec: &NodePoolSpec{
				Accelerators: pool.Config.Accelerators,
				LocalSSDConfig: &LocalSSDConfig{
					LocalSsdCount:                  pool.Config.LocalSsdCount,
					EphemeralStorageConfig:         pool.Config.EphemeralStorageConfig,
					EphemeralStorageLocalSsdConfig: pool.Config.EphemeralStorageLocalSsdConfig,
					LocalNvmeSsdBlockConfig:        pool.Config.LocalNvmeSsdBlockConfig,
				},
				Labels:                      pool.Config.Labels,
				Taints:                      convertGkeApiTaintsToApiV1Taints(pool.Config.Taints),
				Locations:                   pool.Locations,
				DiskSize:                    pool.Config.DiskSizeGb,
				DiskType:                    pool.Config.DiskType,
				DiskEncryptionKey:           pool.Config.BootDiskKmsKey,
				MachineType:                 getMachineTypeV1Beta1(pool),
				ImageType:                   pool.Config.ImageType,
				SandboxType:                 sandboxType,
				SystemArchitecture:          arch,
				ExtendedDurationPods:        getExtendedDurationPods(pool),
				MinCpuPlatform:              pool.Config.MinCpuPlatform,
				ReservationAffinity:         pool.Config.ReservationAffinity,
				Spot:                        pool.Config.Spot,
				FlexStart:                   pool.Config.FlexStart,
				MaxRunDurationInSeconds:     getMaxRunDurationInSeconds(pool),
				TpuType:                     tpuType,
				TpuTopology:                 tpuTopo,
				TpuMultiHost:                tpuMultiHost,
				Metadata:                    pool.Config.Metadata,
				MaxPodsPerNode:              nodePoolMppn,
				NetworkConfigs:              networkConfigs,
				PodIpv4CidrBlock:            podIpv4CidrBlock,
				SecondaryBootDisks:          pool.Config.SecondaryBootDisks,
				ServiceAccount:              pool.Config.ServiceAccount,
				LinuxNodeConfig:             linuxNodeConfig(pool.Config.LinuxNodeConfig),
				KubeletConfig:               pool.Config.KubeletConfig,
				SelfServiceMetadata:         selfservice.NodepoolMetadata(pool),
				UpgradeSettings:             pool.UpgradeSettings,
				Subnetwork:                  subnet,
				PlacementGroup:              placementGroup(pool),
				ConfidentialNodeType:        confidentialType,
				ConsolidationDelayInSeconds: getConsolidationDelayInSeconds(pool),
				NodeVersion:                 getNodeVersionV1Beta1(pool),
				ArchTaintBehavior:           getArchTaintBehavior(pool),
			},
			QueuedProvisioning: queuedProvisioning,
			AutopilotManaged:   autopilotManaged,
			Status:             pool.Status,
		}
		if m.nodePoolTranslator != nil {
			m.nodePoolTranslator.FromGkeApi(&np, pool)
		}
		nodePools = append(nodePools, np)
	}

	confidentialNodesEnabled := IsConfidentialNodesEnabled(clusterResponse.ConfidentialNodes)
	nodeLocalDNSEnabled := (clusterResponse.AddonsConfig != nil && clusterResponse.AddonsConfig.DnsCacheConfig != nil && clusterResponse.AddonsConfig.DnsCacheConfig.Enabled) || (clusterResponse.EnableKubernetesAlpha)
	workloadIdentityEnabled := clusterResponse.WorkloadIdentityConfig != nil && clusterResponse.WorkloadIdentityConfig.WorkloadPool != ""
	isClusterUsingPSCInfrastructure := clusterResponse.PrivateClusterConfig != nil && clusterResponse.PrivateClusterConfig.PrivateEndpoint != ""
	isClusterUsingPSCInfrastructure = isClusterUsingPSCInfrastructure || clusterResponse.ControlPlaneEndpointsConfig == nil || clusterResponse.ControlPlaneEndpointsConfig.IpEndpointsConfig == nil || !clusterResponse.ControlPlaneEndpointsConfig.IpEndpointsConfig.Enabled
	enablePrivateNodes := clusterResponse.PrivateClusterConfig != nil && clusterResponse.PrivateClusterConfig.EnablePrivateNodes
	dataplaneV2Enabled := clusterResponse.NetworkConfig != nil && clusterResponse.NetworkConfig.DatapathProvider == "ADVANCED_DATAPATH"
	defaultCCCEnabled := clusterResponse.Autoscaling != nil && clusterResponse.Autoscaling.DefaultComputeClassConfig != nil && clusterResponse.Autoscaling.DefaultComputeClassConfig.Enabled
	var defaultMaxPodsPerNode *MaxPodsConstraint
	if clusterResponse.DefaultMaxPodsConstraint != nil {
		defaultMaxPodsPerNode = &MaxPodsConstraint{
			MaxPodsPerNode: clusterResponse.DefaultMaxPodsConstraint.MaxPodsPerNode,
		}
	}

	createTime, err := time.Parse(time.RFC3339, clusterResponse.CreateTime)
	if err != nil {
		klog.Error("Error Parsing Cluster Create Time: ", clusterResponse.CreateTime)
		createTime = time.Time{}
	}

	releaseChannel := ""
	if clusterResponse.ReleaseChannel != nil {
		releaseChannel = clusterResponse.ReleaseChannel.Channel
	}

	clusterNetworkPath := ""
	clusterSubnetworkPath := ""
	if clusterResponse.NetworkConfig != nil {
		clusterNetworkPath = clusterResponse.NetworkConfig.Network
		clusterSubnetworkPath = clusterResponse.NetworkConfig.Subnetwork
	}

	return Cluster{
		Status:                           clusterResponse.Status,
		ClusterVersion:                   clusterResponse.CurrentMasterVersion,
		EmulatedClusterVersion:           clusterResponse.CurrentEmulatedVersion,
		ReleaseChannel:                   releaseChannel,
		Locations:                        clusterResponse.Locations,
		NodePools:                        nodePools,
		AllNodePoolNames:                 allNodePoolNames,
		ResourceLimiter:                  m.buildResourceLimiter(clusterResponse),
		AutoprovisioningLocations:        getAutoprovisioningLocationsV1Beta1(clusterResponse),
		NodeAutoprovisioningEnabled:      getNodeAutoprovisioningEnabledV1Beta1(clusterResponse),
		AutoprovisioningNodePoolDefaults: getAutoprovisioningNodePoolDefaultsV1Beta1(clusterResponse),
		NodePoolDefaults:                 clusterResponse.NodePoolDefaults,
		DefaultMaxPodsConstraint:         defaultMaxPodsPerNode,
		ConfidentialNodesEnabled:         confidentialNodesEnabled,
		ConfidentialInstanceType:         getConfidentialInstanceType(clusterResponse.ConfidentialNodes),
		NodeLocalDNSEnabled:              nodeLocalDNSEnabled,
		WorkloadIdentityEnabled:          workloadIdentityEnabled,
		HighThroughputLoggingEnabled:     isHighThroughputLoggingEnabled(clusterResponse),
		CreateTime:                       createTime,
		IsClusterUsingPSCInfrastructure:  isClusterUsingPSCInfrastructure,
		EnablePrivateNodes:               enablePrivateNodes,
		DataplaneV2Enabled:               dataplaneV2Enabled,
		DefaultCCCEnabled:                defaultCCCEnabled,
		NetworkPath:                      clusterNetworkPath,
		SubnetworkPath:                   clusterSubnetworkPath,
		Subnetwork:                       clusterResponse.Subnetwork,
	}, nil
}

func placementGroup(pool *container.NodePool) placement.Spec {
	placementPolicy := pool.PlacementPolicy
	placementGroup := placement.Spec{}

	if placementPolicy != nil {
		placementGroup = placement.Spec{
			Policy: placementPolicy.PolicyName,
		}
	}
	return placementGroup
}

func isHighThroughputLoggingEnabled(clusterResponse *container.Cluster) bool {
	if clusterResponse.NodePoolDefaults == nil {
		return false
	}
	if clusterResponse.NodePoolDefaults.NodeConfigDefaults == nil {
		return false
	}
	if clusterResponse.NodePoolDefaults.NodeConfigDefaults.LoggingConfig == nil {
		return false
	}
	if clusterResponse.NodePoolDefaults.NodeConfigDefaults.LoggingConfig.VariantConfig == nil {
		return false
	}
	return clusterResponse.NodePoolDefaults.NodeConfigDefaults.LoggingConfig.VariantConfig.Variant == MaxThroughputLoggingEnabledLabel
}

func convertGkeApiTaintsToApiV1Taints(taints []*gke_api_beta.NodeTaint) []apiv1.Taint {
	apiv1Taints := make([]apiv1.Taint, 0, len(taints))
	for _, t := range taints {
		apiv1Taints = append(apiv1Taints, v1Taint(t))
	}
	return apiv1Taints
}

func v1Taint(nodeTaint *gke_api_beta.NodeTaint) apiv1.Taint {
	if nodeTaint == nil {
		return apiv1.Taint{}
	}
	for v1TaintEffect, gkeNodeTaintEffect := range taintEffectsMap {
		if gkeNodeTaintEffect == nodeTaint.Effect {
			return apiv1.Taint{
				Key:    nodeTaint.Key,
				Value:  nodeTaint.Value,
				Effect: v1TaintEffect,
			}
		}
	}
	effect := snakeCaseToUpperCamelCase(nodeTaint.Effect)
	klog.Errorf("Unknown taint effect %q (will assume conversion to %q for usage in Kubernetes API)", nodeTaint.Effect, effect)
	return apiv1.Taint{
		Key:    nodeTaint.Key,
		Value:  nodeTaint.Value,
		Effect: apiv1.TaintEffect(effect),
	}
}

func snakeCaseToUpperCamelCase(s string) string {
	words := strings.Split(s, "_")
	for i, word := range words {
		if len(word) == 0 {
			continue
		}
		words[i] = strings.ToUpper(word[:1]) + strings.ToLower(word[1:])
	}
	return strings.Join(words, "")
}

func (m *autoscalingGkeClientV1beta1) getTPUSpec(pool *gke_api_beta.NodePool) (tpuType, tpuTopo string, tpuMultiHost bool) {
	mf, err := m.machineConfigProvider.GetMachineFamilyFromMachineName(pool.Config.MachineType)
	if err != nil {
		klog.Errorf("Unable to get machine family for node pool %q: %s", pool.Name, err)
		gke_metrics.EmitUnexpectedNodePool(reasonInvalidMachineFamily)
		return "", tpuTopo, false
	}
	tpuType, err = m.machineConfigProvider.TpuTypeForMachineFamily(mf.Name())
	if err != nil {
		// this machine type doesn't have TPU
		return "", "", false
	}
	if pool.PlacementPolicy != nil && pool.PlacementPolicy.TpuTopology != "" {
		tpuTopo = pool.PlacementPolicy.TpuTopology
	} else {
		var ok bool
		tpuTopo, ok = m.machineConfigProvider.GetSingleHostTopology(pool.Config.MachineType)
		if !ok {
			klog.Errorf("Unable to get single-host TPU topology for machine type %q", pool.Config.MachineType)
			gke_metrics.EmitUnexpectedNodePool(metrics.ReasonMissingSingleHostTopology)
			return tpuType, "", false
		}
	}
	tpuCount, err := m.machineConfigProvider.GetTpuCountForMachineType(pool.Config.MachineType)
	if err != nil {
		klog.Errorf("Unable to get tpu count for node pool %q: %s", pool.Name, err)
		gke_metrics.EmitUnexpectedNodePool(metrics.ReasonTPUAcceleratorCountMissing)
		return tpuType, tpuTopo, false
	}
	tpuMultiHost, err = m.machineConfigProvider.IsMultiHostTpuPodslice(tpuType, tpuTopo, tpuCount)
	if err != nil {
		klog.Errorf("Unable to check if tpu node pool %q is multi-host: %s", pool.Name, err)
		gke_metrics.EmitUnexpectedNodePool(metrics.ReasonInvalidTPUTopology)
		return tpuType, tpuTopo, false
	}
	return tpuType, tpuTopo, tpuMultiHost
}

func maxPodsPerNode(mppn *gke_api_beta.MaxPodsConstraint) int64 {
	if mppn != nil {
		return mppn.MaxPodsPerNode
	}
	return 0
}

func getPodIpv4CidrBlock(pool *gke_api_beta.NodePool) (podIpv4CidrBlock string) {
	if pool.NetworkConfig != nil {
		return pool.NetworkConfig.PodIpv4CidrBlock
	}

	return ""
}

func getNodePoolSubnetwork(pool *gke_api_beta.NodePool) string {
	if pool.NetworkConfig != nil {
		return pool.NetworkConfig.Subnetwork
	}
	return ""
}

func IsConfidentialNodesEnabled(confidentialNodes *gke_api_beta.ConfidentialNodes) bool {
	if confidentialNodes != nil {
		return confidentialNodes.Enabled || labels.ValidConfidentialNodeTypes[confidentialNodes.ConfidentialInstanceType]
	}
	return false
}

func getConfidentialInstanceType(confidentialNodes *gke_api_beta.ConfidentialNodes) string {
	if confidentialNodes != nil {
		return confidentialNodes.ConfidentialInstanceType
	}
	return ""
}

func getAutoprovisioningLocationsV1Beta1(cluster *gke_api_beta.Cluster) []string {
	if cluster == nil || cluster.Autoscaling == nil {
		return nil
	}
	return cluster.Autoscaling.AutoprovisioningLocations
}

func getNodeAutoprovisioningEnabledV1Beta1(cluster *gke_api_beta.Cluster) bool {
	if cluster == nil || cluster.Autoscaling == nil {
		return false
	}
	return cluster.Autoscaling.EnableNodeAutoprovisioning
}

func getAutoprovisioningNodePoolDefaultsV1Beta1(cluster *gke_api_beta.Cluster) *gke_api_beta.AutoprovisioningNodePoolDefaults {
	if cluster == nil || cluster.Autoscaling == nil {
		return nil
	}
	return cluster.Autoscaling.AutoprovisioningNodePoolDefaults
}

func getMachineTypeV1Beta1(pool *gke_api_beta.NodePool) string {
	if pool == nil || pool.Config == nil {
		return ""
	}
	return pool.Config.MachineType
}

func getThreadsPerCoreV1Beta1(pool *gke_api_beta.NodePool) int64 {
	if pool == nil || pool.Config == nil || pool.Config.AdvancedMachineFeatures == nil {
		return int64(0)
	}
	return pool.Config.AdvancedMachineFeatures.ThreadsPerCore
}

func getSandboxTypeV1Beta1(pool *gke_api_beta.NodePool) string {
	if pool == nil || pool.Config == nil || pool.Config.SandboxConfig == nil {
		return ""
	}
	return pool.Config.SandboxConfig.Type
}

func getExtendedDurationPods(pool *gke_api_beta.NodePool) string {
	if pool.Config == nil {
		return ""
	}
	return pool.Config.Labels[labels.ExtendedDurationPodsLabel]
}

func getNodeVersionV1Beta1(pool *gke_api_beta.NodePool) string {
	if pool == nil {
		return ""
	}
	return pool.Version
}

func getArchTaintBehavior(pool *gke_api_beta.NodePool) string {
	if pool == nil || pool.Config == nil || pool.Config.TaintConfig == nil {
		return ""
	}
	return pool.Config.TaintConfig.ArchitectureTaintBehavior
}

func getBlueGreenInfoV1Beta1(pool *gke_api_beta.NodePool) (*BlueGreenInfo, error) {
	if pool == nil || pool.UpdateInfo == nil || pool.UpdateInfo.BlueGreenInfo == nil {
		return nil, nil
	}
	bgi := pool.UpdateInfo.BlueGreenInfo
	phase := UpdatePhase(bgi.Phase)
	if !ValidUpdatePhases[phase] {
		return nil, fmt.Errorf("invalid B/G phase %q", phase)
	}
	autoscaled := false
	if pool.UpgradeSettings != nil && pool.UpgradeSettings.BlueGreenSettings != nil {
		if pool.UpgradeSettings.BlueGreenSettings.AutoscaledRolloutPolicy != nil {
			autoscaled = true
		}
	}

	return &BlueGreenInfo{
		BlueMigUrls:  bgi.BlueInstanceGroupUrls,
		GreenMigUrls: bgi.GreenInstanceGroupUrls,
		Phase:        phase,
		Autoscaled:   autoscaled,
	}, nil
}

func getArchV1Beta1(pool *gke_api_beta.NodePool) (*gce.SystemArchitecture, error) {
	if pool == nil || pool.Config == nil || pool.Config.Labels == nil {
		return nil, nil
	}

	archLabel, found := pool.Config.Labels[apiv1.LabelArchStable]
	if !found {
		return nil, nil
	}
	arch := gce.ToSystemArchitecture(archLabel)
	if arch == gce.UnknownArch {
		return nil, fmt.Errorf("Unknown System Arch %q", archLabel)
	}
	return &arch, nil
}

// removeSecondsSuffixAndFloor removes 's' from the end of the string, floors it and checks whether it's a correct int.
// Used for parsing mrd & consolidation delay values from the API.
func removeSecondsSuffixAndFloor(val string) (string, error) {
	// val contains the "s" unit specified at the end, because the string type returned in mrd & consolidation delay actually represents 'google-duration' type,
	// i.e. google.protobuf.Duration, which in .json representation (which CA indeed uses - container-api.json)
	// uses format with "s" suffix, e.g."3s" or "3.000000001s",
	// see: https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/duration.proto#L94-L100
	// In MaxRunDurationInSeconds and ConsolidationDelayInSeconds we want to store only the integer representing number of seconds, so we have to cut off:
	// the unit suffix and the decimal part (which should be absent)
	valInSeconds := strings.TrimSuffix(val, "s")
	valInSeconds, _, _ = strings.Cut(valInSeconds, ".")

	_, err := strconv.ParseInt(valInSeconds, 10, 0)
	if err != nil {
		return "", err
	}

	return valInSeconds, nil
}

func getMaxRunDurationInSeconds(pool *gke_api_beta.NodePool) string {
	if pool == nil || pool.Config == nil || pool.Config.MaxRunDuration == "" {
		return ""
	}

	maxRunDurationInSeconds, err := removeSecondsSuffixAndFloor(pool.Config.MaxRunDuration)
	if err != nil {
		// This should never happen
		klog.Errorf("Failed to parse node pool's %q MaxRunDurationInSeconds - field will be empty; got unparsable MaxRunDuration %q: %v", pool.Name, pool.Config.MaxRunDuration, err)
		maxRunDurationInSeconds = ""
	}

	return maxRunDurationInSeconds
}

func getConsolidationDelayInSeconds(pool *gke_api_beta.NodePool) string {
	if pool == nil || pool.Config == nil || pool.Config.ConsolidationDelay == "" {
		return ""
	}

	// ConsolidationDelay uses same format as MaxRunDuration
	consolidationDelayInSeconds, err := removeSecondsSuffixAndFloor(pool.Config.ConsolidationDelay)
	if err != nil {
		// This should never happen
		klog.Errorf("Failed to parse node pool's %q ConsolidationDelayInSeconds - field will be empty; got unparsable ConsolidationDelay %q: %v", pool.Name, pool.Config.ConsolidationDelay, err)
		consolidationDelayInSeconds = ""
	}

	return consolidationDelayInSeconds
}

func (m *autoscalingGkeClientV1beta1) buildResourceLimiter(cluster *gke_api_beta.Cluster) *cloudprovider.ResourceLimiter {
	// In Autopilot clusters customers do not control resource limits
	if cluster.Autopilot != nil && cluster.Autopilot.Enabled {
		return m.buildLargestResouceLimiter()
	}

	// If limits are not defined, use the largest limits for consistency
	if cluster == nil || cluster.Autoscaling == nil || len(cluster.Autoscaling.ResourceLimits) == 0 {
		return m.buildLargestResouceLimiter()
	}

	// build min/max maps for resources limits
	minLimits := make(map[string]int64)
	maxLimits := make(map[string]int64)
	for _, limit := range cluster.Autoscaling.ResourceLimits {
		if !m.isSupportedResource(limit.ResourceType) {
			klog.Warningf("Unsupported limit defined %s: %d - %d", limit.ResourceType, limit.Minimum, limit.Maximum)
		}
		minLimits[limit.ResourceType] = limit.Minimum
		maxLimits[limit.ResourceType] = limit.Maximum
	}

	// GKE API provides memory in GB, but ResourceLimiter expects them in bytes
	maxGiB := int64(math.MaxInt64 / units.GiB)
	if limit, found := minLimits[cloudprovider.ResourceNameMemory]; found {
		if limit > maxGiB {
			klog.Warning("Min memory limit overflowed, defaulting to 0")
			minLimits[cloudprovider.ResourceNameMemory] = 0
		} else {
			minLimits[cloudprovider.ResourceNameMemory] = limit * units.GiB
		}
	}
	if limit, found := maxLimits[cloudprovider.ResourceNameMemory]; found {
		if limit > maxGiB {
			klog.Warning("Max memory limit overflowed, defaulting to MaxInt64")
			maxLimits[cloudprovider.ResourceNameMemory] = math.MaxInt64
		} else {
			maxLimits[cloudprovider.ResourceNameMemory] = limit * units.GiB
		}
	}

	return cloudprovider.NewResourceLimiter(minLimits, maxLimits)
}

func (m *autoscalingGkeClientV1beta1) isSupportedResource(resource string) bool {
	if _, found := supportedBasicResources[resource]; found {
		return found
	}
	if _, found := m.machineConfigProvider.ToGpuType(resource); found {
		return found
	}
	return m.machineConfigProvider.IsTpuSupported(resource)
}

func (m *autoscalingGkeClientV1beta1) buildLargestResouceLimiter() *cloudprovider.ResourceLimiter {
	largestCPU := int64(0)
	largestMemory := int64(0)
	for _, family := range m.machineConfigProvider.AllMachineFamilies() {
		machineType := family.LargestAutoprovisionedMachineType(machinetypes.NoConstraints)
		if machineType.CPU > largestCPU {
			largestCPU = machineType.CPU
		}
		if machineType.Memory > largestMemory {
			largestMemory = machineType.Memory
		}
	}

	minLimits := make(map[string]int64)
	maxLimits := make(map[string]int64)

	minLimits[cloudprovider.ResourceNameCores] = 0
	minLimits[cloudprovider.ResourceNameMemory] = 0
	maxLimits[cloudprovider.ResourceNameCores] = autopilotMaxNodesPerCluster * largestCPU
	maxLimits[cloudprovider.ResourceNameMemory] = autopilotMaxNodesPerCluster * largestMemory

	for _, gpuType := range m.machineConfigProvider.GetAllGpuTypes() {
		minLimits[gpuType.Name()] = 0

		// for empty partitionSize and maxSharedClients the allocatable gpu count is equal physical gpu count
		maxGpuCountInt64, _ := gpuType.GetMaxAllocatableGpuCount("", "")
		maxLimits[gpuType.Name()] = autopilotMaxNodesPerCluster * int64(maxGpuCountInt64)
	}

	for _, tpuType := range m.machineConfigProvider.GetAllSupportedTpuTypes() {
		minLimits[tpuType] = 0
		maxTpuCount, _ := m.machineConfigProvider.GetMaxTpuCount(tpuType)
		maxLimits[tpuType] = autopilotMaxNodesPerCluster * maxTpuCount
	}

	return cloudprovider.NewResourceLimiter(minLimits, maxLimits)
}

func (m *autoscalingGkeClientV1beta1) DeleteNodePool(toBeRemoved string) error {
	start := time.Now()
	deleteOp, err := m.apiClient.DeleteNodePool(fmt.Sprintf(m.nodePoolPath, toBeRemoved))
	gke_metrics.EmitGkeLatency("node_pools", "delete", deleteOp, err, start)
	if err != nil {
		return err
	}
	statusErr := m.waitForGkeOp(deleteOp, "delete")
	if statusErr == nil || statusErr.Code == int64(0) {
		return nil
	}
	return errors.New(statusErr.Message)
}

func (m *autoscalingGkeClientV1beta1) UpdateNodePoolLabels(name string, labels map[string]string) error {
	start := time.Now()
	updateRequest := &gke_api_beta.UpdateNodePoolRequest{Labels: &gke_api_beta.NodeLabels{Labels: labels}}
	op, err := m.apiClient.UpdateNodePoolLabels(fmt.Sprintf(m.nodePoolPath, name), updateRequest)
	gke_metrics.EmitGkeLatency("node_pools", "update", op, err, start)
	if err != nil {
		return parseNodePoolUpdateError(err)
	}
	statusErr := m.waitForGkeOp(op, "update")
	if statusErr == nil || statusErr.Code == int64(0) {
		return nil
	}
	return errors.New(statusErr.Message)
}

func (m *autoscalingGkeClientV1beta1) CreateNodePool(name string, spec *NodePoolSpec) error {
	createRequest, err := m.createNodePoolRequest(name, spec)
	if err != nil {
		return err
	}
	start := time.Now()
	createOp, err := m.apiClient.CreateNodePool(m.clusterPath, &createRequest)
	gke_metrics.EmitGkeLatency("node_pools", "create", createOp, err, start)
	if err != nil {
		return parseNodePoolCreationError(err, createRequest.NodePool.PlacementPolicy, spec)
	}
	statusErr := m.waitForGkeOp(createOp, "create")
	return parseOperationError(statusErr)
}

func (m *autoscalingGkeClientV1beta1) createNodePoolRequest(name string, spec *NodePoolSpec) (gke_api_beta.CreateNodePoolRequest, error) {
	if spec == nil {
		return gke_api_beta.CreateNodePoolRequest{}, errors.New("NodePoolSpec is nil")
	}
	config := gke_api_beta.NodeConfig{
		MachineType:         spec.MachineType,
		MinCpuPlatform:      spec.MinCpuPlatform,
		Labels:              spec.Labels,
		ResourceLabels:      spec.ResourceLabels,
		Accelerators:        spec.Accelerators,
		Taints:              v1beta1NodeTaints(spec.Taints),
		DiskType:            spec.DiskType,
		DiskSizeGb:          spec.DiskSize,
		BootDiskKmsKey:      spec.DiskEncryptionKey,
		Preemptible:         spec.Preemptible,
		Spot:                spec.Spot,
		FlexStart:           spec.FlexStart,
		ImageType:           spec.ImageType,
		Metadata:            spec.Metadata,
		SecondaryBootDisks:  spec.SecondaryBootDisks,
		ServiceAccount:      spec.ServiceAccount,
		LinuxNodeConfig:     v1beta1LinuxNodeConfig(spec.LinuxNodeConfig),
		KubeletConfig:       spec.KubeletConfig,
		ReservationAffinity: spec.ReservationAffinity,
	}
	if spec.ArchTaintBehavior != "" {
		config.TaintConfig = &gke_api_beta.TaintConfig{
			ArchitectureTaintBehavior: spec.ArchTaintBehavior,
		}
	}
	if spec.MaxRunDurationInSeconds != "" {
		// MaxRunDuration needs to contain the unit specified at the end, because the string type actually represents 'google-duration' type,
		// i.e. google.protobuf.Duration, which in .json representation (which CA indeed uses - container-api.json)
		// uses format with "s" suffix, e.g."3s" or "3.000000001s",
		// see: http://google3/google/protobuf/duration.proto;rcl=714010407;l=96-103
		config.MaxRunDuration = fmt.Sprintf("%ss", spec.MaxRunDurationInSeconds)
	}
	if spec.ConsolidationDelayInSeconds != "" {
		// ConsolidationDelay needs to contain the unit specified at the end, because the string type actually represents 'google-duration' type,
		// i.e. google.protobuf.Duration, which in .json representation (which CA indeed uses - container-api.json)
		// uses format with "s" suffix, e.g."3s" or "3.000000001s",
		// see: http://google3/google/protobuf/duration.proto;rcl=714010407;l=96-103
		config.ConsolidationDelay = fmt.Sprintf("%ss", spec.ConsolidationDelayInSeconds)
	}

	if spec.SandboxType == sandbox.GVisor {
		config.SandboxConfig = &gke_api_beta.SandboxConfig{
			Type: spec.SandboxType.String(),
		}
	}

	var isMicroVM bool
	if spec.SandboxType == sandbox.MicroVM {
		isMicroVM = true
	} else if stStr, ok := spec.SelfServiceMetadata[labels.SandboxLabelKey]; ok {
		// Fallback to check SelfServiceMetadata if spec.SandboxType is not explicitly set.
		// If users explicitly specify the runtime class on the Pod, then this fallback is not needed
		// as SandboxType will be populated.
		if st, err := sandbox.TypeFromString(stStr); err == nil && st == sandbox.MicroVM {
			isMicroVM = true
		}
	}

	if isMicroVM {
		config.SandboxConfig = &gke_api_beta.SandboxConfig{
			Type: sandbox.MicroVM.String(),
		}
		if config.AdvancedMachineFeatures == nil {
			config.AdvancedMachineFeatures = &gke_api_beta.AdvancedMachineFeatures{}
		}
		// TODO(b/530285556): nested virtualization here is enabled for passing the API validation.
		// We are evaluating whether to relax the validation, and this will be removed once the validation is relaxed.
		config.AdvancedMachineFeatures.EnableNestedVirtualization = true
	}

	if spec.SystemArchitecture != nil && *spec.SystemArchitecture == gce.Arm64 {
		config.Gvnic = &gke_api_beta.VirtualNIC{Enabled: true}
	}

	setLocalSSDConfig(spec, &config)

	placementPolicy := gke_api_beta.PlacementPolicy{
		Type:       spec.PlacementGroup.Type(),
		PolicyName: spec.PlacementGroup.Policy,
	}

	// TODO(b/468235302): investigate whether UpgradeSettings should be set from Defaults for all machines
	if spec.UpgradeSettings == nil && spec.Defaults != nil {
		// use defaults if not specified
		spec.UpgradeSettings = spec.Defaults.UpgradeSettings
	}

	if spec.TpuMultiHost {
		mf, err := m.machineConfigProvider.GetMachineFamilyFromMachineName(spec.MachineType)
		if err == nil && mf.IsAcceleratorSliceSupported() && placementPolicy.PolicyName != "" {
			// BYOWP scenario for multi-host tpu. Policy name is already set.
			placementPolicy.Type = placement.Unspecified
		} else {
			// Set compact placement for multi-host VMs.
			placementPolicy.Type = placement.Compact
		}
		placementPolicy.TpuTopology = spec.TpuTopology
	}

	maxNodes, err := calculateMaxSizeForNewNodePools(m.machineConfigProvider, m.napMaxNodes, spec)
	if err != nil {
		return gke_api_beta.CreateNodePoolRequest{}, caerrors.NewAutoscalerError(caerrors.ConfigurationError, err.Error())
	}
	var queuedProvisioning *gke_api_beta.QueuedProvisioning
	if spec.QueuedProvisioning {
		queuedProvisioning = &gke_api_beta.QueuedProvisioning{Enabled: true}
	}
	autoscaling := gke_api_beta.NodePoolAutoscaling{
		Enabled:         true,
		MinNodeCount:    napMinNodes,
		MaxNodeCount:    maxNodes,
		Autoprovisioned: true,
		LocationPolicy:  spec.LocationPolicy,
	}
	// Autopilot mode config
	var autopilotConfig *gke_api_beta.AutopilotConfig
	if spec.AutopilotManaged {
		autopilotConfig = &gke_api_beta.AutopilotConfig{Enabled: true}
	}
	podNetworkConfig, nodeNetworkConfig := internalNetworkConfigToAdditionalPodAndNodeNetworkConfig(spec.NetworkConfigs, spec.ClusterSubnetwork)
	pool := &gke_api_beta.NodePool{
		Name:              name,
		InitialNodeCount:  0,
		Config:            &config,
		Autoscaling:       &autoscaling,
		Locations:         spec.Locations,
		PlacementPolicy:   &placementPolicy,
		MaxPodsConstraint: maxPodsConstraint(spec),
		NetworkConfig: &gke_api_beta.NodeNetworkConfig{
			AdditionalNodeNetworkConfigs: nodeNetworkConfig,
			AdditionalPodNetworkConfigs:  podNetworkConfig,
		},
		QueuedProvisioning: queuedProvisioning,
		AutopilotConfig:    autopilotConfig,
		UpgradeSettings:    spec.UpgradeSettings,
		Management: &container.NodeManagement{
			// Both fields are set to true by default.

			// It mostly replicates logic from node pool management defaulter:
			// http://google3/cloud/kubernetes/server/patch/field/node/node_pool_management.go;l=39
			// As of right now we do not specify maintenance interval explicitly for node pool
			// which results in it defaulting to MAINTENANCE_INTERVAL_UNSPECIFIED value. This previosly resulted in autorepair
			// being set to true if nothing else was specified.
			AutoRepair: true,
			// Full replication of logic from node pool management defaulter.
			// http://google3/cloud/kubernetes/server/patch/field/node/node_pool_management.go;l=35
			AutoUpgrade: true,
		},
	}

	if spec.NodeVersion != "" {
		pool.Version = spec.NodeVersion
	}

	createRequest := gke_api_beta.CreateNodePoolRequest{
		NodePool: pool,
	}

	m.applyDefaults(&createRequest, spec.Defaults)

	selfservice.UpdateNodepool(pool, spec.SelfServiceMetadata)

	if m.nodePoolTranslator != nil {
		m.nodePoolTranslator.ToGkeApi(pool, spec)
	}

	// Overriding node pool disk encryption key after applying cluster defaults
	// to prevent ignoring per node pool setting
	if spec.DiskEncryptionKey != "" {
		createRequest.NodePool.Config.BootDiskKmsKey = spec.DiskEncryptionKey
	}
	if spec.PodRange != "" {
		createRequest.NodePool.NetworkConfig.PodRange = spec.PodRange
	}
	if spec.Subnetwork != "" {
		createRequest.NodePool.NetworkConfig.Subnetwork = spec.Subnetwork
	}
	if spec.Network != "" {
		createRequest.NodePool.NetworkConfig.Network = spec.Network
	}

	// Autopilot managed node pools should always have the following fields set
	// even if NAP Defaults override them.
	if spec.AutopilotManaged {
		// Force integrity monitoring and secure boot.
		if createRequest.NodePool.Config.ShieldedInstanceConfig == nil {
			createRequest.NodePool.Config.ShieldedInstanceConfig = &gke_api_beta.ShieldedInstanceConfig{}
		}
		createRequest.NodePool.Config.ShieldedInstanceConfig.EnableIntegrityMonitoring = true
		createRequest.NodePool.Config.ShieldedInstanceConfig.EnableSecureBoot = true
		// Force auto-repair and auto-upgrade.
		if createRequest.NodePool.Management == nil {
			createRequest.NodePool.Management = &gke_api_beta.NodeManagement{}
		}
		createRequest.NodePool.Management.AutoRepair = true
		createRequest.NodePool.Management.AutoUpgrade = true
		// Force surge upgrade strategy.
		if createRequest.NodePool.UpgradeSettings == nil {
			createRequest.NodePool.UpgradeSettings = &gke_api_beta.UpgradeSettings{}
		}
		createRequest.NodePool.UpgradeSettings.Strategy = strategySurge
		// Force cos_containerd image type on Autopilot nodes.
		createRequest.NodePool.Config.ImageType = string(gce.OperatingSystemImageCOSContainerd)
	}

	if spec.ConfidentialNodeType != "" {
		createRequest.NodePool.Config.ConfidentialNodes = &gke_api_beta.ConfidentialNodes{ConfidentialInstanceType: spec.ConfidentialNodeType}
	}

	return createRequest, nil
}

func parseNodePoolUpdateError(err error) caerrors.AutoscalerError {
	apierr, ok := err.(*googleapi.Error)
	if !ok {
		return caerrors.NewAutoscalerError(caerrors.CloudProviderError, err.Error())
	}
	msg := strings.TrimSuffix(apierr.Error(), ".")
	if apierr.Code == http.StatusTooManyRequests {
		return caerrors.NewAutoscalerError(GkeTooManyRequestsError, msg)
	}
	if apierr.Code == http.StatusBadRequest && regexClusterSizeLimitReachedError.MatchString(msg) {
		return caerrors.NewAutoscalerError(GkePersistentOperationError, msg)
	}
	if strings.Contains(msg, ipRangeNotFoundError) {
		return caerrors.NewAutoscalerError(GkePersistentOperationError, msg)
	}
	return caerrors.NewAutoscalerError(caerrors.CloudProviderError, msg)
}

func parseNodePoolCreationError(err error, placementPolicy *gke_api_beta.PlacementPolicy, spec *NodePoolSpec) caerrors.AutoscalerError {
	apierr, ok := err.(*googleapi.Error)
	if !ok {
		return caerrors.NewAutoscalerError(caerrors.CloudProviderError, err.Error())
	}

	msg := strings.TrimSuffix(apierr.Error(), ".")

	// put backoff on 5xx errors to avoid calling GKE too much in case of incidents
	if apierr.Code >= http.StatusInternalServerError {
		return caerrors.NewAutoscalerError(GkePersistentOperationError, msg)
	}

	if apierr.Code == http.StatusTooManyRequests {
		return caerrors.NewAutoscalerError(GkeTooManyRequestsError, msg)
	}
	if apierr.Code == http.StatusBadRequest && regexClusterSizeLimitReachedError.MatchString(msg) {
		return caerrors.NewAutoscalerError(GkePersistentOperationError, msg)
	}

	if strings.Contains(msg, ipRangeNotFoundError) {
		return caerrors.NewAutoscalerError(GkePersistentOperationError, msg)
	}

	if placementPolicy.Type == placement.Compact {
		if apierr.Code == http.StatusBadRequest {
			if placementPolicy.TpuTopology != "" {
				return tpu.NewInvalidTpuTopologyError(placementPolicy.TpuTopology, msg)
			} else {
				return placement.NewUnsupportedCompactPlacementConfigError(spec.PlacementGroup.GroupId, msg)
			}
		} else if apierr.Code == http.StatusConflict {
			return placement.NewNodeGroupAlreadyExistsError(spec.PlacementGroup.GroupId)
		}
	}

	return caerrors.NewAutoscalerError(caerrors.CloudProviderError, msg)
}

func parseOperationError(statusErr *container.Status) caerrors.AutoscalerError {
	if statusErr == nil {
		return nil
	}
	if elementInList(statusErr.Code, []int{permissionDenied, unauthenticated, resourceExhausted}) {
		return caerrors.NewAutoscalerError(GkePersistentOperationError, statusErr.Message)
	}
	if regexIPSpaceExhaustedError.MatchString(statusErr.Message) {
		return caerrors.NewAutoscalerError(GkePersistentOperationError, statusErr.Message)
	}
	klog.Errorf("Node pool creation operation failed: %s", statusErr.Message)
	return nil
}

func maxPodsConstraint(spec *NodePoolSpec) *gke_api_beta.MaxPodsConstraint {
	if spec.MaxPodsPerNode != 0 {
		return &gke_api_beta.MaxPodsConstraint{
			MaxPodsPerNode: spec.MaxPodsPerNode,
		}
	}
	return nil
}

func (m *autoscalingGkeClientV1beta1) applyDefaults(request *gke_api_beta.CreateNodePoolRequest, defaults *gke_api_beta.AutoprovisioningNodePoolDefaults) {
	if defaults == nil {
		return
	}
	if defaults.Management != nil {
		request.NodePool.Management = defaults.Management
	}
	request.NodePool.Config.OauthScopes = defaults.OauthScopes
	request.NodePool.Config.ShieldedInstanceConfig = defaults.ShieldedInstanceConfig

	if request.NodePool.Config.BootDiskKmsKey == "" {
		request.NodePool.Config.BootDiskKmsKey = defaults.BootDiskKmsKey
	}
	if request.NodePool.Config.ServiceAccount == "" {
		request.NodePool.Config.ServiceAccount = defaults.ServiceAccount
	}
}

func (m *autoscalingGkeClientV1beta1) waitForGkeOp(originalOp *gke_api_beta.Operation, opType string) *gke_api_beta.Status {
	klog.V(4).Infof("Waiting for operation %s %s %s", originalOp.OperationType, originalOp.TargetLink, originalOp.Name)
	resource := "operations_node_pools_" + opType
	start := time.Now()
	for time.Since(start) < m.operationWaitTimeout {
		requestStart := time.Now()
		freshOp, err := m.apiClient.GetOperation(fmt.Sprintf(m.operationPath, originalOp.Name))
		gke_metrics.EmitGkeLatency(resource, "get", freshOp, err, requestStart)
		if err != nil {
			klog.Warningf("Error while getting operation %s on %s: %v", originalOp.Name, originalOp.TargetLink, err)
		} else {
			klog.V(4).Infof("Operation %s %s status: %s", freshOp.TargetLink, freshOp.Name, freshOp.Status)
			if freshOp.Status == "DONE" {
				gke_metrics.EmitGkeLatency(resource, "get_polling", freshOp, err, start)
				return freshOp.Error
			}
		}

		time.Sleep(m.operationPollInterval)
	}
	gke_metrics.EmitGkeLatency(resource, "get_polling", nil, context.DeadlineExceeded, start)
	return &gke_api_beta.Status{
		Code:    int64(4),
		Message: fmt.Sprintf("timeout while waiting for operation %s on %s to complete.", originalOp.Name, originalOp.TargetLink),
	}
}

func elementInList(code int64, list []int) bool {
	for _, el := range list {
		if int64(el) == code {
			return true
		}
	}
	return false
}

func setLocalSSDConfig(spec *NodePoolSpec, config *gke_api_beta.NodeConfig) {
	if spec.LocalSSDConfig == nil || spec.LocalSSDConfig.EphemeralStorageConfig == nil {
		return
	}
	if count := spec.LocalSSDConfig.EphemeralStorageConfig.LocalSsdCount; count > 0 {
		config.EphemeralStorageConfig = &gke_api_beta.EphemeralStorageConfig{
			LocalSsdCount: count,
		}
		// TODO(b/317509315): if we enable local SSD, we can probably shrink the boot disk.
		// I don't know if that will mess up a fixed size boot image though.
	}
}

// internalNetworkConfigToAdditionalPodAndNodeNetworkConfig maps internal
// representation of AdditionalNetworkConfig to AdditionalPodNetworkConfig and
// AdditionalNodeNetworkConfig structures used by GKE API.
// AdditionalNodeNetworkConfig uses VPCNetName & VPCSubNetName of
// internal structure, while AdditionalPodNetworkConfig uses VPCSubnetName,
// Subrange and MaxPodsPerNode (if they are not empty - if they are it means we
// are handling high performance network).
func internalNetworkConfigToAdditionalPodAndNodeNetworkConfig(networkConfigs []AdditionalNetworkConfig, clusterSubnetwork string) ([]*gke_api_beta.AdditionalPodNetworkConfig, []*gke_api_beta.AdditionalNodeNetworkConfig) {
	var podNetworks []*gke_api_beta.AdditionalPodNetworkConfig
	var nodeNetworks []*gke_api_beta.AdditionalNodeNetworkConfig
	existingSubnetworks := make(map[string]struct{})
	// if some internal network config uses cluster subnetwork,
	// we won't be creating additional node network config for it.
	existingSubnetworks[clusterSubnetwork] = struct{}{}
	for _, nConfig := range networkConfigs {
		if nConfig.NetworkAttachment != "" {
			pNetwork := gkeAPIPodNetworkConfig(nConfig)
			podNetworks = append(podNetworks, pNetwork)
			continue
		}
		if nConfig.SubRange != "" {
			pNetwork := gkeAPIPodNetworkConfig(nConfig)
			podNetworks = append(podNetworks, pNetwork)
		}
		if _, exists := existingSubnetworks[nConfig.VPCSubnetName]; !exists {
			nodeNetworks = append(nodeNetworks, &gke_api_beta.AdditionalNodeNetworkConfig{
				Network:    nConfig.VPCNetName,
				Subnetwork: nConfig.VPCSubnetName,
			})
			existingSubnetworks[nConfig.VPCSubnetName] = struct{}{}
		}
	}
	return podNetworks, nodeNetworks
}

func gkeAPIPodNetworkConfig(nConfig AdditionalNetworkConfig) *gke_api_beta.AdditionalPodNetworkConfig {
	pNetwork := &gke_api_beta.AdditionalPodNetworkConfig{
		Subnetwork:        nConfig.VPCSubnetName,
		SecondaryPodRange: nConfig.SubRange,
		NetworkAttachment: nConfig.NetworkAttachment,
	}
	if nConfig.MaxPodsPerNode != 0 {
		pNetwork.MaxPodsPerNode = &gke_api_beta.MaxPodsConstraint{MaxPodsPerNode: nConfig.MaxPodsPerNode}
	}
	return pNetwork
}

// toInternalNetworkConfig matches together and maps AdditionalPodNetworkConfig
// and AdditionalNodeNetworkConfig to internal AdditionalNetworkConfig structure. AdditionalNodeNetworkConfig
// is matched with AdditionalPodNetworkConfig which uses the same Subnetwork,
// if there is such AdditionalPodNetworkConfig.
// MaxPodsPerNode and Subrange of internal fields are taken from AdditionalPodNetworkConfig, if it is present.
// In case of high performance network matching AdditionalPodNetworkConfig doesn't
// exist for a given AdditionalNodeNetworkConfig and fields mentioned before remain empty.
// In GKE API those fields are separate, to better signal to user what they are meant for,
// however in Autoscaler code it's easier to have them bundled together.
// If the max pods per node of AdditionalPodNetworkConfig is nil, we use node
// pool max pods per node or cluster max pods per node.
func toInternalNetworkConfig(podNetworkConfigs []*gke_api_beta.AdditionalPodNetworkConfig,
	nodeNetworkConfigs []*gke_api_beta.AdditionalNodeNetworkConfig, npMaxPodsPerNode, clusterMaxPodsPerNode int64, clusterSubnetwork, clusterNetwork string,
) []AdditionalNetworkConfig {
	var result []AdditionalNetworkConfig
	podNetworks := make(map[string][]*gke_api_beta.AdditionalPodNetworkConfig)
	for _, podNetwork := range podNetworkConfigs {
		if podNetwork.NetworkAttachment == "" {
			subnetwork := podNetwork.Subnetwork
			podNetworks[subnetwork] = append(podNetworks[subnetwork], podNetwork)
		} else {
			mppn := maxPodsPerNodeForNetworkConfig(npMaxPodsPerNode, clusterMaxPodsPerNode, podNetwork)
			networkAttachmentBasedConfig := AdditionalNetworkConfig{MaxPodsPerNode: mppn, NetworkAttachment: podNetwork.NetworkAttachment}
			result = append(result, networkAttachmentBasedConfig)
		}
	}
	// this is a fake "nodeNetworkConfig", representing the cluster network config.
	// for podNetworkConfig within cluster subnetwork, there won't be associated
	// AdditionalNodeNetworkConfig entry.
	nodeNetworkConfigs = append(nodeNetworkConfigs,
		&gke_api_beta.AdditionalNodeNetworkConfig{Network: clusterNetwork, Subnetwork: clusterSubnetwork})

	for _, nodeNetworkConfig := range nodeNetworkConfigs {
		nodeNetworkBasedConfig := AdditionalNetworkConfig{VPCSubnetName: nodeNetworkConfig.Subnetwork, VPCNetName: nodeNetworkConfig.Network}
		// for high performance networking it is expected to not have associated
		// additional pod network config. In such case MaxPodsPerNode and Subrange fields remain empty.
		// Otherwise, we fill those values based on the AdditionalPodNetworkConfig.
		if podNetworks, found := podNetworks[nodeNetworkConfig.Subnetwork]; found {
			for _, podNetwork := range podNetworks {
				podNetworkBasedConfig := nodeNetworkBasedConfig
				mppn := maxPodsPerNodeForNetworkConfig(npMaxPodsPerNode, clusterMaxPodsPerNode, podNetwork)
				podNetworkBasedConfig.MaxPodsPerNode = mppn
				podNetworkBasedConfig.SubRange = podNetwork.SecondaryPodRange
				result = append(result, podNetworkBasedConfig)
			}
		} else {
			if nodeNetworkBasedConfig.VPCSubnetName == clusterSubnetwork {
				continue
			}
			result = append(result, nodeNetworkBasedConfig)
		}
	}

	return result
}

func maxPodsPerNodeForNetworkConfig(npMaxPodsPerNode int64, clusterMaxPodsPerNode int64, podNetwork *gke_api_beta.AdditionalPodNetworkConfig) int64 {
	mppn := npMaxPodsPerNode
	if mppn == 0 {
		mppn = clusterMaxPodsPerNode
	}
	if podNetwork.MaxPodsPerNode != nil {
		mppn = podNetwork.MaxPodsPerNode.MaxPodsPerNode
	}
	return mppn
}

// calculateMaxSizeForNewNodePools calculates maxSize that should be sent to GKE.
// Takes into account surge options, GCE limits, topology configurations and other
func calculateMaxSizeForNewNodePools(provider *machinetypes.MachineConfigProvider, napMaxNodes int, spec *NodePoolSpec) (int64, error) {
	if spec == nil {
		return 0, fmt.Errorf("invalid spec")
	}
	if spec.TpuMultiHost {
		return provider.NumNodesFromTopology(spec.MachineType, spec.TpuTopology)
	}
	if spec.PlacementGroup.UsesPlacement() {
		maxNodes, err := placement.MaxNodes(provider, spec.MachineType, spec.PlacementGroup.ResourcePolicy)
		if err != nil {
			return 0, err
		}
		machineFamily, err := provider.GetMachineFamilyFromMachineName(spec.MachineType)
		if err != nil {
			return 0, caerrors.NewAutoscalerErrorf(caerrors.InternalError, "unknown machine family for machine type %q: %v", spec.MachineType, err)
		}
		if spec.PlacementGroup.UsesSlice() && machineFamily.IsAcceleratorSliceSupported() {
			return maxNodes, nil
		}
		return maxNodes - maxSurgeFromUpgradeSettings(spec), nil
	}
	if spec.ReservationBlockCount != 0 {
		// TODO(b/468235302, b/432406833): possibly substract surge from block counts
		if spec.ReservationSubBlockCount != 0 {
			// If the sub-block count is set then the node pool max size is set to it
			return spec.ReservationSubBlockCount, nil
		}
		// If the block count is set then the node pool max size is set to it
		return spec.ReservationBlockCount, nil
	}
	return int64(napMaxNodes), nil
}

func maxSurgeFromUpgradeSettings(spec *NodePoolSpec) int64 {
	if spec.UpgradeSettings != nil &&
		(spec.UpgradeSettings.MaxSurge != 0 || spec.UpgradeSettings.MaxUnavailable != 0) {
		return spec.UpgradeSettings.MaxSurge
	}
	return defaultMaxSurge
}
