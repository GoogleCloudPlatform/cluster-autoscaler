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

package autoprovisioning

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	netapi "github.com/GoogleCloudPlatform/gke-networking-api/apis/network/v1"
	gce_api "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apilabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	caerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	taintutils "k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/selfservice"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/extendeddurationpods"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/util/taints"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/machineselection"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/napcloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/preemption"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/sandbox"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	ccpkg "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	computeclass "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	computeclass_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	ekvms_utils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking"
	networkingutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking/util"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podsharding"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
)

const (
	// labelEphemeralLocalSsdDisksCount is an internal only system label used to pass information how many ssd disks should be attached.
	// TODO(go/ccc-static-local-ssd): Add data cache and NVME block count.
	labelEphemeralLocalSsdDisksCount = "cloud.google.com/gke-ephemeral-storage-local-ssd-count"
	// labelComputeClassRequired is an internal only system label used to pass information if Compute Class is required.
	labelComputeClassRequired = "cloud.google.com/gke-compute-class-required"
	// labelLinuxNodeConfig is an internal only system label used to pass information about linux node config.
	labelLinuxNodeConfig = "cloud.google.com/gke-linux-node-config"
	// labelKubeletConfig is an internal only system label used to pass information about node kubelet config.
	labelKubeletConfig = "cloud.google.com/gke-node-kubelet-config"
	// labelArchitectureTaintBehavior is an internal only system label used to pass information about architecture taint behavior.
	labelArchitectureTaintBehavior = "cloud.google.com/gke-architecture-taint-behavior"

	// https://cloud.google.com/compute/docs/instances/limit-vm-runtime#restrictions
	minMRDInSeconds = 30                 // 30 seconds
	maxMRDInSeconds = 120 * 24 * 60 * 60 // 120 days

	// maxFlexStartMRDInSeconds is the maximum MaxRunDuration permitted to use with Flex Start provisioning model.
	maxFlexStartMRDInSeconds = 7 * 24 * 60 * 60 // 7 days
	// minFlexStartMRDInSeconds is the minimum MaxRunDuration permitted to use with Flex Start provisioning model. See go/gke-flexstart-ug.
	minFlexStartMRDInSeconds = 10 * 60 // 10 minutes

	// LocationPolicyAny represents the ANY location policy, which picks zones
	// that have the highest capacity available.
	LocationPolicyAny = "ANY"

	// minConsolidationDelayInSeconds minimum consolidationDelay label value
	minConsolidationDelayInSeconds = 60
	// maxConsolidationDelayInSeconds maximum consolidationDelay label value
	maxConsolidationDelayInSeconds = 24 * 60 * 60
)

// injectionContext contains all common data and components required for injecting new node groups. It should live only
// for the duration of a AutoprovisioningNodeGroupManager.Process() call, and shouldn't be copied elsewhere (or at all).
type injectionContext struct {
	status                          *ProcessingStatus
	nodeInfos                       map[string]*framework.NodeInfo
	existingNodeGroups              []cloudprovider.NodeGroup
	injectedNodeGroups              []cloudprovider.NodeGroup
	resourceLimiter                 *cloudprovider.ResourceLimiter
	clusterSnapshot                 clustersnapshot.ClusterSnapshot
	daemonSets                      []*appsv1.DaemonSet
	taintConfig                     taintutils.TaintConfig
	zones                           []string
	applyReducedZoneSetOptimisation bool
	injectedNodeGroupSignatures     sets.Set[string]
}

func (c *injectionContext) allNodeGroups() []cloudprovider.NodeGroup {
	return append(c.existingNodeGroups, c.injectedNodeGroups...)
}

// NodeGroupRequirementsGenerator generates multiple nodeGroupRequirements for a given pod requirements
type NodeGroupRequirementsGenerator interface {
	GenerateNodeGroupRequirements(ngReqs []nodeGroupRequirements, podReq *podrequirements.Requirements) ([]nodeGroupRequirements, caerrors.AutoscalerError)
}

// NodeGroupOptionsGenerator is used to generate different nodeGroupOption based on requirements
type NodeGroupOptionsGenerator interface {
	GenerateNodeGroupOptionsForRequirements(options []NodeGroupOptions, requirements nodeGroupRequirements) []NodeGroupOptions
}

// NodePoolSpecGenerator is an interface for Node Pool spec generator.
type NodePoolSpecGenerator interface {
	UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, gpuReq machinetypes.GpuRequest, tpuReq TpuRequest) caerrors.AutoscalerError
	ValidateRequirements(ngReq *nodeGroupRequirements) caerrors.AutoscalerError
	UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, opts NodeGroupOptions) error
	UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, extraResources map[string]resource.Quantity) error
}

// nodeGroupParameters contains all parameters needed to inject a new node group. They can be used to call NewNodeGroup.
// Ideally NewNodeGroup would accept this struct as an argument, but that would require changing the cloud provider interface.
type nodeGroupParameters struct {
	machineType    string
	labels         map[string]string
	systemLabels   map[string]string
	taints         []apiv1.Taint
	extraResources map[string]resource.Quantity
}

// System labels that are not taken into account when calculating node group signature.
// This means that two node groups with different values of these labels can still be considered the same.
var systemLabelsIgnoredForSignature = sets.New(gkelabels.ComputeClassPriorityIdxLabel)

func (p nodeGroupParameters) signature() string {
	systemLabels := make(map[string]string)
	for k, v := range p.systemLabels {
		if systemLabelsIgnoredForSignature.Has(k) {
			continue
		}
		systemLabels[k] = v
	}

	var buffer bytes.Buffer

	buffer.WriteString(p.machineType)
	buffer.WriteString(";")
	buffer.WriteString(utils.LabelsToCanonicalString(p.labels))
	buffer.WriteString(";")
	buffer.WriteString(utils.LabelsToCanonicalString(systemLabels))
	buffer.WriteString(";")
	buffer.WriteString(utils.TaintsToCanonicalString(p.taints))
	buffer.WriteString(";")

	// extraResources map needs to be sorted
	keys := make([]string, 0, len(p.extraResources))
	for k := range p.extraResources {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		q := p.extraResources[k]
		buffer.WriteString(fmt.Sprintf("%s=%s,", k, q.String()))
	}

	return buffer.String()
}

// nodeGroupRequirements contains requirements for injected node groups imposed by particular pods (plus the pods themselves).
type nodeGroupRequirements struct {
	pods                      []*apiv1.Pod
	computeClass              computeclass.CRD
	computeClassRule          rules.Rule
	nodeVersion               string
	gpuRequest                machinetypes.GpuRequest
	tpuRequest                TpuRequest
	machineSpec               machinetypes.MachineSpec
	machineSelectionType      machinetypes.SelectionType
	workloadSeparationTaints  []apiv1.Taint
	workloadSeparationLabels  map[string]string
	systemLabels              map[string]string
	extendedDurationPodCPUReq string
	placementGroup            placement.Spec
	sandboxType               sandbox.Type
	reservation               reservationRequirements
	networkReq                podrequirements.NetworkingRequirements
	networkAnnotation         string
	queuedProvisioningReq     podrequirements.QueuedProvisioningRequirements
	// Slice of Hardware PPVM
	podCapacity                   int
	podIsolationCPUReq            string
	explicitlyRequiresLocalSSD    bool
	bootDiskType                  string
	bootDiskSize                  int
	bootDiskEncryptionKey         string
	bootDiskEncryptionAnnotation  string
	ephemeralStorageLocalSSDCount int
	// Sum of all local SSDs to provision for the node, including ephemeral
	// storage, data cache, NVME block, and swap dedicated local SSDs.
	// Swap dedicated local SSD count is inside linuxNodeConfig.
	// TODO(go/ccc-static-local-ssd): Add data cache and NVME block count.
	totalLSSDCount              int
	maxRunDurationInSeconds     string
	flexStartReq                flexStartRequirements
	secondaryBootDisks          []*gke_api_beta.SecondaryBootDisk
	linuxNodeConfig             *gkeclient.LinuxNodeConfig
	kubeletConfig               *gke_api_beta.NodeKubeletConfig
	maxPodsPerNode              int
	specifiedZones              []string
	confidentialNodeType        string
	selfServiceMetadata         selfservice.Metadata
	consolidationDelayInSeconds string
	usesZoneTypes               bool
}

// RequiresComputeClass checks if node group requirements contain Compute Class
func (r *nodeGroupRequirements) RequiresComputeClass() bool {
	if r.computeClass == nil {
		return false
	}

	val := reflect.ValueOf(r.computeClass)
	if val.Kind() == reflect.Pointer && val.IsNil() {
		return false
	}

	return true
}

// reservationRequirements contains GCE reservation requirements for injected
// node groups imposed by particular pods.
type reservationRequirements struct {
	exists         bool
	name           string
	project        string
	block          string // block for injected node groups imposed by particular pods.
	subBlock       string // subBlock for injected node groups imposed by particular pods.
	affinity       string
	zone           string
	machineType    string
	totalLSSDCount int
	blockCount     int64
	subBlockCount  int64
}

func (r reservationRequirements) Signature() string {
	return fmt.Sprintf("name: %q, project: %q, affinity: %q, exists: %v, zone: %q, machineType: %q, block: %q, subBlock: %q, localSSDCount: %v, blockCount: %v",
		r.name, r.project, r.affinity, r.exists, r.zone, r.machineType, r.block, r.subBlock, r.totalLSSDCount, r.blockCount)
}

// FlexStartRequirements contains information about Flex Start requirements.
type flexStartRequirements struct {
	enabled bool
	tainted bool
}

func (r flexStartRequirements) signature(ccRule rules.Rule) string {
	recycling := "nil"
	if ccRule != nil && ccRule.FlexStartNodeRecyclingLeadTimeSeconds() != nil {
		recycling = fmt.Sprintf("%d seconds", ccRule.FlexStartNodeRecyclingLeadTimeSeconds())
	}
	return fmt.Sprintf("flex start enabled: %v, tainted: %v, nodeRecycling: %v", r.enabled, r.tainted, recycling)
}

// NodeGroupOptions denotes that specification used to generate the node group parameters used by NAP
type NodeGroupOptions struct {
	Zone                      string
	MachineType               string
	Preemption                preemption.VmPreemptionType
	ExtendedDurationPodCPUReq string
	PodIsolationCPUReq        string
	PodCapacity               int
	MaxPodsPerNode            int
	DynamicMaxPodsPerNode     bool
}

func (n NodeGroupOptions) String() string {
	return fmt.Sprintf("<zone: %q, machine_type: %q, preemption: %q, extended-duration-pod: %q>, pod-capacity: %v, pod-isolation-cpu: %q, max-pods-per-node: %v, dynamic-max-pods-per-node: %v>", n.Zone, n.MachineType, n.Preemption.ShortName(), n.ExtendedDurationPodCPUReq, n.PodCapacity, n.PodIsolationCPUReq, n.MaxPodsPerNode, n.DynamicMaxPodsPerNode)
}

// String returns a human-readable description of the requirements.
func (r nodeGroupRequirements) String() string {
	examplePodDesc := ""
	if len(r.pods) > 0 {
		examplePod := r.pods[0]
		examplePodDesc = fmt.Sprintf("%s/%s", examplePod.Namespace, examplePod.Name)
	}
	ccName := ""
	if r.RequiresComputeClass() {
		ccName = r.computeClass.Name()
	}

	// Uses a similar approach as nodeGroupRequirements.Signature() to prevent having an extremely long fmt.Sprintf() call.
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("example pod: %q, ", examplePodDesc))
	buffer.WriteString(fmt.Sprintf("pods count: %d, ", len(r.pods)))
	buffer.WriteString(fmt.Sprintf("GPU request: <%s>, ", r.gpuRequest.String()))
	buffer.WriteString(fmt.Sprintf("TPU request: <%s>, ", r.tpuRequest.String()))
	buffer.WriteString(fmt.Sprintf("machine spec: <%s>, ", r.machineSpec.String()))
	buffer.WriteString(fmt.Sprintf("flexStart: <%s>, ", r.flexStartReq.signature(r.computeClassRule)))
	buffer.WriteString(fmt.Sprintf("queuedProvisioning: <%s>, ", r.queuedProvisioningReq.String()))
	buffer.WriteString(fmt.Sprintf("workload separation: <%v>, ", r.workloadSeparationLabels))
	buffer.WriteString(fmt.Sprintf("ExtendedDurationPodCPUReq: <%s>, ", r.extendedDurationPodCPUReq))
	buffer.WriteString(fmt.Sprintf("ccName: <%s>, ", ccName))
	buffer.WriteString(fmt.Sprintf("nodeVersion: <%s>, ", r.nodeVersion))
	buffer.WriteString(fmt.Sprintf("pod-isolation-cpu: <%s>, ", r.podIsolationCPUReq))
	buffer.WriteString(fmt.Sprintf("pod-capacity: <%v>, ", r.podCapacity))
	buffer.WriteString(fmt.Sprintf("requires-localssd: <%t>, ", r.explicitlyRequiresLocalSSD))
	buffer.WriteString(fmt.Sprintf("total lssd count: <%d>, ", r.totalLSSDCount))
	buffer.WriteString(fmt.Sprintf("network-req: <%s>, ", strings.Join(r.networkReq.AdditionalNetworkResources, ",")))
	buffer.WriteString(fmt.Sprintf("network-interface-annotation: <%s>, ", r.networkAnnotation))
	buffer.WriteString(fmt.Sprintf("system labels: <%v>, ", r.systemLabels))
	buffer.WriteString(fmt.Sprintf("boot disk type: <%s>, ", r.bootDiskType))
	buffer.WriteString(fmt.Sprintf("boot disk size: <%v>, ", r.bootDiskSize))
	buffer.WriteString(fmt.Sprintf("boot disk encryption key: <%v>, ", r.bootDiskEncryptionKey))
	buffer.WriteString(fmt.Sprintf("boot disk encryption annotation: <%v>, ", r.bootDiskEncryptionAnnotation))
	buffer.WriteString(fmt.Sprintf("reservation request: <%s>, ", r.reservation.Signature()))
	buffer.WriteString(fmt.Sprintf("secondary boot disks: <%s>, ", r.secondaryBootDisksSignature()))
	buffer.WriteString(fmt.Sprintf("node system config: <%s>, ", linuxNodeConfigSignature(r.linuxNodeConfig)))
	buffer.WriteString(fmt.Sprintf("kubelet config: <%s>, ", kubeletConfigSignature(r.kubeletConfig)))
	buffer.WriteString(fmt.Sprintf("max pods per node: <%v>, ", r.maxPodsPerNode))
	buffer.WriteString(fmt.Sprintf("maxRunDurationInSeconds: <%v>, ", r.maxRunDurationInSeconds))
	buffer.WriteString(fmt.Sprintf("specified zones: <%v>, ", r.specifiedZones))
	buffer.WriteString(fmt.Sprintf("confidential nodes instance type: <%v>, ", r.confidentialNodeType))
	buffer.WriteString(fmt.Sprintf("capacityCheckWaitTimeSeconds: <%s>", capacityCheckWaitTimeSecondsSignature(r.computeClassRule)))
	buffer.WriteString(fmt.Sprintf("selfServiceMetadata: <%v>", r.selfServiceMetadata))
	buffer.WriteString(fmt.Sprintf("consolidationDelayInSeconds: <%v>", r.consolidationDelayInSeconds))

	return buffer.String()
}

// hasPods returns true if the requirements contain any pods. Only requirements containing pods are valid and can be
// used further (e.g. to call getNodeGroupParameters).
func (r nodeGroupRequirements) hasPods() bool {
	return len(r.pods) > 0
}

func (r nodeGroupRequirements) secondaryBootDisksSignature() string {
	var diskImages []string
	for _, disk := range r.secondaryBootDisks {
		diskImages = append(diskImages, disk.DiskImage)
	}
	return strings.Join(diskImages, "; ")
}

// signature returns a stable string representation of all fields of nodeGroupRequirements apart from the pods.
// It can be used as a key to group pods by their node group requirements.
// TODO(b/267142470): Move sinature to generator level.
func (r nodeGroupRequirements) signature() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%s\n", r.tpuRequest.Signature()))
	buffer.WriteString(fmt.Sprintf("%s\n", r.gpuRequest.Signature()))
	buffer.WriteString(fmt.Sprintf("%v\n", r.machineSpec.Signature()))
	buffer.WriteString(fmt.Sprintf("%v\n", r.flexStartReq.signature(r.computeClassRule)))
	buffer.WriteString(fmt.Sprintf("%v\n", r.queuedProvisioningReq.Signature()))
	buffer.WriteString(fmt.Sprintf("%s\n", utils.TaintsToCanonicalString(r.workloadSeparationTaints)))
	buffer.WriteString(fmt.Sprintf("%s\n", utils.LabelsToCanonicalString(r.workloadSeparationLabels)))
	buffer.WriteString(fmt.Sprintf("%s\n", r.extendedDurationPodCPUReq))
	buffer.WriteString(fmt.Sprintf("%s\n", r.podIsolationCPUReq))
	buffer.WriteString(fmt.Sprintf("%v\n", r.podCapacity))
	buffer.WriteString(fmt.Sprintf("%q\n", r.placementGroup))
	buffer.WriteString(fmt.Sprintf("%q\n", r.sandboxType))
	buffer.WriteString(fmt.Sprintf("%q\n", r.reservation.Signature()))
	buffer.WriteString(fmt.Sprintf("%t\n", r.explicitlyRequiresLocalSSD))
	buffer.WriteString(fmt.Sprintf("%d\n", r.totalLSSDCount))
	buffer.WriteString(fmt.Sprintf("%s\n", strings.Join(r.networkReq.AdditionalNetworkResources, ",")))
	buffer.WriteString(fmt.Sprintf("%s\n", r.networkAnnotation))
	buffer.WriteString(fmt.Sprintf("%s\n", utils.LabelsToCanonicalString(r.systemLabels)))
	buffer.WriteString(fmt.Sprintf("%s\n", r.bootDiskType))
	buffer.WriteString(fmt.Sprintf("%v\n", r.bootDiskSize))
	buffer.WriteString(fmt.Sprintf("%v\n", r.bootDiskEncryptionKey))
	buffer.WriteString(fmt.Sprintf("%v\n", r.bootDiskEncryptionAnnotation))
	buffer.WriteString(fmt.Sprintf("%v\n", r.secondaryBootDisksSignature()))
	buffer.WriteString(fmt.Sprintf("%v\n", linuxNodeConfigSignature(r.linuxNodeConfig)))
	buffer.WriteString(fmt.Sprintf("%v\n", kubeletConfigSignature(r.kubeletConfig)))
	buffer.WriteString(fmt.Sprintf("%v\n", r.maxPodsPerNode))
	buffer.WriteString(fmt.Sprintf("%s\n", r.maxRunDurationInSeconds))
	buffer.WriteString(fmt.Sprintf("%v\n", r.specifiedZones))
	buffer.WriteString(fmt.Sprintf("%v\n", r.confidentialNodeType))
	buffer.WriteString(fmt.Sprintf("%v\n", capacityCheckWaitTimeSecondsSignature(r.computeClassRule)))
	buffer.WriteString(fmt.Sprintf("%v\n", r.selfServiceMetadata))
	buffer.WriteString(fmt.Sprintf("%v\n", r.consolidationDelayInSeconds))
	buffer.WriteString(fmt.Sprintf("%s\n", r.nodeVersion))

	ccName := ""
	if r.RequiresComputeClass() {
		ccName = r.computeClass.Name()
	}
	buffer.WriteString(fmt.Sprintf("%s\n", ccName))
	return buffer.String()
}

// KubeletConfigGenerator is a kublet config spec generator.
type KubeletConfigGenerator struct {
}

func NewKubeletConfigGenerator() *KubeletConfigGenerator {
	return &KubeletConfigGenerator{}
}

func (k *KubeletConfigGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, _ *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	return nil
}

func (k *KubeletConfigGenerator) ValidateRequirements(ngReq *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (k *KubeletConfigGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if ngReq.kubeletConfig != nil {
		serializedKubeletConfig, err := serializeKubeletConfig(ngReq.kubeletConfig)
		if err != nil {
			return err
		}
		params.systemLabels[labelKubeletConfig] = serializedKubeletConfig
	}
	return nil
}

func (k *KubeletConfigGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, extraResources map[string]resource.Quantity) error {
	if serializedKubeletConfig, exists := systemLabels[labelKubeletConfig]; exists {
		deserializedKubeletConfig, err := deserializeKubeletConfig(serializedKubeletConfig)
		if err != nil {
			return err
		}
		spec.KubeletConfig = deserializedKubeletConfig
	}
	return nil
}

// LinuxNodeConfigGenerator is a Linux node config spec generator.
type LinuxNodeConfigGenerator struct {
}

func NewLinuxNodeConfigGenerator() *LinuxNodeConfigGenerator {
	return &LinuxNodeConfigGenerator{}
}

func (l *LinuxNodeConfigGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, _ *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	return nil
}

func (l *LinuxNodeConfigGenerator) ValidateRequirements(ngReq *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (l *LinuxNodeConfigGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if ngReq.linuxNodeConfig != nil {
		serializedLinuxNodeConfig, err := serializeLinuxNodeConfig(ngReq.linuxNodeConfig)
		if err != nil {
			return err
		}
		params.systemLabels[labelLinuxNodeConfig] = serializedLinuxNodeConfig
	}
	return nil
}

func (l *LinuxNodeConfigGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, extraResources map[string]resource.Quantity) error {
	if serializedLinuxNodeConfig, exists := systemLabels[labelLinuxNodeConfig]; exists {
		deserializedLinuxNodeConfig, err := deserializeLinuxNodeConfig(serializedLinuxNodeConfig)
		if err != nil {
			return err
		}
		spec.LinuxNodeConfig = deserializedLinuxNodeConfig
	}
	return nil
}

// MaxPodsPerNodeGenerator is a node spec generator for Max pods per node.
type MaxPodsPerNodeGenerator struct {
	cloudProvider napcloudprovider.AutoprovisioningCloudProvider
	podLister     kubernetes.PodLister
}

func NewMaxPodsPerNodeGenerator(cloudProvider napcloudprovider.AutoprovisioningCloudProvider, podLister kubernetes.PodLister) *MaxPodsPerNodeGenerator {
	return &MaxPodsPerNodeGenerator{
		cloudProvider: cloudProvider,
		podLister:     podLister,
	}
}

func (m *MaxPodsPerNodeGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, _ *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	return nil
}

func (m *MaxPodsPerNodeGenerator) ValidateRequirements(ngReq *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (m *MaxPodsPerNodeGenerator) GenerateNodeGroupOptionsForRequirements(options []NodeGroupOptions, ngReq nodeGroupRequirements) []NodeGroupOptions {
	var result []NodeGroupOptions

	for _, option := range options {
		var maxPodsPerNodeOptions []int
		dynamicMaxPodsPerNodeEnabled := false
		if ngReq.maxPodsPerNode > 0 {
			maxPodsPerNodeOptions = []int{ngReq.maxPodsPerNode}

		} else {
			maxPodsPerNodeOptions, dynamicMaxPodsPerNodeEnabled = m.getMppnOptionsForNodeGroupRequirements(ngReq, option.MachineType)
		}
		for _, mppnOption := range maxPodsPerNodeOptions {
			option.MaxPodsPerNode = mppnOption
			option.DynamicMaxPodsPerNode = dynamicMaxPodsPerNodeEnabled
			result = append(result, option)
		}
	}
	return result
}

func (m *MaxPodsPerNodeGenerator) UpdateParameters(params *nodeGroupParameters, _ nodeGroupRequirements, option NodeGroupOptions) error {
	if option.MaxPodsPerNode > 0 {
		params.systemLabels[gkelabels.MaxPodsPerNodeLabel] = strconv.Itoa(option.MaxPodsPerNode)
	}
	if option.DynamicMaxPodsPerNode {
		params.systemLabels[gkelabels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey] = "true"
	}
	return nil
}

// UpdateNodePoolSpec sets either the default value for max pods per node or
// sets the configured value specified for it
func (m *MaxPodsPerNodeGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, extraResources map[string]resource.Quantity) error {
	configuredMppnFromLabels, err := configuredMaxPodsPerNodeFromLabels(systemLabels)
	if err != nil {
		return err
	}
	spec.MaxPodsPerNode = int64(configuredMppnFromLabels)
	if spec.Labels == nil {
		spec.Labels = make(map[string]string)
	}
	// Set label that dynamic max pods per node is enabled
	if val := systemLabels[gkelabels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey]; val == "true" {
		// Set node label
		spec.Labels[gkelabels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey] = val
	}
	return nil
}

// getMppnOptionsForNodeGroupRequirements returns max pods per node options for a new node
// group, based on a cluster size, node group requirements and machineType. It's set to 0 by default for not managed node groups for
// standard clusters, if dynamic max pods per node is enabled (i.e. cluster is Autopilot or node group is managed)
// it will be either 128 and/or 256 if cluster is large enough.
func (m *MaxPodsPerNodeGenerator) getMppnOptionsForNodeGroupRequirements(ngReq nodeGroupRequirements, machineType string) ([]int, bool) {
	cc := ngReq.computeClass
	maxMppn := 256
	isDynamicMaxPodsPerNodeEnabled := m.cloudProvider.IsAutopilotEnabled()
	if !m.cloudProvider.IsAutopilotEnabled() && cc != nil {
		isDynamicMaxPodsPerNodeEnabled = cc.DynamicMaxPodsPerNodeEnabled()
	}
	// If it is a standard cluster with either isNodeGroupManaged or isDynamicMaxPodsPerNodeEnabled
	// equal to false 0 is returned
	if !isDynamicMaxPodsPerNodeEnabled {
		return []int{0}, isDynamicMaxPodsPerNodeEnabled
	}
	pods, err := m.podLister.List()
	if err != nil {
		klog.Errorf("Failed to get pods: %v", err)
	}
	podGroups := kubernetes.ArrangePodsBySchedulability(pods, map[string]bool{})
	// we only want to use 256 as maximum if there will be at least 10 nodes.
	// We don't want to create nodes with 256 max pods per node in small cluster
	// (< 10 nodes), as failure of one node might lead to failure of significantly
	// large group of pods.
	if len(podGroups.Scheduled)+len(podGroups.Unschedulable) < 2560 {
		return []int{110}, isDynamicMaxPodsPerNodeEnabled
	}
	estimatedNumberOfPods := 0
	if machineTypeInfo, err := m.cloudProvider.MachineConfigProvider().ToMachineType(machineType); err != nil {
		klog.Errorf("failed to get machine types, can't estimate the number of pods fitting onto a node: %v", err)
	} else {
		estimatedNumberOfPods = getEstimatedNumberOfPods(maxMppn, ngReq, machineTypeInfo)
	}
	if estimatedNumberOfPods < 110 {
		return []int{110}, isDynamicMaxPodsPerNodeEnabled
	}
	return []int{110, maxMppn}, isDynamicMaxPodsPerNodeEnabled
}

func validateMachineFamilyDwsSupport(ngReq *nodeGroupRequirements) caerrors.AutoscalerError {
	var machinesAllowingDWS []machinetypes.MachineFamily
	for _, mf := range ngReq.machineSpec.Families {
		if !mf.IsDwsDisabled() {
			machinesAllowingDWS = append(machinesAllowingDWS, mf)
		}
	}
	if len(machinesAllowingDWS) == 0 {
		var err []string
		if len(ngReq.machineSpec.Families) > 0 {
			// Using the implication as above that only one machine family supports given accelerator.
			err = append(err, ngReq.machineSpec.Families[0].Name())
		}
		return NewInvalidDwsMachineFamilyError(err)
	}
	return nil
}

// TpuRequestGenerator is a TPU request spec generator.
type TpuRequestGenerator struct {
	provider machineConfigProvider
}

func NewTpuRequestGenerator(p machineConfigProvider) *TpuRequestGenerator {
	return &TpuRequestGenerator{
		provider: p,
	}
}

func (trg TpuRequestGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, _ *podrequirements.Requirements, _ machinetypes.GpuRequest, tpuReq TpuRequest) caerrors.AutoscalerError {
	ngReq.tpuRequest = tpuReq
	if ngReq.computeClassRule != nil && ngReq.computeClassRule.TpuType() != "" {
		if !tpuReq.Empty() {
			return NewComputeClassPodIncompatibleError(ngReq.computeClass.Name(), ngReq.computeClass.CrdType())
		}
		if !trg.provider.MachineConfigProvider().IsTpuNapSupported(ngReq.computeClassRule.TpuType()) {
			metrics.Metrics.RegisterUnexpectedPod(metrics.ReasonTPUTypeUnsupported)
			return NewTpuTypeNotSupportedError(ngReq.computeClassRule.TpuType())
		}
		ngReq.tpuRequest = TpuRequest{
			TpuType:      ngReq.computeClassRule.TpuType(),
			Topology:     ngReq.computeClassRule.TpuTopology(),
			ChipsPerNode: ngReq.computeClassRule.TpuCount(),
		}
	}
	return nil
}

func (trg TpuRequestGenerator) ValidateRequirements(ngReq *nodeGroupRequirements) caerrors.AutoscalerError {
	if isUsingDWS(*ngReq) && !ngReq.tpuRequest.Empty() {
		return validateMachineFamilyDwsSupport(ngReq)
	}
	return nil
}

func (trg TpuRequestGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if ngReq.tpuRequest.Empty() {
		return nil
	}

	params.systemLabels[gkelabels.TPULabel] = ngReq.tpuRequest.TpuType
	if ngReq.tpuRequest.Topology != "" {
		params.systemLabels[gkelabels.TPUTopologyLabel] = ngReq.tpuRequest.Topology
	}
	tpuCount, err := trg.provider.MachineConfigProvider().GetTpuCountForMachineType(params.machineType)
	if err != nil {
		metrics.Metrics.RegisterUnexpectedPod(metrics.ReasonTPUMachineTypeUnsupported)
		return err
	}
	params.systemLabels[gkelabels.AcceleratorCountLabel] = strconv.FormatInt(tpuCount, 10)
	params.extraResources[tpu.ResourceGoogleTPU] = *resource.NewQuantity(tpuCount, resource.DecimalSI)
	return nil
}

func (trg TpuRequestGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, extraResources map[string]resource.Quantity) error {
	_, tpuSpecified := extraResources[tpu.ResourceGoogleTPU]
	if !tpuSpecified {
		return nil
	}

	tpuType, found := systemLabels[gkelabels.TPULabel]
	if !found {
		metrics.Metrics.RegisterUnexpectedPod(metrics.ReasonTPUTypeMissing)
		klog.V(5).Infof("%s resource is found but %s label is empty", tpu.ResourceGoogleTPU, gkelabels.TPULabel)
		return fmt.Errorf("%s resource is found but %s label is empty", tpu.ResourceGoogleTPU, gkelabels.TPULabel)
	}

	spec.Labels[gkelabels.TPULabel] = tpuType
	spec.TpuType = tpuType
	tpuTopology, isTopologyLabelPresent := systemLabels[gkelabels.TPUTopologyLabel]
	if isTopologyLabelPresent {
		spec.Labels[gkelabels.TPUTopologyLabel] = tpuTopology
		spec.TpuTopology = tpuTopology
	}

	tpuChipsPerVM, err := trg.provider.MachineConfigProvider().GetTpuCountForMachineType(spec.MachineType)
	if err != nil {
		metrics.Metrics.RegisterUnexpectedPod(metrics.ReasonTPUMachineTypeUnsupported)
		return fmt.Errorf("%s resource is found but the machine type %s doesn't support TPU", tpu.ResourceGoogleTPU, spec.MachineType)
	}
	tpuChipsPerVMLabel, found := systemLabels[gkelabels.AcceleratorCountLabel]
	if found {
		chipsPerNodeFromLabel, err := strconv.ParseInt(tpuChipsPerVMLabel, 10, 64)
		if err != nil {
			return fmt.Errorf("%s resource is found but accelerator count %s label is not a valid integer", tpu.ResourceGoogleTPU, tpuChipsPerVMLabel)
		}
		if tpuChipsPerVM != chipsPerNodeFromLabel {
			metrics.Metrics.RegisterUnexpectedPod(metrics.ReasonTPUAcceleratorCountDoesntMatch)
			return fmt.Errorf("accelerator count %v found, but doesn't match TPU machine type %s", chipsPerNodeFromLabel, spec.MachineType)
		}
	}
	spec.Labels[gkelabels.AcceleratorCountLabel] = fmt.Sprintf("%v", tpuChipsPerVM)
	if isTopologyLabelPresent {
		isMultiHost, err := trg.provider.MachineConfigProvider().IsMultiHostTpuPodslice(tpuType, tpuTopology, tpuChipsPerVM)
		if err != nil {
			metrics.Metrics.RegisterUnexpectedPod(metrics.ReasonInvalidTPUTopology)
			return err
		}
		spec.TpuMultiHost = isMultiHost
	} else {
		spec.TpuMultiHost = false
	}

	// Add taint for TPU for simulation purposes. Later removed by gke_manager while creating node pool.
	taint := apiv1.Taint{
		Effect: apiv1.TaintEffectNoSchedule,
		Key:    tpu.ResourceGoogleTPU,
		Value:  "present",
	}
	spec.Taints = append(spec.Taints, taint)

	return nil
}

// GpuRequestGenerator is a GPU request spec Generator.
type GpuRequestGenerator struct {
	cloudProvider napcloudprovider.AutoprovisioningCloudProvider
}

func NewGpuRequestGenerator(cloudProvider napcloudprovider.AutoprovisioningCloudProvider) *GpuRequestGenerator {
	return &GpuRequestGenerator{
		cloudProvider: cloudProvider,
	}
}

func (grg GpuRequestGenerator) ValidateRequirements(ngReq *nodeGroupRequirements) caerrors.AutoscalerError {
	if isUsingDWS(*ngReq) && !ngReq.gpuRequest.Empty() {
		return validateMachineFamilyDwsSupport(ngReq)
	}
	return nil
}

func (grg GpuRequestGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, _ *podrequirements.Requirements, gpuReq machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	ngReq.gpuRequest = gpuReq
	if ngReq.computeClassRule != nil {
		if !ngReq.computeClassRule.GpuRequest().Empty() {
			if !gpuReq.Empty() {
				if gpuReq.Signature() != ngReq.computeClassRule.GpuRequest().Signature() {
					return NewComputeClassPodIncompatibleError(ngReq.computeClass.Name(), ngReq.computeClass.CrdType())
				}
			}
			ngReq.gpuRequest = ngReq.computeClassRule.GpuRequest()
		}
	}
	return nil
}

func (grg GpuRequestGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if ngReq.gpuRequest.Empty() {
		return nil
	}

	// GPU request is passed through systemLabels and extraResources.
	gpuLabels := gkelabels.SystemLabelsForGPU(ngReq.gpuRequest.Config.GpuType,
		ngReq.gpuRequest.Config.PartitionSize,
		ngReq.gpuRequest.Config.MaxSharedClients,
		ngReq.gpuRequest.Config.SharingStrategy,
		ngReq.gpuRequest.Config.DriverVersion,
		grg.cloudProvider.IsNodeAutoprovisioningEnabled())
	for k, v := range gpuLabels {
		params.systemLabels[k] = v
	}
	params.extraResources[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(int64(ngReq.gpuRequest.Count), resource.DecimalSI)
	return nil
}

func (grg GpuRequestGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string,
	extraResources map[string]resource.Quantity) error {
	gpuRequest, gpuSpecified := extraResources[gpu.ResourceNvidiaGPU]
	if !gpuSpecified {
		return nil
	}

	zone, found := systemLabels[apiv1.LabelZoneFailureDomain]
	if !found {
		return fmt.Errorf("LabelZoneFailureDomain label not found")
	}
	gpuType, found := systemLabels[gkelabels.GPULabel]
	if !found {
		klog.V(5).Infof("GPULabel not found")
		return fmt.Errorf("GPULabel not found")
	}
	gpuPartitionSize := systemLabels[gkelabels.GPUPartitionSizeLabel]
	gpuMaxSharedClients := systemLabels[gkelabels.GPUMaxSharedClientsLabel]
	gpuSharingStrategy := systemLabels[gkelabels.GPUSharingStrategyLabel]
	gpuDriverVersion := systemLabels[gkelabels.GPUDriverVersionLabel]
	isManagedNode := systemLabels[gkelabels.ManagedNodeLabel] == "true"

	machine, err := grg.cloudProvider.GetMachineType(spec.MachineType, zone)
	if err != nil {
		klog.V(5).Infof("Couldn't get cpus and mem for machineType %s in zone %s; %v",
			spec.MachineType, zone, err)
		return fmt.Errorf("couldn't get cpus and mem for machineType %s in zone %s; %v", spec.MachineType, zone, err)
	}

	err = grg.cloudProvider.ValidateGpuConfig(gpuType, gpuPartitionSize, gpuMaxSharedClients, gpuSharingStrategy, spec.MachineType, gpuRequest.Value(), zone, machine.CPU, machine.Memory)
	if err != nil {
		klog.V(5).Infof("Invalid acceleratorType configuration for machine type %q: %v", spec.MachineType, err)
		return fmt.Errorf("invalid acceleratorType configuration for machine type %q: %v", spec.MachineType, err)
	}
	spec.Labels[gkelabels.GPULabel] = gpuType
	if gpuPartitionSize != "" {
		spec.Labels[gkelabels.GPUPartitionSizeLabel] = gpuPartitionSize
	}
	if gpuMaxSharedClients != "" {
		spec.Labels[gkelabels.GPUMaxSharedClientsLabel] = gpuMaxSharedClients
	}
	if gpuSharingStrategy != "" {
		spec.Labels[gkelabels.GPUSharingStrategyLabel] = gpuSharingStrategy
	}
	if gpuDriverVersion == gkelabels.DisabledGPUDriverVersionValue {
		spec.Labels[gkelabels.GPUDriverVersionLabel] = "disabled"
	} else if gpuDriverVersion != "" {
		spec.Labels[gkelabels.GPUDriverVersionLabel] = gpuDriverVersion
	}

	taint := apiv1.Taint{
		Effect: apiv1.TaintEffectNoSchedule,
		Key:    gpu.ResourceNvidiaGPU,
		Value:  "present",
	}
	if !slices.Contains(spec.Taints, taint) {
		spec.Taints = append(spec.Taints, taint)
	}

	physicalGpuCount, err := grg.cloudProvider.MachineConfigProvider().ToPhysicalGPUCount(gpuType, gpuPartitionSize, gpuMaxSharedClients, machinetypes.AllocatableGpuCount(gpuRequest.Value()))
	if err != nil {
		return err
	}

	if grg.cloudProvider.IsAutopilotEnabled() || isManagedNode {
		// In Autopilot clusters and on managed nodes, GPU pods are always 1 pod per node. We don't want to allow pods to leave GPUs unused.
		// We add a label containing the number of GPUs on a node, and Autopilot pods that request GPUs have affinity
		// for that label and the number of GPUs they request added by a webhook. This ensures that a pods always consumes
		// (and so pays for) all GPUs on a given node. More details: go/autopilot-gpu-design.
		spec.Labels[gkelabels.AcceleratorCountLabel] = fmt.Sprintf("%d", physicalGpuCount)
	}

	sharingConfig, err := buildGPUSharingConfig(gpuMaxSharedClients, gpuSharingStrategy)
	if err != nil {
		return err
	}

	var gpuDriverInstallationConfig *gke_api_beta.GPUDriverInstallationConfig
	if gpuDriverVersion == gkelabels.DisabledGPUDriverVersionValue {
		gpuDriverInstallationConfig = &gke_api_beta.GPUDriverInstallationConfig{
			GpuDriverVersion: "INSTALLATION_DISABLED",
		}
	}

	spec.Accelerators = []*gke_api_beta.AcceleratorConfig{
		{
			AcceleratorType:             gpuType,
			AcceleratorCount:            int64(physicalGpuCount),
			GpuPartitionSize:            gpuPartitionSize,
			GpuSharingConfig:            sharingConfig,
			GpuDriverInstallationConfig: gpuDriverInstallationConfig,
		},
	}
	return nil
}

func buildGPUSharingConfig(gpuMaxSharedClients, gpuSharingStrategy string) (*gke_api_beta.GPUSharingConfig, error) {
	if gpuMaxSharedClients == "" {
		return nil, nil
	}
	maxSharedClients, err := machinetypes.GetMaxGpuSharedClients(gpuMaxSharedClients)
	if err != nil {
		return nil, err
	}
	sharingConfig := &gke_api_beta.GPUSharingConfig{}
	sharingConfig.MaxSharedClientsPerGpu = maxSharedClients
	switch gpuSharingStrategy {
	case gkelabels.GPUTimeSharingStrategy:
		sharingConfig.GpuSharingStrategy = string(gkeclient.TimeSharing)
	case gkelabels.GPUMpsStrategy:
		sharingConfig.GpuSharingStrategy = string(gkeclient.Mps)
	default:
		klog.Warningf("The gpuSharing '%v' strategy cannot be converted to "+
			"gkelables value, e.g., %v", gpuSharingStrategy, gkelabels.GPUTimeSharingStrategy)
		// This should never happen, as sharing strategy should be set up at this point.
		return nil, fmt.Errorf("GPU sharing strategy not set up")
	}
	return sharingConfig, nil
}

// WorkloadSeparationGenerator is a workload separation spec Generator.
type WorkloadSeparationGenerator struct {
	checker *podrequirements.WorkloadSeparationLabelsChecker
}

func NewWorkloadSeparationGenerator(matcher *gkelabels.Matcher) *WorkloadSeparationGenerator {
	return &WorkloadSeparationGenerator{
		checker: podrequirements.NewWorkloadSeparationWorkloadChecker(matcher),
	}
}

func (wsg WorkloadSeparationGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	workloadSeparationTaints, workloadSeparationLabels, err := podReq.WorkloadSeparationTaintsAndLabels(wsg.checker, podrequirements.AllowedNonWorkloadSeparationLabels(ngReq.pods...))
	if err != nil {
		return err
	}
	ngReq.workloadSeparationTaints = workloadSeparationTaints
	ngReq.workloadSeparationLabels = workloadSeparationLabels
	return nil
}

func (wsg WorkloadSeparationGenerator) ValidateRequirements(ngReq *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (wsg WorkloadSeparationGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	// Workload separation is passed through labels and taints.
	for k, v := range ngReq.workloadSeparationLabels {
		params.labels[k] = v
	}
	params.taints = append(params.taints, ngReq.workloadSeparationTaints...)
	return nil
}

func (wsg WorkloadSeparationGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string,
	extraResources map[string]resource.Quantity) error {
	return nil
}

// CSNGenerator is Generator responsible for creating the CSN soft taint and label in NAP nodegroups.
type CSNGenerator struct {
	isCSNEnabled bool
}

func NewCSNGenerator(isCSNEnabled bool) *CSNGenerator {
	return &CSNGenerator{
		isCSNEnabled: isCSNEnabled,
	}
}

func (g CSNGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	if !g.isCSNEnabled {
		return nil
	}

	if v, ok := podReq.LabelReq.GetSingleValue(csn.SoftWorkloadSeparationKey); ok && v == csn.SoftWorkloadSeparationValue {
		if ngReq.systemLabels == nil {
			ngReq.systemLabels = make(map[string]string)
		}
		ngReq.systemLabels[csn.SoftWorkloadSeparationKey] = csn.SoftWorkloadSeparationValue
	}
	return nil
}

func (g CSNGenerator) ValidateRequirements(ngReq *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (g CSNGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if !g.isCSNEnabled {
		return nil
	}

	if ngReq.systemLabels[csn.SoftWorkloadSeparationKey] != csn.SoftWorkloadSeparationValue {
		return nil
	}

	if !taints.TaintExists(params.taints, &csn.SoftWorkloadSeparationTaint) {
		params.taints = append(params.taints, csn.SoftWorkloadSeparationTaint)
	}

	if params.systemLabels == nil {
		params.systemLabels = make(map[string]string)
	}
	params.systemLabels[csn.SoftWorkloadSeparationKey] = csn.SoftWorkloadSeparationValue

	return nil
}

func (g CSNGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string,
	extraResources map[string]resource.Quantity) error {
	return nil
}

// MachineSelectionGenerator is a machine selection spec Generator.
type MachineSelectionGenerator struct {
	cloudProvider                 napcloudprovider.AutoprovisioningCloudProvider
	machineSelector               machineselection.Selector
	resizableMachineTypesProvider config.Provider[sets.Set[string]]
}

func NewMachineSelectionGenerator(cloudProvider napcloudprovider.AutoprovisioningCloudProvider, machineSelector machineselection.Selector, resizableMachineTypesProvider config.Provider[sets.Set[string]]) *MachineSelectionGenerator {
	return &MachineSelectionGenerator{
		cloudProvider:                 cloudProvider,
		machineSelector:               machineSelector,
		resizableMachineTypesProvider: resizableMachineTypesProvider,
	}
}

func (msg MachineSelectionGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	autopilotEnabled := msg.cloudProvider.IsAutopilotEnabled()
	autopilotManaged := ngReq.computeClass != nil && ngReq.computeClass.AutopilotManaged()
	isStateless := !isStatefulWorkload(ngReq)
	machineSpec, machineSelectionType, err := msg.machineSelector.Select(
		podReq.LabelReq,
		ngReq.gpuRequest.Config.GpuType,
		ngReq.tpuRequest.TpuType,
		ngReq.bootDiskType,
		ngReq.computeClassRule,
		wantsSpot(ngReq.pods, ngReq.computeClassRule, ngReq.flexStartReq.enabled),
		ngReq.reservation.machineType,
		autopilotEnabled,
		autopilotManaged,
		isStateless,
	)
	if err != nil {
		return err
	}
	ngReq.machineSpec = machineSpec
	ngReq.machineSelectionType = machineSelectionType
	if vals, exists := podReq.LabelReq.GetValues(apiv1.LabelInstanceTypeStable); exists {
		mtMap := vals.Get()
		mtList := []string{}
		for mt, ok := range mtMap {
			if ok {
				mtList = append(mtList, mt)
			}
		}
		ngReq.machineSpec.ExplicitMachineTypes = mtList
	}
	if ngReq.computeClassRule != nil && ngReq.computeClassRule.MachineType() != "" {
		ngReq.machineSpec.ExplicitMachineTypes = []string{ngReq.computeClassRule.MachineType()}
	}
	return nil
}

func (msg MachineSelectionGenerator) ValidateRequirements(ngReq *nodeGroupRequirements) caerrors.AutoscalerError {
	return msg.machineSelector.ValidateSpec(ngReq.machineSpec)
}

func (msg MachineSelectionGenerator) GenerateNodeGroupOptionsForRequirements(options []NodeGroupOptions, requirements nodeGroupRequirements) []NodeGroupOptions {
	var minCores, minMemoryGb int64 = 0, 0
	var hugepage1g, hugepage2m, totalHugepageSizeInMB int64
	var evictionMemoryAvailableStr string
	var overcommitSysctlSet, numaAlignmentNeeded bool
	var overcommitSysctlMachineMemoryLowerBound int64 = 15 * units.GiB

	if requirements.computeClassRule != nil {
		minCores = requirements.computeClassRule.MinCores()
		minMemoryGb = requirements.computeClassRule.MinMemoryGb()
		if requirements.computeClassRule.HugepageSize1g() != nil {
			hugepage1g = *requirements.computeClassRule.HugepageSize1g()
		}
		if requirements.computeClassRule.HugepageSize2m() != nil {
			hugepage2m = *requirements.computeClassRule.HugepageSize2m()
		}
		totalHugepageSizeInMB = hugepage2m*2 + hugepage1g*1024
		if requirements.computeClassRule.EvictionSoftMemoryAvailable() != nil {
			evictionMemoryAvailableStr = *requirements.computeClassRule.EvictionSoftMemoryAvailable()
		}
		if sysctls := requirements.computeClassRule.Sysctls(); sysctls != nil {
			if v, exists := sysctls["vm.overcommit_memory"]; exists && v == "2" {
				overcommitSysctlSet = true
			}
		}
		if requirements.computeClassRule.MemoryManagerPolicy() != nil ||
			requirements.computeClassRule.TopologyManagerPolicy() != nil ||
			requirements.computeClassRule.TopologyManagerScope() != nil {
			numaAlignmentNeeded = true
		}
	}

	var evictionMemoryAvailable int64
	if evictionMemoryAvailableStr != "" {
		if q, err := resource.ParseQuantity(evictionMemoryAvailableStr); err == nil {
			evictionMemoryAvailable = q.Value()
		} else {
			klog.Errorf("Failed to parse EvictionSoft.MemoryAvailable: %v", err)
		}
	}

	var supportedResizableMachineTypes sets.Set[string]
	if msg.resizableMachineTypesProvider != nil {
		supportedResizableMachineTypes = msg.resizableMachineTypesProvider.Provide()
	}
	var result []NodeGroupOptions
	for _, option := range options {
		for _, machine := range requirements.machineSpec.AutoprovisionedMachineTypes() {
			// check if machine spec supports min required CPU and memory.
			if machine.CPU < minCores || machine.Memory < minMemoryGb*units.GiB {
				continue
			}

			// check if hugepages are supported by this machine type.
			if hugepage1g > 0 {
				if mf, err := msg.cloudProvider.MachineConfigProvider().GetMachineFamilyFromMachineName(machine.Name); err == nil && !mf.IsHugepageSize1gSupported() {
					continue
				}
			}
			if totalHugepageSizeInMB > machine.MaximumAllocatableHugepageCapacityInMB() {
				continue
			}

			if numaAlignmentNeeded {
				mf, err := msg.cloudProvider.MachineConfigProvider().GetMachineFamilyFromMachineName(machine.Name)
				if err != nil {
					klog.V(5).Infof("Skipping machine type %s: unable to determine family for NUMA alignment check", machine.Name)
					continue
				}
				if !mf.IsNumaAlignmentSupported() {
					continue
				}
			}

			if evictionMemoryAvailable > 0 {
				if evictionMemoryAvailable >= machine.MaximumAllowedEvictionMemory() {
					continue
				}
			}

			if overcommitSysctlSet {
				if machine.Memory < overcommitSysctlMachineMemoryLowerBound {
					continue
				}
			}

			// check if machine type is supported.
			if msg.shouldSkipMachineType(machine.Name, option.Zone, supportedResizableMachineTypes) {
				continue
			}

			option.MachineType = machine.Name
			result = append(result, option)
		}
	}
	return result
}

func (msg MachineSelectionGenerator) shouldSkipMachineType(machineName, zone string, supportedResizableMachineTypes sets.Set[string]) bool {
	// Check if the machine type is resizable.
	isResizable, err := ekvms_utils.IsResizableMachineType(msg.cloudProvider.MachineConfigProvider(), machineName)
	if err != nil {
		// This probably shouldn't happen, but good to handle.
		klog.Errorf("Error checking if machine type %s is resizable: %v", machineName, err)
		return true
	}

	// If the machine type is resizable, check if it is supported.
	if isResizable {
		return !IsResizableMachineTypeSupported(machineName, supportedResizableMachineTypes)
	}

	// If the machine type is not resizable, check if it is a valid machine type in the zone.
	if _, err := msg.cloudProvider.GetMachineType(machineName, zone); err != nil {
		klog.V(5).Infof("Skipping machine type %s in zone %s, err: %v", machineName, zone, err)
		return true
	}

	return false
}

func (msg MachineSelectionGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	// Min cpu platform is passed through system labels, but only if it's concrete (as opposed to AnyPlatform, which
	// means passing an empty string to GCE).
	if platformName := machinetypes.CanonicalCpuPlatformName(ngReq.machineSpec.MinCpuPlatform, true); platformName != "" {
		// K8s labels don't allow spaces, so spaces in CPU platform names are converted to underscores.
		params.systemLabels[gkelabels.RequestedMinCpuPlatformLabel] = platformName
	}

	// Compute class is passed through system labels, but only if it's set.
	if ngReq.machineSpec.ComputeClassName != "" {
		params.systemLabels[gkelabels.ComputeClassLabel] = ngReq.machineSpec.ComputeClassName
	}
	if ngReq.computeClass != nil && ngReq.computeClass.ArchitectureTaintBehavior() != "" {
		params.systemLabels[labelArchitectureTaintBehavior] = ngReq.computeClass.ArchitectureTaintBehavior()
	}
	return nil
}

func (msg MachineSelectionGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string,
	extraResources map[string]resource.Quantity) error {
	machineFamily, err := msg.cloudProvider.MachineConfigProvider().GetMachineFamilyFromMachineName(spec.MachineType)
	if err != nil {
		return fmt.Errorf("unknown machine type %q, %v", spec.MachineType, err)
	}
	machineType, err := msg.cloudProvider.MachineConfigProvider().ToMachineType(spec.MachineType)
	if err != nil {
		return err
	}

	// Set "cloud.google.com/gke-cpu-scaling-level" node label for the number of vCPUs in the node VM.
	// This label is used to allow system pods to have different DaemonSets based on the number of VM vCPUs.
	// See go/gke-metrics-agent-vertical-scaling-vm-size
	// Control plane xref: http://cs/google3/cloud/kubernetes/distro/legacy/kube_env.go;rcl=586955697;l=4521
	spec.Labels[gkelabels.CpuScalingLevelLabel] = strconv.FormatInt(machineType.CPU, 10)

	// Set "cloud.google.com/gke-memory-scaling-level" node label for the memory size (GB) in the node VM.
	// This label is used to allow system pods to have different DaemonSets based on the memory size (GB) of VMs.
	// See go/dpv2-deployment-ekvm
	// Control plane xref: http://cs/google3/cloud/kubernetes/distro/legacy/kube_env.go;rcl=586955697;l=4521
	spec.Labels[gkelabels.MemoryScalingLevelLabel] = strconv.FormatInt(machineType.Memory/units.GB, 10)

	// Autopilot pods are billed depending on the family /compute class of the node they get scheduled on. We want
	// to taint node pools with compute classes or non-default machine families in Autopilot clusters so that pods
	// without any affinity don't get scheduled there. Autopilot pods requesting compute classes and non-default
	// machine families automatically get the corresponding toleration via a webhook.
	computeClassName, computeClassSpecified := systemLabels[gkelabels.ComputeClassLabel]
	if msg.cloudProvider.IsAutopilotEnabled() {

		// Do not taint node pool with compute class if
		// 1. The node pool has no compute class label.
		// 2. The node pool has custom compute class specified.
		if computeClassSpecified && !machinetypes.IsCustomComputeClass(computeClassName) {
			spec.ComputeClass = computeClassName
			spec.Labels[gkelabels.ComputeClassLabel] = computeClassName
			spec.Taints = append(spec.Taints, apiv1.Taint{
				Key:    gkelabels.ComputeClassLabel,
				Value:  computeClassName,
				Effect: apiv1.TaintEffectNoSchedule,
			})
		}

		_, gpuSpecified := extraResources[gpu.ResourceNvidiaGPU]
		_, tpuSpecified := extraResources[tpu.ResourceGoogleTPU]
		defaultFamily, _ := msg.machineSelector.DefaultMachineFamily()
		isDefaultFamily := machineFamily.Equal(defaultFamily)
		isEkFamily := machineFamily.Equal(machinetypes.EK) && msg.cloudProvider.IsResizableVmEnabledInAutopilot(machinetypes.EK.Name())
		// Do not taint the node pool with machine family if
		// 1. The node pool already has compute class taint.
		// 2. The node pool already has GPU taint.
		// 3. The node pool already has TPU taint.
		// 4. The node pool is using default machine family.
		// 5. The node pool is using EK machine family in ekAutoprovisioning mode.
		if !computeClassSpecified && !gpuSpecified && !tpuSpecified && !isDefaultFamily && !isEkFamily {
			spec.Taints = append(spec.Taints, apiv1.Taint{
				Key:    gkelabels.MachineFamilyLabel,
				Value:  machineFamily.Name(),
				Effect: apiv1.TaintEffectNoSchedule,
			})
		}
	}

	// TODO(b/266688085): Remove arch label.
	arch := machineFamily.SystemArchitecture()
	spec.SystemArchitecture = &arch
	spec.ArchTaintBehavior = systemLabels[labelArchitectureTaintBehavior]

	if arch.Name() != "" {
		// Add SystemArchitecture Label
		spec.Labels[apiv1.LabelArchStable] = arch.Name()

		// In GKE standard we only want to taint non-default architectures.
		// In Autopilot clusters taint is always added if compute class Scale-Out is used.
		addTaint := arch.Name() == gce.Arm64.Name() || computeClassName == machinetypes.ScaleOutClass.Name()

		// Users can explicitly disable architecture tainting by setting the
		// `architectureTaintBehavior` to "NONE".
		if spec.ArchTaintBehavior == "NONE" {
			addTaint = false
		}

		if addTaint {
			spec.Taints = append(spec.Taints, apiv1.Taint{
				Key:    apiv1.LabelArchStable,
				Value:  arch.Name(),
				Effect: apiv1.TaintEffectNoSchedule,
			})
		}
	}

	// min_cpu_platform is passed through labels to avoid changing the OSS NewNodeGroup signature.
	minCpuPlatformName := ""
	minCpuPlatformNameUnderscores, minCpuPlatformSpecified := systemLabels[gkelabels.RequestedMinCpuPlatformLabel]
	if minCpuPlatformSpecified {
		minCpuPlatform, err := machinetypes.ToCpuPlatform(minCpuPlatformNameUnderscores)
		if err != nil {
			return fmt.Errorf("unknown min_cpu_platform %q in systemLabels - this should never happen", minCpuPlatformNameUnderscores)
		}
		// The platform name in labels has to have underscores instead of spaces, but the value passed to GCE has to
		// have spaces - convert back to spaces here.
		minCpuPlatformName = machinetypes.CanonicalCpuPlatformName(minCpuPlatform, false)

		// More details about the labels and the rest of the API: go/ap-min-cpu-platform.
		spec.Labels[gkelabels.RequestedMinCpuPlatformLabel] = minCpuPlatformNameUnderscores
		for _, platform := range machinetypes.PlatformsLowerOrEqualTo(minCpuPlatform) {
			spec.Labels[gkelabels.SupportedCpuPlatformKeyPrefix+machinetypes.CanonicalCpuPlatformName(platform, true)] = gkelabels.SupportedCpuPlatformValue
		}
	}
	spec.MinCpuPlatform = minCpuPlatformName

	// Add GPU taint if GPU machine type is used. Otherwise, NAP will consider
	// such a machine type valid for non-GPU pod. GPU taint will be added
	// by control plane and NAP will create GPU node pools in an endless loop.
	// See b/404855936.
	if machineType.HasFixedGPU() {
		gpuTaint := apiv1.Taint{
			Key:    gpu.ResourceNvidiaGPU,
			Value:  "present",
			Effect: apiv1.TaintEffectNoSchedule,
		}
		if !slices.Contains(spec.Taints, gpuTaint) {
			spec.Taints = append(spec.Taints, gpuTaint)
		}
	}

	return nil
}

// IsResizableMachineTypeSupported returns true if the machine type is on the list of supported resizable machine types.
func IsResizableMachineTypeSupported(machineType string, supportedResizableMachineTypeNames sets.Set[string]) bool {
	// checking for supported resizable machine types disabled
	if supportedResizableMachineTypeNames == nil {
		return false
	}
	return supportedResizableMachineTypeNames.Has(machineType)
}

// ProvisioningRequestGenerator is a ProvisioningRequest node spec Generator.
type ProvisioningRequestGenerator struct {
	experimentsManager experiments.Manager
	provider           machineConfigProvider
}

func NewProvisioningRequestGenerator(experimentsManager experiments.Manager, provider machineConfigProvider) *ProvisioningRequestGenerator {
	return &ProvisioningRequestGenerator{
		experimentsManager: experimentsManager,
		provider:           provider,
	}
}

func (prg *ProvisioningRequestGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	ngReq.queuedProvisioningReq = podReq.QueuedProvisioningReq
	return nil
}

func (prg *ProvisioningRequestGenerator) ValidateRequirements(_ *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (prg *ProvisioningRequestGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if ngReq.queuedProvisioningReq.Enabled {
		params.systemLabels[gkelabels.ProvisioningRequestLabelKey] = ngReq.queuedProvisioningReq.ResizeRequestName
	}
	return nil
}

func (prg *ProvisioningRequestGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, extraResources map[string]resource.Quantity) error {
	rrName, isQueuedProvisioning := systemLabels[gkelabels.ProvisioningRequestLabelKey]
	if !isQueuedProvisioning {
		return nil
	}

	spec.QueuedProvisioning = true

	bulkFSQEnabled := prg.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ProvisioningRequestBulkMigsFlag, false)
	if bulkFSQEnabled && spec.UsesBulkProvisioning(prg.provider.MachineConfigProvider()) {
		if spec.Labels == nil {
			spec.Labels = make(map[string]string)
		}
		// This label is set only for bulk MIGs for the following reasons:
		//
		// 1. Non-bulk queued-provisioning MIGs use Resize Requests to create nodes.
		//    The resulting GCE instances have the 'google-compute-mig-resize-request' metadata item.
		//    The gcp-controller-manager detects this metadata and sets the
		//    'autoscaling.gke.io/provisioning-request' label on the corresponding Node.
		//
		// 2. Bulk queued-provisioning MIGs use the CreateInstances API to create nodes.
		//    Instances created this way do not originate from a Resize Request and thus lack the
		//    'google-compute-mig-resize-request' metadata item. Consequently, gcp-controller-manager
		//    does not automatically set the 'autoscaling.gke.io/provisioning-request' label on these Nodes.
		//
		// 3. GCW adds an 'autoscaling.gke.io/provisioning-request' node selector to Pods consuming
		//    Provisioning Requests. For the Pods to be scheduled on the provisioned nodes, the nodes must have this label.
		//
		// More details can be found in b/436471463.
		spec.Labels[gkelabels.ProvisioningRequestLabelKey] = rrName
	}

	defaultDwsUpgradeStrategy(spec)
	return nil
}

// MaxRunDurationGenerator is a MaxRunDuration Generator.
type MaxRunDurationGenerator struct {
	cloudProvider      cloudprovider.CloudProvider
	experimentsManager experiments.Manager
}

func NewMaxRunDurationGenerator(experimentsManager experiments.Manager, cloudProvider cloudprovider.CloudProvider) *MaxRunDurationGenerator {
	return &MaxRunDurationGenerator{
		cloudProvider:      cloudProvider,
		experimentsManager: experimentsManager,
	}
}

func (mrdg *MaxRunDurationGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	podMRD, mrdIsRequested := podReq.LabelReq.GetSingleValue(gkelabels.MaxRunDurationLabelKey)
	ngReq.maxRunDurationInSeconds = podMRD

	if ngReq.computeClassRule != nil && ngReq.computeClassRule.MaxRunDurationSeconds() != nil {
		ruleMRD := fmt.Sprintf("%d", *ngReq.computeClassRule.MaxRunDurationSeconds())
		if mrdIsRequested && podMRD != ruleMRD {
			return NewComputeClassPodIncompatibleError(ngReq.computeClass.Name(), ngReq.computeClass.CrdType())
		}
		ngReq.maxRunDurationInSeconds = ruleMRD
	}

	return nil
}

func (mrdg *MaxRunDurationGenerator) ValidateRequirements(ngReq *nodeGroupRequirements) caerrors.AutoscalerError {
	if ngReq.maxRunDurationInSeconds == "" {
		return nil
	}

	if ngReq.queuedProvisioningReq.Enabled {
		if len(ngReq.pods) == 0 || !pods.UsesBulkProvisioning(mrdg.cloudProvider, mrdg.experimentsManager, ngReq.pods[0]) {
			return caerrors.NewAutoscalerError(caerrors.ConfigurationError, "MaxRunDuration cannot be set on non-bulk QueuedProvisioning pools.")
		}
	}

	mrd, err := strconv.ParseInt(ngReq.maxRunDurationInSeconds, 10, 64)
	if err != nil {
		return caerrors.NewAutoscalerError(caerrors.ConfigurationError, "MaxRunDuration is not a valid int64.")
	}
	if mrd < minMRDInSeconds || mrd > maxMRDInSeconds {
		return caerrors.NewAutoscalerErrorf(caerrors.ConfigurationError, "MaxRunDuration is not within the allowed range (30 seconds - 120 days). Got %v seconds.", mrd)
	}

	if ngReq.flexStartReq.enabled {
		if mrd < minFlexStartMRDInSeconds || mrd > maxFlexStartMRDInSeconds {
			return caerrors.NewAutoscalerErrorf(caerrors.ConfigurationError, "MaxRunDuration is not within the allowed range (10 minutes - 7 days). Got %v seconds.", mrd)
		}
	}

	return nil
}

func (mrdg *MaxRunDurationGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if ngReq.maxRunDurationInSeconds != "" {
		params.systemLabels[gkelabels.MaxRunDurationLabelKey] = ngReq.maxRunDurationInSeconds
	}
	return nil
}

func (mrdg *MaxRunDurationGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, extraResources map[string]resource.Quantity) error {
	mrd, ok := systemLabels[gkelabels.MaxRunDurationLabelKey]
	if !ok || mrd == "" {
		return nil
	}
	if spec.Labels == nil {
		spec.Labels = make(map[string]string)
	}
	spec.Labels[gkelabels.MaxRunDurationLabelKey] = mrd
	spec.MaxRunDurationInSeconds = mrd
	return nil
}

// FlexStartGenerator is a FlexStart Generator.
type FlexStartGenerator struct {
	experimentsManager experiments.Manager
}

func NewFlexStartGenerator(experimentsManager experiments.Manager) *FlexStartGenerator {
	return &FlexStartGenerator{
		experimentsManager: experimentsManager,
	}
}

func (fsg *FlexStartGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, tpuReq TpuRequest) caerrors.AutoscalerError {
	if err := fsg.updateFlexStartEnabledReq(ngReq, podReq); err != nil {
		return err
	}

	flexStartTainted := fsg.hasFlexStartToleration(podReq)
	if !ngReq.flexStartReq.enabled {
		if flexStartTainted {
			return NewFlexStartMisconfiguredError(fmt.Sprintf("pod cannot have only Flex Start toleration %q to trigger node auto-provisioning, please also specify the node selector", gkelabels.FlexStartLabel))
		}
		return nil
	}
	ngReq.flexStartReq.tainted = flexStartTainted

	if selectors, found := fsg.incompatibleNodeSelectors(podReq); found {
		return NewFlexStartMisconfiguredError(fmt.Sprintf("Flex Start pod has incompatible node selectors: %v", selectors))
	}
	return nil
}

func (fsg *FlexStartGenerator) updateFlexStartEnabledReq(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements) caerrors.AutoscalerError {
	if fsg.experimentsManager == nil {
		return nil
	}

	flexStartEnabled := fsg.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexStartNonQueuedEnabledFlag, false)
	flexStartNAPEnabled := fsg.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexStartNonQueuedNAPEnabledFlag, false)
	if !flexStartEnabled || !flexStartNAPEnabled {
		return nil
	}

	flexStartLabelPresent := false
	if v, found := podReq.LabelReq.GetSingleValue(gkelabels.FlexStartLabel); found {
		if v != gkelabels.FlexStartValue {
			return NewFlexStartMisconfiguredError(fmt.Sprintf("Flex Start invalid %q node selector value %q", gkelabels.FlexStartLabel, v))
		}
		ngReq.flexStartReq.enabled = true
		flexStartLabelPresent = true
	}

	if v, found := podReq.LabelReq.GetSingleValue(gkelabels.ProvisioningLabel); found {
		if flexStartLabelPresent && v != gkelabels.FlexStartProvisioningValue {
			return NewFlexStartMisconfiguredError(fmt.Sprintf("Flex Start incompatible %q node selector value %q", gkelabels.ProvisioningLabel, v))
		}
		if v == gkelabels.FlexStartProvisioningValue {
			ngReq.flexStartReq.enabled = true
		}
	}

	if ok := fsg.updateFlexStartReqFromComputeClassReq(ngReq, podReq); !ok {
		return NewComputeClassPodIncompatibleError(ngReq.computeClass.Name(), ngReq.computeClass.CrdType())
	}
	return nil
}

func (*FlexStartGenerator) updateFlexStartReqFromComputeClassReq(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements) bool {
	if ngReq.computeClassRule == nil || !ngReq.computeClassRule.FlexStartEnabled() {
		return true
	}
	ngReq.flexStartReq.enabled = true

	ccLeadTimeSeconds := ""
	if ngReq.computeClassRule.FlexStartNodeRecyclingLeadTimeSeconds() != nil {
		ccLeadTimeSeconds = fmt.Sprintf("%d", *ngReq.computeClassRule.FlexStartNodeRecyclingLeadTimeSeconds())
	}
	podLeadTimeSeconds := ""
	if v, found := podReq.LabelReq.GetSingleValue(gkelabels.NodeRecycleLeadTimeSecondsLabelKey); found {
		podLeadTimeSeconds = v
	}
	return podLeadTimeSeconds == "" || podLeadTimeSeconds == ccLeadTimeSeconds
}

func (*FlexStartGenerator) incompatibleNodeSelectors(podReq *podrequirements.Requirements) ([]string, bool) {
	flexStartIncompatibleSelectors := []string{gkelabels.SpotLabel, gkelabels.PreemptibleLabel}

	res := []string{}
	for _, selector := range flexStartIncompatibleSelectors {
		if _, found := podReq.LabelReq.GetValues(selector); found {
			res = append(res, selector)
		}
	}
	return res, len(res) > 0
}

func (*FlexStartGenerator) hasFlexStartToleration(podReq *podrequirements.Requirements) bool {
	for _, toleration := range podReq.Tolerations {
		if toleration.Key == gkelabels.FlexStartLabel && toleration.Value == gkelabels.FlexStartValue {
			return true
		}
	}
	return false
}

func (fsg *FlexStartGenerator) ValidateRequirements(ngReq *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (fsg *FlexStartGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, opts NodeGroupOptions) error {
	if ngReq.flexStartReq.enabled {
		params.systemLabels[gkelabels.FlexStartLabel] = gkelabels.FlexStartValue
		if ngReq.flexStartReq.tainted {
			params.taints = append(params.taints, apiv1.Taint{
				Effect: apiv1.TaintEffectNoSchedule,
				Key:    gkelabels.FlexStartLabel,
				Value:  gkelabels.FlexStartValue,
			})
		}
		if ngReq.computeClassRule != nil && ngReq.computeClassRule.FlexStartNodeRecyclingLeadTimeSeconds() != nil {
			params.labels[gkelabels.NodeRecycleLeadTimeSecondsLabelKey] = fmt.Sprintf("%d", *ngReq.computeClassRule.FlexStartNodeRecyclingLeadTimeSeconds())
		}
	}
	return nil
}

func defaultDwsUpgradeStrategy(spec *gkeclient.NodePoolSpec) {
	if !(spec.FlexStart || spec.QueuedProvisioning) {
		return
	}
	// Default the upgrade strategy to SHORT_LIVED - the only strategy supported by DWS node pools
	if spec.UpgradeSettings == nil {
		spec.UpgradeSettings = &gke_api_beta.UpgradeSettings{}
	}
	spec.UpgradeSettings.Strategy = "SHORT_LIVED"
}

func (fsg *FlexStartGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, extraResources map[string]resource.Quantity) error {
	if _, found := systemLabels[gkelabels.FlexStartLabel]; found {
		if spec.Labels == nil {
			spec.Labels = make(map[string]string)
		}
		spec.Labels[gkelabels.FlexStartLabel] = gkelabels.FlexStartValue
		spec.Labels[gkelabels.ProvisioningLabel] = gkelabels.FlexStartProvisioningValue
		spec.FlexStart = true
	}
	defaultDwsUpgradeStrategy(spec)
	return nil
}

// DWSSupportFilteringGenerator is a DWS support node group option generator
type DWSSupportFilteringGenerator struct {
	provider dwsMachineConfigProvider
}

type dwsMachineConfigProvider interface {
	IsNotInDWS(string) (bool, error)
}

func NewDWSSupportFilteringGenerator(provider dwsMachineConfigProvider) *DWSSupportFilteringGenerator {
	return &DWSSupportFilteringGenerator{
		provider: provider,
	}
}

func isUsingDWS(requirements nodeGroupRequirements) bool {
	return requirements.flexStartReq.enabled || requirements.queuedProvisioningReq.Enabled
}

func (dsg *DWSSupportFilteringGenerator) GenerateNodeGroupOptionsForRequirements(options []NodeGroupOptions, requirements nodeGroupRequirements) []NodeGroupOptions {
	var result []NodeGroupOptions
	for _, opt := range options {
		if isUsingDWS(requirements) {
			notInDWS, err := dsg.provider.IsNotInDWS(opt.MachineType)
			// Exclude only known machine types explicitly marked as not in DWS
			if notInDWS && err == nil {
				continue
			}
		}
		result = append(result, opt)
	}
	if len(result) == 0 {
		klog.Warningf("No machine type in families %v supports DWS, it should be disabled on machine family level",
			requirements.machineSpec.Families)
	}
	return result
}

// BootDiskConfigGenerator is a boot disk config node spec Generator.
type BootDiskConfigGenerator struct {
	cloudProvider napcloudprovider.AutoprovisioningCloudProvider
}

func NewBootDiskConfigGenerator(cloudProvider napcloudprovider.AutoprovisioningCloudProvider) *BootDiskConfigGenerator {
	return &BootDiskConfigGenerator{
		cloudProvider: cloudProvider,
	}
}

func (bcg *BootDiskConfigGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	if v, exist := podReq.LabelReq.GetSingleValue(gkelabels.BootDiskTypeLabelKey); exist {
		ngReq.bootDiskType = v
	}

	if v, exist := podReq.LabelReq.GetSingleValue(gkelabels.BootDiskSizeLabelKey); exist {
		size, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return podrequirements.NewInvalidLabelValueError(gkelabels.BootDiskSizeLabelKey, v)
		}
		ngReq.bootDiskSize = int(size)
	}

	if v, exist := podReq.LabelReq.GetSingleValue(gkelabels.BootDiskEncryptionLabelKey); exist {
		ngReq.bootDiskEncryptionKey = v
		ngReq.bootDiskEncryptionAnnotation = podReq.DiskEncryptionKeyAnnotation
	}

	return nil
}

func (bcg *BootDiskConfigGenerator) ValidateRequirements(_ *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (bcg *BootDiskConfigGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if ngReq.bootDiskType != "" {
		params.systemLabels[gkelabels.BootDiskTypeLabelKey] = ngReq.bootDiskType
	}
	if ngReq.bootDiskSize != 0 {
		params.systemLabels[gkelabels.BootDiskSizeLabelKey] = strconv.Itoa(ngReq.bootDiskSize)
	}
	if ngReq.bootDiskEncryptionKey != "" {
		params.systemLabels[gkelabels.BootDiskEncryptionLabelKey] = ngReq.bootDiskEncryptionKey
	}
	if ngReq.bootDiskEncryptionAnnotation != "" {
		params.systemLabels[gkelabels.BootDiskEncryptionAnnotationKey] = ngReq.bootDiskEncryptionAnnotation
	}
	if len(ngReq.secondaryBootDisks) != 0 {
		// http://screenshot/BXxDYkzQ5PiCTLq.png
		// Json parsing is around 3 times more time consuming than the primitive custom one,
		// that stores objects' data divided by `,` and `;` special signs (test was done for 100000 5 sized slices of SecondaryBootDisk).
		// Overall for the amount of data we would probably face it would take just several milliseconds more in the worst case scenario.
		// In the common case additional process time would be less than 1ms which is acceptable.
		secondaryBootDisksJsonBytes, err := json.Marshal(ngReq.secondaryBootDisks)
		if err != nil {
			return err
		}
		secondaryBootDisksJsonString := string(secondaryBootDisksJsonBytes)
		params.systemLabels[gkelabels.SecondaryBootDisksLabelKey] = secondaryBootDisksJsonString
	}
	return nil
}

func (bcg *BootDiskConfigGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, extraResources map[string]resource.Quantity) error {
	if diskType, exist := systemLabels[gkelabels.BootDiskTypeLabelKey]; exist {
		zone, found := systemLabels[apiv1.LabelZoneFailureDomain]
		if !found {
			return fmt.Errorf("LabelZoneFailureDomain label not found")
		}

		ok, reason, err := bcg.cloudProvider.ValidateLocationForDiskType(zone, diskType)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New(reason)
		}

		spec.DiskType = diskType
		// Add this label to nodepool spec irrespective of the case when ComputeClass is required.
		// This label is used by gce_price_model.go in OSS to calculate node price.
		if spec.Labels == nil {
			spec.Labels = make(map[string]string)
		}
		spec.Labels[gkelabels.BootDiskTypeLabelKey] = diskType

	}
	if v, exist := systemLabels[gkelabels.BootDiskSizeLabelKey]; exist {
		size, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return fmt.Errorf("unable to parse int value %q for boot disk size label", v)
		}
		spec.DiskSize = size
		// Add this label to nodepool spec irrespective of the case when ComputeClass is required.
		// This label is used by estimator when the flag dynamicBootDiskSizingEnabled is true.
		if spec.Labels == nil {
			spec.Labels = make(map[string]string)
		}
		spec.Labels[gkelabels.BootDiskSizeLabelKey] = v
	}

	if labelValue, exist := systemLabels[gkelabels.BootDiskEncryptionLabelKey]; exist {
		// BootDiskEncryptionLabelKey is also being injected by the ComputeClassGenerator
		// which doesn't supply annotation value if it's not found - threating label
		// value as the encryption key itself
		if annotationValue, exist := systemLabels[gkelabels.BootDiskEncryptionAnnotationKey]; exist {
			if spec.Labels == nil {
				spec.Labels = make(map[string]string)
			}
			spec.Labels[gkelabels.BootDiskEncryptionLabelKey] = labelValue
			spec.DiskEncryptionKey = annotationValue
		} else {
			spec.DiskEncryptionKey = labelValue
		}
	}

	if secondaryBootDisksJsonString, exist := systemLabels[gkelabels.SecondaryBootDisksLabelKey]; exist {
		secondaryBootDisksJsonBytes := []byte(secondaryBootDisksJsonString)
		var deserializedSecondaryBootDisks []*gke_api_beta.SecondaryBootDisk
		err := json.Unmarshal(secondaryBootDisksJsonBytes, &deserializedSecondaryBootDisks)
		if err != nil {
			return err
		}
		spec.SecondaryBootDisks = deserializedSecondaryBootDisks
	}

	return nil
}

// ComputeClassGenerator is a generator for node provisioning configs and custom compute classes.
type ComputeClassGenerator struct {
	cloudProvider                 napcloudprovider.AutoprovisioningCloudProvider
	lister                        computeclass_lister.Lister
	enableComputeClassMinCapacity bool
	experimentsManager            experiments.Manager
}

// NewComputeClassGenerator creates a new generator.
func NewComputeClassGenerator(cloudProvider napcloudprovider.AutoprovisioningCloudProvider, lister computeclass_lister.Lister, enableComputeClassMinCapacity bool, experimentsManager experiments.Manager) *ComputeClassGenerator {
	return &ComputeClassGenerator{
		cloudProvider:                 cloudProvider,
		lister:                        lister,
		enableComputeClassMinCapacity: enableComputeClassMinCapacity,
		experimentsManager:            experimentsManager,
	}
}

func (ng ComputeClassGenerator) generateRuleRequirements(requirements nodeGroupRequirements, cc computeclass.CRD, rule rules.Rule) []nodeGroupRequirements {
	requirements.computeClass = cc
	requirements.computeClassRule = rule
	requirements.bootDiskType = rule.BootDiskType()
	requirements.bootDiskSize = int(rule.BootDiskSize())
	requirements.ephemeralStorageLocalSSDCount = rule.EphemerslStorageLSSDCount()
	requirements.totalLSSDCount = int(rule.TotalLSSDCount())
	requirements.bootDiskEncryptionKey = rule.BootDiskKMSKey()
	requirements.secondaryBootDisks = rule.SecondaryBootDisks()
	requirements.maxPodsPerNode = rule.MaxPodsPerNode()
	requirements.linuxNodeConfig = linuxNodeConfigFromCCRule(rule)
	requirements.kubeletConfig = kubeletConfigFromCCRule(rule)

	if len(rule.Reservations()) == 0 {
		return []nodeGroupRequirements{requirements}
	}

	ruleRequirements := make([]nodeGroupRequirements, len(rule.Reservations()))
	for i, reservation := range rule.Reservations() {
		requirements := requirements
		// Populating specifiedZones which is the main source of information about zones to be included in injected node pools.
		requirements.specifiedZones = reservation.Zones()
		requirements.reservation.project = reservation.Project()
		requirements.reservation.affinity = reservation.Affinity()
		requirements.reservation.name = reservation.Name()
		requirements.reservation.block = reservation.BlockName()
		requirements.reservation.subBlock = reservation.SubBlockName()
		ruleRequirements[i] = requirements
	}

	return ruleRequirements
}

func (ng ComputeClassGenerator) GenerateNodeGroupRequirements(ngReqs []nodeGroupRequirements, podReq *podrequirements.Requirements) ([]nodeGroupRequirements, caerrors.AutoscalerError) {
	cc, ccName, err := ng.lister.PodReqCrd(podReq)
	if err != nil {
		ccType, ccTypeErr := ng.lister.PodReqCrdType(podReq)
		return nil, NewComputeClassNotFoundError(ccName, ccType, ccTypeErr)
	}

	if cc == nil || ccName == "" {
		return ngReqs, nil
	}

	if !cc.AutoprovisioningEnabled() {
		return nil, NewComputeClassAutoprovisioningDisabled(cc.Name(), cc.CrdType())
	}

	var allRequirements []nodeGroupRequirements
	for _, rule := range cc.Rules() {
		// Do not generate ruleRequirements for nodepool rules
		if len(rule.NodePoolNames()) > 0 {
			continue
		}

		for _, requirements := range ngReqs {
			ruleRequirements := ng.generateRuleRequirements(requirements, cc, rule)
			allRequirements = append(allRequirements, ruleRequirements...)
		}
	}

	if cc.ScaleUpAnyway() {
		for _, requirements := range ngReqs {
			requirements.computeClass = cc
			allRequirements = append(allRequirements, requirements)
		}
	}

	if len(allRequirements) == 0 {
		return nil, NewComputeClassPodIncompatibleError(cc.Name(), cc.CrdType())
	}

	return allRequirements, nil
}

func (ng ComputeClassGenerator) UpdateRequirements(_ *nodeGroupRequirements, _ *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	return nil
}

func (ng ComputeClassGenerator) ValidateRequirements(_ *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (ng ComputeClassGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if !ngReq.RequiresComputeClass() {
		return nil
	}

	params.systemLabels[ngReq.computeClass.Label()] = ngReq.computeClass.Name()
	// The value of the label should never be used.
	params.systemLabels[labelComputeClassRequired] = "true"

	// Autopilot node pools in standard are tainted with additional taint and have additional label.
	if !ng.cloudProvider.IsAutopilotEnabled() && ngReq.computeClass.AutopilotManaged() {
		params.systemLabels[gkelabels.ManagedNodeLabel] = "true"
	}

	// Mark node group to use BPSoHW if cluster is Autopilot and pod family is not set
	usesAutopilotMode := ng.cloudProvider.IsAutopilotEnabled() || ngReq.computeClass.AutopilotManaged()
	usesPodFamily := ngReq.computeClassRule != nil && ngReq.computeClassRule.PodFamilyName() != ""
	if usesAutopilotMode {
		if usesPodFamily {
			params.systemLabels[gkelabels.GeneralPurposePodFamilyLabel] = "true"
		} else {
			params.systemLabels[gkelabels.PodsPerNodeKey] = gkelabels.BinpackedSliceOfHardwareValue
		}
	}

	// Mark node group to have DynamicBootDiskSize enabled (required for NapResourceAnalyzerFunc)
	if ngReq.computeClass.DynamicBootDiskSizeEnabled() {
		params.systemLabels[gkelabels.NodeGroupDynamicBootDiskSizeEnabledLabelKey] = "true"
	}

	if ngReq.computeClass.ServiceAccount() != "" {
		params.systemLabels[gkelabels.ServiceAccountLabelKey] = ngReq.computeClass.ServiceAccount()
	}

	if ngReq.computeClass.ImageType() != "" {
		params.systemLabels[gkelabels.ImageTypeLabelKey] = ngReq.computeClass.ImageType()
	}

	if ngReq.computeClass.TpuDriverMode() == computeclass.TpuDriverModeDynamicResourceAllocation {
		params.systemLabels[gkelabels.DraTpuNodeLabel] = "true"
	}

	params.taints = appendTaintsWithOverride(params.taints, ngReq.computeClass.UserDefinedTaints())
	if ngReq.computeClassRule != nil {
		params.taints = appendTaintsWithOverride(params.taints, ngReq.computeClassRule.UserDefinedTaints())
	}

	// Set compute class priority index label
	if !ng.enableComputeClassMinCapacity || !ccpkg.IsComputeClassMinCapacityEnabled(ng.experimentsManager) {
		return nil
	}
	priorityIdx := -1
	if ngReq.computeClassRule != nil {
		for idx, rule := range ngReq.computeClass.Rules() {
			if rule == ngReq.computeClassRule {
				priorityIdx = idx
				break
			}
		}
	}
	params.systemLabels[gkelabels.ComputeClassPriorityIdxLabel] = strconv.Itoa(priorityIdx)

	return nil
}

type taintId struct {
	key    string
	effect apiv1.TaintEffect
}

func getTaintId(t apiv1.Taint) taintId {
	return taintId{
		key:    t.Key,
		effect: t.Effect,
	}
}

func appendTaintsWithOverride(oldTaints, newTaints []apiv1.Taint) []apiv1.Taint {
	var taints = make(map[taintId]apiv1.Taint)
	var taintIds []taintId // preserve the order of taints
	for _, taint := range oldTaints {
		id := getTaintId(taint)
		taintIds = append(taintIds, id)
		taints[id] = taint
	}
	for _, taint := range newTaints {
		id := getTaintId(taint)
		if t, found := taints[id]; !found {
			taintIds = append(taintIds, id)
		} else if t != taint {
			klog.Warningf("Overriding taint %v with taint %v during nodeGroupParameters update. This might result in no scale-up option.", t, taint)
		}
		taints[id] = taint
	}

	var result []apiv1.Taint
	for _, id := range taintIds {
		taint, _ := taints[id]
		result = append(result, taint)
	}
	return result
}

func (ng ComputeClassGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, _ map[string]resource.Quantity) error {
	for _, label := range ng.lister.Labels() {
		ccName, ccExists := systemLabels[label]
		if !ccExists {
			continue
		}

		// Do not inject anything in case compute class specified is predefined one
		if label == gkelabels.ComputeClassLabel && machinetypes.IsPredefinedComputeClass(ccName) {
			continue
		}

		spec.Labels[label] = ccName

		// Set BPSoHW label if specified
		if val := systemLabels[gkelabels.PodsPerNodeKey]; val == gkelabels.BinpackedSliceOfHardwareValue {
			spec.Labels[gkelabels.PodsPerNodeKey] = val
		}

		// Set Autopilot managed node field and taint Autopilot managed node pools.
		if val := systemLabels[gkelabels.ManagedNodeLabel]; val == "true" && !ng.cloudProvider.IsAutopilotEnabled() {
			// Set nodepool spec to use Autopilot managed node.
			spec.AutopilotManaged = true
			// Set node label
			spec.Labels[gkelabels.ManagedNodeLabel] = val
			// Set taint
			spec.Taints = append(spec.Taints, apiv1.Taint{
				Effect: apiv1.TaintEffectNoSchedule,
				Key:    gkelabels.ManagedNodeLabel,
				Value:  val,
			})
		}

		// Set label that dynamic boot disk size is enabled
		if val := systemLabels[gkelabels.NodeGroupDynamicBootDiskSizeEnabledLabelKey]; val == "true" {
			// Set node label
			spec.Labels[gkelabels.NodeGroupDynamicBootDiskSizeEnabledLabelKey] = val
		}

		defaultName, defaultLabel, found := ng.lister.Default()
		if !found || defaultName != ccName || defaultLabel != label {
			spec.Taints = append(spec.Taints, apiv1.Taint{
				Effect: apiv1.TaintEffectNoSchedule,
				Key:    label,
				Value:  ccName,
			})
		}

		break
	}

	spec.ServiceAccount = systemLabels[gkelabels.ServiceAccountLabelKey]
	spec.ImageType = systemLabels[gkelabels.ImageTypeLabelKey]

	if systemLabels[gkelabels.DraTpuNodeLabel] == "true" {
		spec.Labels[gkelabels.DraTpuNodeLabel] = "true"
	}

	if val := systemLabels[gkelabels.ComputeClassPriorityIdxLabel]; val != "" {
		spec.Labels[gkelabels.ComputeClassPriorityIdxLabel] = val
	}
	if val := systemLabels[gkelabels.GeneralPurposePodFamilyLabel]; val == "true" {
		spec.Labels[gkelabels.GeneralPurposePodFamilyLabel] = val
	}

	return nil
}

// PlacementGroupGenerator is a placement group spec Generator.
type PlacementGroupGenerator struct {
	cloudProvider        napcloudprovider.AutoprovisioningCloudProvider
	resourcePolicyPuller placement.ResourcePolicyPuller
}

func NewPlacementGroupGenerator(cloudProvider napcloudprovider.AutoprovisioningCloudProvider, resourcePolicyPuller placement.ResourcePolicyPuller) *PlacementGroupGenerator {
	return &PlacementGroupGenerator{
		cloudProvider:        cloudProvider,
		resourcePolicyPuller: resourcePolicyPuller,
	}
}

func (pgg PlacementGroupGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	placementGroup := placementGroupSpec(ngReq, podReq.LabelReq)
	if ngReq.placementGroup.UsesPlacement() {
		placementGroup = ngReq.placementGroup
	}
	// skip if the placement policy wasn't specified
	if !placementGroup.UsesPlacement() {
		return nil
	}
	if err := placementGroup.Validate(pgg.cloudProvider.GetAllNodePoolNames()); err != nil {
		return err
	}
	if err := placementGroup.UpdateMachineSpec(&ngReq.machineSpec, ngReq.machineSelectionType); err != nil {
		return err
	}
	ngReq.placementGroup = placementGroup
	return nil
}

func (pgg PlacementGroupGenerator) ValidateRequirements(ngReq *nodeGroupRequirements) caerrors.AutoscalerError {
	machineConfigProvider := pgg.cloudProvider.MachineConfigProvider()
	ngReq.placementGroup.ResourcePolicy = pgg.resourcePolicyPuller.GetResourcePolicy(ngReq.placementGroup.Policy)
	return ngReq.placementGroup.ValidateMachineSpec(&ngReq.machineSpec, ngReq.machineSelectionType, ngReq.tpuRequest.Topology, ngReq.tpuRequest.ChipsPerNode, machineConfigProvider)
}

func (pgg PlacementGroupGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if !ngReq.placementGroup.UsesPlacement() {
		return nil
	}
	if ngReq.placementGroup.GroupId != "" {
		params.systemLabels[gkelabels.PlacementGroupLabel] = ngReq.placementGroup.GroupId
	}
	if ngReq.placementGroup.Policy != "" {
		params.systemLabels[gkelabels.PolicyLabel] = ngReq.placementGroup.Policy
	}
	return nil
}

func (pgg PlacementGroupGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, _ map[string]resource.Quantity) error {
	placementGroup := placement.FromLabels(systemLabels)
	if !placementGroup.UsesPlacement() {
		return nil
	}
	if pgg.cloudProvider.IsCompactPlacementEnabled() {
		if placementGroup.GroupId != "" {
			spec.Labels[gkelabels.PlacementGroupLabel] = placementGroup.GroupId
		}
		if placementGroup.Policy != "" {
			spec.Labels[gkelabels.PolicyLabel] = placementGroup.Policy
		}
	}
	placementGroup.ResourcePolicy = pgg.resourcePolicyPuller.GetResourcePolicy(placementGroup.Policy)
	spec.PlacementGroup = placementGroup
	machineFamily, err := pgg.cloudProvider.MachineConfigProvider().GetMachineFamilyFromMachineName(spec.MachineType)
	if err != nil {
		return fmt.Errorf("failed to get machine family from machine type %q, this should never happen at this point", spec.MachineType)
	}
	if placementGroup.ResourcePolicy != nil || !machineFamily.RequiresBYOResourcePolicy() {
		return nil
	}
	// TPUs support topology label (TPUs) which provides a fallback source of topology information.
	// We optimistically assume that correct WorkloadPolicy exists but hasn't been fetched yet.
	// If that's not true, node pool creation will fail.
	if spec.Labels[gkelabels.TPUTopologyLabel] != "" {
		spec.PlacementGroup.ResourcePolicy = &gceclient.GceResourcePolicy{
			Name:           placementGroup.Policy,
			WorkloadPolicy: gceclient.WorkloadPolicy{AcceleratorTopology: spec.Labels[gkelabels.TPUTopologyLabel]},
		}
	}
	if !spec.TpuMultiHost && machineFamily.IsTpuSupported() {
		return nil
	}
	return fmt.Errorf("machine family %s requires workload policy to determine the slice topology", machineFamily.Name())
}

// SandboxTypeGenerator is a sandbox type spec Generator.
type SandboxTypeGenerator struct{}

func NewSandboxTypeGenerator() *SandboxTypeGenerator {
	return &SandboxTypeGenerator{}
}

func (stg SandboxTypeGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	if v, ok := podReq.LabelReq.GetSingleValue(sandbox.RuntimeLabelKey); ok {
		if st, err := sandbox.TypeFromString(v); err == nil && st != sandbox.None && st != sandbox.Unsupported {
			ngReq.sandboxType = st
		}
	}
	return nil
}

func (stg SandboxTypeGenerator) ValidateRequirements(_ *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (stg SandboxTypeGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if ngReq.sandboxType != sandbox.None && ngReq.sandboxType != sandbox.Unsupported {
		params.systemLabels[sandbox.RuntimeLabelKey] = ngReq.sandboxType.String()
	}
	return nil
}

func (stg SandboxTypeGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string,
	extraResources map[string]resource.Quantity) error {
	v, ok := systemLabels[sandbox.RuntimeLabelKey]
	if !ok {
		return nil
	}
	st, err := sandbox.TypeFromString(v)
	if err != nil || st == sandbox.None || st == sandbox.Unsupported {
		return nil
	}

	spec.SandboxType = st
	// Propagate the label to the Mig to ensure scheduler can use it
	// in simulations when trying to schedule sandboxed pods.
	spec.Labels[sandbox.RuntimeLabelKey] = v
	// Add matching taint.
	spec.Taints = append(spec.Taints, apiv1.Taint{
		Effect: apiv1.TaintEffectNoSchedule,
		Key:    sandbox.RuntimeTaintKey,
		Value:  v,
	})
	return nil
}

// PreemeptionOptionGenerator is a preemeption option spec Generator.
type PreemeptionOptionGenerator struct {
	provisioningLabelEnabled bool
}

func NewPreemeptionOptionGenerator(provisioningLabelEnabled bool) *PreemeptionOptionGenerator {
	return &PreemeptionOptionGenerator{
		provisioningLabelEnabled: provisioningLabelEnabled,
	}
}

func (pog PreemeptionOptionGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, _ *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	return nil
}

func (pog PreemeptionOptionGenerator) ValidateRequirements(_ *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (pog PreemeptionOptionGenerator) GenerateNodeGroupOptionsForRequirements(options []NodeGroupOptions, requirements nodeGroupRequirements) []NodeGroupOptions {
	result := []NodeGroupOptions{}
	for _, option := range options {
		for _, preemptionOption := range preemptionOptions(requirements.pods, requirements.computeClassRule, requirements.flexStartReq.enabled) {
			option.Preemption = preemptionOption
			result = append(result, option)
		}
	}
	return result
}

func (pog PreemeptionOptionGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, opts NodeGroupOptions) error {
	// Preemption is passed through systemLabels and taints.
	if opts.Preemption != preemption.NoPreemption {
		preemptionLabelKey, preemptionLabelValue, err := opts.Preemption.LabelKeyValue()
		if err != nil {
			return fmt.Errorf("error while getting the preemption label - this should never happen: %v", err)
		}
		params.systemLabels[preemptionLabelKey] = preemptionLabelValue

		if ngReq.computeClassRule == nil {
			preemptionTaint, err := opts.Preemption.Taint()
			if err != nil {
				return fmt.Errorf("error while getting the preemption taint - this should never happen: %v", err)
			}
			params.taints = append(params.taints, preemptionTaint)
		}
	}

	if pog.provisioningLabelEnabled {
		params.systemLabels[gkelabels.ProvisioningLabel] = opts.Preemption.ProvisioningLabelValue()
	}
	return nil
}

func (pog PreemeptionOptionGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string,
	extraResources map[string]resource.Quantity) error {
	vmPreemptionType := preemption.TypeFromLabels(systemLabels)
	if vmPreemptionType != preemption.NoPreemption {
		preemptionLabelKey, preemptionLabelValue, err := vmPreemptionType.LabelKeyValue()
		if err != nil {
			return fmt.Errorf("error while getting the preemption label - this should never happen: %v", err)
		}
		spec.Labels[preemptionLabelKey] = preemptionLabelValue
	}

	if vmPreemptionType == preemption.Spot {
		spec.Spot = true
	} else if vmPreemptionType == preemption.LegacyPreemptible {
		spec.Preemptible = true
	}
	if value, found := systemLabels[gkelabels.ProvisioningLabel]; found {
		spec.Labels[gkelabels.ProvisioningLabel] = value
	}
	return nil
}

// ExtendedDurationGenerator is a extended duration option spec Generator.
// Details in go/extended-duration-pod-design.
type ExtendedDurationGenerator struct {
	cloudProvider napcloudprovider.AutoprovisioningCloudProvider
}

func NewExtendedDurationPodGenerator(cloudProvider napcloudprovider.AutoprovisioningCloudProvider) *ExtendedDurationGenerator {
	return &ExtendedDurationGenerator{
		cloudProvider: cloudProvider,
	}
}

func (edpg ExtendedDurationGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, pReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	edpValue := extendeddurationpods.GetExtendedDurationValueFromReq(pReq)
	if edpValue == "" {
		return nil
	}

	if !edpg.cloudProvider.IsAutopilotEnabled() {
		return NewExtendedDurationPodNonAutopilotError()
	}
	if edpValue != ekvmtypes.ExtendedDurationLabelX {
		if _, err := resource.ParseQuantity(edpValue); err != nil {
			return NewInvalidExtendedDurationPodCPUReq(edpValue)
		}
	}

	ngReq.extendedDurationPodCPUReq = edpValue
	return nil
}

func (edpg ExtendedDurationGenerator) ValidateRequirements(_ *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

// GenerateNodeGroupOptionsForRequirements TODO(b/279938116): we should be validating that preemption isn't enabled with ExtendedDurationPods
func (edpg ExtendedDurationGenerator) GenerateNodeGroupOptionsForRequirements(options []NodeGroupOptions, requirements nodeGroupRequirements) []NodeGroupOptions {
	if requirements.extendedDurationPodCPUReq == "" {
		return options
	}
	var result []NodeGroupOptions
	for _, option := range options {
		if resolvedValue, ok := edpg.resolveExtendedDurationValue(option.MachineType, requirements.extendedDurationPodCPUReq); ok {
			result = append(result, option)
			edpOption := option
			edpOption.ExtendedDurationPodCPUReq = resolvedValue
			result = append(result, edpOption)
		}
	}

	return result
}

func (ExtendedDurationGenerator) UpdateParameters(params *nodeGroupParameters, _ nodeGroupRequirements, opt NodeGroupOptions) error {
	if opt.ExtendedDurationPodCPUReq != "" {
		params.systemLabels[gkelabels.ExtendedDurationPodsLabel] = opt.ExtendedDurationPodCPUReq
	}
	return nil
}

func (ExtendedDurationGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, _ map[string]resource.Quantity) error {
	if val, ok := systemLabels[gkelabels.ExtendedDurationPodsLabel]; ok {
		spec.Labels[gkelabels.ExtendedDurationPodsLabel] = val
		spec.ExtendedDurationPods = val
	}
	return nil
}

// resolveExtendedDurationValue determines the correct value for the extended duration pods label.
// It returns the resolved value and a boolean indicating if the combination was valid.
func (edpg ExtendedDurationGenerator) resolveExtendedDurationValue(machineType, value string) (string, bool) {
	machineFamily, err := edpg.cloudProvider.MachineConfigProvider().GetMachineFamilyFromMachineName(machineType)
	isEK := err == nil && machineFamily.Name() == machinetypes.EK.Name()

	if isEK && edpg.cloudProvider.IsEkEdpEnabled() {
		// The machine is EK and the feature is on. Always use "X".
		return ekvmtypes.ExtendedDurationLabelX, true
	}

	// For all other cases, the value "X" is an invalid combination.
	if value == ekvmtypes.ExtendedDurationLabelX {
		return "", false
	}

	// Otherwise, the value is valid.
	return value, true
}

// ReservationGenerator enables support for GCE reservations provisioning decisions.
type ReservationGenerator struct {
	reservationsPuller                    *gceclient.ReservationsPuller
	reservationBlocksPuller               *reservations.BlocksPuller
	specificTypeReservationMatchEnabled   bool
	specificTypeReservationsEnabled       bool
	reservationsAnyLocationPolicyOverride bool
	projectId                             string
	experimentsManager                    experiments.Manager
}

func NewReservationGenerator(reservationsPuller *gceclient.ReservationsPuller, flags ReservationFlags, projectId string, experimentsManager experiments.Manager, reservationBlocksPuller *reservations.BlocksPuller) *ReservationGenerator {
	return &ReservationGenerator{
		reservationsPuller:                    reservationsPuller,
		reservationBlocksPuller:               reservationBlocksPuller,
		specificTypeReservationMatchEnabled:   flags.SpecificTypeReservationMatchEnabled,
		specificTypeReservationsEnabled:       flags.SpecificTypeReservationsEnabled,
		reservationsAnyLocationPolicyOverride: flags.ReservationsAnyLocationPolicyOverride,
		projectId:                             projectId,
		experimentsManager:                    experimentsManager,
	}
}

func (rg ReservationGenerator) GenerateNodeGroupOptionsForRequirements(options []NodeGroupOptions, requirements nodeGroupRequirements) []NodeGroupOptions {
	if requirements.reservation.name == "" {
		return options
	}

	if !requirements.reservation.exists {
		klog.Errorf("No matching usable reservation found for '%s' in project '%s' (empty means local project). Cannot generate node groups that match reserved machines.", requirements.reservation.name, requirements.reservation.project)
		return []NodeGroupOptions{}
	}

	result := []NodeGroupOptions{}
	for _, option := range options {
		// Not all reservations types specify machineType & zone. Filter  only when a specificaion is found.
		if requirements.reservation.machineType != "" && requirements.reservation.machineType != option.MachineType {
			continue
		}
		if requirements.reservation.zone != "" && requirements.reservation.zone != option.Zone {
			continue
		}
		result = append(result, option)
	}
	return result
}

func (rg ReservationGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, pReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	if v, ok := pReq.LabelReq.GetSingleValue(gkelabels.ReservationNameLabel); ok {
		ngReq.reservation.name = v
	}
	if v, ok := pReq.LabelReq.GetSingleValue(gkelabels.ReservationProjectLabel); ok {
		ngReq.reservation.project = v
	}
	if v, ok := pReq.LabelReq.GetSingleValue(gkelabels.ReservationAffinityLabel); ok {
		ngReq.reservation.affinity = v
	}
	if v, ok := pReq.LabelReq.GetSingleValue(gkelabels.ReservationBlocksLabel); ok {
		ngReq.reservation.block = v
	}
	subBlocksEnabled := rg.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ReservationSubblocksTargetingEnabledFlag, false)
	if v, ok := pReq.LabelReq.GetSingleValue(gkelabels.ReservationSubBlocksLabel); subBlocksEnabled && ok {
		ngReq.reservation.subBlock = v
	}
	// If no explicit matching to a specified reservation is needed, assume a reservation exists & matches.
	ra, ok := reservations.GkeAffinityFromSelectorValue(ngReq.reservation.affinity)
	nonSpecificAffinity := ok && ra != gkeclient.ReservationAffinitySpecific
	if nonSpecificAffinity {
		ngReq.reservation.exists = true
		return nil
	}
	// For match disabled, specific affinity and shared reservation, assume a reservation exists & matches.
	// For local reservation we try to steer all the relevant information.
	if !rg.specificTypeReservationMatchEnabled && ngReq.reservation.project != "" && ngReq.reservation.project != rg.projectId {
		ngReq.reservation.exists = true
		return nil
	}

	if ngReq.reservation.name == "" {
		return nil
	}

	// defaulting to local project if reservation specified
	if ngReq.reservation.project == "" {
		ngReq.reservation.project = rg.projectId
	}

	// Shared reservation logic
	if ngReq.reservation.project != rg.projectId {
		rg.reservationsPuller.AddProject(ngReq.reservation.project)
		// If a failure occurred while fetching from shared project, and it is an aggregate reservation, assume an aggregate reservation exists and matches
		if err := rg.reservationsPuller.LastLoopErrorInProject(ngReq.reservation.project); err != nil && canUseAggregateReservation(pReq, ngReq.tpuRequest) {
			ngReq.reservation.exists = true
			return nil
		}
	}

	ref := gceclient.ReservationRef{
		Name:         ngReq.reservation.name,
		Project:      ngReq.reservation.project,
		BlockName:    ngReq.reservation.block,
		SubBlockName: ngReq.reservation.subBlock,
	}

	for _, r := range rg.reservationsPuller.GetReservationsInProject(ngReq.reservation.project) {
		// Skip any reservations that do not match the configured reservation
		if r.Name != ngReq.reservation.name {
			continue
		}

		if r.Zone != "" {
			ngReq.reservation.zone = gceclient.GetReservationZone(r)
		}

		// Skip any reservations that do not match zone constraints.
		if len(ngReq.specifiedZones) != 0 {
			if !slices.Contains(ngReq.specifiedZones, ngReq.reservation.zone) {
				klog.Infof("Ignoring reservation '%s' outside of preferred zones (%v).", r.Name, ngReq.specifiedZones)
				continue
			}
		}

		ngReq.reservation.exists = true
		// Validate reservation
		if !r.SpecificReservationRequired {
			return reservations.NewErrUnusableReservation(ref, "SpecificReservationRequired is not enabled so reservation cannot be specifically targeted for consumption")
		}

		// Match reservation block only if specified
		if ngReq.reservation.block != "" {
			if err := rg.matchReservationBlock(&ngReq.reservation); err != nil {
				return err
			}
		}
		// For TPU reservations we return earlier
		if reservations.IsAggregateReservation(r) {
			// Aggregate reservations are not usable without TPU request specified
			// or accelerator count and type present in pod labels (e.g. for balloon pods).
			if !canUseAggregateReservation(pReq, ngReq.tpuRequest) {
				return reservations.NewErrUnusableReservation(ref, "Unable to consume aggregate reservation for non-TPU workloads")
			}
			return nil
		}

		// Match reservation settings only if specified
		if r.SpecificReservation != nil &&
			r.SpecificReservation.InstanceProperties != nil &&
			r.SpecificReservation.InstanceProperties.MachineType != "" {
			ngReq.reservation.machineType = r.SpecificReservation.InstanceProperties.MachineType
		}

		// Steer for Local SSD Count
		steerFlagEnabled := rg.experimentsManager != nil &&
			(rg.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.SliceOfHardwareReservationSteerLocalSSDFlag, false) ||
				rg.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.SliceOfHardwareReservationSteerLocalSSD2Flag, false))
		if steerFlagEnabled &&
			r.SpecificReservation != nil &&
			r.SpecificReservation.InstanceProperties != nil &&
			len(r.SpecificReservation.InstanceProperties.LocalSsds) > 0 {
			nvmeCount := 0
			for _, ssd := range r.SpecificReservation.InstanceProperties.LocalSsds {
				if ssd.Interface == "NVME" {
					nvmeCount += 1
				}
			}
			ngReq.reservation.totalLSSDCount = nvmeCount
		}
		// Infer placement policy
		pgSpec, err := rg.inferPlacementPolicyFromReservation(r, ngReq)
		if err != nil {
			return err
		}
		if pgSpec != nil {
			pgSpecFromLabels := placementGroupSpec(ngReq, pReq.LabelReq)
			if pgSpecFromLabels.Policy != "" && pgSpecFromLabels.Policy != pgSpec.Policy {
				return reservations.NewErrUnusableReservation(ref,
					fmt.Sprintf("Unable to consume specific reservation with placement policy when conflicting placement policy (%v) is provided via node selectors.", pgSpecFromLabels.Policy))
			}
			if pgSpecFromLabels.GroupId != "" {
				klog.V(5).Infof("Adding groupId '%s' from label requirements to placement policy inferred from reservation '%v'", pgSpecFromLabels.GroupId, r.Name)
				pgSpec.GroupId = pgSpecFromLabels.GroupId
			}
			ngReq.placementGroup = *pgSpec
		}
		return nil
	}
	// For match disabled, if no reservation was found, assume a reservation exists & matches.
	// TODO(b/405036075): check if this condition is needed, if no local reservation then no reservation should be available
	if !rg.specificTypeReservationMatchEnabled {
		ngReq.reservation.exists = true
		return nil
	}
	return reservations.NewErrUnusableReservation(ref, "Specified reservation either does not exist or has no ready capacity to consume")
}

func (rg ReservationGenerator) inferPlacementPolicyFromReservation(r *gce_api.Reservation, ngReq *nodeGroupRequirements) (*placement.Spec, caerrors.AutoscalerError) {
	// Infer placement policy only from specific reservation.
	if r.SpecificReservation == nil || !r.SpecificReservationRequired {
		klog.Infof("Won't infer placement policy from reservation '%s' without requirement for or without specific reservation", r.Name)
		return nil, nil
	}
	policySpec, err := placement.FromReservationResourcePolices(r.ResourcePolicies)
	ref := gceclient.ReservationRef{
		Name:         ngReq.reservation.name,
		Project:      ngReq.reservation.project,
		BlockName:    ngReq.reservation.block,
		SubBlockName: ngReq.reservation.subBlock,
	}
	if err != nil {
		return nil, reservations.NewErrUnusableReservation(ref, fmt.Sprintf("Reservation %s has %s", r.Name, err.Error()))
	}
	if policySpec.Policy == "" {
		return nil, nil
	}
	klog.V(5).Infof("ReservationGenerator reservation '%s' has placement policy '%s'", r.Name, policySpec.Policy)
	// GKE does not yet support using placement policy from another project using a reservation.
	if ngReq.reservation.project != rg.projectId {
		return nil, reservations.NewErrUnusableReservation(ref, "Shared reservations with placement policy are not supported")
	}
	return &policySpec, nil
}

func (rg ReservationGenerator) ValidateRequirements(r *nodeGroupRequirements) caerrors.AutoscalerError {
	// TPUs are assumed to be using aggregate reservations
	aggregateReservation := r.machineSpec.TpuType != ""
	ref := gceclient.ReservationRef{
		Name:         r.reservation.name,
		Project:      r.reservation.project,
		BlockName:    r.reservation.block,
		SubBlockName: r.reservation.subBlock,
	}
	// The user is also not using aggregate type reservation & specific type reservations are not enabled.
	// There are no other types of reservations.
	if !rg.specificTypeReservationsEnabled && !aggregateReservation && r.reservation.name != "" {
		// Error to end user does not leak why this combination is not supported as it is an implementation detail
		return reservations.NewErrUnusableReservation(ref, "Specifying reservations without TPUs are not supported")
	}
	if r.reservation.block != "" && r.reservation.name == "" {
		// Error to end user, if reservation block name is present then the reservation name is required
		return reservations.NewErrUnusableReservation(ref, "Specifying reservation block without reservation name")
	}

	if r.reservation.subBlock != "" && r.reservation.block == "" {
		return reservations.NewErrUnusableReservation(ref, "Specifying reservation subblock without reservation block")
	}

	gkeReservationAffinity, ok := reservations.GkeAffinityFromSelectorValue(r.reservation.affinity)
	if r.reservation.affinity != "" && !ok {
		return reservations.NewUnsupportedReservationAffinityError(
			r.reservation.affinity,
			fmt.Sprintf("must be one of the supported values of %v or not set", reservations.SupportedReservationAffinitySelectorValues()))
	}
	// TODO(b/324839410): Update this logic to support reservations
	if r.queuedProvisioningReq.Enabled && ok && gkeReservationAffinity != gkeclient.ReservationAffinityNone {
		return reservations.NewUnsupportedReservationAffinityError(
			r.reservation.affinity,
			"Provisioning Requests don't support reservations")
	}
	if r.flexStartReq.enabled && ok && gkeReservationAffinity != gkeclient.ReservationAffinityNone {
		return reservations.NewUnsupportedReservationAffinityError(
			r.reservation.affinity,
			"Flex Start Non-Queued doesn't support reservations")
	}

	if r.reservation.name != "" {
		if gkeReservationAffinity != gkeclient.ReservationAffinitySpecific && gkeReservationAffinity != "" {
			return reservations.NewUnsupportedReservationAffinityError(
				r.reservation.affinity,
				"unsupported to specify a reservation and not specify specific reservation affinity")
		}
	} else {
		if gkeReservationAffinity == gkeclient.ReservationAffinitySpecific {
			return reservations.NewUnsupportedReservationAffinityError(
				r.reservation.affinity,
				"unsupported to both specify no reservation and specific reservation affinity")
		}
		if aggregateReservation && gkeReservationAffinity == gkeclient.ReservationAffinityAny {
			// Error to end user, reservation affinity any cannot be used with tpu request, for single and multi host topology
			return reservations.NewUnsupportedReservationAffinityError(r.reservation.affinity, "reservation affinity any is not supported with TPUs")
		}
	}
	return nil
}

func (rg ReservationGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if ngReq.reservation.name == "" && ngReq.reservation.affinity == "" {
		return nil
	}

	if !ngReq.reservation.exists {
		klog.Warningf("Preventing unschedulable pods from simulating as schedulable by not setting reservation parameters. Nodepools tied to missing reservation '%s' in '%s' with affinity '%s' cannot scale up.", ngReq.reservation.name, ngReq.reservation.project, ngReq.reservation.affinity)
		return nil
	}

	if ngReq.reservation.name != "" {
		params.systemLabels[gkelabels.ReservationNameLabel] = ngReq.reservation.name
	}
	if ngReq.reservation.project != "" {
		params.systemLabels[gkelabels.ReservationProjectLabel] = ngReq.reservation.project
	}
	if ngReq.reservation.zone != "" {
		params.systemLabels[gkelabels.ReservationZoneLabel] = ngReq.reservation.zone
	}
	if ngReq.reservation.affinity != "" {
		params.systemLabels[gkelabels.ReservationAffinityLabel] = ngReq.reservation.affinity
	}
	if ngReq.reservation.totalLSSDCount > 0 {
		// Do nothing; local SSD labels are handled in LocalSSDConfigGenerator.UpdateParameters
	}
	if ngReq.reservation.block != "" {
		params.systemLabels[gkelabels.ReservationBlocksLabel] = ngReq.reservation.block
		params.systemLabels[gkelabels.ReservationBlocksCountLabel] = fmt.Sprintf("%d", ngReq.reservation.blockCount)
	}
	if rg.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ReservationSubblocksTargetingEnabledFlag, false) && ngReq.reservation.subBlock != "" {
		params.systemLabels[gkelabels.ReservationSubBlocksLabel] = ngReq.reservation.subBlock
		params.systemLabels[gkelabels.ReservationSubBlocksCountLabel] = fmt.Sprintf("%d", ngReq.reservation.subBlockCount)
	}
	return nil
}

// TODO(b/517093792): add error handling to UpdateNodePoolSpec once ReservationGenerator becomes compliant
// with PodSharder. Enabling such behavior before it would result in sharder malfunctioning or
// bloating logs with error messages in cases where any affinity is being used, no reservation
// specified or ComputeClass being the source of the injected labels.
func (rg ReservationGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, _ map[string]resource.Quantity) error {
	name := systemLabels[gkelabels.ReservationNameLabel]
	project := systemLabels[gkelabels.ReservationProjectLabel]
	reservationZone := systemLabels[gkelabels.ReservationZoneLabel]
	reservationBlock := systemLabels[gkelabels.ReservationBlocksLabel]
	reservationBlockCount := systemLabels[gkelabels.ReservationBlocksCountLabel]
	reservationSubBlock := systemLabels[gkelabels.ReservationSubBlocksLabel]
	reservationSubBlockCount := systemLabels[gkelabels.ReservationSubBlocksCountLabel]
	labeledReservationAffinity := systemLabels[gkelabels.ReservationAffinityLabel]

	if _, isQueuedProvisioning := systemLabels[gkelabels.ProvisioningRequestLabelKey]; isQueuedProvisioning {
		spec.ReservationAffinity = &gke_api_beta.ReservationAffinity{ConsumeReservationType: gkeclient.ReservationAffinityNone}
		return nil
	}
	if _, isFlexStart := systemLabels[gkelabels.FlexStartLabel]; isFlexStart {
		spec.ReservationAffinity = &gke_api_beta.ReservationAffinity{ConsumeReservationType: gkeclient.ReservationAffinityNone}
		return nil
	}

	gkeReservationAffinity := gkeclient.ReservationAffinitySpecific
	if labeledReservationAffinity != "" {
		gkeReservationAffinity, _ = reservations.GkeAffinityFromSelectorValue(labeledReservationAffinity)
	}
	subBlocksEnabled := rg.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ReservationSubblocksTargetingEnabledFlag, false)
	if !subBlocksEnabled {
		reservationSubBlock = ""
	}
	path := gceclient.ReservationRef{
		Name:         name,
		Project:      project,
		BlockName:    reservationBlock,
		SubBlockName: reservationSubBlock,
	}
	if affinity, err := reservations.NewNodepoolReservationAffinity(path.RelativePath(rg.projectId), gkeReservationAffinity); err == nil {
		spec.ReservationAffinity = affinity
	}

	hasSpecificReservation := gkeReservationAffinity == gkeclient.ReservationAffinitySpecific && name != ""

	if hasSpecificReservation {
		spec.Labels[gkelabels.ReservationNameLabel] = name
		if project == "" {
			spec.Labels[gkelabels.ReservationProjectLabel] = rg.projectId
		} else {
			spec.Labels[gkelabels.ReservationProjectLabel] = project
		}
		if reservationZone != "" {
			spec.Labels[gkelabels.ReservationZoneLabel] = reservationZone
		}
		if reservationBlock != "" {
			spec.Labels[gkelabels.ReservationBlocksLabel] = reservationBlock
			if count, err := strconv.ParseInt(reservationBlockCount, 10, 64); err == nil {
				spec.ReservationBlockCount = count
			}
		}
		if reservationSubBlock != "" {
			spec.Labels[gkelabels.ReservationSubBlocksLabel] = reservationSubBlock
			if count, err := strconv.ParseInt(reservationSubBlockCount, 10, 64); err == nil {
				spec.ReservationSubBlockCount = count
			}
		}
	}

	if rg.reservationsAnyLocationPolicyOverride && (hasSpecificReservation || gkeReservationAffinity == gkeclient.ReservationAffinityAny) {
		spec.LocationPolicy = LocationPolicyAny
	}

	if spec.ReservationAffinity != nil && labeledReservationAffinity != "" {
		spec.Labels[gkelabels.ReservationAffinityLabel] = labeledReservationAffinity
	}

	return nil
}

// matchReservationBlock method validates the block specified by the customer.
// First if the block is present into the reservation. If present then sets the maximum size to match the actual block count.
func (rg ReservationGenerator) matchReservationBlock(req *reservationRequirements) caerrors.AutoscalerError {
	ref := gceclient.ReservationRef{
		Name:         req.name,
		Project:      req.project,
		BlockName:    req.block,
		SubBlockName: req.subBlock,
	}
	if req.name == "" {
		// Error to end user, if reservation block name is present then the reservation name is required
		return reservations.NewErrUnusableReservation(ref, "Specifying reservation block without reservation name")
	}
	if rg.reservationBlocksPuller == nil {
		// If the puller is disabled the validation is skipped, so the customer can still use the reservation block
		return nil
	}
	// Don't use block and subblock name when pulling reservation blocks, we want to pull all blocks to cache them
	key := gceclient.ReservationRef{Project: req.project, Name: req.name, Zone: req.zone}
	reservationBlocks := rg.reservationBlocksPuller.GetReservationBlocksInReservation(key)
	for _, reservationBlock := range reservationBlocks {
		if req.block == reservationBlock.Name {
			req.blockCount = reservationBlock.Count
			subBlocksEnabled := rg.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ReservationSubblocksTargetingEnabledFlag, false)
			if subBlocksEnabled && req.subBlock != "" {
				for _, subBlock := range reservationBlock.SubBlocks {
					if req.subBlock == subBlock.Name {
						req.subBlockCount = subBlock.Count
						return nil
					}
				}
				return reservations.NewErrUnusableReservation(ref, fmt.Sprintf("reservation sub-block '%s' not found", req.subBlock))
			}
			return nil
		}
	}
	return reservations.NewErrUnusableReservation(ref, fmt.Sprintf("Reservation block '%s' not found", req.block))
}

// requiresAggregateReservation returns if the request info means an aggregate
// reservation is needed.
func requiresAggregateReservation(tpuReq TpuRequest) bool {
	// TPU usage requires aggregate reservations
	return !tpuReq.Empty()
}

// canUseAggregateReservation returns true if the pod requirements or TPU request
// implies that an aggregate reservation is usable.
func canUseAggregateReservation(pReq *podrequirements.Requirements, tpuReq TpuRequest) bool {
	if requiresAggregateReservation(tpuReq) {
		return true
	}
	accCount, hasAccCount := pReq.LabelReq.GetSingleValue(gkelabels.AcceleratorCountLabel)
	tpuType, hasTpuType := pReq.LabelReq.GetSingleValue(gkelabels.TPULabel)
	return hasAccCount && hasTpuType && accCount != "" && tpuType != ""
}

// LocalSSDConfigGenerator enables Local SSD support in NAP.
// go/ap-localssd and go/ap-sohw-localssd-selection.
type LocalSSDConfigGenerator struct {
	cloudProvider napcloudprovider.AutoprovisioningCloudProvider
}

func NewLocalSSDConfigGenerator(cloudProvider napcloudprovider.AutoprovisioningCloudProvider) *LocalSSDConfigGenerator {
	return &LocalSSDConfigGenerator{
		cloudProvider: cloudProvider,
	}
}

func (g LocalSSDConfigGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, pReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	if v, ok := pReq.LabelReq.GetSingleValue(gkelabels.EphemeralLocalSsdLabel); ok && v == gkelabels.EphemeralLocalSsdEnabledValue {
		ngReq.explicitlyRequiresLocalSSD = true
	}
	if ngReq.reservation.totalLSSDCount > 0 {
		if ngReq.reservation.totalLSSDCount < ngReq.totalLSSDCount {
			return caerrors.NewAutoscalerErrorf(caerrors.ConfigurationError, "reservation only have %d local SSDs and node group requested %d local SSDs", ngReq.reservation.totalLSSDCount, ngReq.totalLSSDCount)
		}
		ngReq.totalLSSDCount = ngReq.reservation.totalLSSDCount
		ngReq.ephemeralStorageLocalSSDCount = defaultEphemeralStorageLSSDCount(*ngReq, ngReq.totalLSSDCount)
	}

	return nil
}

func (g LocalSSDConfigGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, opt NodeGroupOptions) error {
	// Handle automatically attached Local SSD disks (for eligible supported machine types).
	if allowedLSSDCount, found, err := g.cloudProvider.MachineConfigProvider().AutomaticEphemeralLocalSsdCountByMachineType(opt.MachineType); found && err == nil {
		if int64(ngReq.totalLSSDCount) > allowedLSSDCount {
			return NewInvalidLocalSSDCountForMachineTypeError(opt.MachineType, ngReq.totalLSSDCount, []int{int(allowedLSSDCount)})
		}
		ephemeralStorageLocalSSDCount := defaultEphemeralStorageLSSDCount(ngReq, int(allowedLSSDCount))
		if ephemeralStorageLocalSSDCount > 0 {
			params.systemLabels[labelEphemeralLocalSsdDisksCount] = fmt.Sprintf("%d", ephemeralStorageLocalSSDCount)
		}
	} else if err != nil {
		return err
	}
	// Add Local SSD count requirement from reservation config to systemLabels so it will propagate to NPSpec.
	// Assign to ngReq for disk count validation below.
	if count := ngReq.reservation.totalLSSDCount; count > 0 {
		if count < ngReq.totalLSSDCount {
			return NewInvalidLocalSSDCountForReservationError(ngReq.reservation.name, count, ngReq.totalLSSDCount)
		}
		ngReq.totalLSSDCount = count
		ngReq.ephemeralStorageLocalSSDCount = defaultEphemeralStorageLSSDCount(ngReq, ngReq.totalLSSDCount)
		if ngReq.ephemeralStorageLocalSSDCount > 0 {
			params.systemLabels[labelEphemeralLocalSsdDisksCount] = fmt.Sprintf("%d", ngReq.ephemeralStorageLocalSSDCount)
		}
	}
	// Handle remaining cases.
	if ngReq.explicitlyRequiresLocalSSD {
		params.systemLabels[gkelabels.EphemeralLocalSsdLabel] = gkelabels.EphemeralLocalSsdEnabledValue
		// Ensure at least one local SSD is used as ephemeral storage.
		if ngReq.ephemeralStorageLocalSSDCount == 0 {
			ngReq.ephemeralStorageLocalSSDCount = 1
			ngReq.totalLSSDCount += 1
		}
		// variable SSD counts is not supported if pod explicitly request local SSDs using label.
		// refer: Milestone 1 (go/ap-localssd).
		if !g.isMachineTypeSupported(opt.MachineType, false) {
			return NewLocalSSDNotSupportedForMachineTypeError(opt.MachineType)
		}
		allowedLSSDCount, err := g.chooseLocalSSDCount(opt.MachineType)
		if err != nil {
			return caerrors.NewAutoscalerErrorf(caerrors.InternalError, "Could not determine Local SSD count for machine type: %s", opt.MachineType)
		}
		if allowedLSSDCount < ngReq.totalLSSDCount {
			return NewInvalidLocalSSDCountForMachineTypeError(opt.MachineType, ngReq.totalLSSDCount, []int{allowedLSSDCount})
		}
		ephemeralStorageLocalSSDCount := defaultEphemeralStorageLSSDCount(ngReq, int(allowedLSSDCount))
		if ephemeralStorageLocalSSDCount > 0 {
			params.systemLabels[labelEphemeralLocalSsdDisksCount] = fmt.Sprintf("%d", ephemeralStorageLocalSSDCount)
		}
	} else if ngReq.totalLSSDCount > 0 {
		// ngReq.localSSDCount is currently only set by ComputeClassGenerator.
		// variable SSD counts is supported by ComputeClass.
		if !g.isMachineTypeSupported(opt.MachineType, true) {
			return NewLocalSSDNotSupportedForMachineTypeError(opt.MachineType)
		}
		isValid, allowedCounts, err := g.validLocalSSDCount(ngReq.totalLSSDCount, opt.MachineType)
		if err != nil {
			return caerrors.NewAutoscalerErrorf(caerrors.InternalError, "error validating local SSD count: %d for machine type: %s, err: %s", ngReq.totalLSSDCount, opt.MachineType, err.Error())
		}
		if !isValid {
			return NewInvalidLocalSSDCountForMachineTypeError(opt.MachineType, ngReq.totalLSSDCount, allowedCounts)
		}
		ephemeralStorageLocalSSDCount := defaultEphemeralStorageLSSDCount(ngReq, ngReq.totalLSSDCount)
		if ephemeralStorageLocalSSDCount > 0 {
			params.systemLabels[labelEphemeralLocalSsdDisksCount] = fmt.Sprintf("%d", ngReq.ephemeralStorageLocalSSDCount)
		}
	}
	return nil
}

func (g LocalSSDConfigGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, _ map[string]resource.Quantity) error {
	if val, ok := systemLabels[labelEphemeralLocalSsdDisksCount]; ok {
		count, err := strconv.Atoi(val)
		if err != nil {
			return caerrors.NewAutoscalerErrorf(caerrors.InternalError, "invalid Local SSD count passed: %s", val)
		}
		if count == 0 {
			return nil
		}
		if spec.LocalSSDConfig == nil {
			spec.LocalSSDConfig = &gkeclient.LocalSSDConfig{}
		}
		if spec.LocalSSDConfig.EphemeralStorageConfig == nil {
			spec.LocalSSDConfig.EphemeralStorageConfig = &gke_api_beta.EphemeralStorageConfig{}
		}
		spec.LocalSSDConfig.EphemeralStorageConfig.LocalSsdCount = int64(count)
		// Only add taints for Autopilot if ComputeClass is not enabled.
		if _, exists := systemLabels[labelComputeClassRequired]; !exists && g.cloudProvider.IsAutopilotEnabled() {
			if spec.Labels == nil {
				spec.Labels = make(map[string]string)
			}
			spec.Labels[gkelabels.EphemeralLocalSsdLabel] = gkelabels.EphemeralLocalSsdEnabledValue
			// Add a taint only if the Local SSD was explicitly requested.
			if _, ok := systemLabels[gkelabels.EphemeralLocalSsdLabel]; ok {
				taint := apiv1.Taint{
					Effect: apiv1.TaintEffectNoSchedule,
					Key:    gkelabels.EphemeralLocalSsdLabel,
					Value:  gkelabels.EphemeralLocalSsdEnabledValue,
				}
				spec.Taints = append(spec.Taints, taint)
			}
		}
	}
	return nil
}

func (g LocalSSDConfigGenerator) ValidateRequirements(_ *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (g LocalSSDConfigGenerator) chooseLocalSSDCount(machineType string) (int, error) {
	counts, ok, err := g.cloudProvider.MachineConfigProvider().AllowedEphemeralLocalSsdCountByMachineType(machineType)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("allowedEphemeralLocalSsdCount not defined for machineType: %s", machineType)
	}
	// Use first (and only) allowed value for Milestone 1 (go/ap-localssd).
	return counts[0], nil
}

func (g LocalSSDConfigGenerator) validLocalSSDCount(count int, machineType string) (bool, []int, error) {
	counts, ok, err := g.cloudProvider.MachineConfigProvider().AllowedEphemeralLocalSsdCountByMachineType(machineType)
	if err != nil {
		return false, nil, err
	}
	if !ok {
		return false, counts, nil
	}
	for _, c := range counts {
		if c == count {
			return true, counts, nil
		}
	}
	return false, counts, nil
}

func (g LocalSSDConfigGenerator) isMachineTypeSupported(machineType string, variableSSDCountSupported bool) bool {
	// Machines with automatically attached Local SSD disk counts are supported.
	if _, found, err := g.cloudProvider.MachineConfigProvider().AutomaticEphemeralLocalSsdCountByMachineType(machineType); found && err == nil {
		return true
	}
	// Machines with defined allowed Local SSD disk counts are supported.
	if counts, found, err := g.cloudProvider.MachineConfigProvider().AllowedEphemeralLocalSsdCountByMachineType(machineType); found && err == nil {
		// This is a temporary flag. With ComputeClasses we support all machine types which support variable SSD counts.
		// Previously, we only supported machine types which support single SSD count.
		// Refer: Milestone 1 (go/ap-localssd).
		if !variableSSDCountSupported {
			return len(counts) == 1
		}
		return true
	}
	return false
}

// MultiNetworkingGenerator enables support for multi networking resources in NAP.
// More details in go/gke-nap-support-multi-net.
type MultiNetworkingGenerator struct {
	matcher networking.Matcher
}

func NewMultiNetworkingGenerator(matcher networking.Matcher) *MultiNetworkingGenerator {
	return &MultiNetworkingGenerator{matcher: matcher}
}

func (m *MultiNetworkingGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	ngReq.networkReq = podReq.NetworkingReq
	ngReq.networkAnnotation = podReq.NetworkingAnnotation
	return nil
}

func (m *MultiNetworkingGenerator) ValidateRequirements(_ *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (m *MultiNetworkingGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if len(ngReq.networkReq.AdditionalNetworkResources) == 0 {
		return nil
	}
	if params.extraResources == nil {
		params.extraResources = make(map[string]resource.Quantity)
	}
	params.systemLabels[netapi.InterfaceAnnotationKey] = ngReq.networkAnnotation
	for _, network := range ngReq.networkReq.AdditionalNetworkResources {
		if networkingutils.IsHighPerformanceNetworkResource(network) {
			// high performance network.
			params.extraResources[network] = *networkingutils.HighPerformanceDefaultResourceValue
			// Additionally simulate extra node label when there is a high performance network present.
			params.systemLabels[gkelabels.HighPerformanceNetworkLabel] = gkelabels.HighPerformanceNetworkValue
		} else {
			params.extraResources[network] = *resource.NewQuantity(networkingutils.DefaultAdditionalMultiNetworkConfigMaxPodsPerNode, resource.DecimalSI)
		}
	}
	return nil
}

func (m *MultiNetworkingGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, extraResources map[string]resource.Quantity) error {
	networkingExtraResource := make(map[string]resource.Quantity)
	for resourceName, quantity := range extraResources {
		if networkingutils.IsNetworkResource(resourceName) {
			networkingExtraResource[resourceName] = quantity
		}
	}
	if len(networkingExtraResource) == 0 {
		return nil
	}
	networks, err := m.matcher.GetNetworkConfigFromResources(extraResources, systemLabels[netapi.InterfaceAnnotationKey])
	if err != nil {
		return err
	}
	spec.NetworkConfigs = networks
	if highPerformanceNetworkVal, ok := systemLabels[gkelabels.HighPerformanceNetworkLabel]; ok {
		if spec.Labels == nil {
			spec.Labels = make(map[string]string)
		}
		spec.Labels[gkelabels.HighPerformanceNetworkLabel] = highPerformanceNetworkVal
	}
	return nil
}

// SystemLabelsGenerator is a generator responsible for injecting subset of
// system labels (if they were previously allowlisted). Labels passed here
// should have appropriate code in Cluster Server translating labels into spec.
// More details in go/accelerate-nap-features. This generator stores compiled
// regexp for efficiency.
type SystemLabelsGenerator struct {
	matcher *gkelabels.Matcher
}

// NewSystemLabelsGenerator returns a new instance of SystemLabelsGenerator.
func NewSystemLabelsGenerator(matcher *gkelabels.Matcher) *SystemLabelsGenerator {
	return &SystemLabelsGenerator{
		matcher: matcher,
	}
}

func (s *SystemLabelsGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	if ngReq.systemLabels == nil {
		ngReq.systemLabels = make(map[string]string)
	}

	for key, val := range podReq.LabelReq.GetAllKeyValueMatches(s.matcher) {
		ngReq.systemLabels[key] = val
	}

	if ngReq.computeClass != nil {
		for key, val := range ngReq.computeClass.UserDefinedLabels() {
			if _, found := ngReq.systemLabels[key]; found {
				klog.Warningf("NAP: unexpected override of existing labels with compute class node labels: key %q", key)
			}
			ngReq.systemLabels[key] = val
		}
	}
	if ngReq.computeClassRule != nil {
		for key, val := range ngReq.computeClassRule.UserDefinedLabels() {
			if _, found := ngReq.systemLabels[key]; found {
				klog.Warningf("NAP: unexpected override of existing labels with priority node labels: key %q", key)
			}
			ngReq.systemLabels[key] = val
		}
	}
	return nil
}

func (s *SystemLabelsGenerator) ValidateRequirements(_ *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (s *SystemLabelsGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	for key, val := range ngReq.systemLabels {
		// it's possible that the value has been already set by the workload
		// separation generator - if so, we skip overriding the value.
		if _, found := params.labels[key]; !found {
			params.labels[key] = val
		}
	}
	return nil
}

func (s *SystemLabelsGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, _ map[string]string, _ map[string]resource.Quantity) error {
	return nil
}

// ResourceLabelsGenerator is a generator responsible for passing resource
// labels from the pod requirements into the injected node group
// More details in go/ap-vertex-billing-internal.
type ResourceLabelsGenerator struct{}

func NewResourceLabelsGenerator() *ResourceLabelsGenerator {
	return &ResourceLabelsGenerator{}
}

func (s *ResourceLabelsGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	if ngReq.systemLabels == nil {
		ngReq.systemLabels = make(map[string]string)
	}

	for key := range podReq.LabelReq.KeysWithPrefix(gkelabels.ResourceLabelPrefix) {
		value, single := podReq.LabelReq.GetSingleValue(key)
		if !single {
			return caerrors.NewAutoscalerErrorf(caerrors.ConfigurationError, "malformed resource label '%s': too many values provided while 1 expected", key)
		}

		ngReq.systemLabels[key] = value
	}

	return nil
}

func (s *ResourceLabelsGenerator) ValidateRequirements(ngReq *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (s *ResourceLabelsGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	for key, value := range ngReq.systemLabels {
		if gkelabels.IsResourceLabel(key) {
			params.systemLabels[key] = value
		}
	}

	return nil
}

func (s *ResourceLabelsGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, _ map[string]resource.Quantity) error {
	if spec.ResourceLabels == nil {
		spec.ResourceLabels = make(map[string]string)
	}

	if spec.Labels == nil {
		spec.Labels = make(map[string]string)
	}

	for key, value := range systemLabels {
		if !gkelabels.IsResourceLabel(key) {
			continue
		}

		labelParts := strings.SplitN(value, ":", 2)
		if len(labelParts) != 2 {
			return caerrors.NewAutoscalerErrorf(
				caerrors.ConfigurationError,
				"malformed resource label '%s': invalid annotation value format, should be <key:value>, while '%v' found",
				key,
				value,
			)
		}

		spec.Labels[key] = ""
		spec.ResourceLabels[labelParts[0]] = labelParts[1]
	}

	return nil
}

// SelfServiceGenerator is responsible for passing self-service features
type SelfServiceGenerator struct{}

func NewSelfServiceGenerator() *SelfServiceGenerator {
	return &SelfServiceGenerator{}
}

func (*SelfServiceGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	// Add pod requirements self-service
	selfServiceMetadata := make(selfservice.Metadata)
	for k, v := range selfservice.LabelRequirementsMetadata(podReq.LabelReq) {
		selfServiceMetadata[k] = v
	}
	if len(selfServiceMetadata) > 0 {
		ngReq.selfServiceMetadata = selfServiceMetadata
	}
	return nil
}

func (*SelfServiceGenerator) ValidateRequirements(_ *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (*SelfServiceGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	// Add serialized self-service features
	selfServiceMetadata := make(selfservice.Metadata)
	for k, v := range ngReq.selfServiceMetadata {
		selfServiceMetadata[k] = v
	}
	if ngReq.computeClass != nil {
		for k, v := range ngReq.computeClass.SelfServiceMetadata() {
			selfServiceMetadata[k] = v
		}
	}
	if ngReq.computeClassRule != nil {
		for k, v := range ngReq.computeClassRule.SelfServiceMetadata() {
			selfServiceMetadata[k] = v
		}
	}

	if len(selfServiceMetadata) == 0 {
		return nil
	}

	// TODO(go/gkecl/1159622): Remove the marshaling once the NewNodeGroup refactor is merged
	metadataBytes, err := json.Marshal(selfServiceMetadata)
	if err != nil {
		return err
	}
	metadataString := string(metadataBytes)
	params.systemLabels[gkelabels.SelfServiceLabelKey] = metadataString
	return nil
}

func (*SelfServiceGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, _ map[string]resource.Quantity) error {
	if metadataString, exist := systemLabels[gkelabels.SelfServiceLabelKey]; exist {
		// TODO(go/gkecl/1159622): Remove the unmarshaling once the NewNodeGroup refactor is merged
		metadataBytes := []byte(metadataString)
		var metadata selfservice.Metadata
		err := json.Unmarshal(metadataBytes, &metadata)
		if err != nil {
			return err
		}

		spec.SelfServiceMetadata = metadata
		if spec.Labels == nil {
			spec.Labels = make(map[string]string)
		}
		selfservice.UpdateNodePoolLabels(spec.Labels, metadata)
	}
	return nil
}

// SpecifiedZonesGenerator is responsible for passing ComputeClass zonal preferences into the injected node group
type SpecifiedZonesGenerator struct {
	enableUserAnyZoneSelection bool
	cloudProvider              napcloudprovider.AutoprovisioningCloudProvider
	optionsTracker             *optstracking.OptionsTracker
}

func NewSpecifiedZonesGenerator(cloudProvider napcloudprovider.AutoprovisioningCloudProvider, enableUserAnyZoneSelection bool, optionsTracker *optstracking.OptionsTracker) *SpecifiedZonesGenerator {
	return &SpecifiedZonesGenerator{
		enableUserAnyZoneSelection: enableUserAnyZoneSelection,
		cloudProvider:              cloudProvider,
		optionsTracker:             optionsTracker,
	}
}

func (g *SpecifiedZonesGenerator) GenerateNodeGroupOptionsForRequirements(options []NodeGroupOptions, requirements nodeGroupRequirements) []NodeGroupOptions {
	if !g.enableUserAnyZoneSelection && len(requirements.specifiedZones) == 0 {
		return options
	}
	// Fallback to AutoprovisioningLocations.
	// When zones are not specified by the user, disregard all the options
	// which are not part of AutoprovisioningLocations.
	if len(requirements.specifiedZones) == 0 {
		autoprovisioningZonesMap := make(map[string]bool)
		for _, zone := range g.cloudProvider.GetAutoprovisioningLocations() {
			autoprovisioningZonesMap[zone] = true
		}

		var result []NodeGroupOptions
		for _, opt := range options {
			if _, found := autoprovisioningZonesMap[opt.Zone]; found {
				result = append(result, opt)
			}
		}
		return result
	}

	specifiedZonesMap := make(map[string]bool)
	for _, zone := range requirements.specifiedZones {
		specifiedZonesMap[zone] = true
	}

	var result []NodeGroupOptions
	for _, option := range options {
		// Disregard all the options which are not part of specifiedZones.
		if _, found := specifiedZonesMap[option.Zone]; found {
			result = append(result, option)
		}
	}
	return result
}

func (g *SpecifiedZonesGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	// Prioritize internally populated specified zones.
	// At this point, this information may come from:
	// ComputeClass - spec.priorities.reservations.specific.zones
	if len(ngReq.specifiedZones) != 0 {
		return nil
	}

	// Prioritize zonal preferences from ComputeClass - spec.priorites.location.zones if exists.
	if ngReq.computeClassRule != nil && len(ngReq.computeClassRule.Zones()) != 0 {
		ngReq.specifiedZones = ngReq.computeClassRule.Zones()
		return nil
	}

	// Prioritize zonal preferences from ComputeClass - spec.priorites.location.zoneTypes if exists.
	if ngReq.computeClassRule != nil && len(ngReq.computeClassRule.ZoneTypes()) != 0 && g.optionsTracker.Options().ZoneTypesEnabled {
		zones, err := ngReq.computeClassRule.GetZoneTypesZones()
		if err != nil {
			return err
		}
		ngReq.specifiedZones = zones
		ngReq.usesZoneTypes = true
		return nil
	}

	if g.enableUserAnyZoneSelection {
		// Prioritize zonal preferences (if outside of AP locations - user intent)
		// from Pod node selector or node affinity if exists.
		if vals, exists := podReq.LabelReq.GetValues(apiv1.LabelTopologyZone); exists {
			zoneOutsideOfAPLocations := false
			zonesMap := vals.Get()
			zonesList := []string{}
			for zone, ok := range zonesMap {
				if ok {
					found, err := g.isZoneOutsideOfAPLocations(zone)
					if err != nil {
						klog.Errorf("skipping requirement for zone %s, err: %v", zone, err)
						continue
					}
					if found {
						zoneOutsideOfAPLocations = true
					}
					zonesList = append(zonesList, zone)
				}
			}
			// If there is node selector / node affinity for zones and they
			// contain any zone outside of AP locations then node pool
			// should only be created from these zones.
			if zoneOutsideOfAPLocations {
				ngReq.specifiedZones = zonesList
				return nil
			}
		}
	}
	// TODO(b/424097638): Refactor GCE reservation zone selection.
	// SpecifiedZonesGenerator should be single source of truth.
	return nil
}

func (g *SpecifiedZonesGenerator) ValidateRequirements(ngReq *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (*SpecifiedZonesGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, opts NodeGroupOptions) error {
	if len(ngReq.specifiedZones) != 0 {
		zonesJsonBytes, err := json.Marshal(ngReq.specifiedZones)
		if err != nil {
			return err
		}
		zonesJsonString := string(zonesJsonBytes)
		params.systemLabels[gkelabels.SpecifiedZonesLabelKey] = zonesJsonString
		params.systemLabels[gkelabels.PickedZoneLabelKey] = opts.Zone
		if ngReq.usesZoneTypes {
			params.systemLabels[gkelabels.ZoneTypesLabelKey] = "true"
		}
	}
	return nil
}

func (g *SpecifiedZonesGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, _ map[string]resource.Quantity) error {
	if specifiedZonesJsonString, exist := systemLabels[gkelabels.SpecifiedZonesLabelKey]; exist {
		zonalPreferenceJsonBytes := []byte(specifiedZonesJsonString)
		var deserializedSpecifiedZones []string
		err := json.Unmarshal(zonalPreferenceJsonBytes, &deserializedSpecifiedZones)
		if err != nil {
			return err
		}

		// Remove duplicates from specified zones
		var sanitizedSpecifiedZones []string
		seen := make(map[string]bool)
		for _, zone := range deserializedSpecifiedZones {
			if _, duplicate := seen[zone]; !duplicate {
				seen[zone] = true
				sanitizedSpecifiedZones = append(sanitizedSpecifiedZones, zone)
			}
		}
		spec.Locations = sanitizedSpecifiedZones

		_, zoneTypesUsed := systemLabels[gkelabels.ZoneTypesLabelKey]
		// ZoneTypes by default span the whole region, e.g. STANDARD maps to all standard zones. We need
		// to trim that set to set of zones that support the config.
		if zoneTypesUsed && g.optionsTracker.Options().ZoneTypesEnabled {
			var acceleratorConfig *gke_api_beta.AcceleratorConfig
			if len(spec.Accelerators) > 0 {
				acceleratorConfig = spec.Accelerators[0]
			}
			spec.Locations = g.cloudProvider.TrimLocationsForMachineConfig(spec.Locations, spec.MachineType, acceleratorConfig, spec.MinCpuPlatform, spec.DiskType)
			if len(spec.Locations) == 0 {
				return caerrors.NewAutoscalerErrorf(caerrors.InternalError, "zoneTypes: there are no locations where the given machine config is available")
			}
			if isZonePresent(systemLabels[gkelabels.PickedZoneLabelKey], spec) {
				return caerrors.NewAutoscalerErrorf(caerrors.InternalError, "zoneTypes: picked zone does not have selected machine config available")
			}
		}

		// For TPU multi-host and compact node pools, we want to ensure they are single-zonal when ZoneTypes are used.
		if (spec.TpuMultiHost || spec.PlacementGroup.UsesPlacement()) && g.optionsTracker.Options().ZoneTypesEnabled && zoneTypesUsed {
			if pickedZone, exist := systemLabels[gkelabels.PickedZoneLabelKey]; exist {
				spec.Locations = []string{pickedZone}
			}
		}
	}
	return nil
}

func isZonePresent(zone string, spec *gkeclient.NodePoolSpec) bool {
	var baseZoneNotPresent bool
	locations := make(map[string]bool)
	for _, location := range spec.Locations {
		locations[location] = true
	}
	if _, ok := locations[zone]; !ok {
		baseZoneNotPresent = true
	}
	return baseZoneNotPresent
}

func (g *SpecifiedZonesGenerator) isZoneOutsideOfAPLocations(zone string) (bool, error) {
	for _, location := range g.cloudProvider.GetAutoprovisioningLocations() {
		if zone == location {
			return false, nil
		}
	}
	// check if zone is part of all available zones.
	allZones, err := g.cloudProvider.GetAllZones()
	if err != nil {
		klog.Errorf("error fetching zones for the cluster, err: %v", err)
	}
	for _, z := range allZones {
		if zone == z {
			return true, nil
		}
	}
	return true, fmt.Errorf("zone %s not found in cluster region", zone)
}

// AcceleratorSliceGenerator is NodePoolSpecGenerator instance responsible for any accelerator slice related adjustments to injected node pool
type AcceleratorSliceGenerator struct {
	provider machineConfigProvider
}

func NewAcceleratorSliceGenerator(provider machineConfigProvider) *AcceleratorSliceGenerator {
	return &AcceleratorSliceGenerator{
		provider: provider,
	}
}

// NodeVersionGenerator is a node version spec Generator.
type NodeVersionGenerator struct {
}

func NewNodeVersionGenerator() *NodeVersionGenerator {
	return &NodeVersionGenerator{}
}

func (g *NodeVersionGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, _ *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	if ngReq.RequiresComputeClass() && ngReq.computeClass.NodeVersion() != "" {
		ngReq.nodeVersion = ngReq.computeClass.NodeVersion()
	}
	return nil
}

func (g *NodeVersionGenerator) ValidateRequirements(_ *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (g *NodeVersionGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if ngReq.nodeVersion != "" {
		params.systemLabels[gkelabels.NodeVersionLabelKey] = ngReq.nodeVersion
	}
	return nil
}

func (g *NodeVersionGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, _ map[string]resource.Quantity) error {
	if nodeVersion, exist := systemLabels[gkelabels.NodeVersionLabelKey]; exist {
		spec.NodeVersion = nodeVersion
	}
	return nil
}

func (*AcceleratorSliceGenerator) UpdateRequirements(_ *nodeGroupRequirements, _ *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	return nil
}

func (*AcceleratorSliceGenerator) ValidateRequirements(_ *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (*AcceleratorSliceGenerator) UpdateParameters(_ *nodeGroupParameters, _ nodeGroupRequirements, _ NodeGroupOptions) error {
	return nil
}

func (acs *AcceleratorSliceGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, _ map[string]string, _ map[string]resource.Quantity) error {
	machineFamily, err := acs.provider.MachineConfigProvider().GetMachineFamilyFromMachineName(spec.MachineType)
	if err != nil {
		return fmt.Errorf("failed to get machine family from machine type %s, this should never happen at this point", spec.MachineType)
	}
	// For non-flex sliced gpus we set SURGE in a way that even if one
	// machine becomes unavailable all of them will be recreated.
	// This correlates with SHORT_LIVED behavior for flex machines.
	// Upgrade settings are updated only for GPU slices
	if !spec.FlexStart &&
		!spec.QueuedProvisioning &&
		machineFamily.IsGpuAcceleratorSliceSupported() &&
		spec.PlacementGroup.UsesSlice() &&
		spec.PlacementGroup.SupportsMachineFamily(machineFamily) {
		maxUnavailable, _ := placement.MaxNodes(acs.provider.MachineConfigProvider(), spec.MachineType, spec.PlacementGroup.ResourcePolicy)
		spec.UpgradeSettings = &gke_api_beta.UpgradeSettings{
			Strategy:       "SURGE",
			MaxSurge:       0,
			MaxUnavailable: maxUnavailable,
		}
	}
	return nil
}

type ConfidentialNodeGenerator struct {
	provider machineConfigProvider
}

type machineConfigProvider interface {
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

func NewConfidentialNodeGenerator(provider machineConfigProvider) *ConfidentialNodeGenerator {
	return &ConfidentialNodeGenerator{
		provider: provider,
	}
}

func (g *ConfidentialNodeGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	confidentialNodeType, found := podReq.LabelReq.GetSingleValue(gkelabels.GkeConfidentialNodeType)
	if !found {
		return nil
	}
	if !gkelabels.ValidConfidentialNodeTypes[confidentialNodeType] {
		return NewInvalidConfidentialNodeTypeError(confidentialNodeType)
	}
	machineFamily, err := g.machineFamilyFromPodReq(podReq)
	if err != nil {
		return err
	}
	if !machineFamily.IsConfidentialNodeTypeSupported(confidentialNodeType) {
		return NewInvalidMachineFamilyForConfidentialNodeTypeError(fmt.Sprintf("Machine family: %s does not support confidential node type: %s", machineFamily.Name(), confidentialNodeType))
	}
	ngReq.confidentialNodeType = confidentialNodeType
	return nil
}

func (g *ConfidentialNodeGenerator) machineFamilyFromPodReq(podReq *podrequirements.Requirements) (machinetypes.MachineFamily, caerrors.AutoscalerError) {
	if machineFamilyName, found := podReq.LabelReq.GetSingleValue(gkelabels.MachineFamilyLabel); found {
		machineFamily, err := g.provider.MachineConfigProvider().ToMachineFamily(machineFamilyName)
		if err != nil {
			return machinetypes.MachineFamily{}, NewInvalidMachineFamilyForConfidentialNodeTypeError(fmt.Sprintf("Unknown machine family: %s", machineFamilyName))
		}
		return machineFamily, nil
	}
	if gpuType, found := podReq.LabelReq.GetSingleValue(gkelabels.GPULabel); found {
		machineFamily, ok := g.provider.MachineConfigProvider().MachineFamilyForGpuType(gpuType)
		if !ok {
			return machinetypes.MachineFamily{}, NewInvalidMachineFamilyForConfidentialNodeTypeError(fmt.Sprintf("No known machine family for gpu type: %s", gpuType))
		}
		return machineFamily, nil
	}
	return machinetypes.MachineFamily{}, NewInvalidMachineFamilyForConfidentialNodeTypeError("Machine family or GPU must be explicitly specified to use confidential node types")
}

func (*ConfidentialNodeGenerator) ValidateRequirements(_ *nodeGroupRequirements) caerrors.AutoscalerError {
	return nil
}

func (*ConfidentialNodeGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if ngReq.confidentialNodeType != "" {
		params.systemLabels[gkelabels.GkeConfidentialNodeType] = ngReq.confidentialNodeType
		params.taints = append(params.taints, apiv1.Taint{
			Effect: apiv1.TaintEffectNoSchedule,
			Key:    gkelabels.GkeConfidentialNodeType,
			Value:  ngReq.confidentialNodeType,
		})
	}
	return nil
}

func (*ConfidentialNodeGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, _ map[string]resource.Quantity) error {
	if confidentialNodeType, found := systemLabels[gkelabels.GkeConfidentialNodeType]; found {
		if spec.Labels == nil {
			spec.Labels = make(map[string]string)
		}
		spec.ConfidentialNodeType = confidentialNodeType
		spec.Labels[gkelabels.GkeConfidentialNodeType] = confidentialNodeType
	}
	return nil
}

func (g *ConfidentialNodeGenerator) GenerateNodeGroupOptionsForRequirements(options []NodeGroupOptions, requirements nodeGroupRequirements) []NodeGroupOptions {
	var result []NodeGroupOptions
	for _, opt := range options {
		if gkelabels.ValidConfidentialNodeTypes[requirements.confidentialNodeType] {
			mt, err := g.provider.MachineConfigProvider().ToMachineType(opt.MachineType)
			if err != nil || !mt.IsConfidentialNodeTypeSupported(requirements.confidentialNodeType) {
				continue
			}
		}
		result = append(result, opt)
	}
	return result
}

// ConsolidationDelayGenerator is responsible for passing consolidation delay spec
type ConsolidationDelayGenerator struct {
	experimentsManager experiments.Manager
}

func NewConsolidationDelayGenerator(experimentsManager experiments.Manager) *ConsolidationDelayGenerator {
	return &ConsolidationDelayGenerator{
		experimentsManager: experimentsManager,
	}
}

func (g *ConsolidationDelayGenerator) UpdateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, _ machinetypes.GpuRequest, _ TpuRequest) caerrors.AutoscalerError {
	if !g.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.NodePoolConsolidationDelayMinCAVersionFlag, false) {
		return nil
	}

	podConsolidationDelayInSeconds, consolidationDelayIsRequested := podReq.LabelReq.GetSingleValue(gkelabels.ConsolidationDelayLabelKey)
	if consolidationDelayIsRequested {
		// Either pod or ComputeClass can specify ConsolidationDelay, not both.
		if ngReq.computeClass != nil && ngReq.computeClass.ConsolidationDelay() != nil {
			return NewComputeClassPodIncompatibleError(ngReq.computeClass.Name(), ngReq.computeClass.CrdType())
		}
		ngReq.consolidationDelayInSeconds = podConsolidationDelayInSeconds
	}

	return nil
}

func (g *ConsolidationDelayGenerator) ValidateRequirements(ngReq *nodeGroupRequirements) caerrors.AutoscalerError {
	if !g.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.NodePoolConsolidationDelayMinCAVersionFlag, false) {
		return nil
	}

	if ngReq.consolidationDelayInSeconds == "" {
		return nil
	}
	c, err := strconv.ParseInt(ngReq.consolidationDelayInSeconds, 10, 64)
	if err != nil {
		return caerrors.NewAutoscalerError(caerrors.ConfigurationError, "ConsolidationDelay is not a valid int64.")
	}
	if c < minConsolidationDelayInSeconds || c > maxConsolidationDelayInSeconds {
		return caerrors.NewAutoscalerErrorf(caerrors.ConfigurationError, "ConsolidationDelay is not within the allowed range (1 minute - 1 day). Got %v seconds.", c)
	}

	return nil
}

/**
 * UpdateParameters is called before passing parameters to NewNodeGroup() which creates NodePoolSpec object on which later we can set `nodePool.ConsolidationDelay` in `UpdateNodePoolSpec` below.
 * We need to indirectly copy that pod's label value to systemLabels so then later it can be copied to nodePoolSpec. We actually don't need that value in systemLabels.
 */
func (g *ConsolidationDelayGenerator) UpdateParameters(params *nodeGroupParameters, ngReq nodeGroupRequirements, _ NodeGroupOptions) error {
	if !g.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.NodePoolConsolidationDelayMinCAVersionFlag, false) {
		return nil
	}

	if ngReq.consolidationDelayInSeconds != "" {
		params.systemLabels[gkelabels.ConsolidationDelayLabelKey] = ngReq.consolidationDelayInSeconds
	}
	return nil
}

/**
 * Actually set protos ConsolidationDelay
 */
func (g *ConsolidationDelayGenerator) UpdateNodePoolSpec(spec *gkeclient.NodePoolSpec, systemLabels map[string]string, _ map[string]resource.Quantity) error {
	if !g.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.NodePoolConsolidationDelayMinCAVersionFlag, false) {
		return nil
	}

	if consolidationDelayInSeconds, exists := systemLabels[gkelabels.ConsolidationDelayLabelKey]; exists {
		if consolidationDelayInSeconds == "" {
			return nil
		}
		if spec.Labels == nil {
			spec.Labels = make(map[string]string)
		}
		spec.Labels[gkelabels.ConsolidationDelayLabelKey] = consolidationDelayInSeconds
		spec.ConsolidationDelayInSeconds = consolidationDelayInSeconds
	}
	return nil
}

// getNodeGroupParameters returns the parameters needed to inject a new node group for given requirements and other variables.
func (m *AutoprovisioningNodeGroupManager) getNodeGroupParameters(ngReq nodeGroupRequirements, opts NodeGroupOptions) (nodeGroupParameters, error) {
	params := &nodeGroupParameters{
		// machineType is passed directly as an argument.
		machineType:    opts.MachineType,
		labels:         map[string]string{},
		systemLabels:   map[string]string{},
		taints:         []apiv1.Taint{},
		extraResources: map[string]resource.Quantity{},
	}

	for _, generator := range m.specGenerators {
		err := generator.UpdateParameters(params, ngReq, opts)
		if err != nil {
			return nodeGroupParameters{}, err
		}
	}

	// Zone is passed through systemLabels.
	params.systemLabels[apiv1.LabelZoneFailureDomain] = opts.Zone

	params.taints = sanitizeTaints(params.taints)
	return *params, nil
}

// injectNodeGroups creates new non-existent node groups and associated node infos to accommodate the pods in requirements.
// Properties of the created node groups (e.g. GPU, preemption) depend on requirements. The created node groups and
// node infos are injected into appropriate structures in injectionContext. Every pod in requirements is marked as picked in
// ctx.status. Returns the number of node groups injected.
func (m *AutoprovisioningNodeGroupManager) injectNodeGroups(ctx *injectionContext, requirements nodeGroupRequirements) int {
	for _, pod := range requirements.pods {
		ctx.status.MarkPodPicked(pod.UID)
	}
	options := m.generateNodeGroupOptions(ctx, requirements)

	if ctx.applyReducedZoneSetOptimisation {
		options = m.applyReducedZoneSetOptimisation(options)
	}

	injected := 0
	for _, opts := range options {

		params, err := m.getNodeGroupParameters(requirements, opts)
		if err != nil {
			klog.Errorf("NAP: not injecting %s - couldn't get node group parameters, this shouldn't happen (err: %v)", opts.String(), err)
			ctx.status.AddDisregardedNodeGroup(opts, InternalError)
			continue
		}

		signature := params.signature()
		if ctx.injectedNodeGroupSignatures.Has(signature) {
			klog.V(4).Infof("NAP: not injecting %s - same node group already injected (signature: %s)", opts.String(), signature)
			continue
		}

		nodeGroup, err := m.cloudProvider.NewNodeGroup(params.machineType, params.labels, params.systemLabels, params.taints, params.extraResources)
		if err != nil {
			// NewNodeGroup is expected to fail if a certain configuration is not available in GKE (e.g. a specific GPU in a zone
			// where the GPU is not available), and we currently don't have a way to distinguish this case. So it's not necessarily
			// an error if it happens, which is why it's only logged as a warning.
			klog.Warningf("NAP: not injecting %s - couldn't build node group, this can be expected (err: %v)", opts.String(), err)
			ctx.status.AddDisregardedNodeGroup(opts, UnableToBuildNodeGroup)
			continue
		}
		nodeInfo, err := simulator.SanitizedTemplateNodeInfoFromNodeGroup(nodeGroup, ctx.daemonSets, ctx.taintConfig)
		if err != nil {
			klog.Errorf("NAP: not injecting %s - couldn't build node info, this shouldn't happen (err: %v)", opts.String(), err)
			ctx.status.AddDisregardedNodeGroup(opts, InternalError)
			continue
		}

		backoffCheckTime := time.Now()
		if st := m.nodeGroupBackoff.BackoffStatus(nodeGroup, nodeInfo, backoffCheckTime); st.IsBackedOff {
			klog.Infof("NAP: not injecting %s - in backoff, this is expected (extra resources: %v), reason: %s", opts.String(), params.extraResources, st.ErrorInfo)
			m.reportNodeGroupBackoff(ctx, opts, nodeGroup, nodeInfo, backoffCheckTime)
			continue
		}

		ctx.nodeInfos[nodeGroup.Id()] = nodeInfo
		ctx.injectedNodeGroups = append(ctx.injectedNodeGroups, nodeGroup)
		ctx.injectedNodeGroupSignatures.Insert(signature)
		injected++
	}
	return injected
}

// applyReducedZoneSetOptimisation trim options that only differ by zone.
func (m *AutoprovisioningNodeGroupManager) applyReducedZoneSetOptimisation(options []NodeGroupOptions) []NodeGroupOptions {
	optionsByZone := make(map[NodeGroupOptions][]string)
	for _, o := range options {
		zone := o.Zone
		o.Zone = ""
		optionsByZone[o] = append(optionsByZone[o], zone)
	}
	// Recreate options with a single random zone.
	var singleZoneOptions []NodeGroupOptions
	for option, zones := range optionsByZone {
		o := option
		selectedZone := zones[m.randInt(len(zones))]
		klog.V(4).Infof("Unschedulable pods are zone agnostic. Selected zone %v for nodegroup option %v from available zones %v", selectedZone, o.String(), zones)
		o.Zone = selectedZone
		singleZoneOptions = append(singleZoneOptions, o)
	}
	return singleZoneOptions
}

// generateNodeGroupOptions is a utility func used to generate the nodegroup options that should be considered
// by traversing through all the generators available for the nodeGroupRequirements and zone
func (m *AutoprovisioningNodeGroupManager) generateNodeGroupOptions(ctx *injectionContext, reqs nodeGroupRequirements) []NodeGroupOptions {
	options := make([]NodeGroupOptions, 0, len(ctx.zones))
	for _, zone := range ctx.zones {
		options = append(options, NodeGroupOptions{Zone: zone})
	}
	for _, generator := range m.nodeGroupOptionsGenerators {
		options = generator.GenerateNodeGroupOptionsForRequirements(options, reqs)
	}
	return options
}

// nonGpuPodsRequirements extracts nodeGroupRequirements from pods that don't request any GPU. If there are any pod-specific caerrors
// // encountered, they're reported via ctx.status.
func (m *AutoprovisioningNodeGroupManager) nonGpuPodsRequirements(ctx *injectionContext, unschedulablePods []*apiv1.Pod) []nodeGroupRequirements {
	var nonGpuPods []*apiv1.Pod
	for _, pod := range unschedulablePods {
		if !m.isGpuPod(pod) && !m.isTpuPod(pod) {
			nonGpuPods = append(nonGpuPods, pod)
		}
	}

	requirements, podErrors := m.extractRequirements(nonGpuPods, machinetypes.GpuRequest{}, TpuRequest{})
	for podUid, podErr := range podErrors {
		ctx.status.SetPodError(podUid, podErr)
	}
	var nonEmptyRequirements []nodeGroupRequirements
	for _, req := range requirements {
		if req.hasPods() {
			nonEmptyRequirements = append(nonEmptyRequirements, req)
		}
	}
	return nonEmptyRequirements
}

// NoPSCInfrastructureError means that a pod has private node affinity in a cluster without PSC infrastructure
const NoPSCInfrastructureError caerrors.AutoscalerErrorType = "NoPSCInfrastructureError"

// ErrNoPSCInfrastructure is an instance of AutoscalerError with NoPSCInfrastructureError type and an appropriate error message.
var ErrNoPSCInfrastructure = caerrors.NewAutoscalerError(NoPSCInfrastructureError, "pod requests non-default private nodes setting in clusters without PSC infrastructure")

// extractRequirements extracts node group requirements from the provided pods and their GPU request. The provided GPU request
// should satisfy GPU requirements for all provided pods. An empty GPU request should be passed for pods that don't
// request any GPU. If no valid requirements can be extracted, an empty (zero-value) requirements are returned. hasPods() can
// be used on the returned requirements to determine if they are empty.
//
// The requirements are extracted by grouping the pods by their requirements, and picking one of the groups at random.
// The idea is that if two pods have different requirements for injected node groups, it doesn't make sense to try injecting
// new node groups for both of them.
//
// If trying to determine a pod's requirements results in an error, the error will be returned in the error
// map under the pod's UID.
//
// TODO(b/216495457): The strategy described above is essentially re-doing what's already happening in sharding, but with
// an extended key. We could achieve similar effects by adding machine spec to the sharding key, computing nodeGroupRequirements
// while sharding, and plumbing it here (similar to AreUnschedulablePodsZoneAgnostic). This was decided against because of
// the effort required, and possible unintended consequences. However, the decision might be worth revisiting in the future.
func (m *AutoprovisioningNodeGroupManager) extractRequirements(pods []*apiv1.Pod, gpuReq machinetypes.GpuRequest, tpuReq TpuRequest) ([]nodeGroupRequirements, map[types.UID]caerrors.AutoscalerError) {
	possibleRequirements, podErrors := m.computePossibleRequirements(pods, gpuReq, tpuReq)
	if len(possibleRequirements) == 0 {
		return []nodeGroupRequirements{}, podErrors
	}
	chosenRequirements := possibleRequirements[m.randInt(len(possibleRequirements))]
	return chosenRequirements, podErrors
}

// computePossibleRequirements groups provided pods by their requirements for injected node groups and returns nodeGroupRequirements
// for each group. If trying to determine a pod's requirements results in an error, the error will be returned in the error
// map under the pod's UID.
func (m *AutoprovisioningNodeGroupManager) computePossibleRequirements(pods []*apiv1.Pod, gpuReq machinetypes.GpuRequest, tpuReq TpuRequest) ([][]nodeGroupRequirements, map[types.UID]caerrors.AutoscalerError) {
	requirementsBySig := map[string][]*nodeGroupRequirements{}
	podErrors := map[types.UID]caerrors.AutoscalerError{}
	for _, pod := range pods {
		podReqs := podrequirements.GetRequirements(pod)
		if !m.cloudProvider.UseAutoprovisioningFeaturesForPodRequirements(podReqs) {
			continue
		}

		allRequirements := []nodeGroupRequirements{{pods: []*apiv1.Pod{pod}}}
		var err caerrors.AutoscalerError
		for _, generator := range m.nodeGroupRequirementsGenerators {
			allRequirements, err = generator.GenerateNodeGroupRequirements(allRequirements, podReqs)
			if err != nil {
				break
			}
		}
		if err != nil {
			podErrors[pod.UID] = err
			continue
		}

		var updatedRequirements []*nodeGroupRequirements
		var errs []caerrors.AutoscalerError
		for _, requirements := range allRequirements {
			requirements := requirements
			err = m.updateAndValidateRequirements(&requirements, podReqs, gpuReq, tpuReq)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			updatedRequirements = append(updatedRequirements, &requirements)
		}
		combinedErr := caerrors.Combine(errs)
		if combinedErr != nil {
			klog.Errorf("Errors in updateAndValidateRequirements: %v", combinedErr)
		}
		if len(updatedRequirements) == 0 {
			podErrors[pod.UID] = combinedErr
			continue
		}

		// TODO(b/517093793): check pod requirements matching

		var signatures []string
		for _, requirements := range updatedRequirements {
			signatures = append(signatures, requirements.signature())
		}
		signature := strings.Join(signatures, ";")

		existingRequirements, found := requirementsBySig[signature]
		if !found {
			requirementsBySig[signature] = updatedRequirements
		} else {
			for _, requirements := range existingRequirements {
				requirements.pods = append(requirements.pods, pod)
			}
		}
	}

	var result [][]nodeGroupRequirements
	for _, existingRequirements := range requirementsBySig {
		var partialResult []nodeGroupRequirements
		for _, requirements := range existingRequirements {
			partialResult = append(partialResult, *requirements)
		}
		result = append(result, partialResult)
	}
	return result, podErrors
}

func (m *AutoprovisioningNodeGroupManager) updateAndValidateRequirements(ngReq *nodeGroupRequirements, podReq *podrequirements.Requirements, gpuReq machinetypes.GpuRequest, tpuReq TpuRequest) caerrors.AutoscalerError {
	for _, generator := range m.specGenerators {
		err := generator.UpdateRequirements(ngReq, podReq, gpuReq, tpuReq)
		if err != nil {
			return err
		}
	}
	for _, generator := range m.specGenerators {
		err := generator.ValidateRequirements(ngReq)
		if err != nil {
			return err
		}
	}
	return nil
}

// prepareInjectionContext prepares and gathers all common data and components required for injecting new node groups.
func (m *AutoprovisioningNodeGroupManager) prepareInjectionContext(ctx *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo, status *ProcessingStatus) (*injectionContext, error) {
	var zones []string
	noReservations := m.reservationsPuller == nil || len(m.reservationsPuller.GetReservations()) == 0
	applyReducedZoneSetOptimisation := podsharding.AreUnschedulablePodsZoneAgnostic(ctx) && noReservations
	// If "reduced zone set optimization" is used, there is a risk that all autoprovisioning
	// zones will be removed from consideration, so we have to exclude this case.
	// This won't affect pod's using "any zone selection" feature, since they have to select a specific
	// zone directly and thus are not zone-agnostic.
	if m.enableUserAnyZoneSelection && !applyReducedZoneSetOptimisation {
		var err error
		zones, err = m.cloudProvider.GetAllZones()
		if err != nil {
			return nil, fmt.Errorf("error fetching all zones in the cluster, err: %v", err)
		}
	} else {
		zones = m.cloudProvider.GetAutoprovisioningLocations()
	}
	if len(zones) == 0 {
		status.SetResult(NoAutoprovisioningLocationsAvailable)
		return nil, fmt.Errorf("no node locations found")
	}

	resourceLimiter, errCP := m.cloudProvider.GetResourceLimiter()
	if errCP != nil {
		status.SetResult(ResourceLimiterNotAvailable)
		return nil, errCP
	}

	daemonSets, err := ctx.ListerRegistry.DaemonSetLister().List(apilabels.Everything())
	if err != nil {
		status.SetResult(DaemonSetsNotAvailable)
		return nil, caerrors.NewAutoscalerErrorf(caerrors.ApiCallError, "failed to get daemonset list: %v", err)
	}

	taintConfig := taintutils.NewTaintConfig(ctx.AutoscalingOptions)

	return &injectionContext{
		status:                          status,
		nodeInfos:                       nodeInfos,
		existingNodeGroups:              nodeGroups,
		injectedNodeGroups:              nil,
		resourceLimiter:                 resourceLimiter,
		clusterSnapshot:                 ctx.ClusterSnapshot,
		daemonSets:                      daemonSets,
		taintConfig:                     taintConfig,
		zones:                           zones,
		applyReducedZoneSetOptimisation: applyReducedZoneSetOptimisation,
		injectedNodeGroupSignatures:     sets.New[string](),
	}, nil
}

// reportNodeGroupBackoff marks the node group as disregarded because of backoff, distinguishing between standard and
// resource-based backoff.
func (m *AutoprovisioningNodeGroupManager) reportNodeGroupBackoff(ctx *injectionContext, ngKey NodeGroupOptions, nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, checkTime time.Time) {
	// We want to distinguish between the default backoff and resource-based backoff.
	// TODO(b/145978721): Clean up after Backoff interface is changed in OSS.
	if resourceBackoff := getResourceBasedBackoff(m.nodeGroupBackoff); resourceBackoff != nil && resourceBackoff.BackoffStatus(nodeGroup, nodeInfo, checkTime).IsBackedOff {
		ctx.status.AddDisregardedNodeGroup(ngKey, InResourceBasedBackoff)
	} else {
		ctx.status.AddDisregardedNodeGroup(ngKey, InStandardBackoff)
	}
}

// preemptionOptions returns which preemption options should be tried for given pods. If no pods tolerate preemption,
// a single option with NoPreemption will be returned. If any of the pods tolerates preemption, two options will be
// returned - NoPreemption, and an appropriate preemption type.
func preemptionOptions(pods []*apiv1.Pod, rule rules.Rule, flexStart bool) []preemption.VmPreemptionType {
	// Do not try preemption options for FlexStart provisioning model requirement as they're incompatible
	if flexStart {
		return []preemption.VmPreemptionType{preemption.NoPreemption}
	}
	if rule != nil {
		if rule.Spot() {
			return []preemption.VmPreemptionType{preemption.Spot}
		} else {
			return []preemption.VmPreemptionType{preemption.NoPreemption}
		}
	}

	result := []preemption.VmPreemptionType{preemption.NoPreemption}
	if toleratedVmPreemption := preemption.ToleratedVmPreemptionForAnyPod(pods); toleratedVmPreemption != preemption.NoPreemption {
		result = append(result, toleratedVmPreemption)
	}
	return result
}

// wantsSpot returns true if preemption.Spot is one of the preemption options to be tried for given pods.
func wantsSpot(pods []*apiv1.Pod, rule rules.Rule, flexStart bool) bool {
	for _, preemptionType := range preemptionOptions(pods, rule, flexStart) {
		if preemptionType == preemption.Spot {
			return true
		}
	}
	return false
}

// sanitizeTaints replaces empty taints effect (incorrect from node group
// config perspective) with a default effect value (NoSchedule).
func sanitizeTaints(taints []apiv1.Taint) []apiv1.Taint {
	var result []apiv1.Taint
	for _, t := range taints {
		if t.Effect == "" {
			t.Effect = apiv1.TaintEffectNoSchedule
		}
		result = append(result, t)
	}
	return result
}

func isStatefulWorkload(ngReq *nodeGroupRequirements) bool {
	for _, pod := range ngReq.pods {
		if podrequirements.IsPodStateful(pod) {
			return true
		}
	}
	return false
}
