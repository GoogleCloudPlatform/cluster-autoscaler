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

package gkeprice

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	gce_api "google.golang.org/api/compute/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups/asyncnodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/preemption"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/provider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klog "k8s.io/klog/v2"
)

// *************
// The detailed description of what is going on in this expander can be found here:
// https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/proposals/pricing.md
// https://docs.google.com/document/d/1GjHJAMPG_CRHICWExzr5lxXWbUzi_1vOAqlhuDovEm8
// **********

type RelaxedNodeGroupPenaltyChecker interface {
	// Enabled decides if relaxed group penalty should be used when scoring scale-up options.
	Enabled() bool
}

type gkePriceBased struct {
	pricingModel                   cloudprovider.PricingModel
	clusterAnalyzer                ClusterAnalyzer
	groupCountReducer              GroupCountReducer
	machineTypeBalancer            MachineTypeBalancer
	reservationsPuller             *gceclient.ReservationsPuller
	localSSDDiskSizeProvider       localssdsize.LocalSSDSizeProvider
	relaxedNodeGroupPenaltyChecker RelaxedNodeGroupPenaltyChecker
	pvmUnfitnessPenaltyEnabled     bool
	epsilon                        float64
	autopilotEnabled               bool
	upcomingChecker                asyncnodegroups.AsyncNodeGroupStateChecker
	cloudProvider                  provider.GkeExpanderCloudProvider
}

const (
	// autopilotEpsilon is a small constant used to compare option scores. It ensures that we don't
	// switch the currently best option to a marginally better one, which along with shuffling
	// of the expansion options provides randomness between options with close enough scores.
	autopilotEpsilon = 0.0001

	// defaultPreferredCpuCount is preferred count of CPUs for new nodes if ClusterAnalyzer fails.
	defaultPreferredCpuCount = 4

	// reservationDiscount is a discount used for nodes with matching GCE Reservations.
	// Using the 100% discount could result in weird expander behaviour caused by
	// "free" scale-up options. Additionally, this constant limits the inefficiency
	// of scale-ups using GCE Reservations, trying to avoid creating giant reserved
	// nodes for small pending pods. Current value effectively prevents us from using
	// more than 8 times bigger machines than the optimal ones.

	// Example pricing (from cheapest to most expensive):
	// 1. non-existing e2-standard-16 with reservations
	// 2. existing e2-standard-32 with reservations
	// 3. existing e2-standard-2 without reservations
	// 4. non-existing e2-standard-32 with reservations
	reservationDiscount = 0.98

	// unfitnessMultiplier is a base for simpleNodeUnfitness calculations. Power of 2 results
	// of CPU count ratio are converted to power of notExistCoefficient*unfitnessMultiplier.
	// The base should be higher than notExistCoefficient to allow new node pool creation,
	// but lower than a price difference between instance types sizes.
	// unfitnessMultiplier < notExistCoefficient*unfitnessMultiplier << 2
	unfitnessMultiplier = 1.1

	// This value will be used as unfitness for node groups using accelerators, such as
	// GPU or TPU. This serves 2 purposes:
	// - It makes nodes with accelerators extremely unattractive to expander, so it will
	//   never use nodes with expensive GPUs or TPUs for pods that don't require it.
	// - By overriding unfitness for node groups with accelerators we ignore preferred
	//   cluster shape when comparing such node groups. Node unfitness logic is meant to
	//   minimize per-node cost (resources consumed by kubelet, kube-proxy, etc) and
	//   resource fragmentation, while avoiding putting a significant fraction of all
	//   pods on a single node for availability reasons.
	//   Those goals don't apply well to nodes with accelerators that are generally dedicated
	//   for specific workload and need to be optimized for accelerator utilization, not CPU
	//   utilization.
	acceleratorsUnfitnessOverride = 1000.0
	// Preemption VMs using greater than preemptionUnfitnessCPUThreshold vcpus or
	// preemptionUnfitnessMemoryThreshold GB of RAM are more prone to preemption,
	// unless high-cpu classes are used. preemptionUnfitnessMultiplier is the
	// penalty applied to these options.
	preemptionUnfitnessMultiplier      = 3
	preemptionUnfitnessCPUThreshold    = 32
	preemptionUnfitnessMemoryThreshold = 64 * units.GiB
	largeScaleUpNodeCountThreshold     = 10
)

var (
	// priceStabilizationPod is the pod cost to stabilize node_cost/pod_cost ratio a bit.
	// 0.5 cpu, 500 mb ram
	priceStabilizationPod = buildPod("stabilize", 500, 500*units.MiB, 0)
)

// NewStrategy returns an expansion strategy based on node pricing and additional preferences
func NewStrategy(
	cloudProvider provider.GkeExpanderCloudProvider,
	nodeLister kube_util.NodeLister,
	podLister kube_util.PodLister,
	reservationsPuller *gceclient.ReservationsPuller,
	penaltyChecker RelaxedNodeGroupPenaltyChecker,
	pvmUnfitnessPenaltyEnabled bool,
	localssdDiskSizeProvider localssdsize.LocalSSDSizeProvider,
	upcomingChecker asyncnodegroups.AsyncNodeGroupStateChecker,
) (*gkePriceBased, errors.AutoscalerError) {
	pricingModel, err := cloudProvider.Pricing()
	if err != nil {
		return nil, err
	}

	clusterAnalyzer := NewGroupingClusterAnalyzer(cloudProvider, nodeLister, podLister, nil)
	groupCountReducer := NewProgressiveGroupCountReducer(cloudProvider)

	var machineTypeBalancer MachineTypeBalancer
	var epsilon float64
	if cloudProvider.IsAutopilotEnabled() {
		machineTypeBalancer = NewComputeClassBalancer(cloudProvider)
		epsilon = autopilotEpsilon
	}

	return &gkePriceBased{
		cloudProvider:                  cloudProvider,
		pricingModel:                   pricingModel,
		clusterAnalyzer:                clusterAnalyzer,
		groupCountReducer:              groupCountReducer,
		machineTypeBalancer:            machineTypeBalancer,
		pvmUnfitnessPenaltyEnabled:     pvmUnfitnessPenaltyEnabled,
		epsilon:                        epsilon,
		reservationsPuller:             reservationsPuller,
		relaxedNodeGroupPenaltyChecker: penaltyChecker,
		autopilotEnabled:               cloudProvider.IsAutopilotEnabled(),
		localSSDDiskSizeProvider:       localssdDiskSizeProvider,
		upcomingChecker:                upcomingChecker,
	}, nil
}

// BestOption selects option based on cost and preferred node type.
func (p *gkePriceBased) BestOption(expansionOptions []expander.Option, nodeInfos map[string]*framework.NodeInfo) *expander.Option {
	var bestOption *expander.Option
	bestOptionScore := 0.0
	bestOptionUnfitness := math.MaxFloat64
	now := time.Now()
	then := now.Add(time.Hour)

	// Evaluated only once per call to guarantee the same behavior for all options.
	relaxedNodeGroupPenaltyEnabled := p.relaxedNodeGroupPenaltyChecker.Enabled()

	// shuffling introduces randomness between options with the same score
	rand.Shuffle(len(expansionOptions), func(i, j int) {
		expansionOptions[i], expansionOptions[j] = expansionOptions[j], expansionOptions[i]
	})

	clusterAnalysis, err := p.clusterAnalyzer.Analyze(nodeInfos)
	if err != nil {
		klog.Errorf("Failed to analyze the cluster: %v", err)
		// continuing without analysis.
	}

	stabilizationPrice, err := p.pricingModel.PodPrice(priceStabilizationPod, now, then)
	if err != nil {
		klog.Errorf("Failed to get price for stabilization pod: %v", err)
		// continuing without stabilization.
	}

	var gceReservations []*gce_api.Reservation
	if p.reservationsPuller != nil {
		gceReservations = p.reservationsPuller.GetReservations()
	}

	minAddedNodeCount := math.MaxInt
	for _, option := range expansionOptions {
		if minAddedNodeCount > option.NodeCount {
			minAddedNodeCount = option.NodeCount
		}
	}

nextoption:
	for idx, option := range expansionOptions {
		nodeInfo, found := nodeInfos[option.NodeGroup.Id()]
		if !found {
			klog.Errorf("No node info for %s", option.NodeGroup.Id())
			continue
		}
		machineType := nodeInfo.Node().Labels[apiv1.LabelInstanceType]

		totalNodePrice, totalReservations, err := p.totalNodePrice(option, nodeInfo.Node(), gceReservations, minAddedNodeCount, now, then)
		if err != nil {
			klog.Errorf("Failed to calculate node price for %s: %v", option.NodeGroup.Id(), err)
			continue
		}

		totalPodPrice := 0.0
		for _, pod := range option.Pods {
			podPrice, err := p.pricingModel.PodPrice(pod, now, then)
			if err != nil {
				klog.Warningf("Failed to calculate pod price for %s/%s: %v", pod.Namespace, pod.Name, err)
				continue nextoption
			}
			totalPodPrice += podPrice
		}

		nodePoolHasGpu := gpu.NodeHasGpu(labels.GPULabel, nodeInfo.Node())
		nodePoolHasAccelerators := nodePoolHasGpu || tpu.NodeHasTpu(nodeInfo.Node())
		reclaimablePrice := p.reclaimablePrice(nodePoolHasGpu, clusterAnalysis, option, nodeInfo, now, then)
		// Total pod price is 0 when the pods have no requests. The pods must have some other
		// requirements that prevent them from scheduling like AntiAffinity, HostPort or the
		// pods quota on all nodes has been already used. We use stabilizationPrice in the formula
		// below so this should not be a problem.

		// How well the money is spent.
		priceSubScore := (totalNodePrice + stabilizationPrice) / (totalPodPrice + reclaimablePrice + stabilizationPrice)

		preferredCpuCount := int64(defaultPreferredCpuCount)
		if clusterAnalysis != nil {
			var err error
			preferredCpuCount, err = clusterAnalysis.GetPreferredCpuCount(option, nodeInfo)
			if err != nil {
				klog.Errorf("Failed to get preferred node, switching to default: %v", err)
				preferredCpuCount = defaultPreferredCpuCount
			}
		}
		// How well the node matches generic cluster needs
		nodeUnfitness := simpleNodeUnfitness(preferredCpuCount, nodeInfo.Node())

		if p.pvmUnfitnessPenaltyEnabled {
			nodeUnfitness *= preemptionUnfitness(nodeInfo.Node())
		}

		supressedUnfitness := (nodeUnfitness-1.0)*(1.0-math.Tanh(float64(option.NodeCount-1)/15.0)) + 1.0
		if p.autopilotEnabled {
			supressedUnfitness = getSupressedUnfitness(nodeUnfitness, minAddedNodeCount)
		}

		// Set constant, very high unfitness to make them unattractive for pods that doesn't need GPU and
		// avoid optimizing them for CPU utilization.
		if nodePoolHasAccelerators {
			klog.V(4).Infof("GKE price expander overriding unfitness for node group with accelerators %s", option.NodeGroup.Id())
			// Discourage nodes that could contain many GPU pods. Such nodes have lower average resource utilization
			// as even when some pods finish earlier, the rest of them would usually prevent a scale down.
			supressedUnfitness = acceleratorsUnfitnessOverride * float64(countNodeAccelerators(nodeInfo.Node()))
		}

		optionScore := supressedUnfitness * priceSubScore

		groupCountPenalty := 1.0
		if !option.NodeGroup.Exist() && !p.upcomingChecker.IsUpcoming(option.NodeGroup) {
			if relaxedNodeGroupPenaltyEnabled {
				groupCountPenalty = p.groupCountReducer.BaseGroupCreationPenalty()
			} else {
				groupCountPenalty = p.groupCountReducer.GroupCreationPenalty(nodePoolHasGpu)
			}
			optionScore *= groupCountPenalty
		}

		machineTypeBalancingFactor := 1.0
		if p.machineTypeBalancer != nil {
			machineTypeBalancingFactor = p.machineTypeBalancer.MachineTypeBalancingFactor(machineType, nodeInfos)
			optionScore *= machineTypeBalancingFactor
		}

		debug := fmt.Sprintf("machine_type=%s node_count=%d all_nodes_price=%f total_reservations=%d pods_price=%f reclaimable_price=%f stabilized_ratio=%f preferred_cpu_count=%d unfitness=%f suppressed=%f group_count_penalty=%f machine_type_balancing_factor=%f node_annotations=%#v, final_score=%f",
			machineType,
			option.NodeCount,
			totalNodePrice,
			totalReservations,
			totalPodPrice,
			reclaimablePrice,
			priceSubScore,
			preferredCpuCount,
			nodeUnfitness,
			supressedUnfitness,
			groupCountPenalty,
			machineTypeBalancingFactor,
			nodeInfo.Node().Annotations,
			optionScore,
		)

		klog.V(5).Infof("GKE expander for %s: %s", option.NodeGroup.Id(), debug)

		if p.isBetterOption(bestOption, bestOptionScore, optionScore, bestOptionUnfitness, nodeUnfitness) {
			bestOption = &expansionOptions[idx]
			bestOption.Debug = fmt.Sprintf("%s | gke-expander: %s", option.Debug, debug)
			bestOptionScore = optionScore
			bestOptionUnfitness = nodeUnfitness
		}
	}
	return bestOption
}

// BestOptions narrows down the list of expansion options to a subset which is equally good as far a given expander is concerned.
// In case of gke-price expander, there's only a single winning option.
func (p *gkePriceBased) BestOptions(expansionOptions []expander.Option, nodeInfos map[string]*framework.NodeInfo) []expander.Option {
	opts := make([]expander.Option, 0, 1)
	best := p.BestOption(expansionOptions, nodeInfos)
	if best != nil {
		opts = append(opts, *best)
	}
	return opts
}

// simpleNodeUnfitness returns unfitness based on cpu only. Power of 2 results
// of CPU count ratio are converted to power of notExistCoefficient*unfitnessMultiplier.
// The base should be higher than notExistCoefficient to allow new node pool creation,
// but lower than a price difference between instance types sizes to handle limits
// of pods per node number like anti-affinity, pod count capacity, etc.
func simpleNodeUnfitness(preferredCpuCount int64, evaluatedNode *apiv1.Node) float64 {
	evaluatedCpu := evaluatedNode.Status.Capacity[apiv1.ResourceCPU]
	ratio := math.Max(float64(1000*preferredCpuCount)/float64(evaluatedCpu.MilliValue()),
		float64(evaluatedCpu.MilliValue())/float64(1000*preferredCpuCount))
	// Unfitness should be just above node group creation penalty for close node groups.
	power := math.Log2(ratio)
	return math.Pow(notExistCoefficient*unfitnessMultiplier, power)
}

// Certain types of preemption VMs are more prone to preemption.
// preemptionUnfitness returns an unfitness score based on cpu and mem
// considerations.
func preemptionUnfitness(node *apiv1.Node) float64 {
	if preemptionType := preemption.TypeFromLabels(node.Labels); preemptionType == preemption.NoPreemption {
		return 1.0
	}
	cpu := node.Status.Capacity[apiv1.ResourceCPU]
	mem := node.Status.Capacity[apiv1.ResourceMemory]
	if (cpu.Value() <= preemptionUnfitnessCPUThreshold &&
		mem.Value() <= preemptionUnfitnessMemoryThreshold) ||
		cpu.Value()*units.GiB >= mem.Value() {
		return 1.0
	}
	return preemptionUnfitnessMultiplier
}

func countNodeAccelerators(node *apiv1.Node) int64 {
	var accCount int64 = 0
	if gpu.NodeHasGpu(gpu.ResourceNvidiaGPU, node) {
		gpuAllocatable := node.Status.Allocatable[gpu.ResourceNvidiaGPU]
		accCount = accCount + gpuAllocatable.Value()
	}
	if tpu.NodeHasTpu(node) {
		tpuAllocatable := node.Status.Allocatable[tpu.ResourceGoogleTPU]
		accCount = accCount + tpuAllocatable.Value()
	}

	return accCount
}

// buildPod creates a pod with specified resources.
func buildPod(name string, millicpu, mem, ephStr int64) *apiv1.Pod {
	return &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      name,
			SelfLink:  fmt.Sprintf("/api/v1/namespaces/default/pods/%s", name),
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							apiv1.ResourceCPU:              *resource.NewMilliQuantity(millicpu, resource.DecimalSI),
							apiv1.ResourceMemory:           *resource.NewQuantity(mem, resource.DecimalSI),
							apiv1.ResourceEphemeralStorage: *resource.NewQuantity(ephStr, resource.DecimalSI),
						},
					},
				},
			},
		},
	}
}

func (p *gkePriceBased) totalNodePrice(option expander.Option, node *apiv1.Node, gceReservations []*gce_api.Reservation, minAddedNodeCount int, now time.Time, then time.Time) (float64, int64, error) {
	nodePrice, err := p.pricingModel.NodePrice(node, now, then)
	if err != nil {
		return 0.0, 0, err
	}

	nodeGroups := append(option.SimilarNodeGroups, option.NodeGroup)
	totalReservations := int64(0)
	for _, nodeGroup := range nodeGroups {
		totalReservations += int64(reservations.MatchingUnusedReservations(p.cloudProvider, nodeGroup, gceReservations, p.localSSDDiskSizeProvider))
	}

	nodeCount := int64(option.NodeCount)
	// After binpacking, one node is typically only partially utilized. When the scale up is large,
	// we don't calculate reclaimable price heuristic, but instead pretend that one node is simply not there at all.
	if p.autopilotEnabled && minAddedNodeCount >= largeScaleUpNodeCountThreshold {
		nodeCount--
	}
	totalReservations = min(nodeCount, totalReservations)
	if totalReservations > 0 {
		klog.V(4).Infof("GKE expander found matching GCE Reservations for %s: reservations=%d", option.NodeGroup.Id(), totalReservations)
	}

	totalNodePrice := nodePrice * (float64(nodeCount) - float64(totalReservations)*reservationDiscount)
	return totalNodePrice, totalReservations, nil
}

func getSupressedUnfitness(unfitness float64, minAddedNodeCount int) float64 {
	if minAddedNodeCount <= 3 {
		return unfitness
	} else if minAddedNodeCount < largeScaleUpNodeCountThreshold {
		return (unfitness + 1) * 0.5
	}
	return 1
}

// isBetterOption returns true iff the option with optionScore should be considered better than the current bestOption.
// In Autopilot, when two options have equal score (within epsilon), the option with better unfitness wins.
func (p *gkePriceBased) isBetterOption(bestOption *expander.Option, bestOptionScore, optionScore float64, bestOptionUnfitness, nodeUnfitness float64) bool {
	if bestOption == nil {
		return true
	}
	if bestOptionScore > optionScore*(1+p.epsilon) {
		return true
	}
	if !p.autopilotEnabled {
		return false
	}
	if bestOptionScore*(1+p.epsilon) < optionScore {
		return false
	}
	return nodeUnfitness < bestOptionUnfitness
}

// reclaimablePrice returns the predicted cost of resources that will be reclaimed with new pods in the future
func (p *gkePriceBased) reclaimablePrice(nodePoolHasGpu bool, clusterAnalysis ClusterAnalysis, option expander.Option, nodeInfo *framework.NodeInfo, now, then time.Time) float64 {
	if p.autopilotEnabled && option.NodeCount >= largeScaleUpNodeCountThreshold {
		return 0
	}
	// Nodes with GPUs have taints and probably won't be reclaimed
	if nodePoolHasGpu {
		return 0
	}
	if clusterAnalysis == nil {
		return 0
	}

	reclaimableResourcesPod, err := clusterAnalysis.GetReusableResources(option, nodeInfo)
	if err != nil {
		if err != cloudprovider.ErrNotImplemented {
			klog.Errorf("Failed to compute reclaimable resources, skipping: %v", err)
		}
		return 0
	}

	reclaimablePrice, err := p.pricingModel.PodPrice(reclaimableResourcesPod, now, then)
	if err != nil {
		klog.Errorf("Failed to calculate reclaimable price, skipping: %v", err)
	}
	return reclaimablePrice
}
