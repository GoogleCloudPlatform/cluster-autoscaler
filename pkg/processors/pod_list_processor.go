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

package processors

import (
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/podlistprocessor"
	cbprocessors "k8s.io/autoscaler/cluster-autoscaler/processors/capacitybuffer"
	"k8s.io/autoscaler/cluster-autoscaler/processors/podinjection"
	"k8s.io/autoscaler/cluster-autoscaler/processors/pods"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"

	cr_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/processors"
	csn_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/processors"
	defrag_processor "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/processor"
	lookaheadbuffer_processor "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer/processor"
	ekvms_processor "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/processor"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	cb_metrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/capacitybuffer"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/podstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podsharding"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podtopologyspread"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/capacitybuffers"
	metrics_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/metrics"
	pr_pods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	pr_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/processors"
	klog "k8s.io/klog/v2"

	cc_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/processors"

	apiv1 "k8s.io/api/core/v1"
)

type ScaleToZeroProcessor interface {
	Process(context *context.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error)
	Drainable(drainContext *drainability.DrainContext, pod *apiv1.Pod, node *framework.NodeInfo) drainability.Status
	Name() string
}

type DefragProcessor interface {
	Process(context *context.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error)
	DefragPickedCandidate() bool
	CleanUp()
}

// GkeInternalPodListProcessor is a PodListProcessor used in gke internal CA.
type GkeInternalPodListProcessor struct {
	crProcessor                    *cr_processors.CapacityRequestPodListProcessor
	prProcessor                    *pr_processors.ProvisioningRequestPodListProcessor
	defaultPodListerProcessor      pods.PodListProcessor
	ekvmsProcessor                 *ekvms_processor.ScaleUpNodeProcessor
	podStatusAggregator            *metrics_processors.PodStatusAggregator
	podShardingProcessor           *podsharding.PodShardingProcessor
	scaleToZeroProcessor           ScaleToZeroProcessor
	defragProcessor                DefragProcessor
	podInjectionProcessor          *podinjection.PodInjectionPodListProcessor
	ossProvReqPodsInjector         pods.PodListProcessor
	enforceFakePodsLimitProcessor  *podinjection.EnforceInjectedPodsLimitProcessor
	lookaheadPodsInjection         *lookaheadbuffer_processor.LookaheadPodInjectionProcessor
	podStateObserver               *podstate.PodStateObserver
	podTopologySpreadProcessor     *podtopologyspread.Processor
	flexAdvisorPodListProcessor    *flexadvisor.PodListProcessor
	cbPodInjectionProcessor        *cbprocessors.CapacityBufferPodListProcessor
	csnNodeReconcilationProcessor  *csn_processors.NodeReconcilationProcessor
	csnBufferConsumptionProcessor  pods.PodListProcessor
	csnCSNPodsLifecycleProcessor   *csn_processors.CSNPodsLifecycleProcessor
	capacityBufferMetricsProcessor *capacitybuffers.MetricProcessor
	cbFakePodStateObserver         *cb_metrics.FakePodStateObserver
	ccMinCapacityProcessor         *cc_processors.MinCapacityPodListProcessor

	experimentsManager experiments.Manager
}

// NewGkeInternalPodListProcessor returns a new GkeInternalPodListProcessor.
func NewGkeInternalPodListProcessor(crProcessor *cr_processors.CapacityRequestPodListProcessor,
	prProcessor *pr_processors.ProvisioningRequestPodListProcessor,
	ekvmsProcessor *ekvms_processor.ScaleUpNodeProcessor,
	podStatusAggregator *metrics_processors.PodStatusAggregator,
	podShardingProcessor *podsharding.PodShardingProcessor,
	scaleToZeroProcessor ScaleToZeroProcessor,
	defragProcessor *defrag_processor.Processor,
	podInjectionProcessor *podinjection.PodInjectionPodListProcessor,
	ossProvReqPodsInjector pods.PodListProcessor,
	enforceFakePodsLimitProcessor *podinjection.EnforceInjectedPodsLimitProcessor,
	lookaheadPodsInjection *lookaheadbuffer_processor.LookaheadPodInjectionProcessor,
	podStateObserver *podstate.PodStateObserver,
	podTopologySpreadProcessor *podtopologyspread.Processor,
	flexAdvisorPodListProcessor *flexadvisor.PodListProcessor,
	cbPodInjectionProcessor *cbprocessors.CapacityBufferPodListProcessor,
	csnNodeReconcilationProcessor *csn_processors.NodeReconcilationProcessor,
	csnBufferConsumptionProcessor *csn_processors.BufferConsumptionProcessor,
	csnCSNPodsLifecycleProcessor *csn_processors.CSNPodsLifecycleProcessor,
	capacityBufferMetricsProcessor *capacitybuffers.MetricProcessor,
	cbReactionTimeReporter *cb_metrics.FakePodStateObserver,
	ccMinCapacityProcessor *cc_processors.MinCapacityPodListProcessor,
	em experiments.Manager,
) *GkeInternalPodListProcessor {
	p := &GkeInternalPodListProcessor{
		crProcessor:                    crProcessor,
		defaultPodListerProcessor:      podlistprocessor.NewDefaultPodListProcessor(pr_pods.DoNotScheduleOnDWS),
		prProcessor:                    prProcessor,
		ekvmsProcessor:                 ekvmsProcessor,
		podStatusAggregator:            podStatusAggregator,
		podShardingProcessor:           podShardingProcessor,
		scaleToZeroProcessor:           scaleToZeroProcessor,
		podInjectionProcessor:          podInjectionProcessor,
		ossProvReqPodsInjector:         ossProvReqPodsInjector,
		enforceFakePodsLimitProcessor:  enforceFakePodsLimitProcessor,
		lookaheadPodsInjection:         lookaheadPodsInjection,
		podTopologySpreadProcessor:     podTopologySpreadProcessor,
		flexAdvisorPodListProcessor:    flexAdvisorPodListProcessor,
		cbPodInjectionProcessor:        cbPodInjectionProcessor,
		csnNodeReconcilationProcessor:  csnNodeReconcilationProcessor,
		csnCSNPodsLifecycleProcessor:   csnCSNPodsLifecycleProcessor,
		capacityBufferMetricsProcessor: capacityBufferMetricsProcessor,
		podStateObserver:               podStateObserver,
		cbFakePodStateObserver:         cbReactionTimeReporter,
		ccMinCapacityProcessor:         ccMinCapacityProcessor,
		experimentsManager:             em,
	}
	if defragProcessor != nil {
		p.defragProcessor = defragProcessor
	}
	if csnBufferConsumptionProcessor != nil {
		p.csnBufferConsumptionProcessor = csnBufferConsumptionProcessor
	}
	return p
}

// Process calls all gke internal PodListProcessors.
func (p *GkeInternalPodListProcessor) Process(context *context.AutoscalingContext,
	unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {

	if p.cbFakePodStateObserver != nil {
		p.cbFakePodStateObserver.Reset()
	}

	klog.Infof("Unschedulable pods count before processing: %v", len(unschedulablePods))

	p.podStatusAggregator.Unschedulable = append([]*apiv1.Pod{}, unschedulablePods...)

	if p.csnNodeReconcilationProcessor != nil {
		// Updates context before processing.
		err := p.csnNodeReconcilationProcessor.Preprocess(context)
		if err != nil {
			return []*apiv1.Pod{}, err
		}
	}

	var err error
	if p.ekvmsProcessor != nil {
		// Updates context before processing.
		err = p.ekvmsProcessor.Preprocess(context)
		if err != nil {
			return []*apiv1.Pod{}, err
		}
	}

	if p.flexAdvisorPodListProcessor != nil {
		_, err = p.flexAdvisorPodListProcessor.Process(context, unschedulablePods)
		if err != nil {
			klog.Errorf("Error while processing Flex Advisor pod list processor. err: %v", err)
		}
	}

	if p.crProcessor != nil {
		unschedulablePods, err = p.crProcessor.Process(context, unschedulablePods)
		if err != nil {
			return []*apiv1.Pod{}, err
		}
		klog.Infof("Unschedulable pods count after capacity request processor: %v", len(unschedulablePods))
	}

	if p.cbPodInjectionProcessor != nil {
		unschedulablePods, err = p.cbPodInjectionProcessor.Process(context, unschedulablePods)
		if err != nil {
			klog.Warningf("Failed to inject capacity buffers pods: %v", err)
		}
		klog.Infof("Unschedulable pods count after capacity buffers pods injection processor: %v", len(unschedulablePods))

		if p.cbFakePodStateObserver != nil {
			p.cbFakePodStateObserver.ObserveInjectedPods(unschedulablePods)
		}
	}

	if p.podInjectionProcessor != nil {
		unschedulablePods, err = p.podInjectionProcessor.Process(context, unschedulablePods)
		if err != nil {
			klog.Warningf("Failed to inject pods: %v", err)
		}
		klog.Infof("Unschedulable pods count after fake pod injection processor: %v", len(unschedulablePods))
	}

	if p.lookaheadPodsInjection != nil {
		unschedulablePods, err = p.lookaheadPodsInjection.Process(context, unschedulablePods)
		if err != nil {
			klog.Warningf("Failed to inject lookahead pods: %v", err)
		}
		klog.Infof("Unschedulable pods count after lookahead pods injection processor: %v", len(unschedulablePods))
	}

	if p.prProcessor != nil {
		unschedulablePods = p.prProcessor.IgnorePodsConsumingProvisioningRequest(context, unschedulablePods)
		klog.Infof("Unschedulable pods count after ProvisioningRequest.IgnorePodsConsumingProvisioningRequest: %v", len(unschedulablePods))
	}

	// TODO(b/519117759): Clean-up this experiment after launch.
	runScaleToZeroLate := p.experimentsManager.DirectLaunchBoolFlag(experiments.ScaleToZeroLateRunFlag)
	if p.scaleToZeroProcessor != nil && !runScaleToZeroLate {
		unschedulablePods, err = p.scaleToZeroProcessor.Process(context, unschedulablePods)
		if err != nil {
			return []*apiv1.Pod{}, err
		}
		klog.Infof("Unschedulable pods count after cluster scale-to-0 processor: %v", len(unschedulablePods))
	}

	// PTS preprocess builds its internal backoff based on unschedulable and unhelpable pods in the same controller.
	// We do that here, before pods could be removed from the list or replaced by other processors (e.g. defrag processor)
	// which could result in premature backoff lifting.
	if p.podTopologySpreadProcessor != nil {
		p.podTopologySpreadProcessor.Preprocess(unschedulablePods)
	}

	unschedulablePodsBeforeFiltering := unschedulablePods
	unschedulablePods, err = p.processAndObserve(p.defaultPodListerProcessor, context, unschedulablePods, metrics.NoActionNeeded)
	if err != nil {
		return []*apiv1.Pod{}, err
	}
	klog.Infof("Unschedulable pods count after filtering out schedulable: %v", len(unschedulablePods))

	if p.ccMinCapacityProcessor != nil {
		unschedulablePods, err = p.ccMinCapacityProcessor.Process(context, unschedulablePods)
		if err != nil {
			return []*apiv1.Pod{}, err
		}
		klog.Infof("Unschedulable pods count after compute class min capacity processor: %v", len(unschedulablePods))
	}

	if p.ekvmsProcessor != nil {
		// We need to schedule LA pods before defrag for 2 reasons:
		// 1. Due to "ek-consilidation" plugin which will undo the work of lookahead buffer if it is not aware of schedulde LA pods.
		// 2. If there is always unschedulable pods in defrag step, defrag will run only once per 5 minutes which will decrease its throughput.
		// This is likely not needed in post-v1 of lookahead buffer (as ek-consilidation might get removed and adding LA pods might occur after defrag).
		unschedulablePods, err = p.ekvmsProcessor.ScheduleLookaheadPods(context, unschedulablePods)
		if err != nil {
			klog.Warningf("Failed to schedule lookahead pods: %v", err)
		}
		klog.Infof("Unschedulable pods count after scheduling lookahead pods: %v", len(unschedulablePods))
	}

	if p.ossProvReqPodsInjector != nil {
		unschedulablePods, err = p.ossProvReqPodsInjector.Process(context, unschedulablePods)
		if err != nil {
			klog.Warningf("Failed to inject pods for provisioned OSS ProvReqs : %v", err)
		}
		klog.Infof("Unschedulable pods count after OSS ProvReq fake pod injection processor: %v", len(unschedulablePods))
	}

	if p.enforceFakePodsLimitProcessor != nil {
		unschedulablePods, _ = p.enforceFakePodsLimitProcessor.Process(context, unschedulablePods)
		klog.Infof("Unschedulable pods count after enforce fake pod limit processor: %v", len(unschedulablePods))
	}

	if p.prProcessor != nil {
		unschedulablePods, err = p.prProcessor.InjectProvisioningRequestPods(context, unschedulablePods)
		if err != nil {
			return []*apiv1.Pod{}, err
		}
		klog.Infof("Unschedulable pods count after ProvisioningRequest.InjectProvisioningRequestPods: %v", len(unschedulablePods))
	}

	defragPickedCandidate := false
	// We capture defragPickedCandidate locally rather than querying defrag state inside downstream processors.
	// This ensures proper execution order: if a downstream processor relying on this state were moved before defragProcessor, querying the processor directly would return the value in the previous loop which is incorrect. Handling it explicitly here makes the dependency clear and enforces the correct pipeline order.
	if p.defragProcessor != nil {
		defragPods, err := p.defragProcessor.Process(context, unschedulablePods)
		if err != nil {
			klog.Errorf("Defrag processor failed: %v", err)
		} else {
			defragPickedCandidate = p.defragProcessor.DefragPickedCandidate()
			unschedulablePods = defragPods
		}
		klog.Infof("Unschedulable pods count after defrag processor: %v", len(unschedulablePods))
	}

	if p.podTopologySpreadProcessor != nil {
		unschedulablePods, err = p.podTopologySpreadProcessor.Process(context, unschedulablePods)
		if err != nil {
			return []*apiv1.Pod{}, err
		}
		klog.Infof("Unschedulable pods count after pod topology spread processor: %v", len(unschedulablePods))
	}

	if p.ekvmsProcessor != nil {
		unschedulablePodsBeforeEkvmsProcessor := unschedulablePods
		unschedulablePods, err = p.processAndObserve(p.ekvmsProcessor, context, unschedulablePods, metrics.EkUpsize)
		unschedulablePodsAfterEkvmsProcessor := unschedulablePods
		p.ekvmsProcessor.TrackUnschedulablePods(unschedulablePodsBeforeFiltering, unschedulablePodsBeforeEkvmsProcessor, unschedulablePodsAfterEkvmsProcessor)
		if err != nil {
			return []*apiv1.Pod{}, err
		}
		klog.Infof("Unschedulable pods count after EK VMs processor: %v", len(unschedulablePods))
	}

	if p.csnBufferConsumptionProcessor != nil {
		if defragPickedCandidate {
			// If defrag picked a candidate, we don't want to consider any of the unschedulable pods in CSN as we don't want to resume a node that might be a worse candidate.
			// We still run the processor (but without any unschedulable pods) to trigger consumption of chilling CSN nodes where Kubernetes Scheduler scheduled pods on.
			_, err = p.csnBufferConsumptionProcessor.Process(context, nil)
		} else {
			unschedulablePods, err = p.csnBufferConsumptionProcessor.Process(context, unschedulablePods)
		}
		if err != nil {
			return []*apiv1.Pod{}, err
		}
		klog.Infof("Unschedulable pods count after CSN buffer consumption processor: %v", len(unschedulablePods))
	}

	if p.csnCSNPodsLifecycleProcessor != nil {
		unschedulablePods, err = p.csnCSNPodsLifecycleProcessor.Process(context, unschedulablePods)
		if err != nil {
			return []*apiv1.Pod{}, err
		}
		klog.Infof("Unschedulable pods count after CSN pods lifecycle processor: %v", len(unschedulablePods))
	}

	if p.capacityBufferMetricsProcessor != nil {
		err := p.capacityBufferMetricsProcessor.ProcessMetrics(context, unschedulablePods)
		if err != nil {
			klog.Errorf("Failed to process capacity buffer metrics: %v", err)
		}
	}

	if p.scaleToZeroProcessor != nil && runScaleToZeroLate {
		unschedulablePods, err = p.scaleToZeroProcessor.Process(context, unschedulablePods)
		if err != nil {
			return []*apiv1.Pod{}, err
		}
		klog.Infof("Unschedulable pods count after cluster scale-to-0 processor: %v", len(unschedulablePods))
	}

	if p.podShardingProcessor != nil {
		unschedulablePods, err = p.podShardingProcessor.Process(context, unschedulablePods)
		if err != nil {
			return []*apiv1.Pod{}, err
		}
		klog.Infof("Unschedulable pods count after pod sharding: %v", len(unschedulablePods))
	}

	return unschedulablePods, nil
}

func (p *GkeInternalPodListProcessor) processAndObserve(processor pods.PodListProcessor, context *context.AutoscalingContext, unschedulablePods []*apiv1.Pod, reactionType metrics.ReactionType) ([]*apiv1.Pod, error) {

	withoutSchedulable, err := processor.Process(context, unschedulablePods)
	if err != nil {
		return []*apiv1.Pod{}, err
	}

	if p.podStateObserver != nil || p.cbFakePodStateObserver != nil {
		schedulablePods := getSchedulable(unschedulablePods, withoutSchedulable)
		if p.podStateObserver != nil {
			p.podStateObserver.ObserveReaction(schedulablePods, reactionType)
		}
		if p.cbFakePodStateObserver != nil {
			p.cbFakePodStateObserver.ObserveSchedulablePods(schedulablePods)
		}
	}

	return withoutSchedulable, nil
}

// CleanUp cleans up all internal PodListProcessor.
func (p *GkeInternalPodListProcessor) CleanUp() {
	if p.cbFakePodStateObserver != nil {
		p.cbFakePodStateObserver.CleanUp()
	}
	if p.flexAdvisorPodListProcessor != nil {
		p.flexAdvisorPodListProcessor.CleanUp()
	}
	if p.crProcessor != nil {
		p.crProcessor.CleanUp()
	}
	if p.cbPodInjectionProcessor != nil {
		p.cbPodInjectionProcessor.CleanUp()
	}
	if p.podInjectionProcessor != nil {
		p.podInjectionProcessor.CleanUp()
	}
	if p.prProcessor != nil {
		p.prProcessor.CleanUp()
	}
	p.defaultPodListerProcessor.CleanUp()
	if p.podStateObserver != nil {
		p.podStateObserver.CleanUp()
	}
	if p.ccMinCapacityProcessor != nil {
		p.ccMinCapacityProcessor.CleanUp()
	}
	if p.ossProvReqPodsInjector != nil {
		p.ossProvReqPodsInjector.CleanUp()
	}
	if p.enforceFakePodsLimitProcessor != nil {
		p.enforceFakePodsLimitProcessor.CleanUp()
	}
	if p.defragProcessor != nil {
		p.defragProcessor.CleanUp()
	}
	if p.podTopologySpreadProcessor != nil {
		p.podTopologySpreadProcessor.CleanUp()
	}
	if p.ekvmsProcessor != nil {
		p.ekvmsProcessor.CleanUp()
	}
	if p.csnBufferConsumptionProcessor != nil {
		p.csnBufferConsumptionProcessor.CleanUp()
	}
	if p.capacityBufferMetricsProcessor != nil {
		p.capacityBufferMetricsProcessor.CleanUp()
	}
	if p.podShardingProcessor != nil {
		p.podShardingProcessor.CleanUp()
	}
}

type podKey struct {
	UID  types.UID
	Name string
}

func getSchedulable(unschedulableBefore, unschedulableAfter []*apiv1.Pod) []*apiv1.Pod {
	schedulable := []*apiv1.Pod{}
	afterKeys := make(map[podKey]struct{}, len(unschedulableAfter))

	for _, pod := range unschedulableAfter {
		key := podKey{UID: pod.UID, Name: pod.Name}
		afterKeys[key] = struct{}{}
	}

	for _, pod := range unschedulableBefore {
		key := podKey{UID: pod.UID, Name: pod.Name}
		if _, found := afterKeys[key]; !found {
			schedulable = append(schedulable, pod)
		}
	}
	return schedulable
}
