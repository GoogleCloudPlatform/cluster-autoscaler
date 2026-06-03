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

package pods

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/pods"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	"k8s.io/klog/v2"
)

const (
	// QueuedTaintKey - key of the taint added to the queued provisioning nodepools.
	// For more see: go/ca-pr-dd
	QueuedTaintKey = "cloud.google.com/gke-queued"
	// QueuedTaintValue - value of the taint added to the queued provisioning nodepools.
	// For more see: go/ca-pr-dd
	QueuedTaintValue = "true"
	// ProvisioningCapacitySearchStrategyAnnotationKey - only used internally in Cluster Autoscaler in order to propagate
	// PCSS from the Provisioning Request to pod sharding
	ProvisioningCapacitySearchStrategyAnnotationKey = "autoscaling.gke.io/provisioning-capacity-search-strategy"
)

type gkeCloudProviderImpl interface {
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

// ProvReqID is a unique identifier of a ProvisioningRequest.
type ProvReqID struct {
	Namespace string
	Name      string
}

func GetProvReqID(pr *provreqwrapper.ProvisioningRequest) ProvReqID {
	return ProvReqID{Namespace: pr.Namespace, Name: pr.Name}
}

// ProvisioningRequestName checks if pod is consuming ProvReq and returns the name of
// the Provisioning Request. If pod is not consuming ProvReq returns false and empty string.
func ProvisioningRequestName(pod *v1.Pod) (string, bool) {
	if pod == nil || pod.Annotations == nil {
		return "", false
	}
	provReqName, found := pod.Annotations[provreqv1.ProvisioningRequestPodAnnotationKey]
	if !found {
		provReqName, found = pod.Annotations[pods.DeprecatedProvisioningRequestPodAnnotationKey]
	}
	return provReqName, found
}

// ProvisioningClassName checks if pod is consuming ProvReq and returns the name of
// the ProvisioningClass. If pod is not consuming ProvReq returns false and empty string.
func ProvisioningClassName(pod *v1.Pod) (string, bool) {
	if pod == nil || pod.Annotations == nil {
		return "", false
	}
	provClass, found := pod.Annotations[provreqv1.ProvisioningClassPodAnnotationKey]
	if !found {
		provClass, found = pod.Annotations[pods.DeprecatedProvisioningClassPodAnnotationKey]
	}

	return provClass, found
}

func ProvisioningCapacitySearchStrategy(pod *v1.Pod) (string, bool) {
	if pod == nil || pod.Annotations == nil {
		return "", false
	}
	pcss, found := pod.Annotations[ProvisioningCapacitySearchStrategyAnnotationKey]
	return pcss, found
}

// PodsForProvisioningRequest returns a list of pods for which Provisioning
// Request needs to provision resources.
// TODO(b/492464739): accept `machineConfigProvider MachineConfigProvider` instead of `cloudprovider.CloudProvider`
func PodsForProvisioningRequest(cloudProvider cloudprovider.CloudProvider, experimentsManager experiments.Manager, pr *provreqwrapper.ProvisioningRequest) ([]*v1.Pod, error) {
	pods, err := pods.PodsForProvisioningRequest(pr)
	if err != nil {
		return nil, err
	}
	for _, pod := range pods {
		PopulatePodToleration(pod)
		clearPodNodeSelector(pod)
		setControllerKind(pod)
		setCapacitySearchStrategyAnnotation(pr, pod)
	}

	if cloudProvider != nil && experimentsManager != nil && len(pods) != 0 && UsesBulkProvisioning(cloudProvider, experimentsManager, pods[0]) {
		mrd, err := queuedwrapper.ToQueuedProvisioningRequest(*pr).MaxRunDurationOrDefaultWithWarning()
		if err != nil {
			return nil, err
		}
		mrdFormatted := fmt.Sprintf("%d", int64(mrd.Seconds()))
		for _, pod := range pods {
			pod.Spec.NodeSelector[gkelabels.MaxRunDurationLabelKey] = mrdFormatted
		}
	}

	return pods, nil
}

func setControllerKind(pod *v1.Pod) {
	ownerRef := metav1.GetControllerOfNoCopy(pod)
	if ownerRef != nil && ownerRef.Kind == "" {
		// ProvisioningRequestClient doesn't populate the Kind. This won't work for OSS PRs - b/393321394 addresses that.
		ownerRef.Kind = "ProvisioningRequest"
	}
}

// InjectedPodProvReqRef returns ObjectReference to ProvisioningRequest if the Pod was injected.
func InjectedPodProvReqRef(pod *v1.Pod) (*v1.ObjectReference, bool) {
	if pod == nil {
		return nil, false
	}
	ownerRef := metav1.GetControllerOfNoCopy(pod)
	if ownerRef == nil || ownerRef.Kind != "ProvisioningRequest" {
		return nil, false
	}

	prRef := &v1.ObjectReference{
		APIVersion: ownerRef.APIVersion,
		Kind:       ownerRef.Kind,
		Name:       ownerRef.Name,
		UID:        ownerRef.UID,
		Namespace:  pod.Namespace,
	}
	return prRef, true
}

func PopulatePodToleration(pod *v1.Pod) {
	queuedToleration := v1.Toleration{
		Key:      QueuedTaintKey,
		Operator: v1.TolerationOpEqual,
		Value:    QueuedTaintValue,
		Effect:   v1.TaintEffectNoSchedule,
	}

	found := false
	for _, t := range pod.Spec.Tolerations {
		if t == queuedToleration {
			found = true
			break
		}
	}
	if !found {
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, queuedToleration)
	}
}

// UsesBulkProvisioning returns whether pod will utilize bulk provisioning
// This helper needs to be kept in sync with cluster-autoscaler/pkg/cloudprovider/gke/gkeclient/autoscaling_gke_client.go's UsesBulkProvisioning
// TODO(b/485538080): move this and NodePoolSpec's UsesBulkProvisioning to single file
func UsesBulkProvisioning(cloudProvider cloudprovider.CloudProvider, experimentsManager experiments.Manager, pod *v1.Pod) bool {
	if !experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ProvisioningRequestBulkMigsFlag, false) {
		return false
	}
	if pod.Spec.NodeSelector == nil || pod.Spec.NodeSelector[gkelabels.GPULabel] == "" || pod.Spec.NodeSelector[gkelabels.PolicyLabel] == "" {
		return false
	}
	if pod.Spec.NodeSelector[gkelabels.FlexStartLabel] != "true" {
		return false
	}

	gkeCloudProvider, ok := cloudProvider.(gkeCloudProviderImpl)
	if !ok {
		klog.Errorf("pods.go UsesBulkProvisioning could not cast cloudProvider to gkeCloudProviderImpl")
		return false
	}
	mf, ok := gkeCloudProvider.MachineConfigProvider().MachineFamilyForGpuType(pod.Spec.NodeSelector[gkelabels.GPULabel])
	if !ok {
		return false
	}
	return mf.IsGpuAcceleratorSliceSupported()
}

// clearPodNodeSelector removes the pod selector for the ProvReq label.
// This node selector is not needed for the CA in-memory simulation as
// CA only tries to schedule the injected pods on the newly created nodes:
// https://github.com/kubernetes/autoscaler/blob/82f85c273d989ae8543a8d0b16c83f61e0da5c49/cluster-autoscaler/estimator/binpacking_estimator.go#L95
// Without this guarantee the ProvReq pods could end-up being scheduled
// on existing nodes and the number of requested nodes could be too low.
func clearPodNodeSelector(pod *v1.Pod) {
	if pod.Spec.NodeSelector == nil {
		return
	}
	delete(pod.Spec.NodeSelector, gkelabels.ProvisioningRequestLabelKey)
}

// only used in order to propagate Provisioning Capacity Search Strategy from ProvReq to podsharding, ignored afterwards
func setCapacitySearchStrategyAnnotation(pr *provreqwrapper.ProvisioningRequest, pod *v1.Pod) {
	if queuedwrapper.ToQueuedProvisioningRequest(*pr).ObtainabilityStrategy() {
		pod.Annotations[ProvisioningCapacitySearchStrategyAnnotationKey] = string(queuedwrapper.CapacitySearchStrategyObtainability)
	}
}

// DoNotScheduleOnDWS schedule on all nodes, except DWS nodes.
func DoNotScheduleOnDWS(n *framework.NodeInfo) bool {
	if n.Node() == nil || n.Node().Spec.Taints == nil {
		return true
	}
	for _, t := range n.Node().Spec.Taints {
		if t.Key == QueuedTaintKey {
			return false
		}
	}
	return true
}
