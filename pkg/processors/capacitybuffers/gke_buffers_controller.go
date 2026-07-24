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

package capacitybuffers

import (
	"context"
	"fmt"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	capacitybuffer "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer"
	cbclient "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/client"
	controller "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/controller"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/fakepods"
	filters "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/filters"
	cbmetrics "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/metrics"
	translators "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/translators"
	updater "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/updater"
	kube_client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/capacitybuffers/accelerators"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/capacitybuffers/billing"
	klog "k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// NewCapacityBufferClientIfCRDPresent creates a CapacityBufferClient if the CapacityBuffer CRD is present in the cluster.
// If the CRD is not present, it logs a warning and returns an error without initializing the client informers.
func NewCapacityBufferClientIfCRDPresent(kubeClient kube_client.Interface, kubeConfig *rest.Config) (*cbclient.CapacityBufferClient, error) {
	crdPresent, err := IsCapacityBufferCRDPresent(kubeClient)
	if err != nil {
		klog.Warningf("Failed to check if CapacityBuffer CRD is present: %v. Capacity Buffers will be disabled.", err)
		return nil, err
	}
	if !crdPresent {
		klog.Warning("CapacityBuffer CRD is not present in the cluster. Capacity Buffers will be disabled.")
		return nil, fmt.Errorf("CapacityBuffer CRD is missing")
	}
	return cbclient.NewCapacityBufferClientFromConfig(kubeConfig)
}

// IsCapacityBufferCRDPresent checks if the CapacityBuffer CRD is registered in the Kubernetes cluster.
func IsCapacityBufferCRDPresent(kubeClient kube_client.Interface) (bool, error) {
	resourceList, err := kubeClient.Discovery().ServerResourcesForGroupVersion("autoscaling.x-k8s.io/v1beta1")
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	for _, resource := range resourceList.APIResources {
		if resource.Name == "capacitybuffers" {
			return true, nil
		}
	}
	return false, nil
}

const (
	ColdProvisioningStrategy = "buffer.gke.io/standby-capacity"
)

func InitializeAndRunBufferController(
	ctx context.Context,
	capacitybufferClient *cbclient.CapacityBufferClient,
	fakePodsResolver fakepods.Resolver,
	cccLister lister.Lister,
	autopilotEnabled bool,
	csnEnabled bool,
) {
	if capacitybufferClient == nil {
		return
	}
	realClock := clock.RealClock{}
	reconciledBuffersCache := cbmetrics.NewReconciliationCache()
	strategies := getGkeBufferStrategies(csnEnabled)
	translatorParts := getGkeBufferTranslatorParts(capacitybufferClient, fakePodsResolver, cccLister, autopilotEnabled, csnEnabled)
	controller := controller.NewBufferController(
		capacitybufferClient,
		filters.NewStrategyFilter(strategies),
		translators.NewCombinedTranslator(translatorParts),
		*updater.NewStatusUpdater(capacitybufferClient),
		realClock,
		reconciledBuffersCache,
	)

	go controller.Run(ctx.Done())
	cbmetrics.RegisterReconciliationTimestampCollector(capacitybufferClient, strategies, reconciledBuffersCache, realClock)
}

func getGkeBufferTranslatorParts(
	client *cbclient.CapacityBufferClient,
	fakePodsResolver fakepods.Resolver,
	cccLister lister.Lister,
	autopilotEnabled bool,
	csnEnabled bool,
) []translators.Translator {
	// The order of translators is important as as the limits translator should be applied after
	// pod template and scalable objects to act and limit number of replicas if needed, also
	// for ComputeClassErrorTranslator to act on ready buffers pod templates and exclude those
	// with pod based billing
	parts := []translators.Translator{
		translators.NewPodTemplateBufferTranslator(client, fakePodsResolver),
		translators.NewDefaultScalableObjectsTranslator(client, fakePodsResolver),
		billing.NewBillingModelTranslator(client, cccLister, autopilotEnabled),
	}

	// Accelerators translator should be applied last, as it can only prevent provisioning
	// This can be moved to be part of the list no matter any checks once CSNs are fully integrated
	// Similarly to ColdProvisioningStrategy in getGkeBufferStrategies
	if csnEnabled {
		parts = append(parts, accelerators.NewAcceleratorsFilteringTranslator(client, ColdProvisioningStrategy))
	}
	return parts
}

func getGkeBufferStrategies(csnEnabled bool) []string {
	strategies := []string{capacitybuffer.ActiveProvisioningStrategy, ""}
	if csnEnabled {
		strategies = append(strategies, ColdProvisioningStrategy)
	}
	return strategies
}
