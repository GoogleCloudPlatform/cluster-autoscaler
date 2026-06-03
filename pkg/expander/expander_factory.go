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

package factory

import (
	"strings"

	kube_client "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/expander/factory"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups/asyncnodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	defrag_processor "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/processor"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/edp"
	gkepriceexpander "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/gkeprice"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/mppn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/provider"
	scalabilitytestexpander "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/scalabilitytest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/snowflake"
)

// ExpanderStrategyFromString creates an expander.Strategy according to its name
func ExpanderStrategyFromString(
	expanderFlag string,
	cloudProvider provider.GkeExpanderCloudProvider,
	nodeGroupPenaltyChecker gkepriceexpander.RelaxedNodeGroupPenaltyChecker,
	autoscalingKubeClients *context.AutoscalingKubeClients,
	kubeClient kube_client.Interface,
	defragProcessor *defrag_processor.Processor,
	reservationsPuller *gceclient.ReservationsPuller,
	configNamespace string,
	pvmUnfitnessPenaltyEnabled, autopilotEnabled bool,
	localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider,
	upcomingChecker asyncnodegroups.AsyncNodeGroupStateChecker,
) (expander.Strategy, errors.AutoscalerError) {

	expanderFactory := factory.NewFactory()
	expanderFactory.RegisterDefaultExpanders(cloudProvider, autoscalingKubeClients, kubeClient, configNamespace, "", "")
	expanderFactory.RegisterFilter(internalopts.PriceBasedImprovedExpanderName, func() expander.Filter {
		return createGkePriceExpander(cloudProvider, autoscalingKubeClients, reservationsPuller, nodeGroupPenaltyChecker, pvmUnfitnessPenaltyEnabled, localSSDDiskSizeProvider, upcomingChecker)
	})
	expanderFactory.RegisterFilter(internalopts.ScalabilityTestExpanderName, func() expander.Filter {
		return createScalabilityTestExpander(cloudProvider, autoscalingKubeClients, reservationsPuller, nodeGroupPenaltyChecker, pvmUnfitnessPenaltyEnabled, localSSDDiskSizeProvider, upcomingChecker)
	})
	expanderNames := strings.Split(expanderFlag, ",")
	// Add Defrag Processor as filter if defrag is enabled
	if defragProcessor != nil {
		expanderFactory.RegisterFilter(internalopts.DefragExpanderName, func() expander.Filter { return defragProcessor })
		expanderNames = append([]string{internalopts.DefragExpanderName}, expanderNames...)
	}
	expanderFactory.RegisterFilter(internalopts.SnowflakeExpanderName, func() expander.Filter {
		return snowflake.NewSnowflakeFilter(cloudProvider.IsAutopilotEnabled())
	})
	expanderFactory.RegisterFilter(internalopts.EdpExpanderName, func() expander.Filter {
		return edp.NewEdpFilter(cloudProvider)
	})
	r := gkepriceexpander.NewProgressiveGroupCountReducer(cloudProvider)
	expanderFactory.RegisterFilter(internalopts.MaxPodsPerNodeExpanderName, func() expander.Filter {
		return mppn.NewFilter(r, autopilotEnabled)
	})

	return expanderFactory.Build(expanderNames)
}

type filterStrategy interface {
	expander.Filter
	expander.Strategy
}

func createGkePriceExpander(cloudProvider provider.GkeExpanderCloudProvider, autoscalingKubeClients *context.AutoscalingKubeClients, reservationsPuller *gceclient.ReservationsPuller, penaltyChecker gkepriceexpander.RelaxedNodeGroupPenaltyChecker, pvmUnfitnessPenaltyEnabled bool, localssdDiskSizeProvider localssdsize.LocalSSDSizeProvider, upcomingChecker asyncnodegroups.AsyncNodeGroupStateChecker) filterStrategy {
	gkePriceExpander, err := gkepriceexpander.NewStrategy(cloudProvider, autoscalingKubeClients.AllNodeLister(), autoscalingKubeClients.AllPodLister(), reservationsPuller, penaltyChecker, pvmUnfitnessPenaltyEnabled, localssdDiskSizeProvider, upcomingChecker)
	if err != nil {
		klog.Fatalf("Failed to create %s expander: %v", internalopts.PriceBasedImprovedExpanderName, err)
	}
	klog.V(4).Infof("Using GKE price expander with grouping cluster analyzer and progressive mig count reducer")
	return gkePriceExpander
}

func createScalabilityTestExpander(cloudProvider provider.GkeExpanderCloudProvider, autoscalingKubeClients *context.AutoscalingKubeClients, reservationsPuller *gceclient.ReservationsPuller, penaltyChecker gkepriceexpander.RelaxedNodeGroupPenaltyChecker, pvmUnfitnessPenaltyEnabled bool, localssdDiskSizeProvider localssdsize.LocalSSDSizeProvider, upcomingChecker asyncnodegroups.AsyncNodeGroupStateChecker) expander.Filter {
	klog.V(4).Infof("Using scalability test expander with GKE price expander as the original strategy.")
	return scalabilitytestexpander.NewStrategy(createGkePriceExpander(cloudProvider, autoscalingKubeClients, reservationsPuller, penaltyChecker, pvmUnfitnessPenaltyEnabled, localssdDiskSizeProvider, upcomingChecker))
}
