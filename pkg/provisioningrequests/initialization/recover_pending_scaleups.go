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

package initialization

import (
	"time"

	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/utils/klogx"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/logging"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
	klog "k8s.io/klog/v2"
)

type gkeCloudProvider interface {
	GetGkeMigs() []*gke.GkeMig
}

// RecoverPendingScaleUps goes though all migs with queued provisioning enabled,
// if it contains a Accepted Resize Request a pending scale up is logged with a most recent RRs time.
func RecoverPendingScaleUps(context *ca_context.AutoscalingContext, cloudProvider gkeCloudProvider) gke.InitializationFunc {
	return func() error {
		loggingQuota := logging.NodeGroupLoggingQuota()
		multiError := utils.NewMultiErr(10) // Catch first ten errors.
		for _, gkeMig := range cloudProvider.GetGkeMigs() {
			if !gkeMig.QueuedProvisioning() {
				continue
			}

			resizeRequests, err := gkeMig.ResizeRequests()
			if err != nil {
				klog.Errorf("Received erorr while recovering a pending queued scale-up in mig %q: %v", gkeMig.Id(), err)
				multiError.Append(err)
				continue
			}

			queuedNodes := 0
			latestTime := time.Time{}
			for _, rr := range resizeRequests {
				if rr.State != resizerequestclient.ResizeRequestStateAccepted {
					continue
				}
				if latestTime.Before(rr.CreationTime) {
					latestTime = rr.CreationTime
				}
				queuedNodes += int(rr.ResizeBy)
			}

			if queuedNodes > 0 {
				klogx.V(1).UpTo(loggingQuota).Infof("Recovering a pending queued scale-up in mig %q for %d nodes, last of which was triggered at %q", gkeMig.Id(), queuedNodes, latestTime.String())
				context.ClusterStateRegistry.RegisterScaleUp(gkeMig, queuedNodes, latestTime)
			}
		}
		klogx.V(1).Over(loggingQuota).Infof("There are also %v other node groups for which a scale up was recovered", -loggingQuota.Left())
		return multiError.ErrorOrNil()
	}
}
