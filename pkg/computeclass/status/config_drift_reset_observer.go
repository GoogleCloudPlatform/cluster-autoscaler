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

package status

import (
	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"k8s.io/client-go/tools/cache"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	computeclass "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

// SetupConfigDriftResetObserver clears the reported config drift migration
// progress of a ComputeClass as soon as its spec changes.
//
// Drift is defined against the ComputeClass configuration, so an edit redefines
// it for every node of that ComputeClass at once, and the counters in the status
// answer a question nobody is asking any more. ConfigDriftReportingProcessor
// recomputes them on its next loop, but the status is flushed on a schedule of
// its own, so without this the numbers describing the previous configuration can
// still be published after the edit that invalidated them. Clearing them means a
// reader sees no answer for a moment rather than a wrong one.
//
// The window is not closed entirely: a loop that was already under way when
// the edit arrived still reports counters computed against the previous
// configuration, which stand until the following loop replaces them.
func SetupConfigDriftResetObserver(informer cache.SharedIndexInformer, updatesCh chan<- UpdateMessage, manager experiments.Manager) {
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj any) {
			if !computeclass.IsComputeClassConfigDriftStatusEnabled(manager) {
				return
			}
			oldCC, okOld := oldObj.(*cccv1.ComputeClass)
			newCC, okNew := newObj.(*cccv1.ComputeClass)
			if !okOld || !okNew {
				return
			}
			// The ComputeClass status is a subresource, so the API server bumps
			// the generation on spec changes only. Informer resyncs and the
			// autoscaler's own status patches therefore cannot trigger a reset,
			// which a hash of the spec fields drift happens to depend on today
			// would also have to get right, and would have to be revisited every
			// time the matcher learns about a new field.
			if oldCC.Generation == newCC.Generation {
				return
			}
			klog.V(4).Infof("ComputeClass %s changed, clearing the reported config drift migration progress", newCC.Name)
			TrySendUpdate(updatesCh, UpdateMessage{
				Id: CRDId{CRDName: newCC.Name, CRDLabel: gkelabels.ComputeClassLabel},
				// The informer is cluster-wide, so in a sharded autoscaler
				// every shard sees the edit, while only the shard reporting on
				// this ComputeClass has counters to clear. The others must not
				// start tracking it, or their next flush would take over the
				// status fields the reporting shard applied.
				OnlyIfTracked: true,
				Mutate: func(status crd.CRDStatus) {
					status.ResetConfigDriftInfo()
				},
			})
		},
	})
}
