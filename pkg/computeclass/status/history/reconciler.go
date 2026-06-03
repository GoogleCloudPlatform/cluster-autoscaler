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

package history

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"k8s.io/client-go/tools/cache"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_status "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/klog/v2"
)

// SetupHistoryResetObserver watches for ComputeClass updates and resets scaling
// history if the priorities list changes or CA was restarted.
func SetupHistoryResetObserver(informer cache.SharedIndexInformer, updatesCh chan<- npc_status.UpdateMessage) {
	informer.AddEventHandler(cache.ResourceEventHandlerDetailedFuncs{
		AddFunc: func(obj interface{}, isInInitialList bool) {
			cc, ok := obj.(*cccv1.ComputeClass)
			if !ok {
				return
			}
			// Only reset history on the initial add event during the informer's first sync. This covers CA restarts.
			if isInInitialList {
				klog.V(4).Infof("Observing ComputeClass %s during informer sync for priority history reset", cc.Name)
				crdId := npc_status.CRDId{CRDName: cc.Name, CRDLabel: gkelabels.ComputeClassLabel}
				updatesCh <- npc_status.UpdateMessage{
					Id: crdId,
					Mutate: func(s crd.CRDStatus) {
						s.ResetAllScalingHistories()
					},
				}
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldCC, okOld := oldObj.(*cccv1.ComputeClass)
			newCC, okNew := newObj.(*cccv1.ComputeClass)
			if !okOld || !okNew {
				return
			}

			// Reset only if priorities actually changed
			oldHash, err := hashPriorities(oldCC.Spec.Priorities)
			if err != nil {
				klog.Errorf("Error hashing old priorities for %s: %v", oldCC.Name, err)
				return
			}
			newHash, err := hashPriorities(newCC.Spec.Priorities)
			if err != nil {
				klog.Errorf("Error hashing new priorities for %s: %v", newCC.Name, err)
				return
			}

			if oldHash != newHash {
				klog.V(4).Infof("Observing ComputeClass %s for priority history reset", newCC.Name)
				crdId := npc_status.CRDId{CRDName: newCC.Name, CRDLabel: gkelabels.ComputeClassLabel}
				updatesCh <- npc_status.UpdateMessage{
					Id: crdId,
					Mutate: func(s crd.CRDStatus) {
						s.ResetAllScalingHistories()
					},
				}
			}
		},
	})
}

func hashPriorities(priorities []cccv1.Priority) (string, error) {
	b, err := json.Marshal(priorities)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(b)
	return fmt.Sprintf("%x", hash), nil
}
