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

package kubernetes

import (
	"fmt"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/apis/nodemanagement.gke.io/v1alpha1"
	clientset "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/listers/nodemanagement.gke.io/v1alpha1"
	klog "k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// UpdateInfoFetcher fetches updateInfo CRDs
type UpdateInfoFetcher interface {
	// GetUpdateInfos gets all UpdateInfo CRDs.
	GetUpdateInfos() ([]*v1alpha1.UpdateInfo, error)
	// GetUpdateInfosForMig gets UpdateInfo CRDs for a particular MIG.
	GetUpdateInfosForMig(migId string) ([]*v1alpha1.UpdateInfo, error)
	// Refresh snapshots CRDs when called
	Refresh() error
}

const (
	// UpgradeType represents an upgrade type of UpdateInfo.
	UpgradeType = "Upgrade"
	// RepairType represents a repair type of UpdateInfo.
	RepairType = "Repair"
)

type updateInfoFetcher struct {
	lister             clientset.UpdateInfoLister
	updateInfos        []*v1alpha1.UpdateInfo
	updateInfosByMigId map[string][]*v1alpha1.UpdateInfo
	refreshSuccessful  bool
	clock              clock.PassiveClock
}

// NewUpdateInfoFetcher configures and returns a updateInfoFetcher.
func NewUpdateInfoFetcher(upgradeInfoInterface clientset.UpdateInfoLister, clock clock.PassiveClock) *updateInfoFetcher {
	return &updateInfoFetcher{
		lister: upgradeInfoInterface,
		clock:  clock,
	}
}

// GetUpdateInfos gets UpdateInfo CRDs.
func (u *updateInfoFetcher) GetUpdateInfos() ([]*v1alpha1.UpdateInfo, error) {
	if !u.refreshSuccessful {
		return nil, fmt.Errorf("last refresh of updateInfoFetcher failed")
	}
	return u.updateInfos, nil
}

// GetUpdateInfosForMig gets UpdateInfo CRDs for a particular MIG.
func (u *updateInfoFetcher) GetUpdateInfosForMig(migId string) ([]*v1alpha1.UpdateInfo, error) {
	if !u.refreshSuccessful {
		return nil, fmt.Errorf("last refresh of updateInfoFetcher failed")
	}

	return u.updateInfosByMigId[migId], nil
}

// Refresh snapshots CRDs when called.
func (u *updateInfoFetcher) Refresh() error {
	if u.lister == nil {
		u.refreshSuccessful = false
		return fmt.Errorf("update info client is nil")
	}
	updateInfos, err := u.lister.List(labels.Everything())
	if err != nil {
		u.refreshSuccessful = false
		return fmt.Errorf("error fetching updateInfos: %v", err)
	}
	if len(updateInfos) > 0 {
		klog.V(1).Infof("fetched %d updateInfo objects", len(updateInfos))
	}

	var validatedUpdateInfos []*v1alpha1.UpdateInfo

	for _, updateInfo := range updateInfos {
		if err := validateUpdateInfo(updateInfo, u.clock); err != nil {
			klog.Warningf("UpdateInfo validation failed: %v", err)
			continue
		}
		validatedUpdateInfos = append(validatedUpdateInfos, updateInfo)

		if updateInfo.Spec.InstanceGroupUrl == "" {
			return fmt.Errorf("all UpdateInfo objects should have Spec.InstanceGroupUrl set. %+v", updateInfo)
		}
	}
	if len(validatedUpdateInfos) > 0 {
		klog.V(4).Infof("%d valid updateInfo objects", len(validatedUpdateInfos))
	}

	u.updateInfos = validatedUpdateInfos
	u.refreshSuccessful = true

	u.updateInfosByMigId = make(map[string][]*v1alpha1.UpdateInfo)
	for _, updateInfo := range validatedUpdateInfos {
		u.updateInfosByMigId[updateInfo.Spec.InstanceGroupUrl] = append(u.updateInfosByMigId[updateInfo.Spec.InstanceGroupUrl], updateInfo)
	}

	return nil
}

func validateUpdateInfo(updateInfo *v1alpha1.UpdateInfo, clock clock.PassiveClock) error {
	if updateInfo == nil {
		return fmt.Errorf("updateInfo is nil")
	}
	if updateInfo.Spec.ValidUntil.Time.Before(clock.Now()) {
		return fmt.Errorf("updateInfo %s expired; ValidUntil %v", updateInfo.Name, updateInfo.Spec.ValidUntil.Time)
	}
	if updateInfo.Spec.Type != RepairType && updateInfo.Spec.Type != UpgradeType {
		return fmt.Errorf("invalid type: %s for %s", updateInfo.Spec.Type, updateInfo.Name)
	}
	if updateInfo.Spec.Type == RepairType && updateInfo.Spec.SurgeNode != "" {
		return fmt.Errorf("type of %s is Repair, but contains non empty surge node: %s", updateInfo.Name, updateInfo.Spec.SurgeNode)
	}
	return nil
}
