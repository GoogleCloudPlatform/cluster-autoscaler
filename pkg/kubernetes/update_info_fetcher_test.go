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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/apis/nodemanagement.gke.io/v1alpha1"
	clock "k8s.io/utils/clock/testing"
)

func TestValidateUpdateInfo(t *testing.T) {
	testStartTime := time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC)
	testClock := clock.NewFakeClock(testStartTime)
	wrongTypeUpdateInfo := &v1alpha1.UpdateInfo{
		ObjectMeta: v1.ObjectMeta{
			Name:      "n5",
			Namespace: "kube-system",
		},
		Spec: v1alpha1.UpdateInfoSpec{
			SurgeNode:  "",
			TargetNode: "n5",
			Type:       "wrong",
			ValidUntil: v1.Time{Time: testStartTime.Add(time.Hour)},
		},
	}
	assert.Error(t, validateUpdateInfo(wrongTypeUpdateInfo, testClock))

	surgeWithRepairUpdateInfo := &v1alpha1.UpdateInfo{
		ObjectMeta: v1.ObjectMeta{
			Name:      "n5",
			Namespace: "kube-system",
		},
		Spec: v1alpha1.UpdateInfoSpec{
			SurgeNode:  "wrong",
			TargetNode: "n5",
			Type:       RepairType,
			ValidUntil: v1.Time{Time: testStartTime.Add(time.Hour)},
		},
	}
	assert.Error(t, validateUpdateInfo(surgeWithRepairUpdateInfo, testClock))

	validUpdateInfo := &v1alpha1.UpdateInfo{
		ObjectMeta: v1.ObjectMeta{
			Name:      "n3n4",
			Namespace: "kube-system",
		},
		Spec: v1alpha1.UpdateInfoSpec{
			SurgeNode:  "n3",
			TargetNode: "n4",
			Type:       UpgradeType,
			ValidUntil: v1.Time{Time: testStartTime.Add(time.Hour)},
		},
	}
	assert.NoError(t, validateUpdateInfo(validUpdateInfo, testClock))

	expiredUpdateInfo := &v1alpha1.UpdateInfo{
		ObjectMeta: v1.ObjectMeta{
			Name:      "n3n4",
			Namespace: "kube-system",
		},
		Spec: v1alpha1.UpdateInfoSpec{
			SurgeNode:  "n3",
			TargetNode: "n4",
			Type:       UpgradeType,
			ValidUntil: v1.Time{Time: testStartTime.Add(-time.Hour)},
		},
	}
	assert.Error(t, validateUpdateInfo(expiredUpdateInfo, testClock))
}
