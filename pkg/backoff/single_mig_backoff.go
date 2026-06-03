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

package backoff

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	base_backoff "k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
	klog "k8s.io/klog/v2"
)

type singleMigBackoff struct {
	base base_backoff.Backoff
}

// SingleMigBackoff creates a singleMigBackoff.
func NewSingleMigBackoff() *singleMigBackoff {
	return &singleMigBackoff{
		base: base_backoff.NewExponentialBackoff(InitialNodeGroupBackoffDuration, MaxNodeGroupBackoffDuration, NodeGroupBackoffResetTimeout, gkeMigBackoffKey),
	}
}

// Backoff enters backoff for node group and its injected Mig if it exists.
func (b *singleMigBackoff) Backoff(
	ng cloudprovider.NodeGroup,
	ni *framework.NodeInfo,
	ei cloudprovider.InstanceErrorInfo,
	ct time.Time,
) time.Time {
	until := b.base.Backoff(ng, ni, ei, ct)
	mig, ok := ng.(*gke.GkeMig)
	if !ok {
		klog.Errorf("Expected GkeMig; got %+v", ng)
		return until
	}
	if iMig := mig.GetInjectedMig(); iMig != nil {
		// Injected migs should be backed off only in the real mig's zone, where the scale-up actually failed.
		iUntil := b.base.Backoff(iMig.ShallowCopyInZone(mig.GceRef().Zone), ni, ei, ct)
		klog.Warningf(
			"Disabling scale-up for injected MIG %v until %v; errorClass=%v; errorCode=%v", mig.Id(), iUntil, ei.ErrorClass, ei.ErrorCode)
	}
	return until
}

// BackoffStatus returns backoff status.
func (b *singleMigBackoff) BackoffStatus(ng cloudprovider.NodeGroup, ni *framework.NodeInfo, ct time.Time) base_backoff.Status {
	return b.base.BackoffStatus(ng, ni, ct)
}

// RemoveBackoff removes backoff.
func (b *singleMigBackoff) RemoveBackoff(ng cloudprovider.NodeGroup, ni *framework.NodeInfo) {
	b.base.RemoveBackoff(ng, ni)
}

// RemoveStaleBackoffData removes stale backoff data.
func (b *singleMigBackoff) RemoveStaleBackoffData(ct time.Time) {
	b.base.RemoveStaleBackoffData(ct)
}

func gkeMigBackoffKey(nodeGroup cloudprovider.NodeGroup) string {
	if nodeGroup.Exist() {
		return nodeGroup.Id()
	}
	mig, ok := nodeGroup.(*gke.GkeMig)
	if !ok || mig.Spec() == nil {
		klog.Errorf("Expected GkeMig with non-nil Spec; got %+v", mig)
		return nodeGroup.Id()
	}

	// By using a canonical string based on mig.Spec() as backoff key,
	// CA we can prevent NAP from creating same group shape over and over
	// even if problem with node group was seen during creation or initial scale-up
	// and not in NAP processor itself.
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("machineType:%s;", mig.Spec().MachineType))
	buffer.WriteString(fmt.Sprintf("labels:%s;", utils.LabelsToCanonicalString(dropUniqueLabels(mig.Spec().Labels))))
	buffer.WriteString(fmt.Sprintf("taints:%s;", utils.TaintsToCanonicalString(mig.Spec().Taints)))
	buffer.WriteString(fmt.Sprintf("extraResources:%s;", extraResourcesToCanonicalString(mig.ExtraResources())))
	buffer.WriteString(fmt.Sprintf("zone:%s", mig.GceRef().Zone))
	return buffer.String()
}

func dropUniqueLabels(l map[string]string) map[string]string {
	copy := map[string]string{}
	for k, v := range l {
		if k != labels.GkeNodePoolLabel {
			copy[k] = v
		}
	}
	return copy
}

func extraResourcesToCanonicalString(extraResources map[string]resource.Quantity) string {
	var buffer bytes.Buffer
	sortedExtraResources := make([]string, len(extraResources))
	i := 0
	for k, v := range extraResources {
		result, exponent := v.AsCanonicalBytes([]byte{})
		sortedExtraResources[i] = fmt.Sprintf("{%s:%se%d}", k, result, exponent)
		i++
	}
	sort.Strings(sortedExtraResources)
	for _, l := range sortedExtraResources {
		buffer.WriteString(l)
	}
	return buffer.String()
}
