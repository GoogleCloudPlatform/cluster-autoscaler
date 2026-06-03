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

package filter

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
)

func TestSimpleStockout(t *testing.T) {
	f := NewMetricsFilter()

	pods := getPods("p1", "p2", "p3")

	now := time.Now()
	f.ObserveScaleUp(pods, []string{"ng1", "ng2", "ng3"}, now)
	stockOutPods := f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng1")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng2")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng3")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), len(pods))
}

func TestMultipleScaleUpsStockout(t *testing.T) {
	f := NewMetricsFilter()

	pods := getPods("p1", "p2", "p3")

	now := time.Now()
	f.ObserveScaleUp(pods, []string{"ng1", "ng2", "ng3"}, now)
	stockOutPods := f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng1")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng2")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng3")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), len(pods))

	f.ObserveScaleUp(pods, []string{"ng4", "ng5"}, now.Add(time.Minute))
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), len(pods))
}

func TestMultipleScaleUpsNotAllNodeGroupsStockout(t *testing.T) {
	f := NewMetricsFilter()

	pods := getPods("p1", "p2", "p3")

	now := time.Now()
	f.ObserveScaleUp(pods, []string{"ng1", "ng2", "ng3"}, now)
	stockOutPods := f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng1")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng2")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	newPods := []*apiv1.Pod{
		BuildTestPod("p1", 1, 1),
		BuildTestPod("p5", 1, 1),
	}

	allPods := getPods("p1", "p2", "p3", "p5")

	f.ObserveScaleUp(newPods, []string{"ng4", "ng5"}, now.Add(time.Minute))
	stockOutPods = f.GetsPodsEncounteringStockOut(allPods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng4")
	stockOutPods = f.GetsPodsEncounteringStockOut(allPods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng5")
	stockOutPods = f.GetsPodsEncounteringStockOut(allPods)
	assert.Equal(t, len(stockOutPods), len(newPods))

	f.ObserveNodeGroupStockOut("ng3")
	stockOutPods = f.GetsPodsEncounteringStockOut(allPods)
	assert.Equal(t, len(stockOutPods), len(allPods))
}

func TestIsPodScaledToZero(t *testing.T) {
	f := NewMetricsFilter()
	pods := getPods("p1", "p2", "p3")
	f.ObserveScaleToZero(nil, nil, nil, false)
	assert.ElementsMatch(t, pods, f.FilterOutPods(pods))
	for _, pod := range pods {
		assert.False(t, f.IsPodScaledToZero(pod.UID))
	}

	f.ObserveScaleToZero(nil, nil, nil, true)
	for _, pod := range pods {
		assert.True(t, f.IsPodScaledToZero(pod.UID))
	}
	assert.Empty(t, f.FilterOutPods(pods))

	f.ObserveScaleToZero(nil, nil, nil, false)
	assert.ElementsMatch(t, pods, f.FilterOutPods(pods))
	for _, pod := range pods {
		assert.False(t, f.IsPodScaledToZero(pod.UID))
	}
}

func TestSimpleFilterableIssues(t *testing.T) {
	f := NewMetricsFilter()

	pods := getPods("p1", "p2", "p3")

	now := time.Now()
	f.ObserveScaleUp(pods, []string{"ng1", "ng2", "ng3"}, now)
	filteredOutPods := f.FilterOutPods(pods)
	assert.Equal(t, len(filteredOutPods), len(pods))

	f.ObserveNodeGroupFilterableIssue("ng1")
	filteredOutPods = f.FilterOutPods(pods)
	assert.Equal(t, len(filteredOutPods), 0)

	f.ObserveNodeGroupFilterableIssue("ng2")
	filteredOutPods = f.FilterOutPods(pods)
	assert.Equal(t, len(filteredOutPods), 0)

	f.ObserveNodeGroupFilterableIssue("ng3")
	filteredOutPods = f.FilterOutPods(pods)
	assert.Equal(t, len(filteredOutPods), 0)
}

func TestMultipleSucceedingScaleUpFilterableIssue(t *testing.T) {
	f := NewMetricsFilter()

	pods := getPods("p1", "p2", "p3")

	now := time.Now()
	f.ObserveScaleUp(pods, []string{"ng1", "ng2", "ng3"}, now)
	filteredOutPods := f.FilterOutPods(pods)
	assert.Equal(t, len(filteredOutPods), len(pods))

	f.ObserveNodeGroupFilterableIssue("ng1")
	filteredOutPods = f.FilterOutPods(pods)
	assert.Equal(t, len(filteredOutPods), 0)

	f.ObserveNodeGroupFilterableIssue("ng2")
	filteredOutPods = f.FilterOutPods(pods)
	assert.Equal(t, len(filteredOutPods), 0)

	f.ObserveNodeGroupFilterableIssue("ng3")
	filteredOutPods = f.FilterOutPods(pods)
	assert.Equal(t, len(filteredOutPods), 0)

	// Pods should still be filtered out after a second scale up
	f.ObserveScaleUp(pods, []string{"ng4", "ng5"}, now.Add(time.Minute))
	filteredOutPods = f.FilterOutPods(pods)
	assert.Equal(t, len(filteredOutPods), 0)
}

func TestSimpleForgetPod(t *testing.T) {
	f := NewMetricsFilter()

	pods := getPods("p1", "p2", "p3")

	now := time.Now()
	f.ObserveScaleUp(pods, []string{"ng1", "ng2", "ng3"}, now)
	stockOutPods := f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng1")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng2")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng3")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), len(pods))

	f.ForgetPod("p1")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), len(pods)-1)
}

func TestForgetPodDuringStockouts(t *testing.T) {
	f := NewMetricsFilter()

	pods := getPods("p1", "p2", "p3")

	now := time.Now()
	f.ObserveScaleUp(pods, []string{"ng1", "ng2", "ng3"}, now)
	stockOutPods := f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng1")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng2")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ForgetPod("p1")

	f.ObserveNodeGroupStockOut("ng3")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), len(pods)-1)

	f.ForgetPod("p2")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), len(pods)-2)
}

func TestMultipleScaleupsAcrossNodeGroupsForOnePod_Quota(t *testing.T) {
	f := NewMetricsFilter()

	podsSet1 := getPods("p1", "p2", "p3")

	podsSet2 := []*apiv1.Pod{
		BuildTestPod("p1", 1, 1),
	}

	now := time.Now()
	f.ObserveScaleUp(podsSet2, []string{"ng1"}, now)

	f.ObserveScaleUp(podsSet1, []string{"ng2", "ng3", "ng4"}, now.Add(time.Minute))

	// Quota issue in only the first scale up
	f.ObserveNodeGroupFilterableIssue("ng1")
	filtered := f.FilterOutPods(podsSet1)
	assert.Equal(t, len(podsSet1)-1, len(filtered))

	// Quota issue in the second too
	f.ObserveNodeGroupFilterableIssue("ng2")
	filtered = f.FilterOutPods(podsSet1)
	assert.Equal(t, 0, len(filtered))
}

func TestMultipleScaleupsAcrossNodeGroupsForOnePod_Stockout(t *testing.T) {
	f := NewMetricsFilter()

	now := time.Now()
	f.ObserveScaleUp(getPods("p1", "p2"), []string{"ng1", "ng2"}, now)

	f.ObserveScaleUp(getPods("p1", "p3"), []string{"ng2", "ng3"}, now.Add(time.Minute))

	// Stockout in only the first scale up
	f.ObserveNodeGroupStockOut("ng1")
	f.ObserveNodeGroupStockOut("ng2")
	stockout := f.GetsPodsEncounteringStockOut(getPods("p1", "p2"))
	assert.Equal(t, len(getPods("p1", "p2")), len(stockout))

	stockout = f.GetsPodsEncounteringStockOut(getPods("p1", "p3"))
	assert.Equal(t, len(getPods("p1", "p3"))-1, len(stockout))

	// Stockout in the second scale up
	f.ObserveNodeGroupStockOut("ng3")
	stockout = f.GetsPodsEncounteringStockOut(getPods("p1", "p3"))
	assert.Equal(t, len(getPods("p1", "p3")), len(stockout))
}

func TestMetricsFilterImpl_ForgetOldPods(t *testing.T) {
	f := NewMetricsFilter()

	pods := getPods("p1", "p2", "p3")

	now := time.Now()
	f.ObserveScaleUp(pods, []string{"ng1", "ng2", "ng3"}, now)
	stockOutPods := f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng1")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng2")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), 0)

	f.ObserveNodeGroupStockOut("ng3")
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, len(stockOutPods), len(pods))

	// All pods except p1 forgotten
	f.CleanCache([]*apiv1.Pod{BuildTestPod("p1", 1, 1)}, now.Add(2*time.Hour))
	stockOutPods = f.GetsPodsEncounteringStockOut(pods)
	assert.Equal(t, 1, len(stockOutPods))
}

func TestMetricsFilterImpl_MultipleScaleUpAndCleanUp(t *testing.T) {
	f := NewMetricsFilter()

	pods := getPods("p1", "p2", "p3")

	now := time.Now()
	f.ObserveScaleUp(pods, []string{"ng1", "ng2", "ng3"}, now)
	f.ObserveScaleUp(getPods("p4"), []string{"ng1", "ng2"}, now.Add(80*time.Minute))
	f.ObserveScaleUp(getPods("p5"), []string{"ng1", "ng2"}, now.Add(90*time.Minute))

	f.CleanCache([]*apiv1.Pod{}, now.Add(2*time.Hour))
	f.ObserveNodeGroupStockOut("ng1")
	f.ObserveNodeGroupStockOut("ng2")
	f.ObserveNodeGroupStockOut("ng3")
	stockOutPods := f.GetsPodsEncounteringStockOut(getPods("p1", "p2", "p3", "p4", "p5"))
	assert.Equal(t, 2, len(stockOutPods))
}

func TestCleanUp(t *testing.T) {
	f := NewMetricsFilter()

	now := time.Now()
	for i := 0; i < 100; i++ {
		var pods []*apiv1.Pod
		for j := 0; j < 10; j++ {
			pods = append(pods, BuildTestPod(fmt.Sprintf("p%v", i+j), 1, 1))
		}
		f.ObserveScaleUp(pods, []string{fmt.Sprintf("ng%v", i), fmt.Sprintf("ng%v", i+1), fmt.Sprintf("ng%v", i+2)}, now)
	}

	f.CleanCache([]*apiv1.Pod{}, now.Add(2*time.Hour))
	assert.Equal(t, 0, len(f.podToScaleUp))
	assert.Equal(t, 0, len(f.ngToScaleUp))
	assert.Equal(t, 0, len(f.podWentThroughFilterableIssue))
	assert.Equal(t, 0, len(f.podWentThroughStockout))
}

func TestCleanUpScaleUps(t *testing.T) {
	f := NewMetricsFilter()

	now := time.Now()
	for i := 0; i < 50; i++ {
		f.ObserveScaleUp([]*apiv1.Pod{BuildTestPod(fmt.Sprintf("p%v", i), 1, 1)}, []string{fmt.Sprintf("ng%v", i)}, now)
	}

	for i := 50; i < 100; i++ {
		f.ObserveScaleUp([]*apiv1.Pod{BuildTestPod(fmt.Sprintf("p%v", i), 1, 1)}, []string{fmt.Sprintf("ng%v", i)}, now.Add(time.Hour))
	}

	var pods []*apiv1.Pod
	for i := 0; i < 100; i++ {
		pods = append(pods, BuildTestPod(fmt.Sprintf("p%v", i), 1, 1))
	}

	f.CleanCache(pods, now.Add(180*time.Minute))
	assert.Equal(t, 50, len(f.podToScaleUp))
	assert.Equal(t, 50, len(f.ngToScaleUp))
	assert.Equal(t, 0, len(f.podWentThroughFilterableIssue))
	assert.Equal(t, 0, len(f.podWentThroughStockout))
	assert.Equal(t, 50, len(f.scaleUpSeenAt))

	f.CleanCache(pods, now.Add(211*time.Minute))
	assert.Equal(t, 0, len(f.podToScaleUp))
	assert.Equal(t, 0, len(f.ngToScaleUp))
	assert.Equal(t, 0, len(f.podWentThroughFilterableIssue))
	assert.Equal(t, 0, len(f.podWentThroughStockout))
	assert.Equal(t, 0, len(f.scaleUpSeenAt))
}

func getPods(uids ...string) []*apiv1.Pod {
	var pods []*apiv1.Pod
	for _, uid := range uids {
		pods = append(pods, BuildTestPod(uid, 1, 1))
	}
	return pods
}
