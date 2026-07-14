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

package metrics_processors

import (
	"reflect"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	cccMetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/ccc"
)

type labelAndCount struct {
	labels map[string]string
	count  int
}

type labelCounter struct {
	labelsAndCounts []labelAndCount
}

func (c *labelCounter) reset() {
	for i := range c.labelsAndCounts {
		c.labelsAndCounts[i].count = 0
	}
}

func (c *labelCounter) increment(labels map[string]string) {
	found := false
	for i, l := range c.labelsAndCounts {
		if reflect.DeepEqual(l.labels, labels) {
			found = true
			c.labelsAndCounts[i].count++
		}
	}
	if !found {
		n := make(map[string]string)
		for k, v := range labels {
			n[k] = v
		}
		c.labelsAndCounts = append(c.labelsAndCounts, labelAndCount{
			labels: n,
			count:  1,
		})
	}
}

func (c *labelCounter) process(f func(l labelAndCount)) {
	for _, l := range c.labelsAndCounts {
		f(l)
	}
}

type podSchedulableObserver interface {
	observePodUnschedulableDuration(duration time.Duration, labels map[string]string)
	observePodSchedulingDuration(duration time.Duration, stockout, cccName, allocationMode string)
	setLongUnschedulablePodCount(l labelAndCount)
}

type metricsPodSchedulableObserver struct{}

func (m *metricsPodSchedulableObserver) observePodUnschedulableDuration(duration time.Duration, labels map[string]string) {
	metrics.Metrics.ObservePodUnschedulableDuration(duration, labels)
}

func (m *metricsPodSchedulableObserver) observePodSchedulingDuration(duration time.Duration, stockout, cccName, allocationMode string) {
	metrics.Metrics.ObservePodSchedulingDuration(duration, stockout, allocationMode)
	cccMetrics.Metrics.ObservePodSchedulingDuration(duration, stockout, cccName)
}

func (m *metricsPodSchedulableObserver) setLongUnschedulablePodCount(l labelAndCount) {
	metrics.Metrics.SetLongUnschedulablePodCount(float64(l.count), l.labels)
}
