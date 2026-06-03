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

package test

import (
	"fmt"
	"testing"

	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/testutil"
)

type ValueCheck func(float64) error

func ExpectedValue(val float64) ValueCheck {
	return func(f float64) error {
		if f != val {
			return fmt.Errorf("unexpected metric diff, expected %f, got %f", val, f)
		}
		return nil
	}
}

func Positive() ValueCheck {
	return func(f float64) error {
		if f < 0 {
			return fmt.Errorf("expected metric value %f to be positive", f)
		}
		return nil
	}
}

type MetricVerifier interface {
	Init(t *testing.T)
	Verify(t *testing.T) error
}

type MetricDelta struct {
	metric  k8smetrics.Registerable
	labels  []string
	initial float64
	check   ValueCheck
}

func NewMetricDelta(check ValueCheck, metric k8smetrics.Registerable, labels []string) *MetricDelta {
	return &MetricDelta{
		check:  check,
		metric: metric,
		labels: labels,
	}
}

func (d *MetricDelta) Init(t *testing.T) {
	t.Helper()
	d.initial = mustReadMetric(t, d.metric, d.labels)
}

func (d *MetricDelta) Verify(t *testing.T) error {
	t.Helper()
	return d.check(mustReadMetric(t, d.metric, d.labels) - d.initial)
}

type MetricValue struct {
	metric k8smetrics.Registerable
	labels []string
	check  ValueCheck
}

func NewMetricValue(check ValueCheck, metric k8smetrics.Registerable, labels []string) *MetricValue {
	return &MetricValue{
		check:  check,
		metric: metric,
		labels: labels,
	}
}

func (v *MetricValue) Init(_ *testing.T) {
}

func (v *MetricValue) Verify(t *testing.T) error {
	t.Helper()
	return v.check(mustReadMetric(t, v.metric, v.labels))
}

func mustReadMetric(t *testing.T, metric k8smetrics.Registerable, labels []string) (val float64) {
	t.Helper()
	var err error
	switch m := metric.(type) {
	case *k8smetrics.CounterVec:
		g := m.WithLabelValues(labels...)
		val, err = testutil.GetCounterMetricValue(g)
	case *k8smetrics.GaugeVec:
		g := m.WithLabelValues(labels...)
		val, err = testutil.GetGaugeMetricValue(g)
	case *k8smetrics.Gauge:
		val, err = testutil.GetGaugeMetricValue(m)
	case *k8smetrics.Counter:
		val, err = testutil.GetCounterMetricValue(m)
	case *k8smetrics.HistogramVec:
		g := m.WithLabelValues(labels...)
		val, err = testutil.GetHistogramMetricValue(g)
	default:
		t.Fatalf("Unknown metric type: %T", metric)
		return 0
	}
	if err != nil {
		t.Fatalf("Error when getting metric: %v", err)
	}
	return val
}
