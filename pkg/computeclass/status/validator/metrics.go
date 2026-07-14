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

package validator

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
)

const (
	scaleUpAnyway = "ScaleUpAnyway"
	doNotScaleUp  = "DoNotScaleUp"
)

type healthiness struct {
	healthy   int
	unhealthy int
}

type crdConditions struct {
	crd        crd.CRD
	conditions []metav1.Condition
}

func (v *validator) observeMetrics(crds []crd.CRD, healthConditionMap map[crd.CRD]metav1.Condition) {
	v.observeCountMetrics(crds)
	v.observeHealthMetrics(healthConditionMap)
	v.observeRuleCountMetrics(crds)
}

func (v *validator) observeRuleCountMetrics(crds []crd.CRD) {
	var samples []metrics.NpcRuleCountSample

	for _, c := range crds {
		if c.Rules() == nil {
			continue
		}
		for ruleIndex, rule := range c.Rules() {
			var ruleType string
			if len(rule.NodePoolNames()) > 0 {
				ruleType = "Nodepools"
			} else {
				ruleType = "NodeConfig"
			}

			s := metrics.NpcRuleCountSample{
				RuleIndex: ruleIndex,
				RuleType:  ruleType,
				CrdType:   c.CrdType(),
			}
			samples = append(samples, s)
		}
	}
	v.metrics.ObserveNpcRuleCount(samples)
}

func (v *validator) observeCountMetrics(crds []crd.CRD) {
	var samples []metrics.NpcCountSample
	for _, c := range crds {
		whenUnsatisfiable := ""
		if c.ScaleUpAnyway() {
			whenUnsatisfiable = scaleUpAnyway
		} else {
			// There are currently only 2 options in NPC Crds.
			whenUnsatisfiable = doNotScaleUp
		}

		s := metrics.NpcCountSample{
			NapEnabled:        c.AutoprovisioningEnabled(),
			DefragEnabled:     c.OptimizeRulePriority(),
			WhenUnsatisfiable: whenUnsatisfiable,
			CrdType:           c.CrdType(),
		}
		samples = append(samples, s)
	}

	v.metrics.ObserveNpcCount(samples)
}

func (v *validator) observeHealthMetrics(healthConditionMap map[crd.CRD]metav1.Condition) {
	healthinessPerCrdType := make(map[string]*healthiness)

	// Storing healthiness metrics data for every crd type in healthinessPerCrdType map
	for c, healthCondition := range healthConditionMap {
		crdType := c.CrdType()

		if _, found := healthinessPerCrdType[crdType]; !found {
			healthinessPerCrdType[crdType] = &healthiness{}
		}

		if healthCondition.Status == metav1.ConditionTrue {
			healthinessPerCrdType[crdType].healthy += 1
		} else if healthCondition.Status == metav1.ConditionFalse {
			healthinessPerCrdType[crdType].unhealthy += 1
		} else {
			klog.Errorf("health condition with unexpected status %v in crd %s/%s, omitting this crd from health metrics", healthCondition.Status, c.Label(), c.Name())
		}
	}

	// Sending stored data from  healthinessPerCrdType map to metrics
	for crdType, healthinessValues := range healthinessPerCrdType {
		v.metrics.ObserveNpcHealth(crdType, healthinessValues.healthy, healthinessValues.unhealthy)
	}
}

// Observes detailed info about unhealthy CRDs
func (v *validator) observeUnhealthinessConditionsMetrics(unhealthinessConditions []crdConditions) {
	var samples []metrics.CrdUnhealthinessConditionSample

	for _, unhealthinessData := range unhealthinessConditions {

		unhealthinessConditions := unhealthinessData.conditions

		if len(unhealthinessConditions) <= 0 {
			klog.Warningf("unhealthy crd %s/%s has no conditions to specify why it is unhealthy, omitting this crd from unhealthiness conditions metrics",
				unhealthinessData.crd.Label(),
				unhealthinessData.crd.Name())
			continue
		}

		for _, unhealthinessCondition := range unhealthinessConditions {
			sample := metrics.CrdUnhealthinessConditionSample{
				Condition: unhealthinessCondition.Type,
				Reason:    unhealthinessCondition.Reason,
				CrdType:   unhealthinessData.crd.CrdType(),
			}
			samples = append(samples, sample)
		}
	}

	v.metrics.ObserveCrdUnhealthinessConditions(samples)
}
