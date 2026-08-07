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
	"context"
	"fmt"
	"time"

	"golang.org/x/exp/slices"
	gceapiv1 "google.golang.org/api/compute/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/client"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/validator/conditions"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/gce/localssdsize"

	"k8s.io/klog/v2"
)

const (
	validationInterval = time.Minute
)

type validatorCloudProvider interface {
	IsNodeAutoprovisioningEnabled() bool
	IsAutopilotEnabled() bool
	GetGkeMigs() []*gke.GkeMig
	GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily
	GetAllZones() ([]string, error)
	GetAutoprovisioningLocations() []string
	GetClusterNetwork() (*gceapiv1.Network, error)
	ValidateMachineTypeConfig(machineType, zone string) error
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

type validatorMetrics interface {
	ObserveNpcHealth(crdType string, healthy, unhealthy int)
	ObserveCrdUnhealthinessConditions(samples []metrics.CrdUnhealthinessConditionSample)
	ObserveNpcCount(samples []metrics.NpcCountSample)
	ObserveNpcRuleCount(samples []metrics.NpcRuleCountSample)
}

// validator validates Crd Objects.
type validator struct {
	provider                   validatorCloudProvider
	client                     client.Client
	lister                     lister.Lister
	metrics                    validatorMetrics
	updatesCh                  chan status.UpdateMessage
	evaluator                  *conditions.Evaluator
	enhancedCrdStatusReporting bool
	experimentsManager         experiments.Manager
}

// NewValidator returns validator.
func NewValidator(
	client client.Client,
	lister lister.Lister,
	provider validatorCloudProvider,
	metrics validatorMetrics,
	reservationsPuller conditions.ValidatorReservationsPuller,
	localSsdSizeProvider localssdsize.LocalSSDSizeProvider,
	reservationBlocksPuller *reservations.BlocksPuller,
	cloudConfigFile string,
	updatesCh chan status.UpdateMessage,
	enhancedCrdStatusReporting bool,
	experimentsManager experiments.Manager,
) (*validator, error) {

	evaluator := conditions.NewEvaluator(provider, reservationsPuller, localSsdSizeProvider, cloudConfigFile, lister, reservationBlocksPuller)

	return &validator{
		provider:                   provider,
		client:                     client,
		lister:                     lister,
		metrics:                    metrics,
		updatesCh:                  updatesCh,
		evaluator:                  evaluator,
		enhancedCrdStatusReporting: enhancedCrdStatusReporting,
		experimentsManager:         experimentsManager,
	}, nil
}

// Run runs the NPC Crd objects validator.
func (v *validator) Run(ctx context.Context) {
	klog.V(0).Infof("Enabling NPC Crd Conditions")
	v.loop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(validationInterval):
			v.loop()
		}
	}
}

func (v *validator) loop() {
	klog.V(4).Infof("Starting NPC Crd Conditions loop")
	startTime := time.Now()

	// Fetch Migs.
	migs := make(map[string]*gke.GkeMig)
	for _, mig := range v.provider.GetGkeMigs() {
		migs[mig.NodePoolName()] = mig
	}

	// List Crds
	crds, err := v.lister.ListCrds()
	if err != nil {
		klog.Errorf("Failed to list CRDs: %v", err)
		return
	}

	v.evaluator.UpdateReservations(crds)

	var unhealthinessConditions []crdConditions
	healthConditionMap := make(map[crd.CRD]metav1.Condition)
	for _, c := range crds {
		evalConditions := v.evaluator.GetCRDConditions(c, migs)
		healthCondition := v.healthCondition(evalConditions)

		allConditionsWithoutHealth := append(v.getCRDConditionsAddedByOtherComponent(c.Conditions()), evalConditions...)

		// In case the crd is unhealthy we store its conditions
		// into unhealthinessConditions slice for further metrics
		// observability
		if healthCondition.Status == metav1.ConditionFalse {
			unhealthinessConditions = append(unhealthinessConditions,
				crdConditions{
					crd:        c,
					conditions: allConditionsWithoutHealth,
				})
		}

		allConditions := append(allConditionsWithoutHealth, healthCondition)
		healthConditionMap[c] = healthCondition

		if v.anyConditionsChanged(c, allConditions) {
			if v.updatesCh != nil {
				status.TrySendUpdate(v.updatesCh, status.UpdateMessage{
					Id: status.CRDId{
						CRDName:  c.Name(),
						CRDLabel: c.Label(),
					},
					Mutate: func(s crd.CRDStatus) {
						otherConditions := v.getCRDConditionsAddedByOtherComponent(s.GetConditions())
						newConditions := append(otherConditions, evalConditions...)
						newConditions = append(newConditions, healthCondition)
						s.UpdateConditions(newConditions)
					},
				})
			} else {
				if err := c.UpdateConditions(v.client, allConditions); err != nil {
					klog.Errorf("Cannot update status of Crd: %v:%v, err: %v", c.Label(), c.Name(), err)
				}
			}
		}
		v.validateRules(c)
	}

	v.observeUnhealthinessConditionsMetrics(unhealthinessConditions)
	v.observeMetrics(crds, healthConditionMap)
	klog.V(5).Infof("NPC Crd Conditions loop: allCrds=%d, allMigs=%d, duration=%v", len(crds), len(migs), time.Since(startTime))
}

func (v *validator) validateRules(c crd.CRD) {
	if !v.enhancedCrdStatusReporting || !computeclass.IsComputeClassEnhancedObservabilityEnabled(v.experimentsManager) {
		// Rule specific conditions are not reported when enhanced reporting or experiment is disabled.
		return
	}
	for idx, rule := range c.Rules() {
		conditions := v.evaluator.GetRuleConditions(rule)
		ruleIdx := fmt.Sprintf("%d", idx)
		if v.updatesCh != nil && crd.AnyConditionsChanged(c.GetRuleCondition(ruleIdx), conditions, nil) {
			status.TrySendRuleUpdate(v.updatesCh, status.UpdateMessage{
				Id: status.CRDId{
					CRDName:  c.Name(),
					CRDLabel: c.Label(),
				},
				Mutate: func(s crd.CRDStatus) {
					otherRuleConditions := v.getRuleConditionsAddedByOtherComponent(s.GetRuleConditions(ruleIdx))
					newRuleConditions := append(otherRuleConditions, conditions...)
					s.UpdateRuleConditions(ruleIdx, newRuleConditions)
				},
			}, ruleIdx)
		}
	}
}

// healthCondition returns Crd health condition.
func (v *validator) healthCondition(newConditions []metav1.Condition) metav1.Condition {
	// Exclude noRuleMatchingType from Crd health check because nodepools not
	// matching to any PR in Crd are still allowed for scale up, although not prioritized.
	excludedReasonTypesForHealthy := []string{conditions.NoRuleMatchingReason}
	for _, condition := range newConditions {
		if slices.Contains(excludedReasonTypesForHealthy, condition.Reason) {
			continue
		}
		if condition.Status == metav1.ConditionTrue {
			return *conditions.CrdNotHealthyCondition()
		}
	}
	return *conditions.CrdHealthyCondition()
}

// anyConditionsChanged checks if there were any changes to the existing Crd conditions.
func (v *validator) anyConditionsChanged(c crd.CRD, newConditions []metav1.Condition) bool {
	changed := crd.AnyConditionsChanged(c.Conditions(), newConditions, nil)
	if changed {
		klog.V(5).Infof("CRD %q with label %q has condition changes", c.Name(), c.Label())
	}
	return changed
}

// getCRDConditionsAddedByOtherComponent gets CRD conditions previously added by other components.
func (v *validator) getCRDConditionsAddedByOtherComponent(allConditions []metav1.Condition) []metav1.Condition {
	return getConditionsAddedByOtherComponent(allConditions, append(v.evaluator.GetCRDConditionTypes(), conditions.HealthCondition))
}

// getRuleConditionsAddedByOtherComponent gets rule conditions previously added by other components.
func (v *validator) getRuleConditionsAddedByOtherComponent(allConditions []metav1.Condition) []metav1.Condition {
	return getConditionsAddedByOtherComponent(allConditions, v.evaluator.GetRuleConditionTypes())
}

func getConditionsAddedByOtherComponent(currentConditions []metav1.Condition, validatorConditionTypes []string) []metav1.Condition {
	var otherConditions []metav1.Condition
	for _, condition := range currentConditions {
		if !slices.Contains(validatorConditionTypes, condition.Type) {
			otherConditions = append(otherConditions, condition)
		}
	}
	return otherConditions
}
