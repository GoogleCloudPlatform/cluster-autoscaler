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

package conditions

import (
	"context"
	"fmt"
	"os"

	gceapiv1 "google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"gopkg.in/gcfg.v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	provider_gce "k8s.io/cloud-provider-gcp/providers/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/resourcemanager"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/klog/v2"
)

type crdConditionCheck interface {
	checkCrd(c crd.CRD, migs map[string]*gke.GkeMig) *metav1.Condition
	conditionType() string
}

type ruleConditionCheck interface {
	checkRule(rule rules.Rule) *metav1.Condition
	conditionType() string
}

const resourceManagerEndpoint = "cloudresourcemanager.googleapis.com:443"

type CloudProvider interface {
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

type ReservationProvider interface {
	GetReservation(name, project string) *gceapiv1.Reservation
	PopulateForCrds(crds []crd.CRD)
}

type Evaluator struct {
	provider                CloudProvider
	rsvCache                ReservationProvider
	localSsdProvider        localssdsize.LocalSSDSizeProvider
	tagClient               resourcemanager.TagClient
	lister                  lister.Lister
	matcher                 computeclass.Matcher
	reservationBlocksPuller *reservations.BlocksPuller
	// cache for tag validation
	cache      map[crd.CRD]cccData
	crdChecks  []crdConditionCheck
	ruleChecks []ruleConditionCheck
}

type cccData struct {
	tags map[string]string
}

func newCccData() cccData {
	return cccData{
		tags: make(map[string]string),
	}
}

func NewEvaluator(
	provider CloudProvider,
	reservationsPuller ValidatorReservationsPuller,
	localSsdProvider localssdsize.LocalSSDSizeProvider,
	cloudConfigFile string,
	lister lister.Lister,
	reservationBlocksPuller *reservations.BlocksPuller,
) *Evaluator {
	tagClient, err := createTagClient(cloudConfigFile)
	if err != nil {
		// Fail-open to not block creation of validator.
		klog.Errorf("Failed to create tag client: %v", err)
	}

	matcher := computeclass.NewMatcher(lister, provider)
	rsvCache := NewReservationsCache(reservationsPuller)

	e := &Evaluator{
		provider:                provider,
		rsvCache:                rsvCache,
		localSsdProvider:        localSsdProvider,
		tagClient:               tagClient,
		lister:                  lister,
		matcher:                 matcher,
		reservationBlocksPuller: reservationBlocksPuller,
		cache:                   make(map[crd.CRD]cccData),
	}
	e.setUpConditionCheckers()
	return e
}

func (e *Evaluator) UpdateReservations(crds []crd.CRD) {
	e.rsvCache.PopulateForCrds(crds)
}

func (e *Evaluator) setUpConditionCheckers() {
	e.ruleChecks = []ruleConditionCheck{
		&machineFamilyConfigChecker{provider: e.provider},
		&machineTypeExistenceChecker{provider: e.provider},
		&machineTypeConfigChecker{provider: e.provider},
		&gpuConfigChecker{provider: e.provider},
		&storageConfigChecker{provider: e.provider},
		&reservationConfigChecker{
			rsvCache:                e.rsvCache,
			localSsdProvider:        e.localSsdProvider,
			reservationBlocksPuller: e.reservationBlocksPuller,
			cloudProvider:           e.provider,
		},
		&nodeSystemConfigChecker{provider: e.provider, localSsdProvider: e.localSsdProvider},
		&minCpuPlatformConfigChecker{provider: e.provider},
	}
	e.crdChecks = []crdConditionCheck{
		&noRuleMatchingCheck{matcher: e.matcher},
		&crdLabelNotMatchingCheck{matcher: e.matcher},
		&taintMissingCheck{lister: e.lister, matcher: e.matcher},
		&taintValueNotMatchingCheck{lister: e.lister, matcher: e.matcher},
		&nodepoolWillNeverScaleUpCheck{matcher: e.matcher},
		&multipleCrdTaintsCheck{matcher: e.matcher},
		&napCannotBeEnabledCheck{provider: e.provider},
		&nodePoolNotExistCheck{},
		&locationNotEnabledForAutoprovisioningCheck{provider: e.provider},
		&napDisabledAndNoMatchingNodegroupsCheck{matcher: e.matcher},
		&resourceManagerTagsCheck{tagClient: e.tagClient, provider: e.provider, cache: e.cache},
	}

}

// GetCRDConditions returns conditions evaluated for provided CRD.
func (e *Evaluator) GetCRDConditions(c crd.CRD, migs map[string]*gke.GkeMig) []metav1.Condition {
	var result []metav1.Condition
	seenTypes := make(map[string]bool)
	for _, ch := range e.crdChecks {
		if currentCondition := ch.checkCrd(c, migs); currentCondition != nil && !seenTypes[currentCondition.Type] {
			result = append(result, *currentCondition)
			seenTypes[currentCondition.Type] = true
		}
	}

	if condition := e.validateNodeConfigRule(c); condition != nil && !seenTypes[condition.Type] {
		condition.Type = CrdMisconfiguredCondition
		result = append(result, *condition)
		seenTypes[condition.Type] = true
	}

	return result
}

func (e *Evaluator) validateNodeConfigRule(c crd.CRD) *metav1.Condition {
	for _, rule := range c.Rules() {
		if len(rule.NodePoolNames()) > 0 {
			continue
		}
		for _, check := range e.ruleChecks {
			if condition := check.checkRule(rule); condition != nil {
				condition.Type = CrdMisconfiguredCondition
				return condition
			}
		}
	}
	return nil
}

// GetRuleConditions  returns conditions evaluated for provided CRD.
func (e *Evaluator) GetRuleConditions(rule rules.Rule) []metav1.Condition {
	var result []metav1.Condition
	seenTypes := make(map[string]bool)
	for _, ch := range e.ruleChecks {
		if ruleCondition := ch.checkRule(rule); ruleCondition != nil && !seenTypes[ruleCondition.Type] {
			result = append(result, *ruleCondition)
			seenTypes[ruleCondition.Type] = true
		}
	}
	return result
}

// GetCRDConditionTypes returns a list of condition types present in evaluator crd checks.
func (e *Evaluator) GetCRDConditionTypes() []string {
	var result []string
	alreadyPresent := make(map[string]bool)
	for _, ch := range e.crdChecks {
		if !alreadyPresent[ch.conditionType()] {
			result = append(result, ch.conditionType())
			alreadyPresent[ch.conditionType()] = true
		}
	}
	return result
}

// GetRuleConditionTypes returns a list of condition types present in evaluator rule checks.
func (e *Evaluator) GetRuleConditionTypes() []string {
	var result []string
	alreadyPresent := make(map[string]bool)
	for _, ch := range e.ruleChecks {
		if !alreadyPresent[ch.conditionType()] {
			result = append(result, ch.conditionType())
			alreadyPresent[ch.conditionType()] = true
		}
	}
	return result
}

func createTagClient(config string) (resourcemanager.TagClient, error) {
	// local dev and testing will not have a config file
	if config == "" {
		return nil, nil
	}

	configReader, err := os.Open(config)
	if err != nil {
		return nil, fmt.Errorf("open gce config file %s: %#v", config, err)
	}
	var configFile provider_gce.ConfigFile
	if err := gcfg.ReadInto(&configFile, configReader); err != nil {
		return nil, fmt.Errorf("couldn't read config from file %s: %v", config, err)
	}

	tokenSource := provider_gce.NewAltTokenSource(configFile.Global.TokenURL, configFile.Global.TokenBody)
	conn, err := grpc.NewClient(
		resourceManagerEndpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(nil)),
		grpc.WithPerRPCCredentials(oauth.TokenSource{TokenSource: tokenSource}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc connection: %v", err)
	}

	return resourcemanager.NewTagClient(context.Background(), option.WithGRPCConn(conn))
}
