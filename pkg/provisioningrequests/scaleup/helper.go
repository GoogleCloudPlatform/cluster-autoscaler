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

package scaleup

import (
	"strings"
	"sync"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
)

type scaleUpState struct {
	mu                     sync.Mutex
	scaleUpInfos           []nodegroupset.ScaleUpInfo
	podsAwaitEvaluation    []*apiv1.Pod
	failedResizeNodeGroups []cloudprovider.NodeGroup
	autoscalerErrors       []errors.AutoscalerError
	scaleUpsMade           map[string]int
}

func newScaleUpState() *scaleUpState {
	return &scaleUpState{
		scaleUpsMade: make(map[string]int),
	}
}

func (s *scaleUpState) appendAutoscalerErrors(errs ...errors.AutoscalerError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.autoscalerErrors = append(s.autoscalerErrors, errs...)
}

func (s *scaleUpState) appendResizeNodeGroups(groups ...cloudprovider.NodeGroup) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedResizeNodeGroups = append(s.failedResizeNodeGroups, groups...)
}

func (s *scaleUpState) appendScaleUpInfos(infos ...nodegroupset.ScaleUpInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scaleUpInfos = append(s.scaleUpInfos, infos...)
}

func (s *scaleUpState) appendPodsAwaitEvaluation(pods ...*apiv1.Pod) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.podsAwaitEvaluation = append(s.podsAwaitEvaluation, pods...)
}

func (s *scaleUpState) registerScaleUp(ngID string, size int) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	previousScaleUps := s.scaleUpsMade[ngID]
	s.scaleUpsMade[ngID] += size
	return previousScaleUps
}

func aggregateAutoscalerErrors(autoscalerErrors []errors.AutoscalerError) errors.AutoscalerError {
	if len(autoscalerErrors) == 0 {
		return errors.NewAutoscalerError(errors.InternalError, "")
	}
	if len(autoscalerErrors) == 1 {
		return autoscalerErrors[0]
	}
	var aggregateMessage strings.Builder
	errIdx := 0
	sameErrorType := true
	for ; errIdx < min(len(autoscalerErrors), autoscalerErrorReturnLimit); errIdx++ {
		aggregateMessage.WriteString(autoscalerErrors[errIdx].Error())
		aggregateMessage.WriteString("; ")
		sameErrorType = sameErrorType && (autoscalerErrors[0].Type() == autoscalerErrors[errIdx].Type())
	}
	if errIdx < len(autoscalerErrors) {
		aggregateMessage.WriteString("...")
	}
	if sameErrorType {
		return errors.NewAutoscalerError(autoscalerErrors[0].Type(), aggregateMessage.String())
	}
	return errors.NewAutoscalerError(errors.InternalError, aggregateMessage.String())
}

func aggregateMigIds(migs []*gke.GkeMig, idLimit int) string {
	var aggregateMessage strings.Builder
	migIdx := 0
	for ; migIdx < min(len(migs), idLimit); migIdx++ {
		aggregateMessage.WriteString(migs[migIdx].Id())
		aggregateMessage.WriteString("; ")
	}
	if migIdx < len(migs) {
		aggregateMessage.WriteString("...")
	}
	return aggregateMessage.String()
}

func optionToProvReqSet(option *CompositeOption) sets.Set[pods.ProvReqID] {
	set := sets.New[pods.ProvReqID]()
	if option == nil {
		return set
	}
	for _, po := range option.partialOptions {
		set.Insert(po.ProvReqID)
	}
	return set
}

func optionsTotalCount(options []*CompositeOption) int {
	result := 0
	for _, option := range options {
		result += option.NodeCount
	}
	return result
}

func committedNodeGroups(migs []*gke.GkeMig) []string {
	result := []string{}
	for _, mig := range migs {
		result = append(result, mig.GceRef().Name)
	}
	return result
}

func committedZones(migs []*gke.GkeMig) []string {
	result := []string{}
	for _, mig := range migs {
		result = append(result, mig.GceRef().Zone)
	}
	return result
}
