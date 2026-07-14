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

package capacityquota

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	cqv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacityquota/autoscaling.x-k8s.io/v1beta1"
)

const errorMsg = "selector contains blocklisted labels: %s"

var (
	defaultBlocklistedLabels = []string{"node.kubernetes.io/instance-type", "beta.kubernetes.io/instance-type"}
	selectorPath             = field.NewPath("spec").Child("selector")
)

// BlocklistedLabelsValidator validates that CapacityQuota does not contain blocklisted labels.
type BlocklistedLabelsValidator struct {
	BlocklistedLabels sets.Set[string]
}

// NewDefaultBlocklistedLabelsValidator returns a new BlocklistedLabelsValidator.
func NewDefaultBlocklistedLabelsValidator() *BlocklistedLabelsValidator {
	return &BlocklistedLabelsValidator{
		BlocklistedLabels: sets.New(defaultBlocklistedLabels...),
	}
}

// Validate validates that CapacityQuota does not contain blocklisted labels.
func (v *BlocklistedLabelsValidator) Validate(_ context.Context, cq *cqv1beta1.CapacityQuota) error {
	if cq.Spec.Selector == nil {
		return nil
	}

	foundBlocklistedLabels := make(sets.Set[string])

	for label := range cq.Spec.Selector.MatchLabels {
		if v.BlocklistedLabels.Has(label) {
			foundBlocklistedLabels.Insert(label)
		}
	}

	for _, req := range cq.Spec.Selector.MatchExpressions {
		if v.BlocklistedLabels.Has(req.Key) {
			foundBlocklistedLabels.Insert(req.Key)
		}
	}

	if len(foundBlocklistedLabels) > 0 {
		return containsBlocklistedLabelsErr(cq, foundBlocklistedLabels)
	}

	return nil
}

func containsBlocklistedLabelsErr(cq *cqv1beta1.CapacityQuota, foundBlocklistedLabels sets.Set[string]) error {
	msg := fmt.Sprintf(errorMsg, strings.Join(foundBlocklistedLabels.UnsortedList(), "; "))
	return field.Invalid(selectorPath, cq.Spec.Selector, msg)
}
