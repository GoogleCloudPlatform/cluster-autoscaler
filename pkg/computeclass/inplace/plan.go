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

// Package inplace reconciles ComputeClass drift that the GKE Node Pool API can
// apply to existing nodes without recreating them.
package inplace

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"maps"
	"slices"
	"strings"

	apiv1 "k8s.io/api/core/v1"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

const networkTagsSelfServicePrefix = "network-tags.cloud.google.com/"

// Plan describes an in-place update of a single node pool from its currently
// reported specification to a desired specification.
//
// The live drift detector-trigger builds a Plan against the specification
// reported by GKE and submits it to the operation manager. Plan carries full
// NodePoolSpec values so additional live-updatable categories (such as M2
// kubelet and Linux node configuration) require no changes to the struct.
type Plan struct {
	// NodePoolName is the name of the target GKE node pool.
	NodePoolName string
	// ComputeClassName is used for observability and Kubernetes events; empty
	// for synthetic debug plans.
	ComputeClassName string
	// CurrentSpec is the specification GKE reported when the plan was built.
	CurrentSpec *gkeclient.NodePoolSpec
	// DesiredSpec is the specification the node pool converges to once the
	// in-place update completes.
	DesiredSpec *gkeclient.NodePoolSpec
}

// Hash returns a short, deterministic identifier for the live-updatable target
// state of the plan, independent of map iteration order, taint slice order,
// ComputeClassName, or CurrentSpec.
//
// The operation manager keys backoff and repeat spacing by Hash so that a
// changed ComputeClass target gets a fresh attempt immediately while repeated
// submissions of the same target keep backing off or spacing out.
//
// Currently covers M1 metadata categories (labels, taints, architecture taint
// behavior, network tags, and resource manager tags).
//
// TODO(b/586886749): Include live-updatable KubeletConfig and LinuxNodeConfig
// fields alongside Satisfied when M2 nodeSystemConfig support lands.
func (p *Plan) Hash() string {
	if p == nil {
		return ""
	}
	payload := canonicalPlan{NodePool: p.NodePoolName}
	if p.DesiredSpec != nil {
		rmTags, _ := resourceManagerTags(p.DesiredSpec)
		payload.Labels = p.DesiredSpec.Labels
		payload.Taints = canonicalTaints(p.DesiredSpec.Taints)
		payload.ArchTaintBehavior = p.DesiredSpec.ArchTaintBehavior
		payload.NetworkTags = networkTags(p.DesiredSpec)
		payload.ResourceManagerTags = rmTags
	}
	// encoding/json writes map keys in sorted order, so the encoding is
	// canonical.
	encoded, err := json.Marshal(payload)
	if err != nil {
		// Cannot happen for the string/slice/map fields above; degrade safely.
		return p.NodePoolName
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:8])
}

// Satisfied reports whether the specification reported by GKE reflects all
// live-updatable fields that the plan converges to, i.e. whether the update
// has been committed and observed in GKE's node pool state.
//
// Matching mirrors ComputeClass matching: labels, taints, and network tags use
// superset semantics, while resource manager tags require exact map equality
// when present in DesiredSpec (matching matcher.MatchesCrdConfig).
//
// Currently covers M1 metadata categories.
//
// TODO(b/586886749): Check live-updatable KubeletConfig and LinuxNodeConfig
// fields alongside Hash when M2 nodeSystemConfig support lands.
func (p *Plan) Satisfied(reported *gkeclient.NodePoolSpec) bool {
	if p == nil || p.DesiredSpec == nil || reported == nil {
		return false
	}
	return kubernetesMetadataSatisfied(p.DesiredSpec, reported) &&
		cloudMetadataSatisfied(p.DesiredSpec, reported)
}

func kubernetesMetadataSatisfied(desired, reported *gkeclient.NodePoolSpec) bool {
	for key, want := range desired.Labels {
		got, ok := reported.Labels[key]
		if !ok || got != want {
			return false
		}
	}
	for _, taint := range desired.Taints {
		if !containsTaint(reported.Taints, taint) {
			return false
		}
	}
	if desired.ArchTaintBehavior != "" {
		wantArch := effectiveArchTaintBehavior(desired.ArchTaintBehavior)
		gotArch := effectiveArchTaintBehavior(reported.ArchTaintBehavior)
		if gotArch != wantArch {
			return false
		}
	}
	return true
}

func cloudMetadataSatisfied(desired, reported *gkeclient.NodePoolSpec) bool {
	for _, tag := range networkTags(desired) {
		if reported.SelfServiceMetadata[networkTagsSelfServicePrefix+tag] != "true" {
			return false
		}
	}
	wantRMTags, wantValid := resourceManagerTags(desired)
	if !wantValid {
		return false
	}
	if len(wantRMTags) > 0 {
		gotRMTags, gotValid := resourceManagerTags(reported)
		if !gotValid || !maps.Equal(gotRMTags, wantRMTags) {
			return false
		}
	}
	return true
}

type canonicalPlan struct {
	NodePool            string            `json:"nodePool"`
	Labels              map[string]string `json:"labels,omitempty"`
	Taints              []canonicalTaint  `json:"taints,omitempty"`
	ArchTaintBehavior   string            `json:"archTaintBehavior,omitempty"`
	NetworkTags         []string          `json:"networkTags,omitempty"`
	ResourceManagerTags map[string]string `json:"resourceManagerTags,omitempty"`
}

type canonicalTaint struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect,omitempty"`
}

func canonicalTaints(taints []apiv1.Taint) []canonicalTaint {
	if len(taints) == 0 {
		return nil
	}
	result := make([]canonicalTaint, 0, len(taints))
	for _, t := range taints {
		result = append(result, canonicalTaint{
			Key:    t.Key,
			Value:  t.Value,
			Effect: string(t.Effect),
		})
	}
	slices.SortFunc(result, func(a, b canonicalTaint) int {
		return cmp.Or(
			cmp.Compare(a.Key, b.Key),
			cmp.Compare(a.Value, b.Value),
			cmp.Compare(a.Effect, b.Effect),
		)
	})
	return slices.Compact(result)
}

func networkTags(spec *gkeclient.NodePoolSpec) []string {
	if spec == nil {
		return nil
	}
	var tags []string
	for key, val := range spec.SelfServiceMetadata {
		if val != "true" {
			continue
		}
		if tag, ok := strings.CutPrefix(key, networkTagsSelfServicePrefix); ok && tag != "" {
			tags = append(tags, tag)
		}
	}
	slices.Sort(tags)
	return slices.Compact(tags)
}

// resourceManagerTags parses the JSON-encoded resource manager tags stored in
// spec's self-service metadata. It reports false if the JSON is malformed.
func resourceManagerTags(spec *gkeclient.NodePoolSpec) (map[string]string, bool) {
	if spec == nil {
		return nil, true
	}
	raw, found := spec.SelfServiceMetadata[gkelabels.TagsLabelKey]
	if !found || raw == "" {
		return nil, true
	}
	var tags map[string]string
	if err := json.Unmarshal([]byte(raw), &tags); err != nil {
		return nil, false
	}
	if len(tags) == 0 {
		return nil, true
	}
	return tags, true
}

func containsTaint(taints []apiv1.Taint, want apiv1.Taint) bool {
	for _, candidate := range taints {
		if candidate.Key == want.Key && candidate.Value == want.Value && candidate.Effect == want.Effect {
			return true
		}
	}
	return false
}

func effectiveArchTaintBehavior(behavior string) string {
	if behavior == "" {
		return gkeclient.DefaultArchTaintBehavior
	}
	return behavior
}
