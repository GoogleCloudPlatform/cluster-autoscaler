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

package computeclass

import (
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/nodeannotator"
)

const (
	// cccNodeAnnotatotPluginName is the registered name for this node annotator plugin.
	cccNodeAnnotatotPluginName = "CCC-NodeAnnotator"

	// cccDeletedAnnotationValue is the annotation value indicating the CCC
	// referenced by the node's compute class label was not found.
	cccDeletedAnnotationValue = "ccc_deleted"

	// cccScaleUpAnywayAnnotationValue is the annotation value indicating that although no rule
	// matched the node's node group, the corresponding CCC specified that nodes associated
	// with it should still be considered for scale-up.
	cccScaleUpAnywayAnnotationValue = "ccc_scale_up_anyway"

	// noRuleMatchingAnnotationValue is the annotation value indicating that no rule in the
	// corresponding CCC matched the node's node group, and the CCC did not specify
	// ScaleUpAnyway.
	noRuleMatchingAnnotationValue = "ccc_no_rule_matching"
)

// annotationCloudprovider defines the subset of cloudprovider.
type annotationCloudprovider interface {
	NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error)
	// This is only required for the Matcher.
	IsAutopilotEnabled() bool
}

// lister defines the subset of lister.Lister interface.
type annotationCRDLister interface {
	ListCrds() ([]crd.CRD, error)
}

// cccNodeAnnotatorPlugin implements the nodeannotator.Plugin interface.
type cccNodeAnnotatorPlugin struct {
	lister                  annotationCRDLister
	annotationCloudprovider annotationCloudprovider
	matcher                 Matcher
}

// NewCCCNodeAnnotatorPlugin creates a new instance of the cccNodeAnnotatorPlugin.
func NewCCCNodeAnnotatorPlugin(lister lister.Lister, provider annotationCloudprovider) nodeannotator.Plugin {
	return &cccNodeAnnotatorPlugin{
		lister:                  lister,
		annotationCloudprovider: provider,
		matcher:                 NewMatcher(lister, provider),
	}
}

// Name returns the registered name of this plugin.
func (p *cccNodeAnnotatorPlugin) Name() string {
	return cccNodeAnnotatotPluginName
}

// GetAnnotation determines the appropriate annotation for a given node based on
// its associated CCC Configuration).
func (p *cccNodeAnnotatorPlugin) GetAnnotation(node *apiv1.Node) (map[string]string, error) {
	found, cccName := cccNameFromNode(node)
	if !found {
		// No relevant CCC label found, no annotation needed from this plugin.
		return nil, nil
	}

	crds, err := p.lister.ListCrds()
	if err != nil {
		// This likely indicates an issue with the informer/cache.
		// Return error to signal issue, will retry next cycle.
		return nil, fmt.Errorf("failed to list CRDs: %w", err)
	}

	cccExists, ccc := cccExists(crds, cccName)
	if !cccExists {
		// The CRD associated with the node's label doesn't exist (e.g., was deleted).
		return map[string]string{gkelabels.CCCPriorityIndexAnnotationKey: cccDeletedAnnotationValue}, nil
	}

	nodeGroup, err := p.annotationCloudprovider.NodeGroupForNode(node)
	if err != nil {
		// Failed to get NodeGroup for the node. This might be transient or indicate a config issue.
		// Return error to signal issue, will retry next cycle.
		return nil, fmt.Errorf("failed to get NodeGroup for node %s: %w", node.Name, err)
	}

	if nodeGroup == nil {
		// This should not happen.
		return nil, fmt.Errorf("NodeGroup not found for node %s", node.Name)
	}

	mig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		return nil, fmt.Errorf("expected GkeMig; got %+v", mig)
	}

	matches, idx, _ := p.matcher.FirstMatchedRule(nodeGroup, ccc)

	if !matches {
		// No rule in the CRD matched the node's group.
		if ccc.ScaleUpAnyway() {
			// CCC specified ScaleUpAnyway, allows scale-up even without a matching rule.
			return map[string]string{gkelabels.CCCPriorityIndexAnnotationKey: cccScaleUpAnywayAnnotationValue}, nil
		}
		// No rule matched, and ScaleUpAnyway is false.
		// This is a misconfiguration, but may happen (eg. node pool created with CCC label with no rule matching).
		return map[string]string{gkelabels.CCCPriorityIndexAnnotationKey: noRuleMatchingAnnotationValue}, nil
	}

	// A rule matched. Annotate the node with the index (priority) of the first matching rule.
	return map[string]string{gkelabels.CCCPriorityIndexAnnotationKey: fmt.Sprintf("%d", idx)}, nil
}

// cccNameFromNode extracts the value of the ComputeClassLabel from a node's labels.
// It returns true and the name if a compute class label is found.
// It returns false if the node is nil, the label is missing, or the label value
// corresponds to a predefined GKE compute class (which aren't handled by CCCs).
func cccNameFromNode(node *apiv1.Node) (found bool, cccName string) {
	if node == nil || node.Labels == nil {
		return false, ""
	}
	cccName, found = node.Labels[gkelabels.ComputeClassLabel]
	if !found || machinetypes.IsPredefinedComputeClass(cccName) {
		// Label not found OR it's a predefined class (e.g., "Scale-Out"). Ignore these.
		return false, cccName
	}

	return true, cccName
}

// cccExists searches a list of CRDs for one with the given name and the correct label type.
// It returns true and the found CRD if a match occurs, otherwise false and nil.
func cccExists(crds []crd.CRD, cccName string) (bool, crd.CRD) {
	for _, crd := range crds {
		// Ensure we are only considering CRDs for the ComputeClassLabel.
		if crd.Label() != gkelabels.ComputeClassLabel {
			continue
		}
		// Check if the name matches the one from the node label.
		if crd.Name() == cccName {
			return true, crd
		}
	}
	// No matching CRD found in the list.
	return false, nil
}
