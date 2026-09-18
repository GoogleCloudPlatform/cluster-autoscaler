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

package lister

import (
	"maps"
	"slices"

	apiv1 "k8s.io/api/core/v1"
	klog "k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

// FilteredCccLister is a wrapper over Lister that filters CRDs based on
// the autoscaler.gke.io/autoscaler-name annotation matching the current CA instance's --autoscaler-names.
type FilteredCccLister struct {
	baseLister         Lister
	autoscalerNames    []string
	handlesUnannotated bool
}

// NewFilteredCccLister creates a new FilteredCccLister wrapper, or returns base lister i.e. disables filtering if autoscalerNames is empty.
func NewFilteredCccLister(base Lister, autoscalerNames []string) Lister {
	if len(autoscalerNames) == 0 {
		klog.Infof("ComputeClass ownership filtering disabled (no autoscalerNames)")
		return base
	}
	handlesUnannotated := slices.Contains(autoscalerNames, options.UnannotatedAutoscalerName)
	klog.Infof("ComputeClass ownership filtering enabled for autoscalerNames=%v (handles unannotated: %t)", autoscalerNames, handlesUnannotated)
	return &FilteredCccLister{
		baseLister:         base,
		autoscalerNames:    autoscalerNames,
		handlesUnannotated: handlesUnannotated,
	}
}

// IsOwned returns true if a ComputeClass with the given annotations is owned by an
// autoscaler instance configured with autoscalerNames.
func IsOwned(annotations map[string]string, autoscalerNames []string) bool {
	if len(autoscalerNames) == 0 {
		return true
	}
	annotatedName := annotations[options.AutoscalerNameAnnotation]
	if annotatedName == "" {
		annotatedName = options.UnannotatedAutoscalerName
	}
	return slices.Contains(autoscalerNames, annotatedName)
}

// ListCrds returns all CRDs owned by this autoscaler instance.
func (f *FilteredCccLister) ListCrds() ([]crd.CRD, error) {
	crds, err := f.baseLister.ListCrds()
	if err != nil {
		return nil, err
	}
	var filtered []crd.CRD
	for _, c := range crds {
		if c != nil && IsOwned(c.Annotations(), f.autoscalerNames) {
			filtered = append(filtered, c)
		}
	}
	return filtered, nil
}

// GetCrd returns the CRD with the given name if owned by this autoscaler instance.
func (f *FilteredCccLister) GetCrd(name string) (crd.CRD, error) {
	return f.ownedCrd(f.baseLister.GetCrd(name))
}

// CrdAnnotations returns the annotations of the CRD with the given name if owned by this autoscaler instance.
func (f *FilteredCccLister) CrdAnnotations(name string) (map[string]string, error) {
	annotations, err := f.baseLister.CrdAnnotations(name)
	if err != nil {
		return nil, err
	}
	if !IsOwned(annotations, f.autoscalerNames) {
		return nil, nil
	}
	return annotations, nil
}

// Crd returns the CRD associated with the given label and name if owned by this instance.
func (f *FilteredCccLister) Crd(crdLabel string, crdName string) (crd.CRD, error) {
	return f.ownedCrd(f.baseLister.Crd(crdLabel, crdName))
}

// PodReqCrd returns CRD for Requirements if owned by this instance.
func (f *FilteredCccLister) PodReqCrd(req *podrequirements.Requirements) (crd.CRD, string, error) {
	return f.ownedCrdWithName(f.baseLister.PodReqCrd(req))
}

// PodReqCrdType delegates to base lister.
func (f *FilteredCccLister) PodReqCrdType(req *podrequirements.Requirements) (string, error) {
	return f.baseLister.PodReqCrdType(req)
}

// PodCrd returns CRD for Pod if owned by this instance.
func (f *FilteredCccLister) PodCrd(pod *apiv1.Pod) (crd.CRD, string, error) {
	return f.ownedCrdWithName(f.baseLister.PodCrd(pod))
}

// NodeGroupCrd returns CRD for NodeGroup if owned by this instance.
func (f *FilteredCccLister) NodeGroupCrd(nodeGroup cloudprovider.NodeGroup) (crd.CRD, string, error) {
	return f.ownedCrdWithName(f.baseLister.NodeGroupCrd(nodeGroup))
}

type specProvider interface {
	Spec() *gkeclient.NodePoolSpec
}

func (f *FilteredCccLister) isCrdNameOwned(name string) bool {
	if name == "" || machinetypes.IsPredefinedComputeClass(name) {
		return f.handlesUnannotated
	}
	annotations, err := f.baseLister.CrdAnnotations(name)
	if err != nil {
		return f.handlesUnannotated
	}
	return IsOwned(annotations, f.autoscalerNames)
}

// IsNodeGroupOwned returns whether the given NodeGroup is owned by this
// autoscaler instance.
func (f *FilteredCccLister) IsNodeGroupOwned(nodeGroup cloudprovider.NodeGroup) bool {
	var name string
	if mig, ok := nodeGroup.(specProvider); ok && mig != nil && mig.Spec() != nil && mig.Spec().Labels != nil {
		name = mig.Spec().Labels[gkelabels.ComputeClassLabel]
	}
	if name == "" {
		if defaultName, _, found := f.baseLister.Default(); found {
			name = defaultName
		}
	}
	return f.isCrdNameOwned(name)
}

// IsPodOwned returns whether the given Pod is owned by this autoscaler
// instance.
func (f *FilteredCccLister) IsPodOwned(pod *apiv1.Pod) bool {
	if pod == nil {
		return false
	}
	vals, found := podrequirements.GetLabelRequirementsForKey(pod, gkelabels.ComputeClassLabel).GetValues(gkelabels.ComputeClassLabel)
	if !found || vals.IsAny() || len(vals.Get()) == 0 {
		var name string
		if defaultName, _, defaultFound := f.baseLister.Default(); defaultFound {
			name = defaultName
		}
		return f.isCrdNameOwned(name)
	}

	var hasOwned, hasUnowned bool
	for name := range vals.Get() {
		if f.isCrdNameOwned(name) {
			hasOwned = true
		} else {
			hasUnowned = true
		}
		if hasOwned && hasUnowned {
			// Mixed ownership across multi-value In affinity falls back to
			// unannotated handling.
			klog.V(4).Infof("Pod %s/%s requires ComputeClasses %v owned by different autoscalers; falling back to unannotated handling (autoscalerNames=%v, owned=%v)",
				pod.Namespace, pod.Name, slices.Sorted(maps.Keys(vals.Get())), f.autoscalerNames, f.handlesUnannotated)
			return f.handlesUnannotated
		}
	}
	return hasOwned
}

// IsNodeOwned returns whether the given Node is owned by this autoscaler
// instance, based on its ComputeClass label. It expects a real node: fake
// nodes, e.g. clusterstate.FakeNode, have no ComputeClass label and resolve to
// the default ComputeClass.
func (f *FilteredCccLister) IsNodeOwned(node *apiv1.Node) bool {
	if node == nil {
		return false
	}
	var name string
	if node.Labels != nil {
		name = node.Labels[gkelabels.ComputeClassLabel]
	}
	if name == "" {
		if defaultName, _, found := f.baseLister.Default(); found {
			name = defaultName
		}
	}
	return f.isCrdNameOwned(name)
}

// NodeCrd returns CRD for Node if owned by this instance.
func (f *FilteredCccLister) NodeCrd(node *apiv1.Node) (crd.CRD, string, error) {
	return f.ownedCrdWithName(f.baseLister.NodeCrd(node))
}

// Labels returns base lister supported labels.
func (f *FilteredCccLister) Labels() []string {
	return f.baseLister.Labels()
}

// Default returns default CRD if owned by this instance.
func (f *FilteredCccLister) Default() (name string, label string, found bool) {
	name, label, found = f.baseLister.Default()
	if !found {
		return name, label, found
	}
	c, err := f.GetCrd(name)
	if err != nil || c == nil {
		return "", "", false
	}
	return name, label, true
}

// SetCloudProvider delegates cloud provider assignment.
func (f *FilteredCccLister) SetCloudProvider(provider listerCloudProvider) {
	f.baseLister.SetCloudProvider(provider)
}

func (f *FilteredCccLister) ownedCrdWithName(c crd.CRD, name string, err error) (crd.CRD, string, error) {
	if err != nil || c == nil {
		return c, name, err
	}
	if !IsOwned(c.Annotations(), f.autoscalerNames) {
		return nil, "", nil
	}
	return c, name, nil
}

func (f *FilteredCccLister) ownedCrd(c crd.CRD, err error) (crd.CRD, error) {
	crd, _, err := f.ownedCrdWithName(c, "", err)
	return crd, err
}
