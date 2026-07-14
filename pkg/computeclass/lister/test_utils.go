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
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

type MockCrdLister struct {
	crds           []crd.CRD
	crdLabel       string
	defaultCRDName string
	defaultCRDSet  bool
	// Default CC behaves slightly differently than NPC in case it is enabled, but CRD is not defined
	// pods(nodes) with no node selector(label) are treated as if they had no CRD and do not err
	useCCCDefaulting bool
}

func NewMockCrdLister(crds []crd.CRD) *MockCrdLister {
	return &MockCrdLister{
		crds: crds,
	}
}

func NewMockCrdListerWithLabel(crds []crd.CRD, crdLabel string) *MockCrdLister {
	return &MockCrdLister{
		crds:     crds,
		crdLabel: crdLabel,
	}
}

func NewMockCrdListerWithCCCDefaulting(crds []crd.CRD, cccDefaulting bool) *MockCrdLister {
	return &MockCrdLister{
		crds:             crds,
		useCCCDefaulting: cccDefaulting,
	}
}

func (l *MockCrdLister) SetCrds(crds []crd.CRD) {
	l.crds = crds
}

func (l *MockCrdLister) ListCrds() ([]crd.CRD, error) {
	return l.crds, nil
}

// Crd returns the CRD associated with the given label and name.
func (l *MockCrdLister) Crd(CRDLabel string, CRDName string) (crd.CRD, error) {
	if CRDLabel != l.crdLabel {
		return nil, fmt.Errorf("unknown CRD label %s", CRDLabel)
	}
	c, _, err := l.getCrd(CRDName, true)
	return c, err
}

func (l *MockCrdLister) NodeGroupCrd(nodeGroup cloudprovider.NodeGroup) (crd.CRD, string, error) {
	name, found, err := crd.NodeGroupCrdLabel(nodeGroup, l.crdLabel)
	if err != nil {
		return nil, "", err
	}
	return l.getCrd(name, found)
}

func (l *MockCrdLister) NodeCrd(node *apiv1.Node) (crd.CRD, string, error) {
	name, found, err := crd.NodeCrdLabel(node, l.crdLabel)
	if err != nil {
		return nil, "", err
	}
	return l.getCrd(name, found)
}

func (l *MockCrdLister) PodReqCrd(req *podrequirements.Requirements) (crd.CRD, string, error) {
	name, found := req.LabelReq.GetSingleValue(l.crdLabel)
	return l.getCrd(name, found)
}

func (l *MockCrdLister) PodReqCrdType(req *podrequirements.Requirements) (string, error) {
	if l.crdLabel == gkelabels.ComputeClassLabel {
		return ccc.CrdType, nil
	}
	if l.crdLabel == "autoscaling.gke.io/cc-label" {
		return "CC", nil
	}

	return "", nil
}

func (l *MockCrdLister) PodCrd(pod *apiv1.Pod) (crd.CRD, string, error) {
	podRequirements := podrequirements.GetRequirements(pod)
	return l.PodReqCrd(podRequirements)
}

func (l *MockCrdLister) getCCCCrd(name string, found bool) (crd.CRD, string, error) {
	noLabelAndDefaultCC := false
	if !found {
		if l.defaultCRDSet {
			name = l.defaultCRDName
			noLabelAndDefaultCC = true
		} else {
			return nil, "", nil
		}
	}

	if machinetypes.IsPredefinedComputeClass(name) {
		return nil, name, nil
	}

	cc, ok := l.crdByName(name)
	if !ok {
		if noLabelAndDefaultCC {
			return nil, "", nil
		}
		return nil, name, fmt.Errorf("crd doesnt exist")
	}

	return cc, name, nil
}

func (l *MockCrdLister) getCrd(name string, found bool) (crd.CRD, string, error) {
	// Hack to enforce CC-like behavior in case matching compute class label
	if l.useCCCDefaulting || l.crdLabel == gkelabels.ComputeClassLabel {
		return l.getCCCCrd(name, found)
	}

	if !found {
		if l.defaultCRDSet {
			name = l.defaultCRDName
		} else {
			return nil, "", nil
		}
	}

	if c, f := l.crdByName(name); f {
		return c, name, nil
	}
	return nil, name, fmt.Errorf("crd doesnt exist")
}

func (l *MockCrdLister) Labels() []string {
	return []string{l.crdLabel}
}

func (l *MockCrdLister) Default() (string, string, bool) {
	if l.defaultCRDSet {
		return l.defaultCRDName, l.crdLabel, true
	}
	return "", l.crdLabel, false
}

func (l *MockCrdLister) SetDefaultCrdName(defaultCRDName string) {
	l.defaultCRDName = defaultCRDName
	l.defaultCRDSet = true
}

func (l *MockCrdLister) SetCrdLabel(label string) {
	l.crdLabel = label
}

func (l *MockCrdLister) crdByName(crdName string) (crd.CRD, bool) {
	for _, c := range l.crds {
		if c.Label() != l.crdLabel {
			// This should never happen.
			continue
		}
		if c.Name() == crdName {
			return c, true
		}
	}
	return nil, false
}

// SetCloudProvider sets cloud provider
func (l *MockCrdLister) SetCloudProvider(provider listerCloudProvider) {}

// GetCrd returns CRD for given name.
func (l *MockCrdLister) GetCrd(name string) (crd.CRD, error) {
	if machinetypes.IsPredefinedComputeClass(name) {
		return nil, nil
	}
	for _, c := range l.crds {
		if c.Name() == name {
			return c, nil
		}
	}
	return nil, fmt.Errorf("crd doesnt exist")
}

// UpdateCrds updates the list of CRDs in the mock lister.
func (l *MockCrdLister) UpdateCrds(crds []crd.CRD) {
	l.crds = crds
}
