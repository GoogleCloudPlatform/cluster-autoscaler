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
	"context"
	"fmt"
	"sync"
	"time"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	informer "github.com/googlecloudplatform/compute-class-api/client/informers/externalversions"
	lister "github.com/googlecloudplatform/compute-class-api/client/listers/cloud.google.com/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/client"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/klog/v2"
)

type cachedCrd struct {
	resourceVersion string
	crd             crd.CRD
}

// cccLister implements the Lister interface for CCCs.
type cccLister struct {
	lister.ComputeClassLister

	provider       listerCloudProvider
	optionsTracker *optstracking.OptionsTracker
	cacheMutex     sync.RWMutex
	crdCache       map[types.UID]cachedCrd
}

// ListCrds returns all CCCs
func (l *cccLister) ListCrds() ([]crd.CRD, error) {
	var crds []crd.CRD
	cccs, err := l.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	projectId, _, _ := l.provider.GetClusterInfo()
	autopilotEnabled := l.provider.IsAutopilotEnabled()

	activeUIDs := make(map[types.UID]bool, len(cccs))
	for _, c := range cccs {
		activeUIDs[c.UID] = true
		crds = append(crds, l.createComputeClassCrd(c, projectId, autopilotEnabled))
	}

	l.cacheMutex.Lock()
	for uid := range l.crdCache {
		if !activeUIDs[uid] {
			delete(l.crdCache, uid)
		}
	}
	l.cacheMutex.Unlock()

	return crds, nil
}

// Crd returns the CRD associated with the given label and name.
func (l *cccLister) Crd(crdLabel string, crdName string) (crd.CRD, error) {
	if crdLabel != gkelabels.ComputeClassLabel {
		return nil, fmt.Errorf("unknown CRD label %s", crdLabel)
	}
	return l.GetCrd(crdName)
}

// PodReqCrd returns the CCC for pod requirements. CCC should be assigned iff.
// the requirements specify it.
func (l *cccLister) PodReqCrd(req *podrequirements.Requirements) (crd.CRD, string, error) {
	name, found := req.LabelReq.GetSingleValue(gkelabels.ComputeClassLabel)
	if !found {
		return l.GetDefaultCrd()
	}
	c, err := l.GetCrd(name)
	return c, name, err
}

// PodReqCrdType returns CRD type for given pod requirements.
// Always returns CCC type.
func (l *cccLister) PodReqCrdType(req *podrequirements.Requirements) (string, error) {
	return ccc.CrdType, nil
}

// PodCrd returns the CCC for a pod. CCC should be assigned iff
// the pod specifies it.
func (l *cccLister) PodCrd(pod *corev1.Pod) (crd.CRD, string, error) {
	podRequirements := podrequirements.GetRequirements(pod)
	return l.PodReqCrd(podRequirements)
}

// NodeGroupCrd returns the CCC for a nodegroup. CCC should be assigned iff.
// the nodegroup specifies it.
func (l *cccLister) NodeGroupCrd(nodeGroup cloudprovider.NodeGroup) (crd.CRD, string, error) {
	name, found, err := crd.NodeGroupCrdLabel(nodeGroup, gkelabels.ComputeClassLabel)
	if err != nil {
		return nil, "", err
	}
	if !found {
		return l.GetDefaultCrd()
	}
	c, err := l.GetCrd(name)
	return c, name, err
}

// NodeCrd returns the CCC for a node. CCC should be assigned iff
// the node specifies it.
func (l *cccLister) NodeCrd(node *corev1.Node) (crd.CRD, string, error) {
	name, found, err := crd.NodeCrdLabel(node, gkelabels.ComputeClassLabel)
	if err != nil {
		return nil, "", err
	}
	if !found {
		return l.GetDefaultCrd()
	}
	c, err := l.GetCrd(name)
	return c, name, err
}

// GetCrd retrieves a CCC crd for a specific name.
func (l *cccLister) GetCrd(name string) (crd.CRD, error) {
	if machinetypes.IsPredefinedComputeClass(name) {
		return nil, nil
	}
	cc, err := l.ComputeClassLister.Get(name)
	if err != nil {
		return nil, err
	}
	projectId, _, _ := l.provider.GetClusterInfo()
	autopilotEnabled := l.provider.IsAutopilotEnabled()
	return l.createComputeClassCrd(cc, projectId, autopilotEnabled), nil
}

// GetDefaultCrd returns the default CCC for the cluster.
// returns nil, "", nil if default CCC is not enabled.
func (l *cccLister) GetDefaultCrd() (crd.CRD, string, error) {
	if !l.provider.IsDefaultCCCEnabled() {
		return nil, "", nil
	}
	name := gkelabels.DefaultCCCName
	crd, err := l.GetCrd(name)
	if err != nil {
		return nil, "", nil // Return nil if default is not found
	}
	return crd, name, nil
}

func (l *cccLister) createComputeClassCrd(cc *cccv1.ComputeClass, projectId string, autopilotEnabled bool) crd.CRD {
	l.cacheMutex.RLock()
	entry, hit := l.crdCache[cc.UID]
	l.cacheMutex.RUnlock()

	if hit && entry.resourceVersion == cc.ResourceVersion {
		return entry.crd
	}

	// Cache miss or stale, create a new cccCrd instance
	newCrd := ccc.NewCccCrd(cc, projectId, autopilotEnabled, l.provider, l.optionsTracker)

	l.cacheMutex.Lock()
	l.crdCache[cc.UID] = cachedCrd{
		resourceVersion: cc.ResourceVersion,
		crd:             newCrd,
	}
	l.cacheMutex.Unlock()
	return newCrd
}

// Labels returns the CCC label
func (l *cccLister) Labels() []string {
	return []string{gkelabels.ComputeClassLabel}
}

// Default returns the default CCC name
func (l *cccLister) Default() (string, string, bool) {
	if l.provider.IsDefaultCCCEnabled() {
		return gkelabels.DefaultCCCName, gkelabels.ComputeClassLabel, true
	}
	return "", gkelabels.ComputeClassLabel, false
}

// SetCloudProvider sets cloud provider
func (l *cccLister) SetCloudProvider(provider listerCloudProvider) {
	l.provider = provider
}

// NewCccLister initialises the CCC lister and returns it
func NewCccLister(ctx context.Context, client client.Client, optionsTracker *optstracking.OptionsTracker) (*cccLister, error) {
	factory := informer.NewSharedInformerFactory(client.CccClient(), 1*time.Hour)
	lister := factory.Cloud().V1().ComputeClasses().Lister()
	factory.Start(ctx.Done())

	informersSynced := factory.WaitForCacheSync(ctx.Done())
	for _, synced := range informersSynced {
		if !synced {
			return nil, fmt.Errorf("can't create CCC lister")
		}
	}

	klog.V(2).Info("Successful CCCs sync")
	return &cccLister{
		ComputeClassLister: lister,
		optionsTracker:     optionsTracker,
		crdCache:           make(map[types.UID]cachedCrd),
	}, nil
}
