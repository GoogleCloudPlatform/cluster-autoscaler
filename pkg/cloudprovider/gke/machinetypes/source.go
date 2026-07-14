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

package machinetypes

import (
	"context"
	"reflect"
	"sync"

	"k8s.io/client-go/tools/cache"
	informers "k8s.io/gke-autoscaling/cluster-autoscaler/apis/machineconfig/client/informers/externalversions"
	mcv1 "k8s.io/gke-autoscaling/cluster-autoscaler/apis/machineconfig/cloud.google.com/v1"
	"k8s.io/klog/v2"
)

// Source is a skeleton of MachineConfig source of truth implementation.
// Currently it only watches the resources and logs them.
// TODO(b/436221975): Convert the CRD to CA-internal machine types implementation
// and use it as an authoritative source. Fall back to the hardcoded list (current machine_types.go)
// if uninitialized.
type Source struct {
	informer     cache.SharedIndexInformer
	mfCache      map[string]MachineFamily
	cpSource     *cpuPlatformsSource
	lock         sync.RWMutex
	enableCvmSot bool
	updateCount  uint64
}

// NewSource creates a new machine config source of truth.
// TODO(b/517095748): cache configs by version and not just by the family name
func NewSource(f informers.SharedInformerFactory, enableCvmSot bool) *Source {
	return &Source{
		informer:     f.Cloud().V1().MachineConfigs().Informer(),
		mfCache:      make(map[string]MachineFamily),
		cpSource:     newCpuPlatformsSource(),
		enableCvmSot: enableCvmSot,
	}
}

// Run starts watching MachineConfig objects.
func (p *Source) Run(ctx context.Context) {
	_, err := p.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    p.onAdd,
		UpdateFunc: p.onUpdate,
		DeleteFunc: p.onDelete,
	})
	if err != nil {
		klog.Errorf("MachineConfig Source: failed to add event handler: %v", err)
	}
	// TODO(b/517097121): fix the logging to include possible errors in earlier stages
	klog.Infof("MachineConfig Source: starting to watch resources")
	p.informer.Run(ctx.Done())
}

// Snapshot returns a copy of current cache state and its generation.
func (s *Source) Snapshot() (map[string]MachineFamily, uint64) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	result := make(map[string]MachineFamily, len(s.mfCache))
	for k, v := range s.mfCache {
		result[k] = v
	}
	return result, s.updateCount
}

func (s *Source) onAdd(obj interface{}) {
	mc, ok := obj.(*mcv1.MachineConfig)
	if !ok {
		klog.Errorf("MachineConfig Source: failed to cast to MachineConfig: %+v", obj)
		return
	}
	klog.Infof("MachineConfig Source: detected new resource: %s", mc.Name)
	s.processMachineConfig(mc)
}

func (s *Source) onUpdate(oldObj, newObj interface{}) {
	newMC, ok := newObj.(*mcv1.MachineConfig)
	if !ok {
		klog.Errorf("MachineConfig Source: failed to cast to MachineConfig: %+v", newObj)
		return
	}
	klog.Infof("MachineConfig Source: detected update for resource: %s", newMC.Name)
	s.processMachineConfig(newMC)
}

func (s *Source) processMachineConfig(mc *mcv1.MachineConfig) {
	for _, p := range CollectCPUPlatforms(&mc.Spec.MachineFamily) {
		platformInfo, err := ToCpuPlatformInfoObject(p)
		if err != nil {
			klog.Errorf("MachineConfig Source: failed to convert CPU platform %q: %v", p.Name, err)
			continue
		}
		s.cpSource.registerDynamic(platformInfo)
	}

	mf, warnsAndErrs := ToMachineFamilyObject(&mc.Spec.MachineFamily, s.cpSource, s.enableCvmSot)
	if warnsAndErrs.Warning != nil {
		klog.Warningf("MachineConfig Source: %v", warnsAndErrs.Warning)
	}
	if warnsAndErrs.Err != nil {
		klog.Errorf(
			"MachineConfig Source: failed to convert MachineConfig %s in version %s to a MachineFamily: %v",
			mc.Name,
			mc.Spec.Version,
			warnsAndErrs.Err,
		)
		return
	}
	s.updateCache(mf, mc.Spec.Version)
}

func (s *Source) onDelete(obj interface{}) {
	mc, ok := obj.(*mcv1.MachineConfig)
	if !ok {
		// Handle Tombstone for deleted objects
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("MachineConfig Source: failed to cast to MachineConfig or Tombstone: %+v", obj)
			return
		}
		mc, ok = tombstone.Obj.(*mcv1.MachineConfig)
		if !ok {
			klog.Errorf("MachineConfig Source: tombstone contained object that is not a MachineConfig: %+v", obj)
			return
		}
	}
	s.removeFromCache(mc.Spec.MachineFamily.Name, mc.Spec.Version)
	klog.Infof("MachineConfig Source: resource deleted: %s", mc.Name)
}

func (s *Source) updateCache(mf MachineFamily, version string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if old, exists := s.mfCache[mf.Name()]; exists {
		if reflect.DeepEqual(old, mf) {
			return
		}
	}

	s.mfCache[mf.Name()] = mf
	s.updateCount++
	klog.Infof("MachineConfig Source: internal cache state updated for %s, version %s", mf.Name(), version)
}

func (s *Source) removeFromCache(mfName string, version string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exists := s.mfCache[mfName]; !exists {
		klog.Infof("MachineConfig Source: removal of %s, version %s, skipped (not in cache)", mfName, version)
		return
	}

	delete(s.mfCache, mfName)
	s.updateCount++
	klog.Infof("MachineConfig Source: removed %s, version %s, from cache", mfName, version)
}
