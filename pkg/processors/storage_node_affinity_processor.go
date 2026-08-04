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

package processors

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	klog "k8s.io/klog/v2"
)

const (
	// storageTypeParameterKey is the parameter/attribute key used in StorageClass and PV CSI attributes to specify disk type.
	storageTypeParameterKey = "type"
	// dynamicStorageClassType is the value of parameters["type"] in dynamic StorageClasses that automate disk selection.
	dynamicStorageClassType = "dynamic"
	// isDefaultStorageClassAnnotation is the annotation key used to mark a StorageClass as default.
	isDefaultStorageClassAnnotation = "storageclass.kubernetes.io/is-default-class"
	// gkePdCsiDriver is the CSI driver name for GKE Persistent Disks and Hyperdisks.
	gkePdCsiDriver = "pd.csi.storage.gke.io"
)

func isGkePdDriver(name string) bool {
	return name == gkePdCsiDriver
}

// StorageNodeAffinityPodListProcessor inspects pods' PVC requirements and injects the corresponding
// disk-type.gke.io/* serenity label selector into CA's internal in-memory copy of the Pod spec.
type StorageNodeAffinityPodListProcessor struct {
	pvcLister corelisters.PersistentVolumeClaimLister
	scLister  storagelisters.StorageClassLister
	pvLister  corelisters.PersistentVolumeLister
}

// NewStorageNodeAffinityPodListProcessor creates a new StorageNodeAffinityPodListProcessor.
func NewStorageNodeAffinityPodListProcessor(pvcLister corelisters.PersistentVolumeClaimLister, scLister storagelisters.StorageClassLister, pvLister corelisters.PersistentVolumeLister) *StorageNodeAffinityPodListProcessor {
	return &StorageNodeAffinityPodListProcessor{
		pvcLister: pvcLister,
		scLister:  scLister,
		pvLister:  pvLister,
	}
}

// Process injects storage nodeSelector labels into in-memory copies of unschedulable pods.
func (p *StorageNodeAffinityPodListProcessor) Process(ctx *context.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	if ctx == nil {
		klog.V(5).Infof("StorageNodeAffinityPodListProcessor.Process skipped (ctx is nil)")
		return unschedulablePods, nil
	}
	if p == nil || p.pvcLister == nil {
		klog.V(5).Infof("StorageNodeAffinityPodListProcessor.Process skipped (processor or pvcLister is nil: p=%v)", p)
		return unschedulablePods, nil
	}
	if len(unschedulablePods) == 0 {
		return unschedulablePods, nil
	}

	result := make([]*apiv1.Pod, 0, len(unschedulablePods))
	injectedCount := 0
	for _, pod := range unschedulablePods {
		diskTypes := requiredDiskTypes(pod, p.pvcLister, p.scLister, p.pvLister)
		klog.V(5).Infof("Pod %s/%s -> discovered required diskTypes=%v", pod.Namespace, pod.Name, diskTypes)
		if len(diskTypes) == 0 {
			result = append(result, pod)
			continue
		}

		// Create a deep copy of the pod to avoid mutating the shared informer cache
		podCopy := pod.DeepCopy()
		if podCopy.Spec.NodeSelector == nil {
			podCopy.Spec.NodeSelector = make(map[string]string)
		}

		injectedAny := false
		for _, diskType := range diskTypes {
			labelKey := gkelabels.SupportedDiskTypeKey(diskType)
			if podCopy.Spec.NodeSelector[labelKey] != "true" {
				podCopy.Spec.NodeSelector[labelKey] = "true"
				injectedAny = true
				klog.V(5).Infof("Injected in-memory nodeSelector %s=true for pod %s/%s", labelKey, pod.Namespace, pod.Name)
			} else {
				klog.V(5).Infof("Pod %s/%s already has nodeSelector %s=true", pod.Namespace, pod.Name, labelKey)
			}
		}
		if injectedAny {
			injectedCount++
		}
		result = append(result, podCopy)
	}
	klog.V(4).Infof("StorageNodeAffinityPodListProcessor.Process complete: inspected %d pods, injected label into %d pods", len(unschedulablePods), injectedCount)
	return result, nil
}

// requiredDiskTypes inspects a pod's PVCs and StorageClasses using cached listers
// to identify all storage disk types required by the pod (e.g. pd-balanced, hyperdisk-balanced).
func requiredDiskTypes(pod *apiv1.Pod, pvcLister corelisters.PersistentVolumeClaimLister, scLister storagelisters.StorageClassLister, pvLister corelisters.PersistentVolumeLister) []string {
	if pod == nil || pvcLister == nil {
		return nil
	}

	diskTypeSet := make(map[string]bool)
	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim == nil {
			continue
		}

		claimName := vol.PersistentVolumeClaim.ClaimName
		if claimName == "" {
			continue
		}

		pvc, err := pvcLister.PersistentVolumeClaims(pod.Namespace).Get(claimName)
		if err != nil {
			klog.V(5).Infof("Failed to fetch PVC %s/%s from lister: %v", pod.Namespace, claimName, err)
			continue
		}

		var diskType string

		// 1. Bound PV Check (rescheduled / pre-bound workload)
		if pvc.Spec.VolumeName != "" && pvLister != nil {
			pv, err := pvLister.Get(pvc.Spec.VolumeName)
			if err == nil && pv.Spec.CSI != nil && isGkePdDriver(pv.Spec.CSI.Driver) && pv.Spec.CSI.VolumeAttributes != nil {
				diskType = pv.Spec.CSI.VolumeAttributes[storageTypeParameterKey]
				klog.V(5).Infof("requiredDiskTypes: pod %s/%s -> PVC %s is BOUND to GKE PD PV %s with CSI type %q", pod.Namespace, pod.Name, claimName, pvc.Spec.VolumeName, diskType)
			}
		}

		// 2. Unbound PVC / StorageClass Check (new dynamic volume provisioning)
		if diskType == "" && scLister != nil {
			scName := ""
			if pvc.Spec.StorageClassName != nil {
				scName = *pvc.Spec.StorageClassName
			}
			if scName != "" {
				sc, err := scLister.Get(scName)
				if err == nil && isGkePdDriver(sc.Provisioner) && sc.Parameters != nil {
					diskType = sc.Parameters[storageTypeParameterKey]
					klog.V(5).Infof("requiredDiskTypes: pod %s/%s -> UNBOUND PVC %s references GKE PD StorageClass %s with parameters type %q", pod.Namespace, pod.Name, claimName, scName, diskType)
				} else if err != nil {
					klog.V(5).Infof("requiredDiskTypes: pod %s/%s -> UNBOUND PVC %s references StorageClass %s (err=%v)", pod.Namespace, pod.Name, claimName, scName, err)
				} else {
					klog.V(5).Infof("requiredDiskTypes: pod %s/%s -> UNBOUND PVC %s references StorageClass %s (not GKE PD or parameters=nil)", pod.Namespace, pod.Name, claimName, scName)
				}
			} else {
				// Search for default StorageClass annotated with storageclass.kubernetes.io/is-default-class: "true"
				classes, err := scLister.List(labels.Everything())
				if err == nil {
					for _, sc := range classes {
						if sc.Annotations[isDefaultStorageClassAnnotation] == "true" {
							if isGkePdDriver(sc.Provisioner) && sc.Parameters != nil {
								diskType = sc.Parameters[storageTypeParameterKey]
								klog.V(5).Infof("requiredDiskTypes: pod %s/%s -> UNBOUND PVC %s references default GKE PD StorageClass %s with parameters type %q", pod.Namespace, pod.Name, claimName, sc.Name, diskType)
							}
							break
						}
					}
				}
			}
		}

		if diskType == dynamicStorageClassType {
			klog.V(5).Infof("requiredDiskTypes: pod %s/%s -> PVC %s specifies dynamic disk type; skipping serenity label injection and letting PDCSI handle selection", pod.Namespace, pod.Name, claimName)
		} else if diskType != "" {
			diskTypeSet[diskType] = true
		}
	}

	result := make([]string, 0, len(diskTypeSet))
	for dt := range diskTypeSet {
		result = append(result, dt)
	}
	return result
}
