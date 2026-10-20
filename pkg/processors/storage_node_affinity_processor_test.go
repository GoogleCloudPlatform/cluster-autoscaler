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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
)

type mockSerenityCloudProvider struct {
	ProcessorsCloudProvider
}

func (m *mockSerenityCloudProvider) IsMachineSerenityLabelsEnabled() bool {
	return true
}

func TestStorageNodeAffinityPodListProcessor(t *testing.T) {
	client := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	scInformer := informerFactory.Storage().V1().StorageClasses()
	pvInformer := informerFactory.Core().V1().PersistentVolumes()

	processor := NewStorageNodeAffinityPodListProcessor(pvcInformer.Lister(), scInformer.Lister(), pvInformer.Lister())

	scName := "hd-sc"
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: "pd.csi.storage.gke.io",
		Parameters:  map[string]string{"type": "hyperdisk-balanced"},
	}
	err := scInformer.Informer().GetIndexer().Add(sc)
	assert.NoError(t, err)

	pvc := &apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "hd-pvc", Namespace: "default"},
		Spec: apiv1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
		},
	}
	err = pvcInformer.Informer().GetIndexer().Add(pvc)
	assert.NoError(t, err)

	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
		Spec: apiv1.PodSpec{
			Volumes: []apiv1.Volume{
				{
					Name: "data",
					VolumeSource: apiv1.VolumeSource{
						PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
							ClaimName: "hd-pvc",
						},
					},
				},
			},
		},
	}

	ctx := &ca_context.AutoscalingContext{CloudProvider: &mockSerenityCloudProvider{}}
	processedPods, err := processor.Process(context.TODO(), ctx, []*apiv1.Pod{pod})
	assert.NoError(t, err)
	assert.Len(t, processedPods, 1)
	assert.Equal(t, "true", processedPods[0].Spec.NodeSelector["disk-type.gke.io/hyperdisk-balanced"])
}

func TestStorageNodeAffinityPodListProcessor_DualStorage(t *testing.T) {
	client := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	scInformer := informerFactory.Storage().V1().StorageClasses()
	pvInformer := informerFactory.Core().V1().PersistentVolumes()

	processor := NewStorageNodeAffinityPodListProcessor(pvcInformer.Lister(), scInformer.Lister(), pvInformer.Lister())

	hdSCName := "hd-sc"
	hdSC := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: hdSCName},
		Provisioner: "pd.csi.storage.gke.io",
		Parameters:  map[string]string{"type": "hyperdisk-balanced"},
	}
	err := scInformer.Informer().GetIndexer().Add(hdSC)
	assert.NoError(t, err)

	pdSCName := "pd-sc"
	pdSC := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: pdSCName},
		Provisioner: "pd.csi.storage.gke.io",
		Parameters:  map[string]string{"type": "pd-balanced"},
	}
	err = scInformer.Informer().GetIndexer().Add(pdSC)
	assert.NoError(t, err)

	hdPVC := &apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "hd-pvc", Namespace: "default"},
		Spec: apiv1.PersistentVolumeClaimSpec{
			StorageClassName: &hdSCName,
		},
	}
	err = pvcInformer.Informer().GetIndexer().Add(hdPVC)
	assert.NoError(t, err)

	pdPVC := &apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pd-pvc", Namespace: "default"},
		Spec: apiv1.PersistentVolumeClaimSpec{
			StorageClassName: &pdSCName,
		},
	}
	err = pvcInformer.Informer().GetIndexer().Add(pdPVC)
	assert.NoError(t, err)

	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "dual-pod", Namespace: "default"},
		Spec: apiv1.PodSpec{
			Volumes: []apiv1.Volume{
				{
					Name: "hd-data",
					VolumeSource: apiv1.VolumeSource{
						PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
							ClaimName: "hd-pvc",
						},
					},
				},
				{
					Name: "pd-data",
					VolumeSource: apiv1.VolumeSource{
						PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
							ClaimName: "pd-pvc",
						},
					},
				},
			},
		},
	}

	ctx := &ca_context.AutoscalingContext{CloudProvider: &mockSerenityCloudProvider{}}
	processedPods, err := processor.Process(context.TODO(), ctx, []*apiv1.Pod{pod})
	assert.NoError(t, err)
	assert.Len(t, processedPods, 1)
	assert.Equal(t, "true", processedPods[0].Spec.NodeSelector["disk-type.gke.io/hyperdisk-balanced"])
	assert.Equal(t, "true", processedPods[0].Spec.NodeSelector["disk-type.gke.io/pd-balanced"])

	// Test skip when experiment flag is disabled (ctx is nil or IsMachineSerenityLabelsEnabled is false)
	skippedPods, err := processor.Process(context.TODO(), nil, []*apiv1.Pod{pod})
	assert.NoError(t, err)
	assert.Len(t, skippedPods, 1)
	assert.Nil(t, skippedPods[0].Spec.NodeSelector)
}

func TestStorageNodeAffinityPodListProcessor_NonGkePdCsiDriver(t *testing.T) {
	client := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	scInformer := informerFactory.Storage().V1().StorageClasses()
	pvInformer := informerFactory.Core().V1().PersistentVolumes()

	processor := NewStorageNodeAffinityPodListProcessor(pvcInformer.Lister(), scInformer.Lister(), pvInformer.Lister())

	scName := "filestore-sc"
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: "filestore.csi.storage.gke.io",
		Parameters:  map[string]string{"type": "multishare"},
	}
	err := scInformer.Informer().GetIndexer().Add(sc)
	assert.NoError(t, err)

	pvc := &apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "filestore-pvc", Namespace: "default"},
		Spec: apiv1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
		},
	}
	err = pvcInformer.Informer().GetIndexer().Add(pvc)
	assert.NoError(t, err)

	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "filestore-pod", Namespace: "default"},
		Spec: apiv1.PodSpec{
			Volumes: []apiv1.Volume{
				{
					Name: "data",
					VolumeSource: apiv1.VolumeSource{
						PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
							ClaimName: "filestore-pvc",
						},
					},
				},
			},
		},
	}

	ctx := &ca_context.AutoscalingContext{CloudProvider: &mockSerenityCloudProvider{}}
	processedPods, err := processor.Process(context.TODO(), ctx, []*apiv1.Pod{pod})
	assert.NoError(t, err)
	assert.Len(t, processedPods, 1)
	assert.Nil(t, processedPods[0].Spec.NodeSelector)
}

func TestStorageNodeAffinityPodListProcessor_DynamicDiskType(t *testing.T) {
	client := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	scInformer := informerFactory.Storage().V1().StorageClasses()
	pvInformer := informerFactory.Core().V1().PersistentVolumes()

	processor := NewStorageNodeAffinityPodListProcessor(pvcInformer.Lister(), scInformer.Lister(), pvInformer.Lister())

	scName := "dynamic-sc"
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: "pd.csi.storage.gke.io",
		Parameters:  map[string]string{"type": "dynamic"},
	}
	err := scInformer.Informer().GetIndexer().Add(sc)
	assert.NoError(t, err)

	pvc := &apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "dynamic-pvc", Namespace: "default"},
		Spec: apiv1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
		},
	}
	err = pvcInformer.Informer().GetIndexer().Add(pvc)
	assert.NoError(t, err)

	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "dynamic-pod", Namespace: "default"},
		Spec: apiv1.PodSpec{
			Volumes: []apiv1.Volume{
				{
					Name: "data",
					VolumeSource: apiv1.VolumeSource{
						PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
							ClaimName: "dynamic-pvc",
						},
					},
				},
			},
		},
	}

	ctx := &ca_context.AutoscalingContext{CloudProvider: &mockSerenityCloudProvider{}}
	processedPods, err := processor.Process(context.TODO(), ctx, []*apiv1.Pod{pod})
	assert.NoError(t, err)
	assert.Len(t, processedPods, 1)
	assert.Nil(t, processedPods[0].Spec.NodeSelector)
}

func TestStorageNodeAffinityPodListProcessor_BoundPV(t *testing.T) {
	client := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	scInformer := informerFactory.Storage().V1().StorageClasses()
	pvInformer := informerFactory.Core().V1().PersistentVolumes()

	processor := NewStorageNodeAffinityPodListProcessor(pvcInformer.Lister(), scInformer.Lister(), pvInformer.Lister())

	scName := "hd-sc"
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: "pd.csi.storage.gke.io",
		Parameters:  map[string]string{"type": "hyperdisk-balanced"},
	}
	err := scInformer.Informer().GetIndexer().Add(sc)
	assert.NoError(t, err)

	pvName := "bound-pv"
	pv := &apiv1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: apiv1.PersistentVolumeSpec{
			PersistentVolumeSource: apiv1.PersistentVolumeSource{
				CSI: &apiv1.CSIPersistentVolumeSource{
					Driver: "pd.csi.storage.gke.io",
				},
			},
		},
	}
	err = pvInformer.Informer().GetIndexer().Add(pv)
	assert.NoError(t, err)

	pvc := &apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "bound-pvc", Namespace: "default"},
		Spec: apiv1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			VolumeName:       pvName,
		},
	}
	err = pvcInformer.Informer().GetIndexer().Add(pvc)
	assert.NoError(t, err)

	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "bound-pod", Namespace: "default"},
		Spec: apiv1.PodSpec{
			Volumes: []apiv1.Volume{
				{
					Name: "data",
					VolumeSource: apiv1.VolumeSource{
						PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
							ClaimName: "bound-pvc",
						},
					},
				},
			},
		},
	}

	ctx := &ca_context.AutoscalingContext{CloudProvider: &mockSerenityCloudProvider{}}
	processedPods, err := processor.Process(context.TODO(), ctx, []*apiv1.Pod{pod})
	assert.NoError(t, err)
	assert.Len(t, processedPods, 1)
	assert.Equal(t, "true", processedPods[0].Spec.NodeSelector["disk-type.gke.io/hyperdisk-balanced"])
}

func TestStorageNodeAffinityPodListProcessor_BoundNonPdPVAndEmptyStorageClass(t *testing.T) {
	emptySC := ""
	defaultSCName := "standard-rwo"
	defaultSC := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        defaultSCName,
			Annotations: map[string]string{isDefaultStorageClassAnnotation: "true"},
		},
		Provisioner: "pd.csi.storage.gke.io",
		Parameters:  map[string]string{"type": "pd-balanced"},
	}

	filestorePV := &apiv1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "filestore-pv"},
		Spec: apiv1.PersistentVolumeSpec{
			PersistentVolumeSource: apiv1.PersistentVolumeSource{
				CSI: &apiv1.CSIPersistentVolumeSource{
					Driver: "filestore.csi.storage.gke.io",
				},
			},
		},
	}
	pdPVWithoutTypeAttr := &apiv1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pd-pv"},
		Spec: apiv1.PersistentVolumeSpec{
			PersistentVolumeSource: apiv1.PersistentVolumeSource{
				CSI: &apiv1.CSIPersistentVolumeSource{
					Driver: "pd.csi.storage.gke.io",
				},
			},
		},
	}

	tests := []struct {
		name             string
		pvc              *apiv1.PersistentVolumeClaim
		wantNodeSelector map[string]string
	}{
		{
			name: "bound to Filestore CSI PV with empty storageClassName does not fall back to default PD StorageClass",
			pvc: &apiv1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc", Namespace: "default"},
				Spec: apiv1.PersistentVolumeClaimSpec{
					StorageClassName: &emptySC,
					VolumeName:       "filestore-pv",
				},
			},
			wantNodeSelector: nil,
		},
		{
			name: "unbound PVC with explicit empty storageClassName does not fall back to default PD StorageClass",
			pvc: &apiv1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc", Namespace: "default"},
				Spec: apiv1.PersistentVolumeClaimSpec{
					StorageClassName: &emptySC,
				},
			},
			wantNodeSelector: nil,
		},
		{
			name: "bound to PD CSI PV without volumeAttributes type falls back to PVC StorageClass",
			pvc: &apiv1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc", Namespace: "default"},
				Spec: apiv1.PersistentVolumeClaimSpec{
					StorageClassName: &defaultSCName,
					VolumeName:       "pd-pv",
				},
			},
			wantNodeSelector: map[string]string{"disk-type.gke.io/pd-balanced": "true"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			informerFactory := informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0)
			pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
			scInformer := informerFactory.Storage().V1().StorageClasses()
			pvInformer := informerFactory.Core().V1().PersistentVolumes()

			assert.NoError(t, scInformer.Informer().GetIndexer().Add(defaultSC))
			assert.NoError(t, pvInformer.Informer().GetIndexer().Add(filestorePV))
			assert.NoError(t, pvInformer.Informer().GetIndexer().Add(pdPVWithoutTypeAttr))
			assert.NoError(t, pvcInformer.Informer().GetIndexer().Add(tc.pvc))

			processor := NewStorageNodeAffinityPodListProcessor(pvcInformer.Lister(), scInformer.Lister(), pvInformer.Lister())
			pod := &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
				Spec: apiv1.PodSpec{
					Volumes: []apiv1.Volume{
						{
							Name: "data",
							VolumeSource: apiv1.VolumeSource{
								PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
									ClaimName: tc.pvc.Name,
								},
							},
						},
					},
				},
			}

			ctx := &ca_context.AutoscalingContext{CloudProvider: &mockSerenityCloudProvider{}}
			processedPods, err := processor.Process(context.TODO(), ctx, []*apiv1.Pod{pod})
			assert.NoError(t, err)
			assert.Len(t, processedPods, 1)
			assert.Equal(t, tc.wantNodeSelector, processedPods[0].Spec.NodeSelector)
		})
	}
}
