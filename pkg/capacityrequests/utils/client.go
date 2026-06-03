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

package utils

import (
	"fmt"
	"time"

	"k8s.io/client-go/rest"
	cr_clientset "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/client/clientset/versioned"
	cr_informer "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/client/informers/externalversions"
	cr_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/client/listers/internal.autoscaling.gke.io/v1"

	klog "k8s.io/klog/v2"
)

// NewCrClient creates a new Capacity Request client from the given config.
func NewCrClient(kubeConfig *rest.Config) (*cr_clientset.Clientset, error) {
	return cr_clientset.NewForConfig(kubeConfig)
}

// NewAllCrsLister creates a lister for all Capacity Requests in the cluster.
func NewAllCrsLister(crClient cr_clientset.Interface, stopChannel <-chan struct{}) (cr_lister.CapacityRequestLister, error) {
	factory := cr_informer.NewSharedInformerFactory(crClient, 1*time.Hour)
	crLister := factory.Internal().V1().CapacityRequests().Lister()
	go factory.Start(stopChannel)
	informersSynced := factory.WaitForCacheSync(stopChannel)
	for _, synced := range informersSynced {
		if !synced {
			return nil, fmt.Errorf("Can't create Capacity Request lister")
		}
	}
	klog.V(2).Info("Successful initial Capacity Request sync")
	return crLister, nil
}
