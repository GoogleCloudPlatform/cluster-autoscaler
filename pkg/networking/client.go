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

package networking

import (
	"context"
	"fmt"
	"time"

	network_clientset "github.com/GoogleCloudPlatform/gke-networking-api/client/network/clientset/versioned"
	network_informer "github.com/GoogleCloudPlatform/gke-networking-api/client/network/informers/externalversions"
	network_lister "github.com/GoogleCloudPlatform/gke-networking-api/client/network/listers/network/v1"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// NewClientset returns a new GKE Network ParamSets clientset.
func NewClientset(kubeConfig *rest.Config) (*network_clientset.Clientset, error) {
	return network_clientset.NewForConfig(kubeConfig)
}

// NewLister initialises the GKE Network ParamSets lister and returns it.
func NewLister(ctx context.Context, client network_clientset.Interface) (network_lister.GKENetworkParamSetLister, error) {
	factory := network_informer.NewSharedInformerFactory(client, 1*time.Hour)
	lister := factory.Networking().V1().GKENetworkParamSets().Lister()
	factory.Start(ctx.Done())
	informersSynced := factory.WaitForCacheSync(ctx.Done())
	for _, synced := range informersSynced {
		if !synced {
			return nil, fmt.Errorf("can't create GKE Network ParamSets lister")
		}
	}
	klog.V(2).Info("Successful GKE Network ParamSets sync")
	return lister, nil
}
