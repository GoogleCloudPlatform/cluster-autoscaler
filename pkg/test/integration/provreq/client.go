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

package provreq

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	prfake "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/clientset/versioned/fake"
	prexternalversions "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/client/informers/externalversions"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
)

// NewFakeClientForTB creates a fake provisioning request client specifically tailored for tests.
func NewFakeClientForTB(ctx context.Context, t testing.TB, prs ...*provreqwrapper.ProvisioningRequest) *provreqclient.ProvisioningRequestClient {
	t.Helper()
	provReqClient := prfake.NewSimpleClientset()
	podTemplClient := fake.NewSimpleClientset()
	for _, pr := range prs {
		if pr == nil {
			continue
		}
		if _, err := provReqClient.AutoscalingV1().ProvisioningRequests(pr.Namespace).Create(ctx, pr.ProvisioningRequest, metav1.CreateOptions{}); err != nil {
			t.Errorf("While adding a ProvisioningRequest: %s/%s to fake client, got error: %v", pr.Namespace, pr.Name, err)
		}
		for _, pd := range pr.PodTemplates {
			if _, err := podTemplClient.CoreV1().PodTemplates(pr.Namespace).Create(ctx, pd, metav1.CreateOptions{}); err != nil {
				t.Errorf("While adding a PodTemplate: %s/%s to fake client, got error: %v", pr.Namespace, pd.Name, err)
			}
		}
	}
	prFactory := prexternalversions.NewSharedInformerFactory(provReqClient, 1*time.Hour)
	provReqLister := prFactory.Autoscaling().V1().ProvisioningRequests().Lister()
	prFactory.Start(ctx.Done())

	podFactory := informers.NewSharedInformerFactory(podTemplClient, 1*time.Hour)
	podTemplLister := podFactory.Core().V1().PodTemplates().Lister()
	podFactory.Start(ctx.Done())

	informersSynced := prFactory.WaitForCacheSync(ctx.Done())
	for _, synced := range informersSynced {
		if !synced {
			t.Fatalf("Failed to sync Provisioning Request informers")
		}
	}

	podInformersSynced := podFactory.WaitForCacheSync(ctx.Done())
	for _, synced := range podInformersSynced {
		if !synced {
			t.Fatalf("Failed to sync Pod Template informers")
		}
	}

	return provreqclient.NewProvisioningRequestClient(
		provReqClient,
		provReqLister,
		podTemplLister,
	)
}
