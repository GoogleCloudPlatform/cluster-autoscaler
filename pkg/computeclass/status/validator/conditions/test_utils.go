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

package conditions

import (
	gceapiv1 "google.golang.org/api/compute/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
)

func newMockReservationsPuller(localProject string, projects []string, rsvs []*gceapiv1.Reservation) *gceclient.ReservationsPuller {
	mGceClient := gceclient.BuildAutoscalingInternalGceClientMock().
		WithFetchZones(func(region string) ([]string, error) { return []string{"us-central1-c"}, nil })
	puller, _ := gceclient.NewReservationsPuller(mGceClient, nil, nil, localProject, false, "us-central1")
	for _, project := range projects {
		puller.AddProject(project)
	}
	puller.SetReservations(rsvs)

	return puller
}

func newTestEvaluator(provider CloudProvider, opts ...func(*Evaluator)) *Evaluator {
	e := NewEvaluator(provider, nil, nil, "", nil, nil)
	for _, opt := range opts {
		opt(e)
	}
	e.setUpConditionCheckers()
	return e
}

func withSsdProvider(provider localssdsize.LocalSSDSizeProvider) func(*Evaluator) {
	return func(e *Evaluator) {
		e.localSsdProvider = provider
	}
}

func withLister(lister lister.Lister) func(*Evaluator) {
	return func(e *Evaluator) {
		e.lister = lister
		if e.provider != nil {
			e.matcher = computeclass.NewMatcher(lister, e.provider)
		}
	}
}

func withListerAndMatcher(lister lister.Lister, matcher computeclass.Matcher) func(*Evaluator) {
	return func(e *Evaluator) {
		e.lister = lister
		e.matcher = matcher
	}
}

func withReservations(cache ReservationProvider, blocksPuller *reservations.BlocksPuller) func(*Evaluator) {
	return func(e *Evaluator) {
		e.rsvCache = cache
		e.reservationBlocksPuller = blocksPuller
	}
}

func newTestProvider() *gke.TestAutoprovisioningCloudProviderBuilder {
	return gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithAutoprovisioningDefaultFamily(machinetypes.E2).
		WithAutoprovisioningEnabled(true)
}
