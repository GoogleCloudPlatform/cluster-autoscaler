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

package futurereservations

import (
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/klog/v2"
)

const (
	// runDelay specifies the delay before first fetch of GCE Future Reservations
	// set to 0 to fetch future reservations ASAP as they may be necessary for
	// backoff logic after Cluster Autoscaler restart
	runDelay = 0 * time.Second
	// loopDelay specifies the delay between each fetch of GCE Future Reservations
	loopDelay = 1 * time.Minute
)

// noOpPuller is a no-operation implementation of FutureReservationsPuller interface
type noOpPuller struct{}

func (*noOpPuller) GetLocalFutureReservations() []*gceclient.GceFutureReservation {
	return []*gceclient.GceFutureReservation{}
}
func (*noOpPuller) Run(_ context.Context) {
	// noop
}
func NewNoOpPuller() *noOpPuller {
	return &noOpPuller{}
}

// FutureReservationsGceProvider provides info about GCE future reservations,
// used as an GCE API in GKE, methods may be long running
type FutureReservationsGceProvider interface {
	GetFutureReservationsInProject(projectID string) ([]*gceclient.GceFutureReservation, error)
}

// frPuller implements FutureReservationsProvider that periodically fetches
// future reservations info from GCE
type frPuller struct {
	gceProvider    FutureReservationsGceProvider
	localProjectID string

	cacheLock sync.RWMutex
	cache     []*gceclient.GceFutureReservation

	// guards that only 1 loop is run, even if called from multiple go routines
	loopLock sync.Mutex
}

// NewFutureReservationsPuller creates a new puller implementation for given projectID
func NewFutureReservationsPuller(
	gceProvider FutureReservationsGceProvider,
	localProjectID string) *frPuller {
	return &frPuller{
		gceProvider:    gceProvider,
		localProjectID: localProjectID,
		cache:          make([]*gceclient.GceFutureReservation, 0),
	}
}

// GetLocalFutureReservations implements FutureReservationsProvider.GetLocalFutureReservations
func (p *frPuller) GetLocalFutureReservations() []*gceclient.GceFutureReservation {
	p.cacheLock.RLock()
	defer p.cacheLock.RUnlock()
	// return a copy of cache slice so that if it's modified outside, it doesn't affect the cache itself
	// (ignoring possible changes to GceFutureReservations in the slice)
	return append([]*gceclient.GceFutureReservation{}, p.cache...)
}

// Run implements FutureReservationsPuller.Run
func (p *frPuller) Run(ctx context.Context) {
	// wait before first run of the loop to let cluster-autoscaler initialize
	klog.V(1).Infof("Starting Future Reservations puller, will sleep %v before first fetch from GCE...", runDelay)
	time.Sleep(runDelay)
	wait.NonSlidingUntil(p.loop, loopDelay, ctx.Done())
}

// loop is a puller loop where GCE Future Reservations are fetched
// TODO(b/369518830): investigate activating the loop on future reservation create/edit/delete in GCE
func (p *frPuller) loop() {
	p.loopLock.Lock()
	defer p.loopLock.Unlock()

	klog.V(1).Infof("Fetching Future Reservations from GCE for project '%s'...", p.localProjectID)

	next, err := p.gceProvider.GetFutureReservationsInProject(p.localProjectID)
	if err != nil {
		klog.Errorf("Error while getting GCE Future Reservations: %v", err)
		return
	}

	klog.V(1).Infof("Fetched %d Future Reservation(s) from GCE", len(next))

	p.cacheLock.Lock()
	p.cache = next
	p.cacheLock.Unlock()
}
