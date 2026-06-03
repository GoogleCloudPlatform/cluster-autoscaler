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

package gceclient

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/consumablereservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"

	gce_api "google.golang.org/api/compute/v1"
	gkeutil "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util"
	"k8s.io/klog/v2"
)

const (
	// jitterSize is applied to the time between puller loops
	jitterSize = 0.2

	// initialActivityTime tells how long after the component startup we use active sleep
	initialActivityTime = 15 * time.Minute

	// inactivityTimeThreshold tells how long after the last reservation was seen we use inactive sleep
	inactivityTimeThreshold = time.Hour

	// inactiveSleep is a duration of time between puller loops if there are no reservations
	inactiveSleep = time.Hour
	// activeSleep is a duration of time between puller loops if there are some reservations
	activeSleep = time.Minute

	// consumablePullerReactivateThreshold indicates how long to wait before turning the consumable puller
	// back on after an error.
	// TODO(b/407422935): make this configurable via a flag.
	consumablePullerReactivateThreshold = time.Hour

	// experimentUpdaterInterval indicates how often we refresh experiment values.
	experimentUpdaterInterval = 3 * time.Minute
)

// ProjectPuller periodically pulls GCE Reservations for a specific project.
type ProjectPuller struct {
	gceClient AutoscalingInternalGceClient
	zones     []string
	projectID string

	// DO NOT refer .reservations directly
	// use getter and setter to leverage locks
	mutex        sync.Mutex
	reservations []*gce_api.Reservation

	// loopLock guards loop state from data races
	loopLock            sync.Mutex
	lastLoopError       error
	lastReservationSeen time.Time
}

// NewProjectPuller builds a new Puller for a specific project.
func NewProjectPuller(gceClient AutoscalingInternalGceClient, projectID string, zones []string) *ProjectPuller {
	return &ProjectPuller{
		gceClient:           gceClient,
		projectID:           projectID,
		lastReservationSeen: time.Now().Add(initialActivityTime - inactivityTimeThreshold),
		zones:               zones,
	}
}

func (p *ProjectPuller) GetReservations() []*gce_api.Reservation {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.reservations
}

func (p *ProjectPuller) SetReservations(reservations []*gce_api.Reservation) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.reservations = reservations
}

// Run runs the Puller.
func (p *ProjectPuller) Run(ctx context.Context) {
	klog.V(0).Infof("Enabling Reservations Project Puller for %q", p.projectID)
	timer := time.NewTimer(jitter(p.UpdateInterval()))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			p.Loop()
			timer.Reset(jitter(p.UpdateInterval()))
		}
	}
}

func (p *ReservationsPuller) runExperimentUpdater(ctx context.Context) {
	ticker := time.NewTicker(experimentUpdaterInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.updateExperiments()
		}
	}
}

// Loop runs a single loop of reservation pulling.
func (p *ProjectPuller) Loop() {
	p.loopLock.Lock()
	defer p.loopLock.Unlock()
	klog.V(4).Infof("Starting reservations.Puller loop: projectID='%s'", p.projectID)
	startTime := time.Now()
	p.lastLoopError = nil

	reservations, err := p.gceClient.FetchReservationsInProject(p.projectID)
	if err != nil {
		klog.Warningf("Couldn't get reservations from project %s: %v", p.projectID, err)
		p.lastLoopError = err
		return
	}
	filteredReservations, err := filterReservations(reservations, p.zones)
	if err != nil {
		klog.Errorf("Error when filtering reservations %v", err)
		p.lastLoopError = err
		return
	}
	if len(filteredReservations) > 0 {
		p.lastReservationSeen = time.Now()
	}
	p.SetReservations(filteredReservations)

	klog.V(4).Infof("reservations.Puller loop: projectID='%s' allReservations=%d, filteredReservations=%d, duration=%v", p.projectID, len(reservations), len(filteredReservations), time.Since(startTime))
}

func filterReservations(reservations []*gce_api.Reservation, zones []string) ([]*gce_api.Reservation, error) {
	result := make([]*gce_api.Reservation, 0, len(reservations))
	for _, reservation := range reservations {
		// Filter out unusable reservations
		if !IsReservationUsable(reservation, true) {
			continue
		}
		// Filter out reservations from other zones
		zonePresent := false
		for _, zone := range zones {
			zonePresent = zonePresent || (GetReservationZone(reservation) == zone)
		}
		if !zonePresent {
			continue
		}

		result = append(result, reservation)
	}
	return result, nil
}

// UpdateInterval shares the interval to wait between updates
func (p *ProjectPuller) UpdateInterval() time.Duration {
	p.loopLock.Lock()
	defer p.loopLock.Unlock()
	// if Cx is using reservations or last loop failed, refresh more often
	if p.lastLoopError != nil || time.Since(p.lastReservationSeen) < inactivityTimeThreshold {
		return activeSleep
	}
	return inactiveSleep
}

func jitter(duration time.Duration) time.Duration {
	msec := float64(duration.Milliseconds())
	return time.Duration(int64(msec*(1.0-jitterSize))+
		rand.Int63n(int64(msec*2.0*jitterSize))) * time.Millisecond
}

// ReservationsPuller pulls GCE reservations. Can pull from a set of projects.
type ReservationsPuller struct {
	gceClient                    AutoscalingInternalGceClient
	consumableReservationsClient consumablereservations.Client
	zones                        []string
	localProjectId               string

	mutex   sync.RWMutex
	context context.Context
	wg      sync.WaitGroup

	projectPullers     map[string]*ProjectPuller
	experimentsManager experiments.Manager

	consumableReservations                 []*gce_api.Reservation
	useConsumablePuller                    atomic.Bool
	consumableReservationExperimentEnabled atomic.Bool
	consumableLoopLock                     sync.Mutex
	lastConsumableLoopFailed               bool
	lastConsumableReservationSeen          time.Time
	// consumableReactivateAt tracks when the consumable puller should be reactivated after an error.
	consumableReactivateAt time.Time
}

func (p *ReservationsPuller) updateConsumablePullerInterval() time.Duration {
	p.consumableLoopLock.Lock()
	defer p.consumableLoopLock.Unlock()
	// if Cx is using reservations or last loop failed, refresh more often
	if p.lastConsumableLoopFailed || time.Since(p.lastConsumableReservationSeen) < inactivityTimeThreshold {
		return activeSleep
	}
	return inactiveSleep
}

// NewReservationsPuller builds a new Puller.
func NewReservationsPuller(gceClient AutoscalingInternalGceClient, consumableReservationsClient consumablereservations.Client, experimentsManager experiments.Manager, projectID string, enableConsumablePuller bool, location string) *ReservationsPuller {
	useConsumablePuller := false
	if experimentsManager != nil {
		// Only enable the consumable puller if experiments manager is reachable.
		useConsumablePuller = enableConsumablePuller
	}

	region, err := gkeutil.GetRegionFromLocation(location)
	if err != nil {
		klog.Errorf("Disabling puller, couldn't get region from location %s: %v, ", location, err)
		return nil
	}

	zones, err := gceClient.FetchZones(region)
	if err != nil {
		klog.Errorf("Disabling puller, couldn't get zones from region %s: %v, ", region, err)
		return nil
	}

	pp := &ReservationsPuller{
		gceClient:                     gceClient,
		consumableReservationsClient:  consumableReservationsClient,
		localProjectId:                projectID,
		projectPullers:                map[string]*ProjectPuller{},
		experimentsManager:            experimentsManager,
		lastConsumableReservationSeen: time.Now().Add(initialActivityTime - inactivityTimeThreshold),
		zones:                         zones,
	}

	pp.useConsumablePuller.Store(useConsumablePuller)

	// Adding local project puller with aggregate reservations enabled
	pp.AddProject(projectID)
	return pp
}

// updateExperiments queries experiments manager to check whether consumable reservation experiment
// is enabled or disabled while also persisting the result in the Puller state
func (p *ReservationsPuller) updateExperiments() {
	experimentEnabled := p.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.ConsumableReservationExperimentName, false)
	p.consumableReservationExperimentEnabled.CompareAndSwap(!experimentEnabled, experimentEnabled)
}

// GetReservations gets all pulled reservations.
//
// If the consumable reservations puller is activated, the result list is first populated by
// the ProjectPullers, then supplemented by the ListConsumableReservations API. This is
// required because ListConsumableReservations does not return shared Aggregate reservations.
func (p *ReservationsPuller) GetReservations() []*gce_api.Reservation {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// For now, when a reservation is returned by both the legacy puller and the new API, we
	// prefer the result from the legacy puller because is not set by the new API for
	// aggregate reservations.
	reservations := map[uint64]*gce_api.Reservation{}
	for _, puller := range p.projectPullers {
		rs := puller.GetReservations()
		for _, r := range rs {
			reservations[r.Id] = r

		}
	}

	if p.consumablePullerFeatureEnabled() {
		for _, r := range p.consumableReservations {
			if _, ok := reservations[r.Id]; !ok {
				reservations[r.Id] = r
			}
		}
	}

	result := make([]*gce_api.Reservation, 0, len(reservations))
	for _, r := range reservations {
		result = append(result, r)
	}
	return result
}

// GetLocalReservations gets pulled reservations in a local project.
func (p *ReservationsPuller) GetLocalReservations() []*gce_api.Reservation {
	return p.GetReservationsInProject(p.localProjectId)
}

// GetReservationsInProject gets all pulled reservations in a particular project. Empty
// list gets returned for a project that has not been pulled for.
//
// If the consumable reservations puller is activated, the result list is first populated by
// the ProjectPullers, then supplemented by the ListConsumableReservations API. This is
// required because ListConsumableReservations does not return shared Aggregate reservations.
func (p *ReservationsPuller) GetReservationsInProject(project string) []*gce_api.Reservation {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	reservations := map[uint64]*gce_api.Reservation{}
	pp := p.projectPullers[project]
	if pp != nil {
		for _, r := range pp.GetReservations() {
			reservations[r.Id] = r

		}
	}

	if p.consumablePullerFeatureEnabled() {
		for _, r := range p.consumableReservations {
			if GetReservationProject(r) == project {
				if _, ok := reservations[r.Id]; !ok {
					reservations[r.Id] = r
				}
			}
		}
	}

	result := make([]*gce_api.Reservation, 0, len(reservations))
	for _, r := range reservations {
		result = append(result, r)
	}
	return result
}

func (p *ReservationsPuller) SetReservations(reservations []*gce_api.Reservation) {
	// Organize by project
	reservationsByProject := make(map[string][]*gce_api.Reservation)
	for _, r := range reservations {
		proj := GetReservationProject(r)
		reservationsByProject[proj] = append(reservationsByProject[proj], r)
	}

	// Update pullers
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for k, pp := range p.projectPullers {
		pp.SetReservations(reservationsByProject[k])
	}
}

// LastLoopErrorInProject returns if an error occurred while fetching the reservations in the specified project
func (p *ReservationsPuller) LastLoopErrorInProject(project string) error {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	pp := p.projectPullers[project]
	if pp == nil {
		return fmt.Errorf("puller for project %s does not exist", project)
	}
	return pp.LastLoopError()
}

// LastLoopError returns the last error that occurred during the loop.
func (p *ProjectPuller) LastLoopError() error {
	p.loopLock.Lock()
	defer p.loopLock.Unlock()
	return p.lastLoopError
}

// AddProject adds the project to the set of projects while optionally
// allowing aggregate reservations to get discovered.
func (p *ReservationsPuller) AddProject(projectID string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if _, ok := p.projectPullers[projectID]; ok {
		return
	}

	puller := NewProjectPuller(p.gceClient, projectID, p.zones)
	p.projectPullers[projectID] = puller
	if p.context != nil {
		p.runProjectPuller(puller, p.context)
	}
}

// Run runs pulling on all the watched projects.
func (p *ReservationsPuller) Run(ctx context.Context) {
	if p.Running() {
		klog.Warning("More than one call to PullerSet.Run(), may result in same pullers on different threads.")
	}

	if p.experimentsManager != nil {
		p.updateExperiments()
		p.runAsync(p.runExperimentUpdater, ctx)
	}

	p.runAsync(p.runConsumablePuller, ctx)

	p.mutex.Lock()
	pullers := make([]*ProjectPuller, 0, len(p.projectPullers))
	for _, puller := range p.projectPullers {
		pullers = append(pullers, puller)
	}
	// Running() method checks if p.context is not nil. Therefore, it must be assigned.
	// Otherwise, there is a risk of a race condition described in b/454905749.
	p.context = ctx
	p.mutex.Unlock()

	for _, puller := range pullers {
		p.runProjectPuller(puller, ctx)
	}

	p.runAsync(p.runUpdateMetrics, ctx)
}

func (p *ReservationsPuller) runAsync(runner func(context.Context), ctx context.Context) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		runner(ctx)
	}()
}

// Wait blocks until all async goroutines have exited.
func (p *ReservationsPuller) Wait() {
	p.wg.Wait()
}

func (p *ReservationsPuller) runConsumablePuller(ctx context.Context) {
	timer := time.NewTimer(jitter(p.updateConsumablePullerInterval()))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			p.tryReenableConsumablePuller()

			if p.consumablePullerFeatureEnabled() {
				p.consumablePullerLoop(ctx)
			}
			timer.Reset(jitter(p.updateConsumablePullerInterval()))
		}
	}
}

func (p *ReservationsPuller) consumablePullerLoop(ctx context.Context) {
	p.consumableLoopLock.Lock()
	defer p.consumableLoopLock.Unlock()
	p.lastConsumableLoopFailed = false

	klog.V(4).Infof("Starting ConsumableReservationPuller.Pull: projectID='%s'", p.localProjectId)
	startTime := time.Now()

	apiCallAlwaysFailed := true
	var reservations []*gce_api.Reservation
	for _, zone := range p.zones {
		rs, err := p.consumableReservationsClient.FetchConsumableReservations(ctx, p.localProjectId, zone)
		if err != nil {
			if err.Type() == consumablereservations.ClientError {
				// Don't fallback here because the call may succeed in other zones. Only fallback if fails for all zones.
				klog.Errorf("Error when calling ListConsumableReservations() for zone %q: %v", zone, err)
			} else {
				// Unexpected internal error
				p.lastConsumableLoopFailed = true
				p.sleepConsumablePuller()
				return
			}
		} else {
			apiCallAlwaysFailed = false
			reservations = append(reservations, rs...)
		}
	}
	if apiCallAlwaysFailed {
		klog.Errorf("ListConsumableReservations() failed for all zones. Sleeping consumable reservations puller.")
		p.lastConsumableLoopFailed = true
		p.sleepConsumablePuller()
		return
	}
	// filter out unusable reservations
	filteredReservations, err := filterReservations(reservations, p.zones)
	if err != nil {
		klog.Errorf("Error when filtering reservations %v", err)
		p.lastConsumableLoopFailed = true
		return
	}
	if len(filteredReservations) > 0 {
		p.lastConsumableReservationSeen = time.Now()
	}
	p.mutex.Lock()
	p.consumableReservations = filteredReservations
	p.mutex.Unlock()
	klog.V(4).Infof("ConsumableReservationPuller.Pull: projectID='%s' allReservations=%d, duration=%v", p.localProjectId, len(filteredReservations), time.Since(startTime))
}

func (p *ReservationsPuller) sleepConsumablePuller() {
	p.setUseConsumablePuller(false)
	// Reactivate ConsumablePuller after some time threshold in the case of an API error.
	// To permanently deactivate, turn off the experiment.
	p.consumableReactivateAt = time.Now().Add(consumablePullerReactivateThreshold)
}

func (p *ReservationsPuller) setUseConsumablePuller(useConsumablePuller bool) {
	p.useConsumablePuller.CompareAndSwap(!useConsumablePuller, useConsumablePuller)
}

func (p *ReservationsPuller) consumablePullerFeatureEnabled() bool {
	return p.useConsumablePuller.Load() && p.consumableReservationExperimentEnabled.Load()
}

func (p *ReservationsPuller) tryReenableConsumablePuller() {
	p.consumableLoopLock.Lock()
	defer p.consumableLoopLock.Unlock()

	if !p.useConsumablePuller.Load() && !p.consumableReactivateAt.IsZero() {
		if time.Now().After(p.consumableReactivateAt) {
			p.setUseConsumablePuller(true)
			p.consumableReactivateAt = time.Time{}
		}
	}
}

// runProjectPuller runs a single loop before kicking off async run loop.
// this is used by clients to ensure there will be an initial set of data right
// after the call.
func (p *ReservationsPuller) runProjectPuller(pp *ProjectPuller, ctx context.Context) {
	pp.Loop()
	p.runAsync(pp.Run, ctx)
}

func (p *ReservationsPuller) Running() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.context != nil
}

func (p *ReservationsPuller) runUpdateMetrics(ctx context.Context) {
	timer := time.NewTimer(jitter(p.updateMetricsInterval()))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			p.updateMetrics()
			timer.Reset(jitter(p.updateMetricsInterval()))
		}
	}
}

func (p *ReservationsPuller) updateMetricsInterval() time.Duration {
	p.mutex.RLock()
	pullers := make([]*ProjectPuller, 0, len(p.projectPullers))
	for _, pp := range p.projectPullers {
		pullers = append(pullers, pp)
	}
	p.mutex.RUnlock()

	interval := inactiveSleep
	for _, pp := range pullers {
		ppi := pp.UpdateInterval()
		if ppi < interval {
			interval = ppi
		}
	}
	if cpi := p.updateConsumablePullerInterval(); cpi < interval {
		interval = cpi
	}
	return interval
}

func (p *ReservationsPuller) updateMetrics() {
	availableReservations := map[MetricLabels]int64{}
	for _, reservation := range p.GetReservations() {
		if reservation.SpecificReservation == nil {
			continue
		}
		available := reservation.SpecificReservation.Count - reservation.SpecificReservation.InUseCount
		availableReservations[ReservationMetricLabels(reservation)] += available
	}
	for labels, count := range availableReservations {
		metrics.Metrics.SetReservationsAvailable(labels.ToMap(), int(count))
	}
	metrics.Metrics.SetReservationsUseConsumablePuller(p.useConsumablePuller.Load())
}
