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

package gke

import (
	"fmt"
	"net/http"
	"time"

	"google.golang.org/api/googleapi"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

const (
	// maxIrretrievableErrsBeforeBlocked is number of NodeGroup can be irretrievable before its blocked
	maxIrretrievableErrsBeforeBlocked = 3
	// markedIrretrievableRefreshIntervalTime is the time before marked irretrievable list is refreshed
	markedIrretrievableRefreshIntervalTime = 24 * time.Hour
	// blockListRefreshIntervalTime is the time before the blocked list is refreshed
	blockListRefreshIntervalTime = 15 * time.Minute
)

// IrretrievableMigBlockReason informs why the MIG has been blocked.
type IrretrievableMigBlockReason int

func (r IrretrievableMigBlockReason) String() string {
	switch r {
	case IrretrievableMigReasonCloudProviderError:
		return "CloudProviderError"
	case IrretrievableMigReasonNotFound:
		return "NotFound"
	case IrretrievableMigReasonServerError:
		return "ServerError"
	default:
		return fmt.Sprintf("%d", r)
	}
}

var (
	// IrretrievableMigReasonCloudProviderError means a MIG has been blocked because it had incorrect request.
	IrretrievableMigReasonCloudProviderError IrretrievableMigBlockReason = -1
	// IrretrievableMigReasonNotFound means a MIG has been blocked because it could not be found.
	IrretrievableMigReasonNotFound IrretrievableMigBlockReason = 404
	// IrretrievableMigReasonServerError means a MIG has been blocked because data could not have been fetched due to a server error.
	IrretrievableMigReasonServerError IrretrievableMigBlockReason = 500
)

type gkeMigLister struct {
	cache                                  *GkeCache
	lastRefreshBlockList                   time.Time
	lastRefreshMarkedIrretrievable         time.Time
	markedIrretrievableRefreshIntervalTime time.Duration
	blockListRefreshIntervalTime           time.Duration
	maxIrretrievableErrsBeforeBlocked      int
}

// NewGkeMigLister returns an instance of gkeMigLister
func NewGkeMigLister(cache *GkeCache, blockListRefreshIntervalTime time.Duration, markedIrretrievableRefreshIntervalTime time.Duration, maxIrretrievableErrsBeforeBlocked int) *gkeMigLister {
	return &gkeMigLister{
		cache:                                  cache,
		lastRefreshBlockList:                   time.Now(),
		lastRefreshMarkedIrretrievable:         time.Now(),
		markedIrretrievableRefreshIntervalTime: markedIrretrievableRefreshIntervalTime,
		blockListRefreshIntervalTime:           blockListRefreshIntervalTime,
		maxIrretrievableErrsBeforeBlocked:      maxIrretrievableErrsBeforeBlocked,
	}
}

// GetMigs returns the list of migs
func (l *gkeMigLister) GetMigs() []gce.Mig {
	var result []gce.Mig
	for _, mig := range l.cache.GetMigs() {
		if !l.cache.IsMigBlocked(mig.GceRef()) {
			result = append(result, mig)
		}
	}
	return result
}

// GetGkeMigs returns the cached list of gkeMigs
func (l *gkeMigLister) GetGkeMigs() []*GkeMig {
	var result []*GkeMig
	for _, mig := range l.cache.GetGkeMigs() {
		if !l.cache.IsMigBlocked(mig.GceRef()) {
			result = append(result, mig)
		}
	}
	return result
}

// GetBlockedGkeMigs returns the cached list of gkeMigs blocked for the given reason
func (l *gkeMigLister) GetBlockedGkeMigs(reason IrretrievableMigBlockReason) []*GkeMig {
	var result []*GkeMig
	for _, mig := range l.cache.GetGkeMigs() {
		if blocked, blockReason := l.cache.BlockReason(mig.GceRef()); blocked && blockReason == reason {
			result = append(result, mig)
		}
	}
	return result
}

// GetGkeMigsLocations returns list of locations where nodes can be created by existing Migs.
func (l *gkeMigLister) GetGkeMigsLocations() []string {
	var results []string
	added := make(map[string]bool)
	for _, mig := range l.GetGkeMigs() {
		if !added[mig.gceRef.Zone] {
			added[mig.gceRef.Zone] = true
			results = append(results, mig.gceRef.Zone)
		}
	}
	return results
}

// HandleMigIssue handles an issue with a given mig
func (l *gkeMigLister) HandleMigIssue(migRef gce.GceRef, err error) {
	if isNotFoundError(err) {
		l.cache.MarkIrretrievableMig(migRef, l.maxIrretrievableErrsBeforeBlocked, IrretrievableMigReasonNotFound)
	} else if isServerError(err) {
		l.cache.MarkIrretrievableMig(migRef, l.maxIrretrievableErrsBeforeBlocked, IrretrievableMigReasonServerError)
	}
}

func isServerError(err error) bool {
	gErr, gOk := err.(*googleapi.Error)
	return gOk && IsStatusCodeServerErrorResponse(gErr.Code)
}

func isNotFoundError(err error) bool {
	gErr, gOk := err.(*googleapi.Error)
	caErr, caOk := err.(errors.AutoscalerError)
	return (gOk && gErr.Code == http.StatusNotFound) || (caOk && caErr.Type() == errors.NodeGroupDoesNotExistError)
}

func isCloudProviderError(err error) bool {
	caErr, caOk := err.(errors.AutoscalerError)
	return caOk && caErr.Type() == errors.CloudProviderError
}

// InvalidateIrretrievableMigsCacheIfExpired invalidates irretrievable Migs cache if it has expired
// It also collects previously blocked migs to validate them again.
func (l *gkeMigLister) InvalidateIrretrievableMigsCacheIfExpired() map[gce.GceRef]bool {
	prevBlockedMigs := map[gce.GceRef]bool{}
	if time.Now().After(l.lastRefreshBlockList.Add(l.blockListRefreshIntervalTime)) {
		prevBlockedMigs = l.cache.InvalidateBlockedIrretrievableMigs()
		l.lastRefreshBlockList = time.Now()
	}
	if time.Now().After(l.lastRefreshMarkedIrretrievable.Add(l.markedIrretrievableRefreshIntervalTime)) {
		l.cache.InvalidateMarkedIrretrievableMigs()
		l.lastRefreshMarkedIrretrievable = time.Now()
	}
	return prevBlockedMigs
}

// IsStatusCodeServerErrorResponse returns true if the given http status code is in the
// Server Error Response range, false otherwise
func IsStatusCodeServerErrorResponse(code int) bool {
	return code >= 500 && code < 600
}
