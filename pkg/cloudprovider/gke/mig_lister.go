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
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"google.golang.org/api/googleapi"

	machinerynet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	caerrors "sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
)

const (
	// MaxIrretrievableErrsBeforeBlocked is number of NodeGroup can be irretrievable before its blocked
	MaxIrretrievableErrsBeforeBlocked = 3
	// MarkedIrretrievableRefreshIntervalTime is the time before marked irretrievable list is refreshed
	MarkedIrretrievableRefreshIntervalTime = 24 * time.Hour
	// BlockListRefreshIntervalTime is the time before the blocked list is refreshed
	BlockListRefreshIntervalTime = 15 * time.Minute
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
	} else if isServerError(err) || isUnreachableError(err) {
		l.cache.MarkIrretrievableMig(migRef, l.maxIrretrievableErrsBeforeBlocked, IrretrievableMigReasonServerError)
	}
}

// isUnreachableError returns true if err is a transport-level failure, i.e. GCE
// never produced a usable HTTP response (DNS/dial failures, timeouts,
// connection refused/reset before or while reading the response).
//
// Any *googleapi.Error means GCE did respond, so it is never treated as
// unreachable, context.Canceled is not counted either, as it is caused by the caller.
func isUnreachableError(err error) bool {
	if err == nil {
		return false
	}
	var gErr *googleapi.Error
	if errors.As(err, &gErr) {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Any net.Error reporting Timeout(), including *url.Error (which delegates
	// to its cause), http.Client.Timeout and i/o deadlines on the connection.
	if machinerynet.IsTimeout(err) {
		return true
	}
	// Connection terminated by the peer or the transport: io.EOF /
	// io.ErrUnexpectedEOF (directly or as the cause of a *url.Error),
	// "connection reset by peer", http2 GOAWAY, closed network connection.
	if machinerynet.IsProbableEOF(err) {
		return true
	}
	// ECONNREFUSED / ECONNRESET anywhere in the chain
	// (*url.Error -> *net.OpError -> *os.SyscallError -> syscall.Errno).
	if machinerynet.IsConnectionRefused(err) || machinerynet.IsConnectionReset(err) {
		return true
	}
	// Hostname resolution failures (no such host, resolver unreachable); not
	// covered by the helpers above unless the lookup timed out.
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}

	return false
}

func isServerError(err error) bool {
	var gErr *googleapi.Error
	if errors.As(err, &gErr) {
		return IsStatusCodeServerErrorResponse(gErr.Code)
	}
	return false
}

func isNotFoundError(err error) bool {
	var gErr *googleapi.Error
	if errors.As(err, &gErr) {
		if gErr.Code == http.StatusNotFound {
			return true
		}
	}
	var caErr caerrors.AutoscalerError
	if errors.As(err, &caErr) {
		return caErr.Type() == caerrors.NodeGroupDoesNotExistError
	}
	return false
}

func isCloudProviderError(err error) bool {
	var caErr caerrors.AutoscalerError
	return errors.As(err, &caErr) && caErr.Type() == caerrors.CloudProviderError
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
