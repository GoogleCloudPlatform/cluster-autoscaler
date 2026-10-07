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
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/api/googleapi"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_errors "sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
)

type scaleUpTimeProviderMock struct {
	mock.Mock
}

func (p *scaleUpTimeProviderMock) NodeGroupScaleUpTime(nodeGroup cloudprovider.NodeGroup) (time.Time, error) {
	args := p.Called(nodeGroup)
	return args.Get(0).(time.Time), args.Error(1)
}

func TestGkeCache(t *testing.T) {
	gkeCache := NewGkeCache(&MockGceCache{}, nodetemplate.NewCache())
	gceRefA := gce.GceRef{
		Project: "project1",
		Zone:    "zone",
		Name:    "instanceA",
	}

	gceRefB := gce.GceRef{
		Project: "project1",
		Zone:    "zone",
		Name:    "instanceB",
	}
	gkeMigA := &GkeMig{
		gceRef: gceRefA,
	}
	gkeMigB := &GkeMig{
		gceRef: gceRefB,
	}

	gkeCache.RegisterMig(gkeMigA)
	gkeCache.RegisterMig(gkeMigB)

	migList := gkeCache.GetGkeMigs()
	assert.Equal(t, 2, len(migList))
	assert.Contains(t, migList, gkeMigA)
	assert.Contains(t, migList, gkeMigB)

	gkeCache.UnregisterMig(gkeMigB)
	migList = gkeCache.GetGkeMigs()
	assert.Equal(t, 1, len(migList))
	assert.NotContains(t, migList, gkeMigB)
	gkeCache.UnregisterMig(gkeMigA)
	assert.Equal(t, 0, len(gkeCache.GetGkeMigs()))
}

func TestGkeCacheNodePoolToMigsMap(t *testing.T) {
	gkeCache := NewGkeCache(&MockGceCache{}, nodetemplate.NewCache())
	gceRefA := gce.GceRef{
		Project: "project1",
		Zone:    "zone1",
		Name:    "instanceA",
	}

	gceRefB := gce.GceRef{
		Project: "project1",
		Zone:    "zone1",
		Name:    "instanceB",
	}
	gceRefC := gce.GceRef{
		Project: "project1",
		Zone:    "zone2",
		Name:    "instanceC",
	}
	gkeMigA := &GkeMig{
		gceRef: gceRefA,
	}
	gkeMigC := &GkeMig{
		gceRef: gceRefC,
	}
	gkeMigB := &GkeMig{
		gceRef: gceRefB,
	}
	AddMigsToNodePool("foo", gkeMigA, gkeMigC)
	AddMigsToNodePool("bar", gkeMigB)

	gkeCache.RegisterMig(gkeMigA)
	gkeCache.RegisterMig(gkeMigB)
	gkeCache.RegisterMig(gkeMigC)

	fooMigs := gkeCache.ExistingMigsInNodePool("foo")
	assert.Equal(t, 2, len(fooMigs))
	assert.Contains(t, fooMigs, gkeMigA)
	assert.Contains(t, fooMigs, gkeMigC)

	barMigs := gkeCache.ExistingMigsInNodePool("bar")
	assert.Equal(t, 1, len(barMigs))
	assert.Contains(t, barMigs, gkeMigB)

	gkeCache.UnregisterMig(gkeMigA)
	fooMigs = gkeCache.ExistingMigsInNodePool("foo")
	assert.Equal(t, 1, len(fooMigs))
	assert.Contains(t, fooMigs, gkeMigC)

	gkeCache.UnregisterMig(gkeMigB)
	assert.Equal(t, 0, len(gkeCache.ExistingMigsInNodePool("bar")))
}

func TestGkeCacheUnregisterNodePool(t *testing.T) {
	gkeCache := NewGkeCache(&MockGceCache{}, nodetemplate.NewCache())
	gceRefA := gce.GceRef{
		Project: "project1",
		Zone:    "zone1",
		Name:    "instanceA",
	}

	gceRefB := gce.GceRef{
		Project: "project1",
		Zone:    "zone1",
		Name:    "instanceB",
	}
	gceRefC := gce.GceRef{
		Project: "project1",
		Zone:    "zone2",
		Name:    "instanceC",
	}
	gkeMigA := &GkeMig{
		gceRef: gceRefA,
	}
	gkeMigC := &GkeMig{
		gceRef: gceRefC,
	}
	gkeMigB := &GkeMig{
		gceRef: gceRefB,
	}
	AddMigsToNodePool("foo", gkeMigA, gkeMigC)
	AddMigsToNodePool("bar", gkeMigB)

	gkeCache.RegisterMig(gkeMigA)
	gkeCache.RegisterMig(gkeMigB)
	gkeCache.RegisterMig(gkeMigC)

	fooMigs := gkeCache.ExistingMigsInNodePool("foo")
	assert.Equal(t, 2, len(fooMigs))

	gkeCache.UnregisterNodePool("foo")

	assert.Equal(t, 0, len(gkeCache.ExistingMigsInNodePool("foo")))

	barMigs := gkeCache.ExistingMigsInNodePool("bar")
	assert.Equal(t, 1, len(barMigs))
	assert.Contains(t, barMigs, gkeMigB)
}

const (
	expiredCacheValidity = -1 * time.Second
)

func TestZonesInRegionCache(t *testing.T) {
	cache := NewGkeCache(&MockGceCache{}, nodetemplate.NewCache())

	regionA := "us-central1"
	zonesA := []string{"us-central1-c", "us-central1-a", "us-central1-b"}
	cache.SetZonesInRegion(regionA, zonesA)
	zones, found := cache.GetZonesInRegion(regionA)
	assert.True(t, found)
	assert.Equal(t, zonesA, zones)

	regionB := "europe-test"
	zonesB := []string{"europe-test-c", "europe-test-a", "europe-test-b"}
	cache.zonesInRegionCache.Set(
		regionB,
		zonesB,
		expiredCacheValidity,
	)
	zones, found = cache.GetZonesInRegion(regionB)
	assert.False(t, found)
	assert.Equal(t, []string(nil), zones)
}

func TestStandardZonesInRegionCache(t *testing.T) {
	testCases := []struct {
		name      string
		region    string
		zones     []string
		validity  time.Duration
		wantFound bool
		wantZones []string
	}{
		{
			name:      "valid_cache",
			region:    "us-central1",
			zones:     []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			validity:  time.Hour,
			wantFound: true,
			wantZones: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
		},
		{
			name:      "expired_cache",
			region:    "us-central1",
			zones:     []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			validity:  expiredCacheValidity,
			wantFound: false,
			wantZones: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewGkeCache(&MockGceCache{}, nodetemplate.NewCache())
			cache.standardZonesInRegionCache.Set(tc.region, tc.zones, tc.validity)
			zones, found := cache.GetStandardZonesInRegion(tc.region)
			assert.Equal(t, tc.wantFound, found)
			assert.Equal(t, tc.wantZones, zones)
		})
	}
}

func TestAIZonesInRegionCache(t *testing.T) {
	cache := NewGkeCache(&MockGceCache{}, nodetemplate.NewCache())

	regionA := "us-central1"
	zonesA := []string{"us-central1-ai1", "us-central1-ai2", "us-central1-ai3"}
	cache.SetAIZonesInRegion(regionA, zonesA)
	zones, found := cache.GetAIZonesInRegion(regionA)
	assert.True(t, found)
	assert.Equal(t, zonesA, zones)

	regionB := "europe-test"
	zonesB := []string{"europe-test-ai1", "europe-test-ai2", "europe-test-ai3"}
	cache.aiZonesInRegionCache.Set(
		regionB,
		zonesB,
		expiredCacheValidity,
	)
	zones, found = cache.GetAIZonesInRegion(regionB)
	assert.False(t, found)
	assert.Equal(t, []string(nil), zones)
}

func TestPreviouslyBlockedIrretrievableMigs(t *testing.T) {
	cache := NewGkeCache(&MockGceCache{}, nodetemplate.NewCache())

	gceRefA := gce.GceRef{
		Project: "project1",
		Zone:    "zone",
		Name:    "instanceA",
	}

	gceRefB := gce.GceRef{
		Project: "project1",
		Zone:    "zone",
		Name:    "instanceB",
	}

	gceRefC := gce.GceRef{
		Project: "project1",
		Zone:    "zone",
		Name:    "instanceC",
	}

	cache.MarkIrretrievableMig(gceRefA, 0, IrretrievableMigReasonCloudProviderError)
	cache.MarkIrretrievableMig(gceRefB, 0, IrretrievableMigReasonCloudProviderError)
	cache.MarkIrretrievableMig(gceRefC, 0, IrretrievableMigReasonServerError)

	blockedMigs := cache.InvalidateBlockedIrretrievableMigs()
	assert.Equal(t, 2, len(blockedMigs))
}

func TestNodesScaleDownAllowedCache(t *testing.T) {
	cache := NewGkeCache(&MockGceCache{}, nodetemplate.NewCache())

	// Test nil handling
	cache.updateNodesScaleDownAllowed(nil)
	assert.Equal(t, map[string]bool{}, cache.getNodesScaleDownAllowed(nil))

	// Test changing cache without update
	cache.nodesScaleDownAllowed = map[string]bool{"node-1": true, "node-2": false}
	nodesScaleDownAllowed := cache.getNodesScaleDownAllowed([]string{"node-1", "node-2"})
	nodesScaleDownAllowed["node-2"] = true
	nodesScaleDownAllowed["node-3"] = true
	assert.Equal(t, map[string]bool{"node-1": true, "node-2": false}, cache.getNodesScaleDownAllowed([]string{"node-1", "node-2", "node-3"}))

	// Test changing cache with update
	cache.updateNodesScaleDownAllowed(nodesScaleDownAllowed)
	assert.Equal(t, map[string]bool{"node-1": true, "node-2": true, "node-3": true}, cache.getNodesScaleDownAllowed([]string{"node-1", "node-2", "node-3"}))

	// Test cache invalidation
	cache.invalidateNodesScaleDownAllowed()
	assert.Equal(t, map[string]bool{}, cache.getNodesScaleDownAllowed([]string{"node-1", "node-2", "node-3"}))
}

func TestGkeCacheScaleUpTime(t *testing.T) {
	now := time.Now()
	ref := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "ref1"}
	mig := &GkeMig{gceRef: ref}

	cache := NewGkeCache(&MockGceCache{}, nodetemplate.NewCache())

	// uninitialized ScaleUpTimeProvider
	_, gotErr := cache.ScaleUpTime(ref)
	assert.ErrorContains(t, gotErr, "nil scaleUpTimeProvider")

	// unregistered MIG
	provider := &scaleUpTimeProviderMock{}
	provider.On("NodeGroupScaleUpTime", mock.Anything).Return(now, nil)
	cache.scaleUpTimeProvider = provider
	_, gotErr = cache.ScaleUpTime(ref)
	assert.ErrorContains(t, gotErr, "failed to find GkeMig for migRef")

	// registered MIG
	cache.RegisterMig(mig)
	gotScaleUpTime, gotErr := cache.ScaleUpTime(ref)
	assert.Equal(t, now, gotScaleUpTime)
	assert.Nil(t, gotErr)
}

func TestGkeCacheCapacityCheckWaitTimeSeconds(t *testing.T) {
	gkeManager := &GkeManagerMock{}
	ref := gce.GceRef{Project: "project1", Zone: "zoneA", Name: "ref1"}
	mig := &GkeMig{gceRef: ref, gkeManager: gkeManager}

	// unregistered MIG
	cache := NewGkeCache(&MockGceCache{}, nodetemplate.NewCache())
	_, gotErr := cache.CapacityCheckWaitTimeSeconds(ref)
	assert.ErrorContains(t, gotErr, "failed to find GkeMig for migRef")

	// returns CapacityCheckWaitTimeSeconds
	cache.RegisterMig(mig)
	gkeManager.On("EvaluateCapacityCheckWaitTimeSeconds", mig).Return(time.Second, nil)
	got, gotErr := cache.CapacityCheckWaitTimeSeconds(ref)
	assert.Equal(t, time.Second, got)
	assert.Nil(t, gotErr)
}

func TestNodePoolSpecCacheInvalidation(t *testing.T) {
	cache := NewGkeCache(&MockGceCache{}, nodetemplate.NewCache())
	cache.RegisterNodePoolSpecs(map[string]*gkeclient.NodePoolSpec{"something": {}})

	assert.Len(t, cache.nodePoolSpecs, 1)
	cache.InvalidateNodePoolSpecCache()
	assert.Len(t, cache.nodePoolSpecs, 0)
	spec, found := cache.GetNodePoolSpec("something")
	assert.False(t, found)
	assert.Nil(t, spec)
}

func TestNodePoolSpecCache(t *testing.T) {
	cache := NewGkeCache(&MockGceCache{}, nodetemplate.NewCache())
	registeredSpec := &gkeclient.NodePoolSpec{MachineType: "machine-type"}

	spec, found := cache.GetNodePoolSpec("something")
	assert.False(t, found)
	assert.Nil(t, spec)

	cache.RegisterNodePoolSpecs(map[string]*gkeclient.NodePoolSpec{"something": registeredSpec})
	spec, found = cache.GetNodePoolSpec("something")
	assert.True(t, found)
	assert.Equal(t, registeredSpec, spec)
}

func TestMachineTypeErrorCachePolicy(t *testing.T) {
	tests := []struct {
		name            string
		err             error
		wantTTL         time.Duration
		wantShouldCache bool
	}{
		{
			name:            "429 rate limit error is cacheable with 30s TTL",
			err:             &googleapi.Error{Code: http.StatusTooManyRequests, Message: "Too Many Requests"},
			wantTTL:         machineTypeRateLimitCacheValidity,
			wantShouldCache: true,
		},
		{
			name:            "404 API error is cacheable with 5m TTL",
			err:             &googleapi.Error{Code: http.StatusNotFound, Message: "Not Found"},
			wantTTL:         MachineTypeErrorCacheValidity,
			wantShouldCache: true,
		},
		{
			name:            "400 API error is cacheable with 5m TTL",
			err:             &googleapi.Error{Code: http.StatusBadRequest, Message: "Bad Request"},
			wantTTL:         MachineTypeErrorCacheValidity,
			wantShouldCache: true,
		},
		{
			name:            "500 API error is not cacheable",
			err:             &googleapi.Error{Code: http.StatusInternalServerError, Message: "Internal Server Error"},
			wantTTL:         0,
			wantShouldCache: false,
		},
		{
			name:            "Generic non-API error is not cacheable",
			err:             errors.New("some generic error"),
			wantTTL:         0,
			wantShouldCache: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotTTL, gotShouldCache := machineTypeErrorCachePolicy(tc.err)
			assert.Equal(t, tc.wantTTL, gotTTL)
			assert.Equal(t, tc.wantShouldCache, gotShouldCache)
		})
	}
}

// testTimeoutError is a net.Error reporting a timeout, as returned by the HTTP
// transport when a request to GCE times out.
type testTimeoutError struct{}

func (testTimeoutError) Error() string   { return "i/o timeout" }
func (testTimeoutError) Timeout() bool   { return true }
func (testTimeoutError) Temporary() bool { return true }

func TestGkeMigListerHandleMigIssue(t *testing.T) {
	gceRef := gce.GceRef{
		Project: "project1",
		Zone:    "zone",
		Name:    "mig1",
	}

	tests := map[string]struct {
		err         error
		wantBlocked bool
		wantReason  IrretrievableMigBlockReason
	}{
		"GoogleAPINotFound": {
			err:         &googleapi.Error{Code: http.StatusNotFound, Message: "Not Found"},
			wantBlocked: true,
			wantReason:  IrretrievableMigReasonNotFound,
		},
		"GkeAuthNotFoundWrappedInURLError": {
			err: &url.Error{
				Op:  "Post",
				URL: "https://compute.googleapis.com/...",
				Err: fmt.Errorf("token request failed: %w", &googleapi.Error{
					Code:    http.StatusNotFound,
					Message: "Not Found",
				}),
			},
			wantBlocked: true,
			wantReason:  IrretrievableMigReasonNotFound,
		},
		"WrappedNodeGroupDoesNotExistError": {
			err: fmt.Errorf("helper failed: %w", ca_errors.NewAutoscalerError(
				ca_errors.NodeGroupDoesNotExistError,
				"Node group does not exist",
			)),
			wantBlocked: true,
			wantReason:  IrretrievableMigReasonNotFound,
		},
		"GoogleAPIServiceUnavailable": {
			err:         &googleapi.Error{Code: http.StatusServiceUnavailable, Message: "Service Unavailable"},
			wantBlocked: true,
			wantReason:  IrretrievableMigReasonServerError,
		},
		"GoogleAPIServiceUnavailableWrappedWithFmt": {
			err:         fmt.Errorf("x: %w", &googleapi.Error{Code: http.StatusServiceUnavailable, Message: "Service Unavailable"}),
			wantBlocked: true,
			wantReason:  IrretrievableMigReasonServerError,
		},
		"GkeAuthServerErrorWrappedInURLError": {
			err: &url.Error{
				Op:  "Post",
				URL: "https://compute.googleapis.com/...",
				Err: fmt.Errorf("token request failed: %w", &googleapi.Error{
					Code:    http.StatusInternalServerError,
					Message: "Internal Server Error",
				}),
			},
			wantBlocked: true,
			wantReason:  IrretrievableMigReasonServerError,
		},
		"GoogleAPITooManyRequestsDoesNotBlock": {
			err: &googleapi.Error{
				Code:    http.StatusTooManyRequests,
				Message: "Rate Limit Exceeded",
				Errors:  []googleapi.ErrorItem{{Reason: "rateLimitExceeded"}},
			},
			wantBlocked: false,
		},
		"GoogleAPIBadRequestDoesNotBlock": {
			err:         &googleapi.Error{Code: http.StatusBadRequest, Message: "Bad Request"},
			wantBlocked: false,
		},
		"ContextDeadlineExceeded": {
			err:         context.DeadlineExceeded,
			wantBlocked: true,
			wantReason:  IrretrievableMigReasonServerError,
		},
		"WrappedContextDeadlineExceeded": {
			err:         fmt.Errorf("failed to list managed instances: %w", context.DeadlineExceeded),
			wantBlocked: true,
			wantReason:  IrretrievableMigReasonServerError,
		},
		"TimeoutWrappedInURLError": {
			err: &url.Error{
				Op:  "Get",
				URL: "https://compute.googleapis.com/...",
				Err: testTimeoutError{},
			},
			wantBlocked: true,
			wantReason:  IrretrievableMigReasonServerError,
		},
		"DNSError": {
			err: &url.Error{
				Op:  "Get",
				URL: "https://compute.googleapis.com/...",
				Err: &net.DNSError{Err: "no such host", Name: "compute.googleapis.com"},
			},
			wantBlocked: true,
			wantReason:  IrretrievableMigReasonServerError,
		},
		"ConnectionRefusedWrappedInURLError": {
			err: &url.Error{
				Op:  "Get",
				URL: "https://compute.googleapis.com/...",
				Err: &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}},
			},
			wantBlocked: true,
			wantReason:  IrretrievableMigReasonServerError,
		},
		"HTTP2GoAwayWrappedInURLError": {
			err: &url.Error{
				Op:  "Get",
				URL: "https://compute.googleapis.com/...",
				Err: errors.New("http2: server sent GOAWAY and closed the connection; LastStreamID=1, ErrCode=NO_ERROR, debug=\"\""),
			},
			wantBlocked: true,
			wantReason:  IrretrievableMigReasonServerError,
		},
		"BareOpErrorFromBodyRead": {
			err:         fmt.Errorf("decode: %w", &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}),
			wantBlocked: true,
			wantReason:  IrretrievableMigReasonServerError,
		},
		"BareTimeoutFromBodyRead": {
			err:         fmt.Errorf("decode: %w", testTimeoutError{}),
			wantBlocked: true,
			wantReason:  IrretrievableMigReasonServerError,
		},
		"UnexpectedEOFWrappedInURLError": {
			err: &url.Error{
				Op:  "Get",
				URL: "https://compute.googleapis.com/...",
				Err: io.ErrUnexpectedEOF,
			},
			wantBlocked: true,
			wantReason:  IrretrievableMigReasonServerError,
		},
		"UnexpectedEOFNotInURLErrorDoesNotBlock": {
			err:         fmt.Errorf("decode: %w", io.ErrUnexpectedEOF),
			wantBlocked: false,
		},
		"URLParseErrorDoesNotBlock": {
			err: &url.Error{
				Op:  "parse",
				URL: "://compute.googleapis.com",
				Err: errors.New("missing protocol scheme"),
			},
			wantBlocked: false,
		},
		"ContextCanceledDoesNotBlock": {
			err:         context.Canceled,
			wantBlocked: false,
		},
		"ContextCanceledWrappedInURLErrorDoesNotBlock": {
			err: &url.Error{
				Op:  "Get",
				URL: "https://compute.googleapis.com/...",
				Err: context.Canceled,
			},
			wantBlocked: false,
		},
		"GenericErrorDoesNotBlock": {
			err:         errors.New("some generic error"),
			wantBlocked: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cache := NewGkeCache(&MockGceCache{}, nodetemplate.NewCache())
			// maxIrretrievableErrsBeforeBlocked = 2
			migLister := NewGkeMigLister(cache, 15*time.Minute, 24*time.Hour, 2)

			// First failure - should mark but not block
			migLister.HandleMigIssue(gceRef, tc.err)
			assert.False(t, cache.IsMigBlocked(gceRef))

			// Second failure
			migLister.HandleMigIssue(gceRef, tc.err)

			if tc.wantBlocked {
				assert.True(t, cache.IsMigBlocked(gceRef))
				blocked, reason := cache.BlockReason(gceRef)
				assert.True(t, blocked)
				assert.Equal(t, tc.wantReason, reason)
			} else {
				assert.False(t, cache.IsMigBlocked(gceRef))
			}
		})
	}
}

func TestGetBlockedGkeMigsByReason(t *testing.T) {
	notFoundRef := gce.GceRef{Project: "project1", Zone: "zone", Name: "not-found"}
	serverErrorRef := gce.GceRef{Project: "project1", Zone: "zone", Name: "server-error"}
	unreachableRef := gce.GceRef{Project: "project1", Zone: "zone", Name: "unreachable"}

	tests := map[string]struct {
		reason   IrretrievableMigBlockReason
		wantRefs []gce.GceRef
	}{
		"NotFound": {
			reason:   IrretrievableMigReasonNotFound,
			wantRefs: []gce.GceRef{notFoundRef},
		},
		"ServerErrorIncludesUnreachable": {
			reason:   IrretrievableMigReasonServerError,
			wantRefs: []gce.GceRef{serverErrorRef, unreachableRef},
		},
		"CloudProviderErrorIsEmpty": {
			reason:   IrretrievableMigReasonCloudProviderError,
			wantRefs: nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cache := NewGkeCache(&MockGceCache{}, nodetemplate.NewCache())
			for _, ref := range []gce.GceRef{notFoundRef, serverErrorRef, unreachableRef} {
				cache.RegisterMig(&GkeMig{gceRef: ref})
			}
			migLister := NewGkeMigLister(cache, 15*time.Minute, 24*time.Hour, 1)
			migLister.HandleMigIssue(notFoundRef, &googleapi.Error{Code: http.StatusNotFound})
			migLister.HandleMigIssue(serverErrorRef, &googleapi.Error{Code: http.StatusServiceUnavailable})
			migLister.HandleMigIssue(unreachableRef, context.DeadlineExceeded)

			var gotRefs []gce.GceRef
			for _, mig := range migLister.GetBlockedGkeMigs(tc.reason) {
				gotRefs = append(gotRefs, mig.GceRef())
			}
			assert.ElementsMatch(t, tc.wantRefs, gotRefs)
			assert.Empty(t, migLister.GetMigs())
		})
	}
}
