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

package backoff

import (
	"sync"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	base_backoff "k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
)

// CSNCompositeBackoff handles backoff logic specifically for CSN (Cold Standby Nodes, publicly known as standby buffers) resumption errors.
//
// Specialized CSN backoff is needed because CSN node resumption operations can produce
// error codes that are unique to CSN and shouldn't affect scale-up backoff.
//
// When a node resumption error occurs:
// - Standard GCE errors (recognized by gce.GetErrorInfo, such as quota exhaustion or stockouts) trigger backoff in both scale-up and CSN backoff.
// - CSN-specific errors (unrecognized by gce.GetErrorInfo) trigger only CSN backoff.
//
// For design details, see go/csn-backoff-strategy.
type CSNCompositeBackoff struct {
	CompositeBackoff
	csnBackoff base_backoff.Backoff
	mu         sync.Mutex
}

// NewCSNCompositeBackoff creates a new CSNCompositeBackoff combining a composite backoff and a dedicated CSN backoff while allowing for direct access to the CSN backoff.
func NewCSNCompositeBackoff(composite base_backoff.Backoff, csnBackoff base_backoff.Backoff) *CSNCompositeBackoff {
	syncComposite := NewSynchronizedCompositeBackoff([]base_backoff.Backoff{composite, csnBackoff}, nil)
	return &CSNCompositeBackoff{
		CompositeBackoff: syncComposite,
		csnBackoff:       csnBackoff,
	}
}

// ReportResumptionError processes and triggers backoff for errors encountered during CSN node resumption.
// If the error code corresponds to a known GCE instance error (e.g., quota or stockout, recognized by gce.GetErrorInfo), it delegates to the CompositeBackoff (which backoffs both scale-up and CSN backoffs).
// Otherwise, for CSN-specific errors, it constructs an InstanceErrorInfo with cloudprovider.OtherErrorClass and triggers backoff on csnBackoff.
func (c *CSNCompositeBackoff) ReportResumptionError(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorCode, errorMessage, instanceStatus string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	errInfo := gce.GetErrorInfo(errorCode, errorMessage, instanceStatus, nil)

	if errInfo == nil { // CSN specific error.
		errInfo = &cloudprovider.InstanceErrorInfo{
			ErrorClass:   cloudprovider.OtherErrorClass,
			ErrorCode:    errorCode,
			ErrorMessage: errorMessage,
		}

		c.csnBackoff.Backoff(nodeGroup, nodeInfo, *errInfo, time.Now())
		return
	}

	if errInfo.ErrorMessage == "" {
		errInfo.ErrorMessage = errorMessage
	}

	c.CompositeBackoff.Backoff(nodeGroup, nodeInfo, *errInfo, time.Now())
}
