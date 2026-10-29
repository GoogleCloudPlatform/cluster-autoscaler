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

package capacitybuffers

import (
	"context"
	"errors"
	"fmt"
	"time"

	ccapiv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	ccapiv1ac "github.com/googlecloudplatform/compute-class-api/client/applyconfiguration/cloud.google.com/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	sweeperControllerName = "computeclass_capacitybuffers_sweeper"
	sweepInterval         = 5 * time.Minute
)

// Sweeper periodically deletes all CapacityBuffers and PodTemplates in
// NamespaceGkeManagedCCC and removes BuffersConfigured and ObservedBuffers from
// ComputeClass statuses when cleanup mode is enabled.
type Sweeper struct {
	Client client.Client
}

// SetupSweeper registers the periodic cleanup Sweeper and the capacity
// buffers metric collector with the controller manager.
func SetupSweeper(mgr ctrl.Manager) error {
	c, err := setupClientAndCollector(mgr)
	if err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		Named(sweeperControllerName).
		WatchesRawSource(source.Func(enqueueInitialSweep)).
		Complete(&Sweeper{Client: c})
}

func enqueueInitialSweep(ctx context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	q.Add(reconcile.Request{NamespacedName: types.NamespacedName{Name: NamespaceGkeManagedCCC}})
	return nil
}

// Reconcile deletes all CapacityBuffers in NamespaceGkeManagedCCC first, and once all
// CapacityBuffer deletions succeed, deletes all PodTemplates in NamespaceGkeManagedCCC
// and clears capacity buffer statuses from ComputeClasses. On success it requeues after
// sweepInterval; on error controller-runtime logs the error and retries with rate-limited backoff.
func (s *Sweeper) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var cbList cbv1beta1.CapacityBufferList
	if err := s.Client.List(ctx, &cbList, client.InNamespace(NamespaceGkeManagedCCC)); err != nil {
		return reconcile.Result{}, fmt.Errorf("listing CapacityBuffers: %w", err)
	}

	var cbErrs []error
	for i := range cbList.Items {
		cb := &cbList.Items[i]
		if cb.GetDeletionTimestamp() != nil {
			continue
		}
		if err := s.Client.Delete(ctx, cb); err != nil && !apierrors.IsNotFound(err) {
			cbErrs = append(cbErrs, fmt.Errorf("deleting CapacityBuffer %q: %w", cb.Name, err))
		}
	}
	if len(cbErrs) > 0 {
		return reconcile.Result{}, errors.Join(cbErrs...)
	}

	var errs []error
	var ptList corev1.PodTemplateList
	if err := s.Client.List(ctx, &ptList, client.InNamespace(NamespaceGkeManagedCCC)); err != nil {
		errs = append(errs, fmt.Errorf("listing PodTemplates: %w", err))
	} else {
		for i := range ptList.Items {
			pt := &ptList.Items[i]
			if pt.GetDeletionTimestamp() != nil {
				continue
			}
			if err := s.Client.Delete(ctx, pt); err != nil && !apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Errorf("deleting PodTemplate %q: %w", pt.Name, err))
			}
		}
	}

	var ccList ccapiv1.ComputeClassList
	if err := s.Client.List(ctx, &ccList); err != nil {
		errs = append(errs, fmt.Errorf("listing ComputeClasses: %w", err))
	} else {
		for i := range ccList.Items {
			cc := &ccList.Items[i]
			if cc.GetDeletionTimestamp() != nil || !hasBufferStatus(cc) {
				continue
			}
			// An empty apply removes the BuffersConfigured condition and observed buffers that fieldOwnerCCC owns.
			if err := applyBufferStatus(ctx, s.Client, cc.Name, ccapiv1ac.ComputeClassStatus()); err != nil && !apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Errorf("clearing buffer status of ComputeClass %q: %w", cc.Name, err))
			}
		}
	}
	if len(errs) > 0 {
		return reconcile.Result{}, errors.Join(errs...)
	}

	return reconcile.Result{RequeueAfter: sweepInterval}, nil
}
