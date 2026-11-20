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

package autoscaler

import (
	"context"
	"fmt"
	"slices"
	"time"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	informer "github.com/googlecloudplatform/compute-class-api/client/informers/externalversions"
	ccclister "github.com/googlecloudplatform/compute-class-api/client/listers/cloud.google.com/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

// WaitForOwnedComputeClass blocks until this CA instance owns a ComputeClass,
// stand-by mode is disabled by StandbyModeFeature (StandbyMode::Enabled=false
// or StandbyMode::MinCAVersion above this CA version), or ctx is done.
//
// A CA instance that handles only annotated ComputeClasses, e.g. the solver,
// has nothing to do until a ComputeClass is annotated for it. Waiting before
// the autoscaler is built saves the memory, CPU and API calls of its informers,
// background controllers and autoscaling loop while the instance isn't used.
// Only a ComputeClass informer (plus experiment updates) runs while waiting;
// the ComputeClass informer is stopped, without waiting for it, when this
// returns.
//
// It checks the ComputeClass informer's cache and the experiment flags every
// opts.ScanInterval. It returns immediately in a single CA, in the instance
// handling unannotated ComputeClasses, or when stand-by mode is disabled by
// StandbyModeFeature.
func WaitForOwnedComputeClass(ctx context.Context, opts internalopts.AutoscalingOptions, experimentsManager experiments.Manager) error {
	if internalopts.HandlesUnannotated(opts.AutoscalerNames) {
		return nil
	}
	if !isStandbyEnabled(experimentsManager) {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	_, kubeConfigJSON := KubeConfig(opts)
	factory := informer.NewSharedInformerFactory(MustCreateNpcCrdClient(kubeConfigJSON).CccClient(), 0)
	cccLister := factory.Cloud().V1().ComputeClasses().Lister()
	klog.Infof("Stand-by mode: syncing ComputeClasses to check if this autoscaler instance (autoscalerNames=%v) owns one.", opts.AutoscalerNames)
	factory.Start(ctx.Done())
	for _, synced := range factory.WaitForCacheSync(ctx.Done()) {
		if !synced {
			return fmt.Errorf("ComputeClass informer didn't sync: %w", ctx.Err())
		}
	}
	return waitForOwnedComputeClass(ctx, cccLister, opts.AutoscalerNames, opts.ScanInterval, experimentsManager)
}

func isStandbyEnabled(experimentsManager experiments.Manager) bool {
	if experimentsManager == nil {
		klog.Warning("Stand-by mode: experimentsManager is nil, disabling stand-by and starting.")
		return false
	}
	enabled := experimentsManager.IsFeatureEnabled(experiments.StandbyModeFeature, true)
	if !enabled {
		klog.Infof("Stand-by mode: disabled by experiment %s, starting.", experiments.StandbyModeFeature)
	}
	return enabled
}

func waitForOwnedComputeClass(ctx context.Context, cccLister ccclister.ComputeClassLister, autoscalerNames []string, interval time.Duration, experimentsManager experiments.Manager) error {
	ownsComputeClass := func() bool {
		cccs, err := cccLister.List(labels.Everything())
		if err != nil {
			// Listing from the informer's cache doesn't fail in practice. If it
			// ever does, starting is safer than waiting forever.
			klog.Errorf("Stand-by mode: listing ComputeClasses failed, starting anyway: %v", err)
			return true
		}
		owned := slices.ContainsFunc(cccs, func(ccc *cccv1.ComputeClass) bool {
			return npc_lister.IsOwned(ccc.Annotations, autoscalerNames)
		})
		if owned {
			klog.Info("Stand-by mode: found a ComputeClass owned by this autoscaler instance, starting.")
		}
		return owned
	}
	if !isStandbyEnabled(experimentsManager) || ownsComputeClass() {
		return nil
	}

	klog.Infof("Stand-by mode: no ComputeClasses owned by this autoscaler instance (autoscalerNames=%v), waiting for one before starting.", autoscalerNames)
	return wait.PollUntilContextCancel(ctx, interval, false, func(context.Context) (bool, error) {
		return !isStandbyEnabled(experimentsManager) || ownsComputeClass(), nil
	})
}
