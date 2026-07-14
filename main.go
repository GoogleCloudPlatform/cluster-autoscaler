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

package main

import (
	ctx "context"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	cqv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacityquota/autoscaling.x-k8s.io/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/apiserver/pkg/server/routes"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/loop"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	kube_client "k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/leaderelection"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"k8s.io/client-go/tools/leaderelection/resourcelock"
	kube_flag "k8s.io/component-base/cli/flag"
	componentbaseconfig "k8s.io/component-base/config"
	componentopts "k8s.io/component-base/config/options"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/gke-autoscaling/cluster-autoscaler/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/cli"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/gkedebuggingsnapshot"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/multitenancy"
	"k8s.io/klog/v2"
	schedulermetrics "k8s.io/kubernetes/pkg/scheduler/metrics"
)

const (
	defaultLeaseDuration = 15 * time.Second
	defaultRenewDeadline = 10 * time.Second
	defaultRetryPeriod   = 2 * time.Second
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(cbv1beta1.AddToScheme(scheme))
	utilruntime.Must(cqv1beta1.AddToScheme(scheme))
}

func run(healthCheck *metrics.HealthCheck, optsTracker *optstracking.OptionsTracker, gkeDebuggingSnapshotter *gkedebuggingsnapshot.GkeDebuggingSnapshotter) {
	context := ctrl.SetupSignalHandler()

	schedulermetrics.Register()
	metrics.RegisterAll(false)
	internalmetrics.RegisterAll()

	opts := optsTracker.Options()
	restConfig := kube_util.GetKubeConfig(opts.KubeClientOpts)
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme: scheme,
		Cache: cache.Options{
			DefaultTransform: cache.TransformStripManagedFields(),
		},
		// TODO: migrate leader election, metrics, healthcheck, pprof servers to Manager
		LeaderElection:          false,
		Metrics:                 metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress:  "0",
		PprofBindAddress:        "0",
		GracefulShutdownTimeout: new(time.Duration(-1)), // disabled timeout for compatibility, TODO: configure reasonable timeout
	})
	if err != nil {
		klog.Fatalf("Failed to create manager: %v", err)
	}

	builder := initBuilder(context, optsTracker, mgr)
	autoscaler, trigger, err := builder.Build(context, gkeDebuggingSnapshotter, config.OsReservedContent)

	if err != nil {
		klog.Fatalf("Failed to create autoscaler: %v", err)
	}

	// Start updating health check endpoint.
	healthCheck.StartMonitoring()

	// Start components running in background.
	if err := autoscaler.Start(); err != nil {
		klog.Fatalf("Failed to autoscaler background components: %v", err)
	}

	// Autoscale ad infinitum.
	err = mgr.Add(manager.RunnableFunc(func(ctx ctx.Context) error {
		loopStart := time.Now()
		lastRun := time.Now()
		for {
			select {
			case <-ctx.Done():
				klog.V(1).Info("Received signal, attempting cleanup")

				autoscaler.ExitCleanUp()
				return nil
			default:
				trigger.Wait(lastRun)
				lastRun = time.Now()
				loop.RunAutoscalerOnce(ctx, autoscaler, healthCheck, lastRun)
				// Let Cluster Autoscaler run at least 5 minutes before restarting to pick up a new configuration
				if time.Now().After(loopStart.Add(5*time.Minute)) && optsTracker.OptionChangesRequireRestart() {
					// TODO(b/409515258): We could just return here, but the cleanup takes ~15 min, exiting with an error is faster.
					klog.Fatalf("Cluster Autoscaler configuration changed, restarting to pick up a new configuration.")
				}
			}
		}
	}))
	if err != nil {
		klog.Fatalf("Failed to add runnable to manager: %v", err)
	}

	if err := mgr.Start(context); err != nil {
		klog.Fatalf("Manager exited with error: %v", err)
	}
	klog.V(1).Info("Cleaned up, exiting...")
	klog.Flush()
}

func updateMachineFamiliesOnFlag(options internalopts.AutoscalingOptions) {
	klog.Infof("Maximum compact placement nodes for machine families from flag: %v", options.MaxCompactPlacementNodes)
	if len(options.MaxCompactPlacementNodes) > 0 {
		if err := machinetypes.ApplyMaxCompactPlacementNodesUpdates(options.MaxCompactPlacementNodes); err != nil {
			klog.Info(err)
		}
	}
}

func defaultLeaderElectionConfiguration() componentbaseconfig.LeaderElectionConfiguration {
	return componentbaseconfig.LeaderElectionConfiguration{
		LeaderElect:   false,
		LeaseDuration: metav1.Duration{Duration: defaultLeaseDuration},
		RenewDeadline: metav1.Duration{Duration: defaultRenewDeadline},
		RetryPeriod:   metav1.Duration{Duration: defaultRetryPeriod},
		ResourceLock:  resourcelock.LeasesResourceLock,
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	klog.InitFlags(nil)
	// Contextual logging is hard-coded to be enabled
	// This overwrites the default value
	klog.EnableContextualLogging(false)
	ctrl.SetLogger(klog.NewKlogr())

	leaderElection := defaultLeaderElectionConfiguration()
	leaderElection.LeaderElect = true
	componentopts.BindLeaderElectionFlags(&leaderElection, pflag.CommandLine)

	kube_flag.InitFlags()

	optionsFromFlags := internalopts.AutoscalingOptions{
		AutoscalingOptions: cli.OssOptionsFromFlags(),
		InternalOptions:    cli.InternalOptsFromFlags(),
	}
	experimentsManager := optstracking.InitExperimentsManager(optionsFromFlags)
	optsTracker := optstracking.NewOptionsTracker(optionsFromFlags, experimentsManager)
	options := optsTracker.Options()

	healthCheck := metrics.NewHealthCheck(options.MaxInactivityTime, options.MaxFailingTime, options.MaxStartupTime)

	klog.V(1).Infof("Cluster Autoscaler image tag: %s", ClusterAutoscalerVersion)

	correctEstimator := false
	for _, availableEstimator := range estimator.AvailableEstimators {
		if options.EstimatorName == availableEstimator {
			correctEstimator = true
		}
	}
	if !correctEstimator {
		klog.Fatalf("Unrecognized estimator: %v", options.EstimatorName)
	}

	gkeDebuggingSnapshotter, err := gkedebuggingsnapshot.NewGkeDebuggingSnapshotter(options.DebuggingSnapshotEnabled)
	if err != nil {
		klog.Fatalf("Unable to create GkeDebuggingSnapshotter: %v", err)
	}

	updateMachineFamiliesOnFlag(options)

	go func() {
		pathRecorderMux := mux.NewPathRecorderMux(("cluster-autoscaler"))
		defaultMetricsHandler := legacyregistry.Handler().ServeHTTP
		pathRecorderMux.HandleFunc("/metrics", func(w http.ResponseWriter, req *http.Request) {
			defaultMetricsHandler(w, req)
		})
		if options.MultitenancyEnabled {
			multitenancyMetricsHandler := multitenancy.MetricsRegistryHandler().ServeHTTP
			pathRecorderMux.HandleFunc("/metrics/multitenancy", func(w http.ResponseWriter, req *http.Request) {
				multitenancyMetricsHandler(w, req)
			})
		}
		if options.MetricsPerCccEnabled {
			cccMetricsHandler := ccc.MetricsRegistryHandler().ServeHTTP
			pathRecorderMux.HandleFunc("/metrics/ccc", func(w http.ResponseWriter, req *http.Request) {
				cccMetricsHandler(w, req)
			})
		}
		if options.DebuggingSnapshotEnabled {
			pathRecorderMux.HandleFunc("/snapshotz", gkeDebuggingSnapshotter.ResponseHandler)
		}
		pathRecorderMux.HandleFunc("/health-check", healthCheck.ServeHTTP)
		if options.EnableProfiling {
			routes.Profiling{}.Install(pathRecorderMux)
		}
		err := http.ListenAndServe(options.Address, pathRecorderMux)
		klog.Fatalf("Failed to start metrics: %v", err)
	}()

	if !leaderElection.LeaderElect {
		run(healthCheck, optsTracker, gkeDebuggingSnapshotter)
	} else {
		id, err := os.Hostname()
		if err != nil {
			klog.Fatalf("Unable to get hostname: %v", err)
		}

		kubeconfig := kube_util.GetKubeConfig(options.KubeClientOpts)
		kubeClient := kube_client.NewForConfigOrDie(kubeconfig)

		// Validate that the client is ok.
		_, err = kubeClient.CoreV1().Nodes().List(ctx.TODO(), metav1.ListOptions{})
		if err != nil {
			klog.Fatalf("Failed to get nodes from apiserver: %v", err)
		}

		lock, err := resourcelock.NewFromKubeconfig(
			leaderElection.ResourceLock,
			options.ConfigNamespace,
			"cluster-autoscaler",
			resourcelock.ResourceLockConfig{
				Identity:      id,
				EventRecorder: kube_util.CreateEventRecorder(ctx.TODO(), kubeClient, options.RecordDuplicatedEvents),
			},
			kubeconfig,
			leaderElection.RenewDeadline.Duration,
		)
		if err != nil {
			klog.Fatalf("Unable to create leader election lock: %v", err)
		}

		leaderelection.RunOrDie(ctx.TODO(), leaderelection.LeaderElectionConfig{
			Lock:          lock,
			LeaseDuration: leaderElection.LeaseDuration.Duration,
			RenewDeadline: leaderElection.RenewDeadline.Duration,
			RetryPeriod:   leaderElection.RetryPeriod.Duration,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(_ ctx.Context) {
					// Since we are committing a suicide after losing
					// mastership, we can safely ignore the argument.
					run(healthCheck, optsTracker, gkeDebuggingSnapshotter)
				},
				OnStoppedLeading: func() {
					klog.Fatalf("lost master")
				},
			},
		})
	}
}
