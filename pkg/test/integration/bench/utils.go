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

package bench

import (
	"context"
	"flag"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"testing"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
)

var (
	profileCPU = flag.String("profile-cpu", "", "If set, the benchmark writes a CPU profile to this file, covering the RunOnce execution during the first iteration.")
	disableGC  = flag.Bool("disable-gc", false, "Disable garbage collection to stabilize the runtime.")
)

// TestMain configures global benchmark and logging flags.
func TestMain(m *testing.M) {
	flag.Parse()
	flag.Set("v", "4")
	os.Exit(m.Run())
}

type scenario struct {
	given func() *integration.TestConfig
	when  func(infra *integration.TestInfrastructure)
	then  func(tb testing.TB, infra *integration.TestInfrastructure)
}

func defaultBenchmarkConfig() *integration.TestConfig {
	maxNGSize := 1000
	mcp := machinetypes.NewMachineConfigProvider(nil)
	e2, err := mcp.ToMachineType("e2-standard-32")
	if err != nil {
		panic(err)
	}
	maxCores := e2.CPU * int64(maxNGSize)
	maxMemory := e2.Memory * int64(maxNGSize)

	return integration.NewTestConfig().
		WithNodePools(integration.DefaultNodePool(
			integration.WithNodePoolMachineType("e2-standard-32"),
			integration.WithNodePoolSize(1),
		)).
		WithClusterWideLimits(maxNGSize, maxCores, maxMemory)
}

func (s scenario) run(b *testing.B) {
	// Pause the benchmark timer to prevent heavy setup and initialization overhead
	// from inflating the final benchmarking time.
	b.StopTimer()

	if *disableGC {
		oldGC := debug.SetGCPercent(-1)
		defer debug.SetGCPercent(oldGC)
	}

	var f *os.File
	if *profileCPU != "" {
		var err error
		f, err = os.Create(*profileCPU)
		if err != nil {
			b.Fatalf("Failed to create cpu profile file: %v", err)
		}
		defer f.Close()
	}

	for i := 0; i < b.N; i++ {
		// Create a cancellable context.
		// Down the call stack in the new newFakeClientForTB helper,
		// we start informers using prFactory.Start(ctx.Done()).
		// The Kubernetes Scheduler Framework started by NewHandle also stores this ctx.
		// The context must be cancellable so that those don't continue running forever,
		// otherwise they stack up in each iteration and distort benchmarking metrics.
		ctx, cancel := context.WithCancel(context.Background())
		infra := integration.SetupInfrastructure(ctx, b)
		stopCh := make(chan struct{})

		var testConfig *integration.TestConfig
		if s.given != nil {
			testConfig = s.given()
		} else {
			b.Fatalf("Test cluster config is not defined. Add WithCluster func to your scenario")
		}

		if s.when != nil {
			s.when(infra)
		}

		autoscaler, err := integration.SetupAutoscaler(b, ctx, testConfig, infra, stopCh)
		if err != nil {
			b.Fatalf("SetupAutoscaler failed: %v", err)
		}

		infra.Fakes.InformerFactory.Start(stopCh)
		infra.Fakes.InformerFactory.WaitForCacheSync(stopCh)

		runtime.GC()

		if f != nil && i == 0 {
			if err := pprof.StartCPUProfile(f); err != nil {
				b.Fatalf("Failed to start cpu profile: %v", err)
			}
		}

		b.StartTimer()
		err = autoscaler.RunOnce(time.Now().Add(10 * time.Second))
		b.StopTimer()

		if f != nil && i == 0 {
			pprof.StopCPUProfile()
		}

		// We need to call cancel() and close(stopCh) to prevent the
		// informers and the scheduler framework from running in the background.
		close(stopCh)
		cancel()

		if err != nil {
			b.Fatalf("RunOnce failed: %v", err)
		}

		if s.then != nil {
			s.then(b, infra)
		}
	}
}

func verifyTargetNumberOfNodes(expectedTargetSize int) func(tb testing.TB, infra *integration.TestInfrastructure) {
	return func(tb testing.TB, infra *integration.TestInfrastructure) {
		tb.Helper()
		migs, err := infra.Fakes.GceService.FetchAllMigs("us-central1-b")
		if err != nil {
			tb.Fatalf("Failed to fetch MIGs: %v", err)
		}
		totalTargetSize := 0
		for _, mig := range migs {
			totalTargetSize += int(mig.TargetSize)
		}
		if totalTargetSize != expectedTargetSize {
			tb.Fatalf("expected total target size %d, got %d", expectedTargetSize, totalTargetSize)
		}
	}
}
