# GKE Autoscaler Benchmarks

This directory contains benchmarks for the GKE Autoscaler.

## Running Benchmarks

To run the benchmark:

```bash
go test -v -bench=. -run=^$ -benchmem -count=1 -benchtime=1x ./pkg/test/integration/bench > results.txt
```

### Benchmark Flags

- `-benchmem`: Includes memory allocation statistics in the benchmark output. It provides:
  - `B/op`: The average number of bytes allocated per benchmark operation.
  - `allocs/op`: The average number of distinct memory allocations (heap allocations) performed per operation. High `allocs/op` typically indicate potential garbage collection pressure.
- `-count=N`: Runs the benchmark `N` times. Each run yields a separate data point for `benchstat`. `benchstat` needs at least 6 samples to compute reliable statistical confidence.
- `-benchtime=Nx`: Runs exactly `N` iterations per sample. For heavy benchmarks, specifying a fixed count (like `10x` or `1x`) prevents it from running for too long.
- `-v`: Enables verbose output. Prints the name of each individual test or benchmark as it runs, and ensures any output generated via `t.Log()`, `b.Log()`, etc., is immediately streamed to standard output instead of being suppressed.

## Comparing Results

Install `benchstat`:

```bash
GOBIN=$PWD/pkg/test/integration/bench/bin go install golang.org/x/perf/cmd/benchstat@latest
```

Compare results:

```bash
$PWD/pkg/test/integration/bench/bin/benchstat before.txt after.txt
```

## Profiling

The benchmark supports generating CPU profiles. To generate a profile:

```bash
go test -v -bench=. -run=^$ -benchmem -benchtime=1x ./pkg/test/integration/bench -profile-cpu=cpu.out
```

To view the profile, upload it to http://pprof UI.

## Writing a New Benchmark

To add a new benchmark, define a new `Benchmark...` function inside a `_test.go` file within this directory.

### TL;DR, show me the code

Example:

```go
func BenchmarkMyAdvancedScaleUp(b *testing.B) {
	s := scenario{
		given: func() *integration.TestConfig {
			tc := defaultBenchmarkConfig()
			tc.WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled())
			tc.WithOverrides(
				integration.WithMaxNodesPerScaleUp(maxNGSize),
			)
			return tc
		},
		when: func(infra *integration.TestInfrastructure) {
			for i := 0; i < 500; i++ {
				podName := fmt.Sprintf("pod-%d", i)
				pod := tu.BuildTestPod(podName, 600, 1000, tu.MarkUnschedulable())
				infra.Fakes.K8s.AddPod(pod)
			}
		},
		then: verifyTargetNumberOfNodes(10),
	}
	s.run(b)
}
```

### API Reference

The shared `scenario` runner (defined in ./utils.go) creates ClusterAutoscaler, intiializes fakes and executes RunOnce() once.

Define a `scenario` struct, with `given`, `when` and `then` functions, and then invoke `s.run(b)`.

- `given` function defines the cluster (node pools, their sizes, cluster-wide limits). This is a test equivalent of `container clusters create` command.
  - Example: call `tc.WithNodePools( integration.DefaultNodePool().)` to define a cluster with a node pool.
- `when` function defines the k8s resources on this cluster, like unschedulable pods. This is a test equivalent of `kubectl apply`.
  - Example: call `infra.Fakes.K8s.AddPod` to add pods.
- `then` function: Assert the test invariants by inspecting final state (e.g target MIGs and their sizes via `infra.Fakes.GceService`).
