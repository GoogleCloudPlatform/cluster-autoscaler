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

package lockwatchdog

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/klog/v2"
)

const sampleDump = `goroutine 1 [running]:
main.main()
	/src/main.go:22 +0x1b4

goroutine 18 [sync.Mutex.Lock, 5 minutes]:
internal/sync.runtime_SemacquireMutex(0x0?, 0x0?, 0x0?)
	/go/src/internal/sync/runtime.go:31 +0x1a
internal/sync.(*Mutex).lockSlow(0x1d42939a008)
	/go/src/internal/sync/mutex.go:149 +0x15d
internal/sync.(*Mutex).Lock(...)
	/go/src/internal/sync/mutex.go:70
sync.(*Mutex).Lock(...)
	/go/src/sync/mutex.go:46
k8s.io/autoscaler/cluster-autoscaler/clusterstate.(*ClusterStateRegistry).RegisterScaleDown(0xc0001a2000, {0x1d4293aa000, 0x2})
	/src/clusterstate/clusterstate.go:312 +0x45
k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange.(*NodeGroupChangeObserversList).RegisterScaleDown(0xc000190000, {0x1d4293aa000, 0x2})
	/src/observers/nodegroupchange/scale_state_observer.go:88 +0xe4
created by k8s.io/autoscaler/cluster-autoscaler/core/scaledown/actuation.(*Actuator).deleteNodesAsync in goroutine 1
	/src/core/scaledown/actuation/actuator.go:201 +0x3b1

goroutine 19 [sync.RWMutex.RLock]:
sync.runtime_SemacquireRWMutexR(0x989680?, 0x0?, 0x0?)
	/go/src/runtime/sema.go:100 +0x25
sync.(*RWMutex).RLock(...)
	/go/src/sync/rwmutex.go:72
k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange.(*NodeGroupChangeObserversList).RegisterFailedScaleUp(0xc000190000, ...)
	/src/observers/nodegroupchange/scale_state_observer.go:70 +0x5e
k8s.io/autoscaler/cluster-autoscaler/clusterstate.(*ClusterStateRegistry).UpdateNodes(0xc0001a2000, ...)
	/src/clusterstate/clusterstate.go:412 +0x1c8
created by main.main in goroutine 1
	/src/main.go:30 +0x1f2

goroutine 20 [select]:
k8s.io/client-go/tools/cache.(*Reflector).watch(0xc000200000, ...)
	/src/vendor/k8s.io/client-go/tools/cache/reflector.go:400 +0x2a0
created by k8s.io/client-go/tools/cache.(*Reflector).Run in goroutine 7
	/src/vendor/k8s.io/client-go/tools/cache/reflector.go:300 +0x1e5

goroutine 21 [select]:
k8s.io/client-go/tools/cache.(*Reflector).watch(0xc000200100, ...)
	/src/vendor/k8s.io/client-go/tools/cache/reflector.go:400 +0x2a0
created by k8s.io/client-go/tools/cache.(*Reflector).Run in goroutine 7
	/src/vendor/k8s.io/client-go/tools/cache/reflector.go:300 +0x1e5
`

func TestParseDump(t *testing.T) {
	gs := parseDump([]byte(sampleDump))
	require.Len(t, gs, 5)

	assert.Equal(t, 1, gs[0].id)
	assert.Equal(t, "running", gs[0].state)
	assert.False(t, gs[0].isLockWait())
	assert.Equal(t, "", gs[0].createdBy)

	g := gs[1]
	assert.Equal(t, 18, g.id)
	assert.Equal(t, "sync.Mutex.Lock", g.state)
	assert.Equal(t, 5*time.Minute, g.runtimeWait)
	assert.Equal(t, "0x1d42939a008", g.lockAddr)
	assert.True(t, g.isLockWait())
	assert.Equal(t, "k8s.io/autoscaler/cluster-autoscaler/core/scaledown/actuation.(*Actuator).deleteNodesAsync", g.createdBy)
	require.Len(t, g.frames, 6)
	assert.Equal(t, frame{function: "internal/sync.runtime_SemacquireMutex", location: "/go/src/internal/sync/runtime.go:31"}, g.frames[0])
	// Inlined frames have no "+0x" offset.
	assert.Equal(t, frame{function: "sync.(*Mutex).Lock", location: "/go/src/sync/mutex.go:46"}, g.frames[3])
	assert.Equal(t, "k8s.io/autoscaler/cluster-autoscaler/clusterstate.(*ClusterStateRegistry).RegisterScaleDown", g.topFunction())

	g = gs[2]
	assert.Equal(t, "sync.RWMutex.RLock", g.state)
	assert.Equal(t, time.Duration(0), g.runtimeWait)
	assert.Equal(t, "", g.lockAddr)
	assert.True(t, g.isLockWait())
	assert.Equal(t, "k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange.(*NodeGroupChangeObserversList).RegisterFailedScaleUp", g.topFunction())
	assert.Equal(t, "main.main", g.createdBy)

	assert.Equal(t, "select", gs[3].state)
	assert.False(t, gs[3].isLockWait())
}

func TestParseDumpSystemTracebackAndRWMutexAddr(t *testing.T) {
	// GOTRACEBACK=system adds gp/m fields, concurrent GC adds "(scan)",
	// and -race keeps the RWMutex receiver live ("0xc0001a8000") while
	// CGO_ENABLED=0 marks it dead ("0x0?").
	raw := `goroutine 7 gp=0xc000002380 m=0 mp=0x5f1b20 [sync.RWMutex.Lock (scan), 1 minutes]:
sync.runtime_SemacquireRWMutex(0x0?, 0x0?, 0x0?)
	/go/src/runtime/sema.go:105 +0x25
sync.(*RWMutex).Lock(0xc0001a8000)
	/go/src/sync/rwmutex.go:155 +0x7b
main.f()
	/src/main.go:1 +0x1

goroutine 8 [sync.RWMutex.Lock]:
sync.runtime_SemacquireRWMutex(0x0?, 0x0?, 0x0?)
	/go/src/runtime/sema.go:105 +0x25
sync.(*RWMutex).Lock(0x0?)
	/go/src/sync/rwmutex.go:155 +0x6b
main.f()
	/src/main.go:1 +0x1
`
	gs := parseDump([]byte(raw))
	require.Len(t, gs, 2)
	assert.Equal(t, 7, gs[0].id)
	assert.Equal(t, "sync.RWMutex.Lock", gs[0].state)
	assert.True(t, gs[0].isLockWait())
	assert.Equal(t, time.Minute, gs[0].runtimeWait)
	assert.Equal(t, "0xc0001a8000", gs[0].lockAddr)
	assert.Equal(t, "", gs[1].lockAddr)
}

func TestParseDumpSkipsMalformedBlocks(t *testing.T) {
	raw := "not a goroutine header\n\n" +
		"goroutine abc [running]:\nmain.f()\n\t/a.go:1\n\n" +
		"goroutine 18 [sync.Mutex.Lock]:\nmain.f()\n\t/a.go:1\ncreated by "
	gs := parseDump([]byte(raw))
	require.Len(t, gs, 1)
	assert.Equal(t, 18, gs[0].id)
	assert.Equal(t, "sync.Mutex.Lock", gs[0].state)
	assert.Equal(t, "", gs[0].createdBy)
}

// newTestWatchdog returns an enabled watchdog with production thresholds that
// reads the given dumps in order (repeating the last one).
func newTestWatchdog(dumps ...string) *Watchdog {
	w := New(true)
	i := 0
	w.stacks = func() []byte {
		d := dumps[min(i, len(dumps)-1)]
		i++
		return []byte(d)
	}
	return w
}

func stalledFunctions(w *Watchdog) ([]string, bool) {
	_, stalled, restart := w.check()
	var functions []string
	for _, g := range stalled {
		functions = append(functions, g.topFunction())
	}
	return functions, restart
}

func TestCheckReportsAfterLogThresholdAndRestartsAtRestartThreshold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dumpNoWait := strings.ReplaceAll(sampleDump, ", 5 minutes", "")
		dump2mWait := strings.ReplaceAll(sampleDump, ", 5 minutes", ", 2 minutes")
		dump3mWait := strings.ReplaceAll(sampleDump, ", 5 minutes", ", 3 minutes")
		w := newTestWatchdog(dumpNoWait, dumpNoWait, dumpNoWait, dump2mWait, dump3mWait)

		funcs, restart := stalledFunctions(w)
		assert.Empty(t, funcs, "first sighting must not trigger")
		assert.False(t, restart)

		time.Sleep(30 * time.Second)
		funcs, restart = stalledFunctions(w)
		assert.Empty(t, funcs, "30s is below the 1m log threshold")
		assert.False(t, restart)

		time.Sleep(30 * time.Second)
		funcs, restart = stalledFunctions(w)
		assert.Equal(t, []string{
			"k8s.io/autoscaler/cluster-autoscaler/clusterstate.(*ClusterStateRegistry).RegisterScaleDown",
			"k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange.(*NodeGroupChangeObserversList).RegisterFailedScaleUp",
		}, funcs, "blocked for 1m triggers log tier")
		assert.False(t, restart, "1m must not restart")

		time.Sleep(4 * time.Minute)
		funcs, restart = stalledFunctions(w)
		assert.False(t, restart, "2m runtimeWait is within forcegcperiod backdating window (< 3m)")

		time.Sleep(15 * time.Second)
		funcs, restart = stalledFunctions(w)
		assert.True(t, restart, "blocked >= 5m with runtimeWait >= 3m triggers restart")
		// Goroutine 18 qualifies for restart (runtimeWait=3m), so it
		// sorts ahead of goroutine 19 (runtimeWait=0).
		assert.Equal(t, "k8s.io/autoscaler/cluster-autoscaler/clusterstate.(*ClusterStateRegistry).RegisterScaleDown", funcs[0])
	})
}

func TestCheckResetsWhenRuntimeWaitDecreases(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// A short wait right before a GC can read runtimeWait=2m (from
		// the previous gcMark's work.tstart). When the goroutine
		// unparks and re-blocks at the same call site, runtimeWait
		// drops back to 0 and checkAt must reset its stall clock.
		dump2m := strings.ReplaceAll(sampleDump, ", 5 minutes", ", 2 minutes")
		dump0m := strings.ReplaceAll(sampleDump, ", 5 minutes", "")
		w := newTestWatchdog(dump2m, dump0m, dump0m)

		funcs, restart := stalledFunctions(w)
		assert.Empty(t, funcs, "first sighting must not trigger")
		assert.False(t, restart)

		time.Sleep(30 * time.Second)
		funcs, restart = stalledFunctions(w)
		assert.Empty(t, funcs, "drop in runtimeWait proves unpark and resets since")
		assert.False(t, restart)

		time.Sleep(30 * time.Second)
		funcs, restart = stalledFunctions(w)
		// Goroutine 19 stayed at runtimeWait=0 from t=0 (now 60s ->
		// logged), whereas goroutine 18 reset at t=30s (only 30s ->
		// not logged).
		assert.Equal(t, []string{"k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange.(*NodeGroupChangeObserversList).RegisterFailedScaleUp"}, funcs)
		assert.False(t, restart)
	})
}

func TestCheckUsesObservedDurationEvenWhenRuntimeWaitOverstates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Even if runtimeWait reports 5m (e.g. 3m of real wait + up to
		// 2m of forcegcperiod backdating), checkAt must not trigger a
		// restart until the watchdog itself has observed the goroutine
		// stalled for 5m.
		w := newTestWatchdog(sampleDump)
		funcs, restart := stalledFunctions(w)
		assert.Empty(t, funcs)
		assert.False(t, restart)

		time.Sleep(3 * time.Minute)
		funcs, restart = stalledFunctions(w)
		assert.NotEmpty(t, funcs, "3m observed wait triggers log tier")
		assert.False(t, restart, "3m observed wait must not trigger 5m restart despite 5m runtimeWait")

		time.Sleep(2 * time.Minute)
		funcs, restart = stalledFunctions(w)
		assert.Equal(t, []string{
			"k8s.io/autoscaler/cluster-autoscaler/clusterstate.(*ClusterStateRegistry).RegisterScaleDown",
			"k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange.(*NodeGroupChangeObserversList).RegisterFailedScaleUp",
		}, funcs)
		assert.True(t, restart)
	})
}

func TestCheckSortsRestartEligibleAndLongestWaitFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Place goroutine 19 (no runtimeWait) first in the dump and
		// goroutine 18 (3m runtimeWait) second, and verify checkAt
		// still sorts the restart-eligible goroutine 18 first at 5m.
		reordered := `goroutine 19 [sync.RWMutex.RLock]:
sync.runtime_SemacquireRWMutexR(0x989680?, 0x0?, 0x0?)
	/go/src/runtime/sema.go:100 +0x25
sync.(*RWMutex).RLock(...)
	/go/src/sync/rwmutex.go:72
k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange.(*NodeGroupChangeObserversList).RegisterFailedScaleUp(0xc000190000, ...)
	/src/observers/nodegroupchange/scale_state_observer.go:70 +0x5e

goroutine 18 [sync.Mutex.Lock, 3 minutes]:
internal/sync.runtime_SemacquireMutex(0x0?, 0x0?, 0x0?)
	/go/src/internal/sync/runtime.go:31 +0x1a
internal/sync.(*Mutex).lockSlow(0x1d42939a008)
	/go/src/internal/sync/mutex.go:149 +0x15d
k8s.io/autoscaler/cluster-autoscaler/clusterstate.(*ClusterStateRegistry).RegisterScaleDown(0xc0001a2000, {0x1d4293aa000, 0x2})
	/src/clusterstate/clusterstate.go:312 +0x45
`
		onlyG18 := `goroutine 18 [sync.Mutex.Lock]:
internal/sync.runtime_SemacquireMutex(0x0?, 0x0?, 0x0?)
	/go/src/internal/sync/runtime.go:31 +0x1a
internal/sync.(*Mutex).lockSlow(0x1d42939a008)
	/go/src/internal/sync/mutex.go:149 +0x15d
k8s.io/autoscaler/cluster-autoscaler/clusterstate.(*ClusterStateRegistry).RegisterScaleDown(0xc0001a2000, {0x1d4293aa000, 0x2})
	/src/clusterstate/clusterstate.go:312 +0x45
`
		bothNoWait := strings.ReplaceAll(reordered, ", 3 minutes", "")
		w := newTestWatchdog(onlyG18, bothNoWait, reordered)

		// t=0: only goroutine 18 is blocked.
		stalledFunctions(w)
		// t=30s: goroutine 19 also blocks (appears first in the dump).
		time.Sleep(30 * time.Second)
		stalledFunctions(w)
		// t=1m30s: goroutine 18 has waited 90s vs goroutine 19's 60s,
		// so 18 sorts first.
		time.Sleep(time.Minute)
		funcs, restart := stalledFunctions(w)
		assert.False(t, restart)
		assert.Equal(t, []string{
			"k8s.io/autoscaler/cluster-autoscaler/clusterstate.(*ClusterStateRegistry).RegisterScaleDown",
			"k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange.(*NodeGroupChangeObserversList).RegisterFailedScaleUp",
		}, funcs)

		// t=5m30s: both have waited >= 5m, and goroutine 18 (second in
		// dump) is restart-eligible (runtimeWait=3m) so it still sorts
		// first.
		time.Sleep(4 * time.Minute)
		funcs, restart = stalledFunctions(w)
		assert.True(t, restart)
		assert.Equal(t, []string{
			"k8s.io/autoscaler/cluster-autoscaler/clusterstate.(*ClusterStateRegistry).RegisterScaleDown",
			"k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange.(*NodeGroupChangeObserversList).RegisterFailedScaleUp",
		}, funcs)
	})
}

func TestCheckResetsWhenGoroutineMakesProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		blocked := strings.ReplaceAll(sampleDump, ", 5 minutes", "")
		// Same goroutine id, now blocked at another line or mutex: it
		// made progress.
		movedLine := strings.Replace(blocked, "clusterstate.go:312", "clusterstate.go:999", 1)
		movedMutex := strings.Replace(movedLine, "0x1d42939a008", "0x1d42939b999", 1)
		w := newTestWatchdog(blocked, movedLine, movedMutex)

		funcs, _ := stalledFunctions(w)
		assert.Empty(t, funcs)
		time.Sleep(45 * time.Second)
		funcs, _ = stalledFunctions(w)
		assert.Empty(t, funcs)
		time.Sleep(45 * time.Second)
		// Goroutine 19 stayed at an unchanged stack for 90s and
		// triggers the log tier; goroutine 18 changed line at 45s and
		// mutex address at 90s, restarting its clock each time.
		funcs, restart := stalledFunctions(w)
		assert.Equal(t, []string{"k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange.(*NodeGroupChangeObserversList).RegisterFailedScaleUp"}, funcs)
		assert.False(t, restart)
	})
}

func TestCheckIgnoresNonLockWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dump := "goroutine 5 [select, 10 minutes]:\nmain.f()\n\t/src/main.go:1 +0x1\n\ngoroutine 6 [sync.Cond.Wait, 10 minutes]:\nmain.g()\n\t/src/main.go:2 +0x1\n"
		w := newTestWatchdog(dump)
		_, stalled, restart := w.check()
		assert.Empty(t, stalled)
		assert.False(t, restart)
		time.Sleep(time.Hour)
		_, stalled, restart = w.check()
		assert.Empty(t, stalled)
		assert.False(t, restart)
	})
}

func TestStepLogsEveryMinuteAndRestartsAtFiveMinutes(t *testing.T) {
	klogState := klog.CaptureState()
	t.Cleanup(klogState.Restore)
	var logBuf bytes.Buffer
	klog.LogToStderr(false)
	klog.SetOutput(&logBuf)

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		// Simulate sysmon stamping gp.waitsince at t=1m.
		gcStampedAt := start.Add(time.Minute)

		w := New(true)
		w.stacks = func() []byte {
			var waitHeader string
			now := time.Now()
			if now.After(gcStampedAt) {
				if mins := int(now.Sub(gcStampedAt) / time.Minute); mins >= 1 {
					waitHeader = fmt.Sprintf(", %d minutes", mins)
				}
			}
			return []byte(strings.ReplaceAll(sampleDump, ", 5 minutes", waitHeader))
		}
		exitCode := -1
		w.exit = func(code int) { exitCode = code }

		// Tick at t=0s and step by 15s up to 5m (ticks 0..20).
		var loggedAt []time.Duration
		for tick := 0; tick <= 20; tick++ {
			logBuf.Reset()
			exited := w.step()
			klog.Flush()
			out := logBuf.String()
			elapsed := time.Since(start)

			if tick < 20 {
				require.False(t, exited, "must not exit before 5m (elapsed=%v)", elapsed)
				if strings.Contains(out, "LOCK_WAIT_STALL") {
					assert.Contains(t, out, "action=log")
					assert.NotContains(t, out, "action=restart")
					loggedAt = append(loggedAt, elapsed)
				}
				time.Sleep(15 * time.Second)
				continue
			}

			require.True(t, exited, "must exit at 5m")
			assert.Equal(t, ExitCode, exitCode)
			assert.Contains(t, out, "action=restart")
			assert.Contains(t, out, "exiting with code 3")
		}

		assert.Equal(t, []time.Duration{
			1 * time.Minute,
			2 * time.Minute,
			3 * time.Minute,
			4 * time.Minute,
		}, loggedAt, "must log once per minute before 5m restart")
	})
}

func TestStepStartupLogOnlyModeUntilRestartEnabled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newTestWatchdog(sampleDump)
		w.SetRestartEnabled(false)
		exitCode := -1
		w.exit = func(code int) { exitCode = code }

		// Even with a 5m stall, log-only startup mode does not exit.
		for range 25 {
			require.False(t, w.step())
			time.Sleep(15 * time.Second)
		}
		assert.Equal(t, -1, exitCode)

		// Once the first successful run finishes and enables restarts,
		// the next tick exits.
		w.SetRestartEnabled(true)
		require.True(t, w.step())
		assert.Equal(t, ExitCode, exitCode)
	})
}

func TestStepResetsLastLogAfterRecovery(t *testing.T) {
	klogState := klog.CaptureState()
	t.Cleanup(klogState.Restore)
	var logBuf bytes.Buffer
	klog.LogToStderr(false)
	klog.SetOutput(&logBuf)

	synctest.Test(t, func(t *testing.T) {
		dumpStalled := strings.ReplaceAll(sampleDump, ", 5 minutes", "")
		dumpHealthy := "goroutine 1 [running]:\nmain.main()\n\t/src/main.go:22 +0x1b4\n"

		w := newTestWatchdog(dumpStalled, dumpStalled, dumpHealthy, dumpStalled, dumpStalled)

		// First stall episode: sighted at t=0, logged at t=1m.
		require.False(t, w.step())
		time.Sleep(time.Minute)
		logBuf.Reset()
		require.False(t, w.step())
		klog.Flush()
		assert.Contains(t, logBuf.String(), "LOCK_WAIT_STALL")

		// Recovery at t=1m15s clears w.lastLog; new stall sighted at
		// t=1m30s logs immediately once it reaches 1m (at t=2m30s).
		time.Sleep(15 * time.Second)
		require.False(t, w.step())
		assert.True(t, w.lastLog.IsZero())

		time.Sleep(15 * time.Second)
		require.False(t, w.step())
		time.Sleep(time.Minute)
		logBuf.Reset()
		require.False(t, w.step())
		klog.Flush()
		assert.Contains(t, logBuf.String(), "LOCK_WAIT_STALL")
	})
}

func TestStepDoesNotRestartWithoutRuntimeWaitConfirmation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Simulates a goroutine caught at the same call site on every
		// 15s tick for 10 minutes while making progress between ticks
		// (so gp.waitsince resets on every unpark and runtimeWait stays
		// below 3m).
		dumpNoWait := strings.ReplaceAll(sampleDump, ", 5 minutes", "")
		w := newTestWatchdog(dumpNoWait)
		exitCode := -1
		w.exit = func(code int) { exitCode = code }

		for range 41 { // 0s..10m in 15s steps
			require.False(t, w.step())
			time.Sleep(15 * time.Second)
		}
		assert.Equal(t, -1, exitCode, "watchdog must not exit without runtime wait confirmation")
	})
}

func TestRunExitsOnStall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newTestWatchdog(sampleDump)
		stop := make(chan struct{})
		defer close(stop)
		exited := make(chan int, 1)
		w.exit = func(code int) {
			exited <- code
		}
		go w.Run(stop)

		// First tick at t=15s records entry.since=15s; 5m of observed
		// stall reaches restartThreshold at t=5m15s.
		time.Sleep(5 * time.Minute)
		synctest.Wait()
		assert.Empty(t, exited, "must not exit before 5m of observed wait")

		time.Sleep(15 * time.Second)
		synctest.Wait()
		require.Len(t, exited, 1)
		assert.Equal(t, ExitCode, <-exited)
	})
}

func TestRunDisabledAtStartupDoesNotCheck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := New(false)
		var checks atomic.Int32
		w.stacks = func() []byte {
			checks.Add(1)
			return []byte(sampleDump)
		}
		exited := make(chan int, 1)
		w.exit = func(code int) {
			exited <- code
		}
		stop := make(chan struct{})
		go w.Run(stop)

		time.Sleep(10 * time.Minute)
		synctest.Wait()
		close(stop)
		synctest.Wait()
		assert.Zero(t, checks.Load())
		assert.Empty(t, exited)
	})
}

func TestRunSetEnabledAtRuntime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// 3m runtime wait satisfies minRuntimeWait (3m) while staying
		// below restartThreshold (5m), so the watchdog exits only once
		// 5m of observed time elapses while continuously enabled.
		dump := strings.ReplaceAll(sampleDump, ", 5 minutes", ", 3 minutes")
		w := newTestWatchdog(dump)
		exited := make(chan int, 1)
		w.exit = func(code int) {
			exited <- code
		}

		stop := make(chan struct{})
		defer close(stop)
		go w.Run(stop)

		// Populate seen across two 15s ticks while enabled.
		time.Sleep(30 * time.Second)
		synctest.Wait()
		assert.Empty(t, exited)

		// Disable at runtime and advance past 10m: must not exit.
		w.SetEnabled(false)
		synctest.Wait()
		time.Sleep(10 * time.Minute)
		synctest.Wait()
		assert.Empty(t, exited, "disabled watchdog must not exit")

		// Re-enabling starts a fresh 5m window (seen cleared on
		// disable): first tick after re-enable is 15s later, so 5m of
		// observed wait arrives at +5m15s.
		w.SetEnabled(true)
		synctest.Wait()
		time.Sleep(5 * time.Minute)
		synctest.Wait()
		assert.Empty(t, exited, "re-enabled watchdog must start a fresh threshold window")

		time.Sleep(15 * time.Second)
		synctest.Wait()
		require.Len(t, exited, 1)
		assert.Equal(t, ExitCode, <-exited)
	})
}

func TestContendedMutexMakingProgress(t *testing.T) {
	t.Parallel()

	t.Run("same contended mutex with default minRuntimeWait", func(t *testing.T) {
		var mu sync.Mutex
		stopWorkers := make(chan struct{})
		var done sync.WaitGroup
		done.Add(8)
		for range 8 {
			go func() {
				defer done.Done()
				for {
					select {
					case <-stopWorkers:
						return
					default:
					}
					mu.Lock()
					time.Sleep(500 * time.Microsecond)
					mu.Unlock()
				}
			}()
		}
		t.Cleanup(func() {
			close(stopWorkers)
			done.Wait()
		})

		w := New(true)
		w.interval = 10 * time.Millisecond
		w.logThreshold = 20 * time.Millisecond
		w.restartThreshold = 40 * time.Millisecond
		exited := make(chan int, 1)
		w.exit = func(code int) { exited <- code }
		stop := make(chan struct{})
		defer close(stop)
		go w.Run(stop)
		select {
		case code := <-exited:
			t.Fatalf("watchdog exited (%d) despite continuous progress", code)
		case <-time.After(120 * time.Millisecond):
		}
	})

	t.Run("distinct mutexes in loop at same call site even with minRuntimeWait=0", func(t *testing.T) {
		var mus [64]sync.Mutex
		stopWorkers := make(chan struct{})
		var done sync.WaitGroup
		done.Add(8)
		for worker := range 8 {
			go func() {
				defer done.Done()
				for i := worker; ; i++ {
					select {
					case <-stopWorkers:
						return
					default:
					}
					mu := &mus[i%len(mus)]
					mu.Lock()
					time.Sleep(500 * time.Microsecond)
					mu.Unlock()
				}
			}()
		}
		t.Cleanup(func() {
			close(stopWorkers)
			done.Wait()
		})

		w := New(true)
		w.interval = 10 * time.Millisecond
		w.logThreshold = 20 * time.Millisecond
		w.restartThreshold = 60 * time.Millisecond
		w.minRuntimeWait = 0
		exited := make(chan int, 1)
		w.exit = func(code int) { exited <- code }
		stop := make(chan struct{})
		defer close(stop)
		go w.Run(stop)
		select {
		case code := <-exited:
			t.Fatalf("watchdog exited (%d) despite rotating across distinct mutexes", code)
		case <-time.After(120 * time.Millisecond):
		}
	})
}

// TestRealDeadlock runs the watchdog against the real runtime with goroutines
// deadlocked on sync.Mutex and sync.RWMutex (both Lock and RLock), the same
// shape as the deadlock between ClusterStateRegistry and
// NodeGroupChangeObserversList in b/549275083.
func TestRealDeadlock(t *testing.T) {
	startDeadlockedGoroutines(t)
	startDeadlockedRWMutexGoroutines(t)
	w := New(true)
	w.logThreshold = 20 * time.Millisecond
	w.restartThreshold = 20 * time.Millisecond
	// runtime.Stack prints gp.waitsince only in whole minutes, so disable
	// the 3m floor here to exercise sub-second deadlock detection; see
	// TestRealDeadlockRuntimeWaitSubprocess for the 61s runtimeWait check.
	w.minRuntimeWait = 0
	deadline := time.Now().Add(5 * time.Second)
	for {
		all, stalled, restart := w.check()
		if restart && len(stalled) == 4 {
			w.report(time.Now(), all, stalled, true)
			states := map[string]int{}
			for _, g := range stalled {
				states[g.state]++
			}
			assert.Equal(t, map[string]int{
				"sync.Mutex.Lock":    2,
				"sync.RWMutex.Lock":  1,
				"sync.RWMutex.RLock": 1,
			}, states)
			return
		}
		require.True(t, time.Now().Before(deadline), "watchdog did not detect all 4 deadlocked goroutines (got %d)", len(stalled))
		time.Sleep(5 * time.Millisecond)
	}
}

var runtimeWaitSubprocessPassed atomic.Bool

// TestRealDeadlockRuntimeWaitSubprocess compiles a CGO_ENABLED=0 (non-race)
// test binary matching production builds, stamps gp.waitsince via GC at t=0,
// holds a real deadlock alongside contended-mutex workers for 61s, and
// verifies that runtime.Stack reports >= 1m runtimeWait only on the deadlocked
// goroutines (contended workers report 0) and a live 0x... lockAddr on every
// goroutine parked in lockSlow. Deliberately not skipped under -short because
// Kokoro presubmits pass -short; skips repeat -count=N iterations after the
// first pass so postsubmit (-count=10) stays under the 10m package timeout.
func TestRealDeadlockRuntimeWaitSubprocess(t *testing.T) {
	t.Parallel()
	if runtimeWaitSubprocessPassed.Load() {
		t.Skip("already passed in an earlier -count iteration")
	}

	goBin, err := exec.LookPath("go")
	if err != nil && runtime.GOROOT() != "" {
		goBin = filepath.Join(runtime.GOROOT(), "bin", "go")
		err = nil
	}
	require.NoError(t, err, "go toolchain must be on PATH")

	binPath := filepath.Join(t.TempDir(), "lockwatchdog_norace.test")
	buildCmd := exec.CommandContext(t.Context(), goBin, "test", "-c", "-o", binPath, ".")
	buildCmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	buildOut, err := buildCmd.CombinedOutput()
	require.NoErrorf(t, err, "compile CGO_ENABLED=0 test binary: %s", buildOut)

	runCmd := exec.CommandContext(t.Context(), binPath, "-test.run=^TestHelperRealRuntimeWait$", "-test.v")
	runCmd.Env = append(os.Environ(), "LOCKWATCHDOG_HELPER_SUBPROCESS=1")
	runOut, err := runCmd.CombinedOutput()
	require.NoErrorf(t, err, "subprocess TestHelperRealRuntimeWait failed:\n%s", runOut)
	runtimeWaitSubprocessPassed.Store(true)
}

// TestHelperRealRuntimeWait is the subprocess body for
// TestRealDeadlockRuntimeWaitSubprocess and is a no-op unless
// LOCKWATCHDOG_HELPER_SUBPROCESS=1 is set.
func TestHelperRealRuntimeWait(t *testing.T) {
	if os.Getenv("LOCKWATCHDOG_HELPER_SUBPROCESS") != "1" {
		return
	}

	startDeadlockedGoroutines(t)

	var contended sync.Mutex
	stopWorkers := make(chan struct{})
	var workersDone sync.WaitGroup
	workersDone.Add(8)
	for range 8 {
		go func() {
			defer workersDone.Done()
			for {
				select {
				case <-stopWorkers:
					return
				default:
				}
				contended.Lock()
				time.Sleep(500 * time.Microsecond)
				contended.Unlock()
			}
		}()
	}
	t.Cleanup(func() {
		close(stopWorkers)
		workersDone.Wait()
	})

	// Wait briefly for both deadlocked goroutines to park in lockSlow,
	// then run two GC cycles: cycle 1 updates work.tstart at gcMark and
	// cycle 2 stamps gp.waitsince in markroot.
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	runtime.GC()

	w := New(true)
	w.restartThreshold = time.Minute
	w.minRuntimeWait = time.Minute
	w.check()

	time.Sleep(61 * time.Second)
	all, stalled, restart := w.check()
	assert.True(t, restart, "deadlocked goroutines must satisfy runtimeWait >= 1m")
	require.GreaterOrEqual(t, len(stalled), 2)

	var deadlockedAddrs []string
	contendedParked := 0
	for _, g := range all {
		if !g.isLockWait() {
			continue
		}
		if strings.Contains(g.topFunction(), "startDeadlockedGoroutines") {
			assert.GreaterOrEqual(t, g.runtimeWait, time.Minute)
			assert.NotEmpty(t, g.lockAddr)
			deadlockedAddrs = append(deadlockedAddrs, g.lockAddr)
		} else if strings.Contains(g.topFunction(), "TestHelperRealRuntimeWait") {
			contendedParked++
			assert.Zero(t, g.runtimeWait, "unparking workers must reset gp.waitsince")
			assert.NotEmpty(t, g.lockAddr)
		}
	}
	require.Len(t, deadlockedAddrs, 2)
	assert.NotEqual(t, deadlockedAddrs[0], deadlockedAddrs[1])
	t.Logf("verified CGO_ENABLED=0 dump: deadlocked=%v contendedParked=%d", deadlockedAddrs, contendedParked)
}

func startDeadlockedGoroutines(t *testing.T) {
	t.Helper()
	var a, b sync.Mutex
	var bothLocked, done sync.WaitGroup
	bothLocked.Add(2)
	done.Add(2)
	go func() {
		defer done.Done()
		a.Lock()
		bothLocked.Done()
		bothLocked.Wait()
		b.Lock()
		b.Unlock()
	}()
	go func() {
		defer done.Done()
		b.Lock()
		bothLocked.Done()
		bothLocked.Wait()
		a.Lock()
		a.Unlock()
	}()
	bothLocked.Wait()
	// In Go a Mutex is not bound to a goroutine, so unlocking a and b from
	// Cleanup breaks the cycle and lets both goroutines exit between
	// -count=N test runs.
	t.Cleanup(func() {
		a.Unlock()
		b.Unlock()
		done.Wait()
	})
}

func startDeadlockedRWMutexGoroutines(t *testing.T) {
	t.Helper()
	var a, b sync.RWMutex
	var bothLocked, done sync.WaitGroup
	bothLocked.Add(2)
	done.Add(2)
	go func() {
		defer done.Done()
		a.RLock()
		bothLocked.Done()
		bothLocked.Wait()
		b.RLock()
		b.RUnlock()
	}()
	go func() {
		defer done.Done()
		b.Lock()
		bothLocked.Done()
		bothLocked.Wait()
		a.Lock()
		a.Unlock()
	}()
	bothLocked.Wait()
	t.Cleanup(func() {
		a.RUnlock()
		b.Unlock()
		done.Wait()
	})
}
