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

// Package lockwatchdog detects goroutines that have been blocked acquiring a
// lock for too long: after 1 minute it logs all goroutine stacks (at most once
// per minute while the stall continues), and after 5 minutes (when restarts
// are enabled) it logs a final report and exits the process so kubelet
// restarts the container.
//
// While enabled, the watchdog takes a dump of all goroutines (runtime.Stack)
// every 15s. A goroutine observed in sync.Mutex.Lock, sync.RWMutex.Lock, or
// sync.RWMutex.RLock with the same stack across consecutive dumps is logged
// once it reaches 1 minute, and triggers a restart at 5 minutes only when the
// Go runtime's own wait duration (gp.waitsince, reset on every unpark) is at
// least 3 minutes. Detection does not depend on what the main autoscaler loop
// is doing or how long it takes.
//
// A plain goroutine dump cannot show who holds a lock (the Go runtime does not
// track mutex owners), so the watchdog logs every goroutine: the blocked ones
// first, then everything else grouped by identical stack, so that the holder
// can be found by hand.
package lockwatchdog

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"
)

const (
	defaultInterval     = 15 * time.Second
	defaultLogThreshold = time.Minute
	defaultLogInterval  = time.Minute
	// defaultRestartThreshold exceeds the 3m GCE HTTP client timeout so a
	// single slow GCE call under a lock cannot trigger a restart.
	// TODO(b/584577365): Bound paginated GCE .Pages() calls with
	// operationPerCallTimeout so multi-page fetches under a lock are also
	// bounded by a per-call timeout.
	defaultRestartThreshold = 5 * time.Minute
	// defaultMinRuntimeWait is the minimum runtime-reported wait duration
	// (", N minutes" from gp.waitsince in runtime.Stack) required before
	// restarting. In Go's runtime, gcMark updates work.tstart at mark
	// termination of the previous GC cycle (runtime/mgc.go) and sysmon
	// forces a GC at least every 2m (forcegcperiod), so markroot can
	// backdate gp.waitsince by up to 2m. Requiring >= 3m rules out both
	// that 2m backdating window and contended mutexes that unpark between
	// ticks (which reset gp.waitsince to 0 in runtime/proc.go:execute).
	defaultMinRuntimeWait = 3 * time.Minute

	// ExitCode is the process exit code used when a stall reaches
	// restartThreshold. It is distinct from klog.Fatal (255) and os.Exit(1)
	// so the restart reason shows up in the container's termination state.
	ExitCode = 3

	// logMarker prefixes every log line of a report so all lines of one
	// incident can be found with a single text filter.
	logMarker = "LOCK_WAIT_STALL"

	maxDumpBytes = 64 << 20
)

type seenEntry struct {
	since          time.Time
	maxRuntimeWait time.Duration
}

// Watchdog periodically checks for goroutines blocked on locks.
type Watchdog struct {
	interval         time.Duration
	logThreshold     time.Duration
	logInterval      time.Duration
	restartThreshold time.Duration
	minRuntimeWait   time.Duration

	enabled        atomic.Bool
	restartEnabled atomic.Bool
	notify         chan struct{}

	// Injected for tests.
	stacks func() []byte
	exit   func(code int)

	seen    map[string]seenEntry
	lastLog time.Time
}

// New creates a watchdog with the given initial enabled state and restarts
// enabled. Call Run to start it.
func New(enabled bool) *Watchdog {
	w := &Watchdog{
		interval:         defaultInterval,
		logThreshold:     defaultLogThreshold,
		logInterval:      defaultLogInterval,
		restartThreshold: defaultRestartThreshold,
		minRuntimeWait:   defaultMinRuntimeWait,
		notify:           make(chan struct{}, 1),
		stacks:           allStacks,
		exit:             os.Exit,
		seen:             map[string]seenEntry{},
	}
	w.enabled.Store(enabled)
	w.restartEnabled.Store(true)
	return w
}

// SetEnabled enables or disables the watchdog at runtime without restarting
// the process. Safe to call concurrently from any goroutine.
func (w *Watchdog) SetEnabled(enabled bool) {
	if w.enabled.Swap(enabled) == enabled {
		return
	}
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

// SetRestartEnabled controls whether stalls reaching restartThreshold exit the
// process or stay in log-only mode (for example until the first successful
// autoscaler run completes). Safe to call concurrently.
func (w *Watchdog) SetRestartEnabled(enabled bool) {
	w.restartEnabled.Store(enabled)
}

// Run checks goroutines every interval while enabled until stop is closed. Run
// is meant to be started in its own goroutine.
func (w *Watchdog) Run(stop <-chan struct{}) {
	var ticker *time.Ticker
	var tickCh <-chan time.Time
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()

	syncState := func() {
		if w.enabled.Load() {
			if ticker == nil {
				ticker = time.NewTicker(w.interval)
				tickCh = ticker.C
				klog.Infof("Lock wait watchdog started: interval=%v logThreshold=%v restartThreshold=%v", w.interval, w.logThreshold, w.restartThreshold)
			}
			return
		}
		if ticker != nil {
			ticker.Stop()
			ticker = nil
			tickCh = nil
			clear(w.seen)
			w.lastLog = time.Time{}
			klog.Infof("Lock wait watchdog disabled")
		}
	}

	if w.enabled.Load() {
		syncState()
	} else {
		klog.Infof("Lock wait watchdog disabled")
	}

	for {
		select {
		case <-stop:
			return
		case <-w.notify:
			syncState()
		case <-tickCh:
			if !w.enabled.Load() {
				syncState()
				continue
			}
			if w.step() {
				return
			}
		}
	}
}

// step runs a single watchdog check and returns true if the process exited.
func (w *Watchdog) step() bool {
	now := time.Now()
	all, stalled, restart := w.checkAt(now)
	if len(stalled) == 0 {
		w.lastLog = time.Time{}
		return false
	}
	if restart && w.restartEnabled.Load() {
		w.report(now, all, stalled, true)
		klog.Errorf("%s: exiting with code %d", logMarker, ExitCode)
		klog.Flush()
		w.exit(ExitCode)
		return true
	}
	if w.lastLog.IsZero() || now.Sub(w.lastLog) >= w.logInterval {
		w.report(now, all, stalled, false)
		w.lastLog = now
	}
	return false
}

// check takes one goroutine dump and returns all goroutines, the ones that
// have been blocked on a lock for at least logThreshold (longest first), and
// whether any of them has reached restartThreshold with runtime-confirmed wait
// time.
func (w *Watchdog) check() (all, stalled []*goroutine, restart bool) {
	return w.checkAt(time.Now())
}

func (w *Watchdog) checkAt(now time.Time) (all, stalled []*goroutine, restart bool) {
	all = parseDump(w.stacks())
	seen := map[string]seenEntry{}
	canRestart := func(g *goroutine) bool {
		return g.waited >= w.restartThreshold && g.runtimeWait >= w.minRuntimeWait
	}
	for _, g := range all {
		if !g.isLockWait() {
			continue
		}
		key := fmt.Sprintf("%d|%s|%s|%s", g.id, g.state, g.lockAddr, g.stack())
		entry, ok := w.seen[key]
		if !ok || g.runtimeWait < entry.maxRuntimeWait {
			// Either this is the first dump observing this lock
			// wait, or gp.waitsince decreased because the goroutine
			// unparked and re-blocked at the same call site.
			entry = seenEntry{since: now}
		}
		if g.runtimeWait > entry.maxRuntimeWait {
			entry.maxRuntimeWait = g.runtimeWait
		}
		seen[key] = entry
		g.waited = now.Sub(entry.since)
		shouldRestart := canRestart(g)
		if shouldRestart || g.waited >= w.logThreshold {
			stalled = append(stalled, g)
		}
		if shouldRestart {
			restart = true
		}
	}
	w.seen = seen
	sort.Slice(stalled, func(i, j int) bool {
		ri, rj := canRestart(stalled[i]), canRestart(stalled[j])
		if ri != rj {
			return ri
		}
		return stalled[i].waited > stalled[j].waited
	})
	return all, stalled, restart
}

// report logs the stalled goroutines, then every other goroutine grouped by
// identical state and stack so that hundreds of idle informer or HTTP
// goroutines collapse into a few lines. Smaller groups are logged first:
// the unusual goroutines are the interesting ones.
func (w *Watchdog) report(now time.Time, all, stalled []*goroutine, restart bool) {
	id := now.Format("150405.000")
	worst := stalled[0]
	action, logf := "log", klog.Warningf
	if restart {
		action, logf = "restart", klog.Errorf
	}
	logf("%s id=%s action=%s: %d goroutine(s) blocked on a lock for >= %v (longest: goroutine %d in %s, state %s, waited >= %v, runtimeWait >= %v); %d goroutines total; see the following %s lines for all stacks",
		logMarker, id, action, len(stalled), w.logThreshold, worst.id, worst.topFunction(), worst.state, worst.waited.Round(time.Second), worst.runtimeWait, len(all), logMarker)
	isStalled := map[int]bool{}
	for _, g := range stalled {
		isStalled[g.id] = true
		logf("%s id=%s action=%s blocked goroutine %d [%s] waited >= %v runtimeWait >= %v created_by=%s stack: %s",
			logMarker, id, action, g.id, g.state, g.waited.Round(time.Second), g.runtimeWait, g.createdBy, g.stack())
	}
	groups := map[string][]int{}
	var order []string
	for _, g := range all {
		if isStalled[g.id] {
			continue
		}
		desc := fmt.Sprintf("[%s] created_by=%s stack: %s", g.state, g.createdBy, g.stack())
		if _, ok := groups[desc]; !ok {
			order = append(order, desc)
		}
		groups[desc] = append(groups[desc], g.id)
	}
	sort.SliceStable(order, func(i, j int) bool { return len(groups[order[i]]) < len(groups[order[j]]) })
	for _, desc := range order {
		logf("%s id=%s action=%s %d goroutine(s) %v %s", logMarker, id, action, len(groups[desc]), groups[desc], desc)
	}
}

// allStacks returns a dump of all goroutines, growing the buffer as needed.
func allStacks() []byte {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) || len(buf) >= maxDumpBytes {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}
