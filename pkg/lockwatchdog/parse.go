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
	"regexp"
	"strconv"
	"strings"
	"time"
)

// goroutine is a single goroutine parsed from a runtime.Stack(buf, true) dump.
type goroutine struct {
	id int
	// state is the goroutine status or wait reason from the runtime,
	// e.g. "running", "select", "sync.Mutex.Lock" (with any transient
	// "(scan)" / "(leaked)" / "(durable)" suffix stripped).
	state string
	// runtimeWait is the wait duration printed by the runtime in the
	// goroutine header ("[sync.Mutex.Lock, 3 minutes]"). The runtime sets
	// gp.waitsince when GC scans a blocked goroutine and resets it to 0
	// whenever the goroutine unparks, so runtimeWait > 0 means the
	// goroutine has stayed parked since that GC. Because markroot stamps
	// gp.waitsince from the previous cycle's work.tstart, runtimeWait can
	// overstate the actual wait by up to ~2m (forcegcperiod); see
	// defaultMinRuntimeWait.
	runtimeWait time.Duration
	// lockAddr is the live lock pointer ("0x...") from (*Mutex).lockSlow or
	// sync.(*RWMutex).Lock/RLock when printed without a "?" dead-slot
	// marker. In CGO_ENABLED=0 builds without -race, RWMutex.RLock is
	// inlined and RWMutex.Lock marks its receiver dead ("0x0?"), so
	// lockAddr is empty there and Watchdog relies on runtimeWait to rule
	// out false restarts on RWMutex loops. Caller frame arguments are not
	// used because GC stack shrinking can relocate stack pointers on parked
	// goroutines.
	lockAddr string
	// frames are the stack frames, innermost first.
	frames    []frame
	createdBy string
	// waited is how long the watchdog has observed the goroutine blocked
	// in the current stall episode. Set by Watchdog.checkAt.
	waited time.Duration
}

type frame struct {
	function string
	location string // file:line
}

var (
	// goroutine 18 [sync.Mutex.Lock, 3 minutes]:
	// goroutine 1 gp=0xc000002380 m=0 mp=0x5f1b20 [running]:
	headerRe  = regexp.MustCompile(`^goroutine (\d+)(?: [^\[]*)? \[([^\]]*)\]:$`)
	minutesRe = regexp.MustCompile(`^(\d+) minutes?$`)
)

// lockWaitStates are the runtime wait reasons that mean "blocked acquiring a
// sync.Mutex or sync.RWMutex". sync.Cond.Wait and sync.WaitGroup.Wait are
// deliberately excluded: idle workqueue workers legitimately sit in Cond.Wait
// forever.
var lockWaitStates = map[string]bool{
	"sync.Mutex.Lock":    true,
	"sync.RWMutex.Lock":  true,
	"sync.RWMutex.RLock": true,
}

func (g *goroutine) isLockWait() bool {
	return lockWaitStates[g.state]
}

// runtimeFramePrefixes identify frames that belong to the Go runtime or the
// sync package rather than to the program.
var runtimeFramePrefixes = []string{"runtime.", "sync.", "internal/sync.", "internal/poll.", "internal/runtime/"}

// topFunction returns the innermost function that belongs to the program, or
// the innermost function if all of them are runtime frames.
func (g *goroutine) topFunction() string {
	for _, f := range g.frames {
		if !isRuntimeFrame(f.function) {
			return f.function
		}
	}
	if len(g.frames) > 0 {
		return g.frames[0].function
	}
	return ""
}

func isRuntimeFrame(function string) bool {
	for _, p := range runtimeFramePrefixes {
		if strings.HasPrefix(function, p) {
			return true
		}
	}
	return false
}

func isLockMethodFrame(function string) bool {
	return strings.HasSuffix(function, ".(*Mutex).lockSlow") ||
		function == "sync.(*RWMutex).Lock" ||
		function == "sync.(*RWMutex).RLock"
}

// stack renders the frames as a single line, innermost first.
func (g *goroutine) stack() string {
	parts := make([]string, 0, len(g.frames))
	for _, f := range g.frames {
		parts = append(parts, f.function+" ("+f.location+")")
	}
	return strings.Join(parts, " <- ")
}

// parseDump parses the output of runtime.Stack(buf, true). Unparseable blocks
// are skipped: the dump is best-effort debugging output.
func parseDump(dump []byte) []*goroutine {
	var result []*goroutine
	for _, block := range strings.Split(string(dump), "\n\n") {
		if g := parseGoroutine(block); g != nil {
			result = append(result, g)
		}
	}
	return result
}

func parseGoroutine(block string) *goroutine {
	lines := strings.Split(strings.TrimRight(block, "\n"), "\n")
	m := headerRe.FindStringSubmatch(lines[0])
	if m == nil {
		return nil
	}
	id, err := strconv.Atoi(m[1])
	if err != nil {
		return nil
	}
	g := &goroutine{id: id}
	for i, part := range strings.Split(m[2], ", ") {
		if i == 0 {
			g.state, _, _ = strings.Cut(part, " (")
		} else if mm := minutesRe.FindStringSubmatch(part); mm != nil {
			minutes, _ := strconv.Atoi(mm[1])
			g.runtimeWait = time.Duration(minutes) * time.Minute
		}
	}
	// Frames come in pairs: "pkg.Func(args)" then "\tfile:line +0xoff".
	// Inlined frames have no offset.
	for i := 1; i < len(lines); i++ {
		line := lines[i]
		if rest, ok := strings.CutPrefix(line, "created by "); ok {
			g.createdBy, _, _ = strings.Cut(strings.TrimSpace(rest), " ")
			break
		}
		if strings.HasPrefix(line, "\t") || line == "" {
			continue
		}
		fn, args := splitFuncArgs(line)
		if g.lockAddr == "" && isLockMethodFrame(fn) && strings.HasPrefix(args, "0x") && !strings.ContainsAny(args, "?, ") {
			g.lockAddr = args
		}
		f := frame{function: fn}
		if i+1 < len(lines) && strings.HasPrefix(lines[i+1], "\t") {
			f.location, _, _ = strings.Cut(strings.TrimSpace(lines[i+1]), " +0x")
			i++
		}
		g.frames = append(g.frames, f)
	}
	return g
}

// splitFuncArgs splits "pkg.(*T).Method(0xc000, 0x1?)" into fn and raw args.
func splitFuncArgs(line string) (fn, args string) {
	if idx := strings.LastIndex(line, "("); idx > 0 && strings.HasSuffix(line, ")") {
		return line[:idx], line[idx+1 : len(line)-1]
	}
	return line, ""
}
