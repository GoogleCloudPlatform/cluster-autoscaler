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
	"context"
	"k8s.io/klog/v2"
)

func logTarget() {
	klog.V(4).Infof("This is the end of the line")
}

func FuncA(ctx context.Context, msg string) {
	klog.Infof("Message from FuncA: %s", msg)
	FuncB(msg)
}

func FuncB(text string) {
	formatStr := "FuncB logging %s"
	klog.Infof(formatStr, text)
	FuncC(text, 0)
}

func FuncC(val string, depth int) {
	if depth > 3 {
		logTarget()
		return
	}
	klog.Errorf("Reached max depth %d", depth)
	FuncC(val, depth+1)
}

func Unreachable(msg string) {
	klog.Warningf("This function is not on path: %s", msg)
}

func FuncWithClosures() {
	myCtx := context.Background()
	_ = myCtx

	func() {
		klog.Infof("Inside closure!")
		logTarget()
	}()
}

type StaticAutoscaler struct{}

func (s *StaticAutoscaler) RunOnce() {
	myCtx := context.Background()
	FuncA(myCtx, "start")
	TriggerThirdParty()
	FuncWithClosures()
}

func main() {
	s := &StaticAutoscaler{}
	s.RunOnce()
}
