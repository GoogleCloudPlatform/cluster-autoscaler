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

func logTarget(ctx context.Context) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("This is the end of the line")
}

func FuncA(ctx context.Context, msg string) {
	logger := klog.FromContext(ctx)
	logger.Info("Message from FuncA:", "msg", msg)
	FuncB(ctx, msg)
}

func FuncB(ctx context.Context, text string) {
	logger := klog.FromContext(ctx)
	formatStr := "FuncB logging %s"
	logger.
		// TODO: unable to migrate log message. Please migrate manually.
		Info(formatStr, "text", text)
	FuncC(ctx, text, 0)
}

func FuncC(ctx context.Context, val string, depth int) {
	logger := klog.FromContext(ctx)
	if depth > 3 {
		logTarget(ctx)
		return
	}
	logger.Error(nil, "Reached max depth", "depth", depth)
	FuncC(ctx, val, depth+1)
}

func Unreachable(msg string) {
	klog.Warningf("This function is not on path: %s", msg)
}

func FuncWithClosures(ctx context.Context) {
	logger := klog.FromContext(ctx)
	myCtx := context.Background()
	_ = myCtx

	func() {
		logger.Info("Inside closure!")
		logTarget(ctx)
	}()
}

type StaticAutoscaler struct{}

func (s *StaticAutoscaler) RunOnce(ctx context.Context) {
	myCtx := context.Background()
	FuncA(myCtx, "start")
	TriggerThirdParty(ctx)
	FuncWithClosures(ctx)
}

func main() {
	s := &StaticAutoscaler{}
	s.RunOnce(context.TODO())
}
