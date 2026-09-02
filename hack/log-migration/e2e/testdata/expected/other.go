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
	ca_context "e2e_test/context"
	"k8s.io/klog/v2"
)

func thirdPartyAction(ctx context.Context, fakeCtx ca_context.CustomContext) {
	logger := klog.FromContext(ctx)
	logger.Info("Doing third party stuff")
	FuncB(ctx, "from third party")
}

func TriggerThirdParty(ctx context.Context) {
	var c ca_context.CustomContext
	ca_context.DoStuff(c)
	thirdPartyAction(ctx, c)
}
