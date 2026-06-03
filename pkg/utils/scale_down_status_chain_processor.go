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

package utils

import (
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	processors "k8s.io/autoscaler/cluster-autoscaler/processors/status"
)

// ScaleDownStatusChainProcessor chains the execution of a list of scale down status Processors.
type ScaleDownStatusChainProcessor struct {
	Processors []processors.ScaleDownStatusProcessor
}

// Process executes Process for all of the internal processors.
func (p *ScaleDownStatusChainProcessor) Process(context *context.AutoscalingContext, status *status.ScaleDownStatus) {
	for _, proc := range p.Processors {
		proc.Process(context, status)
	}
}

// CleanUp executes CleanUp for all of the internal processors.
func (p *ScaleDownStatusChainProcessor) CleanUp() {
	for _, proc := range p.Processors {
		proc.CleanUp()
	}
}

// AddProcessor adds a processor to the end of the processor chain.
func (p *ScaleDownStatusChainProcessor) AddProcessor(processor processors.ScaleDownStatusProcessor) {
	p.Processors = append(p.Processors, processor)
}
