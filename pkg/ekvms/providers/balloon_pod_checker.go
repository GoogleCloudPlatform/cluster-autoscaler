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

package providers

import (
	"context"
	"fmt"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	"k8s.io/klog/v2"
)

const balloonPodCreationTimeout = 15 * time.Second

type balloonPodSize struct {
	CPU    int64
	Memory string
}

var balloonPodTestSizes = []balloonPodSize{
	{CPU: 0, Memory: "50Mi"},
	{CPU: 1, Memory: "4Gi"},
	{CPU: 2, Memory: "8Gi"},
	{CPU: 4, Memory: "16Gi"},
	{CPU: 8, Memory: "32Gi"},
	{CPU: 12, Memory: "48Gi"},
	{CPU: 16, Memory: "64Gi"},
	{CPU: 20, Memory: "80Gi"},
	{CPU: 24, Memory: "96Gi"},
	{CPU: 28, Memory: "112Gi"},
}

type balloonPodChecker struct {
	clientSet                    clientset.Interface
	isBalloonPodCreatable        bool
	balloonPodCreationErrorCount int
	balloonPodSizeIndex          int
	runInterval                  time.Duration
}

func (c *balloonPodChecker) Run(stopCh <-chan struct{}) {
	wait.Until(func() {
		if err := c.dryRunCreateBalloonPod(); err != nil {
			c.recordFailure(err)
			return
		}
		c.resetSuccess()
	}, c.runInterval, stopCh)
}

func (c *balloonPodChecker) dryRunCreateBalloonPod() error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), balloonPodCreationTimeout)
	defer cancelFunc()

	node := &apiv1.Node{}
	cpu := *resource.NewQuantity(c.currentSize().CPU, resource.DecimalSI)
	memory := resource.MustParse(c.currentSize().Memory)
	bp, err := operationtracker.GenerateBalloonPod(node, cpu, memory, false)
	if err != nil {
		return fmt.Errorf("balloon pod generation error: %v", err)
	}
	_, err = c.clientSet.CoreV1().Pods(bp.Namespace).Create(ctx, bp, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err != nil {
		return fmt.Errorf("balloon pod creation error: %v", err)
	}

	return nil
}

func (c *balloonPodChecker) currentSize() balloonPodSize {
	return balloonPodTestSizes[c.balloonPodSizeIndex]
}

func (c *balloonPodChecker) resetSuccess() {
	if !c.isBalloonPodCreatable {
		klog.Infof("Balloon pod dry run creation succeeded after failures")
	}
	c.isBalloonPodCreatable = true
	c.balloonPodCreationErrorCount = 0
	// Increment the balloonPodSizeIndex.
	c.balloonPodSizeIndex = (c.balloonPodSizeIndex + 1) % len(balloonPodTestSizes)
}

func (c *balloonPodChecker) recordFailure(err error) {
	c.balloonPodCreationErrorCount++
	if c.isBalloonPodCreatable && c.balloonPodCreationErrorCount == 3 {
		klog.Warningf("Balloon pod dry run creation failed 3 times consecutively on size %v: %v", c.currentSize(), err)
		c.isBalloonPodCreatable = false
	}
}
