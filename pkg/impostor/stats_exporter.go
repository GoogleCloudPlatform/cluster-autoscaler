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

package impostor

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

var outputParentDir string

func init() {
	outputParentDir = path.Join("/tmp/scale_down_test_result", time.Now().Format("2006-1-2_15:4:5"))
}

var (
	csvHeader                 = []string{"time elapsed [s]", "node count", "pod evictions count"}
	nodeCountRecorderInterval = 5 * time.Second
	metadataFilename          = "TEST_METADATA"
	outputFilename            = "output.csv"
)

type clusterInfoDataPoint struct {
	nodeCount           int
	podEvictionsCount   int
	timeSinceStartInSec int64
}

// SdTestParameters includes parametes that are passed to scale down test.
type SdTestParameters struct {
	nodesInNodeGroup        int
	numNodeGroups           int
	maxScaleDownParallelism int
	maxDrainParallelism     int
}

type statsExporter struct {
	SdTestParameters
	name                   string
	mutex                  sync.Mutex
	client                 *MockKubeClient
	nodeCountDPs           []*clusterInfoDataPoint
	scaleDownUnneededTime  time.Duration
	scaleDownUtilThreshold float64
	podEvictionsCount      int
}

func NewStatsExporter(p *SdTestParameters, name string, scaleDownUnneededTime time.Duration, scaleDownUtilThreshold float64) *statsExporter {
	return &statsExporter{
		SdTestParameters:       *p,
		name:                   name,
		scaleDownUnneededTime:  scaleDownUnneededTime,
		scaleDownUtilThreshold: scaleDownUtilThreshold,
	}
}

func (e *statsExporter) trackNodesCount(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	start := time.Now()
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			nCount := getNodeCount(e.client)
			e.mutex.Lock()
			e.nodeCountDPs = append(e.nodeCountDPs, &clusterInfoDataPoint{nodeCount: nCount, timeSinceStartInSec: int64(time.Since(start).Seconds()), podEvictionsCount: e.podEvictionsCount})
			e.mutex.Unlock()
		}
	}
}

func (e *statsExporter) PodEvicted(_ *v1.Pod) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.podEvictionsCount++
}

func (e *statsExporter) PodScheduled(_ *v1.Pod) {}

func (e *statsExporter) export(nodeCount int, totalTime time.Duration) error {
	if !e.exportingEnabled() {
		return nil
	}
	name := "legacy"
	if e.maxDrainParallelism > 1 {
		name = "parallel-sd"
	}
	name = fmt.Sprintf("%s-%d", name, e.nodesInNodeGroup*e.numNodeGroups)
	dirName := path.Join(outputParentDir, e.name, name)
	klog.Infof("Dumping test data to: %s", dirName)
	if err := os.MkdirAll(dirName, os.ModePerm); err != nil {
		return err
	}
	err := e.writeMetadata(path.Join(dirName, metadataFilename), nodeCount, totalTime)
	if err != nil {
		return err
	}
	return e.writePerfData(path.Join(dirName, outputFilename))
}

func (e *statsExporter) exportingEnabled() bool {
	return e.name != ""
}

func (e *statsExporter) writeMetadata(filename string, nodeCount int, totalTime time.Duration) error {
	metadataFile, err := os.Create(filename)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(metadataFile)
	for _, line := range []string{
		fmt.Sprintf("Initial node count: %v", e.nodesInNodeGroup*e.numNodeGroups),
		fmt.Sprintf("Final node count: %v", nodeCount),
		fmt.Sprintf("Total pod eviction: %v", e.podEvictionsCount),
		fmt.Sprintf("Test time: %v", totalTime),
		fmt.Sprintf("Parameters:"),
		fmt.Sprintf("Scale down unneeded time: %v", e.scaleDownUnneededTime),
		fmt.Sprintf("Scale down utilization threshold: %v", e.scaleDownUtilThreshold),
		fmt.Sprintf("Max scale down parallelism: %d", e.maxScaleDownParallelism),
		fmt.Sprintf("Max drain parallelism: %d", e.maxDrainParallelism),
	} {
		if _, err = fmt.Fprintln(w, line); err != nil {
			return err
		}
	}
	return w.Flush()
}

func (e *statsExporter) writePerfData(filename string) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	csvFile, err := os.Create(filename)
	if err != nil {
		return err
	}
	csvWriter := csv.NewWriter(csvFile)

	// write header
	err = csvWriter.Write(csvHeader)
	if err != nil {
		return err
	}
	for _, dp := range e.nodeCountDPs {
		err = csvWriter.Write([]string{fmt.Sprintf("%d", dp.timeSinceStartInSec), fmt.Sprintf("%d", dp.nodeCount), fmt.Sprintf("%d", dp.podEvictionsCount)})
		if err != nil {
			return err
		}
	}
	csvWriter.Flush()
	return nil
}
