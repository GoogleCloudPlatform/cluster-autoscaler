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

package nodeannotator

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	// logPrefix is added to all log messages from this package.
	logPrefix = "Node Annotation: "
	// Default Annotation Cycle Interval
	defaultInterval = 1 * time.Minute
	// Default Timeout for API Patch calls
	defaultAPITimeout = 10 * time.Second
	// Default number of concurrent node processing workers
	defaultWorkerCount = 5
)

// Plugin defines the interface for node annotation plugins.
type Plugin interface {
	// Name returns the unique name of the plugin, used primarily for logging.
	Name() string
	// GetAnnotation returns a map of annotations the plugin wants to ensure exist on the node.
	// It should return an empty map or nil if no annotations are needed from this plugin.
	// An error indicates a problem processing the node or determining annotations for this specific plugin.
	// Returning an error will prevent annotations from *this* plugin being applied in the current cycle,
	// but will not stop other plugins or the overall node processing.
	GetAnnotation(node *apiv1.Node) (map[string]string, error)
}

// Config holds configuration options for the NodeAnnotator.
type Config struct {
	// Interval is the time duration between full cycles of processing nodes from the cache.
	Interval time.Duration
	// APITimeout is the timeout for individual Kubernetes API Patch calls used to apply annotations.
	APITimeout time.Duration
	// WorkerCount is the number of concurrent workers processing nodes during each cycle.
	WorkerCount int
}

// NodeAnnotator orchestrates the node annotation process.
type NodeAnnotator struct {
	kubeClient kubernetes.Interface
	plugins    []Plugin
	config     Config
	nodeLister kube_util.NodeLister
}

// NewNodeAnnotator creates a new NodeAnnotator instance.
func NewNodeAnnotator(
	kubeClient kubernetes.Interface,
	nodeLister kube_util.NodeLister,
	cfg Config,
) *NodeAnnotator {

	// Apply default config values if zero values are provided.
	if cfg.Interval <= 0 {
		cfg.Interval = defaultInterval
	}
	if cfg.APITimeout <= 0 {
		cfg.APITimeout = defaultAPITimeout
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = defaultWorkerCount
	}

	klog.V(1).Infof(logPrefix+"initialized with Interval=%v, APITimeout=%v, WorkerCount=%d",
		cfg.Interval, cfg.APITimeout, cfg.WorkerCount)

	return &NodeAnnotator{
		kubeClient: kubeClient,
		plugins:    []Plugin{},
		config:     cfg,
		nodeLister: nodeLister,
	}
}

// RegisterPlugin adds a new annotation plugin to the annotator.
// This should be called before Start(). It is not safe for concurrent use after Start().
func (na *NodeAnnotator) RegisterPlugin(plugin Plugin) {
	if plugin == nil {
		klog.Warningf(logPrefix + "attempted to register a nil plugin.")
		return
	}
	na.plugins = append(na.plugins, plugin)
	klog.V(1).Infof(logPrefix+"registered plugin %q (%T)", plugin.Name(), plugin)
}

// Start begins the node annotation process in the background.
func (na *NodeAnnotator) Start(ctx context.Context) error {
	defer runtime.HandleCrash()

	klog.V(1).Infof(logPrefix+"starting annotation processing loop with interval %v.", na.config.Interval)
	// Start go routine for node annotation.
	// Once one cycle of runAnnotationCycle finishes, it waits na.config.Interval to run runAnnotationCycle again.
	go wait.UntilWithContext(ctx, na.runAnnotationCycle, na.config.Interval)

	klog.V(1).Infof(logPrefix + "started successfully.")
	return nil
}

// runAnnotationCycle performs a single cycle of processing nodes found in the cache.
func (na *NodeAnnotator) runAnnotationCycle(ctx context.Context) {
	klog.V(1).Infof(logPrefix + "starting annotation cycle...")
	startTime := time.Now()

	// List all nodes from the local cache via the Lister.
	nodes, err := na.nodeLister.List()
	if err != nil {
		// Errors listing from cache are generally unexpected after initial sync.
		klog.Errorf(logPrefix+"failed to list nodes from cache: %v", err)
		return // Abort this cycle; will retry on the next interval.
	}

	nodeCount := len(nodes)
	klog.V(5).Infof(logPrefix+"found %d nodes in cache to process.", nodeCount)
	if nodeCount == 0 {
		klog.V(5).Infof(logPrefix + "no nodes found, cycle finished.")
		return
	}

	// Use a WaitGroup and worker pool to process nodes concurrently.
	var wg sync.WaitGroup
	// Create a buffered channel to distribute node work.
	// Note: Buffer size can be tuned; nodeCount is safe but potentially high memory.
	nodeChan := make(chan *apiv1.Node, nodeCount)

	// Start worker goroutines.
	actualWorkerCount := na.config.WorkerCount
	if nodeCount < actualWorkerCount {
		actualWorkerCount = nodeCount
	}
	klog.V(5).Infof(logPrefix+"starting %d worker(s) for node processing.", actualWorkerCount)
	for i := 0; i < actualWorkerCount; i++ {
		wg.Add(1)
		go na.nodeWorker(ctx, &wg, i, nodeChan)
	}

	// Distribute nodes to workers.
	for _, node := range nodes {
		// IMPORTANT: Send a deep copy of the node to the worker channel.
		// This prevents race conditions where the worker processes a node
		// while the informer updates the object in the cache simultaneously.
		nodeCopy := node.DeepCopy()
		select {
		case nodeChan <- nodeCopy:
			// Work submitted successfully.
		case <-ctx.Done():
			klog.V(1).Infof(logPrefix + "context cancelled during node distribution, aborting cycle.")
			close(nodeChan)
			goto cleanup
		}
	}
	// Close the channel to signal workers that no more nodes are coming.
	close(nodeChan)

cleanup:
	// Wait for all workers launched in *this cycle* to complete.
	wg.Wait()
	klog.V(1).Infof(logPrefix+"annotation cycle finished processing %d nodes in %v.", nodeCount, time.Since(startTime))
}

// nodeWorker is a goroutine that processes nodes received from the nodeChan.
func (na *NodeAnnotator) nodeWorker(ctx context.Context, wg *sync.WaitGroup, workerID int, nodeChan <-chan *apiv1.Node) {
	defer wg.Done()
	klog.V(5).Infof(logPrefix+"worker %d started.", workerID)

	for {
		select {
		case <-ctx.Done():
			// Annotator context cancelled, shut down the worker.
			klog.V(5).Infof(logPrefix+"worker %d shutting down due to context cancellation.", workerID)
			return
		case node, ok := <-nodeChan:
			if !ok {
				klog.V(5).Infof(logPrefix+"worker %d exiting (channel closed).", workerID)
				return
			}
			// Process the received node (which is a DeepCopy).
			if err := na.processNode(ctx, node); err != nil {
				nodeName := "unknown"
				if node != nil {
					nodeName = node.Name
				}
				klog.Errorf(logPrefix+"worker %d failed to process node %q: %v", workerID, nodeName, err)
			}
		}
	}
}

// processNode checks a single node, gathers desired annotations from plugins,
// and applies / modifies any missing annotations via a Patch request.
func (na *NodeAnnotator) processNode(ctx context.Context, node *apiv1.Node) error {
	if node == nil {
		return fmt.Errorf("received nil node")
	}
	klog.V(5).Infof(logPrefix+"processing node %q", node.Name)
	processStart := time.Now()

	annotationsToApply := make(map[string]string)

	// Get current annotations from the node (handle nil map).
	nodeAnnotations := node.GetAnnotations()
	if nodeAnnotations == nil {
		nodeAnnotations = make(map[string]string)
	}

	// Gather annotations from all registered plugins.
	for _, plugin := range na.plugins {
		pluginName := plugin.Name()
		klog.V(5).Infof(logPrefix+"running plugin %q for node %q", pluginName, node.Name)
		pluginStart := time.Now()

		// Get annotations from the current plugin.
		pluginAnnotations, err := plugin.GetAnnotation(node)
		if err != nil {
			klog.Warningf(logPrefix+"plugin %q failed for node %q (took %v): %v", pluginName, node.Name, time.Since(pluginStart), err)
			continue
		}

		// Check if annotations from this plugin need to be applied.
		for key, desiredValue := range pluginAnnotations {
			if key == "" {
				klog.Warningf(logPrefix+"plugin %q provided an empty annotation key for node %q, skipping.", pluginName, node.Name)
				continue
			}
			currentValue, exists := nodeAnnotations[key]
			// Apply only if the annotation does not exist on the node.
			if !exists {
				annotationsToApply[key] = desiredValue
				klog.V(4).Infof(logPrefix+"annotation needed for node %q from plugin %q: %s=%q", node.Name, pluginName, key, desiredValue)
			} else if currentValue != desiredValue {
				annotationsToApply[key] = desiredValue
				klog.V(5).Infof(logPrefix+"annotation %q already exists on node %q with value %q, plugin %q suggests %q (applying change).", key, node.Name, currentValue, pluginName, desiredValue)
			} else {
				klog.V(5).Infof(logPrefix+"annotation %q already exists on node %q with correct value from plugin %q.", key, node.Name, pluginName)
			}
		}
	}

	if len(annotationsToApply) == 0 {
		klog.V(5).Infof(logPrefix+"no new annotations needed for node %q (processing took %v).", node.Name, time.Since(processStart))
		return nil
	}

	klog.V(1).Infof(logPrefix+"applying %d annotation(s) to node %q: %v", len(annotationsToApply), node.Name, keysToString(annotationsToApply))

	// Construct the JSON Merge Patch payload. This only adds/updates specified fields
	// under metadata.annotations without affecting other annotations or metadata.
	patchPayload := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": annotationsToApply,
		},
	}
	patchBytes, err := json.Marshal(patchPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal patch payload for node %s: %w", node.Name, err)
	}

	// Use the configured API timeout for the Patch operation.
	patchCtx, cancelPatch := context.WithTimeout(ctx, na.config.APITimeout)
	defer cancelPatch()

	patchStart := time.Now()
	_, err = na.kubeClient.CoreV1().Nodes().Patch(patchCtx, node.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	patchDuration := time.Since(patchStart)

	if err != nil {
		if errors.IsConflict(err) {
			// Conflicts are common if the node was updated between the cache read and the patch attempt.
			// The next cycle will likely succeed if the annotation is still missing. Treat as non-fatal for this cycle.
			klog.Warningf(logPrefix+"conflict patching node %q (took %v), annotation might be applied in next cycle if still needed: %v", node.Name, patchDuration, err)
			return nil
		}
		if errors.IsTimeout(err) || patchCtx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("timeout patching node %s (took %v): %w", node.Name, patchDuration, err)
		}
		// Log other API errors.
		klog.Errorf(logPrefix+"failed to patch node %q (took %v): %v", node.Name, patchDuration, err)
		return fmt.Errorf("failed to patch node %s: %w", node.Name, err)
	}

	klog.V(5).Infof(logPrefix+"successfully applied annotations to node %q (patch took %v, total processing %v).", node.Name, patchDuration, time.Since(processStart))
	return nil
}

// keysToString is a helper to log keys being applied without potentially long values.
func keysToString(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
