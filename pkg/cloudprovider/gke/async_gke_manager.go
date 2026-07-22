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

package gke

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	klog "k8s.io/klog/v2"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/interfaces"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/taskqueue"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/manager"
	prpods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
)

type nodePoolCreationStatus string
type nodePoolTerminationStatus string
type initializationStatus string

const (
	// initializationIdle represents a node pool that doesn't need initialization and is not being initialized
	initializationIdle initializationStatus = "idle"
	// initializationPending represents a node pool that needs initialization and is not being initialized
	initializationPending initializationStatus = "pending"
	// initializationInProgress represents a node pool being initialized and doesn't need further initialization
	initializationInProgress initializationStatus = "in-progress"
	// initializationInProgressDirty represents a node pool being initialized and already needs further initialization
	initializationInProgressDirty initializationStatus = "in-progress-dirty"

	// nodePoolCreationStatusQueued represents a node pool that is queued to start the creation process and awaits available worker
	nodePoolCreationStatusQueued nodePoolCreationStatus = "queued"
	// nodePoolCreationStatusProvisioning represents a node pool that awaits GKE provisioning operation to finish
	nodePoolCreationStatusProvisioning nodePoolCreationStatus = "provisioning"
	// nodePoolCreationStatusSyncing represents a node pool for which we observed create operation to finish, but didn't find it in GetCluster response yet
	nodePoolCreationStatusSyncing nodePoolCreationStatus = "syncing"
	// nodePoolCreationStatusRunning represents a running node pool that awaits initialization (initial scale-up)
	nodePoolCreationStatusRunning nodePoolCreationStatus = "running"
	// nodePoolCreationStatusProcessed represents an initialized node pool that will be treated as regular existing node-pool starting from next CA loop iteration
	nodePoolCreationStatusProcessed nodePoolCreationStatus = "processed"

	// nodePoolTerminationStatusQueued represents a node pool that is to start the termination process and awaits available worker
	nodePoolTerminationStatusQueued nodePoolTerminationStatus = "queued"
	// nodePoolTerminationStatusTerminating represents a node pool that awaits GKE termination operation to finish
	nodePoolTerminationStatusTerminating nodePoolTerminationStatus = "terminating"
	// nodePoolTerminationStatusTerminated represents a removed node pool that awaits finalization (event log, metrics)
	nodePoolTerminationStatusTerminated nodePoolTerminationStatus = "terminated"
	// nodePoolCreationStatus_processed represents a node pool that was finalized and awaits next Refresh so it is removed from GKE manager
	nodePoolTerminationStatusProcessed nodePoolTerminationStatus = "processed"

	// taskTypeNodePoolCreation - node pool creation async task type and task id prefix
	taskTypeNodePoolCreation taskqueue.TaskType = "node-pool-creation"
	// taskTypeNodePoolDeletion - node pool deletion async task type and task id prefix
	taskTypeNodePoolDeletion taskqueue.TaskType = "node-pool-deletion"
	// taskTypeNodePoolCleanUp - node pool clean up async task type and task id prefix. Executed when node pool creation did not fully succeed.
	taskTypeNodePoolCleanUp taskqueue.TaskType = "node-pool-cleanup"

	// maxConcurrentErrorRetries is the maximum number of retries applied to an async operation before returning error max concurrent error
	maxConcurrentErrorRetries = 60
	// defaultInitialBackoffDuration is the default value for exponential backoff starting duration
	defaultInitialBackoffDuration = 2 * time.Second
	// maxBackoffDuration is the max value for exponential backoff duration
	maxBackoffDuration = 30 * time.Second
	// nodePoolObserverTimeout timeout for node pool synchronization done in the observer, added for reliability. If there is no bug in the observer it should never be triggered.
	nodePoolObserverTimeout = migCreationWaitTimeout * 2
)

var nodePoolCreationStatuses []nodePoolCreationStatus = []nodePoolCreationStatus{
	nodePoolCreationStatusQueued,
	nodePoolCreationStatusProvisioning,
	nodePoolCreationStatusSyncing,
	nodePoolCreationStatusRunning,
	nodePoolCreationStatusProcessed,
}

var nodePoolTerminationStatuses []nodePoolTerminationStatus = []nodePoolTerminationStatus{
	nodePoolTerminationStatusQueued,
	nodePoolTerminationStatusTerminating,
	nodePoolTerminationStatusTerminated,
	nodePoolTerminationStatusProcessed,
}

type asyncDwsUpdater interface {
	interfaces.AsyncNodeGroupUpdater
	ScheduleProvReqResize(pr prpods.ProvReqID, ng string, delta int64, shouldUpdateProvReqDetails manager.ShouldUpdateProvReqDetails)
}

// extendedGkeManager exposes package internal logic for GKE cluster management
type extendedGkeManager interface {
	GkeManager
	GetCluster() (gkeclient.Cluster, error)
	NewNodePoolSpec(mig *GkeMig) (*gkeclient.NodePoolSpec, error)
	CreateNodePoolNoRefresh(nodePoolName string, nodePoolSpec *gkeclient.NodePoolSpec) error
	DeleteNodePoolNoRefresh(mig *GkeMig) error
	RefreshForce() error
}

type processedNodePoolListener = func(nodePoolName string)

// asyncGkeManager implements async operations of GkeManager
//
// asyncGkeManager introduces a lot of concurrency, because of that some conventions were introduced:
// - Most private (lowercased) methods are not locked. Most of them require locking from caller methods.
// - Locked private methods have "Locked" suffix.
// - Most public methods are synchronized but not all. Their names cannot be suffixed to match those from interfaces.
type asyncGkeManager struct {
	GkeManager
	extendedGkeManager           extendedGkeManager
	cache                        *GkeCache
	context                      context.Context
	taskQueue                    *taskqueue.TaskQueue
	nodePoolRegistrationObserver *asyncNodePoolRegistrationObserver
	processedNodePoolListener    processedNodePoolListener
	// mutex guards manager's internal state, guards all read/write access to fields below.
	mutex sync.Mutex
	// scheduledMigs a map of upcoming or terminating migs by mig id.
	scheduledMigs map[string]*GkeMig
	// upcomingNodePools a map of upcoming node pools indexed by node-pool name.
	upcomingNodePools map[string]*upcomingNodePool
	// uninitializedGkeMigs a map of migs reported by gke that have not yet been initialized.
	// Uses real ids (from GCE), different from ids used by upcoming migs.
	// Used to identify migs available in GCE that have not yet been initialized.
	uninitializedGkeMigs map[string]*GkeMig
	// terminatingNodePools node pools by node-pool name.
	terminatingNodePools map[string]*terminatingNodePool
	// maxConcurrentErrorRetries is the max number of retries for concurrent operations error.
	maxConcurrentErrorRetries int
	// initialBackoffDuration the initial time duration to start exponential back off from in case of errors.
	initialBackoffDuration time.Duration
	// bufferedCleanUps used to buffer node pool to clean up (delete) no space in the task queue
	bufferedCleanUps []string
}

// newAsyncGkeManager creates version of GkeManager that supports async operations.
func newAsyncGkeManager(
	ctx context.Context,
	extendedGkeManager extendedGkeManager,
	cache *GkeCache,
	cpMaxParallelOps int,
	cpMaxQueuedOps int,
	domainURL string,
) *asyncGkeManager {
	if cpMaxParallelOps < 1 {
		klog.Fatalf("expected cpMaxParallelOps > 0, got: %v", cpMaxParallelOps)
	}
	if cpMaxQueuedOps < 1 {
		klog.Fatalf("expected cpMaxQueuedOps > 0, got: %v", cpMaxQueuedOps)
	}
	klog.Infof("Initializing AsyncGkeManager with workers=%d and queue_size=%d", cpMaxParallelOps, cpMaxQueuedOps)
	taskQueue := taskqueue.NewTaskQueue(ctx, cpMaxParallelOps, cpMaxQueuedOps,
		[]taskqueue.TaskType{taskTypeNodePoolCleanUp, taskTypeNodePoolCreation, taskTypeNodePoolDeletion})
	nodePoolRegistrationObserver := newAsyncNodePoolRegistrationObserver(ctx, extendedGkeManager.GetCluster, domainURL)
	noopProcessedNodePoolListener := func(nodePoolName string) {}
	return &asyncGkeManager{
		GkeManager:                   extendedGkeManager,
		extendedGkeManager:           extendedGkeManager,
		cache:                        cache,
		context:                      ctx,
		taskQueue:                    taskQueue,
		nodePoolRegistrationObserver: nodePoolRegistrationObserver,
		processedNodePoolListener:    noopProcessedNodePoolListener,
		uninitializedGkeMigs:         make(map[string]*GkeMig),
		scheduledMigs:                make(map[string]*GkeMig),
		upcomingNodePools:            make(map[string]*upcomingNodePool),
		terminatingNodePools:         make(map[string]*terminatingNodePool),
		bufferedCleanUps:             make([]string, 0, cpMaxQueuedOps),
		maxConcurrentErrorRetries:    maxConcurrentErrorRetries,
		initialBackoffDuration:       defaultInitialBackoffDuration,
	}
}

// Refresh refreshes GkeManager state at the beginning of each scale-up loop.
// Drops terminating/upcoming node pools that were fully processed and replaces them with their real counterparts.
func (m *asyncGkeManager) Refresh() error {
	m.trySchedulingNodePoolCleanUps()
	dropped := m.dropFinalizedNodePoolsLocked()
	if dropped {
		start := time.Now()
		klog.Infof("Fully refreshing cluster state after finalized async node pool operation")
		if err := m.extendedGkeManager.RefreshForce(); err != nil {
			klog.Warningf("Failed fully refreshing cluster state: %v", err)
			return err
		}
		klog.Infof("Fully refreshed cluster state after finalized async node pool operation [time: %s]", time.Since(start))
	} else {
		if err := m.GkeManager.Refresh(); err != nil {
			return err
		}
	}
	m.measureOngoingOperationsSynced()
	return nil
}

func (m *asyncGkeManager) trySchedulingNodePoolCleanUps() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if len(m.bufferedCleanUps) == 0 {
		return
	}
	for len(m.bufferedCleanUps) > 0 {
		nodePool := m.bufferedCleanUps[0]
		m.bufferedCleanUps = m.bufferedCleanUps[1:]
		terminating := m.terminatingNodePools[nodePool]
		if terminating == nil {
			klog.Warningf("Could not find terminating node pool %s for a buffered clean-up. Skipping...", nodePool)
			continue
		}
		err := m.taskQueue.Schedule(m.nodePoolCleanUpTask(terminating))
		if err != nil {
			klog.Warningf("Could not schedule buffered clean-up for node pool %s: %v", nodePool, err)
			if taskqueue.IsTaskQueueErr(err, taskqueue.QueueIsFullErr) {
				m.bufferedCleanUps = append(m.bufferedCleanUps, nodePool)
				return
			}
		} else {
			klog.Infof("Scheduled buffered clean-up for node pool %s", nodePool)
		}
	}
}

// dropFinalizedNodePoolsLocked drops in-mem representants of processed upcoming/terminating node pools.
func (m *asyncGkeManager) dropFinalizedNodePoolsLocked() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	dropped := false
	for nodePoolName, upcoming := range m.upcomingNodePools {
		if upcoming.canBeDropped() {
			klog.Infof("Async node pool creation process finished %s. Dropping upcoming migs during refresh.", nodePoolName)
			m.dropNodePoolReferences(nodePoolName)
			dropped = true
		}
	}
	for nodePoolName, terminating := range m.terminatingNodePools {
		if status, _ := terminating.status(); status == nodePoolTerminationStatusProcessed {
			klog.Infof("Async node pool deletion process finished %s (migs: %v). Dropping terminating migs during refresh.", nodePoolName, nodeGroupIds(terminating.migs))
			m.dropNodePoolReferences(nodePoolName)
			dropped = true
		}
	}
	return dropped
}

// IsUpcoming checks if mig belongs to a node pool being asynchronously created.
func (m *asyncGkeManager) IsUpcoming(mig *GkeMig) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if _, found := m.upcomingNodePools[mig.NodePoolName()]; found {
		return true
	}
	_, found := m.uninitializedGkeMigs[mig.Id()]
	return found
}

// CreateNodePoolAsync asynchronously creates a node pool.
func (m *asyncGkeManager) CreateNodePoolAsync(mig *GkeMig, updater interfaces.AsyncNodeGroupUpdater, initializer interfaces.AsyncNodeGroupInitializer) (MigCreateNodePoolResult, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	start := time.Now()
	if err := m.validateNodePoolToCreate(mig); err != nil {
		return MigCreateNodePoolResult{}, fmt.Errorf("could not schedule node pool creation, invalid mig: %v", err)
	}
	upcoming, err := m.createUpcomingNodePool(mig, updater, initializer)
	if err != nil {
		return MigCreateNodePoolResult{}, fmt.Errorf("could not construct upcoming node pool: %v", err)
	}
	err = m.taskQueue.Schedule(taskqueue.Task{
		Id:       m.taskIdForNodePoolCreation(upcoming.nodePoolName()),
		TaskType: taskTypeNodePoolCreation,
		Action:   func() error { return m.nodePoolCreationAsyncAction(upcoming) },
	})
	if err != nil {
		upcoming.setError(err)
		klog.Warningf("Could not schedule node pool creation %s [time: %s] (triggeringMig: %s, upcomingMigs: %s): %v", upcoming.nodePoolName(), time.Since(start), mig.Id(), upcoming.migIds(), err)
		m.dropNodePoolReferences(upcoming.nodePoolName())
		return MigCreateNodePoolResult{}, err
	}
	klog.Infof("Scheduled node pool creation %s [time: %s] (triggeringMig: %s, upcomingMigs: %s)", upcoming.nodePoolName(), time.Since(start), mig.Id(), upcoming.migIds())
	return MigCreateNodePoolResult{MainCreatedMig: upcoming.mainMig, ExtraCreatedMigs: upcoming.extraMigs()}, nil
}

func (m *asyncGkeManager) changeTerminatingStatusSynced(terminating *terminatingNodePool, status nodePoolTerminationStatus) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	terminating.setStatus(status)
	m.measureOngoingOperations()
}

func (m *asyncGkeManager) changeUpcomingStatusSynced(upcoming *upcomingNodePool, status nodePoolCreationStatus) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	upcoming.setStatus(status)
	m.measureOngoingOperations()
}

func (m *asyncGkeManager) measureOngoingOperationsSynced() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.measureOngoingOperations()
}

func (m *asyncGkeManager) measureOngoingOperations() {
	upcomingCounts := make(map[nodePoolCreationStatus]int)
	deleteCounts := make(map[nodePoolTerminationStatus]int)
	cleanupCounts := make(map[nodePoolTerminationStatus]int)
	for _, status := range nodePoolCreationStatuses {
		upcomingCounts[status] = 0
	}
	for _, status := range nodePoolTerminationStatuses {
		deleteCounts[status] = 0
		cleanupCounts[status] = 0
	}

	for _, upcoming := range m.upcomingNodePools {
		status, _ := upcoming.status()
		upcomingCounts[status]++
	}
	for _, terminating := range m.terminatingNodePools {
		status, _ := terminating.status()
		if terminating.cleanUp {
			cleanupCounts[status]++
		} else {
			deleteCounts[status]++
		}
	}
	for status, count := range upcomingCounts {
		metrics.UpdateNodePoolOngoingOperations(metrics.CreateNodePool, string(status), count)
	}
	for status, count := range deleteCounts {
		metrics.UpdateNodePoolOngoingOperations(metrics.DeleteNodePool, string(status), count)
	}
	for status, count := range cleanupCounts {
		metrics.UpdateNodePoolOngoingOperations(metrics.CleanupNodePool, string(status), count)
	}
}

// createUpcomingNodePool creates upcoming node pool as a part of the async node-pool creation process.
func (m *asyncGkeManager) createUpcomingNodePool(
	mig *GkeMig,
	updater interfaces.AsyncNodeGroupUpdater,
	initializer interfaces.AsyncNodeGroupInitializer,
) (*upcomingNodePool, error) {
	nodePoolSpec, err := m.extendedGkeManager.NewNodePoolSpec(mig)
	if err != nil {
		return nil, fmt.Errorf("could not construct node pool spec for a new node-pool. Skipping async node-pool creation for: %s (triggeringMig: %s), error: %v", mig.NodePoolName(), mig.Id(), err)
	}
	if len(nodePoolSpec.Locations) == 0 {
		return nil, fmt.Errorf("no locations found for a new node pool. Skipping async node-pool creation for: %s (triggeringMig: %s)", mig.NodePoolName(), mig.Id())
	}
	// upcoming spec must be enriched with network details for RLA request
	// clone by value
	upcomingSpec := *mig.Spec()
	upcomingSpec.ClusterNetworkPath = nodePoolSpec.ClusterNetworkPath
	upcomingSpec.ClusterSubnetworkPath = nodePoolSpec.ClusterSubnetworkPath
	var migs []*GkeMig
	var mainMig *GkeMig
	for i, location := range nodePoolSpec.Locations {
		// clone by value
		clone := *mig
		clone.spec = &upcomingSpec
		clone.gkeManager = m
		clone.gceRef = gce.GceRef{
			Project: mig.gceRef.Project,
			Name:    fmt.Sprintf("%s-async-%d", mig.gceRef.Name, i),
			Zone:    location,
		}
		clone.id = gce.GenerateMigUrl(clone.domainUrl, clone.gceRef)
		migs = append(migs, &clone)
		m.scheduledMigs[clone.Id()] = &clone
		if location == mig.GceRef().Zone {
			mainMig = &clone
		}
	}
	AddMigsToNodePool(mig.NodePoolName(), migs...)
	if mainMig == nil {
		klog.Errorf("Could not find main mig by zone for upcoming node pool. Original mig zone: %s", mig.GceRef().Zone)
		mainMig = migs[0]
	}
	now := time.Now()
	upcoming := &upcomingNodePool{
		mainMig:                    mainMig,
		migs:                       migs,
		injectedMig:                mig,
		updater:                    updater,
		initializer:                initializer,
		spec:                       nodePoolSpec,
		statusSynced:               nodePoolCreationStatusQueued,
		createdAt:                  now,
		lastStatusChangeSynced:     now,
		initializationStatusSynced: initializationIdle,
	}
	m.upcomingNodePools[mig.NodePoolName()] = upcoming
	m.measureOngoingOperations()
	return upcoming, nil
}

// nodePoolCreationAsyncAction creates the node pool as a part of the async node-pool creation process.
func (m *asyncGkeManager) nodePoolCreationAsyncAction(upcoming *upcomingNodePool) error {
	m.trySchedulingNodePoolCleanUps()
	klog.Infof("Starting node pool creation worker for %s [queueTime: %s]", upcoming.nodePoolName(), upcoming.sinceLastChange())
	m.changeUpcomingStatusSynced(upcoming, nodePoolCreationStatusProvisioning)
	var err error
	backoffDuration := m.initialBackoffDuration
	numberOfRetries := 0
	for {
		klog.Infof("Requesting GKE to create node pool %s [provTime: %s, retries: %d]", upcoming.nodePoolName(), upcoming.sinceLastChange(), numberOfRetries)
		// slow operation - calls GKE and waits for node pool creation to finish
		err = m.extendedGkeManager.CreateNodePoolNoRefresh(upcoming.nodePoolName(), upcoming.spec)
		if err == nil {
			klog.Infof("GKE created node pool %s [provTime: %s]", upcoming.nodePoolName(), upcoming.sinceLastChange())
			m.changeUpcomingStatusSynced(upcoming, nodePoolCreationStatusSyncing)
			go m.syncNodePool(upcoming)
			return nil
		}
		if !m.isRetryableCreationError(err, numberOfRetries) {
			break
		}
		klog.Warningf("GKE failed creating node pool %s, retrying after backoff %s [provTime: %s]: %v", upcoming.nodePoolName(), backoffDuration, upcoming.sinceLastChange(), err)
		time.Sleep(backoffDuration)
		backoffDuration = min(2*backoffDuration, maxBackoffDuration)
		numberOfRetries++
	}
	klog.Warningf("GKE failed creating node pool %s [provTime: %s, retries: %d]: %v", upcoming.nodePoolName(), upcoming.sinceLastChange(), numberOfRetries, err)
	upcoming.setError(err)
	go m.runNodePoolInitalizer(upcoming)
	return err
}

// isRetryableCreationCall returns true in case the passed creation error should be retried after the passed number of calls
func (m *asyncGkeManager) isRetryableCreationError(reqErr error, numberOfRetries int) bool {
	if isErrorOfAutoscalerErrorType(reqErr, gkeclient.GkeTooManyRequestsError) {
		return numberOfRetries < m.maxConcurrentErrorRetries
	}
	return false
}

// isGkeError returns true if the passed error is of the type of the passed autoscaler error type
func isErrorOfAutoscalerErrorType(err error, errorType errors.AutoscalerErrorType) bool {
	if err == nil {
		return false
	}
	autoscalerErr, ok := err.(errors.AutoscalerError)
	return ok && autoscalerErr.Type() == errorType
}

func (m *asyncGkeManager) syncNodePool(upcoming *upcomingNodePool) {
	nodePoolName := upcoming.nodePoolName()
	handleError := func(err error) {
		err = fmt.Errorf("node pool %s failed registering in the cluster: %v", nodePoolName, err)
		upcoming.setError(err)
		m.runNodePoolInitalizer(upcoming)
	}
	syncChannel, err := m.nodePoolRegistrationObserver.wait(m, upcoming.mainMig)
	if err != nil {
		handleError(fmt.Errorf("failed scheduling synchronization: %v", err))
		return
	}
	select {
	case <-time.After(nodePoolObserverTimeout):
		// mig creation timeout is appied in nodePoolRegistrationObserver
		// here we just double check the node pool syncing goroutine does not leak
		handleError(fmt.Errorf("node pool creation timeout during synchronization"))
		return
	case <-m.context.Done():
		handleError(fmt.Errorf("closed by context"))
		return
	case syncResult := <-syncChannel:
		if syncResult.err != nil {
			handleError(syncResult.err)
			return
		}
		creationResult := syncResult.creationResult
		if creationResult.MainCreatedMig != nil && creationResult.MainCreatedMig.Status() == NodePoolErrorStatus {
			handleError(fmt.Errorf("node pool has error status"))
			return
		}
		klog.Infof("Node pool %s registered in the cluster [time: %s] created migs: %v", nodePoolName, upcoming.sinceLastChange(), nodeGroupIds(creationResult.AllCreatedMigs()))
		upcoming.running(*creationResult)
		m.runNodePoolInitalizer(upcoming)
	}
}

func (m *asyncGkeManager) runNodePoolInitalizer(upcoming *upcomingNodePool) {
	if !upcoming.startInitialization() {
		klog.Infof("Initialization for node pool %s is already in progress, skipping...", upcoming.nodePoolName())
		return
	}
	defer func() {
		if upcoming.finishInitializationAndCheckIfReinitializationRequired() {
			klog.Infof("Re-triggering initialization for node pool %s due to concurrent scale-up", upcoming.nodePoolName())
			go m.runNodePoolInitalizer(upcoming)
		} else {
			m.processedNodePoolListener(upcoming.nodePoolName())
		}
	}()

	creationResult, err := upcoming.creationResult()
	status, _ := upcoming.status()
	if err != nil {
		m.addUninitializedMigs(creationResult.AllCreatedMigs())
	}
	for _, mig := range creationResult.AllCreatedMigs() {
		// It is also necessary to mark mig as injected for backoff
		m.GkeManager.SetInjectedMig(mig, upcoming.injectedMig)
		// Newly created mig must be registered in the cache so there are no vm duplication errors
		// on scale-ups triggered after async mig intiailization and before the refresh.
		m.cache.RegisterMig(mig)
	}
	result := interfaces.AsyncCreateNodePoolResult{
		InjectedMigs: convertMigsToAutoprovisioning(upcoming.migs),
		Error:        err,
	}
	if err == nil {
		result.CreatedToUpcomingMapping = m.translateCreatedToUpcomingMigs(upcoming)
		result.CreationResult = convertMigCreateNodePoolResult(creationResult)
	}
	start := time.Now()
	klog.Infof("Initializing node pool %s", upcoming.nodePoolName())
	upcoming.initializer.InitializeNodeGroup(result)
	klog.Infof("Initialized node pool %s [time: %s]", upcoming.nodePoolName(), time.Since(start))
	if err != nil && (status != nodePoolCreationStatusProvisioning || isErrorOfAutoscalerErrorType(err, gkeclient.GkePersistentOperationError)) {
		m.cleanUpNodePoolAsync(upcoming.mainMig)
	} else {
		m.changeUpcomingStatusSynced(upcoming, nodePoolCreationStatusProcessed)
	}
}

func (m *asyncGkeManager) addUninitializedMigs(migs []*GkeMig) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, mig := range migs {
		m.uninitializedGkeMigs[mig.Id()] = mig
	}
}

func (m *asyncGkeManager) runNodePoolFinalizer(terminating *terminatingNodePool) {
	_, err := terminating.status()
	result := interfaces.AsyncDeleteNodePoolResult{
		Migs:  convertMigsToAutoprovisioning(terminating.migs),
		Error: err,
	}
	start := time.Now()
	klog.Infof("Finalizing node pool deletion %s", terminating.nodePoolName)
	terminating.finalizer.FinalizeNodeGroup(result)
	klog.Infof("Finalized node pool deletion %s [time: %s]", terminating.nodePoolName, time.Since(start))
	m.changeTerminatingStatusSynced(terminating, nodePoolTerminationStatusProcessed)
	m.processedNodePoolListener(terminating.nodePoolName)
}

// DeleteNodePool deletes a node pool.
func (m *asyncGkeManager) DeleteNodePool(mig *GkeMig) error {
	if err := m.validateNodePoolToDeleteLocked(mig); err != nil {
		return fmt.Errorf("could not schedule node pool deletion: %v", err)
	}
	if upcoming := m.deleteQueuedUpcomingNodePoolLocked(mig.NodePoolName(), nil); upcoming != nil {
		return nil
	}
	return m.GkeManager.DeleteNodePool(mig)
}

// DeleteNodePoolAsync asynchronously deletes a node pool or drops its upcoming representant.
func (m *asyncGkeManager) DeleteNodePoolAsync(mig *GkeMig, finalizer interfaces.AsyncNodeGroupFinalizer) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	start := time.Now()
	nodePoolName := mig.NodePoolName()
	if err := m.validateNodePoolToDelete(mig); err != nil {
		return fmt.Errorf("could not schedule node pool deletion: %v", err)
	}
	if upcoming := m.deleteQueuedUpcomingNodePool(nodePoolName, finalizer); upcoming != nil {
		return nil
	}
	terminating := m.createTerminatingNodePool(mig, finalizer, false)
	err := m.taskQueue.Schedule(taskqueue.Task{
		Id:       fmt.Sprintf("%s_%s", taskTypeNodePoolDeletion, nodePoolName),
		TaskType: taskTypeNodePoolDeletion,
		Action:   func() error { return m.nodePoolDeletionAsyncAction(terminating, mig) },
	})
	if err != nil {
		terminating.setError(err)
		klog.Infof("Could not schedule node pool deletion %s [time: %s] (triggeringMig: %s, migs: %s): %v", nodePoolName, time.Since(start), mig.Id(),
			terminating.migIds(), err)
		m.dropNodePoolReferences(nodePoolName)
		return err
	}
	klog.Infof("Scheduled node pool deletion %s [time: %s] (triggeringMig: %s, migs: %s)", nodePoolName, time.Since(start), mig.Id(), terminating.migIds())
	return nil
}

func (m *asyncGkeManager) cleanUpNodePoolAsync(mig *GkeMig) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	start := time.Now()
	nodePoolName := mig.NodePoolName()
	m.dropNodePoolReferences(nodePoolName)
	terminating := m.createTerminatingNodePool(mig, nil, true)
	err := m.taskQueue.Schedule(m.nodePoolCleanUpTask(terminating))
	if taskqueue.IsTaskQueueErr(err, taskqueue.QueueIsFullErr) {
		if len(m.bufferedCleanUps) < cap(m.bufferedCleanUps) {
			m.bufferedCleanUps = append(m.bufferedCleanUps, nodePoolName)
			klog.Warningf("Could not schedule node pool deletion (clean-up) %s [time: %s] (triggeringMig: %s, migs: %s) beacuase task queue is full. Putting clean-up in the clean-up buffer.", nodePoolName, time.Since(start), mig.Id(),
				terminating.migIds())
		} else {
			klog.Warningf("Could not schedule node pool deletion (clean-up) %s [time: %s] (triggeringMig: %s, migs: %s) beacuase task queue is full and clean up buffer is full. Dropping clean-up operation.", nodePoolName, time.Since(start), mig.Id(),
				terminating.migIds())
			m.dropNodePoolReferences(nodePoolName)
		}
		return
	}
	if err != nil {
		klog.Warningf("Could not schedule node pool deletion (clean-up) %s [time: %s] (triggeringMig: %s, migs: %s): %v", nodePoolName, time.Since(start), mig.Id(),
			terminating.migIds(), err)
		m.dropNodePoolReferences(nodePoolName)
		return
	}
	klog.Infof("Scheduled node pool deletion (clean-up) %s [time: %s] (triggeringMig: %s, migs: %s)", nodePoolName, time.Since(start), mig.Id(), terminating.migIds())
}

func (m *asyncGkeManager) nodePoolCleanUpTask(terminating *terminatingNodePool) taskqueue.Task {
	return taskqueue.Task{
		Id:       fmt.Sprintf("%s_%s", taskTypeNodePoolCleanUp, terminating.nodePoolName),
		TaskType: taskTypeNodePoolCleanUp,
		Action:   func() error { return m.nodePoolCleanUpAsyncAction(terminating) },
	}
}

// nodePoolCleanUpAsyncAction deletes the node pool as a part of the async node pool clean up process.
func (m *asyncGkeManager) nodePoolCleanUpAsyncAction(terminating *terminatingNodePool) error {
	m.trySchedulingNodePoolCleanUps()
	klog.Infof("Requesting GKE to delete (clean-up) node pool %s [queueTime: %s] (migs: %s)", terminating.nodePoolName, terminating.sinceLastChange(), terminating.migIds())
	m.changeTerminatingStatusSynced(terminating, nodePoolTerminationStatusTerminating)
	// slow operation - calls GKE and waits for node pool removal operation to finish
	reqErr := m.extendedGkeManager.DeleteNodePoolNoRefresh(terminating.mainMig())
	if reqErr != nil {
		klog.Infof("Failed to delete (clean-up) node pool: %s [time: %s]. It's possible that node pool was not fully created: %v", terminating.nodePoolName, terminating.sinceLastChange(), reqErr)
		terminating.setError(reqErr)
	} else {
		klog.Infof("GKE deleted (clean-up) node pool %s [time: %s] (migs: %v)", terminating.nodePoolName, terminating.sinceLastChange(), terminating.migIds())
		m.changeTerminatingStatusSynced(terminating, nodePoolTerminationStatusTerminated)
	}
	// there is no need to run node pool finalizer for a clean-up
	m.changeTerminatingStatusSynced(terminating, nodePoolTerminationStatusProcessed)
	m.processedNodePoolListener(terminating.nodePoolName)
	return reqErr
}

func (m *asyncGkeManager) deleteQueuedUpcomingNodePoolLocked(nodePoolName string, finalizer interfaces.AsyncNodeGroupFinalizer) *upcomingNodePool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.deleteQueuedUpcomingNodePool(nodePoolName, finalizer)
}

// deleteQueuedUpcomingNodePool deletes queued upcoming node pool and executes its initializer/finalizer.
func (m *asyncGkeManager) deleteQueuedUpcomingNodePool(nodePoolName string, finalizer interfaces.AsyncNodeGroupFinalizer) *upcomingNodePool {
	// only fully created or queued node pools can be deleted
	taskDropped := m.taskQueue.Drop(m.taskIdForNodePoolCreation(nodePoolName))
	if upcoming, found := m.upcomingNodePools[nodePoolName]; taskDropped && found {
		if status, _ := upcoming.status(); status == nodePoolCreationStatusQueued {
			m.dropNodePoolReferences(nodePoolName)
			upcoming.setError(fmt.Errorf("node pool has been deleted when scheduled for creation"))
			go m.runNodePoolInitalizer(upcoming)
			if finalizer != nil {
				now := time.Now()
				terminating := &terminatingNodePool{
					nodePoolName:           nodePoolName,
					migs:                   upcoming.migs,
					finalizer:              finalizer,
					createdAt:              now,
					statusSynced:           nodePoolTerminationStatusTerminated,
					lastStatusChangeSynced: now,
				}
				go m.runNodePoolFinalizer(terminating)
			}
			klog.Infof("Removed upcoming node pool scheduled for creation %s (migs: %v)", nodePoolName, nodeGroupIds(upcoming.migs))
			return upcoming
		}
	}
	return nil
}

// createTerminatingNodePool creates terminating node pool. Part of the async deletion preocess.
func (m *asyncGkeManager) createTerminatingNodePool(mig *GkeMig, finalizer interfaces.AsyncNodeGroupFinalizer, cleanUp bool) *terminatingNodePool {
	var migs []*GkeMig
	migs = append(migs, mig)
	m.scheduledMigs[mig.Id()] = mig

	if np := mig.NodePool(); np != nil {
		for _, gkeMig := range np.Migs() {
			if gkeMig.Id() != mig.Id() {
				migs = append(migs, gkeMig)
				m.scheduledMigs[gkeMig.Id()] = gkeMig
			}
		}
	}

	now := time.Now()
	terminating := &terminatingNodePool{
		migs:                   migs,
		nodePoolName:           mig.NodePoolName(),
		finalizer:              finalizer,
		statusSynced:           nodePoolTerminationStatusQueued,
		createdAt:              now,
		lastStatusChangeSynced: now,
		cleanUp:                cleanUp,
	}
	m.terminatingNodePools[mig.NodePoolName()] = terminating
	m.measureOngoingOperations()
	return terminating
}

// nodePoolDeletionAsyncAction deletes the node pool as a part of the async node pool deletion process.
func (m *asyncGkeManager) nodePoolDeletionAsyncAction(terminating *terminatingNodePool, mig *GkeMig) error {
	m.trySchedulingNodePoolCleanUps()
	klog.Infof("Requesting GKE to delete node pool %s [queueTime: %s] (migs: %s)", terminating.nodePoolName, terminating.sinceLastChange(), terminating.migIds())
	m.changeTerminatingStatusSynced(terminating, nodePoolTerminationStatusTerminating)
	// slow operation - calls GKE and waits for node pool removal operation to finish
	reqErr := m.extendedGkeManager.DeleteNodePoolNoRefresh(mig)
	if reqErr != nil {
		klog.Warningf("GKE failed to delete node pool %s [time: %s] (migs: %s): %v", terminating.nodePoolName, terminating.sinceLastChange(), terminating.migIds(), reqErr)
		terminating.setError(reqErr)
	} else {
		klog.Infof("GKE deleted node pool %s [time: %s] (migs: %v)", terminating.nodePoolName, terminating.sinceLastChange(), terminating.migIds())
		m.changeTerminatingStatusSynced(terminating, nodePoolTerminationStatusTerminated)
	}
	go m.runNodePoolFinalizer(terminating)
	return reqErr
}

// taskIdForNodePoolCreation creates task id that represents node pool creation.
func (m *asyncGkeManager) taskIdForNodePoolCreation(nodePoolName string) string {
	return fmt.Sprintf("%s_%s", taskTypeNodePoolCreation, nodePoolName)
}

func (m *asyncGkeManager) validateNodePoolToCreate(mig *GkeMig) error {
	nodePoolName := mig.NodePoolName()
	if _, found := m.terminatingNodePools[nodePoolName]; found {
		return fmt.Errorf("node pool is being removed: %s (mig: %s)", nodePoolName, mig.Id())
	}
	if _, found := m.upcomingNodePools[nodePoolName]; found {
		return fmt.Errorf("node pool is being created: %s (mig: %s)", nodePoolName, mig.Id())
	}
	return nil
}

func (m *asyncGkeManager) validateNodePoolToDeleteLocked(mig *GkeMig) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.validateNodePoolToDelete(mig)
}

func (m *asyncGkeManager) validateNodePoolToDelete(mig *GkeMig) error {
	nodePoolName := mig.NodePoolName()
	if _, found := m.terminatingNodePools[nodePoolName]; found {
		return fmt.Errorf("node pool is being removed: %s (mig: %s)", nodePoolName, mig.Id())
	}
	if upcoming, found := m.upcomingNodePools[nodePoolName]; found {
		status, err := upcoming.status()
		if err == nil && status != nodePoolCreationStatusQueued {
			return fmt.Errorf("node pool is being created: %s (mig: %s)", nodePoolName, mig.Id())
		}
	}
	return nil
}

// GetGkeMigsBlockedByServerError returns a list of irretrievable migs blocked by server error (5xx).
func (m *asyncGkeManager) GetGkeMigsBlockedByServerError() []*GkeMig {
	migs := m.GkeManager.GetGkeMigsBlockedByServerError()
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.toFilteredAsyncMigs(migs)
}

// GetGkeMigsBlockedByNotFoundError returns a list of irretrievable migs blocked by not found error (404).
func (m *asyncGkeManager) GetGkeMigsBlockedByNotFoundError() []*GkeMig {
	migs := m.GkeManager.GetGkeMigsBlockedByNotFoundError()
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.toFilteredAsyncMigs(migs)
}

// GetGkeMigs returns created and upcoming migs
func (m *asyncGkeManager) GetGkeMigs() []*GkeMig {
	createdMigs := m.GkeManager.GetGkeMigs()
	m.mutex.Lock()
	defer m.mutex.Unlock()
	result := m.toFilteredAsyncMigs(createdMigs)
	for _, upcomingMig := range m.upcomingNodePools {
		result = append(result, upcomingMig.migs...)
	}
	return result
}

// GetAllNodePoolNames returns created and upcoming node pool names.
func (m *asyncGkeManager) GetAllNodePoolNames() sets.Set[string] {
	names := m.GkeManager.GetAllNodePoolNames().Clone()
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for name := range m.terminatingNodePools {
		names.Delete(name)
	}
	for name := range m.upcomingNodePools {
		names.Insert(name)
	}
	return names
}

// GetMigSize returns real mig size or its upcoming size that will be used during initial scale up after async creation.
func (m *asyncGkeManager) GetMigSize(mig gce.Mig) (int64, error) {
	if upcoming := m.getUpcomingNodePoolByMigIdLocked(mig.Id()); upcoming != nil {
		return upcoming.updater.GetTargetSize(mig.Id()), nil
	}
	return m.GkeManager.GetMigSize(mig)
}

// SetMigSize sets mig size or the size of initial scale up after async node pool creation.
func (m *asyncGkeManager) SetMigSize(mig gce.Mig, size int64) error {
	upcoming := m.getUpcomingNodePoolByMigIdLocked(mig.Id())
	if upcoming == nil {
		return m.GkeManager.SetMigSize(mig, size)
	}
	previousSize := upcoming.updater.GetTargetSize(mig.Id())
	upcoming.updater.SetTargetSize(mig.Id(), size)
	klog.Infof("Setting upcoming mig size %s to %d -> %d", mig.Id(), previousSize, size)
	if createdMig := upcoming.getCreatedMigByZone(mig.GceRef().Zone); createdMig != nil {
		klog.Infof("Setting mig size to %d instances on async created mig %v (upcomingMig: %v)", size, createdMig.Id(), mig.Id())
		return m.GkeManager.SetMigSize(createdMig, size)
	}
	return nil
}

// GetMigNodes return mig nodes or nil for upcoming node pools.
func (m *asyncGkeManager) GetMigNodes(mig gce.Mig) ([]gce.GceInstance, error) {
	if scheduledMig := m.getScheduledMigLocked(mig.Id()); scheduledMig != nil {
		return nil, nil
	}
	return m.GkeManager.GetMigNodes(mig)
}

// getScheduledMigLocked returns mig belonging to upcoming or terminating node pool or nil.
func (m *asyncGkeManager) getScheduledMigLocked(migId string) *GkeMig {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.scheduledMigs[migId]
}

// GetMigForInstance returns mig the instance is part of.
func (m *asyncGkeManager) GetMigForInstance(instance gce.GceRef) (gce.Mig, error) {
	if mig := m.getUpcomingMigForInstanceLocked(instance); mig != nil {
		return mig, nil
	}
	// gkeManager will return also uninitialized and terminating migs
	mig, err := m.GkeManager.GetMigForInstance(instance)
	if err != nil || mig == nil || reflect.ValueOf(mig).IsNil() {
		return mig, err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	unitialized := m.uninitializedGkeMigs[mig.Id()]
	if unitialized != nil {
		return unitialized, nil
	}
	if mig, found := m.scheduledMigs[mig.Id()]; found {
		if _, found := m.terminatingNodePools[mig.NodePoolName()]; found {
			// terminating node pools should not be accessible
			return nil, nil
		}
	}
	return mig, nil
}

func (m *asyncGkeManager) getUpcomingMigForInstanceLocked(instance gce.GceRef) gce.Mig {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, np := range m.upcomingNodePools {
		for _, mig := range np.migs {
			migRef := mig.GceRef()
			if instance.Zone == migRef.Zone && instance.Project == migRef.Project && strings.HasPrefix(instance.Name, migRef.Name) {
				return mig
			}
		}
	}
	return nil
}

func (m *asyncGkeManager) CreateInstances(mig gce.Mig, delta int64) error {
	if upcoming := m.getUpcomingNodePoolByMigIdLocked(mig.Id()); upcoming != nil {
		klog.Infof("Scheduling addition of %d new instances to upcoming mig %v", delta, mig.Id())
		if createdMig := upcoming.getCreatedMigByZone(mig.GceRef().Zone); createdMig != nil {
			klog.Infof("Adding %d new instances in parallel to async created mig %v (upcomingMig: %v)", delta, createdMig.Id(), mig.Id())
			// Invalidate cache for mig as it doesn't contain VM names that were added in the initial async scale-up.
			// This invalidation happens at most once per newly created mig in the loop following mig async creation.
			m.cache.InvalidateMigInstances(createdMig.GceRef())
			upcoming.updater.ChangeTargetSize(mig.Id(), delta)
			upcoming.markInProgressDirty()
			m.runNodePoolInitalizer(upcoming)
		} else {
			upcoming.updater.ChangeTargetSize(mig.Id(), delta)
		}
		return nil
	}
	return m.GkeManager.CreateInstances(mig, delta)
}

func (m *asyncGkeManager) CreateQueuedInstances(pr prpods.ProvReqID, mig *GkeMig, delta int64, shouldUpdateProvReqDetails manager.ShouldUpdateProvReqDetails) error {
	upcoming := m.getUpcomingNodePoolByMigIdLocked(mig.Id())
	if upcoming == nil {
		return m.GkeManager.CreateQueuedInstances(pr, mig, delta, shouldUpdateProvReqDetails)
	}
	klog.Infof("Scheduling PR %s/%s resize to increase mig %v by %d", pr.Namespace, pr.Name, mig.Id(), delta)
	dwsUpdater, ok := upcoming.updater.(asyncDwsUpdater)
	if !ok {
		return fmt.Errorf("async initializer for mig %v doesn't support DWS", mig.Id())
	}
	dwsUpdater.ScheduleProvReqResize(pr, mig.Id(), delta, shouldUpdateProvReqDetails)
	if createdMig := upcoming.getCreatedMigByZone(mig.GceRef().Zone); createdMig != nil {
		klog.Infof("Scheduling PR %s/%s resize to increase mig %v by %d", pr.Namespace, pr.Name, createdMig.Id(), delta)
		return m.GkeManager.CreateQueuedInstances(pr, createdMig, delta, shouldUpdateProvReqDetails)
	}
	return nil
}

func (m *asyncGkeManager) CreateFlexResizeRequests(mig gce.Mig, delta int64) error {
	upcoming := m.getUpcomingNodePoolByMigIdLocked(mig.Id())
	if upcoming == nil {
		return m.GkeManager.CreateFlexResizeRequests(mig, delta)
	}
	upcoming.updater.ChangeTargetSize(mig.Id(), delta)
	if createdMig := upcoming.getCreatedMigByZone(mig.GceRef().Zone); createdMig != nil {
		klog.Infof("Creating %d resize requests for async created mig %v (upcomingMig: %v)", delta, createdMig.Id(), mig.Id())
		return m.GkeManager.CreateFlexResizeRequests(createdMig, delta)
	}
	return nil
}

func (m *asyncGkeManager) CreateResizeRequest(mig gce.Mig, delta int64) error {
	upcoming := m.getUpcomingNodePoolByMigIdLocked(mig.Id())
	if upcoming == nil {
		return m.GkeManager.CreateResizeRequest(mig, delta)
	}
	klog.Infof("Scheduling atomic addition of %d new instances to upcoming mig %v", delta, mig.Id())
	upcoming.updater.ChangeTargetSize(mig.Id(), delta)
	if createdMig := upcoming.getCreatedMigByZone(mig.GceRef().Zone); createdMig != nil {
		klog.Infof("Atomically adding %d new instances to async created mig %v (upcomingMig: %v)", delta, createdMig.Id(), mig.Id())
		return m.GkeManager.CreateResizeRequest(createdMig, delta)
	}
	return nil
}

func (m *asyncGkeManager) AdvanceResizeRequestCleanUp(resizeRequest resizerequestclient.ResizeRequestStatus) error {
	ref := gce.GceRef{
		Zone: resizeRequest.Zone,
		Name: resizeRequest.MigName,
	}
	if upcomingNodePool, _ := m.getUpcomingNodePoolByGceRefLocked(ref); upcomingNodePool == nil {
		return m.GkeManager.AdvanceResizeRequestCleanUp(resizeRequest)
	}
	return fmt.Errorf("Internal error: deleting a resize request from an upcoming node pool is not supported")
}

func (m *asyncGkeManager) toFilteredAsyncMigs(migs []*GkeMig) []*GkeMig {
	var result []*GkeMig
	for _, mig := range migs {
		if _, found := m.upcomingNodePools[mig.NodePoolName()]; found {
			continue
		}
		if _, found := m.terminatingNodePools[mig.NodePoolName()]; found {
			continue
		}
		result = append(result, m.toAsyncMig(mig))
	}
	return result
}

func (m *asyncGkeManager) toAsyncMig(mig *GkeMig) *GkeMig {
	var clone = *mig
	clone.gkeManager = m
	return &clone
}

func (m *asyncGkeManager) getUpcomingNodePoolByGceRefLocked(ref gce.GceRef) (*upcomingNodePool, *GkeMig) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, mig := range m.scheduledMigs {
		upcomingRef := mig.GceRef()
		if len(ref.Project) > 0 && ref.Project != upcomingRef.Project {
			continue
		}
		if upcomingRef.Zone == ref.Zone && upcomingRef.Name == ref.Name {
			if upcomingNodePool, found := m.upcomingNodePools[mig.NodePoolName()]; found {
				return upcomingNodePool, mig
			}
		}
	}
	return nil, nil
}

func (m *asyncGkeManager) getUpcomingNodePoolByMigIdLocked(migId string) *upcomingNodePool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	scheduledMig := m.scheduledMigs[migId]
	if scheduledMig == nil {
		return nil
	}
	return m.upcomingNodePools[scheduledMig.NodePoolName()]
}

// dropNodePoolReferences drops all traces of the node pool.
func (m *asyncGkeManager) dropNodePoolReferences(nodePoolName string) {
	m.nodePoolRegistrationObserver.stopWaitingFor(nodePoolName)
	if upcoming, ok := m.upcomingNodePools[nodePoolName]; ok {
		upcoming.completed()
		delete(m.upcomingNodePools, nodePoolName)
		for _, mig := range upcoming.migs {
			delete(m.scheduledMigs, mig.Id())
		}
		creationResult, _ := upcoming.creationResult()
		for _, mig := range creationResult.AllCreatedMigs() {
			delete(m.uninitializedGkeMigs, mig.Id())
		}
	}
	if terminating, ok := m.terminatingNodePools[nodePoolName]; ok {
		terminating.completed()
		delete(m.terminatingNodePools, nodePoolName)
		for _, mig := range terminating.migs {
			delete(m.scheduledMigs, mig.Id())
		}
	}
	nodePoolsToCleanUp := make([]string, 0, cap(m.bufferedCleanUps))
	for _, nodePoolToCleanUp := range m.bufferedCleanUps {
		if nodePoolToCleanUp != nodePoolName {
			nodePoolsToCleanUp = append(nodePoolsToCleanUp, nodePoolToCleanUp)
		} else if terminating, ok := m.terminatingNodePools[nodePoolName]; ok {
			terminating.completed()
		}
	}
	m.bufferedCleanUps = nodePoolsToCleanUp
}

func (m *asyncGkeManager) translateCreatedToUpcomingMigs(upcoming *upcomingNodePool) map[string]string {
	translated := make(map[string]string)
	upcomingMigsByZone := make(map[string][]string)
	for _, mig := range upcoming.migs {
		migs := upcomingMigsByZone[mig.gceRef.Zone]
		migs = append(migs, mig.Id())
		upcomingMigsByZone[mig.gceRef.Zone] = migs
	}
	creationResult, _ := upcoming.creationResult()
	for _, mig := range creationResult.AllCreatedMigs() {
		migs := upcomingMigsByZone[mig.gceRef.Zone]
		if len(migs) == 0 {
			continue
		}
		translated[mig.Id()] = migs[0]
		upcomingMigsByZone[mig.gceRef.Zone] = migs[1:]
	}
	if len(upcoming.migs) != len(translated) {
		klog.Warningf("Could not map by location all upcomingMigs for created ones. Injected: %v, translated: %v", upcoming.migs, translated)
	}
	return translated
}

// SetRecommendation assigns a scaling recommendation to an instance group.
func (m *asyncGkeManager) SetRecommendation(migId string, rec ScaleUpRecommendation) {
	m.GkeManager.SetRecommendation(migId, rec)
}

// PopRecommendation returns and removes the scaling recommendation for an instance group.
func (m *asyncGkeManager) PopRecommendation(migId string) (rec ScaleUpRecommendation, ok bool) {
	return m.GkeManager.PopRecommendation(migId)
}

// ClearRecommendations clears all pending scaling recommendations.
func (m *asyncGkeManager) ClearRecommendations() {
	m.GkeManager.ClearRecommendations()
}

// upcomingNodePool represents a node pool that will be created in the near future.
// Access to fields with "Synced" suffix using methods - they require synchronization.
type upcomingNodePool struct {
	mainMig     *GkeMig
	migs        []*GkeMig
	injectedMig *GkeMig
	spec        *gkeclient.NodePoolSpec
	initializer interfaces.AsyncNodeGroupInitializer
	updater     interfaces.AsyncNodeGroupUpdater
	createdAt   time.Time
	// guards *Synced fields
	mutex sync.Mutex
	// statusSynced tracks the high-level creation lifecycle of the node pool (e.g., Scheduling, Provisioning, Running, Processed).
	statusSynced nodePoolCreationStatus
	// lastStatusChangeSynced records the timestamp of the last statusSynced transition, used for metrics.
	lastStatusChangeSynced time.Time
	// errSynced holds any terminal error encountered during the node pool creation or initialization process.
	errSynced error
	// creationResultSynced stores the newly created MIGs once the cloud provider confirms their creation.
	creationResultSynced MigCreateNodePoolResult
	// initializationStatusSynced tracks the state of the post-creation initialization process (Idle, Pending, InProgress, InProgressDirty).
	// It ensures that concurrent scale-ups trigger re-initialization if the target size changes during an ongoing initialization.
	initializationStatusSynced initializationStatus
	// reinitializationCounterSynced limits the number of times initialization can be re-triggered to prevent infinite loops.
	reinitializationCounterSynced int
}

func (n *upcomingNodePool) extraMigs() []*GkeMig {
	var result []*GkeMig
	for _, mig := range n.migs {
		if mig != n.mainMig {
			result = append(result, mig)
		}
	}
	return result
}

func (n *upcomingNodePool) running(creationResult MigCreateNodePoolResult) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.creationResultSynced = creationResult
	metrics.ObserveNodePoolOperationStatusDuration(metrics.CreateNodePool, string(n.statusSynced), time.Since(n.lastStatusChangeSynced))
	n.lastStatusChangeSynced = time.Now()
	n.statusSynced = nodePoolCreationStatusRunning
}

func (n *upcomingNodePool) status() (nodePoolCreationStatus, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	return n.statusSynced, n.errSynced
}

func (n *upcomingNodePool) setStatus(status nodePoolCreationStatus) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	metrics.ObserveNodePoolOperationStatusDuration(metrics.CreateNodePool, string(n.statusSynced), time.Since(n.lastStatusChangeSynced))
	n.lastStatusChangeSynced = time.Now()
	n.statusSynced = status
}

func (n *upcomingNodePool) nodePoolName() string {
	return n.mainMig.NodePoolName()
}

func (n *upcomingNodePool) startInitialization() bool {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if n.initializationStatusSynced == initializationInProgress || n.initializationStatusSynced == initializationInProgressDirty {
		return false
	}
	n.initializationStatusSynced = initializationInProgress
	return true
}

const maxReinitializationCount = 3

func (n *upcomingNodePool) finishInitializationAndCheckIfReinitializationRequired() bool {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if n.initializationStatusSynced == initializationInProgressDirty {
		if n.reinitializationCounterSynced < maxReinitializationCount {
			n.initializationStatusSynced = initializationPending
			n.reinitializationCounterSynced++
			return true
		}
		klog.Warningf("Max re-initialization count reached for node pool %s. Skipping further re-initializations.", n.nodePoolName())
	}
	n.initializationStatusSynced = initializationIdle
	return false
}

func (n *upcomingNodePool) markInProgressDirty() {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if n.initializationStatusSynced == initializationInProgress {
		n.initializationStatusSynced = initializationInProgressDirty
	}
}

func (n *upcomingNodePool) canBeDropped() bool {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	return n.statusSynced == nodePoolCreationStatusProcessed && n.initializationStatusSynced == initializationIdle
}

func (n *upcomingNodePool) migIds() []string {
	return nodeGroupIds(n.migs)
}

func (n *upcomingNodePool) sinceLastChange() time.Duration {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	return time.Since(n.lastStatusChangeSynced)
}

func (n *upcomingNodePool) setError(err error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.errSynced = err
	n.lastStatusChangeSynced = time.Now()
}

func (n *upcomingNodePool) creationResult() (MigCreateNodePoolResult, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	return n.creationResultSynced, n.errSynced
}

func (n *upcomingNodePool) getCreatedMigByZone(zone string) *GkeMig {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	for _, mig := range n.creationResultSynced.AllCreatedMigs() {
		if mig.GceRef().Zone == zone {
			return mig
		}
	}
	return nil
}

func (n *upcomingNodePool) completed() {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	result := metrics.Success
	if n.errSynced != nil {
		result = metrics.Failure
	}
	metrics.ObserveNodePoolOperationDuration(metrics.CreateNodePool, result, time.Since(n.createdAt))
	// measure time spent in the last status
	metrics.ObserveNodePoolOperationStatusDuration(metrics.CreateNodePool, string(n.statusSynced), time.Since(n.lastStatusChangeSynced))
}

// terminatingNodePool represents a node pool that will be deleted in the near future.
// Access to fields with "Synced" suffix using methods - they require synchronization.
type terminatingNodePool struct {
	mutex                  sync.Mutex
	nodePoolName           string
	cleanUp                bool
	migs                   []*GkeMig
	finalizer              interfaces.AsyncNodeGroupFinalizer
	createdAt              time.Time
	lastStatusChangeSynced time.Time
	statusSynced           nodePoolTerminationStatus
	errSynced              error
}

func (n *terminatingNodePool) mainMig() *GkeMig {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if len(n.migs) > 0 {
		return n.migs[0]
	}
	return nil
}

func (n *terminatingNodePool) status() (nodePoolTerminationStatus, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	return n.statusSynced, n.errSynced
}

func (n *terminatingNodePool) setStatus(status nodePoolTerminationStatus) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	metrics.ObserveNodePoolOperationStatusDuration(metrics.DeleteNodePool, string(n.statusSynced), time.Since(n.lastStatusChangeSynced))
	n.lastStatusChangeSynced = time.Now()
	n.statusSynced = status
}

func (n *terminatingNodePool) setError(err error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.errSynced = err
	n.lastStatusChangeSynced = time.Now()
}

func (n *terminatingNodePool) migIds() []string {
	return nodeGroupIds(n.migs)
}

func (n *terminatingNodePool) sinceLastChange() time.Duration {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	return time.Since(n.lastStatusChangeSynced)
}

func (n *terminatingNodePool) completed() {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	result := metrics.Success
	if n.errSynced != nil {
		result = metrics.Failure
	}
	operationType := metrics.DeleteNodePool
	if n.cleanUp {
		operationType = metrics.CleanupNodePool
	}
	metrics.ObserveNodePoolOperationDuration(operationType, result, time.Since(n.createdAt))
	// measure time spent in the last status
	metrics.ObserveNodePoolOperationStatusDuration(operationType, string(n.statusSynced), time.Since(n.lastStatusChangeSynced))
}

func nodeGroupIds(migs []*GkeMig) []string {
	var result []string
	for _, mig := range migs {
		result = append(result, mig.Id())
	}
	return result
}
