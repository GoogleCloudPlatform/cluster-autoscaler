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
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"
)

type mockPlugin struct {
	nameFunc          func() string
	getAnnotationFunc func(node *apiv1.Node) (map[string]string, error)
}

func (m *mockPlugin) Name() string {
	if m.nameFunc != nil {
		return m.nameFunc()
	}
	return "MockPlugin"
}

func (m *mockPlugin) GetAnnotation(node *apiv1.Node) (map[string]string, error) {
	if m.getAnnotationFunc != nil {
		return m.getAnnotationFunc(node)
	}
	return nil, nil
}

func createNode(name string, initialAnnotations map[string]string) *apiv1.Node {
	return &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: initialAnnotations, ResourceVersion: "1"}}
}

// ReactionFunc is the type for reactor functions used with the fake client.
type ReactionFunc func(action k8stesting.Action, tracker k8stesting.ObjectTracker) (handled bool, ret runtime.Object, err error)

// noPatchReactor returns a reactor that fails the test if a patch action occurs for nodes.
func noPatchReactor(t *testing.T) ReactionFunc {
	return func(action k8stesting.Action, tracker k8stesting.ObjectTracker) (handled bool, ret runtime.Object, err error) {
		patchAction, ok := action.(k8stesting.PatchAction)
		// Only fail if it's specifically a patch on the nodes resource
		if ok && patchAction.GetResource().Resource == "nodes" {
			assert.Fail(t, "Patch reactor called unexpectedly for nodes")
			return true, nil, errors.New("unexpected patch call") // Return error to potentially halt caller
		}
		// Otherwise, don't handle this action
		return false, nil, nil
	}
}

// successfulPatchReactor returns a reactor that expects a successful patch for a specific node,
// verifies the payload contains exactly the expectedAnnotationsToPatch, simulates the patch
// by updating the tracker, and returns the updated node.
func successfulPatchReactor(t *testing.T, nodeName string, expectedAnnotationsToPatch map[string]string) ReactionFunc {
	return func(action k8stesting.Action, tracker k8stesting.ObjectTracker) (handled bool, ret runtime.Object, err error) {
		patchAction, ok := action.(k8stesting.PatchAction)
		if !ok || patchAction.GetResource().Resource != "nodes" || patchAction.GetPatchType() != types.MergePatchType || patchAction.GetName() != nodeName {
			return false, nil, nil // Not the action we are handling
		}

		// Verify patch payload precisely matches expected annotations
		expectedPayload := map[string]interface{}{
			"metadata": map[string]interface{}{
				"annotations": make(map[string]interface{}), // Ensure correct type for comparison
			},
		}
		// Populate expected payload with correct interface{} type values
		for k, v := range expectedAnnotationsToPatch {
			expectedPayload["metadata"].(map[string]interface{})["annotations"].(map[string]interface{})[k] = v
		}

		var actualPayload map[string]interface{}
		err = json.Unmarshal(patchAction.GetPatch(), &actualPayload)
		assert.NoError(t, err, "Reactor: Failed to unmarshal actual patch payload")
		if err != nil {
			return true, nil, err
		}
		assert.Equal(t, expectedPayload, actualPayload, "Reactor: Patch payload mismatch")

		// Apply patch manually to the object in the tracker
		gvr := action.GetResource()
		ns := action.GetNamespace()
		name := patchAction.GetName()
		currentObj, err := tracker.Get(gvr, ns, name)
		if err != nil {
			assert.FailNow(t, "Reactor: Failed to get object from tracker", err.Error())
			return true, nil, err
		}
		currentNode := currentObj.(*apiv1.Node).DeepCopy()

		patchData := make(map[string]json.RawMessage)
		if err := json.Unmarshal(patchAction.GetPatch(), &patchData); err != nil {
			assert.FailNow(t, "Reactor failed to unmarshal root patch", err.Error())
			return true, nil, err
		}
		if metaRaw, ok := patchData["metadata"]; ok {
			metaPatch := make(map[string]json.RawMessage)
			if err := json.Unmarshal(metaRaw, &metaPatch); err == nil {
				if annRaw, ok := metaPatch["annotations"]; ok {
					newAnnos := make(map[string]string)
					if err := json.Unmarshal(annRaw, &newAnnos); err == nil {
						if currentNode.Annotations == nil {
							currentNode.Annotations = make(map[string]string)
						}
						for k, v := range newAnnos {
							currentNode.Annotations[k] = v
						}
					} else {
						assert.FailNow(t, "Reactor failed to unmarshal annotations patch", err.Error())
						return true, nil, err
					}
				}
			} else {
				assert.FailNow(t, "Reactor failed to unmarshal metadata patch", err.Error())
				return true, nil, err
			}
		}

		currentNode.ResourceVersion = "2"
		err = tracker.Update(gvr, currentNode, ns)
		assert.NoError(t, err, "Reactor: Failed to update tracker")
		if err != nil {
			return true, nil, err
		}

		// Return the modified node as the result of the API call
		return true, currentNode, nil
	}
}

// failingPatchReactor returns a reactor that expects a patch for a specific node
// and simulates a failure by returning the provided error.
func failingPatchReactor(t *testing.T, nodeName string, errorToReturn error) ReactionFunc {
	return func(action k8stesting.Action, tracker k8stesting.ObjectTracker) (handled bool, ret runtime.Object, err error) {
		patchAction, ok := action.(k8stesting.PatchAction)
		if !ok || patchAction.GetResource().Resource != "nodes" || patchAction.GetName() != nodeName {
			return false, nil, nil // Not the action we are handling
		}
		// Return the predefined error
		return true, nil, errorToReturn
	}
}
