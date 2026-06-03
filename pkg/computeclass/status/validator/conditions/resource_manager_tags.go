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

package conditions

import (
	"context"
	"fmt"
	"strings"

	rmpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"golang.org/x/exp/slices"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/resourcemanager"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/klog/v2"
)

// resourceManagerTagsCheck provides observability for tag-related errors in CCC status.
// If the error is not severe enough to be worth reporting, it will return nil since
// this function is repeating redundant validation the Cluster Server is already doing.
type resourceManagerTagsCheck struct {
	tagClient resourcemanager.TagClient
	provider  CloudProvider
	cache     map[crd.CRD]cccData
}

func (ch *resourceManagerTagsCheck) checkCrd(c crd.CRD, migs map[string]*gke.GkeMig) *metav1.Condition {

	// Cannot use SelfServiceMetadata here. CCC spec tag list is translated into a string map before
	// being marshalled into a metadata string, de-duping any duplicate keys that need to be reported.
	tags := c.ResourceManagerTags()
	if len(tags) == 0 {
		return nil
	}

	if ch.tagClient == nil {
		// If tagClient is nil, error was likely already logged at validator creation
		klog.Errorf("tags could not be validated due to missing tag client")
		return nil
	}

	clusterNetwork, err := ch.provider.GetClusterNetwork()
	if err != nil || clusterNetwork == nil {
		klog.Errorf("tags could not be validated due to missing cluster network")
		return nil
	}

	tagKeysSeen := make(map[string]bool)
	if _, ok := ch.cache[c]; !ok {
		ch.cache[c] = newCccData()
	}
	for _, tag := range tags {

		// Either both key and value must use tag key/vale ID format or neither do.
		// tagKeys/{tag_key_id}=tagValues/{tag_value_id}
		hasKeyPrefix := strings.HasPrefix(tag.Key, resourcemanager.TagKeysPrefix)
		hasValuePrefix := strings.HasPrefix(tag.Value, resourcemanager.TagValuesPrefix)
		if hasKeyPrefix != hasValuePrefix {
			return ResourceManagerValidationCondition(fmt.Sprintf("invalid format for tag key %q and value %q. If key has prefix 'tagKeys/', value must have prefix 'tagValues/' (and vice-versa)", tag.Key, tag.Value))
		}

		// Check for dupe keys before checking cache since
		// a dupe key would be a guaranteed cache hit.
		if tagKeysSeen[tag.Key] {
			return ResourceManagerValidationCondition(fmt.Sprintf("duplicate tag keys specified: %s", tag.Key))
		}

		cachedVal, ok := ch.cache[c].tags[tag.Key]
		if ok && cachedVal == tag.Value {
			continue
		}

		rmTagKey, err := ch.tagClient.GetTagKey(context.Background(), tag.Key)
		if err != nil {
			return ResourceManagerValidationCondition(fmt.Sprintf("tag key %q not found", tag.Key))
		}

		purpose := rmTagKey.GetPurpose()
		purposeData := rmTagKey.GetPurposeData()
		name := rmTagKey.GetName()
		shortname := rmTagKey.GetShortName()

		// if the short name starts with gke-managed (reserved for internal use), reject the tag
		if strings.HasPrefix(shortname, "gke-managed-") {
			return ResourceManagerValidationCondition(fmt.Sprintf("tag key short name %q cannot start with 'gke-managed-'", shortname))
		}

		// if the purpose is GCE_FIREWALL, check for the correct network info
		if purpose == rmpb.Purpose_GCE_FIREWALL {
			network, hasNetwork := purposeData["network"]
			_, hasOrg := purposeData["organization"]

			if hasNetwork && network != clusterNetwork.SelfLinkWithId {
				return ResourceManagerValidationCondition(fmt.Sprintf("tag key %q is for network %q, but cluster is in network %q", name, network, clusterNetwork.SelfLinkWithId))
			}

			// let cluster server handle org-scoped tag validation
			if !hasOrg && !hasNetwork {
				return ResourceManagerValidationCondition(fmt.Sprintf("tag key %q has invalid purpose data %q, purpose data key must be network or organization", name, purposeData))
			}
		}

		var queryValue string
		if hasValuePrefix {
			queryValue = tag.Value
		} else {
			// query with value namespaced name
			queryValue = fmt.Sprintf("%s/%s", rmTagKey.GetNamespacedName(), tag.Value)
		}
		rmTagValue, err := ch.tagClient.GetTagValue(context.Background(), queryValue)
		if err != nil {
			return ResourceManagerValidationCondition(fmt.Sprintf("tag value %q not found", tag.Value))
		}

		resp, err := ch.tagClient.ValidateIamPermissions(context.Background(), rmTagValue.GetName())
		if err != nil {
			return ResourceManagerValidationCondition(fmt.Sprintf("permission denied on tag value %q", tag.Value))
		}

		// TestIamPermissionsResponse returns a subset of the requested permissions if not all are granted.
		perms := resp.GetPermissions()
		if len(perms) < len(resourcemanager.TagsRequiredIamPermissions) {
			var missingPermissions []string
			for _, p := range resourcemanager.TagsRequiredIamPermissions {
				if !slices.Contains(perms, p) {
					missingPermissions = append(missingPermissions, p)
				}
			}
			permErr := fmt.Sprintf("service account missing the following required permissions for tag value %q: %v", tag.Value, missingPermissions)
			return ResourceManagerValidationCondition(permErr)
		}

		tagKeysSeen[tag.Key] = true
		ch.cache[c].tags[tag.Key] = tag.Value

	}

	return nil
}

func (ch *resourceManagerTagsCheck) conditionType() string {
	return CrdMisconfiguredCondition
}
