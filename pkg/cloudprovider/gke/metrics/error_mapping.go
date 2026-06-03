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

package metrics

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"google.golang.org/api/googleapi"
	"k8s.io/klog/v2"
)

// reasonWeights contains an integer for each of the important error reasons,
// the higher the number the more important the reason. Those outside of the
// map will get 0, values with the same weight will be sorted lexicographically.
var reasonWeights = map[string]int{
	"badRequest":             1,
	"backendError":           2,
	"quotaExceeded":          3,
	"insufficientCapacity":   4,
	"requestExceedsCapacity": 5,
}

func PickReason(errors []googleapi.ErrorItem) string {
	reasons := make([]string, 0, len(errors))
	for _, err := range errors {
		reasons = append(reasons, err.Reason)
	}
	sort.Slice(reasons, func(i, j int) bool {
		iWeight := reasonWeights[reasons[i]]
		jWeight := reasonWeights[reasons[j]]
		if iWeight == jWeight {
			return reasons[i] < reasons[j]
		}
		return iWeight > jWeight
	})
	return reasons[0]
}

func determineResponseStatusForLatencyMetric(rsp *googleapi.ServerResponse, err error, withReason bool) string {
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return "timeout"
		} else if gErr, ok := err.(*googleapi.Error); ok {
			if withReason && len(gErr.Errors) > 0 {
				return fmt.Sprintf("%d_%s", gErr.Code, PickReason(gErr.Errors))
			} else {
				return fmt.Sprintf("%d", gErr.Code)
			}
		} else {
			return "internal_error"
		}
	}

	if rsp != nil {
		return fmt.Sprintf("%d", rsp.HTTPStatusCode)
	}

	// if no response was provided and there was no error, assume success
	return "200"
}

func serverResponseFromAny(obj any) *googleapi.ServerResponse {
	fieldname := "ServerResponse"
	fieldInterface, err := structFieldByName(obj, fieldname)
	if err != nil {
		klog.Warningf("received error when parsing ServerResponse field: %v", err)
		return nil
	}
	if fieldInterface == nil {
		return nil
	}
	serverResponse, ok := fieldInterface.(googleapi.ServerResponse)
	if !ok {
		klog.Warningf("field value for fieldname %s is not of expected type *googleapi.ServerResponse, but %T", fieldname, reflect.TypeOf(fieldInterface))
		return nil
	}
	return &serverResponse
}

func structFieldByName(obj any, fieldname string) (any, error) {
	if obj == nil {
		return nil, nil
	}
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Pointer {
		if val.IsNil() {
			return nil, nil
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("type %T is not a struct", obj)
	}
	fieldVal := val.FieldByName(fieldname)
	if !fieldVal.IsValid() {
		return nil, fmt.Errorf("for struct of type %T, field value for fieldname %s is not valid", obj, fieldname)
	}
	if !fieldVal.CanInterface() {
		return nil, fmt.Errorf("for struct of type %T, field value for fieldname %s is not addressable", obj, fieldname)
	}
	return fieldVal.Interface(), nil
}
