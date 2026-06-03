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

package internal

// IsSuspended describes whether an instance should be considered
// as suspended according to its status.
func IsSuspended(instanceStatus string) bool {
	return instanceStatus == "SUSPENDED" || instanceStatus == "SUSPENDING"
}

// IsStopped describes whether an instance should be considered
// as suspended according to its status.
func IsStopped(instanceStatus string) bool {
	return instanceStatus == "PENDING_STOP" ||
		instanceStatus == "STOPPING" ||
		instanceStatus == "TERMINATED"
}
