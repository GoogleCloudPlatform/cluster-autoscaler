#!/bin/bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


set -o nounset
set -o pipefail

KUBE_ROOT=$(dirname "${BASH_SOURCE}")/..

if [[ -z "${color_start-}" ]]; then
  declare -r color_start="\033["
  declare -r color_red="${color_start}0;31m"
  declare -r color_yellow="${color_start}0;33m"
  declare -r color_green="${color_start}0;32m"
  declare -r color_norm="${color_start}0m"
fi

EXCLUDE=${EXCLUDE:-}

function is-excluded {
  if [[ $1 -ef ${BASH_SOURCE} ]]; then
    return
  fi

  for e in $EXCLUDE; do
    if [[ $1 -ef "$KUBE_ROOT/hack/$e" ]]; then
      return
    fi
  done
  return 1
}

ret=0      # Return value of the script
pids=()    # Array to store process IDs
outputs=() # Array to store output file paths

# Loop through each script
for t in $(ls $KUBE_ROOT/hack/verify-*.sh); do
  # Skip excluded scripts
  if is-excluded "$t"; then
    echo "Skipping $t"
    continue
  fi

  # Create a temporary file for output
  output_file=$(mktemp)
  outputs+=("$output_file")

  # Run the script in the background, redirecting output to the file
  {
    echo -e "Verifying $t"
    bash "$t"

    if [ $? -eq 0 ]; then
      echo -e "${color_green}SUCCESS${color_norm}"
      exit 0
    else
      echo -e "${color_red}FAILED${color_norm}"
      exit 1
    fi
  } &>"$output_file" &

  # Store the process ID
  pids+=($!)
done

# Wait for each process to complete in the order they were started and print output
for i in "${!pids[@]}"; do
  if ! wait "${pids[i]}"; then # Wait for the process to complete
    ret=1
  fi
  cat "${outputs[i]}" # Print the output from the temporary file

  # Clean up the temporary file
  rm -f "${outputs[i]}"
done

exit $ret
