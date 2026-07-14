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


# Deploys a given CA image to all masters in a given cluster, updating component version
# and optionally also adding given flags to CA command line arguments.
#
# Doesn't snowflake the cluster that would prevent updates/upgrades/KCP scaling
# from overriding the changes.
#
# Usage:
# --cluster-hash: target cluster hash
# --env: environment of the target cluster
# --image: CA image URL to set in CA manifest's image
# --version: version to set in CA manifest's component-version
# --ca-flags (optional): flags to append to CA manifest's command, without '--', separate multiple flags with '|'.
# --sandbox-name (optional): a custom sandbox name to provide to kap when listing master vms in env=dev
# --run-ca-under-dlv: if CA should be run under Delve Go debugger control
#
# Example:
# ./deploy.sh --cluster-hash 143fe4e085cc4627bc411686191238709cee01c01ab349079d9da5423eb42c4f --env test \
#     --image gcr.io/gke-pbetkier-hosted-master/crp/cluster-autoscaler/test/cluster-autoscaler:tst \
#     --version tst-fa8b3ec53d-dirty --ca-flags "my-feature=true|flag-two=false" --run-ca-under-dlv true
#
# Limitations:
# - Doesn't handle CA flags with quotes – would strip quotes from the input

set -e

# load arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --cluster-hash) CLUSTER_HASH="$2"; shift ;;
        --env) ENV="$2"; shift ;;
        --image) IMAGE="$2"; shift ;;
        --ca-flags) CA_FLAGS="$2"; shift ;;
        --sandbox-name) SANDBOX_NAME="$2"; shift ;;
        --run-ca-under-dlv) RUN_CA_UNDER_DLV="$2"; shift ;;
        *) echo "Unknown parameter: '$1'"; exit 1 ;;
    esac
    shift
done

# format CA_FLAGS input into a string to append to clusterautoscaler manifest
if [[ $CA_FLAGS != "" ]]; then
    ca_flag_per_line=$(echo $CA_FLAGS | tr "|" "\n")
    for f in $ca_flag_per_line; do
        FORMATTED_FLAGS="$FORMATTED_FLAGS\n    - --$f"
    done
fi

# run /cluster-autoscaler under Delve Go debugger control when requested
# (requires CA debug docker image which contains /dlv binary as well)
# RUN_CA_COMMAND value is prepared to be later used in sed substitution
if [[ $RUN_CA_UNDER_DLV = "true" ]]; then
    RUN_CA_COMMAND=$(sed -z -e 's/.$//' -e 's/\n/\\n/g' <<-END
    - /dlv
    - exec
    - /cluster-autoscaler
    - --listen=127.0.0.1:40001
    - --headless=true
    - --api-version=2
    - --accept-multiclient
    - --only-same-user=false
    - --
END
)
else
    RUN_CA_COMMAND=$(sed -z -e 's/.$//' -e 's/\n/\\n/g' <<-END
    - /cluster-autoscaler
END
)
fi

# prepare command to run on each master
# CA command in manifest is processed in 2 steps to make possible changes in two directions (run CA standalone or
# run CA under Delve Go debugger control):
#   1. change lines sequence starting from "    - /dlv" and ending with "    - --" to "    - /cluster-autoscaler"
#   2. change "    - /cluster-autoscaler" to $RUN_CA_COMMAND (standalone /cluster-autoscaler or under /dlv control)
REMOTE_CMD=$(cat <<-END
    CA_MANIFEST=\$(sudo find /etc/kubernetes/manifests -iname "clusterautoscaler*" -print) &&
    CA_ADDON=/etc/kubernetes/addons/gce-extras/clusterautoscaler.yaml &&
    sudo cp \$CA_MANIFEST ~/clusterautoscaler.yaml &&
    sudo cp \$CA_ADDON ~/clusterautoscaler-addon.yaml &&
    sed -i "s|image: .*\/cluster-autoscaler.*|image: $IMAGE|g" ~/clusterautoscaler.yaml &&
    sed -i -e "0,/env:/{//i\ $FORMATTED_FLAGS" -e\} ~/clusterautoscaler.yaml &&
    sed -i "/^    - \/dlv$/,/^    - --$/c\    - /cluster-autoscaler" ~/clusterautoscaler.yaml &&
    sed -i "s|^    - /cluster-autoscaler$|$RUN_CA_COMMAND|" ~/clusterautoscaler.yaml &&
    sed -i "s|addonmanager.kubernetes.io/mode: Reconcile|addonmanager.kubernetes.io/mode: EnsureExists|g" ~/clusterautoscaler-addon.yaml &&
    sudo cp ~/clusterautoscaler.yaml \$CA_MANIFEST &&
    sudo cp ~/clusterautoscaler-addon.yaml \$CA_ADDON
END
)
echo "Command to execute on masters:"
echo "$REMOTE_CMD"

# get cluster masters
MASTERS="$(kap cluster info get --hash "$CLUSTER_HASH" --env "$ENV" --sandbox_name "$SANDBOX_NAME" | grep "Master:" | awk '{print $3}')"
echo "Detected masters to update:"
echo "$MASTERS"

# execute deploy command on each master
for m in $MASTERS; do
    echo "Deploying to master $m"
    COMMAND=(
      kap
      cluster
      master
      ssh
      --hash "$CLUSTER_HASH"
      --env "$ENV"
      --master_name=$m
      --command="$REMOTE_CMD"
    )
    if [[ "$ENV" == "dev" ]]; then
      COMMAND+=( --sandbox_name "$SANDBOX_NAME" )
    fi

    "${COMMAND[@]}"

    if [[ $? != 0 ]]; then
        echo "Failed to deploy to master $m"
        exit 1
    fi
done

echo "Deploy successful."

