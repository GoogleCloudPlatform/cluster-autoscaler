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


MODE=${1:-tag}
SKIP_COMMIT_SUFFIX=false

if [[ "$MODE" != "tag" && "$MODE" != "version" ]]; then
  echo Operation mode must be either \"tag\" or \"version\" >&2
  exit 1
fi

if [[ "$TAG" != "" ]]; then
  # if tag was set explicitly use it
  BASE_VERSION=$TAG
  SKIP_COMMIT_SUFFIX=true
fi

if ! git rev-parse --git-dir >/dev/null 2>&1; then
  # Not a git repository or git command is malfunctioning
  if [[ "$TAG" != "" ]]; then
    echo $TAG
    exit 0
  else
    echo "Not a git repository and TAG not set" >&2
    exit 2
  fi
fi

if [[ "$BASE_VERSION" == "" ]]; then
  # If commit is tagged with cluster-autoscaler-v* set TAG based on that.
  GIT_TAG=$(git describe --tags --exact-match 2>/dev/null |grep cluster-autoscaler-v | sort | head -1)
  if [[ "$GIT_TAG" != "" ]];then
    BASE_VERSION=$(echo $GIT_TAG | cut -d - -f 3-)
    SKIP_COMMIT_SUFFIX=true
  fi
fi

if [[ "$BASE_VERSION" == "" ]]; then
  # if refs/heads/master is not available we are assuming that the repository is checked out
  # without any named refs. Then we use "unknown" as BASE_VERSION.
  if ! git show-ref -q --verify refs/heads/master; then
    BASE_VERSION="unknown"
  fi
fi

if [[ "$BASE_VERSION" == "" ]]; then
  # no explicit tag. Then try to find which version branch is nearest one.
  BRANCHES="$(git branch |cut -b 3- |grep -e '^cluster-autoscaler-release-[0-9]\+\.[0-9]\+$')"
  NEAREST_BRANCH=master
  NEAREST_MERGE_BASE=$(git merge-base HEAD master)
  for BRANCH in $BRANCHES; do
    MERGE_BASE=$(git merge-base HEAD $BRANCH)
    if [[ $MERGE_BASE != $NEAREST_MERGE_BASE ]]; then
      if git merge-base --is-ancestor $NEAREST_MERGE_BASE $MERGE_BASE; then
        NEAREST_MERGE_BASE=$MERGE_BASE
        NEAREST_BRANCH=$BRANCH
      fi
    fi
  done

  if echo $NEAREST_BRANCH | grep -q cluster-autoscaler-release; then
    BASE_VERSION=$(echo $NEAREST_BRANCH | cut -d - -f 4-)
  else
    BASE_VERSION=master
  fi
fi

if [[ $MODE == "tag" && "$SKIP_COMMIT_SUFFIX" == "true" ]]; then
  # we do not add commit/dirty suffix if we are operating in tag mode and
  # TAG was set explicitly or based on git tag
  echo "$BASE_VERSION"
else
  CURRENT_COMMIT=$(git rev-parse --short HEAD)
  DIRTY_SUFFIX=""
  if ! git diff --quiet; then
    DIRTY_SUFFIX="-dirty"
  fi
  echo "$BASE_VERSION-$CURRENT_COMMIT$DIRTY_SUFFIX"
fi
