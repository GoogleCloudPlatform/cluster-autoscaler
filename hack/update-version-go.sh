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


CURDIR=$(dirname $(readlink -f $BASH_SOURCE))
VERSION_FILE=$CURDIR/../version.go
BACKUP_FILE=$CURDIR/../version.go-orig

VERSION="$1"
if [[ $VERSION == "" ]]; then
  echo "Script expects a version or reset argument"
  exit 1
fi

# reset
if [[ "$VERSION" == "reset" ]]; then
  if [ ! -f $BACKUP_FILE ]; then
    echo "Backup file does not exist"
    exit 1
  fi
  mv $BACKUP_FILE $VERSION_FILE
  exit 0
fi

# version update
if [ ! -f $BACKUP_FILE ]; then
  # skip backup if it already exists
  cp $VERSION_FILE $BACKUP_FILE
fi

sed -e "s#\\(ClusterAutoscalerVersion\\s*=\\s*\\)\"[^\"]*\"#\\1\"$VERSION\"#" -i $VERSION_FILE

if ! grep -q "\"$VERSION\"" $VERSION_FILE; then
  echo "ERROR: Expected version to be updated to $VERSION"
  exit 1
fi


