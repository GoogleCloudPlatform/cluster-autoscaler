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

FROM google-go.pkg.dev/golang:1.26.1
LABEL maintainer="Marcin Wielgus <mwielgus@google.com>"

ENV GO111MODULE=auto

# Install tools for build verification
RUN \
    apt-get update && \
    apt-get --yes --no-install-recommends install \
    libseccomp-dev \
    autoconf \
    automake \
    libtool \
    curl \
    make \
    g++ \
    unzip \
    qemu-user-static \
    rsync \
    jq \
    apt-transport-https \
    gpg \
    gnupg \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ARG GCLOUD_VERSION="575.0.0-0"
RUN curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-cloud-cli=${GCLOUD_VERSION} \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install protoc v3.21.12
ARG PROTOC_VERSION=21.12
ARG PROTOC_ZIP=protoc-${PROTOC_VERSION}-linux-x86_64.zip
ARG PROTOC_SHA256=3a4c1e5f2516c639d3079b1586e703fc7bcfa2136d58bda24d1d54f949c315e8
RUN curl -fLO https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${PROTOC_ZIP} && \
    echo "${PROTOC_SHA256}  ${PROTOC_ZIP}" | sha256sum -c - && \
    unzip -o ${PROTOC_ZIP} -d /usr/local bin/protoc && \
    unzip -o ${PROTOC_ZIP} -d /usr/local 'include/*' && \
    rm -f ${PROTOC_ZIP}


ARG PROTOC_GEN_GO_VERSION=v1.31.0
ARG GOLANGCI_LINT_VERSION=v2.9.0
ARG MISSPEL_VERSION=v0.3.4
RUN \
    go install google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION} && \
    go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION} && \
    go install github.com/client9/misspell/cmd/misspell@${MISSPEL_VERSION}

RUN go version && protoc --version

CMD ["/bin/bash"]
