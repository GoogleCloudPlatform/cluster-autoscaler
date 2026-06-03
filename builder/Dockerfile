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

ENV GOPATH=/tmpfs/gopath/
ENV PATH=$GOPATH/bin:$PATH
ENV GO111MODULE=auto

RUN apt-get update && apt-get --yes install libseccomp-dev autoconf automake libtool curl make g++ unzip qemu-user-static

# Install protoc v3.21.12
ARG PROTOC_VERSION=21.12
ARG PROTOC_ZIP=protoc-${PROTOC_VERSION}-linux-x86_64.zip
ARG PROTOC_SHA256=3a4c1e5f2516c639d3079b1586e703fc7bcfa2136d58bda24d1d54f949c315e8
RUN curl -fLO https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${PROTOC_ZIP} && \
    echo "${PROTOC_SHA256}  ${PROTOC_ZIP}" | sha256sum -c - && \
    unzip -o ${PROTOC_ZIP} -d /usr/local bin/protoc && \
    unzip -o ${PROTOC_ZIP} -d /usr/local 'include/*' && \
    rm -f ${PROTOC_ZIP}

RUN go version
RUN go install github.com/tools/godep@latest
RUN GO111MODULE=on go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
RUN godep version
RUN protoc --version
CMD ["/bin/bash"]
