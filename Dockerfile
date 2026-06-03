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

# Builder stage
FROM --platform=$BUILDPLATFORM google-go.pkg.dev/golang:1.26.1 AS builder

ARG PROTOC_VERSION=21.12
ARG PROTOC_GEN_GO_VERSION=v1.31.0

# Install dependencies needed for compilation (from builder/Dockerfile)
RUN apt-get update && apt-get --yes install curl unzip && \
    rm -rf /var/lib/apt/lists/*

# Install protoc
ARG BUILDPLATFORM
COPY hack/install-protoc.sh hack/
RUN ./hack/install-protoc.sh ${PROTOC_VERSION} /usr/local ${BUILDPLATFORM}

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION}

WORKDIR /tmpfs/gopath/src/k8s.io/gke-autoscaling/cluster-autoscaler
COPY . .

RUN for protofile in $(find . -name "*.proto" -not -path "**/vendor/**"); do protoc $protofile --go_out=.; done

ARG TARGETARCH
ARG VERSION

# Stage for release build
FROM builder AS builder-release
RUN ./hack/update-version-go.sh ${VERSION} && \
    CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -mod=vendor -ldflags "-s -w" -v -o cluster-autoscaler && \
    ./hack/update-version-go.sh reset

# Stage for debug build
FROM builder AS builder-debug
RUN ./hack/update-version-go.sh ${VERSION} && \
    CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -mod=vendor -gcflags=all="-N -l" -v -o cluster-autoscaler && \
    ./hack/update-version-go.sh reset

# Delve installation stage
FROM google-go.pkg.dev/golang:1.26.1 AS delve
RUN CGO_ENABLED=0 \
    go install -ldflags "-s -w -extldflags '-static'" github.com/go-delve/delve/cmd/dlv@latest && \
    rm -rf /root/.cache/go-build/ /go/pkg/mod/

# Final debug image
FROM gcr.io/distroless/static:latest AS debug
LABEL maintainer="Vlad Vasilyeu <vladvasilyeu@google.com>"
EXPOSE 40001
COPY --from=builder-debug /tmpfs/gopath/src/k8s.io/gke-autoscaling/cluster-autoscaler/cluster-autoscaler /cluster-autoscaler
COPY --from=delve /go/bin/dlv /
CMD ["/dlv", "exec", "/cluster-autoscaler", "--listen=127.0.0.1:40001", "--headless=true", "--api-version=2", "--accept-multiclient", "--only-same-user=false"]

# Final release image
FROM gcr.io/distroless/static:latest AS release
LABEL maintainer="Vlad Vasilyeu <vladvasilyeu@google.com>"
COPY --from=builder-release /tmpfs/gopath/src/k8s.io/gke-autoscaling/cluster-autoscaler/cluster-autoscaler /cluster-autoscaler
CMD ["/cluster-autoscaler"]
