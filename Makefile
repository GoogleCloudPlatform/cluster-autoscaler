.PHONY: all
all: build

FLAGS=
ENVVAR=CGO_ENABLED=0
GITROOT=$(if $(HOST_INPUT_ROOT),$(HOST_INPUT_ROOT)/src/git/cluster-autoscaler,$(shell pwd)/..)
GOARCH?=amd64
GOOS?=linux
BUILD_DEBUG_VERSION?=false
DOCKERFILE=Dockerfile

GOBUILDFLAGS=-ldflags "-s -w" -v
DOCKER_TARGET=release
ifeq (${BUILD_DEBUG_VERSION},true)
	GOBUILDFLAGS=-gcflags=all="-N -l" -v
	DOCKER_TARGET=debug
endif

ARM_EMULATOR_PATH?=/usr/bin/qemu-aarch64-static
ifeq (${GOARCH},arm64)
	ARM_EMULATOR_PARAMS=--exec ${ARM_EMULATOR_PATH}
	RACE_TEST_PARAMS=
else
	ARM_EMULATOR_PARAMS=
	RACE_TEST_PARAMS=-race
endif

# Trigger --quiet mode by adding QUIET=true env, effectively skipping user confirmation. Supported by some of the commands
ifeq ($(QUIET),true)
	QUIET_FLAG=--quiet
else
	QUIET_FLAG=
endif

.PHONY: gen-clients
gen-clients: gen-clients-updateinfos gen-clients-capacityrequests gen-clients-mc

.PHONY: gen-clients-init
gen-clients-init:
	go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.14.0

.PHONY: gen-clients-updateinfos
gen-clients-updateinfos:
	bash hack/gen-clients.sh updateinfos \
	mkdir -p pkg/updateinfos/client/listers/nodemanagement.gke.io/v1alpha1/mock; \
	mockgen --destination ${GOPATH}/src/k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/listers/nodemanagement.gke.io/v1alpha1/mock/upgradeinfo.go \
	--package mock k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/listers/nodemanagement.gke.io/v1alpha1 \
	UpdateInfoLister,UpdateInfoListerExpansion;

.PHONY: gen-clients-capacityrequests
gen-clients-capacityrequests:
	bash hack/gen-clients.sh capacityrequests

.PHONY: gen-clients-mc
gen-clients-mc:
	bash hack/gen-clients.sh machineconfig

.PHONY: compute-tag
compute-tag:
	$(eval TAG=$(shell ./hack/compute-version-tag.sh tag))
	if [ -n "${TAG}" ]; then \
		echo "computed TAG: ${TAG}"; \
	else \
		echo "Could not compute TAG"; \
		exit 1; \
	fi

.PHONY: compute-version
compute-version:
	$(eval VERSION=$(shell ./hack/compute-version-tag.sh version))
	if [ -n "${VERSION}" ]; then \
		echo "computed VERSION: ${VERSION}"; \
	else \
		echo "Could not compute VERSION"; \
		exit 1; \
	fi

.PHONY: check-registry-defined
check-registry-defined:
ifndef REGISTRY
	ERR = $(error REGISTRY is undefined)
	$(ERR)
endif

.PHONY: build
build: clean-binary compile-proto compute-version
	echo "Building Cluster-Autoscaler version ${VERSION}"; \
	./hack/update-version-go.sh ${VERSION}; \
	trap "./hack/update-version-go.sh reset" EXIT; \
	$(ENVVAR) GOOS=$(GOOS) GOARCH=$(GOARCH) go build ${GOBUILDFLAGS} ./... && \
	$(ENVVAR) GOOS=$(GOOS) GOARCH=$(GOARCH) go build ${GOBUILDFLAGS} -o cluster-autoscaler

.PHONY: build-binary
build-binary: clean-binary compile-proto compute-version
	echo "Building Cluster-Autoscaler version ${VERSION}"; \
	./hack/update-version-go.sh ${VERSION}; \
	trap "./hack/update-version-go.sh reset" EXIT; \
	$(ENVVAR) GOOS=$(GOOS) GOARCH=$(GOARCH) go build ${GOBUILDFLAGS} -o cluster-autoscaler

.PHONY: build-binary-no-internal-client
build-binary-no-internal-client: clean-binary compile-proto compute-version
	echo "Building Cluster-Autoscaler version ${VERSION}"; \
	./hack/update-version-go.sh ${VERSION}; \
	trap "./hack/update-version-go.sh reset" EXIT; \
	$(ENVVAR) GOOS=$(GOOS) GOARCH=$(GOARCH) go build ${GOBUILDFLAGS} -tags no_internal_clients -o cluster-autoscaler

.PHONY: test-unit
test-unit: clean-binary build
	CA_RUN_LONG_TESTS=${CA_RUN_LONG_TESTS} go test --test.short ${ARM_EMULATOR_PARAMS} ./... $(FLAGS)

.PHONY: test-unit-oss
test-unit-oss: clean-binary build
	go test --test.short ${RACE_TEST_PARAMS} ${ARM_EMULATOR_PARAMS} $$(go list ./vendor/... | grep /vendor/k8s.io/autoscaler) $(FLAGS)

.PHONY: clean
clean:
	rm -f cluster-autoscaler go-build

# TODO(b/339821239): remove excessive usage of clean-binary
.PHONY: clean-binary
clean-binary:
	rm -f cluster-autoscaler

.PHONY: format
format:
	test -z "$$(find . -path ./vendor -prune -type f -o -name '*.go' -exec gofmt -s -d {} + | tee /dev/stderr)" || \
    test -z "$$(find . -path ./vendor -prune -type f -o -name '*.go' -exec gofmt -s -w {} + | tee /dev/stderr)"

.PHONY: docker-builder
docker-builder:
	docker build -t autoscaling-builder ../builder

.PHONY: build-binary-in-docker
build-binary-in-docker: clean-binary docker-builder
	EXTUSER=$$(id -u); \
	EXTGROUP=$$(id -g); \
	mkdir -p ${GITROOT}/cluster-autoscaler/go-build; \
	docker run -e TAG -v ${GITROOT}:/tmpfs/gopath/src/k8s.io/gke-autoscaling/ \
		-e GOCACHE=/tmpfs/gopath/src/k8s.io/gke-autoscaling/cluster-autoscaler/go-build \
		autoscaling-builder:latest bash -c \
		"git config --global --add safe.directory /tmpfs/gopath/src/k8s.io/gke-autoscaling && \
		set -x; cd /tmpfs/gopath/src/k8s.io/gke-autoscaling/cluster-autoscaler && \
		make BUILD_DEBUG_VERSION=${BUILD_DEBUG_VERSION} GOARCH=$(GOARCH) build-binary && \
		chown $${EXTUSER}:$${EXTGROUP} cluster-autoscaler && \
		chown $${EXTUSER}:$${EXTGROUP} version.go && \
		find pkg -name '*.pb.go' -exec chown $${EXTUSER}:$${EXTGROUP} {} \;"

.PHONY: build-binary-in-docker-no-internal-client
build-binary-in-docker-no-internal-client: clean-binary docker-builder
	EXTUSER=$$(id -u); \
	EXTGROUP=$$(id -g); \
	mkdir -p ${GITROOT}/cluster-autoscaler/go-build; \
	docker run -e TAG -v ${GITROOT}:/tmpfs/gopath/src/k8s.io/gke-autoscaling/ \
		--tmpfs /tmpfs/gopath/src/k8s.io/gke-autoscaling/cluster-autoscaler/pkg/internalclients \
		-e GOCACHE=/tmpfs/gopath/src/k8s.io/gke-autoscaling/cluster-autoscaler/go-build \
		autoscaling-builder:latest bash -c \
		"git config --global --add safe.directory /tmpfs/gopath/src/k8s.io/gke-autoscaling && \
		set -x; cd /tmpfs/gopath/src/k8s.io/gke-autoscaling/cluster-autoscaler && \
		make BUILD_DEBUG_VERSION=${BUILD_DEBUG_VERSION} GOARCH=$(GOARCH) build-binary-no-internal-client && \
		chown $${EXTUSER}:$${EXTGROUP} cluster-autoscaler && \
		chown $${EXTUSER}:$${EXTGROUP} version.go && \
		find pkg -name '*.pb.go' -exec chown $${EXTUSER}:$${EXTGROUP} {} \;"

.PHONY: build-image compute-tag
build-image: compute-tag compute-version check-registry-defined

.PHONY: build-image-in-docker
build-image-in-docker: compute-tag compute-version check-registry-defined

build-image build-image-in-docker:
	docker build --platform linux/${GOARCH} --pull -f ${DOCKERFILE} --target ${DOCKER_TARGET} --build-arg VERSION=${VERSION} -t ${REGISTRY}/cluster-autoscaler:${TAG} .

.PHONY: release
release: build-image-in-docker compute-tag check-registry-defined
	./push_image.sh ${REGISTRY}/cluster-autoscaler:${TAG} && \
	echo "Full in-docker release ${TAG} completed"

.PHONY: release-ci
release-ci: build-image-in-docker compute-tag check-registry-defined
	./push_image.sh ${REGISTRY}/cluster-autoscaler:${TAG} ${QUIET_FLAG} --force && \
	echo "Full in-docker release ${TAG} completed"

.PHONY: setup-multiarch-builder
setup-multiarch-builder:
	docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
	docker buildx use multiarch-builder || docker buildx create --name multiarch-builder --use

.PHONY: release-multiarch
release-multiarch: compute-tag compute-version check-registry-defined setup-multiarch-builder
	./push_image.sh ${REGISTRY}/cluster-autoscaler:${TAG} --check-only ${QUIET_FLAG} --force && \
	docker buildx build --platform linux/amd64,linux/arm64 --push -f ${DOCKERFILE} --target ${DOCKER_TARGET} --build-arg VERSION=${VERSION} -t ${REGISTRY}/cluster-autoscaler:${TAG} . && \
	echo "Full in-docker multi-arch release ${TAG} completed"

.PHONY: dev-release
dev-release: build-image compute-tag check-registry-defined
	./push_image.sh ${REGISTRY}/cluster-autoscaler:${TAG} && \
	echo "Release ${TAG} completed"

.PHONY: test-in-docker
test-in-docker: clean-binary docker-builder
	# GODEBUG=asynctimerchan=0 is required for the testing/synctest package.
	# synctest strictly requires the Go 1.23+ synchronous timer implementation, ensuring tests don't fall back to older async behaviors and fail.
	docker run -e GODEBUG=asynctimerchan=0 -v ${GITROOT}:/tmpfs/gopath/src/k8s.io/gke-autoscaling/ autoscaling-builder:latest bash -c \
		'cd /tmpfs/gopath/src/k8s.io/gke-autoscaling/cluster-autoscaler && \
		make compile-proto && \
	  CA_RUN_LONG_TESTS=${CA_RUN_LONG_TESTS} CA_MANUAL_TEST=${CA_MANUAL_TEST} GOOS=$(GOOS) GOARCH=$(GOARCH) go test ${ARM_EMULATOR_PARAMS} ./...'

.PHONY: test-in-docker-no-internal-client
test-in-docker-no-internal-client: clean-binary docker-builder
	# GODEBUG=asynctimerchan=0 is required for the testing/synctest package.
	# synctest strictly requires the Go 1.23+ synchronous timer implementation, ensuring tests don't fall back to older async behaviors and fail.
	docker run -e GODEBUG=asynctimerchan=0 -v ${GITROOT}:/tmpfs/gopath/src/k8s.io/gke-autoscaling/ \
		--tmpfs /tmpfs/gopath/src/k8s.io/gke-autoscaling/cluster-autoscaler/pkg/internalclients \
		autoscaling-builder:latest bash -c \
		'cd /tmpfs/gopath/src/k8s.io/gke-autoscaling/cluster-autoscaler && \
		make compile-proto && \
	  CA_RUN_LONG_TESTS=${CA_RUN_LONG_TESTS} CA_MANUAL_TEST=${CA_MANUAL_TEST} GOOS=$(GOOS) GOARCH=$(GOARCH) go test -tags no_internal_clients ${ARM_EMULATOR_PARAMS} ./...'

.PHONY: test-in-docker-oss
test-in-docker-oss: clean-binary docker-builder
	docker run -v ${GITROOT}:/tmpfs/gopath/src/k8s.io/gke-autoscaling/ autoscaling-builder:latest bash -c \
		'cd /tmpfs/gopath/src/k8s.io/gke-autoscaling/cluster-autoscaler && \
		GOOS=$(GOOS) GOARCH=$(GOARCH) go test ${RACE_TEST_PARAMS} ${ARM_EMULATOR_PARAMS} $$(go list ./vendor/... | grep /vendor/k8s.io/autoscaler)'

.PHONY: compile-proto
compile-proto:
	for protofile in $$(find . -name "*.proto" -not -path "**/vendor/**"); do protoc $$protofile --go_out=.; done

.PHONY: test-scaledown
test-scaledown: clean-binary docker-builder
	docker run -v ${GITROOT}:/tmpfs/gopath/src/k8s.io/gke-autoscaling/ -v /tmp/:/tmp/ autoscaling-builder:latest bash -c \
		'cd /tmpfs/gopath/src/k8s.io/gke-autoscaling/cluster-autoscaler && \
		make compile-proto && \
	  CA_MANUAL_TEST=true go test ./pkg/impostor -run "TestScaleDown" -timeout=24h -race'

.PHONY: test-largescaleuprequest
test-largescaleuprequest: clean-binary docker-builder
	docker run -v ${GITROOT}:/tmpfs/gopath/src/k8s.io/gke-autoscaling/ -v /tmp/:/tmp/ autoscaling-builder:latest bash -c \
		'cd /tmpfs/gopath/src/k8s.io/gke-autoscaling/cluster-autoscaler && \
		make compile-proto && \
	  CA_MANUAL_TEST=true go test ./pkg/expander/test -run "TestLargeScaleUpRequest" -timeout=1h -race'

.PHONY: deploy
deploy: check-registry-defined compute-tag
	./hack/deploy.sh --cluster-hash ${CLUSTER_HASH} --env ${ENV} --image ${REGISTRY}/cluster-autoscaler:${TAG} \
		--ca-flags "${CA_FLAGS}" --sandbox-name "${SANDBOX_NAME}" --run-ca-under-dlv "${BUILD_DEBUG_VERSION}"

.PHONY: test-in-docker-multiarch
test-in-docker-multiarch:
	$(MAKE) GOARCH=amd64 test-in-docker
	$(MAKE) GOARCH=arm64 test-in-docker

.PHONY: build-binary-in-docker-multiarch
build-binary-in-docker-multiarch:
	$(MAKE) GOARCH=amd64 build-binary-in-docker
	$(MAKE) GOARCH=arm64 build-binary-in-docker

.PHONY: build-binary-in-docker-no-internal-client-multiarch
build-binary-in-docker-no-internal-client-multiarch:
	$(MAKE) GOARCH=amd64 build-binary-in-docker-no-internal-client
	$(MAKE) GOARCH=arm64 build-binary-in-docker-no-internal-client

.PHONY: test-in-docker-oss-multiarch
test-in-docker-oss-multiarch:
	$(MAKE) GOARCH=amd64 test-in-docker-oss
	$(MAKE) GOARCH=arm64 test-in-docker-oss

