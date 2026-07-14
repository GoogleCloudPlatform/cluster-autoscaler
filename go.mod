module k8s.io/gke-autoscaling/cluster-autoscaler

go 1.26.0

godebug default=go1.25

godebug winsymlink=0

require (
	cloud.google.com/go/compute/metadata v0.9.0
	cloud.google.com/go/iam v1.5.3
	cloud.google.com/go/resourcemanager v1.10.7
	github.com/GoogleCloudPlatform/gke-networking-api v0.2.1-0.20250318085121-e88f4ed9f50a
	github.com/blang/semver/v4 v4.0.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da
	github.com/golang/mock v1.6.0
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/googlecloudplatform/compute-class-api v0.0.0-20260622085058-ec3fc1b9c8c6
	github.com/prometheus/client_golang v1.23.2
	github.com/satori/go.uuid v1.2.0
	github.com/spf13/pflag v1.0.10
	github.com/stretchr/testify v1.11.1
	golang.org/x/exp v0.0.0-20251219203646-944ab1f22d93
	golang.org/x/oauth2 v0.36.0
	google.golang.org/api v0.284.0
	google.golang.org/grpc v1.81.1
	google.golang.org/protobuf v1.36.12-0.20260120151049-f2248ac996af
	gopkg.in/gcfg.v1 v1.2.3
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.36.2
	k8s.io/apimachinery v0.36.2
	k8s.io/apiserver v0.36.2
	k8s.io/autoscaler/cluster-autoscaler v0.0.0-20260714115920-f5c4ad8a6440
	k8s.io/autoscaler/cluster-autoscaler/apis v0.0.0-20260714115920-f5c4ad8a6440
	k8s.io/client-go v0.36.2
	k8s.io/cloud-provider v0.36.2
	k8s.io/cloud-provider-gcp/providers v0.28.2
	k8s.io/component-base v0.36.2
	k8s.io/component-helpers v0.36.2
	k8s.io/gke-autoscaling/cluster-autoscaler/apis v0.0.0-00010101000000-000000000000
	k8s.io/klog/v2 v2.140.0
	k8s.io/kube-scheduler v0.36.2
	k8s.io/kubelet v0.36.2
	k8s.io/kubernetes v1.36.2
	k8s.io/utils v0.0.0-20260617174310-a95e086a2553
	sigs.k8s.io/controller-runtime v0.24.1
	sigs.k8s.io/structured-merge-diff/v4 v4.7.0
	sigs.k8s.io/yaml v1.6.0
)

require (
	github.com/Masterminds/semver/v3 v3.4.0 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/go-openapi/swag/cmdutils v0.26.1 // indirect
	github.com/go-openapi/swag/conv v0.26.1 // indirect
	github.com/go-openapi/swag/fileutils v0.26.1 // indirect
	github.com/go-openapi/swag/jsonname v0.26.1 // indirect
	github.com/go-openapi/swag/jsonutils v0.26.1 // indirect
	github.com/go-openapi/swag/loading v0.26.1 // indirect
	github.com/go-openapi/swag/mangling v0.26.1 // indirect
	github.com/go-openapi/swag/netutils v0.26.1 // indirect
	github.com/go-openapi/swag/stringutils v0.26.1 // indirect
	github.com/go-openapi/swag/typeutils v0.26.1 // indirect
	github.com/go-openapi/swag/yamlutils v0.26.1 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	golang.org/x/net v0.56.0 // indirect
	gomodules.xyz/jsonpatch/v2 v2.4.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	k8s.io/streaming v0.36.2 // indirect
)

require (
	cel.dev/expr v0.25.1 // indirect
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/auth v0.20.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/longrunning v0.8.0 // indirect
	github.com/GoogleCloudPlatform/k8s-cloud-provider v1.25.0 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/NYTimes/gziphandler v1.1.1 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/emicklei/go-restful/v3 v3.13.0 // indirect
	github.com/evanphx/json-patch/v5 v5.9.11
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/fxamacker/cbor/v2 v2.9.2 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/jsonpointer v0.23.1 // indirect
	github.com/go-openapi/jsonreference v0.21.6 // indirect
	github.com/go-openapi/swag v0.26.1 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/google/cel-go v0.26.1 // indirect
	github.com/google/gnostic-models v0.7.1 // indirect
	github.com/google/pprof v0.0.0-20260402051712-545e8a4df936 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.16 // indirect
	github.com/googleapis/gax-go/v2 v2.22.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.28.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/onsi/ginkgo/v2 v2.32.0 // indirect
	github.com/onsi/gomega v1.42.1 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.67.5 // indirect
	github.com/prometheus/procfs v0.20.1 // indirect
	github.com/spf13/cobra v1.10.2 // indirect
	github.com/stoewer/go-strcase v1.3.1 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.67.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.67.0 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.43.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.43.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.opentelemetry.io/proto/otlp v1.10.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	// CVE-2025-58181, CVE-2025-47914: golang.org/x/crypto >= 0.45.0
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/mod v0.36.0 // indirect
	golang.org/x/sync v0.21.0
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/term v0.44.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/tools v0.45.0 // indirect
	google.golang.org/genproto v0.0.0-20260319201613-d00831a3d3e7 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260401024825-9d38bb4040a9 // indirect
	gopkg.in/evanphx/json-patch.v4 v4.13.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/warnings.v0 v0.1.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/apiextensions-apiserver v0.36.2 // indirect
	k8s.io/code-generator v0.36.2 // indirect
	k8s.io/controller-manager v0.36.2 // indirect
	k8s.io/cri-api v0.36.2 // indirect
	k8s.io/cri-client v0.36.2 // indirect
	k8s.io/csi-translation-lib v0.36.2 // indirect
	k8s.io/dynamic-resource-allocation v0.36.2 // indirect
	k8s.io/gengo/v2 v2.0.0-20250922181213-ec3ebc5fd46b // indirect
	k8s.io/kube-openapi v0.0.0-20260624041617-8f3fa4921821 // indirect
	sigs.k8s.io/json v0.0.0-20250730193827-2d320260d730 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v6 v6.4.0 // indirect
)

// Kubernetes dependencies, the version we depend on is dictated by the version of
// OSS cluster autoscaler being used and the k8s minor we are packaging the internal binary for
//
// Managed in the scope of the sync process
replace (
	k8s.io/api => k8s.io/api v0.36.2
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.36.2
	k8s.io/apimachinery => k8s.io/apimachinery v0.36.2
	k8s.io/apiserver => k8s.io/apiserver v0.36.2
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.36.2
	k8s.io/client-go => k8s.io/client-go v0.36.2
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.36.2
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.36.2
	k8s.io/code-generator => k8s.io/code-generator v0.36.2
	k8s.io/component-base => k8s.io/component-base v0.36.2
	k8s.io/component-helpers => k8s.io/component-helpers v0.36.2
	k8s.io/controller-manager => k8s.io/controller-manager v0.36.2
	k8s.io/cri-api => k8s.io/cri-api v0.36.2
	k8s.io/cri-client => k8s.io/cri-client v0.36.2
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.36.2
	k8s.io/dynamic-resource-allocation => k8s.io/dynamic-resource-allocation v0.36.2
	k8s.io/endpointslice => k8s.io/endpointslice v0.36.2
	k8s.io/externaljwt => k8s.io/externaljwt v0.36.2
	k8s.io/kms => k8s.io/kms v0.36.2
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.36.2
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.36.2
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.36.2
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.36.2
	k8s.io/kubectl => k8s.io/kubectl v0.36.2
	k8s.io/kubelet => k8s.io/kubelet v0.36.2
	k8s.io/kubernetes => k8s.io/kubernetes v1.36.2
	k8s.io/metrics => k8s.io/metrics v0.36.2
	k8s.io/mount-utils => k8s.io/mount-utils v0.36.2
	k8s.io/pod-security-admission => k8s.io/pod-security-admission v0.36.2
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.36.2
	k8s.io/sample-cli-plugin => k8s.io/sample-cli-plugin v0.36.2
	k8s.io/sample-controller => k8s.io/sample-controller v0.36.2
)

// Internally managed library forks and packages
//
// Managed in the scope of the sync process
replace k8s.io/gke-autoscaling/cluster-autoscaler/apis => ./apis

// We are replacing transitive glog dependency with a klog shim as it's interferes with klog flag initialization logic
replace github.com/golang/glog => ./modreplaces/glog

replace k8s.io/cri-streaming => k8s.io/cri-streaming v0.36.2

replace k8s.io/streaming => k8s.io/streaming v0.36.2

replace k8s.io/autoscaler/cluster-autoscaler => k8s.io/autoscaler/cluster-autoscaler v0.0.0-20260714115920-f5c4ad8a6440

replace k8s.io/autoscaler/cluster-autoscaler/apis => k8s.io/autoscaler/cluster-autoscaler/apis v0.0.0-20260714115920-f5c4ad8a6440
