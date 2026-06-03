# Node Prediction decoupling

go/ca-node-prediction-decoupling

Date: 2026-05-06

## Decision

1. All node prediction logic should be grouped in a single package `pkg/nodeprediction`.

   pkg/nodeprediction will:
   1. **Not** be tested with Big Unit Tests (correctness of node prediction can be only verified against real GKE).
   2. Document the contract between CA and GCE\&GKE, or at least how CA views it
   3. In addition to Big Unit Tests, can be re-used for "Fake GCE" tests.

2. Avoid adding any heavyweight dependencies to this package (i.e. anything that we want to have covered with Big Unit Tests)
   1. Allowed examples: Fakes (fake GCE, fake GKE, fake K8s…) and their dependencie
   2. Not allowed: CA Core, CloudProvider, Processors, ...

## Context

[Big Unit Tests](go/big-unit-tests) introduce somewhat-smart fakes for GKE that need to simulate some external APIs interactions:

* For GKE entities (Clusters, Node Pools) we simulate underlying GCE entities (MIGs, VMs and more).
* For GKE/GKE configuration (e.g. Node Pools with matching MIGs and instances) we simulate Kubernetes nodes.

Cluster Autoscaler already makes similar predictions, needed for NAP and scale-up from zero simulations. But this logic is scattered across the codebase, for example:

* BuildNodeFromTemplate in OSS: cluster-autoscaler/vendor/k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/templates.go
* BuildNodeFromMigSpec in GKE Cloud Provider: cluster-autoscaler/pkg/cloudprovider/gke/templates.go
* DRA prediction in GKE Cloud Provider: cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources/predictor.go
* ...

