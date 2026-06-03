# GKE Cluster Autoscaler

This repository contains a version of the [Kubernetes Cluster Autoscaler](https://github.com/kubernetes/autoscaler) used in Google Kubernetes Engine (GKE).

## Overview

This project contains the **GKE Cluster Autoscaler**, a core component of Google Kubernetes Engine (GKE). Its primary function is to automatically resize the number of nodes in a given node pool based on the demands of your workloads.

It dynamically adjusts the size of a cluster in two ways:
*   **Scaling up:** When there are Pods that fail to schedule onto current nodes due to insufficient resources, the autoscaler adds new nodes to the node pool.
*   **Scaling down:** When nodes are underutilized and all of their Pods can be safely scheduled onto other nodes, the autoscaler removes those excess nodes to optimize resource usage and costs.

## Documentation

* [More details about cluster autoscaler](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/cluster-autoscaler)

## Requirements

* **Go**: Version `1.25.0` or higher (as defined in `go.mod`).

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for details on how to contribute to this repository.

## License

Apache 2.0; see [`LICENSE`](LICENSE) for details.
