# Self-Hosted GKE Cluster Autoscaler

This helm chart is focused on cluster autoscaler configurations, it's intentionally kept as generic as possible in order for it to not require any changes for specific use cases

> [!WARNING]
> Deploying self-hosted in a GKE cluster currently will result in race conditions between self-hosted instance and the built-in GKE Cluster Autoscaler. The functionality to disable the built-in GKE Cluster Autoscaler is currently pending.

## Managed CRDs installation

Cluster autoscaler integrates with multiple custom resource definitions both GKE-only and external, in case any of them is not installed or not accessible - cluster autoscaler may function incorrectly. It's recommended to install this component only into GKE cluster in order to have all the dependencies already installed

### Prerequisites

1. GKE cluster **v1.36.0 or higher**: `kubectl version`
1. Helm **v4.0.0 or higher** [installed](https://helm.sh/docs/using_helm/#installing-helm): `helm version`

## Installing

⚠️ OCI repository pending, only local installation supported right now

To install the chart with default values:

```bash
helm install selfhosted-ca .
```

To customize the installation, provide a custom `values` file:

```bash
helm install -f myvalues.yaml selfhosted-ca .
```

## Customization

If you want to customize the deployment for your needs, you can override default recommended chart configuration via [YAML or CLI](https://helm.sh/docs/chart_template_guide/values_files/)

If you need to include additional Kubernetes objects or extend functionality, use `extraObjects` or add this chart as a subchart.

For complete documentation on all available parameters, check the [default values file](./values.yaml) or refer to the [Deployment configurations](#deployment-configurations)

## Authentification & authorization

In order for autoscaler to manage your cluster it needs to authorize it to manage your GCP resources in the deployed project, recommended way to achieve that is [Workload Identity Federation](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/workload-identity)

What needs to be done in the cluster in order for cluster autoscaler to be able to manage your resources:

* [Enable Identity Federation in the GKE cluster](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#enable_on_clusters_and_node_pools)
* [Create node pool with Identity Federation enabled](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#migrate_applications_to)
* [Create custom role](https://docs.cloud.google.com/iam/docs/creating-custom-roles#creating) using [GCP_ROLE.yaml](./GCP_ROLE.yaml) YAML manifest
* [Create service account using custom role](https://docs.cloud.google.com/iam/docs/service-accounts-create#creating)
* [Grant kubernetes service account access to use created IAM SA](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#kubernetes-sa-to-iam)
* Ensure that cluster autoscaler runs on the nodes with metadata server enabled

```yaml
nodeSelector:
    iam.gke.io/gke-metadata-server-enabled: "true"
```

* Configure service account to use created IAM SA

```yaml
serviceAccount:
    gcpWorkloadIdentity: "IAM_SA_NAME@IAM_SA_PROJECT_ID.iam.gserviceaccount.com"
```

OR

```yaml
serviceAccount:
    annotations:
        iam.gke.io/gcp-service-account: "IAM_SA_NAME@IAM_SA_PROJECT_ID.iam.gserviceaccount.com"
```

## Testing

Static unit tests are implemented using the [`helm-unittest`](https://github.com/helm-unittest/helm-unittest) plugin to validate chart rendering and configuration options without requiring a live cluster.

Run the test suite locally:

```bash
# If helm-unittest plugin is installed
helm unittest .

# Or using docker
docker run --rm -v $(pwd):/apps helmunittest/helm-unittest .
```

## Deployment configurations

### Required Configurations

The `image` and `cluster` configuration sections are currently required when deploying the chart:

```yaml
# Container image repository and tag
image:
  repository: REPOSITORY
  tag: TAG

# Target GKE cluster identity (example values, should be replaced with your cluster details)
cluster:
  name: NAME
  hash: ID
  projectNumber: PROJECT_NUMBER
  location: LOCATION
  regional: REGIONAL
```

In order to obtain cluster ID:

```bash
gcloud container clusters describe CLUSTER_NAME --location=LOCATION --project=PROJECT_ID --format="value(id)"
```

In order to obtain project number:

```bash
gcloud projects describe PROJECT_ID --format="value(projectNumber)"
```

### Autoscaling Profiles

You can select which autoscaling profile to apply using `config.autoscalingProfile`. The chart comes with preconfigured profiles such as `default` (balanced) and `optimizeUtilization` (aggressive scale-down):

```yaml
config:
  autoscalingProfile: optimizeUtilization # Options: default, optimizeUtilization, or a custom profile
```

You can also override parameters in existing profiles or define custom profiles under `autoscalingProfiles`:

```yaml
autoscalingProfiles:
  optimizeUtilization:
    scale-down-unneeded-time: "1m"
    scale-down-utilization-threshold: "0.85"
```