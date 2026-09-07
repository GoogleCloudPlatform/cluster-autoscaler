# cluster-autoscaler

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 1.36.0](https://img.shields.io/badge/AppVersion-1.36.0-informational?style=flat-square)

Scales Kubernetes worker nodes within autoscaling pools in GKE.

**Homepage:** <https://github.com/GoogleCloudPlatform/cluster-autoscaler>

## Source Code

* <https://github.com/GoogleCloudPlatform/cluster-autoscaler/tree/main>

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| additionalLabels | object | `{}` |  |
| affinity | object | `{}` |  |
| autoscalingProfiles.default.daemonset-eviction-for-empty-nodes | string | `"true"` |  |
| autoscalingProfiles.default.max-drain-parallelism | string | `"100"` |  |
| autoscalingProfiles.default.max-scale-down-parallelism | string | `"100"` |  |
| autoscalingProfiles.default.scale-down-delay-after-delete | string | `"10s"` |  |
| autoscalingProfiles.optimizeUtilization.max-drain-parallelism | string | `"100"` |  |
| autoscalingProfiles.optimizeUtilization.max-scale-down-parallelism | string | `"100"` |  |
| autoscalingProfiles.optimizeUtilization.scale-down-delay-after-delete | string | `"5s"` |  |
| autoscalingProfiles.optimizeUtilization.scale-down-delay-after-failure | string | `"5s"` |  |
| autoscalingProfiles.optimizeUtilization.scale-down-gpu-utilization-threshold | string | `"0.85"` |  |
| autoscalingProfiles.optimizeUtilization.scale-down-unneeded-time | string | `"1m"` |  |
| autoscalingProfiles.optimizeUtilization.scale-down-unready-time | string | `"5m"` |  |
| autoscalingProfiles.optimizeUtilization.scale-down-utilization-threshold | string | `"0.85"` |  |
| autoscalingProfiles.optimizeUtilization.scan-interval | string | `"5s"` |  |
| autoscalingProfiles.optimizeUtilization.unremovable-node-recheck-timeout | string | `"20s"` |  |
| cloudConfig | string | `""` |  |
| cluster.hash | string | `""` |  |
| cluster.location | string | `""` |  |
| cluster.name | string | `""` |  |
| cluster.projectNumber | string | `""` |  |
| cluster.regional | bool | `true` |  |
| config.autoscalingProfile | string | `"default"` |  |
| config.envFromConfigMap | string | `""` |  |
| config.envFromSecret | string | `""` |  |
| config.extraArguments | list | `[]` |  |
| config.extraEnv | object | `{}` |  |
| config.extraEnvFromConfigMaps | object | `{}` |  |
| config.extraEnvFromSecrets | object | `{}` |  |
| config.flags.address | string | `":8085"` |  |
| config.flags.async-node-groups | string | `"true"` |  |
| config.flags.balance-similar-node-groups | string | `"true"` |  |
| config.flags.bypassed-scheduler-names | string | `"default-scheduler,gke.io/default-scheduler,gke.io/optimize-utilization-scheduler,gke.io/high-throughput-scheduler,gke.io/first-fit"` |  |
| config.flags.capacity-buffer-controller-enabled | string | `"true"` |  |
| config.flags.capacity-buffer-pod-injection-enabled | string | `"true"` |  |
| config.flags.capacity-quotas-enabled | string | `"true"` |  |
| config.flags.ccc-node-autoprovisioning-enabled | string | `"true"` |  |
| config.flags.cloud-provider | string | `"gke"` |  |
| config.flags.cores-total | string | `"0:12800000"` |  |
| config.flags.cp-max-parallel-ops | string | `"90"` |  |
| config.flags.cp-max-queued-ops | string | `"200"` |  |
| config.flags.debugging-snapshot-enabled | string | `"true"` |  |
| config.flags.defrag-candidate-node-limit | string | `"20"` |  |
| config.flags.defrag-plugins | string | `"daemonset,recycling,high-priority-migration,ek-consolidation,failed-nodes"` |  |
| config.flags.drain-priority-config | string | `"0:3600,1000000000:600"` |  |
| config.flags.dynamic-node-delete-delay-after-taint-enabled | string | `"true"` |  |
| config.flags.enable-compact-placement | string | `"true"` |  |
| config.flags.enable-compute-class-min-capacity | string | `"true"` |  |
| config.flags.enable-consumable-reservations-puller | string | `"true"` |  |
| config.flags.enable-defrag | string | `"true"` |  |
| config.flags.enable-dynamic-resource-allocation | string | `"true"` |  |
| config.flags.enable-graceful-degradation | string | `"true"` |  |
| config.flags.enable-node-pool-updates | string | `"true"` |  |
| config.flags.enable-pending-pods-metric | string | `"true"` |  |
| config.flags.enable-pending-pods-per-ccc-metric | string | `"true"` |  |
| config.flags.enable-proactive-scaleup | string | `"true"` |  |
| config.flags.enable-provisioning-requests | string | `"true"` |  |
| config.flags.enable-reservation-blocks | string | `"true"` |  |
| config.flags.enable-reservation-match | string | `"true"` |  |
| config.flags.enable-tpu-autoprovisioning | string | `"true"` |  |
| config.flags.enable-user-any-zone-selection | string | `"true"` |  |
| config.flags.enable-zone-types | string | `"true"` |  |
| config.flags.expander | string | `"edp-filter,snowflake,mppn-filter,fleet-efficiency,gke-price"` |  |
| config.flags.expendable-pods-priority-cutoff | string | `"-10"` |  |
| config.flags.fastpath-binpacking-enabled | string | `"true"` |  |
| config.flags.force-delete-failed-nodes | string | `"true"` |  |
| config.flags.force-delete-unregistered-nodes | string | `"true"` |  |
| config.flags.frequent-loops-enabled | string | `"true"` |  |
| config.flags.ignore-daemonsets-utilization | string | `"true"` |  |
| config.flags.ignore-mirror-pods-utilization | string | `"true"` |  |
| config.flags.kube-client-burst | string | `"100"` |  |
| config.flags.kube-client-qps | string | `"100"` |  |
| config.flags.logtostderr | string | `"true"` |  |
| config.flags.machine-config-enabled | string | `"true"` |  |
| config.flags.machine-serenity-labels-enabled | string | `"true"` |  |
| config.flags.max-autoprovisioned-node-group-count | string | `"999999"` |  |
| config.flags.max-node-skip-eval-time-tracker-enabled | string | `"true"` |  |
| config.flags.max-nodegroup-binpacking-duration | string | `"7s"` |  |
| config.flags.max-nodes-per-scaleup | string | `"1000"` |  |
| config.flags.max-total-unready-percentage | string | `"101"` |  |
| config.flags.memory-total | string | `"0:256000000"` |  |
| config.flags.metrics-per-ccc-enabled | string | `"true"` |  |
| config.flags.nap-default-machine-type-family | string | `"e2"` |  |
| config.flags.node-delete-delay-after-taint | string | `"1s"` |  |
| config.flags.node-info-cache-expire-time | string | `"10m"` |  |
| config.flags.node-removal-latency-tracking-enabled | string | `"true"` |  |
| config.flags.parallel-scale-up | string | `"true"` |  |
| config.flags.pod-injection-limit | string | `"5000"` |  |
| config.flags.pvm-unfitness-penalty-enabled | string | `"true"` |  |
| config.flags.scale-down-delay-after-add | string | `"0m"` |  |
| config.flags.scale-up-from-zero | string | `"true"` |  |
| config.flags.scaleup-per-ccc-metrics-enabled | string | `"true"` |  |
| config.flags.skip-nodes-with-local-storage | string | `"false"` |  |
| config.flags.startup-taint | string | `"readiness.k8s.io/gke-node-custom-script"` |  |
| config.flags.system-namespaces | string | `"kube-system,gke-gmp-system,gmp-system,gke-managed-cim,gke-managed-volumepopulator,gke-managed-checkpointing,gkebackup,gke-managed-lustrecsi,gke-managed-otel,gke-managed-mldiagnostics,gke-managed-networking-dra-driver,gke-managed-pod-snapshots,gke-managed-slurm,gke-managed-ambient"` |  |
| config.flags.v | string | `"4"` |  |
| deployment.annotations | object | `{}` | Annotations to add to the Deployment object. |
| deployment.selector | object | `{}` | Labels for Deployment `spec.selector.matchLabels`. |
| dnsConfig | object | `{}` | Pod's DNS Config (https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#pod-dns-config) |
| dnsPolicy | string | `"ClusterFirst"` | Defaults to `ClusterFirst`. Valid values: `ClusterFirstWithHostNet`, `ClusterFirst`, `Default`, or `None`. |
| fullnameOverride | string | `""` |  |
| hostNetwork | bool | `false` | Whether to expose network interfaces of the host machine to pods.  Warning: enabling hostNetwork would likely be problematic due to inability to contact GKE metadata server from autoscler pods: https://docs.cloud.google.com/kubernetes-engine/docs/concepts/workload-identity#restrictions  Alternative in case when it's required to enable host networking: https://docs.cloud.google.com/kubernetes-engine/docs/concepts/workload-identity#alternatives_to |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.pullSecrets | list | `[]` |  |
| image.repository | string | `"registry.k8s.io/autoscaling/cluster-autoscaler"` |  |
| image.tag | string | `""` |  |
| initContainers | list | `[]` | Any additional init containers. |
| livenessProbe.httpGet.path | string | `"/health-check"` |  |
| livenessProbe.httpGet.port | int | `8085` |  |
| livenessProbe.initialDelaySeconds | int | `600` |  |
| livenessProbe.periodSeconds | int | `60` |  |
| nameOverride | string | `""` |  |
| nodeSelector | object | `{}` |  |
| podAnnotations | object | `{}` |  |
| podDisruptionBudget | object | `{"annotations":{},"enabled":true,"maxUnavailable":1,"selector":{}}` | Pod disruption budget. |
| podDisruptionBudget.annotations | object | `{}` | Annotations to add to the PodDisruptionBudget. |
| podDisruptionBudget.enabled | bool | `true` | If true, creates a PodDisruptionBudget. |
| podDisruptionBudget.selector | object | `{}` | Override labels for PodDisruptionBudget `spec.selector.matchLabels`. |
| podLabels | object | `{}` |  |
| podSecurityContext.runAsGroup | int | `2049` |  |
| podSecurityContext.runAsUser | int | `2049` |  |
| podSecurityContext.seccompProfile.type | string | `"RuntimeDefault"` |  |
| priorityClassName | string | `""` |  |
| rbac.additionalRules | list | `[]` | Additional rules for role/clusterrole |
| rbac.annotations | object | `{}` | Additional annotations to add to RBAC resources (Role/RoleBinding/ClusterRole/ClusterRoleBinding). |
| rbac.create | bool | `true` | If `true`, create and use RBAC resources. |
| readinessProbe | object | `{}` |  |
| replicaCount | int | `1` |  |
| resources | object | `{}` |  |
| revisionHistoryLimit | int | `10` | The number of old ReplicaSets to retain to allow rollback. |
| securityContext.allowPrivilegeEscalation | bool | `false` |  |
| securityContext.capabilities.drop[0] | string | `"ALL"` |  |
| securityContext.readOnlyRootFilesystem | bool | `true` |  |
| server.port | int | `8085` | Port the autoscaler server listens on. |
| server.portName | string | `"http"` | Name for the server port in container spec. |
| service.annotations | object | `{}` | Annotations to add to service |
| service.clusterIP | string | `""` | IP address to assign to service |
| service.create | bool | `true` | If `true`, a Service will be created. |
| service.externalIPs | list | `[]` | List of IP addresses at which the service is available. Ref: https://kubernetes.io/docs/concepts/services-networking/service/#external-ips. |
| service.labels | object | `{}` | Labels to add to service |
| service.loadBalancerSourceRanges | list | `[]` | List of IP CIDRs allowed access to load balancer (if supported). |
| service.port | int | `8085` | Service port to expose. |
| service.selector | object | `{}` | Override labels for Service `spec.selector`. |
| service.type | string | `"ClusterIP"` | Type of service to create. |
| serviceAccount.annotations | object | `{}` |  |
| serviceAccount.automount | bool | `true` |  |
| serviceAccount.create | bool | `true` |  |
| serviceAccount.gcpWorkloadIdentity | string | `""` |  |
| serviceAccount.name | string | `""` |  |
| serviceMonitor.annotations | object | `{}` | Annotations to add to service monitor |
| serviceMonitor.enabled | bool | `false` | If true, creates a Prometheus Operator ServiceMonitor. |
| serviceMonitor.interval | string | `"10s"` | Interval that Prometheus scrapes Cluster Autoscaler metrics. |
| serviceMonitor.metricRelabelings | list | `[]` | MetricRelabelConfigs to apply to samples before ingestion. |
| serviceMonitor.namespace | string | `""` | Namespace to deploy the ServiceMonitor into. If not set, the release namespace is used. |
| serviceMonitor.path | string | `"/metrics"` | The path to scrape for metrics; autoscaler exposes `/metrics` (this is standard) |
| serviceMonitor.relabelings | list | `[]` | RelabelConfigs to apply to metrics before scraping. |
| serviceMonitor.selector | object | `{"release":"prometheus-operator"}` | Default to kube-prometheus install (CoreOS recommended), but should be set according to Prometheus install. |
| tolerations | list | `[]` |  |
| topologySpreadConstraints | list | `[]` |  |
| updateStrategy | object | `{}` |  |
| volumeMounts | list | `[]` |  |
| volumes | list | `[]` |  |

