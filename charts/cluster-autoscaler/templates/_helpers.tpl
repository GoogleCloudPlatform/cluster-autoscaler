{{/*
Expand the name of the chart.
*/}}
{{- define "cluster-autoscaler.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "cluster-autoscaler.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Placeholder version checked into the repository (see Chart.yaml).
Rendering the chart while this value is still in place is a hard error.
*/}}
{{- define "cluster-autoscaler.placeholderVersion" -}}
0.0.0-placeholder
{{- end }}

{{/*
Fail the render if the given Chart.yaml field still holds the placeholder version.
Usage: {{ include "cluster-autoscaler.requireRealVersion" (dict "field" "version" "value" .Chart.Version) }}
*/}}
{{- define "cluster-autoscaler.requireRealVersion" -}}
{{- $placeholder := include "cluster-autoscaler.placeholderVersion" . }}
{{- if eq .value $placeholder }}
{{- fail (printf "Chart.yaml %s is the placeholder %q and must be replaced with a real version before the chart can be rendered. The repository checkout is not installable as-is: package it with a real version first, e.g. `helm package --version 37.234.0 --app-version 37.234.0 .`, or install the published chart package." .field .value) }}
{{- end }}
{{- end }}

{{/*
Chart version, validated to not be the placeholder.
Always use this instead of .Chart.Version in templates.
*/}}
{{- define "cluster-autoscaler.version" -}}
{{- include "cluster-autoscaler.requireRealVersion" (dict "field" "version" "value" .Chart.Version) }}
{{- .Chart.Version }}
{{- end }}

{{/*
Application version, validated to not be the placeholder.
Always use this instead of .Chart.AppVersion in templates.
*/}}
{{- define "cluster-autoscaler.appVersion" -}}
{{- include "cluster-autoscaler.requireRealVersion" (dict "field" "appVersion" "value" .Chart.AppVersion) }}
{{- .Chart.AppVersion }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "cluster-autoscaler.chart" -}}
{{- printf "%s-%s" .Chart.Name (include "cluster-autoscaler.version" .) | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "cluster-autoscaler.labels" -}}
helm.sh/chart: {{ include "cluster-autoscaler.chart" . }}
{{ include "cluster-autoscaler.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ include "cluster-autoscaler.appVersion" . | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.additionalLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "cluster-autoscaler.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cluster-autoscaler.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "cluster-autoscaler.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "cluster-autoscaler.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the experiments configmap to use
*/}}
{{- define "cluster-autoscaler.experimentsConfigMap" -}}
{{- include "cluster-autoscaler.fullname" . }}-experiments
{{- end }}
