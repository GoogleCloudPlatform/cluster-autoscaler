// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package selfservice

import (
	"cmp"
	"encoding/json"
	"slices"
	"strconv"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	container "google.golang.org/api/container/v1beta1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/klog/v2"
)

// containerdConfig is a self-service feature that enables ContainerdConfig
// for NAP-created node pools.
type containerdConfig struct {
	internalFeatureDefaultImplementation
}

func newContainerdConfig() feature {
	return &containerdConfig{}
}

func (c *containerdConfig) FromNodepool(pool *container.NodePool) Metadata {
	if pool == nil || pool.Config == nil || pool.Config.ContainerdConfig == nil {
		return nil
	}

	m := make(Metadata)
	if pool.Config.ContainerdConfig.PrivateRegistryAccessConfig != nil {
		prac := pool.Config.ContainerdConfig.PrivateRegistryAccessConfig
		m[gkelabels.ContainerdPrivateRegistryEnabledKey] = strconv.FormatBool(prac.Enabled)
		if len(prac.CertificateAuthorityDomainConfig) > 0 {
			sortedCadc := cloneAndSortCertificateAuthorityDomainConfigs(prac.CertificateAuthorityDomainConfig)
			bytes, err := json.Marshal(sortedCadc)
			if err != nil {
				klog.Errorf("Error marshalling certificateAuthorityDomainConfig from NodePool: %v", err)
			} else {
				m[gkelabels.ContainerdPrivateRegistryCAKey] = string(bytes)
			}
		}
	}

	if pool.Config.ContainerdConfig.WritableCgroups != nil {
		m[gkelabels.ContainerdWritableCgroupsKey] = strconv.FormatBool(pool.Config.ContainerdConfig.WritableCgroups.Enabled)
	}

	if len(pool.Config.ContainerdConfig.RegistryHosts) > 0 {
		sortedHosts := cloneAndSortRegistryHosts(pool.Config.ContainerdConfig.RegistryHosts)
		bytes, err := json.Marshal(sortedHosts)
		if err != nil {
			klog.Errorf("Error marshalling registryHosts from NodePool: %v", err)
		} else {
			m[gkelabels.ContainerdRegistryHostsKey] = string(bytes)
		}
	}

	if len(m) == 0 {
		return nil
	}

	return m
}

func (c *containerdConfig) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (c *containerdConfig) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig == nil || spec.NodePoolConfig.ContainerdConfig == nil {
		return nil
	}

	cccCfg := spec.NodePoolConfig.ContainerdConfig
	m := make(Metadata)

	if cccCfg.PrivateRegistryAccessConfig != nil {
		prac := cccCfg.PrivateRegistryAccessConfig
		if prac.Enabled != nil {
			m[gkelabels.ContainerdPrivateRegistryEnabledKey] = strconv.FormatBool(*prac.Enabled)
		}
		if len(prac.CertificateAuthorityDomainConfig) > 0 {
			gkeCadc := cccCertificateAuthorityDomainConfigToGke(prac.CertificateAuthorityDomainConfig)
			if len(gkeCadc) > 0 {
				bytes, err := json.Marshal(gkeCadc)
				if err != nil {
					klog.Errorf("Error marshalling certificateAuthorityDomainConfig from CCC: %v", err)
				} else {
					m[gkelabels.ContainerdPrivateRegistryCAKey] = string(bytes)
				}
			}
		}
	}

	if cccCfg.WritableCgroups != nil && cccCfg.WritableCgroups.Enabled != nil {
		m[gkelabels.ContainerdWritableCgroupsKey] = strconv.FormatBool(*cccCfg.WritableCgroups.Enabled)
	}

	if len(cccCfg.RegistryHosts) > 0 {
		gkeRh := cccRegistryHostsToGke(cccCfg.RegistryHosts)
		if len(gkeRh) > 0 {
			bytes, err := json.Marshal(gkeRh)
			if err != nil {
				klog.Errorf("Error marshalling registryHosts from CCC: %v", err)
			} else {
				m[gkelabels.ContainerdRegistryHostsKey] = string(bytes)
			}
		}
	}

	if len(m) == 0 {
		return nil
	}

	return m
}

func (c *containerdConfig) FromPriority(_ v1.Priority) Metadata {
	return nil
}

func (c *containerdConfig) ToNodePoolLabels(_ map[string]string, _ Metadata) {
}

func (c *containerdConfig) ToNodepool(pool *container.NodePool, metadata Metadata) {
	if pool == nil {
		return
	}

	var pracEnabled *bool
	if val, found := metadata[gkelabels.ContainerdPrivateRegistryEnabledKey]; found && val != "" {
		b, err := strconv.ParseBool(val)
		if err != nil {
			klog.Errorf("Error parsing private registry access enabled from metadata: %v", err)
		} else {
			pracEnabled = &b
		}
	}

	var cadc []*container.CertificateAuthorityDomainConfig
	if val, found := metadata[gkelabels.ContainerdPrivateRegistryCAKey]; found && val != "" {
		var cfg []*container.CertificateAuthorityDomainConfig
		if err := json.Unmarshal([]byte(val), &cfg); err != nil {
			klog.Errorf("Error unmarshalling certificateAuthorityDomainConfig from metadata: %v", err)
		} else {
			sortCertificateAuthorityDomainConfigs(cfg)
			cadc = cfg
		}
	}

	var wcEnabled *bool
	if val, found := metadata[gkelabels.ContainerdWritableCgroupsKey]; found && val != "" {
		b, err := strconv.ParseBool(val)
		if err != nil {
			klog.Errorf("Error parsing writable cgroups enabled from metadata: %v", err)
		} else {
			wcEnabled = &b
		}
	}

	var rh []*container.RegistryHostConfig
	if val, found := metadata[gkelabels.ContainerdRegistryHostsKey]; found && val != "" {
		var cfg []*container.RegistryHostConfig
		if err := json.Unmarshal([]byte(val), &cfg); err != nil {
			klog.Errorf("Error unmarshalling registryHosts from metadata: %v", err)
		} else {
			sortRegistryHosts(cfg)
			rh = cfg
		}
	}

	if pracEnabled == nil && len(cadc) == 0 && wcEnabled == nil && len(rh) == 0 {
		return
	}

	if pool.Config == nil {
		pool.Config = &container.NodeConfig{}
	}
	if pool.Config.ContainerdConfig == nil {
		pool.Config.ContainerdConfig = &container.ContainerdConfig{}
	}
	if pracEnabled != nil || len(cadc) > 0 {
		if pool.Config.ContainerdConfig.PrivateRegistryAccessConfig == nil {
			pool.Config.ContainerdConfig.PrivateRegistryAccessConfig = &container.PrivateRegistryAccessConfig{}
		}
		if pracEnabled != nil {
			pool.Config.ContainerdConfig.PrivateRegistryAccessConfig.Enabled = *pracEnabled
			pool.Config.ContainerdConfig.PrivateRegistryAccessConfig.ForceSendFields = append(pool.Config.ContainerdConfig.PrivateRegistryAccessConfig.ForceSendFields, "Enabled")
		}
		if len(cadc) > 0 {
			pool.Config.ContainerdConfig.PrivateRegistryAccessConfig.CertificateAuthorityDomainConfig = cadc
		}
	}
	if wcEnabled != nil {
		if pool.Config.ContainerdConfig.WritableCgroups == nil {
			pool.Config.ContainerdConfig.WritableCgroups = &container.WritableCgroups{}
		}
		pool.Config.ContainerdConfig.WritableCgroups.Enabled = *wcEnabled
		pool.Config.ContainerdConfig.WritableCgroups.ForceSendFields = append(pool.Config.ContainerdConfig.WritableCgroups.ForceSendFields, "Enabled")
	}
	if len(rh) > 0 {
		pool.Config.ContainerdConfig.RegistryHosts = rh
	}
}

func cccCertificateAuthorityDomainConfigToGke(cadcs []*v1.CertificateAuthorityDomainConfig) []*container.CertificateAuthorityDomainConfig {
	if len(cadcs) == 0 {
		return nil
	}
	var gkeCadcs []*container.CertificateAuthorityDomainConfig
	for _, cadc := range cadcs {
		if cadc == nil {
			continue
		}
		gkeCadc := &container.CertificateAuthorityDomainConfig{
			Fqdns: slices.Clone(cadc.FQDNs),
		}
		if cadc.GCPSecretManagerCertificateConfig != nil && cadc.GCPSecretManagerCertificateConfig.SecretURI != nil {
			gkeCadc.GcpSecretManagerCertificateConfig = &container.GCPSecretManagerCertificateConfig{
				SecretUri: *cadc.GCPSecretManagerCertificateConfig.SecretURI,
			}
		}
		gkeCadcs = append(gkeCadcs, gkeCadc)
	}
	sortCertificateAuthorityDomainConfigs(gkeCadcs)
	return gkeCadcs
}

func cccRegistryHostsToGke(registryHosts []*v1.RegistryHostConfig) []*container.RegistryHostConfig {
	if len(registryHosts) == 0 {
		return nil
	}
	var gkeHosts []*container.RegistryHostConfig
	for _, rh := range registryHosts {
		if rh == nil {
			continue
		}
		gkeRh := &container.RegistryHostConfig{
			Server: rh.Server,
		}
		for _, h := range rh.Hosts {
			if h == nil {
				continue
			}
			gkeH := &container.HostConfig{
				Host:         h.Host,
				Capabilities: slices.Clone(h.Capabilities),
			}
			if h.OverridePath != nil {
				gkeH.OverridePath = *h.OverridePath
			}
			if h.DialTimeout != nil {
				gkeH.DialTimeout = *h.DialTimeout
			}
			for _, hdr := range h.Header {
				if hdr == nil {
					continue
				}
				gkeH.Header = append(gkeH.Header, &container.RegistryHeader{
					Key:   hdr.Key,
					Value: slices.Clone(hdr.Value),
				})
			}
			for _, ca := range h.CA {
				if ca == nil || ca.GcpSecretManagerSecretUri == nil {
					continue
				}
				gkeH.Ca = append(gkeH.Ca, &container.CertificateConfig{
					GcpSecretManagerSecretUri: *ca.GcpSecretManagerSecretUri,
				})
			}
			for _, cl := range h.Client {
				if cl == nil {
					continue
				}
				pair := &container.CertificateConfigPair{}
				if cl.Cert != nil && cl.Cert.GcpSecretManagerSecretUri != nil {
					pair.Cert = &container.CertificateConfig{
						GcpSecretManagerSecretUri: *cl.Cert.GcpSecretManagerSecretUri,
					}
				}
				if cl.Key != nil && cl.Key.GcpSecretManagerSecretUri != nil {
					pair.Key = &container.CertificateConfig{
						GcpSecretManagerSecretUri: *cl.Key.GcpSecretManagerSecretUri,
					}
				}
				if pair.Cert != nil || pair.Key != nil {
					gkeH.Client = append(gkeH.Client, pair)
				}
			}
			gkeRh.Hosts = append(gkeRh.Hosts, gkeH)
		}
		gkeHosts = append(gkeHosts, gkeRh)
	}
	sortRegistryHosts(gkeHosts)
	return gkeHosts
}

func cmpBool(a, b bool) int {
	if a == b {
		return 0
	}
	if a {
		return 1
	}
	return -1
}

func cmpCertificateAuthorityDomainConfig(a, b *container.CertificateAuthorityDomainConfig) int {
	if a == b {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	uriA, uriB := "", ""
	if a.GcpSecretManagerCertificateConfig != nil {
		uriA = a.GcpSecretManagerCertificateConfig.SecretUri
	}
	if b.GcpSecretManagerCertificateConfig != nil {
		uriB = b.GcpSecretManagerCertificateConfig.SecretUri
	}

	return cmp.Or(
		slices.Compare(a.Fqdns, b.Fqdns),
		cmp.Compare(uriA, uriB),
	)
}

func sortCertificateAuthorityDomainConfigs(cadcs []*container.CertificateAuthorityDomainConfig) {
	for _, cadc := range cadcs {
		if cadc != nil {
			slices.Sort(cadc.Fqdns)
		}
	}
	slices.SortFunc(cadcs, cmpCertificateAuthorityDomainConfig)
}

func cloneAndSortCertificateAuthorityDomainConfigs(cadcs []*container.CertificateAuthorityDomainConfig) []*container.CertificateAuthorityDomainConfig {
	if len(cadcs) == 0 {
		return nil
	}
	cloned := make([]*container.CertificateAuthorityDomainConfig, len(cadcs))
	for i, cadc := range cadcs {
		if cadc == nil {
			continue
		}
		c := *cadc
		c.Fqdns = slices.Clone(cadc.Fqdns)
		cloned[i] = &c
	}
	sortCertificateAuthorityDomainConfigs(cloned)
	return cloned
}

func cmpRegistryHeader(a, b *container.RegistryHeader) int {
	if a == b {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	return cmp.Or(
		cmp.Compare(a.Key, b.Key),
		slices.Compare(a.Value, b.Value),
	)
}

func cmpCertificateConfig(a, b *container.CertificateConfig) int {
	if a == b {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	return cmp.Compare(a.GcpSecretManagerSecretUri, b.GcpSecretManagerSecretUri)
}

func cmpCertificateConfigPair(a, b *container.CertificateConfigPair) int {
	if a == b {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	var certA, certB, keyA, keyB *container.CertificateConfig
	if a != nil {
		certA, keyA = a.Cert, a.Key
	}
	if b != nil {
		certB, keyB = b.Cert, b.Key
	}

	return cmp.Or(
		cmpCertificateConfig(certA, certB),
		cmpCertificateConfig(keyA, keyB),
	)
}

func cmpHostConfig(a, b *container.HostConfig) int {
	if a == b {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	return cmp.Or(
		cmp.Compare(a.Host, b.Host),
		cmpBool(a.OverridePath, b.OverridePath),
		cmp.Compare(a.DialTimeout, b.DialTimeout),
		slices.Compare(a.Capabilities, b.Capabilities),
		slices.CompareFunc(a.Header, b.Header, cmpRegistryHeader),
		slices.CompareFunc(a.Ca, b.Ca, cmpCertificateConfig),
		slices.CompareFunc(a.Client, b.Client, cmpCertificateConfigPair),
	)
}

func cmpRegistryHostConfig(a, b *container.RegistryHostConfig) int {
	if a == b {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	return cmp.Or(
		cmp.Compare(a.Server, b.Server),
		slices.CompareFunc(a.Hosts, b.Hosts, cmpHostConfig),
	)
}

func sortRegistryHosts(rhs []*container.RegistryHostConfig) {
	for _, rh := range rhs {
		if rh == nil {
			continue
		}
		for _, h := range rh.Hosts {
			if h == nil {
				continue
			}
			slices.Sort(h.Capabilities)
			for _, hdr := range h.Header {
				if hdr != nil {
					slices.Sort(hdr.Value)
				}
			}
			slices.SortFunc(h.Header, cmpRegistryHeader)
			slices.SortFunc(h.Ca, cmpCertificateConfig)
			slices.SortFunc(h.Client, cmpCertificateConfigPair)
		}
		slices.SortFunc(rh.Hosts, cmpHostConfig)
	}
	slices.SortFunc(rhs, cmpRegistryHostConfig)
}

func cloneAndSortRegistryHosts(rhs []*container.RegistryHostConfig) []*container.RegistryHostConfig {
	if len(rhs) == 0 {
		return nil
	}
	cloned := make([]*container.RegistryHostConfig, len(rhs))
	for i, rh := range rhs {
		if rh == nil {
			continue
		}
		rhCopy := *rh
		if len(rh.Hosts) > 0 {
			rhCopy.Hosts = make([]*container.HostConfig, len(rh.Hosts))
			for j, h := range rh.Hosts {
				if h == nil {
					continue
				}
				hCopy := *h
				hCopy.Capabilities = slices.Clone(h.Capabilities)
				if len(h.Header) > 0 {
					hCopy.Header = make([]*container.RegistryHeader, len(h.Header))
					for k, hdr := range h.Header {
						if hdr == nil {
							continue
						}
						hdrCopy := *hdr
						hdrCopy.Value = slices.Clone(hdr.Value)
						hCopy.Header[k] = &hdrCopy
					}
				}
				hCopy.Ca = slices.Clone(h.Ca)
				hCopy.Client = slices.Clone(h.Client)
				rhCopy.Hosts[j] = &hCopy
			}
		}
		cloned[i] = &rhCopy
	}
	sortRegistryHosts(cloned)
	return cloned
}
