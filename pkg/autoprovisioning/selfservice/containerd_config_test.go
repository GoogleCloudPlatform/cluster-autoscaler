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
	"testing"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	container "google.golang.org/api/container/v1beta1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/utils/ptr"
)

func TestContainerdConfig(t *testing.T) {
	feature := newContainerdConfig()

	t.Run("FromCccSpec - empty or nil", func(t *testing.T) {
		assert.Nil(t, feature.FromCccSpec(v1.ComputeClassSpec{}))
		assert.Nil(t, feature.FromCccSpec(v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{},
		}))
		assert.Nil(t, feature.FromCccSpec(v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ContainerdConfig: &v1.ContainerdConfig{},
			},
		}))
	})

	t.Run("FromCccSpec - writable cgroups", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ContainerdConfig: &v1.ContainerdConfig{
					WritableCgroups: &v1.WritableCgroups{
						Enabled: ptr.To(true),
					},
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			gkelabels.ContainerdWritableCgroupsKey: "true",
		}, metadata)
	})

	t.Run("FromCccSpec - writable cgroups explicitly false", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ContainerdConfig: &v1.ContainerdConfig{
					WritableCgroups: &v1.WritableCgroups{
						Enabled: ptr.To(false),
					},
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			gkelabels.ContainerdWritableCgroupsKey: "false",
		}, metadata)
	})

	t.Run("FromCccSpec - private registry access explicitly false", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ContainerdConfig: &v1.ContainerdConfig{
					PrivateRegistryAccessConfig: &v1.PrivateRegistryAccessConfig{
						Enabled: ptr.To(false),
					},
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			gkelabels.ContainerdPrivateRegistryEnabledKey: "false",
		}, metadata)
	})

	t.Run("FromCccSpec - private registry access only certificates", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ContainerdConfig: &v1.ContainerdConfig{
					PrivateRegistryAccessConfig: &v1.PrivateRegistryAccessConfig{
						CertificateAuthorityDomainConfig: []*v1.CertificateAuthorityDomainConfig{
							{
								FQDNs: []string{"registry.example.com"},
								GCPSecretManagerCertificateConfig: &v1.GCPSecretManagerCertificateConfig{
									SecretURI: ptr.To("projects/p/secrets/s/versions/1"),
								},
							},
						},
					},
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			gkelabels.ContainerdPrivateRegistryCAKey: `[{"fqdns":["registry.example.com"],"gcpSecretManagerCertificateConfig":{"secretUri":"projects/p/secrets/s/versions/1"}}]`,
		}, metadata)
	})

	fullCccSpec := v1.ComputeClassSpec{
		NodePoolConfig: &v1.NodePoolConfig{
			ContainerdConfig: &v1.ContainerdConfig{
				PrivateRegistryAccessConfig: &v1.PrivateRegistryAccessConfig{
					Enabled: ptr.To(true),
					CertificateAuthorityDomainConfig: []*v1.CertificateAuthorityDomainConfig{
						{
							FQDNs: []string{"registry.example.com"},
							GCPSecretManagerCertificateConfig: &v1.GCPSecretManagerCertificateConfig{
								SecretURI: ptr.To("projects/p/secrets/s/versions/1"),
							},
						},
					},
				},
				WritableCgroups: &v1.WritableCgroups{
					Enabled: ptr.To(true),
				},
				RegistryHosts: []*v1.RegistryHostConfig{
					{
						Server: "registry.example.com",
						Hosts: []*v1.HostConfig{
							{
								Host:         "mirror.example.com",
								Capabilities: []string{"HOST_CAPABILITY_PULL"},
								OverridePath: ptr.To(true),
								DialTimeout:  ptr.To("30s"),
								Header: []*v1.HostHeader{
									{
										Key:   "X-Custom-Header",
										Value: []string{"value1", "value2"},
									},
								},
								CA: []*v1.RegistryHostCertificateConfig{
									{
										GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/ca/versions/1"),
									},
								},
								Client: []*v1.RegistryHostClientCertificateConfig{
									{
										Cert: &v1.RegistryHostCertificateConfig{
											GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/cert/versions/1"),
										},
										Key: &v1.RegistryHostCertificateConfig{
											GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/key/versions/1"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	expectedFullMetadata := Metadata{
		gkelabels.ContainerdPrivateRegistryEnabledKey: "true",
		gkelabels.ContainerdPrivateRegistryCAKey:      `[{"fqdns":["registry.example.com"],"gcpSecretManagerCertificateConfig":{"secretUri":"projects/p/secrets/s/versions/1"}}]`,
		gkelabels.ContainerdWritableCgroupsKey:        "true",
		gkelabels.ContainerdRegistryHostsKey:          `[{"hosts":[{"ca":[{"gcpSecretManagerSecretUri":"projects/p/secrets/ca/versions/1"}],"capabilities":["HOST_CAPABILITY_PULL"],"client":[{"cert":{"gcpSecretManagerSecretUri":"projects/p/secrets/cert/versions/1"},"key":{"gcpSecretManagerSecretUri":"projects/p/secrets/key/versions/1"}}],"dialTimeout":"30s","header":[{"key":"X-Custom-Header","value":["value1","value2"]}],"host":"mirror.example.com","overridePath":true}],"server":"registry.example.com"}]`,
	}

	t.Run("FromCccSpec - full config", func(t *testing.T) {
		metadata := feature.FromCccSpec(fullCccSpec)
		assert.Equal(t, expectedFullMetadata, metadata)
	})

	t.Run("FromCccSpec - overridePath false", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ContainerdConfig: &v1.ContainerdConfig{
					RegistryHosts: []*v1.RegistryHostConfig{
						{
							Server: "registry.example.com",
							Hosts: []*v1.HostConfig{
								{
									Host:         "mirror.example.com",
									OverridePath: ptr.To(false),
								},
							},
						},
					},
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			gkelabels.ContainerdRegistryHostsKey: `[{"hosts":[{"host":"mirror.example.com"}],"server":"registry.example.com"}]`,
		}, metadata)
	})

	t.Run("FromCccSpec - overridePath true", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ContainerdConfig: &v1.ContainerdConfig{
					RegistryHosts: []*v1.RegistryHostConfig{
						{
							Server: "registry.example.com",
							Hosts: []*v1.HostConfig{
								{
									Host:         "mirror.example.com",
									OverridePath: ptr.To(true),
								},
							},
						},
					},
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			gkelabels.ContainerdRegistryHostsKey: `[{"hosts":[{"host":"mirror.example.com","overridePath":true}],"server":"registry.example.com"}]`,
		}, metadata)
	})

	t.Run("FromNodepool - empty or nil", func(t *testing.T) {
		assert.Nil(t, feature.FromNodepool(nil))
		assert.Nil(t, feature.FromNodepool(&container.NodePool{}))
		assert.Nil(t, feature.FromNodepool(&container.NodePool{
			Config: &container.NodeConfig{},
		}))
		assert.Nil(t, feature.FromNodepool(&container.NodePool{
			Config: &container.NodeConfig{
				ContainerdConfig: &container.ContainerdConfig{},
			},
		}))
	})

	t.Run("FromNodepool - writable cgroups", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{
				ContainerdConfig: &container.ContainerdConfig{
					WritableCgroups: &container.WritableCgroups{
						Enabled: true,
					},
				},
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, Metadata{
			gkelabels.ContainerdWritableCgroupsKey: "true",
		}, metadata)
	})

	t.Run("FromNodepool - full config", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{
				ContainerdConfig: &container.ContainerdConfig{
					PrivateRegistryAccessConfig: &container.PrivateRegistryAccessConfig{
						Enabled: true,
						CertificateAuthorityDomainConfig: []*container.CertificateAuthorityDomainConfig{
							{
								Fqdns: []string{"registry.example.com"},
								GcpSecretManagerCertificateConfig: &container.GCPSecretManagerCertificateConfig{
									SecretUri: "projects/p/secrets/s/versions/1",
								},
							},
						},
					},
					WritableCgroups: &container.WritableCgroups{
						Enabled: true,
					},
					RegistryHosts: []*container.RegistryHostConfig{
						{
							Server: "registry.example.com",
							Hosts: []*container.HostConfig{
								{
									Host:         "mirror.example.com",
									Capabilities: []string{"HOST_CAPABILITY_PULL"},
									OverridePath: true,
									DialTimeout:  "30s",
									Header: []*container.RegistryHeader{
										{
											Key:   "X-Custom-Header",
											Value: []string{"value1", "value2"},
										},
									},
									Ca: []*container.CertificateConfig{
										{
											GcpSecretManagerSecretUri: "projects/p/secrets/ca/versions/1",
										},
									},
									Client: []*container.CertificateConfigPair{
										{
											Cert: &container.CertificateConfig{
												GcpSecretManagerSecretUri: "projects/p/secrets/cert/versions/1",
											},
											Key: &container.CertificateConfig{
												GcpSecretManagerSecretUri: "projects/p/secrets/key/versions/1",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, expectedFullMetadata, metadata)
	})

	t.Run("ToNodepool - apply to empty nodepool", func(t *testing.T) {
		np := &container.NodePool{}
		feature.ToNodepool(np, Metadata{gkelabels.ContainerdWritableCgroupsKey: "true"})
		assert.NotNil(t, np.Config)
		assert.NotNil(t, np.Config.ContainerdConfig)
		assert.Equal(t, &container.ContainerdConfig{
			WritableCgroups: &container.WritableCgroups{
				Enabled:         true,
				ForceSendFields: []string{"Enabled"},
			},
		}, np.Config.ContainerdConfig)
	})

	t.Run("ToNodepool - apply full config", func(t *testing.T) {
		np := &container.NodePool{}
		feature.ToNodepool(np, expectedFullMetadata)
		assert.NotNil(t, np.Config)
		assert.NotNil(t, np.Config.ContainerdConfig)
		assert.Equal(t, &container.ContainerdConfig{
			PrivateRegistryAccessConfig: &container.PrivateRegistryAccessConfig{
				Enabled: true,
				CertificateAuthorityDomainConfig: []*container.CertificateAuthorityDomainConfig{
					{
						Fqdns: []string{"registry.example.com"},
						GcpSecretManagerCertificateConfig: &container.GCPSecretManagerCertificateConfig{
							SecretUri: "projects/p/secrets/s/versions/1",
						},
					},
				},
				ForceSendFields: []string{"Enabled"},
			},
			WritableCgroups: &container.WritableCgroups{
				Enabled:         true,
				ForceSendFields: []string{"Enabled"},
			},
			RegistryHosts: []*container.RegistryHostConfig{
				{
					Server: "registry.example.com",
					Hosts: []*container.HostConfig{
						{
							Host:         "mirror.example.com",
							Capabilities: []string{"HOST_CAPABILITY_PULL"},
							OverridePath: true,
							DialTimeout:  "30s",
							Header: []*container.RegistryHeader{
								{
									Key:   "X-Custom-Header",
									Value: []string{"value1", "value2"},
								},
							},
							Ca: []*container.CertificateConfig{
								{
									GcpSecretManagerSecretUri: "projects/p/secrets/ca/versions/1",
								},
							},
							Client: []*container.CertificateConfigPair{
								{
									Cert: &container.CertificateConfig{
										GcpSecretManagerSecretUri: "projects/p/secrets/cert/versions/1",
									},
									Key: &container.CertificateConfig{
										GcpSecretManagerSecretUri: "projects/p/secrets/key/versions/1",
									},
								},
							},
						},
					},
				},
			},
		}, np.Config.ContainerdConfig)
	})

	t.Run("Roundtrip - CCC to Nodepool to Metadata", func(t *testing.T) {
		metadataFromCcc := feature.FromCccSpec(fullCccSpec)
		np := &container.NodePool{}
		feature.ToNodepool(np, metadataFromCcc)
		metadataFromNp := feature.FromNodepool(np)
		assert.Equal(t, metadataFromCcc, metadataFromNp)
	})

	t.Run("ToNodepool - empty or missing metadata", func(t *testing.T) {
		np := &container.NodePool{}
		feature.ToNodepool(np, Metadata{})
		assert.Nil(t, np.Config)

		feature.ToNodepool(np, Metadata{gkelabels.ContainerdWritableCgroupsKey: ""})
		assert.Nil(t, np.Config)

		feature.ToNodepool(np, Metadata{gkelabels.ContainerdWritableCgroupsKey: "invalid bool"})
		assert.Nil(t, np.Config)

		feature.ToNodepool(np, Metadata{gkelabels.ContainerdPrivateRegistryEnabledKey: "invalid bool"})
		assert.Nil(t, np.Config)

		feature.ToNodepool(np, Metadata{gkelabels.ContainerdPrivateRegistryCAKey: "invalid json"})
		assert.Nil(t, np.Config)
	})

	t.Run("FromPriority and FromLabelRequirements return nil", func(t *testing.T) {
		assert.Nil(t, feature.FromPriority(v1.Priority{}))
		assert.Nil(t, feature.FromLabelRequirements(podrequirements.LabelRequirements{}))
	})

	t.Run("ToNodePoolLabels is no-op", func(t *testing.T) {
		labels := map[string]string{}
		feature.ToNodePoolLabels(labels, Metadata{gkelabels.ContainerdWritableCgroupsKey: "test"})
		assert.Empty(t, labels)
	})

	t.Run("FromCccSpec - CertificateAuthorityDomainConfig arrays and nested Fqdns order does not matter", func(t *testing.T) {
		cccSpec1 := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ContainerdConfig: &v1.ContainerdConfig{
					PrivateRegistryAccessConfig: &v1.PrivateRegistryAccessConfig{
						Enabled: ptr.To(true),
						CertificateAuthorityDomainConfig: []*v1.CertificateAuthorityDomainConfig{
							{
								FQDNs: []string{"z-domain.com", "a-domain.com"},
								GCPSecretManagerCertificateConfig: &v1.GCPSecretManagerCertificateConfig{
									SecretURI: ptr.To("projects/p/secrets/s1/versions/1"),
								},
							},
							{
								FQDNs: []string{"m-domain.com"},
								GCPSecretManagerCertificateConfig: &v1.GCPSecretManagerCertificateConfig{
									SecretURI: ptr.To("projects/p/secrets/s2/versions/1"),
								},
							},
						},
					},
				},
			},
		}

		cccSpec2 := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ContainerdConfig: &v1.ContainerdConfig{
					PrivateRegistryAccessConfig: &v1.PrivateRegistryAccessConfig{
						Enabled: ptr.To(true),
						CertificateAuthorityDomainConfig: []*v1.CertificateAuthorityDomainConfig{
							{
								FQDNs: []string{"m-domain.com"},
								GCPSecretManagerCertificateConfig: &v1.GCPSecretManagerCertificateConfig{
									SecretURI: ptr.To("projects/p/secrets/s2/versions/1"),
								},
							},
							{
								FQDNs: []string{"a-domain.com", "z-domain.com"},
								GCPSecretManagerCertificateConfig: &v1.GCPSecretManagerCertificateConfig{
									SecretURI: ptr.To("projects/p/secrets/s1/versions/1"),
								},
							},
						},
					},
				},
			},
		}

		meta1 := feature.FromCccSpec(cccSpec1)
		meta2 := feature.FromCccSpec(cccSpec2)
		assert.Equal(t, meta1, meta2)
	})

	t.Run("FromNodepool - CertificateAuthorityDomainConfig arrays and nested Fqdns order does not matter", func(t *testing.T) {
		np1 := &container.NodePool{
			Config: &container.NodeConfig{
				ContainerdConfig: &container.ContainerdConfig{
					PrivateRegistryAccessConfig: &container.PrivateRegistryAccessConfig{
						Enabled: true,
						CertificateAuthorityDomainConfig: []*container.CertificateAuthorityDomainConfig{
							{
								Fqdns: []string{"z-domain.com", "a-domain.com"},
								GcpSecretManagerCertificateConfig: &container.GCPSecretManagerCertificateConfig{
									SecretUri: "projects/p/secrets/s1/versions/1",
								},
							},
							{
								Fqdns: []string{"m-domain.com"},
								GcpSecretManagerCertificateConfig: &container.GCPSecretManagerCertificateConfig{
									SecretUri: "projects/p/secrets/s2/versions/1",
								},
							},
						},
					},
				},
			},
		}

		np2 := &container.NodePool{
			Config: &container.NodeConfig{
				ContainerdConfig: &container.ContainerdConfig{
					PrivateRegistryAccessConfig: &container.PrivateRegistryAccessConfig{
						Enabled: true,
						CertificateAuthorityDomainConfig: []*container.CertificateAuthorityDomainConfig{
							{
								Fqdns: []string{"m-domain.com"},
								GcpSecretManagerCertificateConfig: &container.GCPSecretManagerCertificateConfig{
									SecretUri: "projects/p/secrets/s2/versions/1",
								},
							},
							{
								Fqdns: []string{"a-domain.com", "z-domain.com"},
								GcpSecretManagerCertificateConfig: &container.GCPSecretManagerCertificateConfig{
									SecretUri: "projects/p/secrets/s1/versions/1",
								},
							},
						},
					},
				},
			},
		}

		meta1 := feature.FromNodepool(np1)
		meta2 := feature.FromNodepool(np2)
		assert.Equal(t, meta1, meta2)
	})

	t.Run("FromCccSpec - RegistryHosts high levels and nested arrays order does not matter", func(t *testing.T) {
		cccSpec1 := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ContainerdConfig: &v1.ContainerdConfig{
					RegistryHosts: []*v1.RegistryHostConfig{
						{
							Server: "server-b.com",
							Hosts: []*v1.HostConfig{
								{
									Host:         "mirror-2.com",
									Capabilities: []string{"HOST_CAPABILITY_RESOLVE", "HOST_CAPABILITY_PULL"},
									Header: []*v1.HostHeader{
										{Key: "Header-Z", Value: []string{"val-2", "val-1"}},
										{Key: "Header-A", Value: []string{"val-a"}},
									},
									CA: []*v1.RegistryHostCertificateConfig{
										{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/ca-2/versions/1")},
										{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/ca-1/versions/1")},
									},
									Client: []*v1.RegistryHostClientCertificateConfig{
										{
											Cert: &v1.RegistryHostCertificateConfig{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/cert-2/versions/1")},
											Key:  &v1.RegistryHostCertificateConfig{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/key-2/versions/1")},
										},
										{
											Cert: &v1.RegistryHostCertificateConfig{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/cert-1/versions/1")},
											Key:  &v1.RegistryHostCertificateConfig{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/key-1/versions/1")},
										},
									},
								},
								{
									Host: "mirror-1.com",
								},
							},
						},
						{
							Server: "server-a.com",
							Hosts: []*v1.HostConfig{
								{Host: "mirror-a.com"},
							},
						},
					},
				},
			},
		}

		cccSpec2 := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ContainerdConfig: &v1.ContainerdConfig{
					RegistryHosts: []*v1.RegistryHostConfig{
						{
							Server: "server-a.com",
							Hosts: []*v1.HostConfig{
								{Host: "mirror-a.com"},
							},
						},
						{
							Server: "server-b.com",
							Hosts: []*v1.HostConfig{
								{
									Host: "mirror-1.com",
								},
								{
									Host:         "mirror-2.com",
									Capabilities: []string{"HOST_CAPABILITY_PULL", "HOST_CAPABILITY_RESOLVE"},
									Header: []*v1.HostHeader{
										{Key: "Header-A", Value: []string{"val-a"}},
										{Key: "Header-Z", Value: []string{"val-1", "val-2"}},
									},
									CA: []*v1.RegistryHostCertificateConfig{
										{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/ca-1/versions/1")},
										{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/ca-2/versions/1")},
									},
									Client: []*v1.RegistryHostClientCertificateConfig{
										{
											Cert: &v1.RegistryHostCertificateConfig{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/cert-1/versions/1")},
											Key:  &v1.RegistryHostCertificateConfig{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/key-1/versions/1")},
										},
										{
											Cert: &v1.RegistryHostCertificateConfig{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/cert-2/versions/1")},
											Key:  &v1.RegistryHostCertificateConfig{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/key-2/versions/1")},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		meta1 := feature.FromCccSpec(cccSpec1)
		meta2 := feature.FromCccSpec(cccSpec2)
		assert.Equal(t, meta1, meta2)
	})

	t.Run("FromNodepool - RegistryHosts high levels and nested arrays order does not matter", func(t *testing.T) {
		np1 := &container.NodePool{
			Config: &container.NodeConfig{
				ContainerdConfig: &container.ContainerdConfig{
					RegistryHosts: []*container.RegistryHostConfig{
						{
							Server: "server-b.com",
							Hosts: []*container.HostConfig{
								{
									Host:         "mirror-2.com",
									Capabilities: []string{"HOST_CAPABILITY_RESOLVE", "HOST_CAPABILITY_PULL"},
									Header: []*container.RegistryHeader{
										{Key: "Header-Z", Value: []string{"val-2", "val-1"}},
										{Key: "Header-A", Value: []string{"val-a"}},
									},
									Ca: []*container.CertificateConfig{
										{GcpSecretManagerSecretUri: "projects/p/secrets/ca-2/versions/1"},
										{GcpSecretManagerSecretUri: "projects/p/secrets/ca-1/versions/1"},
									},
									Client: []*container.CertificateConfigPair{
										{
											Cert: &container.CertificateConfig{GcpSecretManagerSecretUri: "projects/p/secrets/cert-2/versions/1"},
											Key:  &container.CertificateConfig{GcpSecretManagerSecretUri: "projects/p/secrets/key-2/versions/1"},
										},
										{
											Cert: &container.CertificateConfig{GcpSecretManagerSecretUri: "projects/p/secrets/cert-1/versions/1"},
											Key:  &container.CertificateConfig{GcpSecretManagerSecretUri: "projects/p/secrets/key-1/versions/1"},
										},
									},
								},
								{
									Host: "mirror-1.com",
								},
							},
						},
						{
							Server: "server-a.com",
							Hosts: []*container.HostConfig{
								{Host: "mirror-a.com"},
							},
						},
					},
				},
			},
		}

		np2 := &container.NodePool{
			Config: &container.NodeConfig{
				ContainerdConfig: &container.ContainerdConfig{
					RegistryHosts: []*container.RegistryHostConfig{
						{
							Server: "server-a.com",
							Hosts: []*container.HostConfig{
								{Host: "mirror-a.com"},
							},
						},
						{
							Server: "server-b.com",
							Hosts: []*container.HostConfig{
								{
									Host: "mirror-1.com",
								},
								{
									Host:         "mirror-2.com",
									Capabilities: []string{"HOST_CAPABILITY_PULL", "HOST_CAPABILITY_RESOLVE"},
									Header: []*container.RegistryHeader{
										{Key: "Header-A", Value: []string{"val-a"}},
										{Key: "Header-Z", Value: []string{"val-1", "val-2"}},
									},
									Ca: []*container.CertificateConfig{
										{GcpSecretManagerSecretUri: "projects/p/secrets/ca-1/versions/1"},
										{GcpSecretManagerSecretUri: "projects/p/secrets/ca-2/versions/1"},
									},
									Client: []*container.CertificateConfigPair{
										{
											Cert: &container.CertificateConfig{GcpSecretManagerSecretUri: "projects/p/secrets/cert-1/versions/1"},
											Key:  &container.CertificateConfig{GcpSecretManagerSecretUri: "projects/p/secrets/key-1/versions/1"},
										},
										{
											Cert: &container.CertificateConfig{GcpSecretManagerSecretUri: "projects/p/secrets/cert-2/versions/1"},
											Key:  &container.CertificateConfig{GcpSecretManagerSecretUri: "projects/p/secrets/key-2/versions/1"},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		meta1 := feature.FromNodepool(np1)
		meta2 := feature.FromNodepool(np2)
		assert.Equal(t, meta1, meta2)
	})

	t.Run("FromCccSpec and FromNodepool - CertificateAuthorityDomainConfig arrays and nested Fqdns order does not matter", func(t *testing.T) {
		cccSpec := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ContainerdConfig: &v1.ContainerdConfig{
					PrivateRegistryAccessConfig: &v1.PrivateRegistryAccessConfig{
						Enabled: ptr.To(true),
						CertificateAuthorityDomainConfig: []*v1.CertificateAuthorityDomainConfig{
							{
								FQDNs: []string{"z-domain.com", "a-domain.com"},
								GCPSecretManagerCertificateConfig: &v1.GCPSecretManagerCertificateConfig{
									SecretURI: ptr.To("projects/p/secrets/s1/versions/1"),
								},
							},
							{
								FQDNs: []string{"m-domain.com"},
								GCPSecretManagerCertificateConfig: &v1.GCPSecretManagerCertificateConfig{
									SecretURI: ptr.To("projects/p/secrets/s2/versions/1"),
								},
							},
						},
					},
				},
			},
		}

		np := &container.NodePool{
			Config: &container.NodeConfig{
				ContainerdConfig: &container.ContainerdConfig{
					PrivateRegistryAccessConfig: &container.PrivateRegistryAccessConfig{
						Enabled: true,
						CertificateAuthorityDomainConfig: []*container.CertificateAuthorityDomainConfig{
							{
								Fqdns: []string{"m-domain.com"},
								GcpSecretManagerCertificateConfig: &container.GCPSecretManagerCertificateConfig{
									SecretUri: "projects/p/secrets/s2/versions/1",
								},
							},
							{
								Fqdns: []string{"a-domain.com", "z-domain.com"},
								GcpSecretManagerCertificateConfig: &container.GCPSecretManagerCertificateConfig{
									SecretUri: "projects/p/secrets/s1/versions/1",
								},
							},
						},
					},
				},
			},
		}

		cccMeta := feature.FromCccSpec(cccSpec)
		npMeta := feature.FromNodepool(np)
		assert.Equal(t, cccMeta, npMeta)
	})

	t.Run("FromCccSpec and FromNodepool - RegistryHosts high levels and nested arrays order does not matter", func(t *testing.T) {
		cccSpec := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ContainerdConfig: &v1.ContainerdConfig{
					RegistryHosts: []*v1.RegistryHostConfig{
						{
							Server: "server-b.com",
							Hosts: []*v1.HostConfig{
								{
									Host:         "mirror-2.com",
									Capabilities: []string{"HOST_CAPABILITY_RESOLVE", "HOST_CAPABILITY_PULL"},
									Header: []*v1.HostHeader{
										{Key: "Header-Z", Value: []string{"val-2", "val-1"}},
										{Key: "Header-A", Value: []string{"val-a"}},
									},
									CA: []*v1.RegistryHostCertificateConfig{
										{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/ca-2/versions/1")},
										{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/ca-1/versions/1")},
									},
									Client: []*v1.RegistryHostClientCertificateConfig{
										{
											Cert: &v1.RegistryHostCertificateConfig{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/cert-2/versions/1")},
											Key:  &v1.RegistryHostCertificateConfig{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/key-2/versions/1")},
										},
										{
											Cert: &v1.RegistryHostCertificateConfig{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/cert-1/versions/1")},
											Key:  &v1.RegistryHostCertificateConfig{GcpSecretManagerSecretUri: ptr.To("projects/p/secrets/key-1/versions/1")},
										},
									},
								},
								{
									Host: "mirror-1.com",
								},
							},
						},
						{
							Server: "server-a.com",
							Hosts: []*v1.HostConfig{
								{Host: "mirror-a.com"},
							},
						},
					},
				},
			},
		}

		np := &container.NodePool{
			Config: &container.NodeConfig{
				ContainerdConfig: &container.ContainerdConfig{
					RegistryHosts: []*container.RegistryHostConfig{
						{
							Server: "server-a.com",
							Hosts: []*container.HostConfig{
								{Host: "mirror-a.com"},
							},
						},
						{
							Server: "server-b.com",
							Hosts: []*container.HostConfig{
								{
									Host: "mirror-1.com",
								},
								{
									Host:         "mirror-2.com",
									Capabilities: []string{"HOST_CAPABILITY_PULL", "HOST_CAPABILITY_RESOLVE"},
									Header: []*container.RegistryHeader{
										{Key: "Header-A", Value: []string{"val-a"}},
										{Key: "Header-Z", Value: []string{"val-1", "val-2"}},
									},
									Ca: []*container.CertificateConfig{
										{GcpSecretManagerSecretUri: "projects/p/secrets/ca-1/versions/1"},
										{GcpSecretManagerSecretUri: "projects/p/secrets/ca-2/versions/1"},
									},
									Client: []*container.CertificateConfigPair{
										{
											Cert: &container.CertificateConfig{GcpSecretManagerSecretUri: "projects/p/secrets/cert-1/versions/1"},
											Key:  &container.CertificateConfig{GcpSecretManagerSecretUri: "projects/p/secrets/key-1/versions/1"},
										},
										{
											Cert: &container.CertificateConfig{GcpSecretManagerSecretUri: "projects/p/secrets/cert-2/versions/1"},
											Key:  &container.CertificateConfig{GcpSecretManagerSecretUri: "projects/p/secrets/key-2/versions/1"},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		cccMeta := feature.FromCccSpec(cccSpec)
		npMeta := feature.FromNodepool(np)
		assert.Equal(t, cccMeta, npMeta)
	})
}
