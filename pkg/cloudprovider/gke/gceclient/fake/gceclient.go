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

package fake

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"path"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	gcev1 "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	gceinternal "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	fakek8s "k8s.io/autoscaler/cluster-autoscaler/utils/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/klog/v2"
)

var (
	letterRunes = []rune("abcdefghijklmnopqrstuvwxyz0123456789")
)

// CapacityKey uniquely identifies hardware capacity based on zone and machine type.
type CapacityKey struct {
	Zone        string
	MachineType string
}

// GceClient implements the AutoscalingInternalGceClient interface using function fields for testing.
type GceClient struct {
	sync.Mutex

	t     testing.TB
	k8s   *fakek8s.Kubernetes
	mcp   *machinetypes.MachineConfigProvider
	dracp machinetypes.DranetConfigProvider
	// nextFreeId is used to assign unique IDs to GCE objects, preventing cache key collisions.
	nextFreeId uint64

	// --- Internal state
	machineTypes          map[string]*gcev1.MachineType                 // Key: {zone}/{machineType}
	migs                  map[string]*gcev1.InstanceGroupManager        // Key: {zone}/{migName}
	templates             map[string]*gcev1.InstanceTemplate            // Key: {templateName}
	instances             map[string]map[string]gceinternal.GceInstance // Key: {zone}/{migName}, Val: {instanceName} -> Instance
	zoneToDiskTypes       map[string][]string
	projectToReservations map[string][]*gcev1.Reservation
	accelerators          *gcev1.AcceleratorTypeList
	regionToZones         map[string][]string
	regionToAiZones       map[string][]string
	availableCpuPlatforms map[string][]string

	// --- Behavior modifiers
	createInstanceForMIGError  map[string]cloudprovider.InstanceErrorInfo // Key: migName
	createInstanceForZoneError map[string]cloudprovider.InstanceErrorInfo // Key: zoneName
	fetchMachineTypeHandler    map[string]func() error                    // Key: {zone}/{machineType}
	capacityMap                map[CapacityKey]int64                      // Key: CapacityKey
}

// NewGceClient creates a new, empty fake GCE client.
func NewGceClient(t testing.TB, k8s *fakek8s.Kubernetes) *GceClient {
	return &GceClient{
		t:                          t,
		dracp:                      machinetypes.NewDranetConfigProvider(),
		machineTypes:               make(map[string]*gcev1.MachineType),
		migs:                       make(map[string]*gcev1.InstanceGroupManager),
		templates:                  make(map[string]*gcev1.InstanceTemplate),
		instances:                  make(map[string]map[string]gceinternal.GceInstance),
		zoneToDiskTypes:            make(map[string][]string),
		projectToReservations:      make(map[string][]*gcev1.Reservation),
		regionToZones:              make(map[string][]string),
		regionToAiZones:            make(map[string][]string),
		availableCpuPlatforms:      make(map[string][]string),
		createInstanceForMIGError:  make(map[string]cloudprovider.InstanceErrorInfo),
		createInstanceForZoneError: make(map[string]cloudprovider.InstanceErrorInfo),
		fetchMachineTypeHandler:    make(map[string]func() error),
		capacityMap:                make(map[CapacityKey]int64),
		k8s:                        k8s,
	}
}

// WithMachineConfigProvider sets the MachineConfigProvider for the fake client.
func (g *GceClient) WithMachineConfigProvider(mcp *machinetypes.MachineConfigProvider) *GceClient {
	if len(g.machineTypes) > 0 || (g.accelerators != nil && len(g.accelerators.Items) > 0) {
		g.t.Fatalf("WithMachineConfigProvider called after defaults were already populated. Set MCP first.")
	}
	g.mcp = mcp
	return g
}

// --- Internal state builders

// WithMachineTypes adds a list of machine types to the fake's internal state.
func (g *GceClient) withMachineTypes(names ...string) *GceClient {
	if len(g.regionToZones) == 0 {
		g.t.Fatalf("WithMachineTypes called before zones were set. Call WithZones/WithRegionToZones first so the fake knows which zones to register these machine types in.")
	}
	mcp := g.mcp
	if mcp == nil {
		mcp = machinetypes.NewMachineConfigProvider(nil)
	}
	for _, name := range names {
		mtInfo, err := mcp.ToMachineType(name)
		if err != nil {
			g.t.Fatalf("Failed to get machine type info for %q: %v", name, err)
		}
		for _, zoneList := range g.regionToZones {
			for _, zone := range zoneList {
				mt := &gcev1.MachineType{
					Name:      mtInfo.Name,
					GuestCpus: mtInfo.CPU,
					MemoryMb:  mtInfo.Memory / (1024 * 1024),
					Zone:      zone,
				}
				if mtInfo.HasFixedGPU() {
					mt.Accelerators = []*gcev1.MachineTypeAccelerators{{
						GuestAcceleratorType:  mtInfo.GpuType(),
						GuestAcceleratorCount: int64(mtInfo.FixedGpuCount()),
					}}
				} else {
					tpuType, tpuCount, err := mtInfo.TpuConfig()
					if err != nil {
						g.t.Fatalf("Failed to get TPU config for machine type %q: %v", name, err)
					}
					if tpuType != "" && tpuCount > 0 {
						mt.Accelerators = []*gcev1.MachineTypeAccelerators{{
							GuestAcceleratorType:  tpuType,
							GuestAcceleratorCount: tpuCount,
						}}
					}
				}
				key := machineTypeKey(zone, name)
				g.machineTypes[key] = mt
			}
		}
	}
	return g
}

// WithReservations adds a list of reservations to the fake's internal state.
func (g *GceClient) WithReservations(reservations map[string][]*gcev1.Reservation) *GceClient {
	g.Lock()
	defer g.Unlock()
	g.projectToReservations = reservations
	return g
}

func (g *GceClient) WithMigs(migs ...*gcev1.InstanceGroupManager) *GceClient {
	for _, mig := range migs {
		key := fmt.Sprintf("%s/%s", mig.Zone, mig.Name)
		g.migs[key] = mig
	}
	return g
}

// WithTemplates adds a list of instance templates to the fake's internal state.
func (g *GceClient) WithTemplates(templates ...*gcev1.InstanceTemplate) *GceClient {
	for _, t := range templates {
		g.templates[t.Name] = t
	}
	return g
}

// WithAcceleratorTypes sets the accelerator types.
func (g *GceClient) WithAcceleratorTypes(accelerators *gcev1.AcceleratorTypeList) *GceClient {
	g.Lock()
	defer g.Unlock()
	g.accelerators = accelerators
	return g
}

// WithDiskTypes sets the disk types.
func (g *GceClient) WithDiskTypes(diskTypes map[string][]string) *GceClient {
	g.Lock()
	defer g.Unlock()
	g.zoneToDiskTypes = diskTypes
	return g
}

// WithZones sets both standard and AI zones.
func (g *GceClient) WithZones(standardZones map[string][]string, aiZones map[string][]string) *GceClient {
	if len(g.machineTypes) > 0 || len(g.zoneToDiskTypes) > 0 || (g.accelerators != nil && len(g.accelerators.Items) > 0) {
		g.t.Fatalf("WithZones called after default data was already populated. Set zones before populating defaults.")
	}
	g.Lock()
	defer g.Unlock()

	// Populate regionToZones with union of standard and AI zones
	g.regionToZones = make(map[string][]string)
	for r, zList := range standardZones {
		zonesCopy := make([]string, len(zList))
		copy(zonesCopy, zList)
		g.regionToZones[r] = zonesCopy
	}

	// Deep copy AI zones
	g.regionToAiZones = make(map[string][]string)
	for r, zList := range aiZones {
		zonesCopy := make([]string, len(zList))
		copy(zonesCopy, zList)
		g.regionToAiZones[r] = zonesCopy
		for _, z := range zList {
			if !slices.Contains(g.regionToZones[r], z) {
				g.regionToZones[r] = append(g.regionToZones[r], z)
			}
		}
	}
	return g
}

// WithAvailableCpuPlatforms sets the available CPU platforms for zones.
func (g *GceClient) WithAvailableCpuPlatforms(platforms map[string][]string) *GceClient {
	g.availableCpuPlatforms = platforms
	return g
}

func (g *GceClient) WithInstances(instanceMap map[string][]gceinternal.GceInstance) *GceClient {
	for migKey, instanceList := range instanceMap {
		if g.instances[migKey] == nil {
			g.instances[migKey] = make(map[string]gceinternal.GceInstance)
		}
		for _, inst := range instanceList {
			// providerId is "gce://{project}/{zone}/{instanceName}"
			parts := strings.Split(inst.Id, "/")
			instanceName := parts[len(parts)-1]
			g.instances[migKey][instanceName] = inst
		}
	}
	return g
}

// AddCustomMachineType adds a machine type to the fake's internal state.
func (g *GceClient) AddCustomMachineType(mt *gcev1.MachineType) {
	if mt.Zone == "" {
		g.t.Fatalf("AddCustomMachineType: MachineType.Zone must be set")
	}
	g.Lock()
	defer g.Unlock()
	key := machineTypeKey(mt.Zone, mt.Name)
	g.machineTypes[key] = mt
}

// WithDefaultMachineTypes populates the client with all default machine types for all configured zones.
func (g *GceClient) WithDefaultMachineTypes() *GceClient {
	mcp := g.mcp
	if mcp == nil {
		mcp = machinetypes.NewMachineConfigProvider(nil)
	}
	names := allMachineTypeNames(mcp)
	return g.withMachineTypes(names...)
}

// WithDefaultZones populates the client with default zones for testing.
func (g *GceClient) WithDefaultZones() *GceClient {
	zones := []string{"us-central1-a", "us-central1-b", "us-central1-c"}
	return g.WithZones(map[string][]string{
		"us-central1": zones,
	}, nil)
}

// WithDefaultDiskTypes populates the client with default disk types for all configured zones.
func (g *GceClient) WithDefaultDiskTypes() *GceClient {
	diskTypes := make(map[string][]string)
	for _, zList := range g.regionToZones {
		for _, zone := range zList {
			diskTypes[zone] = []string{"pd-standard", "pd-balanced", "pd-ssd", "boot"}
		}
	}
	return g.WithDiskTypes(diskTypes)
}

// WithDefaultAccelerators populates the client with default accelerator types for all configured zones.
func (g *GceClient) WithDefaultAccelerators() *GceClient {
	mcp := g.mcp
	if mcp == nil {
		mcp = machinetypes.NewMachineConfigProvider(nil)
	}
	supportedGPUs := mcp.GetAllGpuTypes()
	supportedTPUs := mcp.GetAllSupportedTpuTypes()

	var acceleratorItems []*gcev1.AcceleratorType
	for _, zList := range g.regionToZones {
		for _, zone := range zList {
			for _, gpu := range supportedGPUs {
				acceleratorItems = append(acceleratorItems, &gcev1.AcceleratorType{
					Name: gpu.Name(),
					Zone: zone,
				})
			}
			for _, tpu := range supportedTPUs {
				acceleratorItems = append(acceleratorItems, &gcev1.AcceleratorType{
					Name: tpu,
					Zone: zone,
				})
			}
		}
	}

	return g.WithAcceleratorTypes(&gcev1.AcceleratorTypeList{
		Items: acceleratorItems,
	})
}

// WithDefaults populates the fake GCE client with default zones,
// regions, disk types, and accelerator types for testing.
func (g *GceClient) WithDefaults() *GceClient {
	return g.WithDefaultZones().
		WithDefaultDiskTypes().
		WithDefaultAccelerators().
		WithDefaultMachineTypes()
}

// allMachineTypes builds a list of standard machine types.
func allMachineTypeNames(mcp *machinetypes.MachineConfigProvider) []string {
	if mcp == nil {
		mcp = machinetypes.NewMachineConfigProvider(nil)
	}

	families := mcp.AllMachineFamilies()
	sort.Slice(families, func(i, j int) bool {
		return families[i].Name() < families[j].Name()
	})

	var result []string
	for _, family := range families {
		allTypesMap := family.AllMachineTypes(machinetypes.NoConstraints)
		var allTypes []machinetypes.MachineType
		for _, mt := range allTypesMap {
			allTypes = append(allTypes, mt)
		}
		sort.Slice(allTypes, func(i, j int) bool {
			return allTypes[i].Name < allTypes[j].Name
		})

		for _, mt := range allTypes {
			result = append(result, mt.Name)
		}
	}
	return result
}

// --- Public functions used for testing ---

// AddMig adds or updates a MIG in the fake's internal state.
func (g *GceClient) AddMig(mig *gcev1.InstanceGroupManager) {
	g.Lock()
	defer g.Unlock()
	key := fmt.Sprintf("%s/%s", mig.Zone, mig.Name)
	g.migs[key] = mig
}

// AddTemplate adds a template to the fake's internal state.
func (g *GceClient) AddTemplate(template *gcev1.InstanceTemplate) {
	g.Lock()
	defer g.Unlock()
	g.templates[template.Name] = template
}

// --- GCE API Fakes to resemble real logic ---

// InsertInstanceTemplate fakes the compute.instanceTemplates.insert API call.
func (g *GceClient) InsertInstanceTemplate(project string, template *gcev1.InstanceTemplate) error {
	g.Lock()
	defer g.Unlock()

	if _, exists := g.templates[template.Name]; exists {
		return fmt.Errorf("instance template %s already exists", template.Name)
	}
	if template.SelfLink == "" {
		template.SelfLink = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/instanceTemplates/%s", project, template.Name)
	}
	if template.Id == 0 {
		// Assign a unique ID to prevent cache key collisions in GkeManager
		// when caching parsed node templates (which are keyed by template.Id).
		g.nextFreeId++
		template.Id = g.nextFreeId
	}
	g.templates[template.Name] = template
	return nil
}

// DeleteInstanceTemplate fakes the compute.instanceTemplates.delete API call.
func (g *GceClient) DeleteInstanceTemplate(name string) error {
	g.Lock()
	defer g.Unlock()

	if _, exists := g.templates[name]; !exists {
		return fmt.Errorf("template %s not found", name)
	}
	delete(g.templates, name)
	return nil
}

// InsertInstanceGroupManager fakes the compute.instanceGroupManagers.insert API call.
func (g *GceClient) InsertInstanceGroupManager(project, zone string, mig *gcev1.InstanceGroupManager) error {
	g.Lock()
	defer g.Unlock()

	if mig.Zone == "" {
		mig.Zone = zone
	}
	migKey := fmt.Sprintf("%s/%s", mig.Zone, mig.Name)
	if _, exists := g.migs[migKey]; exists {
		return fmt.Errorf("MIG %s already exists in zone %s", mig.Name, mig.Zone)
	}

	if mig.InstanceTemplate == "" {
		return fmt.Errorf("MIG %s missing instance template reference", mig.Name)
	}
	templateName := path.Base(mig.InstanceTemplate)
	template, tFound := g.templates[templateName]
	if !tFound {
		return fmt.Errorf("instance template %s not found", templateName)
	}

	if mig.SelfLink == "" {
		mig.SelfLink = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instanceGroupManagers/%s", project, mig.Zone, mig.Name)
	}
	if mig.BaseInstanceName == "" {
		mig.BaseInstanceName = mig.Name
	}
	g.migs[migKey] = mig
	if g.instances[migKey] == nil {
		g.instances[migKey] = make(map[string]gceinternal.GceInstance)
	}
	return g.reconcileMIGInstances(project, migKey, template)
}

// DeleteInstanceGroupManager fakes the compute.instanceGroupManagers.delete API call.
func (g *GceClient) DeleteInstanceGroupManager(zone, name string) error {
	g.Lock()
	defer g.Unlock()

	key := fmt.Sprintf("%s/%s", zone, name)
	if _, exists := g.migs[key]; !exists {
		return fmt.Errorf("mig %s not found in zone %s", name, zone)
	}
	delete(g.migs, key)
	delete(g.instances, key)
	return nil
}

// reconcileMIGInstances is an internal function to adjust the number of instances in a MIG.
func (g *GceClient) reconcileMIGInstances(project, migKey string, template *gcev1.InstanceTemplate) error {
	mig := g.migs[migKey]
	if mig == nil {
		return fmt.Errorf("MIG not found for key %s in reconcileMIGInstances", migKey)
	}

	currentInstances := g.instances[migKey]
	if currentInstances == nil {
		currentInstances = make(map[string]gceinternal.GceInstance)
		g.instances[migKey] = currentInstances
	}

	currentSize := int64(len(currentInstances))
	targetSize := mig.TargetSize

	if targetSize == currentSize {
		return nil
	}

	if targetSize > currentSize {
		return g.addInstances(mig, template, project, targetSize-currentSize)
	} else {
		g.removeInstances(currentSize-targetSize, currentInstances)
	}
	return nil
}

func (g *GceClient) addInstances(mig *gcev1.InstanceGroupManager, template *gcev1.InstanceTemplate, project string, numToAdd int64) error {
	migKey := fmt.Sprintf("%s/%s", mig.Zone, mig.Name)
	ref := gceinternal.GceRef{Project: project, Zone: mig.Zone, Name: mig.Name}

	mt := g.findMachineType(template.Properties.MachineType, ref.Zone)
	if mt == nil {
		return fmt.Errorf("machine type %s not found in zone %s", template.Properties.MachineType, ref.Zone)
	}

	existingNames := g.getExistingInstanceNames(migKey, nil)
	newNames := generateInstanceNames(mig.BaseInstanceName, numToAdd, existingNames)

	return g.createInstancesInternal(ref, mig, mt, template, newNames, template.Name)
}

func (g *GceClient) removeInstances(numToRemove int64, currentInstances map[string]gceinternal.GceInstance) {
	removedCount := 0
	for instanceName := range currentInstances {
		if removedCount >= int(numToRemove) {
			break
		}
		delete(currentInstances, instanceName)
		g.k8s.DeleteNode(instanceName)
		removedCount++
	}
}

func (g *GceClient) findMachineType(name, zone string) *gcev1.MachineType {
	key := fmt.Sprintf("%s/%s", zone, name)
	if mt, found := g.machineTypes[key]; found {
		return mt
	}
	for k, mt := range g.machineTypes {
		if strings.HasSuffix(k, "/"+name) {
			return mt
		}
	}
	return nil
}

// --- AutoscalingFakeGceClient implementation ---

func (g *GceClient) FetchMachineType(zone, machineType string) (*gcev1.MachineType, error) {
	g.Lock()
	defer g.Unlock()

	key := machineTypeKey(zone, machineType)
	if handler, found := g.fetchMachineTypeHandler[key]; found && handler != nil {
		if err := handler(); err != nil {
			return nil, err
		}
	}

	if mt, found := g.machineTypes[key]; found {
		return mt, nil
	}
	var keys []string
	for k := range g.machineTypes {
		keys = append(keys, k)
	}
	return nil, &googleapi.Error{
		Code:    http.StatusNotFound,
		Message: fmt.Sprintf("machine type %s in zone %s not found in fake GCE. Available: %v", machineType, zone, keys),
	}
}

func (g *GceClient) FetchMachineTypes(zone string) ([]*gcev1.MachineType, error) {
	g.Lock()
	defer g.Unlock()
	var results []*gcev1.MachineType
	for _, mt := range g.machineTypes {
		if mt.Zone == zone {
			key := machineTypeKey(zone, mt.Name)
			if handler, found := g.fetchMachineTypeHandler[key]; found && handler != nil {
				if err := handler(); err != nil {
					return nil, err
				}
			}
			results = append(results, mt)
		}
	}
	return results, nil
}

func (g *GceClient) FetchAllMigs(zone string) ([]*gcev1.InstanceGroupManager, error) {
	g.Lock()
	defer g.Unlock()
	var results []*gcev1.InstanceGroupManager
	for key, mig := range g.migs {
		if strings.HasPrefix(key, zone+"/") {
			results = append(results, cloneMig(mig))
		}
	}
	return results, nil
}

func (g *GceClient) FetchMig(ref gceinternal.GceRef) (*gcev1.InstanceGroupManager, error) {
	g.Lock()
	defer g.Unlock()
	key := fmt.Sprintf("%s/%s", ref.Zone, ref.Name)
	if mig, found := g.migs[key]; found {
		return cloneMig(mig), nil
	}
	return nil, fmt.Errorf("MIG %s in zone %s not found in fake GCE", ref.Name, ref.Zone)
}

func (g *GceClient) FetchAllInstances(project, zone string, filter string) ([]gceinternal.GceInstance, error) {
	g.Lock()
	defer g.Unlock()
	var results []gceinternal.GceInstance
	for _, instances := range g.instances {
		for _, inst := range instances {
			if inst.Igm.Zone == zone && inst.Igm.Project == project {
				results = append(results, inst)
			}
		}
	}
	return results, nil
}

func (g *GceClient) FetchMigTargetSize(ref gceinternal.GceRef) (int64, error) {
	g.Lock()
	defer g.Unlock()
	key := fmt.Sprintf("%s/%s", ref.Zone, ref.Name)
	if mig, found := g.migs[key]; found {
		return mig.TargetSize, nil
	}
	return 0, fmt.Errorf("MIG %s in zone %s not found in fake GCE", ref.Name, ref.Zone)
}

func (g *GceClient) FetchMigBasename(ref gceinternal.GceRef) (string, error) {
	return ref.Name, nil
}

func (g *GceClient) FetchMigInstances(ref gceinternal.GceRef) ([]gceinternal.GceInstance, error) {
	g.Lock()
	defer g.Unlock()
	var results []gceinternal.GceInstance
	for _, instances := range g.instances {
		for _, inst := range instances {
			if inst.Igm == ref {
				results = append(results, inst)
			}
		}
	}
	return results, nil
}

func (g *GceClient) FetchMigTemplateName(migRef gceinternal.GceRef) (gceinternal.InstanceTemplateName, error) {
	g.Lock()
	defer g.Unlock()
	key := fmt.Sprintf("%s/%s", migRef.Zone, migRef.Name)
	mig, found := g.migs[key]
	if !found {
		return gceinternal.InstanceTemplateName{}, fmt.Errorf("MIG %s in zone %s not found in fake GCE", migRef.Name, migRef.Zone)
	}
	if mig.InstanceTemplate == "" {
		return gceinternal.InstanceTemplateName{}, fmt.Errorf("MIG %s in zone %s has no instance template", migRef.Name, migRef.Zone)
	}

	templateURL := mig.InstanceTemplate
	templateName := path.Base(templateURL)
	regional := strings.Contains(templateURL, "/regions/")

	return gceinternal.InstanceTemplateName{
		Name:     templateName,
		Regional: regional,
	}, nil
}

func (g *GceClient) FetchMigTemplate(migRef gceinternal.GceRef, templateName string, regional bool) (*gcev1.InstanceTemplate, error) {
	g.Lock()
	defer g.Unlock()
	if t, found := g.templates[templateName]; found {
		return t, nil
	}
	suffixedTemplateName := fmt.Sprintf("%s-tmpl", templateName)
	if t, foundTmpl := g.templates[suffixedTemplateName]; foundTmpl {
		return t, nil
	}
	return nil, fmt.Errorf("template %s not found in fake GCE", templateName)
}

func (g *GceClient) FetchMigsWithName(zone string, filter *regexp.Regexp) ([]string, error) {
	g.Lock()
	defer g.Unlock()
	var links []string
	for _, mig := range g.migs {
		if mig.Zone == zone {
			links = append(links, mig.SelfLink)
		}
	}
	return links, nil
}

func (g *GceClient) fetchZones(region string) ([]string, error) {
	zones, ok := g.regionToZones[region]
	if !ok {
		return []string{}, nil
	}

	results := make([]string, len(zones))
	copy(results, zones)
	return results, nil
}

func (g *GceClient) FetchZones(region string) ([]string, error) {
	g.Lock()
	defer g.Unlock()
	return g.fetchZones(region)
}

func (g *GceClient) FetchAIZones(region string) ([]string, error) {
	g.Lock()
	defer g.Unlock()
	aiZones := g.regionToAiZones[region]
	if len(aiZones) > 0 {
		results := make([]string, len(aiZones))
		copy(results, aiZones)
		return results, nil
	}

	return g.fetchZones(region)
}

func (g *GceClient) FetchStandardZones(region string) ([]string, error) {
	g.Lock()
	defer g.Unlock()

	zones := g.regionToZones[region]
	aiZones := g.regionToAiZones[region]

	var results []string
	for _, z := range zones {
		if !slices.Contains(aiZones, z) {
			results = append(results, z)
		}
	}
	return results, nil
}

func (g *GceClient) FetchAvailableCpuPlatforms() (map[string][]string, error) {
	return g.availableCpuPlatforms, nil
}

func (g *GceClient) FetchAvailableDiskTypes(zone string) ([]string, error) {
	return g.zoneToDiskTypes[zone], nil
}

func (g *GceClient) FetchReservations() ([]*gcev1.Reservation, error) {
	g.Lock()
	defer g.Unlock()
	var reservations []*gcev1.Reservation
	for _, rsv := range g.projectToReservations {
		reservations = append(reservations, rsv...)
	}
	return reservations, nil
}

func (g *GceClient) FetchReservationsInProject(projectID string) ([]*gcev1.Reservation, error) {
	g.Lock()
	defer g.Unlock()
	source, ok := g.projectToReservations[projectID]
	if !ok {
		return []*gcev1.Reservation{}, nil
	}
	results := make([]*gcev1.Reservation, len(source))
	copy(results, g.projectToReservations[projectID])
	return results, nil
}

func (g *GceClient) FetchReservation(projectID string, name string) (*gcev1.Reservation, error) {
	g.Lock()
	defer g.Unlock()
	for _, reservation := range g.projectToReservations[projectID] {
		if reservation.Name == name {
			copy := *reservation
			return &copy, nil
		}
	}
	return nil, fmt.Errorf("reservation %s not found in fake GCE", name)
}

func (g *GceClient) FetchListManagedInstancesResults(migRef gceinternal.GceRef) (string, error) {
	g.Lock()
	defer g.Unlock()
	key := fmt.Sprintf("%s/%s", migRef.Zone, migRef.Name)
	if mig, found := g.migs[key]; found {
		return mig.ListManagedInstancesResults, nil
	}
	return "", fmt.Errorf("MIG %s in zone %s not found in fake GCE", migRef.Name, migRef.Zone)
}

func (g *GceClient) ResizeMig(ref gceinternal.GceRef, size int64) error {
	g.Lock()
	defer g.Unlock()

	key := fmt.Sprintf("%s/%s", ref.Zone, ref.Name)
	mig, found := g.migs[key]
	if !found {
		if size == 0 {
			return nil
		}
		return fmt.Errorf("MIG %s in zone %s not found in fake GCE", ref.Name, ref.Zone)
	}

	mig.TargetSize = size

	templateName := path.Base(mig.InstanceTemplate)
	template, tFound := g.templates[templateName]
	if !tFound {
		return fmt.Errorf("instance template %s not found for MIG %s", templateName, mig.Name)
	}
	return g.reconcileMIGInstances(ref.Project, key, template)
}

func (g *GceClient) DeleteInstances(migRef gceinternal.GceRef, instances []gceinternal.GceRef) error {
	g.Lock()
	defer g.Unlock()

	migKey := fmt.Sprintf("%s/%s", migRef.Zone, migRef.Name)
	klog.Infof("GceClient.DeleteInstances called for MIG %s, instances: %v", migKey, instances)
	mig, migFound := g.migs[migKey]
	if !migFound {
		return fmt.Errorf("fake gce: MIG %s not found during DeleteInstances", migKey)
	}
	currentInstances, instancesFound := g.instances[migKey]
	if !instancesFound {
		return fmt.Errorf("fake gce: instances for MIG %s not found during DeleteInstances", migKey)
	}

	deletedCount := 0
	for _, instRef := range instances {
		if _, exists := currentInstances[instRef.Name]; exists {
			delete(currentInstances, instRef.Name)
			deletedCount++
		}
	}

	mig.TargetSize -= int64(deletedCount)

	for _, instRef := range instances {
		if g.k8s == nil {
			return fmt.Errorf("fake k8s client is nil")
		}
		nodeName := instRef.Name
		if nodeList := g.k8s.Nodes(); nodeList != nil {
			for _, node := range nodeList.Items {
				if strings.HasSuffix(node.Spec.ProviderID, "/"+instRef.Name) {
					nodeName = node.Name
					break
				}
			}
		}
		g.k8s.DeleteNode(nodeName)
	}

	return nil
}

func (g *GceClient) CreateInstances(ref gceinternal.GceRef, templateName string, count int64, names []string) ([]string, error) {
	g.Lock()
	defer g.Unlock()

	migKey := fmt.Sprintf("%s/%s", ref.Zone, ref.Name)
	mig, found := g.migs[migKey]

	if !found {
		return nil, fmt.Errorf("instance group manager %s not found", migKey)
	}

	baseName := ref.Name
	if mig.BaseInstanceName != "" {
		baseName = mig.BaseInstanceName
	}

	existingNames := g.getExistingInstanceNames(migKey, names)
	newNames := generateInstanceNames(baseName, count, existingNames)

	err := g.resizeAtomicallyLocked(ref, templateName, newNames)
	if err != nil {
		return nil, err
	}
	return newNames, nil
}

// ResizeAtomically creates specific instances in the fake client immediately, simulating atomic resize.
func (g *GceClient) ResizeAtomically(ref gceinternal.GceRef, templateName string, names []string) error {
	g.Lock()
	defer g.Unlock()

	return g.resizeAtomicallyLocked(ref, templateName, names)
}

func (g *GceClient) resizeAtomicallyLocked(ref gceinternal.GceRef, templateName string, names []string) error {
	migKey := fmt.Sprintf("%s/%s", ref.Zone, ref.Name)
	mig, found := g.migs[migKey]

	if !found {
		return fmt.Errorf("instance group manager %s not found", migKey)
	}

	resolvedTemplateName, err := g.resolveTemplateName(mig, templateName)
	if err != nil {
		return err
	}

	template, found := g.templates[resolvedTemplateName]
	if !found {
		return fmt.Errorf("instance template %s not found", templateName)
	}

	machineType := template.Properties.MachineType
	mt := g.findMachineType(machineType, ref.Zone)
	if mt == nil {
		return fmt.Errorf("machine type %s not found in zone %s", machineType, ref.Zone)
	}

	if err := g.createInstancesInternal(ref, mig, mt, template, names, resolvedTemplateName); err != nil {
		return err
	}

	mig.TargetSize += int64(len(names))
	return nil
}

func (g *GceClient) getExistingInstanceNames(migKey string, names []string) map[string]bool {
	existingNames := make(map[string]bool)
	for _, id := range names {
		existingNames[path.Base(id)] = true
	}
	for name := range g.instances[migKey] {
		existingNames[name] = true
	}
	return existingNames
}

func generateInstanceNames(baseName string, count int64, existingNames map[string]bool) []string {
	newNames := make([]string, count)
	for i := 0; i < int(count); i++ {
		newNames[i] = generateInstanceName(baseName, existingNames)
		existingNames[newNames[i]] = true
	}
	return newNames
}

// resolveTemplateName resolves the template name to use for a MIG.
//
// Production callers (e.g. GkeManager.CreateInstances) may
// pass 'basename' (which often equals mig.Name) as the second argument string,
// rather than a strict template name. When we detect templateName == mig.Name,
// we fall back to extracting the template name from the attached MIG.InstanceTemplate
// URL to maintain framework forgery simulator cache parity keys (%s-%s-tmpl).
func (g *GceClient) resolveTemplateName(mig *gcev1.InstanceGroupManager, templateName string) (string, error) {
	if templateName != "" && templateName != mig.Name {
		return templateName, nil
	}
	if mig.InstanceTemplate != "" {
		return path.Base(mig.InstanceTemplate), nil
	}
	if templateName != "" {
		return templateName, nil
	}
	return "", fmt.Errorf("instance template name is empty and MIG has no template")
}

func (g *GceClient) createInstancesInternal(ref gceinternal.GceRef, mig *gcev1.InstanceGroupManager, mt *gcev1.MachineType, template *gcev1.InstanceTemplate, names []string, templateName string) error {
	if template != nil && template.Properties != nil && template.Properties.MachineType != "" {
		if err := g.consumeHardwareCapacityLocked(ref.Zone, template.Properties.MachineType, int64(len(names))); err != nil {
			return err
		}
	}

	migKey := fmt.Sprintf("%s/%s", ref.Zone, ref.Name)
	if g.instances[migKey] == nil {
		g.instances[migKey] = make(map[string]gceinternal.GceInstance)
	}

	for _, instanceName := range names {
		providerId := fmt.Sprintf("gce://%s/%s/%s", ref.Project, ref.Zone, instanceName)
		status := g.instanceStatus(ref)

		instance := gceinternal.GceInstance{
			Instance: cloudprovider.Instance{
				Id:     providerId,
				Status: status,
			},
			Igm:                  ref,
			InstanceTemplateName: templateName,
		}
		g.instances[migKey][instanceName] = instance

		if status.ErrorInfo != nil {
			// If there is an instance error (like stockout), we don't add the node to k8s.
			// This simulates the VM failing to start/register.
			continue
		}

		if g.k8s == nil {
			return fmt.Errorf("fake k8s client is nil")
		}

		node, err := buildNodeFromTemplate(mig, mt, template, instanceName)
		if err != nil {
			return err
		}
		node.Spec.ProviderID = providerId
		node.Name = instanceName
		g.k8s.AddNode(node)

		slices, err := predictResourceSlices(g.mcp, g.dracp, mt, node)
		if err != nil {
			return err
		}
		for _, slice := range slices {
			_, err = g.k8s.Client.ResourceV1().ResourceSlices().Create(context.Background(), slice, metav1.CreateOptions{})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *GceClient) instanceStatus(ref gceinternal.GceRef) *cloudprovider.InstanceStatus {
	errInfoMIG, hasMIGErr := g.createInstanceForMIGError[ref.Name]
	errInfoZone, hasZoneErr := g.createInstanceForZoneError[ref.Zone]

	switch {
	case hasMIGErr:
		return &cloudprovider.InstanceStatus{
			State:     cloudprovider.InstanceCreating,
			ErrorInfo: &errInfoMIG,
		}
	case hasZoneErr:
		return &cloudprovider.InstanceStatus{
			State:     cloudprovider.InstanceCreating,
			ErrorInfo: &errInfoZone,
		}
	default:
		return &cloudprovider.InstanceStatus{
			State: cloudprovider.InstanceRunning,
		}
	}
}

func generateInstanceName(baseName string, existingNames map[string]bool) string {
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("%v-%v", baseName, strings.ToLower(randString(4)))
		if !existingNames[name] {
			return name
		}
	}
	return fmt.Sprintf("%v-%v", baseName, strings.ToLower(randString(4)))
}

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func (g *GceClient) WaitForOperation(operationName, operationType, project, zone string) error {
	return nil
}

func (g *GceClient) FetchAcceleratorTypes(zone string) (*gcev1.AcceleratorTypeList, error) {
	return g.accelerators, nil
}

func (g *GceClient) GetHttpTimeout() time.Duration {
	return 30 * time.Second
}

func (g *GceClient) FetchFutureReservationsInProject(projectID string) ([]*gceclient.GceFutureReservation, error) {
	return nil, nil
}

func (g *GceClient) FetchReservationBlocksInReservation(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationBlock, error) {
	return nil, nil
}

func (g *GceClient) FetchReservationSubBlocksInReservationBlock(reservationRef gceclient.ReservationRef) ([]*gceclient.GceReservationSubBlock, error) {
	return nil, nil
}

func (g *GceClient) FetchResourcePolicies(projectId, region string) ([]*gceclient.GceResourcePolicy, error) {
	return nil, nil
}

func (g *GceClient) FetchNetwork(projectId, name string) (*gcev1.Network, error) {
	return nil, nil
}

func (g *GceClient) ResumeInstances(migRef gceinternal.GceRef, instances []gceinternal.GceRef) error {
	return nil
}

func (g *GceClient) SuspendInstances(migRef gceinternal.GceRef, instances []gceinternal.GceRef, forceSuspend bool) error {
	return nil
}

// --- Behavior modifier functions ---

func (g *GceClient) SetCreateInstanceForMigError(migName string, errInfo cloudprovider.InstanceErrorInfo) {
	g.Lock()
	defer g.Unlock()
	g.createInstanceForMIGError[migName] = errInfo
}

func (g *GceClient) SetCreateInstanceForZoneError(zoneName string, errInfo cloudprovider.InstanceErrorInfo) {
	g.Lock()
	defer g.Unlock()
	g.createInstanceForZoneError[zoneName] = errInfo
}

func (g *GceClient) ClearCreateInstanceForZoneError(zone string) {
	g.Lock()
	defer g.Unlock()
	delete(g.createInstanceForZoneError, zone)
}

// SetFetchMachineTypeHandler configures the fake GCE client to execute the given handler
// when FetchMachineType is called for the given zone and machine type.
// Pass nil handler to clear.
func (g *GceClient) SetFetchMachineTypeHandler(zone, machineType string, handler func() error) {
	g.Lock()
	defer g.Unlock()
	key := machineTypeKey(zone, machineType)
	if handler == nil {
		delete(g.fetchMachineTypeHandler, key)
	} else {
		g.fetchMachineTypeHandler[key] = handler
	}
}

// machineTypeKey constructs the internal map key for a zonal machine type.
func machineTypeKey(zone, machineType string) string {
	return fmt.Sprintf("%s/%s", zone, machineType)
}

// SetBackendMachineCount allows simulating capacity (in machine count) constraints for a given zone and machineType.
func (g *GceClient) SetBackendMachineCount(zone string, machineType string, capacity int64) {
	g.Lock()
	defer g.Unlock()
	g.capacityMap[CapacityKey{Zone: zone, MachineType: machineType}] = capacity
}

// --- Manual Deep Cloning Functions ---
// TODO(b/529258147): The fundamental problem here is that we are using external GCP API types directly.
// We should follow the pattern used for reservation blocks/subblocks and create internal wrapper
// structs (e.g. GceInstanceGroupManager) that only contain the fields actually used by the codebase.
// Once wrapped in internal structs, we can use k8s deepcopy-gen to eliminate this manual cloning boilerplate.
//
// This function is used to safely return copies of fake internal state to prevent data races.
// We explicitly use manual deep copying (instead of json.Marshal/Unmarshal) because the fakes
// are heavily utilized in Big Unit Tests which profile CPU and memory usage to measure the
// Autoscaler's efficiency. JSON serialization would cause significant reflection overhead and
// heap allocations, polluting the profiling metrics. This function performs lightweight
// shallow copies of the struct and explicitly copy only the nested pointers and slices modified by the fakes.

func cloneMig(mig *gcev1.InstanceGroupManager) *gcev1.InstanceGroupManager {
	if mig == nil {
		return nil
	}
	clone := *mig
	if mig.Status != nil {
		statusCopy := *mig.Status
		clone.Status = &statusCopy
	}
	return &clone
}

// ResetHardwareCapacity clears all simulated stockouts.
func (g *GceClient) ResetHardwareCapacity() {
	g.Lock()
	defer g.Unlock()
	g.capacityMap = make(map[CapacityKey]int64)
}

// ConsumeHardwareCapacityAtomic attempts to consume capacity for the given zone and machineType.
// If capacity is not configured, it assumes unlimited capacity.
func (g *GceClient) ConsumeHardwareCapacityAtomic(zone string, machineType string, count int64) error {
	g.Lock()
	defer g.Unlock()
	return g.consumeHardwareCapacityLocked(zone, machineType, count)
}

func (g *GceClient) consumeHardwareCapacityLocked(zone string, machineType string, count int64) error {
	key := CapacityKey{Zone: zone, MachineType: machineType}
	cap, ok := g.capacityMap[key]
	if !ok {
		return nil // Unlimited capacity if not explicitly mocked
	}

	if cap < count {
		return &googleapi.Error{
			Code:    400,
			Message: "ZONE_RESOURCE_POOL_EXHAUSTED",
			Errors: []googleapi.ErrorItem{
				{Reason: "ZONE_RESOURCE_POOL_EXHAUSTED", Message: "GCE API error: stock out"},
			},
		}
	}
	g.capacityMap[key] = cap - count
	return nil
}
