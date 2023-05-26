/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package deprovisioning

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/cloudprovider"
	deprovisioningevents "github.com/aws/karpenter-core/pkg/controllers/deprovisioning/events"
	"github.com/aws/karpenter-core/pkg/controllers/provisioning"
	pscheduling "github.com/aws/karpenter-core/pkg/controllers/provisioning/scheduling"
	"github.com/aws/karpenter-core/pkg/controllers/state"
	"github.com/aws/karpenter-core/pkg/events"
	"github.com/aws/karpenter-core/pkg/metrics"
	"github.com/aws/karpenter-core/pkg/scheduling"
)

// consolidation is the base consolidation controller that provides common functionality used across the different
// consolidation methods.
type consolidation struct {
	clock                  clock.Clock
	cluster                *state.Cluster
	kubeClient             client.Client
	provisioner            *provisioning.Provisioner
	cloudProvider          cloudprovider.CloudProvider
	recorder               events.Recorder
	lastConsolidationState int64
}

func makeConsolidation(clock clock.Clock, cluster *state.Cluster, kubeClient client.Client, provisioner *provisioning.Provisioner,
	cloudProvider cloudprovider.CloudProvider, recorder events.Recorder) consolidation {
	return consolidation{
		clock:                  clock,
		cluster:                cluster,
		kubeClient:             kubeClient,
		provisioner:            provisioner,
		cloudProvider:          cloudProvider,
		recorder:               recorder,
		lastConsolidationState: 0,
	}
}

// consolidationTTL is the TTL between creating a consolidation command and validating that it still works.
const consolidationTTL = 15 * time.Second

// string is the string representation of the deprovisioner
func (c *consolidation) String() string {
	return metrics.ConsolidationReason
}

// sortAndFilterCandidates orders deprovisionable nodes by the disruptionCost, removing any that we already know won't
// be viable consolidation options.
func (c *consolidation) sortAndFilterCandidates(ctx context.Context, nodes []*Candidate) ([]*Candidate, error) {
	candidates, err := filterCandidates(ctx, c.kubeClient, c.recorder, nodes)
	if err != nil {
		return nil, fmt.Errorf("filtering candidates, %w", err)
	}

	sort.Slice(candidates, func(i int, j int) bool {
		return candidates[i].disruptionCost < candidates[j].disruptionCost
	})
	return candidates, nil
}

// ShouldDeprovision is a predicate used to filter deprovisionable nodes
func (c *consolidation) ShouldDeprovision(_ context.Context, cn *Candidate) bool {
	if val, ok := cn.Annotations()[v1alpha5.DoNotConsolidateNodeAnnotationKey]; ok {
		c.recorder.Publish(deprovisioningevents.Unconsolidatable(cn.Node, fmt.Sprintf("%s annotation exists", v1alpha5.DoNotConsolidateNodeAnnotationKey))...)
		return val != "true"
	}
	if cn.provisioner == nil {
		c.recorder.Publish(deprovisioningevents.Unconsolidatable(cn.Node, "provisioner is unknown")...)
		return false
	}
	if cn.provisioner.Spec.Consolidation == nil || !ptr.BoolValue(cn.provisioner.Spec.Consolidation.Enabled) {
		c.recorder.Publish(deprovisioningevents.Unconsolidatable(cn.Node, fmt.Sprintf("provisioner %s has consolidation disabled", cn.provisioner.Name))...)
		return false
	}

	// only allow spot nodes if they are annotated
	if cn.capacityType == v1alpha5.CapacityTypeSpot {

		val, ok := cn.Node.Annotations[v1alpha5.SpotConsolidateAfterAnnotationKey]

		if !ok {
			c.recorder.Publish(deprovisioningevents.Unconsolidatable(cn.Node, "spot node is not annotated to allow consolidation")...)
			return false
		}

		spotConsolidateAfterDuration, err := time.ParseDuration(val)
		if err != nil {
			c.recorder.Publish(deprovisioningevents.Unconsolidatable(cn.Node, "failed to parse spot consolidation annotation duration")...)
			return false
		}

		if c.clock.Now().Before(cn.Node.CreationTimestamp.Add(spotConsolidateAfterDuration)) {
			c.recorder.Publish(deprovisioningevents.Unconsolidatable(cn.Node, "waiting for spot consolidation timeout")...)
			return false
		}
	}

	return true
}

// computeConsolidation computes a consolidation action to take
//
// nolint:gocyclo
func (c *consolidation) computeConsolidation(ctx context.Context, candidates ...*Candidate) (Command, error) {
	defer metrics.Measure(deprovisioningDurationHistogram.WithLabelValues("Replace/Delete"))()

	candidateInstanceTypes := lo.Map(candidates, func(i *Candidate, _ int) string {
		return i.instanceType.Name
	})

	ctx = logging.WithLogger(ctx, logging.FromContext(ctx).With("candidateInstanceTypes", candidateInstanceTypes))

	logging.FromContext(ctx).Debugf("computing consolidation")

	// Run scheduling simulation to compute consolidation option
	results, err := simulateScheduling(ctx, c.kubeClient, c.cluster, c.provisioner, candidates...)
	if err != nil {
		// if a candidate node is now deleting, just retry
		if errors.Is(err, errCandidateDeleting) {
			return Command{action: actionDoNothing}, nil
		}
		return Command{}, err
	}

	// if not all of the pods were scheduled, we can't do anything
	if !results.AllPodsScheduled() {
		// This method is used by multi-node consolidation as well, so we'll only report in the single node case
		if len(candidates) == 1 {
			c.recorder.Publish(deprovisioningevents.Unconsolidatable(candidates[0].Node, results.PodSchedulingErrors())...)
		}
		return Command{action: actionDoNothing}, nil
	}

	// were we able to schedule all the pods on the inflight candidates?
	if len(results.NewMachines) == 0 {
		return Command{
			candidates: candidates,
		}, nil
	}

	// never turn multiple nodes into multiple, but allow 1 node to be turned
	// into multiple
	if len(candidates) > 1 && len(results.NewMachines) > 1 {
		return Command{
			action: actionDoNothing,
		}, nil
	}

	// get the current node price based on the offering
	// fallback if we can't find the specific zonal pricing data
	nodesPrice, err := getCandidatePrices(candidates)
	if err != nil {
		return Command{}, fmt.Errorf("getting offering price from candidate node, %w", err)
	}

	// build up a map of instance type name -> list of candidates (sorted by price)
	candidatesByInstanceType := lo.MapValues(
		lo.GroupBy(candidates, func(c *Candidate) string {
			return c.instanceType.Name
		}),
		func(cs []*Candidate, _ string) []*Candidate {
			sort.Slice(cs, func(i, j int) bool {
				a := cs[i]
				b := cs[j]

				aOffering, _ := a.instanceType.Offerings.Get(a.capacityType, a.zone)
				bOffering, _ := b.instanceType.Offerings.Get(b.capacityType, b.zone)

				return aOffering.Price < bOffering.Price
			})
			return cs
		},
	)

	// for every new machine get the cheapest instance-type & offering
	type cheapestInstanceTypeOffering struct {
		instanceType *cloudprovider.InstanceType
		offering     cloudprovider.Offering
	}

	type cheapestInstanceType struct {
		machine  *pscheduling.Machine
		cheapest *cheapestInstanceTypeOffering
	}

	cheapestMachines := lo.FilterMap(results.NewMachines, func(m *pscheduling.Machine, _ int) (*cheapestInstanceType, bool) {
		cheapest := lo.MinBy(
			lo.Map(m.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) *cheapestInstanceTypeOffering {
				return &cheapestInstanceTypeOffering{it, it.Offerings.Available().Requirements(m.Requirements).Cheapest()}
			}),
			func(item *cheapestInstanceTypeOffering, min *cheapestInstanceTypeOffering) bool {
				return item.offering.Price < min.offering.Price
			},
		)

		// limit instance type to cheapest
		m.InstanceTypeOptions = cloudprovider.InstanceTypes{cheapest.instanceType}

		// add a scheduling requirement to force the cheapest offering
		m.Requirements.Add(
			scheduling.NewRequirement(v1alpha5.LabelCapacityType, v1.NodeSelectorOpIn, cheapest.offering.CapacityType),
			scheduling.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, cheapest.offering.Zone),
		)

		// filter pointless replacements
		instanceTypeCandidates, ok := candidatesByInstanceType[m.InstanceTypeOptions[0].Name]

		// no candidate of this type exists already; allow
		if !ok {
			return &cheapestInstanceType{m, cheapest}, true
		}

		// all exisiting candiadtes of this type are already used for other new machines; allow
		if len(instanceTypeCandidates) == 0 {
			return &cheapestInstanceType{m, cheapest}, true
		}

		cheapestInstanceTypeCandidate := instanceTypeCandidates[0]
		cheapestInstanceTypeCandidateOffering, _ := cheapestInstanceTypeCandidate.instanceType.Offerings.Get(cheapestInstanceTypeCandidate.capacityType, cheapestInstanceTypeCandidate.zone)

		// exisiting candidate of this type exists, but it's more expensive than the replacement; allow
		if cheapest.offering.Price < cheapestInstanceTypeCandidateOffering.Price {
			return &cheapestInstanceType{m, cheapest}, true
		}

		// exisiting candidate is the same as the replacement; disallow
		candidatesByInstanceType[m.InstanceTypeOptions[0].Name] = lo.Slice(instanceTypeCandidates, 1, len(instanceTypeCandidates))
		return nil, false
	})

	candidates = lo.FlatMap(lo.Values(candidatesByInstanceType), func(i []*Candidate, _ int) []*Candidate {
		return i
	})

	results.NewMachines = lo.Map(cheapestMachines, func(i *cheapestInstanceType, _ int) *pscheduling.Machine {
		return i.machine
	})

	if len(candidates) == 0 {
		return Command{
			action: actionDoNothing,
		}, nil
	}

	newNodesPrice := lo.SumBy(cheapestMachines, func(i *cheapestInstanceType) float64 {
		return i.cheapest.offering.Price
	})

	candidateInstanceTypesWithPrices := strings.Join(lo.Map(candidates, func(c *Candidate, _ int) string {
		of, _ := c.instanceType.Offerings.Get(c.capacityType, c.zone)
		return fmt.Sprintf("%s/%s/%s ($%.6f/hr)", c.instanceType.Name, c.capacityType, c.zone, of.Price)
	}), ", ")

	newInstanceTypesWithPrices := strings.Join(lo.Map(results.NewMachines, func(m *pscheduling.Machine, _ int) string {
		it := m.InstanceTypeOptions[0]
		of := it.Offerings.Requirements(m.Requirements).Cheapest()
		return fmt.Sprintf("%s/%s/%s ($%.6f/hr)", it.Name, of.CapacityType, of.Zone, of.Price)
	}), ", ")

	if nodesPrice-newNodesPrice < 0.0001 {
		msg := fmt.Sprintf("will not replace %s; (total $%.6f/hr) with %s (total $%.6f/hr)", candidateInstanceTypesWithPrices, nodesPrice, newInstanceTypesWithPrices, newNodesPrice)
		logging.FromContext(ctx).
			With("newNodesPrice", newNodesPrice).
			With("candidateNodesPrice", nodesPrice).
			Debug(msg)

		for _, cn := range candidates {
			c.recorder.Publish(deprovisioningevents.Unconsolidatable(cn.Node, msg)...)
		}

		return Command{action: actionDoNothing}, nil
	}

	logging.FromContext(ctx).
		With("newNodesPrice", newNodesPrice).
		With("candidateNodesPrice", nodesPrice).
		Debugf("replace %s; (total $%.6f/hr) with %s (total $%.6f/hr)", candidateInstanceTypesWithPrices, nodesPrice, newInstanceTypesWithPrices, newNodesPrice)

	return Command{
		candidates:   candidates,
		action:       actionReplace,
		replacements: results.NewMachines,
	}, nil
}

// getCandidatePrices returns the sum of the prices of the given candidate nodes
func getCandidatePrices(candidates []*Candidate) (float64, error) {
	var price float64
	for _, c := range candidates {
		offering, ok := c.instanceType.Offerings.Get(c.capacityType, c.zone)
		if !ok {
			return 0.0, fmt.Errorf("unable to determine offering for %s/%s/%s", c.instanceType.Name, c.capacityType, c.zone)
		}
		price += offering.Price
	}
	return price, nil
}
