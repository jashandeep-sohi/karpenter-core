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
	"k8s.io/utils/clock"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/cloudprovider"
	deprovisioningevents "github.com/aws/karpenter-core/pkg/controllers/deprovisioning/events"
	"github.com/aws/karpenter-core/pkg/controllers/provisioning"
	"github.com/aws/karpenter-core/pkg/controllers/state"
	"github.com/aws/karpenter-core/pkg/events"
	"github.com/aws/karpenter-core/pkg/metrics"
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

	candidateInstanceTypes := strings.Join(lo.Map(candidates, func(i *Candidate, _ int) string {
		return i.instanceType.Name
	}), "+")

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

	logging.FromContext(ctx).
		With("newMachineNum", len(results.NewMachines)).
		Debug("scheduling simulation results")

	// if not all of the pods were scheduled, we can't do anything
	if !results.AllPodsScheduled() {
		logging.FromContext(ctx).Debugf("%v", results.PodSchedulingErrors())
		// This method is used by multi-node consolidation as well, so we'll only report in the single node case
		if len(candidates) == 1 {
			c.recorder.Publish(deprovisioningevents.Unconsolidatable(candidates[0].Node, results.PodSchedulingErrors())...)
		}
		return Command{action: actionDoNothing}, nil
	}

	logging.FromContext(ctx).
		Debug("all pods scheduled")

	// were we able to schedule all the pods on the inflight candidates?
	if len(results.NewMachines) == 0 {
		logging.FromContext(ctx).Debug("no new machines needed")
		return Command{
			candidates: candidates,
		}, nil
	}

	// get the current node price based on the offering
	// fallback if we can't find the specific zonal pricing data
	nodesPrice, err := getCandidatePrices(candidates)
	if err != nil {
		return Command{}, fmt.Errorf("getting offering price from candidate node, %w", err)
	}

	if len(results.NewMachines) != 1 {
		newNodesPrice := float64(0)
		for _, n := range results.NewMachines {
			var cheapestInstanceTypeOption *cloudprovider.InstanceType
			for _, i := range n.InstanceTypeOptions {
				cheapestOffer := i.Offerings.Available().Requirements(n.Requirements).Cheapest()
				if cheapestInstanceTypeOption == nil || cheapestOffer.Price < cheapestInstanceTypeOption.Offerings[0].Price {
					cheapestInstanceTypeOption = i
				}
			}
			n.InstanceTypeOptions = cloudprovider.InstanceTypes{cheapestInstanceTypeOption}
			newNodesPrice += cheapestInstanceTypeOption.Offerings.Cheapest().Price
		}

		if newNodesPrice > nodesPrice {
			logging.FromContext(ctx).
				With("newNodesPrice", newNodesPrice).
				With("candidateNodesPrice", nodesPrice).
				Debugf("not replacing candidates with more expensive nodes")

			return Command{action: actionDoNothing}, nil
		}
	} else {
		newInstanceTypesSample := strings.Join(lo.Map(lo.Slice(results.NewMachines[0].InstanceTypeOptions.OrderByPrice(results.NewMachines[0].Requirements), 0, 5), func(i *cloudprovider.InstanceType, _ int) string {
			return fmt.Sprintf("%s ($%.2f)", i.Name, worstLaunchPrice(i.Offerings.Available().Requirements(results.NewMachines[0].Requirements), results.NewMachines[0].Requirements))
		}), "|")

		results.NewMachines[0].InstanceTypeOptions = filterByPrice(results.NewMachines[0].InstanceTypeOptions, results.NewMachines[0].Requirements, nodesPrice)

		if len(results.NewMachines[0].InstanceTypeOptions) == 0 {
			logging.FromContext(ctx).Debugf("can't replace %s ($%.2f) with a cheaper node from %s...", candidateInstanceTypes, nodesPrice, newInstanceTypesSample)

			for _, cn := range candidates {
				c.recorder.Publish(deprovisioningevents.Unconsolidatable(cn.Node, fmt.Sprintf("can't replace %s ($%.2f)/hr with a cheaper node", candidateInstanceTypes, nodesPrice))...)
			}
			// no instance types remain after filtering by price
			return Command{action: actionDoNothing}, nil
		}
	}

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
