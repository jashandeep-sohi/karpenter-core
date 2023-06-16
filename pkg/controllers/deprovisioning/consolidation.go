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
		var err error

		spotConsolidationAnnotationValue, hasSpotConsolidationAnnotation := cn.Node.Annotations[v1alpha5.SpotConsolidationAnnotationKey]

		if !hasSpotConsolidationAnnotation || spotConsolidationAnnotationValue != "true" {
			c.recorder.Publish(deprovisioningevents.Unconsolidatable(cn.Node, "spot node is not annotated to allow consolidation")...)
			return false
		}

		// parse the annoations
		spotConsolidationWaitAfterCreationAnnotationValue, hasSpotConsolidationWaitAfterCreationAnnotation := cn.Node.Annotations[v1alpha5.SpotConsolidationeWaitAfterCreationAnnotationKey]
		spotConsolidationWaitAfterCreation := 5 * time.Minute

		if hasSpotConsolidationWaitAfterCreationAnnotation {
			spotConsolidationWaitAfterCreation, err = time.ParseDuration(spotConsolidationWaitAfterCreationAnnotationValue)

			if err != nil {
				c.recorder.Publish(deprovisioningevents.Unconsolidatable(cn.Node, fmt.Sprintf("unable to parse %s annotation duration: %v", v1alpha5.SpotConsolidationeWaitAfterCreationAnnotationKey, err))...)
				return false
			}
		}

		spotConsolidationWaitAfterEmptyAnnotationValue, hasSpotConsolidationWaitAfterEmptyAnnotation := cn.Node.Annotations[v1alpha5.SpotConsolidationeWaitAfterEmptyAnnotationKey]
		spotConsolidationWaitAfterEmpty := 5 * time.Minute

		if hasSpotConsolidationWaitAfterEmptyAnnotation {
			spotConsolidationWaitAfterEmpty, err = time.ParseDuration(spotConsolidationWaitAfterEmptyAnnotationValue)

			if err != nil {
				c.recorder.Publish(deprovisioningevents.Unconsolidatable(cn.Node, fmt.Sprintf("unable to parse %s annotation duration: %v", v1alpha5.SpotConsolidationeWaitAfterEmptyAnnotationKey, err))...)
				return false
			}
		}

		// if spot-consolidation-wait-after-creation timeout has expired, it takes precedence
		if c.clock.Now().After(cn.Node.CreationTimestamp.Add(spotConsolidationWaitAfterCreation)) {
			return true
		}

		c.recorder.Publish(deprovisioningevents.Unconsolidatable(cn.Node, fmt.Sprintf("waiting for %s=%s to expire", v1alpha5.SpotConsolidationeWaitAfterCreationAnnotationKey, spotConsolidationWaitAfterCreationAnnotationValue))...)

		// otherwise check if the empty timeout has expired
		emptinessTimestamp, hasEmptinessTimestamp := cn.Node.Annotations[v1alpha5.EmptinessTimestampAnnotationKey]
		if !hasEmptinessTimestamp {
			return false
		}

		emptinessTime, err := time.Parse(time.RFC3339, emptinessTimestamp)
		if err != nil {
			c.recorder.Publish(deprovisioningevents.Unconsolidatable(cn.Node, fmt.Sprintf("unable to parse emptiness timestamp %s: %v", emptinessTimestamp, err))...)
			return false
		}

		if c.clock.Now().Before(emptinessTime.Add(spotConsolidationWaitAfterEmpty)) {
			c.recorder.Publish(deprovisioningevents.Unconsolidatable(cn.Node, fmt.Sprintf("waiting for %s=%s to expire", v1alpha5.SpotConsolidationeWaitAfterEmptyAnnotationKey, spotConsolidationWaitAfterEmptyAnnotationValue))...)
			return false
		}
	}

	return true
}

// computeConsolidation computes a consolidation action to take
//
// nolint:gocyclo
func (c *consolidation) computeConsolidation(ctx context.Context, candidates ...*Candidate) (Command, error) {
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
		logging.FromContext(ctx).
			With("podErros", lo.MapKeys(results.PodErrors, func(_ error, p *v1.Pod) string {
				return fmt.Sprintf("%s/%s", p.Namespace, p.Name)
			})).
			Debugf("failed to schedule all pods")

		// This method is used by multi-node consolidation as well, so we'll only report in the single node case
		if len(candidates) == 1 {
			c.recorder.Publish(deprovisioningevents.Unconsolidatable(candidates[0].Node, results.PodSchedulingErrors())...)
		}
		return Command{action: actionDoNothing}, nil
	}

	// were we able to schedule all the pods on the inflight candidates?
	if len(results.NewMachines) == 0 {
		logging.FromContext(ctx).Debugf("all Pods can fit without candidate(s)")
		return Command{
			candidates: candidates,
			action:     actionDelete,
		}, nil
	}

	// we're not going to turn a single node into multiple candidates
	if len(results.NewMachines) != 1 {
		logging.FromContext(ctx).Debugf("will not consolidate to more than 1 node")
		if len(candidates) == 1 {
			c.recorder.Publish(deprovisioningevents.Unconsolidatable(candidates[0].Node, fmt.Sprintf("can't remove without creating %d candidates", len(results.NewMachines)))...)
		}
		return Command{action: actionDoNothing}, nil
	}

	// get the current node price based on the offering
	// fallback if we can't find the specific zonal pricing data
	nodesPrice, err := getCandidatePrices(candidates)
	if err != nil {
		return Command{}, fmt.Errorf("getting offering price from candidate node, %w", err)
	}

	resultMachine := results.NewMachines[0]

	cheaperInstanceTypeOptions := lo.FilterMap(resultMachine.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) (*cloudprovider.InstanceType, bool) {
		cheaperOfferings := lo.Filter(it.Offerings.Available().Requirements(resultMachine.Requirements), func(of cloudprovider.Offering, _ int) bool {
			if nodesPrice-of.Price < 0.001 {
				return false
			}
			return true
		})

		if len(cheaperOfferings) == 0 {
			return nil, false
		}

		itCopy := *it
		itCopy.Offerings = cheaperOfferings

		return &itCopy, true
	})

	candidateInstanceTypesWithPrices := strings.Join(
		lo.Map(candidates, func(c *Candidate, _ int) string {
			of, _ := c.instanceType.Offerings.Get(c.capacityType, c.zone)
			return fmt.Sprintf("%s/%s/%s/%s ($%.5f/hr)", c.Name(), c.instanceType.Name, c.capacityType, c.zone, of.Price)
		}),
		", ",
	)

	if len(cheaperInstanceTypeOptions) == 0 {
		msg := fmt.Sprintf("no cheaper options found for %s", candidateInstanceTypesWithPrices)

		logging.FromContext(ctx).Debug(msg)

		for _, cn := range candidates {
			c.recorder.Publish(deprovisioningevents.Unconsolidatable(cn.Node, msg)...)
		}

		return Command{action: actionDoNothing}, nil
	}

	// limit result machine to cheaper instance types
	resultMachine.InstanceTypeOptions = cheaperInstanceTypeOptions

	newInstanceTypesWithPrices := strings.Join(
		lo.FlatMap(resultMachine.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) []string {
			return lo.Map(it.Offerings, func(of cloudprovider.Offering, _ int) string {
				return fmt.Sprintf("%s/%s/%s ($%.5f/hr)", it.Name, of.CapacityType, of.Zone, of.Price)
			})
		}),
		", ",
	)

	logging.FromContext(ctx).Debugf("replace %s from %s", candidateInstanceTypesWithPrices, newInstanceTypesWithPrices)

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
