/*
Copyright The Kubernetes Authors.

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

package scheduling

import (
	"math"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

const (
	// minPriceSavingsRatio is the minimum price reduction (relative to current
	// price) an earlier scheduling state must offer to be considered a split candidate.
	minPriceSavingsRatio = 0.30
	// minEfficiencyGain is the minimum weighted efficiency improvement an earlier
	// scheduling state must have over the current state to qualify as a split candidate.
	minEfficiencyGain = 0.10
	// memoryGiBToCPURatio is the number of GiB of memory equivalent in cost to
	// 1 vCPU, derived from AWS general-purpose instance family pricing.
	memoryGiBToCPURatio = 9.0
	// maxOptimizationPasses is the number of optimization passes to run before
	// exiting the scheduling loop.
	maxOptimizationPasses = 1
)

// WeightedResourceValue computes a single scalar representing the cost-weighted
// resource footprint. The ratio (1 vCPU ≈ 9× the cost-weight of 1 GiB memory)
// is derived from AWS general-purpose instance family pricing.
func WeightedResourceValue(cpuCores, memGiB float64) float64 {
	return cpuCores + memGiB/memoryGiBToCPURatio
}

// ResourceEfficiencyResult holds utilization ratios for a set of resource
// requests against an instance type's allocatable capacity.
type ResourceEfficiencyResult struct {
	CPU      float64
	Memory   float64
	Weighted float64
}

// ResourceEfficiency computes utilization ratios of requests against the given
// instance type's allocatable resources.
func ResourceEfficiency(requests corev1.ResourceList, it *cloudprovider.InstanceType) ResourceEfficiencyResult {
	alloc := it.Allocatable()
	cpuReq := float64(requests.Cpu().MilliValue()) / 1000
	memReq := float64(requests.Memory().Value()) / (1 << 30) // bytes to GiB
	cpuAlloc := float64(alloc.Cpu().MilliValue()) / 1000
	memAlloc := float64(alloc.Memory().Value()) / (1 << 30) // bytes to GiB

	if cpuAlloc <= 0 || memAlloc <= 0 {
		return ResourceEfficiencyResult{}
	}

	denom := WeightedResourceValue(cpuAlloc, memAlloc)
	weighted := 0.0
	if denom > 0 {
		weighted = WeightedResourceValue(cpuReq, memReq) / denom
	}

	return ResourceEfficiencyResult{
		CPU:      cpuReq / cpuAlloc,
		Memory:   memReq / memAlloc,
		Weighted: weighted,
	}
}

// SchedulingOption captures the NodeClaim's state at an instance type transition
// point. Only stored when the cheapest fitting instance type changes; replaced
// in-place when additional pods still fit the same instance type.
type SchedulingOption struct {
	PodCount         int
	InstanceTypes    []*cloudprovider.InstanceType
	Requirements     scheduling.Requirements
	ResourceRequests corev1.ResourceList
	CheapestInstance *cloudprovider.InstanceType
	Price            float64
}

// cheapestFit finds the cheapest instance type that is compatible with the
// requirements and can fit the given resource requests. Returns nil and +Inf
// if no instance type fits.
func cheapestFit(instanceTypes []*cloudprovider.InstanceType, reqs scheduling.Requirements, totalRequests corev1.ResourceList) (*cloudprovider.InstanceType, float64) {
	var best *cloudprovider.InstanceType
	bestPrice := math.MaxFloat64
	for _, it := range instanceTypes {
		if !compatible(it, reqs) || !resources.Fits(totalRequests, it.Allocatable()) {
			continue
		}
		if p := it.Offerings.Available().WorstLaunchPrice(reqs); p < bestPrice {
			best = it
			bestPrice = p
		}
	}
	return best, bestPrice
}

// estimateCheapestPlacement estimates the cost of placing the given pods on a
// fresh NodeClaim by starting from the NodePool's original requirements and
// instance types. Returns nil and +Inf if no instance type can fit.
func (n *NodeClaim) estimateCheapestPlacement(pods []*corev1.Pod, podData map[types.UID]*PodData) (*cloudprovider.InstanceType, float64) {
	reqs := scheduling.NewRequirements(n.nodePoolRequirements.Values()...)
	for _, pod := range pods {
		if err := reqs.Compatible(podData[pod.UID].Requirements, scheduling.AllowUndefinedWellKnownLabels); err != nil {
			return nil, math.MaxFloat64
		}
		reqs.Add(podData[pod.UID].Requirements.Values()...)
	}
	totalRequests := resources.Merge(n.daemonResources, resources.RequestsForPods(pods...))
	return cheapestFit(n.nodePoolInstanceTypes, reqs, totalRequests)
}

// findSplitPoint identifies an earlier scheduling state that would be cheaper
// and more efficient to revert to. Returns the SchedulingOption to revert to,
// or nil if no valid split is found.
func (n *NodeClaim) findSplitPoint(podData map[types.UID]*PodData) *SchedulingOption {
	if len(n.schedulingOptions) < 2 {
		return nil
	}
	current := n.schedulingOptions[len(n.schedulingOptions)-1]
	currentEfficiency := ResourceEfficiency(current.ResourceRequests, current.CheapestInstance).Weighted

	// Find the first earlier state with at least minPriceSavingsRatio cheaper instance
	var candidate *SchedulingOption
	for i := len(n.schedulingOptions) - 2; i >= 0; i-- {
		savings := (current.Price - n.schedulingOptions[i].Price) / current.Price
		if savings >= minPriceSavingsRatio {
			candidate = &n.schedulingOptions[i]
			break
		}
	}
	if candidate == nil {
		return nil
	}

	// Check that the candidate state is at least minEfficiencyGain more efficient
	candidateEfficiency := ResourceEfficiency(candidate.ResourceRequests, candidate.CheapestInstance).Weighted
	if candidateEfficiency-currentEfficiency < minEfficiencyGain {
		return nil
	}

	// Estimate cost of placing displaced pods on a new NodeClaim
	displacedPods := n.Pods[candidate.PodCount:]
	_, displacedCost := n.estimateCheapestPlacement(displacedPods, podData)
	if candidate.Price+displacedCost >= current.Price {
		return nil
	}

	return candidate
}

// applySplit reverts the NodeClaim to the given scheduling option state.
func (n *NodeClaim) applySplit(split *SchedulingOption) {
	n.Pods = n.Pods[:split.PodCount]
	n.InstanceTypeOptions = split.InstanceTypes
	n.Requirements = split.Requirements
	n.Spec.Resources.Requests = split.ResourceRequests
}

// optimizeNodeClaims examines new NodeClaims for split opportunities. Displaced
// pods are collected into a fresh queue for re-scheduling. Returns true if any
// pods were displaced.
func (s *Scheduler) optimizeNodeClaims(q *Queue) bool {
	var displaced []*corev1.Pod
	for _, nc := range s.newNodeClaims {
		if nc.locked {
			continue
		}
		if split := nc.findSplitPoint(s.cachedPodData); split != nil {
			displaced = append(displaced, nc.Pods[split.PodCount:]...)
			nc.applySplit(split)
		}
		nc.locked = true
	}
	if len(displaced) == 0 {
		return false
	}
	*q = *NewQueue(displaced, s.cachedPodData, byCPUAndMemoryWeightDescending)
	return true
}