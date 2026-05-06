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
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// EnableNodeClaimOptimization opts a Scheduler in to the NodeClaim optimization pass.
var EnableNodeClaimOptimization = func(opts *options) {
	opts.optimizeNodeClaim = true
}

const (
	// minPriceSavingsRatio is the minimum price reduction (relative to current
	// price) an earlier scheduling state must offer to be considered a split candidate.
	minPriceSavingsRatio = 0.24
	// minEfficiencyGain is the minimum weighted efficiency improvement an earlier
	// scheduling state must have over the current state to qualify as a split candidate.
	minEfficiencyGain = 0.10
	// memoryGiBToCPURatio is the number of GiB of memory equivalent in cost to
	// 1 vCPU, derived from AWS general-purpose instance family pricing.
	memoryGiBToCPURatio = 12.0
	// maxOptimizationPasses is the number of optimization passes to run before
	// exiting the scheduling loop.
	maxOptimizationPasses = 2
	// costEpsilon is the minimum cost difference required to accept a split.
	// Guards against floating-point rounding that can make candidate+displaced
	// appear fractionally cheaper than current when the true costs are equal
	// (e.g. c-64x + c-32x == c-96x).
	costEpsilon = 0.0001
	// minNetSavingsRatio is the minimum fraction of current.Price that net
	// savings (current - candidate - displacedCost) must reach for a split
	// to be accepted. Floors out marginal splits whose headline price drop
	// is large but whose net-of-displaced savings are tiny — the class most
	// vulnerable to pass-level composition effects (e.g. two claims' displaced
	// sets merging into an extra spillover claim that no per-claim estimate
	// accounted for).
	minNetSavingsRatio = 0.05
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

// NodeClaimSnapshot is a frozen view of a NodeClaim at a single point in
// time. Values are copied out at capture, so the snapshot is stable across
// later RevertTo / Add mutations of the underlying NodeClaim.
//
// Hostname is the stable identity key: it's assigned once in NewNodeClaim
// and never rewritten (RevertTo does not touch it), so a hostname appearing
// in both OptimizationSnapshot.Pre and .Post identifies the same claim
// across the optimization pass.
//
// CheapestInstance retains the full instance-type pointer (not just the
// name) so consumers can read Capacity / Overhead / Offerings without a
// separate lookup table. Instance types are treated as read-only by the
// scheduler, so sharing the pointer is safe.
type NodeClaimSnapshot struct {
	Hostname         string
	CheapestInstance *cloudprovider.InstanceType
	Price            float64
	Pods             []*corev1.Pod
}

// OptimizationSnapshot bundles the pre/post NodeClaim state and total cost
// for a single Solve call. Pre is captured inside tryOptimize on the first
// pass (before any RevertTo has run); Post is captured at Solve exit. Both
// slices are nil when optimization didn't run (disabled, or Solve exited
// before draining). PreCost/PostCost mirror TotalNodeClaimPrice over the
// respective snapshot — stored explicitly so consumers don't reimplement
// the sum and so the per-run cost invariant is a direct field comparison.
type OptimizationSnapshot struct {
	Pre      []NodeClaimSnapshot
	PreCost  float64
	Post     []NodeClaimSnapshot
	PostCost float64
}

// SnapshotNodeClaims copies out the display-relevant fields of each NodeClaim.
// The pod slice is a fresh slice of the same pointers — safe because
// *corev1.Pod is effectively immutable by the time a NodeClaim holds it,
// and RevertTo only truncates the NodeClaim's own Pods slice, it doesn't
// alter the pod objects. Exported so tests can snapshot arbitrary NodeClaim
// slices (e.g. a baseline Solve result) and diff them against the
// OptimizationSnapshot captured on the Scheduler.
func SnapshotNodeClaims(ncs []*NodeClaim) []NodeClaimSnapshot {
	out := make([]NodeClaimSnapshot, len(ncs))
	for i, nc := range ncs {
		var best *cloudprovider.InstanceType
		price := 0.0
		if len(nc.InstanceTypeOptions) > 0 {
			if it, p := nc.CheapestInstance(); it != nil {
				best = it
				price = p
			}
		}
		pods := make([]*corev1.Pod, len(nc.Pods))
		copy(pods, nc.Pods)
		out[i] = NodeClaimSnapshot{
			Hostname:         nc.hostname,
			CheapestInstance: best,
			Price:            price,
			Pods:             pods,
		}
	}
	return out
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

// optimizationState holds per-NodeClaim state used exclusively by the
// optimization pass. Embedded in NodeClaim to keep field definitions
// co-located with the optimization logic.
type optimizationState struct {
	enabled               bool
	schedulingOptions     []SchedulingOption
	nodePoolRequirements  scheduling.Requirements
	nodePoolInstanceTypes []*cloudprovider.InstanceType
	locked                bool
	// podTopologyDeltas[i] is the TopologyDelta slice returned by
	// Topology.Record when NodeClaim.Pods[i] was added. Lockstep with
	// n.Pods. Populated only when the optimization pass is enabled — see
	// recordPodTopologyDeltas. Read by RevertTo to reverse per-pod
	// topology mutations without re-deriving them from a (potentially
	// tightened) n.Requirements.
	podTopologyDeltas [][]TopologyDelta
}

// newOptimizationState returns the initial optimization state for a new NodeClaim.
// When enabled is false, the Add hook into recordSchedulingOption becomes a no-op
// and no scheduling history is accumulated.
func newOptimizationState(enabled bool, reqs scheduling.Requirements, instanceTypes []*cloudprovider.InstanceType) optimizationState {
	return optimizationState{
		enabled:               enabled,
		nodePoolRequirements:  scheduling.NewRequirements(reqs.Values()...),
		nodePoolInstanceTypes: instanceTypes,
	}
}

// recordSchedulingStep captures the optimization-state side effects of a
// single NodeClaim.Add call. Two pieces of accounting, one gate:
//
//  1. Append the pod's topology deltas to podTopologyDeltas, in lockstep
//     with n.Pods. Always one entry per pod; RevertTo consumes them to
//     reverse per-pod topology mutations.
//  2. If the cheapest compatible instance type for the current resource
//     request changed, snapshot the NodeClaim state into schedulingOptions
//     — one entry per instance-type transition, replaced in place when
//     the same type remains cheapest across multiple pods.
//
// The two pieces have different cardinality (lockstep vs. sparse) but
// share the same self-gate: when the optimization pass is disabled on
// this NodeClaim, the whole function is a no-op and the disabled path
// allocates nothing.
//
// The delta append must precede the instance-type transition logic
// below: the transition branch can early-return on best == nil, and
// skipping the delta append there would break the lockstep invariant
// RevertTo depends on.
func (n *NodeClaim) recordSchedulingStep(instanceTypes []*cloudprovider.InstanceType, reqs scheduling.Requirements, deltas []TopologyDelta) {
	if !n.optimizationState.enabled {
		return
	}
	n.optimizationState.podTopologyDeltas = append(n.optimizationState.podTopologyDeltas, deltas)

	best, price := cheapestFit(instanceTypes, reqs, n.Spec.Resources.Requests)
	if best == nil {
		return
	}
	opt := SchedulingOption{
		PodCount:         len(n.Pods),
		InstanceTypes:    instanceTypes,
		Requirements:     reqs,
		ResourceRequests: n.Spec.Resources.Requests.DeepCopy(),
		CheapestInstance: best,
		Price:            price,
	}
	if last := len(n.optimizationState.schedulingOptions) - 1; last >= 0 && n.optimizationState.schedulingOptions[last].CheapestInstance.Name == best.Name {
		n.optimizationState.schedulingOptions[last] = opt
	} else {
		n.optimizationState.schedulingOptions = append(n.optimizationState.schedulingOptions, opt)
	}
}

// CheapestInstance returns the cheapest instance type currently in
// n.InstanceTypeOptions (and its price) under n.Requirements. The options
// have already been filtered for compatibility and fit at Add time, so this
// is a straight price reduction — no re-check.
//
// Returns (nil, +Inf) for a claim with an empty InstanceTypeOptions list.
//
// Distinct from cheapestFit, which is for hypothetical placements against
// an arbitrary instance-type list and re-runs the compatibility and fit
// checks.
func (n *NodeClaim) CheapestInstance() (*cloudprovider.InstanceType, float64) {
	var best *cloudprovider.InstanceType
	bestPrice := math.MaxFloat64
	for _, it := range n.InstanceTypeOptions {
		if p := it.Offerings.Available().WorstLaunchPrice(n.Requirements); p < bestPrice {
			best = it
			bestPrice = p
		}
	}
	return best, bestPrice
}

// TotalNodeClaimPrice sums CheapestInstance().price across ncs. NodeClaims
// with an empty InstanceTypeOptions list contribute zero (rather than +Inf),
// matching the prior inline behavior in the test-only calculateCost helper.
func TotalNodeClaimPrice(ncs []*NodeClaim) float64 {
	cost := 0.0
	for _, nc := range ncs {
		if len(nc.InstanceTypeOptions) == 0 {
			continue
		}
		_, price := nc.CheapestInstance()
		cost += price
	}
	return cost
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
	reqs := scheduling.NewRequirements(n.optimizationState.nodePoolRequirements.Values()...)
	for _, pod := range pods {
		if err := reqs.Compatible(podData[pod.UID].Requirements, scheduling.AllowUndefinedWellKnownLabels); err != nil {
			return nil, math.MaxFloat64
		}
		reqs.Add(podData[pod.UID].Requirements.Values()...)
	}
	totalRequests := resources.Merge(n.daemonResources, resources.RequestsForPods(pods...))
	return cheapestFit(n.optimizationState.nodePoolInstanceTypes, reqs, totalRequests)
}

// findSplitPoint identifies an earlier scheduling state that would be cheaper
// and more efficient to revert to. Returns the index into
// n.optimizationState.schedulingOptions to revert to, or -1 if no valid
// split is found.
//
// Returning an index (rather than a pointer into the slice's backing array)
// avoids two latent failure modes: a mid-sequence append that reallocates
// the backing array leaves a stale pointer pointing at the old copy, and
// recordSchedulingOption's in-place replacement of the last element can
// mutate data under a caller holding a pointer to it. An index is stable
// under both.
func (n *NodeClaim) findSplitPoint(podData map[types.UID]*PodData) int {
	// Splitting a NodeClaim that holds a reserved offering fragments that
	// reservation's accounting: intersection-based reconciliation retains the
	// seat on the now-locked claim, and the fresh NodeClaim built from the
	// displaced pods cannot reserve it. In strict mode, offeringsToReserve
	// promotes "no compatible reservation could be made" to a scheduling
	// failure, so splitting could turn a utilization regression into a
	// scheduling failure. Skip entirely in that mode — correctness over
	// savings.
	if n.reservedOfferingMode == ReservedOfferingModeStrict && len(n.reservedOfferings) > 0 {
		return -1
	}
	if len(n.optimizationState.schedulingOptions) < 2 {
		return -1
	}
	current := n.optimizationState.schedulingOptions[len(n.optimizationState.schedulingOptions)-1]
	currentEfficiency := ResourceEfficiency(current.ResourceRequests, current.CheapestInstance).Weighted

	// Find the first earlier state with at least minPriceSavingsRatio cheaper instance
	candidateIdx := -1
	for i := len(n.optimizationState.schedulingOptions) - 2; i >= 0; i-- {
		savings := (current.Price - n.optimizationState.schedulingOptions[i].Price) / current.Price
		if savings >= minPriceSavingsRatio {
			candidateIdx = i
			break
		}
	}
	if candidateIdx == -1 {
		return -1
	}
	candidate := n.optimizationState.schedulingOptions[candidateIdx]

	// Check that the candidate state is at least minEfficiencyGain more efficient
	candidateEfficiency := ResourceEfficiency(candidate.ResourceRequests, candidate.CheapestInstance).Weighted
	if candidateEfficiency-currentEfficiency < minEfficiencyGain {
		return -1
	}

	// Estimate cost of placing displaced pods on a new NodeClaim, then gate
	// on two thresholds:
	//  - costEpsilon: rounding-noise guard (e.g. c-64x + c-32x == c-96x
	//    can otherwise appear fractionally cheaper than equal).
	//  - minNetSavingsRatio: policy floor that rejects materially-thin wins.
	// Both checks are kept even though minNetSavingsRatio > 0 subsumes
	// costEpsilon today: if the ratio is ever tuned to 0 (e.g. in a test or
	// a "save anything positive" configuration), the epsilon check still
	// protects against float-equal cases being mis-classified as savings.
	displacedPods := n.Pods[candidate.PodCount:]
	_, displacedCost := n.estimateCheapestPlacement(displacedPods, podData)
	netSavings := current.Price - candidate.Price - displacedCost
	if netSavings <= costEpsilon {
		return -1
	}
	if netSavings/current.Price < minNetSavingsRatio {
		return -1
	}

	return candidateIdx
}

// releasePodFromSharedState undoes the shared-state effects of NodeClaim.Add
// for a single displaced pod. Mirror of the shared-state lines in Add:
//
//	n.topology.Record(pod, ...)    -> n.topology.Unrecord(deltas)
//	n.hostPortUsage.Add(pod, ...)  -> n.hostPortUsage.DeletePod(key)
//
// The topology reversal is driven by the deltas stored at Add time rather
// than by re-deriving from n.Requirements, which has typically tightened
// since the pod was added and would misidentify the domains to decrement.
//
// Reservations are NOT reversed here — they're per-hostname, not per-pod.
// See reconcileReservations for the post-revert reconciliation step.
func (n *NodeClaim) releasePodFromSharedState(pod *corev1.Pod, deltas []TopologyDelta) {
	n.topology.Unrecord(deltas)
	n.hostPortUsage.DeletePod(client.ObjectKeyFromObject(pod))
}

// reconcileReservations reduces n.reservedOfferings to the intersection of
// what the NodeClaim currently holds and what was reachable at the
// snapshot. Offerings in the intersection are retained; anything else is
// released back to the shared pool via releaseReservedOfferings.
//
// Why intersection, not "snapshot minus current" or "current minus
// snapshot":
//   - The set of reserved offerings a NodeClaim holds can expand AND
//     contract across Add iterations (see nodeclaim.go:54–58). An offering
//     may have been in the snapshot but later dropped when requirements
//     tightened, or acquired mid-sequence when another claim released a
//     seat. Neither direction is monotonic, so a simple diff is wrong.
//   - Seats acquired for displaced pods should return to the pool so a
//     fresh NodeClaim built from those pods can reserve them.
//   - Seats dropped mid-sequence are already gone; no re-acquire path —
//     the claim is about to be locked and won't need them.
//   - Seats held in both are already correct in both the claim and the
//     manager and need no action.
//
// The snapshot's reservation ID set is rebuilt from
// snapshot.InstanceTypes[*].Offerings rather than stored on
// SchedulingOption. n.InstanceTypeOptions narrows monotonically across
// Adds, so every reservation ID reachable through a currently-held
// offering is also reachable through the snapshot's instance types —
// completeness is guaranteed without an extra field.
func (n *NodeClaim) reconcileReservations(snapshot *SchedulingOption) {
	snapshotIDs := sets.New[string]()
	for _, it := range snapshot.InstanceTypes {
		for _, o := range it.Offerings {
			if o.CapacityType() == v1.CapacityTypeReserved {
				snapshotIDs.Insert(o.ReservationID())
			}
		}
	}
	var retained cloudprovider.Offerings
	for _, o := range n.reservedOfferings {
		if snapshotIDs.Has(o.ReservationID()) {
			retained = append(retained, o)
		}
	}
	// Delegate the actual release to the existing helper used by Add.
	// Same symmetry as tenet 3: a single named inverse per mutation.
	n.releaseReservedOfferings(n.reservedOfferings, retained)
	n.reservedOfferings = retained
}

// RevertTo rolls the NodeClaim back to the state captured in
// schedulingOptions[index] and returns the pods that were displaced — the
// ones added after the snapshot — in Add order, safe to re-queue onto a
// fresh NodeClaim.
//
// Preconditions:
//   - 0 <= index < len(n.optimizationState.schedulingOptions).
//   - The NodeClaim must not be locked. Enforced by the caller.
//
// Postconditions:
//   - n.Pods, n.InstanceTypeOptions, n.Requirements, n.Spec.Resources.Requests
//     match the state at the snapshot.
//   - Topology groups and HostPortUsage reflect only the pods still on
//     n.Pods: per-pod deltas recorded at Add time have been replayed in
//     reverse on Topology; DeletePod has been called for each displaced
//     pod on HostPortUsage.
//   - n.reservedOfferings is the intersection of the offerings held before
//     the revert and those reachable through the snapshot's instance
//     types. Anything outside the intersection has been released back to
//     the shared pool.
//   - n.optimizationState.schedulingOptions is truncated to [:index].
//   - n.optimizationState.podTopologyDeltas is truncated to [:PodCount],
//     preserving the lockstep-with-n.Pods invariant.
//
// RevertTo is infallible. No error return; no partial-state recovery path.
// Any error return would force the caller into a "half-reverted" recovery
// branch that can't safely exist — the claim is about to be locked.
func (n *NodeClaim) RevertTo(index int) []*corev1.Pod {
	snapshot := &n.optimizationState.schedulingOptions[index]
	displaced := n.Pods[snapshot.PodCount:]

	// Reverse per-pod shared-state mutations first. Deltas are
	// self-describing, so ordering against local-state restore is cosmetic;
	// doing it first keeps the Add <-> RevertTo symmetry legible.
	displacedDeltas := n.optimizationState.podTopologyDeltas[snapshot.PodCount:]
	for i, pod := range displaced {
		n.releasePodFromSharedState(pod, displacedDeltas[i])
	}

	// Reconcile per-hostname reservations before overwriting
	// n.InstanceTypeOptions — reconcileReservations reads from
	// snapshot.InstanceTypes directly, but ordering here is defensive in
	// case a future change shifts the source of truth.
	n.reconcileReservations(snapshot)

	n.Pods = n.Pods[:snapshot.PodCount]
	n.InstanceTypeOptions = snapshot.InstanceTypes
	n.Requirements = snapshot.Requirements
	n.Spec.Resources.Requests = snapshot.ResourceRequests

	n.optimizationState.schedulingOptions = n.optimizationState.schedulingOptions[:index]
	n.optimizationState.podTopologyDeltas = n.optimizationState.podTopologyDeltas[:snapshot.PodCount]

	return displaced
}

// optimizeNodeClaims examines new NodeClaims for split opportunities. Displaced
// pods are collected into a fresh queue for re-scheduling. Returns true if any
// pods were displaced.
func (s *Scheduler) optimizeNodeClaims(q *Queue) bool {
	var displaced []*corev1.Pod
	for _, nc := range s.newNodeClaims {
		if nc.optimizationState.locked {
			continue
		}
		if idx := nc.findSplitPoint(s.cachedPodData); idx >= 0 {
			displaced = append(displaced, nc.RevertTo(idx)...)
		}
		nc.optimizationState.locked = true
	}
	if len(displaced) == 0 {
		return false
	}
	*q = *NewQueue(displaced, s.cachedPodData)
	return true
}

// tryOptimize runs a single optimization pass if optimization is enabled and
// the pass limit has not been reached. Returns true if pods were displaced.
//
// On first entry (OptimizationPasses == 0), captures the Pre half of
// OptimizationSnapshot — total price and per-NodeClaim state — before any
// revert mutates the claim set, so tests can compare the post-Solve cost
// against this baseline to verify each split decision paid off against its
// own estimate.
func (s *Scheduler) tryOptimize(q *Queue) bool {
	if !s.OptimizationEnabled || s.OptimizationPasses >= maxOptimizationPasses {
		return false
	}
	if s.OptimizationPasses == 0 {
		s.OptimizationSnapshot.PreCost = TotalNodeClaimPrice(s.newNodeClaims)
		s.OptimizationSnapshot.Pre = SnapshotNodeClaims(s.newNodeClaims)
	}
	if !s.optimizeNodeClaims(q) {
		return false
	}
	s.OptimizationPasses++
	return true
}
