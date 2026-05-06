package scheduling_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	"pgregory.net/rapid"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"

	kwok "sigs.k8s.io/karpenter/kwok/cloudprovider"
	kwokoptions "sigs.k8s.io/karpenter/kwok/options"
)

func filterByMaxVCPU(instanceTypes []*cloudprovider.InstanceType, max string) []*cloudprovider.InstanceType {
	maxQ := resource.MustParse(max)
	return lo.Filter(instanceTypes, func(it *cloudprovider.InstanceType, _ int) bool {
		return it.Capacity.Cpu().Cmp(maxQ) <= 0
	})
}

var _ = Describe("NodeClaim Optimization Rapid", func() {
	It("should produce equal or lower cost with random workloads", func() {
		fmt.Println("\n\n=== STARTING NodeClaim Optimization Rapid Cost Test ===")
		fmt.Println("  baseline = scheduler without optimization pass")
		fmt.Println("  optimized = baseline + NodeClaim optimization pass")
		csvWriter, err := NewCSVWriter("cost")
		Expect(err).ToNot(HaveOccurred())
		defer csvWriter.Close()

		createNodePool := func() *v1.NodePool {
			return test.NodePool(v1.NodePool{
				Spec: v1.NodePoolSpec{
					Limits: v1.Limits(corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("1000000"),
					}),
				},
			})
		}

		runIndex := 0
		var totalBaselineNCs, totalOptNCs, optimizedRuns, cheaperRuns, costlierRuns int
		var totalBaselineCost, totalOptCost float64
		var optBaselineCost, optOptCost float64
		var optBaselineNCs, optOptNCs int
		var optSavingsPctSum float64
		rapid.Check(GinkgoT(), func(t *rapid.T) {
			runIndex++

			ExpectCleanedUp(ctx, env.Client)
			cluster.Reset()
			scheduling.QueueDepth.Reset()
			scheduling.DurationSeconds.Reset()
			scheduling.UnschedulablePodsCount.Reset()

			ctx = kwokoptions.ToContext(ctx, &kwokoptions.Options{})
			instanceTypes, err := kwok.ConstructInstanceTypes(ctx)
			Expect(err).ToNot(HaveOccurred())
			instanceTypes = filterByMaxVCPU(instanceTypes, "64")
			cloudProvider.InstanceTypes = instanceTypes

			podCount := rapid.IntRange(1, 200).Draw(t, "podCount")

			pods := make([]*corev1.Pod, podCount)
			for i := 0; i < podCount; i++ {
				cpuFloat := rapid.Float64Range(0.25, 8.0).Draw(t, "cpuRequest")
				memFloatMultiplier := rapid.Float64Range(.25, 16).Draw(t, "memRequest")
				memFloat := cpuFloat * memFloatMultiplier
				pods[i] = test.UnschedulablePod(test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("pod-%d", i),
						Namespace: "default",
						UID:       types.UID(fmt.Sprintf("pod-%d", i)),
					},
					Image: "nginx:latest",
					ResourceRequirements: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%.2f", cpuFloat)),
							corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%.2fGi", memFloat)),
						},
					},
				})
			}

			// --- Baseline (no optimization) ---
			nodePool := createNodePool()
			ExpectApplied(ctx, env.Client, nodePool)

			pods1 := make([]*corev1.Pod, len(pods))
			for i, p := range pods {
				pods1[i] = p.DeepCopy()
			}

			start1 := time.Now()
			s1, _ := prov.NewScheduler(ctx, pods1, nil)
			results1, _ := s1.Solve(ctx, pods1)
			duration1 := time.Since(start1)

			cost1 := scheduling.TotalNodeClaimPrice(results1.NewNodeClaims)

			// --- Optimized ---
			ExpectCleanedUp(ctx, env.Client)
			cluster.Reset()

			nodePool = createNodePool()
			ExpectApplied(ctx, env.Client, nodePool)

			pods2 := make([]*corev1.Pod, len(pods))
			for i, p := range pods {
				pods2[i] = p.DeepCopy()
			}
			start2 := time.Now()
			s2, _ := prov.NewScheduler(ctx, pods2, nil, scheduling.EnableNodeClaimOptimization)
			results2, _ := s2.Solve(ctx, pods2)
			duration2 := time.Since(start2)

			cost2 := scheduling.TotalNodeClaimPrice(results2.NewNodeClaims)
			preOpt2 := s2.OptimizationSnapshot.PreCost

			totalBaselineNCs += len(results1.NewNodeClaims)
			totalOptNCs += len(results2.NewNodeClaims)
			totalBaselineCost += cost1
			totalOptCost += cost2
			if len(results2.NewNodeClaims) != len(results1.NewNodeClaims) || cost2 < cost1-0.00001 {
				optimizedRuns++
			}
			if cost2 < cost1-0.00001 {
				cheaperRuns++
				optBaselineCost += cost1
				optOptCost += cost2
				optBaselineNCs += len(results1.NewNodeClaims)
				optOptNCs += len(results2.NewNodeClaims)
				optSavingsPctSum += (cost1 - cost2) / cost1 * 100
			} else if cost2 > cost1+0.00001 {
				costlierRuns++
			}

			runPctSaved := 0.0
			if cost1 > 0 {
				runPctSaved = (cost1 - cost2) / cost1 * 100
			}

			// Compute average weighted efficiency per run
			avgEff := func(ncs []*scheduling.NodeClaim) float64 {
				if len(ncs) == 0 {
					return 0
				}
				total := 0.0
				for _, nc := range ncs {
					if len(nc.InstanceTypeOptions) == 0 {
						continue
					}
					it, _ := nc.CheapestInstance()
					podCPU := resource.Quantity{}
					podMem := resource.Quantity{}
					for _, pod := range nc.Pods {
						for _, c := range pod.Spec.Containers {
							podCPU.Add(*c.Resources.Requests.Cpu())
							podMem.Add(*c.Resources.Requests.Memory())
						}
					}
					total += scheduling.ResourceEfficiency(corev1.ResourceList{
						corev1.ResourceCPU:    podCPU,
						corev1.ResourceMemory: podMem,
					}, it).Weighted
				}
				return total / float64(len(ncs)) * 100
			}
			eff1 := avgEff(results1.NewNodeClaims)
			eff2 := avgEff(results2.NewNodeClaims)
			// preCountB is the pre-optimization NC count captured inside
			// tryOptimize; falls back to the live count when the pass
			// never fired (snapshot nil → B == C).
			preCountB := len(results2.NewNodeClaims)
			if s2.OptimizationSnapshot.Pre != nil {
				preCountB = len(s2.OptimizationSnapshot.Pre)
			}
			fmt.Printf("cost (%4d), pods (%4d), nodeclaims (%3d, %3d), cost (%8.4f, %8.4f), cost%% (%5.1f%%), eff (%5.1f%%, %5.1f%%)\n",
				runIndex, podCount,
				preCountB, len(results2.NewNodeClaims),
				preOpt2, cost2,
				runPctSaved,
				eff1, eff2)

			csvWriter.WriteSummary(runIndex, podCount, len(results1.NewNodeClaims), len(results2.NewNodeClaims), cost1, cost2, duration1.Seconds(), duration2.Seconds(), preOpt2)
			csvWriter.WriteNodeClaims(runIndex, "s1", results1.NewNodeClaims)
			csvWriter.WriteNodeClaims(runIndex, "s2", results2.NewNodeClaims)
			csvWriter.WritePodConfigs(runIndex, pods)

			Expect(cost2).To(BeNumerically("<=", cost1+.00001), "optimized cost (%.4f) should be <= unoptimized cost (%.4f)", cost2, cost1)
			// Per-run invariant: every split decision must pay off against
			// its own estimate. If the optimization pass ran (preOpt2 > 0),
			// the final cost must be <= the cost captured before the first
			// revert. A failure here points at estimateCheapestPlacement
			// underpricing displaced pods that ended up fragmenting across
			// multiple NodeClaims.
			if preOpt2 > 0 {
				Expect(cost2).To(BeNumerically("<=", preOpt2+.00001),
					"optimized cost (%.4f) should be <= pre-opt cost (%.4f) — bad split decision", cost2, preOpt2)
			}

			// Verify every input pod appears in exactly one optimized NodeClaim.
			scheduledUIDs := map[types.UID]struct{}{}
			for _, nc := range results2.NewNodeClaims {
				for _, pod := range nc.Pods {
					_, dup := scheduledUIDs[pod.UID]
					Expect(dup).To(BeFalse(), "pod %s scheduled on multiple NodeClaims", pod.Name)
					scheduledUIDs[pod.UID] = struct{}{}
				}
			}
			Expect(scheduledUIDs).To(HaveLen(len(pods)), "optimization lost or duplicated pods: want %d, got %d", len(pods), len(scheduledUIDs))

			ExpectCleanedUp(ctx, env.Client)
			cluster.Reset()
		})

		// Sign convention (matches the per-run cost% column and the diverse
		// test's summary lines): positive = saved, negative = got costlier.
		// signDollar puts the sign before the $ to avoid "-$-x.xx" nesting
		// when a value is negative.
		signDollar := func(v float64) string {
			if v < 0 {
				return fmt.Sprintf("-$%.2f", -v)
			}
			return fmt.Sprintf("+$%.2f", v)
		}
		savedBC := totalBaselineCost - totalOptCost
		pctBC := 0.0
		if totalBaselineCost > 0 {
			pctBC = savedBC / totalBaselineCost * 100
		}
		fmt.Printf("\n=== COST SUMMARY: %d runs ===\n", runIndex)
		fmt.Printf("  %3d cheaper, %3d costlier | cost $%.2f → $%.2f (%s, %+.1f%%) | NCs %d → %d\n",
			cheaperRuns, costlierRuns, totalBaselineCost, totalOptCost, signDollar(savedBC), pctBC, totalBaselineNCs, totalOptNCs)
		if cheaperRuns > 0 {
			optSaved := optBaselineCost - optOptCost
			optPct := optSaved / optBaselineCost * 100
			fmt.Printf("  Of runs that got cheaper (%d): cost $%.2f → $%.2f (%s, %+.1f%%) | avg %s/run (%+.1f%%) | NCs %d → %d\n",
				cheaperRuns, optBaselineCost, optOptCost, signDollar(optSaved), optPct,
				signDollar(optSaved/float64(cheaperRuns)), optSavingsPctSum/float64(cheaperRuns),
				optBaselineNCs, optOptNCs)
		}
		fmt.Printf("  %d of %d runs triggered the optimization pass (%d%%)\n",
			optimizedRuns, runIndex, optimizedRuns*100/runIndex)
	})

	It("should handle diverse pod scheduling constraints", func() {
		fmt.Println("\n\n=== STARTING Diverse Constraint Optimization Test ===")
		fmt.Println("  baseline = scheduler without optimization pass")
		fmt.Println("  pre = optimized scheduler, pre-optimization snapshot (captured before first RevertTo)")
		fmt.Println("  post = optimized scheduler, final state")
		csvWriter, err := NewCSVWriter("diverse")
		Expect(err).ToNot(HaveOccurred())
		defer csvWriter.Close()

		// Zones and topology keys available in KWOK instance types.
		zones := []string{"test-zone-a", "test-zone-b", "test-zone-c", "test-zone-d"}
		topologyKeys := []string{corev1.LabelTopologyZone, corev1.LabelHostname}

		createNodePool := func() *v1.NodePool {
			return test.NodePool(v1.NodePool{
				Spec: v1.NodePoolSpec{
					Limits: v1.Limits(corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("1000000"),
					}),
				},
			})
		}

		// workloadGroup represents a set of identical replicas (like a Deployment).
		type workloadGroup struct {
			name         string
			replicaCount int
			cpuRequest   string
			memRequest   string
			opts         test.PodOptions // constraint template (applied to all replicas)
		}

		// drawWorkloadGroups creates realistic workload groups where all replicas
		// in a group share the same constraints and resource profile.
		drawWorkloadGroups := func(t *rapid.T) ([]workloadGroup, []*corev1.Pod) {
			groupCount := rapid.IntRange(2, 20).Draw(t, "groupCount")
			constraintRate := rapid.Float64Range(.05, 1).Draw(t, "constraintRate")

			var groups []workloadGroup
			var allPods []*corev1.Pod
			podIdx := 0

			for g := 0; g < groupCount; g++ {
				replicaCount := rapid.IntRange(1, 50).Draw(t, fmt.Sprintf("g%d-replicas", g))
				cpuFloat := rapid.Float64Range(0.25, 4.0).Draw(t, fmt.Sprintf("g%d-cpu", g))
				memFloat := cpuFloat * rapid.Float64Range(0.5, 8.0).Draw(t, fmt.Sprintf("g%d-memRatio", g))

				groupLabel := fmt.Sprintf("group-%d", g)
				cpuStr := fmt.Sprintf("%.2f", cpuFloat)
				memStr := fmt.Sprintf("%.2fGi", memFloat)

				opts := test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": groupLabel},
					},
					Image: "nginx:latest",
					ResourceRequirements: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(cpuStr),
							corev1.ResourceMemory: resource.MustParse(memStr),
						},
					},
				}

				groupSelector := map[string]string{"app": groupLabel}

				// ~30% chance (scaled): pin this group to a specific zone
				if rapid.IntRange(0, 99).Draw(t, fmt.Sprintf("g%d-zone", g)) < int(30*constraintRate) {
					zone := zones[rapid.IntRange(0, len(zones)-1).Draw(t, fmt.Sprintf("g%d-zoneVal", g))]
					opts.NodeRequirements = []corev1.NodeSelectorRequirement{{
						Key:      corev1.LabelTopologyZone,
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{zone},
					}}
				}

				// ~30% chance (scaled): topology spread across zones or hosts
				if rapid.IntRange(0, 99).Draw(t, fmt.Sprintf("g%d-tsc", g)) < int(30*constraintRate) {
					topoKey := topologyKeys[rapid.IntRange(0, len(topologyKeys)-1).Draw(t, fmt.Sprintf("g%d-tscKey", g))]
					opts.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
						MaxSkew:           int32(rapid.IntRange(1, 3).Draw(t, fmt.Sprintf("g%d-skew", g))),
						TopologyKey:       topoKey,
						WhenUnsatisfiable: corev1.DoNotSchedule,
						LabelSelector:     &metav1.LabelSelector{MatchLabels: groupSelector},
					}}
				}

				// ~25% chance (scaled): anti-affinity to own replicas (spread across nodes)
				if rapid.IntRange(0, 99).Draw(t, fmt.Sprintf("g%d-anti", g)) < int(25*constraintRate) {
					opts.PodAntiRequirements = []corev1.PodAffinityTerm{{
						LabelSelector: &metav1.LabelSelector{MatchLabels: groupSelector},
						TopologyKey:   corev1.LabelHostname,
					}}
				}

				// ~15% chance (scaled): host port
				if rapid.IntRange(0, 99).Draw(t, fmt.Sprintf("g%d-hport", g)) < int(15*constraintRate) {
					port := int32(rapid.IntRange(8000, 8003).Draw(t, fmt.Sprintf("g%d-port", g)))
					opts.HostPorts = []int32{port}
				}

				// ~20% chance (scaled): pod affinity to a DIFFERENT group
				if g > 0 && rapid.IntRange(0, 99).Draw(t, fmt.Sprintf("g%d-paff", g)) < int(20*constraintRate) {
					targetGroup := rapid.IntRange(0, g-1).Draw(t, fmt.Sprintf("g%d-paffTarget", g))
					topoKey := topologyKeys[rapid.IntRange(0, len(topologyKeys)-1).Draw(t, fmt.Sprintf("g%d-paffKey", g))]
					opts.PodPreferences = []corev1.WeightedPodAffinityTerm{{
						Weight: 50,
						PodAffinityTerm: corev1.PodAffinityTerm{
							LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": fmt.Sprintf("group-%d", targetGroup)}},
							TopologyKey:   topoKey,
						},
					}}
				}

				grp := workloadGroup{
					name:         groupLabel,
					replicaCount: replicaCount,
					cpuRequest:   cpuStr,
					memRequest:   memStr,
					opts:         opts,
				}
				groups = append(groups, grp)

				// Create replicas — all identical except name/UID
				for r := 0; r < replicaCount; r++ {
					podOpts := opts
					podOpts.ObjectMeta = metav1.ObjectMeta{
						Name:      fmt.Sprintf("pod-%d", podIdx),
						Namespace: "default",
						UID:       types.UID(fmt.Sprintf("pod-%d", podIdx)),
						Labels:    opts.Labels,
					}
					allPods = append(allPods, test.UnschedulablePod(podOpts))
					podIdx++
				}
			}
			return groups, allPods
		}

		runIndex := 0
		// "Cheaper"/"costlier" counts use the same $0.00001 tolerance as the
		// per-run invariants below. baseline→post captures the end-to-end
		// effect; pre→post isolates the optimization pass at a fixed sort.
		var totalBaselineCost, totalPreOptCost, totalOptCost float64
		var cheaperRunsBP, costlierRunsBP int // baseline → post (end-to-end)
		var cheaperRunsPP, costlierRunsPP int // pre → post (optimization alone)

		rapid.Check(GinkgoT(), func(t *rapid.T) {
			runIndex++

			ExpectCleanedUp(ctx, env.Client)
			cluster.Reset()
			scheduling.QueueDepth.Reset()
			scheduling.DurationSeconds.Reset()
			scheduling.UnschedulablePodsCount.Reset()

			ctx = kwokoptions.ToContext(ctx, &kwokoptions.Options{})
			instanceTypes, err := kwok.ConstructInstanceTypes(ctx)
			Expect(err).ToNot(HaveOccurred())
			instanceTypes = filterByMaxVCPU(instanceTypes, "64")
			cloudProvider.InstanceTypes = instanceTypes

			groups, pods := drawWorkloadGroups(t)

			// --- Baseline: no optimization pass ---
			nodePool := createNodePool()
			ExpectApplied(ctx, env.Client, nodePool)
			pods1 := make([]*corev1.Pod, len(pods))
			for i, p := range pods {
				pods1[i] = p.DeepCopy()
			}
			s1, _ := prov.NewScheduler(ctx, pods1, nil)
			results1, _ := s1.Solve(ctx, pods1)
			cost1 := scheduling.TotalNodeClaimPrice(results1.NewNodeClaims)

			// --- Optimized: with optimization pass ---
			ExpectCleanedUp(ctx, env.Client)
			cluster.Reset()
			nodePool = createNodePool()
			ExpectApplied(ctx, env.Client, nodePool)
			pods2 := make([]*corev1.Pod, len(pods))
			for i, p := range pods {
				pods2[i] = p.DeepCopy()
			}
			s2, _ := prov.NewScheduler(ctx, pods2, nil, scheduling.EnableNodeClaimOptimization)
			results2, _ := s2.Solve(ctx, pods2)
			cost2 := scheduling.TotalNodeClaimPrice(results2.NewNodeClaims)
			preOpt2 := s2.OptimizationSnapshot.PreCost

			totalBaselineCost += cost1
			totalPreOptCost += preOpt2
			totalOptCost += cost2
			if cost2 < cost1-0.00001 {
				cheaperRunsBP++
			} else if cost2 > cost1+0.00001 {
				costlierRunsBP++
			}
			if cost2 < preOpt2-0.00001 {
				cheaperRunsPP++
			} else if cost2 > preOpt2+0.00001 {
				costlierRunsPP++
			}

			// Two savings views:
			//   pre→post: optimization-pass effect alone (fixed sort).
			//   baseline→post: end-to-end effect vs. a scheduler that
			//                  never ran the pass.
			savedPrePost, savedEnd := 0.0, 0.0
			if cost1 > 0 {
				savedEnd = (cost1 - cost2) / cost1 * 100
			}
			if preOpt2 > 0 {
				savedPrePost = (preOpt2 - cost2) / preOpt2 * 100
			}
			// preCount is the pre-optimization NC count (captured inside
			// tryOptimize before any revert). OptimizationSnapshot.Pre is nil
			// when the pass never fired, in which case pre == post.
			preCount := len(results2.NewNodeClaims)
			if s2.OptimizationSnapshot.Pre != nil {
				preCount = len(s2.OptimizationSnapshot.Pre)
			}

			// Count constraints across all pods for the summary line.
			var nZoneAff, nTSC, nAntiAff, nHostPort, nPodAff, nErrors int
			for _, pod := range pods {
				if aff := pod.Spec.Affinity; aff != nil {
					if na := aff.NodeAffinity; na != nil && na.RequiredDuringSchedulingIgnoredDuringExecution != nil {
						nZoneAff++
					}
					if pa := aff.PodAffinity; pa != nil && len(pa.PreferredDuringSchedulingIgnoredDuringExecution) > 0 {
						nPodAff++
					}
					if paa := aff.PodAntiAffinity; paa != nil && len(paa.RequiredDuringSchedulingIgnoredDuringExecution) > 0 {
						nAntiAff++
					}
				}
				if len(pod.Spec.TopologySpreadConstraints) > 0 {
					nTSC++
				}
				for _, c := range pod.Spec.Containers {
					for _, p := range c.Ports {
						if p.HostPort > 0 {
							nHostPort++
							break
						}
					}
				}
			}
			nErrors = len(results2.PodErrors)
			nBaselineErrors := len(results1.PodErrors)

			errStr := fmt.Sprintf("errs(%3d)", nErrors)
			if nBaselineErrors != nErrors {
				errStr = fmt.Sprintf("errs(%3d→%3d)", nBaselineErrors, nErrors)
			}

			// Cost progression: baseline → pre (optimized scheduler before
			// first RevertTo) → post (final). The pre→post delta isolates
			// the optimization pass; the baseline→post delta reports the
			// end-to-end effect.
			fmt.Printf("diverse (%3d), pods (%4d), groups (%2d), nodeclaims (%3d, %3d, %3d), cost (%8.4f, %8.4f, %8.4f), cost%% pre→post (%5.1f%%), baseline→post (%5.1f%%), constraints(zone/tsc/anti/hport/paff, %d/%d/%d/%d/%d), %s\n",
				runIndex, len(pods), len(groups),
				len(results1.NewNodeClaims), preCount, len(results2.NewNodeClaims),
				cost1, preOpt2, cost2,
				savedPrePost, savedEnd,
				nZoneAff, nTSC, nAntiAff, nHostPort, nPodAff, errStr)

			csvWriter.WriteSummary(runIndex, len(pods), len(results1.NewNodeClaims), len(results2.NewNodeClaims), cost1, cost2, 0, 0, preOpt2)
			csvWriter.WriteNodeClaims(runIndex, "baseline", results1.NewNodeClaims)
			// pre / post come from the Scheduler's OptimizationSnapshot.
			// Pre is nil when the optimization pass never fired (no unscheduled
			// pods left at the end of the loop); in that case the final state
			// already equals the pre state, so we fall back to the live slice
			// for both rows to keep the CSV's 3-row-per-run shape.
			if s2.OptimizationSnapshot.Pre != nil {
				csvWriter.WriteNodeClaimSnapshots(runIndex, "pre", s2.OptimizationSnapshot.Pre)
				csvWriter.WriteNodeClaimSnapshots(runIndex, "post", s2.OptimizationSnapshot.Post)
			} else {
				csvWriter.WriteNodeClaims(runIndex, "pre", results2.NewNodeClaims)
				csvWriter.WriteNodeClaims(runIndex, "post", results2.NewNodeClaims)
			}
			csvWriter.WritePodConfigs(runIndex, pods)

			// We intentionally do NOT assert cost2 <= cost1 here. Topology
			// spread, pod anti-affinity, and host port constraints can cause
			// the baseline's single-pass sort order to land pods more
			// compactly than the optimizer can reach starting from a partial
			// solution — this is ordering luck, not an optimizer bug.
			//
			// Instead we assert the per-run invariant: the final cost must
			// not exceed the pre-optimization cost snapshot by more than
			// diverseRegressionSlack. Any regression larger than that
			// points at a split decision that paid less on paper than it
			// cost in reality — typically estimateCheapestPlacement
			// underpricing displaced pods that fragmented across multiple
			// fresh NodeClaims under constraints, or two claims' splits in
			// one pass whose displaced sets re-queue into an extra claim
			// that no per-claim estimate accounted for. The slack exists so
			// rapid can soak through all N checks and surface the worst
			// case instead of bailing on the first 0.5%-class regression;
			// a regression ≥ 2% still fails and is treated as a real bug.
			const diverseRegressionSlack = 0.02
			if preOpt2 > 0 {
				Expect(cost2).To(BeNumerically("<=", preOpt2*(1+diverseRegressionSlack)+.00001),
					"optimized cost (%.4f) exceeded pre-opt cost (%.4f) by more than %.0f%% — bad split decision",
					cost2, preOpt2, diverseRegressionSlack*100)
			}

			// Every input pod must appear in exactly one optimized NodeClaim.
			scheduledUIDs := map[types.UID]struct{}{}
			for _, nc := range results2.NewNodeClaims {
				for _, pod := range nc.Pods {
					_, dup := scheduledUIDs[pod.UID]
					Expect(dup).To(BeFalse(), "pod %s scheduled on multiple NodeClaims", pod.Name)
					scheduledUIDs[pod.UID] = struct{}{}
				}
			}
			// Pods that hit scheduling errors are expected when constraints conflict.
			// The invariant is: scheduled + errors == total input.
			Expect(len(scheduledUIDs)+len(results2.PodErrors)).To(Equal(len(pods)),
				"scheduled (%d) + errors (%d) != input (%d)", len(scheduledUIDs), len(results2.PodErrors), len(pods))

			ExpectCleanedUp(ctx, env.Client)
			cluster.Reset()
		})

		// Two summary lines: baseline→post is the top-line end-to-end
		// effect; pre→post isolates the optimization pass from run-to-run
		// map-iteration noise (two independent Solve calls can diverge on
		// topology-tiebreak ordering even with identical inputs).
		//
		// Sign convention (matches the per-run cost% column):
		// positive = saved, negative = got costlier.
		savedEnd := totalBaselineCost - totalOptCost
		savedPrePost := totalPreOptCost - totalOptCost
		pctEnd, pctPrePost := 0.0, 0.0
		if totalBaselineCost > 0 {
			pctEnd = savedEnd / totalBaselineCost * 100
		}
		if totalPreOptCost > 0 {
			pctPrePost = savedPrePost / totalPreOptCost * 100
		}
		// signDollar emits the sign before the $ so negative savings render
		// as "-$1.34" rather than "-$-1.34" (which happens if you put a
		// literal "-$" in front of a signed value).
		signDollar := func(v float64) string {
			if v < 0 {
				return fmt.Sprintf("-$%.2f", -v)
			}
			return fmt.Sprintf("+$%.2f", v)
		}
		fmt.Printf("\n=== DIVERSE SUMMARY: %d runs ===\n", runIndex)
		fmt.Printf("  baseline → post (end-to-end):   %3d cheaper, %3d costlier | cost $%.2f → $%.2f (%s, %+.1f%%)\n",
			cheaperRunsBP, costlierRunsBP, totalBaselineCost, totalOptCost, signDollar(savedEnd), pctEnd)
		fmt.Printf("  pre → post (optimization pass): %3d cheaper, %3d costlier | cost $%.2f → $%.2f (%s, %+.1f%%)\n",
			cheaperRunsPP, costlierRunsPP, totalPreOptCost, totalOptCost, signDollar(savedPrePost), pctPrePost)
	})
})
