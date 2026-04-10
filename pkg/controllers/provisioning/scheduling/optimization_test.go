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

package scheduling_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/tools/record"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/events"
	pscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("NodeClaim Optimization", func() {
	var nodePool *v1.NodePool
	var instanceTypes []*cloudprovider.InstanceType

	// Create a set of instance types with clear size tiers so split detection
	// has obvious transition points.
	makeInstanceTypes := func() []*cloudprovider.InstanceType {
		return []*cloudprovider.InstanceType{
			fake.NewInstanceType(fake.InstanceTypeOptions{
				Name: "small",
				Resources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("2"),
					corev1.ResourceMemory: resource.MustParse("4Gi"),
					corev1.ResourcePods:   resource.MustParse("32"),
				},
				Offerings: []*cloudprovider.Offering{{
					Available: true,
					Requirements: pscheduling.NewLabelRequirements(map[string]string{
						v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
						corev1.LabelTopologyZone: "test-zone-1",
					}),
					Price: 0.10,
				}},
			}),
			fake.NewInstanceType(fake.InstanceTypeOptions{
				Name: "medium",
				Resources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("4"),
					corev1.ResourceMemory: resource.MustParse("8Gi"),
					corev1.ResourcePods:   resource.MustParse("32"),
				},
				Offerings: []*cloudprovider.Offering{{
					Available: true,
					Requirements: pscheduling.NewLabelRequirements(map[string]string{
						v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
						corev1.LabelTopologyZone: "test-zone-1",
					}),
					Price: 0.20,
				}},
			}),
			fake.NewInstanceType(fake.InstanceTypeOptions{
				Name: "large",
				Resources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("8"),
					corev1.ResourceMemory: resource.MustParse("16Gi"),
					corev1.ResourcePods:   resource.MustParse("32"),
				},
				Offerings: []*cloudprovider.Offering{{
					Available: true,
					Requirements: pscheduling.NewLabelRequirements(map[string]string{
						v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
						corev1.LabelTopologyZone: "test-zone-1",
					}),
					Price: 0.40,
				}},
			}),
			fake.NewInstanceType(fake.InstanceTypeOptions{
				Name: "xlarge",
				Resources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("16"),
					corev1.ResourceMemory: resource.MustParse("32Gi"),
					corev1.ResourcePods:   resource.MustParse("32"),
				},
				Offerings: []*cloudprovider.Offering{{
					Available: true,
					Requirements: pscheduling.NewLabelRequirements(map[string]string{
						v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
						corev1.LabelTopologyZone: "test-zone-1",
					}),
					Price: 0.80,
				}},
			}),
		}
	}

	BeforeEach(func() {
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Template: v1.NodeClaimTemplate{
					Spec: v1.NodeClaimTemplateSpec{
						Requirements: []v1.NodeSelectorRequirementWithMinValues{
							{
								Key:      v1.CapacityTypeLabelKey,
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{v1.CapacityTypeOnDemand},
							},
							{
								Key:      corev1.LabelArchStable,
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{v1.ArchitectureAmd64},
							},
							{
								Key:      corev1.LabelOSStable,
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{string(corev1.Linux)},
							},
						},
					},
				},
			},
		})
		instanceTypes = makeInstanceTypes()
		cloudProvider.InstanceTypes = instanceTypes
	})

	solve := func(pods []*corev1.Pod, opts ...scheduling.Options) scheduling.Results {
		ExpectApplied(ctx, env.Client, nodePool)
		topology, err := scheduling.NewTopology(ctx, env.Client, cluster, nil, []*v1.NodePool{nodePool},
			map[string][]*cloudprovider.InstanceType{nodePool.Name: instanceTypes}, pods)
		Expect(err).ToNot(HaveOccurred())
		s := scheduling.NewScheduler(ctx, env.Client, []*v1.NodePool{nodePool}, cluster, nil, topology,
			map[string][]*cloudprovider.InstanceType{nodePool.Name: instanceTypes},
			nil, events.NewRecorder(&record.FakeRecorder{}), fakeClock, nil, opts...)
		results, err := s.Solve(ctx, pods)
		Expect(err).ToNot(HaveOccurred())
		return results
	}

	totalCost := func(results scheduling.Results) float64 {
		cost := 0.0
		for _, nc := range results.NewNodeClaims {
			cheapest := nc.InstanceTypeOptions[0].Offerings.Available().WorstLaunchPrice(nc.Requirements)
			for _, it := range nc.InstanceTypeOptions[1:] {
				if p := it.Offerings.Available().WorstLaunchPrice(nc.Requirements); p < cheapest {
					cheapest = p
				}
			}
			cost += cheapest
		}
		return cost
	}

	Context("Split Detection", func() {
		It("should split when a small pod forces a jump to a much larger instance", func() {
			// 3 pods that fit on a medium (1 cpu each = 3 cpu total), then 1 pod
			// that needs 2 cpu, pushing to large. The 2-cpu pod should split off.
			pods := []*corev1.Pod{
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				}}),
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				}}),
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				}}),
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("2Gi")},
				}}),
			}

			withOpt := solve(pods, scheduling.EnableNodeClaimOptimization)
			// Optimization should produce 2 NodeClaims instead of 1 oversized one
			Expect(len(withOpt.NewNodeClaims)).To(BeNumerically(">=", 2))
		})

		It("should not split when all pods pack efficiently", func() {
			// 2 pods that together fill a small instance well
			pods := []*corev1.Pod{
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")},
				}}),
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")},
				}}),
			}

			withOpt := solve(pods, scheduling.EnableNodeClaimOptimization)
			Expect(withOpt.NewNodeClaims).To(HaveLen(1))
		})

		It("should not split when there are fewer than 2 scheduling options", func() {
			// Single pod — only one scheduling option, nothing to split
			pods := []*corev1.Pod{
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				}}),
			}

			withOpt := solve(pods, scheduling.EnableNodeClaimOptimization)
			Expect(withOpt.NewNodeClaims).To(HaveLen(1))
		})
	})

	Context("Cost Comparison", func() {
		It("should produce equal or lower total cost with optimization enabled", func() {
			pods := []*corev1.Pod{
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				}}),
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				}}),
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				}}),
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("2Gi")},
				}}),
			}

			without := solve(pods)
			withOpt := solve(pods, scheduling.EnableNodeClaimOptimization)

			Expect(totalCost(withOpt)).To(BeNumerically("<=", totalCost(without)))
		})
	})

	Context("Feature Toggle", func() {
		It("should not optimize when OptimizeNodeClaim is disabled", func() {
			pods := []*corev1.Pod{
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				}}),
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				}}),
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				}}),
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("2Gi")},
				}}),
			}

			without := solve(pods)
			// Without optimization, all pods should land on a single NodeClaim
			Expect(without.NewNodeClaims).To(HaveLen(1))
		})

		It("should schedule all pods even with optimization enabled", func() {
			pods := []*corev1.Pod{
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				}}),
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")},
				}}),
				test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("2Gi")},
				}}),
			}

			results := solve(pods, scheduling.EnableNodeClaimOptimization)
			// All pods should be scheduled — none in PodErrors
			Expect(results.PodErrors).To(BeEmpty())
			totalPods := 0
			for _, nc := range results.NewNodeClaims {
				totalPods += len(nc.Pods)
			}
			Expect(totalPods).To(Equal(len(pods)))
		})
	})
})
