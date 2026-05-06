package scheduling_test

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
)

// nodeClaimNameForHostname returns "<state>-nc-<suffix>" where suffix is the
// trailing numeric token of the hostname (e.g. "hostname-placeholder-0025" ->
// "s2-post-nc-0025"). Hostname is the stable NodeClaim identity across states
// — keying the display name off it lets pre/post rows line up by eye.
// Falls back to the full hostname when it has no trailing "-<number>" tail.
func nodeClaimNameForHostname(state, hostname string) string {
	suffix := hostname
	if i := strings.LastIndex(hostname, "-"); i >= 0 {
		suffix = hostname[i+1:]
	}
	return fmt.Sprintf("%s-nc-%s", state, suffix)
}

type CSVWriter struct {
	summaryWriter *csv.Writer
	ncWriter      *csv.Writer
	podWriter     *csv.Writer
	podConfigFile *os.File
	summaryFile   *os.File
	ncFile        *os.File
	podFile       *os.File
}

func cpuToFloat(q resource.Quantity) float64 {
	return float64(q.MilliValue()) / 1000.0
}

func memToGiB(q resource.Quantity) float64 {
	return float64(q.Value()) / (1024 * 1024 * 1024)
}

func NewCSVWriter(prefix ...string) (*CSVWriter, error) {
	w := &CSVWriter{}

	dir := os.Getenv("TEST_OUTPUT_DIR")
	if dir == "" {
		return w, nil // no-op writer; all methods are nil-safe
	}

	p := ""
	if len(prefix) > 0 && prefix[0] != "" {
		p = prefix[0] + "_"
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	var err error
	w.summaryFile, err = os.Create(dir + "/" + p + "optimization_summary.csv")
	if err != nil {
		return nil, err
	}
	w.summaryWriter = csv.NewWriter(w.summaryFile)
	w.summaryWriter.Write([]string{"run", "pod_count", "s1_nodeclaims", "s2_nodeclaims", "s1_cost", "s2_cost", "s1_duration_sec", "s2_duration_sec", "s2_pre_opt_cost"})

	w.ncFile, err = os.Create(dir + "/" + p + "nodeclaim_details.csv")
	if err != nil {
		return nil, err
	}
	w.ncWriter = csv.NewWriter(w.ncFile)
	// nodeclaim_name is keyed off hostname (the stable NodeClaim identity)
	// so rows line up across s1/s2-pre/s2-post by eye.
	w.ncWriter.Write([]string{"run", "state", "nodeclaim_name", "hostname", "instance_type", "cpu_capacity", "memory_capacity", "kube_overhead_cpu", "kube_overhead_memory", "pod_count", "pod_cpu_sum", "pod_memory_sum", "price"})

	w.podFile, err = os.Create(dir + "/" + p + "pod_details.csv")
	if err != nil {
		return nil, err
	}
	w.podWriter = csv.NewWriter(w.podFile)
	w.podWriter.Write([]string{"run", "state", "nodeclaim_name", "hostname", "pod_name", "cpu_request", "memory_request"})

	w.podConfigFile, err = os.Create(dir + "/" + p + "pod_configs.jsonl")
	if err != nil {
		return nil, err
	}

	return w, nil
}

func (w *CSVWriter) WriteSummary(run, podCount, s1NodeClaims, s2NodeClaims int, s1Cost, s2Cost, s1Duration, s2Duration, s2PreOptCost float64) {
	if w.summaryWriter == nil {
		return
	}
	w.summaryWriter.Write([]string{
		fmt.Sprintf("%d", run),
		fmt.Sprintf("%d", podCount),
		fmt.Sprintf("%d", s1NodeClaims),
		fmt.Sprintf("%d", s2NodeClaims),
		fmt.Sprintf("%.4f", s1Cost),
		fmt.Sprintf("%.4f", s2Cost),
		fmt.Sprintf("%.3f", s1Duration),
		fmt.Sprintf("%.3f", s2Duration),
		fmt.Sprintf("%.4f", s2PreOptCost),
	})
}

// WriteNodeClaims emits one nodeclaim row + one pod row per pod for each
// NodeClaim. Goes through SnapshotNodeClaims so live and post-Solve call
// sites share a single rendering path with WriteNodeClaimSnapshots.
func (w *CSVWriter) WriteNodeClaims(run int, state string, nodeClaims []*scheduling.NodeClaim) {
	if w.ncWriter == nil {
		return
	}
	w.WriteNodeClaimSnapshots(run, state, scheduling.SnapshotNodeClaims(nodeClaims))
}

// WriteNodeClaimSnapshots emits rows for a pre-captured snapshot slice.
// Used to record the pre-optimization state (captured inside tryOptimize
// before any RevertTo) alongside the baseline and final-post states.
func (w *CSVWriter) WriteNodeClaimSnapshots(run int, state string, snaps []scheduling.NodeClaimSnapshot) {
	if w.ncWriter == nil {
		return
	}
	// Copy before sorting so we don't reorder the caller's slice (the live
	// Scheduler state or a captured OptimizationSnapshot). Hostname order is
	// the reader-friendly ordering: the same hostname appears at the same
	// position across s1/s2-pre/s2-post, so split survivors, untouched claims,
	// and new claims are obvious at a glance.
	ordered := make([]scheduling.NodeClaimSnapshot, len(snaps))
	copy(ordered, snaps)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].Hostname < ordered[j].Hostname
	})
	for _, snap := range ordered {
		if snap.CheapestInstance == nil {
			continue
		}
		it := snap.CheapestInstance
		name := nodeClaimNameForHostname(state, snap.Hostname)

		podCPU := resource.Quantity{}
		podMem := resource.Quantity{}
		for _, pod := range snap.Pods {
			for _, c := range pod.Spec.Containers {
				podCPU.Add(*c.Resources.Requests.Cpu())
				podMem.Add(*c.Resources.Requests.Memory())
			}
		}

		w.ncWriter.Write([]string{
			fmt.Sprintf("%d", run),
			state,
			name,
			snap.Hostname,
			it.Name,
			fmt.Sprintf("%.2f", cpuToFloat(*it.Capacity.Cpu())),
			fmt.Sprintf("%.2f", memToGiB(*it.Capacity.Memory())),
			fmt.Sprintf("%.2f", cpuToFloat(*it.Overhead.KubeReserved.Cpu())),
			fmt.Sprintf("%.2f", memToGiB(*it.Overhead.KubeReserved.Memory())),
			fmt.Sprintf("%d", len(snap.Pods)),
			fmt.Sprintf("%.2f", cpuToFloat(podCPU)),
			fmt.Sprintf("%.2f", memToGiB(podMem)),
			fmt.Sprintf("%.4f", snap.Price),
		})

		for _, pod := range snap.Pods {
			cpu := resource.Quantity{}
			mem := resource.Quantity{}
			for _, c := range pod.Spec.Containers {
				cpu.Add(*c.Resources.Requests.Cpu())
				mem.Add(*c.Resources.Requests.Memory())
			}
			w.podWriter.Write([]string{
				fmt.Sprintf("%d", run),
				state,
				name,
				snap.Hostname,
				pod.Name,
				fmt.Sprintf("%.2f", cpuToFloat(cpu)),
				fmt.Sprintf("%.2f", memToGiB(mem)),
			})
		}
	}
	w.ncWriter.Flush()
	w.podWriter.Flush()
}

func (w *CSVWriter) WritePodConfigs(run int, pods []*corev1.Pod) {
	if w.podConfigFile == nil {
		return
	}
	type hostPort struct {
		Port     int32  `json:"port"`
		Protocol string `json:"protocol"`
	}
	type podConfig struct {
		Name                      string                            `json:"name"`
		UID                       string                            `json:"uid,omitempty"`
		CPURequest                string                            `json:"cpuRequest"`
		MemoryRequest             string                            `json:"memoryRequest"`
		NodeRequirements          []corev1.NodeSelectorRequirement  `json:"nodeRequirements,omitempty"`
		TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
		PodAntiAffinity           []corev1.PodAffinityTerm          `json:"podAntiAffinity,omitempty"`
		PodAffinityPreferences    []corev1.WeightedPodAffinityTerm  `json:"podAffinityPreferences,omitempty"`
		HostPorts                 []hostPort                        `json:"hostPorts,omitempty"`
		Labels                    map[string]string                 `json:"labels,omitempty"`
	}
	type runEntry struct {
		Run  int         `json:"run"`
		Pods []podConfig `json:"pods"`
	}

	entry := runEntry{Run: run}
	for _, pod := range pods {
		pc := podConfig{
			Name:   pod.Name,
			UID:    string(pod.UID),
			Labels: pod.Labels,
		}
		// Resource requests from first container
		for _, c := range pod.Spec.Containers {
			if cpu := c.Resources.Requests.Cpu(); cpu != nil {
				pc.CPURequest = cpu.String()
			}
			if mem := c.Resources.Requests.Memory(); mem != nil {
				pc.MemoryRequest = fmt.Sprintf("%.2fGi", memToGiB(*mem))
			}
			for _, p := range c.Ports {
				if p.HostPort > 0 {
					pc.HostPorts = append(pc.HostPorts, hostPort{Port: p.HostPort, Protocol: string(p.Protocol)})
				}
			}
			break // first container only
		}
		if aff := pod.Spec.Affinity; aff != nil {
			if na := aff.NodeAffinity; na != nil && na.RequiredDuringSchedulingIgnoredDuringExecution != nil {
				for _, term := range na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
					pc.NodeRequirements = append(pc.NodeRequirements, term.MatchExpressions...)
				}
			}
			if paa := aff.PodAntiAffinity; paa != nil {
				pc.PodAntiAffinity = paa.RequiredDuringSchedulingIgnoredDuringExecution
			}
			if pa := aff.PodAffinity; pa != nil {
				pc.PodAffinityPreferences = pa.PreferredDuringSchedulingIgnoredDuringExecution
			}
		}
		pc.TopologySpreadConstraints = pod.Spec.TopologySpreadConstraints
		entry.Pods = append(entry.Pods, pc)
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return
	}
	w.podConfigFile.Write(data)
	w.podConfigFile.Write([]byte("\n"))
}

func (w *CSVWriter) Close() {
	if w.summaryWriter == nil {
		return
	}
	w.summaryWriter.Flush()
	w.ncWriter.Flush()
	w.podWriter.Flush()
	w.summaryFile.Close()
	w.ncFile.Close()
	w.podFile.Close()
	w.podConfigFile.Close()
}

