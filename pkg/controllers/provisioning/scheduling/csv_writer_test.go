package scheduling_test

import (
	"encoding/csv"
	"fmt"
	"k8s.io/apimachinery/pkg/api/resource"
	"os"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
)

type CSVWriter struct {
	summaryWriter *csv.Writer
	ncWriter      *csv.Writer
	podWriter     *csv.Writer
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

func NewCSVWriter() (*CSVWriter, error) {
	w := &CSVWriter{}

	var err error
	w.summaryFile, err = os.Create("optimization_summary.csv")
	if err != nil {
		return nil, err
	}
	w.summaryWriter = csv.NewWriter(w.summaryFile)
	w.summaryWriter.Write([]string{"run", "pod_count", "s1_nodeclaims", "s2_nodeclaims", "s1_cost", "s2_cost", "s1_duration_sec", "s2_duration_sec"})

	w.ncFile, err = os.Create("nodeclaim_details.csv")
	if err != nil {
		return nil, err
	}
	w.ncWriter = csv.NewWriter(w.ncFile)
	w.ncWriter.Write([]string{"run", "state", "nodeclaim_name", "nodeclaim_hostname", "instance_type", "cpu_capacity", "memory_capacity", "kube_overhead_cpu", "kube_overhead_memory", "pod_count", "pod_cpu_sum", "pod_memory_sum", "price"})

	w.podFile, err = os.Create("pod_details.csv")
	if err != nil {
		return nil, err
	}
	w.podWriter = csv.NewWriter(w.podFile)
	w.podWriter.Write([]string{"run", "state", "nodeclaim_name", "nodeclaim_hostname", "pod_name", "cpu_request", "memory_request"})

	return w, nil
}

func (w *CSVWriter) WriteSummary(run, podCount, s1NodeClaims, s2NodeClaims int, s1Cost, s2Cost, s1Duration, s2Duration float64) {
	w.summaryWriter.Write([]string{
		fmt.Sprintf("%d", run),
		fmt.Sprintf("%d", podCount),
		fmt.Sprintf("%d", s1NodeClaims),
		fmt.Sprintf("%d", s2NodeClaims),
		fmt.Sprintf("%.4f", s1Cost),
		fmt.Sprintf("%.4f", s2Cost),
		fmt.Sprintf("%.3f", s1Duration),
		fmt.Sprintf("%.3f", s2Duration),
	})
}

func (w *CSVWriter) WriteNodeClaims(run int, state string, nodeClaims []*scheduling.NodeClaim) {
	for idx, nc := range nodeClaims {
		it, _, price := nc.CalculateEfficiency()

		name := fmt.Sprintf("%s-nc-%d", state, idx)

		podCPU := resource.Quantity{}
		podMem := resource.Quantity{}
		for _, pod := range nc.Pods {
			for _, c := range pod.Spec.Containers {
				podCPU.Add(*c.Resources.Requests.Cpu())
				podMem.Add(*c.Resources.Requests.Memory())
			}
		}

		w.ncWriter.Write([]string{
			fmt.Sprintf("%d", run),
			state,
			name,
			nc.Hostname(),
			it.Name,

			fmt.Sprintf("%.2f", cpuToFloat(*it.Capacity.Cpu())),
			fmt.Sprintf("%.2f", memToGiB(*it.Capacity.Memory())),
			fmt.Sprintf("%.2f", cpuToFloat(*it.Overhead.KubeReserved.Cpu())),
			fmt.Sprintf("%.2f", memToGiB(*it.Overhead.KubeReserved.Memory())),
			fmt.Sprintf("%d", len(nc.Pods)),
			fmt.Sprintf("%.2f", cpuToFloat(podCPU)),
			fmt.Sprintf("%.2f", memToGiB(podMem)),
			fmt.Sprintf("%.4f", price),
		})

		for _, pod := range nc.Pods {
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
				nc.Hostname(),
				pod.Name,
				fmt.Sprintf("%.2f", cpuToFloat(cpu)),
				fmt.Sprintf("%.2f", memToGiB(mem)),
			})
		}
	}
	w.ncWriter.Flush()
	w.podWriter.Flush()
}

func (w *CSVWriter) Close() {
	w.summaryWriter.Flush()
	w.ncWriter.Flush()
	w.podWriter.Flush()
	w.summaryFile.Close()
	w.ncFile.Close()
	w.podFile.Close()
}

func calculateCost(nodeClaims []*scheduling.NodeClaim) float64 {
	cost := 0.0
	for _, nc := range nodeClaims {
		instance, _, lowestPrice := nc.CalculateEfficiency()
		if instance != nil {
			cost += lowestPrice
		}
	}
	return cost
}
