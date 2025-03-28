package system

import (
	"fmt"
	"log/slog"
	"slices"

	"github.com/lcrownover/process-job-stats-go/internal/types"
)

var preemptWeight = 1

// Service units are calculated at the following rates:
// Open-Use job with GPU usage:				1CpuHour == 3 SU
// Open-Use job on high memory nodes:		1CpuHour == 2 SU
// Open-Use job on standard compute nodes: 	1CpuHour == 1 SU
// Condo job:								0 SU
// Preempt job:								(CpuHours*1 + GpuHours*3)*weight SU
func calculateServiceUnits(category types.JobCategory, partition string, cpuHoursOpenUse, cpuHoursCondo, gpuHoursOpenUse, gpuHoursCondo float64) float64 {
	// open use
	// high memory partition -> return 2x open use cpus + 3x gpus
	// else -> return open use cpus + 3x gpus
	if category == types.JobCategoryOpen {
		if slices.Contains([]string{"memory", "memorylong"}, partition) {
			su := (cpuHoursOpenUse * 2) + (gpuHoursOpenUse * 3)
			slog.Debug(fmt.Sprintf("    service units: high memory partition: %f", su))
			return su
		} else {
			su := cpuHoursOpenUse + (gpuHoursOpenUse * 3)
			slog.Debug(fmt.Sprintf("    service units: standard partition: %f", su))
			return su
		}
	}
	// condo -> return 0
	if category == types.JobCategoryCondo {
		slog.Debug("    service units: condo job: 0")
		return 0
	}
	// otherwise its preempt
	su := ((cpuHoursOpenUse + cpuHoursCondo) * 1) + ((gpuHoursOpenUse + gpuHoursCondo) * 3) * float64(preemptWeight)
	slog.Debug(fmt.Sprintf("    service units: preempt job: %f", su))
	return su
}
