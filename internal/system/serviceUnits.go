package system

import (
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
			return (cpuHoursOpenUse * 2) + (gpuHoursOpenUse * 3)
		} else {
			return cpuHoursOpenUse + (gpuHoursOpenUse * 3)
		}
	}
	// condo -> return 0
	if category == types.JobCategoryCondo {
		return 0
	}
	// otherwise its preempt
	return (((cpuHoursOpenUse + cpuHoursCondo) * 1) + ((gpuHoursOpenUse + gpuHoursCondo) * 3)) * float64(preemptWeight)
}
