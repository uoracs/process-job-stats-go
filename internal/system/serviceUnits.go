package system

import "github.com/lcrownover/process-job-stats-go/internal/types"

var preemptWeight = 1

func calculateServiceUnits(category types.JobCategory, cpuHoursOpenUse, cpuHoursCondo, gpuHoursOpenUse, gpuHoursCondo float64) float64 {
	// open use -> just return open use cpus + 3x gpus
	if category == types.JobCategoryOpen {
		return cpuHoursOpenUse + (gpuHoursOpenUse * 3)
	}
	// condo -> return 0
	if category == types.JobCategoryCondo {
		return 0
	}
	// otherwise its preempt
	return (((cpuHoursOpenUse + cpuHoursCondo) * 1) + ((gpuHoursOpenUse + gpuHoursCondo) * 3)) * float64(preemptWeight)
}
