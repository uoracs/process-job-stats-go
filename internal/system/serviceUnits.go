package system

func calculateServiceUnits(cpuHoursOpenUse, cpuHoursCondo, gpuHoursOpenUse, gpuHoursCondo float64) float64 {
	// so far it's just 1 * cpu hours and 3* gpu hours
	return ((cpuHoursOpenUse + cpuHoursCondo) * 1) + ((gpuHoursOpenUse + gpuHoursCondo) * 3)
}
