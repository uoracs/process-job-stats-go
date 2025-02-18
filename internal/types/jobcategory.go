package types

type JobCategory string

const (
	JobCategoryOpen    JobCategory = "openuse"
	JobCategoryPreempt JobCategory = "preempt"
	JobCategoryCondo   JobCategory = "condo"
	JobCategoryUnknown JobCategory = "unknown"
)
