package types

type JobCategory string

const (
	JobCategoryOpen    JobCategory = "openuse"
	JobCategoryDonated JobCategory = "donated"
	JobCategoryCondo   JobCategory = "condo"
	JobCategoryUnknown JobCategory = "unknown"
)
