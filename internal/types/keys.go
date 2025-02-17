package types

type Key int

const (
	ProcessDayKey Key = iota
	SlurmBinDirKey
	GpfsBinDirKey
	OpenUsePartitionsKey
	AccountPIsKey
	AccountStoragesKey
	NodePartitionsKey
	NodeListCacheKey
)

type JobState string

const (
	JobStateCompleted JobState = "completed"
	JobStateCancelled JobState = "cancelled"
	JobStateFailed JobState = "failed"
	JobStateUnknown JobState = "unknown"
)
