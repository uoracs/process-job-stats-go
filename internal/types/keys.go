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
)
