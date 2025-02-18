package system

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lcrownover/process-job-stats-go/internal/types"
)

type Job struct {
	// Information from SLURM
	JobID      string
	JobName    string
	Username   string
	Account    string
	Partition  string
	Elapsed    string
	NodeCount  int
	CPUs       int
	TRES       string
	SubmitTime string
	StartTime  string
	EndTime    string
	NodeList   string
	State      types.JobState

	// Generated Fields
	PI               string
	AccountStorageGB int
	Category         types.JobCategory
	OpenuseWeight    float64
	CondoWeight      float64
	GPUs             int
	CPUHoursOpenUse  float64
	CPUHoursCondo    float64
	CPUHoursTotal    float64
	GPUHoursOpenUse  float64
	GPUHoursCondo    float64
	GPUHoursTotal    float64
	WaitTimeHours    float64
	RunTimeHours     float64
	Date             string
}

// 29148459_925|ld_stats_array|akapoor|kernlab|kern|00:07:23|1|8|billing=8,cpu=8,mem=64G,node=1|2025-02-03T23:38:14|2025-02-03T23:53:21|n0335
// job_id|job_name|username|account|partition|elapsed|nodes|cpus|tres|submit_time|start_time|nodelist
func NewJob(ctx context.Context, jobString string) (*Job, error) {
	var err error
	var ok bool
	processDayDate := ctx.Value(types.ProcessDayKey).(*string)
	nodePartitions := ctx.Value(types.NodePartitionsKey).(*NodePartitions)
	accountPIs := ctx.Value(types.AccountPIsKey).(*AccountPIs)
	accountStorages := ctx.Value(types.AccountStoragesKey).(*AccountStorages)
	nlc := ctx.Value(types.NodeListCacheKey).(*nodeListCache)
	if nodePartitions == nil || accountPIs == nil || accountStorages == nil {
		return nil, fmt.Errorf("failed to unpack data from context")
	}
	slog.Debug("  Starting: Parsing job")
	slog.Debug(fmt.Sprintf("    %s", jobString))
	parts := strings.Split(jobString, "|")
	j := &Job{}
	j.JobID = parts[0]
	j.JobName = parts[1]
	j.Username = parts[2]
	j.Account = parts[3]
	j.Partition = parts[4]
	j.Elapsed = parts[5]
	j.NodeCount, err = strconv.Atoi(parts[6])
	if err != nil {
		return nil, fmt.Errorf("failed to parse nodes: %v", err)
	}
	j.CPUs, err = strconv.Atoi(parts[7])
	if err != nil {
		return nil, fmt.Errorf("failed to parse nodes: %v", err)
	}
	j.TRES = parts[8]
	j.SubmitTime = parts[9]
	j.StartTime = parts[10]
	j.EndTime = parts[11]

	j.NodeList, err = expandNodeList(ctx, nlc, parts[12])
	if err != nil {
		return nil, fmt.Errorf("failed to expand nodelist: %v", err)
	}
	// jobs that were cancelled before running
	if j.NodeList == "" {
		return nil, nil
	}

	j.State, err = getJobState(parts[13])
	if err != nil {
		return nil, fmt.Errorf("failed to parse job state: %v", err)
	}

	j.PI, ok = accountPIs.GetPI(j.Account)
	if !ok {
		return nil, fmt.Errorf("failed to get PI for account: %s", j.Account)
	}

	j.AccountStorageGB, err = accountStorages.GetStorage(j.Account)
	if !ok {
		return nil, fmt.Errorf("failed to get account storage for account %s: %v", j.Account, err)
	}

	j.Category, err = categorizeJob(ctx, j.Partition)
	if err != nil {
		return nil, fmt.Errorf("failed to categorize job: %v", err)
	}

	j.OpenuseWeight, err = calculateWeight(ctx, types.JobCategoryOpen, j.NodeList)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate openuse weight: %v", err)
	}

	j.CondoWeight, err = calculateWeight(ctx, types.JobCategoryCondo, j.NodeList)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate condo weight: %v", err)
	}

	j.GPUs, err = calculateGPUsFromTRES(j.TRES)
	if err != nil {
		return nil, fmt.Errorf("failed to parse gpus from tres: %v", err)
	}

	j.WaitTimeHours, err = calculateWaitTimeHours(j.SubmitTime, j.StartTime)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate wait time hours: %v", err)
	}

	j.RunTimeHours, err = calculateRunTimeHours(j.Elapsed)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate run time hours: %v", err)
	}

	slog.Debug("  Calculating OpenUse CPU Hours")
	j.CPUHoursOpenUse, err = calculateComputeHours(j.CPUs, j.OpenuseWeight, j.Elapsed)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate openuse cpu hours: %v", err)
	}

	slog.Debug("  Calculating Condo CPU Hours")
	j.CPUHoursCondo, err = calculateComputeHours(j.CPUs, j.CondoWeight, j.Elapsed)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate condo cpu hours: %v", err)
	}
	j.CPUHoursTotal = j.CPUHoursOpenUse + j.CPUHoursCondo

	slog.Debug("  Calculating OpenUse GPU Hours")
	j.GPUHoursOpenUse, err = calculateComputeHours(j.GPUs, j.OpenuseWeight, j.Elapsed)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate openuse gpu hours: %v", err)
	}

	slog.Debug("  Calculating Condo GPU Hours")
	j.GPUHoursCondo, err = calculateComputeHours(j.GPUs, j.CondoWeight, j.Elapsed)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate condo gpu hours: %v", err)
	}
	j.GPUHoursTotal = j.GPUHoursOpenUse + j.GPUHoursCondo

	j.Date = *processDayDate

	slog.Debug("  Finished: Parsing job")
	return j, nil
}

func JobKeys() []string {
	return []string{
		"JobID",
		"JobName",
		"Username",
		"Account",
		"Partition",
		"Elapsed",
		"NodeCount",
		"CPUs",
		"TRES",
		"SubmitTime",
		"StartTime",
		"EndTime",
		"NodeList",
		"State",
		"PI",
		"AccountStorageGB",
		"Category",
		"OpenuseWeight",
		"CondoWeight",
		"GPUs",
		"CPUHoursOpenUse",
		"CPUHoursCondo",
		"CPUHoursTotal",
		"GPUHoursOpenUse",
		"GPUHoursCondo",
		"GPUHoursTotal",
		"WaitTimeHours",
		"RunTimeHours",
		"Date",
	}
}

func (j *Job) Fields() []string {
	return []string{
		j.JobID,
		j.JobName,
		j.Username,
		j.Account,
		j.Partition,
		j.Elapsed,
		fmt.Sprintf("%d", j.NodeCount),
		fmt.Sprintf("%d", j.CPUs),
		j.TRES,
		j.SubmitTime,
		j.StartTime,
		j.EndTime,
		j.NodeList,
		string(j.State),
		j.PI,
		fmt.Sprintf("%d", j.AccountStorageGB),
		string(j.Category),
		fmt.Sprintf("%f", j.OpenuseWeight),
		fmt.Sprintf("%f", j.CondoWeight),
		fmt.Sprintf("%d", j.GPUs),
		fmt.Sprintf("%f", j.CPUHoursOpenUse),
		fmt.Sprintf("%f", j.CPUHoursCondo),
		fmt.Sprintf("%f", j.CPUHoursTotal),
		fmt.Sprintf("%f", j.GPUHoursOpenUse),
		fmt.Sprintf("%f", j.GPUHoursCondo),
		fmt.Sprintf("%f", j.GPUHoursTotal),
		fmt.Sprintf("%f", j.WaitTimeHours),
		fmt.Sprintf("%f", j.RunTimeHours),
		j.Date,
	}
}

// elapsed could be in two forms:
//
//	1-00:00:00 -> days-hours:minutes:seconds
//	00:00:00   -> hours:minutes:seconds
func parseElapsedToSeconds(elapsed string) (int, error) {
	e_days := 0
	var err error
	if strings.Contains(elapsed, "-") {
		p := strings.Split(elapsed, "-")
		e_days, err = strconv.Atoi(p[0])
		if err != nil {
			return 0, fmt.Errorf("failed to parse elapsed when day section found: %v", err)
		}
		elapsed = p[1]
	}
	hms := strings.Split(elapsed, ":")
	h, err := strconv.Atoi(hms[0])
	if err != nil {
		return 0, fmt.Errorf("failed to parse elapsed hours: %v", err)
	}
	m, err := strconv.Atoi(hms[1])
	if err != nil {
		return 0, fmt.Errorf("failed to parse elapsed minutes: %v", err)
	}
	s, err := strconv.Atoi(hms[2])
	if err != nil {
		return 0, fmt.Errorf("failed to parse elapsed seconds: %v", err)
	}
	return e_days*86400 + h*60*60 + m*60 + s, nil

}

func categorizeJob(ctx context.Context, partition string) (types.JobCategory, error) {
	openusePartitions := ctx.Value(types.OpenUsePartitionsKey).(*[]string)
	if openusePartitions == nil {
		return types.JobCategoryUnknown, fmt.Errorf("failed to get openuse partitions from context")
	}
	if slices.Contains(*openusePartitions, partition) {
		return types.JobCategoryOpen, nil
	}
	if partition == "preempt" {
		return types.JobCategoryDonated, nil
	}
	return types.JobCategoryCondo, nil
}

// This represents the weight of the nodes that ran in open-use nodes.
// Used to multiply by things like CPU Hours, etc, to properly weight jobs.
func calculateWeight(ctx context.Context, category types.JobCategory, nodeList string) (float64, error) {
	slog.Debug(fmt.Sprintf("    Started Calculating Weight for %s: %s", string(category), nodeList))
	if !slices.Contains([]types.JobCategory{types.JobCategoryOpen, types.JobCategoryCondo}, category) {
		return 0.0, fmt.Errorf("category must be JobCategoryOpen or JobCategoryCondo")
	}
	openusePartitions := ctx.Value(types.OpenUsePartitionsKey).(*[]string)
	if openusePartitions == nil {
		return 0.0, fmt.Errorf("failed to unpack openuse partitions from context")
	}
	nodePartitions := ctx.Value(types.NodePartitionsKey).(*NodePartitions)
	if nodePartitions == nil {
		return 0.0, fmt.Errorf("failed to unpack node partitions from context")
	}
	c := float64(0)
	nodes := strings.Split(nodeList, ",")
	nl := len(nodes)
	slog.Debug(fmt.Sprintf("      nodeList length: %d", nl))
	for _, n := range nodes {
		slog.Debug(fmt.Sprintf("      node: %s", n))
		partition, ok := nodePartitions.GetPartition(n)
		if !ok { // if partition not found, scale back the metric
			slog.Debug("        partition not found, reducing nodeList length by 1")
			nl -= 1
			continue
		}
		slog.Debug(fmt.Sprintf("        partition: %s", partition))
		isOpenuse := false
		if slices.Contains(*openusePartitions, partition) {
			isOpenuse = true
		}
		slog.Debug(fmt.Sprintf("        open use?: %v", isOpenuse))
		if category == types.JobCategoryOpen && isOpenuse {
			slog.Debug("        increasing count for open use by 1")
			c += 1
		}
		if category == types.JobCategoryCondo && !isOpenuse {
			slog.Debug("        increasing count for condo by 1")
			c += 1
		}
	}
	if nl <= 0 {
		slog.Debug("      length is 0 or lower due to missing partitions, just counting as 0.0 weight")
		return 0.0, nil
	}
	wt := c / float64(nl)
	slog.Debug(fmt.Sprintf("      resulting weight: %f", wt))
	slog.Debug(fmt.Sprintf("    Finished Calculating Weight for %s: %s", string(category), nodeList))
	return wt, nil
}

func calculateGPUsFromTRES(tres string) (int, error) {
	if !strings.Contains(tres, "gres/gpu=") {
		return 0, nil
	}
	for _, part := range strings.Split(tres, ",") {
		if strings.Contains(part, "gres/gpu=") {
			gpus, err := strconv.Atoi(strings.Split(part, "=")[1])
			if err != nil {
				return 0, fmt.Errorf("failed to parse gpus from tres: %v", err)
			}
			return gpus, nil
		}
	}
	return 0, nil
}

func calculateWaitTimeHours(iso1 string, iso2 string) (float64, error) {
	d1, err := time.Parse("2006-01-02T15:04:05", iso1)
	if err != nil {
		return 0.0, err
	}
	d2, err := time.Parse("2006-01-02T15:04:05", iso2)
	if err != nil {
		return 0.0, err
	}
	return float64(d2.Sub(d1).Hours()), nil
}

func calculateRunTimeHours(elapsed string) (float64, error) {
	es, err := parseElapsedToSeconds(elapsed)
	if err != nil {
		return 0.0, fmt.Errorf("failed to calculate run time hours: %v", err)
	}
	return float64(es) / 60 / 60, nil
}

func calculateComputeHours(processors int, weight float64, elapsed string) (float64, error) {
	slog.Debug("  Starting Calculation of Compute Hours")
	slog.Debug(fmt.Sprintf("    processors: %d", processors))
	slog.Debug(fmt.Sprintf("    weight: %f", weight))
	es, err := parseElapsedToSeconds(elapsed)
	slog.Debug(fmt.Sprintf("  elapsed seconds: %d", es))
	if err != nil {
		return 0.0, fmt.Errorf("failed to calculate run time hours: %v", err)
	}
	hours := float64(processors) * weight * float64(es) / 60 / 60
	slog.Debug(fmt.Sprintf("  elapsed hours: %f", hours))
	slog.Debug("  Finished Calculation of Compute Hours")
	return hours, nil
}

func getJobState(stateString string) (types.JobState, error) {
	if stateString == "COMPLETED" {
		return types.JobStateCompleted, nil
	}
	if stateString == "FAILED" {
		return types.JobStateFailed, nil
	}
	if strings.Contains(stateString, "CANCELLED") {
		return types.JobStateCancelled, nil
	}
	return types.JobStateUnknown, fmt.Errorf("failed to find job state from string: %s", stateString)
}
