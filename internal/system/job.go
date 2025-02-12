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

	// Generated Fields
	PI               string
	AccountStorageGB int
	Category         types.JobCategory
	OpenuseWeight    float64
	CondoWeight      float64
	GPUs             int
	CPUHoursOpenUse  float64
	CPUHoursCondo    float64
	GPUHoursOpenUse  float64
	GPUHoursCondo    float64
	WaitTimeHours    float64
	RunTimeHours     float64
	Date             string
}

// 29148459_925|ld_stats_array|akapoor|kernlab|kern|00:07:23|1|8|billing=8,cpu=8,mem=64G,node=1|2025-02-03T23:38:14|2025-02-03T23:53:21|n0335
// job_id|job_name|username|account|partition|elapsed|nodes|cpus|tres|submit_time|start_time|nodelist
func NewJob(ctx context.Context, jobString string) (*Job, error) {
	var err error
	var ok bool
	yesterdayDate := ctx.Value(types.YesterdayKey).(*string)
	nodePartitions := ctx.Value(types.NodePartitionsKey).(*NodePartitions)
	accountPIs := ctx.Value(types.AccountPIsKey).(*AccountPIs)
	accountStorages := ctx.Value(types.AccountStoragesKey).(*AccountStorages)
	if nodePartitions == nil || accountPIs == nil || accountStorages == nil {
		return nil, fmt.Errorf("failed to unpack data from context")
	}
	slog.Debug("  Starting: Parsing job")
	slog.Debug(fmt.Sprintf("    %s", jobString))
	parts := strings.Split(jobString, ",")
	slog.Debug(fmt.Sprintf("    %v", parts))
	if parts[0] == "JobID" {
		return nil, nil
	}
	j := &Job{}
	j.JobID = parts[0]
	j.JobName = parts[1]
	j.Username = parts[2]
	j.Account = parts[3]
	j.Partition = parts[4]
	j.Elapsed = parts[5]
	es, err := parseElapsedToSeconds(j.Elapsed)
	if err != nil {
		return nil, fmt.Errorf("failed to parse elapsed seconds: %v", err)
	}
	if es == 0 {
		return nil, nil
	}
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
	j.NodeList, err = expandNodeList(ctx, parts[12])
	if err != nil {
		return nil, fmt.Errorf("failed to expand nodelist: %v", err)
	}

	j.PI, ok = accountPIs.GetPI(j.Account)
	if !ok {
		return nil, fmt.Errorf("failed to get PI for account %s: %v", j.Account, err)
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
	j.CPUHoursOpenUse, err = calculateComputeHours(j.CPUs, j.OpenuseWeight, j.Elapsed)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate openuse cpu hours: %v", err)
	}
	j.CPUHoursCondo, err = calculateComputeHours(j.CPUs, j.CondoWeight, j.Elapsed)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate condo cpu hours: %v", err)
	}
	j.GPUHoursOpenUse, err = calculateComputeHours(j.GPUs, j.OpenuseWeight, j.Elapsed)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate openuse gpu hours: %v", err)
	}
	j.GPUHoursOpenUse, err = calculateComputeHours(j.GPUs, j.CondoWeight, j.Elapsed)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate condo gpu hours: %v", err)
	}
	j.Date = *yesterdayDate

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
		"PI",
		"AccountStorageGB",
		"Category",
		"OpenuseWeight",
		"CondoWeight",
		"GPUs",
		"CPUHours",
		"GPUHours",
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
		j.PI,
		fmt.Sprintf("%d", j.AccountStorageGB),
		string(j.Category),
		fmt.Sprintf("%f", j.OpenuseWeight),
		fmt.Sprintf("%f", j.CondoWeight),
		fmt.Sprintf("%d", j.GPUs),
		fmt.Sprintf("%f", j.CPUHoursOpenUse),
		fmt.Sprintf("%f", j.CPUHoursCondo),
		fmt.Sprintf("%f", j.GPUHoursOpenUse),
		fmt.Sprintf("%f", j.GPUHoursCondo),
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
	openusePartitions := ctx.Value(types.OpenUsePartitionsKey).([]string)
	if openusePartitions == nil {
		return types.JobCategoryUnknown, fmt.Errorf("failed to get openuse partitions from context")
	}
	if slices.Contains(openusePartitions, partition) {
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
	nl := float64(len(nodes))
	for _, n := range nodes {
		partition, ok := nodePartitions.GetPartition(n)
		if !ok { // if partition not found, scale back the metric
			nl -= 1
			continue
		}
		isOpenuse := false
		if slices.Contains(*openusePartitions, partition) {
			isOpenuse = true
		}
		if category == types.JobCategoryOpen && isOpenuse {
			c += 1
		}
		if category == types.JobCategoryCondo && !isOpenuse {
			c += 1
		}
	}
	if nl <= 0 {
		return 0.0, nil
	}
	return c / nl, nil
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

func calculateComputeHours(number int, weight float64, elapsed string) (float64, error) {
	es, err := parseElapsedToSeconds(elapsed)
	if err != nil {
		return 0.0, fmt.Errorf("failed to calculate run time hours: %v", err)
	}
	return float64(number) * weight * float64(es) / 60 / 60, nil
}
