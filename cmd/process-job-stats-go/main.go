package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/lcrownover/process-job-stats-go/internal/system"
	"github.com/lcrownover/process-job-stats-go/internal/types"
)

var err error
var wg sync.WaitGroup

var OPEN_USE_PARTITIONS = []string{
	"compute",
	"compute_intel",
	"computelong",
	"computelong_intel",
	"gpu",
	"gpulong",
	"interactive",
	"interactivegpu",
	"memory",
	"memorylong",
}

func main() {
	outputFileFlag := flag.String("output", "", "path to output file")
	noHeaderFlag := flag.Bool("noheader", false, "don't show header row")
	dayFlag := flag.String("day", "", "day to process in YYYY-mm-dd")
	debugFlag := flag.Bool("debug", false, "show debug output")
	workersFlag := flag.Int("workers", 16, "number of workers")
	cpuProfileFlag := flag.String("cpuprofile", "", "write cpu profile to this path")

	slurmBinDirFlag := flag.String("slurm-bin-dir", "/gpfs/t2/slurm/apps/current/bin", "directory to find the slurm binaries")
	gpfsBinDirFlag := flag.String("gpfs-bin-dir", "/usr/lpp/mmfs/bin", "directory to find the gpfs binaries")

	skipEmptyDaysFlag := flag.Bool("skip-empty-days", false, "if no jobs, skip writing the output file")

	flag.Parse()

	logLevel := slog.LevelInfo
	if *debugFlag {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	if *cpuProfileFlag != "" {
		f, err := os.Create(*cpuProfileFlag)
		if err != nil {
			log.Fatal("Failed to create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("Failed to start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	processDayDate := time.Now().Add(-24 * time.Hour).Format("2006-01-02")
	if *dayFlag != "" {
		day, err := time.Parse("2006-01-02", *dayFlag)
		if err != nil {
			log.Fatal("Failed to parse provided date:", *dayFlag)
		}
		processDayDate = day.Format("2006-01-02")
	}
	slog.Debug(fmt.Sprintf("Processing jobs for day: %s", processDayDate))

	output := os.Stdout
	if *outputFileFlag != "" {
		output, err = os.OpenFile(*outputFileFlag, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			log.Fatal("Error opening output file:", err)
			return
		}
	}
	defer output.Close()

	writer := csv.NewWriter(output)
	defer writer.Flush()

	if !*noHeaderFlag {
		err := writer.Write(system.JobKeys())
		if err != nil {
			log.Fatal("Failed to write to output:", err)
		}
	}

	slog.Info("Starting job processing")

	ctx := context.Background()

	ctx = context.WithValue(ctx, types.ProcessDayKey, &processDayDate)
	ctx = context.WithValue(ctx, types.SlurmBinDirKey, *slurmBinDirFlag)
	ctx = context.WithValue(ctx, types.GpfsBinDirKey, *gpfsBinDirFlag)
	ctx = context.WithValue(ctx, types.OpenUsePartitionsKey, &OPEN_USE_PARTITIONS)

	rawJobData, err := system.NewRawJobData(ctx)
	if err != nil {
		log.Fatal("Failed to get job data:", err)
	}
	nodePartitions, err := system.NewNodePartitions(ctx)
	if err != nil {
		log.Fatal("Failed to get node partition map:", err)
	}
	accountPIs, err := system.NewAccountPIs(ctx)
	if err != nil {
		log.Fatal("Failed to get account pi map:", err)
	}
	accountStorages, err := system.NewAccountStorages(ctx)
	if err != nil {
		log.Fatal("Failed to get account storage map:", err)
	}
	nlc := system.NewNodeListCache()
	ulc := system.NewUserListCache()

	ctx = context.WithValue(ctx, types.NodePartitionsKey, nodePartitions)
	ctx = context.WithValue(ctx, types.AccountPIsKey, accountPIs)
	ctx = context.WithValue(ctx, types.AccountStoragesKey, accountStorages)
	ctx = context.WithValue(ctx, types.NodeListCacheKey, nlc)
	ctx = context.WithValue(ctx, types.UserListCacheKey, ulc)

	jobCount := len(rawJobData.Jobs)
	workCh := make(chan string, jobCount)
	resultCh := make(chan *system.Job, jobCount)

	slog.Info(fmt.Sprintf("Processing %d jobs", jobCount))

	workerCount := *workersFlag
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(ctx, workCh, resultCh)
		}()
	}

	for _, js := range rawJobData.Jobs {
		workCh <- js
	}
	close(workCh)

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for job := range resultCh {
		if job != nil && !*skipEmptyDaysFlag {
			writer.Write(job.Fields())
		}
	}
}

func worker(ctx context.Context, jobs <-chan string, results chan<- *system.Job) {
	for j := range jobs {
		job, err := system.NewJob(ctx, j)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to parse job: %v", err))
			continue
		}
		if job == nil {
			slog.Info(fmt.Sprintf("Skipping job: %s", j))
			continue
		}
		results <- job
	}
}
