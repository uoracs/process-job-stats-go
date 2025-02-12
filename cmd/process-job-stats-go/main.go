package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/lcrownover/process-job-stats-go/internal/system"
	"github.com/lcrownover/process-job-stats-go/internal/types"
)

var err error

const SLURM_BIN_DIR = "/gpfs/t2/slurm/apps/current/bin"
const GPFS_BIN_DIR = "/usr/lpp/mmfs/bin"

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
	outputFile := flag.String("output", "", "path to output file")
	noHeader := flag.Bool("noheader", false, "don't show header row")
	debug := flag.Bool("debug", false, "show debug output")
	flag.Parse()

	logLevel := slog.LevelInfo
	if *debug {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	output := os.Stdout
	if *outputFile != "" {
		output, err = os.OpenFile(*outputFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			log.Fatal("Error opening output file:", err)
			return
		}
	}
	defer output.Close()

	writer := csv.NewWriter(output)
	defer writer.Flush()

	if !*noHeader {
		err := writer.Write(system.JobKeys())
		if err != nil {
			log.Fatal("Failed to write to output:", err)
		}
	}

	slog.Debug("Starting job processing")

	ctx := context.Background()
	yesterdayDate := time.Now().Add(-24 * time.Hour).Format("2006-01-02")
	ctx = context.WithValue(ctx, types.YesterdayKey, &yesterdayDate)
	ctx = context.WithValue(ctx, types.SlurmBinDirKey, SLURM_BIN_DIR)
	ctx = context.WithValue(ctx, types.GpfsBinDirKey, GPFS_BIN_DIR)
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
	ctx = context.WithValue(ctx, types.NodePartitionsKey, nodePartitions)
	ctx = context.WithValue(ctx, types.AccountPIsKey, accountPIs)
	ctx = context.WithValue(ctx, types.AccountStoragesKey, accountStorages)

	jobCount := len(rawJobData.Jobs)
	workCh := make(chan string, jobCount)
	resultCh := make(chan *system.Job, jobCount)

	workerCount := 1000
	for range workerCount {
		go worker(ctx, workCh, resultCh)
	}

	for _, js := range rawJobData.Jobs {
		workCh <- js
	}
	close(workCh)

	for range jobCount {
		job := <-resultCh
		if job != nil {
			writer.Write(job.Fields())
		}
	}
}

func worker(ctx context.Context, jobs <-chan string, results chan<- *system.Job) {
	for j := range jobs {
		job, err := system.NewJob(ctx, j)
		if err != nil {
			log.Fatal("Failed to parse job:", err)
		}
		if job == nil {
			slog.Info(fmt.Sprintf("Skipping job: %s", j))
			continue
		}
		results <- job
	}
}
