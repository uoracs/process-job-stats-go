package system

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"

	"github.com/lcrownover/process-job-stats-go/internal/types"
)

type RawJobData struct {
	Jobs []string
}

func NewRawJobData(ctx context.Context) (*RawJobData, error) {
	slog.Debug("  Starting: Getting jobs from sacct")
	slurmBinDir := ctx.Value(types.SlurmBinDirKey)
	if slurmBinDir == nil {
		return nil, fmt.Errorf("failed to find slurm bin dir in context")
	}
	processDayDate := ctx.Value(types.ProcessDayKey).(*string)
	if processDayDate == nil {
		return nil, fmt.Errorf("failed to find process day in context")
	}
	startTime := fmt.Sprintf("%sT00:00:00", *processDayDate)
	endTime := fmt.Sprintf("%sT23:59:59", *processDayDate)
	slog.Debug(fmt.Sprintf("    date range: %s -> %s", startTime, endTime))

	sacctBin := fmt.Sprintf("%s/sacct", slurmBinDir)
	cmd := exec.Command(
		"bash",
		"-c",
		fmt.Sprintf("%s -X -P -n --starttime='%s' --endtime='%s' --state=F,CD,CA --format=JobID,JobName,User,Account,Partition,Elapsed,NNodes,NCPUS,AllocTRES,Submit,Start,End,Nodelist,State", sacctBin, startTime, endTime),
	)
	var outb, errb bytes.Buffer
	cmd.Stdout = &outb
	cmd.Stderr = &errb
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to run command: %v: %v", err, errb.String())
	}

	stdoutStr := outb.String()
	lines := []string{}
	for _, l := range strings.Split(stdoutStr, "\n") {
		if strings.TrimSpace(l) != "" {
			lines = append(lines, l)
		}
	}

	slog.Debug("  Finished: Getting jobs from sacct")
	return &RawJobData{
		Jobs: lines,
	}, nil
}
