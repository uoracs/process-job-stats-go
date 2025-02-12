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
	yesterday := ctx.Value(types.YesterdayKey)
	if yesterday == nil {
		return nil, fmt.Errorf("failed to find yesterday in context")
	}
	sacctBin := fmt.Sprintf("%s/sacct", slurmBinDir)
	cmd := exec.Command(
		"bash",
		"-c",
		fmt.Sprintf("%s -X -P -n --starttime='%sT00:00:00' --endtime='%sT23:59:59' --state=F,CD --format=JobID,JobName,User,Account,Partition,Elapsed,NNodes,NCPUS,AllocTRES,Submit,Start,End,Nodelist", sacctBin, yesterday, yesterday)
	)
	var outb, errb bytes.Buffer
	cmd.Stdout = &outb
	cmd.Stderr = &errb
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to run command: %v: %v", err, errb.String())
	}

	stdoutStr := outb.String()
	lines := strings.Split(stdoutStr, "\n")

	slog.Debug("  Finished: Getting jobs from sacct")
	return &RawJobData{
		Jobs: lines,
	}, nil
}
