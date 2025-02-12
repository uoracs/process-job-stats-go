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

func expandNodeList(ctx context.Context, nodeList string) (string, error) {
	slog.Debug("  Started: Expanding nodelist")
	slurmBinDir := ctx.Value(types.SlurmBinDirKey)
	if strings.TrimSpace(nodeList) == "" {
		return "", nil
	}
	if slurmBinDir == nil {
		return "", fmt.Errorf("failed to find slurm bin dir in context")
	}
	scontrolBin := fmt.Sprintf("%s/scontrol", slurmBinDir)
	cmd := exec.Command(
		"bash",
		"-c",
		fmt.Sprintf("%s show hostnames %s", scontrolBin, nodeList),
	)
	var outb, errb bytes.Buffer
	cmd.Stdout = &outb
	cmd.Stderr = &errb
	err := cmd.Run()
	if err != nil {
		return "", fmt.Errorf("failed to run command: %v", errb.String())
	}

	stdoutStr := outb.String()
	lines := []string{}
	for _, l := range strings.Split(stdoutStr, "\n") {
		if strings.TrimSpace(l) != "" {
			lines = append(lines, l)
		}
	}
	nodes := strings.Join(lines, ",")

	slog.Debug(fmt.Sprintf("    %v", nodes))
	slog.Debug("  Finished: Expanding nodelist")
	return nodes, nil
}
