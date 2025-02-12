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

type NodePartitions struct {
	data map[string]string
}

func NewNodePartitions(ctx context.Context) (*NodePartitions, error) {
	slog.Debug("  Starting: Getting Node -> Partition associations")
	slurmBinDir := ctx.Value(types.SlurmBinDirKey)
	if slurmBinDir == nil {
		return nil, fmt.Errorf("failed to find slurm bin dir in context")
	}
	sinfoBin := fmt.Sprintf("%s/sinfo", slurmBinDir)
	cmd := exec.Command(
		"bash",
		"-c",
		fmt.Sprintf("%s -h -o '%%n,%%P'", sinfoBin),
	)
	var outb, errb bytes.Buffer
	cmd.Stdout = &outb
	cmd.Stderr = &errb
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to run command: %v", errb.String())
	}

	stdoutStr := outb.String()
	slog.Debug(fmt.Sprintf("  %s", stdoutStr))
	lines := []string{}
	for _, l := range strings.Split(stdoutStr, "\n") {
		if strings.TrimSpace(l) != "" {
			lines = append(lines, l)
		}
	}

	m := make(map[string]string)

	for _, line := range lines {
		slog.Debug(fmt.Sprintf("    %s", line))
		p := strings.Split(line, ",")
		node := strings.TrimSpace(p[0])
		partition := strings.TrimSpace(p[1])
		slog.Debug(fmt.Sprintf("    Adding node->partition: %s->%s", node, partition))
		m[node] = partition
	}

	slog.Debug("  Finished: Getting Node -> Partition associations")
	return &NodePartitions{
		data: m,
	}, nil
}

func (np *NodePartitions) GetPartition(node string) (string, bool) {
	p, ok := np.data[node]
	return p, ok
}
