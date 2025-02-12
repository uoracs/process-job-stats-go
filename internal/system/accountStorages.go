package system

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"

	"github.com/lcrownover/process-job-stats-go/internal/types"
)

type AccountStorages struct {
	data map[string]string
}

func NewAccountStorages(ctx context.Context) (*AccountStorages, error) {
	slog.Debug("  Starting: Getting Account -> StorageGB")
	gpfsBinDir := ctx.Value(types.GpfsBinDirKey)
	if gpfsBinDir == nil {
		return nil, fmt.Errorf("failed to find gpfs bin dir in context")
	}
	mmrepquotaBin := fmt.Sprintf("%s/mmrepquota", gpfsBinDir)
	cmd := exec.Command(
		"bash",
		"-c",
		fmt.Sprintf("%s -j fs1 --block-size g | awk '/FILESET/ {print $1\",\"$4}'", mmrepquotaBin),
	)
	var outb, errb bytes.Buffer
	cmd.Stdout = &outb
	cmd.Stderr = &errb
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to run command: %v", errb.String())
	}

	stdoutStr := outb.String()
	lines := []string{}
	for _, l := range strings.Split(stdoutStr, "\n") {
		if strings.TrimSpace(l) != "" {
			lines = append(lines, l)
		}
	}

	m := make(map[string]string)

	for _, line := range lines {
		p := strings.Split(line, ",")
		account := strings.TrimSpace(p[0])
		storageGB := strings.TrimSpace(p[1])
		slog.Debug(fmt.Sprintf("    Adding account->storageGB: %s->%s", account, storageGB))
		m[account] = storageGB
	}

	slog.Debug("  Finished: Getting Account -> StorageGB")
	return &AccountStorages{
		data: m,
	}, nil
}

func (as *AccountStorages) GetStorage(account string) (int, error) {
	p, ok := as.data[account]
	if !ok {
		return 0, fmt.Errorf("failed to find account")
	}
	v, err := strconv.Atoi(p)
	return v, err
}
