package system

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
)

type AccountPIs struct {
	data map[string]string
}

func NewAccountPIs(ctx context.Context) (*AccountPIs, error) {
	slog.Debug("  Starting: Getting Account -> PI associations")
	cmd := exec.Command(
		"bash",
		"-c",
		"find /gpfs/projects/* -maxdepth 0",
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
		p := strings.Split(line, "/")
		account := p[len(p)-1]
		pi, err := getDirOwner(line)
		if err != nil {
			return nil, fmt.Errorf("failed to get owner for directory: %v", err)
		}
		m[account] = pi
	}

	slog.Debug("  Finished: Getting Account -> PI associations")
	return &AccountPIs{
		data: m,
	}, nil
}

func (as *AccountPIs) GetPI(account string) (string, bool) {
	p, ok := as.data[account]
	return p, ok
}

func getDirOwner(dirPath string) (string, error) {
	fileInfo, err := os.Stat(dirPath)
	if err != nil {
		return "", err
	}

	stat, ok := fileInfo.Sys().(interface{ Uid() int })
	if !ok {
		return "", fmt.Errorf("could not get UID from file info")
	}
	uid := stat.Uid()

	usr, err := user.LookupId(strconv.Itoa(uid))
	if err != nil {
		return "", err
	}
	return usr.Username, nil
}
