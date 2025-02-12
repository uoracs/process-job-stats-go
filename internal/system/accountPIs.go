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
		"ls -l /gpfs/projects/ | awk '{print $3\", \"$9}'",
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
		slog.Debug(fmt.Sprintf("    Adding account->pi: %s->%s", p[1], p[0]))
		m[strings.TrimSpace(p[1])] = strings.TrimSpace(p[0])
	}

	slog.Debug(fmt.Sprintf("    %v", m))
	slog.Debug("  Finished: Getting Account -> PI associations")
	slog.Debug(fmt.Sprintf("      %s", m["jamming"]))
	return &AccountPIs{
		data: m,
	}, nil
}

func (as *AccountPIs) GetPI(account string) (string, bool) {
	slog.Debug(fmt.Sprintf("  Getting Account PI for: %s", account))
	p, ok := as.data[account]
	slog.Debug(fmt.Sprintf("    PI:%s, ok:%s", p, ok))
	return p, ok
}

func getDirOwner(dirPath string) (string, error) {
	slog.Debug("    Getting Directory Owner")
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
