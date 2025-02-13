package system

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"sync"

	"github.com/lcrownover/process-job-stats-go/internal/types"
)

type nodeListCache struct {
	data  map[string]string
	mutex *sync.RWMutex
}

func NewNodeListCache() *nodeListCache {
	return &nodeListCache{
		data:  make(map[string]string),
		mutex: &sync.RWMutex{},
	}
}

func (n *nodeListCache) Read(key string) (string, bool) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	v, ok := n.data[key]
	return v, ok
}

func (n *nodeListCache) Write(key, value string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.data[key] = value
}

func expandNodeList(ctx context.Context, nlc *nodeListCache, nodeList string) (string, error) {
	slog.Debug("  Started: Expanding nodelist")
	// try the cache first
	nodes, ok := nlc.Read(nodeList)
	if ok {
		slog.Debug(fmt.Sprintf("    Found nodelist in cache: %s->%s", nodeList, nodes))
		return nodes, nil
	}
	slurmBinDir := ctx.Value(types.SlurmBinDirKey)
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
	nodes = strings.Join(lines, ",")

	slog.Debug(fmt.Sprintf("    Writing nodelist to cache: %s->%s", nodeList, nodes))
	nlc.Write(nodeList, nodes)

	slog.Debug(fmt.Sprintf("    %v", nodes))
	slog.Debug("  Finished: Expanding nodelist")
	return nodes, nil
}
