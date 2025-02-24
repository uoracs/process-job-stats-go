package system

import (
	"bytes"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
)

type userListCache struct {
	data  map[string]string
	mutex *sync.RWMutex
}

func NewUserListCache() *userListCache {
	return &userListCache{
		data:  make(map[string]string),
		mutex: &sync.RWMutex{},
	}
}

func (n *userListCache) Read(key string) (string, bool) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	v, ok := n.data[key]
	return v, ok
}

func (n *userListCache) Write(key, value string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.data[key] = value
}

func getUserFullName(ulc *userListCache, username string) (string, error) {
	slog.Debug("  Starting: Getting Full Name for user")
	fullName, found := ulc.Read(username)
	if found {
		return fullName, nil
	}
	// check for command injection
	if len(strings.Fields(username)) != 1 {
		return "", fmt.Errorf("username invalid, must be single string no spaces: %v", username)
	}

	cmd := exec.Command(
		"bash",
		"-c",
		fmt.Sprintf("getent passwd %s | awk -F ':' '{print $5}'", username),
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
	if len(lines) != 1 {
		return "", fmt.Errorf("should only have one line of stdout: %v", lines)
	}
	fullName = lines[0]

	ulc.Write(username, fullName)

	slog.Debug("  Finished: Getting Full Name for user")
	return fullName, nil

}
