package codexbar

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type Runner struct {
	Binary  string
	Timeout time.Duration
}

type CommandResponse struct {
	Payload    json.RawMessage
	Stderr     string
	ExitCode   int
	StartedAt  time.Time
	FinishedAt time.Time
	Duration   time.Duration
	Command    []string
}

func DiscoverBinary(configured string) (string, error) {
	candidates := []string{}
	if configured != "" {
		candidates = append(candidates, configured)
	}
	if path, err := exec.LookPath("codexbar"); err == nil {
		candidates = append(candidates, path)
	}
	if runtime.GOOS == "darwin" {
		candidates = append(candidates,
			"/opt/homebrew/bin/codexbar",
			"/usr/local/bin/codexbar",
			"/Applications/CodexBar.app/Contents/Helpers/CodexBarCLI",
		)
	}
	seen := map[string]struct{}{}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		absolute, err := filepath.Abs(candidate)
		if err != nil {
			continue
		}
		if _, exists := seen[absolute]; exists {
			continue
		}
		seen[absolute] = struct{}{}
		info, err := os.Stat(absolute)
		if err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
			return absolute, nil
		}
	}
	return "", errors.New("CodexBar CLI not found; install it from CodexBar Preferences → Advanced → Install CLI or set codexbar.binary")
}

func (r Runner) RunJSON(ctx context.Context, args ...string) (CommandResponse, error) {
	if r.Timeout <= 0 {
		r.Timeout = 120 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, r.Binary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	started := time.Now().UTC()
	err := cmd.Run()
	finished := time.Now().UTC()
	duration := finished.Sub(started)
	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			exitCode = -1
		} else {
			exitCode = -1
		}
	}
	response := CommandResponse{
		Payload:    append(json.RawMessage(nil), stdout.Bytes()...),
		Stderr:     boundedString(stderr.String(), 16384),
		ExitCode:   exitCode,
		StartedAt:  started,
		FinishedAt: finished,
		Duration:   duration,
		Command:    append([]string{r.Binary}, args...),
	}
	if !json.Valid(response.Payload) {
		if err != nil {
			return response, fmt.Errorf("CodexBar command failed (exit %d): %s", exitCode, strings.TrimSpace(response.Stderr))
		}
		return response, errors.New("CodexBar command returned invalid JSON")
	}
	if err != nil {
		return response, fmt.Errorf("CodexBar command exited %d", exitCode)
	}
	return response, nil
}

func (r Runner) Version(ctx context.Context) (string, error) {
	if r.Timeout <= 0 {
		r.Timeout = 15 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.Binary, "--version")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func boundedString(value string, max int) string {
	value = strings.TrimSpace(value)
	if len(value) > max {
		return value[:max] + "…"
	}
	return value
}
