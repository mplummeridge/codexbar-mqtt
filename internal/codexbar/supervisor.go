package codexbar

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type ServeSupervisor struct {
	Runner          Runner
	HTTP            *HTTPClient
	Port            int
	RefreshInterval int
	RequestTimeout  int
	Logger          *slog.Logger

	mu  sync.Mutex
	cmd *exec.Cmd
}

func (s *ServeSupervisor) Run(ctx context.Context) error {
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			s.stop()
			return ctx.Err()
		}
		healthCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		healthy := s.HTTP.Healthy(healthCtx)
		cancel()
		if healthy {
			if !sleepContext(ctx, 5*time.Second) {
				s.stop()
				return ctx.Err()
			}
			continue
		}

		args := []string{
			"serve",
			"--port", strconv.Itoa(s.Port),
			"--refresh-interval", strconv.Itoa(s.RefreshInterval),
			"--request-timeout", strconv.Itoa(s.RequestTimeout),
			"--json-output",
			"--log-level", "warning",
		}
		cmd := exec.CommandContext(ctx, s.Runner.Binary, args...)
		stdout, _ := cmd.StdoutPipe()
		stderr, _ := cmd.StderrPipe()
		if err := cmd.Start(); err != nil {
			s.Logger.Error("failed to start CodexBar serve", "error", err)
			if !sleepContext(ctx, backoff) {
				return ctx.Err()
			}
			backoff = minDuration(backoff*2, 30*time.Second)
			continue
		}
		s.mu.Lock()
		s.cmd = cmd
		s.mu.Unlock()
		s.Logger.Info("started managed CodexBar serve", "pid", cmd.Process.Pid, "port", s.Port)
		go logPipe(s.Logger, "codexbar-serve-stdout", stdout)
		go logPipe(s.Logger, "codexbar-serve-stderr", stderr)
		err := cmd.Wait()
		s.mu.Lock()
		if s.cmd == cmd {
			s.cmd = nil
		}
		s.mu.Unlock()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.Logger.Warn("managed CodexBar serve exited", "error", err)
		if !sleepContext(ctx, backoff) {
			return ctx.Err()
		}
		backoff = minDuration(backoff*2, 30*time.Second)
	}
}

func (s *ServeSupervisor) stop() {
	s.mu.Lock()
	cmd := s.cmd
	s.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
}

func logPipe(logger *slog.Logger, component string, pipe interface{ Read([]byte) (int, error) }) {
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		logger.Debug(component, "line", scanner.Text())
	}
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (s *ServeSupervisor) String() string {
	return fmt.Sprintf("%s serve --port %d", s.Runner.Binary, s.Port)
}
