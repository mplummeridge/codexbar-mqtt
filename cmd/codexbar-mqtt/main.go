package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mplummeridge/codexbar-mqtt/internal/app"
	"github.com/mplummeridge/codexbar-mqtt/internal/config"
	"github.com/mplummeridge/codexbar-mqtt/internal/envelope"
	"github.com/mplummeridge/codexbar-mqtt/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		printUsage()
		return flag.ErrHelp
	}
	switch os.Args[1] {
	case "init":
		return runInit(os.Args[2:])
	case "run":
		return runAgent(os.Args[2:], false)
	case "once":
		return runAgent(os.Args[2:], true)
	case "doctor":
		return runDoctor(os.Args[2:])
	case "version", "--version", "-v":
		fmt.Printf("codexbar-mqtt %s (%s, %s)\n", version.Version, version.Commit, version.Date)
		return nil
	case "schema":
		return runSchema()
	case "help", "--help", "-h":
		printUsage()
		return nil
	default:
		printUsage()
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}

func runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	path := fs.String("config", config.DefaultConfigPath, "configuration path")
	force := fs.Bool("force", false, "overwrite an existing configuration")
	if err := fs.Parse(args); err != nil {
		return err
	}
	written, err := config.WriteExample(*path, *force)
	if err != nil {
		return err
	}
	fmt.Println(written)
	return nil
}

func runAgent(args []string, once bool) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	path := fs.String("config", config.DefaultConfigPath, "configuration path")
	level := fs.String("log-level", "info", "debug|info|warn|error")
	format := fs.String("log-format", "text", "text|json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	logger, err := newLogger(*level, *format)
	if err != nil {
		return err
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	agent, err := app.New(cfg, logger)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if once {
		return agent.RunOnce(ctx)
	}
	return agent.Run(ctx)
}

func runDoctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	path := fs.String("config", config.DefaultConfigPath, "configuration path")
	level := fs.String("log-level", "warn", "debug|info|warn|error")
	if err := fs.Parse(args); err != nil {
		return err
	}
	logger, err := newLogger(*level, "text")
	if err != nil {
		return err
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	agent, err := app.New(cfg, logger)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	report := agent.Doctor(ctx)
	data, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(data))
	if !report.OK {
		return errors.New("doctor found one or more failures")
	}
	return nil
}

func runSchema() error {
	payload := map[string]any{
		"observation_schema": envelope.Schema,
		"discovery_schema":   "dev.mmv3.codexbar-mqtt.discovery.v1",
		"contract_major":     1,
		"topic_layout": map[string]string{
			"discovery":    "codexbar/discovery/v1/<fleet-id>/<machine-id>",
			"availability": "<prefix>/nodes/<machine-id>/availability",
			"meta":         "<prefix>/nodes/<machine-id>/meta",
			"heartbeat":    "<prefix>/nodes/<machine-id>/heartbeat",
			"events":       "<prefix>/events/<machine-id>/<kind>",
			"snapshots":    "<prefix>/nodes/<machine-id>/snapshots/<kind>/<scope>",
		},
		"kinds": []string{
			"serve/health", "serve/usage", "serve/cost", "cli/usage-status",
			"cli/active-account-probe", "cli/account-catalogue", "cli/cost-horizon",
			"cli/config-validation", "agent/error",
		},
	}
	data, _ := json.MarshalIndent(payload, "", "  ")
	fmt.Println(string(data))
	return nil
}

func newLogger(level, format string) (*slog.Logger, error) {
	var parsed slog.Level
	switch strings.ToLower(level) {
	case "debug":
		parsed = slog.LevelDebug
	case "info":
		parsed = slog.LevelInfo
	case "warn", "warning":
		parsed = slog.LevelWarn
	case "error":
		parsed = slog.LevelError
	default:
		return nil, fmt.Errorf("invalid log level %q", level)
	}
	opts := &slog.HandlerOptions{Level: parsed}
	if format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), nil
	}
	if format != "text" {
		return nil, fmt.Errorf("invalid log format %q", format)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts)), nil
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `codexbar-mqtt publishes raw CodexBar observations to MQTT.

Usage:
  codexbar-mqtt init    [--config PATH] [--force]
  codexbar-mqtt run     [--config PATH] [--log-level LEVEL] [--log-format text|json]
  codexbar-mqtt once    [--config PATH]
  codexbar-mqtt doctor  [--config PATH]
  codexbar-mqtt schema
  codexbar-mqtt version`)
}
