package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/mplummeridge/codexbar-mqtt/internal/codexbar"
	"github.com/mplummeridge/codexbar-mqtt/internal/config"
	"github.com/mplummeridge/codexbar-mqtt/internal/envelope"
	"github.com/mplummeridge/codexbar-mqtt/internal/mqttclient"
	"github.com/mplummeridge/codexbar-mqtt/internal/spool"
	"github.com/mplummeridge/codexbar-mqtt/internal/version"
)

type JobState struct {
	LastAttemptAt time.Time `json:"last_attempt_at,omitempty"`
	LastSuccessAt time.Time `json:"last_success_at,omitempty"`
	LastErrorAt   time.Time `json:"last_error_at,omitempty"`
	LastError     string    `json:"last_error,omitempty"`
	Attempts      uint64    `json:"attempts"`
	Successes     uint64    `json:"successes"`
}

type App struct {
	cfg       config.Config
	logger    *slog.Logger
	http      *codexbar.HTTPClient
	runner    codexbar.Runner
	spool     *spool.Queue
	mqtt      *mqttclient.Manager
	machine   envelope.Machine
	agent     envelope.Agent
	startedAt time.Time

	cliMu           sync.Mutex
	mu              sync.RWMutex
	jobs            map[string]JobState
	codexBarVersion string
}

func New(cfg config.Config, logger *slog.Logger) (*App, error) {
	if logger == nil {
		logger = slog.Default()
	}
	binary, err := codexbar.DiscoverBinary(cfg.CodexBar.Binary)
	if err != nil {
		return nil, err
	}
	runner := codexbar.Runner{Binary: binary, Timeout: time.Duration(cfg.CodexBar.CLITimeoutSeconds) * time.Second}
	hostname, _ := os.Hostname()
	queue, err := spool.New(cfg.Spool.Directory, cfg.Spool.MaxMessages, cfg.Spool.MaxBytes, logger)
	if err != nil {
		return nil, err
	}
	a := &App{
		cfg:    cfg,
		logger: logger,
		http:   codexbar.NewHTTPClient(cfg.CodexBar.BaseURL, time.Duration(cfg.CodexBar.HTTPTimeoutSeconds)*time.Second),
		runner: runner,
		spool:  queue,
		machine: envelope.Machine{
			ID: cfg.Machine.ID, Name: cfg.Machine.Name, Hostname: hostname, Tags: cfg.Machine.Tags,
		},
		agent:     envelope.Agent{Version: version.Version, Commit: version.Commit, BuildDate: version.Date},
		startedAt: time.Now().UTC(),
		jobs:      make(map[string]JobState),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	a.codexBarVersion, _ = runner.Version(ctx)
	cancel()

	mqttCfg := mqttclient.Config{
		BrokerURL:          cfg.MQTT.Broker,
		ClientID:           cfg.MQTT.ClientID,
		Username:           cfg.MQTT.Username,
		Password:           cfg.MQTT.Password,
		KeepAlive:          time.Duration(cfg.MQTT.KeepAliveSeconds) * time.Second,
		ConnectTimeout:     time.Duration(cfg.MQTT.ConnectTimeoutSeconds) * time.Second,
		PublishTimeout:     time.Duration(cfg.MQTT.PublishTimeoutSeconds) * time.Second,
		ReconnectMin:       time.Duration(cfg.MQTT.ReconnectMinSeconds) * time.Second,
		ReconnectMax:       time.Duration(cfg.MQTT.ReconnectMaxSeconds) * time.Second,
		WillTopic:          a.availabilityTopic(),
		WillPayload:        []byte("offline"),
		WillQoS:            cfg.MQTT.QoS,
		WillRetain:         true,
		CAFile:             cfg.MQTT.TLS.CAFile,
		CertFile:           cfg.MQTT.TLS.CertFile,
		KeyFile:            cfg.MQTT.TLS.KeyFile,
		ServerName:         cfg.MQTT.TLS.ServerName,
		InsecureSkipVerify: cfg.MQTT.TLS.InsecureSkipVerify,
	}
	a.mqtt = mqttclient.NewManager(mqttCfg, logger, a.onMQTTConnect)
	return a, nil
}

func (a *App) Run(ctx context.Context) error {
	mqttCtx, mqttCancel := context.WithCancel(context.Background())
	defer mqttCancel()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := a.mqtt.Run(mqttCtx); err != nil && !errors.Is(err, context.Canceled) {
			a.logger.Error("MQTT manager stopped", "error", err)
		}
	}()

	if a.cfg.CodexBar.ManageServe {
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervisor := codexbar.ServeSupervisor{
				Runner:          a.runner,
				HTTP:            a.http,
				Port:            a.cfg.CodexBar.ServePort,
				RefreshInterval: a.cfg.CodexBar.ServeRefreshIntervalSeconds,
				RequestTimeout:  a.cfg.CodexBar.ServeRequestTimeoutSeconds,
				Logger:          a.logger,
			}
			if err := supervisor.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				a.logger.Error("CodexBar serve supervisor stopped", "error", err)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		a.drainLoop(ctx)
	}()

	for _, job := range a.buildJobs() {
		job := job
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.scheduleJob(ctx, job)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		a.heartbeatLoop(ctx)
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_ = a.mqtt.Publish(shutdownCtx, a.availabilityTopic(), a.cfg.MQTT.QoS, true, []byte("offline"))
	cancel()
	mqttCancel()
	wg.Wait()
	return nil
}

func (a *App) RunOnce(ctx context.Context) error {
	managedCtx, managedCancel := context.WithCancel(ctx)
	defer managedCancel()
	if a.cfg.CodexBar.ManageServe {
		go func() {
			supervisor := codexbar.ServeSupervisor{
				Runner: a.runner, HTTP: a.http, Port: a.cfg.CodexBar.ServePort,
				RefreshInterval: a.cfg.CodexBar.ServeRefreshIntervalSeconds,
				RequestTimeout:  a.cfg.CodexBar.ServeRequestTimeoutSeconds, Logger: a.logger,
			}
			_ = supervisor.Run(managedCtx)
		}()
	}
	if err := a.waitForServe(ctx, 30*time.Second); err != nil {
		return err
	}

	mqttCtx, mqttCancel := context.WithCancel(context.Background())
	defer mqttCancel()
	go func() { _ = a.mqtt.Run(mqttCtx) }()
	connectCtx, cancel := context.WithTimeout(ctx, time.Duration(a.cfg.MQTT.ConnectTimeoutSeconds+5)*time.Second)
	if err := a.mqtt.WaitConnected(connectCtx); err != nil {
		cancel()
		return fmt.Errorf("connect MQTT: %w", err)
	}
	cancel()

	jobs := a.buildJobs()
	var firstErr error
	for _, job := range jobs {
		if err := a.executeJob(ctx, job); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := a.publishHeartbeat(ctx); err != nil && firstErr == nil {
		firstErr = err
	}
	for {
		stats, err := a.spool.Stats()
		if err != nil {
			return err
		}
		if stats.Messages == 0 {
			break
		}
		if err := a.spool.Drain(ctx, a.mqtt, time.Duration(a.cfg.MQTT.PublishTimeoutSeconds)*time.Second); err != nil {
			return err
		}
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	_ = a.mqtt.Publish(shutdownCtx, a.availabilityTopic(), a.cfg.MQTT.QoS, true, []byte("offline"))
	shutdownCancel()
	mqttCancel()
	return firstErr
}

func (a *App) waitForServe(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		healthy := a.http.Healthy(checkCtx)
		cancel()
		if healthy {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("CodexBar serve did not become healthy at %s", a.cfg.CodexBar.BaseURL)
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *App) onMQTTConnect(ctx context.Context, manager *mqttclient.Manager) error {
	if err := manager.Publish(ctx, a.availabilityTopic(), a.cfg.MQTT.QoS, true, []byte("online")); err != nil {
		return err
	}
	meta, err := a.metaPayload()
	if err != nil {
		return err
	}
	if err := manager.Publish(ctx, a.metaTopic(), a.cfg.MQTT.QoS, true, meta); err != nil {
		return err
	}
	discovery, err := a.discoveryPayload()
	if err != nil {
		a.logger.Warn("marshal Home Assistant discovery beacon failed", "error", err)
	} else if err := manager.Publish(ctx, a.discoveryTopic(), a.cfg.MQTT.QoS, true, discovery); err != nil {
		// Discovery is a bootstrap convenience. A legacy ACL that blocks the
		// well-known topic must not stop the observation data plane.
		a.logger.Warn("publish Home Assistant discovery beacon failed", "topic", a.discoveryTopic(), "error", err)
	}
	go func() {
		drainCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		_ = a.spool.Drain(drainCtx, a.mqtt, time.Duration(a.cfg.MQTT.PublishTimeoutSeconds)*time.Second)
	}()
	return nil
}

func (a *App) metaPayload() ([]byte, error) {
	payload := map[string]any{
		"schema":   "io.github.mplummeridge.codexbar_mqtt.node_meta.v1",
		"machine":  a.machine,
		"agent":    a.agent,
		"platform": map[string]string{"os": runtime.GOOS, "arch": runtime.GOARCH},
		"codexbar": map[string]any{
			"binary":        a.runner.Binary,
			"cli_version":   a.codexBarVersion,
			"base_url":      a.cfg.CodexBar.BaseURL,
			"managed_serve": a.cfg.CodexBar.ManageServe,
		},
		"fleet": map[string]any{
			"id":             a.fleetID(),
			"topic_prefix":   joinTopic(a.cfg.MQTT.TopicPrefix),
			"contract_major": contractMajor,
		},
		"topic_contract": map[string]string{
			"discovery":    a.discoveryTopic(),
			"availability": a.availabilityTopic(),
			"heartbeat":    a.heartbeatTopic(),
			"events":       joinTopic(a.cfg.MQTT.TopicPrefix, "events", a.cfg.Machine.ID, "<kind>"),
			"snapshots":    joinTopic(a.cfg.MQTT.TopicPrefix, "nodes", a.cfg.Machine.ID, "snapshots", "<kind>", "<scope>"),
		},
		"published_at": time.Now().UTC(),
	}
	return json.Marshal(payload)
}

func (a *App) enqueueObservation(obs envelope.Observation) error {
	payload, err := obs.Marshal()
	if err != nil {
		return err
	}
	records := make([]spool.Record, 0, 2)
	if a.cfg.Publish.PublishEvents {
		records = append(records, spool.Record{
			Class: "event", Topic: a.eventTopic(obs.Kind), QoS: a.cfg.MQTT.QoS, Retain: false, Payload: payload,
		})
	}
	if a.cfg.Publish.RetainSnapshots && obs.Collection.Success {
		records = append(records, spool.Record{
			Class: "snapshot", Topic: a.snapshotTopic(obs.Kind, obs.SnapshotScope), QoS: a.cfg.MQTT.QoS, Retain: true, Payload: payload,
		})
	}
	if len(records) == 0 {
		return nil
	}
	return a.spool.Enqueue(records...)
}

func (a *App) drainLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !a.mqtt.IsConnected() {
				continue
			}
			if err := a.spool.Drain(ctx, a.mqtt, time.Duration(a.cfg.MQTT.PublishTimeoutSeconds)*time.Second); err != nil && ctx.Err() == nil && !mqttclient.IsNotConnected(err) {
				a.logger.Warn("spool drain failed", "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (a *App) heartbeatLoop(ctx context.Context) {
	interval := time.Duration(a.cfg.Poll.HeartbeatSeconds) * time.Second
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	_ = a.publishHeartbeat(ctx)
	for {
		select {
		case <-ticker.C:
			_ = a.publishHeartbeat(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (a *App) publishHeartbeat(ctx context.Context) error {
	stats, err := a.spool.Stats()
	if err != nil {
		return err
	}
	a.mu.RLock()
	jobs := make(map[string]JobState, len(a.jobs))
	keys := make([]string, 0, len(a.jobs))
	for key := range a.jobs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		jobs[key] = a.jobs[key]
	}
	a.mu.RUnlock()
	payload, err := json.Marshal(map[string]any{
		"schema":         "io.github.mplummeridge.codexbar_mqtt.heartbeat.v1",
		"machine_id":     a.cfg.Machine.ID,
		"observed_at":    time.Now().UTC(),
		"uptime_seconds": int64(time.Since(a.startedAt).Seconds()),
		"mqtt_connected": a.mqtt.IsConnected(),
		"spool":          stats,
		"jobs":           jobs,
	})
	if err != nil {
		return err
	}
	publishCtx, cancel := context.WithTimeout(ctx, time.Duration(a.cfg.MQTT.PublishTimeoutSeconds)*time.Second)
	defer cancel()
	return a.mqtt.Publish(publishCtx, a.heartbeatTopic(), a.cfg.MQTT.QoS, true, payload)
}

func (a *App) updateJob(name string, err error) {
	now := time.Now().UTC()
	a.mu.Lock()
	state := a.jobs[name]
	state.LastAttemptAt = now
	state.Attempts++
	if err == nil {
		state.LastSuccessAt = now
		state.Successes++
		state.LastError = ""
	} else {
		state.LastErrorAt = now
		state.LastError = err.Error()
	}
	a.jobs[name] = state
	a.mu.Unlock()
}
