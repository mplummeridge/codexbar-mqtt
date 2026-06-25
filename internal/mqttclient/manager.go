package mqttclient

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

type ConnectHandler func(context.Context, *Manager) error

type Manager struct {
	cfg       Config
	logger    *slog.Logger
	onConnect ConnectHandler

	mu        sync.RWMutex
	client    *Client
	started   bool
	connected chan struct{}
}

func NewManager(cfg Config, logger *slog.Logger, onConnect ConnectHandler) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		cfg:       cfg,
		logger:    logger,
		onConnect: onConnect,
		connected: make(chan struct{}),
	}
}

// Run owns the reconnect loop until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return errors.New("mqtt manager already started")
	}
	m.started = true
	m.mu.Unlock()

	minBackoff := m.cfg.ReconnectMin
	if minBackoff <= 0 {
		minBackoff = time.Second
	}
	maxBackoff := m.cfg.ReconnectMax
	if maxBackoff < minBackoff {
		maxBackoff = 30 * time.Second
	}
	backoff := minBackoff

	for {
		if err := ctx.Err(); err != nil {
			m.disconnectCurrent()
			return err
		}

		client, err := Dial(ctx, m.cfg, m.logger)
		if err != nil {
			m.logger.Warn("MQTT connect failed", "broker", m.cfg.BrokerURL, "error", err, "retry_in", backoff)
			if !sleepContext(ctx, backoff) {
				return ctx.Err()
			}
			backoff = minDuration(backoff*2, maxBackoff)
			continue
		}

		m.setClient(client)
		backoff = minBackoff
		m.logger.Info("MQTT connected", "broker", m.cfg.BrokerURL, "client_id", m.cfg.ClientID)

		if m.onConnect != nil {
			callbackCtx, cancel := context.WithTimeout(ctx, maxDuration(2*m.cfg.PublishTimeout, 10*time.Second))
			if err := m.onConnect(callbackCtx, m); err != nil {
				m.logger.Warn("MQTT on-connect publish failed", "error", err)
			}
			cancel()
		}

		select {
		case <-client.Done():
			m.clearClient(client)
			m.logger.Warn("MQTT disconnected", "retry_in", backoff)
		case <-ctx.Done():
			closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = client.Disconnect(closeCtx)
			cancel()
			m.clearClient(client)
			return ctx.Err()
		}

		if !sleepContext(ctx, backoff) {
			return ctx.Err()
		}
		backoff = minDuration(backoff*2, maxBackoff)
	}
}

func (m *Manager) Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error {
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client == nil || !client.IsConnected() {
		return ErrNotConnected
	}
	return client.Publish(ctx, topic, qos, retain, payload)
}

func (m *Manager) WaitConnected(ctx context.Context) error {
	for {
		if m.IsConnected() {
			return nil
		}
		m.mu.RLock()
		signal := m.connected
		m.mu.RUnlock()
		select {
		case <-signal:
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (m *Manager) IsConnected() bool {
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	return client != nil && client.IsConnected()
}

func IsNotConnected(err error) bool {
	return errors.Is(err, ErrNotConnected)
}

func (m *Manager) setClient(client *Client) {
	m.mu.Lock()
	m.client = client
	close(m.connected)
	m.connected = make(chan struct{})
	m.mu.Unlock()
}

func (m *Manager) clearClient(expected *Client) {
	m.mu.Lock()
	if m.client == expected {
		m.client = nil
	}
	m.mu.Unlock()
}

func (m *Manager) disconnectCurrent() {
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_ = client.Disconnect(ctx)
	cancel()
	m.clearClient(client)
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
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

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
