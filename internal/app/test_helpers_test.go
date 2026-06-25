package app

import (
	"github.com/mplummeridge/codexbar-mqtt/internal/config"
	"github.com/mplummeridge/codexbar-mqtt/internal/envelope"
)

func testAppConfig() config.Config {
	cfg := config.Defaults()
	cfg.Machine.ID = "macbook-m4"
	cfg.Machine.Name = "MacBook M4"
	cfg.MQTT.TopicPrefix = "codexbar/v1"
	return cfg
}

func testMachine() envelope.Machine {
	return envelope.Machine{ID: "macbook-m4", Name: "MacBook M4", Hostname: "macbook.local"}
}

func testAgent() envelope.Agent {
	return envelope.Agent{Version: "0.2.0", Commit: "test", BuildDate: "2026-06-25T00:00:00Z"}
}
