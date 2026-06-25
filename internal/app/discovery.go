package app

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

const (
	discoveryRoot   = "codexbar/discovery/v1"
	discoverySchema = "io.github.mplummeridge.codexbar_mqtt.discovery.v1"
	contractMajor   = 1
)

// fleetID derives a stable fleet identifier from the effective MQTT topic
// prefix. It intentionally matches the Home Assistant integration: the first
// 64 bits of SHA-256 over the normalized prefix.
func fleetID(topicPrefix string) string {
	canonical := joinTopic(topicPrefix)
	digest := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(digest[:8])
}

func (a *App) fleetID() string {
	return fleetID(a.cfg.MQTT.TopicPrefix)
}

func (a *App) discoveryTopic() string {
	return joinTopic(discoveryRoot, a.fleetID(), a.cfg.Machine.ID)
}

func (a *App) discoveryPayload() ([]byte, error) {
	prefix := joinTopic(a.cfg.MQTT.TopicPrefix)
	payload := map[string]any{
		"schema": discoverySchema,
		"fleet": map[string]any{
			"id":             a.fleetID(),
			"topic_prefix":   prefix,
			"contract_major": contractMajor,
		},
		"machine": a.machine,
		"agent":   a.agent,
		"topic_contract": map[string]string{
			"availability": a.availabilityTopic(),
			"meta":         a.metaTopic(),
			"heartbeat":    a.heartbeatTopic(),
			"events":       joinTopic(prefix, "events", a.cfg.Machine.ID, "<kind>"),
			"snapshots":    joinTopic(prefix, "nodes", a.cfg.Machine.ID, "snapshots", "<kind>", "<scope>"),
		},
		"published_at": time.Now().UTC(),
	}
	return json.Marshal(payload)
}
