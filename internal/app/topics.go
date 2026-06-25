package app

import (
	"strings"

	"github.com/mplummeridge/codexbar-mqtt/internal/envelope"
)

func joinTopic(parts ...string) string {
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		for _, segment := range strings.Split(part, "/") {
			if strings.TrimSpace(segment) == "" {
				continue
			}
			segment = envelope.TopicSegment(segment)
			if segment != "" {
				clean = append(clean, segment)
			}
		}
	}
	return strings.Join(clean, "/")
}

func (a *App) availabilityTopic() string {
	return joinTopic(a.cfg.MQTT.TopicPrefix, "nodes", a.cfg.Machine.ID, "availability")
}

func (a *App) metaTopic() string {
	return joinTopic(a.cfg.MQTT.TopicPrefix, "nodes", a.cfg.Machine.ID, "meta")
}

func (a *App) heartbeatTopic() string {
	return joinTopic(a.cfg.MQTT.TopicPrefix, "nodes", a.cfg.Machine.ID, "heartbeat")
}

func (a *App) eventTopic(kind string) string {
	return joinTopic(a.cfg.MQTT.TopicPrefix, "events", a.cfg.Machine.ID, kind)
}

func (a *App) snapshotTopic(kind, scope string) string {
	return joinTopic(a.cfg.MQTT.TopicPrefix, "nodes", a.cfg.Machine.ID, "snapshots", kind, scope)
}
