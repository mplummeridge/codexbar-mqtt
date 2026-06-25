package app

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type DoctorCheck struct {
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
	Error  string `json:"error,omitempty"`
}

type DoctorReport struct {
	OK        bool                   `json:"ok"`
	MachineID string                 `json:"machine_id"`
	Checks    map[string]DoctorCheck `json:"checks"`
}

func (a *App) Doctor(ctx context.Context) DoctorReport {
	report := DoctorReport{OK: true, MachineID: a.cfg.Machine.ID, Checks: map[string]DoctorCheck{}}
	set := func(name string, check DoctorCheck) {
		report.Checks[name] = check
		if !check.OK {
			report.OK = false
		}
	}

	versionCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	codexVersion, err := a.runner.Version(versionCtx)
	cancel()
	if err != nil {
		set("codexbar_binary", DoctorCheck{OK: false, Error: err.Error()})
	} else {
		set("codexbar_binary", DoctorCheck{OK: true, Detail: fmt.Sprintf("%s (%s)", a.runner.Binary, codexVersion)})
	}

	healthCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	health, err := a.http.Fetch(healthCtx, "/health", nil)
	cancel()
	if err != nil {
		set("codexbar_health", DoctorCheck{OK: false, Error: err.Error()})
	} else {
		set("codexbar_health", DoctorCheck{OK: true, Detail: string(health.Payload)})
	}

	usageCtx, cancel := context.WithTimeout(ctx, time.Duration(a.cfg.CodexBar.HTTPTimeoutSeconds)*time.Second)
	usage, err := a.http.Fetch(usageCtx, "/usage", map[string]string{"provider": "both"})
	cancel()
	if err != nil {
		set("codexbar_usage", DoctorCheck{OK: false, Error: err.Error()})
	} else {
		var rows []json.RawMessage
		if err := json.Unmarshal(usage.Payload, &rows); err != nil {
			set("codexbar_usage", DoctorCheck{OK: false, Error: err.Error()})
		} else {
			set("codexbar_usage", DoctorCheck{OK: true, Detail: fmt.Sprintf("%d provider/account rows", len(rows))})
		}
	}

	mqttCtx, mqttCancel := context.WithCancel(context.Background())
	go func() { _ = a.mqtt.Run(mqttCtx) }()
	connectCtx, cancel := context.WithTimeout(ctx, time.Duration(a.cfg.MQTT.ConnectTimeoutSeconds+5)*time.Second)
	err = a.mqtt.WaitConnected(connectCtx)
	cancel()
	if err != nil {
		set("mqtt", DoctorCheck{OK: false, Error: err.Error()})
	} else {
		payload, _ := json.Marshal(map[string]any{
			"schema":      "dev.mmv3.codexbar-mqtt.doctor.v1",
			"machine_id":  a.cfg.Machine.ID,
			"observed_at": time.Now().UTC(),
		})
		publishCtx, publishCancel := context.WithTimeout(ctx, time.Duration(a.cfg.MQTT.PublishTimeoutSeconds)*time.Second)
		err = a.mqtt.Publish(publishCtx, joinTopic(a.cfg.MQTT.TopicPrefix, "doctor", a.cfg.Machine.ID), a.cfg.MQTT.QoS, false, payload)
		publishCancel()
		if err != nil {
			set("mqtt", DoctorCheck{OK: false, Error: err.Error()})
		} else {
			set("mqtt", DoctorCheck{OK: true, Detail: a.cfg.MQTT.Broker})
		}

		discovery, discoveryErr := a.discoveryPayload()
		if discoveryErr == nil {
			discoveryCtx, discoveryCancel := context.WithTimeout(ctx, time.Duration(a.cfg.MQTT.PublishTimeoutSeconds)*time.Second)
			discoveryErr = a.mqtt.Publish(discoveryCtx, a.discoveryTopic(), a.cfg.MQTT.QoS, true, discovery)
			discoveryCancel()
		}
		if discoveryErr != nil {
			set("mqtt_discovery", DoctorCheck{OK: false, Error: discoveryErr.Error()})
		} else {
			set("mqtt_discovery", DoctorCheck{OK: true, Detail: a.discoveryTopic()})
		}
	}
	mqttCancel()

	stats, err := a.spool.Stats()
	if err != nil {
		set("spool", DoctorCheck{OK: false, Error: err.Error()})
	} else {
		set("spool", DoctorCheck{OK: true, Detail: fmt.Sprintf("%s: %d messages, %d bytes", a.cfg.Spool.Directory, stats.Messages, stats.Bytes)})
	}
	return report
}
