package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
)

const DefaultConfigPath = "~/Library/Application Support/codexbar-mqtt/config.json"

var machineIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type Config struct {
	Machine  MachineConfig  `json:"machine"`
	CodexBar CodexBarConfig `json:"codexbar"`
	MQTT     MQTTConfig     `json:"mqtt"`
	Poll     PollConfig     `json:"poll"`
	Spool    SpoolConfig    `json:"spool"`
	Publish  PublishConfig  `json:"publish"`
}

type MachineConfig struct {
	ID   string            `json:"id"`
	Name string            `json:"name,omitempty"`
	Tags map[string]string `json:"tags,omitempty"`
}

type CodexBarConfig struct {
	BaseURL                     string `json:"base_url"`
	Binary                      string `json:"binary,omitempty"`
	ManageServe                 bool   `json:"manage_serve"`
	ServePort                   int    `json:"serve_port"`
	ServeRefreshIntervalSeconds int    `json:"serve_refresh_interval_seconds"`
	ServeRequestTimeoutSeconds  int    `json:"serve_request_timeout_seconds"`
	HTTPTimeoutSeconds          int    `json:"http_timeout_seconds"`
	CLITimeoutSeconds           int    `json:"cli_timeout_seconds"`
	UsageProvider               string `json:"usage_provider,omitempty"`
	CostProvider                string `json:"cost_provider,omitempty"`
}

type MQTTConfig struct {
	Broker                string    `json:"broker"`
	ClientID              string    `json:"client_id,omitempty"`
	Username              string    `json:"username,omitempty"`
	Password              string    `json:"password,omitempty"`
	PasswordEnv           string    `json:"password_env,omitempty"`
	PasswordFile          string    `json:"password_file,omitempty"`
	TopicPrefix           string    `json:"topic_prefix"`
	QoS                   byte      `json:"qos"`
	KeepAliveSeconds      int       `json:"keep_alive_seconds"`
	ConnectTimeoutSeconds int       `json:"connect_timeout_seconds"`
	PublishTimeoutSeconds int       `json:"publish_timeout_seconds"`
	ReconnectMinSeconds   int       `json:"reconnect_min_seconds"`
	ReconnectMaxSeconds   int       `json:"reconnect_max_seconds"`
	TLS                   TLSConfig `json:"tls,omitempty"`
}

type TLSConfig struct {
	CAFile             string `json:"ca_file,omitempty"`
	CertFile           string `json:"cert_file,omitempty"`
	KeyFile            string `json:"key_file,omitempty"`
	ServerName         string `json:"server_name,omitempty"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify,omitempty"`
}

type PollConfig struct {
	HeartbeatSeconds            int      `json:"heartbeat_seconds"`
	HealthSeconds               int      `json:"health_seconds"`
	UsageSeconds                int      `json:"usage_seconds"`
	CostSeconds                 int      `json:"cost_seconds"`
	AllProvidersSeconds         int      `json:"all_providers_seconds"`
	StatusSeconds               int      `json:"status_seconds"`
	StatusProvider              string   `json:"status_provider,omitempty"`
	ActiveAccountProbeSeconds   int      `json:"active_account_probe_seconds"`
	ActiveAccountProbeProviders []string `json:"active_account_probe_providers,omitempty"`
	AccountCatalogueSeconds     int      `json:"account_catalogue_seconds"`
	AccountCatalogueProviders   []string `json:"account_catalogue_providers,omitempty"`
	CostHorizonSeconds          int      `json:"cost_horizon_seconds"`
	CostHorizonsDays            []int    `json:"cost_horizons_days,omitempty"`
	ConfigValidateSeconds       int      `json:"config_validate_seconds"`
}

type SpoolConfig struct {
	Directory   string `json:"directory"`
	MaxMessages int    `json:"max_messages"`
	MaxBytes    int64  `json:"max_bytes"`
}

type PublishConfig struct {
	RetainSnapshots bool `json:"retain_snapshots"`
	PublishEvents   bool `json:"publish_events"`
	IncludeStderr   bool `json:"include_stderr"`
}

func Defaults() Config {
	host, _ := os.Hostname()
	if host == "" {
		host = "mac"
	}
	id := sanitizeMachineID(host)
	return Config{
		Machine: MachineConfig{ID: id, Name: host, Tags: map[string]string{}},
		CodexBar: CodexBarConfig{
			BaseURL:                     "http://127.0.0.1:8080",
			ManageServe:                 true,
			ServePort:                   8080,
			ServeRefreshIntervalSeconds: 60,
			ServeRequestTimeoutSeconds:  30,
			HTTPTimeoutSeconds:          90,
			CLITimeoutSeconds:           120,
			CostProvider:                "both",
		},
		MQTT: MQTTConfig{
			Broker:                "mqtt://homeassistant:1883",
			TopicPrefix:           "codexbar/v1",
			QoS:                   1,
			KeepAliveSeconds:      30,
			ConnectTimeoutSeconds: 10,
			PublishTimeoutSeconds: 15,
			ReconnectMinSeconds:   1,
			ReconnectMaxSeconds:   30,
		},
		Poll: PollConfig{
			HeartbeatSeconds:            30,
			HealthSeconds:               30,
			UsageSeconds:                60,
			CostSeconds:                 300,
			AllProvidersSeconds:         1800,
			StatusSeconds:               900,
			StatusProvider:              "all",
			ActiveAccountProbeSeconds:   60,
			ActiveAccountProbeProviders: []string{"codex", "claude"},
			AccountCatalogueSeconds:     1800,
			AccountCatalogueProviders:   []string{"codex", "claude"},
			CostHorizonSeconds:          3600,
			CostHorizonsDays:            []int{1, 7, 30, 90},
			ConfigValidateSeconds:       3600,
		},
		Spool: SpoolConfig{
			Directory:   "~/Library/Application Support/codexbar-mqtt/spool",
			MaxMessages: 10000,
			MaxBytes:    100 * 1024 * 1024,
		},
		Publish: PublishConfig{RetainSnapshots: true, PublishEvents: true, IncludeStderr: false},
	}
}

func Load(path string) (Config, error) {
	cfg := Defaults()
	expanded, err := ExpandPath(path)
	if err != nil {
		return Config{}, err
	}
	data, err := os.ReadFile(expanded)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", expanded, err)
	}
	data = []byte(os.ExpandEnv(string(data)))
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config %q: %w", expanded, err)
	}
	cfg.Spool.Directory, err = ExpandPath(cfg.Spool.Directory)
	if err != nil {
		return Config{}, fmt.Errorf("expand spool directory: %w", err)
	}
	for _, p := range []*string{&cfg.CodexBar.Binary, &cfg.MQTT.PasswordFile, &cfg.MQTT.TLS.CAFile, &cfg.MQTT.TLS.CertFile, &cfg.MQTT.TLS.KeyFile} {
		if *p == "" {
			continue
		}
		*p, err = ExpandPath(*p)
		if err != nil {
			return Config{}, err
		}
	}
	cfg.MQTT.TopicPrefix = strings.Trim(cfg.MQTT.TopicPrefix, "/ ")
	cfg.Machine.ID = sanitizeMachineID(cfg.Machine.ID)
	if cfg.Machine.Name == "" {
		cfg.Machine.Name = cfg.Machine.ID
	}
	if cfg.MQTT.ClientID == "" {
		cfg.MQTT.ClientID = "codexbar-mqtt-" + cfg.Machine.ID
	}
	if cfg.MQTT.PasswordFile != "" {
		value, readErr := os.ReadFile(cfg.MQTT.PasswordFile)
		if readErr != nil {
			return Config{}, fmt.Errorf("read mqtt.password_file: %w", readErr)
		}
		cfg.MQTT.Password = strings.TrimSpace(string(value))
	}
	if cfg.MQTT.PasswordEnv != "" {
		if value, ok := os.LookupEnv(cfg.MQTT.PasswordEnv); ok {
			cfg.MQTT.Password = value
		}
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func WriteExample(path string, force bool) (string, error) {
	expanded, err := ExpandPath(path)
	if err != nil {
		return "", err
	}
	if !force {
		if _, err := os.Stat(expanded); err == nil {
			return "", fmt.Errorf("config already exists: %s", expanded)
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
	}
	cfg := Defaults()
	cfg.MQTT.PasswordEnv = "CODEXBAR_MQTT_PASSWORD"
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(expanded), 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(expanded, append(data, '\n'), 0o600); err != nil {
		return "", err
	}
	return expanded, nil
}

func (c Config) Validate() error {
	var problems []string
	if !machineIDPattern.MatchString(c.Machine.ID) {
		problems = append(problems, "machine.id must match "+machineIDPattern.String())
	}
	if c.CodexBar.BaseURL == "" {
		problems = append(problems, "codexbar.base_url is required")
	} else if u, err := url.Parse(c.CodexBar.BaseURL); err != nil || u.Scheme == "" || u.Host == "" {
		problems = append(problems, "codexbar.base_url must be an absolute HTTP URL")
	}
	if c.CodexBar.ServePort < 1 || c.CodexBar.ServePort > 65535 {
		problems = append(problems, "codexbar.serve_port must be 1..65535")
	}
	if c.CodexBar.ManageServe {
		u, _ := url.Parse(c.CodexBar.BaseURL)
		host := strings.ToLower(u.Hostname())
		if host != "127.0.0.1" && host != "localhost" && host != "::1" {
			problems = append(problems, "codexbar.manage_serve requires a loopback base_url")
		}
	}
	if c.MQTT.Broker == "" {
		problems = append(problems, "mqtt.broker is required")
	} else if u, err := url.Parse(c.MQTT.Broker); err != nil || u.Scheme == "" || u.Host == "" {
		problems = append(problems, "mqtt.broker must be an absolute MQTT URL")
	} else if !contains([]string{"mqtt", "mqtts", "tcp", "tls", "ssl"}, strings.ToLower(u.Scheme)) {
		problems = append(problems, "mqtt.broker scheme must be mqtt, mqtts, tcp, tls, or ssl")
	}
	if c.MQTT.TopicPrefix == "" || strings.ContainsAny(c.MQTT.TopicPrefix, "+#") {
		problems = append(problems, "mqtt.topic_prefix is required and cannot contain MQTT wildcards")
	}
	if c.MQTT.QoS > 1 {
		problems = append(problems, "mqtt.qos must be 0 or 1")
	}
	if (c.MQTT.TLS.CertFile == "") != (c.MQTT.TLS.KeyFile == "") {
		problems = append(problems, "mqtt.tls.cert_file and key_file must be configured together")
	}
	if c.Spool.Directory == "" || c.Spool.MaxMessages < 1 || c.Spool.MaxBytes < 1024 {
		problems = append(problems, "spool directory and positive limits are required")
	}
	for _, d := range c.Poll.CostHorizonsDays {
		if d < 1 || d > 365 {
			problems = append(problems, fmt.Sprintf("poll.cost_horizons_days value %d must be 1..365", d))
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func ExpandPath(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	path = os.ExpandEnv(path)
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if path == "~" {
			return home, nil
		}
		path = filepath.Join(home, strings.TrimPrefix(path, "~/"))
	}
	if runtime.GOOS == "windows" {
		path = filepath.Clean(path)
	}
	return path, nil
}

func sanitizeMachineID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = "mac"
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), ".-_")
	if out == "" {
		out = "mac"
	}
	if len(out) > 128 {
		out = out[:128]
	}
	return out
}

func contains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}
