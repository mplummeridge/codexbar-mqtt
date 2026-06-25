package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAndLoadExample(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if _, err := WriteExample(path, false); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CODEXBAR_MQTT_PASSWORD", "secret")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MQTT.Password != "secret" || cfg.Machine.ID == "" || cfg.Poll.ActiveAccountProbeSeconds <= 0 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("config is too permissive: %o", info.Mode().Perm())
	}
}
