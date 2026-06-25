package app

import (
	"encoding/json"
	"testing"
)

func TestFleetIDNormalisesPrefix(t *testing.T) {
	t.Parallel()

	got := fleetID("/CodexBar//V1/")
	want := fleetID("codexbar/v1")
	if got != want {
		t.Fatalf("fleet IDs differ after normalization: got %q want %q", got, want)
	}
	if len(got) != 16 {
		t.Fatalf("fleet ID length = %d, want 16", len(got))
	}
}

func TestDiscoveryPayload(t *testing.T) {
	t.Parallel()

	a := &App{
		cfg:     testAppConfig(),
		machine: testMachine(),
		agent:   testAgent(),
	}
	payload, err := a.discoveryPayload()
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(payload, &document); err != nil {
		t.Fatal(err)
	}
	if document["schema"] != discoverySchema {
		t.Fatalf("schema = %v", document["schema"])
	}
	fleet, ok := document["fleet"].(map[string]any)
	if !ok {
		t.Fatalf("fleet payload type = %T", document["fleet"])
	}
	if fleet["id"] != a.fleetID() {
		t.Fatalf("fleet.id = %v", fleet["id"])
	}
	if fleet["topic_prefix"] != "codexbar/v1" {
		t.Fatalf("fleet.topic_prefix = %v", fleet["topic_prefix"])
	}
	if fleet["contract_major"] != float64(contractMajor) {
		t.Fatalf("fleet.contract_major = %v", fleet["contract_major"])
	}
	if got, want := a.discoveryTopic(), "codexbar/discovery/v1/"+a.fleetID()+"/macbook-m4"; got != want {
		t.Fatalf("discovery topic = %q, want %q", got, want)
	}
}
