package envelope

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const Schema = "io.github.mplummeridge.codexbar_mqtt.observation.v1"

type Machine struct {
	ID       string            `json:"id"`
	Name     string            `json:"name,omitempty"`
	Hostname string            `json:"hostname,omitempty"`
	Tags     map[string]string `json:"tags,omitempty"`
}

type Agent struct {
	Version   string `json:"version"`
	Commit    string `json:"commit,omitempty"`
	BuildDate string `json:"build_date,omitempty"`
}

type Collection struct {
	Transport     string            `json:"transport"`
	Operation     string            `json:"operation"`
	SemanticScope string            `json:"semantic_scope"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	Phase         string            `json:"phase,omitempty"`
	StartedAt     time.Time         `json:"started_at,omitempty"`
	FinishedAt    time.Time         `json:"finished_at,omitempty"`
	Endpoint      string            `json:"endpoint,omitempty"`
	Query         map[string]string `json:"query,omitempty"`
	Command       []string          `json:"command,omitempty"`
	ExitCode      *int              `json:"exit_code,omitempty"`
	DurationMS    int64             `json:"duration_ms"`
	ContentType   string            `json:"content_type,omitempty"`
	Success       bool              `json:"success"`
	Error         string            `json:"error,omitempty"`
	Stderr        string            `json:"stderr,omitempty"`
}

type Observation struct {
	Schema        string          `json:"schema"`
	EventID       string          `json:"event_id"`
	Kind          string          `json:"kind"`
	SnapshotScope string          `json:"snapshot_scope"`
	ObservedAt    time.Time       `json:"observed_at"`
	Machine       Machine         `json:"machine"`
	Agent         Agent           `json:"agent"`
	Collection    Collection      `json:"collection"`
	PayloadSHA256 string          `json:"payload_sha256,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

type ErrorPayload struct {
	Job       string `json:"job"`
	Error     string `json:"error"`
	Retryable bool   `json:"retryable"`
}

func New(kind, scope string, machine Machine, agent Agent, collection Collection, payload json.RawMessage) Observation {
	observedAt := collection.FinishedAt
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	obs := Observation{
		Schema:        Schema,
		EventID:       newEventID(),
		Kind:          kind,
		SnapshotScope: scope,
		ObservedAt:    observedAt.UTC(),
		Machine:       machine,
		Agent:         agent,
		Collection:    collection,
	}
	if len(payload) > 0 {
		obs.Payload = append(json.RawMessage(nil), payload...)
		h := sha256.Sum256(payload)
		obs.PayloadSHA256 = hex.EncodeToString(h[:])
	}
	return obs
}

func NewError(kind, scope string, machine Machine, agent Agent, collection Collection, job string, err error, retryable bool) Observation {
	collection.Success = false
	if err != nil {
		collection.Error = err.Error()
	}
	payload, _ := json.Marshal(ErrorPayload{Job: job, Error: collection.Error, Retryable: retryable})
	return New(kind, scope, machine, agent, collection, payload)
}

func (o Observation) Marshal() ([]byte, error) {
	return json.Marshal(o)
}

func newEventID() string {
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return fmt.Sprintf("%x-%d", time.Now().UnixNano(), time.Now().Unix())
	}
	return fmt.Sprintf("%016x-%s", uint64(time.Now().UnixNano()), hex.EncodeToString(random[:]))
}

func TopicSegment(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		valid := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-'
		if valid {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "default"
	}
	return out
}
