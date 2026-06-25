package spool

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

type fakePublisher struct {
	records []Record
}

func (f *fakePublisher) Publish(_ context.Context, topic string, qos byte, retain bool, payload []byte) error {
	f.records = append(f.records, Record{Topic: topic, QoS: qos, Retain: retain, Payload: append([]byte(nil), payload...)})
	return nil
}

func TestQueueDrain(t *testing.T) {
	queue, err := New(t.TempDir(), 100, 1<<20, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Enqueue(
		Record{Class: "event", Topic: "events/a", QoS: 1, Payload: []byte("one")},
		Record{Class: "snapshot", Topic: "snapshots/a", QoS: 1, Retain: true, Payload: []byte("two")},
	); err != nil {
		t.Fatal(err)
	}
	publisher := &fakePublisher{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := queue.Drain(ctx, publisher, time.Second); err != nil {
		t.Fatal(err)
	}
	if len(publisher.records) != 2 {
		t.Fatalf("got %d records", len(publisher.records))
	}
	stats, err := queue.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Messages != 0 {
		t.Fatalf("queue not empty: %+v", stats)
	}
}

func TestQueueCoalescesSnapshotsWhenOverLimit(t *testing.T) {
	queue, err := New(t.TempDir(), 2, 1<<20, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range []string{"one", "two", "three"} {
		if err := queue.Enqueue(Record{Class: "snapshot", Topic: "snapshots/a", QoS: 1, Retain: true, Payload: []byte(payload)}); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := queue.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Messages != 1 || stats.Dropped < 2 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	publisher := &fakePublisher{}
	if err := queue.Drain(context.Background(), publisher, time.Second); err != nil {
		t.Fatal(err)
	}
	if len(publisher.records) != 1 || string(publisher.records[0].Payload) != "three" {
		t.Fatalf("unexpected records: %+v", publisher.records)
	}
}
