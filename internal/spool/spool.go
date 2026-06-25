package spool

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Record struct {
	ID      string `json:"id"`
	Class   string `json:"class"` // event | snapshot
	Topic   string `json:"topic"`
	QoS     byte   `json:"qos"`
	Retain  bool   `json:"retain"`
	Payload []byte `json:"payload"`
}

type Publisher interface {
	Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error
}

type Stats struct {
	Messages int    `json:"messages"`
	Bytes    int64  `json:"bytes"`
	Dropped  uint64 `json:"dropped"`
}

type Queue struct {
	dir         string
	maxMessages int
	maxBytes    int64
	logger      *slog.Logger
	mu          sync.Mutex
	drainMu     sync.Mutex
	dropped     atomic.Uint64
}

func New(dir string, maxMessages int, maxBytes int64, logger *slog.Logger) (*Queue, error) {
	if dir == "" {
		return nil, errors.New("spool directory is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create spool directory: %w", err)
	}
	return &Queue{dir: dir, maxMessages: maxMessages, maxBytes: maxBytes, logger: logger}, nil
}

func (q *Queue) Enqueue(records ...Record) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i := range records {
		if records[i].ID == "" {
			records[i].ID = newID()
		}
		if records[i].Class == "" {
			records[i].Class = "event"
		}
		data, err := json.Marshal(records[i])
		if err != nil {
			return err
		}
		name := records[i].ID + ".json"
		tmp := filepath.Join(q.dir, "."+name+".tmp")
		final := filepath.Join(q.dir, name)
		if err := os.WriteFile(tmp, data, 0o600); err != nil {
			return fmt.Errorf("write spool record: %w", err)
		}
		if err := os.Rename(tmp, final); err != nil {
			_ = os.Remove(tmp)
			return fmt.Errorf("commit spool record: %w", err)
		}
	}
	return q.pruneLocked()
}

func (q *Queue) Drain(ctx context.Context, publisher Publisher, publishTimeout time.Duration) error {
	q.drainMu.Lock()
	defer q.drainMu.Unlock()
	for {
		path, record, ok, err := q.peek()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		publishCtx := ctx
		cancel := func() {}
		if publishTimeout > 0 {
			publishCtx, cancel = context.WithTimeout(ctx, publishTimeout)
		}
		err = publisher.Publish(publishCtx, record.Topic, record.QoS, record.Retain, record.Payload)
		cancel()
		if err != nil {
			return err
		}
		q.mu.Lock()
		removeErr := os.Remove(path)
		q.mu.Unlock()
		if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return removeErr
		}
	}
}

func (q *Queue) Stats() (Stats, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	files, total, err := q.listLocked()
	if err != nil {
		return Stats{}, err
	}
	return Stats{Messages: len(files), Bytes: total, Dropped: q.dropped.Load()}, nil
}

func (q *Queue) peek() (string, Record, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	files, _, err := q.listLocked()
	if err != nil {
		return "", Record{}, false, err
	}
	if len(files) == 0 {
		return "", Record{}, false, nil
	}
	path := filepath.Join(q.dir, files[0].Name())
	data, err := os.ReadFile(path)
	if err != nil {
		return "", Record{}, false, err
	}
	var record Record
	if err := json.Unmarshal(data, &record); err != nil {
		q.logger.Error("dropping corrupt spool record", "path", path, "error", err)
		_ = os.Remove(path)
		q.dropped.Add(1)
		return q.peekUnlockedRetry()
	}
	return path, record, true, nil
}

func (q *Queue) peekUnlockedRetry() (string, Record, bool, error) {
	files, _, err := q.listLocked()
	if err != nil || len(files) == 0 {
		return "", Record{}, false, err
	}
	path := filepath.Join(q.dir, files[0].Name())
	data, err := os.ReadFile(path)
	if err != nil {
		return "", Record{}, false, err
	}
	var record Record
	if err := json.Unmarshal(data, &record); err != nil {
		_ = os.Remove(path)
		q.dropped.Add(1)
		return q.peekUnlockedRetry()
	}
	return path, record, true, nil
}

func (q *Queue) pruneLocked() error {
	files, total, err := q.listLocked()
	if err != nil {
		return err
	}
	if len(files) <= q.maxMessages && total <= q.maxBytes {
		return nil
	}

	// First discard obsolete retained snapshots, preserving the newest file per topic.
	latestByTopic := map[string]string{}
	recordByName := map[string]Record{}
	for _, file := range files {
		path := filepath.Join(q.dir, file.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var record Record
		if json.Unmarshal(data, &record) != nil {
			continue
		}
		recordByName[file.Name()] = record
		if record.Retain || record.Class == "snapshot" {
			latestByTopic[record.Topic] = file.Name()
		}
	}
	for _, file := range files {
		record, ok := recordByName[file.Name()]
		if !ok || (!record.Retain && record.Class != "snapshot") {
			continue
		}
		if latestByTopic[record.Topic] != file.Name() {
			if os.Remove(filepath.Join(q.dir, file.Name())) == nil {
				q.dropped.Add(1)
			}
		}
	}

	files, total, err = q.listLocked()
	if err != nil {
		return err
	}
	for (len(files) > q.maxMessages || total > q.maxBytes) && len(files) > 0 {
		file := files[0]
		if err := os.Remove(filepath.Join(q.dir, file.Name())); err != nil {
			return err
		}
		if info, err := file.Info(); err == nil {
			total -= info.Size()
		}
		files = files[1:]
		q.dropped.Add(1)
	}
	return nil
}

func (q *Queue) listLocked() ([]os.DirEntry, int64, error) {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return nil, 0, err
	}
	files := make([]os.DirEntry, 0, len(entries))
	var total int64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files = append(files, entry)
		total += info.Size()
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })
	return files, total, nil
}

func newID() string {
	var random [6]byte
	_, _ = rand.Read(random[:])
	return fmt.Sprintf("%020d-%s", time.Now().UTC().UnixNano(), hex.EncodeToString(random[:]))
}
