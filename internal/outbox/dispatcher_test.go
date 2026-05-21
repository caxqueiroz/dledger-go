package outbox

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	mu        sync.Mutex
	pending   []Event
	published map[string]bool
}

func (f *fakeStore) PendingOutbox(ctx context.Context, limit int) ([]Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := limit
	if n > len(f.pending) {
		n = len(f.pending)
	}
	out := make([]Event, n)
	copy(out, f.pending[:n])
	return out, nil
}

func (f *fakeStore) MarkOutboxPublished(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published[id] = true
	kept := f.pending[:0]
	for _, e := range f.pending {
		if e.ID != id {
			kept = append(kept, e)
		}
	}
	f.pending = kept
	return nil
}

func (f *fakeStore) IncrementOutboxAttempts(ctx context.Context, id string) error { return nil }

type fakeSink struct {
	mu     sync.Mutex
	calls  []Event
	failID string
}

func (s *fakeSink) Publish(ctx context.Context, e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, e)
	if e.ID == s.failID {
		return context.DeadlineExceeded
	}
	return nil
}

func TestDispatcher_PublishesPending(t *testing.T) {
	store := &fakeStore{
		pending:   []Event{{ID: "a"}, {ID: "b"}},
		published: map[string]bool{},
	}
	sink := &fakeSink{}
	d := NewDispatcher(store, sink, Config{Interval: 5 * time.Millisecond, BatchSize: 10})
	ctx, cancel := context.WithCancel(context.Background())
	go d.Run(ctx)
	defer cancel()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		store.mu.Lock()
		ok := store.published["a"] && store.published["b"]
		store.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("not all published; got %v", store.published)
}

func TestDispatcher_FailedSinkLeavesPending(t *testing.T) {
	store := &fakeStore{
		pending:   []Event{{ID: "ok"}, {ID: "bad"}},
		published: map[string]bool{},
	}
	sink := &fakeSink{failID: "bad"}
	d := NewDispatcher(store, sink, Config{Interval: 5 * time.Millisecond, BatchSize: 10})
	ctx, cancel := context.WithCancel(context.Background())
	go d.Run(ctx)
	defer cancel()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		store.mu.Lock()
		okPublished := store.published["ok"]
		badPublished := store.published["bad"]
		store.mu.Unlock()
		if okPublished && !badPublished {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected ok published and bad NOT published; got %v", store.published)
}
