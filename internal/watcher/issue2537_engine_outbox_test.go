package watcher

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/sendqueue"
	"github.com/asheshgoplani/agent-deck/internal/statedb"
)

// The engine's side of durable watcher delivery (#2537): a routed event is
// queued for its conductor when the engine routes it, before it is stored.

// recordingOutbox is an Outbox that remembers what it was given and whether
// the event was already stored at that moment.
type recordingOutbox struct {
	db      *statedb.StateDB
	mu      sync.Mutex
	events  []Event
	stored  []bool
	pending map[string][]PendingDelivery
}

func (o *recordingOutbox) Enqueue(watcherID, _ string, evt Event) error {
	stored, err := o.db.HasWatcherEvent(watcherID, evt.DedupKey())
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, evt)
	o.stored = append(o.stored, stored || err != nil)
	return nil
}

func (o *recordingOutbox) Pending() (map[string][]PendingDelivery, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.pending, nil
}

func (o *recordingOutbox) got() ([]Event, []bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]Event(nil), o.events...), append([]bool(nil), o.stored...)
}

func newOutboxTestEngine(t *testing.T, db *statedb.StateDB, outbox Outbox, health time.Duration) *Engine {
	t.Helper()
	return NewEngine(EngineConfig{
		DB:                  db,
		Router:              NewRouter(map[string]ClientEntry{"alice@example.com": {Conductor: "demo", Name: "Alice"}}),
		MaxEventsPerWatcher: 500,
		HealthCheckInterval: health,
		TriageSpawner:       &fakeSpawner{},
		TriageDir:           t.TempDir(),
		ClientsPath:         filepath.Join(t.TempDir(), "clients.json"),
		Outbox:              outbox,
	})
}

// runEvents runs one mock watcher "w1" emitting events through the engine
// and stops it once the writer had time to handle them.
func runEvents(t *testing.T, engine *Engine, events ...Event) {
	t.Helper()
	engine.RegisterAdapter("w1", &MockAdapter{events: events, listenDelay: 10 * time.Millisecond},
		AdapterConfig{Type: "mock", Name: "inbox"}, 60)
	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(time.Duration(len(events))*10*time.Millisecond + 300*time.Millisecond)
	engine.Stop()
}

// TestIssue2537_EngineQueuesARoutedEventBeforeStoringIt: an event routed to
// a conductor reaches the outbox once, routed, and before its watcher_events
// row exists, so a crash between the two cannot leave a stored event nothing
// delivers. A replay of a stored event, an event for triage and an event no
// rule routes are not queued.
func TestIssue2537_EngineQueuesARoutedEventBeforeStoringIt(t *testing.T) {
	db := newTestDB(t)
	saveTestWatcher(t, db, "w1", "inbox", "mock")
	outbox := &recordingOutbox{db: db}
	engine := newOutboxTestEngine(t, db, outbox, 0)
	routed := Event{Source: "mock", Sender: "alice@example.com", Subject: "hi", Body: "hello", Timestamp: time.Now()}
	unrouted := Event{Source: "mock", Sender: "mallory@example.com", Subject: "spam", Timestamp: time.Now()}

	runEvents(t, engine, routed, routed, unrouted)

	events, stored := outbox.got()
	if len(events) != 1 {
		t.Fatalf("outbox got %d events, want the routed one once", len(events))
	}
	if events[0].RoutedTo != "demo" || events[0].Body != "hello" {
		t.Fatalf("queued %+v, want the event routed to demo", events[0])
	}
	if stored[0] {
		t.Fatal("the event was stored before it was queued")
	}
	if n := countWatcherEvents(t, db, "w1"); n != 2 {
		t.Fatalf("watcher_events holds %d rows, want 2 (routed + triage)", n)
	}
}

// TestIssue2537_TheSameEventRoutedTwiceIsOneRecord: the maintainer's test
// (c) through the engine. The second time, the event's row is gone (pruned
// past max_events_per_watcher), so the engine routes it again; the queue key
// keeps it to one record and one worker start.
func TestIssue2537_TheSameEventRoutedTwiceIsOneRecord(t *testing.T) {
	db := newTestDB(t)
	saveTestWatcher(t, db, "w1", "inbox", "mock")
	conductor := saveTestConductor(t, db, "demo")
	outbox, dir, spawned := newTestOutbox(t, db, 0)
	evt := Event{Source: "mock", Sender: "alice@example.com", Subject: "hi", Body: "routed twice", Timestamp: time.Now()}

	runEvents(t, newOutboxTestEngine(t, db, outbox, 0), evt)
	if _, err := db.DB().Exec(`DELETE FROM watcher_events WHERE watcher_id = ?`, "w1"); err != nil {
		t.Fatal(err)
	}
	runEvents(t, newOutboxTestEngine(t, db, outbox, 0), evt)

	if n := countWatcherEvents(t, db, "w1"); n != 1 {
		t.Fatalf("watcher_events holds %d rows after the second routing, want 1", n)
	}
	recs, err := sendqueue.List(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].SessionID != conductor {
		t.Fatalf("%d records (%+v), want one for the conductor", len(recs), recs)
	}
	if n := spawned.count(conductor); n != 1 {
		t.Fatalf("worker started %d times, want once", n)
	}
}

// TestIssue2537_HealthStateReportsUndeliveredEvents: each health state
// carries the watcher's routed events still waiting, per conductor, so the
// panel, the event bus and the health alerts (#2531) can see a stall.
func TestIssue2537_HealthStateReportsUndeliveredEvents(t *testing.T) {
	db := newTestDB(t)
	saveTestWatcher(t, db, "w1", "inbox", "mock")
	queued := time.Now().Add(-time.Hour)
	outbox := &recordingOutbox{db: db, pending: map[string][]PendingDelivery{
		"w1":    {{Conductor: "demo", Count: 3, Oldest: queued}},
		"other": {{Conductor: "ops", Count: 1, Oldest: queued}},
	}}
	engine := newOutboxTestEngine(t, db, outbox, 20*time.Millisecond)
	engine.RegisterAdapter("w1", &MockAdapter{}, AdapterConfig{Type: "mock", Name: "inbox"}, 60)
	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	select {
	case state := <-engine.HealthCh():
		if len(state.Undelivered) != 1 || state.Undelivered[0].Conductor != "demo" || state.Undelivered[0].Count != 3 || !state.Undelivered[0].Oldest.Equal(queued) {
			t.Fatalf("health state undelivered = %+v, want w1's 3 events for demo", state.Undelivered)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no health state")
	}
}

// flakyOutbox fails Enqueue with err for its first failures calls, then
// records like recordingOutbox; failures < 0 fails every call.
type flakyOutbox struct {
	recordingOutbox
	err      error
	failures int

	callMu sync.Mutex
	calls  int
}

func (o *flakyOutbox) Enqueue(watcherID, name string, evt Event) error {
	o.callMu.Lock()
	o.calls++
	fail := o.failures < 0 || o.calls <= o.failures
	o.callMu.Unlock()
	if fail {
		return o.err
	}
	return o.recordingOutbox.Enqueue(watcherID, name, evt)
}

func (o *flakyOutbox) callCount() int {
	o.callMu.Lock()
	defer o.callMu.Unlock()
	return o.calls
}

// TestIssue2537_AnEventTheOutboxFailsToQueueIsQueuedOnRetry: the outbox
// fails twice for a reason a retry can fix (here a locked database). The
// event is not stored meanwhile, since a stored event is never queued again;
// the writer retries, the event is queued once, unstored at that point, and
// then stored.
func TestIssue2537_AnEventTheOutboxFailsToQueueIsQueuedOnRetry(t *testing.T) {
	db := newTestDB(t)
	saveTestWatcher(t, db, "w1", "inbox", "mock")
	outbox := &flakyOutbox{recordingOutbox: recordingOutbox{db: db}, err: errors.New("database is locked"), failures: 2}
	engine := newOutboxTestEngine(t, db, outbox, 0)
	engine.cfg.QueueRetryInterval = 100 * time.Millisecond
	evt := Event{Source: "mock", Sender: "alice@example.com", Subject: "hi", Body: "retried", Timestamp: time.Now()}
	engine.RegisterAdapter("w1", &MockAdapter{events: []Event{evt}, listenDelay: 10 * time.Millisecond},
		AdapterConfig{Type: "mock", Name: "inbox"}, 60)
	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(engine.Stop)

	deadline := time.Now().Add(5 * time.Second)
	for outbox.callCount() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("the event never reached the outbox")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := countWatcherEvents(t, db, "w1"); n != 0 {
		t.Fatalf("the event was stored after the outbox failed to queue it (%d rows)", n)
	}
	for {
		if events, _ := outbox.got(); len(events) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the event was not queued after %d tries", outbox.callCount())
		}
		time.Sleep(10 * time.Millisecond)
	}
	for countWatcherEvents(t, db, "w1") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the event was not stored once it was queued")
		}
		time.Sleep(10 * time.Millisecond)
	}
	events, stored := outbox.got()
	if len(events) != 1 || stored[0] || outbox.callCount() != 3 {
		t.Fatalf("queued %d time(s) after %d tries, stored at that point %v; want once on the third try, unstored", len(events), outbox.callCount(), stored)
	}
}

// TestIssue2537_AnEventTheOutboxCannotQueueIsStillStored: an event for a
// conductor with no session is stored at once, without retries, since none
// would help. An event the outbox keeps failing for another reason is
// retried until the engine stops, and then stored, as every received event
// is.
func TestIssue2537_AnEventTheOutboxCannotQueueIsStillStored(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"no conductor session", fmt.Errorf("%w: conductor %q has no session", ErrNoConductorSession, "demo")},
		{"outbox keeps failing", errors.New("database is locked")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := newTestDB(t)
			saveTestWatcher(t, db, "w1", "inbox", "mock")
			outbox := &flakyOutbox{recordingOutbox: recordingOutbox{db: db}, err: c.err, failures: -1}
			engine := newOutboxTestEngine(t, db, outbox, 0)
			engine.cfg.QueueRetryInterval = 50 * time.Millisecond
			runEvents(t, engine, Event{Source: "mock", Sender: "alice@example.com", Subject: "hi", Body: c.name, Timestamp: time.Now()})

			if n := countWatcherEvents(t, db, "w1"); n != 1 {
				t.Fatalf("watcher_events holds %d rows after the engine stopped, want the event", n)
			}
			calls := outbox.callCount()
			if errors.Is(c.err, ErrNoConductorSession) && calls != 1 {
				t.Fatalf("the outbox was tried %d times for a conductor with no session, want once", calls)
			}
			if !errors.Is(c.err, ErrNoConductorSession) && calls < 3 {
				t.Fatalf("the outbox was tried %d times, want retries and a last try at stop", calls)
			}
		})
	}
}
