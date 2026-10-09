package watcher

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/asheshgoplani/agent-deck/internal/logging"
	"github.com/asheshgoplani/agent-deck/internal/session"
	"github.com/asheshgoplani/agent-deck/internal/statedb"
)

// DefaultEngineRetryInterval is how often a process that does not own its
// profile's watcher engine tries to take it over.
const DefaultEngineRetryInterval = 5 * time.Second

// relayStopWait bounds how long Stop waits for the relay to hand on what the
// stopped engine had buffered.
const relayStopWait = 5 * time.Second

// HostConfig configures an EngineHost.
type HostConfig struct {
	// Profile is the profile whose engine owner lock the host takes
	// (EngineLockPath).
	Profile string

	// DB is the profile's state database: the watchers to run and where their
	// events are stored.
	DB *statedb.StateDB

	// DeliverEvent and DeliverHealth receive every routed event and health
	// state of an engine this host runs, on the relay's goroutines (see
	// relayEngine). Either may be nil.
	DeliverEvent  func(Event)
	DeliverHealth func(HealthState)

	// RetryInterval is how often a standby host tries to take the engine
	// over. Defaults to DefaultEngineRetryInterval.
	RetryInterval time.Duration

	// Logger defaults to logging.ForComponent(logging.CompWatcher).
	Logger *slog.Logger
}

// EngineHost runs a profile's watcher engine in whichever long-lived
// agent-deck process owns it (#2530). Every TUI, `web` and `web --no-tui`
// process has one. The host that takes the engine owner lock starts the
// engine and the relay that hands its output to the delivery callbacks; a
// host that loses stays on standby and retries on a ticker, so when the owner
// exits another process takes over without any coordination channel. There is
// at most one engine per profile, and the host has no Bubble Tea types: the
// TUI and the headless server both run it.
//
// Lifecycle: NewEngineHost -> Start -> Stop. The panel channels stay the same
// for the host's whole life, so a TUI listens on them once, and gets events
// after a takeover too.
type EngineHost struct {
	cfg HostConfig
	log *slog.Logger

	panelEvents chan Event
	panelHealth chan HealthState

	stop     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once

	mu        sync.Mutex
	stopped   bool
	owner     *EngineOwner
	engine    *Engine
	relayDone <-chan struct{}
	// standbyPID and lockErr are the last waiting reason logged, so a standby
	// host logs once per change instead of on every retry.
	standbyPID int
	lockErr    string
}

// NewEngineHost returns a host that has not joined the election yet.
func NewEngineHost(cfg HostConfig) *EngineHost {
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = DefaultEngineRetryInterval
	}
	log := cfg.Logger
	if log == nil {
		log = logging.ForComponent(logging.CompWatcher)
	}
	return &EngineHost{
		cfg:         cfg,
		log:         log,
		panelEvents: make(chan Event, 1),
		panelHealth: make(chan HealthState, 1),
		stop:        make(chan struct{}),
	}
}

// Start tries to take the engine owner lock and, if it wins, starts the
// engine before returning. If another process owns the engine, the host
// waits on standby and retries every RetryInterval until it wins or Stop is
// called. Call it once.
func (h *EngineHost) Start() {
	if h.tryOwn(false) {
		return
	}
	h.wg.Add(1)
	go h.standby()
}

func (h *EngineHost) standby() {
	defer h.wg.Done()
	ticker := time.NewTicker(h.cfg.RetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-ticker.C:
			if h.tryOwn(true) {
				return
			}
		}
	}
}

// tryOwn takes the lock and starts the engine. It reports whether the host
// is done waiting: it owns the engine now, or it was stopped.
func (h *EngineHost) tryOwn(takeover bool) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped {
		return true
	}
	path, err := EngineLockPath(h.cfg.Profile)
	var owner *EngineOwner
	if err == nil {
		owner, err = AcquireEngineOwner(path)
	}
	if err != nil {
		var running *AlreadyRunningError
		if errors.As(err, &running) {
			if running.PID != h.standbyPID || h.lockErr != "" {
				h.log.Info("watcher_engine_standby",
					slog.String("profile", h.cfg.Profile),
					slog.Int("owner_pid", running.PID))
			}
			h.standbyPID, h.lockErr = running.PID, ""
		} else if err.Error() != h.lockErr {
			h.log.Warn("watcher_engine_lock_failed",
				slog.String("profile", h.cfg.Profile),
				slog.String("error", err.Error()))
			h.lockErr = err.Error()
		}
		return false
	}

	h.owner = owner
	outcome := "watcher_engine_owner"
	if takeover {
		outcome = "watcher_engine_took_over"
	}
	h.log.Info(outcome,
		slog.String("profile", h.cfg.Profile),
		slog.Int("pid", os.Getpid()),
		slog.String("lock", path))

	eng := startEngine(h.cfg.DB, h.log)
	if eng == nil {
		// Nothing to run: no relay will close the panel channels.
		close(h.panelEvents)
		close(h.panelHealth)
		return true
	}
	h.engine = eng
	h.relayDone = relayEngine(eng.EventCh(), eng.HealthCh(), h.cfg.DeliverEvent, h.cfg.DeliverHealth,
		h.panelEvents, h.panelHealth, h.log)
	return true
}

// Stop leaves the election. An owner stops its engine, waits up to 5 s for
// the relay to hand on what the engine had buffered, runs drain (when not
// nil) and only then releases the lock: a successor never binds a port this
// engine still holds, or delivers next to a delivery this process still has
// in flight. Safe to call more than once and from several goroutines; later
// calls wait for the first.
func (h *EngineHost) Stop(drain func()) {
	h.stopOnce.Do(func() {
		h.mu.Lock()
		h.stopped = true
		h.mu.Unlock()
		close(h.stop)
		h.wg.Wait()

		h.mu.Lock()
		owner, eng, relayDone := h.owner, h.engine, h.relayDone
		h.mu.Unlock()
		if eng != nil {
			eng.Stop()
			select {
			case <-relayDone:
			case <-time.After(relayStopWait):
				h.log.Warn("watcher_relay_stop_timeout")
			}
		}
		if drain != nil {
			drain()
		}
		if owner == nil {
			// Never owned: no relay will close the panel channels.
			close(h.panelEvents)
			close(h.panelHealth)
			return
		}
		if err := owner.Close(); err != nil {
			h.log.Warn("watcher_engine_release_failed", slog.String("error", err.Error()))
		}
		h.log.Info("watcher_engine_released", slog.String("profile", h.cfg.Profile))
	})
}

// IsOwner reports whether this host holds the engine owner lock.
func (h *EngineHost) IsOwner() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.owner != nil && !h.stopped
}

// Engine returns the running engine, or nil while the host is on standby,
// when the profile had no watchers when it took over, and after Stop.
func (h *EngineHost) Engine() *Engine {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped {
		return nil
	}
	return h.engine
}

// PanelEvents and PanelHealth carry what the relay delivered, for a TUI's
// watcher panel. They hold at most one pending item and close when the host
// stops (or right away if it took over with no watchers to run).
func (h *EngineHost) PanelEvents() <-chan Event       { return h.panelEvents }
func (h *EngineHost) PanelHealth() <-chan HealthState { return h.panelHealth }

// startEngine builds the watcher engine from the state database and starts
// it: every watcher marked running gets its adapter. It returns nil when db
// is nil or holds no watchers.
func startEngine(db *statedb.StateDB, log *slog.Logger) *Engine {
	if db == nil {
		return nil
	}
	rows, err := db.LoadWatchers()
	if err != nil || len(rows) == 0 {
		return nil
	}

	// Load user config for watcher settings.
	cfg, _ := session.LoadUserConfig()
	var watcherCfg session.WatcherSettings
	if cfg != nil {
		watcherCfg = cfg.Watcher
	}

	// Build engine config.
	router, _ := LoadFromWatcherDir() // nil on error (no clients.json yet)
	healthInterval := time.Duration(watcherCfg.GetHealthCheckIntervalSeconds()) * time.Second

	engineCfg := EngineConfig{
		DB:                  db,
		Router:              router,
		MaxEventsPerWatcher: watcherCfg.GetMaxEventsPerWatcher(),
		HealthCheckInterval: healthInterval,
	}
	eng := NewEngine(engineCfg)

	maxSilenceMinutes := watcherCfg.GetMaxSilenceMinutes()

	// Register all running watchers as adapters.
	for _, row := range rows {
		if row.Status != "running" {
			continue
		}
		var adapter WatcherAdapter
		switch row.Type {
		case "webhook":
			adapter = &WebhookAdapter{}
		case "ntfy":
			adapter = &NtfyAdapter{}
		case "slack":
			adapter = &SlackAdapter{}
		case "github":
			adapter = &GitHubAdapter{}
		default:
			continue
		}

		adapterCfg := AdapterConfig{
			Type:     row.Type,
			Name:     row.Name,
			Settings: loadSourceSettings(row.Name),
		}
		eng.RegisterAdapter(row.ID, adapter, adapterCfg, maxSilenceMinutes)
	}

	if err := eng.Start(); err != nil {
		log.Warn("watcher_engine_start_failed", "error", err.Error())
		return nil
	}

	log.Info("watcher_engine_started",
		slog.Int("watcher_count", len(rows)),
		slog.Int("running_count", runningCount(rows)))
	return eng
}

// runningCount returns how many watcher rows are in the "running" state.
func runningCount(rows []*statedb.WatcherRow) int {
	n := 0
	for _, r := range rows {
		if r != nil && r.Status == "running" {
			n++
		}
	}
	return n
}

// loadSourceSettings reads the [source] table from
// ~/.agent-deck/watcher/<name>/watcher.toml into a map[string]string suitable for
// AdapterConfig.Settings. Returns an empty (non-nil) map on any error so the engine
// falls back to per-adapter defaults instead of failing to register.
func loadSourceSettings(name string) map[string]string {
	out := map[string]string{}
	dir, err := session.WatcherNameDir(name)
	if err != nil {
		return out
	}
	path := filepath.Join(dir, "watcher.toml")
	var cfg struct {
		Source map[string]string `toml:"source"`
	}
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return out
	}
	for k, v := range cfg.Source {
		out[k] = v
	}
	return out
}
