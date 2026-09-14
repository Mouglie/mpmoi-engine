// Command mpmoi-spike is the Stage 0 make-or-break spike for the pure-Go engine
// pivot (docs/bridgev2-local-connector-plan.md).
//
// It boots the real mautrix-whatsapp bridge (bridgev2) but replaces the Synapse
// homeserver connector (bridgev2/matrix) with our own LocalConnector, which
// writes bridge events straight into the mpmoi product Postgres. Success = a
// real WhatsApp message from the phone becomes a public.messages row +
// public.conversations row, with NO Synapse anywhere.
//
// Env:
//
//	MPMOI_PRODUCT_DSN  Postgres DSN for the product DB (conversations/messages).
//	MPMOI_BRIDGE_DB    path to a scratch sqlite file for the bridge's own state
//	                   (default: $TMPDIR/mpmoi-spike-bridge.db).
//	MPMOI_USER_ID      auth.users uuid the rows belong to.
//	MPMOI_SPACE_ID     spaces uuid.
//	MPMOI_ACCOUNT_ID   connected_accounts uuid.
//	MPMOI_NETWORK      network enum value (default "whatsapp").
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/mdp/qrterminal/v3"
	"github.com/rs/zerolog"
	"rsc.io/qr"

	_ "modernc.org/sqlite" // pure-Go SQLite driver (registers "sqlite") — no cgo, so the bridge-state
	// DB cross-compiles for Windows/mobile. The only remaining native link is libsignal.
	"go.mau.fi/util/dbutil"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

// engineConfig is the process-wide config shared by every network.
type engineConfig struct {
	productDSN string
	userID     string
	spaceID    string
	storageDir string // product media bucket root
	dataDir    string // base for per-network bridge state DBs
}

// netSpec is one network to run: its impl, its connected_accounts id, and where
// its bridge-state DB lives.
type netSpec struct {
	net       Network
	accountID string
	bridgeDB  string
}

func run() error {
	log := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()
	ctx := log.WithContext(context.Background())

	cfg := engineConfig{
		productDSN: mustEnv("MPMOI_PRODUCT_DSN"),
		userID:     mustEnv("MPMOI_USER_ID"),
		spaceID:    mustEnv("MPMOI_SPACE_ID"),
		dataDir:    os.Getenv("MPMOI_DATA_DIR"),
	}
	// Media bucket root: {DATA_DIR}/storage/telegram-media (the generic on-device
	// media bucket the web already reads), overridable via MPMOI_STORAGE_DIR.
	cfg.storageDir = os.Getenv("MPMOI_STORAGE_DIR")
	if cfg.storageDir == "" && cfg.dataDir != "" {
		cfg.storageDir = filepath.Join(cfg.dataDir, "storage", "telegram-media")
	}

	// Single-network / PAIRING mode: MPMOI_NETWORK selects one network and drives
	// an interactive login (QR or cookies). This is how a network is first paired.
	if network := os.Getenv("MPMOI_NETWORK"); network != "" {
		net, ok := networkByID(network)
		if !ok {
			return fmt.Errorf("unsupported MPMOI_NETWORK %q (have: whatsapp, signal, instagram, messenger)", network)
		}
		spec := netSpec{net: net, accountID: mustEnv("MPMOI_ACCOUNT_ID"), bridgeDB: bridgeDBPath(cfg, net)}
		br, err := runNetwork(ctx, log, cfg, spec, true)
		if err != nil {
			return err
		}
		waitForSignal(log)
		if br != nil {
			br.Stop()
		}
		return nil
	}

	// SUPERVISOR mode (no MPMOI_NETWORK): run every connected + already-paired
	// account concurrently in this one process, AND watch matrix_login_requests so
	// the in-app Connect UI can pair NEW networks live (login.go). Unpaired accounts
	// with no pending request are skipped until the user connects them.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	specs, err := discoverNetworks(cfg)
	if err != nil {
		return err
	}
	sup := newEngineSupervisor(cfg, log)
	for _, spec := range specs {
		br, err := runNetwork(ctx, log, cfg, spec, false)
		if err != nil {
			log.Error().Err(err).Str("net", spec.net.ID()).Msg("network failed to start — continuing")
			continue
		}
		if br != nil {
			sup.add(spec.net.ID(), br)
		}
	}
	// Always log the health marker (even with 0 networks — a fresh install idles here waiting for the
	// first Connect request; the desktop supervisor treats an exit as a crash, so we never exit early).
	log.Info().Int("running", sup.count()).Int("accounts", len(specs)).Msg("supervisor up — no Synapse in sight")

	// Watch the product DB for the app's Connect requests and pair them live.
	go runLoginPoller(ctx, cfg, sup, log)

	waitForSignal(log)
	cancel() // stop the poller + outbound goroutines
	sup.stopAll()
	return nil
}

// bridgeDBPath resolves where a network's bridge-state DB lives: MPMOI_BRIDGE_DB
// wins (single-network override), else the canonical {dataDir}/engine/{net}.db so
// supervisor mode and pairing mode share the same file.
func bridgeDBPath(cfg engineConfig, net Network) string {
	if p := os.Getenv("MPMOI_BRIDGE_DB"); p != "" {
		return p
	}
	return filepath.Join(cfg.dataDir, "engine", net.ID()+".db")
}

// discoverNetworks reads the user's active connected_accounts and maps the
// supported ones to netSpecs (each with its canonical bridge-DB path).
func discoverNetworks(cfg engineConfig) ([]netSpec, error) {
	db, err := sql.Open("postgres", cfg.productDSN)
	if err != nil {
		return nil, fmt.Errorf("open product db: %w", err)
	}
	defer db.Close()
	rows, err := db.Query(
		`select network::text, id from public.connected_accounts
		   where user_id = $1 and status = 'active' order by network`,
		cfg.userID,
	)
	if err != nil {
		return nil, fmt.Errorf("query connected_accounts: %w", err)
	}
	defer rows.Close()
	var specs []netSpec
	for rows.Next() {
		var network, accountID string
		if err := rows.Scan(&network, &accountID); err != nil {
			return nil, err
		}
		net, ok := networkByID(network)
		if !ok {
			continue // telegram/slack/teams etc. are served elsewhere, not by the engine
		}
		specs = append(specs, netSpec{net: net, accountID: accountID, bridgeDB: filepath.Join(cfg.dataDir, "engine", net.ID()+".db")})
	}
	return specs, rows.Err()
}

// builtBridge is a wired, started bridge for one network, before a login is
// attached. buildBridge produces it; runNetwork (reuse) and pairNetwork (login.go,
// interactive Connect) both drive a login and then call activate.
type builtBridge struct {
	br         *bridgev2.Bridge
	store      *productStore
	matrixConn *LocalConnector
	netConn    bridgev2.NetworkConnector
	spec       netSpec
	nlog       zerolog.Logger
}

// buildBridge wires and starts one network's bridge: product store, bridge-state
// DB, connectors, and bridgev2.NewBridge().Start — everything EXCEPT the login.
func buildBridge(ctx context.Context, log zerolog.Logger, cfg engineConfig, spec netSpec) (*builtBridge, error) {
	nlog := log.With().Str("net", spec.net.ID()).Logger()

	store, err := newProductStore(cfg.productDSN, cfg.userID, cfg.spaceID, spec.accountID, spec.net.ID(), cfg.storageDir)
	if err != nil {
		return nil, err
	}
	// Key this network's conversations to the ACCOUNT's Space, not the engine's ONE
	// configured (personal) space. A network connected into a 2nd Space (e.g. "reseaux")
	// has its account row in that space; without this, every conversation is written with
	// cfg.spaceID and surfaces under the personal Space regardless of the account. This
	// single override covers all three entry paths (discovery, single-network env, pairing).
	if sid, serr := store.accountSpaceID(spec.accountID); serr != nil {
		nlog.Warn().Err(serr).Msg("resolve account space failed — using default space")
	} else if sid != "" {
		store.spaceID = sid
	}
	// Network-specific 1:1-peer resolver, so isSelfChat works for any network.
	store.peerOf = spec.net.PeerGhostID

	// The bridge's OWN state DB (portal/message/ghost/... + network session).
	if err := os.MkdirAll(filepath.Dir(spec.bridgeDB), 0o755); err != nil {
		return nil, fmt.Errorf("make bridge db dir: %w", err)
	}
	// Pure-Go modernc driver ("sqlite"). Pragmas go through modernc's ?_pragma= syntax (not mattn's
	// ?_foreign_keys=): foreign keys on, WAL for reader/writer concurrency, a busy timeout so a brief
	// lock retries instead of erroring, and _txlock=immediate (mautrix's SQLite recommendation, so a
	// write txn takes the lock up front rather than deadlocking on upgrade).
	dsn := "file:" + spec.bridgeDB +
		"?_txlock=immediate&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	db, err := dbutil.NewWithDialect(dsn, "sqlite")
	if err != nil {
		return nil, fmt.Errorf("open bridge state db: %w", err)
	}
	// Single connection: SQLite allows only one writer, and serialising here avoids "database is
	// locked" churn (mautrix runs its SQLite bridges the same way).
	db.RawDB.SetMaxOpenConns(1)
	db.Log = dbutil.ZeroLogger(nlog.With().Str("db_section", "bridge").Logger())

	netConn, err := spec.net.NewConnector()
	if err != nil {
		return nil, err
	}
	matrixConn := NewLocalConnector(store, spec.net)
	// Grant the local user permission to send events (needed for outbound).
	userPerms := bridgeconfig.PermissionLevelUser
	bcfg := &bridgeconfig.BridgeConfig{
		CommandPrefix: spec.net.CommandPrefix(),
		Permissions:   bridgeconfig.PermissionConfig{"*": &userPerms},
		// Backfill existing chats on first login. WhatsApp/Signal deliver their history sync through
		// the bridgev2 backfill QUEUE, which runs only when BOTH Backfill.Enabled and Queue.Enabled
		// are set — without this a fresh pair produces an EMPTY inbox (only new live messages appear).
		// Our LocalConnector reports no batch-send, so the queue replays each historical message as a
		// SendMessage → our sink writes it to the product DB (same path as live messages).
		Backfill: bridgeconfig.BackfillConfig{
			Enabled:            true,
			MaxInitialMessages: 50,
			MaxCatchupMessages: 500,
			Queue: bridgeconfig.BackfillQueueConfig{
				Enabled:    true,
				BatchSize:  100,
				BatchDelay: 1,
				MaxBatches: -1,
			},
		},
	}
	br := bridgev2.NewBridge("", db, nlog, bcfg, matrixConn, netConn, commands.NewProcessor)
	if err := br.Start(ctx); err != nil {
		return nil, fmt.Errorf("start bridge: %w", err)
	}
	return &builtBridge{br: br, store: store, matrixConn: matrixConn, netConn: netConn, spec: spec, nlog: nlog}, nil
}

// activate binds the login's identity, reconciles titles, and starts the outbound
// poller — the shared tail of both the reuse path (runNetwork) and the interactive
// Connect path (pairNetwork).
func (b *builtBridge) activate(ctx context.Context, login *bridgev2.UserLogin) {
	bindIdentity(b.spec.net, b.store, b.matrixConn, login, b.nlog)
	reconcileTitles(ctx, b.matrixConn, b.store, b.nlog)
	go runOutbound(ctx, b.br, b.matrixConn, b.store, string(login.ID), id.UserID("@mpmoi:"+localServerName), b.nlog)
	b.nlog.Info().Str("bridge_db", b.spec.bridgeDB).Str("login", string(login.ID)).Msg("network up")
}

// runNetwork wires and starts one network end-to-end, reusing an existing login
// (interactive=false, supervisor mode). It returns the started bridge (for
// shutdown), or nil if the network isn't paired yet (skipped).
func runNetwork(ctx context.Context, log zerolog.Logger, cfg engineConfig, spec netSpec, interactive bool) (*bridgev2.Bridge, error) {
	bb, err := buildBridge(ctx, log, cfg, spec)
	if err != nil {
		return nil, err
	}
	// Reuse an existing login; in interactive (pairing) mode, drive a new one.
	login, err := ensureLogin(ctx, bb.br, spec.net, bb.netConn, bb.nlog, interactive)
	if err != nil {
		bb.br.Stop()
		return nil, err
	}
	if login == nil {
		bb.nlog.Warn().Msgf("not paired — skipping (connect it from the app, or MPMOI_NETWORK=%s)", spec.net.ID())
		bb.br.Stop()
		return nil, nil
	}
	bb.activate(ctx, login)
	return bb.br, nil
}

// engineSupervisor tracks the bridges running in this process (one per network),
// so the login poller can tell what's already connected and shutdown can stop
// everything. Guarded by a mutex — the poller adds bridges from its own goroutine.
type engineSupervisor struct {
	cfg     engineConfig
	log     zerolog.Logger
	mu      sync.Mutex
	bridges map[string]*bridgev2.Bridge // key = network id
}

func newEngineSupervisor(cfg engineConfig, log zerolog.Logger) *engineSupervisor {
	return &engineSupervisor{cfg: cfg, log: log, bridges: map[string]*bridgev2.Bridge{}}
}

func (s *engineSupervisor) add(netID string, br *bridgev2.Bridge) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bridges[netID] = br
}

// running reports whether a bridge for this network is already up in-process (so
// the poller can short-circuit a duplicate Connect request instead of opening a
// second bridge on the same sqlite state DB).
func (s *engineSupervisor) running(netID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.bridges[netID]
	return ok
}

func (s *engineSupervisor) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.bridges)
}

// remove takes a network's bridge out of the supervisor and returns it (nil if not
// present), so the caller can log it out + stop it — used when an account is
// disconnected (or its Space deleted) and the bridge must be torn down.
func (s *engineSupervisor) remove(netID string) *bridgev2.Bridge {
	s.mu.Lock()
	defer s.mu.Unlock()
	br := s.bridges[netID]
	delete(s.bridges, netID)
	return br
}

// runningNetworks returns the network ids with a bridge currently up in-process.
func (s *engineSupervisor) runningNetworks() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	nets := make([]string, 0, len(s.bridges))
	for netID := range s.bridges {
		nets = append(nets, netID)
	}
	return nets
}

func (s *engineSupervisor) stopAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, br := range s.bridges {
		br.Stop()
	}
}

func waitForSignal(log zerolog.Logger) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info().Msg("shutting down")
}

// ensureLogin returns the existing login if the bridge already has one. When
// interactive, it drives a headless login (QR for WhatsApp/Signal, cookies for
// Meta) if there's none. When NOT interactive (supervisor mode), an unpaired
// network returns (nil, nil) so the caller skips it rather than blocking.
func ensureLogin(ctx context.Context, br *bridgev2.Bridge, net Network, netConn bridgev2.NetworkConnector, log zerolog.Logger, interactive bool) (*bridgev2.UserLogin, error) {
	user, err := br.GetUserByMXID(ctx, id.UserID("@mpmoi:"+localServerName))
	if err != nil {
		return nil, fmt.Errorf("get user: %w", err)
	}
	if logins := user.GetUserLogins(); len(logins) > 0 {
		log.Info().Str("login", string(logins[0].ID)).Msg("reusing existing login")
		return logins[0], nil
	}
	if !interactive {
		return nil, nil // supervisor mode: skip unpaired networks
	}

	// QR networks use the connector's first advertised flow; Meta names a
	// specific cookie flow (its Instagram flow isn't advertised).
	flowID := net.LoginFlowID()
	if flowID == "" {
		flows := netConn.GetLoginFlows()
		if len(flows) == 0 {
			return nil, fmt.Errorf("connector exposes no login flows")
		}
		flowID = flows[0].ID
	}
	lp, err := netConn.CreateLogin(ctx, user, flowID)
	if err != nil {
		return nil, fmt.Errorf("create login: %w", err)
	}
	step, err := lp.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("login start: %w", err)
	}
	waiter, _ := lp.(bridgev2.LoginProcessDisplayAndWait)
	for {
		switch step.Type {
		case bridgev2.LoginStepTypeDisplayAndWait:
			if step.DisplayAndWaitParams != nil && step.DisplayAndWaitParams.Data != "" {
				fmt.Fprintln(os.Stderr, "\nScan this QR with the app → Linked devices:")
				qrterminal.GenerateHalfBlock(step.DisplayAndWaitParams.Data, qrterminal.L, os.Stderr)
				if path := os.Getenv("MPMOI_QR_PNG"); path != "" {
					if code, err := qr.Encode(step.DisplayAndWaitParams.Data, qr.M); err == nil {
						if err := os.WriteFile(path, code.PNG(), 0o644); err == nil {
							log.Info().Str("png", path).Msg("QR_PNG_WRITTEN")
						}
					}
				}
			}
			if waiter == nil {
				return nil, fmt.Errorf("login needs display_and_wait but process doesn't implement Wait")
			}
			step, err = waiter.Wait(ctx)
			if err != nil {
				return nil, fmt.Errorf("login wait: %w", err)
			}
		case bridgev2.LoginStepTypeCookies:
			// Meta cookie login: feed cookies harvested from the user's authed
			// FB/IG browser session via MPMOI_META_COOKIES (a JSON object of
			// cookie name→value).
			raw := os.Getenv("MPMOI_META_COOKIES")
			if raw == "" {
				return nil, fmt.Errorf("%s login needs cookies — set MPMOI_META_COOKIES to a Copy-as-cURL command, a JSON object, or a name=value cookie string", net.ID())
			}
			cookies, err := parseMetaCookies(raw)
			if err != nil {
				return nil, fmt.Errorf("MPMOI_META_COOKIES: %w", err)
			}
			cl, ok := lp.(bridgev2.LoginProcessCookies)
			if !ok {
				return nil, fmt.Errorf("login is at a cookies step but the process doesn't implement SubmitCookies")
			}
			log.Info().Int("cookies", len(cookies)).Str("network", net.ID()).Msg("submitting harvested cookies")
			step, err = cl.SubmitCookies(ctx, cookies)
			if err != nil {
				return nil, fmt.Errorf("submit cookies: %w", err)
			}
		case bridgev2.LoginStepTypeComplete:
			if step.CompleteParams == nil {
				return nil, fmt.Errorf("login complete step missing params")
			}
			log.Info().Str("login", string(step.CompleteParams.UserLoginID)).Msg("login complete")
			if step.CompleteParams.UserLogin != nil {
				return step.CompleteParams.UserLogin, nil
			}
			// Fall back to re-fetching the login by its id.
			if logins := user.GetUserLogins(); len(logins) > 0 {
				return logins[0], nil
			}
			return nil, fmt.Errorf("login complete but no UserLogin available")
		default:
			return nil, fmt.Errorf("unexpected login step type %q (engine only handles QR)", step.Type)
		}
	}
}

// bindIdentity applies the login's own network ids (so self-sent messages read
// as outgoing) and, if the network exposes one, wires a contact-name resolver.
func bindIdentity(net Network, store *productStore, conn *LocalConnector, login *bridgev2.UserLogin, log zerolog.Logger) {
	ident := net.Bind(login)
	store.setSelfIDs(ident.SelfIDs...)
	store.setSelfChatID(ident.SelfChatID)
	conn.setLoginReceiver(login.ID)
	log.Info().Interface("self_ids", ident.SelfIDs).Str("network", net.ID()).Msg("self identities")
	if ident.ContactName != nil {
		conn.setContactResolver(ident.ContactName)
	}
}

// reconcileTitles re-derives every conversation title from the (now wired)
// contact store and updates any that changed. One-shot at startup.
func reconcileTitles(ctx context.Context, conn *LocalConnector, store *productStore, log zerolog.Logger) {
	convos, err := store.listConversations()
	if err != nil {
		log.Warn().Err(err).Msg("reconcile: list conversations failed")
		return
	}
	updated := 0
	for _, cv := range convos {
		if conn.net.SkipChat(cv.RemoteChatID) {
			continue
		}
		// Only touch real chats for THIS network. Conversations from the old
		// Synapse build are keyed by Matrix room ids ("!room:server") — never
		// resolve or overwrite those.
		if !conn.net.IsRoutable(cv.RemoteChatID) {
			continue
		}
		title := conn.resolveChatTitle(ctx, cv.RemoteChatID, networkid.PortalKey{ID: networkid.PortalID(cv.RemoteChatID)})
		// Never write a title that's just the chat id back onto the row.
		if title != "" && title != cv.Title && title != cv.RemoteChatID {
			if err := store.updateConversationTitle(cv.ID, title); err == nil {
				updated++
			}
		}
	}
	log.Info().Int("updated", updated).Int("scanned", len(convos)).Msg("reconciled conversation titles from contacts")
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		fmt.Fprintln(os.Stderr, "missing required env:", k)
		os.Exit(2)
	}
	return v
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
