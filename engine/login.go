package main

// login.go — the in-app Connect flow for the pure-Go engine.
//
// The web Connect UI already writes a pending public.matrix_login_requests row
// (network + phone/credential) and watches it via realtime for pairing_code +
// status (see apps/web AppClient.startWhatsAppLogin/startSignalLogin/startMetaLogin
// + watchMatrixLogin). The OLD matrix-worker consumed those rows; the engine does
// it now, so the ENTIRE existing web UI works unchanged against the engine.
//
// A single poller (runLoginPoller) watches for pending requests and pairs each one
// live in this already-running supervisor process: it drives the network's login
// flow (WhatsApp phone-code, Signal QR, Meta cookies) and surfaces the code/QR into
// matrix_login_requests.pairing_code (status code_ready), then on completion creates
// the connected_account, keeps the new bridge running, and marks the request
// connected. No new IPC/QR channel — the DB row IS the channel.

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"rsc.io/qr"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/id"
)

// loginRequest is one pending row of public.matrix_login_requests.
type loginRequest struct {
	id         string
	network    string
	phone      string
	credential string
	space      string // target space_id from the Connect request (may be "" → fall back to cfg.spaceID)
}

// loginTimeout bounds one interactive pairing. Default 3 min; MPMOI_LOGIN_TIMEOUT
// (a Go duration like "20s") overrides it for tests.
func loginTimeout() time.Duration {
	if v := os.Getenv("MPMOI_LOGIN_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 3 * time.Minute
}

// runLoginPoller watches public.matrix_login_requests for the app's Connect
// requests and pairs each live in this process. One poller for the whole process;
// it runs until ctx is cancelled (shutdown).
func runLoginPoller(ctx context.Context, cfg engineConfig, sup *engineSupervisor, log zerolog.Logger) {
	plog := log.With().Str("component", "login_poller").Logger()
	db, err := sql.Open("postgres", cfg.productDSN)
	if err != nil {
		plog.Error().Err(err).Msg("open product db failed — the in-app Connect flow won't work")
		return
	}
	defer db.Close()
	ls := &loginStore{db: db, userID: cfg.userID, spaceID: cfg.spaceID}
	plog.Info().Msg("watching matrix_login_requests for in-app Connect requests")

	// One pairing per network at a time (each opens the network's single sqlite state
	// DB; a second concurrent open would conflict). Different networks pair in parallel.
	var mu sync.Mutex
	pairing := map[string]bool{}

	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		// Reconcile debris: a pending/code_ready request for a network that's ALREADY connected
		// (active account + running bridge) is marked connected against the existing account — no
		// re-pair. This clears a stale code_ready left by a crash/restart (its driver goroutine is
		// gone, so pending() would never re-touch it) and collapses a duplicate/racing request for a
		// live network instead of opening a second bridge on its single sqlite state DB.
		for _, netID := range sup.runningNetworks() {
			mu.Lock()
			busy := pairing[netID]
			mu.Unlock()
			if busy {
				continue // being (re)paired or already torn down this tick — leave it alone
			}
			accountID, err := ls.accountID(netID)
			if err != nil {
				continue
			}
			if accountID != "" {
				if n, err := ls.reconcileConnected(netID, accountID); err == nil && n > 0 {
					plog.Info().Str("net", netID).Int64("requests", n).Msg("reconciled lingering connect request(s) → connected")
				}
				continue
			}
			// A network still running in-process but with NO connected_account = the user disconnected
			// it (or deleted its Space). Tear the bridge down — log out (deletes the login + unlinks the
			// remote device) and stop it — so it stops syncing/notifying and a later reconnect starts
			// fresh. Guarded by the pairing map so it can't race a concurrent (re)pair of the same net.
			mu.Lock()
			pairing[netID] = true
			mu.Unlock()
			go func(netID string) {
				defer func() {
					mu.Lock()
					delete(pairing, netID)
					mu.Unlock()
				}()
				br := sup.remove(netID)
				if br == nil {
					return
				}
				nlog := plog.With().Str("net", netID).Logger()
				logoutBridge(ctx, br, nlog)
				br.Stop()
				nlog.Info().Msg("account gone — bridge logged out + stopped")
			}(netID)
		}
		reqs, err := ls.pending()
		if err != nil {
			plog.Debug().Err(err).Msg("poll pending login requests failed")
			continue
		}
		for _, r := range reqs {
			if _, ok := networkByID(r.network); !ok {
				continue // telegram/slack/teams — served by their own workers, not the engine
			}
			mu.Lock()
			busy := pairing[r.network]
			if !busy {
				pairing[r.network] = true
			}
			mu.Unlock()
			if busy {
				continue // this network is already being paired
			}
			go func(r loginRequest) {
				defer func() {
					mu.Lock()
					delete(pairing, r.network)
					mu.Unlock()
				}()
				handleLoginRequest(ctx, cfg, sup, ls, r, plog)
			}(r)
		}
	}
}

// handleLoginRequest pairs one network for one Connect request end-to-end.
func handleLoginRequest(ctx context.Context, cfg engineConfig, sup *engineSupervisor, ls *loginStore, r loginRequest, log zerolog.Logger) {
	net, _ := networkByID(r.network)
	rlog := log.With().Str("net", r.network).Str("request", r.id).Logger()
	rlog.Info().Msg("connect request — pairing")

	// Already connected in this process → idempotent success (the user re-clicked, or
	// discovery already started it at boot). Point the request at the existing account.
	if sup.running(r.network) {
		if accountID, _ := ls.accountID(r.network); accountID != "" {
			_ = ls.complete(r.id, accountID)
			rlog.Info().Msg("network already connected — request marked done")
			return
		}
	}

	// A connected_accounts row to hang the paired content on — reuse an existing row for
	// this (user, network) or create a fresh pending one.
	accountID, err := ls.ensureAccount(r.network, r.space)
	if err != nil {
		rlog.Error().Err(err).Msg("ensure connected_account failed")
		_ = ls.setError(r.id, "Couldn't prepare the account. Please try again.")
		return
	}

	spec := netSpec{net: net, accountID: accountID, bridgeDB: bridgeDBPath(cfg, net)}
	bb, err := buildBridge(ctx, log, cfg, spec)
	if err != nil {
		rlog.Error().Err(err).Msg("build bridge failed")
		_ = ls.setError(r.id, "Couldn't start the connector. Please try again.")
		_ = ls.failAccount(accountID)
		return
	}
	// Bound the interactive login: a QR/code the user never scans would otherwise block this
	// goroutine forever, and the per-network pairing guard with it — no retry for that network
	// would ever run. 3 min comfortably covers a real scan (WhatsApp/Signal rotate the code well
	// before then). MPMOI_LOGIN_TIMEOUT overrides it (tests use a few seconds).
	loginCtx, cancel := context.WithTimeout(ctx, loginTimeout())
	login, err := driveRequestLogin(loginCtx, bb, net, r, ls, rlog)
	cancel()
	if err != nil {
		msg := friendlyLoginError(err)
		if loginCtx.Err() == context.DeadlineExceeded {
			msg = "The code expired — please try again."
		}
		rlog.Error().Err(err).Msg("login failed")
		_ = ls.setError(r.id, msg)
		bb.br.Stop()
		_ = ls.failAccount(accountID)
		return
	}
	bb.activate(ctx, login)
	sup.add(r.network, bb.br)
	_ = ls.finishAccount(accountID, string(login.ID))
	_ = ls.complete(r.id, accountID)
	rlog.Info().Str("login", string(login.ID)).Str("account", accountID).Msg("connected")
}

// logoutBridge logs out every UserLogin on a bridge (deletes the local login AND unlinks the remote
// device), so the session is gone and a later reconnect starts fresh. Best-effort: errors are logged.
func logoutBridge(ctx context.Context, br *bridgev2.Bridge, log zerolog.Logger) {
	user, err := br.GetUserByMXID(ctx, id.UserID("@mpmoi:"+localServerName))
	if err != nil {
		log.Warn().Err(err).Msg("logout: get bridge user failed")
		return
	}
	for _, l := range user.GetUserLogins() {
		log.Info().Str("login", string(l.ID)).Msg("logging out bridge login")
		l.Logout(ctx)
	}
}

// driveRequestLogin runs the network's login flow for a Connect request, surfacing
// each display step (WhatsApp phone code, Signal QR) into the request's pairing_code,
// and returns the completed login. Cookie networks (Meta) submit r.credential.
func driveRequestLogin(ctx context.Context, bb *builtBridge, net Network, r loginRequest, ls *loginStore, log zerolog.Logger) (*bridgev2.UserLogin, error) {
	user, err := bb.br.GetUserByMXID(ctx, id.UserID("@mpmoi:"+localServerName))
	if err != nil {
		return nil, fmt.Errorf("get user: %w", err)
	}
	// The user explicitly clicked Connect. Any login still persisted in this bridge's state DB is
	// STALE — an active account would have short-circuited in handleLoginRequest before reaching here —
	// so a leftover login means a prior disconnect never reached the bridge (disconnect only deletes the
	// app's connected_account row, not the bridge session). Reusing it silently reported a dead
	// "connected" with no chats and no QR (the Signal-reconnect bug). Log it out (deletes the local
	// login + unlinks the remote device) and fall through to drive a FRESH login with a real QR/code.
	for _, l := range user.GetUserLogins() {
		log.Info().Str("login", string(l.ID)).Msg("clearing a stale persisted login before a fresh connect")
		l.Logout(ctx)
	}

	flowID := net.LoginFlowID() // meta: cookie flow; wa/signal: ""
	if flowID == "" && net.ID() == "whatsapp" && r.phone != "" {
		flowID = "phone" // the app collects a phone → WhatsApp phone-pairing-code flow
	}
	if flowID == "" {
		flows := bb.netConn.GetLoginFlows()
		if len(flows) == 0 {
			return nil, fmt.Errorf("connector exposes no login flows")
		}
		flowID = flows[0].ID // signal: qr
	}
	lp, err := bb.netConn.CreateLogin(ctx, user, flowID)
	if err != nil {
		return nil, fmt.Errorf("create login: %w", err)
	}
	step, err := lp.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("login start: %w", err)
	}
	for {
		switch step.Type {
		case bridgev2.LoginStepTypeUserInput:
			ui, ok := lp.(bridgev2.LoginProcessUserInput)
			if !ok {
				return nil, fmt.Errorf("login needs user input but the process doesn't implement it")
			}
			// The only user-input network here is WhatsApp phone-code (asks phone_number).
			step, err = ui.SubmitUserInput(ctx, map[string]string{"phone_number": r.phone})
			if err != nil {
				return nil, fmt.Errorf("submit phone number: %w", err)
			}
		case bridgev2.LoginStepTypeDisplayAndWait:
			if p := step.DisplayAndWaitParams; p != nil && p.Data != "" {
				code, err := surfaceCode(p.Type, p.Data)
				if err != nil {
					return nil, err
				}
				if err := ls.setCode(r.id, code); err != nil {
					log.Warn().Err(err).Msg("writing pairing code to the request failed")
				}
				log.Info().Str("display", string(p.Type)).Msg("pairing code/QR surfaced to the app")
			}
			waiter, ok := lp.(bridgev2.LoginProcessDisplayAndWait)
			if !ok {
				return nil, fmt.Errorf("login needs display_and_wait but the process doesn't implement Wait")
			}
			step, err = waiter.Wait(ctx)
			if err != nil {
				return nil, fmt.Errorf("login wait: %w", err)
			}
		case bridgev2.LoginStepTypeCookies:
			cl, ok := lp.(bridgev2.LoginProcessCookies)
			if !ok {
				return nil, fmt.Errorf("login is at a cookies step but the process doesn't implement SubmitCookies")
			}
			if r.credential == "" {
				return nil, fmt.Errorf("this network needs a login credential")
			}
			cookies, err := parseMetaCookies(r.credential)
			if err != nil {
				return nil, fmt.Errorf("parse credential: %w", err)
			}
			step, err = cl.SubmitCookies(ctx, cookies)
			if err != nil {
				return nil, fmt.Errorf("submit cookies: %w", err)
			}
		case bridgev2.LoginStepTypeComplete:
			if step.CompleteParams != nil && step.CompleteParams.UserLogin != nil {
				return step.CompleteParams.UserLogin, nil
			}
			if logins := user.GetUserLogins(); len(logins) > 0 {
				return logins[0], nil
			}
			return nil, fmt.Errorf("login complete but no UserLogin available")
		default:
			return nil, fmt.Errorf("unexpected login step type %q", step.Type)
		}
	}
}

// surfaceCode turns a bridge display step into the pairing_code value the web UI
// renders: a QR string becomes a PNG data URL (Signal renders an <img>); a text
// code (WhatsApp) is passed through as-is.
func surfaceCode(t bridgev2.LoginDisplayType, data string) (string, error) {
	if t == bridgev2.LoginDisplayTypeQR {
		code, err := qr.Encode(data, qr.M)
		if err != nil {
			return "", fmt.Errorf("encode QR: %w", err)
		}
		return "data:image/png;base64," + base64.StdEncoding.EncodeToString(code.PNG()), nil
	}
	return data, nil // code / emoji / nothing → the raw text the UI shows
}

// friendlyLoginError maps a login error to a short message the Connect UI shows.
func friendlyLoginError(err error) string {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "too short"):
		return "That phone number looks too short — use international format, e.g. +33612345678."
	case strings.Contains(msg, "international"):
		return "Enter your number in international format, e.g. +33612345678."
	case strings.Contains(msg, "rate"):
		return "WhatsApp is rate-limiting new logins right now. Wait a few minutes and try again."
	case strings.Contains(msg, "timed out") || strings.Contains(msg, "timeout") || strings.Contains(msg, "wait"):
		return "The code expired before it was entered. Please try again."
	default:
		return "Couldn't complete the connection. Please try again."
	}
}

// loginStore is the product-DB access for the Connect flow: read pending requests,
// write back the code/status, and create/finish the connected_account.
type loginStore struct {
	db      *sql.DB
	userID  string
	spaceID string
}

func (s *loginStore) pending() ([]loginRequest, error) {
	rows, err := s.db.Query(
		`select id, network::text, coalesce(phone,''), coalesce(credential,''), coalesce(space_id::text,'')
		   from public.matrix_login_requests
		  where user_id = $1 and status = 'pending'
		  order by created_at asc limit 10`,
		s.userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []loginRequest
	for rows.Next() {
		var r loginRequest
		if err := rows.Scan(&r.id, &r.network, &r.phone, &r.credential, &r.space); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *loginStore) setCode(reqID, code string) error {
	_, err := s.db.Exec(
		`update public.matrix_login_requests set pairing_code = $2, status = 'code_ready', error = null where id = $1`,
		reqID, code,
	)
	return err
}

func (s *loginStore) setError(reqID, msg string) error {
	_, err := s.db.Exec(
		`update public.matrix_login_requests set status = 'error', error = $2 where id = $1`,
		reqID, msg,
	)
	return err
}

// reconcileConnected marks every lingering pending/code_ready request for an already-connected
// network as connected against the existing account (no re-pair). Returns rows affected.
func (s *loginStore) reconcileConnected(network, accountID string) (int64, error) {
	res, err := s.db.Exec(
		`update public.matrix_login_requests set status = 'connected', account_id = $3, error = null
		   where user_id = $1 and network = $2::network and status in ('pending', 'code_ready')`,
		s.userID, network, accountID,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *loginStore) complete(reqID, accountID string) error {
	_, err := s.db.Exec(
		`update public.matrix_login_requests set status = 'connected', account_id = $2, error = null where id = $1`,
		reqID, accountID,
	)
	return err
}

// accountID returns the connected_accounts id for this (user, network), if any.
func (s *loginStore) accountID(network string) (string, error) {
	var id string
	err := s.db.QueryRow(
		`select id from public.connected_accounts where user_id = $1 and network = $2::network order by created_at limit 1`,
		s.userID, network,
	).Scan(&id)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return id, err
}

// ensureAccount returns a connected_accounts id for this (user, network), reusing an
// existing row or creating a fresh pending one.
func (s *loginStore) ensureAccount(network, space string) (string, error) {
	if id, err := s.accountID(network); err != nil {
		return "", err
	} else if id != "" {
		return id, nil
	}
	// Create the account in the space the Connect request targeted (a second Space like
	// "reseaux"), not the engine's default/personal space — otherwise its conversations
	// surface under the wrong Space. Fall back to the configured space if none was passed.
	if space == "" {
		space = s.spaceID
	}
	var id string
	err := s.db.QueryRow(
		`insert into public.connected_accounts (user_id, space_id, network, status)
		   values ($1, $2, $3::network, 'pending') returning id`,
		s.userID, space, network,
	).Scan(&id)
	return id, err
}

// finishAccount marks the account active and records the network handle, so the
// supervisor's discovery starts it on the next boot.
func (s *loginStore) finishAccount(accountID, handle string) error {
	_, err := s.db.Exec(
		`update public.connected_accounts set status = 'active', external_handle = $2 where id = $1`,
		accountID, handle,
	)
	return err
}

// failAccount marks a still-pending account (a fresh row from a failed attempt) as
// errored, so discovery doesn't try to start an unpaired network next boot.
func (s *loginStore) failAccount(accountID string) error {
	_, err := s.db.Exec(
		`update public.connected_accounts set status = 'error' where id = $1 and status = 'pending'`,
		accountID,
	)
	return err
}
