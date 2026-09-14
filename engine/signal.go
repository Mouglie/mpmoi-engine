package main

// signal.go — the Signal Network impl. Signal uses UUID-based ids (no
// "@s.whatsapp.net"): a DM portal id is the peer's serviceID string, a group
// portal id is a 44-char group identifier, and the login's own id is its ACI
// UUID — which is already MakeUserID(ACI), so self detection needs no client
// binding. Contact names fall back to the bridge's ghost table.
//
// Signal links libsignal (Rust) via cgo, so building the engine with Signal
// needs libsignal_ffi.a on the linker path (see the engine build invocation).

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	sigconnector "go.mau.fi/mautrix-signal/pkg/connector"
)

type signalNetwork struct{}

var _ Network = signalNetwork{}

func (signalNetwork) ID() string            { return "signal" }
func (signalNetwork) CommandPrefix() string { return "!sg" }
func (signalNetwork) GhostPrefix() string   { return "sg" }
func (signalNetwork) LoginFlowID() string   { return "" } // QR: use the first advertised flow

func (signalNetwork) NewConnector() (bridgev2.NetworkConnector, error) {
	c := &sigconnector.SignalConnector{}
	if err := yaml.Unmarshal([]byte(sigconnector.ExampleConfig), &c.Config); err != nil {
		return nil, fmt.Errorf("seed signal config: %w", err)
	}
	return c, nil
}

// Signal has no WhatsApp-Status / newsletter equivalent.
func (signalNetwork) SkipChat(chatID string) bool { return false }

// A Signal group portal id is a 44-char group identifier; DMs are serviceID
// strings (36-char ACI UUID, or "PNI:<uuid>").
func (signalNetwork) IsGroup(chatID string) bool { return len(chatID) == 44 }

// IsRoutable: any Signal-formatted id (i.e. not an old Matrix room id
// "!room:server" left over from the Synapse build).
func (signalNetwork) IsRoutable(chatID string) bool {
	return chatID != "" && !strings.HasPrefix(chatID, "!")
}

// PeerGhostID: a DM chat id IS the peer's serviceID string. For an ACI that
// equals the ghost UserID (the plain UUID); a PNI service id "PNI:<uuid>" maps
// to the "pni_<uuid>" ghost id form (see signalid.MakeUserIDFromServiceID).
func (signalNetwork) PeerGhostID(chatID string) networkid.UserID {
	if strings.HasPrefix(chatID, "PNI:") {
		return networkid.UserID("pni_" + strings.TrimPrefix(chatID, "PNI:"))
	}
	return networkid.UserID(chatID)
}

func (signalNetwork) CleanName(n string) string {
	return strings.TrimSuffix(strings.TrimSpace(n), " (Signal)")
}

func (signalNetwork) Bind(login *bridgev2.UserLogin) NetworkIdentity {
	// login.ID is the ACI UUID string, which is exactly MakeUserID(ACI) — the
	// login's own ghost id. Contact names fall back to the bridge ghost table.
	return NetworkIdentity{SelfIDs: []networkid.UserID{networkid.UserID(login.ID)}}
}
