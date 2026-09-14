package main

// network.go — the per-network abstraction that lets the SAME engine core
// (LocalConnector + product store + outbound poller) drive WhatsApp, Signal, and
// (later) Meta. Everything network-specific — id/chat formats, self identity,
// contact names, login flow, which chats to skip — lives behind the Network
// interface; the engine core stays network-agnostic.
//
// One engine process runs one network (selected by MPMOI_NETWORK); the desktop
// supervisor runs one process per connected account. main.go picks the Network
// impl and hands it to runNetwork.

import (
	"context"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// Network is the static, login-independent description of a messaging network.
type Network interface {
	// ID is the public.network enum value the product DB rows are keyed by
	// ("whatsapp", "signal", "instagram", "messenger").
	ID() string
	// CommandPrefix is the bridge management-room command prefix ("!wa", "!sg").
	CommandPrefix() string
	// GhostPrefix namespaces ghost MXIDs per network ("wa", "sg") so the
	// deterministic ids never collide across networks.
	GhostPrefix() string
	// NewConnector builds + seeds the bridgev2 network connector for this bridge.
	NewConnector() (bridgev2.NetworkConnector, error)
	// LoginFlowID is the login flow to drive. "" = use the connector's first
	// advertised flow (QR networks). Meta returns a specific cookie flow id
	// (its Instagram flow isn't advertised, so we pass it explicitly).
	LoginFlowID() string

	// ---- chat-id classification (no live login needed) ----

	// SkipChat reports chats that must never become conversations (WhatsApp
	// Status/newsletters, etc.).
	SkipChat(chatID string) bool
	// IsGroup reports whether a chat id is a group (vs a 1:1 DM).
	IsGroup(chatID string) bool
	// IsRoutable reports whether a chat id is one we can send to / one that
	// belongs to THIS network (filters out old Synapse room-id-keyed rows).
	IsRoutable(chatID string) bool
	// PeerGhostID maps a 1:1 chat id to the other party's ghost id.
	PeerGhostID(chatID string) networkid.UserID
	// CleanName strips the bridge's network name suffix (" (WA)", …).
	CleanName(name string) string

	// Bind attaches to the live, logged-in client to expose the login's own
	// identity and (optionally) a contact-name resolver.
	Bind(login *bridgev2.UserLogin) NetworkIdentity
}

// NetworkIdentity is the login-dependent glue: who "I" am on this network, and
// how to resolve a peer's saved contact name (nil ContactName = fall back to the
// bridge's ghost table).
type NetworkIdentity struct {
	SelfIDs []networkid.UserID
	// SelfChatID is the canonical remote_chat_id of the "message yourself" chat, used
	// to collapse WhatsApp's phone-JID + LID self-chats onto one conversation. "" = the
	// network has a single self-chat id already (no canonicalisation needed).
	SelfChatID  string
	ContactName func(ctx context.Context, uid networkid.UserID) string
}

// networkByID returns the Network impl for a public.network enum value.
func networkByID(id string) (Network, bool) {
	switch id {
	case "whatsapp":
		return whatsAppNetwork{}, true
	case "signal":
		return signalNetwork{}, true
	case "instagram":
		return instagramNetwork(), true
	case "messenger":
		return messengerNetwork(), true
	default:
		return nil, false
	}
}
