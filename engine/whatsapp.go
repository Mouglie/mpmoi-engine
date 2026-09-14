package main

// whatsapp.go — the WhatsApp Network impl. Holds everything that used to be
// WhatsApp-specific in main.go/connector.go: id/chat formats, self identity
// (phone JID + LID), contact names from whatsmeow's store, and the connector
// seeding. This is the reference Network; Signal mirrors its shape.

import (
	"context"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-whatsapp/pkg/connector"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
	"go.mau.fi/whatsmeow/types"
)

type whatsAppNetwork struct{}

var _ Network = whatsAppNetwork{}

func (whatsAppNetwork) ID() string            { return "whatsapp" }
func (whatsAppNetwork) CommandPrefix() string { return "!wa" }
func (whatsAppNetwork) GhostPrefix() string   { return "wa" }
func (whatsAppNetwork) LoginFlowID() string   { return "" } // QR: use the first advertised flow

func (whatsAppNetwork) NewConnector() (bridgev2.NetworkConnector, error) {
	waConn := &connector.WhatsAppConnector{}
	if err := yaml.Unmarshal([]byte(connector.ExampleConfig), &waConn.Config); err != nil {
		return nil, fmt.Errorf("seed whatsapp config: %w", err)
	}
	return waConn, nil
}

// SkipChat: WhatsApp Status (status@broadcast) and channels/newsletters.
func (whatsAppNetwork) SkipChat(chatID string) bool {
	return strings.Contains(chatID, "@broadcast") || strings.Contains(chatID, "@newsletter")
}

func (whatsAppNetwork) IsGroup(chatID string) bool {
	return strings.Contains(chatID, "@g.us")
}

// IsRoutable: a real WhatsApp chat id always contains '@' (…@s.whatsapp.net /
// @g.us / @lid). Old Synapse-build rows are Matrix room ids ("!room:server").
func (whatsAppNetwork) IsRoutable(chatID string) bool {
	return strings.Contains(chatID, "@")
}

// PeerGhostID maps a 1:1 chat id to the other party's ghost id
// ("33672724904@s.whatsapp.net" → "33672724904"; "…@lid" → "lid-…").
func (whatsAppNetwork) PeerGhostID(chatID string) networkid.UserID {
	user := chatID
	if idx := strings.IndexByte(chatID, '@'); idx >= 0 {
		user = chatID[:idx]
	}
	if strings.HasSuffix(chatID, "@lid") {
		return networkid.UserID("lid-" + user)
	}
	return networkid.UserID(user)
}

// CleanName strips the bridge's " (WA)" network suffix.
func (whatsAppNetwork) CleanName(n string) string {
	return strings.TrimSuffix(strings.TrimSpace(n), " (WA)")
}

func (whatsAppNetwork) Bind(login *bridgev2.UserLogin) NetworkIdentity {
	ni := NetworkIdentity{
		// The login id is the phone number — a valid self id even before connect.
		SelfIDs: []networkid.UserID{networkid.UserID(login.ID)},
	}
	wa, ok := login.Client.(*connector.WhatsAppClient)
	if !ok || wa == nil {
		return ni
	}
	// WhatsApp self identity is the phone JID AND the LID; self-sent messages
	// arrive under either. Mirrors mautrix-whatsapp's own IsFromMe check.
	ni.SelfIDs = append(ni.SelfIDs, waid.MakeUserID(wa.JID))
	if wa.Device != nil {
		ni.SelfIDs = append(ni.SelfIDs, waid.MakeUserID(wa.Device.GetLID()))
	}
	// Canonical self-chat id = the phone JID (device stripped), e.g.
	// "33695402683@s.whatsapp.net". The LID self-chat is folded onto this so there's
	// a single "Note to self" conversation instead of one per identity.
	ni.SelfChatID = wa.JID.ToNonAD().String()
	ni.ContactName = makeWAContactResolver(wa)
	return ni
}

// makeWAContactResolver looks a network user id up in whatsmeow's contact store
// and returns the saved contact name (full → first → push → business), or "".
func makeWAContactResolver(wa *connector.WhatsAppClient) func(context.Context, networkid.UserID) string {
	return func(ctx context.Context, uid networkid.UserID) string {
		jid := waid.ParseUserID(uid)
		if jid.User == "" {
			return ""
		}
		dev := wa.GetStore()
		if dev == nil {
			return ""
		}
		// A LID chat maps to a phone-number JID for the contact lookup.
		if jid.Server == types.HiddenUserServer {
			if pn, err := dev.LIDs.GetPNForLID(ctx, jid); err == nil && pn.User != "" {
				jid = pn
			}
		}
		info, err := dev.Contacts.GetContact(ctx, jid)
		if err != nil || !info.Found {
			return ""
		}
		for _, n := range []string{info.FullName, info.FirstName, info.PushName, info.BusinessName} {
			if n != "" {
				return n
			}
		}
		return ""
	}
}
