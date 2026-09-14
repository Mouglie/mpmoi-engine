package main

// connector.go — the custom bridgev2 MatrixConnector for the Stage 0 spike.
//
// Instead of bridgev2/matrix (which turns bridge events into appservice calls to
// a Synapse homeserver), this connector is a thin SINK + a tiny read-back store:
// it satisfies the ~20 MatrixConnector methods with deterministic IDs and
// no-ops, and hands out *localIntent (see intent.go) as the MatrixAPI that
// actually writes product-DB rows.
//
// Proving this compiles + drives a real WhatsApp message into public.messages
// with no homeserver is the whole make-or-break gate for the Windows/mobile
// pure-Go engine pivot (docs/bridgev2-local-connector-plan.md).

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

const localServerName = "local"

// LocalConnector implements bridgev2.MatrixConnector.
type LocalConnector struct {
	br    *bridgev2.Bridge
	log   zerolog.Logger
	store *productStore
	net   Network // network-specific glue (id formats, skip rules, names)

	bot *localIntent

	namesMu sync.RWMutex
	names   map[networkid.UserID]string

	// contactResolver (set after login) resolves a network user id to the saved
	// contact name straight from whatsmeow's contact store — the source of truth,
	// fresher and cleaner than the bridge's ghost table.
	contactResolver func(context.Context, networkid.UserID) string

	// loginReceiver (set after login) is this login's id, which is the Receiver of
	// its non-split DM portals. Used as the receiver hint when looking a DM portal
	// up by chat id alone (the portal-receiver query is `receiver=$3 OR ''`, so a
	// zero hint never matches a DM's non-empty receiver).
	loginReceiver networkid.UserLoginID
}

func (c *LocalConnector) setContactResolver(f func(context.Context, networkid.UserID) string) {
	c.contactResolver = f
}

func (c *LocalConnector) setLoginReceiver(r networkid.UserLoginID) {
	c.loginReceiver = r
}

func (c *LocalConnector) setGhostName(uid networkid.UserID, name string) {
	c.namesMu.Lock()
	defer c.namesMu.Unlock()
	if c.names == nil {
		c.names = make(map[networkid.UserID]string)
	}
	c.names[uid] = name
}

// ghostName returns the ghost's display name, falling back to the raw network
// user id when the bridge hasn't set one yet.
func (c *LocalConnector) ghostName(uid networkid.UserID) string {
	c.namesMu.RLock()
	defer c.namesMu.RUnlock()
	if n, ok := c.names[uid]; ok && n != "" {
		return n
	}
	return string(uid)
}

// resolveGhostName resolves a sender's display name durably: in-memory cache
// first (populated by SetDisplayName during live sync), then the bridge's own
// ghost table (survives restarts, so names work on a resumed session too).
func (c *LocalConnector) resolveGhostName(ctx context.Context, uid networkid.UserID) string {
	if uid == "" {
		return ""
	}
	// Saved contact name from whatsmeow (already clean, no " (WA)" suffix).
	if c.contactResolver != nil {
		if n := c.contactResolver(ctx, uid); n != "" {
			return n
		}
	}
	c.namesMu.RLock()
	n, ok := c.names[uid]
	c.namesMu.RUnlock()
	if ok && n != "" {
		return c.net.CleanName(n)
	}
	if c.br != nil {
		if g, err := c.br.GetExistingGhostByID(ctx, uid); err == nil && g != nil && g.Name != "" {
			c.setGhostName(uid, g.Name)
			return c.net.CleanName(g.Name)
		}
	}
	return string(uid)
}

// resolveChatTitle picks a conversation title the way the product expects:
// self-chat → "Note to self"; group → the portal (group) name; 1:1 DM → the
// peer contact's name (mautrix leaves DM portal.Name empty by design).
func (c *LocalConnector) resolveChatTitle(ctx context.Context, chatID string, room networkid.PortalKey) string {
	// Only real network chat ids get a title. Anything else (e.g. a leftover
	// Matrix room id from the Synapse build) must never become a title.
	if !c.net.IsRoutable(chatID) {
		return ""
	}
	switch {
	case c.store.isSelfChat(chatID):
		return "Note to self"
	case c.net.IsGroup(chatID):
		key := room
		if key.ID == "" {
			key.ID = networkid.PortalID(chatID)
		}
		// Networks whose chat id can't tell a DM from a group report every chat as
		// a "group" (Meta: the chat id is a thread FBID, not the peer's id), so a
		// real 1:1 DM lands here with an empty portal.Name (mautrix leaves DM names
		// empty by design). The bridge records the DM peer as the portal's
		// OtherUserID — prefer that peer's contact name, and fall back to the
		// portal/group name for genuine groups (where OtherUserID is unset).
		if peer := c.portalOtherUser(ctx, key); peer != "" {
			if name := c.resolveGhostName(ctx, peer); name != "" && name != string(peer) {
				return name
			}
		}
		return c.resolvePortalName(ctx, key)
	default:
		return c.resolveGhostName(ctx, c.net.PeerGhostID(chatID))
	}
}

// mediaKind maps a Matrix media msgtype to the product attachment kind.
func mediaKind(t event.MessageType) string {
	switch t {
	case event.MsgImage:
		return "image"
	case "m.sticker":
		return "sticker"
	case event.MsgVideo:
		return "video"
	case event.MsgAudio:
		return "audio"
	case event.MsgFile:
		return "file"
	}
	return ""
}

// resolvePortalName reads a chat's title from the bridge's own portal table.
func (c *LocalConnector) resolvePortalName(ctx context.Context, key networkid.PortalKey) string {
	if c.br == nil || key.ID == "" {
		return ""
	}
	if p, err := c.br.GetExistingPortalByKey(ctx, key); err == nil && p != nil {
		return p.Name
	}
	return ""
}

// portalOtherUser returns the DM peer's ghost id for a chat (the bridge records it
// as the portal's OtherUserID), or "" for a group / when the portal isn't found.
func (c *LocalConnector) portalOtherUser(ctx context.Context, key networkid.PortalKey) networkid.UserID {
	if c.br == nil || key.ID == "" {
		return ""
	}
	// The startup reconcile passes only the chat id. A DM portal is stored with
	// Receiver=loginID, and the lookup query matches `receiver=hint OR receiver=''`,
	// so hint the login's receiver — a zero hint would only match groups.
	if key.Receiver == "" {
		key.Receiver = c.loginReceiver
	}
	if p, err := c.br.GetExistingPortalByKey(ctx, key); err == nil && p != nil {
		return p.OtherUserID
	}
	return ""
}

// chatIsGroup reports whether a chat is a group (vs a 1:1 DM), for the conversation's
// is_group flag (the web shows a per-message sender only in groups). Accurate across
// networks: WhatsApp/Signal encode group-ness in the chat id; Meta reports EVERY chat
// as a "group", so a real Meta DM is distinguished by having a portal peer (OtherUserID).
func (c *LocalConnector) chatIsGroup(ctx context.Context, chatID string, room networkid.PortalKey) bool {
	if c.store.isSelfChat(chatID) {
		return false
	}
	if !c.net.IsGroup(chatID) {
		return false // WhatsApp/Signal 1:1 DM (chat id says so)
	}
	key := room
	if key.ID == "" {
		key.ID = networkid.PortalID(chatID)
	}
	if c.portalOtherUser(ctx, key) != "" {
		return false // a recorded DM peer ⇒ not a group (covers Meta DMs)
	}
	return true
}

// chatHandle returns a network handle to show under a chat's title — currently the
// Instagram @username of a DM peer (whose display name is often just an emoji, so
// the handle is what identifies them, like Instagram shows when you open the chat).
// "" for groups, self-chats, and networks without a handle concept.
func (c *LocalConnector) chatHandle(ctx context.Context, chatID string, room networkid.PortalKey) string {
	if c.net.ID() != "instagram" || c.store.isSelfChat(chatID) {
		return ""
	}
	key := room
	if key.ID == "" {
		key.ID = networkid.PortalID(chatID)
	}
	peer := c.portalOtherUser(ctx, key)
	if peer == "" || c.br == nil {
		return ""
	}
	g, err := c.br.GetExistingGhostByID(ctx, peer)
	if err != nil || g == nil {
		return ""
	}
	// igconnector stores the peer's username as an identifier "instagram:<handle>".
	for _, ident := range g.Identifiers {
		if h, ok := strings.CutPrefix(ident, "instagram:"); ok && h != "" {
			return h
		}
	}
	return ""
}

// mediaDir is where UploadMedia persists blobs for the spike.
func mediaDir() string {
	dir := os.Getenv("MPMOI_MEDIA_DIR")
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "mpmoi-spike-media")
	}
	_ = os.MkdirAll(dir, 0o755)
	return dir
}

var _ bridgev2.MatrixConnector = (*LocalConnector)(nil)

func NewLocalConnector(store *productStore, net Network) *LocalConnector {
	return &LocalConnector{store: store, net: net}
}

func (c *LocalConnector) Init(br *bridgev2.Bridge) {
	c.br = br
	c.log = br.Log.With().Str("component", "local_connector").Logger()
	c.bot = &localIntent{conn: c, mxid: id.UserID("@wabot:" + localServerName), isBot: true}
}

func (c *LocalConnector) Start(ctx context.Context) error {
	c.log.Info().Msg("local connector started (no homeserver)")
	return nil
}

func (c *LocalConnector) PreStop() {}
func (c *LocalConnector) Stop()    {}

func (c *LocalConnector) GetCapabilities() *bridgev2.MatrixCapabilities {
	// AutoJoinInvites: don't wait for real joins. BatchSending false: force the
	// bridge to backfill via ordinary SendMessage (which we implement) rather
	// than BatchSend (which we don't).
	return &bridgev2.MatrixCapabilities{
		AutoJoinInvites:       true,
		BatchSending:          false,
		ArbitraryMemberChange: true,
	}
}

// ---- ghost / intent plumbing ----

func (c *LocalConnector) FormatGhostMXID(userID networkid.UserID) id.UserID {
	return id.UserID(fmt.Sprintf("@%s_%s:%s", c.net.GhostPrefix(), userID, localServerName))
}

func (c *LocalConnector) ParseGhostMXID(userID id.UserID) (networkid.UserID, bool) {
	s := string(userID)
	prefix := "@" + c.net.GhostPrefix() + "_"
	suffix := ":" + localServerName
	if strings.HasPrefix(s, prefix) && strings.HasSuffix(s, suffix) {
		return networkid.UserID(s[len(prefix) : len(s)-len(suffix)]), true
	}
	return "", false
}

func (c *LocalConnector) GhostIntent(userID networkid.UserID) bridgev2.MatrixAPI {
	return &localIntent{conn: c, ghostID: userID, mxid: c.FormatGhostMXID(userID)}
}

func (c *LocalConnector) NewUserIntent(ctx context.Context, userID id.UserID, accessToken string) (bridgev2.MatrixAPI, string, error) {
	// No double-puppeting in the spike; hand back a passive intent.
	return &localIntent{conn: c, mxid: userID, isDoublePuppet: true}, accessToken, nil
}

func (c *LocalConnector) BotIntent() bridgev2.MatrixAPI { return c.bot }

// ---- status / no-ops ----

func (c *LocalConnector) SendBridgeStatus(ctx context.Context, state *status.BridgeState) error {
	c.log.Debug().Str("state", string(state.StateEvent)).Msg("bridge status")
	return nil
}

// SendMessageStatus is the bridge's authoritative per-event delivery callback:
// after the portal event loop actually relays (or fails to relay) a Matrix event
// to WhatsApp, it calls this with the real outcome. We use it to reconcile the
// product row's delivery_status — the enqueue in drainOutbound is NOT proof of
// delivery (QueueMatrixEvent only buffers the event), so this is where an
// outbound message truly becomes "sent" or "failed".
func (c *LocalConnector) SendMessageStatus(ctx context.Context, ms *bridgev2.MessageStatus, evt *bridgev2.MessageStatusEventInfo) {
	if ms == nil || evt == nil {
		return
	}
	rowID, ok := strings.CutPrefix(string(evt.SourceEventID), "$mpmoi-out-")
	if !ok {
		// Reactions/redactions ($mpmoi-rxn-/$mpmoi-unrxn-) and bridge-internal
		// events aren't tracked as message rows; still surface failures in the log.
		if ms.Status == event.MessageStatusRetriable || ms.Status == event.MessageStatusFail {
			c.log.Warn().Str("event", string(evt.SourceEventID)).Str("status", string(ms.Status)).
				Str("err", statusErr(ms)).Msg("outbound non-message event failed")
		}
		return
	}
	switch ms.Status {
	case event.MessageStatusSuccess:
		_ = c.store.markOutbound(rowID, "sent", "")
		c.store.clearInflight(rowID)
		c.log.Info().Str("row", rowID).Str("network", c.net.ID()).Msg("outbound delivered")
	case event.MessageStatusPending:
		// Still in flight — leave the row pending and keep the in-flight guard.
	default: // FAIL_RETRIABLE / FAIL_PERMANENT
		_ = c.store.markOutbound(rowID, "failed", "")
		c.store.clearInflight(rowID)
		c.log.Error().Str("row", rowID).Str("status", string(ms.Status)).
			Str("err", statusErr(ms)).Str("network", c.net.ID()).Msg("outbound send FAILED")
	}
}

// statusErr extracts the most useful human string from a MessageStatus.
func statusErr(ms *bridgev2.MessageStatus) string {
	if ms.InternalError != nil {
		return ms.InternalError.Error()
	}
	return ms.Message
}

// ---- media content URIs (fake mxc:// that ParseContentURI reverses) ----

func (c *LocalConnector) GenerateContentURI(ctx context.Context, mediaID networkid.MediaID) (id.ContentURIString, error) {
	return id.ContentURIString(fmt.Sprintf("mxc://%s/%s", localServerName, base64.RawURLEncoding.EncodeToString([]byte(mediaID)))), nil
}

func (c *LocalConnector) ParseContentURI(ctx context.Context, uri id.ContentURIString) (networkid.MediaID, error) {
	parsed, err := uri.Parse()
	if err != nil {
		return nil, err
	}
	raw, err := base64.RawURLEncoding.DecodeString(parsed.FileID)
	if err != nil {
		return nil, err
	}
	return networkid.MediaID(raw), nil
}

// ---- room membership queries (minimal) ----

func (c *LocalConnector) GetPowerLevels(ctx context.Context, roomID id.RoomID) (*event.PowerLevelsEventContent, error) {
	return &event.PowerLevelsEventContent{}, nil
}

func (c *LocalConnector) GetMembers(ctx context.Context, roomID id.RoomID) (map[id.UserID]*event.MemberEventContent, error) {
	return map[id.UserID]*event.MemberEventContent{}, nil
}

func (c *LocalConnector) GetMemberInfo(ctx context.Context, roomID id.RoomID, userID id.UserID) (*event.MemberEventContent, error) {
	return &event.MemberEventContent{Membership: event.MembershipJoin}, nil
}

// ---- batch send (disabled via capabilities) ----

func (c *LocalConnector) BatchSend(ctx context.Context, roomID id.RoomID, req *mautrix.ReqBeeperBatchSend, extras []*bridgev2.MatrixSendExtra) (*mautrix.RespBeeperBatchSend, error) {
	return nil, fmt.Errorf("batch send not supported by local connector")
}

// ---- deterministic IDs ----

func (c *LocalConnector) GenerateDeterministicRoomID(key networkid.PortalKey) id.RoomID {
	roomID := id.RoomID(fmt.Sprintf("!%s:%s", hashShort("room", string(key.ID), string(key.Receiver)), localServerName))
	// Cache the mapping so CreateRoom/SendMessage can resolve the room back to
	// its remote chat id (the PortalKey.ID) for the conversations row.
	c.store.rememberRoom(roomID, string(key.ID))
	return roomID
}

func (c *LocalConnector) GenerateDeterministicEventID(roomID id.RoomID, _ networkid.PortalKey, messageID networkid.MessageID, partID networkid.PartID) id.EventID {
	return id.EventID(fmt.Sprintf("$%s", hashShort("evt", string(roomID), string(messageID), string(partID))))
}

func (c *LocalConnector) GenerateReactionEventID(roomID id.RoomID, targetMessage *database.Message, sender networkid.UserID, emojiID networkid.EmojiID) id.EventID {
	return id.EventID(fmt.Sprintf("$%s", hashShort("rea", string(roomID), string(targetMessage.ID), string(sender), string(emojiID))))
}

func (c *LocalConnector) ServerName() string { return localServerName }

func hashShort(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return base64.RawURLEncoding.EncodeToString(h[:])[:24]
}
