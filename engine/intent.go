package main

// intent.go — the MatrixAPI the bridge sends through. Each *localIntent is bound
// to one "sender" (a ghost = a remote contact, or the bridge bot). The bridge
// calls SendMessage/SendState/CreateRoom on it; we turn those into product-DB
// rows instead of homeserver events.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type localIntent struct {
	conn           *LocalConnector
	ghostID        networkid.UserID // empty for the bot
	mxid           id.UserID
	isBot          bool
	isDoublePuppet bool
}

var _ bridgev2.MatrixAPI = (*localIntent)(nil)

func (i *localIntent) GetMXID() id.UserID    { return i.mxid }
func (i *localIntent) IsDoublePuppet() bool  { return i.isDoublePuppet }

// SendMessage is the heart of the spike: a remote message the bridge decoded
// becomes a public.messages row.
func (i *localIntent) SendMessage(ctx context.Context, roomID id.RoomID, eventType event.Type, content *event.Content, extra *bridgev2.MatrixSendExtra) (*mautrix.RespSendEvent, error) {
	// Reactions come through SendMessage too (event type m.reaction) but must NOT
	// become message rows — otherwise every 👍 shows up as an empty bubble. Route
	// them to reaction handling instead. (TODO Stage 1: write to the reactions
	// table keyed on the target message; for now we just don't pollute messages.)
	if eventType == event.EventReaction {
		evtID := i.newEventID(roomID, extra)
		if extra != nil && extra.ReactionMeta != nil {
			rm := extra.ReactionMeta
			if convID, err := i.conn.store.convIDForChat(string(rm.Room.ID)); err == nil {
				chosen := i.conn.store.isSelf(rm.SenderID)
				if err := i.conn.store.applyReaction(convID, string(rm.MessageID), rm.Emoji, chosen); err != nil {
					i.conn.log.Debug().Err(err).Msg("reaction target not mirrored yet — skipped")
				} else {
					i.conn.log.Info().Str("emoji", rm.Emoji).Bool("chosen", chosen).Msg("→ reaction merged")
				}
			}
		}
		i.conn.store.putEvent(evtID, &storedEvent{RoomID: roomID, Sender: i.mxid, Type: eventType, Content: content, TS: time.Now()})
		return &mautrix.RespSendEvent{EventID: evtID}, nil
	}

	// Redactions carry reaction removals (ReactionMeta) and message deletions
	// (MessageMeta). They must NOT become message rows.
	if eventType == event.EventRedaction {
		evtID := i.newEventID(roomID, extra)
		if extra != nil && extra.ReactionMeta != nil {
			rm := extra.ReactionMeta
			if convID, err := i.conn.store.convIDForChat(string(rm.Room.ID)); err == nil {
				if err := i.conn.store.removeReaction(convID, string(rm.MessageID), rm.Emoji, i.conn.store.isSelf(rm.SenderID)); err == nil {
					i.conn.log.Info().Str("emoji", rm.Emoji).Msg("← reaction removed")
				}
			}
		} else if extra != nil && extra.MessageMeta != nil {
			mm := extra.MessageMeta
			if convID, err := i.conn.store.convIDForChat(string(mm.Room.ID)); err == nil {
				_ = i.conn.store.deleteMessage(convID, string(mm.ID))
			}
		}
		return &mautrix.RespSendEvent{EventID: evtID}, nil
	}

	msg, _ := content.Parsed.(*event.MessageEventContent)

	// The bot only ever emits notices/management text; skip it entirely. (Business/OTP
	// messages arrive under the user login intent, not the bot — the notice skip below is
	// empty-only so those real messages aren't dropped.)
	if i.isBot {
		return &mautrix.RespSendEvent{EventID: i.newEventID(roomID, extra)}, nil
	}

	// Message timestamp — drives sent_at AND the conversation's last_message_at (list order).
	// Prefer the bridge's explicit send timestamp; on backfill that can be zero (which bit us:
	// an old [media] message fell back to time.Now() and sorted an ancient group to the top), so
	// fall back to the message's OWN stored timestamp before ever using now().
	// Prefer the message's OWN stored timestamp (MessageMeta) over extra.Timestamp: on
	// backfill, extra.Timestamp is the moment the event is replayed into Matrix (≈ now for
	// late-delivered media, which sorted an old group to the top), whereas MessageMeta.Timestamp
	// is the real WhatsApp message time. Fall back to extra.Timestamp, then now().
	ts := time.Now()
	if extra != nil {
		if extra.MessageMeta != nil && !extra.MessageMeta.Timestamp.IsZero() {
			ts = extra.MessageMeta.Timestamp
		} else if !extra.Timestamp.IsZero() {
			ts = extra.Timestamp
		}
	}
	// Prefer the authoritative chat id the bridge hands us on the message
	// (survives restarts, unlike the in-memory room→chat map). Fall back to the
	// map, then the room id only as a last resort.
	chatID := ""
	if extra != nil && extra.MessageMeta != nil {
		chatID = string(extra.MessageMeta.Room.ID)
	}
	if chatID == "" {
		var ok bool
		if chatID, ok = i.conn.store.chatForRoom(roomID); !ok {
			chatID = string(roomID)
		}
	}
	if i.conn.net.SkipChat(chatID) { // network non-chats (WhatsApp Status/channels, …)
		return &mautrix.RespSendEvent{EventID: i.newEventID(roomID, extra)}, nil
	}
	// Skip EMPTY bridge/status notices, but let notices carrying real text through: WhatsApp
	// classifies some business/OTP messages (e.g. "Amazon: votre code est …") as m.notice, and
	// dropping every notice lost those whole conversations.
	if msg != nil && msg.MsgType == event.MsgNotice && strings.TrimSpace(msg.Body) == "" {
		return &mautrix.RespSendEvent{EventID: i.newEventID(roomID, extra)}, nil
	}
	var room networkid.PortalKey
	if extra != nil && extra.MessageMeta != nil {
		room = extra.MessageMeta.Room
	}
	title := i.conn.resolveChatTitle(ctx, chatID, room)
	isGroup := i.conn.chatIsGroup(ctx, chatID, room)
	handle := i.conn.chatHandle(ctx, chatID, room)
	convID, err := i.conn.store.upsertConversation(roomID, chatID, title, handle, isGroup, ts)
	if err != nil {
		return nil, err
	}

	body := ""
	if msg != nil {
		body = msg.Body
		if msg.NewContent != nil { // an edit — use the clean new text, not the "* …" fallback
			body = msg.NewContent.Body
		}
	}
	// The sender ghost id: for messages from other people bridgev2 sends via the
	// sender's GhostIntent (so i.ghostID is set). But the user's OWN messages are
	// sent via the double-puppet user intent, whose ghostID is empty — so fall
	// back to the message's real SenderID to recognise self-sends.
	senderID := i.ghostID
	if senderID == "" && extra != nil && extra.MessageMeta != nil {
		senderID = extra.MessageMeta.SenderID
	}
	sender := i.conn.resolveGhostName(ctx, senderID)
	direction := "incoming"
	if i.conn.store.isSelf(senderID) {
		direction = "outgoing"
		sender = "You"
	}

	var remoteMsgID string
	if extra != nil && extra.MessageMeta != nil {
		remoteMsgID = string(extra.MessageMeta.ID)
		// The self-chat is delivered via TWO portals (phone JID + LID); their message ids are
		// portal-prefixed ("<chat>:<sender>:<waMsgID>"), so the same WhatsApp message arrives with
		// two different ids and both landed in the merged "Note to self" conversation — every
		// message doubled. Dedupe self-chat messages by the underlying WhatsApp id (last ':' segment)
		// so the upsert on (conversation_id, remote_message_id) collapses the two deliveries into one.
		if i.conn.store.isSelfChat(chatID) {
			if idx := strings.LastIndexByte(remoteMsgID, ':'); idx >= 0 {
				remoteMsgID = remoteMsgID[idx+1:]
			}
		}
	}
	evtID := i.newEventID(roomID, extra)

	atts := []any{}
	if msg != nil {
		// Media: the bridge already handed us the bytes via UploadMedia; move them
		// into the product media bucket and attach a storagePath the web signs.
		kind := mediaKind(msg.MsgType)
		// Stickers arrive as m.sticker EVENTS (event.EventSticker) whose MessageEventContent
		// carries an EMPTY MsgType, so mediaKind misses them — they fell through to the text
		// path and rendered the sticker's DESCRIPTION (e.g. "Spider-Man Marvel Sticker by …")
		// instead of the image. Force the sticker kind so it's stored + rendered as an image.
		if eventType == event.EventSticker {
			kind = "sticker"
		}
		if kind != "" && msg.URL != "" {
			if data, err := i.DownloadMedia(ctx, msg.URL, msg.File); err == nil && len(data) > 0 {
				mime := "application/octet-stream"
				if msg.Info != nil && msg.Info.MimeType != "" {
					mime = msg.Info.MimeType
				}
				if att, err := i.conn.store.saveMedia(convID, string(evtID), kind, mime, msg.FileName, data); err == nil {
					atts = append(atts, att)
				} else {
					i.conn.log.Warn().Err(err).Str("room", string(roomID)).Msg("saveMedia failed")
				}
			} else if err != nil {
				i.conn.log.Warn().Err(err).Msg("media download for storage failed")
			}
		}
		// Link previews (WhatsApp/Beeper) → link attachments.
		for _, lp := range msg.BeeperLinkPreviews {
			if lp == nil {
				continue
			}
			url := lp.MatchedURL
			if url == "" {
				url = lp.CanonicalURL
			}
			if url == "" {
				continue
			}
			atts = append(atts, linkAttachment{
				Kind: "link", URL: url, Name: lp.Title,
				Description: lp.Description, SiteName: lp.SiteName,
			})
		}
	}
	// Captionless media: the bridge sets the message body to the FILENAME (a Matrix
	// convention), not a real caption. Don't surface it — otherwise the inbox shows
	// "audioclip-….mp4" / "video-….mp4" under the player. A genuine caption differs
	// from the filename, so it survives this check.
	if msg != nil && msg.FileName != "" && body == msg.FileName {
		body = ""
	}
	// A sticker's body is a DESCRIPTION ("Spider-Man Marvel Sticker by …"), not a caption —
	// drop it once the image is attached so the sticker renders as an image, not as text.
	if eventType == event.EventSticker && len(atts) > 0 {
		body = ""
	}

	var attachments any
	if len(atts) > 0 {
		attachments = atts
	}

	// Skip empty artifacts (no text and no attachment) — they'd show as blank
	// bubbles and clobber the conversation preview.
	if body == "" && attachments == nil {
		return &mautrix.RespSendEvent{EventID: evtID}, nil
	}

	if err := i.conn.store.upsertMessage(convID, remoteMsgID, sender, direction, body, ts, attachments); err != nil {
		return nil, err
	}

	i.conn.store.putEvent(evtID, &storedEvent{RoomID: roomID, Sender: i.mxid, Type: eventType, Content: content, TS: ts})
	i.conn.log.Info().
		Str("room", string(roomID)).Str("sender", sender).
		Str("direction", direction).Str("body", truncate(body, 60)).
		Bool("media", attachments != nil).
		Msg("→ product DB message row")
	return &mautrix.RespSendEvent{EventID: evtID}, nil
}

func (i *localIntent) newEventID(roomID id.RoomID, extra *bridgev2.MatrixSendExtra) id.EventID {
	if extra != nil && extra.MessageMeta != nil {
		return i.conn.GenerateDeterministicEventID(roomID, extra.MessageMeta.Room, extra.MessageMeta.ID, extra.MessageMeta.PartID)
	}
	return id.EventID("$" + hashShort("evt", string(roomID), time.Now().String()))
}

func (i *localIntent) SendState(ctx context.Context, roomID id.RoomID, eventType event.Type, stateKey string, content *event.Content, ts time.Time) (*mautrix.RespSendEvent, error) {
	// The only state we care about for the spike is the room name → conversation title.
	if eventType == event.StateRoomName {
		if nc, ok := content.Parsed.(*event.RoomNameEventContent); ok && nc.Name != "" {
			if chatID, ok := i.conn.store.chatForRoom(roomID); ok {
				// Update-only: never CREATE a row from a rename (that would resurrect the
				// empty-conversation bug). If the chat has no message yet, the name is read
				// from the bridge portal when its first message creates the conversation.
				i.conn.store.updateTitleByChat(chatID, nc.Name)
			}
		}
	}
	return &mautrix.RespSendEvent{EventID: id.EventID("$" + hashShort("state", string(roomID), string(eventType.Type), stateKey))}, nil
}

// ---- profile ----

func (i *localIntent) SetDisplayName(ctx context.Context, name string) error {
	if i.ghostID != "" {
		i.conn.setGhostName(i.ghostID, name)
	}
	return nil
}
func (i *localIntent) SetAvatarURL(ctx context.Context, avatarURL id.ContentURIString) error { return nil }
func (i *localIntent) SetExtraProfileMeta(ctx context.Context, data any) error              { return nil }
func (i *localIntent) SetProfile(ctx context.Context, data any) error                       { return nil }

// ---- rooms ----

func (i *localIntent) CreateRoom(ctx context.Context, req *mautrix.ReqCreateRoom) (id.RoomID, error) {
	roomID := req.BeeperLocalRoomID
	if roomID == "" {
		roomID = id.RoomID("!" + hashShort("room", req.Name, req.Topic) + ":" + localServerName)
	}
	chatID, ok := i.conn.store.chatForRoom(roomID)
	if !ok {
		chatID = string(roomID)
		i.conn.store.rememberRoom(roomID, chatID)
	}
	if i.conn.net.SkipChat(chatID) { // network non-chats (WhatsApp Status/channels, …)
		return roomID, nil
	}
	// Conversations are MESSAGE-GATED: CreateRoom no longer inserts a row. During a
	// backfill the bridge creates a portal for EVERY chat (incl. empty ones with no
	// history), which showed up as nameless "?" conversations. A row is now created
	// lazily by the first SendMessage (below), so only chats with real content appear.
	// The name still lands: resolveChatTitle reads it from the bridge's portal table.
	i.conn.log.Debug().Str("room", string(roomID)).Str("chat", chatID).Str("name", req.Name).Msg("room created (conversation deferred to first message)")
	return roomID, nil
}

func (i *localIntent) DeleteRoom(ctx context.Context, roomID id.RoomID, puppetsOnly bool) error { return nil }
func (i *localIntent) EnsureJoined(ctx context.Context, roomID id.RoomID, params ...bridgev2.EnsureJoinedParams) error {
	return nil
}
func (i *localIntent) EnsureInvited(ctx context.Context, roomID id.RoomID, userID id.UserID) error {
	return nil
}
func (i *localIntent) TagRoom(ctx context.Context, roomID id.RoomID, tag event.RoomTag, isTagged bool) error {
	return nil
}
func (i *localIntent) MuteRoom(ctx context.Context, roomID id.RoomID, until time.Time) error { return nil }

// ---- read / typing ----

func (i *localIntent) MarkRead(ctx context.Context, roomID id.RoomID, eventID id.EventID, ts time.Time) error {
	return nil
}
func (i *localIntent) MarkUnread(ctx context.Context, roomID id.RoomID, unread bool) error { return nil }
func (i *localIntent) MarkTyping(ctx context.Context, roomID id.RoomID, typingType bridgev2.TypingType, timeout time.Duration) error {
	return nil
}

// ---- media ----

func (i *localIntent) UploadMedia(ctx context.Context, roomID id.RoomID, data []byte, fileName, mimeType string) (id.ContentURIString, *event.EncryptedFileInfo, error) {
	mediaID := hashShort("media", string(data[:min(len(data), 4096)]), fileName)
	path := filepath.Join(mediaDir(), mediaID)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", nil, err
	}
	uri, err := i.conn.GenerateContentURI(ctx, networkid.MediaID(mediaID))
	return uri, nil, err
}

func (i *localIntent) UploadMediaStream(ctx context.Context, roomID id.RoomID, size int64, requireFile bool, cb bridgev2.FileStreamCallback) (id.ContentURIString, *event.EncryptedFileInfo, error) {
	tmp, err := os.CreateTemp(mediaDir(), "stream-*")
	if err != nil {
		return "", nil, err
	}
	defer os.Remove(tmp.Name())
	res, err := cb(tmp)
	if err != nil {
		tmp.Close()
		return "", nil, err
	}
	tmp.Close()
	data, err := os.ReadFile(tmp.Name())
	if err != nil {
		return "", nil, err
	}
	name := ""
	if res != nil {
		name = res.FileName
	}
	return i.UploadMedia(ctx, roomID, data, name, "")
}

func (i *localIntent) DownloadMedia(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error) {
	mediaID, err := i.conn.ParseContentURI(ctx, uri)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(filepath.Join(mediaDir(), string(mediaID)))
}

func (i *localIntent) DownloadMediaToFile(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo, writable bool, callback func(*os.File) error) error {
	data, err := i.DownloadMedia(ctx, uri, file)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(mediaDir(), "dl-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := io.Copy(tmp, bytes.NewReader(data)); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		tmp.Close()
		return err
	}
	err = callback(tmp)
	tmp.Close()
	return err
}

// ---- read-back (the path the plan says the spike must exercise) ----

func (i *localIntent) GetEvent(ctx context.Context, roomID id.RoomID, eventID id.EventID) (*event.Event, error) {
	e, ok := i.conn.store.getEvent(eventID)
	if !ok {
		return nil, fmt.Errorf("event %s not found", eventID)
	}
	return &event.Event{
		Type:     e.Type,
		Sender:   e.Sender,
		RoomID:   e.RoomID,
		ID:       eventID,
		Content:  *e.Content,
		StateKey: nil,
	}, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
