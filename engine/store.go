package main

// store.go — the product-DB sink for the Stage 0 spike.
//
// This is the whole point of the spike: turn the bridge's Matrix "sink" calls
// (CreateRoom / SendMessage / SetDisplayName) directly into rows in the mpmoi
// product database (public.conversations + public.messages), with NO Synapse
// and NO matrix-worker in between.
//
// It also keeps two in-memory maps that the connector needs to answer read-back
// calls (GetEvent) and to map a deterministic Matrix room ID back to the remote
// chat it represents. In-memory is fine for a single-session spike; Stage 1
// persists these (the bridge already owns durable state, so we mostly need the
// room->chat mapping which we can rebuild from GenerateDeterministicRoomID).

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"

	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// storedEvent is the minimum we need to satisfy the bridge's GetEvent read-back
// (used for replies/edits — the "read-back path" the plan says the spike must
// exercise so we discover any dependency now, not in Stage 1).
type storedEvent struct {
	RoomID  id.RoomID
	Sender  id.UserID
	Type    event.Type
	Content *event.Content
	TS      time.Time
}

// productStore writes to the mpmoi product Postgres and holds the spike's
// in-memory bookkeeping.
type productStore struct {
	db *sql.DB

	// identity of the single local user / space / account these rows belong to.
	// Seeded from env (see main.go). In the real engine these come from the
	// signed-in user + the connected_accounts row for this WhatsApp login.
	userID    string
	spaceID   string
	accountID string
	network   string // must match the public.network enum, e.g. "whatsapp"
	// storageDir is the on-device media bucket root (…/storage/telegram-media).
	// Media is written to {storageDir}/{userId}/{convId}/{file}; the web signs the
	// bucket-relative storagePath on read.
	storageDir string

	mu sync.RWMutex
	// deterministic roomID -> remote chat id (networkid.PortalID as string).
	roomToChat map[id.RoomID]string
	// deterministic roomID -> product conversations.id (uuid).
	roomToConv map[id.RoomID]string
	// eventID -> stored event, for GetEvent read-back.
	events map[id.EventID]*storedEvent
	// the logged-in user's own network user ids (WhatsApp gives us TWO: the phone
	// JID and the LID — self-sent messages arrive under either, so both count as
	// "me"). Mirrors mautrix-whatsapp's own IsFromMe check.
	selfIDs map[networkid.UserID]bool
	// canonical remote_chat_id for the user's own "message yourself" chat. WhatsApp
	// exposes the self-chat under BOTH the phone JID and the LID, which would otherwise
	// create two "Note to self" conversation rows — so every self-chat id is rewritten
	// to this one canonical (phone-JID) id before it touches the conversations table.
	selfChatID string

	// outbound rows currently handed to the bridge and awaiting a delivery result
	// (SendMessageStatus). The 2s poller skips these so a message isn't re-sent
	// while the first attempt is still in flight. Cleared when the bridge reports
	// success or failure.
	inflight map[string]bool

	// peerOf maps a 1:1 chat id to the other party's ghost id (network-specific;
	// set from Network.PeerGhostID at wiring). Used by isSelfChat.
	peerOf func(chatID string) networkid.UserID
}

func newProductStore(dsn, userID, spaceID, accountID, network, storageDir string) (*productStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open product db: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping product db: %w", err)
	}
	return &productStore{
		db:         db,
		userID:     userID,
		spaceID:    spaceID,
		accountID:  accountID,
		network:    network,
		storageDir: storageDir,
		roomToChat: make(map[id.RoomID]string),
		roomToConv: make(map[id.RoomID]string),
		events:     make(map[id.EventID]*storedEvent),
		selfIDs:    make(map[networkid.UserID]bool),
		inflight:   make(map[string]bool),
	}, nil
}

// accountSpaceID returns the space_id recorded on this store's connected_account,
// so conversations are keyed to the ACCOUNT's Space (a 2nd Space like "reseaux"),
// not the engine's ONE configured/default (personal) space. One network = one
// account = one space, so the account's space is the correct per-conversation
// space. Empty string if the account has no space (caller keeps the default).
func (s *productStore) accountSpaceID(accountID string) (string, error) {
	var sid sql.NullString
	err := s.db.QueryRow(
		`select space_id::text from public.connected_accounts where id = $1`,
		accountID,
	).Scan(&sid)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return sid.String, nil
}

// markInflight records that an outbound row has been queued to the bridge and is
// awaiting a delivery result. Returns false if it was already in flight (so the
// caller skips re-sending it).
func (s *productStore) markInflight(rowID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[rowID] {
		return false
	}
	s.inflight[rowID] = true
	return true
}

func (s *productStore) isInflight(rowID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inflight[rowID]
}

func (s *productStore) clearInflight(rowID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, rowID)
}

func (s *productStore) rememberRoom(roomID id.RoomID, chatID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.roomToChat[roomID] = chatID
}

func (s *productStore) chatForRoom(roomID id.RoomID) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.roomToChat[roomID]
	return c, ok
}

func (s *productStore) setSelfIDs(uids ...networkid.UserID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, uid := range uids {
		if uid != "" {
			s.selfIDs[uid] = true
		}
	}
}

// isSelf reports whether a sender network id is the logged-in user (phone or LID).
func (s *productStore) isSelf(uid networkid.UserID) bool {
	if uid == "" {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.selfIDs[uid]
}

// isSelfChat reports whether a 1:1 chat id is the user's own "message yourself"
// chat: its peer ghost id is one of the login's own ids.
func (s *productStore) isSelfChat(chatID string) bool {
	if s.peerOf == nil {
		return false
	}
	return s.isSelf(s.peerOf(chatID))
}

// setSelfChatID records the canonical (phone-JID) remote id of the self-chat.
func (s *productStore) setSelfChatID(id string) {
	s.mu.Lock()
	s.selfChatID = id
	s.mu.Unlock()
}

// canonChatID collapses every representation of the self-chat (phone JID + LID)
// onto the one canonical id so both map to a SINGLE "Note to self" conversation.
// Non-self chats pass through unchanged. Both the write (upsertConversation) and
// the read (conversationIDByChat) sides call this so lookups match inserts.
func (s *productStore) canonChatID(chatID string) string {
	s.mu.RLock()
	self := s.selfChatID
	s.mu.RUnlock()
	if self != "" && chatID != self && s.isSelfChat(chatID) {
		return self
	}
	return chatID
}

// upsertConversation creates/updates a public.conversations row keyed on
// (account_id, remote_chat_id) and caches its uuid. Called from CreateRoom and
// lazily from SendMessage (in case a message arrives before CreateRoom).
func (s *productStore) upsertConversation(roomID id.RoomID, chatID, title, handle string, isGroup bool, ts time.Time) (string, error) {
	chatID = s.canonChatID(chatID) // collapse the self-chat's phone-JID + LID ids into one
	s.mu.Lock()
	if convID, ok := s.roomToConv[roomID]; ok {
		s.mu.Unlock()
		// keep title / handle fresh
		if title != "" {
			_, _ = s.db.Exec(`update public.conversations set title = $1 where id = $2`, title, convID)
		}
		if handle != "" {
			_, _ = s.db.Exec(`update public.conversations set handle = $1 where id = $2 and coalesce(handle,'') <> $1`, handle, convID)
		}
		return convID, nil
	}
	s.mu.Unlock()

	var convID string
	err := s.db.QueryRow(
		`insert into public.conversations
		   (user_id, account_id, space_id, network, remote_chat_id, title, handle, is_group, last_message_at)
		 values ($1, $2, $3, $4::public.network, $5, $6, $7, $8, $9)
		 on conflict (account_id, remote_chat_id) do update
		   set title = case when excluded.title <> '' then excluded.title else public.conversations.title end,
		       handle = case when excluded.handle <> '' then excluded.handle else public.conversations.handle end,
		       is_group = excluded.is_group,
		       space_id = excluded.space_id
		 returning id`,
		s.userID, s.accountID, s.spaceID, s.network, chatID, title, nullStr(handle), isGroup, nullTime(ts),
	).Scan(&convID)
	if err != nil {
		return "", fmt.Errorf("upsert conversation: %w", err)
	}
	s.mu.Lock()
	s.roomToConv[roomID] = convID
	s.mu.Unlock()
	return convID, nil
}

// reconcileGroupTitles gives nameless group conversations a title built from the
// members who've spoken (the distinct incoming senders, first three) — like Messenger
// shows "Nathan, Loïs" for an unnamed group instead of a bare "?". Called periodically
// from the outbound poller so it fills in as backfill delivers messages; it's a no-op
// once a group has a real title (Meta-set name or this fallback). Own messages
// ("You"/outgoing) are excluded by the direction filter.
func (s *productStore) reconcileGroupTitles() {
	_, _ = s.db.Exec(
		`update public.conversations c
		    set title = t.names
		   from (
		     select m.conversation_id,
		            array_to_string((array_agg(distinct m.sender order by m.sender))[1:3], ', ') as names
		       from public.messages m
		       join public.conversations cc on cc.id = m.conversation_id
		      where cc.account_id = $1 and cc.is_group and coalesce(cc.title,'') = ''
		        and m.direction = 'incoming' and coalesce(m.sender,'') <> ''
		      group by m.conversation_id
		   ) t
		  where c.id = t.conversation_id and coalesce(c.title,'') = '' and coalesce(t.names,'') <> ''`,
		s.accountID,
	)
}

// upsertMessage writes a public.messages row (idempotent on
// (conversation_id, remote_message_id), which also makes edits/redelivery update
// the existing row) and bumps the conversation preview.
func (s *productStore) upsertMessage(convID, remoteMsgID, sender, direction, body string, ts time.Time, attachments any) error {
	att, _ := json.Marshal(attachments)
	if len(att) == 0 || string(att) == "null" {
		att = []byte("[]")
	}
	_, err := s.db.Exec(
		`insert into public.messages
		   (user_id, conversation_id, direction, sender, body, sent_at, attachments, remote_message_id, reactions)
		 values ($1, $2, $3::public.message_direction, $4, $5, $6, $7::jsonb, $8, '[]'::jsonb)
		 on conflict (conversation_id, remote_message_id) do update
		   set body = excluded.body,
		       sender = excluded.sender,
		       sent_at = excluded.sent_at,
		       attachments = case when excluded.attachments <> '[]'::jsonb
		                          then excluded.attachments else public.messages.attachments end`,
		s.userID, convID, direction, sender, body, ts, string(att), nullStr(remoteMsgID),
	)
	if err != nil {
		return fmt.Errorf("upsert message: %w", err)
	}
	// The web reads conversations.last_message_preview for the chat-list preview.
	preview := body
	if preview == "" && string(att) != "[]" {
		preview = mediaPreview(att)
	}
	_, _ = s.db.Exec(
		`update public.conversations set last_message_at = $1, last_message_preview = $2 where id = $3`,
		ts, preview, convID,
	)
	return nil
}

// mediaPreview derives a human-friendly chat-list preview for a media-only message
// (no caption) from the first attachment's kind — nicer than a bare "[media]".
func mediaPreview(attJSON []byte) string {
	var atts []struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(attJSON, &atts); err != nil || len(atts) == 0 {
		return "[media]"
	}
	switch atts[0].Kind {
	case "sticker":
		return "Sticker"
	case "image":
		return "Photo"
	case "video":
		return "Video"
	case "audio":
		return "Voice message"
	case "file":
		return "File"
	case "link":
		return "Link"
	default:
		return "[media]"
	}
}

type convRow struct {
	ID           string
	RemoteChatID string
	Title        string
}

// listConversations returns this account's conversations (for the title reconcile).
func (s *productStore) listConversations() ([]convRow, error) {
	rows, err := s.db.Query(
		`select id, remote_chat_id, coalesce(title,'') from public.conversations where account_id = $1`,
		s.accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []convRow
	for rows.Next() {
		var c convRow
		if err := rows.Scan(&c.ID, &c.RemoteChatID, &c.Title); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *productStore) updateConversationTitle(id, title string) error {
	_, err := s.db.Exec(`update public.conversations set title = $1 where id = $2`, title, id)
	return err
}

// convIDForChat resolves a conversation uuid from its remote chat id.
func (s *productStore) convIDForChat(chatID string) (string, error) {
	chatID = s.canonChatID(chatID) // match the canonical self-chat id used on insert
	var id string
	err := s.db.QueryRow(
		`select id from public.conversations where account_id = $1 and remote_chat_id = $2`,
		s.accountID, chatID,
	).Scan(&id)
	return id, err
}

// updateTitleByChat sets a conversation's title WITHOUT creating it — used by the
// room-name state handler so a rename never conjures an empty (message-less) row.
func (s *productStore) updateTitleByChat(chatID, title string) {
	if title == "" {
		return
	}
	_, _ = s.db.Exec(
		`update public.conversations set title = $1 where account_id = $2 and remote_chat_id = $3`,
		title, s.accountID, s.canonChatID(chatID),
	)
}

type reactionEntry struct {
	Emoji  string `json:"emoji"`
	Count  int    `json:"count"`
	Chosen bool   `json:"chosen"`
}

// applyReaction merges an incoming reaction into a target message's reactions
// jsonb (shape {emoji,count,chosen} that the web inbox renders). chosen marks the
// signed-in user's own reaction.
func (s *productStore) applyReaction(convID, targetRemoteMsgID, emoji string, chosen bool) error {
	if emoji == "" {
		return nil
	}
	var raw []byte
	err := s.db.QueryRow(
		`select reactions from public.messages where conversation_id = $1 and remote_message_id = $2`,
		convID, targetRemoteMsgID,
	).Scan(&raw)
	if err != nil {
		return err // target not mirrored yet — skip
	}
	var reactions []reactionEntry
	_ = json.Unmarshal(raw, &reactions)
	found := false
	for i := range reactions {
		if reactions[i].Emoji == emoji {
			reactions[i].Count++
			if chosen {
				reactions[i].Chosen = true
			}
			found = true
			break
		}
	}
	if !found {
		reactions = append(reactions, reactionEntry{Emoji: emoji, Count: 1, Chosen: chosen})
	}
	next, _ := json.Marshal(reactions)
	_, err = s.db.Exec(
		`update public.messages set reactions = $1::jsonb where conversation_id = $2 and remote_message_id = $3`,
		string(next), convID, targetRemoteMsgID,
	)
	return err
}

// removeReaction decrements/removes an emoji from a target message's reactions.
func (s *productStore) removeReaction(convID, targetRemoteMsgID, emoji string, wasSelf bool) error {
	if emoji == "" {
		return nil
	}
	var raw []byte
	err := s.db.QueryRow(
		`select reactions from public.messages where conversation_id = $1 and remote_message_id = $2`,
		convID, targetRemoteMsgID,
	).Scan(&raw)
	if err != nil {
		return err
	}
	var reactions []reactionEntry
	_ = json.Unmarshal(raw, &reactions)
	out := make([]reactionEntry, 0, len(reactions))
	for _, r := range reactions {
		if r.Emoji == emoji {
			r.Count--
			if wasSelf {
				r.Chosen = false
			}
			if r.Count <= 0 {
				continue // drop the entry entirely
			}
		}
		out = append(out, r)
	}
	next := []byte("[]")
	if len(out) > 0 {
		next, _ = json.Marshal(out)
	}
	_, err = s.db.Exec(
		`update public.messages set reactions = $1::jsonb where conversation_id = $2 and remote_message_id = $3`,
		string(next), convID, targetRemoteMsgID,
	)
	return err
}

// deleteMessage removes a message row (for remote message deletions).
func (s *productStore) deleteMessage(convID, remoteMsgID string) error {
	_, err := s.db.Exec(
		`delete from public.messages where conversation_id = $1 and remote_message_id = $2`,
		convID, remoteMsgID,
	)
	return err
}

type outboundRow struct {
	ID           string
	Body         string
	RemoteChatID string
	Attachments  []mediaAttachment
}

// pendingOutbound returns user-sent messages (written by the web) awaiting
// delivery to the network.
func (s *productStore) pendingOutbound() ([]outboundRow, error) {
	rows, err := s.db.Query(
		`select m.id, m.body, c.remote_chat_id, m.attachments
		   from public.messages m join public.conversations c on c.id = m.conversation_id
		  where m.direction = 'outgoing' and m.delivery_status = 'pending' and c.account_id = $1
		  order by m.created_at asc limit 20`,
		s.accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []outboundRow
	for rows.Next() {
		var r outboundRow
		var att []byte
		if err := rows.Scan(&r.ID, &r.Body, &r.RemoteChatID, &att); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(att, &r.Attachments)
		out = append(out, r)
	}
	return out, rows.Err()
}

// setRemoteMessageID rewrites a message's remote_message_id (used to replace the
// synthesized "$mpmoi-out-…" id with the real WhatsApp id once known, so inbound
// reactions to our own sends can find the row).
func (s *productStore) setRemoteMessageID(rowID, remoteMsgID string) error {
	if remoteMsgID == "" {
		return nil
	}
	_, err := s.db.Exec(`update public.messages set remote_message_id = $1 where id = $2`, remoteMsgID, rowID)
	return err
}

// mpmoiSentRows lists messages still carrying a synthesized outbound id (for the
// startup WhatsApp-id backfill). Returns rows of (product id, current mxid).
func (s *productStore) mpmoiSentRows() ([][2]string, error) {
	rows, err := s.db.Query(
		`select id, remote_message_id from public.messages
		  where remote_message_id like '$mpmoi-out-%' and user_id = $1`,
		s.userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][2]string
	for rows.Next() {
		var id, mxid string
		if err := rows.Scan(&id, &mxid); err != nil {
			return nil, err
		}
		out = append(out, [2]string{id, mxid})
	}
	return out, rows.Err()
}

func (s *productStore) markOutbound(id, status, remoteMsgID string) error {
	if remoteMsgID != "" {
		_, err := s.db.Exec(
			`update public.messages set delivery_status = $1::public.message_delivery_status,
			        remote_message_id = coalesce(remote_message_id, $2) where id = $3`,
			status, remoteMsgID, id,
		)
		return err
	}
	_, err := s.db.Exec(
		`update public.messages set delivery_status = $1::public.message_delivery_status where id = $2`,
		status, id,
	)
	return err
}

type reactionIntent struct {
	ID           string
	MessageRowID string
	Emoji        string // "" = remove
	RemoteMsgID  string
	RemoteChatID string
}

func (s *productStore) pendingReactionIntents() ([]reactionIntent, error) {
	rows, err := s.db.Query(
		`select ri.id, ri.message_id, coalesce(ri.emoji,''), coalesce(m.remote_message_id,''), c.remote_chat_id
		   from public.reaction_intents ri
		   join public.messages m on m.id = ri.message_id
		   join public.conversations c on c.id = m.conversation_id
		  where c.account_id = $1
		  order by ri.created_at asc limit 20`,
		s.accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []reactionIntent
	for rows.Next() {
		var r reactionIntent
		if err := rows.Scan(&r.ID, &r.MessageRowID, &r.Emoji, &r.RemoteMsgID, &r.RemoteChatID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *productStore) deleteReactionIntent(id string) error {
	_, err := s.db.Exec(`delete from public.reaction_intents where id = $1`, id)
	return err
}

// chosenEmojiForRow returns the emoji the user currently has chosen on a message.
func (s *productStore) chosenEmojiForRow(messageRowID string) (string, error) {
	var raw []byte
	if err := s.db.QueryRow(`select reactions from public.messages where id = $1`, messageRowID).Scan(&raw); err != nil {
		return "", err
	}
	var rs []reactionEntry
	_ = json.Unmarshal(raw, &rs)
	for _, r := range rs {
		if r.Chosen {
			return r.Emoji, nil
		}
	}
	return "", nil
}

// applyReactionRow adds/removes the user's own reaction on a message row (by id),
// keeping the reactions jsonb in sync with what we send to WhatsApp.
//
// WhatsApp allows exactly ONE reaction per user per message: reacting again
// replaces your previous reaction (WhatsApp does this automatically on its side).
// So on add we first clear any other emoji this user had chosen, then set the new
// one — otherwise mpmoi stacks ❤ 🔥 🥰 while WhatsApp shows only the latest.
func (s *productStore) applyReactionRow(messageRowID, emoji string, add bool) error {
	if emoji == "" {
		return nil
	}
	var raw []byte
	if err := s.db.QueryRow(`select reactions from public.messages where id = $1`, messageRowID).Scan(&raw); err != nil {
		return err
	}
	var rs []reactionEntry
	_ = json.Unmarshal(raw, &rs)
	out := make([]reactionEntry, 0, len(rs)+1)
	found := false
	for _, r := range rs {
		switch {
		case r.Emoji == emoji:
			found = true
			if add {
				if !r.Chosen {
					r.Count++
				}
				r.Chosen = true
			} else {
				r.Count--
				r.Chosen = false
				if r.Count <= 0 {
					continue
				}
			}
		case add && r.Chosen:
			// A different emoji this user previously chose — replaced by the new one.
			r.Count--
			r.Chosen = false
			if r.Count <= 0 {
				continue
			}
		}
		out = append(out, r)
	}
	if add && !found {
		out = append(out, reactionEntry{Emoji: emoji, Count: 1, Chosen: true})
	}
	next := []byte("[]")
	if len(out) > 0 {
		next, _ = json.Marshal(out)
	}
	_, err := s.db.Exec(`update public.messages set reactions = $1::jsonb where id = $2`, string(next), messageRowID)
	return err
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (s *productStore) putEvent(evtID id.EventID, e *storedEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events[evtID] = e
}

func (s *productStore) getEvent(evtID id.EventID) (*storedEvent, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.events[evtID]
	return e, ok
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// mediaAttachment matches the shape the web inbox reads from messages.attachments
// (see apps/matrix-worker/src/lib/media.ts): url is empty until the app signs
// storagePath from the media bucket.
type mediaAttachment struct {
	Kind        string `json:"kind"` // image | video | audio | file
	URL         string `json:"url"`
	StoragePath string `json:"storagePath"`
	MimeType    string `json:"mimeType"`
	Name        string `json:"name,omitempty"`
}

// linkAttachment matches the web's LinkAttachment shape (media.ts).
type linkAttachment struct {
	Kind        string `json:"kind"` // "link"
	URL         string `json:"url"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	SiteName    string `json:"siteName,omitempty"`
	StoragePath string `json:"storagePath,omitempty"`
}

// saveMedia writes media bytes into the on-device bucket at
// {userId}/{convId}/{eventId}.{ext} and returns the attachment record.
func (s *productStore) saveMedia(convID, eventID, kind, mimeType, name string, data []byte) (mediaAttachment, error) {
	if s.storageDir == "" {
		return mediaAttachment{}, fmt.Errorf("no storage dir configured")
	}
	rel := path.Join(s.userID, convID, sanitizeID(eventID)+"."+extForMime(mimeType))
	abs := filepath.Join(s.storageDir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return mediaAttachment{}, err
	}
	if err := os.WriteFile(abs, data, 0o644); err != nil {
		return mediaAttachment{}, err
	}
	return mediaAttachment{Kind: kind, URL: "", StoragePath: rel, MimeType: mimeType, Name: name}, nil
}

var extByMime = map[string]string{
	"image/jpeg": "jpg", "image/png": "png", "image/gif": "gif", "image/webp": "webp",
	"video/mp4": "mp4", "video/quicktime": "mov", "video/webm": "webm",
	"audio/ogg": "ogg", "audio/mpeg": "mp3", "audio/mp4": "m4a", "audio/wav": "wav",
	"application/pdf": "pdf",
}

func extForMime(mime string) string {
	if e, ok := extByMime[mime]; ok {
		return e
	}
	if i := strings.LastIndexByte(mime, '/'); i >= 0 && i+1 < len(mime) {
		if sub := mime[i+1:]; len(sub) <= 5 && !strings.ContainsAny(sub, ".;") {
			return sub
		}
	}
	return "bin"
}

// sanitizeID makes a Matrix event id safe as a filename component.
func sanitizeID(id string) string {
	return strings.NewReplacer("$", "", "/", "_", "\\", "_", ":", "_").Replace(id)
}
