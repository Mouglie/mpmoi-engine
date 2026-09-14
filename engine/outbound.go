package main

// outbound.go — the mpmoi → WhatsApp path. The web writes a user's message as an
// outgoing/pending row in the product DB (same contract as the old matrix-worker
// outbound.ts). We poll for those and deliver them by feeding a synthesized
// Matrix event into the bridge, which relays it to WhatsApp via the network side.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func runOutbound(ctx context.Context, br *bridgev2.Bridge, conn *LocalConnector, store *productStore, loginID string, senderMXID id.UserID, log zerolog.Logger) {
	log.Info().Str("login", loginID).Str("network", conn.net.ID()).Msg("outbound poller started (mpmoi → network)")
	backfillOutboundIDs(ctx, br, store, log)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	tick := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			drainOutbound(ctx, br, conn, store, loginID, senderMXID, log)
			drainReactionIntents(ctx, br, conn, store, loginID, senderMXID, log)
			// Backfill delivers group messages over time; give nameless groups a
			// member-derived title once their senders are in. Every ~10s (cheap; a
			// no-op once titled), not every tick.
			if tick%5 == 0 {
				store.reconcileGroupTitles()
			}
			tick++
		}
	}
}

func drainReactionIntents(ctx context.Context, br *bridgev2.Bridge, conn *LocalConnector, store *productStore, loginID string, senderMXID id.UserID, log zerolog.Logger) {
	intents, err := store.pendingReactionIntents()
	if err != nil {
		log.Debug().Err(err).Msg("outbound: reaction intents query failed")
		return
	}
	for _, it := range intents {
		if !conn.net.IsRoutable(it.RemoteChatID) || it.RemoteMsgID == "" {
			_ = store.deleteReactionIntent(it.ID)
			continue
		}
		portalKey := networkid.PortalKey{ID: networkid.PortalID(it.RemoteChatID), Receiver: networkid.UserLoginID(loginID)}
		roomID := conn.GenerateDeterministicRoomID(portalKey)
		// The reaction's target must carry the EXACT Matrix event id the bridge
		// stored for that message — handleMatrixReaction resolves it with
		// GetPartByMXID. Reconstructing it via GenerateDeterministicEventID does
		// NOT match what the bridge stored (our own sends live under
		// "$mpmoi-out-<rowID>"), which is why mpmoi→WhatsApp reactions never
		// reached WhatsApp. Look the real MXID up from the bridge's message store.
		targetEventID := resolveTargetMXID(ctx, br, it.RemoteMsgID, loginID)
		if targetEventID == "" {
			// Message not in the bridge store yet (e.g. our own send still being
			// confirmed). Leave the intent so a later tick retries.
			log.Debug().Str("chat", it.RemoteChatID).Str("msg", truncate(it.RemoteMsgID, 40)).
				Msg("outbound reaction: target not in bridge db yet — will retry")
			continue
		}
		reactionEvtID := id.EventID("$mpmoi-rxn-" + it.MessageRowID)

		if it.Emoji != "" {
			evt := &event.Event{
				Type:      event.EventReaction,
				Sender:    senderMXID,
				RoomID:    roomID,
				ID:        reactionEvtID,
				Timestamp: time.Now().UnixMilli(),
				Content: event.Content{Parsed: &event.ReactionEventContent{RelatesTo: event.RelatesTo{
					Type:    event.RelAnnotation,
					EventID: targetEventID,
					Key:     it.Emoji,
				}}},
			}
			br.QueueMatrixEvent(ctx, evt)
			_ = store.applyReactionRow(it.MessageRowID, it.Emoji, true)
			log.Info().Str("emoji", it.Emoji).Str("chat", it.RemoteChatID).Msg("→ outbound reaction")
		} else {
			// Removal: redact our earlier reaction event.
			emoji, _ := store.chosenEmojiForRow(it.MessageRowID)
			evt := &event.Event{
				Type:      event.EventRedaction,
				Sender:    senderMXID,
				RoomID:    roomID,
				ID:        id.EventID("$mpmoi-unrxn-" + it.MessageRowID),
				Redacts:   reactionEvtID,
				Timestamp: time.Now().UnixMilli(),
				Content:   event.Content{Parsed: &event.RedactionEventContent{Redacts: reactionEvtID}},
			}
			br.QueueMatrixEvent(ctx, evt)
			_ = store.applyReactionRow(it.MessageRowID, emoji, false)
			log.Info().Str("emoji", emoji).Str("chat", it.RemoteChatID).Msg("→ outbound reaction removed")
		}
		_ = store.deleteReactionIntent(it.ID)
	}
}

// resolveTargetMXID returns the Matrix event id the bridge stored for a message,
// given the product row's remote_message_id. This is what a reaction/redaction
// must point at (the bridge looks the target up by MXID). Two cases:
//   - our own sends not yet WA-id-backfilled still carry the synthetic
//     "$mpmoi-out-<rowID>" id, which is exactly the MXID the bridge stored;
//   - everything else (inbound msgs, backfilled own sends) is keyed by the
//     network message id, so look the part up by id and return its real MXID.
//
// Returns "" if the message isn't in the bridge store (caller retries later).
func resolveTargetMXID(ctx context.Context, br *bridgev2.Bridge, remoteMsgID, loginID string) id.EventID {
	if strings.HasPrefix(remoteMsgID, "$") {
		if m, err := br.DB.Message.GetPartByMXID(ctx, id.EventID(remoteMsgID)); err == nil && m != nil {
			return m.MXID
		}
	}
	if m, err := br.DB.Message.GetFirstPartByID(ctx, networkid.UserLoginID(loginID), networkid.MessageID(remoteMsgID)); err == nil && m != nil {
		return m.MXID
	}
	return ""
}

func drainOutbound(ctx context.Context, br *bridgev2.Bridge, conn *LocalConnector, store *productStore, loginID string, senderMXID id.UserID, log zerolog.Logger) {
	rows, err := store.pendingOutbound()
	if err != nil {
		log.Debug().Err(err).Msg("outbound: query failed")
		return
	}
	for _, r := range rows {
		if !conn.net.IsRoutable(r.RemoteChatID) {
			// Old Synapse-build room-id-keyed conversation — not routable here.
			_ = store.markOutbound(r.ID, "failed", "")
			continue
		}
		// Skip rows already handed to the bridge and awaiting a delivery result,
		// so we don't send the same message twice while the first attempt is in
		// flight. The row flips to sent/failed in SendMessageStatus.
		if !store.markInflight(r.ID) {
			continue
		}
		portalKey := networkid.PortalKey{
			ID:       networkid.PortalID(r.RemoteChatID),
			Receiver: networkid.UserLoginID(loginID),
		}
		roomID := conn.GenerateDeterministicRoomID(portalKey)
		evtID := id.EventID("$mpmoi-out-" + r.ID)

		content := &event.MessageEventContent{MsgType: event.MsgText, Body: r.Body}
		media := firstMedia(r.Attachments)
		if media != nil {
			mc, err := buildMediaContent(ctx, conn, store, media, r.Body)
			if err != nil {
				log.Warn().Err(err).Str("chat", r.RemoteChatID).Msg("outbound media prep failed — leaving pending")
				store.clearInflight(r.ID) // let the next tick retry
				continue
			}
			content = mc
		}

		evt := &event.Event{
			Type:      event.EventMessage,
			Sender:    senderMXID,
			RoomID:    roomID,
			ID:        evtID,
			Timestamp: time.Now().UnixMilli(),
			Content:   event.Content{Parsed: content},
		}
		// QueueMatrixEvent only buffers the event (PortalEventBuffer > 0), so its
		// result is "queued", NOT "delivered". The row flips to sent/failed later
		// in LocalConnector.SendMessageStatus once the portal loop actually relays
		// it to WhatsApp. This is the fix for messages showing "sent" in the UI
		// while never reaching WhatsApp.
		br.QueueMatrixEvent(ctx, evt)
		// Backfill the real WhatsApp id once the bridge records the send (and, as a
		// safety net, fail the row if the bridge never confirms it).
		go resolveOutboundWAID(ctx, br, store, r.ID, evtID, log)
		log.Info().
			Str("chat", r.RemoteChatID).
			Str("body", truncate(r.Body, 40)).
			Msg("→ outbound queued")
	}
}

// firstMedia returns the first non-link attachment (image/video/audio/file).
func firstMedia(atts []mediaAttachment) *mediaAttachment {
	for i := range atts {
		if atts[i].Kind != "link" && atts[i].StoragePath != "" {
			return &atts[i]
		}
	}
	return nil
}

// buildMediaContent reads the bucket file, stages it where the connector's
// DownloadMedia can fetch it, and returns an m.image/… content the bridge sends.
func buildMediaContent(ctx context.Context, conn *LocalConnector, store *productStore, att *mediaAttachment, body string) (*event.MessageEventContent, error) {
	data, err := os.ReadFile(filepath.Join(store.storageDir, filepath.FromSlash(att.StoragePath)))
	if err != nil {
		return nil, err
	}
	mediaID := hashShort("out", att.StoragePath)
	if err := os.WriteFile(filepath.Join(mediaDir(), mediaID), data, 0o644); err != nil {
		return nil, err
	}
	uri, err := conn.GenerateContentURI(ctx, networkid.MediaID(mediaID))
	if err != nil {
		return nil, err
	}
	// Body carries the caption (empty = no caption); FileName is the file name.
	// The WhatsApp connector treats Body as the caption when it differs from FileName.
	return &event.MessageEventContent{
		MsgType:  kindToMsgType(att.Kind),
		Body:     body,
		URL:      uri,
		FileName: att.Name,
		Info:     &event.FileInfo{MimeType: att.MimeType, Size: len(data)},
	}, nil
}

func kindToMsgType(kind string) event.MessageType {
	switch kind {
	case "image":
		return event.MsgImage
	case "video":
		return event.MsgVideo
	case "audio":
		return event.MsgAudio
	default:
		return event.MsgFile
	}
}

// resolveOutboundWAID waits for the bridge to record the sent message, then
// backfills the product row's remote_message_id with the real WhatsApp id.
func resolveOutboundWAID(ctx context.Context, br *bridgev2.Bridge, store *productStore, rowID string, mxid id.EventID, log zerolog.Logger) {
	for i := 0; i < 20; i++ {
		time.Sleep(500 * time.Millisecond)
		dbMsg, err := br.DB.Message.GetPartByMXID(ctx, mxid)
		if err == nil && dbMsg != nil && dbMsg.ID != "" {
			// The bridge recorded the send → it reached WhatsApp. Make sure the row
			// is marked sent (in case SendMessageStatus was missed) and backfill the
			// real WhatsApp id so inbound reactions to our own message can find it.
			_ = store.markOutbound(rowID, "sent", "")
			store.clearInflight(rowID)
			if err := store.setRemoteMessageID(rowID, string(dbMsg.ID)); err == nil {
				log.Debug().Str("wa_id", string(dbMsg.ID)).Msg("outbound: backfilled real WhatsApp id")
			}
			return
		}
	}
	// 10s and the bridge never recorded the message: the send did not complete.
	// If SendMessageStatus already resolved it (success or failure) the row is no
	// longer in flight and we leave it alone; otherwise fail it so the UI stops
	// showing a message that never reached WhatsApp.
	if store.isInflight(rowID) {
		_ = store.markOutbound(rowID, "failed", "")
		store.clearInflight(rowID)
		log.Error().Str("row", rowID).Msg("outbound: no delivery confirmation after 10s — marked failed")
	}
}

// backfillOutboundIDs fixes messages sent before this resolver existed (their
// remote_message_id is still "$mpmoi-out-…").
func backfillOutboundIDs(ctx context.Context, br *bridgev2.Bridge, store *productStore, log zerolog.Logger) {
	rows, err := store.mpmoiSentRows()
	if err != nil {
		return
	}
	n := 0
	for _, r := range rows {
		dbMsg, err := br.DB.Message.GetPartByMXID(ctx, id.EventID(r[1]))
		if err == nil && dbMsg != nil && dbMsg.ID != "" {
			if store.setRemoteMessageID(r[0], string(dbMsg.ID)) == nil {
				n++
			}
		}
	}
	if n > 0 {
		log.Info().Int("updated", n).Msg("outbound: backfilled real WhatsApp ids for past mpmoi sends")
	}
}
