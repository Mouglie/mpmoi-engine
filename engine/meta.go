package main

// meta.go — the Meta (Instagram + Messenger) Network impls.
//
// Meta is TWO networks in our model, each its own process/bridge-DB/login (same
// shape as Signal), and login is COOKIE-based, not QR. They share all id/chat
// classification (metaBase) but use DIFFERENT bridgev2 connectors:
//
//   - Messenger/Facebook → the messagix `MetaConnector` (mode-specific). This is
//     the messagix path's real target and works.
//   - Instagram → the DEDICATED `igconnector.IGConnector` (backed by the instameow
//     client). MetaConnector's Instagram MODE uses the abandoned messagix/DGW path
//     whose realtime websocket Meta rejects with close 4003 ("Unauthorized 401" —
//     it even sends the Facebook auth-type). igconnector is upstream's maintained
//     Instagram bridge (cmd/mautrix-instagram) with an IG-correct realtime
//     transport (streamcontroller/mqttbypass, Facebook:false). Login is the SAME
//     cookie flow, so the engine's Connect path (driveRequestLogin cookie branch)
//     is unchanged. See docs/multi-network-supervisor-plan.md Phase 4.
//
// Meta chat ids are numeric thread FBIDs, which are NOT the peer's user id, so a
// DM's peer can't be derived from the chat id. We rely on the bridge-set portal
// name for titles (IsGroup returns true so resolveChatTitle always uses it), and
// direction still works via the message sender id vs SelfIDs.
//
// Live login needs cookies harvested from the user's authed FB/IG browser
// session, fed in via MPMOI_META_COOKIES (JSON object of cookie name→value) or the
// desktop's embedded login window (matrix_login_requests.credential).

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	metaconnector "go.mau.fi/mautrix-meta/pkg/connector"
	igconnector "go.mau.fi/mautrix-meta/pkg/igconnector"
	metatypes "go.mau.fi/mautrix-meta/pkg/messagix/types"
)

// metaBase holds the id/classification behaviour shared by both Meta networks;
// only the connector + login flow differ (see metaNetwork / igNetwork below).
type metaBase struct {
	id       string // product network enum: "instagram" | "messenger"
	flowID   string // Meta cookie login flow id
	cmdPfx   string
	ghostPfx string
	suffix   string // display-name suffix the bridge appends, e.g. " (IG)"
}

func (n metaBase) ID() string            { return n.id }
func (n metaBase) CommandPrefix() string { return n.cmdPfx }
func (n metaBase) GhostPrefix() string   { return n.ghostPfx }
func (n metaBase) LoginFlowID() string   { return n.flowID }

// Meta has no Status/newsletter-style non-chats we need to filter.
func (metaBase) SkipChat(chatID string) bool { return false }

// Meta DM portal ids ARE thread FBIDs (not the peer's user id), so we always take
// the title from the bridge-set portal name (which holds the DM peer name and the
// group name alike) rather than trying to resolve a peer ghost from the chat id.
func (metaBase) IsGroup(chatID string) bool { return true }

// IsRoutable: a Meta chat id is a numeric thread FBID; reject old Synapse
// room-id-keyed rows ("!room:server").
func (metaBase) IsRoutable(chatID string) bool {
	return chatID != "" && !strings.HasPrefix(chatID, "!")
}

// PeerGhostID is unused for Meta (IsGroup is always true) but must satisfy the
// interface; there's no reliable peer id in a thread FBID.
func (metaBase) PeerGhostID(chatID string) networkid.UserID { return "" }

func (n metaBase) CleanName(name string) string {
	return strings.TrimSuffix(strings.TrimSpace(name), n.suffix)
}

func (metaBase) Bind(login *bridgev2.UserLogin) NetworkIdentity {
	// login.ID is the numeric FBID string, which is exactly MakeUserID(fbid) —
	// the login's own ghost id. Contact names fall back to the bridge ghost table.
	// (metaid.MakeUserLoginID == MakeUserID, so this holds for igconnector too.)
	return NetworkIdentity{SelfIDs: []networkid.UserID{networkid.UserID(login.ID)}}
}

// metaNetwork = Facebook/Messenger via the messagix MetaConnector (mode-specific).
type metaNetwork struct {
	metaBase
	mode metatypes.Platform
}

func messengerNetwork() metaNetwork {
	return metaNetwork{
		metaBase: metaBase{id: "messenger", flowID: metaconnector.FlowIDMessengerCookies, cmdPfx: "!fb", ghostPfx: "fb", suffix: " (FB)"},
		mode:     metatypes.Messenger,
	}
}

var _ Network = metaNetwork{}

func (n metaNetwork) NewConnector() (bridgev2.NetworkConnector, error) {
	c := &metaconnector.MetaConnector{}
	if err := yaml.Unmarshal([]byte(metaconnector.ExampleConfig), &c.Config); err != nil {
		return nil, fmt.Errorf("seed meta config: %w", err)
	}
	// Pin this connector to one mode (Messenger).
	c.Config.Mode = n.mode
	c.Config.RawMode = strings.ToLower(n.id)
	return c, nil
}

// igNetwork = Instagram via the dedicated igconnector (instameow client).
type igNetwork struct {
	metaBase
}

func instagramNetwork() igNetwork {
	return igNetwork{metaBase: metaBase{id: "instagram", flowID: igconnector.FlowIDInstagramCookies, cmdPfx: "!ig", ghostPfx: "ig", suffix: " (IG)"}}
}

var _ Network = igNetwork{}

func (igNetwork) NewConnector() (bridgev2.NetworkConnector, error) {
	c := &igconnector.IGConnector{}
	// Seed the config (compiles the displayname template in PostProcess — an
	// unseeded config would panic when the bridge formats a ghost name).
	if err := yaml.Unmarshal([]byte(igconnector.ExampleConfig), &c.Config); err != nil {
		return nil, fmt.Errorf("seed instagram config: %w", err)
	}
	return c, nil
}

// Chrome "Copy as cURL" puts cookies in a -b/--cookie flag; Firefox in a
// -H 'Cookie: …' header. Go's RE2 has no backreferences, so match each quote
// style explicitly (cookie values don't contain the surrounding quote char).
var (
	reFlagSingle   = regexp.MustCompile(`(?:^|\s)(?:-b|--cookie)\s+'([^']*)'`)
	reFlagDouble   = regexp.MustCompile(`(?:^|\s)(?:-b|--cookie)\s+"([^"]*)"`)
	reHeaderSingle = regexp.MustCompile(`(?i)'\s*cookie:\s*([^']*)'`)
	reHeaderDouble = regexp.MustCompile(`(?i)"\s*cookie:\s*([^"]*)"`)
)

// parseMetaCookies accepts the MPMOI_META_COOKIES blob in whatever form is
// easiest to copy: a JSON object {"name":"value"}, a full "Copy as cURL" command,
// a raw "Cookie: …" header, or a bare "name=value; name=value" string. Returns
// the cookie map SubmitCookies expects (it picks out the ones it needs).
func parseMetaCookies(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty")
	}
	if strings.HasPrefix(raw, "{") {
		var m map[string]string
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			return nil, fmt.Errorf("invalid JSON cookies: %w", err)
		}
		return m, nil
	}
	cookieStr := extractCookieString(raw)
	if cookieStr == "" {
		return nil, fmt.Errorf("no cookies found — paste a JSON object, a Copy-as-cURL command, or a \"name=value; …\" string")
	}
	m := map[string]string{}
	for _, part := range strings.Split(cookieStr, ";") {
		part = strings.TrimSpace(part)
		eq := strings.IndexByte(part, '=')
		if eq <= 0 {
			continue
		}
		if name := strings.TrimSpace(part[:eq]); name != "" {
			m[name] = strings.TrimSpace(part[eq+1:])
		}
	}
	if len(m) == 0 {
		return nil, fmt.Errorf("no name=value cookie pairs found")
	}
	return m, nil
}

func extractCookieString(raw string) string {
	for _, re := range []*regexp.Regexp{reFlagSingle, reFlagDouble, reHeaderSingle, reHeaderDouble} {
		if m := re.FindStringSubmatch(raw); m != nil {
			return m[1]
		}
	}
	if low := strings.ToLower(raw); strings.HasPrefix(low, "cookie:") {
		return strings.TrimSpace(raw[len("cookie:"):])
	}
	// Already a bare cookie string (has pairs, isn't a cURL command).
	if strings.Contains(raw, "=") && !strings.Contains(strings.ToLower(raw), "curl ") {
		return raw
	}
	return ""
}
