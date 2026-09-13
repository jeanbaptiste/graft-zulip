package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"graftzulip/internal/ap"
	"graftzulip/internal/graft"
	"graftzulip/internal/state"
	"graftzulip/internal/zulip"
)

// zulipAPI is the subset of the Zulip client the bridge needs.
type zulipAPI interface {
	LatestMessages(ctx context.Context, limit int) ([]zulip.Message, error)
	SendMessage(ctx context.Context, streamID int64, topic, content string) (zulip.Message, error)
}

// graftAPI is the subset of the Graft client the bridge needs.
type graftAPI interface {
	Outbox(ctx context.Context, series string) (*ap.OrderedCollection, error)
	ReplyToIssue(ctx context.Context, series, noteURI, content string) error
}

// Options are the security-relevant knobs of a bridge pass.
type Options struct {
	// Explicit maps a conversation key (state.ConversationKey) to a Graft
	// note URI. This is the trusted way to bind a stream+topic to a repo
	// issue.
	Explicit map[string]string
	// AllowTitleMatching enables the heuristic fallback (topic name equals
	// issue title). Off by default.
	AllowTitleMatching bool
	// AllowedStreams, when non-empty, restricts forwarding to these Zulip
	// stream ids.
	AllowedStreams map[int64]bool
	// MaxContentRunes caps a single mirrored message.
	MaxContentRunes int
	// MaxDeliveriesPerPass bounds outbound comments per pass.
	MaxDeliveriesPerPass int
	// BotEmail is the Zulip account the bridge itself posts as
	// (zulip.email). A message sent by this account is always the
	// bridge's own reverse-mirror (see reverseGraftToZulip) — forwarding
	// it back to Graft would create a duplicate comment every time a
	// native Forgejo/Radicle comment gets mirrored out.
	BotEmail string
}

// Bridge reconciles Zulip stream/topic conversations and
// Graft-mirrored repo issues.
type Bridge struct {
	GraftHost string
	Series    []string
	// MaxMsgs bounds how many of the most recent Zulip messages each pass
	// inspects.
	MaxMsgs int
	Opts    Options

	Zulip zulipAPI
	Graft graftAPI
	State *state.State
	Log   *slog.Logger
}

// RunOnce performs one full reconciliation pass.
func (b *Bridge) RunOnce(ctx context.Context) error {
	messages, err := b.Zulip.LatestMessages(ctx, b.maxMsgs())
	if err != nil {
		return fmt.Errorf("zulip latest messages: %w", err)
	}
	notes, err := b.refreshMapping(ctx, messages)
	if err != nil {
		return err
	}
	if err := b.forwardZulipToGraft(ctx, messages); err != nil {
		return err
	}
	return b.reverseGraftToZulip(ctx, notes)
}

// Issues lists the replyable issue/patch notes across the configured
// series, so operators can choose a note URI to bind.
func (b *Bridge) Issues(ctx context.Context) ([]graft.IssueRef, error) {
	var out []graft.IssueRef
	for _, series := range b.Series {
		oc, err := b.Graft.Outbox(ctx, series)
		if err != nil {
			return out, fmt.Errorf("fetch %s outbox: %w", series, err)
		}
		for _, n := range graft.ParseNotes(b.GraftHost, oc) {
			if !n.IsIssueOrPatch() {
				continue
			}
			out = append(out, graft.IssueRef{
				Series:  series,
				EntryID: n.EntryID,
				Kind:    n.Kind,
				Title:   n.Summary,
				NoteURI: n.URI,
				URL:     n.URL,
			})
		}
	}
	return out, nil
}

// Bind links a Zulip stream+topic to a Graft note, validating that the
// note belongs to a configured series and is an issue/patch Graft will
// accept replies to. It persists the mapping and returns an error
// otherwise.
func (b *Bridge) Bind(ctx context.Context, streamID int64, topic, noteURI string) error {
	series, _, ok := ap.ParseNoteURI(b.GraftHost, noteURI)
	if !ok {
		return fmt.Errorf("note_uri %q does not belong to %s", noteURI, b.GraftHost)
	}
	if !b.seriesConfigured(series) {
		return fmt.Errorf("series %q is not configured", series)
	}
	oc, err := b.Graft.Outbox(ctx, series)
	if err != nil {
		return fmt.Errorf("fetch %s outbox: %w", series, err)
	}
	key := state.ConversationKey(streamID, topic)
	for _, n := range graft.ParseNotes(b.GraftHost, oc) {
		if n.URI != noteURI {
			continue
		}
		if !n.IsIssueOrPatch() {
			return fmt.Errorf("note is a %s; Graft only accepts replies to issues and patches", n.Kind)
		}
		if err := b.State.SetConversationNote(key, noteURI); err != nil {
			return err
		}
		if n.URL != "" {
			if err := b.State.SetIssueConversation(series, n.URL, key); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("note %q not found in the %s outbox (it may be older than the outbox window)", noteURI, series)
}

// Unbind removes a conversation's mapping.
func (b *Bridge) Unbind(streamID int64, topic string) error {
	return b.State.DeleteConversationNote(state.ConversationKey(streamID, topic))
}

// Mappings returns the current conversation -> note URI bindings.
func (b *Bridge) Mappings() map[string]string {
	return b.State.AllConversationNotes()
}

func (b *Bridge) seriesConfigured(series string) bool {
	for _, s := range b.Series {
		if s == series {
			return true
		}
	}
	return false
}

// conversation groups the recent messages seen for one stream+topic pair,
// used both to derive a title-matching candidate and to walk a
// conversation's messages oldest-first when forwarding.
type conversation struct {
	streamID int64
	topic    string
	messages []zulip.Message
}

// groupByConversation buckets messages by (stream id, topic), skipping
// private messages (type != "stream") entirely — the bridge only mirrors
// public stream discussion.
func groupByConversation(messages []zulip.Message) map[string]*conversation {
	out := map[string]*conversation{}
	for _, m := range messages {
		if m.Type != "stream" {
			continue
		}
		key := state.ConversationKey(m.StreamID, m.Subject)
		c, ok := out[key]
		if !ok {
			c = &conversation{streamID: m.StreamID, topic: m.Subject}
			out[key] = c
		}
		c.messages = append(c.messages, m)
	}
	for _, c := range out {
		sort.Slice(c.messages, func(i, j int) bool { return c.messages[i].ID < c.messages[j].ID })
	}
	return out
}

// refreshMapping pulls each series' outbox and links conversations to
// issues: explicit mappings first, then (only if opted in) title
// matching against the topic name.
func (b *Bridge) refreshMapping(ctx context.Context, messages []zulip.Message) (map[string][]graft.NoteInfo, error) {
	notesBySeries := make(map[string][]graft.NoteInfo, len(b.Series))
	index := make(map[string]graft.NoteInfo) // noteURI -> note
	for _, series := range b.Series {
		oc, err := b.Graft.Outbox(ctx, series)
		if err != nil {
			b.logf(slog.LevelError, "graft outbox failed; skipping series", "series", series, "err", err)
			continue
		}
		notes := graft.ParseNotes(b.GraftHost, oc)
		notesBySeries[series] = notes
		for _, n := range notes {
			index[n.URI] = n
		}
	}

	if err := b.applyExplicit(index); err != nil {
		return nil, err
	}
	if b.Opts.AllowTitleMatching {
		if err := b.applyTitleMatching(messages, notesBySeries); err != nil {
			return nil, err
		}
	}
	return notesBySeries, nil
}

// applyExplicit records admin-provided conversation<->note links.
func (b *Bridge) applyExplicit(index map[string]graft.NoteInfo) error {
	for key, noteURI := range b.Opts.Explicit {
		series, _, ok := ap.ParseNoteURI(b.GraftHost, noteURI)
		if !ok {
			return fmt.Errorf("explicit mapping for %q: %q is not a %s actor note URI", key, noteURI, b.GraftHost)
		}
		if _, ok := b.State.ConversationNote(key); !ok {
			if err := b.State.SetConversationNote(key, noteURI); err != nil {
				return err
			}
		}
		if n, ok := index[noteURI]; ok && n.URL != "" {
			if err := b.State.SetIssueConversation(series, n.URL, key); err != nil {
				return err
			}
		}
	}
	return nil
}

// applyTitleMatching is the opt-in heuristic fallback: a conversation's
// topic name is matched against an issue's title the same way a
// Discourse topic title would be.
func (b *Bridge) applyTitleMatching(messages []zulip.Message, notesBySeries map[string][]graft.NoteInfo) error {
	conversations := groupByConversation(messages)
	for series, notes := range notesBySeries {
		for key, conv := range conversations {
			if _, ok := b.State.ConversationNote(key); ok {
				continue
			}
			n, ok := graft.MatchIssueByTitle(notes, conv.topic)
			if !ok {
				continue
			}
			if err := b.State.SetConversationNote(key, n.URI); err != nil {
				return err
			}
			if n.URL != "" {
				if err := b.State.SetIssueConversation(series, n.URL, key); err != nil {
					return err
				}
			}
			b.logf(slog.LevelWarn, "mapped conversation by title heuristic (insecure)", "stream_id", conv.streamID, "topic", conv.topic, "series", series)
		}
	}
	return nil
}

// forwardZulipToGraft turns new Zulip messages into signed Create{Note}
// activities whose inReplyTo is the conversation's Graft issue note.
func (b *Bridge) forwardZulipToGraft(ctx context.Context, messages []zulip.Message) error {
	conversations := groupByConversation(messages)

	// Flatten back into one oldest-first list so delivery order matches
	// the order messages actually arrived, across conversations too.
	var ordered []zulip.Message
	for _, c := range conversations {
		ordered = append(ordered, c.messages...)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })

	delivered := 0
	for _, m := range ordered {
		if b.Opts.MaxDeliveriesPerPass > 0 && delivered >= b.Opts.MaxDeliveriesPerPass {
			b.logf(slog.LevelWarn, "delivery rate limit reached for this pass", "limit", b.Opts.MaxDeliveriesPerPass)
			break
		}
		// A message sent by the bridge's own bot account is always its
		// own reverse-mirror — never a real reply to forward back.
		if b.Opts.BotEmail != "" && m.SenderEmail == b.Opts.BotEmail {
			continue
		}
		if !b.streamAllowed(m.StreamID) {
			continue
		}
		key := state.ConversationKey(m.StreamID, m.Subject)
		noteURI, ok := b.State.ConversationNote(key)
		if !ok {
			continue
		}
		series, _, ok := ap.ParseNoteURI(b.GraftHost, noteURI)
		if !ok {
			continue
		}
		if b.State.MessageDelivered(series, m.ID) {
			continue
		}
		content := truncateRunes("**via Zulip, "+senderLabel(m)+":**\n\n"+m.Content, b.maxContent())
		if err := b.Graft.ReplyToIssue(ctx, series, noteURI, content); err != nil {
			b.logf(slog.LevelError, "deliver reply failed", "message", m.ID, "stream_id", m.StreamID, "topic", m.Subject, "err", err)
			continue
		}
		if err := b.State.MarkMessageDelivered(series, m.ID); err != nil {
			return err
		}
		if err := b.State.MarkDelivered(hashContent(truncateRunes(content, 200))); err != nil {
			return err
		}
		delivered++
		b.logf(slog.LevelInfo, "delivered zulip message to repo", "message", m.ID, "stream_id", m.StreamID, "topic", m.Subject, "note", noteURI)
	}
	return nil
}

// reverseGraftToZulip mirrors repo-origin comments back into the
// matching Zulip conversation. Comment notes whose content came from the
// fediverse (i.e. our own outbound posts, echoed back) are dropped.
func (b *Bridge) reverseGraftToZulip(ctx context.Context, notes map[string][]graft.NoteInfo) error {
	for series, list := range notes {
		for _, n := range list {
			if n.Kind != "comment" {
				continue
			}
			if b.State.SeenNote(series, n.URI) {
				continue
			}
			if b.State.IsDelivered(hashContent(truncateRunes(n.Summary, 200))) ||
				strings.Contains(n.Summary, "via Fediverse") || strings.Contains(n.Summary, "via Bluesky") {
				if err := b.State.MarkSeenNote(series, n.URI); err != nil {
					return err
				}
				continue
			}
			key, ok := b.State.IssueConversation(series, n.URL)
			if !ok {
				// No conversation mapping for this issue/patch yet; leave
				// unseen and retry on a later pass.
				continue
			}
			streamID, topic, ok := state.ParseConversationKey(key)
			if !ok {
				continue
			}
			body := truncateRunes("**Comment on the repository:**\n\n"+n.Summary, b.maxContent())
			if _, err := b.Zulip.SendMessage(ctx, streamID, topic, body); err != nil {
				b.logf(slog.LevelError, "send zulip message failed", "stream_id", streamID, "topic", topic, "note", n.URI, "err", err)
				continue
			}
			if err := b.State.MarkSeenNote(series, n.URI); err != nil {
				return err
			}
			b.logf(slog.LevelInfo, "mirrored repo comment to zulip", "stream_id", streamID, "topic", topic, "note", n.URI)
		}
	}
	return nil
}

// streamAllowed implements the stream allowlist. With no allowlist
// configured every stream is allowed; with one, an unknown stream is
// denied.
func (b *Bridge) streamAllowed(streamID int64) bool {
	if len(b.Opts.AllowedStreams) == 0 {
		return true
	}
	return b.Opts.AllowedStreams[streamID]
}

func (b *Bridge) maxContent() int {
	if b.Opts.MaxContentRunes <= 0 {
		return 8000
	}
	return b.Opts.MaxContentRunes
}

func (b *Bridge) maxMsgs() int {
	if b.MaxMsgs <= 0 {
		return 100
	}
	return b.MaxMsgs
}

func (b *Bridge) logf(level slog.Level, msg string, args ...any) {
	if b.Log != nil {
		b.Log.Log(context.Background(), level, msg, args...)
	}
}

// senderLabel formats a message's author for the "via Zulip" attribution
// line: the full name if set, falling back to the email.
func senderLabel(m zulip.Message) string {
	if m.SenderFullName != "" {
		return "@" + m.SenderFullName
	}
	return m.SenderEmail
}

func hashContent(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
