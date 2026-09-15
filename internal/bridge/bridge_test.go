package bridge

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"graftzulip/internal/ap"
	"graftzulip/internal/state"
	"graftzulip/internal/zulip"
)

const testHost = "g.example"

type replyCall struct {
	series, noteURI, content, sourceURL string
}

type fakeGraft struct {
	outbox  *ap.OrderedCollection
	replies []replyCall
}

func (f *fakeGraft) Outbox(_ context.Context, _ string) (*ap.OrderedCollection, error) {
	return f.outbox, nil
}

func (f *fakeGraft) ReplyToIssue(_ context.Context, series, noteURI, content, sourceURL string) error {
	f.replies = append(f.replies, replyCall{series, noteURI, content, sourceURL})
	return nil
}

type sentMessage struct {
	streamID int64
	topic    string
	content  string
}

type fakeZulip struct {
	messages []zulip.Message
	sent     []sentMessage
}

func (f *fakeZulip) LatestMessages(context.Context, int) ([]zulip.Message, error) {
	return f.messages, nil
}

func (f *fakeZulip) SendMessage(_ context.Context, streamID int64, topic, content string) (zulip.Message, error) {
	f.sent = append(f.sent, sentMessage{streamID, topic, content})
	return zulip.Message{ID: 1, StreamID: streamID, Subject: topic}, nil
}

func newTestState(t *testing.T) *state.State {
	t.Helper()
	st, err := state.Load(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("state.Load: %v", err)
	}
	return st
}

func fluxOutbox(issueURI, issueURL string) *ap.OrderedCollection {
	return &ap.OrderedCollection{OrderedItems: []ap.Create{
		{Object: ap.Note{ID: issueURI, Content: "Issue: Fix the flux capacitor", URL: issueURL}},
		{Object: ap.Note{ID: ap.NoteURI(testHost, "fedx", 9), Content: "comment: Fixed in #12", URL: issueURL}},
	}}
}

func TestRunOnceBothDirections(t *testing.T) {
	issueURI := ap.NoteURI(testHost, "fedx", 7)
	issueURL := "https://forgejo.example/fedx/issues/7"

	g := &fakeGraft{outbox: fluxOutbox(issueURI, issueURL)}
	z := &fakeZulip{
		messages: []zulip.Message{
			{ID: 999, Type: "stream", StreamID: 100, Subject: "Fix the flux capacitor", SenderEmail: "bob@example.org", Content: "Opening the issue"},
			{ID: 1000, Type: "stream", StreamID: 100, Subject: "Fix the flux capacitor", SenderEmail: "alice@example.org", SenderFullName: "Alice", Content: "Any progress on this?"},
		},
	}
	b := &Bridge{
		GraftHost: testHost,
		Series:    []string{"fedx"},
		Opts:      Options{AllowTitleMatching: true},
		Zulip:     z,
		Graft:     g,
		State:     newTestState(t),
	}
	if err := b.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// Both messages in the conversation get forwarded — Zulip has no
	// "first post is the issue body" concept the way Discourse does.
	if len(g.replies) != 2 {
		t.Fatalf("bad replies: %+v", g.replies)
	}
	if !strings.Contains(g.replies[1].content, "Any progress on this?") {
		t.Fatalf("bad reply content: %+v", g.replies[1])
	}
	if len(z.sent) != 1 || z.sent[0].streamID != 100 || !strings.Contains(z.sent[0].content, "Fixed in #12") {
		t.Fatalf("bad sent: %+v", z.sent)
	}
}

func TestExplicitMappingWithoutTitleMatching(t *testing.T) {
	issueURI := ap.NoteURI(testHost, "fedx", 7)
	issueURL := "https://forgejo.example/fedx/issues/7"

	g := &fakeGraft{outbox: fluxOutbox(issueURI, issueURL)}
	z := &fakeZulip{messages: []zulip.Message{
		{ID: 1000, Type: "stream", StreamID: 100, Subject: "totally unrelated topic", SenderEmail: "alice@example.org", Content: "ping"},
	}}
	key := state.ConversationKey(100, "totally unrelated topic")
	b := &Bridge{
		GraftHost: testHost,
		Series:    []string{"fedx"},
		Opts:      Options{Explicit: map[string]string{key: issueURI}},
		Zulip:     z,
		Graft:     g,
		State:     newTestState(t),
	}
	if err := b.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(g.replies) != 1 || g.replies[0].noteURI != issueURI {
		t.Fatalf("explicit mapping not honored: %+v", g.replies)
	}
}

func TestTitleMatchingDisabledByDefault(t *testing.T) {
	issueURI := ap.NoteURI(testHost, "fedx", 7)
	issueURL := "https://forgejo.example/fedx/issues/7"

	g := &fakeGraft{outbox: fluxOutbox(issueURI, issueURL)}
	z := &fakeZulip{messages: []zulip.Message{
		{ID: 1000, Type: "stream", StreamID: 100, Subject: "Fix the flux capacitor", SenderEmail: "mallory@example.org", Content: "impersonating"},
	}}
	b := &Bridge{GraftHost: testHost, Series: []string{"fedx"}, Zulip: z, Graft: g, State: newTestState(t)}
	if err := b.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(g.replies) != 0 {
		t.Fatalf("title matching ran without opt-in: %+v", g.replies)
	}
}

func TestStreamAllowlist(t *testing.T) {
	issueURI := ap.NoteURI(testHost, "fedx", 7)
	issueURL := "https://forgejo.example/fedx/issues/7"

	g := &fakeGraft{outbox: fluxOutbox(issueURI, issueURL)}
	z := &fakeZulip{messages: []zulip.Message{
		{ID: 1000, Type: "stream", StreamID: 99, Subject: "Flux", SenderEmail: "alice@example.org", Content: "ping"},
	}}
	key := state.ConversationKey(99, "Flux")
	b := &Bridge{
		GraftHost: testHost,
		Series:    []string{"fedx"},
		Opts: Options{
			Explicit:       map[string]string{key: issueURI},
			AllowedStreams: map[int64]bool{5: true},
		},
		Zulip: z, Graft: g, State: newTestState(t),
	}
	if err := b.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(g.replies) != 0 {
		t.Fatalf("disallowed stream was forwarded: %+v", g.replies)
	}
}

func TestRunOnceIsIdempotent(t *testing.T) {
	issueURI := ap.NoteURI(testHost, "fedx", 7)
	issueURL := "https://forgejo.example/fedx/issues/7"

	g := &fakeGraft{outbox: fluxOutbox(issueURI, issueURL)}
	z := &fakeZulip{messages: []zulip.Message{
		{ID: 1000, Type: "stream", StreamID: 100, Subject: "Fix the flux capacitor", SenderEmail: "alice@example.org", Content: "ping"},
	}}
	b := &Bridge{
		GraftHost: testHost, Series: []string{"fedx"},
		Opts:  Options{AllowTitleMatching: true},
		Zulip: z, Graft: g, State: newTestState(t),
	}
	if err := b.RunOnce(context.Background()); err != nil {
		t.Fatalf("first RunOnce: %v", err)
	}
	if err := b.RunOnce(context.Background()); err != nil {
		t.Fatalf("second RunOnce: %v", err)
	}
	if len(g.replies) != 1 || len(z.sent) != 1 {
		t.Fatalf("not idempotent: replies=%d sent=%d", len(g.replies), len(z.sent))
	}
}

func TestForwardSkipsBotsOwnMessage(t *testing.T) {
	issueURI := ap.NoteURI(testHost, "fedx", 7)
	issueURL := "https://forgejo.example/fedx/issues/7"

	g := &fakeGraft{outbox: fluxOutbox(issueURI, issueURL)}
	z := &fakeZulip{messages: []zulip.Message{
		{ID: 1000, Type: "stream", StreamID: 100, Subject: "Fix the flux capacitor", SenderEmail: "bridge-bot@zulip.example", Content: "**Comment on the repository:**\n\nFixed in #12"},
	}}
	b := &Bridge{
		GraftHost: testHost,
		Series:    []string{"fedx"},
		Opts:      Options{AllowTitleMatching: true, BotEmail: "bridge-bot@zulip.example"},
		Zulip:     z,
		Graft:     g,
		State:     newTestState(t),
	}
	if err := b.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(g.replies) != 0 {
		t.Fatalf("bot's own message was forwarded back to Graft: %+v", g.replies)
	}
}

func TestReverseDropsFediverseEcho(t *testing.T) {
	issueURI := ap.NoteURI(testHost, "fedx", 7)
	issueURL := "https://forgejo.example/fedx/issues/7"

	g := &fakeGraft{outbox: &ap.OrderedCollection{OrderedItems: []ap.Create{
		{Object: ap.Note{ID: issueURI, Content: "Issue: Flux", URL: issueURL}},
		{Object: ap.Note{
			ID:      ap.NoteURI(testHost, "fedx", 9),
			Content: "comment: via Fediverse, @alice: ping",
			URL:     issueURL,
		}},
	}}}
	z := &fakeZulip{}
	b := &Bridge{
		GraftHost: testHost, Series: []string{"fedx"},
		Opts:  Options{AllowTitleMatching: true},
		Zulip: z, Graft: g, State: newTestState(t),
	}
	if err := b.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(z.sent) != 0 {
		t.Fatalf("fediverse echo mirrored back: %+v", z.sent)
	}
}

func TestBindValidatesNote(t *testing.T) {
	issueURI := ap.NoteURI(testHost, "fedx", 7)
	issueURL := "https://forgejo.example/fedx/issues/7"
	g := &fakeGraft{outbox: &ap.OrderedCollection{OrderedItems: []ap.Create{
		{Object: ap.Note{ID: issueURI, Content: "Issue: Flux", URL: issueURL}},
		{Object: ap.Note{ID: ap.NoteURI(testHost, "fedx", 8), Content: "Commit: bump"}},
	}}}
	b := &Bridge{GraftHost: testHost, Series: []string{"fedx"}, Graft: g, State: newTestState(t)}
	ctx := context.Background()

	if err := b.Bind(ctx, 100, "flux", issueURI); err != nil {
		t.Fatalf("bind issue: %v", err)
	}
	key := state.ConversationKey(100, "flux")
	if uri, ok := b.State.ConversationNote(key); !ok || uri != issueURI {
		t.Fatalf("mapping not stored: %q %v", uri, ok)
	}
	if err := b.Bind(ctx, 101, "flux", ap.NoteURI(testHost, "fedx", 8)); err == nil {
		t.Fatal("bind to a commit note should fail")
	}
	if err := b.Bind(ctx, 102, "flux", "https://evil.example/actors/fedx/notes/7"); err == nil {
		t.Fatal("bind to a foreign host should fail")
	}
	if err := b.Bind(ctx, 103, "flux", ap.NoteURI(testHost, "other", 1)); err == nil {
		t.Fatal("bind to a non-configured series should fail")
	}
	if err := b.Unbind(100, "flux"); err != nil {
		t.Fatalf("unbind: %v", err)
	}
	if _, ok := b.State.ConversationNote(key); ok {
		t.Fatal("mapping not removed")
	}
}

func TestMessageURL(t *testing.T) {
	got := messageURL("https://zulip.example/", 7, "Issue #3: a.b (draft)", 42)
	want := "https://zulip.example/#narrow/stream/7/topic/Issue.20.233.3A.20a.2Eb.20.28draft.29/near/42"
	if got != want {
		t.Errorf("messageURL = %q, want %q", got, want)
	}
	if got := messageURL("", 7, "x", 42); got != "" {
		t.Errorf("no web URL should give no trackback, got %q", got)
	}
}
