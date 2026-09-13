package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// dedupRetention bounds how long an entry survives in the Delivered,
// PostsDelivered and SeenNotes sets before it's pruned. Nothing ever
// removed entries before, so a long-running bridge's state.json grew
// without bound; 180 days is far past any realistic window for a Zulip
// message or Graft note to still be "in flight" for dedup purposes, so
// pruning past it can't cause a duplicate delivery in practice while
// still keeping the file bounded over the bridge's lifetime.
const dedupRetention = 180 * 24 * time.Hour

// ConversationKey identifies one Zulip stream+topic pair — the Zulip
// equivalent of a Discourse topic id. Formatted, not a struct, since it's
// used directly as a map key throughout.
func ConversationKey(streamID int64, topic string) string {
	return strconv.FormatInt(streamID, 10) + "/" + topic
}

// ParseConversationKey is ConversationKey's inverse.
func ParseConversationKey(key string) (streamID int64, topic string, ok bool) {
	i := strings.Index(key, "/")
	if i < 0 {
		return 0, "", false
	}
	id, err := strconv.ParseInt(key[:i], 10, 64)
	if err != nil {
		return 0, "", false
	}
	return id, key[i+1:], true
}

// KeyPair is a bridge actor's PEM keypair.
type KeyPair struct {
	PrivatePEM string `json:"private_pem"`
	PublicPEM  string `json:"public_pem"`
}

// State is the bridge's durable bookkeeping, persisted as a JSON file. It
// is deliberately small: identity keys, the topic<->issue mapping, and the
// dedup sets that keep the two sync directions from looping.
type State struct {
	mu   sync.Mutex
	path string

	ActorKeys map[string]KeyPair `json:"actor_keys"`
	// ConversationNotes and IssueConversations are keyed by
	// ConversationKey(streamID, topic) — Zulip has no single stable id
	// for "stream X, topic Y" the way Discourse has a topic id, so the
	// pair is the identity.
	ConversationNotes  map[string]string `json:"conversation_notes"`  // conversation key -> Graft issue/patch note URI
	IssueConversations map[string]string `json:"issue_conversations"` // "series|issueURL" -> conversation key

	// Delivered, MessagesDelivered and SeenNotes are dedup sets keyed by
	// the unix-second timestamp an entry was first recorded, not just a
	// bare bool, so saveLocked can prune anything older than
	// dedupRetention instead of growing these forever.
	Delivered         map[string]int64 `json:"delivered"`          // content hash of a Zulip message already sent to Graft
	MessagesDelivered map[string]int64 `json:"messages_delivered"` // "series|messageID" already sent to Graft
	SeenNotes         map[string]int64 `json:"seen_notes"`         // "series|noteURI" already mirrored to Zulip
}

// Load reads state from path, creating an empty state if the file is absent.
// Because the file holds the actor's private key, a group- or
// world-accessible file is refused outright rather than silently used.
func Load(path string) (*State, error) {
	s := &State{path: path}
	if info, err := os.Stat(path); err == nil {
		if mode := info.Mode().Perm(); mode&0o077 != 0 {
			return nil, fmt.Errorf("state file %s is accessible to other users (mode %04o); run: chmod 600 %s", path, mode, path)
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		s.initMaps()
		return s, nil
	}
	if err := json.Unmarshal(b, s); err != nil {
		return nil, fmt.Errorf("parse state %s: %w", path, err)
	}
	s.initMaps()
	return s, nil
}

func (s *State) initMaps() {
	if s.ActorKeys == nil {
		s.ActorKeys = map[string]KeyPair{}
	}
	if s.ConversationNotes == nil {
		s.ConversationNotes = map[string]string{}
	}
	if s.IssueConversations == nil {
		s.IssueConversations = map[string]string{}
	}
	if s.Delivered == nil {
		s.Delivered = map[string]int64{}
	}
	if s.MessagesDelivered == nil {
		s.MessagesDelivered = map[string]int64{}
	}
	if s.SeenNotes == nil {
		s.SeenNotes = map[string]int64{}
	}
}

// prune drops dedup entries older than dedupRetention. Called from
// saveLocked, under s.mu, before every write.
func (s *State) prune() {
	cutoff := time.Now().Add(-dedupRetention).Unix()
	for _, m := range []map[string]int64{s.Delivered, s.MessagesDelivered, s.SeenNotes} {
		for k, ts := range m {
			if ts < cutoff {
				delete(m, k)
			}
		}
	}
}

// Save atomically writes the state back to disk.
func (s *State) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked()
}

func (s *State) saveLocked() error {
	s.prune()
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	// The state file holds the actor's private key and mapping bookkeeping;
	// keep it owner-only regardless of umask.
	if err := os.Chmod(tmpName, 0o600); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, s.path)
}

// KeyPair returns the stored keypair for name.
func (s *State) KeyPair(name string) (KeyPair, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kp, ok := s.ActorKeys[name]
	return kp, ok
}

// SetKeyPair stores name's keypair and persists.
func (s *State) SetKeyPair(name string, kp KeyPair) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ActorKeys[name] = kp
	return s.saveLocked()
}

// ConversationNote returns the Graft note URI mapped to a Zulip
// stream+topic conversation key (see ConversationKey).
func (s *State) ConversationNote(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.ConversationNotes[key]
	return v, ok
}

// SetConversationNote records a conversation -> Graft note URI mapping.
func (s *State) SetConversationNote(key, noteURI string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ConversationNotes[key] = noteURI
	return s.saveLocked()
}

// DeleteConversationNote removes a conversation's mapping and any
// issue-URL links that point at it, and persists. It is a no-op if the
// conversation was not mapped.
func (s *State) DeleteConversationNote(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.ConversationNotes, key)
	for k, v := range s.IssueConversations {
		if v == key {
			delete(s.IssueConversations, k)
		}
	}
	return s.saveLocked()
}

// AllConversationNotes returns a copy of the conversation -> note URI
// mappings.
func (s *State) AllConversationNotes() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.ConversationNotes))
	for k, v := range s.ConversationNotes {
		out[k] = v
	}
	return out
}

// IssueConversation returns the conversation key mapped to a repo issue
// URL.
func (s *State) IssueConversation(series, issueURL string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.IssueConversations[series+"|"+issueURL]
	return v, ok
}

// SetIssueConversation records a repo issue URL -> conversation key
// mapping.
func (s *State) SetIssueConversation(series, issueURL, conversationKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.IssueConversations[series+"|"+issueURL] = conversationKey
	return s.saveLocked()
}

// IsDelivered reports whether a Zulip message hash was already sent.
func (s *State) IsDelivered(hash string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.Delivered[hash]
	return ok
}

// MarkDelivered records a Discourse post hash and persists.
func (s *State) MarkDelivered(hash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Delivered[hash] = time.Now().Unix()
	return s.saveLocked()
}

// MessageDelivered reports whether a specific Zulip message already
// reached Graft, keyed on its stable id (not its content, which a user
// can edit).
func (s *State) MessageDelivered(series string, messageID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.MessagesDelivered[series+"|"+strconv.FormatInt(messageID, 10)]
	return ok
}

// MarkMessageDelivered records a Zulip message id as sent and persists.
func (s *State) MarkMessageDelivered(series string, messageID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.MessagesDelivered[series+"|"+strconv.FormatInt(messageID, 10)] = time.Now().Unix()
	return s.saveLocked()
}

// SeenNote reports whether a Graft note was already mirrored to Zulip.
func (s *State) SeenNote(series, noteURI string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.SeenNotes[series+"|"+noteURI]
	return ok
}

// MarkSeenNote records a Graft note as mirrored and persists.
func (s *State) MarkSeenNote(series, noteURI string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SeenNotes[series+"|"+noteURI] = time.Now().Unix()
	return s.saveLocked()
}
