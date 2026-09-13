# Security — inherited from graft-discourse, plus what's Zulip-specific

`graft-zulip` shares its entire protocol/security layer with
[graft-discourse](https://github.com/jeanbaptiste/graft-discourse):
`internal/ap`, `internal/admin`, `internal/netguard`, `internal/state` and
`internal/graft` are the same code, unmodified. Every finding tracked in
graft-discourse's own `SECURITY.md` (H1–H3, M1–M8, L1–L6) therefore
already applies here too — read that file for the full history. This
file covers only what's different: the Zulip client and the
stream/topic-shaped bridge logic built on top of it.

## Built in from the start (not retrofitted)

### The bot's-own-message guard
graft-discourse shipped without checking whether a forwarded post was
actually the bridge's own reverse-mirror, authored by its own API user —
fixed later as M8, after every native repo comment produced a spurious
duplicate round-trip. `graft-zulip`'s `forwardZulipToGraft` checks
`Options.BotEmail` (set from `zulip.email`) from the first commit: a
message sent by the bridge's own bot account is always its own mirror
and is never forwarded back.

### Auth transport
Zulip's API is authenticated with HTTP Basic (bot email + API key) on
every request, rather than Discourse's custom `Api-Key`/`Api-Username`
headers. Go's `net/http` already strips `Authorization` on any
cross-host redirect by default, so — unlike the Discourse client, which
needed `netguard.Options.SensitiveHeaders` to cover its custom header
names — no extra stripping logic is needed here; the stdlib's existing
protection already covers it.

### Conversation identity
Zulip has no single stable id for "this stream, this topic" the way
Discourse has an integer topic id — a topic name can be reused across
streams, and (unlike a Discourse topic) isn't itself a distinct object
with its own id. `state.ConversationKey(streamID, topic)` formats the
pair as `"<stream_id>/<topic>"` and is used as the map key everywhere a
Discourse topic id would have been. `ParseConversationKey` is the exact
inverse — every write path goes through `ConversationKey` so the two
never drift apart.

### Admin API path encoding
`DELETE /admin/mappings/<stream_id>/<topic>` — since a Zulip topic name
is free text and may itself contain `/`, the handler splits the URL
path's remainder on the *first* `/` only and takes everything after
as the topic verbatim, rather than assuming a fixed number of path
segments.

## Not addressed (same as graft-discourse)

- Edits/deletes are not propagated (Graft mirrors create-only).
- No structured link field yet; `mappings` is manual by design.
- No tokenization/secret manager; the API key lives in a `0600` config file.
- No cursor for the Zulip side: `zulip.max_msgs` bounds each pass's
  lookback window rather than resuming from a persisted `anchor`, so a
  burst larger than that window between two polls can drop messages
  (same class of limitation as graft-discourse's outbox-window L2, just
  on the inbound side instead).
