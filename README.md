# graft-zulip

A standalone Go daemon that bridges a **Zulip** organization and a
**Graft**-mirrored repository (Forgejo/Radicle), in both directions, using
Graft's public [ActivityPub](https://www.w3.org/TR/activitypub/) surface.
It's [graft-discourse](https://github.com/jeanbaptiste/graft-discourse)'s
sibling — same protocol layer, same security posture, Zulip's
stream+topic model instead of Discourse's topics.

```
 Zulip message   ──signed Create{Note}──▶ Graft inbox ──▶ Forgejo/Radicle comment
 repo comment    ◀──Graft outbox notes──── Graft outbox
```

## How it maps

- A Zulip **stream + topic** (a "conversation") is bound to a
  Graft-mirrored repo **issue** through an explicit, admin-controlled
  `mappings` entry (`stream_id` + `topic` + `note_uri`). Title matching
  exists but is **off by default** (`allow_title_matching`), because it
  would let anyone who can start a topic impersonate an issue.
- A **message** in a mapped conversation is delivered to the Graft series
  inbox as a signed `Create{Note}` whose `inReplyTo` is the issue's Graft
  note URI. Graft then creates a real comment on the Forgejo **and**
  Radicle issue. Unlike Discourse, every message in a bound conversation
  is forwarded — Zulip has no "first post is the issue body" concept the
  way a Discourse topic's opening post is.
- Graft **comment** notes appearing in the series outbox are mirrored back
  as Zulip messages into the bound stream+topic.

## Compatibility with Graft

The protocol layer (`internal/ap`) is byte-compatible with Graft's
`internal/activitypub` — identical to graft-discourse's, unchanged here:

- draft-cavage HTTP Signatures over `(request-target) host date digest`,
  `rsa-sha256` — the exact header set Graft signs and verifies, with both
  headers required regardless of what an inbound signature claims to
  cover.
- A `Service` actor with a `#main-key` public key, so Graft can fetch it and
  verify deliveries.
- Replies only count for `issue`/`patch` notes; replies to commit notes are
  dropped by Graft, so the bridge doesn't send them.

## Security

Inherits graft-discourse's full v2.1 audit (`internal/ap`, `internal/admin`,
`internal/netguard`, `internal/state`, `internal/graft` are unchanged) —
see [SECURITY.md](SECURITY.md) for the complete history plus what's
specific to the Zulip side:

- **Explicit, admin-controlled conversation↔issue bindings** — no
  title-based impersonation by default.
- **SSRF-hardened HTTP client** (`internal/netguard`): public-address-only
  dialing with DNS-rebinding protection, validated redirect hops, and inbox
  URL validation before any signed delivery.
- **HTTPS enforced unconditionally** for `public_base_url` and
  `graft.base_url`; only `zulip.base_url` may drop to plaintext, via
  `allow_insecure_http`, scoped so it can't relax the Graft-facing or
  public endpoints. Zulip's own API key travels as HTTP Basic auth, which
  Go's stdlib already strips on any cross-host redirect.
- **`0600` secrets**: a group/world-readable `state.json` (holds the actor
  private key) or `config.json` (holds the API key) is refused at startup.
- **Timeouts everywhere**: actor server read/write/idle limits, a per-pass
  deadline, and panic recovery in the loop.
- **Rate/size limits**: `max_content_runes`, `max_deliveries_per_pass`, and
  an optional `allowed_streams` allowlist.
- **The bridge's own bot never re-forwards itself**: a message sent by
  the bot account configured in `zulip.email` is always its own
  reverse-mirror and is skipped in the forward direction — the exact bug
  graft-discourse shipped with until its M8 fix, avoided here from the
  start.

## Why an actor server

Graft verifies every inbound request by fetching the sender's actor over the
network and rejecting private/loopback URLs. The bridge therefore serves its
own actor document and **must be reachable over public HTTPS**
(`public_base_url`). A plaintext `http://` or a `localhost` URL will be
rejected by Graft's SSRF guard.

## Build & run

```sh
go build -o graft-zulip-bridge ./cmd/bridge
cp config.example.json config.json
chmod 600 config.json          # required: it holds the Zulip API key
# edit config.json: public_base_url, mappings, graft.series, zulip.*
./graft-zulip-bridge -config config.json
./graft-zulip-bridge -config config.json -once   # a single pass, then exit
```

The Zulip side needs a bot account (Zulip organization settings →
"Bots"), scoped to whichever streams it should bridge — its email and API
key go into `zulip.email` / `zulip.api_key`.

The state file (JSON) holds the actor keypair, the conversation↔issue
mapping and the dedup sets, and is written `0600`. Back it up; losing the
keypair invalidates verification, and losing the dedup sets can
re-deliver.

## Admin API

Binding a conversation to an issue no longer requires editing config and
restarting. Set `admin_token` in config (or, better, the
`GRAFT_BRIDGE_ADMIN_TOKEN` environment variable, which overrides it). The
API is **disabled entirely when the token is empty**.

```sh
TOKEN=...
# discover replyable issues/patches to bind (optional ?series= and ?q= filters)
curl -H "Authorization: Bearer $TOKEN" \
  'https://bridge.example.org/admin/issues?series=federation-x&q=flux'
# list current bindings
curl -H "Authorization: Bearer $TOKEN" https://bridge.example.org/admin/mappings
# bind stream 100, topic "federation-x issue #7", to an issue note
curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"stream_id":100,"topic":"federation-x issue #7","note_uri":"https://f1.cyberwild.org/actors/federation-x/notes/7"}' \
  https://bridge.example.org/admin/mappings
# unbind — stream id then topic name in the path
curl -X DELETE -H "Authorization: Bearer $TOKEN" \
  'https://bridge.example.org/admin/mappings/100/federation-x%20issue%20%237'
```

`GET /admin/issues` returns `{series, entry_id, kind, title, note_uri, url}`
for every replyable note in the configured series' outboxes, so you can copy
the `note_uri` straight into a binding.

`Bind` refuses a note URI that isn't on the configured Graft host, isn't in a
configured series, or is a commit note (Graft only accepts replies to
issues/patches). Requests are bearer-authenticated in constant time and
body-limited.

## Tests

```sh
go test ./...
```

The AP signature round-trip, note parsing/matching, and both sync directions
(including echo suppression, the bot's-own-message guard, and idempotency)
are covered with fakes — no live Graft or Zulip needed.

## Limitations

- **Manual mapping.** Conversations are bound to issues via the
  `mappings` config. There is no discovery service; a new issue needs a
  new mapping entry. The opt-in title heuristic is a stopgap, not a
  substitute.
- **Create-only.** Graft does not propagate edits or deletes, in either
  direction, so neither does the bridge.
- **Flattened comments.** Graft stores comments as text (HTML stripped); the
  bridge mirrors them as plain messages, without threading.
- **ATProto is not wired.** Graft's Bluesky client only reads replies to its
  own posts and is inert without credentials, so only the ActivityPub path is
  used here.
- **Echo handling is heuristic** for the reverse direction: Fediverse-origin
  comments (including our own, which Graft re-publishes) are recognized by
  the `via Fediverse` marker and truncated-content hashes and dropped from
  reverse sync.
- **No cursor.** `zulip.max_msgs` bounds how far back each pass looks;
  a burst larger than that window between two polls can drop messages.
  The durable fix is Zulip's `anchor`-based paging with a persisted
  cursor, not implemented yet.
- Graft itself is young; its note format is not frozen.

## Layout

```
cmd/bridge        wiring, actor HTTP server, poll loop
internal/ap       ActivityPub actor, signatures, notes (Graft-compatible)
internal/admin    authenticated conversation<->issue binding API
internal/netguard SSRF-safe HTTP clients (public-only dialing, redirect hygiene)
internal/graft    Graft REST/AP client + outbox note parsing
internal/zulip    Zulip REST client (bot auth, messages)
internal/state    JSON bookkeeping (keys, mapping, dedup), 0600
internal/bridge   the two-direction reconciliation
```
